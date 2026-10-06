package table

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
)

// logRecords 打印一段变更流水，便于在测试日志中核对场景覆盖了哪些记录。
func logRecords(t *testing.T, prefix string, recs []ChangeRecord) {
	t.Helper()
	for _, r := range recs {
		t.Logf("%s record seq=%d snapshot=%s op=%s key=%v row=%v",
			prefix, r.Seq, r.SnapshotID, r.Op, r.Key, r.Row)
	}
	if len(recs) == 0 {
		t.Logf("%s (no change records)", prefix)
	}
}

// TestNoOpUpsertNoChangeRecord 场景：增量明细只反映真实发生的行级变更。
//   - 把某一行按原样（列值一个都没改）再提交一遍，流水里不多出任何记录；
//   - 确实改了值的行（哪怕只改一列）仍正常报 update；
//   - 新增、删除照旧；删除不存在的行不留痕迹；
//   - 同批次同一主键连写：同值连写只留一条，改值连写按序各留一条；
//   - 重复读取一致、断点续读只返回未消费部分，不出现重复上报或漏报。
func TestNoOpUpsertNoChangeRecord(t *testing.T) {
	tbl := newTestTable(t)
	head, _ := tbl.Head()

	// snap1：新增 a、b。
	b1 := Batch{BatchID: "noop-1", Changes: []Change{
		{Upsert: &Row{"id": "a", "name": "alice", "score": int64(10)}},
		{Upsert: &Row{"id": "b", "name": "bob", "score": int64(20)}},
	}}
	snap1, err := tbl.Commit(head, b1)
	if err != nil {
		t.Fatal(err)
	}
	logBatch(t, "[noop-upsert] seed inserts", snap1, b1)
	sumBefore := checksum(t, mustRead(t, tbl, ""))

	// snap2：把 a 按原样再提交一遍（列值一个都没改）→ 不产生任何变更记录。
	// 故意用 float64 写 score（模拟 JSON 反序列化后的调用方），
	// 与基线中的 int64 语义同值，不应被误判为更新。
	identical := Batch{BatchID: "noop-2", Changes: []Change{
		{Upsert: &Row{"id": "a", "name": "alice", "score": float64(10)}},
	}}
	snap2, err := tbl.Commit(snap1, identical)
	if err != nil {
		t.Fatal(err)
	}
	logBatch(t, "[noop-upsert] identical re-commit of row a", snap2, identical)

	recs, err := tbl.ReadChangesSince(0)
	if err != nil {
		t.Fatal(err)
	}
	logRecords(t, "[noop-upsert] feed after identical re-commit", recs)
	if len(recs) != 2 { // 只有 snap1 的两条 insert
		t.Fatalf("identical re-commit produced spurious records: %+v", recs)
	}
	for _, r := range recs {
		if r.SnapshotID == snap2 {
			t.Errorf("no-op commit snapshot %s leaked into feed: %+v", snap2, r)
		}
	}
	// 数据本身也不变。
	if sum := checksum(t, mustRead(t, tbl, "")); sum != sumBefore {
		t.Fatalf("identical re-commit changed data: checksum %d -> %d", sumBefore, sum)
	}
	t.Logf("[noop-upsert] snapshot=%s data checksum=%d unchanged, feed has no record for it", snap2, sumBefore)

	// snap3：只改 a 的一列（score 10->11）→ 必须正常报 update。
	oneCol := Batch{BatchID: "noop-3", Changes: []Change{
		{Upsert: &Row{"id": "a", "name": "alice", "score": int64(11)}},
	}}
	snap3, err := tbl.Commit(snap2, oneCol)
	if err != nil {
		t.Fatal(err)
	}
	logBatch(t, "[noop-upsert] one-column change on a", snap3, oneCol)

	// snap4：同批次同一主键连写 + 删不存在的行 + 删了再原样写回。
	//   c: upsert v1, upsert v1（同值）→ 只留 1 条 insert；
	//   d: upsert v1, upsert v2（改值）→ insert + update 各一条；
	//   ghost: 删除不存在的行 → 不留痕迹；
	//   b: 删除后原样写回 → delete + insert 两条真实记录。
	mixed := Batch{BatchID: "noop-4", Changes: []Change{
		{Upsert: &Row{"id": "c", "name": "carol", "score": int64(30)}},
		{Upsert: &Row{"id": "c", "name": "carol", "score": int64(30)}},
		{Upsert: &Row{"id": "d", "name": "dan", "score": int64(40)}},
		{Upsert: &Row{"id": "d", "name": "dan", "score": int64(41)}},
		{Delete: ptr(Value("ghost"))},
		{Delete: ptr(Value("b"))},
		{Upsert: &Row{"id": "b", "name": "bob", "score": int64(20)}},
	}}
	snap4, err := tbl.Commit(snap3, mixed)
	if err != nil {
		t.Fatal(err)
	}
	logBatch(t, "[noop-upsert] mixed batch (same-pk rewrites, ghost delete, delete+reinsert)", snap4, mixed)

	recs, err = tbl.ReadChangesSince(0)
	if err != nil {
		t.Fatal(err)
	}
	logRecords(t, "[noop-upsert] full feed", recs)

	want := []struct {
		snap string
		op   ChangeOp
		key  string
	}{
		{snap1, OpInsert, "a"}, {snap1, OpInsert, "b"},
		// snap2（原样重复提交）没有任何记录
		{snap3, OpUpdate, "a"},
		{snap4, OpInsert, "c"},
		{snap4, OpInsert, "d"}, {snap4, OpUpdate, "d"},
		{snap4, OpDelete, "b"}, {snap4, OpInsert, "b"},
	}
	if len(recs) != len(want) {
		t.Fatalf("want %d records, got %d: %+v", len(want), len(recs), recs)
	}
	for i, w := range want {
		r := recs[i]
		if r.SnapshotID != w.snap || r.Op != w.op || r.Key != w.key {
			t.Errorf("record %d: want {%s %s %s}, got {%s %s %v}",
				i, w.snap, w.op, w.key, r.SnapshotID, r.Op, r.Key)
		}
	}
	// update 记录带变更后整行；delete 记录是主键 tombstone。
	// （记录经 JSON 落盘读回，数值为 float64，按语义值比较。）
	if fmt.Sprint(recs[2].Row["score"]) != "11" {
		t.Errorf("update record should carry new values: %+v", recs[2].Row)
	}
	if recs[6].Row != nil {
		t.Errorf("delete record should be key-only tombstone: %+v", recs[6])
	}
	// 顺序稳定：Seq 非递减。
	for i := 1; i < len(recs); i++ {
		if recs[i].Seq < recs[i-1].Seq {
			t.Fatalf("record seq not monotonic: %+v", recs)
		}
	}

	// 重复读取结果一致。
	recs2, err := tbl.ReadChangesSince(0)
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprintf("%v", recs) != fmt.Sprintf("%v", recs2) {
		t.Fatal("repeated ReadChangesSince returned different results")
	}
	t.Logf("[noop-upsert] repeated read identical: %d records", len(recs2))

	// 断点续读：游标推到 snap3 之后，只返回 snap4 的未消费部分，
	// 且不重报 snap3 及之前的记录、不漏报 snap4 的记录。
	snaps, err := tbl.ReadSince(0)
	if err != nil {
		t.Fatal(err)
	}
	var seqSnap3 uint64
	for _, s := range snaps {
		if s.ID == snap3 {
			seqSnap3 = s.Seq
		}
		t.Logf("[noop-upsert] flow snapshot=%s seq=%d batch=%q", s.ID, s.Seq, s.BatchID)
	}
	resumed, err := tbl.ReadChangesSince(seqSnap3)
	if err != nil {
		t.Fatal(err)
	}
	logRecords(t, "[noop-upsert] resumed after snap3", resumed)
	if len(resumed) != 5 { // snap4 的 5 条
		t.Fatalf("resume from seq=%d: want 5 records, got %+v", seqSnap3, resumed)
	}
	for _, r := range resumed {
		if r.Seq <= seqSnap3 {
			t.Errorf("resumed record already consumed: %+v", r)
		}
		if r.SnapshotID != snap4 {
			t.Errorf("resumed record from wrong snapshot: %+v", r)
		}
	}
	// 游标推到最新后结果为空，不多报。
	latest, err := tbl.ReadChangesSince(snaps[len(snaps)-1].Seq)
	if err != nil {
		t.Fatal(err)
	}
	if len(latest) != 0 {
		t.Fatalf("want empty feed at latest seq, got %+v", latest)
	}
	t.Log("[noop-upsert] resume returns only unconsumed records; cursor at latest yields empty")
}

// mustRead 读取 HEAD 数据，失败即终止测试。
func mustRead(t *testing.T, tbl *Table, snapID string) []Row {
	t.Helper()
	rows, err := tbl.Read(snapID)
	if err != nil {
		t.Fatal(err)
	}
	return rows
}

// TestConcurrentCreateTable 场景：多个实例同时创建同一张表，
// 恰好一个成功，其余收到可区分的 ErrTableExists；
// 被拒绝方不得改写已建好的结构与数据，最终表结构与数据自洽。
func TestConcurrentCreateTable(t *testing.T) {
	dir := t.TempDir()
	store := NewStore(dir)

	const creators = 8
	// 每个创建者用不同的结构（不同的标记列），事后可辨认胜者是哪一份。
	schemas := make([]Schema, creators)
	for i := range schemas {
		schemas[i] = Schema{PrimaryKey: "id", Columns: []Column{
			{Name: "id", Type: TypeString},
			{Name: fmt.Sprintf("mark_%d", i), Type: TypeString},
		}}
	}

	errs := make([]error, creators)
	var wg sync.WaitGroup
	for i := 0; i < creators; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, errs[i] = CreateTable(store, "contested", schemas[i])
			t.Logf("[create-race] creator=%d result=%v", i, errs[i])
		}(i)
	}
	wg.Wait()

	var success, exists int
	winner := -1
	for i, err := range errs {
		switch {
		case err == nil:
			success++
			winner = i
		case errors.Is(err, ErrTableExists):
			exists++
			// 必须能与其他失败原因区分开。
			if errors.Is(err, ErrCommitConflict) || errors.Is(err, ErrSchemaIncompatible) ||
				errors.Is(err, ErrInvalidChange) || errors.Is(err, ErrDuplicateBatch) {
				t.Errorf("creator %d: table-exists error misclassified: %v", i, err)
			}
		default:
			t.Errorf("creator %d: unexpected error: %v", i, err)
		}
	}
	t.Logf("[create-race] success=%d table-exists=%d winner=creator%d", success, exists, winner)
	if success != 1 || exists != creators-1 {
		t.Fatalf("want 1 success + %d ErrTableExists, got %d/%d", creators-1, success, exists)
	}

	// 最终结构就是胜者那一份：标记列唯一且属于胜者，失败者的列没有混入。
	tbl, err := OpenTable(store, "contested")
	if err != nil {
		t.Fatal(err)
	}
	schema, err := tbl.SchemaAt("")
	if err != nil {
		t.Fatal(err)
	}
	if len(schema.Columns) != 2 || schema.Columns[1].Name != fmt.Sprintf("mark_%d", winner) {
		t.Fatalf("final schema is not the winner's (creator%d): %+v", winner, schema.Columns)
	}
	t.Logf("[create-race] final schema columns=%v (winner's schema intact)",
		[]string{schema.Columns[0].Name, schema.Columns[1].Name})

	// 初始数据自洽：空数据、HEAD 为初始快照，后续可正常提交。
	rows := mustRead(t, tbl, "")
	if len(rows) != 0 {
		t.Fatalf("initial data not empty: %+v", rows)
	}
	head, _ := tbl.Head()
	b := Batch{BatchID: "post-create", Changes: []Change{
		{Upsert: &Row{"id": "x", fmt.Sprintf("mark_%d", winner): "v"}},
	}}
	if _, err := tbl.Commit(head, b); err != nil {
		t.Fatalf("commit on won table failed: %v", err)
	}
	if got := mustRead(t, tbl, ""); len(got) != 1 {
		t.Fatalf("table not usable after create race: %+v", got)
	}

	// 事后再建同名表仍被可区分地拒绝，且不影响已有数据。
	if _, err := CreateTable(store, "contested", schemas[0]); !errors.Is(err, ErrTableExists) {
		t.Fatalf("re-create after race: want ErrTableExists, got %v", err)
	}
	if got := mustRead(t, tbl, ""); len(got) != 1 {
		t.Fatalf("re-create attempt modified existing data: %+v", got)
	}
	t.Log("[create-race] late re-create rejected with ErrTableExists, existing data untouched")
}

// TestHelperCreateProcess 不是真正的测试，而是跨进程并发建表的子进程入口：
// 子进程独立以带标记列的结构创建同一张表，把结果分类打印到 stdout（RESULT|... 行）。
func TestHelperCreateProcess(t *testing.T) {
	if os.Getenv("GO_WANT_HELPER_PROCESS") != "1" {
		return
	}
	// os.Args: <binary> -test.run=... -- <dir> <table> <tag>
	sep := 0
	for i, a := range os.Args {
		if a == "--" {
			sep = i
			break
		}
	}
	args := os.Args[sep+1:]
	dir, name, tag := args[0], args[1], args[2]

	schema := Schema{PrimaryKey: "id", Columns: []Column{
		{Name: "id", Type: TypeString},
		{Name: "mark_" + tag, Type: TypeString},
	}}
	_, err := CreateTable(NewStore(dir), name, schema)
	switch {
	case err == nil:
		fmt.Printf("RESULT|OK|%s\n", tag)
	case errors.Is(err, ErrTableExists):
		fmt.Printf("RESULT|EXISTS|%v\n", err)
	case errors.Is(err, ErrCommitConflict), errors.Is(err, ErrSchemaIncompatible),
		errors.Is(err, ErrInvalidChange), errors.Is(err, ErrDuplicateBatch):
		fmt.Printf("RESULT|WRONGKIND|%v\n", err)
	default:
		fmt.Printf("RESULT|OTHER|%v\n", err)
	}
}

// runHelperCreate 启动一个子进程建表，返回 RESULT 行的分类与详情。
func runHelperCreate(t *testing.T, dir, table, tag string) (string, string) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestHelperCreateProcess$", "--",
		dir, table, tag)
	cmd.Env = append(os.Environ(), "GO_WANT_HELPER_PROCESS=1")
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("helper create process tag=%s: %v\n%s", tag, err, out)
	}
	for _, line := range strings.Split(string(out), "\n") {
		if strings.HasPrefix(line, "RESULT|") {
			parts := strings.SplitN(line, "|", 3)
			return parts[1], parts[2]
		}
	}
	t.Fatalf("helper create process tag=%s printed no RESULT line:\n%s", tag, out)
	return "", ""
}

// TestCrossProcessConcurrentCreateTable 跨进程建表竞态：多个独立进程（模拟脚本/命令行工具）同时创建同一张表，
// 恰好一个成功，其余收到可区分的 ErrTableExists；
// 最终表结构就是胜者那一份，失败者的结构没有顶掉已建好的表。
func TestCrossProcessConcurrentCreateTable(t *testing.T) {
	dir := t.TempDir()

	const procs = 5
	kinds := make([]string, procs)
	details := make([]string, procs)
	var wg sync.WaitGroup
	for i := 0; i < procs; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			kind, detail := runHelperCreate(t, dir, "contested", fmt.Sprintf("p%d", i))
			kinds[i], details[i] = kind, detail
			t.Logf("[create-race-xproc] proc=%d tag=p%d result=%s %s", i, i, kind, detail)
		}(i)
	}
	wg.Wait()

	var ok, exists int
	winner := ""
	for i, k := range kinds {
		switch k {
		case "OK":
			ok++
			winner = details[i]
		case "EXISTS":
			exists++
		default:
			t.Errorf("proc %d got non-table-exists failure kind=%s detail=%s", i, k, details[i])
		}
	}
	t.Logf("[create-race-xproc] ok=%d table-exists=%d winner-tag=%s", ok, exists, winner)
	if ok != 1 || exists != procs-1 {
		t.Fatalf("want exactly 1 success and %d ErrTableExists, got ok=%d exists=%d", procs-1, ok, exists)
	}

	// 最终结构自洽且就是胜者那一份。
	tbl, err := OpenTable(NewStore(dir), "contested")
	if err != nil {
		t.Fatal(err)
	}
	schema, err := tbl.SchemaAt("")
	if err != nil {
		t.Fatal(err)
	}
	if len(schema.Columns) != 2 || schema.Columns[1].Name != "mark_"+winner {
		t.Fatalf("final schema is not the winner's (tag=%s): %+v", winner, schema.Columns)
	}
	if rows := mustRead(t, tbl, ""); len(rows) != 0 {
		t.Fatalf("initial data not empty after create race: %+v", rows)
	}
	head, _ := tbl.Head()
	t.Logf("[create-race-xproc] final head snapshot=%s schema columns=[id mark_%s], data consistent",
		head, winner)
}
