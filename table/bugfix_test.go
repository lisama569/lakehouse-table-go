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

// TestChangeFeedOnlyRealChanges 增量明细只记录真实发生的行级变更：
//   - 原样重复提交（列值一个都没改）不产生任何记录；
//   - 只改一列的提交正常报成 update，insert/delete 照旧；
//   - 删除不存在的行不留痕迹；
//   - 同一批次同一主键被写多次时按发生顺序逐条判定；
//   - 重复读取一致，断点续读只返回未消费部分。
func TestChangeFeedOnlyRealChanges(t *testing.T) {
	tbl := newTestTable(t)
	head, _ := tbl.Head()

	// snap1：新增 a、b。
	b1 := Batch{BatchID: "real-1", Changes: []Change{
		{Upsert: &Row{"id": "a", "name": "alice", "score": int64(10)}},
		{Upsert: &Row{"id": "b", "name": "bob", "score": int64(20)}},
	}}
	snap1, err := tbl.Commit(head, b1)
	if err != nil {
		t.Fatal(err)
	}
	logBatch(t, "[real-change] inserts", snap1, b1)
	rows1, _ := tbl.Read("")
	sum1 := checksum(t, rows1)

	// snap2：把 a、b 按原样再提交一遍（score 用 int64，落盘后是 float64，
	// 数学值相同即不算变更）；同时删除一个不存在的主键。
	b2 := Batch{BatchID: "real-2-noop", Changes: []Change{
		{Upsert: &Row{"id": "a", "name": "alice", "score": int64(10)}},
		{Upsert: &Row{"id": "b", "name": "bob", "score": int64(20)}},
		{Delete: ptr(Value("ghost"))},
	}}
	snap2, err := tbl.Commit(snap1, b2)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("[real-change] no-op resubmit committed snapshot=%s, expecting zero row-level records", snap2)

	recs, err := tbl.ReadChangesSince(snap1Seq(t, tbl))
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 0 {
		t.Fatalf("unchanged resubmit must not produce records, got %d: %+v", len(recs), recs)
	}
	rows2, _ := tbl.Read("")
	if checksum(t, rows2) != sum1 {
		t.Fatal("no-op resubmit changed table data")
	}
	t.Logf("[real-change] no-op snapshot=%s produced 0 records, data checksum stable=%d", snap2, sum1)

	// snap3：只改 a 的一列（score 10 -> 11），必须报成一条 update。
	b3 := Batch{BatchID: "real-3-update", Changes: []Change{
		{Upsert: &Row{"id": "a", "name": "alice", "score": int64(11)}},
	}}
	snap3, err := tbl.Commit(snap2, b3)
	if err != nil {
		t.Fatal(err)
	}
	recs3 := changesOf(t, tbl, snap3)
	if len(recs3) != 1 || recs3[0].Op != OpUpdate || recs3[0].Key != "a" {
		t.Fatalf("single-column change must yield 1 update a, got %+v", recs3)
	}
	if fmt.Sprint(recs3[0].Row["score"]) != "11" {
		t.Fatalf("update row should carry new value: %+v", recs3[0].Row)
	}
	t.Logf("[real-change] one-column change snapshot=%s -> %s key=%v row=%v",
		snap3, recs3[0].Op, recs3[0].Key, recs3[0].Row)

	// snap4：同一批次同一主键写多次 + 删后重插 + 删除不存在的行。
	b4 := Batch{BatchID: "real-4-multi", Changes: []Change{
		{Upsert: &Row{"id": "a", "name": "alice2", "score": int64(11)}}, // 真实 update
		{Upsert: &Row{"id": "a", "name": "alice2", "score": int64(11)}}, // 与上条效果相同：无记录
		{Delete: ptr(Value("b"))},                                    // 真实 delete
		{Upsert: &Row{"id": "b", "name": "bob", "score": int64(20)}}, // 同批重插：insert
		{Delete: ptr(Value("nope"))},                                 // 不存在：无痕迹
	}}
	snap4, err := tbl.Commit(snap3, b4)
	if err != nil {
		t.Fatal(err)
	}
	recs4 := changesOf(t, tbl, snap4)
	want4 := []struct {
		op  ChangeOp
		key string
	}{
		{OpUpdate, "a"}, {OpDelete, "b"}, {OpInsert, "b"},
	}
	if len(recs4) != len(want4) {
		t.Fatalf("same-batch multi-write: want %d records, got %d: %+v", len(want4), len(recs4), recs4)
	}
	for i, w := range want4 {
		t.Logf("[real-change] multi-write record[%d] snapshot=%s op=%s key=%v row=%v",
			i, recs4[i].SnapshotID, recs4[i].Op, recs4[i].Key, recs4[i].Row)
		if recs4[i].Op != w.op || recs4[i].Key != w.key {
			t.Fatalf("record[%d] want %s %s, got %s %v", i, w.op, w.key, recs4[i].Op, recs4[i].Key)
		}
	}

	// 全量流水：只反映真实变更，顺序稳定。
	all, err := tbl.ReadChangesSince(0)
	if err != nil {
		t.Fatal(err)
	}
	wantAll := []struct {
		op  ChangeOp
		key string
	}{
		{OpInsert, "a"}, {OpInsert, "b"},
		{OpUpdate, "a"},
		{OpUpdate, "a"}, {OpDelete, "b"}, {OpInsert, "b"},
	}
	if len(all) != len(wantAll) {
		t.Fatalf("full feed want %d records, got %d: %+v", len(wantAll), len(all), all)
	}
	for i, w := range wantAll {
		t.Logf("[real-change] feed[%d] seq=%d snapshot=%s op=%s key=%v",
			i, all[i].Seq, all[i].SnapshotID, all[i].Op, all[i].Key)
		if all[i].Op != w.op || all[i].Key != w.key {
			t.Fatalf("feed[%d] want %s %s, got %s %v", i, w.op, w.key, all[i].Op, all[i].Key)
		}
	}

	// 同一次读取反复执行结果一致。
	all2, _ := tbl.ReadChangesSince(0)
	if fmt.Sprintf("%v", all) != fmt.Sprintf("%v", all2) {
		t.Fatal("repeated feed reads diverged")
	}

	// 断点续读：从 snap3 之后只拿到 snap4 的 3 条；游标到最新后为空。
	s3 := mustSnapshot(t, tbl, snap3)
	resumed, err := tbl.ReadChangesSince(s3.Seq)
	if err != nil {
		t.Fatal(err)
	}
	if len(resumed) != 3 {
		t.Fatalf("resume want 3 unconsumed records, got %d: %+v", len(resumed), resumed)
	}
	for _, r := range resumed {
		if r.Seq <= s3.Seq {
			t.Fatalf("resumed record already consumed: %+v", r)
		}
	}
	latest, _ := tbl.ReadChangesSince(mustSnapshot(t, tbl, snap4).Seq)
	if len(latest) != 0 {
		t.Fatalf("feed after latest must be empty, got %+v", latest)
	}
	t.Log("[real-change] feed stable, no duplicates on resubmit/resume, no false updates")
}

// TestConcurrentCreateTable 多实例同时建同一张表：恰好一个成功，
// 其余全部收到可区分的 ErrTableExists；败者结构不得覆盖胜者，初始数据自洽。
func TestConcurrentCreateTable(t *testing.T) {
	dir := t.TempDir()
	const n = 10
	schemas := make([]Schema, n)
	for i := range schemas {
		// 每个实例给出不同结构（第二列各自不同），以便事后确认败者没有覆盖。
		schemas[i] = Schema{PrimaryKey: "id", Columns: []Column{
			{Name: "id", Type: TypeString},
			{Name: fmt.Sprintf("owner_%d", i), Type: TypeString},
		}}
	}

	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, errs[i] = CreateTable(NewStore(dir), "svc", schemas[i])
		}(i)
	}
	wg.Wait()

	winner := -1
	var ok, exists int
	for i, err := range errs {
		switch {
		case err == nil:
			ok++
			winner = i
		case errors.Is(err, ErrTableExists):
			exists++
			// “表已存在”必须能与其他失败原因区分，调用方才知道不必改数据、不必重试冲突。
			if errors.Is(err, ErrCommitConflict) || errors.Is(err, ErrSchemaIncompatible) ||
				errors.Is(err, ErrInvalidChange) || errors.Is(err, ErrDuplicateBatch) {
				t.Errorf("creator %d: ErrTableExists misclassified: %v", i, err)
			}
		default:
			t.Errorf("creator %d: unexpected error: %v", i, err)
		}
	}
	t.Logf("[create-race] in-process creators: success=%d table-exists=%d (expect 1/%d)", ok, exists, n-1)
	if ok != 1 || exists != n-1 || winner < 0 {
		t.Fatalf("want exactly 1 winner and %d ErrTableExists, got ok=%d exists=%d", n-1, ok, exists)
	}

	// 最终生效的结构必须是胜者那份，败者没有顶掉它。
	tbl, err := OpenTable(NewStore(dir), "svc")
	if err != nil {
		t.Fatal(err)
	}
	head, _ := tbl.Head()
	got, err := tbl.SchemaAt("")
	if err != nil {
		t.Fatal(err)
	}
	wantCol := fmt.Sprintf("owner_%d", winner)
	if len(got.Columns) != 2 || got.Columns[1].Name != wantCol {
		t.Fatalf("winner schema overwritten: want column %s, got %+v", wantCol, got.Columns)
	}
	rows, err := tbl.Read("")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 0 {
		t.Fatalf("new table must start empty, got %+v", rows)
	}
	snaps, err := tbl.store.listSnapshots("svc")
	if err != nil {
		t.Fatal(err)
	}
	if len(snaps) != 1 || snaps[0].ID != head || head != "snap-000000" {
		t.Fatalf("table should have exactly the initial snapshot, head=%s snaps=%+v", head, snaps)
	}
	t.Logf("[create-race] winner=%d schema column=%q head=%s initial rows=0, structure self-consistent",
		winner, wantCol, head)

	// 顺序再来一次（表已建好）：同样被可区分地拒绝。
	if _, err := CreateTable(NewStore(dir), "svc", schemas[winner]); !errors.Is(err, ErrTableExists) {
		t.Fatalf("sequential recreate want ErrTableExists, got %v", err)
	}
	// 败者之后正常 OpenTable 拿到的仍是胜者的表。
	if _, err := OpenTable(NewStore(dir), "svc"); err != nil {
		t.Fatalf("losers can open the existing table afterwards: %v", err)
	}
	t.Log("[create-race] sequential recreate rejected with ErrTableExists; open-after-reject works")
}

// --- 跨进程并发建表 ---

// TestHelperCreateProcess 是跨进程建表的子进程入口（由父测试以
// GO_WANT_HELPER_PROCESS=1 启动）：独立创建同名表，打印 RESULT|... 分类。
func TestHelperCreateProcess(t *testing.T) {
	if os.Getenv("GO_WANT_HELPER_PROCESS") != "1" {
		return
	}
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
		{Name: "owner", Type: TypeString, Default: tag},
	}}
	tbl, err := CreateTable(NewStore(dir), name, schema)
	switch {
	case err == nil:
		head, _ := tbl.Head()
		fmt.Printf("RESULT|OK|%s\n", head)
	case errors.Is(err, ErrTableExists):
		fmt.Printf("RESULT|EXISTS|%v\n", err)
	case errors.Is(err, ErrCommitConflict):
		fmt.Printf("RESULT|CONFLICT|%v\n", err)
	default:
		fmt.Printf("RESULT|OTHER|%v\n", err)
	}
}

func runHelperCreate(t *testing.T, dir, table, tag string) (string, string) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestHelperCreateProcess$", "--", dir, table, tag)
	cmd.Env = append(os.Environ(), "GO_WANT_HELPER_PROCESS=1")
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("helper create %s: %v\n%s", tag, err, out)
	}
	for _, line := range strings.Split(string(out), "\n") {
		if strings.HasPrefix(line, "RESULT|") {
			parts := strings.SplitN(line, "|", 3)
			return parts[1], parts[2]
		}
	}
	t.Fatalf("helper create %s printed no RESULT line:\n%s", tag, out)
	return "", ""
}

// TestCrossProcessConcurrentCreate 多个独立进程同时建同一张表：
// 恰好一个成功，其余收到可区分的 ErrTableExists；
// 最终结构与初始数据只属于胜者，败者未做任何改写。
func TestCrossProcessConcurrentCreate(t *testing.T) {
	dir := t.TempDir()
	const procs = 6
	kinds := make([]string, procs)
	tags := make([]string, procs)
	var wg sync.WaitGroup
	for i := 0; i < procs; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			tags[i] = fmt.Sprintf("proc-%d", i)
			k, detail := runHelperCreate(t, dir, "svc", tags[i])
			kinds[i] = k
			t.Logf("[create-race-proc] proc=%d result=%s %s", i, k, detail)
		}(i)
	}
	wg.Wait()

	var ok, exists int
	for _, k := range kinds {
		switch k {
		case "OK":
			ok++
		case "EXISTS":
			exists++
		default:
			t.Errorf("unexpected create result kind=%s", k)
		}
	}
	t.Logf("[create-race-proc] first round: ok=%d exists=%d (expect 1/%d)", ok, exists, procs-1)
	if ok != 1 || exists != procs-1 {
		t.Fatalf("want 1 OK + %d EXISTS, got ok=%d exists=%d", procs-1, ok, exists)
	}

	// 最终结构只有一份且自洽：HEAD 为初始快照、表为空、owner 缺省值标识出胜者进程。
	tbl, err := OpenTable(NewStore(dir), "svc")
	if err != nil {
		t.Fatal(err)
	}
	head, _ := tbl.Head()
	if head != "snap-000000" {
		t.Fatalf("winner's initial head overwritten: %s", head)
	}
	schema, _ := tbl.SchemaAt("")
	winnerTag := ""
	for _, c := range schema.Columns {
		if c.Name == "owner" {
			winnerTag, _ = c.Default.(string)
		}
	}
	if winnerTag == "" {
		t.Fatalf("winner schema missing owner default marker: %+v", schema.Columns)
	}
	rows, _ := tbl.Read("")
	if len(rows) != 0 {
		t.Fatalf("initial data must be empty, got %+v", rows)
	}
	snaps, err := tbl.store.listSnapshots("svc")
	if err != nil {
		t.Fatal(err)
	}
	if len(snaps) != 1 {
		t.Fatalf("exactly one snapshot expected, got %+v", snaps)
	}
	t.Logf("[create-race-proc] final head=%s snapshots=%d rows=0 winner marker owner.default=%q, structure/data consistent",
		head, len(snaps), winnerTag)

	// 所有败者进程此刻重试建表仍被可区分拒绝（且不会改写）。
	for _, tag := range tags {
		if tag == winnerTag {
			continue
		}
		if k, _ := runHelperCreate(t, dir, "svc", tag); k != "EXISTS" {
			t.Fatalf("repeated create by %s: want EXISTS, got %s", tag, k)
		}
	}
	schema2, _ := tbl.SchemaAt("")
	for _, c := range schema2.Columns {
		if c.Name == "owner" && c.Default != winnerTag {
			t.Fatalf("rejected creator overwritten structure: %q -> %q", winnerTag, c.Default)
		}
	}
	t.Log("[create-race-proc] retried creates still rejected; winner structure intact")
}

func snap1Seq(t *testing.T, tbl *Table) uint64 {
	t.Helper()
	return mustSnapshot(t, tbl, "snap-000001").Seq
}

func mustSnapshot(t *testing.T, tbl *Table, id string) *Snapshot {
	t.Helper()
	s, err := tbl.store.readSnapshot(tbl.name, id)
	if err != nil {
		t.Fatalf("read snapshot %s: %v", id, err)
	}
	return s
}

func changesOf(t *testing.T, tbl *Table, snapID string) []ChangeRecord {
	t.Helper()
	recs, err := tbl.store.readChanges(tbl.name, snapID)
	if err != nil {
		t.Fatal(err)
	}
	return recs
}
