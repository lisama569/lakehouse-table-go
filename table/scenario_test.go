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

// TestHelperCommitProcess 不是真正的测试，而是跨进程并发提交的子进程入口：
// 父测试以 GO_WANT_HELPER_PROCESS=1 启动本测试二进制，子进程独立打开同一张表，
// 基于指定父快照提交一批变更，并把结果分类打印到 stdout（RESULT|... 行）。
func TestHelperCommitProcess(t *testing.T) {
	if os.Getenv("GO_WANT_HELPER_PROCESS") != "1" {
		return
	}
	// os.Args: <binary> -test.run=... -- <dir> <table> <head> <batchID> <rowID>
	sep := 0
	for i, a := range os.Args {
		if a == "--" {
			sep = i
			break
		}
	}
	args := os.Args[sep+1:]
	dir, name, head, batchID, rowID := args[0], args[1], args[2], args[3], args[4]

	tbl, err := OpenTable(NewStore(dir), name)
	if err != nil {
		fmt.Printf("RESULT|OTHER|open: %v\n", err)
		return
	}
	b := Batch{BatchID: batchID, Changes: []Change{{Upsert: &Row{
		"id": rowID, "name": "name-" + rowID, "score": int64(1),
	}}}}
	snapID, err := tbl.Commit(head, b)
	switch {
	case err == nil:
		fmt.Printf("RESULT|OK|%s\n", snapID)
	case errors.Is(err, ErrCommitConflict):
		fmt.Printf("RESULT|CONFLICT|%v\n", err)
	case errors.Is(err, ErrDuplicateBatch):
		fmt.Printf("RESULT|DUPLICATE|%v\n", err)
	case errors.Is(err, ErrSchemaIncompatible), errors.Is(err, ErrInvalidChange):
		fmt.Printf("RESULT|WRONGKIND|%v\n", err)
	default:
		fmt.Printf("RESULT|OTHER|%v\n", err)
	}
}

// runHelperCommit 启动一个子进程提交，返回 RESULT 行的分类与详情。
func runHelperCommit(t *testing.T, dir, table, head, batchID, rowID string) (string, string) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestHelperCommitProcess$", "--",
		dir, table, head, batchID, rowID)
	cmd.Env = append(os.Environ(), "GO_WANT_HELPER_PROCESS=1")
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("helper process for %s: %v\n%s", batchID, err, out)
	}
	for _, line := range strings.Split(string(out), "\n") {
		if strings.HasPrefix(line, "RESULT|") {
			parts := strings.SplitN(line, "|", 3)
			return parts[1], parts[2]
		}
	}
	t.Fatalf("helper process %s printed no RESULT line:\n%s", batchID, out)
	return "", ""
}

// TestCrossProcessConcurrentCommit 场景一（跨进程）：多个独立进程同时打开同一张表、
// 基于同一父快照提交，恰好一个成功，其余被可区分地拒绝（ErrCommitConflict，
// 而非结构/变更类错误）；被拒绝方基于最新 HEAD 重试后数据完整、无重复、不覆盖胜者。
func TestCrossProcessConcurrentCommit(t *testing.T) {
	dir := t.TempDir()
	tbl, err := CreateTable(NewStore(dir), "users", testSchema())
	if err != nil {
		t.Fatal(err)
	}
	head, err := tbl.Head()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("[cross-process] table created, initial head snapshot=%s", head)

	const procs = 6
	kinds := make([]string, procs)
	details := make([]string, procs)
	var wg sync.WaitGroup
	for i := 0; i < procs; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			kind, detail := runHelperCommit(t, dir, "users", head,
				fmt.Sprintf("pbatch-%d", i), fmt.Sprintf("p%d", i))
			kinds[i], details[i] = kind, detail
			t.Logf("[cross-process] proc=%d batch=pbatch-%d result=%s %s", i, i, kind, detail)
		}(i)
	}
	wg.Wait()

	var ok, conflict int
	for i, k := range kinds {
		switch k {
		case "OK":
			ok++
		case "CONFLICT":
			conflict++
		default:
			t.Errorf("proc %d got non-conflict failure kind=%s detail=%s", i, k, details[i])
		}
	}
	t.Logf("[cross-process] first round: ok=%d conflict=%d (expect 1/%d)", ok, conflict, procs-1)
	if ok != 1 || conflict != procs-1 {
		t.Fatalf("want exactly 1 success and %d conflicts, got ok=%d conflict=%d", procs-1, ok, conflict)
	}

	// 被拒绝的进程基于最新 HEAD 重试（数据不变，仍是各自那一行）。
	for i, k := range kinds {
		if k != "CONFLICT" {
			continue
		}
		cur, err := tbl.Head()
		if err != nil {
			t.Fatal(err)
		}
		kind, detail := runHelperCommit(t, dir, "users", cur,
			fmt.Sprintf("pbatch-%d", i), fmt.Sprintf("p%d", i))
		t.Logf("[cross-process] retry proc=%d batch=pbatch-%d result=%s %s", i, i, kind, detail)
		if kind != "OK" {
			t.Fatalf("retry proc %d: want OK, got %s %s", i, kind, detail)
		}
	}

	// 最终数据：每个进程各一行、无重复、无丢失，先成功者未被覆盖。
	rows, err := tbl.Read("")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != procs {
		t.Fatalf("want %d rows, got %d: %+v", procs, len(rows), rows)
	}
	seen := map[string]int{}
	for _, r := range rows {
		id, _ := r["id"].(string)
		seen[id]++
		if r["name"] != "name-"+id {
			t.Errorf("row %s overwritten or corrupted: %+v", id, r)
		}
	}
	for i := 0; i < procs; i++ {
		id := fmt.Sprintf("p%d", i)
		if seen[id] != 1 {
			t.Errorf("row %s appears %d times (want exactly 1)", id, seen[id])
		}
	}
	finalHead, _ := tbl.Head()
	t.Logf("[cross-process] final head snapshot=%s rows=%d, all rows unique and intact", finalHead, len(rows))
}

// TestMultiInstanceConcurrentCommit 场景一（多实例）：同一进程内模拟多个服务实例
// （各自 OpenTable 得到独立句柄），并发基于同一父快照提交，验证互斥与可区分拒绝。
func TestMultiInstanceConcurrentCommit(t *testing.T) {
	dir := t.TempDir()
	store := NewStore(dir)
	if _, err := CreateTable(store, "users", testSchema()); err != nil {
		t.Fatal(err)
	}
	const instances = 8
	handles := make([]*Table, instances)
	for i := range handles {
		h, err := OpenTable(store, "users") // 每个实例独立打开同一张表
		if err != nil {
			t.Fatal(err)
		}
		handles[i] = h
	}
	head, err := handles[0].Head()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("[multi-instance] %d instances opened the same table, head snapshot=%s", instances, head)

	errs := make([]error, instances)
	var wg sync.WaitGroup
	for i := 0; i < instances; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			b := Batch{BatchID: fmt.Sprintf("ibatch-%d", i), Changes: []Change{{Upsert: &Row{
				"id": fmt.Sprintf("i%d", i), "name": fmt.Sprintf("inst%d", i), "score": int64(i),
			}}}}
			_, errs[i] = handles[i].Commit(head, b)
		}(i)
	}
	wg.Wait()

	var success, conflict int
	for i, err := range errs {
		switch {
		case err == nil:
			success++
		case errors.Is(err, ErrCommitConflict):
			conflict++
			// 冲突错误必须能与其他失败原因区分开。
			if errors.Is(err, ErrSchemaIncompatible) || errors.Is(err, ErrInvalidChange) || errors.Is(err, ErrDuplicateBatch) {
				t.Errorf("instance %d: conflict error misclassified: %v", i, err)
			}
		default:
			t.Errorf("instance %d: unexpected error: %v", i, err)
		}
	}
	t.Logf("[multi-instance] first round: success=%d conflict=%d", success, conflict)
	if success != 1 || conflict != instances-1 {
		t.Fatalf("want 1 success + %d conflicts, got %d/%d", instances-1, success, conflict)
	}

	// 冲突实例基于最新 HEAD 重试，全部落库。
	for i, err := range errs {
		if !errors.Is(err, ErrCommitConflict) {
			continue
		}
		cur, _ := handles[i].Head()
		b := Batch{BatchID: fmt.Sprintf("ibatch-%d", i), Changes: []Change{{Upsert: &Row{
			"id": fmt.Sprintf("i%d", i), "name": fmt.Sprintf("inst%d", i), "score": int64(i),
		}}}}
		snapID, err := handles[i].Commit(cur, b)
		if err != nil {
			t.Fatalf("retry instance %d: %v", i, err)
		}
		logBatch(t, "[multi-instance] retry succeeded", snapID, b)
	}

	rows, err := handles[0].Read("")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != instances {
		t.Fatalf("want %d rows after retries, got %d", instances, len(rows))
	}
	seen := map[string]int{}
	for _, r := range rows {
		id, _ := r["id"].(string)
		seen[id]++
	}
	for i := 0; i < instances; i++ {
		if id := fmt.Sprintf("i%d", i); seen[id] != 1 {
			t.Errorf("row %s appears %d times", id, seen[id])
		}
	}
	t.Logf("[multi-instance] final rows=%d, no duplicates, no lost updates", len(rows))
}

// TestRenameColumnMigration 场景二：带数据的列改名迁移。
// 迁移后新列名下能读到改名前的历史值，同行其他列不受影响，按主键对得上；
// 旧快照仍按当时结构与值读取、结果可复现；不兼容改动被明确拒绝。
func TestRenameColumnMigration(t *testing.T) {
	tbl := newTestTable(t)
	head, _ := tbl.Head()

	seed := Batch{BatchID: "seed", Changes: []Change{
		{Upsert: &Row{"id": "a", "name": "alice", "score": int64(10)}},
		{Upsert: &Row{"id": "b", "name": "bob", "score": int64(20)}},
	}}
	snapOld, err := tbl.Commit(head, seed)
	if err != nil {
		t.Fatal(err)
	}
	logBatch(t, "[rename] seed committed", snapOld, seed)
	rowsBefore, _ := tbl.Read(snapOld)
	sumBefore := checksum(t, rowsBefore)
	beforeByPK := map[string]Row{}
	for _, r := range rowsBefore {
		beforeByPK[r["id"].(string)] = r
	}

	// 带数据改名：name -> display_name。
	snapNew, err := tbl.RenameColumn("name", "display_name")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("[rename] column name->display_name migrated: old snapshot=%s new snapshot=%s", snapOld, snapNew)

	// 迁移后：新列名读到历史值，其他列不受影响，按主键一一对应。
	rowsAfter, err := tbl.Read("")
	if err != nil {
		t.Fatal(err)
	}
	if len(rowsAfter) != len(rowsBefore) {
		t.Fatalf("row count changed by rename: %d -> %d", len(rowsBefore), len(rowsAfter))
	}
	for _, r := range rowsAfter {
		pk := r["id"].(string)
		before := beforeByPK[pk]
		if before == nil {
			t.Fatalf("row %q appeared after rename", pk)
		}
		if r["display_name"] != before["name"] {
			t.Errorf("pk=%s renamed value mismatch: before name=%v after display_name=%v",
				pk, before["name"], r["display_name"])
		}
		if _, ok := r["name"]; ok {
			t.Errorf("pk=%s old column name still visible: %+v", pk, r)
		}
		if fmt.Sprint(r["score"]) != fmt.Sprint(before["score"]) {
			t.Errorf("pk=%s untouched column score changed: %v -> %v", pk, before["score"], r["score"])
		}
		t.Logf("[rename] pk=%s name=%v -> display_name=%v score=%v", pk, before["name"], r["display_name"], r["score"])
	}
	// 新结构：列位置与类型随列迁移。
	schemaNew, _ := tbl.SchemaAt(snapNew)
	if schemaNew.Columns[1].Name != "display_name" || schemaNew.Columns[1].Type != TypeString {
		t.Fatalf("renamed column should keep position and type: %+v", schemaNew.Columns)
	}

	// 旧快照读取不回归：结构与值保持当时状态，重复读取一致。
	rowsOld1, _ := tbl.Read(snapOld)
	rowsOld2, _ := tbl.Read(snapOld)
	if checksum(t, rowsOld1) != sumBefore || checksum(t, rowsOld2) != sumBefore {
		t.Fatal("old snapshot not reproducible after rename")
	}
	schemaOld, _ := tbl.SchemaAt(snapOld)
	if schemaOld.Columns[1].Name != "name" {
		t.Fatalf("old snapshot schema changed: %+v", schemaOld.Columns)
	}
	t.Logf("[rename] old snapshot=%s still reads name column, checksum=%d stable", snapOld, sumBefore)

	// 改名后按新结构继续写入。
	headNow, _ := tbl.Head()
	post := Batch{BatchID: "post-rename", Changes: []Change{
		{Upsert: &Row{"id": "c", "display_name": "carol", "score": int64(30)}},
	}}
	snapPost, err := tbl.Commit(headNow, post)
	if err != nil {
		t.Fatal(err)
	}
	logBatch(t, "[rename] post-rename commit", snapPost, post)
	rows, _ := tbl.Read("")
	if len(rows) != 3 {
		t.Fatalf("want 3 rows after post-rename commit, got %d", len(rows))
	}

	// 不兼容改动被明确拒绝，且与并发冲突错误可区分。
	for _, tc := range []struct {
		desc     string
		from, to string
	}{
		{"rename primary key", "id", "pk"},
		{"rename to existing column", "display_name", "score"},
		{"rename missing column", "nope", "x"},
	} {
		if _, err := tbl.RenameColumn(tc.from, tc.to); !errors.Is(err, ErrSchemaIncompatible) {
			t.Errorf("%s: want ErrSchemaIncompatible, got %v", tc.desc, err)
		} else if errors.Is(err, ErrCommitConflict) {
			t.Errorf("%s: error misclassified as conflict: %v", tc.desc, err)
		} else {
			t.Logf("[rename] rejected %s: %v", tc.desc, err)
		}
	}
	// 改既有列类型仍被拒绝。
	badType := Schema{PrimaryKey: "id", Columns: []Column{
		{Name: "id", Type: TypeString},
		{Name: "display_name", Type: TypeInt},
		{Name: "score", Type: TypeInt},
	}}
	if _, err := tbl.EvolveSchema(badType); !errors.Is(err, ErrSchemaIncompatible) {
		t.Errorf("type change: want ErrSchemaIncompatible, got %v", err)
	}
}

// TestChangeFeed 场景三：增量读取给出快照推进信息的同时，
// 输出真实发生的行级变更（insert/update/delete + 主键 + 变更后整行），
// 顺序稳定、重复读取一致、断点续读只返回未消费部分。
func TestChangeFeed(t *testing.T) {
	tbl := newTestTable(t)
	head, _ := tbl.Head()

	// snap1：新增 a、b。
	b1 := Batch{BatchID: "feed-1", Changes: []Change{
		{Upsert: &Row{"id": "a", "name": "alice", "score": int64(10)}},
		{Upsert: &Row{"id": "b", "name": "bob", "score": int64(20)}},
	}}
	snap1, err := tbl.Commit(head, b1)
	if err != nil {
		t.Fatal(err)
	}
	logBatch(t, "[change-feed] commit inserts", snap1, b1)

	// snap2：更新 a、删除 b、新增 c；删除不存在的行不产生变更记录。
	b2 := Batch{BatchID: "feed-2", Changes: []Change{
		{Upsert: &Row{"id": "a", "name": "alice2", "score": int64(11)}},
		{Delete: ptr(Value("b"))},
		{Delete: ptr(Value("ghost"))}, // 不存在，应被忽略
		{Upsert: &Row{"id": "c", "name": "carol", "score": int64(30)}},
	}}
	snap2, err := tbl.Commit(snap1, b2)
	if err != nil {
		t.Fatal(err)
	}
	logBatch(t, "[change-feed] commit update/delete/insert", snap2, b2)

	// snap3：结构演进，不产生行级变更记录。
	evoSchema := Schema{PrimaryKey: "id", Columns: []Column{
		{Name: "id", Type: TypeString},
		{Name: "name", Type: TypeString},
		{Name: "score", Type: TypeInt},
		{Name: "active", Type: TypeBool, Default: true},
	}}
	snap3, err := tbl.EvolveSchema(evoSchema)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("[change-feed] schema evolved snapshot=%s (no row-level changes expected)", snap3)

	// 快照推进信息（ReadSince）与行级明细（ReadChangesSince）同时可得。
	flow, err := tbl.ReadSince(0)
	if err != nil {
		t.Fatal(err)
	}
	if len(flow) != 3 || flow[0].ID != snap1 || flow[1].ID != snap2 || flow[2].ID != snap3 {
		t.Fatalf("snapshot flow wrong: %+v", flow)
	}
	for _, s := range flow {
		t.Logf("[change-feed] flow snapshot=%s seq=%d parent=%s batch=%q", s.ID, s.Seq, s.ParentID, s.BatchID)
	}

	recs, err := tbl.ReadChangesSince(0)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range recs {
		t.Logf("[change-feed] record seq=%d snapshot=%s op=%s key=%v row=%v",
			r.Seq, r.SnapshotID, r.Op, r.Key, r.Row)
	}

	// 内容完整：2 insert + (1 update + 1 delete + 1 insert)，演进快照无记录。
	want := []struct {
		op  ChangeOp
		key string
	}{
		{OpInsert, "a"}, {OpInsert, "b"},
		{OpUpdate, "a"}, {OpDelete, "b"}, {OpInsert, "c"},
	}
	if len(recs) != len(want) {
		t.Fatalf("want %d change records, got %d: %+v", len(want), len(recs), recs)
	}
	for i, w := range want {
		r := recs[i]
		if r.Op != w.op || r.Key != w.key {
			t.Errorf("record %d: want %s %s, got %s %v", i, w.op, w.key, r.Op, r.Key)
		}
		wantSnap := snap1
		if i >= 2 {
			wantSnap = snap2
		}
		if r.SnapshotID != wantSnap {
			t.Errorf("record %d: want snapshot %s, got %s", i, wantSnap, r.SnapshotID)
		}
	}
	// 变更后的整行随记录给出；删除记录只带主键作删除标记。
	if recs[2].Row["name"] != "alice2" {
		t.Errorf("update record should carry new row values: %+v", recs[2].Row)
	}
	if recs[3].Row != nil {
		t.Errorf("delete record should be a tombstone (key only): %+v", recs[3])
	}
	if recs[4].Row["name"] != "carol" {
		t.Errorf("insert record should carry row values: %+v", recs[4].Row)
	}

	// 顺序稳定且单调：Seq 非递减，批内顺序与提交顺序一致。
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
	t.Logf("[change-feed] repeated read identical: %d records", len(recs2))

	// 断点续读：从 snap1 之后续读，只返回未消费的部分。
	snap1Seq := flow[0].Seq
	resumed, err := tbl.ReadChangesSince(snap1Seq)
	if err != nil {
		t.Fatal(err)
	}
	if len(resumed) != 3 || resumed[0].Op != OpUpdate || resumed[2].Op != OpInsert {
		t.Fatalf("resume from seq=%d wrong: %+v", snap1Seq, resumed)
	}
	for _, r := range resumed {
		if r.Seq <= snap1Seq {
			t.Errorf("resumed record already consumed: %+v", r)
		}
	}
	t.Logf("[change-feed] resume from seq=%d returned %d unconsumed records", snap1Seq, len(resumed))

	// 续读游标推进到最新后，没有新增提交时结果为空。
	latest, err := tbl.ReadChangesSince(flow[len(flow)-1].Seq)
	if err != nil {
		t.Fatal(err)
	}
	if len(latest) != 0 {
		t.Fatalf("want no records after latest seq, got %+v", latest)
	}
	t.Log("[change-feed] cursor at latest seq yields empty result; feed is stable and resumable")
}
