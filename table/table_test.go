package table

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"sort"
	"sync"
	"testing"
)

func testSchema() Schema {
	return Schema{
		PrimaryKey: "id",
		Columns: []Column{
			{Name: "id", Type: TypeString},
			{Name: "name", Type: TypeString},
			{Name: "score", Type: TypeInt},
		},
	}
}

func newTestTable(t *testing.T) *Table {
	t.Helper()
	store := NewStore(t.TempDir())
	tbl, err := CreateTable(store, "users", testSchema())
	if err != nil {
		t.Fatalf("create table: %v", err)
	}
	return tbl
}

// checksum 对行集合计算确定性校验和，用于验证幂等重放后数据不变。
func checksum(t *testing.T, rows []Row) uint64 {
	t.Helper()
	h := fnv.New64a()
	for _, r := range rows {
		keys := make([]string, 0, len(r))
		for k := range r {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			b, err := json.Marshal(r[k])
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			fmt.Fprintf(h, "%s=%s;", k, b)
		}
		h.Write([]byte("\n"))
	}
	return h.Sum64()
}

func logBatch(t *testing.T, msg string, snapID string, b Batch) {
	t.Helper()
	t.Logf("snapshot=%s batch=%s changes=%d %s", snapID, b.BatchID, len(b.Changes), msg)
}

// TestConcurrentCommitConflict 并发提交：只有一个成功，其余收到可区分的冲突错误。
func TestConcurrentCommitConflict(t *testing.T) {
	tbl := newTestTable(t)
	head, err := tbl.Head()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("initial head snapshot=%s", head)

	const writers = 8
	var wg sync.WaitGroup
	errs := make([]error, writers)
	snaps := make([]string, writers)
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			b := Batch{
				BatchID: fmt.Sprintf("batch-w%d", i),
				Changes: []Change{{Upsert: &Row{
					"id": fmt.Sprintf("u%d", i), "name": fmt.Sprintf("user%d", i), "score": int64(i),
				}}},
			}
			snapID, err := tbl.Commit(head, b) // 所有写入者基于同一父快照
			errs[i] = err
			snaps[i] = snapID
			logBatch(t, "commit attempt", snapID, b)
		}(i)
	}
	wg.Wait()

	var success, conflicts, others int
	for _, err := range errs {
		switch {
		case err == nil:
			success++
		case errors.Is(err, ErrCommitConflict):
			conflicts++
		default:
			others++
			t.Errorf("unexpected error type: %v", err)
		}
	}
	t.Logf("results: success=%d conflict=%d other=%d", success, conflicts, others)
	if success != 1 {
		t.Fatalf("want exactly 1 success, got %d", success)
	}
	if conflicts != writers-1 {
		t.Fatalf("want %d conflicts, got %d", writers-1, conflicts)
	}

	// 读取只能看到完整快照：恰好 1 行。
	rows, err := tbl.Read("")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("want 1 row after concurrent commits, got %d", len(rows))
	}
	newHead, _ := tbl.Head()
	t.Logf("final head snapshot=%s rows=%d", newHead, len(rows))

	// 冲突的写入者用新 HEAD 重试，应成功。
	for i, err := range errs {
		if errors.Is(err, ErrCommitConflict) {
			cur, _ := tbl.Head()
			b := Batch{
				BatchID: fmt.Sprintf("batch-w%d", i),
				Changes: []Change{{Upsert: &Row{
					"id": fmt.Sprintf("u%d", i), "name": fmt.Sprintf("user%d", i), "score": int64(i),
				}}},
			}
			snapID, err := tbl.Commit(cur, b)
			if err != nil {
				t.Fatalf("retry writer %d: %v", i, err)
			}
			logBatch(t, "retry succeeded", snapID, b)
		}
	}
	rows, _ = tbl.Read("")
	if len(rows) != writers {
		t.Fatalf("want %d rows after retries, got %d", writers, len(rows))
	}
}

// TestIdempotentReplay 幂等重放：重复提交同一 BatchID 返回 ErrDuplicateBatch，
// 行数与校验和保持不变。
func TestIdempotentReplay(t *testing.T) {
	tbl := newTestTable(t)
	head, _ := tbl.Head()

	b := Batch{BatchID: "batch-1", Changes: []Change{
		{Upsert: &Row{"id": "a", "name": "alice", "score": int64(10)}},
		{Upsert: &Row{"id": "b", "name": "bob", "score": int64(20)}},
	}}
	snap1, err := tbl.Commit(head, b)
	if err != nil {
		t.Fatal(err)
	}
	logBatch(t, "first commit", snap1, b)

	rows1, _ := tbl.Read("")
	sum1 := checksum(t, rows1)
	t.Logf("snapshot=%s rows=%d checksum=%d", snap1, len(rows1), sum1)

	// 重放同一批次（基于同一父快照）：应报重复批次，数据不变。
	_, err = tbl.Commit(head, b)
	if !errors.Is(err, ErrDuplicateBatch) {
		t.Fatalf("want ErrDuplicateBatch, got %v", err)
	}
	// 重放同一批次（基于新 HEAD）：同样应报重复批次。
	head2, _ := tbl.Head()
	_, err = tbl.Commit(head2, b)
	if !errors.Is(err, ErrDuplicateBatch) {
		t.Fatalf("want ErrDuplicateBatch on new head, got %v", err)
	}
	rows2, _ := tbl.Read("")
	sum2 := checksum(t, rows2)
	t.Logf("after replay rows=%d checksum=%d", len(rows2), sum2)
	if len(rows1) != len(rows2) || sum1 != sum2 {
		t.Fatalf("replay changed data: rows %d->%d checksum %d->%d",
			len(rows1), len(rows2), sum1, sum2)
	}

	// upsert 合并：同主键覆盖，删除生效；再重放仍幂等。
	b2 := Batch{BatchID: "batch-2", Changes: []Change{
		{Upsert: &Row{"id": "a", "name": "alice2", "score": int64(11)}},
		{Delete: ptr(Value("b"))},
	}}
	snap2, err := tbl.Commit(head2, b2)
	if err != nil {
		t.Fatal(err)
	}
	logBatch(t, "merge commit", snap2, b2)
	rows3, _ := tbl.Read("")
	if len(rows3) != 1 {
		t.Fatalf("want 1 row after merge, got %d", len(rows3))
	}
	if rows3[0]["name"] != "alice2" || rows3[0]["score"] != int64(11) && rows3[0]["score"] != float64(11) {
		t.Fatalf("merge wrong: %+v", rows3[0])
	}
	sum3 := checksum(t, rows3)
	if _, err := tbl.Commit(snap2, b2); !errors.Is(err, ErrDuplicateBatch) {
		t.Fatalf("want ErrDuplicateBatch replaying batch-2, got %v", err)
	}
	rows4, _ := tbl.Read("")
	if checksum(t, rows4) != sum3 {
		t.Fatal("replay of batch-2 changed data")
	}
	t.Logf("snapshot=%s rows=%d checksum=%d (stable after replay)", snap2, len(rows4), sum3)
}

// TestSchemaEvolution 结构演进：新增列有缺省值，删除列不串列，旧快照可读且可复现。
func TestSchemaEvolution(t *testing.T) {
	tbl := newTestTable(t)
	head, _ := tbl.Head()

	b := Batch{BatchID: "seed", Changes: []Change{
		{Upsert: &Row{"id": "a", "name": "alice", "score": int64(10)}},
		{Upsert: &Row{"id": "b", "name": "bob", "score": int64(20)}},
	}}
	snapOld, err := tbl.Commit(head, b)
	if err != nil {
		t.Fatal(err)
	}
	logBatch(t, "seed", snapOld, b)
	rowsBefore, _ := tbl.Read(snapOld)
	sumBefore := checksum(t, rowsBefore)

	// 演进：删除 score，重命名 name->display_name，新增列 active（缺省 true）。
	newSchema := Schema{
		PrimaryKey: "id",
		Columns: []Column{
			{Name: "id", Type: TypeString},
			{Name: "display_name", Type: TypeString, Default: "unknown"},
			{Name: "active", Type: TypeBool, Default: true},
		},
	}
	snapNew, err := tbl.EvolveSchema(newSchema)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("schema evolved: old snapshot=%s new snapshot=%s", snapOld, snapNew)

	rows, err := tbl.Read("")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("want 2 rows after evolution, got %d", len(rows))
	}
	for _, r := range rows {
		// 新增列填缺省值
		if r["active"] != true {
			t.Fatalf("new column default missing: %+v", r)
		}
		// 重命名列按新列缺省（旧 name 列已删除，不串列）
		if r["display_name"] != "unknown" {
			t.Fatalf("renamed column should use default, got %+v", r)
		}
		// 删除的列不可见
		if _, ok := r["score"]; ok {
			t.Fatalf("dropped column still visible: %+v", r)
		}
		if _, ok := r["name"]; ok {
			t.Fatalf("renamed old column still visible: %+v", r)
		}
	}
	t.Logf("post-evolution rows=%+v", rows)

	// 旧快照仍可读且结果可复现。
	rowsOld1, _ := tbl.Read(snapOld)
	rowsOld2, _ := tbl.Read(snapOld)
	if checksum(t, rowsOld1) != sumBefore || checksum(t, rowsOld2) != sumBefore {
		t.Fatal("old snapshot not reproducible after evolution")
	}
	schemaOld, _ := tbl.SchemaAt(snapOld)
	if len(schemaOld.Columns) != 3 {
		t.Fatalf("old snapshot schema changed: %+v", schemaOld)
	}
	t.Logf("old snapshot=%s still readable rows=%d checksum=%d", snapOld, len(rowsOld1), sumBefore)

	// 演进后按新结构继续写入。
	headNow, _ := tbl.Head()
	b2 := Batch{BatchID: "post-evolve", Changes: []Change{
		{Upsert: &Row{"id": "c", "display_name": "carol", "active": false}},
	}}
	snap3, err := tbl.Commit(headNow, b2)
	if err != nil {
		t.Fatal(err)
	}
	logBatch(t, "post-evolution commit", snap3, b2)
	rows, _ = tbl.Read("")
	if len(rows) != 3 {
		t.Fatalf("want 3 rows, got %d", len(rows))
	}

	// 不兼容演进被拒绝：改主键 / 改列类型，错误可区分。
	badPK := newSchema
	badPK.PrimaryKey = "display_name"
	if _, err := tbl.EvolveSchema(badPK); !errors.Is(err, ErrSchemaIncompatible) {
		t.Fatalf("want ErrSchemaIncompatible for pk change, got %v", err)
	}
	badType := Schema{PrimaryKey: "id", Columns: []Column{
		{Name: "id", Type: TypeInt}, // 类型变更
	}}
	if _, err := tbl.EvolveSchema(badType); !errors.Is(err, ErrSchemaIncompatible) {
		t.Fatalf("want ErrSchemaIncompatible for type change, got %v", err)
	}
	t.Log("incompatible evolutions rejected with ErrSchemaIncompatible")
}

// TestReadSince 增量读取返回变更流水。
func TestReadSince(t *testing.T) {
	tbl := newTestTable(t)
	head, _ := tbl.Head()
	var lastSnap string
	for i := 0; i < 3; i++ {
		b := Batch{BatchID: fmt.Sprintf("b%d", i), Changes: []Change{
			{Upsert: &Row{"id": fmt.Sprintf("k%d", i), "name": "n", "score": int64(i)}},
		}}
		snap, err := tbl.Commit(head, b)
		if err != nil {
			t.Fatal(err)
		}
		logBatch(t, "commit", snap, b)
		head = snap
		lastSnap = snap
	}
	flow, err := tbl.ReadSince(0)
	if err != nil {
		t.Fatal(err)
	}
	if len(flow) != 3 {
		t.Fatalf("want 3 snapshots in flow, got %d", len(flow))
	}
	for _, s := range flow {
		t.Logf("flow snapshot=%s seq=%d parent=%s batch=%s", s.ID, s.Seq, s.ParentID, s.BatchID)
	}
	if flow[len(flow)-1].ID != lastSnap {
		t.Fatalf("flow tail %s != last commit %s", flow[len(flow)-1].ID, lastSnap)
	}
}

func ptr(v Value) *Value { return &v }
