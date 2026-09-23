package table

import (
	"errors"
	"fmt"
	"sync"
	"testing"
)

func newTestStore(t *testing.T) *Store {
	t.Helper()
	st, err := NewStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	return st
}

func mustCreate(t *testing.T, st *Store, name string, cols ...Column) *Table {
	t.Helper()
	tb, err := st.CreateTable(name, Schema{Columns: cols})
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	return tb
}

func logSnapshot(t *testing.T, prefix string, snap *Snapshot) {
	t.Helper()
	t.Logf("%s snapshot=%d parent=%d schemaV=%d batch=%q rows=%d checksum=%s changes=%d",
		prefix, snap.ID, snap.ParentID, snap.SchemaVersion, snap.BatchID, snap.RowCount, snap.Checksum, len(snap.Changes))
	for _, rec := range snap.Changes {
		t.Logf("    change: op=%s key=%s values=%v", rec.Op, rec.Key, rec.Values)
	}
}

// TestConcurrentCommitConflict 多个写入者基于同一父快照并发提交，
// 只允许一个成功，其余必须收到可区分的 ErrCommitConflict。
func TestConcurrentCommitConflict(t *testing.T) {
	st := newTestStore(t)
	tb := mustCreate(t, st, "events",
		Column{Name: "user", Type: TypeString},
		Column{Name: "score", Type: TypeInt64},
	)

	const writers = 8
	base, err := tb.Head()
	if err != nil {
		t.Fatalf("Head: %v", err)
	}

	var wg sync.WaitGroup
	results := make([]error, writers)
	snaps := make([]*Snapshot, writers)
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			snap, err := tb.Commit(base, Batch{
				ID: fmt.Sprintf("batch-w%d", i),
				Records: []Record{
					{Op: OpUpsert, Key: fmt.Sprintf("k%d", i), Values: map[int]any{1: fmt.Sprintf("u%d", i), 2: int64(i)}},
				},
			})
			snaps[i] = snap
			results[i] = err
		}(i)
	}
	wg.Wait()

	var succeeded, conflicted, other int
	for i, err := range results {
		switch {
		case err == nil:
			succeeded++
			logSnapshot(t, fmt.Sprintf("writer-%d committed:", i), snaps[i])
		case errors.Is(err, ErrCommitConflict):
			conflicted++
			t.Logf("writer-%d rejected: %v", i, err)
		default:
			other++
			t.Errorf("writer-%d unexpected error: %v", i, err)
		}
	}
	if succeeded != 1 || conflicted != writers-1 || other != 0 {
		t.Fatalf("succeeded=%d conflicted=%d other=%d, want 1/%d/0", succeeded, conflicted, other, writers-1)
	}

	// 失败的写入者刷新 base 后重试，必须全部成功且无重复行。
	for i, err := range results {
		if err == nil {
			continue
		}
		head, herr := tb.Head()
		if herr != nil {
			t.Fatalf("Head: %v", herr)
		}
		snap, rerr := tb.Commit(head, Batch{
			ID: fmt.Sprintf("batch-w%d", i),
			Records: []Record{
				{Op: OpUpsert, Key: fmt.Sprintf("k%d", i), Values: map[int]any{1: fmt.Sprintf("u%d", i), 2: int64(i)}},
			},
		})
		if rerr != nil {
			t.Fatalf("retry writer-%d: %v", i, rerr)
		}
		logSnapshot(t, fmt.Sprintf("writer-%d retried:", i), snap)
	}

	head, _ := tb.Head()
	rows, err := tb.Scan(head)
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if len(rows) != writers {
		t.Fatalf("final rows=%d, want %d", len(rows), writers)
	}
	snap, _ := tb.Snapshot(head)
	if snap.RowCount != writers {
		t.Fatalf("snapshot rowcount=%d, want %d", snap.RowCount, writers)
	}
	t.Logf("final head snapshot=%d rows=%d checksum=%s", snap.ID, snap.RowCount, snap.Checksum)
}

// TestSnapshotIsolationDuringCommit 提交进行中，读者只能看到完整快照。
func TestSnapshotIsolationDuringCommit(t *testing.T) {
	st := newTestStore(t)
	tb := mustCreate(t, st, "iso", Column{Name: "v", Type: TypeInt64})

	const commits = 20
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { // 读者：每次读到的快照元数据与数据行数必须一致
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			head, err := tb.Head()
			if err != nil {
				t.Errorf("Head: %v", err)
				return
			}
			if head == 0 {
				continue
			}
			snap, err := tb.Snapshot(head)
			if err != nil {
				t.Errorf("Snapshot(%d): %v", head, err)
				return
			}
			rows, err := tb.Scan(head)
			if err != nil {
				t.Errorf("Scan(%d): %v", head, err)
				return
			}
			if len(rows) != snap.RowCount {
				t.Errorf("partial state at snapshot=%d: meta rows=%d actual=%d", head, snap.RowCount, len(rows))
				return
			}
		}
	}()

	base, _ := tb.Head()
	for i := 0; i < commits; i++ {
		snap, err := tb.Commit(base, Batch{
			ID:      fmt.Sprintf("iso-%d", i),
			Records: []Record{{Op: OpUpsert, Key: fmt.Sprintf("k%d", i), Values: map[int]any{1: int64(i)}}},
		})
		if err != nil {
			t.Fatalf("Commit %d: %v", i, err)
		}
		base = snap.ID
	}
	close(stop)
	wg.Wait()
	t.Logf("committed %d snapshots, head=%d", commits, base)
}

// TestIdempotentReplay 同一批次重复提交：行数与校验和保持不变。
func TestIdempotentReplay(t *testing.T) {
	st := newTestStore(t)
	tb := mustCreate(t, st, "idem",
		Column{Name: "user", Type: TypeString},
		Column{Name: "score", Type: TypeInt64},
	)

	batch := Batch{
		ID: "batch-0001",
		Records: []Record{
			{Op: OpUpsert, Key: "alice", Values: map[int]any{1: "alice", 2: int64(10)}},
			{Op: OpUpsert, Key: "bob", Values: map[int]any{1: "bob", 2: int64(20)}},
			{Op: OpUpsert, Key: "alice", Values: map[int]any{1: "alice", 2: int64(11)}}, // 批内后者覆盖前者
		},
	}
	head, _ := tb.Head()
	snap1, err := tb.Commit(head, batch)
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}
	logSnapshot(t, "first apply:", snap1)
	if snap1.RowCount != 2 {
		t.Fatalf("rows=%d, want 2 (in-batch upsert merge)", snap1.RowCount)
	}

	// 模拟失败重试：同一批次原样重放多次（甚至带着过期 base）。
	for i := 0; i < 3; i++ {
		snap, err := tb.Commit(head, batch) // head 已过期，但批次已应用 → 幂等返回
		if err != nil {
			t.Fatalf("replay %d: %v", i, err)
		}
		logSnapshot(t, fmt.Sprintf("replay %d:", i), snap)
		if snap.ID != snap1.ID {
			t.Fatalf("replay %d produced new snapshot %d, want %d", i, snap.ID, snap1.ID)
		}
	}

	// 后续提交再重放旧批次，也不得改变数据。
	head2, _ := tb.Head()
	snap2, err := tb.Commit(head2, Batch{
		ID:      "batch-0002",
		Records: []Record{{Op: OpUpsert, Key: "carol", Values: map[int]any{1: "carol", 2: int64(30)}}},
	})
	if err != nil {
		t.Fatalf("Commit batch-0002: %v", err)
	}
	logSnapshot(t, "second batch:", snap2)
	if _, err := tb.Commit(snap2.ID, batch); err != nil {
		t.Fatalf("late replay: %v", err)
	}

	head, _ = tb.Head()
	final, err := tb.Snapshot(head)
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if final.ID != snap2.ID || final.RowCount != 3 || final.Checksum != snap2.Checksum {
		t.Fatalf("after replay: id=%d rows=%d checksum=%s, want id=%d rows=3 checksum=%s",
			final.ID, final.RowCount, final.Checksum, snap2.ID, snap2.Checksum)
	}
	rows, _ := tb.Scan(head)
	t.Logf("final head=%d rows=%d checksum=%s", final.ID, final.RowCount, final.Checksum)
	for _, r := range rows {
		t.Logf("    row: %v", r)
	}
	// 校验批内合并结果可复现：alice 的 score 为 11。
	if rows[0]["user"] != "alice" || rows[0]["score"] != int64(11) {
		t.Fatalf("unexpected merged row: %v", rows[0])
	}
}

// TestSchemaEvolution 增/删/重命名列后：新快照缺省语义确定，
// 旧快照仍可读且结果可复现，数据不错位。
func TestSchemaEvolution(t *testing.T) {
	st := newTestStore(t)
	tb := mustCreate(t, st, "users",
		Column{Name: "name", Type: TypeString},
		Column{Name: "age", Type: TypeInt64},
	)

	head, _ := tb.Head()
	snap1, err := tb.Commit(head, Batch{
		ID: "seed",
		Records: []Record{
			{Op: OpUpsert, Key: "alice", Values: map[int]any{1: "alice", 2: int64(30)}},
			{Op: OpUpsert, Key: "bob", Values: map[int]any{1: "bob", 2: int64(40)}},
		},
	})
	if err != nil {
		t.Fatalf("seed: %v", err)
	}
	logSnapshot(t, "seed:", snap1)

	// 1) 新增列 email。
	snap2, err := tb.EvolveSchema(snap1.ID, SchemaChange{
		ChangeID: "add-email",
		Add:      []Column{{Name: "email", Type: TypeString}},
	})
	if err != nil {
		t.Fatalf("add column: %v", err)
	}
	logSnapshot(t, "add column email:", snap2)
	if snap2.Checksum != snap1.Checksum {
		t.Fatalf("schema-only change altered data checksum: %s -> %s", snap1.Checksum, snap2.Checksum)
	}

	rows, err := tb.Scan(snap2.ID)
	if err != nil {
		t.Fatalf("Scan snap2: %v", err)
	}
	for _, r := range rows {
		t.Logf("    snap2 row: %v", r)
		if v, ok := r["email"]; !ok || v != nil {
			t.Fatalf("new column default must be nil, got %v (present=%v)", v, ok)
		}
	}

	// 2) 重命名 age -> years：列 ID 不变，数据不得串列。
	snap3, err := tb.EvolveSchema(snap2.ID, SchemaChange{
		ChangeID: "rename-age",
		Rename:   map[string]string{"age": "years"},
	})
	if err != nil {
		t.Fatalf("rename: %v", err)
	}
	logSnapshot(t, "rename age->years:", snap3)
	rows, _ = tb.Scan(snap3.ID)
	for _, r := range rows {
		t.Logf("    snap3 row: %v", r)
		if _, ok := r["age"]; ok {
			t.Fatalf("old column name still visible: %v", r)
		}
	}
	if rows[0]["years"] != int64(30) || rows[1]["years"] != int64(40) {
		t.Fatalf("rename misaligned data: %v", rows)
	}

	// 3) 删除列 name。
	snap4, err := tb.EvolveSchema(snap3.ID, SchemaChange{
		ChangeID: "drop-name",
		Drop:     []string{"name"},
	})
	if err != nil {
		t.Fatalf("drop: %v", err)
	}
	logSnapshot(t, "drop name:", snap4)
	rows, _ = tb.Scan(snap4.ID)
	for _, r := range rows {
		t.Logf("    snap4 row: %v", r)
		if _, ok := r["name"]; ok {
			t.Fatalf("dropped column still visible: %v", r)
		}
	}

	// 4) 旧快照仍可读且结果可复现：用当时的结构解读。
	oldRows, err := tb.Scan(snap1.ID)
	if err != nil {
		t.Fatalf("Scan old snapshot: %v", err)
	}
	for _, r := range oldRows {
		t.Logf("    snap1 row (re-read): %v", r)
	}
	if oldRows[0]["name"] != "alice" || oldRows[0]["age"] != int64(30) {
		t.Fatalf("old snapshot not reproducible: %v", oldRows[0])
	}
	if _, ok := oldRows[0]["email"]; ok {
		t.Fatalf("old snapshot must use its own schema, got email: %v", oldRows[0])
	}

	// 5) 演进后基于新结构写入：新列可写，重命名列按新名寻址。
	head, _ = tb.Head()
	schema, err := tb.SchemaAt(head)
	if err != nil {
		t.Fatalf("SchemaAt: %v", err)
	}
	var emailID, yearsID int
	for _, c := range schema.Columns {
		switch c.Name {
		case "email":
			emailID = c.ID
		case "years":
			yearsID = c.ID
		}
	}
	snap5, err := tb.Commit(head, Batch{
		ID: "post-evolution",
		Records: []Record{
			{Op: OpUpsert, Key: "alice", Values: map[int]any{emailID: "a@x.com", yearsID: int64(31)}},
		},
	})
	if err != nil {
		t.Fatalf("post-evolution commit: %v", err)
	}
	logSnapshot(t, "post-evolution upsert:", snap5)
	rows, _ = tb.Scan(snap5.ID)
	if rows[0]["email"] != "a@x.com" || rows[0]["years"] != int64(31) {
		t.Fatalf("post-evolution row wrong: %v", rows[0])
	}
	if rows[1]["email"] != nil || rows[1]["years"] != int64(40) {
		t.Fatalf("untouched row changed: %v", rows[1])
	}

	// 6) 不兼容演进必须报可区分错误。
	if _, err := tb.EvolveSchema(snap5.ID, SchemaChange{
		ChangeID: "bad-add",
		Add:      []Column{{Name: "email", Type: TypeString}},
	}); !errors.Is(err, ErrSchemaIncompatible) {
		t.Fatalf("add existing column: want ErrSchemaIncompatible, got %v", err)
	}
	if _, err := tb.EvolveSchema(snap5.ID, SchemaChange{
		ChangeID: "bad-drop",
		Drop:     []string{"nope"},
	}); !errors.Is(err, ErrSchemaIncompatible) {
		t.Fatalf("drop unknown column: want ErrSchemaIncompatible, got %v", err)
	}
}

// TestIncrementalRead 增量读取返回 (from, to] 区间的变更流水。
func TestIncrementalRead(t *testing.T) {
	st := newTestStore(t)
	tb := mustCreate(t, st, "inc", Column{Name: "v", Type: TypeInt64})

	base, _ := tb.Head()
	var snaps []*Snapshot
	for i := 0; i < 3; i++ {
		snap, err := tb.Commit(base, Batch{
			ID: fmt.Sprintf("inc-%d", i),
			Records: []Record{
				{Op: OpUpsert, Key: fmt.Sprintf("k%d", i), Values: map[int]any{1: int64(i)}},
			},
		})
		if err != nil {
			t.Fatalf("Commit %d: %v", i, err)
		}
		logSnapshot(t, fmt.Sprintf("commit %d:", i), snap)
		snaps = append(snaps, snap)
		base = snap.ID
	}

	all, err := tb.Changes(0, base)
	if err != nil {
		t.Fatalf("Changes: %v", err)
	}
	if len(all) != 3 {
		t.Fatalf("changes=%d, want 3", len(all))
	}
	for i, rec := range all {
		t.Logf("    change[%d]: op=%s key=%s values=%v", i, rec.Op, rec.Key, rec.Values)
		if rec.Key != fmt.Sprintf("k%d", i) {
			t.Fatalf("change[%d] key=%s out of order", i, rec.Key)
		}
	}

	partial, err := tb.Changes(snaps[0].ID, snaps[2].ID)
	if err != nil {
		t.Fatalf("Changes partial: %v", err)
	}
	if len(partial) != 2 || partial[0].Key != "k1" || partial[1].Key != "k2" {
		t.Fatalf("partial changes wrong: %v", partial)
	}
	t.Logf("incremental (%d, %d] yielded %d changes", snaps[0].ID, snaps[2].ID, len(partial))
}
