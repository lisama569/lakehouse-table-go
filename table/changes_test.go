package table

import (
	"errors"
	"fmt"
	"testing"
)

// TestDistinguishableRejection 验证提交被拒绝时原因可区分：
// ErrCommitConflict（父快照过期，可直接重试）与
// ErrInvalidChange / ErrSchemaIncompatible（数据或结构有问题，需回去改）互不混淆。
func TestDistinguishableRejection(t *testing.T) {
	tbl := newTestTable(t)
	head, _ := tbl.Head()

	// 先产生一个新快照，使 head 过期。
	s1, err := tbl.Commit(head, Batch{BatchID: "cls-1", Changes: []Change{
		{Upsert: &Row{"id": "a", "name": "alice", "score": int64(1)}},
	}})
	if err != nil {
		t.Fatal(err)
	}

	// 基于过期父快照：冲突，可重试。
	_, err = tbl.Commit(head, Batch{BatchID: "cls-2", Changes: []Change{
		{Upsert: &Row{"id": "b", "name": "bob", "score": int64(2)}},
	}})
	if !errors.Is(err, ErrCommitConflict) {
		t.Fatalf("stale parent must be ErrCommitConflict, got %v", err)
	}
	t.Logf("过期父快照 -> ErrCommitConflict（调用方可重试）: %v", err)

	// 基于最新父快照但数据非法：缺主键，不能靠重试解决。
	_, err = tbl.Commit(s1, Batch{BatchID: "cls-3", Changes: []Change{
		{Upsert: &Row{"name": "nobody", "score": int64(3)}},
	}})
	if !errors.Is(err, ErrInvalidChange) {
		t.Fatalf("missing pk must be ErrInvalidChange, got %v", err)
	}
	if errors.Is(err, ErrCommitConflict) {
		t.Fatal("invalid change must not be classified as conflict")
	}
	t.Logf("缺主键 -> ErrInvalidChange（调用方需改数据）: %v", err)

	// 基于最新父快照但类型不符：同样是非法变更。
	_, err = tbl.Commit(s1, Batch{BatchID: "cls-4", Changes: []Change{
		{Upsert: &Row{"id": "c", "name": true, "score": int64(4)}},
	}})
	if !errors.Is(err, ErrInvalidChange) {
		t.Fatalf("type mismatch must be ErrInvalidChange, got %v", err)
	}
	t.Logf("类型不符 -> ErrInvalidChange（调用方需改数据）: %v", err)
}

// TestChangeFeed 增量消费行级变更明细：
// 内容完整（insert/update/delete、主键、变更后值）、顺序稳定、
// 重复读取结果一致、断点续读只返回未消费部分。
func TestChangeFeed(t *testing.T) {
	tbl := newTestTable(t)
	head, _ := tbl.Head()
	t.Logf("场景: 增量变更明细，初始 snapshot=%s", head)

	// 批次1：两条 insert。
	b1 := Batch{BatchID: "feed-b1", Changes: []Change{
		{Upsert: &Row{"id": "a", "name": "alice", "score": int64(10)}},
		{Upsert: &Row{"id": "b", "name": "bob", "score": int64(20)}},
	}}
	s1, err := tbl.Commit(head, b1)
	if err != nil {
		t.Fatal(err)
	}
	logBatch(t, "两条新增", s1, b1)

	// 批次2：update a、delete b、insert c，顺序刻意交错。
	head = s1
	b2 := Batch{BatchID: "feed-b2", Changes: []Change{
		{Upsert: &Row{"id": "a", "name": "alice2", "score": int64(11)}},
		{Delete: ptr(Value("b"))},
		{Upsert: &Row{"id": "c", "name": "carol", "score": int64(30)}},
	}}
	s2, err := tbl.Commit(head, b2)
	if err != nil {
		t.Fatal(err)
	}
	logBatch(t, "更新/删除/新增", s2, b2)

	// 批次3：同值 upsert 不应产生事件；同主键先插后改产生两条事件。
	head = s2
	b3 := Batch{BatchID: "feed-b3", Changes: []Change{
		{Upsert: &Row{"id": "a", "name": "alice2", "score": int64(11)}}, // 无变化
		{Delete: ptr(Value("zzz"))},                                     // 删不存在的行
		{Upsert: &Row{"id": "d", "name": "dan", "score": int64(40)}},    // insert
		{Upsert: &Row{"id": "d", "name": "dan2", "score": int64(41)}},   // update
	}}
	s3, err := tbl.Commit(head, b3)
	if err != nil {
		t.Fatal(err)
	}
	logBatch(t, "无变化更新+同主键连续变更", s3, b3)

	// 全量增量读取（从初始快照之后）。
	feed, err := tbl.ReadChangesSince(0)
	if err != nil {
		t.Fatal(err)
	}
	if len(feed) != 3 {
		t.Fatalf("want 3 snapshot changes, got %d", len(feed))
	}

	// --- 快照1：2 条 insert，顺序即批次顺序 ---
	f1 := feed[0]
	if f1.Snapshot.ID != s1 {
		t.Fatalf("feed[0] snapshot=%s want %s", f1.Snapshot.ID, s1)
	}
	if len(f1.Changes) != 2 {
		t.Fatalf("snapshot1 want 2 changes, got %d", len(f1.Changes))
	}
	wantKinds1 := []RowChangeKind{RowInserted, RowInserted}
	wantPKs1 := []string{"a", "b"}
	for i, ch := range f1.Changes {
		t.Logf("流水 snapshot=%s order=%d kind=%s pk=%v row=%v",
			f1.Snapshot.ID, ch.Order, ch.Kind, ch.PK, ch.Row)
		if ch.Kind != wantKinds1[i] || ch.PK != wantPKs1[i] {
			t.Fatalf("snapshot1 change %d = %s/%v, want %s/%s",
				i, ch.Kind, ch.PK, wantKinds1[i], wantPKs1[i])
		}
		if ch.Row == nil || ch.Row["name"] == nil {
			t.Fatalf("insert change missing after-image row: %+v", ch)
		}
		if ch.Seq != f1.Snapshot.Seq || ch.Order != i {
			t.Fatalf("change ordering fields wrong: %+v", ch)
		}
	}

	// --- 快照2：update(a) -> delete(b) -> insert(c) ---
	f2 := feed[1]
	if len(f2.Changes) != 3 {
		t.Fatalf("snapshot2 want 3 changes, got %d", len(f2.Changes))
	}
	want2 := []struct {
		kind RowChangeKind
		pk   string
	}{
		{RowUpdated, "a"}, {RowDeleted, "b"}, {RowInserted, "c"},
	}
	for i, ch := range f2.Changes {
		t.Logf("流水 snapshot=%s order=%d kind=%s pk=%v row=%v",
			f2.Snapshot.ID, ch.Order, ch.Kind, ch.PK, ch.Row)
		if ch.Kind != want2[i].kind || ch.PK != want2[i].pk {
			t.Fatalf("snapshot2 change %d = %s/%v, want %s/%s",
				i, ch.Kind, ch.PK, want2[i].kind, want2[i].pk)
		}
	}
	if f2.Changes[0].Row["name"] != "alice2" {
		t.Fatalf("update after-image wrong: %+v", f2.Changes[0].Row)
	}
	if f2.Changes[1].Row != nil {
		t.Fatalf("delete change must be a tombstone without row values, got %+v", f2.Changes[1].Row)
	}

	// --- 快照3：无变化 upsert 与无效 delete 不产生事件；d 有 insert+update 两条 ---
	f3 := feed[2]
	if len(f3.Changes) != 2 {
		t.Fatalf("snapshot3 want 2 changes, got %d: %+v", len(f3.Changes), f3.Changes)
	}
	if f3.Changes[0].Kind != RowInserted || f3.Changes[0].PK != "d" {
		t.Fatalf("snapshot3 change0 wrong: %+v", f3.Changes[0])
	}
	if f3.Changes[1].Kind != RowUpdated || f3.Changes[1].PK != "d" {
		t.Fatalf("snapshot3 change1 wrong: %+v", f3.Changes[1])
	}
	t.Logf("无变化变更已过滤，snapshot=%s 实际流水=%d 条", f3.Snapshot.ID, len(f3.Changes))

	// --- 重复读取结果一致（快照推进 + 流水内容均稳定） ---
	feedAgain, err := tbl.ReadChangesSince(0)
	if err != nil {
		t.Fatal(err)
	}
	if !feedEqual(feed, feedAgain) {
		t.Fatal("repeated ReadChangesSince returned different results")
	}
	t.Log("重复执行 ReadChangesSince(0) 结果一致")

	// --- 断点续读：消费到 s1(Seq=1) 后，只返回 s2、s3 ---
	cont, err := tbl.ReadChangesSince(s1Seq(f1))
	if err != nil {
		t.Fatal(err)
	}
	if len(cont) != 2 {
		t.Fatalf("checkpoint read want 2 snapshots, got %d", len(cont))
	}
	if cont[0].Snapshot.ID != s2 || cont[1].Snapshot.ID != s3 {
		t.Fatalf("checkpoint read tail wrong: %s %s", cont[0].Snapshot.ID, cont[1].Snapshot.ID)
	}
	// 最终断点之后无新增。
	tail, err := tbl.ReadChangesSince(f3.Snapshot.Seq)
	if err != nil {
		t.Fatal(err)
	}
	if len(tail) != 0 {
		t.Fatalf("read past last snapshot want 0 items, got %d", len(tail))
	}
	t.Logf("断点续读 Seq>%d 只返回未消费的 %s,%s", f1.Snapshot.Seq, s2, s3)

	// --- 变更明细与最终快照数据一致（事件重放得到的 a 行值 == 快照值） ---
	rows, err := tbl.Read("")
	if err != nil {
		t.Fatal(err)
	}
	rowA := findRow(rows, "a")
	if rowA["name"] != "alice2" {
		t.Fatalf("final snapshot row a wrong: %+v", rowA)
	}
	t.Logf("最终快照行数=%d 行 a=%v，与流水最后状态一致", len(rows), rowA)
}

func s1Seq(f SnapshotChanges) uint64 { return f.Snapshot.Seq }

func findRow(rows []Row, pk string) Row {
	for _, r := range rows {
		if r["id"] == pk {
			return r
		}
	}
	return nil
}

func feedEqual(a, b []SnapshotChanges) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Snapshot.ID != b[i].Snapshot.ID || len(a[i].Changes) != len(b[i].Changes) {
			return false
		}
		for j := range a[i].Changes {
			x, y := a[i].Changes[j], b[i].Changes[j]
			if x.Order != y.Order || x.Kind != y.Kind || fmt.Sprint(x.PK) != fmt.Sprint(y.PK) {
				return false
			}
		}
	}
	return true
}
