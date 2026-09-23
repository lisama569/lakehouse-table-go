package table

import (
	"errors"
	"fmt"
	"io/fs"
	"sync"
	"time"
)

// Table 一张表的句柄，提供快照提交与读取。
type Table struct {
	store *Store
	name  string
	mu    sync.Mutex // 串行化提交，保证冲突检测原子性
}

// CreateTable 创建新表，初始快照为空数据。
func CreateTable(store *Store, name string, schema Schema) (*Table, error) {
	if err := validateSchema(schema); err != nil {
		return nil, err
	}
	if store.tableExists(name) {
		return nil, fmt.Errorf("%w: %s", ErrTableExists, name)
	}
	if err := store.initTable(name); err != nil {
		return nil, err
	}
	t := &Table{store: store, name: name}
	snap := &Snapshot{
		ID:        snapshotID(0),
		Seq:       0,
		ParentID:  "",
		Schema:    schema,
		DataFile:  snapshotID(0) + ".json",
		CreatedAt: time.Now().UnixNano(),
	}
	if err := store.writeData(name, snap.DataFile, []Row{}); err != nil {
		return nil, err
	}
	if err := store.writeSnapshot(name, snap); err != nil {
		return nil, err
	}
	if err := writeFileAtomic(store.headPath(name), []byte(snap.ID)); err != nil {
		return nil, err
	}
	return t, nil
}

// OpenTable 打开已存在的表。
func OpenTable(store *Store, name string) (*Table, error) {
	if !store.tableExists(name) {
		return nil, fmt.Errorf("%w: %s", ErrTableNotFound, name)
	}
	return &Table{store: store, name: name}, nil
}

// snapshotID 由序号生成确定性的快照标识。
func snapshotID(seq uint64) string {
	return fmt.Sprintf("snap-%06d", seq)
}

// Head 返回当前快照 ID。
func (t *Table) Head() (string, error) {
	return t.store.readHead(t.name)
}

// Commit 以 expectParent 为父快照提交一批变更，返回新快照 ID。
// expectParent 不是当前 HEAD 时返回 ErrCommitConflict；
// BatchID 已提交过时返回 ErrDuplicateBatch（幂等重放，数据不变）。
func (t *Table) Commit(expectParent string, batch Batch) (string, error) {
	if batch.BatchID == "" {
		return "", fmt.Errorf("%w: batch id is empty", ErrInvalidChange)
	}
	t.mu.Lock()
	defer t.mu.Unlock()

	head, err := t.store.readHead(t.name)
	if err != nil {
		return "", err
	}
	// 幂等判定先于冲突判定：已提交过的批次无论基于哪个父快照重放，
	// 都返回 ErrDuplicateBatch，且数据保持不变。
	if t.store.hasBatch(t.name, batch.BatchID) {
		return head, ErrDuplicateBatch
	}
	if head != expectParent {
		return "", fmt.Errorf("%w: expected parent %s, current head %s",
			ErrCommitConflict, expectParent, head)
	}

	parent, err := t.store.readSnapshot(t.name, head)
	if err != nil {
		return "", err
	}
	for _, ch := range batch.Changes {
		if err := validateChange(parent.Schema, ch); err != nil {
			return "", err
		}
	}
	base, err := t.store.readData(t.name, parent.DataFile)
	if err != nil {
		return "", err
	}
	merged := applyBatch(base, parent.Schema, batch)

	snap := &Snapshot{
		ID:        snapshotID(parent.Seq + 1),
		Seq:       parent.Seq + 1,
		ParentID:  head,
		Schema:    parent.Schema,
		DataFile:  snapshotID(parent.Seq+1) + ".json",
		BatchID:   batch.BatchID,
		CreatedAt: time.Now().UnixNano(),
	}
	// 先写数据文件与快照元数据，最后 CAS 推进 HEAD：
	// HEAD 之前的任何失败都不会产生可见的中间状态。
	if err := t.store.writeData(t.name, snap.DataFile, merged); err != nil {
		return "", err
	}
	if err := t.store.writeSnapshot(t.name, snap); err != nil {
		return "", err
	}
	ok, err := t.store.compareAndSwapHead(t.name, expectParent, snap.ID)
	if err != nil {
		return "", err
	}
	if !ok {
		return "", fmt.Errorf("%w: head moved during commit", ErrCommitConflict)
	}
	if err := t.store.markBatch(t.name, batch.BatchID); err != nil {
		return "", err
	}
	return snap.ID, nil
}

// EvolveSchema 演进表结构，生成一个新快照（数据按新结构重写）。
// 兼容规则见 checkEvolution；不兼容时返回 ErrSchemaIncompatible。
func (t *Table) EvolveSchema(newSchema Schema) (string, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	head, err := t.store.readHead(t.name)
	if err != nil {
		return "", err
	}
	parent, err := t.store.readSnapshot(t.name, head)
	if err != nil {
		return "", err
	}
	if err := checkEvolution(parent.Schema, newSchema); err != nil {
		return "", err
	}
	base, err := t.store.readData(t.name, parent.DataFile)
	if err != nil {
		return "", err
	}
	projected := make([]Row, 0, len(base))
	for _, r := range base {
		projected = append(projected, projectRow(r, newSchema))
	}
	projected = sortRows(projected, newSchema.PrimaryKey)

	snap := &Snapshot{
		ID:        snapshotID(parent.Seq + 1),
		Seq:       parent.Seq + 1,
		ParentID:  head,
		Schema:    newSchema,
		DataFile:  snapshotID(parent.Seq+1) + ".json",
		BatchID:   "", // 结构演进快照，无数据批次
		CreatedAt: time.Now().UnixNano(),
	}
	if err := t.store.writeData(t.name, snap.DataFile, projected); err != nil {
		return "", err
	}
	if err := t.store.writeSnapshot(t.name, snap); err != nil {
		return "", err
	}
	ok, err := t.store.compareAndSwapHead(t.name, head, snap.ID)
	if err != nil {
		return "", err
	}
	if !ok {
		return "", fmt.Errorf("%w: head moved during schema evolution", ErrCommitConflict)
	}
	return snap.ID, nil
}

// Read 读取指定快照的完整数据（按主键排序，结果可复现）。
// snapshotID 为空时读取 HEAD。读取只触碰已完整落盘的快照，
// 不会看到提交过程中的中间状态。
func (t *Table) Read(snapshotID string) ([]Row, error) {
	id := snapshotID
	if id == "" {
		var err error
		id, err = t.store.readHead(t.name)
		if err != nil {
			return nil, err
		}
	}
	snap, err := t.store.readSnapshot(t.name, id)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, fmt.Errorf("snapshot %q not found", id)
		}
		return nil, err
	}
	return t.store.readData(t.name, snap.DataFile)
}

// SchemaAt 返回指定快照的结构；snapshotID 为空时为 HEAD。
func (t *Table) SchemaAt(snapshotID string) (*Schema, error) {
	id := snapshotID
	if id == "" {
		var err error
		id, err = t.store.readHead(t.name)
		if err != nil {
			return nil, err
		}
	}
	snap, err := t.store.readSnapshot(t.name, id)
	if err != nil {
		return nil, err
	}
	return &snap.Schema, nil
}

// ReadSince 增量读取：返回 Seq 大于 sinceSeq 的各快照（变更流水），按序排列。
func (t *Table) ReadSince(sinceSeq uint64) ([]Snapshot, error) {
	snaps, err := t.store.listSnapshots(t.name)
	if err != nil {
		return nil, err
	}
	var out []Snapshot
	for _, s := range snaps {
		if s.Seq > sinceSeq {
			out = append(out, *s)
		}
	}
	return out, nil
}
