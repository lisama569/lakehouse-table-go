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
//
// 并发语义：多个实例、脚本或命令行工具同时创建同名表时，恰好一个成功，
// 其余收到 ErrTableExists（与结构不兼容、数据非法、提交冲突均可区分）。
// 存在性判定与初始快照落盘在同一个 flock 临界区内完成（先建锁文件再判定），
// 因此被拒绝的一方不会改写已建好的结构与数据；无论并发多少、谁先谁后，
// 最终表的结构与数据自洽（就是胜者那一份）。
func CreateTable(store *Store, name string, schema Schema) (*Table, error) {
	if err := validateSchema(schema); err != nil {
		return nil, err
	}
	// 先取表级排他锁（会顺带建好 meta 目录与 LOCK 文件），
	// 把 "判定表是否已建好 → 写初始快照 → 推进 HEAD" 整体串行化，
	// 消除 check-then-act 竞态。
	lk, err := store.lock(name)
	if err != nil {
		return nil, err
	}
	defer lk.unlock()

	// 以 HEAD 是否已建立为准：HEAD 存在说明初始快照已完整落盘、表已生效。
	// （不能用 meta 目录是否存在判定——本函数取锁时就会创建该目录。）
	head, err := store.readHead(name)
	if err != nil {
		return nil, err
	}
	if head != "" {
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

	// 跨进程互斥：多个实例/脚本同时打开同一张表时，
	// 整个提交临界区只有一个提交者，其余在重读 HEAD 时被冲突检测拒绝。
	lk, err := t.store.lock(t.name)
	if err != nil {
		return "", err
	}
	defer lk.unlock()

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
	// 先写数据文件、行级变更明细与快照元数据，最后 CAS 推进 HEAD：
	// HEAD 之前的任何失败都不会产生可见的中间状态。
	if err := t.store.writeData(t.name, snap.DataFile, merged); err != nil {
		return "", err
	}
	recs := changeRecords(base, parent.Schema, batch, snap.ID, snap.Seq)
	if err := t.store.writeChanges(t.name, snap.ID, recs); err != nil {
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

	lk, err := t.store.lock(t.name)
	if err != nil {
		return "", err
	}
	defer lk.unlock()

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
	// 结构演进快照无行级业务变更，显式写空明细，便于增量消费统一处理。
	if err := t.store.writeChanges(t.name, snap.ID, []ChangeRecord{}); err != nil {
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

// RenameColumn 带数据的列改名迁移：生成一个新快照，
// 新列名下保留改名前的全部历史值，同一行其他列不受影响。
// 改名后旧快照仍按当时的结构与值读取，结果可复现。
// 不允许改主键列、不允许新列名与既有列冲突（ErrSchemaIncompatible）。
func (t *Table) RenameColumn(from, to string) (string, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	lk, err := t.store.lock(t.name)
	if err != nil {
		return "", err
	}
	defer lk.unlock()

	head, err := t.store.readHead(t.name)
	if err != nil {
		return "", err
	}
	parent, err := t.store.readSnapshot(t.name, head)
	if err != nil {
		return "", err
	}
	if err := checkRename(parent.Schema, from, to); err != nil {
		return "", err
	}
	newSchema := renameColumnInSchema(parent.Schema, from, to)

	base, err := t.store.readData(t.name, parent.DataFile)
	if err != nil {
		return "", err
	}
	migrated := make([]Row, 0, len(base))
	for _, r := range base {
		migrated = append(migrated, renameColumnInRow(r, from, to))
	}
	migrated = sortRows(migrated, newSchema.PrimaryKey)

	snap := &Snapshot{
		ID:        snapshotID(parent.Seq + 1),
		Seq:       parent.Seq + 1,
		ParentID:  head,
		Schema:    newSchema,
		DataFile:  snapshotID(parent.Seq+1) + ".json",
		BatchID:   "", // 结构迁移快照，无数据批次
		CreatedAt: time.Now().UnixNano(),
	}
	if err := t.store.writeData(t.name, snap.DataFile, migrated); err != nil {
		return "", err
	}
	// 列改名迁移快照无行级业务变更，显式写空明细。
	if err := t.store.writeChanges(t.name, snap.ID, []ChangeRecord{}); err != nil {
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
		return "", fmt.Errorf("%w: head moved during column rename", ErrCommitConflict)
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

// ReadChangesSince 增量读取行级变更明细：返回 Seq 大于 sinceSeq 的各快照
// 中真实发生的行级变更（insert/update/delete + 主键 + 变更后整行），
// 按快照序号与批次内顺序稳定输出。重复执行结果一致；
// 断点续读时传入已消费的最大 Seq，只会返回未消费的部分。
// 结构演进/列改名快照不产生行级变更记录。
func (t *Table) ReadChangesSince(sinceSeq uint64) ([]ChangeRecord, error) {
	snaps, err := t.store.listSnapshots(t.name)
	if err != nil {
		return nil, err
	}
	var out []ChangeRecord
	for _, s := range snaps {
		if s.Seq <= sinceSeq {
			continue
		}
		recs, err := t.store.readChanges(t.name, s.ID)
		if err != nil {
			return nil, err
		}
		out = append(out, recs...)
	}
	return out, nil
}
