package table

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Table 一张表的句柄，提供快照提交与读取。
type Table struct {
	store *Store
	name  string
	mu    sync.Mutex // 串行化进程内提交，保证冲突检测原子性
	lock  tableLock  // 跨进程互斥锁（懒加载）
}

// acquireCommitLock 取跨进程文件锁（调用方必须已持有 t.mu），返回释放函数。
// 提交/改名/结构演进均通过它串行化（跨进程、跨实例）。
func (t *Table) acquireCommitLock() (func(), error) {
	if t.lock == nil {
		l, err := newTableLock(filepath.Join(t.store.metaDir(t.name), "LOCK"))
		if err != nil {
			return nil, err
		}
		t.lock = l
	}
	if err := t.lock.lock(); err != nil {
		return nil, err
	}
	return func() { _ = t.lock.unlock() }, nil
}

// CreateTable 创建新表，初始快照为空数据。
// 通过根目录锁 + 二次检查保证多进程/多实例并发创建同一张表时
// 恰好一个成功，其余收到 ErrTableExists。
func CreateTable(store *Store, name string, schema Schema) (*Table, error) {
	if err := validateSchema(schema); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(store.metaDir(name), 0o755); err != nil {
		return nil, err
	}
	t := &Table{store: store, name: name}
	release, err := t.acquireCommitLock()
	if err != nil {
		return nil, err
	}
	defer release()
	// 持锁后二次检查：可能在 MkdirAll 与加锁之间已被其他进程创建。
	if head, err := store.readHead(name); err != nil {
		return nil, err
	} else if head != "" {
		return nil, fmt.Errorf("%w: %s", ErrTableExists, name)
	}
	if err := store.initTable(name); err != nil {
		return nil, err
	}
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
	release, err := t.acquireCommitLock()
	if err != nil {
		return "", err
	}
	defer release()

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
	seq := parent.Seq + 1
	merged, changeLog := applyBatchWithLog(base, parent.Schema, batch, seq)

	changeFile := snapshotID(seq) + ".changes.json"
	snap := &Snapshot{
		ID:         snapshotID(seq),
		Seq:        seq,
		ParentID:   head,
		Schema:     parent.Schema,
		DataFile:   snapshotID(seq) + ".json",
		ChangeFile: changeFile,
		BatchID:    batch.BatchID,
		CreatedAt:  time.Now().UnixNano(),
	}
	// 先写数据文件与行级变更流水、再写快照元数据，最后 CAS 推进 HEAD：
	// HEAD 之前的任何失败都不会产生可见的中间状态。
	if err := t.store.writeData(t.name, snap.DataFile, merged); err != nil {
		return "", err
	}
	if err := t.store.writeChanges(t.name, changeFile, changeLog); err != nil {
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
	release, err := t.acquireCommitLock()
	if err != nil {
		return "", err
	}
	defer release()

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

// RenameColumn 执行带数据的列改名迁移：列值原样搬运到新列名，
// 生成一个新快照（旧快照结构与值保持不变）。
// 不兼容改动（改主键列、旧列不存在、新列名冲突、新旧同名）返回
// ErrSchemaIncompatible；并发冲突返回 ErrCommitConflict，调用方可重试。
func (t *Table) RenameColumn(from, to string) (string, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	release, err := t.acquireCommitLock()
	if err != nil {
		return "", err
	}
	defer release()

	head, err := t.store.readHead(t.name)
	if err != nil {
		return "", err
	}
	parent, err := t.store.readSnapshot(t.name, head)
	if err != nil {
		return "", err
	}
	if err := validateRename(parent.Schema, from, to); err != nil {
		return "", err
	}

	// 新结构：仅替换列名，列的顺序、类型、缺省值全部保留。
	newSchema := Schema{PrimaryKey: parent.Schema.PrimaryKey}
	for _, c := range parent.Schema.Columns {
		if c.Name == from {
			c.Name = to
		}
		newSchema.Columns = append(newSchema.Columns, c)
	}

	// 数据搬运：旧列名的值逐行重写到新列名，其余列原样保留。
	base, err := t.store.readData(t.name, parent.DataFile)
	if err != nil {
		return "", err
	}
	projected := make([]Row, 0, len(base))
	for _, r := range base {
		nr := make(Row, len(r))
		for k, v := range r {
			if k == from {
				nr[to] = v
			} else {
				nr[k] = v
			}
		}
		projected = append(projected, nr)
	}
	projected = sortRows(projected, newSchema.PrimaryKey)

	seq := parent.Seq + 1
	snap := &Snapshot{
		ID:        snapshotID(seq),
		Seq:       seq,
		ParentID:  head,
		Schema:    newSchema,
		DataFile:  snapshotID(seq) + ".json",
		BatchID:   "", // 改名迁移不对应数据批次
		Rename:    &ColumnRename{From: from, To: to},
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
		return "", fmt.Errorf("%w: head moved during column rename", ErrCommitConflict)
	}
	return snap.ID, nil
}

// ReadChangesSince 增量消费：返回 Seq 大于 sinceSeq 的各快照推进信息，
// 以及每个快照内按发生顺序排列的行级变更明细。
// 重复调用结果一致；调用方以最后一个快照的 Seq 作为断点续读，
// 后续调用只返回尚未消费的部分。
func (t *Table) ReadChangesSince(sinceSeq uint64) ([]SnapshotChanges, error) {
	snaps, err := t.store.listSnapshots(t.name)
	if err != nil {
		return nil, err
	}
	var out []SnapshotChanges
	for _, s := range snaps {
		if s.Seq <= sinceSeq {
			continue
		}
		changes, err := t.store.readChanges(t.name, s.ChangeFile)
		if err != nil {
			return nil, fmt.Errorf("read changes of snapshot %s: %w", s.ID, err)
		}
		out = append(out, SnapshotChanges{Snapshot: *s, Changes: changes})
	}
	return out, nil
}
