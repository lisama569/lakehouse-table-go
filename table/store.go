package table

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"syscall"
)

// Store 本地文件系统存储：负责表元数据与数据文件的读写。
// 目录布局：
//
//	<root>/<table>/meta/HEAD            当前快照 ID（原子替换写入）
//	<root>/<table>/meta/LOCK            跨进程提交互斥锁（flock）
//	<root>/<table>/meta/snapshots/<id>.json
//	<root>/<table>/meta/batches/<batchID> 已提交批次标记（幂等）
//	<root>/<table>/meta/changes/<id>.json 快照对应的行级变更明细
//	<root>/<table>/data/<file>.json     快照数据文件（不可变）
type Store struct {
	root string
}

// NewStore 创建本地存储，root 为数据根目录。
func NewStore(root string) *Store {
	return &Store{root: root}
}

func (s *Store) tableDir(name string) string  { return filepath.Join(s.root, name) }
func (s *Store) metaDir(name string) string   { return filepath.Join(s.tableDir(name), "meta") }
func (s *Store) dataDir(name string) string   { return filepath.Join(s.tableDir(name), "data") }
func (s *Store) headPath(name string) string  { return filepath.Join(s.metaDir(name), "HEAD") }
func (s *Store) batchDir(name string) string  { return filepath.Join(s.metaDir(name), "batches") }
func (s *Store) snapDir(name string) string   { return filepath.Join(s.metaDir(name), "snapshots") }
func (s *Store) changeDir(name string) string { return filepath.Join(s.metaDir(name), "changes") }
func (s *Store) lockPath(name string) string  { return filepath.Join(s.metaDir(name), "LOCK") }

func (s *Store) snapPath(table, id string) string {
	return filepath.Join(s.snapDir(table), id+".json")
}

func (s *Store) dataPath(table, file string) string {
	return filepath.Join(s.dataDir(table), file)
}

// initTable 创建表目录结构。
func (s *Store) initTable(name string) error {
	for _, d := range []string{s.metaDir(name), s.dataDir(name), s.batchDir(name), s.snapDir(name), s.changeDir(name)} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			return err
		}
	}
	return nil
}

// tableLock 是基于 flock 的跨进程互斥锁，用于串行化不同进程/实例的提交临界区。
type tableLock struct {
	f *os.File
}

// lock 阻塞式获取表级排他锁。同一进程内由 Table.mu 先行串行化，
// 这里的 flock 保证多个进程/实例同时打开同一张表时，
// "读 HEAD → 校验 → 写文件 → 推进 HEAD" 整个临界区也只有一个提交者。
func (s *Store) lock(name string) (*tableLock, error) {
	if err := os.MkdirAll(s.metaDir(name), 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(s.lockPath(name), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		f.Close()
		return nil, fmt.Errorf("acquire table lock: %w", err)
	}
	return &tableLock{f: f}, nil
}

func (l *tableLock) unlock() {
	_ = syscall.Flock(int(l.f.Fd()), syscall.LOCK_UN)
	_ = l.f.Close()
}

func (s *Store) tableExists(name string) bool {
	st, err := os.Stat(s.metaDir(name))
	return err == nil && st.IsDir()
}

// tableReady 判断表是否已经完整建好：meta 目录存在且 HEAD 已写入。
// 仅有 meta 目录占位（建表进行中或建表者中途失败）不算就绪，
// 这样并发建表的后来者在持锁复查时能区分"已建好"与"半成品"。
func (s *Store) tableReady(name string) bool {
	if !s.tableExists(name) {
		return false
	}
	head, err := s.readHead(name)
	return err == nil && head != ""
}

// writeFileAtomic 先写临时文件再 rename，保证读取方不会看到部分写入。
func writeFileAtomic(path string, data []byte) error {
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// readHead 读取当前 HEAD 快照 ID；不存在时返回空串。
func (s *Store) readHead(name string) (string, error) {
	b, err := os.ReadFile(s.headPath(name))
	if errors.Is(err, fs.ErrNotExist) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// compareAndSwapHead 仅当当前 HEAD 等于 expect 时替换为 next，返回是否成功。
// 调用方必须已持有表锁（进程内 mu + 跨进程 flock），因此这里的
// 重读校验 + 原子替换在跨进程场景下同样构成原子比较交换。
func (s *Store) compareAndSwapHead(name, expect, next string) (bool, error) {
	cur, err := s.readHead(name)
	if err != nil {
		return false, err
	}
	if cur != expect {
		return false, nil
	}
	if err := writeFileAtomic(s.headPath(name), []byte(next)); err != nil {
		return false, err
	}
	return true, nil
}

// writeSnapshot 持久化快照元数据。
func (s *Store) writeSnapshot(table string, snap *Snapshot) error {
	b, err := json.MarshalIndent(snap, "", "  ")
	if err != nil {
		return err
	}
	return writeFileAtomic(s.snapPath(table, snap.ID), b)
}

// readSnapshot 读取快照元数据。
func (s *Store) readSnapshot(table, id string) (*Snapshot, error) {
	b, err := os.ReadFile(s.snapPath(table, id))
	if err != nil {
		return nil, err
	}
	var snap Snapshot
	if err := json.Unmarshal(b, &snap); err != nil {
		return nil, err
	}
	return &snap, nil
}

// writeData 写入不可变数据文件（行集合）。
func (s *Store) writeData(table, file string, rows []Row) error {
	b, err := json.Marshal(rows)
	if err != nil {
		return err
	}
	return writeFileAtomic(s.dataPath(table, file), b)
}

// readData 读取数据文件。
func (s *Store) readData(table, file string) ([]Row, error) {
	b, err := os.ReadFile(s.dataPath(table, file))
	if err != nil {
		return nil, err
	}
	var rows []Row
	if err := json.Unmarshal(b, &rows); err != nil {
		return nil, err
	}
	return rows, nil
}

// writeChanges 持久化一个快照对应的行级变更明细（不可变）。
func (s *Store) writeChanges(table, snapID string, recs []ChangeRecord) error {
	b, err := json.Marshal(recs)
	if err != nil {
		return err
	}
	return writeFileAtomic(filepath.Join(s.changeDir(table), snapID+".json"), b)
}

// readChanges 读取快照对应的行级变更明细；文件不存在时返回空（兼容旧表）。
func (s *Store) readChanges(table, snapID string) ([]ChangeRecord, error) {
	b, err := os.ReadFile(filepath.Join(s.changeDir(table), snapID+".json"))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var recs []ChangeRecord
	if err := json.Unmarshal(b, &recs); err != nil {
		return nil, err
	}
	return recs, nil
}

// hasBatch 判断批次是否已提交。
func (s *Store) hasBatch(table, batchID string) bool {
	_, err := os.Stat(filepath.Join(s.batchDir(table), batchID))
	return err == nil
}

// markBatch 记录批次已提交。
func (s *Store) markBatch(table, batchID string) error {
	return writeFileAtomic(filepath.Join(s.batchDir(table), batchID), []byte("1"))
}

// listSnapshots 按序号升序列出全部快照元数据。
func (s *Store) listSnapshots(table string) ([]*Snapshot, error) {
	entries, err := os.ReadDir(s.snapDir(table))
	if err != nil {
		return nil, err
	}
	var snaps []*Snapshot
	for _, e := range entries {
		if filepath.Ext(e.Name()) != ".json" {
			continue
		}
		snap, err := s.readSnapshot(table, e.Name()[:len(e.Name())-len(".json")])
		if err != nil {
			return nil, fmt.Errorf("read snapshot %s: %w", e.Name(), err)
		}
		snaps = append(snaps, snap)
	}
	// 按 Seq 排序（插入排序，快照数量小）
	for i := 1; i < len(snaps); i++ {
		for j := i; j > 0 && snaps[j].Seq < snaps[j-1].Seq; j-- {
			snaps[j], snaps[j-1] = snaps[j-1], snaps[j]
		}
	}
	return snaps, nil
}
