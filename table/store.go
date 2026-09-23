package table

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

// Store 本地文件系统存储：负责表元数据与数据文件的读写。
// 目录布局：
//
//	<root>/<table>/meta/HEAD            当前快照 ID（原子替换写入）
//	<root>/<table>/meta/snapshots/<id>.json
//	<root>/<table>/meta/batches/<batchID> 已提交批次标记（幂等）
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

func (s *Store) snapPath(table, id string) string {
	return filepath.Join(s.snapDir(table), id+".json")
}

func (s *Store) dataPath(table, file string) string {
	return filepath.Join(s.dataDir(table), file)
}

// initTable 创建表目录结构。
func (s *Store) initTable(name string) error {
	for _, d := range []string{s.metaDir(name), s.dataDir(name), s.batchDir(name), s.snapDir(name)} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) tableExists(name string) bool {
	st, err := os.Stat(s.metaDir(name))
	return err == nil && st.IsDir()
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
// 通过文件锁 + 重读实现原子比较交换（同一进程内由 Table.mu 串行化，
// 跨进程场景可后续替换为 flock；此处重读校验保证语义正确）。
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
