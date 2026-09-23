package table

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

// 目录布局：
//   <root>/<table>/HEAD                 当前快照 ID（原子替换更新）
//   <root>/<table>/LOCK                 提交时使用的 flock 锁文件
//   <root>/<table>/schemas/v<N>.json    各版本结构
//   <root>/<table>/snapshots/<ID>.json  快照元数据
//   <root>/<table>/data/<ID>.jsonl      快照全量数据（写时复制）

// Store 管理本地根目录下的多张表。
type Store struct {
	Root string
}

// NewStore 创建存储句柄，root 不存在时创建。
func NewStore(root string) (*Store, error) {
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, err
	}
	return &Store{Root: root}, nil
}

// CreateTable 以初始结构建表。
func (s *Store) CreateTable(name string, schema Schema) (*Table, error) {
	dir := filepath.Join(s.Root, name)
	if _, err := os.Stat(dir); err == nil {
		return nil, fmt.Errorf("%w: %s", ErrTableExists, name)
	}
	for _, sub := range []string{"schemas", "snapshots", "data"} {
		if err := os.MkdirAll(filepath.Join(dir, sub), 0o755); err != nil {
			return nil, err
		}
	}
	schema.Version = 1
	if err := assignColumnIDs(&schema); err != nil {
		return nil, err
	}
	t := &Table{Name: name, dir: dir}
	if err := t.writeSchema(schema); err != nil {
		return nil, err
	}
	// HEAD=0 表示空表。
	if err := atomicWriteFile(filepath.Join(dir, "HEAD"), []byte("0")); err != nil {
		return nil, err
	}
	return t, nil
}

// OpenTable 打开已存在的表。
func (s *Store) OpenTable(name string) (*Table, error) {
	dir := filepath.Join(s.Root, name)
	if st, err := os.Stat(dir); err != nil || !st.IsDir() {
		return nil, fmt.Errorf("%w: %s", ErrTableNotFound, name)
	}
	return &Table{Name: name, dir: dir}, nil
}

// assignColumnIDs 为未分配 ID 的列分配递增 ID。
func assignColumnIDs(s *Schema) error {
	max := 0
	seen := map[int]bool{}
	for _, c := range s.Columns {
		if c.ID > max {
			max = c.ID
		}
		if seen[c.ID] && c.ID != 0 {
			return fmt.Errorf("%w: duplicate column id %d", ErrSchemaIncompatible, c.ID)
		}
		seen[c.ID] = true
	}
	for i := range s.Columns {
		if s.Columns[i].ID == 0 {
			max++
			s.Columns[i].ID = max
		}
	}
	return nil
}

// atomicWriteFile 先写临时文件再 rename，保证读者只见完整内容。
func atomicWriteFile(path string, data []byte) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), ".tmp-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), path)
}

// writeJSON 以原子方式写 JSON 文件。
func writeJSON(path string, v any) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return atomicWriteFile(path, data)
}

// readJSON 读取 JSON 文件。
func readJSON(path string, v any) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, v)
}

// fileLock 对表目录的 LOCK 文件加排他 flock，返回解锁函数。
// 使“检查 HEAD + 落盘新快照 + 更新 HEAD”成为临界区。
func fileLock(dir string) (func(), error) {
	f, err := os.OpenFile(filepath.Join(dir, "LOCK"), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		f.Close()
		return nil, err
	}
	return func() {
		syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		f.Close()
	}, nil
}
