package table

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"
)

// Table 一张表的读写句柄。
type Table struct {
	Name string
	dir  string
}

// persistedRow 数据文件中的一行：主键 + 按列 ID 存储的值。
type persistedRow struct {
	Key    string       `json:"key"`
	Values map[int]any `json:"values"`
}

func (t *Table) headPath() string { return filepath.Join(t.dir, "HEAD") }

func (t *Table) schemaPath(version int) string {
	return filepath.Join(t.dir, "schemas", fmt.Sprintf("v%d.json", version))
}

func (t *Table) snapshotPath(id int64) string {
	return filepath.Join(t.dir, "snapshots", fmt.Sprintf("%06d.json", id))
}

func (t *Table) dataPath(id int64) string {
	return filepath.Join(t.dir, "data", fmt.Sprintf("%06d.jsonl", id))
}

// Head 返回当前最新快照 ID；表为空时返回 0。
func (t *Table) Head() (int64, error) {
	data, err := os.ReadFile(t.headPath())
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(string(data), 10, 64)
}

// Snapshot 读取指定快照的元数据。
func (t *Table) Snapshot(id int64) (*Snapshot, error) {
	var s Snapshot
	if err := readJSON(t.snapshotPath(id), &s); err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("%w: %d", ErrSnapshotNotFound, id)
		}
		return nil, err
	}
	return &s, nil
}

// SchemaAt 返回指定快照对应的结构。
func (t *Table) SchemaAt(snapID int64) (Schema, error) {
	snap, err := t.Snapshot(snapID)
	if err != nil {
		return Schema{}, err
	}
	return t.schemaVersion(snap.SchemaVersion)
}

// schemaVersion 读取指定版本的结构。
func (t *Table) schemaVersion(version int) (Schema, error) {
	var s Schema
	if err := readJSON(t.schemaPath(version), &s); err != nil {
		return Schema{}, err
	}
	return s, nil
}

func (t *Table) writeSchema(s Schema) error {
	return writeJSON(t.schemaPath(s.Version), s)
}

// latestSchema 返回当前最高版本的结构。
func (t *Table) latestSchema() (Schema, error) {
	entries, err := os.ReadDir(filepath.Join(t.dir, "schemas"))
	if err != nil {
		return Schema{}, err
	}
	maxV := 0
	for _, e := range entries {
		var v int
		if _, err := fmt.Sscanf(e.Name(), "v%d.json", &v); err == nil && v > maxV {
			maxV = v
		}
	}
	if maxV == 0 {
		return Schema{}, fmt.Errorf("%w: no schema", ErrTableNotFound)
	}
	return t.schemaVersion(maxV)
}

// loadRows 读取快照的全量数据。
func (t *Table) loadRows(snap *Snapshot) (map[string]persistedRow, error) {
	rows := map[string]persistedRow{}
	if snap.DataFile == "" {
		return rows, nil
	}
	data, err := os.ReadFile(filepath.Join(t.dir, "data", snap.DataFile))
	if os.IsNotExist(err) {
		return rows, nil
	}
	if err != nil {
		return nil, err
	}
	for _, line := range splitLines(data) {
		var r persistedRow
		if err := json.Unmarshal(line, &r); err != nil {
			return nil, err
		}
		rows[r.Key] = r
	}
	return rows, nil
}

func splitLines(data []byte) [][]byte {
	var out [][]byte
	start := 0
	for i, b := range data {
		if b == '\n' {
			if i > start {
				out = append(out, data[start:i])
			}
			start = i + 1
		}
	}
	if start < len(data) {
		out = append(out, data[start:])
	}
	return out
}

// Commit 以 base 为父快照提交一批变更，返回新快照。
// base 必须等于当前 Head，否则返回 ErrCommitConflict。
// 若 b.ID 已被应用过，直接返回既有快照（幂等重放）。
func (t *Table) Commit(base int64, b Batch) (*Snapshot, error) {
	if b.ID == "" {
		return nil, fmt.Errorf("table: batch id is required for idempotent commit")
	}
	unlock, err := fileLock(t.dir)
	if err != nil {
		return nil, err
	}
	defer unlock()

	// 幂等重放：该批次已应用则直接返回既有快照，不产生重复行。
	if snap, ok, err := t.findByBatchID(b.ID); err != nil {
		return nil, err
	} else if ok {
		return snap, nil
	}

	head, err := t.Head()
	if err != nil {
		return nil, err
	}
	if head != base {
		return nil, fmt.Errorf("%w: base=%d head=%d", ErrCommitConflict, base, head)
	}

	schema, err := t.latestSchema()
	if err != nil {
		return nil, err
	}

	// 合并：以父快照数据为底，按批次内记录顺序应用 upsert/delete。
	rows := map[string]persistedRow{}
	if base > 0 {
		parent, err := t.Snapshot(base)
		if err != nil {
			return nil, err
		}
		rows, err = t.loadRows(parent)
		if err != nil {
			return nil, err
		}
	}
	for _, rec := range b.Records {
		if err := validateRecord(schema, rec); err != nil {
			return nil, err
		}
		switch rec.Op {
		case OpDelete:
			delete(rows, rec.Key)
		default: // OpUpsert，同键后者覆盖前者，规则确定可复现
			rows[rec.Key] = persistedRow{Key: rec.Key, Values: rec.Values}
		}
	}

	return t.commitLocked(base, schema.Version, b.ID, b.Records, rows)
}

// commitLocked 在持有锁的临界区内落盘新快照并推进 HEAD。
// 调用方需已完成冲突检查。数据文件与快照元数据先于 HEAD 落盘，
// 因此任何时刻读者只能看到完整快照。
func (t *Table) commitLocked(parent int64, schemaVersion int, batchID string, changes []Record, rows map[string]persistedRow) (*Snapshot, error) {
	id := parent + 1
	dataFile := fmt.Sprintf("%06d.jsonl", id)

	keys := make([]string, 0, len(rows))
	for k := range rows {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var buf []byte
	h := sha256.New()
	for _, k := range keys {
		line, err := json.Marshal(rows[k])
		if err != nil {
			return nil, err
		}
		buf = append(buf, line...)
		buf = append(buf, '\n')
		h.Write(line)
		h.Write([]byte{'\n'})
	}
	if err := atomicWriteFile(filepath.Join(t.dir, "data", dataFile), buf); err != nil {
		return nil, err
	}

	snap := &Snapshot{
		ID:            id,
		ParentID:      parent,
		SchemaVersion: schemaVersion,
		BatchID:       batchID,
		DataFile:      dataFile,
		RowCount:      len(rows),
		Checksum:      hex.EncodeToString(h.Sum(nil)),
		Changes:       changes,
		CreatedAt:     time.Now().UTC(),
	}
	if err := writeJSON(t.snapshotPath(id), snap); err != nil {
		return nil, err
	}
	// 最后推进 HEAD：此前读者仍看到旧快照。
	if err := atomicWriteFile(t.headPath(), []byte(strconv.FormatInt(id, 10))); err != nil {
		return nil, err
	}
	return snap, nil
}

// findByBatchID 沿快照链查找批次 ID 是否已应用。
func (t *Table) findByBatchID(batchID string) (*Snapshot, bool, error) {
	head, err := t.Head()
	if err != nil {
		return nil, false, err
	}
	for id := head; id > 0; id-- {
		snap, err := t.Snapshot(id)
		if err != nil {
			return nil, false, err
		}
		if snap.BatchID == batchID {
			return snap, true, nil
		}
	}
	return nil, false, nil
}

// validateRecord 校验记录引用的列存在于结构中。
func validateRecord(schema Schema, rec Record) error {
	if rec.Op == OpDelete {
		return nil
	}
	for colID := range rec.Values {
		if _, ok := schema.ColumnByID(colID); !ok {
			return fmt.Errorf("%w: column id %d", ErrUnknownColumn, colID)
		}
	}
	return nil
}
