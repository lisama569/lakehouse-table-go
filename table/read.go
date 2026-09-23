package table

import (
	"fmt"
	"sort"
)

// Scan 读取指定快照的全部行，按主键排序返回，键为该快照结构下的列名。
// 结构演进后新增的列在旧数据中读出为 nil（确定的缺省语义）；
// 已删除的列不再出现；重命名列按新名读出同一列 ID 的数据，不错位。
func (t *Table) Scan(snapID int64) ([]Row, error) {
	snap, err := t.Snapshot(snapID)
	if err != nil {
		return nil, err
	}
	schema, err := t.schemaVersion(snap.SchemaVersion)
	if err != nil {
		return nil, err
	}
	rows, err := t.loadRows(snap)
	if err != nil {
		return nil, err
	}

	keys := make([]string, 0, len(rows))
	for k := range rows {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	out := make([]Row, 0, len(keys))
	for _, k := range keys {
		r := rows[k]
		row := Row{"_pk": k}
		for _, col := range schema.Columns {
			row[col.Name] = coerceValue(r.Values[col.ID], col.Type) // 缺失则为 nil
		}
		out = append(out, row)
	}
	return out, nil
}

// coerceValue 把 JSON 解码出的值按列类型归一化，
// 保证读出的 Go 类型与列声明一致（如 int64 不会变成 float64）。
func coerceValue(v any, dt DataType) any {
	if v == nil {
		return nil
	}
	switch dt {
	case TypeInt64:
		if f, ok := v.(float64); ok {
			return int64(f)
		}
	case TypeFloat64:
		if f, ok := v.(float64); ok {
			return f
		}
	}
	return v
}

// Changes 返回 (from, to] 区间内的增量变更流水，按快照顺序排列。
// from 传 0 表示从首个快照开始。
func (t *Table) Changes(from, to int64) ([]Record, error) {
	if to <= from {
		return nil, fmt.Errorf("table: invalid range (%d, %d]", from, to)
	}
	head, err := t.Head()
	if err != nil {
		return nil, err
	}
	if to > head {
		return nil, fmt.Errorf("%w: %d beyond head %d", ErrSnapshotNotFound, to, head)
	}
	var out []Record
	for id := from + 1; id <= to; id++ {
		snap, err := t.Snapshot(id)
		if err != nil {
			return nil, err
		}
		out = append(out, snap.Changes...)
	}
	return out, nil
}
