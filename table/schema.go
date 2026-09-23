package table

import "fmt"

// SchemaChange 一次结构演进：新增/删除/重命名列。
// 变更以 ChangeID 作为幂等键，随一次快照提交生效。
type SchemaChange struct {
	ChangeID string
	Add      []Column          // 新增列（ID 由系统分配）
	Drop     []string          // 按列名删除
	Rename   map[string]string // old -> new
}

// EvolveSchema 以 base 为父快照提交结构演进，返回新快照。
// 数据行不变，仅推进结构版本；幂等键为 ChangeID。
func (t *Table) EvolveSchema(base int64, ch SchemaChange) (*Snapshot, error) {
	if ch.ChangeID == "" {
		return nil, fmt.Errorf("table: change id is required for idempotent schema evolution")
	}
	unlock, err := fileLock(t.dir)
	if err != nil {
		return nil, err
	}
	defer unlock()

	batchID := "schema:" + ch.ChangeID
	if snap, ok, err := t.findByBatchID(batchID); err != nil {
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

	cur, err := t.latestSchema()
	if err != nil {
		return nil, err
	}
	next, err := applySchemaChange(cur, ch)
	if err != nil {
		return nil, err
	}
	if err := t.writeSchema(next); err != nil {
		return nil, err
	}

	// 结构演进不改动数据：沿用父快照数据行。
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
	return t.commitLocked(base, next.Version, batchID, nil, rows)
}

// applySchemaChange 校验并应用结构变更，返回新版本结构。
// 兼容规则：
//   - 新增列：列名不得已存在，分配新列 ID，旧数据读出时该列为 nil；
//   - 删除列：列名必须存在，仅移出结构，底层数据保留但不再可读；
//   - 重命名：旧名必须存在、新名不得占用，列 ID 不变，数据不错位；
//   - 不允许修改既有列类型。
func applySchemaChange(s Schema, ch SchemaChange) (Schema, error) {
	cols := make([]Column, len(s.Columns))
	copy(cols, s.Columns)

	for _, c := range ch.Add {
		if _, ok := s.ColumnByName(c.Name); ok {
			return Schema{}, fmt.Errorf("%w: add existing column %q", ErrSchemaIncompatible, c.Name)
		}
		cols = append(cols, Column{Name: c.Name, Type: c.Type})
	}
	for _, name := range ch.Drop {
		idx := -1
		for i, c := range cols {
			if c.Name == name {
				idx = i
				break
			}
		}
		if idx < 0 {
			return Schema{}, fmt.Errorf("%w: drop unknown column %q", ErrSchemaIncompatible, name)
		}
		cols = append(cols[:idx], cols[idx+1:]...)
	}
	for old, newName := range ch.Rename {
		if _, ok := s.ColumnByName(newName); ok {
			return Schema{}, fmt.Errorf("%w: rename target %q already exists", ErrSchemaIncompatible, newName)
		}
		found := false
		for i := range cols {
			if cols[i].Name == old {
				cols[i].Name = newName // 列 ID 保持不变
				found = true
				break
			}
		}
		if !found {
			return Schema{}, fmt.Errorf("%w: rename unknown column %q", ErrSchemaIncompatible, old)
		}
	}

	next := Schema{Version: s.Version + 1, Columns: cols}
	if err := assignColumnIDs(&next); err != nil {
		return Schema{}, err
	}
	return next, nil
}
