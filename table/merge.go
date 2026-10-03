package table

import (
	"fmt"
	"sort"
)

// pkKey 把主键值规范化为可比较的字符串键，保证合并规则确定且可复现。
func pkKey(v Value) string {
	switch x := v.(type) {
	case string:
		return "s:" + x
	case int64:
		return fmt.Sprintf("i:%d", x)
	case int:
		return fmt.Sprintf("i:%d", x)
	case float64:
		return fmt.Sprintf("f:%v", x)
	case bool:
		return fmt.Sprintf("b:%v", x)
	default:
		return fmt.Sprintf("%T:%v", v, v)
	}
}

// applyBatch 把一批变更确定性地合并进基线行集合，返回新的行集合。
//
// 合并规则（确定且可复现）：
//   - 行按主键标识；Upsert 覆盖同主键整行，Delete 删除同主键行；
//   - 同一批次内同一主键出现多次时，按变更在批次中的顺序后者覆盖前者；
//   - Upsert 行中缺失的列按结构缺省值补齐，保证快照内每行结构完整。
//
// 由于合并只依赖（基线, 批次）两者，同一批次重复应用的结果与
// 应用一次后再重放到同一基线完全一致；配合批次去重即实现幂等。
func applyBatch(base []Row, schema Schema, batch Batch) []Row {
	idx := map[string]int{} // pkKey -> 在 merged 中的位置
	merged := make([]Row, 0, len(base)+len(batch.Changes))
	for _, r := range base {
		pk, ok := r[schema.PrimaryKey]
		if !ok {
			continue
		}
		idx[pkKey(pk)] = len(merged)
		merged = append(merged, r)
	}
	for _, ch := range batch.Changes {
		switch {
		case ch.Upsert != nil:
			row := projectRow(*ch.Upsert, schema) // 补齐缺省列
			k := pkKey(row[schema.PrimaryKey])
			if i, ok := idx[k]; ok {
				merged[i] = row
			} else {
				idx[k] = len(merged)
				merged = append(merged, row)
			}
		case ch.Delete != nil:
			k := pkKey(*ch.Delete)
			if i, ok := idx[k]; ok {
				merged = append(merged[:i], merged[i+1:]...)
				// 重建索引（数据量小，保持实现直白）
				idx = map[string]int{}
				for j, r := range merged {
					idx[pkKey(r[schema.PrimaryKey])] = j
				}
			}
		}
	}
	return sortRows(merged, schema.PrimaryKey)
}

// applyBatchWithLog 与 applyBatch 相同，额外输出按发生顺序排列的行级变更。
// seq 为目标快照序号；只有真正改变行状态的变更才会进入日志：
//   - upsert 作用于不存在的行  -> insert；
//   - upsert 作用于已存在的行且值发生变化 -> update；值完全相同则不产生事件；
//   - delete 命中已存在的行    -> delete（墓碑，只带主键）；
//     删除不存在的行不产生事件。
//
// 日志顺序即变更在批次中的出现顺序；同一主键在同批次内多次变更会产生
// 多条事件，与最终快照数据严格一致（可重放审计）。
func applyBatchWithLog(base []Row, schema Schema, batch Batch, seq uint64) ([]Row, []RowChange) {
	idx := map[string]int{}
	merged := make([]Row, 0, len(base)+len(batch.Changes))
	for _, r := range base {
		pk, ok := r[schema.PrimaryKey]
		if !ok {
			continue
		}
		idx[pkKey(pk)] = len(merged)
		merged = append(merged, r)
	}
	var log []RowChange
	order := 0
	emit := func(kind RowChangeKind, pk Value, after Row) {
		log = append(log, RowChange{
			Seq: seq, Order: order, Kind: kind, PK: pk, Row: after,
		})
		order++
	}
	for _, ch := range batch.Changes {
		switch {
		case ch.Upsert != nil:
			row := projectRow(*ch.Upsert, schema) // 补齐缺省列
			pk := row[schema.PrimaryKey]
			k := pkKey(pk)
			if i, ok := idx[k]; ok {
				if !rowsEqual(merged[i], row, schema) {
					emit(RowUpdated, pk, row)
				}
				merged[i] = row
			} else {
				idx[k] = len(merged)
				merged = append(merged, row)
				emit(RowInserted, pk, row)
			}
		case ch.Delete != nil:
			k := pkKey(*ch.Delete)
			if i, ok := idx[k]; ok {
				emit(RowDeleted, *ch.Delete, nil)
				merged = append(merged[:i], merged[i+1:]...)
				// 重建索引（数据量小，保持实现直白）
				idx = map[string]int{}
				for j, r := range merged {
					idx[pkKey(r[schema.PrimaryKey])] = j
				}
			}
		}
	}
	return sortRows(merged, schema.PrimaryKey), log
}

// rowsEqual 按结构中全部列比较两行是否等价；数字按数值比较
// （JSON 反序列化会把 int64 读成 float64，不视为差异）。
func rowsEqual(a, b Row, schema Schema) bool {
	for _, c := range schema.Columns {
		if !valuesEqual(a[c.Name], b[c.Name]) {
			return false
		}
	}
	return true
}

func valuesEqual(x, y Value) bool {
	// 整型/浮点统一按 float64 数值比较。
	if xf, okx := numeric(x); okx {
		if yf, oky := numeric(y); oky {
			return xf == yf
		}
	}
	return x == y
}

func numeric(v Value) (float64, bool) {
	switch n := v.(type) {
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	case float64:
		return n, true
	}
	return 0, false
}

// validateRename 校验带数据的列改名迁移是否兼容：
//   - 新旧列名均非空且不相同；
//   - 旧列必须存在；主键列不允许改名；
//   - 新列名不得与任何既有列冲突（类型由旧列保留，不存在类型变更问题）。
func validateRename(s Schema, from, to string) error {
	if from == "" || to == "" {
		return fmt.Errorf("%w: rename column names must not be empty", ErrSchemaIncompatible)
	}
	if from == to {
		return fmt.Errorf("%w: rename target equals source %q", ErrSchemaIncompatible, from)
	}
	if from == s.PrimaryKey {
		return fmt.Errorf("%w: cannot rename primary key column %q", ErrSchemaIncompatible, from)
	}
	cols := map[string]bool{}
	found := false
	for _, c := range s.Columns {
		cols[c.Name] = true
		if c.Name == from {
			found = true
		}
	}
	if !found {
		return fmt.Errorf("%w: source column %q not found", ErrSchemaIncompatible, from)
	}
	if cols[to] {
		return fmt.Errorf("%w: target column %q already exists", ErrSchemaIncompatible, to)
	}
	return nil
}

// sortRows 按主键排序，保证读取结果可复现。
func sortRows(rows []Row, pk string) []Row {
	out := make([]Row, len(rows))
	copy(out, rows)
	sort.Slice(out, func(i, j int) bool {
		return pkKey(out[i][pk]) < pkKey(out[j][pk])
	})
	return out
}
