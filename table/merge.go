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

// valueEqual 比较两个单元格值是否相等。
// 数值做跨类型归一：基线行从 JSON 读回后整数变为 float64，
// 与提交方传入的 int64/int 应视为同值，否则原样重复提交会被误判为更新。
func valueEqual(a, b Value) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	if fa, ok := toNumber(a); ok {
		fb, ok2 := toNumber(b)
		return ok2 && fa == fb
	}
	return a == b
}

func toNumber(v Value) (float64, bool) {
	switch x := v.(type) {
	case int:
		return float64(x), true
	case int64:
		return float64(x), true
	case float64:
		return x, true
	}
	return 0, false
}

// rowsEqual 按结构逐列比较两行是否完全一致（两行均已按结构投影，列集合相同）。
func rowsEqual(a, b Row, schema Schema) bool {
	for _, c := range schema.Columns {
		if !valueEqual(a[c.Name], b[c.Name]) {
			return false
		}
	}
	return true
}

// changeRecords 依据（基线, 批次）推导本批次真实发生的行级变更明细，
// 顺序与变更在批次中的出现顺序一致，结果确定且可复现：
//   - Upsert 的主键在基线（含本批次此前变更）中不存在 → insert；
//     主键已存在且任一列值不同 → update；
//     主键已存在且所有列值完全相同（原样重复提交）→ 不产生记录；
//   - Delete 仅当主键存在时产生一条 delete 记录（删除不存在的行不算变更）；
//   - 记录中的 Row 为变更后的整行（缺省列已补齐）；delete 记录只带主键。
//
// 批次内同一主键多次出现时，逐条与"应用了本批次此前变更后的当前行"比较，
// 因此同值连写只留一条记录，改值连写按发生顺序各留一条。
func changeRecords(base []Row, schema Schema, batch Batch, snapID string, seq uint64) []ChangeRecord {
	current := map[string]Row{} // pkKey -> 当前行（基线 + 本批次已应用的变更）
	for _, r := range base {
		if pk, ok := r[schema.PrimaryKey]; ok {
			current[pkKey(pk)] = r
		}
	}
	var recs []ChangeRecord
	for _, ch := range batch.Changes {
		switch {
		case ch.Upsert != nil:
			row := projectRow(*ch.Upsert, schema)
			k := pkKey(row[schema.PrimaryKey])
			old, existed := current[k]
			if existed && rowsEqual(old, row, schema) {
				continue // 值没有任何变化：不是真实变更，不留记录
			}
			op := OpInsert
			if existed {
				op = OpUpdate
			}
			current[k] = row
			recs = append(recs, ChangeRecord{
				Seq: seq, SnapshotID: snapID, Op: op,
				Key: row[schema.PrimaryKey], Row: row,
			})
		case ch.Delete != nil:
			k := pkKey(*ch.Delete)
			if _, ok := current[k]; ok {
				delete(current, k)
				recs = append(recs, ChangeRecord{
					Seq: seq, SnapshotID: snapID, Op: OpDelete,
					Key: *ch.Delete,
				})
			}
		}
	}
	return recs
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
