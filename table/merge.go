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

// changeRecords 依据（基线, 批次）推导本批次真实发生的行级变更明细，
// 顺序与变更在批次中的出现顺序一致，结果确定且可复现：
//   - Upsert 的主键在基线（含本批次此前变更）中不存在 → insert；
//     已存在但任一列值与当前行不同 → update；值完全相同 → 不产生记录；
//   - Delete 仅当主键当前存在时产生一条 delete 记录（删除不存在的行不算变更）；
//   - 同一批次内同一主键被写多次时，以"变更发生时刻的当前行"为准逐条判定，
//     记录按发生顺序输出；
//   - 记录中的 Row 为变更后的整行（缺省列已补齐）；delete 记录只带主键。
func changeRecords(base []Row, schema Schema, batch Batch, snapID string, seq uint64) []ChangeRecord {
	// 当前行（含本批次此前变更）的最新值，用于判定本次 Upsert 是否真的改变了数据。
	current := map[string]Row{}
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
			prev, existed := current[k]
			if !existed {
				recs = append(recs, ChangeRecord{
					Seq: seq, SnapshotID: snapID, Op: OpInsert,
					Key: row[schema.PrimaryKey], Row: row,
				})
			} else if !rowsEqual(prev, row, schema) {
				recs = append(recs, ChangeRecord{
					Seq: seq, SnapshotID: snapID, Op: OpUpdate,
					Key: row[schema.PrimaryKey], Row: row,
				})
			}
			// 值相同则为原样重复提交：不产生任何记录，当前行也无需变化。
			current[k] = row
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

// rowsEqual 判定两行在给定结构的每一列上值是否相同。
// 基线行来自 JSON 反序列化（整型列值为 float64），提交行是内存中的 int64，
// 因此数值按数学值比较，避免序列化往返造成的假变更。
func rowsEqual(a, b Row, schema Schema) bool {
	for _, c := range schema.Columns {
		if !valuesEqual(a[c.Name], b[c.Name]) {
			return false
		}
	}
	return true
}

// valuesEqual 判定两个单元格值是否相等；数值类型（int/int64/float64）按数值比较。
func valuesEqual(a, b Value) bool {
	an, aok := numericValue(a)
	bn, bok := numericValue(b)
	if aok && bok {
		return an == bn
	}
	return a == b
}

// numericValue 把整型/浮点型值统一为 float64 比较；非数值返回 ok=false。
func numericValue(v Value) (float64, bool) {
	switch x := v.(type) {
	case int:
		return float64(x), true
	case int64:
		return float64(x), true
	case float64:
		return x, true
	default:
		return 0, false
	}
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
