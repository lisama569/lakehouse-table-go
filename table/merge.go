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

// sortRows 按主键排序，保证读取结果可复现。
func sortRows(rows []Row, pk string) []Row {
	out := make([]Row, len(rows))
	copy(out, rows)
	sort.Slice(out, func(i, j int) bool {
		return pkKey(out[i][pk]) < pkKey(out[j][pk])
	})
	return out
}
