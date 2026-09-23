package table

import "fmt"

// validateSchema 校验结构定义本身合法。
func validateSchema(s Schema) error {
	if s.PrimaryKey == "" {
		return fmt.Errorf("%w: primary key is empty", ErrSchemaIncompatible)
	}
	seen := map[string]bool{}
	pkFound := false
	for _, c := range s.Columns {
		if c.Name == "" {
			return fmt.Errorf("%w: column name is empty", ErrSchemaIncompatible)
		}
		if seen[c.Name] {
			return fmt.Errorf("%w: duplicate column %q", ErrSchemaIncompatible, c.Name)
		}
		seen[c.Name] = true
		if c.Name == s.PrimaryKey {
			pkFound = true
		}
	}
	if !pkFound {
		return fmt.Errorf("%w: primary key %q not in columns", ErrSchemaIncompatible, s.PrimaryKey)
	}
	return nil
}

// checkEvolution 校验从 old 到 new 的演进是否兼容：
// 允许新增列、删除列、重命名（体现为删一增一，按名称对齐）；
// 不允许删除主键、不允许保留列的类型变更。
func checkEvolution(old, newS Schema) error {
	if err := validateSchema(newS); err != nil {
		return err
	}
	if newS.PrimaryKey != old.PrimaryKey {
		return fmt.Errorf("%w: primary key changed from %q to %q",
			ErrSchemaIncompatible, old.PrimaryKey, newS.PrimaryKey)
	}
	oldCols := map[string]Column{}
	for _, c := range old.Columns {
		oldCols[c.Name] = c
	}
	for _, c := range newS.Columns {
		if oc, ok := oldCols[c.Name]; ok && oc.Type != c.Type {
			return fmt.Errorf("%w: column %q type changed %s -> %s",
				ErrSchemaIncompatible, c.Name, oc.Type, c.Type)
		}
	}
	return nil
}

// projectRow 把一行旧结构数据映射到新结构：
//   - 新结构中保留的列：拷贝原值；
//   - 新结构中新增的列：填列缺省值（nil 即 NULL）；
//   - 新结构中删除的列：丢弃，不会错位串列。
func projectRow(row Row, newS Schema) Row {
	out := make(Row, len(newS.Columns))
	for _, c := range newS.Columns {
		if v, ok := row[c.Name]; ok {
			out[c.Name] = v
		} else {
			out[c.Name] = c.Default
		}
	}
	return out
}

// validateChange 校验单条变更与结构匹配。
func validateChange(s Schema, ch Change) error {
	switch {
	case ch.Upsert != nil:
		row := *ch.Upsert
		pk, ok := row[s.PrimaryKey]
		if !ok || pk == nil {
			return fmt.Errorf("%w: upsert row missing primary key %q", ErrInvalidChange, s.PrimaryKey)
		}
		for _, c := range s.Columns {
			if v, ok := row[c.Name]; ok && v != nil && !typeMatches(c.Type, v) {
				return fmt.Errorf("%w: column %q expects %s, got %T", ErrInvalidChange, c.Name, c.Type, v)
			}
		}
		return nil
	case ch.Delete != nil:
		return nil
	default:
		return fmt.Errorf("%w: change has neither upsert nor delete", ErrInvalidChange)
	}
}

func typeMatches(t ColumnType, v Value) bool {
	switch t {
	case TypeString:
		_, ok := v.(string)
		return ok
	case TypeInt:
		switch v.(type) {
		case int64, int, float64: // JSON 数字反序列化为 float64
			return true
		}
		return false
	case TypeFloat:
		switch v.(type) {
		case int64, int, float64:
			return true
		}
		return false
	case TypeBool:
		_, ok := v.(bool)
		return ok
	}
	return false
}
