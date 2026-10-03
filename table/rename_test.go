package table

import (
	"errors"
	"fmt"
	"testing"
)

// TestRenameColumn 带数据的列改名迁移：
// 新列名读到改名前的历史值、其他列不受影响、按主键读新旧快照对得上；
// 旧快照仍按当时结构与值稳定读取；不兼容改动（动主键/新名冲突）被拒绝。
func TestRenameColumn(t *testing.T) {
	tbl := newTestTable(t)
	head, _ := tbl.Head()

	seed := Batch{BatchID: "rename-seed", Changes: []Change{
		{Upsert: &Row{"id": "a", "name": "alice", "score": int64(10)}},
		{Upsert: &Row{"id": "b", "name": "bob", "score": int64(20)}},
	}}
	snapBefore, err := tbl.Commit(head, seed)
	if err != nil {
		t.Fatal(err)
	}
	logBatch(t, "改名前种子数据", snapBefore, seed)

	rowsBefore, err := tbl.Read(snapBefore)
	if err != nil {
		t.Fatal(err)
	}
	beforeByPK := map[string]Row{}
	for _, r := range rowsBefore {
		beforeByPK[r["id"].(string)] = r
	}
	schemaBefore, _ := tbl.SchemaAt(snapBefore)
	t.Logf("场景: 列改名 name -> display_name，旧快照=%s 旧列数=%d",
		snapBefore, len(schemaBefore.Columns))

	// 执行带数据改名。
	snapRename, err := tbl.RenameColumn("name", "display_name")
	if err != nil {
		t.Fatalf("rename column: %v", err)
	}
	t.Logf("改名迁移完成: 新快照=%s rename=%v", snapRename, "")

	// 新快照结构：列类型保留、列数不变、旧列名消失、新列名出现。
	schemaAfter, err := tbl.SchemaAt("")
	if err != nil {
		t.Fatal(err)
	}
	colType := map[string]ColumnType{}
	for _, c := range schemaAfter.Columns {
		colType[c.Name] = c.Type
	}
	if colType["display_name"] != TypeString {
		t.Fatalf("renamed column type not preserved: %+v", schemaAfter.Columns)
	}
	if _, ok := colType["name"]; ok {
		t.Fatal("old column name still present after rename")
	}
	if len(schemaAfter.Columns) != len(schemaBefore.Columns) {
		t.Fatalf("rename must not change column count: %d -> %d",
			len(schemaBefore.Columns), len(schemaAfter.Columns))
	}

	// 新列名读到改名前的历史值；同主键其他列完全不受影响。
	rowsAfter, err := tbl.Read(snapRename)
	if err != nil {
		t.Fatal(err)
	}
	if len(rowsAfter) != len(rowsBefore) {
		t.Fatalf("row count changed across rename: %d -> %d", len(rowsBefore), len(rowsAfter))
	}
	for _, r := range rowsAfter {
		pk := r["id"].(string)
		old := beforeByPK[pk]
		if r["display_name"] != old["name"] {
			t.Fatalf("renamed value not carried over for %s: %v != %v",
				pk, r["display_name"], old["name"])
		}
		// 其他列不受影响（score 保持；主键保持）。
		if r["id"] != old["id"] || !valuesEqual(r["score"], old["score"]) {
			t.Fatalf("other columns changed for %s: old=%+v new=%+v", pk, old, r)
		}
		if _, ok := r["name"]; ok {
			t.Fatalf("stale old column value present: %+v", r)
		}
		t.Logf("主键=%s 改名前后对得上: name=%v -> display_name=%v, score=%v",
			pk, old["name"], r["display_name"], r["score"])
	}

	// 旧快照仍按当时结构与值稳定、可复现读取。
	oldRows1, _ := tbl.Read(snapBefore)
	oldRows2, _ := tbl.Read(snapBefore)
	if checksum(t, oldRows1) != checksum(t, rowsBefore) ||
		checksum(t, oldRows2) != checksum(t, rowsBefore) {
		t.Fatal("old snapshot regressed after rename")
	}
	oldSchema, _ := tbl.SchemaAt(snapBefore)
	if _, hasOldName := colOf(oldSchema, "name"); !hasOldName {
		t.Fatal("old snapshot schema must keep the pre-rename column name")
	}
	t.Logf("旧快照=%s 读取不回归，仍含旧列 name、值可复现", snapBefore)

	// 改名后按新结构继续写入；新列名生效，旧列名数据被视为非法列（允许携带但不存储）。
	headNow, _ := tbl.Head()
	b := Batch{BatchID: "post-rename", Changes: []Change{
		{Upsert: &Row{"id": "c", "display_name": "carol", "score": int64(30)}},
	}}
	snapPost, err := tbl.Commit(headNow, b)
	if err != nil {
		t.Fatal(err)
	}
	logBatch(t, "改名后按新结构写入", snapPost, b)
	rowsPost, _ := tbl.Read("")
	rowC := findRow(rowsPost, "c")
	if rowC == nil || rowC["display_name"] != "carol" {
		t.Fatalf("post-rename insert wrong: %+v", rowC)
	}
	if _, ok := rowC["name"]; ok {
		t.Fatalf("writing old column name after rename must not resurrect it: %+v", rowC)
	}

	// 改名后再做普通结构演进（新增列），应继续工作，且不影响改名结果。
	schemaNow, _ := tbl.SchemaAt("")
	evolved := *schemaNow
	evolved.Columns = append(evolved.Columns, Column{Name: "active", Type: TypeBool, Default: true})
	snapEvo, err := tbl.EvolveSchema(evolved)
	if err != nil {
		t.Fatalf("evolve after rename: %v", err)
	}
	evoRows, _ := tbl.Read(snapEvo)
	rowA := findRow(evoRows, "a")
	if rowA["display_name"] != "alice" || rowA["active"] != true {
		t.Fatalf("rename values must survive a later evolution: %+v", rowA)
	}
	t.Logf("改名后再演进新增列成功: snapshot=%s display_name 值连续", snapEvo)

	// --- 不兼容改动被明确拒绝，错误可区分 ---
	// 主键列不允许改名。
	if _, err := tbl.RenameColumn("id", "ident"); !errors.Is(err, ErrSchemaIncompatible) {
		t.Fatalf("rename primary key must fail with ErrSchemaIncompatible, got %v", err)
	}
	// 源列不存在。
	if _, err := tbl.RenameColumn("nope", "x"); !errors.Is(err, ErrSchemaIncompatible) {
		t.Fatalf("rename missing source must fail with ErrSchemaIncompatible, got %v", err)
	}
	// 目标列名冲突（score 仍存在）。
	if _, err := tbl.RenameColumn("score", "display_name"); !errors.Is(err, ErrSchemaIncompatible) {
		t.Fatalf("rename to existing column must fail with ErrSchemaIncompatible, got %v", err)
	}
	// 新旧同名。
	if _, err := tbl.RenameColumn("score", "score"); !errors.Is(err, ErrSchemaIncompatible) {
		t.Fatalf("rename to same name must fail with ErrSchemaIncompatible, got %v", err)
	}
	// 动主键 / 改既有列类型仍然走 EvolveSchema 被拒绝。
	badType := *schemaNow
	for i := range badType.Columns {
		if badType.Columns[i].Name == "score" {
			badType.Columns[i].Type = TypeString
		}
	}
	if _, err := tbl.EvolveSchema(badType); !errors.Is(err, ErrSchemaIncompatible) {
		t.Fatalf("type change must fail with ErrSchemaIncompatible, got %v", err)
	}
	t.Logf("不兼容改动均被拒绝: %s（动主键/缺源列/目标冲突/同名/改类型）",
		fmt.Sprintf("ErrSchemaIncompatible"))
}

func colOf(s *Schema, name string) (Column, bool) {
	for _, c := range s.Columns {
		if c.Name == name {
			return c, true
		}
	}
	return Column{}, false
}
