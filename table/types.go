// Package table 提供基于本地存储的快照式表格读写层。
package table

// Value 是单元格的值，当前支持 string / int64 / float64 / bool / nil。
type Value = any

// Row 表示一行数据，键为列名。
type Row map[string]Value

// ColumnType 列类型。
type ColumnType string

const (
	TypeString ColumnType = "string"
	TypeInt    ColumnType = "int"
	TypeFloat  ColumnType = "float"
	TypeBool   ColumnType = "bool"
)

// Column 表结构中的一列。
type Column struct {
	Name    string     `json:"name"`
	Type    ColumnType `json:"type"`
	Default Value      `json:"default,omitempty"` // 新增列的缺省值；nil 表示 NULL
}

// Schema 表结构：主键 + 有序列集合。
type Schema struct {
	PrimaryKey string   `json:"primary_key"`
	Columns    []Column `json:"columns"`
}

// Change 单条变更：Upsert 与 Delete 二选一。
type Change struct {
	Upsert *Row   `json:"upsert,omitempty"`
	Delete *Value `json:"delete,omitempty"` // 主键值
}

// Batch 一次提交的一批变更，BatchID 用于幂等重放。
type Batch struct {
	BatchID string   `json:"batch_id"`
	Changes []Change `json:"changes"`
}

// Snapshot 一个不可变快照的元数据。
type Snapshot struct {
	ID        string `json:"id"`
	Seq       uint64 `json:"seq"`
	ParentID  string `json:"parent_id"`
	Schema    Schema `json:"schema"`
	DataFile  string `json:"data_file"`
	BatchID   string `json:"batch_id"` // 产生本快照的批次；结构演进时为空
	CreatedAt int64  `json:"created_at"`
}

// ChangeOp 行级变更类型。
type ChangeOp string

const (
	OpInsert ChangeOp = "insert" // 新增行
	OpUpdate ChangeOp = "update" // 覆盖既有行
	OpDelete ChangeOp = "delete" // 删除行
)

// ChangeRecord 一条真实发生的行级变更记录，随快照持久化，按发生顺序输出。
type ChangeRecord struct {
	Seq        uint64   `json:"seq"`
	SnapshotID string   `json:"snapshot_id"`
	Op         ChangeOp `json:"op"`
	Key        Value    `json:"key"`           // 涉及行的主键值
	Row        Row      `json:"row,omitempty"` // 变更后的整行列值；删除时为空（按主键作删除标记）
}
