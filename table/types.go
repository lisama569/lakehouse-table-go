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

// RowChangeKind 行级变更类型。
type RowChangeKind string

const (
	// RowInserted 新增行。
	RowInserted RowChangeKind = "insert"
	// RowUpdated 更新已有行。
	RowUpdated RowChangeKind = "update"
	// RowDeleted 删除行（墓碑事件，只带主键）。
	RowDeleted RowChangeKind = "delete"
)

// ColumnRename 记录一次带数据的列改名迁移。
type ColumnRename struct {
	From string `json:"from"`
	To   string `json:"to"`
}

// RowChange 快照内的一条行级变更（按发生顺序编号）。
type RowChange struct {
	Seq   uint64        `json:"seq"`           // 所属快照序号
	Order int           `json:"order"`         // 在快照内的发生顺序
	Kind  RowChangeKind `json:"kind"`          // insert / update / delete
	PK    Value         `json:"pk"`            // 主键值
	Row   Row           `json:"row,omitempty"` // 变更后的整行；delete 时为空
}

// SnapshotChanges 一次增量读取单元：快照推进信息 + 该快照的行级变更。
type SnapshotChanges struct {
	Snapshot Snapshot    `json:"snapshot"`
	Changes  []RowChange `json:"changes"`
}

// Snapshot 一个不可变快照的元数据。
type Snapshot struct {
	ID         string        `json:"id"`
	Seq        uint64        `json:"seq"`
	ParentID   string        `json:"parent_id"`
	Schema     Schema        `json:"schema"`
	DataFile   string        `json:"data_file"`
	ChangeFile string        `json:"change_file,omitempty"` // 行级变更流水文件
	BatchID    string        `json:"batch_id"`              // 产生本快照的批次；结构演进时为空
	Rename     *ColumnRename `json:"rename,omitempty"`      // 带数据列改名迁移标记
	CreatedAt  int64         `json:"created_at"`
}
