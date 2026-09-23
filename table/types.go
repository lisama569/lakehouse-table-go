// Package table 提供基于本地文件系统的表格式读写层：
// 快照提交、主键 upsert、增量读取与结构演进。
package table

import (
	"errors"
	"time"
)

// 可区分的失败原因。
var (
	// ErrTableNotFound 表不存在。
	ErrTableNotFound = errors.New("table: not found")
	// ErrTableExists 表已存在。
	ErrTableExists = errors.New("table: already exists")
	// ErrSnapshotNotFound 快照不存在。
	ErrSnapshotNotFound = errors.New("table: snapshot not found")
	// ErrCommitConflict 提交基于的父快照已过期（并发冲突）。
	ErrCommitConflict = errors.New("table: commit conflict, base snapshot is stale")
	// ErrSchemaIncompatible 结构演进不兼容（类型变更等）。
	ErrSchemaIncompatible = errors.New("table: incompatible schema evolution")
	// ErrUnknownColumn 记录引用了结构中不存在的列。
	ErrUnknownColumn = errors.New("table: record references unknown column")
)

// DataType 列类型。
type DataType string

const (
	TypeString  DataType = "string"
	TypeInt64   DataType = "int64"
	TypeFloat64 DataType = "float64"
	TypeBool    DataType = "bool"
)

// Column 列定义。ID 在表生命周期内稳定，重命名只改 Name，
// 数据文件按列 ID 存储，避免删除/重命名后读错位。
type Column struct {
	ID   int      `json:"id"`
	Name string   `json:"name"`
	Type DataType `json:"type"`
}

// Schema 某一版本的结构。
type Schema struct {
	Version int      `json:"version"`
	Columns []Column `json:"columns"`
}

// ColumnByID 按 ID 查列。
func (s Schema) ColumnByID(id int) (Column, bool) {
	for _, c := range s.Columns {
		if c.ID == id {
			return c, true
		}
	}
	return Column{}, false
}

// ColumnByName 按名查列。
func (s Schema) ColumnByName(name string) (Column, bool) {
	for _, c := range s.Columns {
		if c.Name == name {
			return c, true
		}
	}
	return Column{}, false
}

// Op 变更操作类型。
type Op string

const (
	OpUpsert Op = "upsert"
	OpDelete Op = "delete"
)

// Record 一条变更。Values 以列 ID 为键。
type Record struct {
	Op     Op            `json:"op"`
	Key    string        `json:"key"`
	Values map[int]any `json:"values,omitempty"`
}

// Batch 一批以快照为单位提交的变更。ID 为幂等键：
// 同一 ID 的批次重复提交不会产生重复行。
type Batch struct {
	ID      string   `json:"id"`
	Records []Record `json:"records"`
}

// Snapshot 一次提交产生的不可变快照。
type Snapshot struct {
	ID            int64    `json:"id"`
	ParentID      int64    `json:"parent_id"`
	SchemaVersion int      `json:"schema_version"`
	BatchID       string   `json:"batch_id"`
	DataFile      string   `json:"data_file"`
	RowCount      int      `json:"row_count"`
	Checksum      string   `json:"checksum"`
	Changes       []Record `json:"changes"`
	CreatedAt     time.Time `json:"created_at"`
}

// Row 读出的一行，以列名为键。
type Row map[string]any
