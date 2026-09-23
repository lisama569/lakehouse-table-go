package table

import "errors"

// 可区分的失败原因，调用方可用 errors.Is 判定。
var (
	// ErrCommitConflict 提交基于的父快照已过期（乐观并发冲突）。
	ErrCommitConflict = errors.New("commit conflict: parent snapshot is stale")
	// ErrDuplicateBatch 同一 BatchID 已提交过（幂等重放，非错误数据）。
	ErrDuplicateBatch = errors.New("duplicate batch: already committed")
	// ErrSchemaIncompatible 结构演进不兼容（如删除主键、类型变更）。
	ErrSchemaIncompatible = errors.New("schema incompatible")
	// ErrTableNotFound 表不存在。
	ErrTableNotFound = errors.New("table not found")
	// ErrTableExists 表已存在。
	ErrTableExists = errors.New("table already exists")
	// ErrInvalidChange 变更内容非法（缺主键、类型不符等）。
	ErrInvalidChange = errors.New("invalid change")
)
