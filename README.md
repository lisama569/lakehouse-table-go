# lakehouse-table-go

基于本地文件系统的快照式表格读写层：用本地目录模拟数据文件与表元数据，
保证并发写入、失败重试与结构演进之后的数据结论一致且可解释。

## 存储布局

```
<root>/<table>/
├── meta/
│   ├── HEAD                  # 当前快照 ID（原子替换写入）
│   ├── snapshots/<id>.json # 快照元数据（不可变）
│   └── batches/<batchID>   # 已提交批次标记（幂等依据）
└── data/<file>.json        # 快照数据文件（不可变）
```

## 提交与快照语义

- **快照为单位提交**：`Commit(expectParent, batch)` 把一批变更（upsert/delete）
  合并为一个新快照。提交过程先写数据文件与快照元数据，最后 CAS 推进 `HEAD`；
  读取只触碰 `HEAD` 或历史快照，因此**任何时刻都看不到部分生效的中间状态**。
- **乐观并发控制**：`expectParent` 必须等于当前 `HEAD`，否则提交被拒绝并返回
  `ErrCommitConflict`。多个写入者并发基于同一父快照提交时，恰好一个成功，
  其余收到冲突错误，可用新 `HEAD` 重试。
- **可区分的失败原因**（均可用 `errors.Is` 判定）：
  | 错误 | 含义 |
  |---|---|
  | `ErrCommitConflict` | 父快照已过期（并发冲突） |
  | `ErrDuplicateBatch` | 同一 `BatchID` 已提交（幂等重放，数据不变） |
  | `ErrSchemaIncompatible` | 结构演进不兼容 |
  | `ErrInvalidChange` | 变更内容非法（缺主键、类型不符等） |
  | `ErrTableNotFound` / `ErrTableExists` | 表不存在 / 已存在 |
- **幂等与重试**：每个批次携带唯一 `BatchID`。已提交的批次重放时返回
  `ErrDuplicateBatch` 且不改动数据——失败重试不会产生重复行或丢失更新，
  重放前后行数与校验和一致（测试中以 FNV 校验和验证）。
- **确定性合并规则**：行按主键标识；upsert 覆盖同主键整行，delete 删除；
  同批次同主键多次出现时后者覆盖前者；upsert 缺失列按结构缺省值补齐；
  快照内行按主键排序，读取结果可复现。
- **增量读取**：`ReadSince(seq)` 返回序号大于 `seq` 的快照流水（含快照 ID、
  父快照、批次 ID），可用于增量消费与审计。

## 结构兼容规则

`EvolveSchema(newSchema)` 生成一个新快照，数据按新结构重写：

- **新增列**：允许。旧数据该行填列的 `Default` 值（未设置则为 NULL）。
- **删除列**：允许。该列数据被丢弃，读取方不会看到错位或串列的数据。
- **重命名列**：视为"删除旧列 + 新增新列"，新列按缺省值填充；
  如需保留数据，应显式迁移。
- **不允许**：修改主键、修改保留列的类型（返回 `ErrSchemaIncompatible`）。
- **旧快照可读**：历史快照各自携带当时的结构，结构变更后读取旧快照
  结果不变且可复现。

## 本地验证

```bash
go build ./...          # 编译
go test ./table/ -v     # 运行测试，日志打印快照标识与变更流水
go test ./... -race     # 竞态检测
```

测试覆盖：

- `TestConcurrentCommitConflict`：8 个写入者并发提交同一父快照，
  恰好 1 个成功、7 个收到 `ErrCommitConflict`，冲突者重试后全部落库；
- `TestIdempotentReplay`：同一批次重放返回 `ErrDuplicateBatch`，
  行数与校验和保持不变；
- `TestSchemaEvolution`：新增列缺省值、删除/重命名列不串列、
  旧快照可复现读取、不兼容演进被拒绝；
- `TestReadSince`：增量读取返回完整变更流水。
