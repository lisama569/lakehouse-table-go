# lakehouse-table-go

用本地文件系统模拟数据文件与表元数据的表格式读写层，提供快照提交、
主键 upsert、增量读取与结构演进能力，保证并发写入、失败重试与
结构演进之后的数据结论一致且可解释。

## 存储布局

```
<root>/<table>/
  HEAD                  当前快照 ID（原子替换更新，读者只见完整快照）
  LOCK                  提交临界区使用的 flock 锁文件
  schemas/v<N>.json     各版本表结构（列含稳定 ID）
  snapshots/NNNNNN.json 快照元数据（父快照、批次 ID、行数、校验和、变更流水）
  data/NNNNNN.jsonl     快照全量数据（写时复制，按列 ID 存储）
```

## 提交与快照语义

- **快照为单位提交**：`Commit(base, batch)` 把一批变更物化为新快照
  `base+1`。提交过程先写数据文件、再写快照元数据、最后原子替换
  `HEAD`，因此任何时刻的读取只能看到完整快照，不会看到部分生效的
  中间状态（`TestSnapshotIsolationDuringCommit` 验证）。
- **乐观并发控制**：`base` 必须等于当前 `HEAD`，否则提交被拒绝并返回
  可区分的 `ErrCommitConflict`（可用 `errors.Is` 判定）。冲突方刷新
  `base` 后重试即可，不会产生重复行。临界区由表目录上的 `flock` 保护，
  检查与落盘是原子的。
- **幂等提交**：每个批次携带唯一 `Batch.ID`。若该批次已被应用，
  `Commit` 直接返回既有快照而不产生新快照——失败后的重试与重放
  既不产生重复行也不丢失更新，行数与校验和保持不变
  （`TestIdempotentReplay` 验证）。
- **主键 upsert 合并规则**：以父快照数据为底，按批次内记录顺序应用，
  同一主键后者覆盖前者（`OpDelete` 删除）；跨批次按快照顺序
  last-writer-wins。规则确定且可复现。
- **增量读取**：`Changes(from, to]` 按快照顺序返回区间内的变更流水。
- **可校验性**：每个快照记录行数与数据文件的 SHA-256 校验和，
  可用于重放前后的一致性比对。

## 结构兼容规则

数据文件按**列 ID**（而非列位置）存储，列 ID 在表生命周期内稳定：

- **新增列**：分配新列 ID；旧数据读出时该列为 `nil`（确定的缺省语义）。
- **删除列**：仅从结构中移除，底层数据保留但不再可读，不会错位。
- **重命名列**：列 ID 不变，仅改名字，新旧快照各自按当时的名字读出
  同一份数据，不会串列。
- **不允许**修改既有列类型；新增已存在的列名、删除/重命名不存在的
  列名都会被拒绝并返回 `ErrSchemaIncompatible`。
- 结构演进本身也是一次快照提交（`EvolveSchema`，幂等键为
  `ChangeID`），只推进结构版本、不改动数据，演进前后数据校验和一致。
- 旧快照永久可读：`Scan(snapID)` 总是用该快照对应的结构版本解读数据，
  结果可复现。

## 可区分的失败原因

| 错误 | 含义 |
|---|---|
| `ErrCommitConflict` | 提交的 base 快照已过期（并发冲突） |
| `ErrSchemaIncompatible` | 结构演进不兼容（重名、缺列等） |
| `ErrTableNotFound` / `ErrTableExists` | 表不存在 / 已存在 |
| `ErrSnapshotNotFound` | 快照不存在 |
| `ErrUnknownColumn` | 记录引用了结构中不存在的列 |

## 本地验证

```bash
go build ./...        # 编译
go test ./... -v      # 运行测试，日志中打印快照标识与变更流水
go test -race ./...   # 竞态检测
```

测试覆盖：

- `TestConcurrentCommitConflict`：8 个写入者并发提交同一 base，
  恰好 1 个成功，其余收到 `ErrCommitConflict`，重试后全部落库且无重复行。
- `TestSnapshotIsolationDuringCommit`：提交进行中并发读取，
  校验快照元数据与数据行数始终一致。
- `TestIdempotentReplay`：同一批次重放多次，快照 ID、行数、
  校验和均不变。
- `TestSchemaEvolution`：增/删/重命名列后的缺省语义、不错位读取、
  旧快照可复现读取，以及不兼容演进的可区分报错。
- `TestIncrementalRead`：增量变更流水的范围与顺序。
