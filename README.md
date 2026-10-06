# lakehouse-table-go

基于本地文件系统的快照式表格读写层：用本地目录模拟数据文件与表元数据，
保证并发写入、失败重试与结构演进之后的数据结论一致且可解释。

## 存储布局

```
<root>/<table>/
├── meta/
│   ├── HEAD                  # 当前快照 ID（原子替换写入）
│   ├── LOCK                  # 跨进程提交互斥锁（flock）
│   ├── snapshots/<id>.json # 快照元数据（不可变）
│   ├── batches/<batchID>   # 已提交批次标记（幂等依据）
│   └── changes/<id>.json   # 快照对应的行级变更明细（不可变）
└── data/<file>.json        # 快照数据文件（不可变）
```

## 提交与快照语义

- **快照为单位提交**：`Commit(expectParent, batch)` 把一批变更（upsert/delete）
  合并为一个新快照。提交过程先写数据文件、行级变更明细与快照元数据，
  最后 CAS 推进 `HEAD`；读取只触碰 `HEAD` 或历史快照，
  因此**任何时刻都看不到部分生效的中间状态**。
- **乐观并发控制（含跨进程）**：`expectParent` 必须等于当前 `HEAD`，否则提交
  被拒绝并返回 `ErrCommitConflict`。提交临界区由进程内互斥锁 +
  `meta/LOCK` 上的 flock 双重串行化：多个服务实例、独立脚本或命令行工具
  同时打开同一张表并基于同一父快照提交时，**恰好一个成功**，其余收到
  `ErrCommitConflict`，可用新 `HEAD` 重试。重试方不会覆盖已成功的提交，
  也不会产生重复行或丢失更新（由主键合并 + 批次幂等共同保证）。
- **可区分的失败原因**（均可用 `errors.Is` 判定，调用方据此决定重试还是改数据）：
  | 错误 | 含义 | 建议动作 |
  |---|---|---|
  | `ErrCommitConflict` | 父快照已过期（并发冲突） | 基于最新 `HEAD` 重试 |
  | `ErrDuplicateBatch` | 同一 `BatchID` 已提交（幂等重放，数据不变） | 视为已成功，无需重试 |
  | `ErrSchemaIncompatible` | 结构演进/列改名不兼容 | 修改结构定义后重试 |
  | `ErrInvalidChange` | 变更内容非法（缺主键、类型不符等） | 修正数据后重试 |
  | `ErrTableExists` | 表已存在（建表竞态中落败） | 改用 `OpenTable` 打开现有表 |
  | `ErrTableNotFound` | 表不存在 | 检查表名或先建表 |
- **并发建表语义**：多个实例、脚本或命令行工具同时 `CreateTable` 同名表时，
  **恰好一个成功**，其余收到 `ErrTableExists`（与结构不兼容、数据非法、
  提交冲突均可区分）。存在性判定与初始快照落盘在同一个 flock 临界区内完成，
  被拒绝的一方不会改写已建好的结构与初始数据；无论并发多少、谁先谁后，
  最终表的结构与数据自洽（就是胜者那一份），落败方可改用 `OpenTable` 打开。
- **幂等与重试**：每个批次携带唯一 `BatchID`。已提交的批次重放时返回
  `ErrDuplicateBatch` 且不改动数据——失败重试不会产生重复行或丢失更新，
  重放前后行数与校验和一致（测试中以 FNV 校验和验证）。
- **确定性合并规则**：行按主键标识；upsert 覆盖同主键整行，delete 删除；
  同批次同主键多次出现时后者覆盖前者；upsert 缺失列按结构缺省值补齐；
  快照内行按主键排序，读取结果可复现。
- **增量读取（快照流水）**：`ReadSince(seq)` 返回序号大于 `seq` 的快照流水
  （含快照 ID、父快照、批次 ID）。
- **增量读取（行级变更明细）**：`ReadChangesSince(seq)` 在快照推进信息之外，
  返回这批快照中真实发生的行级变更记录 `ChangeRecord`：
  - `Op`：`insert` / `update` / `delete`；
  - `Key`：涉及行的主键值；
  - `Row`：变更后的整行列值（缺省列已补齐）；`delete` 记录不带 `Row`，
    以主键作删除标记（tombstone）；
  - **只记录真实变更**：把某一行按原样（列值一个都没改）再提交一遍，
    不产生任何记录；确实改了值的行（哪怕只改一列）仍报 `update`；
    删除只有落在真实存在的行上才产生记录，删不存在的行不留痕迹；
    结构演进/列改名快照不产生行级记录。
    （值比较按语义进行：JSON 读回的 `float64` 与提交时的 `int64` 同值，
    不会被误判为更新。）
  - 同批次同一主键连写时逐条与当前行比较：同值连写只留一条记录，
    改值连写按发生顺序各留一条；删除后原样写回记为 `delete` + `insert`。
  记录按快照序号与批次内顺序稳定输出，同一读取重复执行结果一致；
  断点续读时传入已消费的最大 `Seq`，只返回未消费的部分，
  不重报、不漏报，可直接用于增量同步与审计。

## 结构兼容规则

`EvolveSchema(newSchema)` 生成一个新快照，数据按新结构重写：

- **新增列**：允许。旧数据该行填列的 `Default` 值（未设置则为 NULL）。
- **删除列**：允许。该列数据被丢弃，读取方不会看到错位或串列的数据。
- **重命名列（不带数据）**：在 `EvolveSchema` 中视为"删除旧列 + 新增新列"，
  新列按缺省值填充；如需保留数据，请使用下面的 `RenameColumn`。
- **不允许**：修改主键、修改保留列的类型（返回 `ErrSchemaIncompatible`）。
- **旧快照可读**：历史快照各自携带当时的结构，结构变更后读取旧快照
  结果不变且可复现。

## 带数据的列改名迁移

`RenameColumn(from, to)` 生成一个新快照，把一列改名并保留全部历史值：

- 新列名下能读到改名前的历史值，同一行的其他列完全不受影响，
  迁移前后按主键读出的数据一一对应；
- 列在结构中的位置与类型随列迁移，保持不变；
- 改名之后读取变更前的旧快照，仍按当时的结构（旧列名）和值给出结果，
  稳定且可复现；
- 兼容范围：仅允许改**非主键**列，且新列名未被占用；
  改主键列、改成已存在的列名、改不存在的列、以及任何既有列的类型变更，
  均返回 `ErrSchemaIncompatible`（与 `ErrCommitConflict` 可区分）。

## 本地验证

```bash
go build ./...          # 编译
go test ./table/ -v     # 运行测试，日志按场景打印快照标识与变更流水
go test ./... -race     # 竞态检测
```

测试覆盖（日志中带 `[cross-process]` / `[multi-instance]` / `[rename]` /
`[change-feed]` / `[noop-upsert]` / `[create-race]` / `[create-race-xproc]`
等场景前缀，并打印涉及的快照 ID 与变更明细）：

- `TestNoOpUpsertNoChangeRecord`：增量明细只反映真实变更——
  原样重复提交（列值一个都没改，含 JSON 数值类型差异）不产生任何记录、
  数据校验和不变；只改一列仍正常报 `update`；同批次同主键同值连写只留一条、
  改值连写按序各留一条；删除不存在的行不留痕迹、删除后原样写回记为
  `delete` + `insert`；重复读取一致、断点续读只返回未消费部分；
- `TestConcurrentCreateTable`：8 个实例并发 `CreateTable` 同名表（各带不同
  标记列的结构），恰好 1 个成功、7 个收到可区分的 `ErrTableExists`，
  最终结构就是胜者那一份、初始数据自洽，事后再建仍被拒绝且不改写数据；
- `TestCrossProcessConcurrentCreateTable`：5 个**独立子进程**同时创建同一张表，
  同样验证唯一成功、可区分拒绝与最终结构/数据自洽；
- `TestCrossProcessConcurrentCommit`：6 个**独立子进程**同时打开同一张表、
  基于同一父快照提交，恰好 1 个成功、5 个收到 `ErrCommitConflict`
  （与结构/变更类错误可区分），冲突方基于最新 HEAD 重试后全部落库，
  最终每行恰好出现一次、先成功者未被覆盖；
- `TestMultiInstanceConcurrentCommit`：8 个实例句柄（各自 `OpenTable`）
  并发提交，同样验证唯一成功、可区分拒绝与重试后数据完整无重复；
- `TestRenameColumnMigration`：列改名后新列名读到历史值、其他列不受影响、
  按主键对得上；旧快照读取不回归且可复现；改主键/改类型/目标列已存在
  等不兼容改动被 `ErrSchemaIncompatible` 拒绝；
- `TestChangeFeed`：行级变更明细内容完整（insert/update/delete + 主键 +
  变更后整行，删除为主键 tombstone）、顺序稳定、重复读取一致、
  断点续读只返回未消费部分，结构演进快照不产生行级记录；
- 原有用例 `TestConcurrentCommitConflict` / `TestIdempotentReplay` /
  `TestSchemaEvolution` / `TestReadSince` 全部保持通过，不回归。
