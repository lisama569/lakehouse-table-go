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
  | `ErrTableNotFound` / `ErrTableExists` | 表不存在 / 已存在 | 检查表名 |
- **幂等与重试**：每个批次携带唯一 `BatchID`。已提交的批次重放时返回
  `ErrDuplicateBatch` 且不改动数据——失败重试不会产生重复行或丢失更新，
  重放前后行数与校验和一致（测试中以 FNV 校验和验证）。
- **确定性合并规则**：行按主键标识；upsert 覆盖同主键整行，delete 删除；
  同批次同主键多次出现时后者覆盖前者；upsert 缺失列按结构缺省值补齐；
  快照内行按主键排序，读取结果可复现。
- **增量读取（快照流水）**：`ReadSince(seq)` 返回序号大于 `seq` 的快照流水
  （含快照 ID、父快照、批次 ID）。
- **增量读取（行级变更明细）**：`ReadChangesSince(seq)` 在快照推进信息之外，
  返回这批快照中**真实发生**的行级变更记录 `ChangeRecord`：
  - `Op`：`insert` / `update` / `delete`；
  - `Key`：涉及行的主键值；
  - `Row`：变更后的整行列值（缺省列已补齐）；`delete` 记录不带 `Row`，
    以主键作删除标记（tombstone）；
  - **只记录真实变更**：upsert 仅当主键不存在（`insert`）或**任一列值确实
    发生变化**（`update`）时产生记录；一行按原样（列值一个都没改）重新提交，
    即使经过"落盘 → 再提交"的序列化往返（内存中的 `int64` 与落盘后的
    `float64` 按数值判等），也**不产生任何记录**；
  - 删除只有落在真实存在的行上才算数；删除不存在的行不留任何痕迹；
  - 同一批次内同一主键被写多次时，以每条变更发生时刻的当前行为准逐条判定，
    记录按发生顺序输出（如 `update → delete → insert` 会给出三条记录，
    而与上一条效果完全相同的重复 upsert 不再产生记录）；
  - 结构演进/列改名快照不产生行级记录（空明细）。
  记录按快照序号与批次内顺序稳定输出，同一读取重复执行结果一致；
  断点续读时传入已消费的最大 `Seq`，只返回未消费的部分，
  不会重复上报也不会漏报，可直接用于增量同步与审计。
- **并发建表（多实例唯一成功）**：多个服务实例、独立脚本或命令行工具在
  同一时刻 `CreateTable` 同名表时，由"原子目录占位（`mkdir meta`）+
  表级 flock 串行 + 持锁复查 HEAD"保证：**恰好一个真正建立成功并生效**，
  写入自己的结构与空初始数据；其余全部收到 `ErrTableExists`，且在持锁复查
  阶段就被拒绝，**不会写入或顶掉**先建成功者的任何结构与数据。无论并发
  多少个、谁先谁后，最终该表只有一份结构和初始快照（`snap-000000`），
  自洽且可复现。若先到者在写 HEAD 前异常退出留下半成品目录，后来者会持锁
  接手完成建表，不会卡死在残留目录上。
  `ErrTableExists` 与下列失败原因彼此可区分（均可用 `errors.Is` 判定，
  调用方据此判断是重试、改数据还是放弃）：
  建表前的结构定义不合法是 `ErrSchemaIncompatible`；普通提交冲突是
  `ErrCommitConflict`（父快照过期，应基于新 HEAD 重试）；变更数据非法是
  `ErrInvalidChange`；批次重放是 `ErrDuplicateBatch`；表已存在是
  `ErrTableExists`（结构已固定，不应重试覆盖，应改为 `OpenTable`）。

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
`[change-feed]` / `[real-change]` / `[create-race]` 等场景前缀，并打印涉及
的快照 ID 与变更明细）：

- `TestChangeFeedOnlyRealChanges`：增量明细只记录真实变更——原样重复提交
  （含 `int64` 落盘为 `float64` 的序列化往返）产生 0 条记录且数据校验和不变；
  只改一列仍正常报 1 条 update；同批同主键多次写入（update→重复→delete→
  重插）只按真实效果输出 3 条、顺序正确；删不存在的行无痕迹；全量流水内容
  与顺序符合预期、重复读取一致、断点续读不重不漏；
- `TestConcurrentCreateTable` / `TestCrossProcessConcurrentCreate`：10 个
  进程内实例句柄、6 个**独立子进程**同时建同一张表，均验证恰好 1 个成功、
  其余全部收到与冲突/结构/数据类错误**可区分**的 `ErrTableExists`；
  最终结构只属于胜者（列名/缺省值标记可辨识）、初始数据为空、只有
  `snap-000000` 一个快照；败者事后重试仍被拒绝且结构不被改写；
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
