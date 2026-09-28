# workflow-orchestrator

基于 FastAPI + SQLite 的本地工作流调度与编排服务。支持依赖编排、并发控制、
失败重试与退避、执行超时、崩溃恢复、手动/定时触发、多调用方隔离、
**无业务标识时的并发收敛**、**按工作流配置的历史保留与有界清理**、
**清理后可区分的查询语义与可解释审计**，全部状态与执行历史持久化在本地
SQLite 中，无外部服务依赖。

## 运行与验证

```bash
uv sync                              # 安装依赖
uv run pytest tests/ -v              # 运行全部测试（日志含输入与判定依据）
uv run uvicorn main:app --port 8000  # 启动服务
```

快速体验（另开一个终端）：

```bash
H='-H "X-Owner-Id: alice" -H "Content-Type: application/json"'
# 注册工作流：a -> b(失败重试2次) -> c
curl -s -X POST localhost:8000/workflows -H 'X-Owner-Id: alice' \
  -H 'Content-Type: application/json' -d '{
    "name":"demo","tasks":[
      {"name":"a","handler":"echo"},
      {"name":"b","handler":"fail","depends_on":["a"],"max_attempts":2,"backoff_seconds":0.5},
      {"name":"c","handler":"echo","depends_on":["b"]}]}'
# 手动触发（幂等键去重）
curl -s -X POST localhost:8000/workflows/demo/runs -H 'X-Owner-Id: alice' \
  -H 'Content-Type: application/json' -d '{"idempotency_key":"run-1"}'
# 查询状态与执行历史
curl -s localhost:8000/runs/<run_id> -H 'X-Owner-Id: alice'
curl -s localhost:8000/runs/<run_id>/events -H 'X-Owner-Id: alice'

# 并发收敛：注册一个"同刻只允许一条活跃运行、后来者复用"的工作流
curl -s -X POST localhost:8000/workflows -H 'X-Owner-Id: alice' \
  -H 'Content-Type: application/json' -d '{
    "name":"single","tasks":[{"name":"a","handler":"sleep"}],
    "active_run_policy":"reuse"}'
curl -s -X POST localhost:8000/workflows/single/runs -H 'X-Owner-Id: alice'  # 201 created
curl -s -X POST localhost:8000/workflows/single/runs -H 'X-Owner-Id: alice'  # 200 reused

# 历史保留：只保留最近 2 条已结束运行，更新后立即清理一轮（或等后台守护线程）
curl -s -X PUT localhost:8000/workflows/demo/retention -H 'X-Owner-Id: alice' \
  -H 'Content-Type: application/json' -d '{"retain_last_runs":2}'
curl -s localhost:8000/runs/purged -H 'X-Owner-Id: alice'   # 被清理运行的墓碑（410 可查）
curl -s localhost:8000/audit -H 'X-Owner-Id: alice'        # 复用/拒绝/清理的可解释记录
```

内置处理器：`echo`（回显）、`sleep`（休眠，payload 传 seconds）、
`fail`（必失败，用于验证重试/失败传播）。自定义处理器通过
`HandlerRegistry.register(name, fn)` 注册，payload 含
`run_id / task_name / attempt / owner`，处理器应据此保证幂等。

## 调度语义

- **工作流定义**：任务含 `depends_on` 依赖列表。注册时校验：
  依赖不存在的任务 → `UNKNOWN_TASK`（422）；依赖成环 →
  `DEPENDENCY_CYCLE`（422）；同名工作流 → `DUPLICATE_WORKFLOW`（409）。
- **依赖调度**：无依赖任务可并行；任务在其全部前置 `SUCCEEDED` 后进入
  `READY` 等待执行槽位；任一前置进入失败类终态（FAILED/TIMED_OUT/
  SKIPPED/CANCELLED），本任务直接置 `SKIPPED` 终态，**不会被执行**。
- **并发上限**：全局信号量 `ORCH_MAX_CONCURRENCY`（默认 4）限制同时在执行的
  任务数；`UNIQUE(run_id, task_name)` + 状态机保证同一任务在同一次运行中
  不会被重复执行。
- **重复触发（幂等键）**：同一工作流下幂等键唯一（数据库唯一索引兜底，并发安全）。
  策略 `ORCH_DUP_POLICY=dedup`（默认）返回已有运行；`reject` 则拒绝并返回
  `DUPLICATE_RUN`（409）。
- **并发收敛（无业务去重标识也生效）**：每条工作流可用 `active_run_policy`
  单独配置收敛策略，未配置时取全局 `ORCH_DEFAULT_ACTIVE_POLICY`（默认 `allow`）：
  - `allow`：不收敛（历史默认语义，仅受积压上限约束，可并行多条活跃运行）；
  - `reuse`：同一时刻该工作流最多一条活跃运行（PENDING/RUNNING），后来的触发
    （无论有无幂等键、无论手动还是定时）**复用**那条活跃运行；
  - `reject`：后来的触发被明确拒绝，返回 `ACTIVE_RUN_CONFLICT`（409），
    错误体带 `active_run_id`/`active_status`，明确告知与哪条冲突。

  检查与插入在存储层单锁单事务内完成，并发窗口内不可能两条都创建成功。
  手动触发与定时触发撞车走同一套规则：定时触发在活跃窗口内复用或按槽跳过，
  结果与原因写入运行事件和审计记录。触发响应以 HTTP 状态码区分：新建 `201`、
  复用 `200`（响应体含 `trigger_outcome=created|reused`、`convergence_reason`）。
  活跃运行结束后，新的触发可正常创建新运行。
- **积压控制**：活跃运行（PENDING/RUNNING）达到 `ORCH_MAX_PENDING_RUNS`
  （默认 100）后，新触发被拒绝并返回 `QUEUE_FULL`（429），
  队列与状态存储不会无界增长。
- **定时触发**：工作流可设 `schedule_interval_seconds`，调度器按时间槽
  （`slot = now // interval`）生成幂等键 `sched-<wf>-<slot>` 触发，
  同一槽位不会重复触发；槽位触发失败（如积压）按策略跳过并记录事件。

## 状态机

- Run：`PENDING → RUNNING → SUCCEEDED / FAILED / CANCELLED`
- TaskRun：`PENDING → READY → RUNNING → SUCCEEDED / FAILED / TIMED_OUT`
  （另：`PENDING/READY → SKIPPED/CANCELLED`，`RUNNING → READY` 用于重试与恢复）

非法转移（如向已终态任务写回结果）会被状态机拒绝并记录
`LATE_RESULT_DISCARDED` 事件。

## 重试、超时与恢复

- **重试与退避**：任务失败时，若 `attempt < max_attempts`，回到 `READY`
  并按 `backoff_seconds * 2^(attempt-1)` 指数退避后重试；重试耗尽进入
  `FAILED` 终态。同一时刻同一任务只有一个尝试在执行，重试只在前次尝试
  明确结束后发起，不会造成重复副作用（处理器仍需按 attempt 幂等）。
- **超时**：单次执行超过 `timeout_seconds` 进入 `TIMED_OUT` 终态（与
  `FAILED` 可区分），不再重试；超时后迟到的线程结果会被状态机拒绝。
- **重启恢复**：服务启动时扫描仍处于 `RUNNING` 的任务（进程已死，必为
  中断）：剩余尝试充足则按退避重新入队，否则置 `FAILED`；`SUCCEEDED`
  任务保持不动、绝不重复触发；`PENDING/RUNNING` 的运行由调度循环继续
  推进。每条恢复决策写入 `RECOVERY` 事件，可由执行历史完整解释。

## 隔离边界

- 调用方以 `X-Owner-Id` 头标识身份（缺失 → 401）。
- 所有查询按 owner 过滤；访问他人运行/历史一律返回 404（不泄露存在性），
  取消等写操作同理拒绝。

## 历史保留策略（存储有上界）

每条工作流可单独配置保留策略（注册时或之后通过接口更新），持久化在工作流
定义里，**服务重启后原样生效**；未配置时可用全局默认
（`ORCH_DEFAULT_RETAIN_LAST_RUNS` / `ORCH_DEFAULT_RETAIN_SECONDS`）：

- `retain_last_runs`：只保留最近 N 条**已结束**的运行（按结束时间倒序）；
- `retain_seconds`：只保留结束时间在最近 N 秒内的运行（边界 `now-N` 上的保留）；
- 两者同时配置时取交集（更严格的生效）；都不配置表示该工作流不自动清理。

清理语义与安全保证：

- **只清理终态运行**（SUCCEEDED/FAILED/CANCELLED）。PENDING/RUNNING 的进行中
  运行及其未结束的依赖关系**绝不受影响**；删除语句也带终态过滤做双保险。
- 每次清理在**单个 SQLite 事务**内原子完成：运行、任务执行明细（task_runs）、
  执行历史（events）连同墓碑一起提交——要么整条运行连同明细全部清掉，要么本次
  清理整体回滚，绝不留半条记录。
- 清理跑在**独立的慢速守护线程**（默认每 1s 一轮），每轮按 `retention_batch_size`
  （默认 200）分批、每批独立短事务提交，不会长时间持锁卡住新的触发与查询；
  也可随时调用 `enforce_retention_once()` 立即执行一轮。
- 被清理的运行写入 `run_tombstones` 墓碑表（含被清理时的终态、时间、原因），
  每个工作流的墓碑数受 `tombstone_limit`（默认 100）约束，墓碑自身不无限增长。

## 清理后的查询语义

对 `GET /runs/{id}`（及其 `/events`、取消等）严格区分三种情况：

| 情况 | 返回 |
| --- | --- |
| 运行存在且属于自己 | 200，正常返回运行与任务状态 |
| 运行存在但属于他人 | **404**（与不存在不可区分，不泄露存在性） |
| 运行不存在，但有**自己的**墓碑（被保留策略清掉了） | **410 `RUN_PURGED`**，body 带 `terminal_status`/`purged_at`/`reason` |
| 运行不存在、也没有自己的墓碑（含墓碑属于他人） | **404 `RUN_NOT_FOUND`** |

关键点：墓碑同样按 owner 校验——越权查询一条已被清理的他人运行，得到的仍是 404，
无法据此推断对方是否存在。`GET /runs/purged` 可列出本调用方自己的墓碑。

## 可解释且有界的审计

每次因并发策略被**复用/拒绝**（含定时触发撞车、队列满跳槽）以及每次**历史清理**、
每次**策略更新**，都写一条审计记录（`audit_log`），含 owner、工作流/运行、
分类（`convergence` / `retention`）、动作、**影响范围 scope 与原因 detail**、时间。

- `GET /audit?limit=` 按 owner 返回自己的记录（倒序），他人审计同样不可见；
- 审计自身有界：守护线程周期性按 `ORCH_AUDIT_MAX_ENTRIES`（默认 5000 条）
  与 `ORCH_AUDIT_MAX_AGE_SECONDS`（默认 7 天）收缩，长期运行不会无限膨胀。

## API 一览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/workflows` | 注册工作流（含依赖校验、`active_run_policy` 与保留策略） |
| GET | `/workflows` | 列出本调用方工作流 |
| PUT | `/workflows/{name}/retention` | 更新该工作流的历史保留策略（持久化） |
| POST | `/workflows/{name}/runs` | 触发运行（可带幂等键）；新建 201 / 复用 200 |
| GET | `/runs` | 列出本调用方运行 |
| GET | `/runs/purged` | 列出本调用方被保留策略清理的运行墓碑 |
| GET | `/audit` | 本调用方的收敛/拒绝/清理审计记录（有界） |
| GET | `/runs/{run_id}` | 运行与任务状态（被清理 → 410，越权/不存在 → 404） |
| GET | `/runs/{run_id}/events` | 执行历史（调度决策/重试/拒绝原因） |
| POST | `/runs/{run_id}/cancel` | 取消运行 |

错误响应统一为 `{"error": <原因码>, "detail": ...}`，原因码见
`app/models.py` 的 `RejectReason`。

## 配置项（环境变量）

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `ORCH_MAX_CONCURRENCY` | 4 | 全局任务并发上限 |
| `ORCH_MAX_PENDING_RUNS` | 100 | 全局活跃运行积压上限 |
| `ORCH_DUP_POLICY` | dedup | 同幂等键：`dedup`/`reject` |
| `ORCH_DEFAULT_ACTIVE_POLICY` | allow | 工作流未配置时的默认并发收敛策略 |
| `ORCH_DEFAULT_RETAIN_LAST_RUNS` | 未设置 | 默认按条数保留（未设置=不限） |
| `ORCH_DEFAULT_RETAIN_SECONDS` | 未设置 | 默认按时间保留（未设置=不限） |

## 测试覆盖

`tests/` 覆盖：依赖成环与未知依赖、并发重复触发（dedup/reject 两策略）、
失败重试与退避、重试耗尽后下游跳过、执行超时终态、重启恢复（成功任务
不重复、中断任务恢复、事件可解释）、积压拒绝、非法状态转移、越权隔离、
定时触发去重、HTTP 层错误码。本轮新增：

- `test_convergence.py`：无业务去重标识时 20 线程并发触发同一条工作流，
  `reuse` 恰好一条活跃、其余全部复用且结果可区分；`reject` 恰好一条成功、
  其余 `ACTIVE_RUN_CONFLICT` 且携带冲突 run_id；手动/定时撞车同一套规则且
  审计可查；`allow` 保持历史并行语义；活跃结束后可再创建；非法策略拒绝。
- `test_retention.py`：按数量边界（只删超出的 N 条）、按时间边界
  （恰好 `retain_seconds` 的保留）、两策略取交集、进行中运行绝不清、
  分批原子清理（不留半条、任务明细与事件一起消失）、重启后策略原样生效、
  策略更新持久化与审计、非法策略拒绝。
- `test_query_and_audit.py`：被清理运行 → 410 `RUN_PURGED`（带终态/时间/
  原因）与"从未存在" 404 可区分；越权查他人存活/已清理运行一律 404 不泄露；
  墓碑与审计按 owner 隔离；复用/拒绝/清理均留可解释记录；审计按条数/时长自收缩
  有上界；后台守护线程自动清理。
- `test_new_features_api.py`：HTTP 层 201/200 收敛响应体、409 冲突带
  active_run_id、410 清理语义与越权 404、保留策略更新 200/非法 422、
  审计接口隔离、非法并发策略注册 422。

单测日志通过 `[TEST-EXPLAIN]` 打印输入与判定依据。
