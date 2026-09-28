# workflow-orchestrator

基于 FastAPI + SQLite 的本地工作流调度与编排服务。支持依赖编排、并发控制、
失败重试与退避、执行超时、崩溃恢复、手动/定时触发、多调用方隔离，
**同一工作流的并发触发收敛**、**按工作流配置的历史保留与自动清理**，
全部状态、执行历史与审计记录持久化在本地 SQLite 中，无外部服务依赖。

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
- **重复触发**：同一工作流下幂等键唯一（数据库唯一索引兜底，并发安全）。
  策略 `ORCH_DUP_POLICY=dedup`（默认）返回已有运行；`reject` 则拒绝并返回
  `DUPLICATE_RUN`（409）。
- **积压控制**：活跃运行（PENDING/RUNNING）达到 `ORCH_MAX_PENDING_RUNS`
  （默认 100）后，新触发被拒绝并返回 `QUEUE_FULL`（429），
  队列与状态存储不会无界增长。
- **定时触发**：工作流可设 `schedule_interval_seconds`，调度器按时间槽
  （`slot = now // interval`）生成幂等键 `sched-<wf>-<slot>` 触发，
  同一槽位不会重复触发；槽位触发失败（如积压）按策略跳过并记录事件。

## 并发触发收敛（同一时刻最多一条活跃运行）

多个上游可能同时触发同一条工作流，而且不一定能提供业务去重标识。
每条工作流可用 `active_run_policy` 配置自己的收敛策略，**手动触发与
定时触发撞在一起时走同一套规则**：

| 策略值 | 行为 |
| --- | --- |
| `allow`（默认） | 不做活跃收敛，保持历史语义：允许并发多条活跃运行 |
| `reuse` | 已有 PENDING/RUNNING 运行时，新触发**复用同一条**，绝不新建第二条 |
| `reject` | 已有活跃运行时，新触发被**明确拒绝**（`ACTIVE_RUN_EXISTS`，HTTP 409），detail 指明占位的 run_id |

- 触发响应在运行字段之外额外携带 `outcome`：`CREATED`（新建）、
  `REUSED_KEY`（命中幂等键复用）、`REUSED_ACTIVE`（按策略复用活跃运行），
  以及 `detail`（如 `active_run_id=<id>`）。被复用方因此能明确知道自己
  复用了哪一条运行。
- 正确性由存储层单事务的"检查活跃 + 插入"保证（单连接 + 锁串行化）：
  并发窗口里只有一条能创建成功，不会出现两条都成功，拒绝方也不会落库。
- 收敛只约束**同一时刻**：活跃运行进入终态后，新触发可正常创建下一条。
- 幂等键规则保持不变：带键触发先按键去重（唯一索引兜底）；键未命中或
  无键时才应用上面的活跃收敛策略。
- 每次复用/拒绝都写一条**审计记录**（含策略、活跃 run_id、双方触发来源
  manual/schedule），可通过 `GET /audit?workflow=<name>` 查询"结果与原因"。

配置方式（注册时或事后更新）：

```bash
# 注册时声明收敛策略
curl -s -X POST localhost:8000/workflows -H 'X-Owner-Id: alice' \
  -H 'Content-Type: application/json' -d '{
    "name":"demo","active_run_policy":"reuse","tasks":[...]}'
# 事后调整（保留策略也可一并更新）
curl -s -X PUT localhost:8000/workflows/demo/policies \
  -H 'X-Owner-Id: alice' -H 'Content-Type: application/json' \
  -d '{"active_run_policy":"reject"}'
```

## 历史保留策略（长期运行存储有上界）

每条工作流可独立配置已结束运行的保留策略 `retention`：

- `keep_last`：只保留最近 N 条**已结束**运行（按结束时间倒序）；
- `keep_seconds`：只保留最近 N 秒内**结束**的运行；
- 两者同时设置时取**交集**：既在最近 N 条内、又未超龄的运行才保留。
- 都不设置（默认）表示不自动清理，保持历史语义。

生效范围与保证：

- **只清理终态运行**（SUCCEEDED/FAILED/CANCELLED）。PENDING/RUNNING 的
  运行绝不触碰，进行中的依赖关系不受影响——清理候选查询显式排除活跃状态，
  删除事务内还会二次校验，发现活跃 id 直接整体放弃。
- **整条清理，全有或全无**：一条运行连同它的 `task_runs` 明细与执行
  `events` 在**同一 SQLite 事务**内删除，要么整条清干净，要么本次完全
  不生效，不会留下半条记录。
- **不长期卡住触发与查询**：后台清理在独立线程中按
  `ORCH_CLEANUP_INTERVAL`（默认 30s）节流运行，单条工作流每批最多删除
  `cleanup_batch_runs`（默认 100）条以控制锁持有时间；也提供手动入口
  `POST /workflows/{name}/retention:enforce` 立即执行一次。
- **策略持久化**：保存在工作流 spec（`workflows.spec_json`）中，服务
  重启后继续按原策略生效；旧版本数据库打开时自动补全新表，旧 spec
  缺省回到"不收敛、不清理"。

清理之后的查询语义：

- owner 查询自己**被清理**的运行 → 核心层 `RUN_REAPED`，HTTP **410 Gone**，
  detail 含清理时间与原因；
- 查询**从未存在**的 id → `RUN_NOT_FOUND`，HTTP **404**；
- 查询**正常保留**的运行 → 200。三种结果对 owner 自己可明确区分。
- 清理时写入**墓碑**（`run_tombstones`，TTL 由 `ORCH_TOMBSTONE_TTL`
  控制，默认 7 天）支撑上面的区分；墓碑过期后该 id 回到"从未存在"语义。

### 审计记录自身有上界

所有复用、拒绝、清理事件写入 `audit_events`，记录"发生了什么、范围是
什么"（策略值、活跃/被清理的 run_id 列表、数量边界、时间边界、是否因
批次上限被截断等）。审计表本身通过 TTL（`ORCH_AUDIT_TTL`，默认 7 天）
与行数硬上界（`ORCH_AUDIT_MAX_ROWS`，默认 5000，超出时删除最旧记录）
双重约束，绝不会无限膨胀；墓碑与审计的裁剪也会留下一条裁剪记录。

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
- 所有查询按 owner 过滤；取消等写操作、审计查询同理。访问他人资源在核心
  层抛 `FORBIDDEN`（用于服务端日志/本地诊断），**HTTP 层统一映射为固定的
  `404 {"error":"RUN_NOT_FOUND","detail":"run not found"}`**。
- 关键性质：越权者查询他人"仍存在的运行 / 已被清理的运行 / 根本不存在的
  id"时，**HTTP 状态码与响应体完全一致**（均为 404 固定体），无法借此
  判断某个 id 是否真实存在，清理引入的 410 区分只对资源真正的 owner 可见。
- 审计同样按 owner 隔离：`GET /audit` 只返回本 owner 的记录，
  他人记录不可见。

## API 一览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/workflows` | 注册工作流（含依赖校验、`active_run_policy`、`retention`） |
| GET | `/workflows` | 列出本调用方工作流 |
| PUT | `/workflows/{name}/policies` | 更新并发收敛策略 / 历史保留策略（持久化） |
| POST | `/workflows/{name}/runs` | 触发运行（可带幂等键）；响应含 `outcome` 收敛判定 |
| POST | `/workflows/{name}/retention:enforce` | 立即按该工作流策略执行一次历史清理 |
| GET | `/runs` | 列出本调用方运行 |
| GET | `/runs/{run_id}` | 运行与任务状态（被清理 → 410；不存在/越权 → 404） |
| GET | `/runs/{run_id}/events` | 执行历史（调度决策/重试/拒绝原因） |
| POST | `/runs/{run_id}/cancel` | 取消运行 |
| GET | `/audit?workflow=<name>` | 本 owner 的触发复用/拒绝、历史清理审计（可按工作流过滤） |

错误响应统一为 `{"error": <原因码>, "detail": ...}`，原因码见
`app/models.py` 的 `RejectReason`。本轮新增：`ACTIVE_RUN_EXISTS`（409）、
`RUN_REAPED`（410）、`INVALID_POLICY` / `INVALID_RETENTION`（422）。

相关环境变量：

| 变量 | 默认 | 含义 |
| --- | --- | --- |
| `ORCH_CLEANUP_INTERVAL` | 30 | 后台历史清理尝试间隔（秒） |
| `ORCH_TOMBSTONE_TTL` | 604800 | 清理墓碑保留期（秒），期内可区分"被清理" |
| `ORCH_AUDIT_TTL` | 604800 | 审计记录保留期（秒） |
| `ORCH_AUDIT_MAX_ROWS` | 5000 | 审计记录行数硬上界 |

## 本地验证

```bash
uv run pytest tests/ -v   # 含本轮新增的并发收敛/保留清理/查询语义用例
```

手动体验并发收敛与清理（间隔极短，便于观察）：

```bash
uv run uvicorn main:app --port 8000
# reuse 策略 + 只保留最近 1 条历史
curl -s -X POST localhost:8000/workflows -H 'X-Owner-Id: alice' \
  -H 'Content-Type: application/json' -d '{
    "name":"demo","active_run_policy":"reuse",
    "retention":{"keep_last":1},"tasks":[{"name":"a","handler":"sleep",
    "timeout_seconds":5}]}'
# 连续两次触发（处理器在睡眠 => 第二条应返回 REUSED_ACTIVE 且同一 run_id）
curl -s -X POST localhost:8000/workflows/demo/runs -H 'X-Owner-Id: alice'
curl -s -X POST localhost:8000/workflows/demo/runs -H 'X-Owner-Id: alice'
# 立即清理；之后查询被清理的 run_id 返回 410 RUN_REAPED
curl -s -X POST localhost:8000/workflows/demo/retention:enforce \
  -H 'X-Owner-Id: alice'
curl -s localhost:8000/audit?workflow=demo -H 'X-Owner-Id: alice'
```

## 测试覆盖

`tests/` 覆盖：依赖成环与未知依赖、并发重复触发（dedup/reject 两策略）、
失败重试与退避、重试耗尽后下游跳过、执行超时终态、重启恢复（成功任务
不重复、中断任务恢复、事件可解释）、积压拒绝、非法状态转移、越权隔离、
定时触发去重、HTTP 层错误码。

本轮新增（仍保留全部原有用例）：

- `test_convergence.py`：无业务去重键时多上游并发触发，reuse 策略只产生
  一条活跃运行且其余结果为 `REUSED_ACTIVE`；reject 策略只一条成功、其余
  `ACTIVE_RUN_EXISTS` 且指明占位 run；手动/定时双向撞车走同一规则且审计
  可查触发来源；活跃结束后可再触发。
- `test_retention.py`：`keep_last` 边界（恰好等于不清理、超出只清旧的、
  明细与事件级联删除）、`keep_seconds` 时间边界、数量与时间取交集、
  进行中运行绝不被清理、清理事务中途故障整体回滚不留半条、重启后策略
  仍生效并可更新再持久化、按工作流隔离清理、后台周期清理且不阻塞触发。
- `test_query_semantics.py`：被清理（`RUN_REAPED`）与不存在
  （`RUN_NOT_FOUND`）对 owner 可区分；越权者读不到数据且三种 id 不可
  区分；复用/拒绝/清理审计含范围；审计与墓碑受 TTL 与行数上界约束。
- `test_api_retention.py`：HTTP 触发响应 `outcome`、409 拒绝、手动清理
  端点、owner 视角 410/404/200 区分、越权三种 id 的状态码与响应体完全
  一致、非法策略/保留参数 422。

单测日志通过 `[TEST-EXPLAIN]` 打印输入与判定依据。
