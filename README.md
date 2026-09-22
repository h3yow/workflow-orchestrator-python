# workflow-orchestrator

基于 FastAPI + SQLite 的本地工作流调度与编排服务。支持依赖编排、并发控制、
失败重试与退避、执行超时、崩溃恢复、手动/定时触发、多调用方隔离，
全部状态与执行历史持久化在本地 SQLite 中，无外部服务依赖。

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

## API 一览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/workflows` | 注册工作流（含依赖校验） |
| GET | `/workflows` | 列出本调用方工作流 |
| POST | `/workflows/{name}/runs` | 触发运行（可带幂等键） |
| GET | `/runs` | 列出本调用方运行 |
| GET | `/runs/{run_id}` | 运行与任务状态 |
| GET | `/runs/{run_id}/events` | 执行历史（调度决策/重试/拒绝原因） |
| POST | `/runs/{run_id}/cancel` | 取消运行 |

错误响应统一为 `{"error": <原因码>, "detail": ...}`，原因码见
`app/models.py` 的 `RejectReason`。

## 测试覆盖

`tests/` 覆盖：依赖成环与未知依赖、并发重复触发（dedup/reject 两策略）、
失败重试与退避、重试耗尽后下游跳过、执行超时终态、重启恢复（成功任务
不重复、中断任务恢复、事件可解释）、积压拒绝、非法状态转移、越权隔离、
定时触发去重、HTTP 层错误码。单测日志通过 `[TEST-EXPLAIN]` 打印输入与
判定依据。
