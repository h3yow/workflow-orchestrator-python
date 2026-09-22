# 本地工作流调度与编排服务

一个仅依赖本地能力的 Python/FastAPI 工作流编排服务：用 SQLite（WAL）持久化任务
状态与执行历史，提供依赖调度、有界并发、去重触发、失败重试与指数退避、执行
超时、进程重启恢复、手动/定时触发、多租户隔离与可解释的决策历史。无需任何
外部服务或真实凭据。

## 1. 快速开始（本地验证）

```bash
uv sync                                  # 安装依赖（fastapi + pytest）
uv run uvicorn main:app --reload --port 8000
# 另一个终端：
curl -s localhost:8000/health
# 带 API Key 调用（默认开发密钥，见“隔离边界”一节）
curl -s -X POST localhost:8000/api/v1/workflows \
  -H 'X-API-Key: dev-key-a' -H 'Content-Type: application/json' \
  -d '{"name":"demo","tasks":[{"key":"a","handler":"builtin.echo"},
      {"key":"b","handler":"builtin.echo","depends_on":["a"]}]}'
curl -s -X POST localhost:8000/api/v1/workflows/demo/trigger \
  -H 'X-API-Key: dev-key-a' -H 'Content-Type: application/json' -d '{}'
```

运行测试（含依赖成环、并发重复触发、失败重试、超时、重启恢复）：

```bash
uv run pytest -q                       # 全部 34 个用例
uv run pytest -s                       # 打印每个用例的输入与判定依据
uv run pytest tests/test_recovery.py   # 仅重启恢复场景
```

关键环境变量：

| 变量 | 默认值 | 含义 |
| --- | --- | --- |
| `WORKFLOW_DB` | `data/workflow.db` | SQLite 数据文件路径 |
| `WORKFLOW_MAX_WORKERS` | `4` | 全局任务执行并发上限 |
| `WORKFLOW_POLL_INTERVAL` | `0.05` | 调度/定时循环节拍（秒） |
| `WORKFLOW_RETAIN_RUNS` | `100` | 每工作流保留的最近终态运行数 |
| `WORKFLOW_API_KEYS` | `dev-key-a=tenant-a,dev-key-b=tenant-b` | `key=租户` 映射 |
| `WORKFLOW_LOG_LEVEL` | `INFO` | 日志级别 |

## 2. 核心模型与状态机

### 2.1 运行（Run）状态

```
PENDING ──调度放行──> RUNNING ──全部任务成功──> SUCCEEDED
                        │
                        └──任一任务 FAILED/TIMED_OUT（其余级联 SKIPPED）──> FAILED
```

- `PENDING`：已创建但在队列中等待（仅 QUEUE 策略可能出现）。
- 终态只有 `SUCCEEDED` / `FAILED`；终态不可再转移。
- 所有转移都是带 `WHERE state IN (...)` 的条件 UPDATE，非法/竞态转移不生效，
  上层映射为 `INVALID_TRANSITION` 或具体的拒绝错误码。

### 2.2 任务（Task）状态

```
PENDING ─依赖满足─> READY ─原子领取─> RUNNING ─成功─> SUCCEEDED
   │                   ▲                │
   │                   │                ├─失败且仍有重试余量─> RETRY_WAIT
   │                   └──退避到期────────┤
   │                                    ├─失败且重试耗尽────> FAILED
   │                                    └─超时─────────────> TIMED_OUT
   └──任一前置进入失败终态──────────────────────────────────> SKIPPED
```

- 任务终态：`SUCCEEDED`、`FAILED`、`TIMED_OUT`、`SKIPPED`，四种可区分。
- **同一任务在同一次运行中绝不重复执行**：READY→RUNNING 由单条条件 UPDATE
  裁决（`claim_ready_task`），配合执行器进程内去重，跨线程/跨调度循环安全。
- 前置失败时下游一律 `SKIPPED`（错误原因 `UPSTREAM_FAILED` 并注明是哪些前置），
  不会被误执行。

### 2.3 错误码（拒绝原因可区分）

| 错误码 | 触发条件 | HTTP |
| --- | --- | --- |
| `CYCLIC_DEPENDENCY` | 依赖图成环（含自环），Kahn 排序后仍有节点 | 422 |
| `MISSING_DEPENDENCY` | `depends_on` 引用了不存在的任务 | 422 |
| `DUPLICATE_TASK` | 同一工作流内任务 key 重复 | 422 |
| `INVALID_DEFINITION` | 名称为空、重试/超时参数非法、未知并发策略 | 422 |
| `ALREADY_ACTIVE` | REJECT 策略下已有活动运行 | 409 |
| `QUEUE_FULL` | QUEUE 策略下活动运行超过 `1 + max_queued_runs` | 429 |
| `INVALID_TRANSITION` | 状态机非法转移 | 409 |
| `WORKFLOW_NOT_FOUND` / `RUN_NOT_FOUND` / `TASK_NOT_FOUND` / `SCHEDULE_NOT_FOUND` | 资源不存在（跨租户访问同样返回 404，不泄漏存在性） | 404 |
| `DUPLICATE_SCHEDULE` | 定时计划重名 | 409 |
| `UNAUTHORIZED` | 缺失/错误 API Key | 401 |
| `INTERRUPTED` | 任务执行期间进程重启（恢复时补记） | — |
| `TIMED_OUT` / `TASK_FAILED` | 任务超时 / 处理器抛异常 | — |

## 3. 调度语义

调度器为单个后台线程，每个节拍严格按固定顺序推进：

1. **排队放行**：PENDING 运行按 FIFO 放行；同一（租户、工作流）任一时刻最多
   一个 RUNNING。
2. **退避重试**：RETRY_WAIT 任务按
   `backoff_seconds * backoff_multiplier^(失败次数-1)` 计算 `next_run_at`，
   到期才转回 READY。
3. **依赖传播**：PENDING 任务的**全部**前置都 SUCCEEDED 才进入 READY；
   无依赖任务立即可调度（彼此并行）；任一前置处于失败终态则 SKIPPED（级联）。
4. **有界派发**：只有当在途任务数 `< WORKFLOW_MAX_WORKERS` 时才提交 READY
   任务（背压），因此并发度有硬上限。
5. **终态收敛**：所有任务进入终态后运行转 SUCCEEDED；否则只要存在失败终态
   任务即转 FAILED，并把首个失败任务的错误码/信息写入运行的 `error`。

### 3.1 并发触发策略（每个工作流可配）

- `REJECT`（默认）：已有活动运行（PENDING 或 RUNNING）时拒绝 `ALREADY_ACTIVE`。
- `SKIP`：去重，不新建运行，直接返回已有活动运行 ID（响应中 `deduped=true`）。
- `QUEUE`：允许积压，容量 = 1 个运行中 + `max_queued_runs` 个排队；超出容量
  返回 `QUEUE_FULL`。多个调用方同时触发时，“计数→裁决→插入”在进程互斥锁内
  原子完成，SQLite 写事务进一步串行化，不会出现两个运行或状态错乱。

### 3.2 定时触发

- 固定间隔计划（秒级），`POST /api/v1/schedules` 创建，计划按租户隔离。
- 到点时按工作流**自身的并发策略**触发；若被拒绝（如 REJECT 下仍在运行），
  记录 `SCHED_TRIGGER_REJECTED` 决策日志并把下一次触发时刻推进一个间隔，
  不会忙等刷屏；积压消化后下一周期自动恢复。
- 手动触发与定时触发共用同一套去重/排队规则。

### 3.3 重试、超时与副作用幂等

- `retry: {max_retries, backoff_seconds, backoff_multiplier}` 在任务级配置；
  总尝试次数 = `1 + max_retries`。
- 超时为**协作式判定**：超过 `timeout_seconds` 后该尝试记为 TIMED_OUT，
  尚有重试余量则退避后重试，否则进入 TIMED_OUT 终态。Python 无法强杀线程，
  超时线程的迟到结果会被丢弃且不记账（线程本身可能稍后才结束）。
- 每次尝试在 `task_attempts` 表留痕（开始/结束时刻、错误码、是否超时）。
- **幂等键**：处理器上下文含稳定的 `idempotency_key = sha256(run|task|attempt)`。
  同一次尝试即便因崩溃恢复被重放，幂等键不变；有外部副作用的处理器应使用它
  做去重。调度层保证：成功完成的任务永不重新执行，普通重试对应全新 attempt。

## 4. 重启恢复规则

启动时（`Scheduler.recover`）：

1. 扫描所有仍为 RUNNING 的任务行（即崩溃时在途的任务），在 `task_attempts`
   中补记一次 `INTERRUPTED` 尝试（开始时刻取旧值、结束时刻取恢复时刻），
   使“未完成的任务不被静默丢弃”；
2. 尚有重试余量的任务置 RETRY_WAIT 并在恢复后立即重新参与调度；
   重试耗尽的任务置 FAILED（错误码 `INTERRUPTED`，与 `TIMED_OUT`/业务失败
   可区分），下游随后级联 SKIPPED；
3. RUNNING 运行保持 RUNNING 继续调度；PENDING 运行继续排队（记
   `RECOVER_QUEUED_RUN`）；SUCCEEDED 任务与 SUCCEEDED/FAILED 运行**完全不
   触碰**——已成功的任务绝不会被重复触发。

恢复过程的每个动作都写 `decision_logs`，可经
`GET /runs/{id}/history` 查看，恢复结论能被执行历史完整解释。测试
`tests/test_recovery.py` 覆盖：成功任务不重跑、中断任务重试成功、无余量时
进入可区分终态、排队运行不丢失、菱形依赖恢复后全链路一致。

## 5. 隔离边界

- 所有数据行带 `tenant_id`；API Key 经 `X-API-Key` 头解析为租户
  （`WORKFLOW_API_KEYS` 配置，生产部署请替换默认开发密钥）。
- 无效/缺失 Key → 401；所有读写都强制 `WHERE tenant_id=?`。
- 跨租户访问他人的 run/task/history 一律返回 404（而非 403），避免泄漏资源
  是否存在；工作流定义、定时计划同样按租户过滤。
- 触发互斥、并发上限均在（租户、工作流）维度生效。

## 6. 存储与有界增长

- SQLite WAL 模式，单连接 + 进程锁串行写入，外键级联开启；状态裁决全部落在
  条件 UPDATE 上，可抵抗进程内竞态。
- 每次运行终态后执行历史裁剪：每个（租户、工作流）仅保留最近
  `WORKFLOW_RETAIN_RUNS` 条终态运行，其任务/尝试/决策日志级联删除；
  活动运行（PENDING/RUNNING）永不裁剪。定时计划数量由用户显式管理。

## 7. HTTP 接口（前缀 `/api/v1`）

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/workflows` | 注册/更新工作流定义（校验成环、缺失依赖等） |
| GET | `/workflows` | 列出本租户工作流 |
| GET | `/workflows/{name}/definition` | 读取定义 |
| POST | `/workflows/{name}/trigger` | 手动触发（body: `{"trigger":"manual"}`） |
| GET | `/runs?workflow_name=` | 列出运行 |
| GET | `/runs/{id}` | 运行状态（含失败码） |
| GET | `/runs/{id}/tasks` | 任务状态与每次尝试记录 |
| GET | `/runs/{id}/history` | 决策日志（调度/重试/拒绝/恢复） |
| POST | `/schedules` | 创建定时计划 |
| GET | `/schedules` | 列出定时计划 |
| DELETE | `/schedules/{name}` | 删除定时计划 |
| GET | `/health` | 健康检查 |

工作流定义示例：

```json
{
  "name": "etl",
  "version": 1,
  "concurrency": "QUEUE",
  "max_queued_runs": 4,
  "tasks": [
    {"key": "extract", "handler": "builtin.echo", "timeout_seconds": 5},
    {"key": "transform", "handler": "builtin.echo", "depends_on": ["extract"],
     "retry": {"max_retries": 3, "backoff_seconds": 1, "backoff_multiplier": 2}},
    {"key": "load_a", "handler": "builtin.echo", "depends_on": ["transform"]},
    {"key": "load_b", "handler": "builtin.echo", "depends_on": ["transform"]}
  ]
}
```

## 8. 注册自定义任务处理器

处理器是普通 Python 函数，通过注册表登记（服务启动代码中注册，例如
`main.py` 或应用工厂）：

```python
from app.handlers import register_handler

def my_handler(params: dict, context: dict):
    # context: run_id / task_key / tenant_id / attempt_no /
    #          idempotency_key（同一次尝试稳定，用于副作用去重）
    return {"rows": 42}

register_handler("my_handler", my_handler)
```

内置示例处理器 `builtin.echo` 由服务启动时注册，便于手工 curl 验证。

## 9. 代码结构

```
app/
  errors.py      可区分错误码与异常
  models.py      枚举/状态机定义、退避计算、领域 dataclass
  validation.py  定义校验（重复、缺失依赖、成环）
  store.py       SQLite 持久化：条件状态转移、快照、决策日志、裁剪
  handlers.py    任务处理器注册表
  executor.py    线程池执行、原子领取、超时收割、尝试记账
  scheduler.py   调度循环：放行/退避/依赖/派发/收敛/重启恢复
  engine.py      门面：触发策略、定时触发、查询收口
  auth.py        API Key -> 租户
  schemas.py / api.py / main.py   HTTP 层
tests/           34 个用例（成环/并发/重试/超时/恢复/隔离/API）
```

## 10. 已知边界

- 超时为协作式（不杀线程），仅保证状态与记账正确。
- 触发去重的互斥在单进程内成立；多进程部署需以 SQLite 文件锁/单实例运行
  （本项目定位为本地单实例服务）。
