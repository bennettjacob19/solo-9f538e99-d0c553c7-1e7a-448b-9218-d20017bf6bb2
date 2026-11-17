# 制品目录历史查询服务

发布值班工程师追查“误上线时制品目录实际指向哪个版本”的历史查询系统。
源 PostgreSQL 中的制品目录（制品编号 / 服务名 / 版本 / SHA-256）通过
**一致基线 + 逻辑复制**被完整捕获到本地 SQLite 仅增日志，可按任意已捕获
提交点回看整张目录，支持删除后编号重用、多行事务整体可见。

在历史查询之外，本服务还提供**逐事务可靠通知**：只有已完整落入 SQLite 的
制品变更才会按源事务提交顺序，整笔投递给 Docker Compose 内的固定示例接收端
（部署候选重建服务）；未确认通知持续续投、严格不越序，接收端对重发幂等
（只安排一次重建）。

## 1. 架构与正确性设计

```
PostgreSQL 源库 (artifacts 固定四列)
   │  ① 建逻辑复制槽 test_decoding + pg_export_snapshot 导出一致性快照
   │  ② 另开连接 SET TRANSACTION SNAPSHOT 复用快照做整表基线
   │  ③ 从槽 LSN 持续消费逻辑变更流（walsender）
   ▼
捕获工作线程（FastAPI 进程内 daemon thread）
   │  每个源事务：行变更 + 提交点 + 通知发件箱行在同一个 SQLite 事务落盘
   │              落盘成功后才 send_feedback 推进源端槽位
   ▼
SQLite 历史库（WAL，仅增不改）  ←→  FastAPI 网页 / JSON API
   │  通知工作线程：按 seq 取最早未确认通知，整笔 HTTP POST，2xx 才确认
   ▼
固定示例接收端 receiver 服务（幂等账本 + 待重建制品编号队列，SQLite 持久化）
```

### 关键不变量

| 需求 | 实现方式 |
| --- | --- |
| 一致基线，普通 SQL 不中断 | 建槽与 `pg_export_snapshot()` 在同一事务完成；基线用 `SET TRANSACTION SNAPSHOT` 读一致性快照，源库无需停机、普通读写照常提交 |
| 基线前开始、基线后提交的事务不漏 | 该事务对导出快照不可见（不进基线），但其提交记录的 LSN 晚于槽起始 LSN，必然出现在逻辑流中被补捕获（自测第 2 组确定性验证） |
| 多行事务整体可见 | 一个源事务的全部行变更与提交点在**同一个 SQLite 事务**落盘，不存在半条事务 |
| 删除后重用编号保留旧历史 | `artifact_history` 仅追加，每行带 `change_type` 与提交点；按编号取“最后一条 ≤ 查询点”的记录重建目录，DELETE 与后续 INSERT 各自留痕 |
| 源断连 / 强杀重启后续接 | 复制槽持久化在源库；应用重启从槽位重放，SQLite 以源事务 xid 为主键幂等去重；断线自动指数/固定退避重连 |
| 不确认尚未落盘的更改 | 严格“先 SQLite commit，后 `send_feedback(flush_lsn=...)`”；keepalive 与无关消息一律不回送反馈。自测断言槽 `confirmed_flush_lsn` 永不超前已落盘提交点 |
| 初始化未完成不开放历史 | 状态机 `INITIALIZING → STREAMING / ERROR`；未到 STREAMING 时 `/api/points`、`/api/directory`、`/api/history/*` 一律 503 |
| 固定结构的增改删 | 网页/API 只接受 `insert/update/delete` 三类操作，pydantic 判别联合按 `op` 分流，仅四列，sha256 强制 64 位十六进制；无任何 DDL/任意 SQL 通道 |

### SQLite 表结构（`/data/history.db`）

- `capture_state`：阶段、槽名、基线 LSN、最近提交点/消息 LSN、错误信息。
- `txns`：源事务 xid（主键）、提交 LSN、提交时间。
- `artifact_history`：仅增变更链（BASELINE/INSERT/UPDATE/DELETE），带 xid、
  提交 LSN 与数值化 LSN，支持按提交点过滤与编号重建。
- `notifications`：逐事务通知发件箱（outbox），见下节。

## 1.5 逐事务可靠通知（事务性发件箱 + 幂等接收端）

```
捕获线程：源事务数据 + outbox 通知行 ——同一 SQLite 事务落盘
                          │（仅已完整落盘的事务才有通知）
                          ▼
通知线程：按 seq 取最早未确认项 → HTTP POST 整笔事务 → 接收端 2xx 才标 SENT
                          │ 失败/无响应：指数退避续投，前一笔未确认不越过
                          ▼
固定示例接收端(receiver 服务)：按 notification_seq 幂等去重，
                          待重建制品编号去重入队，SQLite 持久化
```

### 可靠性不变量

| 需求 | 实现方式 |
| --- | --- |
| 只通知已完整落盘的变更 | 通知行与该事务的 `txns`/`artifact_history` 行在**同一个 SQLite 事务**写入；未落盘的事务不存在通知 |
| 同一源事务的增删改整笔送达 | 一条通知包含该事务的全部 changes（insert/update/delete），不拆分 |
| 按提交顺序、后续事务不得越过 | 通知 `seq` 单调递增即提交顺序；通知线程只取 `seq` 最小的未确认项（head-of-line），前一笔未 ACK 时后续项 attempts 保持 0 |
| 接收端已处理但响应丢失 | 发送端未收到 2xx 即视为未确认，原样重发整笔；接收端按 `notification_seq` 去重，重发只累加 `duplicate_count`，**不安排第二次重建** |
| 接收端暂不可用 / 服务重启 | 通知状态（PENDING/FAILED/SENT、attempts、退避时间）持久化在 SQLite；重启后通知线程自动从最早未确认项续投 |
| 捕获重放不制造第二笔通知 | 重放事务按 xid 幂等跳过（`persist_transaction` 返回 False），不会再写 outbox |
| 投递故障不影响历史查询 | 通知线程独立 daemon，HTTP 调用在历史库锁之外；失败只写通知表错误字段，历史接口路径完全不依赖接收端 |
| 网页可重试未确认通知 | `POST /api/notifications/{id}/retry` 清空退避并立即唤醒通知线程；已确认通知重试返回 409 |

### 接收端（`receiver` 服务，固定示例）

仅用 Python 标准库实现（`app/receiver/`），自带 SQLite（`/data/receiver.db`）：

- `POST /notify`：幂等接收事务通知，涉及的制品编号去重进入待重建队列；
- `GET /receiver/pending`：待重建制品编号（含触发来源 seq）；
- `POST /receiver/drain`：演示“执行重建”，队列转入重建历史并清空；
- `GET /receiver/processed`、`/receiver/history`、`/receiver/stats`：
  幂等账本（含重复投递计数）、重建历史、汇总统计；
- `POST /receiver/fail-mode`：测试钩子，`{"status":500}` 模拟“已处理但
  响应丢失”，`{"status":0}` 解除（生产不暴露外部入口，仅 Compose 内网）。

## 2. 用 Docker Compose 启动（交付方式）

```bash
docker compose up --build
# 网页：http://localhost:8000/
# 接收端（固定示例）：http://localhost:8090/healthz
# 源 PostgreSQL：localhost:5432（user/pass/db = postgres/postgres/postgres）
```

- `postgres` 服务：`postgres:16-bookworm`，已开启 `wal_level=logical`，
  命名卷 `pgdata` 持久化；首次启动自动执行 `init/01_schema.sql`
  幂等建表并播种 3 行示例数据。
- `receiver` 服务：与主应用同一镜像、以 `python -m app.receiver.server`
  启动的固定示例接收端（stdlib HTTP 服务 + SQLite），命名卷 `receiverdata`
  挂载到 `/data` 持久化幂等账本与待重建队列；自带 `/healthz` 健康检查。
- `app` 服务：本仓库构建的 Python 3.12 镜像，命名卷 `historydata`
  挂载到 `/data` 持久化 SQLite（历史 + 通知发件箱）；`restart: unless-stopped`，
  源库与接收端健康检查均通过后才启动，通过 `NOTIFIER_URL` 指向
  `http://receiver:8090/notify`。
- 三个命名卷，`docker compose down` 不丢数据，`down -v` 才清空。

## 3. 网页功能（单页应用，无前端构建）

1. **捕获进度**：阶段徽章、复制槽、基线/最近提交 LSN、已捕获事务数、
   历史行数、通知计数（SENT/PENDING/FAILED）、最近错误，3 秒自动刷新。
2. **成组修改源目录**：动态增删 insert/update/delete 行，整组在一个源事务
   提交；任一操作失败整组回滚。
3. **提交点回看**：列出 BASELINE 与每个源事务提交点（LSN、xid、时间、
   变更行数），点击任一行即查看该时刻整张目录。
4. **单编号完整历史**：含 BASELINE/INSERT/UPDATE/DELETE 全链，
   直观验证“删除后重用编号，旧历史保留”。
5. **逐事务投递状态**：发件箱每笔通知的 seq、状态徽章、尝试次数、
   整笔变更内容、最近错误与更新时间，可对未确认通知点击“立即重试”。
6. **接收端待重建编号**：待重建队列、已处理通知账本（含重复投递计数，
   可直观验证重发只安排一次重建）、重建历史，并可模拟执行重建清空队列。

## 4. HTTP API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/api/progress` | 捕获状态与进度（含通知计数，初始化期也可用） |
| GET | `/api/source/artifacts` | 源库当前目录 |
| POST | `/api/source/operations` | 成组增改删，单源事务，失败整组回滚 |
| GET | `/api/points` | 全部可查询提交点（未就绪时 503） |
| GET | `/api/directory?point=0/16B3748` | 该提交点（含）的整目录 |
| GET | `/api/history/{artifact_no}` | 单编号完整变更链 |
| GET | `/api/notifications` | 发件箱逐笔投递状态与统计 |
| POST | `/api/notifications/{id}/retry` | 手动重试未确认通知（已确认 409，不存在 404） |
| GET | `/api/receiver/pending` | 接收端待重建制品编号（代理，接收端宕机 503） |
| GET | `/api/receiver/processed` | 接收端幂等账本（含重复投递计数） |
| GET | `/api/receiver/history` | 接收端重建历史 |
| POST | `/api/receiver/drain` | 接收端执行重建：队列转历史并清空 |

接收端自身端点（容器内网 `http://receiver:8090`，亦映射到主机 8090 端口）：

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/healthz` | 存活探针（Compose healthcheck 使用） |
| POST | `/notify` | 接收一笔事务通知，按 seq 幂等，重发不重复重建 |
| GET | `/receiver/pending` `/history` `/processed` `/stats` | 队列/历史/账本/统计 |
| POST | `/receiver/drain` `/fail-mode` | 执行重建；测试钩子模拟响应丢失 |

请求示例：

```bash
curl -X POST localhost:8000/api/source/operations -H 'Content-Type: application/json' -d '{
  "operations": [
    {"op":"insert","artifact_no":1004,"service_name":"cart-svc","version":"3.1.0",
     "sha256":"abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234"},
    {"op":"delete","artifact_no":1003}
  ]}'
curl 'localhost:8000/api/directory?point=0/16B3748'
```

## 5. 自测

### 5.1 解析器与通知单元测试

```bash
python3 -m unittest discover -s tests -v
```

覆盖：test_decoding 解析（BEGIN/COMMIT、转义、忽略其他表）；事务性发件箱
（同事务落盘、重放不造第二笔、head-of-line 不越序、响应丢失重发幂等、
故障不影响历史读）；接收端存储与 HTTP 层（重复投递只入队一次、fail-mode、
drain、4xx 校验）；通知 API（404/409、代理 503 隔离）。

### 5.2 端到端自测（54 项断言）

`tests/selftest.sh` 在本机自动拉起一个便携 PostgreSQL（不依赖 Docker）、
接收端与应用并完成全链路验证：

```bash
# 可选：指定本机 PostgreSQL 二进制位置
export PGBIN=/path/to/postgresql/bin PGCLIENT=/path/to/client/bin
bash tests/selftest.sh
```

覆盖场景：

1. 初始化窗口历史接口 503，完成后 STREAMING 与基线目录正确；
2. **跨基线事务**：通过应用内置测试钩子
   （`TEST_INFLIGHT_ARTIFACT_NO` 环境变量，默认关闭）确定性构造
   “基线快照点之后打开、基线落盘之后提交”的事务，断言不进基线、
   不被遗漏、在其提交点可见；
3. 多行事务只产生一个提交点、同事务插入/删除同时生效；
4. 删除后重用编号，旧历史链完整（BASELINE,DELETE,INSERT）；
5. 失败整组回滚、非法 sha256/非法 LSN 参数校验；
6. `kill -9` 强杀应用后重启：事务数不重复、最后更新仍在、
   槽 `confirmed_flush_lsn` 不超前已落盘 LSN、续接后新变更继续捕获；
7. 源 PostgreSQL 重启后应用自动重连恢复 STREAMING，恢复后变更不丢；
8. **逐事务可靠通知（22 项断言）**：整笔事务一条通知按序 ACK；
   接收端“已处理但响应丢失”后发送端退避重发、接收端幂等只安排一次重建；
   head-of-line 阻塞（后续事务 attempts=0 不越过）；接收端宕机时手动重试
   立即打断退避、历史查询不受影响、代理接口 503；接收端与发送端分别重启后
   未确认通知自动续投、重放不造第二笔通知、同编号多次变更队列只占一行、
   重建 drain 与两侧 SQLite 持久化；
9. SQLite 与 PostgreSQL 数据目录均落盘持久化。

最近一次结果：**54 passed / 0 failed**（单元测试 26 项另计，全部通过）。

## 6. 配置（环境变量）

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `PG_DSN` | 本地开发 DSN | 源库 libpq 连接串，Compose 中指向 `postgres` 服务 |
| `SQLITE_PATH` | `/tmp/capture/history.db` | 历史库路径（Compose 中为 `/data/history.db`） |
| `REPLICATION_SLOT` | `artifacts_history_slot` | 逻辑复制槽名 |
| `SOURCE_TABLE` | `artifacts` | 捕获的源表（结构固定） |
| `REPL_STATUS_INTERVAL_S` | `5` | 复制协议 keepalive 间隔 |
| `REPL_RECONNECT_BACKOFF_S` | `2` | 断线重连退避秒数 |
| `NOTIFIER_URL` | `http://127.0.0.1:8090/notify` | 固定接收端通知地址（Compose 指向 `receiver` 服务） |
| `NOTIFIER_TIMEOUT_S` | `5` | 单次通知 HTTP 超时 |
| `NOTIFIER_POLL_S` | `1` | 发件箱空闲轮询/兜底唤醒间隔 |
| `NOTIFIER_BACKOFF_S` | `2` | 通知失败指数退避基数（上限 `NOTIFIER_MAX_BACKOFF_S=60`） |
| `RECEIVER_HOST` / `RECEIVER_PORT` | `0.0.0.0` / `8090` | 接收端监听地址（仅 receiver 服务使用） |
| `RECEIVER_DB` | `/data/receiver.db` | 接收端 SQLite 路径（仅 receiver 服务使用） |
| `CAPTURE_INIT_SLEEP_MS` | `0` | 测试钩子：拉长初始化窗口 |
| `TEST_INFLIGHT_ARTIFACT_NO` | `0`(关) | 测试钩子：跨基线事务编号 |
| `TEST_INFLIGHT_SLEEP_S` | `3` | 测试钩子：在途事务持有秒数 |

## 7. 已知边界 / 未完成项

- 逻辑解码使用 PostgreSQL 内置 `test_decoding`（无需安装插件），解析器
  依赖本项目固定的四列结构；表结构变更不在支持范围内（需求明确禁止 DDL）。
- 历史库为单进程 SQLite，适合值班排查类读多写少场景；如需多副本水平扩展
  需更换存储层。
- 复制槽若被人工从源库删除，应用进入 ERROR 并在 `/api/progress` 暴露，
  不会自动重建（重建会使旧 LSN 失效，需人工确认后清空历史库重新基线）。
- 通知为至少一次（at-least-once）语义：正确性依赖接收端按
  `notification_seq` 幂等；接收端固定示例仅去重安排“重建”，不真正执行
  部署流水线动作，接入真实流水线时替换 `app/receiver/` 即可，发件箱与
  通知线程无需改动。
- 通知仅覆盖含目标表行变更的已提交事务；无相关变更的空事务不产生通知。
- 开发沙箱未安装 Docker 守护进程，`docker compose up` 未在该沙箱实跑；
  Compose 语法已用 YAML 解析校验，三个服务的启动方式、健康检查、卷与
  环境变量均与本机端到端验证（便携 PostgreSQL 15 + uvicorn + 真实 HTTP
  接收端，54 项断言全过）使用的配置一致，镜像基础为官方
  `python:3.12-slim` 与 `postgres:16-bookworm`。
