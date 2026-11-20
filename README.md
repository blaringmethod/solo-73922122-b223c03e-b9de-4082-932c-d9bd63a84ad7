# 观测数据 SQL 清洗沙箱

面向采样数据工程师的外协清洗 SQL 试跑平台：上传观测 CSV、按站点提交只读
SQL，成功后下载结果 CSV 交给质控。基于 **FastAPI + DuckDB**，观测数据驻留
内存、每轮独立隔离执行；成功结果以文件留存，不占用进程内存。

## 功能与约束（与需求逐条对应）

| 需求 | 实现 |
| --- | --- |
| 上传至多 1 MiB 观测 CSV（站号、UTC时刻、温度） | `POST /api/upload`，超限返回 413；表头支持任意列序，UTF-8 编码 |
| 选站后提交 SQL，每轮输入固定为所选站点数据 | 每轮仅把该站点行装入该轮**私有 DuckDB 连接**的 `observations` 表 |
| 仅允许 SELECT / WITH，支持筛选、连接、聚合 | 静态 SQL 守卫（sqlparse 单语句判定 + 关键字/模式黑名单） |
| 不得读写服务器文件、联网、加载扩展 | 守卫拦截 `read_csv`/`COPY`/`ATTACH`/`LOAD`/`INSTALL`/`http_get` 等；DuckDB 连接再以 `enable_external_access=false` 兜底关闭文件系统、网络与扩展 |
| 不得访问其他轮次数据 | 每轮独立线程 + 独立内存库 + 独立数据副本，其他轮次的连接不可达 |
| 最多两轮并行，满额拒绝 | 注册表实时统计活动轮次，第 3 个请求返回 503 |
| 执行满 5 秒终止 | 看门狗在 5 秒调用 DuckDB 原生 `interrupt()` 立即中止，状态置 `failed` |
| 结果超过 8 MiB 整轮失败 | 边写盘边累计字节，超限即失败并删除未发布的 spool |
| 可查询状态、可取消 | `GET /api/rounds/{id}`、`POST /api/rounds/{id}/cancel` |
| 取消受理不等执行结束；实际退出后才报取消并释放名额；迟到结果不开放 | 取消立即返回 `accepted`，监督线程等 worker 真正退出后才置 `cancelled`；取消优先于同时到达的结果 |
| 成功先发布则不可取消 | 已 `succeeded` 的轮次取消返回 409，结果仍可下载 |
| 失败/超时不供部分结果 | 失败轮次结果接口返回 409 |
| 最近 100 个已结束轮次可查询；成功结果可下载 | 注册表保留最近 `MAX_FINISHED_ROUNDS=100` 个终结轮次，成功轮次的结果文件发布为 `<round_id>.csv` |
| 留存不随历史 CSV 总量增长进程内存 | 结果边算边写入磁盘 spool（每批 ≤ 1 批内存），完成后 `rename` 发布；下载按 256 KiB 分块流式读取，进程内只保留一块缓冲 |
| 结果写入失败 ⇒ 轮次失败 | spool 创建/写入/发布任一失败即 `failed`，不发布半成品；未发布的 spool 一律删除 |
| 淘汰旧轮次时回收结果；进行中的下载可读完 | 超过 100 个终结轮次时按结束时间淘汰并 `unlink` 结果文件；下载先取得文件描述符，POSIX 下 `unlink` 不影响已开始的读取 |
| 慢查询不挡另一轮、状态查询、已有结果下载 | 每轮独立线程；DuckDB 执行 C++ 期间释放 GIL 真并行；HTTP 接口均为短操作 |

## 目录结构

```
app/
  config.py       限额常量（1 MiB / 5 s / 8 MiB / 2 并行）
  csv_io.py       CSV 解析与校验
  sql_guard.py    SQL 静态安全校验
  worker.py       每轮查询执行器（独立 DuckDB 连接、interrupt 中止、8MiB 判定）
  registry.py     轮次注册表、名额控制、状态机、看门狗与监督线程
  store.py        内存数据集（按站点切分）
  results.py      结果留存（磁盘 spool/发布/流式下载/回收）
  main.py         FastAPI 路由
  static/index.html  演示网页（上传/选站/提交/轮询/取消/下载）
sample_data/observations.csv  演示数据（3 个站点 9 行）
tests/            76 个 pytest 用例（SQL 守卫、CSV、端到端 API、结果留存）
Dockerfile, docker-compose.yml
```

## 本地运行（无需 Docker）

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 8000
# 浏览器打开 http://localhost:8000
```

## Docker Compose 运行

```bash
docker compose up --build
# 打开 http://localhost:8000
```

Compose 额外做了加固：只读根文件系统、`no-new-privileges`、丢弃所有 Linux
capability、512 MiB 内存上限、内置健康检查。

唯一可写的路径是具名卷 `results:/var/lib/sql-sandbox/results`
（`RESULT_DIR`）。观测数据仍只驻留内存；成功轮次的结果 CSV 写到该卷，因此
保留 100 轮历史不会占用进程内存。卷只保存结果，删除容器/卷即可清空。

## HTTP API 摘要

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/` | 演示网页 |
| GET | `/api/health` | 健康状态、当前活动轮数、站点列表 |
| POST | `/api/upload` | multipart 上传 CSV（≤1 MiB） |
| POST | `/api/rounds` | 提交 `{station_id, sql}` 创建一轮；满额 503、SQL 非法 400 |
| GET | `/api/rounds` / `/api/rounds/{id}` | 轮次列表 / 单轮状态 |
| POST | `/api/rounds/{id}/cancel` | 请求取消（成功轮返回 409） |
| GET | `/api/rounds/{id}/result` | 下载结果 CSV（仅 succeeded 可用） |

轮次状态：`pending` → `running` → `succeeded` / `failed` / `cancelled`。

## 结果留存与内存

* 执行期间 worker 将 CSV 逐批写入 `<RESULT_DIR>/<round_id>.part`，只有完整
  写完并在 8 MiB 上限内才会 `os.replace` 成 `<round_id>.csv` 对外发布；失败、
  超时、取消或写盘出错都会删除 spool。
* `GET /api/rounds/{id}` 只返回元数据（行数、字节数、下载地址），CSV 内容
  从不放在 Python 堆里。
* 下载使用 `StreamingResponse`，每次从磁盘读 256 KiB，因此并发下载只占用少量
  内存；响应带 `Content-Length`。
* 每条终结轮次都会触发一次淘汰检查：仅保留最近 100 个，其余连同结果文件一起
  删除。下载在淘汰前已打开文件描述符，仍可完整读完。
* 服务启动时清空结果目录中的遗留 `.part`/`.csv`（注册表在内存中，重启后这些
  文件本就不可达）。

## 测试

```bash
pip install pytest httpx
python -m pytest tests/ -q
```

## 设计说明：为什么用线程 + interrupt 而非子进程

首选方案本为每轮独立子进程，以便从进程边界硬终止。当前构建/运行环境的
seccomp 策略禁止向子进程发送信号（`SIGTERM/SIGKILL` 均返回 EPERM），硬杀
进程不可行。因此改为：每轮一个守护线程持有**独立的内存 DuckDB 连接**，
超时/取消时调用 DuckDB 官方的 `connection.interrupt()`，查询在 C++ 层立即
抛 `InterruptException` 退出（已实测中断延迟 < 10 ms）。轮次间无共享连接、
无共享表，数据隔离不依赖进程边界；DuckDB 执行期间释放 GIL，两个慢查询可
真正并行。
