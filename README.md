# 房间整数时段预约服务（FastAPI + SQLite）

管理 **1～10 个房间**的整数 tick 时间段预约。申请先取得**限时保留（held）**，
确认后成为**预约（reserved）**；与现有保留/预约冲突的申请进入**候补（waiting）**。
时钟由外部显式推进；保留过期、主动取消或缩短时段时，服务在**同一事务**中
释放容量并按入队顺序扫描候补，晋升当前能**完整容纳**的申请（不拆分时段）。

## 语义约定

- 时间为半开整数区间 `[start_tick, end_tick)`，相邻区间
  （`a.end == b.start`）**不冲突**；冲突只在**同一房间**内判定。
- 状态机：
  - `held`：限时保留，`expires_at = 创建时刻 + ttl`；到达保留截止点或预约
    起点被当前时钟越过后均不可确认；
  - `reserved`：已确认，不会因时钟推进自动过期；
  - `waiting`：候补，按自增 id（入队顺序）FIFO；预约起点被时钟越过后立即
    进入 `expired`，不能在已过去的时段上被晋升；
  - `expired` / `cancelled`：终态。
- **过期判定**：时钟推进到 `t` 时，所有 `expires_at <= t` 或 `start_tick < t`
  的 held 过期；所有 `start_tick < t` 的 waiting 过期。过期、扫描、晋升与时钟
  更新在同一 `BEGIN IMMEDIATE` 事务中完成。
- **候补晋升**：候补按 FIFO 逐个检查，只有仍能取得**当前或未来的完整时段**
  （`start_tick >= 当前时刻`）才晋升为 held（`expires_at = 当前时刻 +
  PROMOTION_TTL`）；已过去的候选项直接终态化，放不下的未来候选项保留 waiting
  并继续检查后面的申请——不拆分、不丢弃顺序。
- **取消/缩短**：仅 held/reserved 释放容量并触发同房间候补扫描；
  取消 waiting 不释放容量。缩短只允许在原区间范围内收紧，且缩短后的区间必须
  仍在当前时钟之后。
- **新申请**：不接受 `start_tick < 当前时钟` 的过去时段；`start_tick == 当前时钟`
  是最后可申请/晋升的边界。
- **并发**：所有写事务均为 `BEGIN IMMEDIATE`，SQLite 库级写锁将冲突申请
  串行化，因此同一空档的并发请求**恰好一个**拿到 held，其余按落库顺序
  成为候补（`busy_timeout=30s`）。
- **幂等键**：所有写接口接受 `Idempotency-Key` 头。
  - 同键 + 同方法/路径/查询/载荷 → 返回首次的状态码与响应体（原结果）；
  - 同键但载荷不同 → `409 idempotency_conflict`，不执行；
  - 幂等记录与业务结果同事务提交，业务错误也会被记录并重放。
- **持久化**：WAL + `synchronous=FULL`。时钟、保留/候补/终态、候补顺序、
  晋升结果、幂等记录全部落盘；进程重启后状态不变。

## 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/bookings` | 申请；body `{room,start_tick,end_tick,ttl}`，返回 201 + held/waiting |
| POST | `/bookings/{id}/confirm` | held → reserved |
| POST | `/bookings/{id}/cancel` | 取消；同事务扫描候补，返回 `{booking,promoted}` |
| POST | `/bookings/{id}/shorten` | body `{start_tick,end_tick}`，收紧区间并扫描候补 |
| POST | `/clock/advance` | body `{ticks}` 或 `{to}`（互斥，只进不退） |
| GET | `/bookings/{id}` | 单个申请 |
| GET | `/state` | `{now, room_count, bookings}` 全量状态 |
| GET | `/healthz` | 存活检查 |

`promoted` 形如 `{"<room>": [booking, ...]}`，只包含有晋升发生的房间。

## 运行

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements-dev.txt

DB_PATH=booking.db ROOM_COUNT=5 PROMOTION_TTL=5 \
  uvicorn app.main:app --host 0.0.0.0 --port 8000
```

## 测试

```bash
pytest
```

- `tests/test_api.py`：生命周期、相邻区间、跨房间隔离、FIFO、不拆分晋升、
  过期链、缩短、幂等、时钟校验；
- `tests/test_concurrency.py`：24 路并发同档单 held、同幂等键并发只落一条、
  过期后恰好晋升一个；
- `tests/test_persistence.py`：重启后保留/候补顺序/晋升结果/时钟/幂等不变；
- `tests/test_reference_model.py`：30 个随机种子的差分测试。用一个**独立
  纯 Python 参考模型**（自行实现重叠判定、FIFO、不拆分晋升、过期、幂等）
  驱动真实服务，含并发同档批次、逐步推进时钟与中途重启，逐步比对响应与
  `/state`，并校验"同房间活跃区间两两不重叠、held 必未过期、候补 FIFO"
  等不变量。
