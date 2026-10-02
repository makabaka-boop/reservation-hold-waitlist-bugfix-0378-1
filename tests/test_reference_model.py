"""差分测试：用独立实现的参考模型，逐步驱动真实服务并核对。

参考模型用纯 Python 重写同一套规则（半开区间重叠、FIFO 候补、
不拆分晋升、过期链、幂等），与 FastAPI/SQLite 服务相互独立。
每一步操作后：
  - 串行操作：逐个比对响应体；
  - 并发批次：比对每个结果的多重集与最终 /state；
  - 定期"重启"（重建 App）后比对完整状态。
"""

from __future__ import annotations

import random
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest
from fastapi.testclient import TestClient

from app.main import create_app

PROMOTION_TTL = 5
ROOMS = 3


# ---------------- 参考模型 ----------------

class RefBooking:
    def __init__(self, bid, room, s, e, status, expires_at, created_at):
        self.id = bid
        self.room = room
        self.start_tick = s
        self.end_tick = e
        self.status = status
        self.expires_at = expires_at
        self.created_at = created_at

    def dict(self):
        return {
            "id": self.id,
            "room": self.room,
            "start_tick": self.start_tick,
            "end_tick": self.end_tick,
            "status": self.status,
            "expires_at": self.expires_at,
            "created_at": self.created_at,
        }


def _overlaps(a_s, a_e, b_s, b_e):
    return a_s < b_e and b_s < a_e


class ReferenceModel:
    def __init__(self):
        self.now = 0
        self.next_id = 1
        self.bookings: dict[int, RefBooking] = {}
        self.idem: dict[str, tuple[str, int, dict]] = {}

    def _overlap(self, room, s, e):
        return any(
            b.room == room and b.status in ("held", "reserved")
            and _overlaps(s, e, b.start_tick, b.end_tick)
            for b in self.bookings.values()
        )

    def _scan(self, room):
        promoted = []
        waiting = sorted(
            (b for b in self.bookings.values()
             if b.room == room and b.status == "waiting"),
            key=lambda b: b.id,
        )
        for b in waiting:
            if b.start_tick < self.now:
                continue
            if self._overlap(room, b.start_tick, b.end_tick):
                continue
            b.status = "held"
            b.expires_at = self.now + PROMOTION_TTL
            promoted.append(b.dict())
        return promoted

    def _expire_stale_waiting(self, room=None):
        stale = sorted(
            (b for b in self.bookings.values()
             if b.status == "waiting" and b.start_tick < self.now
             and (room is None or b.room == room)),
            key=lambda b: b.id,
        )
        for b in stale:
            b.status = "expired"
            b.expires_at = None
        return stale

    # 每个操作对应一个事务，返回 (status_code, body)
    def create(self, room, s, e, ttl, key, fp):
        if key is not None and key in self.idem:
            saved_fp, code, body = self.idem[key]
            if saved_fp != fp:
                return 409, {"error": {"code": "idempotency_conflict",
                                       "message": "x"}}
            return code, body

        code, body = 201, None
        if not (0 <= room < ROOMS) or ttl <= 0 or s >= e or s < self.now:
            code, body = 400, {"error": {"code": "validation_error", "message": "x"}}
        else:
            if self._overlap(room, s, e):
                status, exp = "waiting", None
            else:
                status, exp = "held", self.now + ttl
            b = RefBooking(self.next_id, room, s, e, status, exp, self.now)
            self.next_id += 1
            self.bookings[b.id] = b
            body = b.dict()
        if key is not None:
            self.idem[key] = (fp, code, body)
        return code, body

    def confirm(self, bid):
        b = self.bookings.get(bid)
        if b is None:
            return 404, {"error": {"code": "not_found", "message": "x"}}
        if b.status == "reserved":
            return 409, {"error": {"code": "conflict", "message": "x"}}
        if b.status != "held":
            return 409, {"error": {"code": "conflict", "message": "x"}}
        b.status = "reserved"
        b.expires_at = None
        return 200, b.dict()

    def cancel(self, bid):
        b = self.bookings.get(bid)
        if b is None:
            return 404, {"error": {"code": "not_found", "message": "x"}}
        if b.status in ("expired", "cancelled"):
            return 409, {"error": {"code": "conflict", "message": "x"}}
        was_active = b.status in ("held", "reserved")
        b.status = "cancelled"
        b.expires_at = None
        promoted = {}
        if was_active:
            self._expire_stale_waiting(b.room)
            p = self._scan(b.room)
            if p:
                promoted[str(b.room)] = p
        return 200, {"booking": b.dict(), "promoted": promoted}

    def shorten(self, bid, s, e):
        b = self.bookings.get(bid)
        if b is None:
            return 404, {"error": {"code": "not_found", "message": "x"}}
        if b.status not in ("held", "reserved"):
            return 409, {"error": {"code": "conflict", "message": "x"}}
        if s >= e:
            return 400, {"error": {"code": "validation_error", "message": "x"}}
        if s < b.start_tick or e > b.end_tick:
            return 400, {"error": {"code": "validation_error", "message": "x"}}
        b.start_tick, b.end_tick = s, e
        self._expire_stale_waiting(b.room)
        p = self._scan(b.room)
        promoted = {str(b.room): p} if p else {}
        return 200, {"booking": b.dict(), "promoted": promoted}

    def advance(self, ticks=None, to=None):
        if ticks is None and to is None:
            return 422, None
        target = self.now + ticks if ticks is not None else to
        if target is None or target < self.now:
            return 400, {"error": {"code": "validation_error", "message": "x"}}
        if target == self.now:
            return 200, {"now": self.now, "expired": [], "promoted": {}}
        self.now = target
        expired = sorted(
            (b for b in self.bookings.values()
             if b.status == "held" and b.expires_at <= target),
            key=lambda b: b.id,
        )
        for b in expired:
            b.status = "expired"
            b.expires_at = None
        stale_waiting = self._expire_stale_waiting()
        expired = sorted([*expired, *stale_waiting], key=lambda b: b.id)
        promoted = {}
        for room in sorted({b.room for b in expired}):
            p = self._scan(room)
            if p:
                promoted[str(room)] = p
        return 200, {
            "now": target,
            "expired": [b.dict() for b in expired],
            "promoted": promoted,
        }

    def state(self):
        return {
            "now": self.now,
            "room_count": ROOMS,
            "bookings": [
                self.bookings[i].dict()
                for i in sorted(self.bookings)
            ],
        }


# ---------------- 测试辅助 ----------------

def fp_create(room, s, e, ttl):
    import hashlib
    import json
    body = {"room": room, "start_tick": s, "end_tick": e, "ttl": ttl}
    blob = json.dumps(
        {"method": "POST", "path": "/bookings", "query": "", "body": body},
        sort_keys=True, separators=(",", ":"),
    ).encode()
    return hashlib.sha256(blob).hexdigest()


def normalize_state(st):
    return (st["now"], st["room_count"], [tuple(sorted(b.items())) for b in st["bookings"]])


def assert_same_state(client, ref):
    actual = client.get("/state").json()
    assert normalize_state(actual) == normalize_state(ref.state())


def assert_invariants(ref):
    # 1. 同房间 held/reserved 两两不重叠
    active = [b for b in ref.bookings.values() if b.status in ("held", "reserved")]
    for i, a in enumerate(active):
        for b in active[i + 1:]:
            if a.room == b.room:
                assert not _overlaps(
                    a.start_tick, a.end_tick, b.start_tick, b.end_tick
                ), f"重叠: {a.dict()} vs {b.dict()}"
    # 2. held 必须未过期且 expires_at > now
    for b in active:
        if b.status == "held":
            assert b.expires_at is not None and b.expires_at > ref.now
    # 3. 候补按 id FIFO
    waiting = [b for b in ref.bookings.values() if b.status == "waiting"]
    assert [b.id for b in waiting] == sorted(b.id for b in waiting)
    # 4. 候补必须仍可完整使用未来时段
    assert all(b.start_tick >= ref.now for b in waiting)


# ---------------- 差分测试 ----------------

@pytest.mark.parametrize("seed", range(30))
def test_differential_against_reference_model(tmp_path, seed):
    db = str(tmp_path / f"diff-{seed}.db")
    app = create_app(db, room_count=ROOMS, promotion_ttl=PROMOTION_TTL)
    client = TestClient(app)
    ref = ReferenceModel()
    rng = random.Random(seed)

    def api_create(room, s, e, ttl, key):
        headers = {"Idempotency-Key": key} if key else {}
        r = client.post(
            "/bookings",
            json={"room": room, "start_tick": s, "end_tick": e, "ttl": ttl},
            headers=headers,
        )
        return r.status_code, r.json()

    def random_interval():
        s = rng.randrange(0, 40)
        e = s + rng.choice([1, 2, 3, 5, 10, 20])
        return s, e

    # key -> 首次请求载荷 (room, s, e, ttl)
    key_payload: dict[str, tuple] = {}
    steps = 120

    for step in range(steps):
        kind = rng.random()

        # --- 并发同档批次（约 12%）---
        if kind < 0.12:
            n = rng.randint(4, 12)
            room = rng.randrange(ROOMS)
            s, e = random_interval()
            ttl = rng.choice([2, 3, 5, 1000])
            barrier = threading.Barrier(n)

            def attempt(_):
                local = TestClient(app)
                barrier.wait()
                r = local.post(
                    "/bookings",
                    json={"room": room, "start_tick": s, "end_tick": e, "ttl": ttl},
                )
                return r.status_code, r.json()

            with ThreadPoolExecutor(max_workers=n) as pool:
                api_results = list(pool.map(attempt, range(n)))
            assert [c for c, _ in api_results] == [201] * n

            # 模型顺序应用（等价于某一串行化顺序）
            ref_results = [
                ref.create(room, s, e, ttl, None, fp_create(room, s, e, ttl))
                for _ in range(n)
            ]
            # 最终状态与模型一致；held/waiting 数量逐一对齐
            # （空档为空时恰好 1 held + n-1 waiting；被占用时全部 waiting）
            assert_same_state(client, ref)
            api_counts = (
                sum(b["status"] == "held" for _, b in api_results),
                sum(b["status"] == "waiting" for _, b in api_results),
            )
            ref_counts = (
                sum(b["status"] == "held" for _, b in ref_results),
                sum(b["status"] == "waiting" for _, b in ref_results),
            )
            assert api_counts == ref_counts
            assert api_counts[0] <= 1  # 并发同档至多一个拿到保留

        # --- 普通创建（约 40%，其中部分带幂等键/重试）---
        elif kind < 0.52:
            room = rng.randrange(ROOMS)
            s, e = random_interval()
            ttl = rng.choice([2, 3, 5, 10, 1000])
            key = None
            invalid_room = rng.random() < 0.05
            if invalid_room:
                room = ROOMS + 1  # 服务层 400；非法载荷不挂幂等键
            elif key_payload and rng.random() < 0.30:
                # 复用已有键：用与首次完全相同的载荷重放
                key = rng.choice(list(key_payload))
                room, s, e, ttl = key_payload[key]
            elif rng.random() < 0.30:
                key = f"seed{seed}-key{len(key_payload)}"
                key_payload[key] = (room, s, e, ttl)

            fp = fp_create(room, s, e, ttl)
            rc, rb = ref.create(room, s, e, ttl, key, fp)
            ac, ab = api_create(room, s, e, ttl, key)
            assert ac == rc, (step, (ac, ab), (rc, rb))
            if isinstance(ab, dict) and "error" in ab:
                assert ab["error"]["code"] == rb["error"]["code"]
            else:
                assert ab == rb

        # --- 时钟推进（约 18%）---
        elif kind < 0.70:
            if rng.random() < 0.7:
                ticks = rng.choice([1, 2, 3, 5])
                rc, rb = ref.advance(ticks=ticks)
                r = client.post("/clock/advance", json={"ticks": ticks})
            else:
                to = ref.now + rng.choice([0, 1, 4, 10])
                rc, rb = ref.advance(to=to)
                r = client.post("/clock/advance", json={"to": to})
            assert r.status_code == rc
            assert r.json() == rb

        # --- 确认（约 12%）---
        elif kind < 0.82:
            if not ref.bookings:
                continue
            bid = rng.choice(list(ref.bookings))
            rc, rb = ref.confirm(bid)
            r = client.post(f"/bookings/{bid}/confirm")
            assert r.status_code == rc, (step, r.json(), rb)
            if rc == 200:
                assert r.json() == rb

        # --- 取消（约 12%）---
        elif kind < 0.94:
            if not ref.bookings:
                continue
            bid = rng.choice(list(ref.bookings))
            rc, rb = ref.cancel(bid)
            r = client.post(f"/bookings/{bid}/cancel")
            assert r.status_code == rc, (step, r.json(), rb)
            if rc == 200:
                assert r.json() == rb

        # --- 缩短（约 6%）---
        else:
            candidates = [b for b in ref.bookings.values()
                          if b.status in ("held", "reserved")]
            if not candidates:
                continue
            b = rng.choice(candidates)
            # 一半合法缩短，一半尝试非法扩张
            if rng.random() < 0.5:
                ns = b.start_tick
                ne = rng.randint(b.start_tick + 1, b.end_tick)
            else:
                ns = b.start_tick
                ne = b.end_tick + rng.randint(1, 5)
            rc, rb = ref.shorten(b.id, ns, ne)
            r = client.post(
                f"/bookings/{b.id}/shorten",
                json={"start_tick": ns, "end_tick": ne},
            )
            assert r.status_code == rc, (step, r.json(), rb)
            if rc == 200:
                assert r.json() == rb

        assert_same_state(client, ref)
        assert_invariants(ref)

        # 定期模拟重启：重建 App（同一 DB 文件），结果不得变化
        if step % 25 == 24:
            client.close()
            client = TestClient(create_app(db, room_count=ROOMS,
                                           promotion_ttl=PROMOTION_TTL))
            assert_same_state(client, ref)

    client.close()
