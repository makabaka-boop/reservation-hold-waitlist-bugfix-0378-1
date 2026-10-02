"""并发语义测试：同一空档只有一个保留，其余严格按入队顺序候补。"""

import threading
from concurrent.futures import ThreadPoolExecutor

import pytest
from fastapi.testclient import TestClient

from app.main import create_app


@pytest.fixture
def client(tmp_path):
    app = create_app(str(tmp_path / "c.db"), room_count=2, promotion_ttl=5)
    with TestClient(app) as c:
        yield c


def _book(client, room, s, e, ttl=1000, key=None):
    headers = {"Idempotency-Key": key} if key else {}
    r = client.post(
        "/bookings",
        json={"room": room, "start_tick": s, "end_tick": e, "ttl": ttl},
        headers=headers,
    )
    return r.status_code, r.json()


def test_concurrent_requests_same_slot_single_hold_fifo_waiting(client, n=24):
    barrier = threading.Barrier(n)

    def attempt(i):
        local = TestClient(client.app)
        barrier.wait()
        return _book(local, 0, 10, 20)

    with ThreadPoolExecutor(max_workers=n) as pool:
        results = list(pool.map(attempt, range(n)))

    statuses = [b["status"] for _, b in results]
    assert statuses.count("held") == 1
    assert statuses.count("waiting") == n - 1

    st = client.get("/state").json()
    waiting = sorted(
        (b for b in st["bookings"] if b["status"] == "waiting"),
        key=lambda b: b["id"],
    )
    # 候补顺序 = 自增 id 顺序，且 id 连续
    ids = [b["id"] for b in waiting]
    assert ids == sorted(ids)
    assert ids == list(range(min(ids), max(ids) + 1))


def test_concurrent_same_idempotency_key_single_booking(client, n=16):
    barrier = threading.Barrier(n)

    def attempt(i):
        local = TestClient(client.app)
        barrier.wait()
        return _book(local, 1, 0, 5, key="same-key")

    with ThreadPoolExecutor(max_workers=n) as pool:
        results = list(pool.map(attempt, range(n)))

    # 全部返回同一个申请，没有载荷冲突
    assert all(code == 201 for code, _ in results)
    booking_ids = {b["id"] for _, b in results}
    assert booking_ids == {results[0][1]["id"]}
    st = client.get("/state").json()
    assert len(st["bookings"]) == 1
    assert st["bookings"][0]["status"] == "held"


def test_concurrent_then_expire_promotes_exactly_one(client, n=16):
    barrier = threading.Barrier(n)

    def attempt(i):
        local = TestClient(client.app)
        barrier.wait()
        return _book(local, 0, 10, 20, ttl=1)

    with ThreadPoolExecutor(max_workers=n) as pool:
        list(pool.map(attempt, range(n)))

    r = client.post("/clock/advance", json={"ticks": 1})
    promoted = [p["id"] for p in r.json()["promoted"].get("0", [])]
    assert len(promoted) == 1
    expired = [b["id"] for b in r.json()["expired"]]
    assert len(expired) == 1

    st = client.get("/state").json()
    counts = {}
    for b in st["bookings"]:
        counts[b["status"]] = counts.get(b["status"], 0) + 1
    assert counts == {"expired": 1, "held": 1, "waiting": n - 2}
