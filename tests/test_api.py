import pytest
from fastapi.testclient import TestClient

from app.main import create_app


@pytest.fixture
def client(tmp_path):
    db = tmp_path / "test.db"
    app = create_app(str(db), room_count=3, promotion_ttl=5)
    with TestClient(app) as c:
        yield c


def create(client, room, s, e, ttl=5, key=None, expected=201):
    headers = {"Idempotency-Key": key} if key else {}
    r = client.post(
        "/bookings",
        json={"room": room, "start_tick": s, "end_tick": e, "ttl": ttl},
        headers=headers,
    )
    assert r.status_code == expected, r.text
    return r.json()


def state(client):
    return client.get("/state").json()


def by_id(st):
    return {b["id"]: b for b in st["bookings"]}


def test_held_confirm_lifecycle(client):
    b = create(client, 0, 10, 20, ttl=5)
    assert b["status"] == "held"
    assert b["expires_at"] == 5
    r = client.post(f"/bookings/{b['id']}/confirm")
    assert r.status_code == 200
    assert r.json()["status"] == "reserved"
    assert r.json()["expires_at"] is None
    # 重复确认 409
    r = client.post(f"/bookings/{b['id']}/confirm")
    assert r.status_code == 409


def test_adjacent_half_open_intervals_do_not_conflict(client):
    # [10,20) 占用；紧邻的 [20,30) 不重叠
    create(client, 0, 10, 20)
    b2 = create(client, 0, 20, 30)
    assert b2["status"] == "held"
    # 真重叠才入候补
    b3 = create(client, 0, 19, 21)
    assert b3["status"] == "waiting"


def test_different_rooms_are_isolated(client):
    create(client, 0, 0, 100)
    for room in (1, 2):
        assert create(client, room, 0, 100)["status"] == "held"


def test_overlapping_requests_go_to_fifo_waitlist(client):
    create(client, 0, 0, 100)
    w1 = create(client, 0, 0, 50)
    w2 = create(client, 0, 50, 60)
    w3 = create(client, 0, 0, 100)
    assert [b["status"] for b in (w1, w2, w3)] == ["waiting"] * 3
    st = state(client)
    waiting = [b["id"] for b in st["bookings"] if b["status"] == "waiting"]
    assert waiting == sorted(waiting) == [w1["id"], w2["id"], w3["id"]]


def test_cancel_promotes_fifo_in_single_transaction(client):
    holder = create(client, 0, 100, 200)
    w1 = create(client, 0, 100, 200)   # 与整个区间等长
    w2 = create(client, 0, 100, 110)    # 小，但排在后面
    r = client.post(f"/bookings/{holder['id']}/cancel")
    promoted = r.json()["promoted"]["0"]
    # 只能完整容纳：w1 占满后 w2 放不下；不拆分
    assert [p["id"] for p in promoted] == [w1["id"]]
    st = by_id(state(client))
    assert st[w1["id"]]["status"] == "held"
    assert st[w1["id"]]["expires_at"] == 5  # now=0 + promotion_ttl
    assert st[w2["id"]]["status"] == "waiting"


def test_scan_continues_past_non_fitting_entries(client):
    # 释放一个小空档：队首放不下应跳过，队中能放下的继续晋升
    big = create(client, 0, 100, 200)
    w1 = create(client, 0, 100, 200)  # 大，永远放不下
    w2 = create(client, 0, 190, 200)  # 小，能放进 [190,200)
    w3 = create(client, 0, 195, 200)  # w2 晋升后它放不下
    client.post(f"/bookings/{big['id']}/shorten", json={
        "start_tick": 100, "end_tick": 190,
    })
    st = by_id(state(client))
    assert st[w1["id"]]["status"] == "waiting"
    assert st[w2["id"]]["status"] == "held"
    assert st[w3["id"]]["status"] == "waiting"


def test_expiry_chain_on_clock_advance(client):
    # held [10,110) ttl=3 -> 3 个候补排成链
    create(client, 0, 10, 110, ttl=3)
    w1 = create(client, 0, 10, 110)
    w2 = create(client, 0, 10, 110)
    w3 = create(client, 0, 10, 110)

    r = client.post("/clock/advance", json={"ticks": 3})
    body = r.json()
    assert body["now"] == 3
    assert [b["id"] for b in body["expired"]] == [1]
    assert [p["id"] for p in body["promoted"]["0"]] == [w1["id"]]
    st = by_id(state(client))
    assert st[w1["id"]]["status"] == "held"
    assert st[w1["id"]]["expires_at"] == 8  # 3 + 5
    assert st[w2["id"]]["status"] == "waiting"
    assert st[w3["id"]]["status"] == "waiting"

    # 再推进 5：w1 过期 -> w2 晋升（expires_at=13）
    r = client.post("/clock/advance", json={"ticks": 5})
    assert [p["id"] for p in r.json()["promoted"]["0"]] == [w2["id"]]
    st = by_id(state(client))
    assert st[w2["id"]]["expires_at"] == 13
    assert st[w3["id"]]["status"] == "waiting"

    # 确认 w2 后再推进，w3 必须继续等待（reserved 不过期）
    client.post(f"/bookings/{w2['id']}/confirm")
    r = client.post("/clock/advance", json={"ticks": 1})
    assert r.json()["promoted"] == {}
    assert by_id(state(client))[w3["id"]]["status"] == "waiting"


def test_clock_jump_expires_past_waitlist_and_promotes_only_future_slots(client):
    morning_hold = create(client, 0, 10, 20, ttl=15)
    stale1 = create(client, 0, 10, 20)
    stale2 = create(client, 0, 10, 19)
    # 推进到 15 时仍可完整使用的候补（起点 == 当前时钟），以及排在其后的候补
    usable = create(client, 0, 15, 20)
    later = create(client, 0, 16, 20)
    # 相邻下午时段申请本就直接 held；给足 ttl，时钟跳到 15 不受影响
    afternoon = create(client, 0, 20, 30, ttl=100)

    r = client.post("/clock/advance", json={"to": 15})
    body = r.json()
    assert [b["id"] for b in body["expired"]] == [
        morning_hold["id"], stale1["id"], stale2["id"]
    ]
    assert [p["id"] for p in body["promoted"]["0"]] == [usable["id"]]

    st = by_id(state(client))
    assert st[morning_hold["id"]]["status"] == "expired"
    assert st[stale1["id"]]["status"] == "expired"
    assert st[stale2["id"]]["status"] == "expired"
    assert st[usable["id"]]["status"] == "held"
    assert st[usable["id"]]["expires_at"] == 20
    assert st[later["id"]]["status"] == "waiting"   # 被 usable 占住
    assert st[afternoon["id"]]["status"] == "held"  # 相邻不冲突，未被误过期

    # 过期上午保留即使 ttl 尚未到也不可确认；新晋升的可用保留此刻可确认。
    assert client.post(f"/bookings/{morning_hold['id']}/confirm").status_code == 409
    r = client.post(f"/bookings/{usable['id']}/confirm")
    assert r.status_code == 200
    assert r.json()["status"] == "reserved"
    assert client.post(
        "/bookings", json={"room": 0, "start_tick": 14, "end_tick": 15, "ttl": 1}
    ).status_code == 400

    # 再越过时段起点：waiting 终态化；reserved 永久保留，下午 held 仍有效
    body = client.post("/clock/advance", json={"to": 20}).json()
    assert [b["id"] for b in body["expired"]] == [later["id"]]
    st = by_id(state(client))
    assert st[usable["id"]]["status"] == "reserved"
    assert st[later["id"]]["status"] == "expired"
    assert st[afternoon["id"]]["status"] == "held"
    assert client.post(f"/bookings/{usable['id']}/confirm").status_code == 409


def test_clock_jump_handles_multiple_waitlists_independently(client):
    holds = [create(client, room, 10, 20, ttl=15) for room in (0, 1)]
    stale = [create(client, room, 10, 20) for room in (0, 1)]
    usable = [create(client, room, 15, 20) for room in (0, 1)]

    body = client.post("/clock/advance", json={"to": 15}).json()
    st = by_id(state(client))
    for room, hold, old, new in zip((0, 1), holds, stale, usable):
        assert st[hold["id"]]["status"] == "expired"
        assert st[old["id"]]["status"] == "expired"
        assert st[new["id"]]["status"] == "held"
        assert [p["id"] for p in body["promoted"][str(room)]] == [new["id"]]


def test_cancelling_historical_reservation_promotes_future_waiter(client):
    reservation = create(client, 0, 10, 30, ttl=15)
    stale_wait = create(client, 0, 10, 20)
    future_wait = create(client, 0, 20, 30)
    client.post(f"/bookings/{reservation['id']}/confirm")

    body = client.post("/clock/advance", json={"to": 20}).json()
    assert [b["id"] for b in body["expired"]] == [stale_wait["id"]]
    assert body["promoted"] == {}

    r = client.post(f"/bookings/{reservation['id']}/cancel")
    assert [p["id"] for p in r.json()["promoted"]["0"]] == [future_wait["id"]]
    assert by_id(state(client))[future_wait["id"]]["status"] == "held"


def test_clock_advance_idempotent_replay_does_not_reapply_at_later_now(client):
    hold = create(client, 0, 10, 20, ttl=5)
    waiter = create(client, 0, 10, 20)

    first = client.post(
        "/clock/advance", json={"to": 5}, headers={"Idempotency-Key": "advance-5"}
    )
    assert first.status_code == 200
    first_body = first.json()

    # 另一个请求继续推进；之后重放旧键，必须返回首次响应且不改变当前时钟/状态。
    client.post("/clock/advance", json={"to": 20})
    replay = client.post(
        "/clock/advance", json={"to": 5}, headers={"Idempotency-Key": "advance-5"}
    )
    assert replay.status_code == 200
    assert replay.json() == first_body

    st = state(client)
    assert st["now"] == 20
    statuses = by_id(st)
    assert statuses[hold["id"]]["status"] == "expired"
    assert statuses[waiter["id"]]["status"] == "expired"


def test_shorten_releases_tail_and_promotes(client):
    h = create(client, 0, 100, 200, ttl=1000)
    w = create(client, 0, 180, 190)
    r = client.post(f"/bookings/{h['id']}/shorten", json={
        "start_tick": 100, "end_tick": 180,
    })
    assert [p["id"] for p in r.json()["promoted"]["0"]] == [w["id"]]
    # 不允许扩张
    r = client.post(f"/bookings/{h['id']}/shorten", json={
        "start_tick": 100, "end_tick": 181,
    })
    assert r.status_code == 400


def test_cannot_confirm_or_shorten_hold_after_slot_start(client):
    hold = create(client, 0, 10, 20, ttl=100)
    client.post("/clock/advance", json={"to": 10})
    assert client.post(f"/bookings/{hold['id']}/confirm").status_code == 409
    r = client.post(
        f"/bookings/{hold['id']}/shorten",
        json={"start_tick": 10, "end_tick": 11},
    )
    assert r.status_code == 400


def test_cancel_waiting_does_not_release_capacity(client):
    create(client, 0, 100, 200)
    w1 = create(client, 0, 100, 150)
    w2 = create(client, 0, 150, 160)
    r = client.post(f"/bookings/{w1['id']}/cancel")
    assert r.json()["promoted"] == {}
    assert by_id(state(client))[w2["id"]]["status"] == "waiting"


def test_idempotent_retry_returns_original_result(client):
    b1 = create(client, 1, 0, 10, key="k-1")
    b2 = create(client, 1, 0, 10, key="k-1")
    assert b1 == b2
    # 同键换载荷 -> 409，且不产生新申请
    r = client.post(
        "/bookings",
        json={"room": 1, "start_tick": 0, "end_tick": 11, "ttl": 5},
        headers={"Idempotency-Key": "k-1"},
    )
    assert r.status_code == 409
    assert len(state(client)["bookings"]) == 1


def test_idempotent_error_is_replayed_and_committed(client):
    # 首次请求非法房间 -> 400，记录幂等结果
    r1 = client.post(
        "/bookings",
        json={"room": 99, "start_tick": 0, "end_tick": 1, "ttl": 5},
        headers={"Idempotency-Key": "bad-room"},
    )
    assert r1.status_code == 400
    r2 = client.post(
        "/bookings",
        json={"room": 99, "start_tick": 0, "end_tick": 1, "ttl": 5},
        headers={"Idempotency-Key": "bad-room"},
    )
    assert r2.status_code == 400
    assert r1.json() == r2.json()
    assert len(state(client)["bookings"]) == 0


def test_clock_cannot_go_backward_and_advance_to(client):
    client.post("/clock/advance", json={"ticks": 10})
    r = client.post("/clock/advance", json={"to": 9})
    assert r.status_code == 400
    r = client.post("/clock/advance", json={"to": 20})
    assert r.status_code == 200
    assert r.json()["now"] == 20


def test_validation_and_404(client):
    assert client.post(
        "/bookings", json={"room": 0, "start_tick": 5, "end_tick": 5, "ttl": 1}
    ).status_code == 422
    assert client.post(
        "/bookings", json={"room": 0, "start_tick": 5, "end_tick": 4, "ttl": 1}
    ).status_code == 422
    assert client.get("/bookings/999").status_code == 404
