"""重启不变性：保留、候补顺序、晋升结果、时钟、幂等记录均持久化。"""

from fastapi.testclient import TestClient

from app.main import create_app


def test_restart_preserves_everything(tmp_path):
    db = str(tmp_path / "persist.db")

    def fresh():
        return TestClient(create_app(db, room_count=2, promotion_ttl=5))

    c = fresh()
    # held ttl=10，候补两人，另一房间一条 reserved
    h = c.post("/bookings", json={"room": 0, "start_tick": 0, "end_tick": 100, "ttl": 10}).json()
    w1 = c.post("/bookings", json={"room": 0, "start_tick": 0, "end_tick": 60, "ttl": 1000}).json()
    w2 = c.post("/bookings", json={"room": 0, "start_tick": 60, "end_tick": 100, "ttl": 1000}).json()
    r = c.post("/bookings", json={"room": 1, "start_tick": 0, "end_tick": 5, "ttl": 10}).json()
    c.post(f"/bookings/{r['id']}/confirm")

    # 缩短为 [0,50)：w1 [0,60) 放不下；w2 [60,100) 能放进腾出的尾部，被晋升
    sh = c.post(f"/bookings/{h['id']}/shorten", json={"start_tick": 0, "end_tick": 50})
    assert [p["id"] for p in sh.json()["promoted"].get("0", [])] == [w2["id"]]

    # 幂等键首次结果
    k1 = c.post(
        "/bookings",
        json={"room": 1, "start_tick": 50, "end_tick": 60, "ttl": 10},
        headers={"Idempotency-Key": "k1"},
    ).json()

    before = c.get("/state").json()

    # ---- 模拟重启：全新进程式的 App/连接 ----
    c2 = fresh()
    after = c2.get("/state").json()
    assert before == after
    assert after["now"] == 0

    # 幂等键重放原结果（连 id 都不变）
    again = c2.post(
        "/bookings",
        json={"room": 1, "start_tick": 50, "end_tick": 60, "ttl": 10},
        headers={"Idempotency-Key": "k1"},
    )
    assert again.status_code == 201
    assert again.json() == k1
    # 同键换载荷仍然拒绝
    conflict = c2.post(
        "/bookings",
        json={"room": 1, "start_tick": 50, "end_tick": 61, "ttl": 10},
        headers={"Idempotency-Key": "k1"},
    )
    assert conflict.status_code == 409

    # 先推进到 5：w2（晋升保留 ttl=5）过期；h 仍持有 [0,50)，w1 仍放不下
    body = c2.post("/clock/advance", json={"to": 5}).json()
    assert [b["id"] for b in body["expired"]] == [w2["id"]]
    assert body["promoted"] == {}

    # 再推进到 10：h（ttl=10）与房间1的 k1（held ttl=10）同时过期；
    # 房间0 上 w1 [0,60) 完整放下，一次晋升
    body = c2.post("/clock/advance", json={"to": 10}).json()
    assert [b["id"] for b in body["expired"]] == [h["id"], k1["id"]]
    assert [p["id"] for p in body["promoted"]["0"]] == [w1["id"]]
    assert "1" not in body["promoted"]  # 房间1 无候补
    st = {b["id"]: b for b in c2.get("/state").json()["bookings"]}
    assert st[w1["id"]]["status"] == "held"
    assert st[w1["id"]]["expires_at"] == 15
    assert st[w2["id"]]["status"] == "expired"
    assert st[k1["id"]]["status"] == "expired"
    assert st[r["id"]]["status"] == "reserved"

    # 再次重启，晋升结果不变
    c3 = fresh()
    final = c3.get("/state").json()
    assert {b["id"]: b["status"] for b in final["bookings"]} == {
        h["id"]: "expired", w1["id"]: "held", w2["id"]: "expired",
        r["id"]: "reserved", k1["id"]: "expired",
    }
    assert final["now"] == 10
