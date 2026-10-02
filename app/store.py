"""核心数据操作：时钟、申请/保留/确认、候补晋升、过期、取消、缩短。

约定：
- 区间为半开整数区间 [start_tick, end_tick)，相邻区间（a.end == b.start）不冲突；
- 冲突只在同一房间内判定（不同房间的相同时间段互不影响）；
- 候补按自增 id（即入队顺序）FIFO；
- 本模块的每个公开操作都在调用方开启的 IMMEDIATE 事务内完成
  "释放容量 + 扫描候补 + 晋升"，保证原子性。
"""

from __future__ import annotations

import sqlite3
from typing import Any

from .errors import ConflictError, NotFoundError, ValidationError

ACTIVE = ("held", "reserved")
TERMINAL = ("expired", "cancelled")


def _booking_dict(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "id": row["id"],
        "room": row["room"],
        "start_tick": row["start_tick"],
        "end_tick": row["end_tick"],
        "status": row["status"],
        "expires_at": row["expires_at"],
        "created_at": row["created_at"],
    }


class Store:
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    # ---- 时钟与配置 ----

    def now(self) -> int:
        row = self.conn.execute("SELECT value FROM meta WHERE key='now'").fetchone()
        return int(row["value"])

    def set_now(self, value: int) -> None:
        self.conn.execute("UPDATE meta SET value=? WHERE key='now'", (str(value),))

    def room_count(self) -> int:
        row = self.conn.execute(
            "SELECT value FROM meta WHERE key='room_count'"
        ).fetchone()
        return int(row["value"])

    # ---- 幂等记录 ----

    def get_idempotency(self, key: str) -> sqlite3.Row | None:
        return self.conn.execute(
            "SELECT * FROM idempotency WHERE key=?", (key,)
        ).fetchone()

    def put_idempotency(
        self, key: str, fingerprint: str, status_code: int, response_json: str, now: int
    ) -> None:
        self.conn.execute(
            "INSERT INTO idempotency(key, fingerprint, status_code, response, created_at)"
            " VALUES(?,?,?,?,?)",
            (key, fingerprint, status_code, response_json, now),
        )

    # ---- 查询 ----

    def get_booking_row(self, booking_id: int) -> sqlite3.Row:
        row = self.conn.execute(
            "SELECT * FROM bookings WHERE id=?", (booking_id,)
        ).fetchone()
        if row is None:
            raise NotFoundError(f"预约 {booking_id} 不存在")
        return row

    def get_booking(self, booking_id: int) -> dict[str, Any]:
        return _booking_dict(self.get_booking_row(booking_id))

    def list_bookings(self) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            "SELECT * FROM bookings ORDER BY id ASC"
        ).fetchall()
        return [_booking_dict(r) for r in rows]

    # ---- 核心操作 ----

    def _has_overlap(self, room: int, start: int, end: int) -> bool:
        """同房间内是否与任一 held/reserved 区间重叠（半开区间，相邻不算）。"""
        row = self.conn.execute(
            """
            SELECT 1 FROM bookings
            WHERE room = ?
              AND status IN ('held', 'reserved')
              AND start_tick < ? AND end_tick > ?
            LIMIT 1
            """,
            (room, end, start),
        ).fetchone()
        return row is not None

    def create_booking(
        self, room: int, start: int, end: int, ttl: int, now: int
    ) -> dict[str, Any]:
        """申请：有空档则取得限时保留(held)，否则进入候补(waiting)。"""
        if not 0 <= room < self.room_count():
            raise ValidationError(f"房间号必须在 0..{self.room_count() - 1} 之间")
        if ttl <= 0:
            raise ValidationError("ttl 必须为正整数")

        overlap = self._has_overlap(room, start, end)
        status = "waiting" if overlap else "held"
        expires_at = None if overlap else now + ttl
        cur = self.conn.execute(
            "INSERT INTO bookings(room, start_tick, end_tick, status, expires_at, created_at)"
            " VALUES(?,?,?,?,?,?) RETURNING *",
            (room, start, end, status, expires_at, now),
        )
        return _booking_dict(cur.fetchone())

    def _scan_waitlist(self, room: int, ttl: int, now: int) -> list[dict[str, Any]]:
        """按入队顺序扫描某房间的候补，晋升当前能**完整**容纳的申请。

        不拆分时段：放不下就保留其 waiting 状态并继续检查后面的申请。
        """
        promoted: list[dict[str, Any]] = []
        waiting = self.conn.execute(
            "SELECT * FROM bookings WHERE room=? AND status='waiting' ORDER BY id ASC",
            (room,),
        ).fetchall()
        for row in waiting:
            if self._has_overlap(room, row["start_tick"], row["end_tick"]):
                continue
            expires_at = now + ttl
            self.conn.execute(
                "UPDATE bookings SET status='held', expires_at=? WHERE id=?",
                (expires_at, row["id"]),
            )
            updated = self.conn.execute(
                "SELECT * FROM bookings WHERE id=?", (row["id"],)
            ).fetchone()
            promoted.append(_booking_dict(updated))
        return promoted

    def confirm_booking(self, booking_id: int) -> dict[str, Any]:
        """确认保留 -> 预约。只接受 held。"""
        row = self.get_booking_row(booking_id)
        if row["status"] == "reserved":
            raise ConflictError("该保留已确认，无需重复确认")
        if row["status"] != "held":
            raise ConflictError(f"当前状态 {row['status']} 不可确认，仅 held 可确认")
        self.conn.execute(
            "UPDATE bookings SET status='reserved', expires_at=NULL WHERE id=?",
            (booking_id,),
        )
        return self.get_booking(booking_id)

    def cancel_booking(self, ttl: int, now: int, booking_id: int) -> dict[str, Any]:
        """取消。释放 held/reserved 的容量后，在同一事务内扫描候补。"""
        row = self.get_booking_row(booking_id)
        if row["status"] in TERMINAL:
            raise ConflictError(f"预约已处于终态 {row['status']}，不可取消")

        self.conn.execute(
            "UPDATE bookings SET status='cancelled', expires_at=NULL WHERE id=?",
            (booking_id,),
        )
        promoted: dict[int, list[dict[str, Any]]] = {}
        # waiting 取消不释放容量；held/reserved 取消才可能腾出空档。
        if row["status"] in ACTIVE:
            room_promoted = self._scan_waitlist(row["room"], ttl, now)
            if room_promoted:
                promoted[row["room"]] = room_promoted
        return {"booking": self.get_booking(booking_id), "promoted": promoted}

    def shorten_booking(
        self,
        booking_id: int,
        new_start: int,
        new_end: int,
        ttl: int,
        now: int,
    ) -> dict[str, Any]:
        """缩短（收紧）held/reserved 的时段，再在同一事务内扫描候补。"""
        row = self.get_booking_row(booking_id)
        if row["status"] not in ACTIVE:
            raise ConflictError(
                f"当前状态 {row['status']} 不可缩短，仅 held/reserved 可缩短"
            )
        if new_start >= new_end:
            raise ValidationError("必须满足 start_tick < end_tick")
        if new_start < row["start_tick"] or new_end > row["end_tick"]:
            raise ValidationError("只允许在原时段范围内缩短，不得扩张或平移出界")

        self.conn.execute(
            "UPDATE bookings SET start_tick=?, end_tick=? WHERE id=?",
            (new_start, new_end, booking_id),
        )
        room_promoted = self._scan_waitlist(row["room"], ttl, now)
        promoted = {row["room"]: room_promoted} if room_promoted else {}
        return {"booking": self.get_booking(booking_id), "promoted": promoted}

    def expire_holds(
        self, ttl: int, now: int
    ) -> tuple[list[dict[str, Any]], dict[int, list[dict[str, Any]]]]:
        """过期所有 expires_at <= now 的 held，再逐房间扫描候补。

        与时钟推进在同一事务中完成。
        """
        expired_ids = [
            r["id"]
            for r in self.conn.execute(
                "SELECT id FROM bookings WHERE status='held' AND expires_at <= ?"
                " ORDER BY id ASC",
                (now,),
            ).fetchall()
        ]
        if expired_ids:
            self.conn.execute(
                "UPDATE bookings SET status='expired', expires_at=NULL"
                " WHERE status='held' AND expires_at <= ?",
                (now,),
            )
        # 更新之后再读取，返回内容与最终状态一致。
        placeholders = ",".join("?" * len(expired_ids))
        expired_rows = (
            self.conn.execute(
                f"SELECT * FROM bookings WHERE id IN ({placeholders}) ORDER BY id ASC",
                expired_ids,
            ).fetchall()
            if expired_ids
            else []
        )
        expired = [_booking_dict(r) for r in expired_rows]

        affected_rooms = sorted({r["room"] for r in expired_rows})
        promoted: dict[int, list[dict[str, Any]]] = {}
        for room in affected_rooms:
            room_promoted = self._scan_waitlist(room, ttl, now)
            if room_promoted:
                promoted[room] = room_promoted
        return expired, promoted
