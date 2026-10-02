"""SQLite 连接与模式初始化。

设计要点：
- WAL + 同步 FULL，崩溃/断电后已提交状态不丢；
- busy_timeout 让写锁竞争时等待而不是立刻报 SQLITE_BUSY；
- 所有写事务使用 BEGIN IMMEDIATE，写操作在库级别串行化，
  从而保证"同一空档并发申请只有一个拿到保留"。
"""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from typing import Iterator

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS bookings (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    room       INTEGER NOT NULL,
    start_tick INTEGER NOT NULL,
    end_tick   INTEGER NOT NULL,
    status     TEXT NOT NULL,              -- held / reserved / waiting / expired / cancelled
    expires_at INTEGER,                   -- held 的过期时刻；waiting/reserved/终态为 NULL
    created_at INTEGER NOT NULL           -- 入队时刻（也是候补排序的次要依据）
);
-- 候补 FIFO：同房间按入队顺序，自增 id 单调，直接用主键排序。
CREATE INDEX IF NOT EXISTS idx_bookings_wait
    ON bookings(room, id) WHERE status = 'waiting';
CREATE INDEX IF NOT EXISTS idx_bookings_active
    ON bookings(room, start_tick, end_tick)
    WHERE status IN ('held', 'reserved');

CREATE TABLE IF NOT EXISTS idempotency (
    key         TEXT PRIMARY KEY,
    fingerprint TEXT NOT NULL,
    status_code INTEGER NOT NULL,
    response    TEXT NOT NULL,
    created_at  INTEGER NOT NULL
);
"""


def connect(db_path: str) -> sqlite3.Connection:
    # check_same_thread=False：FastAPI 的同步依赖与端点函数可能在不同的
    # 线程池线程中先后执行；每个请求独占连接、访问严格串行，故跨线程安全。
    conn = sqlite3.connect(
        db_path, timeout=30.0, isolation_level=None, check_same_thread=False
    )
    conn.row_factory = sqlite3.Row
    conn.execute(f"PRAGMA busy_timeout={30_000}")
    # foreign_keys / wal 是数据库级属性，每次连接重复设置也无害。
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=FULL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db(db_path: str, room_count: int) -> None:
    """创建模式并写入初始配置（时钟 0、房间数）。已有库则保留既有配置。"""
    if not 1 <= room_count <= 10:
        raise ValueError("room_count 必须在 1..10 之间")
    conn = connect(db_path)
    try:
        # executescript 会隐式提交，所以先在自动提交模式下建表，
        # 再单独开一个 IMMEDIATE 事务写初始配置。
        conn.executescript(SCHEMA)
        with transaction(conn):
            conn.execute(
                "INSERT OR IGNORE INTO meta(key, value) VALUES('now', '0')"
            )
            conn.execute(
                "INSERT OR IGNORE INTO meta(key, value) VALUES('room_count', ?)",
                (str(room_count),),
            )
    finally:
        conn.close()


@contextmanager
def transaction(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    """立即取写锁的事务，提交/回滚成对出现。"""
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield conn
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
