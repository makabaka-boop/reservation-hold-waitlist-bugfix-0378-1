"""服务层：幂等键处理与事务边界。

幂等规则：
- 同一键重试：返回首次的状态码与响应体（原结果，含业务错误）；
- 同一键但方法/路径/查询/载荷指纹不同：409 拒绝（不执行、不记录）；
- 幂等记录与业务结果在同一写事务中提交。业务动作放在保存点内，
  失败时只回滚业务改动，错误响应仍作为该键的首次结果持久化。
"""

from __future__ import annotations

import hashlib
import json
from typing import Any, Callable

from .db import transaction
from .errors import IdempotencyConflict, ServiceError
from .store import Store

IdemHeader = "Idempotency-Key"


def fingerprint(method: str, path: str, query: str, body: Any) -> str:
    payload = {
        "method": method,
        "path": path,
        "query": query,
        "body": body if body is not None else {},
    }
    blob = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(blob).hexdigest()


def error_body(err: ServiceError) -> dict[str, Any]:
    return {"error": {"code": err.slug, "message": err.message}}


def mutate(
    store: Store,
    *,
    key: str | None,
    fp: str,
    action: Callable[[Store, int], tuple[int, Any]],
) -> tuple[int, Any]:
    """在 IMMEDIATE 事务中执行写操作，返回 (status_code, response_body)。"""
    with transaction(store.conn):
        if key is not None:
            existing = store.get_idempotency(key)
            if existing is not None:
                if existing["fingerprint"] != fp:
                    raise IdempotencyConflict(
                        "幂等键已被使用，但请求载荷与首次不同"
                    )
                return existing["status_code"], json.loads(existing["response"])

        now = store.now()
        try:
            store.conn.execute("SAVEPOINT action")
            status_code, body = action(store, now)
            store.conn.execute("RELEASE SAVEPOINT action")
        except ServiceError as err:
            store.conn.execute("ROLLBACK TO SAVEPOINT action")
            store.conn.execute("RELEASE SAVEPOINT action")
            status_code = err.status_code
            body = error_body(err)

        if key is not None:
            store.put_idempotency(
                key, fp, status_code, json.dumps(body, ensure_ascii=False), now
            )
        return status_code, body
