"""FastAPI 入口：房间整数时段预约服务。

接口：
  POST /bookings                 申请（held 或 waiting）
  POST /bookings/{id}/confirm    确认 held -> reserved
  POST /bookings/{id}/cancel     取消（同事务释放容量并晋升候补）
  POST /bookings/{id}/shorten    缩短 held/reserved（同事务晋升候补）
  POST /clock/advance            推进可控时钟（同事务过期保留并晋升候补）
  GET  /bookings/{id}            查询单个
  GET  /state                    全量状态（时钟、房间数、全部申请）
  GET  /healthz                  存活检查

所有写接口都接受可选请求头 Idempotency-Key。
"""

from __future__ import annotations

import os
from typing import Any

from fastapi import Depends, FastAPI, Header, Request, Response
from pydantic import BaseModel, Field, model_validator

from .db import connect, init_db
from .errors import ServiceError
from .service import IdemHeader, error_body, fingerprint, mutate
from .store import Store

# 候补晋升后得到的保留时长（tick 数）。可由环境变量覆盖。
DEFAULT_PROMOTION_TTL = 5


class CreateBody(BaseModel):
    room: int = Field(ge=0)
    start_tick: int
    end_tick: int
    ttl: int = Field(gt=0, description="保留时长（tick）")

    @model_validator(mode="after")
    def check_interval(self) -> "CreateBody":
        if self.start_tick >= self.end_tick:
            raise ValueError("必须满足 start_tick < end_tick")
        return self


class ShortenBody(BaseModel):
    start_tick: int
    end_tick: int

    @model_validator(mode="after")
    def check_interval(self) -> "ShortenBody":
        if self.start_tick >= self.end_tick:
            raise ValueError("必须满足 start_tick < end_tick")
        return self


class AdvanceBody(BaseModel):
    ticks: int | None = Field(default=None, ge=0)
    to: int | None = Field(default=None, ge=0)

    @model_validator(mode="after")
    def check_exclusive(self) -> "AdvanceBody":
        if self.ticks is None and self.to is None:
            raise ValueError("必须提供 ticks 或 to 之一")
        return self


def create_app(
    db_path: str,
    room_count: int = 5,
    promotion_ttl: int | None = None,
) -> FastAPI:
    init_db(db_path, room_count)
    if promotion_ttl is None:
        promotion_ttl = int(os.environ.get("PROMOTION_TTL", DEFAULT_PROMOTION_TTL))

    app = FastAPI(title="房间时段预约服务")
    app.state.db_path = db_path
    app.state.promotion_ttl = promotion_ttl

    def get_store() -> Store:
        conn = connect(app.state.db_path)
        try:
            yield Store(conn)
        finally:
            conn.close()

    async def _mutate(
        request: Request,
        store: Store,
        idem_key: str | None,
        body: Any,
        action,
    ) -> tuple[int, Any]:
        body_json = body if isinstance(body, dict) else (
            body.model_dump() if body is not None else {}
        )
        fp = fingerprint(
            request.method,
            request.url.path,
            str(request.url.query),
            body_json,
        )
        return mutate(store, key=idem_key, fp=fp, action=action)

    @app.exception_handler(ServiceError)
    async def service_error_handler(request: Request, exc: ServiceError):
        from fastapi.responses import JSONResponse

        return JSONResponse(
            status_code=exc.status_code, content=error_body(exc)
        )

    @app.get("/healthz")
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.post("/bookings")
    async def create_booking(
        request: Request,
        body: CreateBody,
        response: Response,
        store: Store = Depends(get_store),
        idem_key: str | None = Header(default=None, alias=IdemHeader),
    ):
        def action(s: Store, now: int) -> tuple[int, Any]:
            booking = s.create_booking(
                room=body.room,
                start=body.start_tick,
                end=body.end_tick,
                ttl=body.ttl,
                now=now,
            )
            return 201, booking

        status, payload = await _mutate(request, store, idem_key, body, action)
        response.status_code = status
        return payload

    @app.post("/bookings/{booking_id}/confirm")
    async def confirm_booking(
        booking_id: int,
        request: Request,
        response: Response,
        store: Store = Depends(get_store),
        idem_key: str | None = Header(default=None, alias=IdemHeader),
    ):
        def action(s: Store, now: int) -> tuple[int, Any]:
            return 200, s.confirm_booking(booking_id, now)

        status, payload = await _mutate(request, store, idem_key, None, action)
        response.status_code = status
        return payload

    @app.post("/bookings/{booking_id}/cancel")
    async def cancel_booking(
        booking_id: int,
        request: Request,
        response: Response,
        store: Store = Depends(get_store),
        idem_key: str | None = Header(default=None, alias=IdemHeader),
    ):
        def action(s: Store, now: int) -> tuple[int, Any]:
            return 200, s.cancel_booking(
                app.state.promotion_ttl, now, booking_id
            )

        status, payload = await _mutate(request, store, idem_key, None, action)
        response.status_code = status
        return payload

    @app.post("/bookings/{booking_id}/shorten")
    async def shorten_booking(
        booking_id: int,
        request: Request,
        body: ShortenBody,
        response: Response,
        store: Store = Depends(get_store),
        idem_key: str | None = Header(default=None, alias=IdemHeader),
    ):
        def action(s: Store, now: int) -> tuple[int, Any]:
            return 200, s.shorten_booking(
                booking_id,
                body.start_tick,
                body.end_tick,
                app.state.promotion_ttl,
                now,
            )

        status, payload = await _mutate(request, store, idem_key, body, action)
        response.status_code = status
        return payload

    @app.post("/clock/advance")
    async def advance_clock(
        request: Request,
        body: AdvanceBody,
        response: Response,
        store: Store = Depends(get_store),
        idem_key: str | None = Header(default=None, alias=IdemHeader),
    ):
        def action(s: Store, now: int) -> tuple[int, Any]:
            from .errors import ValidationError

            target = now + body.ticks if body.ticks is not None else body.to
            assert target is not None
            if target < now:
                raise ValidationError("时钟只能向前推进，不可回退")
            if target == now:
                return 200, {"now": now, "expired": [], "promoted": {}}
            s.set_now(target)
            expired, promoted = s.expire_holds(app.state.promotion_ttl, target)
            return 200, {"now": target, "expired": expired, "promoted": promoted}

        status, payload = await _mutate(request, store, idem_key, body, action)
        response.status_code = status
        return payload

    @app.get("/bookings/{booking_id}")
    async def get_booking(booking_id: int, store: Store = Depends(get_store)):
        return store.get_booking(booking_id)

    @app.get("/state")
    async def get_state(store: Store = Depends(get_store)) -> dict[str, Any]:
        return {
            "now": store.now(),
            "room_count": store.room_count(),
            "bookings": store.list_bookings(),
        }

    return app


app = create_app(
    db_path=os.environ.get("DB_PATH", "booking.db"),
    room_count=int(os.environ.get("ROOM_COUNT", "5")),
)
