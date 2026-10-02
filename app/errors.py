"""业务错误类型。HTTP 层据此映射状态码。"""


class ServiceError(Exception):
    status_code = 400
    slug = "bad_request"

    def __init__(self, message: str):
        super().__init__(message)
        self.message = message


class ValidationError(ServiceError):
    status_code = 400
    slug = "validation_error"


class NotFoundError(ServiceError):
    status_code = 404
    slug = "not_found"


class ConflictError(ServiceError):
    status_code = 409
    slug = "conflict"


class IdempotencyConflict(ConflictError):
    slug = "idempotency_conflict"
