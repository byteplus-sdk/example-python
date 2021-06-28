from byteplus.core.constant import STATUS_CODE_IDEMPOTENT, STATUS_CODE_SUCCESS, STATUS_CODE_TOO_MANY_REQUEST, \
    STATUS_CODE_OPERATION_LOSS
from byteplus.retail.protocol.byteplus_retail_pb2 import Status


def is_upload_success(status: Status) -> bool:
    code: int = status.code
    return code == STATUS_CODE_SUCCESS or code == STATUS_CODE_IDEMPOTENT


def is_success(status: Status) -> bool:
    code: int = status.code
    return code == STATUS_CODE_SUCCESS


def is_server_overload(status: Status) -> bool:
    return status.code == STATUS_CODE_TOO_MANY_REQUEST


def is_loss_operation(status: Status) -> bool:
    return status.code == STATUS_CODE_OPERATION_LOSS
