import datetime
import logging
import random
import time
import uuid
from typing import Optional

from google.protobuf.any_pb2 import Any
from google.protobuf.message import Message

from byteplus.core import BizException, NetException, Option
from byteplus.retail.protocol import GetOperationRequest, OperationResponse
from byteplus.retail import Client
from example.retail.status_helper import is_server_overload, is_upload_success, is_loss_operation

log = logging.getLogger(__name__)

# The maximum time for polling the execution results of the import task
_POLLING_TIMEOUT = datetime.timedelta(seconds=10)

# The time interval between requests during polling
_POLLING_INTERVAL = datetime.timedelta(milliseconds=100)

# The interval base of retry for server overload
_OVERLOAD_RETRY_INTERVAL = datetime.timedelta(milliseconds=200)

_GET_OPERATION_TIMEOUT = datetime.timedelta(milliseconds=600)


class RequestHelper(object):

    def __init__(self, client: Client):
        self._client: Client = client

    def do_import(self, call, request, response, opts, retry_times):
        # To ensure that the request is successfully received by the server,
        # it should be retried after network or overload exception occurs.
        op_rsp = self.do_with_retry_although_overload(call, request, opts, retry_times)
        if not is_upload_success(op_rsp.status):
            log.error("[PollingImportResponse] server return error info, rsp:\n{}", op_rsp)
            raise BizException(op_rsp.status.message)
        self._polling_response(op_rsp, response)
        return response

    # If the task is submitted too fast or the server is overloaded,
    # the server may refuse the request. In order to ensure the accuracy
    # of data transmission, you should wait some time and request again,
    # but it cannot retry endlessly. The maximum count of retries should be set.
    #
    # @param callable the task need to execute
    # @param request  the request type of task
    # @param opts     the options need by the task
    # @return the response of task
    # @throws BizException throw by task or still overload after retry
    def do_with_retry_although_overload(self, call, request, opts: tuple, retry_times: int):
        if retry_times < 0:
            retry_times = 0
        try_times: int = retry_times + 1
        for i in range(try_times):
            rsp = self.do_with_retry(call, request, opts, retry_times - i)
            if is_server_overload(rsp.status):
                # Wait some time before request again,
                # and the wait time will increase by the number of retried
                time.sleep(self._random_overload_wait_time(i).total_seconds())
                continue
            return rsp
        raise BizException("Server overload")

    def do_with_retry(self, call, request, opts: tuple, retry_times: int):
        # To ensure the request is successfully received by the server,
        # it should be retried after a network exception occurs.
        # To prevent the retry from causing duplicate uploading same data,
        # the request should be retried by using the same requestId.
        # If a new requestId is used, it will be treated as a new request
        # by the server, which may save duplicate data
        opts = self._with_request_id(opts)
        if retry_times < 0:
            retry_times = 0
        try_times = retry_times + 1
        for i in range(try_times):
            try:
                rsp = call(request, *opts)
            except NetException as e:
                if i == try_times - 1:
                    raise BizException(str(e))
                continue
            return rsp
        return

    @staticmethod
    def _with_request_id(opts: tuple) -> tuple:
        request_id_opt = Option.with_request_id(str(uuid.uuid1()))
        if opts is None:
            return request_id_opt,
        return (request_id_opt,) + opts

    @staticmethod
    def _random_overload_wait_time(retried_times: int) -> datetime.timedelta:
        increase_speed: int = 3
        if retried_times < 0:
            return _OVERLOAD_RETRY_INTERVAL
        rate: float = 1 + random.random() * (increase_speed ** retried_times)
        return _OVERLOAD_RETRY_INTERVAL * rate

    def _polling_response(self, op_rsp: OperationResponse, response: Message):
        rsp_any = self._do_polling_response(op_rsp.operation.name)
        try:
            response.ParseFromString(rsp_any.value)
        except BaseException as e:
            log.error("[PollingResponse] parse response fail, %s", e)
            raise BizException("parse import response fail")
        return response

    def _do_polling_response(self, name: str) -> Any:
        end_time = datetime.datetime.now() + _POLLING_TIMEOUT
        while datetime.datetime.now() < end_time:
            op_rsp = self._get_polling_operation(name)
            if op_rsp is None:
                continue
            if is_loss_operation(op_rsp.status):
                log.error("[PollingResponse] operation loss, rsp:\n%s", op_rsp)
                raise BizException("operation loss, please feedback to bytedance")
            op = op_rsp.operation
            if op.done:
                return op.response
            time.sleep(_POLLING_INTERVAL.total_seconds())
        log.error("[PollingResponse] timeout after %s", _POLLING_INTERVAL)
        raise BizException("polling import result timeout")

    def _get_polling_operation(self, name: str) -> Optional[OperationResponse]:
        request = GetOperationRequest()
        request.name = name
        timeout_opt = Option.with_timeout(_GET_OPERATION_TIMEOUT)
        try:
            return self._client.get_operation(request, timeout_opt)
        except NetException:
            # The NetException should not be thrown.
            # Throwing an exception means the request could not continue,
            # while polling for import results should be continue until the
            # maximum polling time is exceeded, as long as there is no obvious
            # error that should not continue, such as server telling operation lost,
            # parse response body fail, etc.
            return None
