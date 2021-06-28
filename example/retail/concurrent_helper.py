import logging
from concurrent.futures import ThreadPoolExecutor

from byteplus.core.exception import BizException
from byteplus.core.option import Option
from byteplus.retail.protocol import *
from byteplus.retail import Client
from example.retail.request_helper import RequestHelper
from example.retail.status_helper import is_success

log = logging.getLogger(__name__)

_RETRY_TIMES = 2


class ConcurrentHelper(object):

    def __init__(self, client: Client):
        self._client = client
        self._request_helper = RequestHelper(client)
        self._executor = ThreadPoolExecutor(max_workers=5)

    def submit_request(self, request, *opts: Option):
        if isinstance(request, WriteUsersRequest):
            call = self._do_write_users
        elif isinstance(request, WriteProductsRequest):
            call = self._do_write_products
        elif isinstance(request, WriteUserEventsRequest):
            call = self._do_write_user_events
        elif isinstance(request, ImportUsersRequest):
            call = self._do_import_users
        elif isinstance(request, ImportProductsRequest):
            call = self._do_import_products
        elif isinstance(request, ImportUserEventsRequest):
            call = self._do_import_user_events
        elif isinstance(request, AckServerImpressionsRequest):
            call = self._do_ack
        else:
            raise BizException("can't support this request type:" + str(type(request)))
        self._executor.submit(call, request, opts)
        return

    def _do_write_users(self, request: WriteUsersRequest, opts: tuple):
        self._do_write(self._client.write_users, request, opts)
        return

    def _do_write_products(self, request: WriteProductsRequest, opts: tuple):
        self._do_write(self._client.write_products, request, opts)
        return

    def _do_write_user_events(self, request: WriteUserEventsRequest, opts: tuple):
        self._do_write(self._client.write_user_events, request, opts)
        return

    def _do_write(self, call, request, opts: tuple) -> None:
        try:
            rsp = self._request_helper.do_with_retry(call, request, opts, _RETRY_TIMES)
            if is_success(rsp.status):
                log.info("[AsyncWrite] success")
                return
            call_name = call.__name__
            log.error("[AsyncWrite] fail, call:%s rsp:\n{}", call_name, rsp)
        except BaseException as e:
            call_name = call.__name__
            log.error("[AsyncWrite] occur error, call:%s msg:%s", call_name, str(e))
        return

    def _do_import_users(self, request: ImportUsersRequest, opts: tuple):
        response: ImportUsersResponse = ImportUsersResponse()
        self._do_import(self._client.import_users, request, response, opts)
        return

    def _do_import_products(self, request: ImportProductsRequest, opts: tuple):
        response: ImportProductsResponse = ImportProductsResponse()
        self._do_import(self._client.import_products, request, response, opts)
        return

    def _do_import_user_events(self, request: ImportUserEventsRequest, opts: tuple):
        response: ImportUserEventsResponse = ImportUserEventsResponse()
        self._do_import(self._client.import_user_events, request, response, opts)
        return

    def _do_import(self, call, request, response, opts: tuple) -> None:
        try:
            self._request_helper.do_import(call, request, response, opts, _RETRY_TIMES)
            if is_success(response.status):
                log.info("[AsyncImport] success")
                return
            call_name = call.__name__
            log.error("[AsyncImport] fail, call:%s rsp:\n%s", call_name, response)
        except BaseException as e:
            call_name = call.__name__
            log.error("[AsyncImport] occur error, call:%s msg:%s", call_name, str(e))

    def _do_ack(self, request, opts: tuple):
        try:
            call = self._client.ack_server_impressions
            response = self._request_helper.do_with_retry(call, request, opts, _RETRY_TIMES)
            if is_success(response.status):
                log.info("[AsyncAckImpression] success")
                return
            log.error("[AsyncAckImpression] fail, rsp:\n%s", response)

        except BaseException as e:
            log.error("[AsyncAckImpression] occur error, msg:%s", e)
