import logging
from concurrent.futures import ThreadPoolExecutor

from byteplus.core.exception import BizException
from byteplus.core.option import Option
from byteplus.rutenad.protocol import *
from byteplus.rutenad import Client
from example.common.request_helper import RequestHelper
from example.common.status_helper import is_success

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
        elif isinstance(request, WriteAdvertisementsRequest):
            call = self._do_write_advertisements
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

    def _do_write_advertisements(self, request: WriteAdvertisementsRequest, opts: tuple):
        self._do_write(self._client.write_advertisements, request, opts)
        return

    def _do_write(self, call, request, opts: tuple) -> None:
        try:
            rsp = self._request_helper.do_with_retry(call, request, opts, _RETRY_TIMES)
            if is_success(rsp.status):
                log.info("[AsyncWrite] success")
                return
            call_name = call.__name__
            log.error("[AsyncWrite] fail, call:%s rsp:\n%s", call_name, rsp)
        except BaseException as e:
            call_name = call.__name__
            log.error("[AsyncWrite] occur error, call:%s msg:%s", call_name, str(e))
        return


