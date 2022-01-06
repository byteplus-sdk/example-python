import logging
from concurrent.futures import ThreadPoolExecutor

from byteplus.core.option import Option
from byteplus.byteair import Client
from byteplus.byteair.protocol import CallbackRequest, ImportResponse, WriteResponse
from byteplus.common.protocol import  DoneResponse
from example.common.request_helper import RequestHelper
from example.common.status_helper import is_success, is_success_code

log = logging.getLogger(__name__)

_RETRY_TIMES = 2


class ConcurrentHelper(object):

    def __init__(self, client: Client):
        self._client = client
        self._request_helper = RequestHelper(client)
        self._executor = ThreadPoolExecutor(max_workers=5)

    def submit_write_request(self, data_list: list, topic: str, *opts: Option):
        self._executor.submit(self._do_write, data_list, topic, *opts)

    def _do_write(self, data_list: list, topic: str, *opts: Option):
        def call(call_data_list: list, *call_opts: Option) -> WriteResponse:
            return self._client.write_data(call_data_list, topic, *call_opts)

        try:
            rsp = self._request_helper.do_with_retry(call, data_list, opts, _RETRY_TIMES)
            if is_success(rsp.status):
                log.info("[AsyncWrite] success")
                return
            log.error("[AsyncWrite] fail, rsp:\n%s", rsp)
        except BaseException as e:
            log.error("[AsyncWrite] occur error, msg:%s", str(e))
        return

    def submit_done_request(self, date_list: list, topic: str, *opts: Option):
        self._executor.submit(self._do_done, date_list, topic, *opts)

    def _do_done(self, date_list: list, topic: str, *opts: Option):
        def call(call_date_list: list, *call_opts: Option) -> DoneResponse:
            return self._client.done(call_date_list, topic, *call_opts)

        try:
            rsp = self._request_helper.do_with_retry(call, date_list, opts, _RETRY_TIMES)
            if is_success(rsp.status):
                log.info("[AsyncDone] success")
                return
            log.error("[AsyncDone] fail, rsp:\n%s", rsp)
        except BaseException as e:
            log.error("[AsyncDone] occur error, msg:%s", str(e))
        return

    def submit_callback_request(self, request, *opts: tuple):
        self._executor.submit(self._do_callback, request, *opts)

    def _do_callback(self, request: CallbackRequest, *opts: Option):
        try:
            rsp = self._request_helper.do_with_retry(self._client.callback, request, opts, _RETRY_TIMES)
            if is_success_code(rsp.code):
                log.info("[AsyncCallback] success")
                return
            log.error("[AsyncCallback] fail, rsp:\n%s", rsp)
        except BaseException as e:
            log.error("[AsyncCallback] occur error, msg:%s", str(e))
        return

    def wait_and_shutdown(self):
        self._executor.shutdown(wait=True)
