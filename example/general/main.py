import json
import logging
import os
import time
import uuid
from datetime import datetime, timedelta
from signal import SIGKILL

from byteplus.core import Region, BizException, Option, NetException
from byteplus.general import Client, ClientBuilder
from byteplus.common.protocol import DoneResponse
from byteplus.general.protocol import ImportResponse, WriteResponse, PredictRequest, PredictUser, \
    CallbackRequest, CallbackItem, PredictResponse
from example.common.request_helper import RequestHelper
from example.common.status_helper import is_upload_success, is_success, is_success_code
from example.general.mock_hlper import mock_data_list

log = logging.getLogger(__name__)

# A unique token assigned by bytedance, which is used to
# generate an authenticated signature when building a request.
# It is sometimes called "secret".
TOKEN = "xxxxxxxxxxxxxxxxxxxxx"

# A unique ID assigned by Bytedance, which is used to
# generate an authenticated signature when building a request
# It is sometimes called "appkey".
TENANT_ID = "xxxxxxxxxxxx"

# A unique identity assigned by Bytedance, which is need to fill in URL.
# It is sometimes called "company".
TENANT = "general_demo"

# Required Param:
#       tenant
#       tenant_id
#       region
# Optional Param:
#       scheme
#       headers
client: Client = ClientBuilder() \
    .tenant(TENANT) \
    .tenant_id(TENANT_ID) \
    .token(TOKEN) \
    .region(Region.CN) \
    .build()

request_helper: RequestHelper = RequestHelper(client)

DEFAULT_RETRY_TIMES = 2

DEFAULT_WRITE_TIMEOUT = timedelta(milliseconds=800)

DEFAULT_IMPORT_TIMEOUT = timedelta(milliseconds=800)

DEFAULT_DONE_TIMEOUT = timedelta(milliseconds=800)

DEFAULT_PREDICT_TIMEOUT = timedelta(milliseconds=800)

DEFAULT_ACK_IMPRESSIONS_TIMEOUT = timedelta(milliseconds=800)

# default logLevel is Warning
logging.basicConfig(level=logging.NOTSET)


def main():
    # upload data
    write_data_example()

    # Mark some day's data has been entirely imported
    # Only used when uploading incremental day-level data
    done_example()

    # Get recommendation results
    recommend_example()

    # Do search request
    search_example()

    time.sleep(3)
    client.release()
    os.kill(os.getpid(), SIGKILL)


def write_data_example():
    # The count of items included in one "Write" request
    # is better to less than 10000 when upload data.
    data_list: list = mock_data_list(2)
    # The `topic` is some enums provided by bytedance,
    # who according to tenant's situation
    topic: str = "user"
    opts: tuple = _write_options()

    def call(call_data_list, *call_opts: Option) -> WriteResponse:
        return client.write_data(call_data_list, topic, *call_opts)

    try:
        response = request_helper.do_with_retry(call, data_list, opts, DEFAULT_RETRY_TIMES)
    except BizException as e:
        log.error("write occur err, msg:%s", e)
        return
    if is_upload_success(response.status):
        log.info("write success")
        return
    log.error("write find failure info, msg:%s errItems:%s", response.status, response.errors)
    return


def _write_options() -> tuple:
    return (
        # Required, uniquely identifies a request
        Option.with_request_id(str(uuid.uuid1())),
        # The date of uploaded data
        # Incremental data uploading: required.
        # Historical data and real-time data uploading: not required.
        Option.with_data_date(datetime(year=2021, month=11, day=1)),
        # Optional, the request timeout time, which can be adjusted
        # according to the actual situation, it is recommended to set a larger one
        Option.with_timeout(DEFAULT_IMPORT_TIMEOUT),
    )


def done_example():
    date: datetime = datetime(year=2021, month=6, day=10)
    date_list: list = [date]
    # The `topic` is some enums provided by bytedance,
    # who according to tenant's situation
    topic = "user"
    opts = _default_opts(DEFAULT_DONE_TIMEOUT)

    def call(call_date_list: list, *call_opts: Option) -> DoneResponse:
        return client.done(call_date_list, topic, *call_opts)

    try:
        response = request_helper.do_with_retry(call, date_list, opts, DEFAULT_RETRY_TIMES)
    except BizException as e:
        log.error("[Done] occur error, msg:%s", str(e))
        return
    if is_success(response.status):
        log.info("[Done] success")
        return
    log.error("[Done] find failure info, rsp:%s", response)
    return


def recommend_example():
    predict_request: PredictRequest = _build_predict_request()
    # The `scene` is provided by ByteDance, according to tenant's situation
    scene = "home"
    predict_opts = _default_opts(DEFAULT_PREDICT_TIMEOUT)
    try:
        predict_response = client.predict(predict_request, scene, *predict_opts)
    except (NetException, BizException) as e:
        log.error("predict occur error, msg:%s", e)
        return
    if not is_success_code(predict_response.code):
        log.error("predict find failure info, rsp:\n%s", predict_response)
        return
    log.info("predict success")

    # The items, which is eventually shown to user,
    # should send back to Bytedance for deduplication
    # callback_example(scene, predict_request, predict_response)


def _build_predict_request() -> PredictRequest:
    request = PredictRequest()

    request.size = 20

    user: PredictUser = request.user
    user.uid = "uid"

    context = request.context
    context.spm = "xx$$xxx$$xx"

    candidate_item = request.candidate_items.add()
    candidate_item.id = "item_id"

    related_item = request.related_item
    related_item.id = "item_id"

    extra = request.extra
    extra.extra["extra_key"] = "value"

    return request


# Report the recommendation request result (actual exposure data) through the callback interface
def callback_example(scene: str, predict_request: PredictRequest, predict_response: PredictResponse):
    callback_items = do_something_with_predict_result(predict_response.value)
    callback_request = CallbackRequest()
    # required, should be consistent with the uid passed in the recommendation request
    callback_request.scene = scene
    callback_request.predict_request_id = predict_response.request_id
    # required，should be consistent with `scene` used in the recommendation request
    callback_request.uid = predict_request.user.uid
    callback_request.items.extend(callback_items)
    callback_opts = _default_opts(DEFAULT_ACK_IMPRESSIONS_TIMEOUT)

    try:
        rsp = client.callback(callback_request, *callback_opts)
        if is_success_code(rsp.code):
            log.info("[Callback] success")
            return
        log.error("[Callback] fail, rsp:\n%s", rsp)
    except BaseException as e:
        log.error("[Callback] occur error, msg:%s", str(e))
    return


def do_something_with_predict_result(predict_result) -> list:
    # You can handle recommend results here,
    # such as filter, insert other items, sort again, etc.
    # The list of goods finally displayed to user and the filtered goods
    # should be sent back to bytedance for deduplication
    return conv_to_callback_items(predict_result.items)


def conv_to_callback_items(product_items: list) -> list:
    if product_items is None or len(product_items) == 0:
        return []
    size = len(product_items)
    callback_items = [None] * size
    for i in range(size):
        product_result = product_items[i]
        extra_map = {"reason": "kept"}
        callback_item = CallbackItem()
        callback_item.id = product_result.id
        callback_item.pos = str(i + 1)
        callback_item.extra = json.dumps(extra_map)
        callback_items[i] = callback_item
    return callback_items


def search_example():
    search_request = build_search_request()
    opts = _default_opts(DEFAULT_PREDICT_TIMEOUT)
    # The `scene` is provided by ByteDance,
    # that usually is "search" in search request
    scene = "search"
    try:
        predict_response = client.predict(search_request, scene, *opts)
    except BaseException as e:
        log.error("search occur error, msg:%s", str(e))
        return
    if not is_success_code(predict_response.code):
        log.error("search find failure info, msg:%s", predict_response)
        return
    log.info("search success")


def build_search_request():
    request = PredictRequest()
    request.size = 20
    condition = request.search_condition
    condition.search_type = 0
    condition.query = "key_word"
    extra = request.extra
    extra.extra["extra_key"] = "value"
    return request


def _default_opts(timeout: timedelta) -> tuple:
    return (
        Option.with_timeout(timeout),
        Option.with_request_id(str(uuid.uuid1())),
    )


if __name__ == '__main__':
    main()
