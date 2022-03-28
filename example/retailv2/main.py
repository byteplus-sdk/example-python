import logging
import os
import time
import uuid
from datetime import datetime, timezone, timedelta
from signal import SIGKILL

from google.protobuf.message import Message

from byteplus.core import Region, BizException, Option, NetException
from byteplus.common.protocol import DoneResponse
from byteplus.retailv2 import Client, ClientBuilder
from byteplus.retailv2.protocol import WriteUsersRequest, WriteProductsRequest, WriteUserEventsRequest,\
    PredictRequest, AckServerImpressionsRequest

from example.retailv2.concurrent_helper import ConcurrentHelper
from example.retailv2.mock_helper import mock_users, mock_products, mock_user_events, mock_product, mock_device
from example.common.request_helper import RequestHelper
from example.common.status_helper import is_upload_success, is_success

log = logging.getLogger(__name__)

# A unique token assigned by bytedance, which is used to
# generate an authenticated signature when building a request.
# It is sometimes called "secret".
TOKEN = "xxxxxxxxxxxxxxxxx"

# A unique ID assigned by Bytedance, which is used to
# generate an authenticated signature when building a request
# It is sometimes called "appkey".
TENANT_ID = "xxxxxxxxxxxx"

# A unique identity assigned by Bytedance, which is need to fill in URL.
# It is sometimes called "company".
TENANT = "retail_demo"

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
    .region(Region.SG) \
    .build()

request_helper: RequestHelper = RequestHelper(client)

concurrent_helper: ConcurrentHelper = ConcurrentHelper(client)

DEFAULT_RETRY_TIMES = 2

DEFAULT_WRITE_TIMEOUT = timedelta(milliseconds=800)

DEFAULT_DONE_TIMEOUT = timedelta(milliseconds=800)

DEFAULT_PREDICT_TIMEOUT = timedelta(milliseconds=800)

DEFAULT_ACK_IMPRESSIONS_TIMEOUT = timedelta(milliseconds=800)

# default logLevel is Warning
logging.basicConfig(level=logging.NOTSET)


def main():
    # Write real-time user data
    write_users_example()
    # Write real-time user data concurrently
    concurrent_write_users_example()

    # Write real-time product data
    write_products_example()
    # Write real-time product data concurrently
    concurrent_write_products_example()

    # Write real-time user event data
    write_user_events_example()
    # Write real-time user event data concurrently
    concurrent_write_user_events_example()

    # Pass a date list to mark the completion of data synchronization for these days.
    done_example()

    # Get recommendation results
    recommend_example()

    time.sleep(3)
    client.release()
    os.kill(os.getpid(), SIGKILL)


def write_users_example():
    # The "WriteXXX" api can transfer max to 2000 items at one request
    request = _build_write_user_request(1)
    opts = _default_opts(DEFAULT_WRITE_TIMEOUT)
    try:
        response = request_helper.do_with_retry(client.write_users, request, opts, DEFAULT_RETRY_TIMES)
    except BizException as e:
        log.error("write user occur err, msg:%s", e)
        return
    if is_upload_success(response.status):
        log.info("write user success")
        return
    log.error("write user find fail, msg:%s errItems:%s", response.status, response.errors)
    return


def concurrent_write_users_example():
    # The "WriteXXX" api can transfer max to 2000 items at one request
    request = _build_write_user_request(1)
    opts = _default_opts(DEFAULT_WRITE_TIMEOUT)
    concurrent_helper.submit_request(request, *opts)
    return


def _build_write_user_request(count: int) -> WriteUsersRequest:
    request = WriteUsersRequest()
    request.users.extend(mock_users(count))
    # Optional
    # request.extra["extra_info"] = "value"
    return request


def write_products_example():
    # The "WriteXXX" api can transfer max to 2000 items at one request
    request = _build_write_product_request(1)
    opts = _default_opts(DEFAULT_WRITE_TIMEOUT)
    try:
        response = request_helper.do_with_retry(client.write_products, request, opts, DEFAULT_RETRY_TIMES)
    except BizException as e:
        log.error("write product occur err, msg:%s", e)
        return
    if is_upload_success(response.status):
        log.info("write product success")
        return
    log.error("write product find fail, msg:%s errItems:%s", response.status, response.errors)
    return


def concurrent_write_products_example():
    # The "WriteXXX" api can transfer max to 2000 items at one request
    request = _build_write_product_request(1)
    opts = _default_opts(DEFAULT_WRITE_TIMEOUT)
    concurrent_helper.submit_request(request, *opts)
    return


def _build_write_product_request(count: int) -> WriteProductsRequest:
    request = WriteProductsRequest()
    request.products.extend(mock_products(count))
    # Optional
    # request.extra["extra_info"] = "value"
    return request


def write_user_events_example():
    # The "WriteXXX" api can transfer max to 2000 items at one request
    request = _build_write_user_event_request(1)
    opts = _default_opts(DEFAULT_WRITE_TIMEOUT)
    try:
        response = request_helper.do_with_retry(client.write_user_events, request, opts, DEFAULT_RETRY_TIMES)
    except BizException as e:
        log.error("write user_event occur err, msg:%s", e)
        return
    if is_upload_success(response.status):
        log.info("write user_event success")
        return
    log.error("write user event find failure info, msg:%s errItems:%s", response.status, response.errors)
    return


def concurrent_write_user_events_example():
    # The "WriteXXX" api can transfer max to 2000 items at one request
    request = _build_write_user_event_request(1)
    opts = _default_opts(DEFAULT_WRITE_TIMEOUT)
    concurrent_helper.submit_request(request, *opts)
    return


def _build_write_user_event_request(count: int) -> WriteUserEventsRequest:
    user_events = mock_user_events(count)
    request = WriteUserEventsRequest()
    request.user_events.extend(user_events)
    # Optional
    # request.extra["extra_info"] = "value"
    return request


def done_example():
    date: datetime = datetime(year=2021, month=12, day=12)
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
    predict_request = _build_predict_request()
    predict_opts = _default_opts(DEFAULT_PREDICT_TIMEOUT)
    try:
        # The "home" is scene name, which provided by ByteDance, usually is "home"
        predict_response = client.predict(predict_request, "home", *predict_opts)
    except (NetException, BizException) as e:
        log.error("predict occur error, msg:%s", e)
        return
    if not is_success(predict_response.status):
        log.error("predict find failure info, rsp:\n%s", predict_response)
        return
    log.info("predict success")
    # The items, which is eventually shown to user,
    # should send back to Bytedance for deduplication
    altered_products = do_something_with_predict_result(predict_response.value)
    ack_request = _build_ack_impressions_request(predict_response.request_id, predict_request, altered_products)
    ack_opts = _default_opts(DEFAULT_ACK_IMPRESSIONS_TIMEOUT)
    concurrent_helper.submit_request(ack_request, *ack_opts)


def _build_predict_request() -> PredictRequest:
    request = PredictRequest()
    request.user_id = "user_id"
    request.size = 20
    request.extra["clear_impression"] = "true"

    scene = request.scene
    scene.scene_name = "home"

    ctx = request.context
    ctx.candidate_product_ids[:] = ["pid1", "pid2"]
    ctx.root_product.CopyFrom(mock_product())
    ctx.device.CopyFrom(mock_device())
    return request


def do_something_with_predict_result(predict_result):
    # You can handle recommend results here,
    # such as filter, insert other items, sort again, etc.
    # The list of goods finally displayed to user and the filtered goods
    # should be sent back to bytedance for deduplication
    return conv_to_altered_products(predict_result.response_products)


def conv_to_altered_products(product_results):
    if product_results is None or len(product_results) == 0:
        return
    size = len(product_results)
    altered_products = [None] * size
    for i in range(size):
        product_result = product_results[i]
        altered_product = AckServerImpressionsRequest.AlteredProduct()
        altered_product.altered_reason = "kept"
        altered_product.product_id = product_result.product_id
        altered_product.rank = product_result.rank
        altered_products[i] = altered_product
    return altered_products


def _build_ack_impressions_request(predict_request_id: str, predict_request, altered_products: list):
    request = AckServerImpressionsRequest()
    request.predict_request_id = predict_request_id
    request.user_id = predict_request.user_id
    scene: Message = request.scene
    scene.CopyFrom(predict_request.scene)
    request.altered_products.extend(altered_products)
    return request


def _default_opts(timeout: timedelta) -> tuple:
    # customer_headers = {}
    return (
        Option.with_timeout(timeout),
        Option.with_request_id(str(uuid.uuid1())),
        # Option.with_headers(customer_headers),
    )


if __name__ == '__main__':
    main()
