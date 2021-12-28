import json
import logging
import os
import time
import uuid
from datetime import datetime, timedelta
from signal import SIGKILL

from google.protobuf.message import Message

from byteplus.core import Region, BizException, Option, NetException
from byteplus.general import Client, ClientBuilder
from byteplus.common.protocol import DoneResponse
from byteplus.general.protocol import ImportResponse, WriteResponse, PredictRequest, PredictUser,\
    CallbackRequest, CallbackItem
from example.common.example import get_operation_example as do_get_operation
from example.common.example import list_operations_example as do_list_operations
from example.common.request_helper import RequestHelper
from example.common.status_helper import is_upload_success, is_success, is_success_code
from example.general.concurrent_helper import ConcurrentHelper
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

concurrent_helper: ConcurrentHelper = ConcurrentHelper(client)

DEFAULT_RETRY_TIMES = 2

DEFAULT_WRITE_TIMEOUT = timedelta(milliseconds=800)

DEFAULT_IMPORT_TIMEOUT = timedelta(milliseconds=800)

DEFAULT_DONE_TIMEOUT = timedelta(milliseconds=800)

DEFAULT_PREDICT_TIMEOUT = timedelta(milliseconds=800)

DEFAULT_ACK_IMPRESSIONS_TIMEOUT = timedelta(milliseconds=800)

# default logLevel is Warning
logging.basicConfig(level=logging.NOTSET)


def main():
    # Write real-time data
    write_data_example()
    # Write real-time data concurrently
    concurrent_write_data_example()
    # Import daily offline data
    import_data_example()
    # Import daily offline data concurrently
    concurrent_import_data_example()

    # Mark data in some days has been entirely imported
    done_example()
    # Do 'done' request concurrently
    concurrent_done_example()

    # Obtain Operation information according to operationName,
    # if the corresponding task is executing, the real-time
    # result of task execution will be returned
    get_operation_example()

    # Lists operations that match the specified filter in the request.
    # It can be used to retrieve the task when losing 'operation.name',
    # or to statistic the execution of the task within the specified range,
    # for example, the total count of successfully imported data.
    # The result of "listOperations" is not real-time.
    # The real-time info should be obtained through "getOperation"
    list_operations_example()

    # Get recommendation results
    recommend_example()

    # Do search request
    search_example()

    time.sleep(3)
    client.release()
    os.kill(os.getpid(), SIGKILL)


def write_data_example():
    # The count of items included in one "Write" request
    # is better to less than 100 when upload real-time data.
    data_list: list = mock_data_list(2)
    # The `topic` is some enums provided by bytedance,
    # who according to tenant's situation
    topic: str = "user_event"
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


def concurrent_write_data_example():
    # The count of items included in one "Write" request
    # is better to less than 100 when upload real-time data.
    data_list: list = mock_data_list(2)
    # The `topic` is some enums provided by bytedance,
    # who according to tenant's situation
    topic: str = "user_event"
    opts: tuple = _write_options()
    concurrent_helper.submit_write_request(data_list, topic, *opts)
    return


def _write_options() -> tuple:
    # All options are optional
    # customer_headers = {}
    return (
        Option.with_timeout(DEFAULT_WRITE_TIMEOUT),
        Option.with_request_id(str(uuid.uuid1())),
        # Option.with_headers(customer_headers),
        # The server is expected to return within a certain period，
        # to prevent can't return before client is timeout
        Option.with_server_timeout(DEFAULT_WRITE_TIMEOUT - timedelta(milliseconds=50))
    )


def import_data_example():
    # The "ImportXXX" api can transfer max to 10k items at one request
    data_list: list = mock_data_list(2)
    # The `topic` is some enums provided by bytedance,
    # who according to tenant's situation
    topic: str = "user_event"
    opts: tuple = _import_options()
    response: ImportResponse = ImportResponse()

    def call(call_data_list, *call_opts: Option) -> ImportResponse:
        return client.import_data(call_data_list, topic, *call_opts)

    try:
        request_helper.do_import(call, data_list, response, opts, DEFAULT_RETRY_TIMES)
    except BizException as e:
        log.error("import occur err, msg:%s", e)
        return
    if is_upload_success(response.status):
        log.info("import success")
        return
    log.error("import find failure info, msg:%s errSamples:%s", response.status, response.error_samples)
    return


def concurrent_import_data_example():
    # The count of items included in one "Write" request
    # is better to less than 100 when upload real-time data.
    data_list: list = mock_data_list(2)
    # The `topic` is some enums provided by bytedance,
    # who according to tenant's situation
    topic: str = "user_event"
    opts: tuple = _import_options()
    concurrent_helper.submit_import_request(data_list, topic, *opts)
    return


def _import_options() -> tuple:
    # All options are optional
    # customer_headers = {}
    return (
        Option.with_timeout(DEFAULT_IMPORT_TIMEOUT),
        Option.with_request_id(str(uuid.uuid1())),
        # Option.with_headers(customer_headers),
        # Required for import request
        # The date in produced of data in this 'import' request
        Option.with_data_date(datetime.now())
        # If data in a whole day has been imported completely,
        # the import request need be with this option
        # Option.withDataEnd(true)
    )


def done_example():
    date: datetime = datetime(year=2021, month=6, day=10)
    date_list: list = [date]
    # The `topic` is some enums provided by bytedance,
    # who according to tenant's situation
    topic = "user_event"
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


def concurrent_done_example():
    date: datetime = datetime(year=2021, month=6, day=10)
    date_list: list = [date]
    # The `topic` is some enums provided by bytedance,
    # who according to tenant's situation
    topic = "user_event"
    opts = _default_opts(DEFAULT_DONE_TIMEOUT)
    concurrent_helper.submit_done_request(date_list, topic, *opts)
    return


def get_operation_example():
    name = "0c5a1145-2c12-4b83-8998-2ae8153ca089"
    do_get_operation(client, name)
    return


def list_operations_example():
    filter_query = "date>=2021-06-15 and done=true"
    operations = do_list_operations(client, filter_query)
    _parse_task_response(operations)
    return


def _parse_task_response(operations: list):
    if operations is None or len(operations) == 0:
        return
    for operation in operations:
        if not operation.done:
            continue
        response_any = operation.response
        type_url = response_any.type_url
        try:
            if "ImportResponse" in type_url:
                response: Message = ImportResponse()
                response.ParseFromString(response_any.value)
                log.info("[ListOperations] Import rsp:\n%s", response)
            else:
                log.error("[ListOperations] unexpected task response type:%s", type_url)
        except BaseException as e:
            log.error("[ListOperations] parse task response fail, msg:%s", e)
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
    callback_items = do_something_with_predict_result(predict_response.value)
    callback_request = CallbackRequest()
    callback_request.predict_request_id = predict_response.request_id
    callback_request.uid = predict_request.user.uid
    callback_request.scene = scene
    callback_request.items.extend(callback_items)
    callback_opts = _default_opts(DEFAULT_ACK_IMPRESSIONS_TIMEOUT)

    concurrent_helper.submit_callback_request(callback_request, *callback_opts)
    return


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
    # customer_headers = {}
    return (
        Option.with_timeout(timeout),
        Option.with_request_id(str(uuid.uuid1())),
        # Option.with_headers(customer_headers),
    )


if __name__ == '__main__':
    main()
