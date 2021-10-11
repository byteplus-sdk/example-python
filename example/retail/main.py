import logging
import os
import time
import uuid
from datetime import datetime, timezone, timedelta
from signal import SIGKILL

from google.protobuf.message import Message

from byteplus.core import Region, BizException, Option, NetException
from byteplus.core.time_hlper import rfc3339_format
from byteplus.retail import Client, ClientBuilder
from byteplus.retail.protocol import *

from example.retail.concurrent_helper import ConcurrentHelper
from example.retail.constant import TENANT, TENANT_ID, TOKEN
from example.retail.mock_helper import mock_users, mock_products, mock_user_events, mock_product, mock_device
from example.common.request_helper import RequestHelper
from example.common.status_helper import is_upload_success, is_success
from example.common.example import get_operation_example as do_get_operation
from example.common.example import list_operations_example as do_list_operations

log = logging.getLogger(__name__)

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

DEFAULT_IMPORT_TIMEOUT = timedelta(milliseconds=800)

DEFAULT_PREDICT_TIMEOUT = timedelta(milliseconds=800)

DEFAULT_ACK_IMPRESSIONS_TIMEOUT = timedelta(milliseconds=800)

# default logLevel is Warning
logging.basicConfig(level=logging.NOTSET)


def main():
    # Write real-time user data
    write_users_example()
    # Write real-time user data concurrently
    concurrent_write_users_example()
    # Import daily offline user data
    import_users_example()
    # Import daily offline user data concurrently
    concurrent_import_users_example()

    # Write real-time product data
    write_products_example()
    # Write real-time product data concurrently
    concurrent_write_products_example()
    # Import daily offline product data
    import_products_example()
    # Concurrent import daily offline product data
    concurrent_import_products_example()

    # Write real-time user event data
    write_user_events_example()
    # Write real-time user event data concurrently
    concurrent_write_user_events_example()
    # Import daily offline user event data
    import_user_events_example()
    # Concurrent import daily offline user event data
    concurrent_import_user_events_example()

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

    time.sleep(3)
    client.release()
    os.kill(os.getpid(), SIGKILL)


def write_users_example():
    # The "WriteXXX" api can transfer max to 100 items at one request
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
    log.error("write suer find fail, msg:%s errItems:%s", response.status, response.errors)
    return


def concurrent_write_users_example():
    # The "WriteXXX" api can transfer max to 100 items at one request
    request = _build_write_user_request(1)
    opts = _default_opts(DEFAULT_WRITE_TIMEOUT)
    concurrent_helper.submit_request(request, *opts)
    return


def _build_write_user_request(count: int) -> WriteUsersRequest:
    request = WriteUsersRequest()
    request.users.extend(mock_users(count))
    request.extra["extra_info"] = "value"
    return request


def import_users_example():
    # The "ImportXXX" api can transfer max to 10k items at one request
    request: ImportUsersRequest = _build_import_users_request(10)
    response: ImportUsersResponse = ImportUsersResponse()
    opts: tuple = _default_opts(DEFAULT_IMPORT_TIMEOUT)
    try:
        request_helper.do_import(client.import_users, request, response, opts, DEFAULT_RETRY_TIMES)
    except BizException as e:
        log.error("import user occur err, msg:%s", str(e))
        return
    if is_success(response.status):
        log.info("import user success")
        return
    log.error("import user find failure info, msg:%s errSamples:%s", response.status, response.error_samples)
    return


def concurrent_import_users_example():
    # The "ImportXXX" api can transfer max to 10k items at one request
    request: ImportUsersRequest = _build_import_users_request(10)
    opts: tuple = _default_opts(DEFAULT_IMPORT_TIMEOUT)
    concurrent_helper.submit_request(request, *opts)
    return


def _build_import_users_request(count: int) -> ImportUsersRequest:
    request: ImportUsersRequest = ImportUsersRequest()
    input_config = request.input_config
    inline_source = input_config.users_inline_source
    inline_source.users.extend(mock_users(count))
    date_config = request.date_config
    # format time by RCF3339
    date_config.date = datetime.now(timezone.utc).isoformat()
    date_config.is_end = False
    return request


def write_products_example():
    # The "WriteXXX" api can transfer max to 100 items at one request
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
    log.error("write suer find fail, msg:%s errItems:%s", response.status, response.errors)
    return


def concurrent_write_products_example():
    # The "WriteXXX" api can transfer max to 100 items at one request
    request = _build_write_product_request(1)
    opts = _default_opts(DEFAULT_WRITE_TIMEOUT)
    concurrent_helper.submit_request(request, *opts)
    return


def _build_write_product_request(count: int) -> WriteProductsRequest:
    request = WriteProductsRequest()
    request.products.extend(mock_products(count))
    request.extra["extra_info"] = "value"
    return request


def import_products_example():
    # The "ImportXXX" api can transfer max to 10k items at one request
    request: ImportProductsRequest = _build_import_products_request(10)
    response: ImportProductsResponse = ImportProductsResponse()
    opts: tuple = _default_opts(DEFAULT_IMPORT_TIMEOUT)
    try:
        request_helper.do_import(client.import_products, request, response, opts, DEFAULT_RETRY_TIMES)
    except BizException as e:
        log.error("import product occur err, msg:%s", str(e))
        return
    if is_success(response.status):
        log.info("import product success")
        return
    log.error("import product find failure info, msg:%s errSamples:%s",
              response.status, response.error_samples)
    return


def concurrent_import_products_example():
    # The "ImportXXX" api can transfer max to 10k items at one request
    request: ImportProductsRequest = _build_import_products_request(10)
    opts: tuple = _default_opts(DEFAULT_IMPORT_TIMEOUT)
    concurrent_helper.submit_request(request, *opts)
    return


def _build_import_products_request(count: int) -> ImportProductsRequest:
    request: ImportProductsRequest = ImportProductsRequest()
    input_config = request.input_config
    inline_source = input_config.products_inline_source
    inline_source.products.extend(mock_products(count))
    date_config = request.date_config
    # format time by RFC3339
    date_config.date = datetime.now(timezone.utc).isoformat()
    date_config.is_end = False
    return request


def write_user_events_example():
    # The "WriteXXX" api can transfer max to 100 items at one request
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
    log.error("write user find failure info, msg:%s errItems:%s", response.status, response.errors)
    return


def concurrent_write_user_events_example():
    # The "WriteXXX" api can transfer max to 100 items at one request
    request = _build_write_user_event_request(1)
    opts = _default_opts(DEFAULT_WRITE_TIMEOUT)
    concurrent_helper.submit_request(request, *opts)
    return


def _build_write_user_event_request(count: int) -> WriteUserEventsRequest:
    user_events = mock_user_events(count)
    request = WriteUserEventsRequest()
    request.user_events.extend(user_events)
    request.extra["extra_info"] = "value"
    return request


def import_user_events_example():
    # The "ImportXXX" api can transfer max to 10k items at one request
    request: ImportUserEventsRequest = _build_import_user_events_request(10)
    response: ImportUserEventsResponse = ImportUserEventsResponse()
    opts: tuple = _default_opts(DEFAULT_IMPORT_TIMEOUT)
    try:
        request_helper.do_import(client.import_user_events, request, response, opts, DEFAULT_RETRY_TIMES)
    except BizException as e:
        log.error("import user_event occur err, msg:%s", str(e))
        return
    if is_success(response.status):
        log.info("import user_event success")
        return
    log.error("import user_event find failure info, msg:%s errSamples:%s",
              response.status, response.error_samples)
    return


def concurrent_import_user_events_example():
    # The "ImportXXX" api can transfer max to 10k items at one request
    request: ImportUserEventsRequest = _build_import_user_events_request(10)
    opts: tuple = _default_opts(DEFAULT_IMPORT_TIMEOUT)
    concurrent_helper.submit_request(request, *opts)
    return


def _build_import_user_events_request(count: int) -> ImportUserEventsRequest:
    request: ImportUserEventsRequest = ImportUserEventsRequest()
    input_config = request.input_config
    inline_source = input_config.user_events_inline_source
    inline_source.user_events.extend(mock_user_events(count))
    date_config = request.date_config
    # format time by RFC3339
    date_config.date = rfc3339_format(datetime.now())
    date_config.is_end = False
    return request


def get_operation_example():
    name = "750eca88-5165-4aae-851f-a93b75a27b03"
    do_get_operation(client, name)


def list_operations_example():
    filter_query = "date>=2021-06-15 and worksOn=ImportUsers and done=true"
    operations = do_list_operations(client, filter_query)
    _parse_task_response(operations)


def _parse_task_response(operations: list):
    if operations is None or len(operations) == 0:
        return
    for operation in operations:
        if not operation.done:
            continue
        response_any = operation.response
        type_url = response_any.type_url
        try:
            if "ImportUsers" in type_url:
                response: Message = ImportUsersResponse()
                response.ParseFromString(response_any.value)
                log.info("[ListOperations] ImportUsers rsp:\n%s", response)
            elif "ImportProducts" in type_url:
                response: Message = ImportProductsResponse()
                response.ParseFromString(response_any.value)
                log.info("[ListOperations] ImportProducts rsp:\n%s", response)
            elif "ImportUserEvents" in type_url:
                response: Message = ImportUserEventsResponse()
                response.ParseFromString(response_any.value)
                log.info("[ListOperations] ImportUserEvents rsp:\n%s", response)
            else:
                log.error("[ListOperations] unexpected task response type:%s", type_url)
        except BaseException as e:
            log.error("[ListOperations] parse task response fail, msg:%s", e)
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
    customer_headers = {}
    return (
        Option.with_timeout(timeout),
        Option.with_request_id(str(uuid.uuid1())),
        Option.with_headers(customer_headers),
    )


if __name__ == '__main__':
    main()
