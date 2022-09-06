import logging
import os
import time
import uuid
from datetime import timedelta, datetime
from signal import SIGKILL

from google.protobuf.message import Message

from byteplus.core.host_availabler_config import Config
from byteplus.core.metrics.metrics_option import MetricsCfg
from byteplus.common.protocol import DoneResponse
from byteplus.core import Option, Region, BizException, NetException
from byteplus.media import ClientBuilder, Client
from byteplus.media.protocol import WriteUsersRequest, WriteContentsRequest, WriteUserEventsRequest, \
    WriteUserEventsResponse, WriteContentsResponse, WriteUsersResponse, PredictRequest, AckServerImpressionsRequest
from example.common.status_helper import is_upload_success, is_success
from example.media.concurrent_helper import ConcurrentHelper
from example.media.mock_helper import mock_users, mock_contents, mock_user_events, mock_content

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
TENANT = "media_demo"

# # Full Param Client
# # Required Param:
# #       tenant
# #       tenant_id
# #       region
# # Optional Param:
# #       scheme
# #       headers
# #       host_availabler_config
# #       metrics_config
#
# # ping_timeout_seconds: The timeout for sending ping requests when hostAvailabler sorts the host, default is 300ms.
# # ping_interval_seconds: The interval for sending ping requests when hostAvailabler sorts the host, default is 1s.
# host_availabler_config = Config(ping_timeout_seconds=0.3, ping_interval_seconds=1)
#
# # Metrics configuration, when Metrics and Metrics Log are turned on,
# # the metrics and logs at runtime will be collected and sent to the byteplus server.
# # During debugging, byteplus can help customers troubleshoot problems.
# # enable_metrics: enable metrics, default is false.
# # enable_metrics_log: enable metrics log, default is false.
# # report_interval_seconds: The time interval for reporting metrics to the byteplus server, the default is 15s.
# #   When the QPS is high, the value of the reporting interval can be reduced to prevent loss of metrics.
# #   The longest should not exceed 30s, otherwise it will cause the loss of metrics accuracy.
# metrics_config = MetricsCfg(enable_metrics=True, enable_metrics_log=True, report_interval_seconds=15)
#
# client: Client = ClientBuilder() \
#     .tenant(TENANT) \
#     .tenant_id(TENANT_ID) \
#     .token(TOKEN) \
#     .region(Region.SG) \
#     .host_availabler_config(host_availabler_config) \
#     .metrics_config(metrics_config) \
#     .build()

client: Client = ClientBuilder() \
    .tenant(TENANT) \
    .tenant_id(TENANT_ID) \
    .token(TOKEN) \
    .region(Region.SG) \
    .build()

concurrent_helper: ConcurrentHelper = ConcurrentHelper(client)

DEFAULT_WRITE_TIMEOUT = timedelta(milliseconds=800)

DEFAULT_DONE_TIMEOUT = timedelta(milliseconds=800)

DEFAULT_PREDICT_TIMEOUT = timedelta(milliseconds=8000)

DEFAULT_ACK_IMPRESSIONS_TIMEOUT = timedelta(milliseconds=8000)

# default logLevel is Warning
logging.basicConfig(level=logging.NOTSET)


def main():
    # Write real-time user data
    write_users_example()
    # Write real-time user data concurrently
    concurrent_write_users_example()

    # Write real-time content data
    write_contents_example()
    # Write real-time content data concurrently
    concurrent_write_contents_example()

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
        response: WriteUsersResponse = client.write_users(request, *opts)
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


def write_contents_example():
    # The "WriteXXX" api can transfer max to 2000 items at one request
    request = _build_write_content_request(1)
    opts = _default_opts(DEFAULT_WRITE_TIMEOUT)
    try:
        response: WriteContentsResponse = client.write_contents(request, *opts)
    except BizException as e:
        log.error("write content occur err, msg:%s", e)
        return
    if is_upload_success(response.status):
        log.info("write content success")
        return
    log.error("write content find fail, msg:%s errItems:%s", response.status, response.errors)
    return


def concurrent_write_contents_example():
    # The "WriteXXX" api can transfer max to 2000 items at one request
    request = _build_write_content_request(1)
    opts = _default_opts(DEFAULT_WRITE_TIMEOUT)
    concurrent_helper.submit_request(request, *opts)
    return


def _build_write_content_request(count: int) -> WriteContentsRequest:
    request = WriteContentsRequest()
    request.contents.extend(mock_contents(count))
    # Optional
    # request.extra["extra_info"] = "value"
    return request


def write_user_events_example():
    # The "WriteXXX" api can transfer max to 2000 items at one request
    request = _build_write_user_event_request(1)
    opts = _default_opts(DEFAULT_WRITE_TIMEOUT)
    try:
        response: WriteUserEventsResponse = client.write_user_events(request, *opts)
    except BizException as e:
        log.error("write user_event occur err, msg:%s", e)
        return
    if is_upload_success(response.status):
        log.info("write user_event success")
        return
    log.error("write user event find failure info, msg:%s errItems:%s", response.status, response.errors)
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
    try:
        response: DoneResponse = client.done(date_list, topic, *opts)
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
    altered_contents = do_something_with_predict_result(predict_response.value)
    ack_request = _build_ack_impressions_request(predict_response.request_id, predict_request, altered_contents)
    ack_opts = _default_opts(DEFAULT_ACK_IMPRESSIONS_TIMEOUT)
    concurrent_helper.submit_request(ack_request, *ack_opts)


def _build_predict_request() -> PredictRequest:
    request = PredictRequest()
    request.user_id = "user_id"
    request.size = 20

    scene = request.scene
    scene.scene_name = "home"

    ctx = request.context
    ctx.candidate_content_ids[:] = ["cid1", "cid2"]
    ctx.root_content.CopyFrom(mock_content())
    ctx.device = "android"
    ctx.os_type = "phone"
    ctx.app_version = "app_version"
    ctx.device_model = "device_model"
    ctx.device_brand = "device_brand"
    ctx.os_version = "os_version"
    ctx.browser_type = "firefox"
    ctx.user_agent = "user_agent"
    ctx.network = "3g"

    # Optional.
    request.extra["page_num"] = "1"
    return request


def do_something_with_predict_result(predict_result):
    # You can handle recommend results here,
    # such as filter, insert other items, sort again, etc.
    # The list of goods finally displayed to user and the filtered goods
    # should be sent back to bytedance for deduplication
    return conv_to_altered_contents(predict_result.response_contents)


def conv_to_altered_contents(content_results):
    if content_results is None or len(content_results) == 0:
        return
    size = len(content_results)
    altered_contents = [None] * size
    for i in range(size):
        content_result = content_results[i]
        altered_content = AckServerImpressionsRequest.AlteredContent()
        altered_content.altered_reason = "kept"
        altered_content.content_id = content_result.content_id
        altered_content.rank = content_result.rank
        altered_contents[i] = altered_content
    return altered_contents


def _build_ack_impressions_request(predict_request_id: str, predict_request, altered_contents: list):
    request = AckServerImpressionsRequest()
    request.predict_request_id = predict_request_id
    request.user_id = predict_request.user_id
    scene: Message = request.scene
    scene.CopyFrom(predict_request.scene)
    request.altered_contents.extend(altered_contents)
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