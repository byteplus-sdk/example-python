import logging
import os
import time
import uuid
from datetime import timedelta
from signal import SIGKILL

from byteplus.core.host_availabler_config import Config
from byteplus.core.metrics.metrics_option import MetricsCfg
from byteplus.core import Region, BizException, Option
from byteplus.rutenad import Client, ClientBuilder
from byteplus.rutenad.protocol import WriteUsersRequest, WriteProductsRequest, WriteAdvertisementsRequest, \
    WriteUserEventsRequest

from example.rutenad.concurrent_helper import ConcurrentHelper
from example.rutenad.mock_helper import mock_users, mock_products, mock_user_events, mock_advertisements
from example.common.request_helper import RequestHelper
from example.common.status_helper import is_upload_success

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
TENANT = "ruten_demo"

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
    .region(Region.CN) \
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

    # Write real-time product data
    write_products_example()
    # Write real-time product data concurrently
    concurrent_write_products_example()

    # Write real-time user event data
    write_user_events_example()
    # Write real-time user event data concurrently
    concurrent_write_user_events_example()

    # Write real-time advertisements data
    write_advertisements_example()
    # Write real-time advertisements data concurrently
    concurrent_write_advertisements_example()

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


def write_advertisements_example():
    # The "WriteXXX" api can transfer max to 2000 items at one request
    request = _build_write_advertisements_request(1)
    opts = _default_opts(DEFAULT_WRITE_TIMEOUT)
    try:
        response = request_helper.do_with_retry(client.write_advertisements, request, opts, DEFAULT_RETRY_TIMES)
    except BizException as e:
        log.error("write advertisements occur err, msg:%s", e)
        return
    if is_upload_success(response.status):
        log.info("write advertisements success")
        return
    log.error("write advertisements find failure info, msg:%s errItems:%s", response.status, response.errors)
    return


def concurrent_write_advertisements_example():
    # The "WriteXXX" api can transfer max to 2000 items at one request
    request = _build_write_advertisements_request(1)
    opts = _default_opts(DEFAULT_WRITE_TIMEOUT)
    concurrent_helper.submit_request(request, *opts)
    return


def _build_write_advertisements_request(count: int) -> WriteAdvertisementsRequest:
    advertisements = mock_advertisements(count)
    request = WriteAdvertisementsRequest()
    request.advertisements.extend(advertisements)
    # Optional
    # request.extra["extra_info"] = "value"
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
