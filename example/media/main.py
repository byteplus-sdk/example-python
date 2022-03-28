import logging
import os
import time
import uuid
from datetime import timedelta, datetime
from signal import SIGKILL

from byteplus.common.protocol import DoneResponse
from byteplus.core import Option, Region, BizException
from byteplus.media import ClientBuilder, Client
from byteplus.media.protocol import WriteUsersRequest, WriteContentsRequest, WriteUserEventsRequest, \
    WriteUserEventsResponse, WriteContentsResponse, WriteUsersResponse
from example.common.status_helper import is_upload_success, is_success
from example.media.concurrent_helper import ConcurrentHelper
from example.media.mock_helper import mock_users, mock_contents, mock_user_events

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

concurrent_helper: ConcurrentHelper = ConcurrentHelper(client)

DEFAULT_WRITE_TIMEOUT = timedelta(milliseconds=800)

DEFAULT_DONE_TIMEOUT = timedelta(milliseconds=800)

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


def _default_opts(timeout: timedelta) -> tuple:
    # customer_headers = {}
    return (
        Option.with_timeout(timeout),
        Option.with_request_id(str(uuid.uuid1())),
        # Option.with_headers(customer_headers),
    )


if __name__ == '__main__':
    main()