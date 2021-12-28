import logging
from datetime import timedelta
from typing import Optional

from byteplus.common.client import CommonClient
from byteplus.common.protocol import *
from byteplus.core import BizException, NetException, Option
from example.common.status_helper import is_success, is_loss_operation

log = logging.getLogger(__name__)

GET_OPERATION_TIMEOUT = timedelta(milliseconds=800)

DEFAULT_LIST_OPERATIONS_TIMEOUT = timedelta(milliseconds=800)


def get_operation_example(common_client: CommonClient, name: str):
    request = GetOperationRequest()
    request.name = name
    opts: tuple = (
        Option.with_timeout(GET_OPERATION_TIMEOUT),
    )
    try:
        response = common_client.get_operation(request, *opts)
    except (BizException, NetException,) as e:
        log.error("get operation occur error, msg:%s", str(e))
        return
    if is_success(response.status):
        log.info("get operation success rsp:\n%s", response)
        return
    if is_loss_operation(response.status):
        log.error("operation loss, name:%s", request.name)
        return
    log.error("get operation find failure info, rsp:\n%s", response)


def list_operations_example(common_client: CommonClient, filter_query: str) -> Optional[list]:
    request: ListOperationsRequest = _build_list_operation_request(filter_query, "")
    opts: tuple = (
        Option.with_timeout(DEFAULT_LIST_OPERATIONS_TIMEOUT),
    )
    try:
        response = common_client.list_operations(request, *opts)
    except (BizException, NetException) as e:
        log.error("list operations occur err, msg:%s", e)
        return None
    if not is_success(response.status):
        log.error("list operations find failure info, rsp:\n%s", response)
        return None
    log.info("list operation success")
    # When you get the next Page, you need to put the "nextPageToken"
    # returned by this Page into the request of next Page
    # nextPageRequest = build_list_operation_request(response.next_page_token)
    # request next page
    return response.operations


def _build_list_operation_request(filter_query: str, page_token: str):
    request: ListOperationsRequest = ListOperationsRequest()
    request.filter = filter_query
    request.page_size = 3
    request.page_token = page_token
    return request