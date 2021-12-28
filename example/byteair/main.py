import json
import logging
import os
import time
import uuid
from datetime import datetime, timedelta
from signal import SIGKILL


from byteplus.core import Region, BizException, Option, NetException
from byteplus.general import Client, ClientBuilder
from byteplus.general.protocol import WriteResponse, ImportResponse, PredictRequest,\
    CallbackRequest, CallbackItem, PredictUser
from byteplus.common.protocol import DoneResponse
from example.common.example import get_operation_example as do_get_operation
from example.common.request_helper import RequestHelper
from example.common.status_helper import is_upload_success, is_success, is_success_code
from example.byteair.concurrent_helper import ConcurrentHelper
from example.byteair.mock_hlper import mock_data_list

log = logging.getLogger(__name__)

"""
租户相关信息
"""
# 字节侧提供，用于签名
TOKEN = "xxxxxxxxxxxx"

# 火山引擎申请的账号id/租户id(tenant_id)，如"2100021"
TENANT_ID = "xxxxxxxxxxxx"

# 个性化推荐服务新建的项目id(project_id)，如"1231314"
PROJECT_ID = "xxxxxxxxxxxx"

"""
stage枚举值，与推荐平台四种同步阶段相对应
"""
STAGE_INCREMENTAL_SYNC_STREAMING = "incremental_sync_streaming"  # 增量实时数据同步阶段

STAGE_INCREMENTAL_SYNC_DAILY = "incremental_sync_daily"  # 增量天级数据同步阶段

STAGE_PRE_SYNC = "pre_sync"  # 测试数据/预同步阶段

STAGE_HISTORY_SYNC = "history_sync"  # 历史数据同步阶段

"""
标准数据topic枚举值，包括：item(物品，如商品、媒资数据、社区内容等)、user(用户)、behavior(行为)
"""
TOPIC_ITEM = "item"  # 物品

TOPIC_USER = "user"  # 用户

TOPIC_BEHAVIOR = "behavior"  # 行为

# 必传参数:
#       tenant 填项目project_id
#       tenant_id
#       region, 必须填Region.AIR，默认使用byteair-api-cn1.snssdk.com为host
#
# 可选参数:
#       hosts, 如果设置了region则host可不设置
#       scheme, 仅支持"https"和"http"
#       headers, 支持添加自定义header
client: Client = ClientBuilder() \
    .tenant(PROJECT_ID) \
    .tenant_id(TENANT_ID) \
    .token(TOKEN) \
    .region(Region.AIR) \
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
    # 实时数据上传
    write_data_example()
    # 并发实时数据上传
    concurrent_write_data_example()
    # 天级离线数据上传
    import_data_example()
    # 并发天级离线数据上传
    concurrent_import_data_example()

    # 标识天级离线数据上传完成
    done_example()
    # 并发标识天级离线数据上传完成
    concurrent_done_example()

    # 与Import接口一起使用，用于天级数据上传状态（是否处理完成，成功/失败条数）监听
    get_operation_example()

    # 请求推荐服务获取推荐结果
    recommend_example()

    time.sleep(3)
    client.release()
    os.kill(os.getpid(), SIGKILL)


# 增量实时数据上传example
def write_data_example():
    # 此处为测试数据，实际调用时需注意字段类型和格式
    data_list: list = mock_data_list(2)
    # topic为枚举值，请参考API文档
    topic: str = TOPIC_USER
    opts: tuple = _write_options()

    def call(call_data_list, *call_opts: Option) -> WriteResponse:
        return client.write_data(call_data_list, topic, *call_opts)

    try:
        # 带重试的请求，自行实现重试时请参考此处重试逻辑
        response = request_helper.do_with_retry(call, data_list, opts, DEFAULT_RETRY_TIMES)
    except BizException as e:
        log.error("write occur err, msg:%s", e)
        return
    if is_upload_success(response.status):
        log.info("write success")
        return
    # 出现错误、异常时请记录好日志，方便自行排查问题
    log.error("write find failure info, msg:%s errItems:%s", response.status, response.errors)
    return


# 增量实时数据并发/异步上传example
def concurrent_write_data_example():
    data_list: list = mock_data_list(2)
    topic: str = TOPIC_USER
    opts: tuple = _write_options()
    concurrent_helper.submit_write_request(data_list, topic, *opts)
    return


# Write请求参数说明，请根据说明修改
def _write_options() -> tuple:
    # customer_headers = {}
    return (
        # 必选.Write接口只能用于实时数据传输，此处只能填"incremental_sync_streaming"
        Option.with_stage(STAGE_INCREMENTAL_SYNC_STREAMING),
        # 必传，要求每次请求的Request-Id不重复，若未传，sdk会默认为每个请求添加
        Option.with_request_id(str(uuid.uuid1())),
        # 可选，请求超时时间，根据实际情况调整，建议设置大些
        Option.with_timeout(DEFAULT_WRITE_TIMEOUT),
        # 可选.添加自定义header.
        # Option.with_headers(customer_headers),
        # 可选. 服务端期望在一定时间内返回，避免客户端超时前响应无法返回。
        # 此服务器超时应小于Write请求设置的总超时。
        Option.with_server_timeout(DEFAULT_WRITE_TIMEOUT - timedelta(milliseconds=50))
    )


# 离线天级数据上传example
def import_data_example():
    # 一个“Import”请求中包含的数据条数最多为10k，如果数据太多，服务器将拒绝请求。
    data_list: list = mock_data_list(2)
    # topic: str = TOPIC_USER
    topic: str = TOPIC_USER
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
    log.error("import find failure info, msg: %s error_samples: %s", response.status, response.error_samples)
    return


# 离线天级数据并发/异步上传example
def concurrent_import_data_example():
    data_list: list = mock_data_list(2)
    topic: str = TOPIC_USER
    opts: tuple = _write_options()
    concurrent_helper.submit_write_request(data_list, topic, *opts)
    return


# Import请求参数说明，请根据说明修改
def _import_options() -> tuple:
    # customer_headers = {}
    return (
        # 必传， Import接口数据传输阶段，包括：
        # 测试数据/预同步阶段（"pre_sync"）、历史数据同步（"history_sync"）和增量天级数据上传（"incremental_sync_daily"）
        Option.with_stage(STAGE_PRE_SYNC),
        # 必传，要求每次请求的Request-Id不重复，若未传，sdk会默认为每个请求添加
        Option.with_request_id(str(uuid.uuid1())),
        # 必传，数据产生日期，实际传输时需修改为实际日期
        Option.with_data_date(datetime(year=2021, month=9, day=1)),
        # 可选，请求超时时间，根据实际情况调整，建议设置大些
        Option.with_timeout(DEFAULT_IMPORT_TIMEOUT),
        # 可选.添加自定义header.
        # Option.with_headers(customer_headers)
    )


# 离线天级数据上传完成后Done接口example
def done_example():
    date: datetime = datetime(year=2021, month=9, day=1)
    # 已经上传完成的数据日期，可在一次请求中传多个
    date_list: list = [date]
    # 与离线天级数据传输的topic保持一致
    topic = TOPIC_USER
    opts = _done_options()

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


# 离线天级数据上传完成后异步Done接口example，done接口一般无需异步
def concurrent_done_example():
    date: datetime = datetime(year=2021, month=9, day=1)
    date_list: list = [date]
    topic = TOPIC_USER
    opts = _done_options()
    concurrent_helper.submit_done_request(date_list, topic, *opts)
    return


# Import请求参数说明，请根据说明修改
def _done_options() -> tuple:
    # customer_headers = {}
    return (
        # 必传， Import接口数据传输阶段，包括：
        # 测试数据/预同步阶段（"pre_sync"）、历史数据同步（"history_sync"）和增量天级数据上传（"incremental_sync_daily"）
        Option.with_stage(STAGE_PRE_SYNC),
        # 必传，要求每次请求的Request-Id不重复，若未传，sdk会默认为每个请求添加
        Option.with_request_id(str(uuid.uuid1())),
        # 可选，请求超时时间，根据实际情况调整，建议设置大些
        Option.with_timeout(DEFAULT_IMPORT_TIMEOUT),
        # 可选.添加自定义header.
        # Option.with_headers(customer_headers)
    )


# getOperation接口使用example，一般与Import接口一起使用，用于天级数据上传状态监听
def get_operation_example():
    name = "0c5a1145-2c12-4b83-8998-2ae8153ca089"
    do_get_operation(client, name)
    return


# 推荐服务请求example
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


def _default_opts(timeout: timedelta) -> tuple:
    # customer_headers = {}
    return (
        Option.with_timeout(timeout),
        Option.with_request_id(str(uuid.uuid1())),
        # Option.with_headers(customer_headers),
    )


if __name__ == '__main__':
    main()
