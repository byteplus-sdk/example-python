import json
import uuid
from datetime import datetime, timedelta

from byteplus.common.protocol import DoneResponse
from byteplus.general import Client, ClientBuilder
from byteplus.core import Region, Option, BizException, NetException

from byteplus.general.protocol import WriteResponse, PredictRequest, PredictUser, CallbackRequest, PredictResponse, \
    CallbackItem

client: Client = ClientBuilder() \
    .tenant_id("xxxxx") \
    .tenant("general_demo") \
    .token("dwffe2") \
    .region(Region.CN) \
    .build()


# 数据上传example
def write_data_example():
    # 此处为测试数据，实际调用时需注意字段类型和格式
    data_list: list = [{
        "user_id": "1457789",
        "register_time": "12413",
        "age": 12,
        "gender": "male",
        "extra_info": "{\"other\":\"xxx\"}"
    }, {
        "user_id": "32342",
        "register_time": "12413",
        "age": 12,
        "gender": "male",
        "extra_info": "{\"other\":\"xxx\"}"
    }]
    # topic为枚举值，请参考API文档
    topic: str = "user"

    # 传输增量天级数据，如数据2021-09-01的数据
    opts: tuple = write_options()

    try:
        rsp = client.write_data(data_list, topic, *opts)
    except BizException as e:
        print("upload occur error, msg:%s" % e)
        return
    code: int = rsp.status.code
    if code is None or code == 0:
        print("upload success, rsp:%s" % rsp)
        return

    # 出现错误、异常时请记录好日志，方便自行排查问题
    print("upload fail, msg:%s errItems:%s" % rsp.status, rsp.errors)
    return


# 数据同步请求参数说明，请根据说明修改
def write_options() -> tuple:
    return (
        # 必传，要求每次请求的Request-Id不重复，若未传，sdk会默认为每个请求添加
        Option.with_request_id(str(uuid.uuid1())),
        # 数据产生日期，实际传输时需修改为实际数据对应的日期
        # 增量天级数据上报时【必传】
        # 增量实时数据/历史数据上报时【不传】
        Option.with_data_date(datetime(year=2021, month=9, day=1)),
        # 可选，请求超时时间，根据实际情况调整，建议设置大些
        Option.with_timeout(timedelta(milliseconds=800)),
    )


def done_example():
    date: datetime = datetime(year=2021, month=9, day=1)
    # 已经上传完成的数据日期，可在一次请求中传多个
    date_list: list = [date]
    # 与离线天级数据传输的topic保持一致
    topic = "user"
    opts = done_options()
    try:
        rsp = client.done(date_list, topic, *opts)
    except BizException as e:
        print("done occur error, msg:%s" % e)
        return
    code: int = rsp.status.code
    if code is None or code == 0:
        print("done success, rsp:%s" % rsp)
        return

    # 出现错误、异常时请记录好日志，方便自行排查问题
    print("done fail, msg:%s" % rsp.status)
    return


# done请求参数说明，请根据说明修改
def done_options() -> tuple:
    return (
        # 必传，要求每次请求的Request-Id不重复，若未传，sdk会默认为每个请求添加
        Option.with_request_id(str(uuid.uuid1())),
        # 可选，请求超时时间，根据实际情况调整，建议设置大些
        Option.with_timeout(timedelta(milliseconds=800)),
    )


def recommend_example():
    predict_request: PredictRequest = _build_predict_request()
    # The `scene` is provided by ByteDance, according to tenant's situation
    scene = "home"
    predict_opts = default_opts(timedelta(milliseconds=800))
    try:
        rsp = client.predict(predict_request, scene, *predict_opts)
    except BizException as e:
        print("predict occur exception, msg:%s" % e)
        return
    code: int = rsp.code
    if code is None or code == 0 or code == 200:
        print("predict success, rsp:%s" % rsp)

        # 如果客户服务端没有对推荐结果进行任何后置处理，而是直接将火山引擎的结果原封不动下发至客户端，
        # 则不需要调用callback，否则应当调用callback返回实际曝光结果
        # callback_example(predict_request, rsp)
        return

    # 出现错误、异常时请记录好日志，方便自行排查问题
    print("predict fail, rsp:%s" % rsp)



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


# 将推荐请求结果（实际曝光数据）通过callback接口上报
def callback_example(predict_request: PredictRequest, predict_response: PredictResponse):
    # 需要与predict的scene保持一致
    scene = "home"
    callback_items = do_something_with_predict_result(predict_response.value)
    callback_request = CallbackRequest()
    callback_request.scene = scene
    callback_request.predict_request_id = predict_response.request_id
    callback_request.uid = predict_request.user.uid
    callback_request.items.extend(callback_items)
    callback_opts = default_opts(timedelta(milliseconds=800))
    try:
        rsp = client.callback(callback_request, *callback_opts)
    except BizException as e:
        print("callback occur exception, msg:%s" % e)
        return
    code: int = rsp.code
    if code is None or code == 0 or code == 200:
        print("callback success, rsp:%s" % rsp)
        return


def do_something_with_predict_result(predict_result) -> list:
    # 推荐结果处理后（如过滤、插入其他项、再次排序等），
    # 需将最终显示给用户的商品列表和过滤后的商品上报给服务端，进行重复数据删除
    return conv_to_callback_items(predict_result.items)


def conv_to_callback_items(product_items: list) -> list:
    if product_items is None or len(product_items) == 0:
        return []
    size = len(product_items)
    callback_items = [None] * size
    for i in range(size):
        product_result = product_items[i]
        extra_map = {}
        # extra_map = {"reason": "kept"}
        callback_item = CallbackItem()
        callback_item.id = product_result.id
        callback_item.pos = str(i + 1)
        callback_item.extra = json.dumps(extra_map)
        callback_items[i] = callback_item
    return callback_items



def default_opts(timeout: timedelta) -> tuple:
    return (
        Option.with_timeout(timeout),
        Option.with_request_id(str(uuid.uuid1())),
    )

if __name__ == '__main__':
    # write_data_example()
    # done_example()
    recommend_example()