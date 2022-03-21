import time

from byteplus.rutenad.protocol import *


def mock_users(count: int) -> list:
    users = [None] * count
    for i in range(count):
        user: User = mock_user()
        user.user_id = user.user_id + str(i)
        users[i] = user
    return users


def mock_user() -> User:
    user: User = User()
    user.user_id = "user_id"
    user.gender = "male"
    user.age = "23"
    user.tags.extend(["tag1", "tag2", "tag3"])
    user.activation_channel = "AppStore"
    user.membership_level = "silver"
    user.registration_timestamp = int(time.time())

    location: User.Location = user.location
    location.country = "china"
    location.city = "beijing"
    location.district_or_area = "haidian"
    location.postcode = "123456"
    user.app_list.extend(["app1", "app2", "app3"])

    extra: dict = user.extra
    extra["first_name"] = "first"

    return user


def mock_products(count: int) -> list:
    products = [None] * count
    for i in range(count):
        product: Product = mock_product()
        product.product_id = product.product_id + str(i)
        products[i] = product
    return products


def mock_product() -> Product:
    product = Product()
    product.product_id = "product_id"
    product.is_recommendable = True
    product.title = "title"
    product.quality_score = 3.4
    product.tags.extend(["tag1", "tag2", "tag3"])
    product.extra["count"] = "20"

    category1 = product.categories.add()
    category1.category_depth = 1
    category1.category_nodes.extend(["cate_1_1"])
    category2 = product.categories.add()
    category2.category_depth = 1
    category2.category_nodes.extend(["cate_2_1", "cate_2_2"])

    brand1 = product.brands.add()
    brand1.brand_depth = 1
    brand1.id_or_name = "brand_1"
    brand2 = product.brands.add()
    brand2.brand_depth = 2
    brand2.id_or_name = "brand_2"

    price = product.price
    price.current_price = 10
    price.origin_price = 10

    display = product.display
    display.detail_page_display_tags.extend(["tag1", "tag2"])
    display.listing_page_display_tags.extend(["taga", "tagb"])
    display.listing_page_display_type = "image"
    display.cover_multimedia_url = "https://www.google.com"

    spec = product.product_spec
    spec.product_group_id = "group_id"
    spec.user_rating = 2.0
    spec.comment_count = 100
    spec.source = "自营"
    spec.publish_timestamp = int(time.time())

    seller = product.seller
    seller.id = "seller_id"
    seller.seller_level = "level1"
    seller.seller_rating = 3.5

    return product


def mock_user_events(count: int) -> list:
    user_events = [None] * count
    for i in range(count):
        user_event: UserEvent = mock_user_event()
        user_events[i] = user_event
    return user_events


def mock_user_event() -> UserEvent:
    user_event = UserEvent()
    user_event.user_id = "user_id"
    user_event.event_type = "purchase"
    user_event.event_timestamp = int(time.time())
    user_event.product_id = "product_id"
    user_event.attribution_token = "attribution_token"
    user_event.rec_info = "trans_data"
    user_event.traffic_source = "self"
    user_event.purchase_count = 20
    user_event.extra["children"] = "true"

    scene = user_event.scene
    scene.scene_name = "scene_name"
    scene.page_number = 2
    scene.offset = 10

    user_event.device.CopyFrom(mock_device())

    context = user_event.context
    context.query = "query"
    context.root_product_id = "root_product_id"

    return user_event


def mock_device() -> UserEvent.Device:
    device = UserEvent.Device()
    device.platform = "app"
    device.os_type = "android"
    device.app_version = "app_version"
    device.device_model = "device_model"
    device.device_brand = "device_brand"
    device.os_version = "os_version"
    device.browser_type = "firefox"
    device.user_agent = "user_agent"
    device.network = "3g"
    return device


def mock_advertisements(count: int) -> list:
    advertisements = [None] * count
    for i in range(count):
        advertisement: Advertisement = mock_advertisement()
        advertisement.advertisement_id = advertisement.advertisement_id + str(i)
        advertisements[i] = advertisement
    return advertisements


def mock_advertisement() -> Advertisement:
    advertisement = Advertisement()
    advertisement.advertisement_id = "advertisement_id"
    advertisement.advertiser_id_or_name = "15"
    advertisement.cost_type = "cpm"
    advertisement.convert_type = "购买"
    advertisement.scene = "首页"
    advertisement.bid_price = 10012
    advertisement.status = "1"
    advertisement.start_timestamp = int(time.time())
    advertisement.end_timestamp = int(time.time())
    advertisement.extra["product_id"] = "11011"

    target1 = advertisement.targets.add()
    target1.age = "25"
    target1.gender = "male"
    target1.platform = "ios"
    target1.extra["ios_version"] = "14.7.1"
    target1.extra["interest_tags"] = '["连衣裙", "雪纺"]'
    target2 = advertisement.targets.add()
    target2.age = "25"
    target2.gender = "male"
    target2.platform = "ios"

    bid_words1 = advertisement.bid_words.add()
    bid_words1.query = "女装"
    bid_words1.price = "10"
    bid_words1.match_type = "广泛匹配"
    bid_words1.extra["poularity"] = "0.8"
    bid_words2 = advertisement.bid_words.add()
    bid_words1.query = "连衣裙"
    bid_words1.price = "10"
    bid_words1.match_type = "短语匹配"

    advertisement.source = "自营广告"
    advertisement.campaign_id = "10001"
    advertisement.content_type = "落地页"
    advertisement.title = "最新雪纺连衣裙网红款"
    advertisement.image_mode = "大图"
    advertisement.image_url = "https://xxx.jpg"
    advertisement.video_md5 = "098F6BCD4621D373CADE4E832627B4F6"
    advertisement.tags.extend(["连衣裙", "包包", "大码", "满减"])

    category1 = advertisement.categories.add()
    category1.category_depth = 1
    category1.category_nodes.extend(["cate_1_1"])
    category2 = advertisement.categories.add()
    category2.category_depth = 1
    category2.category_nodes.extend(["cate_2_1", "cate_2_2"])

    return advertisement
