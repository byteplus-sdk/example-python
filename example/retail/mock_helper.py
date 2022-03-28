import time

from byteplus.retail.protocol import *


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
    category1_node1 = category1.category_nodes.add()
    category1_node1.id_or_name = "cate_1_1"
    category2 = product.categories.add()
    category2.category_depth = 1
    category2_node1 = category2.category_nodes.add()
    category2_node1.id_or_name = "cate_2_1"
    category2_node2 = category2.category_nodes.add()
    category2_node2.id_or_name = "cate_2_2"

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
    spec.user_rating = 0.23
    spec.comment_count = 100
    spec.source = "self"
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
    device.platform = "android"
    device.os_type = "phone"
    device.app_version = "app_version"
    device.device_model = "device_model"
    device.device_brand = "device_brand"
    device.os_version = "os_version"
    device.browser_type = "firefox"
    device.user_agent = "user_agent"
    device.network = "3g"
    return device
