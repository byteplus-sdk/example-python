from byteplus.media.protocol import User, Content, UserEvent


def mock_users(count: int) -> list:
    users = [None] * count
    for i in range(count):
        user: User = mock_user()
        user.user_id = user.user_id + str(i)
        users[i] = user
    return users


def mock_user() -> User:
    user: User = User()
    user.user_id = "1457789"
    user.gender = "female"
    user.age = "18-25"
    user.tags.extend(["new user", "low purchasing power", "bargain seeker"])
    user.device_id = "abc123"
    user.device_type = "app"
    user.subscriber_type = "free"
    user.language = "English"
    user.view_history.extend(["632461", "632462"])
    user.activation_channel = "AppStore"
    user.membership_level = "silver"
    user.registration_timestamp = 1623593487
    user.country = "USA"
    user.city = "Kirkland"
    user.district_or_area = "King County"
    user.postcode = "98033"

    # extra: dict = user.extra
    # extra["additionalProp1"] = "additionalVal1"

    return user


def mock_contents(count: int) -> list:
    contents = [None] * count
    for i in range(count):
        content: Content = mock_content()
        content.content_id = content.content_id + str(i)
        contents[i] = content
    return contents


def mock_content() -> Content:
    content = Content()
    content.content_id = "632461"
    content.is_recommendable = 1
    content.categories = "[{\"category_depth\":1,\"category_nodes\":[{\"id_or_name\":\"Movie\"}]},{\"category_depth\":2,\"category_nodes\":[{\"id_or_name\":\" Comedy\"}]}]"
    content.content_title = "Video #1"
    content.description = "This is a test video"
    content.content_type = "video"
    content.content_owner = "testuser#1"
    content.language = "English"
    content.tags.extend(["New", "Trending"])
    content.listing_page_display_tags.extend(["popular", "recommend"])
    content.detail_page_display_tags.extend(["popular", "recommend"])
    content.listing_page_display_type = "image"
    content.cover_multimedia_url = "https://images-na.ssl-images-amazon.com/images/I/81WmojBxvbL._AC_UL1500_.jpg"
    content.user_rating = 3.0
    content.views_count = 10000
    content.comments_count = 100
    content.likes_count = 1000
    content.shares_count = 50
    content.is_paid_content = 1
    content.origin_price = 12300
    content.current_price = 12100
    content.publish_region = "US"
    content.available_region.extend(["Singapore", "India", "US"])
    content.entity_id = "1"
    content.entity_name = "Friends"
    content.series_id = "11"
    content.series_index = 1
    content.series_name = "Friends Season 1"
    content.series_count = 10
    content.video_id = "111"
    content.video_index = 6
    content.video_name = "The One With Ross' New Girlfriend"
    content.video_count = 10
    content.video_type = "series"
    content.video_duration = 2400000
    content.publish_timestamp = 1623193487
    content.copyright_start_timestamp = 1623193487
    content.copyright_end_timestamp = 1623493487
    content.actors.extend(["Rachel Green", "Ross Geller"])
    content.source = "self"

    # extra: dict = content.extra
    # extra["additionalProp1"] = "additionalVal1"

    return content


def mock_user_events(count: int) -> list:
    user_events = [None] * count
    for i in range(count):
        user_event: UserEvent = mock_user_event()
        user_events[i] = user_event
    return user_events


def mock_user_event() -> UserEvent:
    user_event = UserEvent()
    user_event.user_id = "1457789"
    user_event.event_type = "impression"
    user_event.event_timestamp = 1623681888
    user_event.content_id = "632461"
    user_event.traffic_source = "self"
    user_event.request_id = "67a9fcf74a82fdc55a26ab4ee12a7b96890407fc0042f8cc014e07a4a560a9ac"
    user_event.rec_info = "CiRiMjYyYjM1YS0xOTk1LTQ5YmMtOGNkNS1mZTVmYTczN2FkNDASJAobcmVjZW50X2hvdF9jbGlja3NfcmV0cmlldmVyFQAAAAAYDxoKCgNjdHIdog58PBoKCgNjdnIdANK2OCIHMjcyNTgwMg=="
    user_event.attribution_token = "eyJpc3MiOiJuaW5naGFvLm5ldCIsImV4cCI6IjE0Mzg5NTU0NDUiLCJuYW1lIjoid2FuZ2hhbyIsImFkbWluIjp0cnVlfQ"
    user_event.scene_name = "Home Page"
    user_event.page_number = 2
    user_event.offset = 10
    user_event.play_type = "0"
    user_event.play_duration = 6000
    user_event.start_time = 150
    user_event.end_time = 300
    user_event.entity_id = "1"
    user_event.series_id = "11"
    user_event.video_id = "111"
    user_event.parent_content_id = "630000"
    user_event.detail_stay_time = 10
    user_event.query = "comedy"
    user_event.device = "app"
    user_event.os_type = "android"
    user_event.app_version = "9.2.0"
    user_event.device_model = "huawei-mate30"
    user_event.device_brand = "huawei"
    user_event.os_version = "10"
    user_event.browser_type = "chrome"
    user_event.user_agent = "Mozilla/5.0 (Linux; Android 10; TAS-AN00; HMSCore 5.3.0.312) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.106 HuaweiBrowser/11.0.8.303 Mobile Safari/537.36"
    user_event.network = "4g"

    # user_event.extra["additionalProp1"] = "additionalVal1"

    return user_event
