def mock_data_list(count: int) -> list:
    data_list: list = [None] * count
    for i in range(count):
        data_list[i] = mock_data()
    return data_list


def mock_data():
    result = {
        "user_id": "1457789",
        "goods_id": "123134",
        "event_type": "purchase",
        "event_timestamp": 1623681767,
        "scene_scene_name": "product detail page",
        "scene_page_number": 2,
        "scene_offset": 10,
        "product_id": "632461",
        "device_platform": "android",
        "device_os_type": "phone",
        "device_app_version": "9.2.0",
        "device_device_model": "huawei-mate30",
        "device_device_brand": "huawei",
        "device_os_version": "10",
        "device_browser_type": "chrome",
        "device_user_agent": "Mozilla/5.0 (Linux Android 10 TAS-AN00 HMSCore 5.3.0.312) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.106 HuaweiBrowser/11.0.8.303 Mobile Safari/537.36",
        "device_network": "3g",
        "context_query": "",
        "context_root_product_id": "441356",
        "attribution_token": "eyJpc3MiOiJuaW5naGFvLm5ldCIsImV4cCI6IjE0Mzg5NTU0NDUiLCJuYW1lIjoid2FuZ2hhbyIsImFkbWluIjp0cnVlfQ",
        "rec_info": "CiRiMjYyYjM1YS0xOTk1LTQ5YmMtOGNkNS1mZTVmYTczN2FkNDASJAobcmVjZW50X2hvdF9jbGlja3NfcmV0cmlldmVyFQAAAAAYDxoKCgNjdHIdog58PBoKCgNjdnIdANK2OCIHMjcyNTgwMg==",
        "traffic_source": "self",
        "purchase_count": 20,
        "extra": "{\"session_id\":\"sess_89j9ifuqrbplk0rti2va2k1ha0\",\"request_id\":\"860ae3f6-7e4d-43a9-8699-114cbd72c287\"}",
    }
    return result
