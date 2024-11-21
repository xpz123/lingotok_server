import json

from volcengine.ApiInfo import ApiInfo
from volcengine.Credentials import Credentials
from volcengine.ServiceInfo import ServiceInfo
from volcengine.base.Service import Service

def translate_text2ar(text_list, target_lang, source_lang="en"):
    k_access_key = 'AKLTOTgzODg1Y2FiNDI5NGE3Mzk3MWEzYzJlODE3MDk2MzQ' # https://console.volcengine.com/iam/keymanage/
    k_secret_key = 'TTJJM016azRaR0V3WXpRMk5EUXhPR0kyT0RBNVlUY3hZVGd5WlRrMlpHTQ=='
    k_service_info = \
        ServiceInfo('translate.volcengineapi.com',
                    {'Content-Type': 'application/json'},
                    Credentials(k_access_key, k_secret_key, 'translate', 'cn-north-1'),
                    5,
                    5)
    k_query = {
        'Action': 'TranslateText',
        'Version': '2020-06-01'
    }
    k_api_info = {
        'translate': ApiInfo('POST', '/', k_query, {}, {})
    }
    service = Service(k_service_info, k_api_info)
    body = {
        'TargetLanguage': target_lang,
        'TextList': text_list,
    }
    res = service.json('translate', {}, json.dumps(body))
    return (json.loads(res))