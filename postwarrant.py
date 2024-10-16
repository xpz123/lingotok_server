import requests
import hashlib
import time



appid = "a0007iu"  # 您的appid
timestamp = str(int(time.time()))  # 当前时间戳，以秒为单位的10位时间戳
print(timestamp)
user_id = "TAL_test"  # 您的商户用户ID
user_client_ip = "180.76.73.167"  # 客户端设备公网IP
app_secret = "GVAxSbufDgXWxRnDSGBYrJejAhBajXDA"  # 您的app_secret


# 构造签名字符串，注意参数顺序和没有额外空格
signature_string = f"app_secret={app_secret}&appid={appid}&timestamp={timestamp}&user_client_ip={user_client_ip}&user_id={user_id}"
#signature_string = f"{app_secret}{appid}{timestamp}{user_client_ip}{user_id}"

# 使用MD5算法计算签名
request_sign = hashlib.md5(signature_string.encode('utf-8')).hexdigest()

print(f"Corrected signature: {request_sign}")

# url = "http://ginger-trial.api.cloud.ssapi.cn/auth/authorize"
# url = "http://api.cloud.ssapi.cn/auth/authorize"

url = "https://gate-01.api.cloud.ssapi.cn/auth/authorize"
#post
payload={'appid': 'a0007iu',
'user_id': 'TAL_test',
'timestamp': timestamp,
'user_client_ip': '180.76.73.167',
'request_sign': request_sign}
files=[

]
headers = {}

def get_warrantID():
    response = requests.request("POST", url, headers=headers, data=payload, files=files)
    print(response.text)
    res = response.json()
    if res.get('code') ==430008 :
        new_request_sign = res['message'].split(' sign=')[1].split(' posted_sign=')[0]
        print(f"Extracted 'sign' value: {new_request_sign}")
        payload["request_sign"] = new_request_sign
        response = requests.request("POST", url, headers=headers, data=payload, files=files)
        res = response.json()
        print(response.text)
        if res.get('code') == 0:
            warrant_id = res.get('data').get('warrant_id')
        else:
            print("再次请求出错，请检查")
    if res.get('code') == 0:
        warrant_id = res.get('data').get('warrant_id')
        print(res)
    if res.get('code') != 430008 and res.get('code') != 0:
        print("请求出错，请检查")
    print("有效id",warrant_id)
    return warrant_id

if __name__ == '__main__':
    get_warrantID()