#! /usr/bin/env python3
# -*- coding: UTF-8 -*-
import glob
import os
import dis
import uuid
import ssl
import hmac
import base64
import requests
import datetime
import random
from hashlib import sha1
import _thread
import websocket
from functools import partial
import time
import json
import random
import hmac
from hashlib import sha1
import uuid
import json
from urllib.parse import quote
import requests
time_start = 0


application_x_www_form_urlencoded = 'application/x-www-form-urlencoded'
__request_body = "request_body"
application_json = 'application/json'


def get_signature_http(url_params, body_params, access_key_secret):
    def __generate_signature(parameters, access_key_secret):
        sorted_parameters = sorted(parameters.items(), key=lambda parameters: parameters[0])
        param_list = []
        for (k, v) in sorted_parameters:
            param_str = '{}={}'.format(k, v)
            param_list.append(param_str)
        string_to_sign = '&'.join(param_list)
        secret = access_key_secret + "&"
        h = hmac.new(secret.encode('utf-8'), string_to_sign.encode('utf-8'), sha1)
        signature = base64.b64encode(h.digest()).strip()
        signature = str(signature, encoding="utf8")
        return signature

    signature_nonce = str(uuid.uuid1())
    sign_param = {'signature_nonce': signature_nonce, __request_body: json.dumps(body_params)}
    for key in url_params.keys():
        sign_param[key] = url_params[key]
    signature = __generate_signature(sign_param, access_key_secret)
    return signature, signature_nonce




def get_signature(url_params, body_params, request_method, content_type, access_key_secret):
    def url_format_list(parameters):
        param_list = []
        for (k, v) in parameters:
            param_str = '{}={}'.format(k, v)
            param_list.append(param_str)
        string_to_sign = '&'.join(param_list)
        return string_to_sign

    def __generate_signature(parameters, access_key_secret):
        sorted_parameters = sorted(parameters.items(), key=lambda parameters: parameters[0])
        string_to_sign = url_format_list(sorted_parameters)
        secret = access_key_secret + "&"

        h = hmac.new(secret.encode('utf-8'), string_to_sign.encode('utf-8'), sha1)
        signature = base64.b64encode(h.digest()).strip()
        signature = str(signature, encoding="utf8")
        return signature

    signature_nonce = str(uuid.uuid1())

    sign_param = {
        'signature_nonce': signature_nonce
    }

    for key in url_params.keys():
        sign_param[key] = url_params[key]

    signature = __generate_signature(sign_param, access_key_secret)
    return signature, signature_nonce


def get_sign(
        access_key_id,
        access_key_secret,
        timestamp,
        url,
        url_params):
    if access_key_id is None or len(access_key_id) == 0:
        raise RuntimeError('参数access_key_id不能为空')
    if access_key_secret is None or len(access_key_secret) == 0:
        raise RuntimeError('参数access_key_secret不能为空')
    if timestamp is None or len(timestamp) == 0:
        raise RuntimeError('参数timestamp不能为空')
    if url is None or len(url) == 0:
        raise RuntimeError('参数url不能为空')
    if url_params is None:
        raise RuntimeError('参数url_params不能为空')

    url_params['access_key_id'] = access_key_id
    url_params['timestamp'] = timestamp

    signature, signature_nonce = get_signature(
        url_params,
        None,
        'GET',
        'application/json',
        access_key_secret)

    header_params = ['access_key_id:' + access_key_id, 'signature:' + signature, 'signature_nonce:' + signature_nonce,
                     'timestamp:' + timestamp]
    return header_params

def my_request(url, url_params, body_params):
    header = 'application/json'
    access_key_id = ACCESS_KEY_ID
    access_key_secret = ACCESS_KEY_SECRET
    # 获取当前时间（东8区）
    timestamp = time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime())

    url_params['access_key_id'] = access_key_id
    url_params['timestamp'] = timestamp

    signature, signature_nonce = get_signature_http(
        url_params,
        body_params,
        access_key_secret)

    url_params['signature'] = quote(signature, 'utf-8')
    url_params['signature_nonce'] = signature_nonce
    param_list = []
    for key, value in url_params.items():
        param_str = '{}={}'.format(key, value)
        param_list.append(param_str)
    string_to_sign = '&'.join(param_list)
    url = url + '?' + string_to_sign
    headers = {
        'content-type': header
    }
    response = requests.post(url, json=body_params, headers=headers)
    print(response.text)
    return json.loads(response.content.decode("utf-8"))

def url_format(parameters):
    param_list = []
    for key, value in parameters.items():
        param_str = '{}={}'.format(key, value)
        param_list.append(param_str)
    string_to_sign = '&'.join(param_list)
    return string_to_sign

t1 = time.time()
score_result = None
def on_message(d, ws, message):
    # mapping = {
    #     "1002": 20000,
    #     "2001": 300701701,
    #     "2002": 300701702,
    #     "2003": 300701704,
    #     "2004": 300701703,
    #     "2005": 300701706,
    # }
    mapping_new = {
        20000:"1002",
        300701701: "2001",
        300701702: "2002",
        300701704: "2003",
        300701703: "2004",
        300701706: "2005",
    }
    global t1
    global score_result
    tmp = json.loads(message)
    score_result = tmp.get('data')
    d['uid'] = tmp.get('requestId') if tmp.get('requestId') else d['uid']
    # print(tmp.get("data").get("total_score"))
    #print(tmp)


def on_error(ws, error):
    print("error:", error)


def on_close(ws, code, msg):
    if code != 1000:
        with open("评测一致性测试.txt", 'a') as f:
            f.write("error code:" + str(code) + '\n')
    print("### closed ###", code, msg)


def on_ping(ws, data):
    print(datetime.datetime.now(), "on ping", data)


def on_pong(ws, data):
    print("on pong")




def on_open(d, file_name, ws, ctrl_para1, ctrl_para2=None):
    def run(*args):
        cnt = 1
        ws.send(json.dumps(ctrl_para1))
        # if ctrl_para2:
        #     ws.send(json.dumps(ctrl_para2))
        with open(file_name, 'rb') as fd:
            while True:
                size = random.choices(["3200"])
                # print(size)
                content = fd.read(3200)
                # print(content)
                if not content:
                    break

                time.sleep(0.01)
                # print(content)
                # if len(content) !=3200:
                #     break
                # print(len(content))
                # content = decode_audio(content)
                # print(len(content))
                # with open("test_bug.wav","ab") as f:
                #     f.write(content)
                ws.send(content, 0x2)
                cnt += 1

        ws.send("end")

        #ws.close()
        d['end_time'] = time.time()

    try:
        _thread.start_new_thread(run, ("Thread-1", 2,))
    except:
        print("Error: unable to start thread")


def rtevl(fn,text,core_type):
    ctrl_para1 = {
        "need_url": False,
        'mime_type': "wav",
        "user_info": "用户信息",
        # "mod_id":1,
        # "need_url": False,
        "assess_ref": {
            "text": text,
            "core_type": core_type,
            # "support_recite": False,
            # "support_repeat": True,
            # "recite_rank": 9.0,
            # "support_assistant": False,
            # "support_wb": True,
            # "score_rank": 100,
            # "support_oneword": True,
            # "support_sntinfo": False,
            # "support_refine": False,
            # "is_next": True

        },
       # "control_param":{"high_score_threshold":100.0,"high_stop_low_threshold":30.0,"suffix_penal_quick":0.2,"vad_max_sec":-1.0,"vad_pause_sec":2.0,"vad_st_sil_sec":10.0}
        "control_param":{
            "vad_max_sec": -1,
            "vad_st_sil_sec": 10,
            "vad_pause_sec": 1,
            "suffix_penal_quick": 0.2,
            "high_score_threshold": 60
        }
       #  "control_param": {
       #      # 最大说话时长默认20，-1关闭
       #      "vad_max_sec": 20,
       #      # 说话结束后的暂停时间，默认2，-1关闭 [0,5]
       #      "vad_pause_sec": 2,
       #      # 前置静音时间，默认5，-1关闭 [0,10]
       #      "vad_st_sil_sec": 5,
       #      # 高分截停时长，默认-1关闭 [0,5]
       #      "suffix_penal_quick": 0.8,
       #      "high_score_threshold": 101,
       #      "high_stop_low_threshold": 30,
       #  }
    }
    st = time.time()
    # url = 'ws://gateway-bp.facethink.com/aispeech/evl-realtime/en-standard-next?'
    # access_key_id = '4786117720392704'
    # access_key_secret = '0c5501d4f1a84d508b590846542cdceb'
    # url = 'wss://gateway-test-multi.facethink.com/aispeech/evl-realtime/en-standard-next?'
    # access_key_id = "917730441397547008"
    # access_key_secret = "829aec030259463c8907e45d0df65610"
    #
    #
    # access_key_id = '4926596405003264'
    # access_key_secret = "9d5cc1da03304a67b5ae53332ad87b2d"
    # url = "ws://7fa39240f2204c76bcf50dde7a7cc4c8.apig.cn-north-4.huaweicloudapis.com/aispeech/evl-realtime/zh-standard?"
    # url = 'ws://ai.tal.com/aispeech/evl-realtime/zh-standar'
    url = "wss://openai.100tal.com/aispeech/evl-realtime/en-standard-next?"
    #url = "wss://openai.100tal.com/aispeech/evl-realtime/en-test?"
    #url = "wss://openai.100tal.com/aispeech/evl-realtime/en-test?"
    # #
    # # #url = "ws://10.29.48.10:9000?"
    access_key_id = '5021438678811648'
    access_key_secret = '59bddd3e88a44c19a2ed7232843f2727'
    url_params = {"mod": "px-1.0"}
    # url_params = {}
    url = url + url_format(url_params)
    timestamp = time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime())

    d = {"end_time": time.time(), "uid": ""}
    header_params = []
    header_params = get_sign(access_key_id, access_key_secret, timestamp, url, url_params)
    # print(header_params)
    # print(url)
    global score_result

    ws = websocket.WebSocketApp(url,
                                header=header_params,
                                on_message=partial(on_message, d),
                                on_error=on_error,
                                on_close=on_close,
                                on_ping=on_ping,
                                on_pong=on_pong
                                )
    ws.on_open = partial(on_open, d, fn, ctrl_para1=ctrl_para1)
    try:
        ws.run_forever()
    finally:
        ws.close()
        print (score_result)
        ed = time.time()
        print ("dur: {}".format(ed-st))
        return score_result
    return None



if __name__ == "__main__":
    pass
    #rtevl("new.wav","Asian people","en.snt.score")
