# -*- coding: utf-8 -*-
# 发音数据传输接口附带初始化过程（https://cloud.tencent.com/document/product/884/32605）
import json
import os
import uuid
import base64

from tencentcloud.common import credential
from tencentcloud.common.exception.tencent_cloud_sdk_exception import TencentCloudSDKException
from tencentcloud.soe.v20180724 import soe_client, models


# 实例化一个认证对象，入参需要传入腾讯云账户 SecretId 和 SecretKey，此处还需注意密钥对的保密
# 代码泄露可能会导致 SecretId 和 SecretKey 泄露，并威胁账号下所有资源的安全性。以下代码示例仅供参考，建议采用更安全的方式来使用密钥，请参见：https://cloud.tencent.com/document/product/1278/85305
# 密钥可前往官网控制台 https://console.cloud.tencent.com/cam/capi 进行获取

def call_tencent_zh_mdd(filename, ref_text):
    cred = credential.Credential("AKIDITYQ5XnqhMPFxSkpiPtmbGk9Xpxwra7a", "gAUwMb5VyLiw5A7udElAWkppkXFsPn04")
    # 实例化soe的client对象
    client = soe_client.SoeClient(cred, "")

    # 实例化发音数据传输接口请求对象
    req = models.TransmitOralProcessWithInitRequest()

    # 请求参数赋值
    SessionId = str(uuid.uuid1())  # 使用uuid作为请求SessionId
    # 读取音频文件，
    with open(filename, "rb") as f:
        base64_data = base64.b64encode(f.read()).decode()  # 读取音频文件byte数据转成base64格式

    params = {
        "SeqId": 1,
        "IsEnd": 1,
        "VoiceFileType": 3,
        "VoiceEncodeType": 1,
        "UserVoiceData": base64_data,
        "SessionId": SessionId,
        "RefText": "{::cmd{F_TDET=true}}" + ref_text,
        "WorkMode": 1,
        "EvalMode": 1,
        "ServerType": 1,
        "ScoreCoeff": 1.0
    }
    req.from_json_string(json.dumps(params))
    try:
        # 通过client对象调用TransmitOralProcessWithInit方法发起请求
        resp = client.TransmitOralProcessWithInit(req)
        # 输出json格式的字符串回包
        json_res = resp.to_json_string()
        print(json_res)
        return json_res

    except TencentCloudSDKException as err:
        print(err)
    
    return None

if __name__ == "__main__":
    pass
    # call_tencent_zh_mdd("tmp_audio/nihao.mp3", "你好，你好")
