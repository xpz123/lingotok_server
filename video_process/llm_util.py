import requests
import json
from openai import OpenAI
from volcenginesdkarkruntime import Ark
from concurrent.futures import ThreadPoolExecutor
from typing import List, Any
import base64
from tenacity import retry, stop_after_attempt, wait_fixed

doubao_client = Ark(
    # 此为默认路径，您可根据业务所在地域进行配置
    base_url="https://ark.cn-beijing.volces.com/api/v3",
    # 从环境变量中获取您的 API Key。此为默认方式，您可根据需要进行修改
    api_key="09351b42-cf23-4549-8955-d338e2dfe9b6",
)

def call_doubao_pro_32k(prompt):
    client = OpenAI(
    api_key = "09351b42-cf23-4549-8955-d338e2dfe9b6",
    base_url = "https://ark.cn-beijing.volces.com/api/v3",
    )

    # Non-streaming:
    # print("----- standard request -----")
    completion = client.chat.completions.create(
        model = "ep-20250103165151-lsh9k",  # your model endpoint ID
        messages = [
            {"role": "system", "content": ""},
            {"role": "user", "content": prompt},
        ],
    )
    return (completion.choices[0].message.content)

def call_doubao_pro_128k(prompt):
    completion = doubao_client.chat.completions.create(
   # 指定您创建的方舟推理接入点 ID，此处已帮您修改为您的推理接入点 ID
    model="doubao-1-5-pro-32k-250115",
    messages=[
        {"role": "system", "content": "你是人工智能翻译专家"},
        {"role": "user", "content": prompt},
    ],)
    print(completion.choices[0].message.content)

@retry(stop=stop_after_attempt(3), wait=wait_fixed(3))
def call_doubao_pro_15_32k(prompt):
    try:
        completion = doubao_client.chat.completions.create(
    # 指定您创建的方舟推理接入点 ID，此处已帮您修改为您的推理接入点 ID
        model="doubao-1-5-pro-32k-250115",
        messages=[
            {"role": "system", "content": "你是人工智能助手."},
            {"role": "user", "content": prompt},
        ],)
    # print(completion.choices[0].message.content)
        return completion.choices[0].message.content
    except Exception as e:
        print (e)
        return ""

    # print(response.json())

def call_doubao_vl_1_5(prompt, image_path):
    # 读取并编码本地图片
    with open(image_path, "rb") as image_file:
        image_base64 = base64.b64encode(image_file.read()).decode('utf-8')

    response = doubao_client.chat.completions.create(
        model="doubao-1.5-vision-pro-250328",
        messages=[
            {
                "role": "user",
                "content": [
                    {
                        "type": "image_url",
                        "image_url": {
                            "url": f"data:image/jpeg;base64,{image_base64}"
                        },
                    },
                    {"type": "text", "text": prompt},
                ],
            }
        ],
    )
    
    return response.choices[0].message.content

def call_api_parallel(func, prompts, max_workers=5):
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        results = list(executor.map(func, prompts))
    return results

def call_doubao_pro_15_32k_parallel(prompts: List[str], max_workers: int = 5) -> List[Any]:
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        results = list(executor.map(call_doubao_pro_15_32k, prompts))
    return results

if __name__ == "__main__":
    result = call_doubao_vl_1_5("你是一个中文老师，你想要向同学们介绍 人 车 公交车 出租车 晴天 太阳 等词语。分析一下这个图片，你可以通过这个视频介绍什么简单的汉字或词语？ ##请注意，只需要返回图片中最有代表性的一个词，并且只返回这个词或字，不要输出其他内容。", "/Users/tal/work/lingtok_server/video_process/自制视频/视频加文字/1/frame_0510.jpg")
    print (result)
    # print (call_gemini_2_5_pro("请把下面的中文翻译成阿拉伯语：森林中弥漫的清新气息是大自然赋予的独特"))
    # print (call_gpt4o("请把下面的中文翻译成阿拉伯语：森林中弥漫的清新气息是大自然赋予的独特"))
    # print (call_doubao_pro_15_32k("请把下面的中文翻译成英文：森林中弥漫的清新气息是大自然赋予的独特"))
    # prompts = ["请把下面的中文翻译成英文：森林中弥漫的清新气息是大自然赋予的独特", "请把下面的中文翻译成英文：森林中弥漫的清新气息是大自然赋予的独特"] * 10
    # results = call_gpto1_preview("请把下面的中文翻译成阿拉伯语：森林中弥漫的清新气息是大自然赋予的独特")
    # prompts = ["请把下面的中文翻译成阿拉伯语：森林中弥漫的清新气息是大自然赋予的独特"] * 3
    # results = call_api_parallel(call_claude_3_7, prompts)
    # print (results)
    # results = call_doubao_pro_15_32k_parallel(prompts)
    # import pdb;pdb.set_trace()


