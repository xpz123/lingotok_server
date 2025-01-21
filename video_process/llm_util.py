import requests
import json
from openai import OpenAI

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
    url = "http://ai-service-test.tal.com/openai-compatible/v1/chat/completions"
    headers = {
        "Content-Type": "application/json",
        "api-key": "1000080817:9d63da093db44efb2668e263b0d1f06b"
    }
    data = {
        "model": "doubao-pro-128k",
        "messages": [
            {
                "role": "user",
                "content": prompt
            }
        ]
    }
    response = requests.post(url, headers=headers, data=json.dumps(data))
    return response.json()
    # print(response.json())

def call_gpt4o(prompt):
    # url = "http://ai-service-test.tal.com/openai-compatible/v1/chat/completions"
    url= "http://ai-service.tal.com/openai-compatible/v1/chat/completions"
    headers = {
        "Content-Type": "application/json",
        "api-key": "1000080777:97e29110ef8758e097f4c41cd67f19d6"
    }
    data = {
        "model": "gpt-4o",
        "messages": [
            {
                "role": "user",
                "content": prompt
            }
        ]
    }
    response = requests.post(url, headers=headers, data=json.dumps(data))
    return response.json()

if __name__ == "__main__":
    # pass
    import json
    data ={"question": "森林中弥漫的清新气息是大自然赋予的独特__？", "options": ["A. 纹理", "B. 质地", "C. 韵味", "D. 馈赠"], "answer": "D", "explanation": "森林中弥漫的清新气息是大自然给予的，‘纹理’和‘质地’通常用来形容物体的表面特性，‘韵味’侧重于表达某种含蓄的意味，都不合适，而‘馈赠’能准确表达大自然给予的意思，所以应选 D 选项‘馈赠’。"}
    json_str = json.dumps(data,  ensure_ascii=False)
    # print (json_str)
    resp = (call_doubao_pro_128k("以下是一个中文句子，“荷花 全身 上下 所 积蓄 的 夏日 能量”，如果我遮挡其中“能量”这个词之后，将该句子变成一个选择题目，其中“能量“是正确选项，而其他词则是和“能量” 不想进并且也不符合语法语义的词。请注意！给我的结果需要按照如下的json格式: {}".format(json_str)))
    # resp = (call_gpt4o("以下是一个中文句子，“荷花 全身 上下 所 积蓄 的 夏日 能量”，如果我遮挡其中“能量”这个词之后，将该句子变成一个选择题目，其中“能量“是正确选项，而其他词则是和“能量” 不想进并且也不符合语法语义的词。请注意！给我的结果需要按照如下的json格式: {}".format(json_str)))
    print (resp)


