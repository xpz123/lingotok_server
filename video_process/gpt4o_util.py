from openai import OpenAI
from openai import OpenAIError
import os
import time
import json
import random
from tqdm import tqdm


# system_prompt = """假设你是一个方程检测助手，请你完成下面的要求：
# *************************************************************************************************************************************************
# step 1: 请你检测出text中出现的所有解方程这一过程的【起始的等式】、【它的中间变换过程】、以及【它对应的解】。
# 注意：
# 1、检测出的解方程内容一定包含一个等式。
# 2、同一个解方程过程一定要包含相同的变量，否则不能属于同一个解方程过程。
# 3、你只需要检测出text中解方程这一过程的【起始的等式】、【它的中间变换过程】、以及【它对应的解】，不需要对它进行求解，千万不要对它进行求解。
# 4、输出时不要改变原text内容，要求原封不动的输出。
# 5、注意只需要检测方程，不要检测不等式。
# 6、请确保【方程的起始等式】【方程的中间过程】【方程的解】它们之间是相关的，即它们有共同的变量，否则它们不属于同一个解方程过程。
# *************************************************************************************************************************************************
# step 2: 请你对找出的式子进行转换，将latex符号转换为更通用的符号，转换后不能再出现latex符号。具体规则如下：
# %s
# *************************************************************************************************************************************************
# step 3: 请你用json的格式输出。
# 格式：{"方程的起始等式": ["-a-4-4=0"], "转写后方程的起始等式":["-a-4-4=0"], "方程的中间过程":["-a-8=0"], "转写后方程的中间过程":["-a-8=0"], "方程的解":["a=-8"], "转写后方程的解":["a=-8"]}
# 注意：
# 1、如果某个解方程过程检测不出它对应的解，则输出""。
# 2、一个解方程的过程是一个整体，用一个dict表示。
# 3、多元解方程算是同一个解方程的过程，但是需要用List装起来。
# *************************************************************************************************************************************************
# 下面是一些例子，给出了text和输出，你可以作为参考。输出过程中你可以写出你的思考过程，并在最后写出json格式的答案。
# *******************************************************************************************************************************************************************************************
# """

# def process_multi_round(conversation, system_prompt):
#     # 分割对话为 'human' 和 'assistant' 的部分
#     parts = conversation.replace("\\n","\n").split('<|im_end|>\n<|im_start|>assistant\n')
#     question = parts[0].strip()
#     parts = parts[1:]
    
#     # 初始化data字典
#     data = {
#         "messages": [
#             {
#                 "role": "system",
#                 "content": system_prompt
#             },
#             {
#                 "role": "user",
#                 "content": question
#             }
#         ]
#     }
    
#     # 遍历分割后的对话部分
#     for part in parts:
#         # print(part.replace("\n","\\n"))
#         try:
#             current_response = part.split("<|im_end|>\n<|im_start|>user\n")[0].strip()
#             next_question = part.split("<|im_end|>\n<|im_start|>user\n")[1].strip()
#         except:
#             continue
#         if current_response != "" and next_question != "":
#             data["messages"].append({
#                 "role": "assistant",
#                 "content": current_response
#             })
#             data["messages"].append({
#                 "role": "user",
#                 "content": next_question
#             })
#         else:
#             print("Error! current_response or next_question is empty!")
#             print(part)
#             exit(1)
#     return data
# def get_system_prompt(system_prompt):
#     system_prompt = system_prompt % rewrite_rule
#     return system_prompt


# def get_prompt(system_prompt):
#     equation_path = "/mnt/pfs/jinfeng_team/PPO/chenhuixi/datasets/detection/equation/train_trans_v2.jsonl"
#     train_shots = [json.loads(_) for _ in open(equation_path)]
#     # 随机打乱
#     random.shuffle(train_shots)
#     train_shots = train_shots[:100]

#     system_prompt = get_system_prompt(system_prompt)
#     messages = [
#         {"role": "system", "content": system_prompt},
#     ]

#     for shot in train_shots:
#         message = {"role": "user", "content": f'text: {shot["analysis"]}'}
#         messages.append(message)
#         message = {"role": "assistant", "content": f'json: {shot["trans_label_post_str"]}'}
#         messages.append(message)
#     return messages


class Model:
    def __init__(self):
        self.client = OpenAI(
            api_key="1000080777:97e29110ef8758e097f4c41cd67f19d6",  # 如果您没有配置环境变量，请在此处用您的API Key进行替换
            base_url="http://ai-service.tal.com/openai-compatible/v1",  # 填写DashScope服务的base_url
        )
        self.max_retries = 2
        self.retry_delay = 5

    def request(self, prompt, system_prompt=""):
        messages = {
            "messages": [
                {
                    "role": "system",
                    "content": system_prompt
                },
                {
                    "role": "user",
                    "content": prompt
                }
            ]
        }
        retries = 0
        while retries < self.max_retries:
            try:
                completion = self.client.chat.completions.create(
                    model="gpt-4o",
                    messages=messages,
                    temperature=0.0
                )
                response = completion.model_dump_json()
                response = json.loads(response)
                return response["choices"][0]["message"]["content"]
            except OpenAIError as e:
                print(f"请求失败: {e}. 尝试重新连接...")
                retries += 1
                if retries < self.max_retries:
                    time.sleep(self.retry_delay)  # 等待一段时间后重试
                else:
                    print("达到最大重试次数，放弃请求。")
                    return "error"

    # def get_response(self):
    #     output_file = "/mnt/pfs/jinfeng_team/SFT/luohaixia/workspace/b_multi_turn/data/test/multi_test/all_v2_gpt4o_response.1.jsonl"
    #     test_path = "/mnt/pfs/jinfeng_team/SFT/luohaixia/workspace/b_multi_turn/data/test/multi_test/all_v2_for_qwen.1.jsonl"
    #     test_datas = [json.loads(_) for _ in open(test_path)]
    #     for test_data in tqdm(test_datas):
    #         messages = process_multi_round(test_data["prompt"],"")["messages"]
    #         response = self.request(messages)
    #         print("**************************************************xw****************************************")
    
    #         test_data["gpt4o_response"] = response
    #         with open(output_file, "a+", encoding='utf-8') as fp:
    #             fp.write(json.dumps(test_data, ensure_ascii=False) + "\n")


if __name__ == '__main__':
    pass
    # model = Model()
    # model.request("")
