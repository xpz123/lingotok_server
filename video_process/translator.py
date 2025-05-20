import json
import pysrt
from volcengine.ApiInfo import ApiInfo
from volcengine.Credentials import Credentials
from volcengine.ServiceInfo import ServiceInfo
from volcengine.base.Service import Service
from llm_util import call_doubao_pro_15_32k, call_doubao_pro_15_32k_parallel, call_gpt4o, call_api_parallel, call_claude_3_7
import os
from tqdm import tqdm
import pandas as pd
from pypinyin import pinyin
from tenacity import retry, stop_after_attempt, wait_fixed
from concurrent.futures import ThreadPoolExecutor

def filter_bad_ensrt_files(csv_file, out_csv_file):
    df = pd.read_csv(csv_file)
    translator = Translator()
    bad_video_ids = []
    for i in range(df.shape[0]):
        ensrt_file = df.iloc[i]["en_srt"]
        if translator.detect_bad_ensrt_file(ensrt_file):
            bad_video_ids.append(df.iloc[i]["video_id"])
    
    # 从原始DataFrame中过滤出有问题的记录
    bad_df = df[df['video_id'].isin(bad_video_ids)]
    bad_df.to_csv(out_csv_file, index=False)

def translate_srt_dir_with_context_zh2en(srt_dir, csv_file=None, refine_ar=True, refine_py=True):
    translator = Translator()
    if csv_file is None:
        for file in tqdm(os.listdir(srt_dir)):
            try:
                if file.endswith("_Chinese.srt"):
                    translator.translate_zhsrt2ensrt_with_context(os.path.join(srt_dir, file), os.path.join(srt_dir, file.replace("_Chinese.srt", "_English.srt")))
                    if refine_ar:
                        translator.translate_zhsrt2arsrt_huoshan(os.path.join(srt_dir, file), os.path.join(srt_dir, file.replace("_Chinese.srt", "_Arabic.srt")))
                    if refine_py:
                        translator.convert_zhsrt_to_pinyinsrt(os.path.join(srt_dir, file), os.path.join(srt_dir, file.replace("_Chinese.srt", "_Pinyin.srt")))
            except Exception as e:
                print (str(e))

    else:
        df = pd.read_csv(csv_file)
        for i in tqdm(range(df.shape[0])):
            zh_srt_file = df.iloc[i]["zh_srt"]
            en_srt_file = df.iloc[i]["en_srt"]
            ar_srt_file = df.iloc[i]["ar_srt"]
            py_srt_file = df.iloc[i]["pinyin_srt"]
            translator.translate_zhsrt2ensrt_with_context(zh_srt_file, en_srt_file)
            if refine_ar:
                translator.translate_zhsrt2arsrt_huoshan(zh_srt_file, ar_srt_file)
            if refine_py:
                translator.convert_zhsrt_to_pinyinsrt(zh_srt_file, py_srt_file)

def translate_srt_dir_with_keyword_zh2en(srt_dir, csv_file=None, refine_ar=True, refine_py=True):
    translator = Translator()
    if csv_file is None:
        
        for file in tqdm(os.listdir(srt_dir)):
            if file.endswith("_Chinese.srt"):
                srt = pysrt.open(os.path.join(srt_dir, file))
                zh_word = srt[0].text
                en_word = translator.translate_zhword(zh_word)["en"]
                if en_word != "":
                    translator.translate_word_sent_zhsrt2ensrt_with_keyword(os.path.join(srt_dir, file), os.path.join(srt_dir, file.replace("_Chinese.srt", "_English.srt")), keyword=(zh_word, en_word))
    else:
        df = pd.read_csv(csv_file)
        for i in tqdm(range(df.shape[0])):
            if refine_ar:
                translator.translate_zhsrt2arsrt_huoshan(df.iloc[i]["zh_srt"], df.iloc[i]["ar_srt"])
            if refine_py:
                translator.convert_zhsrt_to_pinyinsrt(df.iloc[i]["zh_srt"], df.iloc[i]["pinyin_srt"])
            # title = df.iloc[i]["title"]
            # zh_word = title.split("_")[-1].strip().split("（")[0]
            # en_word = translator.translate_zhword(zh_word)["en"]
            # if en_word == "":
            #     en_word = translate_text2ar([zh_word], "en")[0]["Translation"]
            # zh_srt_file = df.iloc[i]["zh_srt"]
            # en_srt_file = df.iloc[i]["en_srt"]
            # translator.translate_word_sent_zhsrt2ensrt_with_keyword(zh_srt_file, en_srt_file, keyword=(zh_word, en_word))

def translate_srt_dir_with_char_zh2en(srt_dir, csv_file=None, refine_ar=True, refine_py=True):
    translator = Translator()
    if csv_file is None:
        for file in tqdm(os.listdir(srt_dir)):
            if file.endswith("_Chinese.srt"):
                translator.translate_char_zhsrt2ensrt(os.path.join(srt_dir, file), os.path.join(srt_dir, file.replace("_Chinese.srt", "_English.srt")))
                if refine_ar:
                    translator.translate_char_zhsrt2arsrt(os.path.join(srt_dir, file), os.path.join(srt_dir, file.replace("_Chinese.srt", "_Arabic.srt")))
                if refine_py:
                    translator.convert_zhsrt_to_pinyinsrt(os.path.join(srt_dir, file), os.path.join(srt_dir, file.replace("_Chinese.srt", "_Pinyin.srt")))
    else:
        df = pd.read_csv(csv_file)
        for i in tqdm(range(df.shape[0])):
            zh_srt_file = df.iloc[i]["zh_srt"]
            en_srt_file = df.iloc[i]["en_srt"]
            ar_srt_file = df.iloc[i]["ar_srt"]
            py_srt_file = df.iloc[i]["pinyin_srt"]
            translator.translate_char_zhsrt2ensrt(zh_srt_file, en_srt_file)
            if refine_ar:
                translator.translate_char_zhsrt2arsrt(zh_srt_file, ar_srt_file)
            if refine_py:
                translator.convert_zhsrt_to_pinyinsrt(zh_srt_file, py_srt_file)


def translate_text2ar(text_list, target_lang):
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

    def chunk_list(lst, chunk_size):
        return [lst[i:i + chunk_size] for i in range(0, len(lst), chunk_size)]

    chunk_text_list = chunk_list(text_list, 15)
    translated_text_list = []
    for item in chunk_text_list:
        body = {
            'TargetLanguage': target_lang,
            'TextList': item,
        }
        res = service.json('translate', {}, json.dumps(body))
        translated_text_list += json.loads(res)["TranslationList"]

    return translated_text_list

class Translator:
    def __init__(self):
        self.hsk_word_dict = {}
        df = pd.read_csv("hsk_dictionary/HSK_zh_en_ar.csv")
        for i in range(df.shape[0]):
            zh_word = df.iloc[i]["单词"].split("（")[0].strip()
            en_word = df.iloc[i]["English"] if pd.notna(df.iloc[i]["English"]) else ""
            ar_word = df.iloc[i]["Arabic"] if pd.notna(df.iloc[i]["Arabic"]) else ""
            self.hsk_word_dict[zh_word] = {"en": en_word, "ar": ar_word}
    
    def post_process_zh2en(self, zh_text_list, en_text_list):
        for i in range(len(zh_text_list)):
            # 检查文本是否包含中文字符
            has_chinese = False
            for char in zh_text_list[i]:
                if '\u4e00' <= char <= '\u9fff':
                    has_chinese = True
                    break
            if not has_chinese:
                en_text_list[i] = zh_text_list[i]
        
        return en_text_list
    
    def detect_bad_ensrt_file(self, ensrt_file):
        lines = open(ensrt_file).readlines()

        for l in lines:
            chinese_count = 0
            for char in l:
                if '\u4e00' <= char <= '\u9fff':  # 检查是否是中文字符
                    chinese_count += 1
                    if chinese_count >= 5:  # 如果发现一行5个以上中文字符
                        return True
        return False
    
    def translate_zhword(self, zh_word):
        # 预处理:去除空格和标点符号
        zh_word = ''.join(char for char in zh_word if '\u4e00' <= char <= '\u9fff')
        
        if zh_word in self.hsk_word_dict:
            res = self.hsk_word_dict[zh_word]
        else:
            res = {"en": "", "ar": ""}
        
        if res["en"] == "":
            prompt = "请将下面的中文单词翻译成英文：{}\n###注意只返回英文翻译，不需要任何其他内容。###".format(zh_word)
            res["en"] = call_doubao_pro_15_32k(prompt)
        
        if res["ar"] == "":
            prompt = "请将下面的中文单词翻译成阿拉伯语：{}\n###注意只返回阿拉伯语翻译，不需要任何其他内容。###".format(zh_word)
            res["ar"] = call_gpt4o(prompt)
        
        return res
    
    def translate_with_llm_zh2en_with_keywords(self, text_list, keywords=None):
        if not keywords is None:
            assert len(text_list) == len(keywords)
        prompts = []
        for i in range(len(text_list)):
            if keywords is not None:
                keywords_str = "###同时注意，" + keywords[i][0] + "这个词要用'{}'来表示###".format(keywords[i][1])
                prompt = "###请将下面的中文句子或词翻译成英文：{}\n###{}。\n###注意只返回英文翻译，不需要任何其他内容。###".format(keywords_str, text_list[i])
            else:
                prompt = "###请将下面的中文句子或词翻译成英文：{}。###\n###注意只返回英文翻译即可不需要任何其他内容。###".format(text_list[i])
            prompts.append(prompt)
        results = call_doubao_pro_15_32k_parallel(prompts)
        return results
    
    def translate_with_llm_zh2en_with_contexts(self, text_list, contexts=None):
        if not contexts is None:
            assert len(text_list) == len(contexts)
        prompts = []
        for i in range(len(text_list)):
            prompt = "###请结合上文：\"{}\"\n###将下面的中文翻译成英文:{}\n###注意只翻译本句，不要翻译上文，并且不要返回除翻译外的任何其他内容!同时为了能更好理解，可以引用一些中文偏旁或字进行翻译，但是一定不能出现过多的中文！###\n当句子无法翻译成英文时，保留原始文本即可不要输出其他内容！###".format(contexts[i], text_list[i])
            prompts.append(prompt)
        results = call_doubao_pro_15_32k_parallel(prompts)
        return results
    
    def translate_with_llm_zh2ar_with_contexts(self, text_list, contexts=None):
        if not contexts is None:
            assert len(text_list) == len(contexts)
        prompts = []
        for i in range(len(text_list)):
            # prompt = "###请结合上文：\"{}\"\n###将下面的中文翻译成阿拉伯语:{}\n###注意只翻译本句，不要翻译上文，并且不要返回除翻译外的任何其他内容!同时为了能更好理解，可以引用一些中文偏旁或字进行翻译，但是一定不能出现过多的中文！###\n当句子无法翻译成阿拉伯语时，保留原始文本即可不要输出其他内容！###".format(contexts[i], text_list[i])
            prompt = "##请结合上文：\"{}\"\n##将下面的中文翻译成阿拉伯语:{}\n###注意只翻译本句，不要翻译上文，并且不要返回除翻译外的任何其他内容!".format(contexts[i], text_list[i])
            prompts.append(prompt)

        # results = call_api_parallel(call_gpt4o, prompts)
        results = call_api_parallel(call_claude_3_7, prompts)
        return results
    
    # 用于翻译：每一行都是单个单词/汉字的中文字幕到英文字幕
    def translate_char_zhsrt2ensrt(self, zh_srt_file_path, en_srt_file_path):
        zh_srt = pysrt.open(zh_srt_file_path)
        en_srt_fw = open(en_srt_file_path, "w")
        for sub in zh_srt:
            en_word = self.translate_zhword(sub.text)["en"]
            if en_word != "":
                en_srt_fw.write("{}\n{} --> {}\n{}\n\n".format(sub.index, sub.start, sub.end, en_word))
            else:
                en_word = translate_text2ar([sub.text], "en")[0]["Translation"]
                en_srt_fw.write("{}\n{} --> {}\n{}\n\n".format(sub.index, sub.start, sub.end, en_word))
        en_srt_fw.close()
    

    # 用于翻译：每一行都是单个单词/汉字的中文字幕到阿语字幕
    def translate_char_zhsrt2arsrt(self, zh_srt_file_path, ar_srt_file_path):
        zh_srt = pysrt.open(zh_srt_file_path)
        ar_srt_fw = open(ar_srt_file_path, "w")
        for sub in zh_srt:
            ar_word = self.translate_zhword(sub.text)["ar"]
            if ar_word != "":
                ar_srt_fw.write("{}\n{} --> {}\n{}\n\n".format(sub.index, sub.start, sub.end, ar_word))
            else:
                ar_word = translate_text2ar([sub.text], "ar")[0]["Translation"]
                ar_srt_fw.write("{}\n{} --> {}\n{}\n\n".format(sub.index, sub.start, sub.end, ar_word))
        ar_srt_fw.close()
        
    # 用于翻译：单词 单词 例句 例句形式的中文字幕
    def translate_word_sent_zhsrt2ensrt_with_keyword(self, zh_srt_file_path, en_srt_file_path, keyword=None):
        zh_srt = pysrt.open(zh_srt_file_path)
        en_srt_fw = open(en_srt_file_path, "w")
        text_list = []
        en_text_list = []
        for sub in zh_srt:
            if sub.text.strip() == keyword[0]:
                en_text_list.append(keyword[1])
            else:
                text_list.append(sub.text)
        keywords = [keyword] * len(text_list)
        en_text_list += self.translate_with_llm_zh2en_with_keywords(text_list, keywords)
        # assert len(text_list) == len(en_text_list)
        for i in range(len(zh_srt)):
            en_srt_fw.write("{}\n{} --> {}\n{}\n\n".format(zh_srt[i].index, zh_srt[i].start, zh_srt[i].end, en_text_list[i]))
        en_srt_fw.close()
    
    @retry(stop=stop_after_attempt(3), wait=wait_fixed(3))
    def translate_zhsrt2ensrt_with_context(self, zh_srt_file_path, en_srt_file_path, is_ar=False):
        zh_srt = pysrt.open(zh_srt_file_path)
        en_srt_fw = open(en_srt_file_path, "w")
        context_len = 3
        text_list = []
        contexts = []
        for sub in zh_srt:
            text_list.append(sub.text)
            contexts.append("，".join(text_list[-context_len-1:-1]))
        if is_ar:
            en_text_list = self.translate_with_llm_zh2ar_with_contexts(text_list, contexts)
        else:
            en_text_list = self.translate_with_llm_zh2en_with_contexts(text_list, contexts)
            en_text_list = self.post_process_zh2en(text_list, en_text_list)
        assert len(text_list) == len(en_text_list)
        for i in range(len(zh_srt)):
            if zh_srt[i].text.strip() == "":
                en_srt_fw.write("{}\n{} --> {}\n{}\n\n".format(zh_srt[i].index, zh_srt[i].start, zh_srt[i].end, ""))
            else:
                en_srt_fw.write("{}\n{} --> {}\n{}\n\n".format(zh_srt[i].index, zh_srt[i].start, zh_srt[i].end, en_text_list[i].split("\n")[0]))
        en_srt_fw.close()

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(3))
    def translate_zhsrt2arsrt_huoshan(self, zh_srt_file_path, ar_srt_file_path):
        zh_srt = pysrt.open(zh_srt_file_path)
        ar_srt_fw = open(ar_srt_file_path, "w")
        text_list = []
        for sub in zh_srt:
            text_list.append(sub.text)
        ar_text_list = translate_text2ar(text_list, "ar")
        for i in range(len(zh_srt)):
            ar_srt_fw.write("{}\n{} --> {}\n{}\n\n".format(zh_srt[i].index, zh_srt[i].start, zh_srt[i].end, ar_text_list[i]["Translation"]))
        ar_srt_fw.close()

    def convert_zhsrt_to_pinyinsrt(self, zh_srt_file, pinyin_srt_file):
        zh_srt = pysrt.open(zh_srt_file)
        pinyin_srt_fw = open(pinyin_srt_file, "w", encoding="utf-8")
        for sub in zh_srt:
            text = sub.text
            try:
                text = text.replace("#", "")
                pinyin_list = pinyin(text)
                replace_list = []
                for item in pinyin_list:
                    if text.find(item[0]) != -1:
                        replace_list.append(item[0])
                        text = text.replace(item[0], "#", 1)
                replace_idx = 0
                pinyin_text = ""
                for idx, c in enumerate(text):
                    if c == "#":
                        pinyin_text += replace_list[replace_idx]
                        replace_idx += 1
                    else:
                        pinyin_text += "{}({})".format(c, pinyin_list[idx][0])
            except Exception as e:
                print (str(e))
                pinyin_text = text
            pinyin_srt_fw.write("{}\n{} --> {}\n{}\n\n".format(sub.index, sub.start, sub.end, pinyin_text))

if __name__ == "__main__":
    translator = Translator()
    print (translator.translate_zhword("湖"))
    # translator.translate_zhsrt2ensrt_with_context("/Users/tal/work/lingtok_server/video_process/hw/videos/短剧/0110-8233-被偷走爱的那十年--君-Easylove43-01/srt_dir/0a8f8619-fffd-42c3-a4c8-af88bc578688_Chinese.srt", "tmp.srt", is_ar=True)
    # translate_srt_dir_with_char_zh2en("/Users/tal/work/lingtok_server/video_process/视频内容库查漏补缺-已修改-428/汉字基础/1.从象形字（如日、月）入手，理解汉字构造逻辑，降低记忆难度/srt_dir")
    import pdb; pdb.set_trace()
    # translate_srt_dir_with_keyword_zh2en("", csv_file="/Users/tal/work/lingtok_server/video_process/HSK_video/hsk_video/600words_mp4/video_info_video.csv")
    # translate_srt_dir_with_context_zh2en("/Users/tal/work/lingtok_server/video_process/悟空识字1200/悟空识字1200/srt_dir", refine_ar=True, refine_py=True)
    # translate_srt_dir_with_context_zh2en("/Users/tal/work/lingtok_server/video_process/已修改/拼音与声调/5. 声母练习/srt_dir", refine_ar=True, refine_py=True)
    # translate_srt_dir_with_context_zh2en("/Users/tal/work/lingtok_server/video_process/hw/videos/短剧/0110-8233-被偷走爱的那十年--君-Easylove43-01/srt_dir", refine_ar=True, refine_py=True)
    # translate_srt_dir_with_context_zh2en("/Users/tal/work/lingtok_server/video_process/hw/videos/短剧/8147-我被套路撞了N下腰-06-01-君-富士山下/srt_dir", refine_ar=True, refine_py=True)
    # translate_srt_dir_with_context_zh2en("/Users/tal/work/lingtok_server/video_process/已修改/拼音与声调/4. 拼音视频带声调/srt_dir", refine_ar=True, refine_py=True)
    # translate_srt_dir_with_context_zh2en("/Users/tal/work/lingtok_server/video_process/已修改/拼音与声调/2. 声母歌/srt_dir", refine_ar=True, refine_py=True)
    # translate_srt_dir_with_context_zh2en("/Users/tal/work/lingtok_server/video_process/已修改/拼音与声调/3. 拼音规则（高级拼音）/srt_dir", refine_ar=True, refine_py=False)
    # translate_srt_dir_with_context_zh2en("/Users/tal/work/lingtok_server/video_process/字有道理/srt_dir", refine_ar=True, refine_py=True)
    # translate_srt_dir_with_context_zh2en("/Users/tal/work/lingtok_server/video_process/已修改/拼音与声调/3. 拼音规则（高级拼音）/srt_dir")
    # translate_srt_dir_with_context_zh2en("", csv_file="/Users/tal/work/lingtok_server/video_process/悟空识字1200/悟空识字1200/video_info_badensrt.csv")
    # translator = Translator()
    # translator.convert_zhsrt_to_pinyinsrt("/Users/tal/work/lingtok_server/video_process/字有道理/srt_dir/0a17a22d-e16d-45b3-83fe-8221d22a0f2d_Chinese.srt", "tmp.srt")
    # translator.translate_zhsrt2ensrt_with_context("/Users/tal/work/lingtok_server/video_process/hw/videos/视频内容库查漏补缺-已修改-字幕-56已人工/3.拼音规则/srt_dir/3239e1ad-6d05-4dc7-a2f4-8e50132c9ccf_Chinese.srt", "tmp.srt")
    # translator.translate_zhsrt2arsrt_huoshan("/Users/tal/work/lingtok_server/video_process/hw/videos/视频内容库查漏补缺-已修改-字幕-56已人工/3.拼音规则/srt_dir/3239e1ad-6d05-4dc7-a2f4-8e50132c9ccf_Chinese.srt", "tmp.srt")

    # translator.translate_zhsrt2ensrt_with_context("/Users/tal/work/lingtok_server/video_process/悟空识字1200/悟空识字1200/srt_dir/e0c2b60d-adf2-4e04-961c-a4b624488c40_Chinese.srt", "tmp.srt")
    # filter_bad_ensrt_files("/Users/tal/work/lingtok_server/video_process/悟空识字1200/悟空识字1200/video_info.csv", "/Users/tal/work/lingtok_server/video_process/悟空识字1200/悟空识字1200/video_info_badensrt.csv")

    # print (translator.detect_bad_ensrt_file("/Users/tal/work/lingtok_server/video_process/悟空识字1200/悟空识字1200/srt_dir/faab5011-eae7-468c-8710-bb72ade4c6cb_English.srt"))
    # translate_srt_dir_with_context_zh2en("/Users/tal/work/lingtok_server/video_process/悟空识字1200/悟空识字1200_new/srt_dir")
    # translate_srt_dir_with_keyword_zh2en("/Users/tal/work/lingtok_server/video_process/沙特女子Demo/初级汉语/词汇练习/srt_dir")
    # translator = Translator()
    # translator.translate_zhsrt2ensrt_with_context("/Users/tal/work/lingtok_server/video_process/悟空识字1200/悟空识字1200/srt_dir/0b8d8fb5-a1a0-42f4-815a-2183c230fd28_Chinese.srt", "tmp.srt")
    # print (translator.translate_with_llm_zh2en_with_contexts(["左边三点水", "湖里都是水", "其余部分就像西湖的美景"], ["湖字是左中右结构", "湖字是左中右结构, 左边三点水", "湖字是左中右结构, 左边三点水, 湖里都是水"]))
    # translator.translate_word_sent_zhsrt2ensrt("/Users/tal/work/lingtok_server/video_process/沙特女子Demo/初级汉语/词汇练习/srt_dir/944_适合_Chinese.srt", "tmp.srt", keyword=("适合", "fit"))

    # text_list = [""]
    # keywords = [("合适", "fit"), ("")]
    # text_list = ['我爸是离了婚才过来追的你', '堂堂正正把你娶进家门', '怎么就见不得人呀', '这么多年过去了', '我奶奶连门都不让我们进', '还口口声声骂你是小三', '凭什么呀', '爸嗯妈', '我敬你们一杯', '祝你们来年嗯', '平平安安', '快快乐乐', '事事顺心', '尤其是老爸', '多赚银子少喝酒', '听到没有', '嗯女儿的话老爸你一定听', '来走着来', '新年快乐', '哎新年快乐', '新年快乐', '新年快乐', '哎呀今年啊', '咱们潇潇不得了', '你看自己开了公司', '还做了老总', '把公司打理得井井有条', '你这大老板', '是不是应该给小老板一点奖励啊', '嗯早就准备好了', '明天一早给', '一早给潇潇', '来', '爸爸也敬你一杯', '龙生龙凤生凤', '我生的女儿像我', '别不会就会做生意', '那是走着', '是啊我这妹妹本事可大', '多大能耐', '刚回来一年', '七拐八绕的朋友', '乱七八糟的狐朋狗友', '能绕着魔都拉一圈了', '什么事办不成啊', '是吧妹妹', '那我也比不上哥哥你呀', '你招惹的女孩都能排成一个', '排了怎么样', '操作心嫂怎么样', '好了好了', '大过年的掐什么掐', '接电话一定是奶奶的电话', '喂啊奶奶', '我就知道是您', '是啊过年了', '孙子给您拜个年', '祝您呃福寿绵长', '新春大吉', '身体健健康康的啊', '是我们明天就回去', '那您也知道嘛', '这两年情况都比较', '去吧去什么去啊', '曲家都不认识我们俩', '我还得假扮假设给他拜年', '他认不认识他的事', '你认不认他是你的事', '你看你爸就得认他', '去吧啊你等着啊', '你孙女跟你说话', '奶奶我是曲潇潇啊', '祝你新年快乐', '美国买的那个保养品没拿吧', '回来这么早啊', '不用送咱了', '爸爸过两天就回来', '那个孩子', '别人送的', '那个发酵火腿和老山参一块拿上啊', '够了够了', '平时也没少往家里送东西啊', '拿这么多妈肯定吃不了', '妈吃不吃是他的事', '我这个媳妇得做好', '要不然你妈还不定怎么说我呢', '那行了我知道你受委屈了', '我妈她她', '她就那么个臭脾气', '认定了我可以下来连接她吗', '她一个乡下女人没见识', '你何必跟她计较啊', '这么多年都这么', '过来就打声', '为了我再忍忍再忍忍', '哎呦潇潇啊', '我和你哥去两天就回', '这两天在家里好好陪着妈妈', '听到了没有', '你这个话头说吧', '要什么雪莲袄', '包包限量的', '刚看中一个俩', '好好好买买买买', '爸怎么了', '爸啊你快点', '磨磨蹭蹭的', '你们着什么急啊', '我的车比你们车快', '快点干嘛呢', '坐哪呢差劲样', '哎呀有些人机关算尽', '可是就没想到连回家的资格都没有', '不就是回趟乡下吗', '谁稀罕啊', '好走不送', '呵呵怎么了', '看你憋屈那样', '难受妈', '我爸是离了婚才过来追的你', '堂堂正正把你娶进家门', '怎么就见不得人呀', '这么多年过去了', '我奶奶连门都不让我们进', '还口口声声骂你是小三', '凭什么呀', '你爸不说了吗', '他刚到上海那几年', '都是那一边在照顾你奶奶', '后来虽然离了婚', '可你奶奶那死脑筋就是转不过弯来', '只认那边是儿媳妇', '有什么办法', '那你不能这么包子呀', '你帮我爸这么多年', '他假装没看见我这么大个孙女他', '假装没看见他说起笑', '醒来也没见他手软啊', '拿着我们家的东西去补贴那边', '什么意思', '算了他毕竟是你奶奶', '再说了大过年的', '我也不想让你爸为难你', '不去就不去呗', '真让你跟你爸回老家', '你未必待的惯', '不是吧你也玩自我安慰这一套', '这么心慈手软', '我可看不下去啊', '不说了不说了', '越说越烦', '不吃了回房间去', '哎徐潇潇', '你干什么呢', '嗨安迪姐', '你在干什么呀', '安迪姐', '你去那个普吉岛好不好玩啊', '我听说那海可漂亮了', '我都不知道我什么时候才能去', '邱莹莹你是猪啊', '别吃了过年不减肥', '年后徒伤悲', '可千万别听樊大姐的', '她有王帅哥', '你可什么都没', '普吉岛有什么好玩的', '要去就去大溪地', '那才叫一个美呢', '徐小胖', '你说为什么人家过年家里热热闹闹', '我们家就这么冷清', '问你呢说话呀', '不是吧那七门九溜', '大过年的连句新年问候都没有']
    # ar_text_list = translate_text2ar(text_list, "ar")
    # assert len(text_list) == len(ar_text_list)