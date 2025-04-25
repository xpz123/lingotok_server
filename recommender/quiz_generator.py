import json
from enum import Enum
from recaller import get_redis
import pandas as pd
import re
from collections import defaultdict
import random as rd
from copy import deepcopy
from pypinyin import pinyin
import asyncio

class Language(Enum):
    pinyin = 1
    en = 2
    ar = 3
    zh = 4

class QuizType(Enum):
    invalid = 0
    single_choice = 1

class QuizGeneratingCtx:
    def __init__(self):
        self.extracted_word = None
        self.recent_video_infos = list()

class QuizGeneratingWorker:
    def __init__(self, quiz_worker_config):
        self.quiz_zh_template = {"language": Language.zh.value, "question": "", "option_list": [], "answer_list": [], "explanation": ""}
        self.quiz_en_template = {"language": Language.en.value, "question": "", "option_list": [], "answer_list": [], "explanation": ""}
        self.quiz_ar_template = {"language": Language.ar.value, "question": "", "option_list": [], "answer_list": [], "explanation": ""}
        self.quiz_template = {
            "quiz_type": QuizType.invalid.value,
            "quiz_language_list": []
        }
        self.word_level_dict, self.word_pos_level_dict, self.level_wordlist_dict, self.level_wordposlist_dict = self.parse_hsk_level(quiz_worker_config.get("hsk_zh_en_ar_path", None))
    
    def parse_hsk_level(self, hsk_csv):
        df = pd.read_csv(hsk_csv)
        word_level_dict = {}  # 词汇-等级对应
        word_pos_level_dict = {}  # 词汇-词性-等级对应
        level_wordlist_dict = defaultdict(list)
        level_wordposlist_dict = defaultdict(list)

        level_str_to_int = {"一级": 1, "二级": 2, "三级": 3, "四级": 4, "五级": 5, "六级": 6}
        
        for _, row in df.iterrows():
            word = row['单词'].strip()
            pattern = r'(.*?)\s*(?:（(.*?)）)?\s*（([一二三四五六]级)）'
            match = re.match(pattern, word)
            if match:
                pure_word = match.group(1).strip()
                pos = match.group(2)  # 词性可能为None
                level = match.group(3)
                level_int = level_str_to_int[level]  # 等级
                
                word_level_dict[pure_word] = level_int
                level_wordlist_dict[level_int].append(pure_word)
                
                if pos:
                    word_pos_level_dict["{}（{}）".format(pure_word, pos)] = level_int
                    level_wordposlist_dict[level_int].append("{}（{}）".format(pure_word, pos))
                else:
                    word_pos_level_dict[pure_word] = level_int
                    level_wordposlist_dict[level_int].append(pure_word)
        
        
        return word_level_dict, word_pos_level_dict, level_wordlist_dict, level_wordposlist_dict
    
    def assemble_single_choice_quiz(self, language, question, extracted_word, distracted_words, answer_idx, choices_num=4):
        if language == Language.zh.value:
            quiz = deepcopy(self.quiz_zh_template)
        elif language == Language.en.value:
            quiz = deepcopy(self.quiz_en_template)
        elif language == Language.ar.value:
            quiz = deepcopy(self.quiz_ar_template)
        
        quiz["question"] = question
        quiz["answer_list"] = [extracted_word]
        
        options = distracted_words[:choices_num-1]
        options.insert(answer_idx, extracted_word)
        quiz["option_list"] = options
        
        return quiz
            

    def action(self, quiz_generating_ctx):
        pass

    def able_to_generate(self, quiz_generating_ctx):
        pass

    def to_dict(self, quiz_res):
        content = dict()
        content["question"] = quiz_res["quiz_language_list"][0]["question"]
        content["options"] = quiz_res["quiz_language_list"][0]["options"]
        content["answer"] = quiz_res["quiz_language_list"][0]["answer"]
        content["explanation"] = quiz_res["quiz_language_list"][0]["explanation"]
        
        # multi_lingual_quiz = video_processor.translate_zh_quiz(content)
        content["ar_question"] = quiz_res["quiz_language_list"][1]["question"]
        content["ar_options"] = quiz_res["quiz_language_list"][1]["options"]
        content["ar_explanation"] = quiz_res["quiz_language_list"][1]["explanation"]
        content["en_question"] = quiz_res["quiz_language_list"][2]["question"]
        content["en_options"] = quiz_res["quiz_language_list"][2]["options"]
        content["en_explanation"] = quiz_res["quiz_language_list"][2]["explanation"]

        return content

class WordTranslationQuizGeneratingWorker(QuizGeneratingWorker):
    def __init__(self, quiz_worker_config):
        # 调用父类的__init__方法
        super().__init__(quiz_worker_config)
        
        hsk_zh_en_ar_path = quiz_worker_config.get("hsk_zh_en_ar_path", None)
        assert hsk_zh_en_ar_path is not None
        self.clean_hsk_infod = defaultdict(list)
        df = pd.read_csv(hsk_zh_en_ar_path)
        for i in range(df.shape[0]):
            word = df.iloc[i]["单词"].strip()
            pattern = r'(.*?)\s*(?:（(.*?)）)?\s*（([一二三四五六]级)）'
            match = re.match(pattern, word)
            if not match:
                print (word)
                continue
            clean_word = match.group(1).strip()
            pos = match.group(2)
            level_str = match.group(3)
            if pd.isna(df.iloc[i]["English"]):
                english = ""
            else:
                english = df.iloc[i]["English"].strip()
            if pd.isna(df.iloc[i]["Arabic"]):
                arabic = ""
            else:
                arabic = df.iloc[i]["Arabic"].strip()
            self.clean_hsk_infod[clean_word].append({"ori_word": word, "pos": pos, "level": level_str, "en": english, "ar": arabic})
    
    def able_to_generate(self, quiz_generating_ctx):
        extracted_word = quiz_generating_ctx.extracted_word
        info_list = self.clean_hsk_infod.get(extracted_word, None)
        if info_list is None or len(info_list) == 0:
            return False
        ar_word = info_list[0].get("ar", "")
        en_word = info_list[0].get("en", "")
        if ar_word == "" or en_word == "":
            return False
        return True
        
    def action(self, quiz_generating_ctx):
        extracted_word = quiz_generating_ctx.extracted_word
        recent_video_infos = quiz_generating_ctx.recent_video_infos
        info_list = self.clean_hsk_infod.get(extracted_word, None)
        if info_list is None or len(info_list) == 0:
            return self.quiz_template
        
        ar_word = info_list[0].get("ar", "")
        en_word = info_list[0].get("en", "")
        if ar_word == "" or en_word == "":
            return self.quiz_template
        
        distracted_words = list()
        if len(recent_video_infos) > 0:
            pass

        else:
            level_int = self.word_level_dict.get(extracted_word, None)
            if level_int is None:
                for i in range(1, 7):
                    distracted_words += self.level_wordlist_dict[i]
            else:
                distracted_words = self.level_wordlist_dict[level_int]

            distracted_words = list(set(distracted_words))
            rd.shuffle(distracted_words)
            # 取前10个不为extracted_word的单词
            distracted_words = [word for word in distracted_words[:10] if word != extracted_word]

        zh_question = "Select the correct translation: {}".format(en_word)
        ar_question = "اختر الترجمة الصحيحة: {}".format(ar_word)
        en_question = "Select the correct translation: {}".format(en_word)
        
        answer_idx = rd.randint(0, 4)
        zh_quiz = self.assemble_single_choice_quiz(Language.zh.value, zh_question, extracted_word, distracted_words, answer_idx)
        ar_quiz = self.assemble_single_choice_quiz(Language.ar.value, ar_question, extracted_word, distracted_words, answer_idx)
        en_quiz = self.assemble_single_choice_quiz(Language.en.value, en_question, extracted_word, distracted_words, answer_idx)

        quiz_result = {
            "quiz_type": QuizType.single_choice.value,
            "quiz_language_list": [zh_quiz, en_quiz, ar_quiz]
        }

        return quiz_result

class CharFillingQuizGeneratingWorker(QuizGeneratingWorker):
    def __init__(self, quiz_worker_config):
        super().__init__(quiz_worker_config)

        hsk_char_path = quiz_worker_config.get("hsk_char_path", None)
        assert hsk_char_path is not None
        self.hsk_char_list = list()
        df = pd.read_csv(hsk_char_path)
        for i in range(df.shape[0]):
            word = df.iloc[i]["汉字"].strip()
            if word == "":
                continue
            self.hsk_char_list.append(word)
        rd.shuffle(self.hsk_char_list)
    
    def able_to_generate(self, quiz_generating_ctx):
        extracted_word = quiz_generating_ctx.extracted_word
        if len(extracted_word) <= 1:
            return False
        return True

    def action(self, quiz_generating_ctx):
        extracted_word = quiz_generating_ctx.extracted_word
        recent_video_infos = quiz_generating_ctx.recent_video_infos
        char_len = len(extracted_word)
        if char_len <= 1:
            return self.quiz_template
        
        del_char_idx = rd.randint(0, char_len-1)
        del_char = extracted_word[del_char_idx]
        extracted_word_with_blank = extracted_word[:del_char_idx] + "__" + extracted_word[del_char_idx+1:]
        distract_char_list = list()
        rd.shuffle(self.hsk_char_list)
        distract_char_list = self.hsk_char_list[:10]
        distract_char_list = [char for char in distract_char_list if char != del_char]
        
        zh_question = "用正确的汉字填空: {}".format(extracted_word_with_blank)
        ar_question = "املأ الفراغات باستخدام الأحرف الصينية الصحيحة： {}".format(extracted_word_with_blank)
        en_question = "Fill in the blanks with the correct Chinese characters: {}".format(extracted_word_with_blank)
        
        answer_idx = rd.randint(0, 4)
        zh_quiz = self.assemble_single_choice_quiz(Language.zh.value, zh_question, del_char, distract_char_list, answer_idx)
        ar_quiz = self.assemble_single_choice_quiz(Language.ar.value, ar_question, del_char, distract_char_list, answer_idx)
        en_quiz = self.assemble_single_choice_quiz(Language.en.value, en_question, del_char, distract_char_list, answer_idx)

        quiz_result = {
            "quiz_type": QuizType.single_choice.value,
            "quiz_language_list": [zh_quiz, en_quiz, ar_quiz]
        }

        return quiz_result

class CharPinyinQuizGeneratingWorker(QuizGeneratingWorker):
    def __init__(self, quiz_worker_config):
        super().__init__(quiz_worker_config)
        self.word_py_dict = dict()
        self.charlen_dict = defaultdict(list)
        for word in self.word_level_dict:
            self.word_py_dict[word] = [item[0] for item in pinyin(word)]
            charlen = len(word)
            self.charlen_dict[charlen].append(word)
        
    def able_to_generate(self, quiz_generating_ctx):
        extracted_word = quiz_generating_ctx.extracted_word
        if len(extracted_word) > 4:
            return False
        return True

    def action(self, quiz_generating_ctx):
        extracted_word = quiz_generating_ctx.extracted_word
        extracted_word_py = [item[0] for item in pinyin(extracted_word)]
        extracted_word_py_str = " ".join(extracted_word_py)
        recent_video_infos = quiz_generating_ctx.recent_video_infos
        char_len = len(extracted_word)
        assert char_len <= 4
        rd.shuffle(self.charlen_dict[char_len])
        equal_char_len_words = self.charlen_dict[char_len][:10]
        distract_words = []
        for word in equal_char_len_words:
            if word == extracted_word or (" ".join(self.word_py_dict[word]) == extracted_word_py_str):
                continue
            distract_words.append(word)
        
        zh_question = "What is the Chinese character for {}?".format(extracted_word_py_str)
        ar_question = "ما هو الحرف الصيني لـ {}".format(extracted_word_py_str)
        en_question = "What is the Chinese character for {}?".format(extracted_word_py_str)

        answer_idx = rd.randint(0, 4)
        zh_quiz = self.assemble_single_choice_quiz(Language.zh.value, zh_question, extracted_word, distract_words, answer_idx)
        ar_quiz = self.assemble_single_choice_quiz(Language.ar.value, ar_question, extracted_word, distract_words, answer_idx)
        en_quiz = self.assemble_single_choice_quiz(Language.en.value, en_question, extracted_word, distract_words, answer_idx)

        quiz_result = {
            "quiz_type": QuizType.single_choice.value,
            "quiz_language_list": [zh_quiz, en_quiz, ar_quiz]
        }

        return quiz_result
        
        
class QuizGeneratingWorkerFactory:
    def __init__(self):
        quiz_worker_config = {"hsk_zh_en_ar_path": "video_process/hsk_dictionary/HSK_zh_en_ar.csv", "hsk_char_path": "video_process/hsk_dictionary/HSK_char.csv"}
        self.word_translation_quiz_worker = WordTranslationQuizGeneratingWorker(quiz_worker_config)
        self.char_filling_quiz_worker = CharFillingQuizGeneratingWorker(quiz_worker_config)
        self.char_pinyin_quiz_worker = CharPinyinQuizGeneratingWorker(quiz_worker_config)

    # 拼音选字（可逆）0; 词汇翻译选择（可逆）1; 词汇翻译连线 2; 句子选词翻译 3; 看词选图翻译（可逆）4; 听音频选词/字（可逆）5; 看单词输入中文 6; 词汇挖空选字 7
    def get_worker(self, quiz_type):
        if quiz_type == 0:
            return self.char_pinyin_quiz_worker
        if quiz_type == 1:
            return self.word_translation_quiz_worker
        if quiz_type == 7:
            return self.char_filling_quiz_worker
        return None

class QuizGenerator:
    def __init__(self):
        self.quiz_generating_worker_factory = QuizGeneratingWorkerFactory()
        self.avaliable_quiz_types = [0, 1, 7]
    
    def extract_word_from_video_info(self, video_info):
        series_name = video_info.get("series_name", None)
        title = video_info.get("title", None)
        if series_name in ["单字视频", "HSK_表情包", "HSK 600 20250104", "PNU_1", "PNU_2", "videos", "HSK output_new_2 20250107", "HSK output"]:
            return title.strip().split("_")[-1]
        if series_name in ["HSK_1_2_3_写字视频"]:
            return title.strip().split("_").split("（")[0]
        if series_name in ["悟空识字"]:
            return title.strip().split("_")[-2].strip()
        return ""
        
    async def generate_quiz(self, video_id):
        
        quiz_zh_template = {"language": Language.zh.value, "question": "", "option_list": [], "answer_list": [], "explanation": ""}
        quiz_en_template = {"language": Language.en.value, "question": "", "option_list": [], "answer_list": [], "explanation": ""}
        quiz_ar_template = {"language": Language.ar.value, "question": "", "option_list": [], "answer_list": [], "explanation": ""}
        quiz_template = {
            "quiz_type": QuizType.invalid.value,
            "quiz_language_list": [quiz_zh_template, quiz_en_template, quiz_ar_template]
        }
        quiz_result_list = []
        try:
            video_info_str = await get_redis("video-{}".format(video_id))
            video_info = json.loads(video_info_str)
            
            extracted_word = self.extract_word_from_video_info(video_info)
            # 检查extracted_word是否包含数字或英文字母
            if extracted_word == "" or any(char.isdigit() or char.isascii() for char in extracted_word):
                return quiz_template
            # For debug
            # print (video_info["title"])
            # print (video_info["series_name"])
            # print (extracted_word)
            #########################################################
            quiz_generating_ctx = QuizGeneratingCtx()
            quiz_generating_ctx.extracted_word = extracted_word

            for quiz_type in self.avaliable_quiz_types:
                quiz_generating_worker = self.quiz_generating_worker_factory.get_worker(quiz_type)
                if quiz_generating_worker.able_to_generate(quiz_generating_ctx):
                    quiz_result = quiz_generating_worker.action(quiz_generating_ctx)
                    quiz_result_list.append(quiz_result)
        

        except Exception as e:
            print (e)
            return quiz_template
        if len(quiz_result_list) == 0:
            return quiz_template
        rd.shuffle(quiz_result_list)
        
        return quiz_result_list[0]


if __name__ == "__main__":
    # pass
    from recaller import init_redis_pool, close_redis_pool
    asyncio.run(init_redis_pool())

    # quiz_worker_config = {"hsk_zh_en_ar_path": "../video_process/hsk_dictionary/HSK_zh_en_ar.csv", "hsk_char_path": "../video_process/hsk_dictionary/HSK_char.csv"}
    # word_trans_quiz_worker = WordTranslationQuizGeneratingWorker(quiz_worker_config)
    # char_filling_quiz_worker = CharFillingQuizGeneratingWorker(quiz_worker_config)
    # # char_py_quiz_worker = CharPinyinQuizGeneratingWorker(quiz_worker_config)
    # quiz_generating_ctx = QuizGeneratingCtx()
    # quiz_generating_ctx.extracted_word = "我们"
    # quiz_generating_ctx.recent_video_infos = []
    # quiz_result = word_trans_quiz_worker.action(quiz_generating_ctx)
    # quiz_result = char_filling_quiz_worker.action(quiz_generating_ctx)
    # quiz_result = char_py_quiz_worker.action(quiz_generating_ctx)

    generator = QuizGenerator()
    
    # 创建事件循环并运行异步函数
    quiz_result = asyncio.run(generator.generate_quiz("678f6985e19148c899eeb41f"))
    # quiz_result = asyncio.run(generator.generate_quiz("6790d5abcc3e1daa477a2a13"))
    print(quiz_result)
    asyncio.run(close_redis_pool())