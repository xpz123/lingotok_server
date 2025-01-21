import requests
import pysrt
import json
from datetime import timedelta
from call_huoshan_srt import *
from binary_sta_demo import huoshan_srt_with_text
from translator import translate_text2ar
import sys
import os
from vod_huoshan_util import get_vid_playurl
import ast
from tqdm import tqdm
import jieba
from zhon.hanzi import punctuation
import string
from llm_util import call_doubao_pro_128k, call_gpt4o, call_doubao_pro_32k
import random as rd
import pandas as pd
from pypinyin import pinyin
import cv2
import math
os.environ["IMAGEIO_FFMPEG_EXE"] = "/opt/homebrew/Cellar/ffmpeg/7.1_3/bin/ffmpeg"
from moviepy.editor import VideoFileClip
from copy import deepcopy

def get_video_resolution(video_file):
	cap = cv2.VideoCapture(video_file)
	assert cap.isOpened()

	width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
	height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
	cap.release()
	
	return width, height

def zhihu_url_convert(page_url):
	prefix = "https://lens.zhihu.com/api/v4/videos/"
	html_text = requests.get(page_url).text
	vid_begin_idx = html_text.find("videoId") + 10
	vid_end_idx = html_text[vid_begin_idx:].find('"') + vid_begin_idx
	vid = html_text[vid_begin_idx:vid_end_idx]
	static_url = "{}{}".format(prefix, vid)

	play_url_dict = dict()
	play_info_json = json.loads(requests.get(static_url).text)
	for item in play_info_json["playlist"].keys():
		play_url_dict[item] = play_info_json["playlist"][item]["play_url"]
	return static_url, play_url_dict


def milliseconds_to_time_string(ms):
    delta = timedelta(milliseconds=ms)

    total_seconds = int(delta.total_seconds())
    
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    milliseconds = delta.microseconds // 1000  # 转换微秒为毫秒
    
    time_string = f"{hours:02}:{minutes:02}:{seconds:02},{milliseconds:03}"
    return time_string

def post_http_request(prompt: str,
                      api_url: str,
                      seed: int,
                      stream: bool = False) -> requests.Response:
	headers = {"User-Agent": "Test Client"}
	pload = {
		"prompt": prompt,
		"repetition_penalty": 1.0,
		"temperature": 0,
		"top_p": 1,
		"top_k": -1,
		"max_tokens": 8192,
		"stream": stream,
		# "stop" : []
		"stop" : ['\n#输出结束', '\n\n', 'Human:']
	}
	response = requests.post(api_url, headers=headers, json=pload, stream=stream)
	return response

def translate_quiz_metainfo(metainfo_filename, new_metainfo_filename):
	video_processor = VideoProcessor()
	lines = open(metainfo_filename).readlines()
	fw = open(new_metainfo_filename, "w", encoding="utf-8")
	for l in tqdm(lines):
		quiz = json.loads(l.strip())
		try:
			trans_res = video_processor.translate_zh_quiz(quiz, gen_ar=True, gen_en=True)
			
			quiz["ar_question"] = trans_res["ar_quiz"]["question"]
			quiz["ar_options"] = trans_res["ar_quiz"]["options"]
			quiz["ar_explanation"] = trans_res["ar_quiz"]["explanation"]
			quiz["en_question"] = trans_res["en_quiz"]["question"]
			quiz["en_options"] = trans_res["en_quiz"]["options"]
			quiz["en_explanation"] = trans_res["en_quiz"]["explanation"]
			fw.write(json.dumps(quiz, ensure_ascii=False) + "\n")
		except Exception as e:
			os.system("sleep 5")
			print ("vid: {} error: {}".format(quiz["vid"], str(e)))
	fw.close()
		
def update_quiz_metainfo(video_info_filename, metainfo_filename):
	video_processor = VideoProcessor()
	df = pd.read_csv(video_info_filename)
	fw = open(metainfo_filename, "w", encoding="utf-8")
	for i in tqdm(range(df.shape[0])):
		zh_srt = df.iloc[i]["zh_srt"]
		try:
			vid = df.iloc[i]["VID"]
			quiz = video_processor.generate_quiz_zh_tiankong(zh_srt.replace("\\", "/"))
			if quiz:
				quiz["vid"] = vid
				fw.write(json.dumps(quiz, ensure_ascii=False) + "\n")
		except Exception as e:
			print ("vid: {} error: {}".format(vid, str(e)))
	fw.close()

def add_pinyin_srt(video_info_file, new_video_info_file):
	video_processor = VideoProcessor()
	import pdb
	pdb.set_trace()
	df = pd.read_csv(video_info_file)
	for i in tqdm(range(df.shape[0])):
		zh_srt = df.iloc[i]["zh_srt"].replace("\\", "/")
		pinyin_srt = zh_srt.replace("Chinese", "Pinyin")
		video_processor.convert_zhsrt_to_pinyinsrt(zh_srt, pinyin_srt)
		df.at[i, "pinyin_srt"] = pinyin_srt.replace("/", "\\")
		os.system("scp  {} root@54.248.147.60:/dev/data/lingotok_server/huoshan/srt_dir".format(pinyin_srt))
	df.to_csv(new_video_info_file, index=False)

def compress_videos(video_info_file, new_video_info_file):
	video_processor = VideoProcessor()
	df = pd.read_csv(video_info_file)
	columns = df.columns.tolist()
	columns.append("compressed_FileName")
	df_list = df.values.tolist()
	for i in tqdm(range(df.shape[0])):
		try:
			video_file = df.iloc[i]["FileName"]
			if video_processor.compress_video(video_file, video_file.replace(".mp4", "_compressed.mp4")):
				df_list[i].append(video_file.replace(".mp4", "_compressed.mp4"))
			else:
				df_list[i].append("null")
		except Exception as e:
			print (str(e))
			df_list[i].append("null")

	df_new = pd.DataFrame(df_list, columns=columns)
	df_new.to_csv(new_video_info_file, index=False)

def chunk_videos(video_info_file, new_video_info_file):
	video_processor = VideoProcessor()
	df = pd.read_csv(video_info_file)
	columns = df.columns.tolist()
	for tag_idx, tag in enumerate(columns):
		if tag == "zh_srt":
			zh_srt_idx = tag_idx
			continue
		if tag == "en_srt":
			en_srt_idx = tag_idx
			continue
		if tag == "ar_srt":
			ar_srt_idx = tag_idx
			continue
		if tag == "pinyin_srt":
			py_srt_idx = tag_idx
			continue
		if tag == "FileName":
			video_file_idx = tag_idx
			continue
	columns.append("compressed_FileName")
	df_list = df.values.tolist()
	df_new_list = list()
	for i in tqdm(range(df.shape[0])):
		video_file = df.iloc[i]["FileName"]
		zh_srt = df.iloc[i]["zh_srt"]
		clip = VideoFileClip(video_file)  # 加载视频文件
		if clip.duration < 120:
			df_new_list.append(df_list[i])
			continue
		video_dir = os.path.dirname(video_file)
		chunk_list = video_processor.chunk_video(video_file, zh_srt, video_dir)
		ori_list = df_list[i]
		for chunk in chunk_list:
			new_list = deepcopy(ori_list)
			new_list[video_file_idx] = chunk["video_file"]
			new_list[zh_srt_idx] = chunk["zh_srt"]
			new_list[en_srt_idx] = chunk["en_srt"]
			new_list[ar_srt_idx] = chunk["ar_srt"]
			new_list[py_srt_idx] = chunk["py_srt"]
			df_new_list.append(new_list)

	df_new = pd.DataFrame(df_new_list, columns=columns)
	df_new.to_csv(new_video_info_file, index=False)

class VideoProcessor:
	def __init__(self):
		hsk_word_list = list()
		with open("hsk_dictionary/hsk-level6.txt", "r") as f:
			for line in f:
				line = line.strip()
				if line == "":
					continue
				if line.find("(") != -1:
					line = line.split("(")[0]
				if line.find("……") != -1:
					hsk_word_list.append(line.split("……")[0])
					hsk_word_list.append(line.split("……")[1])
				else:
					hsk_word_list.append(line)
		self.hsk_word_set = set(hsk_word_list)
	

	def load_srt(self, srt_file_name):
		self.subtitles = pysrt.open(srt_file_name)
		return self.subtitles

	def get_srt_text(self):
		return self.subtitles.text.replace("\n", "")

	def judge_srt_level(self):
		subtitle_text = self.get_srt_text().replace("\n", " ")
		if len(subtitle_text) > 1000:
			subtitle_text = subtitle_text[0:1000]
		prompt = "#Requirements: Please classify the listening comprehension of the following English text into CEFR categories (A1, A2, B1, B2, C1, C2) from the perspective of vocabulary and grammar. The reason needs to be explained before giving the classification result. The reason is wrapped with <reason>, and the classification result is wrapped with <res>. There is three examples: \n#English Text: You wanted to come this time. Little sis! Big sis! You were gone forever. It was 3 days. Where are we going? Before Molly stole Tiffany's heart? Ancestors wanted to connect our island to all the people of the entire ocean. It's my job as a leaf finder to finish what they started. I wanna show how people just how far we'll go. <reason>The text contains a mix of simple and more complex vocabulary and grammatical structures. It includes basic vocabulary such as 'wanted,'' come,' 'sister,' and 'heart,' which are typical of A1 and A2 levels. However, it also includes more advanced vocabulary like 'ancestors,' 'island,' 'ocean,' and 'leaf finder,' which are more characteristic of B1 and B2 levels. The sentence structure is relatively simple, but there are some complex ideas and longer sentences that require a good understanding of context and abstract concepts. The text also includes some idiomatic expressions and colloquial language, such as 'Little sis! Big sis!' and 'Ancestors wanted to connect our island to all the people of the entire ocean,' which might pose a challenge for lower levels. Overall, the text requires a good grasp of both basic and more advanced vocabulary and some understanding of abstract concepts, which aligns with the B1 level.</reason>\n<res>B1</res>\n#English Text: I need an egg.Thank you. <reason>The text contains very basic vocabulary and simple grammatical structures. It includes common words such as 'egg' and 'thank you,' which are typical of A1 level. The sentence structure is straightforward, with simple subject-verb-object patterns. There are no complex ideas or abstract concepts, and the text does not include idiomatic expressions or colloquial language that might pose additional challenges. Therefore, the text is suitable for learners at the A1 level, as it uses basic vocabulary and simple grammar to convey a clear and straightforward message.</reason>\n<res>A1</res>\n#English Text: you complain about a life that you choseyou are not a victimnot at allyour generosity concealed something dirtier and meaneryou're incapable of facing your ambitionsand you resent me for itbut I'm not the one who put you where you areI have nothing to do with ityou're not sacrificing yourself as you sayyouchoose to sit on the sidelines because you're afraidbecause your pride makes your headexplode before you can even come up with a littlegerm of an ideaand now you wake upand your 40 and you need someone to blameand you're the one to blameyou're petrified by your own fucking standardsand your fear of failurethis is the truth. <reason>The text contains a high level of complexity in both vocabulary and grammatical structures, which is characteristic of a C1 or C2 level. It includes a wide range of advanced vocabulary such as 'ambitions,' 'petrified,' 'standards,' and 'fear of failure,' which are typically found in more advanced language proficiency levels. The text also features complex grammatical structures, including the use of subjunctive mood ('you\'re not a victim not at all'), conditional sentences ('you\'re not sacrificing yourself as you say'), and various sentence types that require a deep understanding of English syntax and semantics. The text is also laden with idiomatic expressions and colloquial language, such as 'sitting on the sidelines,' 'petrified by your own fucking standards,' and 'come up with a little germ of an idea,' which can be challenging for learners at lower levels. The overall tone and content of the text are also quite advanced, dealing with mature and complex themes such as personal responsibility, ambition, and self-perception. Therefore, the text is best classified as C1 or C2.</reason>\n<res>C1</res>\n This is a real input. \n#English Text: {}".format(subtitle_text)
		data = {"sysinfo": "You are an experienced English teacher who can differentiate the difficulty of a piece of English content by its vocabulary and grammatical content.", "prompt": ""}
		url = "http://10.202.196.9:8087/call_qwen25_7b"
		data["prompt"] = prompt
		response = requests.post(url, data=data)
		llm_input = json.loads(response.text)["text"]
		resp = post_http_request(prompt=llm_input, api_url="http://10.202.196.9:6679/generate", seed=1234)
		tag_text = json.loads(resp.text)["text"][0]
		# print (tag_text)

		start_index = tag_text.rfind("<res>") + len("<res>")
		end_index = tag_text.rfind("</res>")
		res = tag_text[start_index:end_index].replace("<res>", "").replace("</res>", "")

		reason_start_index = tag_text.rfind("<reason>") + len("<reason>")
		reason_end_index = tag_text.rfind("</reason>")
		reason = tag_text[reason_start_index:reason_end_index]

		return res, reason
	
	def generate_srt(self, file_name, play_url, gen_ar=False, gen_zh=False):
		res = {"er_srt": "{}_English.srt".format(file_name)}
		en_srt_fw = open("{}_English.srt".format(file_name), "w")
		if gen_ar:
			ar_srt_fw = open("{}_Arabic.srt".format(file_name), "w")
			res["ar_srt"] = "{}_Arabic.srt".format(file_name)
		if gen_zh:
			zh_srt_fw = open("{}_Chinese.srt".format(file_name), "w")
			res["zh_srt"] = "{}_Chinese.srt".format(file_name)
		try:
			ori_resp = call_huoshan_srt(play_url)
			text_list = []
			start_time_list = []
			end_time_list = []
			for i, utterance in enumerate(ori_resp["utterances"]):
				start_time = milliseconds_to_time_string(utterance["start_time"])
				start_time_list.append(start_time)
				end_time = milliseconds_to_time_string(utterance["end_time"])
				end_time_list.append(end_time)
				text = utterance["text"]
				text_list.append(text)
			if gen_ar:
				ar_text_list = translate_text2ar(text_list, "ar")["TranslationList"]
				assert len(ar_text_list) == len(text_list)
			if gen_zh:
				zh_text_list = translate_text2ar(text_list, "zh")["TranslationList"]
				assert len(zh_text_list) == len(text_list)
			
			en_srt_content = ""
			ar_srt_content = ""
			zh_srt_content = ""
			for i in range(len(text_list)):
				text = text_list[i]
				start_time = start_time_list[i]
				end_time = end_time_list[i]
				en_srt_content = f"{i}\n{start_time} --> {end_time}\n{text}\n\n"
				en_srt_fw.write(en_srt_content)
				if gen_ar:
					ar_text = ar_text_list[i]["Translation"]
					ar_srt_content = f"{i}\n{start_time} --> {end_time}\n{ar_text}\n\n"
					ar_srt_fw.write(ar_srt_content)
				if gen_zh:
					zh_text = zh_text_list[i]["Translation"]
					zh_srt_content = f"{i}\n{start_time} --> {end_time}\n{zh_text}\n\n"
					zh_srt_fw.write(zh_srt_content)
			en_srt_fw.close()
			if gen_ar:
				ar_srt_fw.close()
			if gen_zh:
				zh_srt_fw.close()
		except Exception as inst:
			print (str(inst))
		return res
	
	def convert_zhsrt_to_pinyinsrt(self, zh_srt_file, pinyin_srt_file):
		zh_srt = pysrt.open(zh_srt_file)
		pinyin_srt_fw = open(pinyin_srt_file, "w")
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
		pinyin_srt_fw.close()
	
	def generate_zhsrt(self, play_url, file_name, audio_path=None, gen_ar=True, gen_en=True, audio_text=None, gen_pinyin=True):
		res = {"zh_srt": "{}_Chinese.srt".format(file_name)}
		zh_srt_fw = open("{}_Chinese.srt".format(file_name), "w")
		if gen_ar:
			ar_srt_fw = open("{}_Arabic.srt".format(file_name), "w")
			res["ar_srt"] = "{}_Arabic.srt".format(file_name)
		if gen_en:
			en_srt_fw = open("{}_English.srt".format(file_name), "w")
			res["en_srt"] = "{}_English.srt".format(file_name)
		
		if gen_pinyin:
			res["pinyin_srt"] = "{}_Pinyin.srt".format(file_name)
		try:
			if audio_path:
				if audio_text == None:
					ori_resp = call_huoshan_srt_wav(audio_path, language="zh-CN", words_per_line=15)
				else:
					ori_resp = huoshan_srt_with_text(audio_text, audio_path)
			else:
				ori_resp = call_huoshan_srt(play_url, language="zh-CN", words_per_line=15)
			text_list = []
			start_time_list = []
			end_time_list = []
			for i, utterance in enumerate(ori_resp["utterances"]):
				start_time = milliseconds_to_time_string(utterance["start_time"])
				start_time_list.append(start_time)
				end_time = milliseconds_to_time_string(utterance["end_time"])
				end_time_list.append(end_time)
				text = utterance["text"]
				text_list.append(text)
			if gen_ar:
				ar_text_list = translate_text2ar(text_list, "ar")
				assert len(ar_text_list) == len(text_list)
			if gen_en:
				en_text_list = translate_text2ar(text_list, "en")
				assert len(en_text_list) == len(text_list)
			
			ar_srt_content = ""
			for i in range(len(text_list)):
				text = text_list[i]
				start_time = start_time_list[i]
				end_time = end_time_list[i]
				en_srt_content = f"{i}\n{start_time} --> {end_time}\n{text}\n\n"
				zh_srt_fw.write(en_srt_content)
				if gen_ar:
					ar_text = ar_text_list[i]["Translation"]
					ar_srt_content = f"{i}\n{start_time} --> {end_time}\n{ar_text}\n\n"
					ar_srt_fw.write(ar_srt_content)
				if gen_en:
					en_text = en_text_list[i]["Translation"]
					en_srt_content = f"{i}\n{start_time} --> {end_time}\n{en_text}\n\n"
					en_srt_fw.write(en_srt_content)
			zh_srt_fw.close()
			if gen_ar:
				ar_srt_fw.close()
			if gen_en:
				en_srt_fw.close()
			if gen_pinyin:
				self.convert_zhsrt_to_pinyinsrt(res["zh_srt"], res["pinyin_srt"])
		except Exception as inst:
			print (str(inst))
			return None
		return res
	
	def translate_srt(self, filepath, gen_ar=True, gen_zh=False):
		filename = filepath.split("\\")[-1]
		srt_dir = "/".join(filepath.split("\\")[:-1])
		res = {"en_srt": os.path.join(srt_dir,filename)}
		en_srt_data = pysrt.open( res["en_srt"])
		
		if gen_ar:
			res["ar_srt"] = os.path.join(srt_dir, filename.replace("English", "Arbic"))
		if gen_zh:
			res["zh_srt"] = os.path.join(srt_dir , filename.replace("English", "Chinese"))
		
		en_srt_text_list = list()
		for sub in en_srt_data:
			en_srt_text_list.append(sub.text)
		
		try:
			if gen_ar:
				ar_text_list = translate_text2ar(en_srt_text_list, "ar")["TranslationList"]
				assert len(ar_text_list) == len(en_srt_text_list)
			if gen_zh:
				zh_text_list = translate_text2ar(en_srt_text_list, "zh")["TranslationList"]
				assert len(zh_text_list) == len(en_srt_text_list)
			
			if gen_ar:
				for i in range(len(en_srt_data)):
					ar_text = ar_text_list[i]["Translation"]
					en_srt_data[i].text = ar_text
				en_srt_data.save(res["ar_srt"])
					
			if gen_zh:
				for i in range(len(en_srt_data)):
					zh_text = zh_text_list[i]["Translation"]
					en_srt_data[i].text = zh_text
				en_srt_data.save(res["zh_srt"])
		except Exception as inst:
			print (str(inst))
		return res

	def generate_quiz(self):
		subtitle_text = self.get_srt_text().replace("\n", " ")
		if len(subtitle_text) > 1000:
			subtitle_text = subtitle_text[0:1000]
		tmp = {
			"question": "What is the one thing the speaker admits they cannot do in basketball?",
			"options": [
				"Spin a ball on their feet",
				"Bounce the ball off their knee",
				"Spin a ball on their finger",
				"Play as a point guard"
			],
			"answer": "C) Spin a ball on their finger"}

		tmps = json.dumps(tmp)
		prompt = "#Requirement: Please create a multiple-choice question with four options based on the main content of the following English video. Return in json format. Here is an example of result: {}#English Text: {}".format(tmps, subtitle_text)
		data = {"sysinfo": "You are an experienced English teacher.", "prompt": ""}
		url = "http://10.202.196.9:8087/call_qwen25_7b"
		data["prompt"] = prompt
		response = requests.post(url, data=data)
		llm_input = json.loads(response.text)["text"]
		resp = post_http_request(prompt=llm_input, api_url="http://10.202.196.9:6679/generate", seed=1234)
		tag_text = json.loads(resp.text)["text"][0]
		# print (tag_text)

		start_index = tag_text.rfind("```json")
		end_index = tag_text.rfind("```")
		res = json.loads(tag_text[start_index:end_index].replace("```json", ""))
		if res["answer"][0] not in ["A", "B", "C", "D"]:
			res["answer"] = "B"
		else:
			res["answer"] = res["answer"][0]
		if res["options"][0].find("A.") == -1:
			res["options"][0] = "A. " + res["options"][0]
		if res["options"][1].find("B.") == -1:
			res["options"][1] = "B. " + res["options"][1]
		if res["options"][2].find("C.") == -1:
			res["options"][2] = "C. " + res["options"][2]
		if res["options"][3].find("D.") == -1:
			res["options"][3] = "D. " + res["options"][3]
		# print (res)
		# reason_start_index = tag_text.rfind("<reason>") + len("<reason>")
		# reason_end_index = tag_text.rfind("</reason>")
		# reason = tag_text[reason_start_index:reason_end_index]

		return res
	
	def generate_quiz_zh(self, subtitle_file):
		subtitle_text = pysrt.open(subtitle_file).text.replace("\n", "。 ")
		if len(subtitle_text) < 5:
			print ("Too short subtitle text, skip!")
			return None
		# print (subtitle_text)
		# subtitle_text = self.get_srt_text().replace("\n", " ")
		if len(subtitle_text) > 1000:
			subtitle_text = subtitle_text[0:1000]
		tmp = {
			"question": "以下哪项陈述是正确的？",
			"options": [
				"A. 张小姐找到了她的父母。",
				"B. 张淑妍怀孕了。",
				"C. 赵景轩是沈总的未婚妻。",
				"D. 苏医生没有钱治病。"
			],
			"answer": "B",
			"explanation": "根据文中内容，张淑妍确实怀孕了。文中提到'对了这是你的体检单确认怀孕'和'加叶我怀孕了'，可以确认张淑妍怀孕的事实。"
		}

		tmps = json.dumps(tmp)
		# prompt = "#要求: 请根据以下的视频对话内容，出一个阅读理解的选择题目，用json格式返回。下面是一个json的例子： {}#视频对话内容: {}".format(tmps, subtitle_text)
		prompt = '''#要求: 请根据以下的视频对话内容，出一个阅读理解的选择题目，一定只包括A、B、C、D四个选项，并且采用下面的json格式返回。下面是json的例子：
		```json
		{
			"question": "以下哪项陈述是正确的？",
			"options": [
				"A. 张小姐找到了她的父母。",
				"B. 张淑妍怀孕了。",
				"C. 赵景轩是沈总的未婚妻。",
				"D. 苏医生没有钱治病。"
			],
			"answer": "B",
			"explanation": "根据视频中的内容，张淑妍确实怀孕了。文中提到'对了这是你的体检单确认怀孕'和'加叶我怀孕了'，可以确认张淑妍怀孕的事实。"
		}
		```
		#视频对话内容: '''
		prompt += subtitle_text
		data = {"sysinfo": "你是一个资深的中文老师，会理解中文的内容并出阅读理解题目。", "prompt": ""}
		url = "http://10.202.196.9:8087/call_qwen25_7b"
		data["prompt"] = prompt
		response = requests.post(url, data=data)
		llm_input = json.loads(response.text)["text"]
		resp = post_http_request(prompt=llm_input, api_url="http://10.202.196.9:6679/generate", seed=1234)
		tag_text = json.loads(resp.text)["text"][0]
		# print (tag_text)

		start_index = tag_text.rfind("```json")
		end_index = tag_text.rfind("```")
		res = json.loads(tag_text[start_index:end_index].replace("```json", "").replace("\n", "").replace("\t", ""))
		if res["answer"][0] not in ["A", "B", "C", "D"]:
			res["answer"] = "B"
		else:
			res["answer"] = res["answer"][0]
		if res["options"][0].find("A.") == -1:
			res["options"][0] = "A. " + res["options"][0]
		if res["options"][1].find("B.") == -1:
			res["options"][1] = "B. " + res["options"][1]
		if res["options"][2].find("C.") == -1:
			res["options"][2] = "C. " + res["options"][2]
		if res["options"][3].find("D.") == -1:
			res["options"][3] = "D. " + res["options"][3]
		return res
	
	def generate_quiz_zh_tiankong(self, subtitle_file):
		# print (subtitle_file)
		
		subtitles = pysrt.open(subtitle_file)
		subtitle_list = list(subtitles)
		rd.shuffle(subtitle_list)

		example_quiz ={"question": "荷花全身上下所积蓄的夏日__？", "options": ["A. 色彩", "B. 芬芳", "C. 姿态", "D. 能量"], "answer": "D", "explanation": "‘色彩’主要指物体呈现出的颜色样貌，‘芬芳’侧重于气味方面，‘姿态’多形容样子、形态，而句子表达的是积蓄的一种抽象的、类似力量的事物，‘能量’符合上下文视频中的语境，所以应选 D 选项‘能量’。"}
		example_quiz_json = json.dumps(example_quiz,  ensure_ascii=False)

		stop_words = ["哎", "了", "的", "地", "吧", "吗", "啊", "你", "我", "他", "您", "嗯", "我们", "你们", "他们"]
		# stop_words = []

		for sub in subtitle_list:
			if len(sub.text) <= 5:
				continue
			subtitle_text = sub.text.replace("\n", "。 ")
			seg_list = jieba.cut(subtitle_text, cut_all=False)
			text = " ".join(seg_list)
			for word in text.split(" "):
				if word in stop_words:
					continue
				if word in self.hsk_word_set:
					# print (word)
					prompt = "以下是一个示例：当给到一个##中文句子##：“荷花 全身 上下 所 积蓄 的 夏日 能量”，遮挡其中“能量”这个词之后，将该句子变成一个选择题目，其中“能量“是正确选项，而其他词则是和“能量” 不相近并且也不符合语法语义的词。请注意！给我的结果需要按照如下的json格式： {}。这是一个##中文句子##：“{}”，遮挡其中“{}”这个词之后，将该句子变成一个选择题目，其中“{}“是正确选项，而其他词则是和“{}” 不相近并且也不符合语法语义的词。同时在给出理由的时候，要提及“根据视频中的语境”这类原因。请注意！给我的结果需要按照如下的json格式".format(example_quiz_json, text, word, word, word)
					# print (prompt)
					try:
						# resp = call_doubao_pro_128k(prompt)
						resp = call_doubao_pro_32k(prompt)
						content_ori = resp.replace("```json", "").replace("```", "")
						print (content_ori)
						res = json.loads(content_ori)
						print (res)
						if res["answer"][0] not in ["A", "B", "C", "D"]:
								res["answer"] = "B"
						else:
							res["answer"] = res["answer"][0]
						if res["options"][0].find("A.") == -1:
							res["options"][0] = "A. " + res["options"][0]
						if res["options"][1].find("B.") == -1:
							res["options"][1] = "B. " + res["options"][1]
						if res["options"][2].find("C.") == -1:
							res["options"][2] = "C. " + res["options"][2]
						if res["options"][3].find("D.") == -1:
							res["options"][3] = "D. " + res["options"][3]
						# res["question"] = "下面是刚刚视频中出现过的句子，请根据视频内容，选择最合适的词填入空格处：\n{}".format(text)
						return res
					except Exception as e:
						print (str(e))
						continue
				
		for sub in subtitle_list:
			if len(sub.text) <= 3:
				continue
			subtitle_text = sub.text.replace("\n", "。 ")
			seg_list = jieba.cut(subtitle_text, cut_all=False)
			for word in seg_list:
				if word in self.hsk_word_set:
					text = " ".join(seg_list)
					print (word)
					try:
						prompt = "以下是一个中文句子：“{}”，如果我遮挡其中“{}”这个词之后，将该句子变成一个选择题目，其中“{}“是正确选项，而其他词则是和“{}” 不相近并且也不符合语法语义的词。请注意！给我的结果需要按照如下的json格式: {}".format(text, word, word, word, example_quiz_json)
						# resp = call_doubao_pro_128k(prompt)
						resp = call_doubao_pro_32k(prompt)
						# res = json.loads(resp["choices"][0]['message']['content'])
						res = json.loads(resp.replace("```json", "").replace("```", ""))
						print (res)
						if res["answer"][0] not in ["A", "B", "C", "D"]:
							res["answer"] = "B"
						else:
							res["answer"] = res["answer"][0]
						if res["options"][0].find("A.") == -1:
							res["options"][0] = "A. " + res["options"][0]
						if res["options"][1].find("B.") == -1:
							res["options"][1] = "B. " + res["options"][1]
						if res["options"][2].find("C.") == -1:
							res["options"][2] = "C. " + res["options"][2]
						if res["options"][3].find("D.") == -1:
							res["options"][3] = "D. " + res["options"][3]
						return res
					except Exception as e:
						print (str(e))
						continue
						
		return None
	
	def generate_quiz_zh_tiankong_v2(self, subtitle_file):
		# print (subtitle_file)
		
		subtitles = pysrt.open(subtitle_file)
		subtitle_list = list(subtitles)
		rd.shuffle(subtitle_list)

		example_quiz ={"question": "我跟朋友分别时会说____。", "options": ["再见", "再贝", "再兄", "再兑"], "answer": "再见", "explanation": "“再见”是人们在分别时常使用的礼貌用语，而“再贝”“再兄”“再兑”并不是正确的表达，不符合日常用语习惯，所以应选“再见”。"}
		example_quiz_json = json.dumps(example_quiz,  ensure_ascii=False)

		stop_words = ["哎", "了", "的", "地", "吧", "吗", "啊", "你", "我", "他", "您", "嗯", "我们", "你们", "他们"]
		# stop_words = []

		for sub in subtitle_list:
			# if len(sub.text) <= 5:
			# 	continue
			subtitle_text = sub.text.replace("\n", "。 ")
			seg_list = jieba.cut(subtitle_text, cut_all=False)
			text = " ".join(seg_list)
			for word in text.split(" "):
				if word in stop_words:
					continue
				if word in self.hsk_word_set:
					# print (word)
					prompt = "以下是一个示例：当给到一个##中文句子##：“我 跟 朋友 分别 时 会 说 再见。”，遮挡其中“再见”这个词之后，将该句子变成一个选择题目，其中“再见“是正确选项，而其余的要和正确选项有一些相似，但是非常不适合填在句子中。请注意！给我的结果需要按照如下的json格式： {}。这是一个##中文句子##：“{}”，遮挡其中“{}”这个词之后，将该句子变成一个选择题目，其中“{}“是正确选项，而其余的要和正确选项有一些相似，但是非常不适合填在句子中。请注意！给我的结果需要按照如下的json格式".format(example_quiz_json, text, word, word, word)
					# print (prompt)
					try:
						# resp = call_doubao_pro_128k(prompt)
						resp = call_doubao_pro_32k(prompt)
						content_ori = resp.replace("```json", "").replace("```", "")
						print (content_ori)
						res = json.loads(content_ori)
						print (res)
						rd.shuffle(res["options"])
						ans_list = ["A", "B", "C", "D"]
						for i in range(4):
							if res["options"][i] == res["answer"]:
								res["answer"] = "{} {}".format(ans_list[i], res["answer"])
								break
						
						if res["options"][0].find("A.") == -1:
							res["options"][0] = "A. " + res["options"][0]
						if res["options"][1].find("B.") == -1:
							res["options"][1] = "B. " + res["options"][1]
						if res["options"][2].find("C.") == -1:
							res["options"][2] = "C. " + res["options"][2]
						if res["options"][3].find("D.") == -1:
							res["options"][3] = "D. " + res["options"][3]
						# res["question"] = "下面是刚刚视频中出现过的句子，请根据视频内容，选择最合适的词填入空格处：\n{}".format(text)
						return res
					except Exception as e:
						print (str(e))
						continue
		return None
	
	def translate_zh_quiz(self, quiz, gen_ar=True, gen_en=True):
		# Quiz format: {"question": "以下哪项陈述是正确的？", "options": ["A. 张小姐找到了她的父母。", "B. 张淑妍怀孕了。", "C. 赵景轩是沈总的未婚妻。", "D. 苏医生没有钱治病。"], "answer": "B", "explanation": "根据文中内容，张淑妍确实怀孕了。文中提到'对了这是你的体检单确认怀孕'和'加叶我怀孕了'，可以确认张淑妍怀孕的事实。"}
		zh_text_list = []
		zh_text_list.append(quiz["question"])
		for i in range(4):
			option = quiz["options"][i]
			zh_text_list.append(option.replace("A. ", "").replace("B. ", "").replace("C. ", "").replace("D. ", ""))
		zh_text_list.append(quiz["explanation"])
		res = dict()
		if gen_ar:
			ar_text_list = translate_text2ar(zh_text_list, "ar")
			assert len(ar_text_list) == len(zh_text_list)
			res["ar_quiz"] = {"question": ar_text_list[0]["Translation"], "options": ["A. " + ar_text_list[1]["Translation"], "B. " + ar_text_list[2]["Translation"], "C. " + ar_text_list[3]["Translation"], "D. " + ar_text_list[4]["Translation"]], "answer": quiz["answer"], "explanation": ar_text_list[5]["Translation"]}
		if gen_en:
			en_text_list = translate_text2ar(zh_text_list, "en")
			assert len(en_text_list) == len(zh_text_list)
			res["en_quiz"] = {"question": en_text_list[0]["Translation"], "options": ["A. " + en_text_list[1]["Translation"], "B. " + en_text_list[2]["Translation"], "C. " + en_text_list[3]["Translation"], "D. " + en_text_list[4]["Translation"]], "answer": quiz["answer"], "explanation": en_text_list[5]["Translation"]}
		return res

	def split_srt_words(self, subtitle_file):
		subtitle_text = pysrt.open(subtitle_file).text.replace("\n", "。 ")
		seg_list = jieba.cut(subtitle_text, cut_all=False)
		# remove Chinese punctuation
		seg_list = [word for word in seg_list if word not in punctuation]
		seg_list = [word for word in seg_list if word not in string.punctuation]
		seg_list = [word for word in seg_list if word.strip() != ""]

		clean_subtitle_text = " ".join(seg_list)

		if len(clean_subtitle_text) > 2000:
			clean_subtitle_text = clean_subtitle_text[0:2000]
		prompt = "#要求:去掉下面已经分词后的中文内容中的人名、地名。并将剩下的内容按照python的list格式返回。 #中文内容：{}".format(clean_subtitle_text)
		# data = {"sysinfo": "你是一个资深的中文老师，知道一个中文词是否是人名、地名、专有名词。", "prompt": ""}
		# url = "http://10.202.196.9:8087/call_qwen25_7b"
		# data["prompt"] = prompt
		
		# response = requests.post(url, data=data)
		# llm_input = json.loads(response.text)["text"]
		# resp = post_http_request(prompt=llm_input, api_url="http://10.202.196.9:6679/generate", seed=1234)
		# tag_text = json.loads(resp.text)["text"][0]

		resp = call_doubao_pro_128k(prompt)
		tag_text = json.loads(resp["choices"][0]['message']['content'])
		
		print (tag_text)
		start_index = tag_text.rfind("[")
		end_index = tag_text.rfind("]")
		# print (tag_text[start_index:end_index+1])
		res = ast.literal_eval(tag_text[start_index:end_index+1].replace("\n", "").replace("\t", ""))
		return res
	
	def compress_video(self, video_file, compressed_video_file, too_high_resolution=480, compress_ratio=1.2):
		if too_high_resolution == 480:
			multi_resolution = 480 * 720
		
		w, h = get_video_resolution(video_file)
		if w * h > multi_resolution:
			reduce_ratio = float(w * h) / float(multi_resolution)
			reduce_ratio = math.sqrt(reduce_ratio)
			assert reduce_ratio > 1
			reduce_ratio = reduce_ratio * compress_ratio
			new_w = int(w / reduce_ratio)
			if new_w % 2 != 0:
				new_w += 1
			new_h = int(h / reduce_ratio)
			if new_h % 2 != 0:
				new_h += 1
			cmd = "/opt/homebrew/Cellar/ffmpeg/7.1_3/bin/ffmpeg -y -loglevel error -i {} -vf scale={}:{} {}".format(video_file.replace(" ", "\\ ").replace("&", "\\&"), new_w, new_h, compressed_video_file.replace(" ", "\\ "))
			os.system(cmd)
			return True
		return  False

	def chunk_video(self, video_file, zh_srt, chunk_dir, chunk_dur=60, discard_dur=5):
		# 按照chunk和字幕对长视频进行切分, 默认chunk时长为1 分钟
		zh_subtitles = pysrt.open(zh_srt)
		chunk_list = list()
		pre_start_second = 0
		pre_idx = 0
		for idx, zh_sub in enumerate(zh_subtitles):
			# start_time = zh_sub.start
			# start_second = start_time.hours * 3600 + start_time.minutes * 60 + start_time.seconds + start_time.milliseconds / 1000.0
			end_time = zh_sub.end
			end_second = end_time.hours * 3600 + end_time.minutes * 60 + end_time.seconds + end_time.milliseconds / 1000.0
			if (end_second - pre_start_second) > chunk_dur:
				chunk_list.append({"sub_start_idx": pre_idx, "sub_end_idx": idx+1, "start_second": pre_start_second, "end_second": end_second + 0.5})
				pre_start_second = end_second
				pre_idx = idx + 1
		if chunk_list[-1]["sub_end_idx"] != len(zh_subtitles):
			end_time = zh_subtitles[-1].end
			end_second = end_time.hours * 3600 + end_time.minutes * 60 + end_time.seconds + end_time.milliseconds / 1000.0
			if (end_second - pre_start_second) > discard_dur:
				chunk_list.append({"sub_start_idx": pre_idx, "sub_end_idx": len(zh_subtitles), "start_second": pre_start_second, "end_second": end_second})
		
		
		video_name = video_file.split("/")[-1]
		en_srt = zh_srt.replace("Chinese", "English")
		ar_srt = zh_srt.replace("Chinese", "Arabic")
		py_srt = zh_srt.replace("Chinese", "Pinyin")
		
		def split_chunk(ori_srt, start_idx, end_idx, chunk_dir, chunk_idx):
			subs = pysrt.open(ori_srt)
			ori_srt_name = ori_srt.split("/")[-1]
			chunk_srt_file = os.path.join(chunk_dir, ori_srt_name.replace(".srt", "_chunk_{}.srt".format(chunk_idx)))
			subs[start_idx:end_idx].save(chunk_srt_file)
			return chunk_srt_file

		video = VideoFileClip(video_file)
		res = list()
		for chunk_idx, chunk in enumerate(chunk_list):
			chunk_file = os.path.join(chunk_dir, video_name.replace(".mp4", "_chunk_{}.mp4".format(chunk_idx)))
			chunk_video = video.subclip(chunk["start_second"], chunk["end_second"])
			chunk_video.write_videofile(chunk_file)
			zh_chunk_srt = split_chunk(zh_srt, chunk["sub_start_idx"], chunk["sub_end_idx"], chunk_dir, chunk_idx)
			en_chunk_srt = split_chunk(en_srt, chunk["sub_start_idx"], chunk["sub_end_idx"], chunk_dir, chunk_idx)
			ar_chunk_srt = split_chunk(ar_srt, chunk["sub_start_idx"], chunk["sub_end_idx"], chunk_dir, chunk_idx)
			py_chunk_srt = split_chunk(py_srt, chunk["sub_start_idx"], chunk["sub_end_idx"], chunk_dir, chunk_idx)
			split_chunk(py_srt, chunk["sub_start_idx"], chunk["sub_end_idx"], chunk_dir, chunk_idx)
			res.append({"video_file": chunk_file, "zh_srt": zh_chunk_srt, "en_srt": en_chunk_srt, "ar_srt": ar_chunk_srt, "py_srt": py_chunk_srt})
		return res
			
		

if __name__ == "__main__":
	video_processor = VideoProcessor()
	test_video = "testdir/test.mp4"
	zh_srt = "testdir/v0232eg10064ct6medaljhtabprl86pg_Chinese.srt"
	video_processor.chunk_video(test_video, zh_srt, "testdir/chunk_dir")


	# compress_videos("../video_info_huoshan.csv", "../video_info_huoshan_compressed.csv")
	

	# video_processor.compress_video("/Users/tal/work/lingtok_server/video_process/huoshan/短剧/8233-被偷走爱的那十年（43集）/1.mp4", "/Users/tal/work/lingtok_server/video_process/huoshan/短剧/8233-被偷走爱的那十年（43集）/1_compressed.mp4")
	
	# add_pinyin_srt("../video_info_huoshan.csv", "../video_info_huoshan_pinyin.csv")

	# video_processor.convert_zhsrt_to_pinyinsrt("/Users/tal/work/lingtok_server/video_process/huoshan/航拍中国/srt_dir/v0d32eg10064ct6nihiljht6nthfplmg_Chinese.srt", "test.srt")


	# file = "/Users/tal/work/沙特女子Demo/Dr. Asmac材料1/合成文本/lesson1-part1/yunyi.wav"
	# audio_text = "中国古代文学发端: 中国是世界文明古国，也是人类的发源地之一。中国到目前为止是世界上发现旧石器时代的人类化石和文化遗址最多的国家，其中重要的有元谋人、蓝田人、北京人、山顶洞人等。中国原始社会从公元前170万年到公元前21世纪。在中国古籍中，有不少关于艺术起源或原始艺术的记述。中国古籍一致认为文学艺术的起源很早。这些记述，揭示了诗歌乐舞与祭祀巫术的密切联系。"
	# srt_file = "/Users/tal/work/沙特女子Demo/Dr. Asmac材料1/合成文本/lesson1-part1/lesson1-part1"
	# video_processor.generate_zhsrt("test",srt_file, audio_path=file, gen_ar=True, gen_en=True, audio_text=audio_text)


	# update_quiz_metainfo("video_info_huoshan.csv", "video_metainfo_zhonly.jsonl")
	# translate_quiz_metainfo("video_metainfo_zhonly.jsonl", "video_metainfo.jsonl")
	# pass
	# update_quiz_metainfo("huoshan/短剧/8233-被偷走爱的那十年（43集）/video_metainfo.jsonl", "video_metainfo_ar_en.jsonl")


	# video_processor = VideoProcessor()
	# zh_quiz = {"question": "根据对话内容，以下哪项陈述是正确的？", "options": ["A. 张小姐找到了她的亲生父母。", "B. 赵景轩的妈妈来找沈总商量订婚的事。", "C. 沈家业和张淑妍已经离婚。", "D. 张淑妍和赵红红长得非常像。"], "answer": "B", "explanation": "根据对话内容，赵景轩的妈妈来找沈总商量订婚的事。对话中有提到'我听说赵景轩的妈妈来找沈总。商量订婚的事。'", "vid": "v0d32eg10064ct2sjjqljht60aaulia0"}
	# print (video_processor.translate_zh_quiz(zh_quiz, gen_ar=True, gen_en=True))
	# print (video_processor.split_srt_words("/Users/tal/work/lingtok_server/video_process/huoshan/短剧/8233-被偷走爱的那十年（43集）/srt_dir/v0d32eg10064ct2sjmaljht0lktqi3lg_Chinese.srt"))
	# vid = "v0332eg10064csvclgqljhtacamhhvu0"
	# playurl = get_vid_playurl(vid)
	# print (playurl)
	# video_processor = VideoProcessor()
	# video_processor.generate_zhsrt(playurl, "huoshan/v0332eg10064csvclgqljhtacamhhvu0", gen_ar=True)


	# page_url = sys.argv[1]
	# srt_name = sys.argv[2]
	# page_url = "http://www.lingotok.com/d313876e469a4550be3135ca7e2e56f3.mp4?auth_key=1732076613-2add8fadd9a84149a0f80a3cc71217be-0-c593db23b69704dacbcad66c0ecb021c"
	# srt_name = "hls.srt"
	# video_processor = VideoProcessor()
	# static_url, play_url_dict =  zhihu_url_convert(page_url)
	# video_processor.generate_srt(page_url, srt_name, gen_ar=True, gen_zh=False)
	# data = {"sysinfo": "You are an experienced English teacher who can differentiate the difficulty of a piece of English content by its vocabulary and grammatical content.", "prompt": ""}
	# url = "http://10.202.196.9:8087/call_qwen25_7b"



	# video_processor = VideoProcessor()
	# video_processor.load_srt("video_Finished/4/4_English.srt")
	# video_processor.generate_quiz()
	# print (video_processor.judge_srt_level())
	# text = video_processor.get_srt_text().replace("\n", " ")
	# prompt = "#要求：请从词汇、语法的角度对下面的英文文本内容的难度进行CEFR分类（A1、A2、B1、B2、C1、C2）。在给出分类结果之前需要说明原因，原因前后用<reason>，分类结果前后用<res>。\n#以下是一个示例，请仿照示例完成任务:\n"
	# prompt = "#Requirements: Please classify the listening comprehension of the following English text into CEFR categories (A1, A2, B1, B2, C1, C2) from the perspective of vocabulary and grammar. The reason needs to be explained before giving the classification result. The reason is wrapped with <reason>, and the classification result is wrapped with <res>.\n#English Text: {}".format(text)
	# shots = "示例1: \n #英文文本: \nYou wanted to come this time. Little sis! Big sis! You were gone forever. It was 3 days. Where are we going? Before Molly stole Tiffany's heart? Ancestors wanted to connect our island to all the people of the entire ocean. It's my job as a leaf finder to finish what they started. I wanna show how people just how far we'll go. \n #输出：\n<reason>从词汇角度来看，文本中使用了一些简单的词汇，如“you”、“want”、“to”、“come”等，这些词汇对于大多数英语学习者来说都是基础词汇。从语法角度来看，文本中的句子结构相对简单，没有复杂的从句或长句，大多数句子都是短句，易于理解。因此，这段文本的难度较低。<res>容易\n#输出结束\n"
	# llm_input = "{} {} #英文/文本：{}".format(prompt, shots, text)
	# data["prompt"] = prompt
	# response = requests.post(url, data=data)
	# llm_input = json.loads(response.text)["text"]

	# print (llm_input)
	# resp = post_http_request(prompt=llm_input, api_url="http://10.202.196.9:6679/generate", seed=1234)
	# print (resp.text)
