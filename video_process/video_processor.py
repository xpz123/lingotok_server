import requests
import pysrt
from openai import OpenAI
import json
from datetime import timedelta
from call_huoshan_srt import *
from translator import translate_text2ar
import sys

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


class VideoProcessor:
	def __init__(self):
		pass

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
	
	def generate_srt(self, play_url, file_name, gen_ar=False, gen_zh=False):
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
	
	def translate_srt(self, filename, gen_ar=True, gen_zh=True):
		res = {"en_srt": filename}
		en_srt_data = pysrt.open(res["en_srt"])
		
		if gen_ar:
			res["ar_srt"] = filename.replace("English", "Arbic")
		if gen_zh:
			res["zh_srt"] = filename.replace("English", "Chinese")
		
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
		url = "http://10.202.196.9:8088/call_qwen25_7b"
		data["prompt"] = prompt
		response = requests.post(url, data=data)
		llm_input = json.loads(response.text)["text"]
		resp = post_http_request(prompt=llm_input, api_url="http://10.202.196.9:6679/generate", seed=1234)
		tag_text = json.loads(resp.text)["text"][0]
		# print (tag_text)
		import pdb
		pdb.set_trace()

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



if __name__ == "__main__":
	pass
	# page_url = sys.argv[1]
	# srt_name = sys.argv[2]
	# video_processor = VideoProcessor()
	# static_url, play_url_dict =  zhihu_url_convert(page_url)
	# video_processor.generate_srt(play_url_dict["HD"], srt_name, gen_ar=True, gen_zh=True)
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
