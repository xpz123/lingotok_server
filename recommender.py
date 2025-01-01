import pandas as pd
import json
import os
from video_process.vod_huoshan_util import get_vid_playurl
from copy import deepcopy
import random as rd


class Recommender:
    def __init__(self):
        if os.path.exists("video_info_hw_created_nona.csv"):
            df_ori = pd.read_csv("video_info_hw_created_nona.csv")
        else:
            df_ori = pd.read_csv("video_info_hw_created_new.csv")
        self.df = df_ori.dropna(subset=["video_id"])

        self.df_pnu = pd.read_csv("DR_1.csv")
        self.video_info_pnu = self.df_pnu.to_dict(orient="list")
        self.pnu_uuid_list = ["0QdJdJH6PJbdQyioviv0Q4i9Ac73"]

        # 0.3 is the threshold for audio_ratio
        self.audio_ratio_threshold = 0.3
        self.df = self.df[self.df["audio_ratio"] > self.audio_ratio_threshold]

        self.video_info = self.df.to_dict(orient="list")
        self.info_idx = [i for i in range(len(self.video_info['VID']))]
        rd.shuffle(self.info_idx)
        self.keys = list(self.video_info.keys())
        self.video_quizd = dict()
        lines = open("video_metainfo.jsonl").readlines()
        for l in lines:
            data = json.loads(l.strip())
            vid = data["vid"]
            self.video_quizd[vid] = deepcopy(data)
            if "explanation" in data.keys():
                self.video_quizd[vid]["explanation"] = data["explanation"]
        self.username_idx = dict()
        # self.recommended_video_count = 5


    def get_pnu_video_with_username(self, username, recommended_video_count=5):
        if not username in self.username_idx.keys():
            self.username_idx[username] = 0
        video_list = list()
        for idx in range(self.username_idx[username], self.username_idx[username]+recommended_video_count):
            try:
                video_info = dict()
                i = idx % len(self.video_info_pnu['vid'])
                video_info['vid'] = self.video_info_pnu['vid'][i]
                video_info['title'] = self.video_info_pnu['FileName'][i]
                video_info['srt_name'] = os.path.join("huoshan/srt_dir", self.video_info_pnu['zh_srt'][i].split("/")[-1]).replace("/", "\\")
                video_info['ar_srt_name'] = os.path.join("huoshan/srt_dir", self.video_info_pnu['ar_srt'][i].split("/")[-1]).replace("/", "\\")
                video_info['en_srt_name'] = os.path.join("huoshan/srt_dir", self.video_info_pnu['en_srt'][i].split("/")[-1]).replace("/", "\\")
                video_info['pinyin_srt_name'] = os.path.join("huoshan/srt_dir", self.video_info_pnu['pinyin_srt'][i].split("/")[-1]).replace("/", "\\")
                # video_info['play_url'] = get_vid_playurl(video_info['vid'])
                video_info['play_url'] = ""
                # video_info.update(test_question)
                if video_info['vid'] in self.video_quizd:
                    video_info.update(self.video_quizd[video_info['vid']])
                    video_info["question"] = "下面是刚刚视频中出现过的句子，请根据视频内容，选择最合适的词填入空格处：\n{}".format(video_info["question"])
                    video_info["ar_question"] = "هذه جملة ظهرت في الفيديو الذي شاهدته للتو، يرجى اختيار الكلمة الأنسب لتعبئة الفراغ:\n{}".format(video_info["ar_question"])
                    video_info["en_question"] = "The following sentence appeared in the video you just watched, please choose the most suitable word to fill in the blank:\n{}".format(video_info["en_question"])
                video_list.append(video_info)
            except:
                print ("error vid : {}".format(self.video_info_pnu['vid'][i]))
        self.username_idx[username] += recommended_video_count
        return video_list    
        
    def get_video_with_username(self, username, recommended_video_count=5):
        if username in self.pnu_uuid_list:
            return self.get_pnu_video_with_username(username)
        if not username in self.username_idx.keys():
            self.username_idx[username] = 0
        video_list = list()
        test_question = {"question": "这是一个测试的题目？", "options": ["A. 选项1", "B. 选项2", "C. 选线3", "D. 选项4"], "answer": "C", "ar_question": "هل هذا سؤال اختبار؟", "ar_options": ["A. خيارات 1", "B. خيارات 2", "C. خيارات 3", "D. خيارات 4"]}
        for idx in range(self.username_idx[username], self.username_idx[username]+recommended_video_count):
            try:
                i = self.info_idx[idx % len(self.video_info['VID'])]
                video_info = dict()
                video_info['vid'] = self.video_info['VID'][i]
                video_info['title'] = self.video_info['title'][i]
                video_info['srt_name'] = os.path.join("huoshan/srt_dir", self.video_info['zh_srt'][i].split("\\")[-1]).replace("/", "\\")
                video_info['ar_srt_name'] = os.path.join("huoshan/srt_dir", self.video_info['ar_srt'][i].split("\\")[-1]).replace("/", "\\")
                video_info['en_srt_name'] = os.path.join("huoshan/srt_dir", self.video_info['en_srt'][i].split("\\")[-1]).replace("/", "\\")
                video_info['pinyin_srt_name'] = os.path.join("huoshan/srt_dir", self.video_info['pinyin_srt'][i].split("\\")[-1]).replace("/", "\\")
                # video_info['play_url'] = get_vid_playurl(video_info['vid'])
                video_info['play_url'] = ""
                video_info["video_id"] = self.video_info['video_id'][i]
                # video_info.update(test_question)
                if video_info['vid'] in self.video_quizd:
                    video_info.update(self.video_quizd[video_info['vid']])
                    video_info["question"] = "下面是刚刚视频中出现过的句子，请根据视频内容，选择最合适的词填入空格处：\n{}".format(video_info["question"])
                    video_info["ar_question"] = "هذه جملة ظهرت في الفيديو الذي شاهدته للتو، يرجى اختيار الكلمة الأنسب لتعبئة الفراغ:\n{}".format(video_info["ar_question"])
                    video_info["en_question"] = "The following sentence appeared in the video you just watched, please choose the most suitable word to fill in the blank:\n{}".format(video_info["en_question"])
                video_list.append(video_info)
            except:
                print ("error vid : {}".format(self.video_info['VID'][i]))
        self.username_idx[username] += recommended_video_count
        return video_list
    def update_video_info(self, video_info):
        video_id = str(video_info["video_id"])
        if len(video_id) < 8:
            return
        if self.df[self.df["video_id"] == video_id].shape[0] == 0:
            new_line = pd.DataFrame([video_info])
            self.df = pd.concat([self.df, new_line], ignore_index=True)
            self.df.to_csv("video_info_hw_created_nona.csv", index=False)


if __name__ == "__main__":
    recommender = Recommender()
    print(recommender.get_video_with_username("test_user"))
            
