import pandas as pd
import json
import os
from video_process.vod_huoshan_util import get_vid_playurl
from copy import deepcopy


class Recommender:
    def __init__(self):
        self.df = pd.read_csv("video_info_huoshan.csv")
        self.video_info = self.df.to_dict(orient="list")
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
        self.recommended_video_count = 5
        
        
    
    def get_video_with_username(self, username):
        if not username in self.username_idx.keys():
            self.username_idx[username] = 0
        video_list = list()
        test_question = {"question": "这是一个测试的题目？", "options": ["A. 选项1", "B. 选项2", "C. 选线3", "D. 选项4"], "answer": "C", "ar_question": "هل هذا سؤال اختبار؟", "ar_options": ["A. خيارات 1", "B. خيارات 2", "C. خيارات 3", "D. خيارات 4"]}
        for idx in range(self.username_idx[username], self.username_idx[username]+self.recommended_video_count):
            i = idx % len(self.video_info['VID'])
            video_info = dict()
            video_info['vid'] = self.video_info['VID'][i]
            video_info['title'] = self.video_info['title'][i]
            video_info['srt_name'] = os.path.join("huoshan/srt_dir", self.video_info['zh_srt'][i].split("\\")[-1]).replace("/", "\\")
            video_info['ar_srt_name'] = os.path.join("huoshan/srt_dir", self.video_info['ar_srt'][i].split("\\")[-1]).replace("/", "\\")
            video_info['en_srt_name'] = os.path.join("huoshan/srt_dir", self.video_info['en_srt'][i].split("\\")[-1]).replace("/", "\\")
            video_info['play_url'] = get_vid_playurl(video_info['vid'])
            # video_info.update(test_question)
            if video_info['vid'] in self.video_quizd:
                video_info.update(self.video_quizd[video_info['vid']])
            video_list.append(video_info)
        self.username_idx[username] += self.recommended_video_count
        return video_list

if __name__ == "__main__":
    recommender = Recommender()
    print(recommender.get_video_with_username("test_user"))
            
