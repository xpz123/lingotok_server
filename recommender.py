import pandas as pd
import json
import os
from video_process.vod_huoshan_util import get_vid_playurl


class Recommender:
    def __init__(self):
        self.df = pd.read_csv("test_zh10_srt.csv")
        self.video_info = self.df.to_dict(orient="list")
        self.keys = list(self.video_info.keys())
        
        
    
    def get_video_with_username(self, username):
        video_list = list()
        test_question = {"question": "这是一个测试的题目？", "options": ["A. 选项1", "B. 选项2", "C. 选线3", "D. 选项4"], "answer": "C", "ar_question": "هل هذا سؤال اختبار؟", "ar_options": ["A. خيارات 1", "B. خيارات 2", "C. خيارات 3", "D. خيارات 4"]}
        for i in range(len(self.video_info['VID'])):
            video_info = dict()
            video_info['vid'] = self.video_info['VID'][i]
            video_info['title'] = self.video_info['title'][i]
            video_info['srt_name'] = self.video_info['zh_srt'][i]
            video_info['ar_srt_name'] = self.video_info['ar_srt'][i]
            video_info['play_url'] = get_vid_playurl(video_info['vid'])
            video_info.update(test_question)
            video_list.append(video_info)
        return video_list

if __name__ == "__main__":
    recommender = Recommender()
    print(recommender.get_video_with_username("test_user"))
            
