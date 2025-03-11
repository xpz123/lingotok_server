from redis.asyncio import Redis, from_url

import asyncio

import ujson
import random as rd
import time
import logging
import logging.handlers
import os
from recaller import LatestRecaller, CustomizedRecaller, ContinuousRecaller, SeriesRecaller, LevelRecaller, RandomRecaller, PopRecaller

class RecommenderCtx:
    def __init__(self):
        self.user_behavior_info = None
        self.recent_watch_videoid_set = None
        self.recent_watch_videoid_time = None
        self.user_status = None
        self.recommended_size = 5
        self.recommended_videoids = []
        self.ready_rtn = False

        self.recall_result = None
        
        self.rank_result = None

        self.rerank_result = None
    
    def is_ok_rtn(self):
        if self.ready_rtn and len(self.recommended_videoids) != 0:
            return True
        else:
            return False

class PrerecallStrategy:
    def __init__(self):
        # 开屏建议视频：山、一、你好、再见、欢迎
        self.newcomer_video_ids = ["67b4a9bc0efa0542fbfe3d9f", "67b4a9cd0efa0542fbfe3dab", "6796909d4201172bc75ffe31", "67a0ff1a3f4ca0bb2aa31126", "67a0ff253f4ca0bb2aa3112a"]

    def check_user_status(self, user_behavior_info):
        # User status:
        # 0 : 全新用户，没有观看记录
        # 1: 新用户，观看视频数量小于10个
        # 2: 老用户，观看数量超过10个
        recent_watch_video_list = user_behavior_info.recent_watch_video_list
        if recent_watch_video_list is None:
            return 0
        if len(recent_watch_video_list) == 0:
            return 0
        if len(recent_watch_video_list) < 10:
            return 1
        return 2
    def action(self, recommender_ctx):
        user_status = self.check_user_status(recommender_ctx.user_behavior_info)
        recommender_ctx.user_status = user_status
        if user_status == 0:
            recommender_ctx.recommended_videoids = self.newcomer_video_ids
            rd.shuffle(recommender_ctx.recommended_videoids)
            recommender_ctx.ready_rtn = True
            return True
        return True


class Ranker:
    def __init__(self):
        pass
    def rank(self, ):
        return {}

class ReRanker:
    def __init__(self):
        pass
    def rerank(self, recommender_ctx):
        watched_video_list = []
        not_watched_video_list = []
        watched_video_list_withtime = list()
        for video in recommender_ctx.rank_result:
            if video["id"] in recommender_ctx.recent_watch_videoid_set:
                watched_video_list.append(video)
                watched_video_list_withtime.append((video, recommender_ctx.recent_watched_videoid_time.get(video["id"], -1)))
            else:
                not_watched_video_list.append(video)
        # watched_video_list = list(reversed(watched_video_list))
        rd.shuffle(watched_video_list)
        sorted_watched_video_list_withtime =sorted(watched_video_list_withtime, key=lambda x: x[1])
        sorted_watched_video_list = [item[0] for item in sorted_watched_video_list_withtime]

        recommender_ctx.rerank_result = not_watched_video_list + sorted_watched_video_list
    

class RecommenderV2_0:
    def __init__(self):
        self.recaller_dict = {"latest": LatestRecaller(), "customized": CustomizedRecaller(), "continuous": ContinuousRecaller(), "series": SeriesRecaller(), "level": LevelRecaller(), "random": RandomRecaller(), "pop": PopRecaller()}
        self.prerecall_strategy = PrerecallStrategy()
        self.rerank_strategy = ReRanker()
        # self.ranker = Ranker()
        self.latest_ratio = 0.2
        self.primary_ratio = 0.4
        self.random_ratio = 0.2
    
    def fetch_recent_watched_videos(self, user_behavior_info):
        recent_watch_videoid_set = set()
        recent_watch_video_list = user_behavior_info.recent_watch_video_list
        if not recent_watch_video_list is None:
            for video in recent_watch_video_list:
                recent_watch_videoid_set.add(video.video_info.video_id)
        like_video_list = user_behavior_info.recent_like_video_list
        if not like_video_list is None:
            for video in like_video_list:
                recent_watch_videoid_set.add(video.video_info.video_id)
        favorite_video_list = user_behavior_info.recent_favorite_video_list
        if not favorite_video_list is None:
            for video in favorite_video_list:
                recent_watch_videoid_set.add(video.video_info.video_id)
        return recent_watch_videoid_set
    
    def fetch_recent_watched_videos_time(self, user_behavior_info):
        recent_watched_videoid_time = dict()
        recent_watch_video_list = user_behavior_info.recent_watch_video_list
        if not recent_watch_video_list is None:
            for video in recent_watch_video_list:
                if not video.watch_time is None:
                    recent_watched_videoid_time[video.video_info.video_id] = video.watch_time
        return recent_watched_videoid_time

    

    async def recommend(self, input_data):
        recommender_ctx = RecommenderCtx()
        size = input_data.size
        size = min(20, size)

        recommender_ctx.size = size
        req_id = input_data.req_id

        user_behavior_info = input_data.user_behavior_info

        # Fetch recent watched video_id from recent_watch_video_list, recent_like_video_list, recent_favorite_video_list
        recommender_ctx.recent_watch_videoid_set = self.fetch_recent_watched_videos(user_behavior_info)
        recommender_ctx.recent_watched_videoid_time = self.fetch_recent_watched_videos_time(user_behavior_info)

        # Pre-Recall Strategy
        recommender_ctx.user_behavior_info = user_behavior_info
        self.prerecall_strategy.action(recommender_ctx)

        # Check if the pre-recall strategy is ready to return
        if recommender_ctx.is_ok_rtn():
            res = list()
            for vid in recommender_ctx.recommended_videoids[:recommender_ctx.recommended_size]:
                res.append({"id": vid, "title": ""})
            return res

        recall_result_dict = {}

        for recaller_name in self.recaller_dict.keys():
            try:
                if recaller_name == "customized":
                    recall_result = await self.recaller_dict[recaller_name].recall(input_data)
                    if len(recall_result) != 0:
                        # process customized only, rerank customized videos with recent watch videos
                        while len(recall_result) < size:
                            recall_result += recall_result
                        watched_video_list = []
                        not_watched_video_list = []
                        watched_video_list_withtime = list()
                        for video in recall_result:
                            if video["id"] in recommender_ctx.recent_watch_videoid_set:
                                watched_video_list.append(video)
                                watched_video_list_withtime.append((video, recommender_ctx.recent_watched_videoid_time.get(video["id"], -1)))
                            else:
                                not_watched_video_list.append(video)
                        # watched_video_list = list(reversed(watched_video_list))
                        rd.shuffle(watched_video_list)
                        sorted_watched_video_list_withtime =sorted(watched_video_list_withtime, key=lambda x: x[1])
                        sorted_watched_video_list = [item[0] for item in sorted_watched_video_list_withtime]
                        
                        recall_result_dict["customized"] = not_watched_video_list + sorted_watched_video_list
                        # import json
                        # print (json.dumps(recall_result_dict["customized"], ensure_ascii=False))
                        break
                    else:
                        recall_result_dict["customized"] = []
                else:
                    recall_result = await self.recaller_dict[recaller_name].recall(input_data)


                    filted_recall_result = []
                    for video in recall_result:
                        if video["id"] not in recommender_ctx.recent_watch_videoid_set:
                            filted_recall_result.append(video)
                    recall_result_dict[recaller_name] = filted_recall_result

            except Exception as e:
                print (str(e))
                recall_result_dict[recaller_name] = []
        recommender_ctx.recall_result = recall_result_dict

        rank_result = []

        rank_result += recall_result_dict["customized"]
        if len(rank_result) > size:
            # rd.shuffle(rank_result)
            return rank_result[:size]
        if input_data.user_info.level <= 1:
            rank_result += recall_result_dict["level"][:(int(size * self.primary_ratio))]
        rank_result += recall_result_dict["latest"][:(int(size * self.latest_ratio))]
        rank_result += recall_result_dict["random"][:(int(size * self.random_ratio))]
        rank_result += recall_result_dict["continuous"]

        if len(rank_result) < size:
            rank_result += recall_result_dict["series"]

        
        if len(rank_result) < size:
            rank_result += recall_result_dict["random"][(int(size * self.random_ratio)):]

        rd.shuffle(rank_result)
        recommender_ctx.rank_result = rank_result

        self.rerank_strategy(recommender_ctx)

        return recommender_ctx.rerank_result[:size]


if __name__ == "__main__":
    pass
    # asyncio.run(get_redis("latest_videos"))
    # pass
    # latest_videos = json.loads(yepzan_redis.get("latest_videos"))
    # series_videos_ = json.loads(yepzan_redis.get("series_videos_新东方比邻国际中文"))