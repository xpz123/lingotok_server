from redis.asyncio import Redis, from_url

import asyncio

import ujson
import random as rd
import time
import logging
import logging.handlers
import os
from recaller import LatestRecaller, CustomizedRecaller, ContinuousRecaller, SeriesRecaller, LevelRecaller, RandomRecaller, PopRecaller, LevelInterestRecaller

async def parallel_recall(recaller_dict, input_data):
    """并行调用多个recaller
    
    Args:
        recallers (List[Recaller]): Recaller实例列表
        input_data: 输入数据
    
    Returns:
        Dict[str, List]: 每个recaller的召回结果，key为recaller类名
    """
    async def _recall_with_name(recaller_name, recaller):
        result = await recaller.recall(input_data)
        return recaller_name, result
    
    results = await asyncio.gather(*[_recall_with_name(r, recaller_dict[r]) for r in recaller_dict])
    return dict(results)

class RecommenderCtx:
    def __init__(self):
        self.input_data = None
        self.user_behavior_info = None
        self.recent_watch_videoid_set = None
        self.recent_watch_videoid_time = None
        self.user_status = None
        self.recommended_size = 5
        self.recommended_videoids = []
        self.ready_rtn = False

        self.recall_result_dict = {}
        
        self.rank_result = []

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
        self.newcommer_recall_counts = 20

    async def check_user_status(self, user_behavior_info):
        # User status:
        # 0 : 全新用户，没有观看记录
        # 1: 新用户，观看视频数量小于10个
        # 2: 老用户，观看数量超过10个
        recent_watch_video_list = user_behavior_info.recent_watch_video_list
        if recent_watch_video_list is None:
            return 0
        if len(recent_watch_video_list) == 0:
            return 0
        if len(recent_watch_video_list) < 20:
            return 1
        return 2
    async def action(self, recommender_ctx):
        user_status = await self.check_user_status(recommender_ctx.user_behavior_info)
        recommender_ctx.user_status = user_status
        if user_status == 0:
            recommender_ctx.recommended_videoids = self.newcomer_video_ids
            rd.shuffle(recommender_ctx.recommended_videoids)
            recommender_ctx.ready_rtn = True
            return True
        
        return True


class Ranker:
    def __init__(self):
        self.newcommer_recall_counts = 20
        self.primary_ratio = 0.6
        self.pop_ratio = 0.2
        self.random_ratio = 0.2

    def newcomer_rank_strategy(self, recommender_ctx):
        # 60% level_interests  40% pop lefted level
        level_interest_recall_video_ids = recommender_ctx.recall_result_dict["level_interest"]
        level_recall_video_ids = recommender_ctx.recall_result_dict["level"]
        pop_recall_video_ids = recommender_ctx.recall_result_dict["pop"]
        recall_video_ids = level_interest_recall_video_ids[:int(0.6*self.newcommer_recall_counts)] + pop_recall_video_ids[:int(0.4*self.newcommer_recall_counts)]
        level_recall_count = self.newcommer_recall_counts - len(recall_video_ids)
        recall_video_ids += level_recall_video_ids[:level_recall_count]
        recommender_ctx.rank_result = recall_video_ids

    async def rank(self, recommender_ctx):
        # Process customized videos
        if len(recommender_ctx.recall_result_dict["customized"]) != 0:
            recommender_ctx.rank_result = recommender_ctx.recall_result_dict["customized"]
            rd.shuffle(recommender_ctx.rank_result)
            return
        # Newcomer rank strategy
        if recommender_ctx.user_status == 1:
            self.newcomer_rank_strategy(recommender_ctx)
        else:
            recommender_ctx.rank_result += recommender_ctx.recall_result_dict["level"][:(int(recommender_ctx.size * self.primary_ratio))]
            recommender_ctx.rank_result += recommender_ctx.recall_result_dict["pop"][:(int(recommender_ctx.size * self.pop_ratio))]
            recommender_ctx.rank_result += recommender_ctx.recall_result_dict["random"][:(int(recommender_ctx.size * self.random_ratio))]

        if len(recommender_ctx.rank_result) < recommender_ctx.size:
            recommender_ctx.rank_result += recommender_ctx.recall_result_dict["random"][(int(recommender_ctx.size * self.random_ratio)):]
        
        recommender_ctx.rank_result = list(set(recommender_ctx.rank_result))
        rd.shuffle(recommender_ctx.rank_result)

class ReRanker:
    def __init__(self):
        pass
    async def rerank(self, recommender_ctx):
        watched_video_list = []
        not_watched_video_list = []
        watched_video_list_withtime = list()
        for video_id in recommender_ctx.rank_result:
            if video_id in recommender_ctx.recent_watch_videoid_set:
                watched_video_list.append(video_id)
                watched_video_list_withtime.append((video_id, recommender_ctx.recent_watched_videoid_time.get(video_id, -1)))
            else:
                not_watched_video_list.append(video_id)
        # watched_video_list = list(reversed(watched_video_list))
        rd.shuffle(watched_video_list)
        sorted_watched_video_list_withtime =sorted(watched_video_list_withtime, key=lambda x: x[1])
        sorted_watched_video_list = [item[0] for item in sorted_watched_video_list_withtime]

        recommender_ctx.rerank_result = not_watched_video_list + sorted_watched_video_list
    

class RecommenderV2_0:
    def __init__(self):
        # self.recaller_dict = {"latest": LatestRecaller(), "customized": CustomizedRecaller(), "continuous": ContinuousRecaller(), "series": SeriesRecaller(), "level": LevelRecaller(), "random": RandomRecaller(), "pop": PopRecaller(), "level_interest": LevelInterestRecaller()}
        self.recaller_dict = {"customized": CustomizedRecaller(), "level": LevelRecaller(), "random": RandomRecaller(), "pop": PopRecaller(), "level_interest": LevelInterestRecaller()}
        self.prerecall_strategy = PrerecallStrategy()
        self.ranker = Ranker()
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
        recommender_ctx.input_data = input_data
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
        await self.prerecall_strategy.action(recommender_ctx)

        # Check if the pre-recall strategy is ready to return, only for Open-Screen Recommendation
        if recommender_ctx.is_ok_rtn():
            return recommender_ctx.recommended_videoids

        recommender_ctx.recall_result_dict = {}


        # 创建所有recaller的协程任务
        tasks = []
        recaller_names = list(self.recaller_dict.keys())
        for recaller_name in recaller_names:
            # 为每个recaller添加超时控制
            task = asyncio.create_task(
                asyncio.wait_for(
                    self.recaller_dict[recaller_name].recall(input_data),
                    timeout=30.0  # 设置30秒超时
                )
            )
            tasks.append(task)

        # 并行执行所有任务
        try:
            recall_results = await asyncio.gather(*tasks, return_exceptions=True)
        except asyncio.TimeoutError:
            logging.error(f"Some recallers timed out for req_id: {req_id}")

        # 处理结果
        assert len(recall_results) == len(recaller_names)
        for i in range(len(recall_results)):
            recall_result = recall_results[i]
            recaller_name = recaller_names[i]
            try:
                # Avoid the recall_result is not a list
                if type(recall_result) != list:
                    continue

                if recaller_name == "customized":
                    if len(recall_result) != 0:
                        # process customized only, rerank customized videos with recent watch videos
                        while len(recall_result) < size:
                            recall_result += recall_result
                        recommender_ctx.recall_result_dict["customized"] = recall_result
                        break
                    else:
                        recommender_ctx.recall_result_dict["customized"] = []
                else:
                    recommender_ctx.recall_result_dict[recaller_name] = recall_result

            except Exception as e:
                print(str(e))
                recommender_ctx.recall_result_dict[recaller_name] = []

        
        # Rank
        await self.ranker.rank(recommender_ctx)

        # Rerank
        await self.rerank_strategy.rerank(recommender_ctx)

        return recommender_ctx.rerank_result[:size]


if __name__ == "__main__":
    pass
    # asyncio.run(get_redis("latest_videos"))
    # pass
    # latest_videos = json.loads(yepzan_redis.get("latest_videos"))
    # series_videos_ = json.loads(yepzan_redis.get("series_videos_新东方比邻国际中文"))