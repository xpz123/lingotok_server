from redis.asyncio import Redis, from_url

import asyncio

import ujson
import random as rd
import time
import logging
import logging.handlers
import os
from util import online_interest_mapping

redis_pool = None
async def init_redis_pool():
    """初始化全局 Redis 连接池"""
    global redis_pool
    redis_pool = await from_url(
        "redis://192.168.0.120:6379",
        password='Lingotok123!',
        decode_responses=True
    )
    # redis_pool = await from_url(
    #     "redis://101.46.56.32:6379",
    #     password='Lingotok123!',
    #     decode_responses=True
    # )
    

async def close_redis_pool():
    if redis_pool:
        await redis_pool.aclose()

async def get_redis(key):
    """从全局连接池中获取 Redis 数据"""
    if redis_pool is None:
        raise RuntimeError("Redis pool is not initialized. Call init_redis_pool() first.")
    return await redis_pool.get(key)


async def zrange_redis(key, start, end):
    """从全局连接池中获取 Redis zset数据"""
    if redis_pool is None:
        raise RuntimeError("Redis pool is not initialized. Call init_redis_pool() first.")
    return await redis_pool.zrange(key, start, end, withscores=True)

async def lrange_redis(key, start, end):
    """从全局连接池中获取 Redis list数据"""
    if redis_pool is None:
        raise RuntimeError("Redis pool is not initialized. Call init_redis_pool() first.")
    return await redis_pool.lrange(key, start, end)

class Recaller:
    def __init__(self):
        pass
    async def recall(self, input_data):
        return {}
class LatestRecaller(Recaller):
    def __init__(self):
        self.recall_count = 10
    async def recall(self, input_data):
        # start_time = time.time()
        redis_data = await get_redis("latest_videos")
        latest_videos = ujson.loads(redis_data)
        # end_time = time.time()
        rd.shuffle(latest_videos)
        return latest_videos[:self.recall_count]
    
class CustomizedRecaller(Recaller):
    def __init__(self):
        self.recall_count = 10
    async def recall(self, input_data):
        basic_user_info = input_data.user_info
        invite_code = basic_user_info.invite_code
        try:
            customize_videos = await lrange_redis("video_customize-{}".format(invite_code), 0, -1)
            rd.shuffle(customize_videos)
            return customize_videos
        except:
            return []

class ContinuousRecaller(Recaller):
    def __init__(self):
        self.recall_count = 10
    async def recall(self, input_data):
        recall_videos = []
        user_behavior_info = input_data.user_behavior_info
        recent_watch_video_list = user_behavior_info.recent_watch_video_list
        if recent_watch_video_list is None:
            recent_watch_video_list = []
        for watched_video in recent_watch_video_list:
            if watched_video.watch_complete:
                series_videos_str = await get_redis("series_videos_{}".format(watched_video.video_info.series_name))
                series_videos = ujson.loads(series_videos_str)
                if len(series_videos) < (watched_video.video_info.series_sequence + 1):
                    recall_videos.append(series_videos[watched_video.video_info.series_sequence + 1])
        
        recent_like_list = user_behavior_info.recent_like_video_list
        if recent_like_list is None:
            recent_like_list = []
        recent_favorite_list = user_behavior_info.recent_favorite_video_list
        if recent_favorite_list is None:
            recent_favorite_list = []
        like_favorite_video_list = recent_like_list + recent_favorite_list
        for video in like_favorite_video_list:
            series_videos_str = await get_redis("series_videos_{}".format(video.video_info.series_name))
            series_videos = ujson.loads(series_videos_str)
            if len(series_videos) < (video.video_info.series_sequence + 1):
                recall_videos.append(series_videos[video.video_info.series_sequence + 1])
        
        rd.shuffle(recall_videos)
        return recall_videos[:self.recall_count]
    
class LevelRecaller(Recaller):
    def __init__(self):
        self.recall_from_series_count = 3
        self.recall_count = 20
        self.max_each_series_count = 10
    async def recall(self, input_data):
        # recall_videos = []
        user_level = input_data.user_info.level
        level_prefix = "video_series_level-"
        # user_level <= 1时， 从redis获取入门、初学系列
        recall_series = []
        if user_level <= 1:
            key = "{}{}".format(level_prefix, "入门")
            recall_series += await lrange_redis(key, 0, -1)
            key = "{}{}".format(level_prefix, "初学")
            recall_series += await lrange_redis(key, 0, -1)
            
        elif user_level == 2:
            key = "{}{}".format(level_prefix, "中等")
            recall_series = await lrange_redis(key, 0, -1)
        else:
            key = "{}{}".format(level_prefix, "难")
            recall_series = await lrange_redis(key, 0, -1)
        rd.shuffle(recall_series)
        recall_videos = []
        for i in range(min(len(recall_series), self.recall_from_series_count)):
            recall_videos += await lrange_redis("video_series-{}".format(recall_series[i]), 0, -1)
        rd.shuffle(recall_videos)
        return recall_videos[:self.recall_count]
    
class LevelInterestRecaller(Recaller):
    def __init__(self):
        self.recall_count = 20
        self.recall_from_series_count = 3
        self.level_interest_prefix = "video_series_interest_level-"
    async def recall(self, input_data):
        user_level = input_data.user_info.level
        user_interests = input_data.user_info.interests
        if user_interests is None:
            return []
        if len(user_interests) == 0:
            return []
        recall_series = []
        if user_level <= 1:
            for app_interest in user_interests:
                for interest in online_interest_mapping(app_interest):
                    key = "{}{}_{}".format(self.level_interest_prefix, interest, "入门")
                    recall_series += await lrange_redis(key, 0, -1)
                    key = "{}{}_{}".format(self.level_interest_prefix, interest, "初学")
                    recall_series += await lrange_redis(key, 0, -1)
            
        elif user_level == 2:
            for app_interest in user_interests:
                for interest in online_interest_mapping(app_interest):
                    key = "{}{}_{}".format(self.level_interest_prefix, interest, "中等")
                    recall_series = await lrange_redis(key, 0, -1)
        else:
            for app_interest in user_interests:
                for interest in online_interest_mapping(app_interest):
                    key = "{}{}_{}".format(self.level_interest_prefix, interest, "难")
                    recall_series = await lrange_redis(key, 0, -1)
        rd.shuffle(recall_series)
        recall_videos = []
        for i in range(min(len(recall_series), self.recall_from_series_count)):
            recall_videos += await lrange_redis("video_series-{}".format(recall_series[i]), 0, -1)
        rd.shuffle(recall_videos)
        return recall_videos[:self.recall_count]
        
    
class SeriesRecaller(Recaller):
    def __init__(self):
        self.recall_count = 10
        self.series_recall_count = 5
    async def recall(self, input_data):
        recall_videos = []
        user_behavior_info = input_data.user_behavior_info
        recent_watch_video_list = user_behavior_info.recent_watch_video_list
        if recent_watch_video_list is None:
            recent_watch_video_list = []
        for watched_video in recent_watch_video_list:
            if watched_video.watch_complete:
                series_videos_str = await get_redis("series_videos_{}".format(watched_video.video_info.series_name))
                series_videos = ujson.loads(series_videos_str)
                rd.shuffle(series_videos)
                recall_videos += series_videos[:self.series_recall_count]
        
        recent_like_list = user_behavior_info.recent_like_video_list
        if recent_like_list is None:
            recent_like_list = []
        recent_favorite_list = user_behavior_info.recent_favorite_video_list
        if recent_favorite_list is None:
            recent_favorite_list = []
        like_favorite_video_list = recent_like_list + recent_favorite_list
        for video in like_favorite_video_list:
            series_videos_str = await get_redis("series_videos_{}".format(video.video_info.series_name))
            series_videos = ujson.loads(series_videos_str)
            rd.shuffle(series_videos)
            recall_videos += series_videos[:self.series_recall_count]
        
        rd.shuffle(recall_videos)
        return recall_videos[:self.recall_count]

class RandomRecaller(Recaller):
    def __init__(self):
        self.recall_count = 20
    async def recall(self, input_data):
        random_video_ids = await lrange_redis("video_random", 0, -1)
        rd.shuffle(random_video_ids)
        return random_video_ids[:self.recall_count]

class PopRecaller(Recaller):
    def __init__(self):
        self.max_redis_count = 100
        self.recall_count = 50
    async def recall(self, input_data):
        pop_video_ids_list = await zrange_redis("video_pop", 0, self.max_redis_count)
        pop_video_ids = [item[0] for item in pop_video_ids_list]
        rd.shuffle(pop_video_ids)
        return pop_video_ids[:self.recall_count]



if __name__ == "__main__":
    # key = "video_series_level-初级"
    # key = "video_series-pnu2"
    # key = "video_pop"
    # key = "video_series_level-入门"
    # key = "video_series-pnu1"
    key = "video_series_interest_level-旅行_难"
    # key = "video_customize-KAU777"
    async def main():
        await init_redis_pool()  # 首先需要初始化redis连接
        recall_series = await lrange_redis(key, 0, -1)
        print(recall_series)
        # recall_videos = await zrange_redis(key, 0, -1)
        
        await close_redis_pool()  # 最后关闭redis连接
    
    asyncio.run(main())