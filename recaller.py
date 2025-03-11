from redis.asyncio import Redis, from_url

import asyncio

import ujson
import random as rd
import time
import logging
import logging.handlers
import os

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
        await redis_pool.close()

async def get_redis(key):
    """从全局连接池中获取 Redis 数据"""
    if redis_pool is None:
        raise RuntimeError("Redis pool is not initialized. Call init_redis_pool() first.")
    return await redis_pool.get(key)


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
            start_time = time.time()
            customize_videos_str = await get_redis("customize_videos_{}".format(invite_code))
            customize_videos = ujson.loads(customize_videos_str)
            end_time = time.time()
            # print ("latest get dur {}".format(end_time - start_time))
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
        self.recall_count = 10
    async def recall(self, input_data):
        # recall_videos = []
        user_level = input_data.user_info.level
        if user_level > 1:
            return []
        start_time = time.time()
        hsk_videos_str = await get_redis("customize_videos_HSK_DIY")
        hsk_videos = ujson.loads(hsk_videos_str)
        end_time = time.time()
        # print ("latest get dur {}".format(end_time - start_time))
        
        rd.shuffle(hsk_videos)
        return hsk_videos[:self.recall_count]
    
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
        self.recall_count = 10
    async def recall(self, input_data):
        random_videos_str = await get_redis("random_videos")
        all_videos = ujson.loads(random_videos_str)
        rd.shuffle(all_videos)
        return all_videos[:self.recall_count]

class PopRecaller(Recaller):
    def __init__(self):
        self.recall_count = 10
    async def recall(self, input_data):
        pop_video_ids = await get_redis("")
        all_videos = ujson.loads(pop_video_ids)
        rd.shuffle(all_videos)
        return all_videos[:self.recall_count]
