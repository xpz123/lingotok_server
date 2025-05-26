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

async def get_redis_concurrent(keys, max_concurrent=10):
    """并发从全局连接池中获取多个 Redis 数据
    
    Args:
        keys: 要获取的键列表
        max_concurrent: 最大并发数，默认10
    
    Returns:
        dict: 键值对字典，key为输入的键，value为对应的Redis值
    """
    if redis_pool is None:
        raise RuntimeError("Redis pool is not initialized. Call init_redis_pool() first.")
    
    async def fetch_batch(batch_keys):
        tasks = [redis_pool.get(key) for key in batch_keys]
        results = await asyncio.gather(*tasks)
        return dict(zip(batch_keys, results))
    
    results = {}
    for i in range(0, len(keys), max_concurrent):
        batch = keys[i:i + max_concurrent]
        batch_results = await fetch_batch(batch)
        results.update(batch_results)
    
    return results

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
    async def recall(self, recommender_ctx):
        return {}
class LatestRecaller(Recaller):
    def __init__(self):
        self.recall_count = 10
    async def recall(self, recommender_ctx):
        # start_time = time.time()
        redis_data = await get_redis("latest_videos")
        latest_videos = ujson.loads(redis_data)
        # end_time = time.time()
        rd.shuffle(latest_videos)
        return latest_videos[:self.recall_count]
    
class CustomizedRecaller(Recaller):
    def __init__(self):
        self.recall_count = 10
    async def recall(self, recommender_ctx):
        basic_user_info = recommender_ctx.user_profile_ctx.user_basic_info
        invite_code = basic_user_info.invite_code
        if invite_code == None:
            return []
        if invite_code.strip() == "":
            return []
        try:
            customize_videos = await lrange_redis("video_customize-{}".format(invite_code), 0, -1)
            rd.shuffle(customize_videos)
            return customize_videos
        except:
            return []

# class ContinuousRecaller(Recaller):
#     def __init__(self):
#         self.recall_count = 10
#     async def recall(self, recommender_ctx):
#         recall_videos = []
#         user_behavior_info = recommender_ctx.user_profile_ctx.user_behavior_info
#         recent_watch_video_list = user_behavior_info.recent_watch_videoid_list
#         if recent_watch_video_list is None:
#             recent_watch_video_list = []
#         for watched_video in recent_watch_video_list:
#             if watched_video.watch_complete:
#                 series_videos_str = await get_redis("series_videos_{}".format(watched_video.video_info.series_name))
#                 series_videos = ujson.loads(series_videos_str)
#                 if len(series_videos) < (watched_video.video_info.series_sequence + 1):
#                     recall_videos.append(series_videos[watched_video.video_info.series_sequence + 1])
        
#         recent_like_list = user_behavior_info.recent_like_video_list
#         if recent_like_list is None:
#             recent_like_list = []
#         recent_favorite_list = user_behavior_info.recent_favorite_video_list
#         if recent_favorite_list is None:
#             recent_favorite_list = []
#         like_favorite_video_list = recent_like_list + recent_favorite_list
#         for video in like_favorite_video_list:
#             series_videos_str = await get_redis("series_videos_{}".format(video.video_info.series_name))
#             series_videos = ujson.loads(series_videos_str)
#             if len(series_videos) < (video.video_info.series_sequence + 1):
#                 recall_videos.append(series_videos[video.video_info.series_sequence + 1])
        
#         rd.shuffle(recall_videos)
#         return recall_videos[:self.recall_count]
    
class LevelRecaller(Recaller):
    def __init__(self):
        self.recall_from_series_count = 5
        self.recall_count = 20
        self.max_each_series_count = 3
    async def recall(self, recommender_ctx):
        # recall_videos = []
        user_level = recommender_ctx.user_profile_ctx.user_basic_info.level
        # level_prefix = "video_series_level-"
        level_prefix = "series_level-"
        # user_level <= 1时， 从redis获取入门、初学系列
        recall_series = []
        if user_level <= 1:
            key = "{}{}".format(level_prefix, "easy")
            recall_series = await lrange_redis(key, 0, -1)
            
        elif user_level == 2:
            key = "{}{}".format(level_prefix, "medium")
            recall_series = await lrange_redis(key, 0, -1)
        else:
            key = "{}{}".format(level_prefix, "hard")
            recall_series = await lrange_redis(key, 0, -1)
        rd.shuffle(recall_series)
        recall_videos = []
        for i in range(min(len(recall_series), self.recall_from_series_count)):
            series_videos = await lrange_redis("series_video-{}".format(recall_series[i]), 0, -1)
            rd.shuffle(series_videos)
            recall_videos += series_videos[:self.max_each_series_count]
        rd.shuffle(recall_videos)
        return recall_videos[:self.recall_count]

class FunvideosRecaller(Recaller):
    # recall videos for unlogin users
    def __init__(self):
        # self.funny_series_names = ['TT_0520_art', 'TT_0520_oddly_satisfy', 'TT_0520_music', 'TT_0520_car', 'TT_0520_fitness_health' 'TT_0520_anime_comics',  'TT_0520_outdoors', 'TT_0520_dance', 'TT_0520_entertainment_culture', 'TT_0520_comedy', 'TT_0520_travel', 'TT_0520_technology',  'TT_0520_gaming', 'TT_0520_family', 'TT_0520_beauty_style', 'TT_0520_finance', 'TT_0520_vlogs', 'TT_0520_sport:sports', 'TT_0520_food_drink', 'TT_0520_diy', 'TT_0520_motivation_advice']
        # self.funny_series_ids = ['68297be2493ae9d4e77ed4ac', '68297be2493ae9d4e77ed4ad', '68297be4493ae9d4e77ed4b1', '68297be4493ae9d4e77ed4b2', '68297be5493ae9d4e77ed4b5', '68297be5493ae9d4e77ed4b6', '68297be7493ae9d4e77ed4b9', '68297be7493ae9d4e77ed4ba', '68297be8493ae9d4e77ed4bc', '68297bec493ae9d4e77ed4c8', '6829ab16733471d672d69f61', '682fea61bc51970be22c35c2', '682fea6abc51970be22c35c3', '682fea71bc51970be22c35c4', '682fea78bc51970be22c35c5', '682fea7cbc51970be22c35c6', '682fea83bc51970be22c35c7', '682fea89bc51970be22c35c8', '682fea8fbc51970be22c35c9', '682fea94bc51970be22c35ca', '682fea99bc51970be22c35cb', '682fea9ebc51970be22c35cc', '682feaa3bc51970be22c35cd', '682feaa9bc51970be22c35ce', '682feaadbc51970be22c35cf', '682feab5bc51970be22c35d0', '682feababc51970be22c35d1', '682feabfbc51970be22c35d2', '682feac3bc51970be22c35d3', '682feacabc51970be22c35d4', '682fead1bc51970be22c35d5', '682fead5bc51970be22c35d6']
        self.recall_series_count = 5
        self.recall_video_perseries_count = 3
        self.recall_count = 10
    
    async def recall(self, recommender_ctx):
        easy_series_ids = await lrange_redis("series_level-easy", 0, -1)

        rd.shuffle(easy_series_ids)

        recall_video_ids = []
        for series_id in easy_series_ids[:self.recall_series_count]:
            series_video_ids = await lrange_redis("series_video-{}".format(series_id), 0, -1)
            rd.shuffle(series_video_ids)
            recall_video_ids += series_video_ids[:self.recall_video_perseries_count]
        rd.shuffle(recall_video_ids)
        return recall_video_ids[:self.recall_count]

class LevelInterestRecaller(Recaller):
    def __init__(self):
        self.recall_count = 20
        self.recall_from_series_count = 3
        self.level_interest_prefix = "video_series_interest_level-"
    async def recall(self, recommender_ctx):
        
        user_level = recommender_ctx.user_profile_ctx.user_basic_info.level
        user_interests = recommender_ctx.user_profile_ctx.user_basic_info.interests
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
                    recall_series += await lrange_redis(key, 0, -1)
        else:
            for app_interest in user_interests:
                for interest in online_interest_mapping(app_interest):
                    key = "{}{}_{}".format(self.level_interest_prefix, interest, "难")
                    recall_series += await lrange_redis(key, 0, -1)
        rd.shuffle(recall_series)
        recall_videos = []
        for i in range(min(len(recall_series), self.recall_from_series_count)):
            recall_videos += await lrange_redis("video_series-{}".format(recall_series[i]), 0, -1)
        rd.shuffle(recall_videos)
        return recall_videos[:self.recall_count]
        
    
# class SeriesRecaller(Recaller):
#     def __init__(self):
#         self.recall_count = 10
#         self.series_recall_count = 5
#     async def recall(self, input_data):
#         recall_videos = []
#         user_behavior_info = input_data.user_behavior_info
#         recent_watch_video_list = user_behavior_info.recent_watch_video_list
#         if recent_watch_video_list is None:
#             recent_watch_video_list = []
#         for watched_video in recent_watch_video_list:
#             if watched_video.watch_complete:
#                 series_videos_str = await get_redis("series_videos_{}".format(watched_video.video_info.series_name))
#                 series_videos = ujson.loads(series_videos_str)
#                 rd.shuffle(series_videos)
#                 recall_videos += series_videos[:self.series_recall_count]
        
#         recent_like_list = user_behavior_info.recent_like_video_list
#         if recent_like_list is None:
#             recent_like_list = []
#         recent_favorite_list = user_behavior_info.recent_favorite_video_list
#         if recent_favorite_list is None:
#             recent_favorite_list = []
#         like_favorite_video_list = recent_like_list + recent_favorite_list
#         for video in like_favorite_video_list:
#             series_videos_str = await get_redis("series_videos_{}".format(video.video_info.series_name))
#             series_videos = ujson.loads(series_videos_str)
#             rd.shuffle(series_videos)
#             recall_videos += series_videos[:self.series_recall_count]
        
#         rd.shuffle(recall_videos)
#         return recall_videos[:self.recall_count]

class RandomRecaller(Recaller):
    def __init__(self):
        self.recall_count = 20
    async def recall(self, recommender_ctx):
        random_video_ids = await lrange_redis("video_random", 0, -1)
        rd.shuffle(random_video_ids)
        return random_video_ids[:self.recall_count]

class PopRecaller(Recaller):
    def __init__(self):
        self.max_redis_count = 100
        self.recall_count = 50
    async def recall(self, recommender_ctx):
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
    # key = "video_series_interest_level-旅行_难"
    # key = "video_customize-KAU777"
    # keys = ['video-67af00380efa0542fbfdfff5', 'video-67af1034f2eb187c98ad6ef9', 'video-67bdd5c732790b134f93ea51', 'video-6786a9ea0fcb32e4238fcec4', 'video-6781456f41a163009adbc072', 'video-677ed4f5c2a3a301cdb08a21', 'video-677e36f14195abbf0944363d', 'video-677222265094f908c47a6ad4', 'video-67af120ef2eb187c98ad6f11', 'video-678c7a3db1b7a679c45074f1', 'video-67aeaa0b0efa0542fbfdf8fb', 'video-677f7d6341a163009adbbb4d', 'video-677ea611c2a3a301cdb08968', 'video-677223fa170f6da8ffa64fce', 'video-67722424170f6da8ffa64ff0', 'video-678c6bde90cab62a6d35f088', 'video-67befeb2aaba77bbb7795283', 'video-67af1578f2eb187c98ad6f45', 'video-678cd1b5e19148c899eeb216', 'video-6772d1faf5a17f43b2787b54', 'video-677e37254195abbf09443665', 'video-677223d05094f908c47a6c14', 'video-67794daa55287df7f874aa98', 'video-67794ee655287df7f874abc6', 'video-677e204151db2063f2d1dff1', 'video-677aa3d450b32456b794e8dd', 'video-67794dcfe257f0bcbe9d49a8', 'video-6785cfc3f94c296761f2593a', 'video-677e426a4195abbf094436fb', 'video-677e3490c2a3a301cdb084d5', 'video-677e37484195abbf09443675', 'video-677e36614195abbf094435fb', 'video-67af15b30efa0542fbfe02b5', 'video-677e41d74195abbf094436a3', 'video-67794e4b55287df7f874ab42', 'video-67722543170f6da8ffa650e2', 'video-67b837960f12a8ea4447e970']
    async def main():
        await init_redis_pool()  # 首先需要初始化redis连接
        # recall_series = await lrange_redis(key, 0, -1)
        # print(recall_series)
        # recall_videos = await zrange_redis(key, 0, -1)
        # results = await get_redis_concurrent(keys)
        level_recaller = LevelRecaller()
        results = await level_recaller.recall(None)
        print(results)
        await close_redis_pool()  # 最后关闭redis连接
    
    asyncio.run(main())