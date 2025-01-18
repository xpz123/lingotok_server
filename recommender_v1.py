import redis
import json
import random as rd
import time
import logging
import logging.handlers
import os

log_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
if not os.path.exists("logs"):
    os.makedirs("logs")
log_handler = logging.handlers.TimedRotatingFileHandler(
    'logs/recommender.log',  # 日志文件名
    when='midnight',  # 每天午夜切割
    interval=1,  # 每天切割一次
    backupCount=50  # 保留最近7个日志文件
)
log_handler.setFormatter(log_formatter)
logger = logging.getLogger('RecommenderLogger')
logger.setLevel(logging.DEBUG)  # 设置日志级别
logger.addHandler(log_handler)

yepzan_redis = redis.StrictRedis(host="192.168.0.120", port=6379, password="Lingotok123!")
# yepzan_redis = redis.StrictRedis(host="101.46.56.32", port=6379, password="Lingotok123!")

class Recaller:
    def __init__(self):
        pass
    def recall(self, input_data):
        return {}
class LatestRecaller(Recaller):
    def __init__(self):
        self.recall_count = 10
    def recall(self, input_data):
        start_time = time.time()
        latest_videos = json.loads(yepzan_redis.get("latest_videos"))
        end_time = time.time()
        print ("latest get dur {}".format(end_time - start_time))
        rd.shuffle(latest_videos)
        return latest_videos[:self.recall_count]
class CustomizedRecaller(Recaller):
    def __init__(self):
        self.recall_count = 10
    def recall(self, input_data):
        basic_user_info = input_data["user_info"]
        invite_code = basic_user_info["invite_code"]
        try:
            start_time = time.time()
            customize_videos = json.loads(yepzan_redis.get("customize_videos_{}".format(invite_code)))
            end_time = time.time()
            print ("latest get dur {}".format(end_time - start_time))
            rd.shuffle(customize_videos)
            return customize_videos[:self.recall_count]
        except:
            return []

class ContinuousRecaller(Recaller):
    def __init__(self):
        self.recall_count = 10
    def recall(self, input_data):
        recall_videos = []
        user_behavior_info = input_data["user_behavior_info"]
        recent_watch_video_list = user_behavior_info.get("recent_watch_video_list", [])
        if recent_watch_video_list is None:
            recent_watch_video_list = []
        for watched_video in recent_watch_video_list:
            if watched_video["watch_complete"]:
                series_videos = json.loads(yepzan_redis.get("series_videos_{}".format(watched_video["video_info"].get("series_name", ""))))
                if len(series_videos) < (watched_video["video_info"].get("series_name", "") + 1):
                    recall_videos.append(series_videos[watched_video["video_info"]["series_sequence"] + 1])
        
        recent_like_list = user_behavior_info.get("recent_like_video_list", [])
        if recent_like_list is None:
            recent_like_list = []
        recent_favorite_list = user_behavior_info.get("recent_favorite_video_list", [])
        if recent_favorite_list is None:
            recent_favorite_list = []
        like_favorite_video_list = recent_like_list + recent_favorite_list
        for video in like_favorite_video_list:
            series_videos = json.loads(yepzan_redis.get("series_videos_{}".format(video["video_info"].get("series_name", ""))))
            if len(series_videos) < (video["video_info"].get("series_sequence", -100) + 1):
                recall_videos.append(series_videos[video["video_info"].get("series_sequence", -100) + 1])
        
        rd.shuffle(recall_videos)
        return recall_videos[:self.recall_count]
    
class LevelRecaller(Recaller):
    def __init__(self):
        self.recall_count = 10
    def recall(self, input_data):
        # recall_videos = []
        user_level = input_data["user_info"]["level"]
        if user_level > 1:
            return []
        start_time = time.time()
        hsk_videos = json.loads(yepzan_redis.get("customize_videos_HSK_DIY"))
        end_time = time.time()
        print ("latest get dur {}".format(end_time - start_time))
        
        rd.shuffle(hsk_videos)
        return hsk_videos[:self.recall_count]
    
class SeriesRecaller(Recaller):
    def __init__(self):
        self.recall_count = 10
        self.series_recall_count = 5
    def recall(self, input_data):
        recall_videos = []
        user_behavior_info = input_data["user_behavior_info"]
        recent_watch_video_list = user_behavior_info.get("recent_watch_video_list", [])
        if recent_watch_video_list is None:
            recent_watch_video_list = []
        for watched_video in recent_watch_video_list:
            if watched_video["watch_complete"]:
                series_videos = json.loads(yepzan_redis.get("series_videos_{}".format(watched_video["video_info"].get("series_name", ""))))
                rd.shuffle(series_videos)
                recall_videos += series_videos[:self.series_recall_count]
        
        recent_like_list = user_behavior_info.get("recent_like_video_list", [])
        if recent_like_list is None:
            recent_like_list = []
        recent_favorite_list = user_behavior_info.get("recent_favorite_video_list", [])
        if recent_favorite_list is None:
            recent_favorite_list = []
        like_favorite_video_list = recent_like_list + recent_favorite_list
        for video in like_favorite_video_list:
            series_videos = json.loads(yepzan_redis.get("series_videos_{}".format(video["video_info"].get("series_name", ""))))
            rd.shuffle(series_videos)
            recall_videos += series_videos[:self.series_recall_count]
        
        rd.shuffle(recall_videos)
        return recall_videos[:self.recall_count]
class RandomRecaller(Recaller):
    def __init__(self):
        self.recall_count = 10
    def recall(self, input_data):
        all_videos = json.loads(yepzan_redis.get("random_videos"))
        rd.shuffle(all_videos)
        return all_videos[:self.recall_count]
    
class Ranker:
    def __init__(self):
        pass
    def recall(self, user_info, recall_result):
        return {}
    

class RecommenderV1:
    def __init__(self):
        self.recaller_dict = {"latest": LatestRecaller(), "customized": CustomizedRecaller(), "continuous": ContinuousRecaller(), "series": SeriesRecaller(), "level": LevelRecaller(), "random": RandomRecaller()}
        # self.ranker = Ranker()
        self.latest_ratio = 0.2
        self.primary_ratio = 0.2
        self.random_ratio = 0.2
    
    def fetch_recent_watched_videos(self, user_behavior_info):
        recent_watch_videoid_set = set()
        recent_watch_video_list = user_behavior_info.get("recent_watch_video_list", None)
        if not recent_watch_video_list is None:
            for video in recent_watch_video_list:
                recent_watch_videoid_set.add(video["video_info"]["video_id"])
        like_video_list = user_behavior_info.get("recent_like_video_list", [])
        if not like_video_list is None:
            for video in like_video_list:
                recent_watch_videoid_set.add(video["video_info"]["video_id"])
        favorite_video_list = user_behavior_info.get("recent_favorite_video_list", [])
        if not favorite_video_list is None:
            for video in favorite_video_list:
                recent_watch_videoid_set.add(video["video_info"]["video_id"])
        return recent_watch_videoid_set

    
    def recommend(self, input_data):

        size = input_data["size"]
        size = min(20, size)
        req_id = input_data["req_id"]

        user_behavior_info = input_data["user_behavior_info"]

        # Fetch recent watched video_id from recent_watch_video_list, recent_like_video_list, recent_favorite_video_list
        recent_watch_videoid_set = self.fetch_recent_watched_videos(user_behavior_info)

        rank_result = []
        recall_result_dict = {}

        recalling_latency = dict()
        for recaller_name in self.recaller_dict.keys():
            try:
                start_time = time.time()
                recall_result = self.recaller_dict[recaller_name].recall(input_data)
                recall_end_time = time.time()
                recalling_latency[recaller_name] = {}
                recalling_latency[recaller_name]["recall_latency"] = recall_end_time - start_time
                filted_recall_result = []
                for video in recall_result:
                    if video["id"] not in recent_watch_videoid_set:
                        filted_recall_result.append(video)
                recall_result_dict[recaller_name] = filted_recall_result
                deduplication_end_time = time.time()
                recalling_latency[recaller_name]["deduplication_latency"] = deduplication_end_time - recall_end_time
            except Exception as e:
                print (str(e))
                recall_result_dict[recaller_name] = []
        
        recalling_latency["req_id"] = req_id
        logger.info(json.dumps(recalling_latency))
        
        # v0.1(已废弃)
        # 定制用户：全部返回定制内容
        # 非定制非初级用户：20%的最新内容+ 80%的（连续内容+系列内容）
        # 非定制初级用户： 20%的最新内容 + 20%的初级内容 + 60%的（连续内容+系列内容）
        # 如果上述内容无法填满size，则随机填充最新内容
        # v0.2(最新)
        # 非定制非初级用户：20%最新内容 + 20% 随机全量内容 + 60%连续内容+系列内容
        # 非定制初级用户：20%最新内容 + 20%初级内容 + 20% 随机全量内容 + 40%连续内容+系列内容
        # 如果上述内容无法填满size，则随机填充库中内容
        rank_result += recall_result_dict["customized"]
        if len(rank_result) > size:
            rd.shuffle(rank_result)
            return rank_result[:size]
        if input_data["user_info"]["level"] <= 1:
            rank_result += recall_result_dict["level"][:(int(size * self.primary_ratio))]
        rank_result += recall_result_dict["latest"][:(int(size * self.latest_ratio))]
        rank_result += recall_result_dict["random"][:(int(size * self.random_ratio))]
        rank_result += recall_result_dict["continuous"]
        if len(rank_result) > size:
            rank_result = rank_result[:size]
            rd.shuffle(rank_result)
            return rank_result[:size]
        
        rank_result += recall_result_dict["series"]
        if len(rank_result) > size:
            rank_result = rank_result[:size]
            rd.shuffle(rank_result)
            return rank_result[:size]
        
        # rank_result += recall_result_dict["latest"][(int(size * self.latest_ratio)):]
        rank_result += recall_result_dict["random"][(int(size * self.random_ratio)):]
        if len(rank_result) > size:
            rank_result = rank_result[:size]
            rd.shuffle(rank_result)
            return rank_result[:size]
        
        # Rank the recall result
        return rank_result


if __name__ == "__main__":
    pass
    # latest_videos = json.loads(yepzan_redis.get("latest_videos"))
    # series_videos_ = json.loads(yepzan_redis.get("series_videos_新东方比邻国际中文"))