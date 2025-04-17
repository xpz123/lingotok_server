import redis
import json
import random as rd

yepzan_redis = redis.StrictRedis(host="101.46.56.32", port=6379, password="Lingotok123!")

class Recaller:
    def __init__(self):
        pass
    def recall(self, input_data):
        return {}
class LatestRecaller(Recaller):
    def __init__(self):
        self.recall_count = 10
    def recall(self, input_data):
        latest_videos = json.loads(yepzan_redis.get("latest_videos"))
        rd.shuffle(latest_videos)
        return latest_videos[:self.recall_count]
class CustomizedRecaller(Recaller):
    def __init__(self):
        self.recall_count = 10
    def recall(self, input_data):
        basic_user_info = input_data["user_info"]
        invite_code = basic_user_info["invite_code"]
        try:
            customize_videos = json.loads(yepzan_redis.get("customize_videos_{}".format(invite_code)))
            rd.shuffle(customize_videos[invite_code])
            return customize_videos[invite_code][:self.recall_count]
        except:
            return []

class ContinuousRecaller(Recaller):
    def __init__(self):
        self.recall_count = 10
    def recall(self, input_data):
        recall_videos = []
        user_behavior_info = input_data["user_behavior_info"]
        recent_watch_video_list = user_behavior_info["recent_watch_video_list"]
        for watched_video in recent_watch_video_list:
            if watched_video["watch_complete"]:
                series_videos = json.loads(yepzan_redis.get("series_videos_{}".format(watched_video["video_info"]["series_name"])))
                if len(series_videos) < (watched_video["video_info"]["series_sequence"] + 1):
                    recall_videos.append(series_videos[watched_video["video_info"]["series_sequence"] + 1])
        
        like_favorite_video_list = user_behavior_info["recent_like_video_list"] + user_behavior_info["recent_favorite_video_list"]
        for video in like_favorite_video_list:
            series_videos = json.loads(yepzan_redis.get("series_videos_{}".format(video["series_name"])))
            if len(series_videos) < (video["series_sequence"] + 1):
                recall_videos.append(series_videos[video["series_sequence"] + 1])
        
        rd.shuffle(recall_videos)
        return recall_videos
                
class SeriesRecaller(Recaller):
    def __init__(self):
        self.recall_count = 10
        self.series_recall_count = 5
    def recall(self, input_data):
        recall_videos = []
        user_behavior_info = input_data["user_behavior_info"]
        recent_watch_video_list = user_behavior_info["recent_watch_video_list"]
        for watched_video in recent_watch_video_list:
            if watched_video["watch_complete"]:
                series_videos = json.loads(yepzan_redis.get("series_videos_{}".format(watched_video["video_info"]["series_name"])))
                rd.shuffle(series_videos)
                recall_videos += series_videos[:self.series_recall_count]
        
        like_favorite_video_list = user_behavior_info["recent_like_video_list"] + user_behavior_info["recent_favorite_video_list"]
        for video in like_favorite_video_list:
            series_videos = json.loads(yepzan_redis.get("series_videos_{}".format(video["series_name"])))
            rd.shuffle(series_videos)
            recall_videos += series_videos[:self.series_recall_count]
        
        rd.shuffle(recall_videos)
        return recall_videos[:self.recall_count]

class Ranker:
    def __init__(self):
        pass
    def recall(self, user_info, recall_result):
        return {}
    

class RecommenderV1:
    def __init__(self):
        self.recaller_dict = {"latest": LatestRecaller(), "customized": CustomizedRecaller(), "continuous": ContinuousRecaller(), "series": SeriesRecaller()}
        # self.ranker = Ranker()
        self.latest_ratio = 0.2
    
    def recommend(self, input_data):
        import pdb
        pdb.set_trace()
        size = input_data["size"]
        size = min(20, size)
        recent_watch_videoid_set = set()
        user_behavior_info = input_data["user_behavior_info"]
        recent_watch_video_list = user_behavior_info["recent_watch_video_list"]
        for video in recent_watch_video_list:
            recent_watch_videoid_set.add(video["video_info"]["video_id"])

        rank_result = []
        recall_result_dict = {}
        for recaller_name in self.recaller_dict.keys():
            recall_result = self.recaller_dict[recaller_name].recall(input_data)
            filted_recall_result = []
            for video in recall_result:
                if video["video_id"] not in recent_watch_videoid_set:
                    filted_recall_result.append(video)
            recall_result_dict[recaller_name] = filted_recall_result
        
        rank_result += recall_result_dict["customized"]
        if len(rank_result) > size:
            rd.shuffle(rank_result)
            return rank_result[:size]
        
        rank_result += recall_result_dict["latest"][:(int(size * self.latest_ratio))]
        rank_result += recall_result_dict["continuous"]
        if len(rank_result) > size:
            rd.shuffle(rank_result)
            return rank_result[:size]
        
        rank_result += recall_result_dict["series"]
        if len(rank_result) > size:
            rd.shuffle(rank_result)
            return rank_result[:size]
        
        rank_result += recall_result_dict["latest"]
        if len(rank_result) > size:
            rd.shuffle(rank_result)
            return rank_result[:size]
        
        # Rank the recall result
        return rank_result


if __name__ == "__main__":
    pass
    # latest_videos = json.loads(yepzan_redis.get("latest_videos"))
    # series_videos_ = json.loads(yepzan_redis.get("series_videos_新东方比邻国际中文"))