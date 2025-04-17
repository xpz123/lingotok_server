import firebase_admin
from firebase_admin import credentials
from firebase_admin import auth
from pymongo import MongoClient, UpdateOne
import json
from tqdm import tqdm
from bson.objectid import ObjectId
from retrying import retry
import pandas as pd
from collections import defaultdict
import redis
import sys
import os
from concurrent.futures import ThreadPoolExecutor

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from video_process.create_video import update_video_info

# cred = credentials.Certificate("/Users/tal/work/lingtok_server/recommender/lingtok-e5a89-firebase-adminsdk-97cm6-d582cf2c95.json")
# firebase_admin.initialize_app(cred)

class VideoUpdater:
    def __init__(self):
        self.mongo_client = MongoClient("mongodb://rwuser:Lingotok123!@101.46.54.186:8635,101.46.58.227:8635/test?authSource=admin")
        
        # populate index weight
        self.populate_weight = {"watch_complete": 1, "like": 3, "favorite": 3}

        self.redis_client = redis.StrictRedis(host="101.46.56.32", port=6379, password="Lingotok123!")
    
    @retry(stop_max_attempt_number=3, wait_fixed=2000)
    def update_db(self, collection_name, datas):
        collection = self.mongo_client["lingotok"][collection_name]
        bulk_operations = []
        for data in datas:
            query = {"_id": ObjectId(data["_id"])}
            data.pop("_id")
            update = {"$set": data}
            bulk_operations.append(UpdateOne(query, update))
        collection.bulk_write(bulk_operations)
    
    def update_video_series_name_offline(self, jsonl_file):
        lines = open(jsonl_file, "r", encoding="utf-8").readlines()
        datas = list()
        for l in tqdm(lines):
            video = json.loads(l)
            series_name = video["title"].split("_")[0]
            data = {"_id": video["_id"], "series_name": series_name}
            datas.append(data)
            if len(datas) % 500 == 0:
                self.update_db("video", datas)
                datas = []
        if len(datas) > 0:
            self.update_db("video", datas)

    def update_video_series_name_online(self, write_jsonl=False):
        db = self.mongo_client["lingotok"]
        collection = db["video"]
        videos_without_series_name = list()
        for video in tqdm(collection.find({"series_name": None})):
            videos_without_series_name.append(video)
        for video in tqdm(collection.find({"series_name": ""})):
            videos_without_series_name.append(video)
        
        print ("Videos need to be updated: {}".format(len(videos_without_series_name)))
        datas = list()
        for video in tqdm(videos_without_series_name):
            series_name = video["title"].split("_")[0]
            data = {"_id": video["_id"], "series_name": series_name}
            datas.append(data)
            if len(datas) % 500 == 0:
                self.update_db("video", datas)
                datas = []
        if len(datas) > 0:
            self.update_db("video", datas)
        if write_jsonl:
            with open("videos_without_series_name.jsonl", "w", encoding="utf-8") as fw:
                for video in videos_without_series_name:
                    video["_id"] = video["_id"].__str__()
                    video["created_at"] = video["created_at"].__str__()
                    video["updated_at"] = video["updated_at"].__str__()
                    fw.write(json.dumps(video, ensure_ascii=False) + "\n")

    def fetch_all_series_name(self, out_csv=None, asset_id_num=0):
        db = self.mongo_client["lingotok"]
        collection = db["video"]
        series_names = collection.distinct("series_name")
        
        
        columns = ["series_name"]
        datas = []
        if asset_id_num > 0:
            for i in range(asset_id_num):
                columns.append("asset_id_{}".format(i))
            for series_name in tqdm(series_names):
                data = []
                data.append(series_name)
                videos = collection.find({"series_name": series_name})
                try:
                    for i in range(asset_id_num):
                        data.append(videos.next()["asset_id"])
                    datas.append(data)
                except:
                    pass
        df = pd.DataFrame(datas, columns=columns)
        if out_csv:
            df.to_csv(out_csv, index=False)

        return series_names
    
    def calc_video_popularity(self, out_csv=None):
        db = self.mongo_client["lingotok"]
        collection = db["user_video"]
        watch_pipeline = [
            {
                "$match": {
                    "delete": {
                        "$ne": True
                    }
                }
            },
            {
                "$group": {
                    "_id": "$video_id",
                    "count": {"$sum": 1}
                }
            }
        ]
        watch_videos = collection.aggregate(watch_pipeline)
        video_watch = dict()
        for video in watch_videos:
            video_id = str(video["_id"])
            video_watch[video_id] = video["count"]
        watch_complete_pipeline = [
            {
                "$match": {
                    "watch_complete": True,
                    "delete": {
                        "$ne": True
                    }
                }
            },
            {
                "$group": {
                    "_id": "$video_id",
                    "count": {"$sum": 1}
                }
            }
        ]
        like_pipeline = [
            {
                "$match": {
                    "like": True,
                    "delete": {
                        "$ne": True
                    }
                }
            },
            {
                "$group": {
                    "_id": "$video_id",
                    "count": {"$sum": 1}
                }
            }
        ]
        colleced_pipeline = [
            {
                "$match": {
                    "favorite": True,
                    "delete": {
                        "$ne": True
                    }
                }
            },
            {
                "$group": {
                    "_id": "$video_id",
                    "count": {"$sum": 1}
                }
            }
        ]
        complete_videos = collection.aggregate(watch_complete_pipeline)
        like_videos = collection.aggregate(like_pipeline)
        colleced_videos = collection.aggregate(colleced_pipeline)
        video_popularity = dict()
        video_complete = dict()
        video_like = dict()
        video_favorite = dict()
        for video in complete_videos:
            video_id = str(video["_id"])
            video_popularity[video_id] = float(video["count"]) / float(video_watch[video_id]) * self.populate_weight["watch_complete"]
            video_complete[video_id] = video["count"]
        for video in like_videos:
            video_id = str(video["_id"])
            if video_id in video_popularity.keys():
                video_popularity[video_id] += float(video["count"]) / float(video_watch[video_id]) * self.populate_weight["like"]
            else:
                video_popularity[video_id] = float(video["count"]) / float(video_watch[video_id]) * self.populate_weight["like"]
            video_like[video_id] = video["count"]
        for video in colleced_videos:
            video_id = str(video["_id"])
            if video_id in video_popularity.keys():
                video_popularity[video_id] += float(video["count"]) / float(video_watch[video_id]) * self.populate_weight["favorite"]
            else:
                video_popularity[video_id] = float(video["count"]) / float(video_watch[video_id]) * self.populate_weight["favorite"]
            video_favorite[video_id] = video["count"]
        # import pdb; pdb.set_trace()
        sorted_video_popularity = sorted(video_popularity.items(), key=lambda x: x[1], reverse=True)
        if out_csv:
            video_ids = list()
            for video_id, _ in sorted_video_popularity:
                try:
                    video_ids.append(ObjectId(video_id))
                except:
                    pass
            
            video_infos = db["video"].find({
                "_id": {
                    "$in": video_ids
                }
            })
            columns = ["video_id", "watch_count", "complete_count", "like_count", "favorite_count", "popularity", "create_time", "asset_id", "title", "series_name"]
            data_list = []
            for video_info in video_infos:
                data = []
                video_id = video_info["_id"].__str__()
                data.append(video_id)
                data.append(video_watch[video_id])
                if video_id in video_complete.keys():
                    data.append(video_complete[video_id])
                else:
                    data.append(0)
                if video_id in video_like.keys():
                    data.append(video_like[video_id])
                else:
                    data.append(0)
                if video_id in video_favorite.keys():
                    data.append(video_favorite[video_id])
                else:    
                    data.append(0)
                data.append(video_popularity[video_id])
                data.append(str(video_info["created_at"]))
                data.append(video_info["asset_id"])
                data.append(video_info["title"])
                data.append(video_info["series_name"])
                data_list.append(data)
            df = pd.DataFrame(data_list, columns=columns)
            df.to_csv(out_csv, index=False)

        return sorted_video_popularity

    def update_redis_series_tag_offline(self, csv_file):
        df = pd.read_csv(csv_file)
        level_dict = defaultdict(list)
        interest_dict = defaultdict(list)
        # age_dict = defaultdict(list)
        # gender_dict = defaultdict(list)
        for i in range(df.shape[0]):
            series_name = df.iloc[i]["series_name"]
            level = df.iloc[i]["难度"]
            if type(level) == str:
                level_dict[level].append(series_name)
            interests = df.iloc[i]["兴趣"]
            if type(interests) == str:
                for interest in interests.split(","):
                    interest_dict[interest].append(series_name)
            # gender = df.iloc[i]["性别"]
            # if gender == "both":
            #     gender_dict["male"].append(series_name)
            #     gender_dict["female"].append(series_name)
            # else:
            #     gender_dict[gender].append(series_name)
            # ages = df.iloc[i]["适合年龄"]
            # for age in ages:
            #     age_dict[age].append(series_name)
        for level in level_dict:
            level_prefix = "video_series_level-"
            key = "{}{}".format(level_prefix, level)
            # if self.redis_client.exists(key):
            #     self.redis_client.delete(key)
            # 使用pipeline批量写入
            pipe = self.redis_client.pipeline()
            for series_name in level_dict[level]:
                pipe.lpush(key, series_name)
            pipe.execute()
        
        for level in level_dict.keys():
            level_set = set(level_dict[level])
            for interest in interest_dict.keys():
                interest_set = set(interest_dict[interest])
                level_interest_set = level_set & interest_set
                level_interest_list = list(level_interest_set)
                # import pdb;pdb.set_trace()
                if len(level_interest_list) != 0:
                    key = "video_series_interest_level-{}_{}".format(interest, level)
                    if self.redis_client.exists(key):
                        self.redis_client.delete(key)
                    pipe = self.redis_client.pipeline()
                    for series_name in level_interest_list:
                        pipe.lpush(key, series_name)
                    pipe.execute()
        
    def fetch_video_infos(self, jsonl_file=None):
        db = self.mongo_client["lingotok"]
        collection = db["video"]
        video_infos = collection.find({})
        if jsonl_file:
            with open(jsonl_file, "w", encoding="utf-8") as fw:
                for video_info in video_infos:
                    # 转换ObjectId为字符串
                    video_info['_id'] = str(video_info['_id'])
                    # 转换其他可能的日期字段
                    if 'created_at' in video_info:
                        video_info['created_at'] = str(video_info['created_at'])
                    if 'updated_at' in video_info:
                        video_info['updated_at'] = str(video_info['updated_at'])
                    fw.write(json.dumps(video_info, ensure_ascii=False) + "\n")
    
    def update_video_taglist(self, jsonl_file, series_csv):
        df = pd.read_csv(series_csv)
        series_name_taglist_dict = dict()
        for i in range(df.shape[0]):
            try:
                series_name = df.iloc[i]["series_name"]
                tag_list = df.iloc[i]["兴趣"].split(",")
                series_name_taglist_dict[series_name] = tag_list
            except:
                print (df.iloc[i])
        
        # 添加错误记录列表
        failed_updates = []
        
        def process_video(args):
            video_id, tag_list = args
            response = update_video_info(video_id, tag_list=tag_list)
            # 检查响应状态
            if isinstance(response, dict):
                if response.get('code') != 200 or response.get('message') != 'success':
                    failed_updates.append({
                        'video_id': video_id,
                        'response': response
                    })
            else:
                failed_updates.append({
                    'video_id': video_id,
                    'response': 'Invalid response format'
                })

        batch = []
        with open(jsonl_file, "r", encoding="utf-8") as fr:
            lines = fr.readlines()
            for line in tqdm(lines):
                video_info = json.loads(line)
                deleted = video_info.get("deleted", False)
                if deleted:
                    continue
                series_name = video_info.get("series_name", "")
                if series_name == "":
                    continue
                tag_list = series_name_taglist_dict.get(series_name, [])
                batch.append((video_info["_id"], tag_list))
                
                # 当积累了5条数据时，并发处理
                if len(batch) >= 20:
                    with ThreadPoolExecutor(max_workers=20) as executor:
                        executor.map(process_video, batch)
                    batch = []  # 清空批次
        # 处理剩余的数据
        if batch:
            with ThreadPoolExecutor(max_workers=len(batch)) as executor:
                executor.map(process_video, batch)

        # 处理完成后，将失败记录写入文件
        if failed_updates:
            with open('failed_video_updates.json', 'w', encoding='utf-8') as f:
                json.dump(failed_updates, f, ensure_ascii=False, indent=2)
            print(f"发现 {len(failed_updates)} 个更新失败的视频，详细信息已写入 failed_video_updates.json")
    def update_series_tag_once(self, series_name, level=None, tag_list=None):
        pipe = self.redis_client.pipeline()
        if level:
            assert level in ["初学", "入门", "难", "中等"]
            level_prefix = "video_series_level-"
            key = "{}{}".format(level_prefix, level)
            pipe.lpush(key, series_name)
        if tag_list:
            for tag in tag_list:
                key = "video_series_interest_level-{}_{}".format(tag, level)
                pipe.lpush(key, series_name)
        pipe.execute()

if __name__ == "__main__":
    video_updater = VideoUpdater()
    # video_updater.update_video_series_name() 
    # video_updater.update_db("video", {"_id": "6777e5064ae288bb9433b196", "series_name": "abcsde"})
    # video_updater.update_video_series_name_offline("videos_without_series_name.jsonl")
    # video_updater.update_video_series_name_online()
    # video_updater.fetch_all_series_name(out_csv = "series_names_20250301.csv", asset_id_num=5)
    # video_updater.calc_video_popularity(out_csv="video_popularity_20250306.csv")
    # video_updater.update_redis_series_tag_offline("/Users/tal/work/lingtok_server/recommender/series_names_20250301_labeled.csv")
    # video_updater.fetch_video_infos(jsonl_file="video_infos_20250322.jsonl")
    video_updater.update_video_taglist(jsonl_file="video_infos_20250322.jsonl", series_csv="series_names_20250301_labeled.csv")
