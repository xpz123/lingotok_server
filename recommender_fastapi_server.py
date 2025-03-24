from recaller import init_redis_pool, close_redis_pool
from recommender_v2_0 import RecommenderV2_0

import uuid
from typing import Optional, Union, List

import uvicorn
from fastapi import FastAPI, Body
from pydantic import BaseModel, HttpUrl
import asyncio

# recommender = Recommender()
# recommenderv1_1 = RecommenderV1_1()
recommender = RecommenderV2_0()

app = FastAPI()


@app.on_event("startup")
async def startup_event():
    await init_redis_pool()


@app.on_event("shutdown")
async def shutdown_event():
    await close_redis_pool()


class UserInfo(BaseModel):
    user_id: str
    uid: str
    user_name: str
    age: int
    gender: str
    level: int
    interests: Optional[List[str]]
    goal: str
    avatar_url: Optional[str] = None
    invite_code: str


class VideoInfo(BaseModel):
    video_id: str
    asset_id: str
    title: str
    duration: int
    level: int
    audio_radio: float
    video_status: Optional[str] = None
    subtitle_list: Optional[List[dict]] = None
    customize: Optional[str] = None
    series_name: Optional[str] = ""
    series_sequence: Optional[int] = -100
    author_name: Optional[str] = None
    author_avatar_url: Optional[str] = None


class VideoUserInfo(BaseModel):
    video_info: VideoInfo
    watch_complete: bool
    watch_time: Optional[int]
    like: bool
    favorite: bool


class UserBehaviorInfo(BaseModel):
    recent_watch_video_list: Optional[List[VideoUserInfo]] = None
    recent_like_video_list: Optional[List[VideoUserInfo]] = None
    recent_favorite_video_list: Optional[List[VideoUserInfo]] = None


class RecommendVideoRequest(BaseModel):
    user_id: Optional[str] = None
    user_info: Optional[UserInfo] = None
    size: int
    user_behavior_info: Optional[UserBehaviorInfo] = None
    req_id: Optional[str] = None


@app.post('/recommend_video_v1')
async def recommend_video_v1(input_data: RecommendVideoRequest):
    # Generate reqid
    req_id = str(uuid.uuid4())
    try:
        input_data.req_id = req_id
        video_id_list = await recommender.recommend(input_data)
        return {"video_id_list": video_id_list, "code": 200, "title_list": [], "req_id": req_id}
    except Exception as e:
        print(e)
        return {"code": -1, "video_id_list": [], "req_id": req_id, "title_list": []}

@app.post('/recommend_video_without_userinfo')
async def recommend_video_without_userinfo(input_data: RecommendVideoRequest):
    # Generate reqid
    req_id = str(uuid.uuid4())
    try:
        input_data.req_id = req_id
        video_id_list = await recommender.recommend_without_userinfo(input_data)
        return {"video_id_list": video_id_list, "code": 200, "title_list": [], "req_id": req_id}
    except Exception as e:
        print(e)
        return {"code": -1, "video_id_list": [], "req_id": req_id, "title_list": []}


if __name__ == "__main__":
    uvicorn.run(app=app, host='0.0.0.0', port=5000)  # http_server = make_server('127.0.0.1', 5000, app)  # http_server.serve_forever()
