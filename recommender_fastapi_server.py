from recommender import Recommender
from recommender_v1_1 import RecommenderV1_1

import uuid
from typing import Optional, Union, List
 
import uvicorn
from fastapi import FastAPI, Body
from pydantic import BaseModel, HttpUrl

recommender = Recommender()
recommenderv1_1 = RecommenderV1_1()
app = FastAPI()

class UserInfo(BaseModel):
    user_id: str
    uid: str
    user_name: str
    age: int
    gender: str
    level: int
    interests: List[str]
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
    like: bool
    favorite: bool

class UserBehaviorInfo(BaseModel):
    recent_watch_video_list: Optional[List[VideoUserInfo]] = None
    recent_like_video_list: Optional[List[VideoUserInfo]] = None
    recent_favorite_video_list: Optional[List[VideoUserInfo]] = None

class RecommendVideoRequest(BaseModel):
    user_id: str
    user_info: UserInfo
    size: int
    user_behavior_info: Optional[UserBehaviorInfo] = None
    req_id: Optional[str] = None


@app.post('/recommend_video_v1')
def recommend_video_v1(input_data: RecommendVideoRequest):
    # Generate reqid
    req_id = str(uuid.uuid4())
    try:
        input_data.req_id = req_id
        video_info_list = recommenderv1_1.recommend(input_data)
        video_id_list = [video_info['id'] for video_info in video_info_list]
        title_list = [video_info['title'] for video_info in video_info_list]
        return {"video_id_list": video_id_list, "code": 200, "title_list": title_list, "req_id": req_id}
    except Exception as e:
        print (e)
        return {"code": -1, "video_id_list": [], "req_id": req_id}

if __name__ == "__main__":
    uvicorn.run(app=app, host='0.0.0.0',
      port=5000)
    # http_server = make_server('127.0.0.1', 5000, app)
    # http_server.serve_forever()