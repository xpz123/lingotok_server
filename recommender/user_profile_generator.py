from recaller import get_redis, init_redis_pool, close_redis_pool, lrange_redis, get_redis_concurrent
import json
# for debug
import asyncio
from pydantic import BaseModel
from typing import Optional, List


class UserBasicInfo(BaseModel):
    user_id: Optional[str] = None
    uid: Optional[str] = None
    username: Optional[str] = None
    level: Optional[int] = None
    gender: Optional[str] = None
    age: Optional[int] = None
    interests: Optional[List[str]] = None
    goal: Optional[int] = None
    avatar_url: Optional[str] = None
    invite_code: Optional[str] = None

class UserBehaviorInfo(BaseModel):
    recent_watch_videoid_list: Optional[List[str]] = []
    recent_like_videoid_list: Optional[List[str]] = []
    recent_favorite_videoid_list: Optional[List[str]] = []

class UserProfileCtx:
    def __init__(self):
        self.user_id = None
        self.user_basic_info = UserBasicInfo()
        self.user_behavior_info = UserBehaviorInfo()

class UserProfileGenerator:
    def __init__(self):
        self.max_fetch_video_count = 100
        self.max_rtn_video_count = 20

    async def process_basic_info(self, user_basic_info, user_id):
        fetched_info_str = await get_redis("user-{}".format(user_id))
        
        if not fetched_info_str:
            return False
        fetched_info = json.loads(fetched_info_str)
        user_basic_info.user_id = fetched_info.get("id", None)
        user_basic_info.uid = fetched_info.get("UID", None)
        user_basic_info.username = fetched_info.get("Username", None)
        user_basic_info.level = fetched_info.get("Level", None)
        user_basic_info.gender = fetched_info.get("Gender", None)
        user_basic_info.age = fetched_info.get("Age", None)
        user_basic_info.interests = fetched_info.get("Interests", None)
        user_basic_info.goal = fetched_info.get("StudyTimeGoal", None)
        user_basic_info.avatar_url = fetched_info.get("AvatarURL", None)
        user_basic_info.invite_code = fetched_info.get("InviteCode", None)

        return True
    
    async def process_behavior_info(self, user_behavior_info, user_id):
        fetched_watch_videos = await lrange_redis("user_watch-{}".format(user_id), 0, self.max_fetch_video_count)
        user_behavior_info.recent_watch_videoid_list = list(dict.fromkeys(fetched_watch_videos))[:self.max_rtn_video_count]
        fetched_like_videos = await lrange_redis("user_like-{}".format(user_id), 0, self.max_fetch_video_count)
        user_behavior_info.recent_like_videoid_list = list(dict.fromkeys(fetched_like_videos))[:self.max_rtn_video_count]
        fetched_favorite_videos = await lrange_redis("user_favorite-{}".format(user_id), 0, self.max_fetch_video_count)
        user_behavior_info.recent_favorite_videoid_list = list(dict.fromkeys(fetched_favorite_videos))[:self.max_rtn_video_count]

        # need_fetched_videoid_list = list(set(user_behavior_info.recent_watch_videoid_list + user_behavior_info.recent_like_videoid_list + user_behavior_info.recent_favorite_videoid_list))

        # fetched_keys = ["video-{}".format(videoid) for videoid in need_fetched_videoid_list]
        # print (fetched_keys)
        # fetched_videos_str_dict = await get_redis_concurrent(fetched_keys)
        return True
        
    async def generate_user_profile(self, user_profile_ctx):
        user_id = user_profile_ctx.user_id
        
        if not await self.process_basic_info(user_profile_ctx.user_basic_info, user_id):
            return False
        
        await self.process_behavior_info(user_profile_ctx.user_behavior_info, user_id)
        
        return True
    

if __name__ == "__main__":
    user_profile_generator = UserProfileGenerator()
    async def main():
        await init_redis_pool()
        user_profile_ctx = UserProfileCtx()
        user_profile_ctx.user_id = "6762bf85b1cf8fa66e085445"
        await user_profile_generator.generate_user_profile(user_profile_ctx)
        
        # 打印user_basic_info的字典表示
        # print("User Basic Info:", user_profile_ctx.user_basic_info.dict())
        # print("User Behavior Info:", user_profile_ctx.user_behavior_info.dict())
        
        await close_redis_pool()
    asyncio.run(main())
