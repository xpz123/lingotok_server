import pandas as pd
import os
import sys
import json
import pickle as pkl
from copy import deepcopy


class UserInfo:
	def __init__(self):
		self.df = pd.read_csv("user_info.csv", dtype={"password": str})
		self.user_behavior_df = pd.read_csv("user_analysis_info.csv")
		# 暂时使用pickle文件记录，后面考虑切换为mongodb
		# user_video_df: {"user1": {"liked_videos": ["vid1", "vid2"], "collected_videos": ["vid1"], "video_detail_info": {"vid1": {"watched_dur": 100, "complete_count": 3}}}}
		self.empty_video_templete = {"liked": [], "collected": [], "video_detail_info": {}}
		if os.path.exists("user_video.pkl"):
			self.user_video_df = pkl.load(open("user_video.pkl", "rb"))
		else:
			self.user_video_df = {}
		
		# user_learning_df: {"user1": {"speak_detail_info": {"vid1": {}}, "quiz_detail_info": {}}}
		self.empty_learning_templete = {"speak_detail_info": {}, "quiz_detail_info": {}}
		if os.path.exists("user_learning.pkl"):
			self.user_learning_df = pkl.load(open("user_learning.pkl", "rb"))
		else:
			self.user_learning_df = {}

		self.username_video_dict = dict()
		lines = open("vip_video_id.jsonl").readlines()
		for l in lines:
			item = json.loads(l.strip())
			self.username_video_dict[item["username"]] = item["video_ids"]

	def user_is_exist(self, username, password):
		user = self.df[(self.df["username"] == username) & (self.df["password"] == password)]
		if user.shape[0] > 0:
			return True
		else:
			return False

	def dump_info(self):
		self.df.to_csv("user_info.csv", index=False)
	
	def dump_behavior(self):
		self.user_behavior_df.to_csv("user_analysis_info.csv", index=False)
	
	def dump_user_video(self):
		with open("user_video.pkl", "wb") as fw:
			pkl.dump(self.user_video_df, fw)
	
	def dump_user_learning(self):
		with open("user_learning.pkl", "wb") as fw:
			pkl.dump(self.user_learning_df, fw)

	def user_signup(self, username, password):
		# status code:
		## 0: success
		## 1: username repeated
		user = self.df[(self.df["username"] == username)]
		if user.shape[0] > 0:
			return 1
		else:
			# Defulat info: age-5 gender-female level-easy interests-
			new_user = {"username": username, "password": password, "age": 5, "gender": "female", "level": "easy", "interests": ""}
			self.df = pd.concat([self.df, pd.DataFrame(new_user, index=[0])], ignore_index=True)
			self.dump_info()
		return 0

	def update_user_info(self, username, age=None, gender=None, level=None, interests=None):
		# status code:
		## 0: success
		## 1: username cannot find
		user = self.df[(self.df["username"] == username)]
		if user.shape[0] == 0:
			new_user = {"username": username}
			if not age == None:
				new_user["age"] = int(age)
			if not gender == None:
				new_user["gender"] = gender
			if not level == None:
				new_user["level"] = level
			if not interests == None:
				new_user["interests"] = interests
			self.df = pd.concat([self.df, pd.DataFrame(new_user, index=[0])], ignore_index=True)
			self.dump_info()
			return 0
		if age != None:
			self.df.loc[(self.df["username"] == username), "age"] = int(age)
		if gender != None:
			self.df.loc[(self.df["username"] == username), "gender"] = gender
		if level != None:
			self.df.loc[(self.df["username"] == username), "level"] = level
		if interests != None:
			self.df.loc[(self.df["username"] == username), "interests"] = interests
		self.dump_info()
		return 0
	
	def update_user_behavior(self, username, behavior_dict):
		# status code:
		## 0: success
		## 1: failed
		user = self.user_behavior_df[(self.user_behavior_df["username"] == username)]
		if user.shape[0] == 0:
			new_user = behavior_dict
			new_user["username"] = username
			self.user_behavior_df = pd.concat([self.user_behavior_df, pd.DataFrame(new_user, index=[0])], ignore_index=True)
		else:
			for key in behavior_dict.keys():
				self.user_behavior_df.loc[self.user_behavior_df["username"] == username, key] = behavior_dict[key]

		self.dump_behavior()
		return 0

	def update_video_status(self, username, vid, watched_video_duration, video_status):
		if not username in self.user_video_df.keys():
			self.user_video_df[username] = deepcopy(self.empty_video_templete)
		
		if video_status != None:
			assert video_status in ["liked", "disliked", "collected", "uncollected"]
			if video_status == "liked":
				self.user_video_df[username]["liked"].append(vid)
			if video_status == "disliked":
				self.user_video_df[username]["liked"] = [item for item in self.user_video_df[username]["liked"] if item != vid]
			if video_status == "collected":
				self.user_video_df[username]["collected"].append(vid)
			if video_status == "uncollected":
				self.user_video_df[username]["collected"] = [item for item in self.user_video_df[username]["collected"] if item != vid]
		
		if watched_video_duration != None:
			if vid in self.user_video_df[username]["video_detail_info"].keys():
				self.user_video_df[username]["video_detail_info"][vid]["watched_video_duration"] += watched_video_duration
			else:
				self.user_video_df[username]["video_detail_info"][vid] = {"watched_video_duration": watched_video_duration, "is_complete_count": 0}
		
		# print (self.user_video_df)
		self.dump_user_video()
		return 0

	
	def update_learning_status(self, username, vid, quiz_status, speaking_status):
		if not username in self.user_learning_df.keys():
			self.user_learning_df[username] = deepcopy(self.empty_learning_templete)
		
		assert not (quiz_status == None and speaking_status == None)
		
		if quiz_status != None:
			assert quiz_status in ["skip", "error", "right"]
			if vid in self.user_learning_df[username].keys():
				self.user_learning_df[username][vid].append(quiz_status)
			else:
				self.user_learning_df[username][vid] = [quiz_status]
		
		if speaking_status != None:
			pass

		print (self.user_learning_df)

		self.dump_user_learning()

		return 1


	def process_vip(self, username):
		if not username in self.username_video_dict.keys():
			return None
		return self.username_video_dict[username]
	
	def fetch_user_info(self, username):
		user = self.df[(self.df["username"] == username)]
		if user.shape[0] <= 0:
			return None
		res = dict()
		res["age"] = user.iloc[0]["age"]
		res["level"] = user.iloc[0]["level"]
		res["gender"] = user.iloc[0]["gender"]
		res["interests"] = user.iloc[0]["interests"]
		return res


if __name__ == "__main__":
	pass