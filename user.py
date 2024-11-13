import pandas as pd
import os
import sys
import json


class UserInfo:
	def __init__(self):
		self.df = pd.read_csv("user_info.csv", dtype={"password": str})
		self.user_behavior_df = pd.read_csv("user_analysis_info.csv")

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