import pandas as pd
import os
import sys


class UserInfo:
	def __init__(self):
		self.df = pd.read_csv("user_info.csv", dtype={"password": str})


	def user_is_exist(self, username, password):
		user = self.df[(self.df["username"] == username) & (self.df["password"] == password)]
		if user.shape[0] > 0:
			return True
		else:
			return False

	def dump_data(self):
		self.df.to_csv("user_info.csv", index=False)

	def user_signup(self, username, password):
		# status code:
		## 0: success
		## 1: username repeated
		user = self.df[(self.df["username"] == username)]
		if user.shape[0] > 0:
			return 1
		else:
			new_user = {"username": username, "password": password, "age": -1, "gender": "", "level": "", "interests": ""}
			self.df = self.df.append(new_user, ignore_index=True)
			self.dump_data()
		return 0

	def update_user_info(self, username, age=None, gender=None, level=None, interests=None):
		# status code:
		## 0: success
		## 1: username cannot find
		user = self.df[(self.df["username"] == username)]
		if user.shape[0] == 0:
			return 1
		if age != None:
			self.df.loc[(self.df["username"] == username), "age"] = age
		if gender != None:
			self.df.loc[(self.df["username"] == username), "gender"] = gender
		if level != None:
			self.df.loc[(self.df["username"] == username), "level"] = level
		if interests != None:
			self.df.loc[(self.df["username"] == username), "interests"] = interests
		self.dump_data()
		return 0

	def fetch_user_info(self, username):
		user = self.df[(self.df["username"] == username)]
		if user.shape[0] > 0:
			return None
		res = dict()
		res["age"] = user.iloc[0]["age"]
		res["level"] = user.iloc[0]["level"]
		res["gender"] = user.iloc[0]["gender"]
		res["interests"] = user.iloc[0]["interests"]
		return res


if __name__ == "__main__":
	pass