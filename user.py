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
			new_user = {"username": username, "password": password}
			self.df = self.df.append(new_user, ignore_index=True)
			self.dump_data()
		return 0


if __name__ == "__main__":
	pass