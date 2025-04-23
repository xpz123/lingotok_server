import pandas as pd
import os

df_srt = pd.read_csv("/Users/tal/work/lingtok_server/video_process/hw/videos/记录生活/小悦de生活日记/video_info_srt.csv")
print (df_srt.shape[0])
import pdb
pdb.set_trace()
for i in range(df_srt.shape[0]):
    if os.path.getsize(df_srt.iloc[i]["zh_srt"]) == 0:
        print ("Empty SRT file: {}".format(df_srt.iloc[i]["zh_srt"]))
        df_srt.drop(i, inplace=True)
print (df_srt.shape[0])