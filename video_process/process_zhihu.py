import os
import sys
import pandas as pd
import requests
import pickle as pkl

def process_zhihu_video_csv(csv_file="zhihu_video.csv"):
    prefix = "https://lens.zhihu.com/api/v4/videos/"
    df = pd.read_csv(csv_file)
    res = dict()
    fw = open("tmp.txt", "a")
    for i in range(515, df.shape[0]):
        try:
            id = df.iloc[i]["ID"]
            page_url = df.iloc[i]["URL"]
            html_text = requests.get(page_url).text
            vid_begin_idx = html_text.find("videoId") + 10
            vid_end_idx = html_text[vid_begin_idx:].find('"') + vid_begin_idx
            vid = html_text[vid_begin_idx:vid_end_idx]
            print ("{},{}".format(id, vid))
            fw.write("{},{}".format(id, vid) + "\n")
            new_url = "{}{}".format(prefix, vid)
            res[id] = new_url
        except:
            pass
    fw.close()
    return res

def load_id2url():
    prefix = "https://lens.zhihu.com/api/v4/videos/"
    res = dict()
    lines = open("zhihu_0_520_url.txt")
    for l in lines:
        id = l.strip().split(",")[0]
        url = "{}{}".format(prefix, l.strip().split(",")[1])
        res[id] = url
    return res

def add_url_to_video_info(id2url):
    df = pd.read_csv("video_info_merged_1_530_relevel.csv", index_col=None)
    idx_to_drop = []
    url_list = []
    count = 0
    for i in range(df.shape[0]):
        vid = str(df.iloc[i]["vid"])
        if not vid in id2url.keys():
            idx_to_drop.append(i)
    df.drop(index=idx_to_drop, inplace=True)

    for i in range(df.shape[0]):
        vid = str(df.iloc[i]["vid"])
        url_list.append(id2url[vid])
    df["zhihu_url"] = url_list
    df.to_csv("video_info_merged_1_530_relevel_withzhihu.csv", index=False)


if __name__ == "__main__":
    # video_urld = process_zhihu_video_csv()
    # print (video_urld)
    id2url = load_id2url()
    add_url_to_video_info(id2url)