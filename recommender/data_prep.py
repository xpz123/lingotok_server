import pandas as pd
import sys
import os
from collections import defaultdict
import requests
from tqdm import tqdm
import time

def sort_strings_by_number(strings):
    def sort_key(s):
        s = s.replace(".mp4", "")
        idx = s.find("NEW")
        if idx != -1:
            number_part = s[idx + 3:]
            if number_part.startswith("-"):
                number_part = number_part[1:]
            else:
                number_part = number_part.split("-")[0]
            try:
                number = int(number_part)
            except ValueError:
                print("OMG, bad filename: ", s)
                number = float("inf")
            return number
        else:
            print("OMG, no NEW: ", s)
            return float("inf")

    sorted_strings = sorted(strings, key=sort_key)
    return sorted_strings

def sort_csv(filename):
    data = pd.read_csv(filename)
    #series_name, FileName
    index = list(range(0, len(data)))
    series_dict = {}
    series_raw_dict = {}
    series_raw_real_dict = {}
    series_raw_real_sorted_dict = {}
    # pdb.set_trace()
    for i in index:
        item = data.iloc[i]
        series_name = item['series_name']
        filepath = item['FileName']
        filename = os.path.basename(filepath).split("\\")[-1]
        if series_name not in series_dict.keys():
            series_dict[series_name] = []
        series_dict[series_name].append(filename)
    # pdb.set_trace()
    # extract series, raw filename and real filename
    for key, value in series_dict.items():
        series_raw_dict[key] = []
        series_raw_real_dict[key] = {}
        for filename in value:
            temp_list = filename.split("NEW")
            if len(temp_list) == 1:
                series_raw_dict[key].append(filename)
                if filename not in series_raw_real_dict[key].keys():
                    series_raw_real_dict[key][filename] = [filename]
                else:
                    print("Sad, duplicated filename: ", filename)
            elif len(temp_list) == 2:
                raw_filename = temp_list[0]
                if raw_filename not in series_raw_dict[key]:
                    series_raw_dict[key].append(raw_filename)
                    series_raw_real_dict[key][raw_filename] = [filename]
                else:
                    series_raw_real_dict[key][raw_filename].append(filename)
            else:
                print("Sad, it's a invaild filename: ", filename)
    # pdb.set_trace()
    # sort final filename
    for key, value in series_raw_real_dict.items():
        series_raw_real_sorted_dict[key] = {}
        for key2, value2 in value.items():
            if len(value2) == 1:
                series_raw_real_sorted_dict[key][key2] = value2
            else:
                sorted_value2 = sort_strings_by_number(value2)
                series_raw_real_sorted_dict[key][key2] = sorted_value2
    # pdb.set_trace()
    return series_raw_real_sorted_dict

def tag_video_series(input_csv, output_csv):
    df = pd.read_csv(input_csv)
    df_list = df.values.tolist()
    columns = df.columns.to_list()
    columns.append("series_name")
    # columns.append("series_id")
    pop_index = []
    for i in range(df.shape[0]):
        video_path = df.iloc[i]["FileName"]
        if video_path.find("(1).mp4") != -1:
            pop_index.append(i)
        series_name = video_path.replace("\\", "/").split("/")[-2]
        df_list[i].append(series_name)
    df_new = pd.DataFrame(df_list, columns=columns)
    df_new = df_new.drop(pop_index)
    df_new.to_csv(output_csv, index=False)

def tag_video_seqnum(input_csv, output_csv):
    df = pd.read_csv(input_csv)
    df_list = df.values.tolist()
    columns = df.columns.to_list()
    columns.append("seq_num")

    series_raw_real_sorted_dict = sort_csv(input_csv)

    series_raw_real_seqdict = dict()
    for series_name in series_raw_real_sorted_dict.keys():
        series_raw_real_seqdict[series_name] = {}
        seq_num = 0
        series_dict = series_raw_real_sorted_dict[series_name]
        for ori_video_name in series_dict.keys():
            for video_name in series_dict[ori_video_name]:
                series_raw_real_seqdict[series_name][video_name] = seq_num
                seq_num += 1
    
    for i in range(df.shape[0]):
        video_path = df.iloc[i]["FileName"]
        series_name = df.iloc[i]["series_name"]
        video_name = video_path.replace("\\", "/").split("/")[-1]
        try:
            seq_num = series_raw_real_seqdict[series_name][video_name]
        except:
            seq_num = -1
            print ("cannot find {}".format(video_name))
        df_list[i].append(seq_num)
    df_new = pd.DataFrame(df_list, columns=columns)
    df_new.to_csv(output_csv, index=False)

def update_video_seriesname_seqnum(input_csv):
    url ="https://api.lingotok.ai/api/v1/video/update_video_info"
    df = pd.read_csv(input_csv)
    for i in tqdm(range(2500, df.shape[0])):
        try:
            req = {}
            req["video_id"] = df.iloc[i]["video_id"]
            req["series_name"] = df.iloc[i]["series_name"]
            req["series_sequence"] = int(df.iloc[i]["seq_num"])
            response = requests.post(url, json=req, headers={"Content-Type": "application/json", "Authorization": "skip_auth", "Env": "test"})
            time.sleep(0.5)
        except Exception as e:
            print (str(e))
            time.sleep(5)

def update_hsk_videos(input_csv):
    url ="https://api.lingotok.ai/api/v1/video/update_video_info"
    df = pd.read_csv(input_csv)
    count = 0
    for i in tqdm(range(df.shape[0])):
        try:
            req = {}
            req["video_id"] = df.iloc[i]["video_id"]
            req["customize"] = "HSK_DIY"
            series_name = df.iloc[i]["series_name"]
            if series_name.find("HSK output_new_2") == -1:
                continue
            response = requests.post(url, json=req, headers={"Content-Type": "application/json", "Authorization": "skip_auth", "Env": "test"})
            time.sleep(0.1)
        except Exception as e:
            print (str(e))
            time.sleep(5)
    
        

if __name__ == "__main__":
    # tag_video_series("../video_info_hw_created_nona.csv", "test.csv")
    # tag_video_seqnum("video_info_with_sname.csv", "video_info_with_sname_seqidx.csv")
    # update_video_seriesname_seqnum("video_info_with_sname_seqidx.csv")
    update_hsk_videos("video_info_with_sname_seqidx.csv")