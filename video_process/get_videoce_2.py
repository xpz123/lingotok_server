import os
import sys
import pandas as pd
import shutil
import json
from video_processor import VideoProcessor, zhihu_url_convert, translate_quiz_metainfo, compress_videos
from tqdm import tqdm
from process_zhihu import load_id2url
from vod_huoshan_util import *
from vod_hw_util import upload_hw_withcsv
from content_tagger import tag_video_info_csv_audio_ratio
os.environ["IMAGEIO_FFMPEG_EXE"] = "/opt/homebrew/Cellar/ffmpeg/7.1_3/bin/ffmpeg"
import uuid
from create_video import create_with_csv
from content_tagger import update_video_info_csv_level



def cp_video_ce(df, root_dir, minid=0, maxid=100000):
    if not os.path.exists(root_dir):
        os.makedirs(root_dir)
    for i in range(df.shape[0]):
        ori_video_path = df.iloc[i][1].replace('\\', '/')
        ori_ensrt_path = df.iloc[i][2].replace('\\', '/')
        ori_zhsrt_path = df.iloc[i][3].replace('\\', '/')
        video_filename = ori_video_path.split("/")[-1]
        video_id = video_filename.split("_")[0]
        if int(video_id) < minid or int(video_id) > maxid:
            continue
        ensrt_filename = ori_ensrt_path.split("/")[-1]
        zhsrt_filename = ori_zhsrt_path.split("/")[-1]
        dir_path = "/".join(ori_video_path.split("/")[:-1])
        video_dir = os.path.join(root_dir, dir_path)
        if not os.path.exists(video_dir):
            os.makedirs(video_dir)
        shutil.copyfile(ori_video_path, os.path.join(video_dir, video_filename))
        shutil.copyfile(ori_ensrt_path, os.path.join(video_dir, ensrt_filename))
        shutil.copyfile(ori_zhsrt_path, os.path.join(video_dir, zhsrt_filename))

def cp_esrt(df, ori_dir, root_dir, minid=0, maxid=100000):
    if not os.path.exists(root_dir):
        os.makedirs(root_dir)
    for i in range(df.shape[0]):
        ori_video_path = df.iloc[i][1].replace('\\', '/').replace("Video_Finished",ori_dir)
        ori_ensrt_path = df.iloc[i][2].replace('\\', '/').replace("Video_Finished",ori_dir)
        ori_zhsrt_path = df.iloc[i][3].replace('\\', '/').replace("Video_Finished",ori_dir)

        video_filename = ori_video_path.split("/")[-1]
        video_id = video_filename.split("_")[0]
        if int(video_id) < minid or int(video_id) > maxid:
            continue

        ensrt_filename = ori_ensrt_path.split("/")[-1]
        # zhsrt_filename = ori_zhsrt_path.split("/")[-1]
        dir_path = "/".join(ori_ensrt_path.split("/")[:-1])
        video_dir = os.path.join(root_dir, dir_path)
        if not os.path.exists(video_dir):
            os.makedirs(video_dir)
        # shutil.copyfile(ori_video_path, os.path.join(video_dir, video_filename))
        try:
            shutil.copyfile(ori_ensrt_path, os.path.join(video_dir, ensrt_filename))
        except:
            print (ensrt_filename)
        # shutil.copyfile(ori_zhsrt_path, os.path.join(video_dir, zhsrt_filename))


def prep_video_quiz(quiz_dir, out_file):
    fw = open(out_file, "w")
    for root, dirs, files in os.walk(quiz_dir):
        for f in files:
            vid = f.split("_")[0]
            data = json.load(open(os.path.join(root, f)))
            template = {"vid": vid, "question": data["Question"], "options": data["Options"], "answer": data["Answer"]}
            fw.write(json.dumps(template) + "\n")
    fw.close()


def update_video_info_csv(csv_filename, new_csv_filename, log_csv_filename=None, minid=0, maxid=10000):
    video_processor = VideoProcessor()
    log_dict = []
    df = pd.read_csv(csv_filename)
    for i in tqdm(range(df.shape[0])):
        vid = df.iloc[i]["vid"]
        # if int(vid) < minid or int(vid) > maxid:
        #     continue
        ensrt_filename = df.iloc[i]["en_srt"].replace("\\", "/")
        video_processor.load_srt(ensrt_filename)

        new_tag, reason = video_processor.judge_srt_level()
        if not new_tag in ["A1", "A2", "B1", "B2", "C1", "C2"]:
            new_tag = "B1"
            print ("Cannot tag, use B1")
        else:
            log_dict.append({"srt_text": video_processor.get_srt_text(), "level": new_tag, "reason": reason})

        df.at[i, "level"] = new_tag
    df.to_csv(new_csv_filename)
    if log_csv_filename != None:
        df_log = pd.DataFrame(log_dict)
        df_log.to_csv(log_csv_filename)


def update_quiz_jsonl_withcsv(ori_file, new_file):
    df = pd.read_csv("210 quiz-summary.csv")
    quizd = dict()
    for i in range(df.shape[0]):
        is_fixed = str(df.iloc[i][0]).strip()
        if is_fixed == "1":
            q_opt = df.iloc[i][5]
            q = q_opt.split("\n")[0].replace("Q:", "").strip()
            ans = df.iloc[i][6]
            options = [item.strip() for item in q_opt.replace("Options:", "").split("\n")[1:]]
            vid = df.iloc[i][7].split(".")[0].strip().split("_")[0].strip()

            quizd[vid] = {"question": q, "options": options, "answer": ans}

    lines = open(ori_file).readlines()
    fw = open(new_file, "w")
    for l in lines:
        item = json.loads(l.strip())
        vid = item["vid"]
        if vid in quizd.keys():
            item["question"] = quizd[vid]["question"]
            item["options"] = quizd[vid]["options"]
            item["answer"] = quizd[vid]["answer"]
            print ("update")

        fw.write(json.dumps(item) + "\n")
    fw.close()

def generate_quiz(ensrt_dir, metainfo_file):
    fw = open(metainfo_file, "w")
    video_processor = VideoProcessor()
    for root, dirs, files in os.walk(ensrt_dir):
        for f in files:
            if f.find("English.srt") == -1:
                continue
            try:
                ensrt_filename = os.path.join(root, f.replace("\\", "/"))
                vid = f.split("_")[0]
                video_processor.load_srt(ensrt_filename)
                quiz = video_processor.generate_quiz()
                quiz["vid"] = vid
                # print (quiz)
                fw.write(json.dumps(quiz) + "\n")
            except Exception as e:
                print (str(e))
    fw.close()

def generate_quiz_zh(ensrt_dir, metainfo_file):
    fw = open(metainfo_file, "w", encoding="utf-8")
    video_processor = VideoProcessor()
    for root, dirs, files in os.walk(ensrt_dir):
        for f in tqdm(files):
            if f.find("Chinese.srt") == -1:
                continue
            try:
                zhsrt_filename = os.path.join(root, f.replace("\\", "/"))
                vid = f.split("_")[0]
                video_processor.load_srt(zhsrt_filename)
                quiz = video_processor.generate_quiz_zh_tiankong(zhsrt_filename)
                if quiz == None:
                    continue
                os.system("sleep 1")
                quiz["vid"] = vid
                # print (quiz)
                fw.write(json.dumps(quiz, ensure_ascii=False) + "\n")
            except Exception as e:
                print (str(e))
    fw.close()

def merge_csv(online_csvfile, new_csvfile, metainfo_file, merged_csvfile):
    lines = open(metainfo_file).readlines()
    vid_set = set()
    for l in lines:
        vid_set.add(json.loads(l.strip())["vid"])
    df_online = pd.read_csv(online_csvfile)
    df_new = pd.read_csv(new_csvfile)
    vid_list = list()
    for i in range(df_new.shape[0]):
        vid = df_new.iloc[i]["vid"]
        en_srt = df_new.iloc[i]["en_srt"].replace("/", "\\")
        ar_srt = df_new.iloc[i]["ar_srt"].replace("/", "\\")
        zhihu_url = df_new.iloc[i]["zhihu_url"]
        age = df_new.iloc[i].get("age", "k12")
        gender = df_new.iloc[i].get("gender", "male")
        interests = df_new.iloc[i].get("interests", "Technology")
        level = df_new.iloc[i]["level"]
        if not str(vid) in vid_set:
            continue
        tmp = df_online[(df_online["vid"] == vid)]
        if tmp.shape[0] > 0:
            continue
    
        video_info = {"vid": str(vid), "video_path": "", "en_srt": en_srt, "age": age, "gender": gender, \
            "interests": interests, "level": level, "zhihu_url": zhihu_url, "ar_srt": ar_srt}
        df_online = pd.concat([df_online, pd.DataFrame(video_info, index=[0])], ignore_index=True)
        vid_list.append(str(vid))
    print (vid_list)


    df_online.to_csv(merged_csvfile, index=False)

def merge_csv_huoshan(online_csvfile, new_csvfile):
    # lines = open(metainfo_file).readlines()
    # vid_set = set()
    # for l in lines:
    #     vid_set.add(json.loads(l.strip())["vid"])
    videoid_set = set()
    df_online = pd.read_csv(online_csvfile)
    for i in range(df_online.shape[0]):
         videoid_set.add(str(df_online.iloc[i]["video_id"]))
        
    df_new = pd.read_csv(new_csvfile)
    # vid_list = list()
    for i in range(df_new.shape[0]):
        try:
            video_id = df_new.iloc[i]["video_id"]
            if str(video_id) == "nan":
                continue
            en_srt = df_new.iloc[i]["en_srt"].replace("/", "\\")
            ar_srt = df_new.iloc[i]["ar_srt"].replace("/", "\\")
            zh_srt = df_new.iloc[i]["zh_srt"].replace("/", "\\")
            py_srt = df_new.iloc[i]["pinyin_srt"].replace("/", "\\")
            level = df_new.iloc[i]["level"]
            asset_id = df_new.iloc[i]["asset_id"]
            audio_ratio = df_new.iloc[i]["audio_ratio"]
            audio_dur = df_new.iloc[i]["audio_dur"]
            cover_path = df_new.iloc[i]["cover_path"]
            compressed_FileName = df_new.iloc[i]["compressed_FileName"]
            if str(video_id) in videoid_set:
                continue

            Filename = df_new.iloc[i]["FileName"]
            title = df_new.iloc[i]["title"]
            description = df_new.iloc[i]["description"]

            video_info = {"en_srt": en_srt, "level": level, "ar_srt": ar_srt, "zh_srt": zh_srt, "pinyin_srt": py_srt, "FileName": Filename, "title": title, "description": description, "level": level, "video_id": video_id, "asset_id": asset_id, "audio_ratio": audio_ratio, "audio_dur": audio_dur, "cover_path": cover_path, "compressed_FileName": compressed_FileName}
            df_online = pd.concat([df_online, pd.DataFrame(video_info, index=[0])], ignore_index=True)
            # vid_list.append(str(vid))
        except Exception as e:
            print (df_new.iloc[i]["VID"])
            print (str(e))
    # print (vid_list)


    df_online.to_csv(online_csvfile, index=False)



def prep_tangzong_data():
    # df = pd.read_excel("tangzong.xlsx")
    # tangzong_urld = load_id2url(filename="tangzong_url.txt")
    # drop_idxs = []
    # zhihu_url_list = []
    # for i in range(df.shape[0]):
    #     vid = str(df.iloc[i]["vid"])
    #     if not vid in tangzong_urld.keys():
    #         drop_idxs.append(i)
    #         zhihu_url_list.append("")
    #         continue
    #     zhihu_url_list.append(tangzong_urld[vid])
    # df["zhihu_url"] = zhihu_url_list
    # df.drop(index=drop_idxs, inplace=True)
    # # df.to_csv("tangzong_zhihu.csv", index=False)
    # ori_dir = "tangzong_srt"
    # ensrt_dir = "Video_Finished"
    # srt_filed = dict()
    # for root, dirs, files in os.walk(ori_dir):
    #     for f in files:
    #         vid = f[0:3]
    #         file_path = os.path.join(root, f)
    #         new_file_dir = os.path.join(ensrt_dir, vid)
    #         # os.makedirs(new_file_dir)
    #         new_file_path = os.path.join(new_file_dir, "{}_English.srt".format(vid))
    #         # shutil.copyfile(file_path, new_file_path)
    #         srt_filed[vid] = new_file_path
    
    # drop_idxs = []
    # ensrt_list = []
    # zhsrt_list = []
    # for i in range(df.shape[0]):
    #     vid = str(df.iloc[i]["vid"])
    #     if not vid in srt_filed.keys():
    #         drop_idxs.append(i)
    #         ensrt_list.append("")
    #         zhsrt_list.append("")
    #         continue
    #     ensrt_list.append(srt_filed[vid])
    #     zhsrt_list.append(srt_filed[vid].replace("English", "Chinese"))
    # df["en_srt"] = ensrt_list
    # df["zh_srt"] = zhsrt_list
    # df.drop(index=drop_idxs, inplace=True)

    # df.to_csv("tangzong_video_info.csv", index=False)
    df = pd.read_csv("tangzong_final_video_info.csv")
    vip_video_list = []
    
    for i in range(df.shape[0]):
        vid = str(df.iloc[i]["vid"])
        vip_video_list.append(vid)
    vipd = {"username": "investor_001", "video_ids": vip_video_list}
    fw = open("../vip_video_id.jsonl", "w")
    fw.write(json.dumps(vipd))


def prep_zhongdong_data():
    pass
    # df = pd.read_csv("zhongdong/zhihu_ori_0_20.csv")
    # srt_dir = "zhongdong/Video_Finished"
    # zhihu_url_list = []
    # en_srt_name_list = []
    # zh_srt_name_list = []
    # ar_srt_name_list = []
    # for i in tqdm(range(df.shape[0])):
    #     try:
    #         vid = df.iloc[i]["vid"]
    #         page_url = df.iloc[i]["zhihu_ori_url"]
    #         static_url, play_url_dict =  zhihu_url_convert(page_url)
    #         zhihu_url_list.append(static_url)
    #         video_processor = VideoProcessor()
    #         srt_vid_dir = os.path.join(srt_dir, vid)
    #         os.makedirs(srt_vid_dir)
    #         srt_name = srt_vid_dir + "/" + vid
    #         play_url = ""
    #         if "HD" in play_url_dict.keys():
    #             play_url = play_url_dict["HD"]
    #         elif "LD" in play_url_dict.keys():
    #             play_url = play_url_dict["LD"]
    #         elif "SD" in play_url_dict.keys():
    #             play_url = play_url_dict["SD"]
    #         srtd = video_processor.generate_srt(play_url, srt_name, gen_ar=True, gen_zh=True)
    #         en_srt_name_list.append(srtd["er_srt"].replace("/", "\\"))
    #         zh_srt_name_list.append(srtd["zh_srt"].replace("/", "\\"))
    #         ar_srt_name_list.append(srtd["ar_srt"].replace("/", "\\"))
    #     except Exception as inst:
    #         print (vid)
    #         print (static_url)
    #         print (str(inst))
    #         if len(zhihu_url_list) == i:
    #             zhihu_url_list.append("")
    #         en_srt_name_list.append("")
    #         zh_srt_name_list.append("")
    #         ar_srt_name_list.append("")
    # df["zhihu_url"] = zhihu_url_list
    # df["srt_name"] = en_srt_name_list
    # df["zh_srt_name"] = zh_srt_name_list
    # df["ar_srt_name"] = ar_srt_name_list
    # df.to_csv("zhongdong/zhihu_url_srt.csv", index=False)

    # generate_quiz("zhongdong/Video_Finished", "zhongdong/zhongdong_video_metainfo.jsonl")
    # update_video_info_csv("zhongdong/zhihu_url_srt.csv", "zhongdong/zhihu_url_srt_level.csv")
    # merge_csv("../video_info.csv", "zhongdong/zhihu_url_srt_level.csv", "../video_metainfo.jsonl", "video_info_merged.csv")

def prep_huoshan_data():
    ### 已经完成的
    # video_dir = "huoshan/短剧/8233-被偷走爱的那十年（43集）"
    # video_dir = "huoshan/家有儿女"
    # video_dir = "huoshan/舌尖"
    # video_dir = "huoshan/航拍中国"
    # video_dir = "huoshan/琅琊榜"
    # video_dir = "huoshan/山海情"
    # video_dir = "huoshan/武林外传1"
    # video_dir = "huoshan/武林外传2"
    # video_dir = "huoshan/短剧/8152-来自星星的4个哥哥都宠我（100集）"
    # video_dir = "huoshan/短剧/8150-叮！我的首富老公已上线（82集）"
    # video_dir = "huoshan/短剧/于龙怕热"

    ###正在进行中
    video_dir = "huoshan/萌宠/爱撒娇的淘气喵"
    video_dir = "huoshan/萌宠/丢那猩动物园"
    video_dir = "huoshan/萌宠/苏苏家的三小只"
    video_dir = "huoshan/萌宠/永智与志胜"
    video_dir = "huoshan/其他/科普星球"
    video_dir = "huoshan/其他/十六不是石榴"
    video_dir = "/Users/tal/work/lingtok_server/video_process/huoshan/美妆/阿min学姐"
    video_dir = "/Users/tal/work/lingtok_server/video_process/huoshan/萌宠/开饭了大熊猫-part"
    video_dir = "/Users/tal/work/lingtok_server/video_process/huoshan/美妆/今天要不要去吃烧烤"
    video_dir = "/Users/tal/work/lingtok_server/video_process/huoshan/其他/清风乍起"

    ### 未完成的

    # video_dir = "huoshan/短剧/8244-被弄丢的你（86集）"
    # video_dir = "huoshan/武林外传"
    

    srt_dir = os.path.join(video_dir, "srt_dir")
    out_csv_file = os.path.join(video_dir, "video_info_test.csv")
    srt_csv_file = os.path.join(video_dir, "video_info_test_srt.csv")

    video_processor = VideoProcessor()

    traverse_and_upload(video_dir, out_csv_file)
    
    # if not os.path.exists(srt_dir):
    #     os.makedirs(srt_dir)

    # df = pd.read_csv(out_csv_file)
    # dict_by_list = df.to_dict(orient="list")
    # zh_srt_list = []
    # ar_srt_list = []
    # en_srt_list = []
    # for i in tqdm(range(df.shape[0])):
    #     vid = df.iloc[i]["VID"]
    #     video_path = df.iloc[i]["FileName"].replace(" ", "\\ ")
    #     # playurl = get_vid_playurl(vid)
    #     os.system("/opt/homebrew/Cellar/ffmpeg/7.1_3/bin/ffmpeg -y -loglevel error -i {} -ac 1 -ar 16000 -f wav test.wav".format(video_path))
    #     srt_res = video_processor.generate_zhsrt("",  os.path.join(srt_dir, vid), audio_path="test.wav", gen_ar=True)
    #     os.system("rm test.wav")
    #     if srt_res == None:
    #         zh_srt_list.append("null")
    #         ar_srt_list.append("null")
    #         en_srt_list.append("null")
    #     else:
    #         zh_srt_list.append(srt_res["zh_srt"])
    #         ar_srt_list.append(srt_res["ar_srt"])
    #         en_srt_list.append(srt_res["en_srt"])
    # dict_by_list["zh_srt"] = zh_srt_list
    # dict_by_list["ar_srt"] = ar_srt_list
    # dict_by_list["en_srt"] = en_srt_list
    # new_df = pd.DataFrame(dict_by_list)
    # new_df.to_csv(srt_csv_file, index=False)

    # generate_quiz_zh(srt_dir, os.path.join(video_dir, "video_metainfo_zhonly.jsonl"))
    # merge_csv_huoshan("video_info_huoshan.csv", srt_csv_file, os.path.join(video_dir, "video_metainfo_zhonly.jsonl"))
    # os.system("scp  {}/*.srt root@54.248.147.60:/dev/data/lingotok_server/huoshan/srt_dir".format(srt_dir))
    # translate_quiz_metainfo(os.path.join(video_dir, "video_metainfo_zhonly.jsonl"), os.path.join(video_dir, "video_metainfo.jsonl"))
    # os.system("cat {} >> ../video_metainfo.jsonl".format(os.path.join(video_dir, "video_metainfo.jsonl")))
    # tag_video_info_csv_audio_ratio("video_info_huoshan.csv", "../video_info_huoshan.csv")

def prep_aigc_huoshan():
    csv_file = "/Users/tal/work/lingtok_server/DR_1.csv"
    zh_metainfo_file = "/Users/tal/work/lingtok_server/video_process/沙特女子Demo/DR_1_metainfo_zh.jsonl"
    metainfo_file = "/Users/tal/work/lingtok_server/video_process/沙特女子Demo/DR_1_metainfo_all.jsonl"
    metainfo_file_withvid = "/Users/tal/work/lingtok_server/video_process/沙特女子Demo/DR_1_metainfo_final.jsonl"

    df = pd.read_csv(csv_file)
    columns = df.columns.tolist()
    columns.append("vid")
    df_list = df.values.tolist()
    # fw = open(zh_metainfo_file, "w", encoding="utf-8")
    fw = open(metainfo_file_withvid, "w", encoding="utf-8")
    video_processor = VideoProcessor()
    lines = open(metainfo_file).readlines()
    for i in tqdm(range(df.shape[0])):
        video_path = df.iloc[i]["FileName"]
        cmd = "/opt/homebrew/Cellar/ffmpeg/7.1_3/bin/ffmpeg -y -loglevel error -i {} -vf subtitles={} {}".format(video_path.replace(" ", "\\ "), df.iloc[i]["zh_srt"].replace(" ", "\\ "), video_path.replace(" ", "\\ ").replace(".mp4", "_zh.mp4"))
        os.system(cmd)
        res = upload_media(video_path.replace(".mp4", "_zh.mp4"), space_name="lingotok", tag="DR1", desc="DR1")
        vid = res["vid"]
        df_list[i].append(vid)
        quiz = json.loads(lines[i].strip())
        quiz["vid"] = vid
        # zh_srt = df.iloc[i]["zh_srt"]
        # quiz = video_processor.generate_quiz_zh_tiankong(zh_srt)
        # if quiz == None:
        #     continue
        # os.system("sleep 1")
        fw.write(json.dumps(quiz, ensure_ascii=False) + "\n")
    fw.close()
    df_new = pd.DataFrame(df_list, columns=columns)
    df_new.to_csv(csv_file, index=False)

    # translate_quiz_metainfo(zh_metainfo_file, metainfo_file)


def prep_srt_data():
    df = pd.read_csv("video_info.csv")
    # zh_srt_list = list()
    ar_srt_list = list()
    video_processor = VideoProcessor()
    for i in tqdm(range(df.shape[0])):
        try:
            tmp_srt = df.iloc[i]["en_srt"]
            res = video_processor.translate_srt(tmp_srt)
            ar_srt_list.append(res["ar_srt"].replace("/", "\\"))
        except:
            print ("bad data")
            ar_srt_list.append("")
    df["ar_srt"] = ar_srt_list
    df.to_csv("video_info_withar.csv")
    
def prep_hw_data():

    # 等待merge
    video_dir_list = []
    # video_dir_list.append("/Users/tal/work/lingtok_server/video_process/hw/videos/记录生活/阿华日记")
    # video_dir_list.append("/Users/tal/work/lingtok_server/video_process/hw/videos/影视/阿奇讲电影")
    # video_dir_list.append("/Users/tal/work/lingtok_server/video_process/hw/videos/记录生活/大白在广州")
    # video_dir_list.append("/Users/tal/work/lingtok_server/video_process/hw/videos/记录生活/黄姐夫在德国")
    
    # 字幕完成
    # video_dir = "/Users/tal/work/lingtok_server/video_process/hw/videos/科技/4AM居士"
    # video_dir = "/Users/tal/work/lingtok_server/video_process/hw/videos/科技/何同学"
    

    # 字幕进行中
    video_dir = "/Users/tal/work/lingtok_server/video_process/hw/videos/科技/亿点点不一样"
    
    video_list = list()
    cover_dict = dict()
    for root, dirs, files in os.walk(video_dir):
        for f in files:
            if f.find(".mp4") != -1:
                video_list.append(os.path.join(root, f))
                cover_path = os.path.join(root, f.replace(".mp4", ".jpg"))
                if not os.path.exists(cover_path):
                    cover_dict[os.path.join(root, f)] = ""
                else:
                    cover_dict[os.path.join(root, f)] = cover_path

    srt_dir = os.path.join(video_dir, "srt_dir")
    out_csv_file = os.path.join(video_dir, "video_info.csv")
    srt_csv_file = os.path.join(video_dir, "video_info_srt.csv")
    # chunk_csv_file = os.path.join(video_dir, "video_info_chunk.csv")

    quiz_zh_metainfo_file = os.path.join(video_dir, "video_metainfo_zh.jsonl")
    quiz_metainfo_file = os.path.join(video_dir, "video_metainfo.jsonl")

    cover_csv_file = os.path.join(video_dir, "video_info_cover.csv")
    compressed_csv_file = os.path.join(video_dir, "video_info_compressed.csv")
    vod_csv_file = os.path.join(video_dir, "video_info_vod.csv")
    tag_csv_file = os.path.join(video_dir, "video_info_tag.csv")
    
    # For debug
    skip_srt = False
    skip_quiz = False
    skip_add_cover = False
    skip_tag_video = False
    # skip_compress = True
    skip_upload = True
    
    skip_create = True

    video_processor = VideoProcessor()

    if not skip_srt:
    
        if not os.path.exists(srt_dir):
            os.makedirs(srt_dir)

        columns = ["FileName", "title", "description", "zh_srt", "ar_srt", "en_srt", "pinyin_srt"]
        df_list = list()
        for i in tqdm(range(len(video_list))):
            item = list()
            video_path = video_list[i]
            srt_name = str(uuid.uuid4())
            item.append(video_path)
            title = "{}_{}".format(video_path.split("/")[-2], video_path.split("/")[-1].split(".")[0])
            item.append(title)
            description = video_path.split("/")[-2]
            item.append(description)
            os.system("/opt/homebrew/Cellar/ffmpeg/7.1_3/bin/ffmpeg -y -loglevel error -i {} -ac 1 -ar 16000 -f wav test.wav".format(video_path.replace(" ", "\\ ").replace("&", "\\&")))
            srt_res = video_processor.generate_zhsrt("",  os.path.join(srt_dir, srt_name), audio_path="test.wav", gen_ar=True)
            os.system("rm test.wav")
            if srt_res == None:
                continue
            else:
                item.append(srt_res["zh_srt"])
                item.append(srt_res["ar_srt"])
                item.append(srt_res["en_srt"])
                item.append(srt_res["pinyin_srt"])
            df_list.append(item)
        df_srt = pd.DataFrame(df_list, columns=columns)
        df_srt.to_csv(srt_csv_file, index=False)
    
    if not skip_quiz:
        generate_quiz_zh(srt_dir, quiz_zh_metainfo_file)
        translate_quiz_metainfo(quiz_zh_metainfo_file, quiz_metainfo_file)
    
    if not skip_add_cover:
        df_srt = pd.read_csv(srt_csv_file)
        columns = df_srt.columns.tolist()
        columns.append("cover_path")
        df_srt_list = df_srt.values.tolist()
        for i in range(len(df_srt_list)):
            video_path = df_srt.iloc[i]["FileName"]
            df_srt_list[i].append(cover_dict[video_path])
        df_cover = pd.DataFrame(df_srt_list, columns=columns)
        df_cover.to_csv(cover_csv_file, index=False)
    
    if not skip_tag_video:
        update_video_info_csv_level(cover_csv_file, tag_csv_file)
    # if not skip_compress:
    #     compress_videos(cover_csv_file, compressed_csv_file)
    
    if not skip_upload:
        if os.path.exists(vod_csv_file):
            upload_hw_withcsv(vod_csv_file, vod_csv_file)
        else:
            upload_hw_withcsv(compressed_csv_file, vod_csv_file)
        
    
    
    if not skip_create:
        create_with_csv(quiz_metainfo_file, tag_csv_file, out_csv_file)
    
    # merge_csv_huoshan("/Users/tal/work/lingtok_server/video_info_hw_created.csv", out_csv_file)

    # generate_quiz_zh(srt_dir, os.path.join(video_dir, "video_metainfo_zhonly.jsonl"))
    # merge_csv_huoshan("video_info_huoshan.csv", srt_csv_file, os.path.join(video_dir, "video_metainfo_zhonly.jsonl"))
    # os.system("scp  {}/*.srt root@54.248.147.60:/dev/data/lingotok_server/huoshan/srt_dir".format(srt_dir))
    # translate_quiz_metainfo(os.path.join(video_dir, "video_metainfo_zhonly.jsonl"), os.path.join(video_dir, "video_metainfo.jsonl"))
    # os.system("cat {} >> ../video_metainfo.jsonl".format(os.path.join(video_dir, "video_metainfo.jsonl")))
    # tag_video_info_csv_audio_ratio("video_info_huoshan.csv", "../video_info_huoshan.csv")

if __name__ == '__main__':
    prep_hw_data()
    # prep_aigc_huoshan()
    # prep_huoshan_data()
    # df = pd.read_csv("video_info_530.csv")
    # cp_esrt(df, "video_Finished_361_525_ori", "video_Finished_361_525_ensrt", minid=361, maxid=525)
    
    # update_quiz_jsonl_withcsv("video_metainfo.jsonl", "video_metainfo_new.jsonl")
    # df = pd.read_csv("video_info_0909.csv")
    # cp_video_ce(df, "video_database")
    # df = pd.read_csv("video_info_0914.csv")
    # cp_video_ce(df, "video_database_0914_videoce", minid=71)
    # prep_video_quiz("Generated_Questions_0918", "video_metainfo.jsonl")
    # update_video_info_csv("../lingtok_server/video_info.csv", "../lingtok_server/video_info_refine.csv", log_csv_filename="210_reason_tag.csv")
    # update_video_info_csv("../lingtok_server/video_info.csv", "../lingtok_server/video_info_refine_14B.csv", log_csv_filename="210_reason_tag_14B.csv")
    # update_video_info_csv("../lingtok_server/video_info.csv", "../lingtok_server/video_info_refine_14B_3shot.csv", log_csv_filename="210_reason_tag_14B_3shot.csv")
    # generate_quiz("video_Finished_361_525_ensrt", "video_metainfo_361_525.jsonl")
    # merge_csv("../lingtok_server/video_info.csv", "video_info_530.csv", "/Users/tal/work/lingtok/lingtok_server/video_metainfo.jsonl", "video_info_merged.csv")

    # df = pd.read_csv("video_info_530.csv")
    # cp_esrt(df, "video_Finished_1_377_ori", "video_Finished_211_360_ensrt", minid=211, maxid=360)
    # generate_quiz("video_Finished_211_360_ensrt", "video_metainfo_211_360.jsonl")
    # merge_csv("../lingtok_server/video_info.csv", "video_info_530.csv", "/Users/tal/work/lingtok/lingtok_server/video_metainfo.jsonl", "video_info_merged_1_530.csv")
    # update_video_info_csv("video_info_merged_1_530.csv", "video_info_merged_1_530_relevel.csv", log_csv_filename="530_7b_reason_tag.csv", minid=211, maxid=600)

    # prep_tangzong_data()
    # update_video_info_csv("tangzong_video_info.csv", "tangzong_video_info_level.csv")
    # generate_quiz("Video_Finished", "tangzong_video_metainfo.jsonl")
    # merge_csv("../video_info.csv", "tangzong_video_info_level.csv", "/Users/tal/work/lingtok/lingtok_server/video_metainfo.jsonl", "video_info_merged_tangzong.csv")

    # prep_zhongdong_data()

    # prep_srt_data()


