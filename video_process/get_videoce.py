import os
import sys
import pandas as pd
import shutil
import json
from video_processor import VideoProcessor, zhihu_url_convert
from tqdm import tqdm
from process_zhihu import load_id2url

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
        if int(vid) < minid or int(vid) > maxid:
            continue
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

def merge_csv(online_csvfile, new_csvfile, metainfo_file, merged_csvfile):
    lines = open(metainfo_file).readlines()
    vid_set = set()
    for l in lines:
        vid_set.add(json.loads(l.strip())["vid"])
    df_online = pd.read_csv(online_csvfile)
    df_new = pd.read_csv(new_csvfile)
    for i in range(df_new.shape[0]):
        vid = df_new.iloc[i]["vid"]
        en_srt = df_new.iloc[i]["en_srt"].replace("/", "\\")
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
            "interests": interests, "level": level, "zhihu_url": zhihu_url}
        df_online = pd.concat([df_online, pd.DataFrame(video_info, index=[0])], ignore_index=True)

    df_online.to_csv(merged_csvfile, index=False)

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

    generate_quiz("zhongdong/Video_Finished", "zhongdong/zhongdong_video_metainfo.jsonl")

def prep_srt_data():
    df = pd.read_csv("../video_info.csv")
    tmp_srt = df.iloc[0]["en_srt"]
    print (tmp_srt)
    video_processor = VideoProcessor()
    import pdb
    pdb.set_trace()
    video_processor.translate_srt(tmp_srt)


if __name__ == '__main__':
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

    prep_srt_data()

