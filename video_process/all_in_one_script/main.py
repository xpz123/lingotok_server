import os
import sys
import pandas as pd
import shutil
import json
from video_processor import VideoProcessor, zhihu_url_convert, translate_quiz_metainfo, compress_videos
from tqdm import tqdm
from vod_huoshan_util import *
from vod_hw_util import upload_hw_withcsv
from content_tagger import tag_video_info_csv_audio_ratio
# os.environ["IMAGEIO_FFMPEG_EXE"] = "/opt/homebrew/Cellar/ffmpeg/7.1_4/bin/ffmpeg"
import uuid
from create_video import create_with_csv, update_videoinfo_recommender_withcsv
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

    
def prep_hw_data(video_dir_list):

    for video_dir in video_dir_list:
        print ("#################### Processing {}####################".format(video_dir))
        video_dir = video_dir.replace("\\", "/")
        video_list = list()
        cover_dict = dict()
        for root, dirs, files in os.walk(video_dir):
            for f in files:
                # if f.find(".mp4") != -1 or f.find(".mov") != -1:
                #     video_list.append(os.path.join(root, f))
                #     cover_path = os.path.join(root, f.replace(".mp4", ".jpg"))
                #     if not os.path.exists(cover_path):
                #         cover_dict[os.path.join(root, f)] = ""
                #     else:
                #         cover_dict[os.path.join(root, f)] = cover_path
                
                if f.find(".mp4") != -1:
                    video_list.append(os.path.join(root, f))
                    cover_path = os.path.join(root, f.replace(".mp4", ".jpg"))
                    if not os.path.exists(cover_path):
                        cover_dict[os.path.join(root, f)] = ""
                    else:
                        cover_dict[os.path.join(root, f)] = cover_path
                elif f.find(".mov") != -1:
                    mp4_file = os.path.join(root, f.replace(".mov", ".mp4"))
                    if os.path.exists(mp4_file):
                        video_list.append(os.path.join(root, f.replace(".mov", ".mp4")))
                    else:
                        video_list.append(os.path.join(root, f.replace(".mov", ".mp4")))
                        cmd = "ffmpeg -y -loglevel error -i \"{}\" \"{}\"".format(os.path.join(root, f), os.path.join(root, f.replace(".mov", ".mp4")))
                        os.system(cmd)
                    cover_path = os.path.join(root, mp4_file.replace(".mp4", ".jpg"))
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
        # compressed_csv_file = os.path.join(video_dir, "video_info_compressed.csv")
        vod_csv_file = os.path.join(video_dir, "video_info_vod_hw.csv")
        tag_csv_file = os.path.join(video_dir, "video_info_tag.csv")
        
        # For debug
        skip_srt = True
        skip_quiz = True
        skip_add_cover = True
        skip_tag_video = True
        # skip_compress = True
        skip_upload = False

        skip_create = False
        skip_update_recommender = True

        video_processor = VideoProcessor()

        if not skip_srt:
            print ("###############Begin to generate Chinese/Arbic/English video scripts...... ################")
            if not os.path.exists(srt_dir):
                os.makedirs(srt_dir)

            columns = ["FileName", "title", "description", "zh_srt", "ar_srt", "en_srt", "pinyin_srt"]
            df_list = list()
            for i in tqdm(range(len(video_list))):
                item = list()
                video_path = video_list[i]
                srt_name = str(uuid.uuid4())
                item.append(video_path)
                title = "{}_{}".format(video_path.replace("\\", "/").split("/")[-2], video_path.replace("\\", "/").split("/")[-1].split(".")[0])
                item.append(title)
                description = video_path.replace("\\", "/").split("/")[-2]
                item.append(description)
                os.system("ffmpeg -y -loglevel error -i \"{}\" -ac 1 -ar 16000 -f wav test.wav".format(video_path))
                srt_res = video_processor.generate_zhsrt("",  os.path.join(srt_dir, srt_name), audio_path="test.wav", gen_ar=True)
                # os.system("rm test.wav")
                os.remove("test.wav")
                if srt_res == None:
                    print ("Empty SRT file: {}".format(video_path))
                    continue
                else:
                    item.append(srt_res["zh_srt"])
                    item.append(srt_res["ar_srt"])
                    item.append(srt_res["en_srt"])
                    item.append(srt_res["pinyin_srt"])
                df_list.append(item)
            df_srt = pd.DataFrame(df_list, columns=columns)
            
            # check SRT file is empty
            drop_idx = []
            for i in range(df_srt.shape[0]):
                if os.path.getsize(df_srt.iloc[i]["zh_srt"]) == 0:
                    print ("Empty SRT file: {}".format(df_srt.iloc[i]["FileName"]))
                    drop_idx.append(i)
            df_srt_dropped = df_srt.drop(drop_idx)
            df_srt_dropped.to_csv(srt_csv_file, index=False)
        
        if not skip_quiz:
            print ("###############Begin to generate Chinese/Arbic/English quiz...... ################")
            generate_quiz_zh(srt_dir, quiz_zh_metainfo_file)
            translate_quiz_metainfo(quiz_zh_metainfo_file, quiz_metainfo_file)
        
        if not skip_add_cover:
            print ("###############Begin to add video cover...... ################")
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
            print ("###############Begin to tag video(audio_ratio/duration/HSK_level)...... ################")
            update_video_info_csv_level(cover_csv_file, tag_csv_file)

        if not skip_upload:
            print ("###############Begin to upload to Huawei Cloud VOD(This step could take a long time. Be patient!) ################")
            if os.path.exists(vod_csv_file):
                upload_hw_withcsv(vod_csv_file, vod_csv_file)
            else:
                upload_hw_withcsv(tag_csv_file, vod_csv_file)
            
            df_vod = pd.read_csv(vod_csv_file)

            null_num = df_vod["asset_id"].isnull().sum()
            all_num = df_vod.shape[0]
            while null_num > int(0.05 * all_num):
                upload_hw_withcsv(vod_csv_file, vod_csv_file)
                df_vod = pd.read_csv(vod_csv_file)
                null_num = df_vod["asset_id"].isnull().sum()
                all_num = df_vod.shape[0]

            # upload_huoshan_withcsv(tag_csv_file, vod_csv_file)

        if not skip_create:
            print ("###############Begin to upload to Huawei Cloud Database ################")
            create_with_csv(quiz_metainfo_file, vod_csv_file, out_csv_file)
        
        if not skip_update_recommender:
            print ("###############Begin to update recommender with new video info ################")
            update_videoinfo_recommender_withcsv(out_csv_file)


if __name__ == '__main__':
    video_dir_list = []
    for i in range(len(sys.argv)):
        if i == 0:
            continue
        video_dir_list.append(sys.argv[i])
    prep_hw_data(video_dir_list)


