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
os.environ["IMAGEIO_FFMPEG_EXE"] = "/opt/homebrew/Cellar/ffmpeg/7.1_4/bin/ffmpeg"
import uuid
from create_video import create_with_csv, update_videoinfo_recommender_withcsv
from content_tagger import update_video_info_csv_level
from pypinyin import pinyin


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
                quiz = video_processor.generate_quiz_zh_tiankong_v2(zhsrt_filename)
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

    video_dir_list = list()
    video_dir_list.append("/Users/tal/work/lingtok_server/video_process/字有道理")

    for video_dir in video_dir_list:
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

        quiz_metainfo_file = os.path.join(video_dir, "video_metainfo.jsonl")

        quiz_csv_file = os.path.join(video_dir, "video_info_quiz.csv")
        # cover_csv_file = os.path.join(video_dir, "video_info_cover.csv")
        # compressed_csv_file = os.path.join(video_dir, "video_info_compressed.csv")
        vod_csv_file = os.path.join(video_dir, "video_info_vod_hw.csv")
        tag_csv_file = os.path.join(video_dir, "video_info_tag.csv")
        
        cus_tag = "PNU888"
        series_name = "字有道理"
        
        # For debug
        skip_srt = True
        skip_quiz = False
        skip_tag_video = False
        # skip_compress = True
        skip_upload = False

        skip_create = False
        skip_series_name = False

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
                os.system("/opt/homebrew/Cellar/ffmpeg/7.1_4/bin/ffmpeg -y -loglevel error -i \"{}\" -ac 1 -ar 16000 -f wav test.wav".format(video_path))
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
            df = pd.read_csv(srt_csv_file)
            columns = df.columns.to_list()
            columns.append("quiz_id")
            columns.append("拼音")
            df_list = df.values.tolist()
            fw = open(quiz_metainfo_file, "w", encoding="utf-8")
            sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            from recommender.quiz_generator import CharPinyinQuizGeneratingWorker, QuizGeneratingCtx

            quiz_ctx = QuizGeneratingCtx()
            quiz_worker_config = {"hsk_zh_en_ar_path": "hsk_dictionary/HSK_zh_en_ar.csv", "hsk_char_path": "hsk_dictionary/HSK_char.csv"}
            quiz_worker = CharPinyinQuizGeneratingWorker(quiz_worker_config)
            for i in tqdm(range(len(df_list))):
                word = df.iloc[i]["title"].split("-")[-1].strip()[0]
                df_list[i][1] = df_list[i][1].replace("_modified", "")
                py = pinyin(word)[0][0]
                quiz_ctx.extracted_word = word
                try:
                    quiz_res = quiz_worker.action(quiz_ctx)
                    content = quiz_worker.to_dict(quiz_res)
                
                    content["vid"] = "{}_{}".format(i, word)
                    df_list[i].append(content["vid"])
                    df_list[i].append(py)
                    fw.write("{}\n".format(json.dumps(content, ensure_ascii=False)))
                except Exception as e:
                    print (str(e))
            fw.close()
            df_list = pd.DataFrame(df_list, columns=columns)
            df_list.to_csv(quiz_csv_file, index=False)
        
        import pdb; pdb.set_trace()
        if not skip_tag_video:
            update_video_info_csv_level(quiz_csv_file, tag_csv_file)

        if not skip_upload:
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


        if not skip_create:
            create_with_csv(quiz_metainfo_file, vod_csv_file, out_csv_file, customize=cus_tag, series_name=series_name)

        if not skip_series_name:
            from recommender.video_updater import VideoUpdater
            video_updater = VideoUpdater()
            video_updater.update_series_tag_once(series_name, level="初学", tag_list=["科学教育"])
    

if __name__ == '__main__':
    prep_hw_data()


