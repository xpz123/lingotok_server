# -*-codeing=utf-8-*-
from flask import Flask, request, send_file
import os
from mdd import rtevl
import random as rd
import pandas as pd
import json
from collections import defaultdict
from user import UserInfo
from util import *

video_quizd = dict()
lines = open("video_metainfo.jsonl").readlines()
for l in lines:
    data = json.loads(l.strip())
    vid = data["vid"]
    video_quizd[vid] = {"question": data["question"], "options": data["options"], "answer": data["answer"].strip().replace(".", "")}

video_infod = dict()
key2vid = defaultdict(list)
df = pd.read_csv("video_info.csv")
for i in range(df.shape[0]):
    item = df.iloc[i].to_dict()
    vid = str(item["vid"])
    video_infod[vid] = item
    ages = item["age"].strip().split(",")
    genders = item["gender"].strip().split(",")
    levels = item["level"].strip().split(",")
    interests = str(item["interests"]).strip().split(",")
    for age in ages:
        if age == "":
            continue
        key2vid["age_{}".format(age.strip())].append(vid)
    for gender in genders:
        if gender == "":
            continue
        key2vid["gender_{}".format(gender.strip())].append(vid)
    for level in levels:
        if level == "":
            continue
        key2vid["level_{}".format(level.strip())].append(vid)
    for interest in interests:
        if interest == "":
            continue
        key2vid["interest_{}".format(interest.strip())].append(vid)

user_info = UserInfo()

real_idx=0
app = Flask(__name__)

@app.route('/login', methods=["POST"])
def login():
    global user_info
    # accessable_user = [{"username": "k12", "password": "123456"}, {"username": "child", "password": "123456"}, {"username": "investor_test", "password": "123123"}]
    username= request.form.get('username')
    password = request.form.get('password')
    # for item in accessable_user:
    #     if item["username"] == username and item["password"] == password:
    if user_info.user_is_exist(username, password):
            return {"code": 200, "status": "success", "msg": "success"}
    msg = {"code": 200, 'status': 'failed', "msg": "failed"}
    return msg

@app.route('/signup', methods=["POST"])
def signup():
    global user_info
    username= request.form.get('username')
    password = request.form.get('password')
    if user_info.user_signup(username, password) == 0:
            return {"code": 200, "status": "success"}
    msg = {"code": 200, 'status': 'failed', "msg": "username repeated"}
    return msg

@app.route('/send_user_info', methods=["POST"])
def send_user_info():
    global user_info
    try:
        username= request.form.get('username')
        age = request.form.get('age')
        level = request.form.get('level')
        gender = request.form.get('gender')
        interests = request.form.get('interests')
        if user_info.update_user_info(username, age=age, gender=gender, level=level, interests=interests) == 0:
            msg = {"code": 200, 'status': 'success', "msg": "update user info successfully"}
        else:
            msg = {"code": 200, 'status': 'failed', "msg": "cannot find username:{}".format(username)}
    except:
        msg = {"code": 200, 'status': 'failed', "msg": "update user info failed"}

    return msg

@app.route('/send_user_behavior', methods=["POST"])
def send_user_behavior():
    global user_info
    try:
        username= request.form.get('username')
        behavior_dict = dict()
        behavior_dict["app_usage_duration"] = request.form.get('app_usage_duration')
        behavior_dict["watched_video_duration"] = request.form.get('watched_video_duration')
        behavior_dict["watched_video_count"] = request.form.get('watched_video_count')
        behavior_dict["made_quiz_count"] = request.form.get('made_quiz_count')
        behavior_dict["correct_quiz_count"] = request.form.get('correct_quiz_count')
        behavior_dict["read_video_count"] = request.form.get('read_video_count')
        behavior_dict["read_sentence_count"] = request.form.get('read_sentence_count')


        if user_info.update_user_behavior(username, behavior_dict) == 0:
            msg = {"code": 200, 'status': 'success', "msg": "update user info successfully"}
        else:
            msg = {"code": 200, 'status': 'failed', "msg": "cannot find username:{}".format(username)}
    except:
        msg = {"code": 200, 'status': 'failed', "msg": "update user info failed"}

    return msg


@app.route("/get_video_file", methods=["GET"])
def get_video_file():
    video_dir = "~/work/lingtok/lingtok_server"
    # video_name = request.form.get("videoname")
    video_name = request.args.get("videoname")
    video_name = video_name.replace("\\", "/")
    return send_file(video_name, mimetype="video/mp4")

@app.route("/get_srt_file", methods=["GET"])
def get_srt_file():
    video_dir = "~/work/lingtok/lingtok_server"
    # video_name = request.form.get("videoname")
    video_name = request.args.get("srtname")
    video_name = video_name.replace("\\", "/")
    return send_file(video_name)


@app.route('/get_video', methods=["POST"])
def get_video():

    global key2vid
    global video_infod
    global video_quizd
    try:
        real_age = int(request.form.get("age"))
        level = request.form.get("level")
        interests = request.form.get("interests")
        gender = request.form.get("gender").lower()
    except:
        vidlist = list(video_infod.keys())
        rd.shuffle(vidlist)
        refer_vid = vidlist[0]

        video_name = video_infod[refer_vid]["video_path"]
        srt_name = video_infod[refer_vid]["en_srt"]
        question = video_quizd[refer_vid]["question"]
        options = video_quizd[refer_vid]["options"]
        answer = video_quizd[refer_vid]["answer"]
        msg = {"code": 200, "msg": "success", "video_name": video_name, "srt_name": srt_name, "question": question, "options": options, "answer": answer}
        return msg

    

    # if not level in {"hard", "easy", "middle"}:
    #     return {"code": 200, "msg": "success", "video_name": "", "srt_name": ""}
    if real_age < 6:
        age = "prek"
    elif real_age < 18:
        age = "k12"
    else:
        age = "adult"


    age_vidset = set(key2vid["age_{}".format(age)])
    gender_vidset = set(key2vid["gender_{}".format(gender)])
    level_vidlist = list()
    if level == "hard":
        cefr_list = ["B1", "B2", "C1", "C2"]
    elif level == "middle":
        cefr_list = ["A2", "B1", "B2"]
    else:
        cefr_list = ["A1", "A2", "B1"]
    for cefr in cefr_list:
        level_vidlist += key2vid["level_{}".format(cefr)]
    level_vidset = set(level_vidlist)

    refer_vidlist = list(age_vidset & gender_vidset & level_vidset)
    # 如果交集为空，就按年龄推荐
    if len(refer_vidlist) == 0:
        refer_vidlist = age_vidset

    rd.shuffle(refer_vidlist)
    refer_vid = refer_vidlist[0]

    video_name = video_infod[refer_vid]["video_path"]
    srt_name = video_infod[refer_vid]["en_srt"]

    question = video_quizd[refer_vid]["question"]
    options = video_quizd[refer_vid]["options"]
    answer = video_quizd[refer_vid]["answer"]



    msg = {"code": 200, "msg": "success", "video_name": video_name, "srt_name": srt_name, "question": question, "options": options, "answer": answer}
    return msg


@app.route('/get_video_list', methods=["POST"])
def get_video_list():

    global key2vid
    global video_infod
    global video_quizd
    try:
        real_age = int(request.form.get("age"))
        level = request.form.get("level")
        interests = request.form.get("interests")
        gender = request.form.get("gender").lower()
    except:
        vidlist = list(video_infod.keys())
        rd.shuffle(vidlist)
        res_list = list()
        for i in range(min(5, len(vidlist))):
            refer_vid = vidlist[i]

            video_name = video_infod[refer_vid]["video_path"]
            srt_name = video_infod[refer_vid]["en_srt"]
            question = video_quizd[refer_vid]["question"]
            options = video_quizd[refer_vid]["options"]
            answer = video_quizd[refer_vid]["answer"]
            res_list.append({"video_name": video_name, "srt_name": srt_name, "question": question, "options": options, "answer": answer})
        msg = {"code": 200, "msg": "success", "video_list": res_list}
        return msg

    

    # if not level in {"hard", "easy", "middle"}:
    #     return {"code": 200, "msg": "success", "video_name": "", "srt_name": ""}
    if real_age < 6:
        age = "prek"
    elif real_age < 18:
        age = "k12"
    else:
        age = "adult"


    age_vidset = set(key2vid["age_{}".format(age)])
    gender_vidset = set(key2vid["gender_{}".format(gender)])
    level_vidlist = list()
    if level == "hard":
        cefr_list = ["B1", "B2", "C1", "C2"]
    elif level == "middle":
        cefr_list = ["A2", "B1", "B2"]
    else:
        cefr_list = ["A1", "A2", "B1"]
    for cefr in cefr_list:
        level_vidlist += key2vid["level_{}".format(cefr)]
    level_vidset = set(level_vidlist)

    refer_vidlist = list(age_vidset & gender_vidset & level_vidset)
    rd.shuffle(refer_vidlist)
    res_list = list()
    for i in range(min(5, len(refer_vidlist))):
        refer_vid = refer_vidlist[i]

        video_name = video_infod[refer_vid]["video_path"]
        srt_name = video_infod[refer_vid]["en_srt"]

        question = video_quizd[refer_vid]["question"]
        options = video_quizd[refer_vid]["options"]
        answer = video_quizd[refer_vid]["answer"]
        res_list.append({"video_name": video_name, "srt_name": srt_name, "question": question, "options": options, "answer": answer})



    msg = {"code": 200, "msg": "success", "video_list": res_list}
    return msg


@app.route('/get_video_with_username', methods=["POST"])
def get_video_with_username():
    global user_info
    global key2vid
    global video_infod
    global video_quizd
    global vip_name_set
    try:
        username = request.form.get("username")

        
        info = user_info.fetch_user_info(username)
        real_age = int(info["age"])
        level = info["level"]
        interests = info["interests"]
        gender = info["gender"].lower()
    except:
        vidlist = list(video_infod.keys())
        rd.shuffle(vidlist)
        res_list = list()
        for i in range(min(5, len(vidlist))):
            refer_vid = vidlist[i]

            video_name = video_infod[refer_vid]["video_path"]
            srt_name = video_infod[refer_vid]["en_srt"]
            zhihu_url = video_infod[refer_vid]["zhihu_url"]
            question = video_quizd[refer_vid]["question"]
            options = video_quizd[refer_vid]["options"]
            answer = video_quizd[refer_vid]["answer"]
            res_list.append({"video_name": srt_name, "srt_name": srt_name, "zhihu_url": zhihu_url, "question": question, "options": options, "answer": answer})
        msg = {"code": 200, "msg": "success", "video_list": res_list}
        return msg

    

    # if not level in {"hard", "easy", "middle"}:
    #     return {"code": 200, "msg": "success", "video_name": "", "srt_name": ""}
    if real_age < 6:
        age = "prek"
    elif real_age < 18:
        age = "k12"
    else:
        age = "adult"


    age_vidset = set(key2vid["age_{}".format(age)])
    gender_vidset = set(key2vid["gender_{}".format(gender)])
    level_vidlist = list()

    if type(level) == float:
        if level >= 4:
            level = "hard"
        elif level >= 2:
            level = "middle"
        else:
            level = "easy"

    if level == "hard":
        cefr_list = ["B1", "B2", "C1", "C2"]
    elif level == "middle":
        cefr_list = ["A2", "B1", "B2"]
    else:
        cefr_list = ["A1", "A2"]
    for cefr in cefr_list:
        level_vidlist += key2vid["level_{}".format(cefr)]
    level_vidset = set(level_vidlist)

    refer_vidlist = list(age_vidset & gender_vidset & level_vidset)
    # 如果交集为空，就按年龄推荐
    if len(refer_vidlist) == 0:
        refer_vidlist = age_vidset
    rd.shuffle(refer_vidlist)
    res_list = list()
    for i in range(min(5, len(refer_vidlist))):
        refer_vid = refer_vidlist[i]

        video_name = video_infod[refer_vid]["video_path"]
        srt_name = video_infod[refer_vid]["en_srt"]
        zhihu_url = video_infod[refer_vid]["zhihu_url"]

        question = video_quizd[refer_vid]["question"]
        options = video_quizd[refer_vid]["options"]
        answer = video_quizd[refer_vid]["answer"]
        zhihu_url = video_infod[refer_vid]["zhihu_url"]
        res_list.append({"video_name": srt_name, "srt_name": srt_name, "zhihu_url": zhihu_url, "question": question, "options": options, "answer": answer})



    msg = {"code": 200, "msg": "success", "video_list": res_list}
    return msg


@app.route("/mdd", methods=["POST"])
def call_mdd():
    """接受前端传送过来的文件"""
    file_obj = request.files.get("audioFile")
    if file_obj is None:
        # 表示没有发送文件
        return {"code": 100, "msg": "No audio file"}

    filepath = os.path.join("tmp_audio", file_obj.filename)
    print (filepath)
    outpath = os.path.join("tmp_audio", "format_{}.wav".format(file_obj.filename))
    f = open(filepath, "wb+")

    data = file_obj.read()
    f.write(data)
    f.close()

    # convert audio format with ffmpeg
    try:
        cmd_line = "ffmpeg -y -i {} -ar 16000  {}".format(filepath, outpath)
        os.system(cmd_line)

        ref_text = request.form.get("subtitleText")
        print ("ref text: {}".format(ref_text))
        score_result = rtevl(outpath, ref_text, "en.snt.score")

        word_list = list()
        score_list = list()
        for item in score_result["words"]:
            word_list.append(item["word"])
            score_list.append(item["score"])

        return {"code": 200, "msg": "success", "pron_score": score_result["pron_score"], "word_list": word_list, "score_list": score_list, "index": request.form.get("index")}
    except:
        return {"code": 100, "msg": "Call MDD failed"}
    finally:
        os.system("rm -f {}".format(filepath))
        os.system("rm -f {}".format(outpath))

if __name__ == '__main__':
    app.run(host='0.0.0.0',
      port=8081,
      debug=True)

