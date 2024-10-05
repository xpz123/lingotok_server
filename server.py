# -*-codeing=utf-8-*-
from flask import Flask, request
import os
from mdd import rtevl
import random as rd
import pandas as pd
import json
from collections import defaultdict

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
    interests = item["interests"].strip().split(",")
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


real_idx=0
app = Flask(__name__)

@app.route('/login', methods=["POST"])
def login():
    accessable_user = [{"username": "k12", "password": "123456"}, {"username": "child", "password": "123456"}]
    username= request.form.get('username')
    password = request.form.get('password')
    for item in accessable_user:
        if item["username"] == username and item["password"] == password:
            return {"code": 200, "msg": "success", "age": item["username"]}
    msg = {"code": 200, 'msg': 'failed'}
    return msg

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
    rd.shuffle(refer_vidlist)
    refer_vid = refer_vidlist[0]

    video_name = video_infod[refer_vid]["video_path"]
    srt_name = video_infod[refer_vid]["en_srt"]

    question = video_quizd[refer_vid]["question"]
    options = video_quizd[refer_vid]["options"]
    answer = video_quizd[refer_vid]["answer"]



    msg = {"code": 200, "msg": "success", "video_name": video_name, "srt_name": srt_name, "question": question, "options": options, "answer": answer}
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

