# -*-codeing=utf-8-*-
from flask import Flask, request
import os
from mdd import rtevl
import random as rd

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
    k12 = {"hard": [2, 5, 10, 11, 15, 20, 22, 33, 35, 38, 42], "easy": [23, 24, 25, 27, 28, 29, 43, 44, 45, 46, 47, 48, 49, 50], "middle": [4, 7, 8, 9, 12, 13, 14, 16, 18, 19, 26, 32, 37, 40, 41]}
    child = {"hard": [1, 6, 17, 34, 36, 39], "easy": [3, 31, 21, 30]}
    #age = request.form.get("age")
    #level = request.form.get("level")
    global real_idx
    age = "child"
    level = "hard"
    real_idx += 1
    if (real_idx % 2) == 0:
       return {"code": 200, "msg": "success", "video_name": "k12\\hard\\22\\22.mp4", "srt_name": "k12\\hard\\22\\22.srt"}
    else:
       return {"code": 200, "msg": "success", "video_name": "k12\\easy\\28\\28.mp4", "srt_name": "k12\\easy\\28\\28.srt"}
    #local_path = "C:\\Users\\duyix\\Desktop\\app\\myapp\\public\\k12"
    if not level in {"hard", "easy", "middle"}:
        return {"code": 200, "msg": "success", "video_name": "", "srt_name": ""}
    if age == "k12":
        tmp_list = k12[level]
        idx = real_idx % len(tmp_list)
        print (idx)

        #rd.shuffle(tmp_list)
        video_name = "{}\\{}\\{}\\{}".format(age, level, tmp_list[idx], "{}.mp4".format(tmp_list[idx]))
        srt_name = "{}\\{}\\{}\\{}".format(age, level, tmp_list[idx], "{}.srt".format(tmp_list[idx]))
    elif age == "child":
        tmp_list = child[level]
        #rd.shuffle(tmp_list)
        idx = real_idx % len(tmp_list)
        print (idx)
        video_name = "{}\\{}\\{}\\{}".format(age, level, tmp_list[idx], "{}.mp4".format(tmp_list[idx]))
        srt_name = "{}\\{}\\{}\\{}".format(age, level, tmp_list[idx], "{}.srt".format(tmp_list[idx]))
    else:
        video_name = ""
        srt_name = ""

    msg = {"code": 200, "msg": "success", "video_name": video_name, "srt_name": srt_name}
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

