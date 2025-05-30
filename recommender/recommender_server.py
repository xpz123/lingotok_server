from recommender import Recommender
from flask import Flask, request

recommender = Recommender()
app = Flask(__name__)
@app.route('/recommend_video', methods=["POST"])
def recommend_video():
    
    input_data = request.get_json()
    uid = input_data["user_id"]
    size = input_data["size"]
    try:
        video_info_list = recommender.get_video_with_username(uid, recommended_video_count=size)
        video_id_list = [video_info['video_id'] for video_info in video_info_list]
        return {"video_id_list": video_id_list, "code": 200}
    except Exception as e:
        print (e)
        return {"code": -1, "video_id_list": []}

@app.route('/update_recommender_video_info', methods=["POST"])
def update_recommender_video_info():
    input_data = request.get_json()
    try:
        recommender.update_video_info(input_data)
        return {"code": 200}
    except Exception as e:
        print (str(e))
        return {"code": -1}

@app.route("/recommend_video_by_userinfo", methods=["POST"])
def recommend_video_by_userinfo():
    input_data = request.get_json()
    try:
        video_info_list = recommender.get_video_with_userinfo(input_data)
        video_id_list = [video_info['video_id'] for video_info in video_info_list]
        return {"video_id_list": video_id_list, "code": 200}
    except Exception as e:
        print (str(e))
        return {"code": -1, "video_id_list": []}

if __name__ == "__main__":
    app.run(host='0.0.0.0',
      port=5000,
      debug=True)