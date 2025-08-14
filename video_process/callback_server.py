# callback_server.py
from flask import Flask, request, jsonify
import logging
import sys
from datetime import datetime

app = Flask(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stdout
)

@app.get("/health")
def health():
    return jsonify({"status": "ok", "ts": datetime.utcnow().isoformat() + "Z"})

@app.post("/callback")
def callback():
    try:
        data = request.get_json(force=True, silent=False)
        logging.info("收到回调：%s", data)
        # 你可以在这里把数据存DB / 发通知 / 写文件等
        return jsonify({"received": True}), 200
    except Exception as e:
        logging.exception("处理回调失败")
        return jsonify({"received": False, "error": str(e)}), 400

if __name__ == "__main__":
    # 监听所有网卡，容器才能访问到
    app.run(host="0.0.0.0", port=18080)
