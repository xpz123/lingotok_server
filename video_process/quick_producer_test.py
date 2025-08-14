import os, json, time
from rocketmq.client import Producer, Message

MQ_NAMESRV_ADDR = os.getenv("MQ_NAMESRV_ADDR", "127.0.0.1:9876")
MQ_TOPIC        = os.getenv("MQ_TOPIC", "video_task")
TAG             = "pre_process"   

def main():
    print(f"[ENV] MQ_NAMESRV_ADDR={MQ_NAMESRV_ADDR}")
    print(f"[ENV] MQ_TOPIC={MQ_TOPIC}")
    print(f"[ENV] MQ_TAG={TAG}")

  
    producer = Producer("video_test_producer_" + str(int(time.time())))
    producer.set_name_server_address(MQ_NAMESRV_ADDR)
    producer.start()

    payload = {
        "file_url": "https://media.w3.org/2010/05/sintel/trailer_hd.mp4",
        "callback_url": "http://127.0.0.1:18080/callback", 
        "language": "zh-CN",
        "video_id": "demo_001",
        "video_path": "/tmp/demo_001/video.mp4",
        "srt_dir": "/tmp/demo_001",
    }



    msg = Message(MQ_TOPIC)
    msg.set_body(json.dumps(payload, ensure_ascii=False).encode("utf-8"))
    msg.set_tags(TAG)  

    ret = producer.send_sync(msg)
    print(f"[SEND] status={ret.status}, msg_id={ret.msg_id}")
    producer.shutdown()

if __name__ == "__main__":
    main()