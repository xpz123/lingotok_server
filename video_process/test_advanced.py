#!/usr/bin/env python3
import os
import logging

# 设置环境变量
os.environ['LD_LIBRARY_PATH'] = '/usr/local/lib'

# 配置日志
logging.basicConfig(level=logging.INFO)

def on_message(msg_ptr, context_ptr):
    """消息回调函数 - 尝试获取消息内容"""
    try:
        print(f"收到消息指针: {msg_ptr}, 上下文指针: {context_ptr}")
        print("消息处理成功")
        return True
    except Exception as e:
        print(f"处理消息时出错: {e}")
        return False

def start_rmq_consumer():
    from rocketmq.client import PushConsumer
    
    # 创建 Consumer
    consumer = PushConsumer("uplaod_video_process_cg")
    consumer.set_namesrv_addr("101.46.50.123:8200")
    consumer.subscribe("uplaod_video_process", "*")
    consumer._register_callback(on_message)
    consumer.start()
    logging.info("RocketMQ consumer started successfully")

if __name__ == "__main__":
    import asyncio
    loop = asyncio.get_event_loop()
    start_rmq_consumer()
    loop.run_forever()
