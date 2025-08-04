#!/usr/bin/env python3
import os
import json
import logging
import asyncio
import aiohttp
from enum import Enum

# 设置环境变量
os.environ['LD_LIBRARY_PATH'] = '/usr/local/lib'

# 配置日志
logging.basicConfig(level=logging.INFO)

# 从环境变量读取配置
MQ_NAMESRV_ADDR = os.getenv('MQ_NAMESRV_ADDR', '127.0.0.1:9876')
MQ_TOPIC = os.getenv('MQ_TOPIC', 'test_topic')
MQ_CONSUMER_GROUP = os.getenv('MQ_CONSUMER_GROUP', 'test_consumer_group')

class UploadVideoProcessType(str, Enum):
    PRE_PROCESS = "pre_process"
    PROCESS_SUBTITLE = "process_subtitle"
    PROCESS_QUIZ = "process_quiz"

def on_message(msg_ptr, context_ptr):
    """消息回调函数 - 兼容旧版本 RocketMQ 客户端"""
    try:
        print(f"收到消息指针: {msg_ptr}, 上下文指针: {context_ptr}")
        print("消息处理成功")
        return True
    except Exception as e:
        print(f"处理消息时出错: {e}")
        import traceback
        traceback.print_exc()
        return False

def start_rmq_consumer():
    from rocketmq.client import PushConsumer
    
    # 1. 从环境变量读取配置
    consumer_group = MQ_CONSUMER_GROUP or 'default_consumer_group'
    namesrv_addr = MQ_NAMESRV_ADDR or '127.0.0.1:9876'
    
    # 支持多个 namesrv_addr
    if ',' in namesrv_addr:
        namesrv_addr_list = [addr.strip() for addr in namesrv_addr.split(',')]
        namesrv_addr = ';'.join(namesrv_addr_list)
    
    # 2. 创建 Consumer
    consumer = PushConsumer(consumer_group)
    consumer.set_namesrv_addr(namesrv_addr)
    
    # 3. 设置实例名称（重要！）
    import socket
    instance_name = f"{consumer_group}_{socket.gethostname()}_{os.getpid()}"
    consumer.set_instance_name(instance_name)
    
    # 4. 订阅 topic + tag
    tags = "pre_process||process_subtitle||process_quiz"
    consumer.subscribe(MQ_TOPIC, tags)
    
    # 5. 注册消息监听器
    consumer._register_callback(on_message)
    
    # 6. 启动 Consumer
    consumer.start()
    logging.info(f"RocketMQ consumer started and listening on group: {consumer_group}, namesrv: {namesrv_addr}, topic: {MQ_TOPIC}, tags: {tags}")

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    start_rmq_consumer()
    loop.run_forever()
