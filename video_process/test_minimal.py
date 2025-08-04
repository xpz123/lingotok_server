#!/usr/bin/env python3
import os
import time
import logging

# 设置环境变量
os.environ['LD_LIBRARY_PATH'] = '/usr/local/lib'

# 设置日志
logging.basicConfig(level=logging.INFO)

from rocketmq.client import PushConsumer, Message

def simple_callback(msg_ptr, context_ptr):
    """简单的消息回调函数"""
    try:
        print(f"收到消息指针: {msg_ptr}, 上下文指针: {context_ptr}")
        
        # 尝试从指针创建消息对象
        try:
            msg = Message()
            # 这里需要根据具体的 API 来处理
            print("尝试处理消息...")
            
            # 暂时返回成功
            return True
        except Exception as e:
            print(f"创建消: {e}")
            return False
        
    except Exception as e:
        print(f"处理消息时出错: {e}")
        import traceback
        traceback.print_exc()
        return False

try:
    consumer = PushConsumer("uplaod_video_process_cg")
    
    print("正在设置 Name Server...")
    consumer.set_namesrv_addr("101.46.50.123:8200")
    
    print("正在订阅主题...")
    consumer.subscribe("uplaod_video_process", "*")
    
    print("正在注册回调函数...")
    consumer._register_callback(simple_callback)
    
    print("正在启动消费者...")
    consumer.start()
    
    print("消费者启动成功，等待消息...")
    print("在华为云平台发送测试消息，按 Ctrl+C 停止...")
    
    while True:
        time.sleep(1)
        
except KeyboardInterrupt:
    print("正在停止消费者...")
    consumer.shutdown()
    print("停止成功")
except Exception as e:
    print(f"错误: {e}")
    import traceback
    traceback.print_exc()

