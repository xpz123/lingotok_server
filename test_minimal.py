#!/usr/bin/env python3
import os
import time
import logging

# 设置环境变量
os.environ['LD_LIBRARY_PATH'] = '/usr/local/lib'

# 设置日志
logging.basicConfig(level=logging.INFO)

from rocketmq.client import PushConsumer

def simple_callback(*args):
    """简单的消息回调函数"""
    try:
        print(f"回调函数参数: {args}")
        print(f"参数类型: {[type(arg) for arg in args]}")
        
        # 尝试不同的参数组合
        if len(args) >= 1:
            msg = args[0]
            if hasattr(msg, 'body'):
                print(f"收到消息: {msg.body.decode('utf-8')}")
                print(f"消息标签: {msg.get_tags()}")
            else:
                print(f"第一个参数不是消息对象: {msg}")
        
        return True
    except Exception as e:
        print(f"处理消息时出错: {e}")
        import traceback
        traceback.print_exc()
        return False

try:
    print("正在创建消费者...")
    consumer = PushConsumer("uplaod_video_process_cg")
    
    print("正在'EOF''EOF' Name Server...")
    consumer.set_namesrv_addr("101.46.50.123:8200")
    
    print("正在订阅主题...")
    consumer.subscribe("uplaod_video_process", "*")
    
    print("正在注册回调函数...")
    consumer._register_callback(simple_callback)
    
...")
    consumer.start()
    
...")
    print("在华为云平台发送测试消息，按 Ctrl+C 停止...")
    
    while True:
        time.sleep(1)
        
except KeyboardInterrupt:
    print(正在停止消...")
    consumer.shutdown()
    print("停止成功")
except Exception as e:
    print(f"错误: {e}")
    import traceback
    traceback.print_exc()
