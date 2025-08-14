#!/usr/bin/env python3
import os
import logging
import ctypes
import json

# 设置环境变量
os.environ['LD_LIBRARY_PATH'] = '/usr/local/lib'

# 配置日志
logging.basicConfig(level=logging.INFO)

def on_message(msg_ptr, context_ptr):
    """消息回调函数 - 尝试获取消息内容"""
    try:
        print(f"收到消息指针: {msg_ptr}, 上下文指针: {context_ptr}")
        
        # 尝试不同的方法来获取消息内容
        try:
            from rocketmq.client import Message
            
            # 方法1: 尝试创建消息对象
            print("尝试方法1: 创建消息对象...")
            msg = Message()
            
            # 方法2: 尝试从指针读取数据
            print("尝试方法2: 从指针读取数据...")
            ptr = ctypes.c_void_p(msg_ptr)
            print(f"指针地址: {ptr.value}")
            
            # 方法3: 尝试使用 ctypes 读取内存
            print("尝试方法3: 读取内存数据...")
            try:
                # 尝试读取一些字节
                data = ctypes.string_at(ptr, 1024)  # 读取1024字节
                print(f"读取到的数据: {data}")
                
                # 尝试解析为JSON
                try:
                    json_data = json.loads(data.decode('utf-8'))
                    print(f"解析的JSON数据: {json_data}")
                except:
                    print("数据不是有效的JSON格式")
                    
            except Exception as e:
                print(f"读取内存失败: {e}")
                
        except Exception as e:
            print(f"获取消息内容失败: {e}")
        
        print("消息处理成功")
        return True
    except Exception as e:
        print(f"处理消息时出错: {e}")
        import traceback
        traceback.print_exc()
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
    print("请在华为云平台发送测试消息...")

if __name__ == "__main__":
    import asyncio
    loop = asyncio.get_event_loop()
    start_rmq_consumer()
    loop.run_forever()