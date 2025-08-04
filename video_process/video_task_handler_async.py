import os
import aiofiles
import json
import asyncio
import time
from rocketmq.client import PushConsumer, Message

# 设置环境变量
os.environ['LD_LIBRARY_PATH'] = '/usr/local/lib'
from call_huoshan_srt import call_huoshan_srt
from translator import Translator
from video_processor import VideoProcessor
from vod_hw_util import upload_media
from call_huoshan_srt import refine_srt_with_videocaptioner
from concurrent.futures import ThreadPoolExecutor 
from dotenv import load_dotenv
import aiohttp
from enum import Enum

# 枚举定义直接移到这里
class UploadVideoProcessType(str, Enum):
    PRE_PROCESS = "pre_process"
    PROCESS_SUBTITLE = "process_subtitle"
    PROCESS_QUIZ = "process_quiz"

load_dotenv()  # 自动加载 .env 文件

# 环境切换：根据 RUN_ENV 加载不同的 .env 文件
run_env = os.getenv("RUN_ENV", "local")  # 默认local
env_file = {
    "local": ".env.local",
    "test": ".env.test",
    "prod": ".env.prod"
}.get(run_env, ".env.local")
load_dotenv(env_file)

# 1. 结构化环境变量加载和配置
MQ_NAMESRV_ADDR = os.getenv("MQ_NAMESRV_ADDR")
MQ_TOPIC = os.getenv("MQ_TOPIC")
MQ_CONSUMER_GROUP = os.getenv("MQ_CONSUMER_GROUP")

# 2. 枚举类型已直接定义在文件顶部

# 3. 日志完善
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')


executor = ThreadPoolExecutor(max_workers=4)
loop = asyncio.get_event_loop()

async def handle_extract_zh(data):
    """
    处理PRE_PROCESS事件：生成中文字幕、优化断句、上传到云
    """
    file_url = data['file_url']
    video_id = data['video_id']
    srt_dir = data['srt_dir']
    language = data.get('language', 'zh-CN')
    video_path = data['video_path']

    loop = asyncio.get_event_loop()
    executor = data.get('executor')

    # 1. 生成中文字幕
    result = await loop.run_in_executor(
        executor, call_huoshan_srt, file_url, language, 15
    )
    srt_path = os.path.join(srt_dir, f"{video_id}_Chinese.srt")
    srt_content = ""
    async with aiofiles.open(srt_path, "w", encoding="utf-8") as fw:
        for idx, seg in enumerate(result.get('result', [])):
            line = f"{idx+1}\n{seg['start']} --> {seg['end']}\n{seg['text']}\n\n"
            await fw.write(line)
            srt_content += line

    # 2. refine_srt_with_videocaptioner 优化断句
    srt_refined_path = srt_path.replace('.srt', '_refined.srt')
    refined_srt = refine_srt_with_videocaptioner(srt_content)
    async with aiofiles.open(srt_refined_path, "w", encoding="utf-8") as f:
        await f.write(refined_srt)

    # 3. 上传到华为云
    asset_id = upload_media(video_path, zh_srt_path=srt_refined_path, title=video_id)
    logging.info(f"上传完成，asset_id: {asset_id}")
    return {"asset_id": asset_id, "zh_srt_path": srt_refined_path}

async def handle_post_zh_tasks(data):
    """
    处理PROCESS_SUBTITLE/PROCESS_QUIZ事件：生成多语字幕、Quiz、上传到云
    """
    zh_srt_path = data['zh_srt_path']
    video_path = data['video_path']
    video_id = data['video_id']
    srt_dir = data['srt_dir']

    loop = asyncio.get_event_loop()
    executor = data.get('executor')  # 可选：传递线程池

    # 1. 英文翻译
    en_srt_path = os.path.join(srt_dir, f"{video_id}_English.srt")
    await loop.run_in_executor(
        executor, Translator().translate_zhsrt2ensrt_with_context, zh_srt_path, en_srt_path
    )

    # 2. 阿语翻译
    ar_srt_path = os.path.join(srt_dir, f"{video_id}_Arabic.srt")
    await loop.run_in_executor(
        executor, Translator().translate_zhsrt2arsrt_huoshan, zh_srt_path, ar_srt_path
    )

    # 3. 拼音生成
    pinyin_srt_path = os.path.join(srt_dir, f"{video_id}_Pinyin.srt")
    await loop.run_in_executor(
        executor, VideoProcessor().convert_zhsrt_to_pinyinsrt, zh_srt_path, pinyin_srt_path
    )

    # 4. Quiz生成
    quiz = await loop.run_in_executor(
        executor, VideoProcessor().generate_quiz_zh_tiankong_v2, zh_srt_path
    )
    quiz_path = os.path.join(srt_dir, f"{video_id}_quiz.json")
    async with aiofiles.open(quiz_path, "w", encoding="utf-8") as f:
        await f.write(json.dumps(quiz, ensure_ascii=False))

    # 5. 上传到华为云
    asset_id = upload_media(
        video_path,
        zh_srt_path=zh_srt_path,
        en_srt_path=en_srt_path,
        ar_srt_path=ar_srt_path,
        py_srt_path=pinyin_srt_path,
        title=video_id
    )
    logging.info(f"上传完成，asset_id: {asset_id}")
    return {
        "asset_id": asset_id,
        "en_srt_path": en_srt_path,
        "ar_srt_path": ar_srt_path,
        "pinyin_srt_path": pinyin_srt_path,
        "quiz_path": quiz_path
    }

async def post_callback(callback_url, payload):
    """
    异步POST回调
    """
    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(callback_url, json=payload, timeout=10) as resp:
                text = await resp.text()
                logging.info(f"回调推送成功: {callback_url}, 状态: {resp.status}, 响应: {text}")
        except Exception as e:
            logging.error(f"回调推送失败: {callback_url}, 错误: {e}")

TYPE_HANDLER_MAP = {
    "extract_zh": handle_extract_zh,
    "post_zh_tasks": handle_post_zh_tasks,
}

def on_message(msg_ptr, context_ptr):
    """消息回调函数 - 新版本 RocketMQ 客户端"""
    try:
        print(f"收到消息指针: {msg_ptr}, 上下文指针: {context_ptr}")
        
                # 获取消息内容
        try:
            import ctypes
            
            print("尝试从指针读取消息内容...")
            
            # 直接读取指针数据
            ptr = ctypes.c_void_p(msg_ptr)
            
            # 尝试从不同的偏移位置读取数据
            for offset in [0, 8, 16, 24, 32, 64, 128, 256]:
                try:
                    offset_ptr = ctypes.c_void_p(ptr.value + offset)
                    print(f"尝试从偏移 {offset} 读取数据...")
                    
                    # 尝试读取不同大小的数据
                    for size in [64, 128, 256, 512, 1024]:
                        try:
                            data = ctypes.string_at(offset_ptr, size)
                            print(f"偏移 {offset}, 读取 {size} 字节数据: {data[:50]}...")  # 打印前50个字节
                            
                            # 尝试不同的编码方式
                            for encoding in ['utf-8', 'latin1', 'cp1252']:
                                try:
                                    text = data.decode(encoding)
                                    print(f"使用 {encoding} 解码: {text[:100]}...")  # 打印前100个字符
                                    
                                    # 查找 JSON 开始和结束的位置
                                    start = text.find('{')
                                    end = text.rfind('}')
                                    
                                    if start != -1 and end != -1 and end > start:
                                        json_str = text[start:end+1]
                                        print(f"找到 JSON 字符串: {json_str}")
                                        try:
                                            message_data = json.loads(json_str)
                                            print(f"找到真实消息数据: {message_data}")
                                            
                                            # 获取视频 URL
                                            file_url = message_data.get('file_url')
                                            if file_url:
                                                print(f"视频 URL: {file_url}")
                                                process_message(message_data)
                                                return True
                                            else:
                                                print("消息中没有找到 file_url")
                                                continue
                                                
                                        except json.JSONDecodeError as e:
                                            print(f"JSON 解析失败: {e}")
                                            continue
                                            
                                except UnicodeDecodeError as e:
                                    print(f"{encoding} 解码失败: {e}")
                                    continue
                                    
                        except Exception as e:
                            print(f"读取 {size} 字节失败: {e}")
                            continue
                            
                except Exception as e:
                    print(f"偏移 {offset} 处理失败: {e}")
                    continue
            
            print("无法从消息中提取有效数据")
            return False
                
        except Exception as e:
            print(f"获取消息内容失败: {e}")
            return False
        
    except Exception as e:
        print(f"处理消息时出错: {e}")
        import traceback
        traceback.print_exc()
        return False

def process_message(data):
    """处理消息数据"""
    try:
        # 获取消息标签（这里我们假设所有消息都是 pre_process）
        tag = "pre_process"  # 可以根据实际需要修改
        
        logging.info(f"收到消息，tag: {tag}, data: {data}")

        # 自动生成缺失的字段
        if 'video_id' not in data:
            # 从 file_url 中提取文件名作为 video_id
            file_url = data.get('file_url', '')
            if file_url:
                # 提取文件名（去掉扩展名）
                import os
                filename = os.path.basename(file_url)
                video_id = os.path.splitext(filename)[0]
                data['video_id'] = video_id
            else:
                data['video_id'] = f"vid_{int(time.time())}"
        
        if 'video_path' not in data:
            data['video_path'] = f"/tmp/{data['video_id']}/video.mp4"
        
        if 'srt_dir' not in data:
            data['srt_dir'] = f"/tmp/{data['video_id']}"
        
        if 'language' not in data:
            data['language'] = 'zh-CN'  # 默认中文

        # 获取主线程的事件循环
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            # 如果没有事件循环，创建一个新的
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        callback_url = data.get("callback_url")
        
        try:
            # 根据 tag 分发任务
            if tag == UploadVideoProcessType.PRE_PROCESS:
                future = asyncio.run_coroutine_threadsafe(handle_extract_zh(data), loop)
            elif tag in (UploadVideoProcessType.PROCESS_SUBTITLE, UploadVideoProcessType.PROCESS_QUIZ):
                future = asyncio.run_coroutine_threadsafe(handle_post_zh_tasks(data), loop)
            else:
                logging.warning(f"未知tag: {tag}")
                return
            
            result = future.result(timeout=600)
            logging.info(f"{tag} 任务完成: {result}")

            if callback_url:
                payload = {
                    "status": "success",
                    "tag": tag,
                    "result": result,
                    "video_id": data.get("video_id"),
                }
                asyncio.run_coroutine_threadsafe(post_callback(callback_url, payload), loop)
            
        except Exception as e:
            logging.error(f"{tag} 任务异常: {e}")
            if callback_url:
                payload = {
                    "status": "fail",
                    "tag": tag,
                    "error": str(e),
                    "video_id": data.get("video_id"),
                }
                asyncio.run_coroutine_threadsafe(post_callback(callback_url, payload), loop)
                
    except Exception as e:
        logging.error(f"处理消息数据失败: {e}")

def start_rmq_consumer():
    # 1. 从环境变量读取配置
    consumer_group = MQ_CONSUMER_GROUP or 'default_consumer_group'
    namesrv_addr = MQ_NAMESRV_ADDR or '127.0.0.1:9876'
    # 支持多个 namesrv_addr
    if ',' in namesrv_addr:
        namesrv_addr_list = [addr.strip() for addr in namesrv_addr.split(',')]
        namesrv_addr = ';'.join(namesrv_addr_list)
    # 2. 创建 Consumer
    consumer = PushConsumer(consumer_group)
    consumer.set_name_server_address(namesrv_addr)
    # 3. 设置实例名称（重要！）
    import socket
    instance_name = f"{consumer_group}_{socket.gethostname()}_{os.getpid()}"
    consumer.set_instance_name(instance_name)
    # 4. 预留鉴权（如有）
    # ak = os.getenv('MQ_AK')
    # sk = os.getenv('MQ_SK')
    # if ak and sk:
    #     consumer.set_session_credentials(ak, sk, '')
    # 5. 订阅 topic + tag
    tags = "pre_process||process_subtitle||process_quiz"
    consumer.subscribe(MQ_TOPIC, tags)
    
    # 6. 注册消息监听器
    consumer._register_callback(on_message)
    # 5. 启动 Consumer
    consumer.start()
    logging.info(f"RocketMQ consumer started and listening on group: {consumer_group}, namesrv: {namesrv_addr}, topic: {MQ_TOPIC}, tags: {tags}")

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    start_rmq_consumer()
    loop.run_forever()
