import os
print("RUNNING:", os.path.abspath(__file__), flush=True)

import subprocess
import math
import json
import time
import asyncio
import logging
import signal
from concurrent.futures import ThreadPoolExecutor
from enum import Enum
import pathlib
import aiofiles
import aiohttp
from dotenv import load_dotenv
from rocketmq.client import PushConsumer, ConsumeStatus, Message
from call_huoshan_srt import call_huoshan_srt, refine_srt_with_videocaptioner
from translator import Translator
from video_processor import VideoProcessor
from vod_hw_util import upload_media

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# ---------- 环境变量 ----------
run_env = os.getenv("RUN_ENV", "local")
env_file = {"local": ".env.local", "test": ".env.test", "prod": ".env.prod"}.get(run_env, ".env.local")
load_dotenv(env_file) 

def _req(name, default=None):
    v = os.getenv(name, default)
    if v is None or not str(v).strip():
        raise ValueError(f"{name} 未设置，请在环境变量或 .env.* 中配置")
    return str(v).strip()

MQ_NAMESRV_ADDR   = _req("MQ_NAMESRV_ADDR", "127.0.0.1:9876")
MQ_TOPIC          = _req("MQ_TOPIC")  
MQ_CONSUMER_GROUP = _req("MQ_CONSUMER_GROUP", "video_consumer_local")

print(f"[ENV] MQ_CONSUMER_GROUP={MQ_CONSUMER_GROUP}")
print(f"[ENV] MQ_NAMESRV_ADDR={MQ_NAMESRV_ADDR}")
print(f"[ENV] MQ_TOPIC={MQ_TOPIC}")

# ---------- 线程池 & 事件循环 ----------
EXECUTOR = ThreadPoolExecutor(max_workers=4)
MAIN_LOOP = asyncio.new_event_loop()
asyncio.set_event_loop(MAIN_LOOP)

# ---------- 小工具：统一字符串/标签、下载文件 ----------
def _to_str(x):
    """把 bytes/bytearray/memoryview/其他 转成 str（utf-8 优先，退回 latin1），并 strip。"""
    if isinstance(x, memoryview):
        x = x.tobytes()
    if isinstance(x, (bytes, bytearray)):
        try:
            return x.decode("utf-8").strip()
        except UnicodeDecodeError:
            return x.decode("latin1", errors="ignore").strip()
    if x is None:
        return ""
    return str(x).strip()

def _normalize_tag(x):
    """规范化 tag：转成小写字符串；None/空则返回空字符串。"""
    return _to_str(x).lower()

def _ensure_parent_dir(path: str):
    p = pathlib.Path(path).expanduser().resolve()
    p.parent.mkdir(parents=True, exist_ok=True)
    return str(p)

async def download_to_path(url: str, dest_path: str, timeout=60):
    """把 url 下载到 dest_path（覆盖写）。"""
    _ensure_parent_dir(dest_path)
    async with aiohttp.ClientSession() as session:
        async with session.get(url, timeout=timeout) as resp:
            if resp.status < 200 or resp.status >= 300:
                text = await resp.text()
                raise RuntimeError(f"下载失败 HTTP {resp.status} url={url} snippet={text[:200]}")
            tmp_path = dest_path + ".part"
            async with aiofiles.open(tmp_path, "wb") as f:
                async for chunk in resp.content.iter_chunked(1024 * 64):
                    if chunk:
                        await f.write(chunk)
            os.replace(tmp_path, dest_path)
    return dest_path

# ---------- 业务枚举 ----------
class UploadVideoProcessType(str, Enum):
    PRE_PROCESS = "pre_process"
    PROCESS_SUBTITLE = "process_subtitle"
    PROCESS_QUIZ = "process_quiz"

# ---------- 业务处理 ----------
async def handle_extract_zh(data: dict):
    """
    PRE_PROCESS：针对中文语音的视频生成中文字幕（URL 直连版，不抽 WAV）：
    1) 确保本地视频（用于后续上传）
    2) 直接调用 ASR（URL），language 默认 zh-CN/可改 auto
    3) 使用上游返回的 segments/srt 写 SRT（不要再重解析 raw）
    4) 上传视频，返回 asset_id 与 zh_srt_path
    需要：file_url, video_id, srt_dir, video_path, (language)
    """
    file_url   = data.get("file_url", "")
    video_id   = data["video_id"]
    srt_dir    = data["srt_dir"]
    language   = data.get("language", "zh-CN") 
    video_path = data["video_path"]

    if not file_url:
        raise ValueError("缺少 file_url（URL 模式必须提供 file_url）")

    os.makedirs(srt_dir, exist_ok=True)
    logging.info("ASR (URL) input file_url=%s language=%s video_path=%s", file_url, language, video_path)

    # 1) 确保本地视频（后续要上传用）
    if not os.path.exists(video_path):
        logging.info("本地视频不存在，开始下载: %s -> %s", file_url, video_path)
        await download_to_path(file_url, video_path)
    else:
        logging.info("本地视频已存在: %s", video_path)

    # 2) 直接调用 ASR（URL）
    try:
        timeout_sec = 180
        result = await MAIN_LOOP.run_in_executor(
            EXECUTOR,
            lambda: call_huoshan_srt(
                file_url=file_url,
                language=language,
                words_per_line=15,
                max_lines=1,
                timeout=timeout_sec,
                debug=True,  
            )
        )

        raw_for_save = result.get("raw", {})
        segments = result.get("segments", [])
        srt_text = result.get("srt", "")

        # 落盘原始响应（仅用于排障）
        raw_path = os.path.join(srt_dir, f"{video_id}_huoshan_raw.json")
        try:
            with open(raw_path, "w", encoding="utf-8") as fw:
                json.dump(raw_for_save, fw, ensure_ascii=False, indent=2)
            logging.info("保存 ASR 原始响应: %s", raw_path)
        except Exception:
            pass

    except AssertionError as ae:
        raise RuntimeError(f"调用火山识别失败（可能是下载/鉴权/格式问题）：{ae}") from ae
    except Exception as e:
        raise RuntimeError(f"调用火山识别异常：{e}") from e

    # 3) 直接使用上游返回的 segments/srt（不再对 raw 进行重解析）
    logging.info("火山识别解析出 %d 段字幕", len(segments))
    if segments[:2]:
        logging.info("segments 预览前2条: %s", segments[:2])

    srt_path = os.path.join(srt_dir, f"{video_id}_Chinese.srt")
    srt_content = srt_text.strip()
    if not srt_content:
        # 保险：若上游没给 srt（几乎不会发生），由 segments 拼接 SRT
        idx = 0
        lines = []
        for seg in segments:
            st, ed, txt = _seg_fields(seg)
            if not txt:
                continue
            idx += 1
            lines += [f"{idx}", f"{st} --> {ed}", f"{txt}", ""]
        if not lines:
            lines = ["1", "00:00:00,000 --> 00:00:01,000", "（未识别到有效内容）", ""]
        srt_content = "\n".join(lines)

    async with aiofiles.open(srt_path, "w", encoding="utf-8") as fw:
        await fw.write(srt_content)

    # 精修（失败回退原始）
    srt_refined_path = srt_path.replace(".srt", "_refined.srt")
    try:
        refined_srt = refine_srt_with_videocaptioner(srt_content) or ""
        if not refined_srt.strip():
            logging.warning("refine_srt 返回空，回退原始 SRT")
            refined_srt = srt_content
    except Exception as e:
        logging.exception("refine_srt 异常，回退原始 SRT：%s", e)
        refined_srt = srt_content

    async with aiofiles.open(srt_refined_path, "w", encoding="utf-8") as f:
        await f.write(refined_srt)

    try:
        logging.info("原始 SRT 写入完成: %s (bytes=%s)", srt_path, os.path.getsize(srt_path))
        logging.info("精修 SRT 写入完成: %s (bytes=%s)", srt_refined_path, os.path.getsize(srt_refined_path))
    except Exception:
        pass

    # 4) 上传视频（保留原逻辑）
    asset_id = await MAIN_LOOP.run_in_executor(EXECUTOR, upload_media, video_path)
    logging.info(f"上传完成，asset_id: {asset_id}")

    return {"asset_id": asset_id, "zh_srt_path": srt_refined_path}


async def handle_post_zh_tasks(data: dict):
    """
    PROCESS_SUBTITLE / PROCESS_QUIZ：多语字幕、拼音、Quiz、上传
    需要：zh_srt_path, video_path, video_id, srt_dir, （可选）file_url
    """
    zh_srt_path = data["zh_srt_path"]
    video_path  = data["video_path"]
    video_id    = data["video_id"]
    srt_dir     = data["srt_dir"]
    file_url    = data.get("file_url") 

    os.makedirs(srt_dir, exist_ok=True)

    # 1) 英文翻译
    en_srt_path = os.path.join(srt_dir, f"{video_id}_English.srt")
    await MAIN_LOOP.run_in_executor(EXECUTOR, Translator().translate_zhsrt2ensrt_with_context, zh_srt_path, en_srt_path)

    # 2) 阿语翻译
    ar_srt_path = os.path.join(srt_dir, f"{video_id}_Arabic.srt")
    await MAIN_LOOP.run_in_executor(EXECUTOR, Translator().translate_zhsrt2arsrt_huoshan, zh_srt_path, ar_srt_path)

    # 3) 拼音生成
    pinyin_srt_path = os.path.join(srt_dir, f"{video_id}_Pinyin.srt")
    await MAIN_LOOP.run_in_executor(EXECUTOR, VideoProcessor().convert_zhsrt_to_pinyinsrt, zh_srt_path, pinyin_srt_path)

    # 4) Quiz 生成
    quiz = await MAIN_LOOP.run_in_executor(EXECUTOR, VideoProcessor().generate_quiz_zh_tiankong_v2, zh_srt_path)
    quiz_path = os.path.join(srt_dir, f"{video_id}_quiz.json")
    async with aiofiles.open(quiz_path, "w", encoding="utf-8") as f:
        await f.write(json.dumps(quiz, ensure_ascii=False))

    # 5) 确保本地有视频，再上传
    if not os.path.exists(video_path) and file_url:
        logging.info("post 阶段本地视频不存在，开始下载: %s -> %s", file_url, video_path)
        await download_to_path(file_url, video_path)
    asset_id = await MAIN_LOOP.run_in_executor(EXECUTOR, upload_media, video_path)
    logging.info(f"上传完成，asset_id: {asset_id}")

    return {
        "asset_id": asset_id,
        "en_srt_path": en_srt_path,
        "ar_srt_path": ar_srt_path,
        "pinyin_srt_path": pinyin_srt_path,
        "quiz_path": quiz_path,
    }

async def post_callback(callback_url: str, payload: dict):
    """异步 POST 回调"""
    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(callback_url, json=payload, timeout=10) as resp:
                text = await resp.text()
                logging.info("回调成功 %s status=%s resp=%s", callback_url, resp.status, text)
        except Exception as e:
            logging.error("回调失败 %s error=%s", callback_url, e)

# ---------- 消息分发 ----------
def _ensure_defaults(data: dict) -> dict:
    file_url = data.get("file_url", "")
    if "video_id" not in data:
        if file_url:
            import os as _os
            filename = _os.path.basename(file_url)
            data["video_id"] = _os.path.splitext(filename)[0] or f"vid_{int(time.time())}"
        else:
            data["video_id"] = f"vid_{int(time.time())}"
    data.setdefault("video_path", f"/tmp/{data['video_id']}/video.mp4")
    data.setdefault("srt_dir",    f"/tmp/{data['video_id']}")
    data.setdefault("language",   "zh-CN")
    return data

def _dispatch_by_tag(tag: str, data: dict):
    """把任何输入（Enum/bytes/str/None）规范成小写字符串后分发。"""
    key = _normalize_tag(getattr(tag, "value", tag)) or UploadVideoProcessType.PRE_PROCESS.value

    if key == UploadVideoProcessType.PRE_PROCESS.value:
        return handle_extract_zh(data)
    elif key == UploadVideoProcessType.PROCESS_SUBTITLE.value:
        return handle_post_zh_tasks(data)
    elif key == UploadVideoProcessType.PROCESS_QUIZ.value:
        return handle_post_zh_tasks(data) 
    else:
        logging.warning("未知 tag: %r，消息忽略。", key)
        return None

def process_message_sync(tag: str, data: dict):
    """在回调线程里：把协程提交到主事件循环执行，并等待结果"""
    data = _ensure_defaults(data)
    coro = _dispatch_by_tag(tag, data)
    if coro is None:
        return
    cb = data.get("callback_url")
    try:
        fut = asyncio.run_coroutine_threadsafe(coro, MAIN_LOOP)
        result = fut.result(timeout=600)
        logging.info("%s 任务完成: %s", tag, result)
        if cb:
            payload = {"status": "success", "tag": str(tag), "result": result, "video_id": data.get("video_id")}
            asyncio.run_coroutine_threadsafe(post_callback(cb, payload), MAIN_LOOP)
    except Exception as e:
        logging.exception("%s 任务异常: %s", tag, e)
        if cb:
            payload = {"status": "fail", "tag": str(tag), "error": str(e), "video_id": data.get("video_id")}
            asyncio.run_coroutine_threadsafe(post_callback(cb, payload), MAIN_LOOP)

# ---------- 解码 ----------
def _decode_body_from_msgobj(msg):
    """
    尽量稳地把消息体拿到（bytes 或 None）：
    - msg.body / msg.get_body()
    - 兼容 memoryview / 其他命名
    """
    body = None
    if hasattr(msg, "body"):
        body = getattr(msg, "body")
        if isinstance(body, memoryview):
            body = body.tobytes()
        elif isinstance(body, (bytes, bytearray)):
            body = bytes(body)
        elif body is not None:
            try:
                body = bytes(body)
            except Exception:
                body = None
    if body is None and hasattr(msg, "get_body"):
        try:
            val = msg.get_body()
            if isinstance(val, memoryview):
                body = val.tobytes()
            elif isinstance(val, (bytes, bytearray)):
                body = bytes(val)
            elif val is not None:
                try:
                    body = bytes(val)
                except Exception:
                    body = None
        except Exception:
            pass
    for alt in ("getBody", "payload", "get_payload"):
        if body is None and hasattr(msg, alt):
            try:
                val = getattr(msg, alt)()
                if isinstance(val, memoryview):
                    body = val.tobytes()
                elif isinstance(val, (bytes, bytearray)):
                    body = bytes(val)
                elif val is not None:
                    try:
                        body = bytes(val)
                    except Exception:
                        body = None
            except Exception:
                pass
    return body  

def on_message_single_msgonly(msg: Message):
    """
    只接收一个 msg 的处理逻辑。返回 ConsumeStatus。
    """
    try:
        body = _decode_body_from_msgobj(msg)
        topic = getattr(msg, "topic", None)
        raw_tags = getattr(msg, "tags", None)

        if not body:
            logging.error("消息体为空，跳过。attrs topic=%s tags=%s", topic, raw_tags)
            return ConsumeStatus.CONSUME_SUCCESS  # 跳过而非重试

        try:
            text = body.decode("utf-8")
        except UnicodeDecodeError:
            text = body.decode("latin1")

        data = json.loads(text)

        # 统一规范化 tag：优先用消息自带，其次 payload 内 tag 字段，最后默认 pre_process
        tag_norm = _normalize_tag(raw_tags) or _normalize_tag(data.get("tag")) or UploadVideoProcessType.PRE_PROCESS.value
        logging.info("收到消息 topic=%s tag=%s", topic or "", tag_norm)

        process_message_sync(tag_norm, data)
        return ConsumeStatus.CONSUME_SUCCESS

    except Exception as e:
        logging.exception("处理消息失败: %s", e)
        return ConsumeStatus.RECONSUME_LATER

# ---------- 自适应：优先 subscribe 回调；否则降级 _register_callback） ----------
def start_rmq_consumer():
    consumer = PushConsumer(MQ_CONSUMER_GROUP)

    # namesrv
    if hasattr(consumer, "set_name_server_address"):
        consumer.set_name_server_address(MQ_NAMESRV_ADDR)
    else:
        consumer.set_namesrv_addr(MQ_NAMESRV_ADDR)

    try:
        import socket
        consumer.set_instance_name(f"{MQ_CONSUMER_GROUP}_{socket.gethostname()}_{os.getpid()}")
    except Exception:
        pass

    tag_expr = "pre_process||process_subtitle||process_quiz"

    # —— 优先尝试 Python 客户端：subscribe(topic, callback) 签名 ——
    def _callback_msg(msg):
        # 新接口通常单条 Message 回调
        return on_message_single_msgonly(msg)

    used_new_callback = False
    try:
        # 若 subscribe 的第二个参数是回调，这里会成功
        consumer.subscribe(MQ_TOPIC, _callback_msg)
        used_new_callback = True
        logging.warning("使用 subscribe(topic, callback) 回调注册")
    except TypeError:
        used_new_callback = False

    if not used_new_callback:
        # 老接口：subscribe(topic, tags) + _register_callback 指针回调
        consumer.subscribe(MQ_TOPIC, tag_expr)

        def _compat_callback(*args):
            """
            兼容批量/单条/bytes 的老式指针回调。返回 0=成功 1=重试
            """
            try:
                candidates = []
                for x in args:
                    if x is None:
                        continue
                    if isinstance(x, (list, tuple)):
                        candidates.extend(list(x))
                    else:
                        candidates.append(x)

                any_retry = False
                any_seen = False
                for obj in candidates:
                    # 裸 bytes / memoryview
                    if isinstance(obj, (bytes, bytearray, memoryview)):
                        any_seen = True
                        body = obj.tobytes() if isinstance(obj, memoryview) else bytes(obj)
                        try:
                            text = body.decode("utf-8")
                        except UnicodeDecodeError:
                            text = body.decode("latin1")
                        try:
                            data = json.loads(text)
                        except Exception:
                            logging.exception("bytes 不是合法 JSON，丢弃")
                            continue
                        tag = _normalize_tag(data.get("tag")) or UploadVideoProcessType.PRE_PROCESS.value
                        try:
                            process_message_sync(tag, data)
                        except Exception:
                            logging.exception("bytes 形态处理异常")
                            any_retry = True
                        continue

                    # 包装的 Message 对象
                    if hasattr(obj, "body") or hasattr(obj, "get_body") or hasattr(obj, "getBody"):
                        any_seen = True
                        st = on_message_single_msgonly(obj)
                        if st == ConsumeStatus.RECONSUME_LATER:
                            any_retry = True

                if not any_seen:
                    logging.error("未识别的 _register_callback 参数形态：%r", args)

                return 1 if any_retry else 0
            except Exception:
                logging.exception("兼容回调异常")
                return 1

        if hasattr(consumer, "_register_callback"):
            consumer._register_callback(_compat_callback)
            logging.warning("使用 _register_callback（指针式回调）注册")
        else:
            raise RuntimeError("当前 PushConsumer 不支持 subscribe 回调，也没有 _register_callback")

 
    if hasattr(consumer, "set_thread_count"):
        consumer.set_thread_count(4)

    consumer.start()
    logging.info("RocketMQ consumer started: group=%s namesrv=%s topic=%s expr/tags=%s (mode=%s)",
                 MQ_CONSUMER_GROUP, MQ_NAMESRV_ADDR, MQ_TOPIC, tag_expr,
                 "subscribe-callback" if used_new_callback else "_register_callback")
    return consumer



def _run(cmd):
    """运行外部命令，返回 (code, stdout, stderr)"""
    p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    return p.returncode, p.stdout.strip(), p.stderr.strip()

def _fmt_ms_to_srt(ms):
    """把毫秒/秒/已是SRT字符串的输入统一成 00:00:00,000"""
    if ms is None:
        return "00:00:00,000"
    try:
        v = float(ms)
    except Exception:
        s = str(ms).strip()
        if ":" in s and "," in s:  # 已是 SRT
            return s
        return "00:00:00,000"
    total_ms = int(round(v if v > 1000 else v * 1000.0))
    hh = total_ms // 3600000
    mm = (total_ms % 3600000) // 60000
    ss = (total_ms % 60000) // 1000
    mmm = total_ms % 1000
    return f"{hh:02d}:{mm:02d}:{ss:02d},{mmm:03d}"

def _pick_segments(res: dict) -> list:
    """多路径兜底，兼容不同返回结构"""
    if not isinstance(res, dict):
        return []
    candidates = [
        ("segments",),                
        ("result",),
        ("data", "segments"),
        ("data", "result", "segments"),
        ("sentence_list",),
        ("results",),
    ]
    for path in candidates:
        cur = res
        ok = True
        for k in path:
            if isinstance(cur, dict) and k in cur:
                cur = cur[k]
            else:
                ok = False
                break
        if ok and isinstance(cur, list) and cur:
            return cur
    return []

def _seg_fields(seg: dict):
    """兼容常见字段名，输出 (srt_start, srt_end, text)"""
    start = seg.get("start") or seg.get("from") or seg.get("begin") or seg.get("start_time") or seg.get("start_time_ms")
    end   = seg.get("end")   or seg.get("to")   or seg.get("finish") or seg.get("end_time")   or seg.get("end_time_ms")
    text  = seg.get("text")  or seg.get("content") or seg.get("sentence") or seg.get("asr_text") or ""
    return _fmt_ms_to_srt(start), _fmt_ms_to_srt(end), str(text).strip()

async def _extract_audio_16k_clean(video_path: str, wav_out: str):
    """
    抽取并净化音频：16k 单声道 + 高通/低通 + 降噪 + 响度规范化 + 压缩 + 噪声门
    若 ffmpeg 不支持某滤镜，会自动降级。
    """
    _ensure_parent_dir(wav_out)
    # 先探测是否有 arnndn / afftdn
    code, out, err = _run(["ffmpeg", "-hide_banner", "-filters"])
    filters = (out + "\n" + err).lower()
    has_arnndn = "arnndn" in filters
    has_afftdn = "afftdn" in filters

    denoise = "arnndn=m=nrnnn/model.rnnn" if has_arnndn else ("afftdn=nf=-25" if has_afftdn else None)
    af_chain = ["highpass=f=100", "lowpass=f=3800"]
    if denoise:
        af_chain.append(denoise)
    af_chain += [
        "loudnorm=I=-20:LRA=11:TP=-1.5",
        "acompressor=threshold=-18dB:ratio=3:attack=10:release=100",
        "agate=threshold=-35dB:ratio=2"
    ]
    af = ",".join(af_chain)

    cmd = [
        "ffmpeg", "-y", "-hide_banner",
        "-i", video_path,
        "-vn", "-ac", "1", "-ar", "16000",
        "-af", af,
        "-c:a", "pcm_s16le", wav_out
    ]
    logging.info("FFmpeg 抽音频并净化: %s", " ".join(cmd))
    code, out, err = _run(cmd)
    if code != 0 or not os.path.exists(wav_out):
        raise RuntimeError(f"音频抽取失败：{err[-500:]}")



# ---------- 退出 ----------
def _install_signal_handlers(consumer):
    def _graceful_shutdown(signum, frame):
        try:
            logging.info("收到信号 %s，准备停止消费者与事件循环...", signum)
            if consumer:
                try:
                    consumer.shutdown()
                    logging.info("RocketMQ consumer shutdown 完成")
                except Exception:
                    logging.exception("consumer.shutdown 异常")
            try:
                MAIN_LOOP.call_soon_threadsafe(MAIN_LOOP.stop)
            except Exception:
                logging.exception("停止事件循环异常")
        except Exception:
            logging.exception("graceful shutdown 异常")

    for s in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(s, _graceful_shutdown)
        except Exception:
            pass


if __name__ == "__main__":
    consumer = start_rmq_consumer()
    _install_signal_handlers(consumer)
    try:
        MAIN_LOOP.run_forever()
    finally:
        try:
            consumer.shutdown()
        except Exception:
            pass
        try:
            EXECUTOR.shutdown(wait=False, cancel_futures=True)
        except Exception:
            pass
