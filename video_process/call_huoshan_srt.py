# -*- coding: utf-8 -*-
"""
Huoshan/Volcengine ASR → SRT 工具集（改进版）
- 修正 Authorization 头为标准 "Bearer <token>"
- 统一 URL/WAV 两种提交参数（caption_type/use_punc 等）
- 强化 _maybe_segments 兼容性与 SRT 生成稳定性
- 提供 save_srt()/refine_and_save_srt() 助手，确保下游不要“重解析”
- 在关键路径加了最小化断言与注释，避免再次出现“解析出 0 段”的情况
"""

import json
import re
import time
import requests
from typing import Any, Dict, List, Tuple, Union
from videocaptioner import split_sentences

BASE_URL = "https://openspeech.bytedance.com/api/v1/vc"
APPID = "4822083580"
ACCESS_TOKEN = "ICPlIxh2QEPMh1otaFjg0AqemFkuyv3a"


# =========================
# 通用：安全的 Authorization 头
# =========================
def _auth_headers(extra=None):
    # 注意分号！
    return {"Authorization": f"Bearer; {ACCESS_TOKEN}", **(extra or {})}



# =========================
# 轮询直到完成
# =========================
def _poll_until_done(
    job_id: str,
    appid: str = APPID,
    interval: float = 2.0,
    timeout: int = 180,
    debug: bool = False,
) -> Dict[str, Any]:
    """
    轮询火山引擎 VC /query 直到任务完成或超时。
    - 使用 blocking=1，确保返回包含完整结果。
    - 若进入成功态但未见分段/字幕，再次以 blocking=1 兜底拉取一次。
    - 发现结果就绪（_maybe_segments 可解析）立即返回。
    """
    url = f"{BASE_URL}/query"
    deadline = time.time() + timeout
    last = None

    while True:
        resp = requests.get(
            url,
            params={"appid": appid, "id": job_id, "blocking": 1},
            headers=_auth_headers(),
            timeout=30,
        )
        raw = resp.text
        if debug:
            print("query raw =", raw)

        if resp.status_code != 200:
            raise RuntimeError(f"query http {resp.status_code}: {raw[:800]}")

        try:
            data = resp.json()
        except Exception:
            raise RuntimeError(f"query not json: {raw[:800]}")

        last = data

        status = _extract_status(data)
        code = data.get("code")
        msg = data.get("message", "")

        bad_code = False
        try:
            bad_code = (code is not None) and (int(code) != 0)
        except Exception:
            bad_code = False

        # 显式失败 或 非零 code
        if status in {"FAILED", "ERROR", "CANCELLED"} or bad_code:
            raise RuntimeError(f"ASR 失败: status={status or '<empty>'} code={code} message={msg}")

        # 若已拿到分段/字幕，直接返回
        if _maybe_segments(data):
            return data

        # 若服务端回的是成功态，但正文尚未并入，再做一次 blocking=1 兜底
        if status in {"SUCCEEDED", "SUCCESS", "FINISHED", "DONE", "COMPLETED"}:
            try:
                resp2 = requests.get(
                    url,
                    params={"appid": appid, "id": job_id, "blocking": 1},
                    headers=_auth_headers(),
                    timeout=30,
                )
                raw2 = resp2.text
                if debug:
                    print("query raw (final pull) =", raw2)

                if resp2.status_code == 200:
                    try:
                        data2 = resp2.json()
                        if _maybe_segments(data2):
                            return data2
                        return data2
                    except Exception:
                        # 二次结果不可解析，退回首次 data
                        return data
                return data
            except Exception:
                return data

        # 超时
        if time.time() > deadline:
            raise TimeoutError(
                f"ASR 轮询超时: last_status={status or '<empty>'} code={code} message={msg} "
                f"last={json.dumps(data, ensure_ascii=False)[:1200]}"
            )

        time.sleep(interval)


# =========================
# 时间与结构辅助
# =========================
def _fmt_time_srt(v: Union[str, int, float]) -> str:
    # 兼容 ms / s / 已是 "00:00:00,000"
    try:
        fv = float(v)
    except Exception:
        s = str(v).strip()
        return s if (":" in s and "," in s) else "00:00:00,000"
    # 经验规则
    is_int_like = abs(fv - int(fv)) < 1e-6
    if fv < 1000:
        total_ms = int(fv) if is_int_like else int(fv * 1000)
    else:
        total_ms = int(fv) if fv < 1_000_000 else int(fv * 1000)
    hh = total_ms // 3600000
    mm = (total_ms % 3600000) // 60000
    ss = (total_ms % 60000) // 1000
    ms = total_ms % 1000
    return f"{hh:02d}:{mm:02d}:{ss:02d},{ms:03d}"


def _walk(obj: Any):
    if isinstance(obj, dict):
        for k, v in obj.items():
            yield k, v
            for kv in _walk(v):
                yield kv
    elif isinstance(obj, list):
        for v in obj:
            for kv in _walk(v):
                yield kv


def _looks_like_srt_text(s: str) -> bool:
    if not isinstance(s, str):
        return False
    return ("-->" in s) and re.search(r"\d{1,2}:\d{2}:\d{2},\d{3}", s) is not None


def _is_probable_srt_url(s: str) -> bool:
    if not isinstance(s, str):
        return False
    sl = s.lower()
    return sl.endswith(".srt") or "caption" in sl or "subtitle" in sl


def _try_fetch_srt(url: str) -> str:
    try:
        r = requests.get(url, timeout=10, headers=_auth_headers())
        if r.status_code == 200 and _looks_like_srt_text(r.text):
            return r.text
    except Exception:
        pass
    return ""


# =========================
# 解析：尽量兼容多种结构
# =========================
def _maybe_segments(payload: Any) -> List[Dict[str, str]]:
    """
    从返回 JSON 中尽量抽取 {start,end,text} 段列表，或直接返回 SRT 文本。
    兼容：顶层 / data / result 层级；SRT 文本 / SRT URL；任意列表里 start/end/text 风格字段。
    """
    # —— 0) 若顶层无 utterances，尝试下钻到 data/result ——
    if isinstance(payload, dict) and "utterances" not in payload:
        for k in ("data", "result"):
            if isinstance(payload.get(k), dict) and "utterances" in payload[k]:
                payload = payload[k]
                break

    if isinstance(payload, dict) and isinstance(payload.get("utterances"), list):
        segs = []
        for it in payload["utterances"]:
            if not isinstance(it, dict):
                continue
            st = it.get("start_time")
            ed = it.get("end_time")
            txt = it.get("text")
            if (st is not None) and (ed is not None) and str(txt or "").strip():
                segs.append(
                    {"start": _fmt_time_srt(st), "end": _fmt_time_srt(ed), "text": str(txt).strip()}
                )
        if segs:
            return segs

    # —— 2) 任意深度的 SRT 文本 ——
    if isinstance(payload, (dict, list)):
        for k, v in _walk(payload):
            kl = str(k).lower()
            if kl in ("srt", "subtitle", "subtitles", "caption", "captions") and isinstance(v, str) and _looks_like_srt_text(v):
                return [{"_raw_srt": v}]
            if isinstance(v, str) and _looks_like_srt_text(v):
                return [{"_raw_srt": v}]

    # —— 3) 任意深度的 SRT URL ——
    if isinstance(payload, (dict, list)):
        for k, v in _walk(payload):
            kl = str(k).lower()
            if kl in ("srt_url", "subtitle_url", "caption_url", "url") and isinstance(v, str) and _is_probable_srt_url(v):
                srt = _try_fetch_srt(v)
                if srt:
                    return [{"_raw_srt": srt}]
                break

    # —— 4) 任意深度的分段列表（字段名容错） ——
    def _extract_from_list(lst):
        segs = []
        for it in lst:
            if not isinstance(it, dict):
                continue
            st = (
                it.get("start") or it.get("from") or it.get("begin")
                or it.get("start_time") or it.get("start_time_ms")
                or it.get("begin_time") or it.get("begin_time_ms")
                or it.get("ts_start") or it.get("time_start")
            )
            ed = (
                it.get("end") or it.get("to") or it.get("finish")
                or it.get("end_time") or it.get("end_time_ms")
                or it.get("finish_time") or it.get("finish_time_ms")
                or it.get("ts_end") or it.get("time_end")
            )
            txt = (
                it.get("text") or it.get("content") or it.get("sentence") or it.get("asr_text")
                or it.get("caption") or it.get("subtitle")
            )
            if (st is not None) and (ed is not None) and str(txt or "").strip():
                segs.append(
                    {"start": _fmt_time_srt(st), "end": _fmt_time_srt(ed), "text": str(txt).strip()}
                )
        return segs

    candidate_list_keys = {
        "utterances", "segments", "results", "sentences", "utterance_list",
        "words", "items", "list", "caption_items", "subtitle_items"
    }
    if isinstance(payload, (dict, list)):
        for k, v in _walk(payload):
            if isinstance(v, list) and v:
                if str(k).lower() in candidate_list_keys:
                    segs = _extract_from_list(v)
                    if segs:
                        return segs
                segs = _extract_from_list(v)
                if segs:
                    return segs

    # —— 都没找到 ——
    return []


# =========================
# 从最终结果生成 SRT 文本（必要时兜底）
# =========================
def huoshan_result_to_srt(final_json: dict = None, segments: list = None) -> str:
    """
    用已有的 segments 直接拼 SRT；若未提供 segments，则从 final_json 里解析（走新版 _maybe_segments）。
    """
    segs = segments if isinstance(segments, list) else _maybe_segments(final_json or {})

    # API 已返回完整 SRT 文本（_maybe_segments 会用 {"_raw_srt": ...} 表示）
    if segs and isinstance(segs[0], dict) and "_raw_srt" in segs[0]:
        srt = segs[0]["_raw_srt"]
        # 基础校验
        if "-->" not in srt:
            raise RuntimeError("收到的 SRT 文本不合规：缺少时间轴箭头 '-->'")
        return srt

    # 没有分段，输出占位（同时让上游能据此报警处理）
    if not segs:
        return "1\n00:00:00,000 --> 00:00:01,000\n（未识别到有效内容）\n"

    # 标准 SRT 拼接
    lines = []
    for i, s in enumerate(segs, 1):
        lines.append(f"{i}")
        lines.append(f"{s['start']} --> {s['end']}")
        lines.append(s['text'])
        lines.append("")  # 空行
    srt_text = "\n".join(lines)

    # 基础校验
    if "-->" not in srt_text:
        raise RuntimeError("生成的 SRT 文本不合规：缺少时间轴箭头 '-->'（请检查 segments）")

    return srt_text


# =========================
# 精修（断句优化）
# =========================
def refine_srt_with_videocaptioner(srt_content: str) -> str:
    """
    对标准SRT内容的字幕文本部分用VideoCaptioner断句优化，保留编号和时间轴。
    """
    if "-->" not in srt_content or not re.search(r"\d{1,2}:\d{2}:\d{2},\d{3}", srt_content):
        # 若不是标准 SRT，则保持原样，交给上游处理
        return srt_content

    pattern = re.compile(r"(\d+)\n([\d:,]+ --> [\d:,]+)\n(.+?)(?=\n\n|\Z)", re.DOTALL)
    new_blocks = []
    for match in pattern.finditer(srt_content):
        idx, timecode, text = match.groups()
        text = text.strip().replace("\n", " ")
        refined_lines = split_sentences(text)
        refined_text = "\n".join(refined_lines) if refined_lines else text
        new_block = f"{idx}\n{timecode}\n{refined_text}"
        new_blocks.append(new_block)
    return "\n\n".join(new_blocks)


# =========================
# 辅助：安全写盘（建议下游只用这些）
# =========================
def save_srt_to_path(srt_text: str, output_path: str) -> int:
    """
    将 SRT 文本写入文件；返回写入的字节数。
    在写盘前做一次最小检查，避免写出占位 SRT。
    """
    if "-->" not in srt_text:
        raise RuntimeError("SRT 非标准格式（缺少 '-->'），放弃写盘，检查上游解析。")
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(srt_text)
    return len(srt_text.encode("utf-8"))


def refine_and_save_srt(srt_text: str, output_path: str) -> Tuple[str, int]:
    """
    先精修，再写盘；返回 (refined_text, bytes)。
    """
    refined = refine_srt_with_videocaptioner(srt_text)
    written = save_srt_to_path(refined, output_path)
    return refined, written


# =========================
# 提交 + 轮询（URL 方式）
# =========================
def call_huoshan_srt(
    file_url: str,
    language: str = "zh-CN",
    words_per_line: int = 15,
    max_lines: int = 1,
    timeout: int = 180,
    debug: bool = False,
) -> Dict[str, Any]:
    submit_url = f"{BASE_URL}/submit"
    payload = {"url": file_url}
    params = dict(
        appid=APPID,
        language=language,
        use_itn="True",
        use_capitalize="True",
        max_lines=max_lines,
        words_per_line=words_per_line,
        caption_type="speech",
        use_punc="True",
    )
    resp = requests.post(
        submit_url,
        params=params,
        json=payload,
        headers=_auth_headers({"content-type": "application/json"}),
        timeout=30,
    )
    if debug:
        print("submit response =", resp.text)
    if resp.status_code != 200:
        raise RuntimeError(f"submit http {resp.status_code}: {resp.text[:500]}")
    j = resp.json()
    if j.get("message") != "Success":
        raise RuntimeError(f"submit failed: {j}")

    job_id = j.get("id") or j.get("task_id")
    if not job_id:
        raise RuntimeError(f"no job id in submit response: {j}")

    final = _poll_until_done(job_id, appid=APPID, timeout=timeout, debug=debug)

    # —— 和 WAV 版保持一致：统一解析并返回 raw/segments/srt —— 
    segs = _maybe_segments(final)
    if not segs and debug:
        print("WARN: 解析到 0 段字幕，请检查 final JSON 结构")

    srt_text = huoshan_result_to_srt(final_json=final, segments=segs)

    return {"raw": final, "segments": segs, "srt": srt_text}


# =========================
# 提交 + 轮询（WAV 文件方式）
# =========================
def _dump_paths(obj: Any, prefix: str = ""):
    """
    调试用：递归打印 JSON 的所有 key 路径，帮助定位结果字段。
    """
    if isinstance(obj, dict):
        for k, v in obj.items():
            np = f"{prefix}.{k}" if prefix else k
            print(np)
            _dump_paths(v, np)
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            np = f"{prefix}[{i}]"
            _dump_paths(v, np)


def call_huoshan_srt_wav(
    file_path: str,
    language: str = "zh-CN",
    words_per_line: int = 15,
    max_lines: int = 1,
    timeout: int = 180,
    debug: bool = False,
) -> Dict[str, Any]:
    with open(file_path, "rb") as f:
        data = f.read()
    submit_url = f"{BASE_URL}/submit"
    # 与 URL 方式保持一致
    params = dict(
        appid=APPID,
        language=language,
        use_itn="True",
        use_capitalize="True",
        max_lines=max_lines,
        words_per_line=words_per_line,
        caption_type="speech",   
        use_punc="True",        
    )
    resp = requests.post(
        submit_url,
        params=params,
        data=data,
        headers=_auth_headers({"content-type": "audio/wav"}),
        timeout=60,
    )
    if debug:
        print("submit response =", resp.text)
    if resp.status_code != 200:
        raise RuntimeError(f"submit http {resp.status_code}: {resp.text[:500]}")
    j = resp.json()
    if j.get("message") != "Success":
        raise RuntimeError(f"submit failed: {j}")

    job_id = j.get("id") or j.get("task_id")
    if not job_id:
        raise RuntimeError(f"no job id in submit response: {j}")

    final = _poll_until_done(job_id, appid=APPID, timeout=timeout, debug=debug)

    # —— 统一解析一次（只在这里解析一次）——
    segs = _maybe_segments(final)
    # 最小断言（避免后续误用 0 段）
    if not segs:
        if debug:
            print("WARN: 解析到 0 段字幕，请检查 final JSON 结构")
    else:
        if debug:
            print(f"INFO 解析出 {len(segs)} 段字幕")

    srt_text = huoshan_result_to_srt(final_json=final, segments=segs)

    return {"raw": final, "segments": segs, "srt": srt_text}


# =========================
# 状态抽取
# =========================
def _extract_status(data: dict) -> str:
    # 顶层字符串
    s = (data.get("status") or data.get("task_status") or "").strip()
    if s:
        return s.upper()

    # 常见嵌套位置
    dd = data.get("data") or data.get("result") or {}
    for k in ("status", "task_status", "state", "job_status"):
        v = dd.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip().upper()
        if isinstance(v, (int, float)):
            mapping = {0: "QUEUED", 1: "RUNNING", 2: "SUCCEEDED", 3: "FAILED", 4: "CANCELLED"}
            return mapping.get(int(v), f"NUM_{int(v)}")

    # 兜底：把 code 尽量转成 int 来判断
    code = data.get("code")
    if code is None and isinstance(dd, dict):
        code = dd.get("code")
    try:
        if int(code) == 0:
            return "RUNNING"
    except Exception:
        pass

    return ""


# =========================
# 方便下游调用的“一条龙”高阶函数（可选）
# =========================
def transcribe_wav_to_srt_file(
    wav_path: str,
    out_srt_path: str,
    language: str = "zh-CN",
    words_per_line: int = 15,
    max_lines: int = 1,
    timeout: int = 180,
    debug: bool = False,
) -> Tuple[str, int]:
    """
    直接完成：提交→轮询→解析→SRT生成→精修→写盘。
    返回 (refined_srt_text, written_bytes)。
    下游如果使用本方法，务必不要再对 raw/segments 进行任何“重解析”。
    """
    result = call_huoshan_srt_wav(
        file_path=wav_path,
        language=language,
        words_per_line=words_per_line,
        max_lines=max_lines,
        timeout=timeout,
        debug=debug,
    )
    srt_text = result["srt"]
    refined, written = refine_and_save_srt(srt_text, out_srt_path)
    return refined, written
