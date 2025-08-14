import re

def split_sentences(text):
    """
    简单的句子分割函数
    根据标点符号分割句子
    """
    # 使用正则表达式分割句子
    sentences = re.split(r'[。！？.!?]', text)
    # 过滤空字符串并去除首尾空格
    sentences = [s.strip() for s in sentences if s.strip()]
    return sentences

def refine_srt_with_videocaptioner(srt_content):
    """
    对标准SRT内容的字幕文本部分用VideoCaptioner断句优化，保留编号和时间轴。
    """
    pattern = re.compile(r'(\d+)\n([\d:,]+ --> [\d:,]+)\n(.+?)(?=\n\n|\Z)', re.DOTALL)
    new_blocks = []
    for match in pattern.finditer(srt_content):
        idx, timecode, text = match.groups()
        text = text.strip().replace('\n', ' ')
        refined_text = "\n".join(split_sentences(text))
        new_block = f"{idx}\n{timecode}\n{refined_text}"
        new_blocks.append(new_block)
    return "\n\n".join(new_blocks) 