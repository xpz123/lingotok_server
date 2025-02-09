import cv2
import os
import pysrt
import re
from tqdm import tqdm
import pandas as pd
os.environ["IMAGEIO_FFMPEG_EXE"] = "/opt/homebrew/Cellar/ffmpeg/7.1_4/bin/ffmpeg"
from moviepy.editor import VideoFileClip, AudioFileClip, TextClip, CompositeVideoClip, ImageClip
from moviepy.config import change_settings
change_settings({"IMAGEMAGICK_BINARY": "/opt/homebrew/Cellar/imagemagick/7.1.1-43/bin/magick"})
# from moviepy.config import change_settings
# change_settings({"FFMPEG_BINARY": "/opt/homebrew/Cellar/ffmpeg/7.1_3/bin/ffmpeg"})
from volcengine.visual.VisualService import VisualService
import urllib.request
from retry import retry
from pydub import AudioSegment
from video_processor import VideoProcessor, milliseconds_to_time_string
from translator import translate_text2ar
import shutil
from vod_huoshan_util import upload_media
from datetime import datetime
from bidi.algorithm import get_display
import arabic_reshaper

visual_service = VisualService()
visual_service.set_ak('AKLTOTgzODg1Y2FiNDI5NGE3Mzk3MWEzYzJlODE3MDk2MzQ')
visual_service.set_sk('TTJJM016azRaR0V3WXpRMk5EUXhPR0kyT0RBNVlUY3hZVGd5WlRrMlpHTQ==')

@retry(tries=3, delay=1)
def call_huoshan_text2image(prompt, image_file):
    def download_image(url, file_name):
        urllib.request.urlretrieve(url, file_name)
        print("图片下载成功！")
    form = {
        "req_key":"high_aes_general_v20_L",
        "prompt": prompt,
        "model_version":"general_v2.0_L",
        "req_schedule_conf":"general_v20_9B_rephraser",
        "seed":-1,
        "scale":3.5,
        "ddim_steps":16,
        "width":512,
        "height":512,
        "use_sr":True,
        "return_url":True,
        "logo_info": {
            "add_logo": True,
            "position": 0,
            "language": 0,
            "opacity": 0.8,
            "logo_text_content": "Lingotok-AI"
        }
    }
    resp = visual_service.high_aes_smart_drawing(form)
    print(resp)
    download_image(resp['data']['image_urls'][0], image_file)
    return resp


def cut_sentences(ori_file, sents_file):
    lines = open(ori_file).readlines()
    # text = " ".join(lines).replace("\n", " ")
    # print (text)
    # # 定义更复杂的正则表达式以处理引号和省略号
    # pattern = r'(?<=[。！？?])\s*'
    # sentences = re.split(pattern, text)
    
    # 进一步处理引号内的内容
    # sentences = [s.strip() for s in sentences if s.strip()]

    file_list = list()
    sent_list = list()
    res = list()
    for idx, sent in enumerate(lines):
        sent_list.append(sent.strip())
        file_list.append(ori_file.split("/")[-1].replace(".txt", "") + "_sent{}.txt".format(idx+1))
        res.append((ori_file.split("/")[-1].replace(".txt", "") + "_sent{}".format(idx+1), sent))
    tmp_dcit = {"filename": file_list, "script": sent_list}
    df = pd.DataFrame(tmp_dcit)
    df.to_csv(sents_file, index=False)
    return res

def images_to_video(audio_dur_dict, img_list, output_video, fps=25):
    # image_dir = "/Users/tal/work/沙特女子Demo/Dr. Asmac材料1/合成文本/lesson1-part1"
    # image_dict = {"sent1": 6.18, "sent2": 18.3, "sent3": 27.36, "sent4": 38.26}
    # image_list = ["sent1", "sent2", "sent3", "sent4"]

    # 读取第一张图片以获取尺寸
    first_image = cv2.imread(img_list[0])
    height, width, layers = first_image.shape
    print (height, width, layers)

    # 创建VideoWriter对象
    fourcc = cv2.VideoWriter_fourcc(*'mp4v')  # 使用mp4v编码
    video_writer = cv2.VideoWriter(output_video, fourcc, fps, (width, height))


    # 遍历所有图片并写入视频
    # pre_end_time = 0

    for img_path in img_list:
        dur = audio_dur_dict[img_path.split("/")[-1].replace(".png", "")]
        img = cv2.imread(img_path)
        resized_img = cv2.resize(img, (width, height))
        image_repeat_num = int(fps * dur)
        for _ in range(image_repeat_num):
            video_writer.write(resized_img)

    # 释放VideoWriter对象
    cv2.destroyAllWindows()
    video_writer.release()
    print(f'视频已保存为: {output_video}')

def add_audio_to_video(video_path, audio_path, output_path):
    # 加载视频文件
    video = VideoFileClip(video_path)
    
    # 加载音频文件
    audio = AudioFileClip(audio_path)
    # 将音频添加到视频中
    video_with_audio = video.set_audio(audio)

    image = ImageClip("/Users/tal/work/lingtok_server/video_process/沙特女子Demo/初级汉语/logo.png")
    image = image.set_duration(video.duration).set_position(("right", "bottom"))

    video_with_subtitles = CompositeVideoClip([video_with_audio]  + [image])

    # 保存合并后的视频为新文件
    video_with_subtitles.write_videofile(output_path, codec="libx264", audio_codec="aac")

    

def merge_audios(audio_list, output_audio, sil_dur=300):
    audio_dur_dict = dict()
    audio = AudioSegment.empty()
    silence = AudioSegment.silent(duration=sil_dur)
    for audio_path in audio_list:
        audio_dur_dict[audio_path.split("/")[-1].replace(".wav", "")] = AudioSegment.from_file(audio_path).duration_seconds + float(sil_dur) / float(1000)
        audio += AudioSegment.from_file(audio_path) + silence
    audio.export(output_audio, format="wav")
    print (audio_dur_dict)
    return audio_dur_dict


def generate_subtitle(sents_csv, srt_prefix, root_dir, audio_dur_dict):
    video_processor = VideoProcessor()
    res = dict()
    zh_srt = os.path.join(root_dir, "{}_Chinese.srt".format(srt_prefix))
    ar_srt = os.path.join(root_dir, "{}_Arabic.srt".format(srt_prefix))
    en_srt = os.path.join(root_dir, "{}_English.srt".format(srt_prefix))
    pinyin_srt = os.path.join(root_dir, "{}_Pinyin.srt".format(srt_prefix))

    res["zh_srt"] = zh_srt
    res["ar_srt"] = ar_srt
    res["en_srt"] = en_srt
    res["pinyin_srt"] = pinyin_srt

    df = pd.read_csv(sents_csv)

    zh_text_list = []
    for i in range(df.shape[0]):
        zh_text_list.append(df.iloc[i]["script"])
    
    ar_text_list = translate_text2ar(zh_text_list, "ar")
    assert len(ar_text_list) == len(zh_text_list)

    en_text_list = translate_text2ar(zh_text_list, "en")
    assert len(en_text_list) == len(zh_text_list)

    fw_zh = open(zh_srt, "w")
    fw_ar = open(ar_srt, "w")
    fw_en = open(en_srt, "w")
    
    start_time = 0
    for i in range(df.shape[0]):
        zh_text_list.append(df.iloc[i]["script"])
        dur_ms = int(audio_dur_dict[df.iloc[i]["filename"].replace(".txt", "")] * 1000)
        start_time_str = milliseconds_to_time_string(start_time)
        end_time_str = milliseconds_to_time_string(start_time + dur_ms)
        start_time = start_time + dur_ms
        zh_srt_content = f"{i}\n{start_time_str} --> {end_time_str}\n{df.iloc[i]['script']}\n\n"
        fw_zh.write(zh_srt_content)

        ar_srt_content = f"{i}\n{start_time_str} --> {end_time_str}\n{ar_text_list[i]['Translation']}\n\n"
        fw_ar.write(ar_srt_content)

        en_srt_content = f"{i}\n{start_time_str} --> {end_time_str}\n{en_text_list[i]['Translation']}\n\n"
        fw_en.write(en_srt_content)
    fw_zh.close()
    fw_ar.close()
    fw_en.close()

    video_processor.convert_zhsrt_to_pinyinsrt(zh_srt, pinyin_srt)
    return res

def create_video(sents_csv, imgs_dir, audio_dir, video_dir):
    
    df = pd.read_csv(sents_csv)
    audio_list = list()
    img_list = list()
    for i in range(df.shape[0]):
        filename = df.iloc[i]["filename"]
        img_path = os.path.join(imgs_dir, filename.replace(".txt", ".png")+ ".png")
        print (img_path)
        img_list.append(img_path)
        assert os.path.exists(img_path)
        audio_path = os.path.join(audio_dir, filename.replace(".txt", ".wav"))
        print (audio_path)
        assert os.path.exists(audio_path)
        audio_list.append(audio_path)
    merged_audio_path = os.path.join(audio_dir, "merged.wav")
    audio_dur_dict = merge_audios(audio_list, merged_audio_path)
    mute_video_path = os.path.join(video_dir, "mute.mp4")
    images_to_video(audio_dur_dict, img_list, mute_video_path)
    final_video_path = os.path.join(video_dir, sents_csv.split("/")[-1].replace(".csv", ".mp4"))
    add_audio_to_video(mute_video_path, merged_audio_path, final_video_path)

    return audio_dur_dict, final_video_path



def create_subtitle_clip(txt1, txt2, start, end, fontsize=30):
    """创建包含两行字幕的文本片段"""
    combined_text = f"{txt1}\n{txt2}"  # 将两行文本合并
    return (TextClip(txt1, fontsize=fontsize, color='white', bg_color='black', font="Songti-SC-Black")
            .set_position(('center', 0.8), relative=True)
            .set_start(start)
            .set_duration((end - start)))

def create_subtitle_clip_ar(txt1, txt2, start, end, fontsize=30):
    """创建包含两行字幕的文本片段"""
    txt2 = get_display(arabic_reshaper.reshape(txt2))
    combined_text = f"{txt1}\n{txt2}"  # 将两行文本合并
    return (TextClip(txt2, fontsize=fontsize, color='white', bg_color='black', font="Noto Sans")
            .set_position(('center', 0.9), relative=True)
            .set_start(start)
            .set_duration((end - start)))


def add_subtitles_to_video(video_path, chinese_srt, arbic_srt, output_path):
    # 加载视频文件
    video = VideoFileClip(video_path)

    # 加载字幕
    chinese_subtitle = pysrt.open(chinese_srt)
    arbic_subtitle = pysrt.open(arbic_srt)
    assert len(chinese_subtitle) == len(arbic_subtitle)

    subtitle_clips = []
    for i in range(len(chinese_subtitle)):
        text1 = chinese_subtitle[i].text
        text2 = arbic_subtitle[i].text
        start_time = chinese_subtitle[i].start
        start_time_ms = (start_time.hours * 3600000 + 
                     start_time.minutes * 60000 + 
                     start_time.seconds * 1000 + 
                     start_time.milliseconds)
        start = float(start_time_ms) / float(1000)
        end_time = chinese_subtitle[i].end
        end_time_ms = (end_time.hours * 3600000 + 
                   end_time.minutes * 60000 + 
                   end_time.seconds * 1000 + 
                   end_time.milliseconds)
        end = float(end_time_ms) / float(1000)
        subtitle_clips.append(create_subtitle_clip(text1, text2, start, end, fontsize=15))
        subtitle_clips.append(create_subtitle_clip_ar(text1, text2, start, end, fontsize=15))

    # image = ImageClip("/Users/tal/work/lingtok_server/video_process/沙特女子Demo/初级汉语/logo.png")
    # image = image.set_duration(video.duration).set_position(("right", "bottom"))
    # # 合成视频与字幕
    # video_with_subtitles = CompositeVideoClip([video] + subtitle_clips + [image])
    video_with_subtitles = CompositeVideoClip([video] + subtitle_clips)

    # 保存合成后的视频
    video_with_subtitles.write_videofile(output_path, codec="libx264", audio_codec="aac")

def create_aigc_csv(ori_file_list, out_csv_file):
    columns = ["ori_file","sents_csv", "FileName", "zh_srt", "ar_srt", "en_srt", "pinyin_srt"]
    df_list = list()
    for ori_file in ori_file_list:
        line_list = list()
        line_list.append(ori_file)
        root_dir = ori_file.replace(".txt", "")
        video_dir = os.path.join(root_dir, "video")
        sents_csv = os.path.join(root_dir, ori_file.split("/")[-1].replace(".txt", "_sents.csv"))
        line_list.append(sents_csv)
        
        final_video_path = os.path.join(video_dir, ori_file.split("/")[-1].replace(".txt", "_sents.mp4"))
        line_list.append(final_video_path)
        zh_srt_path = os.path.join(root_dir, ori_file.split("/")[-1].replace(".txt", "_Chinese.srt"))
        line_list.append(zh_srt_path)
        en_srt_path = os.path.join(root_dir, ori_file.split("/")[-1].replace(".txt", "_English.srt"))
        line_list.append(en_srt_path)
        ar_srt_path = os.path.join(root_dir, ori_file.split("/")[-1].replace(".txt", "_Arabic.srt"))
        line_list.append(ar_srt_path)
        pinyin_srt_path = os.path.join(root_dir, ori_file.split("/")[-1].replace(".txt", "_Pinyin.srt"))
        line_list.append(pinyin_srt_path)
        df_list.append(line_list)
    df = pd.DataFrame(df_list, columns=columns)
    df.to_csv(out_csv_file, index=False)


if __name__ == '__main__':
    ori_file_list = []
    
    ori_file_list.append("/Users/tal/work/lingtok_server/video_process/沙特女子Demo/初级汉语/lesson1/lesson1.txt")

    # create_aigc_csv(ori_file_list, "沙特女子Demo/DR_1.csv")
    # import pdb
    # pdb.set_trace()
    ori_excel_file = "/Users/tal/work/lingtok_server/video_process/沙特女子Demo/初级汉语/0111_PNU_初级口语-1_君_Easylove/PNU数据标记_Easylove.xls"
    sents_file = "/Users/tal/work/lingtok_server/video_process/沙特女子Demo/初级汉语/PNU_0113/lesson5.csv"
    df = pd.read_excel(ori_excel_file)

    df_list = df.values.tolist()
    columns = ["filename", "script"]
    df_new_list = list()
    # for i in range(73, 76):
    # for i in range(58, 72):
    # for i in range(46, 57):
    # for i in range(35, 41):
    for i in range(20, 34):
    # for i in range()
        tmp_list = list()
        tmp_list.append(df.iloc[i]["图片"].replace("png", "txt"))
        tmp_list.append(df.iloc[i]["语句"])
        df_new_list.append(tmp_list)
    df_new = pd.DataFrame(df_new_list, columns=columns)
    df_new.to_csv(sents_file)

    audio_dir = "/Users/tal/work/lingtok_server/video_process/沙特女子Demo/初级汉语/PNU_audio"

    root_dir = "/Users/tal/work/lingtok_server/video_process/沙特女子Demo/初级汉语/PNU_0113"
    video_dir = os.path.join(root_dir, "video")
    image_dir = "/Users/tal/work/lingtok_server/video_process/沙特女子Demo/初级汉语/0111_PNU_初级口语-1_君_Easylove/0111_PNU_初级口语-1_君_Easylove 照片"
    # for ori_file in ori_file_list:
    #     root_dir = "/".join(ori_file.split("/")[:-1])
    #     sents_file = ori_file.replace(".txt", "_sents.csv")
    #     cut_sentences(ori_file, sents_file)
    #     import pdb; pdb.set_trace()
    #     # import shutil
    #     # shutil.copy2(sents_file, "/Users/tal/work/lingtok_server/video_process/沙特女子Demo/Dr. Asmac材料1/合成文本/audio_csv")
    #     if not os.path.exists(root_dir):
    #         os.makedirs(root_dir)
    #     sents_file = os.path.join(root_dir, ori_file.split("/")[-1].replace(".txt", "_sents.csv"))
    #     sents = cut_sentences(ori_file, sents_file)

    
    video_dir = os.path.join(root_dir, "video")
    if not os.path.exists(video_dir):
        os.makedirs(video_dir)
    print (audio_dir)
    print (video_dir)

    final_video_path = os.path.join(video_dir, sents_file.split("/")[-1].replace(".csv", ".mp4"))
        # import shutil
        # shutil.copy2(final_video_path, "/Users/tal/work/lingtok_server/video_process/沙特女子Demo/Dr. Asmac材料1/合成文本/videos")


    
        # for root, dir, files in os.walk(image_dir):
        #     for file in files:
        #         if file.endswith(".png"):
        #             shutil.copy2(os.path.join(root, file), "/Users/tal/work/lingtok_server/video_process/沙特女子Demo/Dr. Asmac材料1/合成文本/素材/images")
        
        # for root, dir, files in os.walk(root_dir):
        #     for f in files:
        #         if f.endswith(".srt"):
        #             shutil.copy2(os.path.join(root, f), "/Users/tal/work/lingtok_server/video_process/沙特女子Demo/Dr. Asmac材料1/合成文本/素材/字幕")

        # if not os.path.exists(image_dir):
        #     os.makedirs(image_dir)
        
    # for filename, sent in tqdm(sents):
    #     print ("processing {}".format(filename))
    #     if os.path.exists(os.path.join(image_dir, filename + ".png")):
    #         continue
    #     try:
    #         call_huoshan_text2image(sent, os.path.join(image_dir, filename + ".png"))
    #     except Exception as e:
    #         print (str(e))
    #         print ("Failed to process {}".format(filename))
    
    audio_dur_dict, final_video_path = create_video(sents_file, image_dir, audio_dir, video_dir)
    res = generate_subtitle(sents_file, sents_file.split("/")[-1].replace(".csv", ""), root_dir, audio_dur_dict)
    add_subtitles_to_video(final_video_path, res["zh_srt"], res["ar_srt"], final_video_path.replace(".mp4", "_zh_ar.mp4"))
    
    
    



    # video_path = "/Users/tal/work/沙特女子Demo/Dr. Asmac材料1/合成文本/lesson1-part1/output_without_speech.mp4"
    # audio_path = "/Users/tal/work/沙特女子Demo/Dr. Asmac材料1/合成文本/lesson1-part1/yunyi.wav"
    # final_video_path = "/Users/tal/work/沙特女子Demo/Dr. Asmac材料1/合成文本/lesson1-part1/output.mp4"
    # images_to_video(output_video='/Users/tal/work/沙特女子Demo/Dr. Asmac材料1/合成文本/lesson1-part1/output_without_speech.mp4', fps=25)
    # add_audio_to_video(video_path, audio_path, final_video_path)
    # os.system("/opt/homebrew/Cellar/ffmpeg/7.1_3/bin/ffmpeg -i {} -i {} -c:v copy -c:a aac {}".format(video_path, audio_path, final_video_path))
    # zh_srt = "/Users/tal/work/沙特女子Demo/Dr. Asmac材料1/合成文本/lesson1-part1/lesson1-part1_Chinese.srt"
    # ar_srt = "/Users/tal/work/沙特女子Demo/Dr. Asmac材料1/合成文本/lesson1-part1/lesson1-part1_Arabic.srt"
    # srt_video_path = "/Users/tal/work/沙特女子Demo/Dr. Asmac材料1/合成文本/lesson1-part1/output_zh_ar.mp4"
    # add_subtitles_to_video(final_video_path, res["zh_srt"], res["ar_srt"], final_video_path.replace(".mp4", "_zh_ar.mp4"))