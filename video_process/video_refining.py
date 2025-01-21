import os
os.environ["IMAGEIO_FFMPEG_EXE"] = "/opt/homebrew/Cellar/ffmpeg/7.1_4/bin/ffmpeg"
from moviepy.editor import VideoFileClip, AudioFileClip, ImageClip, CompositeVideoClip, concatenate_videoclips, ColorClip
from moviepy.video.fx.all import speedx
# from pydub import AudioSegment
import random
import librosa
import soundfile as sf
import numpy as np
import sys

def refine_videos(video_dir, output_dir):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    video_processor = VideoProcessor()
    for name in os.listdir(video_dir):
        if name.endswith(".mp4"):
            new_file = name.replace(".mp4", "_refined.mp4")
            video_processor.change_pitch(os.path.join(video_dir, name), os.path.join(output_dir, new_file), 3)
            # video_processor.add_watermark(os.path.join(output_dir, new_file), os.path.join(output_dir, new_file))
            video_processor.step_printing(os.path.join(output_dir, new_file), os.path.join(output_dir, new_file))
            video_processor.split_and_change_speed(os.path.join(video_dir, name), os.path.join(output_dir, new_file), 30, 2)
            video_processor.add_sticker(os.path.join(output_dir, new_file), os.path.join(output_dir, new_file), "watermark.png", (0.8, 0.8))
            video_processor.add_light_sweep(os.path.join(video_dir, name), os.path.join(output_dir, new_file))
class VideoProcessor:
    def __init__(self):
        pass

    def change_pitch(self, input_video_file, output_audio_file, semitones=3):
        """
        Change the pitch of the audio in the video file.
        Args:
            input_video_file (str): The input video file.
            output_audio_file (str): The output audio file.
            semitones (int): The number of semitones to change the pitch.
        """ 
        try:
            #import pdb; pdb.set_trace()
            # load video file
            video = VideoFileClip(input_video_file)
            # extract audio and save
            audio_path = input_video_file.replace(".mp4", ".wav")
            video.audio.write_audiofile(audio_path)
            #load audio file
            #audio = AudioSegment.from_file(audio_path, format="mp3")
            y, sr = librosa.load(audio_path, sr=None)
            # change pitch, raise the pitch by 3 semitones
            #higher_pitch_audio = audio._spawn(audio.raw_data, overrides={"frame_rate": int(audio.frame_rate * 2 ** (semitones / 12))}).set_frame_rate(audio.frame_rate)
            #higher_pitch_audio = higher_pitch_audio.speedup(playback_speed=1.0 / (2 ** (semitones / 12)))
            y_shifted = librosa.effects.pitch_shift(y=y, sr=sr, n_steps=semitones)
            # save the new audio
            higher_pitch_audio_path = audio_path.replace(".wav", "_higher_pitch.wav")
            #higher_pitch_audio.export(higher_pitch_audio_path, format="mp3")
            sf.write(higher_pitch_audio_path, y_shifted, sr, format='WAV')
            # merge the new audio and video
            new_audio = AudioFileClip(higher_pitch_audio_path).set_duration(video.duration)
            new_video = video.set_audio(new_audio)
            # save the processed video
            new_video.write_videofile(output_audio_file, codec='libx264')
            #delete the temporary audio files
            os.remove(audio_path)
            os.remove(higher_pitch_audio_path)
        except Exception as e:
            print (str(e))
            print ("Cannot change pitch for video file {}".format(input_video_file))
    
    def add_watermark(self, input_video_file, output_audio_file, size_scale=0.2, opacity_scale=0.2):
        """
        Add watermark to the video file.
        Args:
            input_video_file (str): The input video file.
            output_audio_file (str): The output audio file.
            size_scale (float): The scale of the watermark size. Default is 0.2.
            opacity_scale (float): The scale of the watermark opacity.  Default is 0.2.
        """
        try:
            # load video file
            video = VideoFileClip(input_video_file)
            # add watermark
            watermark = (ImageClip("/Users/tal/work/lingtok_server/video_process/沙特女子Demo/Dr. Asmac材料1/合成文本/lesson1-part1/sent1.png").set_duration(video.duration).set_opacity(opacity_scale))
            watermark = watermark.resize(size_scale)
            # set watermark position
            watermark = watermark.set_position((video.size[0] - watermark.size[0] - 10, video.size[1] - watermark.size[1] - 10))
            # composite video and watermark
            new_video = CompositeVideoClip([video, watermark])
            # save the processed video
            new_video.write_videofile(output_audio_file, codec="libx264")
        except Exception as e:
            print (str(e))
            print ("Cannot add watermark for video file {}".format(input_video_file))

    def step_printing(self, input_video_file, output_audio_file, frame_per_second=2):
        """
        Step printing the video file.
        Args:
            input_video_file (str): The input video file.
            output_audio_file (str): The output audio file.
            frame_per_second (int): The number of frames per second. Default is 2.
        """
        try:
            # load video file
            video = VideoFileClip(input_video_file)
            # set the frame interval, 2 frames per second
            frame_interval = int(video.fps / frame_per_second)

            clips = []
            
            for i, frame in enumerate(video.iter_frames()):
                if i % frame_interval != 0:
                    # creat new frame clip
                    start_time = i / video.fps
                    end_time = (i + 1) / video.fps
                    frame_clip = video.subclip(start_time, end_time)
                    clips.append(frame_clip)

            # merge all remaining clips into a new video
            new_video = concatenate_videoclips(clips, method="compose")
            # save the processed video
            new_video.write_videofile(output_audio_file, codec="libx264")
        except Exception as e:
            print (str(e))
            print ("Cannot step printing for video file {}".format(input_video_file))

    def split_and_change_speed(self, input_video_file, output_audio_file, changed_second=10, speed_factor=1.1):
        """
        Split the video file and change the speed.
        Args:
            input_video_file (str): The input video file.
            output_audio_file (str): The output audio file.
            changed_second (int): The number of seconds to change the speed. Default is 10.
            speed_factor (float): The speed factor of the video. Default is 1.1.
        """
        try:
            # import pdb; pdb.set_trace()
            # load video file
            video = VideoFileClip(input_video_file)
            total_duration = video.duration
            # calculate the start time and end time of the middle part
            start_time = (total_duration - changed_second) / 2
            end_time = start_time + 10
            # split the middle part video
            middle_clip = video.subclip(start_time, end_time)
            # change the speed of the middle part video
            speed_clip = middle_clip.fx(speedx, speed_factor)
            # merge all clips into a new video
            clips = [video.subclip(0, start_time), speed_clip, video.subclip(end_time)]
            new_video = concatenate_videoclips(clips, method="compose")
            # save the processed video
            new_video.write_videofile(output_audio_file, codec="libx264")
        except Exception as e:
            print (str(e))
            print ("Cannot split and change speed for video file {}".format(input_video_file))

    def add_sticker(self, input_video_file, output_audio_file, sticker_file, position=(0.8, 0.8)):
        """
        Add sticker to the video file.
        Args:
            input_video_file (str): The input video file.
            output_audio_file (str): The output audio file.
            sticker_file (str): The sticker file.
            position (tuple): The position of the sticker. Default is (0.8, 0.8).
        """
        try:
            # load video file
            video = VideoFileClip(input_video_file)
            # add sticker
            sticker = (ImageClip(sticker_file).set_duration(video.duration))
            # set sticker position
            sticker = sticker.set_position(position)
            # composite video and sticker
            new_video = CompositeVideoClip([video, sticker])
            # save the processed video
            new_video.write_videofile(output_audio_file, codec="libx264")
        except Exception as e:
            print (str(e))
            print ("Cannot add sticker for video file {}".format(input_video_file))

    def add_light_sweep(self, input_video_file, output_audio_file, sweep_duration=2, sweep_color=(255, 255, 255), sweep_width=50):
        """
        Light sweep the video file.
        Args:
            input_video_file (str): The input video file.
            output_audio_file (str): The output audio file.
        """
        try:
            # import pdb; pdb.set_trace()
            # load video file
            video = VideoFileClip(input_video_file)
            # get the size amd duration of the video
            video_duration = video.duration
            video_size = video.size

            def make_frame(t):
                t = t % sweep_duration
                frame = np.zeros((video_size[1], video_size[0], 3), dtype=np.uint8)
                x = int((t / sweep_duration) * video_size[0])
                frame[:, max(0, x - sweep_width):x] = sweep_color
                return frame

            # create a light sweep effect
            light_sweep = ColorClip(size=video_size, color=(0, 0, 0)).set_duration(video_duration).set_make_frame(make_frame)
            # composite video and light sweep
            new_video = CompositeVideoClip([video, light_sweep.set_opacity(0.05)])
            # save the processed video
            new_video.write_videofile(output_audio_file, codec="libx264")
        except Exception as e:
            print (str(e))
            print ("Cannot light sweep for video file {}".format(input_video_file))

if __name__ == "__main__":
    ori_dir = sys.argv[1]
    output_dir = sys.argv[2]
    refine_videos(ori_dir, output_dir)