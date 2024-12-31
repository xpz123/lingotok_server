import os
from huaweicloudsdkcore.auth.credentials import BasicCredentials
from huaweicloudsdkvod.v1.region.vod_region import VodRegion
from huaweicloudsdkcore.exceptions import exceptions
from huaweicloudsdkvod.v1 import *
import requests
import hashlib
import base64
from tqdm import tqdm
import pandas as pd
from time import sleep



ak = "UI29JOFHTKQRBVVQ06TT"
sk = "vaMNt6dy5cJvXDlkzoWVNx3M8O0H5aIkneZSMZom"
projectID = "581332625acf4ebb871296d6a8ce90df"
credentials = BasicCredentials(ak, sk, projectID)

client = VodClient.new_builder() \
    .with_credentials(credentials) \
    .with_region(VodRegion.value_of("ap-southeast-3")) \
    .build()

def put_video(file_path, upload_video_url):
    with open(file_path, "rb") as f:
        response = requests.put(upload_video_url, data=f, headers={"Content-Type": "video/mp4"})
        if response.status_code == 200:
            return True
        else:
            print(f"文件上传失败，状态码: {response.status_code}, 原因: {response.text}")
            return False

def put_cover(file_path, upload_srt_url, md5, cover_type):
    with open(file_path, "rb") as f:
        # response = requests.put(upload_srt_url, data=f, headers={"Content-MD5": md5, "Content-Type": "image/{}".format(cover_type)})
        if cover_type == "jpg":
            cover_type = "jpeg"
        response = requests.put(upload_srt_url, data=f, headers={"Content-Type": "image/{}".format(cover_type)})
        if response.status_code == 200:
            return True
        else:
            print(f"文件上传失败，状态码: {response.status_code}, 原因: {response.text}")
            return False

def put_srt(file_path, upload_srt_url, md5):
    with open(file_path, "rb") as f:
        response = requests.put(upload_srt_url, data=f, headers={"Content-MD5": md5, "Content-Type": "application/octet-stream"})
        if response.status_code == 200:
            return True
        else:
            print(f"文件上传失败，状态码: {response.status_code}, 原因: {response.text}")
            return False

def call_convert(asset_id):
    try:
        request = CreateAssetProcessTaskRequest()
        request.body = AssetProcessReq(
            asset_id=asset_id,
            template_group_name="system_template_group"
        )
        response = client.create_asset_process_task(request)
        print(response)
    except exceptions.ClientRequestException as e:
        print(e.status_code)
        print(e.request_id)
        print(e.error_code)
        print(e.error_msg)

def upload_media(video_path, zh_srt_path=None, en_srt_path=None, ar_srt_path=None, py_srt_path=None, cover_path=None, title=None, description=None):
    # create asset
    video_name = video_path.split("/")[-1].replace(".mp4", "")
    try:
        request = CreateAssetByFileUploadRequest()
        listSubtitlesbody = []
        if py_srt_path != None:
            with open(py_srt_path, 'rb') as fp:
                data = fp.read()
                py_file_md5 = str(base64.b64encode(hashlib.md5(data).digest()), 'utf-8')
            py_srt_name = "{}_Pinyin.srt".format(video_name)
            listSubtitlesbody.append(Subtitle(
                id=1,
                type="SRT",
                language="CN",
                name=py_srt_name,
                md5=py_file_md5
            ))
        
        if en_srt_path != None:
            with open(en_srt_path, 'rb') as fp:
                data = fp.read()
                en_file_md5 = str(base64.b64encode(hashlib.md5(data).digest()), 'utf-8')
            en_srt_name = "{}_English.srt".format(video_name)
            listSubtitlesbody.append(Subtitle(
                id=2,
                type="SRT",
                language="EN",
                name=en_srt_name,
                md5=en_file_md5
            ))
        if ar_srt_path != None:
            with open(ar_srt_path, 'rb') as fp:
                data = fp.read()
                ar_file_md5 = str(base64.b64encode(hashlib.md5(data).digest()), 'utf-8')
            ar_srt_name = "{}_Arabic.srt".format(video_name)
            listSubtitlesbody.append(Subtitle(
                id=3,
                type="SRT",
                language="EN",
                name=ar_srt_name,
                md5=ar_file_md5
            ))
        if zh_srt_path != None:
            with open(zh_srt_path, 'rb') as fp:
                data = fp.read()
                zh_file_md5 = str(base64.b64encode(hashlib.md5(data).digest()), 'utf-8')
            zh_srt_name = "{}_Chinese.srt".format(video_name)
            listSubtitlesbody.append(Subtitle(
                id=4,
                type="SRT",
                language="CN",
                name=zh_srt_name,
                md5=zh_file_md5
            ))
        
        if title == None:
            title = video_name
        if description == None:
            description = ""
        if cover_path == None:
            request.body = CreateAssetByFileUploadReq(
                subtitles=listSubtitlesbody,
                video_type="MP4",
                video_name=video_name,
                description=description,
                title=title
            )
        else:
            with open(cover_path, 'rb') as fp:
                data = fp.read()
                cover_file_md5 = str(base64.b64encode(hashlib.md5(data).digest()), 'utf-8')
                cover_type = cover_path.split(".")[-1]
                assert cover_type in ["png", "jpg"]
            request.body = CreateAssetByFileUploadReq(
                subtitles=listSubtitlesbody,
                video_type="MP4",
                video_name=video_name,
                description=description,
                title=title,
                cover_type=cover_type.upper()
            )
            # cover_md5=cover_file_md5,
            
        response = client.create_asset_by_file_upload(request)
        video_upload_url = response.video_upload_url
        subtitle_upload_urls = response.subtitle_upload_urls
        # print (video_upload_url)
        # print (subtitle_upload_urls[0])
        videl_success = put_video(video_path, video_upload_url)
        assert videl_success
        if cover_path != None:
            cover_success = put_cover(cover_path, response.cover_upload_url, cover_file_md5, cover_type)
            assert cover_success
        
        if zh_srt_path != None:
            srt_success = put_srt(zh_srt_path, subtitle_upload_urls[3], zh_file_md5)
            assert srt_success
        if en_srt_path != None:
            srt_success = put_srt(en_srt_path, subtitle_upload_urls[1], en_file_md5)
            assert srt_success
        if ar_srt_path != None:
            srt_success = put_srt(ar_srt_path, subtitle_upload_urls[2], ar_file_md5)
            assert srt_success
        if py_srt_path != None:
            srt_success = put_srt(py_srt_path, subtitle_upload_urls[0], py_file_md5)
            assert srt_success

        request = ConfirmAssetUploadRequest()
        request.body = ConfirmAssetUploadReq(
            status="CREATED",
            asset_id=response.asset_id
        )
        response = client.confirm_asset_upload(request)
        print(response)
        call_convert(response.asset_id)
        return response.asset_id
        
    except Exception as e:
        print (e)
        return None


def upload_hw_withcsv(video_info_csv, out_csv):
    df = pd.read_csv(video_info_csv)
    columns= df.columns.tolist()
    has_asset_id = False
    if "asset_id" not in columns:
        columns.append("asset_id")
    else:
        has_asset_id  = True
        for idx, col in enumerate(columns):
            if col == "asset_id":
                asset_idx = idx
    df_list = df.values.tolist()
    df_new_list = []
    for i in tqdm(range(df.shape[0])):
        try:

            ori_list = df_list[i]
            if "compressed_FileName" in columns and df.iloc[i]["compressed_FileName"] != "null" and str(df.iloc[i]["compressed_FileName"]) != "nan":
                video_path = df.iloc[i]["compressed_FileName"]
            else:
                video_path = df.iloc[i]["FileName"]
            zh_srt_path = df.iloc[i]["zh_srt"].replace("\\", "/")
            en_srt_path = df.iloc[i]["en_srt"].replace("\\", "/")
            ar_srt_path = df.iloc[i]["ar_srt"].replace("\\", "/")
            py_srt_path = df.iloc[i]["pinyin_srt"].replace("\\", "/")
            title = "_".join(video_path.split("/")[-2:])
            print (title)
            description = video_path.split("/")[-2]
            print (description)
            # cover_path = None
            if (not "cover_path" in columns) or str(df.iloc[i]["cover_path"]) == "nan":
                cover_path = None
            else:
                cover_path = df.iloc[i]["cover_path"].replace("\\", "/")
            if has_asset_id:
                if str(df.iloc[i]["asset_id"]) != "nan":
                    df_new_list.append(ori_list)
                else:
                    try:
                        asset_id = upload_media(video_path, zh_srt_path=zh_srt_path, en_srt_path=en_srt_path, ar_srt_path=ar_srt_path, py_srt_path=py_srt_path, title=title, description=description, cover_path=cover_path)
                        if asset_id == None:
                            print ("###########wait for 3s#############")
                            sleep(3)
                        ori_list[asset_idx] = asset_id
                        df_new_list.append(ori_list)
                    except Exception as e:
                        print (e)
                        sleep(5)
            else:
                try:
                    asset_id = upload_media(video_path, zh_srt_path=zh_srt_path, en_srt_path=en_srt_path, ar_srt_path=ar_srt_path, py_srt_path=py_srt_path, title=title, description=description, cover_path=cover_path)
                    if asset_id == None:
                        print ("###########wait for 3s#############")
                        sleep(3)
                    ori_list.append(asset_id)
                    df_new_list.append(ori_list)
                except Exception as e:
                    print (e)
                    print ("wait for 10s")
                    sleep(10)
        except Exception as e:
            print (e)
            print ("error in {}".format(title))
    df_new = pd.DataFrame(df_new_list, columns=columns)
    df_new.to_csv(out_csv, index=False)

if __name__ == "__main__":
    pass
    # df = pd.read_csv("/Users/tal/work/lingtok_server/video_info_hw_created.csv")
    # df.drop("asset_id", axis=1, inplace=True)
    # df.drop("video_id", axis=1, inplace=True)
    # df.to_csv("/Users/tal/work/lingtok_server/video_process/hw/video_info_800_wait_upload.csv", index=False)
    # vod_csv_file = "/Users/tal/work/lingtok_server/video_process/hw/video_info_800_uploaded.csv"
    # upload_hw_withcsv("/Users/tal/work/lingtok_server/video_process/hw/video_info_800_wait_upload.csv", vod_csv_file)
    # df_vod = pd.read_csv(vod_csv_file)
    # null_num = df_vod["asset_id"].isnull().sum()
    # all_num = df_vod.shape[0]
    # while null_num > int(0.05 * all_num):
    #     upload_hw_withcsv(vod_csv_file, vod_csv_file)
    #     df_vod = pd.read_csv(vod_csv_file)
    #     null_num = df_vod["asset_id"].isnull().sum()
    #     all_num = df_vod.shape[0]

    # call_convert("fe0fbad10f6e0fe65cac76cf69972eb9")


    # upload_hw_withcsv("沙特女子Demo/DR_1.csv", "hw/DR_1.csv")
    # upload_hw_withcsv("hw/video_info_hw_500.csv", "hw/video_info_hw.csv")
    # upload_media("/Users/tal/work/lingtok_server/video_process/沙特女子Demo/Dr. Asmac材料1/合成文本/lesson1-part1/output.mp4", zh_srt_path="/Users/tal/work/lingtok_server/video_process/沙特女子Demo/Dr. Asmac材料1/合成文本/lesson1-part1/lesson1-part1_Chinese.srt", ar_srt_path="/Users/tal/work/lingtok_server/video_process/沙特女子Demo/Dr. Asmac材料1/合成文本/lesson1-part1/lesson1-part1_Arabic.srt", en_srt_path="/Users/tal/work/lingtok_server/video_process/沙特女子Demo/Dr. Asmac材料1/合成文本/lesson1-part1/lesson1-part1_Arabic.srt", cover_path="/Users/tal/work/lingtok_server/video_process/沙特女子Demo/Dr. Asmac材料1/合成文本/lesson1-part1/sent2.png", title="test", description="test test")
    # upload_media("/Users/tal/work/lingtok_server/video_process/沙特女子Demo/Dr. Asmac材料1/合成文本/lesson1-part1/output.mp4", zh_srt_path="/Users/tal/work/lingtok_server/video_process/沙特女子Demo/Dr. Asmac材料1/合成文本/lesson1-part1/lesson1-part1_Chinese.srt", en_srt_path="/Users/tal/work/lingtok_server/video_process/沙特女子Demo/Dr. Asmac材料1/合成文本/lesson1-part1/lesson1-part1_English.srt", title="test", description="test test")
    # ak = "UI29JOFHTKQRBVVQ06TT"
    # sk = "vaMNt6dy5cJvXDlkzoWVNx3M8O0H5aIkneZSMZom"
    # projectID = "581332625acf4ebb871296d6a8ce90df"
    # credentials = BasicCredentials(ak, sk, projectID)

    # client = VodClient.new_builder() \
    #     .with_credentials(credentials) \
    #     .with_region(VodRegion.value_of("ap-southeast-3")) \
    #     .build()
    
    # try:
    #     request = CreateAssetByFileUploadRequest()
    #     listSubtitlesbody = [
    #         Subtitle(
    #             id=1,
    #             type="SRT",
    #             language="CN",
    #             name="/Users/tal/work/lingtok_server/video_process/沙特女子Demo/Dr. Asmac材料1/合成文本/lesson1-part1/lesson1-part1_Chinese.srt"
    #         )
    #     ]
    #     request.body = CreateAssetByFileUploadReq(
    #         subtitles=listSubtitlesbody,
    #         video_type="MP4",
    #         video_name="/Users/tal/work/lingtok_server/video_process/沙特女子Demo/Dr. Asmac材料1/合成文本/lesson1-part1/output.mp4",
    #         description="test",
    #         title="123"
    #     )
    #     response = client.create_asset_by_file_upload(request)
    #     print(response)
    # except exceptions.ClientRequestException as e:
    #     print(e.status_code)
    #     print(e.request_id)
    #     print(e.error_code)
    #     print(e.error_msg)
    # put_video("/Users/tal/work/lingtok_server/video_process/沙特女子Demo/Dr. Asmac材料1/合成文本/lesson1-part1/output.mp4", "https://vod-bucket-01-ap-southeast-3.obs.ap-southeast-3.myhuaweicloud.com:443/581332625acf4ebb871296d6a8ce90df/30f98e227e31cce46075c9d88a4f0677/99381ef6e56ad209958061b8ae73c532.mp4?AWSAccessKeyId=UQEP46QZTV2WIAUZ8G8Y&Expires=1734414691&Signature=%2BwhCHACjZ3IKcB%2BTjnbfmsUmnnc%3D")

    # try:
    #     request = ConfirmAssetUploadRequest()
    #     request.body = ConfirmAssetUploadReq(
    #         status="CREATED",
    #         asset_id="30f98e227e31cce46075c9d88a4f0677"
    #     )
    #     response = client.confirm_asset_upload(request)
    #     print(response)
    # except exceptions.ClientRequestException as e:
    #     print(e.status_code)
    #     print(e.request_id)
    #     print(e.error_code)
    #     print(e.error_msg)

