from volcengine.vod.VodService import VodService
from volcengine.const.Const import *
from volcengine.vod.models.request.request_vod_pb2 import VodListSpaceRequest, VodGetSpaceDetailRequest, VodUploadMediaRequest, VodGetMediaInfosRequest, VodUpdateMediaPublishStatusRequest, VodGetPlayInfoRequest, VodUploadMaterialRequest
from volcengine.util.Functions import Function
import json
from tqdm import tqdm
import os
import csv
import sys
import uuid
import pandas as pd

vod_service = VodService('cn-north-1')
space_name="lingotok"

vod_service.set_ak("AKLTOTgzODg1Y2FiNDI5NGE3Mzk3MWEzYzJlODE3MDk2MzQ")
vod_service.set_sk("TTJJM016azRaR0V3WXpRMk5EUXhPR0kyT0RBNVlUY3hZVGd5WlRrMlpHTQ==")

# space_name="lingotok-fr"
# vod_service = VodService('ap-southeast-1')

def upload_media(file_path, title="", tag="", desc=""):
    get_meta_function = Function.get_meta_func()
    snapshot_function = Function.get_snapshot_func(2.3)
    # get_start_workflow_func = Function.get_start_workflow_template_func(
    #     [{"TemplateIds": ["imp template id"], "TemplateType": "imp"},
    #      {"TemplateIds": ["transcode template id"], "TemplateType": "transcode"}])
    apply_function = Function.get_add_option_info_func(title, tag, desc, 0, False)
    filename = str(uuid.uuid4()) + ".mp4"
    try:
        req = VodUploadMediaRequest()
        req.SpaceName = space_name
        req.FilePath = file_path
        req.Functions = json.dumps([get_meta_function, snapshot_function, apply_function])
        req.CallbackArgs = ''
        req.FileName = filename
        req.FileExtension = '.mp4'
        req.StorageClass = 1
        req.UploadHostPrefer = ''
        resp = vod_service.upload_media(req)
    except Exception:
        print ("Upload failed! File: {}".format(file_path))
        return None
    else:
        if resp.ResponseMetadata.Error.Code == '':
            print(resp.Result.Data.Vid)
            print(resp.Result.Data.SourceInfo.FileName)
            return {"vid": resp.Result.Data.Vid, "title": title, "filename": filename}
        else:
            print(resp.ResponseMetadata.Error)
            print(resp.ResponseMetadata.RequestId)
            return None

def upload_srt(file_path, tag="", desc=""):
    apply_function = Function.get_add_option_info_func("test_title", "test", "test", 4, "srt")
    input_func = Function.get_caption_func(title="test", format="srt", vid="v18f18g00057ctnph4n3ksl620u6d7l0", fid="v18f18g00057ctnph4n3ksl620u6d7l0", language="eng-US", source="", tag="chinese", action_type="upload", store_uri="")

    try:
        req = VodUploadMaterialRequest()
        req.FileType = FILE_TYPE_MEDIA
        req.SpaceName = space_name
        req.FilePath = file_path
        req.Functions = json.dumps([apply_function, input_func])
        req.CallbackArgs = ''
        req.FileExtension = '.srt'
        req.UploadHostPrefer = ''

        resp = vod_service.upload_material(req)
        import pdb
        pdb.set_trace()

    except Exception:
        raise
    else:
        if resp.ResponseMetadata.Error.Code == '':
            print(resp.Result.Data.Vid)
            print(resp.Result.Data.SourceInfo.FileName)
            return {"vid": resp.Result.Data.Vid}
        else:
            print(resp.ResponseMetadata.Error)
            print(resp.ResponseMetadata.RequestId)
            return None

def traverse_and_upload(root_dir, output_csv, compress_ratio=1):
    data = []
    with open(output_csv, 'w', newline='', encoding='utf-8') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(['FileName', 'VID', 'DirName', "title", "vod_filename"])
        for dirpath, _, filenames in os.walk(root_dir):
            for filename in tqdm(filenames):
                if filename.endswith('.mp4'):
                    file_path = os.path.join(dirpath, filename)
                    dirname = os.path.basename(dirpath)
                    media_info = upload_media(file_path, desc=dirname)
                    
                    if media_info != None:
                        vid = media_info["vid"]
                        title = media_info["title"]
                        filename = media_info["filename"]
                        file_name_without_ext = file_path
                        csvwriter.writerow([file_name_without_ext, vid, dirname, title, filename])
                        # data.append([file_name_without_ext, vid, dirname, title, filename])

def get_mdeia_info(vid_list):
    try:
        vids = ",".join(vid_list)
        req = VodGetMediaInfosRequest()
        req.Vids = vids
        resp = vod_service.get_media_infos(req)
    except Exception:
        print ("Get Media Info Failed!")
        return None
    else:
        # print (resp)
        if resp.ResponseMetadata.Error.Code == '':
            # print(resp.Result)
            return resp.Result
        else:
            print(resp.ResponseMetadata.Error)
            return None

def change_media_status(vid, status="Published"):
    assert status in ["Published", "Unpublished"]
    try:
        status = status
        req3 = VodUpdateMediaPublishStatusRequest()
        req3.Vid = vid
        req3.Status = status
        resp3 = vod_service.update_media_publish_status(req3)
    except Exception:
        raise
    else:
        # print(resp3)
        if resp3.ResponseMetadata.Error.Code == '':
            print('update media publish status success')
        else:
            print(resp3.ResponseMetadata.Error)

def get_vid_playurl(vid):
    try:
        req = VodGetPlayInfoRequest()
        req.Vid = vid
        req.Ssl = '1'
        req.NeedOriginal = '1'
        # req.UnionInfo = 'your unionInfo'
        resp = vod_service.get_play_info(req)
    except Exception:
        raise
    else:
        # print(resp)
        if resp.ResponseMetadata.Error.Code == '':
            return resp.Result.PlayInfoList[0].MainPlayUrl.replace("https:", "http:")
        else:
            print(resp.ResponseMetadata.Error)



def upload_huoshan_withcsv(video_info_csv, out_csv):
    df = pd.read_csv(video_info_csv)
    columns= df.columns.tolist()
    columns.append("asset_id")
    # has_asset_id = False
    # if "asset_id" not in columns:
    #     columns.append("asset_id")
    # else:
    #     has_asset_id  = True
    #     for idx, col in enumerate(columns):
    #         if col == "asset_id":
    #             asset_idx = idx
    df_list = df.values.tolist()
    for i in tqdm(range(df.shape[0])):
        try:

            video_path = df.iloc[i]["FileName"]
            zh_srt_path = df.iloc[i]["zh_srt"].replace("\\", "/")
            en_srt_path = df.iloc[i]["en_srt"].replace("\\", "/")
            ar_srt_path = df.iloc[i]["ar_srt"].replace("\\", "/")
            py_srt_path = df.iloc[i]["pinyin_srt"].replace("\\", "/")
            title = "_".join(video_path.split("/")[-2:])
            print (title)
            description = video_path.split("/")[-2]
            print (description)
            media_info = upload_media(video_path, title=title, tag=description, desc=description)
            df_list[i].append(media_info["vid"])
            change_media_status(media_info["vid"], "Published")
        except Exception as e:
            df_list[i].append("nan")
            print (e)
            print ("error in {}".format(title))
    df_new = pd.DataFrame(df_list, columns=columns)
    df_new.to_csv(out_csv, index=False)

if __name__ == "__main__":
    # pass
    df = pd.read_csv("/Users/tal/work/lingtok_server/video_info_hw_created.csv")
    df.drop("asset_id", axis=1, inplace=True)
    df.drop("video_id", axis=1, inplace=True)
    df.to_csv("/Users/tal/work/lingtok_server/video_process/huoshan/video_info_800_wait_upload.csv", index=False)
    upload_huoshan_withcsv("/Users/tal/work/lingtok_server/video_process/huoshan/video_info_800_wait_upload.csv", "/Users/tal/work/lingtok_server/video_process/huoshan/video_info_800_uploaded.csv")
    
    
    # upload_dir_list = []

    # print (get_vid_playurl("v0d32eg10064csvcfn2ljhtd29dgu3rg"))


    # traverse_and_upload("huoshan/欢乐颂", "huoshan/欢乐颂.csv")
    
    # file_path = "/Users/tal/work/lingtok_server/video_process/hw/videos/记录生活/阿华日记/30出头啦，终于学会包粽子啦，提前祝大家端午安康！#回村的生活 #包粽子喽 #陪伴家人 #记录真实生活.mp4"
    # res = upload_media(file_path)

    # file_path = "/Users/tal/work/lingtok_server/video_process/沙特女子Demo/Dr. Asmac材料1/合成文本/lesson1-part3/lesson1-part3_Chinese.srt"
    # upload_srt(file_path)

    

    # req = VodListSpaceRequest()
    # req = VodGetSpaceDetailRequest()
    # req.SpaceName = 'lingotok'
    # resp = vod_service.get_space_detail(req)
    # print (resp)

    # sts2 = vod_service.get_upload_sts2_with_expired_time(60 * 60)
    # print(sts2)
