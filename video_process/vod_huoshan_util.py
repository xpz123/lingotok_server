from volcengine.vod.VodService import VodService
from volcengine.vod.models.request.request_vod_pb2 import VodListSpaceRequest, VodGetSpaceDetailRequest, VodUploadMediaRequest, VodGetMediaInfosRequest, VodUpdateMediaPublishStatusRequest, VodGetPlayInfoRequest
from volcengine.util.Functions import Function
import json
from tqdm import tqdm
import os
import csv
import sys
import uuid

vod_service = VodService('cn-north-1')
vod_service.set_ak("AKLTOTgzODg1Y2FiNDI5NGE3Mzk3MWEzYzJlODE3MDk2MzQ")
vod_service.set_sk("TTJJM016azRaR0V3WXpRMk5EUXhPR0kyT0RBNVlUY3hZVGd5WlRrMlpHTQ==")

def upload_media(file_path, space_name="lingotok", tag="", desc=""):
    space_name = space_name
    get_meta_function = Function.get_meta_func()
    snapshot_function = Function.get_snapshot_func(2.3)
    get_start_workflow_func = Function.get_start_workflow_template_func(
        [{"TemplateIds": ["imp template id"], "TemplateType": "imp"},
         {"TemplateIds": ["transcode template id"], "TemplateType": "transcode"}])
    title = os.path.basename(file_path).replace(".mp4", "")
    apply_function = Function.get_add_option_info_func(title, tag, desc, 0, False)
    filename = str(uuid.uuid4()) + ".mp4"
    try:
        req = VodUploadMediaRequest()
        req.SpaceName = space_name
        req.FilePath = file_path
        req.Functions = json.dumps([get_meta_function, snapshot_function, get_start_workflow_func, apply_function])
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
        # print(resp)
        if resp.ResponseMetadata.Error.Code == '':
            # print(resp.Result.Data)
            print(resp.Result.Data.Vid)
            # print(resp.Result.Data.PosterUri)
            print(resp.Result.Data.SourceInfo.FileName)
            # print(resp.Result.Data.SourceInfo.Height)
            # print(resp.Result.Data.SourceInfo.Width)
            return {"vid": resp.Result.Data.Vid, "title": title, "filename": filename}
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
        status = 'Unpublished'
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


if __name__ == "__main__":
    # pass
    # upload_dir_list = []

    print (get_vid_playurl("v0d32eg10064csvcfn2ljhtd29dgu3rg"))


    # traverse_and_upload("huoshan/欢乐颂", "huoshan/欢乐颂.csv")
    

    # file_path = "/Users/tal/work/lingtok_server/video_process/chinese_test/安迪承认有点喜欢包奕凡#因为一个片段看了整部剧 #欢乐颂2 #我在抖音追剧 #追剧不能停 #好剧推荐.mp4"
    # upload_media(file_path)

    

    # req = VodListSpaceRequest()
    # req = VodGetSpaceDetailRequest()
    # req.SpaceName = 'lingotok'
    # resp = vod_service.get_space_detail(req)
    # print (resp)

    # sts2 = vod_service.get_upload_sts2_with_expired_time(60 * 60)
    # print(sts2)
