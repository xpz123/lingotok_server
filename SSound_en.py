#!/usr/bin/env python
#coding:utf-8
import ctypes   #用于调用C语言库。
import json
from postwarrant import *
import time, os
from multiprocessing import Pool, cpu_count
from requests.exceptions import HTTPError, ConnectionError, Timeout, RequestException
from tqdm import tqdm
import time
import sys
import logging
logging.basicConfig(level=logging.DEBUG)




try:
    import queue
    q = queue.Queue()
except ImportError as e:
    import Queue
    q = Queue.Queue()

import platform
import logging
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s %(process)d %(funcName)s %(lineno)d: %(message)s', level=logging.INFO)

sysstr = platform.system()
current_dir = os.path.dirname(os.path.abspath(__file__))
if(sysstr =="Windows"):
    #加载 Windows 系统下的 DLL 文件
    engine = ctypes.WinDLL(current_dir + "/ssound.dll")
else:
    engine = ctypes.CDLL(current_dir + "/libssound.so")

callback = ctypes.CFUNCTYPE(ctypes.c_int, ctypes.c_void_p, ctypes.POINTER(ctypes.c_char), ctypes.c_int, ctypes.POINTER(ctypes.c_char), ctypes.c_int)


def onResult(userdata, sid, atype, message, size):
    #print type(userdata)
    #print "%X" % userdata
    #obj  = SSound.from_address(userdata)
    #print obj

    #print "sid:" + ctypes.string_at(sid)
    #atype = 0 +kktype
    #print "atype:%d" % atype
    message = ctypes.string_at(message)
    #print q
    m = json.loads(message)
    q.put(m)
    #print q.qsize()
    #print "returned message: %s" % json.loads(message)
    return 0

func = callback(onResult)

class SSound(ctypes.Structure):
    def __init__(self, server, appKey='t0007j1', secretKey='24lsWDswN8Lx2qCTB1VxkoVh8j706PPp'):
        #设置参数"logLevel": 为4输出所有的日志信息
        p_cfg = { 
                "appKey": "t0007j1",
                "secretKey": "24lsWDswN8Lx2qCTB1VxkoVh8j706PPp",
                "logLevel": 1,
                "cloud": { 
                "server": "ws://trial.cloud.ssapi.cn:8080", 
                "coreProvideType": "cloud", 
                }}
        if (server):
            p_cfg["cloud"]["server"] = server

        if appKey:
            p_cfg['appKey'] = appKey
            p_cfg['secretKey'] = secretKey

        logging.info(p_cfg) #cfg = ctypes.c_char_p(json.dumps(p_cfg))
        cfg = ctypes.c_char_p(json.dumps(p_cfg).encode())#创建一个C语言风格的字符串指针，用于传递给C语言函数。
        #print cfg
        engine.ssound_new.restype=ctypes.POINTER(ctypes.c_int)#设置了由engine.ssound_new函数返回的值的类型。restype属性用于指定函数返回值的预期类型。
        self.ss = engine.ssound_new(cfg)
        self.st = 0
        self.et = 0
        self.filesize = 0

    #def __del__(self):
        #engine.ssound_delete(self.ss)

    def delete(self):
        engine.ssound_delete(self.ss)

    def getResult(self):
        #print q.qsize();
        item = q.get(True, 20)
        self.et = time.time()
        return item
        #print q.qsize();


    def start(self, param, rid, resultCallback):
        #print "start: %s" % param["request"]["refText"]
        cfg = json.dumps(param)
        #print cfg
        param = ctypes.c_char_p(cfg.encode())
        rid = ctypes.c_char * 64
        #print "start:"
        #print "%X" % id(self)
        #self.st = time.time()
        self.et = 0
        self.filesize = 0
        r = engine.ssound_start(self.ss, param, rid(), func, id(self))
        if r == 0:
            return 
        self.stop()

    def feed(self, data):
        #print "feed: %d" % len(data)
        r = engine.ssound_feed(self.ss, ctypes.c_char_p(data), len(data))
        #print "feed result %d" % r
        if r == 0:
            return 
        self.stop()

    def stop(self):
        #print "stop:"
        engine.ssound_stop(self.ss)

    def feedFile(self, wav):
        #print "openg file: %s" % wav
        
        f = open(wav, 'rb')
        try:
            while True:
                chunk = f.read(6400)
                if not chunk:
                    break
                self.feed(chunk)
                self.filesize = self.filesize + len(chunk)
                time.sleep(0.1)
                del chunk
            #self.st = time.time()
        finally:
            f.close()

def multi_get_res(file_path,output_file):
    with open(file_path, 'r') as file:
        lines = file.readlines()
    with Pool(processes=1) as pool:
        with open(output_file, 'w') as fw:
            for res in tqdm(pool.imap_unordered(process_line, lines), total=len(lines)):
            # for res in pool.imap_unordered(process_line, lines):

                if res is not None:
                    fw.write(res)
    # obj.delete()
            
            

def process_line(line):
    wavpath = line.split(" ",1)[0]
    text = json.loads(line.split(" ",1)[1])["text"]
    # print("====== w ======: ", w)
    p_cfg = { \
            "coreProvideType": "cloud",
            "app": {"userId": "TAL_test","warrantId":w}, 
            "audio": {"audioType": "wav", "channel": 1, "sampleBytes": 2, "sampleRate": 16000},  
            "request": {"coreType": "en.sent.score", "refText":text, "rank": 100}
            }
    obj.start(p_cfg, '', func)
    obj.feedFile(wavpath)
    obj.stop()
    # print("====== wavpath ======: ", wavpath)
    res = obj.getResult()
    # print(res)
    return wavpath+"\t" + json.dumps(res,ensure_ascii = False)+"\n"
if __name__ == '__main__':
    w = get_warrantID()
    obj = SSound("ws://api.cloud.ssapi.cn:8080", "a0007iu", "GVAxSbufDgXWxRnDSGBYrJejAhBajXDA")
    # wavdata = "/mnt/pfs/jinfeng_team/SFT/luohaixia/workspace/evl/data/test_data/api/xiansheng_py_demon/test.txt"
    # res_name = "/mnt/pfs/jinfeng_team/SFT/luohaixia/workspace/evl/data/test_data/api/xiansheng_py_demon/tmp"
    wavdata = sys.argv[1] #输入音频的wavpath和文本的json格式
    res_name = sys.argv[2]  #输出结果
    
    fr = open(wavdata, "r")
    lines = fr.readlines()
    fw = open(res_name, "w")
    for line in tqdm(lines):
        res = process_line(line)
        fw.write(res)
    obj.delete()
