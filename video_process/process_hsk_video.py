import pandas as pd
import os
import csv
from translator import translate_text2ar
from pypinyin import pinyin

def trans_word_to_ar(ori_csv_file, ar_csv_file):
    df = pd.read_csv(ori_csv_file)
    columns = df.columns.to_list()
    columns.append("阿语翻译")
    text_list = list()
    for i in range(df.shape[0]):
        text = df.iloc[i]["单词名字"]
        text_list.append(text.split("（")[0])
    resp_list = translate_text2ar(text_list, "ar")
    assert len(text_list) == len(resp_list)
    ar_text_list = [resp["Translation"] for resp in resp_list]
    df["阿语翻译"] = ar_text_list
    df.to_csv(ar_csv_file, index=False)

def add_pinyin(ori_csv_file, py_csv_file):
    df = pd.read_csv(ori_csv_file)
    columns = df.columns.to_list()
    columns.append("拼音")
    df_list = df.values.tolist()
    for i in range(df.shape[0]):
        py_list = pinyin(df.iloc[i]["单词名字"])
        py_str = ""
        for item in py_list:
            for py in item:
                py_str += " " + py
        df_list[i].append(py_str)
    df_new = pd.DataFrame(df_list, columns=columns)
    df_new.to_csv(py_csv_file, index=False)


if __name__ == "__main__":
    print (pinyin("的"))
    # add_pinyin("/Users/tal/work/lingtok_server/video_process/HSK_video/word_ar.csv", "/Users/tal/work/lingtok_server/video_process/HSK_video/word_ar_py.csv")
    # trans_word_to_ar("/Users/tal/work/lingtok_server/video_process/HSK_video/words.csv", "/Users/tal/work/lingtok_server/video_process/HSK_video/word_ar.csv")