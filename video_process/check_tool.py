import os
import sys

def cmp_two_dirs(dir1, dir2):
    files1 = os.listdir(dir1)
    files2 = os.listdir(dir2)
    img_files1 = set([file for file in files1 if file.endswith(".jpg") or file.endswith(".png") or file.endswith(".gif")])
    video_files1 = set([file for file in files1 if file.endswith(".mp4")])
    img_files2 = set([file for file in files2 if file.endswith(".jpg") or file.endswith(".png") or file.endswith(".gif")])
    video_files2 = []
    for file in files2:
        if file.endswith(".mp4"):
            ori_file = file.replace(".mp4", "").split("_NEW")[0] + ".mp4"
            video_files2.append(ori_file)
    video_files2 = set(video_files2)

    diff_img_files = img_files1.difference(img_files2)
    diff_video_files = video_files1.difference(video_files2)

    print ("########Diff image files count: {}##########".format(len(diff_img_files)))
    print ("########Diff video files count: {}##########".format(len(diff_video_files)))
    print ("########Diff image files: ##########")
    for file in diff_img_files:
        print (file)
    print ("########Diff video files: ##########")
    for file in diff_video_files:
        print (file)


if __name__ == "__main__":
    ori_dir = sys.argv[1]
    modified_dir = sys.argv[2]
    cmp_two_dirs(ori_dir, modified_dir)