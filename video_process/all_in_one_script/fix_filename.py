import os
import sys

def fix_filename_encoding(directory):
    for root, dirs, files in os.walk(directory):
        for file in files:
            try:
                # 尝试将文件名从GBK编码转换为UTF-8
                original_name = file.encode('latin1').decode('gbk')
                new_name = original_name.encode('utf-8').decode('utf-8')
                
                # 重命名文件
                if original_name != file:
                    old_file_path = os.path.join(root, file)
                    new_file_path = os.path.join(root, new_name)
                    os.rename(old_file_path, new_file_path)
                    print(f'Renamed: {old_file_path} -> {new_file_path}')
            except Exception as e:
                print(f'Error renaming {file}: {e}')

if __name__  == "__main__":
    fix_filename_encoding(sys.argv[1])