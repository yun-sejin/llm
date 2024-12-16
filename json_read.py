import os
import json
from datetime import datetime

# def read_json_files(base_dir):
#     all_data = []
#     for root, dirs, files in os.walk(base_dir):
#         for file in files:
#             if file.endswith('.json'):
#                 file_path = os.path.join(root, file)
#                 with open(file_path, 'r', encoding='utf-8') as json_file:
#                     data = json.load(json_file)
#                     all_data.append(data)
#     return all_data

# # 현재 일자를 YYYYMMDD 형식으로 포맷하여 base_dir 경로 설정
# base_dir = r'C:\upload\{}'.format(datetime.now().strftime('%Y%m%d'))
# all_data = read_json_files(base_dir)
# print(all_data)
# print('#'*20)
# print(f"Loaded {len(all_data)} JSON files.")

# all_data를 파일 이름을 키로 하고, 파일 내용을 값으로 하는 사전으로 변경
# def read_json_files_to_dict(base_dir):
#     all_data = {}
#     for root, dirs, files in os.walk(base_dir):
#         for file in files:
#             if file.endswith('.json'):
#                 file_path = os.path.join(root, file)
#                 with open(file_path, 'r', encoding='utf-8') as json_file:
#                     data = json.load(json_file)
#                     all_data[file] = data
#     return all_data

# # 수정된 함수 사용
# base_dir = r'C:\upload\{}'.format(datetime.now().strftime('%Y%m%d'))
# all_data_dict = read_json_files_to_dict(base_dir)
# print(all_data_dict)
# print('#'*20)
# print(f"Loaded {len(all_data_dict)} JSON files.")

#키를 파일이름 대신에 인덱스로 변경
def read_json_files_to_dict(base_dir):
    all_data = {}
    index = 0
    for root, dirs, files in os.walk(base_dir):
        for file in files:
            if file.endswith('.json'):
                file_path = os.path.join(root, file)
                with open(file_path, 'r', encoding='utf-8') as json_file:
                    data = json.load(json_file)
                    all_data[index] = data
                    index += 1
    return all_data

# 수정된 함수 사용
# base_dir = r'C:\upload\{}'.format(datetime.now().strftime('%Y%m%d'))
# all_data_dict = read_json_files_to_dict(base_dir)
# print(all_data_dict)
# print('#'*20)
# print(f"Loaded {len(all_data_dict)} JSON files.")