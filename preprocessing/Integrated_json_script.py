import os
import json
import re
import shutil
import glob
import random
import pandas as pd
from tqdm.auto import tqdm
from typing import List


error_cases = {}

# json path 전체 반환
def get_json_file_paths(src_path:str,exclude_directories: List[str] = None) -> List[str]:
    '''
    지정된 디렉토리(src_path)에서 모든 json file을 재귀적으로 탐색 -> exclude_dirs list에 담긴 directory명들은 탐색에서 제외
    
    Args:
        src_path (str): json file 찾을 디렉토리 경로
        exclude_dirs (List[str], optional): 검색에서 제외할 하위 디렉토리 이름 목록
        
    Returns:
        List[str]: 디렉토리(src_path) 경로에서 발견된 모든 json 파일의 경로 목록 반환
    '''
    
    total_json_paths = []
    for root,dirs,files in os.walk(src_path):
        if exclude_directories:
            dirs[:] = [one_src_path for one_src_path in dirs if one_src_path not in exclude_directories]
        else:
            pass
        for file in files:
            if file.endswith('.json'):
                total_json_paths.append(os.path.join(root,file))
    return total_json_paths
    
# json path 1개 반환 제너레이터
def yield_json_file_paths(src_path:str) -> List[str]:
    '''
    지정된 디렉토리(src_path)에서 모든 json file path 생성 -> json file path 하나씩 반환하는 제너레이터 함수
    
    Args:
        src_path (str): json file 찾을 디렉토리 경로
    
    Yield : 
        str : json file path
    
    '''
    for root,dirs,files in os.walk(src_path):
        for file in files :
            if file.endswith('.json'):
                yield os.path.join(root,file)
                
# json file 읽고 반환
def load_json_file(src_file_path:str) -> dict:
    '''
    json file을 읽고 딕셔너리 데이터로 반환
    
    Args:
        src_file_path (str): 읽을 json file 경로
    
    Returns:
        dict: json file data를 담은 dict 반환
    '''
    if os.path.isfile(src_file_path) and src_file_path.endswith('.json'):
        try :
            with open(src_file_path,'r',encoding='utf-8-sig') as f:
                return json.load(f)
        except :
            error_cases.setdefault('read_json_error : wrong json', []).append(src_file_path)
    else :
         error_cases.setdefault('read_json_error : check path', []).append(src_file_path)
            
# 경로내에 전체 json file 내용 반환
def load_json_files(src_path:str,mode:str = 'yield') -> List[dict]:
    '''
    src_path 경로에 존재하는 json file을 list에 담고 반환
    
    Args:
        src_path (str): json file 찾을 디렉토리 경로
    
    Returns:
        List(dir): 전체 json 경로 반환
    '''
    total_json = []
    # json path yield
    if mode == 'yield':
        for src_file_path in yield_json_file_paths(src_path):
            total_json.append(load_json_file(src_file_path))
        return total_json
        
    # find all json path
    else: 
        return get_json_file_paths(src_path)
    
# label 통계
def get_annotation_counts(total_json:list, key:str = 'label') -> dict:
    '''
    tatol_json의 annotation에 대하여 각 key값별 수량
    
    Args:
        total_json (dict): 전체 json의 annotation이 담긴 list
        key (str,optional):
        
    Returns:
        dict: 전체 instance에 대한 value 수량
        
    
    '''
    annotation_counts = {}
    for one_json in total_json:
        for one_annotation in one_json['annotations']:
            annotation_label[one_annotation[f'{key}']] = annotation_label.get(one_annotation[f'{key}'],0)+1
    return annotation_label
    
# json file에서 key값들 삭제    
def delete_keys_from_json(json_data:dict,delete_keys: str = ['road_type',"illumination_status","road_status","sensor_status"]) -> dict:
    '''
    json file에서 특정 key값들을 삭제한 후 반환한다.
    
    Args:
        json_data (dict): 하나의 json file이 담긴 dict
        delete_keys (str): 삭제할 key값들
        
    Returens:
        dict: 특정 key값들을 삭제한 dict를 반환
        
    
    '''
    try :
        for key in delete_keys:
            del json_data[key]
    except :
        if json_data['parent_path']:
            error_cases.setdefault('delete_keys_error: check keys ', []).append(json_data['parent_path'] + '/' +json_data['filename'])
        else :
            error_cases.setdefault('delete_keys_error: check keys ', []).append('no path check json')
        
    return json_data
    
# batch 단위 샘플링
def sample_extract_per_batch(save_path:str,batch_path:str,sample_num:int = 10) -> None:
    '''
    batch 단위별로 saple_num 개수만큼 파일을 추출한다.
    
    Args:
        save_path (str): 저장 경로
        batch_path (str): batch 디렉토리 경로
        sample_num (int): 추출할 file 수량
        
    Returns:
        None
    '''
    for batch_file in os.listdir(batch_path):
        batch_num_path = os.path.join(batch_path,batch_file)
        batch_json = get_json_file_paths(batch_num_path)
        batch_json_sample = random.sample(batch_json,sample_num)
        for sample in batch_json_sample:
            os.makedirs(os.path.join(save_path,batch_file),exist_ok=True)
            shutil.copy(sample,os.path.join(save_path,batch_file,os.path.basename(sample)))
            # sample_img_path = sample.replace(label_path,img_path).replace('_Bbox_GT.json','.png')
            # shutil.copy(sample_img_path,os.path.join(save_path,'img')

# file 저장
def save_json_file(json_file,path,filename):
    os.makedirs(path,exist_ok=True)
    with open(os.path.join(path,filename+'_Bbox_GT.json'), 'w',encoding='utf-8') as f:
        json.dump(json_file, f,ensure_ascii=False, indent=4) 

# file 이동
def move_file(src_file_path:str,save_path:str,is_copy:bool=True,preserve_dir_depth:str = 'batch') -> None:
    '''
    지정된 파일경로(src_file_path)의 파일을 지정된 저장경로(save_path)에 이동(복사/이동)
    
    Args:
        src_file_path (str): 이동시킬 파일의 경로 -> 파일 이름 포함
        file_name (str): 이동시킬 파일 이름
        save_path (str): 저장할 디렉토리 경로
        is_copy (bool, optional): 복사 여부 , True면 copy , False면 move
        preserve_dir_depth (str, optional): 파일을 이동할때 디렉토리의 구조를 보존 -> default batch 깊이 이하  
    
    Returns:
        None
    '''
    if os.path.isfile(src_file_path):
        try :
            # 저장 디렉토리 생성
            save_dir = os.path.dirname(src_file_path.replace(src_file_path[:src_file_path.index(f'{preserve_dir_depth}')-1],save_path))
            os.makedirs(save_dir,exist_ok=True)

            #  파일 이동
            save_path_file = os.path.join(save_dir,os.path.basename(src_file_path))
            if is_copy:
                shutil.copy(src_file_path,save_path_file)
            elif not is_copy:
                shutil.move(src_file_path,save_path_file)       
        except :
            error_cases.setdefault('file_move_error: preserve depth ', []).append(src_file_path)

    elif not os.path.isfile(src_file_path):
        error_cases.setdefault('file_move_error : is not file', []).append(src_file_path)
        
        
# 빈 dictectori 삭제
def remove_empty_directories(move_path):
    for root, dirs, files in os.walk(move_path, topdown=False):
        for one_dir in dirs:
            path = os.path.join(root, one_dir)
            if not os.listdir(path):
                os.rmdir(path)