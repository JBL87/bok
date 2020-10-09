import pandas as pd
from glob import glob
import requests
import os
import conn_db
from helper import bok_api_key

api_key =  bok_api_key  

def get_stats_list():
    '''
    서비스 통계 목록 받기
    '''
    url_statList = "http://ecos.bok.or.kr/api/StatisticTableList/{}/json/kr/1/890/".format(api_key)
    response = requests.get(url_statList).json()["StatisticTableList"]["row"]
    df_statList = pd.DataFrame(response)
    
    df_statList.columns = ['검색가능여부','출처','주기','통계명','통계코드','부모통계코드']
    # 한국은행은 출처가 공란으로 되어 있어서 '한국은행'으로 값 채워주기
    df_statList['출처'].replace({'': '한국은행'}, inplace=True)

    # hierachy 때문에 검색불가능한 통계명도 서비스 통계 목록에 들어가 있는데, 검색가능한 것만 필터링
    rows = df_statList['검색가능여부'] == 'Y'
    cols = ['출처', '주기', '통계명', '통계코드'] # 필요한 컬럼만 선택
    statList_searchable = df_statList.loc[rows, cols]
    
    conn_db.to_(statList_searchable, 'Master_한국은행','통계_ID') 
    
def stat_check(stat_id):
    url = "http://ecos.bok.or.kr/api/StatisticItemList/{}/json/kr/1/1/{}".format(api_key, stat_id)
    response = requests.get(url).json()['StatisticItemList']['row']
    df = pd.DataFrame(response)
    print("데이터행수: " + str(df["DATA_CNT"].sum()))
    cols = ['START_TIME', 'END_TIME', 'STAT_NAME', 'CYCLE','DATA_CNT']
    row = [0]
    return df.loc[row, cols]
    
def get_stat_info(stat_id):
    url = f"http://ecos.bok.or.kr/api/StatisticItemList/{api_key}/json/kr/1/1/{stat_id}"
    response = requests.get(url).json()['StatisticItemList']['row']
    df = pd.DataFrame(response)
    cols = ['START_TIME','END_TIME','STAT_NAME','CYCLE','DATA_CNT']
    row = [0]
    df = df.loc[row, cols].copy()
    df['stat_id'] = stat_id
    return df

def update_stat_info():
    stat_id_list = conn_db.from_('Master_bok','get_stat_list')['stat_id']
    df = pd.concat([get_stat_info(stat_id) for stat_id in stat_id_list], ignore_index=True)
    conn_db.to_(df, 'Master_bok','stat_updated')
     
def get_data(param):
    stat_id, name, file_path, start_date, end_date, cycle = param
    url = f"http://ecos.bok.or.kr/api/StatisticSearch/{api_key}/json/kr/1/100000/{stat_id}/{cycle}/{start_date}/{end_date}"
    try:
        response = requests.get(url).json()['StatisticSearch']['row']
        df = pd.DataFrame(response)
        
        df['DATA_VALUE'] = pd.to_numeric(df['DATA_VALUE'])
        df.dropna(subset=['DATA_VALUE'], inplace=True)
        filename = file_path+f'{name}_{start_date}~{end_date}.csv'
        
        all_files = glob(file_path + f'{name}_*.csv')
        all_files.reverse()
        os.remove(all_files[0])

        df.to_csv(filename, encoding='euc-kr', index=False)
    except:
        print('업데이트 안됨. 기간확인 필요')
        pass

def update_bok(start_date):
    bok_stats = conn_db.from_('Master_bok', 'bok_stat_master')

    subject_names = bok_stats['subject_name'].unique()
    cols = ['stat_id', 'stat_nm', 'path', 'START_TIME', 'END_TIME', 'CYCLE']
    for subject_name in subject_names:
        temp = bok_stats.loc[bok_stats['subject_name']==subject_name, cols]
        temp.loc[temp['CYCLE']=='MM','START_TIME'] = start_date
        [get_data(param) for param in temp.values.tolist()]
        print(f'{subject_name} 가져오기 완료')

# def get_key_100_stats():
#     url_key100list = "http://ecos.bok.or.kr/api/KeyStatisticList/Q3KZX3XXQ0Y0RQJ1CQ9L/json/kr/1/1000/"
#     response = requests.get(url_key100list)
#     key100list = response.json()['KeyStatisticList']["row"]
#     key100list = pd.DataFrame(key100list)
#     return key100list
