import pandas as pd
from glob import glob
import conn_db
from helper import timer

bok_stat_df = conn_db.from_('Master_bok', 'bok_stat_master')

def map_df_merge(df, map_df, colname):
    '''
    전체 data가 있는 df와 mapping 전용 df와 mapping하는 함수
    유의할점: map_df에 코드+내용 이외에 공란이 있으면 안됨
    '''
    map_df = map_df.drop_duplicates(subset=colname)  # 중복 삭제
    temp = map_df.merge(df, how='inner', left_on=colname, right_on='key')  # 조인 
    return temp.drop(columns='key')

def del_code_str(df, col_count):
    for col in df.columns.tolist()[:col_count]:
        df[col] = df[col].str.split(" ", expand=True)[1]
    return df

def bok_mapper(stat_nm,): # bok mapping table 불러오기
	return conn_db.from_('bok_mapping_table', stat_nm)

def get_stat_nm(subject_name): # subject_name별 stat name 불러오기
    filt = bok_stat_df['subject_name'] == subject_name
    return bok_stat_df.loc[filt,'stat_nm'].tolist()

def get_bok_path(subject_name): # subject_name 경로 불러오기
    filt = bok_stat_df['subject_name'] == subject_name
    return bok_stat_df.loc[filt,'path'].unique().tolist()[0]

def clean_bok_data(subject_name): # bok data 전처리
    folder = get_bok_path(subject_name)
    name_list = get_stat_nm(subject_name)

    if subject_name=='무역지수':
        df_all = pd.DataFrame()
        for name in name_list:
            map_df = bok_mapper(name)
            map_1 = map_df[map_df['중분류'] == '소계'].copy()
            map_2 = map_df[(map_df['중분류'] != '소계') & (map_df['소분류'] == '소계')].copy()
            map_3 = map_df[(map_df['소분류'] != '소계') & (map_df['품목군'] == '소계')].copy()
            map_4 = map_df[(map_df['품목군'] != '소계') & (map_df['품목'] == '소계')].copy()
            map_5 = map_df[map_df['품목'] != '소계'].copy()

            all_files = glob(folder + f"{name}_*.csv")
            cols = ['ITEM_CODE1', 'ITEM_NAME1', 'DATA_VALUE', 'TIME']
            data_type = {'DATA_VALUE': 'float32', 'TIME': 'str'}
            temp_df = pd.concat([pd.read_csv(file, encoding='euc-kr',
                                            usecols=cols, dtype=data_type) for file in all_files])

            temp_df['key'] = temp_df['ITEM_CODE1'] + " " + temp_df['ITEM_NAME1']

            cols = ['ITEM_CODE1', 'ITEM_NAME1']
            temp_df.drop(columns=cols, inplace=True)
            temp_df = temp_df.dropna(subset=['DATA_VALUE']).drop_duplicates()

            df = pd.concat([map_df_merge(temp_df, map_1, '대분류'),
                            map_df_merge(temp_df, map_2, '중분류'),
                            map_df_merge(temp_df, map_3, '소분류'),
                            map_df_merge(temp_df, map_4, '품목군'),
                            map_df_merge(temp_df, map_5, '품목')], axis=0)

            # 5번째 컬럼까지가 Level임. 코드부분 삭제하기 위해서 split후에 뒷부분만 가져오기
            df = del_code_str(df, 5)

            # split하고 나면 문자만 있던 '소계'는 null이 됨. 그래서 다시 만들어 줌
            df.fillna('소계', inplace=True)
            df.rename(columns={'DATA_VALUE':name, 'TIME':'날짜'}, inplace=True)
            df_all = df_all.append(df)

        index_cols = ['대분류','중분류','소분류','품목군','품목','날짜']
        df = df_all.groupby(index_cols).agg('mean').reset_index()
        df.columns.name=None
        subject_name = '수출입물량+금액지수'
    #--------------------------------------------------------------------
    elif subject_name=='통화금융지표':
        all_files = glob(folder + "*.csv")  # 가져온 data 취합
        df = pd.concat([pd.read_csv(file, encoding='euc-kr', dtype='str')
                        for file in all_files], ignore_index=True)
        df.rename(columns={'UNIT_NAME': '단위',
                           'DATA_VALUE': '값',
                           'TIME': '날짜'}, inplace=True)
        df['STAT_NAME'] = df['STAT_NAME'].str.split(" ", 1).str[1] # 통계명에서 숫자 부분 제거

        cols = ['ITEM_CODE1', 'ITEM_CODE2', 'ITEM_CODE3',
                'ITEM_NAME2', 'ITEM_NAME3', 'STAT_CODE']
        df.drop(columns=cols, inplace=True)

        cols = ['STAT_NAME','ITEM_NAME1']
        temp = df[cols].drop_duplicates()
        conn_db.to_(temp, 'bok_mapping_table', '통화금융지표_import')

        df['key'] = df['STAT_NAME'] + df['ITEM_NAME1']
        df.drop(columns=cols, inplace=True)

        map_df = bok_mapper(subject_name).drop(columns=cols)
        df = df.merge(map_df, on='key').drop(columns='key') 
    #--------------------------------------------------------------------
    elif subject_name == '수출입물가지수':
        df_all = pd.DataFrame()
        for name in name_list:
            all_files = glob(folder + f"{name}*.csv")

            cols = ['ITEM_CODE1', 'ITEM_NAME1', 'ITEM_NAME2', 'DATA_VALUE', 'TIME']
            data_type = {'DATA_VALUE': 'float32', 'TIME': 'str'}
            df = pd.concat([pd.read_csv(file, encoding='euc-kr',
                                        usecols=cols,
                                        dtype=data_type) for file in all_files])

            df['key'] = df['ITEM_CODE1'] + " " + df['ITEM_NAME1']
            cols = ['ITEM_CODE1','ITEM_NAME1']
            df.drop(columns=cols, inplace=True)
            df = df.dropna(subset=['DATA_VALUE']).drop_duplicates()

            map_df = bok_mapper(name)

            if '특수분류' in name:
                map_1 = map_df[map_df['중분류'] == '소계'].copy()
                map_2 = map_df[map_df['중분류'] != '소계'].copy()
                df = pd.concat([map_df_merge(df, map_1, '대분류'),
                                map_df_merge(df, map_2, '중분류')], axis=0)
                df = del_code_str(df,2)
            #---------------------------------------------------------------

            elif '품목별' in name:
                map_1 = map_df[map_df['중분류'] == '소계'].copy()
                map_2 = map_df[(map_df['중분류'] != '소계') & (map_df['품목'] == '소계')].copy()
                map_3 = map_df[map_df['품목'] != '소계'].copy()
                df = pd.concat([map_df_merge(df, map_1, '대분류'),
                                map_df_merge(df, map_2, '중분류'),
                                map_df_merge(df, map_3, '품목')], axis=0)
                df = del_code_str(df,3)
            #---------------------------------------------------------------

            elif '용도별' in name:
                map_1 = map_df[map_df['중분류'] == '소계'].copy()
                map_2 = map_df[(map_df['중분류'] != '소계') & (map_df['소분류'] == '소계')].copy()
                map_3 = map_df[(map_df['소분류'] != '소계') & (map_df['품목군'] == '소계')].copy()
                map_4 = map_df[map_df['품목군'] != '소계'].copy()
                df = pd.concat([map_df_merge(df, map_1, '대분류'),
                                map_df_merge(df, map_2, '중분류'),
                                map_df_merge(df, map_3, '소분류'),
                                map_df_merge(df, map_4, '품목군')], axis=0)
                df = del_code_str(df,4)
            #---------------------------------------------------------------

            else: # 기본분류
                map_1 = map_df[map_df['중분류'] == '소계'].copy()
                map_2 = map_df[(map_df['중분류'] != '소계') & (map_df['소분류'] == '소계')].copy()
                map_3 = map_df[(map_df['소분류'] != '소계') & (map_df['품목군'] == '소계')].copy()
                map_4 = map_df[(map_df['품목군'] != '소계') & (map_df['품목'] == '소계')].copy()
                map_5 = map_df[map_df['품목'] != '소계'].copy()
                df = pd.concat([map_df_merge(df, map_1, '대분류'),
                                map_df_merge(df, map_2, '중분류'),
                                map_df_merge(df, map_3, '소분류'),
                                map_df_merge(df, map_4, '품목군'),
                                map_df_merge(df, map_5, '품목')], axis=0)
                df = del_code_str(df,5)
            #---------------------------------------------------------------

            # split하고 나면 문자만 있던 '소계'는 null이 됨. 그래서 다시 만들어 줌
            df.fillna('소계', inplace=True)
            df = df.reset_index(drop=True).rename(columns={'ITEM_NAME2':'통화기준','TIME': '날짜'})

            nm = '수출' if '수출' in name else '수입'
            df.rename(columns={'DATA_VALUE': f'{nm}물가지수'}, inplace=True)

            df['주제'] = name
            df_all = df_all.append(df)

        # 주제 통일
        for x in ['품목','특수','기본','용도']:
            df_all.loc[df_all['주제'].str.contains(x), '주제'] = f'{x}분류'

        # 같은 주제별로 groupby
        df = pd.DataFrame()
        for topic in df_all['주제'].unique():
            temp_df = df_all.loc[df_all['주제'] == topic].dropna(axis=1, how='all')

            # '지수'이외의 컬럼을 index_col으로 사용
            cols = [col for col in temp_df.columns.tolist() if '지수' not in col]
            temp_df = temp_df.melt(id_vars=cols,
                                    var_name='구분',value_name='값').dropna()

            # 명칭 합치기
            temp_df['구분'] = temp_df['구분']+' (' + temp_df['통화기준']+')'
            temp_df.drop(columns='통화기준', inplace=True)

            cols = [col for col in temp_df.columns.tolist() if '값' not in col and '구분' not in col]
            temp_df = temp_df.pivot_table(index=cols,
                                        columns='구분', values='값').reset_index()
            temp_df.columns.name = None

            df = df.append(temp_df, ignore_index=True)
        cols = ['주제','대분류','중분류','소분류','품목군','품목','날짜',
                '수입물가지수 (계약통화기준)','수입물가지수 (달러기준)','수입물가지수 (원화기준)',
                '수출물가지수 (계약통화기준)','수출물가지수 (달러기준)','수출물가지수 (원화기준)',]
        df = df[cols]
    #--------------------------------------------------------------------
    elif subject_name =='기업경기실사지수':
        df = pd.DataFrame()
        for name in name_list:
            all_files = glob(folder + f"{name}_*.csv")
            data_type = {'DATA_VALUE': 'float32', 'TIME': 'str'}
            temp_df = pd.concat([pd.read_csv(file, encoding='euc-kr',
                                            dtype=data_type) for file in all_files])

            temp_df = temp_df.drop_duplicates().rename(columns={'TIME': '날짜'})
            cols = ['UNIT_NAME','STAT_CODE', 'ITEM_CODE3',
                    'ITEM_NAME3','STAT_NAME','ITEM_CODE2']
            temp_df.drop(columns=cols, inplace=True)

            temp_df.insert(0, '주제', name)
            temp_df['ITEM_NAME1'] = temp_df['ITEM_NAME1'].str.strip()

            if name in ['전국실적','전국전망','업종별 전국실적','업종별 전국전망']:
                temp_df['ITEM_NAME1'] = temp_df['ITEM_NAME1'].str.replace(' ','')
                temp_df['key'] = temp_df['ITEM_CODE1'] + " " + temp_df['ITEM_NAME1']

                temp_df.drop(columns=['ITEM_CODE1', 'ITEM_NAME1'], inplace=True)
                map_df = bok_mapper(name)

                map_1 = map_df[map_df['업종별(2)']=='소계'].copy()
                map_2 = map_df[(map_df['업종별(3)']=='소계') & (map_df['업종별(2)']!='소계')].copy()
                map_3 = map_df[map_df['업종별(3)']!='소계'].copy()
                del map_df
                df_all = pd.concat([map_df_merge(temp_df, map_1, '업종별(1)'),
                                    map_df_merge(temp_df, map_2, '업종별(2)'),
                                    map_df_merge(temp_df, map_3, '업종별(3)')], axis=0)
                df_all = del_code_str(df_all,3)

                df_all.fillna('소계', inplace=True)
                df_all.rename(columns={'TIME': '날짜'}, inplace=True)

                nm = '실적' if '실적' in name else '전망'
                df_all.insert(0,'컬럼', nm)
                df = df.append(df_all, ignore_index=True)

            else:
                temp_df.drop(columns=['ITEM_CODE1'], inplace=True)
                nm = '실적' if '실적' in name else '전망'
                temp_df.insert(0,'컬럼', nm)
                df = df.append(temp_df, ignore_index=True)

        subject = '업종별 전국'
        filt = df['주제'].str.contains(subject)
        df_industry_all = df[filt].copy()
        df_industry_all['주제'] = subject

        for x in df_industry_all['컬럼'].unique():
            df_industry_all['ITEM_NAME2'] = df_industry_all['ITEM_NAME2'].str.replace(x,'').str.strip()

        df_industry_all['업종별(2)'] = df_industry_all['업종별(2)'].str.replace('소계','전산업')
        df_industry_all.drop(columns=['업종별(1)','ITEM_NAME1'], inplace=True)
        names = {'업종별(2)':'대분류',
                '업종별(3)':'중분류',
                'ITEM_NAME2':'항목'}
        df_industry_all.rename(columns=names, inplace=True)

        #-----------
        subject = '업종별 지역'
        filt = df['주제'].str.contains(subject)
        df_industry_region = df[filt].copy()
        df_industry_region['주제'] = subject

        df_industry_region[['대분류','항목']] = df_industry_region['ITEM_NAME1'].str.split(' ',1,expand=True)
        df_industry_region = df_industry_region.drop(columns='ITEM_NAME1').dropna(axis=1, how='all')
        df_industry_region.rename(columns={'ITEM_NAME2':'시도'}, inplace=True)

        #-----------
        subject = '매출액가중'
        filt = df['주제'].str.contains(subject)
        df_sales = df[filt].copy()
        df_sales['주제'] = subject

        for x in df_sales['컬럼'].unique():
            df_sales['ITEM_NAME2'] = df_sales['ITEM_NAME2'].str.replace(x,'').str.strip()

        df_sales.dropna(axis=1, how='all', inplace=True)
        names = {'ITEM_NAME1':'대분류','ITEM_NAME2':'항목'}
        df_sales.rename(columns=names, inplace=True)

        #-----------
        subject = '전국실적/전망'
        filt = (df['주제'].str.contains('전국')) & (df['주제'].apply(len)==4)
        df_all = df[filt].copy()
        df_all['주제'] = subject

        for x in df_all['컬럼'].unique():
            df_all['ITEM_NAME2'] = df_all['ITEM_NAME2'].str.replace(x,'').str.strip()

        df_all['업종별(2)'] = df_all['업종별(2)'].str.replace('소계','전산업')
        df_all.drop(columns=['업종별(1)','ITEM_NAME1'], inplace=True)
        names = {'업종별(2)':'대분류',
                '업종별(3)':'중분류',
                'ITEM_NAME2':'항목'}
        df_all.rename(columns=names, inplace=True)

        #-----------
        # 합쳐서 저장
        df = pd.concat([df_industry_all, df_industry_region,df_sales,df_all], axis=0)
        df['시도'].fillna('전국', inplace=True)
        df['중분류'].fillna('소계', inplace=True)

        cols = ['주제','대분류','중분류','항목','날짜','시도']
        df = df.pivot_table(index=cols, columns='컬럼', values='DATA_VALUE').reset_index()
        df.columns.name = None
    #--------------------------------------------------------------------
    elif subject_name == '소비유형별개인신용카드':
        all_files = glob(folder + "*.csv")
        data_type = {'DATA_VALUE': 'float32', 'TIME': 'str'}
        df = pd.concat([pd.read_csv(file, encoding='euc-kr', dtype=data_type) for file in all_files])

        cols = ['UNIT_NAME','ITEM_CODE1','STAT_CODE',
                'ITEM_CODE2','ITEM_CODE3','STAT_NAME']
        df.drop(columns =cols, inplace=True)

        cols = ['ITEM_NAME1','ITEM_NAME2','TIME']
        df = df.pivot_table(index=cols, columns='ITEM_NAME3', values='DATA_VALUE').reset_index()
        df.columns.name=None

        names = {'ITEM_NAME1':'시도',
                'ITEM_NAME2':'소비유형',
                'TIME':'날짜',
                '월간 일평균':'월간 일평균 (백만원)',
                '총액':'총액 (백만원)'}
        df.rename(columns=names, inplace=True)

        map_df = bok_mapper('소비유형별포괄범위')
        # 대분류만 있는 map_df 따로 만들기
        temp_col = ['소비유형','세부 내용']
        map_df_1 = map_df.drop(columns=temp_col).drop_duplicates()

        # join한 다음에 다시 합치기
        df_1 = df.merge(map_df_1, left_on='소비유형', right_on='대분류', how='inner')
        for col in temp_col:
            df_1[col] = '소계'

        df_all = df.merge(map_df, on='소비유형', how='inner')

        cols = ['시도','대분류','소비유형','세부 내용','날짜']
        df = pd.concat([df_all, df_1])
        df = df.groupby(cols).agg('sum').reset_index()
    #--------------------------------------------------------------------
    elif subject_name=='거시경제분석지표':

        all_files = glob(folder + "*.csv")
        data_type = {'DATA_VALUE': 'float32', 'TIME': 'str'}
        df = pd.concat([pd.read_csv(file, encoding='euc-kr',
                                    dtype=data_type) for file in all_files])

        cols = ['STAT_NAME','ITEM_CODE1','STAT_CODE','ITEM_CODE2',
                'ITEM_CODE3','ITEM_NAME2','ITEM_NAME3']
        df.drop(columns=cols, inplace=True)
        filt = df['UNIT_NAME'].str.contains('%')
        df.loc[filt,'DATA_VALUE'] = df.loc[filt,'DATA_VALUE']/100

        names = {'UNIT_NAME':'단위',
                'ITEM_NAME1':'지표',
                'TIME':'날짜',
                'DATA_VALUE':'값'}
        df.rename(columns=names, inplace=True)
        df = df.merge(bok_mapper(subject_name), on='지표')
    #--------------------------------------------------------------------
    elif subject_name=='은행대출금연체율':
        all_files = glob(folder + "*.csv")
        data_type = {'DATA_VALUE': 'float32', 'TIME': 'str'}
        df = pd.concat([pd.read_csv(file, encoding='euc-kr',
                                    dtype=data_type) for file in all_files])
        cols = ['STAT_NAME','ITEM_CODE1','STAT_CODE','ITEM_CODE2',
                'ITEM_CODE3','UNIT_NAME','ITEM_NAME3']
        df.drop(columns=cols, inplace=True)

        df['DATA_VALUE'] = df['DATA_VALUE']/100

        names = {'ITEM_NAME1':'대출구분',
                'ITEM_NAME2':'은행구분',
                'TIME':'날짜',
                'DATA_VALUE':'연체율'}
        df = df.rename(columns=names).dropna()
    #--------------------------------------------------------------------
    elif subject_name=='교역조건지수':
        all_files = glob(folder + f"{subject_name}_*.csv")
        data_type = {'DATA_VALUE': 'float32', 'TIME': 'str'}
        df = pd.concat([pd.read_csv(file, encoding='euc-kr',
                                    dtype=data_type) for file in all_files])

        cols = ['STAT_NAME', 'ITEM_CODE1', 'STAT_CODE', 'ITEM_CODE2',
                'ITEM_CODE3', 'UNIT_NAME', 'ITEM_NAME2', 'ITEM_NAME3']
        df.drop(columns=cols, inplace=True)

        df.rename(columns={'TIME': '날짜'}, inplace=True)
        df = df.pivot_table(index='날짜', columns='ITEM_NAME1', values='DATA_VALUE').reset_index()
        df.columns.name = None
    #--------------------------------------------------------------------
    elif subject_name == '가계신용(분기별)':
        all_files = glob(folder + f"{subject_name}_*.csv")[0]
        df = pd.read_csv(all_files, encoding='cp949')

        cols = ['ITEM_CODE1','DATA_VALUE','TIME']
        df = df.loc[:,cols].copy()

        df['TIME'] = df['TIME'].astype(str)
        quarter_dict ={'1':'-03-31',
                       '2':'-06-30',
                       '3':'-09-30',
                       '4':'-12-31'}
        df['TIME'] = df['TIME'].str[:4] + df['TIME'].str[-1:].map(quarter_dict)

        map_df = bok_mapper(subject_name)
        map_df.drop(columns='금액', inplace=True)
        map_df = map_df.loc[map_df['Dataset'].apply(len)>0].copy()

        cols = ['계정항목코드별(1)','계정항목코드별(2)','계정항목코드별(3)',
                '계정항목코드별(4)','계정항목코드별(5)']

        for col in cols:
            map_df[col] = map_df[col].str.split(' ',expand=True)[0]

        df_all = pd.DataFrame()
        for i in range(5):
            if i!=4:
                filt1 = map_df[cols[i]]!='소계'
                filt2 = map_df[cols[i+1]]=='소계'
                filt = filt1&filt2
            else:
                filt = map_df[cols[i]]!='소계'
            temp = map_df[filt].copy()
            temp = temp.merge(df, left_on=cols[i], right_on='ITEM_CODE1')
            df_all = df_all.append(temp, ignore_index=True)

        cols.append('ITEM_CODE1')
        df_all.drop(columns=cols, inplace=True)

        df_all['억원'] = df_all['DATA_VALUE']*10
        df_all['조원'] = df_all['DATA_VALUE']/1000

        df = df_all.rename(columns={'TIME':'날짜'})
        df.drop(columns='DATA_VALUE', inplace=True)
    #--------------------------------------------------------------------
    elif subject_name == '총산출물가지수':
        all_files = glob(folder + "총산출물가지수_*.csv")
        df = pd.concat([pd.read_csv(file, encoding='euc-kr',
                                    dtype={'DATA_VALUE': 'float32', 'TIME': 'str'}) for file in all_files])
        cols = ['ITEM_CODE1','DATA_VALUE','TIME']
        df = df[cols].reset_index(drop=True)
        df['ITEM_CODE1'] = df['ITEM_CODE1'].str.replace('*','')
        df.rename(columns={'TIME': '날짜'}, inplace=True)

        map_df = bok_mapper('총산출물가지수_code')
        map_df.columns = [x.split(' ')[1] for x in map_df.columns.tolist()]

        for col in map_df.columns.tolist():
            map_df[col] = map_df[col].str.replace('13102874266ACNT_CODE._','')
            map_df[col] = map_df[col].str.replace('13102874266ACNT_CODE.','')
            name_col = col.replace('코드별','')
            map_df[[col,name_col]] = map_df[col].str.split(' ',expand=True)
            map_df[name_col].fillna('소계', inplace=True)

        map_1 = map_df.loc[map_df['계정항목코드별(2)']=='소계'].copy()
        cols = ['계정항목코드별(2)','계정항목코드별(3)','계정항목코드별(4)']
        map_1.drop(columns=cols, inplace=True)
        map_1.rename(columns={'계정항목코드별(1)':'ITEM_CODE1'},inplace=True)

        #--------------------
        filt1 = map_df['계정항목코드별(3)']=='소계'
        filt2 = map_df['계정항목코드별(2)']!='소계'
        map_2 = map_df.loc[filt1&filt2].copy()
        cols = ['계정항목코드별(1)','계정항목코드별(3)','계정항목코드별(4)']
        map_2.drop(columns=cols, inplace=True)
        map_2.rename(columns={'계정항목코드별(2)':'ITEM_CODE1'},inplace=True)

        #--------------------
        filt1 = map_df['계정항목코드별(4)']=='소계'
        filt2 = map_df['계정항목코드별(3)'] != '소계'
        map_3 = map_df.loc[filt1&filt2].copy()
        cols = ['계정항목코드별(1)','계정항목코드별(2)','계정항목코드별(4)']
        map_3.drop(columns=cols, inplace=True)
        map_3.rename(columns={'계정항목코드별(3)':'ITEM_CODE1'},inplace=True)

        #--------------------
        filt = map_df['계정항목코드별(4)'] != '소계'
        map_4 = map_df.loc[filt].copy()
        cols = ['계정항목코드별(1)','계정항목코드별(2)','계정항목코드별(3)']
        map_4.drop(columns=cols, inplace=True)
        map_4.rename(columns={'계정항목코드별(4)':'ITEM_CODE1'},inplace=True)

        #--------------------
        df_all = pd.DataFrame()
        maps = [map_1, map_2, map_3, map_4]
        for map_df in maps:
            temp = df.merge(map_df, on='ITEM_CODE1', how='inner')
            temp.drop(columns='ITEM_CODE1', inplace=True)
            df_all = df_all.append(temp)
        df_all = df_all.rename(columns={'DATA_VALUE':'총산출물가지수'}).reset_index(drop=True)
        del df, map_df, map_1, map_2, map_3, map_4, filt1, filt2, filt

        #--------------------
        # 처음 작업할때 계층만들기 위해 사용.(다시 업로드시 초기화)
        # cols = ['계정항목(1)','계정항목(2)','계정항목(3)','계정항목(4)']
        # temp = df_all[cols].drop_duplicates().sort_values(by=cols)
        # conn_db.to_(temp,'한국은행_전처리','총산출물가지수')

        cols = ['key','대분류','중분류','구분']
        map_df = bok_mapper('총산출물가지수')[cols]
        map_df = map_df.loc[map_df['중분류']!='삭제'].copy()

        # 전체, 국내, 수출로 총산출물가지수 분류
        df_all['key'] = df_all['계정항목(1)']+df_all['계정항목(2)']+df_all['계정항목(3)']+df_all['계정항목(4)']
        df_all = df_all.merge(map_df, on='key', how='inner').reset_index(drop=True)

        cols = ['key','계정항목(1)','계정항목(2)','계정항목(3)','계정항목(4)']
        df_all.drop(columns=cols, inplace=True)

        cols = ['날짜','대분류','중분류']
        df = df_all.pivot_table(index=cols, columns='구분', values='총산출물가지수').reset_index()
        df.columns.name=None
    #--------------------------------------------------------------------
    # 저장
    conn_db.export_(df, subject_name)
    print(f'{subject_name} 완료')

def union_trade_dfs():
    # import df
    df_price_level = conn_db.import_('수출입물가지수')
    df_vol_price = conn_db.import_('수출입물량+금액지수')

    df_vol_price['주제']='기본분류'
    filt = df_vol_price['품목']!='소계'
    df_vol_price = df_vol_price[filt].copy().reset_index(drop=True)

    # 용도분류 정리
    filt1 = df_price_level['품목군']!='소계'
    filt2 = df_price_level['주제']=='용도분류'
    filt = filt1 & filt2
    df_1 = df_price_level[filt].reset_index(drop=True)

    cols = ['중분류', '소분류', '품목군']
    for col in cols:
        for x in ['내구재','비내구재','소비재','원재료','자본재','중간재','최종재']:
            df_1[col] = df_1[col].str.replace(x,'')

    # 기본분류 정리
    filt1 = df_price_level['품목']!='소계'
    filt2 = df_price_level['주제']=='기본분류'
    filt = filt1&filt2
    df_2 = df_price_level[filt].reset_index(drop=True)

    # 특수분류, 품목분류는 그대로 사용
    filt1 = df_price_level['주제']=='품목분류'
    filt2 = df_price_level['주제']=='특수분류'
    filt = filt1|filt2
    df_3 = df_price_level[filt].reset_index(drop=True)
    df_price_level = pd.concat([df_1, df_2, df_3], ignore_index=True)

    #--------------------
    cols = [col for col in df_price_level.columns.tolist() if '지수' not in col]
    df = df_price_level.merge(df_vol_price, on=cols, how='outer')
    conn_db.export_(df,'취합본_수출입_물량+금액+물가지수')

@helper.timer
def normalize_bok():
    subjects = bok_stat_df['subject_name'].unique().tolist()
    normalize = [clean_bok_data(subject) for subject in subjects]
    union_trade_dfs()
