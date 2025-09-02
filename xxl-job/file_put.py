# -*-coding:utf8-*-
"""
    开发人员：刘康
    开发时间：2022-08-22
    开发功能：
    df.insert(loc=0, column='acct_date', value=file_data)  添加字段
"""
# 引入模块
import os
import sys
import time
import datetime
import shutil
import psycopg2
import pinyin
import pandas as pd
from dateutil.relativedelta import relativedelta
from loguru import logger
from put_pg import Put_info
# 配置信息
starttime = datetime.datetime.now()
time_day = datetime.datetime.now().strftime("%Y%m%d")
time_1day = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y%m%d")
time_mon = datetime.datetime.now().strftime("%Y%m")
time_1mon = (datetime.datetime.now() - relativedelta(months=+1)).strftime("%Y%m")
default_path = r"/home/gladmin/python/DataBase/file_export/pgsql"
logger.add(r'/home/gladmin/python/DataBase/file_export/pgsql/执行记录.log', encoding='utf-8', level="DEBUG")
class Main():
    def __init__(self, ip, port, database, user, passwd):
        '''初始化'''
        self.ip = ip
        self.port = port
        self.database = database 
        self.user = user
        self.passwd = passwd

    def conn_(self):
        '''登录pgsql数据库'''
        try:
            self.conn = psycopg2.connect(database=self.database, user=self.user, password=self.passwd, host=self.ip, port=self.port)
            logger.info("connect pgsql:%s success..."%self.ip)
            self.db = self.conn.cursor()
        except Exception as error:
            logger.error("connect pgsql:%s error"%self.ip)
            logger.error(error)
    
    def dis_conn(self):
        '''关闭连接'''
        if self.conn:
            self.db.close()
            self.conn.close()
            logger.info("disconnect pgsql:%s bye..."%self.ip)
        endtime = datetime.datetime.now()
        logger.info('任务完成时间: %s秒'%(endtime - starttime).seconds)

    def split_file(self, file_path, file_name, split_col='', fgf=',', bczd=[]):
        '''
            模块名称: 切分文件
            使用说明: 
                1.文件路径  (以/结尾)
                2.文件名称
                3.分割列名称
                4.字段分隔符
                ,keep_default_na=False 空值替换
        '''
        if '.xl' in file_name:
            try:
                df = pd.read_excel(f'{file_path}{file_name}')
            except:
                df = pd.read_excel(f'{file_path}{file_name}', encoding='gbk')
        else:
            try:
                df = pd.read_csv(f'{file_path}{file_name}', sep=fgf, dtype=object)
            except:
                df = pd.read_csv(f'{file_path}{file_name}', sep=fgf, encoding='gbk', dtype=object)
        logger.info(f'{file_path}{file_name}')
        print(df.head())
        logger.info(list(df.columns.values))
        new_file_name = file_name.split('.')[0]
        if split_col == '':
            split_col = input("输入分裂字段列名: ")
        groups = df.groupby(df[split_col])
        for group in groups:
            new_df = groups.get_group(group[0])
            logger.info(group[0])
            len_group = len(new_df)
            all_new_file = f'{file_path}/{new_file_name}_{group[0]}_{len_group}.csv'
            logger.info(f"{all_new_file}: {len_group}")
            if bczd:
                new_df.to_csv(all_new_file, columns=bczd, index=False, sep=',', encoding="utf8")
            else:
                new_df.to_csv(all_new_file, index=False, sep=',', encoding="utf8")

    def run_proc(self, sta_date, end_date, proc_name, proc_type):
        '''
            模块名称: 生成存储过程开始到结束时间的执行语句
            使用说明:
                1.开始时间
                2.结束时间
                3.存储过程名称
                4.存储过程类型 (d/m d:日 m:月)
        '''
        if proc_type == 'd':
            sta_time = datetime.datetime.strptime(sta_date, "%Y%m%d")
            end_time = datetime.datetime.strptime(end_date, "%Y%m%d")
            while sta_time <= end_time:
                date_str = sta_time.strftime("%Y%m%d")
                pnt_info = f"select {proc_name}('{date_str}');"
                print(pnt_info)
                sta_time += datetime.timedelta(days=1)
        elif proc_type == 'm':
            sta_time = datetime.datetime.strptime(sta_date, "%Y%m")
            end_time = datetime.datetime.strptime(end_date, "%Y%m")
            while sta_time <= end_time:
                date_str = sta_time.strftime("%Y%m")
                pnt_info = f"select {proc_name}('{date_str}');"
                print(pnt_info)
                sta_time += relativedelta(months=+1)
        else:
            logger.error("请输出日或月(d/m)")
            sys.exit()

    def table_excel(self, sql, save_name, date_type):
        '''
            模块名称: 在pg库运行sql,并保存结果数据到excel
            使用方式:
                1.sql  (可换行)
                2.保存文件名称
                na_rep='' 空值替换
        '''
        self.conn_()
        sql = f"""
{sql}"""
        logger.info(sql)
        df = pd.read_sql(sql, self.conn)
        print(df.head())
        len_df = len(df)
        if date_type == 'd':
            file_name = f'{default_path}{save_name}_{time_day}.xlsx'
        elif date_type == 'd1':
            file_name = f'{default_path}{save_name}_{time_1day}.xlsx'
        elif date_type == 'm':
            file_name = f'{default_path}{save_name}_{time_mon}.xlsx'
        elif date_type == 'm1':
            file_name = f'{default_path}{save_name}_{time_1mon}.xlsx'
        logger.info(f'{file_name}: {len_df}')
        df.to_excel(file_name, index=False, encoding='UTf8')
        self.dis_conn()

    def table_split_csv(self, sql, file_name, split_col, res_columns, col, file_type):
        '''
            模块作用: 在pg库运行sql,并保存结果数据到文件
            使用方式:
                1.sql  (可换行)
                2.分切字段
        '''
        self.conn_()
        logger.info('\n'+sql)
        df = pd.read_sql(sql, self.conn)
        len_df = len(df)
        print(df.head())
        file_mc = f'{default_path}{file_name}_汇总_{len_df}_{time_day}.{file_type}'
        logger.info(f"{file_mc}: {len_df}")
        df.to_csv(file_mc, index=False, header=None, columns=res_columns, sep=col, encoding="utf-8")
        groups = df.groupby(df[split_col])
        for group in groups:
            new_df = groups.get_group(group[0])
            len_group = len(new_df)
            new_file_mc = f'{default_path}{file_name}_{group[0]}_{len_group}_{time_day}.{file_type}'
            logger.info(f"{group[0]}: {new_file_mc}, 数据量: {len_group}")
            new_df.to_csv(new_file_mc, columns=res_columns, index=False, header=None, sep=col, encoding="utf-8")
        self.dis_conn()

    def table_pandas(self, sql, save_name, file_end, split_type, file_path = default_path):
        '''
            模块作用: 在pg库运行sql,并保存结果数据到文件  (带表头)
            使用方式:
                1.sql  (可换行)
                2.保存文件名称
                3.保存文件格式  (csv/txt)
                4.文件分割符
        '''
        self.conn_()
        sql = f"""
{sql}"""
        logger.info(sql)
        df = pd.read_sql(sql, self.conn)
        print(df.head())
        len_df = len(df)
        file_name = f'{file_path}{save_name}_{time_day}.{file_end}'
        logger.info(f"{file_name}: {len_df}")
        df.to_csv(file_name, index=False, sep=split_type, encoding="utf8")
        self.dis_conn()
    
    def table_expert(self, sql, save_name, file_end, split_type, file_path = default_path):
        '''
            模块作用: 在pg库运行sql,并保存结果数据到文件  (不带表头)
            使用方式:
                1.sql  (可换行)
                2.保存文件名称
                3.保存文件格式  (csv/txt)
                4.文件分割符
        '''
        self.conn_()
        sql = f"""
{sql}"""
        logger.info(sql)
        file_name = f'{file_path}{save_name}_{time_day}.{file_end}'
        self.db.copy_expert("COPY (%s) TO STDOUT DELIMITER '%s' CSV " % (sql,split_type), open(file_name, "w"))
        logger.info(file_name)
        self.dis_conn()

    def file_put(self, file_path, file_name, col_name, table_name, head_list = None):
        for ii in os.listdir(file_path):
            if file_name in ii and (ii.endswith('csv') or ii.endswith('txt') or ii.endswith('tsv')):
                if head_list == None or head_list == '':
                    try:
                        df = pd.read_csv(f'{file_path}{ii}', sep=col_name, low_memory=False, dtype=object)
                    except:
                        df = pd.read_csv(f'{file_path}{ii}', sep=col_name, low_memory=False, encoding='gbk', dtype=object)
                    print(list(df.columns.values))
                    col = ''
                    for i in list(df.columns.values):
                        col += "%s text,\n"%pinyin.get_initial(i,delimiter="").lower()
                        # print(pinyin.get_initial(i,delimiter="").upper()+'\t'+i)
                    print(col[:-2])
                else:
                    try:
                        df = pd.read_csv(f'{file_path}{ii}', sep=col_name, low_memory=False, names=head_list, dtype=object)
                    except:
                        df = pd.read_csv(f'{file_path}{ii}', sep=col_name, low_memory=False, names=head_list, encoding='gbk', dtype=object)
                print(df.head())
                len_df = len(df)
                if table_name == None or table_name == '':
                    table_name = input("请输入你要插入的表名:")
                logger.info(f'{file_name}, {table_name}, {len_df}')
                # df.insert(loc=0, column='acct_date', value='20221127')
                Put_info().put(df, table_name)

    def excel_to_file(self, file_path, file_name, file_end, table_name=''):
        '''
            模块作用: excel 保存成文件
            使用方式:
                1.路径位置  (以/结束)
                2.文件名称
                3.读取的sheet名称  (默认Sheet1)
                4.生成的文件格式(csv/txt)
        '''
        for name in os.listdir(file_path):
            if name.startswith(file_name):
                sheet_list = pd.read_excel(f'{file_path}{name}',sheet_name=None)
                if len(sheet_list.keys()) > 1:
                    for key_name in sheet_list.keys():
                        df = pd.read_excel(f'{file_path}{name}', sheet_name=key_name, keep_default_na=False, dtype=str)
                        len_df = str(len(df))
                        logger.info(f"{file_path}{name}:{key_name}|{len_df}")
                    sheet_name = input("请输入你要转换的sheet页数据:")
                    if sheet_name == None or sheet_name == '':
                        sheet_name = 'Sheet1'
                    # 20241121新增
                    elif sheet_name.lower() == 'all':
                        df = pd.DataFrame()
                        for sheet_name in sheet_list.keys():
                            df_sheet = pd.read_excel(f'{file_path}{name}', sheet_name=sheet_name, dtype=str)
                            df = pd.concat([df,df_sheet])
                    else:
                        df = pd.read_excel(f'{file_path}{name}', sheet_name=sheet_name, dtype=str)
                else:
                    df = pd.read_excel(f'{file_path}{name}', dtype=str)
                print(df.head())
                file = f"{name.split('.')[0]}.{file_end}".replace('-','_')
                print(list(df.columns.values))
                col = ''
                for i in list(df.columns.values):
                    col += "%s text,\n"%pinyin.get_initial(i,delimiter="").lower()
                    # print(pinyin.get_initial(i,delimiter="").upper()+'\t'+i)
                print(col[:-2])
                logger.info(f"{file_path}{file}")
                if table_name == '':
                    table_name = input("请输入插入的表名(否或f跳过):")
                if table_name == 'f' or table_name == '否' or table_name == None:
                    df.to_csv(f"{file_path}{file}", index=False, sep='^', encoding="utf8")
                else:
                    # df.insert(loc=0, column='acct_date', value='20221127')
                    Put_info().put(df, table_name)
