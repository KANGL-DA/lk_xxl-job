# -*-coding:utf8-*-
"""
    开发人员：刘康
    开发时间：2021-09-27
    开发功能：
        1.调用程序进行入库
"""
# 引入模块
import os
import sys
import time
import datetime
import psycopg2
import pandas as pd
from numpy import dtype
from psycopg2 import extras as ex
from loguru import logger
# 配置信息
starttime = datetime.datetime.now()
# 程序代码
class Put_info():
    def __init__(self):
        '''初始化'''
        # self.ip = 'localhost'
        self.ip = '20.66.161.21'
        self.port = '5432'
        self.database = 'postgres' 
        self.user = 'postgres'
        # self.passwd = '2024Psql@*'
        self.passwd = 'pgl@2025'
        self.conn_()
    
    def conn_(self):
        '''登录pgsql数据库'''
        try:
            self.conn = psycopg2.connect(database=self.database, user=self.user, password=self.passwd, host=self.ip, port=self.port)
            logger.info("connect pgsql:%s success..."%self.ip)
            self.db = self.conn.cursor()
        except Exception as error:
            logger.error("connect pgsql:%s error"%self.ip)
            logger.error(error)

    def commit(self):
        '''提交事务'''
        self.conn.commit()
    
    def dis_conn(self):
        '''关闭连接'''
        if self.conn:
            self.db.close()
            self.conn.close()
            logger.info("disconnect pgsql:%s bye..."%self.ip)
        endtime = datetime.datetime.now()
        logger.info('任务完成时间: %s秒'%(endtime - starttime).seconds)

    def put(self, data_info, table_name):
        '''主程序'''
        # try:
        df = pd.DataFrame(data_info, dtype=object)
        # except:
        # df = pd.DataFrame(data_info, dtype=str)
        df = df.where(df.notnull(), None)
        len_df = len(df)
        res_info = df.apply(lambda x: tuple(x), axis=1).values.tolist()
        logger.info(f'上传数据量: {len_df}')
        ex.execute_values(self.db, f'insert into {table_name} values %s', res_info, page_size=100000)
        self.commit()

    def __del__(self):
        '''退出程序操作'''
        self.dis_conn()
