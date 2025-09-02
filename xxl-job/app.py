import asyncio
from fastapi import FastAPI

from pyxxl import JobHandler
from pyxxl.ctx import g
from file_put import Main
import ast

import requests
import json
import os

app = FastAPI()
xxl_handler = JobHandler()

@app.get("/")
async def root():
    return {"message": "Hello World"}


@xxl_handler.register(name="demoJobHandler")
async def test_task():
    g.logger.info('正在入库...')
    params = ast.literal_eval(g.xxl_run_data.executorParams)
    g.logger.info(f'get executor params：{params}')
    start_put = Main('20.66.161.21','5432','postgres','postgres','pgl@2025')
    start_put.excel_to_file("/home/xatztmp/file_list/", params['qx'], "csv", "tmp.tmp_cpqx_details_d")
    start_put.excel_to_file("/home/xatztmp/file_list/", params['gd'], "csv", "tmp.tmp_gd_details_d")
    g.logger.info('入库成功，已完成')
    return "成功10"

@xxl_handler.register(name="ddrobot")
async def test_task():
    #await asyncio.sleep(2)
    g.logger.info('开始推送，推送中...')
    url = 'http://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=be3d714a-5029-4496-942f-e90a487479a3'
    headers = {'Content-Type':'application/json'}
    json_text = {
        "msgtype": "text",
        "text": {
            "content": "各位班组长，请按时进行分层审核打卡\n白班：13：00——14：00\n夜班：01：00——02：00",
	    "mentioned_list":["@all"],
        }
    }
    content_sent = requests.post(url, json.dumps(json_text), headers=headers)
    g.logger.info(content_sent)
    return "成功啦。。。。。。。"
