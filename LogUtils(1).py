# -*- coding: utf-8 -*-
"""
    作者:       Peter
    版本:       1.1
    日期:       2019/05/24
    项目名称：   HYUNDAI_Score_Calculation
    python环境： 3.5
"""


import yaml
import logging.config
import os

def setup_logging(default_path = "logging.yaml",default_level = logging.INFO,env_key = "LOG_CFG"):
    path = default_path
    value = os.getenv(env_key,None)
    if value:
        path = value
    if os.path.exists(path):
        with open(path,"r") as f:
            config = yaml.load(f)
            logging.config.dictConfig(config)
    else:
        logging.basicConfig(level = default_level)

def func():
    #项目名|方法名|参数（可以是json或者任何格式）
    logging.info("LogUtils|func|start")

    logging.info("LogUtils|func|start")

    logging.info("LogUtils|func|end")

if __name__ == "__main__":
    setup_logging(default_path = "logging.yaml")
    func()