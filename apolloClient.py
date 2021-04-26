#!/usr/bin/python
# -*- coding: utf-8 -*-
# @Author  : Yunhgu
# @File    : apolloClient.py
# @Software: Vscode
# @Time    : 2021-04-12 15:35:28


import requests
import socket
import json
import time
import logging
from send import RabbitMQ_Send

logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')


class apollo_client(object):
    def __init__(self, config_server_url, appId, namespaceName, clusterName='default'):
        self.config_server_url = config_server_url
        self.appId = appId
        self.clusterName = clusterName
        self.namespaceName = namespaceName
        self.release_key_dic = {appId: ''}
        self.configs = None

    # 初始化url为apollo配置地址
    def init_url(self):
        clientIp = self.clientIp()
        return f'{self.config_server_url}/configs/{self.appId}/{self.clusterName}/{self.namespaceName}?releaseKey={self.release_key_dic[self.appId]}&ip={clientIp}'

    # 获取配置文件
    def get_configs(self):
        try:
            url = self.init_url()
            print(url)
            r = requests.get(url=url)
            print(r.status_code)
            if r.status_code == 200:
                json_content = json.loads(r.content)
                self.release_key_dic[self.appId] = json_content['releaseKey']
                return json_content['configurations']
            else:
                return None
        except Exception as e:
            print(f'get_configs:{e}')
            return {}

    # 向MQ发送配置修改的消息
    def send2MQ(self):
        MQ = RabbitMQ_Send()
        if self.configs:
            for k, v in self.configs.items():
                MQ.sendMessage(f'{k}:{v}')
        MQ.close()

    # 获取配置value
    def get_value(self, key, default_value=None):
        try:
            self.configs = self.get_configs()
            if self.configs:
                return self.configs[key]
            else:
                return None
        except KeyError as e:
            print(f'get_value:{e}')
            return default_value

    # 获取本机ip地址
    def clientIp(self):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(('8.8.8.8', 53))
            ip = s.getsockname()[0]
            return ip
        except Exception as e:
            print(f'clientIp:{e}')
        finally:
            s.close()
        return ""


if __name__ == '__main__':
    apollo_config_url = 'http://10.50.132.68:8080'
    appid = '10002'
    namespace = '0001.BaseConfig'
    ac = apollo_client(apollo_config_url, appid, namespace)
    while True:
        value = ac.get_value('af', 'defaultvalue')
        if value:
            ac.send2MQ()
            print('配置更新，推送到了MQ')
        else:
            print('配置没有更新')
        time.sleep(10)
    # apollo_config_url = 'http://106.54.227.205:8080'
    # appid = '123456654321'
    # namespace = 'yun1.test'
    # ac = apollo_client(apollo_config_url, appid, namespace)
    # while True:
    #     value = ac.get_value('1', 'defaultvalue')
    #     if value:
    #         ac.send2MQ()
    #         print('配置更新，推送到了MQ')
    #     else:
    #         print('配置没有更新')
    #     time.sleep(10)
