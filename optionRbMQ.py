#!/usr/bin/python
# -*- coding: utf-8 -*-
# @Author  : Yunhgu
# @File    : optionRbMQ.py
# @Software: Vscode
# @Time    : 2021-04-14 09:45:30

import pika
from logTool import logTool
logtool = logTool()


class option():
    def __init__(self, username, password, host, port, virtual_host):
        try:
            # mq用户名和密码
            self.credentials = pika.PlainCredentials(username, password)
            # 将登录信息传给参数
            self.parameters = pika.ConnectionParameters(
                host, port, virtual_host, self.credentials)
            # 链接RabblitMQ
            self.connection = pika.BlockingConnection(self.parameters)
        except (pika.exceptions.ProbableAccessDeniedError, pika.exceptions.StreamLostError) as e:
            logtool.error('init have an error:%s', e)
            print(f'init have an error: {e}')

    def creatFanoutExchange(self, exchange_name, queue):
        channel = self.connection.channel()
        # 声明exchange，由exchange指定消息在哪个队列传递，如不存在，则创建。durable = True 代表exchange持久化存储，False 非持久化存储
        channel.exchange_declare(
            exchange=exchange_name, durable=True, exchange_type='fanout')
        # 声明消息队列，消息将在这个队列传递，如不存在，则创建。durable = True 代表消息队列持久化存储，False 非持久化存储
        channel.queue_declare(queue=queue, durable=True)
        # 绑定exchange和队列  exchange 使我们能够确切地指定消息应该到哪个队列去
        channel.queue_bind(exchange=exchange_name,
                           queue=queue)

    def creatDirectExchange(self, exchange_name, queue):
        channel = self.connection.channel()
        # 声明exchange，由exchange指定消息在哪个队列传递，如不存在，则创建。durable = True 代表exchange持久化存储，False 非持久化存储
        channel.exchange_declare(
            exchange=exchange_name, durable=True, exchange_type='direct')
        # 声明消息队列，消息将在这个队列传递，如不存在，则创建。durable = True 代表消息队列持久化存储，False 非持久化存储
        channel.queue_declare(queue=queue, durable=True)
        # 绑定exchange和队列  exchange 使我们能够确切地指定消息应该到哪个队列去
        channel.queue_bind(exchange=exchange_name,
                           queue=queue, routing_key='OrderId')

    def creatTopicdExchange(self, exchange_name, queue):
        channel = self.connection.channel()
        # 声明exchange，由exchange指定消息在哪个队列传递，如不存在，则创建。durable = True 代表exchange持久化存储，False 非持久化存储
        channel.exchange_declare(
            exchange=exchange_name, durable=True, exchange_type='topic')
        # 声明消息队列，消息将在这个队列传递，如不存在，则创建。durable = True 代表消息队列持久化存储，False 非持久化存储
        channel.queue_declare(queue=queue, durable=True)
        # 绑定exchange和队列  exchange 使我们能够确切地指定消息应该到哪个队列去
        channel.queue_bind(exchange=exchange_name,
                           queue=queue, routing_key='OrderId')


if __name__ == '__main__':
    p = option('guest', 'guest', '127.0.0.1', 5672, 'new_virtual')
    p.creatFanoutExchange(exchange_name='FanoutExchange', queue="fanoutQueue")
    p.creatDirectExchange(exchange_name='directExchange',
                          queue='directQueue')
    p.creatTopicdExchange(exchange_name="TopicdExchange", queue="topicdQueue")
    print('success')
