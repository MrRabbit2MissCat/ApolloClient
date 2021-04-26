#!/usr/bin/env python
# coding=utf8
import pika


class RabbitMQ_Send():
    def __init__(self):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters('localhost'))
        self.channel = self.connection.channel()
        # 声明消息队列，消息将在这个队列传递，如不存在，则创建，durable = True 代表消息队列持久化存储，False 非持久化存储
        self.result = self.channel.queue_declare(
            queue='python-test', durable=False)

    def sendMessage(self, message):
        # 向队列插入数值 routing_key是队列名
        self.channel.basic_publish(exchange='', routing_key='python-test',
                                   # no_ack 设置成 False，在调用callback函数时，
                                   # 未收到确认标识，消息会重回队列。True，无论调用callback成功与否，消息都被消费掉
                                   body=message)
        print(f'消息:{message} 发送成功')

    def close(self):
        self.connection.close()
