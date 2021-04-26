import pymysql
from logTool import logTool
logger = logTool()


class OperationMysql:

    def __init__(self):
        # 创建一个连接数据库的对象
        self.conn = pymysql.connect(
            host='127.0.0.1',  # 连接的数据库服务器主机名
            port=3306,  # 数据库端口号
            user='root',  # 数据库登录用户名
            passwd='123456',
            db='rabblitmq_db',  # 数据库名称
            charset='utf8',  # 连接编码
            cursorclass=pymysql.cursors.DictCursor
        )
        # 使用cursor()方法创建一个游标对象，用于操作数据库
        self.cur = self.conn.cursor()

    # 查询数据
    def search_one(self, sql):
        self.cur.execute(sql)
        result = self.cur.fetchall()
        return result

    # 更新SQL
    def updata_one(self, sql):
        try:
            self.cur.execute(sql)  # 执行sql
            self.conn.commit()  # 增删改操作完数据库后，需要执行提交操作
        except Exception as e:
            print(f'updata_one{e}')
            # 发生错误时回滚
            self.conn.rollback()

    # 插入SQL
    def insert_one(self, sql):
        try:
            self.cur.execute(sql)  # 执行sql
            self.conn.commit()  # 增删改操作完数据库后，需要执行提交操作
        except Exception as e:
            print(f'insert_one{e}')
            # 发生错误时回滚
            self.conn.rollback()

    # 删除sql
    def delete_one(self, sql):
        try:
            self.cur.execute(sql)  # 执行sql
            self.conn.commit()  # 增删改操作完数据库后，需要执行提交操作
        except Exception as e:
            print(f'delete_one{e}')
            # 发生错误时回滚
            self.conn.rollback()


if __name__ == '__main__':
    op_mysql = OperationMysql()
    res = op_mysql.search_one(
        "SELECT *  from mq")
    print(res)
