# 定时完成数据库写入
import datetime
import schedule
import time
from RedisDB import RedisDB

def _stop():
    global index
    index = 0
    print('stop')

def _start():
    global index
    index = 1
    print('start')

if __name__ == '__main__':
    # 判断是否是工作日
    if datetime.date.today().weekday() < 5:
        rdb = RedisDB()
        index = 0
        # 每天开盘时间写入数据
        schedule.every().day.at('09:15').do(_start)
        schedule.every().day.at('11:35').do(_stop)
        schedule.every().day.at('12:50').do(_start)
        schedule.every().day.at('15:05').do(_stop)
        while index:
            # 每秒刷新一次数据
            rdb.quote_stk()
            time.sleep(1)