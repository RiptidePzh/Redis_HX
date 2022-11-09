import easyquotation
import json
import redis
import schedule
import time
import datetime
import pandas as pd
import numpy as np
import os
# 将数据读入Redis Server
# 数据形式为name-key-value:ticker-time-jsonstr

class RedisDB():
    # write data into Redis Server
    def __init__(self, host='localhost', port=6379, decode_responses=True):
        self.host = host
        self.port = port
        self.decode_responses = decode_responses
        self.connect()
        self.my_dic = dict() # 一个临时字典，只会储存最新的snapshot

    def connect(self):
        # initalize redis database
        global r
        pool = redis.ConnectionPool(host=self.host,  port=self.port, decode_responses=self.decode_responses)
        r = redis.Redis(connection_pool=pool)

    def quote_stk(self, web='sina'):
        # get snapshot from webpage using easyquotation
        quotation = easyquotation.use(web)
        self.my_dic.update(quotation.market_snapshot(prefix=True))
        for key, value in self.my_dic.items():
            index = value.pop('date')+' '+value.pop('time')
            # 如果key不存在就添加，否则不处理
            r.hsetnx(key, index, json.dumps(value))

    def cal_topic_index_rtn(self):
        # deal for easyquotation
        chinese_map = pd.read_csv('D:/Pzh/txt/top_chinese_map.csv', encoding='gbk')
        chinese_map = dict(zip(list(chinese_map.iloc[:, 0]), list(chinese_map.iloc[:, 1])))
        topics = os.listdir(r"D:/Pzh/topic")
        chinese_topics = dict()
        for topi in topics:
            topi_data = pd.read_csv(fr"D:/Pzh/topic/{topi}")
            tickers = ['sz' + t[:6] if t[-1] == 'Z' else 'sh' + t[:6] for t in topi_data['1'].values]
            chinese_topics[chinese_map[topi.split('.')[0]]] = tickers
        # calculate topic index
        # 结果和wind一致
        top_index_rtn = dict()
        for ct in chinese_topics:
            ct_tickers = chinese_topics[ct]
            ct_rtns = list()
            for ctt in ct_tickers:
                if ctt.startswith('sh8'): continue  # 北交所的票忽略
                ctt_rtn = self.my_dic[ctt]['now'] / self.my_dic[ctt]['close'] - 1
                ct_rtns.append(ctt_rtn)
            # print(ct,np.mean(ct_rtns))
            top_index_rtn[ct] = np.mean(ct_rtns)
        return top_index_rtn

class RedisGet(RedisDB):
    # get data from redis server
    def __init__(self):
        self.mDataDict = dict()

    def get_stk_list(self):
        return r.keys()

    def get_stk(self,tickers=None):
        # 从redis索引股票数据，以字典形式储存
        if isinstance(tickers,str):
            tickers = [tickers]
        for ticker in tickers:
            redis_dic = r.hgetall(ticker)
            mapping = zip(redis_dic.keys(), map(json.loads,redis_dic.values()))
            self.mDataDict.update(mapping)



    @property
    def mDataFrame(self):
        # 将字典转化为数据框
        df = pd.DataFrame()
        for key, value in self.mDataDict.items():
            df_1 = pd.DataFrame(value).transpose()
            df_1['ticker'] = key
            df = pd.concat([df, df_1], axis=0)
        return df


# class RedisSche():
#     def __init__(self, func, index=0):
#         self.index = 0 # default: not execute
#         self.func = func
#
#     def _stop(self):
#         self.index = 0
#
#     def _start(self):
#         self.index = 1
#
#     def _loop(self,n=1):
#         while self.index:
#             self.func
#             time.sleep(n)

# 定时完成数据库写入
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
    import sys
    if datetime.date.today().weekday() < 5:
        rdb = RedisDB()
        rdb.quote_stk()
        Get = RedisGet()
        Get.get_stk(Get.get_stk_list())
        print(sys.getsizeof(Get))

        # index = 0
        # 每天开盘时间写入数据
        # schedule.every().day.at('09:15').do(_start)
        # schedule.every().day.at('11:35').do(_stop)
        # schedule.every().day.at('12:50').do(_start)
        # schedule.every().day.at('15:05').do(_stop)
        # while True:
        #     schedule.run_pending()
        #     time.sleep(60)
        #
        # while True:
        #     # 每秒刷新一次数据
        #     rdb.quote_stk()
        #     time.sleep(1)
