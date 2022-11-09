import os,time
import numpy as np
import pandas as pd

import os,time
import numpy as np
import pandas as pd
from WindPy import *


def download_topic():
    fid = open("D:/Pzh/txt/topic_name.txt",'r')
    line = fid.readline()
    fid.close()
    topics = line.split(',')

    w.start()
    if not w.isconnected():
        print("Wind Connect Failed")
        return
    for topic in topics:

        w_data = w.wset("sectorconstituent","date=2022-11-09;sectorid={}".format(topic))
        if len(w_data.Data)>0:
            print(topic)
            pd.DataFrame(w_data.Data).T.to_csv(fr'D:/Pzh/topic/{topic}.csv')
    w.stop()


def map_chinese_name():
    fid = open("D:/Pzh/txt/topic_name.txt", 'r')
    line = fid.readline()
    fid.close()
    topics = line.split(',')
    chinese = pd.read_excel(r'D:/Pzh/txt/topic_chinese_name.xlsx')

    chinese_ = [str(c) for c in list(chinese.iloc[1:,0])]
    chinese_map = dict(zip(topics[1:],chinese_))
    pd.Series(chinese_map).to_csv('D:/Pzh/txt/top_chinese_map.csv',encoding='gbk')
    pass

def deal_for_easyquotation():
    # 处理成easyquotation能读取的格式

    chinese_map = pd.read_csv('D:/Pzh/txt/top_chinese_map.csv',encoding='gbk')
    chinese_map = dict(zip(list(chinese_map.iloc[:,0]),list(chinese_map.iloc[:,1])))
    topics = os.listdir(r"D:/Pzh/topic")
    chinese_topics = dict()
    for topi in topics:
        topi_data = pd.read_csv(fr"D:/Pzh/topic/{topi}")
        tickers = ['sz'+t[:6] if t[-1]=='Z' else 'sh'+t[:6] for t in topi_data['1'].values]
        chinese_topics[chinese_map[topi.split('.')[0]]] = tickers
    return chinese_topics




if __name__ == '__main__':
    # download_topic()
    # map_chinese_name()
    topics = deal_for_easyquotation()
    print(topics)
