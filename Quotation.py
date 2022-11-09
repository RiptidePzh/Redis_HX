import os,time
from datetime import datetime
import numpy as np
import pandas as pd
import easyquotation
import deal_topic as dt
from loguru import logger


def get_stock_code(stock_code):
    """判断股票ID对应的证券市场
    匹配规则
    ['50', '51', '60', '90', '110'] 为 sh
    ['00', '13', '18', '15', '16', '18', '20', '30', '39', '115'] 为 sz
    ['5', '6', '9'] 开头的为 sh， 其余为 sz
    :param stock_code:股票ID, 若以 'sz', 'sh' 开头直接返回对应类型，否则使用内置规则判断
    :return 'sh' or 'sz'"""
    assert type(stock_code) is str, "stock code need str type"
    sh_head = ("60","68")
    sz_head = ("000","001","002","003","30")
    if stock_code.startswith(sh_head):
        return 'sh'+stock_code
    else:
        if stock_code.startswith(sz_head):
            return 'sz' + stock_code
        else:
            None


def get_all_stock_codes():
    all_codes = easyquotation.update_stock_codes()
    stock_codes = list()
    for ac in all_codes:
        c = get_stock_code(ac)
        if c is not None:
            stock_codes.append(c)
    return stock_codes


def get_snapshort(sc):
    quotation = easyquotation.use('sina') # 新浪 ['sina'] 腾讯 ['tencent', 'qq']
    last_snap = quotation.stocks(sc,prefix=True)
    return last_snap


def cal_topic_index_rtn(chinese_topics,last_snapshort):
    # 计算结果和wind的一样
    top_index_rtn = dict()
    for ct in chinese_topics:
        ct_tickers = chinese_topics[ct]
        ct_rtns = list()
        for ctt in ct_tickers:
            if ctt.startswith('sh8'):continue  # 北交所的票忽略
            ctt_rtn = last_snapshort[ctt]['now']/last_snapshort[ctt]['close']-1
            ct_rtns.append(ctt_rtn)
        # print(ct,np.mean(ct_rtns))
        top_index_rtn[ct] = np.mean(ct_rtns)
    return top_index_rtn


def run():
    chi_topics = dt.deal_for_easyquotation()
    stock_codes = get_all_stock_codes()
    last_snapshort = get_snapshort(stock_codes)
    topic_index_rtn = cal_topic_index_rtn(chi_topics,last_snapshort)
    print(topic_index_rtn)


def run_real_time():
    chi_topics = dt.deal_for_easyquotation()
    stock_codes = get_all_stock_codes()
    #-------------------------------------------------
    # real time 实时更新snapshort
    trading_day = datetime.today().strftime("%Y%m%d")
    td_folder = fr'./data/{trading_day}'
    os.makedirs(td_folder,exist_ok=True)

    fid_index = open(r'{}/indexrtn_{}.csv'.format(td_folder,time.strftime("%H%M%S", time.localtime())),'w')
    topic_names = list(chi_topics.keys())
    fid_index.write('Count,LocalTime')
    for tn in topic_names:
        fid_index.write(',{}'.format(tn))
    fid_index.write('\n')
    count = 0
    topic_rtn_list = list()
    while count<10000:
        last_snapshort = get_snapshort(stock_codes)
        topic_index_rtn = cal_topic_index_rtn(chi_topics, last_snapshort)

        topic_index_rtn_se = pd.Series(topic_index_rtn).sort_values(ascending=False)
        topic_rtn_list.append(topic_index_rtn_se)
        if len(topic_rtn_list)>20:
            top10 = list(topic_index_rtn_se.index[-10:])
            bottom10 = list(topic_index_rtn_se.index[:10])[::-1]
            logger.info("Top10-Topic:{}".format(top10))
            logger.info("Bottom10-Topic:{}".format(bottom10))
            topic_rtn_diff = topic_rtn_list[-1] - topic_rtn_list[-20]
            topic_rtn_diff = topic_rtn_diff.sort_values(ascending=False)
            up_ratio = list(topic_rtn_diff.index[:10])
            logger.info(u"最近100s涨速最快前10:{}".format(up_ratio))

        fid_index.write('{},{}'.format(count,time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
        for tn in topic_names:
            fid_index.write(',{}'.format(topic_index_rtn[tn]))
        fid_index.write('\n')
        fid_index.flush()
        time.sleep(5)
        count += 1
    # -------------------------------------------------


if __name__=="__main__":
    # run()
    os.makedirs('./log',exist_ok=True)
    logger.add('./log/log_{time}.log')
    run_real_time()