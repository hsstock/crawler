from crawler.sina.sinanews import Sinanews
from crawler.sina.mongodbutil import Mongodbutil
from crawler.sina.sinanews_history import Sinanewshistory
import time
import random
import pandas as pd
import datetime
import os
from apscheduler.schedulers.blocking import BlockingScheduler

mongodbutil = Mongodbutil('10.173.32.123', 27017, 'sinanews', 'urls')
sinanews = Sinanews(mongodbutil)
sinanewshistory = Sinanewshistory(mongodbutil)

sched = BlockingScheduler()

# MARKET = ['MY', 'US','HK','SZ','SH']
MARKET = ['MY']

working = False
working_history = False
timerid = 'my_job_id'
timerid_history = 'my_history_job_id'


def path():
    return os.path.dirname(__file__)


def read_file(market):
    data = pd.read_csv(path() + "//../data/ALL_" + market + ".txt", sep=' ', names=['code'])
    return data


def generate_url(market, code):
    if market == 'US':
        return 'http://stock.finance.sina.com.cn/usstock/quotes/' + code + '.html'
    if market == 'HK':
        return 'http://stock.finance.sina.com.cn/hkstock/quotes/' + code + '.html'
    if market == 'SH':
        return 'http://finance.sina.com.cn/realstock/company/' + str.lower(market) + code + '/nc.shtml'
    if market == 'SZ':
        return 'http://finance.sina.com.cn/realstock/company/' + str.lower(market) + code + '/nc.shtml'
    else:
        return "url not found"


# @sched.scheduled_job('cron',day_of_week='mon-fri',hour='0-23', minute='0-59',second='*/1')
# @sched.scheduled_job('interval',seconds=3)
def scheduled_job():
    print('scheduled_job..')
    if working == False:
        sched.remove_job(timerid)
        start_crawl()
    else:
        print('pre-timer is working')


def start_crawl():
    '''
    retrieve news from sina site
    :return:
    '''
    global working
    working = True
    print('start crawl current news...')
    for market in MARKET:
        data = read_file(market)
        for indexs in data.index:
            market = data.loc[indexs].values[0][0:2]
            code = data.loc[indexs].values[0][3:]
            url = generate_url(market, code)

            print('Current Time:{}, code:{}, url:{}'.format(datetime.datetime.now(), code, url))

            try:
                sinanews.get_page(code, url)
                items = sinanews.get_item_array()
                if len(items) > 0:
                    mongodbutil.insertItems(items)
                    time.sleep(4 * random.random())
                    print("store items to mongodb ...")
                else:
                    print("all items exists")
            except Exception as err:
                time.sleep(4 * random.random())
                print(err)
    working = False
    sched.add_job(scheduled_job, 'interval', seconds=1, id=timerid)


def scheduled_history_job():
    print('history_scheduled_job..')
    if working_history == False:
        sched.remove_job(timerid_history)
        start_crawl_history()
    else:
        print('pre-history-timer is working')


def start_crawl_history():
    '''
    retrieve news from sina site
    :return:
    '''
    global working_history
    working_history = True
    print('start crawl history news...')
    for market in MARKET:
        data = read_file(market)
        for indexs in data.index:
            market = data.loc[indexs].values[0][0:2]
            code = data.loc[indexs].values[0][3:]
            sinanewshistory.clear_item_array()
            print('Current Time:{}, code:{}, market:{},history'.format(datetime.datetime.now(), code, market))

            try:
                if market == 'HK':
                    sinanewshistory.get_hk_page(market, code)
                if market == 'US':
                    sinanewshistory.get_us_page(market, code)
                if market == 'SZ' or market == 'SH':
                    sinanewshistory.get_chn_page(market, code)

                items = sinanewshistory.get_item_array()
                if len(items) > 0:
                    mongodbutil.insertItems(items)
                    time.sleep(4 * random.random())
                    print("store items to mongodb ...")
                else:
                    print("all items exists")
            except Exception as err:
                time.sleep(4 * random.random())
                print(err)
    working_history = False
    sched.add_job(scheduled_history_job, 'interval', seconds=1, id=timerid_history)


print('Starting time: {}'.format(datetime.datetime.now()))
sched.add_job(scheduled_job, 'interval', max_instances=2, seconds=1, id=timerid)
sched.add_job(scheduled_history_job, 'interval', max_instances=2, seconds=1, id=timerid_history)
sched.start()
print('Ending time: {}'.format(datetime.datetime.now()))
