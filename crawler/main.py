from crawler.sina.sinanews import Sinanews
from crawler.sina.mongodbutil import Mongodbutil
from crawler.sina.sinanews_history import Sinanewshistory
import time
import random
import pandas as pd
import datetime
import os
from apscheduler.schedulers.blocking import BlockingScheduler
import crawler.logger as loger



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
    loger.info('scheduled_job..')
    if working == False:
        sched.remove_job(timerid)
        start_crawl()
    else:
        loger.info('pre-timer is working')


def start_crawl():
    '''
    retrieve news from sina site
    :return:
    '''
    global working
    working = True
    loger.info('start crawl current news...')
    for market in MARKET:
        data = read_file(market)
        for indexs in data.index:
            market = data.loc[indexs].values[0][0:2]
            code = data.loc[indexs].values[0][3:]
            url = generate_url(market, code)

            loger.info('Current Time:{}, code:{}, url:{}'.format(datetime.datetime.now(), code, url))

            try:
                sinanews.get_page(market, code, url)
                items = sinanews.get_item_array()
                if len(items) > 0:
                    mongodbutil.insertItems(items)
                    time.sleep(4 * random.random())
                    loger.info("store items to mongodb ...")
                else:
                    loger.info("all items exists")
            except Exception as err:
                time.sleep(4 * random.random())
                loger.warning(err)
    working = False
    sched.add_job(scheduled_job, 'interval', seconds=1, id=timerid)


def scheduled_history_job():
    loger.info('history_scheduled_job..')
    if working_history == False:
        sched.remove_job(timerid_history)
        start_crawl_history()
    else:
        loger.info('pre-history-timer is working')


def start_crawl_history():
    '''
    retrieve news from sina site
    :return:
    '''
    global working_history
    working_history = True
    loger.info('start crawl history news...')
    for market in MARKET:
        data = read_file(market)
        for indexs in data.index:
            market = data.loc[indexs].values[0][0:2]
            code = data.loc[indexs].values[0][3:]
            sinanewshistory.clear_item_array()
            loger.info('Current Time:{}, code:{}, market:{},history'.format(datetime.datetime.now(), code, market))
            page = 1
            type = '1'
            while page != -1:
                try:
                    if market == 'HK':
                        page = sinanewshistory.get_hk_page(market, code, page)
                    if market == 'US':
                        page, type = sinanewshistory.get_us_page(market, code, page, type)
                    if market == 'SZ' or market == 'SH':
                        page = sinanewshistory.get_chn_page(market, code, page)

                    items = sinanewshistory.get_item_array()
                    if len(items) > 0:
                        mongodbutil.insertItems(items)
                        time.sleep(4 * random.random())
                        loger.info("store items to mongodb ...")
                    else:
                        loger.info("all items exists")
                except Exception as err:
                    time.sleep(4 * random.random())
                    loger.warning(err)
    working_history = False
    sched.add_job(scheduled_history_job, 'interval', days=1, id=timerid_history)


loger.info('Starting time: {}'.format(datetime.datetime.now()))
sched.add_job(scheduled_job, 'interval', max_instances=2, seconds=1, id=timerid)
sched.add_job(scheduled_history_job, 'interval', max_instances=2, seconds=1, id=timerid_history)
sched.start()
loger.info('Ending time: {}'.format(datetime.datetime.now()))
