from sina.sinanews import Sinanews
from sina.mongodbutil import Mongodbutil
import time
import random
import pandas as pd
import datetime
import os
from apscheduler.schedulers.blocking import BlockingScheduler

mongodbutil = Mongodbutil('10.173.32.123',27017,'sinanews','urls')
sinanews = Sinanews(mongodbutil)


sched = BlockingScheduler()

#MARKET = ['MY', 'US','HK','SZ','SH']
MARKET = ['MY']

def path():
    return os.path.dirname(__file__)


def read_file(market):
    data = pd.read_csv( path() +  "//../data/ALL_" + market + ".txt", sep=' ', names=['code'])
    return data

def generate_url(market,code):
    if market == 'US' :
        return 'http://stock.finance.sina.com.cn/usstock/quotes/' + code + '.html'
    if market == 'HK' :
        return 'http://stock.finance.sina.com.cn/hkstock/quotes/' + code + '.html'
    if market == 'SH' :
        return 'http://finance.sina.com.cn/realstock/company/' + str.lower(market) + code + '/nc.shtml'
    if market == 'SZ' :
        return 'http://finance.sina.com.cn/realstock/company/' + str.lower(market) + code + '/nc.shtml'
    else :
        return "url not found"


working = False
timerid='my_job_id'
#@sched.scheduled_job('cron',day_of_week='mon-fri',hour='0-23', minute='0-59',second='*/1')
#@sched.scheduled_job('interval',seconds=3)
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
    working = True
    for market in MARKET:
        data = read_file(market)
        for indexs in data.index:
            market = data.loc[indexs].values[0][0:2]
            code = data.loc[indexs].values[0][3:]
            url = generate_url(market,code)

            print('Current Time:{}, code:{}, url:{}'.format(datetime.datetime.now(),code,url))

            try:
                sinanews.get_page(code,url)
                items = sinanews.get_item_array()
                if len(items) > 0 :
                    mongodbutil.insertItems(items)
                    time.sleep(4*random.random())
                    print("store items to mongodb ...")
                else:
                    print("all items exists")
            except Exception as err:
                time.sleep(4 * random.random())
                print(err)
    working = False
    job = sched.add_job(scheduled_job, 'interval', seconds=1, id=timerid)

print('Starting time: {}'.format(datetime.datetime.now()))
job = sched.add_job(scheduled_job, 'interval', seconds=1,id=timerid)
sched.start()
print('Ending time: {}'.format(datetime.datetime.now()))


