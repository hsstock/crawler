from crawler.sina.sinanews import Sinanews
from crawler.sina.mongodbutil import Mongodbutil
from crawler.sina.sinanews_history import Sinanewshistory
import time
import random
import pandas as pd
import datetime
import os
from apscheduler.schedulers.blocking import BlockingScheduler
import crawler.logger as logger
import signal


#mongodbutil = Mongodbutil('10.173.32.123', 27017, 'sinanews', 'urls')
#mongodbutil = Mongodbutil('127.0.0.1', 27017, 'sinanews', 'urls')
mongodbutil = Mongodbutil('10.240.154.201', 27017, 'sinanews')
sinanews = Sinanews(mongodbutil)
sinanewshistory = Sinanewshistory(mongodbutil)

sched = BlockingScheduler()

MARKET_ALL = ['US','HK','SZ','SH']
MARKET = ['MY']

working = False
working_history = False
timerid = 'my_job_id'
timerid_history = 'my_history_job_id'
is_closing = False

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
    logger.info('scheduled_job..')
    if working == False:
        sched.remove_job(timerid)
        start_crawl()
    else:
        logger.info('pre-timer is working')


def start_crawl():
    '''
    retrieve news from sina site
    :return:
    '''
    global working
    working = True
    logger.info('start crawl current news...')
    for market in MARKET:
        if is_closing:
            break

        data = read_file(market)
        for indexs in data.index:
            if is_closing:
                break
            market = data.loc[indexs].values[0][0:2]
            code = data.loc[indexs].values[0][3:]
            url = generate_url(market, code)

            logger.info('Current Time:{}, code:{}, url:{}'.format(datetime.datetime.now(), code, url))

            try:
                sinanews.get_page(code, url)
                items = sinanews.get_item_array()
                if len(items) > 0:
                    mongodbutil.insertItems(items)
                    time.sleep(4 * random.random())
                    logger.info("store items to mongodb ...")
                else:
                    logger.info("all items exists")
            except Exception as err:
                time.sleep(4 * random.random())
                logger.warning(err)
    working = False
    if not is_closing:
        sched.add_job(scheduled_job, 'interval', seconds=1, id=timerid)


def scheduled_history_job():
    logger.info('history_scheduled_job..')
    if working_history == False:
        sched.remove_job(timerid_history)
        start_crawl_history()
    else:
        logger.info('pre-history-timer is working')


def start_crawl_history():
    '''
    retrieve news from sina site
    :return:
    '''
    global working_history
    working_history = True
    logger.info('start crawl history news...')
    for market in MARKET:
        if is_closing:
            break
        data = read_file(market)
        for indexs in data.index:
            if is_closing:
                break
            market = data.loc[indexs].values[0][0:2]
            code = data.loc[indexs].values[0][3:]
            sinanewshistory.clear_item_array()
            logger.info('Current Time:{}, code:{}, market:{}'.format(datetime.datetime.now(), code, market))

            page = 1
            type = '1'
            while page != -1:
                if is_closing:
                    break
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
                        logger.info("store items to mongodb ...")
                    else:
                        logger.info("all items exists")
                except Exception as err:
                    time.sleep(4 * random.random())
                    logger.warning(err)

    working_history = False
    if not is_closing:
        sched.add_job(scheduled_history_job, 'interval', days=1, id=timerid_history)



def signal_handler(signum,frame):
    global is_closing
    logger.info('exit success')
    is_closing = True

def try_exit():
    global is_closing
    if is_closing:
        logger.info('exit success2')

def main():
    signal.signal(signal.SIGINT,signal_handler)

    logger.info('Starting time: {}'.format(datetime.datetime.now()))
    #sched.add_job(scheduled_job, 'interval', max_instances=2, seconds=1, id=timerid)
    sched.add_job(scheduled_history_job, 'interval', max_instances=2, seconds=1, id=timerid_history)
    sched.start()
    logger.info('Ending time: {}'.format(datetime.datetime.now()))

def generate_all_urls():
    urls = []
    for market in MARKET_ALL:
        data = read_file(market)
        for indexs in data.index:
            market = data.loc[indexs].values[0][0:2]
            code = data.loc[indexs].values[0][3:]
            url = generate_url(market, code)
            urls.append(url)
    # 更改数组的栏目名称
    datas = pd.DataFrame(urls, columns=['URL'])

    # 生成excel文件到本地
    data = datas.to_excel('./urls.xls')


if __name__ == "__main__":
    main()