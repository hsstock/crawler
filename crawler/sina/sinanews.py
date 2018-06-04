import requests
import bs4
import random
import time
# from eth_bloom import BloomFilter
import os
import crawler.sina.date_util as dateutil
import crawler.logger as loger

class Sinanews(object):
    def __init__(self,mongodbutil):
        self.itemArray = []
        self.mongodbutil = mongodbutil
        self.urlExist = False

    def get_page(self,market, code,url):
        self.itemArray = []
        res = requests.get(url,timeout=10)
        res.encoding = "gbk"
        try:
            res.raise_for_status()
            if res.status_code == 200 :
                    contentSoup = bs4.BeautifulSoup(res.text,'lxml')
                    elems = contentSoup.select('#js_ggzx > li,.li_point > ul > li,.col02_22 > ul > li')
                    for elem in elems:
                        json = {}
                        json['code'] = code
                        temp = elem.__str__()[4:5]
                        if (temp == '\n') and market == 'US':
                            continue
                        ele = elem.select('span')
                        json['date'] = dateutil.format_date(ele[0].getText()[1:-1])
                        s = json['date']
                        ele = elem.select('a')
                        json['title'] = ele[len(ele)-1].getText()
                        loger.info("date:{},title:{}".format(s,json['title']))
                        json['href'] = ele[len(ele)-1].attrs['href']
                        json['year'] = 'guess'
                        ret,content = self.get_content(json['href'])
                        if ret != -1 :
                            time.sleep(4 * random.random())

                        if ret == 0 :
                            json['content'] = content
                            self.itemArray.append(json)
        except Exception as err:
            time.sleep(4 * random.random())
            loger.warning(err)
        finally:
            res.close()


    def get_content(self,url):
        content = ''
        ret = -1

        self.urlExist = self.mongodbutil.urlIsExist(url)
        if self.urlExist:
            loger.info('This url:{} has existed'.format(url))
            return ret, content

        header = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}
        res = requests.get(url,headers=header,timeout=10)
        res.encoding = "utf-8"
        try:
            res.raise_for_status()
            if res.status_code == 200:
                soup = bs4.BeautifulSoup(res.text,'lxml')
                elems = soup.select('#artibody,.entry-content')
                if len(elems) > 0 :
                    content = elems[0].getText()
                    ret = 0
        except Exception as err:
            loger.warning(err)
        finally:
            res.close()
        return ret, content

    def get_item_array(self):
        return self.itemArray



    def path(self):
        return os.path.dirname(__file__)
