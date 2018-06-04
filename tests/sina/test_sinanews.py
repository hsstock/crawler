import unittest
from crawler.sina.sinanews_history import Sinanewshistory
from crawler.sina.mongodbutil import Mongodbutil

class SinanewsTestCase(unittest.TestCase):
    url = 'view-source:http://vip.stock.finance.sina.com.cn/corp/view/vCB_AllNewsStock.php?symbol=sh601318&Page=2'
    mongodbutil = Mongodbutil('10.173.32.123', 27017, 'sinanews', 'urls')
    sinanews = Sinanewshistory(mongodbutil)

    sinanews.get_chn_page('SZ', '000735')


unittest.main
