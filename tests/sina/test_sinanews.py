import unittest
from crawler.sina.sinanews import Sinanews
from crawler.sina.mongodbutil import Mongodbutil

class SinanewsTestCase(unittest.TestCase):
    url = 'http://stock.finance.sina.com.cn/hkstock/quotes/NTES.html'
    mongodbutil = Mongodbutil('10.173.32.123', 27017, 'sinanews', 'urls')
    sinanews = Sinanews(mongodbutil)

    sinanews.get_page('A', url)
    sinanews.writeBloomValueToFile()
    sinanews.readBloomValueFromFile()

    sinanews.get_page('A', url)
    sinanews.writeBloomValueToFile()
    sinanews.readBloomValueFromFile()

unittest.main
