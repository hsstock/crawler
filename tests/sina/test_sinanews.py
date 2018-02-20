import unittest
from crawler.crawler.sina.sinanews import Sinanews

class SinanewsTestCase(unittest.TestCase):
    url = 'http://stock.finance.sina.com.cn/usstock/quotes/A.html'
    sinanews = Sinanews()

    sinanews.addUrl(url)
    #sinanews.get_page('A', url)
    sinanews.writeBloomValueToFile()
    sinanews.readBloomValueFromFile()

    sinanews.addUrl(url)
    #sinanews.get_page('A', url)
    sinanews.writeBloomValueToFile()
    sinanews.readBloomValueFromFile()

    sinanews.addUrl(url)
    #sinanews.get_page('A', url)
    sinanews.writeBloomValueToFile()
    sinanews.readBloomValueFromFile()

    sinanews.addUrl(url)
    #sinanews.get_page('A', url)
    sinanews.writeBloomValueToFile()
    sinanews.readBloomValueFromFile()
    
unittest.main
