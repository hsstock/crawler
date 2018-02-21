import unittest
from crawler.crawler.sina.sinanews import Sinanews

class SinanewsTestCase(unittest.TestCase):
    url = 'http://stock.finance.sina.com.cn/usstock/quotes/A.html'
    sinanews = Sinanews()

    intA = 42175660821892231935534784390944271766118443271891321418998613315392083883057964877097217829913724525502582473452042277353636901681381274575489931813762880564494511842902201416526153330560757901349248891919817242380768455315251657863695815959585071904646992756587259994869512977302917572948716155863339230848070514585078212816540028664663420458027476621246583595995472107292508236329418095678286511628621548757580849713566893269337336223651308211149884913944062031730849300762744343311565485799148320648030567721446420613897261632525126231259074973050841218416640
    print( intA )
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
