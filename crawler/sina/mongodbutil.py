from pymongo import MongoClient

class Mongodbutil(object):
    def __init__(self,ip,port,collection,urlcollection):
        self.ip = ip
        self.port = port
        self.client = MongoClient(ip,port)
        self.db = self.client.admin
        #self.db.authenticate('root','experiment')
        #self.db.authenticate(' ', ' ')
        self.collection = self.db[collection]
        self.urlcollection = self.db[urlcollection]


    # item and items, all both ok
    def insertItems(self,items):
        self.collection.insert(items)

    def urlIsExist(self,url):
        items = self.urlcollection.find({"url":url})
        count = 0
        for item in items:
            count += 1

        if count > 0:
            return True
        else:
            return False

    def insertUrls(self,urls):
        self.urlcollection.insert(urls)

