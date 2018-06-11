from pymongo import MongoClient

class Mongodbutil(object):
    def __init__(self,ip,port,collection):
        self.ip = ip
        self.port = port
        self.client = MongoClient(ip,port)
        self.db = self.client.admin
        #self.db.authenticate('root','experiment')
        #self.db.authenticate(' ', ' ')
        self.collection = self.db[collection]


    # item and items, all both ok
    def insertItems(self,items):
        self.collection.insert(items)

    def urlIsExist(self,url):
        items = self.collection.find({"href":url})
        for item in items:
            return True
        return False

