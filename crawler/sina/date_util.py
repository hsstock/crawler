import time
import datetime
import pymongo

def format_date(strDate):
    if isVaildDate(strDate):
        return strDate
    md = str();
    hms = str();
    ymdhms = str();
    if " " in strDate:
        tupDate = strDate.partition(" ")
        md = tupDate[0]
        hms = tupDate[2] + ":00"
    else:
        md = strDate
        hms = "00:00:00"
    ymdhms = str(datetime.datetime.now().year) + "-" + md + " " + hms
    return ymdhms

def isVaildDate(date):
    try:
        time.strptime(date, "%Y-%m-%d %H:%M:%S")
        return True
    except:
        return False

def format_date_us_history(strDate):
    if isVaildDate(strDate):
        return strDate
    tupDate = strDate.partition("|")
    chineseDate = tupDate[2]+":00"
    date = str(chineseDate)
    date = date.replace("年", "-")
    date = date.replace("月", "-")
    date = date.replace("日", "")
    date = date.strip()
    return date


if __name__ == '__main__':
    connection = pymongo.MongoClient('10.173.32.123', 27017)
    admin = connection['admin']
    admin.authenticate('root', 'experiment')
    for s in admin.sinanews.find({"href": {"$exists": True}}):
        date = s['date']
        date = format_date(date)
        if date > '2018-06-04 20:59:59':
            temp = date.partition("-")
            date = '2017-' + temp[2]
        admin.sinanews.update({'_id': s['_id']}, {'$set': {'date': date}})
        admin.sinanews.update({'_id': s['_id']}, {'$set': {'year': 'guess'}})
        print(s['title'])
