import time
import datetime

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

if __name__ == '__main__':
    print(format_date("03-15"))