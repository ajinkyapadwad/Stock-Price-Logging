import MySQLdb

from yahoo_finance import Share
import time

db = MySQLdb.connect("127.0.0.1","root","","stocks" )

cursor = db.cursor()

Y = Share('YHOO')
G = Share('GOOG')
A = Share('AMZN')
E = Share('EA')
AP = Share('AAPL')

def RealTimeStocks(name):

        sql = """CREATE TABLE realtime (
                date  DATE,
                open FLOAT,
                high FLOAT,
                low FLOAT,
                price FLOAT,
                volume INT )"""

        cursor.execute(sql)      
        
        for i in range(1000):
                date= name.get_trade_datetime()
                open1= name.get_open()
                high= name.get_days_high()
                low= name.get_days_low()
                price= name.get_price()
                volume= name.get_volume()
                sql = "INSERT INTO realtime (date, open, high, low, price, volume )VALUES ('%s', '%s', '%s', '%s', '%s', '%s' )" % (date, open1, high, low, price, volume)
                cursor.execute(sql)
                db.commit()
                name.refresh()
                time.sleep(2)
        db.close()

def HistoricalStocks(name):
        sql = """CREATE TABLE historical (
                date  DATE,
                open FLOAT,
                high FLOAT,
                low FLOAT,
                price FLOAT,
                volume INT )"""

        print " NOW RUNNING: ",name.get_name()
        
        dic = name.get_historical('2014-04-25', '2015-04-25')
        
        for i in range(len(dic)):

            val=dic[i].values()
            
            cursor = db.cursor()
            
            sql = "INSERT INTO yahoo (date, open, high, low, price, volume )VALUES ('%s', '%s', '%s', '%s', '%s', '%s' )" % (val[5], val[2], val[4], val[3], val[7], val[0])
            
            cursor.execute(sql)

            db.commit()

        db.close()

for stock in [Y,G,A,E,AP] :
        RealTimeStocks(stock)
        HistoricalStocks(stock)


