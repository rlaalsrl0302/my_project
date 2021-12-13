import sqlite3
import requests
import time
from datetime import datetime, timedelta
import json
from pymongo import MongoClient

# MongoDB Session
DB_NAME = "project"
COLL_DATA = "movie_data"
COLL_BILL = "movie_bill"

client = MongoClient(port = 27017, username = 'root', password = 'admin1234')
database = client[DB_NAME]
coll_data = database[COLL_DATA]
coll_bill = database[COLL_BILL]

# 영화 진흥
key = "4deb6ede8cec02bd57abb5c4cc42a50f"


# 매출 관련 데이터 NOSQL 저장

def bill_data():
    def date_range(start, end):
        start = datetime.strptime(start, "%Y%m%d")
        end = datetime.strptime(end, "%Y%m%d")
        dates = [(start + timedelta(days=i)).strftime("%Y%m%d") for i in range((end-start).days+1)]
        return dates
    # 20150101 - 20211001
    # 20100101 - 02141231
    dates = date_range("20100101", "20141231")

    for day in dates:
        movie_bill = requests.get(f'http://kobis.or.kr/kobisopenapi/webservice/rest/boxoffice/searchDailyBoxOfficeList.json?key={key}&targetDt={day}')
        time.sleep(0.5)
        bill = json.loads(movie_bill.text)
        coll_bill.insert_one(bill)


def create_db():
    conn = sqlite3.connect('movie.db')
    cur = conn.cursor()

    cur.execute("""CREATE TABLE movie_bill(
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	movie_id VARCHAR(20) NOT NULL UNIQUE,
	name VARCHAR(100),
	open_day VARCHAR(15),
	today VARCHAR(15),
	total_bill INTEGER,
	total_pop INTEGER
    );
    """)

    cur.execute("""CREATE TABLE movie_data(
	movie_id VARCHAR(20) PRIMARY KEY,
	movie_time INTEGER,
	movie_state VARCHAR(10),
	typeNm VARCHAR(10),
	nation VARCHAR(20),
	genres VARCHAR(20),
	directors VARCHAR(20),
    actor1 VARCHAR(20),
    actor2 VARCHAR(20),
	watchage VARCHAR(20),
	company INTEGER,
	staff INTEGER,
	FOREIGN KEY(movie_id) REFERENCES movie_bill(movie_id)
    );
    """)

    conn.close()

def bill_db():
    conn = sqlite3.connect('movie.db')
    cur = conn.cursor()

    for i in coll_bill.find({}, {'_id' : 0}):
        today = i['boxOfficeResult']['showRange'][:8]
        for x in i['boxOfficeResult']['dailyBoxOfficeList']:
            try:
                cur.execute('INSERT INTO movie_bill (movie_id, name, open_day, today, total_bill, total_pop) VALUES (?, ?, ?, ?, ?, ?)', (x['movieCd'], x['movieNm'], x['openDt'], today, x['salesAcc'], x['audiAcc']))
            except:
                cur.execute("SELECT today FROM movie_bill WHERE movie_id = ?", (x['movieCd'], ))
                day = int(cur.fetchall()[0][0])
                if day <= int(today):
                    cur.execute(f"UPDATE movie_bill set (today, total_bill, total_pop) = (?, ?, ?) where movie_id = ?", (today, x['salesAcc'], x['audiAcc'], x['movieCd']))
                    pass
    conn.commit()
    conn.close()


def movie_data():
    conn = sqlite3.connect('movie.db')
    cur = conn.cursor()

    cur.execute('SELECT movie_id FROM movie_bill')
    for i in cur.fetchall():
        movie_data = requests.get(f'http://www.kobis.or.kr/kobisopenapi/webservice/rest/movie/searchMovieInfo.json?key={key}&movieCd={i[0]}')
        time.sleep(1)
        data = json.loads(movie_data.text)
        coll_data.insert_one(data)
        
        
    conn.commit()
    conn.close()


def movie_db():
    conn = sqlite3.connect('movie.db')
    cur = conn.cursor()
    ai = []

    for data in coll_data.find({}, {'_id' : 0}):
        i = data['movieInfoResult']['movieInfo']
        
        print(ai)
        if (i['movieCd'] in ai) == False:
            ai.append(i['movieCd'])
            try:
                cur.execute("INSERT INTO movie_data(movie_id, movie_time, movie_state, typeNm, nation, genres, watchage, company, staff) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", (i['movieCd'], int(i['showTm']), i['prdtStatNm'], i['typeNm'], i['nations'][0]['nationNm'], i['genres'][0]['genreNm'], i['audits'][0]['watchGradeNm'], len(i['companys']), len(i['staffs'])))
            except:
                cur.execute("INSERT INTO movie_data(movie_id, movie_time, movie_state, typeNm, nation, genres, watchage, company, staff) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", (i['movieCd'], i['showTm'], i['prdtStatNm'], i['typeNm'], i['nations'][0]['nationNm'], i['genres'][0]['genreNm'], None, len(i['companys']), len(i['staffs'])))
                pass
        else:
            pass
            
            
            
        
    conn.commit()
    conn.close()
