import pandas as pd
import numpy as np
import pickle
import sqlite3
from category_encoders import OneHotEncoder, OrdinalEncoder
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score


conn = sqlite3.connect('movie.db')
cur = conn.cursor()
cur.execute("SELECT * FROM movie_bill LEFT JOIN movie_data ON movie_bill.movie_id = movie_data.movie_id")
rows = cur.fetchall()
cols = [column[0] for column in cur.description] 
df = pd.DataFrame.from_records(data = rows, columns = cols)
conn.close()
target = 'total_pop'

def engineering(df): 
    # 관람가 기준 변경
    df['watchage'] = df['watchage'].replace(['청소년관람불가', '18세관람가', '고등학생이상관람가', '18세 미만인 자는 관람할 수 없는 등급'], '19세 이상')
    df['watchage'] = df['watchage'].replace(['15세이상관람가', '15세관람가', '연소자관람불가', '15세 미만인 자는 관람할 수 없는 등급', '중학생이상관람가', '15세 미만인 자는 관람할 수 없는 등급 '], '15세 이상')
    df['watchage'] = df['watchage'].replace(['12세이상관람가', '12세관람가', '연소자관람가'], '12세 이상')
    df['watchage'] = df['watchage'].replace(['전체관람가', '모든 관람객이 관람할 수 있는 등급'], '전체')
    df.loc[(df['watchage'].isnull()) & (df['genres'] == '애니메이션'), 'watchage'] = '12세 이상'
    df.loc[df['watchage'].isnull(), 'watchage'] = '15세 이상'
    # 개봉일 생성
    df.loc[df['open_day'] == ' ', 'open_day'] = df['today']
    df['today'] = pd.to_datetime(df['today'])
    df['open_day'] = pd.to_datetime(df['open_day'])
    df['total_open'] = df['today'] - df['open_day']
    df['total_open'] = df['total_open'].astype('str')
    df['total_open'] = df['total_open'].apply(lambda x : int(x.replace('days', '')))
    
    # Column 제거
    df.drop(['name', 'movie_id', 'id', 'today', 'open_day', 'total_bill'], axis = 1, inplace = True)
    return df
engineering(df)

def database():
    conn = sqlite3.connect('movie.db')
    cur = conn.cursor()
    cur.execute("DROP TABLE data_process")
    df.to_sql('data_process', conn)
    conn.commit()
    conn.close()

# Target 나누기
train, test = train_test_split(df, test_size = 2, random_state = 2)

X_train = train.drop('total_pop', axis = 1)
X_test = test.drop('total_pop', axis = 1)
y_train = train[target]
y_test = test[target]

# 기준모델 제작
predict = df[target].mean()


error = predict - df[target]

mae = error.abs().mean()

pipe = make_pipeline(
    OneHotEncoder(use_cat_names = True),
    StandardScaler(),
    LinearRegression()
)
pipe.fit(X_train, y_train)


#with open('model_test1.pickle','wb') as fw:
#    pickle.dump(pipe.fit(X_train, y_train), fw)
pickle.dump(pipe, open("model.pkl", "wb"))

database()