from flask import Blueprint, render_template, request
import pickle
import pandas as pd
import sqlite3
main_bp = Blueprint('main', __name__)

@main_bp.route('/')
def index():
    
    return render_template('index.html')


@main_bp.route('/model', methods = ['GET', 'POST'])
def model():
    if request.method == 'POST':
        result = request.form

    model = pickle.load(open("/Users/mingi.kim/Section3/Project/model.pkl", "rb"))
    def pred(movie_time, nation, genres, watchage, company, staff, total_open, movie_state = '개봉', typeNm = '장편'):
        data = [[movie_time, movie_state, typeNm, nation, genres, watchage, company, staff, total_open]]
        col = ['movie_time', 'movie_state', 'typeNm', 'nation', 'genres', 'watchage', 'company', 'staff', 'total_open']
        X_true = pd.DataFrame(data = data, columns = col)
        return X_true
    vale = [i for i in result.values()]

    
    X_true = pred(movie_time = int(vale[0]), nation = vale[1], genres = vale[2], watchage = vale[3], company = int(vale[4]), staff = int(vale[5]), total_open = int(vale[6]), movie_state = '개봉', typeNm = '장편')
    print(X_true)
    wow = model.predict(X_true)[0]
    if int(wow) < 0:
        wow = 0
    conn = sqlite3.connect('/Users/mingi.kim/Section3/Project/movie.db')
    cur = conn.cursor()
    cur.execute("INSERT INTO movie_pred(y_movie_time, y_nation, y_genres, y_watchage, y_company, y_staff, y_total_open, pred) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", (int(vale[0]), vale[1], vale[2], vale[3], int(vale[4]), int(vale[5]), int(vale[6]), int(wow)))
    conn.commit()
    conn.close()
    return render_template('model.html', result = result, pred = int(wow))
