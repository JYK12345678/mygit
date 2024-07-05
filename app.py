from fastapi import FastAPI, HTTPException
from sqlalchemy import create_engine, Column, Integer, String, Float, ForeignKey, inspect
from sqlalchemy.orm import sessionmaker, relationship, declarative_base
from pydantic import BaseModel
import json
import pandas as pd
from konlpy.tag import Okt
from collections import Counter
import re
import os

app = FastAPI()

# SQLite 데이터베이스 연결
SQLALCHEMY_DATABASE_URL = "sqlite:///./movie_database.db"
engine = create_engine(SQLALCHEMY_DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()

class Movie(Base):
    __tablename__ = "movies"

    id = Column(Integer, primary_key=True, index=True)
    title = Column(String, index=True)
    poster_url = Column(String)
    age_limit = Column(String, default="없음")
    running_time = Column(String)
    release_date = Column(String)
    synopsis = Column(String)
    recommended_movies = Column(String, default="없음")
    genre = Column(String)
    rating = Column(String)
    audience = Column(Integer)

    reviews = relationship("Review", back_populates="movie")

class Review(Base):
    __tablename__ = "reviews"

    id = Column(Integer, primary_key=True, index=True)
    classification = Column(String)
    ratio = Column(Float)
    summary = Column(String)
    movie_id = Column(Integer, ForeignKey("movies.id"))

    movie = relationship("Movie", back_populates="reviews")

def create_tables():
    inspector = inspect(engine)
    if not inspector.has_table("movies"):
        Base.metadata.create_all(bind=engine)
        print("Tables created.")
    else:
        print("Tables already exist. Skipping creation.")

def preprocess_text(text):
    # 불필요한 문자 제거
    text = re.sub(r'[^\w\s]', '', text)
    # 형태소 분석
    okt = Okt()
    tokens = okt.pos(text, stem=True)
    # 명사, 형용사, 동사만 선택
    tokens = [word for word, pos in tokens if pos in ['Noun', 'Adjective', 'Verb']]
    return tokens

def get_top_words(reviews, n=10):
    all_words = []
    for review in reviews:
        all_words.extend(preprocess_text(review['리뷰내용']))
    word_counts = Counter(all_words)
    return ', '.join([word for word, _ in word_counts.most_common(n)])

def process_movie_data(movie_data):
    positive_reviews = [review for review in movie_data['리뷰'] if review['감정'] == '긍정']
    negative_reviews = [review for review in movie_data['리뷰'] if review['감정'] == '부정']
    
    positive_summary = get_top_words(positive_reviews)
    negative_summary = get_top_words(negative_reviews)
    
    total_reviews = len(positive_reviews) + len(negative_reviews)
    positive_ratio = len(positive_reviews) / total_reviews if total_reviews > 0 else 0
    negative_ratio = len(negative_reviews) / total_reviews if total_reviews > 0 else 0
    
    return {
        'positive_summary': positive_summary,
        'negative_summary': negative_summary,
        'positive_ratio': positive_ratio,
        'negative_ratio': negative_ratio
    }

# 파일의 절대 경로를 얻습니다
current_dir = os.path.dirname(os.path.abspath(__file__))
data_file_path = os.path.join(current_dir, 'data.json')

def load_and_process_data():
    with open(data_file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    db = SessionLocal()
    
    for movie_data in data:
        processed_data = process_movie_data(movie_data)
        
        movie = Movie(
            title=movie_data['영화명'],
            poster_url=movie_data['포스터'],
            running_time=movie_data['상영시간'],
            release_date=movie_data['개봉년도'],
            synopsis=movie_data['줄거리'],
            genre=movie_data['장르'],
            rating=movie_data['평점'],
            audience=movie_data['관객수']
        )
        db.add(movie)
        db.flush()
        
        positive_review = Review(
            classification='긍정',
            ratio=processed_data['positive_ratio'],
            summary=processed_data['positive_summary'],
            movie_id=movie.id
        )
        negative_review = Review(
            classification='부정',
            ratio=processed_data['negative_ratio'],
            summary=processed_data['negative_summary'],
            movie_id=movie.id
        )
        db.add(positive_review)
        db.add(negative_review)
    
    db.commit()
    db.close()

@app.get("/movies")
def get_movies():
    db = SessionLocal()
    movies = db.query(Movie).all()
    db.close()
    return [{"id": movie.id, "title": movie.title, "poster_url": movie.poster_url} for movie in movies]

@app.get("/movie/{movie_id}")
def get_movie(movie_id: int):
    db = SessionLocal()
    movie = db.query(Movie).filter(Movie.id == movie_id).first()
    if not movie:
        raise HTTPException(status_code=404, detail="Movie not found")
    
    reviews = db.query(Review).filter(Review.movie_id == movie_id).all()
    db.close()
    
    return {
        "movie": {
            "id": movie.id,
            "title": movie.title,
            "poster_url": movie.poster_url,
            "age_limit": movie.age_limit,
            "running_time": movie.running_time,
            "release_date": movie.release_date,
            "synopsis": movie.synopsis,
            "recommended_movies": movie.recommended_movies,
        },
        "reviews": [
            {
                "classification": review.classification,
                "ratio": review.ratio,
                "summary": review.summary
            } for review in reviews
        ]
    }

if __name__ == "__main__":
    create_tables()
    load_and_process_data()
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)