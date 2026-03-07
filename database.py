from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base

# 🔴 Replace YOUR_PASSWORD with your MySQL root password
DATABASE_URL = "mysql+pymysql://root:617250@localhost:3306/excel_validation_db"

engine = create_engine(DATABASE_URL)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()