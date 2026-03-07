from sqlalchemy import Column, Integer, String, Date, DateTime
from database import Base
from datetime import datetime


class FormEntry(Base):
    __tablename__ = "form_entries"

    id = Column(Integer, primary_key=True, index=True)

    form_type = Column(String(150))
    username = Column(String(255))

    selected_date = Column(Date)

    row_status = Column(String(50))
    circle = Column(String(200))


class UploadHistory(Base):
    __tablename__ = "upload_history"

    id = Column(Integer, primary_key=True, index=True)

    file_name = Column(String(255))
    form_type = Column(String(150))

    selected_date = Column(String(50))

    total_rows = Column(Integer)
    valid_rows = Column(Integer)
    junk_rows = Column(Integer)

    valid_file = Column(String(500))
    junk_file = Column(String(500))

    upload_time = Column(DateTime, default=datetime.utcnow)