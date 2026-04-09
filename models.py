from sqlalchemy import Column, Integer, String, Date, DateTime, Float, Text
from database import Base
from datetime import datetime


class FormEntry(Base):
    __tablename__ = "form_entries"

    id            = Column(Integer, primary_key=True, index=True)
    form_type     = Column(String(150))
    username      = Column(String(255))
    selected_date = Column(Date)
    row_status    = Column(String(50))
    circle        = Column(String(200))


class UploadHistory(Base):
    __tablename__ = "upload_history"

    id            = Column(Integer, primary_key=True, index=True)
    file_name     = Column(String(255))
    form_type     = Column(String(150))
    selected_date = Column(String(50))
    total_rows    = Column(Integer)
    valid_rows    = Column(Integer)
    junk_rows     = Column(Integer)
    valid_file    = Column(String(500))
    junk_file     = Column(String(500))
    upload_time   = Column(DateTime, default=datetime.utcnow)


class SiteMonitoring(Base):
    __tablename__ = "site_monitoring"

    id           = Column(Integer, primary_key=True, index=True)
    site_name    = Column(String(255))
    global_id    = Column(String(100))
    circle       = Column(String(150))
    status       = Column(String(50))
    alarm        = Column(String(255))
    since        = Column(String(100))
    end_time     = Column(String(100))
    last_updated = Column(DateTime, default=datetime.utcnow)


class FormTemplate(Base):
    __tablename__ = "form_templates"

    id         = Column(Integer, primary_key=True, index=True)
    form_name  = Column(String(255), unique=True, index=True)  # ✅ FIXED
    columns    = Column(Text)
    rules      = Column(Text)
    created_at = Column(DateTime, default=datetime.utcnow)