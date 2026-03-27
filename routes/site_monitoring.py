from fastapi import APIRouter, Depends, Body
from sqlalchemy.orm import Session
from typing import List, Dict
from datetime import datetime

from database import SessionLocal
import models

router = APIRouter()


# ======================
# DB SESSION
# ======================
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# ======================
# SAVE SITE DATA
# ======================
@router.post("/SAVE-SITE-DATA")
def save_site_data(data: List[Dict] = Body(...), db: Session = Depends(get_db)):

    print("📥 Incoming records:", len(data))   # 🔥 DEBUG

    # Clear old data
    db.query(models.SiteMonitoring).delete()

    for site in data:
        db.add(models.SiteMonitoring(
            site_name=site.get("site_name"),
            global_id=site.get("global_id"),
            status=site.get("status"),
            alarm=site.get("alarm"),
            since=site.get("since"),
            end_time=site.get("end_time"),
            last_updated=datetime.now()   # ✅ IMPORTANT
        ))

    db.commit()

    print("✅ Data saved in DB")

    return {
        "message": "saved",
        "records_saved": len(data)
    }