# =====================================================
# FASTAPI FULL CLEAN VERSION
# =====================================================

from fastapi import FastAPI, File, UploadFile, HTTPException, Form, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, FileResponse

from sqlalchemy.orm import Session


import pandas as pd
import os
import uuid
from datetime import datetime
from io import BytesIO
import sys

# =====================================================
# DATABASE
# =====================================================

from database import SessionLocal
import models

# =====================================================
# VALIDATORS
# =====================================================

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(CURRENT_DIR, "validators"))

from site_survey_checklist_validator import validate_site_survey_checklist

# =====================================================
# APP INIT
# =====================================================

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"]
)

UPLOAD_FOLDER = "uploads"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

# =====================================================
# DB SESSION
# =====================================================

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# =====================================================
# DATA CLEANER
# =====================================================

def safe_clean_df(df):

    df = df.copy()

    df.columns = (
        df.columns
        .astype(str)
        .str.strip()
        .str.lower()
        .str.replace("\n", " ", regex=False)
        .str.replace("\r", "", regex=False)
        .str.replace("_", " ", regex=False)
        .str.replace("-", " ", regex=False)
    )

    df.columns = df.columns.str.replace(r"\s+", " ", regex=True)

    df = df.fillna("")

    for col in df.columns:
        df[col] = df[col].map(lambda x: str(x).strip() if pd.notna(x) else "")

    return df

# =====================================================
# USERNAME EXTRACTION
# =====================================================

def extract_username(row, form_type):

    USERNAME_COLUMN_MAP = {
        "Site Survey Checklist": ["createduser", "created user", "user name"],
        "Meeting Form": ["user name", "username", "createduser"],
        "OD Operation Form": ["createduser", "created user"],
        "Leave Form": ["createduser"],
        "EB Meter Form": ["createduser"],
        "FTTH Acquisition Form": ["createduser"],
        "OD Survey Form": ["user name", "username", "createduser"]

    }

    possible_cols = USERNAME_COLUMN_MAP.get(form_type, [])

    try:
        for col in possible_cols:

            if col in row:

                value = row[col]

                if isinstance(value, str):
                    value = value.strip()

                if value:
                    return value

        return "UNKNOWN_USER"

    except:
        return "UNKNOWN_USER"

# =====================================================
# NORMALIZER
# =====================================================

def normalize_form_name(form_type):
    return form_type.strip()

# =====================================================
# VALIDATE FORM API
# =====================================================

@app.post("/VALIDATE-FORM")
async def validate_form(
    file: UploadFile = File(...),
    form_type: str = Form(...),
    date: str = Form(...),
    db: Session = Depends(get_db)
):

    form_type = normalize_form_name(form_type)

    selected_date = pd.to_datetime(date, errors="coerce").date()

    # ---------------- File Validation ----------------

    if not file.filename:
        raise HTTPException(400, "No file uploaded")

    if not file.filename.lower().endswith((".csv", ".xlsx", ".xls")):
        raise HTTPException(400, "Invalid file format")

    # ---------------- Read File ----------------

    try:

        file_bytes = await file.read()

        if len(file_bytes) == 0:
            raise HTTPException(400, "Empty file")

        file_stream = BytesIO(file_bytes)

        if file.filename.lower().endswith(".csv"):
            df = pd.read_csv(file_stream, dtype=str)
        else:
            df = pd.read_excel(file_stream, dtype=str, engine="openpyxl")

        if len(df) == 0:
            raise HTTPException(400, "File has no data")

        df = safe_clean_df(df)

    except Exception as e:
        raise HTTPException(400, f"File read error: {str(e)}")

    # ---------------- Validation ----------------

    try:
        valid_df, junk_df = validate_site_survey_checklist(df)

    except Exception as e:
        raise HTTPException(400, f"Validation pipeline failed: {str(e)}")

    valid_df["status"] = "valid"
    junk_df["status"] = "invalid"

    combined_df = pd.concat([valid_df, junk_df], ignore_index=True)

    # ---------------- Save Output Files ----------------

    file_id = str(uuid.uuid4())

    valid_file = os.path.join(UPLOAD_FOLDER, f"{file_id}_valid.csv")
    junk_file = os.path.join(UPLOAD_FOLDER, f"{file_id}_junk.csv")

    valid_df.to_csv(valid_file, index=False)
    junk_df.to_csv(junk_file, index=False)

    # ---------------- DB Transaction ----------------

    try:

        for _, row in combined_df.iterrows():

            username = extract_username(row, form_type)

            db.add(models.FormEntry(
                form_type=form_type,
                username=username,
                selected_date=selected_date,
                row_status=row.get("status", "invalid"),
                circle=str(row.get("circle", "UNKNOWN_CIRCLE"))
            ))

        upload_history = models.UploadHistory(
            file_name=file.filename,
            form_type=form_type,
            selected_date=str(selected_date),
            total_rows=len(df),
            valid_rows=len(valid_df),
            junk_rows=len(junk_df),
            valid_file=valid_file,
            junk_file=junk_file
        )

        db.add(upload_history)
        db.commit()

        return {
            "status": "success",
            "total_rows": len(df),
            "valid_rows": len(valid_df),
            "junk_rows": len(junk_df)
        }

    except Exception as e:
        db.rollback()
        raise HTTPException(500, str(e))

from sqlalchemy import extract
from fastapi import Query
from typing import Optional

@app.get("/ANALYTICS")
def get_analytics(
    month: Optional[str] = Query(None),
    db: Session = Depends(get_db)
):

    query = db.query(models.FormEntry)

    if month:
        try:
            year, month_num = month.split("-")

            query = query.filter(
                extract("year", models.FormEntry.selected_date) == int(year),
                extract("month", models.FormEntry.selected_date) == int(month_num)
            )
        except:
            pass

    results = query.all()

    analytics = {}

    for r in results:

        if not r.username or r.username == "UNKNOWN_USER":
            continue

        username = str(r.username).strip()
        form_type = str(r.form_type)

        if username not in analytics:
            analytics[username] = {
                "username": username,
                "forms": {}
            }

        if form_type not in analytics[username]["forms"]:
            analytics[username]["forms"][form_type] = {
                "valid": 0,
                "invalid": 0,
                "total": 0
            }

        if str(r.row_status).lower() == "valid":
            analytics[username]["forms"][form_type]["valid"] += 1
        else:
            analytics[username]["forms"][form_type]["invalid"] += 1

    # Calculate totals
    for username in analytics:
        for form in analytics[username]["forms"]:

            v = analytics[username]["forms"][form]["valid"]
            i = analytics[username]["forms"][form]["invalid"]

            analytics[username]["forms"][form]["total"] = v + i

    return list(analytics.values())

# =====================================================
# FORM DATA FETCH
# =====================================================

@app.get("/FORM-DATA-MULTI")
def get_form_data_multi(
    forms: str,
    db: Session = Depends(get_db)
):

    try:

        # ⭐ Handle ALL forms request
        if forms == "ALL":
            results = db.query(models.FormEntry)\
                .order_by(models.FormEntry.id.desc())\
                .all()

        else:
            form_list = forms.split(",")

            results = db.query(models.FormEntry)\
                .filter(models.FormEntry.form_type.in_(form_list))\
                .order_by(models.FormEntry.id.desc())\
                .all()

        return [
            {
                "form_type": r.form_type,
                "username": r.username,
                "status": r.row_status,
                "date": str(r.selected_date)
            }
            for r in results
        ]

    except Exception as e:
        return {"error": str(e)}

# =====================================================
# DASHBOARD DATA
# =====================================================

@app.get("/DASHBOARD-DATA")
def get_dashboard_data(db: Session = Depends(get_db)):

    data = db.query(models.UploadHistory).all()

    return {
        "total_forms": len(data),   # ⭐ FIXED (Use total rows, not distinct count)
        "total_rows": sum(e.total_rows or 0 for e in data),
        "valid_rows": sum(e.valid_rows or 0 for e in data),
        "junk_rows": sum(e.junk_rows or 0 for e in data)
    }


# =====================================================
# DOWNLOAD FILE
# =====================================================

@app.get("/DOWNLOAD")
async def download_file(path: str):

    if not os.path.exists(path):
        raise HTTPException(404, "File not found")

    return FileResponse(
        path,
        filename=os.path.basename(path),
        media_type="application/octet-stream"
    )


# =====================================================
# UPLOAD HISTORY
# =====================================================

@app.get("/UPLOAD-HISTORY")
def get_upload_history(db: Session = Depends(get_db)):

    history = db.query(models.UploadHistory)\
        .order_by(models.UploadHistory.id.desc())\
        .all()

    return [
        {
            "file_name": h.file_name,
            "upload_time": str(h.upload_time),
            "total_rows": h.total_rows or 0,
            "valid_rows": h.valid_rows or 0,
            "junk_rows": h.junk_rows or 0
        }
        for h in history
    ]