# =====================================================
# FASTAPI FULL CLEAN VERSION
# =====================================================

from fastapi import FastAPI, File, UploadFile, HTTPException, Form, Depends
from routes import site_monitoring
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from sqlalchemy.orm import Session
from sqlalchemy import extract
from pydantic import BaseModel

import scheduler
import pandas as pd
import os
import uuid
import re
from datetime import datetime
from io import BytesIO
import sys
import glob
import requests
import json

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
from meeting_validator import validate_meeting_form
from ftth_acquisition_validator import validate_ftth_acquisition
from od_operation_validator import validate_od_operation
from leave_form_validator import validate_leave_form
from eb_meter_validator import validate_eb_meter
from od_survey_validator import validate_od_survey

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

app.include_router(site_monitoring.router)

from scheduler import start_scheduler

@app.on_event("startup")
def startup_event():
    start_scheduler()

UPLOAD_FOLDER = "uploads"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

GENERATED_FOLDER = "generated_files"
os.makedirs(GENERATED_FOLDER, exist_ok=True)

SITE_API_URL = "https://cm.shrotitele.com/user_management/api/tpms-tracker/?api_key=MySecretKey@2025"

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
# DYNAMIC VALIDATION ENGINE
# =====================================================

def apply_dynamic_validation(df, rules):
    valid_rows = []
    invalid_rows = []

    for _, row in df.iterrows():
        is_valid = True

        for col, rule in rules.items():
            value = str(row.get(col, "")).strip()

            # REQUIRED CHECK
            if rule.get("required") and not value:
                is_valid = False

            # TYPE CHECK
            if rule.get("type") == "number":
                if value and not value.isdigit():
                    is_valid = False

            if rule.get("type") == "email":
                if value and "@" not in value:
                    is_valid = False

            # LENGTH CHECK
            if "length" in rule:
                if value and len(value) != rule["length"]:
                    is_valid = False

        if is_valid:
            valid_rows.append(row)
        else:
            invalid_rows.append(row)

    return pd.DataFrame(valid_rows), pd.DataFrame(invalid_rows)

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

    for col in possible_cols:

        if col in row:

            value = row[col]

            if isinstance(value, str):
                value = value.strip()

            if value:
                return value

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

    if not file.filename:
        raise HTTPException(400, "No file uploaded")

    if not file.filename.lower().endswith((".csv", ".xlsx", ".xls")):
        raise HTTPException(400, "Invalid file format")

    # =====================================================
    # READ FILE
    # =====================================================

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

    # =====================================================
    # VALIDATION SELECTOR
    # =====================================================

    try:

        if form_type == "Meeting Form":
            valid_df, junk_df = validate_meeting_form(df)

        elif form_type == "Site Survey Checklist":
            valid_df, junk_df = validate_site_survey_checklist(df)

        elif form_type == "FTTH Acquisition Form":
            valid_df, junk_df = validate_ftth_acquisition(df)
        
        elif form_type == "OD Operation Form":
            valid_df, junk_df = validate_od_operation(df)

        elif form_type == "Leave Form":
            valid_df, junk_df = validate_leave_form(df)

        elif form_type == "EB Meter Form":
            valid_df, junk_df = validate_eb_meter(df)
        
        elif form_type == "OD Survey Form":
            valid_df, junk_df = validate_od_survey(df)

        else:
            raise HTTPException(400, f"Unsupported form type: {form_type}")

    except Exception as e:
        raise HTTPException(400, f"Validation pipeline failed: {str(e)}")

    valid_df["status"] = "valid"
    junk_df["status"] = "invalid"

    combined_df = pd.concat([valid_df, junk_df], ignore_index=True)

    # =====================================================
    # DUPLICATE FORM CHECK
    # =====================================================

    previous_upload = db.query(models.UploadHistory).filter(
        models.UploadHistory.form_type == form_type,
        models.UploadHistory.selected_date == str(selected_date)
    ).first()

    message = "File uploaded successfully"

    if previous_upload:

        message = "This form has been uploaded before. Old data replaced."

        db.query(models.FormEntry).filter(
            models.FormEntry.form_type == form_type,
            models.FormEntry.selected_date == selected_date
        ).delete()

        db.query(models.UploadHistory).filter(
            models.UploadHistory.form_type == form_type,
            models.UploadHistory.selected_date == str(selected_date)
        ).delete()

        db.commit()

    # =====================================================
    # SAVE OUTPUT FILES
    # =====================================================

    file_id = str(uuid.uuid4())

    valid_file = os.path.join(UPLOAD_FOLDER, f"{file_id}_valid.csv")
    junk_file = os.path.join(UPLOAD_FOLDER, f"{file_id}_junk.csv")

    valid_df.to_csv(valid_file, index=False)
    junk_df.to_csv(junk_file, index=False)

    # =====================================================
    # DATABASE INSERT
    # =====================================================

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
            "message": message,
            "total_rows": len(df),
            "valid_rows": len(valid_df),
            "junk_rows": len(junk_df)
        }

    except Exception as e:
        db.rollback()
        raise HTTPException(500, str(e))



# ================================
# 🔗 PUT YOUR API LINKS HERE
# ================================
SITE_API_URL = "https://cm.shrotitele.com/user_management/api/tpms-tracker/?api_key=MySecretKey@2025"
ALARM_API_URL = "https://cm.shrotitele.com/user_management/alarm-data/"


# ================================
# FETCH ALL SITES
# ================================
def fetch_all_sites():
    try:
        res = requests.get(SITE_API_URL)
        data = res.json().get("data", [])
        return data
    except Exception as e:
        print("Site API Error:", e)
        return []


# ================================
# FETCH ALARM DATA (DYNAMIC)
# ================================
def fetch_alarm_data(start_date=None, end_date=None, imei=None):

    params = {}

    if start_date and end_date:
        params["start_date"] = start_date
        params["end_date"] = end_date

    if imei:
        params["imei"] = imei

    try:
        res = requests.get(ALARM_API_URL, params=params)
        return res.json().get("data", [])
    except Exception as e:
        print("Alarm API Error:", e)
        return []


# ================================
# BUILD SITE MONITORING
# ================================
def build_site_monitoring(start_date=None, end_date=None, site_id=None):

    sites = fetch_all_sites()
    alarms = fetch_alarm_data(start_date, end_date)

    # 🔥 Create IMEI → alarm mapping
    alarm_map = {}

    for alarm in alarms:
        imei = str(alarm.get("imei")).strip()

        if imei not in alarm_map:
            alarm_map[imei] = []

        alarm_map[imei].append(alarm)

    up_sites = []
    down_sites = []

    for site in sites:

        imei = str(site.get("gsm_imei_no")).strip()
        site_name = site.get("site_name")
        global_id = site.get("globel_id")

        site_alarms = alarm_map.get(imei, [])

        # 🔥 Decide status
        if site_alarms:
            latest_alarm = sorted(
                site_alarms,
                key=lambda x: x.get("start_time", ""),
                reverse=True
            )[0]

            down_sites.append({
                "site_name": site_name,
                "global_id": global_id,
                "imei": imei,
                "status": "DOWN",
                "alarm": latest_alarm.get("alarm_name"),
                "since": latest_alarm.get("start_time"),
                "end_time": latest_alarm.get("end_time")
            })

        else:
            up_sites.append({
                "site_name": site_name,
                "global_id": global_id,
                "imei": imei,
                "status": "UP",
                "since": "Running"
            })

    return sites, up_sites, down_sites


# ================================
# SAVE SITE MONITORING
# ================================
def save_site_monitoring_to_db(db: Session, up_sites, down_sites):

    try:
        # 🔥 Purana data delete (fresh snapshot ke liye)
        db.query(models.SiteMonitoring).delete()

        # 🔥 Insert UP sites
        for site in up_sites:
            db.add(models.SiteMonitoring(
                site_name=site.get("site_name"),
                global_id=site.get("global_id"),
                circle="UNKNOWN",
                status="Active",
                alarm=None,
                since=site.get("since"),
                end_time=None
            ))

        # 🔥 Insert DOWN sites
        for site in down_sites:
            db.add(models.SiteMonitoring(
                site_name=site.get("site_name"),
                global_id=site.get("global_id"),
                circle="UNKNOWN",
                status="Outage",
                alarm=site.get("alarm"),
                since=site.get("since"),
                end_time=site.get("end_time")
            ))

        db.commit()
        print("✅ Site monitoring data saved to DB")

    except Exception as e:
        db.rollback()
        print("❌ DB Save Error:", str(e))


# ================================
# MAIN API (FILTER SUPPORT)
# ================================
@app.get("/SITE-MONITORING")
def site_monitoring(
    start_date: str = None,
    end_date: str = None,
    db: Session = Depends(get_db)
):

    total, up, down = build_site_monitoring(start_date, end_date)

    # 🔥 SAVE TO DB
    save_site_monitoring_to_db(db, up, down)

    return {
        "total_sites": len(total),
        "up_sites": len(up),
        "down_sites": len(down)
    }


# ================================
# GET DOWN SITES
# ================================
@app.get("/SITE-DOWN")
def site_down(start_date: str = None, end_date: str = None):

    _, _, down = build_site_monitoring(start_date, end_date)
    return down


# ================================
# GET UP SITES
# ================================
@app.get("/SITE-UP")
def site_up(start_date: str = None, end_date: str = None):

    _, up, _ = build_site_monitoring(start_date, end_date)
    return up


# =====================================================
# ANALYTICS
# =====================================================

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
        circle = str(r.circle or "UNKNOWN").strip()

        if username not in analytics:
            analytics[username] = {
                "username": username,
                "forms": {}
            }

        if form_type not in analytics[username]["forms"]:
            analytics[username]["forms"][form_type] = {
                "valid": 0,
                "invalid": 0,
                "total": 0,
                "circleWise": {}
            }

        form_data = analytics[username]["forms"][form_type]

        # VALID / INVALID
        if str(r.row_status).lower() == "valid":
            form_data["valid"] += 1
        else:
            form_data["invalid"] += 1

        # ⭐ CIRCLE WISE COUNT
        if circle not in form_data["circleWise"]:
            form_data["circleWise"][circle] = 0

        form_data["circleWise"][circle] += 1

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
        "total_forms": len(data),
        "total_rows": sum(e.total_rows or 0 for e in data),
        "valid_rows": sum(e.valid_rows or 0 for e in data),
        "junk_rows": sum(e.junk_rows or 0 for e in data)
    }


# =====================================================
# DOWNLOAD FILE
# =====================================================

@app.get("/DOWNLOAD")
async def download_file(path: str, filename: str = None):
    """
    Downloads a file.
    - path     : actual file path on disk
    - filename : (optional) the display name the browser should save it as
    """
    if not os.path.exists(path):
        raise HTTPException(404, "File not found")

    # Use the provided display filename if given, else fall back to basename
    display_name = filename if filename else os.path.basename(path)

    # Ensure it ends with .xlsx
    if not display_name.lower().endswith(".xlsx"):
        display_name += ".xlsx"

    return FileResponse(
        path,
        filename=display_name,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
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


# ============================
# CREATE FILE MODEL
# ============================

from typing import List, Dict, Optional
from pydantic import BaseModel

class CreateFileRequest(BaseModel):
    form_name: str
    columns: List[str]
    rules: Optional[Dict] = {}

# =====================================================
# CREATE DYNAMIC FILE (TEMPLATE GENERATOR)
# =====================================================

from openpyxl import Workbook
from openpyxl.worksheet.datavalidation import DataValidation

@app.post("/CREATE-FILE")
def create_dynamic_file(data: CreateFileRequest):
    try:
        wb = Workbook()
        ws = wb.active

        # ✅ FIX: Name the sheet after the form
        ws.title = data.form_name

        columns = data.columns
        rules = data.rules

        # Add headers
        for col_idx, col in enumerate(columns, 1):
            ws.cell(row=1, column=col_idx).value = col

        # 🔥 APPLY VALIDATIONS
        for col_idx, col in enumerate(columns, 1):
            rule = rules.get(col, {})

            if rule.get("type") == "dropdown":
                options = rule.get("options", "")
                dv = DataValidation(
                    type="list",
                    formula1=f'"{options}"',
                    allow_blank=not rule.get("required", False),
                )
                ws.add_data_validation(dv)
                dv.add(f"{chr(64+col_idx)}2:{chr(64+col_idx)}100")

        # ✅ FIX: Save file using sanitized form_name instead of UUID
        # Strip any characters that are unsafe for filenames
        safe_name = re.sub(r'[\\/*?:"<>|]', "", data.form_name).strip()
        if not safe_name:
            safe_name = str(uuid.uuid4())  # fallback if name is empty after sanitizing

        file_path = os.path.join(GENERATED_FOLDER, f"{safe_name}.xlsx")

        # If a file with the same name already exists, add a short unique suffix
        if os.path.exists(file_path):
            suffix = str(uuid.uuid4())[:8]
            file_path = os.path.join(GENERATED_FOLDER, f"{safe_name}_{suffix}.xlsx")

        wb.save(file_path)

        # ✅ FIX: Pass the form_name as the display filename in the download URL
        return {
            "download_url": f"/DOWNLOAD?path={file_path}&filename={safe_name}.xlsx"
        }

    except Exception as e:
        raise HTTPException(500, str(e))