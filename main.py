# =====================================================
# FASTAPI — FULLY DYNAMIC VERSION
# =====================================================

from fastapi import FastAPI, File, UploadFile, HTTPException, Form, Depends, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from sqlalchemy.orm import Session
from sqlalchemy import extract
from pydantic import BaseModel as PydanticBaseModel
from typing import List, Dict, Optional
from routes import site_monitoring

import scheduler
import pandas as pd
import os
import uuid
import re
import requests
import json
from datetime import datetime
from io import BytesIO

# =====================================================
# DATABASE
# =====================================================

from database import SessionLocal, engine
import models

# =====================================================
# AUTO CREATE TABLES (runs on startup)
# =====================================================

models.Base.metadata.create_all(bind=engine)

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

# =====================================================
# REPORT UPLOAD FOLDER — daily files for email reports
# =====================================================

REPORT_DAILY_DIR = "data/daily"
os.makedirs(REPORT_DAILY_DIR, exist_ok=True)

REPORT_FILE_MAP = {
    "employee":    "employee.xlsx",
    "attendance":  "attendance.xlsx",
    "distance":    "distance.xlsx",
    "forms":       "forms.xlsx",
    "managers":    "managers.xlsx",
    "forms_filled":"forms_filled.xlsx",
    "alarm":       "alarm.csv",
}

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
    valid_rows   = []
    invalid_rows = []

    for _, row in df.iterrows():
        is_valid = True

        for col, rule in rules.items():

            # Normalize column name to match safe_clean_df output
            normalized_col = (
                col.strip().lower()
                .replace("\n", " ").replace("\r", "")
                .replace("_", " ").replace("-", " ")
            )
            normalized_col = re.sub(r"\s+", " ", normalized_col)

            value = str(row.get(normalized_col, "")).strip()

            # ── REQUIRED CHECK ──────────────────────────────
            if rule.get("required") and not value:
                is_valid = False

            # ── UUID ────────────────────────────────────────
            if rule.get("type") == "uuid":
                if value:
                    try:
                        import uuid as _uuid
                        _uuid.UUID(value)
                    except (ValueError, AttributeError):
                        is_valid = False

            # ── SYSTEM ID ────────────────────────────────────
            if rule.get("type") == "system_id":
                if value:
                    prefixes = [p.strip() for p in rule.get("allowed_prefixes", "").split(",") if p.strip()]
                    if prefixes:
                        if not any(value.upper().startswith(p.upper()) for p in prefixes):
                            is_valid = False
                    else:
                        # Generic system ID: must have at least one hyphen and end with alphanumeric
                        if not re.match(r'^[A-Za-z0-9]{1,10}(-[A-Za-z0-9]{1,10}){1,}$', value):
                            is_valid = False

            # ── USERNAME ─────────────────────────────────────
            # username: alphanumeric, dots, underscores, hyphens; no spaces; 3-50 chars
            if rule.get("type") == "username":
                if value:
                    if not re.match(r'^[A-Za-z0-9._\-]{3,50}$', value):
                        is_valid = False

            # ── PHONE ────────────────────────────────────────
            if rule.get("type") == "phone":
                if value:
                    digits = re.sub(r'\D', '', value)
                    fmt = rule.get("phone_format", "any")
                    if fmt == "india_10":
                        if len(digits) != 10 or digits[0] not in "6789":
                            is_valid = False
                    elif fmt == "india_with_code":
                        if not (len(digits) == 12 and digits.startswith("91") and digits[2] in "6789"):
                            is_valid = False
                    elif fmt == "international":
                        if not (7 <= len(digits) <= 15):
                            is_valid = False
                    else:  # "any"
                        if len(digits) < 7:
                            is_valid = False

            # ── PINCODE ──────────────────────────────────────
            if rule.get("type") == "pincode":
                if value:
                    digits = re.sub(r'\D', '', value)
                    fmt = rule.get("pincode_format", "any_numeric")
                    if fmt == "india_6":
                        if len(digits) != 6:
                            is_valid = False
                    elif fmt == "us_zip":
                        if len(digits) not in (5, 9):
                            is_valid = False
                    else:  # any_numeric
                        if not value.replace("-", "").isdigit() or len(digits) < 3:
                            is_valid = False

            # ── TEXT / (other non-validated types) ──────────
            # No special validation — just required check applies

            # ── NUMBER ──────────────────────────────────────
            if rule.get("type") == "number":
                if value and not value.replace(".", "", 1).lstrip("-").isdigit():
                    is_valid = False
                if value:
                    try:
                        n  = float(value)
                        mn = rule.get("min")
                        mx = rule.get("max")
                        if mn != "" and mn is not None and n < float(mn):
                            is_valid = False
                        if mx != "" and mx is not None and n > float(mx):
                            is_valid = False
                    except ValueError:
                        is_valid = False

            # ── METER READING ────────────────────────────────
            if rule.get("type") == "meter_reading":
                if value:
                    if not value.isdigit():
                        is_valid = False
                    else:
                        try:
                            n  = int(value)
                            mn = rule.get("min")
                            mx = rule.get("max")
                            if mn != "" and mn is not None and n < int(mn):
                                is_valid = False
                            if mx != "" and mx is not None and n > int(mx):
                                is_valid = False
                        except ValueError:
                            is_valid = False

            # ── CONSUMPTION ──────────────────────────────────
            if rule.get("type") == "consumption":
                if value and not value.replace(".", "", 1).lstrip("-").isdigit():
                    is_valid = False

            # ── INR RATE ─────────────────────────────────────
            if rule.get("type") == "inr_rate":
                if value:
                    cleaned = value.replace("₹", "").replace(",", "").strip()
                    try:
                        float(cleaned)
                    except ValueError:
                        is_valid = False

            # ── INR AMOUNT ───────────────────────────────────
            if rule.get("type") == "inr_amount":
                if value:
                    cleaned = value.replace("₹", "").replace(",", "").strip()
                    try:
                        amt = float(cleaned)
                        mn  = rule.get("min")
                        mx  = rule.get("max")
                        if mn != "" and mn is not None and amt < float(mn):
                            is_valid = False
                        if mx != "" and mx is not None and amt > float(mx):
                            is_valid = False
                    except ValueError:
                        is_valid = False

            # ── DATETIME ─────────────────────────────────────
            if rule.get("type") == "datetime":
                if value:
                    parsed = pd.to_datetime(value, errors="coerce", dayfirst=True)
                    if pd.isna(parsed):
                        is_valid = False

            # ── DATE ─────────────────────────────────────────
            if rule.get("type") == "date":
                if value:
                    parsed = pd.to_datetime(value, errors="coerce", dayfirst=True)
                    if pd.isna(parsed):
                        is_valid = False

            # ── APPROVAL FLAG ────────────────────────────────
            if rule.get("type") == "approval_flag":
                if value:
                    true_vals  = [v.strip().lower() for v in rule.get("true_values",  "").split(",") if v.strip()]
                    false_vals = [v.strip().lower() for v in rule.get("false_values", "").split(",") if v.strip()]
                    all_vals   = true_vals + false_vals
                    if all_vals and value.lower() not in all_vals:
                        is_valid = False

            # ── DROPDOWN ─────────────────────────────────────
            if rule.get("type") == "dropdown":
                options_raw = rule.get("options", "")
                if options_raw and value:
                    options = [o.strip().lower() for o in options_raw.split(",")]
                    if value.lower() not in options:
                        is_valid = False

            # ── EMAIL ────────────────────────────────────────
            if rule.get("type") == "email":
                if value:
                    # Check basic email format: local@domain.tld
                    if not re.match(r'^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$', value):
                        is_valid = False
                    else:
                        domains = [d.strip().lower() for d in rule.get("allowed_domains", "").split(",") if d.strip()]
                        if domains:
                            domain_part = value.split("@", 1)[1].lower()
                            if domain_part not in domains:
                                is_valid = False

            # ── LAT/LONG JSON ────────────────────────────────
            if rule.get("type") == "latlong_json":
                if value:
                    try:
                        geo    = json.loads(value)
                        coords = geo.get("coordinates")
                        if not isinstance(coords, list) or len(coords) != 2:
                            is_valid = False
                        else:
                            float(coords[0])
                            float(coords[1])
                    except Exception:
                        is_valid = False

            # ── LAT/LONG TEXT ────────────────────────────────
            if rule.get("type") == "latlong_text":
                if value:
                    parts = re.split(r"[,\s]+", value.strip())
                    try:
                        if len(parts) != 2:
                            is_valid = False
                        else:
                            float(parts[0])
                            float(parts[1])
                    except Exception:
                        is_valid = False

            # ── LATITUDE ─────────────────────────────────────
            if rule.get("type") == "latitude":
                if value:
                    try:
                        v = float(value)
                        if not (-90 <= v <= 90):
                            is_valid = False
                    except ValueError:
                        is_valid = False

            # ── LONGITUDE ────────────────────────────────────
            if rule.get("type") == "longitude":
                if value:
                    try:
                        v = float(value)
                        if not (-180 <= v <= 180):
                            is_valid = False
                    except ValueError:
                        is_valid = False

            # ── LENGTH CHECK ─────────────────────────────────
            if "length" in rule:
                if value and len(value) != rule["length"]:
                    is_valid = False

        if is_valid:
            valid_rows.append(row)
        else:
            invalid_rows.append(row)

    return pd.DataFrame(valid_rows), pd.DataFrame(invalid_rows)

# =====================================================
# USERNAME EXTRACTION — fully dynamic
# Tries common username column names in order
# =====================================================

def extract_username(row):
    possible_cols = ["createduser", "created user", "user name", "username", "operator"]
    for col in possible_cols:
        if col in row:
            value = row[col]
            if isinstance(value, str):
                value = value.strip()
            if value:
                return value
    return "UNKNOWN_USER"

# =====================================================
# VALIDATE FORM API — 100% dynamic, no hardcoding
# =====================================================

@app.post("/VALIDATE-FORM")
async def validate_form(
    file:      UploadFile = File(...),
    form_type: str        = Form(...),
    date:      str        = Form(...),
    db:        Session    = Depends(get_db)
):
    form_type     = form_type.strip()
    selected_date = pd.to_datetime(date, errors="coerce").date()

    if not file.filename:
        raise HTTPException(400, "No file uploaded")

    if not file.filename.lower().endswith((".csv", ".xlsx", ".xls")):
        raise HTTPException(400, "Invalid file format. Use CSV or Excel.")

    # ── READ FILE ──────────────────────────────────────
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
            raise HTTPException(400, "File has no data rows")

        df = safe_clean_df(df)

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(400, f"File read error: {str(e)}")

    # ── FETCH RULES FROM DB ────────────────────────────
    try:
        config = db.query(models.FormTemplate).filter(
            models.FormTemplate.form_name == form_type
        ).first()

        if not config:
            raise HTTPException(
                400,
                f"No validation rules found for '{form_type}'. "
                f"Please create this form first from the Create Form page."
            )

        rules_dict        = json.loads(config.rules or "{}")
        valid_df, junk_df = apply_dynamic_validation(df, rules_dict)

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(400, f"Validation failed: {str(e)}")

    valid_df = valid_df.copy()
    junk_df  = junk_df.copy()

    valid_df["status"] = "valid"
    junk_df["status"]  = "invalid"

    combined_df = pd.concat([valid_df, junk_df], ignore_index=True)

    # ── DUPLICATE UPLOAD CHECK ─────────────────────────
    previous_upload = db.query(models.UploadHistory).filter(
        models.UploadHistory.form_type     == form_type,
        models.UploadHistory.selected_date == str(selected_date)
    ).first()

    message = "File uploaded successfully"

    if previous_upload:
        message = "This form was uploaded before. Old data replaced."

        db.query(models.FormEntry).filter(
            models.FormEntry.form_type     == form_type,
            models.FormEntry.selected_date == selected_date
        ).delete()

        db.query(models.UploadHistory).filter(
            models.UploadHistory.form_type     == form_type,
            models.UploadHistory.selected_date == str(selected_date)
        ).delete()

        db.commit()

    # ── SAVE CSV FILES ─────────────────────────────────
    file_id    = str(uuid.uuid4())
    valid_file = os.path.join(UPLOAD_FOLDER, f"{file_id}_valid.csv")
    junk_file  = os.path.join(UPLOAD_FOLDER, f"{file_id}_junk.csv")

    valid_df.to_csv(valid_file, index=False)
    junk_df.to_csv(junk_file,  index=False)

    # ── DB INSERT ──────────────────────────────────────
    try:
        for _, row in combined_df.iterrows():
            db.add(models.FormEntry(
                form_type     = form_type,
                username      = extract_username(row),
                selected_date = selected_date,
                row_status    = row.get("status", "invalid"),
                circle        = str(row.get("circle", "UNKNOWN"))
            ))

        db.add(models.UploadHistory(
            file_name     = file.filename,
            form_type     = form_type,
            selected_date = str(selected_date),
            total_rows    = len(df),
            valid_rows    = len(valid_df),
            junk_rows     = len(junk_df),
            valid_file    = valid_file,
            junk_file     = junk_file
        ))

        db.commit()

        return {
            "status":     "success",
            "message":    message,
            "total_rows": len(df),
            "valid_rows": len(valid_df),
            "junk_rows":  len(junk_df)
        }

    except Exception as e:
        db.rollback()
        raise HTTPException(500, str(e))


# =====================================================
# PYDANTIC MODEL
# =====================================================

class CreateFileRequest(PydanticBaseModel):
    form_name: str
    columns:   List[str]
    rules:     Optional[Dict] = {}


# =====================================================
# SAVE FORM RULES
# =====================================================

@app.post("/SAVE-FORM-RULES")
def save_form_rules(data: CreateFileRequest, db: Session = Depends(get_db)):
    try:
        existing = db.query(models.FormTemplate).filter(
            models.FormTemplate.form_name == data.form_name
        ).first()

        if existing:
            existing.columns = json.dumps(data.columns)
            existing.rules   = json.dumps(data.rules)
        else:
            db.add(models.FormTemplate(
                form_name = data.form_name,
                columns   = json.dumps(data.columns),
                rules     = json.dumps(data.rules),
            ))

        db.commit()
        return {"status": "saved", "form_name": data.form_name}

    except Exception as e:
        db.rollback()
        raise HTTPException(500, str(e))


# =====================================================
# GET ALL FORM NAMES — only from DB, no hardcoding
# =====================================================

@app.get("/GET-FORM-NAMES")
def get_form_names(db: Session = Depends(get_db)):
    return [r.form_name for r in db.query(models.FormTemplate).all()]


# =====================================================
# GET FORM RULES
# =====================================================

@app.get("/GET-FORM-RULES")
def get_form_rules(form_name: str, db: Session = Depends(get_db)):
    config = db.query(models.FormTemplate).filter(
        models.FormTemplate.form_name == form_name
    ).first()

    if not config:
        raise HTTPException(404, f"No rules found for form '{form_name}'")

    return {
        "form_name": config.form_name,
        "columns":   json.loads(config.columns or "[]"),
        "rules":     json.loads(config.rules    or "{}")
    }


# =====================================================
# SITE MONITORING
# =====================================================

SITE_API_URL  = "https://cm.shrotitele.com/user_management/api/tpms-tracker/?api_key=MySecretKey@2025"
ALARM_API_URL = "https://cm.shrotitele.com/user_management/alarm-data/"


def fetch_all_sites():
    try:
        return requests.get(SITE_API_URL).json().get("data", [])
    except Exception as e:
        print("Site API Error:", e)
        return []


def fetch_alarm_data(start_date=None, end_date=None, imei=None):
    params = {}
    if start_date and end_date:
        params["start_date"] = start_date
        params["end_date"]   = end_date
    if imei:
        params["imei"] = imei
    try:
        return requests.get(ALARM_API_URL, params=params).json().get("data", [])
    except Exception as e:
        print("Alarm API Error:", e)
        return []


def build_site_monitoring(start_date=None, end_date=None):
    sites  = fetch_all_sites()
    alarms = fetch_alarm_data(start_date, end_date)

    alarm_map = {}
    for alarm in alarms:
        imei = str(alarm.get("imei")).strip()
        alarm_map.setdefault(imei, []).append(alarm)

    up_sites   = []
    down_sites = []

    for site in sites:
        imei      = str(site.get("gsm_imei_no")).strip()
        site_name = site.get("site_name")
        global_id = site.get("globel_id")

        site_alarms = alarm_map.get(imei, [])

        if site_alarms:
            latest = sorted(site_alarms, key=lambda x: x.get("start_time", ""), reverse=True)[0]
            down_sites.append({
                "site_name": site_name,
                "global_id": global_id,
                "imei":      imei,
                "status":    "DOWN",
                "alarm":     latest.get("alarm_name"),
                "since":     latest.get("start_time"),
                "end_time":  latest.get("end_time")
            })
        else:
            up_sites.append({
                "site_name": site_name,
                "global_id": global_id,
                "imei":      imei,
                "status":    "UP",
                "since":     "Running"
            })

    return sites, up_sites, down_sites


def save_site_monitoring_to_db(db: Session, up_sites, down_sites):
    try:
        db.query(models.SiteMonitoring).delete()

        for site in up_sites:
            db.add(models.SiteMonitoring(
                site_name = site.get("site_name"),
                global_id = site.get("global_id"),
                circle    = "UNKNOWN",
                status    = "Active",
                alarm     = None,
                since     = site.get("since"),
                end_time  = None
            ))

        for site in down_sites:
            db.add(models.SiteMonitoring(
                site_name = site.get("site_name"),
                global_id = site.get("global_id"),
                circle    = "UNKNOWN",
                status    = "Outage",
                alarm     = site.get("alarm"),
                since     = site.get("since"),
                end_time  = site.get("end_time")
            ))

        db.commit()
        print("✅ Site monitoring saved to DB")

    except Exception as e:
        db.rollback()
        print("❌ DB Save Error:", str(e))


@app.get("/SITE-MONITORING")
def site_monitoring_api(
    start_date: str = None,
    end_date:   str = None,
    db: Session = Depends(get_db)
):
    total, up, down = build_site_monitoring(start_date, end_date)
    save_site_monitoring_to_db(db, up, down)
    return {
        "total_sites": len(total),
        "up_sites":    len(up),
        "down_sites":  len(down)
    }


@app.get("/SITE-DOWN")
def site_down(start_date: str = None, end_date: str = None):
    _, _, down = build_site_monitoring(start_date, end_date)
    return down


@app.get("/SITE-UP")
def site_up(start_date: str = None, end_date: str = None):
    _, up, _ = build_site_monitoring(start_date, end_date)
    return up


# =====================================================
# ANALYTICS
# =====================================================

@app.get("/ANALYTICS")
def get_analytics(
    month: Optional[str] = Query(None),
    db:    Session       = Depends(get_db)
):
    query = db.query(models.FormEntry)

    if month:
        try:
            year, month_num = month.split("-")
            query = query.filter(
                extract("year",  models.FormEntry.selected_date) == int(year),
                extract("month", models.FormEntry.selected_date) == int(month_num)
            )
        except:
            pass

    analytics = {}

    for r in query.all():
        username  = str(r.username  or "UNKNOWN_USER").strip()
        form_type = str(r.form_type)
        circle    = str(r.circle    or "UNKNOWN").strip()

        analytics.setdefault(username, {"username": username, "forms": {}})
        analytics[username]["forms"].setdefault(form_type, {
            "valid": 0, "invalid": 0, "total": 0, "circleWise": {}
        })

        fd = analytics[username]["forms"][form_type]

        if str(r.row_status).lower() == "valid":
            fd["valid"] += 1
        else:
            fd["invalid"] += 1

        fd["circleWise"].setdefault(circle, 0)
        fd["circleWise"][circle] += 1

    for username in analytics:
        for form in analytics[username]["forms"]:
            fd = analytics[username]["forms"][form]
            fd["total"] = fd["valid"] + fd["invalid"]

    return list(analytics.values())


# =====================================================
# FORM DATA FETCH
# =====================================================

@app.get("/FORM-DATA-MULTI")
def get_form_data_multi(forms: str, db: Session = Depends(get_db)):
    try:
        if forms == "ALL":
            results = db.query(models.FormEntry).order_by(models.FormEntry.id.desc()).all()
        else:
            form_list = [f.strip() for f in forms.split(",") if f.strip()]
            results   = db.query(models.FormEntry)\
                          .filter(models.FormEntry.form_type.in_(form_list))\
                          .order_by(models.FormEntry.id.desc()).all()

        return [
            {
                "form_type": r.form_type,
                "username":  r.username,
                "status":    r.row_status,
                "date":      str(r.selected_date)
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
        "total_rows":  sum(e.total_rows or 0 for e in data),
        "valid_rows":  sum(e.valid_rows  or 0 for e in data),
        "junk_rows":   sum(e.junk_rows   or 0 for e in data)
    }


# =====================================================
# DOWNLOAD FILE
# =====================================================

@app.get("/DOWNLOAD")
async def download_file(path: str, filename: str = None):
    if not os.path.exists(path):
        raise HTTPException(404, "File not found")

    display_name = filename if filename else os.path.basename(path)

    if not display_name.lower().endswith(".xlsx"):
        display_name += ".xlsx"

    return FileResponse(
        path,
        filename   = display_name,
        media_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )


# =====================================================
# UPLOAD HISTORY
# =====================================================

@app.get("/UPLOAD-HISTORY")
def get_upload_history(db: Session = Depends(get_db)):
    history = db.query(models.UploadHistory).order_by(models.UploadHistory.id.desc()).all()

    return [
        {
            "file_name":   h.file_name,
            "form_type":   h.form_type,
            "upload_time": str(h.upload_time),
            "total_rows":  h.total_rows or 0,
            "valid_rows":  h.valid_rows  or 0,
            "junk_rows":   h.junk_rows   or 0
        }
        for h in history
    ]


# =====================================================
# INVALID RECORDS
# =====================================================

@app.get("/INVALID-RECORDS")
def get_invalid_records(form_name: str, db: Session = Depends(get_db)):
    try:
        # Get the most recent upload for this form
        history = db.query(models.UploadHistory).filter(
            models.UploadHistory.form_type == form_name
        ).order_by(models.UploadHistory.id.desc()).first()

        if not history:
            return []

        junk_file = history.junk_file

        if not junk_file or not os.path.exists(junk_file):
            return []

        # Read the junk CSV
        junk_df = pd.read_csv(junk_file, dtype=str).fillna("")

        if len(junk_df) == 0:
            return []

        # Get the rules for this form to know what each column expects
        config = db.query(models.FormTemplate).filter(
            models.FormTemplate.form_name == form_name
        ).first()

        rules_dict = json.loads(config.rules or "{}") if config else {}

        records = []

        for idx, row in junk_df.iterrows():
            # Skip the status column
            row_data = {k: v for k, v in row.items() if k != "status"}
            errors = []

            for col, rule in rules_dict.items():
                # Normalize column name same way as safe_clean_df
                normalized_col = (
                    col.strip().lower()
                    .replace("\n", " ").replace("\r", "")
                    .replace("_", " ").replace("-", " ")
                )
                normalized_col = re.sub(r"\s+", " ", normalized_col)

                value = str(row.get(normalized_col, "")).strip()

                reason = None

                if rule.get("required") and not value:
                    reason = f"'{col}' is required but empty"

                elif rule.get("type") == "uuid" and value:
                    try:
                        import uuid as _uuid
                        _uuid.UUID(value)
                    except (ValueError, AttributeError):
                        reason = f"'{value}' is not a valid UUID"

                elif rule.get("type") == "username" and value:
                    if not re.match(r"^[A-Za-z0-9._\-]{3,50}$", value):
                        reason = f"'{value}' is not a valid username"

                elif rule.get("type") == "system_id" and value:
                    prefixes = [p.strip() for p in rule.get("allowed_prefixes", "").split(",") if p.strip()]
                    if prefixes:
                        if not any(value.upper().startswith(p.upper()) for p in prefixes):
                            reason = f"'{value}' does not start with allowed prefix: {', '.join(prefixes)}"
                    else:
                        if not re.match(r"^[A-Za-z0-9]{1,10}(-[A-Za-z0-9]{1,10}){1,}$", value):
                            reason = f"'{value}' is not a valid system ID (expected format: ABC-123)"

                elif rule.get("type") == "number" and value:
                    if not value.replace(".", "", 1).lstrip("-").isdigit():
                        reason = f"'{value}' is not a valid number"
                    else:
                        try:
                            n = float(value)
                            mn = rule.get("min")
                            mx = rule.get("max")
                            if mn not in ("", None) and n < float(mn):
                                reason = f"Value {value} is below minimum ({mn})"
                            if mx not in ("", None) and n > float(mx):
                                reason = f"Value {value} exceeds maximum ({mx})"
                        except ValueError:
                            reason = f"'{value}' could not be parsed as a number"

                elif rule.get("type") == "meter_reading" and value:
                    if not value.isdigit():
                        reason = f"'{value}' is not a valid meter reading — expected a whole number"

                elif rule.get("type") in ("datetime", "date") and value:
                    parsed = pd.to_datetime(value, errors="coerce", dayfirst=True)
                    if pd.isna(parsed):
                        reason = f"'{value}' is not a valid date/time format"

                elif rule.get("type") == "email" and value:
                    if not re.match(r"^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$", value):
                        reason = f"'{value}' is not a valid email address"
                    else:
                        domains = [d.strip().lower() for d in rule.get("allowed_domains", "").split(",") if d.strip()]
                        if domains and value.split("@", 1)[1].lower() not in domains:
                            reason = f"'{value}' domain not in allowed: {', '.join(domains)}"

                elif rule.get("type") == "phone" and value:
                    digits = re.sub(r"\D", "", value)
                    fmt = rule.get("phone_format", "any")
                    if fmt == "india_10":
                        if len(digits) != 10 or digits[0] not in "6789":
                            reason = f"'{value}' is not a valid 10-digit Indian mobile number"
                    elif fmt == "india_with_code":
                        if not (len(digits) == 12 and digits.startswith("91") and digits[2] in "6789"):
                            reason = f"'{value}' is not a valid Indian number with country code"
                    elif fmt == "international":
                        if not (7 <= len(digits) <= 15):
                            reason = f"'{value}' is not a valid international phone number"
                    else:
                        if len(digits) < 7:
                            reason = f"'{value}' is not a valid phone number"

                elif rule.get("type") == "pincode" and value:
                    digits = re.sub(r"\D", "", value)
                    fmt = rule.get("pincode_format", "any_numeric")
                    if fmt == "india_6":
                        if len(digits) != 6:
                            reason = f"'{value}' is not a valid 6-digit Indian pincode"
                    elif fmt == "us_zip":
                        if len(digits) not in (5, 9):
                            reason = f"'{value}' is not a valid US ZIP code"
                    else:
                        if not value.replace("-", "").isdigit() or len(digits) < 3:
                            reason = f"'{value}' is not a valid pincode"

                elif rule.get("type") in ("consumption", "inr_rate") and value:
                    cleaned = value.replace("₹", "").replace(",", "").strip()
                    try:
                        float(cleaned)
                    except ValueError:
                        reason = f"'{value}' is not a valid numeric value"

                elif rule.get("type") == "inr_amount" and value:
                    cleaned = value.replace("₹", "").replace(",", "").strip()
                    try:
                        amt = float(cleaned)
                        mn = rule.get("min")
                        mx = rule.get("max")
                        if mn not in ("", None) and amt < float(mn):
                            reason = f"Amount {value} below minimum (₹{mn})"
                        if mx not in ("", None) and amt > float(mx):
                            reason = f"Amount {value} exceeds maximum (₹{mx})"
                    except ValueError:
                        reason = f"'{value}' is not a valid amount"

                elif rule.get("type") == "dropdown" and value:
                    options_raw = rule.get("options", "")
                    if options_raw:
                        options = [o.strip().lower() for o in options_raw.split(",")]
                        if value.lower() not in options:
                            reason = f"'{value}' is not in allowed options: {options_raw}"

                elif rule.get("type") == "approval_flag" and value:
                    true_vals  = [v.strip().lower() for v in rule.get("true_values",  "").split(",") if v.strip()]
                    false_vals = [v.strip().lower() for v in rule.get("false_values", "").split(",") if v.strip()]
                    all_vals   = true_vals + false_vals
                    if all_vals and value.lower() not in all_vals:
                        reason = f"'{value}' is not a valid approval value — expected one of: {', '.join(all_vals)}"

                elif rule.get("type") == "latitude" and value:
                    try:
                        v_float = float(value)
                        if not (-90 <= v_float <= 90):
                            reason = f"'{value}' is out of latitude range (-90 to 90)"
                    except ValueError:
                        reason = f"'{value}' is not a valid latitude"

                elif rule.get("type") == "longitude" and value:
                    try:
                        v_float = float(value)
                        if not (-180 <= v_float <= 180):
                            reason = f"'{value}' is out of longitude range (-180 to 180)"
                    except ValueError:
                        reason = f"'{value}' is not a valid longitude"

                elif rule.get("type") == "latlong_json" and value:
                    try:
                        geo = json.loads(value)
                        coords = geo.get("coordinates")
                        if not isinstance(coords, list) or len(coords) != 2:
                            reason = f"Invalid lat/long JSON structure"
                        else:
                            float(coords[0]); float(coords[1])
                    except Exception:
                        reason = f"'{value}' is not valid lat/long JSON"

                elif rule.get("type") == "latlong_text" and value:
                    parts = re.split(r"[,\s]+", value.strip())
                    try:
                        if len(parts) != 2:
                            reason = f"'{value}' must be two coordinates separated by comma/space"
                        else:
                            float(parts[0]); float(parts[1])
                    except Exception:
                        reason = f"'{value}' is not valid lat/long text"

                if not reason and "length" in rule and value:
                    if len(value) != rule["length"]:
                        reason = f"'{value}' must be exactly {rule['length']} characters"

                if reason:
                    errors.append({"field": col, "reason": reason})

            records.append({
                "row": idx + 2,  # +2: 1-based + header row
                "data": row_data,
                "errors": errors
            })

        return records

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# =====================================================
# INVALID RECORDS BY USER
# =====================================================

@app.get("/INVALID-RECORDS-BY-USER")
def get_invalid_records_by_user(form_name: str, db: Session = Depends(get_db)):
    try:
        # Get most recent upload for this form
        history = db.query(models.UploadHistory).filter(
            models.UploadHistory.form_type == form_name
        ).order_by(models.UploadHistory.id.desc()).first()

        if not history or not history.junk_file:
            return []

        if not os.path.exists(history.junk_file):
            return []

        junk_df = pd.read_csv(history.junk_file, dtype=str).fillna("")

        if len(junk_df) == 0:
            return []

        # Get rules for this form
        config = db.query(models.FormTemplate).filter(
            models.FormTemplate.form_name == form_name
        ).first()
        rules_dict = json.loads(config.rules or "{}") if config else {}

        # Try to find username column in junk_df
        possible_user_cols = [
            "createduser", "created user", "username",
            "user name", "operator", "created by"
        ]
        user_col = None
        for col in possible_user_cols:
            if col in junk_df.columns:
                user_col = col
                break

        # Group invalid rows by user
        user_map = {}

        for idx, row in junk_df.iterrows():
            # Get username
            if user_col:
                username = str(row.get(user_col, "")).strip() or "Unknown User"
            else:
                username = "Unknown User"

            errors = []

            for col, rule in rules_dict.items():
                normalized_col = (
                    col.strip().lower()
                    .replace("\n", " ").replace("\r", "")
                    .replace("_", " ").replace("-", " ")
                )
                normalized_col = re.sub(r"\s+", " ", normalized_col)

                value = str(row.get(normalized_col, "")).strip()
                reason = None

                if rule.get("required") and not value:
                    reason = "Required field is empty"

                elif rule.get("type") == "uuid" and value:
                    try:
                        import uuid as _uuid
                        _uuid.UUID(value)
                    except (ValueError, AttributeError):
                        reason = f"'{value}' is not a valid UUID"

                elif rule.get("type") == "username" and value:
                    if not re.match(r"^[A-Za-z0-9._\-]{3,50}$", value):
                        reason = f"'{value}' is not a valid username (3–50 alphanumeric/._- chars)"

                elif rule.get("type") == "system_id" and value:
                    prefixes = [p.strip() for p in rule.get("allowed_prefixes", "").split(",") if p.strip()]
                    if prefixes:
                        if not any(value.upper().startswith(p.upper()) for p in prefixes):
                            reason = f"'{value}' does not start with allowed prefix: {', '.join(prefixes)}"
                    else:
                        if not re.match(r"^[A-Za-z0-9]{1,10}(-[A-Za-z0-9]{1,10}){1,}$", value):
                            reason = f"'{value}' is not a valid system ID (expected format: ABC-123)"

                elif rule.get("type") == "number" and value:
                    if not value.replace(".", "", 1).lstrip("-").isdigit():
                        reason = f"'{value}' is not a valid number"
                    else:
                        try:
                            n = float(value)
                            mn, mx = rule.get("min"), rule.get("max")
                            if mn not in ("", None) and n < float(mn):
                                reason = f"Value {value} is below minimum ({mn})"
                            if mx not in ("", None) and n > float(mx):
                                reason = f"Value {value} exceeds maximum ({mx})"
                        except ValueError:
                            reason = f"'{value}' could not be parsed as number"

                elif rule.get("type") == "meter_reading" and value:
                    if not value.isdigit():
                        reason = f"'{value}' is not a valid meter reading"
                    else:
                        try:
                            n = int(value)
                            mn, mx = rule.get("min"), rule.get("max")
                            if mn not in ("", None) and n < int(mn):
                                reason = f"Value {value} is below minimum ({mn})"
                            if mx not in ("", None) and n > int(mx):
                                reason = f"Value {value} exceeds maximum ({mx})"
                        except ValueError:
                            pass

                elif rule.get("type") in ("datetime", "date") and value:
                    parsed = pd.to_datetime(value, errors="coerce", dayfirst=True)
                    if pd.isna(parsed):
                        reason = f"'{value}' is not a valid date format"

                elif rule.get("type") == "email" and value:
                    if not re.match(r"^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$", value):
                        reason = f"'{value}' is not a valid email"
                    else:
                        domains = [d.strip().lower() for d in rule.get("allowed_domains", "").split(",") if d.strip()]
                        if domains and value.split("@", 1)[1].lower() not in domains:
                            reason = f"'{value}' domain not in allowed: {', '.join(domains)}"

                elif rule.get("type") == "phone" and value:
                    digits = re.sub(r"\D", "", value)
                    fmt = rule.get("phone_format", "any")
                    if fmt == "india_10":
                        if len(digits) != 10 or digits[0] not in "6789":
                            reason = f"'{value}' is not a valid 10-digit Indian mobile number"
                    elif fmt == "india_with_code":
                        if not (len(digits) == 12 and digits.startswith("91") and digits[2] in "6789"):
                            reason = f"'{value}' is not a valid Indian number with country code"
                    elif fmt == "international":
                        if not (7 <= len(digits) <= 15):
                            reason = f"'{value}' is not a valid international phone number"
                    else:
                        if len(digits) < 7:
                            reason = f"'{value}' is not a valid phone number"

                elif rule.get("type") == "pincode" and value:
                    digits = re.sub(r"\D", "", value)
                    fmt = rule.get("pincode_format", "any_numeric")
                    if fmt == "india_6":
                        if len(digits) != 6:
                            reason = f"'{value}' is not a valid 6-digit Indian pincode"
                    elif fmt == "us_zip":
                        if len(digits) not in (5, 9):
                            reason = f"'{value}' is not a valid US ZIP code"
                    else:
                        if not value.replace("-", "").isdigit() or len(digits) < 3:
                            reason = f"'{value}' is not a valid pincode"

                elif rule.get("type") in ("consumption", "inr_rate") and value:
                    cleaned = value.replace("₹", "").replace(",", "").strip()
                    try:
                        float(cleaned)
                    except ValueError:
                        reason = f"'{value}' is not a valid numeric value"

                elif rule.get("type") == "inr_amount" and value:
                    cleaned = value.replace("₹", "").replace(",", "").strip()
                    try:
                        amt = float(cleaned)
                        mn, mx = rule.get("min"), rule.get("max")
                        if mn not in ("", None) and amt < float(mn):
                            reason = f"Amount {value} below minimum (₹{mn})"
                        if mx not in ("", None) and amt > float(mx):
                            reason = f"Amount {value} exceeds maximum (₹{mx})"
                    except ValueError:
                        reason = f"'{value}' is not a valid amount"

                elif rule.get("type") == "dropdown" and value:
                    options_raw = rule.get("options", "")
                    if options_raw:
                        options = [o.strip().lower() for o in options_raw.split(",")]
                        if value.lower() not in options:
                            reason = f"'{value}' not in allowed options: {options_raw}"

                elif rule.get("type") == "approval_flag" and value:
                    true_vals  = [v.strip().lower() for v in rule.get("true_values",  "").split(",") if v.strip()]
                    false_vals = [v.strip().lower() for v in rule.get("false_values", "").split(",") if v.strip()]
                    all_vals   = true_vals + false_vals
                    if all_vals and value.lower() not in all_vals:
                        reason = f"'{value}' not valid — expected: {', '.join(all_vals)}"

                elif rule.get("type") == "latitude" and value:
                    try:
                        v_float = float(value)
                        if not (-90 <= v_float <= 90):
                            reason = f"'{value}' out of latitude range"
                    except ValueError:
                        reason = f"'{value}' is not a valid latitude"

                elif rule.get("type") == "longitude" and value:
                    try:
                        v_float = float(value)
                        if not (-180 <= v_float <= 180):
                            reason = f"'{value}' out of longitude range"
                    except ValueError:
                        reason = f"'{value}' is not a valid longitude"

                elif rule.get("type") == "latlong_json" and value:
                    try:
                        geo = json.loads(value)
                        coords = geo.get("coordinates")
                        if not isinstance(coords, list) or len(coords) != 2:
                            reason = f"Invalid lat/long JSON structure"
                        else:
                            float(coords[0]); float(coords[1])
                    except Exception:
                        reason = f"'{value}' is not valid lat/long JSON"

                elif rule.get("type") == "latlong_text" and value:
                    parts = re.split(r"[,\s]+", value.strip())
                    try:
                        if len(parts) != 2:
                            reason = f"'{value}' must be two coordinates separated by comma/space"
                        else:
                            float(parts[0]); float(parts[1])
                    except Exception:
                        reason = f"'{value}' is not valid lat/long text"

                if not reason and "length" in rule and value:
                    if len(value) != rule["length"]:
                        reason = f"'{value}' must be exactly {rule['length']} characters"

                if reason:
                    errors.append({
                        "field":  col,
                        "value":  value,
                        "reason": reason
                    })

            if errors:
                user_map.setdefault(username, {
                    "username":      username,
                    "total_invalid": 0,
                    "field_summary": {},
                    "sample_errors": []
                })

                user_map[username]["total_invalid"] += 1

                # Field-level summary — count how many times each field failed
                for err in errors:
                    field = err["field"]
                    user_map[username]["field_summary"].setdefault(
                        field, {"count": 0, "sample_values": []}
                    )
                    user_map[username]["field_summary"][field]["count"] += 1
                    samples = user_map[username]["field_summary"][field]["sample_values"]
                    if len(samples) < 5 and err["value"] not in samples:
                        samples.append(err["value"])

                # Keep max 3 full sample error rows per user for preview
                if len(user_map[username]["sample_errors"]) < 3:
                    user_map[username]["sample_errors"].append({
                        "row_number": int(idx) + 2,
                        "errors":     errors
                    })

        # Convert field_summary dict to sorted list
        result = []
        for username, data in user_map.items():
            field_summary_list = sorted(
                [
                    {
                        "field":         field,
                        "fail_count":    info["count"],
                        "sample_values": info["sample_values"],
                        "reason":        f"Failed {info['count']} time(s)"
                    }
                    for field, info in data["field_summary"].items()
                ],
                key=lambda x: x["fail_count"],
                reverse=True
            )
            result.append({
                "username":      username,
                "total_invalid": data["total_invalid"],
                "field_summary": field_summary_list,
                "sample_errors": data["sample_errors"]
            })

        # Sort by most invalid entries first
        result.sort(key=lambda x: x["total_invalid"], reverse=True)
        return result

    except Exception as e:
        raise HTTPException(500, f"Failed to fetch user invalid records: {str(e)}")


# =====================================================
# REVALIDATE FORM — re-runs validation on stored records
# using the current rules (called after a rule change)
# =====================================================

@app.post("/REVALIDATE-FORM")
def revalidate_form(body: dict, db: Session = Depends(get_db)):
    form_name = (body.get("form_name") or "").strip()

    if not form_name:
        raise HTTPException(400, "form_name is required")

    # 1. Load current rules
    config = db.query(models.FormTemplate).filter(
        models.FormTemplate.form_name == form_name
    ).first()

    if not config:
        raise HTTPException(404, f"No form template found for '{form_name}'")

    rules_dict = json.loads(config.rules or "{}")

    # 2. Find all upload history records for this form
    uploads = db.query(models.UploadHistory).filter(
        models.UploadHistory.form_type == form_name
    ).all()

    if not uploads:
        return {"status": "ok", "message": "No uploaded records to revalidate", "updated": 0}

    total_revalidated = 0

    for upload in uploads:
        valid_path = upload.valid_file
        junk_path  = upload.junk_file

        # Skip if CSV files are missing
        if not valid_path or not junk_path:
            continue
        if not os.path.exists(valid_path) or not os.path.exists(junk_path):
            continue

        # 3. Reconstruct the original data by combining valid + junk CSVs
        valid_df = pd.read_csv(valid_path, dtype=str).fillna("")
        junk_df  = pd.read_csv(junk_path,  dtype=str).fillna("")

        # Drop the status column added during validation
        for df in (valid_df, junk_df):
            if "status" in df.columns:
                df.drop(columns=["status"], inplace=True)

        combined_df = pd.concat([valid_df, junk_df], ignore_index=True)

        if len(combined_df) == 0:
            continue

        # 4. Re-run validation with current rules
        new_valid_df, new_junk_df = apply_dynamic_validation(combined_df, rules_dict)

        new_valid_df = new_valid_df.copy()
        new_junk_df  = new_junk_df.copy()

        new_valid_df["status"] = "valid"
        new_junk_df["status"]  = "invalid"

        # 5. Overwrite the CSV files
        new_valid_df.to_csv(valid_path, index=False)
        new_junk_df.to_csv(junk_path,   index=False)

        # 6. Update UploadHistory counts
        upload.valid_rows = len(new_valid_df)
        upload.junk_rows  = len(new_junk_df)

        # 7. Re-build FormEntry rows for this upload's date
        selected_date = upload.selected_date
        db.query(models.FormEntry).filter(
            models.FormEntry.form_type     == form_name,
            models.FormEntry.selected_date == selected_date
        ).delete()

        new_combined = pd.concat([new_valid_df, new_junk_df], ignore_index=True)
        for _, row in new_combined.iterrows():
            db.add(models.FormEntry(
                form_type     = form_name,
                username      = extract_username(row),
                selected_date = selected_date,
                row_status    = row.get("status", "invalid"),
                circle        = str(row.get("circle", "UNKNOWN"))
            ))

        total_revalidated += len(new_combined)

    try:
        db.commit()
    except Exception as e:
        db.rollback()
        raise HTTPException(500, f"DB commit failed: {str(e)}")

    return {
        "status":      "ok",
        "form_name":   form_name,
        "uploads":     len(uploads),
        "revalidated": total_revalidated
    }


# =====================================================
# VALID RECORDS BY USER
# =====================================================

@app.get("/VALID-RECORDS-BY-USER")
def get_valid_records_by_user(form_name: str, db: Session = Depends(get_db)):
    try:
        history = db.query(models.UploadHistory).filter(
            models.UploadHistory.form_type == form_name
        ).order_by(models.UploadHistory.id.desc()).first()

        if not history or not history.valid_file:
            return []

        if not os.path.exists(history.valid_file):
            return []

        valid_df = pd.read_csv(history.valid_file, dtype=str).fillna("")

        if len(valid_df) == 0:
            return []

        possible_user_cols = [
            "createduser", "created user", "username",
            "user name", "operator", "created by"
        ]
        user_col = None
        for col in possible_user_cols:
            if col in valid_df.columns:
                user_col = col
                break

        user_counts = {}

        for _, row in valid_df.iterrows():
            if user_col:
                username = str(row.get(user_col, "")).strip() or "Unknown User"
            else:
                username = "Unknown User"

            user_counts[username] = user_counts.get(username, 0) + 1

        result = [
            {"username": username, "total_valid": count}
            for username, count in user_counts.items()
        ]
        result.sort(key=lambda x: x["total_valid"], reverse=True)
        return result

    except Exception as e:
        raise HTTPException(500, f"Failed to fetch user valid records: {str(e)}")


# =====================================================
# SITE DASHBOARD — CSV-based endpoints
# =====================================================

SITE_STATUS_CSV = os.path.join("data", "Site_Status.csv")

def find_alarm_csv():
    """Find the latest Alarm_Report CSV in the data/ folder."""
    matches = sorted(f for f in os.listdir("data") if f.startswith("Alarm_Report") and f.endswith(".csv"))
    if not matches:
        raise HTTPException(404, "Alarm_Report CSV not found in data/ folder")
    return os.path.join("data", matches[-1])


def load_site_status():
    df = pd.read_csv(SITE_STATUS_CSV, dtype=str).fillna("")
    df.columns = df.columns.str.strip()
    return df


def load_alarm_report():
    path = find_alarm_csv()
    df = pd.read_csv(path, dtype=str).fillna("")
    df.columns = df.columns.str.strip()
    return df


def filter_alarm_by_days(df: pd.DataFrame, days: int) -> pd.DataFrame:
    """Filter alarm rows to the last `days` days relative to the newest record in the data."""
    dt = pd.to_datetime(df["Alarm Start Time"], errors="coerce")
    max_dt = dt.max()
    if pd.isna(max_dt):
        return df
    cutoff = max_dt - pd.Timedelta(days=days)
    return df[dt >= cutoff].reset_index(drop=True)


def parse_duration_to_minutes(duration_str: str) -> float:
    """Convert HH:MM:SS string to total minutes. Returns 0 on parse failure."""
    try:
        parts = str(duration_str).strip().split(":")
        if len(parts) == 3:
            h, m, s = int(parts[0]), int(parts[1]), float(parts[2])
            return h * 60 + m + s / 60
    except Exception:
        pass
    return 0.0


@app.get("/SITE-DASHBOARD-STATS")
def site_dashboard_stats(days: int = Query(7)):
    try:
        site_df  = load_site_status()
        alarm_df = filter_alarm_by_days(load_alarm_report(), days)

        total_active_sites  = len(site_df)
        total_alarm_events  = len(alarm_df)
        unique_alarm_sites  = alarm_df["Global ID"].replace("", pd.NA).dropna().nunique()
        circles_affected    = alarm_df["State/Circle"].replace("", pd.NA).dropna().nunique()

        durations = alarm_df["Duration (HH:MM:SS)"].apply(parse_duration_to_minutes)
        avg_alarm_duration_minutes = round(durations.mean(), 2) if len(durations) > 0 else 0

        return {
            "total_active_sites":        total_active_sites,
            "total_alarm_events":        total_alarm_events,
            "unique_alarm_sites":        int(unique_alarm_sites),
            "circles_affected":          int(circles_affected),
            "avg_alarm_duration_minutes": avg_alarm_duration_minutes,
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, str(e))


@app.get("/SITE-ACTIVE-LIST")
def site_active_list():
    try:
        df = load_site_status()
        df = df.rename(columns={
            "S.No.":             "s_no",
            "Site ID":           "site_id",
            "Site Name":         "site_name",
            "State/Circle":      "circle",
            "H1":                "h1",
            "H2":                "h2",
            "ID":                "id",
            "IMEI No":           "imei_no",
            "Mobile No":         "mobile_no",
            "I&C Date":          "ic_date",
            "Battery":           "battery",
            "Battery (V)":       "battery_v",
            "Temp":              "temp",
            "Signal (dBm)":      "signal_dbm",
            "Last Communication":"last_communication",
            "Aging":             "aging",
        })
        return df.to_dict(orient="records")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, str(e))


@app.get("/SITE-ALARM-LIST")
def site_alarm_list(days: int = Query(7)):
    try:
        df = filter_alarm_by_days(load_alarm_report(), days)
        df = df.rename(columns={
            "S.No.":                "s_no",
            "Global ID":            "global_id",
            "Site Name":            "site_name",
            "State/Circle":         "circle",
            "District":             "district",
            "Cluster":              "cluster",
            "Alarm On-Site":        "alarm_type",
            "Alarm Start Time":     "alarm_start_time",
            "Alarm End Time":       "alarm_end_time",
            "Duration (HH:MM:SS)":  "duration",
            "Battery Start Volt":   "battery_start_v",
            "Battery End Volt":     "battery_end_v",
            "Temp.":                "temp",
            "IMEI":                 "imei",
            "Site Running ON":      "site_running_on",
            "Energy Start Time":    "energy_start_time",
            "Energy End Time":      "energy_end_time",
            "Acknowledge":          "acknowledge",
        })
        return df.to_dict(orient="records")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, str(e))


@app.get("/SITE-ALARM-TREND")
def site_alarm_trend(days: int = Query(7)):
    try:
        df = filter_alarm_by_days(load_alarm_report(), days)
        df["_date"] = pd.to_datetime(
            df["Alarm Start Time"], errors="coerce"
        ).dt.date.astype(str)
        df = df[df["_date"].notna() & (df["_date"] != "NaT") & (df["_date"] != "")]
        trend = (
            df.groupby("_date")
            .size()
            .reset_index(name="count")
            .sort_values("_date")
        )
        return [
            {"date": row["_date"], "count": int(row["count"])}
            for _, row in trend.iterrows()
        ]
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, str(e))


@app.get("/SITE-ALARM-BY-TYPE")
def site_alarm_by_type(days: int = Query(7)):
    try:
        df = filter_alarm_by_days(load_alarm_report(), days)
        df = df[df["Alarm On-Site"].replace("", pd.NA).notna()]
        result = (
            df.groupby("Alarm On-Site")
            .size()
            .reset_index(name="count")
            .sort_values("count", ascending=False)
        )
        return [
            {"alarm_type": row["Alarm On-Site"], "count": int(row["count"])}
            for _, row in result.iterrows()
        ]
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, str(e))


@app.get("/SITE-ALARM-BY-CIRCLE")
def site_alarm_by_circle(days: int = Query(7)):
    try:
        df = filter_alarm_by_days(load_alarm_report(), days)
        df = df[df["State/Circle"].replace("", pd.NA).notna()]
        result = (
            df.groupby("State/Circle")
            .size()
            .reset_index(name="count")
            .sort_values("count", ascending=False)
        )
        return [
            {"circle": row["State/Circle"], "count": int(row["count"])}
            for _, row in result.iterrows()
        ]
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, str(e))


@app.get("/SITE-ACTIVE-BY-CIRCLE")
def site_active_by_circle():
    try:
        df = load_site_status()
        df = df[df["State/Circle"].replace("", pd.NA).notna()]
        result = (
            df.groupby("State/Circle")
            .size()
            .reset_index(name="count")
            .sort_values("count", ascending=False)
        )
        return [
            {"circle": row["State/Circle"], "count": int(row["count"])}
            for _, row in result.iterrows()
        ]
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, str(e))

# =====================================================
# EMAIL REPORT — FILE UPLOAD
# =====================================================

@app.post("/UPLOAD-REPORT-FILE")
async def upload_report_file(
    file_type: str = Form(...),
    file: UploadFile = File(...),
):
    """
    Accepts one of: attendance | distance | employee | alarm
    Saves to data/daily/ with a standardised name so the scheduler
    and the manual-send endpoint both find it.
    """
    if file_type not in REPORT_FILE_MAP:
        raise HTTPException(
            400,
            f"Unknown file_type '{file_type}'. Must be one of: {list(REPORT_FILE_MAP.keys())}"
        )

    save_name = REPORT_FILE_MAP[file_type]
    save_path = os.path.join(REPORT_DAILY_DIR, save_name)

    contents = await file.read()
    with open(save_path, "wb") as f:
        f.write(contents)

    # Persist upload metadata
    meta_path = os.path.join(REPORT_DAILY_DIR, "meta.json")
    try:
        import json as _json
        meta = _json.loads(open(meta_path).read()) if os.path.exists(meta_path) else {}
    except Exception:
        meta = {}

    meta[file_type] = {
        "original_name": file.filename,
        "uploaded_at":   datetime.now().isoformat(),
        "size_bytes":    len(contents),
    }

    with open(meta_path, "w") as f:
        import json as _json
        _json.dump(meta, f)

    return {
        "status":    "success",
        "file_type": file_type,
        "saved_as":  save_name,
        "size":      len(contents),
    }


# =====================================================
# EMAIL REPORT — FILE STATUS
# =====================================================

@app.get("/REPORT-FILES-STATUS")
def report_files_status():
    """Returns upload status for each of the 4 daily report files."""
    meta_path = os.path.join(REPORT_DAILY_DIR, "meta.json")
    try:
        import json as _json
        meta = _json.loads(open(meta_path).read()) if os.path.exists(meta_path) else {}
    except Exception:
        meta = {}

    result = {}
    for key, filename in REPORT_FILE_MAP.items():
        path = os.path.join(REPORT_DAILY_DIR, filename)
        result[key] = {
            "uploaded": os.path.exists(path),
            "meta":     meta.get(key),
        }
    return result


# =====================================================
# EMAIL REPORT — MANUAL SEND TRIGGER
# =====================================================

@app.post("/SEND-DAILY-REPORT")
def trigger_send_daily_report():
    """
    Manually triggers the daily report send using whatever files
    are currently in data/daily/.  Requires attendance, distance
    and employee files to be present.
    """
    from services.notification_service import send_report_now
    result = send_report_now()
    if not result.get("success"):
        raise HTTPException(400, result.get("error", "Failed to send report"))
    return {"status": "success", "message": "Daily reports sent successfully"}


# =====================================================
# EMAIL REPORT — CLEAR DAILY FILES
# =====================================================

@app.post("/CLEAR-REPORT-FILES")
def clear_report_files():
    """Deletes all files in data/daily/ so the operator can start fresh for today."""
    import json as _json

    deleted = []
    for key, filename in REPORT_FILE_MAP.items():
        path = os.path.join(REPORT_DAILY_DIR, filename)
        if os.path.exists(path):
            os.remove(path)
            deleted.append(filename)

    meta_path = os.path.join(REPORT_DAILY_DIR, "meta.json")
    if os.path.exists(meta_path):
        os.remove(meta_path)

    return {"status": "success", "cleared": deleted}


# =====================================================
# EMAIL REPORT — DATA PREVIEWS
# =====================================================

@app.get("/REPORT-PREVIEW/FORMS")
def preview_forms(db: Session = Depends(get_db)):
    """
    Returns form submission counts per user for the most recent upload date.
    Used by the Email Reports page to preview what will appear in the emails.
    """
    from sqlalchemy import func as _func
    from models import FormEntry

    latest_date = db.query(_func.max(FormEntry.selected_date)).scalar()
    if not latest_date:
        return {"date": None, "rows": []}

    results = (
        db.query(
            FormEntry.username,
            FormEntry.form_type,
            _func.count(FormEntry.id).label("count"),
        )
        .filter(
            FormEntry.selected_date == latest_date,
            FormEntry.row_status == "valid",
        )
        .group_by(FormEntry.username, FormEntry.form_type)
        .order_by(FormEntry.username)
        .all()
    )

    # Group by username → list of {form_type, count}
    from collections import defaultdict as _dd
    grouped = _dd(list)
    for r in results:
        grouped[r.username].append({"form_type": r.form_type, "count": r.count})

    rows = [
        {"username": u, "forms": f, "total": sum(x["count"] for x in f)}
        for u, f in grouped.items()
    ]
    rows.sort(key=lambda x: -x["total"])

    return {"date": str(latest_date), "rows": rows}


@app.get("/REPORT-PREVIEW/MANAGERS")
def preview_managers():
    """
    Reads the uploaded manager/employee file and returns:
    - all column names found
    - grouped manager → team list
    Tries managers.xlsx first, falls back to employee.xlsx.
    """
    # Prefer the dedicated managers file, fall back to employee file
    for fname in ["managers.xlsx", "employee.xlsx"]:
        path = os.path.join(REPORT_DAILY_DIR, fname)
        if os.path.exists(path):
            break
    else:
        return {"uploaded": False, "columns": [], "managers": {}}

    try:
        df = pd.read_excel(path)
        df.columns = df.columns.astype(str).str.strip()
        actual_columns = list(df.columns)

        def find_col(candidates):
            for c in candidates:
                for col in df.columns:
                    if c.lower() in col.lower():
                        return col
            return None

        # Try every likely variation of manager / name / username / city column
        manager_col = find_col([
            "Reporting Manager", "Manager Name", "Manager", "Mgr",
            "Team Lead", "Supervisor", "Head",
        ])
        name_col = find_col([
            "Full Name", "Employee Name", "Emp Name", "Name",
        ])
        user_col = find_col([
            "Field Executive Username", "Username", "User Name",
            "User ID", "Emp ID", "Employee ID", "ID",
        ])
        city_col = find_col([
            "City", "Circle", "Region", "Location", "Zone", "State",
        ])

        if not manager_col:
            return {
                "uploaded": True,
                "columns":  actual_columns,
                "managers": {},
                "error": (
                    f"Could not find a Manager column. "
                    f"Columns in your file: {actual_columns}"
                ),
            }

        managers = {}
        for _, row in df.iterrows():
            mgr = str(row.get(manager_col, "")).strip()
            if not mgr or mgr.lower() in ["nan", "none", ""]:
                mgr = "Unassigned"

            emp = {
                "name":     str(row.get(name_col, "")).strip() if name_col else "",
                "username": str(row.get(user_col, "")).strip() if user_col else "",
                "city":     str(row.get(city_col, "")).strip() if city_col else "",
            }
            managers.setdefault(mgr, []).append(emp)

        return {
            "uploaded":      True,
            "columns":       actual_columns,
            "manager_col":   manager_col,
            "name_col":      name_col,
            "user_col":      user_col,
            "city_col":      city_col,
            "managers":      managers,
            "source_file":   fname,
        }

    except Exception as e:
        return {"uploaded": True, "columns": [], "managers": {}, "error": str(e)}
