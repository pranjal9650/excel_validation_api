from fastapi import FastAPI, UploadFile, File
import pandas as pd
import os
import joblib

from validators.meeting_validator import validate_meeting_form
from validators.eb_meter_validator import validate_eb_meter
from validators.leave_form_validator import validate_leave_form
from validators.od_survey_validator import validate_od_survey
from validators.site_survey_checklist_validator import validate_site_survey_checklist
from validators.od_operation_validator import validate_od_operation
from validators.ftth_acquisition_validator import validate_ftth_acquisition

app = FastAPI()

# -----------------------------
# Load Models Safely
# -----------------------------

MODEL_DIR = "ml_model/saved_models"

def safe_load(model_name):
    path = os.path.join(MODEL_DIR, model_name)
    if os.path.exists(path):
        print(f"✅ Loaded {model_name}")
        return joblib.load(path)
    print(f"⚠ Missing {model_name}")
    return None

# Load all models
eb_model = safe_load("eb_meter_model.pkl")
ftth_model = safe_load("ftth_random_forest.pkl")
leave_model = safe_load("leave_random_forest.pkl")
meeting_model = safe_load("meeting_model.pkl")
od_model = safe_load("od_model.pkl")
od_survey_model = safe_load("od_survey_model.pkl")
site_survey_model = safe_load("site_survey_checklist_model.pkl")


# -----------------------------
# Feature Columns
# -----------------------------
EB_COLUMNS = joblib.load(os.path.join(MODEL_DIR, "eb_columns.pkl")) \
    if os.path.exists(os.path.join(MODEL_DIR, "eb_columns.pkl")) else []

FTTH_COLUMNS = joblib.load(os.path.join(MODEL_DIR, "ftth_columns.pkl")) \
    if os.path.exists(os.path.join(MODEL_DIR, "ftth_columns.pkl")) else []


# -----------------------------
# Prediction Helper
# -----------------------------
def predict_model(df, model, feature_cols):
    if model is None or len(feature_cols) == 0:
        return []

    try:
        for col in feature_cols:
            if col not in df.columns:
                df[col] = 0

        df_model = df[feature_cols]
        df_model = df_model.apply(pd.to_numeric, errors="coerce").fillna(0)

        return model.predict(df_model).tolist()

    except Exception as e:
        print("Prediction Error:", e)
        return []


# -----------------------------
# API Endpoint
# -----------------------------
@app.post("/VALIDATE-FORM")
async def validate_form(file: UploadFile = File(...)):

    try:
        df = pd.read_excel(file.file)

        df.columns = (
            df.columns
            .str.strip()
            .str.lower()
            .str.replace(r"\s+", " ", regex=True)
        )

        df = df.dropna(how="all")

        columns = df.columns.tolist()

        valid_df = pd.DataFrame()
        junk_df = pd.DataFrame()
        form_name = "unknown"

        # ---------- FORM DETECTION ----------
        if "type of meeting" in columns:
            valid_df, junk_df = validate_meeting_form(df)
            form_name = "meeting"

        elif "start meter" in columns or "closing reading" in columns:
            valid_df, junk_df = validate_eb_meter(df)
            form_name = "eb_meter"

        elif "employee name" in columns and "leave type" in columns:
            valid_df, junk_df = validate_leave_form(df)
            form_name = "leave_form"

        elif "opco id" in columns:
            valid_df, junk_df = validate_od_survey(df)
            form_name = "od_survey"

        elif "lat as per records" in columns:
            valid_df, junk_df = validate_site_survey_checklist(df)
            form_name = "site_survey_checklist"

        elif "building type" in columns:
            valid_df, junk_df = validate_ftth_acquisition(df)
            form_name = "ftth_acquisition"

        elif "incident remark" in columns:
            valid_df, junk_df = validate_od_operation(df)
            form_name = "od_operation"

        else:
            return {
                "status": "Error",
                "message": "Unknown form type",
                "detected_columns": columns
            }

        # ---------- Add Target ----------
        if not valid_df.empty:
            valid_df["target"] = 0

        if not junk_df.empty:
            junk_df["target"] = 1

        # ---------- Save Outputs ----------
        os.makedirs("outputs", exist_ok=True)

        valid_file = f"outputs/{form_name}_valid.xlsx"
        junk_file = f"outputs/{form_name}_junk.xlsx"

        valid_df.to_excel(valid_file, index=False)
        junk_df.to_excel(junk_file, index=False)

        # ---------- Prediction ----------
        predictions = []

        if form_name == "eb_meter":
            predictions = predict_model(valid_df, eb_model, EB_COLUMNS)

        return {
            "status": "Completed",
            "form_type": form_name,
            "total_rows": len(df),
            "valid_rows": len(valid_df),
            "junk_rows": len(junk_df),
            "valid_file": valid_file,
            "junk_file": junk_file,
            "predictions": predictions
        }

    except Exception as e:
        return {
            "status": "Error",
            "message": str(e)
        }