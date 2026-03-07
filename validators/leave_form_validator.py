import pandas as pd
import re
import json


# ---------------- COLUMN NORMALIZATION ---------------- #

def normalize_columns(df):

    df.columns = (
        df.columns
        .astype(str)
        .str.strip()
        .str.replace("\n", " ", regex=False)
        .str.replace("\r", "", regex=False)
        .str.replace("_", " ", regex=False)
        .str.lower()
    )

    df.columns = df.columns.str.replace(r"\s+", " ", regex=True)

    return df


# ---------------- HELPERS ---------------- #

def is_blank(value):
    return value is None or str(value).strip() == ""


def safe_str(value):

    if pd.isna(value):
        return ""

    return str(value).strip()


def to_float(val):

    try:
        return float(str(val).replace(",", ""))
    except:
        return None


# ---------------- MAIN VALIDATOR ---------------- #

def validate_leave_form(df: pd.DataFrame):

    df = normalize_columns(df)
    df = df.fillna("")

    errors = []
    seen_ids = set()

    for index, row in df.iterrows():

        row_errors = []

        # Flexible column getter
        def val(keyword):

            for col in row.index:

                if keyword in col:
                    return safe_str(row[col])

            return ""

        # 1 Name (very relaxed)
        name = val("name")

        if is_blank(name):
            row_errors.append("name blank")

        # 2 ID (only duplicate check)
        current_id = val("id")

        if not is_blank(current_id):

            if current_id in seen_ids:
                row_errors.append("duplicate id")

            else:
                seen_ids.add(current_id)

        # 3 Employee Name
        emp_name = val("employee")

        if is_blank(emp_name):
            row_errors.append("employee name blank")

        # 4 Reason (very relaxed)
        reason = val("reason")

        if len(reason) > 500:
            row_errors.append("reason too long")

        # 5 Days (very flexible)
        days_val = val("days")

        if not is_blank(days_val):

            days = to_float(days_val)

            if days is None:
                row_errors.append("days invalid")

        # 6 Leave From
        leave_from = pd.to_datetime(
            val("leave form"),
            errors="coerce"
        )

        # 7 Leave To
        leave_to = pd.to_datetime(
            val("leave to"),
            errors="coerce"
        )

        if not pd.isna(leave_from) and not pd.isna(leave_to):

            if leave_to < leave_from:
                row_errors.append("leave to < leave from")

        # 8 Location (optional check)
        location_val = val("location")

        if not is_blank(location_val):

            try:

                location = json.loads(location_val)

                coords = location.get("coordinates")

                if not isinstance(coords, list):
                    row_errors.append("location invalid")

            except:
                # relaxed: ignore bad JSON
                pass

        # 9 Created User
        created_user = val("createduser")

        if is_blank(created_user):
            row_errors.append("created user blank")

        # 10 Modified User
        modified_user = val("modifieduser")

        if is_blank(modified_user):
            row_errors.append("modified user blank")

        errors.append("; ".join(row_errors))

    df["validation_errors"] = errors


    # -------- Username for analytics (circle code keep) -------- #

    if "createduser" in df.columns:
        df["__USERNAME__"] = df["createduser"]

    elif "modifieduser" in df.columns:
        df["__USERNAME__"] = df["modifieduser"]

    else:
        df["__USERNAME__"] = ""


    valid_df = df[df["validation_errors"] == ""].copy()
    junk_df = df[df["validation_errors"] != ""].copy()

    print(f"✅ Valid rows: {len(valid_df)}")
    print(f"❌ Junk rows: {len(junk_df)}")

    return valid_df, junk_df