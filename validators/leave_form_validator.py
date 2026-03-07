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

        # safer column access
        def val(col):

            for c in row.index:
                if col in c:
                    return safe_str(row[c])

            return ""

        # 1 Name
        name = val("name")

        if is_blank(name):
            row_errors.append("name blank")

        # 2 ID (very relaxed)
        current_id = val("id")

        if not is_blank(current_id):

            if current_id in seen_ids:
                row_errors.append("duplicate id")

            else:
                seen_ids.add(current_id)

        # 3 Employee Name
        if is_blank(val("employee")):
            row_errors.append("employee name blank")

        # 4 Reason (very relaxed)
        reason = val("reason")

        if len(reason) > 300:
            row_errors.append("reason too long")

        # 5 Days
        days_val = val("days")

        if not is_blank(days_val):

            days = to_float(days_val)

            if days is None:
                row_errors.append("days invalid")

        # 6 Leave From
        leave_from = pd.to_datetime(
            val("leave"),
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

        # 8 Location (optional)
        location_val = val("location")

        if not is_blank(location_val):

            try:

                location = json.loads(location_val)

                coords = location.get("coordinates")

                if not isinstance(coords, list) or len(coords) != 2:
                    row_errors.append("location invalid")

            except:
                pass

        # 9 Created User
        if is_blank(val("created")):
            row_errors.append("created user blank")

        # 10 Modified User
        if is_blank(val("modified")):
            row_errors.append("modified user blank")

        errors.append("; ".join(row_errors))

    df["validation_errors"] = errors

    valid_df = df[df["validation_errors"] == ""].copy()
    junk_df = df[df["validation_errors"] != ""].copy()

    print(f"✅ Valid rows: {len(valid_df)}")
    print(f"❌ Junk rows: {len(junk_df)}")

    return valid_df, junk_df