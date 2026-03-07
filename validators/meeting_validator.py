import pandas as pd
import json


# =====================================================
# SAFE CLEANING
# =====================================================

def safe_clean(df):

    df = df.fillna("")

    # Convert only object columns to string safely
    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = df[col].astype("string").astype(str).str.strip()

    df = df.replace({
        "nan": "",
        "None": "",
        "NONE": ""
    })

    return df


# =====================================================
# HELPERS
# =====================================================

def is_blank(val):

    if pd.isna(val):
        return True

    if str(val).strip().lower() in ["", "nan", "none"]:
        return True

    return False


# =====================================================
# COLUMN FINDER (SMART MATCH)
# =====================================================

def find_column(df, keywords):

    for col in df.columns:
        for key in keywords:
            if key in col:
                return col

    return None


# =====================================================
# MAIN VALIDATOR
# =====================================================

def validate_meeting_form(df: pd.DataFrame):

    df = pd.DataFrame(df).copy()

    # ⭐ Normalize + Clean
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

    df = safe_clean(df)

    # =====================================================
    # Auto Detect Columns
    # =====================================================

    id_col = find_column(df, ["id"])
    name_col = find_column(df, ["name"])
    user_col = find_column(df, ["user"])
    site_col = find_column(df, ["site"])
    meeting_col = find_column(df, ["meeting"])
    location_col = find_column(df, ["location"])
    remark_col = find_column(df, ["remark"])

    errors = []
    seen_ids = set()

    # =====================================================
    # VALIDATION LOOP
    # =====================================================

    for _, row in df.iterrows():

        row_errors = []

        # ---------------- NAME ----------------
        if name_col:
            if is_blank(row.get(name_col)):
                row_errors.append("name blank")

        # ---------------- USER ----------------
        if user_col:
            if is_blank(row.get(user_col)):
                row_errors.append("user blank")

        # ---------------- SITE ----------------
        if site_col:
            if is_blank(row.get(site_col)):
                row_errors.append("site blank")

        # ---------------- ID DUPLICATE ----------------
        if id_col:

            id_val = row.get(id_col)

            if not is_blank(id_val):

                if id_val in seen_ids:
                    row_errors.append("duplicate id")

                else:
                    seen_ids.add(id_val)

        # ---------------- LOCATION JSON ----------------
        if location_col:

            location = row.get(location_col)

            if not is_blank(location):

                try:
                    geo = json.loads(str(location))

                    coords = geo.get("coordinates")

                    if not isinstance(coords, list) or len(coords) != 2:
                        row_errors.append("location invalid")

                except:
                    row_errors.append("location invalid")

        # ---------------- REMARK ----------------
        if remark_col:

            remark = row.get(remark_col)

            if not is_blank(remark):

                if len(str(remark)) < 2:
                    row_errors.append("remark too short")

        errors.append("; ".join(row_errors))

    # =====================================================
    # RESULT SPLIT
    # =====================================================

    df["validation_errors"] = errors

    # Analytics username preservation
    if user_col:
        df["__USERNAME__"] = df[user_col]
    else:
        df["__USERNAME__"] = ""

    valid_df = df[df["validation_errors"] == ""].copy()
    junk_df = df[df["validation_errors"] != ""].copy()

    print("✅ Valid:", len(valid_df))
    print("❌ Invalid:", len(junk_df))

    return valid_df, junk_df