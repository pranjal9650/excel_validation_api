import pandas as pd


# ============================================================
# HELPERS
# ============================================================

def is_blank(val):

    if val is None:
        return True

    if isinstance(val, pd.Series):
        val = val.iloc[0]

    return str(val).strip().lower() in ["", "nan", "none"]


def valid_lat(val):

    if is_blank(val):
        return False

    try:
        v = float(str(val).replace(",", ""))
        return -90 <= v <= 90
    except:
        return False


def valid_lon(val):

    if is_blank(val):
        return False

    try:
        v = float(str(val).replace(",", ""))
        return -180 <= v <= 180
    except:
        return False


# ============================================================
# MAIN VALIDATOR
# ============================================================

def validate_site_survey_checklist(df: pd.DataFrame):

    if df is None or len(df) == 0:
        raise Exception("Empty dataframe received")

    df = df.copy()

    # ⭐ Normalize column names
    df.columns = (
        df.columns
        .astype(str)
        .str.strip()
        .str.lower()
    )

    errors = []
    seen_ids = set()

    # ========================================================
    # Row Validation
    # ========================================================

    for _, row in df.iterrows():

        row_errors = []

        # ---------------- Identification ---------------- #

        name = row.get("name")
        site_id = row.get("stpl site id")
        rec_id = str(row.get("id"))

        if is_blank(name):
            row_errors.append("name missing")

        if is_blank(site_id):
            row_errors.append("stpl site id missing")

        if is_blank(rec_id):
            row_errors.append("id missing")

        elif rec_id in seen_ids:
            row_errors.append("duplicate id")

        else:
            seen_ids.add(rec_id)

        # ---------------- Location ---------------- #

        if not valid_lat(row.get("actual latitude")):
            row_errors.append("invalid latitude")

        if not valid_lon(row.get("actual longitude")):
            row_errors.append("invalid longitude")

        # ---------------- Geography ---------------- #

        if is_blank(row.get("circle")):
            row_errors.append("circle missing")

        if is_blank(row.get("city")):
            row_errors.append("city missing")

        # ---------------- Site Details ---------------- #

        if is_blank(row.get("site name")):
            row_errors.append("site name missing")

        if is_blank(row.get("type of site")):
            row_errors.append("site type missing")

        errors.append("; ".join(row_errors))

    # ========================================================
    # Attach Validation Results
    # ========================================================

    df["validation_errors"] = errors


    # ========================================================
    # USERNAME CIRCLE CODE (needed for analytics)
    # ========================================================

    if "user name" in df.columns:
        df["__USERNAME__"] = df["user name"]

    elif "createduser" in df.columns:
        df["__USERNAME__"] = df["createduser"]

    else:
        df["__USERNAME__"] = ""


    # ========================================================
    # SPLIT VALID / JUNK
    # ========================================================

    valid_df = df[df["validation_errors"] == ""].copy()
    junk_df = df[df["validation_errors"] != ""].copy()

    print(f"✅ Valid rows: {len(valid_df)}")
    print(f"❌ Junk rows: {len(junk_df)}")

    return valid_df, junk_df