import pandas as pd


# =====================================================
# HELPERS (ULTRA PERMISSIVE)
# =====================================================

def safe_float(val):
    try:
        if pd.isna(val):
            return None

        val = str(val)
        val = val.replace(",", "")
        val = val.replace("₹", "")
        val = val.replace("INR", "")
        val = val.strip()

        if val == "":
            return None

        return float(val)

    except:
        return None


def safe_date(val):
    try:
        return pd.to_datetime(val, errors="coerce")
    except:
        return None


def normalize_columns(df):

    df.columns = (
        df.columns.astype(str)
        .str.strip()
        .str.lower()
        .str.replace("\n", " ", regex=False)
        .str.replace("_", " ", regex=False)
        .str.replace("-", " ", regex=False)
    )

    df.columns = df.columns.str.replace(r"\s+", " ", regex=True)

    return df


def map_columns(df):

    column_map = {
        "id": ["id", "record id", "recordid", "unique id"],
        "start meter": ["start meter", "startmeter"],
        "closing reading": ["closing reading", "closingreading"],
        "total consumption": ["total consumption", "totalconsumption"],
        "per unit in inr": ["per unit in inr", "perunitininr"],
        "amount": ["amount", "total amount"],
        "reading month": ["reading month", "readingmonth"],
        "user name": ["user name", "username", "createduser"]
    }

    for std_col, variations in column_map.items():
        for col in variations:
            if col in df.columns:
                df[std_col] = df[col]
                break

    return df


# =====================================================
# MAIN VALIDATOR (SUPER LOOSE ⭐⭐⭐⭐⭐)
# =====================================================

def validate_eb_meter(df):

    if df is None or len(df) == 0:
        print("No data found")
        return df, df

    df = normalize_columns(df)
    df = map_columns(df)

    errors = []

    for _, row in df.iterrows():

        row_errors = []

        # ⭐ ID check only (nothing else strict)
        id_val = str(row.get("id", "")).strip()

        if id_val == "":
            row_errors.append("id blank")

        # ⭐ Reading sanity (NOT strict)
        if safe_float(row.get("start meter")) is None:
            row_errors.append("start meter missing")

        if safe_float(row.get("closing reading")) is None:
            row_errors.append("closing meter missing")

        # ⭐ Dates only check if parseable (no logic validation)
        if not pd.isna(row.get("reading month")):
            if safe_date(row.get("reading month")) is None:
                row_errors.append("invalid reading month")

        errors.append("; ".join(row_errors))

    df["validation_errors"] = errors

    # Preserve username if exists
    df["__USERNAME__"] = df.get("user name", "")

    # ⭐ VERY IMPORTANT — Make almost everything valid
    valid_df = df.copy()   # <--- ultra loose rule
    junk_df = df[df["validation_errors"] != ""].copy()

    print("✅ Total rows :", len(df))
    print("✅ Valid rows :", len(valid_df))
    print("❌ Invalid rows :", len(junk_df))

    return valid_df, junk_df