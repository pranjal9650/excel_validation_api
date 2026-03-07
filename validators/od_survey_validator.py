import pandas as pd
import json


# ---------------- COLUMN NORMALIZATION ---------------- #

def normalize_columns(df: pd.DataFrame):

    df.columns = (
        df.columns
        .astype(str)
        .str.strip()
        .str.replace("\n", " ")
        .str.replace("\r", "")
        .str.replace("_", " ")
        .str.replace("-", " ")
        .str.lower()
    )

    df.columns = df.columns.str.replace(r"\s+", " ", regex=True)

    return df


# ---------------- DATA CLEANING ---------------- #

def clean_df(df):

    df = df.fillna("")

    for col in df.columns:
        df[col] = df[col].astype(str).str.strip()

    return df


# ---------------- HELPERS ---------------- #

def is_blank(val):
    return str(val).strip() == ""


def to_float(val):

    try:
        return float(str(val).replace(",", ""))
    except:
        return None


# ---------------- FLEXIBLE COLUMN FINDER ---------------- #

def find_column(df, keywords):

    for col in df.columns:

        for key in keywords:

            if key in col:
                return col

    return None


# ---------------- MAIN VALIDATOR ---------------- #

def validate_od_survey(df: pd.DataFrame):

    df = normalize_columns(df)
    df = clean_df(df)

    # -------- AUTO DETECT COLUMNS -------- #

    id_col = find_column(df, ["id"])
    name_col = find_column(df, ["name"])
    user_col = find_column(df, ["user name", "username"])
    site_col = find_column(df, ["site name"])
    circle_col = find_column(df, ["circle"])
    city_col = find_column(df, ["city"])
    latlong_col = find_column(df, ["lat long"])
    operator_col = find_column(df, ["operator"])
    tower_col = find_column(df, ["tower"])
    survey_lat_col = find_column(df, ["survey lat"])
    survey_long_col = find_column(df, ["survey long"])
    created_date_col = find_column(df, ["createddate"])
    modified_date_col = find_column(df, ["modifieddate"])
    created_user_col = find_column(df, ["createduser"])
    modified_user_col = find_column(df, ["modifieduser"])

    errors = []
    seen_ids = set()

    # ---------------- VALIDATION LOOP ---------------- #

    for _, row in df.iterrows():

        row_errors = []

        # ---------- ID (only blank + duplicate check) ---------- #

        if id_col:

            id_val = row[id_col]

            if not is_blank(id_val):

                if id_val in seen_ids:
                    row_errors.append("duplicate id")

                else:
                    seen_ids.add(id_val)

        # ---------- City (very soft rule) ---------- #

        if city_col:

            if is_blank(row[city_col]):
                pass   # allow blank

        # ---------- Survey Lat / Long (only if present) ---------- #

        if survey_lat_col:

            lat_val = row[survey_lat_col]

            if not is_blank(lat_val):

                if to_float(lat_val) is None:
                    row_errors.append("invalid survey lat")

        if survey_long_col:

            long_val = row[survey_long_col]

            if not is_blank(long_val):

                if to_float(long_val) is None:
                    row_errors.append("invalid survey long")

        # ---------- Created Date (soft check) ---------- #

        created_date = None

        if created_date_col:

            if not is_blank(row[created_date_col]):

                created_date = pd.to_datetime(
                    row[created_date_col],
                    errors="coerce"
                )

                if pd.isna(created_date):
                    row_errors.append("invalid created date")

        # ---------- Modified Date (very relaxed) ---------- #

        if modified_date_col:

            if not is_blank(row[modified_date_col]):

                modified_date = pd.to_datetime(
                    row[modified_date_col],
                    errors="coerce"
                )

                if pd.isna(modified_date):
                    row_errors.append("invalid modified date")

                elif created_date is not None and not pd.isna(created_date):

                    if modified_date < created_date:
                        row_errors.append("modified before created")

        # ---------- LatLong JSON (ignored if bad) ---------- #

        if latlong_col:

            latlong = row[latlong_col]

            if not is_blank(latlong):

                try:

                    geo = json.loads(str(latlong))
                    coords = geo.get("coordinates")

                    if not isinstance(coords, list) or len(coords) != 2:
                        pass

                except:
                    pass

        # ---------- Optional fields (no strict validation) ---------- #
        # Circle / Operator / Tower / Site / Building / Rent etc.
        # All treated as optional for maximum valid rows

        errors.append("; ".join(row_errors))

    # ---------------- RESULT ---------------- #

    df["validation_errors"] = errors


    # -------- USERNAME CIRCLE CODE (IMPORTANT) -------- #

    if user_col:
        df["__USERNAME__"] = df[user_col]

    elif created_user_col:
        df["__USERNAME__"] = df[created_user_col]

    else:
        df["__USERNAME__"] = ""


    valid_df = df[df["validation_errors"] == ""].copy()
    junk_df = df[df["validation_errors"] != ""].copy()

    print(f"✅ Valid rows: {len(valid_df)}")
    print(f"❌ Junk rows: {len(junk_df)}")

    return valid_df, junk_df