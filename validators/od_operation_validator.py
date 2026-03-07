import pandas as pd
import re
import json


# ---------------- COMMON HELPERS ---------------- #

def normalize_columns(df: pd.DataFrame):

    df.columns = (
        df.columns
        .str.strip()
        .str.replace("\n", " ", regex=False)
        .str.replace("\r", "", regex=False)
        .str.lower()
    )

    return df


def is_blank(value):

    return pd.isna(value) or str(value).strip() == ""


def is_yes_no(value):

    return str(value).strip().lower() in ["yes", "no", "y", "n"]


def clean_number(num):

    return re.sub(r"\D", "", str(num))


# ---------------- MAIN VALIDATOR ---------------- #

def validate_od_operation(df: pd.DataFrame):

    df = normalize_columns(df)

    errors = []
    seen_ids = set()

    for _, row in df.iterrows():

        row_errors = []

        # ---------- ID (STRICT) ---------- #

        id_val = row.get("id")

        if is_blank(id_val):

            row_errors.append("id blank")

        elif id_val in seen_ids:

            row_errors.append("duplicate id")

        else:

            seen_ids.add(id_val)

        # ---------- NAME (soft) ---------- #

        name = str(row.get("name", "")).strip()

        if name and len(name) < 2:
            row_errors.append("name invalid")

        # ---------- PROBLEM RESOLVED (STRICT) ---------- #

        resolved = row.get("problem resolved")

        if not is_blank(resolved):

            if not is_yes_no(resolved):
                row_errors.append("problem resolved invalid")

        # ---------- OWNER NUMBER ---------- #

        owner_number = clean_number(row.get("owner number", ""))

        if owner_number:

            if len(owner_number) < 8:
                row_errors.append("owner number invalid")

        # ---------- LAT LONG ---------- #

        latlong_val = row.get("lat long")

        if not is_blank(latlong_val):

            try:

                geo = json.loads(str(latlong_val))
                coords = geo.get("coordinates")

                if not isinstance(coords, list) or len(coords) != 2:
                    row_errors.append("latlong invalid")

            except:
                row_errors.append("latlong invalid")

        # ---------- TIME / DATE ---------- #

        time_val = row.get("time/date")

        if not is_blank(time_val):

            parsed_time = pd.to_datetime(
                time_val,
                errors="coerce",
                dayfirst=True
            )

            if pd.isna(parsed_time):
                row_errors.append("time/date invalid")

        # ---------- USERNAME ---------- #

        username = str(row.get("user name", "")).strip()

        if username:

            if not re.match(r'^[a-zA-Z0-9_.@-]+$', username):
                row_errors.append("username invalid")

        # ---------- CREATED DATE ---------- #

        created_date = None

        created_val = row.get("createddate")

        if not is_blank(created_val):

            created_date = pd.to_datetime(
                created_val,
                errors="coerce"
            )

            if pd.isna(created_date):
                row_errors.append("created date invalid")

        # ---------- MODIFIED DATE ---------- #

        modified_val = row.get("modifieddate")

        if not is_blank(modified_val):

            modified_date = pd.to_datetime(
                modified_val,
                errors="coerce"
            )

            if pd.isna(modified_date):

                row_errors.append("modified date invalid")

            elif created_date is not None and not pd.isna(created_date):

                if modified_date < created_date:
                    row_errors.append("modified < created")

        errors.append("; ".join(row_errors))


    df["validation_errors"] = errors


    # -------- Username for analytics -------- #

    if "user name" in df.columns:

        df["__USERNAME__"] = df["user name"]

    elif "createduser" in df.columns:

        df["__USERNAME__"] = df["createduser"]

    else:

        df["__USERNAME__"] = ""


    valid_df = df[df["validation_errors"] == ""].copy()
    junk_df = df[df["validation_errors"] != ""].copy()

    print(f"✅ Valid rows: {len(valid_df)}")
    print(f"❌ Junk rows: {len(junk_df)}")

    return valid_df, junk_df