import pandas as pd
import re
import json


# =====================================================
# COLUMN NORMALIZATION
# =====================================================

def normalize_columns(df: pd.DataFrame):

    df.columns = (
        df.columns
        .astype(str)
        .str.strip()
        .str.lower()
        .str.replace("\n", " ", regex=False)
        .str.replace("\r", "", regex=False)
        .str.replace("_", " ", regex=False)
    )

    df.columns = df.columns.str.replace(r"\s+", " ", regex=True)

    return df


# =====================================================
# CLEAN DATA
# =====================================================

def clean_df(df):

    df = df.fillna("")

    for col in df.columns:
        df[col] = df[col].astype(str).str.strip()

    return df


# =====================================================
# HELPERS
# =====================================================

def get_value(row, column):

    if column in row:
        return row[column]

    return ""


def is_blank(value):

    return str(value).strip() == ""


def is_numeric(value):

    try:
        float(value)
        return True
    except:
        return False


def is_yes_no_na(value):

    v = str(value).strip().lower()

    return v in ["yes", "no", "na", ""]


# =====================================================
# MAIN VALIDATOR
# =====================================================

def validate_ftth_acquisition(df: pd.DataFrame):

    df = normalize_columns(df)
    df = clean_df(df)

    errors = []
    seen_ids = set()

    numeric_fields = [
        "in building wiring mtr",
        "ibw mtr.",
        "no. of towers at the society",
        "no. of tower",
        "no. of floor/tower",
        "no.of shaft",
        "total no.of flats on each floor",
        "total no of flat each tower",
        "cable tray length",
        "total home pass"
    ]

    for _, row in df.iterrows():

        row_errors = []

        # ---------------- NAME ----------------
        name = get_value(row, "name")

        if is_blank(name):
            row_errors.append("name blank")

        # ---------------- ID (optional duplicate check) ----------------
        id_val = get_value(row, "id")

        if not is_blank(id_val):

            if id_val in seen_ids:
                row_errors.append("duplicate id")

            else:
                seen_ids.add(id_val)

        # ---------------- BUILDING NAME ----------------
        building_name = get_value(row, "building name")

        if len(building_name) > 200:
            row_errors.append("building name too long")

        # ---------------- LAT LONG (VERY RELAXED) ----------------
        latlong = get_value(row, "building lat, long")

        if not is_blank(latlong):

            try:

                geo = json.loads(str(latlong))

                coords = geo.get("coordinates", [])

                if isinstance(coords, list) and len(coords) == 2:
                    pass
                else:
                    pass

            except:
                pass

        # ---------------- CITY ----------------
        city = get_value(row, "city")

        if len(city) > 100:
            row_errors.append("city too long")

        # ---------------- STATE ----------------
        state = get_value(row, "state")

        if len(state) > 100:
            row_errors.append("state too long")

        # ---------------- PIN CODE (RELAXED) ----------------
        pin = str(get_value(row, "pin code")).strip()

        if not is_blank(pin):

            digits = re.sub(r"\D", "", pin)

            if len(digits) < 4:
                row_errors.append("pincode invalid")

        # ---------------- SECTOR ----------------
        sector = get_value(row, "sector/locality")

        if len(sector) > 200:
            row_errors.append("sector too long")

        # ---------------- NUMERIC FIELDS ----------------
        for field in numeric_fields:

            if field in df.columns:

                val = get_value(row, field)

                if not is_blank(val):

                    if not is_numeric(val):
                        row_errors.append(f"{field} invalid")

        # ---------------- BASEMENT ----------------
        basement = get_value(row, "common basement")

        if not is_yes_no_na(basement):
            row_errors.append("common basement invalid")

        # ---------------- CONTACT PERSON ----------------
        contact_person = get_value(row, "contact person")

        if len(contact_person) > 100:
            row_errors.append("contact person too long")

        # ---------------- CONTACT NUMBER ----------------
        contact = str(get_value(row, "contact number")).strip()

        if not is_blank(contact):

            digits = re.sub(r"\D", "", contact)

            if len(digits) < 7:
                row_errors.append("contact number invalid")

        # ---------------- DATE CHECK ----------------
        created_date = pd.to_datetime(
            get_value(row, "createddate"),
            errors="coerce"
        )

        modified_date = pd.to_datetime(
            get_value(row, "modifieddate"),
            errors="coerce"
        )

        if not pd.isna(created_date) and not pd.isna(modified_date):

            if modified_date < created_date:
                row_errors.append("modified < created")

        errors.append("; ".join(row_errors))

    df["validation_errors"] = errors

    # preserve username for analytics
    if "createduser" in df.columns:
        df["__USERNAME__"] = df["createduser"]
    else:
        df["__USERNAME__"] = ""

    # preserve circle-like grouping if needed
    if "city" in df.columns:
        df["circle"] = df["city"]
    else:
        df["circle"] = "UNKNOWN_CIRCLE"

    valid_df = df[df["validation_errors"] == ""].copy()
    junk_df = df[df["validation_errors"] != ""].copy()

    print("✅ Valid rows:", len(valid_df))
    print("❌ Junk rows:", len(junk_df))

    return valid_df, junk_df