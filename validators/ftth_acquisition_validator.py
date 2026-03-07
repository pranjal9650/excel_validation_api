import pandas as pd
import re
import json


# ---------------- COMMON HELPERS ---------------- #

def normalize_columns(df: pd.DataFrame):

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


def clean_df(df):

    df = df.fillna("")
    df = df.astype(str).apply(lambda col: col.str.strip())

    return df


def get_value(row, column):

    return row[column] if column in row else ""


def is_blank(value):

    return pd.isna(value) or str(value).strip() == ""


def is_valid_uuid(val):

    if is_blank(val):
        return False

    pattern = re.compile(r'^[a-fA-F0-9\-]{32,36}$')

    return bool(pattern.match(str(val)))


def is_yes_no_na(value):

    val = str(value).strip().lower()

    return val in ["yes", "no", "na", ""]


def is_numeric(value):

    try:
        float(value)
        return True
    except:
        return False


# ---------------- MAIN VALIDATOR ---------------- #

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

    for index, row in df.iterrows():

        row_errors = []

        # Name (only check if blank)
        name = get_value(row, "name")

        if is_blank(name):
            row_errors.append("name blank")

        # ID (optional but check format if present)
        id_val = get_value(row, "id")

        if not is_blank(id_val):

            if not is_valid_uuid(id_val):
                row_errors.append("id invalid")

            elif id_val in seen_ids:
                row_errors.append("duplicate id")

            else:
                seen_ids.add(id_val)

        # Building Type (optional)
        building_type = get_value(row, "building type")

        if len(building_type) > 100:
            row_errors.append("building type too long")

        # Building Name (optional but reasonable)
        building_name = get_value(row, "building name")

        if len(building_name) > 200:
            row_errors.append("building name too long")

        # Lat Long (very relaxed)
        latlong_val = get_value(row, "building lat, long")

        if not is_blank(latlong_val):

            try:

                geo = json.loads(str(latlong_val))

                coords = geo.get("coordinates", [])

                if not isinstance(coords, list) or len(coords) != 2:
                    row_errors.append("latlong invalid")

            except:
                row_errors.append("latlong invalid")

        # City / State optional
        city = get_value(row, "city")

        if len(city) > 100:
            row_errors.append("city too long")

        state = get_value(row, "state")

        if len(state) > 100:
            row_errors.append("state too long")

        # Pin Code (optional)
        pin = str(get_value(row, "pin code")).strip()

        if not is_blank(pin):

            if not pin.isdigit():
                row_errors.append("pincode invalid")

        # Sector optional
        sector = get_value(row, "sector/locality")

        if len(sector) > 200:
            row_errors.append("sector too long")

        # Numeric fields relaxed
        for field in numeric_fields:

            if field in df.columns:

                val = get_value(row, field)

                if not is_blank(val):

                    if not is_numeric(val):
                        row_errors.append(f"{field} invalid")

        # Common Basement relaxed
        basement = get_value(row, "common basement")

        if not is_yes_no_na(basement):
            row_errors.append("common basement invalid")

        # Authority optional
        authority = get_value(row, "authority")

        if len(authority) > 200:
            row_errors.append("authority too long")

        # Contact Person optional
        contact_person = get_value(row, "contact person")

        if len(contact_person) > 100:
            row_errors.append("contact person too long")

        # Contact Number relaxed
        contact = str(get_value(row, "contact number")).strip()

        if not is_blank(contact):

            digits = re.sub(r"\D", "", contact)

            if len(digits) < 8:
                row_errors.append("contact number invalid")

        # Dates optional
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

        # Users optional
        created_user = get_value(row, "createduser")

        if len(created_user) > 100:
            row_errors.append("created user too long")

        modified_user = get_value(row, "modifieduser")

        if len(modified_user) > 100:
            row_errors.append("modified user too long")

        errors.append("; ".join(row_errors))

    df["validation_errors"] = errors

    valid_df = df[df["validation_errors"] == ""].copy()
    junk_df = df[df["validation_errors"] != ""].copy()

    print(f"✅ Valid rows: {len(valid_df)}")
    print(f"❌ Junk rows: {len(junk_df)}")

    return valid_df, junk_df