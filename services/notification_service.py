import pandas as pd
from sqlalchemy import func
from collections import defaultdict

from services.email_service import send_email
from database import SessionLocal
from models import FormEntry


def send_daily_report():

    distance_file = "data/Distance Report -1st feb 25 to 30 Nov'25.xlsx"
    employee_file = "data/EMPLOYEE details'26.xlsx"

    print("Reading excel files...")

    distance_df = pd.read_excel(distance_file)
    employee_df = pd.read_excel(employee_file)

    # ---------------- NORMALIZE USERNAMES ---------------- #

    distance_df["Username"] = (
        distance_df["Username"]
        .astype(str)
        .str.strip()
        .str.lower()
    )

    employee_df["Field Executive Username"] = (
        employee_df["Field Executive Username"]
        .astype(str)
        .str.strip()
        .str.lower()
    )

    print("Merging employee and distance data...")

    merged_df = distance_df.merge(
        employee_df,
        left_on="Username",
        right_on="Field Executive Username",
        how="left"
    )

    # ---------------- GET TODAY DISTANCE COLUMN ---------------- #

    today_column = distance_df.columns[-1]

    print("Using distance column:", today_column)

    db = SessionLocal()

    manager_data = defaultdict(list)
    management_data = defaultdict(list)   # circle wise

    # ---------------- PROCESS EACH USER ---------------- #

    for _, row in merged_df.iterrows():

        username = row.get("Username", "")
        user_name = row.get("Full Name", "Unknown")
        manager = row.get("Reporting Manager", "Unknown")
        circle = row.get("City", "Unknown")

        distance = row.get(today_column, 0)

        # ---------------- GET FORMS FROM DB ---------------- #

        forms_query = db.query(
            FormEntry.form_type,
            func.count(FormEntry.id)
        ).filter(
            FormEntry.username == username,
            FormEntry.row_status == "valid"
        ).group_by(FormEntry.form_type).all()

        if not forms_query:
            form_display = "No Forms"
        else:
            form_list = []
            for form_name, count in forms_query:
                form_list.append(f"{form_name} ({count})")

            form_display = "<br>".join(form_list)

        # ---------------- MANAGER DATA ---------------- #

        manager_data[manager].append({
            "user": user_name,
            "distance": distance,
            "form_names": form_display
        })

        # ---------------- MANAGEMENT DATA (CIRCLE WISE) ---------------- #

        management_data[circle].append({
            "user": user_name,
            "distance": distance,
            "form_names": form_display
        })

    db.close()

    # =====================================================
    # SEND MANAGER REPORTS
    # =====================================================

    for manager, users in manager_data.items():

        body = f"""
        <h2 style="color:#2c3e50;">Daily Field Activity Report</h2>

        <p><b>Manager:</b> {manager}</p>

        <table border="1" cellpadding="8" cellspacing="0"
        style="border-collapse:collapse;font-family:Arial;width:100%;">

        <tr style="background-color:#f2f2f2;">
        <th>User</th>
        <th>Distance Travelled</th>
        <th>Forms Submitted</th>
        </tr>
        """

        for u in users:
            body += f"""
            <tr>
            <td>{u['user']}</td>
            <td style="text-align:center">{u['distance']} KM</td>
            <td>{u['form_names']}</td>
            </tr>
            """

        body += "</table>"

        recipients = [
            "pranjalg.work@gmail.com"
        ]

        send_email(
            recipients,
            f"Daily Report - Manager {manager}",
            body
        )

    print("Manager emails sent.")

    # =====================================================
    # MANAGEMENT REPORT (CIRCLE WISE)
    # =====================================================

    print("Preparing management report...")

    body = """
    <h2 style="color:#2c3e50;">All Circles Daily Field Activity Report</h2>
    """

    # SORT CIRCLES FOR CLEAN ORDER
    for circle in sorted(management_data.keys()):

        body += f"""
        <h3 style="color:#34495e;margin-top:30px;">{circle}</h3>

        <table border="1" cellpadding="8" cellspacing="0"
        style="border-collapse:collapse;font-family:Arial;width:100%;">

        <tr style="background-color:#f2f2f2;">
        <th>User</th>
        <th>Distance Travelled</th>
        <th>Forms Submitted</th>
        </tr>
        """

        for u in management_data[circle]:

            body += f"""
            <tr>
            <td>{u['user']}</td>
            <td style="text-align:center">{u['distance']} KM</td>
            <td>{u['form_names']}</td>
            </tr>
            """

        body += "</table>"

    management_recipients = [
        "pranjalg.work@gmail.com"
    ]

    send_email(
        management_recipients,
        "Daily Field Activity Report - All Circles",
        body
    )

    print("Management email sent.")


if __name__ == "__main__":
    send_daily_report()