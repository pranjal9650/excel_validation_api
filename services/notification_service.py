import pandas as pd
from sqlalchemy import func
from collections import defaultdict
from datetime import date
from datetime import date, datetime 

from services.email_service import send_email
from database import SessionLocal
from models import FormEntry
from models import SiteMonitoring


def send_daily_report():

    distance_file = "data/Distance Report -1st feb 25 to 30 Nov'25.xlsx"
    employee_file = "data/EMPLOYEE details'26.xlsx"
    attendance_file = "data/Report-1773314624370.xlsx"
    alarm_file = "data/Alarm_Report_20260318_125744.csv"

    print("Reading excel files...")

    distance_df = pd.read_excel(distance_file)
    employee_df = pd.read_excel(employee_file)
    attendance_df = pd.read_excel(attendance_file)
    alarm_df = pd.read_csv(alarm_file)

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

    attendance_df["Username"] = (
        attendance_df["Username"]
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

    # ---------------- MERGE ATTENDANCE ---------------- #

    merged_df = merged_df.merge(
        attendance_df,
        on="Username",
        how="left",
        suffixes=("", "_att")
    )

    # ---------------- GET TODAY COLUMNS ---------------- #

    distance_column = distance_df.columns[-1]
    attendance_column = attendance_df.columns[-1]

    print("Using distance column:", distance_column)
    print("Using attendance column:", attendance_column)

    db = SessionLocal()

    latest_date = db.query(func.max(FormEntry.selected_date)).scalar()
    print("Using form date:", latest_date)

    manager_data = defaultdict(list)
    circle_data = defaultdict(list)
    management_data = defaultdict(list)

    # ---------------- PROCESS USERS ---------------- #

    for _, row in merged_df.iterrows():

        username = row.get("Username", "")
        user_name = row.get("Full Name", "Unknown")
        manager = row.get("Reporting Manager", "Unknown")
        circle = row.get("City", "Unknown")

        # -------- DISTANCE CLEANING -------- #

        distance = row.get(distance_column, 0)

        if pd.isna(distance) or str(distance).strip() in ["--", "nan", ""]:
            distance = 0
        else:
            try:
                distance = float(distance)
            except:
                distance = 0

        # -------- ATTENDANCE -------- #

        attendance = row.get(attendance_column, "N/A")

        # -------- TODAY FORMS QUERY -------- #

        forms_query = (
        db.query(
            FormEntry.form_type,
            func.count(FormEntry.id)
        )
        .filter(
            FormEntry.username == username,
            FormEntry.row_status == "valid",
            FormEntry.selected_date == latest_date
        )
        .group_by(FormEntry.form_type)
        .all()
    )

        if not forms_query:
            form_display = "No Forms"
        else:
            form_list = []

            for form_name, count in forms_query:
                form_list.append(f"{form_name} ({count})")

            form_display = "<br>".join(form_list)

        user_record = {
            "user": user_name,
            "attendance": attendance,
            "distance": int(distance),
            "form_names": form_display
        }

        manager_data[manager].append(user_record)
        circle_data[circle].append(user_record)
        management_data[circle].append(user_record)

    db.close()

    # =====================================================
    # SITE DOWN PROCESSING
    # =====================================================

    db = SessionLocal()

    print("Processing site alarm report...")

    site_down_data = defaultdict(list)

    alarm_df = alarm_df.drop_duplicates(subset=["Global ID"])

    for _, row in alarm_df.iterrows():

        circle = str(row.get("State/Circle", "Unknown")).strip()
        site_id = str(row.get("Global ID", "Unknown")).strip()
        site_name = str(row.get("Site Name", "Unknown")).strip()

        # 🔍 Check if already exists
        existing = db.query(SiteMonitoring).filter(
            SiteMonitoring.site_id == site_id
        ).first()

        if not existing:
            record = SiteMonitoring(
                site_id=site_id,
                status="Inactive",
                outage="Yes",
                distance=0,
                manager="NA",
                circle=circle
            )
            db.add(record)

        site_down_data[circle].append({
            "site_id": site_id,
            "site_name": site_name
        })

    db.commit()
    db.close()

    # =====================================================
    # REPORTING MANAGER REPORTS
    # =====================================================

    for manager, users in manager_data.items():

        body = f"""
        <h2>Daily Field Activity Report</h2>
        <p><b>Manager:</b> {manager}</p>

        <table border="1" cellpadding="8" cellspacing="0"
        style="border-collapse:collapse;font-family:Arial;width:100%;">

        <tr style="background:#f2f2f2;">
        <th>User</th>
        <th>Attendance</th>
        <th>Distance</th>
        <th>Forms</th>
        </tr>
        """

        for u in users:
            body += f"""
            <tr>
            <td>{u['user']}</td>
            <td>{u['attendance']}</td>
            <td>{u['distance']} KM</td>
            <td>{u['form_names']}</td>
            </tr>
            """

        body += "</table>"

        recipients = [
            "pranjalg.work@gmail.com",
        ]

        send_email(
            recipients,
            f"Daily Report - Manager {manager}",
            body
        )

    print("Manager reports sent.")

    # =====================================================
    # CIRCLE HEAD REPORTS
    # =====================================================

    for circle, users in circle_data.items():

        body = f"""
        <h2>Circle Report</h2>
        <p><b>Circle:</b> {circle}</p>

        <table border="1" cellpadding="8" cellspacing="0"
        style="border-collapse:collapse;font-family:Arial;width:100%;">

        <tr style="background:#f2f2f2;">
        <th>User</th>
        <th>Attendance</th>
        <th>Distance</th>
        <th>Forms</th>
        </tr>
        """

        for u in users:
            body += f"""
            <tr>
            <td>{u['user']}</td>
            <td>{u['attendance']}</td>
            <td>{u['distance']} KM</td>
            <td>{u['form_names']}</td>
            </tr>
            """

        body += "</table>"

        sites = site_down_data.get(circle, [])

        body += f"""
        <h3 style="margin-top:30px;">
        Sites Down Yesterday ({len(sites)})
        </h3>

        <table border="1" cellpadding="8" cellspacing="0"
        style="border-collapse:collapse;font-family:Arial;width:60%;">

        <tr style="background:#f2f2f2;">
        <th>Site ID</th>
        <th>Site Name</th>
        </tr>
        """

        for s in sites:
            body += f"""
            <tr>
            <td>{s['site_id']}</td>
            <td>{s['site_name']}</td>
            </tr>
            """

        body += "</table>"

        recipients = [
            "pranjalg.work@gmail.com",
        ]

        send_email(
            recipients,
            f"Daily Report - Circle {circle}",
            body
        )

    print("Circle reports sent.")

    # =====================================================
    # MANAGEMENT REPORT
    # =====================================================

    print("Preparing management report...")

    body = "<h2>All Circles Daily Field Activity Report</h2>"

    for circle in sorted(management_data.keys()):

        body += f"<h3>{circle}</h3>"

        body += """
        <table border="1" cellpadding="8" cellspacing="0"
        style="border-collapse:collapse;font-family:Arial;width:100%;">

        <tr style="background:#f2f2f2;">
        <th>User</th>
        <th>Attendance</th>
        <th>Distance</th>
        <th>Forms</th>
        </tr>
        """

        for u in management_data[circle]:
            body += f"""
            <tr>
            <td>{u['user']}</td>
            <td>{u['attendance']}</td>
            <td>{u['distance']} KM</td>
            <td>{u['form_names']}</td>
            </tr>
            """

        body += "</table>"

    body += "<h2 style='margin-top:40px;'>Site Down Summary</h2>"

    for circle, sites in site_down_data.items():

        body += f"""
        <h3>{circle} - {len(sites)} Sites Down</h3>

        <table border="1" cellpadding="6" cellspacing="0"
        style="border-collapse:collapse;width:50%;">

        <tr style="background:#f2f2f2;">
        <th>Site ID</th>
        <th>Site Name</th>
        </tr>
        """

        for s in sites:
            body += f"""
            <tr>
            <td>{s['site_id']}</td>
            <td>{s['site_name']}</td>
            </tr>
            """

        body += "</table>"

    management_recipients = [
        "pranjalg.work@gmail.com",
    ]

    send_email(
        management_recipients,
        "Daily Field Activity Report - All Circles",
        body
    )

    print("Management email sent.")


if __name__ == "__main__":
    send_daily_report()