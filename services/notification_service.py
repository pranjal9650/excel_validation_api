import os
import re
import glob
import pandas as pd
from collections import defaultdict
from datetime import date, datetime

from services.email_service import send_email
from database import SessionLocal
from models import SiteMonitoring


# =====================================================
# DAILY UPLOAD FOLDER — files uploaded via the portal
# =====================================================

DAILY_DIR = "data/daily"
os.makedirs(DAILY_DIR, exist_ok=True)

DAILY_FILES = {
    "employee":    os.path.join(DAILY_DIR, "employee.xlsx"),
    "attendance":  os.path.join(DAILY_DIR, "attendance.xlsx"),
    "distance":    os.path.join(DAILY_DIR, "distance.xlsx"),
    "forms":       os.path.join(DAILY_DIR, "forms.xlsx"),
    "managers":    os.path.join(DAILY_DIR, "managers.xlsx"),
    "forms_filled":os.path.join(DAILY_DIR, "forms_filled.xlsx"),
    "alarm":       os.path.join(DAILY_DIR, "alarm.csv"),
}

# Legacy fallback paths (used only if daily files not uploaded yet)
LEGACY_FILES = {
    "distance":   "data/Distance Report -1st feb 25 to 30 Nov'25.xlsx",
    "employee":   "data/EMPLOYEE details'26.xlsx",
    "attendance": "data/Report-1773314624370.xlsx",
}

# =====================================================
# CIRCLE HEAD CONFIGURATION
# Phone numbers are used to identify circle heads
# dynamically from whatever employee file is uploaded —
# no usernames are hard-coded here.
# Update only when circle heads change.
# =====================================================

CIRCLE_HEADS = {
    "Delhi":        {"head": "Saurabh Gupta",      "phone": "9990009220", "email": "saurabhgupta@shaurryatele.com"},
    "GJ":           {"head": "Rajnish Nimbark",     "phone": "7226080870", "email": "rajnish@shaurryatele.com"},
    "KA":           {"head": "Satish Megaraj",      "phone": "9538886655", "email": "satish.megaraj@shaurryatele.com"},
    "Maharashtra":  {"head": "Dattatray Ranmalkar", "phone": "8888806810", "email": "dattatray@shaurryatele.com"},
    "Mumbai":       {"head": "Sunil Bhagwat",       "phone": "8108779091", "email": "sunilbhagwat@shaurryatele.com"},
    "UPE":          {"head": "Deepanshu Pandey",    "phone": "9140864299", "email": "deepanshupandey@shaurryatele.com"},
    "UPW":          {"head": "Rajesh Shukla",       "phone": "8826162006", "email": "rajesh.shukla@shaurryatele.com"},
    "WB & Kolkata": {"head": "Abhiman Ganguly",     "phone": "9903451369", "email": "abhiman.ganguly@shaurryatele.com"},
    "MP & CG":      {"head": "Piyush Khobragade",   "phone": "9773459073", "email": "piyush.khobragade@shaurryatele.com"},
}


# =====================================================
# EMAIL STYLE — shared across all reports
# =====================================================

EMAIL_CSS = """
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: 'Segoe UI', Arial, sans-serif; background: #fdf0f0; }
  .wrapper { max-width: 920px; margin: 24px auto; background: #fff;
             border-radius: 10px; overflow: hidden;
             box-shadow: 0 4px 20px rgba(180,0,0,0.12); }

  /* ---- Header ---- */
  .header { background: linear-gradient(135deg, #7f0000 0%, #c62828 100%);
            padding: 30px 36px; }
  .header h1 { color: #fff; font-size: 22px; font-weight: 700;
               letter-spacing: 0.4px; margin-bottom: 6px; }
  .header .meta { color: #ef9a9a; font-size: 13px; }

  /* ---- Summary cards ---- */
  .summary { display: flex; gap: 14px; padding: 24px 36px 0; }
  .card { flex: 1; background: #fff5f5; border: 1px solid #ffcdd2;
          border-radius: 8px; padding: 18px; text-align: center; }
  .card .val { font-size: 30px; font-weight: 700; color: #c62828; }
  .card .lbl { font-size: 12px; color: #9e7070; margin-top: 4px;
               text-transform: uppercase; letter-spacing: 0.5px; }

  /* ---- Sections ---- */
  .section { padding: 28px 36px; }
  .section-title { font-size: 15px; font-weight: 700; color: #7f0000;
                   border-left: 4px solid #c62828; padding-left: 12px;
                   margin-bottom: 16px; }

  /* ---- Tables ---- */
  table { width: 100%; border-collapse: collapse; font-size: 13px;
          margin-bottom: 4px; }
  th { background: #c62828; color: #fff; padding: 11px 14px;
       text-align: left; font-size: 12px; font-weight: 600;
       text-transform: uppercase; letter-spacing: 0.4px; }
  td { padding: 10px 14px; border-bottom: 1px solid #fce4e4;
       color: #2c3e50; vertical-align: top; }
  tr:last-child td { border-bottom: none; }
  tr:nth-child(even) td { background: #fff8f8; }
  tr:hover td { background: #ffebee; transition: background 0.15s; }

  /* ---- Badges ---- */
  .badge { display: inline-block; padding: 3px 10px; border-radius: 20px;
           font-size: 11px; font-weight: 600; }
  .badge-present { background: #e8f5e9; color: #2e7d32; }
  .badge-absent  { background: #ffebee; color: #c62828; }
  .badge-na      { background: #f5f5f5; color: #757575; }
  .badge-down    { background: #fff3e0; color: #e65100; }
  .badge-ok      { background: #e8f5e9; color: #2e7d32; }

  /* ---- Divider ---- */
  .divider { height: 1px; background: #fce4e4; margin: 0 36px; }

  /* ---- Footer ---- */
  .footer { background: #fff5f5; padding: 18px 36px; text-align: center;
            font-size: 11px; color: #b07070; border-top: 1px solid #ffcdd2; }
  .footer strong { color: #c62828; }
</style>
"""


# =====================================================
# HELPERS
# =====================================================

def get_latest_alarm_file():
    """Picks the most recently modified Alarm_Report CSV in data/."""
    files = glob.glob("data/Alarm_Report_*.csv")
    if not files:
        print("[Report] No Alarm_Report CSV found in data/")
        return None
    latest = max(files, key=os.path.getmtime)
    print(f"[Report] Using alarm file: {latest}")
    return latest


def attendance_badge(value):
    v = str(value).strip().lower()
    if v in ["present", "p", "yes"]:
        return f'<span class="badge badge-present">{value}</span>'
    if v in ["absent", "a", "no"]:
        return f'<span class="badge badge-absent">{value}</span>'
    return f'<span class="badge badge-na">{value}</span>'


def user_rows_html(users):
    rows = ""
    for u in users:
        dist = u['distance']
        try:
            dist_str = f"{float(dist):g}"   # 56.63 → "56.63", 136.0 → "136"
        except (ValueError, TypeError):
            dist_str = str(dist)
        rows += f"""
        <tr>
          <td><strong>{u['user']}</strong></td>
          <td>{attendance_badge(u['attendance'])}</td>
          <td>{dist_str} km</td>
          <td style="line-height:1.7">{u['form_names']}</td>
        </tr>"""
    return rows


def site_rows_html(sites):
    rows = ""
    for s in sites:
        rows += f"""
        <tr>
          <td>{s['site_id']}</td>
          <td>{s['site_name']}</td>
          <td><span class="badge badge-down">Down</span></td>
        </tr>"""
    return rows


def present_count(users):
    return sum(
        1 for u in users
        if str(u["attendance"]).strip().lower() in ["present", "p", "yes"]
    )


# =====================================================
# REPORT BUILDERS
# =====================================================

def build_manager_email(manager, users, report_date):
    total    = len(users)
    present  = present_count(users)
    total_km = round(sum(u["distance"] for u in users), 2)

    return f"""<!DOCTYPE html><html><head>{EMAIL_CSS}</head><body>
<div class="wrapper">
  <div class="header">
    <h1>Daily Field Activity Report</h1>
    <div class="meta">
      Manager: <strong style="color:#fff">{manager}</strong>
      &nbsp;|&nbsp; Date: {report_date}
    </div>
  </div>

  <div class="summary">
    <div class="card"><div class="val">{total}</div><div class="lbl">Team Members</div></div>
    <div class="card"><div class="val">{present}</div><div class="lbl">Present Today</div></div>
    <div class="card"><div class="val">{total_km}</div><div class="lbl">Total KM Covered</div></div>
  </div>

  <div class="section">
    <div class="section-title">Team Activity</div>
    <table>
      <tr>
        <th>Employee</th><th>Attendance</th><th>Distance</th><th>Forms Submitted</th>
      </tr>
      {user_rows_html(users)}
    </table>
  </div>

  <div class="footer">
    Auto-generated by <strong>Field Activity Reporting System</strong> &nbsp;|&nbsp; {datetime.now().strftime("%d %b %Y, %I:%M %p")}
  </div>
</div>
</body></html>"""


def build_circle_email(circle, head_name, users, sites, report_date):
    total    = len(users)
    present  = present_count(users)
    total_km = round(sum(u["distance"] for u in users), 2)

    return f"""<!DOCTYPE html><html><head>{EMAIL_CSS}</head><body>
<div class="wrapper">
  <div class="header">
    <h1>Circle Daily Report — {circle}</h1>
    <div class="meta">
      Circle Head: <strong style="color:#fff">{head_name}</strong>
      &nbsp;|&nbsp; Date: {report_date}
    </div>
  </div>

  <div class="summary">
    <div class="card"><div class="val">{total}</div><div class="lbl">Field Executives</div></div>
    <div class="card"><div class="val">{present}</div><div class="lbl">Present Today</div></div>
    <div class="card"><div class="val">{total_km}</div><div class="lbl">Total KM Covered</div></div>
    <div class="card"><div class="val">{len(sites)}</div><div class="lbl">Sites Down</div></div>
  </div>

  <div class="section">
    <div class="section-title">Field Activity</div>
    <table>
      <tr>
        <th>Employee</th><th>Attendance</th><th>Distance</th><th>Forms Submitted</th>
      </tr>
      {user_rows_html(users)}
    </table>
  </div>

  <div class="divider"></div>

  <div class="section">
    <div class="section-title">Sites Down ({len(sites)})</div>
    <table>
      <tr><th>Site ID</th><th>Site Name</th><th>Status</th></tr>
      {site_rows_html(sites) if sites else '<tr><td colspan="3" style="text-align:center;color:#2e7d32;padding:20px">No sites down — All clear!</td></tr>'}
    </table>
  </div>

  <div class="footer">
    Auto-generated by <strong>Field Activity Reporting System</strong> &nbsp;|&nbsp; {datetime.now().strftime("%d %b %Y, %I:%M %p")}
  </div>
</div>
</body></html>"""


def build_management_email(management_data, site_down_data, report_date):
    total_staff   = sum(len(v) for v in management_data.values())
    total_present = sum(present_count(v) for v in management_data.values())
    total_sites   = sum(len(v) for v in site_down_data.values())

    circles_html = ""
    for circle in sorted(management_data.keys(), key=str):
        users = management_data[circle]
        circles_html += f"""
        <div class="section">
          <div class="section-title">{circle}</div>
          <table>
            <tr>
              <th>Employee</th><th>Attendance</th><th>Distance</th><th>Forms Submitted</th>
            </tr>
            {user_rows_html(users)}
          </table>
        </div>
        <div class="divider"></div>"""

    sites_html = ""
    for circle, sites in site_down_data.items():
        sites_html += f"""
        <div class="section">
          <div class="section-title">{circle} — {len(sites)} Sites Down</div>
          <table>
            <tr><th>Site ID</th><th>Site Name</th><th>Status</th></tr>
            {site_rows_html(sites)}
          </table>
        </div>
        <div class="divider"></div>"""

    if not sites_html:
        sites_html = '<div class="section"><p style="color:#2e7d32;font-weight:600">All sites are operational. No outages reported.</p></div>'

    return f"""<!DOCTYPE html><html><head>{EMAIL_CSS}</head><body>
<div class="wrapper">
  <div class="header">
    <h1>All-Circle Daily Field Activity Report</h1>
    <div class="meta">Management Summary  |  Date: {report_date}</div>
  </div>

  <div class="summary">
    <div class="card"><div class="val">{total_staff}</div><div class="lbl">Total Staff</div></div>
    <div class="card"><div class="val">{total_present}</div><div class="lbl">Present Today</div></div>
    <div class="card"><div class="val">{total_sites}</div><div class="lbl">Total Sites Down</div></div>
  </div>

  <div class="divider" style="margin-top:24px"></div>
  {circles_html}

  <div class="section">
    <div class="section-title" style="color:#c62828;border-color:#c62828">Site Down Summary</div>
  </div>
  {sites_html}

  <div class="footer">
    Auto-generated by <strong>Field Activity Reporting System</strong> &nbsp;|&nbsp; {datetime.now().strftime("%d %b %Y, %I:%M %p")}
  </div>
</div>
</body></html>"""


# =====================================================
# CORE REPORT RUNNER — accepts explicit file paths
# =====================================================

def _find_col(columns, candidates):
    """Case-insensitive partial match across a list of candidate names.
    Columns are stringified first so datetime/int headers don't crash."""
    str_columns = [str(c) for c in columns]
    for candidate in candidates:
        for i, col_str in enumerate(str_columns):
            if candidate.lower() in col_str.lower():
                return columns[i]  # return the original column object
    return None


def _run_report(attendance_file, distance_file, employee_file, alarm_file=None):
    """Core logic: read files, merge data, build and send all emails."""

    report_date = datetime.now().strftime("%d %B %Y")

    # ---- Validate required files ----
    for f in [distance_file, employee_file, attendance_file]:
        if not os.path.exists(f):
            print(f"[Report] Missing file: {f} — aborting.")
            return {"success": False, "error": f"Missing file: {f}"}

    print("[Report] Reading data files...")
    distance_df   = pd.read_excel(distance_file)
    employee_df   = pd.read_excel(employee_file)
    attendance_df = pd.read_excel(attendance_file)

    # Force all column names to plain strings (Excel can parse headers as datetime/int)
    distance_df.columns   = distance_df.columns.astype(str).str.strip()
    employee_df.columns   = employee_df.columns.astype(str).str.strip()
    attendance_df.columns = attendance_df.columns.astype(str).str.strip()

    emp_cols = list(employee_df.columns)

    # ---- Auto-detect employee file columns (flexible matching) ----
    emp_username_col = _find_col(emp_cols, [
        "Field Executive Username", "FE Username", "Username", "User Name",
        "User ID", "Emp ID", "Employee ID",
    ]) or emp_cols[0]

    manager_col = _find_col(emp_cols, [
        "Reporting Manager", "Manager Name", "Manager", "Mgr",
        "Team Lead", "Supervisor", "Reported To",
    ])

    full_name_col = _find_col(emp_cols, [
        "Full Name", "Employee Name", "Emp Name", "Name",
    ])

    circle_col = _find_col(emp_cols, [
        "City", "Circle", "Region", "Location", "Zone", "State",
    ])

    email_col = _find_col(emp_cols, [
        "Email", "Email ID", "Mail", "Email Address",
    ])

    print(f"[Report] Employee schema — username: '{emp_username_col}', "
          f"manager: '{manager_col}', circle: '{circle_col}', "
          f"name: '{full_name_col}', email: '{email_col}'")

    # ---- Normalize usernames ----
    distance_df["Username"] = (
        distance_df["Username"].astype(str).str.strip().str.lower()
    )
    employee_df[emp_username_col] = (
        employee_df[emp_username_col].astype(str).str.strip().str.lower()
    )
    attendance_df["Username"] = (
        attendance_df["Username"].astype(str).str.strip().str.lower()
    )

    # ---- Detect attendance column IN attendance_df BEFORE any merge ----
    PA_VALUES = {"p", "a", "present", "absent", "yes", "no"}

    def find_attendance_col(df):
        """Find column with P/A values. Name-check first, then value scan."""
        by_name = _find_col(list(df.columns), [
            "Attendance", "Attn", "Status", "Mark", "Present", "Absent",
        ])
        if by_name:
            print(f"[Report] Attendance col found by name: '{by_name}'")
            return by_name
        best_col, best_score = None, -1
        for col in df.columns:
            if str(col).lower() in ("username", "name", "date"):
                continue
            sample = df[col].dropna().astype(str).str.strip().str.lower()
            score = int(sample.isin(PA_VALUES).sum())
            if score > best_score:
                best_score, best_col = score, col
        result = best_col if (best_col and best_score > 0) else df.columns[-1]
        print(f"[Report] Attendance col found by value scan: '{result}' (score={best_score})")
        return result

    def find_distance_col(df):
        """Find column with numeric KM values. Name-check first, then numeric scan."""
        by_name = _find_col(list(df.columns), [
            "Distance", "KM", "Km", "Travel", "Dist", "Total KM",
        ])
        if by_name:
            print(f"[Report] Distance col found by name: '{by_name}'")
            return by_name
        best_col, best_ratio = df.columns[-1], 0.0
        for col in df.columns:
            if str(col).lower() in ("username", "name", "date"):
                continue
            sample = df[col].dropna().astype(str).str.strip()
            numeric_count = sample.apply(
                lambda x: x.replace(".", "", 1).lstrip("-").isdigit()
            ).sum()
            ratio = numeric_count / max(len(sample), 1)
            if ratio > best_ratio:
                best_ratio, best_col = ratio, col
        print(f"[Report] Distance col found by numeric scan: '{best_col}' (ratio={best_ratio:.2f})")
        return best_col

    attendance_column = find_attendance_col(attendance_df)
    distance_column   = find_distance_col(distance_df)

    # ---- Build per-username lookup dicts DIRECTLY from source files ----
    # This avoids all merge column-collision issues completely.
    attendance_lookup = {}
    for _, r in attendance_df.iterrows():
        uname = str(r.get("Username", "")).strip().lower()
        val   = r.get(attendance_column)
        val_s = str(val).strip() if val is not None and str(val).lower() not in ["nan", "none", ""] else "N/A"
        if uname:
            attendance_lookup[uname] = val_s

    distance_lookup = {}
    for _, r in distance_df.iterrows():
        uname = str(r.get("Username", "")).strip().lower()
        val   = r.get(distance_column, 0)
        if pd.isna(val) or str(val).strip() in ["--", "nan", ""]:
            val = 0.0
        else:
            try:
                val = float(val)
            except Exception:
                val = 0.0
        if uname:
            distance_lookup[uname] = val

    print(f"[Report] Attendance lookup: {len(attendance_lookup)} entries | sample: {dict(list(attendance_lookup.items())[:3])}")
    print(f"[Report] Distance lookup  : {len(distance_lookup)} entries | sample: {dict(list(distance_lookup.items())[:3])}")

    # ---- Merge only employee info (name, circle, manager) ----
    employee_df_clean = employee_df.copy()
    employee_df_clean["_uname"] = employee_df_clean[emp_username_col]

    # ---- Build username → manager lookup from managers.xlsx ----
    # This is the dedicated "Manager Data" file uploaded via the portal.
    # It overrides whatever the employee file says about managers.
    username_to_manager = {}
    managers_file = DAILY_FILES.get("managers")
    if managers_file and os.path.exists(managers_file):
        try:
            mgr_df = pd.read_excel(managers_file)
            mgr_df.columns = mgr_df.columns.astype(str).str.strip()
            mgr_cols = list(mgr_df.columns)

            mgr_user_col = _find_col(mgr_cols, [
                "Field Executive Username", "FE Username", "Username",
                "User Name", "User ID", "Emp ID", "Employee ID",
            ])
            mgr_name_col = _find_col(mgr_cols, [
                "Reporting Manager", "Manager Name", "Manager", "Mgr",
                "Team Lead", "Supervisor", "Reported To",
            ])

            print(f"[Report] managers.xlsx — user col: '{mgr_user_col}', manager col: '{mgr_name_col}'")

            if mgr_user_col and mgr_name_col:
                for _, mrow in mgr_df.iterrows():
                    uname = str(mrow.get(mgr_user_col, "")).strip().lower()
                    mname = str(mrow.get(mgr_name_col, "")).strip()
                    if uname and mname and mname.lower() not in ["nan", "none", ""]:
                        username_to_manager[uname] = mname
                print(f"[Report] Manager lookup built: {len(username_to_manager)} entries")
            else:
                print("[Report] managers.xlsx: could not detect username or manager column")
        except Exception as e:
            print(f"[Report] managers.xlsx read error: {e}")
    else:
        print("[Report] managers.xlsx not found — using employee file for manager info")

    # ---- Build forms lookup from forms_filled.xlsx ----
    # Key: normalized employee full name → list of (form_name, count)
    forms_lookup = {}
    forms_filled_path = DAILY_FILES.get("forms_filled")
    if forms_filled_path and os.path.exists(forms_filled_path):
        try:
            ff_df = pd.read_excel(forms_filled_path, dtype=str).fillna("")
            ff_df.columns = ff_df.columns.str.strip()
            # Use the latest date in the file
            if "Action Date" in ff_df.columns:
                latest_ff_date = ff_df["Action Date"].replace("", pd.NA).dropna().max()
                ff_df = ff_df[ff_df["Action Date"] == latest_ff_date]
            for _, frow in ff_df.iterrows():
                emp_name  = str(frow.get("Employee Full Name", "")).strip()
                form_name = str(frow.get("Form Name", "")).strip().strip("\t")
                try:
                    cnt = int(str(frow.get("Records COUNT", "1")).strip())
                except (ValueError, TypeError):
                    cnt = 1
                if emp_name and form_name:
                    key = emp_name.lower()
                    forms_lookup.setdefault(key, {})
                    forms_lookup[key][form_name] = forms_lookup[key].get(form_name, 0) + cnt
            print(f"[Report] forms_filled lookup built: {len(forms_lookup)} employees")
        except Exception as e:
            print(f"[Report] forms_filled.xlsx read error: {e}")
    else:
        print("[Report] forms_filled.xlsx not found — forms column will be empty")

    manager_data    = defaultdict(list)
    circle_data     = defaultdict(list)
    management_data = defaultdict(list)

    # Build username → {name, email} for all employees (managers are also employees in the same file)
    username_to_info = {}
    for _, erow in employee_df.iterrows():
        uname = str(erow.get(emp_username_col, "")).strip().lower()
        fname = ""
        if full_name_col:
            n = erow.get(full_name_col)
            fname = str(n).strip() if n and str(n).lower() not in ["nan", "none", ""] else ""
        email = ""
        if email_col:
            e = erow.get(email_col)
            email = str(e).strip() if e and str(e).lower() not in ["nan", "none", ""] else ""
        if uname and uname not in ["nan", "none"]:
            username_to_info[uname] = {"name": fname or uname, "email": email}
    print(f"[Report] username_to_info built: {len(username_to_info)} entries")

    # Dynamically identify circle head usernames by matching phone numbers from
    # CIRCLE_HEADS against the uploaded employee file — no usernames hard-coded.
    def _norm_phone(p):
        return re.sub(r"\D", "", str(p))

    phone_col = _find_col(emp_cols, ["Phone", "Mobile", "Contact", "Phone Number"])
    phone_to_circle = {_norm_phone(v["phone"]): k for k, v in CIRCLE_HEADS.items() if v.get("phone")}

    def _scan_for_circle_heads(df, uname_col, ph_col):
        result = {}
        for _, erow in df.iterrows():
            uname = str(erow.get(uname_col, "")).strip().lower()
            phone = _norm_phone(erow.get(ph_col, ""))
            if phone and phone in phone_to_circle:
                result[uname] = phone_to_circle[phone]
        return result

    circle_head_unames = {}  # username → circle name (built from uploaded file)
    if phone_col:
        circle_head_unames = _scan_for_circle_heads(employee_df, emp_username_col, phone_col)
        print(f"[Report] Matched {len(circle_head_unames)} circle heads from employee.xlsx phones")

    # Fallback: scan managers.xlsx if employee.xlsx had no phone column or no matches
    if not circle_head_unames:
        mgr_file = DAILY_FILES.get("managers")
        if mgr_file and os.path.exists(mgr_file):
            try:
                fb_df = pd.read_excel(mgr_file, dtype=str).fillna("")
                fb_df.columns = fb_df.columns.astype(str).str.strip()
                fb_uname_col = _find_col(list(fb_df.columns), ["Field Executive Username", "Username"])
                fb_phone_col = _find_col(list(fb_df.columns), ["Phone", "Mobile", "Contact", "Phone Number"])
                if fb_uname_col and fb_phone_col:
                    circle_head_unames = _scan_for_circle_heads(fb_df, fb_uname_col, fb_phone_col)
                    print(f"[Report] Matched {len(circle_head_unames)} circle heads from managers.xlsx phones (fallback)")
            except Exception as e:
                print(f"[Report] managers.xlsx fallback scan error: {e}")

    if not circle_head_unames:
        print("[Report] WARNING: No circle heads matched — circle reports will not be sent")

    # Build parent map: employee username → reporting manager username (for circle lookup)
    parent_map = {}
    if manager_col:
        for _, erow in employee_df.iterrows():
            uname = str(erow.get(emp_username_col, "")).strip().lower()
            mgr   = str(erow.get(manager_col, "")).strip().lower()
            if uname and mgr and mgr not in ["nan", "none", ""]:
                parent_map[uname] = mgr
    if not parent_map and username_to_manager:
        # employee.xlsx has no Reporting Manager col — use the managers.xlsx mapping
        for uname, mgr_val in username_to_manager.items():
            parent_map[uname.lower()] = str(mgr_val).strip().lower()

    def find_circle(username, max_depth=8):
        """Walk up the reporting chain to find this employee's circle."""
        current = username
        for _ in range(max_depth):
            if current in circle_head_unames:
                return circle_head_unames[current]
            parent = parent_map.get(current)
            if not parent or parent == current:
                break
            current = parent
        return "Other"

    manager_email_map = {}  # manager display name → manager's actual email

    # Iterate over employee file — one row per employee
    for _, row in employee_df_clean.iterrows():
        username = str(row.get("_uname", "")).strip().lower()

        # Full name
        raw_name  = row.get(full_name_col) if full_name_col else None
        user_name = str(raw_name).strip() if raw_name and str(raw_name).lower() not in ["nan", "none", ""] else username

        # Circle — derived from reporting hierarchy (not city)
        circle = find_circle(username)

        # Manager — lookup dict first, then employee file's Reporting Manager column (contains username)
        manager_uname = username_to_manager.get(username)
        if not manager_uname and manager_col:
            raw_manager = row.get(manager_col)
            if raw_manager and str(raw_manager).lower() not in ["nan", "none", ""]:
                manager_uname = str(raw_manager).strip().lower()
        # Resolve manager username → display name and email via username_to_info
        mgr_info = username_to_info.get(manager_uname, {}) if manager_uname else {}
        manager = mgr_info.get("name") or manager_uname or circle
        if manager and manager not in manager_email_map and mgr_info.get("email"):
            manager_email_map[manager] = mgr_info["email"]

        # Attendance — direct lookup from attendance file (no merge ambiguity)
        attendance = attendance_lookup.get(username, "N/A")

        # Distance — direct lookup from distance file
        distance = distance_lookup.get(username, 0.0)

        # Forms — from forms_filled.xlsx, matched by employee full name
        emp_forms = forms_lookup.get(user_name.lower(), {})
        if not emp_forms:
            form_display = '<span style="color:#9e9e9e;font-size:12px">No forms</span>'
        else:
            form_display = "<br>".join(
                f'<span class="badge badge-ok">{fname}</span> ×{cnt}'
                for fname, cnt in sorted(emp_forms.items())
            )

        user_record = {
            "user":       user_name,
            "attendance": attendance,
            "distance":   distance,   # keep exact float from the file
            "form_names": form_display,
        }

        manager_data[manager].append(user_record)
        circle_data[circle].append(user_record)
        management_data[circle].append(user_record)

    # =====================================================
    # ALARM / SITE DOWN PROCESSING
    # =====================================================

    site_down_data = defaultdict(list)

    # Resolve alarm file: use provided, or fallback to legacy glob
    resolved_alarm = alarm_file
    if not resolved_alarm or not os.path.exists(resolved_alarm):
        resolved_alarm = get_latest_alarm_file()

    if resolved_alarm:
        alarm_df = pd.read_csv(resolved_alarm)
        alarm_df = alarm_df.drop_duplicates(subset=["Global ID"])

        db = SessionLocal()
        for _, row in alarm_df.iterrows():
            circle    = str(row.get("State/Circle", "Unknown")).strip()
            site_id   = str(row.get("Global ID", "Unknown")).strip()
            site_name = str(row.get("Site Name", "Unknown")).strip()

            existing = db.query(SiteMonitoring).filter(
                SiteMonitoring.global_id == site_id
            ).first()

            if not existing:
                record = SiteMonitoring(
                    global_id=site_id,
                    site_name=site_name,
                    circle=circle,
                    status="Inactive",
                    alarm="Site Down",
                )
                db.add(record)

            site_down_data[circle].append({
                "site_id":   site_id,
                "site_name": site_name,
            })

        db.commit()
        db.close()
    else:
        print("[Report] Skipping alarm data — no alarm file found.")

    # =====================================================
    # SEND — MANAGER REPORTS
    # =====================================================

    for manager, users in manager_data.items():
        body = build_manager_email(manager, users, report_date)
        # Use the manager's actual email if we found it; fall back to admin
        recipient = manager_email_map.get(manager, "pranjalg.work@gmail.com")
        send_email(
            [recipient],
            f"[Daily Report] Manager — {manager} | {report_date}",
            body,
        )
        print(f"[Report] Manager report -> {manager} ({recipient})")

    print("[Report] All manager reports sent.")

    # =====================================================
    # SEND — CIRCLE HEAD REPORTS
    # =====================================================

    for circle, users in circle_data.items():
        sites     = site_down_data.get(circle, [])
        ch        = CIRCLE_HEADS.get(circle, {})
        head_name = ch.get("head", circle)
        recipient = ch.get("email", "pranjalg.work@gmail.com")
        body      = build_circle_email(circle, head_name, users, sites, report_date)
        send_email(
            [recipient],
            f"[Daily Report] Circle {circle} — {head_name} | {report_date}",
            body,
        )
        print(f"[Report] Circle report -> {circle} ({head_name}) -> {recipient}")

    print("[Report] Circle reports sent.")

    # =====================================================
    # SEND — MANAGEMENT REPORT
    # =====================================================

    body = build_management_email(management_data, site_down_data, report_date)
    send_email(
        ["pranjalg.work@gmail.com"],
        f"[Daily Report] All Circles — Management Summary | {report_date}",
        body,
    )

    print("[Report] Management report sent.")
    return {"success": True}


# =====================================================
# PUBLIC ENTRY POINTS
# =====================================================

def send_daily_report():
    """
    Called by the scheduler (6 PM daily).
    Uses files uploaded via the portal (data/daily/).
    Falls back to legacy hardcoded paths if daily files are not present.
    """
    att  = DAILY_FILES["attendance"]
    dist = DAILY_FILES["distance"]
    emp  = DAILY_FILES["employee"]
    alrm = DAILY_FILES["alarm"]

    # Fall back to legacy paths if daily uploads not found
    if not os.path.exists(att):
        att = LEGACY_FILES["attendance"]
    if not os.path.exists(dist):
        dist = LEGACY_FILES["distance"]
    if not os.path.exists(emp):
        emp = LEGACY_FILES["employee"]

    _run_report(
        attendance_file=att,
        distance_file=dist,
        employee_file=emp,
        alarm_file=alrm if os.path.exists(alrm) else None,
    )


def send_report_now():
    """
    Called by the API when the user clicks 'Send Reports Now'.
    Requires files to be present in data/daily/.
    Returns dict with success/error info.
    """
    missing = []
    for key in ["employee", "attendance", "distance", "forms_filled"]:
        if not os.path.exists(DAILY_FILES[key]):
            missing.append(key)

    if missing:
        return {
            "success": False,
            "error": f"Required files not uploaded yet: {', '.join(missing)}"
        }

    return _run_report(
        attendance_file=DAILY_FILES["attendance"],
        distance_file=DAILY_FILES["distance"],
        employee_file=DAILY_FILES["employee"],
        alarm_file=DAILY_FILES["alarm"] if os.path.exists(DAILY_FILES["alarm"]) else None,
    )


if __name__ == "__main__":
    send_daily_report()
