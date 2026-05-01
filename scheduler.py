# =====================================================
# SCHEDULER — SINGLE INSTANCE, TWO JOBS
# =====================================================

from apscheduler.schedulers.background import BackgroundScheduler
from database import SessionLocal

_scheduler = BackgroundScheduler()


# =====================================================
# JOB 1 — SITE MONITORING (every 10 min)
# =====================================================

def update_site_monitoring_job():
    print("[Scheduler] Running Site Monitoring Job...")
    db = SessionLocal()
    try:
        # Import inside function to avoid circular import
        from main import build_site_monitoring, save_site_monitoring_to_db
        _, up_sites, down_sites = build_site_monitoring()
        save_site_monitoring_to_db(db, up_sites, down_sites)
        print("[Scheduler] Site Monitoring Updated Successfully")
    except Exception as e:
        print("[Scheduler] Site Monitoring Error:", str(e))
    finally:
        db.close()


# =====================================================
# JOB 2 — DAILY EMAIL REPORT (every day at 18:00)
# =====================================================

def run_daily_report_job():
    print("[Scheduler] Running Daily Email Report Job...")
    try:
        from services.notification_service import send_daily_report
        send_daily_report()
    except Exception as e:
        print("[Scheduler] Daily Report Error:", str(e))


# =====================================================
# START / STOP
# =====================================================

def start_scheduler():
    _scheduler.add_job(
        update_site_monitoring_job,
        trigger="interval",
        minutes=10,
        id="site_monitoring_job",
        replace_existing=True
    )

    _scheduler.start()
    print("[Scheduler] Started — site monitoring every 10 min (email report disabled)")


def stop_scheduler():
    if _scheduler.running:
        _scheduler.shutdown()
        print("[Scheduler] Stopped")
