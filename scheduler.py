# scheduler.py
from apscheduler.schedulers.background import BackgroundScheduler
from services.notification_service import send_daily_report

scheduler = BackgroundScheduler()


def start_scheduler():
    scheduler.add_job(
        send_daily_report,
        trigger="cron",
        hour=18,
        minute=0,
        id="daily_field_report",
        replace_existing=True
    )
    scheduler.start()
    print("[Scheduler] Daily report job started — runs at 18:00")


def stop_scheduler():
    if scheduler.running:
        scheduler.shutdown()
        print("[Scheduler] Stopped")# =====================================================
# SCHEDULER - FIXED (NO CIRCULAR IMPORT)
# =====================================================

from apscheduler.schedulers.background import BackgroundScheduler
from database import SessionLocal

scheduler = BackgroundScheduler()


# =====================================================
# JOB FUNCTION (IMPORT INSIDE FUNCTION ✅)
# =====================================================
def update_site_monitoring_job():
    print("🔄 Running Site Monitoring Job...")

    db = SessionLocal()

    try:
        # 🔥 IMPORT HERE (not at top)
        from main import build_site_monitoring, save_site_monitoring_to_db

        _, up_sites, down_sites = build_site_monitoring()
        save_site_monitoring_to_db(db, up_sites, down_sites)

        print("✅ Site Monitoring Updated Successfully")

    except Exception as e:
        print("❌ Scheduler Error:", str(e))

    finally:
        db.close()


# =====================================================
# START
# =====================================================
def start_scheduler():
    scheduler.add_job(
        update_site_monitoring_job,
        trigger="interval",
        minutes=10,
        id="site_monitoring_job",
        replace_existing=True
    )

    scheduler.start()
    print("🚀 Scheduler started (every 10 min)")


# =====================================================
# STOP
# =====================================================
def stop_scheduler():
    if scheduler.running:
        scheduler.shutdown()
        print("🛑 Scheduler stopped")