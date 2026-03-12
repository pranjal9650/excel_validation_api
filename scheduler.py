from apscheduler.schedulers.background import BackgroundScheduler
from services.notification_service import send_daily_report
import atexit

scheduler = BackgroundScheduler()

# Run daily report at 6:00 PM
scheduler.add_job(
    send_daily_report,
    trigger="cron",
    hour=18,
    minute=0,
    id="daily_field_report",
    replace_existing=True
)

print("Scheduler started. Daily report will run at 18:00.")

scheduler.start()

# Proper shutdown when application stops
atexit.register(lambda: scheduler.shutdown())