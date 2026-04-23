import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

SENDER_EMAIL = "pranjalg.work@gmail.com"
APP_PASSWORD  = "ajgprixzqbqduhmw"


def send_email(recipients, subject, body):
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = SENDER_EMAIL
    msg["To"]      = ", ".join(recipients)
    msg.attach(MIMEText(body, "html"))

    server = None
    try:
        server = smtplib.SMTP("smtp.gmail.com", 587)
        server.starttls()
        server.login(SENDER_EMAIL, APP_PASSWORD)
        server.sendmail(SENDER_EMAIL, recipients, msg.as_string())
        print(f"[Email] Sent: {subject} to {recipients}")
    except smtplib.SMTPAuthenticationError:
        print("[Email] Authentication failed — check sender email / App Password")
    except smtplib.SMTPException as e:
        print(f"[Email] SMTP error: {e}")
    except Exception as e:
        print(f"[Email] Unexpected error: {e}")
    finally:
        if server:
            try:
                server.quit()
            except Exception:
                pass
