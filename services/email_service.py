import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from email.mime.base import MIMEBase
from email import encoders

SENDER_EMAIL = "pranjalg.work@gmail.com"
APP_PASSWORD  = "ajgprixzqbqduhmw"


def send_email(recipients, subject, body, inline_images=None, attachments=None):
    """
    Send an HTML email.
    inline_images: list of (cid_name, bytes_data, mime_subtype)
    attachments:   list of (filename, bytes_data)  e.g. [("report.xlsx", xlsx_bytes)]
    """
    test_address = os.environ.get("TEST_EMAIL")
    if test_address:
        print(f"[Email] TEST MODE — redirecting to {test_address} (original: {recipients})")
        recipients = [test_address]
        subject    = f"[TEST] {subject}"

    # outer container — always mixed so we can attach files
    outer = MIMEMultipart("mixed")
    outer["Subject"] = subject
    outer["From"]    = SENDER_EMAIL
    outer["To"]      = ", ".join(recipients)

    if inline_images:
        # related wraps html + inline images
        related = MIMEMultipart("related")
        alt = MIMEMultipart("alternative")
        alt.attach(MIMEText(body, "html"))
        related.attach(alt)
        for cid, img_bytes, subtype in inline_images:
            part = MIMEImage(img_bytes, _subtype=subtype)
            part.add_header("Content-ID", f"<{cid}>")
            part.add_header("Content-Disposition", "inline", filename=f"{cid}.{subtype}")
            related.attach(part)
        outer.attach(related)
    else:
        alt = MIMEMultipart("alternative")
        alt.attach(MIMEText(body, "html"))
        outer.attach(alt)

    if attachments:
        for filename, file_bytes in attachments:
            part = MIMEBase("application", "octet-stream")
            part.set_payload(file_bytes)
            encoders.encode_base64(part)
            part.add_header("Content-Disposition", "attachment", filename=filename)
            outer.attach(part)

    server = None
    try:
        server = smtplib.SMTP("smtp.gmail.com", 587)
        server.starttls()
        server.login(SENDER_EMAIL, APP_PASSWORD)
        server.sendmail(SENDER_EMAIL, recipients, outer.as_string())
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
