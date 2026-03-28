"""Email sending via SMTP (Gmail-compatible)."""

import logging
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

logger = logging.getLogger(__name__)

SMTP_HOST = os.environ.get("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.environ.get("SMTP_PORT", "587"))


def _smtp_creds():
    """Read credentials at call time (env may load after import). Strip spaces from Gmail app passwords."""
    user = os.environ.get("SMTP_USER", "").strip()
    pw = os.environ.get("SMTP_PASSWORD", "").replace(" ", "").strip()
    from_addr = os.environ.get("SMTP_FROM", "").strip() or user
    return user, pw, from_addr


def is_configured():
    u, p, _ = _smtp_creds()
    return bool(u and p)


def send_magic_link(to_email: str, to_name: str, magic_link: str, expires: str):
    """Send a magic login link to a newly approved user."""
    subject = "Your DHS Explorer Access Has Been Approved"
    html = f"""\
<div style="font-family:Arial,sans-serif;max-width:560px;margin:0 auto;padding:24px">
  <div style="text-align:center;margin-bottom:24px">
    <div style="display:inline-block;background:#e0e7ff;color:#4f46e5;width:56px;height:56px;
                border-radius:14px;line-height:56px;font-size:24px;font-weight:bold">D</div>
    <h2 style="margin:12px 0 4px;color:#1e293b">DHS Explorer</h2>
  </div>

  <p>Hi <strong>{to_name}</strong>,</p>
  <p>Your access request has been approved! Click the button below to sign in:</p>

  <div style="text-align:center;margin:28px 0">
    <a href="{magic_link}"
       style="display:inline-block;background:#4f46e5;color:#fff;padding:12px 32px;
              border-radius:8px;text-decoration:none;font-weight:600;font-size:15px">
      Sign In to DHS Explorer
    </a>
  </div>

  <p style="font-size:13px;color:#64748b">
    This link is <strong>single-use</strong> and expires in 7 days ({expires[:10]}).
    <br>If you did not request access, please ignore this email.
  </p>

  <hr style="border:none;border-top:1px solid #e2e8f0;margin:24px 0">
  <p style="font-size:12px;color:#94a3b8;text-align:center">
    DHS Data Explorer &middot; Demographic and Health Survey Database
  </p>
</div>"""

    plain = (
        f"Hi {to_name},\n\n"
        f"Your DHS Explorer access has been approved.\n\n"
        f"Sign in here: {magic_link}\n\n"
        f"This link is single-use and expires on {expires[:10]}.\n"
    )

    return _send(to_email, subject, html, plain)


def send_access_notification(admin_email: str, requester_name: str, requester_email: str, manage_url: str):
    """Notify the admin that someone has requested access."""
    subject = f"DHS Explorer: New access request from {requester_name}"
    html = f"""\
<div style="font-family:Arial,sans-serif;max-width:560px;margin:0 auto;padding:24px">
  <h3 style="color:#1e293b">New Access Request</h3>
  <p><strong>{requester_name}</strong> ({requester_email}) has requested access to DHS Explorer.</p>
  <div style="text-align:center;margin:24px 0">
    <a href="{manage_url}"
       style="display:inline-block;background:#4f46e5;color:#fff;padding:10px 28px;
              border-radius:8px;text-decoration:none;font-weight:600">
      Review Requests
    </a>
  </div>
</div>"""

    plain = (
        f"New access request from {requester_name} ({requester_email}).\n\n"
        f"Review at: {manage_url}\n"
    )

    return _send(admin_email, subject, html, plain)


def _send(to: str, subject: str, html: str, plain: str) -> bool:
    smtp_user, smtp_pw, from_addr = _smtp_creds()
    if not smtp_user or not smtp_pw:
        logger.warning("Email not configured (SMTP_USER / SMTP_PASSWORD not set). Skipping send to %s", to)
        return False

    msg = MIMEMultipart("alternative")
    msg["From"] = from_addr
    msg["To"] = to
    msg["Subject"] = subject
    msg.attach(MIMEText(plain, "plain"))
    msg.attach(MIMEText(html, "html"))

    try:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30) as server:
            server.starttls()
            server.login(smtp_user, smtp_pw)
            server.sendmail(from_addr, [to], msg.as_string())
        logger.info("Email sent to %s: %s", to, subject)
        return True
    except Exception:
        logger.exception("Failed to send email to %s", to)
        return False
