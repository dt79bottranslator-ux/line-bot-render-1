# =========================================================
# IMPORT
# =========================================================
import os
import json
import html
import hmac
import hashlib
import base64
import time
import uuid
import logging
from datetime import datetime, timezone, timedelta

import requests
from flask import Flask, request

app = Flask(__name__)

# =========================================================
# LOGGING
# =========================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

# =========================================================
# ENV
# =========================================================
LINE_CHANNEL_ACCESS_TOKEN = os.getenv("LINE_CHANNEL_ACCESS_TOKEN", "").strip()
LINE_CHANNEL_SECRET = os.getenv("LINE_CHANNEL_SECRET", "").strip()
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY", "").strip()

# 👉 ADMIN
ADMIN_IDS = os.getenv("ADMIN_IDS", "")
ADMIN_LIST = [x.strip() for x in ADMIN_IDS.split(",") if x.strip()]

def is_admin(user_id: str) -> bool:
    return user_id in ADMIN_LIST

TW_TZ = timezone(timedelta(hours=8))

# =========================================================
# CONSTANT
# =========================================================
LOCKED_TARGET_LANG = "zh-TW"
HTTP_TIMEOUT_SECONDS = 15
FALLBACK_REPLY_TEXT = "Hệ thống bận, thử lại sau."

# =========================================================
# BASIC
# =========================================================
def make_trace_id():
    return f"trc_{uuid.uuid4().hex[:12]}"

def safe_str(v):
    return str(v).strip() if v else ""

def extract_source_ids(event):
    s = event.get("source", {})
    return (
        safe_str(s.get("type")),
        safe_str(s.get("userId")),
        safe_str(s.get("groupId")),
        safe_str(s.get("roomId"))
    )

# =========================================================
# LINE
# =========================================================
def verify_line_signature(secret, body, signature):
    digest = hmac.new(secret.encode(), body, hashlib.sha256).digest()
    computed = base64.b64encode(digest).decode()
    return hmac.compare_digest(computed, signature)

def line_reply(reply_token, text, trace_id):
    url = "https://api.line.me/v2/bot/message/reply"
    headers = {
        "Authorization": f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}",
        "Content-Type": "application/json"
    }
    payload = {
        "replyToken": reply_token,
        "messages": [{"type": "text", "text": text}]
    }
    try:
        r = requests.post(url, headers=headers, json=payload, timeout=HTTP_TIMEOUT_SECONDS)
        logger.info(f"[{trace_id}] LINE {r.status_code}")
        return r.status_code == 200
    except Exception as e:
        logger.error(f"[{trace_id}] LINE ERROR: {e}")
        return False

# =========================================================
# GOOGLE TRANSLATE
# =========================================================
def detect_lang(text, trace_id):
    try:
        url = "https://translation.googleapis.com/language/translate/v2/detect"
        r = requests.post(url, params={"key": GOOGLE_API_KEY}, data={"q": text}, timeout=HTTP_TIMEOUT_SECONDS)
        if r.status_code != 200:
            return None
        return r.json()["data"]["detections"][0][0]["language"]
    except:
        return None

def translate(text, target, source, trace_id):
    try:
        url = "https://translation.googleapis.com/language/translate/v2"
        data = {"q": text, "target": target}
        if source:
            data["source"] = source
        r = requests.post(url, params={"key": GOOGLE_API_KEY}, data=data, timeout=HTTP_TIMEOUT_SECONDS)
        if r.status_code != 200:
            return None
        return html.unescape(r.json()["data"]["translations"][0]["translatedText"])
    except:
        return None

# =========================================================
# WEBHOOK
# =========================================================
@app.route("/callback", methods=["POST"])
def callback():
    trace_id = make_trace_id()
    body = request.get_data()
    sig = request.headers.get("X-Line-Signature", "")

    if not verify_line_signature(LINE_CHANNEL_SECRET, body, sig):
        return "Invalid", 403

    payload = json.loads(body.decode())
    event = payload["events"][0]

    source_type, user_id, group_id, room_id = extract_source_ids(event)

    # 👉 DEBUG USER_ID
    logger.info(f"[ADMIN_DEBUG] user_id={user_id}")

    input_text = safe_str(event["message"]["text"])
    reply_token = event["replyToken"]

    # =====================================================
    # COMMAND: !all
    # =====================================================
    if input_text.startswith("!all"):
        content = input_text.replace("!all", "", 1).strip()

        if not is_admin(user_id):
            line_reply(reply_token, "❌ Không có quyền", trace_id)
            return "OK", 200

        if not content:
            line_reply(reply_token, "⚠️ !all cần nội dung", trace_id)
            return "OK", 200

        lang = detect_lang(content, trace_id)
        translated = translate(content, LOCKED_TARGET_LANG, lang, trace_id)

        msg = f"📢 THÔNG BÁO:\n{translated or content}"
        line_reply(reply_token, msg, trace_id)
        return "OK", 200

    # =====================================================
    # NORMAL TRANSLATE
    # =====================================================
    lang = detect_lang(input_text, trace_id)
    translated = translate(input_text, LOCKED_TARGET_LANG, lang, trace_id)

    line_reply(reply_token, translated or FALLBACK_REPLY_TEXT, trace_id)
    return "OK", 200

# =========================================================
# MAIN
# =========================================================
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
