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
from typing import Dict, Any, Optional, Tuple, List

import requests
import gspread
from flask import Flask, request, jsonify
from oauth2client.service_account import ServiceAccountCredentials

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
GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
GOOGLE_SHEET_NAME = os.getenv("GOOGLE_SHEET_NAME", "").strip()
GOOGLE_SHEET_WORKSHEET = os.getenv("GOOGLE_SHEET_WORKSHEET", "FORENSIC_JOURNAL").strip()
BALANCE_WORKSHEET = os.getenv("BALANCE_WORKSHEET", "BALANCE").strip()
FREE_DAILY_CHAR_LIMIT = int(os.getenv("FREE_DAILY_CHAR_LIMIT", "1000"))

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
def now_tw_iso():
    return datetime.now(TW_TZ).isoformat()

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

def get_billing_target(source_type, user_id, group_id, room_id):
    if source_type == "group" and group_id:
        return "GROUP", group_id
    if source_type == "room" and room_id:
        return "ROOM", room_id
    return "USER", user_id

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
    except:
        return False

# =========================================================
# GOOGLE TRANSLATE
# =========================================================
def detect_lang(text, trace_id):
    url = "https://translation.googleapis.com/language/translate/v2/detect"
    r = requests.post(url, params={"key": GOOGLE_API_KEY}, data={"q": text})
    if r.status_code != 200:
        return None
    return r.json()["data"]["detections"][0][0]["language"]

def translate(text, target, source, trace_id):
    url = "https://translation.googleapis.com/language/translate/v2"
    data = {"q": text, "target": target}
    if source:
        data["source"] = source
    r = requests.post(url, params={"key": GOOGLE_API_KEY}, data=data)
    if r.status_code != 200:
        return None
    return html.unescape(r.json()["data"]["translations"][0]["translatedText"])

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
    logger.info(f"[ADMIN_DEBUG] user_id={user_id}")
    input_text = safe_str(event["message"]["text"])
    reply_token = event["replyToken"]

    # =====================================================
    # COMMAND DETECT
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

        msg = f"📢 THÔNG BÁO:\n{translated}"
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
