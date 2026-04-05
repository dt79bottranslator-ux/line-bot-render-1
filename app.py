import os
import json
import html
import hmac
import hashlib
import base64
import threading
from datetime import datetime, timezone, timedelta

import gspread
import requests
from flask import Flask, request, jsonify
from oauth2client.service_account import ServiceAccountCredentials

app = Flask(__name__)

# ========================
# ENV
# ========================
LINE_CHANNEL_ACCESS_TOKEN = os.getenv("LINE_CHANNEL_ACCESS_TOKEN", "").strip()
LINE_CHANNEL_SECRET = os.getenv("LINE_CHANNEL_SECRET", "").strip()
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY", "").strip()
GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID", "").strip()
PORT = int(os.getenv("PORT", "10000"))

TRANSLATION_LOG_TAB = "TRANSLATION_LOG"
BOT_CONFIG_TAB = "BOT_CONFIG"

HELP_TEXT = "Dùng:\n/zh nội dung\n/vi nội dung\n/id nội dung\n/en nội dung"

# ========================
# SIMPLE CACHE
# ========================
BOT_CONFIG_CACHE = {
    "data": {},
    "loaded_at": None
}
BOT_CONFIG_TTL_SECONDS = 60

RECENT_EVENT_CACHE = {}
RECENT_EVENT_LOCK = threading.Lock()
RECENT_EVENT_TTL_SECONDS = 180


# ========================
# COMMON UTILS
# ========================
def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def safe_text(value, max_len: int = 5000) -> str:
    if value is None:
        return ""
    return str(value).strip()[:max_len]


def cleanup_recent_event_cache():
    now = datetime.now(timezone.utc)
    expired_keys = []

    with RECENT_EVENT_LOCK:
        for event_id, created_at in RECENT_EVENT_CACHE.items():
            if now - created_at > timedelta(seconds=RECENT_EVENT_TTL_SECONDS):
                expired_keys.append(event_id)

        for key in expired_keys:
            RECENT_EVENT_CACHE.pop(key, None)


def remember_event(event_id: str):
    if not event_id:
        return
    cleanup_recent_event_cache()
    with RECENT_EVENT_LOCK:
        RECENT_EVENT_CACHE[event_id] = datetime.now(timezone.utc)


def recently_seen_event(event_id: str) -> bool:
    if not event_id:
        return False
    cleanup_recent_event_cache()
    with RECENT_EVENT_LOCK:
        return event_id in RECENT_EVENT_CACHE


# ========================
# GOOGLE SHEET
# ========================
def get_gspread_client():
    if not GOOGLE_SERVICE_ACCOUNT_JSON:
        raise ValueError("Missing GOOGLE_SERVICE_ACCOUNT_JSON")

    creds_dict = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    return gspread.authorize(creds)


def get_sheet(tab_name: str):
    client = get_gspread_client()
    spreadsheet = client.open_by_key(SPREADSHEET_ID)
    return spreadsheet.worksheet(tab_name)


def load_bot_config(force_reload: bool = False) -> dict:
    now = datetime.now(timezone.utc)

    if not force_reload:
        loaded_at = BOT_CONFIG_CACHE["loaded_at"]
        if loaded_at and (now - loaded_at).total_seconds() < BOT_CONFIG_TTL_SECONDS:
            return BOT_CONFIG_CACHE["data"]

    config = {}
    try:
        ws = get_sheet(BOT_CONFIG_TAB)
        rows = ws.get_all_records()
        for row in rows:
            key = safe_text(row.get("Key", ""), 200)
            value = row.get("Value", "")
            if key:
                config[key] = value

        BOT_CONFIG_CACHE["data"] = config
        BOT_CONFIG_CACHE["loaded_at"] = now
    except Exception as e:
        print(f"[BOT_CONFIG ERROR] {e}")

    return BOT_CONFIG_CACHE["data"]


def get_fallback_message() -> str:
    config = load_bot_config()
    fallback = safe_text(config.get("FALLBACK_MESSAGE", ""), 500)
    if fallback:
        return fallback
    return "Hệ thống bận, thử lại sau."


# ========================
# SECURITY
# ========================
def verify_signature(req) -> bool:
    if not LINE_CHANNEL_SECRET:
        print("[SECURITY ERROR] Missing LINE_CHANNEL_SECRET")
        return False

    signature = req.headers.get("X-Line-Signature", "").strip()
    body = req.get_data(as_text=True)

    digest = hmac.new(
        LINE_CHANNEL_SECRET.encode("utf-8"),
        body.encode("utf-8"),
        hashlib.sha256
    ).digest()

    expected_signature = base64.b64encode(digest).decode("utf-8")
    return hmac.compare_digest(signature, expected_signature)


# ========================
# ROOT / HEALTH
# ========================
@app.route("/", methods=["GET"])
def root():
    return "BOT RUNNING", 200


@app.route("/health", methods=["GET"])
def health():
    return jsonify({
        "status": "ok",
        "line_token_exists": bool(LINE_CHANNEL_ACCESS_TOKEN),
        "line_secret_exists": bool(LINE_CHANNEL_SECRET),
        "google_api_key_exists": bool(GOOGLE_API_KEY),
        "google_service_account_exists": bool(GOOGLE_SERVICE_ACCOUNT_JSON),
        "spreadsheet_id_exists": bool(SPREADSHEET_ID)
    }), 200


# ========================
# GOOGLE TRANSLATE
# ========================
def detect_language(text: str) -> str:
    if not GOOGLE_API_KEY:
        print("[DETECT ERROR] Missing GOOGLE_API_KEY")
        return "unknown"

    url = "https://translation.googleapis.com/language/translate/v2/detect"
    payload = {
        "q": text,
        "key": GOOGLE_API_KEY
    }

    res = requests.post(url, data=payload, timeout=20)
    if res.status_code != 200:
        print(f"[DETECT ERROR] status={res.status_code} body={res.text}")
        return "unknown"

    try:
        return res.json()["data"]["detections"][0][0]["language"]
    except Exception as e:
        print(f"[DETECT PARSE ERROR] {e}")
        return "unknown"


def translate_text(text: str, target_lang: str) -> str:
    if not GOOGLE_API_KEY:
        raise RuntimeError("Missing GOOGLE_API_KEY")

    url = "https://translation.googleapis.com/language/translate/v2"
    payload = {
        "q": text,
        "target": target_lang,
        "key": GOOGLE_API_KEY
    }

    res = requests.post(url, data=payload, timeout=20)
    if res.status_code != 200:
        raise RuntimeError(f"Translate API error: {res.status_code} {res.text}")

    try:
        translated = res.json()["data"]["translations"][0]["translatedText"]
        return html.unescape(translated)
    except Exception as e:
        raise RuntimeError(f"Translate API parse failed: {e}")


# ========================
# LOGGING
# ========================
def append_translation_log(
    event_id: str,
    timestamp: str,
    user_id: str,
    source_type: str,
    group_id: str,
    room_id: str,
    input_text: str,
    detected_lang: str,
    target_lang: str,
    translated_text: str,
    status: str,
    error_message: str
):
    ws = get_sheet(TRANSLATION_LOG_TAB)
    ws.append_row([
        safe_text(event_id, 200),
        safe_text(timestamp, 100),
        safe_text(user_id, 200),
        safe_text(source_type, 50),
        safe_text(group_id, 200),
        safe_text(room_id, 200),
        safe_text(input_text, 5000),
        safe_text(detected_lang, 50),
        safe_text(target_lang, 50),
        safe_text(translated_text, 5000),
        safe_text(status, 50),
        safe_text(error_message, 5000)
    ])


def event_exists_in_sheet(event_id: str) -> bool:
    try:
        ws = get_sheet(TRANSLATION_LOG_TAB)
        cell = ws.find(event_id, in_column=1)
        return cell is not None
    except gspread.exceptions.CellNotFound:
        return False
    except Exception as e:
        print(f"[IDEMPOTENCY CHECK ERROR] {e}")
        return False


def is_duplicate_event(event_id: str) -> bool:
    if not event_id:
        return False

    if recently_seen_event(event_id):
        return True

    if event_exists_in_sheet(event_id):
        remember_event(event_id)
        return True

    return False


# ========================
# LINE REPLY
# ========================
def reply_text(reply_token: str, text: str):
    if not LINE_CHANNEL_ACCESS_TOKEN:
        print("[LINE REPLY ERROR] Missing LINE_CHANNEL_ACCESS_TOKEN")
        return

    url = "https://api.line.me/v2/bot/message/reply"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}"
    }
    payload = {
        "replyToken": reply_token,
        "messages": [
            {"type": "text", "text": safe_text(text, 5000)}
        ]
    }

    try:
        res = requests.post(url, headers=headers, json=payload, timeout=20)
        print(f"[LINE REPLY] status={res.status_code} body={res.text}")
    except Exception as e:
        print(f"[LINE REPLY ERROR] {e}")


# ========================
# BUSINESS LOGIC
# ========================
def resolve_target_and_content(input_text: str):
    text = safe_text(input_text, 5000)

    if text == "/zh":
        return "zh-TW", "", True
    if text == "/vi":
        return "vi", "", True
    if text == "/id":
        return "id", "", True
    if text == "/en":
        return "en", "", True

    if text.startswith("/zh "):
        return "zh-TW", text[4:].strip(), True
    if text.startswith("/vi "):
        return "vi", text[4:].strip(), True
    if text.startswith("/id "):
        return "id", text[4:].strip(), True
    if text.startswith("/en "):
        return "en", text[4:].strip(), True

    return "", text, False


# ========================
# WEBHOOK
# ========================
@app.route("/webhook", methods=["POST"])
def webhook():
    if not verify_signature(request):
        return "INVALID SIGNATURE", 403

    data = request.get_json(silent=True)
    if not data:
        return "NO DATA", 400

    events = data.get("events", [])

    for event in events:
        if event.get("type") != "message":
            continue

        message = event.get("message", {})
        if message.get("type") != "text":
            continue

        reply_token = safe_text(event.get("replyToken", ""), 200)
        input_text = safe_text(message.get("text", ""), 5000)
        line_message_id = safe_text(message.get("id", ""), 200)

        if not reply_token or not input_text:
            continue

        source = event.get("source", {})
        user_id = safe_text(source.get("userId", ""), 200)
        source_type = safe_text(source.get("type", ""), 50)
        group_id = safe_text(source.get("groupId", ""), 200)
        room_id = safe_text(source.get("roomId", ""), 200)

        event_id = line_message_id
        timestamp = utc_now_iso()

        if not event_id:
            print("[SKIP EVENT] Missing LINE message id")
            continue

        print(f"[EVENT] event_id={event_id} user_id={user_id} source_type={source_type}")

        if is_duplicate_event(event_id):
            print(f"[SKIP DUPLICATE] event_id={event_id}")
            continue

        # Khóa ngay trong bộ nhớ để chặn retry sát nhau
        remember_event(event_id)

        detected_lang = "unknown"
        target_lang = ""
        translated_text = ""
        status = "success"
        error_message = ""

        try:
            target_lang, content_to_translate, is_command = resolve_target_and_content(input_text)

            if is_command:
                if not content_to_translate:
                    translated_text = HELP_TEXT
                else:
                    detected_lang = detect_language(content_to_translate)
                    translated_text = translate_text(content_to_translate, target_lang)
            else:
                translated_text = HELP_TEXT

            reply_text(reply_token, translated_text)

        except Exception as e:
            status = "error"
            error_message = safe_text(str(e), 5000)
            translated_text = get_fallback_message()
            reply_text(reply_token, translated_text)

        try:
            append_translation_log(
                event_id=event_id,
                timestamp=timestamp,
                user_id=user_id,
                source_type=source_type,
                group_id=group_id,
                room_id=room_id,
                input_text=input_text,
                detected_lang=detected_lang,
                target_lang=target_lang,
                translated_text=translated_text,
                status=status,
                error_message=error_message
            )
        except Exception as log_err:
            print(f"[LOG ERROR] {log_err}")

    return "OK", 200


# ========================
# RUN
# ========================
if __name__ == "__main__":
    print("[BOOT] Flask app starting...")
    print(f"[BOOT] PORT={PORT}")
    print(f"[BOOT] line_token_exists={bool(LINE_CHANNEL_ACCESS_TOKEN)}")
    print(f"[BOOT] line_secret_exists={bool(LINE_CHANNEL_SECRET)}")
    print(f"[BOOT] google_api_key_exists={bool(GOOGLE_API_KEY)}")
    print(f"[BOOT] google_service_account_exists={bool(GOOGLE_SERVICE_ACCOUNT_JSON)}")
    print(f"[BOOT] spreadsheet_id_exists={bool(SPREADSHEET_ID)}")

    app.run(host="0.0.0.0", port=PORT)
