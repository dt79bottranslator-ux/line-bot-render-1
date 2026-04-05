import os
import json
import html
import hmac
import hashlib
import base64
from datetime import datetime, timezone

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

_sheet_client = None
_sheet_cache = {}
_bot_config_cache = None


# ========================
# GOOGLE SHEET
# ========================
def get_gspread_client():
    global _sheet_client

    if _sheet_client is not None:
        return _sheet_client

    if not GOOGLE_SERVICE_ACCOUNT_JSON:
        raise ValueError("Missing GOOGLE_SERVICE_ACCOUNT_JSON")

    creds_dict = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    _sheet_client = gspread.authorize(creds)
    return _sheet_client


def get_sheet(tab_name: str):
    global _sheet_cache

    if tab_name in _sheet_cache:
        return _sheet_cache[tab_name]

    client = get_gspread_client()
    spreadsheet = client.open_by_key(SPREADSHEET_ID)
    worksheet = spreadsheet.worksheet(tab_name)
    _sheet_cache[tab_name] = worksheet
    return worksheet


def load_bot_config(force_refresh: bool = False) -> dict:
    global _bot_config_cache

    if _bot_config_cache is not None and not force_refresh:
        return _bot_config_cache

    config = {}
    try:
        ws = get_sheet(BOT_CONFIG_TAB)
        rows = ws.get_all_records()
        for row in rows:
            key = str(row.get("Key", "")).strip()
            value = row.get("Value", "")
            if key:
                config[key] = value
    except Exception as e:
        print(f"[BOT_CONFIG ERROR] {e}")

    _bot_config_cache = config
    return config


def get_fallback_message() -> str:
    config = load_bot_config()
    fallback = str(config.get("FALLBACK_MESSAGE", "")).strip()
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
        hashlib.sha256,
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
        "spreadsheet_id_exists": bool(SPREADSHEET_ID),
    }), 200


# ========================
# TRANSLATE
# ========================
def detect_language(text: str) -> str:
    url = "https://translation.googleapis.com/language/translate/v2/detect"
    payload = {
        "q": text,
        "key": GOOGLE_API_KEY,
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
    url = "https://translation.googleapis.com/language/translate/v2"
    payload = {
        "q": text,
        "target": target_lang,
        "key": GOOGLE_API_KEY,
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
# COMMAND PARSER
# ========================
def resolve_target_and_content(input_text: str):
    text = input_text.strip()

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
# LOGGING
# ========================
def get_logged_event_ids(limit: int = 200) -> set:
    """
    Lấy nhanh event_id đã ghi gần nhất để chống trùng thực chiến.
    Không scan toàn cột để tránh chậm.
    """
    try:
        ws = get_sheet(TRANSLATION_LOG_TAB)
        values = ws.col_values(1)
        if not values:
            return set()

        # bỏ header, chỉ lấy phần dữ liệu cuối
        data_rows = values[1:]
        recent_ids = data_rows[-limit:]
        return {str(x).strip() for x in recent_ids if str(x).strip()}
    except Exception as e:
        print(f"[EVENT CACHE ERROR] {e}")
        return set()


def append_translation_log(row: list):
    ws = get_sheet(TRANSLATION_LOG_TAB)
    ws.append_row(row, value_input_option="RAW")


def build_log_row(
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
    error_message: str,
):
    return [
        event_id,
        timestamp,
        user_id,
        source_type,
        group_id,
        room_id,
        input_text,
        detected_lang,
        target_lang,
        translated_text,
        status,
        error_message,
    ]


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
        "Authorization": f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}",
    }
    payload = {
        "replyToken": reply_token,
        "messages": [
            {"type": "text", "text": str(text)[:5000]}
        ],
    }

    try:
        res = requests.post(url
