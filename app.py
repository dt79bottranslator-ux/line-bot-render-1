import os
import json
import uuid
from datetime import datetime, timezone

import gspread
import requests
from flask import Flask, request, jsonify
from oauth2client.service_account import ServiceAccountCredentials

app = Flask(__name__)

LINE_CHANNEL_ACCESS_TOKEN = os.getenv("LINE_CHANNEL_ACCESS_TOKEN", "").strip()
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY", "").strip()
GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID", "").strip()
PORT = int(os.getenv("PORT", "10000"))


# ========================
# GOOGLE SHEET SETUP
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


def get_sheet(tab_name):
    client = get_gspread_client()
    spreadsheet = client.open_by_key(SPREADSHEET_ID)
    return spreadsheet.worksheet(tab_name)


# ========================
# ROOT
# ========================
@app.route("/", methods=["GET"])
def root():
    return "BOT RUNNING", 200


# ========================
# HEALTH
# ========================
@app.route("/health", methods=["GET"])
def health():
    return jsonify({
        "status": "ok",
        "line_token_exists": bool(LINE_CHANNEL_ACCESS_TOKEN),
        "google_api_key_exists": bool(GOOGLE_API_KEY),
        "google_service_account_exists": bool(GOOGLE_SERVICE_ACCOUNT_JSON),
        "spreadsheet_id_exists": bool(SPREADSHEET_ID)
    }), 200


# ========================
# TRANSLATE
# ========================
def detect_language(text):
    url = "https://translation.googleapis.com/language/translate/v2/detect"
    params = {
        "q": text,
        "key": GOOGLE_API_KEY
    }
    res = requests.post(url, data=params, timeout=20)

    if res.status_code != 200:
        return "unknown"

    try:
        return res.json()["data"]["detections"][0][0]["language"]
    except Exception:
        return "unknown"


def translate_text(text, target_lang):
    url = "https://translation.googleapis.com/language/translate/v2"
    params = {
        "q": text,
        "target": target_lang,
        "key": GOOGLE_API_KEY
    }
    res = requests.post(url, data=params, timeout=20)

    if res.status_code != 200:
        raise RuntimeError(f"Translate API error: {res.text}")

    try:
        return res.json()["data"]["translations"][0]["translatedText"]
    except Exception:
        raise RuntimeError("Translate API parse failed")


# ========================
# LOG WRITER
# ========================
def append_translation_log(
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
    error_message
):
    ws = get_sheet("TRANSLATION_LOG")
    ws.append_row([
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
        error_message
    ])


# ========================
# REPLY
# ========================
def reply_text(reply_token, text):
    url = "https://api.line.me/v2/bot/message/reply"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}"
    }
    body = {
        "replyToken": reply_token,
        "messages": [
            {"type": "text", "text": str(text)[:5000]}
        ]
    }
    res = requests.post(url, headers=headers, json=body, timeout=20)
    print(f"[LINE REPLY] status={res.status_code} body={res.text}")


# ========================
# WEBHOOK
# ========================
@app.route("/webhook", methods=["POST"])
def webhook():
    data = request.get_json(silent=True)

    if not data:
        return "NO DATA", 400

    for event in data.get("events", []):
        if event.get("type") != "message":
            continue

        message = event.get("message", {})
        if message.get("type") != "text":
            continue

        reply_token = event.get("replyToken")
        input_text = message.get("text", "").strip()

        if not reply_token or not input_text:
            continue

        source = event.get("source", {})
        user_id = source.get("userId", "")
        source_type = source.get("type", "")
        group_id = source.get("groupId", "")
        room_id = source.get("roomId", "")

        event_id = str(uuid.uuid4())
        timestamp = datetime.now(timezone.utc).isoformat()

        detected_lang = "unknown"
        target_lang = ""
        translated_text = ""
        status = "success"
        error_message = ""

        try:
            detected_lang = detect_language(input_text)

            if input_text.startswith("/zh "):
                target_lang = "zh-TW"
                translated_text = translate_text(input_text[4:], target_lang)

            elif input_text.startswith("/vi "):
                target_lang = "vi"
                translated_text = translate_text(input_text[4:], target_lang)

            elif input_text.startswith("/id "):
                target_lang = "id"
                translated_text = translate_text(input_text[4:], target_lang)

            elif input_text.startswith("/en "):
                target_lang = "en"
                translated_text = translate_text(input_text[4:], target_lang)

            else:
                translated_text = "Dùng:\n/zh nội dung\n/vi nội dung\n/id nội dung\n/en nội dung"

            reply_text(reply_token, translated_text)

        except Exception as e:
            status = "error"
            error_message = str(e)
            translated_text = os.getenv("FALLBACK_MESSAGE", "Hệ thống bận, thử lại sau.")
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
    print(f"[BOOT] google_api_key_exists={bool(GOOGLE_API_KEY)}")
    print(f"[BOOT] google_service_account_exists={bool(GOOGLE_SERVICE_ACCOUNT_JSON)}")
    print(f"[BOOT] spreadsheet_id_exists={bool(SPREADSHEET_ID)}")

    app.run(host="0.0.0.0", port=PORT)
