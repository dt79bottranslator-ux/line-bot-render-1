import os
import json
import html
import hmac
import hashlib
import base64
import threading
from datetime import datetime, timezone, timedelta
from typing import Dict, Optional, Tuple

import gspread
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
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
FALLBACK_MESSAGE_DEFAULT = "Hệ thống bận, thử lại sau."

HTTP_TIMEOUT_SECONDS = 20
BOT_CONFIG_TTL_SECONDS = 60
EVENT_CACHE_TTL_SECONDS = 180

# ========================
# HTTP SESSION
# ========================
def build_http_session() -> requests.Session:
    session = requests.Session()

    retry = Retry(
        total=2,
        connect=2,
        read=2,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset(["GET", "POST"])
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


HTTP = build_http_session()

# ========================
# BOT CONFIG CACHE
# ========================
BOT_CONFIG_CACHE = {
    "data": {},
    "loaded_at": None
}

# ========================
# EVENT STATE CACHE
# state: processing | done
# ========================
EVENT_STATE_CACHE: Dict[str, Dict[str, datetime | str]] = {}
EVENT_STATE_LOCK = threading.Lock()


# ========================
# LOG UTILS
# ========================
def log_line(message: str):
    print(message, flush=True)


# ========================
# COMMON UTILS
# ========================
def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_now_iso() -> str:
    return utc_now().isoformat()


def safe_text(value, max_len: int = 5000) -> str:
    if value is None:
        return ""
    return str(value).strip()[:max_len]


def line_timestamp_to_iso(timestamp_ms) -> str:
    try:
        if timestamp_ms is None:
            return utc_now_iso()
        ts = datetime.fromtimestamp(int(timestamp_ms) / 1000, tz=timezone.utc)
        return ts.isoformat()
    except Exception:
        return utc_now_iso()


def truncate_text(value: str, max_len: int = 300) -> str:
    return safe_text(value, max_len=max_len)


# ========================
# EVENT CACHE / IDEMPOTENCY
# ========================
def cleanup_event_state_cache():
    now = utc_now()
    expired_keys = []

    with EVENT_STATE_LOCK:
        for event_id, info in EVENT_STATE_CACHE.items():
            updated_at = info.get("updated_at")
            if isinstance(updated_at, datetime):
                if now - updated_at > timedelta(seconds=EVENT_CACHE_TTL_SECONDS):
                    expired_keys.append(event_id)

        for key in expired_keys:
            EVENT_STATE_CACHE.pop(key, None)


def get_event_state(event_id: str) -> Optional[str]:
    if not event_id:
        return None

    cleanup_event_state_cache()

    with EVENT_STATE_LOCK:
        info = EVENT_STATE_CACHE.get(event_id)
        if not info:
            return None
        state = info.get("state")
        return str(state) if state else None


def set_event_state(event_id: str, state: str):
    if not event_id:
        return

    with EVENT_STATE_LOCK:
        EVENT_STATE_CACHE[event_id] = {
            "state": state,
            "updated_at": utc_now()
        }


def clear_event_state(event_id: str):
    if not event_id:
        return

    with EVENT_STATE_LOCK:
        EVENT_STATE_CACHE.pop(event_id, None)


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
    if not SPREADSHEET_ID:
        raise ValueError("Missing SPREADSHEET_ID")
    client = get_gspread_client()
    spreadsheet = client.open_by_key(SPREADSHEET_ID)
    return spreadsheet.worksheet(tab_name)


def load_bot_config(force_reload: bool = False) -> dict:
    now = utc_now()

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
        log_line(f"[BOT-CONFIG-OK] loaded_keys={len(config)}")
    except Exception as e:
        log_line(f"[BOT-CONFIG-ERROR] {safe_text(e, 1000)}")

    return BOT_CONFIG_CACHE["data"]


def get_fallback_message() -> str:
    config = load_bot_config()
    fallback = safe_text(config.get("FALLBACK_MESSAGE", ""), 500)
    return fallback if fallback else FALLBACK_MESSAGE_DEFAULT


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
    if not event_id:
        return False

    try:
        ws = get_sheet(TRANSLATION_LOG_TAB)
        cell = ws.find(event_id, in_column=1)
        return cell is not None
    except gspread.exceptions.CellNotFound:
        return False
    except Exception as e:
        log_line(f"[IDEMPOTENCY-SHEET-CHECK-ERROR] event_id={event_id} error={safe_text(e, 1000)}")
        return False


def is_duplicate_event(event_id: str) -> bool:
    if not event_id:
        return False

    state = get_event_state(event_id)
    if state in ("processing", "done"):
        log_line(f"[DEDUPE] event_id={event_id} duplicate=true source=memory state={state}")
        return True

    if event_exists_in_sheet(event_id):
        set_event_state(event_id, "done")
        log_line(f"[DEDUPE] event_id={event_id} duplicate=true source=sheet state=done")
        return True

    log_line(f"[DEDUPE] event_id={event_id} duplicate=false")
    return False


# ========================
# SECURITY
# ========================
def verify_signature(req) -> bool:
    if not LINE_CHANNEL_SECRET:
        log_line("[SECURITY-ERROR] Missing LINE_CHANNEL_SECRET")
        return False

    signature = req.headers.get("X-Line-Signature", "").strip()
    body = req.get_data(as_text=True)

    digest = hmac.new(
        LINE_CHANNEL_SECRET.encode("utf-8"),
        body.encode("utf-8"),
        hashlib.sha256
    ).digest()

    expected_signature = base64.b64encode(digest).decode("utf-8")
    ok = hmac.compare_digest(signature, expected_signature)

    if not ok:
        log_line("[SECURITY-ERROR] Invalid LINE signature")

    return ok


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
def detect_language(text: str, event_id: str = "") -> str:
    if not GOOGLE_API_KEY:
        log_line(f"[API-REQ-DETECT-SKIP] event_id={event_id} reason=missing_google_api_key")
        return "unknown"

    url = "https://translation.googleapis.com/language/translate/v2/detect"
    payload = {
        "q": text,
        "key": GOOGLE_API_KEY
    }

    log_line(f"[API-REQ-DETECT] event_id={event_id} text_len={len(text)}")

    res = HTTP.post(url, data=payload, timeout=HTTP_TIMEOUT_SECONDS)

    log_line(
        f"[API-RES-DETECT] event_id={event_id} "
        f"status={res.status_code} body={truncate_text(res.text, 300)}"
    )

    if res.status_code != 200:
        return "unknown"

    try:
        return res.json()["data"]["detections"][0][0]["language"]
    except Exception as e:
        log_line(f"[API-PARSE-DETECT-ERROR] event_id={event_id} error={safe_text(e, 1000)}")
        return "unknown"


def translate_text(text: str, target_lang: str, event_id: str = "") -> str:
    if not GOOGLE_API_KEY:
        raise RuntimeError("Missing GOOGLE_API_KEY")

    url = "https://translation.googleapis.com/language/translate/v2"
    payload = {
        "q": text,
        "target": target_lang,
        "key": GOOGLE_API_KEY
    }

    log_line(
        f"[API-REQ-TRANSLATE] event_id={event_id} "
        f"target_lang={target_lang} text_len={len(text)}"
    )

    res = HTTP.post(url, data=payload, timeout=HTTP_TIMEOUT_SECONDS)

    log_line(
        f"[API-RES-TRANSLATE] event_id={event_id} "
        f"status={res.status_code} body={truncate_text(res.text, 300)}"
    )

    if res.status_code != 200:
        raise RuntimeError(f"Translate API error: {res.status_code} {truncate_text(res.text, 500)}")

    try:
        translated = res.json()["data"]["translations"][0]["translatedText"]
        return html.unescape(translated)
    except Exception as e:
        raise RuntimeError(f"Translate API parse failed: {safe_text(e, 1000)}")


# ========================
# LINE REPLY
# ========================
def reply_text(reply_token: str, text: str, event_id: str = "") -> bool:
    if not LINE_CHANNEL_ACCESS_TOKEN:
        log_line(f"[LINE-REPLY-ERROR] event_id={event_id} reason=missing_access_token")
        return False

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

    log_line(
        f"[LINE-REPLY-REQ] event_id={event_id} "
        f"reply_token_exists={bool(reply_token)} text_len={len(safe_text(text, 5000))}"
    )

    try:
        res = HTTP.post(url, headers=headers, json=payload, timeout=HTTP_TIMEOUT_SECONDS)
        log_line(
            f"[LINE-REPLY-RES] event_id={event_id} "
            f"status={res.status_code} body={truncate_text(res.text, 300)}"
        )
        return 200 <= res.status_code < 300
    except Exception as e:
        log_line(f"[LINE-REPLY-ERROR] event_id={event_id} error={safe_text(e, 1000)}")
        return False


# ========================
# BUSINESS LOGIC
# ========================
def resolve_target_and_content(input_text: str) -> Tuple[str, str, bool]:
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
    log_line("[WEBHOOK-ENTER] route=/webhook method=POST")

    if not verify_signature(request):
        return "INVALID SIGNATURE", 403

    data = request.get_json(silent=True)
    if not data:
        log_line("[WEBHOOK-ERROR] no_json_body")
        return "NO DATA", 400

    events = data.get("events", [])
    log_line(f"[WEBHOOK-IN] events_count={len(events)}")

    for event in events:
        event_type = safe_text(event.get("type", ""), 50)
        if event_type != "message":
            log_line(f"[WEBHOOK-SKIP] reason=non_message_event event_type={event_type}")
            continue

        message = event.get("message", {})
        message_type = safe_text(message.get("type", ""), 50)
        if message_type != "text":
            log_line(f"[WEBHOOK-SKIP] reason=non_text_message message_type={message_type}")
            continue

        reply_token = safe_text(event.get("replyToken", ""), 200)
        input_text = safe_text(message.get("text", ""), 5000)
        line_message_id = safe_text(message.get("id", ""), 200)

        source = event.get("source", {})
        user_id = safe_text(source.get("userId", ""), 200)
        source_type = safe_text(source.get("type", ""), 50)
        group_id = safe_text(source.get("groupId", ""), 200)
        room_id = safe_text(source.get("roomId", ""), 200)

        event_id = line_message_id
        timestamp = line_timestamp_to_iso(event.get("timestamp"))

        if not event_id:
            log_line("[WEBHOOK-SKIP] reason=missing_message_id")
            continue

        log_line(
            f"[EVT-IN] event_id={event_id} user_id={user_id} source_type={source_type} "
            f"group_id={group_id} room_id={room_id} text={truncate_text(input_text, 200)}"
        )

        if not reply_token or not input_text:
            log_line(f"[WEBHOOK-SKIP] event_id={event_id} reason=missing_reply_token_or_text")
            continue

        if is_duplicate_event(event_id):
            log_line(f"[WEBHOOK-SKIP-DUPLICATE] event_id={event_id}")
            continue

        set_event_state(event_id, "processing")

        detected_lang = "unknown"
        target_lang = ""
        translated_text = ""
        status = "success"
        error_message = ""

        reply_ok = False
        log_ok = False

        try:
            target_lang, content_to_translate, is_command = resolve_target_and_content(input_text)
            log_line(
                f"[PATH] event_id={event_id} is_command={is_command} "
                f"target_lang={target_lang} content_len={len(content_to_translate)}"
            )

            if is_command:
                if not content_to_translate:
                    translated_text = HELP_TEXT
                    log_line(f"[PATH] event_id={event_id} action=help_text")
                else:
                    detected_lang = detect_language(content_to_translate, event_id=event_id)
                    translated_text = translate_text(
                        content_to_translate,
                        target_lang,
                        event_id=event_id
                    )
                    log_line(
                        f"[PATH] event_id={event_id} action=translated "
                        f"detected_lang={detected_lang}"
                    )
            else:
                translated_text = HELP_TEXT
                log_line(f"[PATH] event_id={event_id} action=non_command_help")

        except Exception as e:
            status = "error"
            error_message = safe_text(str(e), 5000)
            translated_text = get_fallback_message()
            log_line(f"[PROCESS-ERROR] event_id={event_id} error={error_message}")

        try:
            log_line(f"[SHEET-WRITE-REQ] event_id={event_id} tab={TRANSLATION_LOG_TAB}")
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
            log_ok = True
            log_line(f"[SHEET-WRITE-OK] event_id={event_id}")
        except Exception as log_err:
            log_line(f"[SHEET-WRITE-ERROR] event_id={event_id} error={safe_text(log_err, 1000)}")
            log_ok = False

        try:
            reply_ok = reply_text(reply_token, translated_text, event_id=event_id)
        except Exception as reply_err:
            log_line(f"[LINE-REPLY-FATAL] event_id={event_id} error={safe_text(reply_err, 1000)}")
            reply_ok = False

        log_line(
            f"[EVT-OUT] event_id={event_id} status={status} "
            f"log_ok={log_ok} reply_ok={reply_ok}"
        )

        if log_ok:
            set_event_state(event_id, "done")
        else:
            clear_event_state(event_id)

    return "OK", 200


# ========================
# RUN
# ========================
if __name__ == "__main__":
    log_line("[BOOT-MARKER] RUNTIME_VERSION=2026-04-06-WEBHOOK-TRACE-V2")
    log_line("[BOOT] Flask app starting...")
    log_line(f"[BOOT] PORT={PORT}")
    log_line(f"[BOOT] line_token_exists={bool(LINE_CHANNEL_ACCESS_TOKEN)}")
    log_line(f"[BOOT] line_secret_exists={bool(LINE_CHANNEL_SECRET)}")
    log_line(f"[BOOT] google_api_key_exists={bool(GOOGLE_API_KEY)}")
    log_line(f"[BOOT] google_service_account_exists={bool(GOOGLE_SERVICE_ACCOUNT_JSON)}")
    log_line(f"[BOOT] spreadsheet_id_exists={bool(SPREADSHEET_ID)}")

    # Local run only.
    # Production trên Render nên chạy bằng gunicorn (WSGI server)
    app.run(host="0.0.0.0", port=PORT)
