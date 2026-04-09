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
import gspread
from google.oauth2.service_account import Credentials
from flask import Flask, request, jsonify

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
PHASE1_SPREADSHEET_NAME = os.getenv("PHASE1_SPREADSHEET_NAME", "DT79_PHASE1_WORKER_CASES_V1").strip()
USER_STATE_SHEET_NAME = "user_state"

# ADMIN
ADMIN_IDS = os.getenv("ADMIN_IDS", "").strip()
ADMIN_LIST = [x.strip() for x in ADMIN_IDS.split(",") if x.strip()]

# ANTI-ABUSE FOR !ALL
ALL_COOLDOWN_SECONDS = int(os.getenv("ALL_COOLDOWN_SECONDS", "15").strip() or "15")
MAX_ALL_CHARS = int(os.getenv("MAX_ALL_CHARS", "500").strip() or "500")

# =========================================================
# CONSTANT
# =========================================================
TW_TZ = timezone(timedelta(hours=8))
LOCKED_TARGET_LANG = "zh-TW"

CONNECT_TIMEOUT_SECONDS = int(os.getenv("CONNECT_TIMEOUT_SECONDS", "3").strip() or "3")
READ_TIMEOUT_SECONDS = int(os.getenv("READ_TIMEOUT_SECONDS", "8").strip() or "8")
OUTBOUND_TIMEOUT = (CONNECT_TIMEOUT_SECONDS, READ_TIMEOUT_SECONDS)

FALLBACK_REPLY_TEXT = "Hệ thống bận, thử lại sau."
LINE_TEXT_HARD_LIMIT = 5000
RATE_LIMIT_STORE_MAX_KEYS = 5000
ERROR_BODY_LOG_LIMIT = 800

LINE_REPLY_API_URL = "https://api.line.me/v2/bot/message/reply"
GOOGLE_TRANSLATE_API_URL = "https://translation.googleapis.com/language/translate/v2"

# In-memory rate limit store
# key = "{user_id}:{group_or_room_or_user}"
LAST_ALL_USED_AT = {}

# =========================================================
# STARTUP VALIDATION
# =========================================================
def validate_startup_config():
    missing = []

    if not LINE_CHANNEL_ACCESS_TOKEN:
        missing.append("LINE_CHANNEL_ACCESS_TOKEN")
    if not LINE_CHANNEL_SECRET:
        missing.append("LINE_CHANNEL_SECRET")
    if not GOOGLE_API_KEY:
        missing.append("GOOGLE_API_KEY")
    if not GOOGLE_SERVICE_ACCOUNT_JSON:
        missing.append("GOOGLE_SERVICE_ACCOUNT_JSON")

    if missing:
        logger.warning(f"[STARTUP] Missing env: {', '.join(missing)}")
    else:
        logger.info("[STARTUP] Required env loaded")

    logger.info(
        f"[STARTUP] admin_count={len(ADMIN_LIST)} "
        f"all_cooldown_seconds={ALL_COOLDOWN_SECONDS} "
        f"max_all_chars={MAX_ALL_CHARS} "
        f"connect_timeout_s={CONNECT_TIMEOUT_SECONDS} "
        f"read_timeout_s={READ_TIMEOUT_SECONDS} "
        f"phase1_spreadsheet_name={PHASE1_SPREADSHEET_NAME}"
    )

def is_runtime_ready() -> bool:
    return all([
        bool(LINE_CHANNEL_ACCESS_TOKEN),
        bool(LINE_CHANNEL_SECRET),
        bool(GOOGLE_API_KEY),
        bool(GOOGLE_SERVICE_ACCOUNT_JSON),
    ])

validate_startup_config()

# =========================================================
# BASIC HELPERS
# =========================================================
def now_tw_iso():
    return datetime.now(TW_TZ).isoformat()

def make_trace_id():
    return f"trc_{uuid.uuid4().hex[:12]}"

def safe_str(v):
    return str(v).strip() if v else ""

def crop_text(text: str, max_len: int = LINE_TEXT_HARD_LIMIT) -> str:
    text = safe_str(text)
    if len(text) <= max_len:
        return text
    return text[:max_len]

def truncate_log_text(text: str, max_len: int = ERROR_BODY_LOG_LIMIT) -> str:
    text = safe_str(text)
    if len(text) <= max_len:
        return text
    return text[:max_len] + "...<truncated>"

def is_admin(user_id: str) -> bool:
    return user_id in ADMIN_LIST

def extract_source_ids(event):
    s = event.get("source", {})
    return (
        safe_str(s.get("type")),
        safe_str(s.get("userId")),
        safe_str(s.get("groupId")),
        safe_str(s.get("roomId"))
    )

def get_now_ts() -> int:
    return int(time.time())

def get_scope_key(source_type: str, user_id: str, group_id: str, room_id: str) -> str:
    if source_type == "group" and group_id:
        return group_id
    if source_type == "room" and room_id:
        return room_id
    return user_id or "unknown"

def get_all_rate_limit_key(user_id: str, scope_key: str) -> str:
    return f"{user_id}:{scope_key}"

def cleanup_rate_limit_store(now_ts: int):
    if len(LAST_ALL_USED_AT) < RATE_LIMIT_STORE_MAX_KEYS:
        return

    expired_before = now_ts - max(ALL_COOLDOWN_SECONDS * 3, 60)
    stale_keys = [
        k for k, v in LAST_ALL_USED_AT.items()
        if v < expired_before
    ]

    for k in stale_keys:
        LAST_ALL_USED_AT.pop(k, None)

    logger.info(
        f"[RATE_LIMIT] cleanup done removed={len(stale_keys)} "
        f"remaining={len(LAST_ALL_USED_AT)}"
    )

def allow_all_command(user_id: str, scope_key: str):
    now_ts = get_now_ts()
    cleanup_rate_limit_store(now_ts)

    rate_key = get_all_rate_limit_key(user_id, scope_key)
    last_ts = LAST_ALL_USED_AT.get(rate_key, 0)
    diff = now_ts - last_ts

    if diff < ALL_COOLDOWN_SECONDS:
        remaining = ALL_COOLDOWN_SECONDS - diff
        return False, remaining

    LAST_ALL_USED_AT[rate_key] = now_ts
    return True, 0

def parse_all_command(input_text: str):
    raw = safe_str(input_text)
    lowered = raw.lower()

    if lowered == "!all":
        return True, ""

    if lowered.startswith("!all "):
        content = raw[5:].strip()
        return True, content

    return False, ""

def ms_since(start_perf: float) -> int:
    return int((time.perf_counter() - start_perf) * 1000)

# =========================================================
# STATE HELPERS - PHASE 1
# =========================================================
STATE_IDLE = "idle"
STATE_AWAITING_NEED_TYPE = "awaiting_need_type"
STATE_AWAITING_URGENCY = "awaiting_urgency"
STATE_AWAITING_RESIDENCE_CARD = "awaiting_residence_card"
STATE_AWAITING_PHONE_NUMBER = "awaiting_phone_number"
STATE_CASE_COMPLETED = "case_completed"

ALLOWED_STATES = {
    STATE_IDLE,
    STATE_AWAITING_NEED_TYPE,
    STATE_AWAITING_URGENCY,
    STATE_AWAITING_RESIDENCE_CARD,
    STATE_AWAITING_PHONE_NUMBER,
    STATE_CASE_COMPLETED,
}

def get_google_credentials():
    if not GOOGLE_SERVICE_ACCOUNT_JSON:
        raise ValueError("missing GOOGLE_SERVICE_ACCOUNT_JSON")

    try:
        info = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
    except Exception as e:
        raise ValueError(f"invalid GOOGLE_SERVICE_ACCOUNT_JSON: {type(e).__name__}:{e}")

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    return Credentials.from_service_account_info(info, scopes=scopes)

def get_gspread_client():
    creds = get_google_credentials()
    return gspread.authorize(creds)

def open_phase1_spreadsheet():
    gc = get_gspread_client()
    return gc.open(PHASE1_SPREADSHEET_NAME)

def get_user_state_worksheet():
    ss = open_phase1_spreadsheet()
    return ss.worksheet(USER_STATE_SHEET_NAME)

def normalize_state_value(value: str) -> str:
    state = safe_str(value)
    if state in ALLOWED_STATES:
        return state
    return STATE_IDLE

def empty_user_state(user_id: str, scope_key: str):
    return {
        "user_id": safe_str(user_id),
        "scope_key": safe_str(scope_key),
        "current_state": STATE_IDLE,
        "temp_need_type": "",
        "temp_urgency_level": "",
        "temp_residence_card_image_url": "",
        "updated_at": now_tw_iso(),
    }

def find_user_state_row(ws, user_id: str, scope_key: str):
    user_id = safe_str(user_id)
    scope_key = safe_str(scope_key)

    records = ws.get_all_records()
    for idx, row in enumerate(records, start=2):  # row 1 = header
        if (
            safe_str(row.get("user_id")) == user_id and
            safe_str(row.get("scope_key")) == scope_key
        ):
            return idx, row

    return None, None

def get_user_state(user_id: str, scope_key: str, trace_id: str):
    try:
        ws = get_user_state_worksheet()
        row_index, row = find_user_state_row(ws, user_id, scope_key)

        if not row:
            state_data = empty_user_state(user_id, scope_key)
            logger.info(
                f"[{trace_id}] STATE_GET status=NEW_DEFAULT "
                f"user_id={user_id} scope_key={scope_key}"
            )
            return state_data

        state_data = {
            "user_id": safe_str(row.get("user_id")),
            "scope_key": safe_str(row.get("scope_key")),
            "current_state": normalize_state_value(row.get("current_state")),
            "temp_need_type": safe_str(row.get("temp_need_type")),
            "temp_urgency_level": safe_str(row.get("temp_urgency_level")),
            "temp_residence_card_image_url": safe_str(row.get("temp_residence_card_image_url")),
            "updated_at": safe_str(row.get("updated_at")),
        }

        logger.info(
            f"[{trace_id}] STATE_GET status=FOUND "
            f"user_id={user_id} scope_key={scope_key} "
            f"current_state={state_data['current_state']}"
        )
        return state_data

    except Exception as e:
        logger.exception(
            f"[{trace_id}] STATE_GET exception={type(e).__name__}:{e} "
            f"user_id={user_id} scope_key={scope_key}"
        )
        return empty_user_state(user_id, scope_key)

def upsert_user_state(
    user_id: str,
    scope_key: str,
    current_state: str,
    temp_need_type: str,
    temp_urgency_level: str,
    temp_residence_card_image_url: str,
    trace_id: str
):
    ws = get_user_state_worksheet()
    row_index, _existing = find_user_state_row(ws, user_id, scope_key)

    final_row = [
        safe_str(user_id),
        safe_str(scope_key),
        normalize_state_value(current_state),
        safe_str(temp_need_type),
        safe_str(temp_urgency_level),
        safe_str(temp_residence_card_image_url),
        now_tw_iso(),
    ]

    if row_index:
        ws.update(f"A{row_index}:G{row_index}", [final_row])
        logger.info(
            f"[{trace_id}] STATE_UPSERT action=UPDATE "
            f"user_id={user_id} scope_key={scope_key} "
            f"current_state={final_row[2]}"
        )
        return "updated"

    ws.append_row(final_row, value_input_option="RAW")
    logger.info(
        f"[{trace_id}] STATE_UPSERT action=INSERT "
        f"user_id={user_id} scope_key={scope_key} "
        f"current_state={final_row[2]}"
    )
    return "inserted"

def reset_user_state(user_id: str, scope_key: str, trace_id: str):
    return upsert_user_state(
        user_id=user_id,
        scope_key=scope_key,
        current_state=STATE_IDLE,
        temp_need_type="",
        temp_urgency_level="",
        temp_residence_card_image_url="",
        trace_id=trace_id
    )

# =========================================================
# OUTBOUND HTTP HELPERS
# =========================================================
def post_json(url, headers, payload, trace_id, op_name):
    started = time.perf_counter()

    try:
        r = requests.post(
            url,
            headers=headers,
            json=payload,
            timeout=OUTBOUND_TIMEOUT
        )
        latency_ms = ms_since(started)
        logger.info(f"[{trace_id}] {op_name} status={r.status_code} latency_ms={latency_ms}")

        if r.status_code != 200:
            logger.error(
                f"[{trace_id}] {op_name} body={truncate_log_text(r.text)} "
                f"url={url}"
            )

        return r, latency_ms

    except requests.Timeout as e:
        latency_ms = ms_since(started)
        logger.exception(
            f"[{trace_id}] {op_name} timeout_exception={type(e).__name__} "
            f"latency_ms={latency_ms} url={url}"
        )
        return None, latency_ms

    except requests.RequestException as e:
        latency_ms = ms_since(started)
        logger.exception(
            f"[{trace_id}] {op_name} request_exception={type(e).__name__} "
            f"latency_ms={latency_ms} url={url}"
        )
        return None, latency_ms

    except Exception as e:
        latency_ms = ms_since(started)
        logger.exception(
            f"[{trace_id}] {op_name} exception={type(e).__name__}:{e} "
            f"latency_ms={latency_ms} url={url}"
        )
        return None, latency_ms

def post_form(url, params, data, trace_id, op_name):
    started = time.perf_counter()

    try:
        r = requests.post(
            url,
            params=params,
            data=data,
            timeout=OUTBOUND_TIMEOUT
        )
        latency_ms = ms_since(started)
        logger.info(f"[{trace_id}] {op_name} status={r.status_code} latency_ms={latency_ms}")

        if r.status_code != 200:
            logger.error(
                f"[{trace_id}] {op_name} body={truncate_log_text(r.text)} "
                f"url={url}"
            )

        return r, latency_ms

    except requests.Timeout as e:
        latency_ms = ms_since(started)
        logger.exception(
            f"[{trace_id}] {op_name} timeout_exception={type(e).__name__} "
            f"latency_ms={latency_ms} url={url}"
        )
        return None, latency_ms

    except requests.RequestException as e:
        latency_ms = ms_since(started)
        logger.exception(
            f"[{trace_id}] {op_name} request_exception={type(e).__name__} "
            f"latency_ms={latency_ms} url={url}"
        )
        return None, latency_ms

    except Exception as e:
        latency_ms = ms_since(started)
        logger.exception(
            f"[{trace_id}] {op_name} exception={type(e).__name__}:{e} "
            f"latency_ms={latency_ms} url={url}"
        )
        return None, latency_ms

# =========================================================
# LINE HELPERS
# =========================================================
def verify_line_signature(secret, body, signature):
    if not secret or not signature:
        return False

    digest = hmac.new(secret.encode("utf-8"), body, hashlib.sha256).digest()
    computed = base64.b64encode(digest).decode("utf-8")
    return hmac.compare_digest(computed, signature)

def line_reply(reply_token, text, trace_id):
    if not LINE_CHANNEL_ACCESS_TOKEN:
        logger.error(f"[{trace_id}] LINE_REPLY skipped reason=missing_channel_access_token")
        return False, 0

    if not reply_token:
        logger.error(f"[{trace_id}] LINE_REPLY skipped reason=missing_reply_token")
        return False, 0

    final_text = crop_text(text or FALLBACK_REPLY_TEXT)

    headers = {
        "Authorization": f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}",
        "Content-Type": "application/json"
    }
    payload = {
        "replyToken": reply_token,
        "messages": [
            {
                "type": "text",
                "text": final_text
            }
        ]
    }

    r, latency_ms = post_json(
        url=LINE_REPLY_API_URL,
        headers=headers,
        payload=payload,
        trace_id=trace_id,
        op_name="LINE_REPLY"
    )

    if not r:
        return False, latency_ms

    return r.status_code == 200, latency_ms

# =========================================================
# GOOGLE TRANSLATE HELPERS
# =========================================================
def translate_auto_source(text, target, trace_id):
    if not GOOGLE_API_KEY:
        logger.error(f"[{trace_id}] GOOGLE_TRANSLATE skipped reason=missing_google_api_key")
        return None, 0, None

    data = {
        "q": text,
        "target": target,
        "format": "text"
    }

    r, latency_ms = post_form(
        url=GOOGLE_TRANSLATE_API_URL,
        params={"key": GOOGLE_API_KEY},
        data=data,
        trace_id=trace_id,
        op_name="GOOGLE_TRANSLATE"
    )

    if not r:
        return None, latency_ms, None

    if r.status_code != 200:
        return None, latency_ms, None

    try:
        payload = r.json()
    except Exception as e:
        logger.exception(f"[{trace_id}] GOOGLE_TRANSLATE json_parse_exception={type(e).__name__}:{e}")
        return None, latency_ms, None

    translations = payload.get("data", {}).get("translations", [])
    if not translations:
        logger.error(f"[{trace_id}] GOOGLE_TRANSLATE empty_translations")
        return None, latency_ms, None

    first = translations[0]
    translated = html.unescape(first.get("translatedText", ""))
    detected_source_language = first.get("detectedSourceLanguage")

    return translated, latency_ms, detected_source_language

# =========================================================
# AUDIT LOG HELPERS
# =========================================================
def log_all_audit(
    trace_id,
    user_id,
    source_type,
    group_id,
    room_id,
    raw_input,
    content,
    status,
    note=""
):
    logger.info(
        f"[ALL_AUDIT] trace_id={trace_id} "
        f"user_id={user_id} "
        f"source_type={source_type} "
        f"group_id={group_id} "
        f"room_id={room_id} "
        f"status={status} "
        f"raw_input={json.dumps(raw_input, ensure_ascii=False)} "
        f"content={json.dumps(content, ensure_ascii=False)} "
        f"note={json.dumps(note, ensure_ascii=False)}"
    )

def log_total_latency(trace_id, route_name, total_ms, source_type, group_id, room_id):
    logger.info(
        f"[LATENCY] trace_id={trace_id} "
        f"route={route_name} "
        f"total_ms={total_ms} "
        f"source_type={source_type} "
        f"group_id={group_id} "
        f"room_id={room_id}"
    )

# =========================================================
# HEALTH
# =========================================================
@app.route("/", methods=["GET"])
def health():
    ready = is_runtime_ready()
    return jsonify({
        "ok": ready,
        "service": "line-bot-render-phase1-timeout-isolated",
        "time": now_tw_iso(),
        "ready": ready,
        "timeouts": {
            "connect_seconds": CONNECT_TIMEOUT_SECONDS,
            "read_seconds": READ_TIMEOUT_SECONDS
        },
        "phase1": {
            "spreadsheet_name": PHASE1_SPREADSHEET_NAME,
            "user_state_sheet": USER_STATE_SHEET_NAME,
            "state_read_in_callback": False
        }
    }), 200 if ready else 503

# =========================================================
# WEBHOOK
# =========================================================
@app.route("/callback", methods=["POST"])
def callback():
    total_started = time.perf_counter()
    trace_id = make_trace_id()
    body = request.get_data()
    sig = request.headers.get("X-Line-Signature", "").strip()

    logger.info(f"[{trace_id}] WEBHOOK_RECEIVED")

    source_type = ""
    group_id = ""
    room_id = ""

    if not is_runtime_ready():
        logger.error(f"[{trace_id}] RUNTIME_NOT_READY")
        log_total_latency(
            trace_id=trace_id,
            route_name="runtime_not_ready",
            total_ms=ms_since(total_started),
            source_type=source_type,
            group_id=group_id,
            room_id=room_id
        )
        return "Service unavailable", 503

    # 1) SIGNATURE
    if not verify_line_signature(LINE_CHANNEL_SECRET, body, sig):
        logger.error(f"[{trace_id}] INVALID_SIGNATURE")
        log_total_latency(
            trace_id=trace_id,
            route_name="invalid_signature",
            total_ms=ms_since(total_started),
            source_type=source_type,
            group_id=group_id,
            room_id=room_id
        )
        return "Invalid", 403

    # 2) JSON PARSE
    try:
        payload = json.loads(body.decode("utf-8"))
    except Exception as e:
        logger.exception(f"[{trace_id}] JSON_PARSE_ERROR exception={type(e).__name__}:{e}")
        log_total_latency(
            trace_id=trace_id,
            route_name="bad_payload",
            total_ms=ms_since(total_started),
            source_type=source_type,
            group_id=group_id,
            room_id=room_id
        )
        return "Bad payload", 400

    events = payload.get("events", [])
    if not events:
        logger.warning(f"[{trace_id}] NO_EVENTS")
        log_total_latency(
            trace_id=trace_id,
            route_name="no_events",
            total_ms=ms_since(total_started),
            source_type=source_type,
            group_id=group_id,
            room_id=room_id
        )
        return "OK", 200

    event = events[0]

    if event.get("type") != "message":
        logger.info(f"[{trace_id}] SKIP_NON_MESSAGE type={event.get('type')}")
        log_total_latency(
            trace_id=trace_id,
            route_name="skip_non_message",
            total_ms=ms_since(total_started),
            source_type=source_type,
            group_id=group_id,
            room_id=room_id
        )
        return "OK", 200

    message = event.get("message", {})
    if message.get("type") != "text":
        logger.info(f"[{trace_id}] SKIP_NON_TEXT message_type={message.get('type')}")
        log_total_latency(
            trace_id=trace_id,
            route_name="skip_non_text",
            total_ms=ms_since(total_started),
            source_type=source_type,
            group_id=group_id,
            room_id=room_id
        )
        return "OK", 200

    # 3) EXTRACT
    source_type, user_id, group_id, room_id = extract_source_ids(event)
    scope_key = get_scope_key(source_type, user_id, group_id, room_id)

    input_text = safe_str(message.get("text"))
    reply_token = safe_str(event.get("replyToken"))

    logger.info(
        f"[{trace_id}] INPUT source_type={source_type} "
        f"group_id={group_id} room_id={room_id} text={json.dumps(input_text, ensure_ascii=False)}"
    )

    logger.info(
        f"[{trace_id}] STATE_READ_SKIPPED_FOR_TIMEOUT_ISOLATION "
        f"user_id={user_id} scope_key={scope_key}"
    )

    # 4) EMPTY INPUT
    if not input_text:
        logger.info(f"[{trace_id}] SKIP_EMPTY_TEXT")
        log_total_latency(
            trace_id=trace_id,
            route_name="skip_empty_text",
            total_ms=ms_since(total_started),
            source_type=source_type,
            group_id=group_id,
            room_id=room_id
        )
        return "OK", 200

    # =====================================================
    # COMMAND: !all
    # =====================================================
    is_all_command, content = parse_all_command(input_text)

    if is_all_command:
        if not is_admin(user_id):
            log_all_audit(
                trace_id=trace_id,
                user_id=user_id,
                source_type=source_type,
                group_id=group_id,
                room_id=room_id,
                raw_input=input_text,
                content=content,
                status="DENY_NOT_ADMIN",
                note="user is not in ADMIN_IDS"
            )
            reply_ok, reply_ms = line_reply(reply_token, "❌ Không có quyền", trace_id)
            log_total_latency(
                trace_id=trace_id,
                route_name="all_deny_not_admin",
                total_ms=ms_since(total_started),
                source_type=source_type,
                group_id=group_id,
                room_id=room_id
            )
            logger.info(f"[{trace_id}] DENY_NOT_ADMIN reply_ok={reply_ok} reply_ms={reply_ms}")
            return "OK", 200

        if not content:
            log_all_audit(
                trace_id=trace_id,
                user_id=user_id,
                source_type=source_type,
                group_id=group_id,
                room_id=room_id,
                raw_input=input_text,
                content=content,
                status="DENY_EMPTY",
                note="!all without content"
            )
            reply_ok, reply_ms = line_reply(reply_token, "⚠️ !all cần nội dung", trace_id)
            log_total_latency(
                trace_id=trace_id,
                route_name="all_deny_empty",
                total_ms=ms_since(total_started),
                source_type=source_type,
                group_id=group_id,
                room_id=room_id
            )
            logger.info(f"[{trace_id}] DENY_EMPTY reply_ok={reply_ok} reply_ms={reply_ms}")
            return "OK", 200

        if len(content) > MAX_ALL_CHARS:
            log_all_audit(
                trace_id=trace_id,
                user_id=user_id,
                source_type=source_type,
                group_id=group_id,
                room_id=room_id,
                raw_input=input_text,
                content=content,
                status="DENY_TOO_LONG",
                note=f"content length > {MAX_ALL_CHARS}"
            )
            reply_ok, reply_ms = line_reply(
                reply_token,
                f"⚠️ !all tối đa {MAX_ALL_CHARS} ký tự",
                trace_id
            )
            log_total_latency(
                trace_id=trace_id,
                route_name="all_deny_too_long",
                total_ms=ms_since(total_started),
                source_type=source_type,
                group_id=group_id,
                room_id=room_id
            )
            logger.info(f"[{trace_id}] DENY_TOO_LONG reply_ok={reply_ok} reply_ms={reply_ms}")
            return "OK", 200

        allowed, remaining = allow_all_command(user_id, scope_key)
        if not allowed:
            log_all_audit(
                trace_id=trace_id,
                user_id=user_id,
                source_type=source_type,
                group_id=group_id,
                room_id=room_id,
                raw_input=input_text,
                content=content,
                status="DENY_RATE_LIMIT",
                note=f"cooldown_remaining={remaining}s"
            )
            reply_ok, reply_ms = line_reply(
                reply_token,
                f"⏳ Vui lòng chờ {remaining} giây rồi dùng !all lại",
                trace_id
            )
            log_total_latency(
                trace_id=trace_id,
                route_name="all_deny_rate_limit",
                total_ms=ms_since(total_started),
                source_type=source_type,
                group_id=group_id,
                room_id=room_id
            )
            logger.info(f"[{trace_id}] DENY_RATE_LIMIT reply_ok={reply_ok} reply_ms={reply_ms}")
            return "OK", 200

        translated, translate_ms, detected_source_language = translate_auto_source(
            content,
            LOCKED_TARGET_LANG,
            trace_id
        )

        final_text = translated or content
        msg = f"📢 THÔNG BÁO:\n{final_text}"

        reply_ok, reply_ms = line_reply(reply_token, msg, trace_id)

        log_all_audit(
            trace_id=trace_id,
            user_id=user_id,
            source_type=source_type,
            group_id=group_id,
            room_id=room_id,
            raw_input=input_text,
            content=content,
            status="SUCCESS" if reply_ok else "FAILED_REPLY",
            note=(
                f"detected_source_language={detected_source_language or 'unknown'} "
                f"translate_ms={translate_ms} "
                f"reply_ms={reply_ms}"
            )
        )

        log_total_latency(
            trace_id=trace_id,
            route_name="all_success" if reply_ok else "all_failed_reply",
            total_ms=ms_since(total_started),
            source_type=source_type,
            group_id=group_id,
            room_id=room_id
        )
        return "OK", 200

    # =====================================================
    # NORMAL TRANSLATE
    # =====================================================
    translated, translate_ms, detected_source_language = translate_auto_source(
        input_text,
        LOCKED_TARGET_LANG,
        trace_id
    )
    reply_ok, reply_ms = line_reply(reply_token, translated or FALLBACK_REPLY_TEXT, trace_id)

    logger.info(
        f"[NORMAL_FLOW] trace_id={trace_id} "
        f"detected_source_language={detected_source_language or 'unknown'} "
        f"translate_ms={translate_ms} "
        f"reply_ms={reply_ms} "
        f"reply_ok={reply_ok}"
    )

    log_total_latency(
        trace_id=trace_id,
        route_name="normal_translate",
        total_ms=ms_since(total_started),
        source_type=source_type,
        group_id=group_id,
        room_id=room_id
    )
    return "OK", 200

# =========================================================
# MAIN
# =========================================================
if __name__ == "__main__":
    port = int(os.getenv("PORT", "5000"))
    app.run(host="0.0.0.0", port=port)
