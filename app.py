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
FREE_DAILY_CHAR_LIMIT = int(os.getenv("FREE_DAILY_CHAR_LIMIT", "1000").strip() or "1000")

# Múi giờ Đài Loan
TW_TZ = timezone(timedelta(hours=8))

# =========================================================
# BUSINESS LOCK
# =========================================================
LOCKED_TARGET_LANG = "zh-TW"

# =========================================================
# CONSTANTS
# =========================================================
SHEET_HEADERS = [
    "trace_id",
    "event_id",
    "received_at",
    "user_id",
    "source_type",
    "input_text",
    "detected_lang",
    "target_lang",
    "translated_text",
    "final_status",
    "error_code",
    "latency_ms",
    "char_count",
    "request_type",
    "is_group_format",
    "group_id",
    "room_id",
    "billing_scope",
    "billing_key",
    "quota_limit",
    "quota_used",
    "quota_remaining",
]

ALLOWED_FINAL_STATUS = {
    "SUCCESS",
    "FAILED_SIGNATURE",
    "FAILED_PARSE",
    "FAILED_TRANSLATE",
    "FAILED_REPLY",
    "FAILED_SHEET",
    "PARTIAL_SUCCESS",
    "BLOCKED_QUOTA",
}

ALLOWED_ERROR_CODE = {
    "NONE",
    "GOOGLE_TIMEOUT",
    "GOOGLE_API_ERROR",
    "LINE_REPLY_400",
    "SHEET_APPEND_ERROR",
    "INVALID_SIGNATURE",
    "QUOTA_EXCEEDED",
}

HTTP_TIMEOUT_SECONDS = 15
FALLBACK_REPLY_TEXT = "Hệ thống bận, thử lại sau."

# =========================================================
# BASIC HELPERS
# =========================================================
def now_tw_iso() -> str:
    return datetime.now(TW_TZ).isoformat()


def make_trace_id() -> str:
    return f"trc_{uuid.uuid4().hex[:16]}"


def safe_str(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def mask_text(text: str, max_len: int = 1000) -> str:
    text = safe_str(text)
    if len(text) <= max_len:
        return text
    return text[:max_len]


def get_locked_target_lang() -> str:
    return LOCKED_TARGET_LANG


def safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(str(value).strip())
    except Exception:
        return default


def get_tw_date_str() -> str:
    return datetime.now(TW_TZ).date().isoformat()


def extract_source_ids(event: Dict[str, Any]) -> Tuple[str, str, str]:
    source = event.get("source", {})
    source_type = safe_str(source.get("type"))
    user_id = safe_str(source.get("userId"))
    group_id = safe_str(source.get("groupId"))
    room_id = safe_str(source.get("roomId"))
    return source_type, user_id, group_id, room_id


def get_billing_target(source_type: str, user_id: str, group_id: str, room_id: str) -> Tuple[str, str]:
    if source_type == "group" and group_id:
        return "GROUP", group_id
    if source_type == "room" and room_id:
        return "ROOM", room_id
    return "USER", user_id


def make_quota_block_message() -> str:
    return (
        "🚫 Bạn đã dùng hết lượt miễn phí hôm nay\n\n"
        "Để tiếp tục dịch ngay:\n"
        "→ Nâng cấp gói Pro (không giới hạn)\n\n"
        "Chỉ 5 USD/tháng"
    )


# =========================================================
# GOOGLE SHEETS HELPERS
# =========================================================
def load_service_account_info() -> Dict[str, Any]:
    if not GOOGLE_SERVICE_ACCOUNT_JSON:
        raise ValueError("Missing GOOGLE_SERVICE_ACCOUNT_JSON")

    raw = GOOGLE_SERVICE_ACCOUNT_JSON

    try:
        if raw.startswith("{"):
            return json.loads(raw)

        decoded = base64.b64decode(raw).decode("utf-8")
        return json.loads(decoded)
    except Exception as exc:
        raise ValueError(f"Invalid GOOGLE_SERVICE_ACCOUNT_JSON: {exc}") from exc



def get_gspread_client():
    info = load_service_account_info()
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(info, scope)
    return gspread.authorize(creds)



def ensure_headers(ws) -> None:
    existing = ws.row_values(1)
    if existing != SHEET_HEADERS:
        if not existing:
            ws.append_row(SHEET_HEADERS, value_input_option="RAW")
        else:
            raise ValueError(
                f"Worksheet headers mismatch. Found={existing}, Expected={SHEET_HEADERS}"
            )



def get_forensic_ws():
    logger.info("SHEET_TRACE: step=load_client:start")
    gc = get_gspread_client()
    logger.info("SHEET_TRACE: step=load_client:ok")

    logger.info(f"SHEET_TRACE: step=open_spreadsheet:start name={GOOGLE_SHEET_NAME}")
    sh = gc.open(GOOGLE_SHEET_NAME)
    logger.info("SHEET_TRACE: step=open_spreadsheet:ok")

    logger.info(f"SHEET_TRACE: step=open_worksheet:start worksheet={GOOGLE_SHEET_WORKSHEET}")
    ws = sh.worksheet(GOOGLE_SHEET_WORKSHEET)
    logger.info("SHEET_TRACE: step=open_worksheet:ok")

    logger.info("SHEET_TRACE: step=ensure_headers:start")
    ensure_headers(ws)
    logger.info("SHEET_TRACE: step=ensure_headers:ok")

    return ws



def event_already_logged(ws, event_id: str) -> bool:
    if not event_id:
        return False

    values = ws.col_values(2)
    return event_id in values[1:]


def get_today_quota_used(ws, billing_key: str) -> int:
    if not billing_key:
        return 0

    records = ws.get_all_records(expected_headers=SHEET_HEADERS)
    today = get_tw_date_str()
    total = 0

    for record in records:
        if safe_str(record.get("billing_key")) != billing_key:
            continue

        received_at = safe_str(record.get("received_at"))
        if not received_at.startswith(today):
            continue

        total += safe_int(record.get("char_count"), 0)

    return total


# =========================================================
# LINE / MESSAGE HELPERS
# =========================================================
def verify_line_signature(channel_secret: str, body: bytes, signature: str) -> bool:
    if not channel_secret or not signature:
        return False

    digest = hmac.new(
        channel_secret.encode("utf-8"),
        body,
        hashlib.sha256
    ).digest()
    computed = base64.b64encode(digest).decode("utf-8")
    return hmac.compare_digest(computed, signature)



def parse_message_event(payload: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    events = payload.get("events", [])
    if not events:
        return None, "No events in payload"

    event = events[0]
    if event.get("type") != "message":
        return None, "First event is not message"

    message = event.get("message", {})
    if message.get("type") != "text":
        return None, "Message type is not text"

    return event, None



def extract_prefix_and_content(text: str) -> Tuple[str, str]:
    text = safe_str(text)
    if not text:
        return "", ""

    if not text.startswith("@"):
        return "", text

    if " - " in text:
        left, right = text.split(" - ", 1)
        right = safe_str(right)
        right_parts = right.split(" ", 1)

        if len(right_parts) == 2:
            alias = safe_str(right_parts[0])
            content = safe_str(right_parts[1])
            if content:
                prefix = f"{safe_str(left)} - {alias}".strip()
                return prefix, content

    parts = text.split(" ", 1)
    if len(parts) == 2:
        return safe_str(parts[0]), safe_str(parts[1])

    return text, ""



def build_group_reply_text(prefix_text: str, translated_text: str) -> str:
    prefix_text = safe_str(prefix_text)
    translated_text = safe_str(translated_text) or FALLBACK_REPLY_TEXT

    if prefix_text:
        return f"{prefix_text} {translated_text}".strip()
    return translated_text


# =========================================================
# GOOGLE TRANSLATE HELPERS
# =========================================================
def google_detect_language(text: str, trace_id: str) -> Tuple[Optional[str], Optional[str]]:
    url = "https://translation.googleapis.com/language/translate/v2/detect"
    params = {"key": GOOGLE_API_KEY}
    data = {"q": text}

    try:
        resp = requests.post(url, params=params, data=data, timeout=HTTP_TIMEOUT_SECONDS)
        logger.info(f"[{trace_id}] Google detect status={resp.status_code}")

        if resp.status_code != 200:
            logger.error(f"[{trace_id}] Google detect body={resp.text}")
            return None, f"DETECT_HTTP_{resp.status_code}"

        payload = resp.json()
        detections = payload.get("data", {}).get("detections", [])
        if not detections or not detections[0]:
            return None, "DETECT_EMPTY"

        lang = detections[0][0].get("language")
        return lang, None

    except requests.Timeout:
        logger.exception(f"[{trace_id}] Google detect timeout")
        return None, "GOOGLE_TIMEOUT"
    except Exception as exc:
        logger.exception(f"[{trace_id}] Google detect exception: {exc}")
        return None, "DETECT_EXCEPTION"



def google_translate_text(
    text: str,
    target_lang: str,
    source_lang: Optional[str],
    trace_id: str
) -> Tuple[Optional[str], Optional[str]]:
    url = "https://translation.googleapis.com/language/translate/v2"
    params = {"key": GOOGLE_API_KEY}

    data = {
        "q": text,
        "target": target_lang,
        "format": "text",
    }
    if source_lang:
        data["source"] = source_lang

    try:
        resp = requests.post(url, params=params, data=data, timeout=HTTP_TIMEOUT_SECONDS)
        logger.info(f"[{trace_id}] Google translate status={resp.status_code}")

        if resp.status_code != 200:
            logger.error(f"[{trace_id}] Google translate body={resp.text}")
            return None, f"TRANSLATE_HTTP_{resp.status_code}"

        payload = resp.json()
        translations = payload.get("data", {}).get("translations", [])
        if not translations:
            return None, "TRANSLATE_EMPTY"

        translated = translations[0].get("translatedText", "")
        translated = html.unescape(translated)
        return translated, None

    except requests.Timeout:
        logger.exception(f"[{trace_id}] Google translate timeout")
        return None, "GOOGLE_TIMEOUT"
    except Exception as exc:
        logger.exception(f"[{trace_id}] Google translate exception: {exc}")
        return None, "TRANSLATE_EXCEPTION"


# =========================================================
# LINE REPLY HELPERS
# =========================================================
def line_reply(reply_token: str, text: str, trace_id: str) -> Tuple[bool, Optional[str]]:
    final_text = safe_str(text) or FALLBACK_REPLY_TEXT

    url = "https://api.line.me/v2/bot/message/reply"
    headers = {
        "Authorization": f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}",
        "Content-Type": "application/json",
    }
    payload = {
        "replyToken": reply_token,
        "messages": [
            {
                "type": "text",
                "text": final_text,
            }
        ],
    }

    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=HTTP_TIMEOUT_SECONDS)
        logger.info(f"[{trace_id}] LINE reply status={resp.status_code}")

        if resp.status_code == 200:
            return True, None

        logger.error(f"[{trace_id}] LINE reply body={resp.text}")
        if resp.status_code == 400:
            return False, "LINE_REPLY_400"
        return False, f"LINE_REPLY_HTTP_{resp.status_code}"

    except requests.Timeout:
        logger.exception(f"[{trace_id}] LINE reply timeout")
        return False, "LINE_REPLY_TIMEOUT"
    except Exception as exc:
        logger.exception(f"[{trace_id}] LINE reply exception: {exc}")
        return False, "LINE_REPLY_EXCEPTION"


# =========================================================
# FORENSIC HELPERS
# =========================================================
def append_forensic_row(row: List[Any], trace_id: str) -> Tuple[bool, Optional[str]]:
    try:
        logger.info(f"[{trace_id}] SHEET_TRACE: step=append_row:start")
        ws = get_forensic_ws()
        ws.append_row(row, value_input_option="RAW")
        logger.info(f"[{trace_id}] SHEET_TRACE: step=append_row:ok")
        logger.info(f"[{trace_id}] Sheet append success")
        return True, None
    except Exception as exc:
        logger.exception(f"[{trace_id}] Sheet append failed: {exc}")
        return False, "SHEET_APPEND_ERROR"



def build_row(
    trace_id: str,
    event_id: str,
    received_at: str,
    user_id: str,
    source_type: str,
    input_text: str,
    detected_lang: str,
    target_lang: str,
    translated_text: str,
    final_status: str,
    error_code: str,
    latency_ms: int,
    group_id: str = "",
    room_id: str = "",
    billing_scope: str = "",
    billing_key: str = "",
    quota_limit: int = 0,
    quota_used: int = 0,
) -> List[Any]:
    if final_status not in ALLOWED_FINAL_STATUS:
        raise ValueError(f"Invalid final_status: {final_status}")

    if error_code not in ALLOWED_ERROR_CODE:
        error_code = "GOOGLE_API_ERROR" if error_code else "NONE"

    raw_input_text = safe_str(input_text)
    char_count = len(raw_input_text)
    request_type = "text" if raw_input_text else "empty"
    is_group_format = "YES" if raw_input_text.startswith("@") else "NO"

    return [
        trace_id,
        event_id,
        received_at,
        user_id,
        source_type,
        mask_text(raw_input_text),
        detected_lang,
        target_lang,
        mask_text(translated_text),
        final_status,
        error_code,
        latency_ms,
        char_count,
        request_type,
        is_group_format,
    ]



def compute_translate_error_code(err: Optional[str]) -> str:
    if err == "GOOGLE_TIMEOUT":
        return "GOOGLE_TIMEOUT"
    if err:
        return "GOOGLE_API_ERROR"
    return "NONE"



def compute_reply_error_code(err: Optional[str]) -> str:
    if err == "LINE_REPLY_400":
        return "LINE_REPLY_400"
    return "NONE"


# =========================================================
# HEALTH
# =========================================================
@app.route("/", methods=["GET"])
def health():
    return jsonify({
        "ok": True,
        "service": "line-multilang-bot",
        "time": now_tw_iso(),
        "target_lang_lock": LOCKED_TARGET_LANG,
    }), 200


# =========================================================
# WEBHOOK
# =========================================================
@app.route("/callback", methods=["POST"])
def callback():
    started = time.perf_counter()
    trace_id = make_trace_id()
    received_at = now_tw_iso()

    body_bytes = request.get_data()
    body_text = body_bytes.decode("utf-8", errors="replace")
    signature = request.headers.get("X-Line-Signature", "").strip()

    logger.info(f"[{trace_id}] Webhook received")

    event_id = ""
    user_id = ""
    source_type = ""
    group_id = ""
    room_id = ""
    billing_scope = ""
    billing_key = ""
    quota_limit = FREE_DAILY_CHAR_LIMIT
    quota_used = 0
    input_text = ""
    detected_lang = ""
    target_lang = get_locked_target_lang()
    translated_text = ""
    final_status = "FAILED_PARSE"
    error_code = "NONE"

    # 1) VERIFY SIGNATURE
    is_valid_signature = verify_line_signature(
        LINE_CHANNEL_SECRET,
        body_bytes,
        signature,
    )
    if not is_valid_signature:
        latency_ms = int((time.perf_counter() - started) * 1000)
        final_status = "FAILED_SIGNATURE"
        error_code = "INVALID_SIGNATURE"

        logger.error(f"[{trace_id}] Signature invalid")

        row = build_row(
            trace_id=trace_id,
            event_id=event_id,
            received_at=received_at,
            user_id=user_id,
            source_type=source_type,
            input_text=input_text,
            detected_lang=detected_lang,
            target_lang=target_lang,
            translated_text=translated_text,
            final_status=final_status,
            error_code=error_code,
            latency_ms=latency_ms,
            group_id=group_id,
            room_id=room_id,
            billing_scope=billing_scope,
            billing_key=billing_key,
            quota_limit=quota_limit,
            quota_used=quota_used,
        )
        append_forensic_row(row, trace_id)
        return "Invalid signature", 403

    # 2) PARSE PAYLOAD
    try:
        payload = json.loads(body_text)
    except Exception as exc:
        latency_ms = int((time.perf_counter() - started) * 1000)
        final_status = "FAILED_PARSE"

        logger.exception(f"[{trace_id}] JSON parse failed: {exc}")

        row = build_row(
            trace_id=trace_id,
            event_id=event_id,
            received_at=received_at,
            user_id=user_id,
            source_type=source_type,
            input_text=input_text,
            detected_lang=detected_lang,
            target_lang=target_lang,
            translated_text=translated_text,
            final_status=final_status,
            error_code=error_code,
            latency_ms=latency_ms,
            group_id=group_id,
            room_id=room_id,
            billing_scope=billing_scope,
            billing_key=billing_key,
            quota_limit=quota_limit,
            quota_used=quota_used,
        )
        append_forensic_row(row, trace_id)
        return "Bad payload", 400

    event, parse_error = parse_message_event(payload)
    if parse_error:
        logger.warning(f"[{trace_id}] Verify/empty event payload: {parse_error}")
        return "OK", 200

    # 3) EXTRACT DATA
    event_id = safe_str(event.get("message", {}).get("id"))
    source_type, user_id, group_id, room_id = extract_source_ids(event)
    billing_scope, billing_key = get_billing_target(source_type, user_id, group_id, room_id)
    input_text = safe_str(event.get("message", {}).get("text"))
    reply_token = safe_str(event.get("replyToken"))

    prefix_text, clean_input_text = extract_prefix_and_content(input_text)

    logger.info(
        f"[{trace_id}] Parsed event_id={event_id} user_id={user_id} "
        f"source_type={source_type} input_text={input_text}"
    )

    # 4) IDEMPOTENCY CHECK
    try:
        logger.info(f"[{trace_id}] SHEET_TRACE: step=idempotency_check:start")
        ws = get_forensic_ws()
        if event_already_logged(ws, event_id):
            logger.warning(f"[{trace_id}] Duplicate event_id={event_id} ignored")
            return "OK", 200

        quota_used = get_today_quota_used(ws, billing_key)
        current_char_count = len(input_text or "")
        if billing_key and (quota_used + current_char_count) > quota_limit:
            logger.warning(
                f"[{trace_id}] Quota exceeded billing_scope={billing_scope} "
                f"billing_key={billing_key} used={quota_used} current={current_char_count} limit={quota_limit}"
            )
            block_message = make_quota_block_message()
            reply_ok, reply_err = line_reply(reply_token, block_message, trace_id)
            latency_ms = int((time.perf_counter() - started) * 1000)

            final_status = "BLOCKED_QUOTA" if reply_ok else "FAILED_REPLY"
            error_code = "QUOTA_EXCEEDED" if reply_ok else compute_reply_error_code(reply_err)

            row = build_row(
                trace_id=trace_id,
                event_id=event_id,
                received_at=received_at,
                user_id=user_id,
                source_type=source_type,
                input_text=input_text,
                detected_lang="",
                target_lang=target_lang,
                translated_text=block_message,
                final_status=final_status,
                error_code=error_code,
                latency_ms=latency_ms,
                group_id=group_id,
                room_id=room_id,
                billing_scope=billing_scope,
                billing_key=billing_key,
                quota_limit=quota_limit,
                quota_used=quota_used,
            )
            append_forensic_row(row, trace_id)
            return "OK", 200

        logger.info(
            f"[{trace_id}] QUOTA_CHECK billing_scope={billing_scope} billing_key={billing_key} "
            f"used={quota_used} current={current_char_count} limit={quota_limit}"
        )
        logger.info(f"[{trace_id}] SHEET_TRACE: step=idempotency_check:ok")
    except Exception as exc:
        logger.exception(f"[{trace_id}] Idempotency check failed: {exc}")

    # 5) TARGET LANG LOCK
    target_lang = get_locked_target_lang()

    # 6) EMPTY CONTENT AFTER PREFIX SPLIT
    if not clean_input_text:
        final_reply_text = prefix_text or FALLBACK_REPLY_TEXT
        reply_ok, reply_err = line_reply(reply_token, final_reply_text, trace_id)
        latency_ms = int((time.perf_counter() - started) * 1000)

        final_status = "SUCCESS" if reply_ok else "FAILED_REPLY"
        error_code = "NONE" if reply_ok else compute_reply_error_code(reply_err)

        row = build_row(
            trace_id=trace_id,
            event_id=event_id,
            received_at=received_at,
            user_id=user_id,
            source_type=source_type,
            input_text=input_text,
            detected_lang="",
            target_lang=target_lang,
            translated_text=final_reply_text,
            final_status=final_status,
            error_code=error_code,
            latency_ms=latency_ms,
            group_id=group_id,
            room_id=room_id,
            billing_scope=billing_scope,
            billing_key=billing_key,
            quota_limit=quota_limit,
            quota_used=quota_used,
        )
        append_forensic_row(row, trace_id)
        return "OK", 200

    # 7) DETECT LANGUAGE
    detected_lang, detect_err = google_detect_language(clean_input_text, trace_id)
    if detect_err:
        latency_ms = int((time.perf_counter() - started) * 1000)
        final_status = "FAILED_TRANSLATE"
        error_code = compute_translate_error_code(detect_err)

        row = build_row(
            trace_id=trace_id,
            event_id=event_id,
            received_at=received_at,
            user_id=user_id,
            source_type=source_type,
            input_text=input_text,
            detected_lang=detected_lang or "",
            target_lang=target_lang,
            translated_text="",
            final_status=final_status,
            error_code=error_code,
            latency_ms=latency_ms,
            group_id=group_id,
            room_id=room_id,
            billing_scope=billing_scope,
            billing_key=billing_key,
            quota_limit=quota_limit,
            quota_used=quota_used,
        )
        append_forensic_row(row, trace_id)
        return "OK", 200

    # 8) TRANSLATE
    translated_text, translate_err = google_translate_text(
        text=clean_input_text,
        target_lang=target_lang,
        source_lang=detected_lang,
        trace_id=trace_id,
    )

    if translate_err or not translated_text:
        latency_ms = int((time.perf_counter() - started) * 1000)
        final_status = "FAILED_TRANSLATE"
        error_code = compute_translate_error_code(translate_err)

        row = build_row(
            trace_id=trace_id,
            event_id=event_id,
            received_at=received_at,
            user_id=user_id,
            source_type=source_type,
            input_text=input_text,
            detected_lang=detected_lang or "",
            target_lang=target_lang,
            translated_text="",
            final_status=final_status,
            error_code=error_code,
            latency_ms=latency_ms,
            group_id=group_id,
            room_id=room_id,
            billing_scope=billing_scope,
            billing_key=billing_key,
            quota_limit=quota_limit,
            quota_used=quota_used,
        )
        append_forensic_row(row, trace_id)
        return "OK", 200

    # 9) BUILD FINAL REPLY
    final_reply_text = build_group_reply_text(prefix_text, translated_text)

    # 10) REPLY LINE
    reply_ok, reply_err = line_reply(reply_token, final_reply_text, trace_id)
    latency_ms = int((time.perf_counter() - started) * 1000)

    if reply_ok:
        final_status = "SUCCESS"
        error_code = "NONE"
    else:
        final_status = "FAILED_REPLY"
        error_code = compute_reply_error_code(reply_err)

    # 11) APPEND SINGLE FORENSIC ROW
    row = build_row(
        trace_id=trace_id,
        event_id=event_id,
        received_at=received_at,
        user_id=user_id,
        source_type=source_type,
        input_text=input_text,
        detected_lang=detected_lang or "",
        target_lang=target_lang,
        translated_text=final_reply_text or "",
        final_status=final_status,
        error_code=error_code,
        latency_ms=latency_ms,
    )

    append_ok, append_err = append_forensic_row(row, trace_id)

    # 12) FINAL HTTP
    if not append_ok:
        logger.error(f"[{trace_id}] FAILED_SHEET append_err={append_err}")
        return "OK", 200

    return "OK", 200


# =========================================================
# MAIN
# =========================================================
if __name__ == "__main__":
    port = int(os.getenv("PORT", "5000"))
    app.run(host="0.0.0.0", port=port)
