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
HTTP_TIMEOUT_SECONDS = 15
FALLBACK_REPLY_TEXT = "Hệ thống bận, thử lại sau."

# In-memory rate limit store
# key = "{user_id}:{group_or_room_or_user}"
LAST_ALL_USED_AT = {}

# =========================================================
# BASIC HELPERS
# =========================================================
def now_tw_iso():
    return datetime.now(TW_TZ).isoformat()

def make_trace_id():
    return f"trc_{uuid.uuid4().hex[:12]}"

def safe_str(v):
    return str(v).strip() if v else ""

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

def allow_all_command(user_id: str, scope_key: str):
    now_ts = get_now_ts()
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

    if not lowered.startswith("!all"):
        return False, ""

    content = raw[4:].strip()
    return True, content

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
    final_text = safe_str(text) or FALLBACK_REPLY_TEXT

    url = "https://api.line.me/v2/bot/message/reply"
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

    try:
        r = requests.post(
            url,
            headers=headers,
            json=payload,
            timeout=HTTP_TIMEOUT_SECONDS
        )
        logger.info(f"[{trace_id}] LINE_REPLY status={r.status_code}")
        if r.status_code != 200:
            logger.error(f"[{trace_id}] LINE_REPLY body={r.text}")
        return r.status_code == 200
    except Exception as e:
        logger.exception(f"[{trace_id}] LINE_REPLY exception={e}")
        return False

# =========================================================
# GOOGLE TRANSLATE HELPERS
# =========================================================
def detect_lang(text, trace_id):
    try:
        url = "https://translation.googleapis.com/language/translate/v2/detect"
        r = requests.post(
            url,
            params={"key": GOOGLE_API_KEY},
            data={"q": text},
            timeout=HTTP_TIMEOUT_SECONDS
        )

        logger.info(f"[{trace_id}] GOOGLE_DETECT status={r.status_code}")

        if r.status_code != 200:
            logger.error(f"[{trace_id}] GOOGLE_DETECT body={r.text}")
            return None

        payload = r.json()
        return payload["data"]["detections"][0][0]["language"]
    except Exception as e:
        logger.exception(f"[{trace_id}] GOOGLE_DETECT exception={e}")
        return None

def translate(text, target, source, trace_id):
    try:
        url = "https://translation.googleapis.com/language/translate/v2"
        data = {
            "q": text,
            "target": target,
            "format": "text"
        }
        if source:
            data["source"] = source

        r = requests.post(
            url,
            params={"key": GOOGLE_API_KEY},
            data=data,
            timeout=HTTP_TIMEOUT_SECONDS
        )

        logger.info(f"[{trace_id}] GOOGLE_TRANSLATE status={r.status_code}")

        if r.status_code != 200:
            logger.error(f"[{trace_id}] GOOGLE_TRANSLATE body={r.text}")
            return None

        payload = r.json()
        translated = payload["data"]["translations"][0]["translatedText"]
        return html.unescape(translated)
    except Exception as e:
        logger.exception(f"[{trace_id}] GOOGLE_TRANSLATE exception={e}")
        return None

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

# =========================================================
# HEALTH
# =========================================================
@app.route("/", methods=["GET"])
def health():
    return jsonify({
        "ok": True,
        "service": "line-bot-render-step2-clean",
        "time": now_tw_iso()
    }), 200

# =========================================================
# WEBHOOK
# =========================================================
@app.route("/callback", methods=["POST"])
def callback():
    trace_id = make_trace_id()
    body = request.get_data()
    sig = request.headers.get("X-Line-Signature", "").strip()

    logger.info(f"[{trace_id}] WEBHOOK_RECEIVED")

    # 1) SIGNATURE
    if not verify_line_signature(LINE_CHANNEL_SECRET, body, sig):
        logger.error(f"[{trace_id}] INVALID_SIGNATURE")
        return "Invalid", 403

    # 2) JSON PARSE
    try:
        payload = json.loads(body.decode("utf-8"))
    except Exception as e:
        logger.exception(f"[{trace_id}] JSON_PARSE_ERROR exception={e}")
        return "Bad payload", 400

    events = payload.get("events", [])
    if not events:
        logger.warning(f"[{trace_id}] NO_EVENTS")
        return "OK", 200

    event = events[0]

    if event.get("type") != "message":
        logger.info(f"[{trace_id}] SKIP_NON_MESSAGE type={event.get('type')}")
        return "OK", 200

    message = event.get("message", {})
    if message.get("type") != "text":
        logger.info(f"[{trace_id}] SKIP_NON_TEXT message_type={message.get('type')}")
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

    # 4) EMPTY INPUT
    if not input_text:
        logger.info(f"[{trace_id}] SKIP_EMPTY_TEXT")
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
            line_reply(reply_token, "❌ Không có quyền", trace_id)
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
            line_reply(reply_token, "⚠️ !all cần nội dung", trace_id)
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
            line_reply(
                reply_token,
                f"⚠️ !all tối đa {MAX_ALL_CHARS} ký tự",
                trace_id
            )
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
            line_reply(
                reply_token,
                f"⏳ Vui lòng chờ {remaining} giây rồi dùng !all lại",
                trace_id
            )
            return "OK", 200

        lang = detect_lang(content, trace_id)
        translated = translate(content, LOCKED_TARGET_LANG, lang, trace_id)

        final_text = translated or content
        msg = f"📢 THÔNG BÁO:\n{final_text}"

        reply_ok = line_reply(reply_token, msg, trace_id)

        log_all_audit(
            trace_id=trace_id,
            user_id=user_id,
            source_type=source_type,
            group_id=group_id,
            room_id=room_id,
            raw_input=input_text,
            content=content,
            status="SUCCESS" if reply_ok else "FAILED_REPLY",
            note=f"detected_lang={lang or 'unknown'}"
        )
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
    port = int(os.getenv("PORT", "5000"))
    app.run(host="0.0.0.0", port=port)
