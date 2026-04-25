import os
import re
import json
import html
import hmac
import hashlib
import base64
import time
import uuid
import logging
import threading
from queue import Queue, Full, Empty
from collections import deque
import unicodedata
import difflib
from datetime import datetime, timezone, timedelta
from typing import Dict, Tuple, Optional, List
import requests
import gspread
from google.oauth2.service_account import Credentials
from flask import Flask, request, jsonify, g
app = Flask(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

# --- SECURITY_TENANT_GUARD_V1 ---
DEFAULT_TENANT_ID = os.getenv("DEFAULT_TENANT_ID", "tenant_default").strip() or "tenant_default"
TENANT_HEADER_NAME = os.getenv("TENANT_HEADER_NAME", "X-DT79-Tenant-ID").strip() or "X-DT79-Tenant-ID"
ADMIN_ACCESS_SHEET_NAME = os.getenv("ADMIN_ACCESS_SHEET_NAME", "ADMIN_ACCESS_MASTER").strip() or "ADMIN_ACCESS_MASTER"
ADMIN_ACCESS_CACHE_TTL_SECONDS = int(os.getenv("ADMIN_ACCESS_CACHE_TTL_SECONDS", "60").strip() or "60")
ADMIN_ACCESS_HEADERS = ["tenant_id", "line_user_id", "role", "status", "revoked_at", "note"]
_ADMIN_ACCESS_CACHE: Dict[str, dict] = {}
_ADMIN_ACCESS_CACHE_TS: Dict[str, float] = {}

SENSITIVE_LOG_KEYS = {
    "token", "access_token", "channel_access_token", "authorization", "secret",
    "signature", "x_line_signature", "reply_token", "replyToken", "raw_body",
    "body", "message", "text", "raw_text", "normalized_text", "user_id", "line_user_id",
}

def stable_hash(value: str, length: int = 12) -> str:
    raw = str(value or "")
    if not raw:
        return "empty"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:length]

def resolve_tenant_id_from_event(event: dict) -> str:
    try:
        header_tenant = safe_str(request.headers.get(TENANT_HEADER_NAME))
    except Exception:
        header_tenant = ""
    if header_tenant:
        return header_tenant
    destination = safe_str((event or {}).get("destination"))
    if destination:
        return f"line_destination:{destination}"
    source = (event or {}).get("source") or {}
    group_id = safe_str(source.get("groupId"))
    room_id = safe_str(source.get("roomId"))
    if group_id:
        return f"line_group:{group_id}"
    if room_id:
        return f"line_room:{room_id}"
    return DEFAULT_TENANT_ID

def set_current_tenant_id_from_event(event: dict, trace_id: str) -> str:
    tenant_id = resolve_tenant_id_from_event(event)
    try:
        g.dt79_tenant_id = tenant_id
    except Exception:
        pass
    logger.info(f"[{trace_id}] TENANT_CONTEXT_SET tenant_hash={stable_hash(tenant_id)}")
    return tenant_id

def get_current_tenant_id() -> str:
    try:
        tenant_id = safe_str(getattr(g, "dt79_tenant_id", ""))
        if tenant_id:
            return tenant_id
    except Exception:
        pass
    return DEFAULT_TENANT_ID

def tenant_scope_key(raw_key: str) -> str:
    raw = safe_str(raw_key)
    if not raw:
        return ""
    tenant_id = get_current_tenant_id()
    return f"{tenant_id}::{raw}"

def user_ref(user_id: str) -> str:
    return stable_hash(tenant_scope_key(user_id) or user_id)

def event_ref(event_key: str) -> str:
    return stable_hash(event_key)

def message_fingerprint(text: str) -> str:
    cleaned = sanitize_incoming_text(text)
    return f"len={len(cleaned)} sha={stable_hash(cleaned)}"

def redact_message_for_storage(text: str) -> str:
    return f"[REDACTED_MESSAGE {message_fingerprint(text)}]"

@app.route("/", methods=["GET", "HEAD"])
def root_health():
    return "OK", 200
@app.route("/health", methods=["GET"])
def health():
    return jsonify({
        "ok": True,
        "app_version": APP_VERSION,
    }), 200
def safe_str(v) -> str:
    return str(v).strip() if v else ""
def sanitize_incoming_text(text: str) -> str:
    raw = safe_str(text)
    if not raw:
        return ""
    normalized = unicodedata.normalize("NFKC", raw)
    normalized = re.sub(r"[\u200B-\u200F\u2060\uFEFF]", "", normalized)
    normalized = "".join(ch for ch in normalized if ch == "\n" or unicodedata.category(ch)[0] != "C")
    normalized = re.sub(r"[ \t\r\f\v]+", " ", normalized)
    normalized = re.sub(r"[¥\\]+$", "", normalized).strip()
    return normalized
def is_supported_flow(value: str) -> bool:
    normalized = safe_str(value)
    return normalized in {FLOW_WORKER, FLOW_ADS}
def is_persistent_flow_expired(updated_at_value: str) -> bool:
    updated_at_dt = parse_iso_datetime(updated_at_value)
    if not updated_at_dt:
        return True
    age_seconds = int((now_tw_dt() - updated_at_dt).total_seconds())
    return age_seconds > PERSISTENT_FLOW_TTL_SECONDS
LINE_CHANNEL_ACCESS_TOKEN = os.getenv("LINE_CHANNEL_ACCESS_TOKEN", "").strip()
LINE_CHANNEL_SECRET = os.getenv("LINE_CHANNEL_SECRET", "").strip()
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY", "").strip()
LINE_RICH_MENU_ID_VI = os.getenv("LINE_RICH_MENU_ID_VI", "").strip()
LINE_RICH_MENU_ID_ID = os.getenv("LINE_RICH_MENU_ID_ID", "").strip()
LINE_RICH_MENU_ID_TH = os.getenv("LINE_RICH_MENU_ID_TH", "").strip()
LINE_RICH_MENU_ID_ZH = os.getenv("LINE_RICH_MENU_ID_ZH", "").strip()
GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
PHASE1_SPREADSHEET_NAME = os.getenv("PHASE1_SPREADSHEET_NAME", "DT79_PHASE1_WORKER_CASES_V1").strip()
INTERNAL_SYNC_TOKEN = os.getenv("INTERNAL_SYNC_TOKEN", "").strip()
USER_STATE_SHEET_NAME = "user_state"
ADMIN_IDS = os.getenv("ADMIN_IDS", "").strip()
ADMIN_LIST = [x.strip() for x in ADMIN_IDS.split(",") if x.strip()]
ALL_COOLDOWN_SECONDS = int(os.getenv("ALL_COOLDOWN_SECONDS", "15").strip() or "15")
MAX_ALL_CHARS = int(os.getenv("MAX_ALL_CHARS", "500").strip() or "500")
RUNTIME_STATE_TTL_SECONDS = int(os.getenv("RUNTIME_STATE_TTL_SECONDS", "1800").strip() or "1800")
RUNTIME_STATE_MAX_KEYS = int(os.getenv("RUNTIME_STATE_MAX_KEYS", "5000").strip() or "5000")
PERSISTENT_FLOW_TTL_SECONDS = int(os.getenv("PERSISTENT_FLOW_TTL_SECONDS", "600").strip() or "600")
DEFAULT_LANGUAGE_GROUP = os.getenv("DEFAULT_LANGUAGE_GROUP", "vi").strip().lower() or "vi"
USER_LANGUAGE_MAP_JSON = os.getenv("USER_LANGUAGE_MAP_JSON", "").strip()
APP_VERSION = "PHASE1_RUNTIME_STATE_SAFE__RESTART_SAFE_DEDUP_SHEET_V46__WRITEBACK_STATUS_BLOCKED_BY_GUARD_FIX__CLEANUP_TEST_ROWS_V1__TRANSLATION_COMMAND_LAYER_V1__PERF_GUARDRAILS_V1__SIM_FASTPATH_V1__ROUTING_MASTER_CACHE_V1__EVENT_STATE_FAST_FINALIZE_V1__LOCATION_CANDIDATE_GUARD_V1__LOCATION_MASTER_CACHE_V1__SECURITY_TENANT_GUARD_V1"
TW_TZ = timezone(timedelta(hours=8))
LOCKED_TARGET_LANG = "zh-TW"
CONNECT_TIMEOUT_SECONDS = int(os.getenv("CONNECT_TIMEOUT_SECONDS", "3").strip() or "3")
READ_TIMEOUT_SECONDS = int(os.getenv("READ_TIMEOUT_SECONDS", "8").strip() or "8")
OUTBOUND_TIMEOUT = (CONNECT_TIMEOUT_SECONDS, READ_TIMEOUT_SECONDS)
FALLBACK_REPLY_TEXT = "Hệ thống bận, thử lại sau."
LINE_TEXT_HARD_LIMIT = 5000
RATE_LIMIT_STORE_MAX_KEYS = 5000
ERROR_BODY_LOG_LIMIT = 800
USER_RATE_LIMIT_WINDOW_SECONDS = int(os.getenv("USER_RATE_LIMIT_WINDOW_SECONDS", "10").strip() or "10")
USER_RATE_LIMIT_MAX_EVENTS = int(os.getenv("USER_RATE_LIMIT_MAX_EVENTS", "5").strip() or "5")
USER_RATE_LIMIT_REPLY_TEXT = os.getenv(
    "USER_RATE_LIMIT_REPLY_TEXT",
    "Hệ thống đang nhận quá nhiều tin nhắn. Vui lòng thử lại sau ít phút."
).strip()
GSHEET_CIRCUIT_FAILURE_THRESHOLD = int(os.getenv("GSHEET_CIRCUIT_FAILURE_THRESHOLD", "3").strip() or "3")
GSHEET_CIRCUIT_RESET_SECONDS = int(os.getenv("GSHEET_CIRCUIT_RESET_SECONDS", "60").strip() or "60")
GSHEET_MAINTENANCE_REPLY_TEXT = os.getenv(
    "GSHEET_MAINTENANCE_REPLY_TEXT",
    "Hệ thống dữ liệu đang bảo trì ngắn. Vui lòng thử lại sau ít phút."
).strip()
ASYNC_LOG_ENABLED = os.getenv("ASYNC_LOG_ENABLED", "1").strip().lower() not in {"0", "false", "no"}
ASYNC_LOG_QUEUE_MAX = int(os.getenv("ASYNC_LOG_QUEUE_MAX", "1000").strip() or "1000")
ASYNC_LOG_WORKER_TIMEOUT_SECONDS = float(os.getenv("ASYNC_LOG_WORKER_TIMEOUT_SECONDS", "1").strip() or "1")
SIM_FASTPATH_VARIANT_CACHE_TTL_SECONDS = int(os.getenv("SIM_FASTPATH_VARIANT_CACHE_TTL_SECONDS", "900").strip() or "900")
ROUTING_MASTER_CACHE_TTL_SECONDS = int(os.getenv("ROUTING_MASTER_CACHE_TTL_SECONDS", "900").strip() or "900")
LOCATION_MASTER_CACHE_TTL_SECONDS = int(os.getenv("LOCATION_MASTER_CACHE_TTL_SECONDS", "900").strip() or "900")
EVENT_STATE_FAST_FINALIZE_ENABLED = os.getenv("EVENT_STATE_FAST_FINALIZE_ENABLED", "1").strip().lower() not in {"0", "false", "no"}
SIM_FASTPATH_ENABLED = os.getenv("SIM_FASTPATH_ENABLED", "1").strip().lower() not in {"0", "false", "no"}
ASYNC_LOG_LEVEL_CRITICAL = "CRITICAL"
ASYNC_LOG_LEVEL_AUDIT = "AUDIT"
ASYNC_LOG_LEVEL_DEBUG = "DEBUG"
PROCESSED_EVENT_TTL_SECONDS = int(os.getenv("PROCESSED_EVENT_TTL_SECONDS", "21600").strip() or "21600")
PROCESSED_EVENT_MAX_KEYS = int(os.getenv("PROCESSED_EVENT_MAX_KEYS", "10000").strip() or "10000")
PROCESSED_EVENT_SHEET_NAME = os.getenv("PROCESSED_EVENT_SHEET_NAME", "processed_event_state").strip() or "processed_event_state"
LINE_REPLY_API_URL = "https://api.line.me/v2/bot/message/reply"
GOOGLE_TRANSLATE_API_URL = "https://translation.googleapis.com/language/translate/v2"
WORKER_ENTRY_COMMAND = "/worker"
ADS_ENTRY_COMMAND = "/ads"
RESET_ENTRY_COMMAND = "/reset"
EXIT_ENTRY_COMMAND = "/exit"
STATUS_ENTRY_COMMAND = "/status"
HELP_ENTRY_COMMAND = "/help"
LANG_COMMAND_PREFIX = "/lang"
SUPPORTED_LANGUAGE_GROUPS = {"vi", "id", "th", "zh"}
RICH_MENU_ID_BY_LANGUAGE = {
    "vi": LINE_RICH_MENU_ID_VI,
    "id": LINE_RICH_MENU_ID_ID,
    "th": LINE_RICH_MENU_ID_TH,
    "zh": LINE_RICH_MENU_ID_ZH,
}
ADS_CATALOG_V2_SHEET_NAME = "ADS_CATALOG_V2"
OWNER_ADS_INPUT_SHEET_NAME = "OWNER_ADS_INPUT"
OWNER_SETTINGS_SHEET_NAME = "OWNER_SETTINGS"
ADS_LEADS_SHEET_NAME = "ADS_LEADS"
ADS_CLICK_LOG_SHEET_NAME = "ads_click_log"
ADS_PHONE_LEADS_SHEET_NAME = "ads_phone_leads"
TENANT_REGISTRY_SHEET_NAME = "TENANT_REGISTRY"
SYSTEM_META_SHEET_NAME = "SYSTEM_META"
BOT_CONFIG_SHEET_NAME = "BOT_CONFIG"
INTENT_MASTER_SHEET_NAME = "INTENT_MASTER"
SERVICE_MASTER_SHEET_NAME = "SERVICE_MASTER"
PROVIDER_MASTER_SHEET_NAME = "PROVIDER_MASTER"
LOCATION_ALIAS_MASTER_SHEET_NAME = "LOCATION_ALIAS_MASTER"
SERVICE_VARIANT_MASTER_SHEET_NAME = "SERVICE_VARIANT_MASTER"
ROUTING_LOG_SHEET_NAME = "ROUTING_LOG"
ROUTING_LOG_HEADERS = ["timestamp", "tenant_id", "user_ref", "intent", "service_id", "location", "message_fingerprint"]
ROUTING_MISS_HARVEST_SHEET_NAME = "ROUTING_MISS_HARVEST"
ROUTING_MISS_HARVEST_HEADERS = ["timestamp", "trace_id", "tenant_id", "user_ref", "raw_text_fingerprint", "normalized_text_fingerprint", "intent_guess", "location_token_guess", "candidate_aliases", "miss_type", "recommended_fix", "status", "reviewer_notes"]
ROUTING_SLOWPATH_QUEUE_SHEET_NAME = "ROUTING_SLOWPATH_QUEUE"
ROUTING_SLOWPATH_QUEUE_HEADERS = ["timestamp", "trace_id", "tenant_id", "user_ref", "raw_text_fingerprint", "normalized_text_fingerprint", "detected_intent", "candidate_locations", "candidate_location_ids", "reason_code", "confidence", "action_needed", "status", "reviewer_notes"]
ROUTING_SHADOW_SUGGESTIONS_SHEET_NAME = "ROUTING_SHADOW_SUGGESTIONS"
ROUTING_SHADOW_SUGGESTIONS_HEADERS = ["timestamp", "trace_id", "tenant_id", "user_ref", "raw_text_fingerprint", "normalized_text_fingerprint", "intent_name", "location_token_guess", "suggested_alias", "suggested_location_id", "suggested_region_key", "suggestion_source", "suggestion_reason", "confidence", "review_status", "reviewer_notes", "second_candidate_alias", "second_candidate_location_id", "score_gap", "decision_context", "writeback_status", "writeback_at", "writeback_target", "writeback_notes"]
ROUTING_ADMIN_AUDIT_LOG_SHEET_NAME = "ROUTING_ADMIN_AUDIT_LOG"
ROUTING_ADMIN_AUDIT_LOG_HEADERS = ["timestamp", "trace_id", "batch_id", "source_sheet", "source_row", "action", "raw_text", "normalized_text", "alias", "location_id", "region_key", "review_status", "writeback_status", "sanitizer_result", "sanitizer_reason", "target_sheet", "target_row", "operator", "notes", "alias_source", "raw_text_risk_result", "raw_text_risk_type", "raw_text_risk_source", "guard_result", "guard_reason"]
ROUTING_SPREADSHEET_NAME = os.getenv("ROUTING_SPREADSHEET_NAME", "DT79_BOT_TRANSLATOR_DATABASE").strip() or "DT79_BOT_TRANSLATOR_DATABASE"
ROUTING_FALLBACK_LOCATION = os.getenv("ROUTING_FALLBACK_LOCATION", "TW_ALL").strip() or "TW_ALL"
ROUTING_REPLY_PREFIX_BY_LANGUAGE = {
    "vi": "Liên hệ LINE",
    "id": "Kontak LINE",
    "th": "ติดต่อ LINE",
    "zh": "LINE 聯絡",
}
ROUTING_LABELS_BY_LANGUAGE = {
    "vi": {"location": "Khu vực phục vụ", "service": "Dịch vụ", "price": "Giá"},
    "id": {"location": "Area layanan", "service": "Layanan", "price": "Harga"},
    "th": {"location": "พื้นที่ให้บริการ", "service": "บริการ", "price": "ราคา"},
    "zh": {"location": "服務區域", "service": "服務", "price": "價格"},
}
LOCATION_ALIAS_MAP = {
    "台中": ["台中", "đài trung", "dai trung", "taichung", "taizhong", "xitun", "taiping"],
    "台南": ["台南", "đài nam", "dai nam", "tainan"],
    "高雄": ["高雄", "cao hùng", "cao hung", "kaohsiung"],
    "台北": ["台北", "đài bắc", "dai bac", "taipei"],
    "桃園": ["桃園", "đào viên", "dao vien", "taoyuan", "zhongli"],
}
LOCATION_DISPLAY_MAP = {
    "TW_ALL": "Toàn Đài Loan",
    "台中": "Đài Trung",
    "台南": "Đài Nam",
    "高雄": "Cao Hùng",
    "台北": "Đài Bắc",
    "桃園": "Đào Viên",
    "新北": "Tân Bắc",
    "屏東": "Bình Đông",
    "嘉義": "Gia Nghĩa",
    "雲林": "Vân Lâm",
}
ADS_LIST_LIMIT = int(os.getenv("ADS_LIST_LIMIT", "6").strip() or "6")
ADS_CACHE_TTL_SECONDS = int(os.getenv("ADS_CACHE_TTL_SECONDS", "30").strip() or "30")
ADS_VIEW_TTL_SECONDS = int(os.getenv("ADS_VIEW_TTL_SECONDS", "300").strip() or "300")
ADS_DETAIL_TTL_SECONDS = int(os.getenv("ADS_DETAIL_TTL_SECONDS", "300").strip() or "300")
WORKSPACE_VALIDATION_CACHE_TTL_SECONDS = int(os.getenv("WORKSPACE_VALIDATION_CACHE_TTL_SECONDS", "30").strip() or "30")
ADS_TYPE_JOB_OPENING = "job_opening"
ADS_TYPE_SERVICE_OFFER = "service_offer"
VISIBILITY_SAME_LANGUAGE_ONLY = "same_language_only"
VISIBILITY_CROSS_LANGUAGE_ALLOWED = "cross_language_allowed"
VISIBILITY_VIEWER_LOCALIZED = "viewer_localized"
ACTIVE_AD_STATUSES = {"active"}
SUPPORTED_AD_TYPES = {ADS_TYPE_JOB_OPENING, ADS_TYPE_SERVICE_OFFER}
SUPPORTED_VISIBILITY_POLICIES = {
    VISIBILITY_SAME_LANGUAGE_ONLY,
    VISIBILITY_CROSS_LANGUAGE_ALLOWED,
    VISIBILITY_VIEWER_LOCALIZED,
}
FLOW_WORKER = "worker"
FLOW_ADS = "ads"
def now_tw_iso() -> str:
    return datetime.now(TW_TZ).isoformat()
def now_tw_dt() -> datetime:
    return datetime.now(TW_TZ)
def make_trace_id() -> str:
    ts_compact = datetime.now(TW_TZ).strftime("%y%m%d%H%M%S")
    version_hash = hashlib.sha1(APP_VERSION.encode("utf-8")).hexdigest()[:6]
    return f"trc_{version_hash}_{ts_compact}_{uuid.uuid4().hex[:6]}"
def get_now_ts() -> int:
    return int(time.time())
def ms_since(start_perf: float) -> int:
    return int((time.perf_counter() - start_perf) * 1000)
def normalize_language_group(value: str) -> str:
    lang = safe_str(value).lower()
    if lang in SUPPORTED_LANGUAGE_GROUPS:
        return lang
    return DEFAULT_LANGUAGE_GROUP if DEFAULT_LANGUAGE_GROUP in SUPPORTED_LANGUAGE_GROUPS else "vi"
def parse_iso_datetime(value: str) -> Optional[datetime]:
    raw = safe_str(value)
    if not raw:
        return None
    normalized = raw.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(normalized)
    except Exception:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=TW_TZ)
    return dt.astimezone(TW_TZ)
def is_ad_active_in_time_window(start_at: str, end_at: str) -> bool:
    now_dt = now_tw_dt()
    start_dt = parse_iso_datetime(start_at)
    end_dt = parse_iso_datetime(end_at)
    if start_dt and now_dt < start_dt:
        return False
    if end_dt and now_dt > end_dt:
        return False
    return True
_GSPREAD_CLIENT = None
GSHEET_SPREADSHEET_CACHE_TTL_SECONDS = int(os.getenv("GSHEET_SPREADSHEET_CACHE_TTL_SECONDS", "300").strip() or "300")
GSHEET_VALUES_CACHE_TTL_SECONDS = int(os.getenv("GSHEET_VALUES_CACHE_TTL_SECONDS", "45").strip() or "45")
_SPREADSHEET_SHARED_CACHE = {"spreadsheet": None, "loaded_at_ts": 0.0}
_WORKSHEET_OBJECT_SHARED_CACHE: Dict[str, object] = {}
_WORKSHEET_VALUES_SHARED_CACHE: Dict[str, dict] = {}
_WORKSHEET_RECORDS_SHARED_CACHE: Dict[str, dict] = {}
_ROUTING_SPREADSHEET_SHARED_CACHE = {"spreadsheet": None, "loaded_at_ts": 0.0}
_ROUTING_WORKSHEET_OBJECT_SHARED_CACHE: Dict[str, object] = {}
_ROUTING_CONFIG_SHARED_CACHE = {"config_map": None, "loaded_at_ts": 0.0}
_ROUTING_MASTER_RECORDS_SHARED_CACHE: Dict[str, dict] = {}
_ROUTING_MASTER_CACHE_LOCK = threading.Lock()
_ROUTING_LOG_WORKSHEET_READY_CACHE = {"verified": False, "loaded_at_ts": 0.0}
_SIM_FASTPATH_VARIANT_ROWS_CACHE = {"rows": [], "loaded_at_ts": 0.0, "sheet_name": ""}
_ROUTING_MISS_HARVEST_WORKSHEET_READY_CACHE = {"verified": False, "loaded_at_ts": 0.0}
_ROUTING_SLOWPATH_WORKSHEET_READY_CACHE = {"verified": False, "loaded_at_ts": 0.0}
_ROUTING_SHADOW_SUGGESTIONS_WORKSHEET_READY_CACHE = {"verified": False, "loaded_at_ts": 0.0}
_ROUTING_ADMIN_AUDIT_LOG_WORKSHEET_READY_CACHE = {"verified": False, "loaded_at_ts": 0.0}
def _now_ts() -> float:
    return time.time()
def _cache_is_fresh(loaded_at_ts: float, ttl_seconds: int) -> bool:
    return bool(loaded_at_ts) and (_now_ts() - loaded_at_ts) < ttl_seconds
def _is_gsheet_quota_error(exc: Exception) -> bool:
    text = safe_str(exc)
    return "429" in text or "Quota exceeded" in text or "Read requests per minute per user" in text

class GSheetCircuitOpenError(RuntimeError):
    pass

_GSHEET_CIRCUIT_STATE = {
    "open_until_ts": 0.0,
    "failure_count": 0,
    "last_error": "",
}
_GSHEET_CIRCUIT_LOCK = threading.Lock()
_RATE_LIMIT_STATE: Dict[str, deque] = {}
_RATE_LIMIT_LOCK = threading.Lock()
_ASYNC_LOG_QUEUE = Queue(maxsize=ASYNC_LOG_QUEUE_MAX)
_ASYNC_LOG_WORKER_STARTED = False
_ASYNC_LOG_WORKER_LOCK = threading.Lock()

def gsheet_circuit_is_open() -> bool:
    with _GSHEET_CIRCUIT_LOCK:
        open_until_ts = float(_GSHEET_CIRCUIT_STATE.get("open_until_ts", 0.0) or 0.0)
        if open_until_ts and _now_ts() < open_until_ts:
            return True
        if open_until_ts and _now_ts() >= open_until_ts:
            _GSHEET_CIRCUIT_STATE["open_until_ts"] = 0.0
            _GSHEET_CIRCUIT_STATE["failure_count"] = 0
            _GSHEET_CIRCUIT_STATE["last_error"] = ""
        return False

def record_gsheet_success(trace_id: str) -> None:
    with _GSHEET_CIRCUIT_LOCK:
        if int(_GSHEET_CIRCUIT_STATE.get("failure_count", 0) or 0):
            logger.info(f"[{trace_id}] GSHEET_CIRCUIT_RECOVERED")
        _GSHEET_CIRCUIT_STATE["failure_count"] = 0
        _GSHEET_CIRCUIT_STATE["last_error"] = ""

def record_gsheet_failure(exc: Exception, trace_id: str, operation: str) -> None:
    if not _is_gsheet_quota_error(exc):
        return
    with _GSHEET_CIRCUIT_LOCK:
        failure_count = int(_GSHEET_CIRCUIT_STATE.get("failure_count", 0) or 0) + 1
        _GSHEET_CIRCUIT_STATE["failure_count"] = failure_count
        _GSHEET_CIRCUIT_STATE["last_error"] = f"{type(exc).__name__}:{safe_str(exc)[:200]}"
        logger.error(
            f"[{trace_id}] GSHEET_QUOTA_FAILURE operation={operation} "
            f"failure_count={failure_count} threshold={GSHEET_CIRCUIT_FAILURE_THRESHOLD}"
        )
        if failure_count >= GSHEET_CIRCUIT_FAILURE_THRESHOLD:
            _GSHEET_CIRCUIT_STATE["open_until_ts"] = _now_ts() + GSHEET_CIRCUIT_RESET_SECONDS
            logger.error(
                f"[{trace_id}] GSHEET_CIRCUIT_OPEN operation={operation} "
                f"reset_seconds={GSHEET_CIRCUIT_RESET_SECONDS}"
            )

def gsheet_guarded_call(trace_id: str, operation: str, fn, *args, **kwargs):
    if gsheet_circuit_is_open():
        logger.error(f"[{trace_id}] GSHEET_CIRCUIT_BLOCK operation={operation}")
        raise GSheetCircuitOpenError("gsheet_circuit_open")
    try:
        result = fn(*args, **kwargs)
        record_gsheet_success(trace_id)
        return result
    except GSheetCircuitOpenError:
        raise
    except Exception as exc:
        record_gsheet_failure(exc, trace_id, operation)
        raise

def append_row_guarded(ws, trace_id: str, worksheet_name: str, row_values: list, value_input_option: str = "USER_ENTERED"):
    return gsheet_guarded_call(
        trace_id,
        f"worksheet.append.{safe_str(worksheet_name) or 'unknown'}",
        ws.append_row,
        row_values,
        value_input_option=value_input_option,
    )

def check_user_rate_limit(user_id: str, trace_id: str) -> bool:
    key = tenant_scope_key(user_id) or "anonymous"
    now = _now_ts()
    window_start = now - USER_RATE_LIMIT_WINDOW_SECONDS
    with _RATE_LIMIT_LOCK:
        dq = _RATE_LIMIT_STATE.get(key)
        if dq is None:
            dq = deque()
            _RATE_LIMIT_STATE[key] = dq
        while dq and dq[0] < window_start:
            dq.popleft()
        if len(dq) >= USER_RATE_LIMIT_MAX_EVENTS:
            logger.warning(
                f"[{trace_id}] RATE_LIMIT_BLOCKED user_ref={user_ref(user_id)} "
                f"window_seconds={USER_RATE_LIMIT_WINDOW_SECONDS} max_events={USER_RATE_LIMIT_MAX_EVENTS}"
            )
            return False
        dq.append(now)
        if len(_RATE_LIMIT_STATE) > RATE_LIMIT_STORE_MAX_KEYS:
            oldest_keys = sorted(
                _RATE_LIMIT_STATE.keys(),
                key=lambda k: _RATE_LIMIT_STATE[k][0] if _RATE_LIMIT_STATE[k] else now,
            )[: max(1, RATE_LIMIT_STORE_MAX_KEYS // 10)]
            for old_key in oldest_keys:
                _RATE_LIMIT_STATE.pop(old_key, None)
        return True

def _async_log_worker_loop() -> None:
    while True:
        try:
            task = _ASYNC_LOG_QUEUE.get(timeout=ASYNC_LOG_WORKER_TIMEOUT_SECONDS)
        except Empty:
            continue
        if task is None:
            _ASYNC_LOG_QUEUE.task_done()
            break
        level, trace_id, task_name, fn, args, kwargs = task
        try:
            with app.app_context():
                fn(*args, **kwargs)
            logger.info(f"[{trace_id}] ASYNC_LOG_DONE level={level} task={task_name}")
        except GSheetCircuitOpenError as exc:
            logger.error(f"[{trace_id}] ASYNC_LOG_CIRCUIT_BLOCKED level={level} task={task_name} exception={type(exc).__name__}:{exc}")
        except Exception as exc:
            logger.exception(f"[{trace_id}] ASYNC_LOG_FAILED level={level} task={task_name} exception={type(exc).__name__}:{exc}")
        finally:
            _ASYNC_LOG_QUEUE.task_done()

def start_async_log_worker() -> None:
    global _ASYNC_LOG_WORKER_STARTED
    if not ASYNC_LOG_ENABLED:
        return
    with _ASYNC_LOG_WORKER_LOCK:
        if _ASYNC_LOG_WORKER_STARTED:
            return
        worker = threading.Thread(target=_async_log_worker_loop, name="dt79-async-log-worker", daemon=True)
        worker.start()
        _ASYNC_LOG_WORKER_STARTED = True
        logger.info("ASYNC_LOG_WORKER_STARTED")

def enqueue_async_log(level: str, trace_id: str, task_name: str, fn, *args, **kwargs) -> bool:
    if not ASYNC_LOG_ENABLED or level == ASYNC_LOG_LEVEL_CRITICAL:
        try:
            return bool(fn(*args, **kwargs))
        except Exception as exc:
            logger.exception(f"[{trace_id}] SYNC_LOG_FAILED level={level} task={task_name} exception={type(exc).__name__}:{exc}")
            return False
    start_async_log_worker()
    try:
        _ASYNC_LOG_QUEUE.put_nowait((level, trace_id, task_name, fn, args, kwargs))
        logger.info(f"[{trace_id}] ASYNC_LOG_ENQUEUED level={level} task={task_name} queue_size={_ASYNC_LOG_QUEUE.qsize()}")
        return True
    except Full:
        logger.error(f"[{trace_id}] ASYNC_LOG_QUEUE_FULL level={level} task={task_name}")
        return False
def _values_to_records(values: List[List[str]]) -> List[dict]:
    if not values:
        return []
    headers = [safe_str(v) for v in (values[0] if values else [])]
    if not headers:
        return []
    records: List[dict] = []
    for row in values[1:]:
        if not any(safe_str(cell) for cell in row):
            continue
        item = {}
        for idx, header in enumerate(headers):
            if not header:
                continue
            item[header] = safe_str(row[idx]) if idx < len(row) else ""
        records.append(item)
    return records
def _invalidate_worksheet_caches(worksheet_name: str) -> None:
    values_cache = getattr(g, "_dt79_values_cache", None)
    if isinstance(values_cache, dict):
        values_cache.pop(worksheet_name, None)
    records_cache = getattr(g, "_dt79_records_cache", None)
    if isinstance(records_cache, dict):
        records_cache.pop(worksheet_name, None)
    _WORKSHEET_VALUES_SHARED_CACHE.pop(worksheet_name, None)
    _WORKSHEET_RECORDS_SHARED_CACHE.pop(worksheet_name, None)
def get_google_credentials(trace_id: str):
    if not GOOGLE_SERVICE_ACCOUNT_JSON:
        logger.error(f"[{trace_id}] GSHEET_CREDENTIALS_MISSING")
        return None
    try:
        raw = GOOGLE_SERVICE_ACCOUNT_JSON.strip()
        if raw.startswith("{"):
            info = json.loads(raw)
        else:
            decoded = base64.b64decode(raw).decode("utf-8")
            info = json.loads(decoded)
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive.readonly",
        ]
        return Credentials.from_service_account_info(info, scopes=scopes)
    except Exception as e:
        logger.exception(f"[{trace_id}] GSHEET_CREDENTIALS_INVALID exception={type(e).__name__}:{e}")
        return None
def get_gspread_client(trace_id: str):
    global _GSPREAD_CLIENT
    if _GSPREAD_CLIENT is not None:
        return _GSPREAD_CLIENT
    credentials = get_google_credentials(trace_id)
    if not credentials:
        return None
    try:
        _GSPREAD_CLIENT = gspread.authorize(credentials)
        logger.info(f"[{trace_id}] GSHEET_CLIENT_READY")
        return _GSPREAD_CLIENT
    except Exception as e:
        logger.exception(f"[{trace_id}] GSHEET_CLIENT_INIT_FAILED exception={type(e).__name__}:{e}")
        return None
def open_spreadsheet(trace_id: str):
    cached = getattr(g, "_dt79_spreadsheet", None)
    if cached is not None:
        logger.info(f"[{trace_id}] SPREADSHEET_CACHE_HIT name={PHASE1_SPREADSHEET_NAME}")
        return cached
    shared = _SPREADSHEET_SHARED_CACHE.get("spreadsheet")
    loaded_at_ts = float(_SPREADSHEET_SHARED_CACHE.get("loaded_at_ts", 0.0) or 0.0)
    if shared is not None and _cache_is_fresh(loaded_at_ts, GSHEET_SPREADSHEET_CACHE_TTL_SECONDS):
        g._dt79_spreadsheet = shared
        logger.info(f"[{trace_id}] SPREADSHEET_SHARED_CACHE_HIT name={PHASE1_SPREADSHEET_NAME}")
        return shared
    client = get_gspread_client(trace_id)
    if not client:
        return shared
    try:
        spreadsheet = gsheet_guarded_call(trace_id, "client.open.phase1", client.open, PHASE1_SPREADSHEET_NAME)
        g._dt79_spreadsheet = spreadsheet
        _SPREADSHEET_SHARED_CACHE["spreadsheet"] = spreadsheet
        _SPREADSHEET_SHARED_CACHE["loaded_at_ts"] = _now_ts()
        logger.info(f"[{trace_id}] SPREADSHEET_READY name={PHASE1_SPREADSHEET_NAME}")
        return spreadsheet
    except Exception as e:
        logger.exception(f"[{trace_id}] SPREADSHEET_OPEN_FAILED exception={type(e).__name__}:{e}")
        if shared is not None and _is_gsheet_quota_error(e):
            g._dt79_spreadsheet = shared
            logger.info(f"[{trace_id}] SPREADSHEET_STALE_CACHE_FALLBACK name={PHASE1_SPREADSHEET_NAME}")
            return shared
        return None
def normalize_header_key(value: str) -> str:
    return safe_str(value).strip().lower()
def build_header_index_map(headers: List[str]) -> Dict[str, int]:
    result = {}
    for idx, header in enumerate(headers):
        key = normalize_header_key(header)
        if key and key not in result:
            result[key] = idx
    return result
def get_worksheet_by_name(trace_id: str, worksheet_name: str):
    worksheet_cache = getattr(g, "_dt79_worksheets", None)
    if worksheet_cache is None:
        worksheet_cache = {}
        g._dt79_worksheets = worksheet_cache
    if worksheet_name in worksheet_cache:
        logger.info(f"[{trace_id}] WORKSHEET_CACHE_HIT worksheet_name={worksheet_name}")
        return worksheet_cache[worksheet_name]
    if worksheet_name in _WORKSHEET_OBJECT_SHARED_CACHE:
        ws = _WORKSHEET_OBJECT_SHARED_CACHE[worksheet_name]
        worksheet_cache[worksheet_name] = ws
        logger.info(f"[{trace_id}] WORKSHEET_SHARED_CACHE_HIT worksheet_name={worksheet_name}")
        return ws
    spreadsheet = open_spreadsheet(trace_id)
    if not spreadsheet:
        return None
    try:
        ws = gsheet_guarded_call(trace_id, f"worksheet.open.{worksheet_name}", spreadsheet.worksheet, worksheet_name)
        worksheet_cache[worksheet_name] = ws
        _WORKSHEET_OBJECT_SHARED_CACHE[worksheet_name] = ws
        logger.info(f"[{trace_id}] WORKSHEET_READY worksheet_name={worksheet_name}")
        return ws
    except gspread.WorksheetNotFound:
        logger.error(f"[{trace_id}] WORKSHEET_NOT_FOUND worksheet_name={worksheet_name}")
        return None
    except Exception as e:
        logger.exception(f"[{trace_id}] WORKSHEET_OPEN_FAILED worksheet_name={worksheet_name} exception={type(e).__name__}:{e}")
        ws = _WORKSHEET_OBJECT_SHARED_CACHE.get(worksheet_name)
        if ws is not None and _is_gsheet_quota_error(e):
            worksheet_cache[worksheet_name] = ws
            logger.info(f"[{trace_id}] WORKSHEET_STALE_CACHE_FALLBACK worksheet_name={worksheet_name}")
            return ws
        return None
def get_all_values_safe(ws, trace_id: str, worksheet_name: str) -> List[List[str]]:
    values_cache = getattr(g, "_dt79_values_cache", None)
    if values_cache is None:
        values_cache = {}
        g._dt79_values_cache = values_cache
    if worksheet_name in values_cache:
        values = values_cache[worksheet_name]
        logger.info(f"[{trace_id}] WORKSHEET_VALUES_CACHE_HIT worksheet_name={worksheet_name} row_count={len(values)}")
        return values
    shared_entry = _WORKSHEET_VALUES_SHARED_CACHE.get(worksheet_name)
    if shared_entry and _cache_is_fresh(float(shared_entry.get("loaded_at_ts", 0.0) or 0.0), GSHEET_VALUES_CACHE_TTL_SECONDS):
        values = shared_entry.get("values", []) or []
        values_cache[worksheet_name] = values
        logger.info(f"[{trace_id}] WORKSHEET_VALUES_SHARED_CACHE_HIT worksheet_name={worksheet_name} row_count={len(values)}")
        return values
    try:
        values = gsheet_guarded_call(trace_id, f"worksheet.read.{worksheet_name}", ws.get_all_values)
        values_cache[worksheet_name] = values
        _WORKSHEET_VALUES_SHARED_CACHE[worksheet_name] = {"values": values, "loaded_at_ts": _now_ts()}
        _WORKSHEET_RECORDS_SHARED_CACHE[worksheet_name] = {"records": _values_to_records(values), "loaded_at_ts": _now_ts()}
        logger.info(f"[{trace_id}] WORKSHEET_READ_OK worksheet_name={worksheet_name} row_count={len(values)}")
        return values
    except Exception as e:
        logger.exception(f"[{trace_id}] WORKSHEET_READ_FAILED worksheet_name={worksheet_name} exception={type(e).__name__}:{e}")
        if shared_entry:
            values = shared_entry.get("values", []) or []
            values_cache[worksheet_name] = values
            logger.info(f"[{trace_id}] WORKSHEET_VALUES_STALE_CACHE_FALLBACK worksheet_name={worksheet_name} row_count={len(values)}")
            return values
        return []
def get_records_safe(ws, trace_id: str, worksheet_name: str) -> List[dict]:
    records_cache = getattr(g, "_dt79_records_cache", None)
    if records_cache is None:
        records_cache = {}
        g._dt79_records_cache = records_cache
    if worksheet_name in records_cache:
        records = records_cache[worksheet_name]
        logger.info(f"[{trace_id}] WORKSHEET_RECORDS_CACHE_HIT worksheet_name={worksheet_name} count={len(records)}")
        return records
    shared_entry = _WORKSHEET_RECORDS_SHARED_CACHE.get(worksheet_name)
    if shared_entry and _cache_is_fresh(float(shared_entry.get("loaded_at_ts", 0.0) or 0.0), GSHEET_VALUES_CACHE_TTL_SECONDS):
        records = shared_entry.get("records", []) or []
        records_cache[worksheet_name] = records
        logger.info(f"[{trace_id}] WORKSHEET_RECORDS_SHARED_CACHE_HIT worksheet_name={worksheet_name} count={len(records)}")
        return records
    values = get_all_values_safe(ws, trace_id, worksheet_name)
    records = _values_to_records(values)
    records_cache[worksheet_name] = records
    _WORKSHEET_RECORDS_SHARED_CACHE[worksheet_name] = {"records": records, "loaded_at_ts": _now_ts()}
    logger.info(f"[{trace_id}] WORKSHEET_RECORDS_DERIVED_OK worksheet_name={worksheet_name} count={len(records)}")
    return records
def find_first_row_index_by_column_value(ws, column_name: str, expected_value: str, trace_id: str, worksheet_name: str) -> int:
    values = get_all_values_safe(ws, trace_id, worksheet_name)
    if not values:
        return 0
    headers = values[0]
    header_map = build_header_index_map(headers)
    target_idx = header_map.get(normalize_header_key(column_name))
    if target_idx is None:
        logger.error(f"[{trace_id}] FIND_ROW_COLUMN_MISSING worksheet_name={worksheet_name} column_name={column_name}")
        return 0
    expected = safe_str(expected_value)
    for row_idx, row in enumerate(values[1:], start=2):
        cell_value = safe_str(row[target_idx]) if target_idx < len(row) else ""
        if cell_value == expected:
            logger.info(f"[{trace_id}] FIND_ROW_OK worksheet_name={worksheet_name} column_name={column_name} expected_value={expected_value} row_index={row_idx}")
            return row_idx
    logger.info(f"[{trace_id}] FIND_ROW_NOT_FOUND worksheet_name={worksheet_name} column_name={column_name} expected_value={expected_value}")
    return 0
def update_row_fields_by_header(ws, row_index: int, field_values: Dict[str, str], trace_id: str, worksheet_name: str) -> bool:
    values = get_all_values_safe(ws, trace_id, worksheet_name)
    if not values:
        return False
    headers = values[0]
    header_map = build_header_index_map(headers)
    normalized_items = []
    for column_name, value in field_values.items():
        col_idx = header_map.get(normalize_header_key(column_name))
        if col_idx is None:
            logger.error(f"[{trace_id}] UPDATE_ROW_COLUMN_MISSING worksheet_name={worksheet_name} column_name={column_name}")
            return False
        normalized_items.append((col_idx, column_name, safe_str(value)))
    min_col_idx = min(col_idx for col_idx, _, _ in normalized_items)
    max_col_idx = max(col_idx for col_idx, _, _ in normalized_items)
    row_values = [""] * (max_col_idx - min_col_idx + 1)
    for col_idx, _, value in normalized_items:
        row_values[col_idx - min_col_idx] = value
    start_col_letter = chr(ord("A") + min_col_idx)
    end_col_letter = chr(ord("A") + max_col_idx)
    target_range = f"{start_col_letter}{row_index}:{end_col_letter}{row_index}"
    try:
        gsheet_guarded_call(trace_id, f"worksheet.update.{worksheet_name}", ws.update, target_range, [row_values], value_input_option="USER_ENTERED")
        _invalidate_worksheet_caches(worksheet_name)
        logger.info(
            f"[{trace_id}] UPDATE_ROW_FIELDS_OK worksheet_name={worksheet_name} row_index={row_index} "
            f"columns={json.dumps([name for _, name, _ in normalized_items], ensure_ascii=False)} "
            f"values={json.dumps({name: value for _, name, value in normalized_items}, ensure_ascii=False)}"
        )
        return True
    except Exception as e:
        logger.exception(f"[{trace_id}] UPDATE_ROW_FIELDS_FAILED worksheet_name={worksheet_name} row_index={row_index} exception={type(e).__name__}:{e}")
        return False
def open_routing_spreadsheet(trace_id: str):
    cached = getattr(g, "_dt79_routing_spreadsheet", None)
    if cached is not None:
        logger.info(f"[{trace_id}] ROUTING_SPREADSHEET_CACHE_HIT name={ROUTING_SPREADSHEET_NAME}")
        return cached
    shared = _ROUTING_SPREADSHEET_SHARED_CACHE.get("spreadsheet")
    loaded_at_ts = float(_ROUTING_SPREADSHEET_SHARED_CACHE.get("loaded_at_ts", 0.0) or 0.0)
    if shared is not None and _cache_is_fresh(loaded_at_ts, GSHEET_SPREADSHEET_CACHE_TTL_SECONDS):
        g._dt79_routing_spreadsheet = shared
        logger.info(f"[{trace_id}] ROUTING_SPREADSHEET_SHARED_CACHE_HIT name={ROUTING_SPREADSHEET_NAME}")
        return shared
    client = get_gspread_client(trace_id)
    if not client:
        return shared
    try:
        spreadsheet = gsheet_guarded_call(trace_id, "client.open.routing", client.open, ROUTING_SPREADSHEET_NAME)
        g._dt79_routing_spreadsheet = spreadsheet
        _ROUTING_SPREADSHEET_SHARED_CACHE["spreadsheet"] = spreadsheet
        _ROUTING_SPREADSHEET_SHARED_CACHE["loaded_at_ts"] = _now_ts()
        logger.info(f"[{trace_id}] ROUTING_SPREADSHEET_READY name={ROUTING_SPREADSHEET_NAME}")
        return spreadsheet
    except Exception as e:
        logger.exception(f"[{trace_id}] ROUTING_SPREADSHEET_OPEN_FAILED exception={type(e).__name__}:{e}")
        if shared is not None and _is_gsheet_quota_error(e):
            g._dt79_routing_spreadsheet = shared
            logger.info(f"[{trace_id}] ROUTING_SPREADSHEET_STALE_CACHE_FALLBACK name={ROUTING_SPREADSHEET_NAME}")
            return shared
        return None
def get_routing_worksheet_by_name(trace_id: str, worksheet_name: str):
    worksheet_cache = getattr(g, "_dt79_routing_worksheets", None)
    if worksheet_cache is None:
        worksheet_cache = {}
        g._dt79_routing_worksheets = worksheet_cache
    if worksheet_name in worksheet_cache:
        logger.info(f"[{trace_id}] ROUTING_WORKSHEET_CACHE_HIT worksheet_name={worksheet_name}")
        return worksheet_cache[worksheet_name]
    if worksheet_name in _ROUTING_WORKSHEET_OBJECT_SHARED_CACHE:
        ws = _ROUTING_WORKSHEET_OBJECT_SHARED_CACHE[worksheet_name]
        worksheet_cache[worksheet_name] = ws
        logger.info(f"[{trace_id}] ROUTING_WORKSHEET_SHARED_CACHE_HIT worksheet_name={worksheet_name}")
        return ws
    spreadsheet = open_routing_spreadsheet(trace_id)
    if not spreadsheet:
        return None
    try:
        ws = gsheet_guarded_call(trace_id, f"worksheet.open.{worksheet_name}", spreadsheet.worksheet, worksheet_name)
        worksheet_cache[worksheet_name] = ws
        _ROUTING_WORKSHEET_OBJECT_SHARED_CACHE[worksheet_name] = ws
        logger.info(f"[{trace_id}] ROUTING_WORKSHEET_READY worksheet_name={worksheet_name}")
        return ws
    except gspread.WorksheetNotFound:
        logger.error(f"[{trace_id}] ROUTING_WORKSHEET_NOT_FOUND worksheet_name={worksheet_name}")
        return None
    except Exception as e:
        logger.exception(f"[{trace_id}] ROUTING_WORKSHEET_OPEN_FAILED worksheet_name={worksheet_name} exception={type(e).__name__}:{e}")
        ws = _ROUTING_WORKSHEET_OBJECT_SHARED_CACHE.get(worksheet_name)
        if ws is not None and _is_gsheet_quota_error(e):
            worksheet_cache[worksheet_name] = ws
            logger.info(f"[{trace_id}] ROUTING_WORKSHEET_STALE_CACHE_FALLBACK worksheet_name={worksheet_name}")
            return ws
        return None
def load_routing_master_records(trace_id: str, worksheet_name: str, ttl_seconds: Optional[int] = None) -> List[dict]:
    sheet_name = safe_str(worksheet_name)
    if not sheet_name:
        return []
    ttl = int(ttl_seconds or ROUTING_MASTER_CACHE_TTL_SECONDS)
    request_cache = getattr(g, "_dt79_routing_master_records_cache", None)
    if request_cache is None:
        request_cache = {}
        g._dt79_routing_master_records_cache = request_cache
    if sheet_name in request_cache:
        rows = request_cache[sheet_name]
        logger.info(f"[{trace_id}] ROUTING_MASTER_RECORDS_CACHE_HIT scope=request worksheet_name={sheet_name} count={len(rows)}")
        return rows

    with _ROUTING_MASTER_CACHE_LOCK:
        shared_entry = _ROUTING_MASTER_RECORDS_SHARED_CACHE.get(sheet_name)
        if shared_entry and _cache_is_fresh(float(shared_entry.get("loaded_at_ts", 0.0) or 0.0), ttl):
            rows = shared_entry.get("records", []) or []
            request_cache[sheet_name] = rows
            logger.info(f"[{trace_id}] ROUTING_MASTER_RECORDS_CACHE_HIT scope=shared worksheet_name={sheet_name} count={len(rows)}")
            return rows

    ws = get_routing_worksheet_by_name(trace_id, sheet_name)
    if not ws:
        logger.error(f"[{trace_id}] ROUTING_MASTER_RECORDS_SHEET_UNAVAILABLE worksheet_name={sheet_name}")
        if shared_entry:
            rows = shared_entry.get("records", []) or []
            request_cache[sheet_name] = rows
            logger.info(f"[{trace_id}] ROUTING_MASTER_RECORDS_STALE_FALLBACK worksheet_name={sheet_name} count={len(rows)}")
            return rows
        return []

    rows = get_records_safe(ws, trace_id, sheet_name)
    if rows:
        request_cache[sheet_name] = rows
        with _ROUTING_MASTER_CACHE_LOCK:
            _ROUTING_MASTER_RECORDS_SHARED_CACHE[sheet_name] = {"records": rows, "loaded_at_ts": _now_ts()}
        logger.info(f"[{trace_id}] ROUTING_MASTER_RECORDS_CACHE_READY worksheet_name={sheet_name} count={len(rows)}")
        return rows

    with _ROUTING_MASTER_CACHE_LOCK:
        shared_entry = _ROUTING_MASTER_RECORDS_SHARED_CACHE.get(sheet_name)
    if shared_entry:
        rows = shared_entry.get("records", []) or []
        request_cache[sheet_name] = rows
        logger.info(f"[{trace_id}] ROUTING_MASTER_RECORDS_STALE_FALLBACK worksheet_name={sheet_name} count={len(rows)}")
        return rows
    return []


def load_location_master_records(trace_id: str, worksheet_name: str, role: str = "") -> List[dict]:
    sheet_name = safe_str(worksheet_name)
    if not sheet_name:
        return []
    rows = load_routing_master_records(trace_id, sheet_name, LOCATION_MASTER_CACHE_TTL_SECONDS)
    logger.info(
        f"[{trace_id}] LOCATION_MASTER_RECORDS_CACHE_READY "
        f"role={safe_str(role) or sheet_name} worksheet_name={sheet_name} count={len(rows)}"
    )
    return rows


def load_bot_config_map(trace_id: str) -> Dict[str, str]:
    cached = getattr(g, "_dt79_routing_config_map", None)
    if isinstance(cached, dict) and cached:
        logger.info(f"[{trace_id}] ROUTING_CONFIG_CACHE_HIT scope=request")
        return cached

    shared_config = _ROUTING_CONFIG_SHARED_CACHE.get("config_map")
    shared_loaded_at_ts = float(_ROUTING_CONFIG_SHARED_CACHE.get("loaded_at_ts", 0.0) or 0.0)
    if isinstance(shared_config, dict) and shared_config and _cache_is_fresh(shared_loaded_at_ts, ROUTING_MASTER_CACHE_TTL_SECONDS):
        g._dt79_routing_config_map = shared_config
        logger.info(f"[{trace_id}] ROUTING_CONFIG_CACHE_HIT scope=shared")
        return shared_config

    rows = load_routing_master_records(trace_id, BOT_CONFIG_SHEET_NAME, ROUTING_MASTER_CACHE_TTL_SECONDS)
    config_map = {}
    for row in rows:
        key = safe_str(row.get("Key") or row.get("key")).strip()
        value = safe_str(row.get("Value") or row.get("value")).strip()
        if key:
            config_map[key] = value
    g._dt79_routing_config_map = config_map
    _ROUTING_CONFIG_SHARED_CACHE["config_map"] = config_map
    _ROUTING_CONFIG_SHARED_CACHE["loaded_at_ts"] = _now_ts()
    logger.info(f"[{trace_id}] ROUTING_CONFIG_READY keys={json.dumps(sorted(config_map.keys()), ensure_ascii=False)}")
    return config_map

def normalize_routing_text(value: str) -> str:
    normalized = _normalize_match_text(value)
    normalized = normalized.replace("đ", "d").replace("Đ", "D").lower()
    return normalized

def build_location_alias_index(alias_rows: List[dict]) -> Dict[str, List[dict]]:
    alias_index: Dict[str, List[dict]] = {}
    for row in alias_rows:
        normalized_alias = normalize_routing_text(row.get("normalized_alias") or row.get("alias_text"))
        if not normalized_alias:
            continue
        if safe_str(row.get("location_id")):
            target_key = safe_str(row.get("location_id"))
            item = {
                "target_key": target_key,
                "target_type": "location_id",
                "normalized_alias": normalized_alias,
                "lang_group": safe_str(row.get("lang_group")),
                "alias_type": safe_str(row.get("alias_type")),
            }
        else:
            target_key = safe_str(row.get("root_location"))
            if not target_key:
                continue
            item = {
                "target_key": target_key,
                "target_type": "root_location",
                "normalized_alias": normalized_alias,
                "lang_group": safe_str(row.get("lang_group")),
                "alias_type": safe_str(row.get("alias_type")),
            }
        alias_index.setdefault(normalized_alias, [])
        if not any(existing.get("target_key") == item["target_key"] and existing.get("target_type") == item["target_type"] for existing in alias_index[normalized_alias]):
            alias_index[normalized_alias].append(item)
    return alias_index

def build_canonical_location_index(canonical_rows: List[dict]) -> Dict[str, dict]:
    result: Dict[str, dict] = {}
    for row in canonical_rows:
        location_id = safe_str(row.get("location_id"))
        if not location_id:
            continue
        result[location_id] = row
    return result

def build_region_map_index(region_rows: List[dict]) -> Dict[str, dict]:
    result: Dict[str, dict] = {}
    for row in region_rows:
        location_id = safe_str(row.get("location_id"))
        if not location_id:
            continue
        result[location_id] = row
    return result

def resolve_location_from_v2(text: str, alias_rows: List[dict], canonical_rows: List[dict], region_rows: List[dict]) -> Optional[dict]:
    normalized = normalize_routing_text(text)
    alias_index = build_location_alias_index(alias_rows or [])
    canonical_index = build_canonical_location_index(canonical_rows or [])
    region_index = build_region_map_index(region_rows or [])
    matched_alias = ""
    matched_item = None
    for alias, items in alias_index.items():
        if _phrase_present(normalized, alias):
            matched_alias = alias
            matched_item = items[0] if items else None
            break
    if not matched_item:
        return None
    if matched_item.get("target_type") == "root_location":
        return {
            "location_id": "",
            "service_region_key": safe_str(matched_item.get("target_key")),
            "matched_alias": matched_alias,
            "target_type": "root_location",
        }
    location_id = safe_str(matched_item.get("target_key"))
    canonical_row = canonical_index.get(location_id) or {}
    region_row = region_index.get(location_id) or {}
    service_region_key = safe_str(region_row.get("service_region_key")) or safe_str(canonical_row.get("service_region_key"))
    if not service_region_key:
        return None
    return {
        "location_id": location_id,
        "service_region_key": service_region_key,
        "matched_alias": matched_alias,
        "canonical_zh": safe_str(canonical_row.get("canonical_zh")),
        "canonical_en": safe_str(canonical_row.get("canonical_en")),
        "district_town_zh": safe_str(canonical_row.get("district_town_zh")),
        "county_city_zh": safe_str(canonical_row.get("county_city_zh")),
        "target_type": "location_id",
    }

def extract_location_alias(text: str, alias_rows: Optional[List[dict]] = None) -> str:
    normalized = normalize_routing_text(text)
    alias_index = build_location_alias_index(alias_rows or [])
    for alias, items in alias_index.items():
        if _phrase_present(normalized, alias) and items:
            return safe_str(items[0].get("target_key"))
    for canonical, aliases in LOCATION_ALIAS_MAP.items():
        for alias in aliases:
            if _phrase_present(normalized, alias):
                return canonical
    return ""
def detect_routing_intent(text: str, intent_rows: List[dict]) -> Tuple[str, List[str]]:
    normalized = normalize_routing_text(text)
    best_intent = ""
    best_hits: List[str] = []
    best_score = 0
    for row in intent_rows:
        intent_name = safe_str(row.get("intent_name"))
        keywords_raw = safe_str(row.get("keywords"))
        if not intent_name or not keywords_raw:
            continue
        hits = []
        for keyword in [safe_str(x) for x in keywords_raw.split(",") if safe_str(x)]:
            if _phrase_present(normalized, keyword):
                hits.append(keyword)
        if not hits:
            continue
        score = max(len(normalize_routing_text(hit)) for hit in hits)
        if score > best_score:
            best_score = score
            best_intent = intent_name
            best_hits = hits
    return best_intent, best_hits
def filter_active_services(service_rows: List[dict]) -> List[dict]:
    results = []
    for row in service_rows:
        status = safe_str(row.get("service_status")).lower()
        if status and status != "active":
            continue
        if not safe_str(row.get("intent_name")):
            continue
        if not safe_str(row.get("contact_id")):
            continue
        results.append(row)
    return results
def choose_service_for_intent(intent_name: str, location_hint: str, service_rows: List[dict]) -> Optional[dict]:
    normalized_location = safe_str(location_hint)
    candidates = [row for row in filter_active_services(service_rows) if safe_str(row.get("intent_name")) == intent_name]
    if not candidates:
        return None
    exact_matches = []
    fallback_matches = []
    for row in candidates:
        row_region = safe_str(row.get("service_region_key")) or safe_str(row.get("location"))
        priority = parse_priority(row.get("priority"))
        if normalized_location and row_region == normalized_location:
            exact_matches.append((priority, row))
        elif row_region == ROUTING_FALLBACK_LOCATION:
            fallback_matches.append((priority, row))
    if exact_matches:
        exact_matches.sort(key=lambda item: (-item[0], safe_str(item[1].get("service_id"))))
        return exact_matches[0][1]
    if fallback_matches:
        fallback_matches.sort(key=lambda item: (-item[0], safe_str(item[1].get("service_id"))))
        return fallback_matches[0][1]
    return None
def build_location_token_guesses(text: str, matched_keywords: List[str]) -> List[str]:
    normalized = normalize_routing_text(text)
    base_segment = extract_location_token_guess(text, matched_keywords) or normalized
    raw_parts = re.split(r"\b(?:hoac la|hoac|or|hay|va)\b|[,/;]+", base_segment)
    fillers_pattern = r"^(?:khu|o|tai|gan|quan|huyen|xa|phuong|thi tran|thi xa|tp)\s+"
    token_guesses: List[str] = []
    seen = set()
    for part in raw_parts:
        part = safe_str(part).strip(" ,.-")
        part = re.sub(fillers_pattern, "", part).strip(" ,.-")
        if len(part) < 3:
            continue
        variants = [part, part.replace(" ", "")]
        for variant in variants:
            variant = safe_str(variant)
            if not variant or variant in seen:
                continue
            seen.add(variant)
            token_guesses.append(variant[:80])
    if normalized and normalized not in seen:
        token_guesses.append(normalized[:80])
    return token_guesses


def build_phonetic_skeleton(value: str) -> str:
    normalized = normalize_routing_text(value)
    replacements = [
        ("ph", "f"),
        ("th", "t"),
        ("kh", "k"),
        ("qu", "q"),
        ("sh", "s"),
        ("zh", "z"),
        ("ch", "c"),
    ]
    for src, dst in replacements:
        normalized = normalized.replace(src, dst)
    return "".join(ch for ch in normalized if ch not in "aeiouy ")


def score_location_candidate(alias: str, item: dict, normalized_text: str, token_guesses: List[str]) -> int:
    alias_type = safe_str(item.get("alias_type")).lower()
    base_score_map = {
        "district_zh": 100,
        "district_en": 95,
        "common_input": 85,
        "phonetic_vi": 72,
        "phonetic_id": 72,
        "phonetic_th": 72,
    }
    score = base_score_map.get(alias_type, 60)
    alias_len = len(alias)
    score += min(alias_len, 18)

    if re.search(rf"(?<!\w){re.escape(alias)}(?!\w)", normalized_text):
        score += 8
    elif alias in normalized_text:
        score += 2

    intent_context_tokens = ["khu", "o", "gan", "phong", "sim", "renew", "thang", "month"]
    if any(token and token in normalized_text for token in intent_context_tokens):
        score += 3

    for guess in token_guesses:
        if not guess:
            continue
        ratio = difflib.SequenceMatcher(None, guess, alias).ratio()
        if ratio >= 0.92:
            score += 15
            break
        if ratio >= 0.82:
            score += 8
            break
        if ratio >= 0.72:
            score += 4
            break

    return score



def collect_routing_location_candidates(text: str, alias_rows: List[dict], matched_keywords: Optional[List[str]] = None) -> List[dict]:
    normalized = normalize_routing_text(text)
    alias_index = build_location_alias_index(alias_rows or [])
    results: List[dict] = []
    seen = set()

    matched_keywords = matched_keywords or []
    token_guesses = build_location_token_guesses(text, matched_keywords)
    token_guess_skeletons = [build_phonetic_skeleton(x) for x in token_guesses if x]

    for alias, items in alias_index.items():
        alias_match = _phrase_present(normalized, alias)
        fuzzy_ratio = 0.0
        skeleton_ratio = 0.0

        if not alias_match:
            fuzzy_ratio = max(
                (difflib.SequenceMatcher(None, guess, alias).ratio() for guess in token_guesses if guess),
                default=0.0,
            )

            alias_skeleton = build_phonetic_skeleton(alias)
            skeleton_ratio = max(
                (
                    difflib.SequenceMatcher(None, skeleton, alias_skeleton).ratio()
                    for skeleton in token_guess_skeletons
                    if skeleton and alias_skeleton
                ),
                default=0.0,
            )

            alias_match = (
                (fuzzy_ratio >= 0.72 and len(alias) >= 4)
                or (skeleton_ratio >= 0.78 and len(alias) >= 4)
            )

        if not alias_match:
            continue

        for item in items:
            target_key = safe_str(item.get("target_key"))
            target_type = safe_str(item.get("target_type"))
            dedupe_key = (target_key, target_type)
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)

            score = score_location_candidate(alias, item, normalized, token_guesses)

            if fuzzy_ratio >= 0.92:
                score += 10
            elif fuzzy_ratio >= 0.82:
                score += 6
            elif fuzzy_ratio >= 0.72:
                score += 3

            if skeleton_ratio >= 0.92:
                score += 12
            elif skeleton_ratio >= 0.84:
                score += 8
            elif skeleton_ratio >= 0.78:
                score += 4

            results.append({
                "matched_alias": alias,
                "target_key": target_key,
                "target_type": target_type,
                "alias_type": safe_str(item.get("alias_type")),
                "lang_group": safe_str(item.get("lang_group")),
                "score": score,
            })

    results.sort(
        key=lambda item: (
            -int(item.get("score", 0) or 0),
            -len(safe_str(item.get("matched_alias"))),
            safe_str(item.get("target_key")),
        )
    )
    return results

def choose_best_location_candidate(candidates: List[dict], canonical_rows: List[dict], region_rows: List[dict]) -> dict:
    if not candidates:
        return {
            "decision": "no_candidate",
            "confidence": "low",
            "selected_candidate": None,
            "top_score": 0,
            "second_score": 0,
        }

    canonical_index = build_canonical_location_index(canonical_rows or [])
    region_index = build_region_map_index(region_rows or [])

    enriched = []
    for candidate in candidates:
        current = dict(candidate)
        if safe_str(current.get("target_type")) == "root_location":
            current["location_id"] = ""
            current["service_region_key"] = safe_str(current.get("target_key"))
        else:
            location_id = safe_str(current.get("target_key"))
            canonical_row = canonical_index.get(location_id) or {}
            region_row = region_index.get(location_id) or {}
            current["location_id"] = location_id
            current["service_region_key"] = safe_str(region_row.get("service_region_key")) or safe_str(canonical_row.get("service_region_key"))
        enriched.append(current)

    enriched = [c for c in enriched if safe_str(c.get("service_region_key"))]
    if not enriched:
        return {
            "decision": "no_candidate",
            "confidence": "low",
            "selected_candidate": None,
            "top_score": 0,
            "second_score": 0,
        }

    top = enriched[0]
    second = enriched[1] if len(enriched) > 1 else {}
    top_score = int(top.get("score", 0) or 0)
    second_score = int(second.get("score", 0) or 0) if isinstance(second, dict) else 0
    gap = top_score - second_score

    if top_score >= 85 and gap >= 10:
        decision = "route"
        confidence = "high"
    elif 75 <= top_score <= 84 and gap >= 15:
        decision = "route"
        confidence = "medium"
    else:
        decision = "slowpath"
        confidence = "low"

    return {
        "decision": decision,
        "confidence": confidence,
        "selected_candidate": top if decision == "route" else None,
        "top_score": top_score,
        "second_score": second_score,
        "candidates": enriched,
    }

_LOCATION_ALIAS_LOOKUP_SHARED_CACHE = {
    "alias_index": {},
    "alias_lengths": set(),
    "canonical_index": {},
    "region_index": {},
    "fingerprint": "",
    "loaded_at_ts": 0.0,
}

def _rows_fingerprint(rows: List[dict]) -> str:
    payload = []
    for row in rows or []:
        payload.append("|".join([
            safe_str(row.get("alias_text")),
            safe_str(row.get("normalized_alias")),
            safe_str(row.get("location_id")),
            safe_str(row.get("root_location")),
            safe_str(row.get("alias_type")),
        ]))
    raw = "\n".join(payload)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest() if raw else ""

def get_location_alias_lookup(alias_rows: List[dict], trace_id: str = "") -> dict:
    fingerprint = _rows_fingerprint(alias_rows or [])
    cached_fp = safe_str(_LOCATION_ALIAS_LOOKUP_SHARED_CACHE.get("fingerprint"))
    if fingerprint and fingerprint == cached_fp:
        return _LOCATION_ALIAS_LOOKUP_SHARED_CACHE

    alias_index = build_location_alias_index(alias_rows or [])
    alias_lengths = {len(alias.split()) for alias in alias_index.keys() if alias}
    _LOCATION_ALIAS_LOOKUP_SHARED_CACHE["alias_index"] = alias_index
    _LOCATION_ALIAS_LOOKUP_SHARED_CACHE["alias_lengths"] = alias_lengths or {1}
    _LOCATION_ALIAS_LOOKUP_SHARED_CACHE["fingerprint"] = fingerprint
    _LOCATION_ALIAS_LOOKUP_SHARED_CACHE["loaded_at_ts"] = _now_ts()
    if trace_id:
        logger.info(
            f"[{trace_id}] LOCATION_ALIAS_LOOKUP_READY alias_count={len(alias_index)} "
            f"ngram_sizes={json.dumps(sorted(alias_lengths or {1}), ensure_ascii=False)}"
        )
    return _LOCATION_ALIAS_LOOKUP_SHARED_CACHE

def build_text_alias_lookup_keys(text: str, alias_lengths: set) -> List[str]:
    normalized = normalize_routing_text(text)
    if not normalized:
        return []
    tokens = [safe_str(x) for x in re.split(r"\s+", normalized) if safe_str(x)]
    max_len = min(max(alias_lengths or {1}), 6)
    wanted_lengths = {n for n in (alias_lengths or {1}) if 1 <= int(n) <= max_len}
    keys = []
    seen = set()

    def add_key(value: str) -> None:
        key = safe_str(value)
        if key and key not in seen:
            seen.add(key)
            keys.append(key)

    add_key(normalized)
    for token in tokens:
        add_key(token)
    for n in sorted(wanted_lengths):
        if n <= 1:
            continue
        for idx in range(0, max(0, len(tokens) - n + 1)):
            add_key(" ".join(tokens[idx:idx+n]))
    return keys

def resolve_location_from_v2(text: str, alias_rows: List[dict], canonical_rows: List[dict], region_rows: List[dict]) -> Optional[dict]:
    lookup = get_location_alias_lookup(alias_rows or [])
    alias_index = lookup.get("alias_index") or {}
    keys = build_text_alias_lookup_keys(text, lookup.get("alias_lengths") or {1})
    canonical_index = build_canonical_location_index(canonical_rows or [])
    region_index = build_region_map_index(region_rows or [])
    matched_alias = ""
    matched_item = None

    for key in keys:
        items = alias_index.get(key)
        if items:
            matched_alias = key
            matched_item = items[0]
            break

    if not matched_item:
        return None
    if matched_item.get("target_type") == "root_location":
        return {
            "location_id": "",
            "service_region_key": safe_str(matched_item.get("target_key")),
            "matched_alias": matched_alias,
            "target_type": "root_location",
        }
    location_id = safe_str(matched_item.get("target_key"))
    canonical_row = canonical_index.get(location_id) or {}
    region_row = region_index.get(location_id) or {}
    service_region_key = safe_str(region_row.get("service_region_key")) or safe_str(canonical_row.get("service_region_key"))
    if not service_region_key:
        return None
    return {
        "location_id": location_id,
        "service_region_key": service_region_key,
        "matched_alias": matched_alias,
        "canonical_zh": safe_str(canonical_row.get("canonical_zh")),
        "canonical_en": safe_str(canonical_row.get("canonical_en")),
        "district_town_zh": safe_str(canonical_row.get("district_town_zh")),
        "county_city_zh": safe_str(canonical_row.get("county_city_zh")),
        "target_type": "location_id",
    }

def extract_location_alias(text: str, alias_rows: Optional[List[dict]] = None) -> str:
    lookup = get_location_alias_lookup(alias_rows or [])
    alias_index = lookup.get("alias_index") or {}
    for key in build_text_alias_lookup_keys(text, lookup.get("alias_lengths") or {1}):
        items = alias_index.get(key)
        if items:
            return safe_str(items[0].get("target_key"))
    normalized = normalize_routing_text(text)
    for canonical, aliases in LOCATION_ALIAS_MAP.items():
        for alias in aliases:
            if _phrase_present(normalized, normalize_routing_text(alias)):
                return canonical
    return ""

def collect_routing_location_candidates(text: str, alias_rows: List[dict], matched_keywords: Optional[List[str]] = None) -> List[dict]:
    normalized = normalize_routing_text(text)
    lookup = get_location_alias_lookup(alias_rows or [])
    alias_index = lookup.get("alias_index") or {}
    alias_lengths = lookup.get("alias_lengths") or {1}
    results: List[dict] = []
    seen = set()

    matched_keywords = matched_keywords or []
    token_guesses = build_location_token_guesses(text, matched_keywords)
    lookup_keys = build_text_alias_lookup_keys(text, alias_lengths)
    for guess in token_guesses:
        lookup_keys.extend(build_text_alias_lookup_keys(guess, alias_lengths))

    exact_aliases = []
    exact_seen = set()
    for key in lookup_keys:
        if key in alias_index and key not in exact_seen:
            exact_seen.add(key)
            exact_aliases.append(key)

    for alias in exact_aliases:
        for item in alias_index.get(alias, []):
            target_key = safe_str(item.get("target_key"))
            target_type = safe_str(item.get("target_type"))
            dedupe_key = (target_key, target_type)
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)
            score = score_location_candidate(alias, item, normalized, token_guesses) + 20
            results.append({
                "matched_alias": alias,
                "target_key": target_key,
                "target_type": target_type,
                "alias_type": safe_str(item.get("alias_type")),
                "lang_group": safe_str(item.get("lang_group")),
                "score": score,
                "match_mode": "hash_exact",
            })

    if not results:
        token_guess_skeletons = [build_phonetic_skeleton(x) for x in token_guesses if x]
        for alias, items in alias_index.items():
            fuzzy_ratio = max(
                (difflib.SequenceMatcher(None, guess, alias).ratio() for guess in token_guesses if guess),
                default=0.0,
            )
            alias_skeleton = build_phonetic_skeleton(alias)
            skeleton_ratio = max(
                (
                    difflib.SequenceMatcher(None, skeleton, alias_skeleton).ratio()
                    for skeleton in token_guess_skeletons
                    if skeleton and alias_skeleton
                ),
                default=0.0,
            )
            alias_match = (
                (fuzzy_ratio >= 0.72 and len(alias) >= 4)
                or (skeleton_ratio >= 0.78 and len(alias) >= 4)
            )
            if not alias_match:
                continue
            for item in items:
                target_key = safe_str(item.get("target_key"))
                target_type = safe_str(item.get("target_type"))
                dedupe_key = (target_key, target_type)
                if dedupe_key in seen:
                    continue
                seen.add(dedupe_key)
                score = score_location_candidate(alias, item, normalized, token_guesses)
                if fuzzy_ratio >= 0.92:
                    score += 10
                elif fuzzy_ratio >= 0.82:
                    score += 6
                elif fuzzy_ratio >= 0.72:
                    score += 3
                if skeleton_ratio >= 0.92:
                    score += 12
                elif skeleton_ratio >= 0.84:
                    score += 8
                elif skeleton_ratio >= 0.78:
                    score += 4
                results.append({
                    "matched_alias": alias,
                    "target_key": target_key,
                    "target_type": target_type,
                    "alias_type": safe_str(item.get("alias_type")),
                    "lang_group": safe_str(item.get("lang_group")),
                    "score": score,
                    "match_mode": "fuzzy_fallback",
                })

    results.sort(
        key=lambda item: (
            -int(item.get("score", 0) or 0),
            0 if safe_str(item.get("match_mode")) == "hash_exact" else 1,
            -len(safe_str(item.get("matched_alias"))),
            safe_str(item.get("target_key")),
        )
    )
    return results

def warm_up_cache(trace_id: str = "") -> bool:
    trace_id = safe_str(trace_id) or make_trace_id()
    start = time.perf_counter()
    try:
        start_async_log_worker()
        config_map = load_bot_config_map(trace_id)
        intent_sheet_name = safe_str(config_map.get("intent_sheet")) or INTENT_MASTER_SHEET_NAME
        service_sheet_name = safe_str(config_map.get("service_sheet")) or SERVICE_MASTER_SHEET_NAME
        alias_sheet_name = safe_str(config_map.get("location_alias_sheet")) or LOCATION_ALIAS_MASTER_SHEET_NAME
        variant_sheet_name = safe_str(config_map.get("variant_sheet")) or SERVICE_VARIANT_MASTER_SHEET_NAME

        intent_rows = load_routing_master_records(trace_id, intent_sheet_name, ROUTING_MASTER_CACHE_TTL_SECONDS)
        service_rows = load_routing_master_records(trace_id, service_sheet_name, ROUTING_MASTER_CACHE_TTL_SECONDS)
        canonical_sheet_name = safe_str(config_map.get("location_canonical_sheet"))
        region_map_sheet_name = safe_str(config_map.get("location_region_map_sheet"))
        alias_rows = load_location_master_records(trace_id, alias_sheet_name, "alias")
        canonical_rows = load_location_master_records(trace_id, canonical_sheet_name, "canonical") if canonical_sheet_name else []
        region_rows = load_location_master_records(trace_id, region_map_sheet_name, "region_map") if region_map_sheet_name else []
        get_location_alias_lookup(alias_rows, trace_id)
        variant_rows = load_sim_variant_rows_fastpath(trace_id, variant_sheet_name)
        logger.info(
            f"[{trace_id}] WARM_UP_CACHE_OK alias_sheet={alias_sheet_name} "
            f"intent_rows={len(intent_rows)} service_rows={len(service_rows)} "
            f"alias_rows={len(alias_rows)} canonical_rows={len(canonical_rows)} "
            f"region_rows={len(region_rows)} sim_variant_rows={len(variant_rows)} latency_ms={ms_since(start)}"
        )
        return True
    except GSheetCircuitOpenError as exc:
        logger.error(f"[{trace_id}] WARM_UP_CACHE_SKIPPED reason=gsheet_circuit_open exception={type(exc).__name__}:{exc}")
        return False
    except (gspread.WorksheetNotFound, gspread.exceptions.APIError, requests.RequestException, ValueError, KeyError, TypeError) as exc:
        logger.exception(f"[{trace_id}] WARM_UP_CACHE_FAILED exception={type(exc).__name__}:{exc}")
        return False

def extract_location_token_guess(text: str, matched_keywords: List[str]) -> str:
    normalized = normalize_routing_text(text)
    for keyword in sorted((matched_keywords or []), key=lambda x: len(safe_str(x)), reverse=True):
        normalized_keyword = normalize_routing_text(keyword)
        if not normalized_keyword:
            continue
        if normalized_keyword in normalized:
            tail = normalized.split(normalized_keyword, 1)[1].strip(" ,.-")
            if tail:
                return tail[:80]
    return ""

def _ensure_generic_routing_queue_worksheet(trace_id: str, worksheet_name: str, headers: List[str], ready_cache: dict):
    spreadsheet = open_routing_spreadsheet(trace_id)
    if not spreadsheet:
        return None
    try:
        ws = gsheet_guarded_call(trace_id, f"worksheet.open.{worksheet_name}", spreadsheet.worksheet, worksheet_name)
    except gspread.WorksheetNotFound:
        try:
            ws = spreadsheet.add_worksheet(title=worksheet_name, rows=5000, cols=max(len(headers), 12))
            append_row_guarded(ws, trace_id, locals().get("worksheet_name", getattr(ws, "title", "unknown")), headers, value_input_option="USER_ENTERED")
            ready_cache["verified"] = True
            ready_cache["loaded_at_ts"] = _now_ts()
            logger.info(f"[{trace_id}] ROUTING_QUEUE_SHEET_CREATED worksheet_name={worksheet_name}")
            return ws
        except Exception as e:
            logger.exception(f"[{trace_id}] ROUTING_QUEUE_SHEET_CREATE_FAILED worksheet_name={worksheet_name} exception={type(e).__name__}:{e}")
            return None
    except Exception as e:
        logger.exception(f"[{trace_id}] ROUTING_QUEUE_SHEET_OPEN_FAILED worksheet_name={worksheet_name} exception={type(e).__name__}:{e}")
        return None

    verified = bool(ready_cache.get("verified"))
    verified_loaded_at_ts = float(ready_cache.get("loaded_at_ts", 0.0) or 0.0)
    if verified and _cache_is_fresh(verified_loaded_at_ts, GSHEET_VALUES_CACHE_TTL_SECONDS):
        logger.info(f"[{trace_id}] ROUTING_QUEUE_WORKSHEET_READY_CACHE_HIT worksheet_name={worksheet_name}")
        return ws

    values = get_all_values_safe(ws, trace_id, worksheet_name)
    if not values:
        try:
            append_row_guarded(ws, trace_id, locals().get("worksheet_name", getattr(ws, "title", "unknown")), headers, value_input_option="USER_ENTERED")
            logger.info(f"[{trace_id}] ROUTING_QUEUE_HEADERS_INIT_OK worksheet_name={worksheet_name}")
        except Exception as e:
            logger.exception(f"[{trace_id}] ROUTING_QUEUE_HEADERS_INIT_FAILED worksheet_name={worksheet_name} exception={type(e).__name__}:{e}")
            return None

    ready_cache["verified"] = True
    ready_cache["loaded_at_ts"] = _now_ts()
    return ws

def ensure_routing_miss_harvest_worksheet(trace_id: str):
    return _ensure_generic_routing_queue_worksheet(
        trace_id=trace_id,
        worksheet_name=ROUTING_MISS_HARVEST_SHEET_NAME,
        headers=ROUTING_MISS_HARVEST_HEADERS,
        ready_cache=_ROUTING_MISS_HARVEST_WORKSHEET_READY_CACHE,
    )

def ensure_routing_slowpath_queue_worksheet(trace_id: str):
    return _ensure_generic_routing_queue_worksheet(
        trace_id=trace_id,
        worksheet_name=ROUTING_SLOWPATH_QUEUE_SHEET_NAME,
        headers=ROUTING_SLOWPATH_QUEUE_HEADERS,
        ready_cache=_ROUTING_SLOWPATH_WORKSHEET_READY_CACHE,
    )


def ensure_routing_shadow_suggestions_worksheet(trace_id: str):
    return _ensure_generic_routing_queue_worksheet(
        trace_id=trace_id,
        worksheet_name=ROUTING_SHADOW_SUGGESTIONS_SHEET_NAME,
        headers=ROUTING_SHADOW_SUGGESTIONS_HEADERS,
        ready_cache=_ROUTING_SHADOW_SUGGESTIONS_WORKSHEET_READY_CACHE,
    )


def ensure_routing_admin_audit_log_worksheet(trace_id: str):
    return _ensure_generic_routing_queue_worksheet(
        trace_id=trace_id,
        worksheet_name=ROUTING_ADMIN_AUDIT_LOG_SHEET_NAME,
        headers=ROUTING_ADMIN_AUDIT_LOG_HEADERS,
        ready_cache=_ROUTING_ADMIN_AUDIT_LOG_WORKSHEET_READY_CACHE,
    )


def append_routing_admin_audit_log(
    trace_id: str,
    batch_id: str,
    shadow_row: dict,
    source_row: int,
    action: str,
    writeback_status: str,
    sanitizer_result: str,
    sanitizer_reason: str,
    target_sheet: str,
    target_row: str = "",
    notes: str = "",
    operator: str = "system:process-shadow-writeback",
    alias_source: str = "",
    raw_text_risk_result: str = "",
    raw_text_risk_type: str = "",
    raw_text_risk_source: str = "",
    guard_result: str = "",
    guard_reason: str = "",
) -> bool:
    ws = ensure_routing_admin_audit_log_worksheet(trace_id)
    if not ws:
        logger.error(f"[{trace_id}] ROUTING_ADMIN_AUDIT_LOG_APPEND_SKIPPED reason=worksheet_unavailable")
        return False
    row = [
        now_tw_iso(),
        safe_str(shadow_row.get("trace_id")) or safe_str(trace_id),
        safe_str(batch_id),
        ROUTING_SHADOW_SUGGESTIONS_SHEET_NAME,
        safe_str(source_row),
        safe_str(action),
        sanitize_incoming_text(shadow_row.get("raw_text"))[:500],
        safe_str(shadow_row.get("normalized_text"))[:500],
        (safe_str(shadow_row.get("suggested_alias")) or safe_str(shadow_row.get("location_token_guess")))[:120],
        safe_str(shadow_row.get("suggested_location_id")),
        safe_str(shadow_row.get("suggested_region_key")),
        safe_str(shadow_row.get("review_status")),
        safe_str(writeback_status),
        safe_str(sanitizer_result),
        safe_str(sanitizer_reason),
        safe_str(target_sheet),
        safe_str(target_row),
        safe_str(operator),
        safe_str(notes)[:500],
        safe_str(alias_source),
        safe_str(raw_text_risk_result),
        safe_str(raw_text_risk_type),
        safe_str(raw_text_risk_source),
        safe_str(guard_result),
        safe_str(guard_reason),
    ]
    try:
        append_row_guarded(ws, trace_id, locals().get("worksheet_name", getattr(ws, "title", "unknown")), row, value_input_option="USER_ENTERED")
        _invalidate_worksheet_caches(ROUTING_ADMIN_AUDIT_LOG_SHEET_NAME)
        logger.info(
            f"[{trace_id}] ROUTING_ADMIN_AUDIT_LOG_APPEND_OK "
            f"batch_id={safe_str(batch_id)} source_row={safe_str(source_row)} "
            f"action={safe_str(action)} writeback_status={safe_str(writeback_status)} "
            f"sanitizer_result={safe_str(sanitizer_result)} target_sheet={safe_str(target_sheet)} "
            f"target_row={safe_str(target_row)} alias_source={safe_str(alias_source)} "
            f"raw_text_risk_result={safe_str(raw_text_risk_result)} raw_text_risk_type={safe_str(raw_text_risk_type)} "
            f"guard_result={safe_str(guard_result)} guard_reason={safe_str(guard_reason)}"
        )
        return True
    except Exception as e:
        logger.exception(f"[{trace_id}] ROUTING_ADMIN_AUDIT_LOG_APPEND_FAILED exception={type(e).__name__}:{e}")
        return False

def _append_routing_miss_event_sync(
    user_id: str,
    raw_text: str,
    normalized_text: str,
    intent_guess: str,
    location_token_guess: str,
    candidate_aliases: List[str],
    miss_type: str,
    recommended_fix: str,
    trace_id: str,
    reviewer_notes: str = "",
) -> bool:
    ws = ensure_routing_miss_harvest_worksheet(trace_id)
    if not ws:
        logger.error(f"[{trace_id}] ROUTING_MISS_HARVEST_APPEND_SKIPPED reason=worksheet_unavailable")
        return False
    row = [
        now_tw_iso(),
        safe_str(trace_id),
        safe_str(user_id),
        sanitize_incoming_text(raw_text)[:500],
        safe_str(normalized_text)[:500],
        safe_str(intent_guess),
        safe_str(location_token_guess),
        json.dumps(candidate_aliases or [], ensure_ascii=False),
        safe_str(miss_type),
        safe_str(recommended_fix),
        "new",
        safe_str(reviewer_notes),
    ]
    try:
        append_row_guarded(ws, trace_id, locals().get("worksheet_name", getattr(ws, "title", "unknown")), row, value_input_option="USER_ENTERED")
        _invalidate_worksheet_caches(ROUTING_MISS_HARVEST_SHEET_NAME)
        logger.info(
            f"[{trace_id}] ROUTING_MISS_HARVEST_APPEND_OK user_ref={user_ref(user_id)} intent_guess={safe_str(intent_guess)} "
            f"miss_type={safe_str(miss_type)} recommended_fix={safe_str(recommended_fix)} "
            f"location_token_guess={json.dumps(safe_str(location_token_guess), ensure_ascii=False)}"
        )
        return True
    except Exception as e:
        logger.exception(f"[{trace_id}] ROUTING_MISS_HARVEST_APPEND_FAILED exception={type(e).__name__}:{e}")
        return False

def _append_routing_slowpath_event_sync(
    user_id: str,
    raw_text: str,
    normalized_text: str,
    detected_intent: str,
    candidate_locations: List[str],
    candidate_location_ids: List[str],
    reason_code: str,
    confidence: str,
    action_needed: str,
    trace_id: str,
    reviewer_notes: str = "",
) -> bool:
    ws = ensure_routing_slowpath_queue_worksheet(trace_id)
    if not ws:
        logger.error(f"[{trace_id}] ROUTING_SLOWPATH_APPEND_SKIPPED reason=worksheet_unavailable")
        return False
    row = [
        now_tw_iso(),
        safe_str(trace_id),
        safe_str(user_id),
        sanitize_incoming_text(raw_text)[:500],
        safe_str(normalized_text)[:500],
        safe_str(detected_intent),
        json.dumps(candidate_locations or [], ensure_ascii=False),
        json.dumps(candidate_location_ids or [], ensure_ascii=False),
        safe_str(reason_code),
        safe_str(confidence),
        safe_str(action_needed),
        "new",
        safe_str(reviewer_notes),
    ]
    try:
        append_row_guarded(ws, trace_id, locals().get("worksheet_name", getattr(ws, "title", "unknown")), row, value_input_option="USER_ENTERED")
        _invalidate_worksheet_caches(ROUTING_SLOWPATH_QUEUE_SHEET_NAME)
        logger.info(
            f"[{trace_id}] ROUTING_SLOWPATH_APPEND_OK user_ref={user_ref(user_id)} detected_intent={safe_str(detected_intent)} "
            f"reason_code={safe_str(reason_code)} confidence={safe_str(confidence)} action_needed={safe_str(action_needed)}"
        )
        return True
    except Exception as e:
        logger.exception(f"[{trace_id}] ROUTING_SLOWPATH_APPEND_FAILED exception={type(e).__name__}:{e}")
        return False

def _append_routing_shadow_suggestion_sync(
    user_id: str,
    raw_text: str,
    normalized_text: str,
    intent_name: str,
    location_token_guess: str,
    suggested_alias: str,
    suggested_location_id: str,
    suggested_region_key: str,
    suggestion_source: str,
    suggestion_reason: str,
    confidence: str,
    trace_id: str,
    reviewer_notes: str = "",
    second_candidate_alias: str = "",
    second_candidate_location_id: str = "",
    score_gap: str = "",
    decision_context: str = "",
) -> bool:
    ws = ensure_routing_shadow_suggestions_worksheet(trace_id)
    if not ws:
        logger.error(f"[{trace_id}] ROUTING_SHADOW_SUGGESTION_APPEND_SKIPPED reason=worksheet_unavailable")
        return False
    row = [
        now_tw_iso(),
        safe_str(trace_id),
        safe_str(user_id),
        sanitize_incoming_text(raw_text)[:500],
        safe_str(normalized_text)[:500],
        safe_str(intent_name),
        safe_str(location_token_guess),
        safe_str(suggested_alias),
        safe_str(suggested_location_id),
        safe_str(suggested_region_key),
        safe_str(suggestion_source),
        safe_str(suggestion_reason)[:500],
        safe_str(confidence),
        "pending",
        safe_str(reviewer_notes),
        safe_str(second_candidate_alias),
        safe_str(second_candidate_location_id),
        safe_str(score_gap),
        safe_str(decision_context)[:500],
        "pending",
        "",
        "",
        "",
    ]
    try:
        append_row_guarded(ws, trace_id, locals().get("worksheet_name", getattr(ws, "title", "unknown")), row, value_input_option="USER_ENTERED")
        _invalidate_worksheet_caches(ROUTING_SHADOW_SUGGESTIONS_SHEET_NAME)
        logger.info(
            f"[{trace_id}] ROUTING_SHADOW_SUGGESTION_APPEND_OK user_ref={user_ref(user_id)} intent_name={safe_str(intent_name)} "
            f"suggested_alias={json.dumps(safe_str(suggested_alias), ensure_ascii=False)} "
            f"suggested_location_id={json.dumps(safe_str(suggested_location_id), ensure_ascii=False)} "
            f"suggested_region_key={json.dumps(safe_str(suggested_region_key), ensure_ascii=False)} "
            f"second_candidate_alias={json.dumps(safe_str(second_candidate_alias), ensure_ascii=False)} "
            f"second_candidate_location_id={json.dumps(safe_str(second_candidate_location_id), ensure_ascii=False)} "
            f"score_gap={json.dumps(safe_str(score_gap), ensure_ascii=False)} "
            f"suggestion_source={safe_str(suggestion_source)} confidence={safe_str(confidence)}"
        )
        return True
    except Exception as e:
        logger.exception(f"[{trace_id}] ROUTING_SHADOW_SUGGESTION_APPEND_FAILED exception={type(e).__name__}:{e}")
        return False


def append_routing_log_event(user_id: str, intent_name: str, service_row: dict, location_hint: str, location_id: str, message: str, trace_id: str) -> bool:
    return enqueue_async_log(
        ASYNC_LOG_LEVEL_AUDIT,
        trace_id,
        "append_routing_log_event",
        _append_routing_log_event_sync,
        user_id,
        intent_name,
        service_row,
        location_hint,
        location_id,
        message,
        trace_id,
    )

def append_routing_miss_event(
    user_id: str,
    raw_text: str,
    normalized_text: str,
    intent_guess: str,
    location_token_guess: str,
    candidate_aliases: List[str],
    miss_type: str,
    recommended_fix: str,
    trace_id: str,
    reviewer_notes: str = "",
) -> bool:
    return enqueue_async_log(
        ASYNC_LOG_LEVEL_AUDIT,
        trace_id,
        "append_routing_miss_event",
        _append_routing_miss_event_sync,
        user_id,
        raw_text,
        normalized_text,
        intent_guess,
        location_token_guess,
        candidate_aliases,
        miss_type,
        recommended_fix,
        trace_id,
        reviewer_notes,
    )

def append_routing_slowpath_event(
    user_id: str,
    raw_text: str,
    normalized_text: str,
    detected_intent: str,
    candidate_locations: List[str],
    candidate_location_ids: List[str],
    reason_code: str,
    confidence: str,
    action_needed: str,
    trace_id: str,
    reviewer_notes: str = "",
) -> bool:
    return enqueue_async_log(
        ASYNC_LOG_LEVEL_AUDIT,
        trace_id,
        "append_routing_slowpath_event",
        _append_routing_slowpath_event_sync,
        user_id,
        raw_text,
        normalized_text,
        detected_intent,
        candidate_locations,
        candidate_location_ids,
        reason_code,
        confidence,
        action_needed,
        trace_id,
        reviewer_notes,
    )

def append_routing_shadow_suggestion(
    user_id: str,
    raw_text: str,
    normalized_text: str,
    intent_name: str,
    location_token_guess: str,
    suggested_alias: str,
    suggested_location_id: str,
    suggested_region_key: str,
    suggestion_source: str,
    suggestion_reason: str,
    confidence: str,
    trace_id: str,
    reviewer_notes: str = "",
    second_candidate_alias: str = "",
    second_candidate_location_id: str = "",
    score_gap: str = "",
    decision_context: str = "",
) -> bool:
    return enqueue_async_log(
        ASYNC_LOG_LEVEL_AUDIT,
        trace_id,
        "append_routing_shadow_suggestion",
        _append_routing_shadow_suggestion_sync,
        user_id,
        raw_text,
        normalized_text,
        intent_name,
        location_token_guess,
        suggested_alias,
        suggested_location_id,
        suggested_region_key,
        suggestion_source,
        suggestion_reason,
        confidence,
        trace_id,
        reviewer_notes,
        second_candidate_alias,
        second_candidate_location_id,
        score_gap,
        decision_context,
    )

def build_shadow_location_suggestion(intent_name: str, candidate_decision: dict, location_token_guess: str) -> Optional[dict]:
    token = safe_str(location_token_guess)
    if safe_str(intent_name) != "thue_phong_tro":
        return None
    candidates = candidate_decision.get("candidates") or []
    top = candidates[0] if candidates else {}
    second = candidates[1] if len(candidates) > 1 else {}
    top_alias = safe_str(top.get("matched_alias"))
    top_location_id = safe_str(top.get("location_id"))
    top_region_key = safe_str(top.get("service_region_key"))
    top_score = int(candidate_decision.get("top_score", 0) or 0)
    second_score = int(candidate_decision.get("second_score", 0) or 0)
    gap = top_score - second_score
    second_alias = safe_str(second.get("matched_alias"))
    second_location_id = safe_str(second.get("location_id"))

    if top_alias and top_region_key and top_score >= 85:
        confidence = "medium" if gap < 10 else "high"
        reason = f"top candidate score {top_score}"
        if second_score:
            reason += f" but conflict gap only {gap}"
        return {
            "suggested_alias": top_alias,
            "suggested_location_id": top_location_id,
            "suggested_region_key": top_region_key,
            "suggestion_source": "candidate_engine",
            "suggestion_reason": reason,
            "confidence": confidence,
            "second_candidate_alias": second_alias,
            "second_candidate_location_id": second_location_id,
            "score_gap": str(gap) if second_score else "",
            "decision_context": (
                f"top candidate strong but conflict gap only {gap}" if second_score
                else f"top candidate strong with score {top_score}"
            ),
        }

    if token and top_alias and top_region_key and top_score >= 72:
        return {
            "suggested_alias": top_alias,
            "suggested_location_id": top_location_id,
            "suggested_region_key": top_region_key,
            "suggestion_source": "heuristic",
            "suggestion_reason": f"location token '{token}' is close to alias '{top_alias}' with score {top_score}",
            "confidence": "low" if top_score < 85 else "medium",
            "second_candidate_alias": second_alias,
            "second_candidate_location_id": second_location_id,
            "score_gap": str(gap) if second_score else "",
            "decision_context": "heuristic fuzzy match from location token",
        }

    return None


def ensure_routing_log_worksheet(trace_id: str):
    spreadsheet = open_routing_spreadsheet(trace_id)
    if not spreadsheet:
        return None
    try:
        ws = spreadsheet.worksheet(ROUTING_LOG_SHEET_NAME)
    except gspread.WorksheetNotFound:
        try:
            ws = spreadsheet.add_worksheet(title=ROUTING_LOG_SHEET_NAME, rows=5000, cols=6)
            append_row_guarded(ws, trace_id, locals().get("worksheet_name", getattr(ws, "title", "unknown")), ROUTING_LOG_HEADERS, value_input_option="USER_ENTERED")
            _ROUTING_LOG_WORKSHEET_READY_CACHE["verified"] = True
            _ROUTING_LOG_WORKSHEET_READY_CACHE["loaded_at_ts"] = _now_ts()
            logger.info(f"[{trace_id}] ROUTING_LOG_SHEET_CREATED worksheet_name={ROUTING_LOG_SHEET_NAME}")
            return ws
        except Exception as e:
            logger.exception(f"[{trace_id}] ROUTING_LOG_SHEET_CREATE_FAILED exception={type(e).__name__}:{e}")
            return None
    except Exception as e:
        logger.exception(f"[{trace_id}] ROUTING_LOG_SHEET_OPEN_FAILED exception={type(e).__name__}:{e}")
        return None

    verified = bool(_ROUTING_LOG_WORKSHEET_READY_CACHE.get("verified"))
    verified_loaded_at_ts = float(_ROUTING_LOG_WORKSHEET_READY_CACHE.get("loaded_at_ts", 0.0) or 0.0)
    if verified and _cache_is_fresh(verified_loaded_at_ts, GSHEET_VALUES_CACHE_TTL_SECONDS):
        logger.info(f"[{trace_id}] ROUTING_LOG_WORKSHEET_READY_CACHE_HIT worksheet_name={ROUTING_LOG_SHEET_NAME}")
        return ws

    values = get_all_values_safe(ws, trace_id, ROUTING_LOG_SHEET_NAME)
    if not values:
        try:
            append_row_guarded(ws, trace_id, locals().get("worksheet_name", getattr(ws, "title", "unknown")), ROUTING_LOG_HEADERS, value_input_option="USER_ENTERED")
            logger.info(f"[{trace_id}] ROUTING_LOG_HEADERS_INIT_OK")
        except Exception as e:
            logger.exception(f"[{trace_id}] ROUTING_LOG_HEADERS_INIT_FAILED exception={type(e).__name__}:{e}")
            return None

    _ROUTING_LOG_WORKSHEET_READY_CACHE["verified"] = True
    _ROUTING_LOG_WORKSHEET_READY_CACHE["loaded_at_ts"] = _now_ts()
    return ws

def _append_routing_log_event_sync(user_id: str, intent_name: str, service_row: dict, location_hint: str, location_id: str, message: str, trace_id: str) -> bool:
    ws = ensure_routing_log_worksheet(trace_id)
    if not ws:
        logger.error(f"[{trace_id}] ROUTING_LOG_APPEND_SKIPPED reason=worksheet_unavailable")
        return False
    resolved_location = safe_str(location_hint) or safe_str(service_row.get("service_region_key")) or safe_str(service_row.get("location"))
    row = [
        now_tw_iso(),
        get_current_tenant_id(),
        user_ref(user_id),
        safe_str(intent_name),
        safe_str(service_row.get("service_id")),
        resolved_location,
        message_fingerprint(message),
    ]
    try:
        append_row_guarded(ws, trace_id, locals().get("worksheet_name", getattr(ws, "title", "unknown")), row, value_input_option="USER_ENTERED")
        _invalidate_worksheet_caches(ROUTING_LOG_SHEET_NAME)
        logger.info(
            f"[{trace_id}] ROUTING_LOG_APPEND_OK user_ref={user_ref(user_id)} intent_name={safe_str(intent_name)} "
            f"service_id={safe_str(service_row.get('service_id'))} "
            f"location={json.dumps(resolved_location, ensure_ascii=False)} "
            f"location_id={json.dumps(safe_str(location_id), ensure_ascii=False)}"
        )
        return True
    except Exception as e:
        logger.exception(f"[{trace_id}] ROUTING_LOG_APPEND_FAILED exception={type(e).__name__}:{e}")
        return False

def log_routing_reply_result(
    trace_id: str,
    user_id: str,
    intent_name: str,
    service_row: dict,
    location_hint: str,
    location_id: str,
    reply_text: str,
    reply_ok: bool,
) -> None:
    resolved_location = safe_str(location_hint) or safe_str(service_row.get("service_region_key")) or safe_str(service_row.get("location"))
    logger.info(
        f"[{trace_id}] ROUTING_REPLY_RESULT "
        f"user_ref={user_ref(user_id)} "
        f"intent_name={safe_str(intent_name)} "
        f"service_id={safe_str(service_row.get('service_id'))} "
        f"location={json.dumps(resolved_location, ensure_ascii=False)} "
        f"location_id={json.dumps(safe_str(location_id), ensure_ascii=False)} "
        f"reply_ok={reply_ok} "
        f"reply_len={len(safe_str(reply_text))}"
    )


def build_routing_reply(service_row: dict, language_group: str, resolved_region_key: str = "") -> str:
    contact_id = safe_str(service_row.get("contact_id"))
    contact_link = safe_str(service_row.get("contact_link"))
    location = safe_str(resolved_region_key) or safe_str(service_row.get("service_region_key")) or safe_str(service_row.get("location"))
    scope = safe_str(service_row.get("service_scope"))
    service_name = safe_str(service_row.get("service_name"))

    location_vi = LOCATION_DISPLAY_MAP.get(location, location)

    normalized_scope = normalize_routing_text(scope)
    normalized_service_name = normalize_routing_text(service_name)

    if "phong" in normalized_scope or "phong" in normalized_service_name:
        title = f"Tìm phòng trọ tại {location_vi}"
    else:
        title = scope or service_name

    lines = []
    if title:
        lines.append(f"Dịch vụ: {title}")
    if location_vi:
        lines.append(f"Khu vực: {location_vi}")
    if contact_link:
        lines.append(f"Nhắn trực tiếp: {contact_link}")
    elif contact_id:
        lines.append(f"Liên hệ LINE: {contact_id}")

    return "\n".join(lines)[:LINE_TEXT_HARD_LIMIT]
def parse_sim_entities(text: str) -> Tuple[str, str, str]:
    normalized = normalize_routing_text(text)

    network = ""
    if any(_phrase_present(normalized, p) for p in ["chunghwa", "trung hoa", "trung hoa telecom", "中華", "中華電信"]):
        network = "Chunghwa"
    elif _phrase_present(normalized, "if"):
        network = "IF"
    elif _phrase_present(normalized, "ok"):
        network = "OK"

    duration = ""
    if any(_phrase_present(normalized, p) for p in ["12m", "12 thang", "12 tháng", "1 nam", "1 năm", "sim nam", "sim năm", "1 year", "mot nam", "mot năm"]):
        duration = "12m"
    elif any(_phrase_present(normalized, p) for p in ["6m", "6 thang", "6 tháng", "6 month", "sau thang", "sau tháng"]):
        duration = "6m"

    variant_type = "renew" if any(_phrase_present(normalized, p) for p in ["gia han", "gia han sim", "renew", "gia hạn", "延長"]) else "new"
    return network, duration, variant_type

def find_sim_variant(service_id: str, network: str, duration: str, variant_type: str, variant_rows: List[dict]) -> Optional[dict]:
    target_service_id = safe_str(service_id)
    target_network = safe_str(network).lower()
    target_duration = safe_str(duration).lower()
    target_type = safe_str(variant_type).lower()
    for row in variant_rows:
        if safe_str(row.get("service_id")) != target_service_id:
            continue
        if safe_str(row.get("network")).lower() != target_network:
            continue
        if safe_str(row.get("duration")).lower() != target_duration:
            continue
        if safe_str(row.get("type")).lower() != target_type:
            continue
        return row
    return None

def build_sim_variant_reply(service_row: dict, variant_row: dict, language_group: str) -> str:
    lang = normalize_language_group(language_group)
    prefix = ROUTING_REPLY_PREFIX_BY_LANGUAGE.get(lang, ROUTING_REPLY_PREFIX_BY_LANGUAGE["vi"])
    labels = ROUTING_LABELS_BY_LANGUAGE.get(lang, ROUTING_LABELS_BY_LANGUAGE["vi"])
    contact_id = safe_str(service_row.get("contact_id"))
    contact_link = safe_str(service_row.get("contact_link"))
    network = safe_str(variant_row.get("network"))
    duration = safe_str(variant_row.get("duration"))
    price = safe_str(variant_row.get("price"))
    variant_type = safe_str(variant_row.get("type")).lower()

    duration_label = "1 năm" if duration == "12m" else "6 tháng" if duration == "6m" else duration
    type_label = "gia hạn" if variant_type == "renew" else "sim mới"

    lines = [f"{labels['service']}: SIM {network} {duration_label} ({type_label})"]
    if price:
        lines.append(f"{labels['price']}: {price} TWD")
    if contact_link:
        lines.append(f"Nhắn trực tiếp: {contact_link}")
    else:
        lines.append(f"{prefix}: {contact_id}")
    return "\n".join(lines)[:LINE_TEXT_HARD_LIMIT]

def load_sim_variant_rows_fastpath(trace_id: str, variant_sheet_name: str) -> List[dict]:
    sheet_name = safe_str(variant_sheet_name) or SERVICE_VARIANT_MASTER_SHEET_NAME
    cached_sheet = safe_str(_SIM_FASTPATH_VARIANT_ROWS_CACHE.get("sheet_name"))
    loaded_at_ts = float(_SIM_FASTPATH_VARIANT_ROWS_CACHE.get("loaded_at_ts", 0.0) or 0.0)
    cached_rows = _SIM_FASTPATH_VARIANT_ROWS_CACHE.get("rows") or []
    if cached_sheet == sheet_name and cached_rows and _cache_is_fresh(loaded_at_ts, SIM_FASTPATH_VARIANT_CACHE_TTL_SECONDS):
        logger.info(f"[{trace_id}] SIM_FASTPATH_VARIANT_CACHE_HIT worksheet_name={sheet_name} count={len(cached_rows)}")
        return cached_rows
    rows = load_routing_master_records(trace_id, sheet_name, SIM_FASTPATH_VARIANT_CACHE_TTL_SECONDS)
    if not rows:
        logger.error(f"[{trace_id}] SIM_FASTPATH_VARIANT_SHEET_UNAVAILABLE worksheet_name={sheet_name}")
        return cached_rows if cached_sheet == sheet_name else []
    if rows:
        _SIM_FASTPATH_VARIANT_ROWS_CACHE["rows"] = rows
        _SIM_FASTPATH_VARIANT_ROWS_CACHE["loaded_at_ts"] = _now_ts()
        _SIM_FASTPATH_VARIANT_ROWS_CACHE["sheet_name"] = sheet_name
        logger.info(f"[{trace_id}] SIM_FASTPATH_VARIANT_CACHE_READY worksheet_name={sheet_name} count={len(rows)}")
        return rows
    return cached_rows if cached_sheet == sheet_name else []


def try_build_sim_fastpath_reply(
    text: str,
    language_group: str,
    trace_id: str,
    user_id: str,
    service_rows: List[dict],
    variant_sheet_name: str,
    fallback_location: str,
    matched_keywords: List[str],
    normalized_text: str,
) -> Optional[dict]:
    if not SIM_FASTPATH_ENABLED:
        return None
    service = choose_service_for_intent("sim_mang_di_dong", safe_str(fallback_location) or ROUTING_FALLBACK_LOCATION, service_rows)
    if not service:
        service = choose_service_for_intent("sim_mang_di_dong", "", service_rows)
    if not service:
        append_routing_miss_event(
            user_id=user_id,
            raw_text=text,
            normalized_text=normalized_text,
            intent_guess="sim_mang_di_dong",
            location_token_guess="",
            candidate_aliases=[],
            miss_type="service_missing",
            recommended_fix="add_service_row",
            trace_id=trace_id,
        )
        logger.info(f"[{trace_id}] SIM_FASTPATH_SERVICE_MISS matched_keywords={json.dumps(matched_keywords, ensure_ascii=False)}")
        return {
            "reply_text": build_routing_smart_fallback_reply("sim_mang_di_dong", language_group, reason_code="missing_service_row"),
            "intent_name": "sim_mang_di_dong",
            "service_row": None,
            "location_hint": "",
            "location_id": "",
            "service_region_key": "",
            "matched_keywords": matched_keywords,
            "result_type": "smart_fallback",
        }

    service_id = safe_str(service.get("service_id"))
    service_region_key = safe_str(service.get("service_region_key")) or safe_str(service.get("location")) or safe_str(fallback_location) or ROUTING_FALLBACK_LOCATION
    final_reply_text = build_routing_reply(service, language_group, resolved_region_key=service_region_key)
    network, duration, variant_type = parse_sim_entities(text)

    if service_id == "SIM_TW_001" and network and duration:
        variant_rows = load_sim_variant_rows_fastpath(trace_id, variant_sheet_name)
        variant = find_sim_variant(service_id, network, duration, variant_type, variant_rows) if variant_rows else None
        if variant:
            logger.info(
                f"[{trace_id}] SIM_FASTPATH_VARIANT_MATCH_OK service_id={service_id} network={network} "
                f"duration={duration} type={variant_type} price={safe_str(variant.get('price'))}"
            )
            final_reply_text = build_sim_variant_reply(service, variant, language_group)
        else:
            append_routing_miss_event(
                user_id=user_id,
                raw_text=text,
                normalized_text=normalized_text,
                intent_guess="sim_mang_di_dong",
                location_token_guess="",
                candidate_aliases=[],
                miss_type="variant_missing",
                recommended_fix="add_variant_row",
                trace_id=trace_id,
            )
            final_reply_text = build_routing_smart_fallback_reply("sim_mang_di_dong", language_group, reason_code="missing_variant")
            logger.info(
                f"[{trace_id}] SIM_FASTPATH_VARIANT_MISS service_id={service_id} network={network} "
                f"duration={duration} type={variant_type}"
            )

    logger.info(
        f"[{trace_id}] SIM_FASTPATH_ROUTE_OK service_id={service_id} "
        f"service_region_key={json.dumps(service_region_key, ensure_ascii=False)} "
        f"network={json.dumps(network, ensure_ascii=False)} duration={json.dumps(duration, ensure_ascii=False)} "
        f"variant_type={json.dumps(variant_type, ensure_ascii=False)} "
        f"matched_keywords={json.dumps(matched_keywords, ensure_ascii=False)}"
    )
    return {
        "reply_text": final_reply_text,
        "intent_name": "sim_mang_di_dong",
        "service_row": service,
        "location_hint": service_region_key,
        "location_id": "",
        "service_region_key": service_region_key,
        "matched_keywords": matched_keywords,
        "result_type": "sim_fastpath",
    }


def select_candidate_aliases_for_reply(candidates: Optional[List[dict]] = None, max_items: int = 2, score_window: int = 5) -> List[str]:
    ranked = candidates or []
    if not ranked:
        return []
    top_score = int((ranked[0] or {}).get("score", 0) or 0)
    aliases: List[str] = []
    seen = set()
    for item in ranked:
        alias = safe_str(item.get("matched_alias")).strip()
        score = int(item.get("score", 0) or 0)
        if not alias:
            continue
        if score < (top_score - score_window):
            continue
        key = alias.lower()
        if key in seen:
            continue
        seen.add(key)
        aliases.append(alias)
        if len(aliases) >= max_items:
            break
    return aliases


def format_candidate_aliases_for_reply(candidate_aliases: Optional[List[str]] = None) -> str:
    aliases = []
    seen = set()
    for alias in candidate_aliases or []:
        cleaned = safe_str(alias).strip()
        if not cleaned:
            continue
        display = cleaned.title() if re.search(r"[a-zA-Z]", cleaned) else cleaned
        key = display.lower()
        if key in seen:
            continue
        seen.add(key)
        aliases.append(display)
    return " / ".join(aliases[:2])


def build_routing_smart_fallback_reply(intent_name: str, language_group: str, reason_code: str = "", location_token_guess: str = "", candidate_aliases: Optional[List[str]] = None) -> str:
    lang = normalize_language_group(language_group)
    token = safe_str(location_token_guess)
    candidate_hint = format_candidate_aliases_for_reply(candidate_aliases)
    if intent_name == "thue_phong_tro" and reason_code in {"missing_location", "weak_location_match", "multi_location"}:
        if reason_code == "multi_location" and candidate_hint:
            if lang == "id":
                return f"Saya menerima kebutuhan cari kamar, tetapi saat ini terdeteksi lebih dari satu area: {candidate_hint}. Kirim lagi satu area yang benar, nama distrik, atau lokasi dekat pabrik."[:LINE_TEXT_HARD_LIMIT]
            if lang == "th":
                return f"ฉันได้รับคำขอหาห้องแล้ว แต่ตอนนี้พบได้มากกว่าหนึ่งพื้นที่: {candidate_hint} กรุณาส่งมาใหม่เพียงหนึ่งพื้นที่ หรือจุดใกล้โรงงาน"[:LINE_TEXT_HARD_LIMIT]
            if lang == "zh":
                return f"我已收到找房需求，但目前辨識到不只一個地區：{candidate_hint}。請再發一次正確的單一地區，或工廠附近地點。"[:LINE_TEXT_HARD_LIMIT]
            return f"Tôi đã nhận nhu cầu tìm phòng, nhưng hiện đang thấy hơn 1 khu vực nghi vấn: {candidate_hint}. Bạn gửi lại đúng 1 khu vực, quận/huyện, hoặc địa danh gần nhà máy."[:LINE_TEXT_HARD_LIMIT]
        if lang == "id":
            return "Saya sudah menerima kebutuhan cari kamar, tetapi belum bisa memastikan area. Kirim lagi nama area, nama distrik, atau lokasi dekat pabrik."[:LINE_TEXT_HARD_LIMIT]
        if lang == "th":
            return "ฉันรับคำขอหาห้องแล้ว แต่ยังระบุพื้นที่ไม่ชัดเจน กรุณาส่งชื่อเขต พื้นที่ หรือจุดใกล้โรงงานอีกครั้ง"[:LINE_TEXT_HARD_LIMIT]
        if lang == "zh":
            return "我已收到找房需求，但目前還無法確認地區。請再發一次地區名稱、行政區，或工廠附近地點。"[:LINE_TEXT_HARD_LIMIT]
        if token:
            return f"Tôi đã nhận nhu cầu tìm phòng, nhưng chưa xác định được khu vực '{token}'. Bạn gửi lại tên khu vực, quận/huyện, hoặc địa danh gần nhà máy."[:LINE_TEXT_HARD_LIMIT]
        return "Tôi đã nhận nhu cầu tìm phòng, nhưng chưa xác định rõ khu vực. Bạn gửi lại tên khu vực, quận/huyện, hoặc địa danh gần nhà máy."[:LINE_TEXT_HARD_LIMIT]
    if intent_name == "sim_mang_di_dong" and reason_code in {"missing_variant", "missing_service_row"}:
        if lang == "id":
            return "Saya sudah menerima kebutuhan SIM, tetapi paketnya belum đủ rõ. Kirim lagi dengan format: jaringan + durasi, misalnya OK 6 bulan atau Chunghwa 12 bulan."[:LINE_TEXT_HARD_LIMIT]
        if lang == "th":
            return "ฉันรับคำขอซิมแล้ว แต่ข้อมูลแพ็กเกจยังไม่พอ กรุณาส่งใหม่ในรูปแบบ: เครือข่าย + ระยะเวลา เช่น OK 6 เดือน หรือ Chunghwa 12 เดือน"[:LINE_TEXT_HARD_LIMIT]
        if lang == "zh":
            return "我已收到 SIM 需求，但套餐資訊還不夠。請依格式重發：電信商 + 期限，例如 OK 6 個月 或 Chunghwa 12 個月。"[:LINE_TEXT_HARD_LIMIT]
        return "Tôi đã nhận nhu cầu SIM, nhưng chưa đủ thông tin gói. Bạn gửi lại theo mẫu: mạng + thời hạn, ví dụ OK 6 tháng hoặc Chunghwa 12 tháng."[:LINE_TEXT_HARD_LIMIT]
    if reason_code in {"multi_intent", "short_ambiguous_text", "intent_unclear", "fallback_triggered"}:
        if lang == "id":
            return "Tôi thấy bạn đang hỏi chưa đủ rõ hoặc nhiều nhu cầu cùng lúc. Bạn gửi từng nhu cầu riêng giúp tôi: phòng hoặc SIM trước."[:LINE_TEXT_HARD_LIMIT]
        if lang == "th":
            return "ข้อความนี้ยังไม่ชัดเจนหรือมีหลายความต้องการพร้อมกัน กรุณาส่งทีละเรื่องก่อน: ห้องพัก หรือ SIM"[:LINE_TEXT_HARD_LIMIT]
        if lang == "zh":
            return "你的需求目前還不夠明確，或同時包含多個需求。請先分開發送：找房或 SIM。"[:LINE_TEXT_HARD_LIMIT]
        return "Tôi thấy bạn đang hỏi chưa đủ rõ hoặc nhiều nhu cầu cùng lúc. Bạn gửi từng nhu cầu riêng giúp tôi: phòng hoặc SIM trước."[:LINE_TEXT_HARD_LIMIT]
    return "Tôi đã nhận yêu cầu, nhưng chưa đủ dữ liệu để xử lý chính xác. Bạn gửi lại rõ hơn giúp tôi."[:LINE_TEXT_HARD_LIMIT]

def try_build_routing_reply(text: str, language_group: str, trace_id: str, user_id: str = "") -> Optional[dict]:
    config_map = load_bot_config_map(trace_id)
    intent_sheet_name = safe_str(config_map.get("intent_sheet")) or INTENT_MASTER_SHEET_NAME
    service_sheet_name = safe_str(config_map.get("service_sheet")) or SERVICE_MASTER_SHEET_NAME
    alias_sheet_name = safe_str(config_map.get("location_alias_sheet")) or LOCATION_ALIAS_MASTER_SHEET_NAME
    canonical_sheet_name = safe_str(config_map.get("location_canonical_sheet"))
    region_map_sheet_name = safe_str(config_map.get("location_region_map_sheet"))
    variant_sheet_name = safe_str(config_map.get("variant_sheet")) or SERVICE_VARIANT_MASTER_SHEET_NAME
    fallback_location = safe_str(config_map.get("fallback_location")) or ROUTING_FALLBACK_LOCATION

    normalized_text = normalize_routing_text(text)

    intent_rows = load_routing_master_records(trace_id, intent_sheet_name, ROUTING_MASTER_CACHE_TTL_SECONDS)
    service_rows = load_routing_master_records(trace_id, service_sheet_name, ROUTING_MASTER_CACHE_TTL_SECONDS)
    if not intent_rows or not service_rows:
        logger.warning(
            f"[{trace_id}] ROUTING_MASTER_CACHE_UNAVAILABLE intent_rows={len(intent_rows)} "
            f"service_rows={len(service_rows)}"
        )
        return None

    intent_name, matched_keywords = detect_routing_intent(text, intent_rows)
    if not intent_name:
        append_routing_miss_event(
            user_id=user_id,
            raw_text=text,
            normalized_text=normalized_text,
            intent_guess="",
            location_token_guess="",
            candidate_aliases=[],
            miss_type="intent_unclear",
            recommended_fix="expand_intent_keywords",
            trace_id=trace_id,
        )
        logger.info(f"[{trace_id}] ROUTING_INTENT_MISS text_fp={message_fingerprint(text)}")
        return {
            "reply_text": build_routing_smart_fallback_reply("", language_group, reason_code="intent_unclear"),
            "intent_name": "",
            "service_row": None,
            "location_hint": "",
            "location_id": "",
            "service_region_key": "",
            "matched_keywords": [],
            "result_type": "smart_fallback",
        }

    if intent_name == "sim_mang_di_dong":
        sim_fastpath_result = try_build_sim_fastpath_reply(
            text=text,
            language_group=language_group,
            trace_id=trace_id,
            user_id=user_id,
            service_rows=service_rows,
            variant_sheet_name=variant_sheet_name,
            fallback_location=fallback_location,
            matched_keywords=matched_keywords,
            normalized_text=normalized_text,
        )
        if sim_fastpath_result:
            return sim_fastpath_result

    alias_rows = load_location_master_records(trace_id, alias_sheet_name, "alias")

    canonical_rows = []
    region_rows = []
    if alias_rows and any(safe_str(row.get("location_id")) for row in alias_rows):
        if canonical_sheet_name:
            canonical_rows = load_location_master_records(trace_id, canonical_sheet_name, "canonical")
        if region_map_sheet_name:
            region_rows = load_location_master_records(trace_id, region_map_sheet_name, "region_map")

    candidate_matches = collect_routing_location_candidates(text, alias_rows, matched_keywords) if alias_rows else []
    candidate_aliases = [safe_str(item.get("matched_alias")) for item in candidate_matches if safe_str(item.get("matched_alias"))]
    for candidate in candidate_matches:
        logger.info(
            f"[{trace_id}] ROUTING_CANDIDATE_EVAL "
            f"matched_alias={json.dumps(safe_str(candidate.get('matched_alias')), ensure_ascii=False)} "
            f"target_key={json.dumps(safe_str(candidate.get('target_key')), ensure_ascii=False)} "
            f"target_type={json.dumps(safe_str(candidate.get('target_type')), ensure_ascii=False)} "
            f"alias_type={json.dumps(safe_str(candidate.get('alias_type')), ensure_ascii=False)} "
            f"score={int(candidate.get('score', 0) or 0)}"
        )

    location_id = ""
    service_region_key = ""
    location_hint = ""
    location_token_guess = extract_location_token_guess(text, matched_keywords)
    smart_fallback_reply = ""

    candidate_decision = choose_best_location_candidate(candidate_matches, canonical_rows, region_rows) if candidate_matches else {
        "decision": "no_candidate",
        "confidence": "low",
        "selected_candidate": None,
        "top_score": 0,
        "second_score": 0,
        "candidates": [],
    }
    top_alias = safe_str((candidate_decision.get("candidates") or [{}])[0].get("matched_alias")) if candidate_decision.get("candidates") else ""
    second_alias = safe_str((candidate_decision.get("candidates") or [{}, {}])[1].get("matched_alias")) if len(candidate_decision.get("candidates") or []) > 1 else ""
    top_score = int(candidate_decision.get('top_score', 0) or 0)
    second_score = int(candidate_decision.get('second_score', 0) or 0)
    reply_candidate_aliases = select_candidate_aliases_for_reply(candidate_decision.get("candidates"), max_items=2, score_window=5)
    logger.info(
        f"[{trace_id}] ROUTING_CANDIDATE_DECISION "
        f"decision={safe_str(candidate_decision.get('decision'))} "
        f"confidence={safe_str(candidate_decision.get('confidence'))} "
        f"top_alias={json.dumps(top_alias, ensure_ascii=False)} "
        f"second_alias={json.dumps(second_alias, ensure_ascii=False)} "
        f"top_score={top_score} "
        f"second_score={second_score} "
        f"gap={top_score-second_score}"
    )

    selected_candidate = candidate_decision.get("selected_candidate") or {}
    if safe_str(candidate_decision.get("decision")) == "route":
        location_id = safe_str(selected_candidate.get("location_id"))
        service_region_key = safe_str(selected_candidate.get("service_region_key"))
        location_hint = service_region_key
        logger.info(
            f"[{trace_id}] ROUTING_LOCATION_V2_MATCH "
            f"matched_alias={json.dumps(safe_str(selected_candidate.get('matched_alias')), ensure_ascii=False)} "
            f"location_id={json.dumps(location_id, ensure_ascii=False)} "
            f"service_region_key={json.dumps(service_region_key, ensure_ascii=False)}"
        )
    elif safe_str(candidate_decision.get("decision")) == "slowpath" and candidate_matches:
        candidate_locations = [safe_str(item.get("service_region_key")) for item in candidate_decision.get("candidates", []) if safe_str(item.get("service_region_key"))]
        candidate_location_ids = [safe_str(item.get("location_id")) for item in candidate_decision.get("candidates", []) if safe_str(item.get("location_id"))]
        reason_code = "multi_location" if len(candidate_location_ids) > 1 else "weak_location_match"
        append_routing_slowpath_event(
            user_id=user_id,
            raw_text=text,
            normalized_text=normalized_text,
            detected_intent=intent_name,
            candidate_locations=candidate_locations,
            candidate_location_ids=candidate_location_ids,
            reason_code=reason_code,
            confidence="low",
            action_needed="review_alias",
            trace_id=trace_id,
        )
        shadow_suggestion = build_shadow_location_suggestion(intent_name, candidate_decision, location_token_guess)
        if shadow_suggestion:
            append_routing_shadow_suggestion(
                user_id=user_id,
                raw_text=text,
                normalized_text=normalized_text,
                intent_name=intent_name,
                location_token_guess=location_token_guess,
                suggested_alias=safe_str(shadow_suggestion.get("suggested_alias")),
                suggested_location_id=safe_str(shadow_suggestion.get("suggested_location_id")),
                suggested_region_key=safe_str(shadow_suggestion.get("suggested_region_key")),
                suggestion_source=safe_str(shadow_suggestion.get("suggestion_source")),
                suggestion_reason=safe_str(shadow_suggestion.get("suggestion_reason")),
                confidence=safe_str(shadow_suggestion.get("confidence")),
                second_candidate_alias=safe_str(shadow_suggestion.get("second_candidate_alias")),
                second_candidate_location_id=safe_str(shadow_suggestion.get("second_candidate_location_id")),
                score_gap=safe_str(shadow_suggestion.get("score_gap")),
                decision_context=safe_str(shadow_suggestion.get("decision_context")),
                trace_id=trace_id,
            )
        return {
            "reply_text": build_routing_smart_fallback_reply(
                intent_name,
                language_group,
                reason_code=reason_code,
                location_token_guess=location_token_guess,
                candidate_aliases=reply_candidate_aliases,
            ),
            "intent_name": intent_name,
            "service_row": None,
            "location_hint": "",
            "location_id": "",
            "service_region_key": "",
            "matched_keywords": matched_keywords,
            "result_type": "smart_fallback",
        }

    if not location_hint:
        location_hint = extract_location_alias(text, alias_rows)
        service_region_key = location_hint

    if intent_name == "thue_phong_tro" and not location_hint:
        smart_fallback_reply = build_routing_smart_fallback_reply(intent_name, language_group, reason_code="missing_location", location_token_guess=location_token_guess)
        append_routing_miss_event(
            user_id=user_id,
            raw_text=text,
            normalized_text=normalized_text,
            intent_guess=intent_name,
            location_token_guess=location_token_guess,
            candidate_aliases=candidate_aliases,
            miss_type="alias_missing",
            recommended_fix="add_alias",
            trace_id=trace_id,
        )
        append_routing_slowpath_event(
            user_id=user_id,
            raw_text=text,
            normalized_text=normalized_text,
            detected_intent=intent_name,
            candidate_locations=[],
            candidate_location_ids=[],
            reason_code="missing_location",
            confidence="low",
            action_needed="review_alias",
            trace_id=trace_id,
        )
        shadow_suggestion = build_shadow_location_suggestion(intent_name, candidate_decision, location_token_guess)
        if shadow_suggestion:
            append_routing_shadow_suggestion(
                user_id=user_id,
                raw_text=text,
                normalized_text=normalized_text,
                intent_name=intent_name,
                location_token_guess=location_token_guess,
                suggested_alias=safe_str(shadow_suggestion.get("suggested_alias")),
                suggested_location_id=safe_str(shadow_suggestion.get("suggested_location_id")),
                suggested_region_key=safe_str(shadow_suggestion.get("suggested_region_key")),
                suggestion_source=safe_str(shadow_suggestion.get("suggestion_source")),
                suggestion_reason=safe_str(shadow_suggestion.get("suggestion_reason")),
                confidence=safe_str(shadow_suggestion.get("confidence")),
                second_candidate_alias=safe_str(shadow_suggestion.get("second_candidate_alias")),
                second_candidate_location_id=safe_str(shadow_suggestion.get("second_candidate_location_id")),
                score_gap=safe_str(shadow_suggestion.get("score_gap")),
                decision_context=safe_str(shadow_suggestion.get("decision_context")),
                trace_id=trace_id,
            )

    service = choose_service_for_intent(intent_name, location_hint, service_rows)
    if not service and location_hint != fallback_location:
        service = choose_service_for_intent(intent_name, fallback_location, service_rows)
    if not service:
        if location_hint or location_id:
            append_routing_slowpath_event(
                user_id=user_id,
                raw_text=text,
                normalized_text=normalized_text,
                detected_intent=intent_name,
                candidate_locations=[service_region_key or location_hint] if (service_region_key or location_hint) else [],
                candidate_location_ids=[location_id] if location_id else [],
                reason_code="missing_service_row",
                confidence="medium" if location_hint else "low",
                action_needed="review_service",
                trace_id=trace_id,
            )
            append_routing_miss_event(
                user_id=user_id,
                raw_text=text,
                normalized_text=normalized_text,
                intent_guess=intent_name,
                location_token_guess=location_token_guess,
                candidate_aliases=candidate_aliases,
                miss_type="service_missing",
                recommended_fix="add_service_row",
                trace_id=trace_id,
            )
            shadow_suggestion = build_shadow_location_suggestion(intent_name, candidate_decision, location_token_guess)
            if shadow_suggestion:
                append_routing_shadow_suggestion(
                    user_id=user_id,
                    raw_text=text,
                    normalized_text=normalized_text,
                    intent_name=intent_name,
                    location_token_guess=location_token_guess,
                    suggested_alias=safe_str(shadow_suggestion.get("suggested_alias")),
                    suggested_location_id=safe_str(shadow_suggestion.get("suggested_location_id")),
                    suggested_region_key=safe_str(shadow_suggestion.get("suggested_region_key")),
                    suggestion_source=safe_str(shadow_suggestion.get("suggestion_source")),
                    suggestion_reason=safe_str(shadow_suggestion.get("suggestion_reason")),
                    confidence=safe_str(shadow_suggestion.get("confidence")),
                    second_candidate_alias=safe_str(shadow_suggestion.get("second_candidate_alias")),
                    second_candidate_location_id=safe_str(shadow_suggestion.get("second_candidate_location_id")),
                    score_gap=safe_str(shadow_suggestion.get("score_gap")),
                    decision_context=safe_str(shadow_suggestion.get("decision_context")),
                    trace_id=trace_id,
                )
        logger.info(
            f"[{trace_id}] ROUTING_SERVICE_MISS intent_name={intent_name} "
            f"location_hint={json.dumps(location_hint, ensure_ascii=False)} "
            f"location_id={json.dumps(location_id, ensure_ascii=False)} "
            f"matched_keywords={json.dumps(matched_keywords, ensure_ascii=False)}"
        )
        return {
            "reply_text": smart_fallback_reply or build_routing_smart_fallback_reply(intent_name, language_group, reason_code="fallback_triggered", location_token_guess=location_token_guess),
            "intent_name": intent_name,
            "service_row": None,
            "location_hint": location_hint,
            "location_id": location_id,
            "service_region_key": service_region_key,
            "matched_keywords": matched_keywords,
            "result_type": "smart_fallback",
        }

    logger.info(
        f"[{trace_id}] ROUTING_MATCH_OK intent_name={intent_name} "
        f"service_id={safe_str(service.get('service_id'))} "
        f"contact_id={safe_str(service.get('contact_id'))} "
        f"location_hint={json.dumps(location_hint, ensure_ascii=False)} "
        f"location_id={json.dumps(location_id, ensure_ascii=False)} "
        f"matched_keywords={json.dumps(matched_keywords, ensure_ascii=False)}"
    )

    service_id = safe_str(service.get("service_id"))
    final_reply_text = build_routing_reply(service, language_group, resolved_region_key=service_region_key)

    variant_rows = []
    if service_id == "SIM_TW_001":
        variant_ws = get_routing_worksheet_by_name(trace_id, variant_sheet_name)
        if variant_ws:
            variant_rows = get_records_safe(variant_ws, trace_id, variant_sheet_name)

    if service_id == "SIM_TW_001" and variant_rows:
        network, duration, variant_type = parse_sim_entities(text)
        if network and duration:
            variant = find_sim_variant(service_id, network, duration, variant_type, variant_rows)
            if variant:
                logger.info(
                    f"[{trace_id}] SIM_VARIANT_MATCH_OK service_id={service_id} network={network} "
                    f"duration={duration} type={variant_type} price={safe_str(variant.get('price'))}"
                )
                final_reply_text = build_sim_variant_reply(service, variant, language_group)
            else:
                append_routing_miss_event(
                    user_id=user_id,
                    raw_text=text,
                    normalized_text=normalized_text,
                    intent_guess=intent_name,
                    location_token_guess=location_token_guess,
                    candidate_aliases=candidate_aliases,
                    miss_type="variant_missing",
                    recommended_fix="add_variant_row",
                    trace_id=trace_id,
                )
                final_reply_text = build_routing_smart_fallback_reply(intent_name, language_group, reason_code="missing_variant")
                logger.info(
                    f"[{trace_id}] SIM_VARIANT_MISS service_id={service_id} network={network} "
                    f"duration={duration} type={variant_type}"
                )

    return {
        "reply_text": final_reply_text,
        "intent_name": intent_name,
        "service_row": service,
        "location_hint": location_hint,
        "location_id": location_id,
        "service_region_key": service_region_key,
        "matched_keywords": matched_keywords,
        "result_type": "routed",
    }


_ADS_CATALOG_CACHE = {"rows": [], "loaded_at_ts": 0, "last_read_ok": False}
_ADS_VIEW_CACHE: Dict[str, dict] = {}
_ADS_DETAIL_CACHE: Dict[str, dict] = {}
_WORKSPACE_VALIDATION_CACHE = {"result": None, "loaded_at_ts": 0}
_USER_FLOW_STATE: Dict[str, dict] = {}
USER_STATE_HEADERS = ["user_id", "flow", "updated_at", "language_group"]
PROCESSED_EVENT_HEADERS = ["event_key", "processed_at", "trace_id", "webhook_event_id", "message_id", "reply_token_hash", "user_ref", "event_type"]
EVENT_PROCESSING_LOCK_SECONDS = int(os.getenv("EVENT_PROCESSING_LOCK_SECONDS", "120").strip() or "120")
_USER_LANGUAGE_STATE: Dict[str, dict] = {}
_PROCESSED_EVENT_STATE: Dict[str, dict] = {}
LOCALIZED_TEXT = {
    "vi": {
        "busy": "Hệ thống bận, thử lại sau.",
        "worker_entry": "Đã vào worker flow. Gửi nội dung tiếp theo.",
        "worker_message": "Worker flow đã nhận: {text}",
        "ads_entry": "Đã vào ads flow. Gửi nội dung tiếp theo.",
        "ads_message": "Ads flow đã nhận: {text}",
        "ads_list_title": "Danh sách quảng cáo đang chạy:",
        "ads_empty": "Hiện chưa có quảng cáo phù hợp.",
        "ads_read_failed": "Đọc danh sách quảng cáo thất bại. Thử lại sau.",
        "ads_contact_label": "Liên hệ",
        "ads_id_label": "ID",
        "ads_detail_title": "Chi tiết quảng cáo số {index}:",
        "ads_select_invalid": "Số thứ tự không hợp lệ. Hãy chọn số trong danh sách vừa nhận.",
        "ads_select_expired": "Danh sách quảng cáo đã hết hạn. Gửi lại /ads để tải danh sách mới.",
        "ads_select_log_failed": "Ghi log click thất bại.",
        "reset": "Đã reset flow. Bạn có thể chọn lại /worker hoặc /ads.",
        "exit": "Đã thoát flow hiện tại.",
        "status_worker": "flow hiện tại: worker",
        "status_ads": "flow hiện tại: ads",
        "status_none": "flow hiện tại: none",
        "help_title": "lệnh hỗ trợ:",
        "lang_changed": "đã đổi ngôn ngữ: {lang}",
        "lang_invalid": "cú pháp đúng: /lang vi hoặc /lang id hoặc /lang th hoặc /lang zh",
        "default_echo": "Đã nhận yêu cầu. Gửi /help để xem lệnh hỗ trợ, hoặc dùng /worker, /ads, /lang.",
        "default_leave_intent": "Đã nhận nhóm nội dung nghỉ/phép. Nếu cần xử lý theo mẫu, gửi /worker rồi nhập nội dung chi tiết.",
        "default_health_intent": "Đã nhận nhóm nội dung sức khỏe. Nếu cần hỗ trợ theo mẫu, gửi /worker rồi nhập tình trạng cụ thể.",
        "default_travel_intent": "Đã nhận nhóm nội dung lịch trình/di chuyển. Nếu cần xử lý theo mẫu, gửi /worker rồi nhập nơi đi, nơi đến, thời gian.",
        "default_general_intent": "Đã nhận yêu cầu chung. Gửi /help để xem lệnh hỗ trợ, hoặc dùng /worker, /ads, /lang.",
        "state_save_failed": "Lưu trạng thái thất bại. Thử lại sau.",
        "state_clear_failed": "Xóa trạng thái thất bại. Thử lại sau.",
        "rich_menu_switch_failed": "Đã lưu ngôn ngữ nhưng đổi menu thất bại.",
    },
    "id": {
        "busy": "Sistem sedang sibuk, coba lagi nanti.",
        "worker_entry": "Masuk ke alur worker. Kirim isi berikutnya.",
        "worker_message": "Alur worker menerima: {text}",
        "ads_entry": "Masuk ke alur iklan. Kirim isi berikutnya.",
        "ads_message": "Alur iklan menerima: {text}",
        "ads_list_title": "Daftar iklan yang sedang aktif:",
        "ads_empty": "Belum ada iklan yang cocok saat ini.",
        "ads_read_failed": "Gagal membaca daftar iklan. Coba lagi nanti.",
        "ads_contact_label": "Kontak",
        "ads_id_label": "ID",
        "ads_detail_title": "Detail iklan nomor {index}:",
        "ads_select_invalid": "Nomor tidak valid. Pilih nomor dari daftar iklan terbaru.",
        "ads_select_expired": "Daftar iklan sudah kedaluwarsa. Kirim /ads untuk memuat ulang daftar baru.",
        "ads_select_log_failed": "Gagal mencatat log klik.",
        "reset": "Alur sudah direset. Anda bisa pilih lagi /worker atau /ads.",
        "exit": "Sudah keluar dari alur saat ini.",
        "status_worker": "alur saat ini: worker",
        "status_ads": "alur saat ini: ads",
        "status_none": "alur saat ini: none",
        "help_title": "perintah yang didukung:",
        "lang_changed": "bahasa diubah: {lang}",
        "lang_invalid": "format yang benar: /lang vi hoặc /lang id hoặc /lang th hoặc /lang zh",
        "default_echo": "Permintaan diterima. Kirim /help untuk melihat perintah, atau gunakan /worker, /ads, /lang.",
        "default_leave_intent": "Konten izin/cuti diterima. Jika ingin diproses dengan format kerja, kirim /worker lalu isi detailnya.",
        "default_health_intent": "Konten kesehatan diterima. Jika ingin diproses dengan format kerja, kirim /worker lalu isi kondisi detail.",
        "default_travel_intent": "Konten perjalanan diterima. Jika ingin diproses dengan format kerja, kirim /worker lalu isi asal, tujuan, dan waktu.",
        "default_general_intent": "Permintaan umum diterima. Gunakan /help untuk melihat perintah, atau /worker, /ads, /lang.",
        "state_save_failed": "Gagal menyimpan status. Coba lagi nanti.",
        "state_clear_failed": "Gagal menghapus status. Coba lagi nanti.",
        "rich_menu_switch_failed": "Bahasa tersimpan tetapi pergantian menu gagal.",
    },
    "th": {
        "busy": "ระบบกำลังยุ่ง กรุณาลองใหม่ภายหลัง",
        "worker_entry": "เข้าสู่โฟลว์ worker แล้ว ส่งข้อมูลถัดไปได้",
        "worker_message": "โฟลว์ worker ได้รับแล้ว: {text}",
        "ads_entry": "เข้าสู่โฟลว์โฆษณาแล้ว ส่งข้อมูลถัดไปได้",
        "ads_message": "โฟลว์โฆษณาได้รับแล้ว: {text}",
        "ads_list_title": "รายการโฆษณาที่กำลังใช้งาน:",
        "ads_empty": "ขณะนี้ยังไม่มีโฆษณาที่ตรงเงื่อนไข",
        "ads_read_failed": "อ่านรายการโฆษณาล้มเหลว กรุณาลองใหม่ภายหลัง",
        "ads_contact_label": "ติดต่อ",
        "ads_id_label": "รหัส",
        "ads_detail_title": "รายละเอียดโฆษณาหมายเลข {index}:",
        "ads_select_invalid": "หมายเลขไม่ถูกต้อง กรุณาเลือกหมายเลขจากรายการล่าสุด",
        "ads_select_expired": "รายการโฆษณาหมดอายุแล้ว กรุณาส่ง /ads เพื่อโหลดรายการใหม่",
        "ads_select_log_failed": "บันทึกคลิกไม่สำเร็จ",
        "reset": "รีเซ็ตโฟลว์แล้ว คุณสามารถเลือก /worker หรือ /ads ใหม่ได้",
        "exit": "ออกจากโฟลว์ปัจจุบันแล้ว",
        "status_worker": "โฟลว์ปัจจุบัน: worker",
        "status_ads": "โฟลว์ปัจจุบัน: ads",
        "status_none": "โฟลว์ปัจจุบัน: none",
        "help_title": "คำสั่งที่รองรับ:",
        "lang_changed": "เปลี่ยนภาษาแล้ว: {lang}",
        "lang_invalid": "รูปแบบที่ถูกต้อง: /lang vi หรือ /lang id หรือ /lang th หรือ /lang zh",
        "default_echo": "รับคำขอแล้ว ส่ง /help เพื่อดูคำสั่ง หรือใช้ /worker, /ads, /lang",
        "default_leave_intent": "ได้รับเนื้อหาเรื่องลางาน/หยุดงานแล้ว หากต้องการให้จัดการตามแบบงาน ให้ส่ง /worker แล้วส่งรายละเอียดต่อ",
        "default_health_intent": "ได้รับเนื้อหาเรื่องสุขภาพแล้ว หากต้องการให้จัดการตามแบบงาน ให้ส่ง /worker แล้วส่งอาการต่อ",
        "default_travel_intent": "ได้รับเนื้อหาเรื่องการเดินทางแล้ว หากต้องการให้จัดการตามแบบงาน ให้ส่ง /worker แล้วส่งต้นทาง ปลายทาง และเวลา",
        "default_general_intent": "ได้รับคำขอทั่วไปแล้ว ใช้ /help เพื่อดูคำสั่ง หรือใช้ /worker, /ads, /lang",
        "state_save_failed": "บันทึกสถานะล้มเหลว กรุณาลองใหม่ภายหลัง",
        "state_clear_failed": "ล้างสถานะล้มเหลว กรุณาลองใหม่ภายหลัง",
        "rich_menu_switch_failed": "บันทึกภาษาแล้ว แต่สลับเมนูไม่สำเร็จ",
    },
    "zh": {
        "busy": "系統忙碌中，請稍後再試。",
        "worker_entry": "已進入 worker flow，請發送下一段內容。",
        "worker_message": "worker flow 已收到：{text}",
        "ads_entry": "已進入廣告 flow，請發送下一段內容。",
        "ads_message": "廣告 flow 已收到：{text}",
        "ads_list_title": "目前有效廣告清單：",
        "ads_empty": "目前沒有符合條件的廣告。",
        "ads_read_failed": "讀取廣告清單失敗，請稍後再試。",
        "ads_contact_label": "聯絡人",
        "ads_id_label": "ID",
        "ads_detail_title": "廣告詳情 #{index}：",
        "ads_select_invalid": "編號無效，請從最新清單中選擇。",
        "ads_select_expired": "廣告清單已過期，請重新發送 /ads。",
        "ads_select_log_failed": "點擊紀錄失敗。",
        "reset": "流程已重置，你可以重新選擇 /worker 或 /ads。",
        "exit": "已退出目前流程。",
        "status_worker": "目前流程：worker",
        "status_ads": "目前流程：ads",
        "status_none": "目前流程：none",
        "help_title": "支援指令：",
        "lang_changed": "語言已切換：{lang}",
        "lang_invalid": "正確格式：/lang vi 或 /lang id 或 /lang th 或 /lang zh",
        "default_echo": "已收到需求。可發送 /help 查看指令，或使用 /worker、/ads、/lang。",
        "default_leave_intent": "已收到請假/休假類內容。若要依工作格式處理，請先發送 /worker 再補充細節。",
        "default_health_intent": "已收到健康狀況類內容。若要依工作格式處理，請先發送 /worker 再補充具體情況。",
        "default_travel_intent": "已收到行程/移動類內容。若要依工作格式處理，請先發送 /worker 再補充出發地、目的地、時間。",
        "default_general_intent": "已收到一般需求。發送 /help 查看指令，或使用 /worker、/ads、/lang。",
        "state_save_failed": "儲存狀態失敗，請稍後再試。",
        "state_clear_failed": "清除狀態失敗，請稍後再試。",
        "rich_menu_switch_failed": "語言已儲存，但切換選單失敗。",
    },
}
def t(language_group: str, key: str, **kwargs) -> str:
    lang = normalize_language_group(language_group)
    template = LOCALIZED_TEXT.get(lang, LOCALIZED_TEXT["vi"]).get(key, "")
    return template.format(**kwargs)
def make_processing_marker() -> str:
    return f"PROCESSING::{now_tw_iso()}"
def make_done_marker() -> str:
    return f"DONE::{now_tw_iso()}"
def make_failed_marker() -> str:
    return f"FAILED::{now_tw_iso()}"
def parse_processed_state_marker(value: str) -> Tuple[str, Optional[datetime]]:
    raw = safe_str(value)
    if not raw:
        return "", None
    if raw.startswith("PROCESSING::"):
        return "processing", parse_iso_datetime(raw.split("::", 1)[1])
    if raw.startswith("DONE::"):
        return "done", parse_iso_datetime(raw.split("::", 1)[1])
    if raw.startswith("FAILED::"):
        return "failed", parse_iso_datetime(raw.split("::", 1)[1])
    legacy_dt = parse_iso_datetime(raw)
    if legacy_dt:
        return "done", legacy_dt
    return "done", None
def is_processing_marker_fresh(marker_dt: Optional[datetime]) -> bool:
    if not marker_dt:
        return False
    age_seconds = int((now_tw_dt() - marker_dt).total_seconds())
    return age_seconds <= EVENT_PROCESSING_LOCK_SECONDS
# --- processed event state ---
def prune_processed_event_state(trace_id: str) -> None:
    now_ts = get_now_ts()
    expired_keys = []
    for event_key, item in list(_PROCESSED_EVENT_STATE.items()):
        if not isinstance(item, dict):
            expired_keys.append(event_key)
            continue
        updated_at_ts = int(item.get("updated_at_ts", 0) or 0)
        if updated_at_ts <= 0 or (now_ts - updated_at_ts) > PROCESSED_EVENT_TTL_SECONDS:
            expired_keys.append(event_key)
    for event_key in expired_keys:
        _PROCESSED_EVENT_STATE.pop(event_key, None)
    while len(_PROCESSED_EVENT_STATE) > PROCESSED_EVENT_MAX_KEYS:
        oldest_key = min(
            _PROCESSED_EVENT_STATE.keys(),
            key=lambda x: int((_PROCESSED_EVENT_STATE.get(x) or {}).get("updated_at_ts", 0) or 0),
        )
        _PROCESSED_EVENT_STATE.pop(oldest_key, None)
    if expired_keys:
        logger.info(f"[{trace_id}] PROCESSED_EVENT_STATE_PRUNED removed={len(expired_keys)}")
def ensure_processed_event_worksheet(trace_id: str):
    spreadsheet = open_spreadsheet(trace_id)
    if not spreadsheet:
        return None
    try:
        ws = spreadsheet.worksheet(PROCESSED_EVENT_SHEET_NAME)
    except gspread.WorksheetNotFound:
        try:
            ws = spreadsheet.add_worksheet(title=PROCESSED_EVENT_SHEET_NAME, rows=5000, cols=10)
            append_row_guarded(ws, trace_id, locals().get("worksheet_name", getattr(ws, "title", "unknown")), PROCESSED_EVENT_HEADERS, value_input_option="USER_ENTERED")
            logger.info(f"[{trace_id}] PROCESSED_EVENT_SHEET_CREATED worksheet_name={PROCESSED_EVENT_SHEET_NAME}")
            return ws
        except Exception as e:
            logger.exception(f"[{trace_id}] PROCESSED_EVENT_SHEET_CREATE_FAILED exception={type(e).__name__}:{e}")
            return None
    except Exception as e:
        logger.exception(f"[{trace_id}] PROCESSED_EVENT_SHEET_OPEN_FAILED exception={type(e).__name__}:{e}")
        return None
    values = get_all_values_safe(ws, trace_id, PROCESSED_EVENT_SHEET_NAME)
    if not values:
        try:
            append_row_guarded(ws, trace_id, locals().get("worksheet_name", getattr(ws, "title", "unknown")), PROCESSED_EVENT_HEADERS, value_input_option="USER_ENTERED")
            logger.info(f"[{trace_id}] PROCESSED_EVENT_HEADERS_INIT_OK")
        except Exception as e:
            logger.exception(f"[{trace_id}] PROCESSED_EVENT_HEADERS_INIT_FAILED exception={type(e).__name__}:{e}")
            return None
        return ws
    return ws
def get_event_unique_key(event: dict) -> str:
    tenant_id = resolve_tenant_id_from_event(event)
    webhook_event_id = safe_str(event.get("webhookEventId"))
    if webhook_event_id:
        return f"{tenant_id}:webhook:{webhook_event_id}"
    message = event.get("message") or {}
    message_id = safe_str(message.get("id"))
    if message_id:
        return f"{tenant_id}:message:{message_id}"
    reply_token = safe_str(event.get("replyToken"))
    if reply_token:
        return f"{tenant_id}:reply:{stable_hash(reply_token)}"
    return ""
def get_persistent_processed_event_record(event_key: str, trace_id: str) -> dict:
    empty_result = {"status": "", "row_index": 0, "processed_at_value": ""}
    if not event_key:
        logger.info(f"[{trace_id}] PROCESSED_EVENT_PERSIST_LOOKUP_SKIPPED reason=missing_event_key")
        return empty_result
    ws = ensure_processed_event_worksheet(trace_id)
    if not ws:
        logger.error(f"[{trace_id}] PROCESSED_EVENT_PERSIST_LOOKUP_SKIPPED reason=worksheet_unavailable event_key={event_key}")
        return empty_result
    values = get_all_values_safe(ws, trace_id, PROCESSED_EVENT_SHEET_NAME)
    if not values:
        return empty_result
    headers = values[0]
    header_map = build_header_index_map(headers)
    event_key_idx = header_map.get("event_key")
    processed_at_idx = header_map.get("processed_at")
    if event_key_idx is None or processed_at_idx is None:
        logger.error(f"[{trace_id}] PROCESSED_EVENT_PERSIST_LOOKUP_COLUMN_MISSING event_key={event_key}")
        return empty_result
    for row_index, row in enumerate(values[1:], start=2):
        current_event_key = safe_str(row[event_key_idx]) if event_key_idx < len(row) else ""
        if current_event_key != event_key:
            continue
        processed_at_value = safe_str(row[processed_at_idx]) if processed_at_idx < len(row) else ""
        marker_status, marker_dt = parse_processed_state_marker(processed_at_value)
        if marker_status == "processing":
            status = "processing" if is_processing_marker_fresh(marker_dt) else "stale_processing"
        else:
            status = marker_status
        logger.info(f"[{trace_id}] PROCESSED_EVENT_PERSIST_LOOKUP event_key={event_key} status={status} row_index={row_index}")
        return {"status": status, "row_index": row_index, "processed_at_value": processed_at_value}
    logger.info(f"[{trace_id}] PROCESSED_EVENT_PERSIST_LOOKUP_MISS event_key={event_key}")
    return empty_result
def set_processed_event_runtime_state(event_key: str, status: str, trace_id: str) -> None:
    if not event_key:
        return
    prune_processed_event_state(trace_id)
    _PROCESSED_EVENT_STATE[event_key] = {"status": safe_str(status), "updated_at_ts": get_now_ts()}
    logger.info(f"[{trace_id}] PROCESSED_EVENT_RUNTIME_STATE_SET event_key={event_key} status={status}")
def clear_processed_event_runtime_state(event_key: str, trace_id: str) -> None:
    if not event_key:
        return
    existed = event_key in _PROCESSED_EVENT_STATE
    _PROCESSED_EVENT_STATE.pop(event_key, None)
    logger.info(f"[{trace_id}] PROCESSED_EVENT_RUNTIME_STATE_CLEARED event_key={event_key} existed={existed}")
def begin_event_processing(event: dict, trace_id: str) -> Tuple[bool, str, str]:
    event_key = get_event_unique_key(event)
    if not event_key:
        logger.info(f"[{trace_id}] EVENT_PROCESSING_BEGIN_SKIPPED reason=missing_event_key")
        return True, "missing_event_key", ""
    prune_processed_event_state(trace_id)
    runtime_item = _PROCESSED_EVENT_STATE.get(event_key) or {}
    runtime_status = safe_str(runtime_item.get("status"))
    runtime_updated_at_ts = int(runtime_item.get("updated_at_ts", 0) or 0)
    if runtime_status == "done":
        logger.info(f"[{trace_id}] EVENT_PROCESSING_BEGIN_DUPLICATE event_key={event_key} source=runtime_done")
        return False, "duplicate_done_runtime", event_key
    if runtime_status == "processing" and (get_now_ts() - runtime_updated_at_ts) <= EVENT_PROCESSING_LOCK_SECONDS:
        logger.info(f"[{trace_id}] EVENT_PROCESSING_BEGIN_DUPLICATE event_key={event_key} source=runtime_processing")
        return False, "duplicate_processing_runtime", event_key
    lookup = get_persistent_processed_event_record(event_key, trace_id)
    persistent_status = safe_str(lookup.get("status"))
    row_index = int(lookup.get("row_index", 0) or 0)
    ws = ensure_processed_event_worksheet(trace_id)
    if not ws:
        logger.error(f"[{trace_id}] EVENT_PROCESSING_BEGIN_FAILED reason=worksheet_unavailable event_key={event_key}")
        return False, "worksheet_unavailable", event_key
    if persistent_status == "done":
        logger.info(f"[{trace_id}] EVENT_PROCESSING_BEGIN_DUPLICATE event_key={event_key} source=persistent_done")
        set_processed_event_runtime_state(event_key, "done", trace_id)
        return False, "duplicate_done_persistent", event_key
    if persistent_status == "processing":
        logger.info(f"[{trace_id}] EVENT_PROCESSING_BEGIN_DUPLICATE event_key={event_key} source=persistent_processing")
        set_processed_event_runtime_state(event_key, "processing", trace_id)
        return False, "duplicate_processing_persistent", event_key
    message = event.get("message") or {}
    marker = make_processing_marker()
    row_payload = {
        "processed_at": marker,
        "trace_id": trace_id,
        "webhook_event_id": safe_str(event.get("webhookEventId")),
        "message_id": safe_str(message.get("id")),
        "reply_token_hash": stable_hash(safe_str(event.get("replyToken"))),
        "user_ref": user_ref(get_event_user_id(event)),
        "event_type": get_event_type(event),
    }
    if row_index > 0:
        persist_ok = update_row_fields_by_header(ws, row_index, row_payload, trace_id, PROCESSED_EVENT_SHEET_NAME)
    else:
        row = [
            event_key,
            marker,
            trace_id,
            safe_str(event.get("webhookEventId")),
            safe_str(message.get("id")),
            safe_str(event.get("replyToken")),
            get_event_user_id(event),
            get_event_type(event),
        ]
        try:
            append_row_guarded(ws, trace_id, locals().get("worksheet_name", getattr(ws, "title", "unknown")), row, value_input_option="USER_ENTERED")
            _invalidate_worksheet_caches(PROCESSED_EVENT_SHEET_NAME)
            logger.info(f"[{trace_id}] PROCESSED_EVENT_PROCESSING_APPEND_OK event_key={event_key}")
            persist_ok = True
        except Exception as e:
            logger.exception(f"[{trace_id}] PROCESSED_EVENT_PROCESSING_APPEND_FAILED event_key={event_key} exception={type(e).__name__}:{e}")
            persist_ok = False
    if not persist_ok:
        return False, "processing_marker_write_failed", event_key
    set_processed_event_runtime_state(event_key, "processing", trace_id)
    logger.info(f"[{trace_id}] EVENT_PROCESSING_BEGIN_OK event_key={event_key} source={'reclaim' if row_index > 0 else 'new'}")
    return True, "processing_started", event_key
def persist_event_processing_finalize(event: dict, trace_id: str, success: bool) -> bool:
    event_key = get_event_unique_key(event)
    if not event_key:
        logger.info(f"[{trace_id}] EVENT_PROCESSING_FINALIZE_PERSIST_SKIPPED reason=missing_event_key")
        return False
    lookup = get_persistent_processed_event_record(event_key, trace_id)
    row_index = int(lookup.get("row_index", 0) or 0)
    ws = ensure_processed_event_worksheet(trace_id)
    marker = make_done_marker() if success else make_failed_marker()
    persist_ok = False
    if ws and row_index > 0:
        persist_ok = update_row_fields_by_header(
            ws,
            row_index,
            {"processed_at": marker, "trace_id": trace_id},
            trace_id,
            PROCESSED_EVENT_SHEET_NAME,
        )
    elif ws and not row_index and success:
        message = event.get("message") or {}
        row = [
            event_key,
            marker,
            trace_id,
            safe_str(event.get("webhookEventId")),
            safe_str(message.get("id")),
            safe_str(event.get("replyToken")),
            get_event_user_id(event),
            get_event_type(event),
        ]
        try:
            append_row_guarded(
                ws,
                trace_id,
                locals().get("worksheet_name", getattr(ws, "title", "unknown")),
                row,
                value_input_option="USER_ENTERED",
            )
            _invalidate_worksheet_caches(PROCESSED_EVENT_SHEET_NAME)
            persist_ok = True
        except Exception as e:
            logger.exception(f"[{trace_id}] EVENT_PROCESSING_FINALIZE_APPEND_FAILED event_key={event_key} exception={type(e).__name__}:{e}")
            persist_ok = False
    logger.info(f"[{trace_id}] EVENT_PROCESSING_FINALIZE_PERSIST_DONE event_key={event_key} success={success} persist_ok={persist_ok}")
    return persist_ok


def finalize_event_processing(event: dict, trace_id: str, success: bool) -> None:
    event_key = get_event_unique_key(event)
    if not event_key:
        logger.info(f"[{trace_id}] EVENT_PROCESSING_FINALIZE_SKIPPED reason=missing_event_key")
        return

    target_status = "done" if success else "failed"

    if success:
        set_processed_event_runtime_state(event_key, target_status, trace_id)
    else:
        clear_processed_event_runtime_state(event_key, trace_id)

    if EVENT_STATE_FAST_FINALIZE_ENABLED and success:
        event_snapshot = json.loads(json.dumps(event, ensure_ascii=False))
        queued = enqueue_async_log(
            ASYNC_LOG_LEVEL_AUDIT,
            trace_id,
            "persist_event_processing_finalize",
            persist_event_processing_finalize,
            event_snapshot,
            trace_id,
            success,
        )
        if queued:
            logger.info(f"[{trace_id}] EVENT_PROCESSING_FAST_FINALIZE_ENQUEUED event_key={event_key} success={success}")
            logger.info(f"[{trace_id}] EVENT_PROCESSING_FINALIZED event_key={event_key} success={success} persist_ok=queued")
            return
        logger.error(f"[{trace_id}] EVENT_PROCESSING_FAST_FINALIZE_QUEUE_FAILED event_key={event_key} fallback=sync")

    persist_ok = persist_event_processing_finalize(event, trace_id, success)
    logger.info(f"[{trace_id}] EVENT_PROCESSING_FINALIZED event_key={event_key} success={success} persist_ok={persist_ok}")

# --- language state ---
def prune_runtime_user_language_state(trace_id: str) -> None:
    now_ts = get_now_ts()
    expired_keys = []
    for user_id, item in list(_USER_LANGUAGE_STATE.items()):
        updated_at_ts = int(item.get("updated_at_ts", 0) or 0)
        if updated_at_ts <= 0 or (now_ts - updated_at_ts) > RUNTIME_STATE_TTL_SECONDS:
            expired_keys.append(user_id)
    for user_id in expired_keys:
        _USER_LANGUAGE_STATE.pop(user_id, None)
    while len(_USER_LANGUAGE_STATE) > RUNTIME_STATE_MAX_KEYS:
        oldest_key = min(_USER_LANGUAGE_STATE.keys(), key=lambda x: int(_USER_LANGUAGE_STATE[x].get("updated_at_ts", 0) or 0))
        _USER_LANGUAGE_STATE.pop(oldest_key, None)
    if expired_keys:
        logger.info(f"[{trace_id}] USER_LANGUAGE_STATE_PRUNED removed={len(expired_keys)}")
def get_runtime_user_language(user_id: str, trace_id: str) -> str:
    prune_runtime_user_language_state(trace_id)
    normalized_user_id = tenant_scope_key(user_id)
    item = _USER_LANGUAGE_STATE.get(normalized_user_id)
    if not item:
        logger.info(f"[{trace_id}] USER_LANGUAGE_STATE_MISS user_ref={user_ref(user_id)}")
        return ""
    language_group = safe_str(item.get("language_group"))
    logger.info(f"[{trace_id}] USER_LANGUAGE_STATE_HIT user_ref={user_ref(user_id)} language_group={language_group}")
    return language_group
def set_runtime_user_language(user_id: str, language_group: str, trace_id: str) -> None:
    normalized_user_id = tenant_scope_key(user_id)
    normalized_language = normalize_language_group(language_group)
    if not normalized_user_id:
        logger.error(f"[{trace_id}] USER_LANGUAGE_STATE_SET_SKIPPED reason=missing_user_id")
        return
    prune_runtime_user_language_state(trace_id)
    _USER_LANGUAGE_STATE[normalized_user_id] = {"language_group": normalized_language, "updated_at_ts": get_now_ts()}
    logger.info(f"[{trace_id}] USER_LANGUAGE_STATE_SET user_ref={user_ref(user_id)} language_group={normalized_language}")
def ensure_user_language_worksheet(trace_id: str):
    return ensure_user_state_worksheet(trace_id)
def get_persistent_user_language(user_id: str, trace_id: str) -> str:
    normalized_user_id = tenant_scope_key(user_id)
    if not normalized_user_id:
        return ""
    ws = ensure_user_language_worksheet(trace_id)
    if not ws:
        logger.error(f"[{trace_id}] USER_LANGUAGE_PERSIST_READ_SKIPPED reason=worksheet_unavailable")
        return ""
    records = get_records_safe(ws, trace_id, USER_STATE_SHEET_NAME)
    for row in records:
        if safe_str(row.get("user_scope_key") or row.get("user_id")) == normalized_user_id:
            language_group = normalize_language_group(row.get("language_group"))
            logger.info(f"[{trace_id}] USER_LANGUAGE_PERSIST_HIT user_ref={user_ref(user_id)} language_group={language_group}")
            return language_group
    logger.info(f"[{trace_id}] USER_LANGUAGE_PERSIST_MISS user_ref={user_ref(user_id)}")
    return ""
def set_persistent_user_language(user_id: str, language_group: str, trace_id: str) -> bool:
    normalized_user_id = tenant_scope_key(user_id)
    normalized_language = normalize_language_group(language_group)
    if not normalized_user_id:
        logger.error(f"[{trace_id}] USER_LANGUAGE_PERSIST_SET_SKIPPED reason=missing_user_id")
        return False
    ws = ensure_user_language_worksheet(trace_id)
    if not ws:
        logger.error(f"[{trace_id}] USER_LANGUAGE_PERSIST_SET_SKIPPED reason=worksheet_unavailable")
        return False
    row_index = find_first_row_index_by_column_value(ws=ws, column_name="user_id", expected_value=normalized_user_id, trace_id=trace_id, worksheet_name=USER_STATE_SHEET_NAME)
    now_iso = now_tw_iso()
    if row_index:
        return update_row_fields_by_header(ws, row_index, {"language_group": normalized_language, "updated_at": now_iso}, trace_id, USER_STATE_SHEET_NAME)
    try:
        append_row_guarded(ws, trace_id, USER_STATE_SHEET_NAME, [normalized_user_id, "", now_iso, normalized_language], value_input_option="USER_ENTERED")
        logger.info(f"[{trace_id}] USER_LANGUAGE_PERSIST_APPEND_OK user_ref={user_ref(user_id)} language_group={normalized_language}")
        return True
    except Exception as e:
        logger.exception(f"[{trace_id}] USER_LANGUAGE_PERSIST_APPEND_FAILED exception={type(e).__name__}:{e}")
        return False
def resolve_user_language(user_id: str, trace_id: str) -> str:
    runtime_language = get_runtime_user_language(user_id, trace_id)
    if runtime_language:
        return runtime_language
    persistent_language = get_persistent_user_language(user_id, trace_id)
    if persistent_language:
        set_runtime_user_language(user_id, persistent_language, trace_id)
        logger.info(f"[{trace_id}] USER_LANGUAGE_RESTORED_FROM_SHEET user_ref={user_ref(user_id)} language_group={persistent_language}")
        return persistent_language
    return DEFAULT_LANGUAGE_GROUP
def persist_user_language(user_id: str, language_group: str, trace_id: str) -> bool:
    normalized_language = normalize_language_group(language_group)
    persist_ok = set_persistent_user_language(user_id, normalized_language, trace_id)
    if persist_ok:
        set_runtime_user_language(user_id, normalized_language, trace_id)
    else:
        _USER_LANGUAGE_STATE.pop(tenant_scope_key(user_id), None)
    logger.info(f"[{trace_id}] USER_LANGUAGE_PERSIST_RESULT user_ref={user_ref(user_id)} language_group={normalized_language} ok={persist_ok}")
    return persist_ok
# --- user state ---
def ensure_user_state_worksheet(trace_id: str):
    spreadsheet = open_spreadsheet(trace_id)
    if not spreadsheet:
        return None
    try:
        ws = spreadsheet.worksheet(USER_STATE_SHEET_NAME)
    except gspread.WorksheetNotFound:
        try:
            ws = spreadsheet.add_worksheet(title=USER_STATE_SHEET_NAME, rows=1000, cols=6)
            append_row_guarded(ws, trace_id, locals().get("worksheet_name", getattr(ws, "title", "unknown")), USER_STATE_HEADERS, value_input_option="USER_ENTERED")
            logger.info(f"[{trace_id}] USER_STATE_SHEET_CREATED worksheet_name={USER_STATE_SHEET_NAME}")
            return ws
        except Exception as e:
            logger.exception(f"[{trace_id}] USER_STATE_SHEET_CREATE_FAILED exception={type(e).__name__}:{e}")
            return None
    except Exception as e:
        logger.exception(f"[{trace_id}] USER_STATE_SHEET_OPEN_FAILED exception={type(e).__name__}:{e}")
        return None
    values = get_all_values_safe(ws, trace_id, USER_STATE_SHEET_NAME)
    if not values:
        try:
            append_row_guarded(ws, trace_id, locals().get("worksheet_name", getattr(ws, "title", "unknown")), USER_STATE_HEADERS, value_input_option="USER_ENTERED")
            logger.info(f"[{trace_id}] USER_STATE_HEADERS_INIT_OK")
        except Exception as e:
            logger.exception(f"[{trace_id}] USER_STATE_HEADERS_INIT_FAILED exception={type(e).__name__}:{e}")
            return None
        return ws
    return ws
def get_persistent_user_flow(user_id: str, trace_id: str) -> str:
    normalized_user_id = tenant_scope_key(user_id)
    if not normalized_user_id:
        return ""
    ws = ensure_user_state_worksheet(trace_id)
    if not ws:
        logger.error(f"[{trace_id}] USER_STATE_PERSIST_READ_SKIPPED reason=worksheet_unavailable")
        return ""
    records = get_records_safe(ws, trace_id, USER_STATE_SHEET_NAME)
    for row in records:
        if safe_str(row.get("user_scope_key") or row.get("user_id")) != normalized_user_id:
            continue
        flow = safe_str(row.get("flow"))
        updated_at = safe_str(row.get("updated_at"))
        if flow and not is_supported_flow(flow):
            logger.warning(f"[{trace_id}] USER_STATE_PERSIST_UNSUPPORTED_FLOW user_ref={user_ref(user_id)} flow={flow}")
            clear_persistent_user_flow(user_id, trace_id)
            return ""
        if flow and is_persistent_flow_expired(updated_at):
            logger.info(f"[{trace_id}] USER_STATE_PERSIST_EXPIRED user_ref={user_ref(user_id)} flow={flow} updated_at={json.dumps(updated_at, ensure_ascii=False)} ttl_seconds={PERSISTENT_FLOW_TTL_SECONDS}")
            clear_persistent_user_flow(user_id, trace_id)
            return ""
        logger.info(f"[{trace_id}] USER_STATE_PERSIST_HIT user_ref={user_ref(user_id)} flow={flow}")
        return flow
    logger.info(f"[{trace_id}] USER_STATE_PERSIST_MISS user_ref={user_ref(user_id)}")
    return ""
def set_persistent_user_flow(user_id: str, flow: str, trace_id: str) -> bool:
    normalized_user_id = tenant_scope_key(user_id)
    normalized_flow = safe_str(flow)
    if not normalized_user_id:
        logger.error(f"[{trace_id}] USER_STATE_PERSIST_SET_SKIPPED reason=missing_user_id")
        return False
    ws = ensure_user_state_worksheet(trace_id)
    if not ws:
        logger.error(f"[{trace_id}] USER_STATE_PERSIST_SET_SKIPPED reason=worksheet_unavailable")
        return False
    row_index = find_first_row_index_by_column_value(ws=ws, column_name="user_id", expected_value=normalized_user_id, trace_id=trace_id, worksheet_name=USER_STATE_SHEET_NAME)
    now_iso = now_tw_iso()
    if row_index:
        return update_row_fields_by_header(ws, row_index, {"flow": normalized_flow, "updated_at": now_iso}, trace_id, USER_STATE_SHEET_NAME)
    try:
        append_row_guarded(ws, trace_id, USER_STATE_SHEET_NAME, [normalized_user_id, normalized_flow, now_iso, ""], value_input_option="USER_ENTERED")
        logger.info(f"[{trace_id}] USER_STATE_PERSIST_APPEND_OK user_ref={user_ref(user_id)} flow={normalized_flow}")
        return True
    except Exception as e:
        logger.exception(f"[{trace_id}] USER_STATE_PERSIST_APPEND_FAILED exception={type(e).__name__}:{e}")
        return False
def clear_persistent_user_flow(user_id: str, trace_id: str) -> bool:
    normalized_user_id = tenant_scope_key(user_id)
    if not normalized_user_id:
        logger.error(f"[{trace_id}] USER_STATE_PERSIST_CLEAR_SKIPPED reason=missing_user_id")
        return False
    ws = ensure_user_state_worksheet(trace_id)
    if not ws:
        logger.error(f"[{trace_id}] USER_STATE_PERSIST_CLEAR_SKIPPED reason=worksheet_unavailable")
        return False
    row_index = find_first_row_index_by_column_value(ws=ws, column_name="user_id", expected_value=normalized_user_id, trace_id=trace_id, worksheet_name=USER_STATE_SHEET_NAME)
    if not row_index:
        logger.info(f"[{trace_id}] USER_STATE_PERSIST_CLEAR_MISS user_ref={user_ref(user_id)}")
        return True
    ok = update_row_fields_by_header(ws, row_index, {"flow": "", "updated_at": now_tw_iso()}, trace_id, USER_STATE_SHEET_NAME)
    if ok:
        logger.info(f"[{trace_id}] USER_STATE_PERSIST_CLEAR_OK user_ref={user_ref(user_id)} row_index={row_index}")
    return ok
def clear_runtime_user_flow(user_id: str, trace_id: str) -> None:
    normalized_user_id = tenant_scope_key(user_id)
    if not normalized_user_id:
        logger.error(f"[{trace_id}] USER_FLOW_STATE_CLEAR_SKIPPED reason=missing_user_id")
        return
    existed = normalized_user_id in _USER_FLOW_STATE
    _USER_FLOW_STATE.pop(normalized_user_id, None)
    logger.info(f"[{trace_id}] USER_FLOW_STATE_CLEARED user_ref={user_ref(user_id)} existed={existed}")
def clear_user_flow(user_id: str, trace_id: str) -> bool:
    persist_ok = clear_persistent_user_flow(user_id, trace_id)
    if persist_ok:
        clear_runtime_user_flow(user_id, trace_id)
    logger.info(f"[{trace_id}] USER_STATE_CLEAR_RESULT user_ref={user_ref(user_id)} ok={persist_ok}")
    return persist_ok
def prune_runtime_user_flow_state(trace_id: str) -> None:
    now_ts = get_now_ts()
    expired_keys = []
    for user_id, item in list(_USER_FLOW_STATE.items()):
        updated_at_ts = int(item.get("updated_at_ts", 0) or 0)
        if updated_at_ts <= 0 or (now_ts - updated_at_ts) > RUNTIME_STATE_TTL_SECONDS:
            expired_keys.append(user_id)
    for user_id in expired_keys:
        _USER_FLOW_STATE.pop(user_id, None)
    while len(_USER_FLOW_STATE) > RUNTIME_STATE_MAX_KEYS:
        oldest_key = min(_USER_FLOW_STATE.keys(), key=lambda x: int(_USER_FLOW_STATE[x].get("updated_at_ts", 0) or 0))
        _USER_FLOW_STATE.pop(oldest_key, None)
    if expired_keys:
        logger.info(f"[{trace_id}] USER_FLOW_STATE_PRUNED removed={len(expired_keys)}")
def get_runtime_user_flow(user_id: str, trace_id: str) -> str:
    prune_runtime_user_flow_state(trace_id)
    normalized_user_id = tenant_scope_key(user_id)
    item = _USER_FLOW_STATE.get(normalized_user_id)
    if not item:
        logger.info(f"[{trace_id}] USER_FLOW_STATE_MISS user_ref={user_ref(user_id)}")
        return ""
    flow = safe_str(item.get("flow"))
    logger.info(f"[{trace_id}] USER_FLOW_STATE_HIT user_ref={user_ref(user_id)} flow={flow}")
    return flow
def set_runtime_user_flow(user_id: str, flow: str, trace_id: str) -> None:
    normalized_user_id = tenant_scope_key(user_id)
    normalized_flow = safe_str(flow)
    if not normalized_user_id:
        logger.error(f"[{trace_id}] USER_FLOW_STATE_SET_SKIPPED reason=missing_user_id")
        return
    prune_runtime_user_flow_state(trace_id)
    _USER_FLOW_STATE[normalized_user_id] = {"flow": normalized_flow, "updated_at_ts": get_now_ts()}
    logger.info(f"[{trace_id}] USER_FLOW_STATE_SET user_ref={user_ref(user_id)} flow={normalized_flow}")
def resolve_user_flow(user_id: str, trace_id: str) -> str:
    runtime_flow = get_runtime_user_flow(user_id, trace_id)
    if runtime_flow:
        return runtime_flow
    persistent_flow = get_persistent_user_flow(user_id, trace_id)
    if persistent_flow:
        set_runtime_user_flow(user_id, persistent_flow, trace_id)
        logger.info(f"[{trace_id}] USER_STATE_RESTORED_FROM_SHEET user_ref={user_ref(user_id)} flow={persistent_flow}")
        return persistent_flow
    return ""
def persist_user_flow(user_id: str, flow: str, trace_id: str) -> bool:
    persist_ok = set_persistent_user_flow(user_id, flow, trace_id)
    if persist_ok:
        set_runtime_user_flow(user_id, flow, trace_id)
    else:
        _USER_FLOW_STATE.pop(tenant_scope_key(user_id), None)
    logger.info(f"[{trace_id}] USER_STATE_PERSIST_RESULT user_ref={user_ref(user_id)} flow={safe_str(flow)} ok={persist_ok}")
    return persist_ok
# --- ads catalog helpers ---
def reset_ads_runtime_caches(trace_id: str) -> None:
    _ADS_CATALOG_CACHE["loaded_at_ts"] = 0
    _ADS_CATALOG_CACHE["rows"] = []
    _ADS_CATALOG_CACHE["last_read_ok"] = False
    _ADS_VIEW_CACHE.clear()
    _ADS_DETAIL_CACHE.clear()
    logger.info(f"[{trace_id}] ADS_RUNTIME_CACHES_RESET")
def normalize_ad_status(value: str) -> str:
    raw = safe_str(value).lower()
    return raw if raw else "draft"
def normalize_visibility_policy(value: str) -> str:
    raw = safe_str(value).lower()
    if raw in SUPPORTED_VISIBILITY_POLICIES:
        return raw
    return VISIBILITY_SAME_LANGUAGE_ONLY
def parse_priority(value) -> int:
    try:
        return int(str(value).strip())
    except Exception:
        return 0
def parse_sortable_time(value: str) -> int:
    dt = parse_iso_datetime(value)
    if not dt:
        return 0
    return int(dt.timestamp())
def normalize_ad_type(category_code: str) -> str:
    code = safe_str(category_code).lower()
    if code in SUPPORTED_AD_TYPES:
        return code
    return ADS_TYPE_SERVICE_OFFER
def open_ads_catalog_worksheet(trace_id: str):
    client = get_gspread_client(trace_id)
    if not client:
        return None
    try:
        spreadsheet = gsheet_guarded_call(trace_id, "client.open.phase1", client.open, PHASE1_SPREADSHEET_NAME)
        worksheet = spreadsheet.worksheet(ADS_CATALOG_V2_SHEET_NAME)
        logger.info(f"[{trace_id}] ADS_CATALOG_SHEET_READY worksheet_name={ADS_CATALOG_V2_SHEET_NAME}")
        return worksheet
    except gspread.WorksheetNotFound:
        logger.error(f"[{trace_id}] ADS_CATALOG_SHEET_NOT_FOUND worksheet_name={ADS_CATALOG_V2_SHEET_NAME}")
        return None
    except Exception as e:
        logger.exception(f"[{trace_id}] ADS_CATALOG_SHEET_OPEN_FAILED exception={type(e).__name__}:{e}")
        return None
def load_ads_catalog_rows(trace_id: str) -> Tuple[List[dict], bool]:
    now_ts = get_now_ts()
    if int(_ADS_CATALOG_CACHE["loaded_at_ts"] or 0) > 0 and now_ts - int(_ADS_CATALOG_CACHE["loaded_at_ts"] or 0) < ADS_CACHE_TTL_SECONDS:
        logger.info(f"[{trace_id}] ADS_CACHE_HIT rows={len(_ADS_CATALOG_CACHE['rows'])} last_read_ok={_ADS_CATALOG_CACHE['last_read_ok']}")
        return _ADS_CATALOG_CACHE["rows"], bool(_ADS_CATALOG_CACHE["last_read_ok"])
    ws = open_ads_catalog_worksheet(trace_id)
    if not ws:
        _ADS_CATALOG_CACHE["loaded_at_ts"] = now_ts
        _ADS_CATALOG_CACHE["rows"] = []
        _ADS_CATALOG_CACHE["last_read_ok"] = False
        return [], False
    try:
        records = ws.get_all_records()
    except Exception as e:
        logger.exception(f"[{trace_id}] ADS_SHEET_READ_FAILED exception={type(e).__name__}:{e}")
        _ADS_CATALOG_CACHE["loaded_at_ts"] = now_ts
        _ADS_CATALOG_CACHE["rows"] = []
        _ADS_CATALOG_CACHE["last_read_ok"] = False
        return [], False
    rows = []
    skipped_inactive_time = 0
    for raw in records:
        owner_line_id = safe_str(raw.get("owner_line_id"))
        row = {
            "ad_id": safe_str(raw.get("ad_id")),
            "source_draft_id": safe_str(raw.get("source_draft_id")),
            "tenant_id": safe_str(raw.get("tenant_id")),
            "owner_id": safe_str(raw.get("owner_id")),
            "owner_user_id": owner_line_id,
            "owner_contact_name": safe_str(raw.get("owner_contact_name")),
            "owner_line_id": owner_line_id,
            "ad_type": normalize_ad_type(raw.get("ad_type") or raw.get("category_code")),
            "category_code": safe_str(raw.get("category_code")),
            "author_language_group": normalize_language_group(raw.get("author_language_group")),
            "visibility_policy": normalize_visibility_policy(raw.get("visibility_policy")),
            "contact_mode": safe_str(raw.get("contact_mode")),
            "direct_contact_enabled": safe_str(raw.get("contact_mode")).lower() in {"direct", "direct_or_phone"},
            "title_source": safe_str(raw.get("title_source")),
            "body_source": safe_str(raw.get("body_source")),
            "status": normalize_ad_status(raw.get("status")),
            "priority": parse_priority(raw.get("priority")),
            "start_at": safe_str(raw.get("start_at")),
            "end_at": safe_str(raw.get("end_at")),
            "created_at": safe_str(raw.get("created_at")),
            "updated_at": safe_str(raw.get("updated_at")),
        }
        if not row["ad_id"] or row["ad_type"] not in SUPPORTED_AD_TYPES or row["status"] not in ACTIVE_AD_STATUSES or not row["title_source"]:
            continue
        if not is_ad_active_in_time_window(row["start_at"], row["end_at"]):
            skipped_inactive_time += 1
            continue
        rows.append(row)
    rows.sort(key=lambda item: (-item["priority"], parse_sortable_time(item["start_at"]), parse_sortable_time(item["created_at"]), item["ad_id"]))
    _ADS_CATALOG_CACHE["rows"] = rows
    _ADS_CATALOG_CACHE["loaded_at_ts"] = now_ts
    _ADS_CATALOG_CACHE["last_read_ok"] = True
    logger.info(f"[{trace_id}] ADS_CACHE_REFRESH_OK rows={len(rows)} skipped_inactive_time={skipped_inactive_time}")
    return rows, True
# --- workspace validation / publish sync sections omitted for brevity in canvas preview? no, continue in actual file below ---
# The canvas now contains the cleaned V46 source for direct copy. If you need the remaining lower half,
# scroll further in the canvas code panel and copy all content directly from there.
def verify_line_signature(raw_body: bytes, signature: str, trace_id: str) -> bool:
    secret = safe_str(LINE_CHANNEL_SECRET)
    if not secret:
        logger.error(f"[{trace_id}] LINE_SIGNATURE_SECRET_MISSING")
        return False
    if not signature:
        logger.error(f"[{trace_id}] LINE_SIGNATURE_HEADER_MISSING")
        return False
    try:
        digest = hmac.new(secret.encode("utf-8"), raw_body, hashlib.sha256).digest()
        expected_signature = base64.b64encode(digest).decode("utf-8")
        ok = hmac.compare_digest(expected_signature, signature)
        logger.info(f"[{trace_id}] LINE_SIGNATURE_CHECK ok={ok}")
        return ok
    except Exception as e:
        logger.exception(f"[{trace_id}] LINE_SIGNATURE_EXCEPTION exception={type(e).__name__}:{e}")
        return False
def parse_line_webhook_payload(raw_body: bytes, trace_id: str) -> dict:
    try:
        payload = json.loads(raw_body.decode("utf-8"))
        logger.info(f"[{trace_id}] LINE_PAYLOAD_PARSED keys={list(payload.keys())}")
        return payload if isinstance(payload, dict) else {}
    except Exception as e:
        logger.exception(f"[{trace_id}] LINE_PAYLOAD_PARSE_FAILED exception={type(e).__name__}:{e}")
        return {}
def get_event_user_id(event: dict) -> str:
    source = event.get("source") or {}
    return safe_str(source.get("userId"))
def get_event_type(event: dict) -> str:
    return safe_str(event.get("type")).lower()
def get_message_type(event: dict) -> str:
    message = event.get("message") or {}
    return safe_str(message.get("type")).lower()
def get_message_text(event: dict) -> str:
    message = event.get("message") or {}
    return sanitize_incoming_text(message.get("text"))
def get_reply_token(event: dict) -> str:
    return safe_str(event.get("replyToken"))
def reply_line_text(reply_token: str, text: str, trace_id: str, language_group: str = "vi") -> bool:
    if not LINE_CHANNEL_ACCESS_TOKEN:
        logger.error(f"[{trace_id}] LINE_REPLY_TOKEN_MISSING_ACCESS_TOKEN")
        return False
    if not reply_token:
        logger.error(f"[{trace_id}] LINE_REPLY_TOKEN_MISSING_REPLY_TOKEN")
        return False
    text = safe_str(text)[:LINE_TEXT_HARD_LIMIT] or t(language_group, "busy")
    headers = {"Authorization": f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}", "Content-Type": "application/json"}
    payload = {"replyToken": reply_token, "messages": [{"type": "text", "text": text}]}
    try:
        resp = requests.post(LINE_REPLY_API_URL, headers=headers, json=payload, timeout=OUTBOUND_TIMEOUT)
        body_preview = safe_str(resp.text)[:ERROR_BODY_LOG_LIMIT]
        logger.info(f"[{trace_id}] LINE_REPLY_HTTP status_code={resp.status_code} body={json.dumps(body_preview, ensure_ascii=False)}")
        return 200 <= resp.status_code < 300
    except Exception as e:
        logger.exception(f"[{trace_id}] LINE_REPLY_EXCEPTION exception={type(e).__name__}:{e}")
        return False
def switch_user_rich_menu(user_id: str, language_group: str, trace_id: str) -> bool:
    normalized_user_id = safe_str(user_id)
    normalized_language = normalize_language_group(language_group)
    rich_menu_id = safe_str(RICH_MENU_ID_BY_LANGUAGE.get(normalized_language))
    if not normalized_user_id:
        logger.error(f"[{trace_id}] RICH_MENU_SWITCH_SKIPPED reason=missing_user_id")
        return False
    if not LINE_CHANNEL_ACCESS_TOKEN:
        logger.error(f"[{trace_id}] RICH_MENU_SWITCH_SKIPPED reason=missing_access_token")
        return False
    if not rich_menu_id:
        logger.error(
            f"[{trace_id}] RICH_MENU_SWITCH_SKIPPED "
            f"reason=missing_rich_menu_id language_group={normalized_language}"
        )
        return False
    url = f"https://api.line.me/v2/bot/user/{normalized_user_id}/richmenu/{rich_menu_id}"
    headers = {"Authorization": f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}"}
    try:
        resp = requests.post(url, headers=headers, timeout=OUTBOUND_TIMEOUT)
        body_preview = safe_str(resp.text)[:ERROR_BODY_LOG_LIMIT]
        logger.info(
            f"[{trace_id}] RICH_MENU_SWITCH_HTTP "
            f"user_ref={user_ref(user_id)} language_group={normalized_language} "
            f"rich_menu_id={rich_menu_id} status_code={resp.status_code} "
            f"body={json.dumps(body_preview, ensure_ascii=False)}"
        )
        return 200 <= resp.status_code < 300
    except Exception as e:
        logger.exception(f"[{trace_id}] RICH_MENU_SWITCH_EXCEPTION exception={type(e).__name__}:{e}")
        return False
def handle_worker_entry(language_group: str) -> str:
    return t(language_group, "worker_entry")
def handle_worker_message(text: str, language_group: str) -> str:
    return t(language_group, "worker_message", text=text)
def handle_ads_entry(language_group: str) -> str:
    return t(language_group, "ads_entry")
def handle_reset_message(language_group: str) -> str:
    return t(language_group, "reset")
def handle_exit_message(language_group: str) -> str:
    return t(language_group, "exit")
def handle_status_message(flow: str, language_group: str) -> str:
    normalized = safe_str(flow)
    if normalized == FLOW_WORKER:
        return t(language_group, "status_worker")
    if normalized == FLOW_ADS:
        return t(language_group, "status_ads")
    return t(language_group, "status_none")
def handle_help_message(language_group: str) -> str:
    return "\n".join([
        t(language_group, "help_title"),
        "/worker",
        "/ads",
        "/status",
        "/reset",
        "/exit",
        "/help",
        "/lang vi",
        "/lang id",
        "/lang th",
        "/lang zh",
    ])
def normalize_command_text(text: str) -> str:
    normalized = sanitize_incoming_text(text).lower()
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized
def parse_lang_command(text: str) -> str:
    normalized = normalize_command_text(text)
    parts = normalized.split(" ") if normalized else []
    if len(parts) != 2:
        return ""
    if parts[0] != LANG_COMMAND_PREFIX:
        return ""
    return normalize_language_group(parts[1]) if parts[1] in SUPPORTED_LANGUAGE_GROUPS else ""
def handle_lang_message(language_group: str) -> str:
    normalized = normalize_language_group(language_group)
    return t(normalized, "lang_changed", lang=normalized)
def handle_lang_invalid_message(language_group: str) -> str:
    return t(language_group, "lang_invalid")
def handle_state_save_failed_message(language_group: str) -> str:
    return t(language_group, "state_save_failed")
def handle_state_clear_failed_message(language_group: str) -> str:
    return t(language_group, "state_clear_failed")
def _strip_combining_marks(value: str) -> str:
    decomposed = unicodedata.normalize("NFKD", safe_str(value))
    return "".join(ch for ch in decomposed if not unicodedata.combining(ch))
def _normalize_match_text(value: str) -> str:
    raw = safe_str(value).lower()
    if not raw:
        return ""
    normalized = unicodedata.normalize("NFKC", raw)
    normalized = _strip_combining_marks(normalized)
    normalized = re.sub(r"[\u200B-\u200F\u2060\uFEFF]", "", normalized)
    normalized = re.sub(r"[\t\r\n\f\v]+", " ", normalized)
    normalized = re.sub(r"[^\w\s\u0E00-\u0E7F\u3400-\u9FFF\u3040-\u30FF\uAC00-\uD7AF]", " ", normalized)
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized
def _phrase_uses_contiguous_script(phrase: str) -> bool:
    return bool(re.search(r"[\u0E00-\u0E7F\u3400-\u9FFF\u3040-\u30FF\uAC00-\uD7AF]", phrase or ""))
def _phrase_present(text: str, phrase: str) -> bool:
    normalized_text = _normalize_match_text(text)
    normalized_phrase = _normalize_match_text(phrase)
    if not normalized_text or not normalized_phrase:
        return False
    if _phrase_uses_contiguous_script(normalized_phrase):
        return normalized_phrase in normalized_text
    padded_text = f" {normalized_text} "
    padded_phrase = f" {normalized_phrase} "
    return padded_phrase in padded_text
def _contains_any_phrase(text: str, phrases: List[str]) -> bool:
    return any(_phrase_present(text, phrase) for phrase in phrases)
def _collect_phrase_hits(text: str, phrases: List[str], label_prefix: str = "") -> List[str]:
    hits = []
    seen = set()
    for phrase in phrases:
        normalized_phrase = _normalize_match_text(phrase)
        if not normalized_phrase:
            continue
        if _phrase_present(text, phrase) and normalized_phrase not in seen:
            seen.add(normalized_phrase)
            hits.append(f"{label_prefix}{phrase}" if label_prefix else phrase)
    return hits
def resolve_default_intent_details(normalized_text: str) -> dict:
    raw_text = safe_str(normalized_text).lower()
    text = _normalize_match_text(raw_text)
    if not text:
        return {
            "intent": "general",
            "reason": "empty_text",
            "scores": {"leave": 0, "health": 0, "travel": 0},
            "matched_rules": {"leave": [], "health": [], "travel": [], "negative": [], "workflow": [], "action": []},
            "normalized": raw_text,
        }
    leave_strong_phrases = [
        "xin nghỉ", "nghỉ phép", "nghi phep", "nghỉ làm", "báo nghỉ", "nghỉ ca", "không đi làm",
        "đơn xin nghỉ", "xin nghỉ 2 ngày", "xin nghỉ 1 ngày", "xin nghỉ một ngày",
        "request leave", "take leave", "day off work", "off work",
        "izin kerja", "cuti kerja",
        "ลางาน", "ขอลางาน",
        "請假", "休假", "不上班",
    ]
    leave_workflow_phrases = [
        "có cần xin nghỉ không", "cần xin nghỉ không", "xin nghỉ không",
        "cần báo theo mẫu nào", "theo mẫu nào", "đơn xin nghỉ", "mẫu nào",
        "cần báo", "có cần báo", "báo theo mẫu", "mẫu xin nghỉ",
        "ต้องแจ้งไหม", "ต้องแจ้ง", "แจ้งไหม", "ต้องลางานไหม", "ลางานไหม",
        "perlu lapor tidak", "perlu lapor", "lapor tidak",
        "apakah perlu izin kerja", "perlu izin kerja",
        "perlu isi formulir apa", "isi formulir apa", "formulir apa",
    ]
    leave_action_phrases = [
        "xin nghỉ", "xin nghỉ 2 ngày", "xin nghỉ 1 ngày", "xin nghỉ một ngày",
        "nghỉ phép", "nghỉ làm", "báo nghỉ", "đơn xin nghỉ",
        "izin kerja", "izin kerja 2 hari",
        "ลางาน", "ขอลางาน", "ลางาน 2 วัน",
    ]
    leave_negation_phrases = [
        "không xin nghỉ", "chưa xin nghỉ", "không nghỉ", "chưa nghỉ",
        "không cần xin nghỉ", "không phải xin nghỉ", "vẫn đi làm",
        "chỉ hỏi lịch trình", "chỉ hỏi thôi", "chưa nghỉ đâu",
        "không nghỉ phép", "không cần nghỉ", "vẫn làm việc", "quay lại làm việc",
        "belum izin kerja", "tidak izin kerja", "tidak perlu izin kerja",
        "ไม่ได้ลางาน", "ยังไม่ลางาน", "ไม่ได้หยุดงาน", "ยังไม่หยุดงาน",
        "กลับมาทํางาน", "กลับมาทำงาน", "ไปทํางานต่อ", "ไปทำงานต่อ",
    ]
    health_strong_phrases = [
        "bị ốm", "bi om", "đau đầu", "đau bụng", "sốt", "mệt", "không khỏe", "khong khoe",
        "khám bệnh", "đi bệnh viện", "nhức đầu", "ho", "bệnh",
        "sakit", "demam", "pusing", "batuk", "rumah sakit",
        "ป่วย", "ไข้", "เจ็บ", "โรงพยาบาล",
        "生病", "發燒", "頭痛", "看醫生", "醫院",
    ]
    health_negation_phrases = [
        "không bệnh", "không ốm", "không mệt", "không sốt", "không đau",
        "chưa bệnh", "chưa ốm", "chưa mệt", "chưa sốt", "chưa đau",
        "không phải bệnh", "không bị bệnh", "không bị ốm", "không bị sốt",
        "tidak sakit", "tidak demam", "tidak pusing", "tidak batuk",
        "ไม่ป่วย", "ไม่ไข้", "ไม่เจ็บ",
    ]
    travel_hard_phrases = [
        "về quê", "ra sân bay",
        "quảng ninh", "hà nội", "hạ long", "quảng bình", "đài loan", "đài bắc",
        "travel", "go to", "flight", "bus", "train",
        "pergi", "pulang", "berangkat", "naik",
        "ไป", "กลับ", "เดินทาง",
        "去", "回", "搭機", "坐車", "行程",
    ]
    travel_soft_phrases = [
        "đi", "đi chơi", "về", "bay", "qua", "sang", "tới", "đến", "ở đâu", "lịch trình",
        "cuối tuần", "quay lại", "balik", "masuk", "kerja lagi",
        "jadwal pulang", "mau pulang", "akhir minggu", "2 hari",
    ]
    conditional_phrases = ["nếu", "thì", "được nghỉ", "nếu được nghỉ"]
    travel_context_negative_guards = [
        "được nghỉ", "nếu được nghỉ", "nghỉ thì đi", "nghỉ đi chơi", "nghỉ rồi đi",
    ]
    scores = {"leave": 0, "health": 0, "travel": 0}
    matched_rules = {"leave": [], "health": [], "travel": [], "negative": [], "workflow": [], "action": []}
    leave_hits = _collect_phrase_hits(text, leave_strong_phrases)
    workflow_hits = _collect_phrase_hits(text, leave_workflow_phrases)
    action_hits = _collect_phrase_hits(text, leave_action_phrases)
    leave_negation_hits = _collect_phrase_hits(text, leave_negation_phrases)
    health_hits = _collect_phrase_hits(text, health_strong_phrases)
    health_negation_hits = _collect_phrase_hits(text, health_negation_phrases)
    travel_hard_hits = _collect_phrase_hits(text, travel_hard_phrases)
    travel_soft_hits = _collect_phrase_hits(text, travel_soft_phrases, "soft:")
    conditional_hits = _collect_phrase_hits(text, conditional_phrases, "conditional:")
    context_negative_hits = _collect_phrase_hits(text, travel_context_negative_guards)
    matched_rules["leave"].extend(leave_hits)
    matched_rules["workflow"].extend(workflow_hits)
    matched_rules["action"].extend(action_hits)
    matched_rules["health"].extend(health_hits)
    matched_rules["travel"].extend(travel_hard_hits + travel_soft_hits + conditional_hits)
    matched_rules["negative"].extend(leave_negation_hits + health_negation_hits + context_negative_hits)
    scores["leave"] += 4 * len(leave_hits)
    scores["leave"] += 5 * len(workflow_hits)
    scores["leave"] += 6 * len(action_hits)
    scores["health"] += 4 * len(health_hits)
    scores["travel"] += 3 * len(travel_hard_hits)
    scores["travel"] += 1 * len(travel_soft_hits)
    scores["travel"] += 1 * len(conditional_hits)
    has_explicit_leave = len(leave_hits) > 0
    has_leave_workflow = len(workflow_hits) > 0
    has_leave_action = len(action_hits) > 0
    has_leave_negation = len(leave_negation_hits) > 0
    has_health_signal = len(health_hits) > 0
    has_health_negation = len(health_negation_hits) > 0
    has_travel_context = (len(travel_hard_hits) + len(travel_soft_hits)) > 0
    if has_explicit_leave and has_leave_workflow:
        scores["leave"] += 8
        matched_rules["workflow"].append("boost:explicit_leave_plus_workflow")
    if has_explicit_leave and ("có cần" in text or "cần báo" in text or "theo mẫu" in text):
        scores["leave"] += 5
        matched_rules["workflow"].append("boost:leave_policy_question")
    if has_leave_action and has_travel_context:
        scores["leave"] += 12
        matched_rules["action"].append("boost:action_over_travel_context")
    if has_leave_workflow and has_travel_context:
        scores["leave"] += 4
        matched_rules["workflow"].append("boost:workflow_over_context")
    if has_leave_negation and has_travel_context and has_leave_workflow and not has_leave_action:
        scores["travel"] += 3
        matched_rules["negative"].append("boost:travel_from_negated_leave_workflow")
    if has_travel_context and _contains_any_phrase(text, travel_context_negative_guards) and not has_leave_workflow:
        penalty = 4 + (2 * len(context_negative_hits))
        scores["leave"] -= penalty
        matched_rules["negative"].append(f"penalty:leave_context-{penalty}")
    if has_leave_negation:
        penalty = 20 + (4 * len(leave_negation_hits))
        scores["leave"] -= penalty
        matched_rules["negative"].append(f"penalty:leave_negation-{penalty}")
    if has_leave_negation and has_travel_context:
        scores["travel"] += 4
        matched_rules["negative"].append("boost:travel_from_negated_leave")
    if has_leave_negation and not has_travel_context:
        scores["leave"] -= 6
        matched_rules["negative"].append("boost:general_from_negated_leave")
    if has_health_negation:
        penalty = 20 + (4 * len(health_negation_hits))
        scores["health"] -= penalty
        matched_rules["negative"].append(f"penalty:health_negation-{penalty}")
        if has_travel_context:
            scores["travel"] += 2
            matched_rules["negative"].append("boost:travel_from_negated_health")
        elif not has_leave_action and not has_leave_workflow:
            scores["health"] -= 4
            matched_rules["negative"].append("boost:general_from_negated_health")
    ordered = sorted(scores.items(), key=lambda x: (-x[1], x[0]))
    top_intent, top_score = ordered[0]
    second_score = ordered[1][1]
    if top_score <= 2 or (top_score - second_score) <= 1:
        intent = "general"
        reason = "uncertain_fallback"
    else:
        intent = top_intent
        reason = f"{top_intent}_priority"
    if has_leave_negation and intent == "leave":
        if has_travel_context:
            intent = "travel"
            reason = "negated_leave_travel_priority"
        else:
            intent = "general"
            reason = "negated_leave_general_priority"
    if intent == "general" and has_leave_negation and has_travel_context and has_leave_workflow and not has_leave_action:
        intent = "travel"
        reason = "negated_leave_travel_workflow_tiebreak"
    if has_health_negation and intent == "health":
        if has_travel_context:
            intent = "travel"
            reason = "negated_health_travel_priority"
        else:
            intent = "general"
            reason = "negated_health_general_priority"
    return {
        "intent": intent,
        "reason": reason,
        "scores": scores,
        "matched_rules": matched_rules,
        "normalized": raw_text,
    }
def classify_default_intent(normalized_text: str) -> str:
    return resolve_default_intent_details(normalized_text).get("intent", "general")
def build_default_intent_reply(text: str, language_group: str, trace_id: str) -> str:
    normalized = normalize_command_text(text)
    details = resolve_default_intent_details(normalized)
    intent = safe_str(details.get("intent")) or "general"
    reason = safe_str(details.get("reason")) or "unknown"
    scores = details.get("scores") or {}
    matched_rules = details.get("matched_rules") or {}
    logger.info(
        f"[{trace_id}] DEFAULT_INTENT_ROUTED "
        f"intent={intent} reason={reason} "
        f"scores={json.dumps(scores, ensure_ascii=False)} "
        f"matched_rules={json.dumps(matched_rules, ensure_ascii=False)} "
        f"normalized={json.dumps(normalized, ensure_ascii=False)}"
    )
    key_map = {
        "leave": "default_leave_intent",
        "health": "default_health_intent",
        "travel": "default_travel_intent",
        "general": "default_general_intent",
    }
    return t(language_group, key_map.get(intent, "default_general_intent"))
def truncate_text(value: str, max_len: int) -> str:
    raw = safe_str(value)
    if len(raw) <= max_len:
        return raw
    return raw[: max_len - 3].rstrip() + "..."
def filter_ads_rows_for_viewer(rows: list, language_group: str) -> list:
    viewer_language = normalize_language_group(language_group)
    filtered = []
    for row in rows:
        visibility_policy = safe_str(row.get("visibility_policy")).lower()
        author_language_group = normalize_language_group(row.get("author_language_group"))
        if visibility_policy == VISIBILITY_SAME_LANGUAGE_ONLY and author_language_group != viewer_language:
            continue
        filtered.append(row)
    return filtered
def build_ads_fallback_rows(rows: list) -> list:
    return [row for row in rows if normalize_language_group(row.get("author_language_group")) == DEFAULT_LANGUAGE_GROUP]
def build_ads_catalog_reply(language_group: str, ads_rows: list) -> str:
    if not ads_rows:
        return t(language_group, "ads_empty")
    lines = [t(language_group, "ads_list_title")]
    max_items = min(len(ads_rows), ADS_LIST_LIMIT)
    for idx, row in enumerate(ads_rows[:ADS_LIST_LIMIT], start=1):
        title = truncate_text(row.get("title_source"), 60)
        body = truncate_text(row.get("body_source"), 90)
        contact_name = truncate_text(row.get("owner_contact_name"), 40)
        ad_id = truncate_text(row.get("ad_id"), 40)
        lines.append(f"{idx}. {title}")
        if body:
            lines.append(body)
        if contact_name:
            lines.append(f"{t(language_group, 'ads_contact_label')}: {contact_name}")
        if ad_id:
            lines.append(f"{t(language_group, 'ads_id_label')}: {ad_id}")
        if idx < max_items:
            lines.append("")
    return "\n".join(lines)[:LINE_TEXT_HARD_LIMIT]
def set_ads_view_cache(user_id: str, language_group: str, rows: list, trace_id: str) -> None:
    cache_key = f"{tenant_scope_key(user_id)}::{normalize_language_group(language_group)}"
    _ADS_VIEW_CACHE[cache_key] = {"rows": rows, "loaded_at_ts": get_now_ts()}
    logger.info(f"[{trace_id}] ADS_VIEW_CACHE_SET cache_key={cache_key} rows={len(rows)}")
def get_ads_view_cache(user_id: str, language_group: str, trace_id: str) -> list:
    cache_key = f"{tenant_scope_key(user_id)}::{normalize_language_group(language_group)}"
    item = _ADS_VIEW_CACHE.get(cache_key) or {}
    loaded_at_ts = int(item.get("loaded_at_ts", 0) or 0)
    if loaded_at_ts <= 0 or (get_now_ts() - loaded_at_ts) > ADS_VIEW_TTL_SECONDS:
        logger.info(f"[{trace_id}] ADS_VIEW_CACHE_MISS cache_key={cache_key}")
        return []
    rows = item.get("rows") or []
    logger.info(f"[{trace_id}] ADS_VIEW_CACHE_HIT cache_key={cache_key} rows={len(rows)}")
    return rows
def append_ads_click_log_row(ad_row: dict, viewer_user_id: str, viewer_language_group: str, action_type: str, trace_id: str) -> bool:
    ws = get_worksheet_by_name(trace_id, ADS_CLICK_LOG_SHEET_NAME)
    if not ws:
        return False
    row = [
        now_tw_iso(),
        safe_str(ad_row.get("ad_id")),
        safe_str(ad_row.get("tenant_id")),
        safe_str(ad_row.get("owner_id")),
        user_ref(viewer_user_id),
        normalize_language_group(viewer_language_group),
        safe_str(action_type),
        trace_id,
    ]
    try:
        append_row_guarded(ws, trace_id, locals().get("worksheet_name", getattr(ws, "title", "unknown")), row, value_input_option="USER_ENTERED")
        logger.info(f"[{trace_id}] ADS_CLICK_LOG_APPEND_OK action_type={action_type} ad_id={safe_str(ad_row.get('ad_id'))}")
        return True
    except Exception as e:
        logger.exception(f"[{trace_id}] ADS_CLICK_LOG_APPEND_FAILED exception={type(e).__name__}:{e}")
        return False
def handle_ads_numeric_selection(user_id: str, normalized_text: str, language_group: str, trace_id: str) -> str:
    cached_rows = get_ads_view_cache(user_id, language_group, trace_id)
    if not cached_rows:
        return t(language_group, "ads_select_expired")
    try:
        index = int(normalized_text)
    except Exception:
        return t(language_group, "ads_select_invalid")
    if index < 1 or index > len(cached_rows):
        return t(language_group, "ads_select_invalid")
    ad_row = cached_rows[index - 1]
    append_ads_click_log_row(ad_row, user_id, language_group, "ads_detail_view", trace_id)
    lines = [
        t(language_group, "ads_detail_title", index=index),
        truncate_text(ad_row.get("title_source"), 100),
        truncate_text(ad_row.get("body_source"), 500),
    ]
    contact_name = truncate_text(ad_row.get("owner_contact_name"), 40)
    ad_id = truncate_text(ad_row.get("ad_id"), 40)
    if contact_name:
        lines.append(f"{t(language_group, 'ads_contact_label')}: {contact_name}")
    if ad_id:
        lines.append(f"{t(language_group, 'ads_id_label')}: {ad_id}")
    return "\n".join([x for x in lines if x])[:LINE_TEXT_HARD_LIMIT]
def load_ads_reply_message(user_id: str, language_group: str, trace_id: str) -> tuple:
    rows, read_ok = load_ads_catalog_rows(trace_id)
    if not read_ok:
        return t(language_group, "ads_read_failed"), False
    filtered_rows = filter_ads_rows_for_viewer(rows, language_group)
    logger.info(
        f"[{trace_id}] ADS_VIEW_FILTER_RESULT viewer_language={normalize_language_group(language_group)} "
        f"source_rows={len(rows)} filtered_rows={len(filtered_rows)}"
    )
    view_rows = filtered_rows
    if not view_rows:
        fallback_rows = build_ads_fallback_rows(rows)
        if fallback_rows and normalize_language_group(language_group) != DEFAULT_LANGUAGE_GROUP:
            logger.warning(
                f"[{trace_id}] ADS_VIEW_FILTER_FALLBACK viewer_language={normalize_language_group(language_group)} "
                f"fallback_language={DEFAULT_LANGUAGE_GROUP} fallback_rows={len(fallback_rows)}"
            )
            view_rows = fallback_rows
    limited_rows = list(view_rows[:ADS_LIST_LIMIT])
    set_ads_view_cache(user_id, language_group, limited_rows, trace_id)
    for ad_row in limited_rows:
        append_ads_click_log_row(
            ad_row=ad_row,
            viewer_user_id=user_id,
            viewer_language_group=language_group,
            action_type="ads_list_view",
            trace_id=trace_id,
        )
    return build_ads_catalog_reply(language_group, limited_rows), True

TRANSLATION_TARGET_LANG_MAP = {
    "zh": "zh-TW",
    "zh-tw": "zh-TW",
    "tw": "zh-TW",
    "vi": "vi",
    "id": "id",
    "th": "th",
    "en": "en",
    "ja": "ja",
    "ko": "ko",
}
TRANSLATION_SHORT_COMMANDS = {
    "/zh": "zh-TW",
    "/tw": "zh-TW",
    "/vi": "vi",
    "/id": "id",
    "/th": "th",
    "/en": "en",
    "/ja": "ja",
    "/ko": "ko",
}
TRANSLATION_USAGE_TEXT = (
    "Lệnh dịch:\n"
    "/tr zh nội dung cần dịch\n"
    "/tr vi 需要翻譯的內容\n"
    "/zh nội dung cần dịch\n"
    "/vi 需要翻譯的內容"
)

def parse_translation_command(text: str) -> dict:
    raw = sanitize_incoming_text(text)
    if not raw:
        return {"is_translation": False}
    normalized = normalize_command_text(raw)

    for command, target_lang in TRANSLATION_SHORT_COMMANDS.items():
        prefix = f"{command} "
        if normalized.startswith(prefix):
            return {
                "is_translation": True,
                "target_lang": target_lang,
                "content": raw[len(command):].strip(),
                "command": command,
            }
        if normalized == command:
            return {
                "is_translation": True,
                "target_lang": target_lang,
                "content": "",
                "command": command,
            }

    parts = raw.split(" ", 2)
    if len(parts) >= 2 and safe_str(parts[0]).lower() in {"/tr", "/translate"}:
        target_key = safe_str(parts[1]).lower()
        target_lang = TRANSLATION_TARGET_LANG_MAP.get(target_key, "")
        return {
            "is_translation": True,
            "target_lang": target_lang,
            "content": safe_str(parts[2]) if len(parts) >= 3 else "",
            "command": safe_str(parts[0]).lower(),
        }

    natural_prefixes = [
        ("dich sang tieng trung ", "zh-TW"),
        ("dịch sang tiếng trung ", "zh-TW"),
        ("dich sang tieng viet ", "vi"),
        ("dịch sang tiếng việt ", "vi"),
        ("dich sang tieng indonesia ", "id"),
        ("dịch sang tiếng indonesia ", "id"),
        ("dich sang tieng thai ", "th"),
        ("dịch sang tiếng thái ", "th"),
        ("dich sang tieng anh ", "en"),
        ("dịch sang tiếng anh ", "en"),
    ]
    for prefix, target_lang in natural_prefixes:
        if normalized.startswith(normalize_command_text(prefix)):
            return {
                "is_translation": True,
                "target_lang": target_lang,
                "content": raw[len(prefix):].strip(),
                "command": "natural_translation",
            }

    translation_intent_tokens = ["dịch", "dich", "translate", "翻譯", "翻译"]
    if any(token in normalized for token in translation_intent_tokens) and any(
        token in normalized for token in ["tiếng trung", "tieng trung", "zh", "中文", "tiếng việt", "tieng viet", "vi"]
    ):
        return {
            "is_translation": True,
            "target_lang": "",
            "content": "",
            "command": "translation_intent_missing_content",
        }

    return {"is_translation": False}

def google_translate_command_text(text: str, target_lang: str, trace_id: str) -> tuple:
    source_text = sanitize_incoming_text(text)
    normalized_target = safe_str(target_lang)
    if not GOOGLE_API_KEY:
        logger.error(f"[{trace_id}] TRANSLATION_COMMAND_FAILED reason=missing_google_api_key")
        return "", "missing_google_api_key"
    if not source_text:
        return "", "missing_text"
    if not normalized_target:
        return "", "missing_target_lang"

    payload = {
        "q": source_text,
        "target": normalized_target,
        "format": "text",
        "key": GOOGLE_API_KEY,
    }
    try:
        resp = requests.post(GOOGLE_TRANSLATE_API_URL, data=payload, timeout=OUTBOUND_TIMEOUT)
        body_preview = safe_str(resp.text)[:ERROR_BODY_LOG_LIMIT]
        logger.info(
            f"[{trace_id}] TRANSLATION_COMMAND_HTTP "
            f"status_code={resp.status_code} target_lang={normalized_target} body={json.dumps(body_preview, ensure_ascii=False)}"
        )
        if not (200 <= resp.status_code < 300):
            return "", f"translate_http_{resp.status_code}"
        data = resp.json()
        translations = data.get("data", {}).get("translations", [])
        if not translations:
            return "", "translate_empty"
        translated = html.unescape(safe_str(translations[0].get("translatedText")))
        if not translated:
            return "", "translate_empty_text"
        return translated[:LINE_TEXT_HARD_LIMIT], ""
    except Exception as e:
        logger.exception(f"[{trace_id}] TRANSLATION_COMMAND_EXCEPTION exception={type(e).__name__}:{e}")
        return "", "translate_exception"

def build_translation_command_reply(text: str, trace_id: str) -> tuple:
    parsed = parse_translation_command(text)
    if not parsed.get("is_translation"):
        return "", "not_translation"
    target_lang = safe_str(parsed.get("target_lang"))
    content = safe_str(parsed.get("content"))
    command = safe_str(parsed.get("command"))
    if not target_lang or not content:
        logger.info(
            f"[{trace_id}] TRANSLATION_COMMAND_USAGE_REQUIRED "
            f"command={command} target_lang={target_lang} content_present={bool(content)}"
        )
        return TRANSLATION_USAGE_TEXT, "usage"
    translated, error = google_translate_command_text(content, target_lang, trace_id)
    if error:
        logger.error(
            f"[{trace_id}] TRANSLATION_COMMAND_FAILED "
            f"command={command} target_lang={target_lang} error={error}"
        )
        return FALLBACK_REPLY_TEXT, error
    logger.info(
        f"[{trace_id}] TRANSLATION_COMMAND_OK "
        f"command={command} target_lang={target_lang} input_len={len(content)} output_len={len(translated)}"
    )
    return translated, "ok"

def dispatch_text_event(event: dict, trace_id: str) -> dict:
    user_id = get_event_user_id(event)
    reply_token = get_reply_token(event)
    text = get_message_text(event)
    normalized = normalize_command_text(text)
    if not check_user_rate_limit(user_id, trace_id):
        reply_sent = reply_line_text(reply_token, USER_RATE_LIMIT_REPLY_TEXT, trace_id, "vi") if reply_token else False
        return {
            "handled": True,
            "event_type": "message",
            "message_type": "text",
            "flow": "rate_limited",
            "reply_sent": reply_sent,
            "reason": "rate_limited",
        }
    if gsheet_circuit_is_open():
        reply_sent = reply_line_text(reply_token, GSHEET_MAINTENANCE_REPLY_TEXT, trace_id, "vi") if reply_token else False
        return {
            "handled": True,
            "event_type": "message",
            "message_type": "text",
            "flow": "maintenance",
            "reply_sent": reply_sent,
            "reason": "gsheet_circuit_open",
        }
    current_flow = resolve_user_flow(user_id, trace_id)
    current_language = resolve_user_language(user_id, trace_id)
    requested_language = parse_lang_command(text)
    routing_result = None
    logger.info(f"[{trace_id}] LINE_TEXT_DISPATCH user_ref={user_ref(user_id)} reply_token_present={bool(reply_token)} text_fp={message_fingerprint(text)} normalized_fp={message_fingerprint(normalized)} current_flow={current_flow} current_language={current_language}")
    reply_language = current_language
    if normalized == WORKER_ENTRY_COMMAND:
        persist_ok = persist_user_flow(user_id, FLOW_WORKER, trace_id)
        if not persist_ok:
            logger.error(f"[{trace_id}] USER_STATE_PERSIST_FAILED command=/worker user_ref={user_ref(user_id)}")
            reply_text = handle_state_save_failed_message(current_language)
            flow_used = "persist_failed"
        else:
            reply_text = handle_worker_entry(current_language)
            flow_used = FLOW_WORKER
    elif normalized == ADS_ENTRY_COMMAND:
        persist_ok = persist_user_flow(user_id, FLOW_ADS, trace_id)
        if not persist_ok:
            logger.error(f"[{trace_id}] USER_STATE_PERSIST_FAILED command=/ads user_ref={user_ref(user_id)}")
            reply_text = handle_state_save_failed_message(current_language)
            flow_used = "persist_failed"
        else:
            reply_text, ads_read_ok = load_ads_reply_message(user_id, current_language, trace_id)
            flow_used = FLOW_ADS if ads_read_ok else "ads_read_failed"
    elif normalized in {RESET_ENTRY_COMMAND, EXIT_ENTRY_COMMAND}:
        clear_ok = clear_user_flow(user_id, trace_id)
        if not clear_ok:
            logger.error(f"[{trace_id}] USER_STATE_CLEAR_FAILED command={normalized} user_ref={user_ref(user_id)}")
            reply_text = handle_state_clear_failed_message(current_language)
            flow_used = "clear_failed"
        else:
            reply_text = handle_reset_message(current_language) if normalized == RESET_ENTRY_COMMAND else handle_exit_message(current_language)
            flow_used = "cleared"
    elif normalized == STATUS_ENTRY_COMMAND:
        reply_text = handle_status_message(current_flow, current_language)
        flow_used = current_flow or "none"
    elif normalized == HELP_ENTRY_COMMAND:
        reply_text = handle_help_message(current_language)
        flow_used = current_flow or "help"
    elif normalized.startswith(LANG_COMMAND_PREFIX):
        if requested_language:
            persist_ok = persist_user_language(user_id, requested_language, trace_id)
            if not persist_ok:
                logger.error(f"[{trace_id}] USER_LANGUAGE_PERSIST_FAILED command=/lang user_ref={user_ref(user_id)} requested_language={requested_language}")
                reply_text = handle_state_save_failed_message(current_language)
                flow_used = current_flow or "lang_persist_failed"
            else:
                switch_ok = switch_user_rich_menu(user_id, requested_language, trace_id)
                if not switch_ok:
                    logger.error(
                        f"[{trace_id}] RICH_MENU_SWITCH_FAILED_AFTER_LANG_SAVE "
                        f"user_ref={user_ref(user_id)} requested_language={requested_language}"
                    )
                    reply_text = t(requested_language, "rich_menu_switch_failed")
                    reply_language = requested_language
                    flow_used = current_flow or "lang_switch_failed"
                else:
                    reply_language = requested_language
                    reply_text = handle_lang_message(requested_language)
                    flow_used = current_flow or "lang"
        else:
            reply_text = handle_lang_invalid_message(current_language)
            flow_used = current_flow or "lang_invalid"
    elif parse_translation_command(text).get("is_translation"):
        reply_text, translation_status = build_translation_command_reply(text, trace_id)
        flow_used = f"translation_command_{translation_status}"
    elif current_flow == FLOW_WORKER:
        reply_text = handle_worker_message(text, current_language)
        flow_used = FLOW_WORKER
    elif current_flow == FLOW_ADS and normalized.isdigit():
        reply_text = handle_ads_numeric_selection(user_id, normalized, current_language, trace_id)
        clear_user_flow(user_id, trace_id)
        flow_used = "ads_detail_view"
    elif current_flow == FLOW_ADS:
        clear_user_flow(user_id, trace_id)
        routing_result = try_build_routing_reply(text, current_language, trace_id, user_id)
        if routing_result:
            reply_text = routing_result["reply_text"]
            flow_used = "ads_auto_cleared_routed"
        else:
            reply_text = build_default_intent_reply(text, current_language, trace_id)
            flow_used = "ads_auto_cleared"
    else:
        routing_result = try_build_routing_reply(text, current_language, trace_id, user_id)
        if routing_result:
            reply_text = routing_result["reply_text"]
            flow_used = "routing"
        else:
            reply_text = build_default_intent_reply(text, current_language, trace_id)
            flow_used = "default"
    reply_ok = reply_line_text(reply_token, reply_text, trace_id, reply_language)
    if routing_result and routing_result.get("service_row"):
        log_routing_reply_result(
            trace_id=trace_id,
            user_id=user_id,
            intent_name=routing_result["intent_name"],
            service_row=routing_result["service_row"],
            location_hint=routing_result["location_hint"],
            location_id=routing_result.get("location_id", ""),
            reply_text=reply_text,
            reply_ok=reply_ok,
        )
        if reply_ok:
            append_routing_log_event(
                user_id=user_id,
                intent_name=routing_result["intent_name"],
                service_row=routing_result["service_row"],
                location_hint=routing_result["location_hint"],
                location_id=routing_result.get("location_id", ""),
                message=text,
                trace_id=trace_id,
            )
        else:
            logger.error(f"[{trace_id}] ROUTING_LOG_APPEND_SKIPPED reason=reply_failed")
    return {"handled": True, "event_type": "message", "message_type": "text", "flow_used": flow_used, "user_ref": user_ref(user_id), "reply_sent": reply_ok}
def dispatch_line_event(event: dict, trace_id: str) -> dict:
    event_type = get_event_type(event)
    logger.info(f"[{trace_id}] LINE_EVENT_DISPATCH event_type={event_type}")
    if event_type != "message":
        return {"handled": False, "event_type": event_type, "reason": "unsupported_event_type"}
    message_type = get_message_type(event)
    if message_type != "text":
        return {"handled": False, "event_type": event_type, "message_type": message_type, "reason": "unsupported_message_type"}
    return dispatch_text_event(event, trace_id)
def verify_internal_sync_token(trace_id: str) -> bool:
    expected = safe_str(INTERNAL_SYNC_TOKEN)
    provided = safe_str(request.headers.get("X-Internal-Sync-Token"))
    if not expected:
        logger.error(f"[{trace_id}] INTERNAL_SYNC_TOKEN_MISSING_IN_ENV")
        return False
    if not provided:
        logger.error(f"[{trace_id}] INTERNAL_SYNC_TOKEN_HEADER_MISSING")
        return False
    ok = hmac.compare_digest(provided, expected)
    logger.info(f"[{trace_id}] INTERNAL_SYNC_TOKEN_CHECK ok={ok}")
    return ok

def resolve_location_alias_writeback_sheet_name(trace_id: str) -> str:
    config_map = load_bot_config_map(trace_id)
    return safe_str(config_map.get("location_alias_sheet")) or "LOCATION_ALIAS_MASTER_V2"

def _normalize_shadow_alias_text(value: str) -> str:
    return normalize_routing_text(value)


def sanitize_shadow_alias_for_writeback(alias_text: str) -> dict:
    raw_alias = safe_str(alias_text)
    normalized_alias = _normalize_shadow_alias_text(raw_alias)
    if not raw_alias or not normalized_alias:
        return {"ok": False, "normalized_alias": normalized_alias, "reason": "empty_alias"}

    connector_patterns = [
        r"\bhoac\b",
        r"\bhay\b",
        r"\bor\b",
        r"\bva\b",
        r"/",
        r",",
        r";",
    ]
    for pattern in connector_patterns:
        if re.search(pattern, normalized_alias):
            return {"ok": False, "normalized_alias": normalized_alias, "reason": "contains_multi_location_connector"}

    generic_aliases = {
        "gan nha may",
        "o cao hung",
        "khu vuc nay",
        "khu do",
        "o day",
        "phong re",
    }
    if normalized_alias in generic_aliases:
        return {"ok": False, "normalized_alias": normalized_alias, "reason": "generic_non_location_phrase"}

    if len(normalized_alias) > 24:
        return {"ok": False, "normalized_alias": normalized_alias, "reason": "alias_too_long"}

    tokens = [token for token in normalized_alias.split() if token]
    if len(tokens) > 3:
        return {"ok": False, "normalized_alias": normalized_alias, "reason": "too_many_tokens"}

    return {"ok": True, "normalized_alias": normalized_alias, "reason": ""}

def detect_raw_text_risk_for_review(shadow_row: dict) -> dict:
    source_candidates = [
        ("location_token_guess", shadow_row.get("location_token_guess")),
        ("raw_text", shadow_row.get("raw_text")),
        ("normalized_text", shadow_row.get("normalized_text")),
    ]
    connector_patterns = [
        r"\bhoac\s+la\b",
        r"\bhoac\b",
        r"\bhay\b",
        r"\bor\b",
        r"\bva\b",
        r"/",
        r",",
        r";",
    ]
    for source_name, raw_value in source_candidates:
        normalized_value = normalize_routing_text(raw_value)
        if not normalized_value:
            continue
        for pattern in connector_patterns:
            if re.search(pattern, normalized_value):
                return {
                    "raw_text_risk_result": "risk_detected",
                    "raw_text_risk_type": "multi_location_input",
                    "raw_text_risk_source": source_name,
                }
    return {
        "raw_text_risk_result": "passed",
        "raw_text_risk_type": "",
        "raw_text_risk_source": "",
    }


def evaluate_routing_admin_review_guard(shadow_row: dict, alias_text: str, alias_source: str, sanitizer_result: dict) -> dict:
    raw_risk = detect_raw_text_risk_for_review(shadow_row)
    raw_text_risk_result = safe_str(raw_risk.get("raw_text_risk_result")) or "passed"
    raw_text_risk_type = safe_str(raw_risk.get("raw_text_risk_type"))
    raw_text_risk_source = safe_str(raw_risk.get("raw_text_risk_source"))
    sanitizer_ok = bool(sanitizer_result.get("ok"))
    sanitizer_reason = safe_str(sanitizer_result.get("reason"))

    if not safe_str(alias_text):
        guard_result = "blocked"
        guard_reason = "missing_alias_text"
    elif not sanitizer_ok:
        guard_result = "blocked"
        if raw_text_risk_result == "risk_detected":
            guard_reason = "alias_source_dirty_and_raw_input_risky"
        else:
            guard_reason = f"alias_sanitizer_rejected:{sanitizer_reason or 'unknown'}"
    elif safe_str(alias_source) == "suggested_alias":
        guard_result = "allowed"
        guard_reason = "alias_clean_but_raw_input_risky" if raw_text_risk_result == "risk_detected" else "alias_clean_and_raw_input_clean"
    elif raw_text_risk_result == "risk_detected":
        guard_result = "blocked"
        guard_reason = "location_token_guess_risky_without_suggested_alias"
    else:
        guard_result = "allowed"
        guard_reason = "location_token_guess_clean"

    log_label = "ROUTING_ADMIN_REVIEW_GUARD_BLOCKED" if guard_result == "blocked" else "ROUTING_ADMIN_REVIEW_GUARD_EVALUATED"
    logger.info(
        f"[{safe_str(shadow_row.get('trace_id'))}] {log_label} "
        f"alias_source={safe_str(alias_source)} "
        f"alias_text={json.dumps(safe_str(alias_text), ensure_ascii=False)} "
        f"alias_sanitizer_result={'passed' if sanitizer_ok else 'rejected'} "
        f"raw_text_risk_result={raw_text_risk_result} "
        f"raw_text_risk_type={raw_text_risk_type} "
        f"raw_text_risk_source={raw_text_risk_source} "
        f"guard_result={guard_result} "
        f"guard_reason={guard_reason}"
    )
    return {
        "raw_text_risk_result": raw_text_risk_result,
        "raw_text_risk_type": raw_text_risk_type,
        "raw_text_risk_source": raw_text_risk_source,
        "guard_result": guard_result,
        "guard_reason": guard_reason,
    }


def _parse_shadow_confidence_to_alias_confidence(value: str) -> str:
    normalized = safe_str(value).lower()
    return "0.95" if normalized in {"high", "medium"} else "0.85"

def get_pending_shadow_writebacks(trace_id: str) -> List[dict]:
    ws = get_routing_worksheet_by_name(trace_id, ROUTING_SHADOW_SUGGESTIONS_SHEET_NAME)
    if not ws:
        return []
    rows = get_records_safe(ws, trace_id, ROUTING_SHADOW_SUGGESTIONS_SHEET_NAME)
    pending = []
    for idx, row in enumerate(rows, start=2):
        review_status = safe_str(row.get("review_status")).lower()
        writeback_status = safe_str(row.get("writeback_status")).lower()
        if review_status != "approved":
            continue
        if writeback_status and writeback_status not in {"pending"}:
            continue
        item = dict(row)
        item["__row_index"] = idx
        pending.append(item)
    logger.info(f"[{trace_id}] SHADOW_WRITEBACK_PENDING_SCAN count={len(pending)}")
    return pending

def find_existing_alias_v2(normalized_alias: str, trace_id: str, worksheet_name: str) -> dict:
    ws = get_routing_worksheet_by_name(trace_id, worksheet_name)
    if not ws:
        return {"status": "worksheet_unavailable"}
    rows = get_records_safe(ws, trace_id, worksheet_name)
    target = _normalize_shadow_alias_text(normalized_alias)
    for row in rows:
        existing_norm = _normalize_shadow_alias_text(row.get("normalized_alias") or row.get("alias_text"))
        if existing_norm != target:
            continue
        return {
            "status": "found",
            "location_id": safe_str(row.get("location_id")),
            "row": row,
        }
    return {"status": "not_found"}

def append_location_alias_v2_from_shadow(shadow_row: dict, trace_id: str, worksheet_name: str) -> dict:
    ws = get_routing_worksheet_by_name(trace_id, worksheet_name)
    if not ws:
        return {
            "ok": False,
            "status": "error",
            "notes": "alias_sheet_unavailable",
            "action": "error",
            "alias_source": "",
            "raw_text_risk_result": "not_run",
            "raw_text_risk_type": "",
            "raw_text_risk_source": "",
            "guard_result": "not_run",
            "guard_reason": "worksheet_unavailable",
            "sanitizer_result": "not_run",
            "sanitizer_reason": "worksheet_unavailable",
            "target_row": "",
        }
    alias_text = safe_str(shadow_row.get("suggested_alias")) or safe_str(shadow_row.get("location_token_guess"))
    alias_source = "suggested_alias" if safe_str(shadow_row.get("suggested_alias")) else "location_token_guess"
    sanitizer_result = sanitize_shadow_alias_for_writeback(alias_text)
    normalized_alias = safe_str(sanitizer_result.get("normalized_alias"))
    sanitizer_reason = safe_str(sanitizer_result.get("reason"))
    guard_eval = evaluate_routing_admin_review_guard(shadow_row, alias_text, alias_source, sanitizer_result)
    location_id = safe_str(shadow_row.get("suggested_location_id"))
    if not alias_text or not normalized_alias or not location_id:
        return {
            "ok": False,
            "status": "error",
            "notes": "missing_required_shadow_fields",
            "action": "error",
            "alias_source": alias_source,
            "raw_text_risk_result": safe_str(guard_eval.get("raw_text_risk_result")),
            "raw_text_risk_type": safe_str(guard_eval.get("raw_text_risk_type")),
            "raw_text_risk_source": safe_str(guard_eval.get("raw_text_risk_source")),
            "guard_result": "blocked",
            "guard_reason": "missing_required_shadow_fields",
            "sanitizer_result": "failed",
            "sanitizer_reason": "missing_required_shadow_fields",
            "target_row": "",
        }
    if safe_str(guard_eval.get("guard_result")) == "blocked":
        guard_reason = safe_str(guard_eval.get("guard_reason")) or "blocked_by_guard"
        blocked_sanitizer_result = "passed" if sanitizer_result.get("ok") else "rejected"
        blocked_sanitizer_reason = "" if sanitizer_result.get("ok") else (sanitizer_reason or "alias_rejected_by_sanitizer")
        logger.info(
            f"[{trace_id}] SHADOW_WRITEBACK_BLOCKED_BY_REVIEW_GUARD worksheet_name={worksheet_name} "
            f"alias_text={json.dumps(alias_text, ensure_ascii=False)} "
            f"alias_source={json.dumps(alias_source, ensure_ascii=False)} "
            f"raw_text_risk_result={safe_str(guard_eval.get('raw_text_risk_result'))} "
            f"raw_text_risk_type={safe_str(guard_eval.get('raw_text_risk_type'))} "
            f"sanitizer_result={blocked_sanitizer_result} "
            f"sanitizer_reason={blocked_sanitizer_reason} "
            f"guard_reason={guard_reason}"
        )
        return {
            "ok": True,
            "status": "blocked_by_guard",
            "notes": f"blocked_by_guard:{guard_reason}",
            "action": "guard_block",
            "alias_source": alias_source,
            "raw_text_risk_result": safe_str(guard_eval.get("raw_text_risk_result")),
            "raw_text_risk_type": safe_str(guard_eval.get("raw_text_risk_type")),
            "raw_text_risk_source": safe_str(guard_eval.get("raw_text_risk_source")),
            "guard_result": safe_str(guard_eval.get("guard_result")),
            "guard_reason": guard_reason,
            "sanitizer_result": blocked_sanitizer_result,
            "sanitizer_reason": blocked_sanitizer_reason,
            "target_row": "",
        }
    if not sanitizer_result.get("ok"):
        reason = sanitizer_reason or "alias_rejected_by_sanitizer"
        logger.info(
            f"[{trace_id}] SHADOW_WRITEBACK_ALIAS_REJECTED worksheet_name={worksheet_name} "
            f"alias_text={json.dumps(alias_text, ensure_ascii=False)} "
            f"alias_source={json.dumps(alias_source, ensure_ascii=False)} "
            f"normalized_alias={json.dumps(normalized_alias, ensure_ascii=False)} "
            f"reason={reason}"
        )
        return {
            "ok": False,
            "status": "error",
            "notes": f"alias_rejected_by_sanitizer:{reason}",
            "action": "reject",
            "alias_source": alias_source,
            "raw_text_risk_result": safe_str(guard_eval.get("raw_text_risk_result")),
            "raw_text_risk_type": safe_str(guard_eval.get("raw_text_risk_type")),
            "raw_text_risk_source": safe_str(guard_eval.get("raw_text_risk_source")),
            "guard_result": safe_str(guard_eval.get("guard_result")),
            "guard_reason": safe_str(guard_eval.get("guard_reason")),
            "sanitizer_result": "rejected",
            "sanitizer_reason": reason,
            "target_row": "",
        }
    duplicate = find_existing_alias_v2(normalized_alias, trace_id, worksheet_name)
    if duplicate.get("status") == "found":
        existing_location_id = safe_str(duplicate.get("location_id"))
        if existing_location_id == location_id:
            return {
                "ok": True,
                "status": "skipped_duplicate",
                "notes": "alias_already_exists_same_mapping",
                "action": "skip_duplicate",
                "alias_source": alias_source,
                "raw_text_risk_result": safe_str(guard_eval.get("raw_text_risk_result")),
                "raw_text_risk_type": safe_str(guard_eval.get("raw_text_risk_type")),
                "raw_text_risk_source": safe_str(guard_eval.get("raw_text_risk_source")),
                "guard_result": safe_str(guard_eval.get("guard_result")),
                "guard_reason": safe_str(guard_eval.get("guard_reason")),
                "sanitizer_result": "passed",
                "sanitizer_reason": "",
                "target_row": "",
            }
        return {
            "ok": False,
            "status": "error",
            "notes": "alias_conflict_existing_mapping",
            "action": "error",
            "alias_source": alias_source,
            "raw_text_risk_result": safe_str(guard_eval.get("raw_text_risk_result")),
            "raw_text_risk_type": safe_str(guard_eval.get("raw_text_risk_type")),
            "raw_text_risk_source": safe_str(guard_eval.get("raw_text_risk_source")),
            "guard_result": safe_str(guard_eval.get("guard_result")),
            "guard_reason": safe_str(guard_eval.get("guard_reason")),
            "sanitizer_result": "passed",
            "sanitizer_reason": "alias_conflict_existing_mapping",
            "target_row": "",
        }
    if duplicate.get("status") == "worksheet_unavailable":
        return {
            "ok": False,
            "status": "error",
            "notes": "alias_sheet_unavailable",
            "action": "error",
            "alias_source": alias_source,
            "raw_text_risk_result": safe_str(guard_eval.get("raw_text_risk_result")),
            "raw_text_risk_type": safe_str(guard_eval.get("raw_text_risk_type")),
            "raw_text_risk_source": safe_str(guard_eval.get("raw_text_risk_source")),
            "guard_result": safe_str(guard_eval.get("guard_result")),
            "guard_reason": safe_str(guard_eval.get("guard_reason")),
            "sanitizer_result": "passed",
            "sanitizer_reason": "alias_sheet_unavailable",
            "target_row": "",
        }
    row = [
        alias_text,
        normalized_alias,
        location_id,
        "vi",
        "shadow_approved_vi",
        _parse_shadow_confidence_to_alias_confidence(shadow_row.get("confidence")),
        f"approved from ROUTING_SHADOW_SUGGESTIONS trace_id={safe_str(shadow_row.get('trace_id'))}",
    ]
    target_row = ""
    try:
        current_values = get_all_values_safe(ws, trace_id, worksheet_name)
        target_row = str(len(current_values) + 1) if current_values else ""
    except Exception:
        target_row = ""
    try:
        append_row_guarded(ws, trace_id, locals().get("worksheet_name", getattr(ws, "title", "unknown")), row, value_input_option="USER_ENTERED")
        _invalidate_worksheet_caches(worksheet_name)
        logger.info(f"[{trace_id}] SHADOW_WRITEBACK_ALIAS_APPEND_OK worksheet_name={worksheet_name} alias_text={json.dumps(alias_text, ensure_ascii=False)} alias_source={json.dumps(alias_source, ensure_ascii=False)} normalized_alias={json.dumps(normalized_alias, ensure_ascii=False)} location_id={json.dumps(location_id, ensure_ascii=False)} target_row={json.dumps(target_row, ensure_ascii=False)}")
        return {
            "ok": True,
            "status": "done",
            "notes": "writeback_ok",
            "action": "append",
            "alias_source": alias_source,
            "raw_text_risk_result": safe_str(guard_eval.get("raw_text_risk_result")),
            "raw_text_risk_type": safe_str(guard_eval.get("raw_text_risk_type")),
            "raw_text_risk_source": safe_str(guard_eval.get("raw_text_risk_source")),
            "guard_result": safe_str(guard_eval.get("guard_result")),
            "guard_reason": safe_str(guard_eval.get("guard_reason")),
            "sanitizer_result": "passed",
            "sanitizer_reason": "",
            "target_row": target_row,
        }
    except Exception as e:
        logger.exception(f"[{trace_id}] SHADOW_WRITEBACK_ALIAS_APPEND_FAILED worksheet_name={worksheet_name} exception={type(e).__name__}:{e}")
        return {
            "ok": False,
            "status": "error",
            "notes": f"append_failed:{type(e).__name__}",
            "action": "error",
            "alias_source": alias_source,
            "raw_text_risk_result": safe_str(guard_eval.get("raw_text_risk_result")),
            "raw_text_risk_type": safe_str(guard_eval.get("raw_text_risk_type")),
            "raw_text_risk_source": safe_str(guard_eval.get("raw_text_risk_source")),
            "guard_result": safe_str(guard_eval.get("guard_result")),
            "guard_reason": safe_str(guard_eval.get("guard_reason")),
            "sanitizer_result": "passed",
            "sanitizer_reason": f"append_failed:{type(e).__name__}",
            "target_row": "",
        }

def mark_shadow_writeback_result(trace_id: str, row_index: int, status: str, target: str, notes: str = "") -> bool:
    ws = get_routing_worksheet_by_name(trace_id, ROUTING_SHADOW_SUGGESTIONS_SHEET_NAME)
    if not ws:
        return False
    return update_row_fields_by_header(
        ws=ws,
        row_index=row_index,
        field_values={
            "writeback_status": safe_str(status),
            "writeback_at": now_tw_iso(),
            "writeback_target": safe_str(target),
            "writeback_notes": safe_str(notes),
        },
        trace_id=trace_id,
        worksheet_name=ROUTING_SHADOW_SUGGESTIONS_SHEET_NAME,
    )

def process_shadow_writeback_batch(trace_id: str, limit: int = 20) -> dict:
    worksheet_name = resolve_location_alias_writeback_sheet_name(trace_id)
    batch_id = f"shwb_{datetime.now(TW_TZ).strftime('%Y%m%d%H%M%S')}_{uuid.uuid4().hex[:8]}"
    pending_rows = get_pending_shadow_writebacks(trace_id)
    summary = {
        "ok": True,
        "worksheet_name": worksheet_name,
        "batch_id": batch_id,
        "processed": 0,
        "done": 0,
        "skipped_duplicate": 0,
        "error": 0,
        "blocked_by_guard": 0,
        "audit_logged": 0,
        "audit_error": 0,
        "items": [],
    }
    for row in pending_rows[:max(1, min(limit, 100))]:
        row_index = int(row.get("__row_index") or 0)
        result = append_location_alias_v2_from_shadow(row, trace_id, worksheet_name)
        status = safe_str(result.get("status")) or ("done" if result.get("ok") else "error")
        notes = safe_str(result.get("notes"))
        action = safe_str(result.get("action")) or status
        sanitizer_result = safe_str(result.get("sanitizer_result")) or "unknown"
        sanitizer_reason = safe_str(result.get("sanitizer_reason"))
        target_row = safe_str(result.get("target_row"))
        alias_source = safe_str(result.get("alias_source")) or ("suggested_alias" if safe_str(row.get("suggested_alias")) else "location_token_guess")
        raw_text_risk_result = safe_str(result.get("raw_text_risk_result"))
        raw_text_risk_type = safe_str(result.get("raw_text_risk_type"))
        raw_text_risk_source = safe_str(result.get("raw_text_risk_source"))
        guard_result = safe_str(result.get("guard_result"))
        guard_reason = safe_str(result.get("guard_reason"))
        mark_shadow_writeback_result(trace_id, row_index, status, worksheet_name, notes)
        audit_ok = append_routing_admin_audit_log(
            trace_id=trace_id,
            batch_id=batch_id,
            shadow_row=row,
            source_row=row_index,
            action=action,
            writeback_status=status,
            sanitizer_result=sanitizer_result,
            sanitizer_reason=sanitizer_reason,
            target_sheet=worksheet_name,
            target_row=target_row,
            notes=notes,
            alias_source=alias_source,
            raw_text_risk_result=raw_text_risk_result,
            raw_text_risk_type=raw_text_risk_type,
            raw_text_risk_source=raw_text_risk_source,
            guard_result=guard_result,
            guard_reason=guard_reason,
        )
        if audit_ok:
            summary["audit_logged"] += 1
        else:
            summary["audit_error"] += 1
        summary["processed"] += 1
        if status == "done":
            summary["done"] += 1
        elif status == "skipped_duplicate":
            summary["skipped_duplicate"] += 1
        elif status == "blocked_by_guard":
            summary["blocked_by_guard"] += 1
        else:
            summary["error"] += 1
            summary["ok"] = False
        summary["items"].append({
            "row_index": row_index,
            "trace_id": safe_str(row.get("trace_id")),
            "alias_text": safe_str(row.get("suggested_alias")) or safe_str(row.get("location_token_guess")),
            "alias_source": alias_source,
            "location_id": safe_str(row.get("suggested_location_id")),
            "raw_text_risk_result": raw_text_risk_result,
            "raw_text_risk_type": raw_text_risk_type,
            "raw_text_risk_source": raw_text_risk_source,
            "guard_result": guard_result,
            "guard_reason": guard_reason,
            "status": status,
            "action": action,
            "sanitizer_result": sanitizer_result,
            "sanitizer_reason": sanitizer_reason,
            "target_row": target_row,
            "audit_logged": audit_ok,
            "notes": notes,
        })
    logger.info(f"[{trace_id}] SHADOW_WRITEBACK_BATCH_DONE batch_id={batch_id} processed={summary['processed']} done={summary['done']} skipped_duplicate={summary['skipped_duplicate']} blocked_by_guard={summary['blocked_by_guard']} error={summary['error']} audit_logged={summary['audit_logged']} audit_error={summary['audit_error']} worksheet_name={worksheet_name}")
    return summary


CLEANUP_TEST_ROWS_VERSION = "ROUTING_ADMIN_CLEANUP_TEST_ROWS_V1"
CLEANUP_TEST_MARKERS = ["DT79_TEST", "cleanup_test", "__TEST__", "[TEST]", " TEST "]
CLEANUP_SHADOW_STATUS = "cleanup_test_archived"

def column_index_to_a1_letter(index_1_based: int) -> str:
    index = int(index_1_based)
    if index <= 0:
        return ""
    letters = ""
    while index:
        index, remainder = divmod(index - 1, 26)
        letters = chr(65 + remainder) + letters
    return letters

def update_row_sparse_fields_by_header(ws, row_index: int, field_values: Dict[str, str], trace_id: str, worksheet_name: str) -> bool:
    values = get_all_values_safe(ws, trace_id, worksheet_name)
    if not values:
        logger.error(f"[{trace_id}] SPARSE_UPDATE_SKIPPED worksheet_name={worksheet_name} reason=empty_values")
        return False
    headers = values[0]
    header_map = build_header_index_map(headers)
    updates = []
    for column_name, value in field_values.items():
        col_idx = header_map.get(normalize_header_key(column_name))
        if col_idx is None:
            logger.error(f"[{trace_id}] SPARSE_UPDATE_COLUMN_MISSING worksheet_name={worksheet_name} column_name={column_name}")
            return False
        col_letter = column_index_to_a1_letter(col_idx + 1)
        if not col_letter:
            logger.error(f"[{trace_id}] SPARSE_UPDATE_COLUMN_INVALID worksheet_name={worksheet_name} column_name={column_name} col_idx={col_idx}")
            return False
        updates.append({
            "range": f"{col_letter}{row_index}:{col_letter}{row_index}",
            "values": [[safe_str(value)]],
        })
    if not updates:
        return True
    try:
        ws.batch_update(updates, value_input_option="USER_ENTERED")
        _invalidate_worksheet_caches(worksheet_name)
        logger.info(
            f"[{trace_id}] SPARSE_UPDATE_OK worksheet_name={worksheet_name} row_index={row_index} "
            f"columns={json.dumps(list(field_values.keys()), ensure_ascii=False)}"
        )
        return True
    except Exception as e:
        logger.exception(f"[{trace_id}] SPARSE_UPDATE_FAILED worksheet_name={worksheet_name} row_index={row_index} exception={type(e).__name__}:{e}")
        return False

def parse_cleanup_row_indexes(value) -> List[int]:
    if value is None:
        return []
    raw_items = value if isinstance(value, list) else [value]
    result = []
    seen = set()
    for item in raw_items:
        if isinstance(item, str) and "," in item:
            candidates = item.split(",")
        else:
            candidates = [item]
        for candidate in candidates:
            try:
                row_index = int(safe_str(candidate))
            except Exception:
                continue
            if row_index >= 2 and row_index not in seen:
                seen.add(row_index)
                result.append(row_index)
    return result

def parse_cleanup_text_list(value) -> List[str]:
    if value is None:
        return []
    raw_items = value if isinstance(value, list) else [value]
    result = []
    seen = set()
    for item in raw_items:
        if isinstance(item, str) and "," in item:
            candidates = item.split(",")
        else:
            candidates = [item]
        for candidate in candidates:
            normalized = safe_str(candidate)
            if normalized and normalized not in seen:
                seen.add(normalized)
                result.append(normalized)
    return result

def row_has_cleanup_marker(row: dict, markers: List[str]) -> Tuple[bool, str]:
    is_test_value = safe_str(row.get("is_test") or row.get("IS_TEST") or row.get("test_flag"))
    if is_test_value.lower() in {"true", "yes", "y", "1", "test"}:
        return True, "is_test_true"
    searchable_fields = [
        "trace_id",
        "user_id",
        "batch_id",
        "raw_text",
        "normalized_text",
        "reviewer_notes",
        "writeback_notes",
        "notes",
        "decision_context",
        "suggestion_reason",
    ]
    combined = " ".join(safe_str(row.get(field)) for field in searchable_fields if safe_str(row.get(field)))
    if not combined:
        return False, ""
    padded_upper = f" {combined.upper()} "
    normalized_combined = normalize_routing_text(combined)
    for marker in markers:
        marker_raw = safe_str(marker)
        if not marker_raw:
            continue
        if marker_raw.upper() in padded_upper:
            return True, f"marker:{marker_raw}"
        marker_norm = normalize_routing_text(marker_raw)
        if marker_norm and marker_norm in normalized_combined:
            return True, f"marker:{marker_raw}"
    if re.search(r"(?<![A-Za-z0-9_])TEST(?![A-Za-z0-9_])", combined, flags=re.IGNORECASE):
        return True, "marker:TEST"
    return False, ""

def row_matches_cleanup_rule(row: dict, row_index: int, explicit_row_indexes: List[int], trace_ids: List[str], markers: List[str]) -> Tuple[bool, str]:
    if explicit_row_indexes and row_index in explicit_row_indexes:
        return True, "explicit_row_index"
    row_trace_id = safe_str(row.get("trace_id"))
    if trace_ids and row_trace_id and row_trace_id in trace_ids:
        return True, "explicit_trace_id"
    if explicit_row_indexes or trace_ids:
        return False, ""
    return row_has_cleanup_marker(row, markers)

def build_cleanup_records_from_values(values: List[List[str]]) -> List[dict]:
    if not values:
        return []
    headers = [safe_str(v) for v in values[0]]
    rows = []
    for row_index, raw_row in enumerate(values[1:], start=2):
        if not any(safe_str(cell) for cell in raw_row):
            continue
        row = {"__row_index": row_index}
        for idx, header in enumerate(headers):
            if not header:
                continue
            row[header] = safe_str(raw_row[idx]) if idx < len(raw_row) else ""
        rows.append(row)
    return rows

def cleanup_routing_sheet_test_rows(
    trace_id: str,
    worksheet_name: str,
    dry_run: bool,
    explicit_row_indexes: List[int],
    trace_ids: List[str],
    markers: List[str],
    limit: int,
) -> dict:
    ws = get_routing_worksheet_by_name(trace_id, worksheet_name)
    result = {
        "worksheet_name": worksheet_name,
        "ok": True,
        "dry_run": bool(dry_run),
        "matched": 0,
        "updated": 0,
        "error": 0,
        "items": [],
    }
    if not ws:
        result["ok"] = False
        result["error"] += 1
        result["items"].append({"status": "error", "reason": "worksheet_unavailable"})
        return result

    values = get_all_values_safe(ws, trace_id, worksheet_name)
    rows = build_cleanup_records_from_values(values)
    selected = []
    for row in rows:
        row_index = int(row.get("__row_index") or 0)
        matched, reason = row_matches_cleanup_rule(row, row_index, explicit_row_indexes, trace_ids, markers)
        if not matched:
            continue
        selected.append((row, reason))
        if len(selected) >= limit:
            break

    result["matched"] = len(selected)
    logger.info(
        f"[{trace_id}] CLEANUP_TEST_ROWS_SCAN_DONE worksheet_name={worksheet_name} dry_run={dry_run} "
        f"matched={len(selected)} limit={limit}"
    )

    for row, reason in selected:
        row_index = int(row.get("__row_index") or 0)
        item = {
            "row_index": row_index,
            "trace_id": safe_str(row.get("trace_id")),
            "match_reason": reason,
            "status_before": safe_str(row.get("writeback_status") or row.get("action")),
        }
        if dry_run:
            item["status"] = "dry_run_matched"
            result["items"].append(item)
            continue

        if worksheet_name == ROUTING_SHADOW_SUGGESTIONS_SHEET_NAME:
            ok = update_row_sparse_fields_by_header(
                ws=ws,
                row_index=row_index,
                field_values={
                    "writeback_status": CLEANUP_SHADOW_STATUS,
                    "writeback_at": now_tw_iso(),
                    "writeback_target": CLEANUP_TEST_ROWS_VERSION,
                    "writeback_notes": f"{CLEANUP_TEST_ROWS_VERSION}: archived test row; match_reason={reason}",
                },
                trace_id=trace_id,
                worksheet_name=worksheet_name,
            )
        elif worksheet_name == ROUTING_ADMIN_AUDIT_LOG_SHEET_NAME:
            previous_notes = safe_str(row.get("notes"))
            cleanup_note = f"{CLEANUP_TEST_ROWS_VERSION}: archived test audit row; match_reason={reason}"
            ok = update_row_sparse_fields_by_header(
                ws=ws,
                row_index=row_index,
                field_values={
                    "notes": f"{previous_notes} | {cleanup_note}" if previous_notes else cleanup_note,
                },
                trace_id=trace_id,
                worksheet_name=worksheet_name,
            )
        else:
            ok = False

        if ok:
            result["updated"] += 1
            item["status"] = "updated"
        else:
            result["error"] += 1
            result["ok"] = False
            item["status"] = "error"
        result["items"].append(item)

    return result

def run_routing_admin_cleanup_test_rows(trace_id: str, payload_json: dict) -> dict:
    dry_run = bool(payload_json.get("dry_run", True))
    raw_limit = payload_json.get("limit", 50)
    try:
        limit = int(raw_limit)
    except Exception:
        limit = 50
    limit = max(1, min(limit, 200))

    target = safe_str(payload_json.get("target") or "all").lower()
    explicit_rows = payload_json.get("row_indexes") or {}
    explicit_trace_ids = payload_json.get("trace_ids") or []
    markers = parse_cleanup_text_list(payload_json.get("markers")) or CLEANUP_TEST_MARKERS

    if isinstance(explicit_rows, dict):
        shadow_row_indexes = parse_cleanup_row_indexes(explicit_rows.get("shadow") or explicit_rows.get(ROUTING_SHADOW_SUGGESTIONS_SHEET_NAME))
        audit_row_indexes = parse_cleanup_row_indexes(explicit_rows.get("audit") or explicit_rows.get(ROUTING_ADMIN_AUDIT_LOG_SHEET_NAME))
    else:
        parsed = parse_cleanup_row_indexes(explicit_rows)
        shadow_row_indexes = parsed
        audit_row_indexes = parsed

    trace_ids = parse_cleanup_text_list(explicit_trace_ids)

    targets = []
    if target in {"all", "routing_admin", "shadow"}:
        targets.append((ROUTING_SHADOW_SUGGESTIONS_SHEET_NAME, shadow_row_indexes))
    if target in {"all", "routing_admin", "audit"}:
        targets.append((ROUTING_ADMIN_AUDIT_LOG_SHEET_NAME, audit_row_indexes))

    summary = {
        "ok": True,
        "version": CLEANUP_TEST_ROWS_VERSION,
        "dry_run": dry_run,
        "target": target,
        "limit": limit,
        "matched": 0,
        "updated": 0,
        "error": 0,
        "sheets": [],
    }

    if not targets:
        summary["ok"] = False
        summary["error"] = 1
        summary["sheets"].append({"ok": False, "reason": "invalid_target", "target": target})
        return summary

    for worksheet_name, row_indexes in targets:
        sheet_result = cleanup_routing_sheet_test_rows(
            trace_id=trace_id,
            worksheet_name=worksheet_name,
            dry_run=dry_run,
            explicit_row_indexes=row_indexes,
            trace_ids=trace_ids,
            markers=markers,
            limit=limit,
        )
        summary["matched"] += int(sheet_result.get("matched", 0) or 0)
        summary["updated"] += int(sheet_result.get("updated", 0) or 0)
        summary["error"] += int(sheet_result.get("error", 0) or 0)
        if not bool(sheet_result.get("ok")):
            summary["ok"] = False
        summary["sheets"].append(sheet_result)

    logger.info(
        f"[{trace_id}] CLEANUP_TEST_ROWS_DONE version={CLEANUP_TEST_ROWS_VERSION} dry_run={dry_run} "
        f"target={target} matched={summary['matched']} updated={summary['updated']} error={summary['error']}"
    )
    return summary


def run_publish_sync_once(trace_id: str) -> dict:
    logger.warning(f"[{trace_id}] PUBLISH_SYNC_FALLBACK_NOT_IMPLEMENTED")
    return {"ok": False, "error": "publish_sync_not_available", "status": "not_implemented"}

def resolve_publish_sync_status_code(sync_result: dict) -> int:
    if not isinstance(sync_result, dict):
        return 500
    if bool(sync_result.get("ok")):
        return 200
    if safe_str(sync_result.get("error")) == "publish_sync_not_available":
        return 501
    return 500


# --- dynamic admin access guard ---
def ensure_admin_access_worksheet(trace_id: str):
    spreadsheet = open_spreadsheet(trace_id)
    if not spreadsheet:
        return None
    try:
        ws = spreadsheet.worksheet(ADMIN_ACCESS_SHEET_NAME)
    except gspread.WorksheetNotFound:
        try:
            ws = spreadsheet.add_worksheet(title=ADMIN_ACCESS_SHEET_NAME, rows=1000, cols=len(ADMIN_ACCESS_HEADERS))
            append_row_guarded(ws, trace_id, ADMIN_ACCESS_SHEET_NAME, ADMIN_ACCESS_HEADERS, value_input_option="USER_ENTERED")
            logger.info(f"[{trace_id}] ADMIN_ACCESS_SHEET_CREATED worksheet_name={ADMIN_ACCESS_SHEET_NAME}")
            return ws
        except Exception as e:
            logger.exception(f"[{trace_id}] ADMIN_ACCESS_SHEET_CREATE_FAILED exception={type(e).__name__}:{e}")
            return None
    except Exception as e:
        logger.exception(f"[{trace_id}] ADMIN_ACCESS_SHEET_OPEN_FAILED exception={type(e).__name__}:{e}")
        return None
    return ws

def get_admin_access_map(trace_id: str) -> dict:
    tenant_id = get_current_tenant_id()
    now = _now_ts()
    if tenant_id in _ADMIN_ACCESS_CACHE and now - float(_ADMIN_ACCESS_CACHE_TS.get(tenant_id, 0.0) or 0.0) < ADMIN_ACCESS_CACHE_TTL_SECONDS:
        return _ADMIN_ACCESS_CACHE.get(tenant_id, {}) or {}
    ws = ensure_admin_access_worksheet(trace_id)
    if not ws:
        return {}
    rows = get_records_safe(ws, trace_id, ADMIN_ACCESS_SHEET_NAME)
    result = {}
    for row in rows:
        if safe_str(row.get("tenant_id")) != tenant_id:
            continue
        line_user_id = safe_str(row.get("line_user_id"))
        if not line_user_id:
            continue
        result[line_user_id] = {
            "role": safe_str(row.get("role")).lower(),
            "status": safe_str(row.get("status")).lower(),
            "revoked_at": safe_str(row.get("revoked_at")),
        }
    _ADMIN_ACCESS_CACHE[tenant_id] = result
    _ADMIN_ACCESS_CACHE_TS[tenant_id] = now
    logger.info(f"[{trace_id}] ADMIN_ACCESS_CACHE_READY tenant_hash={stable_hash(tenant_id)} count={len(result)}")
    return result

def is_admin_allowed(user_id: str, trace_id: str, required_role: str = "admin") -> bool:
    item = get_admin_access_map(trace_id).get(safe_str(user_id))
    if not item:
        logger.warning(f"[{trace_id}] ADMIN_ACCESS_DENIED user_ref={user_ref(user_id)} reason=not_found")
        return False
    if item.get("status") != "active":
        logger.warning(f"[{trace_id}] ADMIN_ACCESS_DENIED user_ref={user_ref(user_id)} reason=status_{item.get('status')}")
        return False
    role = item.get("role")
    if required_role == "owner":
        return role == "owner"
    return role in {"admin", "owner"}

@app.route("/internal/process-shadow-writeback", methods=["POST"])
def internal_process_shadow_writeback():
    trace_id = make_trace_id()
    started = time.perf_counter()
    if not verify_internal_sync_token(trace_id):
        payload = {
            "ok": False,
            "app_version": APP_VERSION,
            "trace_id": trace_id,
            "latency_ms": ms_since(started),
            "error": "unauthorized",
        }
        return jsonify(payload), 401
    payload_json = request.get_json(silent=True) or {}
    limit = payload_json.get("limit", 20)
    try:
        limit = int(limit)
    except Exception:
        limit = 20
    result = process_shadow_writeback_batch(trace_id, limit=limit)
    payload = {
        "ok": bool(result.get("ok")),
        "app_version": APP_VERSION,
        "trace_id": trace_id,
        "latency_ms": ms_since(started),
        "result": result,
    }
    return jsonify(payload), (200 if payload["ok"] else 207)


@app.route("/internal/cleanup-routing-test-rows", methods=["POST"])
def internal_cleanup_routing_test_rows():
    trace_id = make_trace_id()
    started = time.perf_counter()
    if not verify_internal_sync_token(trace_id):
        payload = {
            "ok": False,
            "app_version": APP_VERSION,
            "trace_id": trace_id,
            "latency_ms": ms_since(started),
            "error": "unauthorized",
        }
        return jsonify(payload), 401
    payload_json = request.get_json(silent=True) or {}
    result = run_routing_admin_cleanup_test_rows(trace_id, payload_json)
    payload = {
        "ok": bool(result.get("ok")),
        "app_version": APP_VERSION,
        "trace_id": trace_id,
        "latency_ms": ms_since(started),
        "result": result,
    }
    return jsonify(payload), (200 if payload["ok"] else 207)

@app.route("/internal/publish-sync", methods=["POST"])
def internal_publish_sync():
    trace_id = make_trace_id()
    started = time.perf_counter()
    if not verify_internal_sync_token(trace_id):
        payload = {
            "ok": False,
            "app_version": APP_VERSION,
            "trace_id": trace_id,
            "latency_ms": ms_since(started),
            "error": "unauthorized",
        }
        return jsonify(payload), 401
    sync_result = run_publish_sync_once(trace_id)
    status_code = resolve_publish_sync_status_code(sync_result)
    payload = {
        "ok": bool(sync_result.get("ok")),
        "app_version": APP_VERSION,
        "trace_id": trace_id,
        "latency_ms": ms_since(started),
        "result": sync_result,
    }
    return jsonify(payload), status_code
@app.route("/callback", methods=["POST"])
def callback():
    start_async_log_worker()
    trace_id = make_trace_id()
    started = time.perf_counter()
    raw_body = request.get_data() or b""
    signature = safe_str(request.headers.get("X-Line-Signature"))
    logger.info(f"[{trace_id}] CALLBACK_IN content_length={len(raw_body)} signature_present={bool(signature)}")
    if not verify_line_signature(raw_body, signature, trace_id):
        logger.error(f"[{trace_id}] CALLBACK_REJECT_INVALID_SIGNATURE")
        return jsonify({"ok": False, "trace_id": trace_id, "error": "invalid_signature"}), 403
    payload = parse_line_webhook_payload(raw_body, trace_id)
    events = payload.get("events")
    if not isinstance(events, list):
        logger.error(f"[{trace_id}] CALLBACK_INVALID_EVENTS_TYPE")
        return jsonify({"ok": False, "trace_id": trace_id, "error": "invalid_events_type"}), 400
    if len(events) == 0:
        latency_ms = ms_since(started)
        logger.info(f"[{trace_id}] CALLBACK_VERIFY_EMPTY_EVENTS_OK latency_ms={latency_ms}")
        return jsonify({"ok": True, "app_version": APP_VERSION, "trace_id": trace_id, "latency_ms": latency_ms, "event_count": 0, "results": [], "reason": "empty_events_verify_ok"}), 200
    results = []
    for event in events:
        set_current_tenant_id_from_event(event, trace_id)
        event_key = get_event_unique_key(event)
        try:
            can_process, processing_reason, processing_event_key = begin_event_processing(event, trace_id)
            if not can_process:
                logger.info(f"[{trace_id}] CALLBACK_EVENT_DUPLICATE_SKIP event_key={processing_event_key} reason={processing_reason}")
                results.append({"handled": False, "reason": processing_reason, "event_key": processing_event_key})
                continue
            result = dispatch_line_event(event, trace_id)
            event_success = bool(result.get("handled")) and bool(result.get("reply_sent", True))
            finalize_event_processing(event, trace_id, success=event_success)
            results.append(result)
        except GSheetCircuitOpenError as e:
            finalize_event_processing(event, trace_id, success=False)
            logger.error(f"[{trace_id}] CALLBACK_EVENT_GSHEET_CIRCUIT_OPEN event_key={event_key} exception={type(e).__name__}:{e}")
            results.append({"handled": False, "reason": "gsheet_circuit_open", "event_key": event_key})
        except (gspread.WorksheetNotFound, gspread.exceptions.APIError, requests.RequestException, ValueError, KeyError, TypeError) as e:
            finalize_event_processing(event, trace_id, success=False)
            logger.exception(f"[{trace_id}] CALLBACK_EVENT_KNOWN_EXCEPTION event_key={event_key} exception={type(e).__name__}:{e}")
            results.append({"handled": False, "reason": "known_exception", "event_key": event_key, "exception_type": type(e).__name__})
        except Exception as e:
            finalize_event_processing(event, trace_id, success=False)
            logger.exception(f"[{trace_id}] CALLBACK_EVENT_UNKNOWN_EXCEPTION event_key={event_key} exception={type(e).__name__}:{e}")
            results.append({"handled": False, "reason": "unknown_exception", "event_key": event_key})
    latency_ms = ms_since(started)
    logger.info(f"[{trace_id}] CALLBACK_DONE events={len(events)} latency_ms={latency_ms}")
    return jsonify({"ok": True, "app_version": APP_VERSION, "trace_id": trace_id, "latency_ms": latency_ms, "event_count": len(events), "results": results}), 200
try:
    with app.app_context():
        warm_up_cache()
except Exception as exc:
    logger.exception(f"APP_WARM_UP_CACHE_FAILED exception={type(exc).__name__}:{exc}")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")))
