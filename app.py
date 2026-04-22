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
import unicodedata
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

APP_VERSION = "PHASE1_RUNTIME_STATE_SAFE__RESTART_SAFE_DEDUP_SHEET_V46__ROUTING_V1"
TW_TZ = timezone(timedelta(hours=8))
LOCKED_TARGET_LANG = "zh-TW"

CONNECT_TIMEOUT_SECONDS = int(os.getenv("CONNECT_TIMEOUT_SECONDS", "3").strip() or "3")
READ_TIMEOUT_SECONDS = int(os.getenv("READ_TIMEOUT_SECONDS", "8").strip() or "8")
OUTBOUND_TIMEOUT = (CONNECT_TIMEOUT_SECONDS, READ_TIMEOUT_SECONDS)

FALLBACK_REPLY_TEXT = "Hệ thống bận, thử lại sau."
LINE_TEXT_HARD_LIMIT = 5000
RATE_LIMIT_STORE_MAX_KEYS = 5000
ERROR_BODY_LOG_LIMIT = 800
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
ROUTING_SPREADSHEET_NAME = os.getenv("ROUTING_SPREADSHEET_NAME", "DT79_BOT_TRANSLATOR_DATABASE").strip() or "DT79_BOT_TRANSLATOR_DATABASE"
ROUTING_FALLBACK_LOCATION = os.getenv("ROUTING_FALLBACK_LOCATION", "TW_ALL").strip() or "TW_ALL"
ROUTING_REPLY_PREFIX_BY_LANGUAGE = {
    "vi": "Liên hệ LINE",
    "id": "Kontak LINE",
    "th": "ติดต่อ LINE",
    "zh": "LINE 聯絡",
}
LOCATION_ALIAS_MAP = {
    "台中": ["台中", "đài trung", "dai trung", "taichung", "taizhong"],
    "台南": ["台南", "đài nam", "dai nam", "tainan"],
    "高雄": ["高雄", "cao hùng", "cao hung", "kaohsiung"],
    "台北": ["台北", "đài bắc", "dai bac", "taipei"],
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
    return f"trc_{uuid.uuid4().hex[:12]}"

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

def _now_ts() -> float:
    return time.time()

def _cache_is_fresh(loaded_at_ts: float, ttl_seconds: int) -> bool:
    return bool(loaded_at_ts) and (_now_ts() - loaded_at_ts) < ttl_seconds

def _is_gsheet_quota_error(exc: Exception) -> bool:
    text = safe_str(exc)
    return "429" in text or "Quota exceeded" in text or "Read requests per minute per user" in text

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
        spreadsheet = client.open(PHASE1_SPREADSHEET_NAME)
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
        ws = spreadsheet.worksheet(worksheet_name)
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
        values = ws.get_all_values()
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
        ws.update(target_range, [row_values], value_input_option="USER_ENTERED")
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
        spreadsheet = client.open(ROUTING_SPREADSHEET_NAME)
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
        ws = spreadsheet.worksheet(worksheet_name)
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


def load_bot_config_map(trace_id: str) -> Dict[str, str]:
    ws = get_routing_worksheet_by_name(trace_id, BOT_CONFIG_SHEET_NAME)
    if not ws:
        return {}
    rows = get_records_safe(ws, trace_id, BOT_CONFIG_SHEET_NAME)
    config_map = {}
    for row in rows:
        key = safe_str(row.get("Key") or row.get("key")).strip()
        value = safe_str(row.get("Value") or row.get("value")).strip()
        if key:
            config_map[key] = value
    logger.info(f"[{trace_id}] ROUTING_CONFIG_READY keys={json.dumps(sorted(config_map.keys()), ensure_ascii=False)}")
    return config_map


def normalize_routing_text(value: str) -> str:
    normalized = _normalize_match_text(value)
    normalized = normalized.replace("đ", "d").replace("Đ", "D").lower()
    return normalized


def extract_location_alias(text: str) -> str:
    normalized = normalize_routing_text(text)
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
        location = safe_str(row.get("location"))
        priority = parse_priority(row.get("priority"))
        if normalized_location and location == normalized_location:
            exact_matches.append((priority, row))
        elif location == ROUTING_FALLBACK_LOCATION:
            fallback_matches.append((priority, row))

    if exact_matches:
        exact_matches.sort(key=lambda item: (-item[0], safe_str(item[1].get("service_id"))))
        return exact_matches[0][1]
    if fallback_matches:
        fallback_matches.sort(key=lambda item: (-item[0], safe_str(item[1].get("service_id"))))
        return fallback_matches[0][1]
    return None


def build_routing_reply(service_row: dict, language_group: str) -> str:
    lang = normalize_language_group(language_group)
    prefix = ROUTING_REPLY_PREFIX_BY_LANGUAGE.get(lang, ROUTING_REPLY_PREFIX_BY_LANGUAGE["vi"])
    contact_id = safe_str(service_row.get("contact_id"))
    service_name = safe_str(service_row.get("service_name"))
    location = safe_str(service_row.get("location"))
    scope = safe_str(service_row.get("service_scope"))
    lines = [f"{prefix}: {contact_id}"]
    if service_name:
        lines.append(f"Dịch vụ: {service_name}")
    if location:
        lines.append(f"Khu vực: {location}")
    if scope:
        lines.append(f"Phạm vi: {scope}")
    return "\n".join(lines)[:LINE_TEXT_HARD_LIMIT]


def try_build_routing_reply(text: str, language_group: str, trace_id: str) -> Optional[str]:
    config_map = load_bot_config_map(trace_id)
    intent_sheet_name = safe_str(config_map.get("intent_sheet")) or INTENT_MASTER_SHEET_NAME
    service_sheet_name = safe_str(config_map.get("service_sheet")) or SERVICE_MASTER_SHEET_NAME
    fallback_location = safe_str(config_map.get("fallback_location")) or ROUTING_FALLBACK_LOCATION

    intent_ws = get_routing_worksheet_by_name(trace_id, intent_sheet_name)
    service_ws = get_routing_worksheet_by_name(trace_id, service_sheet_name)
    if not intent_ws or not service_ws:
        logger.warning(f"[{trace_id}] ROUTING_SHEET_UNAVAILABLE intent_ws={bool(intent_ws)} service_ws={bool(service_ws)}")
        return None

    intent_rows = get_records_safe(intent_ws, trace_id, intent_sheet_name)
    service_rows = get_records_safe(service_ws, trace_id, service_sheet_name)
    intent_name, matched_keywords = detect_routing_intent(text, intent_rows)
    if not intent_name:
        logger.info(f"[{trace_id}] ROUTING_INTENT_MISS text={json.dumps(text, ensure_ascii=False)}")
        return None

    location_hint = extract_location_alias(text)
    service = choose_service_for_intent(intent_name, location_hint, service_rows)
    if not service and location_hint != fallback_location:
        service = choose_service_for_intent(intent_name, fallback_location, service_rows)
    if not service:
        logger.info(
            f"[{trace_id}] ROUTING_SERVICE_MISS intent_name={intent_name} "
            f"location_hint={json.dumps(location_hint, ensure_ascii=False)} "
            f"matched_keywords={json.dumps(matched_keywords, ensure_ascii=False)}"
        )
        return None

    logger.info(
        f"[{trace_id}] ROUTING_MATCH_OK intent_name={intent_name} "
        f"service_id={safe_str(service.get('service_id'))} "
        f"contact_id={safe_str(service.get('contact_id'))} "
        f"location_hint={json.dumps(location_hint, ensure_ascii=False)} "
        f"matched_keywords={json.dumps(matched_keywords, ensure_ascii=False)}"
    )
    return build_routing_reply(service, language_group)

_ADS_CATALOG_CACHE = {"rows": [], "loaded_at_ts": 0, "last_read_ok": False}
_ADS_VIEW_CACHE: Dict[str, dict] = {}
_ADS_DETAIL_CACHE: Dict[str, dict] = {}
_WORKSPACE_VALIDATION_CACHE = {"result": None, "loaded_at_ts": 0}
_USER_FLOW_STATE: Dict[str, dict] = {}

USER_STATE_HEADERS = ["user_id", "flow", "updated_at", "language_group"]
PROCESSED_EVENT_HEADERS = ["event_key", "processed_at", "trace_id", "webhook_event_id", "message_id", "reply_token", "user_id", "event_type"]
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
            ws.append_row(PROCESSED_EVENT_HEADERS, value_input_option="USER_ENTERED")
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
            ws.append_row(PROCESSED_EVENT_HEADERS, value_input_option="USER_ENTERED")
            logger.info(f"[{trace_id}] PROCESSED_EVENT_HEADERS_INIT_OK")
        except Exception as e:
            logger.exception(f"[{trace_id}] PROCESSED_EVENT_HEADERS_INIT_FAILED exception={type(e).__name__}:{e}")
            return None
        return ws
    return ws

def get_event_unique_key(event: dict) -> str:
    webhook_event_id = safe_str(event.get("webhookEventId"))
    if webhook_event_id:
        return f"webhook:{webhook_event_id}"
    message = event.get("message") or {}
    message_id = safe_str(message.get("id"))
    if message_id:
        return f"message:{message_id}"
    reply_token = safe_str(event.get("replyToken"))
    if reply_token:
        return f"reply:{reply_token}"
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
        "reply_token": safe_str(event.get("replyToken")),
        "user_id": get_event_user_id(event),
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
            ws.append_row(row, value_input_option="USER_ENTERED")
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

def finalize_event_processing(event: dict, trace_id: str, success: bool) -> None:
    event_key = get_event_unique_key(event)
    if not event_key:
        logger.info(f"[{trace_id}] EVENT_PROCESSING_FINALIZE_SKIPPED reason=missing_event_key")
        return
    lookup = get_persistent_processed_event_record(event_key, trace_id)
    row_index = int(lookup.get("row_index", 0) or 0)
    ws = ensure_processed_event_worksheet(trace_id)
    marker = make_done_marker() if success else make_failed_marker()
    target_status = "done" if success else "failed"

    persist_ok = False
    if ws and row_index > 0:
        persist_ok = update_row_fields_by_header(ws, row_index, {"processed_at": marker, "trace_id": trace_id}, trace_id, PROCESSED_EVENT_SHEET_NAME)
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
            ws.append_row(row, value_input_option="USER_ENTERED")
            persist_ok = True
        except Exception as e:
            logger.exception(f"[{trace_id}] EVENT_PROCESSING_FINALIZE_APPEND_FAILED event_key={event_key} exception={type(e).__name__}:{e}")
            persist_ok = False

    if success:
        set_processed_event_runtime_state(event_key, target_status, trace_id)
    else:
        clear_processed_event_runtime_state(event_key, trace_id)

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
    item = _USER_LANGUAGE_STATE.get(safe_str(user_id))
    if not item:
        logger.info(f"[{trace_id}] USER_LANGUAGE_STATE_MISS user_id={user_id}")
        return ""
    language_group = safe_str(item.get("language_group"))
    logger.info(f"[{trace_id}] USER_LANGUAGE_STATE_HIT user_id={user_id} language_group={language_group}")
    return language_group

def set_runtime_user_language(user_id: str, language_group: str, trace_id: str) -> None:
    normalized_user_id = safe_str(user_id)
    normalized_language = normalize_language_group(language_group)
    if not normalized_user_id:
        logger.error(f"[{trace_id}] USER_LANGUAGE_STATE_SET_SKIPPED reason=missing_user_id")
        return
    prune_runtime_user_language_state(trace_id)
    _USER_LANGUAGE_STATE[normalized_user_id] = {"language_group": normalized_language, "updated_at_ts": get_now_ts()}
    logger.info(f"[{trace_id}] USER_LANGUAGE_STATE_SET user_id={normalized_user_id} language_group={normalized_language}")

def ensure_user_language_worksheet(trace_id: str):
    return ensure_user_state_worksheet(trace_id)

def get_persistent_user_language(user_id: str, trace_id: str) -> str:
    normalized_user_id = safe_str(user_id)
    if not normalized_user_id:
        return ""
    ws = ensure_user_language_worksheet(trace_id)
    if not ws:
        logger.error(f"[{trace_id}] USER_LANGUAGE_PERSIST_READ_SKIPPED reason=worksheet_unavailable")
        return ""
    records = get_records_safe(ws, trace_id, USER_STATE_SHEET_NAME)
    for row in records:
        if safe_str(row.get("user_id")) == normalized_user_id:
            language_group = normalize_language_group(row.get("language_group"))
            logger.info(f"[{trace_id}] USER_LANGUAGE_PERSIST_HIT user_id={normalized_user_id} language_group={language_group}")
            return language_group
    logger.info(f"[{trace_id}] USER_LANGUAGE_PERSIST_MISS user_id={normalized_user_id}")
    return ""

def set_persistent_user_language(user_id: str, language_group: str, trace_id: str) -> bool:
    normalized_user_id = safe_str(user_id)
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
        ws.append_row([normalized_user_id, "", now_iso, normalized_language], value_input_option="USER_ENTERED")
        logger.info(f"[{trace_id}] USER_LANGUAGE_PERSIST_APPEND_OK user_id={normalized_user_id} language_group={normalized_language}")
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
        logger.info(f"[{trace_id}] USER_LANGUAGE_RESTORED_FROM_SHEET user_id={safe_str(user_id)} language_group={persistent_language}")
        return persistent_language
    return DEFAULT_LANGUAGE_GROUP

def persist_user_language(user_id: str, language_group: str, trace_id: str) -> bool:
    normalized_language = normalize_language_group(language_group)
    persist_ok = set_persistent_user_language(user_id, normalized_language, trace_id)
    if persist_ok:
        set_runtime_user_language(user_id, normalized_language, trace_id)
    else:
        _USER_LANGUAGE_STATE.pop(safe_str(user_id), None)
    logger.info(f"[{trace_id}] USER_LANGUAGE_PERSIST_RESULT user_id={safe_str(user_id)} language_group={normalized_language} ok={persist_ok}")
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
            ws.append_row(USER_STATE_HEADERS, value_input_option="USER_ENTERED")
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
            ws.append_row(USER_STATE_HEADERS, value_input_option="USER_ENTERED")
            logger.info(f"[{trace_id}] USER_STATE_HEADERS_INIT_OK")
        except Exception as e:
            logger.exception(f"[{trace_id}] USER_STATE_HEADERS_INIT_FAILED exception={type(e).__name__}:{e}")
            return None
        return ws
    return ws

def get_persistent_user_flow(user_id: str, trace_id: str) -> str:
    normalized_user_id = safe_str(user_id)
    if not normalized_user_id:
        return ""
    ws = ensure_user_state_worksheet(trace_id)
    if not ws:
        logger.error(f"[{trace_id}] USER_STATE_PERSIST_READ_SKIPPED reason=worksheet_unavailable")
        return ""
    records = get_records_safe(ws, trace_id, USER_STATE_SHEET_NAME)
    for row in records:
        if safe_str(row.get("user_id")) != normalized_user_id:
            continue
        flow = safe_str(row.get("flow"))
        updated_at = safe_str(row.get("updated_at"))
        if flow and not is_supported_flow(flow):
            logger.warning(f"[{trace_id}] USER_STATE_PERSIST_UNSUPPORTED_FLOW user_id={normalized_user_id} flow={flow}")
            clear_persistent_user_flow(normalized_user_id, trace_id)
            return ""
        if flow and is_persistent_flow_expired(updated_at):
            logger.info(f"[{trace_id}] USER_STATE_PERSIST_EXPIRED user_id={normalized_user_id} flow={flow} updated_at={json.dumps(updated_at, ensure_ascii=False)} ttl_seconds={PERSISTENT_FLOW_TTL_SECONDS}")
            clear_persistent_user_flow(normalized_user_id, trace_id)
            return ""
        logger.info(f"[{trace_id}] USER_STATE_PERSIST_HIT user_id={normalized_user_id} flow={flow}")
        return flow
    logger.info(f"[{trace_id}] USER_STATE_PERSIST_MISS user_id={normalized_user_id}")
    return ""

def set_persistent_user_flow(user_id: str, flow: str, trace_id: str) -> bool:
    normalized_user_id = safe_str(user_id)
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
        ws.append_row([normalized_user_id, normalized_flow, now_iso, ""], value_input_option="USER_ENTERED")
        logger.info(f"[{trace_id}] USER_STATE_PERSIST_APPEND_OK user_id={normalized_user_id} flow={normalized_flow}")
        return True
    except Exception as e:
        logger.exception(f"[{trace_id}] USER_STATE_PERSIST_APPEND_FAILED exception={type(e).__name__}:{e}")
        return False

def clear_persistent_user_flow(user_id: str, trace_id: str) -> bool:
    normalized_user_id = safe_str(user_id)
    if not normalized_user_id:
        logger.error(f"[{trace_id}] USER_STATE_PERSIST_CLEAR_SKIPPED reason=missing_user_id")
        return False
    ws = ensure_user_state_worksheet(trace_id)
    if not ws:
        logger.error(f"[{trace_id}] USER_STATE_PERSIST_CLEAR_SKIPPED reason=worksheet_unavailable")
        return False
    row_index = find_first_row_index_by_column_value(ws=ws, column_name="user_id", expected_value=normalized_user_id, trace_id=trace_id, worksheet_name=USER_STATE_SHEET_NAME)
    if not row_index:
        logger.info(f"[{trace_id}] USER_STATE_PERSIST_CLEAR_MISS user_id={normalized_user_id}")
        return True
    ok = update_row_fields_by_header(ws, row_index, {"flow": "", "updated_at": now_tw_iso()}, trace_id, USER_STATE_SHEET_NAME)
    if ok:
        logger.info(f"[{trace_id}] USER_STATE_PERSIST_CLEAR_OK user_id={normalized_user_id} row_index={row_index}")
    return ok

def clear_runtime_user_flow(user_id: str, trace_id: str) -> None:
    normalized_user_id = safe_str(user_id)
    if not normalized_user_id:
        logger.error(f"[{trace_id}] USER_FLOW_STATE_CLEAR_SKIPPED reason=missing_user_id")
        return
    existed = normalized_user_id in _USER_FLOW_STATE
    _USER_FLOW_STATE.pop(normalized_user_id, None)
    logger.info(f"[{trace_id}] USER_FLOW_STATE_CLEARED user_id={normalized_user_id} existed={existed}")

def clear_user_flow(user_id: str, trace_id: str) -> bool:
    persist_ok = clear_persistent_user_flow(user_id, trace_id)
    if persist_ok:
        clear_runtime_user_flow(user_id, trace_id)
    logger.info(f"[{trace_id}] USER_STATE_CLEAR_RESULT user_id={safe_str(user_id)} ok={persist_ok}")
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
    item = _USER_FLOW_STATE.get(safe_str(user_id))
    if not item:
        logger.info(f"[{trace_id}] USER_FLOW_STATE_MISS user_id={user_id}")
        return ""
    flow = safe_str(item.get("flow"))
    logger.info(f"[{trace_id}] USER_FLOW_STATE_HIT user_id={user_id} flow={flow}")
    return flow

def set_runtime_user_flow(user_id: str, flow: str, trace_id: str) -> None:
    normalized_user_id = safe_str(user_id)
    normalized_flow = safe_str(flow)
    if not normalized_user_id:
        logger.error(f"[{trace_id}] USER_FLOW_STATE_SET_SKIPPED reason=missing_user_id")
        return
    prune_runtime_user_flow_state(trace_id)
    _USER_FLOW_STATE[normalized_user_id] = {"flow": normalized_flow, "updated_at_ts": get_now_ts()}
    logger.info(f"[{trace_id}] USER_FLOW_STATE_SET user_id={normalized_user_id} flow={normalized_flow}")

def resolve_user_flow(user_id: str, trace_id: str) -> str:
    runtime_flow = get_runtime_user_flow(user_id, trace_id)
    if runtime_flow:
        return runtime_flow
    persistent_flow = get_persistent_user_flow(user_id, trace_id)
    if persistent_flow:
        set_runtime_user_flow(user_id, persistent_flow, trace_id)
        logger.info(f"[{trace_id}] USER_STATE_RESTORED_FROM_SHEET user_id={safe_str(user_id)} flow={persistent_flow}")
        return persistent_flow
    return ""

def persist_user_flow(user_id: str, flow: str, trace_id: str) -> bool:
    persist_ok = set_persistent_user_flow(user_id, flow, trace_id)
    if persist_ok:
        set_runtime_user_flow(user_id, flow, trace_id)
    else:
        _USER_FLOW_STATE.pop(safe_str(user_id), None)
    logger.info(f"[{trace_id}] USER_STATE_PERSIST_RESULT user_id={safe_str(user_id)} flow={safe_str(flow)} ok={persist_ok}")
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
        spreadsheet = client.open(PHASE1_SPREADSHEET_NAME)
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
            f"user_id={normalized_user_id} language_group={normalized_language} "
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
    cache_key = f"{safe_str(user_id)}::{normalize_language_group(language_group)}"
    _ADS_VIEW_CACHE[cache_key] = {"rows": rows, "loaded_at_ts": get_now_ts()}
    logger.info(f"[{trace_id}] ADS_VIEW_CACHE_SET cache_key={cache_key} rows={len(rows)}")


def get_ads_view_cache(user_id: str, language_group: str, trace_id: str) -> list:
    cache_key = f"{safe_str(user_id)}::{normalize_language_group(language_group)}"
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
        safe_str(viewer_user_id),
        normalize_language_group(viewer_language_group),
        safe_str(action_type),
        trace_id,
    ]
    try:
        ws.append_row(row, value_input_option="USER_ENTERED")
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


def dispatch_text_event(event: dict, trace_id: str) -> dict:
    user_id = get_event_user_id(event)
    reply_token = get_reply_token(event)
    text = get_message_text(event)
    normalized = normalize_command_text(text)
    current_flow = resolve_user_flow(user_id, trace_id)
    current_language = resolve_user_language(user_id, trace_id)
    requested_language = parse_lang_command(text)
    logger.info(f"[{trace_id}] LINE_TEXT_DISPATCH user_id={user_id} reply_token_present={bool(reply_token)} text={json.dumps(text, ensure_ascii=False)} normalized={json.dumps(normalized, ensure_ascii=False)} current_flow={current_flow} current_language={current_language}")

    reply_language = current_language

    if normalized == WORKER_ENTRY_COMMAND:
        persist_ok = persist_user_flow(user_id, FLOW_WORKER, trace_id)
        if not persist_ok:
            logger.error(f"[{trace_id}] USER_STATE_PERSIST_FAILED command=/worker user_id={user_id}")
            reply_text = handle_state_save_failed_message(current_language)
            flow_used = "persist_failed"
        else:
            reply_text = handle_worker_entry(current_language)
            flow_used = FLOW_WORKER
    elif normalized == ADS_ENTRY_COMMAND:
        persist_ok = persist_user_flow(user_id, FLOW_ADS, trace_id)
        if not persist_ok:
            logger.error(f"[{trace_id}] USER_STATE_PERSIST_FAILED command=/ads user_id={user_id}")
            reply_text = handle_state_save_failed_message(current_language)
            flow_used = "persist_failed"
        else:
            reply_text, ads_read_ok = load_ads_reply_message(user_id, current_language, trace_id)
            flow_used = FLOW_ADS if ads_read_ok else "ads_read_failed"
    elif normalized in {RESET_ENTRY_COMMAND, EXIT_ENTRY_COMMAND}:
        clear_ok = clear_user_flow(user_id, trace_id)
        if not clear_ok:
            logger.error(f"[{trace_id}] USER_STATE_CLEAR_FAILED command={normalized} user_id={user_id}")
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
                logger.error(f"[{trace_id}] USER_LANGUAGE_PERSIST_FAILED command=/lang user_id={user_id} requested_language={requested_language}")
                reply_text = handle_state_save_failed_message(current_language)
                flow_used = current_flow or "lang_persist_failed"
            else:
                switch_ok = switch_user_rich_menu(user_id, requested_language, trace_id)
                if not switch_ok:
                    logger.error(
                        f"[{trace_id}] RICH_MENU_SWITCH_FAILED_AFTER_LANG_SAVE "
                        f"user_id={user_id} requested_language={requested_language}"
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
    elif current_flow == FLOW_WORKER:
        reply_text = handle_worker_message(text, current_language)
        flow_used = FLOW_WORKER
    elif current_flow == FLOW_ADS and normalized.isdigit():
        reply_text = handle_ads_numeric_selection(user_id, normalized, current_language, trace_id)
        clear_user_flow(user_id, trace_id)
        flow_used = "ads_detail_view"
    elif current_flow == FLOW_ADS:
        clear_user_flow(user_id, trace_id)
        routing_reply = try_build_routing_reply(text, current_language, trace_id)
        if routing_reply:
            reply_text = routing_reply
            flow_used = "ads_auto_cleared_routed"
        else:
            reply_text = build_default_intent_reply(text, current_language, trace_id)
            flow_used = "ads_auto_cleared"
    else:
        routing_reply = try_build_routing_reply(text, current_language, trace_id)
        if routing_reply:
            reply_text = routing_reply
            flow_used = "routing"
        else:
            reply_text = build_default_intent_reply(text, current_language, trace_id)
            flow_used = "default"

    reply_ok = reply_line_text(reply_token, reply_text, trace_id, reply_language)
    return {"handled": True, "event_type": "message", "message_type": "text", "flow_used": flow_used, "user_id": user_id, "reply_sent": reply_ok}


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
        event_key = get_event_unique_key(event)
        try:
            can_process, processing_reason, processing_event_key = begin_event_processing(event, trace_id)
            if not can_process:
                logger.info(f"[{trace_id}] CALLBACK_EVENT_DUPLICATE_SKIP event_key={processing_event_key} reason={processing_reason}")
                results.append({"handled": False, "reason": processing_reason, "event_key": processing_event_key})
                continue
            result = dispatch_line_event(event, trace_id)
            finalize_event_processing(event, trace_id, success=bool(result.get("handled")))
            results.append(result)
        except Exception as e:
            finalize_event_processing(event, trace_id, success=False)
            logger.exception(f"[{trace_id}] CALLBACK_EVENT_EXCEPTION event_key={event_key} exception={type(e).__name__}:{e}")
            results.append({"handled": False, "reason": "event_exception", "event_key": event_key})
    latency_ms = ms_since(started)
    logger.info(f"[{trace_id}] CALLBACK_DONE events={len(events)} latency_ms={latency_ms}")
    return jsonify({"ok": True, "app_version": APP_VERSION, "trace_id": trace_id, "latency_ms": latency_ms, "event_count": len(events), "results": results}), 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")))
