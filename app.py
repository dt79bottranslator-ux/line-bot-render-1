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
from datetime import datetime, timezone, timedelta
from typing import Dict, Tuple, Optional, List

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
# BASIC HELPER NEEDED EARLY
# =========================================================
def safe_str(v) -> str:
    return str(v).strip() if v else ""


# =========================================================
# ENV
# =========================================================
LINE_CHANNEL_ACCESS_TOKEN = os.getenv("LINE_CHANNEL_ACCESS_TOKEN", "").strip()
LINE_CHANNEL_SECRET = os.getenv("LINE_CHANNEL_SECRET", "").strip()
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY", "").strip()

GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
PHASE1_SPREADSHEET_NAME = os.getenv("PHASE1_SPREADSHEET_NAME", "DT79_PHASE1_WORKER_CASES_V1").strip()
USER_STATE_SHEET_NAME = "user_state"

ADMIN_IDS = os.getenv("ADMIN_IDS", "").strip()
ADMIN_LIST = [x.strip() for x in ADMIN_IDS.split(",") if x.strip()]

ALL_COOLDOWN_SECONDS = int(os.getenv("ALL_COOLDOWN_SECONDS", "15").strip() or "15")
MAX_ALL_CHARS = int(os.getenv("MAX_ALL_CHARS", "500").strip() or "500")

RUNTIME_STATE_TTL_SECONDS = int(os.getenv("RUNTIME_STATE_TTL_SECONDS", "1800").strip() or "1800")
RUNTIME_STATE_MAX_KEYS = int(os.getenv("RUNTIME_STATE_MAX_KEYS", "5000").strip() or "5000")

DEFAULT_LANGUAGE_GROUP = os.getenv("DEFAULT_LANGUAGE_GROUP", "vi").strip().lower() or "vi"
USER_LANGUAGE_MAP_JSON = os.getenv("USER_LANGUAGE_MAP_JSON", "").strip()

# =========================================================
# CONSTANTS
# =========================================================
APP_VERSION = "PHASE1_RUNTIME_STATE_SAFE__WORKER_ADS_PHONE_SUBMIT_FLOW__CALLBACK_VERIFY_V32"
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

WORKER_ENTRY_COMMAND = "/worker"
ADS_ENTRY_COMMAND = "/ads"
SUPPORTED_LANGUAGE_GROUPS = {"vi", "id", "th"}

ADS_CATALOG_V2_SHEET_NAME = "ADS_CATALOG_V2"
OWNER_ADS_INPUT_SHEET_NAME = "OWNER_ADS_INPUT"
OWNER_SETTINGS_SHEET_NAME = "OWNER_SETTINGS"
ADS_LEADS_SHEET_NAME = "ADS_LEADS"
ADS_CLICK_LOG_SHEET_NAME = "ads_click_log"
ADS_PHONE_LEADS_SHEET_NAME = "ads_phone_leads"
TENANT_REGISTRY_SHEET_NAME = "TENANT_REGISTRY"
SYSTEM_META_SHEET_NAME = "SYSTEM_META"

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

# =========================================================
# BASIC HELPERS
# =========================================================
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


# =========================================================
# GSHEET CLIENT
# =========================================================
_GSPREAD_CLIENT = None

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


# =========================================================
# SHEET HEADER / ROW HELPERS
# =========================================================
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
    client = get_gspread_client(trace_id)
    if not client:
        return None

    try:
        spreadsheet = client.open(PHASE1_SPREADSHEET_NAME)
        ws = spreadsheet.worksheet(worksheet_name)
        logger.info(f"[{trace_id}] WORKSHEET_READY worksheet_name={worksheet_name}")
        return ws
    except gspread.WorksheetNotFound:
        logger.error(f"[{trace_id}] WORKSHEET_NOT_FOUND worksheet_name={worksheet_name}")
        return None
    except Exception as e:
        logger.exception(
            f"[{trace_id}] WORKSHEET_OPEN_FAILED worksheet_name={worksheet_name} exception={type(e).__name__}:{e}"
        )
        return None


def get_all_values_safe(ws, trace_id: str, worksheet_name: str) -> List[List[str]]:
    try:
        values = ws.get_all_values()
        logger.info(f"[{trace_id}] WORKSHEET_READ_OK worksheet_name={worksheet_name} row_count={len(values)}")
        return values
    except Exception as e:
        logger.exception(
            f"[{trace_id}] WORKSHEET_READ_FAILED worksheet_name={worksheet_name} exception={type(e).__name__}:{e}"
        )
        return []


def get_records_safe(ws, trace_id: str, worksheet_name: str) -> List[dict]:
    try:
        records = ws.get_all_records()
        logger.info(f"[{trace_id}] WORKSHEET_RECORDS_OK worksheet_name={worksheet_name} count={len(records)}")
        return records
    except Exception as e:
        logger.exception(
            f"[{trace_id}] WORKSHEET_RECORDS_FAILED worksheet_name={worksheet_name} exception={type(e).__name__}:{e}"
        )
        return []


def find_first_row_index_by_column_value(
    ws,
    column_name: str,
    expected_value: str,
    trace_id: str,
    worksheet_name: str,
) -> int:
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
            logger.info(
                f"[{trace_id}] FIND_ROW_OK worksheet_name={worksheet_name} "
                f"column_name={column_name} expected_value={expected_value} row_index={row_idx}"
            )
            return row_idx

    logger.info(
        f"[{trace_id}] FIND_ROW_NOT_FOUND worksheet_name={worksheet_name} "
        f"column_name={column_name} expected_value={expected_value}"
    )
    return 0


def update_cell_by_header(
    ws,
    row_index: int,
    column_name: str,
    value: str,
    trace_id: str,
    worksheet_name: str,
) -> bool:
    values = get_all_values_safe(ws, trace_id, worksheet_name)
    if not values:
        return False

    headers = values[0]
    header_map = build_header_index_map(headers)
    col_idx = header_map.get(normalize_header_key(column_name))
    if col_idx is None:
        logger.error(f"[{trace_id}] UPDATE_CELL_COLUMN_MISSING worksheet_name={worksheet_name} column_name={column_name}")
        return False

    try:
        ws.update_cell(row_index, col_idx + 1, safe_str(value))
        logger.info(
            f"[{trace_id}] UPDATE_CELL_OK worksheet_name={worksheet_name} "
            f"row_index={row_index} column_name={column_name} value={json.dumps(safe_str(value), ensure_ascii=False)}"
        )
        return True
    except Exception as e:
        logger.exception(
            f"[{trace_id}] UPDATE_CELL_FAILED worksheet_name={worksheet_name} "
            f"row_index={row_index} column_name={column_name} exception={type(e).__name__}:{e}"
        )
        return False


# =========================================================
# RUNTIME CACHES
# =========================================================
_ADS_CATALOG_CACHE = {"rows": [], "loaded_at_ts": 0, "last_read_ok": False}
_ADS_VIEW_CACHE: Dict[str, dict] = {}
_ADS_DETAIL_CACHE: Dict[str, dict] = {}
_WORKSPACE_VALIDATION_CACHE = {"result": None, "loaded_at_ts": 0}


def reset_ads_runtime_caches(trace_id: str) -> None:
    _ADS_CATALOG_CACHE["loaded_at_ts"] = 0
    _ADS_CATALOG_CACHE["rows"] = []
    _ADS_CATALOG_CACHE["last_read_ok"] = False
    _ADS_VIEW_CACHE.clear()
    _ADS_DETAIL_CACHE.clear()
    logger.info(f"[{trace_id}] ADS_RUNTIME_CACHES_RESET")


# =========================================================
# ADS CATALOG HELPERS
# =========================================================
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

    if (
        int(_ADS_CATALOG_CACHE["loaded_at_ts"] or 0) > 0
        and now_ts - int(_ADS_CATALOG_CACHE["loaded_at_ts"] or 0) < ADS_CACHE_TTL_SECONDS
    ):
        logger.info(
            f"[{trace_id}] ADS_CACHE_HIT rows={len(_ADS_CATALOG_CACHE['rows'])} "
            f"last_read_ok={_ADS_CATALOG_CACHE['last_read_ok']}"
        )
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

        if not row["ad_id"]:
            continue
        if row["ad_type"] not in SUPPORTED_AD_TYPES:
            continue
        if row["status"] not in ACTIVE_AD_STATUSES:
            continue
        if not row["title_source"]:
            continue
        if not is_ad_active_in_time_window(row["start_at"], row["end_at"]):
            skipped_inactive_time += 1
            continue

        rows.append(row)

    rows.sort(
        key=lambda item: (
            -item["priority"],
            parse_sortable_time(item["start_at"]),
            parse_sortable_time(item["created_at"]),
            item["ad_id"],
        )
    )

    _ADS_CATALOG_CACHE["rows"] = rows
    _ADS_CATALOG_CACHE["loaded_at_ts"] = now_ts
    _ADS_CATALOG_CACHE["last_read_ok"] = True
    logger.info(f"[{trace_id}] ADS_CACHE_REFRESH_OK rows={len(rows)} skipped_inactive_time={skipped_inactive_time}")
    return rows, True


# =========================================================
# TENANT WORKSPACE VALIDATION HELPERS
# =========================================================
REQUIRED_SYSTEM_TABS_V1 = [
    "READ_ME",
    "OWNER_ADS_INPUT",
    "OWNER_LEAD_VIEW",
    "OWNER_SETTINGS",
    "ADS_CATALOG_V2",
    "ADS_LEADS",
    "ads_click_log",
    "SYSTEM_META",
]

WV_OK = "WV_OK"
WV_MISSING_REQUIRED_TAB = "WV_MISSING_REQUIRED_TAB"
WV_EMPTY_TENANT_ID = "WV_EMPTY_TENANT_ID"
WV_EMPTY_OWNER_ID = "WV_EMPTY_OWNER_ID"
WV_EMPTY_WORKSPACE_VERSION = "WV_EMPTY_WORKSPACE_VERSION"
WV_TENANT_REGISTRY_NOT_FOUND = "WV_TENANT_REGISTRY_NOT_FOUND"
WV_TENANT_REGISTRY_DUPLICATED = "WV_TENANT_REGISTRY_DUPLICATED"
WV_OWNER_ID_MISMATCH = "WV_OWNER_ID_MISMATCH"
WV_WORKSPACE_NAME_MISMATCH = "WV_WORKSPACE_NAME_MISMATCH"
WV_TENANT_INACTIVE = "WV_TENANT_INACTIVE"


def make_workspace_validation_result() -> dict:
    return {
        "workspace_status": "invalid",
        "error_code": "",
        "error_detail": "",
        "tenant_id": "",
        "owner_id": "",
        "workspace_version": "",
        "checked_tabs": [],
        "missing_tabs": [],
    }


def open_tenant_registry_worksheet(trace_id: str):
    client = get_gspread_client(trace_id)
    if not client:
        return None

    try:
        spreadsheet = client.open(PHASE1_SPREADSHEET_NAME)
        worksheet = spreadsheet.worksheet(TENANT_REGISTRY_SHEET_NAME)
        logger.info(f"[{trace_id}] TENANT_REGISTRY_SHEET_READY")
        return worksheet
    except gspread.WorksheetNotFound:
        logger.error(f"[{trace_id}] TENANT_REGISTRY_SHEET_NOT_FOUND worksheet_name={TENANT_REGISTRY_SHEET_NAME}")
        return None
    except Exception as e:
        logger.exception(f"[{trace_id}] TENANT_REGISTRY_SHEET_OPEN_FAILED exception={type(e).__name__}:{e}")
        return None


def open_system_meta_worksheet(trace_id: str):
    client = get_gspread_client(trace_id)
    if not client:
        return None

    try:
        spreadsheet = client.open(PHASE1_SPREADSHEET_NAME)
        worksheet = spreadsheet.worksheet(SYSTEM_META_SHEET_NAME)
        logger.info(f"[{trace_id}] SYSTEM_META_SHEET_READY")
        return worksheet
    except gspread.WorksheetNotFound:
        logger.error(f"[{trace_id}] SYSTEM_META_SHEET_NOT_FOUND worksheet_name={SYSTEM_META_SHEET_NAME}")
        return None
    except Exception as e:
        logger.exception(f"[{trace_id}] SYSTEM_META_SHEET_OPEN_FAILED exception={type(e).__name__}:{e}")
        return None


def get_current_workspace_name(trace_id: str) -> str:
    workspace_name = safe_str(PHASE1_SPREADSHEET_NAME)
    logger.info(f"[{trace_id}] WORKSPACE_NAME_RESOLVED name={workspace_name}")
    return workspace_name


def get_all_workspace_tab_names(trace_id: str) -> List[str]:
    client = get_gspread_client(trace_id)
    if not client:
        return []

    try:
        spreadsheet = client.open(PHASE1_SPREADSHEET_NAME)
        tab_names = [safe_str(ws.title) for ws in spreadsheet.worksheets()]
        logger.info(
            f"[{trace_id}] WORKSPACE_TAB_LIST_OK count={len(tab_names)} tabs={json.dumps(tab_names, ensure_ascii=False)}"
        )
        return tab_names
    except Exception as e:
        logger.exception(f"[{trace_id}] WORKSPACE_TAB_LIST_FAILED exception={type(e).__name__}:{e}")
        return []


def read_system_meta_row(trace_id: str) -> dict:
    ws = open_system_meta_worksheet(trace_id)
    if not ws:
        return {}

    try:
        records = ws.get_all_records()
        if not records:
            logger.error(f"[{trace_id}] SYSTEM_META_ROW2_EMPTY")
            return {}

        row = records[0]
        logger.info(
            f"[{trace_id}] SYSTEM_META_ROW2_OK tenant_id={safe_str(row.get('tenant_id'))} "
            f"owner_id={safe_str(row.get('owner_id'))} "
            f"workspace_version={safe_str(row.get('workspace_version'))}"
        )
        return row
    except Exception as e:
        logger.exception(f"[{trace_id}] SYSTEM_META_READ_FAILED exception={type(e).__name__}:{e}")
        return {}


def read_tenant_registry_rows(trace_id: str) -> List[dict]:
    ws = open_tenant_registry_worksheet(trace_id)
    if not ws:
        return []

    try:
        rows = ws.get_all_records()
        logger.info(f"[{trace_id}] TENANT_REGISTRY_READ_OK count={len(rows)}")
        return rows
    except Exception as e:
        logger.exception(f"[{trace_id}] TENANT_REGISTRY_READ_FAILED exception={type(e).__name__}:{e}")
        return []


def validate_tenant_workspace(
    spreadsheet_name: str,
    all_tab_names: List[str],
    system_meta_row: dict,
    tenant_registry_rows: List[dict],
) -> dict:
    result = make_workspace_validation_result()
    result["checked_tabs"] = list(all_tab_names or [])

    current_tabs_map = {
        safe_str(x).lower(): safe_str(x)
        for x in (all_tab_names or [])
        if safe_str(x)
    }
    missing_tabs = [
        tab for tab in REQUIRED_SYSTEM_TABS_V1
        if safe_str(tab).lower() not in current_tabs_map
    ]
    if missing_tabs:
        result["workspace_status"] = "invalid"
        result["error_code"] = WV_MISSING_REQUIRED_TAB
        result["error_detail"] = "Missing required system tab(s)"
        result["missing_tabs"] = missing_tabs
        return result

    tenant_id = safe_str(system_meta_row.get("tenant_id"))
    owner_id = safe_str(system_meta_row.get("owner_id"))
    workspace_version = safe_str(system_meta_row.get("workspace_version"))

    result["tenant_id"] = tenant_id
    result["owner_id"] = owner_id
    result["workspace_version"] = workspace_version

    if not tenant_id:
        result["error_code"] = WV_EMPTY_TENANT_ID
        result["error_detail"] = "SYSTEM_META.tenant_id is empty"
        return result

    if not owner_id:
        result["error_code"] = WV_EMPTY_OWNER_ID
        result["error_detail"] = "SYSTEM_META.owner_id is empty"
        return result

    if not workspace_version:
        result["error_code"] = WV_EMPTY_WORKSPACE_VERSION
        result["error_detail"] = "SYSTEM_META.workspace_version is empty"
        return result

    matched_rows = [
        row for row in (tenant_registry_rows or [])
        if safe_str(row.get("tenant_id")) == tenant_id
    ]

    if not matched_rows:
        result["error_code"] = WV_TENANT_REGISTRY_NOT_FOUND
        result["error_detail"] = "tenant_id not found in TENANT_REGISTRY"
        return result

    if len(matched_rows) > 1:
        result["error_code"] = WV_TENANT_REGISTRY_DUPLICATED
        result["error_detail"] = "duplicated tenant_id in TENANT_REGISTRY"
        return result

    tenant_row = matched_rows[0]

    if safe_str(tenant_row.get("owner_id")) != owner_id:
        result["error_code"] = WV_OWNER_ID_MISMATCH
        result["error_detail"] = "owner_id mismatch between SYSTEM_META and TENANT_REGISTRY"
        return result

    if safe_str(tenant_row.get("workspace_sheet_name")) != safe_str(spreadsheet_name):
        result["error_code"] = WV_WORKSPACE_NAME_MISMATCH
        result["error_detail"] = "workspace_sheet_name mismatch"
        return result

    if safe_str(tenant_row.get("tenant_status")).lower() != "active":
        result["error_code"] = WV_TENANT_INACTIVE
        result["error_detail"] = "tenant_status is not active"
        return result

    result["workspace_status"] = "valid"
    result["error_code"] = WV_OK
    result["error_detail"] = ""
    return result


def run_workspace_validation(trace_id: str) -> dict:
    spreadsheet_name = get_current_workspace_name(trace_id)
    all_tab_names = get_all_workspace_tab_names(trace_id)
    system_meta_row = read_system_meta_row(trace_id)
    tenant_registry_rows = read_tenant_registry_rows(trace_id)

    result = validate_tenant_workspace(
        spreadsheet_name=spreadsheet_name,
        all_tab_names=all_tab_names,
        system_meta_row=system_meta_row,
        tenant_registry_rows=tenant_registry_rows,
    )

    logger.info(
        "[%s] WORKSPACE_VALIDATION_RESULT workspace_status=%s error_code=%s tenant_id=%s owner_id=%s workspace_version=%s missing_tabs=%s",
        trace_id,
        safe_str(result.get("workspace_status")),
        safe_str(result.get("error_code")),
        safe_str(result.get("tenant_id")),
        safe_str(result.get("owner_id")),
        safe_str(result.get("workspace_version")),
        json.dumps(result.get("missing_tabs", []), ensure_ascii=False),
    )
    return result


def run_workspace_validation_cached(trace_id: str) -> dict:
    now_ts = get_now_ts()
    cached = _WORKSPACE_VALIDATION_CACHE.get("result")
    loaded_at_ts = int(_WORKSPACE_VALIDATION_CACHE.get("loaded_at_ts", 0) or 0)

    if cached and loaded_at_ts > 0 and (now_ts - loaded_at_ts) < WORKSPACE_VALIDATION_CACHE_TTL_SECONDS:
        logger.info(
            f"[{trace_id}] WORKSPACE_VALIDATION_CACHE_HIT "
            f"workspace_status={safe_str(cached.get('workspace_status'))} "
            f"error_code={safe_str(cached.get('error_code'))}"
        )
        return cached

    result = run_workspace_validation(trace_id)
    _WORKSPACE_VALIDATION_CACHE["result"] = result
    _WORKSPACE_VALIDATION_CACHE["loaded_at_ts"] = now_ts
    logger.info(
        f"[{trace_id}] WORKSPACE_VALIDATION_CACHE_SET "
        f"workspace_status={safe_str(result.get('workspace_status'))} "
        f"error_code={safe_str(result.get('error_code'))}"
    )
    return result


# =========================================================
# WORKSPACE VALIDATION WRAPPER
# =========================================================
def get_workspace_validation_result(trace_id: str) -> dict:
    validation_fn = globals().get("run_workspace_validation_cached")

    if callable(validation_fn):
        try:
            result = validation_fn(trace_id)
            if isinstance(result, dict):
                return result
            logger.error(
                f"[{trace_id}] WORKSPACE_VALIDATION_INVALID_RESULT_TYPE "
                f"type={type(result).__name__}"
            )
            return {
                "workspace_status": "invalid",
                "error_code": "WV_INVALID_RESULT_TYPE",
            }
        except Exception as e:
            logger.exception(
                f"[{trace_id}] WORKSPACE_VALIDATION_CALL_FAILED "
                f"exception={type(e).__name__}:{e}"
            )
            return {
                "workspace_status": "invalid",
                "error_code": "WV_CALL_FAILED",
            }

    logger.error(f"[{trace_id}] WORKSPACE_VALIDATION_FN_MISSING")
    return {
        "workspace_status": "invalid",
        "error_code": "WV_VALIDATION_FN_MISSING",
    }


# =========================================================
# PUBLISH SYNC
# =========================================================
PUBLISH_REQUEST_YES_VALUES = {"yes", "y", "true", "1", "on"}
PUBLISHABLE_INPUT_STATUSES = {"draft"}
PUBLISHED_INPUT_STATUS = "published"
FAILED_INPUT_STATUS = "publish_error"
PUBLISHED_CATALOG_STATUS = "active"
SYNC_STATUS_PUBLISHED = "published"
SYNC_STATUS_ALREADY_EXISTS = "already_exists"
SYNC_STATUS_PARTIAL_SUCCESS = "partial_success"
SYNC_STATUS_SKIPPED = "skipped"
SYNC_STATUS_FAILED = "failed"
DEFAULT_AUTHOR_LANGUAGE_GROUP = "vi"
DEFAULT_VISIBILITY_POLICY = VISIBILITY_SAME_LANGUAGE_ONLY
DEFAULT_PUBLISH_PRIORITY = "10"


def open_owner_ads_input_worksheet(trace_id: str):
    return get_worksheet_by_name(trace_id, OWNER_ADS_INPUT_SHEET_NAME)


def open_owner_settings_worksheet(trace_id: str):
    return get_worksheet_by_name(trace_id, OWNER_SETTINGS_SHEET_NAME)


def open_ads_catalog_v2_worksheet(trace_id: str):
    return get_worksheet_by_name(trace_id, ADS_CATALOG_V2_SHEET_NAME)


def read_owner_ads_input_rows(trace_id: str) -> List[dict]:
    ws = open_owner_ads_input_worksheet(trace_id)
    if not ws:
        return []
    return get_records_safe(ws, trace_id, OWNER_ADS_INPUT_SHEET_NAME)


def read_owner_settings_rows(trace_id: str) -> List[dict]:
    ws = open_owner_settings_worksheet(trace_id)
    if not ws:
        return []
    return get_records_safe(ws, trace_id, OWNER_SETTINGS_SHEET_NAME)


def read_ads_catalog_v2_rows(trace_id: str) -> List[dict]:
    ws = open_ads_catalog_v2_worksheet(trace_id)
    if not ws:
        return []
    return get_records_safe(ws, trace_id, ADS_CATALOG_V2_SHEET_NAME)


def normalize_yes_no(value: str) -> str:
    return "yes" if safe_str(value).lower() in PUBLISH_REQUEST_YES_VALUES else "no"


def normalize_publish_input_status(value: str) -> str:
    return safe_str(value).lower()


def make_catalog_ad_id(source_draft_id: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9]+", "_", safe_str(source_draft_id)).strip("_").lower()
    return f"ad_v2_{slug or uuid.uuid4().hex[:8]}"


def find_owner_settings_by_owner_id(owner_settings_rows: List[dict], owner_id: str) -> dict:
    target = safe_str(owner_id)
    for row in owner_settings_rows:
        if safe_str(row.get("owner_id")) == target:
            return row
    return {}


def find_catalog_row_by_source_draft_id(catalog_rows: List[dict], source_draft_id: str) -> dict:
    target = safe_str(source_draft_id)
    for row in catalog_rows:
        if safe_str(row.get("source_draft_id")) == target:
            return row
    return {}


def get_current_workspace_meta(trace_id: str) -> dict:
    system_meta_row = read_system_meta_row(trace_id)
    if not system_meta_row:
        return {}

    return {
        "tenant_id": safe_str(system_meta_row.get("tenant_id")),
        "owner_id": safe_str(system_meta_row.get("owner_id")),
        "workspace_version": safe_str(system_meta_row.get("workspace_version")),
    }


def is_publishable_owner_ads_input_row(row: dict) -> Tuple[bool, str]:
    draft_id = safe_str(row.get("draft_id"))
    owner_id = safe_str(row.get("owner_id"))
    category_code = safe_str(row.get("category_code"))
    title = safe_str(row.get("title"))
    body_text = safe_str(row.get("body_text"))
    contact_mode = safe_str(row.get("contact_mode"))
    publish_request = normalize_yes_no(row.get("publish_request"))
    input_status = normalize_publish_input_status(row.get("input_status"))

    if not draft_id:
        return False, "missing_draft_id"
    if not owner_id:
        return False, "missing_owner_id"
    if not category_code:
        return False, "missing_category_code"
    if not title:
        return False, "missing_title"
    if not body_text:
        return False, "missing_body_text"
    if not contact_mode:
        return False, "missing_contact_mode"
    if publish_request != "yes":
        return False, "publish_request_not_yes"
    if input_status not in PUBLISHABLE_INPUT_STATUSES:
        return False, f"input_status_not_publishable:{input_status}"

    return True, "ok"


def build_ads_catalog_v2_row(owner_ads_input_row: dict, owner_settings_row: dict, workspace_meta: dict) -> List[str]:
    draft_id = safe_str(owner_ads_input_row.get("draft_id"))
    owner_id = safe_str(owner_ads_input_row.get("owner_id"))
    category_code = safe_str(owner_ads_input_row.get("category_code"))
    title = safe_str(owner_ads_input_row.get("title"))
    body_text = safe_str(owner_ads_input_row.get("body_text"))
    contact_mode = safe_str(owner_ads_input_row.get("contact_mode"))

    owner_contact_name = safe_str(owner_settings_row.get("display_name")) or "Unknown Owner"
    owner_line_id = safe_str(owner_settings_row.get("line_contact_id"))
    tenant_id = safe_str(workspace_meta.get("tenant_id"))

    now_iso = now_tw_iso()
    ad_id = make_catalog_ad_id(draft_id)
    ad_type = normalize_ad_type(category_code)

    return [
        ad_id,
        draft_id,
        tenant_id,
        owner_id,
        owner_contact_name,
        owner_line_id,
        category_code,
        ad_type,
        DEFAULT_AUTHOR_LANGUAGE_GROUP,
        DEFAULT_VISIBILITY_POLICY,
        contact_mode,
        title,
        body_text,
        PUBLISHED_CATALOG_STATUS,
        DEFAULT_PUBLISH_PRIORITY,
        now_iso,
        now_iso,
    ]


def append_ads_catalog_v2_row(row: List[str], trace_id: str) -> bool:
    ws = open_ads_catalog_v2_worksheet(trace_id)
    if not ws:
        logger.error(f"[{trace_id}] ADS_CATALOG_V2_APPEND_SKIPPED reason=worksheet_unavailable")
        return False

    try:
        ws.append_row(row, value_input_option="USER_ENTERED")
        logger.info(
            f"[{trace_id}] ADS_CATALOG_V2_APPEND_OK "
            f"ad_id={safe_str(row[0])} source_draft_id={safe_str(row[1])}"
        )
        reset_ads_runtime_caches(trace_id)
        return True
    except Exception as e:
        logger.exception(f"[{trace_id}] ADS_CATALOG_V2_APPEND_FAILED exception={type(e).__name__}:{e}")
        return False


def update_owner_ads_input_status(
    draft_id: str,
    new_status: str,
    trace_id: str,
    updated_at: Optional[str] = None,
) -> bool:
    ws = open_owner_ads_input_worksheet(trace_id)
    if not ws:
        logger.error(f"[{trace_id}] OWNER_ADS_INPUT_STATUS_UPDATE_SKIPPED reason=worksheet_unavailable")
        return False

    row_index = find_first_row_index_by_column_value(
        ws=ws,
        column_name="draft_id",
        expected_value=draft_id,
        trace_id=trace_id,
        worksheet_name=OWNER_ADS_INPUT_SHEET_NAME,
    )
    if not row_index:
        logger.error(f"[{trace_id}] OWNER_ADS_INPUT_STATUS_UPDATE_ROW_NOT_FOUND draft_id={draft_id}")
        return False

    ok_status = update_cell_by_header(
        ws=ws,
        row_index=row_index,
        column_name="input_status",
        value=new_status,
        trace_id=trace_id,
        worksheet_name=OWNER_ADS_INPUT_SHEET_NAME,
    )
    ok_updated_at = update_cell_by_header(
        ws=ws,
        row_index=row_index,
        column_name="updated_at",
        value=updated_at or now_tw_iso(),
        trace_id=trace_id,
        worksheet_name=OWNER_ADS_INPUT_SHEET_NAME,
    )
    return bool(ok_status and ok_updated_at)


def sync_single_owner_ads_input_to_catalog(
    owner_ads_input_row: dict,
    workspace_meta: dict,
    owner_settings_rows: List[dict],
    catalog_rows: List[dict],
    trace_id: str,
) -> dict:
    draft_id = safe_str(owner_ads_input_row.get("draft_id"))
    owner_id = safe_str(owner_ads_input_row.get("owner_id"))

    result = {
        "draft_id": draft_id,
        "status": SYNC_STATUS_FAILED,
        "reason": "",
        "ad_id": "",
    }

    existing_catalog_row = find_catalog_row_by_source_draft_id(catalog_rows, draft_id)
    if existing_catalog_row:
        existing_ad_id = safe_str(existing_catalog_row.get("ad_id"))

        update_ok = update_owner_ads_input_status(
            draft_id=draft_id,
            new_status=PUBLISHED_INPUT_STATUS,
            trace_id=trace_id,
        )
        if not update_ok:
            result["status"] = SYNC_STATUS_PARTIAL_SUCCESS
            result["reason"] = "idempotent_hit_source_status_update_failed"
            result["ad_id"] = existing_ad_id
            logger.error(
                f"[{trace_id}] PUBLISH_SYNC_IDEMPOTENT_PARTIAL "
                f"draft_id={draft_id} ad_id={existing_ad_id}"
            )
            return result

        result["status"] = SYNC_STATUS_ALREADY_EXISTS
        result["reason"] = "idempotent_hit"
        result["ad_id"] = existing_ad_id

        logger.info(
            f"[{trace_id}] PUBLISH_SYNC_IDEMPOTENT_HIT "
            f"draft_id={draft_id} ad_id={existing_ad_id}"
        )
        return result

    is_publishable, gate_reason = is_publishable_owner_ads_input_row(owner_ads_input_row)
    if not is_publishable:
        result["status"] = SYNC_STATUS_SKIPPED
        result["reason"] = gate_reason
        logger.info(
            f"[{trace_id}] PUBLISH_SYNC_GATE_BLOCKED "
            f"draft_id={draft_id} reason={gate_reason}"
        )
        return result

    owner_settings_row = find_owner_settings_by_owner_id(owner_settings_rows, owner_id)
    if not owner_settings_row:
        result["reason"] = "owner_settings_not_found"
        logger.error(
            f"[{trace_id}] PUBLISH_SYNC_OWNER_SETTINGS_NOT_FOUND "
            f"draft_id={draft_id} owner_id={owner_id}"
        )
        update_owner_ads_input_status(
            draft_id=draft_id,
            new_status=FAILED_INPUT_STATUS,
            trace_id=trace_id,
        )
        return result

    workspace_owner_id = safe_str(workspace_meta.get("owner_id"))
    tenant_id = safe_str(workspace_meta.get("tenant_id"))
    if not tenant_id:
        result["reason"] = "missing_workspace_tenant_id"
        logger.error(f"[{trace_id}] PUBLISH_SYNC_WORKSPACE_TENANT_ID_MISSING draft_id={draft_id}")
        update_owner_ads_input_status(
            draft_id=draft_id,
            new_status=FAILED_INPUT_STATUS,
            trace_id=trace_id,
        )
        return result

    if workspace_owner_id and workspace_owner_id != owner_id:
        result["reason"] = "workspace_owner_id_mismatch"
        logger.error(
            f"[{trace_id}] PUBLISH_SYNC_OWNER_ID_MISMATCH "
            f"draft_id={draft_id} workspace_owner_id={workspace_owner_id} owner_id={owner_id}"
        )
        update_owner_ads_input_status(
            draft_id=draft_id,
            new_status=FAILED_INPUT_STATUS,
            trace_id=trace_id,
        )
        return result

    catalog_append_row = build_ads_catalog_v2_row(
        owner_ads_input_row=owner_ads_input_row,
        owner_settings_row=owner_settings_row,
        workspace_meta=workspace_meta,
    )

    append_ok = append_ads_catalog_v2_row(catalog_append_row, trace_id)
    if not append_ok:
        result["reason"] = "catalog_append_failed"
        update_owner_ads_input_status(
            draft_id=draft_id,
            new_status=FAILED_INPUT_STATUS,
            trace_id=trace_id,
        )
        return result

    update_ok = update_owner_ads_input_status(
        draft_id=draft_id,
        new_status=PUBLISHED_INPUT_STATUS,
        trace_id=trace_id,
    )
    if not update_ok:
        result["reason"] = "source_status_update_failed"
        result["status"] = SYNC_STATUS_PARTIAL_SUCCESS
        result["ad_id"] = safe_str(catalog_append_row[0])
        logger.error(
            f"[{trace_id}] PUBLISH_SYNC_PARTIAL_SUCCESS "
            f"draft_id={draft_id} ad_id={safe_str(catalog_append_row[0])}"
        )
        return result

    result["status"] = SYNC_STATUS_PUBLISHED
    result["reason"] = "ok"
    result["ad_id"] = safe_str(catalog_append_row[0])

    logger.info(
        f"[{trace_id}] PUBLISH_SYNC_OK "
        f"draft_id={draft_id} ad_id={safe_str(catalog_append_row[0])} "
        f"tenant_id={tenant_id} owner_id={owner_id}"
    )
    return result


def run_publish_sync_once(trace_id: str) -> dict:
    result = {
        "ok": False,
        "workspace_status": "",
        "processed": 0,
        "published": 0,
        "already_exists": 0,
        "partial_success": 0,
        "skipped": 0,
        "failed": 0,
        "items": [],
    }

    workspace_validation = get_workspace_validation_result(trace_id)
    result["workspace_status"] = safe_str(workspace_validation.get("workspace_status"))

    if workspace_validation.get("workspace_status") != "valid":
        logger.error(
            f"[{trace_id}] PUBLISH_SYNC_ABORTED reason=workspace_invalid "
            f"error_code={safe_str(workspace_validation.get('error_code'))}"
        )
        return result

    workspace_meta = get_current_workspace_meta(trace_id)
    owner_ads_input_rows = read_owner_ads_input_rows(trace_id)
    owner_settings_rows = read_owner_settings_rows(trace_id)
    catalog_rows = read_ads_catalog_v2_rows(trace_id)

    if not owner_ads_input_rows:
        logger.info(f"[{trace_id}] PUBLISH_SYNC_NO_SOURCE_ROWS")
        result["ok"] = True
        return result

    for row in owner_ads_input_rows:
        sync_result = sync_single_owner_ads_input_to_catalog(
            owner_ads_input_row=row,
            workspace_meta=workspace_meta,
            owner_settings_rows=owner_settings_rows,
            catalog_rows=catalog_rows,
            trace_id=trace_id,
        )

        result["processed"] += 1
        result["items"].append(sync_result)

        status = safe_str(sync_result.get("status"))
        if status == SYNC_STATUS_PUBLISHED:
            result["published"] += 1
            catalog_rows.append({
                "ad_id": sync_result.get("ad_id", ""),
                "source_draft_id": sync_result.get("draft_id", ""),
            })
        elif status == SYNC_STATUS_ALREADY_EXISTS:
            result["already_exists"] += 1
        elif status == SYNC_STATUS_PARTIAL_SUCCESS:
            result["partial_success"] += 1
        elif status == SYNC_STATUS_SKIPPED:
            result["skipped"] += 1
        else:
            result["failed"] += 1

    result["ok"] = (result["failed"] == 0 and result["partial_success"] == 0)
    logger.info(
        f"[{trace_id}] PUBLISH_SYNC_SUMMARY processed={result['processed']} "
        f"published={result['published']} already_exists={result['already_exists']} "
        f"partial_success={result['partial_success']} skipped={result['skipped']} failed={result['failed']} "
        f"ok={result['ok']}"
    )
    return result


def resolve_publish_sync_status_code(sync_result: dict) -> int:
    workspace_status = safe_str(sync_result.get("workspace_status"))
    failed = int(sync_result.get("failed", 0) or 0)
    partial_success = int(sync_result.get("partial_success", 0) or 0)
    ok = bool(sync_result.get("ok"))

    if ok:
        return 200
    if workspace_status != "valid":
        return 409
    if failed > 0 or partial_success > 0:
        return 500
    return 409


# =========================================================
# INTERNAL ROUTE
# =========================================================
@app.route("/internal/publish-sync", methods=["POST"])
def internal_publish_sync():
    trace_id = make_trace_id()
    started = time.perf_counter()

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


# =========================================================
# LINE WEBHOOK / CALLBACK
# =========================================================
def verify_line_signature(raw_body: bytes, signature: str, trace_id: str) -> bool:
    secret = safe_str(LINE_CHANNEL_SECRET)
    if not secret:
        logger.error(f"[{trace_id}] LINE_SIGNATURE_SECRET_MISSING")
        return False

    if not signature:
        logger.error(f"[{trace_id}] LINE_SIGNATURE_HEADER_MISSING")
        return False

    try:
        digest = hmac.new(
            secret.encode("utf-8"),
            raw_body,
            hashlib.sha256
        ).digest()
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
    return safe_str(message.get("text"))


def get_reply_token(event: dict) -> str:
    return safe_str(event.get("replyToken"))


def reply_line_text(reply_token: str, text: str, trace_id: str) -> bool:
    if not LINE_CHANNEL_ACCESS_TOKEN:
        logger.error(f"[{trace_id}] LINE_REPLY_TOKEN_MISSING_ACCESS_TOKEN")
        return False

    if not reply_token:
        logger.error(f"[{trace_id}] LINE_REPLY_TOKEN_MISSING_REPLY_TOKEN")
        return False

    text = safe_str(text)[:LINE_TEXT_HARD_LIMIT] or FALLBACK_REPLY_TEXT

    headers = {
        "Authorization": f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}",
        "Content-Type": "application/json",
    }
    payload = {
        "replyToken": reply_token,
        "messages": [
            {
                "type": "text",
                "text": text,
            }
        ],
    }

    try:
        resp = requests.post(
            LINE_REPLY_API_URL,
            headers=headers,
            json=payload,
            timeout=OUTBOUND_TIMEOUT,
        )
        body_preview = safe_str(resp.text)[:ERROR_BODY_LOG_LIMIT]
        logger.info(
            f"[{trace_id}] LINE_REPLY_HTTP status_code={resp.status_code} body={json.dumps(body_preview, ensure_ascii=False)}"
        )
        return 200 <= resp.status_code < 300
    except Exception as e:
        logger.exception(f"[{trace_id}] LINE_REPLY_EXCEPTION exception={type(e).__name__}:{e}")
        return False


def handle_worker_command(text: str) -> str:
    return "Đã vào worker flow. Gửi nội dung tiếp theo."


def handle_ads_command(text: str) -> str:
    return "Đã vào ads flow. Gửi nội dung tiếp theo."


def dispatch_text_event(event: dict, trace_id: str) -> dict:
    user_id = get_event_user_id(event)
    reply_token = get_reply_token(event)
    text = get_message_text(event)
    normalized = text.strip().lower()

    logger.info(
        f"[{trace_id}] LINE_TEXT_DISPATCH user_id={user_id} reply_token_present={bool(reply_token)} text={json.dumps(text, ensure_ascii=False)}"
    )

    if normalized == WORKER_ENTRY_COMMAND:
        reply_text = handle_worker_command(text)
        flow = "worker"
    elif normalized == ADS_ENTRY_COMMAND:
        reply_text = handle_ads_command(text)
        flow = "ads"
    else:
        reply_text = f"Đã nhận: {text}"
        flow = "default"

    reply_ok = reply_line_text(reply_token, reply_text, trace_id)

    return {
        "handled": True,
        "event_type": "message",
        "message_type": "text",
        "flow": flow,
        "user_id": user_id,
        "reply_sent": reply_ok,
    }


def dispatch_line_event(event: dict, trace_id: str) -> dict:
    event_type = get_event_type(event)
    logger.info(f"[{trace_id}] LINE_EVENT_DISPATCH event_type={event_type}")

    if event_type != "message":
        return {
            "handled": False,
            "event_type": event_type,
            "reason": "unsupported_event_type",
        }

    message_type = get_message_type(event)
    if message_type != "text":
        return {
            "handled": False,
            "event_type": event_type,
            "message_type": message_type,
            "reason": "unsupported_message_type",
        }

    return dispatch_text_event(event, trace_id)


@app.route("/callback", methods=["POST"])
def callback():
    trace_id = make_trace_id()
    started = time.perf_counter()

    raw_body = request.get_data() or b""
    signature = safe_str(request.headers.get("X-Line-Signature"))

    logger.info(
        f"[{trace_id}] CALLBACK_IN content_length={len(raw_body)} signature_present={bool(signature)}"
    )

    if not verify_line_signature(raw_body, signature, trace_id):
        logger.error(f"[{trace_id}] CALLBACK_REJECT_INVALID_SIGNATURE")
        return jsonify({
            "ok": False,
            "trace_id": trace_id,
            "error": "invalid_signature",
        }), 403

    payload = parse_line_webhook_payload(raw_body, trace_id)
    events = payload.get("events")

    if not isinstance(events, list):
        logger.error(f"[{trace_id}] CALLBACK_INVALID_EVENTS_TYPE")
        return jsonify({
            "ok": False,
            "trace_id": trace_id,
            "error": "invalid_events_type",
        }), 400

    if len(events) == 0:
        latency_ms = ms_since(started)
        logger.info(f"[{trace_id}] CALLBACK_VERIFY_EMPTY_EVENTS_OK latency_ms={latency_ms}")
        return jsonify({
            "ok": True,
            "app_version": APP_VERSION,
            "trace_id": trace_id,
            "latency_ms": latency_ms,
            "event_count": 0,
            "results": [],
            "reason": "empty_events_verify_ok",
        }), 200

    results = []
    for event in events:
        try:
            result = dispatch_line_event(event, trace_id)
            results.append(result)
        except Exception as e:
            logger.exception(f"[{trace_id}] CALLBACK_EVENT_EXCEPTION exception={type(e).__name__}:{e}")
            results.append({
                "handled": False,
                "reason": "event_exception",
            })

    latency_ms = ms_since(started)
    logger.info(f"[{trace_id}] CALLBACK_DONE events={len(events)} latency_ms={latency_ms}")

    return jsonify({
        "ok": True,
        "app_version": APP_VERSION,
        "trace_id": trace_id,
        "latency_ms": latency_ms,
        "event_count": len(events),
        "results": results,
    }), 200
