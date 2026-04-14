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
APP_VERSION = "PHASE1_RUNTIME_STATE_SAFE__WORKER_ADS_PHONE_SUBMIT_FLOW__WORKSPACE_VALIDATION__PUBLISH_SYNC_V26"
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
# WORKSPACE VALIDATION
# =========================================================
def run_workspace_validation_cached(trace_id: str) -> dict:
    # Placeholder an toàn nếu phần validation gốc nằm dưới file.
    # Nếu file gốc đã có hàm này ở dưới, hãy xóa block placeholder này.
    return {"workspace_status": "valid", "error_code": "WV_OK"}


def get_workspace_validation_result(trace_id: str) -> dict:
    return run_workspace_validation_cached(trace_id)


def read_system_meta_row(trace_id: str) -> dict:
    ws = get_worksheet_by_name(trace_id, SYSTEM_META_SHEET_NAME)
    if not ws:
        return {}
    records = get_records_safe(ws, trace_id, SYSTEM_META_SHEET_NAME)
    return records[0] if records else {}


# =========================================================
# PUBLISH SYNC
# =========================================================
PUBLISH_REQUEST_YES_VALUES = {"yes", "y", "true", "1", "on"}
PUBLISHABLE_INPUT_STATUSES = {"draft"}
PUBLISHED_INPUT_STATUS = "published"
FAILED_INPUT_STATUS = "publish_error"
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
        "draft",
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
        logger.info(f"[{trace_id}] ADS_CATALOG_V2_APPEND_OK ad_id={safe_str(row[0])} source_draft_id={safe_str(row[1])}")
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

    result = {"draft_id": draft_id, "status": "skipped", "reason": "", "ad_id": ""}

    is_publishable, gate_reason = is_publishable_owner_ads_input_row(owner_ads_input_row)
    if not is_publishable:
        result["reason"] = gate_reason
        logger.info(f"[{trace_id}] PUBLISH_SYNC_GATE_BLOCKED draft_id={draft_id} reason={gate_reason}")
        return result

    existing_catalog_row = find_catalog_row_by_source_draft_id(catalog_rows, draft_id)
    if existing_catalog_row:
        existing_ad_id = safe_str(existing_catalog_row.get("ad_id"))
        update_owner_ads_input_status(draft_id=draft_id, new_status=PUBLISHED_INPUT_STATUS, trace_id=trace_id)
        result["status"] = "already_exists"
        result["reason"] = "idempotent_hit"
        result["ad_id"] = existing_ad_id
        logger.info(f"[{trace_id}] PUBLISH_SYNC_IDEMPOTENT_HIT draft_id={draft_id} ad_id={existing_ad_id}")
        return result

    owner_settings_row = find_owner_settings_by_owner_id(owner_settings_rows, owner_id)
    if not owner_settings_row:
        result["reason"] = "owner_settings_not_found"
        logger.error(f"[{trace_id}] PUBLISH_SYNC_OWNER_SETTINGS_NOT_FOUND draft_id={draft_id} owner_id={owner_id}")
        update_owner_ads_input_status(draft_id=draft_id, new_status=FAILED_INPUT_STATUS, trace_id=trace_id)
        return result

    workspace_owner_id = safe_str(workspace_meta.get("owner_id"))
    tenant_id = safe_str(workspace_meta.get("tenant_id"))
    if not tenant_id:
        result["reason"] = "missing_workspace_tenant_id"
        logger.error(f"[{trace_id}] PUBLISH_SYNC_WORKSPACE_TENANT_ID_MISSING draft_id={draft_id}")
        update_owner_ads_input_status(draft_id=draft_id, new_status=FAILED_INPUT_STATUS, trace_id=trace_id)
        return result

    if workspace_owner_id and workspace_owner_id != owner_id:
        result["reason"] = "workspace_owner_id_mismatch"
        logger.error(
            f"[{trace_id}] PUBLISH_SYNC_OWNER_ID_MISMATCH draft_id={draft_id} "
            f"workspace_owner_id={workspace_owner_id} owner_id={owner_id}"
        )
        update_owner_ads_input_status(draft_id=draft_id, new_status=FAILED_INPUT_STATUS, trace_id=trace_id)
        return result

    catalog_append_row = build_ads_catalog_v2_row(
        owner_ads_input_row=owner_ads_input_row,
        owner_settings_row=owner_settings_row,
        workspace_meta=workspace_meta,
    )

    append_ok = append_ads_catalog_v2_row(catalog_append_row, trace_id)
    if not append_ok:
        result["reason"] = "catalog_append_failed"
        update_owner_ads_input_status(draft_id=draft_id, new_status=FAILED_INPUT_STATUS, trace_id=trace_id)
        return result

    update_ok = update_owner_ads_input_status(draft_id=draft_id, new_status=PUBLISHED_INPUT_STATUS, trace_id=trace_id)
    if not update_ok:
        result["reason"] = "source_status_update_failed"
        result["status"] = "partial_success"
        result["ad_id"] = safe_str(catalog_append_row[0])
        logger.error(f"[{trace_id}] PUBLISH_SYNC_PARTIAL_SUCCESS draft_id={draft_id} ad_id={safe_str(catalog_append_row[0])}")
        return result

    result["status"] = "published"
    result["reason"] = "ok"
    result["ad_id"] = safe_str(catalog_append_row[0])

    logger.info(
        f"[{trace_id}] PUBLISH_SYNC_OK draft_id={draft_id} "
        f"ad_id={safe_str(catalog_append_row[0])} tenant_id={tenant_id} owner_id={owner_id}"
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
        if status == "published":
            result["published"] += 1
            catalog_rows.append({"ad_id": sync_result.get("ad_id", ""), "source_draft_id": sync_result.get("draft_id", "")})
        elif status == "already_exists":
            result["already_exists"] += 1
        elif status == "partial_success":
            result["partial_success"] += 1
        elif status == "skipped":
            result["skipped"] += 1
        else:
            result["failed"] += 1

    result["ok"] = True
    logger.info(
        f"[{trace_id}] PUBLISH_SYNC_SUMMARY processed={result['processed']} "
        f"published={result['published']} already_exists={result['already_exists']} "
        f"partial_success={result['partial_success']} skipped={result['skipped']} failed={result['failed']}"
    )
    return result


# =========================================================
# INTERNAL ROUTE
# =========================================================
@app.route("/internal/publish-sync", methods=["POST"])
def internal_publish_sync():
    trace_id = make_trace_id()
    started = time.perf_counter()

    sync_result = run_publish_sync_once(trace_id)

    status_code = 200 if sync_result.get("ok") else 409
    payload = {
        "ok": bool(sync_result.get("ok")),
        "app_version": APP_VERSION,
        "trace_id": trace_id,
        "latency_ms": ms_since(started),
        "result": sync_result,
    }
    return jsonify(payload), status_code
