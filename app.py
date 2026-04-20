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
from flask import Flask, request, jsonify

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

APP_VERSION = "PHASE1_RUNTIME_STATE_SAFE__RESTART_SAFE_DEDUP_SHEET_V46"
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
    client = get_gspread_client(trace_id)
    if not client:
        return None
    try:
        spreadsheet = client.open(PHASE1_SPREADSHEET_NAME)
        logger.info(f"[{trace_id}] SPREADSHEET_READY name={PHASE1_SPREADSHEET_NAME}")
        return spreadsheet
    except Exception as e:
        logger.exception(f"[{trace_id}] SPREADSHEET_OPEN_FAILED exception={type(e).__name__}:{e}")
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
    spreadsheet = open_spreadsheet(trace_id)
    if not spreadsheet:
        return None
    try:
        ws = spreadsheet.worksheet(worksheet_name)
        logger.info(f"[{trace_id}] WORKSHEET_READY worksheet_name={worksheet_name}")
        return ws
    except gspread.WorksheetNotFound:
        logger.error(f"[{trace_id}] WORKSHEET_NOT_FOUND worksheet_name={worksheet_name}")
        return None
    except Exception as e:
        logger.exception(f"[{trace_id}] WORKSHEET_OPEN_FAILED worksheet_name={worksheet_name} exception={type(e).__name__}:{e}")
        return None

def get_all_values_safe(ws, trace_id: str, worksheet_name: str) -> List[List[str]]:
    try:
        values = ws.get_all_values()
        logger.info(f"[{trace_id}] WORKSHEET_READ_OK worksheet_name={worksheet_name} row_count={len(values)}")
        return values
    except Exception as e:
        logger.exception(f"[{trace_id}] WORKSHEET_READ_FAILED worksheet_name={worksheet_name} exception={type(e).__name__}:{e}")
        return []

def get_records_safe(ws, trace_id: str, worksheet_name: str) -> List[dict]:
    try:
        records = ws.get_all_records()
        logger.info(f"[{trace_id}] WORKSHEET_RECORDS_OK worksheet_name={worksheet_name} count={len(records)}")
        return records
    except Exception as e:
        logger.exception(f"[{trace_id}] WORKSHEET_RECORDS_FAILED worksheet_name={worksheet_name} exception={type(e).__name__}:{e}")
        return []

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
        logger.info(
            f"[{trace_id}] UPDATE_ROW_FIELDS_OK worksheet_name={worksheet_name} row_index={row_index} "
            f"columns={json.dumps([name for _, name, _ in normalized_items], ensure_ascii=False)} "
            f"values={json.dumps({name: value for _, name, value in normalized_items}, ensure_ascii=False)}"
        )
        return True
    except Exception as e:
        logger.exception(f"[{trace_id}] UPDATE_ROW_FIELDS_FAILED worksheet_name={worksheet_name} row_index={row_index} exception={type(e).__name__}:{e}")
        return False

_ADS_CATALOG_CACHE = {"rows": [], "loaded_at_ts": 0, "last_read_ok": False}
_ADS_VIEW_CACHE: Dict[str, dict] = {}
_ADS_DETAIL_CACHE: Dict[str, dict] = {}
_WORKSPACE_VALIDATION_CACHE = {"result": None, "loaded_at_ts": 0}
_USER_FLOW_STATE: Dict[str, dict] = {}

USER_STATE_HEADERS = ["user_id", "flow", "updated_at", "language_group"]
PROCESSED_EVENT_HEADERS = ["event_key", "processed_at", "trace_id", "webhook_event_id", "message_id", "reply_token", "user_id", "event_type"]

_USER_LANGUAGE_STATE: Dict[str, dict] = {}
_PROCESSED_EVENT_STATE: Dict[str, int] = {}

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
        "default_echo": "Đã nhận: {text}",
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
        "default_echo": "Diterima: {text}",
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
        "default_echo": "รับแล้ว: {text}",
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
        "default_echo": "已收到：{text}",
        "state_save_failed": "儲存狀態失敗，請稍後再試。",
        "state_clear_failed": "清除狀態失敗，請稍後再試。",
        "rich_menu_switch_failed": "語言已儲存，但切換選單失敗。",
    },
}

def t(language_group: str, key: str, **kwargs) -> str:
    lang = normalize_language_group(language_group)
    template = LOCALIZED_TEXT.get(lang, LOCALIZED_TEXT["vi"]).get(key, "")
    return template.format(**kwargs)

# --- processed event state ---

def prune_processed_event_state(trace_id: str) -> None:
    now_ts = get_now_ts()
    expired_keys = []
    for event_key, processed_at_ts in list(_PROCESSED_EVENT_STATE.items()):
        if int(processed_at_ts or 0) <= 0 or (now_ts - int(processed_at_ts or 0)) > PROCESSED_EVENT_TTL_SECONDS:
            expired_keys.append(event_key)
    for event_key in expired_keys:
        _PROCESSED_EVENT_STATE.pop(event_key, None)
    while len(_PROCESSED_EVENT_STATE) > PROCESSED_EVENT_MAX_KEYS:
        oldest_key = min(_PROCESSED_EVENT_STATE.keys(), key=lambda x: int(_PROCESSED_EVENT_STATE.get(x, 0) or 0))
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

def has_persistent_processed_event(event_key: str, trace_id: str) -> bool:
    if not event_key:
        logger.info(f"[{trace_id}] PROCESSED_EVENT_PERSIST_CHECK_SKIPPED reason=missing_event_key")
        return False
    ws = ensure_processed_event_worksheet(trace_id)
    if not ws:
        logger.error(f"[{trace_id}] PROCESSED_EVENT_PERSIST_CHECK_SKIPPED reason=worksheet_unavailable event_key={event_key}")
        return False
    row_index = find_first_row_index_by_column_value(
        ws=ws,
        column_name="event_key",
        expected_value=event_key,
        trace_id=trace_id,
        worksheet_name=PROCESSED_EVENT_SHEET_NAME,
    )
    duplicate = row_index > 0
    logger.info(f"[{trace_id}] PROCESSED_EVENT_PERSIST_CHECK event_key={event_key} duplicate={duplicate} row_index={row_index}")
    return duplicate

def append_persistent_processed_event(event: dict, event_key: str, trace_id: str) -> bool:
    if not event_key:
        logger.info(f"[{trace_id}] PROCESSED_EVENT_PERSIST_APPEND_SKIPPED reason=missing_event_key")
        return False
    ws = ensure_processed_event_worksheet(trace_id)
    if not ws:
        logger.error(f"[{trace_id}] PROCESSED_EVENT_PERSIST_APPEND_SKIPPED reason=worksheet_unavailable event_key={event_key}")
        return False
    if has_persistent_processed_event(event_key, trace_id):
        logger.info(f"[{trace_id}] PROCESSED_EVENT_PERSIST_ALREADY_EXISTS event_key={event_key}")
        return True

    message = event.get("message") or {}
    row = [
        event_key,
        now_tw_iso(),
        trace_id,
        safe_str(event.get("webhookEventId")),
        safe_str(message.get("id")),
        safe_str(event.get("replyToken")),
        get_event_user_id(event),
        get_event_type(event),
    ]
    try:
        ws.append_row(row, value_input_option="USER_ENTERED")
        logger.info(f"[{trace_id}] PROCESSED_EVENT_PERSIST_APPEND_OK event_key={event_key}")
        return True
    except Exception as e:
        logger.exception(f"[{trace_id}] PROCESSED_EVENT_PERSIST_APPEND_FAILED event_key={event_key} exception={type(e).__name__}:{e}")
        return False

def is_duplicate_event(event: dict, trace_id: str) -> bool:
    event_key = get_event_unique_key(event)
    if not event_key:
        logger.info(f"[{trace_id}] EVENT_DEDUP_SKIPPED reason=missing_event_key")
        return False
    prune_processed_event_state(trace_id)
    if event_key in _PROCESSED_EVENT_STATE:
        logger.info(f"[{trace_id}] EVENT_DEDUP_CHECK event_key={event_key} duplicate=True source=runtime")
        return True
    duplicate_persist = has_persistent_processed_event(event_key, trace_id)
    logger.info(f"[{trace_id}] EVENT_DEDUP_CHECK event_key={event_key} duplicate={duplicate_persist} source=persistent")
    return duplicate_persist

def mark_event_processed(event: dict, trace_id: str) -> None:
    event_key = get_event_unique_key(event)
    if not event_key:
        logger.info(f"[{trace_id}] EVENT_DEDUP_MARK_SKIPPED reason=missing_event_key")
        return
    prune_processed_event_state(trace_id)
    _PROCESSED_EVENT_STATE[event_key] = get_now_ts()
    persist_ok = append_persistent_processed_event(event, event_key, trace_id)
    logger.info(f"[{trace_id}] EVENT_DEDUP_MARKED event_key={event_key} persist_ok={persist_ok}")

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
        reply_text = t(current_language, "default_echo", text=text)
        flow_used = "ads_auto_cleared"
    else:
        reply_text = t(current_language, "default_echo", text=text)
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
        try:
            if is_duplicate_event(event, trace_id):
                duplicate_event_key = get_event_unique_key(event)
                logger.info(f"[{trace_id}] CALLBACK_EVENT_DUPLICATE_SKIP event_key={duplicate_event_key}")
                results.append({"handled": False, "reason": "duplicate_event", "event_key": duplicate_event_key})
                continue
            result = dispatch_line_event(event, trace_id)
            if bool(result.get("handled")):
                mark_event_processed(event, trace_id)
            results.append(result)
        except Exception as e:
            logger.exception(f"[{trace_id}] CALLBACK_EVENT_EXCEPTION exception={type(e).__name__}:{e}")
            results.append({"handled": False, "reason": "event_exception"})
    latency_ms = ms_since(started)
    logger.info(f"[{trace_id}] CALLBACK_DONE events={len(events)} latency_ms={latency_ms}")
    return jsonify({"ok": True, "app_version": APP_VERSION, "trace_id": trace_id, "latency_ms": latency_ms, "event_count": len(events), "results": results}), 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")))
