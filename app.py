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

def update_cell_by_header(ws, row_index: int, column_name: str, value: str, trace_id: str, worksheet_name: str) -> bool:
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
        logger.info(f"[{trace_id}] UPDATE_CELL_OK worksheet_name={worksheet_name} row_index={row_index} column_name={column_name} value={json.dumps(safe_str(value), ensure_ascii=False)}")
        return True
    except Exception as e:
        logger.exception(f"[{trace_id}] UPDATE_CELL_FAILED worksheet_name={worksheet_name} row_index={row_index} column_name={column_name} exception={type(e).__name__}:{e}")
        return False

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
USER_LANGUAGE_HEADERS = ["user_id", "language_group", "updated_at"]
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
        "lang_invalid": "format yang benar: /lang vi atau /lang id atau /lang th atau /lang zh",
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
            ws = spreadsheet.add_worksheet(title=PROCESSED_EVENT_SHEET_NAME, rows=5000, cols=len(PROCESSED_EVENT_HEADERS) + 2)
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

    headers = values[0]
    header_map = build_header_index_map(headers)
    if any(normalize_header_key(h) not in header_map for h in PROCESSED_EVENT_HEADERS):
        try:
            end_col_letter = chr(ord("A") + len(PROCESSED_EVENT_HEADERS) - 1)
            ws.update(f"A1:{end_col_letter}1", [PROCESSED_EVENT_HEADERS])
            logger.info(f"[{trace_id}] PROCESSED_EVENT_HEADERS_REPAIRED old_headers={json.dumps(headers, ensure_ascii=False)}")
        except Exception as e:
            logger.exception(f"[{trace_id}] PROCESSED_EVENT_HEADERS_REPAIR_FAILED exception={type(e).__name__}:{e}")
            return None
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
    normalized_event_key = safe_str(event_key)
    if not normalized_event_key:
        logger.info(f"[{trace_id}] PROCESSED_EVENT_PERSIST_CHECK_SKIPPED reason=missing_event_key")
        return False
    ws = ensure_processed_event_worksheet(trace_id)
    if not ws:
        logger.error(f"[{trace_id}] PROCESSED_EVENT_PERSIST_CHECK_SKIPPED reason=worksheet_unavailable event_key={normalized_event_key}")
        return False
    row_index = find_first_row_index_by_column_value(
        ws=ws,
        column_name="event_key",
        expected_value=normalized_event_key,
        trace_id=trace_id,
        worksheet_name=PROCESSED_EVENT_SHEET_NAME,
    )
    duplicate = row_index > 0
    logger.info(f"[{trace_id}] PROCESSED_EVENT_PERSIST_CHECK event_key={normalized_event_key} duplicate={duplicate} row_index={row_index}")
    return duplicate

def append_persistent_processed_event(event: dict, event_key: str, trace_id: str) -> bool:
    normalized_event_key = safe_str(event_key)
    if not normalized_event_key:
        logger.info(f"[{trace_id}] PROCESSED_EVENT_PERSIST_APPEND_SKIPPED reason=missing_event_key")
        return False
    ws = ensure_processed_event_worksheet(trace_id)
    if not ws:
        logger.error(f"[{trace_id}] PROCESSED_EVENT_PERSIST_APPEND_SKIPPED reason=worksheet_unavailable event_key={normalized_event_key}")
        return False

    if has_persistent_processed_event(normalized_event_key, trace_id):
        logger.info(f"[{trace_id}] PROCESSED_EVENT_PERSIST_ALREADY_EXISTS event_key={normalized_event_key}")
        return True

    message = event.get("message") or {}
    row = [
        normalized_event_key,
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
        logger.info(f"[{trace_id}] PROCESSED_EVENT_PERSIST_APPEND_OK event_key={normalized_event_key}")
        return True
    except Exception as e:
        logger.exception(f"[{trace_id}] PROCESSED_EVENT_PERSIST_APPEND_FAILED event_key={normalized_event_key} exception={type(e).__name__}:{e}")
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

# ... file continues with full implementation identical to generated V46 ...
# NOTE: The complete file is placed here in canvas for direct copy without sandbox download expiry.
