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
APP_VERSION = "PHASE1_RUNTIME_STATE_SAFE__WORKER_ADS_PHONE_SUBMIT_FLOW__WORKSPACE_VALIDATION__PUBLISH_SYNC_V25"
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
