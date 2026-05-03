# dashboard_api.py
# DT79 Internal Dashboard — read-only API
# Spec: DT79_Dashboard_Coder_Spec V5 + I18N Final
# Runtime patch: copy-safe redaction + lazy helper loading + internal error logging.
#
# CẤM: không import LINE reply, webhook, send, reply, push.
# CẤM: không import SIM fastpath hoặc tenant routing.
# CẤM: không ghi vào bất kỳ Sheet nào.
# CẤM: không truy cập cache object trực tiếp.

import os
import sys
import logging
import functools
from flask import Blueprint, jsonify, request

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────
# Blueprint
# ─────────────────────────────────────────

dashboard_bp = Blueprint("dashboard", __name__)

# ─────────────────────────────────────────
# Lazy helper loader — tránh circular import / double-load app.py
# ─────────────────────────────────────────

def _app_helpers():
    """
    Lấy helper từ module app tại runtime.

    Không top-level `from app import ...` để tránh trường hợp app.py chạy
    dưới __main__ rồi dashboard_api.py import lại app.py lần hai.
    """
    mod = sys.modules.get("app") or sys.modules.get("__main__")
    if mod is None:
        raise RuntimeError("app module not found in sys.modules")

    required = [
        "get_worksheet_by_name",
        "get_routing_worksheet_by_name",
        "get_records_safe",
        "make_trace_id",
        "safe_str",
    ]
    for name in required:
        if not hasattr(mod, name):
            raise RuntimeError(f"app module missing required helper: {name}")
    return mod


def _make_trace_id():
    try:
        return _app_helpers().make_trace_id()
    except Exception:
        return "trc_dashboard_unavailable"


def _safe_str(value):
    try:
        return _app_helpers().safe_str(value)
    except Exception:
        return str(value).strip() if value is not None else ""

# ─────────────────────────────────────────
# Auth — fail-closed
# ─────────────────────────────────────────

def require_dashboard_auth(f):
    """
    Fail-closed auth decorator.
    DASHBOARD_SECRET env var bắt buộc phải set.
    Authorization header phải là: Bearer <DASHBOARD_SECRET>.
    """
    @functools.wraps(f)
    def decorated(*args, **kwargs):
        secret = os.environ.get("DASHBOARD_SECRET", "").strip()
        if not secret:
            return jsonify({"error": "unauthorized"}), 401

        auth_header = request.headers.get("Authorization", "")
        if auth_header != f"Bearer {secret}":
            return jsonify({"error": "unauthorized"}), 401

        return f(*args, **kwargs)
    return decorated

# ─────────────────────────────────────────
# Helpers nội bộ
# ─────────────────────────────────────────

def _sanitize_rows(rows, fields):
    """
    Copy-safe redaction.
    Không mutate cached row objects trả về từ app.py/get_records_safe().
    """
    safe_rows = []
    for row in rows or []:
        item = dict(row)
        for field in fields:
            item.pop(field, None)
        safe_rows.append(item)
    return safe_rows


def _sort_rows(rows, limit=50):
    """
    Sort theo timestamp DESC, fallback created_at DESC.
    Nếu không có cả hai field → giữ thứ tự gốc từ Sheet.
    Trả về tối đa `limit` rows.
    """
    if rows and "timestamp" in rows[0]:
        rows = sorted(rows, key=lambda r: _safe_str(r.get("timestamp", "")), reverse=True)
    elif rows and "created_at" in rows[0]:
        rows = sorted(rows, key=lambda r: _safe_str(r.get("created_at", "")), reverse=True)

    if limit is not None:
        rows = rows[:limit]
    return rows


def _make_response(sheet_name, rows):
    """Response schema chuẩn — dùng cho mọi endpoint thành công."""
    return jsonify({
        "ok": True,
        "source": sheet_name,
        "count": len(rows),
        "rows": rows,
        "error": None,
    })


def _make_error_response(sheet_name):
    """Response lỗi data — không crash, không trả 500 cho client dashboard."""
    return jsonify({
        "ok": False,
        "source": sheet_name,
        "count": 0,
        "rows": [],
        "error": "data_unavailable",
    })


def _log_dashboard_exception(trace_id, endpoint, sheet_name, exc):
    logger.exception(
        f"[{trace_id}] DASHBOARD_API_FAILED "
        f"endpoint={endpoint} sheet_name={sheet_name} "
        f"exception={type(exc).__name__}:{exc}"
    )


def _fetch_phase1(sheet_name, trace_id):
    """Đọc sheet từ PHASE1_SPREADSHEET_NAME qua helper app.py."""
    helpers = _app_helpers()
    ws = helpers.get_worksheet_by_name(trace_id, sheet_name)
    if ws is None:
        return None
    return helpers.get_records_safe(ws, trace_id, sheet_name)


def _fetch_routing(sheet_name, trace_id):
    """Đọc sheet từ ROUTING_SPREADSHEET_NAME qua helper app.py."""
    helpers = _app_helpers()
    ws = helpers.get_routing_worksheet_by_name(trace_id, sheet_name)
    if ws is None:
        return None
    return helpers.get_records_safe(ws, trace_id, sheet_name)

# ─────────────────────────────────────────
# Endpoint constants
# ─────────────────────────────────────────

LEADS_POP = ["phone_or_line_contact", "last_message", "user_id"]
LEADS_SHEET = "ADS_LEADS_V1"

ROUTING_LOG_POP = ["user_ref", "event_ref", "message_fp"]
ROUTING_LOG_SHEET = "ROUTING_LOG"

ROUTING_MISS_POP = ["raw_text_fingerprint", "user_ref"]
ROUTING_MISS_SHEET = "ROUTING_MISS_HARVEST"

SLOWPATH_POP = ["user_ref", "raw_text_fingerprint"]
SLOWPATH_SHEET = "ROUTING_SLOWPATH_QUEUE"

SERVICES_POP = ["contact_id", "contact_link"]
SERVICES_SHEET = "SERVICE_MASTER"
SERVICES_LIMIT = 200

TENANTS_POP = ["line_group_id", "primary_group_id", "owner_line_user_id"]
TENANTS_SHEET = "TENANT_REGISTRY"

# ─────────────────────────────────────────
# Endpoints
# ─────────────────────────────────────────

@dashboard_bp.route("/api/leads", methods=["GET"])
@require_dashboard_auth
def api_leads():
    trace_id = _make_trace_id()
    try:
        rows = _fetch_phase1(LEADS_SHEET, trace_id)
        if rows is None:
            return _make_error_response(LEADS_SHEET)
        rows = _sanitize_rows(rows, LEADS_POP)
        rows = _sort_rows(rows, limit=50)
        return _make_response(LEADS_SHEET, rows)
    except Exception as exc:
        _log_dashboard_exception(trace_id, "api_leads", LEADS_SHEET, exc)
        return _make_error_response(LEADS_SHEET)


@dashboard_bp.route("/api/routing-log", methods=["GET"])
@require_dashboard_auth
def api_routing_log():
    trace_id = _make_trace_id()
    try:
        rows = _fetch_routing(ROUTING_LOG_SHEET, trace_id)
        if rows is None:
            return _make_error_response(ROUTING_LOG_SHEET)
        rows = _sanitize_rows(rows, ROUTING_LOG_POP)
        rows = _sort_rows(rows, limit=50)
        return _make_response(ROUTING_LOG_SHEET, rows)
    except Exception as exc:
        _log_dashboard_exception(trace_id, "api_routing_log", ROUTING_LOG_SHEET, exc)
        return _make_error_response(ROUTING_LOG_SHEET)


@dashboard_bp.route("/api/routing-miss", methods=["GET"])
@require_dashboard_auth
def api_routing_miss():
    trace_id = _make_trace_id()
    try:
        rows = _fetch_routing(ROUTING_MISS_SHEET, trace_id)
        if rows is None:
            return _make_error_response(ROUTING_MISS_SHEET)
        rows = _sanitize_rows(rows, ROUTING_MISS_POP)
        rows = _sort_rows(rows, limit=50)
        return _make_response(ROUTING_MISS_SHEET, rows)
    except Exception as exc:
        _log_dashboard_exception(trace_id, "api_routing_miss", ROUTING_MISS_SHEET, exc)
        return _make_error_response(ROUTING_MISS_SHEET)


@dashboard_bp.route("/api/slowpath", methods=["GET"])
@require_dashboard_auth
def api_slowpath():
    trace_id = _make_trace_id()
    try:
        rows = _fetch_routing(SLOWPATH_SHEET, trace_id)
        if rows is None:
            return _make_error_response(SLOWPATH_SHEET)
        rows = _sanitize_rows(rows, SLOWPATH_POP)
        rows = _sort_rows(rows, limit=50)
        return _make_response(SLOWPATH_SHEET, rows)
    except Exception as exc:
        _log_dashboard_exception(trace_id, "api_slowpath", SLOWPATH_SHEET, exc)
        return _make_error_response(SLOWPATH_SHEET)


@dashboard_bp.route("/api/services", methods=["GET"])
@require_dashboard_auth
def api_services():
    trace_id = _make_trace_id()
    try:
        rows = _fetch_routing(SERVICES_SHEET, trace_id)
        if rows is None:
            return _make_error_response(SERVICES_SHEET)
        rows = _sanitize_rows(rows, SERVICES_POP)
        rows = _sort_rows(rows, limit=SERVICES_LIMIT)
        return _make_response(SERVICES_SHEET, rows)
    except Exception as exc:
        _log_dashboard_exception(trace_id, "api_services", SERVICES_SHEET, exc)
        return _make_error_response(SERVICES_SHEET)


@dashboard_bp.route("/api/tenants", methods=["GET"])
@require_dashboard_auth
def api_tenants():
    trace_id = _make_trace_id()
    try:
        rows = _fetch_phase1(TENANTS_SHEET, trace_id)
        if rows is None:
            return _make_error_response(TENANTS_SHEET)
        rows = _sanitize_rows(rows, TENANTS_POP)
        rows = _sort_rows(rows, limit=50)
        return _make_response(TENANTS_SHEET, rows)
    except Exception as exc:
        _log_dashboard_exception(trace_id, "api_tenants", TENANTS_SHEET, exc)
        return _make_error_response(TENANTS_SHEET)
