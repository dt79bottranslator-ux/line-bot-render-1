"""
DT79 — OFFLINE_ROUTE_TIMEOUT_PROFILE_V1
CORRECTED PLAN V2 — CLEAN TEST HARNESS

File: test_route_profile.py

Purpose:
  Offline route-level timeout profiling for app.py after revert.

Scope:
  - Import dispatch_text_event() from app.py.
  - Do not call /callback.
  - Do not call LINE API real.
  - Do not call Google Sheets real.
  - Do not modify app.py.
  - Run A = BASELINE_MOCKED_NO_LEDGER.
  - Run B = SIMULATED_BLOCKING_IO_ONLY.

Run:
  python test_route_profile.py
"""

import copy
import json
import logging
import os
import sys
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Tuple
from unittest import mock


# ─────────────────────────────────────────────────────────────
# CONFIG — set fake env before importing app.py
# ─────────────────────────────────────────────────────────────

os.environ.setdefault("LINE_CHANNEL_SECRET", "fake_secret_for_offline_test")
os.environ.setdefault("LINE_CHANNEL_ACCESS_TOKEN", "fake_line_token_for_offline_test")
os.environ.setdefault("GOOGLE_API_KEY", "fake_google_api_key_for_offline_test")
os.environ.setdefault("PHASE1_SPREADSHEET_NAME", "FAKE_SPREADSHEET_OFFLINE_TEST")
os.environ.setdefault("ROUTING_SPREADSHEET_NAME", "FAKE_ROUTING_SPREADSHEET_OFFLINE_TEST")
os.environ.setdefault("MT_TRANSLATION_ENABLED", "1")
os.environ.setdefault("ASYNC_LOG_ENABLED", "1")
os.environ.setdefault("EVENT_INBOX_WORKER_ENABLED", "0")
os.environ.setdefault("ALERT_MANAGER_PUSH_ENABLED", "0")
os.environ.setdefault("USER_RATE_LIMIT_WINDOW_SECONDS", "10")
os.environ.setdefault("USER_RATE_LIMIT_MAX_EVENTS", "5")

os.environ.setdefault(
    "GOOGLE_SERVICE_ACCOUNT_JSON",
    json.dumps(
        {
            "type": "service_account",
            "project_id": "fake-project",
            "private_key_id": "fake-key-id",
            "private_key": (
                "-----BEGIN RSA PRIVATE KEY-----\n"
                "MIIEowIBAAKCAQEA0Z3VS5JJcds3xHnygWep4PAtEsHAE\n"
                "-----END RSA PRIVATE KEY-----\n"
            ),
            "client_email": "fake@fake-project.iam.gserviceaccount.com",
            "client_id": "123456789",
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
        }
    ),
)


# ─────────────────────────────────────────────────────────────
# FAKE GOOGLE SHEETS
# ─────────────────────────────────────────────────────────────

def _records_to_values(headers: List[str], rows: List[Dict[str, str]]) -> List[List[str]]:
    return [headers] + [[str(row.get(header, "")) for header in headers] for row in rows]


SHEET_DATA: Dict[str, Tuple[List[str], List[Dict[str, str]]]] = {
    "BOT_CONFIG": (
        ["Key", "Value"],
        [
            {"Key": "intent_sheet", "Value": "INTENT_MASTER"},
            {"Key": "service_sheet", "Value": "SERVICE_MASTER"},
            {"Key": "location_alias_sheet", "Value": "LOCATION_ALIAS_MASTER_V2"},
            {"Key": "location_canonical_sheet", "Value": "LOCATION_CANONICAL_MASTER"},
            {"Key": "location_region_map_sheet", "Value": "LOCATION_SERVICE_REGION_MAP"},
            {"Key": "variant_sheet", "Value": "SERVICE_VARIANT_MASTER"},
            {"Key": "fallback_location", "Value": "TW_ALL"},
        ],
    ),
    "INTENT_MASTER": (
        ["intent_name", "keywords"],
        [
            {"intent_name": "sim_mang_di_dong", "keywords": "sim,mua sim,làm sim,lam sim,cần sim,can sim"},
            {"intent_name": "thue_phong_tro", "keywords": "phòng,phong,thuê phòng,thue phong,tìm phòng,tim phong"},
            {"intent_name": "chat_general", "keywords": "xin chào,hello,hi"},
        ],
    ),
    "SERVICE_MASTER": (
        ["tenant_id", "service_id", "intent_name", "contact_id", "contact_link", "service_scope", "service_name", "service_region_key", "location", "service_status", "priority"],
        [
            {
                "tenant_id": "TENANT_001",
                "service_id": "SIM_TW_001",
                "intent_name": "sim_mang_di_dong",
                "contact_id": "seller_line_id",
                "contact_link": "https://line.me/R/ti/p/@688xvjuc",
                "service_scope": "SIM",
                "service_name": "SIM Taiwan",
                "service_region_key": "TW_ALL",
                "location": "TW_ALL",
                "service_status": "active",
                "priority": "10",
            }
        ],
    ),
    "LOCATION_ALIAS_MASTER_V2": (
        ["alias_text", "normalized_alias", "location_id", "root_location", "alias_type", "lang_group"],
        [
            {"alias_text": "cao hùng", "normalized_alias": "cao hung", "location_id": "KH_CITY", "root_location": "", "alias_type": "common_input", "lang_group": "vi"},
            {"alias_text": "高雄", "normalized_alias": "高雄", "location_id": "KH_CITY", "root_location": "", "alias_type": "district_zh", "lang_group": "zh"},
            {"alias_text": "toàn đài loan", "normalized_alias": "toan dai loan", "location_id": "", "root_location": "TW_ALL", "alias_type": "common_input", "lang_group": "vi"},
        ],
    ),
    "LOCATION_CANONICAL_MASTER": (
        ["location_id", "canonical_zh", "canonical_en", "district_town_zh", "county_city_zh", "service_region_key"],
        [
            {"location_id": "KH_CITY", "canonical_zh": "高雄", "canonical_en": "Kaohsiung", "district_town_zh": "", "county_city_zh": "高雄市", "service_region_key": "TW_ALL"},
        ],
    ),
    "LOCATION_SERVICE_REGION_MAP": (
        ["location_id", "service_region_key"],
        [
            {"location_id": "KH_CITY", "service_region_key": "TW_ALL"},
        ],
    ),
    "SERVICE_VARIANT_MASTER": (
        ["service_id", "network", "duration", "type", "price"],
        [
            {"service_id": "SIM_TW_001", "network": "OK", "duration": "6m", "type": "new", "price": "999"},
            {"service_id": "SIM_TW_001", "network": "Chunghwa", "duration": "12m", "type": "new", "price": "2999"},
        ],
    ),
    "ADS_LEADS_V1": (
        [
            "lead_id", "timestamp", "user_id", "source_type", "source_group_id", "intent",
            "service_id", "service_type", "location", "budget", "people_count", "move_in_date",
            "telco_preference", "duration", "phone_or_line_contact", "lead_status", "last_message",
            "created_by", "updated_at",
        ],
        [],
    ),
    "ROUTING_LOG": (
        ["timestamp", "tenant_id", "user_ref", "event_ref", "intent", "service_id", "location", "message_fp", "message_len"],
        [],
    ),
    "ROUTING_MISS_HARVEST": (
        ["timestamp", "trace_id", "tenant_id", "user_ref", "raw_text_fingerprint", "normalized_text_fingerprint", "intent_guess", "location_token_guess", "candidate_aliases", "miss_type", "recommended_fix", "status", "reviewer_notes"],
        [],
    ),
    "ROUTING_SLOWPATH_QUEUE": (
        ["timestamp", "trace_id", "tenant_id", "user_ref", "raw_text_fingerprint", "normalized_text_fingerprint", "detected_intent", "candidate_locations", "candidate_location_ids", "reason_code", "confidence", "action_needed", "status", "reviewer_notes"],
        [],
    ),
    "ROUTING_SHADOW_SUGGESTIONS": (
        ["timestamp", "trace_id", "tenant_id", "user_ref", "raw_text_fingerprint", "normalized_text_fingerprint", "intent_name", "location_token_guess", "suggested_alias", "suggested_location_id", "suggested_region_key", "suggestion_source", "suggestion_reason", "confidence", "review_status", "reviewer_notes", "second_candidate_alias", "second_candidate_location_id", "score_gap", "decision_context", "writeback_status", "writeback_at", "writeback_target", "writeback_notes"],
        [],
    ),
}


# Tenant translation core sheets.
# These fake sheets are intentionally minimal and stay in local memory only.
SHEET_DATA.update({
    "TENANT_CONFIG": (
        ["tenant_id", "tenant_name", "status", "remaining_quota", "quota_unit", "reply_mode", "glossary_enabled", "log_enabled", "target_default", "translation_core_enabled", "note"],
        [
            {
                "tenant_id": "TENANT_001",
                "tenant_name": "Offline Test Tenant",
                "status": "ACTIVE",
                "remaining_quota": "9999",
                "quota_unit": "message",
                "reply_mode": "reply",
                "glossary_enabled": "TRUE",
                "log_enabled": "TRUE",
                "target_default": "zh-TW",
                "translation_core_enabled": "TRUE",
                "note": "offline_route_profile",
            }
        ],
    ),
    "TENANT_SOURCE_MAP": (
        ["source_id", "source_type", "tenant_id", "status", "note"],
        [],
    ),
    "TENANT_GLOSSARY": (
        ["tenant_id", "source_term", "target_term", "match_mode", "case_sensitive", "status", "note"],
        [],
    ),
    "TRANSLATION_LOG": (
        ["timestamp", "trace_id", "tenant_id", "source_ref", "source_type", "user_ref", "target_lang", "direction", "direction_confidence", "direction_reason", "raw_text_fp", "glossary_text_fp", "translated_len", "quota_before", "quota_after", "status", "error"],
        [],
    ),
    "TRANSLATION_HEALTH_LOG": (
        ["timestamp", "event_id", "source_type", "tenant_id", "user_id_hash", "source_lang", "target_lang", "domain", "raw_text_hash", "raw_text_redacted", "selected_model", "fallback_used", "latency_ms", "json_valid", "glossary_terms_detected", "glossary_terms_applied", "semantic_risk_level", "final_status", "notes"],
        [],
    ),
})

class FakeWorksheet:
    def __init__(self, title: str):
        self.title = title
        self.name = title
        if title not in SHEET_DATA:
            SHEET_DATA[title] = (["col1", "col2"], [])

    def get_all_values(self, *args: Any, **kwargs: Any) -> List[List[str]]:
        headers, rows = SHEET_DATA.get(self.title, (["col1", "col2"], []))
        return _records_to_values(headers, rows)

    def get_all_records(self, *args: Any, **kwargs: Any) -> List[Dict[str, str]]:
        _headers, rows = SHEET_DATA.get(self.title, (["col1", "col2"], []))
        return [dict(row) for row in rows]

    def append_row(self, values: List[Any], *args: Any, **kwargs: Any) -> Dict[str, Any]:
        headers, rows = SHEET_DATA.get(self.title, (["col1", "col2"], []))
        row = {header: str(values[idx]) if idx < len(values) else "" for idx, header in enumerate(headers)}
        rows.append(row)
        SHEET_DATA[self.title] = (headers, rows)
        return {"updates": {"updatedRows": 1}}

    def update(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        return {"updated": True}

    def update_title(self, new_title: str) -> None:
        SHEET_DATA[new_title] = SHEET_DATA.pop(self.title, (["col1", "col2"], []))
        self.title = new_title
        self.name = new_title


class FakeSpreadsheet:
    def worksheet(self, name: str) -> FakeWorksheet:
        return FakeWorksheet(name)

    def worksheets(self) -> List[FakeWorksheet]:
        return [FakeWorksheet(name) for name in SHEET_DATA.keys()]

    def add_worksheet(self, title: str, rows: int = 1000, cols: int = 20, *args: Any, **kwargs: Any) -> FakeWorksheet:
        if title not in SHEET_DATA:
            SHEET_DATA[title] = ([f"col{i + 1}" for i in range(cols)], [])
        return FakeWorksheet(title)


class FakeGspreadClient:
    def open(self, name: str) -> FakeSpreadsheet:
        return FakeSpreadsheet()

    def open_by_key(self, key: str) -> FakeSpreadsheet:
        return FakeSpreadsheet()


@dataclass
class FakeResponse:
    status_code: int = 200
    text: str = "OK"

    def json(self) -> Dict[str, Any]:
        return {
            "data": {
                "translations": [
                    {
                        "translatedText": "這是一個離線測試翻譯。",
                        "detectedSourceLanguage": "vi",
                    }
                ]
            },
            "ok": True,
        }

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"fake_http_error status_code={self.status_code}")


_fake_gspread_client = FakeGspreadClient()


def _fake_requests_post(*args: Any, **kwargs: Any) -> FakeResponse:
    return FakeResponse(status_code=200, text="OK")


_PATCHERS = [
    mock.patch("gspread.authorize", return_value=_fake_gspread_client),
    mock.patch("google.oauth2.service_account.Credentials.from_service_account_info", return_value=mock.MagicMock()),
    mock.patch("requests.post", side_effect=_fake_requests_post),
]

for patcher in _PATCHERS:
    patcher.start()


# ─────────────────────────────────────────────────────────────
# IMPORT APP
# ─────────────────────────────────────────────────────────────

print("[SETUP] Importing app.py with fake external services...")
_import_t0 = time.perf_counter()

try:
    import app as dt79_app
    from app import app as flask_app
    from app import dispatch_text_event, make_trace_id

    print(f"[SETUP] Import OK — {int((time.perf_counter() - _import_t0) * 1000)}ms")
except Exception as exc:
    print(f"[SETUP] Import FAILED: {type(exc).__name__}: {exc}")
    print("Check: test_route_profile.py must be in the same folder as app.py")
    for patcher in reversed(_PATCHERS):
        patcher.stop()
    sys.exit(1)


# ─────────────────────────────────────────────────────────────
# FAKE EVENTS
# ─────────────────────────────────────────────────────────────

FAKE_MT_EVENT: Dict[str, Any] = {
    "type": "message",
    "replyToken": "OFFLINE_TEST_REPLY_TOKEN_DO_NOT_SEND",
    "source": {"type": "user", "userId": "OFFLINE_TEST_USER_ID_BASE"},
    "message": {
        "type": "text",
        "id": "OFFLINE_TEST_MESSAGE_ID_BASE_MT",
        "text": "dịch sang tiếng trung: hôm nay tôi đi làm ca đêm",
    },
    "timestamp": 1710000000000,
}

FAKE_SIM_EVENT: Dict[str, Any] = {
    "type": "message",
    "replyToken": "OFFLINE_TEST_REPLY_TOKEN_DO_NOT_SEND",
    "source": {"type": "user", "userId": "OFFLINE_TEST_USER_ID_BASE"},
    "message": {
        "type": "text",
        "id": "OFFLINE_TEST_MESSAGE_ID_BASE_SIM",
        "text": "tôi cần mua sim ở cao hùng",
    },
    "timestamp": 1710000000001,
}

SEP = "=" * 72


# ─────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────

def _reset_runtime_noise() -> None:
    """Clear in-memory rate limit and per-test request noise without editing app.py."""
    for attr in ("_RATE_LIMIT_STATE", "_USER_FLOW_STATE", "_USER_LANGUAGE_STATE"):
        store = getattr(dt79_app, attr, None)
        if hasattr(store, "clear"):
            store.clear()


def _make_unique_event(event: Dict[str, Any], trace_id: str, label: str) -> Dict[str, Any]:
    cloned = copy.deepcopy(event)
    suffix = f"{label}_{trace_id}".replace("trc_", "").replace("-", "_").replace(":", "_")
    cloned.setdefault("source", {})
    cloned.setdefault("message", {})
    cloned["source"]["userId"] = f"OFFLINE_USER_{suffix}"
    cloned["message"]["id"] = f"OFFLINE_MSG_{suffix}"
    cloned["replyToken"] = f"OFFLINE_REPLY_{suffix}"
    cloned["timestamp"] = int(time.time() * 1000)
    return cloned




def _stable_hash_for_test(value: str, length: int = 12) -> str:
    import hashlib
    raw = str(value or "")
    if not raw:
        return "empty"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:length]


def _build_fake_tenant_cache_for_event(event: Dict[str, Any]) -> Dict[str, Any]:
    """
    Dynamic fake tenant cache for the unique offline event.
    Prevents MT_TENANT_NOT_FOUND false fail without editing app.py.
    """
    source = event.get("source") or {}
    source_type = str(source.get("type") or "").strip().lower()
    if source_type == "user":
        source_id = str(source.get("userId") or "").strip()
    elif source_type == "group":
        source_id = str(source.get("groupId") or "").strip()
    elif source_type == "room":
        source_id = str(source.get("roomId") or "").strip()
    else:
        source_id = ""

    source_ref = _stable_hash_for_test(source_id)

    tenant = {
        "tenant_id": "TENANT_001",
        "tenant_name": "Offline Test Tenant",
        "status": "ACTIVE",
        "remaining_quota": 9999,
        "quota_unit": "message",
        "reply_mode": "reply",
        "glossary_enabled": True,
        "log_enabled": True,
        "target_default": "zh-TW",
        "translation_core_enabled": True,
        "__row_index": 2,
    }

    source_meta = {
        "source_id": source_id,
        "source_ref": source_ref,
        "source_type": source_type or "user",
        "tenant_id": "TENANT_001",
        "status": "ACTIVE",
        "note": "offline_dynamic_source",
        "lookup_mode": "source_id",
    }

    return {
        "loaded": True,
        "loaded_at_ts": time.time(),
        "loaded_at_iso": "",
        "source_to_tenant": {source_id: "TENANT_001"} if source_id else {},
        "source_ref_to_tenant": {source_ref: "TENANT_001"} if source_ref else {},
        "source_meta": {source_id: source_meta} if source_id else {},
        "source_ref_meta": {source_ref: source_meta} if source_ref else {},
        "tenants": {"TENANT_001": tenant},
        "glossary": {"TENANT_001": []},
    }


def _sync_dynamic_tenant_sheet_for_event(event: Dict[str, Any]) -> None:
    """
    Keep fake TENANT_SOURCE_MAP aligned with the generated unique event.
    Local in-memory only; no Google Sheets call.
    """
    source = event.get("source") or {}
    source_type = str(source.get("type") or "").strip().lower()
    if source_type == "user":
        source_id = str(source.get("userId") or "").strip()
    elif source_type == "group":
        source_id = str(source.get("groupId") or "").strip()
    elif source_type == "room":
        source_id = str(source.get("roomId") or "").strip()
    else:
        source_id = ""

    if not source_id:
        return

    headers, rows = SHEET_DATA.get("TENANT_SOURCE_MAP", (["source_id", "source_type", "tenant_id", "status", "note"], []))
    rows = [row for row in rows if row.get("source_id") != source_id]
    rows.append({
        "source_id": source_id,
        "source_type": source_type or "user",
        "tenant_id": "TENANT_001",
        "status": "ACTIVE",
        "note": "offline_dynamic_source",
    })
    SHEET_DATA["TENANT_SOURCE_MAP"] = (headers, rows)


def run_dispatch(event: Dict[str, Any], trace_id: str, label: str) -> Dict[str, Any]:
    """
    Run dispatch_text_event() offline.
    Each call receives unique userId/message.id to prevent rate-limit false fail.
    """
    _reset_runtime_noise()
    unique_event = _make_unique_event(event, trace_id, label)
    _sync_dynamic_tenant_sheet_for_event(unique_event)
    fake_tenant_cache = _build_fake_tenant_cache_for_event(unique_event)

    def _fake_get_tenant_config(*args: Any, **kwargs: Any) -> Dict[str, Any]:
        return fake_tenant_cache

    with mock.patch("app.reply_line_text", return_value=True) as mocked_reply, \
         mock.patch("app.get_gspread_client", return_value=_fake_gspread_client), \
         mock.patch("app.get_tenant_config", side_effect=_fake_get_tenant_config):

        with flask_app.test_request_context("/"):
            t0 = time.perf_counter()
            try:
                result = dispatch_text_event(unique_event, trace_id)
                elapsed_ms = int((time.perf_counter() - t0) * 1000)
                return {
                    "label": label,
                    "status": "OK",
                    "elapsed_ms": elapsed_ms,
                    "result": result,
                    "reply_mock_called": mocked_reply.called,
                    "reply_mock_call_count": mocked_reply.call_count,
                    "error": "",
                }
            except Exception as exc:
                elapsed_ms = int((time.perf_counter() - t0) * 1000)
                return {
                    "label": label,
                    "status": "EXCEPTION",
                    "elapsed_ms": elapsed_ms,
                    "result": None,
                    "reply_mock_called": mocked_reply.called,
                    "reply_mock_call_count": mocked_reply.call_count,
                    "error": f"{type(exc).__name__}: {exc}",
                }


def _print_result(name: str, result: Dict[str, Any]) -> None:
    print(f"{name}.status={result['status']}")
    print(f"{name}.elapsed_ms={result['elapsed_ms']}")
    print(f"{name}.reply_mock_called={result['reply_mock_called']}")
    print(f"{name}.reply_mock_call_count={result['reply_mock_call_count']}")
    if result["error"]:
        print(f"{name}.error={result['error']}")
    if isinstance(result.get("result"), dict):
        payload = result["result"]
        print(f"{name}.handled={payload.get('handled')}")
        print(f"{name}.flow_used={payload.get('flow_used') or payload.get('flow')}")


# ─────────────────────────────────────────────────────────────
# RUN A
# ─────────────────────────────────────────────────────────────

def run_a_baseline() -> List[Tuple[str, Dict[str, Any]]]:
    print(f"\n{SEP}")
    print("RUN A — BASELINE_MOCKED_NO_LEDGER")
    print(SEP)

    cases = [
        ("MT_EVENT", FAKE_MT_EVENT),
        ("SIM_EVENT", FAKE_SIM_EVENT),
    ]

    results: List[Tuple[str, Dict[str, Any]]] = []

    for name, event in cases:
        trace_id = make_trace_id()
        result = run_dispatch(event, trace_id, name)
        results.append((name, result))
        print(f"\n{name}.trace_id={trace_id}")
        _print_result(name, result)

    print(f"\n--- RUN A VERDICT ---")
    run_a_pass = True

    for name, result in results:
        status_ok = result["status"] == "OK"
        fast_ok = result["elapsed_ms"] < 1000
        reply_ok = result["reply_mock_called"] is True
        case_pass = status_ok and fast_ok and reply_ok
        run_a_pass = run_a_pass and case_pass
        print(
            f"{name}.verdict={'PASS' if case_pass else 'FAIL'} "
            f"status_ok={status_ok} fast_under_1000={fast_ok} reply_mock_called={reply_ok}"
        )

    print(f"RUN_A_FINAL={'PASS' if run_a_pass else 'FAIL'}")
    return results


# ─────────────────────────────────────────────────────────────
# RUN B
# ─────────────────────────────────────────────────────────────

def run_b_simulated_blocking() -> List[Tuple[int, int, int, str]]:
    print(f"\n{SEP}")
    print("RUN B — SIMULATED_BLOCKING_IO_ONLY")
    print(SEP)

    delays_ms = [0, 200, 500, 1000, 2000]
    results: List[Tuple[int, int, int, str]] = []

    for delay_ms in delays_ms:
        trace_id = make_trace_id()
        t0 = time.perf_counter()
        result = run_dispatch(FAKE_MT_EVENT, trace_id, f"BLOCKING_{delay_ms}")
        dispatch_ms = result["elapsed_ms"]

        if delay_ms > 0:
            time.sleep(delay_ms / 1000.0)

        total_ms = int((time.perf_counter() - t0) * 1000)
        results.append((delay_ms, dispatch_ms, total_ms, result["status"]))

        print(
            f"simulated_blocking_ms={delay_ms} | "
            f"dispatch_ms={dispatch_ms} | "
            f"total_ms={total_ms} | "
            f"status={result['status']}"
        )

    print(f"\n--- RUN B VERDICT ---")
    complete = len(results) == len(delays_ms)
    all_ok = all(status == "OK" for _, _, _, status in results)
    print(f"RUN_B_FINAL={'PASS' if complete and all_ok else 'FAIL'}")
    return results


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────

def main() -> int:
    logging.basicConfig(level=logging.INFO, format="[LOG] %(message)s", stream=sys.stdout)

    print(SEP)
    print("DT79 — OFFLINE_ROUTE_TIMEOUT_PROFILE_V1")
    print("CORRECTED PLAN V2 — CLEAN")
    print(SEP)
    print("callback_http      : NOT_USED")
    print("line_api_real      : BLOCKED_BY_MOCK")
    print("google_sheets_real : BLOCKED_BY_FAKE_CLIENT")
    print("app_py_change      : NONE")
    print(SEP)

    results_a = run_a_baseline()
    results_b = run_b_simulated_blocking()

    run_a_pass = all(
        item["status"] == "OK" and item["elapsed_ms"] < 1000 and item["reply_mock_called"]
        for _, item in results_a
    )
    run_b_pass = len(results_b) == 5 and all(status == "OK" for _, _, _, status in results_b)

    print(f"\n{SEP}")
    print("FINAL SUMMARY")
    print(SEP)
    print(f"Run A BASELINE_MOCKED_NO_LEDGER  : {'PASS' if run_a_pass else 'FAIL'}")
    print(f"Run B SIMULATED_BLOCKING_IO_ONLY : {'PASS' if run_b_pass else 'FAIL'}")
    print(f"OFFLINE_ROUTE_TIMEOUT_PROFILE_V1 : {'PASS' if run_a_pass and run_b_pass else 'FAIL'}")
    print(SEP)

    return 0 if run_a_pass and run_b_pass else 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    finally:
        for _patcher in reversed(_PATCHERS):
            _patcher.stop()
