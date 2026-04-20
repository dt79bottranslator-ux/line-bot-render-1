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
        expected = base64.b64encode(digest).decode("utf-8")
        ok = hmac.compare_digest(expected, signature)
        logger.info(f"[{trace_id}] LINE_SIGNATURE_CHECK ok={ok}")
        return ok
    except Exception as e:
        logger.exception(f"[{trace_id}] LINE_SIGNATURE_CHECK_FAILED exception={type(e).__name__}:{e}")
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


def reply_line_text(reply_token: str, text: str, trace_id: str, language_group: str = "") -> bool:
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
        "messages": [{"type": "text", "text": text}],
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
            f"[{trace_id}] LINE_REPLY_HTTP status_code={resp.status_code} "
            f"body={json.dumps(body_preview, ensure_ascii=False)}"
        )
        return 200 <= resp.status_code < 300
    except Exception as e:
        logger.exception(f"[{trace_id}] LINE_REPLY_EXCEPTION exception={type(e).__name__}:{e}")
        return False


def handle_worker_entry(language_group: str) -> str:
    return t(language_group, "worker_entry")


def handle_worker_message(text: str, language_group: str) -> str:
    return t(language_group, "worker_message", text=text)


def handle_ads_entry(language_group: str) -> str:
    return t(language_group, "ads_entry")


def handle_ads_message(text: str, language_group: str, user_id: str, trace_id: str) -> str:
    normalized = normalize_command_text(text)

    if normalized.isdigit():
        return t(language_group, "ads_select_invalid")

    clear_ok = clear_user_flow(user_id, trace_id)
    if not clear_ok:
        logger.error(f"[{trace_id}] USER_STATE_CLEAR_FAILED source=ads_text_fallback user_id={user_id}")
    return t(language_group, "default_echo", text=text)


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


def handle_ads_empty_message(language_group: str) -> str:
    return t(language_group, "ads_empty")


def handle_ads_read_failed_message(language_group: str) -> str:
    return t(language_group, "ads_read_failed")


def filter_ads_rows_for_viewer(rows: List[dict], language_group: str) -> List[dict]:
    viewer_language = normalize_language_group(language_group)
    filtered = []
    for row in rows:
        visibility_policy = safe_str(row.get("visibility_policy")).lower()
        author_language_group = normalize_language_group(row.get("author_language_group"))
        if visibility_policy == VISIBILITY_SAME_LANGUAGE_ONLY and author_language_group != viewer_language:
            continue
        filtered.append(row)
    return filtered


def truncate_text(value: str, max_len: int) -> str:
    raw = safe_str(value)
    if len(raw) <= max_len:
        return raw
    return raw[: max_len - 3].rstrip() + "..."


def build_ads_catalog_reply(language_group: str, ads_rows: List[dict]) -> str:
    if not ads_rows:
        return handle_ads_empty_message(language_group)

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


def load_ads_reply_message(language_group: str, trace_id: str) -> Tuple[str, bool]:
    rows, read_ok = load_ads_catalog_rows(trace_id)
    if not read_ok:
        return handle_ads_read_failed_message(language_group), False
    filtered_rows = filter_ads_rows_for_viewer(rows, language_group)
    return build_ads_catalog_reply(language_group, filtered_rows), True


def dispatch_text_event(event: dict, trace_id: str) -> dict:
    user_id = get_event_user_id(event)
    reply_token = get_reply_token(event)
    text = get_message_text(event)
    normalized = normalize_command_text(text)
    current_flow = resolve_user_flow(user_id, trace_id)
    current_language = resolve_user_language(user_id, trace_id)
    requested_language = parse_lang_command(text)

    logger.info(
        f"[{trace_id}] LINE_TEXT_DISPATCH user_id={user_id} "
        f"reply_token_present={bool(reply_token)} "
        f"text={json.dumps(text, ensure_ascii=False)} "
        f"normalized={json.dumps(normalized, ensure_ascii=False)} "
        f"current_flow={current_flow} current_language={current_language}"
    )

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
            reply_text, ads_read_ok = load_ads_reply_message(current_language, trace_id)
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
                logger.error(
                    f"[{trace_id}] USER_LANGUAGE_PERSIST_FAILED command=/lang "
                    f"user_id={user_id} requested_language={requested_language}"
                )
                reply_text = handle_state_save_failed_message(current_language)
                flow_used = current_flow or "lang_persist_failed"
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

    elif current_flow == FLOW_ADS:
        reply_text = handle_ads_message(text, current_language, user_id, trace_id)
        flow_used = FLOW_ADS

    else:
        reply_text = t(current_language, "default_echo", text=text)
        flow_used = "default"

    reply_ok = reply_line_text(reply_token, reply_text, trace_id, reply_language)
    return {
        "handled": True,
        "event_type": "message",
        "message_type": "text",
        "flow_used": flow_used,
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
        return jsonify({
            "ok": False,
            "app_version": APP_VERSION,
            "trace_id": trace_id,
            "latency_ms": ms_since(started),
            "error": "unauthorized",
        }), 401

    if "run_publish_sync_once" not in globals():
        logger.error(f"[{trace_id}] PUBLISH_SYNC_FN_MISSING")
        return jsonify({
            "ok": False,
            "app_version": APP_VERSION,
            "trace_id": trace_id,
            "latency_ms": ms_since(started),
            "error": "publish_sync_fn_missing",
        }), 500

    sync_result = run_publish_sync_once(trace_id)
    status_code = 200 if bool(sync_result.get("ok")) else 409

    return jsonify({
        "ok": bool(sync_result.get("ok")),
        "app_version": APP_VERSION,
        "trace_id": trace_id,
        "latency_ms": ms_since(started),
        "result": sync_result,
    }), status_code


@app.route("/callback", methods=["POST"])
def callback():
    trace_id = make_trace_id()
    started = time.perf_counter()

    raw_body = request.get_data() or b""
    signature = safe_str(request.headers.get("X-Line-Signature"))

    logger.info(f"[{trace_id}] CALLBACK_IN content_length={len(raw_body)} signature_present={bool(signature)}")

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
            if is_duplicate_event(event, trace_id):
                results.append({
                    "handled": False,
                    "reason": "duplicate_event",
                })
                continue

            result = dispatch_line_event(event, trace_id)
            results.append(result)

            if bool(result.get("handled")):
                mark_event_processed(event, trace_id)

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


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")))
