2026-04-23 03:44:54,412 | INFO | [trc_a62c858ea5cb] UPDATE_ROW_FIELDS_OK worksheet_name=processed_event_state row_index=183 columns=["processed_at", "trace_id"] values={"processed_at": "DONE::2026-04-23T11:44:54.214879+08:00", "trace_id": "trc_a62c858ea5cb"}
2026-04-23 03:44:54,412 | INFO | [trc_a62c858ea5cb] PROCESSED_EVENT_RUNTIME_STATE_SET event_key=webhook:01KPW71E45Z1HTZGZ3CK1J1SSZ status=done
2026-04-23 03:44:54,412 | INFO | [trc_a62c858ea5cb] EVENT_PROCESSING_FINALIZED event_key=webhook:01KPW71E45Z1HTZGZ3CK1J1SSZ success=True persist_ok=True
2026-04-23 03:44:54,412 | INFO | [trc_a62c858ea5cb] CALLBACK_DONE events=1 latency_ms=5982
10.24.4.132 - - [23/Apr/2026:03:44:54 +0000] "POST /callback HTTP/1.1" 200 334 "-" "LineBotWebhook/2.0"
2026-04-23 03:44:56,187 | INFO | [trc_2b6c68b2d8ca] CALLBACK_IN content_length=871 signature_present=True
2026-04-23 03:44:56,187 | INFO | [trc_2b6c68b2d8ca] LINE_SIGNATURE_CHECK ok=True
2026-04-23 03:44:56,187 | INFO | [trc_2b6c68b2d8ca] LINE_PAYLOAD_PARSED keys=['destination', 'events']
2026-04-23 03:44:56,187 | INFO | [trc_2b6c68b2d8ca] SPREADSHEET_SHARED_CACHE_HIT name=DT79_PHASE1_WORKER_CASES_TENANT_002
2026-04-23 03:44:56,619 | INFO | [trc_2b6c68b2d8ca] WORKSHEET_READ_OK worksheet_name=processed_event_state row_count=183
2026-04-23 03:44:56,619 | INFO | [trc_2b6c68b2d8ca] WORKSHEET_VALUES_CACHE_HIT worksheet_name=processed_event_state row_count=183
2026-04-23 03:44:56,619 | INFO | [trc_2b6c68b2d8ca] PROCESSED_EVENT_PERSIST_LOOKUP_MISS event_key=webhook:01KPW71NPW9SF2GF8KJ2M3YYC1
2026-04-23 03:44:56,619 | INFO | [trc_2b6c68b2d8ca] SPREADSHEET_CACHE_HIT name=DT79_PHASE1_WORKER_CASES_TENANT_002
2026-04-23 03:44:56,773 | INFO | [trc_2b6c68b2d8ca] WORKSHEET_VALUES_CACHE_HIT worksheet_name=processed_event_state row_count=183
2026-04-23 03:44:56,939 | INFO | [trc_2b6c68b2d8ca] PROCESSED_EVENT_PROCESSING_APPEND_OK event_key=webhook:01KPW71NPW9SF2GF8KJ2M3YYC1
2026-04-23 03:44:56,939 | INFO | [trc_2b6c68b2d8ca] PROCESSED_EVENT_RUNTIME_STATE_SET event_key=webhook:01KPW71NPW9SF2GF8KJ2M3YYC1 status=processing
2026-04-23 03:44:56,939 | INFO | [trc_2b6c68b2d8ca] EVENT_PROCESSING_BEGIN_OK event_key=webhook:01KPW71NPW9SF2GF8KJ2M3YYC1 source=new
2026-04-23 03:44:56,939 | INFO | [trc_2b6c68b2d8ca] LINE_EVENT_DISPATCH event_type=message
2026-04-23 03:44:56,939 | INFO | [trc_2b6c68b2d8ca] USER_FLOW_STATE_MISS user_id=U83c6ce008a35ef17edfaff25ac003370
2026-04-23 03:44:56,940 | INFO | [trc_2b6c68b2d8ca] SPREADSHEET_CACHE_HIT name=DT79_PHASE1_WORKER_CASES_TENANT_002
2026-04-23 03:44:57,096 | INFO | [trc_2b6c68b2d8ca] WORKSHEET_VALUES_SHARED_CACHE_HIT worksheet_name=user_state row_count=5
2026-04-23 03:44:57,096 | INFO | [trc_2b6c68b2d8ca] WORKSHEET_RECORDS_SHARED_CACHE_HIT worksheet_name=user_state count=4
2026-04-23 03:44:57,096 | INFO | [trc_2b6c68b2d8ca] USER_STATE_PERSIST_HIT user_id=U83c6ce008a35ef17edfaff25ac003370 flow=
2026-04-23 03:44:57,096 | INFO | [trc_2b6c68b2d8ca] USER_LANGUAGE_STATE_HIT user_id=U83c6ce008a35ef17edfaff25ac003370 language_group=vi
2026-04-23 03:44:57,096 | INFO | [trc_2b6c68b2d8ca] LINE_TEXT_DISPATCH user_id=U83c6ce008a35ef17edfaff25ac003370 reply_token_present=True text="tôi muốn tìm phòng ở xitun" normalized="tôi muốn tìm phòng ở xitun" current_flow= current_language=vi
2026-04-23 03:44:57,097 | INFO | [trc_2b6c68b2d8ca] ROUTING_WORKSHEET_SHARED_CACHE_HIT worksheet_name=BOT_CONFIG
2026-04-23 03:44:57,097 | INFO | [trc_2b6c68b2d8ca] WORKSHEET_RECORDS_SHARED_CACHE_HIT worksheet_name=BOT_CONFIG count=10
2026-04-23 03:44:57,097 | INFO | [trc_2b6c68b2d8ca] ROUTING_CONFIG_READY keys=["DEFAULT_TARGET_LANG", "ENABLE_GLOSSARY", "ENABLE_HUMANIZE", "ENABLE_LANG_COMMAND", "ENABLE_LOG_OK", "FALLBACK_MESSAGE", "fallback_location", "intent_sheet", "provider_sheet", "service_sheet"]
2026-04-23 03:44:57,097 | INFO | [trc_2b6c68b2d8ca] ROUTING_WORKSHEET_SHARED_CACHE_HIT worksheet_name=INTENT_MASTER
2026-04-23 03:44:57,097 | INFO | [trc_2b6c68b2d8ca] ROUTING_WORKSHEET_SHARED_CACHE_HIT worksheet_name=SERVICE_MASTER
2026-04-23 03:44:57,097 | INFO | [trc_2b6c68b2d8ca] ROUTING_WORKSHEET_SHARED_CACHE_HIT worksheet_name=LOCATION_ALIAS_MASTER
2026-04-23 03:44:57,097 | INFO | [trc_2b6c68b2d8ca] ROUTING_WORKSHEET_SHARED_CACHE_HIT worksheet_name=SERVICE_VARIANT_MASTER
2026-04-23 03:44:57,097 | INFO | [trc_2b6c68b2d8ca] WORKSHEET_RECORDS_SHARED_CACHE_HIT worksheet_name=INTENT_MASTER count=2
2026-04-23 03:44:57,097 | INFO | [trc_2b6c68b2d8ca] WORKSHEET_RECORDS_SHARED_CACHE_HIT worksheet_name=SERVICE_MASTER count=2
2026-04-23 03:44:57,097 | INFO | [trc_2b6c68b2d8ca] WORKSHEET_RECORDS_SHARED_CACHE_HIT worksheet_name=LOCATION_ALIAS_MASTER count=6
2026-04-23 03:44:57,097 | INFO | [trc_2b6c68b2d8ca] WORKSHEET_RECORDS_SHARED_CACHE_HIT worksheet_name=SERVICE_VARIANT_MASTER count=6
2026-04-23 03:44:57,098 | INFO | [trc_2b6c68b2d8ca] ROUTING_MATCH_OK intent_name=thue_phong_tro service_id=ROOM_TXG_001 contact_id=dungchina79 location_hint="台中" matched_keywords=["tìm phòng", "tim phong", "tôi muốn tìm phòng"]
2026-04-23 03:44:57,315 | INFO | [trc_2b6c68b2d8ca] LINE_REPLY_HTTP status_code=200 body="{\"sentMessages\":[{\"id\":\"610832586239115402\",\"quoteToken\":\"Ieb9qciVDWwL3e99-DBfzKAyoTiILAShSR18HSc2M6PR4DINN5yPhzEW0N_W5w0gR59Ra9XSvTcDgzrhc6aKRJXy4Sk3C_fsqUthnK6Ks8OwXRc7AfkC1yGqD_WapkszEwdjtpKeKUiZb8feBaqN0g\"}]}"
2026-04-23 03:44:57,316 | INFO | [trc_2b6c68b2d8ca] SPREADSHEET_CACHE_HIT name=DT79_PHASE1_WORKER_CASES_TENANT_002
2026-04-23 03:44:57,645 | INFO | [trc_2b6c68b2d8ca] WORKSHEET_READ_OK worksheet_name=processed_event_state row_count=184
2026-04-23 03:44:57,645 | INFO | [trc_2b6c68b2d8ca] WORKSHEET_VALUES_CACHE_HIT worksheet_name=processed_event_state row_count=184
2026-04-23 03:44:57,645 | INFO | [trc_2b6c68b2d8ca] PROCESSED_EVENT_PERSIST_LOOKUP event_key=webhook:01KPW71NPW9SF2GF8KJ2M3YYC1 status=processing row_index=184
2026-04-23 03:44:57,645 | INFO | [trc_2b6c68b2d8ca] SPREADSHEET_CACHE_HIT name=DT79_PHASE1_WORKER_CASES_TENANT_002
2026-04-23 03:44:57,798 | INFO | [trc_2b6c68b2d8ca] WORKSHEET_VALUES_CACHE_HIT worksheet_name=processed_event_state row_count=184
2026-04-23 03:44:57,798 | INFO | [trc_2b6c68b2d8ca] WORKSHEET_VALUES_CACHE_HIT worksheet_name=processed_event_state row_count=184
2026-04-23 03:44:57,957 | INFO | [trc_2b6c68b2d8ca] UPDATE_ROW_FIELDS_OK worksheet_name=processed_event_state row_index=184 columns=["processed_at", "trace_id"] values={"processed_at": "DONE::2026-04-23T11:44:57.798411+08:00", "trace_id": "trc_2b6c68b2d8ca"}
2026-04-23 03:44:57,958 | INFO | [trc_2b6c68b2d8ca] PROCESSED_EVENT_RUNTIME_STATE_SET event_key=webhook:01KPW71NPW9SF2GF8KJ2M3YYC1 status=done
2026-04-23 03:44:57,958 | INFO | [trc_2b6c68b2d8ca] EVENT_PROCESSING_FINALIZED event_key=webhook:01KPW71NPW9SF2GF8KJ2M3YYC1 success=True persist_ok=True
2026-04-23 03:44:57,958 | INFO | [trc_2b6c68b2d8ca] CALLBACK_DONE events=1 latency_ms=1770
10.29.69.3 - - [23/Apr/2026:03:44:57 +0000] "POST /callback HTTP/1.1" 200 334 "-" "LineBotWebhook/2.0"
==> Deploying...
==> Setting WEB_CONCURRENCY=1 by default, based on available CPUs in the instance
==> Running 'gunicorn app:app --bind 0.0.0.0:$PORT'
[2026-04-23 03:45:19 +0000] [58] [INFO] Starting gunicorn 25.3.0
[2026-04-23 03:45:19 +0000] [58] [INFO] Listening at: http://0.0.0.0:10000 (58)
[2026-04-23 03:45:19 +0000] [58] [INFO] Using worker: sync
[2026-04-23 03:45:19 +0000] [60] [INFO] Booting worker with pid: 60
[2026-04-23 03:45:19 +0000] [58] [INFO] Control socket listening at /opt/render/.gunicorn/gunicorn.ctl
127.0.0.1 - - [23/Apr/2026:03:45:19 +0000] "HEAD / HTTP/1.1" 200 0 "-" "Go-http-client/1.1"
==> Your service is live 🎉
==> 
==> ///////////////////////////////////////////////////////////
==> 
==> Available at your primary URL https://line-bot-render-1-m1kt.onrender.com
==> 
==> ///////////////////////////////////////////////////////////
10.27.230.1 - - [23/Apr/2026:03:45:27 +0000] "GET / HTTP/1.1" 200 2 "-" "Go-http-client/2.0"
[2026-04-23 03:45:23 +0000] [58] [INFO] Handling signal: term
[2026-04-23 03:45:23 +0000] [59] [INFO] Worker exiting (pid: 59)
[2026-04-23 03:45:24 +0000] [58] [INFO] Shutting down: Master
