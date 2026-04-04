# =========================================================
# DT79_V16_REINFORCED - BẢN GIA CỐ THỰC CHIẾN
# =========================================================

# (Giữ nguyên các phần import và CONFIG của anh)

def append_pending_event(ws_event, event_id: str, target: str, admin_uid: str, ts: str, payload_json: str) -> tuple[bool, str, int]:
    try:
        # Lấy số dòng hiện tại trước khi ghi để xác định vị trí chính xác
        # Tránh việc tìm kiếm ID sau khi ghi (chậm và dễ sai lệch khi trùng ID)
        current_rows = len(ws_event.col_values(1))
        target_row = current_rows + 1
        
        row = [
            event_id, target, "grant_premium", admin_uid, ts,
            "SYSTEM_START", payload_json, "PENDING", "PENDING", "Processing..."
        ]
        ws_event.append_row(row)
        
        # Hậu kiểm: Xác nhận dòng vừa ghi đúng là EVENT_ID đó
        confirm_id = ws_event.cell(target_row, 1).value
        if confirm_id != event_id:
            # Nếu lệch dòng (do có người ghi cùng lúc), tìm lại trong 5 dòng cuối
            last_ids = ws_event.col_values(1)[-5:]
            for i, _id in enumerate(reversed(last_ids)):
                if _id == event_id:
                    return True, "OK", (current_rows + 1 - i)
            return False, "SYNC_ERROR: Event ID lost in stream", -1
            
        return True, "OK", target_row
    except Exception as e:
        return False, f"CRITICAL_EVENT_FAIL: {str(e)}", -1

def apply_user_grant(ws_user, target: str, ts: str) -> tuple[bool, str]:
    # Gia cố tìm kiếm: Loại bỏ hoàn toàn khoảng trắng khi so khớp cột A
    try:
        col_uids = [normalize_id(x) for x in ws_user.col_values(1)]
        indices = [i + 1 for i, val in enumerate(col_uids) if val == target]
        
        # Loại bỏ header (1) và sentinel (2)
        indices = [i for i in indices if i > 2]

        if len(indices) > 1:
            return False, f"INTEGRITY_ERR: Duplicate UID at rows {indices}"

        if len(indices) == 1:
            row_num = indices[0]
            # Batch update để giảm thiểu rủi ro đứt kết nối giữa chừng
            ws_user.update_cell(row_num, 3, ts)      # Timestamp
            ws_user.update_cell(row_num, 4, "TRUE")  # Premium
            return True, f"STATE_UPDATED_ROW_{row_num}"

        # Nếu không thấy, append mới
        ws_user.append_row([target, "en", ts, "TRUE", "0", "USER", "DT79_AUTO_GRANT"])
        return True, "STATE_CREATED_NEW_USER"
    except Exception as e:
        return False, f"STATE_WRITE_FAIL: {str(e)}"

# =========================================================
# MAIN HANDLER (Gia cố logic chốt chặn)
# =========================================================
# (Trong handler, phần logic chính của anh)

    # Bước: Finalize Event
    # Dù apply_user_grant thành công hay thất bại, bắt buộc phải ghi lại kết quả
    finalize_ok = finalize_event(
        ws_event=ws_event,
        event_row=event_row,
        event_id=event_id,
        target=target,
        admin_uid=uid,
        ts=ts,
        result_status=final_status,
        result_message=result_message,
        payload_json=payload_json,
    )

    if success:
        status_icon = "✅" if finalize_ok else "⚠️ (Log Fail)"
        reply_msg(token, f"{status_icon} TRUY CỐT THÀNH CÔNG\nTarget: {target}\nStatus: {result_message}\nID: {event_id}")
    else:
        reply_msg(token, f"❌ TRUY CỐT THẤT BẠI\nLý do: {result_message}\nID: {event_id}")

# (Giữ nguyên các phần còn lại của V16)
