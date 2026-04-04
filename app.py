import os
import json
import time
import hashlib
import re
from flask import Flask, request, abort
from linebot import LineBotApi, WebhookHandler
from linebot.exceptions import InvalidSignatureError
from linebot.models import MessageEvent, TextMessage, TextSendMessage
import gspread
from google.oauth2.service_account import Credentials

# --- [BƯỚC 1] KHÓA CỨNG ADMIN - VỆ SINH TUYỆT ĐỐI ---
# Hàm re.sub xóa bỏ tất cả ký tự không phải chữ và số (loại bỏ newline, space, rác)
RAW_ID = "U83c6ce008a35ef17edaff25ac003370"
ADMIN_ID = re.sub(r'[^a-zA-Z0-9]', '', RAW_ID)

app = Flask(__name__)

# Kết nối API
line_bot_api = LineBotApi(os.getenv('LINE_CHANNEL_ACCESS_TOKEN'))
handler = WebhookHandler(os.getenv('LINE_CHANNEL_SECRET'))

def get_sheet():
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds_json = json.loads(os.getenv('GOOGLE_SERVICE_ACCOUNT_JSON'))
    creds = Credentials.from_service_account_info(creds_json, scopes=scopes)
    client = gspread.authorize(creds)
    return client.open_by_key(os.getenv('GOOGLE_SHEET_ID'))

@app.route("/callback", methods=['POST'])
def callback():
    signature = request.headers.get('X-Line-Signature')
    body = request.get_data(as_text=True)
    try:
        handler.handle(body, signature)
    except InvalidSignatureError:
        abort(400)
    return 'OK'

@handler.add(MessageEvent, message=TextMessage)
def handle_message(event):
    # Làm sạch tin nhắn và ID người gửi ngay lập tức
    msg_text = event.message.text.strip()
    user_id = re.sub(r'[^a-zA-Z0-9]', '', event.source.user_id)

    # Lệnh kiểm tra danh tính
    if msg_text == "/me":
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text=f"ID của bạn:\n{user_id}"))
        return

    # Lệnh cấp quyền (Logic so sánh đã được bảo vệ)
    if msg_text.startswith("/grant"):
        if user_id != ADMIN_ID:
            # Nếu sai, Bot sẽ show rõ ID trong code để anh đối chiếu
            line_bot_api.reply_message(event.reply_token, TextSendMessage(
                text=f"❌ TỪ CHỐI!\nID Admin trong Code: {ADMIN_ID}\nID thực tế của bạn: {user_id}"
            ))
            return
        
        parts = msg_text.split()
        if len(parts) < 2:
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text="❌ Sai cú pháp. Dùng: /grant [ID]"))
            return

        target_uid = re.sub(r'[^a-zA-Z0-9]', '', parts[1])
        try:
            sh = get_sheet()
            
            # 1. Ghi Log vào ACCESS_EVENTS (Bắt buộc phải có tab này)
            try:
                event_sheet = sh.worksheet("ACCESS_EVENTS")
                event_id = hashlib.md5(f"{target_uid}{time.time()}".encode()).hexdigest()[:8]
                event_sheet.append_row([
                    event_id, target_uid, "GRANT_PREMIUM", ADMIN_ID, 
                    time.strftime("%Y-%m-%d %H:%M:%S"), "SUCCESS", "{}", "LOCKED", "DONE", "OK"
                ])
            except Exception as e:
                line_bot_api.reply_message(event.reply_token, TextSendMessage(text=f"❌ Thiếu tab ACCESS_EVENTS hoặc lỗi: {str(e)}"))
                return

            # 2. Cập nhật USER_LANG_MAP
            user_sheet = sh.worksheet("USER_LANG_MAP")
            try:
                # Tìm kiếm UID ở cột A
                cell = user_sheet.find(target_uid)
                # Cập nhật cột D (Premium) và cột C (Timestamp)
                user_sheet.update_cell(cell.row, 4, "TRUE")
                user_sheet.update_cell(cell.row, 3, time.strftime("%Y-%m-%d %H:%M:%S"))
                res_msg = f"✅ Đã nâng cấp Premium cho:\n{target_uid}\nLog ID: {event_id}"
            except gspread.exceptions.CellNotFound:
                # Nếu không thấy thì tạo mới dòng người dùng
                user_sheet.append_row([target_uid, "vi", time.strftime("%Y-%m-%d %H:%M:%S"), "TRUE", "0", "WORKER", "AUTO_ADDED"])
                res_msg = f"✅ Đã tạo mới & cấp Premium:\n{target_uid}\nLog ID: {event_id}"

            line_bot_api.reply_message(event.reply_token, TextSendMessage(text=res_msg))

        except Exception as e:
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text=f"❌ Lỗi hệ thống: {str(e)}"))

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))
