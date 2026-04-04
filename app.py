import os
import json
import time
import hashlib
from flask import Flask, request, abort
from linebot import LineBotApi, WebhookHandler
from linebot.exceptions import InvalidSignatureError
from linebot.models import MessageEvent, TextMessage, TextSendMessage
import gspread
from google.oauth2.service_account import Credentials

# --- CẤU HÌNH HỆ THỐNG DT79 ---
# Khóa Admin định danh từ image_074ba6.png
ADMIN_ID = "U83c6ce008a35ef17edaff25ac003370"

app = Flask(__name__)

# Kết nối LINE API
line_bot_api = LineBotApi(os.getenv('LINE_CHANNEL_ACCESS_TOKEN'))
handler = WebhookHandler(os.getenv('LINE_CHANNEL_SECRET'))

# Kết nối Google Sheets
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
    msg_text = event.message.text.strip()
    user_id = event.source.user_id

    # Lệnh kiểm tra ID cá nhân
    if msg_text == "/me":
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text=f"ID của bạn:\n{user_id}"))
        return

    # Lệnh cấp quyền Admin (Chỉ dành cho ADMIN_ID)
    if msg_text.startswith("/grant"):
        if user_id != ADMIN_ID:
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text=f"❌ Từ chối! ID không khớp Admin.\nID thực tế:\n{user_id}"))
            return
        
        parts = msg_text.split()
        if len(parts) < 2:
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text="❌ Sai cú pháp. Dùng: /grant [ID]"))
            return

        target_uid = parts[1]
        try:
            sh = get_sheet()
            # Bước 1: Ghi log vào ACCESS_EVENTS (Cơ chế Hộp đen)
            event_sheet = sh.worksheet("ACCESS_EVENTS")
            event_id = hashlib.md5(f"{target_uid}{time.time()}".encode()).hexdigest()[:8]
            
            event_sheet.append_row([
                event_id, target_uid, "GRANT_PREMIUM", ADMIN_ID, 
                time.strftime("%Y-%m-%d %H:%M:%S"), "Manual Grant", 
                "{}", "LOCKED", "PENDING", "Processing..."
            ])

            # Bước 2: Cập nhật quyền tại USER_LANG_MAP
            user_sheet = sh.worksheet("USER_LANG_MAP")
            cell = user_sheet.find(target_uid)
            
            if cell:
                user_sheet.update_cell(cell.row, 4, "TRUE") # Cột Premium
                user_sheet.update_cell(cell.row, 3, time.strftime("%Y-%m-%d %H:%M:%S")) # Cột Timestamp
                res_msg = f"✅ Đã nâng cấp Premium cho ID: {target_uid}\nLog ID: {event_id}"
            else:
                # Nếu chưa có thì thêm mới
                user_sheet.append_row([target_uid, "vi", time.strftime("%Y-%m-%d %H:%M:%S"), "TRUE", "0", "WORKER", "Added via Admin"])
                res_msg = f"✅ Đã tạo mới & cấp Premium cho ID: {target_uid}\nLog ID: {event_id}"

            line_bot_api.reply_message(event.reply_token, TextSendMessage(text=res_msg))

        except Exception as e:
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text=f"❌ Lỗi hệ thống: {str(e)}"))

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
