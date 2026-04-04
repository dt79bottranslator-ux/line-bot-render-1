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

# --- KHÓA CỨNG ADMIN (ĐÃ VÁ LỖI KÝ TỰ ẨN) ---
RAW_ADMIN_ID = "U83c6ce008a35ef17edaff25ac003370"
ADMIN_ID = RAW_ADMIN_ID.strip()

app = Flask(__name__)

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
    msg_text = event.message.text.strip()
    # Tự động làm sạch ID người gửi
    user_id = event.source.user_id.strip()

    if msg_text == "/me":
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text=f"ID của bạn:\n{user_id}"))
        return

    if msg_text.startswith("/grant"):
        # So sánh sau khi đã strip() cả 2 đầu
        if user_id != ADMIN_ID:
            line_bot_api.reply_message(event.reply_token, TextSendMessage(
                text=f"❌ TỪ CHỐI!\nID Admin chuẩn: {ADMIN_ID}\nID của bạn: {user_id}\n(Vui lòng cập nhật lại ADMIN_ID trong code nếu không khớp)"
            ))
            return
        
        parts = msg_text.split()
        if len(parts) < 2:
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text="❌ Dùng: /grant [ID]"))
            return

        target_uid = parts[1].strip()
        try:
            sh = get_sheet()
            # Ghi Log sự kiện
            try:
                event_sheet = sh.worksheet("ACCESS_EVENTS")
                event_id = hashlib.md5(f"{target_uid}{time.time()}".encode()).hexdigest()[:8]
                event_sheet.append_row([event_id, target_uid, "GRANT", ADMIN_ID, time.strftime("%Y-%m-%d %H:%M:%S"), "OK", "{}", "LOCKED", "SUCCESS", "Done"])
            except:
                line_bot_api.reply_message(event.reply_token, TextSendMessage(text="❌ Lỗi: Thiếu tab ACCESS_EVENTS"))
                return

            # Cập nhật quyền
            user_sheet = sh.worksheet("USER_LANG_MAP")
            try:
                cell = user_sheet.find(target_uid)
                user_sheet.update_cell(cell.row, 4, "TRUE")
                res_msg = f"✅ Đã cấp Premium cho: {target_uid}"
            except:
                user_sheet.append_row([target_uid, "vi", time.strftime("%Y-%m-%d %H:%M:%S"), "TRUE", "0", "WORKER", "Auto"])
                res_msg = f"✅ Đã tạo & cấp Premium: {target_uid}"

            line_bot_api.reply_message(event.reply_token, TextSendMessage(text=res_msg))
        except Exception as e:
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text=f"❌ Lỗi Sheet: {str(e)}"))

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))
