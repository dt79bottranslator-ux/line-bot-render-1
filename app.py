import os
import json
from datetime import datetime, timezone
import gspread
from flask import Flask, request, abort
from oauth2client.service_account import ServiceAccountCredentials
from linebot.v3.messaging import Configuration, ApiClient, MessagingApi, ReplyMessageRequest, TextMessage as V3TextMessage
from linebot.v3.webhook import WebhookHandler
from linebot.v3.exceptions import InvalidSignatureError
from linebot.v3.webhooks import MessageEvent, TextMessageContent

app = Flask(__name__)
# Nâng cấp Version để hậu kiểm trên trình duyệt
APP_VERSION = "DT79_V11_ADMIN_FIXED"

LINE_ACCESS_TOKEN = (os.getenv("LINE_CHANNEL_ACCESS_TOKEN") or "").strip()
LINE_SECRET = (os.getenv("LINE_CHANNEL_SECRET") or "").strip()
SHEET_ID = (os.getenv("GOOGLE_SHEET_ID") or "").strip()
GOOGLE_JSON = (os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON") or "").strip()
SHEET_NAME = "USER_LANG_MAP"

# ID ADMIN CỦA ANH DŨNG - ĐÃ ÉP KIỂU CHUỖI VÀ LÀM SẠCH
ADMIN_ID = "U83c6ce008a35ef17edaff25ac003370"

configuration = Configuration(access_token=LINE_ACCESS_TOKEN)
handler = WebhookHandler(LINE_SECRET)

def get_ws():
    try:
        creds = ServiceAccountCredentials.from_json_keyfile_dict(
            json.loads(GOOGLE_JSON), 
            ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        )
        return gspread.authorize(creds).open_by_key(SHEET_ID).worksheet(SHEET_NAME)
    except Exception as e:
        print(f"Sheet Error: {e}")
        return None

def reply_msg(token, text):
    with ApiClient(configuration) as api_client:
        MessagingApi(api_client).reply_message(
            ReplyMessageRequest(reply_token=token, messages=[V3TextMessage(text=text)])
        )

@app.route("/", methods=["GET"])
def home(): 
    return f"{APP_VERSION} LIVE", 200

@app.route("/webhook", methods=["POST"])
def callback():
    sig = request.headers.get("X-Line-Signature", "")
    body = request.get_data(as_text=True)
    try: 
        handler.handle(body, sig)
    except InvalidSignatureError: 
        abort(400)
    return "OK"

@handler.add(MessageEvent, message=TextMessageContent)
def handle_text(event):
    # 1. LÀM SẠCH TUYỆT ĐỐI ID NGƯỜI GỬI
    uid = str(event.source.user_id).strip()
    token = event.reply_token
    msg_text = (event.message.text or "").strip()

    # Lệnh kiểm tra ID nhanh
    if msg_text.lower() == "/me":
        reply_msg(token, f"ID của bạn:\n{uid}")
        return

    # 2. KIỂM TRA QUYỀN ADMIN (VÁ LỖI SO KHỚP TUYỆT ĐỐI)
    # Ép cả hai về dạng chuỗi và gọt sạch mọi khoảng trắng tàng hình ở cả 2 đầu
    is_admin = (uid == str(ADMIN_ID).strip())

    if msg_text.startswith("/grant"):
        if not is_admin:
            # Thông báo kèm ký hiệu | | để anh thấy nếu có dấu cách thừa
            reply_msg(token, f"❌ Từ chối! ID không khớp Admin.\nID thực tế của bạn là:\n|{uid}|")
            return
        
        parts = msg_text.split()
        if len(parts) < 2:
            reply_msg(token, "Gõ đúng cú pháp: /grant USER_ID")
            return
            
        target = parts[1].strip()
        ws = get_ws()
        if not ws:
            reply_msg(token, "Lỗi kết nối Google Sheet")
            return

        try:
            now = datetime.now(timezone.utc).isoformat()
            cells = ws.findall(target)
            if cells:
                for c in cells:
                    ws.update_cell(c.row, 4, "TRUE")
                msg = f"✅ Đã nâng cấp PREMIUM cho:\n{target}"
            else:
                ws.append_row([target, "en", now, "TRUE", "0", "USER", "user"])
                msg = f"✅ Đã tạo mới PREMIUM cho:\n{target}"
            reply_msg(token, msg)
        except Exception as e:
            reply_msg(token, f"Lỗi xử lý Sheet: {str(e)}")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 10000)))
