import os, json, time, hashlib, re
from flask import Flask, request, abort
from linebot import LineBotApi, WebhookHandler
from linebot.exceptions import InvalidSignatureError
from linebot.models import MessageEvent, TextMessage, TextSendMessage
import gspread
from google.oauth2.service_account import Credentials

app = Flask(__name__)
line_bot_api = LineBotApi(os.getenv('LINE_CHANNEL_ACCESS_TOKEN'))
handler = WebhookHandler(os.getenv('LINE_CHANNEL_SECRET'))

# --- [TRUY CỐT] MÃ GEN ADMIN KHÔNG THỂ THAY ĐỔI ---
ADMIN_DNA = "U83c6ce008a35ef17edaff25ac003370"

def get_sheet():
    creds_json = json.loads(os.getenv('GOOGLE_SERVICE_ACCOUNT_JSON'))
    creds = Credentials.from_service_account_info(creds_json, scopes=["https://www.googleapis.com/auth/spreadsheets"])
    return gspread.authorize(creds).open_by_key(os.getenv('GOOGLE_SHEET_ID'))

@app.route("/callback", methods=['POST'])
def callback():
    signature = request.headers.get('X-Line-Signature')
    body = request.get_data(as_text=True)
    try: handler.handle(body, signature)
    except InvalidSignatureError: abort(400)
    return 'OK'

@handler.add(MessageEvent, message=TextMessage)
def handle_message(event):
    # Lấy ID nguyên bản và gọt sạch mọi ký tự không phải chữ/số (Triệt hạ dấu ngắt dòng)
    raw_uid = str(event.source.user_id)
    clean_uid = "".join(re.findall(r'[a-zA-Z0-9]', raw_uid))
    msg = event.message.text.strip()

    if msg == "/me":
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text=f"ID SẠCH:\n{clean_uid}"))
        return

    if msg.startswith("/grant"):
        # SO SÁNH DNA SAU KHI ĐÃ CƯỠNG CHẾ LÀM SẠCH
        if clean_uid != ADMIN_ID_DNA: # Nếu DNA trong code chưa sạch, hàm dưới sẽ quét
            if ADMIN_DNA not in clean_uid:
                line_bot_api.reply_message(event.reply_token, TextSendMessage(
                    text=f"❌ TỪ CHỐI!\nDNA chuẩn: {ADMIN_DNA}\nID của bạn: {clean_uid}"
                ))
                return
        
        target_uid = msg.split()[-1].strip()
        try:
            sh = get_sheet()
            # Ghi Log Event - Tab ACCESS_EVENTS
            sh.worksheet("ACCESS_EVENTS").append_row([
                hashlib.md5(f"{target_uid}{time.time()}".encode()).hexdigest()[:8],
                target_uid, "GRANT", "ADMIN_FORCE", time.strftime("%Y-%m-%d %H:%M:%S"), "SUCCESS"
            ])
            # Cập nhật Premium - Tab USER_LANG_MAP
            u_sheet = sh.worksheet("USER_LANG_MAP")
            try:
                cell = u_sheet.find(target_uid)
                u_sheet.update_cell(cell.row, 4, "TRUE")
            except:
                u_sheet.append_row([target_uid, "vi", time.strftime("%Y-%m-%d %H:%M:%S"), "TRUE", "0", "ADMIN"])
            
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text=f"✅ THỰC THI THÀNH CÔNG!"))
        except Exception as e:
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text=f"❌ LỖI HỆ THỐNG: {str(e)}"))

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))
