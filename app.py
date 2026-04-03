import os
import re
from flask import Flask, request, abort
from linebot import LineBotApi, WebhookHandler
from linebot.exceptions import InvalidSignatureError
from linebot.models import MessageEvent, TextMessage, TextSendMessage

app = Flask(__name__)

# Cấu hình từ Render Environment Variables
line_bot_api = LineBotApi(os.environ.get('LINE_CHANNEL_ACCESS_TOKEN'))
handler = WebhookHandler(os.environ.get('LINE_CHANNEL_SECRET'))
ADMIN_ID = os.environ.get('ADMIN_ID', 'U83c6ce008a35ef17edaff25ac003370')

@app.route("/")
def home():
    return "DT79_V9_FINAL_LOCK"

@app.route("/callback", methods=['POST'])
def callback():
    signature = request.headers['X-Line-Signature']
    body = request.get_data(as_text=True)
    try:
        handler.handle(body, signature)
    except InvalidSignatureError:
        abort(400)
    return 'OK'

@handler.add(MessageEvent, message=TextMessage)
def handle_message(event):
    text = event.message.text.strip()
    user_id = event.source.user_id

    # Lọc ID bằng Regex để tránh ký tự ẩn khi copy từ điện thoại
    if text.startswith('/grant'):
        match = re.search(r'U[0-9a-f]{32}', text)
        if match:
            target_id = match.group(0)
            if target_id == ADMIN_ID:
                line_bot_api.reply_message(event.reply_token, TextSendMessage(text="✅ XÁC NHẬN ADMIN: HỆ THỐNG ĐÃ MỞ KHÓA."))
                return
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text="❌ LỖI: ID KHÔNG HỢP LỆ HOẶC THIẾU QUYỀN."))

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
