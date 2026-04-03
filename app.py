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
APP_VERSION = "DT79_V14_HARDENED"

LINE_ACCESS_TOKEN = (os.getenv("LINE_CHANNEL_ACCESS_TOKEN") or "").strip()
LINE_SECRET = (os.getenv("LINE_CHANNEL_SECRET") or "").strip()
SHEET_ID = (os.getenv("GOOGLE_SHEET_ID") or "").strip()
GOOGLE_JSON = (os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON") or "").strip()
SHEET_NAME = "USER_LANG_MAP"

# =========================
# NORMALIZE
# =========================
def normalize_id(val) -> str:
    s = str(val or "")
    return (
        s.replace("\u200b", "")
         .replace("\ufeff", "")
         .replace(" ", "")
         .replace("\n", "")
         .replace("\r", "")
         .replace("\t", "")
         .strip()
    )

# =========================
# VALIDATE UID LINE
# =========================
def is_valid_line_uid(uid: str) -> bool:
    return uid.startswith("U") and len(uid) == 33 and uid.isalnum()

ADMIN_ID = normalize_id("U83c6ce008a35ef17edaff25ac003370")

configuration = Configuration(access_token=LINE_ACCESS_TOKEN)
handler = WebhookHandler(LINE_SECRET)

# =========================
# SHEET
# =========================
def get_ws():
    creds = ServiceAccountCredentials.from_json_keyfile_dict(
        json.loads(GOOGLE_JSON),
        ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"],
    )
    return gspread.authorize(creds).open_by_key(SHEET_ID).worksheet(SHEET_NAME)

def reply_msg(token, text):
    with ApiClient(configuration) as api_client:
        MessagingApi(api_client).reply_message(
            ReplyMessageRequest(reply_token=token, messages=[V3TextMessage(text=text)])
        )

# =========================
# ROUTE
# =========================
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

# =========================
# MAIN HANDLER
# =========================
@handler.add(MessageEvent, message=TextMessageContent)
def handle_text(event):

    uid = normalize_id(event.source.user_id)
    token = event.reply_token
    raw_text = event.message.text or ""
    msg_text = raw_text.strip()
    cmd = msg_text.lower()

    print(f"[AUTH] uid={repr(uid)} admin={repr(ADMIN_ID)} match={uid == ADMIN_ID}")

    if cmd == "/me":
        reply_msg(token, f"UID:\n{uid}")
        return

    is_admin = (uid == ADMIN_ID)

    if cmd.startswith("/grant"):

        if not is_admin:
            reply_msg(token, "❌ Bạn không có quyền Admin")
            return

        parts = msg_text.split()
        if len(parts) < 2:
            reply_msg(token, "Gõ: /grant USER_ID")
            return

        target = normalize_id(parts[1])

        # VALIDATE UID
        if not is_valid_line_uid(target):
            reply_msg(token, f"❌ UID không hợp lệ:\n{target}")
            return

        ws = get_ws()

        try:
            now = datetime.now(timezone.utc).isoformat()

            # =========================
            # READ COLUMN UID (A)
            # =========================
            uids = ws.col_values(1)

            match_rows = [
                i for i, v in enumerate(uids, start=1)
                if normalize_id(v) == target
            ]

            # =========================
            # HANDLE CASES
            # =========================

            if len(match_rows) > 1:
                reply_msg(token, "❌ UID bị trùng nhiều dòng → cần xử lý tay")
                return

            elif len(match_rows) == 1:
                row = match_rows[0]
                ws.update_cell(row, 4, "TRUE")
                ws.update_cell(row, 3, now)

                reply_msg(token, f"✅ UPDATED:\n{target}")
                return

            else:
                ws.append_row([target, "en", now, "TRUE", "0", "USER", "user"])
                reply_msg(token, f"✅ CREATED:\n{target}")
                return

        except Exception as e:
            reply_msg(token, f"❌ Lỗi: {str(e)}")
            return
