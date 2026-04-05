import os
import requests
from flask import Flask, request, jsonify

app = Flask(__name__)

LINE_CHANNEL_ACCESS_TOKEN = os.getenv("LINE_CHANNEL_ACCESS_TOKEN", "").strip()
LINE_CHANNEL_SECRET = os.getenv("LINE_CHANNEL_SECRET", "").strip()
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY", "").strip()
PORT = int(os.getenv("PORT", "10000"))


# ========================
# ROOT
# ========================
@app.route("/", methods=["GET"])
def root():
    return "BOT RUNNING", 200


# ========================
# HEALTH CHECK
# ========================
@app.route("/health", methods=["GET"])
def health():
    return jsonify({
        "status": "ok",
        "line_token_exists": bool(LINE_CHANNEL_ACCESS_TOKEN),
        "line_secret_exists": bool(LINE_CHANNEL_SECRET),
        "google_api_key_exists": bool(GOOGLE_API_KEY)
    }), 200


# ========================
# WEBHOOK
# ========================
@app.route("/webhook", methods=["POST"])
def webhook():
    data = request.get_json(silent=True)

    if not data:
        return jsonify({
            "ok": False,
            "error": "missing_or_invalid_json"
        }), 400

    events = data.get("events", [])

    for event in events:
        if event.get("type") != "message":
            continue

        message = event.get("message", {})
        if message.get("type") != "text":
            continue

        reply_token = event.get("replyToken")
        user_text = message.get("text", "").strip()

        if not reply_token:
            continue

        if not user_text:
            reply_text(reply_token, "Tin nhắn trống.")
            continue

        reply_text(reply_token, f"Bạn gửi: {user_text}")

    return "OK", 200


# ========================
# REPLY FUNCTION
# ========================
def reply_text(reply_token, text):
    if not LINE_CHANNEL_ACCESS_TOKEN:
        print("[ERROR] Missing LINE_CHANNEL_ACCESS_TOKEN")
        return

    url = "https://api.line.me/v2/bot/message/reply"

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}"
    }

    payload = {
        "replyToken": reply_token,
        "messages": [
            {
                "type": "text",
                "text": str(text)[:5000]
            }
        ]
    }

    try:
        response = requests.post(url, headers=headers, json=payload, timeout=15)
        print(f"[LINE REPLY] status={response.status_code} body={response.text}")
    except Exception as e:
        print(f"[LINE REPLY ERROR] {e}")


# ========================
# RUN
# ========================
if __name__ == "__main__":
    print("[BOOT] Flask app starting...")
    print(f"[BOOT] PORT={PORT}")
    print(f"[BOOT] line_token_exists={bool(LINE_CHANNEL_ACCESS_TOKEN)}")
    print(f"[BOOT] line_secret_exists={bool(LINE_CHANNEL_SECRET)}")
    print(f"[BOOT] google_api_key_exists={bool(GOOGLE_API_KEY)}")

    app.run(host="0.0.0.0", port=PORT)
