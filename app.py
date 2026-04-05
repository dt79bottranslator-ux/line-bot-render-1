import os
import requests
from flask import Flask, request, jsonify

app = Flask(__name__)

LINE_CHANNEL_ACCESS_TOKEN = os.getenv("LINE_CHANNEL_ACCESS_TOKEN", "")
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY", "")
PORT = int(os.getenv("PORT", "10000"))


# ========================
# ROOT
# ========================
@app.route("/", methods=["GET"])
def root():
    return "BOT RUNNING", 200


# ========================
# HEALTH
# ========================
@app.route("/health", methods=["GET"])
def health():
    return jsonify({
        "status": "ok",
        "line_token_exists": bool(LINE_CHANNEL_ACCESS_TOKEN),
        "google_api_key_exists": bool(GOOGLE_API_KEY)
    })


# ========================
# TRANSLATE FUNCTION
# ========================
def translate(text, target):
    url = "https://translation.googleapis.com/language/translate/v2"

    params = {
        "q": text,
        "target": target,
        "key": GOOGLE_API_KEY
    }

    res = requests.post(url, data=params)

    if res.status_code != 200:
        return f"[ERROR TRANSLATE] {res.text}"

    try:
        return res.json()["data"]["translations"][0]["translatedText"]
    except:
        return "[TRANSLATE ERROR]"


# ========================
# WEBHOOK
# ========================
@app.route("/webhook", methods=["POST"])
def webhook():
    data = request.get_json(silent=True)

    if not data:
        return "NO DATA", 400

    for event in data.get("events", []):
        if event.get("type") != "message":
            continue

        msg = event.get("message", {})
        if msg.get("type") != "text":
            continue

        reply_token = event.get("replyToken")
        text = msg.get("text", "")

        if not reply_token:
            continue

        # ========================
        # LOGIC DỊCH
        # ========================
        if text.startswith("/zh "):
            result = translate(text[4:], "zh-TW")

        elif text.startswith("/vi "):
            result = translate(text[4:], "vi")

        elif text.startswith("/id "):
            result = translate(text[4:], "id")

        elif text.startswith("/en "):
            result = translate(text[4:], "en")

        else:
            result = "Dùng:\n/zh nội dung\n/vi nội dung\n/id nội dung\n/en nội dung"

        reply_text(reply_token, result)

    return "OK"


# ========================
# REPLY
# ========================
def reply_text(reply_token, text):
    url = "https://api.line.me/v2/bot/message/reply"

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}"
    }

    body = {
        "replyToken": reply_token,
        "messages": [
            {"type": "text", "text": str(text)[:5000]}
        ]
    }

    requests.post(url, headers=headers, json=body)


# ========================
# RUN
# ========================
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT)
