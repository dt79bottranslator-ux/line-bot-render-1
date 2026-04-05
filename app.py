import os
from flask import Flask, request, jsonify
import requests

app = Flask(__name__)

LINE_CHANNEL_ACCESS_TOKEN = os.getenv("LINE_CHANNEL_ACCESS_TOKEN")
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")

# ========================
# HEALTH CHECK
# ========================
@app.route("/health", methods=["GET"])
def health():
    return jsonify({
        "status": "ok",
        "line_token_exists": bool(LINE_CHANNEL_ACCESS_TOKEN),
        "google_api_key_exists": bool(GOOGLE_API_KEY)
    })


# ========================
# WEBHOOK
# ========================
@app.route("/webhook", methods=["POST"])
def webhook():
    data = request.get_json()

    for event in data.get("events", []):
        if event["type"] == "message":
            reply_token = event["replyToken"]
            text = event["message"]["text"]

            reply(reply_token, f"Bạn gửi: {text}")

    return "OK"


# ========================
# REPLY FUNCTION
# ========================
def reply(reply_token, text):
    url = "https://api.line.me/v2/bot/message/reply"

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}"
    }

    body = {
        "replyToken": reply_token,
        "messages": [
            {"type": "text", "text": text}
        ]
    }

    requests.post(url, headers=headers, json=body)


# ========================
# RUN
# ========================
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
