from flask import Flask

app = Flask(__name__)

@app.route("/", methods=["GET"])
def home():
    return "DT79_ROOT_OK_V1", 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
