from flask import Flask, request, jsonify
import os

app = Flask(__name__)

BASE_DIR = "/home/ec2-user/files"

@app.get("/has_file")
def has_file():
    filename = request.args.get("name")
    if not filename:
        return jsonify({"error": "missing name parameter"}), 400

    full_path = os.path.join(BASE_DIR, filename)
    exists = os.path.exists(full_path)

    return jsonify({"exists": exists})

@app.get("/list_files")
def list_files():
    try:
        files = os.listdir(BASE_DIR)
        return jsonify({"files": files})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5059)
