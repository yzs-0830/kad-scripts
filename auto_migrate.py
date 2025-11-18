#!/usr/bin/python3

import os
import time
import subprocess
import msgpackrpc
import hashlib
import requests

FILES_DIR = "/home/ec2-user/files"
UPLOAD_SCRIPT = "/home/ec2-user/autoscript/migrate_upload.py"
KAD_PORT = 5057   # 你的 Kademlia RPC port

def get_public_ip():
    return requests.get(
        "http://169.254.169.254/latest/meta-data/public-ipv4",
        timeout=2
    ).text

def new_client(ip, port):
    return msgpackrpc.Client(msgpackrpc.Address(ip, port))

def sha1_filename(name: str):
    return hashlib.sha1(name.encode()).hexdigest()

public_ip = get_public_ip()
print(f"[auto_migrate] Public IP = {public_ip}")

# 自己也用公網呼叫
client = new_client(public_ip, KAD_PORT)

while True:
    try:
        files = os.listdir(FILES_DIR)

        for filename in files:
            fullpath = os.path.join(FILES_DIR, filename)
            if not os.path.isfile(fullpath):
                continue

            h = sha1_filename(filename)
            print(f"[auto_migrate] Checking {filename}, sha1={h}")

            # Kademlia find_node
            node_info = client.call("find_node", h)
            target_ip = node_info.get("ip")  # assume public IP

            if target_ip != public_ip:
                print(f"[auto_migrate] {filename} should move to {target_ip}")

                subprocess.call([
                    "/usr/bin/python3",
                    UPLOAD_SCRIPT,
                    fullpath,
                    target_ip
                ])
            else:
                print(f"[auto_migrate] {filename} is already on the correct node")

    except Exception as e:
        print("[auto_migrate] Error:", e)

    time.sleep(10)
