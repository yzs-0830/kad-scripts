#!/usr/bin/python3

import sys
import os
import requests
import msgpackrpc
import hashlib

def new_client(ip, port):
    return msgpackrpc.Client(msgpackrpc.Address(ip, port))

def sha1_str(s: str):
    return hashlib.sha1(s.encode()).hexdigest()

if len(sys.argv) < 3:
    print("Usage: migrate_upload.py <filepath> <target_public_ip>")
    sys.exit(1)

filepath = sys.argv[1]
target_ip = sys.argv[2]   # Public IP

filename = os.path.basename(filepath)

client = new_client(target_ip, 5057)

h = sha1_str(filename)
print(f"[migrate_upload] Hash of {filename} = {h}")

node = client.call("find_node", h)
node_ip = node.get("ip")  # must be public IP

if node_ip is not None:
    node_ip = node_ip.decode()  # decode 成字串

print(f"[migrate_upload] Uploading to http://{node_ip}:5058/upload")

with open(filepath, "rb") as f:
    files = {"files": f}
    response = requests.post(
        f"http://{node_ip}:5058/upload",
        files=files
    )

print("[migrate_upload] Upload status:", response.status_code)
