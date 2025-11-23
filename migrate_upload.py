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
    print("Usage: migrate_upload.py <filepath> <target_public_ip> [normalized_filename]")
    sys.exit(1)

filepath = sys.argv[1] # Path to the original file (e.g., /files/dolphin (1).jpg)
target_ip = sys.argv[2]   # Public IP

if len(sys.argv) > 3:
    # Use the normalized filename passed from auto_migrate.py (sys.argv[3])
    filename_for_key = sys.argv[3].strip("'\"") 
    print(f"[migrate_upload] Using normalized name: {filename_for_key} for Metadata Key.")
else:
    # Fallback to the original filename from the path (if called manually)
    filename_for_key = os.path.basename(filepath).strip("'\"")
    print(f"[migrate_upload] Using original name: {filename_for_key} for Metadata Key.")

# --- Kademlia Lookup ---

client = new_client(target_ip, 5057)

# Calculate Key (h) based on the consistent filename (filename_for_key)
h = sha1_str(filename_for_key)
print(f"[migrate_upload] Hash of {filename_for_key} = {h}")

# Find the node responsible for this Key (Metadata location)
node = client.call("find_node", h)
node_ip = node[b"ip"].decode()  # must be public IP


print(f"[migrate_upload] Uploading to http://{node_ip}:5058/upload")
print(f"[migrate_upload] Storing Metadata as: {filename_for_key}") # Log the name used for storage

# --- File Upload ---

with open(filepath, "rb") as f:
    # 🚨 Fix: Explicitly set the filename used in the POST request to the filename_for_key.
    # This ensures the server stores the file (Metadata) with the consistent name.
    files = {"file": (filename_for_key, f)} 
    
    response = requests.post(
        f"http://{node_ip}:5058/upload",
        files=files
    )

print("[migrate_upload] Upload status:", response.status_code)