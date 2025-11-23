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

filepath = sys.argv[1]
target_ip = sys.argv[2]   # Public IP

# --- Filename Determination and Normalization ---

# Get the original filename from the path
original_filename = os.path.basename(filepath)

# 🚨 Primary fix: Use sys.argv[3] (normalized name) if provided
if len(sys.argv) > 3:
    # Use the normalized filename passed from auto_migrate.py
    metadata_key_name = sys.argv[3] 
    # Optional cleanup for safety, although the name should be clean already
    metadata_key_name = metadata_key_name.strip("'\"")
    print(f"[migrate_upload] Using normalized name: {metadata_key_name} for Metadata Key.")
else:
    # Fallback to the original filename and strip any surrounding quotes/double quotes
    metadata_key_name = original_filename.strip("'\"")
    print(f"[migrate_upload] Using original name: {metadata_key_name} for Metadata Key.")

# The name used to calculate the Kademlia Key (h) and store the Metadata
filename_for_key = metadata_key_name 

# --- Kademlia Lookup ---

client = new_client(target_ip, 5057)

# Calculate Key (h) based on the consistent filename
h = sha1_str(filename_for_key)
print(f"[migrate_upload] Hash of {filename_for_key} = {h}")

# Find the node responsible for this Key (Metadata location)
node = client.call("find_node", h)
node_ip = node[b"ip"].decode()  # must be public IP


print(f"[migrate_upload] Uploading to http://{node_ip}:5058/upload")

# --- File Upload ---

with open(filepath, "rb") as f:
    files = {"file": (filename_for_key, f)} 
    
    response = requests.post(
        f"http://{node_ip}:5058/upload",
        files=files
    )

print("[migrate_upload] Upload status:", response.status_code)