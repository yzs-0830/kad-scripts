#!/usr/bin/python3

import os
import time
import subprocess
import msgpackrpc
import hashlib
import requests
import sys
import json
import re # Add regex for filename normalization

FILES_DIR = "/home/ec2-user/files"
UPLOAD_SCRIPT = "/home/ec2-user/autoscript/migrate_upload.py"
UPLOAD_RECORD = "/home/ec2-user/autoscript/upload_record.json"
KAD_PORT = 5057


def load_record():
    if not os.path.exists(UPLOAD_RECORD):
        return {}
    try:
        with open(UPLOAD_RECORD, "r") as f:
            return json.load(f)
    except:
        return {}

def save_record(record):
    with open(UPLOAD_RECORD, "w") as f:
        # Use normalized filename as record key to ensure consistency
        json.dump(record, f)



def get_public_ip():
    try:
        public_ip = requests.get(
            "http://169.254.169.254/latest/meta-data/public-ipv4",
            timeout=1
        ).text
        if public_ip and len(public_ip.split('.')) == 4:
            return public_ip
    except:
        pass
        
    return "127.0.0.1"


def new_client(ip, port):
    return msgpackrpc.Client(msgpackrpc.Address(ip, port))


def sha1_filename(name: str):
    return hashlib.sha1(name.encode()).hexdigest()

# New: Function to remove OS-added suffixes like (1), (2)
def normalize_filename(name: str) -> str:
    name_without_ext, ext = os.path.splitext(name)
    
    # regex: match one or more spaces followed by (digits) at the end of the filename
    cleaned_name = re.sub(r'(\s\(\d+\))+(\s*)$', '', name_without_ext).strip()
    
    if not cleaned_name:
        return name
        
    return cleaned_name + ext


public_ip = get_public_ip()
print(f"[auto_migrate] Instance Public/Private IP = {public_ip}")

kad_client = new_client("127.0.0.1", KAD_PORT) 
print(f"[auto_migrate] Kademlia Client connected to 127.0.0.1:{KAD_PORT}")

# create record file
if not os.path.exists(UPLOAD_RECORD):
    with open(UPLOAD_RECORD, "w") as f:
        f.write("{}")
    print(f"[auto_migrate] Created upload record at {UPLOAD_RECORD}")

record = load_record()


while True:
    try:
        files = os.listdir(FILES_DIR)

        for filename in files:
            fullpath = os.path.join(FILES_DIR, filename)
            if not os.path.isfile(fullpath):
                continue
            
            # Key fix: use normalized filename for hashing and lookups
            normalized_filename = normalize_filename(filename)
            h = sha1_filename(normalized_filename)
            
            print(f"---")
            print(f"[auto_migrate] Checking {filename} (Normalized: {normalized_filename}), sha1={h}")

            node_info = kad_client.call("find_node", h)
            target_ip = node_info[b"ip"].decode()

            if not target_ip:
                print(f"[auto_migrate] Warning: find_node returned no target IP.")
                continue

            # initial file record
            # Use normalized name as key in the record file
            if normalized_filename not in record:
                record[normalized_filename] = []

            # Primary Check
            if target_ip != public_ip:
                print(f"[auto_migrate] {filename} should move to {target_ip}")

                # Check record using normalized name
                if target_ip not in record[normalized_filename]:
                    
                    # Check remote existence using normalized name
                    check = requests.get(f"http://{target_ip}:5059/has_file", params={"name": normalized_filename}).json()

                    # prevent upload exist file
                    if check["exists"]:
                        print("[auto_migrate] Node already has file (using normalized name), skip upload")
                    else:
                        print("[auto_migrate] Node does not have file, uploading...")
                        # Pass normalized filename as the 3rd argument (sys.argv[3])
                        subprocess.call([
                            "/usr/bin/python3",
                            UPLOAD_SCRIPT,
                            fullpath,
                            target_ip,
                            normalized_filename # Pass normalized name for consistent Metadata Key
                        ])
                    
                    record[normalized_filename].append(target_ip)
                    save_record(record)
                else:
                    print(f"[auto_migrate] Already replicated to {target_ip}, skip.")

                # Replica Check
                client_r = new_client(target_ip, KAD_PORT)
                replica_info = client_r.call("send_closest", h)

                count = 0
                for rnode in replica_info:
                    # Look for 2 replicas (K=2)
                    if count == 2:
                        break

                    r_ip = rnode[b"ip"].decode()

                    # Skip self and the Primary node
                    if r_ip == public_ip or r_ip == target_ip:
                        continue

                    # Check record using normalized name
                    if r_ip not in record[normalized_filename]:
                        
                        # Check remote existence using normalized name
                        check = requests.get(f"http://{r_ip}:5059/has_file", params={"name": normalized_filename}).json()

                        # prevent upload exist file
                        if check["exists"]:
                            print("[auto_migrate] Node already has replica file (using normalized name), skip upload")
                        else:
                            print("[auto_migrate] Node does not have replica file, uploading...")
                            # Pass normalized filename as the 3rd argument (sys.argv[3])
                            subprocess.call([
                                "/usr/bin/python3",
                                UPLOAD_SCRIPT,
                                fullpath,
                                r_ip,
                                normalized_filename # Pass normalized name for consistent Metadata Key
                            ])
                        record[normalized_filename].append(r_ip)
                        save_record(record)
                    else:
                        print(f"[auto_migrate] Replica already exists on {r_ip}, skip.")

                    count += 1

            else:
                print(f"[auto_migrate] {filename} is already on correct node ({public_ip})")

    except Exception as e:
        print(f"[auto_migrate] Global Error: {e}")
        print(f"[auto_migrate] Exception Traceback:", file=sys.stderr)

    time.sleep(10)
