#!/usr/bin/python3

import os
import time
import subprocess
import msgpackrpc
import hashlib
import requests
import sys
import json

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

            h = sha1_filename(filename)
            print(f"---")
            print(f"[auto_migrate] Checking {filename}, sha1={h}")

            node_info = kad_client.call("find_node", h)
            target_ip = node_info[b"ip"].decode()

            if not target_ip:
                print(f"[auto_migrate] Warning: find_node returned no target IP.")
                continue

            # initial file record
            if filename not in record:
                record[filename] = []

            # Primary Check
            if target_ip != public_ip:
                print(f"[auto_migrate] {filename} should move to {target_ip}")

                if target_ip not in record[filename]:
                    check = requests.get(f"http://{target_ip}:5059/has_file", params={"name": filename}).json()

                    # prevent upload exist file
                    if check["exists"]:
                        print("[auto_migrate] Node already has file, skip upload")
                    else:
                        print("[auto_migrate] Node does not have file, uploading...")
                        subprocess.call([
                            "/usr/bin/python3",
                            UPLOAD_SCRIPT,
                            fullpath,
                            target_ip
                        ])
                    record[filename].append(target_ip)
                    save_record(record)
                else:
                    print(f"[auto_migrate] Already replicated to {target_ip}, skip.")

                # Replica Check
                client_r = new_client(target_ip, KAD_PORT)
                replica_info = client_r.call("send_closest", h)

                count = 0
                for rnode in replica_info:
                    if count == 2:
                        break

                    r_ip = rnode[b"ip"].decode()

                    if r_ip == public_ip:
                        continue

                    if r_ip not in record[filename]:
                        check = requests.get(f"http://{r_ip}:5059/has_file", params={"name": filename}).json()

                        # prevent upload exist file
                        if check["exists"]:
                            print("[auto_migrate] Node already has replica file, skip upload")
                        else:
                            print("[auto_migrate] Node does not have replica file, uploading...")
                            subprocess.call([
                                "/usr/bin/python3",
                                UPLOAD_SCRIPT,
                                fullpath,
                                r_ip
                            ])
                        record[filename].append(r_ip)
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
