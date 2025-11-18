#!/usr/bin/python3

import os
import time
import subprocess
import msgpackrpc
import hashlib
import requests
import sys

# --- 設定 ---
FILES_DIR = "/home/ec2-user/files"
UPLOAD_SCRIPT = "/home/ec2-user/autoscript/migrate_upload.py"
KAD_PORT = 5057   # 你的 Kademlia RPC port

# --- 函式定義 ---

def get_public_ip():
    """嘗試獲取 Public IP，若失敗則回傳 Private IP 或 127.0.0.1"""
    try:
        # 嘗試獲取 Public IP
        public_ip = requests.get(
            "http://169.254.169.254/latest/meta-data/public-ipv4",
            timeout=1
        ).text
        if public_ip and len(public_ip.split('.')) == 4:
            return public_ip
    except requests.exceptions.RequestException:
        pass # 忽略錯誤，繼續嘗試 Private IP

    try:
        # 嘗試獲取 Private IP
        private_ip = requests.get(
            "http://169.254.169.254/latest/meta-data/local-ipv4",
            timeout=1
        ).text
        if private_ip and len(private_ip.split('.')) == 4:
            return private_ip
    except requests.exceptions.RequestException:
        pass # 忽略錯誤
        
    # 如果所有嘗試都失敗，返回 localhost
    return "127.0.0.1"


def new_client(ip, port):
    return msgpackrpc.Client(msgpackrpc.Address(ip, port))


def sha1_filename(name: str):
    return hashlib.sha1(name.encode()).hexdigest()


# --- 主程式 ---

public_ip = get_public_ip()
print(f"[auto_migrate] Instance Public/Private IP = {public_ip}")

# 關鍵修正: 在 EC2 實例內部，使用 127.0.0.1 連線到本地 Kademlia 服務
# 避免使用 Public IP 導致的 'Name or service not known' 錯誤
kad_client = new_client("127.0.0.1", KAD_PORT) 
print(f"[auto_migrate] Kademlia Client connected to 127.0.0.1:{KAD_PORT}")


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

            # Kademlia find_node
            # 使用本地 client 呼叫 find_node，查找誰是這個 key 最近的節點
            node_info = kad_client.call("find_node", h)
            
            # 假設 Kademlia 協定回傳的 node_info 是一個 dict，包含 'ip' 鍵
            target_ip = node_info[b"ip"].decode()

            if not target_ip:
                print(f"[auto_migrate] Warning: find_node returned no target IP for {filename}.")
                continue
                
            # 比較: 如果目標 IP 不是自己的 Public IP (或 Private IP)
            if target_ip != public_ip:
                print(f"[auto_migrate] {filename} should move to {target_ip}")
                
                # 執行 migrate_upload.py 將檔案移動到正確的 IP
                # migrate_upload.py (Client) 必須使用 target_ip:5058 (HTTP Upload Port)
                subprocess.call([
                    "/usr/bin/python3",
                    UPLOAD_SCRIPT,
                    fullpath,
                    target_ip
                ])
                
                # 假設 migrate_upload 成功，刪除本地檔案
                # os.remove(fullpath) 
                # 注意: 刪除前需確認 migrate_upload.py 確實成功，否則會丟失數據！
                # 為了安全，這裡暫時註解刪除
                
            else:
                print(f"[auto_migrate] {filename} is already on the correct node ({public_ip})")

    except Exception as e:
        print(f"[auto_migrate] Global Error: {e}")
        # 在遇到錯誤時，將錯誤輸出到標準錯誤流 (stderr)
        # 這樣更容易在日誌中被捕獲
        print(f"[auto_migrate] Exception Traceback:", file=sys.stderr)
        
    time.sleep(10)