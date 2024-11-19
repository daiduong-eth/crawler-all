import requests
import json
from time import sleep
import os

address_balances = {}

# Nạp số dư từ file nếu tồn tại
def load_balances_from_file(file_path):
    if os.path.exists(file_path):
        with open(file_path, "r") as f:
            for line in f:
                if line.startswith("last_processed_block"):
                    last_processed_block = int(line.strip().split(": ")[1])
                    address_balances["last_processed_block"] = last_processed_block
                else:
                    address, balance = line.strip().split(": ")
                    balance = float(balance)
                    address_balances[address] = int(balance)
    else:
        print(f"Không tìm thấy file {file_path}, bắt đầu từ block đầu tiên")

# Nạp dữ liệu từ file `last_processed_block.txt`
load_balances_from_file("last_processed_block.txt")

def fetch_bitcoin_block_data():
    base_url = "https://blockchain.info/block-height/"
    params = "?format=json"
    
    latest_block_url = "https://blockchain.info/q/getblockcount"
    try:
        # Lấy số block mới nhất từ blockchain.info
        latest_block_response = requests.get(latest_block_url)
        latest_block_response.raise_for_status()
        latest_block = latest_block_response.json()
    except requests.exceptions.RequestException as e:
        print(f"Đã xảy ra lỗi khi lấy thông tin block mới nhất: {e}")
        return
    
    # Bắt đầu từ block cuối cùng đã xử lý hoặc từ block 1
    start_block = address_balances.get('last_processed_block', 0) + 1
    print(f"Bắt đầu crawl từ block: {start_block}")
    
    try:
        for block_height in range(start_block, latest_block + 1):
            url = f"{base_url}{block_height}{params}"
            try:
                response = requests.get(url)
                response.raise_for_status()
                data = response.json()
                
                # Lấy thông tin về các giao dịch trong block
                block_info = data['blocks'][0]
                transactions = block_info.get('tx', [])
                
                for tx in transactions:
                    # Lấy danh sách các địa chỉ từ đầu vào và đầu ra của giao dịch
                    for input in tx.get('inputs', []):
                        if 'prev_out' in input and 'addr' in input['prev_out']:
                            address = input['prev_out']['addr']
                            value = input['prev_out'].get('value', 0)
                            address_balances[address] = address_balances.get(address, 0) - value / 1e8
                    for output in tx.get('out', []):
                        if 'addr' in output:
                            address = output['addr']
                            value = output.get('value', 0)
                            address_balances[address] = address_balances.get(address, 0) + value / 1e8
                
                print(f"Đã xử lý block height {block_height}, tổng số địa chỉ: {len(address_balances)}")
            except requests.exceptions.RequestException as e:
                print(f"Đã xảy ra lỗi khi lấy dữ liệu block height {block_height}: {e}")
            
            # Lưu danh sách địa chỉ và số dư sau mỗi 1.000 block
            address_balances['last_processed_block'] = block_height
            if block_height % 1000 == 0:
                with open("last_processed_block.txt", "w") as f:
                    f.write(f"last_processed_block: {block_height}\n")
                    for address, balance in address_balances.items():
                        f.write(f"{address}: {balance}\n")
            
            # Thêm sleep để tránh rate limit
            sleep(0.1)
    except Exception as e:
        print(f"Đã xảy ra lỗi trong quá trình xử lý block: {e}")
    finally:
        # Lưu danh sách địa chỉ và số dư vào file cuối cùng
        with open("last_processed_block.txt", "w") as f:
            f.write(f"last_processed_block: {block_height}\n")
            for address, balance in address_balances.items():
                f.write(f"{address}: {balance}\n")

if __name__ == "__main__":
    fetch_bitcoin_block_data()
