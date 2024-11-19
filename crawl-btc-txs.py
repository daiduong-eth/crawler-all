import requests
import csv
from time import sleep
import os

transaction_history = []  # Danh sách lưu lịch sử giao dịch
LAST_PROCESSED_BLOCK_FILE = "last_processed_block.txt"  # File lưu block cuối cùng đã xử lý


def get_last_processed_block():
    # Đọc block cuối cùng đã xử lý từ file
    if os.path.exists(LAST_PROCESSED_BLOCK_FILE):
        with open(LAST_PROCESSED_BLOCK_FILE, "r") as f:
            return int(f.read().strip())
    return 1  # Nếu file không tồn tại, bắt đầu từ block 1


def save_last_processed_block(block_height):
    # Lưu block cuối cùng đã xử lý vào file
    with open(LAST_PROCESSED_BLOCK_FILE, "w") as f:
        f.write(str(block_height))


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

    # Đọc block cuối cùng đã xử lý
    start_block = get_last_processed_block()
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

                for i, tx in enumerate(transactions):
                    tx_inputs = tx.get('inputs', [])
                    tx_outputs = tx.get('out', [])

                    # Xử lý block reward (coinbase transaction)
                    if i == 0:  # Coinbase transaction là giao dịch đầu tiên trong block
                        for output in tx_outputs:
                            recipient = output.get('addr', None)
                            reward_amount = output.get('value', 0) / 1e8  # Giá trị (BTC)

                            if recipient and reward_amount > 0:
                                transaction_history.append({
                                    "block_height": block_height,
                                    "sender": "COINBASE",  # Giao dịch block reward không có sender
                                    "recipient": recipient,
                                    "amount": reward_amount
                                })
                        continue

                    # Xử lý các giao dịch thông thường
                    for input in tx_inputs:
                        sender = input.get('prev_out', {}).get('addr', None)
                        value_sent = input.get('prev_out', {}).get('value', 0) / 1e8  # Giá trị gửi (BTC)

                        for output in tx_outputs:
                            recipient = output.get('addr', None)
                            value_received = output.get('value', 0) / 1e8  # Giá trị nhận (BTC)

                            if sender and recipient and value_sent > 0:
                                transaction_history.append({
                                    "block_height": block_height,
                                    "sender": sender,
                                    "recipient": recipient,
                                    "amount": value_received
                                })

                print(f"Đã xử lý block height {block_height}, tổng số giao dịch: {len(transaction_history)}")
            except requests.exceptions.RequestException as e:
                print(f"Đã xảy ra lỗi khi lấy dữ liệu block height {block_height}: {e}")

            # Lưu lịch sử giao dịch ra file CSV sau mỗi 1.000 block
            if block_height % 10 == 0:
                save_to_csv(transaction_history, "transaction_history.csv")

            # Lưu block hiện tại vào file
            save_last_processed_block(block_height)

            # Thêm sleep để tránh rate limit
            sleep(0.1)
    except Exception as e:
        print(f"Đã xảy ra lỗi trong quá trình xử lý block: {e}")
    finally:
        # Lưu toàn bộ lịch sử giao dịch ra file CSV
        save_to_csv(transaction_history, "transaction_history.csv")


def save_to_csv(data, filename):
    # Lưu dữ liệu vào file CSV
    with open(filename, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        # Ghi tiêu đề
        writer.writerow(["block_height", "sender", "recipient", "amount"])
        # Ghi dữ liệu
        for tx in data:
            writer.writerow([tx["block_height"], tx["sender"], tx["recipient"], tx["amount"]])


if __name__ == "__main__":
    fetch_bitcoin_block_data()
