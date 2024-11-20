import requests
import csv
from time import sleep
import os

transaction_history = []  # Danh sách lưu lịch sử giao dịch
CSV_FILE = "transaction_history.csv"  # File lưu lịch sử giao dịch


def get_last_processed_block_from_csv():
    # Đọc block cuối cùng từ file CSV
    if os.path.exists(CSV_FILE):
        with open(CSV_FILE, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            rows = list(reader)  # Đọc toàn bộ nội dung file
            if len(rows) > 1:  # Nếu file có dữ liệu (ít nhất 1 dòng header và 1 dòng dữ liệu)
                last_row = rows[-1]
                return int(last_row[0])  # Lấy block_height từ dòng cuối cùng
    return 1  # Nếu file không tồn tại hoặc rỗng, bắt đầu từ block 1


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

    # Đọc block cuối cùng đã xử lý từ CSV
    start_block = get_last_processed_block_from_csv() + 1
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

            # Lưu lịch sử giao dịch ra file CSV sau mỗi 10 block
            if block_height % 10 == 0:
                save_to_csv(transaction_history, CSV_FILE)

            # Thêm sleep để tránh rate limit
            sleep(0.1)
    except Exception as e:
        print(f"Đã xảy ra lỗi trong quá trình xử lý block: {e}")
    finally:
        # Lưu toàn bộ lịch sử giao dịch ra file CSV
        save_to_csv(transaction_history, CSV_FILE)


def save_to_csv(data, filename):
    # Lưu dữ liệu vào file CSV
    file_exists = os.path.exists(filename)
    with open(filename, mode="a" if file_exists else "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        # Ghi tiêu đề nếu file chưa tồn tại
        if not file_exists:
            writer.writerow(["block_height", "sender", "recipient", "amount"])
        # Ghi dữ liệu
        for tx in data:
            writer.writerow([tx["block_height"], tx["sender"], tx["recipient"], tx["amount"]])
    data.clear()  # Xóa dữ liệu khỏi bộ nhớ sau khi lưu


if __name__ == "__main__":
    fetch_bitcoin_block_data()
