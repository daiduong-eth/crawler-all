Dùng Tmux chạy song song các file crawl

tmux

python3 crawl-btc-txs.py

ctrl + b -> c

python3 crawl-btc-wallet.py

tmux list-sessions

tmux attach-session -t 0

tmux kill-session -t <session_name>

tmux attach-session -t <session_name>

tmux new -s my_session
tmux new -s <session_name>



