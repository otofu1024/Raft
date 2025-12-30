# Raft実装
## ファイル構成
Raft/
├─ docker-compose.yml
├─ Dockerfile
├─ .python-version
├─ pyproject.toml
├─ uv.lock
├─ .env                 # 任意（環境変数をまとめたい場合）
├─ .gitignore
├─ README.md
├─ data/                # 永続化データ（各ノードのWAL/スナップショット等）
│  ├─ n1/
│  ├─ n2/
│  └─ n3/
└─ app/
   ├─ main.py           # FastAPIエントリ（ルーティング/起動時初期化）
   ├─ node.py           # Raft本体（状態/タイマー/RPC処理）
   ├─ rpc.py            # 送信側RPCクライアント（HTTP送信の薄い層）
   ├─ storage.py        # 永続化（WAL追記/復元、後でsnapshot）
   ├─ state_machine.py  # 状態機械（KVSなど apply(command)）
   ├─ types.py          # メッセージ型（RequestVote/AppendEntries 等）
   └─ config.py         # 環境変数の読み取り（NODE_ID/PEERS/PORTなど）
