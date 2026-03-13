# Raft

Python + FastAPI で作った、Raft コンセンサスアルゴリズムの学習用実装です。  
複数ノード間で leader election と log replication を行い、クライアントから送られた更新をクラスタ全体へ反映します。

## 概要

このリポジトリは、Raft の基本的な流れを手元で確認するための最小構成の実装です。

- `RequestVote` による leader election
- `AppendEntries` による heartbeat / log replication
- クライアントからの更新受付
- Docker Compose での 5 ノード起動
- 反映確認用のクライアントスクリプト付き

状態として扱うキーは現在以下の 3 つに限定しています。

- `name`
- `age`
- `gender`

値は任意の JSON 値を受け取れます。

---

## アーキテクチャ

各ノードは FastAPI サーバとして動作し、他ノードへ HTTP で RPC を送ります。

### ノード間 RPC

- `POST /RequestVote`
- `POST /AppendEntries`

### クライアント向け API

- `POST /from_client`

### デバッグ用 API

- `GET /get`  
  現在の state と currentTerm を返します

- `GET /get/log`  
  ログ全体を返します

- `GET /get/last_log`  
  最後のログエントリを返します

- `GET /get/leader_id`  
  現在認識している leader ID を返します

- `GET /get/nextindex`  
  leader が持つ `nextIndex` を返します

- `GET /test`  
  他ノード疎通確認用の簡易エンドポイントです

---

## ディレクトリ構成

```text
.
├── Dockerfile
├── docker-compose.yml
├── pyproject.toml
├── uv.lock
├── sample
│   ├── command.json
│   └── state.json
└── src
    ├── raft.py
    ├── client.py
    ├── client_random_100.py
    └── test.py
