# 100%ちゃっぴー

# client_random_100.py
import asyncio
import random
import time
from typing import Any, Dict, Tuple

import httpx

# docker-compose の公開ポート想定（必要ならここだけ直して）
NODES = {
    "n1": "http://127.0.0.1:8001",
    "n2": "http://127.0.0.1:8002",
    "n3": "http://127.0.0.1:8003",
    "n4": "http://127.0.0.1:8004",
    "n5": "http://127.0.0.1:8005",
}

STATE_KEYS = ["name", "age", "gender"]  # Literal に合わせる
TIMEOUT = httpx.Timeout(connect=1.0, read=2.0, write=2.0, pool=2.0)


async def get_state_and_term(client: httpx.AsyncClient, base_url: str) -> Tuple[Dict[str, Any], int]:
    """
    /get は (state, currentTerm) を返しているので JSON は [state, term] を想定
    """
    r = await client.get(f"{base_url}/get")
    r.raise_for_status()
    data = r.json()

    if isinstance(data, list) and len(data) == 2 and isinstance(data[0], dict):
        return data[0], int(data[1])

    # もしサーバ側を変えて dict 形式にした場合の保険
    if isinstance(data, dict) and "state" in data and "currentTerm" in data:
        return data["state"], int(data["currentTerm"])

    raise ValueError(f"Unexpected /get response: {data}")


async def post_update(client: httpx.AsyncClient, base_url: str, key: str, value: Any) -> httpx.Response:
    payload = [{"key": key, "value": value}]
    return await client.post(f"{base_url}/from_client", json=payload)


async def wait_all_nodes_applied(
    client: httpx.AsyncClient,
    key: str,
    value: Any,
    timeout_sec: float = 5.0,
    poll_interval: float = 0.05,
) -> Tuple[bool, Dict[str, Any]]:
    """
    全ノードで state[key] == value になるまで待つ。
    戻り値: (ok, last_seen_values)
    """
    deadline = time.monotonic() + timeout_sec
    last_seen: Dict[str, Any] = {}

    while time.monotonic() < deadline:
        ok = True
        for name, base in NODES.items():
            try:
                st, _term = await get_state_and_term(client, base)
                last_seen[name] = st.get(key)
                if st.get(key) != value:
                    ok = False
            except Exception:
                ok = False
        if ok:
            return True, last_seen

        await asyncio.sleep(poll_interval)

    return False, last_seen


def make_value(key: str, i: int) -> Any:
    """
    key に応じてそれっぽい型の value を作る（Any でもOKだけど、確認しやすいように）
    """
    if key == "name":
        return f"name_{i:03d}"
    if key == "age":
        return i  # int
    if key == "gender":
        return None if i % 3 == 0 else ("male" if i % 2 == 0 else "female")
    raise ValueError(key)


async def final_verify(client: httpx.AsyncClient, expected_state: Dict[str, Any]) -> bool:
    """
    最終状態（name/age/gender）が全ノードで一致しているか確認
    """
    all_ok = True
    for node_name, base in NODES.items():
        try:
            st, term = await get_state_and_term(client, base)
        except Exception as e:
            print(f"[final] {node_name} unreachable: {e}")
            all_ok = False
            continue

        diffs = []
        for k, v in expected_state.items():
            if st.get(k) != v:
                diffs.append(f"{k} expected={v} got={st.get(k)}")
        if diffs:
            print(f"[final] {node_name} term={term} ❌ diffs={diffs}")
            all_ok = False
        else:
            print(f"[final] {node_name} term={term} ✅ ok")

    return all_ok


async def main():
    # 最終的にどうなるべきか（最後に各キーへ書いた値）
    expected_end: Dict[str, Any] = {}

    async with httpx.AsyncClient(timeout=TIMEOUT) as client:
        for i in range(100):
            key = random.choice(STATE_KEYS)  # Literalのどれか
            value = make_value(key, i)

            # 期待最終状態を更新（上書きされる前提）
            expected_end[key] = value

            # ランダムなノードに投げる（503とか失敗なら別ノードでリトライ）
            sent_via = None
            for attempt in range(30):
                node_name = random.choice(list(NODES.keys()))
                base = NODES[node_name]
                try:
                    r = await post_update(client, base, key, value)

                    if r.status_code == 503:
                        # leader election in progress / leader unreachable 等
                        await asyncio.sleep(0.05)
                        continue

                    r.raise_for_status()
                    sent_via = node_name
                    break

                except Exception:
                    await asyncio.sleep(0.05)

            if sent_via is None:
                raise RuntimeError(f"failed to send update {i} ({key}={value}) after retries")

            print(f"[send-ok] {i+1:03d}/100 {key}={value} via {sent_via}")

            # 複製確認：全ノードに反映されるまで待つ
            ok, seen = await wait_all_nodes_applied(client, key, value, timeout_sec=5.0)
            if ok:
                print(f"[replicated] {key}={value} applied on all nodes ✅")
            else:
                print(f"[replication-timeout] {key} expected={value} seen={seen} ❌")
                raise RuntimeError(f"replication timeout for {key}={value}")

        print("\n[info] 100 updates done. final verify (name/age/gender) on all nodes...")
        # 3キー全部が1回は更新されるとは限らないので、入ってる分だけチェック
        if not expected_end:
            print("No updates recorded? (unexpected)")
            return

        all_ok = await final_verify(client, expected_end)
        if all_ok:
            print("\n✅ ALL NODES CONSISTENT for final state keys:", expected_end)
        else:
            print("\n❌ FINAL STATE MISMATCH")


if __name__ == "__main__":
    asyncio.run(main())
