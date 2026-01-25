# client.py
import argparse
import asyncio
import json
import time
from typing import Any, Dict, List, Tuple, Optional

import httpx


def now_ms() -> int:
    return int(time.time() * 1000)


async def fetch_get(client: httpx.AsyncClient, base: str, path: str, timeout: float) -> Any:
    r = await client.get(f"{base}{path}", timeout=timeout)
    r.raise_for_status()
    return r.json()


async def fetch_post(client: httpx.AsyncClient, base: str, path: str, payload: Any, timeout: float) -> Any:
    r = await client.post(f"{base}{path}", json=payload, timeout=timeout)
    r.raise_for_status()
    return r.json()


async def snapshot(
    client: httpx.AsyncClient, nodes: List[str], timeout: float
) -> List[Dict[str, Any]]:
    tasks = []
    for n in nodes:
        tasks.append(fetch_get(client, n, "/get", timeout))
        tasks.append(fetch_get(client, n, "/get/log", timeout))

    out: List[Dict[str, Any]] = []
    results = await asyncio.gather(*tasks, return_exceptions=True)

    for i, n in enumerate(nodes):
        get_res = results[i * 2]
        log_res = results[i * 2 + 1]

        item: Dict[str, Any] = {"node": n}

        if isinstance(get_res, Exception):
            item["get_error"] = repr(get_res)
        else:
            # /get -> [state, term]
            item["state"] = get_res[0]
            item["term"] = get_res[1]

        if isinstance(log_res, Exception):
            item["log_error"] = repr(log_res)
        else:
            item["log_len"] = len(log_res)
            item["last_term"] = log_res[-1]["term"] if log_res else None

        out.append(item)

    return out


def print_snapshot(snap: List[Dict[str, Any]]) -> None:
    lines = []
    for x in snap:
        if "get_error" in x or "log_error" in x:
            lines.append(
                f"- {x['node']}: GET_ERR={x.get('get_error')} LOG_ERR={x.get('log_error')}"
            )
            continue
        st = x.get("state", {})
        term = x.get("term")
        llen = x.get("log_len")
        lterm = x.get("last_term")
        keys = ",".join(list(st.keys())[:6])
        if len(st.keys()) > 6:
            keys += ",..."
        lines.append(f"- {x['node']}: term={term} log_len={llen} last_term={lterm} keys=[{keys}]")
    print("\n".join(lines))


async def send_command_until_ok(
    client: httpx.AsyncClient,
    nodes: List[str],
    commands: List[Dict[str, Any]],
    timeout: float,
    deadline_s: float,
) -> Tuple[str, Any]:
    start = time.monotonic()
    i = 0
    last_err: Optional[str] = None

    while time.monotonic() - start < deadline_s:
        base = nodes[i % len(nodes)]
        i += 1
        try:
            r = await client.post(f"{base}/from_client", json=commands, timeout=timeout)
            if r.status_code == 503:
                # election in progress, try another node
                last_err = r.text
                await asyncio.sleep(0.05)
                continue
            r.raise_for_status()
            return base, r.json()
        except Exception as e:
            last_err = repr(e)
            await asyncio.sleep(0.05)

    raise RuntimeError(f"send_command timed out. last_err={last_err}")


def state_has_key_value(state: Dict[str, Any], key: str, value: Any) -> bool:
    return key in state and state[key] == value


async def wait_converge(
    client: httpx.AsyncClient,
    nodes: List[str],
    key: str,
    value: Any,
    min_log_len: int,
    timeout: float,
    interval: float,
    deadline_s: float,
) -> bool:
    start = time.monotonic()
    while time.monotonic() - start < deadline_s:
        snap = await snapshot(client, nodes, timeout)
        ok = True
        for x in snap:
            if "get_error" in x or "log_error" in x:
                ok = False
                continue
            st = x.get("state", {})
            if not state_has_key_value(st, key, value):
                ok = False
            if int(x.get("log_len", 0)) < min_log_len:
                ok = False
        if ok:
            return True
        await asyncio.sleep(interval)
    return False


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--nodes",
        nargs="+",
        required=True,
        help="例: http://localhost:8001 http://localhost:8002 http://localhost:8003",
    )
    ap.add_argument("--timeout", type=float, default=0.5)
    ap.add_argument("--interval", type=float, default=0.2)
    ap.add_argument("--send", action="store_true", help="コマンドを1回投げる")
    ap.add_argument("--key", type=str, default=f"probe_{now_ms()}")
    ap.add_argument("--value", type=str, default="1")
    ap.add_argument("--value-json", action="store_true", help="value を JSON として解釈する")
    ap.add_argument("--watch", action="store_true", help="反映されるまで待つ")
    ap.add_argument("--watch-deadline", type=float, default=5.0)
    ap.add_argument("--send-deadline", type=float, default=5.0)

    args = ap.parse_args()
    nodes = [n.rstrip("/") for n in args.nodes]

    value: Any = args.value
    if args.value_json:
        value = json.loads(args.value)

    commands = [{"key": args.key, "value": value}]

    async with httpx.AsyncClient() as client:
        print("== snapshot (before) ==")
        snap0 = await snapshot(client, nodes, args.timeout)
        print_snapshot(snap0)

        base_log_len = None
        for x in snap0:
            if "log_len" in x:
                base_log_len = x["log_len"]
                break
        if base_log_len is None:
            base_log_len = 1

        if args.send:
            print(f"\n== send /from_client : {commands} ==")
            who, resp = await send_command_until_ok(
                client, nodes, commands, args.timeout, args.send_deadline
            )
            print(f"sent via: {who}")
            print(f"resp: {resp}")

        if args.watch:
            print("\n== watch converge ==")
            ok = await wait_converge(
                client=client,
                nodes=nodes,
                key=args.key,
                value=value,
                min_log_len=base_log_len + (1 if args.send else 0),
                timeout=args.timeout,
                interval=args.interval,
                deadline_s=args.watch_deadline,
            )
            print("converged!" if ok else "not converged (timeout)")

        print("\n== snapshot (after) ==")
        snap1 = await snapshot(client, nodes, args.timeout)
        print_snapshot(snap1)


if __name__ == "__main__":
    asyncio.run(main())
