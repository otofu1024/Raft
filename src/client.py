import asyncio
import httpx

async def main():
    payload = {
        "term": 1,
        "leaderID": "n1",
        "prevLogIndex": 0,
        "prevLogTerm": 0,
        "entries": [
            {
                "index":1,
                "term":1,
                "command": [
                    {"key":"name","value":"eito"},
                    {"key":"age", "value":3}
                    ]
            }
        ],
        "leaderCommit": 1
    }

    async with httpx.AsyncClient(timeout = 2.0) as client:
        r = await client.post("http://localhost:8001/test/append_entries", json = payload)
        print(r.status_code)
        print(r.json())

if __name__ == "__main__":
    asyncio.run(main())