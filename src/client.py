import asyncio
import httpx

async def main():
    payload = {
        "term": 5,
        "leaderID": "n2",
        "prevLogIndex": 1,
        "prevLogTerm": 1,
        "entries": [
            {
                "index":4,
                "term":4,
                "command": [
                    {"key":"name","value":"sako"},
                    {"key":"age","value":5},
                    {"key":"gender", "value":"female"}
                    ]
            },
            {
                "index":5,
                "term":4,
                "command": [
                    {"key":"name","value":"inari"},
                    {"key":"age","value":10},
                    {"key":"gender", "value":"male"}
                    ]
            }
        ],
        "leaderCommit": 999
    }

    async with httpx.AsyncClient(timeout = 2.0) as client:
        r = await client.post("http://localhost:8001/append_entries", json = payload)
        print(r.status_code)
        print(r.json())

if __name__ == "__main__":
    asyncio.run(main())