from fastapi import FastAPI
import httpx

app = FastAPI()

@app.get("/")
async def read_root():
    return {"Hello": "World"}

