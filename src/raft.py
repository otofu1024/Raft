from fastapi import FastAPI
import httpx
import asyncio
import os
from contextlib import asynccontextmanager
from pydantic import BaseModel, Field
from typing import Literal, Any, Annotated

other_nodes = os.environ["PEERS"].split(",")
other_nodes = list(map(lambda x: x.split(":"), other_nodes))

class PutCommand(BaseModel):
    type: Literal["put"]
    key: str
    value: Any

class DelCommand(BaseModel):
    type: Literal["del"]
    key: str

class PatchCommand(BaseModel):
    type: Literal["patch"]
    values: dict[str, Any]

Command = Annotated[
    PutCommand | DelCommand | PatchCommand,
    Field(discriminator="type")
]

class LogEntry(BaseModel):
    index: int
    term: int
    command: Command

class AppendEntriesResult(BaseModel):
    term: int
    success: bool

class RequestVoteResult(BaseModel):
    term: int
    voteGranted: bool

# 状態と受け取り、送信の関数の設定のみ
class Node():
    def __init__(self):
        self.host: str = os.environ["NODE_ID"]
        self.state :dict[str, Any] = {}

        # LeaderかCandidateかFollowerかを示すid
        self.job: str = "Follower"
        
        # Persistent state on all services
        self.currentTerm: int = 0
        self.votedFor: str | None = None
        self.log: list[LogEntry] = []

        # Volatile state on all services
        self.commitIndex: int = 0
        self.lastApplied: int = 0

        # Volatile state on leaders
        self.election_init()

    def election_init(self):
        self.nextIndex: dict = {}
        self.matchindex: dict = {}

    def get_last_log(self):
        last_log: dict = next(reversed(self.log), {"index": 0, "term": 0, "command": None})
        return last_log
    
    # Leaderからフォロワーへ、未完成
    def AppendEntries_RPC(self,
                          term: int,
                          leaderID: str,
                          prevLogIndex: int,
                          prevLogTerm: int,
                          entries: dict,
                          leaderCommit: int):

        if term < self.currentTerm:
            return AppendEntriesResult(term = self.currentTerm, success = False)
        
        last_log = self.get_last_log
        # if not(len(self.log) == prevLogIndex or self.log[-1]["term"] == prevlogTerm):
        if last_log["index"] < prevLogIndex:
            return AppendEntriesResult(term = self.currentTerm, success = False)
        
        if last_log["term"] != prevLogTerm:
            return AppendEntriesResult(term = self.currentTerm, success = False)
        
        if len(entries) == len(self.log) and not(self.currentTerm == term):

            return AppendEntriesResult()
    
    # Candicateからフォロワーへ
    def RequestVote_RPC(self,
                        term:int,
                        candidateid: str,
                        lastLogIndex: int,
                        lastLogTerm: int):
        voteGranted: bool = False
        last_log: dict = self.get_last_log

        if term < self.currentTerm:
            voteGranted = False
            return RequestVoteResult(term = self.currentTerm, voteGranted = voteGranted)
        elif term == self.currentTerm:
            pass
        else:
            self.votedFor = None
            self.currentTerm = term

        if self.votedFor in (candidateid, None) and lastLogTerm > last_log["term"]:
            voteGranted = True
            self.votedFor = candidateid
            return RequestVoteResult(term = self.currentTerm, voteGranted = voteGranted)
        
        if self.votedFor in (candidateid, None) and lastLogTerm == last_log["term"] and lastLogIndex >= last_log["index"]:
            voteGranted = True
            self.votedFor = candidateid
            return RequestVoteResult(term = self.currentTerm, voteGranted = voteGranted)
    
        return RequestVoteResult(self.currentTerm, voteGranted)
    
    def apply(self, command: Command) -> None:
        self.state 
    

@asynccontextmanager
async def lifespan(app: FastAPI):
    # 起動時に1回（startup）
    # 例: app.state.db = await connect_db()
    while True:
        if node.commitIndex > node.lastApplied:
            node.lastApplied += 1
            if node.log[node.lastApplied].command == PutCommand:
                async with httpx.AsyncClient() as client:
                    r = await client.get('http://localhost:8000/kvs')
                    return (r.status_code, r.text)

        await asyncio.sleep(0.05)

    yield

    # 終了時に1回（shutdown）
    # 例: await app.state.db.close()

node = Node()

app = FastAPI(lifespan=lifespan)

# Rules for Servers
@app.post("/")
async def raft():
    return other_nodes

@app.put("/kvs")
async def put_states():
    return node.state

@app.get("/test")
async def main():
    async with httpx.AsyncClient() as client:
        r = await client.get('http://n2:8000')
        return (r.status_code, r.text)