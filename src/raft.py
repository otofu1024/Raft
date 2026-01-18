from fastapi import FastAPI
import httpx
import asyncio
import os
from contextlib import asynccontextmanager
from pydantic import BaseModel
from typing import Any

other_nodes = os.environ["PEERS"].split(",")
other_nodes = list(map(lambda x: x.split(":"), other_nodes))

class Command(BaseModel):
    key: str
    value: Any

class LogEntry(BaseModel):
    index: int
    term: int
    command: list[Command]

class AppendEntriesResult(BaseModel):
    term: int
    success: bool

class RequestVoteResult(BaseModel):
    term: int
    voteGranted: bool

class State(BaseModel):
    name: str
    age: int
    gender: str | None

class AppendEntriesArgs(BaseModel):
    term: int
    leaderID: str
    prevLogIndex: int
    prevLogTerm: int
    entries: list[LogEntry]
    leaderCommit: int


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

        self.leaderid: str | None = None

    def election_init(self):
        self.nextIndex: dict = {}
        self.matchindex: dict = {}

    def get_last_log(self):
        last_log: LogEntry = next(reversed(self.log), LogEntry(index = 0, term = 0, command = []))
        return last_log
    
    def match_prev(self, prevLogIndex: int, prevLogTerm: int):
        if (prevLogIndex == 0 and prevLogTerm == 0):
            return True
        for e in self.log:
            if e["index"] == prevLogIndex:
                if e["term"] == prevLogTerm:
                    return True
        return False
    
    def AppendEntries_RPC(self,
                          term: int,
                          leaderID: str,
                          prevLogIndex: int,
                          prevLogTerm: int,
                          entries: list[LogEntry],
                          leaderCommit: int):
        self.leaderid = leaderID
        try:
            if term < self.currentTerm:
                return AppendEntriesResult(term = self.currentTerm, success = False)

            if term >= self.currentTerm:
                self.job = "Follower"

            self.currentTerm = term

            pos_by_index = {entry["index"]: i for i, entry in enumerate(self.log)}

            if self.match_prev(prevLogIndex, prevLogTerm) is False:
                return AppendEntriesResult(term = self.currentTerm, success = False)
            
            for i,v in enumerate(entries):
                a = pos_by_index.get(v["index"])
                if a is not None:
                    if self.log[a]["term"] != v["term"]:
                        del self.log[a:]
                        self.log += entries[i:]
                        break
                else:
                    self.log += entries[i:]
                    break

            if leaderCommit > self.commitIndex:
                last_log = self.get_last_log()
                self.commitIndex = min(leaderCommit, last_log["index"])
            
            self.apply()
            return AppendEntriesResult(term= self.currentTerm, success= True)

        except Exception as e:
            print("AppendEntries_RPC error:", e)
            return AppendEntriesResult(term=self.currentTerm, success=False)

    # Candicateからフォロワーへ
    def RequestVote_RPC(self,
                        term:int,
                        candidateid: str,
                        lastLogIndex: int,
                        lastLogTerm: int):
        voteGranted: bool = False
        last_log: LogEntry = self.get_last_log()

        if term < self.currentTerm:
            voteGranted = False
            return RequestVoteResult(term = self.currentTerm, voteGranted = voteGranted)
        elif term == self.currentTerm:
            pass
        else:
            self.votedFor = None
            self.currentTerm = term
            self.job = "Follower"

        if self.votedFor in (candidateid, None) and lastLogTerm > last_log["term"]:
            voteGranted = True
            self.votedFor = candidateid
            return RequestVoteResult(term = self.currentTerm, voteGranted = voteGranted)
        
        if self.votedFor in (candidateid, None) and lastLogTerm == last_log["term"] and lastLogIndex >= last_log["index"]:
            voteGranted = True
            self.votedFor = candidateid
            return RequestVoteResult(term = self.currentTerm, voteGranted = voteGranted)
    
        return RequestVoteResult(self.currentTerm, voteGranted)
    
    def apply(self):
        pos_by_index = {entry["index"]: i for i, entry in enumerate(self.log)}
        while self.commitIndex > self.lastApplied:
            self.lastApplied += 1
            apply_index = pos_by_index.get(self.lastApplied)

            if apply_index is None:
                raise RuntimeError(f"Missing log entry for index={self.lastApplied}")
            
            apply_cmd = self.log[apply_index]["command"]
            for i in apply_cmd:
                self.state[i["key"]] = i["value"]
    
# 常駐処理
async def raft_loop(stop_event: asyncio.Event):
    try:
        while not stop_event.is_set():
            # timeoutとかheartbeatとか色々
            await asyncio.sleep(0.1)

    except asyncio.CancelledError:
        raise

@asynccontextmanager
async def lifespan(app: FastAPI):
    stop_event = asyncio.Event()
    task = asyncio.create_task(raft_loop(stop_event))

    yield

    stop_event.set()
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    

node = Node()

app = FastAPI(lifespan=lifespan)

@app.get("/")
async def raft():
    return other_nodes

@app.get("/get")
async def get_states():
    return node.state

@app.get("/get/log")
async def get_log():
    return node.log

@app.get("/test")
async def test():
    async with httpx.AsyncClient() as client:
        r = await client.get(f'http://{other_nodes[0][0]}:8000/')
        return (r.status_code, r.text)

# Rules for Servers
## All Servers

@app.get("/rpc_term")
async def to_followre():
    pass

@app.post("/test/append_entries")
async def test_send_append_entries(args: AppendEntriesArgs):
        return node.AppendEntries_RPC(
            term=args.term,
            leaderID=args.leaderID,
            prevLogIndex=args.prevLogIndex,
            prevLogTerm=args.prevLogTerm,
            entries=args.entries,
            leaderCommit=args.leaderCommit,
        )