from fastapi import FastAPI
import httpx
import asyncio
from contextlib import asynccontextmanager

import os
import random
import time

from enum import Enum
from pydantic import BaseModel
from typing import Any

other_nodes: list[str] = os.environ["PEERS"].split(",")

# 型定義
class State(BaseModel):
    name: str
    age: int
    gender: str | None

class Job(str, Enum):
    follower = "Follower"
    candidate = "Candidate"
    leader = "Leader"

class Command(BaseModel):
    key: str
    value: Any

class LogEntry(BaseModel):
    index: int
    term: int
    command: list[Command]

class AppendEntriesArgs(BaseModel):
    term: int
    leaderID: str
    prevLogIndex: int
    prevLogTerm: int
    entries: list[LogEntry]
    leaderCommit: int

class AppendEntriesResult(BaseModel):
    term: int
    success: bool

class RequestVoteArgs(BaseModel):
    term:int
    candidateId: str
    lastLogIndex: int
    lastLogTerm: int

class RequestVoteResult(BaseModel):
    term: int
    voteGranted: bool


# 本体。ノードの状態管理
class Node():
    def __init__(self):
        self.host: str = os.environ["NODE_ID"]
        self.state :dict[str, Any] = {}

        # LeaderかCandidateかFollowerかを示すid
        self.job: Job = Job.follower
        
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

        # timer
        self.timer_init()

        self.heartbeat: float = 0.010

        self.client = httpx.AsyncClient(timeout = 0.500)

    def timer_init(self):
        self.start_time:float = time.monotonic()
        self.timeout:float = random.uniform(0.150, 0.300)

    def election_init(self):
        self.nextIndex: dict = {}
        self.matchindex: dict = {}

    def get_last_log(self) -> LogEntry:
        last_log: LogEntry = next(reversed(self.log), LogEntry(index = 0, term = 0, command = []))
        return last_log
    
    def add_log_entry(self, command: Command):
        last_log: LogEntry = self.get_last_log
        index = last_log.index + 1
        term = self.currentTerm
        log_entry = LogEntry(index= index,
                             term= term,
                             command= command)
        self.log.append(log_entry)

    def match_prev(self, prevLogIndex: int, prevLogTerm: int) -> bool:
        if (prevLogIndex == 0 and prevLogTerm == 0):
            return True
        for e in self.log:
            if e.index == prevLogIndex:
                if e.term == prevLogTerm:
                    return True
        return False
    
    def AppendEntries_RPC(self, args: AppendEntriesArgs) -> AppendEntriesResult:
        try:
            self.timer_init()

            if args.term < self.currentTerm:
                return AppendEntriesResult(term = self.currentTerm, success = False)
            elif args.term > self.currentTerm:
                self.votedFor = None
                self.currentTerm = args.term

            self.leaderid = args.leaderID
            self.job = Job.follower

            pos_by_index = {entry.index: i for i, entry in enumerate(self.log)}

            if self.match_prev(args.prevLogIndex, args.prevLogTerm) is False:
                return AppendEntriesResult(term = self.currentTerm, success = False)
            
            for i,v in enumerate(args.entries):
                real_index = pos_by_index.get(v.index)
                if real_index is not None:
                    if self.log[real_index].term != v.term:
                        del self.log[real_index:]
                        self.log += args.entries[i:]
                        break
                else:
                    self.log += args.entries[i:]
                    break

            if args.leaderCommit > self.commitIndex:
                last_log = self.get_last_log()
                self.commitIndex = min(args.leaderCommit, last_log.index)
            
            self.apply()
            self.nextIndex += 1
            self.matchindex += 1
            return AppendEntriesResult(term= self.currentTerm, success= True)

        except Exception as e:
            print("AppendEntries_RPC error:", e)
            return AppendEntriesResult(term=self.currentTerm, success=False)

    # Candicateからフォロワーへ
    def RequestVote_RPC(self, args: RequestVoteArgs) -> RequestVoteResult:
        
        voteGranted: bool = False
        last_log: LogEntry = self.get_last_log()

        if args.term < self.currentTerm:
            voteGranted = False
            return RequestVoteResult(term = self.currentTerm, voteGranted = voteGranted)
        elif args.term == self.currentTerm:
            pass
        else:
            self.votedFor = None
            self.currentTerm = args.term
            self.job = Job.follower

        if self.votedFor in (args.candidateId, None) and args.lastLogTerm > last_log.term:
            voteGranted = True
            self.votedFor = args.candidateId
            return RequestVoteResult(term = self.currentTerm, voteGranted = voteGranted)
        
        if self.votedFor in (args.candidateId, None) and args.lastLogTerm == last_log.term and args.lastLogIndex >= last_log.index:
            voteGranted = True
            self.votedFor = args.candidateId
            return RequestVoteResult(term = self.currentTerm, voteGranted = voteGranted)

        return RequestVoteResult(self.currentTerm, voteGranted)
    
    def apply(self):
        pos_by_index = {entry.index: i for i, entry in enumerate(self.log)}
        while self.commitIndex > self.lastApplied:
            self.lastApplied += 1
            apply_index = pos_by_index.get(self.lastApplied)

            if apply_index is None:
                raise RuntimeError(f"Missing log entry for index={self.lastApplied}")
            
            apply_cmd = self.log[apply_index].command
            for i in apply_cmd:
                self.state[i.key] = i.value

    async def send_RequestVote_rpc(self, url: str, payload: RequestVoteArgs):
        try:
            r = await self.client.post(url, json = payload.model_dump())
            return url, r.status_code, r.json()
        except Exception as e:
            return url, None, {"error": str(e)}
        
    async def send_AppendEntries_rpc(self, url: str, payload: AppendEntriesArgs):
        try:
            r = await self.client.post(url, json = payload.model_dump())
            return url, r.status_code, r.json()
        except Exception as e:
            return url, None, {"error": str(e)}

    async def elect(self):
        self.currentTerm += 1
        self.votedFor = self.host
        self.timer_init()
        last_log = self.get_last_log()
        payload = RequestVoteArgs(term = self.currentTerm,
                                  candidateId = self.host,
                                  lastLogIndex = last_log.index,
                                  lastLogTerm = last_log.term)
        tasks = [asyncio.create_task(self.send_RequestVote_rpc(f"http://{i}/RequestVote", payload)) for i in other_nodes]
        ok: int = 1
        need: int = (len(other_nodes)+1)//2 + 1

        try:
            for done in asyncio.as_completed(tasks):
                try:
                    r = await done
                    vote = RequestVoteResult(**r[2]).voteGranted
                except Exception:
                    vote = False

                if vote is True:
                    ok += 1
                    if ok >= need:
                        ## 見事選挙勝利！
                        self.job = Job.leader
                        self.leader()
                        break
            
        except asyncio.TimeoutError:
            pass

        finally:
            for t in tasks:
                if not t.done():
                    t.cancel()

    # Leader処理
    async def leader(self):
        last_log: LogEntry = self.get_last_log()
        payload = AppendEntriesArgs(term=self.currentTerm,
                                    leaderID=self.host,
                                    prevLogIndex=last_log.index,
                                    prevLogTerm=last_log.term,
                                    entries=self.log,
                                    leaderCommit=self.commitIndex)
        
        tasks = [asyncio.create_task(self.send_AppendEntries_rpc(url=f"http://{i}/RequestVote",
                                                                  payload=payload.model_dump()) for i in other_nodes)]


    # 常駐処理
    async def loop(self, stop_event: asyncio.Event):
        try:
            while not stop_event.is_set():
                # timeoutとかheartbeatとか色々
                now = time.monotonic()

                # 選挙開始
                if now - node.start_time > self.timeout:
                    node.job = Job.candidate
                    try:
                        await asyncio.wait_for(node.elect(), timeout = node.timeout)
                    except asyncio.TimeoutError:
                        continue

                await asyncio.sleep(0.005)

        except asyncio.CancelledError:
            raise

# FastAPI側
@asynccontextmanager
async def lifespan(app: FastAPI):
    stop_event = asyncio.Event()
    task = asyncio.create_task(node.loop(stop_event))

    yield

    stop_event.set()
    task.cancel()
    await node.client.aclose()
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
    return node.state, node.currentTerm

@app.get("/get/log")
async def get_log():
    return node.log

@app.get("/test")
async def test():
    async with httpx.AsyncClient() as client:
        r = await client.get(f'http://{other_nodes[0]}/')
        return (r.status_code, r.text)

# Rules for Servers
## All Servers

@app.post("/RequestVote")
async def vote(args: RequestVoteArgs):
    return node.RequestVote_RPC(args)

@app.post("/append_entries")
async def test_send_append_entries(args: AppendEntriesArgs):
    return node.AppendEntries_RPC(args)

@app.get("/get_nextindex")
async def get_nextindex():
    return node.nextIndex

@app.post("/from_client")
async def client(command: Command):
    if node.host == Job.leader:
        node.add_log_entry(command)
        node.apply()
        return node.state
    else:
        try:
            node.client.post(f"http://{node.leaderid}:8000/from_client", timeout=2.0)
        except TimeoutError:
            pass