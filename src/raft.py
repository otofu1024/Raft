from fastapi import FastAPI, HTTPException
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
    host: str
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

class GetnextIndexResult(BaseModel):
    host: str
    nextIndex: dict[str, int]

class team_index(BaseModel):
    nextIndex: dict[str, int]
    matchIndex: dict[str, int]

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
        self.log: list[LogEntry] = [LogEntry(term=0, command=[])]


        # Volatile state on all services
        self.commitIndex: int = 0
        self.lastApplied: int = 0

        # Volatile state on leaders
        self.election_init()

        self.leaderid: str | None = None

        # timer
        self.timer_init()

        self.heartbeat: float = 0.100

        self.client = httpx.AsyncClient(timeout = 0.500)

    def timer_init(self):
        self.start_time:float = time.monotonic()
        self.timeout:float = random.uniform(0.150, 0.300)

    def election_init(self):
        self.nextIndex: dict[str, int] = {}
        self.matchIndex: dict[str, int] = {}
        
        for i in other_nodes:
            self.nextIndex[i] = len(self.log)
            self.matchIndex[i] = 0

    def get_last_log(self) -> LogEntry:
        return self.log[-1]
    
    def add_log_entry(self, command: list[Command]):
        term = self.currentTerm
        self.log.append(LogEntry(term=term, command=command))

    
    def AppendEntries_RPC(self, args: AppendEntriesArgs) -> AppendEntriesResult:
        self.timer_init()

        # term check
        if args.term < self.currentTerm:
            return AppendEntriesResult(host= self.host, term=self.currentTerm, success=False)

        if args.term > self.currentTerm:
            self.currentTerm = args.term
            self.votedFor = None

        self.leaderid = args.leaderID
        self.job = Job.follower

        # prevLog の整合性チェック
        # prevLogIndex==0 のときはダミーを参照
        if args.prevLogIndex >= len(self.log):
            return AppendEntriesResult(host=self.host, term=self.currentTerm, success=False)

        if self.log[args.prevLogIndex].term != args.prevLogTerm:
            return AppendEntriesResult(host=self.host, term=self.currentTerm, success=False)

        # entries を反映
        start = args.prevLogIndex + 1
        for i, e in enumerate(args.entries):
            idx = start + i
            if idx < len(self.log):
                if self.log[idx].term != e.term:
                    del self.log[idx:]
                    self.log.extend(args.entries[i:])
                    break
            else:
                self.log.extend(args.entries[i:])
                break

        # 4) commitIndex 更新
        last_index = len(self.log) - 1
        if args.leaderCommit > self.commitIndex:
            self.commitIndex = min(args.leaderCommit, last_index)

        self.apply()
        return AppendEntriesResult(host=self.host, term=self.currentTerm, success=True)


    # Candicateからフォロワーへ
    def RequestVote_RPC(self, args: RequestVoteArgs) -> RequestVoteResult:
        
        voteGranted: bool = False
        last_log: LogEntry = self.get_last_log()
        last_index = len(self.log) - 1

        if args.term < self.currentTerm:
            voteGranted = False
            return RequestVoteResult(term = self.currentTerm, voteGranted = voteGranted)
        elif args.term == self.currentTerm:
            pass
        else:
            self.leaderid = None
            self.votedFor = None
            self.currentTerm = args.term
            self.job = Job.follower

        if self.votedFor in (args.candidateId, None) and args.lastLogTerm > last_log.term:
            voteGranted = True
            self.timer_init()
            self.votedFor = args.candidateId
            return RequestVoteResult(term = self.currentTerm, voteGranted = voteGranted)
        
        if self.votedFor in (args.candidateId, None) and args.lastLogTerm == last_log.term and args.lastLogIndex >= last_index:
            voteGranted = True
            self.timer_init()
            self.votedFor = args.candidateId
            return RequestVoteResult(term = self.currentTerm, voteGranted = voteGranted)

        return RequestVoteResult(self.currentTerm, voteGranted)
    
    def apply(self):
        while self.commitIndex > self.lastApplied:
            self.lastApplied += 1
            # self.log[self.lastApplied] が「適用対象のエントリ」
            for cmd in self.log[self.lastApplied].command:
                self.state[cmd.key] = cmd.value


    async def send_AppendEntries_rpc(self, peer: str) -> AppendEntriesResult:
        url = f"http://{peer}/AppendEntries"

        while True:
            nextIndex = self.nextIndex[peer]
            prevLogIndex = nextIndex - 1

            prevLogTerm = self.log[prevLogIndex].term
            entries = self.log[nextIndex:]

            payload = AppendEntriesArgs(
                term=self.currentTerm,
                leaderID=self.host,
                prevLogIndex=prevLogIndex,
                prevLogTerm=prevLogTerm,
                entries=entries,
                leaderCommit=self.commitIndex,
            )

            r = await self.client.post(url, json=payload.model_dump(), timeout=0.05)
            resp = AppendEntriesResult(**r.json())

            # term で負けたら降格
            if resp.term > self.currentTerm:
                self.currentTerm = resp.term
                self.job = Job.follower
                self.votedFor = None
                self.leaderid = None
                return resp

            if resp.success:
                # 送った分だけ進める
                sent = len(entries)
                self.matchIndex[peer] = prevLogIndex + sent
                self.nextIndex[peer] = self.matchIndex[peer] + 1
                return resp
            
            self.nextIndex[peer] = max(1, nextIndex - 1)

    async def elect(self):
        self.currentTerm += 1
        self.votedFor = self.host
        self.timer_init()
        last_log = self.get_last_log()
        payload = RequestVoteArgs(term = self.currentTerm,
                                  candidateId = self.host,
                                  lastLogIndex = len(self.log) - 1,
                                  lastLogTerm = last_log.term)
        tasks = [asyncio.create_task(self.client.post(url = f"http://{i}/RequestVote", json = payload.model_dump())) for i in other_nodes]
        ok: int = 1
        need: int = (len(other_nodes)+1)//2 + 1

        try:
            for done in asyncio.as_completed(tasks):
                try:
                    if self.job != Job.candidate:
                        return
                    r = await done
                    res = RequestVoteResult(**r.json())
                    if res.term > self.currentTerm:
                        self.currentTerm = res.term
                        self.job = Job.follower
                        self.votedFor = None
                        return
                    vote = res.voteGranted

                except Exception:
                    vote = False

                if vote is True:
                    ok += 1
                    if ok >= need:
                        ## 見事選挙勝利！
                        self.job = Job.leader
                        self.leaderid = self.host
                        self.leader_init()
                        break
            
        except asyncio.TimeoutError:
            pass

        finally:
            for t in tasks:
                if not t.done():
                    t.cancel()

    def leader_init(self):
        last_index = len(self.log) - 1
        for i in other_nodes:
            self.nextIndex[i] = last_index + 1
            self.matchIndex[i] = 0

    async def leader(self):
        need = (len(other_nodes) + 1) // 2 + 1

        tasks = [asyncio.create_task(self.send_AppendEntries_rpc(peer)) for peer in other_nodes]

        try:
            for fut in asyncio.as_completed(tasks, timeout=self.heartbeat):
                try:
                    resp = await fut
                    if self.job != Job.leader:
                        return
                except Exception:
                    continue

                if resp.term > self.currentTerm:
                    return

                # ここが本体：matchIndex から commitIndex を進める
                last_index = len(self.log) - 1
                for N in range(last_index, self.commitIndex, -1):
                    if self.log[N].term != self.currentTerm:
                        continue
                    replicated = 1  # self
                    for peer in other_nodes:
                        if self.matchIndex.get(peer, 0) >= N:
                            replicated += 1
                    if replicated >= need:
                        self.commitIndex = N
                        self.apply()
                        break

        except asyncio.TimeoutError:
            pass
        finally:
            for t in tasks:
                if not t.done():
                    t.cancel()

    # 常駐処理
    async def loop(self, stop_event: asyncio.Event):
        try:
            while not stop_event.is_set():
                # timeoutとかheartbeatとか色々
                if self.job != Job.leader:
                    now = time.monotonic()
                    # 選挙開始
                    if now - self.start_time > self.timeout:
                        self.job = Job.candidate
                        try:
                            await asyncio.wait_for(self.elect(), timeout = self.timeout)
                        except asyncio.TimeoutError:
                            continue

                if self.job == Job.leader:
                    await self.leader()

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

@app.get("/get/last_log")
async def get_last_log():
    return node.get_last_log()

@app.get("/get/leader_id")
async def get_leader_id():
    return node.leaderid

@app.get("/get/nextindex")
async def get_nextindex():
    return GetnextIndexResult(host = node.host, nextIndex = node.nextIndex)

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

@app.post("/AppendEntries")
async def test_send_append_entries(args: AppendEntriesArgs):
    return node.AppendEntries_RPC(args)

@app.post("/from_client")
async def client(command: list[Command]):
    if node.job == Job.leader:
        node.add_log_entry(command)
        return node.state
    elif node.leaderid is None:
        raise HTTPException(status_code=503, detail="leader election in progress")
    else:
        try:
            r = await node.client.post(
                f"http://{node.leaderid}:8000/from_client",
                json=[c.model_dump() for c in command],
                timeout=2.0,
            )
            return r.json()

        except Exception:
            raise HTTPException(status_code=503, detail="leader unreachable")

