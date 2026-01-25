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

class GetnextIndexResult(BaseModel):
    host: str
    nextIndex: int

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
        self.log: list[LogEntry] = [LogEntry(index=0, term=0, command=[])]


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
        self.nextIndex: dict[str, int] = {}
        self.matchIndex: dict[str, int] = {}
        
        for i in other_nodes:
            self.nextIndex[i] = self.get_last_log().index + 1
            self.matchIndex[i] = 0

    def get_last_log(self) -> LogEntry:
        return self.log[-1]
    
    def add_log_entry(self, command: list[Command]):
        index = len(self.log)
        term = self.currentTerm
        self.log.append(LogEntry(index=index, term=term, command=command))

    
    def AppendEntries_RPC(self, args: AppendEntriesArgs) -> AppendEntriesResult:
        self.timer_init()

        # term check
        if args.term < self.currentTerm:
            return AppendEntriesResult(term=self.currentTerm, success=False)

        if args.term > self.currentTerm:
            self.currentTerm = args.term
            self.votedFor = None

        self.leaderid = args.leaderID
        self.job = Job.follower

        # prevLog の整合性チェック
        # prevLogIndex==0 のときはダミーを参照
        if args.prevLogIndex >= len(self.log):
            return AppendEntriesResult(term=self.currentTerm, success=False)

        if self.log[args.prevLogIndex].term != args.prevLogTerm:
            return AppendEntriesResult(term=self.currentTerm, success=False)

        # entries を反映
        for e in args.entries:
            if e.index < len(self.log):
                if self.log[e.index].term != e.term:
                    # 衝突 -> そこから先を捨てる
                    del self.log[e.index:]
                    self.log.extend(args.entries[e.index:])
            else:
                # ちょうど末尾に続くなら append
                # （この前提だと e.index は len(self.log) と一致するはず）
                self.log.append(e)

        # 4) commitIndex 更新
        last_index = len(self.log) - 1
        if args.leaderCommit > self.commitIndex:
            self.commitIndex = min(args.leaderCommit, last_index)

        self.apply()
        return AppendEntriesResult(term=self.currentTerm, success=True)


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

            # prevLogIndex は 0 以上である必要がある（ダミーがあるので 0 はOK）
            if prevLogIndex < 0:
                prevLogIndex = 0
                self.nextIndex[peer] = 1
                nextIndex = 1

            # 自分の log が短すぎて prev を参照できないなら nextIndex を詰める
            if prevLogIndex >= len(self.log):
                self.nextIndex[peer] = max(1, len(self.log) - 1)
                continue

            prevLogTerm = self.log[prevLogIndex].term
            entries = self.log[nextIndex:]  # これが「nextIndex からのログ」

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

            # 失敗: nextIndex を下げて再試行（1未満にしない）
            self.nextIndex[peer] = max(1, nextIndex - 1)



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
                        self.leader_init()
                        break
            
        except asyncio.TimeoutError:
            pass

        finally:
            for t in tasks:
                if not t.done():
                    t.cancel()

    async def leader_init(self):
        last_log = self.get_last_log()
        for i in other_nodes:
            self.nextIndex[i] = last_log.index + 1
            self.matchIndex[i] = 0

    # Leader処理
    async def leader(self):
        last_index = len(self.log) - 1
        tasks = []
        for peer in other_nodes:
            if last_index >= self.nextIndex[peer]:
                tasks.append(asyncio.create_task(self.send_AppendEntries_rpc(peer)))
            else:
                # heartbeat を送りたいなら、entries=[] で send_AppendEntries_rpc を呼んでもOK
                tasks.append(asyncio.create_task(self.send_AppendEntries_rpc(peer)))

        # まとめて待つ（待ちたくないなら as_completed にする）
        await asyncio.gather(*tasks, return_exceptions=True)


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

                if node.job == Job.leader:
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

@app.post("/update/Indexies")
async def update_indexies(args: team_index):
    node.nextIndex = args.nextIndex
    node.matchindex = args.matchIndex
    return {node.nextIndex, node.matchindex}

@app.post("/RequestVote")
async def vote(args: RequestVoteArgs):
    return node.RequestVote_RPC(args)

@app.post("/AppendEntries")
async def test_send_append_entries(args: AppendEntriesArgs):
    return node.AppendEntries_RPC(args)

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