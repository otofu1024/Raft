from fastapi import FastAPI
import httpx
import time
import os

from pydantic import BaseModel
from typing import Any

other_nodes = os.environ["PEERS"].split(",")
other_nodes = list(map(lambda x: x.split(":"), other_nodes))

class LogEntry(BaseModel):
    index: int
    term: int
    # command example:
    # x += 1, z -= 3
    command: Any

class AppendEntriesResult(BaseModel):
    term: int
    success: bool

class RequestVoteResult(BaseModel):
    term: int
    voteGranted: bool

class StateMachine(BaseModel):
    x: int
    y: int
    z: int

# 状態と受け取り、送信の関数の設定のみ
class Node():
    def __init__(self):
        entry = StateMachine(x=0, y=0, z=0)
        self.state :StateMachine = entry

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

node = Node()

app = FastAPI()

# Rules for Servers
@app.get("/")
async def raft():
    while True:
        if node.commitIndex > node.lastApplied:
            node.lastApplied += 1
            command = node.log[node.lastApplied]["command"].split(", ")
            node.state = node.log[node.lastApplied]

        return other_nodes

@app.get("/test")
async def main():
    async with httpx.AsyncClient() as client:
            r = await client.get(f'http://n2:8000')
            return (r.status_code, r.text)
