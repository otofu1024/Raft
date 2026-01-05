from pydantic import BaseModel
from typing import Optional, Any

class AppendEntriesResult(BaseModel):
    term: int
    seccess: bool

class Node():
    def __init__(self):
        # Persistent state on all services
        self.currentTerm: int = 0
        self.votedFor: str | None = None
        self.log: dict = {{"term":int | None,
                          "command":Any | None}}

        # Volatile state on all services
        self.commitIndex: int = 0
        self.lastApplied: int = 0

        # Volatile state on leaders
        self.leader_init()

    def leader_init(self):
        self.nextIndex: dict = {}
        self.matchindex: dict = {}

    def AppendEntries_RPC(self,
                          term: int,
                          leaderID: str,
                          prevLogIndex: int,
                          prevlogTerm: int,
                          entries: dict,
                          leaderCommit: int):
        
        if term < self.currentTerm:
            return AppendEntriesResult(term = self.currentTerm, seccess = False)
        
        elif not(len(self.log) == prevLogIndex or self.log[-1]["term"] == prevlogTerm):
            return AppendEntriesResult(term = self.currentTerm, seccess = False)
        
        elif len(entries) == len(self.log) and not(self.currentTerm == term):
            self.log



        return AppendEntriesResult()
    

node = Node()