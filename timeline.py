"""
FLUX Timeline — event sourcing, causal ordering, replay, and branching timelines.

Provides a complete fleet activity tracking system:
  - Event sourcing for fleet activity (append-only event log)
  - Causal ordering of events across agents (vector clocks / Lamport timestamps)
  - Timeline replay (reconstruct state at any point in time)
  - Temporal queries (what happened between T1 and T2?)
  - Branching timelines (fork and merge)
  - Timeline compaction (merge/snapshot old events)
"""
from __future__ import annotations

import copy
import hashlib
import json
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple, Set


# ── Enums ────────────────────────────────────────────────────────────────────

class EventType(Enum):
    EXECUTE = "execute"
    REGISTER_WRITE = "reg_write"
    JUMP = "jump"
    STACK_PUSH = "push"
    STACK_POP = "pop"
    HALT = "halt"
    # Fleet-level event types
    AGENT_CREATED = "agent_created"
    AGENT_REMOVED = "agent_removed"
    PROGRAM_DEPLOYED = "program_deployed"
    PROGRAM_SIGNED = "program_signed"
    SIGNATURE_REVOKED = "signature_revoked"
    TIMELINE_FORKED = "timeline_forked"
    TIMELINE_MERGED = "timeline_merged"
    SNAPSHOT_CREATED = "snapshot_created"


# ── Vector Clock ─────────────────────────────────────────────────────────────

@dataclass
class VectorClock:
    """Logical clock for causal ordering across distributed agents."""
    timestamps: Dict[str, int] = field(default_factory=dict)

    def increment(self, agent_id: str) -> "VectorClock":
        """Increment this agent's counter."""
        self.timestamps[agent_id] = self.timestamps.get(agent_id, 0) + 1
        return self

    def update(self, other: "VectorClock") -> "VectorClock":
        """Merge another vector clock into this one (component-wise max)."""
        for agent, ts in other.timestamps.items():
            self.timestamps[agent] = max(self.timestamps.get(agent, 0), ts)
        return self

    def happens_before(self, other: "VectorClock") -> bool:
        """Check if this clock causally precedes the other."""
        # All our timestamps <= other's, at least one strictly less
        at_least_one_less = False
        for agent, ts in self.timestamps.items():
            other_ts = other.timestamps.get(agent, 0)
            if ts > other_ts:
                return False
            if ts < other_ts:
                at_least_one_less = True
        return at_least_one_less

    def is_concurrent(self, other: "VectorClock") -> bool:
        """Check if two events are concurrent (neither happens before the other)."""
        return not self.happens_before(other) and not other.happens_before(self)

    def __str__(self) -> str:
        items = sorted(self.timestamps.items())
        return "{" + ", ".join(f"{a}:{t}" for a, t in items) + "}"

    def to_dict(self) -> dict:
        return dict(self.timestamps)

    @classmethod
    def from_dict(cls, d: dict) -> "VectorClock":
        return cls(timestamps=dict(d))


# ── Timeline Event ───────────────────────────────────────────────────────────

@dataclass
class TimelineEvent:
    """A single event in the fleet timeline with causal ordering."""
    event_id: str
    event_type: EventType
    agent_id: str
    timestamp: float  # wall-clock (seconds since epoch)
    logical_clock: VectorClock = field(default_factory=VectorClock)
    data: Dict[str, Any] = field(default_factory=dict)
    parent_event_ids: List[str] = field(default_factory=list)  # causal dependencies

    def __post_init__(self):
        if not self.event_id:
            self.event_id = uuid.uuid4().hex[:16]

    @property
    def event_hash(self) -> str:
        """Deterministic hash of this event for integrity."""
        raw = json.dumps({
            "event_type": self.event_type.value,
            "agent_id": self.agent_id,
            "timestamp": self.timestamp,
            "logical_clock": self.logical_clock.to_dict(),
            "data": self.data,
            "parent_event_ids": sorted(self.parent_event_ids),
        }, sort_keys=True)
        return hashlib.sha256(raw.encode()).hexdigest()[:16]

    def to_dict(self) -> dict:
        return {
            "event_id": self.event_id,
            "event_type": self.event_type.value,
            "agent_id": self.agent_id,
            "timestamp": self.timestamp,
            "logical_clock": self.logical_clock.to_dict(),
            "data": self.data,
            "parent_event_ids": self.parent_event_ids,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "TimelineEvent":
        return cls(
            event_id=d["event_id"],
            event_type=EventType(d["event_type"]),
            agent_id=d["agent_id"],
            timestamp=d["timestamp"],
            logical_clock=VectorClock.from_dict(d.get("logical_clock", {})),
            data=d.get("data", {}),
            parent_event_ids=d.get("parent_event_ids", []),
        )


# ── Timeline Branch ──────────────────────────────────────────────────────────

@dataclass
class TimelineBranch:
    """A branch in the timeline (for forking/merging)."""
    branch_id: str
    parent_branch_id: Optional[str] = None
    fork_event_id: Optional[str] = None  # event at which this branch forked
    created_at: float = field(default_factory=time.time)
    is_merged: bool = False
    merged_into: Optional[str] = None  # branch_id this was merged into

    def to_dict(self) -> dict:
        return {
            "branch_id": self.branch_id,
            "parent_branch_id": self.parent_branch_id,
            "fork_event_id": self.fork_event_id,
            "created_at": self.created_at,
            "is_merged": self.is_merged,
            "merged_into": self.merged_into,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "TimelineBranch":
        return cls(**d)


# ── Compacted Snapshot ───────────────────────────────────────────────────────

@dataclass
class CompactedSnapshot:
    """A snapshot of timeline state after compaction."""
    snapshot_id: str
    as_of_event_id: str
    as_of_timestamp: float
    state: Dict[str, Any]
    events_compacted: int
    created_at: float = field(default_factory=time.time)

    def to_dict(self) -> dict:
        return {
            "snapshot_id": self.snapshot_id,
            "as_of_event_id": self.as_of_event_id,
            "as_of_timestamp": self.as_of_timestamp,
            "state": self.state,
            "events_compacted": self.events_compacted,
            "created_at": self.created_at,
        }


# ── Fleet Timeline (main engine) ────────────────────────────────────────────

class FleetTimeline:
    """Main event sourcing engine for FLUX fleet activity."""

    def __init__(self, timeline_id: str = None):
        self.timeline_id = timeline_id or uuid.uuid4().hex[:12]
        self.events: List[TimelineEvent] = []
        self.event_index: Dict[str, int] = {}  # event_id -> index
        self.branches: Dict[str, TimelineBranch] = {}
        self.current_branch: str = "main"
        self.snapshots: List[CompactedSnapshot] = []
        self.logical_clock = VectorClock()
        # State projection for replay
        self._state_handlers: Dict[EventType, Callable] = {}

        # Initialize main branch
        self.branches["main"] = TimelineBranch(branch_id="main")

    # ── Event Sourcing ──

    def append_event(self, event_type: EventType, agent_id: str,
                     data: Dict[str, Any] = None,
                     parent_event_ids: List[str] = None) -> TimelineEvent:
        """Append a new event to the timeline with causal ordering."""
        # Increment logical clock for this agent
        self.logical_clock.increment(agent_id)

        # Merge parent clocks for causal ordering
        vc = VectorClock(dict(self.logical_clock.timestamps))
        if parent_event_ids:
            for pid in parent_event_ids:
                parent_event = self.get_event(pid)
                if parent_event:
                    vc.update(parent_event.logical_clock)

        event = TimelineEvent(
            event_id=uuid.uuid4().hex[:16],
            event_type=event_type,
            agent_id=agent_id,
            timestamp=time.time(),
            logical_clock=vc,
            data=data or {},
            parent_event_ids=parent_event_ids or [],
        )

        self.events.append(event)
        self.event_index[event.event_id] = len(self.events) - 1
        return event

    def get_event(self, event_id: str) -> Optional[TimelineEvent]:
        """Look up an event by ID."""
        idx = self.event_index.get(event_id)
        if idx is not None:
            return self.events[idx]
        return None

    @property
    def event_count(self) -> int:
        return len(self.events)

    @property
    def latest_event(self) -> Optional[TimelineEvent]:
        return self.events[-1] if self.events else None

    # ── Temporal Queries ──

    def query_range(self, start_time: float, end_time: float,
                    agent_id: str = None,
                    event_type: EventType = None) -> List[TimelineEvent]:
        """Query events within a time range with optional filters."""
        results = []
        for event in self.events:
            if not (start_time <= event.timestamp <= end_time):
                continue
            if agent_id and event.agent_id != agent_id:
                continue
            if event_type and event.event_type != event_type:
                continue
            results.append(event)
        return results

    def query_by_agent(self, agent_id: str) -> List[TimelineEvent]:
        """Get all events for a specific agent."""
        return [e for e in self.events if e.agent_id == agent_id]

    def query_by_type(self, event_type: EventType) -> List[TimelineEvent]:
        """Get all events of a specific type."""
        return [e for e in self.events if e.event_type == event_type]

    def query_since(self, event_id: str) -> List[TimelineEvent]:
        """Get all events that happened after a given event (causally)."""
        ref_event = self.get_event(event_id)
        if not ref_event:
            return []
        results = []
        for event in self.events:
            if event.event_id == event_id:
                continue
            if ref_event.logical_clock.happens_before(event.logical_clock):
                results.append(event)
        return results

    def query_concurrent(self, event_id: str) -> List[TimelineEvent]:
        """Get all events concurrent with a given event."""
        ref_event = self.get_event(event_id)
        if not ref_event:
            return []
        return [e for e in self.events if e.event_id != event_id
                and ref_event.logical_clock.is_concurrent(e.logical_clock)]

    # ── Timeline Replay ──

    def register_state_handler(self, event_type: EventType,
                               handler: Callable[[TimelineEvent, Dict[str, Any]], Dict[str, Any]]):
        """Register a state projection handler for an event type."""
        self._state_handlers[event_type] = handler

    def replay(self, up_to_event_id: str = None,
               state: Dict[str, Any] = None) -> Dict[str, Any]:
        """Replay events to reconstruct state. Can stop at a specific event."""
        state = copy.deepcopy(state) if state else {}
        target_events = self.events

        if up_to_event_id:
            idx = self.event_index.get(up_to_event_id)
            if idx is not None:
                target_events = self.events[:idx + 1]
            else:
                target_events = []

        for event in target_events:
            handler = self._state_handlers.get(event.event_type)
            if handler:
                state = handler(event, state)

        return state

    def get_state_at(self, timestamp: float) -> Dict[str, Any]:
        """Reconstruct state as of a specific wall-clock timestamp."""
        state = {}
        for event in self.events:
            if event.timestamp > timestamp:
                break
            handler = self._state_handlers.get(event.event_type)
            if handler:
                state = handler(event, state)
        return state

    # ── Branching Timelines ──

    def fork(self, branch_name: str, event_id: str = None) -> str:
        """Create a new branch. Optionally fork from a specific event."""
        if branch_name in self.branches:
            raise ValueError(f"Branch '{branch_name}' already exists")

        parent_event = self.get_event(event_id) if event_id else self.latest_event

        branch = TimelineBranch(
            branch_id=branch_name,
            parent_branch_id=self.current_branch,
            fork_event_id=parent_event.event_id if parent_event else None,
        )
        self.branches[branch_name] = branch
        self.current_branch = branch_name

        # Record the fork event
        self.append_event(EventType.TIMELINE_FORKED, "system", {
            "new_branch": branch_name,
            "parent_branch": branch.parent_branch_id,
            "fork_event_id": branch.fork_event_id,
        })

        return branch_name

    def merge(self, source_branch: str, target_branch: str = None) -> Optional[str]:
        """Merge a branch into another. Returns merge event ID."""
        if source_branch not in self.branches:
            return None
        source = self.branches[source_branch]
        target_name = target_branch or source.parent_branch_id or "main"

        if target_name not in self.branches:
            return None

        # Mark source as merged
        source.is_merged = True
        source.merged_into = target_name

        # Switch to target branch
        self.current_branch = target_name

        # Record merge event
        event = self.append_event(EventType.TIMELINE_MERGED, "system", {
            "source_branch": source_branch,
            "target_branch": target_name,
            "fork_event_id": source.fork_event_id,
        })

        return event.event_id

    def get_branch(self, branch_id: str) -> Optional[TimelineBranch]:
        return self.branches.get(branch_id)

    def list_branches(self) -> List[TimelineBranch]:
        return list(self.branches.values())

    # ── Timeline Compaction ──

    def compact(self, keep_last: int = 100) -> Optional[CompactedSnapshot]:
        """Compact old events into a snapshot, keeping the last N events."""
        if len(self.events) <= keep_last:
            return None

        cutoff_idx = len(self.events) - keep_last
        events_to_compact = self.events[:cutoff_idx]
        last_compacted = events_to_compact[-1]

        # Build state from compacted events
        state = {}
        for event in events_to_compact:
            handler = self._state_handlers.get(event.event_type)
            if handler:
                state = handler(event, state)

        snapshot = CompactedSnapshot(
            snapshot_id=uuid.uuid4().hex[:12],
            as_of_event_id=last_compacted.event_id,
            as_of_timestamp=last_compacted.timestamp,
            state=state,
            events_compacted=len(events_to_compact),
        )
        self.snapshots.append(snapshot)

        # Record compaction event
        self.append_event(EventType.SNAPSHOT_CREATED, "system", {
            "snapshot_id": snapshot.snapshot_id,
            "events_compacted": snapshot.events_compacted,
        })

        return snapshot

    def replay_with_snapshots(self, state: Dict[str, Any] = None) -> Dict[str, Any]:
        """Replay using the latest snapshot as a starting point."""
        state = copy.deepcopy(state) if state else {}

        if self.snapshots:
            latest_snap = self.snapshots[-1]
            state = copy.deepcopy(latest_snap.state)
            # Find the index after the snapshot
            snap_event_idx = self.event_index.get(latest_snap.as_of_event_id, -1)
            events_to_replay = self.events[snap_event_idx + 1:]
        else:
            events_to_replay = self.events

        for event in events_to_replay:
            handler = self._state_handlers.get(event.event_type)
            if handler:
                state = handler(event, state)

        return state

    # ── Causal Analysis ──

    def causal_chain(self, event_id: str) -> List[TimelineEvent]:
        """Walk the causal chain from an event back to its roots."""
        chain = []
        visited = set()
        queue = [event_id]
        while queue:
            eid = queue.pop(0)
            if eid in visited:
                continue
            visited.add(eid)
            event = self.get_event(eid)
            if event:
                chain.append(event)
                queue.extend(event.parent_event_ids)
        return chain

    def detect_conflicts(self) -> List[Tuple[TimelineEvent, TimelineEvent]]:
        """Detect concurrent events that may represent conflicts."""
        conflicts = []
        for i, e1 in enumerate(self.events):
            for e2 in self.events[i + 1:]:
                if e1.agent_id != e2.agent_id and e1.logical_clock.is_concurrent(e2.logical_clock):
                    # Only flag same-type events as potential conflicts
                    if e1.event_type == e2.event_type:
                        conflicts.append((e1, e2))
        return conflicts

    # ── Serialization ──

    def export(self) -> dict:
        return {
            "timeline_id": self.timeline_id,
            "events": [e.to_dict() for e in self.events],
            "branches": {bid: b.to_dict() for bid, b in self.branches.items()},
            "current_branch": self.current_branch,
            "snapshots": [s.to_dict() for s in self.snapshots],
        }

    def export_events_json(self) -> str:
        """Export events as JSON string."""
        return json.dumps([e.to_dict() for e in self.events], indent=2)

    @classmethod
    def from_export(cls, data: dict) -> "FleetTimeline":
        tl = cls(timeline_id=data.get("timeline_id"))
        for ed in data.get("events", []):
            event = TimelineEvent.from_dict(ed)
            tl.events.append(event)
            tl.event_index[event.event_id] = len(tl.events) - 1
        for bid, bd in data.get("branches", {}).items():
            tl.branches[bid] = TimelineBranch.from_dict(bd)
        tl.current_branch = data.get("current_branch", "main")
        for sd in data.get("snapshots", []):
            tl.snapshots.append(CompactedSnapshot(**sd))
        return tl


# ── Legacy Execution Tracer (kept for backward compatibility) ────────────────

OP_NAMES = {
    0x00: "HALT", 0x01: "NOP", 0x08: "INC", 0x09: "DEC", 0x0B: "NEG",
    0x0C: "PUSH", 0x0D: "POP", 0x18: "MOVI", 0x19: "ADDI",
    0x20: "ADD", 0x21: "SUB", 0x22: "MUL", 0x23: "DIV", 0x24: "MOD",
    0x2C: "CMP_EQ", 0x2D: "CMP_LT", 0x2E: "CMP_GT",
    0x3A: "MOV", 0x3C: "JZ", 0x3D: "JNZ",
}


@dataclass
class LegacyTimelineEvent:
    cycle: int
    pc: int
    event_type: EventType
    mnemonic: str
    detail: str
    register_snapshot: Dict[int, int]


@dataclass
class LegacyTimeline:
    events: List[LegacyTimelineEvent]
    total_cycles: int
    registers_written: Dict[int, int]
    jumps_taken: int
    max_pc: int

    def to_text(self) -> str:
        lines = ["FLUX Execution Timeline", "=" * 60]
        for e in self.events:
            regs_str = " ".join(f"R{i}={v}" for i, v in sorted(e.register_snapshot.items()) if v != 0)
            lines.append(f"[{e.cycle:4d}] PC={e.pc:3d} {e.mnemonic:10s} {e.detail}")
            if regs_str:
                lines.append(f"       {regs_str}")
        lines.append(f"\nTotal: {self.total_cycles} cycles, {self.jumps_taken} jumps, PC range 0-{self.max_pc}")
        return "\n".join(lines)

    def to_csv(self) -> str:
        lines = ["cycle,pc,event,mnemonic,detail"]
        for e in self.events:
            lines.append(f"{e.cycle},{e.pc},{e.event_type.value},{e.mnemonic},{e.detail}")
        return "\n".join(lines)


class TimelineTracer:
    """Legacy execution tracer — traces bytecode execution step by step."""

    def trace(self, bytecode: List[int], initial_regs: Dict[int, int] = None,
              max_cycles: int = 1000) -> LegacyTimeline:
        regs = [0] * 64
        stack = [0] * 4096
        sp = 4096
        pc = 0
        events = []
        cycle = 0
        regs_written = {}
        jumps_taken = 0
        max_pc = 0

        if initial_regs:
            for k, v in initial_regs.items():
                regs[k] = v

        def sb(b):
            return b - 256 if b > 127 else b

        def snap():
            return {i: regs[i] for i in range(16)}

        bc = bytes(bytecode)

        while pc < len(bc) and cycle < max_cycles:
            op = bc[pc]
            name = OP_NAMES.get(op, f"0x{op:02x}")
            max_pc = max(max_pc, pc)

            if op == 0x00:
                events.append(LegacyTimelineEvent(cycle, pc, EventType.HALT, name, "program ended", snap()))
                break
            elif op == 0x08:
                rd = bc[pc + 1]; old = regs[rd]; regs[rd] += 1
                events.append(LegacyTimelineEvent(cycle, pc, EventType.REGISTER_WRITE, name,
                                                  f"R{rd}: {old} → {regs[rd]}", snap()))
                regs_written[rd] = regs_written.get(rd, 0) + 1
                pc += 2
            elif op == 0x09:
                rd = bc[pc + 1]; old = regs[rd]; regs[rd] -= 1
                events.append(LegacyTimelineEvent(cycle, pc, EventType.REGISTER_WRITE, name,
                                                  f"R{rd}: {old} → {regs[rd]}", snap()))
                regs_written[rd] = regs_written.get(rd, 0) + 1
                pc += 2
            elif op == 0x0C:
                rd = bc[pc + 1]; sp -= 1; stack[sp] = regs[rd]
                events.append(LegacyTimelineEvent(cycle, pc, EventType.STACK_PUSH, name,
                                                  f"push R{rd}={regs[rd]}", snap()))
                pc += 2
            elif op == 0x0D:
                rd = bc[pc + 1]; val = stack[sp]; regs[rd] = val; sp += 1
                events.append(LegacyTimelineEvent(cycle, pc, EventType.STACK_POP, name,
                                                  f"pop → R{rd}={val}", snap()))
                regs_written[rd] = regs_written.get(rd, 0) + 1
                pc += 2
            elif op == 0x18:
                rd = bc[pc + 1]; val = sb(bc[pc + 2]); regs[rd] = val
                events.append(LegacyTimelineEvent(cycle, pc, EventType.REGISTER_WRITE, name,
                                                  f"R{rd} = {val}", snap()))
                regs_written[rd] = regs_written.get(rd, 0) + 1
                pc += 3
            elif op == 0x20:
                rd, rs1, rs2 = bc[pc + 1], bc[pc + 2], bc[pc + 3]
                old = regs[rd]; regs[rd] = regs[rs1] + regs[rs2]
                events.append(LegacyTimelineEvent(cycle, pc, EventType.REGISTER_WRITE, name,
                                                  f"R{rd}: {old} → {regs[rd]} (R{rs1}+R{rs2})", snap()))
                regs_written[rd] = regs_written.get(rd, 0) + 1
                pc += 4
            elif op == 0x22:
                rd, rs1, rs2 = bc[pc + 1], bc[pc + 2], bc[pc + 3]
                old = regs[rd]; regs[rd] = regs[rs1] * regs[rs2]
                events.append(LegacyTimelineEvent(cycle, pc, EventType.REGISTER_WRITE, name,
                                                  f"R{rd}: {old} → {regs[rd]} (R{rs1}*R{rs2})", snap()))
                regs_written[rd] = regs_written.get(rd, 0) + 1
                pc += 4
            elif op == 0x2C:
                rd, rs1, rs2 = bc[pc + 1], bc[pc + 2], bc[pc + 3]
                regs[rd] = 1 if regs[rs1] == regs[rs2] else 0
                events.append(LegacyTimelineEvent(cycle, pc, EventType.EXECUTE, name,
                                                  f"R{rd} = (R{rs1}==R{rs2}) = {regs[rd]}", snap()))
                pc += 4
            elif op == 0x3C:
                rd = bc[pc + 1]; off = sb(bc[pc + 2])
                taken = regs[rd] == 0
                if taken:
                    pc += off; jumps_taken += 1
                else:
                    pc += 4
                events.append(LegacyTimelineEvent(cycle, pc, EventType.JUMP, name,
                                                  f"R{rd}={regs[rd]} {'taken' if taken else 'not taken'} → PC={pc}", snap()))
            elif op == 0x3D:
                rd = bc[pc + 1]; off = sb(bc[pc + 2])
                taken = regs[rd] != 0
                if taken:
                    pc += off; jumps_taken += 1
                else:
                    pc += 4
                events.append(LegacyTimelineEvent(cycle, pc, EventType.JUMP, name,
                                                  f"R{rd}={regs[rd]} {'taken' if taken else 'not taken'} → PC={pc}", snap()))
            else:
                events.append(LegacyTimelineEvent(cycle, pc, EventType.EXECUTE, name,
                                                  f"(unhandled)", snap()))
                pc += 1

            cycle += 1

        return LegacyTimeline(events=events, total_cycles=cycle,
                              registers_written=regs_written, jumps_taken=jumps_taken, max_pc=max_pc)
