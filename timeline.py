"""
FLUX Timeline — visualize bytecode execution step by step.

Produces a timeline showing PC movement, register changes, and 
control flow as a program executes.
"""
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Tuple
from enum import Enum


class EventType(Enum):
    EXECUTE = "execute"
    REGISTER_WRITE = "reg_write"
    JUMP = "jump"
    STACK_PUSH = "push"
    STACK_POP = "pop"
    HALT = "halt"


@dataclass
class TimelineEvent:
    cycle: int
    pc: int
    event_type: EventType
    mnemonic: str
    detail: str
    register_snapshot: Dict[int, int]


@dataclass
class Timeline:
    events: List[TimelineEvent]
    total_cycles: int
    registers_written: Dict[int, int]  # reg -> times written
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


OP_NAMES = {
    0x00:"HALT",0x01:"NOP",0x08:"INC",0x09:"DEC",0x0B:"NEG",
    0x0C:"PUSH",0x0D:"POP",0x18:"MOVI",0x19:"ADDI",
    0x20:"ADD",0x21:"SUB",0x22:"MUL",0x23:"DIV",0x24:"MOD",
    0x2C:"CMP_EQ",0x2D:"CMP_LT",0x2E:"CMP_GT",
    0x3A:"MOV",0x3C:"JZ",0x3D:"JNZ",
}


class TimelineTracer:
    """Execute bytecode and record timeline of every event."""
    
    def trace(self, bytecode: List[int], initial_regs: Dict[int, int] = None, 
              max_cycles: int = 1000) -> Timeline:
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
        
        def sb(b): return b - 256 if b > 127 else b
        def snap(): return {i: regs[i] for i in range(16)}
        
        bc = bytes(bytecode)
        
        while pc < len(bc) and cycle < max_cycles:
            op = bc[pc]
            name = OP_NAMES.get(op, f"0x{op:02x}")
            max_pc = max(max_pc, pc)
            
            if op == 0x00:
                events.append(TimelineEvent(cycle, pc, EventType.HALT, name, "program ended", snap()))
                break
            elif op == 0x08:
                rd = bc[pc+1]; old = regs[rd]; regs[rd] += 1
                events.append(TimelineEvent(cycle, pc, EventType.REGISTER_WRITE, name, 
                    f"R{rd}: {old} → {regs[rd]}", snap()))
                regs_written[rd] = regs_written.get(rd, 0) + 1
                pc += 2
            elif op == 0x09:
                rd = bc[pc+1]; old = regs[rd]; regs[rd] -= 1
                events.append(TimelineEvent(cycle, pc, EventType.REGISTER_WRITE, name,
                    f"R{rd}: {old} → {regs[rd]}", snap()))
                regs_written[rd] = regs_written.get(rd, 0) + 1
                pc += 2
            elif op == 0x0C:
                rd = bc[pc+1]; sp -= 1; stack[sp] = regs[rd]
                events.append(TimelineEvent(cycle, pc, EventType.STACK_PUSH, name,
                    f"push R{rd}={regs[rd]}", snap()))
                pc += 2
            elif op == 0x0D:
                rd = bc[pc+1]; val = stack[sp]; regs[rd] = val; sp += 1
                events.append(TimelineEvent(cycle, pc, EventType.STACK_POP, name,
                    f"pop → R{rd}={val}", snap()))
                regs_written[rd] = regs_written.get(rd, 0) + 1
                pc += 2
            elif op == 0x18:
                rd = bc[pc+1]; val = sb(bc[pc+2]); regs[rd] = val
                events.append(TimelineEvent(cycle, pc, EventType.REGISTER_WRITE, name,
                    f"R{rd} = {val}", snap()))
                regs_written[rd] = regs_written.get(rd, 0) + 1
                pc += 3
            elif op == 0x20:
                rd, rs1, rs2 = bc[pc+1], bc[pc+2], bc[pc+3]
                old = regs[rd]; regs[rd] = regs[rs1] + regs[rs2]
                events.append(TimelineEvent(cycle, pc, EventType.REGISTER_WRITE, name,
                    f"R{rd}: {old} → {regs[rd]} (R{rs1}+R{rs2})", snap()))
                regs_written[rd] = regs_written.get(rd, 0) + 1
                pc += 4
            elif op == 0x22:
                rd, rs1, rs2 = bc[pc+1], bc[pc+2], bc[pc+3]
                old = regs[rd]; regs[rd] = regs[rs1] * regs[rs2]
                events.append(TimelineEvent(cycle, pc, EventType.REGISTER_WRITE, name,
                    f"R{rd}: {old} → {regs[rd]} (R{rs1}*R{rs2})", snap()))
                regs_written[rd] = regs_written.get(rd, 0) + 1
                pc += 4
            elif op == 0x2C:
                rd, rs1, rs2 = bc[pc+1], bc[pc+2], bc[pc+3]
                regs[rd] = 1 if regs[rs1] == regs[rs2] else 0
                events.append(TimelineEvent(cycle, pc, EventType.EXECUTE, name,
                    f"R{rd} = (R{rs1}==R{rs2}) = {regs[rd]}", snap()))
                pc += 4
            elif op == 0x3C:
                rd = bc[pc+1]; off = sb(bc[pc+2])
                taken = regs[rd] == 0
                if taken: pc += off; jumps_taken += 1
                else: pc += 4
                events.append(TimelineEvent(cycle, pc, EventType.JUMP, name,
                    f"R{rd}={regs[rd]} {'taken' if taken else 'not taken'} → PC={pc}", snap()))
            elif op == 0x3D:
                rd = bc[pc+1]; off = sb(bc[pc+2])
                taken = regs[rd] != 0
                if taken: pc += off; jumps_taken += 1
                else: pc += 4
                events.append(TimelineEvent(cycle, pc, EventType.JUMP, name,
                    f"R{rd}={regs[rd]} {'taken' if taken else 'not taken'} → PC={pc}", snap()))
            else:
                events.append(TimelineEvent(cycle, pc, EventType.EXECUTE, name, 
                    f"(unhandled)", snap()))
                pc += 1
            
            cycle += 1
        
        return Timeline(events=events, total_cycles=cycle, 
                        registers_written=regs_written, jumps_taken=jumps_taken, max_pc=max_pc)


# ── Tests ──────────────────────────────────────────────

import unittest


class TestTimeline(unittest.TestCase):
    def test_basic_trace(self):
        t = TimelineTracer()
        tl = t.trace([0x18, 0, 42, 0x00])
        self.assertGreater(len(tl.events), 0)
        self.assertEqual(tl.events[-1].event_type, EventType.HALT)
    
    def test_register_changes(self):
        t = TimelineTracer()
        tl = t.trace([0x18, 0, 42, 0x08, 0, 0x00])
        writes = [e for e in tl.events if e.event_type == EventType.REGISTER_WRITE]
        self.assertGreater(len(writes), 0)
    
    def test_jump_tracking(self):
        t = TimelineTracer()
        tl = t.trace([0x18, 0, 0, 0x3C, 0, 1, 0, 0x18, 1, 1, 0x00])
        self.assertGreater(tl.jumps_taken, 0)
    
    def test_stack_events(self):
        t = TimelineTracer()
        tl = t.trace([0x0C, 0, 0x0D, 1, 0x00])
        pushes = [e for e in tl.events if e.event_type == EventType.STACK_PUSH]
        pops = [e for e in tl.events if e.event_type == EventType.STACK_POP]
        self.assertGreater(len(pushes), 0)
        self.assertGreater(len(pops), 0)
    
    def test_text_output(self):
        t = TimelineTracer()
        tl = t.trace([0x18, 0, 42, 0x00])
        text = tl.to_text()
        self.assertIn("MOVI", text)
        self.assertIn("HALT", text)
    
    def test_csv_output(self):
        t = TimelineTracer()
        tl = t.trace([0x18, 0, 42, 0x00])
        csv = tl.to_csv()
        self.assertIn("cycle,pc", csv)
    
    def test_registers_written_count(self):
        t = TimelineTracer()
        tl = t.trace([0x18, 0, 10, 0x18, 0, 20, 0x00])
        self.assertIn(0, tl.registers_written)
        self.assertEqual(tl.registers_written[0], 2)
    
    def test_max_pc(self):
        t = TimelineTracer()
        tl = t.trace([0x18, 0, 42, 0x00])
        self.assertGreater(tl.max_pc, 0)
    
    def test_factorial_trace(self):
        bc = [0x18,0,3, 0x18,1,1, 0x22,1,1,0, 0x09,0, 0x3D,0,0xFA,0, 0x00]
        t = TimelineTracer()
        tl = t.trace(bc)
        self.assertGreater(tl.total_cycles, 5)
        self.assertGreater(tl.jumps_taken, 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
