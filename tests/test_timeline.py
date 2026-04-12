"""Comprehensive tests for flux-timeline module."""
import pytest
from timeline import (
    EventType,
    TimelineEvent,
    Timeline,
    TimelineTracer,
    OP_NAMES,
)


# ── EventType ────────────────────────────────────────────

class TestEventType:
    def test_all_expected_types_exist(self):
        expected = {"execute", "reg_write", "jump", "push", "pop", "halt"}
        actual = {e.value for e in EventType}
        assert actual == expected

    def test_event_type_values_are_strings(self):
        for e in EventType:
            assert isinstance(e.value, str)


# ── TimelineEvent ────────────────────────────────────────

class TestTimelineEvent:
    def test_create_basic_event(self):
        ev = TimelineEvent(
            cycle=0, pc=0, event_type=EventType.HALT,
            mnemonic="HALT", detail="program ended",
            register_snapshot={0: 42},
        )
        assert ev.cycle == 0
        assert ev.pc == 0
        assert ev.event_type == EventType.HALT
        assert ev.mnemonic == "HALT"
        assert ev.detail == "program ended"
        assert ev.register_snapshot == {0: 42}

    def test_event_with_empty_registers(self):
        ev = TimelineEvent(
            cycle=1, pc=5, event_type=EventType.EXECUTE,
            mnemonic="NOP", detail="(unhandled)",
            register_snapshot={},
        )
        assert ev.register_snapshot == {}


# ── Timeline ─────────────────────────────────────────────

class TestTimeline:
    def _make_timeline(self, events=None, **kwargs):
        events = events or []
        defaults = {
            "events": events,
            "total_cycles": len(events),
            "registers_written": {},
            "jumps_taken": 0,
            "max_pc": 0,
        }
        defaults.update(kwargs)
        return Timeline(**defaults)

    def test_to_text_includes_header(self):
        tl = self._make_timeline()
        text = tl.to_text()
        assert "FLUX Execution Timeline" in text
        assert "====" in text

    def test_to_text_shows_events(self):
        ev = TimelineEvent(0, 0, EventType.HALT, "HALT", "ended", {0: 1})
        tl = self._make_timeline(events=[ev])
        text = tl.to_text()
        assert "HALT" in text
        assert "ended" in text

    def test_to_text_shows_summary(self):
        tl = self._make_timeline(total_cycles=42, jumps_taken=3, max_pc=17)
        text = tl.to_text()
        assert "42 cycles" in text
        assert "3 jumps" in text
        assert "0-17" in text

    def test_to_text_hides_zero_registers(self):
        ev = TimelineEvent(0, 0, EventType.HALT, "HALT", "ended", {0: 0, 1: 0})
        tl = self._make_timeline(events=[ev])
        text = tl.to_text()
        # Zero-valued registers should not appear in text output
        assert "R0=" not in text
        assert "R1=" not in text

    def test_to_text_shows_nonzero_registers(self):
        ev = TimelineEvent(0, 0, EventType.HALT, "HALT", "ended", {0: 5, 1: 0})
        tl = self._make_timeline(events=[ev])
        text = tl.to_text()
        assert "R0=5" in text

    def test_to_csv_header(self):
        tl = self._make_timeline()
        csv = tl.to_csv()
        assert csv.startswith("cycle,pc,event,mnemonic,detail")

    def test_to_csv_contains_events(self):
        ev = TimelineEvent(0, 0, EventType.HALT, "HALT", "ended", {})
        tl = self._make_timeline(events=[ev])
        csv = tl.to_csv()
        lines = csv.strip().split("\n")
        assert len(lines) == 2  # header + 1 event
        assert "halt" in lines[1].lower()

    def test_to_csv_multiple_events(self):
        ev1 = TimelineEvent(0, 0, EventType.HALT, "HALT", "ended", {})
        ev2 = TimelineEvent(1, 3, EventType.JUMP, "JZ", "taken", {})
        tl = self._make_timeline(events=[ev1, ev2], total_cycles=2)
        csv = tl.to_csv()
        lines = csv.strip().split("\n")
        assert len(lines) == 3  # header + 2 events

    def test_empty_timeline_to_text(self):
        tl = self._make_timeline(events=[], total_cycles=0)
        text = tl.to_text()
        assert "0 cycles" in text

    def test_empty_timeline_to_csv(self):
        tl = self._make_timeline(events=[], total_cycles=0)
        csv = tl.to_csv()
        lines = csv.strip().split("\n")
        assert len(lines) == 1  # header only


# ── OP_NAMES ─────────────────────────────────────────────

class TestOpNames:
    def test_known_opcodes(self):
        assert OP_NAMES[0x00] == "HALT"
        assert OP_NAMES[0x01] == "NOP"
        assert OP_NAMES[0x08] == "INC"
        assert OP_NAMES[0x09] == "DEC"
        assert OP_NAMES[0x0C] == "PUSH"
        assert OP_NAMES[0x0D] == "POP"
        assert OP_NAMES[0x18] == "MOVI"
        assert OP_NAMES[0x19] == "ADDI"
        assert OP_NAMES[0x20] == "ADD"
        assert OP_NAMES[0x21] == "SUB"
        assert OP_NAMES[0x22] == "MUL"
        assert OP_NAMES[0x23] == "DIV"
        assert OP_NAMES[0x24] == "MOD"
        assert OP_NAMES[0x3A] == "MOV"
        assert OP_NAMES[0x3C] == "JZ"
        assert OP_NAMES[0x3D] == "JNZ"

    def test_unknown_opcode_missing(self):
        assert 0xFF not in OP_NAMES


# ── TimelineTracer ───────────────────────────────────────

class TestTimelineTracer:
    def setup_method(self):
        self.tracer = TimelineTracer()

    # ── Basic tracing ──

    def test_empty_bytecode(self):
        tl = self.tracer.trace([])
        assert tl.total_cycles == 0
        assert tl.events == []
        assert tl.jumps_taken == 0

    def test_single_halt(self):
        tl = self.tracer.trace([0x00])
        assert tl.total_cycles == 0
        assert len(tl.events) == 1
        assert tl.events[0].event_type == EventType.HALT

    def test_movi_halt(self):
        # MOVI R0, 42; HALT
        tl = self.tracer.trace([0x18, 0, 42, 0x00])
        assert tl.total_cycles == 1
        assert tl.events[0].event_type == EventType.REGISTER_WRITE
        assert tl.events[0].mnemonic == "MOVI"
        assert "42" in tl.events[0].detail
        assert tl.events[1].event_type == EventType.HALT

    def test_max_cycles_limit(self):
        # MOVI R0,3; MOVI R1,1; MUL R1,R1,R0; DEC R0; JNZ R0,-6; HALT
        # Factorial loop that would run ~5 cycles normally
        bc = [0x18, 0, 3, 0x18, 1, 1, 0x22, 1, 1, 0, 0x09, 0, 0x3D, 0, 0xFA, 0, 0x00]
        tl = self.tracer.trace(bc, max_cycles=2)
        assert tl.total_cycles <= 2

    # ── Register operations ──

    def test_movi_sets_register(self):
        tl = self.tracer.trace([0x18, 1, 99, 0x00])
        writes = [e for e in tl.events if e.event_type == EventType.REGISTER_WRITE]
        assert len(writes) == 1
        assert writes[0].detail == "R1 = 99"

    def test_movi_signed_byte(self):
        # MOVI R0, -5 (signed: 0xFB = -5)
        tl = self.tracer.trace([0x18, 0, 0xFB, 0x00])
        writes = [e for e in tl.events if e.event_type == EventType.REGISTER_WRITE]
        assert writes[0].detail == "R0 = -5"

    def test_inc(self):
        tl = self.tracer.trace([0x18, 0, 10, 0x08, 0, 0x00])
        writes = [e for e in tl.events if e.event_type == EventType.REGISTER_WRITE]
        assert len(writes) == 2
        assert "R0: 10" in writes[1].detail
        assert "11" in writes[1].detail

    def test_dec(self):
        tl = self.tracer.trace([0x18, 0, 10, 0x09, 0, 0x00])
        writes = [e for e in tl.events if e.event_type == EventType.REGISTER_WRITE]
        assert len(writes) == 2
        assert "9" in writes[1].detail

    def test_add(self):
        # MOVI R0, 3; MOVI R1, 4; ADD R2, R0, R1; HALT
        bc = [0x18, 0, 3, 0x18, 1, 4, 0x20, 2, 0, 1, 0x00]
        tl = self.tracer.trace(bc)
        writes = [e for e in tl.events if e.event_type == EventType.REGISTER_WRITE]
        add_event = writes[-1]
        assert "7" in add_event.detail

    def test_mul(self):
        # MOVI R0, 5; MOVI R1, 6; MUL R2, R0, R1; HALT
        bc = [0x18, 0, 5, 0x18, 1, 6, 0x22, 2, 0, 1, 0x00]
        tl = self.tracer.trace(bc)
        writes = [e for e in tl.events if e.event_type == EventType.REGISTER_WRITE]
        mul_event = writes[-1]
        assert "30" in mul_event.detail

    # ── Stack operations ──

    def test_push_pop(self):
        # MOVI R0, 42; PUSH R0; POP R1; HALT
        bc = [0x18, 0, 42, 0x0C, 0, 0x0D, 1, 0x00]
        tl = self.tracer.trace(bc)
        pushes = [e for e in tl.events if e.event_type == EventType.STACK_PUSH]
        pops = [e for e in tl.events if e.event_type == EventType.STACK_POP]
        assert len(pushes) == 1
        assert len(pops) == 1
        assert "42" in pops[0].detail

    def test_stack_lifo(self):
        # Push R0=10, Push R1=20, Pop R2, Pop R3
        # R2 should get 20, R3 should get 10
        bc = [
            0x18, 0, 10,  # MOVI R0, 10
            0x18, 1, 20,  # MOVI R1, 20
            0x0C, 0,      # PUSH R0
            0x0C, 1,      # PUSH R1
            0x0D, 2,      # POP R2
            0x0D, 3,      # POP R3
            0x00,          # HALT
        ]
        tl = self.tracer.trace(bc)
        pops = [e for e in tl.events if e.event_type == EventType.STACK_POP]
        assert "20" in pops[0].detail  # R2 gets 20 (last pushed)
        assert "10" in pops[1].detail  # R3 gets 10 (first pushed)

    # ── Comparison ──

    def test_cmp_eq_equal(self):
        # MOVI R0, 5; MOVI R1, 5; CMP_EQ R2, R0, R1; HALT
        bc = [0x18, 0, 5, 0x18, 1, 5, 0x2C, 2, 0, 1, 0x00]
        tl = self.tracer.trace(bc)
        cmp_events = [e for e in tl.events if e.mnemonic == "CMP_EQ"]
        assert len(cmp_events) == 1
        assert "= 1" in cmp_events[0].detail  # equal

    def test_cmp_eq_not_equal(self):
        bc = [0x18, 0, 5, 0x18, 1, 7, 0x2C, 2, 0, 1, 0x00]
        tl = self.tracer.trace(bc)
        cmp_events = [e for e in tl.events if e.mnemonic == "CMP_EQ"]
        assert "= 0" in cmp_events[0].detail  # not equal

    # ── Jumps ──

    def test_jz_taken(self):
        # MOVI R0, 0; JZ R0, 1 → jump forward by 1 from end of instruction
        bc = [0x18, 0, 0, 0x3C, 0, 1, 0x18, 1, 99, 0x00]
        tl = self.tracer.trace(bc)
        assert tl.jumps_taken > 0

    def test_jz_not_taken(self):
        # MOVI R0, 5; JZ R0, 100 → not taken
        bc = [0x18, 0, 5, 0x3C, 0, 100, 0x00]
        tl = self.tracer.trace(bc)
        assert tl.jumps_taken == 0

    def test_jnz_taken(self):
        # MOVI R0, 5; JNZ R0, 1 → taken
        bc = [0x18, 0, 5, 0x3D, 0, 1, 0x18, 1, 99, 0x00]
        tl = self.tracer.trace(bc)
        assert tl.jumps_taken > 0

    def test_jnz_not_taken(self):
        # MOVI R0, 0; JNZ R0, 100 → not taken
        bc = [0x18, 0, 0, 0x3D, 0, 100, 0x00]
        tl = self.tracer.trace(bc)
        assert tl.jumps_taken == 0

    # ── NOP / unhandled ──

    def test_nop(self):
        tl = self.tracer.trace([0x01, 0x00])
        assert tl.total_cycles == 1
        assert tl.events[0].mnemonic == "NOP"

    def test_unhandled_opcode(self):
        tl = self.tracer.trace([0xFF, 0x00])
        assert tl.events[0].mnemonic == "0xff"
        assert "unhandled" in tl.events[0].detail

    # ── Tracking ──

    def test_registers_written_count(self):
        bc = [0x18, 0, 10, 0x18, 0, 20, 0x18, 1, 5, 0x00]
        tl = self.tracer.trace(bc)
        assert tl.registers_written[0] == 2
        assert tl.registers_written[1] == 1

    def test_max_pc(self):
        bc = [0x18, 0, 42, 0x18, 1, 99, 0x00]
        tl = self.tracer.trace(bc)
        assert tl.max_pc == 6  # HALT at index 6

    def test_initial_registers(self):
        bc = [0x00]
        tl = self.tracer.trace(bc, initial_regs={0: 100, 5: 200})
        snap = tl.events[0].register_snapshot
        assert snap[0] == 100
        assert snap[5] == 200

    # ── Programs ──

    def test_factorial_program(self):
        """MOVI R0,3; MOVI R1,1; MUL R1,R1,R0; DEC R0; JNZ R0,-6; HALT"""
        bc = [0x18, 0, 3, 0x18, 1, 1, 0x22, 1, 1, 0, 0x09, 0, 0x3D, 0, 0xFA, 0, 0x00]
        tl = self.tracer.trace(bc)
        assert tl.total_cycles > 5
        assert tl.jumps_taken > 0
        # R1 should end up as 3! = 6
        last_reg_event = [e for e in tl.events if e.event_type == EventType.REGISTER_WRITE and e.detail.startswith("R1")][-1]
        assert "6" in last_reg_event.detail

    def test_loop_with_counter(self):
        """Loop 3 times: MOVI R0,3; MOVI R1,0; INC R1; DEC R0; JNZ R0,back_to_INC; HALT"""
        # INC is at PC 6, JNZ is at PC 10, offset = 6 - 10 = -4 = 0xFC
        bc = [0x18, 0, 3, 0x18, 1, 0, 0x08, 1, 0x09, 0, 0x3D, 0, 0xFC, 0x00]
        tl = self.tracer.trace(bc)
        # 2 setup (MOVI) + 3*(INC+DEC+JNZ) = 2 + 9 = 11 cycles, then JNZ not-taken (1 more) = not counted
        assert tl.total_cycles >= 9  # at least 3 iterations
        assert tl.jumps_taken >= 2

    # ── Output formats ──

    def test_to_text_real_program(self):
        tl = self.tracer.trace([0x18, 0, 42, 0x00])
        text = tl.to_text()
        assert "MOVI" in text
        assert "HALT" in text
        assert "42" in text

    def test_to_csv_real_program(self):
        tl = self.tracer.trace([0x18, 0, 42, 0x00])
        csv = tl.to_csv()
        lines = csv.strip().split("\n")
        assert len(lines) == 3  # header + MOVI + HALT
        assert "movi" in lines[1].lower()
        assert "halt" in lines[2].lower()
