"""
FLUX Timeline — Comprehensive test suite.
Tests: event sourcing, vector clocks, causal ordering, replay,
       temporal queries, branching, merging, compaction, conflicts,
       legacy tracer backward compatibility, serialization.
"""
import time
import unittest

from timeline import (
    CompactedSnapshot, EventType, FleetTimeline, LegacyTimeline,
    TimelineBranch, TimelineEvent, TimelineTracer, VectorClock,
)


class TestVectorClock(unittest.TestCase):
    """Test vector clock for causal ordering."""

    def test_increment(self):
        vc = VectorClock()
        vc.increment("agent-1")
        self.assertEqual(vc.timestamps["agent-1"], 1)
        vc.increment("agent-1")
        self.assertEqual(vc.timestamps["agent-1"], 2)

    def test_update_merge(self):
        vc1 = VectorClock({"agent-1": 3, "agent-2": 1})
        vc2 = VectorClock({"agent-1": 1, "agent-2": 5})
        vc1.update(vc2)
        self.assertEqual(vc1.timestamps["agent-1"], 3)
        self.assertEqual(vc1.timestamps["agent-2"], 5)

    def test_happens_before(self):
        vc1 = VectorClock({"a": 1, "b": 2})
        vc2 = VectorClock({"a": 2, "b": 2})
        self.assertTrue(vc1.happens_before(vc2))
        self.assertFalse(vc2.happens_before(vc1))

    def test_not_happens_before(self):
        vc1 = VectorClock({"a": 2, "b": 1})
        vc2 = VectorClock({"a": 1, "b": 2})
        self.assertFalse(vc1.happens_before(vc2))
        self.assertFalse(vc2.happens_before(vc1))

    def test_is_concurrent(self):
        vc1 = VectorClock({"a": 2, "b": 1})
        vc2 = VectorClock({"a": 1, "b": 2})
        self.assertTrue(vc1.is_concurrent(vc2))
        self.assertTrue(vc2.is_concurrent(vc1))

    def test_sequential_not_concurrent(self):
        vc1 = VectorClock({"a": 1})
        vc2 = VectorClock({"a": 2})
        self.assertFalse(vc1.is_concurrent(vc2))

    def test_str_representation(self):
        vc = VectorClock({"agent-2": 1, "agent-1": 3})
        s = str(vc)
        self.assertIn("agent-1:3", s)
        self.assertIn("agent-2:1", s)

    def test_serialization(self):
        vc = VectorClock({"a": 5, "b": 3})
        d = vc.to_dict()
        restored = VectorClock.from_dict(d)
        self.assertEqual(restored.timestamps, vc.timestamps)

    def test_empty_clocks(self):
        vc1 = VectorClock()
        vc2 = VectorClock()
        self.assertFalse(vc1.happens_before(vc2))
        self.assertFalse(vc2.happens_before(vc1))
        # Empty clocks are technically concurrent (no causal relation)
        self.assertTrue(vc1.is_concurrent(vc2))


class TestTimelineEvent(unittest.TestCase):
    """Test event data class."""

    def test_create_event(self):
        event = TimelineEvent(
            event_id="evt-001", event_type=EventType.AGENT_CREATED,
            agent_id="agent-1", timestamp=1700000000.0,
        )
        self.assertEqual(event.event_id, "evt-001")
        self.assertEqual(event.event_type, EventType.AGENT_CREATED)

    def test_event_hash_deterministic(self):
        e1 = TimelineEvent(
            event_id="e1", event_type=EventType.PROGRAM_DEPLOYED,
            agent_id="a1", timestamp=100.0, data={"program": "p1"},
        )
        e2 = TimelineEvent(
            event_id="e2", event_type=EventType.PROGRAM_DEPLOYED,
            agent_id="a1", timestamp=100.0, data={"program": "p1"},
        )
        # Same content produces same hash
        self.assertEqual(e1.event_hash, e2.event_hash)

    def test_event_hash_different(self):
        e1 = TimelineEvent(
            event_id="e1", event_type=EventType.PROGRAM_DEPLOYED,
            agent_id="a1", timestamp=100.0, data={"v": 1},
        )
        e2 = TimelineEvent(
            event_id="e2", event_type=EventType.PROGRAM_DEPLOYED,
            agent_id="a1", timestamp=100.0, data={"v": 2},
        )
        self.assertNotEqual(e1.event_hash, e2.event_hash)

    def test_serialization_roundtrip(self):
        vc = VectorClock({"a": 1})
        event = TimelineEvent(
            event_id="evt-001", event_type=EventType.SIGNATURE_REVOKED,
            agent_id="agent-2", timestamp=200.0,
            logical_clock=vc, data={"reason": "compromised"},
            parent_event_ids=["parent-1"],
        )
        d = event.to_dict()
        restored = TimelineEvent.from_dict(d)
        self.assertEqual(restored.event_id, "evt-001")
        self.assertEqual(restored.event_type, EventType.SIGNATURE_REVOKED)
        self.assertEqual(restored.agent_id, "agent-2")
        self.assertEqual(restored.logical_clock.timestamps, {"a": 1})
        self.assertEqual(restored.parent_event_ids, ["parent-1"])


class TestEventSourcing(unittest.TestCase):
    """Test basic event sourcing append and retrieval."""

    def test_append_event(self):
        tl = FleetTimeline("test-1")
        event = tl.append_event(EventType.AGENT_CREATED, "system", {"name": "Alice"})
        self.assertEqual(tl.event_count, 1)
        self.assertEqual(event.agent_id, "system")
        self.assertIsNotNone(event.event_id)

    def test_get_event(self):
        tl = FleetTimeline("test-2")
        event = tl.append_event(EventType.AGENT_CREATED, "agent-1")
        fetched = tl.get_event(event.event_id)
        self.assertIsNotNone(fetched)
        self.assertEqual(fetched.event_id, event.event_id)

    def test_get_nonexistent_event(self):
        tl = FleetTimeline("test-3")
        self.assertIsNone(tl.get_event("nonexistent"))

    def test_latest_event(self):
        tl = FleetTimeline("test-4")
        self.assertIsNone(tl.latest_event)
        tl.append_event(EventType.AGENT_CREATED, "a1")
        tl.append_event(EventType.PROGRAM_DEPLOYED, "a2")
        self.assertEqual(tl.latest_event.event_type, EventType.PROGRAM_DEPLOYED)

    def test_multiple_appends(self):
        tl = FleetTimeline("test-5")
        for i in range(50):
            tl.append_event(EventType.PROGRAM_DEPLOYED, f"agent-{i % 5}")
        self.assertEqual(tl.event_count, 50)

    def test_event_has_logical_clock(self):
        tl = FleetTimeline("test-6")
        e1 = tl.append_event(EventType.AGENT_CREATED, "a1")
        e2 = tl.append_event(EventType.AGENT_CREATED, "a1")
        self.assertGreater(e2.logical_clock.timestamps["a1"], e1.logical_clock.timestamps["a1"])

    def test_causal_parent_merging(self):
        tl = FleetTimeline("test-7")
        e1 = tl.append_event(EventType.AGENT_CREATED, "a1")
        e2 = tl.append_event(EventType.AGENT_CREATED, "a2")
        # e3 depends on both e1 and e2
        e3 = tl.append_event(EventType.PROGRAM_DEPLOYED, "a3",
                             parent_event_ids=[e1.event_id, e2.event_id])
        # e3's clock should have max of both parents
        self.assertIn("a1", e3.logical_clock.timestamps)
        self.assertIn("a2", e3.logical_clock.timestamps)


class TestTemporalQueries(unittest.TestCase):
    """Test temporal query operations."""

    def setUp(self):
        self.tl = FleetTimeline("temporal-test")
        self.t0 = time.time()
        self.tl.append_event(EventType.AGENT_CREATED, "a1", {"step": 1})
        time.sleep(0.01)
        self.t1 = time.time()
        self.tl.append_event(EventType.PROGRAM_DEPLOYED, "a1", {"step": 2})
        time.sleep(0.01)
        self.t2 = time.time()
        self.tl.append_event(EventType.PROGRAM_DEPLOYED, "a2", {"step": 3})
        time.sleep(0.01)
        self.t3 = time.time()
        self.tl.append_event(EventType.PROGRAM_SIGNED, "a1", {"step": 4})

    def test_query_range_all(self):
        results = self.tl.query_range(self.t0 - 0.001, self.t3 + 0.001)
        self.assertEqual(len(results), 4)

    def test_query_range_partial(self):
        # Use event timestamps directly for precision
        events = self.tl.events
        results = self.tl.query_range(events[1].timestamp, events[2].timestamp)
        self.assertGreaterEqual(len(results), 2)

    def test_query_range_by_agent(self):
        results = self.tl.query_range(self.t0 - 0.001, self.t3 + 0.001, agent_id="a2")
        self.assertEqual(len(results), 1)

    def test_query_range_by_type(self):
        results = self.tl.query_range(self.t0 - 0.001, self.t3 + 0.001,
                                      event_type=EventType.PROGRAM_DEPLOYED)
        self.assertEqual(len(results), 2)

    def test_query_by_agent(self):
        results = self.tl.query_by_agent("a1")
        self.assertEqual(len(results), 3)

    def test_query_by_type(self):
        results = self.tl.query_by_type(EventType.PROGRAM_SIGNED)
        self.assertEqual(len(results), 1)

    def test_query_since(self):
        first_event = self.tl.events[0]
        results = self.tl.query_since(first_event.event_id)
        self.assertGreater(len(results), 0)


class TestTimelineReplay(unittest.TestCase):
    """Test timeline replay and state reconstruction."""

    def setUp(self):
        self.tl = FleetTimeline("replay-test")
        self.tl.register_state_handler(EventType.AGENT_CREATED,
                                       lambda e, s: {**s, "agents": s.get("agents", []) + [e.data.get("name")]})
        self.tl.register_state_handler(EventType.PROGRAM_DEPLOYED,
                                       lambda e, s: {**s, "programs": s.get("programs", 0) + 1})

    def test_full_replay(self):
        self.tl.append_event(EventType.AGENT_CREATED, "system", {"name": "Alice"})
        self.tl.append_event(EventType.AGENT_CREATED, "system", {"name": "Bob"})
        self.tl.append_event(EventType.PROGRAM_DEPLOYED, "a1")
        state = self.tl.replay()
        self.assertEqual(len(state["agents"]), 2)
        self.assertEqual(state["programs"], 1)

    def test_partial_replay(self):
        e1 = self.tl.append_event(EventType.AGENT_CREATED, "system", {"name": "Alice"})
        e2 = self.tl.append_event(EventType.AGENT_CREATED, "system", {"name": "Bob"})
        e3 = self.tl.append_event(EventType.PROGRAM_DEPLOYED, "a1")
        # Replay only up to e2
        state = self.tl.replay(up_to_event_id=e2.event_id)
        self.assertEqual(len(state["agents"]), 2)
        self.assertNotIn("programs", state)

    def test_get_state_at_timestamp(self):
        e1 = self.tl.append_event(EventType.AGENT_CREATED, "system", {"name": "Alice"})
        time.sleep(0.01)
        e2 = self.tl.append_event(EventType.AGENT_CREATED, "system", {"name": "Bob"})
        time.sleep(0.01)
        e3 = self.tl.append_event(EventType.PROGRAM_DEPLOYED, "a1")

        # State at time between e1 and e2
        mid_time = (e1.timestamp + e2.timestamp) / 2
        state = self.tl.get_state_at(mid_time)
        self.assertEqual(len(state["agents"]), 1)

    def test_empty_replay(self):
        state = self.tl.replay()
        self.assertEqual(state, {})

    def test_no_handler(self):
        self.tl.append_event(EventType.SIGNATURE_REVOKED, "admin", {"reason": "test"})
        state = self.tl.replay()  # No handler registered for SIGNATURE_REVOKED
        self.assertEqual(state, {})


class TestBranchingTimelines(unittest.TestCase):
    """Test fork and merge operations."""

    def setUp(self):
        self.tl = FleetTimeline("branch-test")

    def test_main_branch_exists(self):
        branch = self.tl.get_branch("main")
        self.assertIsNotNone(branch)
        self.assertEqual(branch.branch_id, "main")

    def test_fork(self):
        self.tl.append_event(EventType.AGENT_CREATED, "a1")
        self.tl.fork("feature-x")
        branch = self.tl.get_branch("feature-x")
        self.assertIsNotNone(branch)
        self.assertEqual(branch.parent_branch_id, "main")
        self.assertEqual(self.tl.current_branch, "feature-x")

    def test_fork_records_event(self):
        self.tl.append_event(EventType.AGENT_CREATED, "a1")
        self.tl.fork("feature-y")
        fork_events = self.tl.query_by_type(EventType.TIMELINE_FORKED)
        self.assertEqual(len(fork_events), 1)
        self.assertEqual(fork_events[0].data["new_branch"], "feature-y")

    def test_duplicate_branch(self):
        self.tl.fork("branch-1")
        with self.assertRaises(ValueError):
            self.tl.fork("branch-1")

    def test_merge(self):
        self.tl.fork("feature-z")
        self.tl.append_event(EventType.PROGRAM_DEPLOYED, "a1")
        merge_id = self.tl.merge("feature-z")
        self.assertIsNotNone(merge_id)
        branch = self.tl.get_branch("feature-z")
        self.assertTrue(branch.is_merged)
        self.assertEqual(self.tl.current_branch, "main")

    def test_merge_records_event(self):
        self.tl.fork("feature-w")
        self.tl.merge("feature-w")
        merge_events = self.tl.query_by_type(EventType.TIMELINE_MERGED)
        self.assertEqual(len(merge_events), 1)

    def test_merge_nonexistent_branch(self):
        result = self.tl.merge("nonexistent")
        self.assertIsNone(result)

    def test_list_branches(self):
        self.tl.fork("b1")
        self.tl.fork("b2")
        branches = self.tl.list_branches()
        self.assertEqual(len(branches), 3)  # main + b1 + b2

    def test_merge_to_specific_target(self):
        self.tl.fork("exp")
        self.tl.fork("exp-2")
        merge_id = self.tl.merge("exp-2", target_branch="exp")
        self.assertIsNotNone(merge_id)
        self.assertEqual(self.tl.current_branch, "exp")


class TestTimelineCompaction(unittest.TestCase):
    """Test event compaction and snapshots."""

    def setUp(self):
        self.tl = FleetTimeline("compact-test")
        self.tl.register_state_handler(EventType.AGENT_CREATED,
                                       lambda e, s: {**s, "agents": s.get("agents", []) + [e.data.get("name")]}
                                       if e.data.get("name") not in s.get("agents", []) else s)
        self.tl.register_state_handler(EventType.PROGRAM_DEPLOYED,
                                       lambda e, s: {**s, "programs": s.get("programs", 0) + 1})

    def test_compact_basic(self):
        for i in range(20):
            self.tl.append_event(EventType.AGENT_CREATED, "system", {"name": f"agent-{i}"})
        snapshot = self.tl.compact(keep_last=5)
        self.assertIsNotNone(snapshot)
        self.assertEqual(snapshot.events_compacted, 15)
        self.assertIn("agents", snapshot.state)
        self.assertEqual(len(snapshot.state["agents"]), 15)  # first 15 events compacted

    def test_compact_too_few_events(self):
        self.tl.append_event(EventType.AGENT_CREATED, "system", {"name": "A"})
        snapshot = self.tl.compact(keep_last=100)
        self.assertIsNone(snapshot)

    def test_compact_records_event(self):
        for i in range(10):
            self.tl.append_event(EventType.PROGRAM_DEPLOYED, "a1")
        self.tl.compact(keep_last=2)
        snap_events = self.tl.query_by_type(EventType.SNAPSHOT_CREATED)
        self.assertEqual(len(snap_events), 1)

    def test_replay_with_snapshots(self):
        for i in range(15):
            self.tl.append_event(EventType.AGENT_CREATED, "system", {"name": f"agent-{i}"})
        self.tl.compact(keep_last=5)
        # Add more events after compaction
        self.tl.append_event(EventType.AGENT_CREATED, "system", {"name": "post-compact"})
        state = self.tl.replay_with_snapshots()
        self.assertEqual(len(state["agents"]), 16)


class TestCausalAnalysis(unittest.TestCase):
    """Test causal chain walking and conflict detection."""

    def test_causal_chain(self):
        tl = FleetTimeline("causal-test")
        e1 = tl.append_event(EventType.AGENT_CREATED, "a1")
        e2 = tl.append_event(EventType.PROGRAM_DEPLOYED, "a1", parent_event_ids=[e1.event_id])
        e3 = tl.append_event(EventType.PROGRAM_SIGNED, "a2", parent_event_ids=[e2.event_id])
        chain = tl.causal_chain(e3.event_id)
        ids = [e.event_id for e in chain]
        self.assertIn(e1.event_id, ids)
        self.assertIn(e2.event_id, ids)
        self.assertIn(e3.event_id, ids)

    def test_causal_chain_empty(self):
        tl = FleetTimeline("causal-empty")
        chain = tl.causal_chain("nonexistent")
        self.assertEqual(chain, [])

    def test_detect_conflicts(self):
        tl = FleetTimeline("conflict-test")
        # Concurrent events from different agents of the same type
        e1 = tl.append_event(EventType.PROGRAM_DEPLOYED, "a1")
        e2 = tl.append_event(EventType.PROGRAM_DEPLOYED, "a2")
        conflicts = tl.detect_conflicts()
        # e1 and e2 are from different agents, same type, concurrent
        self.assertGreater(len(conflicts), 0)

    def test_no_conflicts_sequential(self):
        tl = FleetTimeline("no-conflict")
        e1 = tl.append_event(EventType.AGENT_CREATED, "a1")
        e2 = tl.append_event(EventType.PROGRAM_DEPLOYED, "a1",
                             parent_event_ids=[e1.event_id])
        conflicts = tl.detect_conflicts()
        self.assertEqual(len(conflicts), 0)


class TestSerialization(unittest.TestCase):
    """Test export/import of timeline state."""

    def test_export_import(self):
        tl = FleetTimeline("export-test")
        tl.append_event(EventType.AGENT_CREATED, "a1", {"name": "Alice"})
        tl.append_event(EventType.PROGRAM_DEPLOYED, "a2")
        tl.fork("feature")
        data = tl.export()
        restored = FleetTimeline.from_export(data)
        self.assertEqual(restored.timeline_id, "export-test")
        self.assertEqual(restored.event_count, 3)  # 2 events + 1 fork event
        self.assertIn("feature", restored.branches)

    def test_export_events_json(self):
        tl = FleetTimeline("json-test")
        tl.append_event(EventType.AGENT_CREATED, "a1", {"name": "Bob"})
        json_str = tl.export_events_json()
        self.assertIn("agent_created", json_str)
        self.assertIn("Bob", json_str)

    def test_event_index_preserved(self):
        tl = FleetTimeline("idx-test")
        e = tl.append_event(EventType.AGENT_CREATED, "a1")
        data = tl.export()
        restored = FleetTimeline.from_export(data)
        fetched = restored.get_event(e.event_id)
        self.assertIsNotNone(fetched)
        self.assertEqual(fetched.agent_id, "a1")


class TestLegacyTracer(unittest.TestCase):
    """Test backward compatibility with the legacy TimelineTracer."""

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

    def test_factorial_trace(self):
        bc = [0x18, 0, 3, 0x18, 1, 1, 0x22, 1, 1, 0, 0x09, 0, 0x3D, 0, 0xFA, 0, 0x00]
        t = TimelineTracer()
        tl = t.trace(bc)
        self.assertGreater(tl.total_cycles, 5)
        self.assertGreater(tl.jumps_taken, 0)


class TestIntegration(unittest.TestCase):
    """End-to-end integration tests combining multiple features."""

    def test_full_fleet_lifecycle(self):
        tl = FleetTimeline("integration")
        tl.register_state_handler(EventType.AGENT_CREATED,
                                 lambda e, s: {**s, "agents": s.get("agents", []) + [e.agent_id]})
        tl.register_state_handler(EventType.PROGRAM_DEPLOYED,
                                 lambda e, s: {**s, "deployments": s.get("deployments", 0) + 1})

        # Fleet starts
        e1 = tl.append_event(EventType.AGENT_CREATED, "system", {"name": "Fleet Init"})
        # Agent joins
        e2 = tl.append_event(EventType.AGENT_CREATED, "agent-1", {"name": "Worker-1"})
        # Deploy program
        e3 = tl.append_event(EventType.PROGRAM_DEPLOYED, "agent-1",
                             parent_event_ids=[e2.event_id])

        # Replay to get state
        state = tl.replay()
        self.assertIn("agent-1", state["agents"])
        self.assertEqual(state["deployments"], 1)

        # Query temporal range
        events = tl.query_range(e1.timestamp, e3.timestamp)
        self.assertEqual(len(events), 3)

    def test_fork_deploy_merge(self):
        tl = FleetTimeline("integration-branch")
        tl.append_event(EventType.AGENT_CREATED, "system")

        # Fork, do work, merge back
        tl.fork("experiment")
        tl.append_event(EventType.PROGRAM_DEPLOYED, "dev")
        tl.merge("experiment")

        # Main branch should see the merged events
        self.assertGreater(tl.event_count, 2)

    def test_compact_then_replay(self):
        tl = FleetTimeline("integration-compact")
        tl.register_state_handler(EventType.AGENT_CREATED,
                                 lambda e, s: {**s, "count": s.get("count", 0) + 1})

        for i in range(30):
            tl.append_event(EventType.AGENT_CREATED, "system")

        snapshot = tl.compact(keep_last=10)
        self.assertIsNotNone(snapshot)

        state = tl.replay_with_snapshots()
        self.assertEqual(state["count"], 30)

    def test_export_import_with_snapshot(self):
        tl = FleetTimeline("integration-export")
        tl.register_state_handler(EventType.AGENT_CREATED,
                                 lambda e, s: {**s, "agents": s.get("agents", []) + [e.agent_id]})

        for i in range(10):
            tl.append_event(EventType.AGENT_CREATED, f"a{i}")
        tl.compact(keep_last=3)

        data = tl.export()
        restored = FleetTimeline.from_export(data)
        # Register the same handler
        restored.register_state_handler(EventType.AGENT_CREATED,
                                        lambda e, s: {**s, "agents": s.get("agents", []) + [e.agent_id]})
        state = restored.replay_with_snapshots()
        self.assertEqual(len(state["agents"]), 10)


if __name__ == "__main__":
    unittest.main(verbosity=2)
