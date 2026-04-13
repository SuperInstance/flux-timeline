# flux-timeline

> Bytecode execution tracer that visualizes FLUX program execution step-by-step with register and control flow tracking.

## What This Is

`flux-timeline` is a Python module that **traces FLUX bytecode execution** and produces human-readable timelines showing PC movement, register changes, jump decisions, and stack operations for every cycle.

## Role in the FLUX Ecosystem

Understanding what a program does at the cycle level is essential for debugging and optimization:

- **`flux-debugger`** provides interactive stepping; timeline provides batch tracing
- **`flux-profiler`** measures performance; timeline shows *what* happened
- **`flux-decompiler`** shows *what* the code is; timeline shows *what it does*
- **`flux-signatures`** identifies patterns statically; timeline confirms them dynamically
- **`flux-coverage`** tracks which instructions executed; timeline shows the order

## Key Features

| Feature | Description |
|---------|-------------|
| **Cycle-by-Cycle Tracing** | Every instruction produces a `TimelineEvent` with full state |
| **Register Snapshots** | Each event captures all R0–R15 values |
| **Jump Tracking** | Records branch taken/not-taken decisions with target PCs |
| **Stack Events** | Tracks PUSH/POP operations |
| **Text Output** | `to_text()` produces formatted execution log |
| **CSV Export** | `to_csv()` for spreadsheet analysis |
| **Opcodes Supported** | HALT, MOVI, INC, DEC, ADD, MUL, CMP_EQ, JZ, JNZ, PUSH, POP |

## Quick Start

```python
from flux_timeline import TimelineTracer

tracer = TimelineTracer()

# Trace a factorial program
bytecode = [0x18, 0, 3, 0x18, 1, 1, 0x22, 1, 1, 0, 0x09, 0, 0x3D, 0, -6, 0, 0x00]
timeline = tracer.trace(bytecode)

print(f"Total cycles: {timeline.total_cycles}")
print(f"Jumps taken: {timeline.jumps_taken}")
print(f"Max PC: {timeline.max_pc}")

# Human-readable output
print(timeline.to_text())

# Machine-readable output
print(timeline.to_csv())
```

## Running Tests

```bash
python -m pytest tests/ -v
# or
python timeline.py
```

## Related Fleet Repos

- [`flux-debugger`](https://github.com/SuperInstance/flux-debugger) — Interactive step debugger
- [`flux-profiler`](https://github.com/SuperInstance/flux-profiler) — Performance profiling
- [`flux-decompiler`](https://github.com/SuperInstance/flux-decompiler) — Bytecode to assembly
- [`flux-signatures`](https://github.com/SuperInstance/flux-signatures) — Pattern detection
- [`flux-coverage`](https://github.com/SuperInstance/flux-coverage) — Code coverage analysis

## License

Part of the [SuperInstance](https://github.com/SuperInstance) FLUX fleet.
