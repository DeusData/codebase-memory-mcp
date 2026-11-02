# Agent-event fixture provenance

These stable replay inputs exercise activity visualization without requiring a live
coding session. They contain no source contents or tool output. They are test
fixtures, not evidence of current agent activity.

## `w11a-replay.jsonl`

Derived from a real tool-hook recording made while working on this repository on
29-30 August 2026. The private original is not committed because it contains local
paths and commands. This fixture preserves its event shape, relative tool mix,
initial timestamp (`1788038561000`) and 44 consecutive time intervals in milliseconds:

```text
10065, 13599, 115703, 5123, 18182, 6789, 1140, 13387, 5243, 5570, 6676,
21168, 51146, 6330, 6339, 28257, 4029, 318, 2709, 709, 83, 6176, 1116,
3373, 3642, 3149, 2275, 4175, 2884, 4066, 2310, 2028, 2838, 33596, 5878,
17621, 826, 5788, 11062, 6988, 5724, 10001, 17767, 13461
```

The intervals came from the first 45-event window without a pause over two minutes,
beginning at line 57 of the original. The following content was deliberately changed:

- Absolute paths became relative `fixtures/atlas-sample` paths. `package.json` is
  intentionally outside the indexed graph to exercise unmapped activity.
- One original agent became the synthetic roles `implementer`, `checker` and
  `explorer`; their eight-character run IDs are invented.
- Explicit `lines` ranges were added from fixture symbols. The original hook did
  not record line ranges.
- Run `b7e4d19c` jumps from sequence 6 to 9 to expose two missing events.
- Two synthetic `intent` fields exercise explicitly labeled self-reports.
- `detail` values describe fixture operations instead of retaining private commands.

The result contains 45 events, three runs, four work types, one two-event sequence
gap, two self-reports and one path outside the index.

## `w11b-replay.jsonl`

Entirely constructed test data, not a session recording. Its paths and line spans
come from `fixtures/atlas-sample`; timestamps and activity are deliberately chosen.

- Eight agents plus the browser reader exercise the eight-body display limit.
- `explorer` and `planner` end 75 and 85 seconds before the last event. Both are
  idle but still within retention; the oldest actor exercises the display cap.
- `implementer` writes `createUser` lines 23-36 five times in 1.8 seconds to test
  aggregation into one visible wave, and visits fifteen symbols to exceed trail limits.
- `reviewer`, `indexer` and `tracer` end at `hotspotScan` in `src/repo/db.ts`, lines
  39-54, to test deterministic orbit separation at one symbol.
- Activity rates differ: fifteen recent events for `implementer`, two for `packer`,
  none for idle `planner`. One `package.json` path remains unmapped and one event
  has a synthetic self-reported `intent`.

There are 73 events across eight runs and four work types over 145 seconds. Last
activity offsets are 0, 3.8, 4.4, 5, 7, 10, 75 and 85 seconds for `implementer`,
`tracer`, `indexer`, `reviewer`, `packer`, `checker`, `explorer` and `planner`.
The fixture does not encode progress, quality assessments or source contents.
