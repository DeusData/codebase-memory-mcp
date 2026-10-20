"""Strict data-only parser for the observed LLVM 22.1.8 frontend proftext.

parse_profile_text(bytes, ParseLimits, *, cancel=None, cancel_context=None)
returns an immutable tuple of ProfileRecord(raw_name, function_hash, counters),
ordered by unsigned raw-name bytes and then uint64 hash. Zero values survive.

Each record is a nonempty name line, '# Func Hash:', uint64, '# Num Counters:',
positive uint64, '# Counter Values:', exactly that many uint64 lines, and one
mandatory blank line. All delimiters are LF. Decimals are canonical. Empty
documents, CR/NUL, extra sections and duplicate name/hash tuples fail closed.
Names remain opaque bytes: no normalization, decoding or comment processing.

Four exact positive integer limits bound input bytes, records, total counter
values and total name bytes. Errors derive from ProfileTextError; MemoryError
and callback exceptions propagate without a partial result. A pure, prompt
cancel(context) predicate must return bool. It is called at entry/final publish
and within explicit loops at intervals of at most 4096 byte/element operations.
Python allocation/resizing/deallocation and callbacks are not preemptible.

Limits bound logical input and result items, not exact interpreter heap use.
This module performs no I/O and makes no image, authentication or closure claim.
"""

from __future__ import annotations

from dataclasses import dataclass
import sys
from typing import Callable


_UINT64_MAX = (1 << 64) - 1
_POLL_INTERVAL = 4096


@dataclass(frozen=True)
class ProfileRecord:
    __slots__ = ("raw_name", "function_hash", "counters")

    raw_name: bytes
    function_hash: int
    counters: tuple[int, ...]


@dataclass(frozen=True)
class ParseLimits:
    __slots__ = ("max_input_bytes", "max_records", "max_counter_values", "max_name_bytes")

    max_input_bytes: int
    max_records: int
    max_counter_values: int
    max_name_bytes: int


class ProfileTextError(ValueError):
    """Base for parser failures; no partial result is published."""


class InvalidArgumentError(ProfileTextError):
    """Invalid public argument, limit, or callback return type."""


class MalformedProfileError(ProfileTextError):
    """Unsupported or malformed text, including duplicate identities."""


class LimitExceededError(ProfileTextError):
    """A configured logical resource bound would be exceeded."""


class ParseCancelledError(ProfileTextError):
    """The caller's predicate requested cancellation."""


class _Gate:
    __slots__ = ("cancel", "context", "work")

    def __init__(self, cancel, context):
        self.cancel = cancel
        self.context = context
        self.work = 0

    def poll(self):
        self.work = 0
        if self.cancel is None:
            return
        answer = self.cancel(self.context)
        if type(answer) is not bool:
            raise InvalidArgumentError("cancel must return bool")
        if answer:
            raise ParseCancelledError("profile parsing cancelled")

    def step(self):
        self.work += 1
        if self.work == _POLL_INTERVAL:
            self.poll()


class _Parser:
    __slots__ = ("data", "limits", "gate", "position", "names", "values")

    def __init__(self, data, limits, gate):
        self.data = data
        self.limits = limits
        self.gate = gate
        self.position = 0
        self.names = 0
        self.values = 0

    def line(self):
        start = self.position
        while self.position < len(self.data):
            self.gate.step()
            value = self.data[self.position]
            self.position += 1
            if value == 0 or value == 13:
                raise MalformedProfileError("NUL and CR are unsupported")
            if value == 10:
                return start, self.position - 1
        raise MalformedProfileError("unterminated or missing line")

    def marker(self, expected):
        start, end = self.line()
        if end - start != len(expected):
            raise MalformedProfileError("unexpected record marker")
        for index, value in enumerate(expected):
            self.gate.step()
            if self.data[start + index] != value:
                raise MalformedProfileError("unexpected record marker")

    def uint64(self):
        start, end = self.line()
        length = end - start
        if length == 0 or length > 20:
            raise MalformedProfileError("invalid unsigned decimal")
        if length > 1 and self.data[start] == 48:
            raise MalformedProfileError("noncanonical unsigned decimal")
        value = 0
        for index in range(start, end):
            self.gate.step()
            digit = self.data[index] - 48
            if digit < 0 or digit > 9:
                raise MalformedProfileError("invalid unsigned decimal")
            if value > (_UINT64_MAX - digit) // 10:
                raise MalformedProfileError("unsigned decimal overflow")
            value = value * 10 + digit
        return value

    def name_values(self, start, end):
        for index in range(start, end):
            self.gate.step()
            yield self.data[index]

    def counter_values(self, count):
        for _ in range(count):
            self.gate.step()
            yield self.uint64()

    def record(self):
        start, end = self.line()
        length = end - start
        if length == 0:
            raise MalformedProfileError("empty record name")
        if length > self.limits.max_name_bytes - self.names:
            raise LimitExceededError("total name bytes limit exceeded")
        self.names += length
        self.marker(b"# Func Hash:")
        function_hash = self.uint64()
        self.marker(b"# Num Counters:")
        count = self.uint64()
        if count == 0:
            raise MalformedProfileError("counter count must be positive")
        if count > self.limits.max_counter_values - self.values:
            raise LimitExceededError("total counter values limit exceeded")
        self.values += count
        self.marker(b"# Counter Values:")
        # Each remaining value needs at least '0\n', then the final blank line.
        remaining = len(self.data) - self.position
        if remaining < 1 or count > (remaining - 1) // 2:
            raise MalformedProfileError("truncated counter values")
        self.gate.poll()
        counters = tuple(self.counter_values(count))
        self.gate.poll()
        blank_start, blank_end = self.line()
        if blank_start != blank_end:
            raise MalformedProfileError("missing record blank line")
        self.gate.poll()
        name = bytes(self.name_values(start, end))
        self.gate.poll()
        return ProfileRecord(name, function_hash, counters)

    def records(self):
        self.gate.poll()
        records = []
        while self.position < len(self.data):
            self.gate.step()
            if len(records) == self.limits.max_records:
                raise LimitExceededError("record count limit exceeded")
            record = self.record()
            self.gate.poll()
            records.append(record)
        if not records:
            raise MalformedProfileError("empty document")
        return records


def _compare(left, right, gate):
    common = min(len(left.raw_name), len(right.raw_name))
    for index in range(common):
        gate.step()
        a = left.raw_name[index]
        b = right.raw_name[index]
        if a != b:
            return -1 if a < b else 1
    gate.step()
    if len(left.raw_name) != len(right.raw_name):
        return -1 if len(left.raw_name) < len(right.raw_name) else 1
    if left.function_hash == right.function_hash:
        return 0
    return -1 if left.function_hash < right.function_hash else 1


def _merge(source, target, start, middle, end, gate):
    left, right = start, middle
    for destination in range(start, end):
        gate.step()
        take_left = right == end or (
            left < middle and _compare(source[left], source[right], gate) <= 0
        )
        if take_left:
            target[destination] = source[left]
            left += 1
        else:
            target[destination] = source[right]
            right += 1


def _sorted_unique(records, gate):
    gate.poll()
    scratch = []
    for _ in records:
        gate.step()
        scratch.append(None)
    gate.poll()
    width = 1
    length = len(records)
    while width < length:
        for start in range(0, length, width * 2):
            middle = min(start + width, length)
            end = min(start + width * 2, length)
            _merge(records, scratch, start, middle, end, gate)
        records, scratch = scratch, records
        width *= 2
    for index in range(1, length):
        gate.step()
        if _compare(records[index - 1], records[index], gate) == 0:
            raise MalformedProfileError("duplicate name/hash identity")
    return records


def _record_values(records, gate):
    for record in records:
        gate.step()
        yield record


def _validate_arguments(data, limits, cancel):
    if type(data) is not bytes or type(limits) is not ParseLimits:
        raise InvalidArgumentError("expected bytes and ParseLimits")
    for value in (limits.max_input_bytes, limits.max_records,
                  limits.max_counter_values, limits.max_name_bytes):
        if type(value) is not int or value <= 0 or value > sys.maxsize:
            raise InvalidArgumentError("limits must be positive bounded integers")
    if cancel is not None and not callable(cancel):
        raise InvalidArgumentError("cancel must be callable or None")


def parse_profile_text(
    data: bytes,
    limits: ParseLimits,
    *,
    cancel: Callable[[object], bool] | None = None,
    cancel_context: object = None,
) -> tuple[ProfileRecord, ...]:
    """Parse all records or raise a specific error; never return a partial result."""
    _validate_arguments(data, limits, cancel)
    gate = _Gate(cancel, cancel_context)
    gate.poll()
    if len(data) > limits.max_input_bytes:
        raise LimitExceededError("input bytes limit exceeded")
    parser = _Parser(data, limits, gate)
    records = _sorted_unique(parser.records(), gate)
    gate.poll()
    result = tuple(_record_values(records, gate))
    gate.poll()
    return result
