"""Pure bounded single-image profile-map v2 encoding and row reduction.

Evidence fields are caller claims, not authentication or closure certificates.
Names/hashes/shapes retain exact profile identity; source names are not inferred.
Limits count logical data, not interpreter RSS. Runtime allocation/deallocation
is not preemptible. This module performs no I/O and leaves format 1 unchanged.
"""

from dataclasses import dataclass
from enum import Enum
import hashlib
import sys
from typing import Callable, Optional, Tuple

from profile_text_parser import (
    LimitExceededError, MalformedProfileError, ParseCancelledError,
    ParseLimits, parse_profile_text,
)


class SuiteOutcome(Enum):
    PASSED = "passed"
    FAILED = "failed"
    TIMED_OUT = "timed_out"
    UNKNOWN = "unknown"


class ObservationRole(Enum):
    PARENT = "parent"
    SETUP = "setup"
    CHILD = "child"


@dataclass(frozen=True)
class ExpectedSuite:
    __slots__ = ("suite", "tests", "outcome")

    suite: bytes
    tests: Tuple[bytes, ...]
    outcome: SuiteOutcome


@dataclass(frozen=True)
class ProfileObservation:
    __slots__ = ("suite", "test", "key", "role", "text")

    suite: bytes
    test: bytes
    key: bytes
    role: ObservationRole
    text: Optional[bytes]  # None means an explicitly unavailable observation.


@dataclass(frozen=True)
class RowEvidence:
    __slots__ = (
        "suite", "test", "interval_complete", "runtime_compatible",
        "same_image", "children_complete",
    )

    suite: bytes
    test: bytes
    interval_complete: bool
    runtime_compatible: bool
    same_image: bool
    children_complete: bool


@dataclass(frozen=True)
class ProfileMapLimits:
    __slots__ = (
        "max_input_bytes", "max_rows", "max_observations", "max_ids",
        "max_records", "max_counter_values", "max_name_bytes",
        "max_id_references", "max_output_bytes",
    )

    max_input_bytes: int
    max_rows: int
    max_observations: int
    max_ids: int
    max_records: int
    max_counter_values: int
    max_name_bytes: int
    max_id_references: int
    max_output_bytes: int


@dataclass(frozen=True)
class ProducedMap:
    __slots__ = (
        "profiles_tsv", "tests_tsv", "profiles_sha256", "tests_sha256",
    )

    profiles_tsv: bytes
    tests_tsv: bytes
    profiles_sha256: bytes
    tests_sha256: bytes


class ProfileMapError(ValueError):
    """Base for classified build failures; no ProducedMap is returned."""


class ProfileMapInvalidArgumentError(ProfileMapError):
    """Invalid exact type, field, routing, membership, duplicate or callback result."""


class ProfileMapEnumerationError(ProfileMapError):
    """Enumeration text is malformed or unsupported, including empty text."""


class ProfileMapCompatibilityError(ProfileMapError):
    """An observation contains an unknown full identity or mismatching shape."""


class ProfileMapLimitExceededError(ProfileMapError):
    """A declared logical or fixed representation bound would be exceeded."""


class ProfileMapCancelledError(ProfileMapError):
    """The caller's pure prompt predicate requested cancellation."""


_INT_MAX = 2147483647
_SHA_BYTES_MAX = ((1 << 64) - 1) // 8
_POLL_INTERVAL = 4096
_HEX = b"0123456789abcdef"
_REASONS = (
    b"suite_failed", b"suite_timeout", b"suite_unknown",
    b"row_evidence_missing", b"interval_unproved", b"runtime_unproved",
    b"image_unproved", b"children_unproved", b"missing_parent",
    b"missing_setup", b"observation_unavailable", b"observation_malformed",
    b"partial_stream",
)
_SUITE_FAILED, _SUITE_TIMEOUT, _SUITE_UNKNOWN = 0, 1, 2
_NO_EVIDENCE, _INTERVAL, _RUNTIME, _IMAGE, _CHILDREN = 3, 4, 5, 6, 7
_NO_PARENT, _NO_SETUP, _UNAVAILABLE, _MALFORMED, _PARTIAL = 8, 9, 10, 11, 12


class _Gate:
    __slots__ = ("cancel", "context", "work", "callback_error")

    def __init__(self, cancel, context):
        self.cancel = cancel
        self.context = context
        self.work = 0
        self.callback_error = None

    def poll(self):
        self.work = 0
        if self.cancel is None:
            return
        try:
            answer = self.cancel(self.context)
        except BaseException as error:
            # Identity distinguishes a callback's parser-shaped exception from
            # a parser failure; neither its class nor message is sufficient.
            self.callback_error = error
            raise
        if type(answer) is not bool:
            raise ProfileMapInvalidArgumentError("cancel must return bool")
        if answer:
            raise ProfileMapCancelledError("profile map build cancelled")

    def step(self):
        self.work += 1
        if self.work == _POLL_INTERVAL:
            self.poll()

    def parser_cancel(self, _context):
        self.poll()
        return False


def _bounded_add(current, amount, cap, label):
    if amount > cap - current:
        raise ProfileMapLimitExceededError(label)
    return current + amount


class _Budget:
    __slots__ = (
        "limits", "input_bytes", "rows", "records", "counter_values",
        "name_bytes", "id_references", "output_bytes",
    )

    def __init__(self, limits, enumeration_text):
        self.limits = limits
        self.input_bytes = _bounded_add(
            32, len(enumeration_text), limits.max_input_bytes, "input byte limit"
        )
        self.rows = 0
        self.records = 0
        self.counter_values = 0
        self.name_bytes = 0
        self.id_references = 0
        self.output_bytes = 0

    def name(self, value):
        self.input_bytes = _bounded_add(
            self.input_bytes, len(value), self.limits.max_input_bytes,
            "input byte limit",
        )
        self.name_bytes = _bounded_add(
            self.name_bytes, len(value), self.limits.max_name_bytes,
            "name byte limit",
        )

    def text(self, value):
        self.input_bytes = _bounded_add(
            self.input_bytes, len(value), self.limits.max_input_bytes,
            "input byte limit",
        )

    def add_rows(self, count):
        self.rows = _bounded_add(
            self.rows, count, min(self.limits.max_rows, _INT_MAX), "row limit"
        )

    def parsed(self, records, gate):
        self.records = _bounded_add(
            self.records, len(records), self.limits.max_records, "record limit"
        )
        for record in records:
            gate.step()
            self.counter_values = _bounded_add(
                self.counter_values, len(record.counters),
                self.limits.max_counter_values, "counter value limit",
            )
            self.name_bytes = _bounded_add(
                self.name_bytes, len(record.raw_name),
                self.limits.max_name_bytes, "name byte limit",
            )

    def reference(self):
        self.id_references = _bounded_add(
            self.id_references, 1, self.limits.max_id_references,
            "ID reference limit",
        )

    def output(self, count):
        self.output_bytes = _bounded_add(
            self.output_bytes, count,
            min(self.limits.max_output_bytes, _SHA_BYTES_MAX), "output byte limit",
        )


class _Row:
    __slots__ = (
        "suite", "test", "outcome", "evidence", "observations", "ids",
        "reasons", "parent_seen", "primary_parsed",
    )

    def __init__(self, suite, test, outcome):
        self.suite = suite
        self.test = test
        self.outcome = outcome
        self.evidence = None
        self.observations = []
        self.ids = set()
        self.reasons = 0
        self.parent_seen = False
        self.primary_parsed = False

    def mark(self, reason):
        self.reasons |= 1 << reason


def _exact(value, cls, label):
    if type(value) is not cls:
        raise ProfileMapInvalidArgumentError(label)


def _validate_top(image, enumeration, suites, observations, evidence, limits, cancel):
    _exact(image, bytes, "image digest must be bytes")
    if len(image) != 32:
        raise ProfileMapInvalidArgumentError("image digest must contain 32 bytes")
    _exact(enumeration, bytes, "enumeration must be bytes")
    _exact(suites, tuple, "expected suites must be a tuple")
    _exact(observations, tuple, "observations must be a tuple")
    _exact(evidence, tuple, "row evidence must be a tuple")
    _exact(limits, ProfileMapLimits, "invalid limits type")
    for field in ProfileMapLimits.__slots__:
        value = getattr(limits, field)
        if type(value) is not int or value <= 0 or value > sys.maxsize:
            raise ProfileMapInvalidArgumentError("limits must be positive bounded ints")
    if limits.max_ids > _INT_MAX:
        raise ProfileMapInvalidArgumentError("max_ids exceeds INT_MAX")
    if cancel is not None and not callable(cancel):
        raise ProfileMapInvalidArgumentError("cancel must be callable or None")


def _name(value, budget, gate, *, suite=False, declared_test=False):
    _exact(value, bytes, "names and keys must be bytes")
    if not value:
        raise ProfileMapInvalidArgumentError("empty name or key")
    budget.name(value)
    for char in value:
        gate.step()
        if char in (0, 9, 10, 13) or (suite and char == 58):
            raise ProfileMapInvalidArgumentError("unrepresentable name or key")
    if declared_test and _is_setup(value):
        raise ProfileMapInvalidArgumentError("setup name is reserved")


def _is_setup(test):
    return len(test) == 1 and test[0] == 42


def _compare_bytes(left, right, gate):
    for index in range(min(len(left), len(right))):
        gate.step()
        a, b = left[index], right[index]
        if a != b:
            return -1 if a < b else 1
    gate.step()
    return (len(left) > len(right)) - (len(left) < len(right))


def _compare_fields(left, right, fields, gate):
    for field in fields:
        gate.step()
        order = _compare_bytes(getattr(left, field), getattr(right, field), gate)
        if order:
            return order
    return 0


def _merge(source, target, start, middle, end, compare, gate):
    left, right = start, middle
    for destination in range(start, end):
        gate.step()
        take_left = right == end or (
            left < middle and compare(source[left], source[right]) <= 0
        )
        if take_left:
            target[destination] = source[left]
            left += 1
        else:
            target[destination] = source[right]
            right += 1


def _sort(values, compare, gate):
    source, target = [], []
    for value in values:
        gate.step()
        source.append(value)
        target.append(None)
    width = 1
    count = len(source)
    while width < count:
        gate.step()
        for start in range(0, count, width * 2):
            gate.step()
            middle, end = min(start + width, count), min(start + width * 2, count)
            _merge(source, target, start, middle, end, compare, gate)
        source, target = target, source
        width *= 2
    return source


def _sort_unique(values, fields, gate, label):
    compare = lambda left, right: _compare_fields(left, right, fields, gate)
    result = _sort(values, compare, gate)
    for index in range(1, len(result)):
        gate.step()
        if compare(result[index - 1], result[index]) == 0:
            raise ProfileMapInvalidArgumentError(label)
    return result


def _suites(suites, budget, gate):
    rows = []
    if len(suites) > min(budget.limits.max_rows, _INT_MAX):
        raise ProfileMapLimitExceededError("row limit")
    for suite in suites:
        gate.step()
        _exact(suite, ExpectedSuite, "invalid expected suite type")
        _name(suite.suite, budget, gate, suite=True)
        _exact(suite.tests, tuple, "expected tests must be a tuple")
        _exact(suite.outcome, SuiteOutcome, "invalid suite outcome")
        budget.add_rows(1 + len(suite.tests))
        rows.append(_Row(suite.suite, b"*", suite.outcome))
        for test in suite.tests:
            gate.step()
            _name(test, budget, gate, declared_test=True)
            rows.append(_Row(suite.suite, test, suite.outcome))
    _sort_unique(suites, ("suite",), gate, "duplicate expected suite")
    return _sort_unique(rows, ("suite", "test"), gate, "duplicate expected test")


def _compare_target(row, suite, test, gate):
    order = _compare_bytes(row.suite, suite, gate)
    return order if order else _compare_bytes(row.test, test, gate)


def _find_row(rows, suite, test, gate):
    low, high = 0, len(rows)
    while low < high:
        gate.step()
        middle = (low + high) // 2
        order = _compare_target(rows[middle], suite, test, gate)
        if order == 0:
            return rows[middle]
        if order < 0:
            low = middle + 1
        else:
            high = middle
    raise ProfileMapInvalidArgumentError("observation/evidence outside expected rows")


def _observation_schema(observation, budget, gate):
    _exact(observation, ProfileObservation, "invalid observation type")
    _name(observation.suite, budget, gate, suite=True)
    _name(observation.test, budget, gate)
    _name(observation.key, budget, gate)
    _exact(observation.role, ObservationRole, "invalid observation role")
    if observation.text is not None:
        _exact(observation.text, bytes, "observation text must be bytes or None")
        budget.text(observation.text)


def _route_observation(observation, row):
    setup = _is_setup(row.test)
    if ((setup and observation.role is ObservationRole.PARENT)
            or (not setup and observation.role is ObservationRole.SETUP)):
        raise ProfileMapInvalidArgumentError("invalid role for row")
    if observation.role is ObservationRole.PARENT:
        if row.parent_seen:
            raise ProfileMapInvalidArgumentError("multiple parent observations")
        row.parent_seen = True
    row.observations.append(observation)


def _observations(observations, rows, budget, gate):
    if len(observations) > budget.limits.max_observations:
        raise ProfileMapLimitExceededError("observation limit")
    for observation in observations:
        gate.step()
        _observation_schema(observation, budget, gate)
    ordered = _sort_unique(
        observations, ("suite", "test", "key"), gate, "duplicate observation key"
    )
    for observation in ordered:
        gate.step()
        row = _find_row(rows, observation.suite, observation.test, gate)
        _route_observation(observation, row)


def _evidence(evidence, rows, budget, gate):
    if len(evidence) > len(rows):
        raise ProfileMapInvalidArgumentError("too many row evidence records")
    for item in evidence:
        gate.step()
        _exact(item, RowEvidence, "invalid row evidence type")
        _name(item.suite, budget, gate, suite=True)
        _name(item.test, budget, gate)
        for field in ("interval_complete", "runtime_compatible", "same_image", "children_complete"):
            gate.step()
            _exact(getattr(item, field), bool, "evidence claims must be bool")
    ordered = _sort_unique(evidence, ("suite", "test"), gate, "duplicate row evidence")
    for item in ordered:
        gate.step()
        _find_row(rows, item.suite, item.test, gate).evidence = item


def _preflight(suites, observations, evidence, budget, gate):
    rows = _suites(suites, budget, gate)
    _observations(observations, rows, budget, gate)
    _evidence(evidence, rows, budget, gate)
    gate.poll()
    return rows


def _parser_limits(budget, enumeration):
    limits = budget.limits
    records = limits.max_records - budget.records
    counters = limits.max_counter_values - budget.counter_values
    names = limits.max_name_bytes - budget.name_bytes
    if enumeration:
        records = min(records, limits.max_ids)
    if records <= 0 or counters <= 0 or names <= 0:
        raise ProfileMapLimitExceededError("no remaining parser item allowance")
    return ParseLimits(limits.max_input_bytes, records, counters, names)


def _parse(text, budget, gate, *, enumeration=False):
    limits = _parser_limits(budget, enumeration)
    try:
        records = parse_profile_text(text, limits, cancel=gate.parser_cancel)
    except MalformedProfileError as error:
        if error is gate.callback_error:
            raise
        if enumeration:
            raise ProfileMapEnumerationError("invalid profile enumeration") from error
        return None
    except LimitExceededError as error:
        if error is gate.callback_error:
            raise
        raise ProfileMapLimitExceededError("profile parser resource limit") from error
    except ParseCancelledError as error:
        if error is gate.callback_error:
            raise
        raise ProfileMapCancelledError("profile parsing cancelled") from error
    budget.parsed(records, gate)
    return records


def _record_order(left, right, gate):
    order = _compare_bytes(left.raw_name, right.raw_name, gate)
    if order:
        return order
    gate.step()
    return (left.function_hash > right.function_hash) - (left.function_hash < right.function_hash)


def _has_hit(record, gate):
    for value in record.counters:
        gate.step()
        if value:
            return True
    return False


def _join(records, universe, row, budget, gate):
    position = 0
    for record in records:
        gate.step()
        while position < len(universe):
            gate.step()
            order = _record_order(universe[position], record, gate)
            if order >= 0:
                break
            position += 1
        if position == len(universe) or order != 0:
            raise ProfileMapCompatibilityError("observation identity outside universe")
        if len(record.counters) != len(universe[position].counters):
            raise ProfileMapCompatibilityError("observation counter shape mismatch")
        if _has_hit(record, gate) and position not in row.ids:
            budget.reference()
            row.ids.add(position)
        position += 1
    if len(records) != len(universe):
        row.mark(_PARTIAL)


def _base_reasons(row, gate):
    outcomes = (
        (SuiteOutcome.FAILED, _SUITE_FAILED),
        (SuiteOutcome.TIMED_OUT, _SUITE_TIMEOUT),
        (SuiteOutcome.UNKNOWN, _SUITE_UNKNOWN),
    )
    for outcome, reason in outcomes:
        gate.step()
        if row.outcome is outcome:
            row.mark(reason)
    if row.evidence is None:
        row.mark(_NO_EVIDENCE)
        return
    for field, reason in (
        ("interval_complete", _INTERVAL), ("runtime_compatible", _RUNTIME),
        ("same_image", _IMAGE), ("children_complete", _CHILDREN),
    ):
        gate.step()
        if not getattr(row.evidence, field):
            row.mark(reason)


def _reduce_row(row, universe, budget, gate):
    _base_reasons(row, gate)
    for observation in row.observations:
        gate.step()
        if observation.text is None:
            row.mark(_UNAVAILABLE)
            continue
        records = _parse(observation.text, budget, gate)
        if records is None:
            row.mark(_MALFORMED)
            continue
        _join(records, universe, row, budget, gate)
        if observation.role in (ObservationRole.PARENT, ObservationRole.SETUP):
            row.primary_parsed = True
    if not row.primary_parsed:
        row.mark(_NO_SETUP if _is_setup(row.test) else _NO_PARENT)


class _Wire:
    __slots__ = ("budget", "gate", "data")

    def __init__(self, budget, gate):
        self.budget = budget
        self.gate = gate
        self.data = bytearray()

    def emit(self, value):
        self.budget.output(len(value))
        for char in value:
            self.gate.step()
            self.data.append(char)

    def hex(self, value):
        self.budget.output(len(value) * 2)
        for char in value:
            self.gate.step()
            self.gate.step()
            self.data.append(_HEX[char >> 4])
            self.gate.step()
            self.data.append(_HEX[char & 15])

    def decimal(self, value):
        # All emitted integers are <= uint64, so conversion is <=20 digits.
        self.gate.poll()
        self.emit(str(value).encode("ascii"))

    def _values(self):
        for char in self.data:
            self.gate.step()
            yield char

    def finish(self):
        self.gate.poll()
        result = bytes(self._values())
        self.gate.poll()
        return result


def _profile_wire(image, universe, budget, gate):
    out = _Wire(budget, gate)
    out.emit(b"CBM_PROFILE_MAP\t2\t")
    out.hex(image)
    out.emit(b"\t")
    out.decimal(len(universe))
    out.emit(b"\n")
    for ident, record in enumerate(universe):
        gate.step()
        out.decimal(ident)
        out.emit(b"\t")
        out.hex(record.raw_name)
        out.emit(b"\t")
        gate.poll()
        out.emit(format(record.function_hash, "016x").encode("ascii"))
        out.emit(b"\t")
        out.decimal(len(record.counters))
        out.emit(b"\n")
    return out.finish()


def _reason_wire(row, out, gate):
    first = True
    for index, reason in enumerate(_REASONS):
        gate.step()
        if row.reasons & (1 << index):
            if not first:
                out.emit(b";")
            out.emit(reason)
            first = False


def _id_wire(row, out, gate):
    def compare(left, right):
        gate.step()
        return (left > right) - (left < right)

    ordered = _sort(row.ids, compare, gate)
    for index, ident in enumerate(ordered):
        gate.step()
        if index:
            out.emit(b" ")
        out.decimal(ident)


def _test_wire(rows, budget, gate):
    out = _Wire(budget, gate)
    for row in rows:
        gate.step()
        out.emit(row.suite)
        out.emit(b":")
        out.emit(row.test)
        out.emit(b"\tincomplete\t" if row.reasons else b"\tcomplete\t")
        _reason_wire(row, out, gate)
        out.emit(b"\t")
        _id_wire(row, out, gate)
        out.emit(b"\n")
    return out.finish()


def _digest(data, gate):
    digest = hashlib.sha256()
    for start in range(0, len(data), _POLL_INTERVAL):
        gate.poll()
        chunk = data[start:start + _POLL_INTERVAL]
        gate.poll()
        digest.update(chunk)
        gate.poll()
    return digest.digest()


def build_profile_map_v2(
    image_sha256: bytes,
    enumeration_text: bytes,
    expected_suites: Tuple[ExpectedSuite, ...],
    observations: Tuple[ProfileObservation, ...],
    row_evidence: Tuple[RowEvidence, ...],
    limits: ProfileMapLimits,
    *,
    cancel: Optional[Callable[[object], bool]] = None,
    cancel_context: object = None,
) -> ProducedMap:
    """Produce exact v2 wires or raise, without I/O or a partial result.

    Inputs are immutable exact types. Evidence is supplied by the caller;
    parsing cannot establish image/process closure, provenance or admission.
    """
    _validate_top(image_sha256, enumeration_text, expected_suites, observations,
                  row_evidence, limits, cancel)
    gate = _Gate(cancel, cancel_context)
    gate.poll()
    budget = _Budget(limits, enumeration_text)
    rows = _preflight(expected_suites, observations, row_evidence, budget, gate)
    universe = _parse(enumeration_text, budget, gate, enumeration=True)
    for row in rows:
        gate.step()
        _reduce_row(row, universe, budget, gate)
    profiles = _profile_wire(image_sha256, universe, budget, gate)
    tests = _test_wire(rows, budget, gate)
    profiles_digest = _digest(profiles, gate)
    tests_digest = _digest(tests, gate)
    gate.poll()
    return ProducedMap(profiles, tests, profiles_digest, tests_digest)
