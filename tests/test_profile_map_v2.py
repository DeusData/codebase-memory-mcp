#!/usr/bin/env python3
"""Independent frozen-contract tests for the pure single-image v2 producer.

Python 3.9 compatible. --module selects the producer under test; --parser can
select its unchanged real dependency for a scratch proof. --fixtures selects
the shared native golden fixture directory. No LLVM, compiler,
runner, network, admission or source-origin proof is performed by this suite.
"""

import argparse
from dataclasses import FrozenInstanceError, replace
import hashlib
import importlib.util
from pathlib import Path
import sys
import unittest


M = None
P = None
FIXTURE_DIR = Path(__file__).resolve().parent / "fixtures/profile_map_v2"
IMAGE_HEX = b"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
IMAGE = bytes.fromhex(IMAGE_HEX.decode("ascii"))
HIGH = 1 << 63
U64 = (1 << 64) - 1
GOLD_PROFILES = (
    b"CBM_PROFILE_MAP\t2\t" + IMAGE_HEX + b"\t3\n"
    b"0\t61\t0000000000000000\t2\n"
    b"1\t61\t8000000000000000\t1\n"
    b"2\tff3a09207a\tffffffffffffffff\t2\n"
)
GOLD_TESTS = (
    b"s\xfe:*\tcomplete\t\t\n"
    b"s\xfe:hit:raw\xfd\tcomplete\t\t0 2\n"
    b"s\xfe:missing\tincomplete\tmissing_parent\t\n"
    b"s\xfe:quiet\tcomplete\t\t\n"
)
GOLD_PROFILES_SHA = bytes.fromhex("ecc78b966bef4eb27a144eba98c2794ebf5f31a318f269e5e21d3453afd02d76")
GOLD_TESTS_SHA = bytes.fromhex("fe662544cf1d1eca44cc08ef6706ab04c59db76481b59aa3a4117a4c6c82da43")


def record(name, function_hash, counters):
    """Hand-shaped supported proftext fixture, not a producer-output oracle."""
    return (name + b"\n# Func Hash:\n" + str(function_hash).encode("ascii") +
            b"\n# Num Counters:\n" + str(len(counters)).encode("ascii") +
            b"\n# Counter Values:\n" +
            b"".join(str(value).encode("ascii") + b"\n" for value in counters) + b"\n")


def text(records):
    return b"".join(record(*row) for row in records)


def ample():
    return M.ProfileMapLimits(8 * 1024 * 1024, 512, 1024, 5000, 50000,
                              100000, 8 * 1024 * 1024, 10000, 16 * 1024 * 1024)


def claims(suite, test, flags=(True, True, True, True)):
    return M.RowEvidence(suite, test, *flags)


def observation(suite, test, key, role, value):
    return M.ProfileObservation(suite, test, key, role, value)


def gold_fixture():
    suite = b"s\xfe"
    zero = text(((b"a", 0, (0, 0)), (b"a", HIGH, (0,)), (b"\xff:\t z", U64, (0, 0))))
    enumeration = text(((b"\xff:\t z", U64, (0, 0)), (b"a", HIGH, (0,)), (b"a", 0, (9, 0))))
    parent = text(((b"a", 0, (0, 7)), (b"a", HIGH, (0,)), (b"\xff:\t z", U64, (0, 0))))
    child = text(((b"a", 0, (0, 0)), (b"a", HIGH, (0,)), (b"\xff:\t z", U64, (0, 3))))
    return dict(image_sha256=IMAGE, enumeration_text=enumeration,
                expected_suites=(M.ExpectedSuite(suite, (b"quiet", b"missing", b"hit:raw\xfd"),
                                                 M.SuiteOutcome.PASSED),),
                observations=(observation(suite, b"hit:raw\xfd", b"\xff:child/ opaque", M.ObservationRole.CHILD, child),
                              observation(suite, b"quiet", b"quiet", M.ObservationRole.PARENT, zero),
                              observation(suite, b"*", b"setup", M.ObservationRole.SETUP, zero),
                              observation(suite, b"hit:raw\xfd", b"a-parent", M.ObservationRole.PARENT, parent)),
                row_evidence=tuple(claims(suite, name) for name in (b"quiet", b"*", b"hit:raw\xfd", b"missing")),
                limits=ample())


def simple_fixture():
    zero = text(((b"a", 1, (0,)), (b"b", 2, (0, 0))))
    return dict(image_sha256=IMAGE,
                enumeration_text=text(((b"a", 1, (5,)), (b"b", 2, (0, 0)))),
                expected_suites=(M.ExpectedSuite(b"s", (b"t",), M.SuiteOutcome.PASSED),),
                observations=(observation(b"s", b"*", b"s", M.ObservationRole.SETUP, zero),
                              observation(b"s", b"t", b"z", M.ObservationRole.PARENT, zero)),
                row_evidence=(claims(b"s", b"*"), claims(b"s", b"t")), limits=ample())


SIMPLE_PROFILES = (b"CBM_PROFILE_MAP\t2\t" + IMAGE_HEX + b"\t2\n"
                   b"0\t61\t0000000000000001\t1\n1\t62\t0000000000000002\t2\n")
SIMPLE_TESTS = b"s:*\tcomplete\t\t\ns:t\tcomplete\t\t\n"


def row_view(wire):
    """Small assertion-side splitter for the already-expected four-field wire."""
    if wire and (not wire.endswith(b"\n") or b"\r" in wire or b"\0" in wire):
        raise AssertionError("producer test wire has unsupported record delimiters")
    rows = {}
    for line in wire.split(b"\n")[:-1]:
        key, state, reason, ids = line.split(b"\t")
        if key in rows:
            raise AssertionError("producer duplicated an expected row")
        rows[key] = (state, reason, ids)
    return rows


class CountCancel:
    def __init__(self, at=0, failure=None):
        self.at = at
        self.failure = failure
        self.calls = 0

    def __call__(self, context):
        self.calls += 1
        if self.at and self.calls >= self.at:
            if self.failure is not None:
                raise self.failure
            return True
        return False


class DependencyControl(unittest.TestCase):
    def test_real_parser_preserves_full_identity_and_zero_records(self):
        raw = text(((b"\xff:\t z", U64, (0, 0)), (b"a", HIGH, (0,)), (b"a", 0, (0, 7))))
        limits = P.ParseLimits(4096, 3, 5, 7)
        parsed = P.parse_profile_text(raw, limits)
        self.assertEqual(tuple((r.raw_name, r.function_hash, r.counters) for r in parsed),
                         ((b"a", 0, (0, 7)), (b"a", HIGH, (0,)), (b"\xff:\t z", U64, (0, 0))))
        with self.assertRaises(P.MalformedProfileError):
            P.parse_profile_text(raw[:-1], limits)


class ProfileMapV2Tests(unittest.TestCase):
    def assert_result(self, result, profiles, tests):
        self.assertIs(type(result), M.ProducedMap)
        self.assertIs(type(result.profiles_tsv), bytes)
        self.assertIs(type(result.tests_tsv), bytes)
        self.assertIs(type(result.profiles_sha256), bytes)
        self.assertIs(type(result.tests_sha256), bytes)
        self.assertEqual(result.profiles_tsv, profiles)
        self.assertEqual(result.tests_tsv, tests)
        self.assertEqual(result.profiles_sha256, hashlib.sha256(profiles).digest())
        self.assertEqual(result.tests_sha256, hashlib.sha256(tests).digest())
        self.assertEqual(len(result.profiles_sha256), 32)
        self.assertEqual(len(result.tests_sha256), 32)

    def gold(self):
        result = M.build_profile_map_v2(**gold_fixture())
        self.assert_result(result, GOLD_PROFILES, GOLD_TESTS)
        self.assertEqual(result.profiles_sha256, GOLD_PROFILES_SHA)
        self.assertEqual(result.tests_sha256, GOLD_TESTS_SHA)
        self.assertEqual(result.profiles_tsv, (FIXTURE_DIR / "profiles.tsv").read_bytes())
        self.assertEqual(result.tests_tsv, (FIXTURE_DIR / "tests.tsv").read_bytes())
        return result

    def assert_row(self, fixture, key, state, reason, ids=b""):
        result = M.build_profile_map_v2(**fixture)
        self.assert_result(result, SIMPLE_PROFILES, result.tests_tsv)
        rows = row_view(result.tests_tsv)
        self.assertEqual(set(rows), {b"s:*", b"s:t"})
        self.assertEqual(rows[key], (state, reason, ids))
        return result

    def test_exact_wire_full_identity_zero_ids_and_input_order(self):
        self.gold()
        f = gold_fixture()
        f["expected_suites"] = (replace(f["expected_suites"][0],
                                        tests=tuple(reversed(f["expected_suites"][0].tests))),)
        f["observations"] = tuple(reversed(f["observations"]))
        f["row_evidence"] = tuple(reversed(f["row_evidence"]))
        self.assert_result(M.build_profile_map_v2(**f), GOLD_PROFILES, GOLD_TESTS)
        # Distinct raw names may share the entire hash; neither is collapsed.
        old = b"\n18446744073709551615\n# Num Counters:"
        new = b"\n0\n# Num Counters:"
        f = gold_fixture()
        f["enumeration_text"] = f["enumeration_text"].replace(old, new)
        f["observations"] = tuple(replace(o, text=o.text.replace(old, new)) for o in f["observations"])
        self.assert_result(M.build_profile_map_v2(**f),
                           GOLD_PROFILES.replace(b"ffffffffffffffff", b"0000000000000000"), GOLD_TESTS)
        empty = simple_fixture()
        self.assert_result(M.build_profile_map_v2(**empty), SIMPLE_PROFILES, SIMPLE_TESTS)
        empty["expected_suites"] = ()
        empty["observations"] = ()
        empty["row_evidence"] = ()
        self.assert_result(M.build_profile_map_v2(**empty), SIMPLE_PROFILES, b"")
        empty["expected_suites"] = (M.ExpectedSuite(b"s", (), M.SuiteOutcome.PASSED),)
        self.assert_result(M.build_profile_map_v2(**empty), SIMPLE_PROFILES,
                           b"s:*\tincomplete\trow_evidence_missing;missing_setup\t\n")
        empty["observations"] = simple_fixture()["observations"][:1]
        empty["row_evidence"] = (claims(b"s", b"*"),)
        self.assert_result(M.build_profile_map_v2(**empty), SIMPLE_PROFILES, b"s:*\tcomplete\t\t\n")

    def test_expected_rows_primary_setup_outcomes_and_all_reason_codes(self):
        self.gold()
        base = simple_fixture()
        self.assert_result(M.build_profile_map_v2(**base), SIMPLE_PROFILES, SIMPLE_TESTS)
        for outcome, reason in ((M.SuiteOutcome.FAILED, b"suite_failed"),
                                (M.SuiteOutcome.TIMED_OUT, b"suite_timeout"),
                                (M.SuiteOutcome.UNKNOWN, b"suite_unknown")):
            with self.subTest(outcome=outcome):
                f = simple_fixture()
                f["expected_suites"] = (replace(f["expected_suites"][0], outcome=outcome),)
                result = self.assert_row(f, b"s:t", b"incomplete", reason)
                self.assertEqual(row_view(result.tests_tsv)[b"s:*"], (b"incomplete", reason, b""))
        fields = (("interval_complete", b"interval_unproved"),
                  ("runtime_compatible", b"runtime_unproved"),
                  ("same_image", b"image_unproved"), ("children_complete", b"children_unproved"))
        for field, reason in fields:
            with self.subTest(field=field):
                f = simple_fixture()
                f["row_evidence"] = (f["row_evidence"][0], replace(f["row_evidence"][1], **{field: False}))
                self.assert_row(f, b"s:t", b"incomplete", reason)
        f = simple_fixture()
        f["row_evidence"] = f["row_evidence"][:1]
        self.assert_row(f, b"s:t", b"incomplete", b"row_evidence_missing")
        for test_name, reason in ((b"t", b"missing_parent"), (b"*", b"missing_setup")):
            f = simple_fixture()
            f["observations"] = tuple(o for o in f["observations"] if o.test != test_name)
            self.assert_row(f, b"s:" + test_name, b"incomplete", reason)
        for value, reason in ((None, b"missing_parent;observation_unavailable"),
                              (b"", b"missing_parent;observation_malformed")):
            f = simple_fixture()
            f["observations"] = (f["observations"][0], replace(f["observations"][1], text=value))
            self.assert_row(f, b"s:t", b"incomplete", reason)
        f = simple_fixture()
        f["observations"] = (f["observations"][0], replace(f["observations"][1],
                                 text=record(b"a", 1, (7,))))
        self.assert_row(f, b"s:t", b"incomplete", b"partial_stream", b"0")

    def test_stream_unions_child_does_not_replace_primary_and_reason_order(self):
        self.gold()
        full = text(((b"a", 1, (0,)), (b"b", 2, (0, U64))))
        f = simple_fixture()
        child = observation(b"s", b"t", b"child", M.ObservationRole.CHILD, full)
        f["observations"] += (child, replace(child, key=b"child-again"))
        f["limits"] = replace(ample(), max_id_references=1)
        self.assert_row(f, b"s:t", b"complete", b"", b"1")
        f["observations"] = (f["observations"][0], child)
        self.assert_row(f, b"s:t", b"incomplete", b"missing_parent", b"1")
        setup_child = replace(child, test=b"*", key=b"setup-child")
        f = simple_fixture()
        f["observations"] = (setup_child, f["observations"][1])
        self.assert_row(f, b"s:*", b"incomplete", b"missing_setup", b"1")
        f = simple_fixture()
        f["observations"] += (replace(setup_child, role=M.ObservationRole.SETUP, key=b"second-setup"),)
        self.assert_row(f, b"s:*", b"complete", b"", b"1")
        f = simple_fixture()
        f["expected_suites"] = (replace(f["expected_suites"][0], outcome=M.SuiteOutcome.FAILED),)
        f["row_evidence"] = (f["row_evidence"][0], claims(b"s", b"t", (False,) * 4))
        f["observations"] = (f["observations"][0],
            replace(f["observations"][1], text=None),
            replace(child, key=b"bad", text=b"not a complete record"),
            replace(child, key=b"partial", text=record(b"a", 1, (2,))))
        reason = (b"suite_failed;interval_unproved;runtime_unproved;image_unproved;"
                  b"children_unproved;missing_parent;observation_unavailable;"
                  b"observation_malformed;partial_stream")
        self.assert_row(f, b"s:t", b"incomplete", reason, b"0")
        f["row_evidence"] = f["row_evidence"][:1]
        reason = (b"suite_failed;row_evidence_missing;missing_parent;observation_unavailable;"
                  b"observation_malformed;partial_stream")
        self.assert_row(f, b"s:t", b"incomplete", reason, b"0")

    def test_each_aggregate_cap_at_boundary_and_one_below(self):
        self.gold()
        f, expected, exact = budget_fixture()
        f["limits"] = exact
        self.assert_result(M.build_profile_map_v2(**f), SIMPLE_PROFILES, expected)
        for field in M.ProfileMapLimits.__slots__:
            value = getattr(exact, field)
            self.assertGreater(value, 1, "one-below must remain a valid positive configured cap")
            with self.subTest(cap=field):
                f["limits"] = replace(exact, **{field: value - 1})
                with self.assertRaises(M.ProfileMapLimitExceededError):
                    M.build_profile_map_v2(**f)
        f["limits"] = exact
        self.assert_result(M.build_profile_map_v2(**f), SIMPLE_PROFILES, expected)

    def test_malformed_attempt_discard_and_zero_remaining_allowance(self):
        self.gold()
        f, expected, exact = budget_fixture()
        f["limits"] = exact
        self.assert_result(M.build_profile_map_v2(**f), SIMPLE_PROFILES, expected)
        # The malformed child's positive prefix must not leak a hit or consume
        # successful record/name/counter allowances needed by the later parent.
        zero = simple_fixture()["observations"][1].text
        malformed = record(b"a", 1, (7,)) + b"b\n# Wrong marker\n"
        f["observations"] = (f["observations"][0], replace(f["observations"][1], text=zero),
                              observation(b"s", b"t", b"a", M.ObservationRole.CHILD, malformed))
        f["limits"] = replace(exact, max_observations=3,
            max_input_bytes=exact.max_input_bytes + len(malformed) + 3,
            max_name_bytes=exact.max_name_bytes + 3, max_output_bytes=4096)
        result = self.assert_row(f, b"s:t", b"incomplete", b"observation_malformed")
        self.assertEqual(row_view(result.tests_tsv)[b"s:*"], (b"complete", b"", b"0"))
        for field, consumed in (("max_records", 4), ("max_counter_values", 6), ("max_name_bytes", 16)):
            f, _, _ = budget_fixture()
            f["limits"] = replace(ample(), **{field: consumed})
            f["observations"] = (f["observations"][0], replace(f["observations"][1], text=None))
            with self.subTest(exhausted=field):
                self.assert_row(f, b"s:t", b"incomplete", b"missing_parent;observation_unavailable")
                f["observations"] = (f["observations"][0], replace(f["observations"][1], text=b""))
                with self.assertRaises(M.ProfileMapLimitExceededError):
                    M.build_profile_map_v2(**f)

    def test_whole_call_preflight_and_parser_limit_is_not_malformed(self):
        self.gold()
        f = simple_fixture()
        f["enumeration_text"] = b""
        f["observations"] += (observation(b"s", b"t", b"zz", M.ObservationRole.CHILD, b"x" * 4096),)
        f["limits"] = replace(ample(), max_input_bytes=1024)
        with self.assertRaises(M.ProfileMapLimitExceededError):
            M.build_profile_map_v2(**f)
        f = simple_fixture()
        f["enumeration_text"] = b""
        f["observations"] += (observation(b"s", b"unexpected", b"zz", M.ObservationRole.CHILD, None),)
        with self.assertRaises(M.ProfileMapInvalidArgumentError):
            M.build_profile_map_v2(**f)
        # An attempt sees a genuine record cap before its later malformed tail.
        f = simple_fixture()
        f["observations"] = (f["observations"][0], replace(f["observations"][1],
            text=record(b"a", 1, (0,)) + record(b"b", 2, (0, 0)) + b"broken"))
        f["limits"] = replace(ample(), max_records=5)
        with self.assertRaises(M.ProfileMapLimitExceededError) as caught:
            M.build_profile_map_v2(**f)
        self.assertIsInstance(caught.exception.__cause__, P.LimitExceededError)

    def test_callback_provenance_invalid_results_and_entry_order(self):
        held = self.gold()
        f = simple_fixture()
        observed = CountCancel()
        self.assert_result(M.build_profile_map_v2(**f, cancel=observed), SIMPLE_PROFILES, SIMPLE_TESTS)
        self.assertGreater(observed.calls, 0)
        token = object()

        def context_predicate(context):
            if context is not token:
                raise AssertionError("callback context was not preserved")
            return False

        self.assert_result(M.build_profile_map_v2(**f, cancel=context_predicate,
                                                 cancel_context=token), SIMPLE_PROFILES, SIMPLE_TESTS)
        positions = sampled_positions(observed.calls)
        failures = (ValueError("caller sentinel"), P.MalformedProfileError("caller malformed class"),
                    P.LimitExceededError("caller limit class"), P.ParseCancelledError("caller cancel class"),
                    M.ProfileMapEnumerationError("caller map class"), MemoryError("caller memory class"))
        for failure in failures:
            for position in positions:
                with self.subTest(error=type(failure).__name__, position=position):
                    callback = CountCancel(position, failure)
                    caught = None
                    result = None
                    try:
                        result = M.build_profile_map_v2(**f, cancel=callback)
                    except BaseException as error:
                        caught = error
                    if callback.calls >= position:
                        self.assertIs(caught, failure)
                        self.assertIsNone(result)
                    else:
                        self.assertIsNone(caught)
                        self.assert_result(result, SIMPLE_PROFILES, SIMPLE_TESTS)
                    if position == 1:
                        self.assertIs(caught, failure)
        for value in (None, 0, 1, b"", object()):
            with self.subTest(callback_result=repr(value)):
                with self.assertRaises(M.ProfileMapInvalidArgumentError):
                    M.build_profile_map_v2(**f, cancel=lambda context, answer=value: answer)
        invalid = dict(f, image_sha256=b"short")
        never = CountCancel(1)
        with self.assertRaises(M.ProfileMapInvalidArgumentError):
            M.build_profile_map_v2(**invalid, cancel=never)
        self.assertEqual(never.calls, 0)
        self.assert_result(held, GOLD_PROFILES, GOLD_TESTS)
        self.assert_result(M.build_profile_map_v2(**f), SIMPLE_PROFILES, SIMPLE_TESTS)

    def test_bounded_cancellation_immutable_results_and_independent_calls(self):
        held = self.gold()
        f, profiles, tests = stress_fixture()
        observed = CountCancel()
        result = M.build_profile_map_v2(**f, cancel=observed, cancel_context=object())
        self.assert_result(result, profiles, tests)
        self.assertGreater(observed.calls, 0)
        for position in sampled_positions(observed.calls):
            callback = CountCancel(position)
            returned = None
            cancelled = False
            try:
                returned = M.build_profile_map_v2(**f, cancel=callback)
            except M.ProfileMapCancelledError:
                cancelled = True
            with self.subTest(position=position):
                if callback.calls >= position:
                    self.assertTrue(cancelled)
                    self.assertIsNone(returned)
                else:
                    self.assertFalse(cancelled)
                    self.assert_result(returned, profiles, tests)
                if position == 1:
                    self.assertTrue(cancelled)
                self.assert_result(result, profiles, tests)
                self.assert_result(held, GOLD_PROFILES, GOLD_TESTS)
        self.assert_result(M.build_profile_map_v2(**f), profiles, tests)
        del f
        self.assert_result(result, profiles, tests)
        for field in M.ProducedMap.__slots__:
            with self.subTest(immutable=field):
                with self.assertRaises((FrozenInstanceError, AttributeError)):
                    setattr(result, field, b"changed")
        self.assert_result(result, profiles, tests)

    def test_strict_schema_membership_roles_names_and_exact_types(self):
        self.gold()
        for label, fixture in bad_schema_cases():
            with self.subTest(case=label):
                with self.assertRaises(M.ProfileMapInvalidArgumentError):
                    M.build_profile_map_v2(**fixture)
        for label, fixture in bad_type_cases():
            with self.subTest(case=label):
                with self.assertRaises(M.ProfileMapInvalidArgumentError):
                    M.build_profile_map_v2(**fixture)

    def test_enumeration_failure_and_observation_incompatibility_are_fatal(self):
        self.gold()
        valid = simple_fixture()["enumeration_text"]
        malformed = (b"", valid[:-1], valid + b"# unknown section\n",
                     record(b"a\r", 1, (0,)), record(b"a\0", 1, (0,)),
                     record(b"a", 1, (0,)) * 2,
                     record(b"a", 1, (0,)) + record(b"a", 1, (0, 0)),
                     valid.replace(b"# Func Hash:", b"# Unknown:", 1),
                     valid.replace(b"\n1\n# Num", b"\n01\n# Num", 1),
                     record(b"a", U64 + 1, (0,)))
        for value in malformed:
            f = simple_fixture()
            f["enumeration_text"] = value
            with self.subTest(enumeration=value[:24]):
                with self.assertRaises(M.ProfileMapEnumerationError) as caught:
                    M.build_profile_map_v2(**f)
                self.assertIsInstance(caught.exception.__cause__, P.MalformedProfileError)
        incompatible = (record(b"a", 3, (0,)), record(b"new", 1, (0,)),
                        record(b"a", 1, (0, 0)))
        for value in incompatible:
            for incomplete in (False, True):
                f = simple_fixture()
                f["observations"] = (f["observations"][0], replace(f["observations"][1], text=value))
                if incomplete:
                    f["expected_suites"] = (replace(f["expected_suites"][0], outcome=M.SuiteOutcome.FAILED),)
                    f["row_evidence"] = (f["row_evidence"][0], claims(b"s", b"t", (False,) * 4))
                with self.subTest(identity=value[:24], incomplete=incomplete):
                    with self.assertRaises(M.ProfileMapCompatibilityError):
                        M.build_profile_map_v2(**f)


def budget_fixture():
    f = simple_fixture()
    setup = text(((b"a", 1, (1,)), (b"b", 2, (0, 0))))
    parent = text(((b"a", 1, (1,)), (b"b", 2, (0, 1))))
    f["observations"] = (replace(f["observations"][0], text=setup),
                          replace(f["observations"][1], text=parent))
    expected = b"s:*\tcomplete\t\t0\ns:t\tcomplete\t\t0 1\n"
    # B = expected (1+1) + two observation routes (1+1+1)*2 +
    # two evidence routes (1+1)*2 = 12. Three two-record streams contribute
    # six names of one byte and nine counter values. Emitted ID refs = 1+2.
    exact = M.ProfileMapLimits(32 + len(f["enumeration_text"]) + len(setup) + len(parent) + 12,
                               2, 2, 2, 6, 9, 18, 3, len(SIMPLE_PROFILES) + len(expected))
    return f, expected, exact


def sampled_positions(calls):
    # At most five observed positions, with no phase/count or time guarantee.
    return tuple(sorted({1, calls // 4 + 1, calls // 2 + 1, calls - calls // 4, calls}))


def stress_fixture():
    first = b"a" * 4097
    second = b"a" * 4096 + b"b"
    zero = text(((first, 0, (0, 0)), (second, U64, (0,))))
    hit = text(((first, 0, (0, 1)), (second, U64, (0,))))
    enumeration = text(((second, U64, (0,)), (first, 0, (9, 0))))
    names = tuple(b"x" * 4097 + ("%03d" % i).encode("ascii") for i in range(128))
    expected = (M.ExpectedSuite(b"s", tuple(reversed(names)), M.SuiteOutcome.PASSED),)
    observations = (observation(b"s", b"*", b"setup", M.ObservationRole.SETUP, zero),) + tuple(
        observation(b"s", name, ("k%03d" % i).encode("ascii"), M.ObservationRole.PARENT, hit)
        for i, name in enumerate(reversed(names)))
    evidence = (claims(b"s", b"*"),) + tuple(claims(b"s", name) for name in names)
    profiles = (b"CBM_PROFILE_MAP\t2\t" + IMAGE_HEX + b"\t2\n0\t" + first.hex().encode("ascii") +
                b"\t0000000000000000\t2\n1\t" + second.hex().encode("ascii") +
                b"\tffffffffffffffff\t1\n")
    tests = b"s:*\tcomplete\t\t\n" + b"".join(b"s:" + name + b"\tcomplete\t\t0\n" for name in names)
    f = dict(image_sha256=IMAGE, enumeration_text=enumeration, expected_suites=expected,
             observations=observations, row_evidence=evidence, limits=ample())
    return f, profiles, tests


def renamed_fixture(suite=None, test=None):
    f = simple_fixture()
    suite = b"s" if suite is None else suite
    test = b"t" if test is None else test
    f["expected_suites"] = (M.ExpectedSuite(suite, (test,), M.SuiteOutcome.PASSED),)
    f["observations"] = tuple(replace(o, suite=suite, test=test if o.test == b"t" else b"*")
                              for o in f["observations"])
    f["row_evidence"] = tuple(replace(e, suite=suite, test=test if e.test == b"t" else b"*")
                              for e in f["row_evidence"])
    return f


def bad_schema_cases():
    for value in (b"", b"s:", b"s\0", b"s\t", b"s\r", b"s\n"):
        yield ("suite-name-" + repr(value), renamed_fixture(suite=value))
    for value in (b"", b"*", b"t\0", b"t\t", b"t\r", b"t\n"):
        yield ("test-name-" + repr(value), renamed_fixture(test=value))
    for value in (b"", b"k\0", b"k\t", b"k\r", b"k\n"):
        f = simple_fixture()
        f["observations"] = (f["observations"][0], replace(f["observations"][1], key=value))
        yield ("observation-key-" + repr(value), f)
    f = simple_fixture()
    f["expected_suites"] *= 2
    yield ("duplicate-suite", f)
    f = simple_fixture()
    f["expected_suites"] = (replace(f["expected_suites"][0], tests=(b"t", b"t")),)
    yield ("duplicate-test", f)
    for field in ("observations", "row_evidence"):
        f = simple_fixture()
        f[field] += (f[field][0],)
        yield ("duplicate-" + field, f)
    f = simple_fixture()
    f["observations"] += (replace(f["observations"][1], key=b"another-parent"),)
    yield ("two-parent-keys", f)
    f = simple_fixture()
    f["observations"] += (replace(f["observations"][1], role=M.ObservationRole.CHILD),)
    yield ("same-key-different-role", f)
    f = simple_fixture()
    f["observations"] += (replace(f["observations"][1], text=None),)
    yield ("same-key-different-text", f)
    for target in ("observations", "row_evidence"):
        for field, value in (("suite", b"other"), ("test", b"other")):
            f = simple_fixture()
            f[target] = (f[target][0], replace(f[target][1], **{field: value}))
            yield ("unexpected-" + target + "-" + field, f)
    for index, role in ((0, M.ObservationRole.PARENT), (1, M.ObservationRole.SETUP)):
        f = simple_fixture()
        observations = list(f["observations"])
        observations[index] = replace(observations[index], role=role)
        f["observations"] = tuple(observations)
        yield ("invalid-role-" + str(index), f)


def bad_type_cases():
    for field, value in (("image_sha256", b"x" * 31), ("image_sha256", b"x" * 33),
                         ("image_sha256", bytearray(32)), ("enumeration_text", "text"),
                         ("expected_suites", []), ("observations", []), ("row_evidence", []),
                         ("limits", None), ("cancel", 17)):
        f = simple_fixture()
        f[field] = value
        yield ("top-level-" + field + "-" + repr(type(value)), f)
    derived_bytes = type("DerivedBytes", (bytes,), {})
    derived_tuple = type("DerivedTuple", (tuple,), {})
    for field, value in (("image_sha256", derived_bytes(IMAGE)),
                         ("observations", derived_tuple(simple_fixture()["observations"]))):
        f = simple_fixture()
        f[field] = value
        yield ("subclass-" + field, f)
    for field, value in (("tests", [b"t"]), ("outcome", "passed"), ("suite", "s")):
        f = simple_fixture()
        f["expected_suites"] = (replace(f["expected_suites"][0], **{field: value}),)
        yield ("expected-field-" + field, f)
    for field, value in (("role", "parent"), ("text", bytearray()), ("key", "key"),
                         ("suite", b"s".decode("ascii")), ("test", 1)):
        f = simple_fixture()
        f["observations"] = (f["observations"][0], replace(f["observations"][1], **{field: value}))
        yield ("observation-field-" + field, f)
    for field in ("interval_complete", "runtime_compatible", "same_image", "children_complete"):
        f = simple_fixture()
        f["row_evidence"] = (f["row_evidence"][0], replace(f["row_evidence"][1], **{field: 1}))
        yield ("evidence-bool-" + field, f)
    for field in ("expected_suites", "observations", "row_evidence"):
        f = simple_fixture()
        f[field] = (object(),)
        yield ("record-type-" + field, f)
    f = simple_fixture()
    derived_expected = type("DerivedExpectedSuite", (M.ExpectedSuite,), {})
    f["expected_suites"] = (derived_expected(b"s", (b"t",), M.SuiteOutcome.PASSED),)
    yield ("dataclass-subclass", f)
    for field in ("observations", "row_evidence"):
        f = simple_fixture()
        old = f[field][0]
        derived = type("DerivedRecord", (type(old),), {})
        f[field] = (derived(*(getattr(old, name) for name in type(old).__slots__)),) + f[field][1:]
        yield ("record-subclass-" + field, f)
    f = simple_fixture()
    derived_limits = type("DerivedLimits", (M.ProfileMapLimits,), {})
    f["limits"] = derived_limits(*(getattr(f["limits"], name) for name in M.ProfileMapLimits.__slots__))
    yield ("limits-subclass", f)
    f = simple_fixture()
    derived_int = type("DerivedInt", (int,), {})
    f["limits"] = replace(f["limits"], max_rows=derived_int(512))
    yield ("limit-value-subclass", f)
    for field in M.ProfileMapLimits.__slots__:
        for value in (0, -1, True, sys.maxsize + 1):
            f = simple_fixture()
            f["limits"] = replace(f["limits"], **{field: value})
            yield ("invalid-limit-" + field + "-" + repr(value), f)
    f = simple_fixture()
    f["limits"] = replace(f["limits"], max_ids=2147483648)
    yield ("ids-int-representation", f)


def load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, str(path))
    if spec is None or spec.loader is None:
        raise RuntimeError("cannot load requested module: " + str(path))
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def main():
    global M, P, FIXTURE_DIR
    arguments = argparse.ArgumentParser(description=__doc__)
    default_module = Path(__file__).resolve().parents[1] / "scripts/test-impact/profile_map_v2.py"
    arguments.add_argument("--module", type=Path, default=default_module)
    arguments.add_argument("--parser", type=Path,
                           help="real unchanged parser; default profile_text_parser.py beside --module")
    arguments.add_argument("--fixtures", type=Path, default=FIXTURE_DIR,
                           help="shared golden directory; default fixtures/profile_map_v2 beside this test")
    selected = arguments.parse_args()
    FIXTURE_DIR = selected.fixtures.resolve()
    module_path = selected.module.resolve()
    parser_path = (selected.parser or module_path.with_name("profile_text_parser.py")).resolve()
    if not module_path.is_file() or not parser_path.is_file():
        arguments.error("--module and --parser must name existing source files")
    sys.dont_write_bytecode = True
    # Freeze the actual dependency before importing the chosen producer; both
    # baseline and implementation use this real parser, never a fake grammar.
    P = load_module("profile_text_parser", parser_path)
    M = load_module("profile_map_v2_under_test", module_path)
    suite = unittest.TestSuite((unittest.defaultTestLoader.loadTestsFromTestCase(DependencyControl),
                               unittest.defaultTestLoader.loadTestsFromTestCase(ProfileMapV2Tests)))
    return 0 if unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful() else 1


if __name__ == "__main__":
    sys.exit(main())
