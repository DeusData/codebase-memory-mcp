#!/usr/bin/env python3
"""Fail-closed source audit for raw C networking calls.

This is intentionally a small lexical scanner rather than a grep expression:
comments and string/character literals cannot satisfy an allowance, calls at
column one or with whitespace before ``(`` are still counted, and multiple
calls on one line remain distinct.  It is a review tripwire, not a substitute
for the runtime network and CodeQL gates.
"""

from __future__ import annotations

import dataclasses
import pathlib
import re
import sys
from typing import Sequence


RAW_FUNCTIONS = ("socket", "connect", "sendto")
CALL_RE = re.compile(
    r"(?<![A-Za-z0-9_])(?P<name>" + "|".join(RAW_FUNCTIONS) + r")[\t\r\n ]*\("
)


@dataclasses.dataclass(frozen=True)
class Call:
    name: str
    line: int
    start: int
    end: int
    text: str


@dataclasses.dataclass(frozen=True)
class FunctionRange:
    name: str
    body_start: int
    body_end: int


@dataclasses.dataclass(frozen=True)
class Allowance:
    file: str
    function: str
    expected: int
    transport: str
    scope: str
    justification: str


def _mask_non_code(source: str) -> str:
    """Replace C comments and literals with spaces while preserving offsets."""

    chars = list(source)
    index = 0
    state = "code"
    quote = ""
    while index < len(chars):
        current = chars[index]
        following = chars[index + 1] if index + 1 < len(chars) else ""
        if state == "code":
            if current == "/" and following == "/":
                chars[index] = chars[index + 1] = " "
                index += 2
                state = "line-comment"
                continue
            if current == "/" and following == "*":
                chars[index] = chars[index + 1] = " "
                index += 2
                state = "block-comment"
                continue
            if current in ('"', "'"):
                quote = current
                chars[index] = " "
                index += 1
                state = "literal"
                continue
            index += 1
            continue
        if state == "line-comment":
            if current == "\n":
                state = "code"
            else:
                chars[index] = " "
            index += 1
            continue
        if state == "block-comment":
            if current == "*" and following == "/":
                chars[index] = chars[index + 1] = " "
                index += 2
                state = "code"
            else:
                if current != "\n":
                    chars[index] = " "
                index += 1
            continue
        if state == "literal":
            if current == "\\" and following:
                chars[index] = " "
                if following != "\n":
                    chars[index + 1] = " "
                index += 2
                continue
            if current == quote:
                chars[index] = " "
                index += 1
                state = "code"
                continue
            if current != "\n":
                chars[index] = " "
            index += 1
    return "".join(chars)


def _scan_calls(masked: str) -> list[Call]:
    calls: list[Call] = []
    for match in CALL_RE.finditer(masked):
        opening = match.end() - 1
        depth = 0
        closing = len(masked)
        for cursor in range(opening, len(masked)):
            if masked[cursor] == "(":
                depth += 1
            elif masked[cursor] == ")":
                depth -= 1
                if depth == 0:
                    closing = cursor + 1
                    break
        raw = masked[match.start("name") : closing]
        calls.append(
            Call(
                name=match.group("name"),
                line=masked.count("\n", 0, match.start("name")) + 1,
                start=match.start("name"),
                end=closing,
                text=re.sub(r"\s+", " ", raw).strip(),
            )
        )
    return calls


def _name_before_body(masked: str, opening: int) -> str | None:
    prefix = masked[max(0, opening - 4096) : opening]
    match = re.search(r"([A-Za-z_][A-Za-z0-9_]*)\s*\([^;{}]*\)\s*$", prefix, re.DOTALL)
    return match.group(1) if match else None


def _function_ranges(masked: str) -> list[FunctionRange]:
    ranges: list[FunctionRange] = []
    depth = 0
    opening = -1
    function_name: str | None = None
    for index, character in enumerate(masked):
        if character == "{":
            if depth == 0:
                opening = index
                function_name = _name_before_body(masked, index)
            depth += 1
        elif character == "}" and depth > 0:
            depth -= 1
            if depth == 0 and function_name is not None:
                ranges.append(FunctionRange(function_name, opening, index + 1))
                opening = -1
                function_name = None
    return ranges


def _scope_for(call: Call, ranges: Sequence[FunctionRange]) -> FunctionRange | None:
    for scope in ranges:
        if scope.body_start < call.start < scope.body_end:
            return scope
    return None


def _fullmatch(pattern: str, call: Call) -> bool:
    return re.fullmatch(pattern, call.text) is not None


SOCKET_UNIX = r"socket\s*\(\s*AF_UNIX\s*,\s*SOCK_STREAM\s*,\s*0\s*\)"
SOCKET_LOOPBACK = r"socket\s*\(\s*AF_INET\s*,\s*SOCK_STREAM\s*,\s*0\s*\)"
CONNECT_UNIX = (
    r"connect\s*\(\s*fd\s*,\s*\(\s*const\s+struct\s+sockaddr\s*\*\s*\)\s*"
    r"address\s*,\s*address_length\s*\)"
)
CONNECT_LOOPBACK = (
    r"connect\s*\(\s*socket_handle\s*,\s*\(\s*const\s+struct\s+sockaddr\s*\*\s*\)"
    r"\s*&\s*address\s*,\s*sizeof\s*\(\s*address\s*\)\s*\)"
)


def _loopback_connect_witness(masked: str, call: Call, scope: FunctionRange) -> bool:
    body = masked[scope.body_start + 1 : scope.body_end - 1]
    call_offset = call.start - scope.body_start - 1
    # The complete witness must be one unconditional, contiguous statement
    # sequence in the same helper. Merely finding the three assignments in
    # order is insufficient: `if (false) address.sin_family = ...` must not
    # satisfy a local-only guarantee.
    witness = re.compile(
        r"\bstruct\s+sockaddr_in\s+address\s*;\s*"
        r"memset\s*\(\s*&\s*address\s*,\s*0\s*,\s*sizeof\s*\(\s*address\s*\)\s*\)\s*;\s*"
        r"address\s*\.\s*sin_family\s*=\s*AF_INET\s*;\s*"
        r"address\s*\.\s*sin_port\s*=\s*htons\s*\(\s*\(\s*unsigned\s+short\s*\)"
        r"\s*port\s*\)\s*;\s*"
        r"address\s*\.\s*sin_addr\s*\.\s*s_addr\s*=\s*htonl\s*\(\s*0x7F000001U\s*\)\s*;\s*"
        r"if\s*\(\s*$",
        re.DOTALL,
    )
    return witness.search(body[:call_offset]) is not None


def _semantic_ok(masked: str, calls: Sequence[Call], allowance: Allowance) -> bool:
    ranges = _function_ranges(masked)
    scopes = [_scope_for(call, ranges) for call in calls]
    if any(scope is None or scope.name != allowance.scope for scope in scopes):
        return False
    if allowance.transport == "unix" and allowance.function == "socket":
        return all(_fullmatch(SOCKET_UNIX, call) for call in calls)
    if allowance.transport == "unix" and allowance.function == "connect":
        if not all(_fullmatch(CONNECT_UNIX, call) for call in calls):
            return False
        scope = scopes[0] if scopes else None
        if scope is None:
            return False
        header = masked[max(0, scope.body_start - 512) : scope.body_start]
        return (
            re.search(r"const\s+struct\s+sockaddr_un\s*\*\s*address", header) is not None
        )
    if allowance.transport == "loopback-v4" and allowance.function == "socket":
        return all(_fullmatch(SOCKET_LOOPBACK, call) for call in calls)
    if allowance.transport == "loopback-v4" and allowance.function == "connect":
        if len(calls) != 1 or not _fullmatch(CONNECT_LOOPBACK, calls[0]):
            return False
        scope = scopes[0] if scopes else None
        return scope is not None and _loopback_connect_witness(masked, calls[0], scope)
    return False


def _read_allowances(path: pathlib.Path) -> tuple[list[Allowance], list[str]]:
    allowances: list[Allowance] = []
    errors: list[str] = []
    for number, raw_line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if not raw_line.startswith("NETWORK:"):
            continue
        fields = raw_line.split(":", 6)
        if len(fields) != 7:
            errors.append(f"{path}:{number}: malformed NETWORK allowance")
            continue
        _, file_name, function, expected_text, transport, scope, justification = fields
        try:
            expected = int(expected_text)
        except ValueError:
            errors.append(f"{path}:{number}: invalid expected count {expected_text!r}")
            continue
        if expected <= 0 or function not in RAW_FUNCTIONS or not scope or not justification:
            errors.append(f"{path}:{number}: incomplete NETWORK allowance")
            continue
        allowances.append(
            Allowance(file_name, function, expected, transport, scope, justification)
        )
    return allowances, errors


def _audit(repo_root: pathlib.Path, allowlist_path: pathlib.Path) -> int:
    allowances, errors = _read_allowances(allowlist_path)
    by_key: dict[tuple[str, str], list[Allowance]] = {}
    for allowance in allowances:
        by_key.setdefault((allowance.file, allowance.function), []).append(allowance)
    consumed: set[tuple[str, str]] = set()
    for source_path in sorted((repo_root / "src").rglob("*.c")):
        relative = source_path.relative_to(repo_root).as_posix()
        if relative == "src/ui/httpd.c":
            continue
        masked = _mask_non_code(source_path.read_text(encoding="utf-8", errors="replace"))
        all_calls = _scan_calls(masked)
        for function in RAW_FUNCTIONS:
            calls = [call for call in all_calls if call.name == function]
            if not calls:
                continue
            key = (relative, function)
            entries = by_key.get(key, [])
            consumed.add(key)
            if len(entries) != 1:
                print(f"BLOCKED: {relative}: unexpected raw {function}() surface")
                print(f"  expected=none actual={len(calls)} transport=none semantic_ok=false")
                for call in calls:
                    print(f"  {call.line}: {call.text}")
                errors.append(f"{relative}:{function}: allowance count is {len(entries)}")
                continue
            allowance = entries[0]
            semantic_ok = _semantic_ok(masked, calls, allowance)
            if len(calls) != allowance.expected or not semantic_ok:
                print(f"BLOCKED: {relative}: unexpected raw {function}() surface")
                print(
                    f"  expected={allowance.expected} actual={len(calls)} "
                    f"transport={allowance.transport} scope={allowance.scope} "
                    f"semantic_ok={str(semantic_ok).lower()}"
                )
                for call in calls:
                    print(f"  {call.line}: {call.text}")
                errors.append(f"{relative}:{function}: reviewed shape changed")
            else:
                print(
                    f"REVIEWED: {relative}: {len(calls)} count-bounded "
                    f"{allowance.transport} {function}() call(s) in {allowance.scope}"
                )
    for key, entries in sorted(by_key.items()):
        if key not in consumed:
            errors.append(f"{key[0]}:{key[1]}: stale NETWORK allowance ({len(entries)} entry)")
    for error in errors:
        print(f"  policy-error: {error}", file=sys.stderr)
    return 1 if errors else 0


class SelfTestFailure(RuntimeError):
    pass


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise SelfTestFailure(message)


def _self_test() -> None:
    scanner_fixture = r'''
// socket(AF_INET, SOCK_STREAM, 0); connect(fd, bad, 1);
const char *decoy = "socket(AF_INET, SOCK_STREAM, 0)";
static void calls(void) {
socket (AF_INET6, SOCK_STREAM, 0); socket(AF_INET, SOCK_STREAM, 0);
  /* connect(fd, bad, 1); */ connect (fd, bad, 1);
}
'''
    scanned = _scan_calls(_mask_non_code(scanner_fixture))
    _require(
        [call.name for call in scanned] == ["socket", "socket", "connect"],
        "comments, literals, whitespace, or same-line calls were counted incorrectly",
    )
    _require([call.line for call in scanned] == [5, 5, 6], "call line mapping changed")

    safe = r'''
static bool loopback_probe(main_daemon_ctl_socket_t socket_handle, int port) {
    struct sockaddr_in address;
    memset(&address, 0, sizeof(address));
    address.sin_family = AF_INET;
    address.sin_port = htons((unsigned short)port);
    address.sin_addr.s_addr = htonl(0x7F000001U);
    if (connect (socket_handle, (const struct sockaddr *)&address, sizeof(address)) == 0) {
        return true;
    }
    return false;
}
static bool endpoint(void) {
    return socket (AF_INET, SOCK_STREAM, 0) >= 0;
}
'''
    masked = _mask_non_code(safe)
    calls = _scan_calls(masked)
    connect_allowance = Allowance(
        "src/main.c", "connect", 1, "loopback-v4", "loopback_probe", "self-test"
    )
    socket_allowance = Allowance(
        "src/main.c", "socket", 1, "loopback-v4", "endpoint", "self-test"
    )
    _require(
        _semantic_ok(masked, [call for call in calls if call.name == "connect"], connect_allowance),
        "safe loopback connect witness was rejected",
    )
    _require(
        _semantic_ok(masked, [call for call in calls if call.name == "socket"], socket_allowance),
        "safe loopback socket witness was rejected",
    )

    non_loopback = safe.replace("0x7F000001U", "0x08080808U")
    masked_bad = _mask_non_code(non_loopback)
    _require(
        not _semantic_ok(
            masked_bad,
            [call for call in _scan_calls(masked_bad) if call.name == "connect"],
            connect_allowance,
        ),
        "non-loopback address was accepted",
    )
    reassigned = safe.replace(
        "    if (connect",
        "    address.sin_addr.s_addr = htonl(0x08080808U);\n    if (connect",
    )
    masked_bad = _mask_non_code(reassigned)
    _require(
        not _semantic_ok(
            masked_bad,
            [call for call in _scan_calls(masked_bad) if call.name == "connect"],
            connect_allowance,
        ),
        "post-witness address reassignment was accepted",
    )
    conditional = safe.replace(
        "    address.sin_family = AF_INET;\n"
        "    address.sin_port = htons((unsigned short)port);\n"
        "    address.sin_addr.s_addr = htonl(0x7F000001U);",
        "    if (false) address.sin_family = AF_INET;\n"
        "    if (false) address.sin_port = htons((unsigned short)port);\n"
        "    if (false) address.sin_addr.s_addr = htonl(0x7F000001U);",
    )
    masked_bad = _mask_non_code(conditional)
    _require(
        not _semantic_ok(
            masked_bad,
            [call for call in _scan_calls(masked_bad) if call.name == "connect"],
            connect_allowance,
        ),
        "conditional loopback assignments were accepted",
    )
    wrong_family = safe.replace("socket (AF_INET, SOCK_STREAM, 0)",
                                "/* socket(AF_INET, SOCK_STREAM, 0) */\n"
                                "    socket (AF_INET6, SOCK_STREAM, 0)")
    masked_bad = _mask_non_code(wrong_family)
    _require(
        not _semantic_ok(
            masked_bad,
            [call for call in _scan_calls(masked_bad) if call.name == "socket"],
            socket_allowance,
        ),
        "wrong socket family plus comment decoy was accepted",
    )


def main(argv: Sequence[str]) -> int:
    try:
        _self_test()
    except SelfTestFailure as error:
        print(f"BLOCKED: network source audit self-test failed: {error}", file=sys.stderr)
        return 1
    if len(argv) == 2 and argv[1] == "--self-test":
        print("network source audit self-test: OK")
        return 0
    if len(argv) != 3:
        print(f"usage: {argv[0]} REPO_ROOT ALLOWLIST", file=sys.stderr)
        return 2
    return _audit(pathlib.Path(argv[1]).resolve(), pathlib.Path(argv[2]).resolve())


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
