# PR 2068: local hook setup review and verification

This delivery addendum records an independent review of the explicit agent setup flow. No coding-agent or model session was started. No account was created, and no personal client configuration was changed.

## Implemented boundary

The **Connect your agent** disclosure reads the selected project's repository root only when opened. It offers a download of the bundled Python hook and a shell-quoted installation command. Downloading or copying the command does not install anything. The user must review and execute it. The installer changes only that repository's `.claude/hooks/cbm-atlas-trace.py` and `.claude/settings.local.json`; the browser does not run it.

The installer preserves unrelated settings and hooks, rejects malformed or symlinked destinations, and is idempotent for its exact configuration. A review found that an existing identical command with a different matcher could previously produce two executions. The installer now refuses any conflicting entry referencing its installed script, including the ordinary relative-path form. Seven Python tests pass, covering merge preservation, repeat installation, conflicting matchers/timeouts, path quoting, destination rejection, sequence allocation and delivery acknowledgment.

A second review finding concerned source evidence: searching `old_string` after an edit can find a different, unchanged occurrence. The hook no longer reads source files to guess edit positions. Only explicit `Read` ranges are recorded; other operations retain their path without an invented line span.

## Actual local process verification

[The recorded result](../../graph-ui/verification/pr-2068/agent-setup-integration.json) identifies the exact hook SHA-256 and the explicitly marked test project `TEST-pr2068-hook-install-e87f2114`. A temporary repository with spaces and single quotes contained existing unrelated settings. The real installer process preserved them, and its second invocation reported no change. The generated shell command executed successfully against that temporary path.

An actual Python hook process received a synthetic, explicitly marked `Read` input after the verification program read its temporary fixture file. Its display name was deliberately overridden to **TEST local hook process; no agent session**. It delivered the requested lines 3 through 4 to `http://127.0.0.1:9749/api/agent-events`. A read-only SQLite query found event ID 8. Neither the fixture source contents nor the supplied test tool response appeared in the event.

The verifier requeued that same captured event in its isolated outbox and ran the real `--flush` process. Exactly one event remained in the daemon database, and the outbox became empty. The successful initial process took 0.074 seconds; this is one observed run, not a performance claim. An earlier attempt during concurrent browser work exceeded the producer's 0.75-second acknowledgment timeout after persistence; its outbox correctly retained the event. Immediate acknowledgment is therefore not assumed by the delivery design.

All temporary client configuration and outbox files were removed. The clearly labeled test event remains in the isolated development daemon's bounded journal. This proves the local producer-to-daemon path and retry behavior, not a completed real Claude Code session or support for other clients.

## Privacy and protocol review

The producer posts only to a loopback HTTP origin, disables environment proxies and redirects, and uses the existing daemon port. External hosts, credentials, URL paths and query parameters are rejected. Installation performs no network request. Normal execution records tool names, timestamps, paths, optional requested line spans, and at most 180 characters of command/search metadata. Such metadata may itself be sensitive; it remains local and is disclosed before installation. No issue submission, telemetry upload, account request, or LLM call is part of this setup.

## Narrow style-gate interpretation

The requested real client integration must use the actual client name and its configuration protocol. These operational references are not authorship attribution. [The gate helper](../../graph-ui/tools/lib/operational-client-reference.mjs) therefore permits only recognized client terms in four explicit setup/hook source and test files, plus four exact installation lines in the frontend README. It does not exempt whole files or generally allow product names. Authorship patterns are still checked unconditionally, including on otherwise permitted lines.

Two gate regression tests pass: valid protocol references are accepted only in those locations, while arbitrary names, other paths, vendor names, and `written by`/coauthor/generated-with claims remain rejected. The full style gate reports zero long-dash violations, zero attribution matches or patterns, and zero hardcoded chrome strings.

The earlier guide browser's original RPC evidence is preserved losslessly as [compressed JSON](../../graph-ui/verification/pr-2068/repair-guide-review.json.gz); [its summary](../../graph-ui/verification/pr-2068/repair-guide-review.json) retains screenshot, request counts and the original-byte SHA-256. Repository-authored source excerpts are evidence rather than application copy. The screenshot predates the subsequent guide ordering and layout fixes.
