#ifndef CBM_TEST_RESOLVER_QUALITY_H
#define CBM_TEST_RESOLVER_QUALITY_H

/* Independent, bounded fixture oracle. Each listed (file, caller, relation)
 * closes that caller's relation domain: list all its genuine occurrences.
 * Other callers/files are outside coverage, except a row at an exact golden
 * site is always checked for a rebound caller or role. No resolver output or
 * capability flag creates expectations. RAW sites may have an explicit,
 * independently authored line-preserving preprocessing map; every other
 * covered expanded/embedded row still fails rather than borrowing raw spans.
 * Include this header in tests only; no production resolver behavior changes. */
#include "cbm.h"
#include "callable_sig.h"
#include "resolve/policy.h"
#include "store/store.h"
#include "foundation/sha256.h"
#include "sqlite3.h"
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct {
    const char *path, *text;
    /* Fixture-authored bytes/configuration, never populated from resolver output. */
    const char *preprocessed_text, *preprocessed_configuration_identity;
} RQ_Source;
typedef struct {
    const char *file, *caller, *needle;
    unsigned occurrence_index;
    /* Without needle, supply an exact nonempty raw byte interval. */
    uint32_t start_byte, end_byte;
    /* Optional alternate coordinates for this SAME raw occurrence. The bytes
     * and source line must agree; zero/zero means no preprocessing map. */
    uint32_t preprocessed_start_byte, preprocessed_end_byte;
} RQ_Site;
typedef enum {
    RQ_TARGET_PROJECT = 0,
    RQ_TARGET_EXTERNAL,
    RQ_TARGET_CONFIGURED_FOLDER
} RQ_TargetDomain;
typedef struct {
    const char *file, *suffix, *label;
    int declaration_line;
    const char *declaration_text;
    /* For project targets, NULL suffix means a known identity not materialized;
     * it never matches a graph target. The file/source anchor remains real. */
    RQ_TargetDomain domain;
    /* External builtins have an exact repository ontology identity, never a
     * fabricated project declaration or a suffix-based target match. */
    const char *external_identity;
    /* Only CONFIGURED_FOLDER uses a separate literal configuration anchor.
     * file/suffix still identify the exact structural Folder; its 0/0 range
     * never substitutes for a declaration range on an ordinary project node. */
    const char *configuration_file;
} RQ_Target;
typedef enum { RQ_REQUIRED, RQ_KNOWN_GAP } RQ_Mode;
typedef struct {
    const char *case_id;
    RQ_Site site;
    const char *relation;
    RQ_Target target;
    RQ_Mode mode;
    const char *gap_id, *why, *tried;
    /* Preserve gap history while promoting recovery to a required positive. */
    bool expected_recovered;
    /* Explicit source semantics may call several targets at one occurrence,
     * e.g. an Ansible notify topic. Never infer this group from observed rows. */
    const char *dispatch_group;
} RQ_Expected;
typedef struct {
    const char *case_id;
    RQ_Site site;
    const char *relation;
    RQ_Target target;
    bool any_target;
} RQ_Forbidden;
typedef struct {
    size_t genuine, found, missing, wrong, projection_errors;
    size_t required_missing, recovered_gaps, invalid_oracle, metadata_errors;
} RQ_Report;
typedef struct {
    const char *case_id, *relation;
    const RQ_Site *site;
    const RQ_Target *target;
    uint32_t start, end;
    int line;
    bool negative, any_target, found;
    const char *dispatch_group;
    uint32_t preprocessed_start, preprocessed_end;
    char preprocessed_content_identity[CBM_SHA256_HEX_LEN + 1];
    const char *preprocessed_configuration_identity;
} RQ_State;

enum { RQ_MAX_EXPECTATIONS = 256 };

static inline bool rq_text(const char *text) {
    return text && text[0];
}
static inline bool rq_equal(const char *left, const char *right) {
    return left && right && !strcmp(left, right);
}
static inline bool rq_suffix(const char *actual, const char *name, const char *wanted) {
    if (!rq_text(actual) || !rq_text(wanted))
        return false;
    size_t length = cbm_qn_callable_base_len_named(actual, name);
    size_t expected = cbm_qn_callable_base_len(wanted);
    return expected && length >= expected &&
           (!wanted[expected] || !strcmp(actual + length, wanted + expected)) &&
           (length == expected || actual[length - expected - 1] == '.') &&
           !memcmp(actual + length - expected, wanted, expected);
}
static inline const RQ_Source *rq_source_input(const RQ_Source *sources, size_t count,
                                               const char *path) {
    for (size_t i = 0; i < count; i++)
        if (rq_equal(sources[i].path, path))
            return sources + i;
    return NULL;
}
static inline const char *rq_source(const RQ_Source *sources, size_t count, const char *path) {
    const RQ_Source *source = rq_source_input(sources, count, path);
    return source ? source->text : NULL;
}
static inline bool rq_anchor(RQ_State *state, const RQ_Source *sources, size_t count) {
    const RQ_Site *site = state->site;
    const RQ_Source *input = rq_source_input(sources, count, site->file);
    const char *source = input ? input->text : NULL;
    if (!source || !rq_text(site->caller) || strlen(source) > UINT32_MAX ||
        site->occurrence_index > strlen(source))
        return false;
    if (rq_text(site->needle)) {
        const char *found = source;
        for (unsigned i = 0; i <= site->occurrence_index; i++) {
            found = strstr(found, site->needle);
            if (!found)
                return false;
            if (i != site->occurrence_index)
                found++;
        }
        state->start = (uint32_t)(found - source);
        state->end = state->start + (uint32_t)strlen(site->needle);
        if (site->end_byte && (site->start_byte != state->start || site->end_byte != state->end))
            return false;
    } else {
        state->start = site->start_byte;
        state->end = site->end_byte;
    }
    if (state->start >= state->end || state->end > strlen(source))
        return false;
    state->line = 1;
    for (uint32_t i = 0; i < state->start; i++)
        state->line += source[i] == '\n';
    if (site->preprocessed_start_byte || site->preprocessed_end_byte) {
        const char *expanded = input->preprocessed_text;
        uint32_t start = site->preprocessed_start_byte, end = site->preprocessed_end_byte;
        if (!expanded || !rq_text(input->preprocessed_configuration_identity) ||
            strlen(expanded) > UINT32_MAX || start >= end || end > strlen(expanded) ||
            end - start != state->end - state->start ||
            memcmp(expanded + start, source + state->start, end - start))
            return false;
        int line = 1;
        for (uint32_t i = 0; i < start; i++)
            line += expanded[i] == '\n';
        if (line != state->line)
            return false;
        state->preprocessed_start = start;
        state->preprocessed_end = end;
        state->preprocessed_configuration_identity = input->preprocessed_configuration_identity;
        cbm_sha256_hex(expanded, strlen(expanded), state->preprocessed_content_identity);
    }
    return true;
}
/* Bounded external ontology: internal/cbm/lsp/py_builtins.c materializes
 * these Python symbols at this synthetic path. The expectation supplies the
 * exact identity and label independently of any observed resolver output. */
static inline bool rq_external_target(const RQ_Target *target) {
    if (target->domain != RQ_TARGET_EXTERNAL || !rq_equal(target->file, "<python-builtins>") ||
        target->suffix || target->declaration_line || target->declaration_text ||
        target->configuration_file || !rq_text(target->external_identity))
        return false;
    static const struct {
        const char *identity, *label;
    } builtins[] = {{"builtins.len", "Function"},     {"builtins.print", "Function"},
                    {"builtins.str", "Class"},        {"builtins.int", "Class"},
                    {"builtins.list", "Class"},       {"builtins.dict", "Class"},
                    {"builtins.range", "Class"},      {"builtins.str.upper", "Method"},
                    {"builtins.str.lower", "Method"}, {"builtins.list.append", "Method"},
                    {"builtins.list.pop", "Method"},  {"builtins.dict.get", "Method"}};
    for (size_t i = 0; i < sizeof(builtins) / sizeof(builtins[0]); i++)
        if (rq_equal(target->external_identity, builtins[i].identity) &&
            rq_equal(target->label, builtins[i].label))
            return true;
    return false;
}
static inline const char *rq_target_identity(const RQ_Target *target) {
    return target->domain == RQ_TARGET_EXTERNAL ? target->external_identity : target->suffix;
}
static inline bool rq_configured_folder(const RQ_Target *target) {
    if (target->domain != RQ_TARGET_CONFIGURED_FOLDER || target->external_identity ||
        !rq_equal(target->label, "Folder") || !rq_text(target->file) || !rq_text(target->suffix) ||
        !rq_text(target->configuration_file) || target->file[0] == '/' ||
        strchr(target->file, '\\'))
        return false;
    const char *segment = target->file;
    for (const char *cursor = target->file;; cursor++) {
        if (*cursor != '/' && *cursor)
            continue;
        size_t length = (size_t)(cursor - segment);
        if (!length || (length == 1 && segment[0] == '.') ||
            (length == 2 && segment[0] == '.' && segment[1] == '.'))
            return false;
        if (!*cursor)
            break;
        segment = cursor + 1;
    }
    size_t length = strlen(target->file);
    if (strlen(target->suffix) != length)
        return false;
    for (size_t i = 0; i < length; i++)
        if (target->suffix[i] != (target->file[i] == '/' ? '.' : target->file[i]))
            return false;
    return true;
}

static inline bool rq_target_anchor(const RQ_Target *target, const RQ_Source *sources,
                                    size_t count) {
    if (target->domain == RQ_TARGET_EXTERNAL)
        return rq_external_target(target);
    bool folder = rq_configured_folder(target);
    if ((!folder && target->domain != RQ_TARGET_PROJECT) || target->external_identity ||
        (!folder && target->configuration_file))
        return false;
    const char *text =
        rq_source(sources, count, folder ? target->configuration_file : target->file);
    if (!text || target->declaration_line <= 0 || !rq_text(target->declaration_text))
        return false;
    for (int line = 1; line < target->declaration_line; line++) {
        text = strchr(text, '\n');
        if (!text)
            return false;
        text++;
    }
    size_t length = strcspn(text, "\n");
    return strlen(target->declaration_text) == length &&
           !memcmp(text, target->declaration_text, length);
}
static inline bool rq_target(const RQ_Target *target, const char *file, const char *qn,
                             const char *name, const char *label, int first, int last) {
    if (target->domain == RQ_TARGET_EXTERNAL)
        return rq_external_target(target) && rq_equal(target->file, file) &&
               rq_equal(target->external_identity, qn) && rq_equal(target->label, label);
    if (target->domain == RQ_TARGET_CONFIGURED_FOLDER)
        return rq_configured_folder(target) && rq_equal(file, target->file) &&
               rq_equal(label, "Folder") && rq_suffix(qn, name, target->suffix) && first == 0 &&
               last == 0;
    bool source_range = first <= target->declaration_line && last >= target->declaration_line;
    bool structural_file =
        rq_equal(target->label, "File") && rq_equal(label, "File") && first == 0 && last == 0;
    return target->domain == RQ_TARGET_PROJECT && !target->external_identity &&
           !target->configuration_file && rq_equal(target->file, file) &&
           rq_suffix(qn, name, target->suffix) &&
           (!target->label || rq_equal(target->label, label)) && (source_range || structural_file);
}
static inline bool rq_generation(sqlite3 *db, const char *project) {
    sqlite3_stmt *stmt = NULL;
    bool ok =
        sqlite3_prepare_v2(db, "SELECT rules_version FROM resolver_generations WHERE project=?1",
                           -1, &stmt, NULL) == SQLITE_OK &&
        sqlite3_bind_text(stmt, 1, project, -1, SQLITE_TRANSIENT) == SQLITE_OK &&
        sqlite3_step(stmt) == SQLITE_ROW &&
        sqlite3_column_int(stmt, 0) == CBM_RESOLVER_RULES_VERSION &&
        sqlite3_step(stmt) == SQLITE_DONE;
    sqlite3_finalize(stmt);
    return ok;
}
static inline bool rq_snapshot(sqlite3 *db, const char *project, const RQ_Source *source) {
    sqlite3_stmt *stmt = NULL;
    char digest[CBM_SHA256_HEX_LEN + 1];
    cbm_sha256_hex(source->text, strlen(source->text), digest);
    bool ok = sqlite3_prepare_v2(db,
                                 "SELECT raw_content_identity,raw_length,"
                                 "preprocessed_content_identity,preprocessed_length,"
                                 "configuration_identity FROM "
                                 "resolver_file_summaries WHERE project=?1 AND file_path=?2",
                                 -1, &stmt, NULL) == SQLITE_OK &&
              sqlite3_bind_text(stmt, 1, project, -1, SQLITE_TRANSIENT) == SQLITE_OK &&
              sqlite3_bind_text(stmt, 2, source->path, -1, SQLITE_TRANSIENT) == SQLITE_OK &&
              sqlite3_step(stmt) == SQLITE_ROW &&
              rq_equal((const char *)sqlite3_column_text(stmt, 0), digest) &&
              sqlite3_column_int64(stmt, 1) == (sqlite3_int64)strlen(source->text);
    if (ok && source->preprocessed_text) {
        cbm_sha256_hex(source->preprocessed_text, strlen(source->preprocessed_text), digest);
        ok = rq_equal((const char *)sqlite3_column_text(stmt, 2), digest) &&
             sqlite3_column_int64(stmt, 3) == (sqlite3_int64)strlen(source->preprocessed_text) &&
             rq_equal((const char *)sqlite3_column_text(stmt, 4),
                      source->preprocessed_configuration_identity);
    }
    ok = ok && sqlite3_step(stmt) == SQLITE_DONE;
    sqlite3_finalize(stmt);
    return ok;
}
static inline bool rq_prepare(RQ_State *states, const RQ_Expected *expected, size_t count,
                              const RQ_Forbidden *forbidden, size_t negative_count,
                              const RQ_Source *sources, size_t source_count, RQ_Report *report,
                              FILE *log) {
    for (size_t i = 0; i < count + negative_count; i++) {
        RQ_State *state = states + i;
        if (i < count) {
            const RQ_Expected *item = expected + i;
            *state = (RQ_State){.case_id = item->case_id,
                                .relation = item->relation,
                                .site = &item->site,
                                .target = &item->target,
                                .dispatch_group = item->dispatch_group};
            bool mode = item->mode == RQ_REQUIRED || item->mode == RQ_KNOWN_GAP;
            bool gap = item->mode != RQ_KNOWN_GAP ||
                       (rq_text(item->gap_id) && rq_text(item->why) && rq_text(item->tried));
            bool required = item->mode == RQ_REQUIRED || item->expected_recovered;
            if (!mode || !gap || (required && !rq_text(rq_target_identity(&item->target))) ||
                (item->dispatch_group &&
                 (!rq_text(item->dispatch_group) ||
                  (!rq_equal(item->relation, "CALLS") && !rq_equal(item->relation, "USAGE")))))
                report->invalid_oracle++;
        } else {
            const RQ_Forbidden *item = forbidden + i - count;
            *state = (RQ_State){.case_id = item->case_id,
                                .relation = item->relation,
                                .site = &item->site,
                                .target = &item->target,
                                .negative = true,
                                .any_target = item->any_target};
        }
        bool valid = rq_text(state->case_id) && rq_text(state->relation) &&
                     rq_anchor(state, sources, source_count) &&
                     (state->any_target || rq_target_anchor(state->target, sources, source_count));
        for (size_t j = 0; j < i; j++) {
            const RQ_State *prior = states + j;
            bool same_site = !state->negative && !prior->negative &&
                             rq_equal(prior->site->file, state->site->file) &&
                             rq_equal(prior->relation, state->relation) &&
                             prior->start == state->start && prior->end == state->end;
            bool distinct_target = prior->target->domain != state->target->domain ||
                                   !rq_equal(prior->target->file, state->target->file);
            if (!distinct_target && prior->target->domain == RQ_TARGET_EXTERNAL)
                distinct_target =
                    !rq_equal(prior->target->external_identity, state->target->external_identity);
            if (!distinct_target && prior->target->domain == RQ_TARGET_PROJECT &&
                rq_text(prior->target->suffix) && rq_text(state->target->suffix))
                distinct_target = !rq_suffix(prior->target->suffix, NULL, state->target->suffix) &&
                                  !rq_suffix(state->target->suffix, NULL, prior->target->suffix);
            bool multicast = rq_text(state->dispatch_group) &&
                             rq_equal(prior->dispatch_group, state->dispatch_group) &&
                             rq_equal(prior->site->caller, state->site->caller) && distinct_target;
            if (rq_equal(prior->case_id, state->case_id) || (same_site && !multicast))
                valid = false;
            /* One transformed site must not recover different raw occurrences.
             * Multicast may share it only when the raw occurrence also agrees. */
            bool same_expanded = state->preprocessed_end && prior->preprocessed_end &&
                                 rq_equal(prior->site->file, state->site->file) &&
                                 rq_equal(prior->relation, state->relation) &&
                                 prior->preprocessed_start == state->preprocessed_start &&
                                 prior->preprocessed_end == state->preprocessed_end;
            if (same_expanded && (!same_site || !multicast) && !state->negative && !prior->negative)
                valid = false;
        }
        if (!valid) {
            report->invalid_oracle++;
            if (log)
                fprintf(log, "[RQ] INVALID_ORACLE case=%s\n",
                        state->case_id ? state->case_id : "?");
        }
    }
    for (size_t i = 0; i < count; i++) {
        if (!states[i].dispatch_group)
            continue;
        bool companion = false;
        for (size_t j = 0; j < count; j++)
            companion |= i != j && rq_equal(states[i].dispatch_group, states[j].dispatch_group) &&
                         rq_equal(states[i].site->file, states[j].site->file) &&
                         rq_equal(states[i].site->caller, states[j].site->caller) &&
                         rq_equal(states[i].relation, states[j].relation) &&
                         states[i].start == states[j].start && states[i].end == states[j].end;
        if (!companion)
            report->invalid_oracle++;
    }
    return report->invalid_oracle == 0;
}

/* Column layout shared by the row and graph checks: file/scope/relation,
 * span/origin, target identity and source declaration, then graph counterpart. */
static inline bool rq_occurrence(sqlite3_stmt *stmt, RQ_State *states, size_t count,
                                 RQ_Report *report, FILE *log) {
    const char *file = (const char *)sqlite3_column_text(stmt, 0);
    const char *scope = (const char *)sqlite3_column_text(stmt, 1);
    const char *relation = (const char *)sqlite3_column_text(stmt, 2);
    sqlite3_int64 start = sqlite3_column_int64(stmt, 3), end = sqlite3_column_int64(stmt, 4);
    int origin = sqlite3_column_int(stmt, 5);
    const char *target = (const char *)sqlite3_column_text(stmt, 6);
    const char *target_file = (const char *)sqlite3_column_text(stmt, 7);
    const char *source_file = (const char *)sqlite3_column_text(stmt, 12);
    const char *source_name = (const char *)sqlite3_column_text(stmt, 15);
    bool covered = false, matched = false, forbidden = false, unprojected = false;
    bool mapped_origin = false;
    size_t positive_matches = 0;
    for (size_t i = 0; i < count; i++) {
        RQ_State *state = states + i;
        bool caller = rq_suffix(scope, source_name, state->site->caller);
        bool same_file = rq_equal(file, state->site->file);
        bool raw_site = same_file && start == state->start && end == state->end;
        bool expanded_site = same_file && state->preprocessed_end &&
                             start == state->preprocessed_start && end == state->preprocessed_end;
        bool expanded_identity = expanded_site && origin == CBM_SOURCE_ORIGIN_PREPROCESSED &&
                                 rq_equal((const char *)sqlite3_column_text(stmt, 19),
                                          state->preprocessed_content_identity) &&
                                 rq_equal((const char *)sqlite3_column_text(stmt, 20),
                                          state->preprocessed_configuration_identity);
        bool same_site = origin == CBM_SOURCE_ORIGIN_RAW ? raw_site : expanded_identity;
        mapped_origin |= expanded_identity;
        covered |= raw_site || expanded_site ||
                   ((same_file || rq_equal(source_file, state->site->file)) && caller &&
                    rq_equal(relation, state->relation));
        bool source_range = sqlite3_column_int(stmt, 13) <= state->line &&
                            sqlite3_column_int(stmt, 14) >= state->line;
        bool structural_file = rq_equal((const char *)sqlite3_column_text(stmt, 18), "File") &&
                               sqlite3_column_int(stmt, 13) == 0 &&
                               sqlite3_column_int(stmt, 14) == 0;
        if (!same_site || !caller || !rq_equal(source_file, state->site->file) ||
            !rq_equal(relation, state->relation) || (!source_range && !structural_file))
            continue;
        bool target_ok = state->any_target ||
                         rq_target(state->target, target_file, target,
                                   (const char *)sqlite3_column_text(stmt, 10),
                                   (const char *)sqlite3_column_text(stmt, 11),
                                   sqlite3_column_int(stmt, 8), sqlite3_column_int(stmt, 9));
        if (!state->any_target && state->target->domain == RQ_TARGET_EXTERNAL &&
            (sqlite3_column_type(stmt, 17) == SQLITE_NULL ||
             sqlite3_column_int(stmt, 17) != CBM_RESOLVE_DOMAIN_EXTERNAL))
            target_ok = false;
        if (state->negative && target_ok)
            forbidden = true;
        if (!state->negative && target_ok) {
            positive_matches++;
            if (sqlite3_column_int(stmt, 16)) {
                state->found = true;
                matched = true;
            } else {
                unprojected = true;
            }
        }
    }
    /* One emitted target cannot recover two supposedly distinct targets,
     * even if suffix aliases or overlapping declaration anchors escaped the
     * static expectation checks. */
    if (positive_matches > 1) {
        report->invalid_oracle++;
        return false;
    }
    if (covered && origin != CBM_SOURCE_ORIGIN_RAW && !mapped_origin) {
        report->metadata_errors++;
        if (log)
            fprintf(log, "[RQ] UNMAPPED_SOURCE_ORIGIN file=%s origin=%d\n", file, origin);
        return false;
    }
    if (covered && (!matched || forbidden)) {
        if (unprojected && !forbidden)
            report->projection_errors++;
        else
            report->wrong++;
        if (log)
            fprintf(log,
                    "[RQ] WRONG_OCCURRENCE file=%s scope=%s span=%lld-%lld role=%s target=%s\n",
                    file, scope, (long long)start, (long long)end, relation, target);
        return false;
    }
    return true;
}
static inline bool rq_projection(sqlite3_stmt *stmt, const RQ_State *states, size_t count,
                                 RQ_Report *report, FILE *log) {
    const char *file = (const char *)sqlite3_column_text(stmt, 0);
    const char *scope = (const char *)sqlite3_column_text(stmt, 1);
    const char *relation = (const char *)sqlite3_column_text(stmt, 2);
    const char *target = (const char *)sqlite3_column_text(stmt, 3);
    const char *target_file = (const char *)sqlite3_column_text(stmt, 4);
    const char *source_name = (const char *)sqlite3_column_text(stmt, 9);
    /* This oracle covers the seven resolver relations. Inheritance and
     * override facts between the same nodes have a separate contract. */
    static const char *const relations[] = {"IMPORTS", "CALLS",  "CALL_REFERENCE", "USAGE",
                                            "READS",   "WRITES", "USES_TYPE"};
    bool resolver_relation = false;
    for (size_t i = 0; i < sizeof(relations) / sizeof(relations[0]); i++)
        resolver_relation |= rq_equal(relation, relations[i]);
    if (!resolver_relation)
        return true;
    bool covered = false, matched = false;
    for (size_t i = 0; i < count; i++) {
        const RQ_State *state = states + i;
        if (!rq_equal(file, state->site->file) ||
            !rq_suffix(scope, source_name, state->site->caller))
            continue;
        bool target_ok = state->any_target ||
                         rq_target(state->target, target_file, target,
                                   (const char *)sqlite3_column_text(stmt, 7),
                                   (const char *)sqlite3_column_text(stmt, 8),
                                   sqlite3_column_int(stmt, 5), sqlite3_column_int(stmt, 6));
        /* A known target under a different role is also inside the audit. */
        covered |= rq_equal(relation, state->relation) || (!state->any_target && target_ok);
        if (!rq_equal(relation, state->relation) || !target_ok)
            continue;
        if (!state->negative && state->found && sqlite3_column_int(stmt, 10))
            matched = true;
    }
    /* Graph edges aggregate sites. A forbidden site is checked in rows; an
     * independent valid site may legitimately project the same caller/target. */
    if (covered && !matched) {
        report->projection_errors++;
        if (log)
            fprintf(log, "[RQ] UNATTESTED_GRAPH_EDGE file=%s scope=%s role=%s target=%s\n", file,
                    scope, relation, target);
        return false;
    }
    return true;
}
static inline bool rq_query(sqlite3 *db, const char *project, const char *sql, bool projection,
                            RQ_State *states, size_t count, RQ_Report *report, FILE *log) {
    sqlite3_stmt *stmt = NULL;
    bool ok = sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) == SQLITE_OK &&
              sqlite3_bind_text(stmt, 1, project, -1, SQLITE_TRANSIENT) == SQLITE_OK;
    int step = SQLITE_ERROR;
    if (ok)
        while ((step = sqlite3_step(stmt)) == SQLITE_ROW) {
            bool valid = projection ? rq_projection(stmt, states, count, report, log)
                                    : rq_occurrence(stmt, states, count, report, log);
            ok = valid && ok;
        }
    if (step != SQLITE_DONE) {
        report->metadata_errors++;
        ok = false;
    }
    sqlite3_finalize(stmt);
    return ok;
}
static inline bool rq_finish(const char *suite, const RQ_Expected *expected, size_t count,
                             const RQ_State *states, RQ_Report *report, FILE *log) {
    for (size_t i = 0; i < count; i++) {
        const RQ_Expected *item = expected + i;
        bool required = item->mode == RQ_REQUIRED || item->expected_recovered;
        report->found += states[i].found;
        if (!states[i].found) {
            report->missing++;
            report->required_missing += required;
            if (log)
                fprintf(log,
                        "[RQ] QUALITY_MISS case=%s gap=%s file=%s role=%s target=%s:%s "
                        "WHY=%s TRIED=%s\n",
                        item->case_id, item->gap_id ? item->gap_id : "required", item->site.file,
                        item->relation, item->target.file,
                        rq_target_identity(&item->target) ? rq_target_identity(&item->target)
                                                          : "<identity-gap>",
                        item->why ? item->why : "required supported relationship",
                        item->tried ? item->tried : "independent source golden");
        } else if (!required) {
            report->recovered_gaps++;
            if (log)
                fprintf(log, "[RQ] PROMOTE_TO_REQUIRED case=%s gap=%s\n", item->case_id,
                        item->gap_id);
        }
    }
    if (log)
        fprintf(log,
                "[RQ] %s genuine=%zu found=%zu missing=%zu wrong=%zu projection_errors=%zu "
                "required_missing=%zu recovered_gaps=%zu metadata_errors=%zu invalid_oracle=%zu\n",
                suite, report->genuine, report->found, report->missing, report->wrong,
                report->projection_errors, report->required_missing, report->recovered_gaps,
                report->metadata_errors, report->invalid_oracle);
    return !report->wrong && !report->projection_errors && !report->required_missing &&
           !report->recovered_gaps && !report->metadata_errors && !report->invalid_oracle;
}

/* Run before closing the fixture store. NULL log suppresses diagnostics only;
 * it never changes accounting or gates. A non-NULL report is always populated. */
static inline bool rq_check(cbm_store_t *store, const char *project, const char *suite,
                            const RQ_Source *sources, size_t source_count,
                            const RQ_Expected *expected, size_t count,
                            const RQ_Forbidden *forbidden, size_t negative_count, RQ_Report *report,
                            FILE *log) {
    if (!report)
        return false;
    *report = (RQ_Report){.genuine = count};
    if (!store || !rq_text(project) || !rq_text(suite) || !sources || !source_count ||
        (!expected && count) || (!forbidden && negative_count) || count > RQ_MAX_EXPECTATIONS ||
        negative_count > RQ_MAX_EXPECTATIONS - count || count + negative_count == 0) {
        report->invalid_oracle++;
        return false;
    }
    RQ_State *states = calloc(count + negative_count, sizeof(*states));
    if (!states) {
        report->metadata_errors++;
        return false;
    }
    bool valid = rq_prepare(states, expected, count, forbidden, negative_count, sources,
                            source_count, report, log);
    sqlite3 *db = cbm_store_get_db(store);
    if (!db || !rq_generation(db, project))
        report->metadata_errors++;
    for (size_t i = 0; i < source_count; i++) {
        bool expanded = sources[i].preprocessed_text != NULL;
        if (!rq_text(sources[i].path) || !sources[i].text ||
            expanded != (sources[i].preprocessed_configuration_identity != NULL) ||
            (expanded && (!rq_text(sources[i].preprocessed_text) ||
                          !rq_text(sources[i].preprocessed_configuration_identity)))) {
            report->invalid_oracle++;
            continue;
        }
        if (!db || !rq_snapshot(db, project, sources + i)) {
            report->metadata_errors++;
            if (log)
                fprintf(log, "[RQ] SOURCE_SNAPSHOT_MISMATCH file=%s\n", sources[i].path);
        }
    }
    const char *rows =
        "SELECT r.file_path,r.scope,r.relation,r.start_byte,r.end_byte,r.source_origin,r.target,"
        "t.file_path,t.start_line,t.end_line,t.name,t.label,s.file_path,s.start_line,s.end_line,"
        "s.name,EXISTS(SELECT 1 FROM edges e WHERE e.project=r.project AND e.source_id=s.id "
        "AND e.target_id=t.id AND e.type=r.relation),r.domain,s.label,"
        "v.content_identity,v.configuration_identity FROM resolver_occurrences r "
        "LEFT JOIN resolver_occurrence_evidence v ON v.project=r.project AND v.ordinal=r.ordinal "
        "LEFT JOIN nodes s ON s.project=r.project AND s.qualified_name=r.scope "
        "LEFT JOIN nodes t ON t.project=r.project AND t.qualified_name=r.target "
        "WHERE r.project=?1 AND r.emitted=1";
    const char *edges =
        "SELECT s.file_path,s.qualified_name,e.type,t.qualified_name,t.file_path,t.start_line,"
        "t.end_line,t.name,t.label,s.name,EXISTS(SELECT 1 FROM resolver_occurrences r "
        "WHERE r.project=e.project AND r.emitted=1 AND r.scope=s.qualified_name "
        "AND r.target=t.qualified_name AND r.relation=e.type) FROM edges e "
        "JOIN nodes s ON s.id=e.source_id JOIN nodes t ON t.id=e.target_id WHERE e.project=?1";
    if (valid && !report->metadata_errors) {
        (void)rq_query(db, project, rows, false, states, count + negative_count, report, log);
        (void)rq_query(db, project, edges, true, states, count + negative_count, report, log);
    }
    bool ok = valid && rq_finish(suite, expected, count, states, report, log);
    free(states);
    return ok;
}
#endif
