/*
 * agent_profiles.h — Canonical tiered codebase-memory agent profiles.
 */
#ifndef CBM_CLI_AGENT_PROFILES_H
#define CBM_CLI_AGENT_PROFILES_H

#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef enum {
    CBM_GRAPH_TIER_SCOUT = 0,
    CBM_GRAPH_TIER_VERIFY,
    CBM_GRAPH_TIER_AUDIT,
    CBM_GRAPH_TIER_COUNT
} cbm_graph_tier_t;

typedef enum {
    CBM_GRAPH_ACCESS_DIRECT = 0,
    CBM_GRAPH_ACCESS_HANDOFF,
    CBM_GRAPH_ACCESS_COUNT
} cbm_graph_access_t;

typedef enum {
    CBM_GRAPH_DIALECT_CLAUDE = 0,
    CBM_GRAPH_DIALECT_CODEX,
    CBM_GRAPH_DIALECT_GEMINI,
    CBM_GRAPH_DIALECT_QWEN,
    CBM_GRAPH_DIALECT_COPILOT,
    CBM_GRAPH_DIALECT_OPENCODE,
    CBM_GRAPH_DIALECT_KILO,
    CBM_GRAPH_DIALECT_KIRO,
    CBM_GRAPH_DIALECT_JUNIE,
    CBM_GRAPH_DIALECT_QODER,
    CBM_GRAPH_DIALECT_CODEBUDDY,
    CBM_GRAPH_DIALECT_FACTORY,
    CBM_GRAPH_DIALECT_VIBE,
    CBM_GRAPH_DIALECT_AUGMENT,
    CBM_GRAPH_DIALECT_CURSOR,
    CBM_GRAPH_DIALECT_ROVO,
    CBM_GRAPH_DIALECT_POCHI,
    CBM_GRAPH_DIALECT_COUNT
} cbm_graph_profile_dialect_t;

/* Stable profile identifier. VERIFY intentionally retains "codebase-memory". */
const char *cbm_graph_tier_slug(cbm_graph_tier_t tier);
const char *cbm_graph_tier_display_name(cbm_graph_tier_t tier);
bool cbm_graph_dialect_direct_capable(cbm_graph_profile_dialect_t dialect);

/* Returns malloc-owned profile content, or NULL for invalid/unsafe combinations.
 * binary_path is required for a direct Kiro profile and ignored otherwise. */
char *cbm_render_graph_profile(cbm_graph_profile_dialect_t dialect, cbm_graph_tier_t tier,
                               cbm_graph_access_t access, const char *binary_path);

/* Vibe stores the behavioral prompt separately from its TOML agent definition.
 * Other integrations may also use this as the canonical contract text. */
char *cbm_render_graph_prompt(cbm_graph_tier_t tier, cbm_graph_access_t access);

#ifdef __cplusplus
}
#endif

#endif /* CBM_CLI_AGENT_PROFILES_H */
