/*
 * servicelink_http.c — Cross-project HTTP endpoint registration.
 *
 * Unlike the other servicelinkers, HTTP detection is already performed
 * by pass_calls.c / pass_parallel.c using cbm_service_patterns. This
 * linker is a registrar + enrichment pass: it walks existing HTTP_CALLS
 * edges and Route nodes, enriches weak endpoints (env-var regex, k8s
 * Service host match), and registers them in protocol_endpoints for
 * cross-repo matching.
 *
 * This is the Phase-1 stub. The full implementation is in builder-linker.
 */
#include "servicelink.h"

int cbm_servicelink_http(cbm_pipeline_ctx_t *ctx) {
    (void)ctx;
    return 0;
}
