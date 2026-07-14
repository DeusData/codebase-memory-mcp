#include "test_framework.h"
#include "foundation/profile.h"

int tf_pass_count = 0;
int tf_fail_count = 0;
int tf_skip_count = 0;
int tf_filter_count = 0;

extern void suite_arena(void);
extern void suite_hash_table(void);
extern void suite_dyn_array(void);
extern void suite_str_intern(void);
extern void suite_log(void);
extern void suite_str_util(void);
extern void suite_platform(void);
extern void suite_dump_verify(void);
extern void suite_subprocess(void);

int main(void) {
    cbm_profile_init();
    printf("\n  codebase-memory-mcp  C foundation test suite\n");
    RUN_SUITE(arena);
    RUN_SUITE(hash_table);
    RUN_SUITE(dyn_array);
    RUN_SUITE(str_intern);
    RUN_SUITE(log);
    RUN_SUITE(str_util);
    RUN_SUITE(platform);
    RUN_SUITE(dump_verify);
    RUN_SUITE(subprocess);
    TEST_SUMMARY();
}
