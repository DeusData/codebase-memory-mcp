/*
 * repro_issue403.c -- Reproduce-first boundary case for issue #403.
 *
 * Automatic session discovery must not treat the default per-user Windows
 * application install tree as a workspace. This is a root-selection policy,
 * not a vendor-name exclusion: ordinary projects elsewhere remain eligible.
 */

#include "test_framework.h"
#include "foundation/workspace.h"

TEST(repro_issue403_windows_user_programs_root_is_sensitive) {
    static const char *const roots[] = {
        "C:/Users/dev/AppData/Local/Programs",
        "C:/Users/dev/AppData/Local/Programs/Antigravity",
        "D:/Users/runneradmin/AppData/Local/Programs/Editor",
        "c:\\uSeRs\\Dev\\aPpDaTa\\LoCaL\\pRoGrAmS\\IDE",
    };

    for (size_t i = 0; i < sizeof(roots) / sizeof(roots[0]); i++) {
        ASSERT_EQ(cbm_workspace_classify_root(roots[i], NULL, NULL), CBM_WS_DENY_SENSITIVE);
    }
    ASSERT_TRUE(cbm_workspace_verdict_is_overridable(CBM_WS_DENY_SENSITIVE));
    PASS();
}

TEST(repro_issue403_nearby_projects_are_not_false_positives) {
    static const char *const roots[] = {
        "C:/Users/dev/projects/app",
        "C:/Users/dev/AppData/Local",
        "C:/Users/dev/AppData/Local/Programs-src",
        "C:/Users/dev/AppData/Local/ProgramsBackup",
        "C:/workspace/Users/dev/AppData/Local/Programs",
        "C:/Users/dev/team/AppData/Local/Programs",
    };

    for (size_t i = 0; i < sizeof(roots) / sizeof(roots[0]); i++) {
        ASSERT_EQ(cbm_workspace_classify_root(roots[i], NULL, NULL), CBM_WS_ALLOW);
    }
    PASS();
}

SUITE(repro_issue403) {
    RUN_TEST(repro_issue403_windows_user_programs_root_is_sensitive);
    RUN_TEST(repro_issue403_nearby_projects_are_not_false_positives);
}
