#include <string.h>

/* External names and a live volatile pointer keep the cold record retained. */
__attribute__((noinline, used)) int cbm_profile_probe_hit(void) {
    return 42;
}

__attribute__((noinline, used)) int cbm_profile_probe_never(void) {
    return 37;
}

__attribute__((used)) int (*volatile cbm_profile_probe_keep_never)(void) =
    cbm_profile_probe_never;

int main(int argc, char **argv) {
    if (argc != 2 || !cbm_profile_probe_keep_never)
        return 2;
    if (strcmp(argv[1], "--list") == 0)
        return 0;
    if (strcmp(argv[1], "--hit") == 0)
        return cbm_profile_probe_hit() == 42 ? 0 : 3;
    return 2;
}
