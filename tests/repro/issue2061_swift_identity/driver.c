#define main graph_main
#define passed graph_passed
#define failed graph_failed
#define errors graph_errors
#include "main.c"
#undef main
#undef passed
#undef failed
#undef errors
#define main mcp_main
#include "mcp_driver.c"
#undef main

int main(int argc, char **argv) {
    if (argc != 3)
        return 2;
    if (strcmp(argv[1], "graph") == 0)
        return graph_main(argc - 1, argv + 1);
    if (strcmp(argv[1], "mcp") == 0)
        return mcp_main(argc - 1, argv + 1);
    return 2;
}
