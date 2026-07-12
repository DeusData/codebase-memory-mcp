#ifndef CBM_PRODUCT_H
#define CBM_PRODUCT_H

/*
 * Product distribution endpoints.
 *
 * Keep fork-specific repository metadata in one place so upstream merges do
 * not require hunting through the CLI, MCP server, and UI implementations.
 * A custom build can still override the repository with:
 *
 *   -DCBM_GITHUB_REPOSITORY=\"owner/repository\"
 */
#ifndef CBM_GITHUB_REPOSITORY
#define CBM_GITHUB_REPOSITORY "bogyie/codebase-memory-mcp"
#endif

#define CBM_GITHUB_URL "https://github.com/" CBM_GITHUB_REPOSITORY
#define CBM_GITHUB_RELEASES_URL CBM_GITHUB_URL "/releases"
#define CBM_GITHUB_LATEST_RELEASE_URL CBM_GITHUB_RELEASES_URL "/latest"
#define CBM_GITHUB_LATEST_DOWNLOAD_URL CBM_GITHUB_LATEST_RELEASE_URL "/download"
#define CBM_GITHUB_API_LATEST_RELEASE_URL                                                   \
    "https://api.github.com/repos/" CBM_GITHUB_REPOSITORY "/releases/latest"
#define CBM_GITHUB_ISSUES_NEW_URL CBM_GITHUB_URL "/issues/new"

#endif /* CBM_PRODUCT_H */
