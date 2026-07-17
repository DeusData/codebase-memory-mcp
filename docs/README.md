# Documentation Catalog

Use this page as the front desk for project documentation. The live source,
tests, and package metadata remain authoritative for behavior and release facts.

## Start here

| Need | Document |
| --- | --- |
| Understand runtime modules and data flow | [Architecture](ARCHITECTURE.md) |
| Find where a file belongs | [Repository index](REPOSITORY_INDEX.md) |
| Review enforced shelves, purposes, and owners | [`repository-layout.json`](repository-layout.json) |
| Follow the active organization work | [Organization plan](ORGANIZATION_PLAN.md) |
| Configure indexing and MCP behavior | [Configuration](CONFIGURATION.md) |
| Configure ignore rules | [`.cbmignore` guide](cbmignore.md) |
| Review measured performance | [Benchmark](BENCHMARK.md) |
| Review the evaluation methodology | [Evaluation plan](EVALUATION_PLAN.md) |
| Report a security concern | [Security disclosure](SECURITY-DISCLOSURE.md) |

## Repository-level guides

- [README](../README.md) covers installation, product use, tools, and supported
  client surfaces.
- [Contributing](../CONTRIBUTING.md) covers local setup, testing, DCO, and pull
  request expectations.
- [Maintainers](../MAINTAINERS.md) covers review routing and operational authority.
- [Third-party policy](../THIRD_PARTY.md) covers bundled dependencies and notices.

## Generated documentation

`REPOSITORY_INDEX.md` is generated from Git's staged index. Update it with
`make repository-index` after staging intended additions and verify it with
`make organization`. The generator reads Git's staged index, so local scratch
files cannot change the checked-in catalog. Other documents
are maintained by hand and should avoid duplicating facts that can be checked
from source.

The website files `index.html`, `robots.txt`, `sitemap.xml`, and `.nojekyll`
belong to the GitHub Pages surface. `graph-ui-screenshot.png` is its referenced
product image.
