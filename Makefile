.DEFAULT_GOAL := help

PYTHON ?= $(shell if command -v python3 >/dev/null 2>&1; then printf python3; elif command -v python >/dev/null 2>&1; then printf python; elif command -v py >/dev/null 2>&1; then printf 'py -3'; fi)
NPM ?= npm
CBM_MAKE := $(MAKE) -f Makefile.cbm

.PHONY: help build build-ui test test-focused lint security frontend frontend-build \
	frontend-test organization organization-tests version-check test-registration repository-index clean

help:
	@echo "Repository commands:"
	@echo "  make build              Build the production C binary"
	@echo "  make build-ui           Build the binary with the embedded graph UI"
	@echo "  make test               Run the complete required C test suite"
	@echo "  make test-focused       Run C suites named in TEST_SUITES"
	@echo "  make lint               Run organization and C static checks"
	@echo "  make security           Build and run the security audit"
	@echo "  make frontend           Install, build, test, and cover the graph UI"
	@echo "  make organization       Check repository layout and metadata"
	@echo "  make organization-tests Run repository guard mutation tests"
	@echo "  make version-check      Check VERSION and release checksum parity"
	@echo "  make test-registration  Check C test discovery and runner parity"
	@echo "  make repository-index   Regenerate docs/REPOSITORY_INDEX.md"
	@echo "  make clean              Remove generated C and frontend artifacts"

build:
	$(CBM_MAKE) cbm

build-ui:
	$(CBM_MAKE) cbm-with-ui

test:
	$(CBM_MAKE) test

test-focused:
	$(CBM_MAKE) test-focused TEST_SUITES="$(TEST_SUITES)"

lint: organization
	$(CBM_MAKE) lint

security:
	$(CBM_MAKE) security

frontend: frontend-build frontend-test

frontend-build:
	cd graph-ui && $(NPM) ci && $(NPM) run build

frontend-test:
	cd graph-ui && $(NPM) run test:coverage

organization: organization-tests
	@test -n "$(PYTHON)" || { echo "Python 3 is required; install it or set PYTHON" >&2; exit 1; }
	$(PYTHON) scripts/check-repo-organization.py

organization-tests:
	@test -n "$(PYTHON)" || { echo "Python 3 is required; install it or set PYTHON" >&2; exit 1; }
	$(PYTHON) -m unittest discover -s scripts/tests -p 'test_*.py'

version-check:
	@test -n "$(PYTHON)" || { echo "Python 3 is required; install it or set PYTHON" >&2; exit 1; }
	$(PYTHON) scripts/sync-version.py

test-registration:
	@test -n "$(PYTHON)" || { echo "Python 3 is required; install it or set PYTHON" >&2; exit 1; }
	$(PYTHON) scripts/check-test-registration.py

repository-index:
	@test -n "$(PYTHON)" || { echo "Python 3 is required; install it or set PYTHON" >&2; exit 1; }
	$(PYTHON) scripts/generate-repository-index.py

clean:
	$(CBM_MAKE) clean-c
	rm -rf graph-ui/dist graph-ui/coverage graph-ui/tsconfig.tsbuildinfo
