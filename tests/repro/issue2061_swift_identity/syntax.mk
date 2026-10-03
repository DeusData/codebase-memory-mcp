# Invoke from an exact archived source root; no linking or test execution.
include Makefile.cbm
.PHONY: issue2061-syntax
issue2061-syntax:
	$(CC) $(CFLAGS_TEST) -fsyntax-only $(ALL_TEST_SRCS)
