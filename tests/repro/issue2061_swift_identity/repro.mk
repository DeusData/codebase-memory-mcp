# Explicit opt-in reproduction; not part of CI or ordinary test suites.
include Makefile.cbm
SWIFT_REPRO = tests/repro/issue2061_swift_identity
.PHONY: issue2061-repro
issue2061-repro: $(BUILD_DIR)/issue2061-driver

$(BUILD_DIR)/issue2061-driver: $(SWIFT_REPRO)/driver.c $(SWIFT_REPRO)/main.c $(SWIFT_REPRO)/mcp_driver.c $(PROD_SRCS) $(EXTRACTION_SRCS) $(AC_LZ4_SRCS) $(ZSTD_SRCS) $(SQLITE_WRITER_SRC) $(OBJS_VENDORED_TEST) $(PROJECT_HDRS) | $(BUILD_DIR)
	$(CC) $(CFLAGS_TEST) -Itests -o $@ $(SWIFT_REPRO)/driver.c $(PROD_SRCS) $(EXTRACTION_SRCS) $(AC_LZ4_SRCS) $(ZSTD_SRCS) $(SQLITE_WRITER_SRC) $(OBJS_VENDORED_TEST) $(LDFLAGS_TEST)
