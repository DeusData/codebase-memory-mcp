#pragma once
#include "arena.h"

/*
 * IRIS Studio Export XML transcoder.
 *
 * Converts <Export generator="Cache"> XML files to equivalent UDL text so
 * they can be fed to the existing ObjectScript UDL extraction pipeline.
 * The XML-to-UDL mapping is 1:1; no new extraction logic is needed.
 *
 * One Export file may contain multiple <Class> blocks. Each produces a
 * separate UDL string. The caller iterates the returned array and calls
 * cbm_extract_file(..., CBM_LANG_OBJECTSCRIPT_UDL, ...) for each entry.
 *
 * Returns arena-allocated array of NUL-terminated UDL strings, or NULL
 * if the file is not an Export file or parsing fails gracefully.
 * *class_count is set to the number of classes found (0 on failure).
 */
char **cbm_iris_export_to_udl(CBMArena *arena, const char *xml, int xml_len, int *class_count);
