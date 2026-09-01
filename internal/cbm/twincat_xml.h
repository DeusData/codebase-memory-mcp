#pragma once
#include "arena.h"

/*
 * TwinCAT 3 PLC XML container transcoder.
 *
 * .TcPOU / .TcDUT / .TcGVL / .TcIO files are <TcPlcObject> XML wrappers
 * carrying IEC 61131-3 Structured Text in <Declaration> and
 * <Implementation><ST> CDATA sections. This transcoder recomposes them into
 * the equivalent textual ST form so they can be fed to the existing
 * CBM_LANG_IEC_ST extraction pipeline; no new extraction logic is needed.
 *
 * Mapping:
 *   <POU>      Declaration + child <Method>/<Property>/<Action> blocks +
 *              Implementation body + synthesized END_* terminator (the
 *              terminator keyword is sniffed from the declaration header).
 *              <Action> has no textual equivalent in the grammar and is
 *              synthesized as a parameterless METHOD (call sites use the
 *              same inst.Name() shape either way).
 *   <DUT>      Declaration, with a `;` appended after END_STRUCT when the
 *              TwinCAT editor omitted it (the grammar is standard-strict).
 *   <GVL>      Declaration verbatim (VAR_GLOBAL blocks).
 *   <Itf>      Declaration + child <Method> prototypes + END_INTERFACE.
 *
 * Newline padding aligns each CDATA payload with its physical line in the
 * XML file, so definition line numbers in the graph point into the real
 * .TcPOU file (unlike the Studio Export transcoder, which accepts skew).
 *
 * Input handling: a UTF-8 BOM is skipped; UTF-16 input (FF FE / FE FF) is
 * rejected. Returns an arena-allocated array of NUL-terminated ST strings,
 * or NULL if the input is not a TcPlcObject file or parsing fails
 * gracefully. *unit_count is set to the number of units found (0 on
 * failure). Current containers yield exactly one unit; the array shape
 * mirrors cbm_iris_export_to_udl for aggregation symmetry.
 */
char **cbm_twincat_to_st(CBMArena *arena, const char *xml, int xml_len, int *unit_count);
