#pragma once
#include "arena.h"

/*
 * CODESYS / PLCopen TC6 XML export transcoder.
 *
 * A PLCopen export (<project xmlns="http://www.plcopen.org/xml/tc6_0201">) is
 * a very different shape from the TwinCAT container (twincat_xml.c): it never
 * carries ST source verbatim. Every POU's interface is structured XML
 * (<inputVars>/<outputVars>/<inOutVars>/<localVars>/<tempVars>/
 * <externalVars>/<globalVars>, each holding <variable name="x"><type>...
 * </type></variable> entries), and only the executable body
 * (<body><ST><xhtml>...</xhtml></ST></body>) is text. This transcoder
 * SYNTHESIZES the declaration text from the structured XML and appends the
 * body text verbatim (entity-decoded), producing the equivalent textual ST
 * form so the result can be fed to the existing CBM_LANG_IEC_ST extraction
 * pipeline; no new extraction logic is needed.
 *
 * Mapping:
 *   pouType="functionBlock"/"program"/"function" -> FUNCTION_BLOCK/PROGRAM/
 *     FUNCTION, closed by the matching END_*. A function's <returnType>
 *     becomes `FUNCTION name : TYPE`; a missing returnType (malformed input)
 *     falls back to `ANY` so the unit still parses.
 *   Each var section -> its ST block (VAR_INPUT/VAR_OUTPUT/VAR_IN_OUT/VAR/
 *     VAR_TEMP/VAR_EXTERNAL/VAR_GLOBAL), each `<variable name="x">` yielding
 *     `x : TYPE;` (TYPE from the <type> child's tag name, or a <derived
 *     name="X"/> child's `name` attribute), with `<initialValue><simpleValue
 *     value="V"/></initialValue>` appending `:= V`. A variable whose <type>
 *     resolves to the `array` or `pointer` element (ARRAY/POINTER are ST
 *     keywords, not valid bare type identifiers in this grammar) is skipped
 *     rather than emitting text the grammar rejects; nested array/pointer
 *     element-type resolution is not attempted.
 *   <body><ST><xhtml>...</xhtml></ST></body> -> emitted verbatim (XML-entity
 *     decoded) after the declaration blocks. A POU whose body is graphical
 *     (<LD>/<FBD>/<SFC>/<CFC>) or otherwise not <ST> still yields its
 *     declaration unit with an empty implementation; no garbage is emitted.
 *   <actions><action name="X">...</action></actions>, functionBlock POUs
 *     only -> synthesized as a parameterless METHOD (same substitution
 *     twincat_xml.c uses for TwinCAT <Action>; verified here too that the
 *     grammar accepts METHOD nested in FUNCTION_BLOCK but not in PROGRAM, so
 *     actions on a program POU are skipped).
 *   <types><dataTypes><dataType name="X"><baseType>...</baseType></dataType>
 *     -> `TYPE X : ... END_TYPE` (an elementary/derived alias, or a
 *     `<struct>` of `<variable>` fields). Other baseType shapes (enum,
 *     subrange, array) are skipped.
 *   Resource/instance-level <globalVars> (outside any POU's <interface>) are
 *     not transcoded — distinguishing them cleanly from interface-scoped
 *     globalVars without double-emitting was judged not worth the risk for
 *     a construct the brief marks optional.
 *
 * One export file holds many <pou> (and <dataType>) elements; this returns
 * one generated ST string per unit, unlike the TwinCAT container which
 * yields exactly one.
 *
 * Input handling: a UTF-8 BOM is skipped; UTF-16 input (FF FE / FE FF) is
 * rejected. XML entities (&amp; &lt; &gt; &quot; &apos;) are decoded in
 * emitted text and attribute values. Returns an arena-allocated array of
 * NUL-terminated ST strings, or NULL if the input is not a PLCopen export or
 * nothing could be transcoded. *unit_count is set to the number of units
 * found (0 on failure).
 */
char **cbm_plcopen_to_st(CBMArena *arena, const char *xml, int xml_len, int *unit_count);
