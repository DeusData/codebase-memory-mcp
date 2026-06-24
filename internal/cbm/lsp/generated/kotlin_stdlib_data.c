/*
 * kotlin_stdlib_data.c — Curated Kotlin stdlib registration for the LSP.
 *
 * Hand-distilled subset of the Kotlin standard library: the types and
 * functions that account for the vast majority of real-world usages.
 * The reverse-engineered LSP relies on this to resolve calls into
 * `kotlin.*`, `kotlin.collections.*`, `kotlin.text.*`, `kotlin.io.*`,
 * `kotlin.ranges.*`, `kotlin.sequences.*`, and a few `java.lang.*` /
 * `java.util.*` types reachable from default imports under the JVM
 * target.
 *
 * Source of truth: the Kotlin language specification + kotlin-stdlib API
 * docs (1.9.x). We do NOT vendor or fetch any new source — all type
 * shapes here were transcribed by hand from the public spec to match
 * the resolver's expectations.
 *
 * This file is included from lsp_all.c.
 */

#include "../type_rep.h"
#include "../type_registry.h"
#include "../kotlin_lsp.h"
#include <string.h>

/* Convenience macros to compress the repetitive registration calls. */

#define KT_TYPE_SIMPLE(qn_, short_)                                                                \
    do {                                                                                           \
        CBMRegisteredType rt = {0};                                                                \
        rt.qualified_name = (qn_);                                                                 \
        rt.short_name = (short_);                                                                  \
        cbm_registry_add_type(reg, rt);                                                            \
    } while (0)

#define KT_TYPE_WITH_METHODS(qn_, short_, methods_)                                                \
    do {                                                                                           \
        CBMRegisteredType rt = {0};                                                                \
        rt.qualified_name = (qn_);                                                                 \
        rt.short_name = (short_);                                                                  \
        rt.method_names = (methods_);                                                              \
        cbm_registry_add_type(reg, rt);                                                            \
    } while (0)

#define KT_FUNC0(qn_, short_, ret_qn_)                                                             \
    do {                                                                                           \
        CBMRegisteredFunc rf = {0};                                                                \
        rf.qualified_name = (qn_);                                                                 \
        rf.short_name = (short_);                                                                  \
        rf.min_params = 0;                                                                         \
        cbm_registry_add_func(reg, rf);                                                            \
        (void)(ret_qn_);                                                                           \
    } while (0)

#define KT_METHOD(class_qn_, method_qn_, short_)                                                   \
    do {                                                                                           \
        CBMRegisteredFunc rf = {0};                                                                \
        rf.qualified_name = (method_qn_);                                                          \
        rf.receiver_type = (class_qn_);                                                            \
        rf.short_name = (short_);                                                                  \
        rf.min_params = 0;                                                                         \
        cbm_registry_add_func(reg, rf);                                                            \
    } while (0)

/* Default-imported package list, exposed to the LSP context for
 * wildcard-style resolution of bare names (e.g. `println` resolves into
 * `kotlin.io`). Order matters: more-specific shadows less-specific. */
static const char *const KT_DEFAULT_IMPORTS[] = {
    "kotlin",
    "kotlin.annotation",
    "kotlin.collections",
    "kotlin.comparisons",
    "kotlin.io",
    "kotlin.ranges",
    "kotlin.sequences",
    "kotlin.text",
    /* JVM target adds: */
    "java.lang",
    "kotlin.jvm",
};

const char *const *cbm_kotlin_default_import_packages(int *count_out) {
    if (count_out) {
        *count_out = (int)(sizeof(KT_DEFAULT_IMPORTS) / sizeof(KT_DEFAULT_IMPORTS[0]));
    }
    return KT_DEFAULT_IMPORTS;
}

/* Method-name tables (NULL-terminated). Used for is_member checks
 * — the resolver walks these to decide whether `obj.foo()` should
 * be attributed as a method call on a known type. */

static const char *KT_ANY_METHODS[] = {
    "equals", "hashCode", "toString", NULL,
};

static const char *KT_STRING_METHODS[] = {
    "length", "isEmpty", "isNotEmpty", "isBlank", "isNotBlank", "compareTo", "contains",
    "startsWith", "endsWith", "indexOf", "lastIndexOf", "substring", "replace", "split",
    "trim", "trimStart", "trimEnd", "trimIndent", "trimMargin", "toUpperCase", "toLowerCase",
    "uppercase", "lowercase", "uppercaseChar", "lowercaseChar", "capitalize", "decapitalize",
    "reversed", "lines", "padStart", "padEnd", "repeat", "removePrefix", "removeSuffix",
    "removeSurrounding", "toByteArray", "toCharArray", "toInt", "toIntOrNull", "toLong",
    "toLongOrNull", "toDouble", "toDoubleOrNull", "toFloat", "toFloatOrNull", "toBoolean",
    "format", "intern", "matches", "replaceFirst", "filter", "filterNot", "filterIndexed",
    "map", "flatMap", "fold", "reduce", "forEach", "any", "all", "none", "count", "first",
    "firstOrNull", "last", "lastOrNull", "single", "singleOrNull", "take", "takeLast",
    "takeWhile", "drop", "dropLast", "dropWhile", "windowed", "chunked", "zip", "associate",
    "associateBy", "associateWith", "groupBy", "partition", "joinToString", "iterator",
    "subSequence", "get", "set", "plus", "minus", "times", "div", "rem", "compareTo",
    "hashCode", "toString", "equals", NULL,
};

static const char *KT_LIST_METHODS[] = {
    "size", "isEmpty", "contains", "containsAll", "iterator", "listIterator", "subList",
    "indexOf", "lastIndexOf", "get", "first", "firstOrNull", "last", "lastOrNull", "single",
    "singleOrNull", "elementAt", "elementAtOrNull", "elementAtOrElse", "find", "findLast",
    "filter", "filterNot", "filterNotNull", "filterIndexed", "filterIsInstance", "map",
    "mapNotNull", "mapIndexed", "mapIndexedNotNull", "flatMap", "flatten", "fold", "foldRight",
    "foldIndexed", "reduce", "reduceRight", "reduceIndexed", "scan", "runningFold",
    "runningReduce", "forEach", "forEachIndexed", "any", "all", "none", "count", "sum",
    "sumBy", "sumOf", "max", "maxOrNull", "maxBy", "maxByOrNull", "maxOf", "maxOfOrNull",
    "min", "minOrNull", "minBy", "minByOrNull", "minOf", "minOfOrNull", "average", "sorted",
    "sortedBy", "sortedDescending", "sortedByDescending", "sortedWith", "reversed",
    "shuffled", "distinct", "distinctBy", "intersect", "union", "subtract", "groupBy",
    "groupingBy", "associate", "associateBy", "associateWith", "partition", "windowed",
    "chunked", "zip", "zipWithNext", "joinToString", "joinTo", "take", "takeLast",
    "takeWhile", "takeLastWhile", "drop", "dropLast", "dropWhile", "dropLastWhile",
    "asSequence", "asIterable", "toList", "toMutableList", "toSet", "toMutableSet",
    "toHashSet", "toSortedSet", "toTypedArray", "toCollection", "plus", "minus",
    "ifEmpty", "stream", NULL,
};

static const char *KT_MUTABLE_LIST_METHODS[] = {
    "add", "addAll", "remove", "removeAt", "removeAll", "removeFirst", "removeLast",
    "removeIf", "retainAll", "clear", "set", "fill", "sort", "sortBy", "sortByDescending",
    "sortWith", "reverse", "shuffle", "swap", "trimToSize",
    /* Plus all read-only methods (inherited): */
    "size", "isEmpty", "contains", "containsAll", "iterator", "listIterator", "subList",
    "indexOf", "lastIndexOf", "get", "first", "firstOrNull", "last", "lastOrNull", "single",
    "singleOrNull", "find", "findLast", "elementAt", "elementAtOrNull", "elementAtOrElse",
    "filter", "filterNot", "filterNotNull", "filterIndexed", "filterIsInstance", "map",
    "mapNotNull", "mapIndexed", "flatMap", "flatten", "fold", "foldRight", "reduce",
    "reduceRight", "forEach", "forEachIndexed", "any", "all", "none", "count", "sum",
    "sumBy", "sumOf", "max", "maxOrNull", "maxBy", "min", "minOrNull", "minBy", "average",
    "sorted", "sortedBy", "sortedDescending", "sortedByDescending", "sortedWith",
    "reversed", "shuffled", "distinct", "distinctBy", "groupBy", "groupingBy", "associate",
    "associateBy", "associateWith", "partition", "windowed", "chunked", "zip", "zipWithNext",
    "joinToString", "joinTo", "take", "takeLast", "takeWhile", "drop", "dropLast",
    "dropWhile", "asSequence", "asIterable", "toList", "toMutableList", "toSet",
    "toMutableSet", "toHashSet", "toTypedArray", "toCollection", "ifEmpty", "stream", NULL,
};

static const char *KT_MAP_METHODS[] = {
    "size", "isEmpty", "containsKey", "containsValue", "get", "getOrDefault", "getOrElse",
    "getOrNull", "getValue", "keys", "values", "entries", "iterator", "forEach", "filter",
    "filterKeys", "filterValues", "filterNot", "filterTo", "map", "mapKeys", "mapValues",
    "mapNotNull", "any", "all", "none", "count", "max", "min", "maxBy", "minBy", "toList",
    "toMap", "toMutableMap", "toSortedMap", "asSequence", "asIterable", "plus", "minus",
    "ifEmpty", NULL,
};

static const char *KT_MUTABLE_MAP_METHODS[] = {
    "put", "putAll", "putIfAbsent", "remove", "clear", "computeIfAbsent", "computeIfPresent",
    "compute", "merge", "replace", "replaceAll", "getOrPut",
    /* Plus read-only methods: */
    "size", "isEmpty", "containsKey", "containsValue", "get", "keys", "values", "entries",
    "iterator", "forEach", "filter", "map", NULL,
};

static const char *KT_SET_METHODS[] = {
    "size", "isEmpty", "contains", "containsAll", "iterator", "intersect", "union",
    "subtract", "filter", "map", "fold", "reduce", "forEach", "any", "all", "none",
    "count", "first", "last", "elementAt", NULL,
};

static const char *KT_ITERATOR_METHODS[] = {
    "hasNext", "next", "remove", "nextIndex", "previousIndex", "previous", "hasPrevious",
    "set", "add", NULL,
};

static const char *KT_SEQUENCE_METHODS[] = {
    "iterator", "filter", "filterNot", "filterNotNull", "filterIndexed", "filterIsInstance",
    "map", "mapNotNull", "mapIndexed", "flatMap", "flatten", "fold", "foldIndexed", "reduce",
    "reduceIndexed", "forEach", "forEachIndexed", "any", "all", "none", "count", "sum",
    "sumBy", "sumOf", "max", "maxOrNull", "min", "minOrNull", "first", "firstOrNull",
    "last", "lastOrNull", "single", "singleOrNull", "find", "findLast", "elementAt",
    "elementAtOrNull", "take", "takeWhile", "drop", "dropWhile", "windowed", "chunked",
    "zip", "zipWithNext", "distinct", "distinctBy", "sorted", "sortedBy", "sortedDescending",
    "sortedWith", "associate", "associateBy", "associateWith", "groupBy", "partition",
    "joinToString", "toList", "toSet", "toMap", "toCollection", "asIterable", "constrainOnce",
    NULL,
};

static const char *KT_INT_RANGE_METHODS[] = {
    "first", "last", "step", "isEmpty", "iterator", "contains", "reversed", "forEach",
    "map", "filter", "sum", "count", "any", "all", "none", "elementAt", NULL,
};

static const char *KT_PAIR_METHODS[] = {
    "first", "second", "toString", "toList", "component1", "component2", "copy",
    NULL,
};

static const char *KT_TRIPLE_METHODS[] = {
    "first", "second", "third", "toString", "toList", "component1", "component2",
    "component3", "copy", NULL,
};

static const char *KT_THROWABLE_METHODS[] = {
    "message", "cause", "stackTrace", "stackTraceToString", "printStackTrace",
    "addSuppressed", "getSuppressed", "fillInStackTrace", "initCause", "toString", NULL,
};

static const char *KT_EXCEPTION_METHODS[] = {
    "message", "cause", "stackTrace", "stackTraceToString", "printStackTrace", "toString",
    NULL,
};

static const char *KT_NUMBER_METHODS[] = {
    "toByte", "toShort", "toInt", "toLong", "toFloat", "toDouble", "toChar", "toString",
    "compareTo", "equals", "hashCode",
    /* Operator conventions */
    "plus", "minus", "times", "div", "rem", "mod", "inc", "dec", "unaryMinus", "unaryPlus",
    "and", "or", "xor", "inv", "shl", "shr", "ushr",
    "rangeTo", "rangeUntil", "downTo", "until", "coerceAtLeast", "coerceAtMost", "coerceIn",
    NULL,
};

static const char *KT_BOOLEAN_METHODS[] = {
    "and", "or", "xor", "not", "compareTo", "toString", "equals", "hashCode", NULL,
};

static const char *KT_CHAR_METHODS[] = {
    "isDigit", "isLetter", "isLetterOrDigit", "isWhitespace", "isUpperCase", "isLowerCase",
    "uppercase", "lowercase", "uppercaseChar", "lowercaseChar", "digitToInt", "code",
    "compareTo", "toString", "equals", "hashCode", "plus", "minus", "rangeTo",
    "isHighSurrogate", "isLowSurrogate", "isSurrogate", NULL,
};

static const char *KT_ARRAY_METHODS[] = {
    "size", "get", "set", "iterator", "isEmpty", "isNotEmpty", "contains", "indexOf",
    "lastIndexOf", "first", "last", "filter", "map", "fold", "reduce", "forEach", "any",
    "all", "none", "count", "sum", "max", "min", "joinToString", "asList", "asSequence",
    "toList", "toMutableList", "toSet", "copyOf", "copyOfRange", "fill", "sort", "sorted",
    "reversed", "reverse", "binarySearch", "plus", "component1", "component2", NULL,
};

static const char *KT_REGEX_METHODS[] = {
    "matchEntire", "matches", "containsMatchIn", "find", "findAll", "replace",
    "replaceFirst", "split", "splitToSequence", "toPattern", "pattern", NULL,
};

static const char *KT_MATCH_RESULT_METHODS[] = {
    "value", "range", "groups", "groupValues", "destructured", "next", NULL,
};

static const char *KT_FILE_METHODS[] = {
    "path", "name", "parent", "exists", "isFile", "isDirectory", "isAbsolute", "length",
    "lastModified", "canRead", "canWrite", "delete", "deleteRecursively", "createNewFile",
    "mkdir", "mkdirs", "renameTo", "list", "listFiles", "walk", "walkTopDown",
    "walkBottomUp", "readText", "readLines", "readBytes", "writeText", "writeBytes",
    "appendText", "appendBytes", "useLines", "forEachLine", "bufferedReader",
    "bufferedWriter", "inputStream", "outputStream", "reader", "writer", "printWriter",
    "absolutePath", "absoluteFile", "canonicalPath", "canonicalFile", "extension",
    "nameWithoutExtension", "toPath", "toURI", "resolve", "resolveSibling", "relativeTo",
    "copyTo", "copyRecursively", "endsWith", "startsWith", "normalize", NULL,
};

static const char *KT_SCOPE_METHODS[] = {
    "let", "run", "with", "apply", "also", "takeIf", "takeUnless", "use", NULL,
};

static const char *KT_LAZY_METHODS[] = {
    "value", "isInitialized", "getValue", NULL,
};

static const char *KT_RESULT_METHODS[] = {
    "getOrNull", "exceptionOrNull", "isSuccess", "isFailure", "getOrThrow", "getOrDefault",
    "getOrElse", "fold", "map", "mapCatching", "recover", "recoverCatching", "onSuccess",
    "onFailure", NULL,
};

/* ── Top-level builtin function registration. ──────────────────────
 * We register the names that show up in the wild as bare calls and
 * route them to their kotlin.* / kotlin.io.* QNs. The resolver checks
 * in-package and class-method lookups before falling through to these.
 */

void cbm_kotlin_stdlib_register(CBMTypeRegistry *reg, CBMArena *arena) {
    (void)arena;

    /* Core types — kotlin.* */
    KT_TYPE_WITH_METHODS("kotlin.Any", "Any", KT_ANY_METHODS);
    KT_TYPE_WITH_METHODS("kotlin.Number", "Number", KT_NUMBER_METHODS);
    KT_TYPE_SIMPLE("kotlin.Unit", "Unit");
    KT_TYPE_SIMPLE("kotlin.Nothing", "Nothing");
    KT_TYPE_WITH_METHODS("kotlin.Boolean", "Boolean", KT_BOOLEAN_METHODS);
    KT_TYPE_WITH_METHODS("kotlin.Byte", "Byte", KT_NUMBER_METHODS);
    KT_TYPE_WITH_METHODS("kotlin.Short", "Short", KT_NUMBER_METHODS);
    KT_TYPE_WITH_METHODS("kotlin.Int", "Int", KT_NUMBER_METHODS);
    KT_TYPE_WITH_METHODS("kotlin.Long", "Long", KT_NUMBER_METHODS);
    KT_TYPE_WITH_METHODS("kotlin.Float", "Float", KT_NUMBER_METHODS);
    KT_TYPE_WITH_METHODS("kotlin.Double", "Double", KT_NUMBER_METHODS);
    KT_TYPE_WITH_METHODS("kotlin.Char", "Char", KT_CHAR_METHODS);
    KT_TYPE_WITH_METHODS("kotlin.String", "String", KT_STRING_METHODS);
    KT_TYPE_WITH_METHODS("kotlin.CharSequence", "CharSequence", KT_STRING_METHODS);
    KT_TYPE_WITH_METHODS("kotlin.Throwable", "Throwable", KT_THROWABLE_METHODS);
    KT_TYPE_WITH_METHODS("kotlin.Pair", "Pair", KT_PAIR_METHODS);
    KT_TYPE_WITH_METHODS("kotlin.Triple", "Triple", KT_TRIPLE_METHODS);
    KT_TYPE_WITH_METHODS("kotlin.Result", "Result", KT_RESULT_METHODS);
    KT_TYPE_WITH_METHODS("kotlin.Lazy", "Lazy", KT_LAZY_METHODS);
    KT_TYPE_SIMPLE("kotlin.Enum", "Enum");
    KT_TYPE_SIMPLE("kotlin.Function", "Function");
    KT_TYPE_SIMPLE("kotlin.KotlinVersion", "KotlinVersion");

    /* Common exceptions */
    KT_TYPE_WITH_METHODS("kotlin.Exception", "Exception", KT_EXCEPTION_METHODS);
    KT_TYPE_WITH_METHODS("kotlin.RuntimeException", "RuntimeException", KT_EXCEPTION_METHODS);
    KT_TYPE_WITH_METHODS("kotlin.IllegalArgumentException", "IllegalArgumentException",
                         KT_EXCEPTION_METHODS);
    KT_TYPE_WITH_METHODS("kotlin.IllegalStateException", "IllegalStateException",
                         KT_EXCEPTION_METHODS);
    KT_TYPE_WITH_METHODS("kotlin.IndexOutOfBoundsException", "IndexOutOfBoundsException",
                         KT_EXCEPTION_METHODS);
    KT_TYPE_WITH_METHODS("kotlin.NoSuchElementException", "NoSuchElementException",
                         KT_EXCEPTION_METHODS);
    KT_TYPE_WITH_METHODS("kotlin.NumberFormatException", "NumberFormatException",
                         KT_EXCEPTION_METHODS);
    KT_TYPE_WITH_METHODS("kotlin.NullPointerException", "NullPointerException",
                         KT_EXCEPTION_METHODS);
    KT_TYPE_WITH_METHODS("kotlin.ClassCastException", "ClassCastException", KT_EXCEPTION_METHODS);
    KT_TYPE_WITH_METHODS("kotlin.UnsupportedOperationException", "UnsupportedOperationException",
                         KT_EXCEPTION_METHODS);
    KT_TYPE_WITH_METHODS("kotlin.ArithmeticException", "ArithmeticException",
                         KT_EXCEPTION_METHODS);

    /* Collections */
    KT_TYPE_WITH_METHODS("kotlin.collections.Iterable", "Iterable", KT_LIST_METHODS);
    KT_TYPE_WITH_METHODS("kotlin.collections.Collection", "Collection", KT_LIST_METHODS);
    KT_TYPE_WITH_METHODS("kotlin.collections.MutableCollection", "MutableCollection",
                         KT_MUTABLE_LIST_METHODS);
    KT_TYPE_WITH_METHODS("kotlin.collections.List", "List", KT_LIST_METHODS);
    KT_TYPE_WITH_METHODS("kotlin.collections.MutableList", "MutableList", KT_MUTABLE_LIST_METHODS);
    KT_TYPE_WITH_METHODS("kotlin.collections.Set", "Set", KT_SET_METHODS);
    KT_TYPE_WITH_METHODS("kotlin.collections.MutableSet", "MutableSet", KT_SET_METHODS);
    KT_TYPE_WITH_METHODS("kotlin.collections.HashSet", "HashSet", KT_SET_METHODS);
    KT_TYPE_WITH_METHODS("kotlin.collections.LinkedHashSet", "LinkedHashSet", KT_SET_METHODS);
    KT_TYPE_WITH_METHODS("kotlin.collections.Map", "Map", KT_MAP_METHODS);
    KT_TYPE_WITH_METHODS("kotlin.collections.MutableMap", "MutableMap", KT_MUTABLE_MAP_METHODS);
    KT_TYPE_WITH_METHODS("kotlin.collections.HashMap", "HashMap", KT_MUTABLE_MAP_METHODS);
    KT_TYPE_WITH_METHODS("kotlin.collections.LinkedHashMap", "LinkedHashMap",
                         KT_MUTABLE_MAP_METHODS);
    KT_TYPE_WITH_METHODS("kotlin.collections.ArrayList", "ArrayList", KT_MUTABLE_LIST_METHODS);
    KT_TYPE_WITH_METHODS("kotlin.collections.Iterator", "Iterator", KT_ITERATOR_METHODS);
    KT_TYPE_WITH_METHODS("kotlin.collections.MutableIterator", "MutableIterator",
                         KT_ITERATOR_METHODS);
    KT_TYPE_WITH_METHODS("kotlin.collections.ListIterator", "ListIterator", KT_ITERATOR_METHODS);

    /* Sequences */
    KT_TYPE_WITH_METHODS("kotlin.sequences.Sequence", "Sequence", KT_SEQUENCE_METHODS);

    /* Arrays */
    KT_TYPE_WITH_METHODS("kotlin.Array", "Array", KT_ARRAY_METHODS);
    KT_TYPE_WITH_METHODS("kotlin.IntArray", "IntArray", KT_ARRAY_METHODS);
    KT_TYPE_WITH_METHODS("kotlin.LongArray", "LongArray", KT_ARRAY_METHODS);
    KT_TYPE_WITH_METHODS("kotlin.DoubleArray", "DoubleArray", KT_ARRAY_METHODS);
    KT_TYPE_WITH_METHODS("kotlin.FloatArray", "FloatArray", KT_ARRAY_METHODS);
    KT_TYPE_WITH_METHODS("kotlin.ByteArray", "ByteArray", KT_ARRAY_METHODS);
    KT_TYPE_WITH_METHODS("kotlin.ShortArray", "ShortArray", KT_ARRAY_METHODS);
    KT_TYPE_WITH_METHODS("kotlin.BooleanArray", "BooleanArray", KT_ARRAY_METHODS);
    KT_TYPE_WITH_METHODS("kotlin.CharArray", "CharArray", KT_ARRAY_METHODS);

    /* Ranges */
    KT_TYPE_WITH_METHODS("kotlin.ranges.IntRange", "IntRange", KT_INT_RANGE_METHODS);
    KT_TYPE_WITH_METHODS("kotlin.ranges.LongRange", "LongRange", KT_INT_RANGE_METHODS);
    KT_TYPE_WITH_METHODS("kotlin.ranges.CharRange", "CharRange", KT_INT_RANGE_METHODS);
    KT_TYPE_WITH_METHODS("kotlin.ranges.IntProgression", "IntProgression", KT_INT_RANGE_METHODS);
    KT_TYPE_WITH_METHODS("kotlin.ranges.LongProgression", "LongProgression", KT_INT_RANGE_METHODS);
    KT_TYPE_WITH_METHODS("kotlin.ranges.ClosedRange", "ClosedRange", KT_INT_RANGE_METHODS);
    KT_TYPE_WITH_METHODS("kotlin.ranges.ClosedFloatingPointRange", "ClosedFloatingPointRange",
                         KT_INT_RANGE_METHODS);

    /* Text */
    KT_TYPE_WITH_METHODS("kotlin.text.Regex", "Regex", KT_REGEX_METHODS);
    KT_TYPE_WITH_METHODS("kotlin.text.MatchResult", "MatchResult", KT_MATCH_RESULT_METHODS);
    KT_TYPE_WITH_METHODS("kotlin.text.StringBuilder", "StringBuilder", KT_STRING_METHODS);

    /* IO (java.io.File is reachable through kotlin.io extension functions) */
    KT_TYPE_WITH_METHODS("java.io.File", "File", KT_FILE_METHODS);
    KT_TYPE_WITH_METHODS("java.io.PrintStream", "PrintStream", KT_FILE_METHODS);

    /* java.lang basics that come in via default JVM imports */
    KT_TYPE_WITH_METHODS("java.lang.String", "String", KT_STRING_METHODS);
    KT_TYPE_WITH_METHODS("java.lang.Object", "Object", KT_ANY_METHODS);
    KT_TYPE_WITH_METHODS("java.lang.Throwable", "Throwable", KT_THROWABLE_METHODS);
    KT_TYPE_WITH_METHODS("java.lang.Exception", "Exception", KT_EXCEPTION_METHODS);
    KT_TYPE_WITH_METHODS("java.lang.RuntimeException", "RuntimeException", KT_EXCEPTION_METHODS);
    KT_TYPE_WITH_METHODS("java.lang.IllegalArgumentException", "IllegalArgumentException",
                         KT_EXCEPTION_METHODS);
    KT_TYPE_WITH_METHODS("java.lang.IllegalStateException", "IllegalStateException",
                         KT_EXCEPTION_METHODS);
    KT_TYPE_WITH_METHODS("java.lang.NumberFormatException", "NumberFormatException",
                         KT_EXCEPTION_METHODS);
    KT_TYPE_SIMPLE("java.lang.System", "System");
    KT_TYPE_SIMPLE("java.lang.Math", "Math");
    KT_TYPE_SIMPLE("java.lang.Integer", "Integer");
    KT_TYPE_SIMPLE("java.lang.Long", "Long");
    KT_TYPE_SIMPLE("java.lang.Double", "Double");
    KT_TYPE_SIMPLE("java.lang.Float", "Float");
    KT_TYPE_SIMPLE("java.lang.Boolean", "Boolean");

    /* ── Top-level functions ── */
    /* kotlin.io */
    KT_FUNC0("kotlin.io.println", "println", "kotlin.Unit");
    KT_FUNC0("kotlin.io.print", "print", "kotlin.Unit");
    KT_FUNC0("kotlin.io.readLine", "readLine", "kotlin.String");
    KT_FUNC0("kotlin.io.readln", "readln", "kotlin.String");
    KT_FUNC0("kotlin.io.readlnOrNull", "readlnOrNull", "kotlin.String");

    /* kotlin (root) */
    KT_FUNC0("kotlin.error", "error", "kotlin.Nothing");
    KT_FUNC0("kotlin.check", "check", "kotlin.Unit");
    KT_FUNC0("kotlin.checkNotNull", "checkNotNull", "kotlin.Any");
    KT_FUNC0("kotlin.require", "require", "kotlin.Unit");
    KT_FUNC0("kotlin.requireNotNull", "requireNotNull", "kotlin.Any");
    KT_FUNC0("kotlin.assert", "assert", "kotlin.Unit");
    KT_FUNC0("kotlin.TODO", "TODO", "kotlin.Nothing");
    KT_FUNC0("kotlin.runCatching", "runCatching", "kotlin.Result");
    KT_FUNC0("kotlin.run", "run", "kotlin.Any");
    KT_FUNC0("kotlin.with", "with", "kotlin.Any");
    KT_FUNC0("kotlin.repeat", "repeat", "kotlin.Unit");
    KT_FUNC0("kotlin.synchronized", "synchronized", "kotlin.Any");
    KT_FUNC0("kotlin.lazy", "lazy", "kotlin.Lazy");
    KT_FUNC0("kotlin.lazyOf", "lazyOf", "kotlin.Lazy");
    KT_FUNC0("kotlin.to", "to", "kotlin.Pair");

    /* kotlin.collections — top-level builders */
    KT_FUNC0("kotlin.collections.listOf", "listOf", "kotlin.collections.List");
    KT_FUNC0("kotlin.collections.mutableListOf", "mutableListOf",
             "kotlin.collections.MutableList");
    KT_FUNC0("kotlin.collections.arrayListOf", "arrayListOf", "kotlin.collections.ArrayList");
    KT_FUNC0("kotlin.collections.emptyList", "emptyList", "kotlin.collections.List");
    KT_FUNC0("kotlin.collections.listOfNotNull", "listOfNotNull", "kotlin.collections.List");
    KT_FUNC0("kotlin.collections.setOf", "setOf", "kotlin.collections.Set");
    KT_FUNC0("kotlin.collections.mutableSetOf", "mutableSetOf", "kotlin.collections.MutableSet");
    KT_FUNC0("kotlin.collections.hashSetOf", "hashSetOf", "kotlin.collections.HashSet");
    KT_FUNC0("kotlin.collections.linkedSetOf", "linkedSetOf",
             "kotlin.collections.LinkedHashSet");
    KT_FUNC0("kotlin.collections.emptySet", "emptySet", "kotlin.collections.Set");
    KT_FUNC0("kotlin.collections.sortedSetOf", "sortedSetOf", "kotlin.collections.Set");
    KT_FUNC0("kotlin.collections.mapOf", "mapOf", "kotlin.collections.Map");
    KT_FUNC0("kotlin.collections.mutableMapOf", "mutableMapOf", "kotlin.collections.MutableMap");
    KT_FUNC0("kotlin.collections.hashMapOf", "hashMapOf", "kotlin.collections.HashMap");
    KT_FUNC0("kotlin.collections.linkedMapOf", "linkedMapOf",
             "kotlin.collections.LinkedHashMap");
    KT_FUNC0("kotlin.collections.emptyMap", "emptyMap", "kotlin.collections.Map");
    KT_FUNC0("kotlin.collections.sortedMapOf", "sortedMapOf", "kotlin.collections.Map");
    KT_FUNC0("kotlin.arrayOf", "arrayOf", "kotlin.Array");
    KT_FUNC0("kotlin.arrayOfNulls", "arrayOfNulls", "kotlin.Array");
    KT_FUNC0("kotlin.emptyArray", "emptyArray", "kotlin.Array");
    KT_FUNC0("kotlin.intArrayOf", "intArrayOf", "kotlin.IntArray");
    KT_FUNC0("kotlin.longArrayOf", "longArrayOf", "kotlin.LongArray");
    KT_FUNC0("kotlin.floatArrayOf", "floatArrayOf", "kotlin.FloatArray");
    KT_FUNC0("kotlin.doubleArrayOf", "doubleArrayOf", "kotlin.DoubleArray");
    KT_FUNC0("kotlin.byteArrayOf", "byteArrayOf", "kotlin.ByteArray");
    KT_FUNC0("kotlin.shortArrayOf", "shortArrayOf", "kotlin.ShortArray");
    KT_FUNC0("kotlin.booleanArrayOf", "booleanArrayOf", "kotlin.BooleanArray");
    KT_FUNC0("kotlin.charArrayOf", "charArrayOf", "kotlin.CharArray");

    /* kotlin.sequences */
    KT_FUNC0("kotlin.sequences.sequenceOf", "sequenceOf", "kotlin.sequences.Sequence");
    KT_FUNC0("kotlin.sequences.emptySequence", "emptySequence", "kotlin.sequences.Sequence");
    KT_FUNC0("kotlin.sequences.generateSequence", "generateSequence",
             "kotlin.sequences.Sequence");
    KT_FUNC0("kotlin.sequences.sequence", "sequence", "kotlin.sequences.Sequence");

    /* kotlin.ranges */
    KT_FUNC0("kotlin.ranges.until", "until", "kotlin.ranges.IntRange");
    KT_FUNC0("kotlin.ranges.downTo", "downTo", "kotlin.ranges.IntProgression");
    KT_FUNC0("kotlin.ranges.coerceAtLeast", "coerceAtLeast", "kotlin.Int");
    KT_FUNC0("kotlin.ranges.coerceAtMost", "coerceAtMost", "kotlin.Int");
    KT_FUNC0("kotlin.ranges.coerceIn", "coerceIn", "kotlin.Int");

    /* kotlin.comparisons */
    KT_FUNC0("kotlin.comparisons.compareValues", "compareValues", "kotlin.Int");
    KT_FUNC0("kotlin.comparisons.compareBy", "compareBy", "java.util.Comparator");
    KT_FUNC0("kotlin.comparisons.compareByDescending", "compareByDescending",
             "java.util.Comparator");
    KT_FUNC0("kotlin.comparisons.minOf", "minOf", "kotlin.Comparable");
    KT_FUNC0("kotlin.comparisons.maxOf", "maxOf", "kotlin.Comparable");
    KT_FUNC0("kotlin.comparisons.naturalOrder", "naturalOrder", "java.util.Comparator");
    KT_FUNC0("kotlin.comparisons.reverseOrder", "reverseOrder", "java.util.Comparator");

    /* kotlin.text — useful builders on String companion */
    KT_FUNC0("kotlin.text.buildString", "buildString", "kotlin.String");
    KT_FUNC0("kotlin.text.toRegex", "toRegex", "kotlin.text.Regex");

    /* kotlin scope-functions are registered as extension functions
     * (receiver_type = "kotlin.Any") so that resolver finds them on every
     * receiver. Their full QN is kotlin.<name>. */
    {
        const char *names[] = {"let", "run", "apply", "also", "takeIf", "takeUnless", "use", NULL};
        const char *qns[] = {"kotlin.let", "kotlin.run", "kotlin.apply", "kotlin.also",
                             "kotlin.takeIf", "kotlin.takeUnless",
                             "kotlin.io.use",  /* `use` lives in kotlin.io for Closeable */
                             NULL};
        for (int i = 0; names[i]; i++) {
            CBMRegisteredFunc rf = {0};
            rf.qualified_name = qns[i];
            rf.short_name = names[i];
            rf.receiver_type = "kotlin.Any";
            rf.min_params = 1;  /* the lambda */
            cbm_registry_add_func(reg, rf);
        }
    }
    (void)KT_SCOPE_METHODS;  /* reserved for future tightening */
}
