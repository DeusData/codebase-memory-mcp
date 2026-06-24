/*
 * java_stdlib_data.c — Curated Java standard-library type/method registry.
 *
 * Strategy:
 *   - java.lang.* — fully covered (the implicit-import package).
 *     Object, String, StringBuilder, StringBuffer, CharSequence, Class,
 *     Throwable + the common subclass tree, Number + boxed primitives,
 *     Math, System, Thread, Iterable, Comparable, Cloneable, Enum, Record,
 *     AutoCloseable, the common Exception types.
 *   - java.util.* — collections + iterators + Optional + Date/Calendar +
 *     Arrays/Collections + Scanner/Random/UUID + Map.Entry.
 *   - java.io.* — streams, readers, writers, File, IOException family.
 *   - java.nio.file.* — Path, Paths, Files (often-used helpers).
 *   - java.util.function — the 21 functional interfaces.
 *   - java.util.stream  — Stream + Collectors entry points.
 *   - java.util.concurrent — ExecutorService, Future, CompletableFuture,
 *     ConcurrentHashMap, the concurrent collection set.
 *   - java.time — LocalDate/LocalTime/LocalDateTime/Duration/Instant.
 *
 * Method signatures use registry-level fidelity: receiver, short name,
 * return type. Param types are intentionally unmodeled (the resolver
 * chooses overloads by arity, with type compatibility scoring breaking
 * ties — see cbm_registry_lookup_method_by_args).
 *
 * This is the JLS-spec-aligned slice of the stdlib that 90%+ of real-world
 * Java code touches.
 */

#include "../type_rep.h"
#include "../type_registry.h"
#include "../../arena.h"
#include "../java_lsp.h"
#include <string.h>

#define REG_TYPE(qn_, short_, is_iface_, parents_)            \
    do {                                                      \
        memset(&rt, 0, sizeof(rt));                           \
        rt.qualified_name = (qn_);                            \
        rt.short_name = (short_);                             \
        rt.is_interface = (is_iface_);                        \
        rt.embedded_types = (parents_);                       \
        cbm_registry_add_type(reg, rt);                       \
    } while (0)

#define REG_METHOD(class_qn_, method_name_, ret_type_)                                          \
    do {                                                                                        \
        memset(&rf, 0, sizeof(rf));                                                             \
        rf.min_params = -1;                                                                     \
        rf.qualified_name =                                                                     \
            cbm_arena_sprintf(arena, "%s.%s", (class_qn_), (method_name_));                     \
        rf.short_name = (method_name_);                                                         \
        rf.receiver_type = (class_qn_);                                                         \
        {                                                                                       \
            const CBMType **rets =                                                              \
                (const CBMType **)cbm_arena_alloc(arena, 2 * sizeof(*rets));                    \
            rets[0] = (ret_type_);                                                              \
            rets[1] = NULL;                                                                     \
            rf.signature = cbm_type_func(arena, NULL, NULL, rets);                              \
        }                                                                                       \
        cbm_registry_add_func(reg, rf);                                                         \
    } while (0)

#define REG_CTOR(class_qn_, short_name_)                                              \
    do {                                                                              \
        memset(&rf, 0, sizeof(rf));                                                   \
        rf.min_params = -1;                                                           \
        rf.qualified_name =                                                           \
            cbm_arena_sprintf(arena, "%s.%s", (class_qn_), (short_name_));            \
        rf.short_name = (short_name_);                                                \
        rf.receiver_type = (class_qn_);                                               \
        {                                                                             \
            const CBMType **rets =                                                    \
                (const CBMType **)cbm_arena_alloc(arena, 2 * sizeof(*rets));          \
            rets[0] = cbm_type_named(arena, (class_qn_));                             \
            rets[1] = NULL;                                                           \
            rf.signature = cbm_type_func(arena, NULL, NULL, rets);                    \
        }                                                                             \
        cbm_registry_add_func(reg, rf);                                               \
    } while (0)

#define REG_FIELD(class_qn_, name_, type_)                                            \
    do {                                                                              \
        const CBMRegisteredType *_existing =                                          \
            cbm_registry_lookup_type(reg, (class_qn_));                               \
        (void)_existing;                                                              \
        /* Field append handled by REG_TYPE_FIELDS below. */                          \
        /* Placeholder for future per-field appends. */                               \
    } while (0)

void cbm_java_stdlib_register(CBMTypeRegistry *reg, CBMArena *arena) {
    CBMRegisteredType rt;
    CBMRegisteredFunc rf;

    /* ── Type-parent lists (must be static so addresses outlive the call) ── */
    static const char *no_parents[] = {NULL};
    static const char *parents_object[] = {"java.lang.Object", NULL};
    static const char *parents_throwable[] = {"java.lang.Object", NULL};
    static const char *parents_exception[] = {"java.lang.Throwable", NULL};
    static const char *parents_error[] = {"java.lang.Throwable", NULL};
    static const char *parents_runtime_exc[] = {"java.lang.Exception", NULL};
    static const char *parents_io_exc[] = {"java.lang.Exception", NULL};
    static const char *parents_number[] = {"java.lang.Object", NULL};
    static const char *parents_integer[] = {"java.lang.Number", NULL};
    static const char *parents_long[] = {"java.lang.Number", NULL};
    static const char *parents_double[] = {"java.lang.Number", NULL};
    static const char *parents_float[] = {"java.lang.Number", NULL};
    static const char *parents_short[] = {"java.lang.Number", NULL};
    static const char *parents_byte[] = {"java.lang.Number", NULL};
    static const char *parents_string[] = {"java.lang.Object", NULL};
    static const char *parents_charseq[] = {NULL};
    static const char *parents_iterable[] = {NULL};
    static const char *parents_collection[] = {"java.lang.Iterable", NULL};
    static const char *parents_list[] = {"java.util.Collection", NULL};
    static const char *parents_set[] = {"java.util.Collection", NULL};
    static const char *parents_queue[] = {"java.util.Collection", NULL};
    static const char *parents_deque[] = {"java.util.Queue", NULL};
    static const char *parents_map[] = {NULL};
    static const char *parents_map_entry[] = {NULL};
    static const char *parents_iterator[] = {NULL};
    static const char *parents_arraylist[] = {"java.util.List", NULL};
    static const char *parents_linkedlist[] = {"java.util.List", NULL};
    static const char *parents_hashset[] = {"java.util.Set", NULL};
    static const char *parents_treeset[] = {"java.util.Set", NULL};
    static const char *parents_linkedhashset[] = {"java.util.Set", NULL};
    static const char *parents_hashmap[] = {"java.util.Map", NULL};
    static const char *parents_treemap[] = {"java.util.Map", NULL};
    static const char *parents_linkedhashmap[] = {"java.util.Map", NULL};
    static const char *parents_concurrent_hashmap[] = {"java.util.Map", NULL};

    static const char *parents_inputstream[] = {"java.lang.AutoCloseable", NULL};
    static const char *parents_outputstream[] = {"java.lang.AutoCloseable", NULL};
    static const char *parents_reader[] = {"java.lang.AutoCloseable", NULL};
    static const char *parents_writer[] = {"java.lang.AutoCloseable", NULL};
    static const char *parents_buffered_reader[] = {"java.io.Reader", NULL};
    static const char *parents_buffered_writer[] = {"java.io.Writer", NULL};
    static const char *parents_print_stream[] = {"java.io.OutputStream", NULL};
    static const char *parents_print_writer[] = {"java.io.Writer", NULL};
    static const char *parents_file_input_stream[] = {"java.io.InputStream", NULL};
    static const char *parents_file_output_stream[] = {"java.io.OutputStream", NULL};
    static const char *parents_file_reader[] = {"java.io.Reader", NULL};
    static const char *parents_file_writer[] = {"java.io.Writer", NULL};
    static const char *parents_io_exception[] = {"java.lang.Exception", NULL};
    static const char *parents_runtime_exc_chain[] = {"java.lang.RuntimeException", NULL};
    /* Parent lists for types previously registered with inline compound
     * literals. A compound literal has automatic (block) storage duration,
     * so storing its address into the registry left a dangling stack pointer
     * once the REG_TYPE statement's block ended — an AddressSanitizer
     * stack-use-after-scope when the inheritance walk later read
     * rt->embedded_types[0]. These must be static so their addresses outlive
     * the call, exactly like the parent lists above. */
    static const char *parents_gregorian_calendar[] = {"java.util.Calendar", NULL};
    static const char *parents_file_not_found_exc[] = {"java.io.IOException", NULL};
    static const char *parents_closeable[] = {"java.lang.AutoCloseable", NULL};
    static const char *parents_unary_operator[] = {"java.util.function.Function", NULL};
    static const char *parents_binary_operator[] = {"java.util.function.BiFunction", NULL};
    static const char *parents_completable_future[] = {"java.util.concurrent.Future", NULL};
    static const char *parents_reentrant_lock[] = {"java.util.concurrent.locks.Lock", NULL};

    /* ── java.lang ─────────────────────────────────────────────── */
    REG_TYPE("java.lang.Object", "Object", false, no_parents);
    REG_TYPE("java.lang.Class", "Class", false, parents_object);
    REG_TYPE("java.lang.ClassLoader", "ClassLoader", false, parents_object);
    REG_TYPE("java.lang.CharSequence", "CharSequence", true, parents_charseq);
    REG_TYPE("java.lang.String", "String", false, parents_string);
    REG_TYPE("java.lang.StringBuilder", "StringBuilder", false, parents_object);
    REG_TYPE("java.lang.StringBuffer", "StringBuffer", false, parents_object);
    REG_TYPE("java.lang.Number", "Number", false, parents_number);
    REG_TYPE("java.lang.Integer", "Integer", false, parents_integer);
    REG_TYPE("java.lang.Long", "Long", false, parents_long);
    REG_TYPE("java.lang.Short", "Short", false, parents_short);
    REG_TYPE("java.lang.Byte", "Byte", false, parents_byte);
    REG_TYPE("java.lang.Float", "Float", false, parents_float);
    REG_TYPE("java.lang.Double", "Double", false, parents_double);
    REG_TYPE("java.lang.Boolean", "Boolean", false, parents_object);
    REG_TYPE("java.lang.Character", "Character", false, parents_object);
    REG_TYPE("java.lang.Void", "Void", false, parents_object);
    REG_TYPE("java.lang.Iterable", "Iterable", true, parents_iterable);
    REG_TYPE("java.lang.Comparable", "Comparable", true, no_parents);
    REG_TYPE("java.lang.Cloneable", "Cloneable", true, no_parents);
    REG_TYPE("java.lang.Runnable", "Runnable", true, no_parents);
    REG_TYPE("java.lang.AutoCloseable", "AutoCloseable", true, no_parents);
    REG_TYPE("java.lang.Math", "Math", false, parents_object);
    REG_TYPE("java.lang.System", "System", false, parents_object);
    REG_TYPE("java.lang.Thread", "Thread", false, parents_object);
    REG_TYPE("java.lang.Process", "Process", false, parents_object);
    REG_TYPE("java.lang.ProcessBuilder", "ProcessBuilder", false, parents_object);
    REG_TYPE("java.lang.StackTraceElement", "StackTraceElement", false, parents_object);
    REG_TYPE("java.lang.Enum", "Enum", false, parents_object);
    REG_TYPE("java.lang.Record", "Record", false, parents_object);
    REG_TYPE("java.lang.Throwable", "Throwable", false, parents_throwable);
    REG_TYPE("java.lang.Exception", "Exception", false, parents_exception);
    REG_TYPE("java.lang.Error", "Error", false, parents_error);
    REG_TYPE("java.lang.RuntimeException", "RuntimeException", false, parents_runtime_exc);
    REG_TYPE("java.lang.NullPointerException", "NullPointerException", false,
             parents_runtime_exc_chain);
    REG_TYPE("java.lang.IllegalArgumentException", "IllegalArgumentException", false,
             parents_runtime_exc_chain);
    REG_TYPE("java.lang.IllegalStateException", "IllegalStateException", false,
             parents_runtime_exc_chain);
    REG_TYPE("java.lang.IndexOutOfBoundsException", "IndexOutOfBoundsException", false,
             parents_runtime_exc_chain);
    REG_TYPE("java.lang.ArrayIndexOutOfBoundsException", "ArrayIndexOutOfBoundsException", false,
             parents_runtime_exc_chain);
    REG_TYPE("java.lang.ArithmeticException", "ArithmeticException", false,
             parents_runtime_exc_chain);
    REG_TYPE("java.lang.ClassCastException", "ClassCastException", false,
             parents_runtime_exc_chain);
    REG_TYPE("java.lang.ClassNotFoundException", "ClassNotFoundException", false,
             parents_exception);
    REG_TYPE("java.lang.NumberFormatException", "NumberFormatException", false,
             parents_runtime_exc_chain);
    REG_TYPE("java.lang.UnsupportedOperationException", "UnsupportedOperationException", false,
             parents_runtime_exc_chain);
    REG_TYPE("java.lang.InterruptedException", "InterruptedException", false, parents_exception);
    REG_TYPE("java.lang.SecurityException", "SecurityException", false,
             parents_runtime_exc_chain);
    REG_TYPE("java.lang.NoSuchMethodException", "NoSuchMethodException", false, parents_exception);
    REG_TYPE("java.lang.NoSuchFieldException", "NoSuchFieldException", false, parents_exception);

    /* Annotation-marker types. */
    REG_TYPE("java.lang.Override", "Override", true, no_parents);
    REG_TYPE("java.lang.Deprecated", "Deprecated", true, no_parents);
    REG_TYPE("java.lang.SuppressWarnings", "SuppressWarnings", true, no_parents);
    REG_TYPE("java.lang.FunctionalInterface", "FunctionalInterface", true, no_parents);

    /* ── Object methods ───────────────────────────────────────── */
    REG_METHOD("java.lang.Object", "toString", cbm_type_named(arena, "java.lang.String"));
    REG_METHOD("java.lang.Object", "hashCode", cbm_type_builtin(arena, "int"));
    REG_METHOD("java.lang.Object", "equals", cbm_type_builtin(arena, "boolean"));
    REG_METHOD("java.lang.Object", "getClass", cbm_type_named(arena, "java.lang.Class"));
    REG_METHOD("java.lang.Object", "wait", cbm_type_builtin(arena, "void"));
    REG_METHOD("java.lang.Object", "notify", cbm_type_builtin(arena, "void"));
    REG_METHOD("java.lang.Object", "notifyAll", cbm_type_builtin(arena, "void"));
    REG_METHOD("java.lang.Object", "clone", cbm_type_named(arena, "java.lang.Object"));
    REG_METHOD("java.lang.Object", "finalize", cbm_type_builtin(arena, "void"));

    /* ── String methods ───────────────────────────────────────── */
    REG_METHOD("java.lang.String", "length", cbm_type_builtin(arena, "int"));
    REG_METHOD("java.lang.String", "isEmpty", cbm_type_builtin(arena, "boolean"));
    REG_METHOD("java.lang.String", "isBlank", cbm_type_builtin(arena, "boolean"));
    REG_METHOD("java.lang.String", "charAt", cbm_type_builtin(arena, "char"));
    REG_METHOD("java.lang.String", "codePointAt", cbm_type_builtin(arena, "int"));
    REG_METHOD("java.lang.String", "equals", cbm_type_builtin(arena, "boolean"));
    REG_METHOD("java.lang.String", "equalsIgnoreCase", cbm_type_builtin(arena, "boolean"));
    REG_METHOD("java.lang.String", "compareTo", cbm_type_builtin(arena, "int"));
    REG_METHOD("java.lang.String", "compareToIgnoreCase", cbm_type_builtin(arena, "int"));
    REG_METHOD("java.lang.String", "indexOf", cbm_type_builtin(arena, "int"));
    REG_METHOD("java.lang.String", "lastIndexOf", cbm_type_builtin(arena, "int"));
    REG_METHOD("java.lang.String", "contains", cbm_type_builtin(arena, "boolean"));
    REG_METHOD("java.lang.String", "startsWith", cbm_type_builtin(arena, "boolean"));
    REG_METHOD("java.lang.String", "endsWith", cbm_type_builtin(arena, "boolean"));
    REG_METHOD("java.lang.String", "matches", cbm_type_builtin(arena, "boolean"));
    REG_METHOD("java.lang.String", "concat", cbm_type_named(arena, "java.lang.String"));
    REG_METHOD("java.lang.String", "substring", cbm_type_named(arena, "java.lang.String"));
    REG_METHOD("java.lang.String", "trim", cbm_type_named(arena, "java.lang.String"));
    REG_METHOD("java.lang.String", "strip", cbm_type_named(arena, "java.lang.String"));
    REG_METHOD("java.lang.String", "stripLeading", cbm_type_named(arena, "java.lang.String"));
    REG_METHOD("java.lang.String", "stripTrailing", cbm_type_named(arena, "java.lang.String"));
    REG_METHOD("java.lang.String", "toLowerCase", cbm_type_named(arena, "java.lang.String"));
    REG_METHOD("java.lang.String", "toUpperCase", cbm_type_named(arena, "java.lang.String"));
    REG_METHOD("java.lang.String", "replace", cbm_type_named(arena, "java.lang.String"));
    REG_METHOD("java.lang.String", "replaceAll", cbm_type_named(arena, "java.lang.String"));
    REG_METHOD("java.lang.String", "replaceFirst", cbm_type_named(arena, "java.lang.String"));
    REG_METHOD("java.lang.String", "split",
               cbm_type_slice(arena, cbm_type_named(arena, "java.lang.String")));
    REG_METHOD("java.lang.String", "toCharArray", cbm_type_slice(arena, cbm_type_builtin(arena, "char")));
    REG_METHOD("java.lang.String", "getBytes", cbm_type_slice(arena, cbm_type_builtin(arena, "byte")));
    REG_METHOD("java.lang.String", "intern", cbm_type_named(arena, "java.lang.String"));
    REG_METHOD("java.lang.String", "format", cbm_type_named(arena, "java.lang.String"));
    REG_METHOD("java.lang.String", "valueOf", cbm_type_named(arena, "java.lang.String"));
    REG_METHOD("java.lang.String", "join", cbm_type_named(arena, "java.lang.String"));
    REG_METHOD("java.lang.String", "repeat", cbm_type_named(arena, "java.lang.String"));
    REG_METHOD("java.lang.String", "lines", cbm_type_named(arena, "java.util.stream.Stream"));
    REG_METHOD("java.lang.String", "chars", cbm_type_named(arena, "java.util.stream.IntStream"));
    REG_METHOD("java.lang.String", "codePoints",
               cbm_type_named(arena, "java.util.stream.IntStream"));
    REG_METHOD("java.lang.String", "hashCode", cbm_type_builtin(arena, "int"));
    REG_METHOD("java.lang.String", "toString", cbm_type_named(arena, "java.lang.String"));
    REG_METHOD("java.lang.String", "toCharArray", cbm_type_slice(arena, cbm_type_builtin(arena, "char")));
    REG_CTOR("java.lang.String", "String");

    /* ── StringBuilder / StringBuffer ─────────────────────────── */
    REG_METHOD("java.lang.StringBuilder", "append",
               cbm_type_named(arena, "java.lang.StringBuilder"));
    REG_METHOD("java.lang.StringBuilder", "insert",
               cbm_type_named(arena, "java.lang.StringBuilder"));
    REG_METHOD("java.lang.StringBuilder", "delete",
               cbm_type_named(arena, "java.lang.StringBuilder"));
    REG_METHOD("java.lang.StringBuilder", "deleteCharAt",
               cbm_type_named(arena, "java.lang.StringBuilder"));
    REG_METHOD("java.lang.StringBuilder", "replace",
               cbm_type_named(arena, "java.lang.StringBuilder"));
    REG_METHOD("java.lang.StringBuilder", "reverse",
               cbm_type_named(arena, "java.lang.StringBuilder"));
    REG_METHOD("java.lang.StringBuilder", "toString",
               cbm_type_named(arena, "java.lang.String"));
    REG_METHOD("java.lang.StringBuilder", "length", cbm_type_builtin(arena, "int"));
    REG_METHOD("java.lang.StringBuilder", "charAt", cbm_type_builtin(arena, "char"));
    REG_METHOD("java.lang.StringBuilder", "setLength", cbm_type_builtin(arena, "void"));
    REG_METHOD("java.lang.StringBuilder", "indexOf", cbm_type_builtin(arena, "int"));
    REG_METHOD("java.lang.StringBuilder", "substring",
               cbm_type_named(arena, "java.lang.String"));
    REG_CTOR("java.lang.StringBuilder", "StringBuilder");

    REG_METHOD("java.lang.StringBuffer", "append",
               cbm_type_named(arena, "java.lang.StringBuffer"));
    REG_METHOD("java.lang.StringBuffer", "toString",
               cbm_type_named(arena, "java.lang.String"));
    REG_METHOD("java.lang.StringBuffer", "length", cbm_type_builtin(arena, "int"));
    REG_CTOR("java.lang.StringBuffer", "StringBuffer");

    /* ── CharSequence ─────────────────────────────────────────── */
    REG_METHOD("java.lang.CharSequence", "length", cbm_type_builtin(arena, "int"));
    REG_METHOD("java.lang.CharSequence", "charAt", cbm_type_builtin(arena, "char"));
    REG_METHOD("java.lang.CharSequence", "subSequence",
               cbm_type_named(arena, "java.lang.CharSequence"));
    REG_METHOD("java.lang.CharSequence", "toString",
               cbm_type_named(arena, "java.lang.String"));

    /* ── Number + boxed types ─────────────────────────────────── */
    REG_METHOD("java.lang.Number", "intValue", cbm_type_builtin(arena, "int"));
    REG_METHOD("java.lang.Number", "longValue", cbm_type_builtin(arena, "long"));
    REG_METHOD("java.lang.Number", "doubleValue", cbm_type_builtin(arena, "double"));
    REG_METHOD("java.lang.Number", "floatValue", cbm_type_builtin(arena, "float"));
    REG_METHOD("java.lang.Number", "shortValue", cbm_type_builtin(arena, "short"));
    REG_METHOD("java.lang.Number", "byteValue", cbm_type_builtin(arena, "byte"));

    REG_METHOD("java.lang.Integer", "intValue", cbm_type_builtin(arena, "int"));
    REG_METHOD("java.lang.Integer", "parseInt", cbm_type_builtin(arena, "int"));
    REG_METHOD("java.lang.Integer", "valueOf", cbm_type_named(arena, "java.lang.Integer"));
    REG_METHOD("java.lang.Integer", "toString", cbm_type_named(arena, "java.lang.String"));
    REG_METHOD("java.lang.Integer", "compare", cbm_type_builtin(arena, "int"));
    REG_METHOD("java.lang.Integer", "compareTo", cbm_type_builtin(arena, "int"));
    REG_METHOD("java.lang.Integer", "equals", cbm_type_builtin(arena, "boolean"));
    REG_METHOD("java.lang.Integer", "hashCode", cbm_type_builtin(arena, "int"));
    REG_METHOD("java.lang.Integer", "max", cbm_type_builtin(arena, "int"));
    REG_METHOD("java.lang.Integer", "min", cbm_type_builtin(arena, "int"));
    REG_METHOD("java.lang.Integer", "sum", cbm_type_builtin(arena, "int"));
    REG_METHOD("java.lang.Integer", "bitCount", cbm_type_builtin(arena, "int"));
    REG_METHOD("java.lang.Integer", "toBinaryString", cbm_type_named(arena, "java.lang.String"));
    REG_METHOD("java.lang.Integer", "toHexString", cbm_type_named(arena, "java.lang.String"));
    REG_METHOD("java.lang.Integer", "toOctalString", cbm_type_named(arena, "java.lang.String"));

    REG_METHOD("java.lang.Long", "longValue", cbm_type_builtin(arena, "long"));
    REG_METHOD("java.lang.Long", "parseLong", cbm_type_builtin(arena, "long"));
    REG_METHOD("java.lang.Long", "valueOf", cbm_type_named(arena, "java.lang.Long"));
    REG_METHOD("java.lang.Long", "toString", cbm_type_named(arena, "java.lang.String"));
    REG_METHOD("java.lang.Long", "compareTo", cbm_type_builtin(arena, "int"));

    REG_METHOD("java.lang.Double", "doubleValue", cbm_type_builtin(arena, "double"));
    REG_METHOD("java.lang.Double", "parseDouble", cbm_type_builtin(arena, "double"));
    REG_METHOD("java.lang.Double", "valueOf", cbm_type_named(arena, "java.lang.Double"));
    REG_METHOD("java.lang.Double", "toString", cbm_type_named(arena, "java.lang.String"));
    REG_METHOD("java.lang.Double", "isNaN", cbm_type_builtin(arena, "boolean"));
    REG_METHOD("java.lang.Double", "isInfinite", cbm_type_builtin(arena, "boolean"));

    REG_METHOD("java.lang.Float", "floatValue", cbm_type_builtin(arena, "float"));
    REG_METHOD("java.lang.Float", "parseFloat", cbm_type_builtin(arena, "float"));
    REG_METHOD("java.lang.Float", "valueOf", cbm_type_named(arena, "java.lang.Float"));

    REG_METHOD("java.lang.Boolean", "booleanValue", cbm_type_builtin(arena, "boolean"));
    REG_METHOD("java.lang.Boolean", "parseBoolean", cbm_type_builtin(arena, "boolean"));
    REG_METHOD("java.lang.Boolean", "valueOf", cbm_type_named(arena, "java.lang.Boolean"));
    REG_METHOD("java.lang.Boolean", "toString", cbm_type_named(arena, "java.lang.String"));

    REG_METHOD("java.lang.Character", "charValue", cbm_type_builtin(arena, "char"));
    REG_METHOD("java.lang.Character", "isDigit", cbm_type_builtin(arena, "boolean"));
    REG_METHOD("java.lang.Character", "isLetter", cbm_type_builtin(arena, "boolean"));
    REG_METHOD("java.lang.Character", "isLetterOrDigit", cbm_type_builtin(arena, "boolean"));
    REG_METHOD("java.lang.Character", "isWhitespace", cbm_type_builtin(arena, "boolean"));
    REG_METHOD("java.lang.Character", "isUpperCase", cbm_type_builtin(arena, "boolean"));
    REG_METHOD("java.lang.Character", "isLowerCase", cbm_type_builtin(arena, "boolean"));
    REG_METHOD("java.lang.Character", "toUpperCase", cbm_type_builtin(arena, "char"));
    REG_METHOD("java.lang.Character", "toLowerCase", cbm_type_builtin(arena, "char"));
    REG_METHOD("java.lang.Character", "getNumericValue", cbm_type_builtin(arena, "int"));
    REG_METHOD("java.lang.Character", "valueOf", cbm_type_named(arena, "java.lang.Character"));

    REG_METHOD("java.lang.Byte", "byteValue", cbm_type_builtin(arena, "byte"));
    REG_METHOD("java.lang.Byte", "parseByte", cbm_type_builtin(arena, "byte"));
    REG_METHOD("java.lang.Byte", "valueOf", cbm_type_named(arena, "java.lang.Byte"));

    REG_METHOD("java.lang.Short", "shortValue", cbm_type_builtin(arena, "short"));
    REG_METHOD("java.lang.Short", "parseShort", cbm_type_builtin(arena, "short"));
    REG_METHOD("java.lang.Short", "valueOf", cbm_type_named(arena, "java.lang.Short"));

    /* ── Math ─────────────────────────────────────────────────── */
    REG_METHOD("java.lang.Math", "abs", cbm_type_builtin(arena, "double"));
    REG_METHOD("java.lang.Math", "min", cbm_type_builtin(arena, "double"));
    REG_METHOD("java.lang.Math", "max", cbm_type_builtin(arena, "double"));
    REG_METHOD("java.lang.Math", "sqrt", cbm_type_builtin(arena, "double"));
    REG_METHOD("java.lang.Math", "cbrt", cbm_type_builtin(arena, "double"));
    REG_METHOD("java.lang.Math", "pow", cbm_type_builtin(arena, "double"));
    REG_METHOD("java.lang.Math", "exp", cbm_type_builtin(arena, "double"));
    REG_METHOD("java.lang.Math", "log", cbm_type_builtin(arena, "double"));
    REG_METHOD("java.lang.Math", "log10", cbm_type_builtin(arena, "double"));
    REG_METHOD("java.lang.Math", "sin", cbm_type_builtin(arena, "double"));
    REG_METHOD("java.lang.Math", "cos", cbm_type_builtin(arena, "double"));
    REG_METHOD("java.lang.Math", "tan", cbm_type_builtin(arena, "double"));
    REG_METHOD("java.lang.Math", "asin", cbm_type_builtin(arena, "double"));
    REG_METHOD("java.lang.Math", "acos", cbm_type_builtin(arena, "double"));
    REG_METHOD("java.lang.Math", "atan", cbm_type_builtin(arena, "double"));
    REG_METHOD("java.lang.Math", "atan2", cbm_type_builtin(arena, "double"));
    REG_METHOD("java.lang.Math", "floor", cbm_type_builtin(arena, "double"));
    REG_METHOD("java.lang.Math", "ceil", cbm_type_builtin(arena, "double"));
    REG_METHOD("java.lang.Math", "round", cbm_type_builtin(arena, "long"));
    REG_METHOD("java.lang.Math", "random", cbm_type_builtin(arena, "double"));
    REG_METHOD("java.lang.Math", "signum", cbm_type_builtin(arena, "double"));
    REG_METHOD("java.lang.Math", "hypot", cbm_type_builtin(arena, "double"));
    REG_METHOD("java.lang.Math", "floorDiv", cbm_type_builtin(arena, "long"));
    REG_METHOD("java.lang.Math", "floorMod", cbm_type_builtin(arena, "long"));
    REG_METHOD("java.lang.Math", "addExact", cbm_type_builtin(arena, "long"));
    REG_METHOD("java.lang.Math", "subtractExact", cbm_type_builtin(arena, "long"));
    REG_METHOD("java.lang.Math", "multiplyExact", cbm_type_builtin(arena, "long"));
    REG_METHOD("java.lang.Math", "toRadians", cbm_type_builtin(arena, "double"));
    REG_METHOD("java.lang.Math", "toDegrees", cbm_type_builtin(arena, "double"));

    /* ── System ───────────────────────────────────────────────── */
    REG_METHOD("java.lang.System", "currentTimeMillis", cbm_type_builtin(arena, "long"));
    REG_METHOD("java.lang.System", "nanoTime", cbm_type_builtin(arena, "long"));
    REG_METHOD("java.lang.System", "exit", cbm_type_builtin(arena, "void"));
    REG_METHOD("java.lang.System", "getenv", cbm_type_named(arena, "java.lang.String"));
    REG_METHOD("java.lang.System", "getProperty", cbm_type_named(arena, "java.lang.String"));
    REG_METHOD("java.lang.System", "setProperty", cbm_type_named(arena, "java.lang.String"));
    REG_METHOD("java.lang.System", "lineSeparator", cbm_type_named(arena, "java.lang.String"));
    REG_METHOD("java.lang.System", "arraycopy", cbm_type_builtin(arena, "void"));
    REG_METHOD("java.lang.System", "identityHashCode", cbm_type_builtin(arena, "int"));
    REG_METHOD("java.lang.System", "gc", cbm_type_builtin(arena, "void"));

    /* ── Thread ───────────────────────────────────────────────── */
    REG_METHOD("java.lang.Thread", "start", cbm_type_builtin(arena, "void"));
    REG_METHOD("java.lang.Thread", "run", cbm_type_builtin(arena, "void"));
    REG_METHOD("java.lang.Thread", "join", cbm_type_builtin(arena, "void"));
    REG_METHOD("java.lang.Thread", "interrupt", cbm_type_builtin(arena, "void"));
    REG_METHOD("java.lang.Thread", "isAlive", cbm_type_builtin(arena, "boolean"));
    REG_METHOD("java.lang.Thread", "sleep", cbm_type_builtin(arena, "void"));
    REG_METHOD("java.lang.Thread", "currentThread", cbm_type_named(arena, "java.lang.Thread"));
    REG_METHOD("java.lang.Thread", "yield", cbm_type_builtin(arena, "void"));
    REG_METHOD("java.lang.Thread", "getName", cbm_type_named(arena, "java.lang.String"));
    REG_METHOD("java.lang.Thread", "setName", cbm_type_builtin(arena, "void"));
    REG_METHOD("java.lang.Thread", "getId", cbm_type_builtin(arena, "long"));
    REG_METHOD("java.lang.Thread", "isInterrupted", cbm_type_builtin(arena, "boolean"));

    /* ── Class ────────────────────────────────────────────────── */
    REG_METHOD("java.lang.Class", "getName", cbm_type_named(arena, "java.lang.String"));
    REG_METHOD("java.lang.Class", "getSimpleName", cbm_type_named(arena, "java.lang.String"));
    REG_METHOD("java.lang.Class", "getCanonicalName", cbm_type_named(arena, "java.lang.String"));
    REG_METHOD("java.lang.Class", "isInterface", cbm_type_builtin(arena, "boolean"));
    REG_METHOD("java.lang.Class", "isArray", cbm_type_builtin(arena, "boolean"));
    REG_METHOD("java.lang.Class", "isAssignableFrom", cbm_type_builtin(arena, "boolean"));
    REG_METHOD("java.lang.Class", "isInstance", cbm_type_builtin(arena, "boolean"));
    REG_METHOD("java.lang.Class", "newInstance", cbm_type_named(arena, "java.lang.Object"));
    REG_METHOD("java.lang.Class", "forName", cbm_type_named(arena, "java.lang.Class"));
    REG_METHOD("java.lang.Class", "getSuperclass", cbm_type_named(arena, "java.lang.Class"));
    REG_METHOD("java.lang.Class", "getInterfaces",
               cbm_type_slice(arena, cbm_type_named(arena, "java.lang.Class")));

    /* ── Iterable / Iterator ──────────────────────────────────── */
    REG_METHOD("java.lang.Iterable", "iterator", cbm_type_named(arena, "java.util.Iterator"));
    REG_METHOD("java.lang.Iterable", "forEach", cbm_type_builtin(arena, "void"));

    /* ── Throwable methods ────────────────────────────────────── */
    REG_METHOD("java.lang.Throwable", "getMessage", cbm_type_named(arena, "java.lang.String"));
    REG_METHOD("java.lang.Throwable", "getLocalizedMessage",
               cbm_type_named(arena, "java.lang.String"));
    REG_METHOD("java.lang.Throwable", "getCause", cbm_type_named(arena, "java.lang.Throwable"));
    REG_METHOD("java.lang.Throwable", "initCause",
               cbm_type_named(arena, "java.lang.Throwable"));
    REG_METHOD("java.lang.Throwable", "printStackTrace", cbm_type_builtin(arena, "void"));
    REG_METHOD("java.lang.Throwable", "toString", cbm_type_named(arena, "java.lang.String"));
    REG_METHOD("java.lang.Throwable", "getStackTrace",
               cbm_type_slice(arena, cbm_type_named(arena, "java.lang.StackTraceElement")));

    /* ── AutoCloseable ────────────────────────────────────────── */
    REG_METHOD("java.lang.AutoCloseable", "close", cbm_type_builtin(arena, "void"));

    /* ── Comparable ───────────────────────────────────────────── */
    REG_METHOD("java.lang.Comparable", "compareTo", cbm_type_builtin(arena, "int"));

    /* ── Runnable ─────────────────────────────────────────────── */
    REG_METHOD("java.lang.Runnable", "run", cbm_type_builtin(arena, "void"));

    /* ── java.util ────────────────────────────────────────────── */
    REG_TYPE("java.util.Collection", "Collection", true, parents_collection);
    REG_TYPE("java.util.List", "List", true, parents_list);
    REG_TYPE("java.util.Set", "Set", true, parents_set);
    REG_TYPE("java.util.Queue", "Queue", true, parents_queue);
    REG_TYPE("java.util.Deque", "Deque", true, parents_deque);
    REG_TYPE("java.util.Map", "Map", true, parents_map);
    REG_TYPE("java.util.Map.Entry", "Entry", true, parents_map_entry);
    REG_TYPE("java.util.Iterator", "Iterator", true, parents_iterator);
    REG_TYPE("java.util.ListIterator", "ListIterator", true, parents_iterator);
    REG_TYPE("java.util.Spliterator", "Spliterator", true, no_parents);
    REG_TYPE("java.util.Comparator", "Comparator", true, no_parents);

    REG_TYPE("java.util.ArrayList", "ArrayList", false, parents_arraylist);
    REG_TYPE("java.util.LinkedList", "LinkedList", false, parents_linkedlist);
    REG_TYPE("java.util.Vector", "Vector", false, parents_arraylist);
    REG_TYPE("java.util.Stack", "Stack", false, parents_arraylist);
    REG_TYPE("java.util.HashSet", "HashSet", false, parents_hashset);
    REG_TYPE("java.util.TreeSet", "TreeSet", false, parents_treeset);
    REG_TYPE("java.util.LinkedHashSet", "LinkedHashSet", false, parents_linkedhashset);
    REG_TYPE("java.util.HashMap", "HashMap", false, parents_hashmap);
    REG_TYPE("java.util.TreeMap", "TreeMap", false, parents_treemap);
    REG_TYPE("java.util.LinkedHashMap", "LinkedHashMap", false, parents_linkedhashmap);
    REG_TYPE("java.util.ArrayDeque", "ArrayDeque", false, parents_deque);
    REG_TYPE("java.util.PriorityQueue", "PriorityQueue", false, parents_queue);

    REG_TYPE("java.util.Optional", "Optional", false, parents_object);
    REG_TYPE("java.util.OptionalInt", "OptionalInt", false, parents_object);
    REG_TYPE("java.util.OptionalLong", "OptionalLong", false, parents_object);
    REG_TYPE("java.util.OptionalDouble", "OptionalDouble", false, parents_object);
    REG_TYPE("java.util.Date", "Date", false, parents_object);
    REG_TYPE("java.util.Calendar", "Calendar", false, parents_object);
    REG_TYPE("java.util.GregorianCalendar", "GregorianCalendar", false,
             parents_gregorian_calendar);
    REG_TYPE("java.util.TimeZone", "TimeZone", false, parents_object);
    REG_TYPE("java.util.Locale", "Locale", false, parents_object);
    REG_TYPE("java.util.UUID", "UUID", false, parents_object);
    REG_TYPE("java.util.Random", "Random", false, parents_object);
    REG_TYPE("java.util.Scanner", "Scanner", false, parents_object);
    REG_TYPE("java.util.Arrays", "Arrays", false, parents_object);
    REG_TYPE("java.util.Collections", "Collections", false, parents_object);
    REG_TYPE("java.util.Objects", "Objects", false, parents_object);
    REG_TYPE("java.util.Properties", "Properties", false, parents_hashmap);
    REG_TYPE("java.util.regex.Pattern", "Pattern", false, parents_object);
    REG_TYPE("java.util.regex.Matcher", "Matcher", false, parents_object);

    /* ── Collection methods ───────────────────────────────────── */
    REG_METHOD("java.util.Collection", "size", cbm_type_builtin(arena, "int"));
    REG_METHOD("java.util.Collection", "isEmpty", cbm_type_builtin(arena, "boolean"));
    REG_METHOD("java.util.Collection", "contains", cbm_type_builtin(arena, "boolean"));
    REG_METHOD("java.util.Collection", "containsAll", cbm_type_builtin(arena, "boolean"));
    REG_METHOD("java.util.Collection", "iterator", cbm_type_named(arena, "java.util.Iterator"));
    REG_METHOD("java.util.Collection", "toArray",
               cbm_type_slice(arena, cbm_type_named(arena, "java.lang.Object")));
    REG_METHOD("java.util.Collection", "add", cbm_type_builtin(arena, "boolean"));
    REG_METHOD("java.util.Collection", "addAll", cbm_type_builtin(arena, "boolean"));
    REG_METHOD("java.util.Collection", "remove", cbm_type_builtin(arena, "boolean"));
    REG_METHOD("java.util.Collection", "removeAll", cbm_type_builtin(arena, "boolean"));
    REG_METHOD("java.util.Collection", "retainAll", cbm_type_builtin(arena, "boolean"));
    REG_METHOD("java.util.Collection", "clear", cbm_type_builtin(arena, "void"));
    REG_METHOD("java.util.Collection", "stream",
               cbm_type_named(arena, "java.util.stream.Stream"));
    REG_METHOD("java.util.Collection", "parallelStream",
               cbm_type_named(arena, "java.util.stream.Stream"));
    REG_METHOD("java.util.Collection", "forEach", cbm_type_builtin(arena, "void"));
    REG_METHOD("java.util.Collection", "removeIf", cbm_type_builtin(arena, "boolean"));

    /* ── List methods ─────────────────────────────────────────── */
    REG_METHOD("java.util.List", "get", cbm_type_named(arena, "java.lang.Object"));
    REG_METHOD("java.util.List", "set", cbm_type_named(arena, "java.lang.Object"));
    REG_METHOD("java.util.List", "add", cbm_type_builtin(arena, "boolean"));
    REG_METHOD("java.util.List", "remove", cbm_type_named(arena, "java.lang.Object"));
    REG_METHOD("java.util.List", "indexOf", cbm_type_builtin(arena, "int"));
    REG_METHOD("java.util.List", "lastIndexOf", cbm_type_builtin(arena, "int"));
    REG_METHOD("java.util.List", "subList", cbm_type_named(arena, "java.util.List"));
    REG_METHOD("java.util.List", "of", cbm_type_named(arena, "java.util.List"));
    REG_METHOD("java.util.List", "copyOf", cbm_type_named(arena, "java.util.List"));
    REG_METHOD("java.util.List", "size", cbm_type_builtin(arena, "int"));
    REG_METHOD("java.util.List", "isEmpty", cbm_type_builtin(arena, "boolean"));
    REG_METHOD("java.util.List", "contains", cbm_type_builtin(arena, "boolean"));
    REG_METHOD("java.util.List", "iterator", cbm_type_named(arena, "java.util.Iterator"));
    REG_METHOD("java.util.List", "stream",
               cbm_type_named(arena, "java.util.stream.Stream"));
    REG_METHOD("java.util.List", "forEach", cbm_type_builtin(arena, "void"));

    /* ── ArrayList ────────────────────────────────────────────── */
    REG_METHOD("java.util.ArrayList", "get", cbm_type_named(arena, "java.lang.Object"));
    REG_METHOD("java.util.ArrayList", "set", cbm_type_named(arena, "java.lang.Object"));
    REG_METHOD("java.util.ArrayList", "add", cbm_type_builtin(arena, "boolean"));
    REG_METHOD("java.util.ArrayList", "remove", cbm_type_named(arena, "java.lang.Object"));
    REG_METHOD("java.util.ArrayList", "size", cbm_type_builtin(arena, "int"));
    REG_METHOD("java.util.ArrayList", "isEmpty", cbm_type_builtin(arena, "boolean"));
    REG_METHOD("java.util.ArrayList", "indexOf", cbm_type_builtin(arena, "int"));
    REG_METHOD("java.util.ArrayList", "iterator", cbm_type_named(arena, "java.util.Iterator"));
    REG_METHOD("java.util.ArrayList", "clear", cbm_type_builtin(arena, "void"));
    REG_METHOD("java.util.ArrayList", "stream",
               cbm_type_named(arena, "java.util.stream.Stream"));
    REG_METHOD("java.util.ArrayList", "toArray",
               cbm_type_slice(arena, cbm_type_named(arena, "java.lang.Object")));
    REG_METHOD("java.util.ArrayList", "subList", cbm_type_named(arena, "java.util.List"));
    REG_METHOD("java.util.ArrayList", "trimToSize", cbm_type_builtin(arena, "void"));
    REG_METHOD("java.util.ArrayList", "ensureCapacity", cbm_type_builtin(arena, "void"));
    REG_METHOD("java.util.ArrayList", "forEach", cbm_type_builtin(arena, "void"));
    REG_METHOD("java.util.ArrayList", "removeIf", cbm_type_builtin(arena, "boolean"));
    REG_METHOD("java.util.List", "removeIf", cbm_type_builtin(arena, "boolean"));
    REG_CTOR("java.util.ArrayList", "ArrayList");

    REG_METHOD("java.util.LinkedList", "addFirst", cbm_type_builtin(arena, "void"));
    REG_METHOD("java.util.LinkedList", "addLast", cbm_type_builtin(arena, "void"));
    REG_METHOD("java.util.LinkedList", "removeFirst", cbm_type_named(arena, "java.lang.Object"));
    REG_METHOD("java.util.LinkedList", "removeLast", cbm_type_named(arena, "java.lang.Object"));
    REG_METHOD("java.util.LinkedList", "getFirst", cbm_type_named(arena, "java.lang.Object"));
    REG_METHOD("java.util.LinkedList", "getLast", cbm_type_named(arena, "java.lang.Object"));
    REG_METHOD("java.util.LinkedList", "peek", cbm_type_named(arena, "java.lang.Object"));
    REG_METHOD("java.util.LinkedList", "poll", cbm_type_named(arena, "java.lang.Object"));
    REG_METHOD("java.util.LinkedList", "offer", cbm_type_builtin(arena, "boolean"));
    REG_METHOD("java.util.LinkedList", "size", cbm_type_builtin(arena, "int"));
    REG_METHOD("java.util.LinkedList", "iterator", cbm_type_named(arena, "java.util.Iterator"));
    REG_CTOR("java.util.LinkedList", "LinkedList");

    /* ── Set methods ──────────────────────────────────────────── */
    REG_METHOD("java.util.Set", "size", cbm_type_builtin(arena, "int"));
    REG_METHOD("java.util.Set", "isEmpty", cbm_type_builtin(arena, "boolean"));
    REG_METHOD("java.util.Set", "contains", cbm_type_builtin(arena, "boolean"));
    REG_METHOD("java.util.Set", "add", cbm_type_builtin(arena, "boolean"));
    REG_METHOD("java.util.Set", "remove", cbm_type_builtin(arena, "boolean"));
    REG_METHOD("java.util.Set", "iterator", cbm_type_named(arena, "java.util.Iterator"));
    REG_METHOD("java.util.Set", "of", cbm_type_named(arena, "java.util.Set"));
    REG_METHOD("java.util.Set", "copyOf", cbm_type_named(arena, "java.util.Set"));
    REG_METHOD("java.util.Set", "stream",
               cbm_type_named(arena, "java.util.stream.Stream"));
    REG_METHOD("java.util.Set", "forEach", cbm_type_builtin(arena, "void"));

    REG_METHOD("java.util.HashSet", "add", cbm_type_builtin(arena, "boolean"));
    REG_METHOD("java.util.HashSet", "remove", cbm_type_builtin(arena, "boolean"));
    REG_METHOD("java.util.HashSet", "contains", cbm_type_builtin(arena, "boolean"));
    REG_METHOD("java.util.HashSet", "size", cbm_type_builtin(arena, "int"));
    REG_METHOD("java.util.HashSet", "isEmpty", cbm_type_builtin(arena, "boolean"));
    REG_METHOD("java.util.HashSet", "iterator", cbm_type_named(arena, "java.util.Iterator"));
    REG_METHOD("java.util.HashSet", "clear", cbm_type_builtin(arena, "void"));
    REG_METHOD("java.util.HashSet", "stream",
               cbm_type_named(arena, "java.util.stream.Stream"));
    REG_CTOR("java.util.HashSet", "HashSet");

    REG_METHOD("java.util.TreeSet", "first", cbm_type_named(arena, "java.lang.Object"));
    REG_METHOD("java.util.TreeSet", "last", cbm_type_named(arena, "java.lang.Object"));
    REG_METHOD("java.util.TreeSet", "headSet", cbm_type_named(arena, "java.util.SortedSet"));
    REG_METHOD("java.util.TreeSet", "tailSet", cbm_type_named(arena, "java.util.SortedSet"));
    REG_CTOR("java.util.TreeSet", "TreeSet");

    REG_CTOR("java.util.LinkedHashSet", "LinkedHashSet");

    /* ── Map methods ──────────────────────────────────────────── */
    REG_METHOD("java.util.Map", "get", cbm_type_named(arena, "java.lang.Object"));
    REG_METHOD("java.util.Map", "put", cbm_type_named(arena, "java.lang.Object"));
    REG_METHOD("java.util.Map", "remove", cbm_type_named(arena, "java.lang.Object"));
    REG_METHOD("java.util.Map", "containsKey", cbm_type_builtin(arena, "boolean"));
    REG_METHOD("java.util.Map", "containsValue", cbm_type_builtin(arena, "boolean"));
    REG_METHOD("java.util.Map", "keySet", cbm_type_named(arena, "java.util.Set"));
    REG_METHOD("java.util.Map", "values", cbm_type_named(arena, "java.util.Collection"));
    REG_METHOD("java.util.Map", "entrySet", cbm_type_named(arena, "java.util.Set"));
    REG_METHOD("java.util.Map", "size", cbm_type_builtin(arena, "int"));
    REG_METHOD("java.util.Map", "isEmpty", cbm_type_builtin(arena, "boolean"));
    REG_METHOD("java.util.Map", "putAll", cbm_type_builtin(arena, "void"));
    REG_METHOD("java.util.Map", "clear", cbm_type_builtin(arena, "void"));
    REG_METHOD("java.util.Map", "getOrDefault", cbm_type_named(arena, "java.lang.Object"));
    REG_METHOD("java.util.Map", "putIfAbsent", cbm_type_named(arena, "java.lang.Object"));
    REG_METHOD("java.util.Map", "computeIfAbsent", cbm_type_named(arena, "java.lang.Object"));
    REG_METHOD("java.util.Map", "compute", cbm_type_named(arena, "java.lang.Object"));
    REG_METHOD("java.util.Map", "merge", cbm_type_named(arena, "java.lang.Object"));
    REG_METHOD("java.util.Map", "of", cbm_type_named(arena, "java.util.Map"));
    REG_METHOD("java.util.Map", "copyOf", cbm_type_named(arena, "java.util.Map"));
    REG_METHOD("java.util.Map", "ofEntries", cbm_type_named(arena, "java.util.Map"));
    REG_METHOD("java.util.Map", "entry", cbm_type_named(arena, "java.util.Map.Entry"));
    REG_METHOD("java.util.Map", "forEach", cbm_type_builtin(arena, "void"));

    REG_METHOD("java.util.Map.Entry", "getKey", cbm_type_named(arena, "java.lang.Object"));
    REG_METHOD("java.util.Map.Entry", "getValue", cbm_type_named(arena, "java.lang.Object"));
    REG_METHOD("java.util.Map.Entry", "setValue", cbm_type_named(arena, "java.lang.Object"));

    REG_METHOD("java.util.HashMap", "get", cbm_type_named(arena, "java.lang.Object"));
    REG_METHOD("java.util.HashMap", "put", cbm_type_named(arena, "java.lang.Object"));
    REG_METHOD("java.util.HashMap", "remove", cbm_type_named(arena, "java.lang.Object"));
    REG_METHOD("java.util.HashMap", "containsKey", cbm_type_builtin(arena, "boolean"));
    REG_METHOD("java.util.HashMap", "containsValue", cbm_type_builtin(arena, "boolean"));
    REG_METHOD("java.util.HashMap", "size", cbm_type_builtin(arena, "int"));
    REG_METHOD("java.util.HashMap", "isEmpty", cbm_type_builtin(arena, "boolean"));
    REG_METHOD("java.util.HashMap", "keySet", cbm_type_named(arena, "java.util.Set"));
    REG_METHOD("java.util.HashMap", "values", cbm_type_named(arena, "java.util.Collection"));
    REG_METHOD("java.util.HashMap", "entrySet", cbm_type_named(arena, "java.util.Set"));
    REG_METHOD("java.util.HashMap", "clear", cbm_type_builtin(arena, "void"));
    REG_METHOD("java.util.HashMap", "getOrDefault", cbm_type_named(arena, "java.lang.Object"));
    REG_METHOD("java.util.HashMap", "putIfAbsent", cbm_type_named(arena, "java.lang.Object"));
    REG_METHOD("java.util.HashMap", "forEach", cbm_type_builtin(arena, "void"));
    REG_CTOR("java.util.HashMap", "HashMap");

    REG_METHOD("java.util.TreeMap", "firstKey", cbm_type_named(arena, "java.lang.Object"));
    REG_METHOD("java.util.TreeMap", "lastKey", cbm_type_named(arena, "java.lang.Object"));
    REG_METHOD("java.util.TreeMap", "headMap", cbm_type_named(arena, "java.util.SortedMap"));
    REG_METHOD("java.util.TreeMap", "tailMap", cbm_type_named(arena, "java.util.SortedMap"));
    REG_CTOR("java.util.TreeMap", "TreeMap");

    REG_CTOR("java.util.LinkedHashMap", "LinkedHashMap");

    /* ── Iterator methods ─────────────────────────────────────── */
    REG_METHOD("java.util.Iterator", "hasNext", cbm_type_builtin(arena, "boolean"));
    REG_METHOD("java.util.Iterator", "next", cbm_type_named(arena, "java.lang.Object"));
    REG_METHOD("java.util.Iterator", "remove", cbm_type_builtin(arena, "void"));
    REG_METHOD("java.util.Iterator", "forEachRemaining", cbm_type_builtin(arena, "void"));

    /* ── Optional ─────────────────────────────────────────────── */
    REG_METHOD("java.util.Optional", "get", cbm_type_named(arena, "java.lang.Object"));
    REG_METHOD("java.util.Optional", "isPresent", cbm_type_builtin(arena, "boolean"));
    REG_METHOD("java.util.Optional", "isEmpty", cbm_type_builtin(arena, "boolean"));
    REG_METHOD("java.util.Optional", "orElse", cbm_type_named(arena, "java.lang.Object"));
    REG_METHOD("java.util.Optional", "orElseGet", cbm_type_named(arena, "java.lang.Object"));
    REG_METHOD("java.util.Optional", "orElseThrow", cbm_type_named(arena, "java.lang.Object"));
    REG_METHOD("java.util.Optional", "ifPresent", cbm_type_builtin(arena, "void"));
    REG_METHOD("java.util.Optional", "ifPresentOrElse", cbm_type_builtin(arena, "void"));
    REG_METHOD("java.util.Optional", "map", cbm_type_named(arena, "java.util.Optional"));
    REG_METHOD("java.util.Optional", "flatMap", cbm_type_named(arena, "java.util.Optional"));
    REG_METHOD("java.util.Optional", "filter", cbm_type_named(arena, "java.util.Optional"));
    REG_METHOD("java.util.Optional", "of", cbm_type_named(arena, "java.util.Optional"));
    REG_METHOD("java.util.Optional", "ofNullable", cbm_type_named(arena, "java.util.Optional"));
    REG_METHOD("java.util.Optional", "empty", cbm_type_named(arena, "java.util.Optional"));
    REG_METHOD("java.util.Optional", "stream",
               cbm_type_named(arena, "java.util.stream.Stream"));

    /* ── Arrays / Collections / Objects helpers ───────────────── */
    REG_METHOD("java.util.Arrays", "asList", cbm_type_named(arena, "java.util.List"));
    REG_METHOD("java.util.Arrays", "stream",
               cbm_type_named(arena, "java.util.stream.Stream"));
    REG_METHOD("java.util.Arrays", "sort", cbm_type_builtin(arena, "void"));
    REG_METHOD("java.util.Arrays", "binarySearch", cbm_type_builtin(arena, "int"));
    REG_METHOD("java.util.Arrays", "fill", cbm_type_builtin(arena, "void"));
    REG_METHOD("java.util.Arrays", "copyOf",
               cbm_type_slice(arena, cbm_type_named(arena, "java.lang.Object")));
    REG_METHOD("java.util.Arrays", "copyOfRange",
               cbm_type_slice(arena, cbm_type_named(arena, "java.lang.Object")));
    REG_METHOD("java.util.Arrays", "equals", cbm_type_builtin(arena, "boolean"));
    REG_METHOD("java.util.Arrays", "hashCode", cbm_type_builtin(arena, "int"));
    REG_METHOD("java.util.Arrays", "toString", cbm_type_named(arena, "java.lang.String"));
    REG_METHOD("java.util.Arrays", "deepEquals", cbm_type_builtin(arena, "boolean"));
    REG_METHOD("java.util.Arrays", "deepToString", cbm_type_named(arena, "java.lang.String"));
    REG_METHOD("java.util.Arrays", "deepHashCode", cbm_type_builtin(arena, "int"));

    REG_METHOD("java.util.Collections", "sort", cbm_type_builtin(arena, "void"));
    REG_METHOD("java.util.Collections", "reverse", cbm_type_builtin(arena, "void"));
    REG_METHOD("java.util.Collections", "shuffle", cbm_type_builtin(arena, "void"));
    REG_METHOD("java.util.Collections", "min", cbm_type_named(arena, "java.lang.Object"));
    REG_METHOD("java.util.Collections", "max", cbm_type_named(arena, "java.lang.Object"));
    REG_METHOD("java.util.Collections", "emptyList", cbm_type_named(arena, "java.util.List"));
    REG_METHOD("java.util.Collections", "emptySet", cbm_type_named(arena, "java.util.Set"));
    REG_METHOD("java.util.Collections", "emptyMap", cbm_type_named(arena, "java.util.Map"));
    REG_METHOD("java.util.Collections", "singletonList",
               cbm_type_named(arena, "java.util.List"));
    REG_METHOD("java.util.Collections", "singleton",
               cbm_type_named(arena, "java.util.Set"));
    REG_METHOD("java.util.Collections", "unmodifiableList",
               cbm_type_named(arena, "java.util.List"));
    REG_METHOD("java.util.Collections", "unmodifiableSet",
               cbm_type_named(arena, "java.util.Set"));
    REG_METHOD("java.util.Collections", "unmodifiableMap",
               cbm_type_named(arena, "java.util.Map"));
    REG_METHOD("java.util.Collections", "frequency", cbm_type_builtin(arena, "int"));
    REG_METHOD("java.util.Collections", "binarySearch", cbm_type_builtin(arena, "int"));

    REG_METHOD("java.util.Objects", "equals", cbm_type_builtin(arena, "boolean"));
    REG_METHOD("java.util.Objects", "hashCode", cbm_type_builtin(arena, "int"));
    REG_METHOD("java.util.Objects", "hash", cbm_type_builtin(arena, "int"));
    REG_METHOD("java.util.Objects", "toString", cbm_type_named(arena, "java.lang.String"));
    REG_METHOD("java.util.Objects", "isNull", cbm_type_builtin(arena, "boolean"));
    REG_METHOD("java.util.Objects", "nonNull", cbm_type_builtin(arena, "boolean"));
    REG_METHOD("java.util.Objects", "requireNonNull", cbm_type_named(arena, "java.lang.Object"));
    REG_METHOD("java.util.Objects", "requireNonNullElse",
               cbm_type_named(arena, "java.lang.Object"));

    /* ── UUID, Random, Scanner ────────────────────────────────── */
    REG_METHOD("java.util.UUID", "randomUUID", cbm_type_named(arena, "java.util.UUID"));
    REG_METHOD("java.util.UUID", "fromString", cbm_type_named(arena, "java.util.UUID"));
    REG_METHOD("java.util.UUID", "toString", cbm_type_named(arena, "java.lang.String"));
    REG_METHOD("java.util.UUID", "getMostSignificantBits", cbm_type_builtin(arena, "long"));
    REG_METHOD("java.util.UUID", "getLeastSignificantBits", cbm_type_builtin(arena, "long"));
    REG_CTOR("java.util.UUID", "UUID");

    REG_METHOD("java.util.Random", "nextInt", cbm_type_builtin(arena, "int"));
    REG_METHOD("java.util.Random", "nextLong", cbm_type_builtin(arena, "long"));
    REG_METHOD("java.util.Random", "nextDouble", cbm_type_builtin(arena, "double"));
    REG_METHOD("java.util.Random", "nextFloat", cbm_type_builtin(arena, "float"));
    REG_METHOD("java.util.Random", "nextBoolean", cbm_type_builtin(arena, "boolean"));
    REG_METHOD("java.util.Random", "nextGaussian", cbm_type_builtin(arena, "double"));
    REG_METHOD("java.util.Random", "setSeed", cbm_type_builtin(arena, "void"));
    REG_CTOR("java.util.Random", "Random");

    REG_METHOD("java.util.Scanner", "next", cbm_type_named(arena, "java.lang.String"));
    REG_METHOD("java.util.Scanner", "nextLine", cbm_type_named(arena, "java.lang.String"));
    REG_METHOD("java.util.Scanner", "nextInt", cbm_type_builtin(arena, "int"));
    REG_METHOD("java.util.Scanner", "nextLong", cbm_type_builtin(arena, "long"));
    REG_METHOD("java.util.Scanner", "nextDouble", cbm_type_builtin(arena, "double"));
    REG_METHOD("java.util.Scanner", "hasNext", cbm_type_builtin(arena, "boolean"));
    REG_METHOD("java.util.Scanner", "hasNextLine", cbm_type_builtin(arena, "boolean"));
    REG_METHOD("java.util.Scanner", "hasNextInt", cbm_type_builtin(arena, "boolean"));
    REG_METHOD("java.util.Scanner", "close", cbm_type_builtin(arena, "void"));
    REG_CTOR("java.util.Scanner", "Scanner");

    /* ── Locale / Date / Calendar / TimeZone ──────────────────── */
    REG_METHOD("java.util.Locale", "getLanguage", cbm_type_named(arena, "java.lang.String"));
    REG_METHOD("java.util.Locale", "getCountry", cbm_type_named(arena, "java.lang.String"));
    REG_METHOD("java.util.Locale", "toString", cbm_type_named(arena, "java.lang.String"));
    REG_METHOD("java.util.Locale", "getDefault", cbm_type_named(arena, "java.util.Locale"));
    REG_CTOR("java.util.Locale", "Locale");

    REG_METHOD("java.util.Date", "getTime", cbm_type_builtin(arena, "long"));
    REG_METHOD("java.util.Date", "setTime", cbm_type_builtin(arena, "void"));
    REG_METHOD("java.util.Date", "before", cbm_type_builtin(arena, "boolean"));
    REG_METHOD("java.util.Date", "after", cbm_type_builtin(arena, "boolean"));
    REG_METHOD("java.util.Date", "compareTo", cbm_type_builtin(arena, "int"));
    REG_METHOD("java.util.Date", "toString", cbm_type_named(arena, "java.lang.String"));
    REG_CTOR("java.util.Date", "Date");

    REG_METHOD("java.util.Calendar", "getInstance", cbm_type_named(arena, "java.util.Calendar"));
    REG_METHOD("java.util.Calendar", "getTime", cbm_type_named(arena, "java.util.Date"));
    REG_METHOD("java.util.Calendar", "set", cbm_type_builtin(arena, "void"));
    REG_METHOD("java.util.Calendar", "get", cbm_type_builtin(arena, "int"));
    REG_METHOD("java.util.Calendar", "add", cbm_type_builtin(arena, "void"));

    REG_METHOD("java.util.TimeZone", "getDefault", cbm_type_named(arena, "java.util.TimeZone"));
    REG_METHOD("java.util.TimeZone", "getID", cbm_type_named(arena, "java.lang.String"));
    REG_METHOD("java.util.TimeZone", "getTimeZone", cbm_type_named(arena, "java.util.TimeZone"));

    /* ── regex ────────────────────────────────────────────────── */
    REG_METHOD("java.util.regex.Pattern", "compile",
               cbm_type_named(arena, "java.util.regex.Pattern"));
    REG_METHOD("java.util.regex.Pattern", "matcher",
               cbm_type_named(arena, "java.util.regex.Matcher"));
    REG_METHOD("java.util.regex.Pattern", "matches", cbm_type_builtin(arena, "boolean"));
    REG_METHOD("java.util.regex.Pattern", "split",
               cbm_type_slice(arena, cbm_type_named(arena, "java.lang.String")));
    REG_METHOD("java.util.regex.Pattern", "pattern", cbm_type_named(arena, "java.lang.String"));

    REG_METHOD("java.util.regex.Matcher", "matches", cbm_type_builtin(arena, "boolean"));
    REG_METHOD("java.util.regex.Matcher", "find", cbm_type_builtin(arena, "boolean"));
    REG_METHOD("java.util.regex.Matcher", "group", cbm_type_named(arena, "java.lang.String"));
    REG_METHOD("java.util.regex.Matcher", "groupCount", cbm_type_builtin(arena, "int"));
    REG_METHOD("java.util.regex.Matcher", "start", cbm_type_builtin(arena, "int"));
    REG_METHOD("java.util.regex.Matcher", "end", cbm_type_builtin(arena, "int"));
    REG_METHOD("java.util.regex.Matcher", "replaceAll", cbm_type_named(arena, "java.lang.String"));
    REG_METHOD("java.util.regex.Matcher", "replaceFirst",
               cbm_type_named(arena, "java.lang.String"));

    /* ── java.io ──────────────────────────────────────────────── */
    REG_TYPE("java.io.InputStream", "InputStream", false, parents_inputstream);
    REG_TYPE("java.io.OutputStream", "OutputStream", false, parents_outputstream);
    REG_TYPE("java.io.Reader", "Reader", false, parents_reader);
    REG_TYPE("java.io.Writer", "Writer", false, parents_writer);
    REG_TYPE("java.io.BufferedReader", "BufferedReader", false, parents_buffered_reader);
    REG_TYPE("java.io.BufferedWriter", "BufferedWriter", false, parents_buffered_writer);
    REG_TYPE("java.io.PrintStream", "PrintStream", false, parents_print_stream);
    REG_TYPE("java.io.PrintWriter", "PrintWriter", false, parents_print_writer);
    REG_TYPE("java.io.FileInputStream", "FileInputStream", false, parents_file_input_stream);
    REG_TYPE("java.io.FileOutputStream", "FileOutputStream", false, parents_file_output_stream);
    REG_TYPE("java.io.FileReader", "FileReader", false, parents_file_reader);
    REG_TYPE("java.io.FileWriter", "FileWriter", false, parents_file_writer);
    REG_TYPE("java.io.File", "File", false, parents_object);
    REG_TYPE("java.io.IOException", "IOException", false, parents_io_exception);
    REG_TYPE("java.io.FileNotFoundException", "FileNotFoundException", false,
             parents_file_not_found_exc);
    REG_TYPE("java.io.UncheckedIOException", "UncheckedIOException", false,
             parents_runtime_exc_chain);
    REG_TYPE("java.io.Serializable", "Serializable", true, no_parents);
    REG_TYPE("java.io.Closeable", "Closeable", true,
             parents_closeable);
    REG_TYPE("java.io.Flushable", "Flushable", true, no_parents);

    REG_METHOD("java.io.PrintStream", "println", cbm_type_builtin(arena, "void"));
    REG_METHOD("java.io.PrintStream", "print", cbm_type_builtin(arena, "void"));
    REG_METHOD("java.io.PrintStream", "printf", cbm_type_named(arena, "java.io.PrintStream"));
    REG_METHOD("java.io.PrintStream", "format", cbm_type_named(arena, "java.io.PrintStream"));
    REG_METHOD("java.io.PrintStream", "write", cbm_type_builtin(arena, "void"));
    REG_METHOD("java.io.PrintStream", "flush", cbm_type_builtin(arena, "void"));
    REG_METHOD("java.io.PrintStream", "close", cbm_type_builtin(arena, "void"));

    REG_METHOD("java.io.PrintWriter", "println", cbm_type_builtin(arena, "void"));
    REG_METHOD("java.io.PrintWriter", "print", cbm_type_builtin(arena, "void"));
    REG_METHOD("java.io.PrintWriter", "printf", cbm_type_named(arena, "java.io.PrintWriter"));
    REG_METHOD("java.io.PrintWriter", "flush", cbm_type_builtin(arena, "void"));
    REG_METHOD("java.io.PrintWriter", "close", cbm_type_builtin(arena, "void"));
    REG_CTOR("java.io.PrintWriter", "PrintWriter");

    REG_METHOD("java.io.InputStream", "read", cbm_type_builtin(arena, "int"));
    REG_METHOD("java.io.InputStream", "close", cbm_type_builtin(arena, "void"));
    REG_METHOD("java.io.InputStream", "available", cbm_type_builtin(arena, "int"));
    REG_METHOD("java.io.InputStream", "skip", cbm_type_builtin(arena, "long"));
    REG_METHOD("java.io.InputStream", "readAllBytes",
               cbm_type_slice(arena, cbm_type_builtin(arena, "byte")));

    REG_METHOD("java.io.OutputStream", "write", cbm_type_builtin(arena, "void"));
    REG_METHOD("java.io.OutputStream", "flush", cbm_type_builtin(arena, "void"));
    REG_METHOD("java.io.OutputStream", "close", cbm_type_builtin(arena, "void"));

    REG_METHOD("java.io.Reader", "read", cbm_type_builtin(arena, "int"));
    REG_METHOD("java.io.Reader", "close", cbm_type_builtin(arena, "void"));
    REG_METHOD("java.io.Reader", "ready", cbm_type_builtin(arena, "boolean"));

    REG_METHOD("java.io.Writer", "write", cbm_type_builtin(arena, "void"));
    REG_METHOD("java.io.Writer", "flush", cbm_type_builtin(arena, "void"));
    REG_METHOD("java.io.Writer", "close", cbm_type_builtin(arena, "void"));

    REG_METHOD("java.io.BufferedReader", "readLine", cbm_type_named(arena, "java.lang.String"));
    REG_METHOD("java.io.BufferedReader", "lines",
               cbm_type_named(arena, "java.util.stream.Stream"));
    REG_METHOD("java.io.BufferedReader", "close", cbm_type_builtin(arena, "void"));
    REG_CTOR("java.io.BufferedReader", "BufferedReader");

    REG_METHOD("java.io.BufferedWriter", "write", cbm_type_builtin(arena, "void"));
    REG_METHOD("java.io.BufferedWriter", "newLine", cbm_type_builtin(arena, "void"));
    REG_METHOD("java.io.BufferedWriter", "flush", cbm_type_builtin(arena, "void"));
    REG_METHOD("java.io.BufferedWriter", "close", cbm_type_builtin(arena, "void"));
    REG_CTOR("java.io.BufferedWriter", "BufferedWriter");

    REG_METHOD("java.io.File", "exists", cbm_type_builtin(arena, "boolean"));
    REG_METHOD("java.io.File", "isFile", cbm_type_builtin(arena, "boolean"));
    REG_METHOD("java.io.File", "isDirectory", cbm_type_builtin(arena, "boolean"));
    REG_METHOD("java.io.File", "canRead", cbm_type_builtin(arena, "boolean"));
    REG_METHOD("java.io.File", "canWrite", cbm_type_builtin(arena, "boolean"));
    REG_METHOD("java.io.File", "getName", cbm_type_named(arena, "java.lang.String"));
    REG_METHOD("java.io.File", "getPath", cbm_type_named(arena, "java.lang.String"));
    REG_METHOD("java.io.File", "getAbsolutePath", cbm_type_named(arena, "java.lang.String"));
    REG_METHOD("java.io.File", "getCanonicalPath", cbm_type_named(arena, "java.lang.String"));
    REG_METHOD("java.io.File", "getParent", cbm_type_named(arena, "java.lang.String"));
    REG_METHOD("java.io.File", "getParentFile", cbm_type_named(arena, "java.io.File"));
    REG_METHOD("java.io.File", "length", cbm_type_builtin(arena, "long"));
    REG_METHOD("java.io.File", "lastModified", cbm_type_builtin(arena, "long"));
    REG_METHOD("java.io.File", "mkdir", cbm_type_builtin(arena, "boolean"));
    REG_METHOD("java.io.File", "mkdirs", cbm_type_builtin(arena, "boolean"));
    REG_METHOD("java.io.File", "delete", cbm_type_builtin(arena, "boolean"));
    REG_METHOD("java.io.File", "renameTo", cbm_type_builtin(arena, "boolean"));
    REG_METHOD("java.io.File", "list",
               cbm_type_slice(arena, cbm_type_named(arena, "java.lang.String")));
    REG_METHOD("java.io.File", "listFiles",
               cbm_type_slice(arena, cbm_type_named(arena, "java.io.File")));
    REG_METHOD("java.io.File", "toPath", cbm_type_named(arena, "java.nio.file.Path"));
    REG_METHOD("java.io.File", "toURI", cbm_type_named(arena, "java.net.URI"));
    REG_CTOR("java.io.File", "File");

    /* ── java.nio.file ───────────────────────────────────────── */
    REG_TYPE("java.nio.file.Path", "Path", true, no_parents);
    REG_TYPE("java.nio.file.Paths", "Paths", false, parents_object);
    REG_TYPE("java.nio.file.Files", "Files", false, parents_object);

    REG_METHOD("java.nio.file.Path", "getFileName", cbm_type_named(arena, "java.nio.file.Path"));
    REG_METHOD("java.nio.file.Path", "getParent", cbm_type_named(arena, "java.nio.file.Path"));
    REG_METHOD("java.nio.file.Path", "getRoot", cbm_type_named(arena, "java.nio.file.Path"));
    REG_METHOD("java.nio.file.Path", "resolve", cbm_type_named(arena, "java.nio.file.Path"));
    REG_METHOD("java.nio.file.Path", "resolveSibling",
               cbm_type_named(arena, "java.nio.file.Path"));
    REG_METHOD("java.nio.file.Path", "relativize", cbm_type_named(arena, "java.nio.file.Path"));
    REG_METHOD("java.nio.file.Path", "normalize", cbm_type_named(arena, "java.nio.file.Path"));
    REG_METHOD("java.nio.file.Path", "toAbsolutePath",
               cbm_type_named(arena, "java.nio.file.Path"));
    REG_METHOD("java.nio.file.Path", "toString", cbm_type_named(arena, "java.lang.String"));
    REG_METHOD("java.nio.file.Path", "toFile", cbm_type_named(arena, "java.io.File"));
    REG_METHOD("java.nio.file.Path", "of", cbm_type_named(arena, "java.nio.file.Path"));

    REG_METHOD("java.nio.file.Paths", "get", cbm_type_named(arena, "java.nio.file.Path"));

    REG_METHOD("java.nio.file.Files", "exists", cbm_type_builtin(arena, "boolean"));
    REG_METHOD("java.nio.file.Files", "isDirectory", cbm_type_builtin(arena, "boolean"));
    REG_METHOD("java.nio.file.Files", "isRegularFile", cbm_type_builtin(arena, "boolean"));
    REG_METHOD("java.nio.file.Files", "readString", cbm_type_named(arena, "java.lang.String"));
    REG_METHOD("java.nio.file.Files", "writeString", cbm_type_named(arena, "java.nio.file.Path"));
    REG_METHOD("java.nio.file.Files", "readAllLines", cbm_type_named(arena, "java.util.List"));
    REG_METHOD("java.nio.file.Files", "readAllBytes",
               cbm_type_slice(arena, cbm_type_builtin(arena, "byte")));
    REG_METHOD("java.nio.file.Files", "lines",
               cbm_type_named(arena, "java.util.stream.Stream"));
    REG_METHOD("java.nio.file.Files", "list",
               cbm_type_named(arena, "java.util.stream.Stream"));
    REG_METHOD("java.nio.file.Files", "walk",
               cbm_type_named(arena, "java.util.stream.Stream"));
    REG_METHOD("java.nio.file.Files", "createDirectory",
               cbm_type_named(arena, "java.nio.file.Path"));
    REG_METHOD("java.nio.file.Files", "createDirectories",
               cbm_type_named(arena, "java.nio.file.Path"));
    REG_METHOD("java.nio.file.Files", "createFile",
               cbm_type_named(arena, "java.nio.file.Path"));
    REG_METHOD("java.nio.file.Files", "delete", cbm_type_builtin(arena, "void"));
    REG_METHOD("java.nio.file.Files", "deleteIfExists", cbm_type_builtin(arena, "boolean"));
    REG_METHOD("java.nio.file.Files", "copy", cbm_type_named(arena, "java.nio.file.Path"));
    REG_METHOD("java.nio.file.Files", "move", cbm_type_named(arena, "java.nio.file.Path"));
    REG_METHOD("java.nio.file.Files", "size", cbm_type_builtin(arena, "long"));

    /* ── java.util.function (the 21 functional interfaces) ──── */
    REG_TYPE("java.util.function.Function", "Function", true, no_parents);
    REG_TYPE("java.util.function.BiFunction", "BiFunction", true, no_parents);
    REG_TYPE("java.util.function.Predicate", "Predicate", true, no_parents);
    REG_TYPE("java.util.function.BiPredicate", "BiPredicate", true, no_parents);
    REG_TYPE("java.util.function.Consumer", "Consumer", true, no_parents);
    REG_TYPE("java.util.function.BiConsumer", "BiConsumer", true, no_parents);
    REG_TYPE("java.util.function.Supplier", "Supplier", true, no_parents);
    REG_TYPE("java.util.function.UnaryOperator", "UnaryOperator", true,
             parents_unary_operator);
    REG_TYPE("java.util.function.BinaryOperator", "BinaryOperator", true,
             parents_binary_operator);
    REG_TYPE("java.util.function.IntFunction", "IntFunction", true, no_parents);
    REG_TYPE("java.util.function.LongFunction", "LongFunction", true, no_parents);
    REG_TYPE("java.util.function.DoubleFunction", "DoubleFunction", true, no_parents);
    REG_TYPE("java.util.function.IntPredicate", "IntPredicate", true, no_parents);
    REG_TYPE("java.util.function.LongPredicate", "LongPredicate", true, no_parents);
    REG_TYPE("java.util.function.DoublePredicate", "DoublePredicate", true, no_parents);
    REG_TYPE("java.util.function.IntConsumer", "IntConsumer", true, no_parents);
    REG_TYPE("java.util.function.LongConsumer", "LongConsumer", true, no_parents);
    REG_TYPE("java.util.function.DoubleConsumer", "DoubleConsumer", true, no_parents);
    REG_TYPE("java.util.function.IntSupplier", "IntSupplier", true, no_parents);
    REG_TYPE("java.util.function.LongSupplier", "LongSupplier", true, no_parents);
    REG_TYPE("java.util.function.DoubleSupplier", "DoubleSupplier", true, no_parents);
    REG_TYPE("java.util.function.BooleanSupplier", "BooleanSupplier", true, no_parents);
    REG_TYPE("java.util.function.ToIntFunction", "ToIntFunction", true, no_parents);
    REG_TYPE("java.util.function.ToLongFunction", "ToLongFunction", true, no_parents);
    REG_TYPE("java.util.function.ToDoubleFunction", "ToDoubleFunction", true, no_parents);

    REG_METHOD("java.util.function.Function", "apply", cbm_type_named(arena, "java.lang.Object"));
    REG_METHOD("java.util.function.Function", "compose",
               cbm_type_named(arena, "java.util.function.Function"));
    REG_METHOD("java.util.function.Function", "andThen",
               cbm_type_named(arena, "java.util.function.Function"));
    REG_METHOD("java.util.function.Function", "identity",
               cbm_type_named(arena, "java.util.function.Function"));

    REG_METHOD("java.util.function.BiFunction", "apply",
               cbm_type_named(arena, "java.lang.Object"));
    REG_METHOD("java.util.function.BiFunction", "andThen",
               cbm_type_named(arena, "java.util.function.BiFunction"));

    REG_METHOD("java.util.function.Predicate", "test", cbm_type_builtin(arena, "boolean"));
    REG_METHOD("java.util.function.Predicate", "and",
               cbm_type_named(arena, "java.util.function.Predicate"));
    REG_METHOD("java.util.function.Predicate", "or",
               cbm_type_named(arena, "java.util.function.Predicate"));
    REG_METHOD("java.util.function.Predicate", "negate",
               cbm_type_named(arena, "java.util.function.Predicate"));
    REG_METHOD("java.util.function.Predicate", "isEqual",
               cbm_type_named(arena, "java.util.function.Predicate"));

    REG_METHOD("java.util.function.Consumer", "accept", cbm_type_builtin(arena, "void"));
    REG_METHOD("java.util.function.Consumer", "andThen",
               cbm_type_named(arena, "java.util.function.Consumer"));

    REG_METHOD("java.util.function.Supplier", "get", cbm_type_named(arena, "java.lang.Object"));

    REG_METHOD("java.util.function.UnaryOperator", "identity",
               cbm_type_named(arena, "java.util.function.UnaryOperator"));
    REG_METHOD("java.util.function.UnaryOperator", "apply",
               cbm_type_named(arena, "java.lang.Object"));

    /* ── java.util.stream ────────────────────────────────────── */
    REG_TYPE("java.util.stream.Stream", "Stream", true, no_parents);
    REG_TYPE("java.util.stream.IntStream", "IntStream", true, no_parents);
    REG_TYPE("java.util.stream.LongStream", "LongStream", true, no_parents);
    REG_TYPE("java.util.stream.DoubleStream", "DoubleStream", true, no_parents);
    REG_TYPE("java.util.stream.Collectors", "Collectors", false, parents_object);
    REG_TYPE("java.util.stream.Collector", "Collector", true, no_parents);

    REG_METHOD("java.util.stream.Stream", "filter",
               cbm_type_named(arena, "java.util.stream.Stream"));
    REG_METHOD("java.util.stream.Stream", "map",
               cbm_type_named(arena, "java.util.stream.Stream"));
    REG_METHOD("java.util.stream.Stream", "flatMap",
               cbm_type_named(arena, "java.util.stream.Stream"));
    REG_METHOD("java.util.stream.Stream", "mapToInt",
               cbm_type_named(arena, "java.util.stream.IntStream"));
    REG_METHOD("java.util.stream.Stream", "mapToLong",
               cbm_type_named(arena, "java.util.stream.LongStream"));
    REG_METHOD("java.util.stream.Stream", "mapToDouble",
               cbm_type_named(arena, "java.util.stream.DoubleStream"));
    REG_METHOD("java.util.stream.Stream", "sorted",
               cbm_type_named(arena, "java.util.stream.Stream"));
    REG_METHOD("java.util.stream.Stream", "distinct",
               cbm_type_named(arena, "java.util.stream.Stream"));
    REG_METHOD("java.util.stream.Stream", "limit",
               cbm_type_named(arena, "java.util.stream.Stream"));
    REG_METHOD("java.util.stream.Stream", "skip",
               cbm_type_named(arena, "java.util.stream.Stream"));
    REG_METHOD("java.util.stream.Stream", "peek",
               cbm_type_named(arena, "java.util.stream.Stream"));
    REG_METHOD("java.util.stream.Stream", "forEach", cbm_type_builtin(arena, "void"));
    REG_METHOD("java.util.stream.Stream", "forEachOrdered", cbm_type_builtin(arena, "void"));
    REG_METHOD("java.util.stream.Stream", "toArray",
               cbm_type_slice(arena, cbm_type_named(arena, "java.lang.Object")));
    REG_METHOD("java.util.stream.Stream", "toList", cbm_type_named(arena, "java.util.List"));
    REG_METHOD("java.util.stream.Stream", "reduce", cbm_type_named(arena, "java.lang.Object"));
    REG_METHOD("java.util.stream.Stream", "collect", cbm_type_named(arena, "java.lang.Object"));
    REG_METHOD("java.util.stream.Stream", "count", cbm_type_builtin(arena, "long"));
    REG_METHOD("java.util.stream.Stream", "anyMatch", cbm_type_builtin(arena, "boolean"));
    REG_METHOD("java.util.stream.Stream", "allMatch", cbm_type_builtin(arena, "boolean"));
    REG_METHOD("java.util.stream.Stream", "noneMatch", cbm_type_builtin(arena, "boolean"));
    REG_METHOD("java.util.stream.Stream", "findFirst",
               cbm_type_named(arena, "java.util.Optional"));
    REG_METHOD("java.util.stream.Stream", "findAny",
               cbm_type_named(arena, "java.util.Optional"));
    REG_METHOD("java.util.stream.Stream", "min", cbm_type_named(arena, "java.util.Optional"));
    REG_METHOD("java.util.stream.Stream", "max", cbm_type_named(arena, "java.util.Optional"));
    REG_METHOD("java.util.stream.Stream", "of",
               cbm_type_named(arena, "java.util.stream.Stream"));
    REG_METHOD("java.util.stream.Stream", "empty",
               cbm_type_named(arena, "java.util.stream.Stream"));
    REG_METHOD("java.util.stream.Stream", "concat",
               cbm_type_named(arena, "java.util.stream.Stream"));
    REG_METHOD("java.util.stream.Stream", "iterate",
               cbm_type_named(arena, "java.util.stream.Stream"));
    REG_METHOD("java.util.stream.Stream", "generate",
               cbm_type_named(arena, "java.util.stream.Stream"));

    REG_METHOD("java.util.stream.IntStream", "sum", cbm_type_builtin(arena, "int"));
    REG_METHOD("java.util.stream.IntStream", "average",
               cbm_type_named(arena, "java.util.OptionalDouble"));
    REG_METHOD("java.util.stream.IntStream", "max",
               cbm_type_named(arena, "java.util.OptionalInt"));
    REG_METHOD("java.util.stream.IntStream", "min",
               cbm_type_named(arena, "java.util.OptionalInt"));
    REG_METHOD("java.util.stream.IntStream", "count", cbm_type_builtin(arena, "long"));
    REG_METHOD("java.util.stream.IntStream", "boxed",
               cbm_type_named(arena, "java.util.stream.Stream"));
    REG_METHOD("java.util.stream.IntStream", "filter",
               cbm_type_named(arena, "java.util.stream.IntStream"));
    REG_METHOD("java.util.stream.IntStream", "map",
               cbm_type_named(arena, "java.util.stream.IntStream"));
    REG_METHOD("java.util.stream.IntStream", "range",
               cbm_type_named(arena, "java.util.stream.IntStream"));
    REG_METHOD("java.util.stream.IntStream", "rangeClosed",
               cbm_type_named(arena, "java.util.stream.IntStream"));
    REG_METHOD("java.util.stream.IntStream", "of",
               cbm_type_named(arena, "java.util.stream.IntStream"));

    REG_METHOD("java.util.stream.Collectors", "toList",
               cbm_type_named(arena, "java.util.stream.Collector"));
    REG_METHOD("java.util.stream.Collectors", "toSet",
               cbm_type_named(arena, "java.util.stream.Collector"));
    REG_METHOD("java.util.stream.Collectors", "toMap",
               cbm_type_named(arena, "java.util.stream.Collector"));
    REG_METHOD("java.util.stream.Collectors", "joining",
               cbm_type_named(arena, "java.util.stream.Collector"));
    REG_METHOD("java.util.stream.Collectors", "groupingBy",
               cbm_type_named(arena, "java.util.stream.Collector"));
    REG_METHOD("java.util.stream.Collectors", "partitioningBy",
               cbm_type_named(arena, "java.util.stream.Collector"));
    REG_METHOD("java.util.stream.Collectors", "counting",
               cbm_type_named(arena, "java.util.stream.Collector"));
    REG_METHOD("java.util.stream.Collectors", "summingInt",
               cbm_type_named(arena, "java.util.stream.Collector"));
    REG_METHOD("java.util.stream.Collectors", "averagingDouble",
               cbm_type_named(arena, "java.util.stream.Collector"));
    REG_METHOD("java.util.stream.Collectors", "mapping",
               cbm_type_named(arena, "java.util.stream.Collector"));
    REG_METHOD("java.util.stream.Collectors", "reducing",
               cbm_type_named(arena, "java.util.stream.Collector"));

    /* ── java.util.concurrent ────────────────────────────────── */
    REG_TYPE("java.util.concurrent.ExecutorService", "ExecutorService", true, no_parents);
    REG_TYPE("java.util.concurrent.Executors", "Executors", false, parents_object);
    REG_TYPE("java.util.concurrent.Future", "Future", true, no_parents);
    REG_TYPE("java.util.concurrent.CompletableFuture", "CompletableFuture", false,
             parents_completable_future);
    REG_TYPE("java.util.concurrent.ConcurrentHashMap", "ConcurrentHashMap", false,
             parents_concurrent_hashmap);
    REG_TYPE("java.util.concurrent.ConcurrentMap", "ConcurrentMap", true, parents_map);
    REG_TYPE("java.util.concurrent.TimeUnit", "TimeUnit", false, parents_object);
    REG_TYPE("java.util.concurrent.atomic.AtomicInteger", "AtomicInteger", false, parents_object);
    REG_TYPE("java.util.concurrent.atomic.AtomicLong", "AtomicLong", false, parents_object);
    REG_TYPE("java.util.concurrent.atomic.AtomicBoolean", "AtomicBoolean", false, parents_object);
    REG_TYPE("java.util.concurrent.atomic.AtomicReference", "AtomicReference", false,
             parents_object);
    REG_TYPE("java.util.concurrent.locks.Lock", "Lock", true, no_parents);
    REG_TYPE("java.util.concurrent.locks.ReentrantLock", "ReentrantLock", false,
             parents_reentrant_lock);
    REG_TYPE("java.util.concurrent.locks.ReadWriteLock", "ReadWriteLock", true, no_parents);

    REG_METHOD("java.util.concurrent.ExecutorService", "submit",
               cbm_type_named(arena, "java.util.concurrent.Future"));
    REG_METHOD("java.util.concurrent.ExecutorService", "execute", cbm_type_builtin(arena, "void"));
    REG_METHOD("java.util.concurrent.ExecutorService", "shutdown", cbm_type_builtin(arena, "void"));
    REG_METHOD("java.util.concurrent.ExecutorService", "shutdownNow",
               cbm_type_named(arena, "java.util.List"));
    REG_METHOD("java.util.concurrent.ExecutorService", "awaitTermination",
               cbm_type_builtin(arena, "boolean"));
    REG_METHOD("java.util.concurrent.ExecutorService", "isShutdown",
               cbm_type_builtin(arena, "boolean"));
    REG_METHOD("java.util.concurrent.ExecutorService", "isTerminated",
               cbm_type_builtin(arena, "boolean"));

    REG_METHOD("java.util.concurrent.Executors", "newFixedThreadPool",
               cbm_type_named(arena, "java.util.concurrent.ExecutorService"));
    REG_METHOD("java.util.concurrent.Executors", "newSingleThreadExecutor",
               cbm_type_named(arena, "java.util.concurrent.ExecutorService"));
    REG_METHOD("java.util.concurrent.Executors", "newCachedThreadPool",
               cbm_type_named(arena, "java.util.concurrent.ExecutorService"));
    REG_METHOD("java.util.concurrent.Executors", "newScheduledThreadPool",
               cbm_type_named(arena, "java.util.concurrent.ExecutorService"));

    REG_METHOD("java.util.concurrent.Future", "get", cbm_type_named(arena, "java.lang.Object"));
    REG_METHOD("java.util.concurrent.Future", "isDone", cbm_type_builtin(arena, "boolean"));
    REG_METHOD("java.util.concurrent.Future", "cancel", cbm_type_builtin(arena, "boolean"));

    REG_METHOD("java.util.concurrent.CompletableFuture", "thenApply",
               cbm_type_named(arena, "java.util.concurrent.CompletableFuture"));
    REG_METHOD("java.util.concurrent.CompletableFuture", "thenAccept",
               cbm_type_named(arena, "java.util.concurrent.CompletableFuture"));
    REG_METHOD("java.util.concurrent.CompletableFuture", "thenCompose",
               cbm_type_named(arena, "java.util.concurrent.CompletableFuture"));
    REG_METHOD("java.util.concurrent.CompletableFuture", "thenCombine",
               cbm_type_named(arena, "java.util.concurrent.CompletableFuture"));
    REG_METHOD("java.util.concurrent.CompletableFuture", "exceptionally",
               cbm_type_named(arena, "java.util.concurrent.CompletableFuture"));
    REG_METHOD("java.util.concurrent.CompletableFuture", "join",
               cbm_type_named(arena, "java.lang.Object"));
    REG_METHOD("java.util.concurrent.CompletableFuture", "supplyAsync",
               cbm_type_named(arena, "java.util.concurrent.CompletableFuture"));
    REG_METHOD("java.util.concurrent.CompletableFuture", "runAsync",
               cbm_type_named(arena, "java.util.concurrent.CompletableFuture"));
    REG_METHOD("java.util.concurrent.CompletableFuture", "completedFuture",
               cbm_type_named(arena, "java.util.concurrent.CompletableFuture"));
    REG_METHOD("java.util.concurrent.CompletableFuture", "allOf",
               cbm_type_named(arena, "java.util.concurrent.CompletableFuture"));
    REG_METHOD("java.util.concurrent.CompletableFuture", "anyOf",
               cbm_type_named(arena, "java.util.concurrent.CompletableFuture"));

    REG_METHOD("java.util.concurrent.atomic.AtomicInteger", "get", cbm_type_builtin(arena, "int"));
    REG_METHOD("java.util.concurrent.atomic.AtomicInteger", "set", cbm_type_builtin(arena, "void"));
    REG_METHOD("java.util.concurrent.atomic.AtomicInteger", "incrementAndGet",
               cbm_type_builtin(arena, "int"));
    REG_METHOD("java.util.concurrent.atomic.AtomicInteger", "decrementAndGet",
               cbm_type_builtin(arena, "int"));
    REG_METHOD("java.util.concurrent.atomic.AtomicInteger", "getAndIncrement",
               cbm_type_builtin(arena, "int"));
    REG_METHOD("java.util.concurrent.atomic.AtomicInteger", "compareAndSet",
               cbm_type_builtin(arena, "boolean"));
    REG_METHOD("java.util.concurrent.atomic.AtomicInteger", "addAndGet",
               cbm_type_builtin(arena, "int"));
    REG_CTOR("java.util.concurrent.atomic.AtomicInteger", "AtomicInteger");

    REG_METHOD("java.util.concurrent.atomic.AtomicLong", "get",
               cbm_type_builtin(arena, "long"));
    REG_METHOD("java.util.concurrent.atomic.AtomicLong", "incrementAndGet",
               cbm_type_builtin(arena, "long"));
    REG_CTOR("java.util.concurrent.atomic.AtomicLong", "AtomicLong");

    REG_METHOD("java.util.concurrent.atomic.AtomicReference", "get",
               cbm_type_named(arena, "java.lang.Object"));
    REG_METHOD("java.util.concurrent.atomic.AtomicReference", "set",
               cbm_type_builtin(arena, "void"));
    REG_METHOD("java.util.concurrent.atomic.AtomicReference", "compareAndSet",
               cbm_type_builtin(arena, "boolean"));
    REG_CTOR("java.util.concurrent.atomic.AtomicReference", "AtomicReference");

    REG_METHOD("java.util.concurrent.locks.ReentrantLock", "lock",
               cbm_type_builtin(arena, "void"));
    REG_METHOD("java.util.concurrent.locks.ReentrantLock", "unlock",
               cbm_type_builtin(arena, "void"));
    REG_METHOD("java.util.concurrent.locks.ReentrantLock", "tryLock",
               cbm_type_builtin(arena, "boolean"));
    REG_CTOR("java.util.concurrent.locks.ReentrantLock", "ReentrantLock");

    REG_METHOD("java.util.concurrent.ConcurrentHashMap", "put",
               cbm_type_named(arena, "java.lang.Object"));
    REG_METHOD("java.util.concurrent.ConcurrentHashMap", "get",
               cbm_type_named(arena, "java.lang.Object"));
    REG_METHOD("java.util.concurrent.ConcurrentHashMap", "putIfAbsent",
               cbm_type_named(arena, "java.lang.Object"));
    REG_CTOR("java.util.concurrent.ConcurrentHashMap", "ConcurrentHashMap");

    /* ── java.time ───────────────────────────────────────────── */
    REG_TYPE("java.time.LocalDate", "LocalDate", false, parents_object);
    REG_TYPE("java.time.LocalTime", "LocalTime", false, parents_object);
    REG_TYPE("java.time.LocalDateTime", "LocalDateTime", false, parents_object);
    REG_TYPE("java.time.ZonedDateTime", "ZonedDateTime", false, parents_object);
    REG_TYPE("java.time.OffsetDateTime", "OffsetDateTime", false, parents_object);
    REG_TYPE("java.time.Instant", "Instant", false, parents_object);
    REG_TYPE("java.time.Duration", "Duration", false, parents_object);
    REG_TYPE("java.time.Period", "Period", false, parents_object);
    REG_TYPE("java.time.ZoneId", "ZoneId", false, parents_object);
    REG_TYPE("java.time.format.DateTimeFormatter", "DateTimeFormatter", false, parents_object);

    REG_METHOD("java.time.LocalDate", "now", cbm_type_named(arena, "java.time.LocalDate"));
    REG_METHOD("java.time.LocalDate", "of", cbm_type_named(arena, "java.time.LocalDate"));
    REG_METHOD("java.time.LocalDate", "parse", cbm_type_named(arena, "java.time.LocalDate"));
    REG_METHOD("java.time.LocalDate", "plusDays", cbm_type_named(arena, "java.time.LocalDate"));
    REG_METHOD("java.time.LocalDate", "minusDays", cbm_type_named(arena, "java.time.LocalDate"));
    REG_METHOD("java.time.LocalDate", "getYear", cbm_type_builtin(arena, "int"));
    REG_METHOD("java.time.LocalDate", "getMonth", cbm_type_named(arena, "java.time.Month"));
    REG_METHOD("java.time.LocalDate", "getDayOfMonth", cbm_type_builtin(arena, "int"));
    REG_METHOD("java.time.LocalDate", "getDayOfWeek",
               cbm_type_named(arena, "java.time.DayOfWeek"));
    REG_METHOD("java.time.LocalDate", "isAfter", cbm_type_builtin(arena, "boolean"));
    REG_METHOD("java.time.LocalDate", "isBefore", cbm_type_builtin(arena, "boolean"));
    REG_METHOD("java.time.LocalDate", "format", cbm_type_named(arena, "java.lang.String"));
    REG_METHOD("java.time.LocalDate", "toString", cbm_type_named(arena, "java.lang.String"));

    REG_METHOD("java.time.LocalDateTime", "now",
               cbm_type_named(arena, "java.time.LocalDateTime"));
    REG_METHOD("java.time.LocalDateTime", "of",
               cbm_type_named(arena, "java.time.LocalDateTime"));
    REG_METHOD("java.time.LocalDateTime", "parse",
               cbm_type_named(arena, "java.time.LocalDateTime"));
    REG_METHOD("java.time.LocalDateTime", "plusHours",
               cbm_type_named(arena, "java.time.LocalDateTime"));
    REG_METHOD("java.time.LocalDateTime", "minusHours",
               cbm_type_named(arena, "java.time.LocalDateTime"));
    REG_METHOD("java.time.LocalDateTime", "format", cbm_type_named(arena, "java.lang.String"));
    REG_METHOD("java.time.LocalDateTime", "toString", cbm_type_named(arena, "java.lang.String"));

    REG_METHOD("java.time.Instant", "now", cbm_type_named(arena, "java.time.Instant"));
    REG_METHOD("java.time.Instant", "ofEpochMilli", cbm_type_named(arena, "java.time.Instant"));
    REG_METHOD("java.time.Instant", "ofEpochSecond", cbm_type_named(arena, "java.time.Instant"));
    REG_METHOD("java.time.Instant", "toEpochMilli", cbm_type_builtin(arena, "long"));
    REG_METHOD("java.time.Instant", "getEpochSecond", cbm_type_builtin(arena, "long"));
    REG_METHOD("java.time.Instant", "plus", cbm_type_named(arena, "java.time.Instant"));
    REG_METHOD("java.time.Instant", "minus", cbm_type_named(arena, "java.time.Instant"));

    REG_METHOD("java.time.Duration", "ofSeconds", cbm_type_named(arena, "java.time.Duration"));
    REG_METHOD("java.time.Duration", "ofMillis", cbm_type_named(arena, "java.time.Duration"));
    REG_METHOD("java.time.Duration", "ofMinutes", cbm_type_named(arena, "java.time.Duration"));
    REG_METHOD("java.time.Duration", "ofHours", cbm_type_named(arena, "java.time.Duration"));
    REG_METHOD("java.time.Duration", "ofDays", cbm_type_named(arena, "java.time.Duration"));
    REG_METHOD("java.time.Duration", "between", cbm_type_named(arena, "java.time.Duration"));
    REG_METHOD("java.time.Duration", "toMillis", cbm_type_builtin(arena, "long"));
    REG_METHOD("java.time.Duration", "toSeconds", cbm_type_builtin(arena, "long"));
    REG_METHOD("java.time.Duration", "toMinutes", cbm_type_builtin(arena, "long"));

    REG_METHOD("java.time.ZoneId", "of", cbm_type_named(arena, "java.time.ZoneId"));
    REG_METHOD("java.time.ZoneId", "systemDefault", cbm_type_named(arena, "java.time.ZoneId"));
    REG_METHOD("java.time.ZoneId", "getId", cbm_type_named(arena, "java.lang.String"));

    REG_METHOD("java.time.format.DateTimeFormatter", "ofPattern",
               cbm_type_named(arena, "java.time.format.DateTimeFormatter"));
    REG_METHOD("java.time.format.DateTimeFormatter", "format",
               cbm_type_named(arena, "java.lang.String"));

    /* ── java.net (minimal) ──────────────────────────────────── */
    REG_TYPE("java.net.URI", "URI", false, parents_object);
    REG_TYPE("java.net.URL", "URL", false, parents_object);
    REG_METHOD("java.net.URI", "create", cbm_type_named(arena, "java.net.URI"));
    REG_METHOD("java.net.URI", "toString", cbm_type_named(arena, "java.lang.String"));
    REG_METHOD("java.net.URI", "toURL", cbm_type_named(arena, "java.net.URL"));
    REG_METHOD("java.net.URL", "openStream", cbm_type_named(arena, "java.io.InputStream"));
    REG_METHOD("java.net.URL", "toString", cbm_type_named(arena, "java.lang.String"));
    REG_CTOR("java.net.URL", "URL");
    REG_CTOR("java.net.URI", "URI");
}
