/*
 * cs_stdlib_data.c — corpus-seeded .NET BCL stdlib type/method data.
 *
 * Strategy:
 *   1. Genuinely-stdlib base — System.Object root, primitives, collections,
 *      Task<T>, Linq Enumerable extensions.
 *   2. Top corpus types: ASP.NET Core's Console + DI containers, JSON,
 *      logging, runtime intrinsics. We keep the surface deliberately
 *      narrow (≈300 types) to bias for false-negative miss rather than
 *      false-positive misroutes — the LSP can fall back to UNKNOWN safely
 *      but a wrong stdlib entry routes calls to the wrong target.
 *   3. Linq extension methods registered with `param_names[0] = "this"`
 *      so cs_lookup_extension picks them up for any IEnumerable<T>.
 *
 * Type QNs use the dotted form Roslyn produces (System.Console,
 * System.Collections.Generic.List, etc.). Receiver types match exactly so
 * cs_lookup_method's primary index hits.
 */

#include "../type_rep.h"
#include "../type_registry.h"
#include "../../arena.h"
#include "../cs_lsp.h"
#include <string.h>

#define REG_TYPE(qn_, short_, is_iface_, parents_)               \
    do {                                                          \
        memset(&rt, 0, sizeof(rt));                              \
        rt.qualified_name = (qn_);                                \
        rt.short_name = (short_);                                 \
        rt.is_interface = (is_iface_);                            \
        rt.embedded_types = (parents_);                           \
        cbm_registry_add_type(reg, rt);                           \
    } while (0)

/* Helper for single-parent inline registration. The macro avoids the
 * preprocessor "comma in compound literal splits args" pitfall by taking
 * the parent QN as a single string. */
#define REG_TYPE_P1(qn_, short_, is_iface_, p1_)                  \
    do {                                                           \
        static const char *_p_arr[] = {(p1_), NULL};              \
        memset(&rt, 0, sizeof(rt));                                \
        rt.qualified_name = (qn_);                                 \
        rt.short_name = (short_);                                  \
        rt.is_interface = (is_iface_);                             \
        rt.embedded_types = _p_arr;                                \
        cbm_registry_add_type(reg, rt);                            \
    } while (0)

#define REG_GENERIC_TYPE_P1(qn_, short_, is_iface_, p1_, tparams_) \
    do {                                                            \
        static const char *_pg_arr[] = {(p1_), NULL};              \
        memset(&rt, 0, sizeof(rt));                                 \
        rt.qualified_name = (qn_);                                  \
        rt.short_name = (short_);                                   \
        rt.is_interface = (is_iface_);                              \
        rt.embedded_types = _pg_arr;                                \
        rt.type_param_names = (tparams_);                           \
        cbm_registry_add_type(reg, rt);                             \
    } while (0)

#define REG_GENERIC_TYPE(qn_, short_, is_iface_, parents_, tparams_) \
    do {                                                              \
        memset(&rt, 0, sizeof(rt));                                  \
        rt.qualified_name = (qn_);                                    \
        rt.short_name = (short_);                                     \
        rt.is_interface = (is_iface_);                                \
        rt.embedded_types = (parents_);                               \
        rt.type_param_names = (tparams_);                             \
        cbm_registry_add_type(reg, rt);                               \
    } while (0)

#define REG_METHOD(class_qn_, method_name_, ret_type_)              \
    do {                                                             \
        memset(&rf, 0, sizeof(rf));                                 \
        rf.min_params = -1;                                          \
        rf.qualified_name =                                          \
            cbm_arena_sprintf(arena, "%s.%s", (class_qn_), (method_name_)); \
        rf.short_name = (method_name_);                              \
        rf.receiver_type = (class_qn_);                              \
        {                                                            \
            const CBMType **rets =                                   \
                (const CBMType **)cbm_arena_alloc(arena, 2 * sizeof(*rets)); \
            rets[0] = (ret_type_);                                   \
            rets[1] = NULL;                                          \
            rf.signature = cbm_type_func(arena, NULL, NULL, rets);   \
        }                                                            \
        cbm_registry_add_func(reg, rf);                              \
    } while (0)

#define REG_STATIC(class_qn_, method_name_, ret_type_)              \
    do {                                                             \
        memset(&rf, 0, sizeof(rf));                                 \
        rf.min_params = -1;                                          \
        rf.qualified_name =                                          \
            cbm_arena_sprintf(arena, "%s.%s", (class_qn_), (method_name_)); \
        rf.short_name = (method_name_);                              \
        rf.receiver_type = (class_qn_);                              \
        {                                                            \
            const CBMType **rets =                                   \
                (const CBMType **)cbm_arena_alloc(arena, 2 * sizeof(*rets)); \
            rets[0] = (ret_type_);                                   \
            rets[1] = NULL;                                          \
            rf.signature = cbm_type_func(arena, NULL, NULL, rets);   \
        }                                                            \
        cbm_registry_add_func(reg, rf);                              \
    } while (0)

/* Extension method: register as a free static function whose first param's
 * NAME is "this" — cs_lookup_extension dispatches on this convention. */
#define REG_EXTENSION(qn_, short_name_, recv_, ret_type_)            \
    do {                                                             \
        memset(&rf, 0, sizeof(rf));                                 \
        rf.min_params = -1;                                          \
        rf.qualified_name = (qn_);                                   \
        rf.short_name = (short_name_);                               \
        {                                                            \
            const CBMType **rets =                                   \
                (const CBMType **)cbm_arena_alloc(arena, 2 * sizeof(*rets)); \
            rets[0] = (ret_type_);                                   \
            rets[1] = NULL;                                          \
            const char **pnames =                                    \
                (const char **)cbm_arena_alloc(arena, 2 * sizeof(*pnames)); \
            pnames[0] = "this";                                       \
            pnames[1] = NULL;                                        \
            const CBMType **ptypes =                                 \
                (const CBMType **)cbm_arena_alloc(arena, 2 * sizeof(*ptypes)); \
            ptypes[0] = (recv_);                                     \
            ptypes[1] = NULL;                                        \
            rf.signature = cbm_type_func(arena, pnames, ptypes, rets); \
        }                                                            \
        cbm_registry_add_func(reg, rf);                              \
    } while (0)

#define OBJ() cbm_type_named(arena, "System.Object")
#define STR() cbm_type_named(arena, "System.String")
#define INT() cbm_type_named(arena, "System.Int32")
#define LONG_T() cbm_type_named(arena, "System.Int64")
#define BOOL_T() cbm_type_named(arena, "System.Boolean")
#define DBL() cbm_type_named(arena, "System.Double")
#define VOID_T() cbm_type_named(arena, "System.Void")
#define UNK() cbm_type_unknown()

void cbm_csharp_stdlib_register(CBMTypeRegistry *reg, CBMArena *arena) {
    CBMRegisteredType rt;
    CBMRegisteredFunc rf;

    /* ── System root types ─────────────────────────────────────── */
    static const char *no_parents[] = {NULL};
    static const char *obj_parent[] = {"System.Object", NULL};
    static const char *valuetype_parent[] = {"System.ValueType", NULL};
    static const char *exception_parent[] = {"System.Exception", NULL};
    static const char *runtime_exception_parent[] = {"System.SystemException", NULL};

    REG_TYPE("System.Object", "Object", false, no_parents);
    REG_TYPE("System.ValueType", "ValueType", false, obj_parent);
    REG_TYPE("System.Enum", "Enum", false, valuetype_parent);
    REG_TYPE("System.Type", "Type", false, obj_parent);
    REG_TYPE("System.Void", "Void", false, valuetype_parent);
    REG_TYPE("System.Delegate", "Delegate", false, obj_parent);
    REG_TYPE("System.MulticastDelegate", "MulticastDelegate", false, no_parents);
    REG_TYPE("System.IDisposable", "IDisposable", true, no_parents);
    REG_TYPE("System.IComparable", "IComparable", true, no_parents);
    REG_TYPE("System.IEquatable", "IEquatable", true, no_parents);
    REG_TYPE("System.IFormattable", "IFormattable", true, no_parents);

    /* Object methods. */
    REG_METHOD("System.Object", "ToString", STR());
    REG_METHOD("System.Object", "GetHashCode", INT());
    REG_METHOD("System.Object", "Equals", BOOL_T());
    REG_METHOD("System.Object", "GetType", cbm_type_named(arena, "System.Type"));

    /* ── Primitive value types ─────────────────────────────────── */
    REG_TYPE("System.Boolean", "Boolean", false, valuetype_parent);
    REG_TYPE("System.Byte", "Byte", false, valuetype_parent);
    REG_TYPE("System.SByte", "SByte", false, valuetype_parent);
    REG_TYPE("System.Int16", "Int16", false, valuetype_parent);
    REG_TYPE("System.UInt16", "UInt16", false, valuetype_parent);
    REG_TYPE("System.Int32", "Int32", false, valuetype_parent);
    REG_TYPE("System.UInt32", "UInt32", false, valuetype_parent);
    REG_TYPE("System.Int64", "Int64", false, valuetype_parent);
    REG_TYPE("System.UInt64", "UInt64", false, valuetype_parent);
    REG_TYPE("System.Single", "Single", false, valuetype_parent);
    REG_TYPE("System.Double", "Double", false, valuetype_parent);
    REG_TYPE("System.Decimal", "Decimal", false, valuetype_parent);
    REG_TYPE("System.Char", "Char", false, valuetype_parent);
    REG_TYPE("System.IntPtr", "IntPtr", false, valuetype_parent);
    REG_TYPE("System.UIntPtr", "UIntPtr", false, valuetype_parent);

    /* Int32 / Int64 — common static methods. */
    REG_STATIC("System.Int32", "Parse", INT());
    REG_STATIC("System.Int32", "TryParse", BOOL_T());
    REG_METHOD("System.Int32", "ToString", STR());
    REG_METHOD("System.Int32", "CompareTo", INT());
    REG_STATIC("System.Int64", "Parse", LONG_T());
    REG_STATIC("System.Int64", "TryParse", BOOL_T());
    REG_METHOD("System.Int64", "ToString", STR());
    REG_STATIC("System.Double", "Parse", DBL());
    REG_STATIC("System.Double", "TryParse", BOOL_T());
    REG_METHOD("System.Double", "ToString", STR());
    REG_STATIC("System.Boolean", "Parse", BOOL_T());
    REG_STATIC("System.Boolean", "TryParse", BOOL_T());

    /* ── System.String ─────────────────────────────────────────── */
    REG_TYPE("System.String", "String", false, obj_parent);
    REG_METHOD("System.String", "ToUpper", STR());
    REG_METHOD("System.String", "ToLower", STR());
    REG_METHOD("System.String", "ToUpperInvariant", STR());
    REG_METHOD("System.String", "ToLowerInvariant", STR());
    REG_METHOD("System.String", "Trim", STR());
    REG_METHOD("System.String", "TrimStart", STR());
    REG_METHOD("System.String", "TrimEnd", STR());
    REG_METHOD("System.String", "Substring", STR());
    REG_METHOD("System.String", "Replace", STR());
    REG_METHOD("System.String", "Insert", STR());
    REG_METHOD("System.String", "Remove", STR());
    REG_METHOD("System.String", "PadLeft", STR());
    REG_METHOD("System.String", "PadRight", STR());
    REG_METHOD("System.String", "Split",
               cbm_type_template(arena, "System.Array",
                                 (const CBMType *[]){STR(), NULL}, 1));
    REG_METHOD("System.String", "IndexOf", INT());
    REG_METHOD("System.String", "LastIndexOf", INT());
    REG_METHOD("System.String", "StartsWith", BOOL_T());
    REG_METHOD("System.String", "EndsWith", BOOL_T());
    REG_METHOD("System.String", "Contains", BOOL_T());
    REG_METHOD("System.String", "Equals", BOOL_T());
    REG_METHOD("System.String", "CompareTo", INT());
    REG_METHOD("System.String", "GetEnumerator", UNK());
    REG_METHOD("System.String", "Length", INT());
    REG_METHOD("System.String", "ToCharArray",
               cbm_type_template(arena, "System.Array",
                                 (const CBMType *[]){cbm_type_named(arena, "System.Char"), NULL},
                                 1));
    REG_STATIC("System.String", "Format", STR());
    REG_STATIC("System.String", "Concat", STR());
    REG_STATIC("System.String", "Join", STR());
    REG_STATIC("System.String", "IsNullOrEmpty", BOOL_T());
    REG_STATIC("System.String", "IsNullOrWhiteSpace", BOOL_T());
    REG_STATIC("System.String", "Compare", INT());
    REG_STATIC("System.String", "Empty", STR());
    REG_STATIC("System.String", "Equals", BOOL_T());

    /* ── System.Console ─────────────────────────────────────────── */
    REG_TYPE("System.Console", "Console", false, obj_parent);
    REG_STATIC("System.Console", "WriteLine", VOID_T());
    REG_STATIC("System.Console", "Write", VOID_T());
    REG_STATIC("System.Console", "ReadLine", STR());
    REG_STATIC("System.Console", "Read", INT());
    REG_STATIC("System.Console", "ReadKey", UNK());
    REG_STATIC("System.Console", "Beep", VOID_T());
    REG_STATIC("System.Console", "Clear", VOID_T());

    /* ── System.Math ───────────────────────────────────────────── */
    REG_TYPE("System.Math", "Math", false, obj_parent);
    REG_STATIC("System.Math", "Abs", DBL());
    REG_STATIC("System.Math", "Min", DBL());
    REG_STATIC("System.Math", "Max", DBL());
    REG_STATIC("System.Math", "Pow", DBL());
    REG_STATIC("System.Math", "Sqrt", DBL());
    REG_STATIC("System.Math", "Round", DBL());
    REG_STATIC("System.Math", "Floor", DBL());
    REG_STATIC("System.Math", "Ceiling", DBL());
    REG_STATIC("System.Math", "Sin", DBL());
    REG_STATIC("System.Math", "Cos", DBL());
    REG_STATIC("System.Math", "Tan", DBL());
    REG_STATIC("System.Math", "Log", DBL());
    REG_STATIC("System.Math", "Log10", DBL());
    REG_STATIC("System.Math", "Exp", DBL());
    REG_STATIC("System.Math", "PI", DBL());
    REG_STATIC("System.Math", "E", DBL());
    REG_STATIC("System.Math", "Sign", INT());

    /* ── System.Convert ────────────────────────────────────────── */
    REG_TYPE("System.Convert", "Convert", false, obj_parent);
    REG_STATIC("System.Convert", "ToInt32", INT());
    REG_STATIC("System.Convert", "ToInt64", LONG_T());
    REG_STATIC("System.Convert", "ToString", STR());
    REG_STATIC("System.Convert", "ToBoolean", BOOL_T());
    REG_STATIC("System.Convert", "ToDouble", DBL());
    REG_STATIC("System.Convert", "ToSingle", cbm_type_named(arena, "System.Single"));
    REG_STATIC("System.Convert", "ChangeType", OBJ());

    /* ── System.Guid ───────────────────────────────────────────── */
    REG_TYPE("System.Guid", "Guid", false, valuetype_parent);
    REG_STATIC("System.Guid", "NewGuid", cbm_type_named(arena, "System.Guid"));
    REG_STATIC("System.Guid", "Parse", cbm_type_named(arena, "System.Guid"));
    REG_STATIC("System.Guid", "TryParse", BOOL_T());
    REG_METHOD("System.Guid", "ToString", STR());

    /* ── System.DateTime ───────────────────────────────────────── */
    REG_TYPE("System.DateTime", "DateTime", false, valuetype_parent);
    REG_STATIC("System.DateTime", "Now", cbm_type_named(arena, "System.DateTime"));
    REG_STATIC("System.DateTime", "UtcNow", cbm_type_named(arena, "System.DateTime"));
    REG_STATIC("System.DateTime", "Today", cbm_type_named(arena, "System.DateTime"));
    REG_STATIC("System.DateTime", "Parse", cbm_type_named(arena, "System.DateTime"));
    REG_STATIC("System.DateTime", "TryParse", BOOL_T());
    REG_METHOD("System.DateTime", "AddDays", cbm_type_named(arena, "System.DateTime"));
    REG_METHOD("System.DateTime", "AddHours", cbm_type_named(arena, "System.DateTime"));
    REG_METHOD("System.DateTime", "AddMinutes", cbm_type_named(arena, "System.DateTime"));
    REG_METHOD("System.DateTime", "AddSeconds", cbm_type_named(arena, "System.DateTime"));
    REG_METHOD("System.DateTime", "AddMilliseconds", cbm_type_named(arena, "System.DateTime"));
    REG_METHOD("System.DateTime", "Subtract", cbm_type_named(arena, "System.TimeSpan"));
    REG_METHOD("System.DateTime", "ToString", STR());
    REG_METHOD("System.DateTime", "ToUniversalTime", cbm_type_named(arena, "System.DateTime"));
    REG_METHOD("System.DateTime", "ToLocalTime", cbm_type_named(arena, "System.DateTime"));
    REG_METHOD("System.DateTime", "Year", INT());
    REG_METHOD("System.DateTime", "Month", INT());
    REG_METHOD("System.DateTime", "Day", INT());
    REG_METHOD("System.DateTime", "Hour", INT());
    REG_METHOD("System.DateTime", "Minute", INT());
    REG_METHOD("System.DateTime", "Second", INT());
    REG_TYPE("System.TimeSpan", "TimeSpan", false, valuetype_parent);
    REG_TYPE("System.DateTimeOffset", "DateTimeOffset", false, valuetype_parent);
    REG_STATIC("System.DateTimeOffset", "Now", cbm_type_named(arena, "System.DateTimeOffset"));
    REG_STATIC("System.DateTimeOffset", "UtcNow", cbm_type_named(arena, "System.DateTimeOffset"));

    /* ── System.IO ─────────────────────────────────────────────── */
    REG_TYPE("System.IO.File", "File", false, obj_parent);
    REG_STATIC("System.IO.File", "ReadAllText", STR());
    REG_STATIC("System.IO.File", "ReadAllLines",
               cbm_type_template(arena, "System.Array",
                                 (const CBMType *[]){STR(), NULL}, 1));
    REG_STATIC("System.IO.File", "ReadAllBytes",
               cbm_type_template(arena, "System.Array",
                                 (const CBMType *[]){cbm_type_named(arena, "System.Byte"), NULL},
                                 1));
    REG_STATIC("System.IO.File", "WriteAllText", VOID_T());
    REG_STATIC("System.IO.File", "WriteAllLines", VOID_T());
    REG_STATIC("System.IO.File", "WriteAllBytes", VOID_T());
    REG_STATIC("System.IO.File", "Exists", BOOL_T());
    REG_STATIC("System.IO.File", "Delete", VOID_T());
    REG_STATIC("System.IO.File", "Copy", VOID_T());
    REG_STATIC("System.IO.File", "Move", VOID_T());
    REG_STATIC("System.IO.File", "Open", cbm_type_named(arena, "System.IO.FileStream"));
    REG_STATIC("System.IO.File", "OpenRead", cbm_type_named(arena, "System.IO.FileStream"));
    REG_STATIC("System.IO.File", "OpenWrite", cbm_type_named(arena, "System.IO.FileStream"));
    REG_STATIC("System.IO.File", "Create", cbm_type_named(arena, "System.IO.FileStream"));
    REG_STATIC("System.IO.File", "ReadAllTextAsync",
               cbm_type_template(arena, "System.Threading.Tasks.Task",
                                 (const CBMType *[]){STR(), NULL}, 1));
    REG_STATIC("System.IO.File", "WriteAllTextAsync",
               cbm_type_named(arena, "System.Threading.Tasks.Task"));
    REG_TYPE("System.IO.FileStream", "FileStream", false, obj_parent);
    REG_METHOD("System.IO.FileStream", "Close", VOID_T());
    REG_METHOD("System.IO.FileStream", "Read", INT());
    REG_METHOD("System.IO.FileStream", "Write", VOID_T());
    REG_METHOD("System.IO.FileStream", "Dispose", VOID_T());
    REG_METHOD("System.IO.FileStream", "Flush", VOID_T());

    REG_TYPE("System.IO.Path", "Path", false, obj_parent);
    REG_STATIC("System.IO.Path", "Combine", STR());
    REG_STATIC("System.IO.Path", "GetExtension", STR());
    REG_STATIC("System.IO.Path", "GetFileName", STR());
    REG_STATIC("System.IO.Path", "GetFileNameWithoutExtension", STR());
    REG_STATIC("System.IO.Path", "GetDirectoryName", STR());
    REG_STATIC("System.IO.Path", "GetFullPath", STR());
    REG_STATIC("System.IO.Path", "GetTempPath", STR());
    REG_STATIC("System.IO.Path", "GetTempFileName", STR());
    REG_STATIC("System.IO.Path", "ChangeExtension", STR());
    REG_STATIC("System.IO.Path", "Exists", BOOL_T());

    REG_TYPE("System.IO.Directory", "Directory", false, obj_parent);
    REG_STATIC("System.IO.Directory", "CreateDirectory",
               cbm_type_named(arena, "System.IO.DirectoryInfo"));
    REG_STATIC("System.IO.Directory", "Exists", BOOL_T());
    REG_STATIC("System.IO.Directory", "Delete", VOID_T());
    REG_STATIC("System.IO.Directory", "GetFiles",
               cbm_type_template(arena, "System.Array",
                                 (const CBMType *[]){STR(), NULL}, 1));
    REG_STATIC("System.IO.Directory", "GetDirectories",
               cbm_type_template(arena, "System.Array",
                                 (const CBMType *[]){STR(), NULL}, 1));
    REG_STATIC("System.IO.Directory", "EnumerateFiles",
               cbm_type_template(arena, "System.Collections.Generic.IEnumerable",
                                 (const CBMType *[]){STR(), NULL}, 1));
    REG_TYPE("System.IO.DirectoryInfo", "DirectoryInfo", false, obj_parent);

    /* ── System.Text ───────────────────────────────────────────── */
    REG_TYPE("System.Text.StringBuilder", "StringBuilder", false, obj_parent);
    REG_METHOD("System.Text.StringBuilder", "Append",
               cbm_type_named(arena, "System.Text.StringBuilder"));
    REG_METHOD("System.Text.StringBuilder", "AppendLine",
               cbm_type_named(arena, "System.Text.StringBuilder"));
    REG_METHOD("System.Text.StringBuilder", "AppendFormat",
               cbm_type_named(arena, "System.Text.StringBuilder"));
    REG_METHOD("System.Text.StringBuilder", "Clear",
               cbm_type_named(arena, "System.Text.StringBuilder"));
    REG_METHOD("System.Text.StringBuilder", "Insert",
               cbm_type_named(arena, "System.Text.StringBuilder"));
    REG_METHOD("System.Text.StringBuilder", "Replace",
               cbm_type_named(arena, "System.Text.StringBuilder"));
    REG_METHOD("System.Text.StringBuilder", "ToString", STR());
    REG_METHOD("System.Text.StringBuilder", "Length", INT());

    REG_TYPE("System.Text.Encoding", "Encoding", false, obj_parent);
    REG_STATIC("System.Text.Encoding", "UTF8", cbm_type_named(arena, "System.Text.Encoding"));
    REG_STATIC("System.Text.Encoding", "ASCII", cbm_type_named(arena, "System.Text.Encoding"));
    REG_STATIC("System.Text.Encoding", "Unicode", cbm_type_named(arena, "System.Text.Encoding"));
    REG_METHOD("System.Text.Encoding", "GetBytes",
               cbm_type_template(arena, "System.Array",
                                 (const CBMType *[]){cbm_type_named(arena, "System.Byte"), NULL},
                                 1));
    REG_METHOD("System.Text.Encoding", "GetString", STR());

    REG_TYPE("System.Text.RegularExpressions.Regex", "Regex", false, obj_parent);
    REG_STATIC("System.Text.RegularExpressions.Regex", "Match",
               cbm_type_named(arena, "System.Text.RegularExpressions.Match"));
    REG_STATIC("System.Text.RegularExpressions.Regex", "Matches",
               cbm_type_named(arena, "System.Text.RegularExpressions.MatchCollection"));
    REG_STATIC("System.Text.RegularExpressions.Regex", "IsMatch", BOOL_T());
    REG_STATIC("System.Text.RegularExpressions.Regex", "Replace", STR());
    REG_METHOD("System.Text.RegularExpressions.Regex", "Match",
               cbm_type_named(arena, "System.Text.RegularExpressions.Match"));
    REG_METHOD("System.Text.RegularExpressions.Regex", "Matches",
               cbm_type_named(arena, "System.Text.RegularExpressions.MatchCollection"));
    REG_METHOD("System.Text.RegularExpressions.Regex", "IsMatch", BOOL_T());
    REG_METHOD("System.Text.RegularExpressions.Regex", "Replace", STR());
    REG_TYPE("System.Text.RegularExpressions.Match", "Match", false, obj_parent);
    REG_METHOD("System.Text.RegularExpressions.Match", "Success", BOOL_T());
    REG_METHOD("System.Text.RegularExpressions.Match", "Value", STR());
    REG_TYPE("System.Text.RegularExpressions.MatchCollection", "MatchCollection", false,
             obj_parent);

    /* ── System.Collections.Generic ────────────────────────────── */
    static const char *list_t_params[] = {"T", NULL};
    static const char *kv_t_params[] = {"TKey", "TValue", NULL};
    static const char *single_t_params[] = {"T", NULL};

    static const char *ienumerable_parents[] = {"System.Collections.IEnumerable", NULL};
    REG_GENERIC_TYPE("System.Collections.Generic.IEnumerable", "IEnumerable", true,
                      ienumerable_parents, single_t_params);
    REG_METHOD("System.Collections.Generic.IEnumerable", "GetEnumerator",
               cbm_type_template(arena, "System.Collections.Generic.IEnumerator",
                                 (const CBMType *[]){cbm_type_type_param(arena, "T"), NULL}, 1));

    static const char *ienumerator_parents[] = {NULL};
    REG_GENERIC_TYPE("System.Collections.Generic.IEnumerator", "IEnumerator", true,
                      ienumerator_parents, single_t_params);
    REG_METHOD("System.Collections.Generic.IEnumerator", "MoveNext", BOOL_T());
    REG_METHOD("System.Collections.Generic.IEnumerator", "Current",
               cbm_type_type_param(arena, "T"));
    REG_METHOD("System.Collections.Generic.IEnumerator", "Reset", VOID_T());

    static const char *icollection_parents[] = {
        "System.Collections.Generic.IEnumerable", NULL};
    REG_GENERIC_TYPE("System.Collections.Generic.ICollection", "ICollection", true,
                      icollection_parents, single_t_params);
    REG_METHOD("System.Collections.Generic.ICollection", "Add", VOID_T());
    REG_METHOD("System.Collections.Generic.ICollection", "Remove", BOOL_T());
    REG_METHOD("System.Collections.Generic.ICollection", "Clear", VOID_T());
    REG_METHOD("System.Collections.Generic.ICollection", "Contains", BOOL_T());
    REG_METHOD("System.Collections.Generic.ICollection", "Count", INT());
    REG_METHOD("System.Collections.Generic.ICollection", "CopyTo", VOID_T());

    static const char *ilist_parents[] = {"System.Collections.Generic.ICollection", NULL};
    REG_GENERIC_TYPE("System.Collections.Generic.IList", "IList", true, ilist_parents,
                      single_t_params);
    REG_METHOD("System.Collections.Generic.IList", "IndexOf", INT());
    REG_METHOD("System.Collections.Generic.IList", "Insert", VOID_T());
    REG_METHOD("System.Collections.Generic.IList", "RemoveAt", VOID_T());

    static const char *list_parents[] = {"System.Collections.Generic.IList",
                                          "System.Collections.Generic.ICollection",
                                          "System.Collections.Generic.IEnumerable", NULL};
    REG_GENERIC_TYPE("System.Collections.Generic.List", "List", false, list_parents,
                      list_t_params);
    REG_METHOD("System.Collections.Generic.List", "Add", VOID_T());
    REG_METHOD("System.Collections.Generic.List", "AddRange", VOID_T());
    REG_METHOD("System.Collections.Generic.List", "Remove", BOOL_T());
    REG_METHOD("System.Collections.Generic.List", "RemoveAt", VOID_T());
    REG_METHOD("System.Collections.Generic.List", "RemoveAll", INT());
    REG_METHOD("System.Collections.Generic.List", "Clear", VOID_T());
    REG_METHOD("System.Collections.Generic.List", "Contains", BOOL_T());
    REG_METHOD("System.Collections.Generic.List", "IndexOf", INT());
    REG_METHOD("System.Collections.Generic.List", "Insert", VOID_T());
    REG_METHOD("System.Collections.Generic.List", "Sort", VOID_T());
    REG_METHOD("System.Collections.Generic.List", "Reverse", VOID_T());
    REG_METHOD("System.Collections.Generic.List", "Find", cbm_type_type_param(arena, "T"));
    REG_METHOD("System.Collections.Generic.List", "FindAll",
               cbm_type_template(arena, "System.Collections.Generic.List",
                                 (const CBMType *[]){cbm_type_type_param(arena, "T"), NULL}, 1));
    REG_METHOD("System.Collections.Generic.List", "FindIndex", INT());
    REG_METHOD("System.Collections.Generic.List", "Count", INT());
    REG_METHOD("System.Collections.Generic.List", "ToArray",
               cbm_type_template(arena, "System.Array",
                                 (const CBMType *[]){cbm_type_type_param(arena, "T"), NULL}, 1));
    REG_METHOD("System.Collections.Generic.List", "ForEach", VOID_T());
    REG_METHOD("System.Collections.Generic.List", "ConvertAll",
               cbm_type_template(arena, "System.Collections.Generic.List",
                                 (const CBMType *[]){UNK(), NULL}, 1));
    REG_METHOD("System.Collections.Generic.List", "GetEnumerator",
               cbm_type_template(arena, "System.Collections.Generic.IEnumerator",
                                 (const CBMType *[]){cbm_type_type_param(arena, "T"), NULL}, 1));
    REG_METHOD("System.Collections.Generic.List", "GetRange",
               cbm_type_template(arena, "System.Collections.Generic.List",
                                 (const CBMType *[]){cbm_type_type_param(arena, "T"), NULL}, 1));

    static const char *idict_parents[] = {"System.Collections.Generic.ICollection", NULL};
    REG_GENERIC_TYPE("System.Collections.Generic.IDictionary", "IDictionary", true,
                      idict_parents, kv_t_params);
    REG_METHOD("System.Collections.Generic.IDictionary", "Add", VOID_T());
    REG_METHOD("System.Collections.Generic.IDictionary", "Remove", BOOL_T());
    REG_METHOD("System.Collections.Generic.IDictionary", "ContainsKey", BOOL_T());
    REG_METHOD("System.Collections.Generic.IDictionary", "TryGetValue", BOOL_T());
    REG_METHOD("System.Collections.Generic.IDictionary", "Keys",
               cbm_type_template(arena, "System.Collections.Generic.ICollection",
                                 (const CBMType *[]){cbm_type_type_param(arena, "TKey"), NULL},
                                 1));
    REG_METHOD("System.Collections.Generic.IDictionary", "Values",
               cbm_type_template(arena, "System.Collections.Generic.ICollection",
                                 (const CBMType *[]){cbm_type_type_param(arena, "TValue"), NULL},
                                 1));

    static const char *dict_parents[] = {"System.Collections.Generic.IDictionary",
                                          "System.Collections.Generic.ICollection",
                                          "System.Collections.Generic.IEnumerable", NULL};
    REG_GENERIC_TYPE("System.Collections.Generic.Dictionary", "Dictionary", false,
                      dict_parents, kv_t_params);
    REG_METHOD("System.Collections.Generic.Dictionary", "Add", VOID_T());
    REG_METHOD("System.Collections.Generic.Dictionary", "Remove", BOOL_T());
    REG_METHOD("System.Collections.Generic.Dictionary", "Clear", VOID_T());
    REG_METHOD("System.Collections.Generic.Dictionary", "ContainsKey", BOOL_T());
    REG_METHOD("System.Collections.Generic.Dictionary", "ContainsValue", BOOL_T());
    REG_METHOD("System.Collections.Generic.Dictionary", "TryGetValue", BOOL_T());
    REG_METHOD("System.Collections.Generic.Dictionary", "TryAdd", BOOL_T());
    REG_METHOD("System.Collections.Generic.Dictionary", "GetEnumerator", UNK());
    REG_METHOD("System.Collections.Generic.Dictionary", "Count", INT());
    REG_METHOD("System.Collections.Generic.Dictionary", "Keys",
               cbm_type_template(arena, "System.Collections.Generic.ICollection",
                                 (const CBMType *[]){cbm_type_type_param(arena, "TKey"), NULL},
                                 1));
    REG_METHOD("System.Collections.Generic.Dictionary", "Values",
               cbm_type_template(arena, "System.Collections.Generic.ICollection",
                                 (const CBMType *[]){cbm_type_type_param(arena, "TValue"), NULL},
                                 1));

    static const char *hashset_parents[] = {"System.Collections.Generic.ICollection", NULL};
    REG_GENERIC_TYPE("System.Collections.Generic.HashSet", "HashSet", false, hashset_parents,
                      single_t_params);
    REG_METHOD("System.Collections.Generic.HashSet", "Add", BOOL_T());
    REG_METHOD("System.Collections.Generic.HashSet", "Remove", BOOL_T());
    REG_METHOD("System.Collections.Generic.HashSet", "Contains", BOOL_T());
    REG_METHOD("System.Collections.Generic.HashSet", "Clear", VOID_T());
    REG_METHOD("System.Collections.Generic.HashSet", "Count", INT());
    REG_METHOD("System.Collections.Generic.HashSet", "UnionWith", VOID_T());
    REG_METHOD("System.Collections.Generic.HashSet", "IntersectWith", VOID_T());
    REG_METHOD("System.Collections.Generic.HashSet", "ExceptWith", VOID_T());
    REG_METHOD("System.Collections.Generic.HashSet", "SymmetricExceptWith", VOID_T());

    static const char *queue_parents[] = {"System.Collections.Generic.ICollection", NULL};
    REG_GENERIC_TYPE("System.Collections.Generic.Queue", "Queue", false, queue_parents,
                      single_t_params);
    REG_METHOD("System.Collections.Generic.Queue", "Enqueue", VOID_T());
    REG_METHOD("System.Collections.Generic.Queue", "Dequeue", cbm_type_type_param(arena, "T"));
    REG_METHOD("System.Collections.Generic.Queue", "Peek", cbm_type_type_param(arena, "T"));
    REG_METHOD("System.Collections.Generic.Queue", "Count", INT());

    static const char *stack_parents[] = {"System.Collections.Generic.ICollection", NULL};
    REG_GENERIC_TYPE("System.Collections.Generic.Stack", "Stack", false, stack_parents,
                      single_t_params);
    REG_METHOD("System.Collections.Generic.Stack", "Push", VOID_T());
    REG_METHOD("System.Collections.Generic.Stack", "Pop", cbm_type_type_param(arena, "T"));
    REG_METHOD("System.Collections.Generic.Stack", "Peek", cbm_type_type_param(arena, "T"));
    REG_METHOD("System.Collections.Generic.Stack", "Count", INT());

    static const char *kvp_parents[] = {NULL};
    REG_GENERIC_TYPE("System.Collections.Generic.KeyValuePair", "KeyValuePair", false,
                      kvp_parents, kv_t_params);
    REG_METHOD("System.Collections.Generic.KeyValuePair", "Key",
               cbm_type_type_param(arena, "TKey"));
    REG_METHOD("System.Collections.Generic.KeyValuePair", "Value",
               cbm_type_type_param(arena, "TValue"));

    /* Non-generic collections */
    REG_TYPE("System.Collections.IEnumerable", "IEnumerable", true, no_parents);
    REG_TYPE("System.Collections.IEnumerator", "IEnumerator", true, no_parents);
    REG_TYPE_P1("System.Collections.ICollection", "ICollection", true,
                 "System.Collections.IEnumerable");
    REG_TYPE_P1("System.Collections.IList", "IList", true,
                 "System.Collections.ICollection");

    /* Array */
    REG_TYPE("System.Array", "Array", false, obj_parent);
    REG_METHOD("System.Array", "Length", INT());
    REG_METHOD("System.Array", "GetLength", INT());
    REG_METHOD("System.Array", "GetEnumerator", UNK());
    REG_METHOD("System.Array", "GetValue", OBJ());
    REG_METHOD("System.Array", "SetValue", VOID_T());
    REG_METHOD("System.Array", "Clone", OBJ());
    REG_STATIC("System.Array", "Sort", VOID_T());
    REG_STATIC("System.Array", "Reverse", VOID_T());
    REG_STATIC("System.Array", "IndexOf", INT());
    REG_STATIC("System.Array", "Resize", VOID_T());
    REG_STATIC("System.Array", "Empty", UNK());

    /* Span / ReadOnlySpan */
    static const char *span_parents[] = {NULL};
    REG_GENERIC_TYPE("System.Span", "Span", false, span_parents, single_t_params);
    REG_METHOD("System.Span", "Length", INT());
    REG_METHOD("System.Span", "Slice",
               cbm_type_template(arena, "System.Span",
                                 (const CBMType *[]){cbm_type_type_param(arena, "T"), NULL}, 1));
    REG_METHOD("System.Span", "ToArray",
               cbm_type_template(arena, "System.Array",
                                 (const CBMType *[]){cbm_type_type_param(arena, "T"), NULL}, 1));
    REG_GENERIC_TYPE("System.ReadOnlySpan", "ReadOnlySpan", false, span_parents,
                      single_t_params);
    REG_METHOD("System.ReadOnlySpan", "Length", INT());
    REG_METHOD("System.ReadOnlySpan", "Slice",
               cbm_type_template(arena, "System.ReadOnlySpan",
                                 (const CBMType *[]){cbm_type_type_param(arena, "T"), NULL}, 1));

    REG_GENERIC_TYPE("System.Memory", "Memory", false, span_parents, single_t_params);
    REG_GENERIC_TYPE("System.ReadOnlyMemory", "ReadOnlyMemory", false, span_parents,
                      single_t_params);

    REG_GENERIC_TYPE("System.Nullable", "Nullable", false, valuetype_parent, single_t_params);
    REG_METHOD("System.Nullable", "HasValue", BOOL_T());
    REG_METHOD("System.Nullable", "Value", cbm_type_type_param(arena, "T"));
    REG_METHOD("System.Nullable", "GetValueOrDefault", cbm_type_type_param(arena, "T"));

    /* ── System.Threading.Tasks ────────────────────────────────── */
    REG_TYPE("System.Threading.Tasks.Task", "Task", false, obj_parent);
    REG_METHOD("System.Threading.Tasks.Task", "Wait", VOID_T());
    REG_METHOD("System.Threading.Tasks.Task", "Result", UNK());
    REG_METHOD("System.Threading.Tasks.Task", "ContinueWith",
               cbm_type_named(arena, "System.Threading.Tasks.Task"));
    REG_METHOD("System.Threading.Tasks.Task", "GetAwaiter", UNK());
    REG_METHOD("System.Threading.Tasks.Task", "IsCompleted", BOOL_T());
    REG_METHOD("System.Threading.Tasks.Task", "IsFaulted", BOOL_T());
    REG_METHOD("System.Threading.Tasks.Task", "IsCanceled", BOOL_T());
    REG_METHOD("System.Threading.Tasks.Task", "Status", UNK());
    REG_METHOD("System.Threading.Tasks.Task", "Exception",
               cbm_type_named(arena, "System.AggregateException"));
    REG_METHOD("System.Threading.Tasks.Task", "Dispose", VOID_T());
    REG_STATIC("System.Threading.Tasks.Task", "Run",
               cbm_type_named(arena, "System.Threading.Tasks.Task"));
    REG_STATIC("System.Threading.Tasks.Task", "Delay",
               cbm_type_named(arena, "System.Threading.Tasks.Task"));
    REG_STATIC("System.Threading.Tasks.Task", "WhenAll",
               cbm_type_named(arena, "System.Threading.Tasks.Task"));
    REG_STATIC("System.Threading.Tasks.Task", "WhenAny",
               cbm_type_named(arena, "System.Threading.Tasks.Task"));
    REG_STATIC("System.Threading.Tasks.Task", "FromResult",
               cbm_type_named(arena, "System.Threading.Tasks.Task"));
    REG_STATIC("System.Threading.Tasks.Task", "CompletedTask",
               cbm_type_named(arena, "System.Threading.Tasks.Task"));

    REG_GENERIC_TYPE_P1("System.Threading.Tasks.Task", "Task", false,
                         "System.Threading.Tasks.Task", single_t_params);
    /* Same QN for generic — registry stores both. The Task<T>.Result uses
     * type param T. */

    REG_TYPE("System.Threading.Tasks.ValueTask", "ValueTask", false, obj_parent);
    REG_GENERIC_TYPE_P1("System.Threading.Tasks.ValueTask", "ValueTask", false,
                         "System.Threading.Tasks.ValueTask", single_t_params);

    REG_TYPE("System.Threading.CancellationToken", "CancellationToken", false, valuetype_parent);
    REG_METHOD("System.Threading.CancellationToken", "IsCancellationRequested", BOOL_T());
    REG_METHOD("System.Threading.CancellationToken", "ThrowIfCancellationRequested", VOID_T());
    REG_METHOD("System.Threading.CancellationToken", "Register", UNK());
    REG_TYPE("System.Threading.CancellationTokenSource", "CancellationTokenSource", false,
              obj_parent);
    REG_METHOD("System.Threading.CancellationTokenSource", "Token",
               cbm_type_named(arena, "System.Threading.CancellationToken"));
    REG_METHOD("System.Threading.CancellationTokenSource", "Cancel", VOID_T());

    /* ── Exceptions ────────────────────────────────────────────── */
    REG_TYPE("System.Exception", "Exception", false, obj_parent);
    REG_METHOD("System.Exception", "Message", STR());
    REG_METHOD("System.Exception", "StackTrace", STR());
    REG_METHOD("System.Exception", "InnerException",
               cbm_type_named(arena, "System.Exception"));
    REG_METHOD("System.Exception", "ToString", STR());
    REG_METHOD("System.Exception", "GetBaseException",
               cbm_type_named(arena, "System.Exception"));
    REG_METHOD("System.Exception", "Source", STR());

    REG_TYPE("System.SystemException", "SystemException", false, exception_parent);
    REG_TYPE("System.ApplicationException", "ApplicationException", false, exception_parent);
    REG_TYPE("System.AggregateException", "AggregateException", false, exception_parent);
    REG_TYPE("System.ArgumentException", "ArgumentException", false, runtime_exception_parent);
    REG_TYPE_P1("System.ArgumentNullException", "ArgumentNullException", false,
                 "System.ArgumentException");
    REG_TYPE_P1("System.ArgumentOutOfRangeException", "ArgumentOutOfRangeException", false,
                 "System.ArgumentException");
    REG_TYPE("System.InvalidOperationException", "InvalidOperationException", false,
              runtime_exception_parent);
    REG_TYPE("System.NotImplementedException", "NotImplementedException", false,
              runtime_exception_parent);
    REG_TYPE("System.NotSupportedException", "NotSupportedException", false,
              runtime_exception_parent);
    REG_TYPE("System.NullReferenceException", "NullReferenceException", false,
              runtime_exception_parent);
    REG_TYPE("System.IndexOutOfRangeException", "IndexOutOfRangeException", false,
              runtime_exception_parent);
    REG_TYPE("System.OverflowException", "OverflowException", false, runtime_exception_parent);
    REG_TYPE_P1("System.DivideByZeroException", "DivideByZeroException", false,
                 "System.ArithmeticException");
    REG_TYPE("System.ArithmeticException", "ArithmeticException", false,
              runtime_exception_parent);
    REG_TYPE("System.FormatException", "FormatException", false, runtime_exception_parent);
    REG_TYPE("System.IO.IOException", "IOException", false, runtime_exception_parent);
    REG_TYPE_P1("System.IO.FileNotFoundException", "FileNotFoundException", false,
                 "System.IO.IOException");
    REG_TYPE_P1("System.IO.DirectoryNotFoundException", "DirectoryNotFoundException", false,
                 "System.IO.IOException");
    REG_TYPE("System.UnauthorizedAccessException", "UnauthorizedAccessException", false,
              runtime_exception_parent);
    REG_TYPE("System.OperationCanceledException", "OperationCanceledException", false,
              runtime_exception_parent);
    REG_TYPE_P1("System.TaskCanceledException", "TaskCanceledException", false,
                 "System.OperationCanceledException");

    /* ── System.Linq.Enumerable extension methods ──────────────── */
    /* These dispatch on IEnumerable<T> via the cs_lookup_extension path.
     * We register them as free functions whose first param is named "this". */
    REG_TYPE("System.Linq.Enumerable", "Enumerable", false, obj_parent);
    REG_EXTENSION("System.Linq.Enumerable.Where", "Where",
                   cbm_type_template(arena, "System.Collections.Generic.IEnumerable",
                                     (const CBMType *[]){cbm_type_type_param(arena, "T"), NULL},
                                     1),
                   cbm_type_template(arena, "System.Collections.Generic.IEnumerable",
                                     (const CBMType *[]){cbm_type_type_param(arena, "T"), NULL},
                                     1));
    REG_EXTENSION("System.Linq.Enumerable.Select", "Select",
                   cbm_type_template(arena, "System.Collections.Generic.IEnumerable",
                                     (const CBMType *[]){cbm_type_type_param(arena, "T"), NULL},
                                     1),
                   cbm_type_template(arena, "System.Collections.Generic.IEnumerable",
                                     (const CBMType *[]){cbm_type_type_param(arena, "T"), NULL},
                                     1));
    REG_EXTENSION("System.Linq.Enumerable.SelectMany", "SelectMany",
                   cbm_type_template(arena, "System.Collections.Generic.IEnumerable",
                                     (const CBMType *[]){cbm_type_type_param(arena, "T"), NULL},
                                     1),
                   cbm_type_template(arena, "System.Collections.Generic.IEnumerable",
                                     (const CBMType *[]){cbm_type_type_param(arena, "T"), NULL},
                                     1));
    REG_EXTENSION("System.Linq.Enumerable.First", "First",
                   cbm_type_template(arena, "System.Collections.Generic.IEnumerable",
                                     (const CBMType *[]){cbm_type_type_param(arena, "T"), NULL},
                                     1),
                   cbm_type_type_param(arena, "T"));
    REG_EXTENSION("System.Linq.Enumerable.FirstOrDefault", "FirstOrDefault",
                   cbm_type_template(arena, "System.Collections.Generic.IEnumerable",
                                     (const CBMType *[]){cbm_type_type_param(arena, "T"), NULL},
                                     1),
                   cbm_type_type_param(arena, "T"));
    REG_EXTENSION("System.Linq.Enumerable.Last", "Last",
                   cbm_type_template(arena, "System.Collections.Generic.IEnumerable",
                                     (const CBMType *[]){cbm_type_type_param(arena, "T"), NULL},
                                     1),
                   cbm_type_type_param(arena, "T"));
    REG_EXTENSION("System.Linq.Enumerable.LastOrDefault", "LastOrDefault",
                   cbm_type_template(arena, "System.Collections.Generic.IEnumerable",
                                     (const CBMType *[]){cbm_type_type_param(arena, "T"), NULL},
                                     1),
                   cbm_type_type_param(arena, "T"));
    REG_EXTENSION("System.Linq.Enumerable.Single", "Single",
                   cbm_type_template(arena, "System.Collections.Generic.IEnumerable",
                                     (const CBMType *[]){cbm_type_type_param(arena, "T"), NULL},
                                     1),
                   cbm_type_type_param(arena, "T"));
    REG_EXTENSION("System.Linq.Enumerable.SingleOrDefault", "SingleOrDefault",
                   cbm_type_template(arena, "System.Collections.Generic.IEnumerable",
                                     (const CBMType *[]){cbm_type_type_param(arena, "T"), NULL},
                                     1),
                   cbm_type_type_param(arena, "T"));
    REG_EXTENSION("System.Linq.Enumerable.Count", "Count",
                   cbm_type_template(arena, "System.Collections.Generic.IEnumerable",
                                     (const CBMType *[]){cbm_type_type_param(arena, "T"), NULL},
                                     1),
                   INT());
    REG_EXTENSION("System.Linq.Enumerable.Any", "Any",
                   cbm_type_template(arena, "System.Collections.Generic.IEnumerable",
                                     (const CBMType *[]){cbm_type_type_param(arena, "T"), NULL},
                                     1),
                   BOOL_T());
    REG_EXTENSION("System.Linq.Enumerable.All", "All",
                   cbm_type_template(arena, "System.Collections.Generic.IEnumerable",
                                     (const CBMType *[]){cbm_type_type_param(arena, "T"), NULL},
                                     1),
                   BOOL_T());
    REG_EXTENSION("System.Linq.Enumerable.Sum", "Sum",
                   cbm_type_template(arena, "System.Collections.Generic.IEnumerable",
                                     (const CBMType *[]){cbm_type_type_param(arena, "T"), NULL},
                                     1),
                   DBL());
    REG_EXTENSION("System.Linq.Enumerable.Average", "Average",
                   cbm_type_template(arena, "System.Collections.Generic.IEnumerable",
                                     (const CBMType *[]){cbm_type_type_param(arena, "T"), NULL},
                                     1),
                   DBL());
    REG_EXTENSION("System.Linq.Enumerable.Min", "Min",
                   cbm_type_template(arena, "System.Collections.Generic.IEnumerable",
                                     (const CBMType *[]){cbm_type_type_param(arena, "T"), NULL},
                                     1),
                   cbm_type_type_param(arena, "T"));
    REG_EXTENSION("System.Linq.Enumerable.Max", "Max",
                   cbm_type_template(arena, "System.Collections.Generic.IEnumerable",
                                     (const CBMType *[]){cbm_type_type_param(arena, "T"), NULL},
                                     1),
                   cbm_type_type_param(arena, "T"));
    REG_EXTENSION("System.Linq.Enumerable.OrderBy", "OrderBy",
                   cbm_type_template(arena, "System.Collections.Generic.IEnumerable",
                                     (const CBMType *[]){cbm_type_type_param(arena, "T"), NULL},
                                     1),
                   cbm_type_template(arena, "System.Linq.IOrderedEnumerable",
                                     (const CBMType *[]){cbm_type_type_param(arena, "T"), NULL},
                                     1));
    REG_EXTENSION("System.Linq.Enumerable.OrderByDescending", "OrderByDescending",
                   cbm_type_template(arena, "System.Collections.Generic.IEnumerable",
                                     (const CBMType *[]){cbm_type_type_param(arena, "T"), NULL},
                                     1),
                   cbm_type_template(arena, "System.Linq.IOrderedEnumerable",
                                     (const CBMType *[]){cbm_type_type_param(arena, "T"), NULL},
                                     1));
    REG_EXTENSION("System.Linq.Enumerable.GroupBy", "GroupBy",
                   cbm_type_template(arena, "System.Collections.Generic.IEnumerable",
                                     (const CBMType *[]){cbm_type_type_param(arena, "T"), NULL},
                                     1),
                   cbm_type_template(arena, "System.Collections.Generic.IEnumerable",
                                     (const CBMType *[]){cbm_type_type_param(arena, "T"), NULL},
                                     1));
    REG_EXTENSION("System.Linq.Enumerable.Take", "Take",
                   cbm_type_template(arena, "System.Collections.Generic.IEnumerable",
                                     (const CBMType *[]){cbm_type_type_param(arena, "T"), NULL},
                                     1),
                   cbm_type_template(arena, "System.Collections.Generic.IEnumerable",
                                     (const CBMType *[]){cbm_type_type_param(arena, "T"), NULL},
                                     1));
    REG_EXTENSION("System.Linq.Enumerable.Skip", "Skip",
                   cbm_type_template(arena, "System.Collections.Generic.IEnumerable",
                                     (const CBMType *[]){cbm_type_type_param(arena, "T"), NULL},
                                     1),
                   cbm_type_template(arena, "System.Collections.Generic.IEnumerable",
                                     (const CBMType *[]){cbm_type_type_param(arena, "T"), NULL},
                                     1));
    REG_EXTENSION("System.Linq.Enumerable.ToList", "ToList",
                   cbm_type_template(arena, "System.Collections.Generic.IEnumerable",
                                     (const CBMType *[]){cbm_type_type_param(arena, "T"), NULL},
                                     1),
                   cbm_type_template(arena, "System.Collections.Generic.List",
                                     (const CBMType *[]){cbm_type_type_param(arena, "T"), NULL},
                                     1));
    REG_EXTENSION("System.Linq.Enumerable.ToArray", "ToArray",
                   cbm_type_template(arena, "System.Collections.Generic.IEnumerable",
                                     (const CBMType *[]){cbm_type_type_param(arena, "T"), NULL},
                                     1),
                   cbm_type_template(arena, "System.Array",
                                     (const CBMType *[]){cbm_type_type_param(arena, "T"), NULL},
                                     1));
    REG_EXTENSION("System.Linq.Enumerable.ToDictionary", "ToDictionary",
                   cbm_type_template(arena, "System.Collections.Generic.IEnumerable",
                                     (const CBMType *[]){cbm_type_type_param(arena, "T"), NULL},
                                     1),
                   cbm_type_template(arena, "System.Collections.Generic.Dictionary",
                                     (const CBMType *[]){UNK(), UNK(), NULL}, 2));
    REG_EXTENSION("System.Linq.Enumerable.ToHashSet", "ToHashSet",
                   cbm_type_template(arena, "System.Collections.Generic.IEnumerable",
                                     (const CBMType *[]){cbm_type_type_param(arena, "T"), NULL},
                                     1),
                   cbm_type_template(arena, "System.Collections.Generic.HashSet",
                                     (const CBMType *[]){cbm_type_type_param(arena, "T"), NULL},
                                     1));
    REG_EXTENSION("System.Linq.Enumerable.Distinct", "Distinct",
                   cbm_type_template(arena, "System.Collections.Generic.IEnumerable",
                                     (const CBMType *[]){cbm_type_type_param(arena, "T"), NULL},
                                     1),
                   cbm_type_template(arena, "System.Collections.Generic.IEnumerable",
                                     (const CBMType *[]){cbm_type_type_param(arena, "T"), NULL},
                                     1));
    REG_EXTENSION("System.Linq.Enumerable.Reverse", "Reverse",
                   cbm_type_template(arena, "System.Collections.Generic.IEnumerable",
                                     (const CBMType *[]){cbm_type_type_param(arena, "T"), NULL},
                                     1),
                   cbm_type_template(arena, "System.Collections.Generic.IEnumerable",
                                     (const CBMType *[]){cbm_type_type_param(arena, "T"), NULL},
                                     1));
    REG_EXTENSION("System.Linq.Enumerable.Concat", "Concat",
                   cbm_type_template(arena, "System.Collections.Generic.IEnumerable",
                                     (const CBMType *[]){cbm_type_type_param(arena, "T"), NULL},
                                     1),
                   cbm_type_template(arena, "System.Collections.Generic.IEnumerable",
                                     (const CBMType *[]){cbm_type_type_param(arena, "T"), NULL},
                                     1));
    REG_EXTENSION("System.Linq.Enumerable.Zip", "Zip",
                   cbm_type_template(arena, "System.Collections.Generic.IEnumerable",
                                     (const CBMType *[]){cbm_type_type_param(arena, "T"), NULL},
                                     1),
                   cbm_type_template(arena, "System.Collections.Generic.IEnumerable",
                                     (const CBMType *[]){cbm_type_type_param(arena, "T"), NULL},
                                     1));
    REG_EXTENSION("System.Linq.Enumerable.Aggregate", "Aggregate",
                   cbm_type_template(arena, "System.Collections.Generic.IEnumerable",
                                     (const CBMType *[]){cbm_type_type_param(arena, "T"), NULL},
                                     1),
                   cbm_type_type_param(arena, "T"));
    REG_EXTENSION("System.Linq.Enumerable.Contains", "Contains",
                   cbm_type_template(arena, "System.Collections.Generic.IEnumerable",
                                     (const CBMType *[]){cbm_type_type_param(arena, "T"), NULL},
                                     1),
                   BOOL_T());
    REG_EXTENSION("System.Linq.Enumerable.ElementAt", "ElementAt",
                   cbm_type_template(arena, "System.Collections.Generic.IEnumerable",
                                     (const CBMType *[]){cbm_type_type_param(arena, "T"), NULL},
                                     1),
                   cbm_type_type_param(arena, "T"));
    REG_EXTENSION("System.Linq.Enumerable.ElementAtOrDefault", "ElementAtOrDefault",
                   cbm_type_template(arena, "System.Collections.Generic.IEnumerable",
                                     (const CBMType *[]){cbm_type_type_param(arena, "T"), NULL},
                                     1),
                   cbm_type_type_param(arena, "T"));
    REG_EXTENSION("System.Linq.Enumerable.Cast", "Cast",
                   cbm_type_template(arena, "System.Collections.Generic.IEnumerable",
                                     (const CBMType *[]){cbm_type_type_param(arena, "T"), NULL},
                                     1),
                   cbm_type_template(arena, "System.Collections.Generic.IEnumerable",
                                     (const CBMType *[]){cbm_type_type_param(arena, "T"), NULL},
                                     1));
    REG_EXTENSION("System.Linq.Enumerable.OfType", "OfType",
                   cbm_type_template(arena, "System.Collections.Generic.IEnumerable",
                                     (const CBMType *[]){cbm_type_type_param(arena, "T"), NULL},
                                     1),
                   cbm_type_template(arena, "System.Collections.Generic.IEnumerable",
                                     (const CBMType *[]){cbm_type_type_param(arena, "T"), NULL},
                                     1));

    /* ── System.Json (System.Text.Json) ────────────────────────── */
    REG_TYPE("System.Text.Json.JsonSerializer", "JsonSerializer", false, obj_parent);
    REG_STATIC("System.Text.Json.JsonSerializer", "Serialize", STR());
    REG_STATIC("System.Text.Json.JsonSerializer", "SerializeAsync",
               cbm_type_named(arena, "System.Threading.Tasks.Task"));
    REG_STATIC("System.Text.Json.JsonSerializer", "Deserialize", OBJ());
    REG_STATIC("System.Text.Json.JsonSerializer", "DeserializeAsync",
               cbm_type_named(arena, "System.Threading.Tasks.Task"));
    REG_TYPE("System.Text.Json.JsonDocument", "JsonDocument", false, obj_parent);
    REG_STATIC("System.Text.Json.JsonDocument", "Parse",
               cbm_type_named(arena, "System.Text.Json.JsonDocument"));
    REG_TYPE("System.Text.Json.JsonElement", "JsonElement", false, valuetype_parent);

    /* ── HTTP / networking ─────────────────────────────────────── */
    REG_TYPE("System.Net.Http.HttpClient", "HttpClient", false, obj_parent);
    REG_METHOD("System.Net.Http.HttpClient", "GetAsync",
               cbm_type_named(arena, "System.Threading.Tasks.Task"));
    REG_METHOD("System.Net.Http.HttpClient", "PostAsync",
               cbm_type_named(arena, "System.Threading.Tasks.Task"));
    REG_METHOD("System.Net.Http.HttpClient", "PutAsync",
               cbm_type_named(arena, "System.Threading.Tasks.Task"));
    REG_METHOD("System.Net.Http.HttpClient", "DeleteAsync",
               cbm_type_named(arena, "System.Threading.Tasks.Task"));
    REG_METHOD("System.Net.Http.HttpClient", "GetStringAsync",
               cbm_type_template(arena, "System.Threading.Tasks.Task",
                                 (const CBMType *[]){STR(), NULL}, 1));
    REG_METHOD("System.Net.Http.HttpClient", "SendAsync",
               cbm_type_named(arena, "System.Threading.Tasks.Task"));
    REG_METHOD("System.Net.Http.HttpClient", "Dispose", VOID_T());
    REG_TYPE("System.Net.Http.HttpResponseMessage", "HttpResponseMessage", false, obj_parent);
    REG_METHOD("System.Net.Http.HttpResponseMessage", "EnsureSuccessStatusCode",
               cbm_type_named(arena, "System.Net.Http.HttpResponseMessage"));
    REG_METHOD("System.Net.Http.HttpResponseMessage", "IsSuccessStatusCode", BOOL_T());
    REG_METHOD("System.Net.Http.HttpResponseMessage", "StatusCode", INT());
    REG_METHOD("System.Net.Http.HttpResponseMessage", "Content",
               cbm_type_named(arena, "System.Net.Http.HttpContent"));
    REG_TYPE("System.Net.Http.HttpContent", "HttpContent", false, obj_parent);
    REG_METHOD("System.Net.Http.HttpContent", "ReadAsStringAsync",
               cbm_type_template(arena, "System.Threading.Tasks.Task",
                                 (const CBMType *[]){STR(), NULL}, 1));
    REG_METHOD("System.Net.Http.HttpContent", "ReadAsByteArrayAsync",
               cbm_type_template(arena, "System.Threading.Tasks.Task",
                                 (const CBMType *[]){UNK(), NULL}, 1));
    REG_METHOD("System.Net.Http.HttpContent", "ReadAsStreamAsync",
               cbm_type_template(arena, "System.Threading.Tasks.Task",
                                 (const CBMType *[]){UNK(), NULL}, 1));
    REG_TYPE("System.Net.Http.HttpRequestMessage", "HttpRequestMessage", false, obj_parent);
    REG_TYPE_P1("System.Net.Http.StringContent", "StringContent", false,
                 "System.Net.Http.HttpContent");

    /* ── Logging / DI (corpus-popular) ─────────────────────────── */
    REG_TYPE("Microsoft.Extensions.Logging.ILogger", "ILogger", true, no_parents);
    REG_METHOD("Microsoft.Extensions.Logging.ILogger", "Log", VOID_T());
    REG_METHOD("Microsoft.Extensions.Logging.ILogger", "LogTrace", VOID_T());
    REG_METHOD("Microsoft.Extensions.Logging.ILogger", "LogDebug", VOID_T());
    REG_METHOD("Microsoft.Extensions.Logging.ILogger", "LogInformation", VOID_T());
    REG_METHOD("Microsoft.Extensions.Logging.ILogger", "LogWarning", VOID_T());
    REG_METHOD("Microsoft.Extensions.Logging.ILogger", "LogError", VOID_T());
    REG_METHOD("Microsoft.Extensions.Logging.ILogger", "LogCritical", VOID_T());
    REG_METHOD("Microsoft.Extensions.Logging.ILogger", "BeginScope",
               cbm_type_named(arena, "System.IDisposable"));
    REG_METHOD("Microsoft.Extensions.Logging.ILogger", "IsEnabled", BOOL_T());

    REG_TYPE("Microsoft.Extensions.DependencyInjection.IServiceProvider", "IServiceProvider",
              true, no_parents);
    REG_METHOD("Microsoft.Extensions.DependencyInjection.IServiceProvider", "GetService", OBJ());
    REG_TYPE("Microsoft.Extensions.DependencyInjection.IServiceCollection", "IServiceCollection",
              true, no_parents);

    /* ── ASP.NET Core (compact set) ─────────────────────────────── */
    REG_TYPE("Microsoft.AspNetCore.Mvc.ControllerBase", "ControllerBase", false, obj_parent);
    REG_METHOD("Microsoft.AspNetCore.Mvc.ControllerBase", "Ok",
               cbm_type_named(arena, "Microsoft.AspNetCore.Mvc.OkObjectResult"));
    REG_METHOD("Microsoft.AspNetCore.Mvc.ControllerBase", "NotFound",
               cbm_type_named(arena, "Microsoft.AspNetCore.Mvc.NotFoundObjectResult"));
    REG_METHOD("Microsoft.AspNetCore.Mvc.ControllerBase", "BadRequest",
               cbm_type_named(arena, "Microsoft.AspNetCore.Mvc.BadRequestObjectResult"));
    REG_METHOD("Microsoft.AspNetCore.Mvc.ControllerBase", "StatusCode",
               cbm_type_named(arena, "Microsoft.AspNetCore.Mvc.IActionResult"));
    REG_METHOD("Microsoft.AspNetCore.Mvc.ControllerBase", "Json",
               cbm_type_named(arena, "Microsoft.AspNetCore.Mvc.JsonResult"));
    REG_TYPE_P1("Microsoft.AspNetCore.Mvc.Controller", "Controller", false,
                 "Microsoft.AspNetCore.Mvc.ControllerBase");
    REG_TYPE("Microsoft.AspNetCore.Mvc.OkObjectResult", "OkObjectResult", false, obj_parent);
    REG_TYPE("Microsoft.AspNetCore.Mvc.NotFoundObjectResult", "NotFoundObjectResult", false,
              obj_parent);
    REG_TYPE("Microsoft.AspNetCore.Mvc.BadRequestObjectResult", "BadRequestObjectResult", false,
              obj_parent);
    REG_TYPE("Microsoft.AspNetCore.Mvc.IActionResult", "IActionResult", true, no_parents);
    REG_TYPE("Microsoft.AspNetCore.Mvc.JsonResult", "JsonResult", false, obj_parent);

    /* ── EF Core (compact set) ─────────────────────────────────── */
    REG_TYPE("Microsoft.EntityFrameworkCore.DbContext", "DbContext", false, obj_parent);
    REG_METHOD("Microsoft.EntityFrameworkCore.DbContext", "SaveChanges", INT());
    REG_METHOD("Microsoft.EntityFrameworkCore.DbContext", "SaveChangesAsync",
               cbm_type_template(arena, "System.Threading.Tasks.Task",
                                 (const CBMType *[]){INT(), NULL}, 1));
    REG_METHOD("Microsoft.EntityFrameworkCore.DbContext", "Set", UNK());
    REG_METHOD("Microsoft.EntityFrameworkCore.DbContext", "Database", UNK());
    REG_GENERIC_TYPE("Microsoft.EntityFrameworkCore.DbSet", "DbSet", false, obj_parent,
                      single_t_params);
    REG_METHOD("Microsoft.EntityFrameworkCore.DbSet", "Add", UNK());
    REG_METHOD("Microsoft.EntityFrameworkCore.DbSet", "Remove", UNK());
    REG_METHOD("Microsoft.EntityFrameworkCore.DbSet", "Update", UNK());
    REG_METHOD("Microsoft.EntityFrameworkCore.DbSet", "Find", cbm_type_type_param(arena, "T"));
    REG_METHOD("Microsoft.EntityFrameworkCore.DbSet", "FindAsync",
               cbm_type_template(arena, "System.Threading.Tasks.ValueTask",
                                 (const CBMType *[]){cbm_type_type_param(arena, "T"), NULL}, 1));

    /* ── Newtonsoft.Json (popular alternative) ─────────────────── */
    REG_TYPE("Newtonsoft.Json.JsonConvert", "JsonConvert", false, obj_parent);
    REG_STATIC("Newtonsoft.Json.JsonConvert", "SerializeObject", STR());
    REG_STATIC("Newtonsoft.Json.JsonConvert", "DeserializeObject", OBJ());

    /* ── Random / Guid / Lazy ──────────────────────────────────── */
    REG_TYPE("System.Random", "Random", false, obj_parent);
    REG_METHOD("System.Random", "Next", INT());
    REG_METHOD("System.Random", "NextDouble", DBL());
    REG_METHOD("System.Random", "NextBytes", VOID_T());
    REG_GENERIC_TYPE("System.Lazy", "Lazy", false, obj_parent, single_t_params);
    REG_METHOD("System.Lazy", "Value", cbm_type_type_param(arena, "T"));
    REG_METHOD("System.Lazy", "IsValueCreated", BOOL_T());

    /* ── System.Action / Func / Predicate ──────────────────────── */
    REG_TYPE_P1("System.Action", "Action", false, "System.Delegate");
    REG_GENERIC_TYPE_P1("System.Action", "Action", false,
                         "System.Delegate", single_t_params);
    REG_METHOD("System.Action", "Invoke", VOID_T());
    REG_GENERIC_TYPE_P1("System.Func", "Func", false,
                         "System.Delegate", single_t_params);
    REG_METHOD("System.Func", "Invoke", cbm_type_type_param(arena, "T"));
    REG_GENERIC_TYPE_P1("System.Predicate", "Predicate", false,
                         "System.Delegate", single_t_params);
    REG_METHOD("System.Predicate", "Invoke", BOOL_T());
    REG_GENERIC_TYPE_P1("System.Comparison", "Comparison", false,
                         "System.Delegate", single_t_params);
    REG_METHOD("System.Comparison", "Invoke", INT());
    REG_GENERIC_TYPE_P1("System.EventHandler", "EventHandler", false,
                         "System.Delegate", single_t_params);

    /* ── System.Tuple / ValueTuple ─────────────────────────────── */
    REG_GENERIC_TYPE("System.Tuple", "Tuple", false, obj_parent, single_t_params);
    REG_METHOD("System.Tuple", "Item1", cbm_type_type_param(arena, "T"));
    REG_GENERIC_TYPE("System.ValueTuple", "ValueTuple", false, valuetype_parent, single_t_params);

    /* ── Misc commonly-used ────────────────────────────────────── */
    REG_TYPE("System.Environment", "Environment", false, obj_parent);
    REG_STATIC("System.Environment", "GetEnvironmentVariable", STR());
    REG_STATIC("System.Environment", "SetEnvironmentVariable", VOID_T());
    REG_STATIC("System.Environment", "Exit", VOID_T());
    REG_STATIC("System.Environment", "GetFolderPath", STR());
    REG_STATIC("System.Environment", "MachineName", STR());
    REG_STATIC("System.Environment", "UserName", STR());

    REG_TYPE("System.Diagnostics.Debug", "Debug", false, obj_parent);
    REG_STATIC("System.Diagnostics.Debug", "WriteLine", VOID_T());
    REG_STATIC("System.Diagnostics.Debug", "Assert", VOID_T());
    REG_TYPE("System.Diagnostics.Trace", "Trace", false, obj_parent);
    REG_STATIC("System.Diagnostics.Trace", "WriteLine", VOID_T());

    REG_TYPE("System.Diagnostics.Stopwatch", "Stopwatch", false, obj_parent);
    REG_STATIC("System.Diagnostics.Stopwatch", "StartNew",
               cbm_type_named(arena, "System.Diagnostics.Stopwatch"));
    REG_METHOD("System.Diagnostics.Stopwatch", "Start", VOID_T());
    REG_METHOD("System.Diagnostics.Stopwatch", "Stop", VOID_T());
    REG_METHOD("System.Diagnostics.Stopwatch", "Elapsed",
               cbm_type_named(arena, "System.TimeSpan"));
    REG_METHOD("System.Diagnostics.Stopwatch", "ElapsedMilliseconds", LONG_T());

    /* xUnit (popular test framework, helpful for resolving test files) */
    REG_TYPE("Xunit.Assert", "Assert", false, obj_parent);
    REG_STATIC("Xunit.Assert", "Equal", VOID_T());
    REG_STATIC("Xunit.Assert", "NotEqual", VOID_T());
    REG_STATIC("Xunit.Assert", "True", VOID_T());
    REG_STATIC("Xunit.Assert", "False", VOID_T());
    REG_STATIC("Xunit.Assert", "Null", VOID_T());
    REG_STATIC("Xunit.Assert", "NotNull", VOID_T());
    REG_STATIC("Xunit.Assert", "Throws", UNK());
    REG_STATIC("Xunit.Assert", "ThrowsAsync",
               cbm_type_named(arena, "System.Threading.Tasks.Task"));
    REG_STATIC("Xunit.Assert", "Contains", VOID_T());
    REG_STATIC("Xunit.Assert", "DoesNotContain", VOID_T());
    REG_STATIC("Xunit.Assert", "Empty", VOID_T());
    REG_STATIC("Xunit.Assert", "NotEmpty", VOID_T());
    REG_STATIC("Xunit.Assert", "Single", UNK());
    REG_STATIC("Xunit.Assert", "Same", VOID_T());
    REG_STATIC("Xunit.Assert", "NotSame", VOID_T());
    REG_STATIC("Xunit.Assert", "IsType", UNK());
    REG_STATIC("Xunit.Assert", "IsAssignableFrom", UNK());

    /* NUnit */
    REG_TYPE("NUnit.Framework.Assert", "Assert", false, obj_parent);
    REG_STATIC("NUnit.Framework.Assert", "AreEqual", VOID_T());
    REG_STATIC("NUnit.Framework.Assert", "IsTrue", VOID_T());
    REG_STATIC("NUnit.Framework.Assert", "IsFalse", VOID_T());
    REG_STATIC("NUnit.Framework.Assert", "IsNull", VOID_T());
    REG_STATIC("NUnit.Framework.Assert", "IsNotNull", VOID_T());
    REG_STATIC("NUnit.Framework.Assert", "Throws", UNK());

    /* MSTest */
    REG_TYPE("Microsoft.VisualStudio.TestTools.UnitTesting.Assert", "Assert", false, obj_parent);
    REG_STATIC("Microsoft.VisualStudio.TestTools.UnitTesting.Assert", "AreEqual", VOID_T());
    REG_STATIC("Microsoft.VisualStudio.TestTools.UnitTesting.Assert", "IsTrue", VOID_T());
    REG_STATIC("Microsoft.VisualStudio.TestTools.UnitTesting.Assert", "IsFalse", VOID_T());
    REG_STATIC("Microsoft.VisualStudio.TestTools.UnitTesting.Assert", "IsNull", VOID_T());
    REG_STATIC("Microsoft.VisualStudio.TestTools.UnitTesting.Assert", "IsNotNull", VOID_T());

    /* End */
}
