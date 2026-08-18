/*
 * ruby_stdlib_data.c — hand-written Ruby core + curated Rails type data.
 *
 * Strategy mirrors perl_stdlib_data.c / php_stdlib_data.c:
 *   1. Ruby core classes (String, Array, Hash, Integer, ...) registered as
 *      types with receiver-keyed instance methods whose return types enable
 *      call-chain typing ("a,b".split(",").first). Method QNs are bare
 *      ("String.upcase") — no project prefix — so they can never collide
 *      with an indexed def; they exist for TYPING, not for edge emission
 *      (the resolver only emits edges to project-defined methods).
 *   2. Kernel built-ins (puts, require, raise, ...) as global functions so
 *      bare calls to them type-check without producing spurious edges.
 *   3. A curated Rails surface: ActiveRecord::Base class-side query methods
 *      and instance persistence methods, ActionController::Base helpers,
 *      and ActiveSupport-style predicates on Object. Registered under
 *      dotted QNs ("ActiveRecord.Base") to match ruby path dotting
 *      (Foo::Bar -> Foo.Bar). The resolver special-cases ActiveRecord
 *      query methods to return the *receiving model's* type (User.find ->
 *      User), which these generic entries cannot express.
 *
 * Return types are left UNKNOWN where the real type is polymorphic; the
 * seed only needs to be right where chaining matters.
 */

#include "../type_rep.h"
#include "../type_registry.h"
#include "../../arena.h"
#include "../ruby_lsp.h"
#include <string.h>

#define MIXED cbm_type_unknown()

/* Register a type by bare dotted QN (e.g. "String", "ActiveRecord.Base"). */
#define REG_TYPE(qn_)                   \
    do {                                \
        CBMRegisteredType rt;           \
        memset(&rt, 0, sizeof(rt));     \
        rt.qualified_name = (qn_);      \
        rt.short_name = (qn_);          \
        cbm_registry_add_type(reg, rt); \
    } while (0)

/* Register an instance method on receiver type `recv_` returning `ret_`. */
#define REG_METHOD(recv_, name_, ret_)                                                          \
    do {                                                                                        \
        memset(&rf, 0, sizeof(rf));                                                             \
        rf.min_params = -1;                                                                     \
        rf.qualified_name = cbm_arena_sprintf(arena, "%s.%s", (recv_), (name_));                \
        rf.short_name = (name_);                                                                \
        rf.receiver_type = (recv_);                                                             \
        {                                                                                       \
            const CBMType **rets = (const CBMType **)cbm_arena_alloc(arena, 2 * sizeof(*rets)); \
            rets[0] = (ret_);                                                                   \
            rets[1] = NULL;                                                                     \
            rf.signature = cbm_type_func(arena, NULL, NULL, rets);                              \
        }                                                                                       \
        cbm_registry_add_func(reg, rf);                                                         \
    } while (0)

/* Register a singleton (class-side) method: receiver key "<qn>.self"
 * matches ruby_lookup_singleton_method's keying convention. */
#define REG_SMETHOD(recv_, name_, ret_)                                                         \
    do {                                                                                        \
        memset(&rf, 0, sizeof(rf));                                                             \
        rf.min_params = -1;                                                                     \
        rf.qualified_name = cbm_arena_sprintf(arena, "%s.%s", (recv_), (name_));                \
        rf.short_name = (name_);                                                                \
        rf.receiver_type = cbm_arena_sprintf(arena, "%s.self", (recv_));                        \
        {                                                                                       \
            const CBMType **rets = (const CBMType **)cbm_arena_alloc(arena, 2 * sizeof(*rets)); \
            rets[0] = (ret_);                                                                   \
            rets[1] = NULL;                                                                     \
            rf.signature = cbm_type_func(arena, NULL, NULL, rets);                              \
        }                                                                                       \
        cbm_registry_add_func(reg, rf);                                                         \
    } while (0)

/* Register a global (Kernel) built-in function returning `ret_`. */
#define REG_BUILTIN(name_, ret_)                                                                \
    do {                                                                                        \
        memset(&rf, 0, sizeof(rf));                                                             \
        rf.min_params = -1;                                                                     \
        rf.qualified_name = (name_);                                                            \
        rf.short_name = (name_);                                                                \
        {                                                                                       \
            const CBMType **rets = (const CBMType **)cbm_arena_alloc(arena, 2 * sizeof(*rets)); \
            rets[0] = (ret_);                                                                   \
            rets[1] = NULL;                                                                     \
            rf.signature = cbm_type_func(arena, NULL, NULL, rets);                              \
        }                                                                                       \
        cbm_registry_add_func(reg, rf);                                                         \
    } while (0)

void cbm_ruby_stdlib_register(CBMTypeRegistry *reg, CBMArena *arena) {
    CBMRegisteredFunc rf;

    const CBMType *T_STRING = cbm_type_named(arena, "String");
    const CBMType *T_ARRAY = cbm_type_named(arena, "Array");
    const CBMType *T_HASH = cbm_type_named(arena, "Hash");
    const CBMType *T_INT = cbm_type_named(arena, "Integer");
    const CBMType *T_FLOAT = cbm_type_named(arena, "Float");
    const CBMType *T_SYM = cbm_type_named(arena, "Symbol");
    const CBMType *T_BOOL = cbm_type_builtin(arena, "bool");

    /* ── Core class constants (resolvable as superclasses / receivers) ── */
    static const char *core_types[] = {"Object",
                                       "BasicObject",
                                       "String",
                                       "Array",
                                       "Hash",
                                       "Integer",
                                       "Float",
                                       "Numeric",
                                       "Symbol",
                                       "Range",
                                       "Regexp",
                                       "Time",
                                       "Date",
                                       "DateTime",
                                       "File",
                                       "IO",
                                       "Dir",
                                       "Struct",
                                       "OpenStruct",
                                       "StringIO",
                                       "Set",
                                       "Proc",
                                       "Method",
                                       "Thread",
                                       "Mutex",
                                       "Queue",
                                       "Rational",
                                       "Complex",
                                       "NilClass",
                                       "TrueClass",
                                       "FalseClass",
                                       "Exception",
                                       "StandardError",
                                       "RuntimeError",
                                       "ArgumentError",
                                       "TypeError",
                                       "NameError",
                                       "NoMethodError",
                                       "IOError",
                                       "KeyError",
                                       "IndexError",
                                       "RangeError",
                                       "NotImplementedError",
                                       "StopIteration",
                                       "Comparable",
                                       "Enumerable",
                                       "Kernel",
                                       "Math",
                                       "JSON",
                                       "ERB",
                                       "Logger",
                                       "Enumerator",
                                       NULL};
    for (int i = 0; core_types[i]; i++)
        REG_TYPE(core_types[i]);

    /* ── String ── */
    REG_METHOD("String", "upcase", T_STRING);
    REG_METHOD("String", "downcase", T_STRING);
    REG_METHOD("String", "capitalize", T_STRING);
    REG_METHOD("String", "strip", T_STRING);
    REG_METHOD("String", "chomp", T_STRING);
    REG_METHOD("String", "gsub", T_STRING);
    REG_METHOD("String", "sub", T_STRING);
    REG_METHOD("String", "split", T_ARRAY);
    REG_METHOD("String", "chars", T_ARRAY);
    REG_METHOD("String", "lines", T_ARRAY);
    REG_METHOD("String", "bytes", T_ARRAY);
    REG_METHOD("String", "to_s", T_STRING);
    REG_METHOD("String", "to_str", T_STRING);
    REG_METHOD("String", "to_i", T_INT);
    REG_METHOD("String", "to_f", T_FLOAT);
    REG_METHOD("String", "to_sym", T_SYM);
    REG_METHOD("String", "length", T_INT);
    REG_METHOD("String", "size", T_INT);
    REG_METHOD("String", "reverse", T_STRING);
    REG_METHOD("String", "include?", T_BOOL);
    REG_METHOD("String", "start_with?", T_BOOL);
    REG_METHOD("String", "end_with?", T_BOOL);
    REG_METHOD("String", "empty?", T_BOOL);
    REG_METHOD("String", "match?", T_BOOL);
    REG_METHOD("String", "freeze", T_STRING);
    REG_METHOD("String", "dup", T_STRING);

    /* ── Array ── */
    REG_METHOD("Array", "map", T_ARRAY);
    REG_METHOD("Array", "flat_map", T_ARRAY);
    REG_METHOD("Array", "select", T_ARRAY);
    REG_METHOD("Array", "filter", T_ARRAY);
    REG_METHOD("Array", "reject", T_ARRAY);
    REG_METHOD("Array", "sort", T_ARRAY);
    REG_METHOD("Array", "sort_by", T_ARRAY);
    REG_METHOD("Array", "uniq", T_ARRAY);
    REG_METHOD("Array", "compact", T_ARRAY);
    REG_METHOD("Array", "flatten", T_ARRAY);
    REG_METHOD("Array", "reverse", T_ARRAY);
    REG_METHOD("Array", "concat", T_ARRAY);
    REG_METHOD("Array", "push", T_ARRAY);
    REG_METHOD("Array", "join", T_STRING);
    REG_METHOD("Array", "first", MIXED);
    REG_METHOD("Array", "last", MIXED);
    REG_METHOD("Array", "sample", MIXED);
    REG_METHOD("Array", "length", T_INT);
    REG_METHOD("Array", "size", T_INT);
    REG_METHOD("Array", "count", T_INT);
    REG_METHOD("Array", "sum", MIXED);
    REG_METHOD("Array", "min", MIXED);
    REG_METHOD("Array", "max", MIXED);
    REG_METHOD("Array", "include?", T_BOOL);
    REG_METHOD("Array", "empty?", T_BOOL);
    REG_METHOD("Array", "any?", T_BOOL);
    REG_METHOD("Array", "all?", T_BOOL);
    REG_METHOD("Array", "none?", T_BOOL);
    REG_METHOD("Array", "each", T_ARRAY);
    REG_METHOD("Array", "each_with_index", T_ARRAY);
    REG_METHOD("Array", "to_a", T_ARRAY);
    REG_METHOD("Array", "group_by", T_HASH);
    REG_METHOD("Array", "each_slice", MIXED);
    REG_METHOD("Array", "zip", T_ARRAY);

    /* ── Hash ── */
    REG_METHOD("Hash", "keys", T_ARRAY);
    REG_METHOD("Hash", "values", T_ARRAY);
    REG_METHOD("Hash", "merge", T_HASH);
    REG_METHOD("Hash", "merge!", T_HASH);
    REG_METHOD("Hash", "fetch", MIXED);
    REG_METHOD("Hash", "dig", MIXED);
    REG_METHOD("Hash", "key?", T_BOOL);
    REG_METHOD("Hash", "has_key?", T_BOOL);
    REG_METHOD("Hash", "include?", T_BOOL);
    REG_METHOD("Hash", "empty?", T_BOOL);
    REG_METHOD("Hash", "size", T_INT);
    REG_METHOD("Hash", "length", T_INT);
    REG_METHOD("Hash", "count", T_INT);
    REG_METHOD("Hash", "each", T_HASH);
    REG_METHOD("Hash", "map", T_ARRAY);
    REG_METHOD("Hash", "select", T_HASH);
    REG_METHOD("Hash", "reject", T_HASH);
    REG_METHOD("Hash", "to_a", T_ARRAY);
    REG_METHOD("Hash", "to_h", T_HASH);
    REG_METHOD("Hash", "transform_values", T_HASH);
    REG_METHOD("Hash", "transform_keys", T_HASH);
    REG_METHOD("Hash", "symbolize_keys", T_HASH); /* ActiveSupport */
    REG_METHOD("Hash", "stringify_keys", T_HASH); /* ActiveSupport */
    REG_METHOD("Hash", "deep_symbolize_keys", T_HASH);
    REG_METHOD("Hash", "with_indifferent_access", T_HASH);

    /* ── Integer / Float ── */
    REG_METHOD("Integer", "to_s", T_STRING);
    REG_METHOD("Integer", "to_i", T_INT);
    REG_METHOD("Integer", "to_f", T_FLOAT);
    REG_METHOD("Integer", "times", MIXED);
    REG_METHOD("Integer", "upto", MIXED);
    REG_METHOD("Integer", "abs", T_INT);
    REG_METHOD("Integer", "zero?", T_BOOL);
    REG_METHOD("Integer", "positive?", T_BOOL);
    REG_METHOD("Integer", "negative?", T_BOOL);
    REG_METHOD("Integer", "even?", T_BOOL);
    REG_METHOD("Integer", "odd?", T_BOOL);
    REG_METHOD("Float", "to_s", T_STRING);
    REG_METHOD("Float", "to_i", T_INT);
    REG_METHOD("Float", "round", T_INT);
    REG_METHOD("Float", "ceil", T_INT);
    REG_METHOD("Float", "floor", T_INT);
    REG_METHOD("Float", "abs", T_FLOAT);

    /* ── Symbol / misc ── */
    REG_METHOD("Symbol", "to_s", T_STRING);
    REG_METHOD("Symbol", "to_sym", T_SYM);
    REG_METHOD("Symbol", "to_proc", cbm_type_named(arena, "Proc"));
    REG_METHOD("Range", "to_a", T_ARRAY);
    REG_METHOD("Range", "each", MIXED);
    REG_METHOD("Range", "map", T_ARRAY);
    REG_METHOD("Range", "include?", T_BOOL);
    REG_METHOD("Time", "to_s", T_STRING);
    REG_METHOD("Time", "to_i", T_INT);
    REG_METHOD("Time", "strftime", T_STRING);
    REG_METHOD("Time", "year", T_INT);
    REG_METHOD("Time", "month", T_INT);
    REG_METHOD("Time", "day", T_INT);
    REG_SMETHOD("Time", "now", cbm_type_named(arena, "Time"));
    REG_SMETHOD("Time", "at", cbm_type_named(arena, "Time"));
    REG_SMETHOD("Time", "parse", cbm_type_named(arena, "Time"));
    REG_SMETHOD("Time", "current", cbm_type_named(arena, "Time")); /* ActiveSupport */
    REG_SMETHOD("Time", "zone", MIXED);
    REG_SMETHOD("File", "read", T_STRING);
    REG_SMETHOD("File", "readlines", T_ARRAY);
    REG_SMETHOD("File", "open", cbm_type_named(arena, "File"));
    REG_SMETHOD("File", "exist?", T_BOOL);
    REG_SMETHOD("File", "basename", T_STRING);
    REG_SMETHOD("File", "dirname", T_STRING);
    REG_SMETHOD("File", "join", T_STRING);
    REG_SMETHOD("File", "expand_path", T_STRING);
    REG_SMETHOD("JSON", "parse", MIXED);
    REG_SMETHOD("JSON", "generate", T_STRING);
    REG_SMETHOD("JSON", "pretty_generate", T_STRING);
    REG_SMETHOD("Math", "sqrt", T_FLOAT);
    REG_SMETHOD("Math", "pow", T_FLOAT);
    REG_SMETHOD("Math", "sin", T_FLOAT);
    REG_SMETHOD("Math", "cos", T_FLOAT);
    REG_SMETHOD("Math", "log", T_FLOAT);

    /* ── Object (universal receivers; ActiveSupport predicates) ── */
    REG_METHOD("Object", "to_s", T_STRING);
    REG_METHOD("Object", "inspect", T_STRING);
    REG_METHOD("Object", "freeze", MIXED);
    REG_METHOD("Object", "frozen?", T_BOOL);
    REG_METHOD("Object", "dup", MIXED);
    REG_METHOD("Object", "clone", MIXED);
    REG_METHOD("Object", "tap", MIXED);
    REG_METHOD("Object", "then", MIXED);
    REG_METHOD("Object", "nil?", T_BOOL);
    REG_METHOD("Object", "is_a?", T_BOOL);
    REG_METHOD("Object", "kind_of?", T_BOOL);
    REG_METHOD("Object", "instance_of?", T_BOOL);
    REG_METHOD("Object", "respond_to?", T_BOOL);
    REG_METHOD("Object", "blank?", T_BOOL);    /* ActiveSupport */
    REG_METHOD("Object", "present?", T_BOOL);  /* ActiveSupport */
    REG_METHOD("Object", "presence", MIXED);   /* ActiveSupport */
    REG_METHOD("Object", "to_json", T_STRING); /* ActiveSupport / json */

    /* ── Kernel built-ins (global functions) ── */
    REG_BUILTIN("puts", MIXED);
    REG_BUILTIN("print", MIXED);
    REG_BUILTIN("p", MIXED);
    REG_BUILTIN("pp", MIXED);
    REG_BUILTIN("require", T_BOOL);
    REG_BUILTIN("require_relative", T_BOOL);
    REG_BUILTIN("raise", MIXED);
    REG_BUILTIN("fail", MIXED);
    REG_BUILTIN("loop", MIXED);
    REG_BUILTIN("sleep", T_INT);
    REG_BUILTIN("rand", MIXED);
    REG_BUILTIN("format", T_STRING);
    REG_BUILTIN("sprintf", T_STRING);
    REG_BUILTIN("gets", T_STRING);
    REG_BUILTIN("exit", MIXED);
    REG_BUILTIN("abort", MIXED);
    REG_BUILTIN("lambda", cbm_type_named(arena, "Proc"));
    REG_BUILTIN("proc", cbm_type_named(arena, "Proc"));
    REG_BUILTIN("binding", MIXED);
    REG_BUILTIN("catch", MIXED);
    REG_BUILTIN("throw", MIXED);
    REG_BUILTIN("attr_accessor", MIXED);
    REG_BUILTIN("attr_reader", MIXED);
    REG_BUILTIN("attr_writer", MIXED);

    /* ── Rails: ActiveRecord::Base ──────────────────────────────────
     * Class-side query interface. Return types here are generic; the
     * resolver's AR special case substitutes the receiving model's type
     * (User.find -> User) when the receiver's ancestry reaches
     * ActiveRecord.Base / ApplicationRecord. */
    REG_TYPE("ActiveRecord.Base");
    REG_TYPE("ApplicationRecord");
    static const char *ar_class_methods[] = {"find",
                                             "find_by",
                                             "find_by!",
                                             "find_or_create_by",
                                             "find_or_initialize_by",
                                             "where",
                                             "all",
                                             "first",
                                             "last",
                                             "take",
                                             "create",
                                             "create!",
                                             "new",
                                             "count",
                                             "order",
                                             "limit",
                                             "offset",
                                             "joins",
                                             "left_joins",
                                             "includes",
                                             "preload",
                                             "eager_load",
                                             "group",
                                             "having",
                                             "distinct",
                                             "pluck",
                                             "ids",
                                             "exists?",
                                             "none",
                                             "unscoped",
                                             "select",
                                             "sum",
                                             "maximum",
                                             "minimum",
                                             "average",
                                             "destroy_all",
                                             "delete_all",
                                             "update_all",
                                             "transaction",
                                             "find_each",
                                             NULL};
    for (int i = 0; ar_class_methods[i]; i++)
        REG_SMETHOD("ActiveRecord.Base", ar_class_methods[i], MIXED);
    REG_METHOD("ActiveRecord.Base", "save", T_BOOL);
    REG_METHOD("ActiveRecord.Base", "save!", T_BOOL);
    REG_METHOD("ActiveRecord.Base", "update", T_BOOL);
    REG_METHOD("ActiveRecord.Base", "update!", T_BOOL);
    REG_METHOD("ActiveRecord.Base", "destroy", MIXED);
    REG_METHOD("ActiveRecord.Base", "destroy!", MIXED);
    REG_METHOD("ActiveRecord.Base", "delete", MIXED);
    REG_METHOD("ActiveRecord.Base", "reload", MIXED);
    REG_METHOD("ActiveRecord.Base", "valid?", T_BOOL);
    REG_METHOD("ActiveRecord.Base", "invalid?", T_BOOL);
    REG_METHOD("ActiveRecord.Base", "persisted?", T_BOOL);
    REG_METHOD("ActiveRecord.Base", "new_record?", T_BOOL);
    REG_METHOD("ActiveRecord.Base", "errors", MIXED);
    REG_METHOD("ActiveRecord.Base", "attributes", T_HASH);
    REG_METHOD("ActiveRecord.Base", "assign_attributes", MIXED);
    REG_METHOD("ActiveRecord.Base", "update_attribute", T_BOOL);
    REG_METHOD("ActiveRecord.Base", "touch", T_BOOL);
    REG_METHOD("ActiveRecord.Base", "id", T_INT);

    /* ── Rails: ActionController::Base / ::API ── */
    REG_TYPE("ActionController.Base");
    REG_TYPE("ActionController.API");
    static const char *ac_receivers[] = {"ActionController.Base", "ActionController.API", NULL};
    for (int i = 0; ac_receivers[i]; i++) {
        const char *acr = ac_receivers[i];
        REG_METHOD(acr, "render", MIXED);
        REG_METHOD(acr, "redirect_to", MIXED);
        REG_METHOD(acr, "head", MIXED);
        REG_METHOD(acr, "params", MIXED);
        REG_METHOD(acr, "session", MIXED);
        REG_METHOD(acr, "cookies", MIXED);
        REG_METHOD(acr, "request", MIXED);
        REG_METHOD(acr, "response", MIXED);
        REG_METHOD(acr, "respond_to", MIXED);
        REG_METHOD(acr, "flash", MIXED);
    }
    REG_SMETHOD("ActionController.Base", "before_action", MIXED);
    REG_SMETHOD("ActionController.Base", "after_action", MIXED);
    REG_SMETHOD("ActionController.Base", "skip_before_action", MIXED);
    REG_SMETHOD("ActionController.API", "before_action", MIXED);
    REG_SMETHOD("ActionController.API", "after_action", MIXED);

    /* ── Rails: jobs / mailers (constants only, for superclass chains) ── */
    REG_TYPE("ActiveJob.Base");
    REG_TYPE("ApplicationJob");
    REG_TYPE("ActionMailer.Base");
    REG_TYPE("ApplicationMailer");
    REG_TYPE("ApplicationController");
    REG_SMETHOD("ActiveJob.Base", "perform_later", MIXED);
    REG_SMETHOD("ActiveJob.Base", "perform_now", MIXED);
    REG_SMETHOD("ActiveJob.Base", "set", MIXED);
    REG_METHOD("ActionMailer.Base", "mail", MIXED);
    REG_SMETHOD("ActionMailer.Base", "with", MIXED);

    /* ── Minitest/RSpec anchors (superclass chains in test files) ── */
    REG_TYPE("Minitest.Test");
    REG_TYPE("ActiveSupport.TestCase");
    REG_TYPE("ActionDispatch.IntegrationTest");
}
