import { comma_list } from "../helpers.js";

export default {

  set_statement: $ => seq(
    $.keyword_set,
    choice(
      seq(
        optional(choice($.keyword_session, $.keyword_local)),
        choice(
          seq(
            $.object_reference,
            choice($.keyword_to, '='),
            choice(
              $.literal,
              $.keyword_default,
              $.identifier,
              $.keyword_on,
              $.keyword_off,
            ),
          ),
          seq($.keyword_schema, $.literal),
          seq($.keyword_names, $.literal),
          seq($.keyword_time, $.keyword_zone, choice($.literal, $.keyword_local, $.keyword_default)),
          seq($.keyword_session, $.keyword_authorization, choice($.identifier, $.keyword_default)),
          seq($.keyword_role, choice($.identifier, $.keyword_none)),
        ),
      ),
      seq($.keyword_constraints, choice($.keyword_all, comma_list($.identifier, true)), choice($.keyword_deferred, $.keyword_immediate)),
      seq($.keyword_transaction, $._transaction_mode),
      seq($.keyword_transaction, $.keyword_snapshot, $._transaction_mode),
      seq($.keyword_session, $.keyword_characteristics, $.keyword_as, $.keyword_transaction, $._transaction_mode),
    ),
  ),

  _transaction_mode: $ => seq(
    $.keyword_isolation,
    $.keyword_level,
    choice(
      $.keyword_serializable,
      seq($.keyword_repeatable, $.keyword_read),
      seq($.keyword_read, $.keyword_committed),
      seq($.keyword_read, $.keyword_uncommitted),
    ),
    choice(
      seq($.keyword_read, $.keyword_write),
      seq($.keyword_read, $.keyword_only),
    ),
    optional($.keyword_not),
    $.keyword_deferrable,
  ),

  reset_statement: $ => seq(
    $.keyword_reset,
    choice(
      $.object_reference,
      $.keyword_all,
      seq($.keyword_session, $.keyword_authorization),
      $.keyword_role,
    ),
  ),

};
