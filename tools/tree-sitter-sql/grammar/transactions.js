export default {

  transaction: $ => seq(
    $.keyword_begin,
    optional(
      $.keyword_transaction,
    ),
    optional(';'),
    repeat(
      seq(
        $.statement,
        ';'
      ),
    ),
    choice(
      $._commit,
      $._rollback,
    ),
  ),

  _commit: $ => seq(
    $.keyword_commit,
    optional(
      $.keyword_transaction,
    ),
  ),

  _rollback: $ => seq(
    $.keyword_rollback,
    optional(
      $.keyword_transaction,
    ),
  ),

};
