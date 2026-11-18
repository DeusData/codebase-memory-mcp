module.exports = {

  inline: $ => [

    // ------------------------------------------------
    // variable
    // ------------------------------------------------

    $._var,
    $._vars,
    $._varids,
    $._varids_ticked,
    $._varop,

    // ------------------------------------------------
    // constructor
    // ------------------------------------------------

    $._constructor,
    $._con,
    $._qcon,
    $._cons,
    $._conids,
    $._conids_ticked,
    $._conop,
    $._op_ticked,
    $._modid,

    // ------------------------------------------------
    // operator
    // ------------------------------------------------

    $._qvarsym,
    $._qconsym,
    $._sym,
    $._qsym,
    $._pqsym,
    $._any_prefix_dot,
    $._any_tight_dot,
    $._unboxed_bar,

    // ------------------------------------------------
    // expression
    // ------------------------------------------------

    $._exp_name,
    $._exp_greedy,
    $._let,

    // ------------------------------------------------
    // pattern
    // ------------------------------------------------

    $._pat_apply_arg,
    $._pat_name,
    $._pat_texp,

    // ------------------------------------------------
    // type
    // ------------------------------------------------

    $._tyconid,
    $._tyconids,
    $._tycon,
    $._qtycon,
    $._tycons,
    $._tyconops,
    $._tyops,

    $._type_name,
    $._forall,
    $._type_apply_arg,
    $._parameter_type,
    $._field_type,
    $._type_head,
    $._type_instance_head,
    $._type_annotation,
    $._kind_annotation,

    // ------------------------------------------------
    // literal
    // ------------------------------------------------

    $._number,
    $._stringly,
    $._unit_cons,
    $._tuple_cons,
    $._universal,

    // ------------------------------------------------
    // decl
    // ------------------------------------------------

    $._function_head_patterns,
    $._function_head,

  ],

}
