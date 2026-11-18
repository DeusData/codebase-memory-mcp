module.exports = {

  precedences: $ => [

    // ------------------------------------------------
    // associativity of expressions, patterns and types
    // ------------------------------------------------

    [
      'projection',
      'record',
      'prefix',
      'apply',
      'negation',
      'infix',
      'implicit',
      'fun',
      'annotated',
      $.quantified_type,
    ],

    // ------------------------------------------------
    // negation
    // ------------------------------------------------

    [
      $._pat_negation,
      $.literal,
    ],

    // ------------------------------------------------
    // function vs bind
    // ------------------------------------------------

    [
      'bind',
      'pat-name',
    ],

    // ------------------------------------------------
    // qualified names
    // ------------------------------------------------

    /**
     * Prioritize shifting over a tight infix dot over reducing to a qualified constructor or module.
     */
    [
      'qualifying-module',
      'qualified-id',
    ],

    /**
     * Prioritize qualified variables over record field projection on constructors.
     */
    [
      'qualifying-module',
      'con',
    ],

    /**
     * Prioritize qualified names over the infix operator dot in types.
     */
    [
      'qualifying-module',
      'type-name',
    ],

    // ------------------------------------------------
    // types and constraints
    // ------------------------------------------------

    [
      $.operator,
      $._type_star,
    ],

    [
      $._type_wildcard,
      $._type_param_wildcard,
    ],

    [
      $._constructor_ticked,
      $._tycon_ticked,
    ],

    [
      'qtype-single',
      'qtype-curried',
    ],

    // ------------------------------------------------
    // misc
    // ------------------------------------------------

    /**
     * > f A a a = a
     * > data A = A Int Int
     *
     * This should not be parsed as constructor application, but separate patterns.
     */
    [
      'patterns',
      'apply',
    ],

  ],

}
