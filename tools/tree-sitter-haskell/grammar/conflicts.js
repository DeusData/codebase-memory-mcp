module.exports = {

  conflicts: $ => [

    /**
     * For reference in GHC:
     * - Note [Ambiguous syntactic categories]
     * - Note [PatBuilder]
     * - Note [Declaration/signature overlap]
     * - These correspond to `DisambECP`
     *
     * (fun x) y = undefined
     * (fun x -> y) = undefined
     * (fun) <> x = undefined
     *
     * The first one is a regular function with some redundant parens, where `fun` is the declared name.
     * The second one is a pattern binder with a view pattern, where `fun` is a free variable.
     * The third one is an infix pattern binder, where `fun` is a simple varid pattern with redundant parens.
     *
     * These conflicts are also relevant for top-level expression splices, which fundamentally conflict with decls, and
     * since decls start with either `var` or `pat`, they cannot be disambiguated.
     *
     * GHC parses functions and binds as expressions and sorts them into the right LHS in a post-processing step.
     * Since this is not possible in tree-sitter, these conflicts are more function-centric than in GHC.
     *
     * function:
     * func (A a) = a
     *
     * bind variable:
     * a : as = [1, 2, 3]
     *
     * pattern bind infix:
     * a : as = [1, 2, 3]
     *
     * pattern bind prefix:
     * Just 1 = Just 1
     *
     * splice:
     * makeLenses ''A
     *
     * Signature and bind:
     *
     * fun :: Int
     * fun :: Int = 5
     */

    // Function vs bind
    [$._function_name, $.pattern],

    // Function vs bind vs splice
    [$._function_name, $.pattern, $.expression],

    // Bind vs splice
    [$.pattern, $.expression],

    // Signature vs bind
    [$.signature, $.pattern],

    /**
     * Unboxed syntax
     *
     * The hash in the opening parenthesis of unboxed tuples can be an operator.
     */
    [$._operator_hash_head, $._unboxed_open],

    /**
     * Types conflicting with structures that look like types
     *
     * Note: These conflicts have been circumvented by a lookahead mechanism in the scanner.
     * This comment is preserved for reference.
     *
     * `name` and `constructor` use the same terminal symbol, but we cannot reduce `constructor` in prefix data
     * constructor declarations.
     *
     * In GHC, this corresponds to `DisambTD`.
     *
     * > data A = Name Int
     * > data A = Maybe Int :+ Int
     * > data A = Monoid a => A a
     *
     * All of these start with a `name` node.
     * In the first example, the `name` is a data constructor, which will not be reduced to a `type`.
     *
     * In the second example, the `name` is a type constructor applied to another type in the left operand of an infix
     * data constructor, so it must be reduced to `type`, and then to `apply` with the `Int`.
     *
     * In the third example, the `name` is a type constructor applied to a variable resulting in a constraint.
     * It will be reduced the same way as the second example, but using the class tree, which is mostly identical to the
     * type tree, but conflicts since we want to distinguish classes from types granularly.
     *
     * In GHC, these correspond to `mkHsAppTyHeadPV` and `mkHsAppTyPV`.
     *
     * > data A a b = a `C` b => a `A` b
     * > data A a b = a `A` b
     *
     * > data a *** b
     * > data a +++ b => a *** b
     *
     * In GHC, this corresponds to `mkHsOpTyPV`.
     *
     * > class A where type a + b = r | r -> a b
     * > class A where type a + b = (a, b)
     *
     * The first one is a type family declaration, the second one an instance.
     *
     * These were the conflicts that have been turned into scanner lookahead:
     *
     * [$._type_con, $.data_constructor],
     * [$._type_con, $._type_head_name],
     * [$._type_variable, $._tyvar],
     * [$._constructor_ticked, $._tycon_ticked],
     */

  ],

}
