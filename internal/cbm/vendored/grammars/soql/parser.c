#include "tree_sitter/parser.h"

#if defined(__GNUC__) || defined(__clang__)
#pragma GCC diagnostic ignored "-Wmissing-field-initializers"
#endif

#ifdef _MSC_VER
#pragma optimize("", off)
#elif defined(__clang__)
#pragma clang optimize off
#elif defined(__GNUC__)
#pragma GCC optimize ("O0")
#endif

#define LANGUAGE_VERSION 14
#define STATE_COUNT 812
#define LARGE_STATE_COUNT 2
#define SYMBOL_COUNT 204
#define ALIAS_COUNT 0
#define TOKEN_COUNT 115
#define EXTERNAL_TOKEN_COUNT 0
#define FIELD_COUNT 15
#define MAX_ALIAS_SEQUENCE_LENGTH 11
#define PRODUCTION_ID_COUNT 517

enum ts_symbol_identifiers {
  anon_sym_SLASH_SLASH = 1,
  aux_sym_header_comment_token1 = 2,
  anon_sym_SLASH_SLASH_SLASH = 3,
  anon_sym_LPAREN = 4,
  anon_sym_RPAREN = 5,
  aux_sym_select_clause_token1 = 6,
  anon_sym_COMMA = 7,
  aux_sym_using_clause_token1 = 8,
  aux_sym_using_scope_clause_token1 = 9,
  aux_sym_using_scope_type_token1 = 10,
  aux_sym_using_scope_type_token2 = 11,
  aux_sym_using_scope_type_token3 = 12,
  aux_sym_using_scope_type_token4 = 13,
  aux_sym_using_scope_type_token5 = 14,
  aux_sym_using_scope_type_token6 = 15,
  aux_sym_using_scope_type_token7 = 16,
  aux_sym_using_lookup_clause_token1 = 17,
  aux_sym_using_listview_clause_token1 = 18,
  anon_sym_EQ = 19,
  aux_sym_using_lookup_bind_clause_token1 = 20,
  aux_sym_type_of_clause_token1 = 21,
  aux_sym_type_of_clause_token2 = 22,
  aux_sym_when_expression_token1 = 23,
  aux_sym_when_expression_token2 = 24,
  aux_sym_else_expression_token1 = 25,
  aux_sym_group_by_clause_token1 = 26,
  aux_sym_for_clause_token1 = 27,
  aux_sym_for_type_token1 = 28,
  aux_sym_for_type_token2 = 29,
  aux_sym_for_type_token3 = 30,
  aux_sym_having_clause_token1 = 31,
  aux_sym_from_clause_token1 = 32,
  aux_sym_storage_alias_token1 = 33,
  aux_sym_fields_expression_token1 = 34,
  aux_sym_fields_type_token1 = 35,
  aux_sym_fields_type_token2 = 36,
  aux_sym_fields_type_token3 = 37,
  aux_sym_where_clause_token1 = 38,
  aux_sym_and_expression_token1 = 39,
  aux_sym_or_expression_token1 = 40,
  aux_sym_not_expression_token1 = 41,
  aux_sym_soql_with_clause_token1 = 42,
  aux_sym_soql_with_type_token1 = 43,
  aux_sym_soql_with_type_token2 = 44,
  aux_sym_soql_with_type_token3 = 45,
  aux_sym_with_user_id_type_token1 = 46,
  aux_sym_with_record_visibility_expression_token1 = 47,
  aux_sym_with_record_visibility_param_token1 = 48,
  aux_sym_with_record_visibility_param_token2 = 49,
  aux_sym_with_record_visibility_param_token3 = 50,
  aux_sym_with_data_cat_expression_token1 = 51,
  aux_sym_with_data_cat_filter_type_token1 = 52,
  aux_sym_with_data_cat_filter_type_token2 = 53,
  aux_sym_with_data_cat_filter_type_token3 = 54,
  aux_sym_with_data_cat_filter_type_token4 = 55,
  aux_sym_limit_clause_token1 = 56,
  aux_sym_offset_clause_token1 = 57,
  aux_sym_update_type_token1 = 58,
  aux_sym_update_type_token2 = 59,
  aux_sym_order_by_clause_token1 = 60,
  aux_sym_order_direction_token1 = 61,
  aux_sym_order_direction_token2 = 62,
  aux_sym_order_null_direction_token1 = 63,
  aux_sym_order_null_direction_token2 = 64,
  aux_sym_geo_location_type_token1 = 65,
  aux_sym_function_expression_token1 = 66,
  anon_sym_DOT = 67,
  aux_sym_boolean_token1 = 68,
  aux_sym_boolean_token2 = 69,
  anon_sym_BANG_EQ = 70,
  anon_sym_LT_GT = 71,
  anon_sym_LT = 72,
  anon_sym_LT_EQ = 73,
  anon_sym_GT = 74,
  anon_sym_GT_EQ = 75,
  aux_sym_value_comparison_operator_token1 = 76,
  aux_sym_set_comparison_operator_token1 = 77,
  aux_sym_set_comparison_operator_token2 = 78,
  aux_sym_set_comparison_operator_token3 = 79,
  aux_sym_set_comparison_operator_token4 = 80,
  aux_sym_date_literal_token1 = 81,
  aux_sym_date_literal_token2 = 82,
  aux_sym_date_literal_token3 = 83,
  aux_sym_date_literal_token4 = 84,
  aux_sym_date_literal_token5 = 85,
  aux_sym_date_literal_token6 = 86,
  aux_sym_date_literal_token7 = 87,
  aux_sym_date_literal_token8 = 88,
  aux_sym_date_literal_token9 = 89,
  aux_sym_date_literal_token10 = 90,
  aux_sym_date_literal_token11 = 91,
  aux_sym_date_literal_token12 = 92,
  aux_sym_date_literal_token13 = 93,
  aux_sym_date_literal_token14 = 94,
  aux_sym_date_literal_token15 = 95,
  aux_sym_date_literal_token16 = 96,
  aux_sym_date_literal_token17 = 97,
  aux_sym_date_literal_token18 = 98,
  aux_sym_date_literal_token19 = 99,
  aux_sym_date_literal_token20 = 100,
  aux_sym_date_literal_token21 = 101,
  aux_sym_date_literal_token22 = 102,
  aux_sym_date_literal_token23 = 103,
  aux_sym_date_literal_with_param_token1 = 104,
  anon_sym_COLON = 105,
  sym_bound_apex_expression = 106,
  aux_sym_null_literal_token1 = 107,
  sym_string_literal = 108,
  sym_int = 109,
  sym_decimal = 110,
  sym_date = 111,
  sym_date_time = 112,
  sym_currency_literal = 113,
  sym_identifier = 114,
  sym_source_file = 115,
  sym_header_comment = 116,
  sym_formatting_comment = 117,
  sym__soql_query_expression = 118,
  sym_subquery = 119,
  sym_soql_query_body = 120,
  sym_count_expression = 121,
  sym_select_clause = 122,
  sym__selectable_expression = 123,
  sym_using_clause = 124,
  sym_using_scope_clause = 125,
  sym_using_scope_type = 126,
  sym_using_lookup_clause = 127,
  sym_using_listview_clause = 128,
  sym_using_lookup_bind_clause = 129,
  sym_using_lookup_bind_expression = 130,
  sym_type_of_clause = 131,
  sym_when_expression = 132,
  sym_else_expression = 133,
  sym_group_by_clause = 134,
  sym__group_by_expression = 135,
  sym_for_clause = 136,
  sym_for_type = 137,
  sym_having_clause = 138,
  sym_from_clause = 139,
  sym_storage_identifier = 140,
  sym_storage_alias = 141,
  sym_fields_expression = 142,
  sym_fields_type = 143,
  sym_where_clause = 144,
  sym__boolean_expression = 145,
  sym_and_expression = 146,
  sym_or_expression = 147,
  sym_not_expression = 148,
  sym__condition_expression = 149,
  sym_comparison_expression = 150,
  sym__comparison = 151,
  sym__value_comparison = 152,
  sym__set_comparison = 153,
  sym_comparable_list = 154,
  sym_soql_with_clause = 155,
  sym_soql_with_type = 156,
  sym_with_user_id_type = 157,
  sym_with_record_visibility_expression = 158,
  sym_with_record_visibility_param = 159,
  sym_with_data_cat_expression = 160,
  sym_with_data_cat_filter = 161,
  sym_with_data_cat_filter_type = 162,
  sym_limit_clause = 163,
  sym_offset_clause = 164,
  sym_update_clause = 165,
  sym_update_type = 166,
  sym_alias_expression = 167,
  sym_order_by_clause = 168,
  sym_order_expression = 169,
  sym_order_direction = 170,
  sym_order_null_direction = 171,
  sym_geo_location_type = 172,
  sym__value_expression = 173,
  sym_function_expression = 174,
  sym_dotted_identifier = 175,
  sym_field_identifier = 176,
  sym_field_list = 177,
  sym_boolean = 178,
  sym_value_comparison_operator = 179,
  sym_set_comparison_operator = 180,
  sym_date_literal = 181,
  sym_date_literal_with_param = 182,
  sym__function_name = 183,
  sym_null_literal = 184,
  sym__soql_literal = 185,
  aux_sym_source_file_repeat1 = 186,
  aux_sym_select_clause_repeat1 = 187,
  aux_sym_using_lookup_bind_clause_repeat1 = 188,
  aux_sym_type_of_clause_repeat1 = 189,
  aux_sym__group_by_expression_repeat1 = 190,
  aux_sym_for_clause_repeat1 = 191,
  aux_sym_from_clause_repeat1 = 192,
  aux_sym_and_expression_repeat1 = 193,
  aux_sym_or_expression_repeat1 = 194,
  aux_sym_comparable_list_repeat1 = 195,
  aux_sym_with_record_visibility_expression_repeat1 = 196,
  aux_sym_with_data_cat_expression_repeat1 = 197,
  aux_sym_with_data_cat_filter_repeat1 = 198,
  aux_sym_update_clause_repeat1 = 199,
  aux_sym_order_by_clause_repeat1 = 200,
  aux_sym_function_expression_repeat1 = 201,
  aux_sym_dotted_identifier_repeat1 = 202,
  aux_sym_field_list_repeat1 = 203,
};

static const char * const ts_symbol_names[] = {
  [ts_builtin_sym_end] = "end",
  [anon_sym_SLASH_SLASH] = "//",
  [aux_sym_header_comment_token1] = "header_comment_token1",
  [anon_sym_SLASH_SLASH_SLASH] = "///",
  [anon_sym_LPAREN] = "(",
  [anon_sym_RPAREN] = ")",
  [aux_sym_select_clause_token1] = "SELECT",
  [anon_sym_COMMA] = ",",
  [aux_sym_using_clause_token1] = "USING",
  [aux_sym_using_scope_clause_token1] = "SCOPE",
  [aux_sym_using_scope_type_token1] = "delegated",
  [aux_sym_using_scope_type_token2] = "everything",
  [aux_sym_using_scope_type_token3] = "mine",
  [aux_sym_using_scope_type_token4] = "mine_and_my_groups",
  [aux_sym_using_scope_type_token5] = "my_territory",
  [aux_sym_using_scope_type_token6] = "my_team_territory",
  [aux_sym_using_scope_type_token7] = "team",
  [aux_sym_using_lookup_clause_token1] = "LOOKUP",
  [aux_sym_using_listview_clause_token1] = "ListView",
  [anon_sym_EQ] = "=",
  [aux_sym_using_lookup_bind_clause_token1] = "BIND",
  [aux_sym_type_of_clause_token1] = "TYPEOF",
  [aux_sym_type_of_clause_token2] = "END",
  [aux_sym_when_expression_token1] = "WHEN",
  [aux_sym_when_expression_token2] = "THEN",
  [aux_sym_else_expression_token1] = "ELSE",
  [aux_sym_group_by_clause_token1] = "GROUP_BY",
  [aux_sym_for_clause_token1] = "FOR",
  [aux_sym_for_type_token1] = "UPDATE",
  [aux_sym_for_type_token2] = "REFERENCE",
  [aux_sym_for_type_token3] = "VIEW",
  [aux_sym_having_clause_token1] = "HAVING",
  [aux_sym_from_clause_token1] = "FROM",
  [aux_sym_storage_alias_token1] = "AS",
  [aux_sym_fields_expression_token1] = "FIELDS",
  [aux_sym_fields_type_token1] = "ALL",
  [aux_sym_fields_type_token2] = "CUSTOM",
  [aux_sym_fields_type_token3] = "STANDARD",
  [aux_sym_where_clause_token1] = "WHERE",
  [aux_sym_and_expression_token1] = "AND",
  [aux_sym_or_expression_token1] = "OR",
  [aux_sym_not_expression_token1] = "NOT",
  [aux_sym_soql_with_clause_token1] = "WITH",
  [aux_sym_soql_with_type_token1] = "Security_Enforced",
  [aux_sym_soql_with_type_token2] = "User_Mode",
  [aux_sym_soql_with_type_token3] = "System_Mode",
  [aux_sym_with_user_id_type_token1] = "UserId",
  [aux_sym_with_record_visibility_expression_token1] = "RecordVisibilityContext",
  [aux_sym_with_record_visibility_param_token1] = "maxDescriptorPerRecord",
  [aux_sym_with_record_visibility_param_token2] = "supportsDomains",
  [aux_sym_with_record_visibility_param_token3] = "supportsDelegates",
  [aux_sym_with_data_cat_expression_token1] = "DATA_CATEGORY",
  [aux_sym_with_data_cat_filter_type_token1] = "AT",
  [aux_sym_with_data_cat_filter_type_token2] = "ABOVE",
  [aux_sym_with_data_cat_filter_type_token3] = "BELOW",
  [aux_sym_with_data_cat_filter_type_token4] = "ABOVE_OR_BELOW",
  [aux_sym_limit_clause_token1] = "LIMIT",
  [aux_sym_offset_clause_token1] = "OFFSET",
  [aux_sym_update_type_token1] = "TRACKING",
  [aux_sym_update_type_token2] = "VIEWSTAT",
  [aux_sym_order_by_clause_token1] = "ORDER_BY",
  [aux_sym_order_direction_token1] = "ASC",
  [aux_sym_order_direction_token2] = "DESC",
  [aux_sym_order_null_direction_token1] = "NULLS_FIRST",
  [aux_sym_order_null_direction_token2] = "NULLS_LAST",
  [aux_sym_geo_location_type_token1] = "identifier",
  [aux_sym_function_expression_token1] = "identifier",
  [anon_sym_DOT] = ".",
  [aux_sym_boolean_token1] = "TRUE",
  [aux_sym_boolean_token2] = "FALSE",
  [anon_sym_BANG_EQ] = "!=",
  [anon_sym_LT_GT] = "<>",
  [anon_sym_LT] = "<",
  [anon_sym_LT_EQ] = "<=",
  [anon_sym_GT] = ">",
  [anon_sym_GT_EQ] = ">=",
  [aux_sym_value_comparison_operator_token1] = "LIKE",
  [aux_sym_set_comparison_operator_token1] = "IN",
  [aux_sym_set_comparison_operator_token2] = "NOT_IN",
  [aux_sym_set_comparison_operator_token3] = "INCLUDES",
  [aux_sym_set_comparison_operator_token4] = "EXCLUDES",
  [aux_sym_date_literal_token1] = "YESTERDAY",
  [aux_sym_date_literal_token2] = "TODAY",
  [aux_sym_date_literal_token3] = "TOMORROW",
  [aux_sym_date_literal_token4] = "LAST_WEEK",
  [aux_sym_date_literal_token5] = "THIS_WEEK",
  [aux_sym_date_literal_token6] = "NEXT_WEEK",
  [aux_sym_date_literal_token7] = "LAST_MONTH",
  [aux_sym_date_literal_token8] = "THIS_MONTH",
  [aux_sym_date_literal_token9] = "NEXT_MONTH",
  [aux_sym_date_literal_token10] = "LAST_90_DAYS",
  [aux_sym_date_literal_token11] = "NEXT_90_DAYS",
  [aux_sym_date_literal_token12] = "THIS_QUARTER",
  [aux_sym_date_literal_token13] = "LAST_QUARTER",
  [aux_sym_date_literal_token14] = "NEXT_QUARTER",
  [aux_sym_date_literal_token15] = "THIS_YEAR",
  [aux_sym_date_literal_token16] = "LAST_YEAR",
  [aux_sym_date_literal_token17] = "NEXT_YEAR",
  [aux_sym_date_literal_token18] = "THIS_FISCAL_QUARTER",
  [aux_sym_date_literal_token19] = "LAST_FISCAL_QUARTER",
  [aux_sym_date_literal_token20] = "NEXT_FISCAL_QUARTER",
  [aux_sym_date_literal_token21] = "THIS_FISCAL_YEAR",
  [aux_sym_date_literal_token22] = "LAST_FISCAL_YEAR",
  [aux_sym_date_literal_token23] = "NEXT_FISCAL_YEAR",
  [aux_sym_date_literal_with_param_token1] = "date_literal",
  [anon_sym_COLON] = ":",
  [sym_bound_apex_expression] = "bound_apex_expression",
  [aux_sym_null_literal_token1] = "NULL",
  [sym_string_literal] = "string_literal",
  [sym_int] = "int",
  [sym_decimal] = "decimal",
  [sym_date] = "date",
  [sym_date_time] = "date_time",
  [sym_currency_literal] = "currency_literal",
  [sym_identifier] = "identifier",
  [sym_source_file] = "source_file",
  [sym_header_comment] = "header_comment",
  [sym_formatting_comment] = "formatting_comment",
  [sym__soql_query_expression] = "_soql_query_expression",
  [sym_subquery] = "subquery",
  [sym_soql_query_body] = "soql_query_body",
  [sym_count_expression] = "count_expression",
  [sym_select_clause] = "select_clause",
  [sym__selectable_expression] = "_selectable_expression",
  [sym_using_clause] = "using_clause",
  [sym_using_scope_clause] = "using_scope_clause",
  [sym_using_scope_type] = "using_scope_type",
  [sym_using_lookup_clause] = "using_lookup_clause",
  [sym_using_listview_clause] = "using_listview_clause",
  [sym_using_lookup_bind_clause] = "using_lookup_bind_clause",
  [sym_using_lookup_bind_expression] = "using_lookup_bind_expression",
  [sym_type_of_clause] = "type_of_clause",
  [sym_when_expression] = "when_expression",
  [sym_else_expression] = "else_expression",
  [sym_group_by_clause] = "group_by_clause",
  [sym__group_by_expression] = "_group_by_expression",
  [sym_for_clause] = "for_clause",
  [sym_for_type] = "for_type",
  [sym_having_clause] = "having_clause",
  [sym_from_clause] = "from_clause",
  [sym_storage_identifier] = "storage_identifier",
  [sym_storage_alias] = "storage_alias",
  [sym_fields_expression] = "fields_expression",
  [sym_fields_type] = "fields_type",
  [sym_where_clause] = "where_clause",
  [sym__boolean_expression] = "_boolean_expression",
  [sym_and_expression] = "and_expression",
  [sym_or_expression] = "or_expression",
  [sym_not_expression] = "not_expression",
  [sym__condition_expression] = "_condition_expression",
  [sym_comparison_expression] = "comparison_expression",
  [sym__comparison] = "_comparison",
  [sym__value_comparison] = "_value_comparison",
  [sym__set_comparison] = "_set_comparison",
  [sym_comparable_list] = "comparable_list",
  [sym_soql_with_clause] = "with_clause",
  [sym_soql_with_type] = "with_type",
  [sym_with_user_id_type] = "with_user_id_type",
  [sym_with_record_visibility_expression] = "with_record_visibility_expression",
  [sym_with_record_visibility_param] = "with_record_visibility_param",
  [sym_with_data_cat_expression] = "with_data_cat_expression",
  [sym_with_data_cat_filter] = "with_data_cat_filter",
  [sym_with_data_cat_filter_type] = "with_data_cat_filter_type",
  [sym_limit_clause] = "limit_clause",
  [sym_offset_clause] = "offset_clause",
  [sym_update_clause] = "update_clause",
  [sym_update_type] = "update_type",
  [sym_alias_expression] = "alias_expression",
  [sym_order_by_clause] = "order_by_clause",
  [sym_order_expression] = "order_expression",
  [sym_order_direction] = "order_direction",
  [sym_order_null_direction] = "order_null_direction",
  [sym_geo_location_type] = "geo_location_type",
  [sym__value_expression] = "_value_expression",
  [sym_function_expression] = "function_expression",
  [sym_dotted_identifier] = "dotted_identifier",
  [sym_field_identifier] = "field_identifier",
  [sym_field_list] = "field_list",
  [sym_boolean] = "boolean",
  [sym_value_comparison_operator] = "value_comparison_operator",
  [sym_set_comparison_operator] = "set_comparison_operator",
  [sym_date_literal] = "date_literal",
  [sym_date_literal_with_param] = "date_literal_with_param",
  [sym__function_name] = "_function_name",
  [sym_null_literal] = "null_literal",
  [sym__soql_literal] = "_soql_literal",
  [aux_sym_source_file_repeat1] = "source_file_repeat1",
  [aux_sym_select_clause_repeat1] = "select_clause_repeat1",
  [aux_sym_using_lookup_bind_clause_repeat1] = "using_lookup_bind_clause_repeat1",
  [aux_sym_type_of_clause_repeat1] = "type_of_clause_repeat1",
  [aux_sym__group_by_expression_repeat1] = "_group_by_expression_repeat1",
  [aux_sym_for_clause_repeat1] = "for_clause_repeat1",
  [aux_sym_from_clause_repeat1] = "from_clause_repeat1",
  [aux_sym_and_expression_repeat1] = "and_expression_repeat1",
  [aux_sym_or_expression_repeat1] = "or_expression_repeat1",
  [aux_sym_comparable_list_repeat1] = "comparable_list_repeat1",
  [aux_sym_with_record_visibility_expression_repeat1] = "with_record_visibility_expression_repeat1",
  [aux_sym_with_data_cat_expression_repeat1] = "with_data_cat_expression_repeat1",
  [aux_sym_with_data_cat_filter_repeat1] = "with_data_cat_filter_repeat1",
  [aux_sym_update_clause_repeat1] = "update_clause_repeat1",
  [aux_sym_order_by_clause_repeat1] = "order_by_clause_repeat1",
  [aux_sym_function_expression_repeat1] = "function_expression_repeat1",
  [aux_sym_dotted_identifier_repeat1] = "dotted_identifier_repeat1",
  [aux_sym_field_list_repeat1] = "field_list_repeat1",
};

static const TSSymbol ts_symbol_map[] = {
  [ts_builtin_sym_end] = ts_builtin_sym_end,
  [anon_sym_SLASH_SLASH] = anon_sym_SLASH_SLASH,
  [aux_sym_header_comment_token1] = aux_sym_header_comment_token1,
  [anon_sym_SLASH_SLASH_SLASH] = anon_sym_SLASH_SLASH_SLASH,
  [anon_sym_LPAREN] = anon_sym_LPAREN,
  [anon_sym_RPAREN] = anon_sym_RPAREN,
  [aux_sym_select_clause_token1] = aux_sym_select_clause_token1,
  [anon_sym_COMMA] = anon_sym_COMMA,
  [aux_sym_using_clause_token1] = aux_sym_using_clause_token1,
  [aux_sym_using_scope_clause_token1] = aux_sym_using_scope_clause_token1,
  [aux_sym_using_scope_type_token1] = aux_sym_using_scope_type_token1,
  [aux_sym_using_scope_type_token2] = aux_sym_using_scope_type_token2,
  [aux_sym_using_scope_type_token3] = aux_sym_using_scope_type_token3,
  [aux_sym_using_scope_type_token4] = aux_sym_using_scope_type_token4,
  [aux_sym_using_scope_type_token5] = aux_sym_using_scope_type_token5,
  [aux_sym_using_scope_type_token6] = aux_sym_using_scope_type_token6,
  [aux_sym_using_scope_type_token7] = aux_sym_using_scope_type_token7,
  [aux_sym_using_lookup_clause_token1] = aux_sym_using_lookup_clause_token1,
  [aux_sym_using_listview_clause_token1] = aux_sym_using_listview_clause_token1,
  [anon_sym_EQ] = anon_sym_EQ,
  [aux_sym_using_lookup_bind_clause_token1] = aux_sym_using_lookup_bind_clause_token1,
  [aux_sym_type_of_clause_token1] = aux_sym_type_of_clause_token1,
  [aux_sym_type_of_clause_token2] = aux_sym_type_of_clause_token2,
  [aux_sym_when_expression_token1] = aux_sym_when_expression_token1,
  [aux_sym_when_expression_token2] = aux_sym_when_expression_token2,
  [aux_sym_else_expression_token1] = aux_sym_else_expression_token1,
  [aux_sym_group_by_clause_token1] = aux_sym_group_by_clause_token1,
  [aux_sym_for_clause_token1] = aux_sym_for_clause_token1,
  [aux_sym_for_type_token1] = aux_sym_for_type_token1,
  [aux_sym_for_type_token2] = aux_sym_for_type_token2,
  [aux_sym_for_type_token3] = aux_sym_for_type_token3,
  [aux_sym_having_clause_token1] = aux_sym_having_clause_token1,
  [aux_sym_from_clause_token1] = aux_sym_from_clause_token1,
  [aux_sym_storage_alias_token1] = aux_sym_storage_alias_token1,
  [aux_sym_fields_expression_token1] = aux_sym_fields_expression_token1,
  [aux_sym_fields_type_token1] = aux_sym_fields_type_token1,
  [aux_sym_fields_type_token2] = aux_sym_fields_type_token2,
  [aux_sym_fields_type_token3] = aux_sym_fields_type_token3,
  [aux_sym_where_clause_token1] = aux_sym_where_clause_token1,
  [aux_sym_and_expression_token1] = aux_sym_and_expression_token1,
  [aux_sym_or_expression_token1] = aux_sym_or_expression_token1,
  [aux_sym_not_expression_token1] = aux_sym_not_expression_token1,
  [aux_sym_soql_with_clause_token1] = aux_sym_soql_with_clause_token1,
  [aux_sym_soql_with_type_token1] = aux_sym_soql_with_type_token1,
  [aux_sym_soql_with_type_token2] = aux_sym_soql_with_type_token2,
  [aux_sym_soql_with_type_token3] = aux_sym_soql_with_type_token3,
  [aux_sym_with_user_id_type_token1] = aux_sym_with_user_id_type_token1,
  [aux_sym_with_record_visibility_expression_token1] = aux_sym_with_record_visibility_expression_token1,
  [aux_sym_with_record_visibility_param_token1] = aux_sym_with_record_visibility_param_token1,
  [aux_sym_with_record_visibility_param_token2] = aux_sym_with_record_visibility_param_token2,
  [aux_sym_with_record_visibility_param_token3] = aux_sym_with_record_visibility_param_token3,
  [aux_sym_with_data_cat_expression_token1] = aux_sym_with_data_cat_expression_token1,
  [aux_sym_with_data_cat_filter_type_token1] = aux_sym_with_data_cat_filter_type_token1,
  [aux_sym_with_data_cat_filter_type_token2] = aux_sym_with_data_cat_filter_type_token2,
  [aux_sym_with_data_cat_filter_type_token3] = aux_sym_with_data_cat_filter_type_token3,
  [aux_sym_with_data_cat_filter_type_token4] = aux_sym_with_data_cat_filter_type_token4,
  [aux_sym_limit_clause_token1] = aux_sym_limit_clause_token1,
  [aux_sym_offset_clause_token1] = aux_sym_offset_clause_token1,
  [aux_sym_update_type_token1] = aux_sym_update_type_token1,
  [aux_sym_update_type_token2] = aux_sym_update_type_token2,
  [aux_sym_order_by_clause_token1] = aux_sym_order_by_clause_token1,
  [aux_sym_order_direction_token1] = aux_sym_order_direction_token1,
  [aux_sym_order_direction_token2] = aux_sym_order_direction_token2,
  [aux_sym_order_null_direction_token1] = aux_sym_order_null_direction_token1,
  [aux_sym_order_null_direction_token2] = aux_sym_order_null_direction_token2,
  [aux_sym_geo_location_type_token1] = sym_identifier,
  [aux_sym_function_expression_token1] = sym_identifier,
  [anon_sym_DOT] = anon_sym_DOT,
  [aux_sym_boolean_token1] = aux_sym_boolean_token1,
  [aux_sym_boolean_token2] = aux_sym_boolean_token2,
  [anon_sym_BANG_EQ] = anon_sym_BANG_EQ,
  [anon_sym_LT_GT] = anon_sym_LT_GT,
  [anon_sym_LT] = anon_sym_LT,
  [anon_sym_LT_EQ] = anon_sym_LT_EQ,
  [anon_sym_GT] = anon_sym_GT,
  [anon_sym_GT_EQ] = anon_sym_GT_EQ,
  [aux_sym_value_comparison_operator_token1] = aux_sym_value_comparison_operator_token1,
  [aux_sym_set_comparison_operator_token1] = aux_sym_set_comparison_operator_token1,
  [aux_sym_set_comparison_operator_token2] = aux_sym_set_comparison_operator_token2,
  [aux_sym_set_comparison_operator_token3] = aux_sym_set_comparison_operator_token3,
  [aux_sym_set_comparison_operator_token4] = aux_sym_set_comparison_operator_token4,
  [aux_sym_date_literal_token1] = aux_sym_date_literal_token1,
  [aux_sym_date_literal_token2] = aux_sym_date_literal_token2,
  [aux_sym_date_literal_token3] = aux_sym_date_literal_token3,
  [aux_sym_date_literal_token4] = aux_sym_date_literal_token4,
  [aux_sym_date_literal_token5] = aux_sym_date_literal_token5,
  [aux_sym_date_literal_token6] = aux_sym_date_literal_token6,
  [aux_sym_date_literal_token7] = aux_sym_date_literal_token7,
  [aux_sym_date_literal_token8] = aux_sym_date_literal_token8,
  [aux_sym_date_literal_token9] = aux_sym_date_literal_token9,
  [aux_sym_date_literal_token10] = aux_sym_date_literal_token10,
  [aux_sym_date_literal_token11] = aux_sym_date_literal_token11,
  [aux_sym_date_literal_token12] = aux_sym_date_literal_token12,
  [aux_sym_date_literal_token13] = aux_sym_date_literal_token13,
  [aux_sym_date_literal_token14] = aux_sym_date_literal_token14,
  [aux_sym_date_literal_token15] = aux_sym_date_literal_token15,
  [aux_sym_date_literal_token16] = aux_sym_date_literal_token16,
  [aux_sym_date_literal_token17] = aux_sym_date_literal_token17,
  [aux_sym_date_literal_token18] = aux_sym_date_literal_token18,
  [aux_sym_date_literal_token19] = aux_sym_date_literal_token19,
  [aux_sym_date_literal_token20] = aux_sym_date_literal_token20,
  [aux_sym_date_literal_token21] = aux_sym_date_literal_token21,
  [aux_sym_date_literal_token22] = aux_sym_date_literal_token22,
  [aux_sym_date_literal_token23] = aux_sym_date_literal_token23,
  [aux_sym_date_literal_with_param_token1] = sym_date_literal,
  [anon_sym_COLON] = anon_sym_COLON,
  [sym_bound_apex_expression] = sym_bound_apex_expression,
  [aux_sym_null_literal_token1] = aux_sym_null_literal_token1,
  [sym_string_literal] = sym_string_literal,
  [sym_int] = sym_int,
  [sym_decimal] = sym_decimal,
  [sym_date] = sym_date,
  [sym_date_time] = sym_date_time,
  [sym_currency_literal] = sym_currency_literal,
  [sym_identifier] = sym_identifier,
  [sym_source_file] = sym_source_file,
  [sym_header_comment] = sym_header_comment,
  [sym_formatting_comment] = sym_formatting_comment,
  [sym__soql_query_expression] = sym__soql_query_expression,
  [sym_subquery] = sym_subquery,
  [sym_soql_query_body] = sym_soql_query_body,
  [sym_count_expression] = sym_count_expression,
  [sym_select_clause] = sym_select_clause,
  [sym__selectable_expression] = sym__selectable_expression,
  [sym_using_clause] = sym_using_clause,
  [sym_using_scope_clause] = sym_using_scope_clause,
  [sym_using_scope_type] = sym_using_scope_type,
  [sym_using_lookup_clause] = sym_using_lookup_clause,
  [sym_using_listview_clause] = sym_using_listview_clause,
  [sym_using_lookup_bind_clause] = sym_using_lookup_bind_clause,
  [sym_using_lookup_bind_expression] = sym_using_lookup_bind_expression,
  [sym_type_of_clause] = sym_type_of_clause,
  [sym_when_expression] = sym_when_expression,
  [sym_else_expression] = sym_else_expression,
  [sym_group_by_clause] = sym_group_by_clause,
  [sym__group_by_expression] = sym__group_by_expression,
  [sym_for_clause] = sym_for_clause,
  [sym_for_type] = sym_for_type,
  [sym_having_clause] = sym_having_clause,
  [sym_from_clause] = sym_from_clause,
  [sym_storage_identifier] = sym_storage_identifier,
  [sym_storage_alias] = sym_storage_alias,
  [sym_fields_expression] = sym_fields_expression,
  [sym_fields_type] = sym_fields_type,
  [sym_where_clause] = sym_where_clause,
  [sym__boolean_expression] = sym__boolean_expression,
  [sym_and_expression] = sym_and_expression,
  [sym_or_expression] = sym_or_expression,
  [sym_not_expression] = sym_not_expression,
  [sym__condition_expression] = sym__condition_expression,
  [sym_comparison_expression] = sym_comparison_expression,
  [sym__comparison] = sym__comparison,
  [sym__value_comparison] = sym__value_comparison,
  [sym__set_comparison] = sym__set_comparison,
  [sym_comparable_list] = sym_comparable_list,
  [sym_soql_with_clause] = sym_soql_with_clause,
  [sym_soql_with_type] = sym_soql_with_type,
  [sym_with_user_id_type] = sym_with_user_id_type,
  [sym_with_record_visibility_expression] = sym_with_record_visibility_expression,
  [sym_with_record_visibility_param] = sym_with_record_visibility_param,
  [sym_with_data_cat_expression] = sym_with_data_cat_expression,
  [sym_with_data_cat_filter] = sym_with_data_cat_filter,
  [sym_with_data_cat_filter_type] = sym_with_data_cat_filter_type,
  [sym_limit_clause] = sym_limit_clause,
  [sym_offset_clause] = sym_offset_clause,
  [sym_update_clause] = sym_update_clause,
  [sym_update_type] = sym_update_type,
  [sym_alias_expression] = sym_alias_expression,
  [sym_order_by_clause] = sym_order_by_clause,
  [sym_order_expression] = sym_order_expression,
  [sym_order_direction] = sym_order_direction,
  [sym_order_null_direction] = sym_order_null_direction,
  [sym_geo_location_type] = sym_geo_location_type,
  [sym__value_expression] = sym__value_expression,
  [sym_function_expression] = sym_function_expression,
  [sym_dotted_identifier] = sym_dotted_identifier,
  [sym_field_identifier] = sym_field_identifier,
  [sym_field_list] = sym_field_list,
  [sym_boolean] = sym_boolean,
  [sym_value_comparison_operator] = sym_value_comparison_operator,
  [sym_set_comparison_operator] = sym_set_comparison_operator,
  [sym_date_literal] = sym_date_literal,
  [sym_date_literal_with_param] = sym_date_literal_with_param,
  [sym__function_name] = sym__function_name,
  [sym_null_literal] = sym_null_literal,
  [sym__soql_literal] = sym__soql_literal,
  [aux_sym_source_file_repeat1] = aux_sym_source_file_repeat1,
  [aux_sym_select_clause_repeat1] = aux_sym_select_clause_repeat1,
  [aux_sym_using_lookup_bind_clause_repeat1] = aux_sym_using_lookup_bind_clause_repeat1,
  [aux_sym_type_of_clause_repeat1] = aux_sym_type_of_clause_repeat1,
  [aux_sym__group_by_expression_repeat1] = aux_sym__group_by_expression_repeat1,
  [aux_sym_for_clause_repeat1] = aux_sym_for_clause_repeat1,
  [aux_sym_from_clause_repeat1] = aux_sym_from_clause_repeat1,
  [aux_sym_and_expression_repeat1] = aux_sym_and_expression_repeat1,
  [aux_sym_or_expression_repeat1] = aux_sym_or_expression_repeat1,
  [aux_sym_comparable_list_repeat1] = aux_sym_comparable_list_repeat1,
  [aux_sym_with_record_visibility_expression_repeat1] = aux_sym_with_record_visibility_expression_repeat1,
  [aux_sym_with_data_cat_expression_repeat1] = aux_sym_with_data_cat_expression_repeat1,
  [aux_sym_with_data_cat_filter_repeat1] = aux_sym_with_data_cat_filter_repeat1,
  [aux_sym_update_clause_repeat1] = aux_sym_update_clause_repeat1,
  [aux_sym_order_by_clause_repeat1] = aux_sym_order_by_clause_repeat1,
  [aux_sym_function_expression_repeat1] = aux_sym_function_expression_repeat1,
  [aux_sym_dotted_identifier_repeat1] = aux_sym_dotted_identifier_repeat1,
  [aux_sym_field_list_repeat1] = aux_sym_field_list_repeat1,
};

static const TSSymbolMetadata ts_symbol_metadata[] = {
  [ts_builtin_sym_end] = {
    .visible = false,
    .named = true,
  },
  [anon_sym_SLASH_SLASH] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_header_comment_token1] = {
    .visible = false,
    .named = false,
  },
  [anon_sym_SLASH_SLASH_SLASH] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_LPAREN] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_RPAREN] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_select_clause_token1] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_COMMA] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_using_clause_token1] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_using_scope_clause_token1] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_using_scope_type_token1] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_using_scope_type_token2] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_using_scope_type_token3] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_using_scope_type_token4] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_using_scope_type_token5] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_using_scope_type_token6] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_using_scope_type_token7] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_using_lookup_clause_token1] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_using_listview_clause_token1] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_EQ] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_using_lookup_bind_clause_token1] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_type_of_clause_token1] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_type_of_clause_token2] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_when_expression_token1] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_when_expression_token2] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_else_expression_token1] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_group_by_clause_token1] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_for_clause_token1] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_for_type_token1] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_for_type_token2] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_for_type_token3] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_having_clause_token1] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_from_clause_token1] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_storage_alias_token1] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_fields_expression_token1] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_fields_type_token1] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_fields_type_token2] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_fields_type_token3] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_where_clause_token1] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_and_expression_token1] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_or_expression_token1] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_not_expression_token1] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_soql_with_clause_token1] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_soql_with_type_token1] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_soql_with_type_token2] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_soql_with_type_token3] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_with_user_id_type_token1] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_with_record_visibility_expression_token1] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_with_record_visibility_param_token1] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_with_record_visibility_param_token2] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_with_record_visibility_param_token3] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_with_data_cat_expression_token1] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_with_data_cat_filter_type_token1] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_with_data_cat_filter_type_token2] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_with_data_cat_filter_type_token3] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_with_data_cat_filter_type_token4] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_limit_clause_token1] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_offset_clause_token1] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_update_type_token1] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_update_type_token2] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_order_by_clause_token1] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_order_direction_token1] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_order_direction_token2] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_order_null_direction_token1] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_order_null_direction_token2] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_geo_location_type_token1] = {
    .visible = true,
    .named = true,
  },
  [aux_sym_function_expression_token1] = {
    .visible = true,
    .named = true,
  },
  [anon_sym_DOT] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_boolean_token1] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_boolean_token2] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_BANG_EQ] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_LT_GT] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_LT] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_LT_EQ] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_GT] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_GT_EQ] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_value_comparison_operator_token1] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_set_comparison_operator_token1] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_set_comparison_operator_token2] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_set_comparison_operator_token3] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_set_comparison_operator_token4] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_date_literal_token1] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_date_literal_token2] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_date_literal_token3] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_date_literal_token4] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_date_literal_token5] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_date_literal_token6] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_date_literal_token7] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_date_literal_token8] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_date_literal_token9] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_date_literal_token10] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_date_literal_token11] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_date_literal_token12] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_date_literal_token13] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_date_literal_token14] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_date_literal_token15] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_date_literal_token16] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_date_literal_token17] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_date_literal_token18] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_date_literal_token19] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_date_literal_token20] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_date_literal_token21] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_date_literal_token22] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_date_literal_token23] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_date_literal_with_param_token1] = {
    .visible = true,
    .named = true,
  },
  [anon_sym_COLON] = {
    .visible = true,
    .named = false,
  },
  [sym_bound_apex_expression] = {
    .visible = true,
    .named = true,
  },
  [aux_sym_null_literal_token1] = {
    .visible = true,
    .named = false,
  },
  [sym_string_literal] = {
    .visible = true,
    .named = true,
  },
  [sym_int] = {
    .visible = true,
    .named = true,
  },
  [sym_decimal] = {
    .visible = true,
    .named = true,
  },
  [sym_date] = {
    .visible = true,
    .named = true,
  },
  [sym_date_time] = {
    .visible = true,
    .named = true,
  },
  [sym_currency_literal] = {
    .visible = true,
    .named = true,
  },
  [sym_identifier] = {
    .visible = true,
    .named = true,
  },
  [sym_source_file] = {
    .visible = true,
    .named = true,
  },
  [sym_header_comment] = {
    .visible = true,
    .named = true,
  },
  [sym_formatting_comment] = {
    .visible = true,
    .named = true,
  },
  [sym__soql_query_expression] = {
    .visible = false,
    .named = true,
  },
  [sym_subquery] = {
    .visible = true,
    .named = true,
  },
  [sym_soql_query_body] = {
    .visible = true,
    .named = true,
  },
  [sym_count_expression] = {
    .visible = true,
    .named = true,
  },
  [sym_select_clause] = {
    .visible = true,
    .named = true,
  },
  [sym__selectable_expression] = {
    .visible = false,
    .named = true,
  },
  [sym_using_clause] = {
    .visible = true,
    .named = true,
  },
  [sym_using_scope_clause] = {
    .visible = true,
    .named = true,
  },
  [sym_using_scope_type] = {
    .visible = true,
    .named = true,
  },
  [sym_using_lookup_clause] = {
    .visible = true,
    .named = true,
  },
  [sym_using_listview_clause] = {
    .visible = true,
    .named = true,
  },
  [sym_using_lookup_bind_clause] = {
    .visible = true,
    .named = true,
  },
  [sym_using_lookup_bind_expression] = {
    .visible = true,
    .named = true,
  },
  [sym_type_of_clause] = {
    .visible = true,
    .named = true,
  },
  [sym_when_expression] = {
    .visible = true,
    .named = true,
  },
  [sym_else_expression] = {
    .visible = true,
    .named = true,
  },
  [sym_group_by_clause] = {
    .visible = true,
    .named = true,
  },
  [sym__group_by_expression] = {
    .visible = false,
    .named = true,
  },
  [sym_for_clause] = {
    .visible = true,
    .named = true,
  },
  [sym_for_type] = {
    .visible = true,
    .named = true,
  },
  [sym_having_clause] = {
    .visible = true,
    .named = true,
  },
  [sym_from_clause] = {
    .visible = true,
    .named = true,
  },
  [sym_storage_identifier] = {
    .visible = true,
    .named = true,
  },
  [sym_storage_alias] = {
    .visible = true,
    .named = true,
  },
  [sym_fields_expression] = {
    .visible = true,
    .named = true,
  },
  [sym_fields_type] = {
    .visible = true,
    .named = true,
  },
  [sym_where_clause] = {
    .visible = true,
    .named = true,
  },
  [sym__boolean_expression] = {
    .visible = false,
    .named = true,
  },
  [sym_and_expression] = {
    .visible = true,
    .named = true,
  },
  [sym_or_expression] = {
    .visible = true,
    .named = true,
  },
  [sym_not_expression] = {
    .visible = true,
    .named = true,
  },
  [sym__condition_expression] = {
    .visible = false,
    .named = true,
  },
  [sym_comparison_expression] = {
    .visible = true,
    .named = true,
  },
  [sym__comparison] = {
    .visible = false,
    .named = true,
  },
  [sym__value_comparison] = {
    .visible = false,
    .named = true,
  },
  [sym__set_comparison] = {
    .visible = false,
    .named = true,
  },
  [sym_comparable_list] = {
    .visible = true,
    .named = true,
  },
  [sym_soql_with_clause] = {
    .visible = true,
    .named = true,
  },
  [sym_soql_with_type] = {
    .visible = true,
    .named = true,
  },
  [sym_with_user_id_type] = {
    .visible = true,
    .named = true,
  },
  [sym_with_record_visibility_expression] = {
    .visible = true,
    .named = true,
  },
  [sym_with_record_visibility_param] = {
    .visible = true,
    .named = true,
  },
  [sym_with_data_cat_expression] = {
    .visible = true,
    .named = true,
  },
  [sym_with_data_cat_filter] = {
    .visible = true,
    .named = true,
  },
  [sym_with_data_cat_filter_type] = {
    .visible = true,
    .named = true,
  },
  [sym_limit_clause] = {
    .visible = true,
    .named = true,
  },
  [sym_offset_clause] = {
    .visible = true,
    .named = true,
  },
  [sym_update_clause] = {
    .visible = true,
    .named = true,
  },
  [sym_update_type] = {
    .visible = true,
    .named = true,
  },
  [sym_alias_expression] = {
    .visible = true,
    .named = true,
  },
  [sym_order_by_clause] = {
    .visible = true,
    .named = true,
  },
  [sym_order_expression] = {
    .visible = true,
    .named = true,
  },
  [sym_order_direction] = {
    .visible = true,
    .named = true,
  },
  [sym_order_null_direction] = {
    .visible = true,
    .named = true,
  },
  [sym_geo_location_type] = {
    .visible = true,
    .named = true,
  },
  [sym__value_expression] = {
    .visible = false,
    .named = true,
  },
  [sym_function_expression] = {
    .visible = true,
    .named = true,
  },
  [sym_dotted_identifier] = {
    .visible = true,
    .named = true,
  },
  [sym_field_identifier] = {
    .visible = true,
    .named = true,
  },
  [sym_field_list] = {
    .visible = true,
    .named = true,
  },
  [sym_boolean] = {
    .visible = true,
    .named = true,
  },
  [sym_value_comparison_operator] = {
    .visible = true,
    .named = true,
  },
  [sym_set_comparison_operator] = {
    .visible = true,
    .named = true,
  },
  [sym_date_literal] = {
    .visible = true,
    .named = true,
  },
  [sym_date_literal_with_param] = {
    .visible = true,
    .named = true,
  },
  [sym__function_name] = {
    .visible = false,
    .named = true,
  },
  [sym_null_literal] = {
    .visible = true,
    .named = true,
  },
  [sym__soql_literal] = {
    .visible = false,
    .named = true,
  },
  [aux_sym_source_file_repeat1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_select_clause_repeat1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_using_lookup_bind_clause_repeat1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_type_of_clause_repeat1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym__group_by_expression_repeat1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_for_clause_repeat1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_from_clause_repeat1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_and_expression_repeat1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_or_expression_repeat1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_comparable_list_repeat1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_with_record_visibility_expression_repeat1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_with_data_cat_expression_repeat1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_with_data_cat_filter_repeat1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_update_clause_repeat1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_order_by_clause_repeat1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_function_expression_repeat1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_dotted_identifier_repeat1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_field_list_repeat1] = {
    .visible = false,
    .named = false,
  },
};

enum ts_field_identifiers {
  field_bound_value = 1,
  field_field = 2,
  field_for_clause = 3,
  field_from_clause = 4,
  field_function_name = 5,
  field_group_by_clause = 6,
  field_limit_clause = 7,
  field_lookup_field = 8,
  field_offset_clause = 9,
  field_order_by_clause = 10,
  field_select_clause = 11,
  field_update_clause = 12,
  field_using_clause = 13,
  field_where_clause = 14,
  field_with_clause = 15,
};

static const char * const ts_field_names[] = {
  [0] = NULL,
  [field_bound_value] = "bound_value",
  [field_field] = "field",
  [field_for_clause] = "for_clause",
  [field_from_clause] = "from_clause",
  [field_function_name] = "function_name",
  [field_group_by_clause] = "group_by_clause",
  [field_limit_clause] = "limit_clause",
  [field_lookup_field] = "lookup_field",
  [field_offset_clause] = "offset_clause",
  [field_order_by_clause] = "order_by_clause",
  [field_select_clause] = "select_clause",
  [field_update_clause] = "update_clause",
  [field_using_clause] = "using_clause",
  [field_where_clause] = "where_clause",
  [field_with_clause] = "with_clause",
};

static const TSFieldMapSlice ts_field_map_slices[PRODUCTION_ID_COUNT] = {
  [1] = {.index = 0, .length = 1},
  [2] = {.index = 1, .length = 2},
  [3] = {.index = 3, .length = 3},
  [4] = {.index = 6, .length = 3},
  [5] = {.index = 9, .length = 3},
  [6] = {.index = 12, .length = 3},
  [7] = {.index = 15, .length = 3},
  [8] = {.index = 18, .length = 3},
  [9] = {.index = 21, .length = 3},
  [10] = {.index = 24, .length = 3},
  [11] = {.index = 27, .length = 3},
  [12] = {.index = 30, .length = 1},
  [13] = {.index = 31, .length = 4},
  [14] = {.index = 35, .length = 4},
  [15] = {.index = 39, .length = 4},
  [16] = {.index = 43, .length = 4},
  [17] = {.index = 47, .length = 4},
  [18] = {.index = 51, .length = 4},
  [19] = {.index = 55, .length = 4},
  [20] = {.index = 59, .length = 4},
  [21] = {.index = 63, .length = 4},
  [22] = {.index = 67, .length = 4},
  [23] = {.index = 71, .length = 4},
  [24] = {.index = 75, .length = 4},
  [25] = {.index = 79, .length = 4},
  [26] = {.index = 83, .length = 4},
  [27] = {.index = 87, .length = 4},
  [28] = {.index = 91, .length = 4},
  [29] = {.index = 95, .length = 4},
  [30] = {.index = 99, .length = 4},
  [31] = {.index = 103, .length = 4},
  [32] = {.index = 107, .length = 4},
  [33] = {.index = 111, .length = 4},
  [34] = {.index = 115, .length = 4},
  [35] = {.index = 119, .length = 4},
  [36] = {.index = 123, .length = 4},
  [37] = {.index = 127, .length = 4},
  [38] = {.index = 131, .length = 4},
  [39] = {.index = 135, .length = 4},
  [40] = {.index = 139, .length = 4},
  [41] = {.index = 143, .length = 4},
  [42] = {.index = 147, .length = 4},
  [43] = {.index = 151, .length = 4},
  [44] = {.index = 155, .length = 4},
  [45] = {.index = 159, .length = 4},
  [46] = {.index = 163, .length = 4},
  [47] = {.index = 167, .length = 4},
  [48] = {.index = 171, .length = 4},
  [49] = {.index = 175, .length = 1},
  [50] = {.index = 176, .length = 5},
  [51] = {.index = 181, .length = 5},
  [52] = {.index = 186, .length = 5},
  [53] = {.index = 191, .length = 5},
  [54] = {.index = 196, .length = 5},
  [55] = {.index = 201, .length = 5},
  [56] = {.index = 206, .length = 5},
  [57] = {.index = 211, .length = 5},
  [58] = {.index = 216, .length = 5},
  [59] = {.index = 221, .length = 5},
  [60] = {.index = 226, .length = 5},
  [61] = {.index = 231, .length = 5},
  [62] = {.index = 236, .length = 5},
  [63] = {.index = 241, .length = 5},
  [64] = {.index = 246, .length = 5},
  [65] = {.index = 251, .length = 5},
  [66] = {.index = 256, .length = 5},
  [67] = {.index = 261, .length = 5},
  [68] = {.index = 266, .length = 5},
  [69] = {.index = 271, .length = 5},
  [70] = {.index = 276, .length = 5},
  [71] = {.index = 281, .length = 5},
  [72] = {.index = 286, .length = 5},
  [73] = {.index = 291, .length = 5},
  [74] = {.index = 296, .length = 5},
  [75] = {.index = 301, .length = 5},
  [76] = {.index = 306, .length = 5},
  [77] = {.index = 311, .length = 5},
  [78] = {.index = 316, .length = 5},
  [79] = {.index = 321, .length = 5},
  [80] = {.index = 326, .length = 5},
  [81] = {.index = 331, .length = 5},
  [82] = {.index = 336, .length = 5},
  [83] = {.index = 341, .length = 5},
  [84] = {.index = 346, .length = 5},
  [85] = {.index = 351, .length = 5},
  [86] = {.index = 356, .length = 5},
  [87] = {.index = 361, .length = 5},
  [88] = {.index = 366, .length = 5},
  [89] = {.index = 371, .length = 5},
  [90] = {.index = 376, .length = 5},
  [91] = {.index = 381, .length = 5},
  [92] = {.index = 386, .length = 5},
  [93] = {.index = 391, .length = 5},
  [94] = {.index = 396, .length = 5},
  [95] = {.index = 401, .length = 5},
  [96] = {.index = 406, .length = 5},
  [97] = {.index = 411, .length = 5},
  [98] = {.index = 416, .length = 5},
  [99] = {.index = 421, .length = 5},
  [100] = {.index = 426, .length = 5},
  [101] = {.index = 431, .length = 5},
  [102] = {.index = 436, .length = 5},
  [103] = {.index = 441, .length = 5},
  [104] = {.index = 446, .length = 5},
  [105] = {.index = 451, .length = 5},
  [106] = {.index = 456, .length = 5},
  [107] = {.index = 461, .length = 5},
  [108] = {.index = 466, .length = 5},
  [109] = {.index = 471, .length = 5},
  [110] = {.index = 476, .length = 5},
  [111] = {.index = 481, .length = 5},
  [112] = {.index = 486, .length = 5},
  [113] = {.index = 491, .length = 5},
  [114] = {.index = 496, .length = 5},
  [115] = {.index = 501, .length = 5},
  [116] = {.index = 506, .length = 5},
  [117] = {.index = 511, .length = 5},
  [118] = {.index = 516, .length = 5},
  [119] = {.index = 521, .length = 5},
  [120] = {.index = 526, .length = 5},
  [121] = {.index = 531, .length = 5},
  [122] = {.index = 536, .length = 5},
  [123] = {.index = 541, .length = 5},
  [124] = {.index = 546, .length = 5},
  [125] = {.index = 551, .length = 5},
  [126] = {.index = 556, .length = 5},
  [127] = {.index = 561, .length = 5},
  [128] = {.index = 566, .length = 5},
  [129] = {.index = 571, .length = 5},
  [130] = {.index = 576, .length = 5},
  [131] = {.index = 581, .length = 5},
  [132] = {.index = 586, .length = 5},
  [133] = {.index = 591, .length = 5},
  [134] = {.index = 596, .length = 6},
  [135] = {.index = 602, .length = 6},
  [136] = {.index = 608, .length = 6},
  [137] = {.index = 614, .length = 6},
  [138] = {.index = 620, .length = 6},
  [139] = {.index = 626, .length = 6},
  [140] = {.index = 632, .length = 6},
  [141] = {.index = 638, .length = 6},
  [142] = {.index = 644, .length = 6},
  [143] = {.index = 650, .length = 6},
  [144] = {.index = 656, .length = 6},
  [145] = {.index = 662, .length = 6},
  [146] = {.index = 668, .length = 6},
  [147] = {.index = 674, .length = 6},
  [148] = {.index = 680, .length = 6},
  [149] = {.index = 686, .length = 6},
  [150] = {.index = 692, .length = 6},
  [151] = {.index = 698, .length = 6},
  [152] = {.index = 704, .length = 6},
  [153] = {.index = 710, .length = 6},
  [154] = {.index = 716, .length = 6},
  [155] = {.index = 722, .length = 6},
  [156] = {.index = 728, .length = 6},
  [157] = {.index = 734, .length = 6},
  [158] = {.index = 740, .length = 6},
  [159] = {.index = 746, .length = 6},
  [160] = {.index = 752, .length = 6},
  [161] = {.index = 758, .length = 6},
  [162] = {.index = 764, .length = 6},
  [163] = {.index = 770, .length = 6},
  [164] = {.index = 776, .length = 6},
  [165] = {.index = 782, .length = 6},
  [166] = {.index = 788, .length = 6},
  [167] = {.index = 794, .length = 6},
  [168] = {.index = 800, .length = 6},
  [169] = {.index = 806, .length = 6},
  [170] = {.index = 812, .length = 6},
  [171] = {.index = 818, .length = 6},
  [172] = {.index = 824, .length = 6},
  [173] = {.index = 830, .length = 6},
  [174] = {.index = 836, .length = 6},
  [175] = {.index = 842, .length = 6},
  [176] = {.index = 848, .length = 6},
  [177] = {.index = 854, .length = 6},
  [178] = {.index = 860, .length = 6},
  [179] = {.index = 866, .length = 6},
  [180] = {.index = 872, .length = 6},
  [181] = {.index = 878, .length = 6},
  [182] = {.index = 884, .length = 6},
  [183] = {.index = 890, .length = 6},
  [184] = {.index = 896, .length = 6},
  [185] = {.index = 902, .length = 6},
  [186] = {.index = 908, .length = 6},
  [187] = {.index = 914, .length = 6},
  [188] = {.index = 920, .length = 6},
  [189] = {.index = 926, .length = 6},
  [190] = {.index = 932, .length = 6},
  [191] = {.index = 938, .length = 6},
  [192] = {.index = 944, .length = 6},
  [193] = {.index = 950, .length = 6},
  [194] = {.index = 956, .length = 6},
  [195] = {.index = 962, .length = 6},
  [196] = {.index = 968, .length = 6},
  [197] = {.index = 974, .length = 6},
  [198] = {.index = 980, .length = 6},
  [199] = {.index = 986, .length = 6},
  [200] = {.index = 992, .length = 6},
  [201] = {.index = 998, .length = 6},
  [202] = {.index = 1004, .length = 6},
  [203] = {.index = 1010, .length = 6},
  [204] = {.index = 1016, .length = 6},
  [205] = {.index = 1022, .length = 6},
  [206] = {.index = 1028, .length = 6},
  [207] = {.index = 1034, .length = 6},
  [208] = {.index = 1040, .length = 6},
  [209] = {.index = 1046, .length = 6},
  [210] = {.index = 1052, .length = 6},
  [211] = {.index = 1058, .length = 6},
  [212] = {.index = 1064, .length = 6},
  [213] = {.index = 1070, .length = 6},
  [214] = {.index = 1076, .length = 6},
  [215] = {.index = 1082, .length = 6},
  [216] = {.index = 1088, .length = 6},
  [217] = {.index = 1094, .length = 6},
  [218] = {.index = 1100, .length = 6},
  [219] = {.index = 1106, .length = 6},
  [220] = {.index = 1112, .length = 6},
  [221] = {.index = 1118, .length = 6},
  [222] = {.index = 1124, .length = 6},
  [223] = {.index = 1130, .length = 6},
  [224] = {.index = 1136, .length = 6},
  [225] = {.index = 1142, .length = 6},
  [226] = {.index = 1148, .length = 6},
  [227] = {.index = 1154, .length = 6},
  [228] = {.index = 1160, .length = 6},
  [229] = {.index = 1166, .length = 6},
  [230] = {.index = 1172, .length = 6},
  [231] = {.index = 1178, .length = 6},
  [232] = {.index = 1184, .length = 6},
  [233] = {.index = 1190, .length = 6},
  [234] = {.index = 1196, .length = 6},
  [235] = {.index = 1202, .length = 6},
  [236] = {.index = 1208, .length = 6},
  [237] = {.index = 1214, .length = 6},
  [238] = {.index = 1220, .length = 6},
  [239] = {.index = 1226, .length = 6},
  [240] = {.index = 1232, .length = 6},
  [241] = {.index = 1238, .length = 6},
  [242] = {.index = 1244, .length = 6},
  [243] = {.index = 1250, .length = 6},
  [244] = {.index = 1256, .length = 6},
  [245] = {.index = 1262, .length = 6},
  [246] = {.index = 1268, .length = 6},
  [247] = {.index = 1274, .length = 6},
  [248] = {.index = 1280, .length = 6},
  [249] = {.index = 1286, .length = 6},
  [250] = {.index = 1292, .length = 6},
  [251] = {.index = 1298, .length = 6},
  [252] = {.index = 1304, .length = 6},
  [253] = {.index = 1310, .length = 6},
  [254] = {.index = 1316, .length = 6},
  [255] = {.index = 1322, .length = 6},
  [256] = {.index = 1328, .length = 6},
  [257] = {.index = 1334, .length = 6},
  [258] = {.index = 1340, .length = 6},
  [259] = {.index = 1346, .length = 6},
  [260] = {.index = 1352, .length = 7},
  [261] = {.index = 1359, .length = 7},
  [262] = {.index = 1366, .length = 7},
  [263] = {.index = 1373, .length = 7},
  [264] = {.index = 1380, .length = 7},
  [265] = {.index = 1387, .length = 7},
  [266] = {.index = 1394, .length = 7},
  [267] = {.index = 1401, .length = 7},
  [268] = {.index = 1408, .length = 7},
  [269] = {.index = 1415, .length = 7},
  [270] = {.index = 1422, .length = 7},
  [271] = {.index = 1429, .length = 7},
  [272] = {.index = 1436, .length = 7},
  [273] = {.index = 1443, .length = 7},
  [274] = {.index = 1450, .length = 7},
  [275] = {.index = 1457, .length = 7},
  [276] = {.index = 1464, .length = 7},
  [277] = {.index = 1471, .length = 7},
  [278] = {.index = 1478, .length = 7},
  [279] = {.index = 1485, .length = 7},
  [280] = {.index = 1492, .length = 7},
  [281] = {.index = 1499, .length = 7},
  [282] = {.index = 1506, .length = 7},
  [283] = {.index = 1513, .length = 7},
  [284] = {.index = 1520, .length = 7},
  [285] = {.index = 1527, .length = 7},
  [286] = {.index = 1534, .length = 7},
  [287] = {.index = 1541, .length = 7},
  [288] = {.index = 1548, .length = 7},
  [289] = {.index = 1555, .length = 7},
  [290] = {.index = 1562, .length = 7},
  [291] = {.index = 1569, .length = 7},
  [292] = {.index = 1576, .length = 7},
  [293] = {.index = 1583, .length = 7},
  [294] = {.index = 1590, .length = 7},
  [295] = {.index = 1597, .length = 7},
  [296] = {.index = 1604, .length = 7},
  [297] = {.index = 1611, .length = 7},
  [298] = {.index = 1618, .length = 7},
  [299] = {.index = 1625, .length = 7},
  [300] = {.index = 1632, .length = 7},
  [301] = {.index = 1639, .length = 7},
  [302] = {.index = 1646, .length = 7},
  [303] = {.index = 1653, .length = 7},
  [304] = {.index = 1660, .length = 7},
  [305] = {.index = 1667, .length = 7},
  [306] = {.index = 1674, .length = 7},
  [307] = {.index = 1681, .length = 7},
  [308] = {.index = 1688, .length = 7},
  [309] = {.index = 1695, .length = 7},
  [310] = {.index = 1702, .length = 7},
  [311] = {.index = 1709, .length = 7},
  [312] = {.index = 1716, .length = 7},
  [313] = {.index = 1723, .length = 7},
  [314] = {.index = 1730, .length = 7},
  [315] = {.index = 1737, .length = 7},
  [316] = {.index = 1744, .length = 7},
  [317] = {.index = 1751, .length = 7},
  [318] = {.index = 1758, .length = 7},
  [319] = {.index = 1765, .length = 7},
  [320] = {.index = 1772, .length = 7},
  [321] = {.index = 1779, .length = 7},
  [322] = {.index = 1786, .length = 7},
  [323] = {.index = 1793, .length = 7},
  [324] = {.index = 1800, .length = 7},
  [325] = {.index = 1807, .length = 7},
  [326] = {.index = 1814, .length = 7},
  [327] = {.index = 1821, .length = 7},
  [328] = {.index = 1828, .length = 7},
  [329] = {.index = 1835, .length = 7},
  [330] = {.index = 1842, .length = 7},
  [331] = {.index = 1849, .length = 7},
  [332] = {.index = 1856, .length = 7},
  [333] = {.index = 1863, .length = 7},
  [334] = {.index = 1870, .length = 7},
  [335] = {.index = 1877, .length = 7},
  [336] = {.index = 1884, .length = 7},
  [337] = {.index = 1891, .length = 7},
  [338] = {.index = 1898, .length = 7},
  [339] = {.index = 1905, .length = 7},
  [340] = {.index = 1912, .length = 7},
  [341] = {.index = 1919, .length = 7},
  [342] = {.index = 1926, .length = 7},
  [343] = {.index = 1933, .length = 7},
  [344] = {.index = 1940, .length = 7},
  [345] = {.index = 1947, .length = 7},
  [346] = {.index = 1954, .length = 7},
  [347] = {.index = 1961, .length = 7},
  [348] = {.index = 1968, .length = 7},
  [349] = {.index = 1975, .length = 7},
  [350] = {.index = 1982, .length = 7},
  [351] = {.index = 1989, .length = 7},
  [352] = {.index = 1996, .length = 7},
  [353] = {.index = 2003, .length = 7},
  [354] = {.index = 2010, .length = 7},
  [355] = {.index = 2017, .length = 7},
  [356] = {.index = 2024, .length = 7},
  [357] = {.index = 2031, .length = 7},
  [358] = {.index = 2038, .length = 7},
  [359] = {.index = 2045, .length = 7},
  [360] = {.index = 2052, .length = 7},
  [361] = {.index = 2059, .length = 7},
  [362] = {.index = 2066, .length = 7},
  [363] = {.index = 2073, .length = 7},
  [364] = {.index = 2080, .length = 7},
  [365] = {.index = 2087, .length = 7},
  [366] = {.index = 2094, .length = 7},
  [367] = {.index = 2101, .length = 7},
  [368] = {.index = 2108, .length = 7},
  [369] = {.index = 2115, .length = 7},
  [370] = {.index = 2122, .length = 7},
  [371] = {.index = 2129, .length = 7},
  [372] = {.index = 2136, .length = 7},
  [373] = {.index = 2143, .length = 7},
  [374] = {.index = 2150, .length = 7},
  [375] = {.index = 2157, .length = 7},
  [376] = {.index = 2164, .length = 7},
  [377] = {.index = 2171, .length = 7},
  [378] = {.index = 2178, .length = 7},
  [379] = {.index = 2185, .length = 7},
  [380] = {.index = 2192, .length = 7},
  [381] = {.index = 2199, .length = 7},
  [382] = {.index = 2206, .length = 7},
  [383] = {.index = 2213, .length = 7},
  [384] = {.index = 2220, .length = 7},
  [385] = {.index = 2227, .length = 7},
  [386] = {.index = 2234, .length = 8},
  [387] = {.index = 2242, .length = 8},
  [388] = {.index = 2250, .length = 8},
  [389] = {.index = 2258, .length = 8},
  [390] = {.index = 2266, .length = 8},
  [391] = {.index = 2274, .length = 8},
  [392] = {.index = 2282, .length = 8},
  [393] = {.index = 2290, .length = 8},
  [394] = {.index = 2298, .length = 8},
  [395] = {.index = 2306, .length = 8},
  [396] = {.index = 2314, .length = 8},
  [397] = {.index = 2322, .length = 8},
  [398] = {.index = 2330, .length = 8},
  [399] = {.index = 2338, .length = 8},
  [400] = {.index = 2346, .length = 8},
  [401] = {.index = 2354, .length = 8},
  [402] = {.index = 2362, .length = 8},
  [403] = {.index = 2370, .length = 8},
  [404] = {.index = 2378, .length = 8},
  [405] = {.index = 2386, .length = 8},
  [406] = {.index = 2394, .length = 8},
  [407] = {.index = 2402, .length = 8},
  [408] = {.index = 2410, .length = 8},
  [409] = {.index = 2418, .length = 8},
  [410] = {.index = 2426, .length = 8},
  [411] = {.index = 2434, .length = 8},
  [412] = {.index = 2442, .length = 8},
  [413] = {.index = 2450, .length = 8},
  [414] = {.index = 2458, .length = 8},
  [415] = {.index = 2466, .length = 8},
  [416] = {.index = 2474, .length = 8},
  [417] = {.index = 2482, .length = 8},
  [418] = {.index = 2490, .length = 8},
  [419] = {.index = 2498, .length = 8},
  [420] = {.index = 2506, .length = 8},
  [421] = {.index = 2514, .length = 8},
  [422] = {.index = 2522, .length = 8},
  [423] = {.index = 2530, .length = 8},
  [424] = {.index = 2538, .length = 8},
  [425] = {.index = 2546, .length = 8},
  [426] = {.index = 2554, .length = 8},
  [427] = {.index = 2562, .length = 8},
  [428] = {.index = 2570, .length = 8},
  [429] = {.index = 2578, .length = 8},
  [430] = {.index = 2586, .length = 8},
  [431] = {.index = 2594, .length = 8},
  [432] = {.index = 2602, .length = 8},
  [433] = {.index = 2610, .length = 8},
  [434] = {.index = 2618, .length = 8},
  [435] = {.index = 2626, .length = 8},
  [436] = {.index = 2634, .length = 8},
  [437] = {.index = 2642, .length = 8},
  [438] = {.index = 2650, .length = 8},
  [439] = {.index = 2658, .length = 8},
  [440] = {.index = 2666, .length = 8},
  [441] = {.index = 2674, .length = 8},
  [442] = {.index = 2682, .length = 8},
  [443] = {.index = 2690, .length = 8},
  [444] = {.index = 2698, .length = 8},
  [445] = {.index = 2706, .length = 8},
  [446] = {.index = 2714, .length = 8},
  [447] = {.index = 2722, .length = 8},
  [448] = {.index = 2730, .length = 8},
  [449] = {.index = 2738, .length = 8},
  [450] = {.index = 2746, .length = 8},
  [451] = {.index = 2754, .length = 8},
  [452] = {.index = 2762, .length = 8},
  [453] = {.index = 2770, .length = 8},
  [454] = {.index = 2778, .length = 8},
  [455] = {.index = 2786, .length = 8},
  [456] = {.index = 2794, .length = 8},
  [457] = {.index = 2802, .length = 8},
  [458] = {.index = 2810, .length = 8},
  [459] = {.index = 2818, .length = 8},
  [460] = {.index = 2826, .length = 8},
  [461] = {.index = 2834, .length = 8},
  [462] = {.index = 2842, .length = 8},
  [463] = {.index = 2850, .length = 8},
  [464] = {.index = 2858, .length = 8},
  [465] = {.index = 2866, .length = 8},
  [466] = {.index = 2874, .length = 8},
  [467] = {.index = 2882, .length = 8},
  [468] = {.index = 2890, .length = 8},
  [469] = {.index = 2898, .length = 8},
  [470] = {.index = 2906, .length = 2},
  [471] = {.index = 2908, .length = 9},
  [472] = {.index = 2917, .length = 9},
  [473] = {.index = 2926, .length = 9},
  [474] = {.index = 2935, .length = 9},
  [475] = {.index = 2944, .length = 9},
  [476] = {.index = 2953, .length = 9},
  [477] = {.index = 2962, .length = 9},
  [478] = {.index = 2971, .length = 9},
  [479] = {.index = 2980, .length = 9},
  [480] = {.index = 2989, .length = 9},
  [481] = {.index = 2998, .length = 9},
  [482] = {.index = 3007, .length = 9},
  [483] = {.index = 3016, .length = 9},
  [484] = {.index = 3025, .length = 9},
  [485] = {.index = 3034, .length = 9},
  [486] = {.index = 3043, .length = 9},
  [487] = {.index = 3052, .length = 9},
  [488] = {.index = 3061, .length = 9},
  [489] = {.index = 3070, .length = 9},
  [490] = {.index = 3079, .length = 9},
  [491] = {.index = 3088, .length = 9},
  [492] = {.index = 3097, .length = 9},
  [493] = {.index = 3106, .length = 9},
  [494] = {.index = 3115, .length = 9},
  [495] = {.index = 3124, .length = 9},
  [496] = {.index = 3133, .length = 9},
  [497] = {.index = 3142, .length = 9},
  [498] = {.index = 3151, .length = 9},
  [499] = {.index = 3160, .length = 9},
  [500] = {.index = 3169, .length = 9},
  [501] = {.index = 3178, .length = 9},
  [502] = {.index = 3187, .length = 9},
  [503] = {.index = 3196, .length = 9},
  [504] = {.index = 3205, .length = 9},
  [505] = {.index = 3214, .length = 9},
  [506] = {.index = 3223, .length = 9},
  [507] = {.index = 3232, .length = 10},
  [508] = {.index = 3242, .length = 10},
  [509] = {.index = 3252, .length = 10},
  [510] = {.index = 3262, .length = 10},
  [511] = {.index = 3272, .length = 10},
  [512] = {.index = 3282, .length = 10},
  [513] = {.index = 3292, .length = 10},
  [514] = {.index = 3302, .length = 10},
  [515] = {.index = 3312, .length = 10},
  [516] = {.index = 3322, .length = 11},
};

static const TSFieldMapEntry ts_field_map_entries[] = {
  [0] =
    {field_function_name, 0},
  [1] =
    {field_from_clause, 1},
    {field_select_clause, 0},
  [3] =
    {field_from_clause, 1},
    {field_select_clause, 0},
    {field_using_clause, 2},
  [6] =
    {field_from_clause, 1},
    {field_group_by_clause, 2},
    {field_select_clause, 0},
  [9] =
    {field_for_clause, 2},
    {field_from_clause, 1},
    {field_select_clause, 0},
  [12] =
    {field_from_clause, 1},
    {field_select_clause, 0},
    {field_where_clause, 2},
  [15] =
    {field_from_clause, 1},
    {field_select_clause, 0},
    {field_with_clause, 2},
  [18] =
    {field_from_clause, 1},
    {field_limit_clause, 2},
    {field_select_clause, 0},
  [21] =
    {field_from_clause, 1},
    {field_offset_clause, 2},
    {field_select_clause, 0},
  [24] =
    {field_from_clause, 1},
    {field_select_clause, 0},
    {field_update_clause, 2},
  [27] =
    {field_from_clause, 1},
    {field_order_by_clause, 2},
    {field_select_clause, 0},
  [30] =
    {field_function_name, 0, .inherited = true},
  [31] =
    {field_from_clause, 1},
    {field_group_by_clause, 3},
    {field_select_clause, 0},
    {field_using_clause, 2},
  [35] =
    {field_for_clause, 3},
    {field_from_clause, 1},
    {field_select_clause, 0},
    {field_using_clause, 2},
  [39] =
    {field_from_clause, 1},
    {field_select_clause, 0},
    {field_using_clause, 2},
    {field_where_clause, 3},
  [43] =
    {field_from_clause, 1},
    {field_select_clause, 0},
    {field_using_clause, 2},
    {field_with_clause, 3},
  [47] =
    {field_from_clause, 1},
    {field_limit_clause, 3},
    {field_select_clause, 0},
    {field_using_clause, 2},
  [51] =
    {field_from_clause, 1},
    {field_offset_clause, 3},
    {field_select_clause, 0},
    {field_using_clause, 2},
  [55] =
    {field_from_clause, 1},
    {field_select_clause, 0},
    {field_update_clause, 3},
    {field_using_clause, 2},
  [59] =
    {field_from_clause, 1},
    {field_order_by_clause, 3},
    {field_select_clause, 0},
    {field_using_clause, 2},
  [63] =
    {field_for_clause, 3},
    {field_from_clause, 1},
    {field_group_by_clause, 2},
    {field_select_clause, 0},
  [67] =
    {field_from_clause, 1},
    {field_group_by_clause, 2},
    {field_limit_clause, 3},
    {field_select_clause, 0},
  [71] =
    {field_from_clause, 1},
    {field_group_by_clause, 2},
    {field_offset_clause, 3},
    {field_select_clause, 0},
  [75] =
    {field_from_clause, 1},
    {field_group_by_clause, 2},
    {field_select_clause, 0},
    {field_update_clause, 3},
  [79] =
    {field_from_clause, 1},
    {field_group_by_clause, 2},
    {field_order_by_clause, 3},
    {field_select_clause, 0},
  [83] =
    {field_for_clause, 2},
    {field_from_clause, 1},
    {field_select_clause, 0},
    {field_update_clause, 3},
  [87] =
    {field_from_clause, 1},
    {field_group_by_clause, 3},
    {field_select_clause, 0},
    {field_where_clause, 2},
  [91] =
    {field_for_clause, 3},
    {field_from_clause, 1},
    {field_select_clause, 0},
    {field_where_clause, 2},
  [95] =
    {field_from_clause, 1},
    {field_select_clause, 0},
    {field_where_clause, 2},
    {field_with_clause, 3},
  [99] =
    {field_from_clause, 1},
    {field_limit_clause, 3},
    {field_select_clause, 0},
    {field_where_clause, 2},
  [103] =
    {field_from_clause, 1},
    {field_offset_clause, 3},
    {field_select_clause, 0},
    {field_where_clause, 2},
  [107] =
    {field_from_clause, 1},
    {field_select_clause, 0},
    {field_update_clause, 3},
    {field_where_clause, 2},
  [111] =
    {field_from_clause, 1},
    {field_order_by_clause, 3},
    {field_select_clause, 0},
    {field_where_clause, 2},
  [115] =
    {field_from_clause, 1},
    {field_group_by_clause, 3},
    {field_select_clause, 0},
    {field_with_clause, 2},
  [119] =
    {field_for_clause, 3},
    {field_from_clause, 1},
    {field_select_clause, 0},
    {field_with_clause, 2},
  [123] =
    {field_from_clause, 1},
    {field_limit_clause, 3},
    {field_select_clause, 0},
    {field_with_clause, 2},
  [127] =
    {field_from_clause, 1},
    {field_offset_clause, 3},
    {field_select_clause, 0},
    {field_with_clause, 2},
  [131] =
    {field_from_clause, 1},
    {field_select_clause, 0},
    {field_update_clause, 3},
    {field_with_clause, 2},
  [135] =
    {field_from_clause, 1},
    {field_order_by_clause, 3},
    {field_select_clause, 0},
    {field_with_clause, 2},
  [139] =
    {field_for_clause, 3},
    {field_from_clause, 1},
    {field_limit_clause, 2},
    {field_select_clause, 0},
  [143] =
    {field_from_clause, 1},
    {field_limit_clause, 2},
    {field_offset_clause, 3},
    {field_select_clause, 0},
  [147] =
    {field_from_clause, 1},
    {field_limit_clause, 2},
    {field_select_clause, 0},
    {field_update_clause, 3},
  [151] =
    {field_for_clause, 3},
    {field_from_clause, 1},
    {field_offset_clause, 2},
    {field_select_clause, 0},
  [155] =
    {field_from_clause, 1},
    {field_offset_clause, 2},
    {field_select_clause, 0},
    {field_update_clause, 3},
  [159] =
    {field_for_clause, 3},
    {field_from_clause, 1},
    {field_order_by_clause, 2},
    {field_select_clause, 0},
  [163] =
    {field_from_clause, 1},
    {field_limit_clause, 3},
    {field_order_by_clause, 2},
    {field_select_clause, 0},
  [167] =
    {field_from_clause, 1},
    {field_offset_clause, 3},
    {field_order_by_clause, 2},
    {field_select_clause, 0},
  [171] =
    {field_from_clause, 1},
    {field_order_by_clause, 2},
    {field_select_clause, 0},
    {field_update_clause, 3},
  [175] =
    {field_lookup_field, 1},
  [176] =
    {field_for_clause, 4},
    {field_from_clause, 1},
    {field_group_by_clause, 3},
    {field_select_clause, 0},
    {field_using_clause, 2},
  [181] =
    {field_from_clause, 1},
    {field_group_by_clause, 3},
    {field_limit_clause, 4},
    {field_select_clause, 0},
    {field_using_clause, 2},
  [186] =
    {field_from_clause, 1},
    {field_group_by_clause, 3},
    {field_offset_clause, 4},
    {field_select_clause, 0},
    {field_using_clause, 2},
  [191] =
    {field_from_clause, 1},
    {field_group_by_clause, 3},
    {field_select_clause, 0},
    {field_update_clause, 4},
    {field_using_clause, 2},
  [196] =
    {field_from_clause, 1},
    {field_group_by_clause, 3},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_using_clause, 2},
  [201] =
    {field_for_clause, 3},
    {field_from_clause, 1},
    {field_select_clause, 0},
    {field_update_clause, 4},
    {field_using_clause, 2},
  [206] =
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_select_clause, 0},
    {field_using_clause, 2},
    {field_where_clause, 3},
  [211] =
    {field_for_clause, 4},
    {field_from_clause, 1},
    {field_select_clause, 0},
    {field_using_clause, 2},
    {field_where_clause, 3},
  [216] =
    {field_from_clause, 1},
    {field_select_clause, 0},
    {field_using_clause, 2},
    {field_where_clause, 3},
    {field_with_clause, 4},
  [221] =
    {field_from_clause, 1},
    {field_limit_clause, 4},
    {field_select_clause, 0},
    {field_using_clause, 2},
    {field_where_clause, 3},
  [226] =
    {field_from_clause, 1},
    {field_offset_clause, 4},
    {field_select_clause, 0},
    {field_using_clause, 2},
    {field_where_clause, 3},
  [231] =
    {field_from_clause, 1},
    {field_select_clause, 0},
    {field_update_clause, 4},
    {field_using_clause, 2},
    {field_where_clause, 3},
  [236] =
    {field_from_clause, 1},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_using_clause, 2},
    {field_where_clause, 3},
  [241] =
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_select_clause, 0},
    {field_using_clause, 2},
    {field_with_clause, 3},
  [246] =
    {field_for_clause, 4},
    {field_from_clause, 1},
    {field_select_clause, 0},
    {field_using_clause, 2},
    {field_with_clause, 3},
  [251] =
    {field_from_clause, 1},
    {field_limit_clause, 4},
    {field_select_clause, 0},
    {field_using_clause, 2},
    {field_with_clause, 3},
  [256] =
    {field_from_clause, 1},
    {field_offset_clause, 4},
    {field_select_clause, 0},
    {field_using_clause, 2},
    {field_with_clause, 3},
  [261] =
    {field_from_clause, 1},
    {field_select_clause, 0},
    {field_update_clause, 4},
    {field_using_clause, 2},
    {field_with_clause, 3},
  [266] =
    {field_from_clause, 1},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_using_clause, 2},
    {field_with_clause, 3},
  [271] =
    {field_for_clause, 4},
    {field_from_clause, 1},
    {field_limit_clause, 3},
    {field_select_clause, 0},
    {field_using_clause, 2},
  [276] =
    {field_from_clause, 1},
    {field_limit_clause, 3},
    {field_offset_clause, 4},
    {field_select_clause, 0},
    {field_using_clause, 2},
  [281] =
    {field_from_clause, 1},
    {field_limit_clause, 3},
    {field_select_clause, 0},
    {field_update_clause, 4},
    {field_using_clause, 2},
  [286] =
    {field_for_clause, 4},
    {field_from_clause, 1},
    {field_offset_clause, 3},
    {field_select_clause, 0},
    {field_using_clause, 2},
  [291] =
    {field_from_clause, 1},
    {field_offset_clause, 3},
    {field_select_clause, 0},
    {field_update_clause, 4},
    {field_using_clause, 2},
  [296] =
    {field_for_clause, 4},
    {field_from_clause, 1},
    {field_order_by_clause, 3},
    {field_select_clause, 0},
    {field_using_clause, 2},
  [301] =
    {field_from_clause, 1},
    {field_limit_clause, 4},
    {field_order_by_clause, 3},
    {field_select_clause, 0},
    {field_using_clause, 2},
  [306] =
    {field_from_clause, 1},
    {field_offset_clause, 4},
    {field_order_by_clause, 3},
    {field_select_clause, 0},
    {field_using_clause, 2},
  [311] =
    {field_from_clause, 1},
    {field_order_by_clause, 3},
    {field_select_clause, 0},
    {field_update_clause, 4},
    {field_using_clause, 2},
  [316] =
    {field_for_clause, 3},
    {field_from_clause, 1},
    {field_group_by_clause, 2},
    {field_select_clause, 0},
    {field_update_clause, 4},
  [321] =
    {field_for_clause, 4},
    {field_from_clause, 1},
    {field_group_by_clause, 2},
    {field_limit_clause, 3},
    {field_select_clause, 0},
  [326] =
    {field_from_clause, 1},
    {field_group_by_clause, 2},
    {field_limit_clause, 3},
    {field_offset_clause, 4},
    {field_select_clause, 0},
  [331] =
    {field_from_clause, 1},
    {field_group_by_clause, 2},
    {field_limit_clause, 3},
    {field_select_clause, 0},
    {field_update_clause, 4},
  [336] =
    {field_for_clause, 4},
    {field_from_clause, 1},
    {field_group_by_clause, 2},
    {field_offset_clause, 3},
    {field_select_clause, 0},
  [341] =
    {field_from_clause, 1},
    {field_group_by_clause, 2},
    {field_offset_clause, 3},
    {field_select_clause, 0},
    {field_update_clause, 4},
  [346] =
    {field_for_clause, 4},
    {field_from_clause, 1},
    {field_group_by_clause, 2},
    {field_order_by_clause, 3},
    {field_select_clause, 0},
  [351] =
    {field_from_clause, 1},
    {field_group_by_clause, 2},
    {field_limit_clause, 4},
    {field_order_by_clause, 3},
    {field_select_clause, 0},
  [356] =
    {field_from_clause, 1},
    {field_group_by_clause, 2},
    {field_offset_clause, 4},
    {field_order_by_clause, 3},
    {field_select_clause, 0},
  [361] =
    {field_from_clause, 1},
    {field_group_by_clause, 2},
    {field_order_by_clause, 3},
    {field_select_clause, 0},
    {field_update_clause, 4},
  [366] =
    {field_for_clause, 4},
    {field_from_clause, 1},
    {field_group_by_clause, 3},
    {field_select_clause, 0},
    {field_where_clause, 2},
  [371] =
    {field_from_clause, 1},
    {field_group_by_clause, 3},
    {field_limit_clause, 4},
    {field_select_clause, 0},
    {field_where_clause, 2},
  [376] =
    {field_from_clause, 1},
    {field_group_by_clause, 3},
    {field_offset_clause, 4},
    {field_select_clause, 0},
    {field_where_clause, 2},
  [381] =
    {field_from_clause, 1},
    {field_group_by_clause, 3},
    {field_select_clause, 0},
    {field_update_clause, 4},
    {field_where_clause, 2},
  [386] =
    {field_from_clause, 1},
    {field_group_by_clause, 3},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_where_clause, 2},
  [391] =
    {field_for_clause, 3},
    {field_from_clause, 1},
    {field_select_clause, 0},
    {field_update_clause, 4},
    {field_where_clause, 2},
  [396] =
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_select_clause, 0},
    {field_where_clause, 2},
    {field_with_clause, 3},
  [401] =
    {field_for_clause, 4},
    {field_from_clause, 1},
    {field_select_clause, 0},
    {field_where_clause, 2},
    {field_with_clause, 3},
  [406] =
    {field_from_clause, 1},
    {field_limit_clause, 4},
    {field_select_clause, 0},
    {field_where_clause, 2},
    {field_with_clause, 3},
  [411] =
    {field_from_clause, 1},
    {field_offset_clause, 4},
    {field_select_clause, 0},
    {field_where_clause, 2},
    {field_with_clause, 3},
  [416] =
    {field_from_clause, 1},
    {field_select_clause, 0},
    {field_update_clause, 4},
    {field_where_clause, 2},
    {field_with_clause, 3},
  [421] =
    {field_from_clause, 1},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_where_clause, 2},
    {field_with_clause, 3},
  [426] =
    {field_for_clause, 4},
    {field_from_clause, 1},
    {field_limit_clause, 3},
    {field_select_clause, 0},
    {field_where_clause, 2},
  [431] =
    {field_from_clause, 1},
    {field_limit_clause, 3},
    {field_offset_clause, 4},
    {field_select_clause, 0},
    {field_where_clause, 2},
  [436] =
    {field_from_clause, 1},
    {field_limit_clause, 3},
    {field_select_clause, 0},
    {field_update_clause, 4},
    {field_where_clause, 2},
  [441] =
    {field_for_clause, 4},
    {field_from_clause, 1},
    {field_offset_clause, 3},
    {field_select_clause, 0},
    {field_where_clause, 2},
  [446] =
    {field_from_clause, 1},
    {field_offset_clause, 3},
    {field_select_clause, 0},
    {field_update_clause, 4},
    {field_where_clause, 2},
  [451] =
    {field_for_clause, 4},
    {field_from_clause, 1},
    {field_order_by_clause, 3},
    {field_select_clause, 0},
    {field_where_clause, 2},
  [456] =
    {field_from_clause, 1},
    {field_limit_clause, 4},
    {field_order_by_clause, 3},
    {field_select_clause, 0},
    {field_where_clause, 2},
  [461] =
    {field_from_clause, 1},
    {field_offset_clause, 4},
    {field_order_by_clause, 3},
    {field_select_clause, 0},
    {field_where_clause, 2},
  [466] =
    {field_from_clause, 1},
    {field_order_by_clause, 3},
    {field_select_clause, 0},
    {field_update_clause, 4},
    {field_where_clause, 2},
  [471] =
    {field_for_clause, 4},
    {field_from_clause, 1},
    {field_group_by_clause, 3},
    {field_select_clause, 0},
    {field_with_clause, 2},
  [476] =
    {field_from_clause, 1},
    {field_group_by_clause, 3},
    {field_limit_clause, 4},
    {field_select_clause, 0},
    {field_with_clause, 2},
  [481] =
    {field_from_clause, 1},
    {field_group_by_clause, 3},
    {field_offset_clause, 4},
    {field_select_clause, 0},
    {field_with_clause, 2},
  [486] =
    {field_from_clause, 1},
    {field_group_by_clause, 3},
    {field_select_clause, 0},
    {field_update_clause, 4},
    {field_with_clause, 2},
  [491] =
    {field_from_clause, 1},
    {field_group_by_clause, 3},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_with_clause, 2},
  [496] =
    {field_for_clause, 3},
    {field_from_clause, 1},
    {field_select_clause, 0},
    {field_update_clause, 4},
    {field_with_clause, 2},
  [501] =
    {field_for_clause, 4},
    {field_from_clause, 1},
    {field_limit_clause, 3},
    {field_select_clause, 0},
    {field_with_clause, 2},
  [506] =
    {field_from_clause, 1},
    {field_limit_clause, 3},
    {field_offset_clause, 4},
    {field_select_clause, 0},
    {field_with_clause, 2},
  [511] =
    {field_from_clause, 1},
    {field_limit_clause, 3},
    {field_select_clause, 0},
    {field_update_clause, 4},
    {field_with_clause, 2},
  [516] =
    {field_for_clause, 4},
    {field_from_clause, 1},
    {field_offset_clause, 3},
    {field_select_clause, 0},
    {field_with_clause, 2},
  [521] =
    {field_from_clause, 1},
    {field_offset_clause, 3},
    {field_select_clause, 0},
    {field_update_clause, 4},
    {field_with_clause, 2},
  [526] =
    {field_for_clause, 4},
    {field_from_clause, 1},
    {field_order_by_clause, 3},
    {field_select_clause, 0},
    {field_with_clause, 2},
  [531] =
    {field_from_clause, 1},
    {field_limit_clause, 4},
    {field_order_by_clause, 3},
    {field_select_clause, 0},
    {field_with_clause, 2},
  [536] =
    {field_from_clause, 1},
    {field_offset_clause, 4},
    {field_order_by_clause, 3},
    {field_select_clause, 0},
    {field_with_clause, 2},
  [541] =
    {field_from_clause, 1},
    {field_order_by_clause, 3},
    {field_select_clause, 0},
    {field_update_clause, 4},
    {field_with_clause, 2},
  [546] =
    {field_for_clause, 3},
    {field_from_clause, 1},
    {field_limit_clause, 2},
    {field_select_clause, 0},
    {field_update_clause, 4},
  [551] =
    {field_for_clause, 4},
    {field_from_clause, 1},
    {field_limit_clause, 2},
    {field_offset_clause, 3},
    {field_select_clause, 0},
  [556] =
    {field_from_clause, 1},
    {field_limit_clause, 2},
    {field_offset_clause, 3},
    {field_select_clause, 0},
    {field_update_clause, 4},
  [561] =
    {field_for_clause, 3},
    {field_from_clause, 1},
    {field_offset_clause, 2},
    {field_select_clause, 0},
    {field_update_clause, 4},
  [566] =
    {field_for_clause, 3},
    {field_from_clause, 1},
    {field_order_by_clause, 2},
    {field_select_clause, 0},
    {field_update_clause, 4},
  [571] =
    {field_for_clause, 4},
    {field_from_clause, 1},
    {field_limit_clause, 3},
    {field_order_by_clause, 2},
    {field_select_clause, 0},
  [576] =
    {field_from_clause, 1},
    {field_limit_clause, 3},
    {field_offset_clause, 4},
    {field_order_by_clause, 2},
    {field_select_clause, 0},
  [581] =
    {field_from_clause, 1},
    {field_limit_clause, 3},
    {field_order_by_clause, 2},
    {field_select_clause, 0},
    {field_update_clause, 4},
  [586] =
    {field_for_clause, 4},
    {field_from_clause, 1},
    {field_offset_clause, 3},
    {field_order_by_clause, 2},
    {field_select_clause, 0},
  [591] =
    {field_from_clause, 1},
    {field_offset_clause, 3},
    {field_order_by_clause, 2},
    {field_select_clause, 0},
    {field_update_clause, 4},
  [596] =
    {field_for_clause, 4},
    {field_from_clause, 1},
    {field_group_by_clause, 3},
    {field_select_clause, 0},
    {field_update_clause, 5},
    {field_using_clause, 2},
  [602] =
    {field_for_clause, 5},
    {field_from_clause, 1},
    {field_group_by_clause, 3},
    {field_limit_clause, 4},
    {field_select_clause, 0},
    {field_using_clause, 2},
  [608] =
    {field_from_clause, 1},
    {field_group_by_clause, 3},
    {field_limit_clause, 4},
    {field_offset_clause, 5},
    {field_select_clause, 0},
    {field_using_clause, 2},
  [614] =
    {field_from_clause, 1},
    {field_group_by_clause, 3},
    {field_limit_clause, 4},
    {field_select_clause, 0},
    {field_update_clause, 5},
    {field_using_clause, 2},
  [620] =
    {field_for_clause, 5},
    {field_from_clause, 1},
    {field_group_by_clause, 3},
    {field_offset_clause, 4},
    {field_select_clause, 0},
    {field_using_clause, 2},
  [626] =
    {field_from_clause, 1},
    {field_group_by_clause, 3},
    {field_offset_clause, 4},
    {field_select_clause, 0},
    {field_update_clause, 5},
    {field_using_clause, 2},
  [632] =
    {field_for_clause, 5},
    {field_from_clause, 1},
    {field_group_by_clause, 3},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_using_clause, 2},
  [638] =
    {field_from_clause, 1},
    {field_group_by_clause, 3},
    {field_limit_clause, 5},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_using_clause, 2},
  [644] =
    {field_from_clause, 1},
    {field_group_by_clause, 3},
    {field_offset_clause, 5},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_using_clause, 2},
  [650] =
    {field_from_clause, 1},
    {field_group_by_clause, 3},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_update_clause, 5},
    {field_using_clause, 2},
  [656] =
    {field_for_clause, 5},
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_select_clause, 0},
    {field_using_clause, 2},
    {field_where_clause, 3},
  [662] =
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_limit_clause, 5},
    {field_select_clause, 0},
    {field_using_clause, 2},
    {field_where_clause, 3},
  [668] =
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_offset_clause, 5},
    {field_select_clause, 0},
    {field_using_clause, 2},
    {field_where_clause, 3},
  [674] =
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_select_clause, 0},
    {field_update_clause, 5},
    {field_using_clause, 2},
    {field_where_clause, 3},
  [680] =
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_order_by_clause, 5},
    {field_select_clause, 0},
    {field_using_clause, 2},
    {field_where_clause, 3},
  [686] =
    {field_for_clause, 4},
    {field_from_clause, 1},
    {field_select_clause, 0},
    {field_update_clause, 5},
    {field_using_clause, 2},
    {field_where_clause, 3},
  [692] =
    {field_from_clause, 1},
    {field_group_by_clause, 5},
    {field_select_clause, 0},
    {field_using_clause, 2},
    {field_where_clause, 3},
    {field_with_clause, 4},
  [698] =
    {field_for_clause, 5},
    {field_from_clause, 1},
    {field_select_clause, 0},
    {field_using_clause, 2},
    {field_where_clause, 3},
    {field_with_clause, 4},
  [704] =
    {field_from_clause, 1},
    {field_limit_clause, 5},
    {field_select_clause, 0},
    {field_using_clause, 2},
    {field_where_clause, 3},
    {field_with_clause, 4},
  [710] =
    {field_from_clause, 1},
    {field_offset_clause, 5},
    {field_select_clause, 0},
    {field_using_clause, 2},
    {field_where_clause, 3},
    {field_with_clause, 4},
  [716] =
    {field_from_clause, 1},
    {field_select_clause, 0},
    {field_update_clause, 5},
    {field_using_clause, 2},
    {field_where_clause, 3},
    {field_with_clause, 4},
  [722] =
    {field_from_clause, 1},
    {field_order_by_clause, 5},
    {field_select_clause, 0},
    {field_using_clause, 2},
    {field_where_clause, 3},
    {field_with_clause, 4},
  [728] =
    {field_for_clause, 5},
    {field_from_clause, 1},
    {field_limit_clause, 4},
    {field_select_clause, 0},
    {field_using_clause, 2},
    {field_where_clause, 3},
  [734] =
    {field_from_clause, 1},
    {field_limit_clause, 4},
    {field_offset_clause, 5},
    {field_select_clause, 0},
    {field_using_clause, 2},
    {field_where_clause, 3},
  [740] =
    {field_from_clause, 1},
    {field_limit_clause, 4},
    {field_select_clause, 0},
    {field_update_clause, 5},
    {field_using_clause, 2},
    {field_where_clause, 3},
  [746] =
    {field_for_clause, 5},
    {field_from_clause, 1},
    {field_offset_clause, 4},
    {field_select_clause, 0},
    {field_using_clause, 2},
    {field_where_clause, 3},
  [752] =
    {field_from_clause, 1},
    {field_offset_clause, 4},
    {field_select_clause, 0},
    {field_update_clause, 5},
    {field_using_clause, 2},
    {field_where_clause, 3},
  [758] =
    {field_for_clause, 5},
    {field_from_clause, 1},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_using_clause, 2},
    {field_where_clause, 3},
  [764] =
    {field_from_clause, 1},
    {field_limit_clause, 5},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_using_clause, 2},
    {field_where_clause, 3},
  [770] =
    {field_from_clause, 1},
    {field_offset_clause, 5},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_using_clause, 2},
    {field_where_clause, 3},
  [776] =
    {field_from_clause, 1},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_update_clause, 5},
    {field_using_clause, 2},
    {field_where_clause, 3},
  [782] =
    {field_for_clause, 5},
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_select_clause, 0},
    {field_using_clause, 2},
    {field_with_clause, 3},
  [788] =
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_limit_clause, 5},
    {field_select_clause, 0},
    {field_using_clause, 2},
    {field_with_clause, 3},
  [794] =
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_offset_clause, 5},
    {field_select_clause, 0},
    {field_using_clause, 2},
    {field_with_clause, 3},
  [800] =
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_select_clause, 0},
    {field_update_clause, 5},
    {field_using_clause, 2},
    {field_with_clause, 3},
  [806] =
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_order_by_clause, 5},
    {field_select_clause, 0},
    {field_using_clause, 2},
    {field_with_clause, 3},
  [812] =
    {field_for_clause, 4},
    {field_from_clause, 1},
    {field_select_clause, 0},
    {field_update_clause, 5},
    {field_using_clause, 2},
    {field_with_clause, 3},
  [818] =
    {field_for_clause, 5},
    {field_from_clause, 1},
    {field_limit_clause, 4},
    {field_select_clause, 0},
    {field_using_clause, 2},
    {field_with_clause, 3},
  [824] =
    {field_from_clause, 1},
    {field_limit_clause, 4},
    {field_offset_clause, 5},
    {field_select_clause, 0},
    {field_using_clause, 2},
    {field_with_clause, 3},
  [830] =
    {field_from_clause, 1},
    {field_limit_clause, 4},
    {field_select_clause, 0},
    {field_update_clause, 5},
    {field_using_clause, 2},
    {field_with_clause, 3},
  [836] =
    {field_for_clause, 5},
    {field_from_clause, 1},
    {field_offset_clause, 4},
    {field_select_clause, 0},
    {field_using_clause, 2},
    {field_with_clause, 3},
  [842] =
    {field_from_clause, 1},
    {field_offset_clause, 4},
    {field_select_clause, 0},
    {field_update_clause, 5},
    {field_using_clause, 2},
    {field_with_clause, 3},
  [848] =
    {field_for_clause, 5},
    {field_from_clause, 1},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_using_clause, 2},
    {field_with_clause, 3},
  [854] =
    {field_from_clause, 1},
    {field_limit_clause, 5},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_using_clause, 2},
    {field_with_clause, 3},
  [860] =
    {field_from_clause, 1},
    {field_offset_clause, 5},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_using_clause, 2},
    {field_with_clause, 3},
  [866] =
    {field_from_clause, 1},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_update_clause, 5},
    {field_using_clause, 2},
    {field_with_clause, 3},
  [872] =
    {field_for_clause, 4},
    {field_from_clause, 1},
    {field_limit_clause, 3},
    {field_select_clause, 0},
    {field_update_clause, 5},
    {field_using_clause, 2},
  [878] =
    {field_for_clause, 5},
    {field_from_clause, 1},
    {field_limit_clause, 3},
    {field_offset_clause, 4},
    {field_select_clause, 0},
    {field_using_clause, 2},
  [884] =
    {field_from_clause, 1},
    {field_limit_clause, 3},
    {field_offset_clause, 4},
    {field_select_clause, 0},
    {field_update_clause, 5},
    {field_using_clause, 2},
  [890] =
    {field_for_clause, 4},
    {field_from_clause, 1},
    {field_offset_clause, 3},
    {field_select_clause, 0},
    {field_update_clause, 5},
    {field_using_clause, 2},
  [896] =
    {field_for_clause, 4},
    {field_from_clause, 1},
    {field_order_by_clause, 3},
    {field_select_clause, 0},
    {field_update_clause, 5},
    {field_using_clause, 2},
  [902] =
    {field_for_clause, 5},
    {field_from_clause, 1},
    {field_limit_clause, 4},
    {field_order_by_clause, 3},
    {field_select_clause, 0},
    {field_using_clause, 2},
  [908] =
    {field_from_clause, 1},
    {field_limit_clause, 4},
    {field_offset_clause, 5},
    {field_order_by_clause, 3},
    {field_select_clause, 0},
    {field_using_clause, 2},
  [914] =
    {field_from_clause, 1},
    {field_limit_clause, 4},
    {field_order_by_clause, 3},
    {field_select_clause, 0},
    {field_update_clause, 5},
    {field_using_clause, 2},
  [920] =
    {field_for_clause, 5},
    {field_from_clause, 1},
    {field_offset_clause, 4},
    {field_order_by_clause, 3},
    {field_select_clause, 0},
    {field_using_clause, 2},
  [926] =
    {field_from_clause, 1},
    {field_offset_clause, 4},
    {field_order_by_clause, 3},
    {field_select_clause, 0},
    {field_update_clause, 5},
    {field_using_clause, 2},
  [932] =
    {field_for_clause, 4},
    {field_from_clause, 1},
    {field_group_by_clause, 2},
    {field_limit_clause, 3},
    {field_select_clause, 0},
    {field_update_clause, 5},
  [938] =
    {field_for_clause, 5},
    {field_from_clause, 1},
    {field_group_by_clause, 2},
    {field_limit_clause, 3},
    {field_offset_clause, 4},
    {field_select_clause, 0},
  [944] =
    {field_from_clause, 1},
    {field_group_by_clause, 2},
    {field_limit_clause, 3},
    {field_offset_clause, 4},
    {field_select_clause, 0},
    {field_update_clause, 5},
  [950] =
    {field_for_clause, 4},
    {field_from_clause, 1},
    {field_group_by_clause, 2},
    {field_offset_clause, 3},
    {field_select_clause, 0},
    {field_update_clause, 5},
  [956] =
    {field_for_clause, 4},
    {field_from_clause, 1},
    {field_group_by_clause, 2},
    {field_order_by_clause, 3},
    {field_select_clause, 0},
    {field_update_clause, 5},
  [962] =
    {field_for_clause, 5},
    {field_from_clause, 1},
    {field_group_by_clause, 2},
    {field_limit_clause, 4},
    {field_order_by_clause, 3},
    {field_select_clause, 0},
  [968] =
    {field_from_clause, 1},
    {field_group_by_clause, 2},
    {field_limit_clause, 4},
    {field_offset_clause, 5},
    {field_order_by_clause, 3},
    {field_select_clause, 0},
  [974] =
    {field_from_clause, 1},
    {field_group_by_clause, 2},
    {field_limit_clause, 4},
    {field_order_by_clause, 3},
    {field_select_clause, 0},
    {field_update_clause, 5},
  [980] =
    {field_for_clause, 5},
    {field_from_clause, 1},
    {field_group_by_clause, 2},
    {field_offset_clause, 4},
    {field_order_by_clause, 3},
    {field_select_clause, 0},
  [986] =
    {field_from_clause, 1},
    {field_group_by_clause, 2},
    {field_offset_clause, 4},
    {field_order_by_clause, 3},
    {field_select_clause, 0},
    {field_update_clause, 5},
  [992] =
    {field_for_clause, 4},
    {field_from_clause, 1},
    {field_group_by_clause, 3},
    {field_select_clause, 0},
    {field_update_clause, 5},
    {field_where_clause, 2},
  [998] =
    {field_for_clause, 5},
    {field_from_clause, 1},
    {field_group_by_clause, 3},
    {field_limit_clause, 4},
    {field_select_clause, 0},
    {field_where_clause, 2},
  [1004] =
    {field_from_clause, 1},
    {field_group_by_clause, 3},
    {field_limit_clause, 4},
    {field_offset_clause, 5},
    {field_select_clause, 0},
    {field_where_clause, 2},
  [1010] =
    {field_from_clause, 1},
    {field_group_by_clause, 3},
    {field_limit_clause, 4},
    {field_select_clause, 0},
    {field_update_clause, 5},
    {field_where_clause, 2},
  [1016] =
    {field_for_clause, 5},
    {field_from_clause, 1},
    {field_group_by_clause, 3},
    {field_offset_clause, 4},
    {field_select_clause, 0},
    {field_where_clause, 2},
  [1022] =
    {field_from_clause, 1},
    {field_group_by_clause, 3},
    {field_offset_clause, 4},
    {field_select_clause, 0},
    {field_update_clause, 5},
    {field_where_clause, 2},
  [1028] =
    {field_for_clause, 5},
    {field_from_clause, 1},
    {field_group_by_clause, 3},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_where_clause, 2},
  [1034] =
    {field_from_clause, 1},
    {field_group_by_clause, 3},
    {field_limit_clause, 5},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_where_clause, 2},
  [1040] =
    {field_from_clause, 1},
    {field_group_by_clause, 3},
    {field_offset_clause, 5},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_where_clause, 2},
  [1046] =
    {field_from_clause, 1},
    {field_group_by_clause, 3},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_update_clause, 5},
    {field_where_clause, 2},
  [1052] =
    {field_for_clause, 5},
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_select_clause, 0},
    {field_where_clause, 2},
    {field_with_clause, 3},
  [1058] =
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_limit_clause, 5},
    {field_select_clause, 0},
    {field_where_clause, 2},
    {field_with_clause, 3},
  [1064] =
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_offset_clause, 5},
    {field_select_clause, 0},
    {field_where_clause, 2},
    {field_with_clause, 3},
  [1070] =
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_select_clause, 0},
    {field_update_clause, 5},
    {field_where_clause, 2},
    {field_with_clause, 3},
  [1076] =
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_order_by_clause, 5},
    {field_select_clause, 0},
    {field_where_clause, 2},
    {field_with_clause, 3},
  [1082] =
    {field_for_clause, 4},
    {field_from_clause, 1},
    {field_select_clause, 0},
    {field_update_clause, 5},
    {field_where_clause, 2},
    {field_with_clause, 3},
  [1088] =
    {field_for_clause, 5},
    {field_from_clause, 1},
    {field_limit_clause, 4},
    {field_select_clause, 0},
    {field_where_clause, 2},
    {field_with_clause, 3},
  [1094] =
    {field_from_clause, 1},
    {field_limit_clause, 4},
    {field_offset_clause, 5},
    {field_select_clause, 0},
    {field_where_clause, 2},
    {field_with_clause, 3},
  [1100] =
    {field_from_clause, 1},
    {field_limit_clause, 4},
    {field_select_clause, 0},
    {field_update_clause, 5},
    {field_where_clause, 2},
    {field_with_clause, 3},
  [1106] =
    {field_for_clause, 5},
    {field_from_clause, 1},
    {field_offset_clause, 4},
    {field_select_clause, 0},
    {field_where_clause, 2},
    {field_with_clause, 3},
  [1112] =
    {field_from_clause, 1},
    {field_offset_clause, 4},
    {field_select_clause, 0},
    {field_update_clause, 5},
    {field_where_clause, 2},
    {field_with_clause, 3},
  [1118] =
    {field_for_clause, 5},
    {field_from_clause, 1},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_where_clause, 2},
    {field_with_clause, 3},
  [1124] =
    {field_from_clause, 1},
    {field_limit_clause, 5},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_where_clause, 2},
    {field_with_clause, 3},
  [1130] =
    {field_from_clause, 1},
    {field_offset_clause, 5},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_where_clause, 2},
    {field_with_clause, 3},
  [1136] =
    {field_from_clause, 1},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_update_clause, 5},
    {field_where_clause, 2},
    {field_with_clause, 3},
  [1142] =
    {field_for_clause, 4},
    {field_from_clause, 1},
    {field_limit_clause, 3},
    {field_select_clause, 0},
    {field_update_clause, 5},
    {field_where_clause, 2},
  [1148] =
    {field_for_clause, 5},
    {field_from_clause, 1},
    {field_limit_clause, 3},
    {field_offset_clause, 4},
    {field_select_clause, 0},
    {field_where_clause, 2},
  [1154] =
    {field_from_clause, 1},
    {field_limit_clause, 3},
    {field_offset_clause, 4},
    {field_select_clause, 0},
    {field_update_clause, 5},
    {field_where_clause, 2},
  [1160] =
    {field_for_clause, 4},
    {field_from_clause, 1},
    {field_offset_clause, 3},
    {field_select_clause, 0},
    {field_update_clause, 5},
    {field_where_clause, 2},
  [1166] =
    {field_for_clause, 4},
    {field_from_clause, 1},
    {field_order_by_clause, 3},
    {field_select_clause, 0},
    {field_update_clause, 5},
    {field_where_clause, 2},
  [1172] =
    {field_for_clause, 5},
    {field_from_clause, 1},
    {field_limit_clause, 4},
    {field_order_by_clause, 3},
    {field_select_clause, 0},
    {field_where_clause, 2},
  [1178] =
    {field_from_clause, 1},
    {field_limit_clause, 4},
    {field_offset_clause, 5},
    {field_order_by_clause, 3},
    {field_select_clause, 0},
    {field_where_clause, 2},
  [1184] =
    {field_from_clause, 1},
    {field_limit_clause, 4},
    {field_order_by_clause, 3},
    {field_select_clause, 0},
    {field_update_clause, 5},
    {field_where_clause, 2},
  [1190] =
    {field_for_clause, 5},
    {field_from_clause, 1},
    {field_offset_clause, 4},
    {field_order_by_clause, 3},
    {field_select_clause, 0},
    {field_where_clause, 2},
  [1196] =
    {field_from_clause, 1},
    {field_offset_clause, 4},
    {field_order_by_clause, 3},
    {field_select_clause, 0},
    {field_update_clause, 5},
    {field_where_clause, 2},
  [1202] =
    {field_for_clause, 4},
    {field_from_clause, 1},
    {field_group_by_clause, 3},
    {field_select_clause, 0},
    {field_update_clause, 5},
    {field_with_clause, 2},
  [1208] =
    {field_for_clause, 5},
    {field_from_clause, 1},
    {field_group_by_clause, 3},
    {field_limit_clause, 4},
    {field_select_clause, 0},
    {field_with_clause, 2},
  [1214] =
    {field_from_clause, 1},
    {field_group_by_clause, 3},
    {field_limit_clause, 4},
    {field_offset_clause, 5},
    {field_select_clause, 0},
    {field_with_clause, 2},
  [1220] =
    {field_from_clause, 1},
    {field_group_by_clause, 3},
    {field_limit_clause, 4},
    {field_select_clause, 0},
    {field_update_clause, 5},
    {field_with_clause, 2},
  [1226] =
    {field_for_clause, 5},
    {field_from_clause, 1},
    {field_group_by_clause, 3},
    {field_offset_clause, 4},
    {field_select_clause, 0},
    {field_with_clause, 2},
  [1232] =
    {field_from_clause, 1},
    {field_group_by_clause, 3},
    {field_offset_clause, 4},
    {field_select_clause, 0},
    {field_update_clause, 5},
    {field_with_clause, 2},
  [1238] =
    {field_for_clause, 5},
    {field_from_clause, 1},
    {field_group_by_clause, 3},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_with_clause, 2},
  [1244] =
    {field_from_clause, 1},
    {field_group_by_clause, 3},
    {field_limit_clause, 5},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_with_clause, 2},
  [1250] =
    {field_from_clause, 1},
    {field_group_by_clause, 3},
    {field_offset_clause, 5},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_with_clause, 2},
  [1256] =
    {field_from_clause, 1},
    {field_group_by_clause, 3},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_update_clause, 5},
    {field_with_clause, 2},
  [1262] =
    {field_for_clause, 4},
    {field_from_clause, 1},
    {field_limit_clause, 3},
    {field_select_clause, 0},
    {field_update_clause, 5},
    {field_with_clause, 2},
  [1268] =
    {field_for_clause, 5},
    {field_from_clause, 1},
    {field_limit_clause, 3},
    {field_offset_clause, 4},
    {field_select_clause, 0},
    {field_with_clause, 2},
  [1274] =
    {field_from_clause, 1},
    {field_limit_clause, 3},
    {field_offset_clause, 4},
    {field_select_clause, 0},
    {field_update_clause, 5},
    {field_with_clause, 2},
  [1280] =
    {field_for_clause, 4},
    {field_from_clause, 1},
    {field_offset_clause, 3},
    {field_select_clause, 0},
    {field_update_clause, 5},
    {field_with_clause, 2},
  [1286] =
    {field_for_clause, 4},
    {field_from_clause, 1},
    {field_order_by_clause, 3},
    {field_select_clause, 0},
    {field_update_clause, 5},
    {field_with_clause, 2},
  [1292] =
    {field_for_clause, 5},
    {field_from_clause, 1},
    {field_limit_clause, 4},
    {field_order_by_clause, 3},
    {field_select_clause, 0},
    {field_with_clause, 2},
  [1298] =
    {field_from_clause, 1},
    {field_limit_clause, 4},
    {field_offset_clause, 5},
    {field_order_by_clause, 3},
    {field_select_clause, 0},
    {field_with_clause, 2},
  [1304] =
    {field_from_clause, 1},
    {field_limit_clause, 4},
    {field_order_by_clause, 3},
    {field_select_clause, 0},
    {field_update_clause, 5},
    {field_with_clause, 2},
  [1310] =
    {field_for_clause, 5},
    {field_from_clause, 1},
    {field_offset_clause, 4},
    {field_order_by_clause, 3},
    {field_select_clause, 0},
    {field_with_clause, 2},
  [1316] =
    {field_from_clause, 1},
    {field_offset_clause, 4},
    {field_order_by_clause, 3},
    {field_select_clause, 0},
    {field_update_clause, 5},
    {field_with_clause, 2},
  [1322] =
    {field_for_clause, 4},
    {field_from_clause, 1},
    {field_limit_clause, 2},
    {field_offset_clause, 3},
    {field_select_clause, 0},
    {field_update_clause, 5},
  [1328] =
    {field_for_clause, 4},
    {field_from_clause, 1},
    {field_limit_clause, 3},
    {field_order_by_clause, 2},
    {field_select_clause, 0},
    {field_update_clause, 5},
  [1334] =
    {field_for_clause, 5},
    {field_from_clause, 1},
    {field_limit_clause, 3},
    {field_offset_clause, 4},
    {field_order_by_clause, 2},
    {field_select_clause, 0},
  [1340] =
    {field_from_clause, 1},
    {field_limit_clause, 3},
    {field_offset_clause, 4},
    {field_order_by_clause, 2},
    {field_select_clause, 0},
    {field_update_clause, 5},
  [1346] =
    {field_for_clause, 4},
    {field_from_clause, 1},
    {field_offset_clause, 3},
    {field_order_by_clause, 2},
    {field_select_clause, 0},
    {field_update_clause, 5},
  [1352] =
    {field_for_clause, 5},
    {field_from_clause, 1},
    {field_group_by_clause, 3},
    {field_limit_clause, 4},
    {field_select_clause, 0},
    {field_update_clause, 6},
    {field_using_clause, 2},
  [1359] =
    {field_for_clause, 6},
    {field_from_clause, 1},
    {field_group_by_clause, 3},
    {field_limit_clause, 4},
    {field_offset_clause, 5},
    {field_select_clause, 0},
    {field_using_clause, 2},
  [1366] =
    {field_from_clause, 1},
    {field_group_by_clause, 3},
    {field_limit_clause, 4},
    {field_offset_clause, 5},
    {field_select_clause, 0},
    {field_update_clause, 6},
    {field_using_clause, 2},
  [1373] =
    {field_for_clause, 5},
    {field_from_clause, 1},
    {field_group_by_clause, 3},
    {field_offset_clause, 4},
    {field_select_clause, 0},
    {field_update_clause, 6},
    {field_using_clause, 2},
  [1380] =
    {field_for_clause, 5},
    {field_from_clause, 1},
    {field_group_by_clause, 3},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_update_clause, 6},
    {field_using_clause, 2},
  [1387] =
    {field_for_clause, 6},
    {field_from_clause, 1},
    {field_group_by_clause, 3},
    {field_limit_clause, 5},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_using_clause, 2},
  [1394] =
    {field_from_clause, 1},
    {field_group_by_clause, 3},
    {field_limit_clause, 5},
    {field_offset_clause, 6},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_using_clause, 2},
  [1401] =
    {field_from_clause, 1},
    {field_group_by_clause, 3},
    {field_limit_clause, 5},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_update_clause, 6},
    {field_using_clause, 2},
  [1408] =
    {field_for_clause, 6},
    {field_from_clause, 1},
    {field_group_by_clause, 3},
    {field_offset_clause, 5},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_using_clause, 2},
  [1415] =
    {field_from_clause, 1},
    {field_group_by_clause, 3},
    {field_offset_clause, 5},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_update_clause, 6},
    {field_using_clause, 2},
  [1422] =
    {field_for_clause, 5},
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_select_clause, 0},
    {field_update_clause, 6},
    {field_using_clause, 2},
    {field_where_clause, 3},
  [1429] =
    {field_for_clause, 6},
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_limit_clause, 5},
    {field_select_clause, 0},
    {field_using_clause, 2},
    {field_where_clause, 3},
  [1436] =
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_limit_clause, 5},
    {field_offset_clause, 6},
    {field_select_clause, 0},
    {field_using_clause, 2},
    {field_where_clause, 3},
  [1443] =
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_limit_clause, 5},
    {field_select_clause, 0},
    {field_update_clause, 6},
    {field_using_clause, 2},
    {field_where_clause, 3},
  [1450] =
    {field_for_clause, 6},
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_offset_clause, 5},
    {field_select_clause, 0},
    {field_using_clause, 2},
    {field_where_clause, 3},
  [1457] =
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_offset_clause, 5},
    {field_select_clause, 0},
    {field_update_clause, 6},
    {field_using_clause, 2},
    {field_where_clause, 3},
  [1464] =
    {field_for_clause, 6},
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_order_by_clause, 5},
    {field_select_clause, 0},
    {field_using_clause, 2},
    {field_where_clause, 3},
  [1471] =
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_limit_clause, 6},
    {field_order_by_clause, 5},
    {field_select_clause, 0},
    {field_using_clause, 2},
    {field_where_clause, 3},
  [1478] =
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_offset_clause, 6},
    {field_order_by_clause, 5},
    {field_select_clause, 0},
    {field_using_clause, 2},
    {field_where_clause, 3},
  [1485] =
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_order_by_clause, 5},
    {field_select_clause, 0},
    {field_update_clause, 6},
    {field_using_clause, 2},
    {field_where_clause, 3},
  [1492] =
    {field_for_clause, 6},
    {field_from_clause, 1},
    {field_group_by_clause, 5},
    {field_select_clause, 0},
    {field_using_clause, 2},
    {field_where_clause, 3},
    {field_with_clause, 4},
  [1499] =
    {field_from_clause, 1},
    {field_group_by_clause, 5},
    {field_limit_clause, 6},
    {field_select_clause, 0},
    {field_using_clause, 2},
    {field_where_clause, 3},
    {field_with_clause, 4},
  [1506] =
    {field_from_clause, 1},
    {field_group_by_clause, 5},
    {field_offset_clause, 6},
    {field_select_clause, 0},
    {field_using_clause, 2},
    {field_where_clause, 3},
    {field_with_clause, 4},
  [1513] =
    {field_from_clause, 1},
    {field_group_by_clause, 5},
    {field_select_clause, 0},
    {field_update_clause, 6},
    {field_using_clause, 2},
    {field_where_clause, 3},
    {field_with_clause, 4},
  [1520] =
    {field_from_clause, 1},
    {field_group_by_clause, 5},
    {field_order_by_clause, 6},
    {field_select_clause, 0},
    {field_using_clause, 2},
    {field_where_clause, 3},
    {field_with_clause, 4},
  [1527] =
    {field_for_clause, 5},
    {field_from_clause, 1},
    {field_select_clause, 0},
    {field_update_clause, 6},
    {field_using_clause, 2},
    {field_where_clause, 3},
    {field_with_clause, 4},
  [1534] =
    {field_for_clause, 6},
    {field_from_clause, 1},
    {field_limit_clause, 5},
    {field_select_clause, 0},
    {field_using_clause, 2},
    {field_where_clause, 3},
    {field_with_clause, 4},
  [1541] =
    {field_from_clause, 1},
    {field_limit_clause, 5},
    {field_offset_clause, 6},
    {field_select_clause, 0},
    {field_using_clause, 2},
    {field_where_clause, 3},
    {field_with_clause, 4},
  [1548] =
    {field_from_clause, 1},
    {field_limit_clause, 5},
    {field_select_clause, 0},
    {field_update_clause, 6},
    {field_using_clause, 2},
    {field_where_clause, 3},
    {field_with_clause, 4},
  [1555] =
    {field_for_clause, 6},
    {field_from_clause, 1},
    {field_offset_clause, 5},
    {field_select_clause, 0},
    {field_using_clause, 2},
    {field_where_clause, 3},
    {field_with_clause, 4},
  [1562] =
    {field_from_clause, 1},
    {field_offset_clause, 5},
    {field_select_clause, 0},
    {field_update_clause, 6},
    {field_using_clause, 2},
    {field_where_clause, 3},
    {field_with_clause, 4},
  [1569] =
    {field_for_clause, 6},
    {field_from_clause, 1},
    {field_order_by_clause, 5},
    {field_select_clause, 0},
    {field_using_clause, 2},
    {field_where_clause, 3},
    {field_with_clause, 4},
  [1576] =
    {field_from_clause, 1},
    {field_limit_clause, 6},
    {field_order_by_clause, 5},
    {field_select_clause, 0},
    {field_using_clause, 2},
    {field_where_clause, 3},
    {field_with_clause, 4},
  [1583] =
    {field_from_clause, 1},
    {field_offset_clause, 6},
    {field_order_by_clause, 5},
    {field_select_clause, 0},
    {field_using_clause, 2},
    {field_where_clause, 3},
    {field_with_clause, 4},
  [1590] =
    {field_from_clause, 1},
    {field_order_by_clause, 5},
    {field_select_clause, 0},
    {field_update_clause, 6},
    {field_using_clause, 2},
    {field_where_clause, 3},
    {field_with_clause, 4},
  [1597] =
    {field_for_clause, 5},
    {field_from_clause, 1},
    {field_limit_clause, 4},
    {field_select_clause, 0},
    {field_update_clause, 6},
    {field_using_clause, 2},
    {field_where_clause, 3},
  [1604] =
    {field_for_clause, 6},
    {field_from_clause, 1},
    {field_limit_clause, 4},
    {field_offset_clause, 5},
    {field_select_clause, 0},
    {field_using_clause, 2},
    {field_where_clause, 3},
  [1611] =
    {field_from_clause, 1},
    {field_limit_clause, 4},
    {field_offset_clause, 5},
    {field_select_clause, 0},
    {field_update_clause, 6},
    {field_using_clause, 2},
    {field_where_clause, 3},
  [1618] =
    {field_for_clause, 5},
    {field_from_clause, 1},
    {field_offset_clause, 4},
    {field_select_clause, 0},
    {field_update_clause, 6},
    {field_using_clause, 2},
    {field_where_clause, 3},
  [1625] =
    {field_for_clause, 5},
    {field_from_clause, 1},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_update_clause, 6},
    {field_using_clause, 2},
    {field_where_clause, 3},
  [1632] =
    {field_for_clause, 6},
    {field_from_clause, 1},
    {field_limit_clause, 5},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_using_clause, 2},
    {field_where_clause, 3},
  [1639] =
    {field_from_clause, 1},
    {field_limit_clause, 5},
    {field_offset_clause, 6},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_using_clause, 2},
    {field_where_clause, 3},
  [1646] =
    {field_from_clause, 1},
    {field_limit_clause, 5},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_update_clause, 6},
    {field_using_clause, 2},
    {field_where_clause, 3},
  [1653] =
    {field_for_clause, 6},
    {field_from_clause, 1},
    {field_offset_clause, 5},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_using_clause, 2},
    {field_where_clause, 3},
  [1660] =
    {field_from_clause, 1},
    {field_offset_clause, 5},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_update_clause, 6},
    {field_using_clause, 2},
    {field_where_clause, 3},
  [1667] =
    {field_for_clause, 5},
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_select_clause, 0},
    {field_update_clause, 6},
    {field_using_clause, 2},
    {field_with_clause, 3},
  [1674] =
    {field_for_clause, 6},
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_limit_clause, 5},
    {field_select_clause, 0},
    {field_using_clause, 2},
    {field_with_clause, 3},
  [1681] =
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_limit_clause, 5},
    {field_offset_clause, 6},
    {field_select_clause, 0},
    {field_using_clause, 2},
    {field_with_clause, 3},
  [1688] =
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_limit_clause, 5},
    {field_select_clause, 0},
    {field_update_clause, 6},
    {field_using_clause, 2},
    {field_with_clause, 3},
  [1695] =
    {field_for_clause, 6},
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_offset_clause, 5},
    {field_select_clause, 0},
    {field_using_clause, 2},
    {field_with_clause, 3},
  [1702] =
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_offset_clause, 5},
    {field_select_clause, 0},
    {field_update_clause, 6},
    {field_using_clause, 2},
    {field_with_clause, 3},
  [1709] =
    {field_for_clause, 6},
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_order_by_clause, 5},
    {field_select_clause, 0},
    {field_using_clause, 2},
    {field_with_clause, 3},
  [1716] =
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_limit_clause, 6},
    {field_order_by_clause, 5},
    {field_select_clause, 0},
    {field_using_clause, 2},
    {field_with_clause, 3},
  [1723] =
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_offset_clause, 6},
    {field_order_by_clause, 5},
    {field_select_clause, 0},
    {field_using_clause, 2},
    {field_with_clause, 3},
  [1730] =
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_order_by_clause, 5},
    {field_select_clause, 0},
    {field_update_clause, 6},
    {field_using_clause, 2},
    {field_with_clause, 3},
  [1737] =
    {field_for_clause, 5},
    {field_from_clause, 1},
    {field_limit_clause, 4},
    {field_select_clause, 0},
    {field_update_clause, 6},
    {field_using_clause, 2},
    {field_with_clause, 3},
  [1744] =
    {field_for_clause, 6},
    {field_from_clause, 1},
    {field_limit_clause, 4},
    {field_offset_clause, 5},
    {field_select_clause, 0},
    {field_using_clause, 2},
    {field_with_clause, 3},
  [1751] =
    {field_from_clause, 1},
    {field_limit_clause, 4},
    {field_offset_clause, 5},
    {field_select_clause, 0},
    {field_update_clause, 6},
    {field_using_clause, 2},
    {field_with_clause, 3},
  [1758] =
    {field_for_clause, 5},
    {field_from_clause, 1},
    {field_offset_clause, 4},
    {field_select_clause, 0},
    {field_update_clause, 6},
    {field_using_clause, 2},
    {field_with_clause, 3},
  [1765] =
    {field_for_clause, 5},
    {field_from_clause, 1},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_update_clause, 6},
    {field_using_clause, 2},
    {field_with_clause, 3},
  [1772] =
    {field_for_clause, 6},
    {field_from_clause, 1},
    {field_limit_clause, 5},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_using_clause, 2},
    {field_with_clause, 3},
  [1779] =
    {field_from_clause, 1},
    {field_limit_clause, 5},
    {field_offset_clause, 6},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_using_clause, 2},
    {field_with_clause, 3},
  [1786] =
    {field_from_clause, 1},
    {field_limit_clause, 5},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_update_clause, 6},
    {field_using_clause, 2},
    {field_with_clause, 3},
  [1793] =
    {field_for_clause, 6},
    {field_from_clause, 1},
    {field_offset_clause, 5},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_using_clause, 2},
    {field_with_clause, 3},
  [1800] =
    {field_from_clause, 1},
    {field_offset_clause, 5},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_update_clause, 6},
    {field_using_clause, 2},
    {field_with_clause, 3},
  [1807] =
    {field_for_clause, 5},
    {field_from_clause, 1},
    {field_limit_clause, 3},
    {field_offset_clause, 4},
    {field_select_clause, 0},
    {field_update_clause, 6},
    {field_using_clause, 2},
  [1814] =
    {field_for_clause, 5},
    {field_from_clause, 1},
    {field_limit_clause, 4},
    {field_order_by_clause, 3},
    {field_select_clause, 0},
    {field_update_clause, 6},
    {field_using_clause, 2},
  [1821] =
    {field_for_clause, 6},
    {field_from_clause, 1},
    {field_limit_clause, 4},
    {field_offset_clause, 5},
    {field_order_by_clause, 3},
    {field_select_clause, 0},
    {field_using_clause, 2},
  [1828] =
    {field_from_clause, 1},
    {field_limit_clause, 4},
    {field_offset_clause, 5},
    {field_order_by_clause, 3},
    {field_select_clause, 0},
    {field_update_clause, 6},
    {field_using_clause, 2},
  [1835] =
    {field_for_clause, 5},
    {field_from_clause, 1},
    {field_offset_clause, 4},
    {field_order_by_clause, 3},
    {field_select_clause, 0},
    {field_update_clause, 6},
    {field_using_clause, 2},
  [1842] =
    {field_for_clause, 5},
    {field_from_clause, 1},
    {field_group_by_clause, 2},
    {field_limit_clause, 3},
    {field_offset_clause, 4},
    {field_select_clause, 0},
    {field_update_clause, 6},
  [1849] =
    {field_for_clause, 5},
    {field_from_clause, 1},
    {field_group_by_clause, 2},
    {field_limit_clause, 4},
    {field_order_by_clause, 3},
    {field_select_clause, 0},
    {field_update_clause, 6},
  [1856] =
    {field_for_clause, 6},
    {field_from_clause, 1},
    {field_group_by_clause, 2},
    {field_limit_clause, 4},
    {field_offset_clause, 5},
    {field_order_by_clause, 3},
    {field_select_clause, 0},
  [1863] =
    {field_from_clause, 1},
    {field_group_by_clause, 2},
    {field_limit_clause, 4},
    {field_offset_clause, 5},
    {field_order_by_clause, 3},
    {field_select_clause, 0},
    {field_update_clause, 6},
  [1870] =
    {field_for_clause, 5},
    {field_from_clause, 1},
    {field_group_by_clause, 2},
    {field_offset_clause, 4},
    {field_order_by_clause, 3},
    {field_select_clause, 0},
    {field_update_clause, 6},
  [1877] =
    {field_for_clause, 5},
    {field_from_clause, 1},
    {field_group_by_clause, 3},
    {field_limit_clause, 4},
    {field_select_clause, 0},
    {field_update_clause, 6},
    {field_where_clause, 2},
  [1884] =
    {field_for_clause, 6},
    {field_from_clause, 1},
    {field_group_by_clause, 3},
    {field_limit_clause, 4},
    {field_offset_clause, 5},
    {field_select_clause, 0},
    {field_where_clause, 2},
  [1891] =
    {field_from_clause, 1},
    {field_group_by_clause, 3},
    {field_limit_clause, 4},
    {field_offset_clause, 5},
    {field_select_clause, 0},
    {field_update_clause, 6},
    {field_where_clause, 2},
  [1898] =
    {field_for_clause, 5},
    {field_from_clause, 1},
    {field_group_by_clause, 3},
    {field_offset_clause, 4},
    {field_select_clause, 0},
    {field_update_clause, 6},
    {field_where_clause, 2},
  [1905] =
    {field_for_clause, 5},
    {field_from_clause, 1},
    {field_group_by_clause, 3},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_update_clause, 6},
    {field_where_clause, 2},
  [1912] =
    {field_for_clause, 6},
    {field_from_clause, 1},
    {field_group_by_clause, 3},
    {field_limit_clause, 5},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_where_clause, 2},
  [1919] =
    {field_from_clause, 1},
    {field_group_by_clause, 3},
    {field_limit_clause, 5},
    {field_offset_clause, 6},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_where_clause, 2},
  [1926] =
    {field_from_clause, 1},
    {field_group_by_clause, 3},
    {field_limit_clause, 5},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_update_clause, 6},
    {field_where_clause, 2},
  [1933] =
    {field_for_clause, 6},
    {field_from_clause, 1},
    {field_group_by_clause, 3},
    {field_offset_clause, 5},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_where_clause, 2},
  [1940] =
    {field_from_clause, 1},
    {field_group_by_clause, 3},
    {field_offset_clause, 5},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_update_clause, 6},
    {field_where_clause, 2},
  [1947] =
    {field_for_clause, 5},
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_select_clause, 0},
    {field_update_clause, 6},
    {field_where_clause, 2},
    {field_with_clause, 3},
  [1954] =
    {field_for_clause, 6},
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_limit_clause, 5},
    {field_select_clause, 0},
    {field_where_clause, 2},
    {field_with_clause, 3},
  [1961] =
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_limit_clause, 5},
    {field_offset_clause, 6},
    {field_select_clause, 0},
    {field_where_clause, 2},
    {field_with_clause, 3},
  [1968] =
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_limit_clause, 5},
    {field_select_clause, 0},
    {field_update_clause, 6},
    {field_where_clause, 2},
    {field_with_clause, 3},
  [1975] =
    {field_for_clause, 6},
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_offset_clause, 5},
    {field_select_clause, 0},
    {field_where_clause, 2},
    {field_with_clause, 3},
  [1982] =
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_offset_clause, 5},
    {field_select_clause, 0},
    {field_update_clause, 6},
    {field_where_clause, 2},
    {field_with_clause, 3},
  [1989] =
    {field_for_clause, 6},
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_order_by_clause, 5},
    {field_select_clause, 0},
    {field_where_clause, 2},
    {field_with_clause, 3},
  [1996] =
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_limit_clause, 6},
    {field_order_by_clause, 5},
    {field_select_clause, 0},
    {field_where_clause, 2},
    {field_with_clause, 3},
  [2003] =
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_offset_clause, 6},
    {field_order_by_clause, 5},
    {field_select_clause, 0},
    {field_where_clause, 2},
    {field_with_clause, 3},
  [2010] =
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_order_by_clause, 5},
    {field_select_clause, 0},
    {field_update_clause, 6},
    {field_where_clause, 2},
    {field_with_clause, 3},
  [2017] =
    {field_for_clause, 5},
    {field_from_clause, 1},
    {field_limit_clause, 4},
    {field_select_clause, 0},
    {field_update_clause, 6},
    {field_where_clause, 2},
    {field_with_clause, 3},
  [2024] =
    {field_for_clause, 6},
    {field_from_clause, 1},
    {field_limit_clause, 4},
    {field_offset_clause, 5},
    {field_select_clause, 0},
    {field_where_clause, 2},
    {field_with_clause, 3},
  [2031] =
    {field_from_clause, 1},
    {field_limit_clause, 4},
    {field_offset_clause, 5},
    {field_select_clause, 0},
    {field_update_clause, 6},
    {field_where_clause, 2},
    {field_with_clause, 3},
  [2038] =
    {field_for_clause, 5},
    {field_from_clause, 1},
    {field_offset_clause, 4},
    {field_select_clause, 0},
    {field_update_clause, 6},
    {field_where_clause, 2},
    {field_with_clause, 3},
  [2045] =
    {field_for_clause, 5},
    {field_from_clause, 1},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_update_clause, 6},
    {field_where_clause, 2},
    {field_with_clause, 3},
  [2052] =
    {field_for_clause, 6},
    {field_from_clause, 1},
    {field_limit_clause, 5},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_where_clause, 2},
    {field_with_clause, 3},
  [2059] =
    {field_from_clause, 1},
    {field_limit_clause, 5},
    {field_offset_clause, 6},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_where_clause, 2},
    {field_with_clause, 3},
  [2066] =
    {field_from_clause, 1},
    {field_limit_clause, 5},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_update_clause, 6},
    {field_where_clause, 2},
    {field_with_clause, 3},
  [2073] =
    {field_for_clause, 6},
    {field_from_clause, 1},
    {field_offset_clause, 5},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_where_clause, 2},
    {field_with_clause, 3},
  [2080] =
    {field_from_clause, 1},
    {field_offset_clause, 5},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_update_clause, 6},
    {field_where_clause, 2},
    {field_with_clause, 3},
  [2087] =
    {field_for_clause, 5},
    {field_from_clause, 1},
    {field_limit_clause, 3},
    {field_offset_clause, 4},
    {field_select_clause, 0},
    {field_update_clause, 6},
    {field_where_clause, 2},
  [2094] =
    {field_for_clause, 5},
    {field_from_clause, 1},
    {field_limit_clause, 4},
    {field_order_by_clause, 3},
    {field_select_clause, 0},
    {field_update_clause, 6},
    {field_where_clause, 2},
  [2101] =
    {field_for_clause, 6},
    {field_from_clause, 1},
    {field_limit_clause, 4},
    {field_offset_clause, 5},
    {field_order_by_clause, 3},
    {field_select_clause, 0},
    {field_where_clause, 2},
  [2108] =
    {field_from_clause, 1},
    {field_limit_clause, 4},
    {field_offset_clause, 5},
    {field_order_by_clause, 3},
    {field_select_clause, 0},
    {field_update_clause, 6},
    {field_where_clause, 2},
  [2115] =
    {field_for_clause, 5},
    {field_from_clause, 1},
    {field_offset_clause, 4},
    {field_order_by_clause, 3},
    {field_select_clause, 0},
    {field_update_clause, 6},
    {field_where_clause, 2},
  [2122] =
    {field_for_clause, 5},
    {field_from_clause, 1},
    {field_group_by_clause, 3},
    {field_limit_clause, 4},
    {field_select_clause, 0},
    {field_update_clause, 6},
    {field_with_clause, 2},
  [2129] =
    {field_for_clause, 6},
    {field_from_clause, 1},
    {field_group_by_clause, 3},
    {field_limit_clause, 4},
    {field_offset_clause, 5},
    {field_select_clause, 0},
    {field_with_clause, 2},
  [2136] =
    {field_from_clause, 1},
    {field_group_by_clause, 3},
    {field_limit_clause, 4},
    {field_offset_clause, 5},
    {field_select_clause, 0},
    {field_update_clause, 6},
    {field_with_clause, 2},
  [2143] =
    {field_for_clause, 5},
    {field_from_clause, 1},
    {field_group_by_clause, 3},
    {field_offset_clause, 4},
    {field_select_clause, 0},
    {field_update_clause, 6},
    {field_with_clause, 2},
  [2150] =
    {field_for_clause, 5},
    {field_from_clause, 1},
    {field_group_by_clause, 3},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_update_clause, 6},
    {field_with_clause, 2},
  [2157] =
    {field_for_clause, 6},
    {field_from_clause, 1},
    {field_group_by_clause, 3},
    {field_limit_clause, 5},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_with_clause, 2},
  [2164] =
    {field_from_clause, 1},
    {field_group_by_clause, 3},
    {field_limit_clause, 5},
    {field_offset_clause, 6},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_with_clause, 2},
  [2171] =
    {field_from_clause, 1},
    {field_group_by_clause, 3},
    {field_limit_clause, 5},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_update_clause, 6},
    {field_with_clause, 2},
  [2178] =
    {field_for_clause, 6},
    {field_from_clause, 1},
    {field_group_by_clause, 3},
    {field_offset_clause, 5},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_with_clause, 2},
  [2185] =
    {field_from_clause, 1},
    {field_group_by_clause, 3},
    {field_offset_clause, 5},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_update_clause, 6},
    {field_with_clause, 2},
  [2192] =
    {field_for_clause, 5},
    {field_from_clause, 1},
    {field_limit_clause, 3},
    {field_offset_clause, 4},
    {field_select_clause, 0},
    {field_update_clause, 6},
    {field_with_clause, 2},
  [2199] =
    {field_for_clause, 5},
    {field_from_clause, 1},
    {field_limit_clause, 4},
    {field_order_by_clause, 3},
    {field_select_clause, 0},
    {field_update_clause, 6},
    {field_with_clause, 2},
  [2206] =
    {field_for_clause, 6},
    {field_from_clause, 1},
    {field_limit_clause, 4},
    {field_offset_clause, 5},
    {field_order_by_clause, 3},
    {field_select_clause, 0},
    {field_with_clause, 2},
  [2213] =
    {field_from_clause, 1},
    {field_limit_clause, 4},
    {field_offset_clause, 5},
    {field_order_by_clause, 3},
    {field_select_clause, 0},
    {field_update_clause, 6},
    {field_with_clause, 2},
  [2220] =
    {field_for_clause, 5},
    {field_from_clause, 1},
    {field_offset_clause, 4},
    {field_order_by_clause, 3},
    {field_select_clause, 0},
    {field_update_clause, 6},
    {field_with_clause, 2},
  [2227] =
    {field_for_clause, 5},
    {field_from_clause, 1},
    {field_limit_clause, 3},
    {field_offset_clause, 4},
    {field_order_by_clause, 2},
    {field_select_clause, 0},
    {field_update_clause, 6},
  [2234] =
    {field_for_clause, 6},
    {field_from_clause, 1},
    {field_group_by_clause, 3},
    {field_limit_clause, 4},
    {field_offset_clause, 5},
    {field_select_clause, 0},
    {field_update_clause, 7},
    {field_using_clause, 2},
  [2242] =
    {field_for_clause, 6},
    {field_from_clause, 1},
    {field_group_by_clause, 3},
    {field_limit_clause, 5},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_update_clause, 7},
    {field_using_clause, 2},
  [2250] =
    {field_for_clause, 7},
    {field_from_clause, 1},
    {field_group_by_clause, 3},
    {field_limit_clause, 5},
    {field_offset_clause, 6},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_using_clause, 2},
  [2258] =
    {field_from_clause, 1},
    {field_group_by_clause, 3},
    {field_limit_clause, 5},
    {field_offset_clause, 6},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_update_clause, 7},
    {field_using_clause, 2},
  [2266] =
    {field_for_clause, 6},
    {field_from_clause, 1},
    {field_group_by_clause, 3},
    {field_offset_clause, 5},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_update_clause, 7},
    {field_using_clause, 2},
  [2274] =
    {field_for_clause, 6},
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_limit_clause, 5},
    {field_select_clause, 0},
    {field_update_clause, 7},
    {field_using_clause, 2},
    {field_where_clause, 3},
  [2282] =
    {field_for_clause, 7},
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_limit_clause, 5},
    {field_offset_clause, 6},
    {field_select_clause, 0},
    {field_using_clause, 2},
    {field_where_clause, 3},
  [2290] =
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_limit_clause, 5},
    {field_offset_clause, 6},
    {field_select_clause, 0},
    {field_update_clause, 7},
    {field_using_clause, 2},
    {field_where_clause, 3},
  [2298] =
    {field_for_clause, 6},
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_offset_clause, 5},
    {field_select_clause, 0},
    {field_update_clause, 7},
    {field_using_clause, 2},
    {field_where_clause, 3},
  [2306] =
    {field_for_clause, 6},
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_order_by_clause, 5},
    {field_select_clause, 0},
    {field_update_clause, 7},
    {field_using_clause, 2},
    {field_where_clause, 3},
  [2314] =
    {field_for_clause, 7},
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_limit_clause, 6},
    {field_order_by_clause, 5},
    {field_select_clause, 0},
    {field_using_clause, 2},
    {field_where_clause, 3},
  [2322] =
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_limit_clause, 6},
    {field_offset_clause, 7},
    {field_order_by_clause, 5},
    {field_select_clause, 0},
    {field_using_clause, 2},
    {field_where_clause, 3},
  [2330] =
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_limit_clause, 6},
    {field_order_by_clause, 5},
    {field_select_clause, 0},
    {field_update_clause, 7},
    {field_using_clause, 2},
    {field_where_clause, 3},
  [2338] =
    {field_for_clause, 7},
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_offset_clause, 6},
    {field_order_by_clause, 5},
    {field_select_clause, 0},
    {field_using_clause, 2},
    {field_where_clause, 3},
  [2346] =
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_offset_clause, 6},
    {field_order_by_clause, 5},
    {field_select_clause, 0},
    {field_update_clause, 7},
    {field_using_clause, 2},
    {field_where_clause, 3},
  [2354] =
    {field_for_clause, 6},
    {field_from_clause, 1},
    {field_group_by_clause, 5},
    {field_select_clause, 0},
    {field_update_clause, 7},
    {field_using_clause, 2},
    {field_where_clause, 3},
    {field_with_clause, 4},
  [2362] =
    {field_for_clause, 7},
    {field_from_clause, 1},
    {field_group_by_clause, 5},
    {field_limit_clause, 6},
    {field_select_clause, 0},
    {field_using_clause, 2},
    {field_where_clause, 3},
    {field_with_clause, 4},
  [2370] =
    {field_from_clause, 1},
    {field_group_by_clause, 5},
    {field_limit_clause, 6},
    {field_offset_clause, 7},
    {field_select_clause, 0},
    {field_using_clause, 2},
    {field_where_clause, 3},
    {field_with_clause, 4},
  [2378] =
    {field_from_clause, 1},
    {field_group_by_clause, 5},
    {field_limit_clause, 6},
    {field_select_clause, 0},
    {field_update_clause, 7},
    {field_using_clause, 2},
    {field_where_clause, 3},
    {field_with_clause, 4},
  [2386] =
    {field_for_clause, 7},
    {field_from_clause, 1},
    {field_group_by_clause, 5},
    {field_offset_clause, 6},
    {field_select_clause, 0},
    {field_using_clause, 2},
    {field_where_clause, 3},
    {field_with_clause, 4},
  [2394] =
    {field_from_clause, 1},
    {field_group_by_clause, 5},
    {field_offset_clause, 6},
    {field_select_clause, 0},
    {field_update_clause, 7},
    {field_using_clause, 2},
    {field_where_clause, 3},
    {field_with_clause, 4},
  [2402] =
    {field_for_clause, 7},
    {field_from_clause, 1},
    {field_group_by_clause, 5},
    {field_order_by_clause, 6},
    {field_select_clause, 0},
    {field_using_clause, 2},
    {field_where_clause, 3},
    {field_with_clause, 4},
  [2410] =
    {field_from_clause, 1},
    {field_group_by_clause, 5},
    {field_limit_clause, 7},
    {field_order_by_clause, 6},
    {field_select_clause, 0},
    {field_using_clause, 2},
    {field_where_clause, 3},
    {field_with_clause, 4},
  [2418] =
    {field_from_clause, 1},
    {field_group_by_clause, 5},
    {field_offset_clause, 7},
    {field_order_by_clause, 6},
    {field_select_clause, 0},
    {field_using_clause, 2},
    {field_where_clause, 3},
    {field_with_clause, 4},
  [2426] =
    {field_from_clause, 1},
    {field_group_by_clause, 5},
    {field_order_by_clause, 6},
    {field_select_clause, 0},
    {field_update_clause, 7},
    {field_using_clause, 2},
    {field_where_clause, 3},
    {field_with_clause, 4},
  [2434] =
    {field_for_clause, 6},
    {field_from_clause, 1},
    {field_limit_clause, 5},
    {field_select_clause, 0},
    {field_update_clause, 7},
    {field_using_clause, 2},
    {field_where_clause, 3},
    {field_with_clause, 4},
  [2442] =
    {field_for_clause, 7},
    {field_from_clause, 1},
    {field_limit_clause, 5},
    {field_offset_clause, 6},
    {field_select_clause, 0},
    {field_using_clause, 2},
    {field_where_clause, 3},
    {field_with_clause, 4},
  [2450] =
    {field_from_clause, 1},
    {field_limit_clause, 5},
    {field_offset_clause, 6},
    {field_select_clause, 0},
    {field_update_clause, 7},
    {field_using_clause, 2},
    {field_where_clause, 3},
    {field_with_clause, 4},
  [2458] =
    {field_for_clause, 6},
    {field_from_clause, 1},
    {field_offset_clause, 5},
    {field_select_clause, 0},
    {field_update_clause, 7},
    {field_using_clause, 2},
    {field_where_clause, 3},
    {field_with_clause, 4},
  [2466] =
    {field_for_clause, 6},
    {field_from_clause, 1},
    {field_order_by_clause, 5},
    {field_select_clause, 0},
    {field_update_clause, 7},
    {field_using_clause, 2},
    {field_where_clause, 3},
    {field_with_clause, 4},
  [2474] =
    {field_for_clause, 7},
    {field_from_clause, 1},
    {field_limit_clause, 6},
    {field_order_by_clause, 5},
    {field_select_clause, 0},
    {field_using_clause, 2},
    {field_where_clause, 3},
    {field_with_clause, 4},
  [2482] =
    {field_from_clause, 1},
    {field_limit_clause, 6},
    {field_offset_clause, 7},
    {field_order_by_clause, 5},
    {field_select_clause, 0},
    {field_using_clause, 2},
    {field_where_clause, 3},
    {field_with_clause, 4},
  [2490] =
    {field_from_clause, 1},
    {field_limit_clause, 6},
    {field_order_by_clause, 5},
    {field_select_clause, 0},
    {field_update_clause, 7},
    {field_using_clause, 2},
    {field_where_clause, 3},
    {field_with_clause, 4},
  [2498] =
    {field_for_clause, 7},
    {field_from_clause, 1},
    {field_offset_clause, 6},
    {field_order_by_clause, 5},
    {field_select_clause, 0},
    {field_using_clause, 2},
    {field_where_clause, 3},
    {field_with_clause, 4},
  [2506] =
    {field_from_clause, 1},
    {field_offset_clause, 6},
    {field_order_by_clause, 5},
    {field_select_clause, 0},
    {field_update_clause, 7},
    {field_using_clause, 2},
    {field_where_clause, 3},
    {field_with_clause, 4},
  [2514] =
    {field_for_clause, 6},
    {field_from_clause, 1},
    {field_limit_clause, 4},
    {field_offset_clause, 5},
    {field_select_clause, 0},
    {field_update_clause, 7},
    {field_using_clause, 2},
    {field_where_clause, 3},
  [2522] =
    {field_for_clause, 6},
    {field_from_clause, 1},
    {field_limit_clause, 5},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_update_clause, 7},
    {field_using_clause, 2},
    {field_where_clause, 3},
  [2530] =
    {field_for_clause, 7},
    {field_from_clause, 1},
    {field_limit_clause, 5},
    {field_offset_clause, 6},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_using_clause, 2},
    {field_where_clause, 3},
  [2538] =
    {field_from_clause, 1},
    {field_limit_clause, 5},
    {field_offset_clause, 6},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_update_clause, 7},
    {field_using_clause, 2},
    {field_where_clause, 3},
  [2546] =
    {field_for_clause, 6},
    {field_from_clause, 1},
    {field_offset_clause, 5},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_update_clause, 7},
    {field_using_clause, 2},
    {field_where_clause, 3},
  [2554] =
    {field_for_clause, 6},
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_limit_clause, 5},
    {field_select_clause, 0},
    {field_update_clause, 7},
    {field_using_clause, 2},
    {field_with_clause, 3},
  [2562] =
    {field_for_clause, 7},
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_limit_clause, 5},
    {field_offset_clause, 6},
    {field_select_clause, 0},
    {field_using_clause, 2},
    {field_with_clause, 3},
  [2570] =
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_limit_clause, 5},
    {field_offset_clause, 6},
    {field_select_clause, 0},
    {field_update_clause, 7},
    {field_using_clause, 2},
    {field_with_clause, 3},
  [2578] =
    {field_for_clause, 6},
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_offset_clause, 5},
    {field_select_clause, 0},
    {field_update_clause, 7},
    {field_using_clause, 2},
    {field_with_clause, 3},
  [2586] =
    {field_for_clause, 6},
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_order_by_clause, 5},
    {field_select_clause, 0},
    {field_update_clause, 7},
    {field_using_clause, 2},
    {field_with_clause, 3},
  [2594] =
    {field_for_clause, 7},
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_limit_clause, 6},
    {field_order_by_clause, 5},
    {field_select_clause, 0},
    {field_using_clause, 2},
    {field_with_clause, 3},
  [2602] =
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_limit_clause, 6},
    {field_offset_clause, 7},
    {field_order_by_clause, 5},
    {field_select_clause, 0},
    {field_using_clause, 2},
    {field_with_clause, 3},
  [2610] =
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_limit_clause, 6},
    {field_order_by_clause, 5},
    {field_select_clause, 0},
    {field_update_clause, 7},
    {field_using_clause, 2},
    {field_with_clause, 3},
  [2618] =
    {field_for_clause, 7},
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_offset_clause, 6},
    {field_order_by_clause, 5},
    {field_select_clause, 0},
    {field_using_clause, 2},
    {field_with_clause, 3},
  [2626] =
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_offset_clause, 6},
    {field_order_by_clause, 5},
    {field_select_clause, 0},
    {field_update_clause, 7},
    {field_using_clause, 2},
    {field_with_clause, 3},
  [2634] =
    {field_for_clause, 6},
    {field_from_clause, 1},
    {field_limit_clause, 4},
    {field_offset_clause, 5},
    {field_select_clause, 0},
    {field_update_clause, 7},
    {field_using_clause, 2},
    {field_with_clause, 3},
  [2642] =
    {field_for_clause, 6},
    {field_from_clause, 1},
    {field_limit_clause, 5},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_update_clause, 7},
    {field_using_clause, 2},
    {field_with_clause, 3},
  [2650] =
    {field_for_clause, 7},
    {field_from_clause, 1},
    {field_limit_clause, 5},
    {field_offset_clause, 6},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_using_clause, 2},
    {field_with_clause, 3},
  [2658] =
    {field_from_clause, 1},
    {field_limit_clause, 5},
    {field_offset_clause, 6},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_update_clause, 7},
    {field_using_clause, 2},
    {field_with_clause, 3},
  [2666] =
    {field_for_clause, 6},
    {field_from_clause, 1},
    {field_offset_clause, 5},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_update_clause, 7},
    {field_using_clause, 2},
    {field_with_clause, 3},
  [2674] =
    {field_for_clause, 6},
    {field_from_clause, 1},
    {field_limit_clause, 4},
    {field_offset_clause, 5},
    {field_order_by_clause, 3},
    {field_select_clause, 0},
    {field_update_clause, 7},
    {field_using_clause, 2},
  [2682] =
    {field_for_clause, 6},
    {field_from_clause, 1},
    {field_group_by_clause, 2},
    {field_limit_clause, 4},
    {field_offset_clause, 5},
    {field_order_by_clause, 3},
    {field_select_clause, 0},
    {field_update_clause, 7},
  [2690] =
    {field_for_clause, 6},
    {field_from_clause, 1},
    {field_group_by_clause, 3},
    {field_limit_clause, 4},
    {field_offset_clause, 5},
    {field_select_clause, 0},
    {field_update_clause, 7},
    {field_where_clause, 2},
  [2698] =
    {field_for_clause, 6},
    {field_from_clause, 1},
    {field_group_by_clause, 3},
    {field_limit_clause, 5},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_update_clause, 7},
    {field_where_clause, 2},
  [2706] =
    {field_for_clause, 7},
    {field_from_clause, 1},
    {field_group_by_clause, 3},
    {field_limit_clause, 5},
    {field_offset_clause, 6},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_where_clause, 2},
  [2714] =
    {field_from_clause, 1},
    {field_group_by_clause, 3},
    {field_limit_clause, 5},
    {field_offset_clause, 6},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_update_clause, 7},
    {field_where_clause, 2},
  [2722] =
    {field_for_clause, 6},
    {field_from_clause, 1},
    {field_group_by_clause, 3},
    {field_offset_clause, 5},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_update_clause, 7},
    {field_where_clause, 2},
  [2730] =
    {field_for_clause, 6},
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_limit_clause, 5},
    {field_select_clause, 0},
    {field_update_clause, 7},
    {field_where_clause, 2},
    {field_with_clause, 3},
  [2738] =
    {field_for_clause, 7},
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_limit_clause, 5},
    {field_offset_clause, 6},
    {field_select_clause, 0},
    {field_where_clause, 2},
    {field_with_clause, 3},
  [2746] =
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_limit_clause, 5},
    {field_offset_clause, 6},
    {field_select_clause, 0},
    {field_update_clause, 7},
    {field_where_clause, 2},
    {field_with_clause, 3},
  [2754] =
    {field_for_clause, 6},
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_offset_clause, 5},
    {field_select_clause, 0},
    {field_update_clause, 7},
    {field_where_clause, 2},
    {field_with_clause, 3},
  [2762] =
    {field_for_clause, 6},
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_order_by_clause, 5},
    {field_select_clause, 0},
    {field_update_clause, 7},
    {field_where_clause, 2},
    {field_with_clause, 3},
  [2770] =
    {field_for_clause, 7},
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_limit_clause, 6},
    {field_order_by_clause, 5},
    {field_select_clause, 0},
    {field_where_clause, 2},
    {field_with_clause, 3},
  [2778] =
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_limit_clause, 6},
    {field_offset_clause, 7},
    {field_order_by_clause, 5},
    {field_select_clause, 0},
    {field_where_clause, 2},
    {field_with_clause, 3},
  [2786] =
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_limit_clause, 6},
    {field_order_by_clause, 5},
    {field_select_clause, 0},
    {field_update_clause, 7},
    {field_where_clause, 2},
    {field_with_clause, 3},
  [2794] =
    {field_for_clause, 7},
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_offset_clause, 6},
    {field_order_by_clause, 5},
    {field_select_clause, 0},
    {field_where_clause, 2},
    {field_with_clause, 3},
  [2802] =
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_offset_clause, 6},
    {field_order_by_clause, 5},
    {field_select_clause, 0},
    {field_update_clause, 7},
    {field_where_clause, 2},
    {field_with_clause, 3},
  [2810] =
    {field_for_clause, 6},
    {field_from_clause, 1},
    {field_limit_clause, 4},
    {field_offset_clause, 5},
    {field_select_clause, 0},
    {field_update_clause, 7},
    {field_where_clause, 2},
    {field_with_clause, 3},
  [2818] =
    {field_for_clause, 6},
    {field_from_clause, 1},
    {field_limit_clause, 5},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_update_clause, 7},
    {field_where_clause, 2},
    {field_with_clause, 3},
  [2826] =
    {field_for_clause, 7},
    {field_from_clause, 1},
    {field_limit_clause, 5},
    {field_offset_clause, 6},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_where_clause, 2},
    {field_with_clause, 3},
  [2834] =
    {field_from_clause, 1},
    {field_limit_clause, 5},
    {field_offset_clause, 6},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_update_clause, 7},
    {field_where_clause, 2},
    {field_with_clause, 3},
  [2842] =
    {field_for_clause, 6},
    {field_from_clause, 1},
    {field_offset_clause, 5},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_update_clause, 7},
    {field_where_clause, 2},
    {field_with_clause, 3},
  [2850] =
    {field_for_clause, 6},
    {field_from_clause, 1},
    {field_limit_clause, 4},
    {field_offset_clause, 5},
    {field_order_by_clause, 3},
    {field_select_clause, 0},
    {field_update_clause, 7},
    {field_where_clause, 2},
  [2858] =
    {field_for_clause, 6},
    {field_from_clause, 1},
    {field_group_by_clause, 3},
    {field_limit_clause, 4},
    {field_offset_clause, 5},
    {field_select_clause, 0},
    {field_update_clause, 7},
    {field_with_clause, 2},
  [2866] =
    {field_for_clause, 6},
    {field_from_clause, 1},
    {field_group_by_clause, 3},
    {field_limit_clause, 5},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_update_clause, 7},
    {field_with_clause, 2},
  [2874] =
    {field_for_clause, 7},
    {field_from_clause, 1},
    {field_group_by_clause, 3},
    {field_limit_clause, 5},
    {field_offset_clause, 6},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_with_clause, 2},
  [2882] =
    {field_from_clause, 1},
    {field_group_by_clause, 3},
    {field_limit_clause, 5},
    {field_offset_clause, 6},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_update_clause, 7},
    {field_with_clause, 2},
  [2890] =
    {field_for_clause, 6},
    {field_from_clause, 1},
    {field_group_by_clause, 3},
    {field_offset_clause, 5},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_update_clause, 7},
    {field_with_clause, 2},
  [2898] =
    {field_for_clause, 6},
    {field_from_clause, 1},
    {field_limit_clause, 4},
    {field_offset_clause, 5},
    {field_order_by_clause, 3},
    {field_select_clause, 0},
    {field_update_clause, 7},
    {field_with_clause, 2},
  [2906] =
    {field_bound_value, 2},
    {field_field, 0},
  [2908] =
    {field_for_clause, 7},
    {field_from_clause, 1},
    {field_group_by_clause, 3},
    {field_limit_clause, 5},
    {field_offset_clause, 6},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_update_clause, 8},
    {field_using_clause, 2},
  [2917] =
    {field_for_clause, 7},
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_limit_clause, 5},
    {field_offset_clause, 6},
    {field_select_clause, 0},
    {field_update_clause, 8},
    {field_using_clause, 2},
    {field_where_clause, 3},
  [2926] =
    {field_for_clause, 7},
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_limit_clause, 6},
    {field_order_by_clause, 5},
    {field_select_clause, 0},
    {field_update_clause, 8},
    {field_using_clause, 2},
    {field_where_clause, 3},
  [2935] =
    {field_for_clause, 8},
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_limit_clause, 6},
    {field_offset_clause, 7},
    {field_order_by_clause, 5},
    {field_select_clause, 0},
    {field_using_clause, 2},
    {field_where_clause, 3},
  [2944] =
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_limit_clause, 6},
    {field_offset_clause, 7},
    {field_order_by_clause, 5},
    {field_select_clause, 0},
    {field_update_clause, 8},
    {field_using_clause, 2},
    {field_where_clause, 3},
  [2953] =
    {field_for_clause, 7},
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_offset_clause, 6},
    {field_order_by_clause, 5},
    {field_select_clause, 0},
    {field_update_clause, 8},
    {field_using_clause, 2},
    {field_where_clause, 3},
  [2962] =
    {field_for_clause, 7},
    {field_from_clause, 1},
    {field_group_by_clause, 5},
    {field_limit_clause, 6},
    {field_select_clause, 0},
    {field_update_clause, 8},
    {field_using_clause, 2},
    {field_where_clause, 3},
    {field_with_clause, 4},
  [2971] =
    {field_for_clause, 8},
    {field_from_clause, 1},
    {field_group_by_clause, 5},
    {field_limit_clause, 6},
    {field_offset_clause, 7},
    {field_select_clause, 0},
    {field_using_clause, 2},
    {field_where_clause, 3},
    {field_with_clause, 4},
  [2980] =
    {field_from_clause, 1},
    {field_group_by_clause, 5},
    {field_limit_clause, 6},
    {field_offset_clause, 7},
    {field_select_clause, 0},
    {field_update_clause, 8},
    {field_using_clause, 2},
    {field_where_clause, 3},
    {field_with_clause, 4},
  [2989] =
    {field_for_clause, 7},
    {field_from_clause, 1},
    {field_group_by_clause, 5},
    {field_offset_clause, 6},
    {field_select_clause, 0},
    {field_update_clause, 8},
    {field_using_clause, 2},
    {field_where_clause, 3},
    {field_with_clause, 4},
  [2998] =
    {field_for_clause, 7},
    {field_from_clause, 1},
    {field_group_by_clause, 5},
    {field_order_by_clause, 6},
    {field_select_clause, 0},
    {field_update_clause, 8},
    {field_using_clause, 2},
    {field_where_clause, 3},
    {field_with_clause, 4},
  [3007] =
    {field_for_clause, 8},
    {field_from_clause, 1},
    {field_group_by_clause, 5},
    {field_limit_clause, 7},
    {field_order_by_clause, 6},
    {field_select_clause, 0},
    {field_using_clause, 2},
    {field_where_clause, 3},
    {field_with_clause, 4},
  [3016] =
    {field_from_clause, 1},
    {field_group_by_clause, 5},
    {field_limit_clause, 7},
    {field_offset_clause, 8},
    {field_order_by_clause, 6},
    {field_select_clause, 0},
    {field_using_clause, 2},
    {field_where_clause, 3},
    {field_with_clause, 4},
  [3025] =
    {field_from_clause, 1},
    {field_group_by_clause, 5},
    {field_limit_clause, 7},
    {field_order_by_clause, 6},
    {field_select_clause, 0},
    {field_update_clause, 8},
    {field_using_clause, 2},
    {field_where_clause, 3},
    {field_with_clause, 4},
  [3034] =
    {field_for_clause, 8},
    {field_from_clause, 1},
    {field_group_by_clause, 5},
    {field_offset_clause, 7},
    {field_order_by_clause, 6},
    {field_select_clause, 0},
    {field_using_clause, 2},
    {field_where_clause, 3},
    {field_with_clause, 4},
  [3043] =
    {field_from_clause, 1},
    {field_group_by_clause, 5},
    {field_offset_clause, 7},
    {field_order_by_clause, 6},
    {field_select_clause, 0},
    {field_update_clause, 8},
    {field_using_clause, 2},
    {field_where_clause, 3},
    {field_with_clause, 4},
  [3052] =
    {field_for_clause, 7},
    {field_from_clause, 1},
    {field_limit_clause, 5},
    {field_offset_clause, 6},
    {field_select_clause, 0},
    {field_update_clause, 8},
    {field_using_clause, 2},
    {field_where_clause, 3},
    {field_with_clause, 4},
  [3061] =
    {field_for_clause, 7},
    {field_from_clause, 1},
    {field_limit_clause, 6},
    {field_order_by_clause, 5},
    {field_select_clause, 0},
    {field_update_clause, 8},
    {field_using_clause, 2},
    {field_where_clause, 3},
    {field_with_clause, 4},
  [3070] =
    {field_for_clause, 8},
    {field_from_clause, 1},
    {field_limit_clause, 6},
    {field_offset_clause, 7},
    {field_order_by_clause, 5},
    {field_select_clause, 0},
    {field_using_clause, 2},
    {field_where_clause, 3},
    {field_with_clause, 4},
  [3079] =
    {field_from_clause, 1},
    {field_limit_clause, 6},
    {field_offset_clause, 7},
    {field_order_by_clause, 5},
    {field_select_clause, 0},
    {field_update_clause, 8},
    {field_using_clause, 2},
    {field_where_clause, 3},
    {field_with_clause, 4},
  [3088] =
    {field_for_clause, 7},
    {field_from_clause, 1},
    {field_offset_clause, 6},
    {field_order_by_clause, 5},
    {field_select_clause, 0},
    {field_update_clause, 8},
    {field_using_clause, 2},
    {field_where_clause, 3},
    {field_with_clause, 4},
  [3097] =
    {field_for_clause, 7},
    {field_from_clause, 1},
    {field_limit_clause, 5},
    {field_offset_clause, 6},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_update_clause, 8},
    {field_using_clause, 2},
    {field_where_clause, 3},
  [3106] =
    {field_for_clause, 7},
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_limit_clause, 5},
    {field_offset_clause, 6},
    {field_select_clause, 0},
    {field_update_clause, 8},
    {field_using_clause, 2},
    {field_with_clause, 3},
  [3115] =
    {field_for_clause, 7},
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_limit_clause, 6},
    {field_order_by_clause, 5},
    {field_select_clause, 0},
    {field_update_clause, 8},
    {field_using_clause, 2},
    {field_with_clause, 3},
  [3124] =
    {field_for_clause, 8},
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_limit_clause, 6},
    {field_offset_clause, 7},
    {field_order_by_clause, 5},
    {field_select_clause, 0},
    {field_using_clause, 2},
    {field_with_clause, 3},
  [3133] =
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_limit_clause, 6},
    {field_offset_clause, 7},
    {field_order_by_clause, 5},
    {field_select_clause, 0},
    {field_update_clause, 8},
    {field_using_clause, 2},
    {field_with_clause, 3},
  [3142] =
    {field_for_clause, 7},
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_offset_clause, 6},
    {field_order_by_clause, 5},
    {field_select_clause, 0},
    {field_update_clause, 8},
    {field_using_clause, 2},
    {field_with_clause, 3},
  [3151] =
    {field_for_clause, 7},
    {field_from_clause, 1},
    {field_limit_clause, 5},
    {field_offset_clause, 6},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_update_clause, 8},
    {field_using_clause, 2},
    {field_with_clause, 3},
  [3160] =
    {field_for_clause, 7},
    {field_from_clause, 1},
    {field_group_by_clause, 3},
    {field_limit_clause, 5},
    {field_offset_clause, 6},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_update_clause, 8},
    {field_where_clause, 2},
  [3169] =
    {field_for_clause, 7},
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_limit_clause, 5},
    {field_offset_clause, 6},
    {field_select_clause, 0},
    {field_update_clause, 8},
    {field_where_clause, 2},
    {field_with_clause, 3},
  [3178] =
    {field_for_clause, 7},
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_limit_clause, 6},
    {field_order_by_clause, 5},
    {field_select_clause, 0},
    {field_update_clause, 8},
    {field_where_clause, 2},
    {field_with_clause, 3},
  [3187] =
    {field_for_clause, 8},
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_limit_clause, 6},
    {field_offset_clause, 7},
    {field_order_by_clause, 5},
    {field_select_clause, 0},
    {field_where_clause, 2},
    {field_with_clause, 3},
  [3196] =
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_limit_clause, 6},
    {field_offset_clause, 7},
    {field_order_by_clause, 5},
    {field_select_clause, 0},
    {field_update_clause, 8},
    {field_where_clause, 2},
    {field_with_clause, 3},
  [3205] =
    {field_for_clause, 7},
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_offset_clause, 6},
    {field_order_by_clause, 5},
    {field_select_clause, 0},
    {field_update_clause, 8},
    {field_where_clause, 2},
    {field_with_clause, 3},
  [3214] =
    {field_for_clause, 7},
    {field_from_clause, 1},
    {field_limit_clause, 5},
    {field_offset_clause, 6},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_update_clause, 8},
    {field_where_clause, 2},
    {field_with_clause, 3},
  [3223] =
    {field_for_clause, 7},
    {field_from_clause, 1},
    {field_group_by_clause, 3},
    {field_limit_clause, 5},
    {field_offset_clause, 6},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_update_clause, 8},
    {field_with_clause, 2},
  [3232] =
    {field_for_clause, 8},
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_limit_clause, 6},
    {field_offset_clause, 7},
    {field_order_by_clause, 5},
    {field_select_clause, 0},
    {field_update_clause, 9},
    {field_using_clause, 2},
    {field_where_clause, 3},
  [3242] =
    {field_for_clause, 8},
    {field_from_clause, 1},
    {field_group_by_clause, 5},
    {field_limit_clause, 6},
    {field_offset_clause, 7},
    {field_select_clause, 0},
    {field_update_clause, 9},
    {field_using_clause, 2},
    {field_where_clause, 3},
    {field_with_clause, 4},
  [3252] =
    {field_for_clause, 8},
    {field_from_clause, 1},
    {field_group_by_clause, 5},
    {field_limit_clause, 7},
    {field_order_by_clause, 6},
    {field_select_clause, 0},
    {field_update_clause, 9},
    {field_using_clause, 2},
    {field_where_clause, 3},
    {field_with_clause, 4},
  [3262] =
    {field_for_clause, 9},
    {field_from_clause, 1},
    {field_group_by_clause, 5},
    {field_limit_clause, 7},
    {field_offset_clause, 8},
    {field_order_by_clause, 6},
    {field_select_clause, 0},
    {field_using_clause, 2},
    {field_where_clause, 3},
    {field_with_clause, 4},
  [3272] =
    {field_from_clause, 1},
    {field_group_by_clause, 5},
    {field_limit_clause, 7},
    {field_offset_clause, 8},
    {field_order_by_clause, 6},
    {field_select_clause, 0},
    {field_update_clause, 9},
    {field_using_clause, 2},
    {field_where_clause, 3},
    {field_with_clause, 4},
  [3282] =
    {field_for_clause, 8},
    {field_from_clause, 1},
    {field_group_by_clause, 5},
    {field_offset_clause, 7},
    {field_order_by_clause, 6},
    {field_select_clause, 0},
    {field_update_clause, 9},
    {field_using_clause, 2},
    {field_where_clause, 3},
    {field_with_clause, 4},
  [3292] =
    {field_for_clause, 8},
    {field_from_clause, 1},
    {field_limit_clause, 6},
    {field_offset_clause, 7},
    {field_order_by_clause, 5},
    {field_select_clause, 0},
    {field_update_clause, 9},
    {field_using_clause, 2},
    {field_where_clause, 3},
    {field_with_clause, 4},
  [3302] =
    {field_for_clause, 8},
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_limit_clause, 6},
    {field_offset_clause, 7},
    {field_order_by_clause, 5},
    {field_select_clause, 0},
    {field_update_clause, 9},
    {field_using_clause, 2},
    {field_with_clause, 3},
  [3312] =
    {field_for_clause, 8},
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_limit_clause, 6},
    {field_offset_clause, 7},
    {field_order_by_clause, 5},
    {field_select_clause, 0},
    {field_update_clause, 9},
    {field_where_clause, 2},
    {field_with_clause, 3},
  [3322] =
    {field_for_clause, 9},
    {field_from_clause, 1},
    {field_group_by_clause, 5},
    {field_limit_clause, 7},
    {field_offset_clause, 8},
    {field_order_by_clause, 6},
    {field_select_clause, 0},
    {field_update_clause, 10},
    {field_using_clause, 2},
    {field_where_clause, 3},
    {field_with_clause, 4},
};

static const TSSymbol ts_alias_sequences[PRODUCTION_ID_COUNT][MAX_ALIAS_SEQUENCE_LENGTH] = {
  [0] = {0},
};

static const uint16_t ts_non_terminal_alias_map[] = {
  0,
};

static const TSStateId ts_primary_state_ids[STATE_COUNT] = {
  [0] = 0,
  [1] = 1,
  [2] = 2,
  [3] = 2,
  [4] = 4,
  [5] = 5,
  [6] = 4,
  [7] = 7,
  [8] = 8,
  [9] = 9,
  [10] = 10,
  [11] = 11,
  [12] = 12,
  [13] = 13,
  [14] = 14,
  [15] = 15,
  [16] = 16,
  [17] = 17,
  [18] = 18,
  [19] = 19,
  [20] = 20,
  [21] = 20,
  [22] = 22,
  [23] = 23,
  [24] = 24,
  [25] = 10,
  [26] = 9,
  [27] = 27,
  [28] = 28,
  [29] = 29,
  [30] = 30,
  [31] = 11,
  [32] = 30,
  [33] = 33,
  [34] = 34,
  [35] = 35,
  [36] = 36,
  [37] = 37,
  [38] = 38,
  [39] = 39,
  [40] = 40,
  [41] = 41,
  [42] = 42,
  [43] = 43,
  [44] = 44,
  [45] = 45,
  [46] = 46,
  [47] = 47,
  [48] = 48,
  [49] = 49,
  [50] = 50,
  [51] = 51,
  [52] = 52,
  [53] = 53,
  [54] = 54,
  [55] = 55,
  [56] = 56,
  [57] = 57,
  [58] = 58,
  [59] = 59,
  [60] = 60,
  [61] = 61,
  [62] = 62,
  [63] = 63,
  [64] = 64,
  [65] = 65,
  [66] = 66,
  [67] = 67,
  [68] = 68,
  [69] = 69,
  [70] = 70,
  [71] = 71,
  [72] = 72,
  [73] = 73,
  [74] = 74,
  [75] = 75,
  [76] = 76,
  [77] = 77,
  [78] = 78,
  [79] = 79,
  [80] = 80,
  [81] = 81,
  [82] = 82,
  [83] = 83,
  [84] = 84,
  [85] = 85,
  [86] = 86,
  [87] = 87,
  [88] = 88,
  [89] = 89,
  [90] = 90,
  [91] = 91,
  [92] = 92,
  [93] = 93,
  [94] = 94,
  [95] = 95,
  [96] = 96,
  [97] = 97,
  [98] = 98,
  [99] = 99,
  [100] = 100,
  [101] = 101,
  [102] = 102,
  [103] = 103,
  [104] = 104,
  [105] = 105,
  [106] = 106,
  [107] = 107,
  [108] = 108,
  [109] = 109,
  [110] = 110,
  [111] = 111,
  [112] = 112,
  [113] = 89,
  [114] = 114,
  [115] = 115,
  [116] = 116,
  [117] = 117,
  [118] = 118,
  [119] = 119,
  [120] = 120,
  [121] = 121,
  [122] = 122,
  [123] = 123,
  [124] = 124,
  [125] = 125,
  [126] = 126,
  [127] = 127,
  [128] = 128,
  [129] = 129,
  [130] = 130,
  [131] = 131,
  [132] = 132,
  [133] = 133,
  [134] = 134,
  [135] = 135,
  [136] = 136,
  [137] = 137,
  [138] = 138,
  [139] = 139,
  [140] = 140,
  [141] = 141,
  [142] = 142,
  [143] = 143,
  [144] = 144,
  [145] = 145,
  [146] = 146,
  [147] = 147,
  [148] = 148,
  [149] = 149,
  [150] = 150,
  [151] = 151,
  [152] = 152,
  [153] = 153,
  [154] = 154,
  [155] = 155,
  [156] = 156,
  [157] = 157,
  [158] = 158,
  [159] = 159,
  [160] = 160,
  [161] = 161,
  [162] = 162,
  [163] = 163,
  [164] = 164,
  [165] = 165,
  [166] = 166,
  [167] = 167,
  [168] = 168,
  [169] = 169,
  [170] = 170,
  [171] = 171,
  [172] = 172,
  [173] = 173,
  [174] = 174,
  [175] = 175,
  [176] = 176,
  [177] = 177,
  [178] = 178,
  [179] = 179,
  [180] = 180,
  [181] = 171,
  [182] = 182,
  [183] = 12,
  [184] = 184,
  [185] = 185,
  [186] = 186,
  [187] = 187,
  [188] = 188,
  [189] = 189,
  [190] = 190,
  [191] = 191,
  [192] = 192,
  [193] = 10,
  [194] = 194,
  [195] = 9,
  [196] = 196,
  [197] = 197,
  [198] = 198,
  [199] = 199,
  [200] = 200,
  [201] = 201,
  [202] = 202,
  [203] = 203,
  [204] = 204,
  [205] = 205,
  [206] = 206,
  [207] = 207,
  [208] = 208,
  [209] = 209,
  [210] = 210,
  [211] = 211,
  [212] = 212,
  [213] = 213,
  [214] = 214,
  [215] = 215,
  [216] = 216,
  [217] = 217,
  [218] = 218,
  [219] = 219,
  [220] = 220,
  [221] = 221,
  [222] = 222,
  [223] = 223,
  [224] = 224,
  [225] = 225,
  [226] = 226,
  [227] = 227,
  [228] = 228,
  [229] = 229,
  [230] = 230,
  [231] = 231,
  [232] = 232,
  [233] = 233,
  [234] = 234,
  [235] = 235,
  [236] = 236,
  [237] = 237,
  [238] = 238,
  [239] = 239,
  [240] = 240,
  [241] = 241,
  [242] = 242,
  [243] = 243,
  [244] = 244,
  [245] = 245,
  [246] = 246,
  [247] = 247,
  [248] = 248,
  [249] = 217,
  [250] = 250,
  [251] = 251,
  [252] = 252,
  [253] = 253,
  [254] = 254,
  [255] = 255,
  [256] = 256,
  [257] = 257,
  [258] = 258,
  [259] = 11,
  [260] = 260,
  [261] = 261,
  [262] = 262,
  [263] = 50,
  [264] = 264,
  [265] = 265,
  [266] = 266,
  [267] = 267,
  [268] = 268,
  [269] = 269,
  [270] = 270,
  [271] = 271,
  [272] = 272,
  [273] = 273,
  [274] = 274,
  [275] = 275,
  [276] = 276,
  [277] = 277,
  [278] = 278,
  [279] = 279,
  [280] = 280,
  [281] = 281,
  [282] = 282,
  [283] = 283,
  [284] = 284,
  [285] = 285,
  [286] = 286,
  [287] = 287,
  [288] = 288,
  [289] = 289,
  [290] = 290,
  [291] = 291,
  [292] = 292,
  [293] = 293,
  [294] = 294,
  [295] = 295,
  [296] = 296,
  [297] = 297,
  [298] = 298,
  [299] = 299,
  [300] = 300,
  [301] = 301,
  [302] = 302,
  [303] = 303,
  [304] = 304,
  [305] = 305,
  [306] = 306,
  [307] = 307,
  [308] = 308,
  [309] = 309,
  [310] = 310,
  [311] = 311,
  [312] = 312,
  [313] = 313,
  [314] = 314,
  [315] = 315,
  [316] = 316,
  [317] = 317,
  [318] = 318,
  [319] = 319,
  [320] = 320,
  [321] = 321,
  [322] = 322,
  [323] = 323,
  [324] = 324,
  [325] = 325,
  [326] = 326,
  [327] = 17,
  [328] = 328,
  [329] = 329,
  [330] = 14,
  [331] = 331,
  [332] = 13,
  [333] = 333,
  [334] = 334,
  [335] = 335,
  [336] = 336,
  [337] = 337,
  [338] = 338,
  [339] = 339,
  [340] = 340,
  [341] = 341,
  [342] = 342,
  [343] = 343,
  [344] = 344,
  [345] = 345,
  [346] = 346,
  [347] = 347,
  [348] = 348,
  [349] = 349,
  [350] = 350,
  [351] = 351,
  [352] = 352,
  [353] = 353,
  [354] = 354,
  [355] = 355,
  [356] = 356,
  [357] = 357,
  [358] = 358,
  [359] = 359,
  [360] = 360,
  [361] = 361,
  [362] = 362,
  [363] = 363,
  [364] = 364,
  [365] = 365,
  [366] = 366,
  [367] = 367,
  [368] = 368,
  [369] = 369,
  [370] = 370,
  [371] = 371,
  [372] = 372,
  [373] = 373,
  [374] = 374,
  [375] = 375,
  [376] = 376,
  [377] = 377,
  [378] = 378,
  [379] = 379,
  [380] = 380,
  [381] = 381,
  [382] = 382,
  [383] = 383,
  [384] = 384,
  [385] = 385,
  [386] = 386,
  [387] = 387,
  [388] = 388,
  [389] = 389,
  [390] = 390,
  [391] = 391,
  [392] = 392,
  [393] = 393,
  [394] = 15,
  [395] = 395,
  [396] = 396,
  [397] = 397,
  [398] = 398,
  [399] = 399,
  [400] = 400,
  [401] = 401,
  [402] = 402,
  [403] = 403,
  [404] = 404,
  [405] = 405,
  [406] = 406,
  [407] = 407,
  [408] = 16,
  [409] = 409,
  [410] = 410,
  [411] = 411,
  [412] = 412,
  [413] = 413,
  [414] = 414,
  [415] = 412,
  [416] = 416,
  [417] = 417,
  [418] = 353,
  [419] = 419,
  [420] = 68,
  [421] = 421,
  [422] = 422,
  [423] = 423,
  [424] = 424,
  [425] = 425,
  [426] = 426,
  [427] = 427,
  [428] = 428,
  [429] = 429,
  [430] = 430,
  [431] = 431,
  [432] = 432,
  [433] = 433,
  [434] = 434,
  [435] = 435,
  [436] = 43,
  [437] = 437,
  [438] = 438,
  [439] = 439,
  [440] = 440,
  [441] = 76,
  [442] = 80,
  [443] = 443,
  [444] = 69,
  [445] = 71,
  [446] = 45,
  [447] = 46,
  [448] = 72,
  [449] = 47,
  [450] = 48,
  [451] = 73,
  [452] = 49,
  [453] = 77,
  [454] = 67,
  [455] = 455,
  [456] = 455,
  [457] = 457,
  [458] = 437,
  [459] = 459,
  [460] = 460,
  [461] = 461,
  [462] = 427,
  [463] = 435,
  [464] = 464,
  [465] = 465,
  [466] = 75,
  [467] = 467,
  [468] = 468,
  [469] = 469,
  [470] = 470,
  [471] = 471,
  [472] = 472,
  [473] = 473,
  [474] = 474,
  [475] = 475,
  [476] = 476,
  [477] = 477,
  [478] = 478,
  [479] = 479,
  [480] = 480,
  [481] = 481,
  [482] = 482,
  [483] = 483,
  [484] = 484,
  [485] = 485,
  [486] = 486,
  [487] = 487,
  [488] = 488,
  [489] = 489,
  [490] = 490,
  [491] = 491,
  [492] = 492,
  [493] = 493,
  [494] = 494,
  [495] = 495,
  [496] = 496,
  [497] = 497,
  [498] = 498,
  [499] = 499,
  [500] = 500,
  [501] = 501,
  [502] = 502,
  [503] = 503,
  [504] = 504,
  [505] = 505,
  [506] = 506,
  [507] = 507,
  [508] = 508,
  [509] = 509,
  [510] = 510,
  [511] = 511,
  [512] = 512,
  [513] = 513,
  [514] = 514,
  [515] = 515,
  [516] = 516,
  [517] = 517,
  [518] = 518,
  [519] = 519,
  [520] = 520,
  [521] = 521,
  [522] = 522,
  [523] = 523,
  [524] = 524,
  [525] = 525,
  [526] = 526,
  [527] = 527,
  [528] = 528,
  [529] = 529,
  [530] = 530,
  [531] = 531,
  [532] = 532,
  [533] = 533,
  [534] = 534,
  [535] = 535,
  [536] = 536,
  [537] = 537,
  [538] = 538,
  [539] = 539,
  [540] = 540,
  [541] = 541,
  [542] = 542,
  [543] = 543,
  [544] = 544,
  [545] = 545,
  [546] = 546,
  [547] = 547,
  [548] = 548,
  [549] = 549,
  [550] = 550,
  [551] = 551,
  [552] = 552,
  [553] = 553,
  [554] = 554,
  [555] = 555,
  [556] = 556,
  [557] = 557,
  [558] = 558,
  [559] = 559,
  [560] = 560,
  [561] = 561,
  [562] = 562,
  [563] = 563,
  [564] = 564,
  [565] = 565,
  [566] = 566,
  [567] = 567,
  [568] = 568,
  [569] = 569,
  [570] = 570,
  [571] = 571,
  [572] = 572,
  [573] = 573,
  [574] = 574,
  [575] = 575,
  [576] = 576,
  [577] = 577,
  [578] = 578,
  [579] = 579,
  [580] = 580,
  [581] = 581,
  [582] = 582,
  [583] = 583,
  [584] = 584,
  [585] = 585,
  [586] = 586,
  [587] = 587,
  [588] = 588,
  [589] = 589,
  [590] = 590,
  [591] = 591,
  [592] = 592,
  [593] = 593,
  [594] = 594,
  [595] = 595,
  [596] = 596,
  [597] = 597,
  [598] = 598,
  [599] = 599,
  [600] = 600,
  [601] = 601,
  [602] = 602,
  [603] = 603,
  [604] = 604,
  [605] = 605,
  [606] = 606,
  [607] = 607,
  [608] = 608,
  [609] = 609,
  [610] = 610,
  [611] = 611,
  [612] = 612,
  [613] = 613,
  [614] = 614,
  [615] = 615,
  [616] = 616,
  [617] = 617,
  [618] = 618,
  [619] = 619,
  [620] = 620,
  [621] = 621,
  [622] = 622,
  [623] = 623,
  [624] = 624,
  [625] = 625,
  [626] = 626,
  [627] = 627,
  [628] = 628,
  [629] = 629,
  [630] = 630,
  [631] = 631,
  [632] = 632,
  [633] = 633,
  [634] = 634,
  [635] = 635,
  [636] = 636,
  [637] = 637,
  [638] = 638,
  [639] = 639,
  [640] = 640,
  [641] = 641,
  [642] = 642,
  [643] = 643,
  [644] = 644,
  [645] = 645,
  [646] = 646,
  [647] = 647,
  [648] = 648,
  [649] = 649,
  [650] = 650,
  [651] = 651,
  [652] = 652,
  [653] = 653,
  [654] = 654,
  [655] = 655,
  [656] = 656,
  [657] = 657,
  [658] = 658,
  [659] = 659,
  [660] = 660,
  [661] = 661,
  [662] = 662,
  [663] = 663,
  [664] = 664,
  [665] = 665,
  [666] = 666,
  [667] = 667,
  [668] = 668,
  [669] = 669,
  [670] = 670,
  [671] = 671,
  [672] = 672,
  [673] = 673,
  [674] = 674,
  [675] = 675,
  [676] = 676,
  [677] = 677,
  [678] = 678,
  [679] = 679,
  [680] = 680,
  [681] = 681,
  [682] = 682,
  [683] = 683,
  [684] = 684,
  [685] = 685,
  [686] = 686,
  [687] = 687,
  [688] = 688,
  [689] = 689,
  [690] = 690,
  [691] = 691,
  [692] = 692,
  [693] = 693,
  [694] = 694,
  [695] = 695,
  [696] = 696,
  [697] = 697,
  [698] = 698,
  [699] = 699,
  [700] = 700,
  [701] = 701,
  [702] = 702,
  [703] = 703,
  [704] = 704,
  [705] = 705,
  [706] = 706,
  [707] = 707,
  [708] = 708,
  [709] = 709,
  [710] = 710,
  [711] = 711,
  [712] = 712,
  [713] = 713,
  [714] = 714,
  [715] = 715,
  [716] = 716,
  [717] = 717,
  [718] = 718,
  [719] = 719,
  [720] = 720,
  [721] = 721,
  [722] = 722,
  [723] = 723,
  [724] = 724,
  [725] = 725,
  [726] = 726,
  [727] = 727,
  [728] = 728,
  [729] = 729,
  [730] = 730,
  [731] = 731,
  [732] = 732,
  [733] = 733,
  [734] = 734,
  [735] = 735,
  [736] = 83,
  [737] = 737,
  [738] = 738,
  [739] = 739,
  [740] = 740,
  [741] = 741,
  [742] = 742,
  [743] = 743,
  [744] = 744,
  [745] = 745,
  [746] = 746,
  [747] = 747,
  [748] = 748,
  [749] = 749,
  [750] = 750,
  [751] = 751,
  [752] = 752,
  [753] = 753,
  [754] = 754,
  [755] = 755,
  [756] = 756,
  [757] = 757,
  [758] = 758,
  [759] = 759,
  [760] = 760,
  [761] = 761,
  [762] = 762,
  [763] = 763,
  [764] = 764,
  [765] = 765,
  [766] = 766,
  [767] = 767,
  [768] = 768,
  [769] = 769,
  [770] = 770,
  [771] = 771,
  [772] = 772,
  [773] = 773,
  [774] = 774,
  [775] = 775,
  [776] = 776,
  [777] = 777,
  [778] = 778,
  [779] = 779,
  [780] = 780,
  [781] = 781,
  [782] = 782,
  [783] = 783,
  [784] = 784,
  [785] = 785,
  [786] = 786,
  [787] = 787,
  [788] = 788,
  [789] = 789,
  [790] = 790,
  [791] = 779,
  [792] = 792,
  [793] = 793,
  [794] = 794,
  [795] = 795,
  [796] = 790,
  [797] = 797,
  [798] = 781,
  [799] = 786,
  [800] = 787,
  [801] = 772,
  [802] = 802,
  [803] = 768,
  [804] = 783,
  [805] = 770,
  [806] = 806,
  [807] = 797,
  [808] = 808,
  [809] = 754,
  [810] = 787,
  [811] = 811,
};

static bool ts_lex(TSLexer *lexer, TSStateId state) {
  START_LEXER();
  eof = lexer->eof(lexer);
  switch (state) {
    case 0:
      if (eof) ADVANCE(658);
      ADVANCE_MAP(
        '!', 28,
        '\'', 2,
        '(', 666,
        ')', 667,
        '*', 10,
        ',', 669,
        '-', 642,
        '.', 745,
        '/', 15,
        ':', 783,
        '<', 750,
        '=', 682,
        '>', 752,
        'A', 123,
        'a', 123,
        'B', 199,
        'b', 199,
        'C', 581,
        'c', 581,
        'D', 68,
        'd', 68,
        'E', 334,
        'e', 334,
        'F', 77,
        'f', 77,
        'G', 202,
        'g', 202,
        'H', 69,
        'h', 69,
        'I', 365,
        'i', 365,
        'L', 101,
        'l', 101,
        'M', 70,
        'm', 70,
        'N', 46,
        'n', 46,
        'O', 265,
        'o', 265,
        'R', 178,
        'r', 178,
        'S', 133,
        's', 133,
        'T', 201,
        't', 201,
        'U', 429,
        'u', 429,
        'V', 298,
        'v', 298,
        'W', 287,
        'w', 287,
        'Y', 258,
        'y', 258,
      );
      if (('1' <= lookahead && lookahead <= '4')) ADVANCE(794);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(795);
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') SKIP(0);
      END_STATE();
    case 1:
      ADVANCE_MAP(
        '\'', 2,
        '*', 10,
        '-', 642,
        '/', 17,
        'F', 72,
        'f', 72,
        'L', 73,
        'l', 73,
        'N', 51,
        'n', 51,
        'R', 197,
        'r', 197,
        'S', 206,
        's', 206,
        'T', 286,
        't', 286,
        'U', 426,
        'u', 426,
        'V', 297,
        'v', 297,
        'Y', 248,
        'y', 248,
      );
      if (('1' <= lookahead && lookahead <= '4')) ADVANCE(792);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(793);
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') SKIP(1);
      if (('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(654);
      END_STATE();
    case 2:
      if (lookahead == '\'') ADVANCE(787);
      if (lookahead == '\\') ADVANCE(653);
      if (lookahead != 0) ADVANCE(2);
      END_STATE();
    case 3:
      if (lookahead == '(') ADVANCE(666);
      if (lookahead == ')') ADVANCE(667);
      if (lookahead == ',') ADVANCE(669);
      if (lookahead == '.') ADVANCE(745);
      if (lookahead == '/') ADVANCE(17);
      if (lookahead == 'F' ||
          lookahead == 'f') ADVANCE(852);
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') SKIP(3);
      if (('A' <= lookahead && lookahead <= 'Z') ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(870);
      END_STATE();
    case 4:
      if (lookahead == '(') ADVANCE(666);
      if (lookahead == ')') ADVANCE(667);
      if (lookahead == '/') ADVANCE(17);
      if (lookahead == 'D' ||
          lookahead == 'd') ADVANCE(826);
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') SKIP(4);
      if (('A' <= lookahead && lookahead <= 'Z') ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(870);
      END_STATE();
    case 5:
      if (lookahead == '(') ADVANCE(666);
      if (lookahead == '*') ADVANCE(10);
      if (lookahead == '/') ADVANCE(17);
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') SKIP(5);
      if (('A' <= lookahead && lookahead <= 'Z') ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(870);
      END_STATE();
    case 6:
      ADVANCE_MAP(
        '(', 666,
        '/', 17,
        'D', 826,
        'd', 826,
        'F', 827,
        'f', 827,
        'T', 867,
        't', 867,
      );
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') SKIP(6);
      if (('A' <= lookahead && lookahead <= 'Z') ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(870);
      END_STATE();
    case 7:
      if (lookahead == '(') ADVANCE(666);
      if (lookahead == '/') ADVANCE(17);
      if (lookahead == 'D' ||
          lookahead == 'd') ADVANCE(826);
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(846);
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') SKIP(7);
      if (('A' <= lookahead && lookahead <= 'Z') ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(870);
      END_STATE();
    case 8:
      if (lookahead == ')') ADVANCE(667);
      if (lookahead == '/') ADVANCE(17);
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(364);
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(457);
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') SKIP(8);
      END_STATE();
    case 9:
      if (lookahead == '*') ADVANCE(10);
      if (lookahead == '/') ADVANCE(17);
      if (lookahead == 'G' ||
          lookahead == 'g') ADVANCE(819);
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') SKIP(9);
      if (('A' <= lookahead && lookahead <= 'Z') ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(870);
      END_STATE();
    case 10:
      if (lookahead == '*') ADVANCE(31);
      END_STATE();
    case 11:
      if (lookahead == '*') ADVANCE(784);
      END_STATE();
    case 12:
      if (lookahead == '*') ADVANCE(11);
      END_STATE();
    case 13:
      if (lookahead == '-') ADVANCE(19);
      END_STATE();
    case 14:
      if (lookahead == '.') ADVANCE(648);
      if (lookahead == 'Z') ADVANCE(800);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(629);
      END_STATE();
    case 15:
      if (lookahead == '/') ADVANCE(659);
      END_STATE();
    case 16:
      if (lookahead == '/') ADVANCE(664);
      END_STATE();
    case 17:
      if (lookahead == '/') ADVANCE(16);
      END_STATE();
    case 18:
      if (lookahead == '0') ADVANCE(640);
      if (lookahead == '1') ADVANCE(630);
      END_STATE();
    case 19:
      if (lookahead == '0') ADVANCE(641);
      if (lookahead == '3') ADVANCE(628);
      if (lookahead == '1' ||
          lookahead == '2') ADVANCE(644);
      END_STATE();
    case 20:
      if (lookahead == '0') ADVANCE(58);
      END_STATE();
    case 21:
      if (lookahead == '0') ADVANCE(59);
      END_STATE();
    case 22:
      if (lookahead == '2') ADVANCE(631);
      if (lookahead == '0' ||
          lookahead == '1') ADVANCE(645);
      END_STATE();
    case 23:
      ADVANCE_MAP(
        '9', 20,
        'F', 316,
        'f', 316,
        'M', 416,
        'm', 416,
        'N', 61,
        'n', 61,
        'Q', 591,
        'q', 591,
        'W', 241,
        'w', 241,
        'Y', 256,
        'y', 256,
      );
      END_STATE();
    case 24:
      ADVANCE_MAP(
        '9', 21,
        'F', 317,
        'f', 317,
        'M', 419,
        'm', 419,
        'N', 61,
        'n', 61,
        'Q', 592,
        'q', 592,
        'W', 244,
        'w', 244,
        'Y', 257,
        'y', 257,
      );
      END_STATE();
    case 25:
      if (lookahead == ':') ADVANCE(639);
      END_STATE();
    case 26:
      if (lookahead == ':') ADVANCE(637);
      END_STATE();
    case 27:
      if (lookahead == ':') ADVANCE(638);
      END_STATE();
    case 28:
      if (lookahead == '=') ADVANCE(748);
      END_STATE();
    case 29:
      if (lookahead == 'A') ADVANCE(41);
      END_STATE();
    case 30:
      if (lookahead == 'C') ADVANCE(34);
      END_STATE();
    case 31:
      if (lookahead == 'D') ADVANCE(37);
      END_STATE();
    case 32:
      if (lookahead == 'E') ADVANCE(42);
      END_STATE();
    case 33:
      if (lookahead == 'E') ADVANCE(39);
      END_STATE();
    case 34:
      if (lookahead == 'H') ADVANCE(32);
      END_STATE();
    case 35:
      if (lookahead == 'M') ADVANCE(29);
      END_STATE();
    case 36:
      if (lookahead == 'N') ADVANCE(38);
      END_STATE();
    case 37:
      if (lookahead == 'O') ADVANCE(36);
      END_STATE();
    case 38:
      if (lookahead == 'O') ADVANCE(40);
      END_STATE();
    case 39:
      if (lookahead == 'R') ADVANCE(12);
      END_STATE();
    case 40:
      if (lookahead == 'T') ADVANCE(35);
      END_STATE();
    case 41:
      if (lookahead == 'T') ADVANCE(30);
      END_STATE();
    case 42:
      if (lookahead == 'V') ADVANCE(33);
      END_STATE();
    case 43:
      if (lookahead == 'Z') ADVANCE(800);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(629);
      END_STATE();
    case 44:
      if (lookahead == 'Z') ADVANCE(800);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(629);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(43);
      END_STATE();
    case 45:
      if (lookahead == 'Z') ADVANCE(800);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(629);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(44);
      END_STATE();
    case 46:
      if (lookahead == '_') ADVANCE(160);
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(611);
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(531);
      if (lookahead == 'U' ||
          lookahead == 'u') ADVANCE(341);
      END_STATE();
    case 47:
      if (lookahead == '_') ADVANCE(23);
      END_STATE();
    case 48:
      if (lookahead == '_') ADVANCE(272);
      END_STATE();
    case 49:
      if (lookahead == '_') ADVANCE(127);
      END_STATE();
    case 50:
      if (lookahead == '_') ADVANCE(437);
      END_STATE();
    case 51:
      if (lookahead == '_') ADVANCE(158);
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(612);
      if (lookahead == 'U' ||
          lookahead == 'u') ADVANCE(332);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(655);
      END_STATE();
    case 52:
      if (lookahead == '_') ADVANCE(360);
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(152);
      END_STATE();
    case 53:
      if (lookahead == '_') ADVANCE(557);
      END_STATE();
    case 54:
      if (lookahead == '_') ADVANCE(361);
      END_STATE();
    case 55:
      if (lookahead == '_') ADVANCE(278);
      END_STATE();
    case 56:
      if (lookahead == '_') ADVANCE(82);
      END_STATE();
    case 57:
      if (lookahead == '_') ADVANCE(227);
      END_STATE();
    case 58:
      if (lookahead == '_') ADVANCE(176);
      END_STATE();
    case 59:
      if (lookahead == '_') ADVANCE(177);
      END_STATE();
    case 60:
      if (lookahead == '_') ADVANCE(565);
      END_STATE();
    case 61:
      if (lookahead == '_') ADVANCE(175);
      END_STATE();
    case 62:
      if (lookahead == '_') ADVANCE(24);
      END_STATE();
    case 63:
      if (lookahead == '_') ADVANCE(439);
      END_STATE();
    case 64:
      if (lookahead == '_') ADVANCE(363);
      END_STATE();
    case 65:
      if (lookahead == '_') ADVANCE(440);
      END_STATE();
    case 66:
      if (lookahead == '_') ADVANCE(441);
      END_STATE();
    case 67:
      if (lookahead == '_') ADVANCE(438);
      END_STATE();
    case 68:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(545);
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(339);
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(522);
      END_STATE();
    case 69:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(598);
      END_STATE();
    case 70:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(609);
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(378);
      if (lookahead == 'Y' ||
          lookahead == 'y') ADVANCE(53);
      END_STATE();
    case 71:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(632);
      END_STATE();
    case 72:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(331);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('B' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('b' <= lookahead && lookahead <= 'z')) ADVANCE(655);
      END_STATE();
    case 73:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(503);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('B' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('b' <= lookahead && lookahead <= 'z')) ADVANCE(655);
      END_STATE();
    case 74:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(356);
      END_STATE();
    case 75:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(627);
      END_STATE();
    case 76:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(627);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(801);
      END_STATE();
    case 77:
      ADVANCE_MAP(
        'A', 347,
        'a', 347,
        'I', 214,
        'i', 214,
        'O', 442,
        'o', 442,
        'R', 389,
        'r', 389,
      );
      END_STATE();
    case 78:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(130);
      END_STATE();
    case 79:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(130);
      if (lookahead == 'U' ||
          lookahead == 'u') ADVANCE(185);
      END_STATE();
    case 80:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(613);
      END_STATE();
    case 81:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(613);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(801);
      END_STATE();
    case 82:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(277);
      END_STATE();
    case 83:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(362);
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(480);
      END_STATE();
    case 84:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(616);
      END_STATE();
    case 85:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(375);
      END_STATE();
    case 86:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(342);
      END_STATE();
    case 87:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(484);
      END_STATE();
    case 88:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(476);
      END_STATE();
    case 89:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(549);
      END_STATE();
    case 90:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(536);
      END_STATE();
    case 91:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(445);
      END_STATE();
    case 92:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(446);
      END_STATE();
    case 93:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(447);
      END_STATE();
    case 94:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(465);
      END_STATE();
    case 95:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(451);
      END_STATE();
    case 96:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(452);
      END_STATE();
    case 97:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(453);
      END_STATE();
    case 98:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(385);
      END_STATE();
    case 99:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(622);
      END_STATE();
    case 100:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(464);
      END_STATE();
    case 101:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(510);
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(324);
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(390);
      END_STATE();
    case 102:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(377);
      END_STATE();
    case 103:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(623);
      END_STATE();
    case 104:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(306);
      END_STATE();
    case 105:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(625);
      END_STATE();
    case 106:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(561);
      END_STATE();
    case 107:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(561);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(801);
      END_STATE();
    case 108:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(562);
      END_STATE();
    case 109:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(519);
      END_STATE();
    case 110:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(564);
      END_STATE();
    case 111:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(570);
      END_STATE();
    case 112:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(486);
      END_STATE();
    case 113:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(487);
      END_STATE();
    case 114:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(488);
      END_STATE();
    case 115:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(489);
      END_STATE();
    case 116:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(490);
      END_STATE();
    case 117:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(491);
      END_STATE();
    case 118:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(492);
      END_STATE();
    case 119:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(351);
      END_STATE();
    case 120:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(352);
      END_STATE();
    case 121:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(353);
      END_STATE();
    case 122:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(354);
      END_STATE();
    case 123:
      ADVANCE_MAP(
        'B', 393,
        'b', 393,
        'L', 329,
        'l', 329,
        'N', 149,
        'n', 149,
        'S', 701,
        's', 701,
        'T', 726,
        't', 726,
      );
      END_STATE();
    case 124:
      if (lookahead == 'B' ||
          lookahead == 'b') ADVANCE(614);
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') ADVANCE(124);
      END_STATE();
    case 125:
      if (lookahead == 'B' ||
          lookahead == 'b') ADVANCE(615);
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') ADVANCE(125);
      END_STATE();
    case 126:
      if (lookahead == 'B' ||
          lookahead == 'b') ADVANCE(305);
      END_STATE();
    case 127:
      if (lookahead == 'B' ||
          lookahead == 'b') ADVANCE(255);
      END_STATE();
    case 128:
      if (lookahead == 'C' ||
          lookahead == 'c') ADVANCE(738);
      END_STATE();
    case 129:
      if (lookahead == 'C' ||
          lookahead == 'c') ADVANCE(737);
      END_STATE();
    case 130:
      if (lookahead == 'C' ||
          lookahead == 'c') ADVANCE(327);
      END_STATE();
    case 131:
      if (lookahead == 'C' ||
          lookahead == 'c') ADVANCE(585);
      if (lookahead == 'L' ||
          lookahead == 'l') ADVANCE(208);
      END_STATE();
    case 132:
      if (lookahead == 'C' ||
          lookahead == 'c') ADVANCE(337);
      END_STATE();
    case 133:
      ADVANCE_MAP(
        'C', 415,
        'c', 415,
        'E', 131,
        'e', 131,
        'T', 98,
        't', 98,
        'U', 430,
        'u', 430,
        'Y', 524,
        'y', 524,
      );
      END_STATE();
    case 134:
      if (lookahead == 'C' ||
          lookahead == 'c') ADVANCE(399);
      if (lookahead == 'F' ||
          lookahead == 'f') ADVANCE(216);
      END_STATE();
    case 135:
      if (lookahead == 'C' ||
          lookahead == 'c') ADVANCE(86);
      END_STATE();
    case 136:
      if (lookahead == 'C' ||
          lookahead == 'c') ADVANCE(535);
      END_STATE();
    case 137:
      if (lookahead == 'C' ||
          lookahead == 'c') ADVANCE(459);
      END_STATE();
    case 138:
      if (lookahead == 'C' ||
          lookahead == 'c') ADVANCE(193);
      END_STATE();
    case 139:
      if (lookahead == 'C' ||
          lookahead == 'c') ADVANCE(194);
      END_STATE();
    case 140:
      if (lookahead == 'C' ||
          lookahead == 'c') ADVANCE(228);
      END_STATE();
    case 141:
      if (lookahead == 'C' ||
          lookahead == 'c') ADVANCE(89);
      END_STATE();
    case 142:
      if (lookahead == 'C' ||
          lookahead == 'c') ADVANCE(418);
      END_STATE();
    case 143:
      if (lookahead == 'C' ||
          lookahead == 'c') ADVANCE(424);
      END_STATE();
    case 144:
      if (lookahead == 'C' ||
          lookahead == 'c') ADVANCE(110);
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') ADVANCE(144);
      END_STATE();
    case 145:
      if (lookahead == 'C' ||
          lookahead == 'c') ADVANCE(119);
      END_STATE();
    case 146:
      if (lookahead == 'C' ||
          lookahead == 'c') ADVANCE(120);
      END_STATE();
    case 147:
      if (lookahead == 'C' ||
          lookahead == 'c') ADVANCE(121);
      END_STATE();
    case 148:
      if (lookahead == 'C' ||
          lookahead == 'c') ADVANCE(122);
      END_STATE();
    case 149:
      if (lookahead == 'D' ||
          lookahead == 'd') ADVANCE(710);
      END_STATE();
    case 150:
      if (lookahead == 'D' ||
          lookahead == 'd') ADVANCE(686);
      END_STATE();
    case 151:
      if (lookahead == 'D' ||
          lookahead == 'd') ADVANCE(683);
      END_STATE();
    case 152:
      if (lookahead == 'D' ||
          lookahead == 'd') ADVANCE(720);
      END_STATE();
    case 153:
      if (lookahead == 'D' ||
          lookahead == 'd') ADVANCE(707);
      END_STATE();
    case 154:
      if (lookahead == 'D' ||
          lookahead == 'd') ADVANCE(673);
      END_STATE();
    case 155:
      if (lookahead == 'D' ||
          lookahead == 'd') ADVANCE(222);
      END_STATE();
    case 156:
      if (lookahead == 'D' ||
          lookahead == 'd') ADVANCE(717);
      END_STATE();
    case 157:
      if (lookahead == 'D' ||
          lookahead == 'd') ADVANCE(722);
      END_STATE();
    case 158:
      ADVANCE_MAP(
        'D', 76,
        'd', 76,
        'F', 296,
        'f', 296,
        'M', 402,
        'm', 402,
        'Q', 588,
        'q', 588,
        'W', 236,
        'w', 236,
        'Y', 253,
        'y', 253,
      );
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(649);
      END_STATE();
    case 159:
      if (lookahead == 'D' ||
          lookahead == 'd') ADVANCE(107);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(649);
      END_STATE();
    case 160:
      ADVANCE_MAP(
        'D', 75,
        'd', 75,
        'F', 295,
        'f', 295,
        'M', 401,
        'm', 401,
        'Q', 587,
        'q', 587,
        'W', 235,
        'w', 235,
        'Y', 252,
        'y', 252,
      );
      END_STATE();
    case 161:
      if (lookahead == 'D' ||
          lookahead == 'd') ADVANCE(494);
      END_STATE();
    case 162:
      if (lookahead == 'D' ||
          lookahead == 'd') ADVANCE(80);
      if (lookahead == 'M' ||
          lookahead == 'm') ADVANCE(403);
      END_STATE();
    case 163:
      if (lookahead == 'D' ||
          lookahead == 'd') ADVANCE(106);
      END_STATE();
    case 164:
      if (lookahead == 'D' ||
          lookahead == 'd') ADVANCE(100);
      END_STATE();
    case 165:
      if (lookahead == 'D' ||
          lookahead == 'd') ADVANCE(54);
      END_STATE();
    case 166:
      if (lookahead == 'D' ||
          lookahead == 'd') ADVANCE(84);
      END_STATE();
    case 167:
      if (lookahead == 'D' ||
          lookahead == 'd') ADVANCE(210);
      END_STATE();
    case 168:
      if (lookahead == 'D' ||
          lookahead == 'd') ADVANCE(250);
      END_STATE();
    case 169:
      if (lookahead == 'D' ||
          lookahead == 'd') ADVANCE(215);
      END_STATE();
    case 170:
      if (lookahead == 'D' ||
          lookahead == 'd') ADVANCE(220);
      END_STATE();
    case 171:
      if (lookahead == 'D' ||
          lookahead == 'd') ADVANCE(195);
      END_STATE();
    case 172:
      if (lookahead == 'D' ||
          lookahead == 'd') ADVANCE(196);
      END_STATE();
    case 173:
      if (lookahead == 'D' ||
          lookahead == 'd') ADVANCE(601);
      END_STATE();
    case 174:
      if (lookahead == 'D' ||
          lookahead == 'd') ADVANCE(81);
      if (lookahead == 'M' ||
          lookahead == 'm') ADVANCE(404);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(649);
      END_STATE();
    case 175:
      ADVANCE_MAP(
        'D', 99,
        'd', 99,
        'F', 319,
        'f', 319,
        'M', 423,
        'm', 423,
        'Q', 594,
        'q', 594,
        'W', 247,
        'w', 247,
        'Y', 260,
        'y', 260,
      );
      END_STATE();
    case 176:
      if (lookahead == 'D' ||
          lookahead == 'd') ADVANCE(103);
      END_STATE();
    case 177:
      if (lookahead == 'D' ||
          lookahead == 'd') ADVANCE(105);
      END_STATE();
    case 178:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(134);
      END_STATE();
    case 179:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(602);
      END_STATE();
    case 180:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(367);
      END_STATE();
    case 181:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(280);
      END_STATE();
    case 182:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(689);
      END_STATE();
    case 183:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(754);
      END_STATE();
    case 184:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(675);
      END_STATE();
    case 185:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(746);
      END_STATE();
    case 186:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(746);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(801);
      END_STATE();
    case 187:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(727);
      END_STATE();
    case 188:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(747);
      END_STATE();
    case 189:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(83);
      END_STATE();
    case 190:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(672);
      END_STATE();
    case 191:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(708);
      END_STATE();
    case 192:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(693);
      END_STATE();
    case 193:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(743);
      END_STATE();
    case 194:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(695);
      END_STATE();
    case 195:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(718);
      END_STATE();
    case 196:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(719);
      END_STATE();
    case 197:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(268);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(655);
      END_STATE();
    case 198:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(607);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(649);
      END_STATE();
    case 199:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(340);
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(373);
      END_STATE();
    case 200:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(443);
      END_STATE();
    case 201:
      ADVANCE_MAP(
        'E', 74,
        'e', 74,
        'H', 207,
        'h', 207,
        'O', 162,
        'o', 162,
        'R', 79,
        'r', 79,
        'Y', 433,
        'y', 433,
      );
      END_STATE();
    case 202:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(414);
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(392);
      END_STATE();
    case 203:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(444);
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(370);
      END_STATE();
    case 204:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(328);
      END_STATE();
    case 205:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(604);
      END_STATE();
    case 206:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(336);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(655);
      END_STATE();
    case 207:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(366);
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(506);
      END_STATE();
    case 208:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(136);
      END_STATE();
    case 209:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(136);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(801);
      END_STATE();
    case 210:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(474);
      END_STATE();
    case 211:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(358);
      END_STATE();
    case 212:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(320);
      END_STATE();
    case 213:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(321);
      END_STATE();
    case 214:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(338);
      END_STATE();
    case 215:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(495);
      END_STATE();
    case 216:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(472);
      END_STATE();
    case 217:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(472);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(801);
      END_STATE();
    case 218:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(322);
      END_STATE();
    case 219:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(608);
      END_STATE();
    case 220:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(496);
      END_STATE();
    case 221:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(325);
      END_STATE();
    case 222:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(350);
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(359);
      END_STATE();
    case 223:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(142);
      END_STATE();
    case 224:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(476);
      END_STATE();
    case 225:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(154);
      END_STATE();
    case 226:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(534);
      END_STATE();
    case 227:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(371);
      END_STATE();
    case 228:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(156);
      END_STATE();
    case 229:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(481);
      END_STATE();
    case 230:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(394);
      END_STATE();
    case 231:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(501);
      END_STATE();
    case 232:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(504);
      END_STATE();
    case 233:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(465);
      END_STATE();
    case 234:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(493);
      END_STATE();
    case 235:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(204);
      END_STATE();
    case 236:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(204);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(801);
      END_STATE();
    case 237:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(448);
      END_STATE();
    case 238:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(449);
      END_STATE();
    case 239:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(450);
      END_STATE();
    case 240:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(478);
      END_STATE();
    case 241:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(212);
      END_STATE();
    case 242:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(454);
      END_STATE();
    case 243:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(455);
      END_STATE();
    case 244:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(213);
      END_STATE();
    case 245:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(456);
      END_STATE();
    case 246:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(218);
      END_STATE();
    case 247:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(221);
      END_STATE();
    case 248:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(505);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(655);
      END_STATE();
    case 249:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(610);
      END_STATE();
    case 250:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(512);
      END_STATE();
    case 251:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(279);
      END_STATE();
    case 252:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(88);
      END_STATE();
    case 253:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(88);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(801);
      END_STATE();
    case 254:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(386);
      END_STATE();
    case 255:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(346);
      END_STATE();
    case 256:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(91);
      END_STATE();
    case 257:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(92);
      END_STATE();
    case 258:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(526);
      END_STATE();
    case 259:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(93);
      END_STATE();
    case 260:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(94);
      END_STATE();
    case 261:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(95);
      END_STATE();
    case 262:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(96);
      END_STATE();
    case 263:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(97);
      END_STATE();
    case 264:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(281);
      END_STATE();
    case 265:
      if (lookahead == 'F' ||
          lookahead == 'f') ADVANCE(270);
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(712);
      END_STATE();
    case 266:
      if (lookahead == 'F' ||
          lookahead == 'f') ADVANCE(270);
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(167);
      END_STATE();
    case 267:
      if (lookahead == 'F' ||
          lookahead == 'f') ADVANCE(684);
      END_STATE();
    case 268:
      if (lookahead == 'F' ||
          lookahead == 'f') ADVANCE(217);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(649);
      END_STATE();
    case 269:
      if (lookahead == 'F' ||
          lookahead == 'f') ADVANCE(310);
      if (lookahead == 'L' ||
          lookahead == 'l') ADVANCE(109);
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') ADVANCE(269);
      END_STATE();
    case 270:
      if (lookahead == 'F' ||
          lookahead == 'f') ADVANCE(521);
      END_STATE();
    case 271:
      if (lookahead == 'F' ||
          lookahead == 'f') ADVANCE(412);
      END_STATE();
    case 272:
      ADVANCE_MAP(
        'F', 318,
        'f', 318,
        'M', 421,
        'm', 421,
        'Q', 593,
        'q', 593,
        'W', 246,
        'w', 246,
        'Y', 259,
        'y', 259,
      );
      END_STATE();
    case 273:
      if (lookahead == 'G' ||
          lookahead == 'g') ADVANCE(670);
      END_STATE();
    case 274:
      if (lookahead == 'G' ||
          lookahead == 'g') ADVANCE(698);
      END_STATE();
    case 275:
      if (lookahead == 'G' ||
          lookahead == 'g') ADVANCE(734);
      END_STATE();
    case 276:
      if (lookahead == 'G' ||
          lookahead == 'g') ADVANCE(674);
      END_STATE();
    case 277:
      if (lookahead == 'G' ||
          lookahead == 'g') ADVANCE(391);
      END_STATE();
    case 278:
      if (lookahead == 'G' ||
          lookahead == 'g') ADVANCE(477);
      END_STATE();
    case 279:
      if (lookahead == 'G' ||
          lookahead == 'g') ADVANCE(409);
      END_STATE();
    case 280:
      if (lookahead == 'G' ||
          lookahead == 'g') ADVANCE(108);
      END_STATE();
    case 281:
      if (lookahead == 'G' ||
          lookahead == 'g') ADVANCE(111);
      END_STATE();
    case 282:
      if (lookahead == 'H' ||
          lookahead == 'h') ADVANCE(715);
      END_STATE();
    case 283:
      if (lookahead == 'H' ||
          lookahead == 'h') ADVANCE(765);
      END_STATE();
    case 284:
      if (lookahead == 'H' ||
          lookahead == 'h') ADVANCE(767);
      END_STATE();
    case 285:
      if (lookahead == 'H' ||
          lookahead == 'h') ADVANCE(766);
      END_STATE();
    case 286:
      if (lookahead == 'H' ||
          lookahead == 'h') ADVANCE(294);
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(174);
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(584);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(655);
      END_STATE();
    case 287:
      if (lookahead == 'H' ||
          lookahead == 'h') ADVANCE(180);
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(532);
      END_STATE();
    case 288:
      if (lookahead == 'H' ||
          lookahead == 'h') ADVANCE(497);
      END_STATE();
    case 289:
      if (lookahead == 'H' ||
          lookahead == 'h') ADVANCE(511);
      END_STATE();
    case 290:
      if (lookahead == 'H' ||
          lookahead == 'h') ADVANCE(304);
      END_STATE();
    case 291:
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(323);
      END_STATE();
    case 292:
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(373);
      END_STATE();
    case 293:
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(126);
      END_STATE();
    case 294:
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(507);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(649);
      END_STATE();
    case 295:
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(523);
      END_STATE();
    case 296:
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(523);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(801);
      END_STATE();
    case 297:
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(198);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(655);
      END_STATE();
    case 298:
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(179);
      END_STATE();
    case 299:
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(372);
      END_STATE();
    case 300:
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(436);
      END_STATE();
    case 301:
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(533);
      END_STATE();
    case 302:
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(374);
      END_STATE();
    case 303:
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(514);
      END_STATE();
    case 304:
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(376);
      END_STATE();
    case 305:
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(348);
      END_STATE();
    case 306:
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(380);
      END_STATE();
    case 307:
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(551);
      END_STATE();
    case 308:
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(369);
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') ADVANCE(308);
      END_STATE();
    case 309:
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(405);
      END_STATE();
    case 310:
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(485);
      END_STATE();
    case 311:
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(554);
      END_STATE();
    case 312:
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(205);
      END_STATE();
    case 313:
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(219);
      END_STATE();
    case 314:
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(556);
      END_STATE();
    case 315:
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(578);
      END_STATE();
    case 316:
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(527);
      END_STATE();
    case 317:
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(528);
      END_STATE();
    case 318:
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(529);
      END_STATE();
    case 319:
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(530);
      END_STATE();
    case 320:
      if (lookahead == 'K' ||
          lookahead == 'k') ADVANCE(762);
      END_STATE();
    case 321:
      if (lookahead == 'K' ||
          lookahead == 'k') ADVANCE(764);
      END_STATE();
    case 322:
      if (lookahead == 'K' ||
          lookahead == 'k') ADVANCE(763);
      END_STATE();
    case 323:
      if (lookahead == 'K' ||
          lookahead == 'k') ADVANCE(183);
      if (lookahead == 'M' ||
          lookahead == 'm') ADVANCE(301);
      END_STATE();
    case 324:
      if (lookahead == 'K' ||
          lookahead == 'k') ADVANCE(183);
      if (lookahead == 'M' ||
          lookahead == 'm') ADVANCE(301);
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(543);
      END_STATE();
    case 325:
      if (lookahead == 'K' ||
          lookahead == 'k') ADVANCE(497);
      END_STATE();
    case 326:
      if (lookahead == 'K' ||
          lookahead == 'k') ADVANCE(583);
      END_STATE();
    case 327:
      if (lookahead == 'K' ||
          lookahead == 'k') ADVANCE(302);
      END_STATE();
    case 328:
      if (lookahead == 'K' ||
          lookahead == 'k') ADVANCE(511);
      END_STATE();
    case 329:
      if (lookahead == 'L' ||
          lookahead == 'l') ADVANCE(705);
      END_STATE();
    case 330:
      if (lookahead == 'L' ||
          lookahead == 'l') ADVANCE(786);
      END_STATE();
    case 331:
      if (lookahead == 'L' ||
          lookahead == 'l') ADVANCE(518);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(649);
      END_STATE();
    case 332:
      if (lookahead == 'L' ||
          lookahead == 'l') ADVANCE(333);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(649);
      END_STATE();
    case 333:
      if (lookahead == 'L' ||
          lookahead == 'l') ADVANCE(785);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(801);
      END_STATE();
    case 334:
      ADVANCE_MAP(
        'L', 513,
        'l', 513,
        'N', 150,
        'n', 150,
        'V', 200,
        'v', 200,
        'X', 132,
        'x', 132,
      );
      END_STATE();
    case 335:
      if (lookahead == 'L' ||
          lookahead == 'l') ADVANCE(513);
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(150);
      if (lookahead == 'X' ||
          lookahead == 'x') ADVANCE(132);
      END_STATE();
    case 336:
      if (lookahead == 'L' ||
          lookahead == 'l') ADVANCE(209);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(649);
      END_STATE();
    case 337:
      if (lookahead == 'L' ||
          lookahead == 'l') ADVANCE(589);
      END_STATE();
    case 338:
      if (lookahead == 'L' ||
          lookahead == 'l') ADVANCE(161);
      END_STATE();
    case 339:
      if (lookahead == 'L' ||
          lookahead == 'l') ADVANCE(181);
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(128);
      END_STATE();
    case 340:
      if (lookahead == 'L' ||
          lookahead == 'l') ADVANCE(395);
      END_STATE();
    case 341:
      if (lookahead == 'L' ||
          lookahead == 'l') ADVANCE(330);
      END_STATE();
    case 342:
      if (lookahead == 'L' ||
          lookahead == 'l') ADVANCE(50);
      END_STATE();
    case 343:
      if (lookahead == 'L' ||
          lookahead == 'l') ADVANCE(345);
      END_STATE();
    case 344:
      if (lookahead == 'L' ||
          lookahead == 'l') ADVANCE(398);
      END_STATE();
    case 345:
      if (lookahead == 'L' ||
          lookahead == 'l') ADVANCE(508);
      END_STATE();
    case 346:
      if (lookahead == 'L' ||
          lookahead == 'l') ADVANCE(400);
      END_STATE();
    case 347:
      if (lookahead == 'L' ||
          lookahead == 'l') ADVANCE(517);
      END_STATE();
    case 348:
      if (lookahead == 'L' ||
          lookahead == 'l') ADVANCE(311);
      END_STATE();
    case 349:
      if (lookahead == 'L' ||
          lookahead == 'l') ADVANCE(590);
      END_STATE();
    case 350:
      if (lookahead == 'L' ||
          lookahead == 'l') ADVANCE(264);
      END_STATE();
    case 351:
      if (lookahead == 'L' ||
          lookahead == 'l') ADVANCE(63);
      END_STATE();
    case 352:
      if (lookahead == 'L' ||
          lookahead == 'l') ADVANCE(65);
      END_STATE();
    case 353:
      if (lookahead == 'L' ||
          lookahead == 'l') ADVANCE(66);
      END_STATE();
    case 354:
      if (lookahead == 'L' ||
          lookahead == 'l') ADVANCE(67);
      END_STATE();
    case 355:
      if (lookahead == 'M' ||
          lookahead == 'm') ADVANCE(699);
      END_STATE();
    case 356:
      if (lookahead == 'M' ||
          lookahead == 'm') ADVANCE(679);
      END_STATE();
    case 357:
      if (lookahead == 'M' ||
          lookahead == 'm') ADVANCE(706);
      END_STATE();
    case 358:
      if (lookahead == 'M' ||
          lookahead == 'm') ADVANCE(64);
      END_STATE();
    case 359:
      if (lookahead == 'M' ||
          lookahead == 'm') ADVANCE(104);
      END_STATE();
    case 360:
      if (lookahead == 'M' ||
          lookahead == 'm') ADVANCE(420);
      END_STATE();
    case 361:
      if (lookahead == 'M' ||
          lookahead == 'm') ADVANCE(626);
      END_STATE();
    case 362:
      if (lookahead == 'M' ||
          lookahead == 'm') ADVANCE(60);
      END_STATE();
    case 363:
      if (lookahead == 'M' ||
          lookahead == 'm') ADVANCE(422);
      END_STATE();
    case 364:
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(149);
      END_STATE();
    case 365:
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(755);
      END_STATE();
    case 366:
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(688);
      END_STATE();
    case 367:
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(687);
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(191);
      END_STATE();
    case 368:
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(741);
      END_STATE();
    case 369:
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(756);
      END_STATE();
    case 370:
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(273);
      END_STATE();
    case 371:
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(271);
      END_STATE();
    case 372:
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(274);
      END_STATE();
    case 373:
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(151);
      END_STATE();
    case 374:
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(275);
      END_STATE();
    case 375:
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(138);
      END_STATE();
    case 376:
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(276);
      END_STATE();
    case 377:
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(165);
      END_STATE();
    case 378:
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(184);
      END_STATE();
    case 379:
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(544);
      END_STATE();
    case 380:
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(500);
      END_STATE();
    case 381:
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(546);
      END_STATE();
    case 382:
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(548);
      END_STATE();
    case 383:
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(550);
      END_STATE();
    case 384:
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(552);
      END_STATE();
    case 385:
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(164);
      END_STATE();
    case 386:
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(139);
      END_STATE();
    case 387:
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(574);
      END_STATE();
    case 388:
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(442);
      END_STATE();
    case 389:
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(355);
      END_STATE();
    case 390:
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(326);
      END_STATE();
    case 391:
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(782);
      END_STATE();
    case 392:
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(582);
      END_STATE();
    case 393:
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(599);
      END_STATE();
    case 394:
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(267);
      END_STATE();
    case 395:
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(603);
      END_STATE();
    case 396:
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(357);
      END_STATE();
    case 397:
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(605);
      END_STATE();
    case 398:
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(141);
      END_STATE();
    case 399:
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(460);
      END_STATE();
    case 400:
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(606);
      END_STATE();
    case 401:
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(379);
      END_STATE();
    case 402:
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(379);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(801);
      END_STATE();
    case 403:
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(467);
      END_STATE();
    case 404:
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(467);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(801);
      END_STATE();
    case 405:
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(368);
      END_STATE();
    case 406:
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(469);
      END_STATE();
    case 407:
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(461);
      END_STATE();
    case 408:
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(462);
      END_STATE();
    case 409:
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(463);
      END_STATE();
    case 410:
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(547);
      if (lookahead == 'U' ||
          lookahead == 'u') ADVANCE(343);
      END_STATE();
    case 411:
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(479);
      END_STATE();
    case 412:
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(482);
      END_STATE();
    case 413:
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(466);
      END_STATE();
    case 414:
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(344);
      END_STATE();
    case 415:
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(434);
      END_STATE();
    case 416:
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(381);
      END_STATE();
    case 417:
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(586);
      END_STATE();
    case 418:
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(468);
      END_STATE();
    case 419:
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(382);
      END_STATE();
    case 420:
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(171);
      END_STATE();
    case 421:
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(383);
      END_STATE();
    case 422:
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(172);
      END_STATE();
    case 423:
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(384);
      END_STATE();
    case 424:
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(387);
      END_STATE();
    case 425:
      if (lookahead == 'P' ||
          lookahead == 'p') ADVANCE(680);
      END_STATE();
    case 426:
      if (lookahead == 'P' ||
          lookahead == 'p') ADVANCE(159);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(655);
      END_STATE();
    case 427:
      if (lookahead == 'P' ||
          lookahead == 'p') ADVANCE(633);
      END_STATE();
    case 428:
      if (lookahead == 'P' ||
          lookahead == 'p') ADVANCE(163);
      END_STATE();
    case 429:
      if (lookahead == 'P' ||
          lookahead == 'p') ADVANCE(163);
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(203);
      END_STATE();
    case 430:
      if (lookahead == 'P' ||
          lookahead == 'p') ADVANCE(431);
      END_STATE();
    case 431:
      if (lookahead == 'P' ||
          lookahead == 'p') ADVANCE(406);
      END_STATE();
    case 432:
      if (lookahead == 'P' ||
          lookahead == 'p') ADVANCE(502);
      END_STATE();
    case 433:
      if (lookahead == 'P' ||
          lookahead == 'p') ADVANCE(230);
      END_STATE();
    case 434:
      if (lookahead == 'P' ||
          lookahead == 'p') ADVANCE(190);
      END_STATE();
    case 435:
      if (lookahead == 'P' ||
          lookahead == 'p') ADVANCE(240);
      END_STATE();
    case 436:
      if (lookahead == 'P' ||
          lookahead == 'p') ADVANCE(577);
      END_STATE();
    case 437:
      if (lookahead == 'Q' ||
          lookahead == 'q') ADVANCE(587);
      if (lookahead == 'Y' ||
          lookahead == 'y') ADVANCE(252);
      END_STATE();
    case 438:
      if (lookahead == 'Q' ||
          lookahead == 'q') ADVANCE(594);
      if (lookahead == 'Y' ||
          lookahead == 'y') ADVANCE(260);
      END_STATE();
    case 439:
      if (lookahead == 'Q' ||
          lookahead == 'q') ADVANCE(595);
      if (lookahead == 'Y' ||
          lookahead == 'y') ADVANCE(261);
      END_STATE();
    case 440:
      if (lookahead == 'Q' ||
          lookahead == 'q') ADVANCE(596);
      if (lookahead == 'Y' ||
          lookahead == 'y') ADVANCE(262);
      END_STATE();
    case 441:
      if (lookahead == 'Q' ||
          lookahead == 'q') ADVANCE(597);
      if (lookahead == 'Y' ||
          lookahead == 'y') ADVANCE(263);
      END_STATE();
    case 442:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(691);
      END_STATE();
    case 443:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(621);
      END_STATE();
    case 444:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(52);
      END_STATE();
    case 445:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(774);
      END_STATE();
    case 446:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(775);
      END_STATE();
    case 447:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(773);
      END_STATE();
    case 448:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(771);
      END_STATE();
    case 449:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(772);
      END_STATE();
    case 450:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(770);
      END_STATE();
    case 451:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(780);
      END_STATE();
    case 452:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(781);
      END_STATE();
    case 453:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(779);
      END_STATE();
    case 454:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(777);
      END_STATE();
    case 455:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(778);
      END_STATE();
    case 456:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(776);
      END_STATE();
    case 457:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(711);
      END_STATE();
    case 458:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(392);
      END_STATE();
    case 459:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(300);
      END_STATE();
    case 460:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(173);
      END_STATE();
    case 461:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(49);
      END_STATE();
    case 462:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(617);
      END_STATE();
    case 463:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(618);
      END_STATE();
    case 464:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(153);
      END_STATE();
    case 465:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(497);
      END_STATE();
    case 466:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(619);
      END_STATE();
    case 467:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(471);
      END_STATE();
    case 468:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(157);
      END_STATE();
    case 469:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(553);
      END_STATE();
    case 470:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(78);
      END_STATE();
    case 471:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(397);
      END_STATE();
    case 472:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(254);
      END_STATE();
    case 473:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(223);
      END_STATE();
    case 474:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(636);
      END_STATE();
    case 475:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(307);
      END_STATE();
    case 476:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(511);
      END_STATE();
    case 477:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(417);
      END_STATE();
    case 478:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(473);
      END_STATE();
    case 479:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(435);
      END_STATE();
    case 480:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(314);
      END_STATE();
    case 481:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(166);
      END_STATE();
    case 482:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(140);
      END_STATE();
    case 483:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(315);
      END_STATE();
    case 484:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(563);
      END_STATE();
    case 485:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(520);
      END_STATE();
    case 486:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(566);
      END_STATE();
    case 487:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(567);
      END_STATE();
    case 488:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(568);
      END_STATE();
    case 489:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(569);
      END_STATE();
    case 490:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(571);
      END_STATE();
    case 491:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(572);
      END_STATE();
    case 492:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(573);
      END_STATE();
    case 493:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(483);
      END_STATE();
    case 494:
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(703);
      END_STATE();
    case 495:
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(758);
      END_STATE();
    case 496:
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(757);
      END_STATE();
    case 497:
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(782);
      END_STATE();
    case 498:
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(768);
      END_STATE();
    case 499:
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(769);
      END_STATE();
    case 500:
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(723);
      END_STATE();
    case 501:
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(724);
      END_STATE();
    case 502:
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(676);
      END_STATE();
    case 503:
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(541);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(649);
      END_STATE();
    case 504:
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(128);
      END_STATE();
    case 505:
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(560);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(649);
      END_STATE();
    case 506:
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(48);
      END_STATE();
    case 507:
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(48);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(801);
      END_STATE();
    case 508:
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(634);
      END_STATE();
    case 509:
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(555);
      END_STATE();
    case 510:
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(540);
      END_STATE();
    case 511:
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(56);
      END_STATE();
    case 512:
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(137);
      END_STATE();
    case 513:
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(182);
      END_STATE();
    case 514:
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(293);
      END_STATE();
    case 515:
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(129);
      END_STATE();
    case 516:
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(155);
      END_STATE();
    case 517:
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(188);
      END_STATE();
    case 518:
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(188);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(801);
      END_STATE();
    case 519:
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(537);
      END_STATE();
    case 520:
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(538);
      END_STATE();
    case 521:
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(226);
      END_STATE();
    case 522:
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(575);
      END_STATE();
    case 523:
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(135);
      END_STATE();
    case 524:
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(558);
      END_STATE();
    case 525:
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(576);
      END_STATE();
    case 526:
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(559);
      END_STATE();
    case 527:
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(145);
      END_STATE();
    case 528:
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(146);
      END_STATE();
    case 529:
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(147);
      END_STATE();
    case 530:
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(148);
      END_STATE();
    case 531:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(713);
      END_STATE();
    case 532:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(282);
      END_STATE();
    case 533:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(730);
      END_STATE();
    case 534:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(732);
      END_STATE();
    case 535:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(668);
      END_STATE();
    case 536:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(735);
      END_STATE();
    case 537:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(740);
      END_STATE();
    case 538:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(739);
      END_STATE();
    case 539:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(721);
      END_STATE();
    case 540:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(47);
      END_STATE();
    case 541:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(47);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(801);
      END_STATE();
    case 542:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(290);
      END_STATE();
    case 543:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(600);
      END_STATE();
    case 544:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(289);
      END_STATE();
    case 545:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(71);
      END_STATE();
    case 546:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(283);
      END_STATE();
    case 547:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(635);
      END_STATE();
    case 548:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(284);
      END_STATE();
    case 549:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(309);
      END_STATE();
    case 550:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(285);
      END_STATE();
    case 551:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(624);
      END_STATE();
    case 552:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(288);
      END_STATE();
    case 553:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(516);
      END_STATE();
    case 554:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(620);
      END_STATE();
    case 555:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(396);
      END_STATE();
    case 556:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(408);
      END_STATE();
    case 557:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(189);
      END_STATE();
    case 558:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(211);
      END_STATE();
    case 559:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(229);
      END_STATE();
    case 560:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(229);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(801);
      END_STATE();
    case 561:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(192);
      END_STATE();
    case 562:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(225);
      END_STATE();
    case 563:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(224);
      END_STATE();
    case 564:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(251);
      END_STATE();
    case 565:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(234);
      END_STATE();
    case 566:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(237);
      END_STATE();
    case 567:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(238);
      END_STATE();
    case 568:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(239);
      END_STATE();
    case 569:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(233);
      END_STATE();
    case 570:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(231);
      END_STATE();
    case 571:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(242);
      END_STATE();
    case 572:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(243);
      END_STATE();
    case 573:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(245);
      END_STATE();
    case 574:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(249);
      END_STATE();
    case 575:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(85);
      END_STATE();
    case 576:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(90);
      END_STATE();
    case 577:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(411);
      END_STATE();
    case 578:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(413);
      END_STATE();
    case 579:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(62);
      END_STATE();
    case 580:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(62);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(801);
      END_STATE();
    case 581:
      if (lookahead == 'U' ||
          lookahead == 'u') ADVANCE(509);
      END_STATE();
    case 582:
      if (lookahead == 'U' ||
          lookahead == 'u') ADVANCE(427);
      END_STATE();
    case 583:
      if (lookahead == 'U' ||
          lookahead == 'u') ADVANCE(425);
      END_STATE();
    case 584:
      if (lookahead == 'U' ||
          lookahead == 'u') ADVANCE(186);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(649);
      END_STATE();
    case 585:
      if (lookahead == 'U' ||
          lookahead == 'u') ADVANCE(475);
      END_STATE();
    case 586:
      if (lookahead == 'U' ||
          lookahead == 'u') ADVANCE(432);
      END_STATE();
    case 587:
      if (lookahead == 'U' ||
          lookahead == 'u') ADVANCE(87);
      END_STATE();
    case 588:
      if (lookahead == 'U' ||
          lookahead == 'u') ADVANCE(87);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(801);
      END_STATE();
    case 589:
      if (lookahead == 'U' ||
          lookahead == 'u') ADVANCE(169);
      END_STATE();
    case 590:
      if (lookahead == 'U' ||
          lookahead == 'u') ADVANCE(170);
      END_STATE();
    case 591:
      if (lookahead == 'U' ||
          lookahead == 'u') ADVANCE(112);
      END_STATE();
    case 592:
      if (lookahead == 'U' ||
          lookahead == 'u') ADVANCE(113);
      END_STATE();
    case 593:
      if (lookahead == 'U' ||
          lookahead == 'u') ADVANCE(114);
      END_STATE();
    case 594:
      if (lookahead == 'U' ||
          lookahead == 'u') ADVANCE(115);
      END_STATE();
    case 595:
      if (lookahead == 'U' ||
          lookahead == 'u') ADVANCE(116);
      END_STATE();
    case 596:
      if (lookahead == 'U' ||
          lookahead == 'u') ADVANCE(117);
      END_STATE();
    case 597:
      if (lookahead == 'U' ||
          lookahead == 'u') ADVANCE(118);
      END_STATE();
    case 598:
      if (lookahead == 'V' ||
          lookahead == 'v') ADVANCE(299);
      END_STATE();
    case 599:
      if (lookahead == 'V' ||
          lookahead == 'v') ADVANCE(187);
      END_STATE();
    case 600:
      if (lookahead == 'V' ||
          lookahead == 'v') ADVANCE(312);
      END_STATE();
    case 601:
      if (lookahead == 'V' ||
          lookahead == 'v') ADVANCE(303);
      END_STATE();
    case 602:
      if (lookahead == 'W' ||
          lookahead == 'w') ADVANCE(697);
      END_STATE();
    case 603:
      if (lookahead == 'W' ||
          lookahead == 'w') ADVANCE(728);
      END_STATE();
    case 604:
      if (lookahead == 'W' ||
          lookahead == 'w') ADVANCE(681);
      END_STATE();
    case 605:
      if (lookahead == 'W' ||
          lookahead == 'w') ADVANCE(761);
      END_STATE();
    case 606:
      if (lookahead == 'W' ||
          lookahead == 'w') ADVANCE(729);
      END_STATE();
    case 607:
      if (lookahead == 'W' ||
          lookahead == 'w') ADVANCE(696);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(801);
      END_STATE();
    case 608:
      if (lookahead == 'W' ||
          lookahead == 'w') ADVANCE(525);
      END_STATE();
    case 609:
      if (lookahead == 'X' ||
          lookahead == 'x') ADVANCE(168);
      END_STATE();
    case 610:
      if (lookahead == 'X' ||
          lookahead == 'x') ADVANCE(539);
      END_STATE();
    case 611:
      if (lookahead == 'X' ||
          lookahead == 'x') ADVANCE(579);
      END_STATE();
    case 612:
      if (lookahead == 'X' ||
          lookahead == 'x') ADVANCE(580);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(649);
      END_STATE();
    case 613:
      if (lookahead == 'Y' ||
          lookahead == 'y') ADVANCE(760);
      END_STATE();
    case 614:
      if (lookahead == 'Y' ||
          lookahead == 'y') ADVANCE(690);
      END_STATE();
    case 615:
      if (lookahead == 'Y' ||
          lookahead == 'y') ADVANCE(736);
      END_STATE();
    case 616:
      if (lookahead == 'Y' ||
          lookahead == 'y') ADVANCE(759);
      END_STATE();
    case 617:
      if (lookahead == 'Y' ||
          lookahead == 'y') ADVANCE(677);
      END_STATE();
    case 618:
      if (lookahead == 'Y' ||
          lookahead == 'y') ADVANCE(725);
      END_STATE();
    case 619:
      if (lookahead == 'Y' ||
          lookahead == 'y') ADVANCE(678);
      END_STATE();
    case 620:
      if (lookahead == 'Y' ||
          lookahead == 'y') ADVANCE(143);
      END_STATE();
    case 621:
      if (lookahead == 'Y' ||
          lookahead == 'y') ADVANCE(542);
      END_STATE();
    case 622:
      if (lookahead == 'Y' ||
          lookahead == 'y') ADVANCE(497);
      END_STATE();
    case 623:
      if (lookahead == 'Y' ||
          lookahead == 'y') ADVANCE(498);
      END_STATE();
    case 624:
      if (lookahead == 'Y' ||
          lookahead == 'y') ADVANCE(57);
      END_STATE();
    case 625:
      if (lookahead == 'Y' ||
          lookahead == 'y') ADVANCE(499);
      END_STATE();
    case 626:
      if (lookahead == 'Y' ||
          lookahead == 'y') ADVANCE(55);
      END_STATE();
    case 627:
      if (lookahead == 'Y' ||
          lookahead == 'y') ADVANCE(511);
      END_STATE();
    case 628:
      if (lookahead == '0' ||
          lookahead == '1') ADVANCE(799);
      END_STATE();
    case 629:
      if (lookahead == '0' ||
          lookahead == '1') ADVANCE(652);
      END_STATE();
    case 630:
      if (('0' <= lookahead && lookahead <= '2')) ADVANCE(13);
      END_STATE();
    case 631:
      if (('0' <= lookahead && lookahead <= '3')) ADVANCE(25);
      END_STATE();
    case 632:
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') ADVANCE(144);
      END_STATE();
    case 633:
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') ADVANCE(124);
      END_STATE();
    case 634:
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') ADVANCE(269);
      END_STATE();
    case 635:
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') ADVANCE(308);
      END_STATE();
    case 636:
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') ADVANCE(125);
      END_STATE();
    case 637:
      if (('0' <= lookahead && lookahead <= '5')) ADVANCE(646);
      END_STATE();
    case 638:
      if (('0' <= lookahead && lookahead <= '5')) ADVANCE(647);
      END_STATE();
    case 639:
      if (('0' <= lookahead && lookahead <= '5')) ADVANCE(651);
      END_STATE();
    case 640:
      if (('1' <= lookahead && lookahead <= '9')) ADVANCE(13);
      END_STATE();
    case 641:
      if (('1' <= lookahead && lookahead <= '9')) ADVANCE(799);
      END_STATE();
    case 642:
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(797);
      END_STATE();
    case 643:
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(798);
      END_STATE();
    case 644:
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(799);
      END_STATE();
    case 645:
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(25);
      END_STATE();
    case 646:
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(14);
      END_STATE();
    case 647:
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(800);
      END_STATE();
    case 648:
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(45);
      END_STATE();
    case 649:
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(801);
      END_STATE();
    case 650:
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(802);
      END_STATE();
    case 651:
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(26);
      END_STATE();
    case 652:
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(27);
      END_STATE();
    case 653:
      ADVANCE_MAP(
        '"', 2,
        '%', 2,
        '\'', 2,
        'B', 2,
        'F', 2,
        'N', 2,
        'R', 2,
        'T', 2,
        'U', 2,
        '\\', 2,
        '_', 2,
        'b', 2,
        'f', 2,
        'n', 2,
        'r', 2,
        't', 2,
        'u', 2,
      );
      END_STATE();
    case 654:
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(655);
      END_STATE();
    case 655:
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(649);
      END_STATE();
    case 656:
      if (eof) ADVANCE(658);
      ADVANCE_MAP(
        '!', 28,
        '(', 666,
        ')', 667,
        '*', 10,
        ',', 669,
        '-', 642,
        '.', 745,
        '/', 17,
        '<', 750,
        '=', 682,
        '>', 752,
        'A', 515,
        'a', 515,
        'B', 292,
        'b', 292,
        'D', 232,
        'd', 232,
        'E', 335,
        'e', 335,
        'F', 388,
        'f', 388,
        'G', 458,
        'g', 458,
        'H', 69,
        'h', 69,
        'I', 365,
        'i', 365,
        'L', 291,
        'l', 291,
        'N', 410,
        'n', 410,
        'O', 266,
        'o', 266,
        'T', 470,
        't', 470,
        'U', 428,
        'u', 428,
        'V', 313,
        'v', 313,
        'W', 287,
        'w', 287,
      );
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') SKIP(656);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(797);
      END_STATE();
    case 657:
      if (eof) ADVANCE(658);
      ADVANCE_MAP(
        ')', 667,
        ',', 669,
        '.', 745,
        '/', 17,
        'A', 857,
        'a', 857,
        'F', 843,
        'f', 843,
        'G', 851,
        'g', 851,
        'L', 828,
        'l', 828,
        'O', 821,
        'o', 821,
        'U', 849,
        'u', 849,
        'W', 825,
        'w', 825,
      );
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') SKIP(657);
      if (('B' <= lookahead && lookahead <= 'Z') ||
          ('b' <= lookahead && lookahead <= 'z')) ADVANCE(870);
      END_STATE();
    case 658:
      ACCEPT_TOKEN(ts_builtin_sym_end);
      END_STATE();
    case 659:
      ACCEPT_TOKEN(anon_sym_SLASH_SLASH);
      if (lookahead == '/') ADVANCE(664);
      END_STATE();
    case 660:
      ACCEPT_TOKEN(aux_sym_header_comment_token1);
      if (lookahead == '/') ADVANCE(662);
      if (lookahead == '\t' ||
          (0x0b <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') ADVANCE(660);
      if (lookahead != 0 &&
          (lookahead < '\t' || '\r' < lookahead)) ADVANCE(663);
      END_STATE();
    case 661:
      ACCEPT_TOKEN(aux_sym_header_comment_token1);
      if (lookahead == '/') ADVANCE(665);
      if (lookahead != 0 &&
          lookahead != '\n') ADVANCE(663);
      END_STATE();
    case 662:
      ACCEPT_TOKEN(aux_sym_header_comment_token1);
      if (lookahead == '/') ADVANCE(661);
      if (lookahead != 0 &&
          lookahead != '\n') ADVANCE(663);
      END_STATE();
    case 663:
      ACCEPT_TOKEN(aux_sym_header_comment_token1);
      if (lookahead != 0 &&
          lookahead != '\n') ADVANCE(663);
      END_STATE();
    case 664:
      ACCEPT_TOKEN(anon_sym_SLASH_SLASH_SLASH);
      END_STATE();
    case 665:
      ACCEPT_TOKEN(anon_sym_SLASH_SLASH_SLASH);
      if (lookahead != 0 &&
          lookahead != '\n') ADVANCE(663);
      END_STATE();
    case 666:
      ACCEPT_TOKEN(anon_sym_LPAREN);
      END_STATE();
    case 667:
      ACCEPT_TOKEN(anon_sym_RPAREN);
      END_STATE();
    case 668:
      ACCEPT_TOKEN(aux_sym_select_clause_token1);
      END_STATE();
    case 669:
      ACCEPT_TOKEN(anon_sym_COMMA);
      END_STATE();
    case 670:
      ACCEPT_TOKEN(aux_sym_using_clause_token1);
      END_STATE();
    case 671:
      ACCEPT_TOKEN(aux_sym_using_clause_token1);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(870);
      END_STATE();
    case 672:
      ACCEPT_TOKEN(aux_sym_using_scope_clause_token1);
      END_STATE();
    case 673:
      ACCEPT_TOKEN(aux_sym_using_scope_type_token1);
      END_STATE();
    case 674:
      ACCEPT_TOKEN(aux_sym_using_scope_type_token2);
      END_STATE();
    case 675:
      ACCEPT_TOKEN(aux_sym_using_scope_type_token3);
      if (lookahead == '_') ADVANCE(102);
      END_STATE();
    case 676:
      ACCEPT_TOKEN(aux_sym_using_scope_type_token4);
      END_STATE();
    case 677:
      ACCEPT_TOKEN(aux_sym_using_scope_type_token5);
      END_STATE();
    case 678:
      ACCEPT_TOKEN(aux_sym_using_scope_type_token6);
      END_STATE();
    case 679:
      ACCEPT_TOKEN(aux_sym_using_scope_type_token7);
      END_STATE();
    case 680:
      ACCEPT_TOKEN(aux_sym_using_lookup_clause_token1);
      END_STATE();
    case 681:
      ACCEPT_TOKEN(aux_sym_using_listview_clause_token1);
      END_STATE();
    case 682:
      ACCEPT_TOKEN(anon_sym_EQ);
      END_STATE();
    case 683:
      ACCEPT_TOKEN(aux_sym_using_lookup_bind_clause_token1);
      END_STATE();
    case 684:
      ACCEPT_TOKEN(aux_sym_type_of_clause_token1);
      END_STATE();
    case 685:
      ACCEPT_TOKEN(aux_sym_type_of_clause_token1);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(870);
      END_STATE();
    case 686:
      ACCEPT_TOKEN(aux_sym_type_of_clause_token2);
      END_STATE();
    case 687:
      ACCEPT_TOKEN(aux_sym_when_expression_token1);
      END_STATE();
    case 688:
      ACCEPT_TOKEN(aux_sym_when_expression_token2);
      END_STATE();
    case 689:
      ACCEPT_TOKEN(aux_sym_else_expression_token1);
      END_STATE();
    case 690:
      ACCEPT_TOKEN(aux_sym_group_by_clause_token1);
      END_STATE();
    case 691:
      ACCEPT_TOKEN(aux_sym_for_clause_token1);
      END_STATE();
    case 692:
      ACCEPT_TOKEN(aux_sym_for_clause_token1);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(870);
      END_STATE();
    case 693:
      ACCEPT_TOKEN(aux_sym_for_type_token1);
      END_STATE();
    case 694:
      ACCEPT_TOKEN(aux_sym_for_type_token1);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(870);
      END_STATE();
    case 695:
      ACCEPT_TOKEN(aux_sym_for_type_token2);
      END_STATE();
    case 696:
      ACCEPT_TOKEN(aux_sym_for_type_token3);
      END_STATE();
    case 697:
      ACCEPT_TOKEN(aux_sym_for_type_token3);
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(576);
      END_STATE();
    case 698:
      ACCEPT_TOKEN(aux_sym_having_clause_token1);
      END_STATE();
    case 699:
      ACCEPT_TOKEN(aux_sym_from_clause_token1);
      END_STATE();
    case 700:
      ACCEPT_TOKEN(aux_sym_from_clause_token1);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(870);
      END_STATE();
    case 701:
      ACCEPT_TOKEN(aux_sym_storage_alias_token1);
      END_STATE();
    case 702:
      ACCEPT_TOKEN(aux_sym_storage_alias_token1);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(870);
      END_STATE();
    case 703:
      ACCEPT_TOKEN(aux_sym_fields_expression_token1);
      END_STATE();
    case 704:
      ACCEPT_TOKEN(aux_sym_fields_expression_token1);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(870);
      END_STATE();
    case 705:
      ACCEPT_TOKEN(aux_sym_fields_type_token1);
      END_STATE();
    case 706:
      ACCEPT_TOKEN(aux_sym_fields_type_token2);
      END_STATE();
    case 707:
      ACCEPT_TOKEN(aux_sym_fields_type_token3);
      END_STATE();
    case 708:
      ACCEPT_TOKEN(aux_sym_where_clause_token1);
      END_STATE();
    case 709:
      ACCEPT_TOKEN(aux_sym_where_clause_token1);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(870);
      END_STATE();
    case 710:
      ACCEPT_TOKEN(aux_sym_and_expression_token1);
      END_STATE();
    case 711:
      ACCEPT_TOKEN(aux_sym_or_expression_token1);
      END_STATE();
    case 712:
      ACCEPT_TOKEN(aux_sym_or_expression_token1);
      if (lookahead == 'D' ||
          lookahead == 'd') ADVANCE(210);
      END_STATE();
    case 713:
      ACCEPT_TOKEN(aux_sym_not_expression_token1);
      END_STATE();
    case 714:
      ACCEPT_TOKEN(aux_sym_not_expression_token1);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(870);
      END_STATE();
    case 715:
      ACCEPT_TOKEN(aux_sym_soql_with_clause_token1);
      END_STATE();
    case 716:
      ACCEPT_TOKEN(aux_sym_soql_with_clause_token1);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(870);
      END_STATE();
    case 717:
      ACCEPT_TOKEN(aux_sym_soql_with_type_token1);
      END_STATE();
    case 718:
      ACCEPT_TOKEN(aux_sym_soql_with_type_token2);
      END_STATE();
    case 719:
      ACCEPT_TOKEN(aux_sym_soql_with_type_token3);
      END_STATE();
    case 720:
      ACCEPT_TOKEN(aux_sym_with_user_id_type_token1);
      END_STATE();
    case 721:
      ACCEPT_TOKEN(aux_sym_with_record_visibility_expression_token1);
      END_STATE();
    case 722:
      ACCEPT_TOKEN(aux_sym_with_record_visibility_param_token1);
      END_STATE();
    case 723:
      ACCEPT_TOKEN(aux_sym_with_record_visibility_param_token2);
      END_STATE();
    case 724:
      ACCEPT_TOKEN(aux_sym_with_record_visibility_param_token3);
      END_STATE();
    case 725:
      ACCEPT_TOKEN(aux_sym_with_data_cat_expression_token1);
      END_STATE();
    case 726:
      ACCEPT_TOKEN(aux_sym_with_data_cat_filter_type_token1);
      END_STATE();
    case 727:
      ACCEPT_TOKEN(aux_sym_with_data_cat_filter_type_token2);
      if (lookahead == '_') ADVANCE(407);
      END_STATE();
    case 728:
      ACCEPT_TOKEN(aux_sym_with_data_cat_filter_type_token3);
      END_STATE();
    case 729:
      ACCEPT_TOKEN(aux_sym_with_data_cat_filter_type_token4);
      END_STATE();
    case 730:
      ACCEPT_TOKEN(aux_sym_limit_clause_token1);
      END_STATE();
    case 731:
      ACCEPT_TOKEN(aux_sym_limit_clause_token1);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(870);
      END_STATE();
    case 732:
      ACCEPT_TOKEN(aux_sym_offset_clause_token1);
      END_STATE();
    case 733:
      ACCEPT_TOKEN(aux_sym_offset_clause_token1);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(870);
      END_STATE();
    case 734:
      ACCEPT_TOKEN(aux_sym_update_type_token1);
      END_STATE();
    case 735:
      ACCEPT_TOKEN(aux_sym_update_type_token2);
      END_STATE();
    case 736:
      ACCEPT_TOKEN(aux_sym_order_by_clause_token1);
      END_STATE();
    case 737:
      ACCEPT_TOKEN(aux_sym_order_direction_token1);
      END_STATE();
    case 738:
      ACCEPT_TOKEN(aux_sym_order_direction_token2);
      END_STATE();
    case 739:
      ACCEPT_TOKEN(aux_sym_order_null_direction_token1);
      END_STATE();
    case 740:
      ACCEPT_TOKEN(aux_sym_order_null_direction_token2);
      END_STATE();
    case 741:
      ACCEPT_TOKEN(aux_sym_geo_location_type_token1);
      END_STATE();
    case 742:
      ACCEPT_TOKEN(aux_sym_geo_location_type_token1);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(870);
      END_STATE();
    case 743:
      ACCEPT_TOKEN(aux_sym_function_expression_token1);
      END_STATE();
    case 744:
      ACCEPT_TOKEN(aux_sym_function_expression_token1);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(870);
      END_STATE();
    case 745:
      ACCEPT_TOKEN(anon_sym_DOT);
      END_STATE();
    case 746:
      ACCEPT_TOKEN(aux_sym_boolean_token1);
      END_STATE();
    case 747:
      ACCEPT_TOKEN(aux_sym_boolean_token2);
      END_STATE();
    case 748:
      ACCEPT_TOKEN(anon_sym_BANG_EQ);
      END_STATE();
    case 749:
      ACCEPT_TOKEN(anon_sym_LT_GT);
      END_STATE();
    case 750:
      ACCEPT_TOKEN(anon_sym_LT);
      if (lookahead == '=') ADVANCE(751);
      if (lookahead == '>') ADVANCE(749);
      END_STATE();
    case 751:
      ACCEPT_TOKEN(anon_sym_LT_EQ);
      END_STATE();
    case 752:
      ACCEPT_TOKEN(anon_sym_GT);
      if (lookahead == '=') ADVANCE(753);
      END_STATE();
    case 753:
      ACCEPT_TOKEN(anon_sym_GT_EQ);
      END_STATE();
    case 754:
      ACCEPT_TOKEN(aux_sym_value_comparison_operator_token1);
      END_STATE();
    case 755:
      ACCEPT_TOKEN(aux_sym_set_comparison_operator_token1);
      if (lookahead == 'C' ||
          lookahead == 'c') ADVANCE(349);
      END_STATE();
    case 756:
      ACCEPT_TOKEN(aux_sym_set_comparison_operator_token2);
      END_STATE();
    case 757:
      ACCEPT_TOKEN(aux_sym_set_comparison_operator_token3);
      END_STATE();
    case 758:
      ACCEPT_TOKEN(aux_sym_set_comparison_operator_token4);
      END_STATE();
    case 759:
      ACCEPT_TOKEN(aux_sym_date_literal_token1);
      END_STATE();
    case 760:
      ACCEPT_TOKEN(aux_sym_date_literal_token2);
      END_STATE();
    case 761:
      ACCEPT_TOKEN(aux_sym_date_literal_token3);
      END_STATE();
    case 762:
      ACCEPT_TOKEN(aux_sym_date_literal_token4);
      END_STATE();
    case 763:
      ACCEPT_TOKEN(aux_sym_date_literal_token5);
      END_STATE();
    case 764:
      ACCEPT_TOKEN(aux_sym_date_literal_token6);
      END_STATE();
    case 765:
      ACCEPT_TOKEN(aux_sym_date_literal_token7);
      END_STATE();
    case 766:
      ACCEPT_TOKEN(aux_sym_date_literal_token8);
      END_STATE();
    case 767:
      ACCEPT_TOKEN(aux_sym_date_literal_token9);
      END_STATE();
    case 768:
      ACCEPT_TOKEN(aux_sym_date_literal_token10);
      END_STATE();
    case 769:
      ACCEPT_TOKEN(aux_sym_date_literal_token11);
      END_STATE();
    case 770:
      ACCEPT_TOKEN(aux_sym_date_literal_token12);
      END_STATE();
    case 771:
      ACCEPT_TOKEN(aux_sym_date_literal_token13);
      END_STATE();
    case 772:
      ACCEPT_TOKEN(aux_sym_date_literal_token14);
      END_STATE();
    case 773:
      ACCEPT_TOKEN(aux_sym_date_literal_token15);
      END_STATE();
    case 774:
      ACCEPT_TOKEN(aux_sym_date_literal_token16);
      END_STATE();
    case 775:
      ACCEPT_TOKEN(aux_sym_date_literal_token17);
      END_STATE();
    case 776:
      ACCEPT_TOKEN(aux_sym_date_literal_token18);
      END_STATE();
    case 777:
      ACCEPT_TOKEN(aux_sym_date_literal_token19);
      END_STATE();
    case 778:
      ACCEPT_TOKEN(aux_sym_date_literal_token20);
      END_STATE();
    case 779:
      ACCEPT_TOKEN(aux_sym_date_literal_token21);
      END_STATE();
    case 780:
      ACCEPT_TOKEN(aux_sym_date_literal_token22);
      END_STATE();
    case 781:
      ACCEPT_TOKEN(aux_sym_date_literal_token23);
      END_STATE();
    case 782:
      ACCEPT_TOKEN(aux_sym_date_literal_with_param_token1);
      END_STATE();
    case 783:
      ACCEPT_TOKEN(anon_sym_COLON);
      END_STATE();
    case 784:
      ACCEPT_TOKEN(sym_bound_apex_expression);
      END_STATE();
    case 785:
      ACCEPT_TOKEN(aux_sym_null_literal_token1);
      END_STATE();
    case 786:
      ACCEPT_TOKEN(aux_sym_null_literal_token1);
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(634);
      END_STATE();
    case 787:
      ACCEPT_TOKEN(sym_string_literal);
      END_STATE();
    case 788:
      ACCEPT_TOKEN(sym_int);
      if (lookahead == '-') ADVANCE(18);
      if (lookahead == '.') ADVANCE(643);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(795);
      END_STATE();
    case 789:
      ACCEPT_TOKEN(sym_int);
      if (lookahead == '.') ADVANCE(643);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(788);
      END_STATE();
    case 790:
      ACCEPT_TOKEN(sym_int);
      if (lookahead == '.') ADVANCE(643);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(789);
      END_STATE();
    case 791:
      ACCEPT_TOKEN(sym_int);
      if (lookahead == '.') ADVANCE(643);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(789);
      if (('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(649);
      END_STATE();
    case 792:
      ACCEPT_TOKEN(sym_int);
      if (lookahead == '.') ADVANCE(643);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(791);
      if (('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(655);
      END_STATE();
    case 793:
      ACCEPT_TOKEN(sym_int);
      if (lookahead == '.') ADVANCE(643);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(796);
      if (('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(655);
      END_STATE();
    case 794:
      ACCEPT_TOKEN(sym_int);
      if (lookahead == '.') ADVANCE(643);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(790);
      END_STATE();
    case 795:
      ACCEPT_TOKEN(sym_int);
      if (lookahead == '.') ADVANCE(643);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(795);
      END_STATE();
    case 796:
      ACCEPT_TOKEN(sym_int);
      if (lookahead == '.') ADVANCE(643);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(795);
      if (('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(649);
      END_STATE();
    case 797:
      ACCEPT_TOKEN(sym_decimal);
      if (lookahead == '.') ADVANCE(643);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(797);
      END_STATE();
    case 798:
      ACCEPT_TOKEN(sym_decimal);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(798);
      END_STATE();
    case 799:
      ACCEPT_TOKEN(sym_date);
      if (lookahead == 'T') ADVANCE(22);
      END_STATE();
    case 800:
      ACCEPT_TOKEN(sym_date_time);
      END_STATE();
    case 801:
      ACCEPT_TOKEN(sym_currency_literal);
      if (lookahead == '.') ADVANCE(650);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(801);
      END_STATE();
    case 802:
      ACCEPT_TOKEN(sym_currency_literal);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(802);
      END_STATE();
    case 803:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(836);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('B' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('b' <= lookahead && lookahead <= 'z')) ADVANCE(870);
      END_STATE();
    case 804:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(865);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('B' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('b' <= lookahead && lookahead <= 'z')) ADVANCE(870);
      END_STATE();
    case 805:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(864);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('B' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('b' <= lookahead && lookahead <= 'z')) ADVANCE(870);
      END_STATE();
    case 806:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'C' ||
          lookahead == 'c') ADVANCE(813);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(870);
      END_STATE();
    case 807:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'C' ||
          lookahead == 'c') ADVANCE(805);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(870);
      END_STATE();
    case 808:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'D' ||
          lookahead == 'd') ADVANCE(856);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(870);
      END_STATE();
    case 809:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'D' ||
          lookahead == 'd') ADVANCE(804);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(870);
      END_STATE();
    case 810:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'D' ||
          lookahead == 'd') ADVANCE(817);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(870);
      END_STATE();
    case 811:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(832);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(870);
      END_STATE();
    case 812:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(839);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(870);
      END_STATE();
    case 813:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(744);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(870);
      END_STATE();
    case 814:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(709);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(870);
      END_STATE();
    case 815:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(694);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(870);
      END_STATE();
    case 816:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(853);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(870);
      END_STATE();
    case 817:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(854);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(870);
      END_STATE();
    case 818:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(862);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(870);
      END_STATE();
    case 819:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(841);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(870);
      END_STATE();
    case 820:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'F' ||
          lookahead == 'f') ADVANCE(685);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(870);
      END_STATE();
    case 821:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'F' ||
          lookahead == 'f') ADVANCE(822);
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(810);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(870);
      END_STATE();
    case 822:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'F' ||
          lookahead == 'f') ADVANCE(858);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(870);
      END_STATE();
    case 823:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'G' ||
          lookahead == 'g') ADVANCE(671);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(870);
      END_STATE();
    case 824:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'H' ||
          lookahead == 'h') ADVANCE(716);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(870);
      END_STATE();
    case 825:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'H' ||
          lookahead == 'h') ADVANCE(816);
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(860);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(870);
      END_STATE();
    case 826:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(855);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(870);
      END_STATE();
    case 827:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(811);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(870);
      END_STATE();
    case 828:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(835);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(870);
      END_STATE();
    case 829:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(837);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(870);
      END_STATE();
    case 830:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(861);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(870);
      END_STATE();
    case 831:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(845);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(870);
      END_STATE();
    case 832:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'L' ||
          lookahead == 'l') ADVANCE(808);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(870);
      END_STATE();
    case 833:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'L' ||
          lookahead == 'l') ADVANCE(842);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(870);
      END_STATE();
    case 834:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'M' ||
          lookahead == 'm') ADVANCE(700);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(870);
      END_STATE();
    case 835:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'M' ||
          lookahead == 'm') ADVANCE(830);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(870);
      END_STATE();
    case 836:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(806);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(870);
      END_STATE();
    case 837:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(823);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(870);
      END_STATE();
    case 838:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(742);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(870);
      END_STATE();
    case 839:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(820);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(870);
      END_STATE();
    case 840:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(866);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(870);
      END_STATE();
    case 841:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(833);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(870);
      END_STATE();
    case 842:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(807);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(870);
      END_STATE();
    case 843:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(850);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(870);
      END_STATE();
    case 844:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(834);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(870);
      END_STATE();
    case 845:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(838);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(870);
      END_STATE();
    case 846:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(863);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(870);
      END_STATE();
    case 847:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'P' ||
          lookahead == 'p') ADVANCE(868);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(870);
      END_STATE();
    case 848:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'P' ||
          lookahead == 'p') ADVANCE(812);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(870);
      END_STATE();
    case 849:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'P' ||
          lookahead == 'p') ADVANCE(809);
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(829);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(870);
      END_STATE();
    case 850:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(692);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(870);
      END_STATE();
    case 851:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(840);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(870);
      END_STATE();
    case 852:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(844);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(870);
      END_STATE();
    case 853:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(814);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(870);
      END_STATE();
    case 854:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(869);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(870);
      END_STATE();
    case 855:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(859);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(870);
      END_STATE();
    case 856:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(704);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(870);
      END_STATE();
    case 857:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(702);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(870);
      END_STATE();
    case 858:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(818);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(870);
      END_STATE();
    case 859:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(803);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(870);
      END_STATE();
    case 860:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(824);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(870);
      END_STATE();
    case 861:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(731);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(870);
      END_STATE();
    case 862:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(733);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(870);
      END_STATE();
    case 863:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(714);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(870);
      END_STATE();
    case 864:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(831);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(870);
      END_STATE();
    case 865:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(815);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(870);
      END_STATE();
    case 866:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'U' ||
          lookahead == 'u') ADVANCE(847);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(870);
      END_STATE();
    case 867:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'Y' ||
          lookahead == 'y') ADVANCE(848);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(870);
      END_STATE();
    case 868:
      ACCEPT_TOKEN(sym_identifier);
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') ADVANCE(124);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(870);
      END_STATE();
    case 869:
      ACCEPT_TOKEN(sym_identifier);
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') ADVANCE(125);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(870);
      END_STATE();
    case 870:
      ACCEPT_TOKEN(sym_identifier);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(870);
      END_STATE();
    default:
      return false;
  }
}

static const TSLexMode ts_lex_modes[STATE_COUNT] = {
  [0] = {.lex_state = 0},
  [1] = {.lex_state = 0},
  [2] = {.lex_state = 1},
  [3] = {.lex_state = 1},
  [4] = {.lex_state = 1},
  [5] = {.lex_state = 1},
  [6] = {.lex_state = 1},
  [7] = {.lex_state = 1},
  [8] = {.lex_state = 1},
  [9] = {.lex_state = 656},
  [10] = {.lex_state = 656},
  [11] = {.lex_state = 656},
  [12] = {.lex_state = 656},
  [13] = {.lex_state = 656},
  [14] = {.lex_state = 656},
  [15] = {.lex_state = 656},
  [16] = {.lex_state = 656},
  [17] = {.lex_state = 656},
  [18] = {.lex_state = 0},
  [19] = {.lex_state = 0},
  [20] = {.lex_state = 656},
  [21] = {.lex_state = 656},
  [22] = {.lex_state = 6},
  [23] = {.lex_state = 0},
  [24] = {.lex_state = 0},
  [25] = {.lex_state = 657},
  [26] = {.lex_state = 657},
  [27] = {.lex_state = 657},
  [28] = {.lex_state = 657},
  [29] = {.lex_state = 7},
  [30] = {.lex_state = 7},
  [31] = {.lex_state = 657},
  [32] = {.lex_state = 7},
  [33] = {.lex_state = 6},
  [34] = {.lex_state = 7},
  [35] = {.lex_state = 0},
  [36] = {.lex_state = 0},
  [37] = {.lex_state = 0},
  [38] = {.lex_state = 657},
  [39] = {.lex_state = 0},
  [40] = {.lex_state = 657},
  [41] = {.lex_state = 0},
  [42] = {.lex_state = 0},
  [43] = {.lex_state = 0},
  [44] = {.lex_state = 0},
  [45] = {.lex_state = 0},
  [46] = {.lex_state = 0},
  [47] = {.lex_state = 0},
  [48] = {.lex_state = 0},
  [49] = {.lex_state = 0},
  [50] = {.lex_state = 0},
  [51] = {.lex_state = 656},
  [52] = {.lex_state = 0},
  [53] = {.lex_state = 0},
  [54] = {.lex_state = 0},
  [55] = {.lex_state = 0},
  [56] = {.lex_state = 0},
  [57] = {.lex_state = 0},
  [58] = {.lex_state = 0},
  [59] = {.lex_state = 0},
  [60] = {.lex_state = 0},
  [61] = {.lex_state = 0},
  [62] = {.lex_state = 0},
  [63] = {.lex_state = 0},
  [64] = {.lex_state = 0},
  [65] = {.lex_state = 0},
  [66] = {.lex_state = 0},
  [67] = {.lex_state = 0},
  [68] = {.lex_state = 0},
  [69] = {.lex_state = 0},
  [70] = {.lex_state = 0},
  [71] = {.lex_state = 0},
  [72] = {.lex_state = 0},
  [73] = {.lex_state = 0},
  [74] = {.lex_state = 0},
  [75] = {.lex_state = 0},
  [76] = {.lex_state = 0},
  [77] = {.lex_state = 0},
  [78] = {.lex_state = 0},
  [79] = {.lex_state = 0},
  [80] = {.lex_state = 0},
  [81] = {.lex_state = 0},
  [82] = {.lex_state = 0},
  [83] = {.lex_state = 0},
  [84] = {.lex_state = 0},
  [85] = {.lex_state = 0},
  [86] = {.lex_state = 0},
  [87] = {.lex_state = 0},
  [88] = {.lex_state = 4},
  [89] = {.lex_state = 4},
  [90] = {.lex_state = 0},
  [91] = {.lex_state = 0},
  [92] = {.lex_state = 0},
  [93] = {.lex_state = 0},
  [94] = {.lex_state = 0},
  [95] = {.lex_state = 0},
  [96] = {.lex_state = 0},
  [97] = {.lex_state = 0},
  [98] = {.lex_state = 0},
  [99] = {.lex_state = 0},
  [100] = {.lex_state = 0},
  [101] = {.lex_state = 0},
  [102] = {.lex_state = 0},
  [103] = {.lex_state = 0},
  [104] = {.lex_state = 0},
  [105] = {.lex_state = 0},
  [106] = {.lex_state = 0},
  [107] = {.lex_state = 0},
  [108] = {.lex_state = 4},
  [109] = {.lex_state = 0},
  [110] = {.lex_state = 0},
  [111] = {.lex_state = 0},
  [112] = {.lex_state = 656},
  [113] = {.lex_state = 4},
  [114] = {.lex_state = 0},
  [115] = {.lex_state = 0},
  [116] = {.lex_state = 0},
  [117] = {.lex_state = 0},
  [118] = {.lex_state = 0},
  [119] = {.lex_state = 0},
  [120] = {.lex_state = 0},
  [121] = {.lex_state = 0},
  [122] = {.lex_state = 656},
  [123] = {.lex_state = 0},
  [124] = {.lex_state = 0},
  [125] = {.lex_state = 0},
  [126] = {.lex_state = 0},
  [127] = {.lex_state = 0},
  [128] = {.lex_state = 0},
  [129] = {.lex_state = 0},
  [130] = {.lex_state = 0},
  [131] = {.lex_state = 0},
  [132] = {.lex_state = 0},
  [133] = {.lex_state = 0},
  [134] = {.lex_state = 0},
  [135] = {.lex_state = 0},
  [136] = {.lex_state = 0},
  [137] = {.lex_state = 0},
  [138] = {.lex_state = 0},
  [139] = {.lex_state = 0},
  [140] = {.lex_state = 0},
  [141] = {.lex_state = 0},
  [142] = {.lex_state = 0},
  [143] = {.lex_state = 0},
  [144] = {.lex_state = 0},
  [145] = {.lex_state = 0},
  [146] = {.lex_state = 0},
  [147] = {.lex_state = 0},
  [148] = {.lex_state = 0},
  [149] = {.lex_state = 0},
  [150] = {.lex_state = 0},
  [151] = {.lex_state = 0},
  [152] = {.lex_state = 0},
  [153] = {.lex_state = 4},
  [154] = {.lex_state = 0},
  [155] = {.lex_state = 0},
  [156] = {.lex_state = 0},
  [157] = {.lex_state = 0},
  [158] = {.lex_state = 0},
  [159] = {.lex_state = 4},
  [160] = {.lex_state = 0},
  [161] = {.lex_state = 0},
  [162] = {.lex_state = 0},
  [163] = {.lex_state = 0},
  [164] = {.lex_state = 0},
  [165] = {.lex_state = 0},
  [166] = {.lex_state = 0},
  [167] = {.lex_state = 0},
  [168] = {.lex_state = 0},
  [169] = {.lex_state = 0},
  [170] = {.lex_state = 4},
  [171] = {.lex_state = 4},
  [172] = {.lex_state = 0},
  [173] = {.lex_state = 0},
  [174] = {.lex_state = 0},
  [175] = {.lex_state = 0},
  [176] = {.lex_state = 0},
  [177] = {.lex_state = 0},
  [178] = {.lex_state = 0},
  [179] = {.lex_state = 4},
  [180] = {.lex_state = 0},
  [181] = {.lex_state = 4},
  [182] = {.lex_state = 4},
  [183] = {.lex_state = 3},
  [184] = {.lex_state = 0},
  [185] = {.lex_state = 0},
  [186] = {.lex_state = 0},
  [187] = {.lex_state = 0},
  [188] = {.lex_state = 0},
  [189] = {.lex_state = 0},
  [190] = {.lex_state = 0},
  [191] = {.lex_state = 0},
  [192] = {.lex_state = 0},
  [193] = {.lex_state = 3},
  [194] = {.lex_state = 0},
  [195] = {.lex_state = 3},
  [196] = {.lex_state = 0},
  [197] = {.lex_state = 0},
  [198] = {.lex_state = 0},
  [199] = {.lex_state = 0},
  [200] = {.lex_state = 0},
  [201] = {.lex_state = 0},
  [202] = {.lex_state = 0},
  [203] = {.lex_state = 0},
  [204] = {.lex_state = 0},
  [205] = {.lex_state = 0},
  [206] = {.lex_state = 0},
  [207] = {.lex_state = 0},
  [208] = {.lex_state = 0},
  [209] = {.lex_state = 0},
  [210] = {.lex_state = 0},
  [211] = {.lex_state = 0},
  [212] = {.lex_state = 0},
  [213] = {.lex_state = 0},
  [214] = {.lex_state = 0},
  [215] = {.lex_state = 0},
  [216] = {.lex_state = 0},
  [217] = {.lex_state = 9},
  [218] = {.lex_state = 0},
  [219] = {.lex_state = 0},
  [220] = {.lex_state = 0},
  [221] = {.lex_state = 0},
  [222] = {.lex_state = 0},
  [223] = {.lex_state = 0},
  [224] = {.lex_state = 0},
  [225] = {.lex_state = 0},
  [226] = {.lex_state = 0},
  [227] = {.lex_state = 0},
  [228] = {.lex_state = 0},
  [229] = {.lex_state = 0},
  [230] = {.lex_state = 0},
  [231] = {.lex_state = 0},
  [232] = {.lex_state = 0},
  [233] = {.lex_state = 0},
  [234] = {.lex_state = 0},
  [235] = {.lex_state = 0},
  [236] = {.lex_state = 0},
  [237] = {.lex_state = 0},
  [238] = {.lex_state = 0},
  [239] = {.lex_state = 0},
  [240] = {.lex_state = 0},
  [241] = {.lex_state = 0},
  [242] = {.lex_state = 0},
  [243] = {.lex_state = 0},
  [244] = {.lex_state = 0},
  [245] = {.lex_state = 0},
  [246] = {.lex_state = 0},
  [247] = {.lex_state = 0},
  [248] = {.lex_state = 0},
  [249] = {.lex_state = 9},
  [250] = {.lex_state = 0},
  [251] = {.lex_state = 0},
  [252] = {.lex_state = 0},
  [253] = {.lex_state = 0},
  [254] = {.lex_state = 0},
  [255] = {.lex_state = 4},
  [256] = {.lex_state = 0},
  [257] = {.lex_state = 0},
  [258] = {.lex_state = 0},
  [259] = {.lex_state = 3},
  [260] = {.lex_state = 0},
  [261] = {.lex_state = 0},
  [262] = {.lex_state = 0},
  [263] = {.lex_state = 8},
  [264] = {.lex_state = 0},
  [265] = {.lex_state = 0},
  [266] = {.lex_state = 0},
  [267] = {.lex_state = 0},
  [268] = {.lex_state = 0},
  [269] = {.lex_state = 0},
  [270] = {.lex_state = 0},
  [271] = {.lex_state = 0},
  [272] = {.lex_state = 0},
  [273] = {.lex_state = 0},
  [274] = {.lex_state = 0},
  [275] = {.lex_state = 0},
  [276] = {.lex_state = 0},
  [277] = {.lex_state = 0},
  [278] = {.lex_state = 0},
  [279] = {.lex_state = 0},
  [280] = {.lex_state = 0},
  [281] = {.lex_state = 0},
  [282] = {.lex_state = 0},
  [283] = {.lex_state = 0},
  [284] = {.lex_state = 0},
  [285] = {.lex_state = 0},
  [286] = {.lex_state = 0},
  [287] = {.lex_state = 0},
  [288] = {.lex_state = 0},
  [289] = {.lex_state = 0},
  [290] = {.lex_state = 0},
  [291] = {.lex_state = 0},
  [292] = {.lex_state = 1},
  [293] = {.lex_state = 0},
  [294] = {.lex_state = 0},
  [295] = {.lex_state = 0},
  [296] = {.lex_state = 0},
  [297] = {.lex_state = 0},
  [298] = {.lex_state = 5},
  [299] = {.lex_state = 0},
  [300] = {.lex_state = 0},
  [301] = {.lex_state = 0},
  [302] = {.lex_state = 0},
  [303] = {.lex_state = 0},
  [304] = {.lex_state = 0},
  [305] = {.lex_state = 0},
  [306] = {.lex_state = 0},
  [307] = {.lex_state = 0},
  [308] = {.lex_state = 0},
  [309] = {.lex_state = 0},
  [310] = {.lex_state = 0},
  [311] = {.lex_state = 0},
  [312] = {.lex_state = 0},
  [313] = {.lex_state = 0},
  [314] = {.lex_state = 0},
  [315] = {.lex_state = 0},
  [316] = {.lex_state = 0},
  [317] = {.lex_state = 0},
  [318] = {.lex_state = 0},
  [319] = {.lex_state = 0},
  [320] = {.lex_state = 0},
  [321] = {.lex_state = 0},
  [322] = {.lex_state = 0},
  [323] = {.lex_state = 0},
  [324] = {.lex_state = 0},
  [325] = {.lex_state = 0},
  [326] = {.lex_state = 0},
  [327] = {.lex_state = 3},
  [328] = {.lex_state = 0},
  [329] = {.lex_state = 0},
  [330] = {.lex_state = 3},
  [331] = {.lex_state = 0},
  [332] = {.lex_state = 3},
  [333] = {.lex_state = 0},
  [334] = {.lex_state = 0},
  [335] = {.lex_state = 0},
  [336] = {.lex_state = 0},
  [337] = {.lex_state = 0},
  [338] = {.lex_state = 0},
  [339] = {.lex_state = 0},
  [340] = {.lex_state = 0},
  [341] = {.lex_state = 0},
  [342] = {.lex_state = 0},
  [343] = {.lex_state = 0},
  [344] = {.lex_state = 0},
  [345] = {.lex_state = 0},
  [346] = {.lex_state = 1},
  [347] = {.lex_state = 0},
  [348] = {.lex_state = 0},
  [349] = {.lex_state = 0},
  [350] = {.lex_state = 0},
  [351] = {.lex_state = 0},
  [352] = {.lex_state = 0},
  [353] = {.lex_state = 5},
  [354] = {.lex_state = 0},
  [355] = {.lex_state = 0},
  [356] = {.lex_state = 0},
  [357] = {.lex_state = 0},
  [358] = {.lex_state = 0},
  [359] = {.lex_state = 0},
  [360] = {.lex_state = 0},
  [361] = {.lex_state = 0},
  [362] = {.lex_state = 0},
  [363] = {.lex_state = 0},
  [364] = {.lex_state = 0},
  [365] = {.lex_state = 0},
  [366] = {.lex_state = 0},
  [367] = {.lex_state = 0},
  [368] = {.lex_state = 0},
  [369] = {.lex_state = 0},
  [370] = {.lex_state = 0},
  [371] = {.lex_state = 0},
  [372] = {.lex_state = 0},
  [373] = {.lex_state = 0},
  [374] = {.lex_state = 0},
  [375] = {.lex_state = 0},
  [376] = {.lex_state = 0},
  [377] = {.lex_state = 0},
  [378] = {.lex_state = 0},
  [379] = {.lex_state = 0},
  [380] = {.lex_state = 0},
  [381] = {.lex_state = 0},
  [382] = {.lex_state = 0},
  [383] = {.lex_state = 0},
  [384] = {.lex_state = 0},
  [385] = {.lex_state = 0},
  [386] = {.lex_state = 0},
  [387] = {.lex_state = 0},
  [388] = {.lex_state = 0},
  [389] = {.lex_state = 0},
  [390] = {.lex_state = 0},
  [391] = {.lex_state = 0},
  [392] = {.lex_state = 0},
  [393] = {.lex_state = 0},
  [394] = {.lex_state = 3},
  [395] = {.lex_state = 0},
  [396] = {.lex_state = 0},
  [397] = {.lex_state = 0},
  [398] = {.lex_state = 0},
  [399] = {.lex_state = 0},
  [400] = {.lex_state = 0},
  [401] = {.lex_state = 0},
  [402] = {.lex_state = 0},
  [403] = {.lex_state = 0},
  [404] = {.lex_state = 0},
  [405] = {.lex_state = 0},
  [406] = {.lex_state = 0},
  [407] = {.lex_state = 0},
  [408] = {.lex_state = 3},
  [409] = {.lex_state = 0},
  [410] = {.lex_state = 0},
  [411] = {.lex_state = 0},
  [412] = {.lex_state = 0},
  [413] = {.lex_state = 0},
  [414] = {.lex_state = 0},
  [415] = {.lex_state = 0},
  [416] = {.lex_state = 0},
  [417] = {.lex_state = 5},
  [418] = {.lex_state = 5},
  [419] = {.lex_state = 0},
  [420] = {.lex_state = 8},
  [421] = {.lex_state = 0},
  [422] = {.lex_state = 3},
  [423] = {.lex_state = 0},
  [424] = {.lex_state = 0},
  [425] = {.lex_state = 0},
  [426] = {.lex_state = 0},
  [427] = {.lex_state = 0},
  [428] = {.lex_state = 0},
  [429] = {.lex_state = 656},
  [430] = {.lex_state = 0},
  [431] = {.lex_state = 5},
  [432] = {.lex_state = 0},
  [433] = {.lex_state = 0},
  [434] = {.lex_state = 0},
  [435] = {.lex_state = 0},
  [436] = {.lex_state = 8},
  [437] = {.lex_state = 0},
  [438] = {.lex_state = 0},
  [439] = {.lex_state = 0},
  [440] = {.lex_state = 0},
  [441] = {.lex_state = 8},
  [442] = {.lex_state = 8},
  [443] = {.lex_state = 0},
  [444] = {.lex_state = 8},
  [445] = {.lex_state = 8},
  [446] = {.lex_state = 8},
  [447] = {.lex_state = 8},
  [448] = {.lex_state = 8},
  [449] = {.lex_state = 8},
  [450] = {.lex_state = 8},
  [451] = {.lex_state = 8},
  [452] = {.lex_state = 8},
  [453] = {.lex_state = 8},
  [454] = {.lex_state = 8},
  [455] = {.lex_state = 0},
  [456] = {.lex_state = 0},
  [457] = {.lex_state = 656},
  [458] = {.lex_state = 0},
  [459] = {.lex_state = 0},
  [460] = {.lex_state = 0},
  [461] = {.lex_state = 5},
  [462] = {.lex_state = 0},
  [463] = {.lex_state = 0},
  [464] = {.lex_state = 0},
  [465] = {.lex_state = 0},
  [466] = {.lex_state = 8},
  [467] = {.lex_state = 0},
  [468] = {.lex_state = 0},
  [469] = {.lex_state = 0},
  [470] = {.lex_state = 0},
  [471] = {.lex_state = 0},
  [472] = {.lex_state = 0},
  [473] = {.lex_state = 0},
  [474] = {.lex_state = 0},
  [475] = {.lex_state = 0},
  [476] = {.lex_state = 0},
  [477] = {.lex_state = 0},
  [478] = {.lex_state = 0},
  [479] = {.lex_state = 0},
  [480] = {.lex_state = 0},
  [481] = {.lex_state = 0},
  [482] = {.lex_state = 0},
  [483] = {.lex_state = 0},
  [484] = {.lex_state = 0},
  [485] = {.lex_state = 0},
  [486] = {.lex_state = 0},
  [487] = {.lex_state = 0},
  [488] = {.lex_state = 0},
  [489] = {.lex_state = 0},
  [490] = {.lex_state = 0},
  [491] = {.lex_state = 0},
  [492] = {.lex_state = 0},
  [493] = {.lex_state = 0},
  [494] = {.lex_state = 0},
  [495] = {.lex_state = 0},
  [496] = {.lex_state = 0},
  [497] = {.lex_state = 0},
  [498] = {.lex_state = 0},
  [499] = {.lex_state = 0},
  [500] = {.lex_state = 0},
  [501] = {.lex_state = 0},
  [502] = {.lex_state = 0},
  [503] = {.lex_state = 0},
  [504] = {.lex_state = 0},
  [505] = {.lex_state = 0},
  [506] = {.lex_state = 0},
  [507] = {.lex_state = 0},
  [508] = {.lex_state = 0},
  [509] = {.lex_state = 0},
  [510] = {.lex_state = 0},
  [511] = {.lex_state = 0},
  [512] = {.lex_state = 0},
  [513] = {.lex_state = 0},
  [514] = {.lex_state = 0},
  [515] = {.lex_state = 0},
  [516] = {.lex_state = 0},
  [517] = {.lex_state = 0},
  [518] = {.lex_state = 0},
  [519] = {.lex_state = 0},
  [520] = {.lex_state = 0},
  [521] = {.lex_state = 0},
  [522] = {.lex_state = 0},
  [523] = {.lex_state = 0},
  [524] = {.lex_state = 0},
  [525] = {.lex_state = 0},
  [526] = {.lex_state = 0},
  [527] = {.lex_state = 0},
  [528] = {.lex_state = 0},
  [529] = {.lex_state = 0},
  [530] = {.lex_state = 0},
  [531] = {.lex_state = 0},
  [532] = {.lex_state = 0},
  [533] = {.lex_state = 0},
  [534] = {.lex_state = 0},
  [535] = {.lex_state = 0},
  [536] = {.lex_state = 0},
  [537] = {.lex_state = 0},
  [538] = {.lex_state = 0},
  [539] = {.lex_state = 0},
  [540] = {.lex_state = 0},
  [541] = {.lex_state = 0},
  [542] = {.lex_state = 0},
  [543] = {.lex_state = 0},
  [544] = {.lex_state = 0},
  [545] = {.lex_state = 0},
  [546] = {.lex_state = 0},
  [547] = {.lex_state = 0},
  [548] = {.lex_state = 0},
  [549] = {.lex_state = 0},
  [550] = {.lex_state = 0},
  [551] = {.lex_state = 0},
  [552] = {.lex_state = 0},
  [553] = {.lex_state = 0},
  [554] = {.lex_state = 0},
  [555] = {.lex_state = 5},
  [556] = {.lex_state = 0},
  [557] = {.lex_state = 0},
  [558] = {.lex_state = 0},
  [559] = {.lex_state = 0},
  [560] = {.lex_state = 0},
  [561] = {.lex_state = 0},
  [562] = {.lex_state = 0},
  [563] = {.lex_state = 0},
  [564] = {.lex_state = 0},
  [565] = {.lex_state = 0},
  [566] = {.lex_state = 0},
  [567] = {.lex_state = 0},
  [568] = {.lex_state = 0},
  [569] = {.lex_state = 0},
  [570] = {.lex_state = 0},
  [571] = {.lex_state = 0},
  [572] = {.lex_state = 0},
  [573] = {.lex_state = 0},
  [574] = {.lex_state = 0},
  [575] = {.lex_state = 0},
  [576] = {.lex_state = 0},
  [577] = {.lex_state = 0},
  [578] = {.lex_state = 0},
  [579] = {.lex_state = 0},
  [580] = {.lex_state = 0},
  [581] = {.lex_state = 0},
  [582] = {.lex_state = 0},
  [583] = {.lex_state = 0},
  [584] = {.lex_state = 0},
  [585] = {.lex_state = 0},
  [586] = {.lex_state = 0},
  [587] = {.lex_state = 5},
  [588] = {.lex_state = 0},
  [589] = {.lex_state = 0},
  [590] = {.lex_state = 0},
  [591] = {.lex_state = 0},
  [592] = {.lex_state = 0},
  [593] = {.lex_state = 0},
  [594] = {.lex_state = 0},
  [595] = {.lex_state = 0},
  [596] = {.lex_state = 0},
  [597] = {.lex_state = 0},
  [598] = {.lex_state = 0},
  [599] = {.lex_state = 0},
  [600] = {.lex_state = 0},
  [601] = {.lex_state = 0},
  [602] = {.lex_state = 0},
  [603] = {.lex_state = 0},
  [604] = {.lex_state = 0},
  [605] = {.lex_state = 0},
  [606] = {.lex_state = 5},
  [607] = {.lex_state = 0},
  [608] = {.lex_state = 0},
  [609] = {.lex_state = 0},
  [610] = {.lex_state = 0},
  [611] = {.lex_state = 0},
  [612] = {.lex_state = 0},
  [613] = {.lex_state = 0},
  [614] = {.lex_state = 0},
  [615] = {.lex_state = 0},
  [616] = {.lex_state = 0},
  [617] = {.lex_state = 0},
  [618] = {.lex_state = 0},
  [619] = {.lex_state = 0},
  [620] = {.lex_state = 0},
  [621] = {.lex_state = 0},
  [622] = {.lex_state = 0},
  [623] = {.lex_state = 0},
  [624] = {.lex_state = 0},
  [625] = {.lex_state = 0},
  [626] = {.lex_state = 0},
  [627] = {.lex_state = 0},
  [628] = {.lex_state = 0},
  [629] = {.lex_state = 0},
  [630] = {.lex_state = 0},
  [631] = {.lex_state = 0},
  [632] = {.lex_state = 0},
  [633] = {.lex_state = 0},
  [634] = {.lex_state = 0},
  [635] = {.lex_state = 0},
  [636] = {.lex_state = 0},
  [637] = {.lex_state = 0},
  [638] = {.lex_state = 0},
  [639] = {.lex_state = 0},
  [640] = {.lex_state = 0},
  [641] = {.lex_state = 0},
  [642] = {.lex_state = 0},
  [643] = {.lex_state = 0},
  [644] = {.lex_state = 0},
  [645] = {.lex_state = 0},
  [646] = {.lex_state = 0},
  [647] = {.lex_state = 0},
  [648] = {.lex_state = 0},
  [649] = {.lex_state = 0},
  [650] = {.lex_state = 0},
  [651] = {.lex_state = 0},
  [652] = {.lex_state = 0},
  [653] = {.lex_state = 0},
  [654] = {.lex_state = 5},
  [655] = {.lex_state = 0},
  [656] = {.lex_state = 0},
  [657] = {.lex_state = 0},
  [658] = {.lex_state = 0},
  [659] = {.lex_state = 0},
  [660] = {.lex_state = 0},
  [661] = {.lex_state = 0},
  [662] = {.lex_state = 0},
  [663] = {.lex_state = 0},
  [664] = {.lex_state = 0},
  [665] = {.lex_state = 0},
  [666] = {.lex_state = 0},
  [667] = {.lex_state = 0},
  [668] = {.lex_state = 0},
  [669] = {.lex_state = 0},
  [670] = {.lex_state = 656},
  [671] = {.lex_state = 0},
  [672] = {.lex_state = 0},
  [673] = {.lex_state = 0},
  [674] = {.lex_state = 0},
  [675] = {.lex_state = 0},
  [676] = {.lex_state = 0},
  [677] = {.lex_state = 0},
  [678] = {.lex_state = 0},
  [679] = {.lex_state = 0},
  [680] = {.lex_state = 0},
  [681] = {.lex_state = 0},
  [682] = {.lex_state = 0},
  [683] = {.lex_state = 0},
  [684] = {.lex_state = 0},
  [685] = {.lex_state = 0},
  [686] = {.lex_state = 0},
  [687] = {.lex_state = 0},
  [688] = {.lex_state = 0},
  [689] = {.lex_state = 0},
  [690] = {.lex_state = 0},
  [691] = {.lex_state = 0},
  [692] = {.lex_state = 0},
  [693] = {.lex_state = 0},
  [694] = {.lex_state = 0},
  [695] = {.lex_state = 0},
  [696] = {.lex_state = 0},
  [697] = {.lex_state = 0},
  [698] = {.lex_state = 0},
  [699] = {.lex_state = 0},
  [700] = {.lex_state = 0},
  [701] = {.lex_state = 0},
  [702] = {.lex_state = 0},
  [703] = {.lex_state = 0},
  [704] = {.lex_state = 0},
  [705] = {.lex_state = 0},
  [706] = {.lex_state = 0},
  [707] = {.lex_state = 0},
  [708] = {.lex_state = 0},
  [709] = {.lex_state = 0},
  [710] = {.lex_state = 0},
  [711] = {.lex_state = 0},
  [712] = {.lex_state = 0},
  [713] = {.lex_state = 0},
  [714] = {.lex_state = 0},
  [715] = {.lex_state = 0},
  [716] = {.lex_state = 0},
  [717] = {.lex_state = 0},
  [718] = {.lex_state = 0},
  [719] = {.lex_state = 0},
  [720] = {.lex_state = 0},
  [721] = {.lex_state = 0},
  [722] = {.lex_state = 0},
  [723] = {.lex_state = 0},
  [724] = {.lex_state = 0},
  [725] = {.lex_state = 0},
  [726] = {.lex_state = 0},
  [727] = {.lex_state = 0},
  [728] = {.lex_state = 0},
  [729] = {.lex_state = 0},
  [730] = {.lex_state = 5},
  [731] = {.lex_state = 0},
  [732] = {.lex_state = 0},
  [733] = {.lex_state = 0},
  [734] = {.lex_state = 5},
  [735] = {.lex_state = 0},
  [736] = {.lex_state = 8},
  [737] = {.lex_state = 0},
  [738] = {.lex_state = 0},
  [739] = {.lex_state = 5},
  [740] = {.lex_state = 0},
  [741] = {.lex_state = 0},
  [742] = {.lex_state = 5},
  [743] = {.lex_state = 0},
  [744] = {.lex_state = 656},
  [745] = {.lex_state = 0},
  [746] = {.lex_state = 0},
  [747] = {.lex_state = 0},
  [748] = {.lex_state = 0},
  [749] = {.lex_state = 0},
  [750] = {.lex_state = 0},
  [751] = {.lex_state = 0},
  [752] = {.lex_state = 5},
  [753] = {.lex_state = 0},
  [754] = {.lex_state = 0},
  [755] = {.lex_state = 660},
  [756] = {.lex_state = 0},
  [757] = {.lex_state = 0},
  [758] = {.lex_state = 0},
  [759] = {.lex_state = 0},
  [760] = {.lex_state = 0},
  [761] = {.lex_state = 0},
  [762] = {.lex_state = 0},
  [763] = {.lex_state = 0},
  [764] = {.lex_state = 5},
  [765] = {.lex_state = 0},
  [766] = {.lex_state = 0},
  [767] = {.lex_state = 0},
  [768] = {.lex_state = 0},
  [769] = {.lex_state = 0},
  [770] = {.lex_state = 0},
  [771] = {.lex_state = 5},
  [772] = {.lex_state = 0},
  [773] = {.lex_state = 660},
  [774] = {.lex_state = 0},
  [775] = {.lex_state = 0},
  [776] = {.lex_state = 0},
  [777] = {.lex_state = 0},
  [778] = {.lex_state = 0},
  [779] = {.lex_state = 0},
  [780] = {.lex_state = 0},
  [781] = {.lex_state = 0},
  [782] = {.lex_state = 0},
  [783] = {.lex_state = 0},
  [784] = {.lex_state = 0},
  [785] = {.lex_state = 0},
  [786] = {.lex_state = 0},
  [787] = {.lex_state = 5},
  [788] = {.lex_state = 0},
  [789] = {.lex_state = 5},
  [790] = {.lex_state = 0},
  [791] = {.lex_state = 0},
  [792] = {.lex_state = 5},
  [793] = {.lex_state = 0},
  [794] = {.lex_state = 0},
  [795] = {.lex_state = 0},
  [796] = {.lex_state = 0},
  [797] = {.lex_state = 0},
  [798] = {.lex_state = 0},
  [799] = {.lex_state = 0},
  [800] = {.lex_state = 5},
  [801] = {.lex_state = 0},
  [802] = {.lex_state = 0},
  [803] = {.lex_state = 0},
  [804] = {.lex_state = 0},
  [805] = {.lex_state = 0},
  [806] = {.lex_state = 0},
  [807] = {.lex_state = 0},
  [808] = {.lex_state = 5},
  [809] = {.lex_state = 0},
  [810] = {.lex_state = 5},
  [811] = {(TSStateId)(-1)},
};

static const uint16_t ts_parse_table[LARGE_STATE_COUNT][SYMBOL_COUNT] = {
  [0] = {
    [sym_formatting_comment] = STATE(0),
    [ts_builtin_sym_end] = ACTIONS(1),
    [anon_sym_SLASH_SLASH] = ACTIONS(1),
    [anon_sym_SLASH_SLASH_SLASH] = ACTIONS(3),
    [anon_sym_LPAREN] = ACTIONS(1),
    [anon_sym_RPAREN] = ACTIONS(1),
    [aux_sym_select_clause_token1] = ACTIONS(1),
    [anon_sym_COMMA] = ACTIONS(1),
    [aux_sym_using_clause_token1] = ACTIONS(1),
    [aux_sym_using_scope_clause_token1] = ACTIONS(1),
    [aux_sym_using_scope_type_token1] = ACTIONS(1),
    [aux_sym_using_scope_type_token2] = ACTIONS(1),
    [aux_sym_using_scope_type_token3] = ACTIONS(1),
    [aux_sym_using_scope_type_token4] = ACTIONS(1),
    [aux_sym_using_scope_type_token5] = ACTIONS(1),
    [aux_sym_using_scope_type_token6] = ACTIONS(1),
    [aux_sym_using_scope_type_token7] = ACTIONS(1),
    [aux_sym_using_lookup_clause_token1] = ACTIONS(1),
    [aux_sym_using_listview_clause_token1] = ACTIONS(1),
    [anon_sym_EQ] = ACTIONS(1),
    [aux_sym_using_lookup_bind_clause_token1] = ACTIONS(1),
    [aux_sym_type_of_clause_token1] = ACTIONS(1),
    [aux_sym_type_of_clause_token2] = ACTIONS(1),
    [aux_sym_when_expression_token1] = ACTIONS(1),
    [aux_sym_when_expression_token2] = ACTIONS(1),
    [aux_sym_else_expression_token1] = ACTIONS(1),
    [aux_sym_group_by_clause_token1] = ACTIONS(1),
    [aux_sym_for_clause_token1] = ACTIONS(1),
    [aux_sym_for_type_token1] = ACTIONS(1),
    [aux_sym_for_type_token2] = ACTIONS(1),
    [aux_sym_for_type_token3] = ACTIONS(1),
    [aux_sym_having_clause_token1] = ACTIONS(1),
    [aux_sym_from_clause_token1] = ACTIONS(1),
    [aux_sym_storage_alias_token1] = ACTIONS(1),
    [aux_sym_fields_expression_token1] = ACTIONS(1),
    [aux_sym_fields_type_token1] = ACTIONS(1),
    [aux_sym_fields_type_token2] = ACTIONS(1),
    [aux_sym_fields_type_token3] = ACTIONS(1),
    [aux_sym_where_clause_token1] = ACTIONS(1),
    [aux_sym_and_expression_token1] = ACTIONS(1),
    [aux_sym_or_expression_token1] = ACTIONS(1),
    [aux_sym_not_expression_token1] = ACTIONS(1),
    [aux_sym_soql_with_clause_token1] = ACTIONS(1),
    [aux_sym_soql_with_type_token1] = ACTIONS(1),
    [aux_sym_soql_with_type_token2] = ACTIONS(1),
    [aux_sym_soql_with_type_token3] = ACTIONS(1),
    [aux_sym_with_user_id_type_token1] = ACTIONS(1),
    [aux_sym_with_record_visibility_expression_token1] = ACTIONS(1),
    [aux_sym_with_record_visibility_param_token1] = ACTIONS(1),
    [aux_sym_with_record_visibility_param_token2] = ACTIONS(1),
    [aux_sym_with_record_visibility_param_token3] = ACTIONS(1),
    [aux_sym_with_data_cat_expression_token1] = ACTIONS(1),
    [aux_sym_with_data_cat_filter_type_token1] = ACTIONS(1),
    [aux_sym_with_data_cat_filter_type_token2] = ACTIONS(1),
    [aux_sym_with_data_cat_filter_type_token3] = ACTIONS(1),
    [aux_sym_with_data_cat_filter_type_token4] = ACTIONS(1),
    [aux_sym_limit_clause_token1] = ACTIONS(1),
    [aux_sym_offset_clause_token1] = ACTIONS(1),
    [aux_sym_update_type_token1] = ACTIONS(1),
    [aux_sym_update_type_token2] = ACTIONS(1),
    [aux_sym_order_by_clause_token1] = ACTIONS(1),
    [aux_sym_order_direction_token2] = ACTIONS(1),
    [aux_sym_order_null_direction_token1] = ACTIONS(1),
    [aux_sym_order_null_direction_token2] = ACTIONS(1),
    [aux_sym_geo_location_type_token1] = ACTIONS(1),
    [aux_sym_function_expression_token1] = ACTIONS(1),
    [anon_sym_DOT] = ACTIONS(1),
    [aux_sym_boolean_token1] = ACTIONS(1),
    [aux_sym_boolean_token2] = ACTIONS(1),
    [anon_sym_BANG_EQ] = ACTIONS(1),
    [anon_sym_LT_GT] = ACTIONS(1),
    [anon_sym_LT] = ACTIONS(1),
    [anon_sym_LT_EQ] = ACTIONS(1),
    [anon_sym_GT] = ACTIONS(1),
    [anon_sym_GT_EQ] = ACTIONS(1),
    [aux_sym_value_comparison_operator_token1] = ACTIONS(1),
    [aux_sym_set_comparison_operator_token1] = ACTIONS(1),
    [aux_sym_set_comparison_operator_token3] = ACTIONS(1),
    [aux_sym_set_comparison_operator_token4] = ACTIONS(1),
    [aux_sym_date_literal_token1] = ACTIONS(1),
    [aux_sym_date_literal_token2] = ACTIONS(1),
    [aux_sym_date_literal_token3] = ACTIONS(1),
    [aux_sym_date_literal_token4] = ACTIONS(1),
    [aux_sym_date_literal_token5] = ACTIONS(1),
    [aux_sym_date_literal_token6] = ACTIONS(1),
    [aux_sym_date_literal_token7] = ACTIONS(1),
    [aux_sym_date_literal_token8] = ACTIONS(1),
    [aux_sym_date_literal_token9] = ACTIONS(1),
    [aux_sym_date_literal_token10] = ACTIONS(1),
    [aux_sym_date_literal_token11] = ACTIONS(1),
    [aux_sym_date_literal_token12] = ACTIONS(1),
    [aux_sym_date_literal_token13] = ACTIONS(1),
    [aux_sym_date_literal_token14] = ACTIONS(1),
    [aux_sym_date_literal_token15] = ACTIONS(1),
    [aux_sym_date_literal_token16] = ACTIONS(1),
    [aux_sym_date_literal_token17] = ACTIONS(1),
    [aux_sym_date_literal_token18] = ACTIONS(1),
    [aux_sym_date_literal_token19] = ACTIONS(1),
    [aux_sym_date_literal_token20] = ACTIONS(1),
    [aux_sym_date_literal_token21] = ACTIONS(1),
    [aux_sym_date_literal_token22] = ACTIONS(1),
    [aux_sym_date_literal_token23] = ACTIONS(1),
    [aux_sym_date_literal_with_param_token1] = ACTIONS(1),
    [anon_sym_COLON] = ACTIONS(1),
    [sym_bound_apex_expression] = ACTIONS(1),
    [aux_sym_null_literal_token1] = ACTIONS(1),
    [sym_string_literal] = ACTIONS(1),
    [sym_int] = ACTIONS(1),
    [sym_decimal] = ACTIONS(1),
    [sym_date] = ACTIONS(1),
    [sym_date_time] = ACTIONS(1),
  },
  [1] = {
    [sym_source_file] = STATE(795),
    [sym_header_comment] = STATE(726),
    [sym_formatting_comment] = STATE(1),
    [sym__soql_query_expression] = STATE(778),
    [sym_soql_query_body] = STATE(780),
    [sym_select_clause] = STATE(488),
    [aux_sym_source_file_repeat1] = STATE(178),
    [anon_sym_SLASH_SLASH] = ACTIONS(5),
    [anon_sym_SLASH_SLASH_SLASH] = ACTIONS(3),
    [aux_sym_select_clause_token1] = ACTIONS(7),
  },
};

static const uint16_t ts_small_parse_table[] = {
  [0] = 14,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(7), 1,
      aux_sym_select_clause_token1,
    ACTIONS(13), 1,
      aux_sym_date_literal_with_param_token1,
    ACTIONS(15), 1,
      sym_bound_apex_expression,
    ACTIONS(17), 1,
      aux_sym_null_literal_token1,
    STATE(2), 1,
      sym_formatting_comment,
    STATE(427), 1,
      sym__soql_literal,
    STATE(488), 1,
      sym_select_clause,
    STATE(799), 1,
      sym_soql_query_body,
    ACTIONS(9), 2,
      aux_sym_boolean_token1,
      aux_sym_boolean_token2,
    ACTIONS(19), 2,
      sym_string_literal,
      sym_date_time,
    ACTIONS(21), 4,
      sym_int,
      sym_decimal,
      sym_date,
      sym_currency_literal,
    STATE(48), 4,
      sym_boolean,
      sym_date_literal,
      sym_date_literal_with_param,
      sym_null_literal,
    ACTIONS(11), 23,
      aux_sym_date_literal_token1,
      aux_sym_date_literal_token2,
      aux_sym_date_literal_token3,
      aux_sym_date_literal_token4,
      aux_sym_date_literal_token5,
      aux_sym_date_literal_token6,
      aux_sym_date_literal_token7,
      aux_sym_date_literal_token8,
      aux_sym_date_literal_token9,
      aux_sym_date_literal_token10,
      aux_sym_date_literal_token11,
      aux_sym_date_literal_token12,
      aux_sym_date_literal_token13,
      aux_sym_date_literal_token14,
      aux_sym_date_literal_token15,
      aux_sym_date_literal_token16,
      aux_sym_date_literal_token17,
      aux_sym_date_literal_token18,
      aux_sym_date_literal_token19,
      aux_sym_date_literal_token20,
      aux_sym_date_literal_token21,
      aux_sym_date_literal_token22,
      aux_sym_date_literal_token23,
  [73] = 14,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(7), 1,
      aux_sym_select_clause_token1,
    ACTIONS(13), 1,
      aux_sym_date_literal_with_param_token1,
    ACTIONS(17), 1,
      aux_sym_null_literal_token1,
    ACTIONS(23), 1,
      sym_bound_apex_expression,
    STATE(3), 1,
      sym_formatting_comment,
    STATE(462), 1,
      sym__soql_literal,
    STATE(488), 1,
      sym_select_clause,
    STATE(786), 1,
      sym_soql_query_body,
    ACTIONS(9), 2,
      aux_sym_boolean_token1,
      aux_sym_boolean_token2,
    ACTIONS(19), 2,
      sym_string_literal,
      sym_date_time,
    ACTIONS(21), 4,
      sym_int,
      sym_decimal,
      sym_date,
      sym_currency_literal,
    STATE(48), 4,
      sym_boolean,
      sym_date_literal,
      sym_date_literal_with_param,
      sym_null_literal,
    ACTIONS(11), 23,
      aux_sym_date_literal_token1,
      aux_sym_date_literal_token2,
      aux_sym_date_literal_token3,
      aux_sym_date_literal_token4,
      aux_sym_date_literal_token5,
      aux_sym_date_literal_token6,
      aux_sym_date_literal_token7,
      aux_sym_date_literal_token8,
      aux_sym_date_literal_token9,
      aux_sym_date_literal_token10,
      aux_sym_date_literal_token11,
      aux_sym_date_literal_token12,
      aux_sym_date_literal_token13,
      aux_sym_date_literal_token14,
      aux_sym_date_literal_token15,
      aux_sym_date_literal_token16,
      aux_sym_date_literal_token17,
      aux_sym_date_literal_token18,
      aux_sym_date_literal_token19,
      aux_sym_date_literal_token20,
      aux_sym_date_literal_token21,
      aux_sym_date_literal_token22,
      aux_sym_date_literal_token23,
  [146] = 11,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(13), 1,
      aux_sym_date_literal_with_param_token1,
    ACTIONS(17), 1,
      aux_sym_null_literal_token1,
    ACTIONS(25), 1,
      sym_bound_apex_expression,
    STATE(4), 1,
      sym_formatting_comment,
    STATE(72), 1,
      sym__soql_literal,
    ACTIONS(9), 2,
      aux_sym_boolean_token1,
      aux_sym_boolean_token2,
    ACTIONS(19), 2,
      sym_string_literal,
      sym_date_time,
    ACTIONS(21), 4,
      sym_int,
      sym_decimal,
      sym_date,
      sym_currency_literal,
    STATE(48), 4,
      sym_boolean,
      sym_date_literal,
      sym_date_literal_with_param,
      sym_null_literal,
    ACTIONS(11), 23,
      aux_sym_date_literal_token1,
      aux_sym_date_literal_token2,
      aux_sym_date_literal_token3,
      aux_sym_date_literal_token4,
      aux_sym_date_literal_token5,
      aux_sym_date_literal_token6,
      aux_sym_date_literal_token7,
      aux_sym_date_literal_token8,
      aux_sym_date_literal_token9,
      aux_sym_date_literal_token10,
      aux_sym_date_literal_token11,
      aux_sym_date_literal_token12,
      aux_sym_date_literal_token13,
      aux_sym_date_literal_token14,
      aux_sym_date_literal_token15,
      aux_sym_date_literal_token16,
      aux_sym_date_literal_token17,
      aux_sym_date_literal_token18,
      aux_sym_date_literal_token19,
      aux_sym_date_literal_token20,
      aux_sym_date_literal_token21,
      aux_sym_date_literal_token22,
      aux_sym_date_literal_token23,
  [210] = 11,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(13), 1,
      aux_sym_date_literal_with_param_token1,
    ACTIONS(17), 1,
      aux_sym_null_literal_token1,
    ACTIONS(27), 1,
      sym_bound_apex_expression,
    STATE(5), 1,
      sym_formatting_comment,
    STATE(674), 1,
      sym__soql_literal,
    ACTIONS(9), 2,
      aux_sym_boolean_token1,
      aux_sym_boolean_token2,
    ACTIONS(19), 2,
      sym_string_literal,
      sym_date_time,
    ACTIONS(21), 4,
      sym_int,
      sym_decimal,
      sym_date,
      sym_currency_literal,
    STATE(48), 4,
      sym_boolean,
      sym_date_literal,
      sym_date_literal_with_param,
      sym_null_literal,
    ACTIONS(11), 23,
      aux_sym_date_literal_token1,
      aux_sym_date_literal_token2,
      aux_sym_date_literal_token3,
      aux_sym_date_literal_token4,
      aux_sym_date_literal_token5,
      aux_sym_date_literal_token6,
      aux_sym_date_literal_token7,
      aux_sym_date_literal_token8,
      aux_sym_date_literal_token9,
      aux_sym_date_literal_token10,
      aux_sym_date_literal_token11,
      aux_sym_date_literal_token12,
      aux_sym_date_literal_token13,
      aux_sym_date_literal_token14,
      aux_sym_date_literal_token15,
      aux_sym_date_literal_token16,
      aux_sym_date_literal_token17,
      aux_sym_date_literal_token18,
      aux_sym_date_literal_token19,
      aux_sym_date_literal_token20,
      aux_sym_date_literal_token21,
      aux_sym_date_literal_token22,
      aux_sym_date_literal_token23,
  [274] = 11,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(33), 1,
      aux_sym_date_literal_with_param_token1,
    ACTIONS(35), 1,
      sym_bound_apex_expression,
    ACTIONS(37), 1,
      aux_sym_null_literal_token1,
    STATE(6), 1,
      sym_formatting_comment,
    STATE(448), 1,
      sym__soql_literal,
    ACTIONS(29), 2,
      aux_sym_boolean_token1,
      aux_sym_boolean_token2,
    ACTIONS(39), 2,
      sym_string_literal,
      sym_date_time,
    ACTIONS(41), 4,
      sym_int,
      sym_decimal,
      sym_date,
      sym_currency_literal,
    STATE(450), 4,
      sym_boolean,
      sym_date_literal,
      sym_date_literal_with_param,
      sym_null_literal,
    ACTIONS(31), 23,
      aux_sym_date_literal_token1,
      aux_sym_date_literal_token2,
      aux_sym_date_literal_token3,
      aux_sym_date_literal_token4,
      aux_sym_date_literal_token5,
      aux_sym_date_literal_token6,
      aux_sym_date_literal_token7,
      aux_sym_date_literal_token8,
      aux_sym_date_literal_token9,
      aux_sym_date_literal_token10,
      aux_sym_date_literal_token11,
      aux_sym_date_literal_token12,
      aux_sym_date_literal_token13,
      aux_sym_date_literal_token14,
      aux_sym_date_literal_token15,
      aux_sym_date_literal_token16,
      aux_sym_date_literal_token17,
      aux_sym_date_literal_token18,
      aux_sym_date_literal_token19,
      aux_sym_date_literal_token20,
      aux_sym_date_literal_token21,
      aux_sym_date_literal_token22,
      aux_sym_date_literal_token23,
  [338] = 10,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(13), 1,
      aux_sym_date_literal_with_param_token1,
    ACTIONS(17), 1,
      aux_sym_null_literal_token1,
    STATE(7), 1,
      sym_formatting_comment,
    STATE(78), 1,
      sym__soql_literal,
    ACTIONS(9), 2,
      aux_sym_boolean_token1,
      aux_sym_boolean_token2,
    ACTIONS(19), 2,
      sym_string_literal,
      sym_date_time,
    ACTIONS(21), 4,
      sym_int,
      sym_decimal,
      sym_date,
      sym_currency_literal,
    STATE(48), 4,
      sym_boolean,
      sym_date_literal,
      sym_date_literal_with_param,
      sym_null_literal,
    ACTIONS(11), 23,
      aux_sym_date_literal_token1,
      aux_sym_date_literal_token2,
      aux_sym_date_literal_token3,
      aux_sym_date_literal_token4,
      aux_sym_date_literal_token5,
      aux_sym_date_literal_token6,
      aux_sym_date_literal_token7,
      aux_sym_date_literal_token8,
      aux_sym_date_literal_token9,
      aux_sym_date_literal_token10,
      aux_sym_date_literal_token11,
      aux_sym_date_literal_token12,
      aux_sym_date_literal_token13,
      aux_sym_date_literal_token14,
      aux_sym_date_literal_token15,
      aux_sym_date_literal_token16,
      aux_sym_date_literal_token17,
      aux_sym_date_literal_token18,
      aux_sym_date_literal_token19,
      aux_sym_date_literal_token20,
      aux_sym_date_literal_token21,
      aux_sym_date_literal_token22,
      aux_sym_date_literal_token23,
  [399] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(8), 1,
      sym_formatting_comment,
    ACTIONS(45), 4,
      sym_int,
      sym_decimal,
      sym_date,
      sym_currency_literal,
    ACTIONS(43), 30,
      aux_sym_boolean_token1,
      aux_sym_boolean_token2,
      aux_sym_date_literal_token1,
      aux_sym_date_literal_token2,
      aux_sym_date_literal_token3,
      aux_sym_date_literal_token4,
      aux_sym_date_literal_token5,
      aux_sym_date_literal_token6,
      aux_sym_date_literal_token7,
      aux_sym_date_literal_token8,
      aux_sym_date_literal_token9,
      aux_sym_date_literal_token10,
      aux_sym_date_literal_token11,
      aux_sym_date_literal_token12,
      aux_sym_date_literal_token13,
      aux_sym_date_literal_token14,
      aux_sym_date_literal_token15,
      aux_sym_date_literal_token16,
      aux_sym_date_literal_token17,
      aux_sym_date_literal_token18,
      aux_sym_date_literal_token19,
      aux_sym_date_literal_token20,
      aux_sym_date_literal_token21,
      aux_sym_date_literal_token22,
      aux_sym_date_literal_token23,
      aux_sym_date_literal_with_param_token1,
      sym_bound_apex_expression,
      aux_sym_null_literal_token1,
      sym_string_literal,
      sym_date_time,
  [444] = 6,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(49), 1,
      anon_sym_DOT,
    STATE(9), 1,
      sym_formatting_comment,
    STATE(10), 1,
      aux_sym_dotted_identifier_repeat1,
    ACTIONS(51), 3,
      anon_sym_LT,
      anon_sym_GT,
      aux_sym_set_comparison_operator_token1,
    ACTIONS(47), 29,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
      anon_sym_COMMA,
      anon_sym_EQ,
      aux_sym_using_lookup_bind_clause_token1,
      aux_sym_type_of_clause_token2,
      aux_sym_when_expression_token1,
      aux_sym_else_expression_token1,
      aux_sym_group_by_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_having_clause_token1,
      aux_sym_where_clause_token1,
      aux_sym_soql_with_clause_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
      aux_sym_order_direction_token1,
      aux_sym_order_direction_token2,
      aux_sym_order_null_direction_token1,
      aux_sym_order_null_direction_token2,
      anon_sym_BANG_EQ,
      anon_sym_LT_GT,
      anon_sym_LT_EQ,
      anon_sym_GT_EQ,
      aux_sym_value_comparison_operator_token1,
      aux_sym_set_comparison_operator_token2,
      aux_sym_set_comparison_operator_token3,
      aux_sym_set_comparison_operator_token4,
  [493] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(55), 1,
      anon_sym_DOT,
    STATE(10), 2,
      sym_formatting_comment,
      aux_sym_dotted_identifier_repeat1,
    ACTIONS(58), 3,
      anon_sym_LT,
      anon_sym_GT,
      aux_sym_set_comparison_operator_token1,
    ACTIONS(53), 29,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
      anon_sym_COMMA,
      anon_sym_EQ,
      aux_sym_using_lookup_bind_clause_token1,
      aux_sym_type_of_clause_token2,
      aux_sym_when_expression_token1,
      aux_sym_else_expression_token1,
      aux_sym_group_by_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_having_clause_token1,
      aux_sym_where_clause_token1,
      aux_sym_soql_with_clause_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
      aux_sym_order_direction_token1,
      aux_sym_order_direction_token2,
      aux_sym_order_null_direction_token1,
      aux_sym_order_null_direction_token2,
      anon_sym_BANG_EQ,
      anon_sym_LT_GT,
      anon_sym_LT_EQ,
      anon_sym_GT_EQ,
      aux_sym_value_comparison_operator_token1,
      aux_sym_set_comparison_operator_token2,
      aux_sym_set_comparison_operator_token3,
      aux_sym_set_comparison_operator_token4,
  [540] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(11), 1,
      sym_formatting_comment,
    ACTIONS(58), 3,
      anon_sym_LT,
      anon_sym_GT,
      aux_sym_set_comparison_operator_token1,
    ACTIONS(53), 30,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
      anon_sym_COMMA,
      anon_sym_EQ,
      aux_sym_using_lookup_bind_clause_token1,
      aux_sym_type_of_clause_token2,
      aux_sym_when_expression_token1,
      aux_sym_else_expression_token1,
      aux_sym_group_by_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_having_clause_token1,
      aux_sym_where_clause_token1,
      aux_sym_soql_with_clause_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
      aux_sym_order_direction_token1,
      aux_sym_order_direction_token2,
      aux_sym_order_null_direction_token1,
      aux_sym_order_null_direction_token2,
      anon_sym_DOT,
      anon_sym_BANG_EQ,
      anon_sym_LT_GT,
      anon_sym_LT_EQ,
      anon_sym_GT_EQ,
      aux_sym_value_comparison_operator_token1,
      aux_sym_set_comparison_operator_token2,
      aux_sym_set_comparison_operator_token3,
      aux_sym_set_comparison_operator_token4,
  [584] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(49), 1,
      anon_sym_DOT,
    ACTIONS(62), 1,
      anon_sym_LPAREN,
    STATE(9), 1,
      aux_sym_dotted_identifier_repeat1,
    STATE(12), 1,
      sym_formatting_comment,
    ACTIONS(64), 3,
      anon_sym_LT,
      anon_sym_GT,
      aux_sym_set_comparison_operator_token1,
    ACTIONS(60), 22,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
      anon_sym_COMMA,
      anon_sym_EQ,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_having_clause_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
      aux_sym_order_direction_token1,
      aux_sym_order_direction_token2,
      aux_sym_order_null_direction_token1,
      aux_sym_order_null_direction_token2,
      anon_sym_BANG_EQ,
      anon_sym_LT_GT,
      anon_sym_LT_EQ,
      anon_sym_GT_EQ,
      aux_sym_value_comparison_operator_token1,
      aux_sym_set_comparison_operator_token2,
      aux_sym_set_comparison_operator_token3,
      aux_sym_set_comparison_operator_token4,
  [629] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(13), 1,
      sym_formatting_comment,
    ACTIONS(64), 3,
      anon_sym_LT,
      anon_sym_GT,
      aux_sym_set_comparison_operator_token1,
    ACTIONS(60), 22,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
      anon_sym_COMMA,
      anon_sym_EQ,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_having_clause_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
      aux_sym_order_direction_token1,
      aux_sym_order_direction_token2,
      aux_sym_order_null_direction_token1,
      aux_sym_order_null_direction_token2,
      anon_sym_BANG_EQ,
      anon_sym_LT_GT,
      anon_sym_LT_EQ,
      anon_sym_GT_EQ,
      aux_sym_value_comparison_operator_token1,
      aux_sym_set_comparison_operator_token2,
      aux_sym_set_comparison_operator_token3,
      aux_sym_set_comparison_operator_token4,
  [665] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(14), 1,
      sym_formatting_comment,
    ACTIONS(68), 3,
      anon_sym_LT,
      anon_sym_GT,
      aux_sym_set_comparison_operator_token1,
    ACTIONS(66), 22,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
      anon_sym_COMMA,
      anon_sym_EQ,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_having_clause_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
      aux_sym_order_direction_token1,
      aux_sym_order_direction_token2,
      aux_sym_order_null_direction_token1,
      aux_sym_order_null_direction_token2,
      anon_sym_BANG_EQ,
      anon_sym_LT_GT,
      anon_sym_LT_EQ,
      anon_sym_GT_EQ,
      aux_sym_value_comparison_operator_token1,
      aux_sym_set_comparison_operator_token2,
      aux_sym_set_comparison_operator_token3,
      aux_sym_set_comparison_operator_token4,
  [701] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(15), 1,
      sym_formatting_comment,
    ACTIONS(72), 3,
      anon_sym_LT,
      anon_sym_GT,
      aux_sym_set_comparison_operator_token1,
    ACTIONS(70), 22,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
      anon_sym_COMMA,
      anon_sym_EQ,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_having_clause_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
      aux_sym_order_direction_token1,
      aux_sym_order_direction_token2,
      aux_sym_order_null_direction_token1,
      aux_sym_order_null_direction_token2,
      anon_sym_BANG_EQ,
      anon_sym_LT_GT,
      anon_sym_LT_EQ,
      anon_sym_GT_EQ,
      aux_sym_value_comparison_operator_token1,
      aux_sym_set_comparison_operator_token2,
      aux_sym_set_comparison_operator_token3,
      aux_sym_set_comparison_operator_token4,
  [737] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(16), 1,
      sym_formatting_comment,
    ACTIONS(76), 3,
      anon_sym_LT,
      anon_sym_GT,
      aux_sym_set_comparison_operator_token1,
    ACTIONS(74), 22,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
      anon_sym_COMMA,
      anon_sym_EQ,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_having_clause_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
      aux_sym_order_direction_token1,
      aux_sym_order_direction_token2,
      aux_sym_order_null_direction_token1,
      aux_sym_order_null_direction_token2,
      anon_sym_BANG_EQ,
      anon_sym_LT_GT,
      anon_sym_LT_EQ,
      anon_sym_GT_EQ,
      aux_sym_value_comparison_operator_token1,
      aux_sym_set_comparison_operator_token2,
      aux_sym_set_comparison_operator_token3,
      aux_sym_set_comparison_operator_token4,
  [773] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(17), 1,
      sym_formatting_comment,
    ACTIONS(80), 3,
      anon_sym_LT,
      anon_sym_GT,
      aux_sym_set_comparison_operator_token1,
    ACTIONS(78), 20,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
      anon_sym_COMMA,
      anon_sym_EQ,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_direction_token1,
      aux_sym_order_direction_token2,
      aux_sym_order_null_direction_token1,
      aux_sym_order_null_direction_token2,
      anon_sym_BANG_EQ,
      anon_sym_LT_GT,
      anon_sym_LT_EQ,
      anon_sym_GT_EQ,
      aux_sym_value_comparison_operator_token1,
      aux_sym_set_comparison_operator_token2,
      aux_sym_set_comparison_operator_token3,
      aux_sym_set_comparison_operator_token4,
  [807] = 21,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(84), 1,
      aux_sym_using_clause_token1,
    ACTIONS(86), 1,
      aux_sym_group_by_clause_token1,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    ACTIONS(92), 1,
      aux_sym_where_clause_token1,
    ACTIONS(94), 1,
      aux_sym_soql_with_clause_token1,
    ACTIONS(96), 1,
      aux_sym_limit_clause_token1,
    ACTIONS(98), 1,
      aux_sym_offset_clause_token1,
    ACTIONS(100), 1,
      aux_sym_order_by_clause_token1,
    STATE(18), 1,
      sym_formatting_comment,
    STATE(19), 1,
      sym_using_clause,
    STATE(23), 1,
      sym_where_clause,
    STATE(35), 1,
      sym_soql_with_clause,
    STATE(66), 1,
      sym_group_by_clause,
    STATE(93), 1,
      sym_order_by_clause,
    STATE(169), 1,
      sym_limit_clause,
    STATE(253), 1,
      sym_offset_clause,
    STATE(411), 1,
      sym_for_clause,
    STATE(747), 1,
      sym_update_clause,
    ACTIONS(82), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [872] = 19,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(86), 1,
      aux_sym_group_by_clause_token1,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    ACTIONS(92), 1,
      aux_sym_where_clause_token1,
    ACTIONS(94), 1,
      aux_sym_soql_with_clause_token1,
    ACTIONS(96), 1,
      aux_sym_limit_clause_token1,
    ACTIONS(98), 1,
      aux_sym_offset_clause_token1,
    ACTIONS(100), 1,
      aux_sym_order_by_clause_token1,
    STATE(19), 1,
      sym_formatting_comment,
    STATE(24), 1,
      sym_where_clause,
    STATE(37), 1,
      sym_soql_with_clause,
    STATE(65), 1,
      sym_group_by_clause,
    STATE(114), 1,
      sym_order_by_clause,
    STATE(168), 1,
      sym_limit_clause,
    STATE(197), 1,
      sym_offset_clause,
    STATE(371), 1,
      sym_for_clause,
    STATE(482), 1,
      sym_update_clause,
    ACTIONS(102), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [931] = 10,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(108), 1,
      aux_sym_set_comparison_operator_token1,
    STATE(4), 1,
      sym_value_comparison_operator,
    STATE(20), 1,
      sym_formatting_comment,
    STATE(76), 1,
      sym__comparison,
    STATE(412), 1,
      sym_set_comparison_operator,
    ACTIONS(106), 2,
      anon_sym_LT,
      anon_sym_GT,
    STATE(80), 2,
      sym__value_comparison,
      sym__set_comparison,
    ACTIONS(110), 3,
      aux_sym_set_comparison_operator_token2,
      aux_sym_set_comparison_operator_token3,
      aux_sym_set_comparison_operator_token4,
    ACTIONS(104), 6,
      anon_sym_EQ,
      anon_sym_BANG_EQ,
      anon_sym_LT_GT,
      anon_sym_LT_EQ,
      anon_sym_GT_EQ,
      aux_sym_value_comparison_operator_token1,
  [971] = 10,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(108), 1,
      aux_sym_set_comparison_operator_token1,
    STATE(6), 1,
      sym_value_comparison_operator,
    STATE(21), 1,
      sym_formatting_comment,
    STATE(415), 1,
      sym_set_comparison_operator,
    STATE(441), 1,
      sym__comparison,
    ACTIONS(106), 2,
      anon_sym_LT,
      anon_sym_GT,
    STATE(442), 2,
      sym__value_comparison,
      sym__set_comparison,
    ACTIONS(110), 3,
      aux_sym_set_comparison_operator_token2,
      aux_sym_set_comparison_operator_token3,
      aux_sym_set_comparison_operator_token4,
    ACTIONS(104), 6,
      anon_sym_EQ,
      anon_sym_BANG_EQ,
      anon_sym_LT_GT,
      anon_sym_LT_EQ,
      anon_sym_GT_EQ,
      aux_sym_value_comparison_operator_token1,
  [1011] = 14,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(112), 1,
      anon_sym_LPAREN,
    ACTIONS(114), 1,
      aux_sym_type_of_clause_token1,
    ACTIONS(116), 1,
      aux_sym_fields_expression_token1,
    ACTIONS(118), 1,
      aux_sym_function_expression_token1,
    ACTIONS(120), 1,
      sym_identifier,
    STATE(22), 1,
      sym_formatting_comment,
    STATE(332), 1,
      sym_dotted_identifier,
    STATE(422), 1,
      sym__value_expression,
    STATE(425), 1,
      sym__selectable_expression,
    STATE(763), 1,
      sym__function_name,
    STATE(802), 1,
      sym_count_expression,
    STATE(327), 2,
      sym_function_expression,
      sym_field_identifier,
    STATE(745), 4,
      sym_subquery,
      sym_type_of_clause,
      sym_fields_expression,
      sym_alias_expression,
  [1058] = 17,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(86), 1,
      aux_sym_group_by_clause_token1,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    ACTIONS(94), 1,
      aux_sym_soql_with_clause_token1,
    ACTIONS(96), 1,
      aux_sym_limit_clause_token1,
    ACTIONS(98), 1,
      aux_sym_offset_clause_token1,
    ACTIONS(100), 1,
      aux_sym_order_by_clause_token1,
    STATE(23), 1,
      sym_formatting_comment,
    STATE(36), 1,
      sym_soql_with_clause,
    STATE(52), 1,
      sym_group_by_clause,
    STATE(85), 1,
      sym_order_by_clause,
    STATE(140), 1,
      sym_limit_clause,
    STATE(216), 1,
      sym_offset_clause,
    STATE(290), 1,
      sym_for_clause,
    STATE(725), 1,
      sym_update_clause,
    ACTIONS(122), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [1111] = 17,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(86), 1,
      aux_sym_group_by_clause_token1,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    ACTIONS(94), 1,
      aux_sym_soql_with_clause_token1,
    ACTIONS(96), 1,
      aux_sym_limit_clause_token1,
    ACTIONS(98), 1,
      aux_sym_offset_clause_token1,
    ACTIONS(100), 1,
      aux_sym_order_by_clause_token1,
    STATE(24), 1,
      sym_formatting_comment,
    STATE(39), 1,
      sym_soql_with_clause,
    STATE(58), 1,
      sym_group_by_clause,
    STATE(84), 1,
      sym_order_by_clause,
    STATE(152), 1,
      sym_limit_clause,
    STATE(208), 1,
      sym_offset_clause,
    STATE(419), 1,
      sym_for_clause,
    STATE(536), 1,
      sym_update_clause,
    ACTIONS(124), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [1164] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(126), 1,
      anon_sym_DOT,
    STATE(25), 2,
      sym_formatting_comment,
      aux_sym_dotted_identifier_repeat1,
    ACTIONS(53), 5,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
      anon_sym_COMMA,
      aux_sym_group_by_clause_token1,
      aux_sym_order_by_clause_token1,
    ACTIONS(58), 9,
      aux_sym_using_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_storage_alias_token1,
      aux_sym_where_clause_token1,
      aux_sym_soql_with_clause_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      sym_identifier,
  [1193] = 6,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(129), 1,
      anon_sym_DOT,
    STATE(25), 1,
      aux_sym_dotted_identifier_repeat1,
    STATE(26), 1,
      sym_formatting_comment,
    ACTIONS(47), 5,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
      anon_sym_COMMA,
      aux_sym_group_by_clause_token1,
      aux_sym_order_by_clause_token1,
    ACTIONS(51), 9,
      aux_sym_using_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_storage_alias_token1,
      aux_sym_where_clause_token1,
      aux_sym_soql_with_clause_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      sym_identifier,
  [1224] = 6,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(129), 1,
      anon_sym_DOT,
    STATE(26), 1,
      aux_sym_dotted_identifier_repeat1,
    STATE(27), 1,
      sym_formatting_comment,
    ACTIONS(131), 5,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
      anon_sym_COMMA,
      aux_sym_group_by_clause_token1,
      aux_sym_order_by_clause_token1,
    ACTIONS(133), 9,
      aux_sym_using_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_storage_alias_token1,
      aux_sym_where_clause_token1,
      aux_sym_soql_with_clause_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      sym_identifier,
  [1255] = 8,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(137), 1,
      anon_sym_COMMA,
    ACTIONS(141), 1,
      aux_sym_storage_alias_token1,
    ACTIONS(143), 1,
      sym_identifier,
    STATE(28), 1,
      sym_formatting_comment,
    STATE(44), 1,
      aux_sym_from_clause_repeat1,
    ACTIONS(135), 4,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
      aux_sym_group_by_clause_token1,
      aux_sym_order_by_clause_token1,
    ACTIONS(139), 7,
      aux_sym_using_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_where_clause_token1,
      aux_sym_soql_with_clause_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
  [1289] = 14,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(145), 1,
      anon_sym_LPAREN,
    ACTIONS(147), 1,
      aux_sym_not_expression_token1,
    ACTIONS(149), 1,
      aux_sym_function_expression_token1,
    ACTIONS(151), 1,
      sym_identifier,
    STATE(13), 1,
      sym_dotted_identifier,
    STATE(20), 1,
      sym__value_expression,
    STATE(29), 1,
      sym_formatting_comment,
    STATE(50), 1,
      sym__condition_expression,
    STATE(68), 1,
      sym_comparison_expression,
    STATE(175), 1,
      sym__boolean_expression,
    STATE(801), 1,
      sym__function_name,
    STATE(17), 2,
      sym_function_expression,
      sym_field_identifier,
    STATE(118), 3,
      sym_and_expression,
      sym_or_expression,
      sym_not_expression,
  [1335] = 14,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(147), 1,
      aux_sym_not_expression_token1,
    ACTIONS(149), 1,
      aux_sym_function_expression_token1,
    ACTIONS(151), 1,
      sym_identifier,
    ACTIONS(153), 1,
      anon_sym_LPAREN,
    STATE(13), 1,
      sym_dotted_identifier,
    STATE(21), 1,
      sym__value_expression,
    STATE(30), 1,
      sym_formatting_comment,
    STATE(263), 1,
      sym__condition_expression,
    STATE(420), 1,
      sym_comparison_expression,
    STATE(779), 1,
      sym__boolean_expression,
    STATE(801), 1,
      sym__function_name,
    STATE(17), 2,
      sym_function_expression,
      sym_field_identifier,
    STATE(118), 3,
      sym_and_expression,
      sym_or_expression,
      sym_not_expression,
  [1381] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(31), 1,
      sym_formatting_comment,
    ACTIONS(53), 6,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
      anon_sym_COMMA,
      aux_sym_group_by_clause_token1,
      aux_sym_order_by_clause_token1,
      anon_sym_DOT,
    ACTIONS(58), 9,
      aux_sym_using_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_storage_alias_token1,
      aux_sym_where_clause_token1,
      aux_sym_soql_with_clause_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      sym_identifier,
  [1407] = 14,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(147), 1,
      aux_sym_not_expression_token1,
    ACTIONS(149), 1,
      aux_sym_function_expression_token1,
    ACTIONS(151), 1,
      sym_identifier,
    ACTIONS(153), 1,
      anon_sym_LPAREN,
    STATE(13), 1,
      sym_dotted_identifier,
    STATE(21), 1,
      sym__value_expression,
    STATE(32), 1,
      sym_formatting_comment,
    STATE(263), 1,
      sym__condition_expression,
    STATE(420), 1,
      sym_comparison_expression,
    STATE(791), 1,
      sym__boolean_expression,
    STATE(801), 1,
      sym__function_name,
    STATE(17), 2,
      sym_function_expression,
      sym_field_identifier,
    STATE(118), 3,
      sym_and_expression,
      sym_or_expression,
      sym_not_expression,
  [1453] = 13,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(112), 1,
      anon_sym_LPAREN,
    ACTIONS(114), 1,
      aux_sym_type_of_clause_token1,
    ACTIONS(116), 1,
      aux_sym_fields_expression_token1,
    ACTIONS(118), 1,
      aux_sym_function_expression_token1,
    ACTIONS(120), 1,
      sym_identifier,
    STATE(33), 1,
      sym_formatting_comment,
    STATE(332), 1,
      sym_dotted_identifier,
    STATE(422), 1,
      sym__value_expression,
    STATE(614), 1,
      sym__selectable_expression,
    STATE(772), 1,
      sym__function_name,
    STATE(327), 2,
      sym_function_expression,
      sym_field_identifier,
    STATE(745), 4,
      sym_subquery,
      sym_type_of_clause,
      sym_fields_expression,
      sym_alias_expression,
  [1497] = 14,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(145), 1,
      anon_sym_LPAREN,
    ACTIONS(147), 1,
      aux_sym_not_expression_token1,
    ACTIONS(149), 1,
      aux_sym_function_expression_token1,
    ACTIONS(151), 1,
      sym_identifier,
    STATE(13), 1,
      sym_dotted_identifier,
    STATE(20), 1,
      sym__value_expression,
    STATE(34), 1,
      sym_formatting_comment,
    STATE(50), 1,
      sym__condition_expression,
    STATE(68), 1,
      sym_comparison_expression,
    STATE(117), 1,
      sym__boolean_expression,
    STATE(801), 1,
      sym__function_name,
    STATE(17), 2,
      sym_function_expression,
      sym_field_identifier,
    STATE(118), 3,
      sym_and_expression,
      sym_or_expression,
      sym_not_expression,
  [1543] = 15,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(86), 1,
      aux_sym_group_by_clause_token1,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    ACTIONS(96), 1,
      aux_sym_limit_clause_token1,
    ACTIONS(98), 1,
      aux_sym_offset_clause_token1,
    ACTIONS(100), 1,
      aux_sym_order_by_clause_token1,
    STATE(35), 1,
      sym_formatting_comment,
    STATE(54), 1,
      sym_group_by_clause,
    STATE(109), 1,
      sym_order_by_clause,
    STATE(135), 1,
      sym_limit_clause,
    STATE(223), 1,
      sym_offset_clause,
    STATE(316), 1,
      sym_for_clause,
    STATE(753), 1,
      sym_update_clause,
    ACTIONS(155), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [1590] = 15,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(86), 1,
      aux_sym_group_by_clause_token1,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    ACTIONS(96), 1,
      aux_sym_limit_clause_token1,
    ACTIONS(98), 1,
      aux_sym_offset_clause_token1,
    ACTIONS(100), 1,
      aux_sym_order_by_clause_token1,
    STATE(36), 1,
      sym_formatting_comment,
    STATE(60), 1,
      sym_group_by_clause,
    STATE(97), 1,
      sym_order_by_clause,
    STATE(139), 1,
      sym_limit_clause,
    STATE(186), 1,
      sym_offset_clause,
    STATE(271), 1,
      sym_for_clause,
    STATE(635), 1,
      sym_update_clause,
    ACTIONS(157), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [1637] = 15,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(86), 1,
      aux_sym_group_by_clause_token1,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    ACTIONS(96), 1,
      aux_sym_limit_clause_token1,
    ACTIONS(98), 1,
      aux_sym_offset_clause_token1,
    ACTIONS(100), 1,
      aux_sym_order_by_clause_token1,
    STATE(37), 1,
      sym_formatting_comment,
    STATE(59), 1,
      sym_group_by_clause,
    STATE(87), 1,
      sym_order_by_clause,
    STATE(163), 1,
      sym_limit_clause,
    STATE(243), 1,
      sym_offset_clause,
    STATE(320), 1,
      sym_for_clause,
    STATE(627), 1,
      sym_update_clause,
    ACTIONS(159), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [1684] = 6,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(141), 1,
      aux_sym_storage_alias_token1,
    ACTIONS(143), 1,
      sym_identifier,
    STATE(38), 1,
      sym_formatting_comment,
    ACTIONS(161), 5,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
      anon_sym_COMMA,
      aux_sym_group_by_clause_token1,
      aux_sym_order_by_clause_token1,
    ACTIONS(163), 7,
      aux_sym_using_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_where_clause_token1,
      aux_sym_soql_with_clause_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
  [1713] = 15,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(86), 1,
      aux_sym_group_by_clause_token1,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    ACTIONS(96), 1,
      aux_sym_limit_clause_token1,
    ACTIONS(98), 1,
      aux_sym_offset_clause_token1,
    ACTIONS(100), 1,
      aux_sym_order_by_clause_token1,
    STATE(39), 1,
      sym_formatting_comment,
    STATE(62), 1,
      sym_group_by_clause,
    STATE(94), 1,
      sym_order_by_clause,
    STATE(155), 1,
      sym_limit_clause,
    STATE(201), 1,
      sym_offset_clause,
    STATE(410), 1,
      sym_for_clause,
    STATE(498), 1,
      sym_update_clause,
    ACTIONS(165), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [1760] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(40), 1,
      sym_formatting_comment,
    ACTIONS(131), 5,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
      anon_sym_COMMA,
      aux_sym_group_by_clause_token1,
      aux_sym_order_by_clause_token1,
    ACTIONS(133), 9,
      aux_sym_using_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_storage_alias_token1,
      aux_sym_where_clause_token1,
      aux_sym_soql_with_clause_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      sym_identifier,
  [1785] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(137), 1,
      anon_sym_COMMA,
    STATE(41), 1,
      sym_formatting_comment,
    STATE(44), 1,
      aux_sym_from_clause_repeat1,
    ACTIONS(135), 11,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
      aux_sym_using_clause_token1,
      aux_sym_group_by_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_where_clause_token1,
      aux_sym_soql_with_clause_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [1811] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(167), 1,
      anon_sym_COMMA,
    STATE(42), 2,
      sym_formatting_comment,
      aux_sym_from_clause_repeat1,
    ACTIONS(161), 11,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
      aux_sym_using_clause_token1,
      aux_sym_group_by_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_where_clause_token1,
      aux_sym_soql_with_clause_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [1835] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(172), 1,
      aux_sym_or_expression_token1,
    STATE(43), 1,
      sym_formatting_comment,
    ACTIONS(170), 12,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
      anon_sym_COMMA,
      aux_sym_group_by_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_from_clause_token1,
      aux_sym_and_expression_token1,
      aux_sym_soql_with_clause_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [1859] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(137), 1,
      anon_sym_COMMA,
    STATE(42), 1,
      aux_sym_from_clause_repeat1,
    STATE(44), 1,
      sym_formatting_comment,
    ACTIONS(174), 11,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
      aux_sym_using_clause_token1,
      aux_sym_group_by_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_where_clause_token1,
      aux_sym_soql_with_clause_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [1885] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(178), 1,
      aux_sym_or_expression_token1,
    STATE(45), 1,
      sym_formatting_comment,
    ACTIONS(176), 12,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
      anon_sym_COMMA,
      aux_sym_group_by_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_where_clause_token1,
      aux_sym_and_expression_token1,
      aux_sym_soql_with_clause_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [1909] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(182), 1,
      aux_sym_or_expression_token1,
    STATE(46), 1,
      sym_formatting_comment,
    ACTIONS(180), 12,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
      anon_sym_COMMA,
      aux_sym_group_by_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_where_clause_token1,
      aux_sym_and_expression_token1,
      aux_sym_soql_with_clause_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [1933] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(186), 1,
      aux_sym_or_expression_token1,
    STATE(47), 1,
      sym_formatting_comment,
    ACTIONS(184), 12,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
      anon_sym_COMMA,
      aux_sym_group_by_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_where_clause_token1,
      aux_sym_and_expression_token1,
      aux_sym_soql_with_clause_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [1957] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(190), 1,
      aux_sym_or_expression_token1,
    STATE(48), 1,
      sym_formatting_comment,
    ACTIONS(188), 12,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
      anon_sym_COMMA,
      aux_sym_group_by_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_where_clause_token1,
      aux_sym_and_expression_token1,
      aux_sym_soql_with_clause_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [1981] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(194), 1,
      aux_sym_or_expression_token1,
    STATE(49), 1,
      sym_formatting_comment,
    ACTIONS(192), 12,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
      anon_sym_COMMA,
      aux_sym_group_by_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_where_clause_token1,
      aux_sym_and_expression_token1,
      aux_sym_soql_with_clause_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [2005] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(198), 1,
      aux_sym_and_expression_token1,
    ACTIONS(200), 1,
      aux_sym_or_expression_token1,
    STATE(50), 1,
      sym_formatting_comment,
    STATE(74), 1,
      aux_sym_and_expression_repeat1,
    STATE(75), 1,
      aux_sym_or_expression_repeat1,
    ACTIONS(196), 9,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
      aux_sym_group_by_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_soql_with_clause_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [2035] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(51), 1,
      sym_formatting_comment,
    STATE(112), 1,
      sym_order_direction,
    STATE(174), 1,
      sym_order_null_direction,
    ACTIONS(204), 2,
      aux_sym_order_direction_token1,
      aux_sym_order_direction_token2,
    ACTIONS(206), 2,
      aux_sym_order_null_direction_token1,
      aux_sym_order_null_direction_token2,
    ACTIONS(202), 7,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
      anon_sym_COMMA,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
  [2065] = 13,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    ACTIONS(96), 1,
      aux_sym_limit_clause_token1,
    ACTIONS(98), 1,
      aux_sym_offset_clause_token1,
    ACTIONS(100), 1,
      aux_sym_order_by_clause_token1,
    STATE(52), 1,
      sym_formatting_comment,
    STATE(90), 1,
      sym_order_by_clause,
    STATE(131), 1,
      sym_limit_clause,
    STATE(185), 1,
      sym_offset_clause,
    STATE(413), 1,
      sym_for_clause,
    STATE(605), 1,
      sym_update_clause,
    ACTIONS(208), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [2106] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(53), 1,
      sym_formatting_comment,
    ACTIONS(210), 12,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
      anon_sym_COMMA,
      aux_sym_using_clause_token1,
      aux_sym_group_by_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_where_clause_token1,
      aux_sym_soql_with_clause_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [2127] = 13,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    ACTIONS(96), 1,
      aux_sym_limit_clause_token1,
    ACTIONS(98), 1,
      aux_sym_offset_clause_token1,
    ACTIONS(100), 1,
      aux_sym_order_by_clause_token1,
    STATE(54), 1,
      sym_formatting_comment,
    STATE(102), 1,
      sym_order_by_clause,
    STATE(142), 1,
      sym_limit_clause,
    STATE(189), 1,
      sym_offset_clause,
    STATE(293), 1,
      sym_for_clause,
    STATE(475), 1,
      sym_update_clause,
    ACTIONS(212), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [2168] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(55), 1,
      sym_formatting_comment,
    ACTIONS(161), 12,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
      anon_sym_COMMA,
      aux_sym_using_clause_token1,
      aux_sym_group_by_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_where_clause_token1,
      aux_sym_soql_with_clause_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [2189] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(216), 1,
      aux_sym_using_lookup_bind_clause_token1,
    STATE(56), 1,
      sym_formatting_comment,
    STATE(103), 1,
      sym_using_lookup_bind_clause,
    ACTIONS(214), 10,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
      aux_sym_group_by_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_where_clause_token1,
      aux_sym_soql_with_clause_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [2214] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(57), 1,
      sym_formatting_comment,
    ACTIONS(218), 12,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
      anon_sym_COMMA,
      aux_sym_using_clause_token1,
      aux_sym_group_by_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_where_clause_token1,
      aux_sym_soql_with_clause_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [2235] = 13,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    ACTIONS(96), 1,
      aux_sym_limit_clause_token1,
    ACTIONS(98), 1,
      aux_sym_offset_clause_token1,
    ACTIONS(100), 1,
      aux_sym_order_by_clause_token1,
    STATE(58), 1,
      sym_formatting_comment,
    STATE(92), 1,
      sym_order_by_clause,
    STATE(151), 1,
      sym_limit_clause,
    STATE(199), 1,
      sym_offset_clause,
    STATE(406), 1,
      sym_for_clause,
    STATE(486), 1,
      sym_update_clause,
    ACTIONS(220), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [2276] = 13,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    ACTIONS(96), 1,
      aux_sym_limit_clause_token1,
    ACTIONS(98), 1,
      aux_sym_offset_clause_token1,
    ACTIONS(100), 1,
      aux_sym_order_by_clause_token1,
    STATE(59), 1,
      sym_formatting_comment,
    STATE(95), 1,
      sym_order_by_clause,
    STATE(158), 1,
      sym_limit_clause,
    STATE(206), 1,
      sym_offset_clause,
    STATE(272), 1,
      sym_for_clause,
    STATE(526), 1,
      sym_update_clause,
    ACTIONS(222), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [2317] = 13,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    ACTIONS(96), 1,
      aux_sym_limit_clause_token1,
    ACTIONS(98), 1,
      aux_sym_offset_clause_token1,
    ACTIONS(100), 1,
      aux_sym_order_by_clause_token1,
    STATE(60), 1,
      sym_formatting_comment,
    STATE(96), 1,
      sym_order_by_clause,
    STATE(166), 1,
      sym_limit_clause,
    STATE(214), 1,
      sym_offset_clause,
    STATE(291), 1,
      sym_for_clause,
    STATE(623), 1,
      sym_update_clause,
    ACTIONS(224), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [2358] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(228), 1,
      anon_sym_COMMA,
    STATE(61), 1,
      sym_formatting_comment,
    STATE(64), 1,
      aux_sym_using_lookup_bind_clause_repeat1,
    ACTIONS(226), 10,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
      aux_sym_group_by_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_where_clause_token1,
      aux_sym_soql_with_clause_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [2383] = 13,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    ACTIONS(96), 1,
      aux_sym_limit_clause_token1,
    ACTIONS(98), 1,
      aux_sym_offset_clause_token1,
    ACTIONS(100), 1,
      aux_sym_order_by_clause_token1,
    STATE(62), 1,
      sym_formatting_comment,
    STATE(99), 1,
      sym_order_by_clause,
    STATE(134), 1,
      sym_limit_clause,
    STATE(227), 1,
      sym_offset_clause,
    STATE(326), 1,
      sym_for_clause,
    STATE(491), 1,
      sym_update_clause,
    ACTIONS(230), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [2424] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(228), 1,
      anon_sym_COMMA,
    STATE(61), 1,
      aux_sym_using_lookup_bind_clause_repeat1,
    STATE(63), 1,
      sym_formatting_comment,
    ACTIONS(232), 10,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
      aux_sym_group_by_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_where_clause_token1,
      aux_sym_soql_with_clause_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [2449] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(236), 1,
      anon_sym_COMMA,
    STATE(64), 2,
      sym_formatting_comment,
      aux_sym_using_lookup_bind_clause_repeat1,
    ACTIONS(234), 10,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
      aux_sym_group_by_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_where_clause_token1,
      aux_sym_soql_with_clause_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [2472] = 13,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    ACTIONS(96), 1,
      aux_sym_limit_clause_token1,
    ACTIONS(98), 1,
      aux_sym_offset_clause_token1,
    ACTIONS(100), 1,
      aux_sym_order_by_clause_token1,
    STATE(65), 1,
      sym_formatting_comment,
    STATE(81), 1,
      sym_order_by_clause,
    STATE(132), 1,
      sym_limit_clause,
    STATE(200), 1,
      sym_offset_clause,
    STATE(414), 1,
      sym_for_clause,
    STATE(676), 1,
      sym_update_clause,
    ACTIONS(239), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [2513] = 13,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    ACTIONS(96), 1,
      aux_sym_limit_clause_token1,
    ACTIONS(98), 1,
      aux_sym_offset_clause_token1,
    ACTIONS(100), 1,
      aux_sym_order_by_clause_token1,
    STATE(66), 1,
      sym_formatting_comment,
    STATE(106), 1,
      sym_order_by_clause,
    STATE(161), 1,
      sym_limit_clause,
    STATE(202), 1,
      sym_offset_clause,
    STATE(409), 1,
      sym_for_clause,
    STATE(513), 1,
      sym_update_clause,
    ACTIONS(241), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [2554] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(245), 1,
      aux_sym_or_expression_token1,
    STATE(67), 1,
      sym_formatting_comment,
    ACTIONS(243), 10,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
      aux_sym_group_by_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_and_expression_token1,
      aux_sym_soql_with_clause_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [2576] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(249), 1,
      aux_sym_or_expression_token1,
    STATE(68), 1,
      sym_formatting_comment,
    ACTIONS(247), 10,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
      aux_sym_group_by_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_and_expression_token1,
      aux_sym_soql_with_clause_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [2598] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(253), 1,
      aux_sym_or_expression_token1,
    STATE(69), 1,
      sym_formatting_comment,
    ACTIONS(251), 10,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
      aux_sym_group_by_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_and_expression_token1,
      aux_sym_soql_with_clause_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [2620] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(257), 1,
      aux_sym_and_expression_token1,
    STATE(70), 2,
      sym_formatting_comment,
      aux_sym_and_expression_repeat1,
    ACTIONS(255), 9,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
      aux_sym_group_by_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_soql_with_clause_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [2642] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(262), 1,
      aux_sym_or_expression_token1,
    STATE(71), 2,
      sym_formatting_comment,
      aux_sym_or_expression_repeat1,
    ACTIONS(260), 9,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
      aux_sym_group_by_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_soql_with_clause_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [2664] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(267), 1,
      aux_sym_or_expression_token1,
    STATE(72), 1,
      sym_formatting_comment,
    ACTIONS(265), 10,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
      aux_sym_group_by_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_and_expression_token1,
      aux_sym_soql_with_clause_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [2686] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(271), 1,
      aux_sym_or_expression_token1,
    STATE(73), 1,
      sym_formatting_comment,
    ACTIONS(269), 10,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
      aux_sym_group_by_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_and_expression_token1,
      aux_sym_soql_with_clause_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [2708] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(198), 1,
      aux_sym_and_expression_token1,
    STATE(70), 1,
      aux_sym_and_expression_repeat1,
    STATE(74), 1,
      sym_formatting_comment,
    ACTIONS(273), 9,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
      aux_sym_group_by_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_soql_with_clause_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [2732] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(200), 1,
      aux_sym_or_expression_token1,
    STATE(71), 1,
      aux_sym_or_expression_repeat1,
    STATE(75), 1,
      sym_formatting_comment,
    ACTIONS(275), 9,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
      aux_sym_group_by_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_soql_with_clause_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [2756] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(279), 1,
      aux_sym_or_expression_token1,
    STATE(76), 1,
      sym_formatting_comment,
    ACTIONS(277), 10,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
      aux_sym_group_by_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_and_expression_token1,
      aux_sym_soql_with_clause_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [2778] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(283), 1,
      aux_sym_or_expression_token1,
    STATE(77), 1,
      sym_formatting_comment,
    ACTIONS(281), 10,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
      aux_sym_group_by_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_and_expression_token1,
      aux_sym_soql_with_clause_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [2800] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(78), 1,
      sym_formatting_comment,
    ACTIONS(285), 11,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
      anon_sym_COMMA,
      aux_sym_group_by_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_where_clause_token1,
      aux_sym_soql_with_clause_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [2820] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(79), 1,
      sym_formatting_comment,
    ACTIONS(234), 11,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
      anon_sym_COMMA,
      aux_sym_group_by_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_where_clause_token1,
      aux_sym_soql_with_clause_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [2840] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(289), 1,
      aux_sym_or_expression_token1,
    STATE(80), 1,
      sym_formatting_comment,
    ACTIONS(287), 10,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
      aux_sym_group_by_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_and_expression_token1,
      aux_sym_soql_with_clause_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [2862] = 11,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    ACTIONS(96), 1,
      aux_sym_limit_clause_token1,
    ACTIONS(98), 1,
      aux_sym_offset_clause_token1,
    STATE(81), 1,
      sym_formatting_comment,
    STATE(150), 1,
      sym_limit_clause,
    STATE(198), 1,
      sym_offset_clause,
    STATE(395), 1,
      sym_for_clause,
    STATE(479), 1,
      sym_update_clause,
    ACTIONS(291), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [2897] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(82), 1,
      sym_formatting_comment,
    ACTIONS(255), 10,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
      aux_sym_group_by_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_and_expression_token1,
      aux_sym_soql_with_clause_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [2916] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(293), 1,
      aux_sym_or_expression_token1,
    STATE(83), 1,
      sym_formatting_comment,
    ACTIONS(260), 9,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
      aux_sym_group_by_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_soql_with_clause_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [2937] = 11,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    ACTIONS(96), 1,
      aux_sym_limit_clause_token1,
    ACTIONS(98), 1,
      aux_sym_offset_clause_token1,
    STATE(84), 1,
      sym_formatting_comment,
    STATE(157), 1,
      sym_limit_clause,
    STATE(204), 1,
      sym_offset_clause,
    STATE(270), 1,
      sym_for_clause,
    STATE(516), 1,
      sym_update_clause,
    ACTIONS(295), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [2972] = 11,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    ACTIONS(96), 1,
      aux_sym_limit_clause_token1,
    ACTIONS(98), 1,
      aux_sym_offset_clause_token1,
    STATE(85), 1,
      sym_formatting_comment,
    STATE(141), 1,
      sym_limit_clause,
    STATE(188), 1,
      sym_offset_clause,
    STATE(285), 1,
      sym_for_clause,
    STATE(751), 1,
      sym_update_clause,
    ACTIONS(297), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [3007] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(301), 1,
      anon_sym_COMMA,
    STATE(86), 1,
      sym_formatting_comment,
    STATE(105), 1,
      aux_sym__group_by_expression_repeat1,
    ACTIONS(299), 8,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_having_clause_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [3030] = 11,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    ACTIONS(96), 1,
      aux_sym_limit_clause_token1,
    ACTIONS(98), 1,
      aux_sym_offset_clause_token1,
    STATE(87), 1,
      sym_formatting_comment,
    STATE(164), 1,
      sym_limit_clause,
    STATE(209), 1,
      sym_offset_clause,
    STATE(277), 1,
      sym_for_clause,
    STATE(552), 1,
      sym_update_clause,
    ACTIONS(303), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [3065] = 11,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(145), 1,
      anon_sym_LPAREN,
    ACTIONS(149), 1,
      aux_sym_function_expression_token1,
    ACTIONS(151), 1,
      sym_identifier,
    STATE(13), 1,
      sym_dotted_identifier,
    STATE(20), 1,
      sym__value_expression,
    STATE(68), 1,
      sym_comparison_expression,
    STATE(82), 1,
      sym__condition_expression,
    STATE(88), 1,
      sym_formatting_comment,
    STATE(801), 1,
      sym__function_name,
    STATE(17), 2,
      sym_function_expression,
      sym_field_identifier,
  [3100] = 11,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(145), 1,
      anon_sym_LPAREN,
    ACTIONS(149), 1,
      aux_sym_function_expression_token1,
    ACTIONS(151), 1,
      sym_identifier,
    STATE(13), 1,
      sym_dotted_identifier,
    STATE(20), 1,
      sym__value_expression,
    STATE(68), 1,
      sym_comparison_expression,
    STATE(83), 1,
      sym__condition_expression,
    STATE(89), 1,
      sym_formatting_comment,
    STATE(801), 1,
      sym__function_name,
    STATE(17), 2,
      sym_function_expression,
      sym_field_identifier,
  [3135] = 11,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    ACTIONS(96), 1,
      aux_sym_limit_clause_token1,
    ACTIONS(98), 1,
      aux_sym_offset_clause_token1,
    STATE(90), 1,
      sym_formatting_comment,
    STATE(165), 1,
      sym_limit_clause,
    STATE(213), 1,
      sym_offset_clause,
    STATE(289), 1,
      sym_for_clause,
    STATE(616), 1,
      sym_update_clause,
    ACTIONS(305), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [3170] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(309), 1,
      aux_sym_and_expression_token1,
    STATE(91), 1,
      sym_formatting_comment,
    STATE(98), 1,
      aux_sym_with_data_cat_expression_repeat1,
    ACTIONS(307), 8,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
      aux_sym_group_by_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [3193] = 11,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    ACTIONS(96), 1,
      aux_sym_limit_clause_token1,
    ACTIONS(98), 1,
      aux_sym_offset_clause_token1,
    STATE(92), 1,
      sym_formatting_comment,
    STATE(133), 1,
      sym_limit_clause,
    STATE(226), 1,
      sym_offset_clause,
    STATE(325), 1,
      sym_for_clause,
    STATE(487), 1,
      sym_update_clause,
    ACTIONS(311), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [3228] = 11,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    ACTIONS(96), 1,
      aux_sym_limit_clause_token1,
    ACTIONS(98), 1,
      aux_sym_offset_clause_token1,
    STATE(93), 1,
      sym_formatting_comment,
    STATE(156), 1,
      sym_limit_clause,
    STATE(240), 1,
      sym_offset_clause,
    STATE(367), 1,
      sym_for_clause,
    STATE(590), 1,
      sym_update_clause,
    ACTIONS(313), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [3263] = 11,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    ACTIONS(96), 1,
      aux_sym_limit_clause_token1,
    ACTIONS(98), 1,
      aux_sym_offset_clause_token1,
    STATE(94), 1,
      sym_formatting_comment,
    STATE(136), 1,
      sym_limit_clause,
    STATE(230), 1,
      sym_offset_clause,
    STATE(331), 1,
      sym_for_clause,
    STATE(500), 1,
      sym_update_clause,
    ACTIONS(315), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [3298] = 11,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    ACTIONS(96), 1,
      aux_sym_limit_clause_token1,
    ACTIONS(98), 1,
      aux_sym_offset_clause_token1,
    STATE(95), 1,
      sym_formatting_comment,
    STATE(137), 1,
      sym_limit_clause,
    STATE(233), 1,
      sym_offset_clause,
    STATE(340), 1,
      sym_for_clause,
    STATE(519), 1,
      sym_update_clause,
    ACTIONS(317), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [3333] = 11,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    ACTIONS(96), 1,
      aux_sym_limit_clause_token1,
    ACTIONS(98), 1,
      aux_sym_offset_clause_token1,
    STATE(96), 1,
      sym_formatting_comment,
    STATE(138), 1,
      sym_limit_clause,
    STATE(237), 1,
      sym_offset_clause,
    STATE(355), 1,
      sym_for_clause,
    STATE(557), 1,
      sym_update_clause,
    ACTIONS(319), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [3368] = 11,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    ACTIONS(96), 1,
      aux_sym_limit_clause_token1,
    ACTIONS(98), 1,
      aux_sym_offset_clause_token1,
    STATE(97), 1,
      sym_formatting_comment,
    STATE(146), 1,
      sym_limit_clause,
    STATE(184), 1,
      sym_offset_clause,
    STATE(297), 1,
      sym_for_clause,
    STATE(656), 1,
      sym_update_clause,
    ACTIONS(321), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [3403] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(325), 1,
      aux_sym_and_expression_token1,
    STATE(98), 2,
      sym_formatting_comment,
      aux_sym_with_data_cat_expression_repeat1,
    ACTIONS(323), 8,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
      aux_sym_group_by_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [3424] = 11,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    ACTIONS(96), 1,
      aux_sym_limit_clause_token1,
    ACTIONS(98), 1,
      aux_sym_offset_clause_token1,
    STATE(99), 1,
      sym_formatting_comment,
    STATE(145), 1,
      sym_limit_clause,
    STATE(244), 1,
      sym_offset_clause,
    STATE(375), 1,
      sym_for_clause,
    STATE(617), 1,
      sym_update_clause,
    ACTIONS(328), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [3459] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(100), 1,
      sym_formatting_comment,
    ACTIONS(330), 10,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
      aux_sym_group_by_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_where_clause_token1,
      aux_sym_soql_with_clause_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [3478] = 8,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(334), 1,
      aux_sym_with_user_id_type_token1,
    ACTIONS(336), 1,
      aux_sym_with_record_visibility_expression_token1,
    ACTIONS(338), 1,
      aux_sym_with_data_cat_expression_token1,
    STATE(101), 1,
      sym_formatting_comment,
    STATE(126), 1,
      sym_soql_with_type,
    ACTIONS(332), 3,
      aux_sym_soql_with_type_token1,
      aux_sym_soql_with_type_token2,
      aux_sym_soql_with_type_token3,
    STATE(160), 3,
      sym_with_user_id_type,
      sym_with_record_visibility_expression,
      sym_with_data_cat_expression,
  [3507] = 11,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    ACTIONS(96), 1,
      aux_sym_limit_clause_token1,
    ACTIONS(98), 1,
      aux_sym_offset_clause_token1,
    STATE(102), 1,
      sym_formatting_comment,
    STATE(127), 1,
      sym_limit_clause,
    STATE(220), 1,
      sym_offset_clause,
    STATE(306), 1,
      sym_for_clause,
    STATE(727), 1,
      sym_update_clause,
    ACTIONS(340), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [3542] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(103), 1,
      sym_formatting_comment,
    ACTIONS(342), 10,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
      aux_sym_group_by_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_where_clause_token1,
      aux_sym_soql_with_clause_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [3561] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(104), 1,
      sym_formatting_comment,
    ACTIONS(344), 10,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
      aux_sym_group_by_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_where_clause_token1,
      aux_sym_soql_with_clause_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [3580] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(348), 1,
      anon_sym_COMMA,
    STATE(105), 2,
      sym_formatting_comment,
      aux_sym__group_by_expression_repeat1,
    ACTIONS(346), 8,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_having_clause_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [3601] = 11,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    ACTIONS(96), 1,
      aux_sym_limit_clause_token1,
    ACTIONS(98), 1,
      aux_sym_offset_clause_token1,
    STATE(106), 1,
      sym_formatting_comment,
    STATE(130), 1,
      sym_limit_clause,
    STATE(254), 1,
      sym_offset_clause,
    STATE(407), 1,
      sym_for_clause,
    STATE(547), 1,
      sym_update_clause,
    ACTIONS(351), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [3636] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(301), 1,
      anon_sym_COMMA,
    STATE(86), 1,
      aux_sym__group_by_expression_repeat1,
    STATE(107), 1,
      sym_formatting_comment,
    ACTIONS(353), 8,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_having_clause_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [3659] = 11,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(145), 1,
      anon_sym_LPAREN,
    ACTIONS(149), 1,
      aux_sym_function_expression_token1,
    ACTIONS(151), 1,
      sym_identifier,
    STATE(13), 1,
      sym_dotted_identifier,
    STATE(20), 1,
      sym__value_expression,
    STATE(68), 1,
      sym_comparison_expression,
    STATE(108), 1,
      sym_formatting_comment,
    STATE(116), 1,
      sym__condition_expression,
    STATE(801), 1,
      sym__function_name,
    STATE(17), 2,
      sym_function_expression,
      sym_field_identifier,
  [3694] = 11,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    ACTIONS(96), 1,
      aux_sym_limit_clause_token1,
    ACTIONS(98), 1,
      aux_sym_offset_clause_token1,
    STATE(109), 1,
      sym_formatting_comment,
    STATE(148), 1,
      sym_limit_clause,
    STATE(191), 1,
      sym_offset_clause,
    STATE(307), 1,
      sym_for_clause,
    STATE(507), 1,
      sym_update_clause,
    ACTIONS(355), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [3729] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(110), 1,
      sym_formatting_comment,
    ACTIONS(357), 10,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
      aux_sym_group_by_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_where_clause_token1,
      aux_sym_soql_with_clause_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [3748] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(309), 1,
      aux_sym_and_expression_token1,
    STATE(91), 1,
      aux_sym_with_data_cat_expression_repeat1,
    STATE(111), 1,
      sym_formatting_comment,
    ACTIONS(359), 8,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
      aux_sym_group_by_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [3771] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(112), 1,
      sym_formatting_comment,
    STATE(177), 1,
      sym_order_null_direction,
    ACTIONS(206), 2,
      aux_sym_order_null_direction_token1,
      aux_sym_order_null_direction_token2,
    ACTIONS(361), 7,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
      anon_sym_COMMA,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
  [3794] = 11,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(149), 1,
      aux_sym_function_expression_token1,
    ACTIONS(151), 1,
      sym_identifier,
    ACTIONS(153), 1,
      anon_sym_LPAREN,
    STATE(13), 1,
      sym_dotted_identifier,
    STATE(21), 1,
      sym__value_expression,
    STATE(113), 1,
      sym_formatting_comment,
    STATE(420), 1,
      sym_comparison_expression,
    STATE(736), 1,
      sym__condition_expression,
    STATE(801), 1,
      sym__function_name,
    STATE(17), 2,
      sym_function_expression,
      sym_field_identifier,
  [3829] = 11,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    ACTIONS(96), 1,
      aux_sym_limit_clause_token1,
    ACTIONS(98), 1,
      aux_sym_offset_clause_token1,
    STATE(114), 1,
      sym_formatting_comment,
    STATE(128), 1,
      sym_limit_clause,
    STATE(251), 1,
      sym_offset_clause,
    STATE(376), 1,
      sym_for_clause,
    STATE(474), 1,
      sym_update_clause,
    ACTIONS(363), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [3864] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(115), 1,
      sym_formatting_comment,
    ACTIONS(365), 10,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
      aux_sym_group_by_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_where_clause_token1,
      aux_sym_soql_with_clause_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [3883] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(116), 1,
      sym_formatting_comment,
    ACTIONS(367), 9,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
      aux_sym_group_by_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_soql_with_clause_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [3901] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(117), 1,
      sym_formatting_comment,
    ACTIONS(369), 9,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
      aux_sym_group_by_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_soql_with_clause_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [3919] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(118), 1,
      sym_formatting_comment,
    ACTIONS(196), 9,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
      aux_sym_group_by_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_soql_with_clause_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [3937] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(119), 1,
      sym_formatting_comment,
    ACTIONS(346), 9,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
      anon_sym_COMMA,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_having_clause_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [3955] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(120), 1,
      sym_formatting_comment,
    ACTIONS(371), 9,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
      aux_sym_group_by_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_and_expression_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [3973] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(121), 1,
      sym_formatting_comment,
    ACTIONS(323), 9,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
      aux_sym_group_by_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_and_expression_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [3991] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(122), 1,
      sym_formatting_comment,
    ACTIONS(373), 9,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
      anon_sym_COMMA,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_null_direction_token1,
      aux_sym_order_null_direction_token2,
  [4009] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(123), 1,
      sym_formatting_comment,
    ACTIONS(375), 9,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
      aux_sym_group_by_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_and_expression_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [4027] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(124), 1,
      sym_formatting_comment,
    ACTIONS(377), 9,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
      aux_sym_group_by_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_and_expression_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [4045] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(381), 1,
      aux_sym_having_clause_token1,
    STATE(125), 1,
      sym_formatting_comment,
    STATE(172), 1,
      sym_having_clause,
    ACTIONS(379), 7,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [4067] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(126), 1,
      sym_formatting_comment,
    ACTIONS(383), 8,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
      aux_sym_group_by_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [4084] = 9,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    ACTIONS(98), 1,
      aux_sym_offset_clause_token1,
    STATE(127), 1,
      sym_formatting_comment,
    STATE(239), 1,
      sym_offset_clause,
    STATE(363), 1,
      sym_for_clause,
    STATE(578), 1,
      sym_update_clause,
    ACTIONS(385), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [4113] = 9,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    ACTIONS(98), 1,
      aux_sym_offset_clause_token1,
    STATE(128), 1,
      sym_formatting_comment,
    STATE(210), 1,
      sym_offset_clause,
    STATE(280), 1,
      sym_for_clause,
    STATE(569), 1,
      sym_update_clause,
    ACTIONS(387), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [4142] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(129), 1,
      sym_formatting_comment,
    ACTIONS(389), 8,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
      aux_sym_group_by_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [4159] = 9,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    ACTIONS(98), 1,
      aux_sym_offset_clause_token1,
    STATE(130), 1,
      sym_formatting_comment,
    STATE(211), 1,
      sym_offset_clause,
    STATE(284), 1,
      sym_for_clause,
    STATE(594), 1,
      sym_update_clause,
    ACTIONS(391), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [4188] = 9,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    ACTIONS(98), 1,
      aux_sym_offset_clause_token1,
    STATE(131), 1,
      sym_formatting_comment,
    STATE(212), 1,
      sym_offset_clause,
    STATE(287), 1,
      sym_for_clause,
    STATE(608), 1,
      sym_update_clause,
    ACTIONS(393), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [4217] = 9,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    ACTIONS(98), 1,
      aux_sym_offset_clause_token1,
    STATE(132), 1,
      sym_formatting_comment,
    STATE(196), 1,
      sym_offset_clause,
    STATE(379), 1,
      sym_for_clause,
    STATE(485), 1,
      sym_update_clause,
    ACTIONS(395), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [4246] = 9,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    ACTIONS(98), 1,
      aux_sym_offset_clause_token1,
    STATE(133), 1,
      sym_formatting_comment,
    STATE(241), 1,
      sym_offset_clause,
    STATE(370), 1,
      sym_for_clause,
    STATE(607), 1,
      sym_update_clause,
    ACTIONS(397), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [4275] = 9,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    ACTIONS(98), 1,
      aux_sym_offset_clause_token1,
    STATE(134), 1,
      sym_formatting_comment,
    STATE(242), 1,
      sym_offset_clause,
    STATE(373), 1,
      sym_for_clause,
    STATE(612), 1,
      sym_update_clause,
    ACTIONS(399), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [4304] = 9,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    ACTIONS(98), 1,
      aux_sym_offset_clause_token1,
    STATE(135), 1,
      sym_formatting_comment,
    STATE(190), 1,
      sym_offset_clause,
    STATE(299), 1,
      sym_for_clause,
    STATE(493), 1,
      sym_update_clause,
    ACTIONS(401), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [4333] = 9,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    ACTIONS(98), 1,
      aux_sym_offset_clause_token1,
    STATE(136), 1,
      sym_formatting_comment,
    STATE(245), 1,
      sym_offset_clause,
    STATE(378), 1,
      sym_for_clause,
    STATE(622), 1,
      sym_update_clause,
    ACTIONS(403), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [4362] = 9,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    ACTIONS(98), 1,
      aux_sym_offset_clause_token1,
    STATE(137), 1,
      sym_formatting_comment,
    STATE(246), 1,
      sym_offset_clause,
    STATE(383), 1,
      sym_for_clause,
    STATE(636), 1,
      sym_update_clause,
    ACTIONS(405), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [4391] = 9,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    ACTIONS(98), 1,
      aux_sym_offset_clause_token1,
    STATE(138), 1,
      sym_formatting_comment,
    STATE(247), 1,
      sym_offset_clause,
    STATE(389), 1,
      sym_for_clause,
    STATE(655), 1,
      sym_update_clause,
    ACTIONS(407), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [4420] = 9,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    ACTIONS(98), 1,
      aux_sym_offset_clause_token1,
    STATE(139), 1,
      sym_formatting_comment,
    STATE(215), 1,
      sym_offset_clause,
    STATE(294), 1,
      sym_for_clause,
    STATE(637), 1,
      sym_update_clause,
    ACTIONS(409), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [4449] = 9,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    ACTIONS(98), 1,
      aux_sym_offset_clause_token1,
    STATE(140), 1,
      sym_formatting_comment,
    STATE(187), 1,
      sym_offset_clause,
    STATE(278), 1,
      sym_for_clause,
    STATE(684), 1,
      sym_update_clause,
    ACTIONS(411), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [4478] = 9,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    ACTIONS(98), 1,
      aux_sym_offset_clause_token1,
    STATE(141), 1,
      sym_formatting_comment,
    STATE(218), 1,
      sym_offset_clause,
    STATE(301), 1,
      sym_for_clause,
    STATE(680), 1,
      sym_update_clause,
    ACTIONS(413), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [4507] = 9,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    ACTIONS(98), 1,
      aux_sym_offset_clause_token1,
    STATE(142), 1,
      sym_formatting_comment,
    STATE(219), 1,
      sym_offset_clause,
    STATE(303), 1,
      sym_for_clause,
    STATE(700), 1,
      sym_update_clause,
    ACTIONS(415), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [4536] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(419), 1,
      aux_sym_using_scope_type_token3,
    STATE(110), 1,
      sym_using_scope_type,
    STATE(143), 1,
      sym_formatting_comment,
    ACTIONS(417), 6,
      aux_sym_using_scope_type_token1,
      aux_sym_using_scope_type_token2,
      aux_sym_using_scope_type_token4,
      aux_sym_using_scope_type_token5,
      aux_sym_using_scope_type_token6,
      aux_sym_using_scope_type_token7,
  [4557] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(144), 1,
      sym_formatting_comment,
    ACTIONS(421), 8,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
      aux_sym_group_by_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [4574] = 9,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    ACTIONS(98), 1,
      aux_sym_offset_clause_token1,
    STATE(145), 1,
      sym_formatting_comment,
    STATE(248), 1,
      sym_offset_clause,
    STATE(399), 1,
      sym_for_clause,
    STATE(690), 1,
      sym_update_clause,
    ACTIONS(423), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [4603] = 9,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    ACTIONS(98), 1,
      aux_sym_offset_clause_token1,
    STATE(146), 1,
      sym_formatting_comment,
    STATE(238), 1,
      sym_offset_clause,
    STATE(358), 1,
      sym_for_clause,
    STATE(564), 1,
      sym_update_clause,
    ACTIONS(425), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [4632] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(147), 1,
      sym_formatting_comment,
    ACTIONS(427), 8,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
      aux_sym_group_by_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [4649] = 9,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    ACTIONS(98), 1,
      aux_sym_offset_clause_token1,
    STATE(148), 1,
      sym_formatting_comment,
    STATE(221), 1,
      sym_offset_clause,
    STATE(309), 1,
      sym_for_clause,
    STATE(733), 1,
      sym_update_clause,
    ACTIONS(429), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [4678] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(433), 1,
      anon_sym_COMMA,
    STATE(149), 2,
      sym_formatting_comment,
      aux_sym_order_by_clause_repeat1,
    ACTIONS(431), 6,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
  [4697] = 9,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    ACTIONS(98), 1,
      aux_sym_offset_clause_token1,
    STATE(150), 1,
      sym_formatting_comment,
    STATE(224), 1,
      sym_offset_clause,
    STATE(319), 1,
      sym_for_clause,
    STATE(476), 1,
      sym_update_clause,
    ACTIONS(436), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [4726] = 9,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    ACTIONS(98), 1,
      aux_sym_offset_clause_token1,
    STATE(151), 1,
      sym_formatting_comment,
    STATE(225), 1,
      sym_offset_clause,
    STATE(322), 1,
      sym_for_clause,
    STATE(481), 1,
      sym_update_clause,
    ACTIONS(438), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [4755] = 9,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    ACTIONS(98), 1,
      aux_sym_offset_clause_token1,
    STATE(152), 1,
      sym_formatting_comment,
    STATE(203), 1,
      sym_offset_clause,
    STATE(416), 1,
      sym_for_clause,
    STATE(506), 1,
      sym_update_clause,
    ACTIONS(440), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [4784] = 9,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(149), 1,
      aux_sym_function_expression_token1,
    ACTIONS(151), 1,
      sym_identifier,
    STATE(13), 1,
      sym_dotted_identifier,
    STATE(51), 1,
      sym__value_expression,
    STATE(153), 1,
      sym_formatting_comment,
    STATE(162), 1,
      sym_order_expression,
    STATE(801), 1,
      sym__function_name,
    STATE(17), 2,
      sym_function_expression,
      sym_field_identifier,
  [4813] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(444), 1,
      anon_sym_COMMA,
    STATE(149), 1,
      aux_sym_order_by_clause_repeat1,
    STATE(154), 1,
      sym_formatting_comment,
    ACTIONS(442), 6,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
  [4834] = 9,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    ACTIONS(98), 1,
      aux_sym_offset_clause_token1,
    STATE(155), 1,
      sym_formatting_comment,
    STATE(228), 1,
      sym_offset_clause,
    STATE(329), 1,
      sym_for_clause,
    STATE(495), 1,
      sym_update_clause,
    ACTIONS(446), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [4863] = 9,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    ACTIONS(98), 1,
      aux_sym_offset_clause_token1,
    STATE(156), 1,
      sym_formatting_comment,
    STATE(192), 1,
      sym_offset_clause,
    STATE(317), 1,
      sym_for_clause,
    STATE(544), 1,
      sym_update_clause,
    ACTIONS(448), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [4892] = 9,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    ACTIONS(98), 1,
      aux_sym_offset_clause_token1,
    STATE(157), 1,
      sym_formatting_comment,
    STATE(231), 1,
      sym_offset_clause,
    STATE(334), 1,
      sym_for_clause,
    STATE(508), 1,
      sym_update_clause,
    ACTIONS(450), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [4921] = 9,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    ACTIONS(98), 1,
      aux_sym_offset_clause_token1,
    STATE(158), 1,
      sym_formatting_comment,
    STATE(232), 1,
      sym_offset_clause,
    STATE(337), 1,
      sym_for_clause,
    STATE(514), 1,
      sym_update_clause,
    ACTIONS(452), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [4950] = 9,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(118), 1,
      aux_sym_function_expression_token1,
    ACTIONS(120), 1,
      sym_identifier,
    ACTIONS(454), 1,
      anon_sym_RPAREN,
    STATE(159), 1,
      sym_formatting_comment,
    STATE(332), 1,
      sym_dotted_identifier,
    STATE(455), 1,
      sym__value_expression,
    STATE(772), 1,
      sym__function_name,
    STATE(327), 2,
      sym_function_expression,
      sym_field_identifier,
  [4979] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(160), 1,
      sym_formatting_comment,
    ACTIONS(456), 8,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
      aux_sym_group_by_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [4996] = 9,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    ACTIONS(98), 1,
      aux_sym_offset_clause_token1,
    STATE(161), 1,
      sym_formatting_comment,
    STATE(252), 1,
      sym_offset_clause,
    STATE(384), 1,
      sym_for_clause,
    STATE(499), 1,
      sym_update_clause,
    ACTIONS(458), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [5025] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(444), 1,
      anon_sym_COMMA,
    STATE(154), 1,
      aux_sym_order_by_clause_repeat1,
    STATE(162), 1,
      sym_formatting_comment,
    ACTIONS(460), 6,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
  [5046] = 9,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    ACTIONS(98), 1,
      aux_sym_offset_clause_token1,
    STATE(163), 1,
      sym_formatting_comment,
    STATE(207), 1,
      sym_offset_clause,
    STATE(275), 1,
      sym_for_clause,
    STATE(540), 1,
      sym_update_clause,
    ACTIONS(462), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [5075] = 9,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    ACTIONS(98), 1,
      aux_sym_offset_clause_token1,
    STATE(164), 1,
      sym_formatting_comment,
    STATE(234), 1,
      sym_offset_clause,
    STATE(343), 1,
      sym_for_clause,
    STATE(527), 1,
      sym_update_clause,
    ACTIONS(464), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [5104] = 9,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    ACTIONS(98), 1,
      aux_sym_offset_clause_token1,
    STATE(165), 1,
      sym_formatting_comment,
    STATE(235), 1,
      sym_offset_clause,
    STATE(349), 1,
      sym_for_clause,
    STATE(546), 1,
      sym_update_clause,
    ACTIONS(466), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [5133] = 9,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    ACTIONS(98), 1,
      aux_sym_offset_clause_token1,
    STATE(166), 1,
      sym_formatting_comment,
    STATE(236), 1,
      sym_offset_clause,
    STATE(352), 1,
      sym_for_clause,
    STATE(551), 1,
      sym_update_clause,
    ACTIONS(468), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [5162] = 10,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(49), 1,
      anon_sym_DOT,
    ACTIONS(470), 1,
      aux_sym_type_of_clause_token2,
    ACTIONS(472), 1,
      aux_sym_when_expression_token1,
    ACTIONS(474), 1,
      aux_sym_else_expression_token1,
    STATE(9), 1,
      aux_sym_dotted_identifier_repeat1,
    STATE(167), 1,
      sym_formatting_comment,
    STATE(222), 1,
      aux_sym_type_of_clause_repeat1,
    STATE(424), 1,
      sym_when_expression,
    STATE(767), 1,
      sym_else_expression,
  [5193] = 9,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    ACTIONS(98), 1,
      aux_sym_offset_clause_token1,
    STATE(168), 1,
      sym_formatting_comment,
    STATE(250), 1,
      sym_offset_clause,
    STATE(341), 1,
      sym_for_clause,
    STATE(694), 1,
      sym_update_clause,
    ACTIONS(476), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [5222] = 9,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    ACTIONS(98), 1,
      aux_sym_offset_clause_token1,
    STATE(169), 1,
      sym_formatting_comment,
    STATE(229), 1,
      sym_offset_clause,
    STATE(328), 1,
      sym_for_clause,
    STATE(517), 1,
      sym_update_clause,
    ACTIONS(478), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [5251] = 9,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(149), 1,
      aux_sym_function_expression_token1,
    ACTIONS(151), 1,
      sym_identifier,
    STATE(13), 1,
      sym_dotted_identifier,
    STATE(51), 1,
      sym__value_expression,
    STATE(170), 1,
      sym_formatting_comment,
    STATE(176), 1,
      sym_order_expression,
    STATE(801), 1,
      sym__function_name,
    STATE(17), 2,
      sym_function_expression,
      sym_field_identifier,
  [5280] = 8,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(118), 1,
      aux_sym_function_expression_token1,
    ACTIONS(120), 1,
      sym_identifier,
    STATE(171), 1,
      sym_formatting_comment,
    STATE(332), 1,
      sym_dotted_identifier,
    STATE(456), 1,
      sym__value_expression,
    STATE(772), 1,
      sym__function_name,
    STATE(327), 2,
      sym_function_expression,
      sym_field_identifier,
  [5306] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(172), 1,
      sym_formatting_comment,
    ACTIONS(480), 7,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [5322] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(173), 1,
      sym_formatting_comment,
    ACTIONS(482), 7,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
      anon_sym_COMMA,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
  [5338] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(174), 1,
      sym_formatting_comment,
    ACTIONS(361), 7,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
      anon_sym_COMMA,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
  [5354] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(175), 1,
      sym_formatting_comment,
    ACTIONS(484), 7,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [5370] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(176), 1,
      sym_formatting_comment,
    ACTIONS(431), 7,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
      anon_sym_COMMA,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
  [5386] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(177), 1,
      sym_formatting_comment,
    ACTIONS(486), 7,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
      anon_sym_COMMA,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
  [5402] = 9,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(5), 1,
      anon_sym_SLASH_SLASH,
    ACTIONS(7), 1,
      aux_sym_select_clause_token1,
    STATE(178), 1,
      sym_formatting_comment,
    STATE(310), 1,
      aux_sym_source_file_repeat1,
    STATE(488), 1,
      sym_select_clause,
    STATE(726), 1,
      sym_header_comment,
    STATE(756), 1,
      sym__soql_query_expression,
    STATE(780), 1,
      sym_soql_query_body,
  [5430] = 8,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(149), 1,
      aux_sym_function_expression_token1,
    ACTIONS(151), 1,
      sym_identifier,
    STATE(13), 1,
      sym_dotted_identifier,
    STATE(125), 1,
      sym__group_by_expression,
    STATE(179), 1,
      sym_formatting_comment,
    STATE(801), 1,
      sym__function_name,
    STATE(107), 2,
      sym_function_expression,
      sym_field_identifier,
  [5456] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(49), 1,
      anon_sym_DOT,
    ACTIONS(488), 1,
      anon_sym_COMMA,
    STATE(9), 1,
      aux_sym_dotted_identifier_repeat1,
    STATE(180), 1,
      sym_formatting_comment,
    STATE(260), 1,
      aux_sym_field_list_repeat1,
    ACTIONS(490), 3,
      aux_sym_type_of_clause_token2,
      aux_sym_when_expression_token1,
      aux_sym_else_expression_token1,
  [5480] = 8,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(118), 1,
      aux_sym_function_expression_token1,
    ACTIONS(120), 1,
      sym_identifier,
    STATE(181), 1,
      sym_formatting_comment,
    STATE(332), 1,
      sym_dotted_identifier,
    STATE(455), 1,
      sym__value_expression,
    STATE(772), 1,
      sym__function_name,
    STATE(327), 2,
      sym_function_expression,
      sym_field_identifier,
  [5506] = 8,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(118), 1,
      aux_sym_function_expression_token1,
    ACTIONS(120), 1,
      sym_identifier,
    STATE(182), 1,
      sym_formatting_comment,
    STATE(332), 1,
      sym_dotted_identifier,
    STATE(591), 1,
      sym__value_expression,
    STATE(772), 1,
      sym__function_name,
    STATE(327), 2,
      sym_function_expression,
      sym_field_identifier,
  [5532] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(62), 1,
      anon_sym_LPAREN,
    ACTIONS(492), 1,
      anon_sym_DOT,
    STATE(183), 1,
      sym_formatting_comment,
    STATE(195), 1,
      aux_sym_dotted_identifier_repeat1,
    ACTIONS(60), 2,
      anon_sym_RPAREN,
      anon_sym_COMMA,
    ACTIONS(64), 2,
      aux_sym_from_clause_token1,
      sym_identifier,
  [5556] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(184), 1,
      sym_formatting_comment,
    STATE(360), 1,
      sym_for_clause,
    STATE(566), 1,
      sym_update_clause,
    ACTIONS(494), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [5579] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(185), 1,
      sym_formatting_comment,
    STATE(288), 1,
      sym_for_clause,
    STATE(611), 1,
      sym_update_clause,
    ACTIONS(496), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [5602] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(186), 1,
      sym_formatting_comment,
    STATE(296), 1,
      sym_for_clause,
    STATE(647), 1,
      sym_update_clause,
    ACTIONS(498), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [5625] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(187), 1,
      sym_formatting_comment,
    STATE(300), 1,
      sym_for_clause,
    STATE(671), 1,
      sym_update_clause,
    ACTIONS(500), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [5648] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(188), 1,
      sym_formatting_comment,
    STATE(302), 1,
      sym_for_clause,
    STATE(688), 1,
      sym_update_clause,
    ACTIONS(502), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [5671] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(189), 1,
      sym_formatting_comment,
    STATE(305), 1,
      sym_for_clause,
    STATE(714), 1,
      sym_update_clause,
    ACTIONS(504), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [5694] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(190), 1,
      sym_formatting_comment,
    STATE(308), 1,
      sym_for_clause,
    STATE(729), 1,
      sym_update_clause,
    ACTIONS(506), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [5717] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(191), 1,
      sym_formatting_comment,
    STATE(311), 1,
      sym_for_clause,
    STATE(735), 1,
      sym_update_clause,
    ACTIONS(508), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [5740] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(192), 1,
      sym_formatting_comment,
    STATE(312), 1,
      sym_for_clause,
    STATE(740), 1,
      sym_update_clause,
    ACTIONS(510), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [5763] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(512), 1,
      anon_sym_DOT,
    ACTIONS(53), 2,
      anon_sym_RPAREN,
      anon_sym_COMMA,
    ACTIONS(58), 2,
      aux_sym_from_clause_token1,
      sym_identifier,
    STATE(193), 2,
      sym_formatting_comment,
      aux_sym_dotted_identifier_repeat1,
  [5782] = 8,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(470), 1,
      aux_sym_type_of_clause_token2,
    ACTIONS(472), 1,
      aux_sym_when_expression_token1,
    ACTIONS(474), 1,
      aux_sym_else_expression_token1,
    STATE(194), 1,
      sym_formatting_comment,
    STATE(222), 1,
      aux_sym_type_of_clause_repeat1,
    STATE(424), 1,
      sym_when_expression,
    STATE(767), 1,
      sym_else_expression,
  [5807] = 6,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(492), 1,
      anon_sym_DOT,
    STATE(193), 1,
      aux_sym_dotted_identifier_repeat1,
    STATE(195), 1,
      sym_formatting_comment,
    ACTIONS(47), 2,
      anon_sym_RPAREN,
      anon_sym_COMMA,
    ACTIONS(51), 2,
      aux_sym_from_clause_token1,
      sym_identifier,
  [5828] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(196), 1,
      sym_formatting_comment,
    STATE(318), 1,
      sym_for_clause,
    STATE(471), 1,
      sym_update_clause,
    ACTIONS(515), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [5851] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(197), 1,
      sym_formatting_comment,
    STATE(359), 1,
      sym_for_clause,
    STATE(743), 1,
      sym_update_clause,
    ACTIONS(517), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [5874] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(198), 1,
      sym_formatting_comment,
    STATE(321), 1,
      sym_for_clause,
    STATE(477), 1,
      sym_update_clause,
    ACTIONS(519), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [5897] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(199), 1,
      sym_formatting_comment,
    STATE(324), 1,
      sym_for_clause,
    STATE(483), 1,
      sym_update_clause,
    ACTIONS(521), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [5920] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(200), 1,
      sym_formatting_comment,
    STATE(390), 1,
      sym_for_clause,
    STATE(470), 1,
      sym_update_clause,
    ACTIONS(523), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [5943] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(201), 1,
      sym_formatting_comment,
    STATE(268), 1,
      sym_for_clause,
    STATE(497), 1,
      sym_update_clause,
    ACTIONS(525), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [5966] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(202), 1,
      sym_formatting_comment,
    STATE(400), 1,
      sym_for_clause,
    STATE(502), 1,
      sym_update_clause,
    ACTIONS(527), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [5989] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(203), 1,
      sym_formatting_comment,
    STATE(333), 1,
      sym_for_clause,
    STATE(503), 1,
      sym_update_clause,
    ACTIONS(529), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [6012] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(204), 1,
      sym_formatting_comment,
    STATE(336), 1,
      sym_for_clause,
    STATE(510), 1,
      sym_update_clause,
    ACTIONS(531), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [6035] = 6,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(533), 1,
      aux_sym_using_scope_clause_token1,
    ACTIONS(535), 1,
      aux_sym_using_lookup_clause_token1,
    ACTIONS(537), 1,
      aux_sym_using_listview_clause_token1,
    STATE(205), 1,
      sym_formatting_comment,
    STATE(100), 3,
      sym_using_scope_clause,
      sym_using_lookup_clause,
      sym_using_listview_clause,
  [6056] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(206), 1,
      sym_formatting_comment,
    STATE(339), 1,
      sym_for_clause,
    STATE(515), 1,
      sym_update_clause,
    ACTIONS(539), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [6079] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(207), 1,
      sym_formatting_comment,
    STATE(342), 1,
      sym_for_clause,
    STATE(522), 1,
      sym_update_clause,
    ACTIONS(541), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [6102] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(208), 1,
      sym_formatting_comment,
    STATE(269), 1,
      sym_for_clause,
    STATE(509), 1,
      sym_update_clause,
    ACTIONS(543), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [6125] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(209), 1,
      sym_formatting_comment,
    STATE(344), 1,
      sym_for_clause,
    STATE(529), 1,
      sym_update_clause,
    ACTIONS(545), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [6148] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(210), 1,
      sym_formatting_comment,
    STATE(345), 1,
      sym_for_clause,
    STATE(467), 1,
      sym_update_clause,
    ACTIONS(547), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [6171] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(211), 1,
      sym_formatting_comment,
    STATE(347), 1,
      sym_for_clause,
    STATE(537), 1,
      sym_update_clause,
    ACTIONS(549), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [6194] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(212), 1,
      sym_formatting_comment,
    STATE(348), 1,
      sym_for_clause,
    STATE(541), 1,
      sym_update_clause,
    ACTIONS(551), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [6217] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(213), 1,
      sym_formatting_comment,
    STATE(351), 1,
      sym_for_clause,
    STATE(548), 1,
      sym_update_clause,
    ACTIONS(553), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [6240] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(214), 1,
      sym_formatting_comment,
    STATE(354), 1,
      sym_for_clause,
    STATE(553), 1,
      sym_update_clause,
    ACTIONS(555), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [6263] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(215), 1,
      sym_formatting_comment,
    STATE(357), 1,
      sym_for_clause,
    STATE(560), 1,
      sym_update_clause,
    ACTIONS(557), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [6286] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(216), 1,
      sym_formatting_comment,
    STATE(281), 1,
      sym_for_clause,
    STATE(718), 1,
      sym_update_clause,
    ACTIONS(559), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [6309] = 8,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(561), 1,
      aux_sym_geo_location_type_token1,
    ACTIONS(563), 1,
      sym_bound_apex_expression,
    ACTIONS(565), 1,
      sym_identifier,
    STATE(217), 1,
      sym_formatting_comment,
    STATE(332), 1,
      sym_dotted_identifier,
    STATE(762), 1,
      sym_field_identifier,
    STATE(805), 1,
      sym_geo_location_type,
  [6334] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(218), 1,
      sym_formatting_comment,
    STATE(361), 1,
      sym_for_clause,
    STATE(570), 1,
      sym_update_clause,
    ACTIONS(567), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [6357] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(219), 1,
      sym_formatting_comment,
    STATE(362), 1,
      sym_for_clause,
    STATE(573), 1,
      sym_update_clause,
    ACTIONS(569), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [6380] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(220), 1,
      sym_formatting_comment,
    STATE(365), 1,
      sym_for_clause,
    STATE(579), 1,
      sym_update_clause,
    ACTIONS(571), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [6403] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(221), 1,
      sym_formatting_comment,
    STATE(366), 1,
      sym_for_clause,
    STATE(583), 1,
      sym_update_clause,
    ACTIONS(573), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [6426] = 8,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(472), 1,
      aux_sym_when_expression_token1,
    ACTIONS(474), 1,
      aux_sym_else_expression_token1,
    ACTIONS(575), 1,
      aux_sym_type_of_clause_token2,
    STATE(222), 1,
      sym_formatting_comment,
    STATE(258), 1,
      aux_sym_type_of_clause_repeat1,
    STATE(424), 1,
      sym_when_expression,
    STATE(761), 1,
      sym_else_expression,
  [6451] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(223), 1,
      sym_formatting_comment,
    STATE(304), 1,
      sym_for_clause,
    STATE(496), 1,
      sym_update_clause,
    ACTIONS(577), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [6474] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(224), 1,
      sym_formatting_comment,
    STATE(368), 1,
      sym_for_clause,
    STATE(598), 1,
      sym_update_clause,
    ACTIONS(579), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [6497] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(225), 1,
      sym_formatting_comment,
    STATE(369), 1,
      sym_for_clause,
    STATE(602), 1,
      sym_update_clause,
    ACTIONS(581), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [6520] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(226), 1,
      sym_formatting_comment,
    STATE(372), 1,
      sym_for_clause,
    STATE(609), 1,
      sym_update_clause,
    ACTIONS(583), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [6543] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(227), 1,
      sym_formatting_comment,
    STATE(374), 1,
      sym_for_clause,
    STATE(613), 1,
      sym_update_clause,
    ACTIONS(585), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [6566] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(228), 1,
      sym_formatting_comment,
    STATE(377), 1,
      sym_for_clause,
    STATE(619), 1,
      sym_update_clause,
    ACTIONS(587), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [6589] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(229), 1,
      sym_formatting_comment,
    STATE(314), 1,
      sym_for_clause,
    STATE(518), 1,
      sym_update_clause,
    ACTIONS(589), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [6612] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(230), 1,
      sym_formatting_comment,
    STATE(380), 1,
      sym_for_clause,
    STATE(624), 1,
      sym_update_clause,
    ACTIONS(591), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [6635] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(231), 1,
      sym_formatting_comment,
    STATE(381), 1,
      sym_for_clause,
    STATE(628), 1,
      sym_update_clause,
    ACTIONS(593), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [6658] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(232), 1,
      sym_formatting_comment,
    STATE(382), 1,
      sym_for_clause,
    STATE(632), 1,
      sym_update_clause,
    ACTIONS(595), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [6681] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(233), 1,
      sym_formatting_comment,
    STATE(385), 1,
      sym_for_clause,
    STATE(638), 1,
      sym_update_clause,
    ACTIONS(597), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [6704] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(234), 1,
      sym_formatting_comment,
    STATE(386), 1,
      sym_for_clause,
    STATE(641), 1,
      sym_update_clause,
    ACTIONS(599), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [6727] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(235), 1,
      sym_formatting_comment,
    STATE(387), 1,
      sym_for_clause,
    STATE(648), 1,
      sym_update_clause,
    ACTIONS(601), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [6750] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(236), 1,
      sym_formatting_comment,
    STATE(388), 1,
      sym_for_clause,
    STATE(651), 1,
      sym_update_clause,
    ACTIONS(603), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [6773] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(237), 1,
      sym_formatting_comment,
    STATE(391), 1,
      sym_for_clause,
    STATE(657), 1,
      sym_update_clause,
    ACTIONS(605), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [6796] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(238), 1,
      sym_formatting_comment,
    STATE(392), 1,
      sym_for_clause,
    STATE(661), 1,
      sym_update_clause,
    ACTIONS(607), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [6819] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(239), 1,
      sym_formatting_comment,
    STATE(393), 1,
      sym_for_clause,
    STATE(667), 1,
      sym_update_clause,
    ACTIONS(609), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [6842] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(240), 1,
      sym_formatting_comment,
    STATE(323), 1,
      sym_for_clause,
    STATE(550), 1,
      sym_update_clause,
    ACTIONS(611), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [6865] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(241), 1,
      sym_formatting_comment,
    STATE(397), 1,
      sym_for_clause,
    STATE(681), 1,
      sym_update_clause,
    ACTIONS(613), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [6888] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(242), 1,
      sym_formatting_comment,
    STATE(398), 1,
      sym_for_clause,
    STATE(685), 1,
      sym_update_clause,
    ACTIONS(615), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [6911] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(243), 1,
      sym_formatting_comment,
    STATE(276), 1,
      sym_for_clause,
    STATE(545), 1,
      sym_update_clause,
    ACTIONS(617), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [6934] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(244), 1,
      sym_formatting_comment,
    STATE(401), 1,
      sym_for_clause,
    STATE(691), 1,
      sym_update_clause,
    ACTIONS(619), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [6957] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(245), 1,
      sym_formatting_comment,
    STATE(402), 1,
      sym_for_clause,
    STATE(695), 1,
      sym_update_clause,
    ACTIONS(621), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [6980] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(246), 1,
      sym_formatting_comment,
    STATE(403), 1,
      sym_for_clause,
    STATE(701), 1,
      sym_update_clause,
    ACTIONS(623), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [7003] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(247), 1,
      sym_formatting_comment,
    STATE(404), 1,
      sym_for_clause,
    STATE(708), 1,
      sym_update_clause,
    ACTIONS(625), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [7026] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(248), 1,
      sym_formatting_comment,
    STATE(405), 1,
      sym_for_clause,
    STATE(719), 1,
      sym_update_clause,
    ACTIONS(627), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [7049] = 8,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(561), 1,
      aux_sym_geo_location_type_token1,
    ACTIONS(563), 1,
      sym_bound_apex_expression,
    ACTIONS(565), 1,
      sym_identifier,
    STATE(249), 1,
      sym_formatting_comment,
    STATE(332), 1,
      sym_dotted_identifier,
    STATE(762), 1,
      sym_field_identifier,
    STATE(770), 1,
      sym_geo_location_type,
  [7074] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(250), 1,
      sym_formatting_comment,
    STATE(279), 1,
      sym_for_clause,
    STATE(556), 1,
      sym_update_clause,
    ACTIONS(629), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [7097] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(251), 1,
      sym_formatting_comment,
    STATE(282), 1,
      sym_for_clause,
    STATE(576), 1,
      sym_update_clause,
    ACTIONS(631), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [7120] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(252), 1,
      sym_formatting_comment,
    STATE(283), 1,
      sym_for_clause,
    STATE(582), 1,
      sym_update_clause,
    ACTIONS(633), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [7143] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(253), 1,
      sym_formatting_comment,
    STATE(356), 1,
      sym_for_clause,
    STATE(586), 1,
      sym_update_clause,
    ACTIONS(635), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [7166] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_for_clause_token1,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(254), 1,
      sym_formatting_comment,
    STATE(286), 1,
      sym_for_clause,
    STATE(597), 1,
      sym_update_clause,
    ACTIONS(637), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [7189] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(149), 1,
      aux_sym_function_expression_token1,
    ACTIONS(151), 1,
      sym_identifier,
    STATE(13), 1,
      sym_dotted_identifier,
    STATE(255), 1,
      sym_formatting_comment,
    STATE(801), 1,
      sym__function_name,
    STATE(119), 2,
      sym_function_expression,
      sym_field_identifier,
  [7212] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(49), 1,
      anon_sym_DOT,
    STATE(9), 1,
      aux_sym_dotted_identifier_repeat1,
    STATE(256), 1,
      sym_formatting_comment,
    ACTIONS(639), 4,
      anon_sym_COMMA,
      aux_sym_type_of_clause_token2,
      aux_sym_when_expression_token1,
      aux_sym_else_expression_token1,
  [7231] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(488), 1,
      anon_sym_COMMA,
    STATE(257), 1,
      sym_formatting_comment,
    STATE(260), 1,
      aux_sym_field_list_repeat1,
    ACTIONS(490), 3,
      aux_sym_type_of_clause_token2,
      aux_sym_when_expression_token1,
      aux_sym_else_expression_token1,
  [7249] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(643), 1,
      aux_sym_when_expression_token1,
    STATE(424), 1,
      sym_when_expression,
    ACTIONS(641), 2,
      aux_sym_type_of_clause_token2,
      aux_sym_else_expression_token1,
    STATE(258), 2,
      sym_formatting_comment,
      aux_sym_type_of_clause_repeat1,
  [7267] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(259), 1,
      sym_formatting_comment,
    ACTIONS(58), 2,
      aux_sym_from_clause_token1,
      sym_identifier,
    ACTIONS(53), 3,
      anon_sym_RPAREN,
      anon_sym_COMMA,
      anon_sym_DOT,
  [7283] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(488), 1,
      anon_sym_COMMA,
    STATE(260), 1,
      sym_formatting_comment,
    STATE(265), 1,
      aux_sym_field_list_repeat1,
    ACTIONS(646), 3,
      aux_sym_type_of_clause_token2,
      aux_sym_when_expression_token1,
      aux_sym_else_expression_token1,
  [7301] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(650), 1,
      anon_sym_COMMA,
    STATE(261), 2,
      sym_formatting_comment,
      aux_sym_for_clause_repeat1,
    ACTIONS(648), 3,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
      aux_sym_for_type_token1,
  [7317] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(655), 1,
      anon_sym_COMMA,
    STATE(262), 1,
      sym_formatting_comment,
    STATE(264), 1,
      aux_sym_for_clause_repeat1,
    ACTIONS(653), 3,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
      aux_sym_for_type_token1,
  [7335] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(196), 1,
      anon_sym_RPAREN,
    ACTIONS(198), 1,
      aux_sym_and_expression_token1,
    ACTIONS(657), 1,
      aux_sym_or_expression_token1,
    STATE(74), 1,
      aux_sym_and_expression_repeat1,
    STATE(263), 1,
      sym_formatting_comment,
    STATE(466), 1,
      aux_sym_or_expression_repeat1,
  [7357] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(655), 1,
      anon_sym_COMMA,
    STATE(261), 1,
      aux_sym_for_clause_repeat1,
    STATE(264), 1,
      sym_formatting_comment,
    ACTIONS(659), 3,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
      aux_sym_for_type_token1,
  [7375] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(661), 1,
      anon_sym_COMMA,
    STATE(265), 2,
      sym_formatting_comment,
      aux_sym_field_list_repeat1,
    ACTIONS(639), 3,
      aux_sym_type_of_clause_token2,
      aux_sym_when_expression_token1,
      aux_sym_else_expression_token1,
  [7391] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(266), 1,
      sym_formatting_comment,
    ACTIONS(664), 5,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_offset_clause_token1,
  [7405] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(668), 1,
      aux_sym_with_data_cat_filter_type_token2,
    STATE(267), 1,
      sym_formatting_comment,
    STATE(739), 1,
      sym_with_data_cat_filter_type,
    ACTIONS(666), 3,
      aux_sym_with_data_cat_filter_type_token1,
      aux_sym_with_data_cat_filter_type_token3,
      aux_sym_with_data_cat_filter_type_token4,
  [7423] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(268), 1,
      sym_formatting_comment,
    STATE(620), 1,
      sym_update_clause,
    ACTIONS(670), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [7440] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(269), 1,
      sym_formatting_comment,
    STATE(504), 1,
      sym_update_clause,
    ACTIONS(672), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [7457] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(270), 1,
      sym_formatting_comment,
    STATE(505), 1,
      sym_update_clause,
    ACTIONS(674), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [7474] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(271), 1,
      sym_formatting_comment,
    STATE(631), 1,
      sym_update_clause,
    ACTIONS(676), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [7491] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(272), 1,
      sym_formatting_comment,
    STATE(511), 1,
      sym_update_clause,
    ACTIONS(678), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [7508] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(273), 1,
      sym_formatting_comment,
    ACTIONS(680), 4,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
      anon_sym_COMMA,
      aux_sym_for_type_token1,
  [7521] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(682), 1,
      aux_sym_with_record_visibility_param_token1,
    STATE(274), 1,
      sym_formatting_comment,
    STATE(465), 1,
      sym_with_record_visibility_param,
    ACTIONS(684), 2,
      aux_sym_with_record_visibility_param_token2,
      aux_sym_with_record_visibility_param_token3,
  [7538] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(275), 1,
      sym_formatting_comment,
    STATE(520), 1,
      sym_update_clause,
    ACTIONS(686), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [7555] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(276), 1,
      sym_formatting_comment,
    STATE(523), 1,
      sym_update_clause,
    ACTIONS(688), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [7572] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(277), 1,
      sym_formatting_comment,
    STATE(524), 1,
      sym_update_clause,
    ACTIONS(690), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [7589] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(278), 1,
      sym_formatting_comment,
    STATE(660), 1,
      sym_update_clause,
    ACTIONS(692), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [7606] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(279), 1,
      sym_formatting_comment,
    STATE(530), 1,
      sym_update_clause,
    ACTIONS(694), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [7623] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(280), 1,
      sym_formatting_comment,
    STATE(531), 1,
      sym_update_clause,
    ACTIONS(696), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [7640] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(281), 1,
      sym_formatting_comment,
    STATE(672), 1,
      sym_update_clause,
    ACTIONS(698), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [7657] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(282), 1,
      sym_formatting_comment,
    STATE(533), 1,
      sym_update_clause,
    ACTIONS(700), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [7674] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(283), 1,
      sym_formatting_comment,
    STATE(534), 1,
      sym_update_clause,
    ACTIONS(702), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [7691] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(284), 1,
      sym_formatting_comment,
    STATE(535), 1,
      sym_update_clause,
    ACTIONS(704), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [7708] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(285), 1,
      sym_formatting_comment,
    STATE(675), 1,
      sym_update_clause,
    ACTIONS(706), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [7725] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(286), 1,
      sym_formatting_comment,
    STATE(538), 1,
      sym_update_clause,
    ACTIONS(708), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [7742] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(287), 1,
      sym_formatting_comment,
    STATE(539), 1,
      sym_update_clause,
    ACTIONS(710), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [7759] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(288), 1,
      sym_formatting_comment,
    STATE(542), 1,
      sym_update_clause,
    ACTIONS(712), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [7776] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(289), 1,
      sym_formatting_comment,
    STATE(543), 1,
      sym_update_clause,
    ACTIONS(714), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [7793] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(290), 1,
      sym_formatting_comment,
    STATE(615), 1,
      sym_update_clause,
    ACTIONS(716), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [7810] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(291), 1,
      sym_formatting_comment,
    STATE(549), 1,
      sym_update_clause,
    ACTIONS(718), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [7827] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(262), 1,
      sym_for_type,
    STATE(292), 1,
      sym_formatting_comment,
    ACTIONS(720), 3,
      aux_sym_for_type_token1,
      aux_sym_for_type_token2,
      aux_sym_for_type_token3,
  [7842] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(293), 1,
      sym_formatting_comment,
    STATE(689), 1,
      sym_update_clause,
    ACTIONS(722), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [7859] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(294), 1,
      sym_formatting_comment,
    STATE(558), 1,
      sym_update_clause,
    ACTIONS(724), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [7876] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(728), 1,
      anon_sym_COMMA,
    STATE(295), 1,
      sym_formatting_comment,
    STATE(396), 1,
      aux_sym_update_clause_repeat1,
    ACTIONS(726), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [7893] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(296), 1,
      sym_formatting_comment,
    STATE(561), 1,
      sym_update_clause,
    ACTIONS(730), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [7910] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(297), 1,
      sym_formatting_comment,
    STATE(562), 1,
      sym_update_clause,
    ACTIONS(732), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [7927] = 6,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(734), 1,
      sym_identifier,
    STATE(38), 1,
      sym_storage_identifier,
    STATE(40), 1,
      sym_dotted_identifier,
    STATE(55), 1,
      sym_storage_alias,
    STATE(298), 1,
      sym_formatting_comment,
  [7946] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(299), 1,
      sym_formatting_comment,
    STATE(728), 1,
      sym_update_clause,
    ACTIONS(736), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [7963] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(300), 1,
      sym_formatting_comment,
    STATE(567), 1,
      sym_update_clause,
    ACTIONS(738), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [7980] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(301), 1,
      sym_formatting_comment,
    STATE(568), 1,
      sym_update_clause,
    ACTIONS(740), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [7997] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(302), 1,
      sym_formatting_comment,
    STATE(571), 1,
      sym_update_clause,
    ACTIONS(742), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [8014] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(303), 1,
      sym_formatting_comment,
    STATE(572), 1,
      sym_update_clause,
    ACTIONS(744), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [8031] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(304), 1,
      sym_formatting_comment,
    STATE(731), 1,
      sym_update_clause,
    ACTIONS(746), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [8048] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(305), 1,
      sym_formatting_comment,
    STATE(574), 1,
      sym_update_clause,
    ACTIONS(748), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [8065] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(306), 1,
      sym_formatting_comment,
    STATE(575), 1,
      sym_update_clause,
    ACTIONS(750), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [8082] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(307), 1,
      sym_formatting_comment,
    STATE(732), 1,
      sym_update_clause,
    ACTIONS(752), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [8099] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(308), 1,
      sym_formatting_comment,
    STATE(580), 1,
      sym_update_clause,
    ACTIONS(754), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [8116] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(309), 1,
      sym_formatting_comment,
    STATE(581), 1,
      sym_update_clause,
    ACTIONS(756), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [8133] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(758), 1,
      anon_sym_SLASH_SLASH,
    ACTIONS(761), 1,
      aux_sym_select_clause_token1,
    STATE(726), 1,
      sym_header_comment,
    STATE(310), 2,
      sym_formatting_comment,
      aux_sym_source_file_repeat1,
  [8150] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(311), 1,
      sym_formatting_comment,
    STATE(584), 1,
      sym_update_clause,
    ACTIONS(763), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [8167] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(312), 1,
      sym_formatting_comment,
    STATE(585), 1,
      sym_update_clause,
    ACTIONS(765), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [8184] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(313), 1,
      sym_formatting_comment,
    ACTIONS(639), 4,
      anon_sym_COMMA,
      aux_sym_type_of_clause_token2,
      aux_sym_when_expression_token1,
      aux_sym_else_expression_token1,
  [8197] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(314), 1,
      sym_formatting_comment,
    STATE(737), 1,
      sym_update_clause,
    ACTIONS(767), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [8214] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(682), 1,
      aux_sym_with_record_visibility_param_token1,
    STATE(315), 1,
      sym_formatting_comment,
    STATE(593), 1,
      sym_with_record_visibility_param,
    ACTIONS(684), 2,
      aux_sym_with_record_visibility_param_token2,
      aux_sym_with_record_visibility_param_token3,
  [8231] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(316), 1,
      sym_formatting_comment,
    STATE(480), 1,
      sym_update_clause,
    ACTIONS(769), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [8248] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(317), 1,
      sym_formatting_comment,
    STATE(738), 1,
      sym_update_clause,
    ACTIONS(771), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [8265] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(318), 1,
      sym_formatting_comment,
    STATE(595), 1,
      sym_update_clause,
    ACTIONS(773), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [8282] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(319), 1,
      sym_formatting_comment,
    STATE(596), 1,
      sym_update_clause,
    ACTIONS(775), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [8299] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(320), 1,
      sym_formatting_comment,
    STATE(532), 1,
      sym_update_clause,
    ACTIONS(777), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [8316] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(321), 1,
      sym_formatting_comment,
    STATE(599), 1,
      sym_update_clause,
    ACTIONS(779), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [8333] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(322), 1,
      sym_formatting_comment,
    STATE(600), 1,
      sym_update_clause,
    ACTIONS(781), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [8350] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(323), 1,
      sym_formatting_comment,
    STATE(741), 1,
      sym_update_clause,
    ACTIONS(783), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [8367] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(324), 1,
      sym_formatting_comment,
    STATE(603), 1,
      sym_update_clause,
    ACTIONS(785), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [8384] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(325), 1,
      sym_formatting_comment,
    STATE(604), 1,
      sym_update_clause,
    ACTIONS(787), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [8401] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(326), 1,
      sym_formatting_comment,
    STATE(610), 1,
      sym_update_clause,
    ACTIONS(789), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [8418] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(327), 1,
      sym_formatting_comment,
    ACTIONS(78), 2,
      anon_sym_RPAREN,
      anon_sym_COMMA,
    ACTIONS(80), 2,
      aux_sym_from_clause_token1,
      sym_identifier,
  [8433] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(328), 1,
      sym_formatting_comment,
    STATE(512), 1,
      sym_update_clause,
    ACTIONS(791), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [8450] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(329), 1,
      sym_formatting_comment,
    STATE(618), 1,
      sym_update_clause,
    ACTIONS(793), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [8467] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(330), 1,
      sym_formatting_comment,
    ACTIONS(66), 2,
      anon_sym_RPAREN,
      anon_sym_COMMA,
    ACTIONS(68), 2,
      aux_sym_from_clause_token1,
      sym_identifier,
  [8482] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(331), 1,
      sym_formatting_comment,
    STATE(621), 1,
      sym_update_clause,
    ACTIONS(795), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [8499] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(332), 1,
      sym_formatting_comment,
    ACTIONS(60), 2,
      anon_sym_RPAREN,
      anon_sym_COMMA,
    ACTIONS(64), 2,
      aux_sym_from_clause_token1,
      sym_identifier,
  [8514] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(333), 1,
      sym_formatting_comment,
    STATE(625), 1,
      sym_update_clause,
    ACTIONS(797), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [8531] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(334), 1,
      sym_formatting_comment,
    STATE(626), 1,
      sym_update_clause,
    ACTIONS(799), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [8548] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(335), 1,
      sym_formatting_comment,
    STATE(793), 1,
      sym_fields_type,
    ACTIONS(801), 3,
      aux_sym_fields_type_token1,
      aux_sym_fields_type_token2,
      aux_sym_fields_type_token3,
  [8563] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(336), 1,
      sym_formatting_comment,
    STATE(629), 1,
      sym_update_clause,
    ACTIONS(803), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [8580] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(337), 1,
      sym_formatting_comment,
    STATE(630), 1,
      sym_update_clause,
    ACTIONS(805), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [8597] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(338), 1,
      sym_formatting_comment,
    ACTIONS(648), 4,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
      anon_sym_COMMA,
      aux_sym_for_type_token1,
  [8610] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(339), 1,
      sym_formatting_comment,
    STATE(633), 1,
      sym_update_clause,
    ACTIONS(807), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [8627] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(340), 1,
      sym_formatting_comment,
    STATE(634), 1,
      sym_update_clause,
    ACTIONS(809), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [8644] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(341), 1,
      sym_formatting_comment,
    STATE(554), 1,
      sym_update_clause,
    ACTIONS(811), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [8661] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(342), 1,
      sym_formatting_comment,
    STATE(639), 1,
      sym_update_clause,
    ACTIONS(813), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [8678] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(343), 1,
      sym_formatting_comment,
    STATE(640), 1,
      sym_update_clause,
    ACTIONS(815), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [8695] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(344), 1,
      sym_formatting_comment,
    STATE(642), 1,
      sym_update_clause,
    ACTIONS(817), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [8712] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(345), 1,
      sym_formatting_comment,
    STATE(643), 1,
      sym_update_clause,
    ACTIONS(819), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [8729] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(338), 1,
      sym_for_type,
    STATE(346), 1,
      sym_formatting_comment,
    ACTIONS(720), 3,
      aux_sym_for_type_token1,
      aux_sym_for_type_token2,
      aux_sym_for_type_token3,
  [8744] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(347), 1,
      sym_formatting_comment,
    STATE(644), 1,
      sym_update_clause,
    ACTIONS(821), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [8761] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(348), 1,
      sym_formatting_comment,
    STATE(645), 1,
      sym_update_clause,
    ACTIONS(823), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [8778] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(349), 1,
      sym_formatting_comment,
    STATE(646), 1,
      sym_update_clause,
    ACTIONS(825), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [8795] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(829), 1,
      anon_sym_COMMA,
    ACTIONS(827), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
    STATE(350), 2,
      sym_formatting_comment,
      aux_sym_update_clause_repeat1,
  [8810] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(351), 1,
      sym_formatting_comment,
    STATE(649), 1,
      sym_update_clause,
    ACTIONS(832), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [8827] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(352), 1,
      sym_formatting_comment,
    STATE(650), 1,
      sym_update_clause,
    ACTIONS(834), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [8844] = 6,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(836), 1,
      sym_bound_apex_expression,
    ACTIONS(838), 1,
      sym_identifier,
    STATE(332), 1,
      sym_dotted_identifier,
    STATE(353), 1,
      sym_formatting_comment,
    STATE(797), 1,
      sym_field_identifier,
  [8863] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(354), 1,
      sym_formatting_comment,
    STATE(652), 1,
      sym_update_clause,
    ACTIONS(840), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [8880] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(355), 1,
      sym_formatting_comment,
    STATE(653), 1,
      sym_update_clause,
    ACTIONS(842), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [8897] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(356), 1,
      sym_formatting_comment,
    STATE(521), 1,
      sym_update_clause,
    ACTIONS(844), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [8914] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(357), 1,
      sym_formatting_comment,
    STATE(658), 1,
      sym_update_clause,
    ACTIONS(846), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [8931] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(358), 1,
      sym_formatting_comment,
    STATE(659), 1,
      sym_update_clause,
    ACTIONS(848), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [8948] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(359), 1,
      sym_formatting_comment,
    STATE(559), 1,
      sym_update_clause,
    ACTIONS(850), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [8965] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(360), 1,
      sym_formatting_comment,
    STATE(662), 1,
      sym_update_clause,
    ACTIONS(852), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [8982] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(361), 1,
      sym_formatting_comment,
    STATE(663), 1,
      sym_update_clause,
    ACTIONS(854), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [8999] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(362), 1,
      sym_formatting_comment,
    STATE(664), 1,
      sym_update_clause,
    ACTIONS(856), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [9016] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(363), 1,
      sym_formatting_comment,
    STATE(665), 1,
      sym_update_clause,
    ACTIONS(858), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [9033] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(364), 1,
      sym_formatting_comment,
    ACTIONS(860), 4,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
  [9046] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(365), 1,
      sym_formatting_comment,
    STATE(668), 1,
      sym_update_clause,
    ACTIONS(862), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [9063] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(366), 1,
      sym_formatting_comment,
    STATE(669), 1,
      sym_update_clause,
    ACTIONS(864), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [9080] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(367), 1,
      sym_formatting_comment,
    STATE(525), 1,
      sym_update_clause,
    ACTIONS(866), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [9097] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(368), 1,
      sym_formatting_comment,
    STATE(677), 1,
      sym_update_clause,
    ACTIONS(868), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [9114] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(369), 1,
      sym_formatting_comment,
    STATE(678), 1,
      sym_update_clause,
    ACTIONS(870), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [9131] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(370), 1,
      sym_formatting_comment,
    STATE(679), 1,
      sym_update_clause,
    ACTIONS(872), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [9148] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(371), 1,
      sym_formatting_comment,
    STATE(748), 1,
      sym_update_clause,
    ACTIONS(874), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [9165] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(372), 1,
      sym_formatting_comment,
    STATE(682), 1,
      sym_update_clause,
    ACTIONS(876), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [9182] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(373), 1,
      sym_formatting_comment,
    STATE(683), 1,
      sym_update_clause,
    ACTIONS(878), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [9199] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(374), 1,
      sym_formatting_comment,
    STATE(686), 1,
      sym_update_clause,
    ACTIONS(880), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [9216] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(375), 1,
      sym_formatting_comment,
    STATE(687), 1,
      sym_update_clause,
    ACTIONS(882), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [9233] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(376), 1,
      sym_formatting_comment,
    STATE(563), 1,
      sym_update_clause,
    ACTIONS(884), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [9250] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(377), 1,
      sym_formatting_comment,
    STATE(692), 1,
      sym_update_clause,
    ACTIONS(886), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [9267] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(378), 1,
      sym_formatting_comment,
    STATE(693), 1,
      sym_update_clause,
    ACTIONS(888), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [9284] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(379), 1,
      sym_formatting_comment,
    STATE(469), 1,
      sym_update_clause,
    ACTIONS(890), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [9301] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(380), 1,
      sym_formatting_comment,
    STATE(696), 1,
      sym_update_clause,
    ACTIONS(892), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [9318] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(381), 1,
      sym_formatting_comment,
    STATE(697), 1,
      sym_update_clause,
    ACTIONS(894), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [9335] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(382), 1,
      sym_formatting_comment,
    STATE(698), 1,
      sym_update_clause,
    ACTIONS(896), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [9352] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(383), 1,
      sym_formatting_comment,
    STATE(699), 1,
      sym_update_clause,
    ACTIONS(898), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [9369] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(384), 1,
      sym_formatting_comment,
    STATE(577), 1,
      sym_update_clause,
    ACTIONS(900), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [9386] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(385), 1,
      sym_formatting_comment,
    STATE(702), 1,
      sym_update_clause,
    ACTIONS(902), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [9403] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(386), 1,
      sym_formatting_comment,
    STATE(703), 1,
      sym_update_clause,
    ACTIONS(904), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [9420] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(387), 1,
      sym_formatting_comment,
    STATE(704), 1,
      sym_update_clause,
    ACTIONS(906), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [9437] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(388), 1,
      sym_formatting_comment,
    STATE(705), 1,
      sym_update_clause,
    ACTIONS(908), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [9454] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(389), 1,
      sym_formatting_comment,
    STATE(706), 1,
      sym_update_clause,
    ACTIONS(910), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [9471] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(390), 1,
      sym_formatting_comment,
    STATE(472), 1,
      sym_update_clause,
    ACTIONS(912), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [9488] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(391), 1,
      sym_formatting_comment,
    STATE(709), 1,
      sym_update_clause,
    ACTIONS(914), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [9505] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(392), 1,
      sym_formatting_comment,
    STATE(710), 1,
      sym_update_clause,
    ACTIONS(916), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [9522] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(393), 1,
      sym_formatting_comment,
    STATE(711), 1,
      sym_update_clause,
    ACTIONS(918), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [9539] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(394), 1,
      sym_formatting_comment,
    ACTIONS(70), 2,
      anon_sym_RPAREN,
      anon_sym_COMMA,
    ACTIONS(72), 2,
      aux_sym_from_clause_token1,
      sym_identifier,
  [9554] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(395), 1,
      sym_formatting_comment,
    STATE(473), 1,
      sym_update_clause,
    ACTIONS(920), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [9571] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(728), 1,
      anon_sym_COMMA,
    STATE(350), 1,
      aux_sym_update_clause_repeat1,
    STATE(396), 1,
      sym_formatting_comment,
    ACTIONS(922), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [9588] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(397), 1,
      sym_formatting_comment,
    STATE(715), 1,
      sym_update_clause,
    ACTIONS(924), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [9605] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(398), 1,
      sym_formatting_comment,
    STATE(716), 1,
      sym_update_clause,
    ACTIONS(926), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [9622] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(399), 1,
      sym_formatting_comment,
    STATE(717), 1,
      sym_update_clause,
    ACTIONS(928), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [9639] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(400), 1,
      sym_formatting_comment,
    STATE(588), 1,
      sym_update_clause,
    ACTIONS(930), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [9656] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(401), 1,
      sym_formatting_comment,
    STATE(720), 1,
      sym_update_clause,
    ACTIONS(932), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [9673] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(402), 1,
      sym_formatting_comment,
    STATE(721), 1,
      sym_update_clause,
    ACTIONS(934), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [9690] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(403), 1,
      sym_formatting_comment,
    STATE(722), 1,
      sym_update_clause,
    ACTIONS(936), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [9707] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(404), 1,
      sym_formatting_comment,
    STATE(723), 1,
      sym_update_clause,
    ACTIONS(938), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [9724] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(405), 1,
      sym_formatting_comment,
    STATE(724), 1,
      sym_update_clause,
    ACTIONS(940), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [9741] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(406), 1,
      sym_formatting_comment,
    STATE(478), 1,
      sym_update_clause,
    ACTIONS(942), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [9758] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(407), 1,
      sym_formatting_comment,
    STATE(589), 1,
      sym_update_clause,
    ACTIONS(944), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [9775] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(408), 1,
      sym_formatting_comment,
    ACTIONS(74), 2,
      anon_sym_RPAREN,
      anon_sym_COMMA,
    ACTIONS(76), 2,
      aux_sym_from_clause_token1,
      sym_identifier,
  [9790] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(409), 1,
      sym_formatting_comment,
    STATE(484), 1,
      sym_update_clause,
    ACTIONS(946), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [9807] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(410), 1,
      sym_formatting_comment,
    STATE(492), 1,
      sym_update_clause,
    ACTIONS(948), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [9824] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(411), 1,
      sym_formatting_comment,
    STATE(528), 1,
      sym_update_clause,
    ACTIONS(950), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [9841] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(952), 1,
      anon_sym_LPAREN,
    ACTIONS(954), 1,
      sym_bound_apex_expression,
    STATE(412), 1,
      sym_formatting_comment,
    STATE(73), 2,
      sym_subquery,
      sym_comparable_list,
  [9858] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(413), 1,
      sym_formatting_comment,
    STATE(601), 1,
      sym_update_clause,
    ACTIONS(956), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [9875] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(414), 1,
      sym_formatting_comment,
    STATE(749), 1,
      sym_update_clause,
    ACTIONS(958), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [9892] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(960), 1,
      anon_sym_LPAREN,
    ACTIONS(962), 1,
      sym_bound_apex_expression,
    STATE(415), 1,
      sym_formatting_comment,
    STATE(451), 2,
      sym_subquery,
      sym_comparable_list,
  [9909] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(416), 1,
      sym_formatting_comment,
    STATE(501), 1,
      sym_update_clause,
    ACTIONS(964), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [9926] = 6,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(734), 1,
      sym_identifier,
    STATE(28), 1,
      sym_storage_identifier,
    STATE(40), 1,
      sym_dotted_identifier,
    STATE(41), 1,
      sym_storage_alias,
    STATE(417), 1,
      sym_formatting_comment,
  [9945] = 6,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(838), 1,
      sym_identifier,
    ACTIONS(966), 1,
      sym_bound_apex_expression,
    STATE(332), 1,
      sym_dotted_identifier,
    STATE(418), 1,
      sym_formatting_comment,
    STATE(807), 1,
      sym_field_identifier,
  [9964] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_for_type_token1,
    STATE(419), 1,
      sym_formatting_comment,
    STATE(490), 1,
      sym_update_clause,
    ACTIONS(968), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [9981] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(420), 1,
      sym_formatting_comment,
    ACTIONS(247), 3,
      anon_sym_RPAREN,
      aux_sym_and_expression_token1,
      aux_sym_or_expression_token1,
  [9993] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(970), 1,
      anon_sym_RPAREN,
    ACTIONS(972), 1,
      anon_sym_COMMA,
    STATE(421), 1,
      sym_formatting_comment,
    STATE(430), 1,
      aux_sym_with_data_cat_filter_repeat1,
  [10009] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(974), 1,
      anon_sym_COMMA,
    ACTIONS(976), 1,
      aux_sym_from_clause_token1,
    ACTIONS(978), 1,
      sym_identifier,
    STATE(422), 1,
      sym_formatting_comment,
  [10025] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(60), 1,
      anon_sym_COMMA,
    ACTIONS(492), 1,
      anon_sym_DOT,
    STATE(195), 1,
      aux_sym_dotted_identifier_repeat1,
    STATE(423), 1,
      sym_formatting_comment,
  [10041] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(424), 1,
      sym_formatting_comment,
    ACTIONS(980), 3,
      aux_sym_type_of_clause_token2,
      aux_sym_when_expression_token1,
      aux_sym_else_expression_token1,
  [10053] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(982), 1,
      anon_sym_COMMA,
    ACTIONS(984), 1,
      aux_sym_from_clause_token1,
    STATE(425), 1,
      sym_formatting_comment,
    STATE(434), 1,
      aux_sym_select_clause_repeat1,
  [10069] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(426), 1,
      sym_formatting_comment,
    ACTIONS(986), 3,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
      anon_sym_COMMA,
  [10081] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(988), 1,
      anon_sym_RPAREN,
    ACTIONS(990), 1,
      anon_sym_COMMA,
    STATE(427), 1,
      sym_formatting_comment,
    STATE(435), 1,
      aux_sym_comparable_list_repeat1,
  [10097] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(428), 1,
      sym_formatting_comment,
    STATE(592), 1,
      sym_boolean,
    ACTIONS(9), 2,
      aux_sym_boolean_token1,
      aux_sym_boolean_token2,
  [10111] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(295), 1,
      sym_update_type,
    STATE(429), 1,
      sym_formatting_comment,
    ACTIONS(992), 2,
      aux_sym_update_type_token1,
      aux_sym_update_type_token2,
  [10125] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(994), 1,
      anon_sym_RPAREN,
    ACTIONS(996), 1,
      anon_sym_COMMA,
    STATE(430), 2,
      sym_formatting_comment,
      aux_sym_with_data_cat_filter_repeat1,
  [10139] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(999), 1,
      sym_identifier,
    STATE(257), 1,
      sym_dotted_identifier,
    STATE(431), 1,
      sym_formatting_comment,
    STATE(460), 1,
      sym_field_list,
  [10155] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1001), 1,
      anon_sym_RPAREN,
    ACTIONS(1003), 1,
      anon_sym_COMMA,
    STATE(432), 1,
      sym_formatting_comment,
    STATE(438), 1,
      aux_sym_with_record_visibility_expression_repeat1,
  [10171] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(7), 1,
      aux_sym_select_clause_token1,
    STATE(433), 1,
      sym_formatting_comment,
    STATE(488), 1,
      sym_select_clause,
    STATE(799), 1,
      sym_soql_query_body,
  [10187] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(982), 1,
      anon_sym_COMMA,
    ACTIONS(1005), 1,
      aux_sym_from_clause_token1,
    STATE(434), 1,
      sym_formatting_comment,
    STATE(459), 1,
      aux_sym_select_clause_repeat1,
  [10203] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(990), 1,
      anon_sym_COMMA,
    ACTIONS(1007), 1,
      anon_sym_RPAREN,
    STATE(435), 1,
      sym_formatting_comment,
    STATE(439), 1,
      aux_sym_comparable_list_repeat1,
  [10219] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(436), 1,
      sym_formatting_comment,
    ACTIONS(170), 3,
      anon_sym_RPAREN,
      aux_sym_and_expression_token1,
      aux_sym_or_expression_token1,
  [10231] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1009), 1,
      anon_sym_RPAREN,
    ACTIONS(1011), 1,
      anon_sym_COMMA,
    STATE(437), 1,
      sym_formatting_comment,
    STATE(464), 1,
      aux_sym_function_expression_repeat1,
  [10247] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1013), 1,
      anon_sym_RPAREN,
    ACTIONS(1015), 1,
      anon_sym_COMMA,
    STATE(438), 2,
      sym_formatting_comment,
      aux_sym_with_record_visibility_expression_repeat1,
  [10261] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1018), 1,
      anon_sym_RPAREN,
    ACTIONS(1020), 1,
      anon_sym_COMMA,
    STATE(439), 2,
      sym_formatting_comment,
      aux_sym_comparable_list_repeat1,
  [10275] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(440), 1,
      sym_formatting_comment,
    ACTIONS(827), 3,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
      anon_sym_COMMA,
  [10287] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(441), 1,
      sym_formatting_comment,
    ACTIONS(277), 3,
      anon_sym_RPAREN,
      aux_sym_and_expression_token1,
      aux_sym_or_expression_token1,
  [10299] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(442), 1,
      sym_formatting_comment,
    ACTIONS(287), 3,
      anon_sym_RPAREN,
      aux_sym_and_expression_token1,
      aux_sym_or_expression_token1,
  [10311] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(972), 1,
      anon_sym_COMMA,
    ACTIONS(1023), 1,
      anon_sym_RPAREN,
    STATE(421), 1,
      aux_sym_with_data_cat_filter_repeat1,
    STATE(443), 1,
      sym_formatting_comment,
  [10327] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(444), 1,
      sym_formatting_comment,
    ACTIONS(251), 3,
      anon_sym_RPAREN,
      aux_sym_and_expression_token1,
      aux_sym_or_expression_token1,
  [10339] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(260), 1,
      anon_sym_RPAREN,
    ACTIONS(1025), 1,
      aux_sym_or_expression_token1,
    STATE(445), 2,
      sym_formatting_comment,
      aux_sym_or_expression_repeat1,
  [10353] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(446), 1,
      sym_formatting_comment,
    ACTIONS(176), 3,
      anon_sym_RPAREN,
      aux_sym_and_expression_token1,
      aux_sym_or_expression_token1,
  [10365] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(447), 1,
      sym_formatting_comment,
    ACTIONS(180), 3,
      anon_sym_RPAREN,
      aux_sym_and_expression_token1,
      aux_sym_or_expression_token1,
  [10377] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(448), 1,
      sym_formatting_comment,
    ACTIONS(265), 3,
      anon_sym_RPAREN,
      aux_sym_and_expression_token1,
      aux_sym_or_expression_token1,
  [10389] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(449), 1,
      sym_formatting_comment,
    ACTIONS(184), 3,
      anon_sym_RPAREN,
      aux_sym_and_expression_token1,
      aux_sym_or_expression_token1,
  [10401] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(450), 1,
      sym_formatting_comment,
    ACTIONS(188), 3,
      anon_sym_RPAREN,
      aux_sym_and_expression_token1,
      aux_sym_or_expression_token1,
  [10413] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(451), 1,
      sym_formatting_comment,
    ACTIONS(269), 3,
      anon_sym_RPAREN,
      aux_sym_and_expression_token1,
      aux_sym_or_expression_token1,
  [10425] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(452), 1,
      sym_formatting_comment,
    ACTIONS(192), 3,
      anon_sym_RPAREN,
      aux_sym_and_expression_token1,
      aux_sym_or_expression_token1,
  [10437] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(453), 1,
      sym_formatting_comment,
    ACTIONS(281), 3,
      anon_sym_RPAREN,
      aux_sym_and_expression_token1,
      aux_sym_or_expression_token1,
  [10449] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(454), 1,
      sym_formatting_comment,
    ACTIONS(243), 3,
      anon_sym_RPAREN,
      aux_sym_and_expression_token1,
      aux_sym_or_expression_token1,
  [10461] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1011), 1,
      anon_sym_COMMA,
    ACTIONS(1028), 1,
      anon_sym_RPAREN,
    STATE(437), 1,
      aux_sym_function_expression_repeat1,
    STATE(455), 1,
      sym_formatting_comment,
  [10477] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1011), 1,
      anon_sym_COMMA,
    ACTIONS(1030), 1,
      anon_sym_RPAREN,
    STATE(456), 1,
      sym_formatting_comment,
    STATE(458), 1,
      aux_sym_function_expression_repeat1,
  [10493] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(440), 1,
      sym_update_type,
    STATE(457), 1,
      sym_formatting_comment,
    ACTIONS(992), 2,
      aux_sym_update_type_token1,
      aux_sym_update_type_token2,
  [10507] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1011), 1,
      anon_sym_COMMA,
    ACTIONS(1032), 1,
      anon_sym_RPAREN,
    STATE(458), 1,
      sym_formatting_comment,
    STATE(464), 1,
      aux_sym_function_expression_repeat1,
  [10523] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1034), 1,
      anon_sym_COMMA,
    ACTIONS(1037), 1,
      aux_sym_from_clause_token1,
    STATE(459), 2,
      sym_formatting_comment,
      aux_sym_select_clause_repeat1,
  [10537] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(460), 1,
      sym_formatting_comment,
    ACTIONS(1039), 3,
      aux_sym_type_of_clause_token2,
      aux_sym_when_expression_token1,
      aux_sym_else_expression_token1,
  [10549] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(999), 1,
      sym_identifier,
    STATE(257), 1,
      sym_dotted_identifier,
    STATE(461), 1,
      sym_formatting_comment,
    STATE(757), 1,
      sym_field_list,
  [10565] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(990), 1,
      anon_sym_COMMA,
    ACTIONS(1041), 1,
      anon_sym_RPAREN,
    STATE(462), 1,
      sym_formatting_comment,
    STATE(463), 1,
      aux_sym_comparable_list_repeat1,
  [10581] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(990), 1,
      anon_sym_COMMA,
    ACTIONS(1043), 1,
      anon_sym_RPAREN,
    STATE(439), 1,
      aux_sym_comparable_list_repeat1,
    STATE(463), 1,
      sym_formatting_comment,
  [10597] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1045), 1,
      anon_sym_RPAREN,
    ACTIONS(1047), 1,
      anon_sym_COMMA,
    STATE(464), 2,
      sym_formatting_comment,
      aux_sym_function_expression_repeat1,
  [10611] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1003), 1,
      anon_sym_COMMA,
    ACTIONS(1050), 1,
      anon_sym_RPAREN,
    STATE(432), 1,
      aux_sym_with_record_visibility_expression_repeat1,
    STATE(465), 1,
      sym_formatting_comment,
  [10627] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(275), 1,
      anon_sym_RPAREN,
    ACTIONS(657), 1,
      aux_sym_or_expression_token1,
    STATE(445), 1,
      aux_sym_or_expression_repeat1,
    STATE(466), 1,
      sym_formatting_comment,
  [10643] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(467), 1,
      sym_formatting_comment,
    ACTIONS(1052), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [10654] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(468), 1,
      sym_formatting_comment,
    ACTIONS(1054), 2,
      sym_bound_apex_expression,
      sym_int,
  [10665] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(469), 1,
      sym_formatting_comment,
    ACTIONS(1056), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [10676] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(470), 1,
      sym_formatting_comment,
    ACTIONS(1058), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [10687] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(471), 1,
      sym_formatting_comment,
    ACTIONS(1060), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [10698] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(472), 1,
      sym_formatting_comment,
    ACTIONS(1062), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [10709] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(473), 1,
      sym_formatting_comment,
    ACTIONS(1064), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [10720] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(474), 1,
      sym_formatting_comment,
    ACTIONS(1066), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [10731] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(475), 1,
      sym_formatting_comment,
    ACTIONS(1068), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [10742] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(476), 1,
      sym_formatting_comment,
    ACTIONS(1070), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [10753] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(477), 1,
      sym_formatting_comment,
    ACTIONS(1072), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [10764] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(478), 1,
      sym_formatting_comment,
    ACTIONS(1074), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [10775] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(479), 1,
      sym_formatting_comment,
    ACTIONS(1076), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [10786] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(480), 1,
      sym_formatting_comment,
    ACTIONS(1078), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [10797] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(481), 1,
      sym_formatting_comment,
    ACTIONS(1080), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [10808] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(482), 1,
      sym_formatting_comment,
    ACTIONS(1082), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [10819] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(483), 1,
      sym_formatting_comment,
    ACTIONS(1084), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [10830] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(484), 1,
      sym_formatting_comment,
    ACTIONS(1086), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [10841] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(485), 1,
      sym_formatting_comment,
    ACTIONS(1088), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [10852] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(486), 1,
      sym_formatting_comment,
    ACTIONS(1090), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [10863] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(487), 1,
      sym_formatting_comment,
    ACTIONS(1092), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [10874] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1094), 1,
      aux_sym_from_clause_token1,
    STATE(18), 1,
      sym_from_clause,
    STATE(488), 1,
      sym_formatting_comment,
  [10887] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(489), 1,
      sym_formatting_comment,
    ACTIONS(1096), 2,
      sym_bound_apex_expression,
      sym_int,
  [10898] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(490), 1,
      sym_formatting_comment,
    ACTIONS(1098), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [10909] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(491), 1,
      sym_formatting_comment,
    ACTIONS(1100), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [10920] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(492), 1,
      sym_formatting_comment,
    ACTIONS(1102), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [10931] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(493), 1,
      sym_formatting_comment,
    ACTIONS(1104), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [10942] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(494), 1,
      sym_formatting_comment,
    ACTIONS(1106), 2,
      anon_sym_LPAREN,
      sym_bound_apex_expression,
  [10953] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(495), 1,
      sym_formatting_comment,
    ACTIONS(1108), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [10964] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(496), 1,
      sym_formatting_comment,
    ACTIONS(1110), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [10975] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(497), 1,
      sym_formatting_comment,
    ACTIONS(1112), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [10986] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(498), 1,
      sym_formatting_comment,
    ACTIONS(1114), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [10997] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(499), 1,
      sym_formatting_comment,
    ACTIONS(1116), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [11008] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(500), 1,
      sym_formatting_comment,
    ACTIONS(1118), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [11019] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(501), 1,
      sym_formatting_comment,
    ACTIONS(1120), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [11030] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(502), 1,
      sym_formatting_comment,
    ACTIONS(1122), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [11041] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(503), 1,
      sym_formatting_comment,
    ACTIONS(1124), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [11052] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(504), 1,
      sym_formatting_comment,
    ACTIONS(1126), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [11063] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(505), 1,
      sym_formatting_comment,
    ACTIONS(1128), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [11074] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(506), 1,
      sym_formatting_comment,
    ACTIONS(1130), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [11085] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(507), 1,
      sym_formatting_comment,
    ACTIONS(1132), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [11096] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(508), 1,
      sym_formatting_comment,
    ACTIONS(1134), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [11107] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(509), 1,
      sym_formatting_comment,
    ACTIONS(1136), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [11118] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(510), 1,
      sym_formatting_comment,
    ACTIONS(1138), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [11129] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(511), 1,
      sym_formatting_comment,
    ACTIONS(1140), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [11140] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(512), 1,
      sym_formatting_comment,
    ACTIONS(1142), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [11151] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(513), 1,
      sym_formatting_comment,
    ACTIONS(1144), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [11162] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(514), 1,
      sym_formatting_comment,
    ACTIONS(1146), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [11173] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(515), 1,
      sym_formatting_comment,
    ACTIONS(1148), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [11184] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(516), 1,
      sym_formatting_comment,
    ACTIONS(1150), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [11195] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(517), 1,
      sym_formatting_comment,
    ACTIONS(1152), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [11206] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(518), 1,
      sym_formatting_comment,
    ACTIONS(1154), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [11217] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(519), 1,
      sym_formatting_comment,
    ACTIONS(1156), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [11228] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(520), 1,
      sym_formatting_comment,
    ACTIONS(1158), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [11239] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(521), 1,
      sym_formatting_comment,
    ACTIONS(1160), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [11250] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(522), 1,
      sym_formatting_comment,
    ACTIONS(1162), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [11261] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(523), 1,
      sym_formatting_comment,
    ACTIONS(1164), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [11272] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(524), 1,
      sym_formatting_comment,
    ACTIONS(1166), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [11283] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(525), 1,
      sym_formatting_comment,
    ACTIONS(1168), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [11294] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(526), 1,
      sym_formatting_comment,
    ACTIONS(1170), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [11305] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(527), 1,
      sym_formatting_comment,
    ACTIONS(1172), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [11316] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(528), 1,
      sym_formatting_comment,
    ACTIONS(1174), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [11327] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(529), 1,
      sym_formatting_comment,
    ACTIONS(1176), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [11338] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(530), 1,
      sym_formatting_comment,
    ACTIONS(1178), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [11349] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(531), 1,
      sym_formatting_comment,
    ACTIONS(1180), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [11360] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(532), 1,
      sym_formatting_comment,
    ACTIONS(1182), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [11371] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(533), 1,
      sym_formatting_comment,
    ACTIONS(1184), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [11382] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(534), 1,
      sym_formatting_comment,
    ACTIONS(1186), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [11393] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(535), 1,
      sym_formatting_comment,
    ACTIONS(1188), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [11404] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(536), 1,
      sym_formatting_comment,
    ACTIONS(1190), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [11415] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(537), 1,
      sym_formatting_comment,
    ACTIONS(1192), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [11426] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(538), 1,
      sym_formatting_comment,
    ACTIONS(1194), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [11437] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(539), 1,
      sym_formatting_comment,
    ACTIONS(1196), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [11448] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(540), 1,
      sym_formatting_comment,
    ACTIONS(1198), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [11459] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(541), 1,
      sym_formatting_comment,
    ACTIONS(1200), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [11470] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(542), 1,
      sym_formatting_comment,
    ACTIONS(1202), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [11481] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(543), 1,
      sym_formatting_comment,
    ACTIONS(1204), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [11492] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(544), 1,
      sym_formatting_comment,
    ACTIONS(1206), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [11503] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(545), 1,
      sym_formatting_comment,
    ACTIONS(1208), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [11514] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(546), 1,
      sym_formatting_comment,
    ACTIONS(1210), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [11525] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(547), 1,
      sym_formatting_comment,
    ACTIONS(1212), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [11536] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(548), 1,
      sym_formatting_comment,
    ACTIONS(1214), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [11547] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(549), 1,
      sym_formatting_comment,
    ACTIONS(1216), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [11558] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(550), 1,
      sym_formatting_comment,
    ACTIONS(1218), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [11569] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(551), 1,
      sym_formatting_comment,
    ACTIONS(1220), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [11580] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(552), 1,
      sym_formatting_comment,
    ACTIONS(1222), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [11591] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(553), 1,
      sym_formatting_comment,
    ACTIONS(1224), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [11602] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(554), 1,
      sym_formatting_comment,
    ACTIONS(1226), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [11613] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1228), 1,
      sym_identifier,
    STATE(313), 1,
      sym_dotted_identifier,
    STATE(555), 1,
      sym_formatting_comment,
  [11626] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(556), 1,
      sym_formatting_comment,
    ACTIONS(1230), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [11637] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(557), 1,
      sym_formatting_comment,
    ACTIONS(1232), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [11648] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(558), 1,
      sym_formatting_comment,
    ACTIONS(1234), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [11659] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(559), 1,
      sym_formatting_comment,
    ACTIONS(1236), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [11670] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(560), 1,
      sym_formatting_comment,
    ACTIONS(1238), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [11681] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(561), 1,
      sym_formatting_comment,
    ACTIONS(1240), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [11692] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(562), 1,
      sym_formatting_comment,
    ACTIONS(1242), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [11703] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(563), 1,
      sym_formatting_comment,
    ACTIONS(1244), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [11714] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(564), 1,
      sym_formatting_comment,
    ACTIONS(1246), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [11725] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(565), 1,
      sym_formatting_comment,
    ACTIONS(1248), 2,
      anon_sym_COMMA,
      aux_sym_from_clause_token1,
  [11736] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(566), 1,
      sym_formatting_comment,
    ACTIONS(1250), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [11747] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(567), 1,
      sym_formatting_comment,
    ACTIONS(1252), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [11758] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(568), 1,
      sym_formatting_comment,
    ACTIONS(1254), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [11769] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(569), 1,
      sym_formatting_comment,
    ACTIONS(1256), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [11780] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(570), 1,
      sym_formatting_comment,
    ACTIONS(1258), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [11791] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(571), 1,
      sym_formatting_comment,
    ACTIONS(1260), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [11802] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(572), 1,
      sym_formatting_comment,
    ACTIONS(1262), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [11813] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(573), 1,
      sym_formatting_comment,
    ACTIONS(1264), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [11824] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(574), 1,
      sym_formatting_comment,
    ACTIONS(1266), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [11835] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(575), 1,
      sym_formatting_comment,
    ACTIONS(1268), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [11846] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(576), 1,
      sym_formatting_comment,
    ACTIONS(1270), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [11857] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(577), 1,
      sym_formatting_comment,
    ACTIONS(1272), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [11868] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(578), 1,
      sym_formatting_comment,
    ACTIONS(1274), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [11879] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(579), 1,
      sym_formatting_comment,
    ACTIONS(1276), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [11890] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(580), 1,
      sym_formatting_comment,
    ACTIONS(1278), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [11901] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(581), 1,
      sym_formatting_comment,
    ACTIONS(1280), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [11912] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(582), 1,
      sym_formatting_comment,
    ACTIONS(1282), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [11923] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(583), 1,
      sym_formatting_comment,
    ACTIONS(1284), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [11934] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(584), 1,
      sym_formatting_comment,
    ACTIONS(1286), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [11945] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(585), 1,
      sym_formatting_comment,
    ACTIONS(1288), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [11956] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(586), 1,
      sym_formatting_comment,
    ACTIONS(1290), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [11967] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1292), 1,
      sym_identifier,
    STATE(79), 1,
      sym_using_lookup_bind_expression,
    STATE(587), 1,
      sym_formatting_comment,
  [11980] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(588), 1,
      sym_formatting_comment,
    ACTIONS(1294), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [11991] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(589), 1,
      sym_formatting_comment,
    ACTIONS(1296), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [12002] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(590), 1,
      sym_formatting_comment,
    ACTIONS(1298), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [12013] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(591), 1,
      sym_formatting_comment,
    ACTIONS(1045), 2,
      anon_sym_RPAREN,
      anon_sym_COMMA,
  [12024] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(592), 1,
      sym_formatting_comment,
    ACTIONS(1300), 2,
      anon_sym_RPAREN,
      anon_sym_COMMA,
  [12035] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(593), 1,
      sym_formatting_comment,
    ACTIONS(1013), 2,
      anon_sym_RPAREN,
      anon_sym_COMMA,
  [12046] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(594), 1,
      sym_formatting_comment,
    ACTIONS(1302), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [12057] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(595), 1,
      sym_formatting_comment,
    ACTIONS(1304), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [12068] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(596), 1,
      sym_formatting_comment,
    ACTIONS(1306), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [12079] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(597), 1,
      sym_formatting_comment,
    ACTIONS(1308), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [12090] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(598), 1,
      sym_formatting_comment,
    ACTIONS(1310), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [12101] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(599), 1,
      sym_formatting_comment,
    ACTIONS(1312), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [12112] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(600), 1,
      sym_formatting_comment,
    ACTIONS(1314), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [12123] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(601), 1,
      sym_formatting_comment,
    ACTIONS(1316), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [12134] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(602), 1,
      sym_formatting_comment,
    ACTIONS(1318), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [12145] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(603), 1,
      sym_formatting_comment,
    ACTIONS(1320), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [12156] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(604), 1,
      sym_formatting_comment,
    ACTIONS(1322), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [12167] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(605), 1,
      sym_formatting_comment,
    ACTIONS(1324), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [12178] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1292), 1,
      sym_identifier,
    STATE(63), 1,
      sym_using_lookup_bind_expression,
    STATE(606), 1,
      sym_formatting_comment,
  [12191] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(607), 1,
      sym_formatting_comment,
    ACTIONS(1326), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [12202] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(608), 1,
      sym_formatting_comment,
    ACTIONS(1328), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [12213] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(609), 1,
      sym_formatting_comment,
    ACTIONS(1330), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [12224] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(610), 1,
      sym_formatting_comment,
    ACTIONS(1332), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [12235] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(611), 1,
      sym_formatting_comment,
    ACTIONS(1334), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [12246] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(612), 1,
      sym_formatting_comment,
    ACTIONS(1336), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [12257] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(613), 1,
      sym_formatting_comment,
    ACTIONS(1338), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [12268] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(614), 1,
      sym_formatting_comment,
    ACTIONS(1037), 2,
      anon_sym_COMMA,
      aux_sym_from_clause_token1,
  [12279] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(615), 1,
      sym_formatting_comment,
    ACTIONS(1340), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [12290] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(616), 1,
      sym_formatting_comment,
    ACTIONS(1342), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [12301] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(617), 1,
      sym_formatting_comment,
    ACTIONS(1344), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [12312] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(618), 1,
      sym_formatting_comment,
    ACTIONS(1346), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [12323] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(619), 1,
      sym_formatting_comment,
    ACTIONS(1348), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [12334] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(620), 1,
      sym_formatting_comment,
    ACTIONS(1350), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [12345] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(621), 1,
      sym_formatting_comment,
    ACTIONS(1352), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [12356] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(622), 1,
      sym_formatting_comment,
    ACTIONS(1354), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [12367] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(623), 1,
      sym_formatting_comment,
    ACTIONS(1356), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [12378] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(624), 1,
      sym_formatting_comment,
    ACTIONS(1358), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [12389] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(625), 1,
      sym_formatting_comment,
    ACTIONS(1360), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [12400] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(626), 1,
      sym_formatting_comment,
    ACTIONS(1362), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [12411] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(627), 1,
      sym_formatting_comment,
    ACTIONS(1364), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [12422] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(628), 1,
      sym_formatting_comment,
    ACTIONS(1366), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [12433] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(629), 1,
      sym_formatting_comment,
    ACTIONS(1368), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [12444] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(630), 1,
      sym_formatting_comment,
    ACTIONS(1370), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [12455] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(631), 1,
      sym_formatting_comment,
    ACTIONS(1372), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [12466] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(632), 1,
      sym_formatting_comment,
    ACTIONS(1374), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [12477] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(633), 1,
      sym_formatting_comment,
    ACTIONS(1376), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [12488] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(634), 1,
      sym_formatting_comment,
    ACTIONS(1378), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [12499] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(635), 1,
      sym_formatting_comment,
    ACTIONS(1380), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [12510] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(636), 1,
      sym_formatting_comment,
    ACTIONS(1382), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [12521] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(637), 1,
      sym_formatting_comment,
    ACTIONS(1384), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [12532] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(638), 1,
      sym_formatting_comment,
    ACTIONS(1386), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [12543] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(639), 1,
      sym_formatting_comment,
    ACTIONS(1388), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [12554] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(640), 1,
      sym_formatting_comment,
    ACTIONS(1390), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [12565] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(641), 1,
      sym_formatting_comment,
    ACTIONS(1392), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [12576] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(642), 1,
      sym_formatting_comment,
    ACTIONS(1394), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [12587] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(643), 1,
      sym_formatting_comment,
    ACTIONS(1396), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [12598] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(644), 1,
      sym_formatting_comment,
    ACTIONS(1398), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [12609] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(645), 1,
      sym_formatting_comment,
    ACTIONS(1400), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [12620] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(646), 1,
      sym_formatting_comment,
    ACTIONS(1402), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [12631] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(647), 1,
      sym_formatting_comment,
    ACTIONS(1404), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [12642] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(648), 1,
      sym_formatting_comment,
    ACTIONS(1406), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [12653] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(649), 1,
      sym_formatting_comment,
    ACTIONS(1408), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [12664] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(650), 1,
      sym_formatting_comment,
    ACTIONS(1410), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [12675] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(651), 1,
      sym_formatting_comment,
    ACTIONS(1412), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [12686] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(652), 1,
      sym_formatting_comment,
    ACTIONS(1414), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [12697] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(653), 1,
      sym_formatting_comment,
    ACTIONS(1416), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [12708] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1418), 1,
      sym_identifier,
    STATE(111), 1,
      sym_with_data_cat_filter,
    STATE(654), 1,
      sym_formatting_comment,
  [12721] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(655), 1,
      sym_formatting_comment,
    ACTIONS(1420), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [12732] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(656), 1,
      sym_formatting_comment,
    ACTIONS(1422), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [12743] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(657), 1,
      sym_formatting_comment,
    ACTIONS(1424), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [12754] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(658), 1,
      sym_formatting_comment,
    ACTIONS(1426), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [12765] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(659), 1,
      sym_formatting_comment,
    ACTIONS(1428), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [12776] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(660), 1,
      sym_formatting_comment,
    ACTIONS(1430), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [12787] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(661), 1,
      sym_formatting_comment,
    ACTIONS(1432), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [12798] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(662), 1,
      sym_formatting_comment,
    ACTIONS(1434), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [12809] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(663), 1,
      sym_formatting_comment,
    ACTIONS(1436), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [12820] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(664), 1,
      sym_formatting_comment,
    ACTIONS(1438), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [12831] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(665), 1,
      sym_formatting_comment,
    ACTIONS(1440), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [12842] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(666), 1,
      sym_formatting_comment,
    ACTIONS(1442), 2,
      anon_sym_COMMA,
      aux_sym_from_clause_token1,
  [12853] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(667), 1,
      sym_formatting_comment,
    ACTIONS(1444), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [12864] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(668), 1,
      sym_formatting_comment,
    ACTIONS(1446), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [12875] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(669), 1,
      sym_formatting_comment,
    ACTIONS(1448), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [12886] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(670), 1,
      sym_formatting_comment,
    ACTIONS(1450), 2,
      sym_bound_apex_expression,
      sym_decimal,
  [12897] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(671), 1,
      sym_formatting_comment,
    ACTIONS(1452), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [12908] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(672), 1,
      sym_formatting_comment,
    ACTIONS(1454), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [12919] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(673), 1,
      sym_formatting_comment,
    ACTIONS(1456), 2,
      anon_sym_COMMA,
      aux_sym_from_clause_token1,
  [12930] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(674), 1,
      sym_formatting_comment,
    ACTIONS(1018), 2,
      anon_sym_RPAREN,
      anon_sym_COMMA,
  [12941] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(675), 1,
      sym_formatting_comment,
    ACTIONS(1458), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [12952] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(676), 1,
      sym_formatting_comment,
    ACTIONS(1460), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [12963] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(677), 1,
      sym_formatting_comment,
    ACTIONS(1462), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [12974] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(678), 1,
      sym_formatting_comment,
    ACTIONS(1464), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [12985] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(679), 1,
      sym_formatting_comment,
    ACTIONS(1466), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [12996] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(680), 1,
      sym_formatting_comment,
    ACTIONS(1468), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [13007] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(681), 1,
      sym_formatting_comment,
    ACTIONS(1470), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [13018] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(682), 1,
      sym_formatting_comment,
    ACTIONS(1472), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [13029] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(683), 1,
      sym_formatting_comment,
    ACTIONS(1474), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [13040] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(684), 1,
      sym_formatting_comment,
    ACTIONS(1476), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [13051] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(685), 1,
      sym_formatting_comment,
    ACTIONS(1478), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [13062] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(686), 1,
      sym_formatting_comment,
    ACTIONS(1480), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [13073] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(687), 1,
      sym_formatting_comment,
    ACTIONS(1482), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [13084] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(688), 1,
      sym_formatting_comment,
    ACTIONS(1484), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [13095] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(689), 1,
      sym_formatting_comment,
    ACTIONS(1486), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [13106] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(690), 1,
      sym_formatting_comment,
    ACTIONS(1488), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [13117] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(691), 1,
      sym_formatting_comment,
    ACTIONS(1490), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [13128] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(692), 1,
      sym_formatting_comment,
    ACTIONS(1492), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [13139] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(693), 1,
      sym_formatting_comment,
    ACTIONS(1494), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [13150] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(694), 1,
      sym_formatting_comment,
    ACTIONS(1496), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [13161] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(695), 1,
      sym_formatting_comment,
    ACTIONS(1498), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [13172] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(696), 1,
      sym_formatting_comment,
    ACTIONS(1500), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [13183] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(697), 1,
      sym_formatting_comment,
    ACTIONS(1502), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [13194] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(698), 1,
      sym_formatting_comment,
    ACTIONS(1504), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [13205] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(699), 1,
      sym_formatting_comment,
    ACTIONS(1506), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [13216] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(700), 1,
      sym_formatting_comment,
    ACTIONS(1508), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [13227] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(701), 1,
      sym_formatting_comment,
    ACTIONS(1510), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [13238] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(702), 1,
      sym_formatting_comment,
    ACTIONS(1512), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [13249] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(703), 1,
      sym_formatting_comment,
    ACTIONS(1514), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [13260] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(704), 1,
      sym_formatting_comment,
    ACTIONS(1516), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [13271] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(705), 1,
      sym_formatting_comment,
    ACTIONS(1518), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [13282] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(706), 1,
      sym_formatting_comment,
    ACTIONS(1520), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [13293] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(707), 1,
      sym_formatting_comment,
    ACTIONS(1522), 2,
      anon_sym_COMMA,
      aux_sym_from_clause_token1,
  [13304] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(708), 1,
      sym_formatting_comment,
    ACTIONS(1524), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [13315] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(709), 1,
      sym_formatting_comment,
    ACTIONS(1526), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [13326] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(710), 1,
      sym_formatting_comment,
    ACTIONS(1528), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [13337] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(711), 1,
      sym_formatting_comment,
    ACTIONS(1530), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [13348] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(712), 1,
      sym_formatting_comment,
    ACTIONS(994), 2,
      anon_sym_RPAREN,
      anon_sym_COMMA,
  [13359] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(713), 1,
      sym_formatting_comment,
    ACTIONS(1532), 2,
      anon_sym_COMMA,
      aux_sym_from_clause_token1,
  [13370] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(714), 1,
      sym_formatting_comment,
    ACTIONS(1534), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [13381] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(715), 1,
      sym_formatting_comment,
    ACTIONS(1536), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [13392] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(716), 1,
      sym_formatting_comment,
    ACTIONS(1538), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [13403] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(717), 1,
      sym_formatting_comment,
    ACTIONS(1540), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [13414] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(718), 1,
      sym_formatting_comment,
    ACTIONS(1542), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [13425] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(719), 1,
      sym_formatting_comment,
    ACTIONS(1544), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [13436] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(720), 1,
      sym_formatting_comment,
    ACTIONS(1546), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [13447] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(721), 1,
      sym_formatting_comment,
    ACTIONS(1548), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [13458] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(722), 1,
      sym_formatting_comment,
    ACTIONS(1550), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [13469] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(723), 1,
      sym_formatting_comment,
    ACTIONS(1552), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [13480] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(724), 1,
      sym_formatting_comment,
    ACTIONS(1554), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [13491] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(725), 1,
      sym_formatting_comment,
    ACTIONS(1556), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [13502] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1558), 1,
      anon_sym_SLASH_SLASH,
    ACTIONS(1560), 1,
      aux_sym_select_clause_token1,
    STATE(726), 1,
      sym_formatting_comment,
  [13515] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(727), 1,
      sym_formatting_comment,
    ACTIONS(1562), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [13526] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(728), 1,
      sym_formatting_comment,
    ACTIONS(1564), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [13537] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(729), 1,
      sym_formatting_comment,
    ACTIONS(1566), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [13548] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1568), 1,
      sym_identifier,
    STATE(194), 1,
      sym_dotted_identifier,
    STATE(730), 1,
      sym_formatting_comment,
  [13561] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(731), 1,
      sym_formatting_comment,
    ACTIONS(1570), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [13572] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(732), 1,
      sym_formatting_comment,
    ACTIONS(1572), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [13583] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(733), 1,
      sym_formatting_comment,
    ACTIONS(1574), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [13594] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(734), 1,
      sym_formatting_comment,
    ACTIONS(1576), 2,
      anon_sym_LPAREN,
      sym_identifier,
  [13605] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(735), 1,
      sym_formatting_comment,
    ACTIONS(1578), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [13616] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(736), 1,
      sym_formatting_comment,
    ACTIONS(260), 2,
      anon_sym_RPAREN,
      aux_sym_or_expression_token1,
  [13627] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(737), 1,
      sym_formatting_comment,
    ACTIONS(1580), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [13638] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(738), 1,
      sym_formatting_comment,
    ACTIONS(1582), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [13649] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1584), 1,
      anon_sym_LPAREN,
    ACTIONS(1586), 1,
      sym_identifier,
    STATE(739), 1,
      sym_formatting_comment,
  [13662] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(740), 1,
      sym_formatting_comment,
    ACTIONS(1588), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [13673] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(741), 1,
      sym_formatting_comment,
    ACTIONS(1590), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [13684] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1418), 1,
      sym_identifier,
    STATE(121), 1,
      sym_with_data_cat_filter,
    STATE(742), 1,
      sym_formatting_comment,
  [13697] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(743), 1,
      sym_formatting_comment,
    ACTIONS(1592), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [13708] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(744), 1,
      sym_formatting_comment,
    ACTIONS(1594), 2,
      sym_bound_apex_expression,
      sym_decimal,
  [13719] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(745), 1,
      sym_formatting_comment,
    ACTIONS(974), 2,
      anon_sym_COMMA,
      aux_sym_from_clause_token1,
  [13730] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(49), 1,
      anon_sym_DOT,
    STATE(9), 1,
      aux_sym_dotted_identifier_repeat1,
    STATE(746), 1,
      sym_formatting_comment,
  [13743] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(747), 1,
      sym_formatting_comment,
    ACTIONS(1596), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [13754] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(748), 1,
      sym_formatting_comment,
    ACTIONS(1598), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [13765] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(749), 1,
      sym_formatting_comment,
    ACTIONS(1600), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [13776] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1602), 1,
      anon_sym_SLASH_SLASH,
    ACTIONS(1604), 1,
      aux_sym_select_clause_token1,
    STATE(750), 1,
      sym_formatting_comment,
  [13789] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(751), 1,
      sym_formatting_comment,
    ACTIONS(1606), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [13800] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1608), 1,
      sym_identifier,
    STATE(56), 1,
      sym_dotted_identifier,
    STATE(752), 1,
      sym_formatting_comment,
  [13813] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(753), 1,
      sym_formatting_comment,
    ACTIONS(1610), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [13824] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1612), 1,
      anon_sym_LPAREN,
    STATE(754), 1,
      sym_formatting_comment,
  [13834] = 3,
    ACTIONS(1614), 1,
      aux_sym_header_comment_token1,
    ACTIONS(1616), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(755), 1,
      sym_formatting_comment,
  [13844] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1618), 1,
      ts_builtin_sym_end,
    STATE(756), 1,
      sym_formatting_comment,
  [13854] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1620), 1,
      aux_sym_type_of_clause_token2,
    STATE(757), 1,
      sym_formatting_comment,
  [13864] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1622), 1,
      anon_sym_EQ,
    STATE(758), 1,
      sym_formatting_comment,
  [13874] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1624), 1,
      anon_sym_EQ,
    STATE(759), 1,
      sym_formatting_comment,
  [13884] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1626), 1,
      anon_sym_EQ,
    STATE(760), 1,
      sym_formatting_comment,
  [13894] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1628), 1,
      aux_sym_type_of_clause_token2,
    STATE(761), 1,
      sym_formatting_comment,
  [13904] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1630), 1,
      anon_sym_COMMA,
    STATE(762), 1,
      sym_formatting_comment,
  [13914] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1632), 1,
      anon_sym_LPAREN,
    STATE(763), 1,
      sym_formatting_comment,
  [13924] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1634), 1,
      sym_identifier,
    STATE(764), 1,
      sym_formatting_comment,
  [13934] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1636), 1,
      anon_sym_LPAREN,
    STATE(765), 1,
      sym_formatting_comment,
  [13944] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1638), 1,
      anon_sym_EQ,
    STATE(766), 1,
      sym_formatting_comment,
  [13954] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(575), 1,
      aux_sym_type_of_clause_token2,
    STATE(767), 1,
      sym_formatting_comment,
  [13964] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1640), 1,
      anon_sym_COLON,
    STATE(768), 1,
      sym_formatting_comment,
  [13974] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1642), 1,
      anon_sym_LPAREN,
    STATE(769), 1,
      sym_formatting_comment,
  [13984] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1644), 1,
      anon_sym_COMMA,
    STATE(770), 1,
      sym_formatting_comment,
  [13994] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1646), 1,
      sym_identifier,
    STATE(771), 1,
      sym_formatting_comment,
  [14004] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1648), 1,
      anon_sym_LPAREN,
    STATE(772), 1,
      sym_formatting_comment,
  [14014] = 3,
    ACTIONS(1616), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1650), 1,
      aux_sym_header_comment_token1,
    STATE(773), 1,
      sym_formatting_comment,
  [14024] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1652), 1,
      anon_sym_LPAREN,
    STATE(774), 1,
      sym_formatting_comment,
  [14034] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1654), 1,
      aux_sym_from_clause_token1,
    STATE(775), 1,
      sym_formatting_comment,
  [14044] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1656), 1,
      anon_sym_RPAREN,
    STATE(776), 1,
      sym_formatting_comment,
  [14054] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1658), 1,
      anon_sym_RPAREN,
    STATE(777), 1,
      sym_formatting_comment,
  [14064] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1660), 1,
      ts_builtin_sym_end,
    STATE(778), 1,
      sym_formatting_comment,
  [14074] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1662), 1,
      anon_sym_RPAREN,
    STATE(779), 1,
      sym_formatting_comment,
  [14084] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1664), 1,
      ts_builtin_sym_end,
    STATE(780), 1,
      sym_formatting_comment,
  [14094] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1666), 1,
      anon_sym_RPAREN,
    STATE(781), 1,
      sym_formatting_comment,
  [14104] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1668), 1,
      anon_sym_COMMA,
    STATE(782), 1,
      sym_formatting_comment,
  [14114] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1670), 1,
      sym_string_literal,
    STATE(783), 1,
      sym_formatting_comment,
  [14124] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1672), 1,
      sym_string_literal,
    STATE(784), 1,
      sym_formatting_comment,
  [14134] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1674), 1,
      aux_sym_when_expression_token2,
    STATE(785), 1,
      sym_formatting_comment,
  [14144] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1676), 1,
      anon_sym_RPAREN,
    STATE(786), 1,
      sym_formatting_comment,
  [14154] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1678), 1,
      sym_identifier,
    STATE(787), 1,
      sym_formatting_comment,
  [14164] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1680), 1,
      anon_sym_EQ,
    STATE(788), 1,
      sym_formatting_comment,
  [14174] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1682), 1,
      sym_identifier,
    STATE(789), 1,
      sym_formatting_comment,
  [14184] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1684), 1,
      sym_int,
    STATE(790), 1,
      sym_formatting_comment,
  [14194] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1686), 1,
      anon_sym_RPAREN,
    STATE(791), 1,
      sym_formatting_comment,
  [14204] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1688), 1,
      sym_identifier,
    STATE(792), 1,
      sym_formatting_comment,
  [14214] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1690), 1,
      anon_sym_RPAREN,
    STATE(793), 1,
      sym_formatting_comment,
  [14224] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1692), 1,
      sym_int,
    STATE(794), 1,
      sym_formatting_comment,
  [14234] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1694), 1,
      ts_builtin_sym_end,
    STATE(795), 1,
      sym_formatting_comment,
  [14244] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1696), 1,
      sym_int,
    STATE(796), 1,
      sym_formatting_comment,
  [14254] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1698), 1,
      anon_sym_COMMA,
    STATE(797), 1,
      sym_formatting_comment,
  [14264] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1700), 1,
      anon_sym_RPAREN,
    STATE(798), 1,
      sym_formatting_comment,
  [14274] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1702), 1,
      anon_sym_RPAREN,
    STATE(799), 1,
      sym_formatting_comment,
  [14284] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1704), 1,
      sym_identifier,
    STATE(800), 1,
      sym_formatting_comment,
  [14294] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1706), 1,
      anon_sym_LPAREN,
    STATE(801), 1,
      sym_formatting_comment,
  [14304] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(984), 1,
      aux_sym_from_clause_token1,
    STATE(802), 1,
      sym_formatting_comment,
  [14314] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1708), 1,
      anon_sym_COLON,
    STATE(803), 1,
      sym_formatting_comment,
  [14324] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1710), 1,
      sym_string_literal,
    STATE(804), 1,
      sym_formatting_comment,
  [14334] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1712), 1,
      anon_sym_COMMA,
    STATE(805), 1,
      sym_formatting_comment,
  [14344] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1714), 1,
      anon_sym_COMMA,
    STATE(806), 1,
      sym_formatting_comment,
  [14354] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1716), 1,
      anon_sym_COMMA,
    STATE(807), 1,
      sym_formatting_comment,
  [14364] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1718), 1,
      sym_identifier,
    STATE(808), 1,
      sym_formatting_comment,
  [14374] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1720), 1,
      anon_sym_LPAREN,
    STATE(809), 1,
      sym_formatting_comment,
  [14384] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1722), 1,
      sym_identifier,
    STATE(810), 1,
      sym_formatting_comment,
  [14394] = 1,
    ACTIONS(1724), 1,
      ts_builtin_sym_end,
};

static const uint32_t ts_small_parse_table_map[] = {
  [SMALL_STATE(2)] = 0,
  [SMALL_STATE(3)] = 73,
  [SMALL_STATE(4)] = 146,
  [SMALL_STATE(5)] = 210,
  [SMALL_STATE(6)] = 274,
  [SMALL_STATE(7)] = 338,
  [SMALL_STATE(8)] = 399,
  [SMALL_STATE(9)] = 444,
  [SMALL_STATE(10)] = 493,
  [SMALL_STATE(11)] = 540,
  [SMALL_STATE(12)] = 584,
  [SMALL_STATE(13)] = 629,
  [SMALL_STATE(14)] = 665,
  [SMALL_STATE(15)] = 701,
  [SMALL_STATE(16)] = 737,
  [SMALL_STATE(17)] = 773,
  [SMALL_STATE(18)] = 807,
  [SMALL_STATE(19)] = 872,
  [SMALL_STATE(20)] = 931,
  [SMALL_STATE(21)] = 971,
  [SMALL_STATE(22)] = 1011,
  [SMALL_STATE(23)] = 1058,
  [SMALL_STATE(24)] = 1111,
  [SMALL_STATE(25)] = 1164,
  [SMALL_STATE(26)] = 1193,
  [SMALL_STATE(27)] = 1224,
  [SMALL_STATE(28)] = 1255,
  [SMALL_STATE(29)] = 1289,
  [SMALL_STATE(30)] = 1335,
  [SMALL_STATE(31)] = 1381,
  [SMALL_STATE(32)] = 1407,
  [SMALL_STATE(33)] = 1453,
  [SMALL_STATE(34)] = 1497,
  [SMALL_STATE(35)] = 1543,
  [SMALL_STATE(36)] = 1590,
  [SMALL_STATE(37)] = 1637,
  [SMALL_STATE(38)] = 1684,
  [SMALL_STATE(39)] = 1713,
  [SMALL_STATE(40)] = 1760,
  [SMALL_STATE(41)] = 1785,
  [SMALL_STATE(42)] = 1811,
  [SMALL_STATE(43)] = 1835,
  [SMALL_STATE(44)] = 1859,
  [SMALL_STATE(45)] = 1885,
  [SMALL_STATE(46)] = 1909,
  [SMALL_STATE(47)] = 1933,
  [SMALL_STATE(48)] = 1957,
  [SMALL_STATE(49)] = 1981,
  [SMALL_STATE(50)] = 2005,
  [SMALL_STATE(51)] = 2035,
  [SMALL_STATE(52)] = 2065,
  [SMALL_STATE(53)] = 2106,
  [SMALL_STATE(54)] = 2127,
  [SMALL_STATE(55)] = 2168,
  [SMALL_STATE(56)] = 2189,
  [SMALL_STATE(57)] = 2214,
  [SMALL_STATE(58)] = 2235,
  [SMALL_STATE(59)] = 2276,
  [SMALL_STATE(60)] = 2317,
  [SMALL_STATE(61)] = 2358,
  [SMALL_STATE(62)] = 2383,
  [SMALL_STATE(63)] = 2424,
  [SMALL_STATE(64)] = 2449,
  [SMALL_STATE(65)] = 2472,
  [SMALL_STATE(66)] = 2513,
  [SMALL_STATE(67)] = 2554,
  [SMALL_STATE(68)] = 2576,
  [SMALL_STATE(69)] = 2598,
  [SMALL_STATE(70)] = 2620,
  [SMALL_STATE(71)] = 2642,
  [SMALL_STATE(72)] = 2664,
  [SMALL_STATE(73)] = 2686,
  [SMALL_STATE(74)] = 2708,
  [SMALL_STATE(75)] = 2732,
  [SMALL_STATE(76)] = 2756,
  [SMALL_STATE(77)] = 2778,
  [SMALL_STATE(78)] = 2800,
  [SMALL_STATE(79)] = 2820,
  [SMALL_STATE(80)] = 2840,
  [SMALL_STATE(81)] = 2862,
  [SMALL_STATE(82)] = 2897,
  [SMALL_STATE(83)] = 2916,
  [SMALL_STATE(84)] = 2937,
  [SMALL_STATE(85)] = 2972,
  [SMALL_STATE(86)] = 3007,
  [SMALL_STATE(87)] = 3030,
  [SMALL_STATE(88)] = 3065,
  [SMALL_STATE(89)] = 3100,
  [SMALL_STATE(90)] = 3135,
  [SMALL_STATE(91)] = 3170,
  [SMALL_STATE(92)] = 3193,
  [SMALL_STATE(93)] = 3228,
  [SMALL_STATE(94)] = 3263,
  [SMALL_STATE(95)] = 3298,
  [SMALL_STATE(96)] = 3333,
  [SMALL_STATE(97)] = 3368,
  [SMALL_STATE(98)] = 3403,
  [SMALL_STATE(99)] = 3424,
  [SMALL_STATE(100)] = 3459,
  [SMALL_STATE(101)] = 3478,
  [SMALL_STATE(102)] = 3507,
  [SMALL_STATE(103)] = 3542,
  [SMALL_STATE(104)] = 3561,
  [SMALL_STATE(105)] = 3580,
  [SMALL_STATE(106)] = 3601,
  [SMALL_STATE(107)] = 3636,
  [SMALL_STATE(108)] = 3659,
  [SMALL_STATE(109)] = 3694,
  [SMALL_STATE(110)] = 3729,
  [SMALL_STATE(111)] = 3748,
  [SMALL_STATE(112)] = 3771,
  [SMALL_STATE(113)] = 3794,
  [SMALL_STATE(114)] = 3829,
  [SMALL_STATE(115)] = 3864,
  [SMALL_STATE(116)] = 3883,
  [SMALL_STATE(117)] = 3901,
  [SMALL_STATE(118)] = 3919,
  [SMALL_STATE(119)] = 3937,
  [SMALL_STATE(120)] = 3955,
  [SMALL_STATE(121)] = 3973,
  [SMALL_STATE(122)] = 3991,
  [SMALL_STATE(123)] = 4009,
  [SMALL_STATE(124)] = 4027,
  [SMALL_STATE(125)] = 4045,
  [SMALL_STATE(126)] = 4067,
  [SMALL_STATE(127)] = 4084,
  [SMALL_STATE(128)] = 4113,
  [SMALL_STATE(129)] = 4142,
  [SMALL_STATE(130)] = 4159,
  [SMALL_STATE(131)] = 4188,
  [SMALL_STATE(132)] = 4217,
  [SMALL_STATE(133)] = 4246,
  [SMALL_STATE(134)] = 4275,
  [SMALL_STATE(135)] = 4304,
  [SMALL_STATE(136)] = 4333,
  [SMALL_STATE(137)] = 4362,
  [SMALL_STATE(138)] = 4391,
  [SMALL_STATE(139)] = 4420,
  [SMALL_STATE(140)] = 4449,
  [SMALL_STATE(141)] = 4478,
  [SMALL_STATE(142)] = 4507,
  [SMALL_STATE(143)] = 4536,
  [SMALL_STATE(144)] = 4557,
  [SMALL_STATE(145)] = 4574,
  [SMALL_STATE(146)] = 4603,
  [SMALL_STATE(147)] = 4632,
  [SMALL_STATE(148)] = 4649,
  [SMALL_STATE(149)] = 4678,
  [SMALL_STATE(150)] = 4697,
  [SMALL_STATE(151)] = 4726,
  [SMALL_STATE(152)] = 4755,
  [SMALL_STATE(153)] = 4784,
  [SMALL_STATE(154)] = 4813,
  [SMALL_STATE(155)] = 4834,
  [SMALL_STATE(156)] = 4863,
  [SMALL_STATE(157)] = 4892,
  [SMALL_STATE(158)] = 4921,
  [SMALL_STATE(159)] = 4950,
  [SMALL_STATE(160)] = 4979,
  [SMALL_STATE(161)] = 4996,
  [SMALL_STATE(162)] = 5025,
  [SMALL_STATE(163)] = 5046,
  [SMALL_STATE(164)] = 5075,
  [SMALL_STATE(165)] = 5104,
  [SMALL_STATE(166)] = 5133,
  [SMALL_STATE(167)] = 5162,
  [SMALL_STATE(168)] = 5193,
  [SMALL_STATE(169)] = 5222,
  [SMALL_STATE(170)] = 5251,
  [SMALL_STATE(171)] = 5280,
  [SMALL_STATE(172)] = 5306,
  [SMALL_STATE(173)] = 5322,
  [SMALL_STATE(174)] = 5338,
  [SMALL_STATE(175)] = 5354,
  [SMALL_STATE(176)] = 5370,
  [SMALL_STATE(177)] = 5386,
  [SMALL_STATE(178)] = 5402,
  [SMALL_STATE(179)] = 5430,
  [SMALL_STATE(180)] = 5456,
  [SMALL_STATE(181)] = 5480,
  [SMALL_STATE(182)] = 5506,
  [SMALL_STATE(183)] = 5532,
  [SMALL_STATE(184)] = 5556,
  [SMALL_STATE(185)] = 5579,
  [SMALL_STATE(186)] = 5602,
  [SMALL_STATE(187)] = 5625,
  [SMALL_STATE(188)] = 5648,
  [SMALL_STATE(189)] = 5671,
  [SMALL_STATE(190)] = 5694,
  [SMALL_STATE(191)] = 5717,
  [SMALL_STATE(192)] = 5740,
  [SMALL_STATE(193)] = 5763,
  [SMALL_STATE(194)] = 5782,
  [SMALL_STATE(195)] = 5807,
  [SMALL_STATE(196)] = 5828,
  [SMALL_STATE(197)] = 5851,
  [SMALL_STATE(198)] = 5874,
  [SMALL_STATE(199)] = 5897,
  [SMALL_STATE(200)] = 5920,
  [SMALL_STATE(201)] = 5943,
  [SMALL_STATE(202)] = 5966,
  [SMALL_STATE(203)] = 5989,
  [SMALL_STATE(204)] = 6012,
  [SMALL_STATE(205)] = 6035,
  [SMALL_STATE(206)] = 6056,
  [SMALL_STATE(207)] = 6079,
  [SMALL_STATE(208)] = 6102,
  [SMALL_STATE(209)] = 6125,
  [SMALL_STATE(210)] = 6148,
  [SMALL_STATE(211)] = 6171,
  [SMALL_STATE(212)] = 6194,
  [SMALL_STATE(213)] = 6217,
  [SMALL_STATE(214)] = 6240,
  [SMALL_STATE(215)] = 6263,
  [SMALL_STATE(216)] = 6286,
  [SMALL_STATE(217)] = 6309,
  [SMALL_STATE(218)] = 6334,
  [SMALL_STATE(219)] = 6357,
  [SMALL_STATE(220)] = 6380,
  [SMALL_STATE(221)] = 6403,
  [SMALL_STATE(222)] = 6426,
  [SMALL_STATE(223)] = 6451,
  [SMALL_STATE(224)] = 6474,
  [SMALL_STATE(225)] = 6497,
  [SMALL_STATE(226)] = 6520,
  [SMALL_STATE(227)] = 6543,
  [SMALL_STATE(228)] = 6566,
  [SMALL_STATE(229)] = 6589,
  [SMALL_STATE(230)] = 6612,
  [SMALL_STATE(231)] = 6635,
  [SMALL_STATE(232)] = 6658,
  [SMALL_STATE(233)] = 6681,
  [SMALL_STATE(234)] = 6704,
  [SMALL_STATE(235)] = 6727,
  [SMALL_STATE(236)] = 6750,
  [SMALL_STATE(237)] = 6773,
  [SMALL_STATE(238)] = 6796,
  [SMALL_STATE(239)] = 6819,
  [SMALL_STATE(240)] = 6842,
  [SMALL_STATE(241)] = 6865,
  [SMALL_STATE(242)] = 6888,
  [SMALL_STATE(243)] = 6911,
  [SMALL_STATE(244)] = 6934,
  [SMALL_STATE(245)] = 6957,
  [SMALL_STATE(246)] = 6980,
  [SMALL_STATE(247)] = 7003,
  [SMALL_STATE(248)] = 7026,
  [SMALL_STATE(249)] = 7049,
  [SMALL_STATE(250)] = 7074,
  [SMALL_STATE(251)] = 7097,
  [SMALL_STATE(252)] = 7120,
  [SMALL_STATE(253)] = 7143,
  [SMALL_STATE(254)] = 7166,
  [SMALL_STATE(255)] = 7189,
  [SMALL_STATE(256)] = 7212,
  [SMALL_STATE(257)] = 7231,
  [SMALL_STATE(258)] = 7249,
  [SMALL_STATE(259)] = 7267,
  [SMALL_STATE(260)] = 7283,
  [SMALL_STATE(261)] = 7301,
  [SMALL_STATE(262)] = 7317,
  [SMALL_STATE(263)] = 7335,
  [SMALL_STATE(264)] = 7357,
  [SMALL_STATE(265)] = 7375,
  [SMALL_STATE(266)] = 7391,
  [SMALL_STATE(267)] = 7405,
  [SMALL_STATE(268)] = 7423,
  [SMALL_STATE(269)] = 7440,
  [SMALL_STATE(270)] = 7457,
  [SMALL_STATE(271)] = 7474,
  [SMALL_STATE(272)] = 7491,
  [SMALL_STATE(273)] = 7508,
  [SMALL_STATE(274)] = 7521,
  [SMALL_STATE(275)] = 7538,
  [SMALL_STATE(276)] = 7555,
  [SMALL_STATE(277)] = 7572,
  [SMALL_STATE(278)] = 7589,
  [SMALL_STATE(279)] = 7606,
  [SMALL_STATE(280)] = 7623,
  [SMALL_STATE(281)] = 7640,
  [SMALL_STATE(282)] = 7657,
  [SMALL_STATE(283)] = 7674,
  [SMALL_STATE(284)] = 7691,
  [SMALL_STATE(285)] = 7708,
  [SMALL_STATE(286)] = 7725,
  [SMALL_STATE(287)] = 7742,
  [SMALL_STATE(288)] = 7759,
  [SMALL_STATE(289)] = 7776,
  [SMALL_STATE(290)] = 7793,
  [SMALL_STATE(291)] = 7810,
  [SMALL_STATE(292)] = 7827,
  [SMALL_STATE(293)] = 7842,
  [SMALL_STATE(294)] = 7859,
  [SMALL_STATE(295)] = 7876,
  [SMALL_STATE(296)] = 7893,
  [SMALL_STATE(297)] = 7910,
  [SMALL_STATE(298)] = 7927,
  [SMALL_STATE(299)] = 7946,
  [SMALL_STATE(300)] = 7963,
  [SMALL_STATE(301)] = 7980,
  [SMALL_STATE(302)] = 7997,
  [SMALL_STATE(303)] = 8014,
  [SMALL_STATE(304)] = 8031,
  [SMALL_STATE(305)] = 8048,
  [SMALL_STATE(306)] = 8065,
  [SMALL_STATE(307)] = 8082,
  [SMALL_STATE(308)] = 8099,
  [SMALL_STATE(309)] = 8116,
  [SMALL_STATE(310)] = 8133,
  [SMALL_STATE(311)] = 8150,
  [SMALL_STATE(312)] = 8167,
  [SMALL_STATE(313)] = 8184,
  [SMALL_STATE(314)] = 8197,
  [SMALL_STATE(315)] = 8214,
  [SMALL_STATE(316)] = 8231,
  [SMALL_STATE(317)] = 8248,
  [SMALL_STATE(318)] = 8265,
  [SMALL_STATE(319)] = 8282,
  [SMALL_STATE(320)] = 8299,
  [SMALL_STATE(321)] = 8316,
  [SMALL_STATE(322)] = 8333,
  [SMALL_STATE(323)] = 8350,
  [SMALL_STATE(324)] = 8367,
  [SMALL_STATE(325)] = 8384,
  [SMALL_STATE(326)] = 8401,
  [SMALL_STATE(327)] = 8418,
  [SMALL_STATE(328)] = 8433,
  [SMALL_STATE(329)] = 8450,
  [SMALL_STATE(330)] = 8467,
  [SMALL_STATE(331)] = 8482,
  [SMALL_STATE(332)] = 8499,
  [SMALL_STATE(333)] = 8514,
  [SMALL_STATE(334)] = 8531,
  [SMALL_STATE(335)] = 8548,
  [SMALL_STATE(336)] = 8563,
  [SMALL_STATE(337)] = 8580,
  [SMALL_STATE(338)] = 8597,
  [SMALL_STATE(339)] = 8610,
  [SMALL_STATE(340)] = 8627,
  [SMALL_STATE(341)] = 8644,
  [SMALL_STATE(342)] = 8661,
  [SMALL_STATE(343)] = 8678,
  [SMALL_STATE(344)] = 8695,
  [SMALL_STATE(345)] = 8712,
  [SMALL_STATE(346)] = 8729,
  [SMALL_STATE(347)] = 8744,
  [SMALL_STATE(348)] = 8761,
  [SMALL_STATE(349)] = 8778,
  [SMALL_STATE(350)] = 8795,
  [SMALL_STATE(351)] = 8810,
  [SMALL_STATE(352)] = 8827,
  [SMALL_STATE(353)] = 8844,
  [SMALL_STATE(354)] = 8863,
  [SMALL_STATE(355)] = 8880,
  [SMALL_STATE(356)] = 8897,
  [SMALL_STATE(357)] = 8914,
  [SMALL_STATE(358)] = 8931,
  [SMALL_STATE(359)] = 8948,
  [SMALL_STATE(360)] = 8965,
  [SMALL_STATE(361)] = 8982,
  [SMALL_STATE(362)] = 8999,
  [SMALL_STATE(363)] = 9016,
  [SMALL_STATE(364)] = 9033,
  [SMALL_STATE(365)] = 9046,
  [SMALL_STATE(366)] = 9063,
  [SMALL_STATE(367)] = 9080,
  [SMALL_STATE(368)] = 9097,
  [SMALL_STATE(369)] = 9114,
  [SMALL_STATE(370)] = 9131,
  [SMALL_STATE(371)] = 9148,
  [SMALL_STATE(372)] = 9165,
  [SMALL_STATE(373)] = 9182,
  [SMALL_STATE(374)] = 9199,
  [SMALL_STATE(375)] = 9216,
  [SMALL_STATE(376)] = 9233,
  [SMALL_STATE(377)] = 9250,
  [SMALL_STATE(378)] = 9267,
  [SMALL_STATE(379)] = 9284,
  [SMALL_STATE(380)] = 9301,
  [SMALL_STATE(381)] = 9318,
  [SMALL_STATE(382)] = 9335,
  [SMALL_STATE(383)] = 9352,
  [SMALL_STATE(384)] = 9369,
  [SMALL_STATE(385)] = 9386,
  [SMALL_STATE(386)] = 9403,
  [SMALL_STATE(387)] = 9420,
  [SMALL_STATE(388)] = 9437,
  [SMALL_STATE(389)] = 9454,
  [SMALL_STATE(390)] = 9471,
  [SMALL_STATE(391)] = 9488,
  [SMALL_STATE(392)] = 9505,
  [SMALL_STATE(393)] = 9522,
  [SMALL_STATE(394)] = 9539,
  [SMALL_STATE(395)] = 9554,
  [SMALL_STATE(396)] = 9571,
  [SMALL_STATE(397)] = 9588,
  [SMALL_STATE(398)] = 9605,
  [SMALL_STATE(399)] = 9622,
  [SMALL_STATE(400)] = 9639,
  [SMALL_STATE(401)] = 9656,
  [SMALL_STATE(402)] = 9673,
  [SMALL_STATE(403)] = 9690,
  [SMALL_STATE(404)] = 9707,
  [SMALL_STATE(405)] = 9724,
  [SMALL_STATE(406)] = 9741,
  [SMALL_STATE(407)] = 9758,
  [SMALL_STATE(408)] = 9775,
  [SMALL_STATE(409)] = 9790,
  [SMALL_STATE(410)] = 9807,
  [SMALL_STATE(411)] = 9824,
  [SMALL_STATE(412)] = 9841,
  [SMALL_STATE(413)] = 9858,
  [SMALL_STATE(414)] = 9875,
  [SMALL_STATE(415)] = 9892,
  [SMALL_STATE(416)] = 9909,
  [SMALL_STATE(417)] = 9926,
  [SMALL_STATE(418)] = 9945,
  [SMALL_STATE(419)] = 9964,
  [SMALL_STATE(420)] = 9981,
  [SMALL_STATE(421)] = 9993,
  [SMALL_STATE(422)] = 10009,
  [SMALL_STATE(423)] = 10025,
  [SMALL_STATE(424)] = 10041,
  [SMALL_STATE(425)] = 10053,
  [SMALL_STATE(426)] = 10069,
  [SMALL_STATE(427)] = 10081,
  [SMALL_STATE(428)] = 10097,
  [SMALL_STATE(429)] = 10111,
  [SMALL_STATE(430)] = 10125,
  [SMALL_STATE(431)] = 10139,
  [SMALL_STATE(432)] = 10155,
  [SMALL_STATE(433)] = 10171,
  [SMALL_STATE(434)] = 10187,
  [SMALL_STATE(435)] = 10203,
  [SMALL_STATE(436)] = 10219,
  [SMALL_STATE(437)] = 10231,
  [SMALL_STATE(438)] = 10247,
  [SMALL_STATE(439)] = 10261,
  [SMALL_STATE(440)] = 10275,
  [SMALL_STATE(441)] = 10287,
  [SMALL_STATE(442)] = 10299,
  [SMALL_STATE(443)] = 10311,
  [SMALL_STATE(444)] = 10327,
  [SMALL_STATE(445)] = 10339,
  [SMALL_STATE(446)] = 10353,
  [SMALL_STATE(447)] = 10365,
  [SMALL_STATE(448)] = 10377,
  [SMALL_STATE(449)] = 10389,
  [SMALL_STATE(450)] = 10401,
  [SMALL_STATE(451)] = 10413,
  [SMALL_STATE(452)] = 10425,
  [SMALL_STATE(453)] = 10437,
  [SMALL_STATE(454)] = 10449,
  [SMALL_STATE(455)] = 10461,
  [SMALL_STATE(456)] = 10477,
  [SMALL_STATE(457)] = 10493,
  [SMALL_STATE(458)] = 10507,
  [SMALL_STATE(459)] = 10523,
  [SMALL_STATE(460)] = 10537,
  [SMALL_STATE(461)] = 10549,
  [SMALL_STATE(462)] = 10565,
  [SMALL_STATE(463)] = 10581,
  [SMALL_STATE(464)] = 10597,
  [SMALL_STATE(465)] = 10611,
  [SMALL_STATE(466)] = 10627,
  [SMALL_STATE(467)] = 10643,
  [SMALL_STATE(468)] = 10654,
  [SMALL_STATE(469)] = 10665,
  [SMALL_STATE(470)] = 10676,
  [SMALL_STATE(471)] = 10687,
  [SMALL_STATE(472)] = 10698,
  [SMALL_STATE(473)] = 10709,
  [SMALL_STATE(474)] = 10720,
  [SMALL_STATE(475)] = 10731,
  [SMALL_STATE(476)] = 10742,
  [SMALL_STATE(477)] = 10753,
  [SMALL_STATE(478)] = 10764,
  [SMALL_STATE(479)] = 10775,
  [SMALL_STATE(480)] = 10786,
  [SMALL_STATE(481)] = 10797,
  [SMALL_STATE(482)] = 10808,
  [SMALL_STATE(483)] = 10819,
  [SMALL_STATE(484)] = 10830,
  [SMALL_STATE(485)] = 10841,
  [SMALL_STATE(486)] = 10852,
  [SMALL_STATE(487)] = 10863,
  [SMALL_STATE(488)] = 10874,
  [SMALL_STATE(489)] = 10887,
  [SMALL_STATE(490)] = 10898,
  [SMALL_STATE(491)] = 10909,
  [SMALL_STATE(492)] = 10920,
  [SMALL_STATE(493)] = 10931,
  [SMALL_STATE(494)] = 10942,
  [SMALL_STATE(495)] = 10953,
  [SMALL_STATE(496)] = 10964,
  [SMALL_STATE(497)] = 10975,
  [SMALL_STATE(498)] = 10986,
  [SMALL_STATE(499)] = 10997,
  [SMALL_STATE(500)] = 11008,
  [SMALL_STATE(501)] = 11019,
  [SMALL_STATE(502)] = 11030,
  [SMALL_STATE(503)] = 11041,
  [SMALL_STATE(504)] = 11052,
  [SMALL_STATE(505)] = 11063,
  [SMALL_STATE(506)] = 11074,
  [SMALL_STATE(507)] = 11085,
  [SMALL_STATE(508)] = 11096,
  [SMALL_STATE(509)] = 11107,
  [SMALL_STATE(510)] = 11118,
  [SMALL_STATE(511)] = 11129,
  [SMALL_STATE(512)] = 11140,
  [SMALL_STATE(513)] = 11151,
  [SMALL_STATE(514)] = 11162,
  [SMALL_STATE(515)] = 11173,
  [SMALL_STATE(516)] = 11184,
  [SMALL_STATE(517)] = 11195,
  [SMALL_STATE(518)] = 11206,
  [SMALL_STATE(519)] = 11217,
  [SMALL_STATE(520)] = 11228,
  [SMALL_STATE(521)] = 11239,
  [SMALL_STATE(522)] = 11250,
  [SMALL_STATE(523)] = 11261,
  [SMALL_STATE(524)] = 11272,
  [SMALL_STATE(525)] = 11283,
  [SMALL_STATE(526)] = 11294,
  [SMALL_STATE(527)] = 11305,
  [SMALL_STATE(528)] = 11316,
  [SMALL_STATE(529)] = 11327,
  [SMALL_STATE(530)] = 11338,
  [SMALL_STATE(531)] = 11349,
  [SMALL_STATE(532)] = 11360,
  [SMALL_STATE(533)] = 11371,
  [SMALL_STATE(534)] = 11382,
  [SMALL_STATE(535)] = 11393,
  [SMALL_STATE(536)] = 11404,
  [SMALL_STATE(537)] = 11415,
  [SMALL_STATE(538)] = 11426,
  [SMALL_STATE(539)] = 11437,
  [SMALL_STATE(540)] = 11448,
  [SMALL_STATE(541)] = 11459,
  [SMALL_STATE(542)] = 11470,
  [SMALL_STATE(543)] = 11481,
  [SMALL_STATE(544)] = 11492,
  [SMALL_STATE(545)] = 11503,
  [SMALL_STATE(546)] = 11514,
  [SMALL_STATE(547)] = 11525,
  [SMALL_STATE(548)] = 11536,
  [SMALL_STATE(549)] = 11547,
  [SMALL_STATE(550)] = 11558,
  [SMALL_STATE(551)] = 11569,
  [SMALL_STATE(552)] = 11580,
  [SMALL_STATE(553)] = 11591,
  [SMALL_STATE(554)] = 11602,
  [SMALL_STATE(555)] = 11613,
  [SMALL_STATE(556)] = 11626,
  [SMALL_STATE(557)] = 11637,
  [SMALL_STATE(558)] = 11648,
  [SMALL_STATE(559)] = 11659,
  [SMALL_STATE(560)] = 11670,
  [SMALL_STATE(561)] = 11681,
  [SMALL_STATE(562)] = 11692,
  [SMALL_STATE(563)] = 11703,
  [SMALL_STATE(564)] = 11714,
  [SMALL_STATE(565)] = 11725,
  [SMALL_STATE(566)] = 11736,
  [SMALL_STATE(567)] = 11747,
  [SMALL_STATE(568)] = 11758,
  [SMALL_STATE(569)] = 11769,
  [SMALL_STATE(570)] = 11780,
  [SMALL_STATE(571)] = 11791,
  [SMALL_STATE(572)] = 11802,
  [SMALL_STATE(573)] = 11813,
  [SMALL_STATE(574)] = 11824,
  [SMALL_STATE(575)] = 11835,
  [SMALL_STATE(576)] = 11846,
  [SMALL_STATE(577)] = 11857,
  [SMALL_STATE(578)] = 11868,
  [SMALL_STATE(579)] = 11879,
  [SMALL_STATE(580)] = 11890,
  [SMALL_STATE(581)] = 11901,
  [SMALL_STATE(582)] = 11912,
  [SMALL_STATE(583)] = 11923,
  [SMALL_STATE(584)] = 11934,
  [SMALL_STATE(585)] = 11945,
  [SMALL_STATE(586)] = 11956,
  [SMALL_STATE(587)] = 11967,
  [SMALL_STATE(588)] = 11980,
  [SMALL_STATE(589)] = 11991,
  [SMALL_STATE(590)] = 12002,
  [SMALL_STATE(591)] = 12013,
  [SMALL_STATE(592)] = 12024,
  [SMALL_STATE(593)] = 12035,
  [SMALL_STATE(594)] = 12046,
  [SMALL_STATE(595)] = 12057,
  [SMALL_STATE(596)] = 12068,
  [SMALL_STATE(597)] = 12079,
  [SMALL_STATE(598)] = 12090,
  [SMALL_STATE(599)] = 12101,
  [SMALL_STATE(600)] = 12112,
  [SMALL_STATE(601)] = 12123,
  [SMALL_STATE(602)] = 12134,
  [SMALL_STATE(603)] = 12145,
  [SMALL_STATE(604)] = 12156,
  [SMALL_STATE(605)] = 12167,
  [SMALL_STATE(606)] = 12178,
  [SMALL_STATE(607)] = 12191,
  [SMALL_STATE(608)] = 12202,
  [SMALL_STATE(609)] = 12213,
  [SMALL_STATE(610)] = 12224,
  [SMALL_STATE(611)] = 12235,
  [SMALL_STATE(612)] = 12246,
  [SMALL_STATE(613)] = 12257,
  [SMALL_STATE(614)] = 12268,
  [SMALL_STATE(615)] = 12279,
  [SMALL_STATE(616)] = 12290,
  [SMALL_STATE(617)] = 12301,
  [SMALL_STATE(618)] = 12312,
  [SMALL_STATE(619)] = 12323,
  [SMALL_STATE(620)] = 12334,
  [SMALL_STATE(621)] = 12345,
  [SMALL_STATE(622)] = 12356,
  [SMALL_STATE(623)] = 12367,
  [SMALL_STATE(624)] = 12378,
  [SMALL_STATE(625)] = 12389,
  [SMALL_STATE(626)] = 12400,
  [SMALL_STATE(627)] = 12411,
  [SMALL_STATE(628)] = 12422,
  [SMALL_STATE(629)] = 12433,
  [SMALL_STATE(630)] = 12444,
  [SMALL_STATE(631)] = 12455,
  [SMALL_STATE(632)] = 12466,
  [SMALL_STATE(633)] = 12477,
  [SMALL_STATE(634)] = 12488,
  [SMALL_STATE(635)] = 12499,
  [SMALL_STATE(636)] = 12510,
  [SMALL_STATE(637)] = 12521,
  [SMALL_STATE(638)] = 12532,
  [SMALL_STATE(639)] = 12543,
  [SMALL_STATE(640)] = 12554,
  [SMALL_STATE(641)] = 12565,
  [SMALL_STATE(642)] = 12576,
  [SMALL_STATE(643)] = 12587,
  [SMALL_STATE(644)] = 12598,
  [SMALL_STATE(645)] = 12609,
  [SMALL_STATE(646)] = 12620,
  [SMALL_STATE(647)] = 12631,
  [SMALL_STATE(648)] = 12642,
  [SMALL_STATE(649)] = 12653,
  [SMALL_STATE(650)] = 12664,
  [SMALL_STATE(651)] = 12675,
  [SMALL_STATE(652)] = 12686,
  [SMALL_STATE(653)] = 12697,
  [SMALL_STATE(654)] = 12708,
  [SMALL_STATE(655)] = 12721,
  [SMALL_STATE(656)] = 12732,
  [SMALL_STATE(657)] = 12743,
  [SMALL_STATE(658)] = 12754,
  [SMALL_STATE(659)] = 12765,
  [SMALL_STATE(660)] = 12776,
  [SMALL_STATE(661)] = 12787,
  [SMALL_STATE(662)] = 12798,
  [SMALL_STATE(663)] = 12809,
  [SMALL_STATE(664)] = 12820,
  [SMALL_STATE(665)] = 12831,
  [SMALL_STATE(666)] = 12842,
  [SMALL_STATE(667)] = 12853,
  [SMALL_STATE(668)] = 12864,
  [SMALL_STATE(669)] = 12875,
  [SMALL_STATE(670)] = 12886,
  [SMALL_STATE(671)] = 12897,
  [SMALL_STATE(672)] = 12908,
  [SMALL_STATE(673)] = 12919,
  [SMALL_STATE(674)] = 12930,
  [SMALL_STATE(675)] = 12941,
  [SMALL_STATE(676)] = 12952,
  [SMALL_STATE(677)] = 12963,
  [SMALL_STATE(678)] = 12974,
  [SMALL_STATE(679)] = 12985,
  [SMALL_STATE(680)] = 12996,
  [SMALL_STATE(681)] = 13007,
  [SMALL_STATE(682)] = 13018,
  [SMALL_STATE(683)] = 13029,
  [SMALL_STATE(684)] = 13040,
  [SMALL_STATE(685)] = 13051,
  [SMALL_STATE(686)] = 13062,
  [SMALL_STATE(687)] = 13073,
  [SMALL_STATE(688)] = 13084,
  [SMALL_STATE(689)] = 13095,
  [SMALL_STATE(690)] = 13106,
  [SMALL_STATE(691)] = 13117,
  [SMALL_STATE(692)] = 13128,
  [SMALL_STATE(693)] = 13139,
  [SMALL_STATE(694)] = 13150,
  [SMALL_STATE(695)] = 13161,
  [SMALL_STATE(696)] = 13172,
  [SMALL_STATE(697)] = 13183,
  [SMALL_STATE(698)] = 13194,
  [SMALL_STATE(699)] = 13205,
  [SMALL_STATE(700)] = 13216,
  [SMALL_STATE(701)] = 13227,
  [SMALL_STATE(702)] = 13238,
  [SMALL_STATE(703)] = 13249,
  [SMALL_STATE(704)] = 13260,
  [SMALL_STATE(705)] = 13271,
  [SMALL_STATE(706)] = 13282,
  [SMALL_STATE(707)] = 13293,
  [SMALL_STATE(708)] = 13304,
  [SMALL_STATE(709)] = 13315,
  [SMALL_STATE(710)] = 13326,
  [SMALL_STATE(711)] = 13337,
  [SMALL_STATE(712)] = 13348,
  [SMALL_STATE(713)] = 13359,
  [SMALL_STATE(714)] = 13370,
  [SMALL_STATE(715)] = 13381,
  [SMALL_STATE(716)] = 13392,
  [SMALL_STATE(717)] = 13403,
  [SMALL_STATE(718)] = 13414,
  [SMALL_STATE(719)] = 13425,
  [SMALL_STATE(720)] = 13436,
  [SMALL_STATE(721)] = 13447,
  [SMALL_STATE(722)] = 13458,
  [SMALL_STATE(723)] = 13469,
  [SMALL_STATE(724)] = 13480,
  [SMALL_STATE(725)] = 13491,
  [SMALL_STATE(726)] = 13502,
  [SMALL_STATE(727)] = 13515,
  [SMALL_STATE(728)] = 13526,
  [SMALL_STATE(729)] = 13537,
  [SMALL_STATE(730)] = 13548,
  [SMALL_STATE(731)] = 13561,
  [SMALL_STATE(732)] = 13572,
  [SMALL_STATE(733)] = 13583,
  [SMALL_STATE(734)] = 13594,
  [SMALL_STATE(735)] = 13605,
  [SMALL_STATE(736)] = 13616,
  [SMALL_STATE(737)] = 13627,
  [SMALL_STATE(738)] = 13638,
  [SMALL_STATE(739)] = 13649,
  [SMALL_STATE(740)] = 13662,
  [SMALL_STATE(741)] = 13673,
  [SMALL_STATE(742)] = 13684,
  [SMALL_STATE(743)] = 13697,
  [SMALL_STATE(744)] = 13708,
  [SMALL_STATE(745)] = 13719,
  [SMALL_STATE(746)] = 13730,
  [SMALL_STATE(747)] = 13743,
  [SMALL_STATE(748)] = 13754,
  [SMALL_STATE(749)] = 13765,
  [SMALL_STATE(750)] = 13776,
  [SMALL_STATE(751)] = 13789,
  [SMALL_STATE(752)] = 13800,
  [SMALL_STATE(753)] = 13813,
  [SMALL_STATE(754)] = 13824,
  [SMALL_STATE(755)] = 13834,
  [SMALL_STATE(756)] = 13844,
  [SMALL_STATE(757)] = 13854,
  [SMALL_STATE(758)] = 13864,
  [SMALL_STATE(759)] = 13874,
  [SMALL_STATE(760)] = 13884,
  [SMALL_STATE(761)] = 13894,
  [SMALL_STATE(762)] = 13904,
  [SMALL_STATE(763)] = 13914,
  [SMALL_STATE(764)] = 13924,
  [SMALL_STATE(765)] = 13934,
  [SMALL_STATE(766)] = 13944,
  [SMALL_STATE(767)] = 13954,
  [SMALL_STATE(768)] = 13964,
  [SMALL_STATE(769)] = 13974,
  [SMALL_STATE(770)] = 13984,
  [SMALL_STATE(771)] = 13994,
  [SMALL_STATE(772)] = 14004,
  [SMALL_STATE(773)] = 14014,
  [SMALL_STATE(774)] = 14024,
  [SMALL_STATE(775)] = 14034,
  [SMALL_STATE(776)] = 14044,
  [SMALL_STATE(777)] = 14054,
  [SMALL_STATE(778)] = 14064,
  [SMALL_STATE(779)] = 14074,
  [SMALL_STATE(780)] = 14084,
  [SMALL_STATE(781)] = 14094,
  [SMALL_STATE(782)] = 14104,
  [SMALL_STATE(783)] = 14114,
  [SMALL_STATE(784)] = 14124,
  [SMALL_STATE(785)] = 14134,
  [SMALL_STATE(786)] = 14144,
  [SMALL_STATE(787)] = 14154,
  [SMALL_STATE(788)] = 14164,
  [SMALL_STATE(789)] = 14174,
  [SMALL_STATE(790)] = 14184,
  [SMALL_STATE(791)] = 14194,
  [SMALL_STATE(792)] = 14204,
  [SMALL_STATE(793)] = 14214,
  [SMALL_STATE(794)] = 14224,
  [SMALL_STATE(795)] = 14234,
  [SMALL_STATE(796)] = 14244,
  [SMALL_STATE(797)] = 14254,
  [SMALL_STATE(798)] = 14264,
  [SMALL_STATE(799)] = 14274,
  [SMALL_STATE(800)] = 14284,
  [SMALL_STATE(801)] = 14294,
  [SMALL_STATE(802)] = 14304,
  [SMALL_STATE(803)] = 14314,
  [SMALL_STATE(804)] = 14324,
  [SMALL_STATE(805)] = 14334,
  [SMALL_STATE(806)] = 14344,
  [SMALL_STATE(807)] = 14354,
  [SMALL_STATE(808)] = 14364,
  [SMALL_STATE(809)] = 14374,
  [SMALL_STATE(810)] = 14384,
  [SMALL_STATE(811)] = 14394,
};

static const TSParseActionEntry ts_parse_actions[] = {
  [0] = {.entry = {.count = 0, .reusable = false}},
  [1] = {.entry = {.count = 1, .reusable = false}}, RECOVER(),
  [3] = {.entry = {.count = 1, .reusable = true}}, SHIFT(755),
  [5] = {.entry = {.count = 1, .reusable = false}}, SHIFT(773),
  [7] = {.entry = {.count = 1, .reusable = true}}, SHIFT(22),
  [9] = {.entry = {.count = 1, .reusable = true}}, SHIFT(45),
  [11] = {.entry = {.count = 1, .reusable = true}}, SHIFT(46),
  [13] = {.entry = {.count = 1, .reusable = true}}, SHIFT(768),
  [15] = {.entry = {.count = 1, .reusable = true}}, SHIFT(427),
  [17] = {.entry = {.count = 1, .reusable = true}}, SHIFT(47),
  [19] = {.entry = {.count = 1, .reusable = true}}, SHIFT(48),
  [21] = {.entry = {.count = 1, .reusable = false}}, SHIFT(48),
  [23] = {.entry = {.count = 1, .reusable = true}}, SHIFT(462),
  [25] = {.entry = {.count = 1, .reusable = true}}, SHIFT(72),
  [27] = {.entry = {.count = 1, .reusable = true}}, SHIFT(674),
  [29] = {.entry = {.count = 1, .reusable = true}}, SHIFT(446),
  [31] = {.entry = {.count = 1, .reusable = true}}, SHIFT(447),
  [33] = {.entry = {.count = 1, .reusable = true}}, SHIFT(803),
  [35] = {.entry = {.count = 1, .reusable = true}}, SHIFT(448),
  [37] = {.entry = {.count = 1, .reusable = true}}, SHIFT(449),
  [39] = {.entry = {.count = 1, .reusable = true}}, SHIFT(450),
  [41] = {.entry = {.count = 1, .reusable = false}}, SHIFT(450),
  [43] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_value_comparison_operator, 1, 0, 0),
  [45] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_value_comparison_operator, 1, 0, 0),
  [47] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_dotted_identifier, 2, 0, 0),
  [49] = {.entry = {.count = 1, .reusable = true}}, SHIFT(787),
  [51] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_dotted_identifier, 2, 0, 0),
  [53] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_dotted_identifier_repeat1, 2, 0, 0),
  [55] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_dotted_identifier_repeat1, 2, 0, 0), SHIFT_REPEAT(787),
  [58] = {.entry = {.count = 1, .reusable = false}}, REDUCE(aux_sym_dotted_identifier_repeat1, 2, 0, 0),
  [60] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_field_identifier, 1, 0, 0),
  [62] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym__function_name, 1, 0, 1),
  [64] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_field_identifier, 1, 0, 0),
  [66] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_function_expression, 5, 0, 12),
  [68] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_function_expression, 5, 0, 12),
  [70] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_function_expression, 8, 0, 1),
  [72] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_function_expression, 8, 0, 1),
  [74] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_function_expression, 4, 0, 12),
  [76] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_function_expression, 4, 0, 12),
  [78] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym__value_expression, 1, 0, 0),
  [80] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym__value_expression, 1, 0, 0),
  [82] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 2, 0, 2),
  [84] = {.entry = {.count = 1, .reusable = true}}, SHIFT(205),
  [86] = {.entry = {.count = 1, .reusable = true}}, SHIFT(179),
  [88] = {.entry = {.count = 1, .reusable = true}}, SHIFT(292),
  [90] = {.entry = {.count = 1, .reusable = true}}, SHIFT(429),
  [92] = {.entry = {.count = 1, .reusable = true}}, SHIFT(34),
  [94] = {.entry = {.count = 1, .reusable = true}}, SHIFT(101),
  [96] = {.entry = {.count = 1, .reusable = true}}, SHIFT(468),
  [98] = {.entry = {.count = 1, .reusable = true}}, SHIFT(489),
  [100] = {.entry = {.count = 1, .reusable = true}}, SHIFT(153),
  [102] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 3, 0, 3),
  [104] = {.entry = {.count = 1, .reusable = true}}, SHIFT(8),
  [106] = {.entry = {.count = 1, .reusable = false}}, SHIFT(8),
  [108] = {.entry = {.count = 1, .reusable = false}}, SHIFT(494),
  [110] = {.entry = {.count = 1, .reusable = true}}, SHIFT(494),
  [112] = {.entry = {.count = 1, .reusable = true}}, SHIFT(433),
  [114] = {.entry = {.count = 1, .reusable = false}}, SHIFT(730),
  [116] = {.entry = {.count = 1, .reusable = false}}, SHIFT(765),
  [118] = {.entry = {.count = 1, .reusable = false}}, SHIFT(754),
  [120] = {.entry = {.count = 1, .reusable = false}}, SHIFT(183),
  [122] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 3, 0, 6),
  [124] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 4, 0, 15),
  [126] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_dotted_identifier_repeat1, 2, 0, 0), SHIFT_REPEAT(800),
  [129] = {.entry = {.count = 1, .reusable = true}}, SHIFT(800),
  [131] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_storage_identifier, 1, 0, 0),
  [133] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_storage_identifier, 1, 0, 0),
  [135] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_from_clause, 2, 0, 0),
  [137] = {.entry = {.count = 1, .reusable = true}}, SHIFT(298),
  [139] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_from_clause, 2, 0, 0),
  [141] = {.entry = {.count = 1, .reusable = false}}, SHIFT(771),
  [143] = {.entry = {.count = 1, .reusable = false}}, SHIFT(57),
  [145] = {.entry = {.count = 1, .reusable = true}}, SHIFT(30),
  [147] = {.entry = {.count = 1, .reusable = false}}, SHIFT(108),
  [149] = {.entry = {.count = 1, .reusable = false}}, SHIFT(809),
  [151] = {.entry = {.count = 1, .reusable = false}}, SHIFT(12),
  [153] = {.entry = {.count = 1, .reusable = true}}, SHIFT(32),
  [155] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 3, 0, 7),
  [157] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 4, 0, 29),
  [159] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 4, 0, 16),
  [161] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_from_clause_repeat1, 2, 0, 0),
  [163] = {.entry = {.count = 1, .reusable = false}}, REDUCE(aux_sym_from_clause_repeat1, 2, 0, 0),
  [165] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 58),
  [167] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_from_clause_repeat1, 2, 0, 0), SHIFT_REPEAT(298),
  [170] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_subquery, 3, 0, 0),
  [172] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_subquery, 3, 0, 0),
  [174] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_from_clause, 3, 0, 0),
  [176] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_boolean, 1, 0, 0),
  [178] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_boolean, 1, 0, 0),
  [180] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_date_literal, 1, 0, 0),
  [182] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_date_literal, 1, 0, 0),
  [184] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_null_literal, 1, 0, 0),
  [186] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_null_literal, 1, 0, 0),
  [188] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym__soql_literal, 1, 0, 0),
  [190] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym__soql_literal, 1, 0, 0),
  [192] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_date_literal_with_param, 3, 0, 0),
  [194] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_date_literal_with_param, 3, 0, 0),
  [196] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym__boolean_expression, 1, 0, 0),
  [198] = {.entry = {.count = 1, .reusable = true}}, SHIFT(88),
  [200] = {.entry = {.count = 1, .reusable = false}}, SHIFT(89),
  [202] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_order_expression, 1, 0, 0),
  [204] = {.entry = {.count = 1, .reusable = true}}, SHIFT(122),
  [206] = {.entry = {.count = 1, .reusable = true}}, SHIFT(173),
  [208] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 4, 0, 27),
  [210] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_storage_alias, 3, 0, 0),
  [212] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 4, 0, 34),
  [214] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_using_lookup_clause, 2, 0, 49),
  [216] = {.entry = {.count = 1, .reusable = true}}, SHIFT(606),
  [218] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_storage_alias, 2, 0, 0),
  [220] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 56),
  [222] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 63),
  [224] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 94),
  [226] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_using_lookup_bind_clause, 3, 0, 0),
  [228] = {.entry = {.count = 1, .reusable = true}}, SHIFT(587),
  [230] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 150),
  [232] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_using_lookup_bind_clause, 2, 0, 0),
  [234] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_using_lookup_bind_clause_repeat1, 2, 0, 0),
  [236] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_using_lookup_bind_clause_repeat1, 2, 0, 0), SHIFT_REPEAT(587),
  [239] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 4, 0, 13),
  [241] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 3, 0, 4),
  [243] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_comparable_list, 4, 0, 0),
  [245] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_comparable_list, 4, 0, 0),
  [247] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym__condition_expression, 1, 0, 0),
  [249] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym__condition_expression, 1, 0, 0),
  [251] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym__condition_expression, 3, 0, 0),
  [253] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym__condition_expression, 3, 0, 0),
  [255] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_and_expression_repeat1, 2, 0, 0),
  [257] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_and_expression_repeat1, 2, 0, 0), SHIFT_REPEAT(88),
  [260] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_or_expression_repeat1, 2, 0, 0),
  [262] = {.entry = {.count = 2, .reusable = false}}, REDUCE(aux_sym_or_expression_repeat1, 2, 0, 0), SHIFT_REPEAT(89),
  [265] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym__value_comparison, 2, 0, 0),
  [267] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym__value_comparison, 2, 0, 0),
  [269] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym__set_comparison, 2, 0, 0),
  [271] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym__set_comparison, 2, 0, 0),
  [273] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_and_expression, 2, 0, 0),
  [275] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_or_expression, 2, 0, 0),
  [277] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_comparison_expression, 2, 0, 0),
  [279] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_comparison_expression, 2, 0, 0),
  [281] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_comparable_list, 3, 0, 0),
  [283] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_comparable_list, 3, 0, 0),
  [285] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_using_lookup_bind_expression, 3, 0, 470),
  [287] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym__comparison, 1, 0, 0),
  [289] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym__comparison, 1, 0, 0),
  [291] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 54),
  [293] = {.entry = {.count = 1, .reusable = false}}, REDUCE(aux_sym_or_expression_repeat1, 2, 0, 0),
  [295] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 62),
  [297] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 4, 0, 33),
  [299] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym__group_by_expression, 2, 0, 0),
  [301] = {.entry = {.count = 1, .reusable = true}}, SHIFT(255),
  [303] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 68),
  [305] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 92),
  [307] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_with_data_cat_expression, 3, 0, 0),
  [309] = {.entry = {.count = 1, .reusable = true}}, SHIFT(742),
  [311] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 148),
  [313] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 3, 0, 11),
  [315] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 155),
  [317] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 169),
  [319] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 214),
  [321] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 99),
  [323] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_with_data_cat_expression_repeat1, 2, 0, 0),
  [325] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_with_data_cat_expression_repeat1, 2, 0, 0), SHIFT_REPEAT(742),
  [328] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 284),
  [330] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_using_clause, 2, 0, 0),
  [332] = {.entry = {.count = 1, .reusable = true}}, SHIFT(160),
  [334] = {.entry = {.count = 1, .reusable = true}}, SHIFT(758),
  [336] = {.entry = {.count = 1, .reusable = true}}, SHIFT(769),
  [338] = {.entry = {.count = 1, .reusable = true}}, SHIFT(654),
  [340] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 113),
  [342] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_using_lookup_clause, 3, 0, 49),
  [344] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_using_listview_clause, 3, 0, 0),
  [346] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym__group_by_expression_repeat1, 2, 0, 0),
  [348] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym__group_by_expression_repeat1, 2, 0, 0), SHIFT_REPEAT(255),
  [351] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 4, 0, 25),
  [353] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym__group_by_expression, 1, 0, 0),
  [355] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 4, 0, 39),
  [357] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_using_scope_clause, 2, 0, 0),
  [359] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_with_data_cat_expression, 2, 0, 0),
  [361] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_order_expression, 2, 0, 0),
  [363] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 4, 0, 20),
  [365] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_using_scope_type, 1, 0, 0),
  [367] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_not_expression, 2, 0, 0),
  [369] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_where_clause, 2, 0, 0),
  [371] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_with_data_cat_filter, 3, 0, 0),
  [373] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_order_direction, 1, 0, 0),
  [375] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_with_data_cat_filter, 5, 0, 0),
  [377] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_with_data_cat_filter, 6, 0, 0),
  [379] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_group_by_clause, 2, 0, 0),
  [381] = {.entry = {.count = 1, .reusable = true}}, SHIFT(29),
  [383] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_with_clause, 2, 0, 0),
  [385] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 242),
  [387] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 75),
  [389] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_with_record_visibility_expression, 4, 0, 0),
  [391] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 85),
  [393] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 89),
  [395] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 51),
  [397] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 277),
  [399] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 281),
  [401] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 4, 0, 36),
  [403] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 292),
  [405] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 312),
  [407] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 352),
  [409] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 96),
  [411] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 4, 0, 30),
  [413] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 106),
  [415] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 110),
  [417] = {.entry = {.count = 1, .reusable = true}}, SHIFT(115),
  [419] = {.entry = {.count = 1, .reusable = false}}, SHIFT(115),
  [421] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_with_record_visibility_expression, 5, 0, 0),
  [423] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 408),
  [425] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 222),
  [427] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_with_user_id_type, 3, 0, 0),
  [429] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 121),
  [431] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_order_by_clause_repeat1, 2, 0, 0),
  [433] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_order_by_clause_repeat1, 2, 0, 0), SHIFT_REPEAT(170),
  [436] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 141),
  [438] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 145),
  [440] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 59),
  [442] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_order_by_clause, 3, 0, 0),
  [444] = {.entry = {.count = 1, .reusable = true}}, SHIFT(170),
  [446] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 152),
  [448] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 4, 0, 46),
  [450] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 162),
  [452] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 166),
  [454] = {.entry = {.count = 1, .reusable = true}}, SHIFT(775),
  [456] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_with_type, 1, 0, 0),
  [458] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 4, 0, 22),
  [460] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_order_by_clause, 2, 0, 0),
  [462] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 65),
  [464] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 177),
  [466] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 207),
  [468] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 211),
  [470] = {.entry = {.count = 1, .reusable = true}}, SHIFT(666),
  [472] = {.entry = {.count = 1, .reusable = true}}, SHIFT(792),
  [474] = {.entry = {.count = 1, .reusable = true}}, SHIFT(461),
  [476] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 4, 0, 17),
  [478] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 3, 0, 8),
  [480] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_group_by_clause, 3, 0, 0),
  [482] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_order_null_direction, 1, 0, 0),
  [484] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_having_clause, 2, 0, 0),
  [486] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_order_expression, 3, 0, 0),
  [488] = {.entry = {.count = 1, .reusable = true}}, SHIFT(555),
  [490] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_field_list, 1, 0, 0),
  [492] = {.entry = {.count = 1, .reusable = true}}, SHIFT(810),
  [494] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 223),
  [496] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 90),
  [498] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 97),
  [500] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 101),
  [502] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 107),
  [504] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 111),
  [506] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 116),
  [508] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 122),
  [510] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 130),
  [512] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_dotted_identifier_repeat1, 2, 0, 0), SHIFT_REPEAT(810),
  [515] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 136),
  [517] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 4, 0, 18),
  [519] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 142),
  [521] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 146),
  [523] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 52),
  [525] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 153),
  [527] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 4, 0, 23),
  [529] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 157),
  [531] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 163),
  [533] = {.entry = {.count = 1, .reusable = true}}, SHIFT(143),
  [535] = {.entry = {.count = 1, .reusable = true}}, SHIFT(752),
  [537] = {.entry = {.count = 1, .reusable = true}}, SHIFT(759),
  [539] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 167),
  [541] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 172),
  [543] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 60),
  [545] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 178),
  [547] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 186),
  [549] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 196),
  [551] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 202),
  [553] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 208),
  [555] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 212),
  [557] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 217),
  [559] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 4, 0, 31),
  [561] = {.entry = {.count = 1, .reusable = false}}, SHIFT(774),
  [563] = {.entry = {.count = 1, .reusable = true}}, SHIFT(762),
  [565] = {.entry = {.count = 1, .reusable = false}}, SHIFT(423),
  [567] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 231),
  [569] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 237),
  [571] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 243),
  [573] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 251),
  [575] = {.entry = {.count = 1, .reusable = true}}, SHIFT(673),
  [577] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 4, 0, 37),
  [579] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 266),
  [581] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 272),
  [583] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 278),
  [585] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 282),
  [587] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 287),
  [589] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 4, 0, 41),
  [591] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 293),
  [593] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 301),
  [595] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 307),
  [597] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 313),
  [599] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 321),
  [601] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 341),
  [603] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 347),
  [605] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 353),
  [607] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 361),
  [609] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 376),
  [611] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 4, 0, 47),
  [613] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 397),
  [615] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 403),
  [617] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 66),
  [619] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 409),
  [621] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 417),
  [623] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 432),
  [625] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 454),
  [627] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 9, 0, 483),
  [629] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 70),
  [631] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 76),
  [633] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 80),
  [635] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 3, 0, 9),
  [637] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 86),
  [639] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_field_list_repeat1, 2, 0, 0),
  [641] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_type_of_clause_repeat1, 2, 0, 0),
  [643] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_type_of_clause_repeat1, 2, 0, 0), SHIFT_REPEAT(792),
  [646] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_field_list, 2, 0, 0),
  [648] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_for_clause_repeat1, 2, 0, 0),
  [650] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_for_clause_repeat1, 2, 0, 0), SHIFT_REPEAT(346),
  [653] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_for_clause, 2, 0, 0),
  [655] = {.entry = {.count = 1, .reusable = true}}, SHIFT(346),
  [657] = {.entry = {.count = 1, .reusable = true}}, SHIFT(113),
  [659] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_for_clause, 3, 0, 0),
  [661] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_field_list_repeat1, 2, 0, 0), SHIFT_REPEAT(555),
  [664] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_limit_clause, 2, 0, 0),
  [666] = {.entry = {.count = 1, .reusable = true}}, SHIFT(734),
  [668] = {.entry = {.count = 1, .reusable = false}}, SHIFT(734),
  [670] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 289),
  [672] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 159),
  [674] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 161),
  [676] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 95),
  [678] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 165),
  [680] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_for_type, 1, 0, 0),
  [682] = {.entry = {.count = 1, .reusable = true}}, SHIFT(760),
  [684] = {.entry = {.count = 1, .reusable = true}}, SHIFT(766),
  [686] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 171),
  [688] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 174),
  [690] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 176),
  [692] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 100),
  [694] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 181),
  [696] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 185),
  [698] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 103),
  [700] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 188),
  [702] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 191),
  [704] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 195),
  [706] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 105),
  [708] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 198),
  [710] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 201),
  [712] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 204),
  [714] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 206),
  [716] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 4, 0, 28),
  [718] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 210),
  [720] = {.entry = {.count = 1, .reusable = true}}, SHIFT(273),
  [722] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 109),
  [724] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 216),
  [726] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_update_clause, 2, 0, 0),
  [728] = {.entry = {.count = 1, .reusable = true}}, SHIFT(457),
  [730] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 219),
  [732] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 221),
  [734] = {.entry = {.count = 1, .reusable = true}}, SHIFT(27),
  [736] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 115),
  [738] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 226),
  [740] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 230),
  [742] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 233),
  [744] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 236),
  [746] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 118),
  [748] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 239),
  [750] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 241),
  [752] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 120),
  [754] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 246),
  [756] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 250),
  [758] = {.entry = {.count = 2, .reusable = false}}, REDUCE(aux_sym_source_file_repeat1, 2, 0, 0), SHIFT_REPEAT(773),
  [761] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_source_file_repeat1, 2, 0, 0),
  [763] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 253),
  [765] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 257),
  [767] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 125),
  [769] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 4, 0, 35),
  [771] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 129),
  [773] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 261),
  [775] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 265),
  [777] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 64),
  [779] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 268),
  [781] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 271),
  [783] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 132),
  [785] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 274),
  [787] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 276),
  [789] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 280),
  [791] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 4, 0, 40),
  [793] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 286),
  [795] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 291),
  [797] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 296),
  [799] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 300),
  [801] = {.entry = {.count = 1, .reusable = true}}, SHIFT(777),
  [803] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 303),
  [805] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 306),
  [807] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 309),
  [809] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 311),
  [811] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 69),
  [813] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 316),
  [815] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 320),
  [817] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 323),
  [819] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 327),
  [821] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 332),
  [823] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 336),
  [825] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 340),
  [827] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_update_clause_repeat1, 2, 0, 0),
  [829] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_update_clause_repeat1, 2, 0, 0), SHIFT_REPEAT(457),
  [832] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 343),
  [834] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 346),
  [836] = {.entry = {.count = 1, .reusable = true}}, SHIFT(797),
  [838] = {.entry = {.count = 1, .reusable = true}}, SHIFT(423),
  [840] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 349),
  [842] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 351),
  [844] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 4, 0, 43),
  [846] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 356),
  [848] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 360),
  [850] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 72),
  [852] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 363),
  [854] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 367),
  [856] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 371),
  [858] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 375),
  [860] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_offset_clause, 2, 0, 0),
  [862] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 378),
  [864] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 382),
  [866] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 4, 0, 45),
  [868] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 388),
  [870] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 392),
  [872] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 396),
  [874] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 4, 0, 14),
  [876] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 399),
  [878] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 402),
  [880] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 405),
  [882] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 407),
  [884] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 74),
  [886] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 412),
  [888] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 416),
  [890] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 135),
  [892] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 419),
  [894] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 423),
  [896] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 427),
  [898] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 431),
  [900] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 79),
  [902] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 434),
  [904] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 438),
  [906] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 445),
  [908] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 449),
  [910] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 453),
  [912] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 138),
  [914] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 456),
  [916] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 460),
  [918] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 466),
  [920] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 140),
  [922] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_update_clause, 3, 0, 0),
  [924] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 9, 0, 474),
  [926] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 9, 0, 478),
  [928] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 9, 0, 482),
  [930] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 82),
  [932] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 9, 0, 485),
  [934] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 9, 0, 489),
  [936] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 9, 0, 495),
  [938] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 9, 0, 502),
  [940] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 10, 0, 510),
  [942] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 144),
  [944] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 84),
  [946] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 4, 0, 21),
  [948] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 151),
  [950] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 3, 0, 5),
  [952] = {.entry = {.count = 1, .reusable = true}}, SHIFT(2),
  [954] = {.entry = {.count = 1, .reusable = true}}, SHIFT(73),
  [956] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 88),
  [958] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 50),
  [960] = {.entry = {.count = 1, .reusable = true}}, SHIFT(3),
  [962] = {.entry = {.count = 1, .reusable = true}}, SHIFT(451),
  [964] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 156),
  [966] = {.entry = {.count = 1, .reusable = true}}, SHIFT(807),
  [968] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 57),
  [970] = {.entry = {.count = 1, .reusable = true}}, SHIFT(124),
  [972] = {.entry = {.count = 1, .reusable = true}}, SHIFT(764),
  [974] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym__selectable_expression, 1, 0, 0),
  [976] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym__selectable_expression, 1, 0, 0),
  [978] = {.entry = {.count = 1, .reusable = false}}, SHIFT(707),
  [980] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_type_of_clause_repeat1, 1, 0, 0),
  [982] = {.entry = {.count = 1, .reusable = true}}, SHIFT(33),
  [984] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_select_clause, 2, 0, 0),
  [986] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_update_type, 1, 0, 0),
  [988] = {.entry = {.count = 1, .reusable = true}}, SHIFT(77),
  [990] = {.entry = {.count = 1, .reusable = true}}, SHIFT(5),
  [992] = {.entry = {.count = 1, .reusable = true}}, SHIFT(426),
  [994] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_with_data_cat_filter_repeat1, 2, 0, 0),
  [996] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_with_data_cat_filter_repeat1, 2, 0, 0), SHIFT_REPEAT(764),
  [999] = {.entry = {.count = 1, .reusable = true}}, SHIFT(180),
  [1001] = {.entry = {.count = 1, .reusable = true}}, SHIFT(144),
  [1003] = {.entry = {.count = 1, .reusable = true}}, SHIFT(315),
  [1005] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_select_clause, 3, 0, 0),
  [1007] = {.entry = {.count = 1, .reusable = true}}, SHIFT(67),
  [1009] = {.entry = {.count = 1, .reusable = true}}, SHIFT(330),
  [1011] = {.entry = {.count = 1, .reusable = true}}, SHIFT(182),
  [1013] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_with_record_visibility_expression_repeat1, 2, 0, 0),
  [1015] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_with_record_visibility_expression_repeat1, 2, 0, 0), SHIFT_REPEAT(315),
  [1018] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_comparable_list_repeat1, 2, 0, 0),
  [1020] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_comparable_list_repeat1, 2, 0, 0), SHIFT_REPEAT(5),
  [1023] = {.entry = {.count = 1, .reusable = true}}, SHIFT(123),
  [1025] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_or_expression_repeat1, 2, 0, 0), SHIFT_REPEAT(113),
  [1028] = {.entry = {.count = 1, .reusable = true}}, SHIFT(408),
  [1030] = {.entry = {.count = 1, .reusable = true}}, SHIFT(16),
  [1032] = {.entry = {.count = 1, .reusable = true}}, SHIFT(14),
  [1034] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_select_clause_repeat1, 2, 0, 0), SHIFT_REPEAT(33),
  [1037] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_select_clause_repeat1, 2, 0, 0),
  [1039] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_when_expression, 4, 0, 0),
  [1041] = {.entry = {.count = 1, .reusable = true}}, SHIFT(453),
  [1043] = {.entry = {.count = 1, .reusable = true}}, SHIFT(454),
  [1045] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_function_expression_repeat1, 2, 0, 0),
  [1047] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_function_expression_repeat1, 2, 0, 0), SHIFT_REPEAT(182),
  [1050] = {.entry = {.count = 1, .reusable = true}}, SHIFT(129),
  [1052] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 328),
  [1054] = {.entry = {.count = 1, .reusable = true}}, SHIFT(266),
  [1056] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 260),
  [1058] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 139),
  [1060] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 262),
  [1062] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 263),
  [1064] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 264),
  [1066] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 77),
  [1068] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 112),
  [1070] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 267),
  [1072] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 269),
  [1074] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 270),
  [1076] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 143),
  [1078] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 114),
  [1080] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 273),
  [1082] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 4, 0, 19),
  [1084] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 275),
  [1086] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 78),
  [1088] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 137),
  [1090] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 147),
  [1092] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 279),
  [1094] = {.entry = {.count = 1, .reusable = true}}, SHIFT(417),
  [1096] = {.entry = {.count = 1, .reusable = true}}, SHIFT(364),
  [1098] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 149),
  [1100] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 283),
  [1102] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 285),
  [1104] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 117),
  [1106] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_set_comparison_operator, 1, 0, 0),
  [1108] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 288),
  [1110] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 119),
  [1112] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 290),
  [1114] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 154),
  [1116] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 81),
  [1118] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 294),
  [1120] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 295),
  [1122] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 83),
  [1124] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 297),
  [1126] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 298),
  [1128] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 299),
  [1130] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 158),
  [1132] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 123),
  [1134] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 302),
  [1136] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 160),
  [1138] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 304),
  [1140] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 305),
  [1142] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 124),
  [1144] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 4, 0, 24),
  [1146] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 308),
  [1148] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 310),
  [1150] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 164),
  [1152] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 4, 0, 42),
  [1154] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 126),
  [1156] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 314),
  [1158] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 315),
  [1160] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 127),
  [1162] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 317),
  [1164] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 318),
  [1166] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 319),
  [1168] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 128),
  [1170] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 168),
  [1172] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 322),
  [1174] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 4, 0, 26),
  [1176] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 324),
  [1178] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 325),
  [1180] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 326),
  [1182] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 170),
  [1184] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 329),
  [1186] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 330),
  [1188] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 331),
  [1190] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 61),
  [1192] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 333),
  [1194] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 334),
  [1196] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 335),
  [1198] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 173),
  [1200] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 337),
  [1202] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 338),
  [1204] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 339),
  [1206] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 131),
  [1208] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 175),
  [1210] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 342),
  [1212] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 87),
  [1214] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 344),
  [1216] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 345),
  [1218] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 133),
  [1220] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 348),
  [1222] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 179),
  [1224] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 350),
  [1226] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 180),
  [1228] = {.entry = {.count = 1, .reusable = true}}, SHIFT(256),
  [1230] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 182),
  [1232] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 354),
  [1234] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 355),
  [1236] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 183),
  [1238] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 357),
  [1240] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 358),
  [1242] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 359),
  [1244] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 184),
  [1246] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 362),
  [1248] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_type_of_clause, 5, 0, 0),
  [1250] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 364),
  [1252] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 365),
  [1254] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 366),
  [1256] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 187),
  [1258] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 368),
  [1260] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 369),
  [1262] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 370),
  [1264] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 372),
  [1266] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 373),
  [1268] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 374),
  [1270] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 189),
  [1272] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 190),
  [1274] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 377),
  [1276] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 379),
  [1278] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 380),
  [1280] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 381),
  [1282] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 192),
  [1284] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 383),
  [1286] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 384),
  [1288] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 385),
  [1290] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 4, 0, 44),
  [1292] = {.entry = {.count = 1, .reusable = true}}, SHIFT(788),
  [1294] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 193),
  [1296] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 194),
  [1298] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 4, 0, 48),
  [1300] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_with_record_visibility_param, 3, 0, 0),
  [1302] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 197),
  [1304] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 386),
  [1306] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 387),
  [1308] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 199),
  [1310] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 389),
  [1312] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 390),
  [1314] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 391),
  [1316] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 200),
  [1318] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 393),
  [1320] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 394),
  [1322] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 395),
  [1324] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 91),
  [1326] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 398),
  [1328] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 203),
  [1330] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 400),
  [1332] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 401),
  [1334] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 205),
  [1336] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 404),
  [1338] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 406),
  [1340] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 93),
  [1342] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 209),
  [1344] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 410),
  [1346] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 411),
  [1348] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 413),
  [1350] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 414),
  [1352] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 415),
  [1354] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 418),
  [1356] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 213),
  [1358] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 420),
  [1360] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 421),
  [1362] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 422),
  [1364] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 67),
  [1366] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 424),
  [1368] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 425),
  [1370] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 426),
  [1372] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 215),
  [1374] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 428),
  [1376] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 429),
  [1378] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 430),
  [1380] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 98),
  [1382] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 433),
  [1384] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 218),
  [1386] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 435),
  [1388] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 436),
  [1390] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 437),
  [1392] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 439),
  [1394] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 440),
  [1396] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 441),
  [1398] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 442),
  [1400] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 443),
  [1402] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 444),
  [1404] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 220),
  [1406] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 446),
  [1408] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 447),
  [1410] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 448),
  [1412] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 450),
  [1414] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 451),
  [1416] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 452),
  [1418] = {.entry = {.count = 1, .reusable = true}}, SHIFT(267),
  [1420] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 455),
  [1422] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 224),
  [1424] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 457),
  [1426] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 458),
  [1428] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 459),
  [1430] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 225),
  [1432] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 461),
  [1434] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 462),
  [1436] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 463),
  [1438] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 464),
  [1440] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 465),
  [1442] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_type_of_clause, 3, 0, 0),
  [1444] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 467),
  [1446] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 468),
  [1448] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 469),
  [1450] = {.entry = {.count = 1, .reusable = true}}, SHIFT(776),
  [1452] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 227),
  [1454] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 228),
  [1456] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_type_of_clause, 4, 0, 0),
  [1458] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 229),
  [1460] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 53),
  [1462] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 9, 0, 471),
  [1464] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 9, 0, 472),
  [1466] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 9, 0, 473),
  [1468] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 232),
  [1470] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 9, 0, 475),
  [1472] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 9, 0, 476),
  [1474] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 9, 0, 477),
  [1476] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 102),
  [1478] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 9, 0, 479),
  [1480] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 9, 0, 480),
  [1482] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 9, 0, 481),
  [1484] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 234),
  [1486] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 235),
  [1488] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 9, 0, 484),
  [1490] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 9, 0, 486),
  [1492] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 9, 0, 487),
  [1494] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 9, 0, 488),
  [1496] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 71),
  [1498] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 9, 0, 490),
  [1500] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 9, 0, 491),
  [1502] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 9, 0, 492),
  [1504] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 9, 0, 493),
  [1506] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 9, 0, 494),
  [1508] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 238),
  [1510] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 9, 0, 496),
  [1512] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 9, 0, 497),
  [1514] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 9, 0, 498),
  [1516] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 9, 0, 499),
  [1518] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 9, 0, 500),
  [1520] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 9, 0, 501),
  [1522] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_alias_expression, 2, 0, 0),
  [1524] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 9, 0, 503),
  [1526] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 9, 0, 504),
  [1528] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 9, 0, 505),
  [1530] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 9, 0, 506),
  [1532] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_fields_expression, 4, 0, 0),
  [1534] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 240),
  [1536] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 10, 0, 507),
  [1538] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 10, 0, 508),
  [1540] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 10, 0, 509),
  [1542] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 104),
  [1544] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 10, 0, 511),
  [1546] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 10, 0, 512),
  [1548] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 10, 0, 513),
  [1550] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 10, 0, 514),
  [1552] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 10, 0, 515),
  [1554] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 11, 0, 516),
  [1556] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 4, 0, 32),
  [1558] = {.entry = {.count = 1, .reusable = false}}, REDUCE(aux_sym_source_file_repeat1, 1, 0, 0),
  [1560] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_source_file_repeat1, 1, 0, 0),
  [1562] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 244),
  [1564] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 245),
  [1566] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 247),
  [1568] = {.entry = {.count = 1, .reusable = true}}, SHIFT(167),
  [1570] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 248),
  [1572] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 249),
  [1574] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 252),
  [1576] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_with_data_cat_filter_type, 1, 0, 0),
  [1578] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 254),
  [1580] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 255),
  [1582] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 256),
  [1584] = {.entry = {.count = 1, .reusable = true}}, SHIFT(808),
  [1586] = {.entry = {.count = 1, .reusable = true}}, SHIFT(120),
  [1588] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 258),
  [1590] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 259),
  [1592] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 73),
  [1594] = {.entry = {.count = 1, .reusable = true}}, SHIFT(806),
  [1596] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 3, 0, 10),
  [1598] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 55),
  [1600] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 134),
  [1602] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_header_comment, 2, 0, 0),
  [1604] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_header_comment, 2, 0, 0),
  [1606] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 108),
  [1608] = {.entry = {.count = 1, .reusable = true}}, SHIFT(746),
  [1610] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 4, 0, 38),
  [1612] = {.entry = {.count = 1, .reusable = true}}, SHIFT(353),
  [1614] = {.entry = {.count = 1, .reusable = false}}, SHIFT(811),
  [1616] = {.entry = {.count = 1, .reusable = false}}, SHIFT(755),
  [1618] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_source_file, 2, 0, 0),
  [1620] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_else_expression, 2, 0, 0),
  [1622] = {.entry = {.count = 1, .reusable = true}}, SHIFT(784),
  [1624] = {.entry = {.count = 1, .reusable = true}}, SHIFT(789),
  [1626] = {.entry = {.count = 1, .reusable = true}}, SHIFT(794),
  [1628] = {.entry = {.count = 1, .reusable = true}}, SHIFT(565),
  [1630] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_geo_location_type, 1, 0, 0),
  [1632] = {.entry = {.count = 1, .reusable = true}}, SHIFT(159),
  [1634] = {.entry = {.count = 1, .reusable = true}}, SHIFT(712),
  [1636] = {.entry = {.count = 1, .reusable = true}}, SHIFT(335),
  [1638] = {.entry = {.count = 1, .reusable = true}}, SHIFT(428),
  [1640] = {.entry = {.count = 1, .reusable = true}}, SHIFT(790),
  [1642] = {.entry = {.count = 1, .reusable = true}}, SHIFT(274),
  [1644] = {.entry = {.count = 1, .reusable = true}}, SHIFT(783),
  [1646] = {.entry = {.count = 1, .reusable = true}}, SHIFT(53),
  [1648] = {.entry = {.count = 1, .reusable = true}}, SHIFT(181),
  [1650] = {.entry = {.count = 1, .reusable = false}}, SHIFT(750),
  [1652] = {.entry = {.count = 1, .reusable = true}}, SHIFT(744),
  [1654] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_count_expression, 3, 0, 12),
  [1656] = {.entry = {.count = 1, .reusable = true}}, SHIFT(782),
  [1658] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_fields_type, 1, 0, 0),
  [1660] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_source_file, 1, 0, 0),
  [1662] = {.entry = {.count = 1, .reusable = true}}, SHIFT(69),
  [1664] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym__soql_query_expression, 1, 0, 0),
  [1666] = {.entry = {.count = 1, .reusable = true}}, SHIFT(394),
  [1668] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_geo_location_type, 6, 0, 1),
  [1670] = {.entry = {.count = 1, .reusable = true}}, SHIFT(781),
  [1672] = {.entry = {.count = 1, .reusable = true}}, SHIFT(147),
  [1674] = {.entry = {.count = 1, .reusable = true}}, SHIFT(431),
  [1676] = {.entry = {.count = 1, .reusable = true}}, SHIFT(436),
  [1678] = {.entry = {.count = 1, .reusable = true}}, SHIFT(11),
  [1680] = {.entry = {.count = 1, .reusable = true}}, SHIFT(7),
  [1682] = {.entry = {.count = 1, .reusable = true}}, SHIFT(104),
  [1684] = {.entry = {.count = 1, .reusable = true}}, SHIFT(49),
  [1686] = {.entry = {.count = 1, .reusable = true}}, SHIFT(444),
  [1688] = {.entry = {.count = 1, .reusable = true}}, SHIFT(785),
  [1690] = {.entry = {.count = 1, .reusable = true}}, SHIFT(713),
  [1692] = {.entry = {.count = 1, .reusable = true}}, SHIFT(592),
  [1694] = {.entry = {.count = 1, .reusable = true}},  ACCEPT_INPUT(),
  [1696] = {.entry = {.count = 1, .reusable = true}}, SHIFT(452),
  [1698] = {.entry = {.count = 1, .reusable = true}}, SHIFT(249),
  [1700] = {.entry = {.count = 1, .reusable = true}}, SHIFT(15),
  [1702] = {.entry = {.count = 1, .reusable = true}}, SHIFT(43),
  [1704] = {.entry = {.count = 1, .reusable = true}}, SHIFT(31),
  [1706] = {.entry = {.count = 1, .reusable = true}}, SHIFT(171),
  [1708] = {.entry = {.count = 1, .reusable = true}}, SHIFT(796),
  [1710] = {.entry = {.count = 1, .reusable = true}}, SHIFT(798),
  [1712] = {.entry = {.count = 1, .reusable = true}}, SHIFT(804),
  [1714] = {.entry = {.count = 1, .reusable = true}}, SHIFT(670),
  [1716] = {.entry = {.count = 1, .reusable = true}}, SHIFT(217),
  [1718] = {.entry = {.count = 1, .reusable = true}}, SHIFT(443),
  [1720] = {.entry = {.count = 1, .reusable = true}}, SHIFT(418),
  [1722] = {.entry = {.count = 1, .reusable = true}}, SHIFT(259),
  [1724] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_formatting_comment, 2, 0, 0),
};

#ifdef __cplusplus
extern "C" {
#endif
#ifdef TREE_SITTER_HIDE_SYMBOLS
#define TS_PUBLIC
#elif defined(_WIN32)
#define TS_PUBLIC __declspec(dllexport)
#else
#define TS_PUBLIC __attribute__((visibility("default")))
#endif

TS_PUBLIC const TSLanguage *tree_sitter_soql(void) {
  static const TSLanguage language = {
    .version = LANGUAGE_VERSION,
    .symbol_count = SYMBOL_COUNT,
    .alias_count = ALIAS_COUNT,
    .token_count = TOKEN_COUNT,
    .external_token_count = EXTERNAL_TOKEN_COUNT,
    .state_count = STATE_COUNT,
    .large_state_count = LARGE_STATE_COUNT,
    .production_id_count = PRODUCTION_ID_COUNT,
    .field_count = FIELD_COUNT,
    .max_alias_sequence_length = MAX_ALIAS_SEQUENCE_LENGTH,
    .parse_table = &ts_parse_table[0][0],
    .small_parse_table = ts_small_parse_table,
    .small_parse_table_map = ts_small_parse_table_map,
    .parse_actions = ts_parse_actions,
    .symbol_names = ts_symbol_names,
    .field_names = ts_field_names,
    .field_map_slices = ts_field_map_slices,
    .field_map_entries = ts_field_map_entries,
    .symbol_metadata = ts_symbol_metadata,
    .public_symbol_map = ts_symbol_map,
    .alias_map = ts_non_terminal_alias_map,
    .alias_sequences = &ts_alias_sequences[0][0],
    .lex_modes = ts_lex_modes,
    .lex_fn = ts_lex,
    .primary_state_ids = ts_primary_state_ids,
  };
  return &language;
}
#ifdef __cplusplus
}
#endif
