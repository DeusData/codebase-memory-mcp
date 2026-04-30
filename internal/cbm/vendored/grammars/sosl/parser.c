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
#define STATE_COUNT 936
#define LARGE_STATE_COUNT 2
#define SYMBOL_COUNT 244
#define ALIAS_COUNT 0
#define TOKEN_COUNT 134
#define EXTERNAL_TOKEN_COUNT 0
#define FIELD_COUNT 15
#define MAX_ALIAS_SEQUENCE_LENGTH 11
#define PRODUCTION_ID_COUNT 517

enum ts_symbol_identifiers {
  anon_sym_SLASH_SLASH = 1,
  aux_sym_header_comment_token1 = 2,
  anon_sym_SLASH_SLASH_SLASH = 3,
  aux_sym_find_clause_token1 = 4,
  aux_sym_in_clause_token1 = 5,
  aux_sym_in_clause_token2 = 6,
  aux_sym_in_type_token1 = 7,
  aux_sym_in_type_token2 = 8,
  aux_sym_in_type_token3 = 9,
  aux_sym_in_type_token4 = 10,
  aux_sym_in_type_token5 = 11,
  sym_term_separator_start = 12,
  sym_term_separator_end = 13,
  sym_term = 14,
  aux_sym_returning_clause_token1 = 15,
  anon_sym_COMMA = 16,
  anon_sym_LPAREN = 17,
  anon_sym_RPAREN = 18,
  aux_sym_using_phrase_search_token1 = 19,
  aux_sym_using_advanced_search_token1 = 20,
  aux_sym_sosl_using_clause_token1 = 21,
  aux_sym_with_division_expression_token1 = 22,
  anon_sym_EQ = 23,
  aux_sym_with_highlight_token1 = 24,
  aux_sym_with_metadata_expression_token1 = 25,
  aux_sym_with_network_expression_token1 = 26,
  aux_sym_with_pricebook_expression_token1 = 27,
  aux_sym_with_snippet_expression_token1 = 28,
  aux_sym_with_snippet_expression_token2 = 29,
  aux_sym_with_spell_correction_expression_token1 = 30,
  aux_sym_sosl_with_clause_token1 = 31,
  aux_sym_select_clause_token1 = 32,
  aux_sym_using_scope_clause_token1 = 33,
  aux_sym_using_scope_type_token1 = 34,
  aux_sym_using_scope_type_token2 = 35,
  aux_sym_using_scope_type_token3 = 36,
  aux_sym_using_scope_type_token4 = 37,
  aux_sym_using_scope_type_token5 = 38,
  aux_sym_using_scope_type_token6 = 39,
  aux_sym_using_scope_type_token7 = 40,
  aux_sym_using_lookup_clause_token1 = 41,
  aux_sym_using_listview_clause_token1 = 42,
  aux_sym_using_lookup_bind_clause_token1 = 43,
  aux_sym_type_of_clause_token1 = 44,
  aux_sym_type_of_clause_token2 = 45,
  aux_sym_when_expression_token1 = 46,
  aux_sym_when_expression_token2 = 47,
  aux_sym_else_expression_token1 = 48,
  aux_sym_group_by_clause_token1 = 49,
  aux_sym_for_clause_token1 = 50,
  aux_sym_for_type_token1 = 51,
  aux_sym_for_type_token2 = 52,
  aux_sym_for_type_token3 = 53,
  aux_sym_having_clause_token1 = 54,
  aux_sym_from_clause_token1 = 55,
  aux_sym_storage_alias_token1 = 56,
  aux_sym_fields_type_token1 = 57,
  aux_sym_fields_type_token2 = 58,
  aux_sym_where_clause_token1 = 59,
  aux_sym_and_expression_token1 = 60,
  aux_sym_or_expression_token1 = 61,
  aux_sym_not_expression_token1 = 62,
  aux_sym_soql_with_type_token1 = 63,
  aux_sym_soql_with_type_token2 = 64,
  aux_sym_soql_with_type_token3 = 65,
  aux_sym_with_user_id_type_token1 = 66,
  aux_sym_with_record_visibility_expression_token1 = 67,
  aux_sym_with_record_visibility_param_token1 = 68,
  aux_sym_with_record_visibility_param_token2 = 69,
  aux_sym_with_record_visibility_param_token3 = 70,
  aux_sym_with_data_cat_expression_token1 = 71,
  aux_sym_with_data_cat_filter_type_token1 = 72,
  aux_sym_with_data_cat_filter_type_token2 = 73,
  aux_sym_with_data_cat_filter_type_token3 = 74,
  aux_sym_with_data_cat_filter_type_token4 = 75,
  aux_sym_limit_clause_token1 = 76,
  aux_sym_offset_clause_token1 = 77,
  aux_sym_update_type_token1 = 78,
  aux_sym_update_type_token2 = 79,
  aux_sym_order_by_clause_token1 = 80,
  aux_sym_order_direction_token1 = 81,
  aux_sym_order_direction_token2 = 82,
  aux_sym_order_null_direction_token1 = 83,
  aux_sym_order_null_direction_token2 = 84,
  aux_sym_geo_location_type_token1 = 85,
  aux_sym_function_expression_token1 = 86,
  anon_sym_DOT = 87,
  aux_sym_boolean_token1 = 88,
  aux_sym_boolean_token2 = 89,
  anon_sym_BANG_EQ = 90,
  anon_sym_LT_GT = 91,
  anon_sym_LT = 92,
  anon_sym_LT_EQ = 93,
  anon_sym_GT = 94,
  anon_sym_GT_EQ = 95,
  aux_sym_value_comparison_operator_token1 = 96,
  aux_sym_set_comparison_operator_token1 = 97,
  aux_sym_set_comparison_operator_token2 = 98,
  aux_sym_set_comparison_operator_token3 = 99,
  aux_sym_date_literal_token1 = 100,
  aux_sym_date_literal_token2 = 101,
  aux_sym_date_literal_token3 = 102,
  aux_sym_date_literal_token4 = 103,
  aux_sym_date_literal_token5 = 104,
  aux_sym_date_literal_token6 = 105,
  aux_sym_date_literal_token7 = 106,
  aux_sym_date_literal_token8 = 107,
  aux_sym_date_literal_token9 = 108,
  aux_sym_date_literal_token10 = 109,
  aux_sym_date_literal_token11 = 110,
  aux_sym_date_literal_token12 = 111,
  aux_sym_date_literal_token13 = 112,
  aux_sym_date_literal_token14 = 113,
  aux_sym_date_literal_token15 = 114,
  aux_sym_date_literal_token16 = 115,
  aux_sym_date_literal_token17 = 116,
  aux_sym_date_literal_token18 = 117,
  aux_sym_date_literal_token19 = 118,
  aux_sym_date_literal_token20 = 119,
  aux_sym_date_literal_token21 = 120,
  aux_sym_date_literal_token22 = 121,
  aux_sym_date_literal_token23 = 122,
  aux_sym_date_literal_with_param_token1 = 123,
  anon_sym_COLON = 124,
  sym_bound_apex_expression = 125,
  aux_sym_null_literal_token1 = 126,
  sym_string_literal = 127,
  sym_int = 128,
  sym_decimal = 129,
  sym_date = 130,
  sym_date_time = 131,
  sym_currency_literal = 132,
  sym_identifier = 133,
  sym_source_file = 134,
  sym_header_comment = 135,
  sym_formatting_comment = 136,
  sym__query_expression = 137,
  sym_sosl_query_body = 138,
  sym_find_clause = 139,
  sym_in_clause = 140,
  sym_in_type = 141,
  sym_returning_clause = 142,
  sym_sobject_return = 143,
  sym_selected_fields = 144,
  sym__selectable_expression = 145,
  sym_using_phrase_search = 146,
  sym_using_advanced_search = 147,
  sym_sosl_using_clause = 148,
  sym_subquery = 149,
  sym_with_division_expression = 150,
  sym_with_highlight = 151,
  sym_with_metadata_expression = 152,
  sym_with_network_expression = 153,
  sym_with_pricebook_expression = 154,
  sym_with_snippet_expression = 155,
  sym_with_spell_correction_expression = 156,
  sym_sosl_with_type = 157,
  sym_sosl_with_clause = 158,
  sym_soql_query_body = 159,
  sym_count_expression = 160,
  sym_select_clause = 161,
  sym_using_clause = 162,
  sym_using_scope_clause = 163,
  sym_using_scope_type = 164,
  sym_using_lookup_clause = 165,
  sym_using_listview_clause = 166,
  sym_using_lookup_bind_clause = 167,
  sym_using_lookup_bind_expression = 168,
  sym_type_of_clause = 169,
  sym_when_expression = 170,
  sym_else_expression = 171,
  sym_group_by_clause = 172,
  sym__group_by_expression = 173,
  sym_for_clause = 174,
  sym_for_type = 175,
  sym_having_clause = 176,
  sym_from_clause = 177,
  sym_storage_identifier = 178,
  sym_storage_alias = 179,
  sym_fields_expression = 180,
  sym_fields_type = 181,
  sym_where_clause = 182,
  sym__boolean_expression = 183,
  sym_and_expression = 184,
  sym_or_expression = 185,
  sym_not_expression = 186,
  sym__condition_expression = 187,
  sym_comparison_expression = 188,
  sym__comparison = 189,
  sym__value_comparison = 190,
  sym__set_comparison = 191,
  sym_comparable_list = 192,
  sym_soql_with_clause = 193,
  sym_soql_with_type = 194,
  sym_with_user_id_type = 195,
  sym_with_record_visibility_expression = 196,
  sym_with_record_visibility_param = 197,
  sym_with_data_cat_expression = 198,
  sym_with_data_cat_filter = 199,
  sym_with_data_cat_filter_type = 200,
  sym_limit_clause = 201,
  sym_offset_clause = 202,
  sym_update_clause = 203,
  sym_update_type = 204,
  sym_alias_expression = 205,
  sym_order_by_clause = 206,
  sym_order_expression = 207,
  sym_order_direction = 208,
  sym_order_null_direction = 209,
  sym_geo_location_type = 210,
  sym__value_expression = 211,
  sym_function_expression = 212,
  sym_dotted_identifier = 213,
  sym_field_identifier = 214,
  sym_field_list = 215,
  sym_boolean = 216,
  sym_value_comparison_operator = 217,
  sym_set_comparison_operator = 218,
  sym_date_literal = 219,
  sym_date_literal_with_param = 220,
  sym__function_name = 221,
  sym_null_literal = 222,
  sym__soql_literal = 223,
  aux_sym_sosl_query_body_repeat1 = 224,
  aux_sym_sosl_query_body_repeat2 = 225,
  aux_sym_returning_clause_repeat1 = 226,
  aux_sym_selected_fields_repeat1 = 227,
  aux_sym_using_lookup_bind_clause_repeat1 = 228,
  aux_sym_type_of_clause_repeat1 = 229,
  aux_sym__group_by_expression_repeat1 = 230,
  aux_sym_for_clause_repeat1 = 231,
  aux_sym_from_clause_repeat1 = 232,
  aux_sym_and_expression_repeat1 = 233,
  aux_sym_or_expression_repeat1 = 234,
  aux_sym_comparable_list_repeat1 = 235,
  aux_sym_with_record_visibility_expression_repeat1 = 236,
  aux_sym_with_data_cat_expression_repeat1 = 237,
  aux_sym_with_data_cat_filter_repeat1 = 238,
  aux_sym_update_clause_repeat1 = 239,
  aux_sym_order_by_clause_repeat1 = 240,
  aux_sym_function_expression_repeat1 = 241,
  aux_sym_dotted_identifier_repeat1 = 242,
  aux_sym_field_list_repeat1 = 243,
};

static const char * const ts_symbol_names[] = {
  [ts_builtin_sym_end] = "end",
  [anon_sym_SLASH_SLASH] = "//",
  [aux_sym_header_comment_token1] = "header_comment_token1",
  [anon_sym_SLASH_SLASH_SLASH] = "///",
  [aux_sym_find_clause_token1] = "FIND",
  [aux_sym_in_clause_token1] = "IN",
  [aux_sym_in_clause_token2] = "FIELDS",
  [aux_sym_in_type_token1] = "ALL",
  [aux_sym_in_type_token2] = "EMAIL",
  [aux_sym_in_type_token3] = "NAME",
  [aux_sym_in_type_token4] = "PHONE",
  [aux_sym_in_type_token5] = "SIDEBAR",
  [sym_term_separator_start] = "term_separator_start",
  [sym_term_separator_end] = "term_separator_end",
  [sym_term] = "term",
  [aux_sym_returning_clause_token1] = "RETURNING",
  [anon_sym_COMMA] = ",",
  [anon_sym_LPAREN] = "(",
  [anon_sym_RPAREN] = ")",
  [aux_sym_using_phrase_search_token1] = "PHRASE_SEARCH",
  [aux_sym_using_advanced_search_token1] = "ADVANCED_SEARCH",
  [aux_sym_sosl_using_clause_token1] = "USING",
  [aux_sym_with_division_expression_token1] = "DIVISION",
  [anon_sym_EQ] = "=",
  [aux_sym_with_highlight_token1] = "HIGHLIGHT",
  [aux_sym_with_metadata_expression_token1] = "METADATA",
  [aux_sym_with_network_expression_token1] = "NETWORK",
  [aux_sym_with_pricebook_expression_token1] = "PricebookId",
  [aux_sym_with_snippet_expression_token1] = "SNIPPET",
  [aux_sym_with_snippet_expression_token2] = "target_length",
  [aux_sym_with_spell_correction_expression_token1] = "SPELL_CORRECTION",
  [aux_sym_sosl_with_clause_token1] = "WITH",
  [aux_sym_select_clause_token1] = "SELECT",
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
  [aux_sym_fields_type_token1] = "CUSTOM",
  [aux_sym_fields_type_token2] = "STANDARD",
  [aux_sym_where_clause_token1] = "WHERE",
  [aux_sym_and_expression_token1] = "AND",
  [aux_sym_or_expression_token1] = "OR",
  [aux_sym_not_expression_token1] = "NOT",
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
  [aux_sym_set_comparison_operator_token1] = "NOT_IN",
  [aux_sym_set_comparison_operator_token2] = "INCLUDES",
  [aux_sym_set_comparison_operator_token3] = "EXCLUDES",
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
  [sym__query_expression] = "_query_expression",
  [sym_sosl_query_body] = "sosl_query_body",
  [sym_find_clause] = "find_clause",
  [sym_in_clause] = "in_clause",
  [sym_in_type] = "in_type",
  [sym_returning_clause] = "returning_clause",
  [sym_sobject_return] = "sobject_return",
  [sym_selected_fields] = "selected_fields",
  [sym__selectable_expression] = "_selectable_expression",
  [sym_using_phrase_search] = "using_phrase_search",
  [sym_using_advanced_search] = "using_advanced_search",
  [sym_sosl_using_clause] = "sosl_using_clause",
  [sym_subquery] = "subquery",
  [sym_with_division_expression] = "with_division_expression",
  [sym_with_highlight] = "with_highlight",
  [sym_with_metadata_expression] = "with_metadata_expression",
  [sym_with_network_expression] = "with_network_expression",
  [sym_with_pricebook_expression] = "with_pricebook_expression",
  [sym_with_snippet_expression] = "with_snippet_expression",
  [sym_with_spell_correction_expression] = "with_spell_correction_expression",
  [sym_sosl_with_type] = "with_type",
  [sym_sosl_with_clause] = "with_clause",
  [sym_soql_query_body] = "soql_query_body",
  [sym_count_expression] = "count_expression",
  [sym_select_clause] = "select_clause",
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
  [aux_sym_sosl_query_body_repeat1] = "sosl_query_body_repeat1",
  [aux_sym_sosl_query_body_repeat2] = "sosl_query_body_repeat2",
  [aux_sym_returning_clause_repeat1] = "returning_clause_repeat1",
  [aux_sym_selected_fields_repeat1] = "selected_fields_repeat1",
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
  [aux_sym_find_clause_token1] = aux_sym_find_clause_token1,
  [aux_sym_in_clause_token1] = aux_sym_in_clause_token1,
  [aux_sym_in_clause_token2] = aux_sym_in_clause_token2,
  [aux_sym_in_type_token1] = aux_sym_in_type_token1,
  [aux_sym_in_type_token2] = aux_sym_in_type_token2,
  [aux_sym_in_type_token3] = aux_sym_in_type_token3,
  [aux_sym_in_type_token4] = aux_sym_in_type_token4,
  [aux_sym_in_type_token5] = aux_sym_in_type_token5,
  [sym_term_separator_start] = sym_term_separator_start,
  [sym_term_separator_end] = sym_term_separator_end,
  [sym_term] = sym_term,
  [aux_sym_returning_clause_token1] = aux_sym_returning_clause_token1,
  [anon_sym_COMMA] = anon_sym_COMMA,
  [anon_sym_LPAREN] = anon_sym_LPAREN,
  [anon_sym_RPAREN] = anon_sym_RPAREN,
  [aux_sym_using_phrase_search_token1] = aux_sym_using_phrase_search_token1,
  [aux_sym_using_advanced_search_token1] = aux_sym_using_advanced_search_token1,
  [aux_sym_sosl_using_clause_token1] = aux_sym_sosl_using_clause_token1,
  [aux_sym_with_division_expression_token1] = aux_sym_with_division_expression_token1,
  [anon_sym_EQ] = anon_sym_EQ,
  [aux_sym_with_highlight_token1] = aux_sym_with_highlight_token1,
  [aux_sym_with_metadata_expression_token1] = aux_sym_with_metadata_expression_token1,
  [aux_sym_with_network_expression_token1] = aux_sym_with_network_expression_token1,
  [aux_sym_with_pricebook_expression_token1] = aux_sym_with_pricebook_expression_token1,
  [aux_sym_with_snippet_expression_token1] = aux_sym_with_snippet_expression_token1,
  [aux_sym_with_snippet_expression_token2] = aux_sym_with_snippet_expression_token2,
  [aux_sym_with_spell_correction_expression_token1] = aux_sym_with_spell_correction_expression_token1,
  [aux_sym_sosl_with_clause_token1] = aux_sym_sosl_with_clause_token1,
  [aux_sym_select_clause_token1] = aux_sym_select_clause_token1,
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
  [aux_sym_fields_type_token1] = aux_sym_fields_type_token1,
  [aux_sym_fields_type_token2] = aux_sym_fields_type_token2,
  [aux_sym_where_clause_token1] = aux_sym_where_clause_token1,
  [aux_sym_and_expression_token1] = aux_sym_and_expression_token1,
  [aux_sym_or_expression_token1] = aux_sym_or_expression_token1,
  [aux_sym_not_expression_token1] = aux_sym_not_expression_token1,
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
  [sym__query_expression] = sym__query_expression,
  [sym_sosl_query_body] = sym_sosl_query_body,
  [sym_find_clause] = sym_find_clause,
  [sym_in_clause] = sym_in_clause,
  [sym_in_type] = sym_in_type,
  [sym_returning_clause] = sym_returning_clause,
  [sym_sobject_return] = sym_sobject_return,
  [sym_selected_fields] = sym_selected_fields,
  [sym__selectable_expression] = sym__selectable_expression,
  [sym_using_phrase_search] = sym_using_phrase_search,
  [sym_using_advanced_search] = sym_using_advanced_search,
  [sym_sosl_using_clause] = sym_sosl_using_clause,
  [sym_subquery] = sym_subquery,
  [sym_with_division_expression] = sym_with_division_expression,
  [sym_with_highlight] = sym_with_highlight,
  [sym_with_metadata_expression] = sym_with_metadata_expression,
  [sym_with_network_expression] = sym_with_network_expression,
  [sym_with_pricebook_expression] = sym_with_pricebook_expression,
  [sym_with_snippet_expression] = sym_with_snippet_expression,
  [sym_with_spell_correction_expression] = sym_with_spell_correction_expression,
  [sym_sosl_with_type] = sym_sosl_with_type,
  [sym_sosl_with_clause] = sym_sosl_with_clause,
  [sym_soql_query_body] = sym_soql_query_body,
  [sym_count_expression] = sym_count_expression,
  [sym_select_clause] = sym_select_clause,
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
  [sym_soql_with_clause] = sym_sosl_with_clause,
  [sym_soql_with_type] = sym_sosl_with_type,
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
  [aux_sym_sosl_query_body_repeat1] = aux_sym_sosl_query_body_repeat1,
  [aux_sym_sosl_query_body_repeat2] = aux_sym_sosl_query_body_repeat2,
  [aux_sym_returning_clause_repeat1] = aux_sym_returning_clause_repeat1,
  [aux_sym_selected_fields_repeat1] = aux_sym_selected_fields_repeat1,
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
  [aux_sym_find_clause_token1] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_in_clause_token1] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_in_clause_token2] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_in_type_token1] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_in_type_token2] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_in_type_token3] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_in_type_token4] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_in_type_token5] = {
    .visible = true,
    .named = false,
  },
  [sym_term_separator_start] = {
    .visible = true,
    .named = true,
  },
  [sym_term_separator_end] = {
    .visible = true,
    .named = true,
  },
  [sym_term] = {
    .visible = true,
    .named = true,
  },
  [aux_sym_returning_clause_token1] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_COMMA] = {
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
  [aux_sym_using_phrase_search_token1] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_using_advanced_search_token1] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_sosl_using_clause_token1] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_with_division_expression_token1] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_EQ] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_with_highlight_token1] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_with_metadata_expression_token1] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_with_network_expression_token1] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_with_pricebook_expression_token1] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_with_snippet_expression_token1] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_with_snippet_expression_token2] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_with_spell_correction_expression_token1] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_sosl_with_clause_token1] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_select_clause_token1] = {
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
  [aux_sym_fields_type_token1] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_fields_type_token2] = {
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
  [sym__query_expression] = {
    .visible = false,
    .named = true,
  },
  [sym_sosl_query_body] = {
    .visible = true,
    .named = true,
  },
  [sym_find_clause] = {
    .visible = true,
    .named = true,
  },
  [sym_in_clause] = {
    .visible = true,
    .named = true,
  },
  [sym_in_type] = {
    .visible = true,
    .named = true,
  },
  [sym_returning_clause] = {
    .visible = true,
    .named = true,
  },
  [sym_sobject_return] = {
    .visible = true,
    .named = true,
  },
  [sym_selected_fields] = {
    .visible = true,
    .named = true,
  },
  [sym__selectable_expression] = {
    .visible = false,
    .named = true,
  },
  [sym_using_phrase_search] = {
    .visible = true,
    .named = true,
  },
  [sym_using_advanced_search] = {
    .visible = true,
    .named = true,
  },
  [sym_sosl_using_clause] = {
    .visible = true,
    .named = true,
  },
  [sym_subquery] = {
    .visible = true,
    .named = true,
  },
  [sym_with_division_expression] = {
    .visible = true,
    .named = true,
  },
  [sym_with_highlight] = {
    .visible = true,
    .named = true,
  },
  [sym_with_metadata_expression] = {
    .visible = true,
    .named = true,
  },
  [sym_with_network_expression] = {
    .visible = true,
    .named = true,
  },
  [sym_with_pricebook_expression] = {
    .visible = true,
    .named = true,
  },
  [sym_with_snippet_expression] = {
    .visible = true,
    .named = true,
  },
  [sym_with_spell_correction_expression] = {
    .visible = true,
    .named = true,
  },
  [sym_sosl_with_type] = {
    .visible = true,
    .named = true,
  },
  [sym_sosl_with_clause] = {
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
  [aux_sym_sosl_query_body_repeat1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_sosl_query_body_repeat2] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_returning_clause_repeat1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_selected_fields_repeat1] = {
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
  [13] = {.index = 31, .length = 1},
  [14] = {.index = 32, .length = 4},
  [15] = {.index = 36, .length = 4},
  [16] = {.index = 40, .length = 4},
  [17] = {.index = 44, .length = 4},
  [18] = {.index = 48, .length = 4},
  [19] = {.index = 52, .length = 4},
  [20] = {.index = 56, .length = 4},
  [21] = {.index = 60, .length = 4},
  [22] = {.index = 64, .length = 4},
  [23] = {.index = 68, .length = 4},
  [24] = {.index = 72, .length = 4},
  [25] = {.index = 76, .length = 4},
  [26] = {.index = 80, .length = 4},
  [27] = {.index = 84, .length = 4},
  [28] = {.index = 88, .length = 4},
  [29] = {.index = 92, .length = 4},
  [30] = {.index = 96, .length = 4},
  [31] = {.index = 100, .length = 4},
  [32] = {.index = 104, .length = 4},
  [33] = {.index = 108, .length = 4},
  [34] = {.index = 112, .length = 4},
  [35] = {.index = 116, .length = 4},
  [36] = {.index = 120, .length = 4},
  [37] = {.index = 124, .length = 4},
  [38] = {.index = 128, .length = 4},
  [39] = {.index = 132, .length = 4},
  [40] = {.index = 136, .length = 4},
  [41] = {.index = 140, .length = 4},
  [42] = {.index = 144, .length = 4},
  [43] = {.index = 148, .length = 4},
  [44] = {.index = 152, .length = 4},
  [45] = {.index = 156, .length = 4},
  [46] = {.index = 160, .length = 4},
  [47] = {.index = 164, .length = 4},
  [48] = {.index = 168, .length = 4},
  [49] = {.index = 172, .length = 4},
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
  [386] = {.index = 2234, .length = 2},
  [387] = {.index = 2236, .length = 8},
  [388] = {.index = 2244, .length = 8},
  [389] = {.index = 2252, .length = 8},
  [390] = {.index = 2260, .length = 8},
  [391] = {.index = 2268, .length = 8},
  [392] = {.index = 2276, .length = 8},
  [393] = {.index = 2284, .length = 8},
  [394] = {.index = 2292, .length = 8},
  [395] = {.index = 2300, .length = 8},
  [396] = {.index = 2308, .length = 8},
  [397] = {.index = 2316, .length = 8},
  [398] = {.index = 2324, .length = 8},
  [399] = {.index = 2332, .length = 8},
  [400] = {.index = 2340, .length = 8},
  [401] = {.index = 2348, .length = 8},
  [402] = {.index = 2356, .length = 8},
  [403] = {.index = 2364, .length = 8},
  [404] = {.index = 2372, .length = 8},
  [405] = {.index = 2380, .length = 8},
  [406] = {.index = 2388, .length = 8},
  [407] = {.index = 2396, .length = 8},
  [408] = {.index = 2404, .length = 8},
  [409] = {.index = 2412, .length = 8},
  [410] = {.index = 2420, .length = 8},
  [411] = {.index = 2428, .length = 8},
  [412] = {.index = 2436, .length = 8},
  [413] = {.index = 2444, .length = 8},
  [414] = {.index = 2452, .length = 8},
  [415] = {.index = 2460, .length = 8},
  [416] = {.index = 2468, .length = 8},
  [417] = {.index = 2476, .length = 8},
  [418] = {.index = 2484, .length = 8},
  [419] = {.index = 2492, .length = 8},
  [420] = {.index = 2500, .length = 8},
  [421] = {.index = 2508, .length = 8},
  [422] = {.index = 2516, .length = 8},
  [423] = {.index = 2524, .length = 8},
  [424] = {.index = 2532, .length = 8},
  [425] = {.index = 2540, .length = 8},
  [426] = {.index = 2548, .length = 8},
  [427] = {.index = 2556, .length = 8},
  [428] = {.index = 2564, .length = 8},
  [429] = {.index = 2572, .length = 8},
  [430] = {.index = 2580, .length = 8},
  [431] = {.index = 2588, .length = 8},
  [432] = {.index = 2596, .length = 8},
  [433] = {.index = 2604, .length = 8},
  [434] = {.index = 2612, .length = 8},
  [435] = {.index = 2620, .length = 8},
  [436] = {.index = 2628, .length = 8},
  [437] = {.index = 2636, .length = 8},
  [438] = {.index = 2644, .length = 8},
  [439] = {.index = 2652, .length = 8},
  [440] = {.index = 2660, .length = 8},
  [441] = {.index = 2668, .length = 8},
  [442] = {.index = 2676, .length = 8},
  [443] = {.index = 2684, .length = 8},
  [444] = {.index = 2692, .length = 8},
  [445] = {.index = 2700, .length = 8},
  [446] = {.index = 2708, .length = 8},
  [447] = {.index = 2716, .length = 8},
  [448] = {.index = 2724, .length = 8},
  [449] = {.index = 2732, .length = 8},
  [450] = {.index = 2740, .length = 8},
  [451] = {.index = 2748, .length = 8},
  [452] = {.index = 2756, .length = 8},
  [453] = {.index = 2764, .length = 8},
  [454] = {.index = 2772, .length = 8},
  [455] = {.index = 2780, .length = 8},
  [456] = {.index = 2788, .length = 8},
  [457] = {.index = 2796, .length = 8},
  [458] = {.index = 2804, .length = 8},
  [459] = {.index = 2812, .length = 8},
  [460] = {.index = 2820, .length = 8},
  [461] = {.index = 2828, .length = 8},
  [462] = {.index = 2836, .length = 8},
  [463] = {.index = 2844, .length = 8},
  [464] = {.index = 2852, .length = 8},
  [465] = {.index = 2860, .length = 8},
  [466] = {.index = 2868, .length = 8},
  [467] = {.index = 2876, .length = 8},
  [468] = {.index = 2884, .length = 8},
  [469] = {.index = 2892, .length = 8},
  [470] = {.index = 2900, .length = 8},
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
    {field_lookup_field, 1},
  [31] =
    {field_function_name, 0, .inherited = true},
  [32] =
    {field_from_clause, 1},
    {field_group_by_clause, 3},
    {field_select_clause, 0},
    {field_using_clause, 2},
  [36] =
    {field_for_clause, 3},
    {field_from_clause, 1},
    {field_select_clause, 0},
    {field_using_clause, 2},
  [40] =
    {field_from_clause, 1},
    {field_select_clause, 0},
    {field_using_clause, 2},
    {field_where_clause, 3},
  [44] =
    {field_from_clause, 1},
    {field_select_clause, 0},
    {field_using_clause, 2},
    {field_with_clause, 3},
  [48] =
    {field_from_clause, 1},
    {field_limit_clause, 3},
    {field_select_clause, 0},
    {field_using_clause, 2},
  [52] =
    {field_from_clause, 1},
    {field_offset_clause, 3},
    {field_select_clause, 0},
    {field_using_clause, 2},
  [56] =
    {field_from_clause, 1},
    {field_select_clause, 0},
    {field_update_clause, 3},
    {field_using_clause, 2},
  [60] =
    {field_from_clause, 1},
    {field_order_by_clause, 3},
    {field_select_clause, 0},
    {field_using_clause, 2},
  [64] =
    {field_for_clause, 3},
    {field_from_clause, 1},
    {field_group_by_clause, 2},
    {field_select_clause, 0},
  [68] =
    {field_from_clause, 1},
    {field_group_by_clause, 2},
    {field_limit_clause, 3},
    {field_select_clause, 0},
  [72] =
    {field_from_clause, 1},
    {field_group_by_clause, 2},
    {field_offset_clause, 3},
    {field_select_clause, 0},
  [76] =
    {field_from_clause, 1},
    {field_group_by_clause, 2},
    {field_select_clause, 0},
    {field_update_clause, 3},
  [80] =
    {field_from_clause, 1},
    {field_group_by_clause, 2},
    {field_order_by_clause, 3},
    {field_select_clause, 0},
  [84] =
    {field_for_clause, 2},
    {field_from_clause, 1},
    {field_select_clause, 0},
    {field_update_clause, 3},
  [88] =
    {field_from_clause, 1},
    {field_group_by_clause, 3},
    {field_select_clause, 0},
    {field_where_clause, 2},
  [92] =
    {field_for_clause, 3},
    {field_from_clause, 1},
    {field_select_clause, 0},
    {field_where_clause, 2},
  [96] =
    {field_from_clause, 1},
    {field_select_clause, 0},
    {field_where_clause, 2},
    {field_with_clause, 3},
  [100] =
    {field_from_clause, 1},
    {field_limit_clause, 3},
    {field_select_clause, 0},
    {field_where_clause, 2},
  [104] =
    {field_from_clause, 1},
    {field_offset_clause, 3},
    {field_select_clause, 0},
    {field_where_clause, 2},
  [108] =
    {field_from_clause, 1},
    {field_select_clause, 0},
    {field_update_clause, 3},
    {field_where_clause, 2},
  [112] =
    {field_from_clause, 1},
    {field_order_by_clause, 3},
    {field_select_clause, 0},
    {field_where_clause, 2},
  [116] =
    {field_from_clause, 1},
    {field_group_by_clause, 3},
    {field_select_clause, 0},
    {field_with_clause, 2},
  [120] =
    {field_for_clause, 3},
    {field_from_clause, 1},
    {field_select_clause, 0},
    {field_with_clause, 2},
  [124] =
    {field_from_clause, 1},
    {field_limit_clause, 3},
    {field_select_clause, 0},
    {field_with_clause, 2},
  [128] =
    {field_from_clause, 1},
    {field_offset_clause, 3},
    {field_select_clause, 0},
    {field_with_clause, 2},
  [132] =
    {field_from_clause, 1},
    {field_select_clause, 0},
    {field_update_clause, 3},
    {field_with_clause, 2},
  [136] =
    {field_from_clause, 1},
    {field_order_by_clause, 3},
    {field_select_clause, 0},
    {field_with_clause, 2},
  [140] =
    {field_for_clause, 3},
    {field_from_clause, 1},
    {field_limit_clause, 2},
    {field_select_clause, 0},
  [144] =
    {field_from_clause, 1},
    {field_limit_clause, 2},
    {field_offset_clause, 3},
    {field_select_clause, 0},
  [148] =
    {field_from_clause, 1},
    {field_limit_clause, 2},
    {field_select_clause, 0},
    {field_update_clause, 3},
  [152] =
    {field_for_clause, 3},
    {field_from_clause, 1},
    {field_offset_clause, 2},
    {field_select_clause, 0},
  [156] =
    {field_from_clause, 1},
    {field_offset_clause, 2},
    {field_select_clause, 0},
    {field_update_clause, 3},
  [160] =
    {field_for_clause, 3},
    {field_from_clause, 1},
    {field_order_by_clause, 2},
    {field_select_clause, 0},
  [164] =
    {field_from_clause, 1},
    {field_limit_clause, 3},
    {field_order_by_clause, 2},
    {field_select_clause, 0},
  [168] =
    {field_from_clause, 1},
    {field_offset_clause, 3},
    {field_order_by_clause, 2},
    {field_select_clause, 0},
  [172] =
    {field_from_clause, 1},
    {field_order_by_clause, 2},
    {field_select_clause, 0},
    {field_update_clause, 3},
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
    {field_bound_value, 2},
    {field_field, 0},
  [2236] =
    {field_for_clause, 6},
    {field_from_clause, 1},
    {field_group_by_clause, 3},
    {field_limit_clause, 4},
    {field_offset_clause, 5},
    {field_select_clause, 0},
    {field_update_clause, 7},
    {field_using_clause, 2},
  [2244] =
    {field_for_clause, 6},
    {field_from_clause, 1},
    {field_group_by_clause, 3},
    {field_limit_clause, 5},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_update_clause, 7},
    {field_using_clause, 2},
  [2252] =
    {field_for_clause, 7},
    {field_from_clause, 1},
    {field_group_by_clause, 3},
    {field_limit_clause, 5},
    {field_offset_clause, 6},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_using_clause, 2},
  [2260] =
    {field_from_clause, 1},
    {field_group_by_clause, 3},
    {field_limit_clause, 5},
    {field_offset_clause, 6},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_update_clause, 7},
    {field_using_clause, 2},
  [2268] =
    {field_for_clause, 6},
    {field_from_clause, 1},
    {field_group_by_clause, 3},
    {field_offset_clause, 5},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_update_clause, 7},
    {field_using_clause, 2},
  [2276] =
    {field_for_clause, 6},
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_limit_clause, 5},
    {field_select_clause, 0},
    {field_update_clause, 7},
    {field_using_clause, 2},
    {field_where_clause, 3},
  [2284] =
    {field_for_clause, 7},
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_limit_clause, 5},
    {field_offset_clause, 6},
    {field_select_clause, 0},
    {field_using_clause, 2},
    {field_where_clause, 3},
  [2292] =
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_limit_clause, 5},
    {field_offset_clause, 6},
    {field_select_clause, 0},
    {field_update_clause, 7},
    {field_using_clause, 2},
    {field_where_clause, 3},
  [2300] =
    {field_for_clause, 6},
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_offset_clause, 5},
    {field_select_clause, 0},
    {field_update_clause, 7},
    {field_using_clause, 2},
    {field_where_clause, 3},
  [2308] =
    {field_for_clause, 6},
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_order_by_clause, 5},
    {field_select_clause, 0},
    {field_update_clause, 7},
    {field_using_clause, 2},
    {field_where_clause, 3},
  [2316] =
    {field_for_clause, 7},
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_limit_clause, 6},
    {field_order_by_clause, 5},
    {field_select_clause, 0},
    {field_using_clause, 2},
    {field_where_clause, 3},
  [2324] =
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_limit_clause, 6},
    {field_offset_clause, 7},
    {field_order_by_clause, 5},
    {field_select_clause, 0},
    {field_using_clause, 2},
    {field_where_clause, 3},
  [2332] =
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_limit_clause, 6},
    {field_order_by_clause, 5},
    {field_select_clause, 0},
    {field_update_clause, 7},
    {field_using_clause, 2},
    {field_where_clause, 3},
  [2340] =
    {field_for_clause, 7},
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_offset_clause, 6},
    {field_order_by_clause, 5},
    {field_select_clause, 0},
    {field_using_clause, 2},
    {field_where_clause, 3},
  [2348] =
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_offset_clause, 6},
    {field_order_by_clause, 5},
    {field_select_clause, 0},
    {field_update_clause, 7},
    {field_using_clause, 2},
    {field_where_clause, 3},
  [2356] =
    {field_for_clause, 6},
    {field_from_clause, 1},
    {field_group_by_clause, 5},
    {field_select_clause, 0},
    {field_update_clause, 7},
    {field_using_clause, 2},
    {field_where_clause, 3},
    {field_with_clause, 4},
  [2364] =
    {field_for_clause, 7},
    {field_from_clause, 1},
    {field_group_by_clause, 5},
    {field_limit_clause, 6},
    {field_select_clause, 0},
    {field_using_clause, 2},
    {field_where_clause, 3},
    {field_with_clause, 4},
  [2372] =
    {field_from_clause, 1},
    {field_group_by_clause, 5},
    {field_limit_clause, 6},
    {field_offset_clause, 7},
    {field_select_clause, 0},
    {field_using_clause, 2},
    {field_where_clause, 3},
    {field_with_clause, 4},
  [2380] =
    {field_from_clause, 1},
    {field_group_by_clause, 5},
    {field_limit_clause, 6},
    {field_select_clause, 0},
    {field_update_clause, 7},
    {field_using_clause, 2},
    {field_where_clause, 3},
    {field_with_clause, 4},
  [2388] =
    {field_for_clause, 7},
    {field_from_clause, 1},
    {field_group_by_clause, 5},
    {field_offset_clause, 6},
    {field_select_clause, 0},
    {field_using_clause, 2},
    {field_where_clause, 3},
    {field_with_clause, 4},
  [2396] =
    {field_from_clause, 1},
    {field_group_by_clause, 5},
    {field_offset_clause, 6},
    {field_select_clause, 0},
    {field_update_clause, 7},
    {field_using_clause, 2},
    {field_where_clause, 3},
    {field_with_clause, 4},
  [2404] =
    {field_for_clause, 7},
    {field_from_clause, 1},
    {field_group_by_clause, 5},
    {field_order_by_clause, 6},
    {field_select_clause, 0},
    {field_using_clause, 2},
    {field_where_clause, 3},
    {field_with_clause, 4},
  [2412] =
    {field_from_clause, 1},
    {field_group_by_clause, 5},
    {field_limit_clause, 7},
    {field_order_by_clause, 6},
    {field_select_clause, 0},
    {field_using_clause, 2},
    {field_where_clause, 3},
    {field_with_clause, 4},
  [2420] =
    {field_from_clause, 1},
    {field_group_by_clause, 5},
    {field_offset_clause, 7},
    {field_order_by_clause, 6},
    {field_select_clause, 0},
    {field_using_clause, 2},
    {field_where_clause, 3},
    {field_with_clause, 4},
  [2428] =
    {field_from_clause, 1},
    {field_group_by_clause, 5},
    {field_order_by_clause, 6},
    {field_select_clause, 0},
    {field_update_clause, 7},
    {field_using_clause, 2},
    {field_where_clause, 3},
    {field_with_clause, 4},
  [2436] =
    {field_for_clause, 6},
    {field_from_clause, 1},
    {field_limit_clause, 5},
    {field_select_clause, 0},
    {field_update_clause, 7},
    {field_using_clause, 2},
    {field_where_clause, 3},
    {field_with_clause, 4},
  [2444] =
    {field_for_clause, 7},
    {field_from_clause, 1},
    {field_limit_clause, 5},
    {field_offset_clause, 6},
    {field_select_clause, 0},
    {field_using_clause, 2},
    {field_where_clause, 3},
    {field_with_clause, 4},
  [2452] =
    {field_from_clause, 1},
    {field_limit_clause, 5},
    {field_offset_clause, 6},
    {field_select_clause, 0},
    {field_update_clause, 7},
    {field_using_clause, 2},
    {field_where_clause, 3},
    {field_with_clause, 4},
  [2460] =
    {field_for_clause, 6},
    {field_from_clause, 1},
    {field_offset_clause, 5},
    {field_select_clause, 0},
    {field_update_clause, 7},
    {field_using_clause, 2},
    {field_where_clause, 3},
    {field_with_clause, 4},
  [2468] =
    {field_for_clause, 6},
    {field_from_clause, 1},
    {field_order_by_clause, 5},
    {field_select_clause, 0},
    {field_update_clause, 7},
    {field_using_clause, 2},
    {field_where_clause, 3},
    {field_with_clause, 4},
  [2476] =
    {field_for_clause, 7},
    {field_from_clause, 1},
    {field_limit_clause, 6},
    {field_order_by_clause, 5},
    {field_select_clause, 0},
    {field_using_clause, 2},
    {field_where_clause, 3},
    {field_with_clause, 4},
  [2484] =
    {field_from_clause, 1},
    {field_limit_clause, 6},
    {field_offset_clause, 7},
    {field_order_by_clause, 5},
    {field_select_clause, 0},
    {field_using_clause, 2},
    {field_where_clause, 3},
    {field_with_clause, 4},
  [2492] =
    {field_from_clause, 1},
    {field_limit_clause, 6},
    {field_order_by_clause, 5},
    {field_select_clause, 0},
    {field_update_clause, 7},
    {field_using_clause, 2},
    {field_where_clause, 3},
    {field_with_clause, 4},
  [2500] =
    {field_for_clause, 7},
    {field_from_clause, 1},
    {field_offset_clause, 6},
    {field_order_by_clause, 5},
    {field_select_clause, 0},
    {field_using_clause, 2},
    {field_where_clause, 3},
    {field_with_clause, 4},
  [2508] =
    {field_from_clause, 1},
    {field_offset_clause, 6},
    {field_order_by_clause, 5},
    {field_select_clause, 0},
    {field_update_clause, 7},
    {field_using_clause, 2},
    {field_where_clause, 3},
    {field_with_clause, 4},
  [2516] =
    {field_for_clause, 6},
    {field_from_clause, 1},
    {field_limit_clause, 4},
    {field_offset_clause, 5},
    {field_select_clause, 0},
    {field_update_clause, 7},
    {field_using_clause, 2},
    {field_where_clause, 3},
  [2524] =
    {field_for_clause, 6},
    {field_from_clause, 1},
    {field_limit_clause, 5},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_update_clause, 7},
    {field_using_clause, 2},
    {field_where_clause, 3},
  [2532] =
    {field_for_clause, 7},
    {field_from_clause, 1},
    {field_limit_clause, 5},
    {field_offset_clause, 6},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_using_clause, 2},
    {field_where_clause, 3},
  [2540] =
    {field_from_clause, 1},
    {field_limit_clause, 5},
    {field_offset_clause, 6},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_update_clause, 7},
    {field_using_clause, 2},
    {field_where_clause, 3},
  [2548] =
    {field_for_clause, 6},
    {field_from_clause, 1},
    {field_offset_clause, 5},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_update_clause, 7},
    {field_using_clause, 2},
    {field_where_clause, 3},
  [2556] =
    {field_for_clause, 6},
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_limit_clause, 5},
    {field_select_clause, 0},
    {field_update_clause, 7},
    {field_using_clause, 2},
    {field_with_clause, 3},
  [2564] =
    {field_for_clause, 7},
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_limit_clause, 5},
    {field_offset_clause, 6},
    {field_select_clause, 0},
    {field_using_clause, 2},
    {field_with_clause, 3},
  [2572] =
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_limit_clause, 5},
    {field_offset_clause, 6},
    {field_select_clause, 0},
    {field_update_clause, 7},
    {field_using_clause, 2},
    {field_with_clause, 3},
  [2580] =
    {field_for_clause, 6},
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_offset_clause, 5},
    {field_select_clause, 0},
    {field_update_clause, 7},
    {field_using_clause, 2},
    {field_with_clause, 3},
  [2588] =
    {field_for_clause, 6},
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_order_by_clause, 5},
    {field_select_clause, 0},
    {field_update_clause, 7},
    {field_using_clause, 2},
    {field_with_clause, 3},
  [2596] =
    {field_for_clause, 7},
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_limit_clause, 6},
    {field_order_by_clause, 5},
    {field_select_clause, 0},
    {field_using_clause, 2},
    {field_with_clause, 3},
  [2604] =
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_limit_clause, 6},
    {field_offset_clause, 7},
    {field_order_by_clause, 5},
    {field_select_clause, 0},
    {field_using_clause, 2},
    {field_with_clause, 3},
  [2612] =
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_limit_clause, 6},
    {field_order_by_clause, 5},
    {field_select_clause, 0},
    {field_update_clause, 7},
    {field_using_clause, 2},
    {field_with_clause, 3},
  [2620] =
    {field_for_clause, 7},
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_offset_clause, 6},
    {field_order_by_clause, 5},
    {field_select_clause, 0},
    {field_using_clause, 2},
    {field_with_clause, 3},
  [2628] =
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_offset_clause, 6},
    {field_order_by_clause, 5},
    {field_select_clause, 0},
    {field_update_clause, 7},
    {field_using_clause, 2},
    {field_with_clause, 3},
  [2636] =
    {field_for_clause, 6},
    {field_from_clause, 1},
    {field_limit_clause, 4},
    {field_offset_clause, 5},
    {field_select_clause, 0},
    {field_update_clause, 7},
    {field_using_clause, 2},
    {field_with_clause, 3},
  [2644] =
    {field_for_clause, 6},
    {field_from_clause, 1},
    {field_limit_clause, 5},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_update_clause, 7},
    {field_using_clause, 2},
    {field_with_clause, 3},
  [2652] =
    {field_for_clause, 7},
    {field_from_clause, 1},
    {field_limit_clause, 5},
    {field_offset_clause, 6},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_using_clause, 2},
    {field_with_clause, 3},
  [2660] =
    {field_from_clause, 1},
    {field_limit_clause, 5},
    {field_offset_clause, 6},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_update_clause, 7},
    {field_using_clause, 2},
    {field_with_clause, 3},
  [2668] =
    {field_for_clause, 6},
    {field_from_clause, 1},
    {field_offset_clause, 5},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_update_clause, 7},
    {field_using_clause, 2},
    {field_with_clause, 3},
  [2676] =
    {field_for_clause, 6},
    {field_from_clause, 1},
    {field_limit_clause, 4},
    {field_offset_clause, 5},
    {field_order_by_clause, 3},
    {field_select_clause, 0},
    {field_update_clause, 7},
    {field_using_clause, 2},
  [2684] =
    {field_for_clause, 6},
    {field_from_clause, 1},
    {field_group_by_clause, 2},
    {field_limit_clause, 4},
    {field_offset_clause, 5},
    {field_order_by_clause, 3},
    {field_select_clause, 0},
    {field_update_clause, 7},
  [2692] =
    {field_for_clause, 6},
    {field_from_clause, 1},
    {field_group_by_clause, 3},
    {field_limit_clause, 4},
    {field_offset_clause, 5},
    {field_select_clause, 0},
    {field_update_clause, 7},
    {field_where_clause, 2},
  [2700] =
    {field_for_clause, 6},
    {field_from_clause, 1},
    {field_group_by_clause, 3},
    {field_limit_clause, 5},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_update_clause, 7},
    {field_where_clause, 2},
  [2708] =
    {field_for_clause, 7},
    {field_from_clause, 1},
    {field_group_by_clause, 3},
    {field_limit_clause, 5},
    {field_offset_clause, 6},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_where_clause, 2},
  [2716] =
    {field_from_clause, 1},
    {field_group_by_clause, 3},
    {field_limit_clause, 5},
    {field_offset_clause, 6},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_update_clause, 7},
    {field_where_clause, 2},
  [2724] =
    {field_for_clause, 6},
    {field_from_clause, 1},
    {field_group_by_clause, 3},
    {field_offset_clause, 5},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_update_clause, 7},
    {field_where_clause, 2},
  [2732] =
    {field_for_clause, 6},
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_limit_clause, 5},
    {field_select_clause, 0},
    {field_update_clause, 7},
    {field_where_clause, 2},
    {field_with_clause, 3},
  [2740] =
    {field_for_clause, 7},
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_limit_clause, 5},
    {field_offset_clause, 6},
    {field_select_clause, 0},
    {field_where_clause, 2},
    {field_with_clause, 3},
  [2748] =
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_limit_clause, 5},
    {field_offset_clause, 6},
    {field_select_clause, 0},
    {field_update_clause, 7},
    {field_where_clause, 2},
    {field_with_clause, 3},
  [2756] =
    {field_for_clause, 6},
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_offset_clause, 5},
    {field_select_clause, 0},
    {field_update_clause, 7},
    {field_where_clause, 2},
    {field_with_clause, 3},
  [2764] =
    {field_for_clause, 6},
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_order_by_clause, 5},
    {field_select_clause, 0},
    {field_update_clause, 7},
    {field_where_clause, 2},
    {field_with_clause, 3},
  [2772] =
    {field_for_clause, 7},
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_limit_clause, 6},
    {field_order_by_clause, 5},
    {field_select_clause, 0},
    {field_where_clause, 2},
    {field_with_clause, 3},
  [2780] =
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_limit_clause, 6},
    {field_offset_clause, 7},
    {field_order_by_clause, 5},
    {field_select_clause, 0},
    {field_where_clause, 2},
    {field_with_clause, 3},
  [2788] =
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_limit_clause, 6},
    {field_order_by_clause, 5},
    {field_select_clause, 0},
    {field_update_clause, 7},
    {field_where_clause, 2},
    {field_with_clause, 3},
  [2796] =
    {field_for_clause, 7},
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_offset_clause, 6},
    {field_order_by_clause, 5},
    {field_select_clause, 0},
    {field_where_clause, 2},
    {field_with_clause, 3},
  [2804] =
    {field_from_clause, 1},
    {field_group_by_clause, 4},
    {field_offset_clause, 6},
    {field_order_by_clause, 5},
    {field_select_clause, 0},
    {field_update_clause, 7},
    {field_where_clause, 2},
    {field_with_clause, 3},
  [2812] =
    {field_for_clause, 6},
    {field_from_clause, 1},
    {field_limit_clause, 4},
    {field_offset_clause, 5},
    {field_select_clause, 0},
    {field_update_clause, 7},
    {field_where_clause, 2},
    {field_with_clause, 3},
  [2820] =
    {field_for_clause, 6},
    {field_from_clause, 1},
    {field_limit_clause, 5},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_update_clause, 7},
    {field_where_clause, 2},
    {field_with_clause, 3},
  [2828] =
    {field_for_clause, 7},
    {field_from_clause, 1},
    {field_limit_clause, 5},
    {field_offset_clause, 6},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_where_clause, 2},
    {field_with_clause, 3},
  [2836] =
    {field_from_clause, 1},
    {field_limit_clause, 5},
    {field_offset_clause, 6},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_update_clause, 7},
    {field_where_clause, 2},
    {field_with_clause, 3},
  [2844] =
    {field_for_clause, 6},
    {field_from_clause, 1},
    {field_offset_clause, 5},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_update_clause, 7},
    {field_where_clause, 2},
    {field_with_clause, 3},
  [2852] =
    {field_for_clause, 6},
    {field_from_clause, 1},
    {field_limit_clause, 4},
    {field_offset_clause, 5},
    {field_order_by_clause, 3},
    {field_select_clause, 0},
    {field_update_clause, 7},
    {field_where_clause, 2},
  [2860] =
    {field_for_clause, 6},
    {field_from_clause, 1},
    {field_group_by_clause, 3},
    {field_limit_clause, 4},
    {field_offset_clause, 5},
    {field_select_clause, 0},
    {field_update_clause, 7},
    {field_with_clause, 2},
  [2868] =
    {field_for_clause, 6},
    {field_from_clause, 1},
    {field_group_by_clause, 3},
    {field_limit_clause, 5},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_update_clause, 7},
    {field_with_clause, 2},
  [2876] =
    {field_for_clause, 7},
    {field_from_clause, 1},
    {field_group_by_clause, 3},
    {field_limit_clause, 5},
    {field_offset_clause, 6},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_with_clause, 2},
  [2884] =
    {field_from_clause, 1},
    {field_group_by_clause, 3},
    {field_limit_clause, 5},
    {field_offset_clause, 6},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_update_clause, 7},
    {field_with_clause, 2},
  [2892] =
    {field_for_clause, 6},
    {field_from_clause, 1},
    {field_group_by_clause, 3},
    {field_offset_clause, 5},
    {field_order_by_clause, 4},
    {field_select_clause, 0},
    {field_update_clause, 7},
    {field_with_clause, 2},
  [2900] =
    {field_for_clause, 6},
    {field_from_clause, 1},
    {field_limit_clause, 4},
    {field_offset_clause, 5},
    {field_order_by_clause, 3},
    {field_select_clause, 0},
    {field_update_clause, 7},
    {field_with_clause, 2},
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
  [6] = 5,
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
  [21] = 21,
  [22] = 22,
  [23] = 23,
  [24] = 20,
  [25] = 25,
  [26] = 26,
  [27] = 27,
  [28] = 28,
  [29] = 29,
  [30] = 30,
  [31] = 30,
  [32] = 32,
  [33] = 33,
  [34] = 34,
  [35] = 10,
  [36] = 36,
  [37] = 32,
  [38] = 38,
  [39] = 39,
  [40] = 9,
  [41] = 41,
  [42] = 42,
  [43] = 43,
  [44] = 44,
  [45] = 45,
  [46] = 46,
  [47] = 11,
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
  [72] = 12,
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
  [100] = 10,
  [101] = 9,
  [102] = 102,
  [103] = 103,
  [104] = 102,
  [105] = 105,
  [106] = 106,
  [107] = 107,
  [108] = 108,
  [109] = 109,
  [110] = 110,
  [111] = 111,
  [112] = 112,
  [113] = 113,
  [114] = 114,
  [115] = 115,
  [116] = 11,
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
  [142] = 13,
  [143] = 143,
  [144] = 144,
  [145] = 145,
  [146] = 146,
  [147] = 147,
  [148] = 148,
  [149] = 149,
  [150] = 150,
  [151] = 17,
  [152] = 152,
  [153] = 153,
  [154] = 15,
  [155] = 16,
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
  [167] = 14,
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
  [181] = 181,
  [182] = 182,
  [183] = 183,
  [184] = 184,
  [185] = 185,
  [186] = 186,
  [187] = 187,
  [188] = 188,
  [189] = 189,
  [190] = 190,
  [191] = 191,
  [192] = 192,
  [193] = 193,
  [194] = 194,
  [195] = 195,
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
  [225] = 180,
  [226] = 226,
  [227] = 227,
  [228] = 228,
  [229] = 229,
  [230] = 230,
  [231] = 180,
  [232] = 232,
  [233] = 233,
  [234] = 234,
  [235] = 235,
  [236] = 236,
  [237] = 12,
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
  [249] = 249,
  [250] = 250,
  [251] = 251,
  [252] = 252,
  [253] = 253,
  [254] = 254,
  [255] = 255,
  [256] = 236,
  [257] = 257,
  [258] = 258,
  [259] = 236,
  [260] = 260,
  [261] = 261,
  [262] = 262,
  [263] = 263,
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
  [327] = 327,
  [328] = 328,
  [329] = 329,
  [330] = 330,
  [331] = 331,
  [332] = 332,
  [333] = 61,
  [334] = 334,
  [335] = 9,
  [336] = 10,
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
  [357] = 11,
  [358] = 358,
  [359] = 353,
  [360] = 360,
  [361] = 361,
  [362] = 347,
  [363] = 363,
  [364] = 364,
  [365] = 365,
  [366] = 353,
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
  [394] = 394,
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
  [408] = 408,
  [409] = 409,
  [410] = 410,
  [411] = 411,
  [412] = 412,
  [413] = 413,
  [414] = 414,
  [415] = 415,
  [416] = 416,
  [417] = 417,
  [418] = 418,
  [419] = 419,
  [420] = 420,
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
  [436] = 436,
  [437] = 437,
  [438] = 438,
  [439] = 439,
  [440] = 440,
  [441] = 441,
  [442] = 442,
  [443] = 443,
  [444] = 444,
  [445] = 445,
  [446] = 446,
  [447] = 447,
  [448] = 448,
  [449] = 449,
  [450] = 450,
  [451] = 451,
  [452] = 452,
  [453] = 453,
  [454] = 454,
  [455] = 455,
  [456] = 456,
  [457] = 457,
  [458] = 458,
  [459] = 459,
  [460] = 460,
  [461] = 461,
  [462] = 462,
  [463] = 463,
  [464] = 464,
  [465] = 465,
  [466] = 466,
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
  [509] = 64,
  [510] = 510,
  [511] = 166,
  [512] = 17,
  [513] = 13,
  [514] = 44,
  [515] = 43,
  [516] = 55,
  [517] = 42,
  [518] = 45,
  [519] = 63,
  [520] = 520,
  [521] = 521,
  [522] = 27,
  [523] = 523,
  [524] = 524,
  [525] = 96,
  [526] = 174,
  [527] = 46,
  [528] = 67,
  [529] = 107,
  [530] = 94,
  [531] = 14,
  [532] = 59,
  [533] = 92,
  [534] = 97,
  [535] = 15,
  [536] = 16,
  [537] = 537,
  [538] = 538,
  [539] = 539,
  [540] = 540,
  [541] = 541,
  [542] = 542,
  [543] = 543,
  [544] = 544,
  [545] = 545,
  [546] = 369,
  [547] = 416,
  [548] = 455,
  [549] = 549,
  [550] = 420,
  [551] = 416,
  [552] = 420,
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
  [578] = 115,
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
  [613] = 588,
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
  [736] = 736,
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
  [791] = 791,
  [792] = 792,
  [793] = 793,
  [794] = 794,
  [795] = 795,
  [796] = 796,
  [797] = 797,
  [798] = 798,
  [799] = 799,
  [800] = 800,
  [801] = 801,
  [802] = 802,
  [803] = 803,
  [804] = 804,
  [805] = 805,
  [806] = 806,
  [807] = 807,
  [808] = 808,
  [809] = 809,
  [810] = 810,
  [811] = 811,
  [812] = 812,
  [813] = 813,
  [814] = 814,
  [815] = 815,
  [816] = 816,
  [817] = 817,
  [818] = 818,
  [819] = 819,
  [820] = 820,
  [821] = 821,
  [822] = 822,
  [823] = 823,
  [824] = 824,
  [825] = 825,
  [826] = 826,
  [827] = 827,
  [828] = 828,
  [829] = 829,
  [830] = 830,
  [831] = 831,
  [832] = 832,
  [833] = 833,
  [834] = 834,
  [835] = 835,
  [836] = 836,
  [837] = 837,
  [838] = 838,
  [839] = 839,
  [840] = 840,
  [841] = 841,
  [842] = 842,
  [843] = 843,
  [844] = 844,
  [845] = 845,
  [846] = 846,
  [847] = 847,
  [848] = 848,
  [849] = 849,
  [850] = 850,
  [851] = 851,
  [852] = 852,
  [853] = 853,
  [854] = 854,
  [855] = 855,
  [856] = 856,
  [857] = 857,
  [858] = 858,
  [859] = 859,
  [860] = 860,
  [861] = 861,
  [862] = 862,
  [863] = 863,
  [864] = 864,
  [865] = 865,
  [866] = 866,
  [867] = 867,
  [868] = 868,
  [869] = 869,
  [870] = 870,
  [871] = 871,
  [872] = 872,
  [873] = 873,
  [874] = 874,
  [875] = 875,
  [876] = 876,
  [877] = 877,
  [878] = 878,
  [879] = 879,
  [880] = 880,
  [881] = 881,
  [882] = 882,
  [883] = 883,
  [884] = 884,
  [885] = 885,
  [886] = 886,
  [887] = 887,
  [888] = 888,
  [889] = 889,
  [890] = 890,
  [891] = 891,
  [892] = 892,
  [893] = 893,
  [894] = 894,
  [895] = 895,
  [896] = 896,
  [897] = 897,
  [898] = 898,
  [899] = 621,
  [900] = 642,
  [901] = 820,
  [902] = 902,
  [903] = 903,
  [904] = 904,
  [905] = 905,
  [906] = 734,
  [907] = 907,
  [908] = 908,
  [909] = 909,
  [910] = 642,
  [911] = 911,
  [912] = 912,
  [913] = 909,
  [914] = 642,
  [915] = 696,
  [916] = 909,
  [917] = 917,
  [918] = 918,
  [919] = 919,
  [920] = 920,
  [921] = 588,
  [922] = 630,
  [923] = 877,
  [924] = 630,
  [925] = 877,
  [926] = 926,
  [927] = 927,
  [928] = 636,
  [929] = 636,
  [930] = 930,
  [931] = 931,
  [932] = 865,
  [933] = 865,
  [934] = 934,
  [935] = 935,
};

static bool ts_lex(TSLexer *lexer, TSStateId state) {
  START_LEXER();
  eof = lexer->eof(lexer);
  switch (state) {
    case 0:
      if (eof) ADVANCE(766);
      ADVANCE_MAP(
        '!', 32,
        '\'', 3,
        '(', 793,
        ')', 794,
        '*', 13,
        ',', 792,
        '-', 752,
        '.', 878,
        '/', 18,
        ':', 915,
        '<', 883,
        '=', 800,
        '>', 885,
        '{', 784,
        '}', 785,
        'A', 140,
        'a', 140,
        'B', 230,
        'b', 230,
        'C', 687,
        'c', 687,
        'D', 74,
        'd', 74,
        'E', 399,
        'e', 399,
        'F', 85,
        'f', 85,
        'G', 234,
        'g', 234,
        'H', 81,
        'h', 81,
        'I', 435,
        'i', 435,
        'L', 110,
        'l', 110,
        'M', 75,
        'm', 75,
        'N', 50,
        'n', 50,
        'O', 307,
        'o', 307,
        'P', 329,
        'p', 329,
        'R', 208,
        'r', 208,
        'S', 151,
        's', 151,
        'T', 82,
        't', 82,
        'U', 514,
        'u', 514,
        'V', 351,
        'v', 351,
        'W', 338,
        'w', 338,
        'Y', 294,
        'y', 294,
      );
      if (('1' <= lookahead && lookahead <= '4')) ADVANCE(926);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(927);
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') SKIP(0);
      END_STATE();
    case 1:
      ADVANCE_MAP(
        '!', 32,
        '(', 793,
        ')', 794,
        '*', 13,
        ',', 792,
        '-', 752,
        '.', 878,
        '/', 21,
        '<', 883,
        '=', 800,
        '>', 885,
        'A', 610,
        'a', 610,
        'B', 345,
        'b', 345,
        'D', 268,
        'd', 268,
        'E', 400,
        'e', 400,
        'F', 465,
        'f', 465,
        'G', 546,
        'g', 546,
        'H', 80,
        'h', 80,
        'I', 435,
        'i', 435,
        'L', 344,
        'l', 344,
        'N', 493,
        'n', 493,
        'O', 308,
        'o', 308,
        'T', 562,
        't', 562,
        'U', 513,
        'u', 513,
        'V', 373,
        'v', 373,
        'W', 338,
        'w', 338,
      );
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') SKIP(1);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(929);
      END_STATE();
    case 2:
      ADVANCE_MAP(
        '\'', 3,
        '*', 13,
        '-', 752,
        '/', 21,
        'F', 78,
        'f', 78,
        'L', 79,
        'l', 79,
        'N', 54,
        'n', 54,
        'R', 228,
        'r', 228,
        'S', 238,
        's', 238,
        'T', 337,
        't', 337,
        'U', 510,
        'u', 510,
        'V', 350,
        'v', 350,
        'Y', 285,
        'y', 285,
      );
      if (('1' <= lookahead && lookahead <= '4')) ADVANCE(924);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(925);
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') SKIP(2);
      if (('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(764);
      END_STATE();
    case 3:
      if (lookahead == '\'') ADVANCE(919);
      if (lookahead == '\\') ADVANCE(763);
      if (lookahead != 0) ADVANCE(3);
      END_STATE();
    case 4:
      ADVANCE_MAP(
        '(', 793,
        ')', 794,
        ',', 792,
        '.', 878,
        '/', 21,
        'L', 961,
        'l', 961,
        'O', 953,
        'o', 953,
        'U', 991,
        'u', 991,
        'W', 958,
        'w', 958,
      );
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') SKIP(4);
      if (('A' <= lookahead && lookahead <= 'Z') ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(1004);
      END_STATE();
    case 5:
      if (lookahead == '(') ADVANCE(793);
      if (lookahead == ')') ADVANCE(794);
      if (lookahead == '/') ADVANCE(21);
      if (lookahead == 'D' ||
          lookahead == 'd') ADVANCE(959);
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') SKIP(5);
      if (('A' <= lookahead && lookahead <= 'Z') ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(1004);
      END_STATE();
    case 6:
      if (lookahead == '(') ADVANCE(793);
      if (lookahead == '*') ADVANCE(13);
      if (lookahead == '/') ADVANCE(21);
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') SKIP(6);
      if (('A' <= lookahead && lookahead <= 'Z') ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(1004);
      END_STATE();
    case 7:
      if (lookahead == '(') ADVANCE(793);
      if (lookahead == ',') ADVANCE(792);
      if (lookahead == '.') ADVANCE(878);
      if (lookahead == '/') ADVANCE(21);
      if (lookahead == 'F' ||
          lookahead == 'f') ADVANCE(986);
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') SKIP(7);
      if (('A' <= lookahead && lookahead <= 'Z') ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(1004);
      END_STATE();
    case 8:
      ADVANCE_MAP(
        '(', 793,
        '/', 21,
        'D', 959,
        'd', 959,
        'F', 960,
        'f', 960,
        'T', 1001,
        't', 1001,
      );
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') SKIP(8);
      if (('A' <= lookahead && lookahead <= 'Z') ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(1004);
      END_STATE();
    case 9:
      if (lookahead == '(') ADVANCE(793);
      if (lookahead == '/') ADVANCE(21);
      if (lookahead == 'D' ||
          lookahead == 'd') ADVANCE(959);
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(974);
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') SKIP(9);
      if (('A' <= lookahead && lookahead <= 'Z') ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(1004);
      END_STATE();
    case 10:
      ADVANCE_MAP(
        ')', 794,
        ',', 792,
        '.', 878,
        '/', 21,
        'A', 990,
        'a', 990,
        'F', 977,
        'f', 977,
        'G', 984,
        'g', 984,
        'L', 961,
        'l', 961,
        'O', 953,
        'o', 953,
        'U', 982,
        'u', 982,
        'W', 957,
        'w', 957,
      );
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') SKIP(10);
      if (('B' <= lookahead && lookahead <= 'Z') ||
          ('b' <= lookahead && lookahead <= 'z')) ADVANCE(1004);
      END_STATE();
    case 11:
      if (lookahead == ')') ADVANCE(794);
      if (lookahead == '/') ADVANCE(21);
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(434);
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(544);
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') SKIP(11);
      END_STATE();
    case 12:
      if (lookahead == '*') ADVANCE(13);
      if (lookahead == '/') ADVANCE(21);
      if (lookahead == 'G' ||
          lookahead == 'g') ADVANCE(950);
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') SKIP(12);
      if (('A' <= lookahead && lookahead <= 'Z') ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(1004);
      END_STATE();
    case 13:
      if (lookahead == '*') ADVANCE(35);
      END_STATE();
    case 14:
      if (lookahead == '*') ADVANCE(916);
      END_STATE();
    case 15:
      if (lookahead == '*') ADVANCE(14);
      END_STATE();
    case 16:
      if (lookahead == '-') ADVANCE(23);
      END_STATE();
    case 17:
      if (lookahead == '.') ADVANCE(758);
      if (lookahead == 'Z') ADVANCE(932);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(737);
      END_STATE();
    case 18:
      if (lookahead == '/') ADVANCE(767);
      END_STATE();
    case 19:
      if (lookahead == '/') ADVANCE(772);
      END_STATE();
    case 20:
      if (lookahead == '/') ADVANCE(788);
      if (lookahead == '\\') ADVANCE(790);
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') ADVANCE(786);
      if (lookahead != 0 &&
          lookahead != '}') ADVANCE(789);
      END_STATE();
    case 21:
      if (lookahead == '/') ADVANCE(19);
      END_STATE();
    case 22:
      if (lookahead == '0') ADVANCE(750);
      if (lookahead == '1') ADVANCE(738);
      END_STATE();
    case 23:
      if (lookahead == '0') ADVANCE(751);
      if (lookahead == '3') ADVANCE(736);
      if (lookahead == '1' ||
          lookahead == '2') ADVANCE(754);
      END_STATE();
    case 24:
      if (lookahead == '0') ADVANCE(64);
      END_STATE();
    case 25:
      if (lookahead == '0') ADVANCE(65);
      END_STATE();
    case 26:
      if (lookahead == '2') ADVANCE(739);
      if (lookahead == '0' ||
          lookahead == '1') ADVANCE(755);
      END_STATE();
    case 27:
      ADVANCE_MAP(
        '9', 24,
        'F', 378,
        'f', 378,
        'M', 501,
        'm', 501,
        'N', 66,
        'n', 66,
        'Q', 698,
        'q', 698,
        'W', 278,
        'w', 278,
        'Y', 295,
        'y', 295,
      );
      END_STATE();
    case 28:
      ADVANCE_MAP(
        '9', 25,
        'F', 379,
        'f', 379,
        'M', 504,
        'm', 504,
        'N', 66,
        'n', 66,
        'Q', 699,
        'q', 699,
        'W', 280,
        'w', 280,
        'Y', 296,
        'y', 296,
      );
      END_STATE();
    case 29:
      if (lookahead == ':') ADVANCE(749);
      END_STATE();
    case 30:
      if (lookahead == ':') ADVANCE(747);
      END_STATE();
    case 31:
      if (lookahead == ':') ADVANCE(748);
      END_STATE();
    case 32:
      if (lookahead == '=') ADVANCE(881);
      END_STATE();
    case 33:
      if (lookahead == 'A') ADVANCE(45);
      END_STATE();
    case 34:
      if (lookahead == 'C') ADVANCE(38);
      END_STATE();
    case 35:
      if (lookahead == 'D') ADVANCE(41);
      END_STATE();
    case 36:
      if (lookahead == 'E') ADVANCE(46);
      END_STATE();
    case 37:
      if (lookahead == 'E') ADVANCE(43);
      END_STATE();
    case 38:
      if (lookahead == 'H') ADVANCE(36);
      END_STATE();
    case 39:
      if (lookahead == 'M') ADVANCE(33);
      END_STATE();
    case 40:
      if (lookahead == 'N') ADVANCE(42);
      END_STATE();
    case 41:
      if (lookahead == 'O') ADVANCE(40);
      END_STATE();
    case 42:
      if (lookahead == 'O') ADVANCE(44);
      END_STATE();
    case 43:
      if (lookahead == 'R') ADVANCE(15);
      END_STATE();
    case 44:
      if (lookahead == 'T') ADVANCE(39);
      END_STATE();
    case 45:
      if (lookahead == 'T') ADVANCE(34);
      END_STATE();
    case 46:
      if (lookahead == 'V') ADVANCE(37);
      END_STATE();
    case 47:
      if (lookahead == 'Z') ADVANCE(932);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(737);
      END_STATE();
    case 48:
      if (lookahead == 'Z') ADVANCE(932);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(737);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(47);
      END_STATE();
    case 49:
      if (lookahead == 'Z') ADVANCE(932);
      if (lookahead == '+' ||
          lookahead == '-') ADVANCE(737);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(48);
      END_STATE();
    case 50:
      ADVANCE_MAP(
        '_', 188,
        'A', 428,
        'a', 428,
        'E', 629,
        'e', 629,
        'O', 630,
        'o', 630,
        'U', 406,
        'u', 406,
      );
      END_STATE();
    case 51:
      if (lookahead == '_') ADVANCE(27);
      END_STATE();
    case 52:
      if (lookahead == '_') ADVANCE(314);
      END_STATE();
    case 53:
      if (lookahead == '_') ADVANCE(523);
      END_STATE();
    case 54:
      if (lookahead == '_') ADVANCE(185);
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(720);
      if (lookahead == 'U' ||
          lookahead == 'u') ADVANCE(397);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(765);
      END_STATE();
    case 55:
      if (lookahead == '_') ADVANCE(144);
      END_STATE();
    case 56:
      if (lookahead == '_') ADVANCE(429);
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(178);
      END_STATE();
    case 57:
      if (lookahead == '_') ADVANCE(659);
      END_STATE();
    case 58:
      if (lookahead == '_') ADVANCE(167);
      END_STATE();
    case 59:
      if (lookahead == '_') ADVANCE(432);
      END_STATE();
    case 60:
      if (lookahead == '_') ADVANCE(325);
      END_STATE();
    case 61:
      if (lookahead == '_') ADVANCE(415);
      END_STATE();
    case 62:
      if (lookahead == '_') ADVANCE(93);
      END_STATE();
    case 63:
      if (lookahead == '_') ADVANCE(266);
      END_STATE();
    case 64:
      if (lookahead == '_') ADVANCE(205);
      END_STATE();
    case 65:
      if (lookahead == '_') ADVANCE(206);
      END_STATE();
    case 66:
      if (lookahead == '_') ADVANCE(204);
      END_STATE();
    case 67:
      if (lookahead == '_') ADVANCE(28);
      END_STATE();
    case 68:
      if (lookahead == '_') ADVANCE(673);
      END_STATE();
    case 69:
      if (lookahead == '_') ADVANCE(525);
      END_STATE();
    case 70:
      if (lookahead == '_') ADVANCE(433);
      END_STATE();
    case 71:
      if (lookahead == '_') ADVANCE(526);
      END_STATE();
    case 72:
      if (lookahead == '_') ADVANCE(527);
      END_STATE();
    case 73:
      if (lookahead == '_') ADVANCE(524);
      END_STATE();
    case 74:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(646);
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(404);
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(621);
      END_STATE();
    case 75:
      ADVANCE_MAP(
        'A', 719,
        'a', 719,
        'E', 648,
        'e', 648,
        'I', 449,
        'i', 449,
        'Y', 57,
        'y', 57,
      );
      END_STATE();
    case 76:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(740);
      END_STATE();
    case 77:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(802);
      END_STATE();
    case 78:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(396);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('B' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('b' <= lookahead && lookahead <= 'z')) ADVANCE(765);
      END_STATE();
    case 79:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(597);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('B' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('b' <= lookahead && lookahead <= 'z')) ADVANCE(765);
      END_STATE();
    case 80:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(706);
      END_STATE();
    case 81:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(706);
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(315);
      END_STATE();
    case 82:
      ADVANCE_MAP(
        'A', 545,
        'a', 545,
        'E', 86,
        'e', 86,
        'H', 239,
        'h', 239,
        'O', 189,
        'o', 189,
        'R', 90,
        'r', 90,
        'Y', 516,
        'y', 516,
      );
      END_STATE();
    case 83:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(734);
      END_STATE();
    case 84:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(734);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(933);
      END_STATE();
    case 85:
      ADVANCE_MAP(
        'A', 416,
        'a', 416,
        'I', 248,
        'i', 248,
        'O', 528,
        'o', 528,
        'R', 469,
        'r', 469,
      );
      END_STATE();
    case 86:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(425);
      END_STATE();
    case 87:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(721);
      END_STATE();
    case 88:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(721);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(933);
      END_STATE();
    case 89:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(149);
      END_STATE();
    case 90:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(149);
      if (lookahead == 'U' ||
          lookahead == 'u') ADVANCE(215);
      END_STATE();
    case 91:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(356);
      END_STATE();
    case 92:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(459);
      END_STATE();
    case 93:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(322);
      END_STATE();
    case 94:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(431);
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(572);
      END_STATE();
    case 95:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(724);
      END_STATE();
    case 96:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(411);
      END_STATE();
    case 97:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(578);
      END_STATE();
    case 98:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(566);
      END_STATE();
    case 99:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(531);
      END_STATE();
    case 100:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(667);
      END_STATE();
    case 101:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(635);
      END_STATE();
    case 102:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(532);
      END_STATE();
    case 103:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(533);
      END_STATE();
    case 104:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(534);
      END_STATE();
    case 105:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(553);
      END_STATE();
    case 106:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(551);
      END_STATE();
    case 107:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(538);
      END_STATE();
    case 108:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(539);
      END_STATE();
    case 109:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(540);
      END_STATE();
    case 110:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(604);
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(387);
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(466);
      END_STATE();
    case 111:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(461);
      END_STATE();
    case 112:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(728);
      END_STATE();
    case 113:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(192);
      END_STATE();
    case 114:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(557);
      END_STATE();
    case 115:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(570);
      END_STATE();
    case 116:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(451);
      END_STATE();
    case 117:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(730);
      END_STATE();
    case 118:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(661);
      END_STATE();
    case 119:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(661);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(933);
      END_STATE();
    case 120:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(618);
      END_STATE();
    case 121:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(731);
      END_STATE();
    case 122:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(662);
      END_STATE();
    case 123:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(616);
      END_STATE();
    case 124:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(658);
      END_STATE();
    case 125:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(365);
      END_STATE();
    case 126:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(663);
      END_STATE();
    case 127:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(664);
      END_STATE();
    case 128:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(460);
      END_STATE();
    case 129:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(581);
      END_STATE();
    case 130:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(582);
      END_STATE();
    case 131:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(583);
      END_STATE();
    case 132:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(584);
      END_STATE();
    case 133:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(585);
      END_STATE();
    case 134:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(586);
      END_STATE();
    case 135:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(587);
      END_STATE();
    case 136:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(420);
      END_STATE();
    case 137:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(421);
      END_STATE();
    case 138:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(422);
      END_STATE();
    case 139:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(423);
      END_STATE();
    case 140:
      ADVANCE_MAP(
        'B', 471,
        'b', 471,
        'D', 705,
        'd', 705,
        'L', 393,
        'l', 393,
        'N', 174,
        'n', 174,
        'S', 839,
        's', 839,
        'T', 859,
        't', 859,
      );
      END_STATE();
    case 141:
      if (lookahead == 'B' ||
          lookahead == 'b') ADVANCE(722);
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') ADVANCE(141);
      END_STATE();
    case 142:
      if (lookahead == 'B' ||
          lookahead == 'b') ADVANCE(723);
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') ADVANCE(142);
      END_STATE();
    case 143:
      if (lookahead == 'B' ||
          lookahead == 'b') ADVANCE(486);
      END_STATE();
    case 144:
      if (lookahead == 'B' ||
          lookahead == 'b') ADVANCE(292);
      END_STATE();
    case 145:
      if (lookahead == 'B' ||
          lookahead == 'b') ADVANCE(362);
      END_STATE();
    case 146:
      if (lookahead == 'B' ||
          lookahead == 'b') ADVANCE(99);
      END_STATE();
    case 147:
      if (lookahead == 'C' ||
          lookahead == 'c') ADVANCE(871);
      END_STATE();
    case 148:
      if (lookahead == 'C' ||
          lookahead == 'c') ADVANCE(870);
      END_STATE();
    case 149:
      if (lookahead == 'C' ||
          lookahead == 'c') ADVANCE(392);
      END_STATE();
    case 150:
      if (lookahead == 'C' ||
          lookahead == 'c') ADVANCE(403);
      END_STATE();
    case 151:
      ADVANCE_MAP(
        'C', 498,
        'c', 498,
        'E', 165,
        'e', 165,
        'I', 194,
        'i', 194,
        'N', 375,
        'n', 375,
        'P', 286,
        'p', 286,
        'T', 111,
        't', 111,
        'U', 512,
        'u', 512,
        'Y', 620,
        'y', 620,
      );
      END_STATE();
    case 152:
      if (lookahead == 'C' ||
          lookahead == 'c') ADVANCE(476);
      if (lookahead == 'F' ||
          lookahead == 'f') ADVANCE(251);
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(692);
      END_STATE();
    case 153:
      if (lookahead == 'C' ||
          lookahead == 'c') ADVANCE(334);
      END_STATE();
    case 154:
      if (lookahead == 'C' ||
          lookahead == 'c') ADVANCE(336);
      END_STATE();
    case 155:
      if (lookahead == 'C' ||
          lookahead == 'c') ADVANCE(633);
      END_STATE();
    case 156:
      if (lookahead == 'C' ||
          lookahead == 'c') ADVANCE(100);
      END_STATE();
    case 157:
      if (lookahead == 'C' ||
          lookahead == 'c') ADVANCE(96);
      END_STATE();
    case 158:
      if (lookahead == 'C' ||
          lookahead == 'c') ADVANCE(555);
      END_STATE();
    case 159:
      if (lookahead == 'C' ||
          lookahead == 'c') ADVANCE(489);
      END_STATE();
    case 160:
      if (lookahead == 'C' ||
          lookahead == 'c') ADVANCE(233);
      END_STATE();
    case 161:
      if (lookahead == 'C' ||
          lookahead == 'c') ADVANCE(257);
      END_STATE();
    case 162:
      if (lookahead == 'C' ||
          lookahead == 'c') ADVANCE(224);
      END_STATE();
    case 163:
      if (lookahead == 'C' ||
          lookahead == 'c') ADVANCE(225);
      END_STATE();
    case 164:
      if (lookahead == 'C' ||
          lookahead == 'c') ADVANCE(267);
      END_STATE();
    case 165:
      if (lookahead == 'C' ||
          lookahead == 'c') ADVANCE(693);
      if (lookahead == 'L' ||
          lookahead == 'l') ADVANCE(243);
      END_STATE();
    case 166:
      if (lookahead == 'C' ||
          lookahead == 'c') ADVANCE(503);
      END_STATE();
    case 167:
      if (lookahead == 'C' ||
          lookahead == 'c') ADVANCE(490);
      END_STATE();
    case 168:
      if (lookahead == 'C' ||
          lookahead == 'c') ADVANCE(669);
      END_STATE();
    case 169:
      if (lookahead == 'C' ||
          lookahead == 'c') ADVANCE(126);
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') ADVANCE(169);
      END_STATE();
    case 170:
      if (lookahead == 'C' ||
          lookahead == 'c') ADVANCE(136);
      END_STATE();
    case 171:
      if (lookahead == 'C' ||
          lookahead == 'c') ADVANCE(137);
      END_STATE();
    case 172:
      if (lookahead == 'C' ||
          lookahead == 'c') ADVANCE(138);
      END_STATE();
    case 173:
      if (lookahead == 'C' ||
          lookahead == 'c') ADVANCE(139);
      END_STATE();
    case 174:
      if (lookahead == 'D' ||
          lookahead == 'd') ADVANCE(845);
      END_STATE();
    case 175:
      if (lookahead == 'D' ||
          lookahead == 'd') ADVANCE(824);
      END_STATE();
    case 176:
      if (lookahead == 'D' ||
          lookahead == 'd') ADVANCE(821);
      END_STATE();
    case 177:
      if (lookahead == 'D' ||
          lookahead == 'd') ADVANCE(775);
      END_STATE();
    case 178:
      if (lookahead == 'D' ||
          lookahead == 'd') ADVANCE(853);
      END_STATE();
    case 179:
      if (lookahead == 'D' ||
          lookahead == 'd') ADVANCE(842);
      END_STATE();
    case 180:
      if (lookahead == 'D' ||
          lookahead == 'd') ADVANCE(812);
      END_STATE();
    case 181:
      if (lookahead == 'D' ||
          lookahead == 'd') ADVANCE(261);
      END_STATE();
    case 182:
      if (lookahead == 'D' ||
          lookahead == 'd') ADVANCE(804);
      END_STATE();
    case 183:
      if (lookahead == 'D' ||
          lookahead == 'd') ADVANCE(850);
      END_STATE();
    case 184:
      if (lookahead == 'D' ||
          lookahead == 'd') ADVANCE(855);
      END_STATE();
    case 185:
      ADVANCE_MAP(
        'D', 84,
        'd', 84,
        'F', 353,
        'f', 353,
        'M', 479,
        'm', 479,
        'Q', 695,
        'q', 695,
        'W', 272,
        'w', 272,
        'Y', 291,
        'y', 291,
      );
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(759);
      END_STATE();
    case 186:
      if (lookahead == 'D' ||
          lookahead == 'd') ADVANCE(119);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(759);
      END_STATE();
    case 187:
      if (lookahead == 'D' ||
          lookahead == 'd') ADVANCE(588);
      END_STATE();
    case 188:
      ADVANCE_MAP(
        'D', 83,
        'd', 83,
        'F', 352,
        'f', 352,
        'M', 478,
        'm', 478,
        'Q', 694,
        'q', 694,
        'W', 271,
        'w', 271,
        'Y', 290,
        'y', 290,
      );
      END_STATE();
    case 189:
      if (lookahead == 'D' ||
          lookahead == 'd') ADVANCE(87);
      if (lookahead == 'M' ||
          lookahead == 'm') ADVANCE(480);
      END_STATE();
    case 190:
      if (lookahead == 'D' ||
          lookahead == 'd') ADVANCE(118);
      END_STATE();
    case 191:
      if (lookahead == 'D' ||
          lookahead == 'd') ADVANCE(59);
      END_STATE();
    case 192:
      if (lookahead == 'D' ||
          lookahead == 'd') ADVANCE(124);
      END_STATE();
    case 193:
      if (lookahead == 'D' ||
          lookahead == 'd') ADVANCE(114);
      END_STATE();
    case 194:
      if (lookahead == 'D' ||
          lookahead == 'd') ADVANCE(214);
      END_STATE();
    case 195:
      if (lookahead == 'D' ||
          lookahead == 'd') ADVANCE(95);
      END_STATE();
    case 196:
      if (lookahead == 'D' ||
          lookahead == 'd') ADVANCE(287);
      END_STATE();
    case 197:
      if (lookahead == 'D' ||
          lookahead == 'd') ADVANCE(250);
      END_STATE();
    case 198:
      if (lookahead == 'D' ||
          lookahead == 'd') ADVANCE(255);
      END_STATE();
    case 199:
      if (lookahead == 'D' ||
          lookahead == 'd') ADVANCE(226);
      END_STATE();
    case 200:
      if (lookahead == 'D' ||
          lookahead == 'd') ADVANCE(227);
      END_STATE();
    case 201:
      if (lookahead == 'D' ||
          lookahead == 'd') ADVANCE(88);
      if (lookahead == 'M' ||
          lookahead == 'm') ADVANCE(481);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(759);
      END_STATE();
    case 202:
      if (lookahead == 'D' ||
          lookahead == 'd') ADVANCE(709);
      END_STATE();
    case 203:
      if (lookahead == 'D' ||
          lookahead == 'd') ADVANCE(245);
      END_STATE();
    case 204:
      ADVANCE_MAP(
        'D', 112,
        'd', 112,
        'F', 381,
        'f', 381,
        'M', 507,
        'm', 507,
        'Q', 701,
        'q', 701,
        'W', 284,
        'w', 284,
        'Y', 298,
        'y', 298,
      );
      END_STATE();
    case 205:
      if (lookahead == 'D' ||
          lookahead == 'd') ADVANCE(117);
      END_STATE();
    case 206:
      if (lookahead == 'D' ||
          lookahead == 'd') ADVANCE(121);
      END_STATE();
    case 207:
      if (lookahead == 'D' ||
          lookahead == 'd') ADVANCE(746);
      END_STATE();
    case 208:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(152);
      END_STATE();
    case 209:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(437);
      END_STATE();
    case 210:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(827);
      END_STATE();
    case 211:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(887);
      END_STATE();
    case 212:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(814);
      END_STATE();
    case 213:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(781);
      END_STATE();
    case 214:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(146);
      END_STATE();
    case 215:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(879);
      END_STATE();
    case 216:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(879);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(933);
      END_STATE();
    case 217:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(860);
      END_STATE();
    case 218:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(880);
      END_STATE();
    case 219:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(94);
      END_STATE();
    case 220:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(782);
      END_STATE();
    case 221:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(811);
      END_STATE();
    case 222:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(843);
      END_STATE();
    case 223:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(831);
      END_STATE();
    case 224:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(876);
      END_STATE();
    case 225:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(833);
      END_STATE();
    case 226:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(851);
      END_STATE();
    case 227:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(852);
      END_STATE();
    case 228:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(310);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(765);
      END_STATE();
    case 229:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(715);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(759);
      END_STATE();
    case 230:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(405);
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(444);
      END_STATE();
    case 231:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(718);
      END_STATE();
    case 232:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(710);
      END_STATE();
    case 233:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(143);
      END_STATE();
    case 234:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(497);
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(468);
      END_STATE();
    case 235:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(529);
      END_STATE();
    case 236:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(327);
      END_STATE();
    case 237:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(391);
      END_STATE();
    case 238:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(402);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(765);
      END_STATE();
    case 239:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(436);
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(600);
      END_STATE();
    case 240:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(530);
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(443);
      END_STATE();
    case 241:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(712);
      END_STATE();
    case 242:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(743);
      END_STATE();
    case 243:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(155);
      END_STATE();
    case 244:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(155);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(933);
      END_STATE();
    case 245:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(565);
      END_STATE();
    case 246:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(427);
      END_STATE();
    case 247:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(383);
      END_STATE();
    case 248:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(407);
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(177);
      END_STATE();
    case 249:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(384);
      END_STATE();
    case 250:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(589);
      END_STATE();
    case 251:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(563);
      END_STATE();
    case 252:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(563);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(933);
      END_STATE();
    case 253:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(385);
      END_STATE();
    case 254:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(717);
      END_STATE();
    case 255:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(590);
      END_STATE();
    case 256:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(388);
      END_STATE();
    case 257:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(207);
      END_STATE();
    case 258:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(470);
      END_STATE();
    case 259:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(632);
      END_STATE();
    case 260:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(566);
      END_STATE();
    case 261:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(419);
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(430);
      END_STATE();
    case 262:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(180);
      END_STATE();
    case 263:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(450);
      END_STATE();
    case 264:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(595);
      END_STATE();
    case 265:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(652);
      END_STATE();
    case 266:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(442);
      END_STATE();
    case 267:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(183);
      END_STATE();
    case 268:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(598);
      END_STATE();
    case 269:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(634);
      END_STATE();
    case 270:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(575);
      END_STATE();
    case 271:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(237);
      END_STATE();
    case 272:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(237);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(933);
      END_STATE();
    case 273:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(553);
      END_STATE();
    case 274:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(580);
      END_STATE();
    case 275:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(535);
      END_STATE();
    case 276:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(536);
      END_STATE();
    case 277:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(537);
      END_STATE();
    case 278:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(247);
      END_STATE();
    case 279:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(541);
      END_STATE();
    case 280:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(249);
      END_STATE();
    case 281:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(542);
      END_STATE();
    case 282:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(253);
      END_STATE();
    case 283:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(543);
      END_STATE();
    case 284:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(256);
      END_STATE();
    case 285:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(599);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(765);
      END_STATE();
    case 286:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(409);
      END_STATE();
    case 287:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(606);
      END_STATE();
    case 288:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(168);
      END_STATE();
    case 289:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(326);
      END_STATE();
    case 290:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(98);
      END_STATE();
    case 291:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(98);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(933);
      END_STATE();
    case 292:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(414);
      END_STATE();
    case 293:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(166);
      END_STATE();
    case 294:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(609);
      END_STATE();
    case 295:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(102);
      END_STATE();
    case 296:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(103);
      END_STATE();
    case 297:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(104);
      END_STATE();
    case 298:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(105);
      END_STATE();
    case 299:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(106);
      END_STATE();
    case 300:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(115);
      END_STATE();
    case 301:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(107);
      END_STATE();
    case 302:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(108);
      END_STATE();
    case 303:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(109);
      END_STATE();
    case 304:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(573);
      END_STATE();
    case 305:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(463);
      END_STATE();
    case 306:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(328);
      END_STATE();
    case 307:
      if (lookahead == 'F' ||
          lookahead == 'f') ADVANCE(312);
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(847);
      END_STATE();
    case 308:
      if (lookahead == 'F' ||
          lookahead == 'f') ADVANCE(312);
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(203);
      END_STATE();
    case 309:
      if (lookahead == 'F' ||
          lookahead == 'f') ADVANCE(822);
      END_STATE();
    case 310:
      if (lookahead == 'F' ||
          lookahead == 'f') ADVANCE(252);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(759);
      END_STATE();
    case 311:
      if (lookahead == 'F' ||
          lookahead == 'f') ADVANCE(370);
      if (lookahead == 'L' ||
          lookahead == 'l') ADVANCE(123);
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') ADVANCE(311);
      END_STATE();
    case 312:
      if (lookahead == 'F' ||
          lookahead == 'f') ADVANCE(615);
      END_STATE();
    case 313:
      if (lookahead == 'F' ||
          lookahead == 'f') ADVANCE(495);
      END_STATE();
    case 314:
      ADVANCE_MAP(
        'F', 380,
        'f', 380,
        'M', 505,
        'm', 505,
        'Q', 700,
        'q', 700,
        'W', 282,
        'w', 282,
        'Y', 297,
        'y', 297,
      );
      END_STATE();
    case 315:
      if (lookahead == 'G' ||
          lookahead == 'g') ADVANCE(339);
      END_STATE();
    case 316:
      if (lookahead == 'G' ||
          lookahead == 'g') ADVANCE(797);
      END_STATE();
    case 317:
      if (lookahead == 'G' ||
          lookahead == 'g') ADVANCE(836);
      END_STATE();
    case 318:
      if (lookahead == 'G' ||
          lookahead == 'g') ADVANCE(867);
      END_STATE();
    case 319:
      if (lookahead == 'G' ||
          lookahead == 'g') ADVANCE(791);
      END_STATE();
    case 320:
      if (lookahead == 'G' ||
          lookahead == 'g') ADVANCE(813);
      END_STATE();
    case 321:
      if (lookahead == 'G' ||
          lookahead == 'g') ADVANCE(341);
      END_STATE();
    case 322:
      if (lookahead == 'G' ||
          lookahead == 'g') ADVANCE(467);
      END_STATE();
    case 323:
      if (lookahead == 'G' ||
          lookahead == 'g') ADVANCE(265);
      END_STATE();
    case 324:
      if (lookahead == 'G' ||
          lookahead == 'g') ADVANCE(657);
      END_STATE();
    case 325:
      if (lookahead == 'G' ||
          lookahead == 'g') ADVANCE(567);
      END_STATE();
    case 326:
      if (lookahead == 'G' ||
          lookahead == 'g') ADVANCE(492);
      END_STATE();
    case 327:
      if (lookahead == 'G' ||
          lookahead == 'g') ADVANCE(122);
      END_STATE();
    case 328:
      if (lookahead == 'G' ||
          lookahead == 'g') ADVANCE(127);
      END_STATE();
    case 329:
      if (lookahead == 'H' ||
          lookahead == 'h') ADVANCE(500);
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(347);
      END_STATE();
    case 330:
      if (lookahead == 'H' ||
          lookahead == 'h') ADVANCE(808);
      END_STATE();
    case 331:
      if (lookahead == 'H' ||
          lookahead == 'h') ADVANCE(897);
      END_STATE();
    case 332:
      if (lookahead == 'H' ||
          lookahead == 'h') ADVANCE(899);
      END_STATE();
    case 333:
      if (lookahead == 'H' ||
          lookahead == 'h') ADVANCE(898);
      END_STATE();
    case 334:
      if (lookahead == 'H' ||
          lookahead == 'h') ADVANCE(795);
      END_STATE();
    case 335:
      if (lookahead == 'H' ||
          lookahead == 'h') ADVANCE(806);
      END_STATE();
    case 336:
      if (lookahead == 'H' ||
          lookahead == 'h') ADVANCE(796);
      END_STATE();
    case 337:
      if (lookahead == 'H' ||
          lookahead == 'h') ADVANCE(346);
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(201);
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(690);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(765);
      END_STATE();
    case 338:
      if (lookahead == 'H' ||
          lookahead == 'h') ADVANCE(209);
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(642);
      END_STATE();
    case 339:
      if (lookahead == 'H' ||
          lookahead == 'h') ADVANCE(408);
      END_STATE();
    case 340:
      if (lookahead == 'H' ||
          lookahead == 'h') ADVANCE(591);
      END_STATE();
    case 341:
      if (lookahead == 'H' ||
          lookahead == 'h') ADVANCE(636);
      END_STATE();
    case 342:
      if (lookahead == 'H' ||
          lookahead == 'h') ADVANCE(605);
      END_STATE();
    case 343:
      if (lookahead == 'H' ||
          lookahead == 'h') ADVANCE(360);
      END_STATE();
    case 344:
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(386);
      END_STATE();
    case 345:
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(444);
      END_STATE();
    case 346:
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(601);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(759);
      END_STATE();
    case 347:
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(160);
      END_STATE();
    case 348:
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(608);
      END_STATE();
    case 349:
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(145);
      END_STATE();
    case 350:
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(229);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(765);
      END_STATE();
    case 351:
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(232);
      END_STATE();
    case 352:
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(619);
      END_STATE();
    case 353:
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(619);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(933);
      END_STATE();
    case 354:
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(522);
      END_STATE();
    case 355:
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(445);
      END_STATE();
    case 356:
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(395);
      END_STATE();
    case 357:
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(631);
      END_STATE();
    case 358:
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(446);
      END_STATE();
    case 359:
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(447);
      END_STATE();
    case 360:
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(448);
      END_STATE();
    case 361:
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(482);
      END_STATE();
    case 362:
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(417);
      END_STATE();
    case 363:
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(182);
      END_STATE();
    case 364:
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(650);
      END_STATE();
    case 365:
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(453);
      END_STATE();
    case 366:
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(485);
      END_STATE();
    case 367:
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(441);
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') ADVANCE(367);
      END_STATE();
    case 368:
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(487);
      END_STATE();
    case 369:
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(674);
      END_STATE();
    case 370:
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(577);
      END_STATE();
    case 371:
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(656);
      END_STATE();
    case 372:
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(241);
      END_STATE();
    case 373:
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(254);
      END_STATE();
    case 374:
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(321);
      END_STATE();
    case 375:
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(519);
      END_STATE();
    case 376:
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(611);
      END_STATE();
    case 377:
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(680);
      END_STATE();
    case 378:
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(625);
      END_STATE();
    case 379:
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(626);
      END_STATE();
    case 380:
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(627);
      END_STATE();
    case 381:
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(628);
      END_STATE();
    case 382:
      if (lookahead == 'K' ||
          lookahead == 'k') ADVANCE(803);
      END_STATE();
    case 383:
      if (lookahead == 'K' ||
          lookahead == 'k') ADVANCE(894);
      END_STATE();
    case 384:
      if (lookahead == 'K' ||
          lookahead == 'k') ADVANCE(896);
      END_STATE();
    case 385:
      if (lookahead == 'K' ||
          lookahead == 'k') ADVANCE(895);
      END_STATE();
    case 386:
      if (lookahead == 'K' ||
          lookahead == 'k') ADVANCE(211);
      if (lookahead == 'M' ||
          lookahead == 'm') ADVANCE(357);
      END_STATE();
    case 387:
      if (lookahead == 'K' ||
          lookahead == 'k') ADVANCE(211);
      if (lookahead == 'M' ||
          lookahead == 'm') ADVANCE(357);
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(644);
      END_STATE();
    case 388:
      if (lookahead == 'K' ||
          lookahead == 'k') ADVANCE(591);
      END_STATE();
    case 389:
      if (lookahead == 'K' ||
          lookahead == 'k') ADVANCE(363);
      END_STATE();
    case 390:
      if (lookahead == 'K' ||
          lookahead == 'k') ADVANCE(689);
      END_STATE();
    case 391:
      if (lookahead == 'K' ||
          lookahead == 'k') ADVANCE(605);
      END_STATE();
    case 392:
      if (lookahead == 'K' ||
          lookahead == 'k') ADVANCE(358);
      END_STATE();
    case 393:
      if (lookahead == 'L' ||
          lookahead == 'l') ADVANCE(779);
      END_STATE();
    case 394:
      if (lookahead == 'L' ||
          lookahead == 'l') ADVANCE(918);
      END_STATE();
    case 395:
      if (lookahead == 'L' ||
          lookahead == 'l') ADVANCE(780);
      END_STATE();
    case 396:
      if (lookahead == 'L' ||
          lookahead == 'l') ADVANCE(614);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(759);
      END_STATE();
    case 397:
      if (lookahead == 'L' ||
          lookahead == 'l') ADVANCE(398);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(759);
      END_STATE();
    case 398:
      if (lookahead == 'L' ||
          lookahead == 'l') ADVANCE(917);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(933);
      END_STATE();
    case 399:
      ADVANCE_MAP(
        'L', 607,
        'l', 607,
        'M', 91,
        'm', 91,
        'N', 175,
        'n', 175,
        'V', 235,
        'v', 235,
        'X', 150,
        'x', 150,
      );
      END_STATE();
    case 400:
      if (lookahead == 'L' ||
          lookahead == 'l') ADVANCE(607);
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(175);
      if (lookahead == 'X' ||
          lookahead == 'x') ADVANCE(150);
      END_STATE();
    case 401:
      if (lookahead == 'L' ||
          lookahead == 'l') ADVANCE(58);
      END_STATE();
    case 402:
      if (lookahead == 'L' ||
          lookahead == 'l') ADVANCE(244);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(759);
      END_STATE();
    case 403:
      if (lookahead == 'L' ||
          lookahead == 'l') ADVANCE(696);
      END_STATE();
    case 404:
      if (lookahead == 'L' ||
          lookahead == 'l') ADVANCE(236);
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(147);
      END_STATE();
    case 405:
      if (lookahead == 'L' ||
          lookahead == 'l') ADVANCE(472);
      END_STATE();
    case 406:
      if (lookahead == 'L' ||
          lookahead == 'l') ADVANCE(394);
      END_STATE();
    case 407:
      if (lookahead == 'L' ||
          lookahead == 'l') ADVANCE(187);
      END_STATE();
    case 408:
      if (lookahead == 'L' ||
          lookahead == 'l') ADVANCE(374);
      END_STATE();
    case 409:
      if (lookahead == 'L' ||
          lookahead == 'l') ADVANCE(401);
      END_STATE();
    case 410:
      if (lookahead == 'L' ||
          lookahead == 'l') ADVANCE(475);
      END_STATE();
    case 411:
      if (lookahead == 'L' ||
          lookahead == 'l') ADVANCE(53);
      END_STATE();
    case 412:
      if (lookahead == 'L' ||
          lookahead == 'l') ADVANCE(413);
      END_STATE();
    case 413:
      if (lookahead == 'L' ||
          lookahead == 'l') ADVANCE(602);
      END_STATE();
    case 414:
      if (lookahead == 'L' ||
          lookahead == 'l') ADVANCE(477);
      END_STATE();
    case 415:
      if (lookahead == 'L' ||
          lookahead == 'l') ADVANCE(263);
      END_STATE();
    case 416:
      if (lookahead == 'L' ||
          lookahead == 'l') ADVANCE(613);
      END_STATE();
    case 417:
      if (lookahead == 'L' ||
          lookahead == 'l') ADVANCE(371);
      END_STATE();
    case 418:
      if (lookahead == 'L' ||
          lookahead == 'l') ADVANCE(697);
      END_STATE();
    case 419:
      if (lookahead == 'L' ||
          lookahead == 'l') ADVANCE(306);
      END_STATE();
    case 420:
      if (lookahead == 'L' ||
          lookahead == 'l') ADVANCE(69);
      END_STATE();
    case 421:
      if (lookahead == 'L' ||
          lookahead == 'l') ADVANCE(71);
      END_STATE();
    case 422:
      if (lookahead == 'L' ||
          lookahead == 'l') ADVANCE(72);
      END_STATE();
    case 423:
      if (lookahead == 'L' ||
          lookahead == 'l') ADVANCE(73);
      END_STATE();
    case 424:
      if (lookahead == 'M' ||
          lookahead == 'm') ADVANCE(837);
      END_STATE();
    case 425:
      if (lookahead == 'M' ||
          lookahead == 'm') ADVANCE(818);
      END_STATE();
    case 426:
      if (lookahead == 'M' ||
          lookahead == 'm') ADVANCE(841);
      END_STATE();
    case 427:
      if (lookahead == 'M' ||
          lookahead == 'm') ADVANCE(70);
      END_STATE();
    case 428:
      if (lookahead == 'M' ||
          lookahead == 'm') ADVANCE(213);
      END_STATE();
    case 429:
      if (lookahead == 'M' ||
          lookahead == 'm') ADVANCE(506);
      END_STATE();
    case 430:
      if (lookahead == 'M' ||
          lookahead == 'm') ADVANCE(125);
      END_STATE();
    case 431:
      if (lookahead == 'M' ||
          lookahead == 'm') ADVANCE(68);
      END_STATE();
    case 432:
      if (lookahead == 'M' ||
          lookahead == 'm') ADVANCE(733);
      END_STATE();
    case 433:
      if (lookahead == 'M' ||
          lookahead == 'm') ADVANCE(508);
      END_STATE();
    case 434:
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(174);
      END_STATE();
    case 435:
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(776);
      END_STATE();
    case 436:
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(826);
      END_STATE();
    case 437:
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(825);
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(222);
      END_STATE();
    case 438:
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(799);
      END_STATE();
    case 439:
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(874);
      END_STATE();
    case 440:
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(807);
      END_STATE();
    case 441:
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(888);
      END_STATE();
    case 442:
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(313);
      END_STATE();
    case 443:
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(316);
      END_STATE();
    case 444:
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(176);
      END_STATE();
    case 445:
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(317);
      END_STATE();
    case 446:
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(318);
      END_STATE();
    case 447:
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(319);
      END_STATE();
    case 448:
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(320);
      END_STATE();
    case 449:
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(212);
      END_STATE();
    case 450:
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(324);
      END_STATE();
    case 451:
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(191);
      END_STATE();
    case 452:
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(645);
      END_STATE();
    case 453:
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(594);
      END_STATE();
    case 454:
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(649);
      END_STATE();
    case 455:
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(651);
      END_STATE();
    case 456:
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(220);
      END_STATE();
    case 457:
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(653);
      END_STATE();
    case 458:
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(655);
      END_STATE();
    case 459:
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(161);
      END_STATE();
    case 460:
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(162);
      END_STATE();
    case 461:
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(193);
      END_STATE();
    case 462:
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(359);
      END_STATE();
    case 463:
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(163);
      END_STATE();
    case 464:
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(665);
      END_STATE();
    case 465:
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(528);
      END_STATE();
    case 466:
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(390);
      END_STATE();
    case 467:
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(914);
      END_STATE();
    case 468:
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(688);
      END_STATE();
    case 469:
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(424);
      END_STATE();
    case 470:
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(309);
      END_STATE();
    case 471:
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(707);
      END_STATE();
    case 472:
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(711);
      END_STATE();
    case 473:
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(426);
      END_STATE();
    case 474:
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(713);
      END_STATE();
    case 475:
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(156);
      END_STATE();
    case 476:
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(548);
      END_STATE();
    case 477:
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(714);
      END_STATE();
    case 478:
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(452);
      END_STATE();
    case 479:
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(452);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(933);
      END_STATE();
    case 480:
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(559);
      END_STATE();
    case 481:
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(559);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(933);
      END_STATE();
    case 482:
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(438);
      END_STATE();
    case 483:
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(547);
      END_STATE();
    case 484:
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(561);
      END_STATE();
    case 485:
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(439);
      END_STATE();
    case 486:
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(499);
      END_STATE();
    case 487:
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(440);
      END_STATE();
    case 488:
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(554);
      END_STATE();
    case 489:
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(464);
      END_STATE();
    case 490:
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(569);
      END_STATE();
    case 491:
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(549);
      END_STATE();
    case 492:
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(552);
      END_STATE();
    case 493:
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(647);
      if (lookahead == 'U' ||
          lookahead == 'u') ADVANCE(412);
      END_STATE();
    case 494:
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(574);
      END_STATE();
    case 495:
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(576);
      END_STATE();
    case 496:
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(556);
      END_STATE();
    case 497:
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(410);
      END_STATE();
    case 498:
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(517);
      END_STATE();
    case 499:
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(389);
      END_STATE();
    case 500:
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(456);
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(120);
      END_STATE();
    case 501:
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(454);
      END_STATE();
    case 502:
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(691);
      END_STATE();
    case 503:
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(560);
      END_STATE();
    case 504:
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(455);
      END_STATE();
    case 505:
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(457);
      END_STATE();
    case 506:
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(199);
      END_STATE();
    case 507:
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(458);
      END_STATE();
    case 508:
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(200);
      END_STATE();
    case 509:
      if (lookahead == 'P' ||
          lookahead == 'p') ADVANCE(819);
      END_STATE();
    case 510:
      if (lookahead == 'P' ||
          lookahead == 'p') ADVANCE(186);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(765);
      END_STATE();
    case 511:
      if (lookahead == 'P' ||
          lookahead == 'p') ADVANCE(741);
      END_STATE();
    case 512:
      if (lookahead == 'P' ||
          lookahead == 'p') ADVANCE(520);
      END_STATE();
    case 513:
      if (lookahead == 'P' ||
          lookahead == 'p') ADVANCE(190);
      END_STATE();
    case 514:
      if (lookahead == 'P' ||
          lookahead == 'p') ADVANCE(190);
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(240);
      END_STATE();
    case 515:
      if (lookahead == 'P' ||
          lookahead == 'p') ADVANCE(596);
      END_STATE();
    case 516:
      if (lookahead == 'P' ||
          lookahead == 'p') ADVANCE(258);
      END_STATE();
    case 517:
      if (lookahead == 'P' ||
          lookahead == 'p') ADVANCE(221);
      END_STATE();
    case 518:
      if (lookahead == 'P' ||
          lookahead == 'p') ADVANCE(269);
      END_STATE();
    case 519:
      if (lookahead == 'P' ||
          lookahead == 'p') ADVANCE(518);
      END_STATE();
    case 520:
      if (lookahead == 'P' ||
          lookahead == 'p') ADVANCE(484);
      END_STATE();
    case 521:
      if (lookahead == 'P' ||
          lookahead == 'p') ADVANCE(304);
      END_STATE();
    case 522:
      if (lookahead == 'P' ||
          lookahead == 'p') ADVANCE(677);
      END_STATE();
    case 523:
      if (lookahead == 'Q' ||
          lookahead == 'q') ADVANCE(694);
      if (lookahead == 'Y' ||
          lookahead == 'y') ADVANCE(290);
      END_STATE();
    case 524:
      if (lookahead == 'Q' ||
          lookahead == 'q') ADVANCE(701);
      if (lookahead == 'Y' ||
          lookahead == 'y') ADVANCE(298);
      END_STATE();
    case 525:
      if (lookahead == 'Q' ||
          lookahead == 'q') ADVANCE(702);
      if (lookahead == 'Y' ||
          lookahead == 'y') ADVANCE(301);
      END_STATE();
    case 526:
      if (lookahead == 'Q' ||
          lookahead == 'q') ADVANCE(703);
      if (lookahead == 'Y' ||
          lookahead == 'y') ADVANCE(302);
      END_STATE();
    case 527:
      if (lookahead == 'Q' ||
          lookahead == 'q') ADVANCE(704);
      if (lookahead == 'Y' ||
          lookahead == 'y') ADVANCE(303);
      END_STATE();
    case 528:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(829);
      END_STATE();
    case 529:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(729);
      END_STATE();
    case 530:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(56);
      END_STATE();
    case 531:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(783);
      END_STATE();
    case 532:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(906);
      END_STATE();
    case 533:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(907);
      END_STATE();
    case 534:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(905);
      END_STATE();
    case 535:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(903);
      END_STATE();
    case 536:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(904);
      END_STATE();
    case 537:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(902);
      END_STATE();
    case 538:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(912);
      END_STATE();
    case 539:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(913);
      END_STATE();
    case 540:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(911);
      END_STATE();
    case 541:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(909);
      END_STATE();
    case 542:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(910);
      END_STATE();
    case 543:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(908);
      END_STATE();
    case 544:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(846);
      END_STATE();
    case 545:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(323);
      END_STATE();
    case 546:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(468);
      END_STATE();
    case 547:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(382);
      END_STATE();
    case 548:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(202);
      END_STATE();
    case 549:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(725);
      END_STATE();
    case 550:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(462);
      END_STATE();
    case 551:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(153);
      END_STATE();
    case 552:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(726);
      END_STATE();
    case 553:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(591);
      END_STATE();
    case 554:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(55);
      END_STATE();
    case 555:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(354);
      END_STATE();
    case 556:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(727);
      END_STATE();
    case 557:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(179);
      END_STATE();
    case 558:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(474);
      END_STATE();
    case 559:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(558);
      END_STATE();
    case 560:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(184);
      END_STATE();
    case 561:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(654);
      END_STATE();
    case 562:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(89);
      END_STATE();
    case 563:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(305);
      END_STATE();
    case 564:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(293);
      END_STATE();
    case 565:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(745);
      END_STATE();
    case 566:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(605);
      END_STATE();
    case 567:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(502);
      END_STATE();
    case 568:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(364);
      END_STATE();
    case 569:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(571);
      END_STATE();
    case 570:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(154);
      END_STATE();
    case 571:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(288);
      END_STATE();
    case 572:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(369);
      END_STATE();
    case 573:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(564);
      END_STATE();
    case 574:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(521);
      END_STATE();
    case 575:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(195);
      END_STATE();
    case 576:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(164);
      END_STATE();
    case 577:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(617);
      END_STATE();
    case 578:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(672);
      END_STATE();
    case 579:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(377);
      END_STATE();
    case 580:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(579);
      END_STATE();
    case 581:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(675);
      END_STATE();
    case 582:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(676);
      END_STATE();
    case 583:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(678);
      END_STATE();
    case 584:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(679);
      END_STATE();
    case 585:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(681);
      END_STATE();
    case 586:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(682);
      END_STATE();
    case 587:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(683);
      END_STATE();
    case 588:
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(777);
      END_STATE();
    case 589:
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(890);
      END_STATE();
    case 590:
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(889);
      END_STATE();
    case 591:
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(914);
      END_STATE();
    case 592:
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(900);
      END_STATE();
    case 593:
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(901);
      END_STATE();
    case 594:
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(856);
      END_STATE();
    case 595:
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(857);
      END_STATE();
    case 596:
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(815);
      END_STATE();
    case 597:
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(641);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(759);
      END_STATE();
    case 598:
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(147);
      END_STATE();
    case 599:
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(671);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(759);
      END_STATE();
    case 600:
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(52);
      END_STATE();
    case 601:
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(52);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(933);
      END_STATE();
    case 602:
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(742);
      END_STATE();
    case 603:
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(666);
      END_STATE();
    case 604:
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(640);
      END_STATE();
    case 605:
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(62);
      END_STATE();
    case 606:
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(158);
      END_STATE();
    case 607:
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(210);
      END_STATE();
    case 608:
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(361);
      END_STATE();
    case 609:
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(670);
      END_STATE();
    case 610:
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(148);
      END_STATE();
    case 611:
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(349);
      END_STATE();
    case 612:
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(181);
      END_STATE();
    case 613:
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(218);
      END_STATE();
    case 614:
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(218);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(933);
      END_STATE();
    case 615:
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(259);
      END_STATE();
    case 616:
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(637);
      END_STATE();
    case 617:
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(638);
      END_STATE();
    case 618:
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(242);
      END_STATE();
    case 619:
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(157);
      END_STATE();
    case 620:
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(660);
      END_STATE();
    case 621:
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(686);
      if (lookahead == 'V' ||
          lookahead == 'v') ADVANCE(348);
      END_STATE();
    case 622:
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(668);
      END_STATE();
    case 623:
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(299);
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') ADVANCE(623);
      END_STATE();
    case 624:
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(300);
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') ADVANCE(624);
      END_STATE();
    case 625:
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(170);
      END_STATE();
    case 626:
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(171);
      END_STATE();
    case 627:
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(172);
      END_STATE();
    case 628:
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(173);
      END_STATE();
    case 629:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(716);
      if (lookahead == 'X' ||
          lookahead == 'x') ADVANCE(684);
      END_STATE();
    case 630:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(848);
      END_STATE();
    case 631:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(863);
      END_STATE();
    case 632:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(865);
      END_STATE();
    case 633:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(810);
      END_STATE();
    case 634:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(805);
      END_STATE();
    case 635:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(868);
      END_STATE();
    case 636:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(801);
      END_STATE();
    case 637:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(873);
      END_STATE();
    case 638:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(872);
      END_STATE();
    case 639:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(854);
      END_STATE();
    case 640:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(51);
      END_STATE();
    case 641:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(51);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(933);
      END_STATE();
    case 642:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(330);
      END_STATE();
    case 643:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(343);
      END_STATE();
    case 644:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(708);
      END_STATE();
    case 645:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(342);
      END_STATE();
    case 646:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(76);
      END_STATE();
    case 647:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(744);
      END_STATE();
    case 648:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(113);
      END_STATE();
    case 649:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(331);
      END_STATE();
    case 650:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(732);
      END_STATE();
    case 651:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(332);
      END_STATE();
    case 652:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(61);
      END_STATE();
    case 653:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(333);
      END_STATE();
    case 654:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(612);
      END_STATE();
    case 655:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(340);
      END_STATE();
    case 656:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(735);
      END_STATE();
    case 657:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(335);
      END_STATE();
    case 658:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(77);
      END_STATE();
    case 659:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(219);
      END_STATE();
    case 660:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(246);
      END_STATE();
    case 661:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(223);
      END_STATE();
    case 662:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(262);
      END_STATE();
    case 663:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(289);
      END_STATE();
    case 664:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(264);
      END_STATE();
    case 665:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(231);
      END_STATE();
    case 666:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(473);
      END_STATE();
    case 667:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(366);
      END_STATE();
    case 668:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(101);
      END_STATE();
    case 669:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(368);
      END_STATE();
    case 670:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(270);
      END_STATE();
    case 671:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(270);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(933);
      END_STATE();
    case 672:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(260);
      END_STATE();
    case 673:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(274);
      END_STATE();
    case 674:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(491);
      END_STATE();
    case 675:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(275);
      END_STATE();
    case 676:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(276);
      END_STATE();
    case 677:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(494);
      END_STATE();
    case 678:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(277);
      END_STATE();
    case 679:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(273);
      END_STATE();
    case 680:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(496);
      END_STATE();
    case 681:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(279);
      END_STATE();
    case 682:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(281);
      END_STATE();
    case 683:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(283);
      END_STATE();
    case 684:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(67);
      END_STATE();
    case 685:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(67);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(933);
      END_STATE();
    case 686:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(128);
      END_STATE();
    case 687:
      if (lookahead == 'U' ||
          lookahead == 'u') ADVANCE(603);
      END_STATE();
    case 688:
      if (lookahead == 'U' ||
          lookahead == 'u') ADVANCE(511);
      END_STATE();
    case 689:
      if (lookahead == 'U' ||
          lookahead == 'u') ADVANCE(509);
      END_STATE();
    case 690:
      if (lookahead == 'U' ||
          lookahead == 'u') ADVANCE(216);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(759);
      END_STATE();
    case 691:
      if (lookahead == 'U' ||
          lookahead == 'u') ADVANCE(515);
      END_STATE();
    case 692:
      if (lookahead == 'U' ||
          lookahead == 'u') ADVANCE(550);
      END_STATE();
    case 693:
      if (lookahead == 'U' ||
          lookahead == 'u') ADVANCE(568);
      END_STATE();
    case 694:
      if (lookahead == 'U' ||
          lookahead == 'u') ADVANCE(97);
      END_STATE();
    case 695:
      if (lookahead == 'U' ||
          lookahead == 'u') ADVANCE(97);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(933);
      END_STATE();
    case 696:
      if (lookahead == 'U' ||
          lookahead == 'u') ADVANCE(197);
      END_STATE();
    case 697:
      if (lookahead == 'U' ||
          lookahead == 'u') ADVANCE(198);
      END_STATE();
    case 698:
      if (lookahead == 'U' ||
          lookahead == 'u') ADVANCE(129);
      END_STATE();
    case 699:
      if (lookahead == 'U' ||
          lookahead == 'u') ADVANCE(130);
      END_STATE();
    case 700:
      if (lookahead == 'U' ||
          lookahead == 'u') ADVANCE(131);
      END_STATE();
    case 701:
      if (lookahead == 'U' ||
          lookahead == 'u') ADVANCE(132);
      END_STATE();
    case 702:
      if (lookahead == 'U' ||
          lookahead == 'u') ADVANCE(133);
      END_STATE();
    case 703:
      if (lookahead == 'U' ||
          lookahead == 'u') ADVANCE(134);
      END_STATE();
    case 704:
      if (lookahead == 'U' ||
          lookahead == 'u') ADVANCE(135);
      END_STATE();
    case 705:
      if (lookahead == 'V' ||
          lookahead == 'v') ADVANCE(92);
      END_STATE();
    case 706:
      if (lookahead == 'V' ||
          lookahead == 'v') ADVANCE(355);
      END_STATE();
    case 707:
      if (lookahead == 'V' ||
          lookahead == 'v') ADVANCE(217);
      END_STATE();
    case 708:
      if (lookahead == 'V' ||
          lookahead == 'v') ADVANCE(372);
      END_STATE();
    case 709:
      if (lookahead == 'V' ||
          lookahead == 'v') ADVANCE(376);
      END_STATE();
    case 710:
      if (lookahead == 'W' ||
          lookahead == 'w') ADVANCE(835);
      END_STATE();
    case 711:
      if (lookahead == 'W' ||
          lookahead == 'w') ADVANCE(861);
      END_STATE();
    case 712:
      if (lookahead == 'W' ||
          lookahead == 'w') ADVANCE(820);
      END_STATE();
    case 713:
      if (lookahead == 'W' ||
          lookahead == 'w') ADVANCE(893);
      END_STATE();
    case 714:
      if (lookahead == 'W' ||
          lookahead == 'w') ADVANCE(862);
      END_STATE();
    case 715:
      if (lookahead == 'W' ||
          lookahead == 'w') ADVANCE(834);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(933);
      END_STATE();
    case 716:
      if (lookahead == 'W' ||
          lookahead == 'w') ADVANCE(483);
      END_STATE();
    case 717:
      if (lookahead == 'W' ||
          lookahead == 'w') ADVANCE(622);
      END_STATE();
    case 718:
      if (lookahead == 'X' ||
          lookahead == 'x') ADVANCE(639);
      END_STATE();
    case 719:
      if (lookahead == 'X' ||
          lookahead == 'x') ADVANCE(196);
      END_STATE();
    case 720:
      if (lookahead == 'X' ||
          lookahead == 'x') ADVANCE(685);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(759);
      END_STATE();
    case 721:
      if (lookahead == 'Y' ||
          lookahead == 'y') ADVANCE(892);
      END_STATE();
    case 722:
      if (lookahead == 'Y' ||
          lookahead == 'y') ADVANCE(828);
      END_STATE();
    case 723:
      if (lookahead == 'Y' ||
          lookahead == 'y') ADVANCE(869);
      END_STATE();
    case 724:
      if (lookahead == 'Y' ||
          lookahead == 'y') ADVANCE(891);
      END_STATE();
    case 725:
      if (lookahead == 'Y' ||
          lookahead == 'y') ADVANCE(816);
      END_STATE();
    case 726:
      if (lookahead == 'Y' ||
          lookahead == 'y') ADVANCE(858);
      END_STATE();
    case 727:
      if (lookahead == 'Y' ||
          lookahead == 'y') ADVANCE(817);
      END_STATE();
    case 728:
      if (lookahead == 'Y' ||
          lookahead == 'y') ADVANCE(591);
      END_STATE();
    case 729:
      if (lookahead == 'Y' ||
          lookahead == 'y') ADVANCE(643);
      END_STATE();
    case 730:
      if (lookahead == 'Y' ||
          lookahead == 'y') ADVANCE(592);
      END_STATE();
    case 731:
      if (lookahead == 'Y' ||
          lookahead == 'y') ADVANCE(593);
      END_STATE();
    case 732:
      if (lookahead == 'Y' ||
          lookahead == 'y') ADVANCE(63);
      END_STATE();
    case 733:
      if (lookahead == 'Y' ||
          lookahead == 'y') ADVANCE(60);
      END_STATE();
    case 734:
      if (lookahead == 'Y' ||
          lookahead == 'y') ADVANCE(605);
      END_STATE();
    case 735:
      if (lookahead == 'Y' ||
          lookahead == 'y') ADVANCE(159);
      END_STATE();
    case 736:
      if (lookahead == '0' ||
          lookahead == '1') ADVANCE(931);
      END_STATE();
    case 737:
      if (lookahead == '0' ||
          lookahead == '1') ADVANCE(762);
      END_STATE();
    case 738:
      if (('0' <= lookahead && lookahead <= '2')) ADVANCE(16);
      END_STATE();
    case 739:
      if (('0' <= lookahead && lookahead <= '3')) ADVANCE(29);
      END_STATE();
    case 740:
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') ADVANCE(169);
      END_STATE();
    case 741:
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') ADVANCE(141);
      END_STATE();
    case 742:
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') ADVANCE(311);
      END_STATE();
    case 743:
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') ADVANCE(623);
      END_STATE();
    case 744:
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') ADVANCE(367);
      END_STATE();
    case 745:
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') ADVANCE(142);
      END_STATE();
    case 746:
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') ADVANCE(624);
      END_STATE();
    case 747:
      if (('0' <= lookahead && lookahead <= '5')) ADVANCE(756);
      END_STATE();
    case 748:
      if (('0' <= lookahead && lookahead <= '5')) ADVANCE(757);
      END_STATE();
    case 749:
      if (('0' <= lookahead && lookahead <= '5')) ADVANCE(761);
      END_STATE();
    case 750:
      if (('1' <= lookahead && lookahead <= '9')) ADVANCE(16);
      END_STATE();
    case 751:
      if (('1' <= lookahead && lookahead <= '9')) ADVANCE(931);
      END_STATE();
    case 752:
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(929);
      END_STATE();
    case 753:
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(930);
      END_STATE();
    case 754:
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(931);
      END_STATE();
    case 755:
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(29);
      END_STATE();
    case 756:
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(17);
      END_STATE();
    case 757:
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(932);
      END_STATE();
    case 758:
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(49);
      END_STATE();
    case 759:
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(933);
      END_STATE();
    case 760:
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(934);
      END_STATE();
    case 761:
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(30);
      END_STATE();
    case 762:
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(31);
      END_STATE();
    case 763:
      ADVANCE_MAP(
        '"', 3,
        '%', 3,
        '\'', 3,
        'B', 3,
        'F', 3,
        'N', 3,
        'R', 3,
        'T', 3,
        'U', 3,
        '\\', 3,
        '_', 3,
        'b', 3,
        'f', 3,
        'n', 3,
        'r', 3,
        't', 3,
        'u', 3,
      );
      END_STATE();
    case 764:
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(765);
      END_STATE();
    case 765:
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(759);
      END_STATE();
    case 766:
      ACCEPT_TOKEN(ts_builtin_sym_end);
      END_STATE();
    case 767:
      ACCEPT_TOKEN(anon_sym_SLASH_SLASH);
      if (lookahead == '/') ADVANCE(772);
      END_STATE();
    case 768:
      ACCEPT_TOKEN(aux_sym_header_comment_token1);
      if (lookahead == '/') ADVANCE(770);
      if (lookahead == '\t' ||
          (0x0b <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') ADVANCE(768);
      if (lookahead != 0 &&
          (lookahead < '\t' || '\r' < lookahead)) ADVANCE(771);
      END_STATE();
    case 769:
      ACCEPT_TOKEN(aux_sym_header_comment_token1);
      if (lookahead == '/') ADVANCE(774);
      if (lookahead != 0 &&
          lookahead != '\n') ADVANCE(771);
      END_STATE();
    case 770:
      ACCEPT_TOKEN(aux_sym_header_comment_token1);
      if (lookahead == '/') ADVANCE(769);
      if (lookahead != 0 &&
          lookahead != '\n') ADVANCE(771);
      END_STATE();
    case 771:
      ACCEPT_TOKEN(aux_sym_header_comment_token1);
      if (lookahead != 0 &&
          lookahead != '\n') ADVANCE(771);
      END_STATE();
    case 772:
      ACCEPT_TOKEN(anon_sym_SLASH_SLASH_SLASH);
      END_STATE();
    case 773:
      ACCEPT_TOKEN(anon_sym_SLASH_SLASH_SLASH);
      if (lookahead == '\\') ADVANCE(790);
      if (lookahead != 0 &&
          lookahead != '}') ADVANCE(789);
      END_STATE();
    case 774:
      ACCEPT_TOKEN(anon_sym_SLASH_SLASH_SLASH);
      if (lookahead != 0 &&
          lookahead != '\n') ADVANCE(771);
      END_STATE();
    case 775:
      ACCEPT_TOKEN(aux_sym_find_clause_token1);
      END_STATE();
    case 776:
      ACCEPT_TOKEN(aux_sym_in_clause_token1);
      if (lookahead == 'C' ||
          lookahead == 'c') ADVANCE(418);
      END_STATE();
    case 777:
      ACCEPT_TOKEN(aux_sym_in_clause_token2);
      END_STATE();
    case 778:
      ACCEPT_TOKEN(aux_sym_in_clause_token2);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(1004);
      END_STATE();
    case 779:
      ACCEPT_TOKEN(aux_sym_in_type_token1);
      END_STATE();
    case 780:
      ACCEPT_TOKEN(aux_sym_in_type_token2);
      END_STATE();
    case 781:
      ACCEPT_TOKEN(aux_sym_in_type_token3);
      END_STATE();
    case 782:
      ACCEPT_TOKEN(aux_sym_in_type_token4);
      END_STATE();
    case 783:
      ACCEPT_TOKEN(aux_sym_in_type_token5);
      END_STATE();
    case 784:
      ACCEPT_TOKEN(sym_term_separator_start);
      END_STATE();
    case 785:
      ACCEPT_TOKEN(sym_term_separator_end);
      END_STATE();
    case 786:
      ACCEPT_TOKEN(sym_term);
      if (lookahead == '/') ADVANCE(788);
      if (lookahead == '\\') ADVANCE(790);
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') ADVANCE(786);
      if (lookahead != 0 &&
          lookahead != '}') ADVANCE(789);
      END_STATE();
    case 787:
      ACCEPT_TOKEN(sym_term);
      if (lookahead == '/') ADVANCE(773);
      if (lookahead == '\\') ADVANCE(790);
      if (lookahead != 0 &&
          lookahead != '}') ADVANCE(789);
      END_STATE();
    case 788:
      ACCEPT_TOKEN(sym_term);
      if (lookahead == '/') ADVANCE(787);
      if (lookahead == '\\') ADVANCE(790);
      if (lookahead != 0 &&
          lookahead != '}') ADVANCE(789);
      END_STATE();
    case 789:
      ACCEPT_TOKEN(sym_term);
      if (lookahead == '\\') ADVANCE(790);
      if (lookahead != 0 &&
          lookahead != '}') ADVANCE(789);
      END_STATE();
    case 790:
      ACCEPT_TOKEN(sym_term);
      if (lookahead == '\\') ADVANCE(790);
      if (lookahead != 0) ADVANCE(789);
      END_STATE();
    case 791:
      ACCEPT_TOKEN(aux_sym_returning_clause_token1);
      END_STATE();
    case 792:
      ACCEPT_TOKEN(anon_sym_COMMA);
      END_STATE();
    case 793:
      ACCEPT_TOKEN(anon_sym_LPAREN);
      END_STATE();
    case 794:
      ACCEPT_TOKEN(anon_sym_RPAREN);
      END_STATE();
    case 795:
      ACCEPT_TOKEN(aux_sym_using_phrase_search_token1);
      END_STATE();
    case 796:
      ACCEPT_TOKEN(aux_sym_using_advanced_search_token1);
      END_STATE();
    case 797:
      ACCEPT_TOKEN(aux_sym_sosl_using_clause_token1);
      END_STATE();
    case 798:
      ACCEPT_TOKEN(aux_sym_sosl_using_clause_token1);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(1004);
      END_STATE();
    case 799:
      ACCEPT_TOKEN(aux_sym_with_division_expression_token1);
      END_STATE();
    case 800:
      ACCEPT_TOKEN(anon_sym_EQ);
      END_STATE();
    case 801:
      ACCEPT_TOKEN(aux_sym_with_highlight_token1);
      END_STATE();
    case 802:
      ACCEPT_TOKEN(aux_sym_with_metadata_expression_token1);
      END_STATE();
    case 803:
      ACCEPT_TOKEN(aux_sym_with_network_expression_token1);
      END_STATE();
    case 804:
      ACCEPT_TOKEN(aux_sym_with_pricebook_expression_token1);
      END_STATE();
    case 805:
      ACCEPT_TOKEN(aux_sym_with_snippet_expression_token1);
      END_STATE();
    case 806:
      ACCEPT_TOKEN(aux_sym_with_snippet_expression_token2);
      END_STATE();
    case 807:
      ACCEPT_TOKEN(aux_sym_with_spell_correction_expression_token1);
      END_STATE();
    case 808:
      ACCEPT_TOKEN(aux_sym_sosl_with_clause_token1);
      END_STATE();
    case 809:
      ACCEPT_TOKEN(aux_sym_sosl_with_clause_token1);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(1004);
      END_STATE();
    case 810:
      ACCEPT_TOKEN(aux_sym_select_clause_token1);
      END_STATE();
    case 811:
      ACCEPT_TOKEN(aux_sym_using_scope_clause_token1);
      END_STATE();
    case 812:
      ACCEPT_TOKEN(aux_sym_using_scope_type_token1);
      END_STATE();
    case 813:
      ACCEPT_TOKEN(aux_sym_using_scope_type_token2);
      END_STATE();
    case 814:
      ACCEPT_TOKEN(aux_sym_using_scope_type_token3);
      if (lookahead == '_') ADVANCE(116);
      END_STATE();
    case 815:
      ACCEPT_TOKEN(aux_sym_using_scope_type_token4);
      END_STATE();
    case 816:
      ACCEPT_TOKEN(aux_sym_using_scope_type_token5);
      END_STATE();
    case 817:
      ACCEPT_TOKEN(aux_sym_using_scope_type_token6);
      END_STATE();
    case 818:
      ACCEPT_TOKEN(aux_sym_using_scope_type_token7);
      END_STATE();
    case 819:
      ACCEPT_TOKEN(aux_sym_using_lookup_clause_token1);
      END_STATE();
    case 820:
      ACCEPT_TOKEN(aux_sym_using_listview_clause_token1);
      END_STATE();
    case 821:
      ACCEPT_TOKEN(aux_sym_using_lookup_bind_clause_token1);
      END_STATE();
    case 822:
      ACCEPT_TOKEN(aux_sym_type_of_clause_token1);
      END_STATE();
    case 823:
      ACCEPT_TOKEN(aux_sym_type_of_clause_token1);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(1004);
      END_STATE();
    case 824:
      ACCEPT_TOKEN(aux_sym_type_of_clause_token2);
      END_STATE();
    case 825:
      ACCEPT_TOKEN(aux_sym_when_expression_token1);
      END_STATE();
    case 826:
      ACCEPT_TOKEN(aux_sym_when_expression_token2);
      END_STATE();
    case 827:
      ACCEPT_TOKEN(aux_sym_else_expression_token1);
      END_STATE();
    case 828:
      ACCEPT_TOKEN(aux_sym_group_by_clause_token1);
      END_STATE();
    case 829:
      ACCEPT_TOKEN(aux_sym_for_clause_token1);
      END_STATE();
    case 830:
      ACCEPT_TOKEN(aux_sym_for_clause_token1);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(1004);
      END_STATE();
    case 831:
      ACCEPT_TOKEN(aux_sym_for_type_token1);
      END_STATE();
    case 832:
      ACCEPT_TOKEN(aux_sym_for_type_token1);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(1004);
      END_STATE();
    case 833:
      ACCEPT_TOKEN(aux_sym_for_type_token2);
      END_STATE();
    case 834:
      ACCEPT_TOKEN(aux_sym_for_type_token3);
      END_STATE();
    case 835:
      ACCEPT_TOKEN(aux_sym_for_type_token3);
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(668);
      END_STATE();
    case 836:
      ACCEPT_TOKEN(aux_sym_having_clause_token1);
      END_STATE();
    case 837:
      ACCEPT_TOKEN(aux_sym_from_clause_token1);
      END_STATE();
    case 838:
      ACCEPT_TOKEN(aux_sym_from_clause_token1);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(1004);
      END_STATE();
    case 839:
      ACCEPT_TOKEN(aux_sym_storage_alias_token1);
      END_STATE();
    case 840:
      ACCEPT_TOKEN(aux_sym_storage_alias_token1);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(1004);
      END_STATE();
    case 841:
      ACCEPT_TOKEN(aux_sym_fields_type_token1);
      END_STATE();
    case 842:
      ACCEPT_TOKEN(aux_sym_fields_type_token2);
      END_STATE();
    case 843:
      ACCEPT_TOKEN(aux_sym_where_clause_token1);
      END_STATE();
    case 844:
      ACCEPT_TOKEN(aux_sym_where_clause_token1);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(1004);
      END_STATE();
    case 845:
      ACCEPT_TOKEN(aux_sym_and_expression_token1);
      END_STATE();
    case 846:
      ACCEPT_TOKEN(aux_sym_or_expression_token1);
      END_STATE();
    case 847:
      ACCEPT_TOKEN(aux_sym_or_expression_token1);
      if (lookahead == 'D' ||
          lookahead == 'd') ADVANCE(245);
      END_STATE();
    case 848:
      ACCEPT_TOKEN(aux_sym_not_expression_token1);
      END_STATE();
    case 849:
      ACCEPT_TOKEN(aux_sym_not_expression_token1);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(1004);
      END_STATE();
    case 850:
      ACCEPT_TOKEN(aux_sym_soql_with_type_token1);
      END_STATE();
    case 851:
      ACCEPT_TOKEN(aux_sym_soql_with_type_token2);
      END_STATE();
    case 852:
      ACCEPT_TOKEN(aux_sym_soql_with_type_token3);
      END_STATE();
    case 853:
      ACCEPT_TOKEN(aux_sym_with_user_id_type_token1);
      END_STATE();
    case 854:
      ACCEPT_TOKEN(aux_sym_with_record_visibility_expression_token1);
      END_STATE();
    case 855:
      ACCEPT_TOKEN(aux_sym_with_record_visibility_param_token1);
      END_STATE();
    case 856:
      ACCEPT_TOKEN(aux_sym_with_record_visibility_param_token2);
      END_STATE();
    case 857:
      ACCEPT_TOKEN(aux_sym_with_record_visibility_param_token3);
      END_STATE();
    case 858:
      ACCEPT_TOKEN(aux_sym_with_data_cat_expression_token1);
      END_STATE();
    case 859:
      ACCEPT_TOKEN(aux_sym_with_data_cat_filter_type_token1);
      END_STATE();
    case 860:
      ACCEPT_TOKEN(aux_sym_with_data_cat_filter_type_token2);
      if (lookahead == '_') ADVANCE(488);
      END_STATE();
    case 861:
      ACCEPT_TOKEN(aux_sym_with_data_cat_filter_type_token3);
      END_STATE();
    case 862:
      ACCEPT_TOKEN(aux_sym_with_data_cat_filter_type_token4);
      END_STATE();
    case 863:
      ACCEPT_TOKEN(aux_sym_limit_clause_token1);
      END_STATE();
    case 864:
      ACCEPT_TOKEN(aux_sym_limit_clause_token1);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(1004);
      END_STATE();
    case 865:
      ACCEPT_TOKEN(aux_sym_offset_clause_token1);
      END_STATE();
    case 866:
      ACCEPT_TOKEN(aux_sym_offset_clause_token1);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(1004);
      END_STATE();
    case 867:
      ACCEPT_TOKEN(aux_sym_update_type_token1);
      END_STATE();
    case 868:
      ACCEPT_TOKEN(aux_sym_update_type_token2);
      END_STATE();
    case 869:
      ACCEPT_TOKEN(aux_sym_order_by_clause_token1);
      END_STATE();
    case 870:
      ACCEPT_TOKEN(aux_sym_order_direction_token1);
      END_STATE();
    case 871:
      ACCEPT_TOKEN(aux_sym_order_direction_token2);
      END_STATE();
    case 872:
      ACCEPT_TOKEN(aux_sym_order_null_direction_token1);
      END_STATE();
    case 873:
      ACCEPT_TOKEN(aux_sym_order_null_direction_token2);
      END_STATE();
    case 874:
      ACCEPT_TOKEN(aux_sym_geo_location_type_token1);
      END_STATE();
    case 875:
      ACCEPT_TOKEN(aux_sym_geo_location_type_token1);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(1004);
      END_STATE();
    case 876:
      ACCEPT_TOKEN(aux_sym_function_expression_token1);
      END_STATE();
    case 877:
      ACCEPT_TOKEN(aux_sym_function_expression_token1);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(1004);
      END_STATE();
    case 878:
      ACCEPT_TOKEN(anon_sym_DOT);
      END_STATE();
    case 879:
      ACCEPT_TOKEN(aux_sym_boolean_token1);
      END_STATE();
    case 880:
      ACCEPT_TOKEN(aux_sym_boolean_token2);
      END_STATE();
    case 881:
      ACCEPT_TOKEN(anon_sym_BANG_EQ);
      END_STATE();
    case 882:
      ACCEPT_TOKEN(anon_sym_LT_GT);
      END_STATE();
    case 883:
      ACCEPT_TOKEN(anon_sym_LT);
      if (lookahead == '=') ADVANCE(884);
      if (lookahead == '>') ADVANCE(882);
      END_STATE();
    case 884:
      ACCEPT_TOKEN(anon_sym_LT_EQ);
      END_STATE();
    case 885:
      ACCEPT_TOKEN(anon_sym_GT);
      if (lookahead == '=') ADVANCE(886);
      END_STATE();
    case 886:
      ACCEPT_TOKEN(anon_sym_GT_EQ);
      END_STATE();
    case 887:
      ACCEPT_TOKEN(aux_sym_value_comparison_operator_token1);
      END_STATE();
    case 888:
      ACCEPT_TOKEN(aux_sym_set_comparison_operator_token1);
      END_STATE();
    case 889:
      ACCEPT_TOKEN(aux_sym_set_comparison_operator_token2);
      END_STATE();
    case 890:
      ACCEPT_TOKEN(aux_sym_set_comparison_operator_token3);
      END_STATE();
    case 891:
      ACCEPT_TOKEN(aux_sym_date_literal_token1);
      END_STATE();
    case 892:
      ACCEPT_TOKEN(aux_sym_date_literal_token2);
      END_STATE();
    case 893:
      ACCEPT_TOKEN(aux_sym_date_literal_token3);
      END_STATE();
    case 894:
      ACCEPT_TOKEN(aux_sym_date_literal_token4);
      END_STATE();
    case 895:
      ACCEPT_TOKEN(aux_sym_date_literal_token5);
      END_STATE();
    case 896:
      ACCEPT_TOKEN(aux_sym_date_literal_token6);
      END_STATE();
    case 897:
      ACCEPT_TOKEN(aux_sym_date_literal_token7);
      END_STATE();
    case 898:
      ACCEPT_TOKEN(aux_sym_date_literal_token8);
      END_STATE();
    case 899:
      ACCEPT_TOKEN(aux_sym_date_literal_token9);
      END_STATE();
    case 900:
      ACCEPT_TOKEN(aux_sym_date_literal_token10);
      END_STATE();
    case 901:
      ACCEPT_TOKEN(aux_sym_date_literal_token11);
      END_STATE();
    case 902:
      ACCEPT_TOKEN(aux_sym_date_literal_token12);
      END_STATE();
    case 903:
      ACCEPT_TOKEN(aux_sym_date_literal_token13);
      END_STATE();
    case 904:
      ACCEPT_TOKEN(aux_sym_date_literal_token14);
      END_STATE();
    case 905:
      ACCEPT_TOKEN(aux_sym_date_literal_token15);
      END_STATE();
    case 906:
      ACCEPT_TOKEN(aux_sym_date_literal_token16);
      END_STATE();
    case 907:
      ACCEPT_TOKEN(aux_sym_date_literal_token17);
      END_STATE();
    case 908:
      ACCEPT_TOKEN(aux_sym_date_literal_token18);
      END_STATE();
    case 909:
      ACCEPT_TOKEN(aux_sym_date_literal_token19);
      END_STATE();
    case 910:
      ACCEPT_TOKEN(aux_sym_date_literal_token20);
      END_STATE();
    case 911:
      ACCEPT_TOKEN(aux_sym_date_literal_token21);
      END_STATE();
    case 912:
      ACCEPT_TOKEN(aux_sym_date_literal_token22);
      END_STATE();
    case 913:
      ACCEPT_TOKEN(aux_sym_date_literal_token23);
      END_STATE();
    case 914:
      ACCEPT_TOKEN(aux_sym_date_literal_with_param_token1);
      END_STATE();
    case 915:
      ACCEPT_TOKEN(anon_sym_COLON);
      END_STATE();
    case 916:
      ACCEPT_TOKEN(sym_bound_apex_expression);
      END_STATE();
    case 917:
      ACCEPT_TOKEN(aux_sym_null_literal_token1);
      END_STATE();
    case 918:
      ACCEPT_TOKEN(aux_sym_null_literal_token1);
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(742);
      END_STATE();
    case 919:
      ACCEPT_TOKEN(sym_string_literal);
      END_STATE();
    case 920:
      ACCEPT_TOKEN(sym_int);
      if (lookahead == '-') ADVANCE(22);
      if (lookahead == '.') ADVANCE(753);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(927);
      END_STATE();
    case 921:
      ACCEPT_TOKEN(sym_int);
      if (lookahead == '.') ADVANCE(753);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(920);
      END_STATE();
    case 922:
      ACCEPT_TOKEN(sym_int);
      if (lookahead == '.') ADVANCE(753);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(921);
      END_STATE();
    case 923:
      ACCEPT_TOKEN(sym_int);
      if (lookahead == '.') ADVANCE(753);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(921);
      if (('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(759);
      END_STATE();
    case 924:
      ACCEPT_TOKEN(sym_int);
      if (lookahead == '.') ADVANCE(753);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(923);
      if (('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(765);
      END_STATE();
    case 925:
      ACCEPT_TOKEN(sym_int);
      if (lookahead == '.') ADVANCE(753);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(928);
      if (('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(765);
      END_STATE();
    case 926:
      ACCEPT_TOKEN(sym_int);
      if (lookahead == '.') ADVANCE(753);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(922);
      END_STATE();
    case 927:
      ACCEPT_TOKEN(sym_int);
      if (lookahead == '.') ADVANCE(753);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(927);
      END_STATE();
    case 928:
      ACCEPT_TOKEN(sym_int);
      if (lookahead == '.') ADVANCE(753);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(927);
      if (('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(759);
      END_STATE();
    case 929:
      ACCEPT_TOKEN(sym_decimal);
      if (lookahead == '.') ADVANCE(753);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(929);
      END_STATE();
    case 930:
      ACCEPT_TOKEN(sym_decimal);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(930);
      END_STATE();
    case 931:
      ACCEPT_TOKEN(sym_date);
      if (lookahead == 'T') ADVANCE(26);
      END_STATE();
    case 932:
      ACCEPT_TOKEN(sym_date_time);
      END_STATE();
    case 933:
      ACCEPT_TOKEN(sym_currency_literal);
      if (lookahead == '.') ADVANCE(760);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(933);
      END_STATE();
    case 934:
      ACCEPT_TOKEN(sym_currency_literal);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(934);
      END_STATE();
    case 935:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(969);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('B' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('b' <= lookahead && lookahead <= 'z')) ADVANCE(1004);
      END_STATE();
    case 936:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(999);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('B' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('b' <= lookahead && lookahead <= 'z')) ADVANCE(1004);
      END_STATE();
    case 937:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(998);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('B' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('b' <= lookahead && lookahead <= 'z')) ADVANCE(1004);
      END_STATE();
    case 938:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'C' ||
          lookahead == 'c') ADVANCE(945);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(1004);
      END_STATE();
    case 939:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'C' ||
          lookahead == 'c') ADVANCE(937);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(1004);
      END_STATE();
    case 940:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'D' ||
          lookahead == 'd') ADVANCE(989);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(1004);
      END_STATE();
    case 941:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'D' ||
          lookahead == 'd') ADVANCE(936);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(1004);
      END_STATE();
    case 942:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'D' ||
          lookahead == 'd') ADVANCE(949);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(1004);
      END_STATE();
    case 943:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(965);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(1004);
      END_STATE();
    case 944:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(972);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(1004);
      END_STATE();
    case 945:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(877);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(1004);
      END_STATE();
    case 946:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(844);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(1004);
      END_STATE();
    case 947:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(832);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(1004);
      END_STATE();
    case 948:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(985);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(1004);
      END_STATE();
    case 949:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(987);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(1004);
      END_STATE();
    case 950:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(975);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(1004);
      END_STATE();
    case 951:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(997);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(1004);
      END_STATE();
    case 952:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'F' ||
          lookahead == 'f') ADVANCE(823);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(1004);
      END_STATE();
    case 953:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'F' ||
          lookahead == 'f') ADVANCE(954);
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(942);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(1004);
      END_STATE();
    case 954:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'F' ||
          lookahead == 'f') ADVANCE(992);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(1004);
      END_STATE();
    case 955:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'G' ||
          lookahead == 'g') ADVANCE(798);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(1004);
      END_STATE();
    case 956:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'H' ||
          lookahead == 'h') ADVANCE(809);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(1004);
      END_STATE();
    case 957:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'H' ||
          lookahead == 'h') ADVANCE(948);
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(995);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(1004);
      END_STATE();
    case 958:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'H' ||
          lookahead == 'h') ADVANCE(948);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(1004);
      END_STATE();
    case 959:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(988);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(1004);
      END_STATE();
    case 960:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(943);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(1004);
      END_STATE();
    case 961:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(968);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(1004);
      END_STATE();
    case 962:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(970);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(1004);
      END_STATE();
    case 963:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(996);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(1004);
      END_STATE();
    case 964:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(979);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(1004);
      END_STATE();
    case 965:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'L' ||
          lookahead == 'l') ADVANCE(940);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(1004);
      END_STATE();
    case 966:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'L' ||
          lookahead == 'l') ADVANCE(976);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(1004);
      END_STATE();
    case 967:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'M' ||
          lookahead == 'm') ADVANCE(838);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(1004);
      END_STATE();
    case 968:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'M' ||
          lookahead == 'm') ADVANCE(963);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(1004);
      END_STATE();
    case 969:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(938);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(1004);
      END_STATE();
    case 970:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(955);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(1004);
      END_STATE();
    case 971:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(875);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(1004);
      END_STATE();
    case 972:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(952);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(1004);
      END_STATE();
    case 973:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(1000);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(1004);
      END_STATE();
    case 974:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(994);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(1004);
      END_STATE();
    case 975:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(966);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(1004);
      END_STATE();
    case 976:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(939);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(1004);
      END_STATE();
    case 977:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(983);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(1004);
      END_STATE();
    case 978:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(967);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(1004);
      END_STATE();
    case 979:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(971);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(1004);
      END_STATE();
    case 980:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'P' ||
          lookahead == 'p') ADVANCE(1002);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(1004);
      END_STATE();
    case 981:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'P' ||
          lookahead == 'p') ADVANCE(944);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(1004);
      END_STATE();
    case 982:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'P' ||
          lookahead == 'p') ADVANCE(941);
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(962);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(1004);
      END_STATE();
    case 983:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(830);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(1004);
      END_STATE();
    case 984:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(973);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(1004);
      END_STATE();
    case 985:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(946);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(1004);
      END_STATE();
    case 986:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(978);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(1004);
      END_STATE();
    case 987:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(1003);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(1004);
      END_STATE();
    case 988:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(993);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(1004);
      END_STATE();
    case 989:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(778);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(1004);
      END_STATE();
    case 990:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(840);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(1004);
      END_STATE();
    case 991:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(962);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(1004);
      END_STATE();
    case 992:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(951);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(1004);
      END_STATE();
    case 993:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(935);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(1004);
      END_STATE();
    case 994:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(849);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(1004);
      END_STATE();
    case 995:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(956);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(1004);
      END_STATE();
    case 996:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(864);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(1004);
      END_STATE();
    case 997:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(866);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(1004);
      END_STATE();
    case 998:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(964);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(1004);
      END_STATE();
    case 999:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(947);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(1004);
      END_STATE();
    case 1000:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'U' ||
          lookahead == 'u') ADVANCE(980);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(1004);
      END_STATE();
    case 1001:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'Y' ||
          lookahead == 'y') ADVANCE(981);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(1004);
      END_STATE();
    case 1002:
      ACCEPT_TOKEN(sym_identifier);
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') ADVANCE(141);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(1004);
      END_STATE();
    case 1003:
      ACCEPT_TOKEN(sym_identifier);
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') ADVANCE(142);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(1004);
      END_STATE();
    case 1004:
      ACCEPT_TOKEN(sym_identifier);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(1004);
      END_STATE();
    default:
      return false;
  }
}

static const TSLexMode ts_lex_modes[STATE_COUNT] = {
  [0] = {.lex_state = 0},
  [1] = {.lex_state = 0},
  [2] = {.lex_state = 2},
  [3] = {.lex_state = 2},
  [4] = {.lex_state = 2},
  [5] = {.lex_state = 2},
  [6] = {.lex_state = 2},
  [7] = {.lex_state = 2},
  [8] = {.lex_state = 2},
  [9] = {.lex_state = 1},
  [10] = {.lex_state = 1},
  [11] = {.lex_state = 1},
  [12] = {.lex_state = 1},
  [13] = {.lex_state = 1},
  [14] = {.lex_state = 1},
  [15] = {.lex_state = 1},
  [16] = {.lex_state = 1},
  [17] = {.lex_state = 1},
  [18] = {.lex_state = 0},
  [19] = {.lex_state = 0},
  [20] = {.lex_state = 1},
  [21] = {.lex_state = 1},
  [22] = {.lex_state = 0},
  [23] = {.lex_state = 0},
  [24] = {.lex_state = 1},
  [25] = {.lex_state = 8},
  [26] = {.lex_state = 8},
  [27] = {.lex_state = 0},
  [28] = {.lex_state = 9},
  [29] = {.lex_state = 0},
  [30] = {.lex_state = 9},
  [31] = {.lex_state = 9},
  [32] = {.lex_state = 8},
  [33] = {.lex_state = 10},
  [34] = {.lex_state = 9},
  [35] = {.lex_state = 10},
  [36] = {.lex_state = 0},
  [37] = {.lex_state = 8},
  [38] = {.lex_state = 0},
  [39] = {.lex_state = 0},
  [40] = {.lex_state = 10},
  [41] = {.lex_state = 0},
  [42] = {.lex_state = 0},
  [43] = {.lex_state = 0},
  [44] = {.lex_state = 0},
  [45] = {.lex_state = 0},
  [46] = {.lex_state = 0},
  [47] = {.lex_state = 10},
  [48] = {.lex_state = 10},
  [49] = {.lex_state = 0},
  [50] = {.lex_state = 10},
  [51] = {.lex_state = 0},
  [52] = {.lex_state = 10},
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
  [65] = {.lex_state = 1},
  [66] = {.lex_state = 0},
  [67] = {.lex_state = 0},
  [68] = {.lex_state = 0},
  [69] = {.lex_state = 0},
  [70] = {.lex_state = 0},
  [71] = {.lex_state = 0},
  [72] = {.lex_state = 4},
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
  [88] = {.lex_state = 0},
  [89] = {.lex_state = 0},
  [90] = {.lex_state = 0},
  [91] = {.lex_state = 0},
  [92] = {.lex_state = 0},
  [93] = {.lex_state = 5},
  [94] = {.lex_state = 0},
  [95] = {.lex_state = 0},
  [96] = {.lex_state = 0},
  [97] = {.lex_state = 0},
  [98] = {.lex_state = 0},
  [99] = {.lex_state = 0},
  [100] = {.lex_state = 4},
  [101] = {.lex_state = 4},
  [102] = {.lex_state = 5},
  [103] = {.lex_state = 0},
  [104] = {.lex_state = 5},
  [105] = {.lex_state = 5},
  [106] = {.lex_state = 0},
  [107] = {.lex_state = 0},
  [108] = {.lex_state = 0},
  [109] = {.lex_state = 0},
  [110] = {.lex_state = 0},
  [111] = {.lex_state = 0},
  [112] = {.lex_state = 0},
  [113] = {.lex_state = 0},
  [114] = {.lex_state = 0},
  [115] = {.lex_state = 0},
  [116] = {.lex_state = 4},
  [117] = {.lex_state = 0},
  [118] = {.lex_state = 0},
  [119] = {.lex_state = 0},
  [120] = {.lex_state = 0},
  [121] = {.lex_state = 0},
  [122] = {.lex_state = 0},
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
  [136] = {.lex_state = 1},
  [137] = {.lex_state = 0},
  [138] = {.lex_state = 0},
  [139] = {.lex_state = 0},
  [140] = {.lex_state = 0},
  [141] = {.lex_state = 0},
  [142] = {.lex_state = 4},
  [143] = {.lex_state = 0},
  [144] = {.lex_state = 0},
  [145] = {.lex_state = 0},
  [146] = {.lex_state = 0},
  [147] = {.lex_state = 0},
  [148] = {.lex_state = 0},
  [149] = {.lex_state = 0},
  [150] = {.lex_state = 0},
  [151] = {.lex_state = 4},
  [152] = {.lex_state = 0},
  [153] = {.lex_state = 0},
  [154] = {.lex_state = 4},
  [155] = {.lex_state = 4},
  [156] = {.lex_state = 0},
  [157] = {.lex_state = 5},
  [158] = {.lex_state = 0},
  [159] = {.lex_state = 5},
  [160] = {.lex_state = 1},
  [161] = {.lex_state = 0},
  [162] = {.lex_state = 0},
  [163] = {.lex_state = 0},
  [164] = {.lex_state = 0},
  [165] = {.lex_state = 0},
  [166] = {.lex_state = 4},
  [167] = {.lex_state = 4},
  [168] = {.lex_state = 0},
  [169] = {.lex_state = 0},
  [170] = {.lex_state = 0},
  [171] = {.lex_state = 0},
  [172] = {.lex_state = 0},
  [173] = {.lex_state = 0},
  [174] = {.lex_state = 0},
  [175] = {.lex_state = 0},
  [176] = {.lex_state = 5},
  [177] = {.lex_state = 0},
  [178] = {.lex_state = 0},
  [179] = {.lex_state = 0},
  [180] = {.lex_state = 5},
  [181] = {.lex_state = 0},
  [182] = {.lex_state = 0},
  [183] = {.lex_state = 0},
  [184] = {.lex_state = 0},
  [185] = {.lex_state = 0},
  [186] = {.lex_state = 5},
  [187] = {.lex_state = 0},
  [188] = {.lex_state = 0},
  [189] = {.lex_state = 0},
  [190] = {.lex_state = 0},
  [191] = {.lex_state = 0},
  [192] = {.lex_state = 0},
  [193] = {.lex_state = 0},
  [194] = {.lex_state = 0},
  [195] = {.lex_state = 0},
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
  [217] = {.lex_state = 0},
  [218] = {.lex_state = 0},
  [219] = {.lex_state = 0},
  [220] = {.lex_state = 0},
  [221] = {.lex_state = 0},
  [222] = {.lex_state = 0},
  [223] = {.lex_state = 0},
  [224] = {.lex_state = 0},
  [225] = {.lex_state = 5},
  [226] = {.lex_state = 0},
  [227] = {.lex_state = 0},
  [228] = {.lex_state = 0},
  [229] = {.lex_state = 0},
  [230] = {.lex_state = 5},
  [231] = {.lex_state = 5},
  [232] = {.lex_state = 0},
  [233] = {.lex_state = 0},
  [234] = {.lex_state = 0},
  [235] = {.lex_state = 0},
  [236] = {.lex_state = 12},
  [237] = {.lex_state = 7},
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
  [249] = {.lex_state = 0},
  [250] = {.lex_state = 0},
  [251] = {.lex_state = 0},
  [252] = {.lex_state = 0},
  [253] = {.lex_state = 5},
  [254] = {.lex_state = 0},
  [255] = {.lex_state = 0},
  [256] = {.lex_state = 12},
  [257] = {.lex_state = 0},
  [258] = {.lex_state = 0},
  [259] = {.lex_state = 12},
  [260] = {.lex_state = 0},
  [261] = {.lex_state = 0},
  [262] = {.lex_state = 0},
  [263] = {.lex_state = 0},
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
  [292] = {.lex_state = 0},
  [293] = {.lex_state = 0},
  [294] = {.lex_state = 0},
  [295] = {.lex_state = 0},
  [296] = {.lex_state = 0},
  [297] = {.lex_state = 0},
  [298] = {.lex_state = 0},
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
  [327] = {.lex_state = 0},
  [328] = {.lex_state = 0},
  [329] = {.lex_state = 0},
  [330] = {.lex_state = 0},
  [331] = {.lex_state = 0},
  [332] = {.lex_state = 0},
  [333] = {.lex_state = 11},
  [334] = {.lex_state = 0},
  [335] = {.lex_state = 7},
  [336] = {.lex_state = 7},
  [337] = {.lex_state = 0},
  [338] = {.lex_state = 0},
  [339] = {.lex_state = 0},
  [340] = {.lex_state = 0},
  [341] = {.lex_state = 0},
  [342] = {.lex_state = 0},
  [343] = {.lex_state = 0},
  [344] = {.lex_state = 0},
  [345] = {.lex_state = 0},
  [346] = {.lex_state = 0},
  [347] = {.lex_state = 0},
  [348] = {.lex_state = 6},
  [349] = {.lex_state = 0},
  [350] = {.lex_state = 0},
  [351] = {.lex_state = 0},
  [352] = {.lex_state = 0},
  [353] = {.lex_state = 6},
  [354] = {.lex_state = 6},
  [355] = {.lex_state = 0},
  [356] = {.lex_state = 0},
  [357] = {.lex_state = 7},
  [358] = {.lex_state = 0},
  [359] = {.lex_state = 6},
  [360] = {.lex_state = 2},
  [361] = {.lex_state = 0},
  [362] = {.lex_state = 0},
  [363] = {.lex_state = 2},
  [364] = {.lex_state = 0},
  [365] = {.lex_state = 0},
  [366] = {.lex_state = 6},
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
  [394] = {.lex_state = 0},
  [395] = {.lex_state = 0},
  [396] = {.lex_state = 0},
  [397] = {.lex_state = 1},
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
  [408] = {.lex_state = 0},
  [409] = {.lex_state = 0},
  [410] = {.lex_state = 0},
  [411] = {.lex_state = 0},
  [412] = {.lex_state = 0},
  [413] = {.lex_state = 0},
  [414] = {.lex_state = 0},
  [415] = {.lex_state = 0},
  [416] = {.lex_state = 0},
  [417] = {.lex_state = 0},
  [418] = {.lex_state = 0},
  [419] = {.lex_state = 0},
  [420] = {.lex_state = 0},
  [421] = {.lex_state = 0},
  [422] = {.lex_state = 0},
  [423] = {.lex_state = 0},
  [424] = {.lex_state = 0},
  [425] = {.lex_state = 1},
  [426] = {.lex_state = 0},
  [427] = {.lex_state = 0},
  [428] = {.lex_state = 0},
  [429] = {.lex_state = 0},
  [430] = {.lex_state = 0},
  [431] = {.lex_state = 0},
  [432] = {.lex_state = 0},
  [433] = {.lex_state = 0},
  [434] = {.lex_state = 0},
  [435] = {.lex_state = 0},
  [436] = {.lex_state = 0},
  [437] = {.lex_state = 0},
  [438] = {.lex_state = 0},
  [439] = {.lex_state = 0},
  [440] = {.lex_state = 0},
  [441] = {.lex_state = 0},
  [442] = {.lex_state = 0},
  [443] = {.lex_state = 0},
  [444] = {.lex_state = 0},
  [445] = {.lex_state = 0},
  [446] = {.lex_state = 0},
  [447] = {.lex_state = 0},
  [448] = {.lex_state = 0},
  [449] = {.lex_state = 0},
  [450] = {.lex_state = 0},
  [451] = {.lex_state = 0},
  [452] = {.lex_state = 0},
  [453] = {.lex_state = 0},
  [454] = {.lex_state = 0},
  [455] = {.lex_state = 0},
  [456] = {.lex_state = 0},
  [457] = {.lex_state = 0},
  [458] = {.lex_state = 0},
  [459] = {.lex_state = 0},
  [460] = {.lex_state = 0},
  [461] = {.lex_state = 0},
  [462] = {.lex_state = 0},
  [463] = {.lex_state = 0},
  [464] = {.lex_state = 0},
  [465] = {.lex_state = 0},
  [466] = {.lex_state = 0},
  [467] = {.lex_state = 0},
  [468] = {.lex_state = 0},
  [469] = {.lex_state = 0},
  [470] = {.lex_state = 0},
  [471] = {.lex_state = 0},
  [472] = {.lex_state = 0},
  [473] = {.lex_state = 0},
  [474] = {.lex_state = 0},
  [475] = {.lex_state = 0},
  [476] = {.lex_state = 6},
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
  [509] = {.lex_state = 11},
  [510] = {.lex_state = 0},
  [511] = {.lex_state = 7},
  [512] = {.lex_state = 7},
  [513] = {.lex_state = 7},
  [514] = {.lex_state = 11},
  [515] = {.lex_state = 11},
  [516] = {.lex_state = 11},
  [517] = {.lex_state = 11},
  [518] = {.lex_state = 11},
  [519] = {.lex_state = 11},
  [520] = {.lex_state = 0},
  [521] = {.lex_state = 0},
  [522] = {.lex_state = 11},
  [523] = {.lex_state = 0},
  [524] = {.lex_state = 0},
  [525] = {.lex_state = 11},
  [526] = {.lex_state = 0},
  [527] = {.lex_state = 11},
  [528] = {.lex_state = 11},
  [529] = {.lex_state = 11},
  [530] = {.lex_state = 11},
  [531] = {.lex_state = 7},
  [532] = {.lex_state = 11},
  [533] = {.lex_state = 11},
  [534] = {.lex_state = 11},
  [535] = {.lex_state = 7},
  [536] = {.lex_state = 7},
  [537] = {.lex_state = 6},
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
  [555] = {.lex_state = 0},
  [556] = {.lex_state = 0},
  [557] = {.lex_state = 0},
  [558] = {.lex_state = 0},
  [559] = {.lex_state = 0},
  [560] = {.lex_state = 0},
  [561] = {.lex_state = 0},
  [562] = {.lex_state = 0},
  [563] = {.lex_state = 6},
  [564] = {.lex_state = 6},
  [565] = {.lex_state = 6},
  [566] = {.lex_state = 0},
  [567] = {.lex_state = 6},
  [568] = {.lex_state = 0},
  [569] = {.lex_state = 6},
  [570] = {.lex_state = 0},
  [571] = {.lex_state = 1},
  [572] = {.lex_state = 0},
  [573] = {.lex_state = 1},
  [574] = {.lex_state = 6},
  [575] = {.lex_state = 6},
  [576] = {.lex_state = 6},
  [577] = {.lex_state = 0},
  [578] = {.lex_state = 11},
  [579] = {.lex_state = 6},
  [580] = {.lex_state = 0},
  [581] = {.lex_state = 0},
  [582] = {.lex_state = 0},
  [583] = {.lex_state = 6},
  [584] = {.lex_state = 0},
  [585] = {.lex_state = 6},
  [586] = {.lex_state = 0},
  [587] = {.lex_state = 0},
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
  [606] = {.lex_state = 20},
  [607] = {.lex_state = 6},
  [608] = {.lex_state = 0},
  [609] = {.lex_state = 0},
  [610] = {.lex_state = 0},
  [611] = {.lex_state = 6},
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
  [635] = {.lex_state = 768},
  [636] = {.lex_state = 0},
  [637] = {.lex_state = 0},
  [638] = {.lex_state = 0},
  [639] = {.lex_state = 0},
  [640] = {.lex_state = 0},
  [641] = {.lex_state = 0},
  [642] = {.lex_state = 6},
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
  [654] = {.lex_state = 0},
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
  [670] = {.lex_state = 0},
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
  [715] = {.lex_state = 768},
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
  [726] = {.lex_state = 6},
  [727] = {.lex_state = 0},
  [728] = {.lex_state = 0},
  [729] = {.lex_state = 0},
  [730] = {.lex_state = 0},
  [731] = {.lex_state = 0},
  [732] = {.lex_state = 0},
  [733] = {.lex_state = 0},
  [734] = {.lex_state = 0},
  [735] = {.lex_state = 0},
  [736] = {.lex_state = 0},
  [737] = {.lex_state = 0},
  [738] = {.lex_state = 0},
  [739] = {.lex_state = 0},
  [740] = {.lex_state = 0},
  [741] = {.lex_state = 0},
  [742] = {.lex_state = 0},
  [743] = {.lex_state = 0},
  [744] = {.lex_state = 0},
  [745] = {.lex_state = 0},
  [746] = {.lex_state = 0},
  [747] = {.lex_state = 0},
  [748] = {.lex_state = 0},
  [749] = {.lex_state = 0},
  [750] = {.lex_state = 0},
  [751] = {.lex_state = 0},
  [752] = {.lex_state = 0},
  [753] = {.lex_state = 0},
  [754] = {.lex_state = 0},
  [755] = {.lex_state = 0},
  [756] = {.lex_state = 0},
  [757] = {.lex_state = 0},
  [758] = {.lex_state = 0},
  [759] = {.lex_state = 0},
  [760] = {.lex_state = 0},
  [761] = {.lex_state = 0},
  [762] = {.lex_state = 0},
  [763] = {.lex_state = 0},
  [764] = {.lex_state = 0},
  [765] = {.lex_state = 0},
  [766] = {.lex_state = 0},
  [767] = {.lex_state = 0},
  [768] = {.lex_state = 0},
  [769] = {.lex_state = 0},
  [770] = {.lex_state = 0},
  [771] = {.lex_state = 0},
  [772] = {.lex_state = 0},
  [773] = {.lex_state = 0},
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
  [787] = {.lex_state = 0},
  [788] = {.lex_state = 0},
  [789] = {.lex_state = 0},
  [790] = {.lex_state = 0},
  [791] = {.lex_state = 0},
  [792] = {.lex_state = 0},
  [793] = {.lex_state = 0},
  [794] = {.lex_state = 0},
  [795] = {.lex_state = 0},
  [796] = {.lex_state = 0},
  [797] = {.lex_state = 0},
  [798] = {.lex_state = 0},
  [799] = {.lex_state = 0},
  [800] = {.lex_state = 0},
  [801] = {.lex_state = 0},
  [802] = {.lex_state = 0},
  [803] = {.lex_state = 0},
  [804] = {.lex_state = 0},
  [805] = {.lex_state = 0},
  [806] = {.lex_state = 0},
  [807] = {.lex_state = 0},
  [808] = {.lex_state = 0},
  [809] = {.lex_state = 0},
  [810] = {.lex_state = 0},
  [811] = {.lex_state = 0},
  [812] = {.lex_state = 0},
  [813] = {.lex_state = 0},
  [814] = {.lex_state = 0},
  [815] = {.lex_state = 0},
  [816] = {.lex_state = 0},
  [817] = {.lex_state = 0},
  [818] = {.lex_state = 0},
  [819] = {.lex_state = 0},
  [820] = {.lex_state = 0},
  [821] = {.lex_state = 0},
  [822] = {.lex_state = 0},
  [823] = {.lex_state = 0},
  [824] = {.lex_state = 0},
  [825] = {.lex_state = 0},
  [826] = {.lex_state = 0},
  [827] = {.lex_state = 0},
  [828] = {.lex_state = 0},
  [829] = {.lex_state = 0},
  [830] = {.lex_state = 0},
  [831] = {.lex_state = 0},
  [832] = {.lex_state = 0},
  [833] = {.lex_state = 0},
  [834] = {.lex_state = 0},
  [835] = {.lex_state = 0},
  [836] = {.lex_state = 0},
  [837] = {.lex_state = 0},
  [838] = {.lex_state = 0},
  [839] = {.lex_state = 0},
  [840] = {.lex_state = 0},
  [841] = {.lex_state = 0},
  [842] = {.lex_state = 0},
  [843] = {.lex_state = 0},
  [844] = {.lex_state = 0},
  [845] = {.lex_state = 0},
  [846] = {.lex_state = 0},
  [847] = {.lex_state = 0},
  [848] = {.lex_state = 0},
  [849] = {.lex_state = 0},
  [850] = {.lex_state = 0},
  [851] = {.lex_state = 0},
  [852] = {.lex_state = 0},
  [853] = {.lex_state = 0},
  [854] = {.lex_state = 0},
  [855] = {.lex_state = 0},
  [856] = {.lex_state = 0},
  [857] = {.lex_state = 0},
  [858] = {.lex_state = 0},
  [859] = {.lex_state = 0},
  [860] = {.lex_state = 6},
  [861] = {.lex_state = 0},
  [862] = {.lex_state = 0},
  [863] = {.lex_state = 0},
  [864] = {.lex_state = 0},
  [865] = {.lex_state = 0},
  [866] = {.lex_state = 0},
  [867] = {.lex_state = 0},
  [868] = {.lex_state = 0},
  [869] = {.lex_state = 0},
  [870] = {.lex_state = 0},
  [871] = {.lex_state = 0},
  [872] = {.lex_state = 0},
  [873] = {.lex_state = 0},
  [874] = {.lex_state = 0},
  [875] = {.lex_state = 0},
  [876] = {.lex_state = 0},
  [877] = {.lex_state = 0},
  [878] = {.lex_state = 0},
  [879] = {.lex_state = 0},
  [880] = {.lex_state = 0},
  [881] = {.lex_state = 0},
  [882] = {.lex_state = 0},
  [883] = {.lex_state = 0},
  [884] = {.lex_state = 0},
  [885] = {.lex_state = 0},
  [886] = {.lex_state = 0},
  [887] = {.lex_state = 0},
  [888] = {.lex_state = 0},
  [889] = {.lex_state = 0},
  [890] = {.lex_state = 0},
  [891] = {.lex_state = 0},
  [892] = {.lex_state = 0},
  [893] = {.lex_state = 0},
  [894] = {.lex_state = 0},
  [895] = {.lex_state = 0},
  [896] = {.lex_state = 0},
  [897] = {.lex_state = 0},
  [898] = {.lex_state = 0},
  [899] = {.lex_state = 0},
  [900] = {.lex_state = 6},
  [901] = {.lex_state = 0},
  [902] = {.lex_state = 0},
  [903] = {.lex_state = 0},
  [904] = {.lex_state = 0},
  [905] = {.lex_state = 0},
  [906] = {.lex_state = 0},
  [907] = {.lex_state = 0},
  [908] = {.lex_state = 0},
  [909] = {.lex_state = 0},
  [910] = {.lex_state = 6},
  [911] = {.lex_state = 0},
  [912] = {.lex_state = 0},
  [913] = {.lex_state = 0},
  [914] = {.lex_state = 6},
  [915] = {.lex_state = 0},
  [916] = {.lex_state = 0},
  [917] = {.lex_state = 0},
  [918] = {.lex_state = 0},
  [919] = {.lex_state = 0},
  [920] = {.lex_state = 6},
  [921] = {.lex_state = 0},
  [922] = {.lex_state = 0},
  [923] = {.lex_state = 0},
  [924] = {.lex_state = 0},
  [925] = {.lex_state = 0},
  [926] = {.lex_state = 0},
  [927] = {.lex_state = 0},
  [928] = {.lex_state = 0},
  [929] = {.lex_state = 0},
  [930] = {.lex_state = 0},
  [931] = {.lex_state = 0},
  [932] = {.lex_state = 0},
  [933] = {.lex_state = 0},
  [934] = {.lex_state = 0},
  [935] = {(TSStateId)(-1)},
};

static const uint16_t ts_parse_table[LARGE_STATE_COUNT][SYMBOL_COUNT] = {
  [0] = {
    [sym_formatting_comment] = STATE(0),
    [ts_builtin_sym_end] = ACTIONS(1),
    [anon_sym_SLASH_SLASH] = ACTIONS(1),
    [anon_sym_SLASH_SLASH_SLASH] = ACTIONS(3),
    [aux_sym_find_clause_token1] = ACTIONS(1),
    [aux_sym_in_clause_token1] = ACTIONS(1),
    [aux_sym_in_clause_token2] = ACTIONS(1),
    [aux_sym_in_type_token1] = ACTIONS(1),
    [aux_sym_in_type_token2] = ACTIONS(1),
    [aux_sym_in_type_token3] = ACTIONS(1),
    [aux_sym_in_type_token4] = ACTIONS(1),
    [aux_sym_in_type_token5] = ACTIONS(1),
    [sym_term_separator_start] = ACTIONS(1),
    [sym_term_separator_end] = ACTIONS(1),
    [aux_sym_returning_clause_token1] = ACTIONS(1),
    [anon_sym_COMMA] = ACTIONS(1),
    [anon_sym_LPAREN] = ACTIONS(1),
    [anon_sym_RPAREN] = ACTIONS(1),
    [aux_sym_using_phrase_search_token1] = ACTIONS(1),
    [aux_sym_using_advanced_search_token1] = ACTIONS(1),
    [aux_sym_sosl_using_clause_token1] = ACTIONS(1),
    [aux_sym_with_division_expression_token1] = ACTIONS(1),
    [anon_sym_EQ] = ACTIONS(1),
    [aux_sym_with_highlight_token1] = ACTIONS(1),
    [aux_sym_with_metadata_expression_token1] = ACTIONS(1),
    [aux_sym_with_network_expression_token1] = ACTIONS(1),
    [aux_sym_with_pricebook_expression_token1] = ACTIONS(1),
    [aux_sym_with_snippet_expression_token1] = ACTIONS(1),
    [aux_sym_with_snippet_expression_token2] = ACTIONS(1),
    [aux_sym_with_spell_correction_expression_token1] = ACTIONS(1),
    [aux_sym_sosl_with_clause_token1] = ACTIONS(1),
    [aux_sym_select_clause_token1] = ACTIONS(1),
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
    [aux_sym_fields_type_token1] = ACTIONS(1),
    [aux_sym_fields_type_token2] = ACTIONS(1),
    [aux_sym_where_clause_token1] = ACTIONS(1),
    [aux_sym_and_expression_token1] = ACTIONS(1),
    [aux_sym_or_expression_token1] = ACTIONS(1),
    [aux_sym_not_expression_token1] = ACTIONS(1),
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
    [aux_sym_set_comparison_operator_token2] = ACTIONS(1),
    [aux_sym_set_comparison_operator_token3] = ACTIONS(1),
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
    [sym_source_file] = STATE(609),
    [sym_header_comment] = STATE(342),
    [sym_formatting_comment] = STATE(1),
    [sym__query_expression] = STATE(652),
    [sym_sosl_query_body] = STATE(704),
    [sym_find_clause] = STATE(19),
    [anon_sym_SLASH_SLASH] = ACTIONS(5),
    [anon_sym_SLASH_SLASH_SLASH] = ACTIONS(3),
    [aux_sym_find_clause_token1] = ACTIONS(7),
  },
};

static const uint16_t ts_small_parse_table[] = {
  [0] = 14,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(9), 1,
      aux_sym_select_clause_token1,
    ACTIONS(15), 1,
      aux_sym_date_literal_with_param_token1,
    ACTIONS(17), 1,
      sym_bound_apex_expression,
    ACTIONS(19), 1,
      aux_sym_null_literal_token1,
    STATE(2), 1,
      sym_formatting_comment,
    STATE(369), 1,
      sym__soql_literal,
    STATE(562), 1,
      sym_select_clause,
    STATE(621), 1,
      sym_soql_query_body,
    ACTIONS(11), 2,
      aux_sym_boolean_token1,
      aux_sym_boolean_token2,
    ACTIONS(21), 2,
      sym_string_literal,
      sym_date_time,
    ACTIONS(23), 4,
      sym_int,
      sym_decimal,
      sym_date,
      sym_currency_literal,
    STATE(45), 4,
      sym_boolean,
      sym_date_literal,
      sym_date_literal_with_param,
      sym_null_literal,
    ACTIONS(13), 23,
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
    ACTIONS(9), 1,
      aux_sym_select_clause_token1,
    ACTIONS(15), 1,
      aux_sym_date_literal_with_param_token1,
    ACTIONS(19), 1,
      aux_sym_null_literal_token1,
    ACTIONS(25), 1,
      sym_bound_apex_expression,
    STATE(3), 1,
      sym_formatting_comment,
    STATE(546), 1,
      sym__soql_literal,
    STATE(562), 1,
      sym_select_clause,
    STATE(899), 1,
      sym_soql_query_body,
    ACTIONS(11), 2,
      aux_sym_boolean_token1,
      aux_sym_boolean_token2,
    ACTIONS(21), 2,
      sym_string_literal,
      sym_date_time,
    ACTIONS(23), 4,
      sym_int,
      sym_decimal,
      sym_date,
      sym_currency_literal,
    STATE(45), 4,
      sym_boolean,
      sym_date_literal,
      sym_date_literal_with_param,
      sym_null_literal,
    ACTIONS(13), 23,
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
    ACTIONS(15), 1,
      aux_sym_date_literal_with_param_token1,
    ACTIONS(19), 1,
      aux_sym_null_literal_token1,
    ACTIONS(27), 1,
      sym_bound_apex_expression,
    STATE(4), 1,
      sym_formatting_comment,
    STATE(586), 1,
      sym__soql_literal,
    ACTIONS(11), 2,
      aux_sym_boolean_token1,
      aux_sym_boolean_token2,
    ACTIONS(21), 2,
      sym_string_literal,
      sym_date_time,
    ACTIONS(23), 4,
      sym_int,
      sym_decimal,
      sym_date,
      sym_currency_literal,
    STATE(45), 4,
      sym_boolean,
      sym_date_literal,
      sym_date_literal_with_param,
      sym_null_literal,
    ACTIONS(13), 23,
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
    ACTIONS(15), 1,
      aux_sym_date_literal_with_param_token1,
    ACTIONS(19), 1,
      aux_sym_null_literal_token1,
    ACTIONS(29), 1,
      sym_bound_apex_expression,
    STATE(5), 1,
      sym_formatting_comment,
    STATE(55), 1,
      sym__soql_literal,
    ACTIONS(11), 2,
      aux_sym_boolean_token1,
      aux_sym_boolean_token2,
    ACTIONS(21), 2,
      sym_string_literal,
      sym_date_time,
    ACTIONS(23), 4,
      sym_int,
      sym_decimal,
      sym_date,
      sym_currency_literal,
    STATE(45), 4,
      sym_boolean,
      sym_date_literal,
      sym_date_literal_with_param,
      sym_null_literal,
    ACTIONS(13), 23,
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
    ACTIONS(35), 1,
      aux_sym_date_literal_with_param_token1,
    ACTIONS(37), 1,
      sym_bound_apex_expression,
    ACTIONS(39), 1,
      aux_sym_null_literal_token1,
    STATE(6), 1,
      sym_formatting_comment,
    STATE(516), 1,
      sym__soql_literal,
    ACTIONS(31), 2,
      aux_sym_boolean_token1,
      aux_sym_boolean_token2,
    ACTIONS(41), 2,
      sym_string_literal,
      sym_date_time,
    ACTIONS(43), 4,
      sym_int,
      sym_decimal,
      sym_date,
      sym_currency_literal,
    STATE(518), 4,
      sym_boolean,
      sym_date_literal,
      sym_date_literal_with_param,
      sym_null_literal,
    ACTIONS(33), 23,
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
    ACTIONS(15), 1,
      aux_sym_date_literal_with_param_token1,
    ACTIONS(19), 1,
      aux_sym_null_literal_token1,
    STATE(7), 1,
      sym_formatting_comment,
    STATE(103), 1,
      sym__soql_literal,
    ACTIONS(11), 2,
      aux_sym_boolean_token1,
      aux_sym_boolean_token2,
    ACTIONS(21), 2,
      sym_string_literal,
      sym_date_time,
    ACTIONS(23), 4,
      sym_int,
      sym_decimal,
      sym_date,
      sym_currency_literal,
    STATE(45), 4,
      sym_boolean,
      sym_date_literal,
      sym_date_literal_with_param,
      sym_null_literal,
    ACTIONS(13), 23,
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
    ACTIONS(47), 4,
      sym_int,
      sym_decimal,
      sym_date,
      sym_currency_literal,
    ACTIONS(45), 30,
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
    ACTIONS(53), 1,
      anon_sym_DOT,
    STATE(9), 1,
      sym_formatting_comment,
    STATE(10), 1,
      aux_sym_dotted_identifier_repeat1,
    ACTIONS(49), 3,
      aux_sym_in_clause_token1,
      anon_sym_LT,
      anon_sym_GT,
    ACTIONS(51), 28,
      anon_sym_COMMA,
      anon_sym_RPAREN,
      anon_sym_EQ,
      aux_sym_sosl_with_clause_token1,
      aux_sym_using_lookup_bind_clause_token1,
      aux_sym_type_of_clause_token2,
      aux_sym_when_expression_token1,
      aux_sym_else_expression_token1,
      aux_sym_group_by_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_having_clause_token1,
      aux_sym_where_clause_token1,
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
      aux_sym_set_comparison_operator_token1,
      aux_sym_set_comparison_operator_token2,
      aux_sym_set_comparison_operator_token3,
  [492] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(59), 1,
      anon_sym_DOT,
    STATE(10), 2,
      sym_formatting_comment,
      aux_sym_dotted_identifier_repeat1,
    ACTIONS(55), 3,
      aux_sym_in_clause_token1,
      anon_sym_LT,
      anon_sym_GT,
    ACTIONS(57), 28,
      anon_sym_COMMA,
      anon_sym_RPAREN,
      anon_sym_EQ,
      aux_sym_sosl_with_clause_token1,
      aux_sym_using_lookup_bind_clause_token1,
      aux_sym_type_of_clause_token2,
      aux_sym_when_expression_token1,
      aux_sym_else_expression_token1,
      aux_sym_group_by_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_having_clause_token1,
      aux_sym_where_clause_token1,
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
      aux_sym_set_comparison_operator_token1,
      aux_sym_set_comparison_operator_token2,
      aux_sym_set_comparison_operator_token3,
  [538] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(11), 1,
      sym_formatting_comment,
    ACTIONS(55), 3,
      aux_sym_in_clause_token1,
      anon_sym_LT,
      anon_sym_GT,
    ACTIONS(57), 29,
      anon_sym_COMMA,
      anon_sym_RPAREN,
      anon_sym_EQ,
      aux_sym_sosl_with_clause_token1,
      aux_sym_using_lookup_bind_clause_token1,
      aux_sym_type_of_clause_token2,
      aux_sym_when_expression_token1,
      aux_sym_else_expression_token1,
      aux_sym_group_by_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_having_clause_token1,
      aux_sym_where_clause_token1,
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
      aux_sym_set_comparison_operator_token1,
      aux_sym_set_comparison_operator_token2,
      aux_sym_set_comparison_operator_token3,
  [581] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(53), 1,
      anon_sym_DOT,
    ACTIONS(66), 1,
      anon_sym_LPAREN,
    STATE(9), 1,
      aux_sym_dotted_identifier_repeat1,
    STATE(12), 1,
      sym_formatting_comment,
    ACTIONS(62), 3,
      aux_sym_in_clause_token1,
      anon_sym_LT,
      anon_sym_GT,
    ACTIONS(64), 21,
      anon_sym_COMMA,
      anon_sym_RPAREN,
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
      aux_sym_set_comparison_operator_token1,
      aux_sym_set_comparison_operator_token2,
      aux_sym_set_comparison_operator_token3,
  [625] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(13), 1,
      sym_formatting_comment,
    ACTIONS(62), 3,
      aux_sym_in_clause_token1,
      anon_sym_LT,
      anon_sym_GT,
    ACTIONS(64), 21,
      anon_sym_COMMA,
      anon_sym_RPAREN,
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
      aux_sym_set_comparison_operator_token1,
      aux_sym_set_comparison_operator_token2,
      aux_sym_set_comparison_operator_token3,
  [660] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(14), 1,
      sym_formatting_comment,
    ACTIONS(68), 3,
      aux_sym_in_clause_token1,
      anon_sym_LT,
      anon_sym_GT,
    ACTIONS(70), 21,
      anon_sym_COMMA,
      anon_sym_RPAREN,
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
      aux_sym_set_comparison_operator_token1,
      aux_sym_set_comparison_operator_token2,
      aux_sym_set_comparison_operator_token3,
  [695] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(15), 1,
      sym_formatting_comment,
    ACTIONS(72), 3,
      aux_sym_in_clause_token1,
      anon_sym_LT,
      anon_sym_GT,
    ACTIONS(74), 21,
      anon_sym_COMMA,
      anon_sym_RPAREN,
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
      aux_sym_set_comparison_operator_token1,
      aux_sym_set_comparison_operator_token2,
      aux_sym_set_comparison_operator_token3,
  [730] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(16), 1,
      sym_formatting_comment,
    ACTIONS(76), 3,
      aux_sym_in_clause_token1,
      anon_sym_LT,
      anon_sym_GT,
    ACTIONS(78), 21,
      anon_sym_COMMA,
      anon_sym_RPAREN,
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
      aux_sym_set_comparison_operator_token1,
      aux_sym_set_comparison_operator_token2,
      aux_sym_set_comparison_operator_token3,
  [765] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(17), 1,
      sym_formatting_comment,
    ACTIONS(80), 3,
      aux_sym_in_clause_token1,
      anon_sym_LT,
      anon_sym_GT,
    ACTIONS(82), 19,
      anon_sym_COMMA,
      anon_sym_RPAREN,
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
      aux_sym_set_comparison_operator_token1,
      aux_sym_set_comparison_operator_token2,
      aux_sym_set_comparison_operator_token3,
  [798] = 21,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(84), 1,
      anon_sym_RPAREN,
    ACTIONS(86), 1,
      aux_sym_sosl_using_clause_token1,
    ACTIONS(88), 1,
      aux_sym_sosl_with_clause_token1,
    ACTIONS(90), 1,
      aux_sym_group_by_clause_token1,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(96), 1,
      aux_sym_where_clause_token1,
    ACTIONS(98), 1,
      aux_sym_limit_clause_token1,
    ACTIONS(100), 1,
      aux_sym_offset_clause_token1,
    ACTIONS(102), 1,
      aux_sym_order_by_clause_token1,
    STATE(18), 1,
      sym_formatting_comment,
    STATE(22), 1,
      sym_using_clause,
    STATE(39), 1,
      sym_where_clause,
    STATE(53), 1,
      sym_soql_with_clause,
    STATE(85), 1,
      sym_group_by_clause,
    STATE(120), 1,
      sym_order_by_clause,
    STATE(195), 1,
      sym_limit_clause,
    STATE(261), 1,
      sym_offset_clause,
    STATE(555), 1,
      sym_for_clause,
    STATE(649), 1,
      sym_update_clause,
  [862] = 19,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(98), 1,
      aux_sym_limit_clause_token1,
    ACTIONS(100), 1,
      aux_sym_offset_clause_token1,
    ACTIONS(104), 1,
      ts_builtin_sym_end,
    ACTIONS(106), 1,
      aux_sym_in_clause_token1,
    ACTIONS(108), 1,
      aux_sym_returning_clause_token1,
    ACTIONS(110), 1,
      aux_sym_sosl_using_clause_token1,
    ACTIONS(112), 1,
      aux_sym_sosl_with_clause_token1,
    STATE(19), 1,
      sym_formatting_comment,
    STATE(29), 1,
      aux_sym_sosl_query_body_repeat1,
    STATE(41), 1,
      sym_in_clause,
    STATE(57), 1,
      aux_sym_sosl_query_body_repeat2,
    STATE(191), 1,
      sym_returning_clause,
    STATE(208), 1,
      sym_sosl_using_clause,
    STATE(255), 1,
      sym_sosl_with_clause,
    STATE(268), 1,
      sym_limit_clause,
    STATE(381), 1,
      sym_offset_clause,
    STATE(746), 1,
      sym_update_clause,
  [920] = 10,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(114), 1,
      aux_sym_in_clause_token1,
    STATE(5), 1,
      sym_value_comparison_operator,
    STATE(20), 1,
      sym_formatting_comment,
    STATE(94), 1,
      sym__comparison,
    STATE(347), 1,
      sym_set_comparison_operator,
    ACTIONS(118), 2,
      anon_sym_LT,
      anon_sym_GT,
    STATE(64), 2,
      sym__value_comparison,
      sym__set_comparison,
    ACTIONS(120), 3,
      aux_sym_set_comparison_operator_token1,
      aux_sym_set_comparison_operator_token2,
      aux_sym_set_comparison_operator_token3,
    ACTIONS(116), 6,
      anon_sym_EQ,
      anon_sym_BANG_EQ,
      anon_sym_LT_GT,
      anon_sym_LT_EQ,
      anon_sym_GT_EQ,
      aux_sym_value_comparison_operator_token1,
  [960] = 10,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(114), 1,
      aux_sym_in_clause_token1,
    STATE(5), 1,
      sym_value_comparison_operator,
    STATE(21), 1,
      sym_formatting_comment,
    STATE(238), 1,
      sym__comparison,
    STATE(347), 1,
      sym_set_comparison_operator,
    ACTIONS(118), 2,
      anon_sym_LT,
      anon_sym_GT,
    STATE(64), 2,
      sym__value_comparison,
      sym__set_comparison,
    ACTIONS(120), 3,
      aux_sym_set_comparison_operator_token1,
      aux_sym_set_comparison_operator_token2,
      aux_sym_set_comparison_operator_token3,
    ACTIONS(116), 6,
      anon_sym_EQ,
      anon_sym_BANG_EQ,
      anon_sym_LT_GT,
      anon_sym_LT_EQ,
      anon_sym_GT_EQ,
      aux_sym_value_comparison_operator_token1,
  [1000] = 19,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_sosl_with_clause_token1,
    ACTIONS(90), 1,
      aux_sym_group_by_clause_token1,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(96), 1,
      aux_sym_where_clause_token1,
    ACTIONS(98), 1,
      aux_sym_limit_clause_token1,
    ACTIONS(100), 1,
      aux_sym_offset_clause_token1,
    ACTIONS(102), 1,
      aux_sym_order_by_clause_token1,
    ACTIONS(122), 1,
      anon_sym_RPAREN,
    STATE(22), 1,
      sym_formatting_comment,
    STATE(36), 1,
      sym_where_clause,
    STATE(51), 1,
      sym_soql_with_clause,
    STATE(86), 1,
      sym_group_by_clause,
    STATE(124), 1,
      sym_order_by_clause,
    STATE(179), 1,
      sym_limit_clause,
    STATE(299), 1,
      sym_offset_clause,
    STATE(374), 1,
      sym_for_clause,
    STATE(669), 1,
      sym_update_clause,
  [1058] = 12,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(124), 1,
      aux_sym_with_division_expression_token1,
    ACTIONS(126), 1,
      aux_sym_with_highlight_token1,
    ACTIONS(128), 1,
      aux_sym_with_metadata_expression_token1,
    ACTIONS(130), 1,
      aux_sym_with_network_expression_token1,
    ACTIONS(132), 1,
      aux_sym_with_pricebook_expression_token1,
    ACTIONS(134), 1,
      aux_sym_with_snippet_expression_token1,
    ACTIONS(136), 1,
      aux_sym_with_spell_correction_expression_token1,
    ACTIONS(138), 1,
      aux_sym_with_data_cat_expression_token1,
    STATE(23), 1,
      sym_formatting_comment,
    STATE(234), 1,
      sym_sosl_with_type,
    STATE(251), 8,
      sym_with_division_expression,
      sym_with_highlight,
      sym_with_metadata_expression,
      sym_with_network_expression,
      sym_with_pricebook_expression,
      sym_with_snippet_expression,
      sym_with_spell_correction_expression,
      sym_with_data_cat_expression,
  [1102] = 10,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(114), 1,
      aux_sym_in_clause_token1,
    STATE(6), 1,
      sym_value_comparison_operator,
    STATE(24), 1,
      sym_formatting_comment,
    STATE(362), 1,
      sym_set_comparison_operator,
    STATE(530), 1,
      sym__comparison,
    ACTIONS(118), 2,
      anon_sym_LT,
      anon_sym_GT,
    STATE(509), 2,
      sym__value_comparison,
      sym__set_comparison,
    ACTIONS(120), 3,
      aux_sym_set_comparison_operator_token1,
      aux_sym_set_comparison_operator_token2,
      aux_sym_set_comparison_operator_token3,
    ACTIONS(116), 6,
      anon_sym_EQ,
      anon_sym_BANG_EQ,
      anon_sym_LT_GT,
      anon_sym_LT_EQ,
      anon_sym_GT_EQ,
      aux_sym_value_comparison_operator_token1,
  [1142] = 14,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(140), 1,
      aux_sym_in_clause_token2,
    ACTIONS(142), 1,
      anon_sym_LPAREN,
    ACTIONS(144), 1,
      aux_sym_type_of_clause_token1,
    ACTIONS(146), 1,
      aux_sym_function_expression_token1,
    ACTIONS(148), 1,
      sym_identifier,
    STATE(25), 1,
      sym_formatting_comment,
    STATE(384), 1,
      sym__selectable_expression,
    STATE(511), 1,
      sym__value_expression,
    STATE(513), 1,
      sym_dotted_identifier,
    STATE(919), 1,
      sym_count_expression,
    STATE(927), 1,
      sym__function_name,
    STATE(512), 2,
      sym_function_expression,
      sym_field_identifier,
    STATE(148), 4,
      sym_subquery,
      sym_type_of_clause,
      sym_fields_expression,
      sym_alias_expression,
  [1189] = 14,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(140), 1,
      aux_sym_in_clause_token2,
    ACTIONS(142), 1,
      anon_sym_LPAREN,
    ACTIONS(144), 1,
      aux_sym_type_of_clause_token1,
    ACTIONS(150), 1,
      aux_sym_function_expression_token1,
    ACTIONS(152), 1,
      sym_identifier,
    STATE(26), 1,
      sym_formatting_comment,
    STATE(74), 1,
      sym_selected_fields,
    STATE(142), 1,
      sym_dotted_identifier,
    STATE(146), 1,
      sym__selectable_expression,
    STATE(166), 1,
      sym__value_expression,
    STATE(630), 1,
      sym__function_name,
    STATE(151), 2,
      sym_function_expression,
      sym_field_identifier,
    STATE(148), 4,
      sym_subquery,
      sym_type_of_clause,
      sym_fields_expression,
      sym_alias_expression,
  [1236] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(156), 1,
      aux_sym_or_expression_token1,
    STATE(27), 1,
      sym_formatting_comment,
    ACTIONS(154), 14,
      ts_builtin_sym_end,
      anon_sym_COMMA,
      anon_sym_RPAREN,
      aux_sym_sosl_using_clause_token1,
      aux_sym_sosl_with_clause_token1,
      aux_sym_group_by_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_from_clause_token1,
      aux_sym_where_clause_token1,
      aux_sym_and_expression_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [1262] = 14,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(158), 1,
      anon_sym_LPAREN,
    ACTIONS(160), 1,
      aux_sym_not_expression_token1,
    ACTIONS(162), 1,
      aux_sym_function_expression_token1,
    ACTIONS(164), 1,
      sym_identifier,
    STATE(13), 1,
      sym_dotted_identifier,
    STATE(20), 1,
      sym__value_expression,
    STATE(28), 1,
      sym_formatting_comment,
    STATE(61), 1,
      sym__condition_expression,
    STATE(96), 1,
      sym_comparison_expression,
    STATE(152), 1,
      sym__boolean_expression,
    STATE(922), 1,
      sym__function_name,
    STATE(17), 2,
      sym_function_expression,
      sym_field_identifier,
    STATE(156), 3,
      sym_and_expression,
      sym_or_expression,
      sym_not_expression,
  [1308] = 17,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(98), 1,
      aux_sym_limit_clause_token1,
    ACTIONS(100), 1,
      aux_sym_offset_clause_token1,
    ACTIONS(108), 1,
      aux_sym_returning_clause_token1,
    ACTIONS(110), 1,
      aux_sym_sosl_using_clause_token1,
    ACTIONS(112), 1,
      aux_sym_sosl_with_clause_token1,
    ACTIONS(166), 1,
      ts_builtin_sym_end,
    STATE(29), 1,
      sym_formatting_comment,
    STATE(62), 1,
      aux_sym_sosl_query_body_repeat2,
    STATE(137), 1,
      aux_sym_sosl_query_body_repeat1,
    STATE(189), 1,
      sym_sosl_using_clause,
    STATE(191), 1,
      sym_returning_clause,
    STATE(255), 1,
      sym_sosl_with_clause,
    STATE(338), 1,
      sym_limit_clause,
    STATE(558), 1,
      sym_offset_clause,
    STATE(672), 1,
      sym_update_clause,
  [1360] = 14,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(160), 1,
      aux_sym_not_expression_token1,
    ACTIONS(162), 1,
      aux_sym_function_expression_token1,
    ACTIONS(164), 1,
      sym_identifier,
    ACTIONS(168), 1,
      anon_sym_LPAREN,
    STATE(13), 1,
      sym_dotted_identifier,
    STATE(24), 1,
      sym__value_expression,
    STATE(30), 1,
      sym_formatting_comment,
    STATE(333), 1,
      sym__condition_expression,
    STATE(525), 1,
      sym_comparison_expression,
    STATE(906), 1,
      sym__boolean_expression,
    STATE(922), 1,
      sym__function_name,
    STATE(17), 2,
      sym_function_expression,
      sym_field_identifier,
    STATE(156), 3,
      sym_and_expression,
      sym_or_expression,
      sym_not_expression,
  [1406] = 14,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(160), 1,
      aux_sym_not_expression_token1,
    ACTIONS(162), 1,
      aux_sym_function_expression_token1,
    ACTIONS(164), 1,
      sym_identifier,
    ACTIONS(168), 1,
      anon_sym_LPAREN,
    STATE(13), 1,
      sym_dotted_identifier,
    STATE(24), 1,
      sym__value_expression,
    STATE(31), 1,
      sym_formatting_comment,
    STATE(333), 1,
      sym__condition_expression,
    STATE(525), 1,
      sym_comparison_expression,
    STATE(734), 1,
      sym__boolean_expression,
    STATE(922), 1,
      sym__function_name,
    STATE(17), 2,
      sym_function_expression,
      sym_field_identifier,
    STATE(156), 3,
      sym_and_expression,
      sym_or_expression,
      sym_not_expression,
  [1452] = 13,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(140), 1,
      aux_sym_in_clause_token2,
    ACTIONS(142), 1,
      anon_sym_LPAREN,
    ACTIONS(144), 1,
      aux_sym_type_of_clause_token1,
    ACTIONS(150), 1,
      aux_sym_function_expression_token1,
    ACTIONS(152), 1,
      sym_identifier,
    STATE(32), 1,
      sym_formatting_comment,
    STATE(142), 1,
      sym_dotted_identifier,
    STATE(166), 1,
      sym__value_expression,
    STATE(171), 1,
      sym__selectable_expression,
    STATE(630), 1,
      sym__function_name,
    STATE(151), 2,
      sym_function_expression,
      sym_field_identifier,
    STATE(148), 4,
      sym_subquery,
      sym_type_of_clause,
      sym_fields_expression,
      sym_alias_expression,
  [1496] = 6,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(174), 1,
      anon_sym_DOT,
    STATE(33), 1,
      sym_formatting_comment,
    STATE(40), 1,
      aux_sym_dotted_identifier_repeat1,
    ACTIONS(170), 4,
      anon_sym_COMMA,
      anon_sym_RPAREN,
      aux_sym_group_by_clause_token1,
      aux_sym_order_by_clause_token1,
    ACTIONS(172), 9,
      aux_sym_sosl_using_clause_token1,
      aux_sym_sosl_with_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_storage_alias_token1,
      aux_sym_where_clause_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      sym_identifier,
  [1526] = 14,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(158), 1,
      anon_sym_LPAREN,
    ACTIONS(160), 1,
      aux_sym_not_expression_token1,
    ACTIONS(162), 1,
      aux_sym_function_expression_token1,
    ACTIONS(164), 1,
      sym_identifier,
    STATE(13), 1,
      sym_dotted_identifier,
    STATE(20), 1,
      sym__value_expression,
    STATE(34), 1,
      sym_formatting_comment,
    STATE(61), 1,
      sym__condition_expression,
    STATE(96), 1,
      sym_comparison_expression,
    STATE(244), 1,
      sym__boolean_expression,
    STATE(922), 1,
      sym__function_name,
    STATE(17), 2,
      sym_function_expression,
      sym_field_identifier,
    STATE(156), 3,
      sym_and_expression,
      sym_or_expression,
      sym_not_expression,
  [1572] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(176), 1,
      anon_sym_DOT,
    STATE(35), 2,
      sym_formatting_comment,
      aux_sym_dotted_identifier_repeat1,
    ACTIONS(57), 4,
      anon_sym_COMMA,
      anon_sym_RPAREN,
      aux_sym_group_by_clause_token1,
      aux_sym_order_by_clause_token1,
    ACTIONS(55), 9,
      aux_sym_sosl_using_clause_token1,
      aux_sym_sosl_with_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_storage_alias_token1,
      aux_sym_where_clause_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      sym_identifier,
  [1600] = 17,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_sosl_with_clause_token1,
    ACTIONS(90), 1,
      aux_sym_group_by_clause_token1,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(98), 1,
      aux_sym_limit_clause_token1,
    ACTIONS(100), 1,
      aux_sym_offset_clause_token1,
    ACTIONS(102), 1,
      aux_sym_order_by_clause_token1,
    ACTIONS(179), 1,
      anon_sym_RPAREN,
    STATE(36), 1,
      sym_formatting_comment,
    STATE(54), 1,
      sym_soql_with_clause,
    STATE(76), 1,
      sym_group_by_clause,
    STATE(121), 1,
      sym_order_by_clause,
    STATE(181), 1,
      sym_limit_clause,
    STATE(266), 1,
      sym_offset_clause,
    STATE(545), 1,
      sym_for_clause,
    STATE(685), 1,
      sym_update_clause,
  [1652] = 13,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(140), 1,
      aux_sym_in_clause_token2,
    ACTIONS(142), 1,
      anon_sym_LPAREN,
    ACTIONS(144), 1,
      aux_sym_type_of_clause_token1,
    ACTIONS(146), 1,
      aux_sym_function_expression_token1,
    ACTIONS(148), 1,
      sym_identifier,
    STATE(37), 1,
      sym_formatting_comment,
    STATE(171), 1,
      sym__selectable_expression,
    STATE(511), 1,
      sym__value_expression,
    STATE(513), 1,
      sym_dotted_identifier,
    STATE(924), 1,
      sym__function_name,
    STATE(512), 2,
      sym_function_expression,
      sym_field_identifier,
    STATE(148), 4,
      sym_subquery,
      sym_type_of_clause,
      sym_fields_expression,
      sym_alias_expression,
  [1696] = 17,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(98), 1,
      aux_sym_limit_clause_token1,
    ACTIONS(100), 1,
      aux_sym_offset_clause_token1,
    ACTIONS(108), 1,
      aux_sym_returning_clause_token1,
    ACTIONS(110), 1,
      aux_sym_sosl_using_clause_token1,
    ACTIONS(112), 1,
      aux_sym_sosl_with_clause_token1,
    ACTIONS(181), 1,
      ts_builtin_sym_end,
    STATE(38), 1,
      sym_formatting_comment,
    STATE(70), 1,
      aux_sym_sosl_query_body_repeat2,
    STATE(137), 1,
      aux_sym_sosl_query_body_repeat1,
    STATE(191), 1,
      sym_returning_clause,
    STATE(221), 1,
      sym_sosl_using_clause,
    STATE(255), 1,
      sym_sosl_with_clause,
    STATE(323), 1,
      sym_limit_clause,
    STATE(542), 1,
      sym_offset_clause,
    STATE(737), 1,
      sym_update_clause,
  [1748] = 17,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(88), 1,
      aux_sym_sosl_with_clause_token1,
    ACTIONS(90), 1,
      aux_sym_group_by_clause_token1,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(98), 1,
      aux_sym_limit_clause_token1,
    ACTIONS(100), 1,
      aux_sym_offset_clause_token1,
    ACTIONS(102), 1,
      aux_sym_order_by_clause_token1,
    ACTIONS(183), 1,
      anon_sym_RPAREN,
    STATE(39), 1,
      sym_formatting_comment,
    STATE(49), 1,
      sym_soql_with_clause,
    STATE(90), 1,
      sym_group_by_clause,
    STATE(109), 1,
      sym_order_by_clause,
    STATE(202), 1,
      sym_limit_clause,
    STATE(314), 1,
      sym_offset_clause,
    STATE(428), 1,
      sym_for_clause,
    STATE(718), 1,
      sym_update_clause,
  [1800] = 6,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(174), 1,
      anon_sym_DOT,
    STATE(35), 1,
      aux_sym_dotted_identifier_repeat1,
    STATE(40), 1,
      sym_formatting_comment,
    ACTIONS(51), 4,
      anon_sym_COMMA,
      anon_sym_RPAREN,
      aux_sym_group_by_clause_token1,
      aux_sym_order_by_clause_token1,
    ACTIONS(49), 9,
      aux_sym_sosl_using_clause_token1,
      aux_sym_sosl_with_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_storage_alias_token1,
      aux_sym_where_clause_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      sym_identifier,
  [1830] = 17,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(98), 1,
      aux_sym_limit_clause_token1,
    ACTIONS(100), 1,
      aux_sym_offset_clause_token1,
    ACTIONS(108), 1,
      aux_sym_returning_clause_token1,
    ACTIONS(110), 1,
      aux_sym_sosl_using_clause_token1,
    ACTIONS(112), 1,
      aux_sym_sosl_with_clause_token1,
    ACTIONS(166), 1,
      ts_builtin_sym_end,
    STATE(38), 1,
      aux_sym_sosl_query_body_repeat1,
    STATE(41), 1,
      sym_formatting_comment,
    STATE(62), 1,
      aux_sym_sosl_query_body_repeat2,
    STATE(189), 1,
      sym_sosl_using_clause,
    STATE(191), 1,
      sym_returning_clause,
    STATE(255), 1,
      sym_sosl_with_clause,
    STATE(338), 1,
      sym_limit_clause,
    STATE(558), 1,
      sym_offset_clause,
    STATE(672), 1,
      sym_update_clause,
  [1882] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(187), 1,
      aux_sym_or_expression_token1,
    STATE(42), 1,
      sym_formatting_comment,
    ACTIONS(185), 13,
      ts_builtin_sym_end,
      anon_sym_COMMA,
      anon_sym_RPAREN,
      aux_sym_sosl_using_clause_token1,
      aux_sym_sosl_with_clause_token1,
      aux_sym_group_by_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_where_clause_token1,
      aux_sym_and_expression_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [1907] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(191), 1,
      aux_sym_or_expression_token1,
    STATE(43), 1,
      sym_formatting_comment,
    ACTIONS(189), 13,
      ts_builtin_sym_end,
      anon_sym_COMMA,
      anon_sym_RPAREN,
      aux_sym_sosl_using_clause_token1,
      aux_sym_sosl_with_clause_token1,
      aux_sym_group_by_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_where_clause_token1,
      aux_sym_and_expression_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [1932] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(195), 1,
      aux_sym_or_expression_token1,
    STATE(44), 1,
      sym_formatting_comment,
    ACTIONS(193), 13,
      ts_builtin_sym_end,
      anon_sym_COMMA,
      anon_sym_RPAREN,
      aux_sym_sosl_using_clause_token1,
      aux_sym_sosl_with_clause_token1,
      aux_sym_group_by_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_where_clause_token1,
      aux_sym_and_expression_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [1957] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(199), 1,
      aux_sym_or_expression_token1,
    STATE(45), 1,
      sym_formatting_comment,
    ACTIONS(197), 13,
      ts_builtin_sym_end,
      anon_sym_COMMA,
      anon_sym_RPAREN,
      aux_sym_sosl_using_clause_token1,
      aux_sym_sosl_with_clause_token1,
      aux_sym_group_by_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_where_clause_token1,
      aux_sym_and_expression_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [1982] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(203), 1,
      aux_sym_or_expression_token1,
    STATE(46), 1,
      sym_formatting_comment,
    ACTIONS(201), 13,
      ts_builtin_sym_end,
      anon_sym_COMMA,
      anon_sym_RPAREN,
      aux_sym_sosl_using_clause_token1,
      aux_sym_sosl_with_clause_token1,
      aux_sym_group_by_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_where_clause_token1,
      aux_sym_and_expression_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [2007] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(47), 1,
      sym_formatting_comment,
    ACTIONS(57), 5,
      anon_sym_COMMA,
      anon_sym_RPAREN,
      aux_sym_group_by_clause_token1,
      aux_sym_order_by_clause_token1,
      anon_sym_DOT,
    ACTIONS(55), 9,
      aux_sym_sosl_using_clause_token1,
      aux_sym_sosl_with_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_storage_alias_token1,
      aux_sym_where_clause_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      sym_identifier,
  [2032] = 8,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(205), 1,
      anon_sym_COMMA,
    ACTIONS(211), 1,
      aux_sym_storage_alias_token1,
    ACTIONS(213), 1,
      sym_identifier,
    STATE(48), 1,
      sym_formatting_comment,
    STATE(56), 1,
      aux_sym_from_clause_repeat1,
    ACTIONS(207), 3,
      anon_sym_RPAREN,
      aux_sym_group_by_clause_token1,
      aux_sym_order_by_clause_token1,
    ACTIONS(209), 7,
      aux_sym_sosl_using_clause_token1,
      aux_sym_sosl_with_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_where_clause_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
  [2065] = 15,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_group_by_clause_token1,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(98), 1,
      aux_sym_limit_clause_token1,
    ACTIONS(100), 1,
      aux_sym_offset_clause_token1,
    ACTIONS(102), 1,
      aux_sym_order_by_clause_token1,
    ACTIONS(215), 1,
      anon_sym_RPAREN,
    STATE(49), 1,
      sym_formatting_comment,
    STATE(78), 1,
      sym_group_by_clause,
    STATE(127), 1,
      sym_order_by_clause,
    STATE(190), 1,
      sym_limit_clause,
    STATE(273), 1,
      sym_offset_clause,
    STATE(377), 1,
      sym_for_clause,
    STATE(863), 1,
      sym_update_clause,
  [2111] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(50), 1,
      sym_formatting_comment,
    ACTIONS(170), 4,
      anon_sym_COMMA,
      anon_sym_RPAREN,
      aux_sym_group_by_clause_token1,
      aux_sym_order_by_clause_token1,
    ACTIONS(172), 9,
      aux_sym_sosl_using_clause_token1,
      aux_sym_sosl_with_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_storage_alias_token1,
      aux_sym_where_clause_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      sym_identifier,
  [2135] = 15,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_group_by_clause_token1,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(98), 1,
      aux_sym_limit_clause_token1,
    ACTIONS(100), 1,
      aux_sym_offset_clause_token1,
    ACTIONS(102), 1,
      aux_sym_order_by_clause_token1,
    ACTIONS(217), 1,
      anon_sym_RPAREN,
    STATE(51), 1,
      sym_formatting_comment,
    STATE(77), 1,
      sym_group_by_clause,
    STATE(123), 1,
      sym_order_by_clause,
    STATE(183), 1,
      sym_limit_clause,
    STATE(267), 1,
      sym_offset_clause,
    STATE(406), 1,
      sym_for_clause,
    STATE(711), 1,
      sym_update_clause,
  [2181] = 6,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(211), 1,
      aux_sym_storage_alias_token1,
    ACTIONS(213), 1,
      sym_identifier,
    STATE(52), 1,
      sym_formatting_comment,
    ACTIONS(219), 4,
      anon_sym_COMMA,
      anon_sym_RPAREN,
      aux_sym_group_by_clause_token1,
      aux_sym_order_by_clause_token1,
    ACTIONS(221), 7,
      aux_sym_sosl_using_clause_token1,
      aux_sym_sosl_with_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_where_clause_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
  [2209] = 15,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_group_by_clause_token1,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(98), 1,
      aux_sym_limit_clause_token1,
    ACTIONS(100), 1,
      aux_sym_offset_clause_token1,
    ACTIONS(102), 1,
      aux_sym_order_by_clause_token1,
    ACTIONS(223), 1,
      anon_sym_RPAREN,
    STATE(53), 1,
      sym_formatting_comment,
    STATE(91), 1,
      sym_group_by_clause,
    STATE(110), 1,
      sym_order_by_clause,
    STATE(211), 1,
      sym_limit_clause,
    STATE(322), 1,
      sym_offset_clause,
    STATE(468), 1,
      sym_for_clause,
    STATE(755), 1,
      sym_update_clause,
  [2255] = 15,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(90), 1,
      aux_sym_group_by_clause_token1,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(98), 1,
      aux_sym_limit_clause_token1,
    ACTIONS(100), 1,
      aux_sym_offset_clause_token1,
    ACTIONS(102), 1,
      aux_sym_order_by_clause_token1,
    ACTIONS(225), 1,
      anon_sym_RPAREN,
    STATE(54), 1,
      sym_formatting_comment,
    STATE(80), 1,
      sym_group_by_clause,
    STATE(131), 1,
      sym_order_by_clause,
    STATE(201), 1,
      sym_limit_clause,
    STATE(286), 1,
      sym_offset_clause,
    STATE(539), 1,
      sym_for_clause,
    STATE(689), 1,
      sym_update_clause,
  [2301] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(229), 1,
      aux_sym_or_expression_token1,
    STATE(55), 1,
      sym_formatting_comment,
    ACTIONS(227), 11,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
      aux_sym_sosl_using_clause_token1,
      aux_sym_sosl_with_clause_token1,
      aux_sym_group_by_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_and_expression_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [2324] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(205), 1,
      anon_sym_COMMA,
    STATE(56), 1,
      sym_formatting_comment,
    STATE(60), 1,
      aux_sym_from_clause_repeat1,
    ACTIONS(231), 10,
      anon_sym_RPAREN,
      aux_sym_sosl_using_clause_token1,
      aux_sym_sosl_with_clause_token1,
      aux_sym_group_by_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_where_clause_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [2349] = 14,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(98), 1,
      aux_sym_limit_clause_token1,
    ACTIONS(100), 1,
      aux_sym_offset_clause_token1,
    ACTIONS(110), 1,
      aux_sym_sosl_using_clause_token1,
    ACTIONS(112), 1,
      aux_sym_sosl_with_clause_token1,
    ACTIONS(166), 1,
      ts_builtin_sym_end,
    STATE(57), 1,
      sym_formatting_comment,
    STATE(163), 1,
      aux_sym_sosl_query_body_repeat2,
    STATE(189), 1,
      sym_sosl_using_clause,
    STATE(255), 1,
      sym_sosl_with_clause,
    STATE(338), 1,
      sym_limit_clause,
    STATE(558), 1,
      sym_offset_clause,
    STATE(672), 1,
      sym_update_clause,
  [2392] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(235), 1,
      aux_sym_and_expression_token1,
    STATE(58), 2,
      sym_formatting_comment,
      aux_sym_with_data_cat_expression_repeat1,
    ACTIONS(233), 10,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
      aux_sym_sosl_using_clause_token1,
      aux_sym_sosl_with_clause_token1,
      aux_sym_group_by_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [2415] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(240), 1,
      aux_sym_or_expression_token1,
    STATE(59), 1,
      sym_formatting_comment,
    ACTIONS(238), 11,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
      aux_sym_sosl_using_clause_token1,
      aux_sym_sosl_with_clause_token1,
      aux_sym_group_by_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_and_expression_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [2438] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(242), 1,
      anon_sym_COMMA,
    STATE(60), 2,
      sym_formatting_comment,
      aux_sym_from_clause_repeat1,
    ACTIONS(219), 10,
      anon_sym_RPAREN,
      aux_sym_sosl_using_clause_token1,
      aux_sym_sosl_with_clause_token1,
      aux_sym_group_by_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_where_clause_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [2461] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(247), 1,
      aux_sym_and_expression_token1,
    ACTIONS(249), 1,
      aux_sym_or_expression_token1,
    STATE(61), 1,
      sym_formatting_comment,
    STATE(99), 1,
      aux_sym_and_expression_repeat1,
    STATE(107), 1,
      aux_sym_or_expression_repeat1,
    ACTIONS(245), 8,
      anon_sym_RPAREN,
      aux_sym_sosl_with_clause_token1,
      aux_sym_group_by_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [2490] = 14,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(98), 1,
      aux_sym_limit_clause_token1,
    ACTIONS(100), 1,
      aux_sym_offset_clause_token1,
    ACTIONS(110), 1,
      aux_sym_sosl_using_clause_token1,
    ACTIONS(112), 1,
      aux_sym_sosl_with_clause_token1,
    ACTIONS(181), 1,
      ts_builtin_sym_end,
    STATE(62), 1,
      sym_formatting_comment,
    STATE(163), 1,
      aux_sym_sosl_query_body_repeat2,
    STATE(221), 1,
      sym_sosl_using_clause,
    STATE(255), 1,
      sym_sosl_with_clause,
    STATE(323), 1,
      sym_limit_clause,
    STATE(542), 1,
      sym_offset_clause,
    STATE(737), 1,
      sym_update_clause,
  [2533] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(253), 1,
      aux_sym_or_expression_token1,
    STATE(63), 1,
      sym_formatting_comment,
    ACTIONS(251), 11,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
      aux_sym_sosl_using_clause_token1,
      aux_sym_sosl_with_clause_token1,
      aux_sym_group_by_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_and_expression_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [2556] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(257), 1,
      aux_sym_or_expression_token1,
    STATE(64), 1,
      sym_formatting_comment,
    ACTIONS(255), 11,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
      aux_sym_sosl_using_clause_token1,
      aux_sym_sosl_with_clause_token1,
      aux_sym_group_by_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_and_expression_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [2579] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(65), 1,
      sym_formatting_comment,
    STATE(136), 1,
      sym_order_direction,
    STATE(248), 1,
      sym_order_null_direction,
    ACTIONS(261), 2,
      aux_sym_order_direction_token1,
      aux_sym_order_direction_token2,
    ACTIONS(263), 2,
      aux_sym_order_null_direction_token1,
      aux_sym_order_null_direction_token2,
    ACTIONS(259), 6,
      anon_sym_COMMA,
      anon_sym_RPAREN,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
  [2608] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(267), 1,
      aux_sym_and_expression_token1,
    STATE(58), 1,
      aux_sym_with_data_cat_expression_repeat1,
    STATE(66), 1,
      sym_formatting_comment,
    ACTIONS(265), 10,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
      aux_sym_sosl_using_clause_token1,
      aux_sym_sosl_with_clause_token1,
      aux_sym_group_by_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [2633] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(271), 1,
      aux_sym_or_expression_token1,
    STATE(67), 1,
      sym_formatting_comment,
    ACTIONS(269), 11,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
      aux_sym_sosl_using_clause_token1,
      aux_sym_sosl_with_clause_token1,
      aux_sym_group_by_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_and_expression_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [2656] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(267), 1,
      aux_sym_and_expression_token1,
    STATE(66), 1,
      aux_sym_with_data_cat_expression_repeat1,
    STATE(68), 1,
      sym_formatting_comment,
    ACTIONS(273), 10,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
      aux_sym_sosl_using_clause_token1,
      aux_sym_sosl_with_clause_token1,
      aux_sym_group_by_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [2681] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(205), 1,
      anon_sym_COMMA,
    STATE(56), 1,
      aux_sym_from_clause_repeat1,
    STATE(69), 1,
      sym_formatting_comment,
    ACTIONS(207), 10,
      anon_sym_RPAREN,
      aux_sym_sosl_using_clause_token1,
      aux_sym_sosl_with_clause_token1,
      aux_sym_group_by_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_where_clause_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [2706] = 14,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(98), 1,
      aux_sym_limit_clause_token1,
    ACTIONS(100), 1,
      aux_sym_offset_clause_token1,
    ACTIONS(110), 1,
      aux_sym_sosl_using_clause_token1,
    ACTIONS(112), 1,
      aux_sym_sosl_with_clause_token1,
    ACTIONS(275), 1,
      ts_builtin_sym_end,
    STATE(70), 1,
      sym_formatting_comment,
    STATE(163), 1,
      aux_sym_sosl_query_body_repeat2,
    STATE(182), 1,
      sym_sosl_using_clause,
    STATE(255), 1,
      sym_sosl_with_clause,
    STATE(295), 1,
      sym_limit_clause,
    STATE(370), 1,
      sym_offset_clause,
    STATE(917), 1,
      sym_update_clause,
  [2749] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(71), 1,
      sym_formatting_comment,
    ACTIONS(219), 11,
      anon_sym_COMMA,
      anon_sym_RPAREN,
      aux_sym_sosl_using_clause_token1,
      aux_sym_sosl_with_clause_token1,
      aux_sym_group_by_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_where_clause_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [2769] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(66), 1,
      anon_sym_LPAREN,
    ACTIONS(277), 1,
      anon_sym_DOT,
    STATE(72), 1,
      sym_formatting_comment,
    STATE(101), 1,
      aux_sym_dotted_identifier_repeat1,
    ACTIONS(64), 3,
      anon_sym_COMMA,
      anon_sym_RPAREN,
      aux_sym_order_by_clause_token1,
    ACTIONS(62), 5,
      aux_sym_sosl_using_clause_token1,
      aux_sym_where_clause_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      sym_identifier,
  [2797] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(73), 1,
      sym_formatting_comment,
    ACTIONS(279), 11,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
      aux_sym_sosl_using_clause_token1,
      aux_sym_sosl_with_clause_token1,
      aux_sym_group_by_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_and_expression_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [2817] = 13,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(86), 1,
      aux_sym_sosl_using_clause_token1,
    ACTIONS(96), 1,
      aux_sym_where_clause_token1,
    ACTIONS(98), 1,
      aux_sym_limit_clause_token1,
    ACTIONS(100), 1,
      aux_sym_offset_clause_token1,
    ACTIONS(102), 1,
      aux_sym_order_by_clause_token1,
    ACTIONS(281), 1,
      anon_sym_RPAREN,
    STATE(74), 1,
      sym_formatting_comment,
    STATE(135), 1,
      sym_using_clause,
    STATE(212), 1,
      sym_where_clause,
    STATE(339), 1,
      sym_order_by_clause,
    STATE(554), 1,
      sym_limit_clause,
    STATE(721), 1,
      sym_offset_clause,
  [2857] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(75), 1,
      sym_formatting_comment,
    ACTIONS(283), 11,
      anon_sym_COMMA,
      anon_sym_RPAREN,
      aux_sym_sosl_using_clause_token1,
      aux_sym_sosl_with_clause_token1,
      aux_sym_group_by_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_where_clause_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [2877] = 13,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(98), 1,
      aux_sym_limit_clause_token1,
    ACTIONS(100), 1,
      aux_sym_offset_clause_token1,
    ACTIONS(102), 1,
      aux_sym_order_by_clause_token1,
    ACTIONS(285), 1,
      anon_sym_RPAREN,
    STATE(76), 1,
      sym_formatting_comment,
    STATE(130), 1,
      sym_order_by_clause,
    STATE(199), 1,
      sym_limit_clause,
    STATE(284), 1,
      sym_offset_clause,
    STATE(510), 1,
      sym_for_clause,
    STATE(675), 1,
      sym_update_clause,
  [2917] = 13,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(98), 1,
      aux_sym_limit_clause_token1,
    ACTIONS(100), 1,
      aux_sym_offset_clause_token1,
    ACTIONS(102), 1,
      aux_sym_order_by_clause_token1,
    ACTIONS(287), 1,
      anon_sym_RPAREN,
    STATE(77), 1,
      sym_formatting_comment,
    STATE(132), 1,
      sym_order_by_clause,
    STATE(204), 1,
      sym_limit_clause,
    STATE(289), 1,
      sym_offset_clause,
    STATE(560), 1,
      sym_for_clause,
    STATE(720), 1,
      sym_update_clause,
  [2957] = 13,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(98), 1,
      aux_sym_limit_clause_token1,
    ACTIONS(100), 1,
      aux_sym_offset_clause_token1,
    ACTIONS(102), 1,
      aux_sym_order_by_clause_token1,
    ACTIONS(289), 1,
      anon_sym_RPAREN,
    STATE(78), 1,
      sym_formatting_comment,
    STATE(133), 1,
      sym_order_by_clause,
    STATE(207), 1,
      sym_limit_clause,
    STATE(340), 1,
      sym_offset_clause,
    STATE(389), 1,
      sym_for_clause,
    STATE(859), 1,
      sym_update_clause,
  [2997] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(291), 1,
      anon_sym_COMMA,
    STATE(79), 1,
      sym_formatting_comment,
    STATE(81), 1,
      aux_sym_using_lookup_bind_clause_repeat1,
    ACTIONS(293), 9,
      anon_sym_RPAREN,
      aux_sym_sosl_with_clause_token1,
      aux_sym_group_by_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_where_clause_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [3021] = 13,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(98), 1,
      aux_sym_limit_clause_token1,
    ACTIONS(100), 1,
      aux_sym_offset_clause_token1,
    ACTIONS(102), 1,
      aux_sym_order_by_clause_token1,
    ACTIONS(295), 1,
      anon_sym_RPAREN,
    STATE(80), 1,
      sym_formatting_comment,
    STATE(134), 1,
      sym_order_by_clause,
    STATE(216), 1,
      sym_limit_clause,
    STATE(309), 1,
      sym_offset_clause,
    STATE(427), 1,
      sym_for_clause,
    STATE(612), 1,
      sym_update_clause,
  [3061] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(291), 1,
      anon_sym_COMMA,
    STATE(81), 1,
      sym_formatting_comment,
    STATE(82), 1,
      aux_sym_using_lookup_bind_clause_repeat1,
    ACTIONS(297), 9,
      anon_sym_RPAREN,
      aux_sym_sosl_with_clause_token1,
      aux_sym_group_by_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_where_clause_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [3085] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(299), 1,
      anon_sym_COMMA,
    STATE(82), 2,
      sym_formatting_comment,
      aux_sym_using_lookup_bind_clause_repeat1,
    ACTIONS(302), 9,
      anon_sym_RPAREN,
      aux_sym_sosl_with_clause_token1,
      aux_sym_group_by_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_where_clause_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [3107] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(83), 1,
      sym_formatting_comment,
    ACTIONS(304), 11,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
      aux_sym_sosl_using_clause_token1,
      aux_sym_sosl_with_clause_token1,
      aux_sym_group_by_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_and_expression_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [3127] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(84), 1,
      sym_formatting_comment,
    ACTIONS(306), 11,
      anon_sym_COMMA,
      anon_sym_RPAREN,
      aux_sym_sosl_using_clause_token1,
      aux_sym_sosl_with_clause_token1,
      aux_sym_group_by_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_where_clause_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [3147] = 13,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(98), 1,
      aux_sym_limit_clause_token1,
    ACTIONS(100), 1,
      aux_sym_offset_clause_token1,
    ACTIONS(102), 1,
      aux_sym_order_by_clause_token1,
    ACTIONS(308), 1,
      anon_sym_RPAREN,
    STATE(85), 1,
      sym_formatting_comment,
    STATE(139), 1,
      sym_order_by_clause,
    STATE(197), 1,
      sym_limit_clause,
    STATE(305), 1,
      sym_offset_clause,
    STATE(412), 1,
      sym_for_clause,
    STATE(682), 1,
      sym_update_clause,
  [3187] = 13,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(98), 1,
      aux_sym_limit_clause_token1,
    ACTIONS(100), 1,
      aux_sym_offset_clause_token1,
    ACTIONS(102), 1,
      aux_sym_order_by_clause_token1,
    ACTIONS(310), 1,
      anon_sym_RPAREN,
    STATE(86), 1,
      sym_formatting_comment,
    STATE(119), 1,
      sym_order_by_clause,
    STATE(232), 1,
      sym_limit_clause,
    STATE(264), 1,
      sym_offset_clause,
    STATE(398), 1,
      sym_for_clause,
    STATE(661), 1,
      sym_update_clause,
  [3227] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(314), 1,
      aux_sym_using_lookup_bind_clause_token1,
    STATE(87), 1,
      sym_formatting_comment,
    STATE(111), 1,
      sym_using_lookup_bind_clause,
    ACTIONS(312), 9,
      anon_sym_RPAREN,
      aux_sym_sosl_with_clause_token1,
      aux_sym_group_by_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_where_clause_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [3251] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(88), 1,
      sym_formatting_comment,
    ACTIONS(316), 11,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
      aux_sym_sosl_using_clause_token1,
      aux_sym_sosl_with_clause_token1,
      aux_sym_group_by_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_and_expression_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [3271] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(89), 1,
      sym_formatting_comment,
    ACTIONS(233), 11,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
      aux_sym_sosl_using_clause_token1,
      aux_sym_sosl_with_clause_token1,
      aux_sym_group_by_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_and_expression_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [3291] = 13,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(98), 1,
      aux_sym_limit_clause_token1,
    ACTIONS(100), 1,
      aux_sym_offset_clause_token1,
    ACTIONS(102), 1,
      aux_sym_order_by_clause_token1,
    ACTIONS(318), 1,
      anon_sym_RPAREN,
    STATE(90), 1,
      sym_formatting_comment,
    STATE(125), 1,
      sym_order_by_clause,
    STATE(188), 1,
      sym_limit_clause,
    STATE(272), 1,
      sym_offset_clause,
    STATE(559), 1,
      sym_for_clause,
    STATE(840), 1,
      sym_update_clause,
  [3331] = 13,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(98), 1,
      aux_sym_limit_clause_token1,
    ACTIONS(100), 1,
      aux_sym_offset_clause_token1,
    ACTIONS(102), 1,
      aux_sym_order_by_clause_token1,
    ACTIONS(320), 1,
      anon_sym_RPAREN,
    STATE(91), 1,
      sym_formatting_comment,
    STATE(128), 1,
      sym_order_by_clause,
    STATE(193), 1,
      sym_limit_clause,
    STATE(277), 1,
      sym_offset_clause,
    STATE(401), 1,
      sym_for_clause,
    STATE(889), 1,
      sym_update_clause,
  [3371] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(324), 1,
      aux_sym_or_expression_token1,
    STATE(92), 1,
      sym_formatting_comment,
    ACTIONS(322), 9,
      anon_sym_RPAREN,
      aux_sym_sosl_with_clause_token1,
      aux_sym_group_by_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_and_expression_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [3392] = 11,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(158), 1,
      anon_sym_LPAREN,
    ACTIONS(162), 1,
      aux_sym_function_expression_token1,
    ACTIONS(164), 1,
      sym_identifier,
    STATE(13), 1,
      sym_dotted_identifier,
    STATE(20), 1,
      sym__value_expression,
    STATE(93), 1,
      sym_formatting_comment,
    STATE(96), 1,
      sym_comparison_expression,
    STATE(147), 1,
      sym__condition_expression,
    STATE(922), 1,
      sym__function_name,
    STATE(17), 2,
      sym_function_expression,
      sym_field_identifier,
  [3427] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(328), 1,
      aux_sym_or_expression_token1,
    STATE(94), 1,
      sym_formatting_comment,
    ACTIONS(326), 9,
      anon_sym_RPAREN,
      aux_sym_sosl_with_clause_token1,
      aux_sym_group_by_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_and_expression_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [3448] = 8,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(138), 1,
      aux_sym_with_data_cat_expression_token1,
    ACTIONS(332), 1,
      aux_sym_with_user_id_type_token1,
    ACTIONS(334), 1,
      aux_sym_with_record_visibility_expression_token1,
    STATE(95), 1,
      sym_formatting_comment,
    STATE(184), 1,
      sym_soql_with_type,
    ACTIONS(330), 3,
      aux_sym_soql_with_type_token1,
      aux_sym_soql_with_type_token2,
      aux_sym_soql_with_type_token3,
    STATE(233), 3,
      sym_with_user_id_type,
      sym_with_record_visibility_expression,
      sym_with_data_cat_expression,
  [3477] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(338), 1,
      aux_sym_or_expression_token1,
    STATE(96), 1,
      sym_formatting_comment,
    ACTIONS(336), 9,
      anon_sym_RPAREN,
      aux_sym_sosl_with_clause_token1,
      aux_sym_group_by_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_and_expression_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [3498] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(342), 1,
      aux_sym_or_expression_token1,
    STATE(97), 2,
      sym_formatting_comment,
      aux_sym_or_expression_repeat1,
    ACTIONS(340), 8,
      anon_sym_RPAREN,
      aux_sym_sosl_with_clause_token1,
      aux_sym_group_by_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [3519] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(347), 1,
      aux_sym_and_expression_token1,
    STATE(98), 2,
      sym_formatting_comment,
      aux_sym_and_expression_repeat1,
    ACTIONS(345), 8,
      anon_sym_RPAREN,
      aux_sym_sosl_with_clause_token1,
      aux_sym_group_by_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [3540] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(247), 1,
      aux_sym_and_expression_token1,
    STATE(98), 1,
      aux_sym_and_expression_repeat1,
    STATE(99), 1,
      sym_formatting_comment,
    ACTIONS(350), 8,
      anon_sym_RPAREN,
      aux_sym_sosl_with_clause_token1,
      aux_sym_group_by_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [3563] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(352), 1,
      anon_sym_DOT,
    STATE(100), 2,
      sym_formatting_comment,
      aux_sym_dotted_identifier_repeat1,
    ACTIONS(57), 3,
      anon_sym_COMMA,
      anon_sym_RPAREN,
      aux_sym_order_by_clause_token1,
    ACTIONS(55), 5,
      aux_sym_sosl_using_clause_token1,
      aux_sym_where_clause_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      sym_identifier,
  [3586] = 6,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(277), 1,
      anon_sym_DOT,
    STATE(100), 1,
      aux_sym_dotted_identifier_repeat1,
    STATE(101), 1,
      sym_formatting_comment,
    ACTIONS(51), 3,
      anon_sym_COMMA,
      anon_sym_RPAREN,
      aux_sym_order_by_clause_token1,
    ACTIONS(49), 5,
      aux_sym_sosl_using_clause_token1,
      aux_sym_where_clause_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      sym_identifier,
  [3611] = 11,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(158), 1,
      anon_sym_LPAREN,
    ACTIONS(162), 1,
      aux_sym_function_expression_token1,
    ACTIONS(164), 1,
      sym_identifier,
    STATE(13), 1,
      sym_dotted_identifier,
    STATE(20), 1,
      sym__value_expression,
    STATE(96), 1,
      sym_comparison_expression,
    STATE(102), 1,
      sym_formatting_comment,
    STATE(115), 1,
      sym__condition_expression,
    STATE(922), 1,
      sym__function_name,
    STATE(17), 2,
      sym_function_expression,
      sym_field_identifier,
  [3646] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(103), 1,
      sym_formatting_comment,
    ACTIONS(355), 10,
      anon_sym_COMMA,
      anon_sym_RPAREN,
      aux_sym_sosl_with_clause_token1,
      aux_sym_group_by_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_where_clause_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [3665] = 11,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(162), 1,
      aux_sym_function_expression_token1,
    ACTIONS(164), 1,
      sym_identifier,
    ACTIONS(168), 1,
      anon_sym_LPAREN,
    STATE(13), 1,
      sym_dotted_identifier,
    STATE(24), 1,
      sym__value_expression,
    STATE(104), 1,
      sym_formatting_comment,
    STATE(525), 1,
      sym_comparison_expression,
    STATE(578), 1,
      sym__condition_expression,
    STATE(922), 1,
      sym__function_name,
    STATE(17), 2,
      sym_function_expression,
      sym_field_identifier,
  [3700] = 11,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(158), 1,
      anon_sym_LPAREN,
    ACTIONS(162), 1,
      aux_sym_function_expression_token1,
    ACTIONS(164), 1,
      sym_identifier,
    STATE(13), 1,
      sym_dotted_identifier,
    STATE(20), 1,
      sym__value_expression,
    STATE(96), 1,
      sym_comparison_expression,
    STATE(105), 1,
      sym_formatting_comment,
    STATE(114), 1,
      sym__condition_expression,
    STATE(922), 1,
      sym__function_name,
    STATE(17), 2,
      sym_function_expression,
      sym_field_identifier,
  [3735] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(106), 1,
      sym_formatting_comment,
    ACTIONS(302), 10,
      anon_sym_COMMA,
      anon_sym_RPAREN,
      aux_sym_sosl_with_clause_token1,
      aux_sym_group_by_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_where_clause_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [3754] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(249), 1,
      aux_sym_or_expression_token1,
    STATE(97), 1,
      aux_sym_or_expression_repeat1,
    STATE(107), 1,
      sym_formatting_comment,
    ACTIONS(357), 8,
      anon_sym_RPAREN,
      aux_sym_sosl_with_clause_token1,
      aux_sym_group_by_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [3777] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(359), 1,
      anon_sym_COMMA,
    STATE(108), 1,
      sym_formatting_comment,
    STATE(118), 1,
      aux_sym__group_by_expression_repeat1,
    ACTIONS(361), 7,
      anon_sym_RPAREN,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_having_clause_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [3799] = 11,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(98), 1,
      aux_sym_limit_clause_token1,
    ACTIONS(100), 1,
      aux_sym_offset_clause_token1,
    ACTIONS(363), 1,
      anon_sym_RPAREN,
    STATE(109), 1,
      sym_formatting_comment,
    STATE(192), 1,
      sym_limit_clause,
    STATE(276), 1,
      sym_offset_clause,
    STATE(394), 1,
      sym_for_clause,
    STATE(883), 1,
      sym_update_clause,
  [3833] = 11,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(98), 1,
      aux_sym_limit_clause_token1,
    ACTIONS(100), 1,
      aux_sym_offset_clause_token1,
    ACTIONS(365), 1,
      anon_sym_RPAREN,
    STATE(110), 1,
      sym_formatting_comment,
    STATE(194), 1,
      sym_limit_clause,
    STATE(279), 1,
      sym_offset_clause,
    STATE(415), 1,
      sym_for_clause,
    STATE(930), 1,
      sym_update_clause,
  [3867] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(111), 1,
      sym_formatting_comment,
    ACTIONS(367), 9,
      anon_sym_RPAREN,
      aux_sym_sosl_with_clause_token1,
      aux_sym_group_by_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_where_clause_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [3885] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(112), 1,
      sym_formatting_comment,
    ACTIONS(369), 9,
      anon_sym_RPAREN,
      aux_sym_sosl_with_clause_token1,
      aux_sym_group_by_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_where_clause_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [3903] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(373), 1,
      anon_sym_COMMA,
    STATE(113), 2,
      sym_formatting_comment,
      aux_sym_returning_clause_repeat1,
    ACTIONS(371), 7,
      ts_builtin_sym_end,
      aux_sym_returning_clause_token1,
      aux_sym_sosl_using_clause_token1,
      aux_sym_sosl_with_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
  [3923] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(114), 1,
      sym_formatting_comment,
    ACTIONS(345), 9,
      anon_sym_RPAREN,
      aux_sym_sosl_with_clause_token1,
      aux_sym_group_by_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_and_expression_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [3941] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(376), 1,
      aux_sym_or_expression_token1,
    STATE(115), 1,
      sym_formatting_comment,
    ACTIONS(340), 8,
      anon_sym_RPAREN,
      aux_sym_sosl_with_clause_token1,
      aux_sym_group_by_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [3961] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(116), 1,
      sym_formatting_comment,
    ACTIONS(57), 4,
      anon_sym_COMMA,
      anon_sym_RPAREN,
      aux_sym_order_by_clause_token1,
      anon_sym_DOT,
    ACTIONS(55), 5,
      aux_sym_sosl_using_clause_token1,
      aux_sym_where_clause_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      sym_identifier,
  [3981] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(117), 1,
      sym_formatting_comment,
    ACTIONS(378), 9,
      anon_sym_RPAREN,
      aux_sym_sosl_with_clause_token1,
      aux_sym_group_by_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_where_clause_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [3999] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(359), 1,
      anon_sym_COMMA,
    STATE(118), 1,
      sym_formatting_comment,
    STATE(129), 1,
      aux_sym__group_by_expression_repeat1,
    ACTIONS(380), 7,
      anon_sym_RPAREN,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_having_clause_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [4021] = 11,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(98), 1,
      aux_sym_limit_clause_token1,
    ACTIONS(100), 1,
      aux_sym_offset_clause_token1,
    ACTIONS(382), 1,
      anon_sym_RPAREN,
    STATE(119), 1,
      sym_formatting_comment,
    STATE(198), 1,
      sym_limit_clause,
    STATE(283), 1,
      sym_offset_clause,
    STATE(491), 1,
      sym_for_clause,
    STATE(665), 1,
      sym_update_clause,
  [4055] = 11,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(98), 1,
      aux_sym_limit_clause_token1,
    ACTIONS(100), 1,
      aux_sym_offset_clause_token1,
    ACTIONS(384), 1,
      anon_sym_RPAREN,
    STATE(120), 1,
      sym_formatting_comment,
    STATE(213), 1,
      sym_limit_clause,
    STATE(332), 1,
      sym_offset_clause,
    STATE(521), 1,
      sym_for_clause,
    STATE(858), 1,
      sym_update_clause,
  [4089] = 11,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(98), 1,
      aux_sym_limit_clause_token1,
    ACTIONS(100), 1,
      aux_sym_offset_clause_token1,
    ACTIONS(386), 1,
      anon_sym_RPAREN,
    STATE(121), 1,
      sym_formatting_comment,
    STATE(203), 1,
      sym_limit_clause,
    STATE(288), 1,
      sym_offset_clause,
    STATE(553), 1,
      sym_for_clause,
    STATE(716), 1,
      sym_update_clause,
  [4123] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(122), 1,
      sym_formatting_comment,
    ACTIONS(388), 9,
      anon_sym_RPAREN,
      aux_sym_sosl_with_clause_token1,
      aux_sym_group_by_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_where_clause_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [4141] = 11,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(98), 1,
      aux_sym_limit_clause_token1,
    ACTIONS(100), 1,
      aux_sym_offset_clause_token1,
    ACTIONS(390), 1,
      anon_sym_RPAREN,
    STATE(123), 1,
      sym_formatting_comment,
    STATE(205), 1,
      sym_limit_clause,
    STATE(291), 1,
      sym_offset_clause,
    STATE(373), 1,
      sym_for_clause,
    STATE(743), 1,
      sym_update_clause,
  [4175] = 11,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(98), 1,
      aux_sym_limit_clause_token1,
    ACTIONS(100), 1,
      aux_sym_offset_clause_token1,
    ACTIONS(392), 1,
      anon_sym_RPAREN,
    STATE(124), 1,
      sym_formatting_comment,
    STATE(185), 1,
      sym_limit_clause,
    STATE(270), 1,
      sym_offset_clause,
    STATE(470), 1,
      sym_for_clause,
    STATE(740), 1,
      sym_update_clause,
  [4209] = 11,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(98), 1,
      aux_sym_limit_clause_token1,
    ACTIONS(100), 1,
      aux_sym_offset_clause_token1,
    ACTIONS(394), 1,
      anon_sym_RPAREN,
    STATE(125), 1,
      sym_formatting_comment,
    STATE(206), 1,
      sym_limit_clause,
    STATE(296), 1,
      sym_offset_clause,
    STATE(386), 1,
      sym_for_clause,
    STATE(855), 1,
      sym_update_clause,
  [4243] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(126), 1,
      sym_formatting_comment,
    ACTIONS(396), 9,
      anon_sym_RPAREN,
      aux_sym_sosl_with_clause_token1,
      aux_sym_group_by_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_where_clause_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [4261] = 11,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(98), 1,
      aux_sym_limit_clause_token1,
    ACTIONS(100), 1,
      aux_sym_offset_clause_token1,
    ACTIONS(398), 1,
      anon_sym_RPAREN,
    STATE(127), 1,
      sym_formatting_comment,
    STATE(209), 1,
      sym_limit_clause,
    STATE(300), 1,
      sym_offset_clause,
    STATE(396), 1,
      sym_for_clause,
    STATE(870), 1,
      sym_update_clause,
  [4295] = 11,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(98), 1,
      aux_sym_limit_clause_token1,
    ACTIONS(100), 1,
      aux_sym_offset_clause_token1,
    ACTIONS(400), 1,
      anon_sym_RPAREN,
    STATE(128), 1,
      sym_formatting_comment,
    STATE(210), 1,
      sym_limit_clause,
    STATE(303), 1,
      sym_offset_clause,
    STATE(405), 1,
      sym_for_clause,
    STATE(890), 1,
      sym_update_clause,
  [4329] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(402), 1,
      anon_sym_COMMA,
    STATE(129), 2,
      sym_formatting_comment,
      aux_sym__group_by_expression_repeat1,
    ACTIONS(405), 7,
      anon_sym_RPAREN,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_having_clause_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [4349] = 11,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(98), 1,
      aux_sym_limit_clause_token1,
    ACTIONS(100), 1,
      aux_sym_offset_clause_token1,
    ACTIONS(407), 1,
      anon_sym_RPAREN,
    STATE(130), 1,
      sym_formatting_comment,
    STATE(215), 1,
      sym_limit_clause,
    STATE(308), 1,
      sym_offset_clause,
    STATE(424), 1,
      sym_for_clause,
    STATE(608), 1,
      sym_update_clause,
  [4383] = 11,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(98), 1,
      aux_sym_limit_clause_token1,
    ACTIONS(100), 1,
      aux_sym_offset_clause_token1,
    ACTIONS(409), 1,
      anon_sym_RPAREN,
    STATE(131), 1,
      sym_formatting_comment,
    STATE(218), 1,
      sym_limit_clause,
    STATE(311), 1,
      sym_offset_clause,
    STATE(431), 1,
      sym_for_clause,
    STATE(623), 1,
      sym_update_clause,
  [4417] = 11,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(98), 1,
      aux_sym_limit_clause_token1,
    ACTIONS(100), 1,
      aux_sym_offset_clause_token1,
    ACTIONS(411), 1,
      anon_sym_RPAREN,
    STATE(132), 1,
      sym_formatting_comment,
    STATE(219), 1,
      sym_limit_clause,
    STATE(315), 1,
      sym_offset_clause,
    STATE(440), 1,
      sym_for_clause,
    STATE(643), 1,
      sym_update_clause,
  [4451] = 11,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(98), 1,
      aux_sym_limit_clause_token1,
    ACTIONS(100), 1,
      aux_sym_offset_clause_token1,
    ACTIONS(413), 1,
      anon_sym_RPAREN,
    STATE(133), 1,
      sym_formatting_comment,
    STATE(220), 1,
      sym_limit_clause,
    STATE(319), 1,
      sym_offset_clause,
    STATE(454), 1,
      sym_for_clause,
    STATE(683), 1,
      sym_update_clause,
  [4485] = 11,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(98), 1,
      aux_sym_limit_clause_token1,
    ACTIONS(100), 1,
      aux_sym_offset_clause_token1,
    ACTIONS(415), 1,
      anon_sym_RPAREN,
    STATE(134), 1,
      sym_formatting_comment,
    STATE(224), 1,
      sym_limit_clause,
    STATE(326), 1,
      sym_offset_clause,
    STATE(481), 1,
      sym_for_clause,
    STATE(748), 1,
      sym_update_clause,
  [4519] = 11,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(96), 1,
      aux_sym_where_clause_token1,
    ACTIONS(98), 1,
      aux_sym_limit_clause_token1,
    ACTIONS(100), 1,
      aux_sym_offset_clause_token1,
    ACTIONS(102), 1,
      aux_sym_order_by_clause_token1,
    ACTIONS(417), 1,
      anon_sym_RPAREN,
    STATE(135), 1,
      sym_formatting_comment,
    STATE(222), 1,
      sym_where_clause,
    STATE(297), 1,
      sym_order_by_clause,
    STATE(390), 1,
      sym_limit_clause,
    STATE(833), 1,
      sym_offset_clause,
  [4553] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(136), 1,
      sym_formatting_comment,
    STATE(243), 1,
      sym_order_null_direction,
    ACTIONS(263), 2,
      aux_sym_order_null_direction_token1,
      aux_sym_order_null_direction_token2,
    ACTIONS(419), 6,
      anon_sym_COMMA,
      anon_sym_RPAREN,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
  [4575] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(423), 1,
      aux_sym_returning_clause_token1,
    STATE(191), 1,
      sym_returning_clause,
    STATE(137), 2,
      sym_formatting_comment,
      aux_sym_sosl_query_body_repeat1,
    ACTIONS(421), 6,
      ts_builtin_sym_end,
      aux_sym_sosl_using_clause_token1,
      aux_sym_sosl_with_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
  [4597] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(428), 1,
      anon_sym_COMMA,
    STATE(113), 1,
      aux_sym_returning_clause_repeat1,
    STATE(138), 1,
      sym_formatting_comment,
    ACTIONS(426), 7,
      ts_builtin_sym_end,
      aux_sym_returning_clause_token1,
      aux_sym_sosl_using_clause_token1,
      aux_sym_sosl_with_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
  [4619] = 11,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(98), 1,
      aux_sym_limit_clause_token1,
    ACTIONS(100), 1,
      aux_sym_offset_clause_token1,
    ACTIONS(430), 1,
      anon_sym_RPAREN,
    STATE(139), 1,
      sym_formatting_comment,
    STATE(187), 1,
      sym_limit_clause,
    STATE(271), 1,
      sym_offset_clause,
    STATE(541), 1,
      sym_for_clause,
    STATE(816), 1,
      sym_update_clause,
  [4653] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(434), 1,
      anon_sym_LPAREN,
    STATE(140), 1,
      sym_formatting_comment,
    ACTIONS(432), 8,
      ts_builtin_sym_end,
      aux_sym_returning_clause_token1,
      anon_sym_COMMA,
      aux_sym_sosl_using_clause_token1,
      aux_sym_sosl_with_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
  [4673] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(428), 1,
      anon_sym_COMMA,
    STATE(138), 1,
      aux_sym_returning_clause_repeat1,
    STATE(141), 1,
      sym_formatting_comment,
    ACTIONS(436), 7,
      ts_builtin_sym_end,
      aux_sym_returning_clause_token1,
      aux_sym_sosl_using_clause_token1,
      aux_sym_sosl_with_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
  [4695] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(142), 1,
      sym_formatting_comment,
    ACTIONS(64), 3,
      anon_sym_COMMA,
      anon_sym_RPAREN,
      aux_sym_order_by_clause_token1,
    ACTIONS(62), 5,
      aux_sym_sosl_using_clause_token1,
      aux_sym_where_clause_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      sym_identifier,
  [4714] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(143), 1,
      sym_formatting_comment,
    ACTIONS(438), 8,
      ts_builtin_sym_end,
      aux_sym_returning_clause_token1,
      anon_sym_COMMA,
      aux_sym_sosl_using_clause_token1,
      aux_sym_sosl_with_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
  [4731] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(144), 1,
      sym_formatting_comment,
    ACTIONS(405), 8,
      anon_sym_COMMA,
      anon_sym_RPAREN,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_having_clause_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [4748] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(145), 1,
      sym_formatting_comment,
    ACTIONS(440), 8,
      anon_sym_COMMA,
      anon_sym_RPAREN,
      aux_sym_sosl_using_clause_token1,
      aux_sym_from_clause_token1,
      aux_sym_where_clause_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [4765] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(442), 1,
      anon_sym_COMMA,
    STATE(146), 1,
      sym_formatting_comment,
    STATE(169), 1,
      aux_sym_selected_fields_repeat1,
    ACTIONS(444), 6,
      anon_sym_RPAREN,
      aux_sym_sosl_using_clause_token1,
      aux_sym_where_clause_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [4786] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(147), 1,
      sym_formatting_comment,
    ACTIONS(446), 8,
      anon_sym_RPAREN,
      aux_sym_sosl_with_clause_token1,
      aux_sym_group_by_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [4803] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(148), 1,
      sym_formatting_comment,
    ACTIONS(448), 8,
      anon_sym_COMMA,
      anon_sym_RPAREN,
      aux_sym_sosl_using_clause_token1,
      aux_sym_from_clause_token1,
      aux_sym_where_clause_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [4820] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(149), 1,
      sym_formatting_comment,
    ACTIONS(450), 8,
      anon_sym_COMMA,
      anon_sym_RPAREN,
      aux_sym_sosl_using_clause_token1,
      aux_sym_from_clause_token1,
      aux_sym_where_clause_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [4837] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(150), 1,
      sym_formatting_comment,
    ACTIONS(452), 8,
      ts_builtin_sym_end,
      aux_sym_returning_clause_token1,
      anon_sym_COMMA,
      aux_sym_sosl_using_clause_token1,
      aux_sym_sosl_with_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
  [4854] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(151), 1,
      sym_formatting_comment,
    ACTIONS(82), 3,
      anon_sym_COMMA,
      anon_sym_RPAREN,
      aux_sym_order_by_clause_token1,
    ACTIONS(80), 5,
      aux_sym_sosl_using_clause_token1,
      aux_sym_where_clause_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      sym_identifier,
  [4873] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(152), 1,
      sym_formatting_comment,
    ACTIONS(454), 8,
      anon_sym_RPAREN,
      aux_sym_sosl_with_clause_token1,
      aux_sym_group_by_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [4890] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(153), 1,
      sym_formatting_comment,
    ACTIONS(456), 8,
      ts_builtin_sym_end,
      aux_sym_returning_clause_token1,
      anon_sym_COMMA,
      aux_sym_sosl_using_clause_token1,
      aux_sym_sosl_with_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
  [4907] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(154), 1,
      sym_formatting_comment,
    ACTIONS(74), 3,
      anon_sym_COMMA,
      anon_sym_RPAREN,
      aux_sym_order_by_clause_token1,
    ACTIONS(72), 5,
      aux_sym_sosl_using_clause_token1,
      aux_sym_where_clause_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      sym_identifier,
  [4926] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(155), 1,
      sym_formatting_comment,
    ACTIONS(78), 3,
      anon_sym_COMMA,
      anon_sym_RPAREN,
      aux_sym_order_by_clause_token1,
    ACTIONS(76), 5,
      aux_sym_sosl_using_clause_token1,
      aux_sym_where_clause_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      sym_identifier,
  [4945] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(156), 1,
      sym_formatting_comment,
    ACTIONS(245), 8,
      anon_sym_RPAREN,
      aux_sym_sosl_with_clause_token1,
      aux_sym_group_by_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [4962] = 9,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(162), 1,
      aux_sym_function_expression_token1,
    ACTIONS(164), 1,
      sym_identifier,
    STATE(13), 1,
      sym_dotted_identifier,
    STATE(65), 1,
      sym__value_expression,
    STATE(157), 1,
      sym_formatting_comment,
    STATE(217), 1,
      sym_order_expression,
    STATE(922), 1,
      sym__function_name,
    STATE(17), 2,
      sym_function_expression,
      sym_field_identifier,
  [4991] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(158), 1,
      sym_formatting_comment,
    ACTIONS(371), 8,
      ts_builtin_sym_end,
      aux_sym_returning_clause_token1,
      anon_sym_COMMA,
      aux_sym_sosl_using_clause_token1,
      aux_sym_sosl_with_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
  [5008] = 9,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(162), 1,
      aux_sym_function_expression_token1,
    ACTIONS(164), 1,
      sym_identifier,
    STATE(13), 1,
      sym_dotted_identifier,
    STATE(65), 1,
      sym__value_expression,
    STATE(159), 1,
      sym_formatting_comment,
    STATE(241), 1,
      sym_order_expression,
    STATE(922), 1,
      sym__function_name,
    STATE(17), 2,
      sym_function_expression,
      sym_field_identifier,
  [5037] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(160), 1,
      sym_formatting_comment,
    ACTIONS(458), 8,
      anon_sym_COMMA,
      anon_sym_RPAREN,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_null_direction_token1,
      aux_sym_order_null_direction_token2,
  [5054] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(161), 1,
      sym_formatting_comment,
    ACTIONS(460), 8,
      anon_sym_COMMA,
      anon_sym_RPAREN,
      aux_sym_sosl_using_clause_token1,
      aux_sym_from_clause_token1,
      aux_sym_where_clause_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [5071] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(162), 1,
      sym_formatting_comment,
    ACTIONS(462), 8,
      anon_sym_COMMA,
      anon_sym_RPAREN,
      aux_sym_sosl_using_clause_token1,
      aux_sym_from_clause_token1,
      aux_sym_where_clause_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [5088] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(466), 1,
      aux_sym_sosl_with_clause_token1,
    STATE(255), 1,
      sym_sosl_with_clause,
    STATE(163), 2,
      sym_formatting_comment,
      aux_sym_sosl_query_body_repeat2,
    ACTIONS(464), 5,
      ts_builtin_sym_end,
      aux_sym_sosl_using_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
  [5109] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(164), 1,
      sym_formatting_comment,
    ACTIONS(469), 8,
      ts_builtin_sym_end,
      aux_sym_in_clause_token1,
      aux_sym_returning_clause_token1,
      aux_sym_sosl_using_clause_token1,
      aux_sym_sosl_with_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
  [5126] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(165), 1,
      sym_formatting_comment,
    ACTIONS(471), 8,
      ts_builtin_sym_end,
      aux_sym_returning_clause_token1,
      anon_sym_COMMA,
      aux_sym_sosl_using_clause_token1,
      aux_sym_sosl_with_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
  [5143] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(475), 1,
      sym_identifier,
    STATE(166), 1,
      sym_formatting_comment,
    ACTIONS(448), 3,
      anon_sym_COMMA,
      anon_sym_RPAREN,
      aux_sym_order_by_clause_token1,
    ACTIONS(473), 4,
      aux_sym_sosl_using_clause_token1,
      aux_sym_where_clause_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
  [5164] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(167), 1,
      sym_formatting_comment,
    ACTIONS(70), 3,
      anon_sym_COMMA,
      anon_sym_RPAREN,
      aux_sym_order_by_clause_token1,
    ACTIONS(68), 5,
      aux_sym_sosl_using_clause_token1,
      aux_sym_where_clause_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      sym_identifier,
  [5183] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(479), 1,
      aux_sym_using_scope_type_token3,
    STATE(126), 1,
      sym_using_scope_type,
    STATE(168), 1,
      sym_formatting_comment,
    ACTIONS(477), 6,
      aux_sym_using_scope_type_token1,
      aux_sym_using_scope_type_token2,
      aux_sym_using_scope_type_token4,
      aux_sym_using_scope_type_token5,
      aux_sym_using_scope_type_token6,
      aux_sym_using_scope_type_token7,
  [5204] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(442), 1,
      anon_sym_COMMA,
    STATE(169), 1,
      sym_formatting_comment,
    STATE(174), 1,
      aux_sym_selected_fields_repeat1,
    ACTIONS(481), 6,
      anon_sym_RPAREN,
      aux_sym_sosl_using_clause_token1,
      aux_sym_where_clause_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [5225] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(170), 1,
      sym_formatting_comment,
    ACTIONS(483), 8,
      ts_builtin_sym_end,
      aux_sym_returning_clause_token1,
      anon_sym_COMMA,
      aux_sym_sosl_using_clause_token1,
      aux_sym_sosl_with_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
  [5242] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(171), 1,
      sym_formatting_comment,
    ACTIONS(485), 8,
      anon_sym_COMMA,
      anon_sym_RPAREN,
      aux_sym_sosl_using_clause_token1,
      aux_sym_from_clause_token1,
      aux_sym_where_clause_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [5259] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(489), 1,
      aux_sym_having_clause_token1,
    STATE(172), 1,
      sym_formatting_comment,
    STATE(252), 1,
      sym_having_clause,
    ACTIONS(487), 6,
      anon_sym_RPAREN,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [5280] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(173), 1,
      sym_formatting_comment,
    ACTIONS(491), 8,
      ts_builtin_sym_end,
      aux_sym_in_clause_token1,
      aux_sym_returning_clause_token1,
      aux_sym_sosl_using_clause_token1,
      aux_sym_sosl_with_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
  [5297] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(493), 1,
      anon_sym_COMMA,
    STATE(174), 2,
      sym_formatting_comment,
      aux_sym_selected_fields_repeat1,
    ACTIONS(485), 6,
      anon_sym_RPAREN,
      aux_sym_sosl_using_clause_token1,
      aux_sym_where_clause_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [5316] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(175), 1,
      sym_formatting_comment,
    ACTIONS(496), 8,
      anon_sym_COMMA,
      anon_sym_RPAREN,
      aux_sym_sosl_using_clause_token1,
      aux_sym_from_clause_token1,
      aux_sym_where_clause_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [5333] = 9,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(150), 1,
      aux_sym_function_expression_token1,
    ACTIONS(152), 1,
      sym_identifier,
    ACTIONS(498), 1,
      anon_sym_RPAREN,
    STATE(142), 1,
      sym_dotted_identifier,
    STATE(176), 1,
      sym_formatting_comment,
    STATE(547), 1,
      sym__value_expression,
    STATE(630), 1,
      sym__function_name,
    STATE(151), 2,
      sym_function_expression,
      sym_field_identifier,
  [5362] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(177), 1,
      sym_formatting_comment,
    ACTIONS(500), 8,
      ts_builtin_sym_end,
      aux_sym_returning_clause_token1,
      anon_sym_COMMA,
      aux_sym_sosl_using_clause_token1,
      aux_sym_sosl_with_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
  [5379] = 10,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(53), 1,
      anon_sym_DOT,
    ACTIONS(502), 1,
      aux_sym_type_of_clause_token2,
    ACTIONS(504), 1,
      aux_sym_when_expression_token1,
    ACTIONS(506), 1,
      aux_sym_else_expression_token1,
    STATE(9), 1,
      aux_sym_dotted_identifier_repeat1,
    STATE(178), 1,
      sym_formatting_comment,
    STATE(254), 1,
      aux_sym_type_of_clause_repeat1,
    STATE(524), 1,
      sym_when_expression,
    STATE(632), 1,
      sym_else_expression,
  [5410] = 9,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(100), 1,
      aux_sym_offset_clause_token1,
    ACTIONS(508), 1,
      anon_sym_RPAREN,
    STATE(179), 1,
      sym_formatting_comment,
    STATE(269), 1,
      sym_offset_clause,
    STATE(438), 1,
      sym_for_clause,
    STATE(719), 1,
      sym_update_clause,
  [5438] = 8,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(150), 1,
      aux_sym_function_expression_token1,
    ACTIONS(152), 1,
      sym_identifier,
    STATE(142), 1,
      sym_dotted_identifier,
    STATE(180), 1,
      sym_formatting_comment,
    STATE(547), 1,
      sym__value_expression,
    STATE(630), 1,
      sym__function_name,
    STATE(151), 2,
      sym_function_expression,
      sym_field_identifier,
  [5464] = 9,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(100), 1,
      aux_sym_offset_clause_token1,
    ACTIONS(510), 1,
      anon_sym_RPAREN,
    STATE(181), 1,
      sym_formatting_comment,
    STATE(287), 1,
      sym_offset_clause,
    STATE(543), 1,
      sym_for_clause,
    STATE(700), 1,
      sym_update_clause,
  [5492] = 9,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(98), 1,
      aux_sym_limit_clause_token1,
    ACTIONS(100), 1,
      aux_sym_offset_clause_token1,
    ACTIONS(512), 1,
      ts_builtin_sym_end,
    STATE(182), 1,
      sym_formatting_comment,
    STATE(265), 1,
      sym_limit_clause,
    STATE(483), 1,
      sym_offset_clause,
    STATE(879), 1,
      sym_update_clause,
  [5520] = 9,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(100), 1,
      aux_sym_offset_clause_token1,
    ACTIONS(514), 1,
      anon_sym_RPAREN,
    STATE(183), 1,
      sym_formatting_comment,
    STATE(290), 1,
      sym_offset_clause,
    STATE(371), 1,
      sym_for_clause,
    STATE(730), 1,
      sym_update_clause,
  [5548] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(184), 1,
      sym_formatting_comment,
    ACTIONS(516), 7,
      anon_sym_RPAREN,
      aux_sym_group_by_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [5564] = 9,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(100), 1,
      aux_sym_offset_clause_token1,
    ACTIONS(518), 1,
      anon_sym_RPAREN,
    STATE(185), 1,
      sym_formatting_comment,
    STATE(292), 1,
      sym_offset_clause,
    STATE(376), 1,
      sym_for_clause,
    STATE(761), 1,
      sym_update_clause,
  [5592] = 8,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(162), 1,
      aux_sym_function_expression_token1,
    ACTIONS(164), 1,
      sym_identifier,
    STATE(13), 1,
      sym_dotted_identifier,
    STATE(172), 1,
      sym__group_by_expression,
    STATE(186), 1,
      sym_formatting_comment,
    STATE(922), 1,
      sym__function_name,
    STATE(108), 2,
      sym_function_expression,
      sym_field_identifier,
  [5618] = 9,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(100), 1,
      aux_sym_offset_clause_token1,
    ACTIONS(520), 1,
      anon_sym_RPAREN,
    STATE(187), 1,
      sym_formatting_comment,
    STATE(293), 1,
      sym_offset_clause,
    STATE(380), 1,
      sym_for_clause,
    STATE(794), 1,
      sym_update_clause,
  [5646] = 9,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(100), 1,
      aux_sym_offset_clause_token1,
    ACTIONS(522), 1,
      anon_sym_RPAREN,
    STATE(188), 1,
      sym_formatting_comment,
    STATE(294), 1,
      sym_offset_clause,
    STATE(383), 1,
      sym_for_clause,
    STATE(821), 1,
      sym_update_clause,
  [5674] = 9,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(98), 1,
      aux_sym_limit_clause_token1,
    ACTIONS(100), 1,
      aux_sym_offset_clause_token1,
    ACTIONS(181), 1,
      ts_builtin_sym_end,
    STATE(189), 1,
      sym_formatting_comment,
    STATE(323), 1,
      sym_limit_clause,
    STATE(542), 1,
      sym_offset_clause,
    STATE(737), 1,
      sym_update_clause,
  [5702] = 9,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(100), 1,
      aux_sym_offset_clause_token1,
    ACTIONS(524), 1,
      anon_sym_RPAREN,
    STATE(190), 1,
      sym_formatting_comment,
    STATE(298), 1,
      sym_offset_clause,
    STATE(393), 1,
      sym_for_clause,
    STATE(864), 1,
      sym_update_clause,
  [5730] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(191), 1,
      sym_formatting_comment,
    ACTIONS(526), 7,
      ts_builtin_sym_end,
      aux_sym_returning_clause_token1,
      aux_sym_sosl_using_clause_token1,
      aux_sym_sosl_with_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
  [5746] = 9,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(100), 1,
      aux_sym_offset_clause_token1,
    ACTIONS(528), 1,
      anon_sym_RPAREN,
    STATE(192), 1,
      sym_formatting_comment,
    STATE(301), 1,
      sym_offset_clause,
    STATE(400), 1,
      sym_for_clause,
    STATE(878), 1,
      sym_update_clause,
  [5774] = 9,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(100), 1,
      aux_sym_offset_clause_token1,
    ACTIONS(530), 1,
      anon_sym_RPAREN,
    STATE(193), 1,
      sym_formatting_comment,
    STATE(302), 1,
      sym_offset_clause,
    STATE(403), 1,
      sym_for_clause,
    STATE(884), 1,
      sym_update_clause,
  [5802] = 9,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(100), 1,
      aux_sym_offset_clause_token1,
    ACTIONS(532), 1,
      anon_sym_RPAREN,
    STATE(194), 1,
      sym_formatting_comment,
    STATE(304), 1,
      sym_offset_clause,
    STATE(408), 1,
      sym_for_clause,
    STATE(898), 1,
      sym_update_clause,
  [5830] = 9,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(100), 1,
      aux_sym_offset_clause_token1,
    ACTIONS(534), 1,
      anon_sym_RPAREN,
    STATE(195), 1,
      sym_formatting_comment,
    STATE(331), 1,
      sym_offset_clause,
    STATE(482), 1,
      sym_for_clause,
    STATE(798), 1,
      sym_update_clause,
  [5858] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(196), 1,
      sym_formatting_comment,
    ACTIONS(536), 7,
      anon_sym_RPAREN,
      aux_sym_group_by_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [5874] = 9,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(100), 1,
      aux_sym_offset_clause_token1,
    ACTIONS(538), 1,
      anon_sym_RPAREN,
    STATE(197), 1,
      sym_formatting_comment,
    STATE(260), 1,
      sym_offset_clause,
    STATE(503), 1,
      sym_for_clause,
    STATE(757), 1,
      sym_update_clause,
  [5902] = 9,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(100), 1,
      aux_sym_offset_clause_token1,
    ACTIONS(540), 1,
      anon_sym_RPAREN,
    STATE(198), 1,
      sym_formatting_comment,
    STATE(306), 1,
      sym_offset_clause,
    STATE(419), 1,
      sym_for_clause,
    STATE(596), 1,
      sym_update_clause,
  [5930] = 9,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(100), 1,
      aux_sym_offset_clause_token1,
    ACTIONS(542), 1,
      anon_sym_RPAREN,
    STATE(199), 1,
      sym_formatting_comment,
    STATE(307), 1,
      sym_offset_clause,
    STATE(422), 1,
      sym_for_clause,
    STATE(602), 1,
      sym_update_clause,
  [5958] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(53), 1,
      anon_sym_DOT,
    ACTIONS(544), 1,
      anon_sym_COMMA,
    STATE(9), 1,
      aux_sym_dotted_identifier_repeat1,
    STATE(200), 1,
      sym_formatting_comment,
    STATE(334), 1,
      aux_sym_field_list_repeat1,
    ACTIONS(546), 3,
      aux_sym_type_of_clause_token2,
      aux_sym_when_expression_token1,
      aux_sym_else_expression_token1,
  [5982] = 9,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(100), 1,
      aux_sym_offset_clause_token1,
    ACTIONS(548), 1,
      anon_sym_RPAREN,
    STATE(201), 1,
      sym_formatting_comment,
    STATE(310), 1,
      sym_offset_clause,
    STATE(430), 1,
      sym_for_clause,
    STATE(617), 1,
      sym_update_clause,
  [6010] = 9,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(100), 1,
      aux_sym_offset_clause_token1,
    ACTIONS(550), 1,
      anon_sym_RPAREN,
    STATE(202), 1,
      sym_formatting_comment,
    STATE(274), 1,
      sym_offset_clause,
    STATE(388), 1,
      sym_for_clause,
    STATE(869), 1,
      sym_update_clause,
  [6038] = 9,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(100), 1,
      aux_sym_offset_clause_token1,
    ACTIONS(552), 1,
      anon_sym_RPAREN,
    STATE(203), 1,
      sym_formatting_comment,
    STATE(312), 1,
      sym_offset_clause,
    STATE(435), 1,
      sym_for_clause,
    STATE(631), 1,
      sym_update_clause,
  [6066] = 9,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(100), 1,
      aux_sym_offset_clause_token1,
    ACTIONS(554), 1,
      anon_sym_RPAREN,
    STATE(204), 1,
      sym_formatting_comment,
    STATE(313), 1,
      sym_offset_clause,
    STATE(437), 1,
      sym_for_clause,
    STATE(637), 1,
      sym_update_clause,
  [6094] = 9,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(100), 1,
      aux_sym_offset_clause_token1,
    ACTIONS(556), 1,
      anon_sym_RPAREN,
    STATE(205), 1,
      sym_formatting_comment,
    STATE(316), 1,
      sym_offset_clause,
    STATE(442), 1,
      sym_for_clause,
    STATE(651), 1,
      sym_update_clause,
  [6122] = 9,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(100), 1,
      aux_sym_offset_clause_token1,
    ACTIONS(558), 1,
      anon_sym_RPAREN,
    STATE(206), 1,
      sym_formatting_comment,
    STATE(317), 1,
      sym_offset_clause,
    STATE(448), 1,
      sym_for_clause,
    STATE(671), 1,
      sym_update_clause,
  [6150] = 9,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(100), 1,
      aux_sym_offset_clause_token1,
    ACTIONS(560), 1,
      anon_sym_RPAREN,
    STATE(207), 1,
      sym_formatting_comment,
    STATE(318), 1,
      sym_offset_clause,
    STATE(451), 1,
      sym_for_clause,
    STATE(677), 1,
      sym_update_clause,
  [6178] = 9,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(98), 1,
      aux_sym_limit_clause_token1,
    ACTIONS(100), 1,
      aux_sym_offset_clause_token1,
    ACTIONS(166), 1,
      ts_builtin_sym_end,
    STATE(208), 1,
      sym_formatting_comment,
    STATE(338), 1,
      sym_limit_clause,
    STATE(558), 1,
      sym_offset_clause,
    STATE(672), 1,
      sym_update_clause,
  [6206] = 9,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(100), 1,
      aux_sym_offset_clause_token1,
    ACTIONS(562), 1,
      anon_sym_RPAREN,
    STATE(209), 1,
      sym_formatting_comment,
    STATE(320), 1,
      sym_offset_clause,
    STATE(458), 1,
      sym_for_clause,
    STATE(691), 1,
      sym_update_clause,
  [6234] = 9,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(100), 1,
      aux_sym_offset_clause_token1,
    ACTIONS(564), 1,
      anon_sym_RPAREN,
    STATE(210), 1,
      sym_formatting_comment,
    STATE(321), 1,
      sym_offset_clause,
    STATE(463), 1,
      sym_for_clause,
    STATE(706), 1,
      sym_update_clause,
  [6262] = 9,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(100), 1,
      aux_sym_offset_clause_token1,
    ACTIONS(566), 1,
      anon_sym_RPAREN,
    STATE(211), 1,
      sym_formatting_comment,
    STATE(278), 1,
      sym_offset_clause,
    STATE(409), 1,
      sym_for_clause,
    STATE(907), 1,
      sym_update_clause,
  [6290] = 9,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(98), 1,
      aux_sym_limit_clause_token1,
    ACTIONS(100), 1,
      aux_sym_offset_clause_token1,
    ACTIONS(102), 1,
      aux_sym_order_by_clause_token1,
    ACTIONS(417), 1,
      anon_sym_RPAREN,
    STATE(212), 1,
      sym_formatting_comment,
    STATE(297), 1,
      sym_order_by_clause,
    STATE(390), 1,
      sym_limit_clause,
    STATE(833), 1,
      sym_offset_clause,
  [6318] = 9,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(100), 1,
      aux_sym_offset_clause_token1,
    ACTIONS(568), 1,
      anon_sym_RPAREN,
    STATE(213), 1,
      sym_formatting_comment,
    STATE(280), 1,
      sym_offset_clause,
    STATE(429), 1,
      sym_for_clause,
    STATE(601), 1,
      sym_update_clause,
  [6346] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(214), 1,
      sym_formatting_comment,
    ACTIONS(570), 7,
      anon_sym_RPAREN,
      aux_sym_group_by_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [6362] = 9,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(100), 1,
      aux_sym_offset_clause_token1,
    ACTIONS(572), 1,
      anon_sym_RPAREN,
    STATE(215), 1,
      sym_formatting_comment,
    STATE(324), 1,
      sym_offset_clause,
    STATE(475), 1,
      sym_for_clause,
    STATE(736), 1,
      sym_update_clause,
  [6390] = 9,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(100), 1,
      aux_sym_offset_clause_token1,
    ACTIONS(574), 1,
      anon_sym_RPAREN,
    STATE(216), 1,
      sym_formatting_comment,
    STATE(325), 1,
      sym_offset_clause,
    STATE(478), 1,
      sym_for_clause,
    STATE(742), 1,
      sym_update_clause,
  [6418] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(576), 1,
      anon_sym_COMMA,
    STATE(217), 1,
      sym_formatting_comment,
    STATE(228), 1,
      aux_sym_order_by_clause_repeat1,
    ACTIONS(578), 5,
      anon_sym_RPAREN,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
  [6438] = 9,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(100), 1,
      aux_sym_offset_clause_token1,
    ACTIONS(580), 1,
      anon_sym_RPAREN,
    STATE(218), 1,
      sym_formatting_comment,
    STATE(327), 1,
      sym_offset_clause,
    STATE(485), 1,
      sym_for_clause,
    STATE(756), 1,
      sym_update_clause,
  [6466] = 9,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(100), 1,
      aux_sym_offset_clause_token1,
    ACTIONS(582), 1,
      anon_sym_RPAREN,
    STATE(219), 1,
      sym_formatting_comment,
    STATE(328), 1,
      sym_offset_clause,
    STATE(490), 1,
      sym_for_clause,
    STATE(771), 1,
      sym_update_clause,
  [6494] = 9,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(100), 1,
      aux_sym_offset_clause_token1,
    ACTIONS(584), 1,
      anon_sym_RPAREN,
    STATE(220), 1,
      sym_formatting_comment,
    STATE(329), 1,
      sym_offset_clause,
    STATE(496), 1,
      sym_for_clause,
    STATE(793), 1,
      sym_update_clause,
  [6522] = 9,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(98), 1,
      aux_sym_limit_clause_token1,
    ACTIONS(100), 1,
      aux_sym_offset_clause_token1,
    ACTIONS(275), 1,
      ts_builtin_sym_end,
    STATE(221), 1,
      sym_formatting_comment,
    STATE(295), 1,
      sym_limit_clause,
    STATE(370), 1,
      sym_offset_clause,
    STATE(917), 1,
      sym_update_clause,
  [6550] = 9,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(98), 1,
      aux_sym_limit_clause_token1,
    ACTIONS(100), 1,
      aux_sym_offset_clause_token1,
    ACTIONS(102), 1,
      aux_sym_order_by_clause_token1,
    ACTIONS(586), 1,
      anon_sym_RPAREN,
    STATE(222), 1,
      sym_formatting_comment,
    STATE(275), 1,
      sym_order_by_clause,
    STATE(387), 1,
      sym_limit_clause,
    STATE(882), 1,
      sym_offset_clause,
  [6578] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(223), 1,
      sym_formatting_comment,
    ACTIONS(588), 7,
      anon_sym_RPAREN,
      aux_sym_group_by_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [6594] = 9,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(100), 1,
      aux_sym_offset_clause_token1,
    ACTIONS(590), 1,
      anon_sym_RPAREN,
    STATE(224), 1,
      sym_formatting_comment,
    STATE(330), 1,
      sym_offset_clause,
    STATE(502), 1,
      sym_for_clause,
    STATE(822), 1,
      sym_update_clause,
  [6622] = 8,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(150), 1,
      aux_sym_function_expression_token1,
    ACTIONS(152), 1,
      sym_identifier,
    STATE(142), 1,
      sym_dotted_identifier,
    STATE(225), 1,
      sym_formatting_comment,
    STATE(416), 1,
      sym__value_expression,
    STATE(630), 1,
      sym__function_name,
    STATE(151), 2,
      sym_function_expression,
      sym_field_identifier,
  [6648] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(226), 1,
      sym_formatting_comment,
    ACTIONS(592), 7,
      ts_builtin_sym_end,
      aux_sym_returning_clause_token1,
      aux_sym_sosl_using_clause_token1,
      aux_sym_sosl_with_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
  [6664] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(594), 1,
      anon_sym_COMMA,
    STATE(227), 2,
      sym_formatting_comment,
      aux_sym_order_by_clause_repeat1,
    ACTIONS(597), 5,
      anon_sym_RPAREN,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
  [6682] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(576), 1,
      anon_sym_COMMA,
    STATE(227), 1,
      aux_sym_order_by_clause_repeat1,
    STATE(228), 1,
      sym_formatting_comment,
    ACTIONS(599), 5,
      anon_sym_RPAREN,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
  [6702] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(603), 1,
      anon_sym_LPAREN,
    STATE(229), 1,
      sym_formatting_comment,
    ACTIONS(601), 6,
      ts_builtin_sym_end,
      aux_sym_sosl_using_clause_token1,
      aux_sym_sosl_with_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
  [6720] = 8,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(150), 1,
      aux_sym_function_expression_token1,
    ACTIONS(152), 1,
      sym_identifier,
    STATE(142), 1,
      sym_dotted_identifier,
    STATE(230), 1,
      sym_formatting_comment,
    STATE(584), 1,
      sym__value_expression,
    STATE(630), 1,
      sym__function_name,
    STATE(151), 2,
      sym_function_expression,
      sym_field_identifier,
  [6746] = 8,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(150), 1,
      aux_sym_function_expression_token1,
    ACTIONS(152), 1,
      sym_identifier,
    STATE(142), 1,
      sym_dotted_identifier,
    STATE(231), 1,
      sym_formatting_comment,
    STATE(551), 1,
      sym__value_expression,
    STATE(630), 1,
      sym__function_name,
    STATE(151), 2,
      sym_function_expression,
      sym_field_identifier,
  [6772] = 9,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(100), 1,
      aux_sym_offset_clause_token1,
    ACTIONS(605), 1,
      anon_sym_RPAREN,
    STATE(232), 1,
      sym_formatting_comment,
    STATE(282), 1,
      sym_offset_clause,
    STATE(479), 1,
      sym_for_clause,
    STATE(645), 1,
      sym_update_clause,
  [6800] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(233), 1,
      sym_formatting_comment,
    ACTIONS(607), 7,
      anon_sym_RPAREN,
      aux_sym_group_by_clause_token1,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [6816] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(234), 1,
      sym_formatting_comment,
    ACTIONS(609), 6,
      ts_builtin_sym_end,
      aux_sym_sosl_using_clause_token1,
      aux_sym_sosl_with_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
  [6831] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(53), 1,
      anon_sym_DOT,
    STATE(9), 1,
      aux_sym_dotted_identifier_repeat1,
    STATE(235), 1,
      sym_formatting_comment,
    ACTIONS(611), 4,
      anon_sym_COMMA,
      aux_sym_type_of_clause_token2,
      aux_sym_when_expression_token1,
      aux_sym_else_expression_token1,
  [6850] = 8,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(613), 1,
      aux_sym_geo_location_type_token1,
    ACTIONS(615), 1,
      sym_bound_apex_expression,
    ACTIONS(617), 1,
      sym_identifier,
    STATE(142), 1,
      sym_dotted_identifier,
    STATE(236), 1,
      sym_formatting_comment,
    STATE(872), 1,
      sym_field_identifier,
    STATE(925), 1,
      sym_geo_location_type,
  [6875] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(64), 1,
      anon_sym_COMMA,
    ACTIONS(66), 1,
      anon_sym_LPAREN,
    ACTIONS(619), 1,
      anon_sym_DOT,
    STATE(237), 1,
      sym_formatting_comment,
    STATE(335), 1,
      aux_sym_dotted_identifier_repeat1,
    ACTIONS(62), 2,
      aux_sym_from_clause_token1,
      sym_identifier,
  [6898] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(238), 1,
      sym_formatting_comment,
    ACTIONS(621), 6,
      ts_builtin_sym_end,
      aux_sym_sosl_using_clause_token1,
      aux_sym_sosl_with_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
  [6913] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(239), 1,
      sym_formatting_comment,
    STATE(597), 1,
      sym_in_type,
    ACTIONS(623), 5,
      aux_sym_in_type_token1,
      aux_sym_in_type_token2,
      aux_sym_in_type_token3,
      aux_sym_in_type_token4,
      aux_sym_in_type_token5,
  [6930] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(240), 1,
      sym_formatting_comment,
    ACTIONS(625), 6,
      ts_builtin_sym_end,
      aux_sym_sosl_using_clause_token1,
      aux_sym_sosl_with_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
  [6945] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(241), 1,
      sym_formatting_comment,
    ACTIONS(597), 6,
      anon_sym_COMMA,
      anon_sym_RPAREN,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
  [6960] = 6,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(627), 1,
      aux_sym_using_scope_clause_token1,
    ACTIONS(629), 1,
      aux_sym_using_lookup_clause_token1,
    ACTIONS(631), 1,
      aux_sym_using_listview_clause_token1,
    STATE(242), 1,
      sym_formatting_comment,
    STATE(117), 3,
      sym_using_scope_clause,
      sym_using_lookup_clause,
      sym_using_listview_clause,
  [6981] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(243), 1,
      sym_formatting_comment,
    ACTIONS(633), 6,
      anon_sym_COMMA,
      anon_sym_RPAREN,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
  [6996] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(244), 1,
      sym_formatting_comment,
    ACTIONS(635), 6,
      anon_sym_RPAREN,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [7011] = 8,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(502), 1,
      aux_sym_type_of_clause_token2,
    ACTIONS(504), 1,
      aux_sym_when_expression_token1,
    ACTIONS(506), 1,
      aux_sym_else_expression_token1,
    STATE(245), 1,
      sym_formatting_comment,
    STATE(254), 1,
      aux_sym_type_of_clause_repeat1,
    STATE(524), 1,
      sym_when_expression,
    STATE(632), 1,
      sym_else_expression,
  [7036] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(246), 1,
      sym_formatting_comment,
    ACTIONS(637), 6,
      anon_sym_COMMA,
      anon_sym_RPAREN,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
  [7051] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(247), 1,
      sym_formatting_comment,
    ACTIONS(639), 6,
      ts_builtin_sym_end,
      aux_sym_sosl_using_clause_token1,
      aux_sym_sosl_with_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
  [7066] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(248), 1,
      sym_formatting_comment,
    ACTIONS(419), 6,
      anon_sym_COMMA,
      anon_sym_RPAREN,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
  [7081] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(249), 1,
      sym_formatting_comment,
    ACTIONS(641), 6,
      ts_builtin_sym_end,
      aux_sym_sosl_using_clause_token1,
      aux_sym_sosl_with_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
  [7096] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(250), 1,
      sym_formatting_comment,
    ACTIONS(643), 6,
      ts_builtin_sym_end,
      aux_sym_sosl_using_clause_token1,
      aux_sym_sosl_with_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
  [7111] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(251), 1,
      sym_formatting_comment,
    ACTIONS(645), 6,
      ts_builtin_sym_end,
      aux_sym_sosl_using_clause_token1,
      aux_sym_sosl_with_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
  [7126] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(252), 1,
      sym_formatting_comment,
    ACTIONS(647), 6,
      anon_sym_RPAREN,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
      aux_sym_order_by_clause_token1,
  [7141] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(162), 1,
      aux_sym_function_expression_token1,
    ACTIONS(164), 1,
      sym_identifier,
    STATE(13), 1,
      sym_dotted_identifier,
    STATE(253), 1,
      sym_formatting_comment,
    STATE(922), 1,
      sym__function_name,
    STATE(144), 2,
      sym_function_expression,
      sym_field_identifier,
  [7164] = 8,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(504), 1,
      aux_sym_when_expression_token1,
    ACTIONS(506), 1,
      aux_sym_else_expression_token1,
    ACTIONS(649), 1,
      aux_sym_type_of_clause_token2,
    STATE(254), 1,
      sym_formatting_comment,
    STATE(263), 1,
      aux_sym_type_of_clause_repeat1,
    STATE(524), 1,
      sym_when_expression,
    STATE(690), 1,
      sym_else_expression,
  [7189] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(255), 1,
      sym_formatting_comment,
    ACTIONS(651), 6,
      ts_builtin_sym_end,
      aux_sym_sosl_using_clause_token1,
      aux_sym_sosl_with_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
  [7204] = 8,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(613), 1,
      aux_sym_geo_location_type_token1,
    ACTIONS(615), 1,
      sym_bound_apex_expression,
    ACTIONS(617), 1,
      sym_identifier,
    STATE(142), 1,
      sym_dotted_identifier,
    STATE(256), 1,
      sym_formatting_comment,
    STATE(872), 1,
      sym_field_identifier,
    STATE(877), 1,
      sym_geo_location_type,
  [7229] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(257), 1,
      sym_formatting_comment,
    ACTIONS(653), 6,
      ts_builtin_sym_end,
      aux_sym_sosl_using_clause_token1,
      aux_sym_sosl_with_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
  [7244] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(258), 1,
      sym_formatting_comment,
    ACTIONS(655), 6,
      ts_builtin_sym_end,
      aux_sym_sosl_using_clause_token1,
      aux_sym_sosl_with_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
  [7259] = 8,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(613), 1,
      aux_sym_geo_location_type_token1,
    ACTIONS(615), 1,
      sym_bound_apex_expression,
    ACTIONS(617), 1,
      sym_identifier,
    STATE(142), 1,
      sym_dotted_identifier,
    STATE(259), 1,
      sym_formatting_comment,
    STATE(872), 1,
      sym_field_identifier,
    STATE(923), 1,
      sym_geo_location_type,
  [7284] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(657), 1,
      anon_sym_RPAREN,
    STATE(260), 1,
      sym_formatting_comment,
    STATE(379), 1,
      sym_for_clause,
    STATE(776), 1,
      sym_update_clause,
  [7306] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(659), 1,
      anon_sym_RPAREN,
    STATE(261), 1,
      sym_formatting_comment,
    STATE(520), 1,
      sym_for_clause,
    STATE(823), 1,
      sym_update_clause,
  [7328] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(544), 1,
      anon_sym_COMMA,
    STATE(262), 1,
      sym_formatting_comment,
    STATE(334), 1,
      aux_sym_field_list_repeat1,
    ACTIONS(546), 3,
      aux_sym_type_of_clause_token2,
      aux_sym_when_expression_token1,
      aux_sym_else_expression_token1,
  [7346] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(663), 1,
      aux_sym_when_expression_token1,
    STATE(524), 1,
      sym_when_expression,
    ACTIONS(661), 2,
      aux_sym_type_of_clause_token2,
      aux_sym_else_expression_token1,
    STATE(263), 2,
      sym_formatting_comment,
      aux_sym_type_of_clause_repeat1,
  [7364] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(666), 1,
      anon_sym_RPAREN,
    STATE(264), 1,
      sym_formatting_comment,
    STATE(486), 1,
      sym_for_clause,
    STATE(650), 1,
      sym_update_clause,
  [7386] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(100), 1,
      aux_sym_offset_clause_token1,
    ACTIONS(668), 1,
      ts_builtin_sym_end,
    STATE(265), 1,
      sym_formatting_comment,
    STATE(469), 1,
      sym_offset_clause,
    STATE(892), 1,
      sym_update_clause,
  [7408] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(670), 1,
      anon_sym_RPAREN,
    STATE(266), 1,
      sym_formatting_comment,
    STATE(549), 1,
      sym_for_clause,
    STATE(705), 1,
      sym_update_clause,
  [7430] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(672), 1,
      anon_sym_RPAREN,
    STATE(267), 1,
      sym_formatting_comment,
    STATE(372), 1,
      sym_for_clause,
    STATE(735), 1,
      sym_update_clause,
  [7452] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(100), 1,
      aux_sym_offset_clause_token1,
    ACTIONS(166), 1,
      ts_builtin_sym_end,
    STATE(268), 1,
      sym_formatting_comment,
    STATE(558), 1,
      sym_offset_clause,
    STATE(672), 1,
      sym_update_clause,
  [7474] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(674), 1,
      anon_sym_RPAREN,
    STATE(269), 1,
      sym_formatting_comment,
    STATE(375), 1,
      sym_for_clause,
    STATE(747), 1,
      sym_update_clause,
  [7496] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(676), 1,
      anon_sym_RPAREN,
    STATE(270), 1,
      sym_formatting_comment,
    STATE(378), 1,
      sym_for_clause,
    STATE(769), 1,
      sym_update_clause,
  [7518] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(678), 1,
      anon_sym_RPAREN,
    STATE(271), 1,
      sym_formatting_comment,
    STATE(382), 1,
      sym_for_clause,
    STATE(804), 1,
      sym_update_clause,
  [7540] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(680), 1,
      anon_sym_RPAREN,
    STATE(272), 1,
      sym_formatting_comment,
    STATE(385), 1,
      sym_for_clause,
    STATE(827), 1,
      sym_update_clause,
  [7562] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(682), 1,
      anon_sym_RPAREN,
    STATE(273), 1,
      sym_formatting_comment,
    STATE(395), 1,
      sym_for_clause,
    STATE(866), 1,
      sym_update_clause,
  [7584] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(684), 1,
      anon_sym_RPAREN,
    STATE(274), 1,
      sym_formatting_comment,
    STATE(399), 1,
      sym_for_clause,
    STATE(873), 1,
      sym_update_clause,
  [7606] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(98), 1,
      aux_sym_limit_clause_token1,
    ACTIONS(100), 1,
      aux_sym_offset_clause_token1,
    ACTIONS(686), 1,
      anon_sym_RPAREN,
    STATE(275), 1,
      sym_formatting_comment,
    STATE(557), 1,
      sym_limit_clause,
    STATE(780), 1,
      sym_offset_clause,
  [7628] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(688), 1,
      anon_sym_RPAREN,
    STATE(276), 1,
      sym_formatting_comment,
    STATE(402), 1,
      sym_for_clause,
    STATE(880), 1,
      sym_update_clause,
  [7650] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(690), 1,
      anon_sym_RPAREN,
    STATE(277), 1,
      sym_formatting_comment,
    STATE(404), 1,
      sym_for_clause,
    STATE(886), 1,
      sym_update_clause,
  [7672] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(692), 1,
      anon_sym_RPAREN,
    STATE(278), 1,
      sym_formatting_comment,
    STATE(407), 1,
      sym_for_clause,
    STATE(893), 1,
      sym_update_clause,
  [7694] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(694), 1,
      anon_sym_RPAREN,
    STATE(279), 1,
      sym_formatting_comment,
    STATE(410), 1,
      sym_for_clause,
    STATE(903), 1,
      sym_update_clause,
  [7716] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(696), 1,
      anon_sym_RPAREN,
    STATE(280), 1,
      sym_formatting_comment,
    STATE(411), 1,
      sym_for_clause,
    STATE(908), 1,
      sym_update_clause,
  [7738] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(698), 1,
      anon_sym_COMMA,
    STATE(281), 2,
      sym_formatting_comment,
      aux_sym_field_list_repeat1,
    ACTIONS(611), 3,
      aux_sym_type_of_clause_token2,
      aux_sym_when_expression_token1,
      aux_sym_else_expression_token1,
  [7754] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(701), 1,
      anon_sym_RPAREN,
    STATE(282), 1,
      sym_formatting_comment,
    STATE(418), 1,
      sym_for_clause,
    STATE(591), 1,
      sym_update_clause,
  [7776] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(703), 1,
      anon_sym_RPAREN,
    STATE(283), 1,
      sym_formatting_comment,
    STATE(421), 1,
      sym_for_clause,
    STATE(598), 1,
      sym_update_clause,
  [7798] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(705), 1,
      anon_sym_RPAREN,
    STATE(284), 1,
      sym_formatting_comment,
    STATE(423), 1,
      sym_for_clause,
    STATE(604), 1,
      sym_update_clause,
  [7820] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(709), 1,
      aux_sym_with_data_cat_filter_type_token2,
    STATE(285), 1,
      sym_formatting_comment,
    STATE(563), 1,
      sym_with_data_cat_filter_type,
    ACTIONS(707), 3,
      aux_sym_with_data_cat_filter_type_token1,
      aux_sym_with_data_cat_filter_type_token3,
      aux_sym_with_data_cat_filter_type_token4,
  [7838] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(711), 1,
      anon_sym_RPAREN,
    STATE(286), 1,
      sym_formatting_comment,
    STATE(367), 1,
      sym_for_clause,
    STATE(619), 1,
      sym_update_clause,
  [7860] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(713), 1,
      anon_sym_RPAREN,
    STATE(287), 1,
      sym_formatting_comment,
    STATE(434), 1,
      sym_for_clause,
    STATE(626), 1,
      sym_update_clause,
  [7882] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(715), 1,
      anon_sym_RPAREN,
    STATE(288), 1,
      sym_formatting_comment,
    STATE(436), 1,
      sym_for_clause,
    STATE(633), 1,
      sym_update_clause,
  [7904] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(717), 1,
      anon_sym_RPAREN,
    STATE(289), 1,
      sym_formatting_comment,
    STATE(439), 1,
      sym_for_clause,
    STATE(639), 1,
      sym_update_clause,
  [7926] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(719), 1,
      anon_sym_RPAREN,
    STATE(290), 1,
      sym_formatting_comment,
    STATE(441), 1,
      sym_for_clause,
    STATE(646), 1,
      sym_update_clause,
  [7948] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(721), 1,
      anon_sym_RPAREN,
    STATE(291), 1,
      sym_formatting_comment,
    STATE(444), 1,
      sym_for_clause,
    STATE(653), 1,
      sym_update_clause,
  [7970] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(723), 1,
      anon_sym_RPAREN,
    STATE(292), 1,
      sym_formatting_comment,
    STATE(445), 1,
      sym_for_clause,
    STATE(657), 1,
      sym_update_clause,
  [7992] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(725), 1,
      anon_sym_RPAREN,
    STATE(293), 1,
      sym_formatting_comment,
    STATE(446), 1,
      sym_for_clause,
    STATE(662), 1,
      sym_update_clause,
  [8014] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(727), 1,
      anon_sym_RPAREN,
    STATE(294), 1,
      sym_formatting_comment,
    STATE(447), 1,
      sym_for_clause,
    STATE(666), 1,
      sym_update_clause,
  [8036] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(100), 1,
      aux_sym_offset_clause_token1,
    ACTIONS(512), 1,
      ts_builtin_sym_end,
    STATE(295), 1,
      sym_formatting_comment,
    STATE(483), 1,
      sym_offset_clause,
    STATE(879), 1,
      sym_update_clause,
  [8058] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(729), 1,
      anon_sym_RPAREN,
    STATE(296), 1,
      sym_formatting_comment,
    STATE(450), 1,
      sym_for_clause,
    STATE(673), 1,
      sym_update_clause,
  [8080] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(98), 1,
      aux_sym_limit_clause_token1,
    ACTIONS(100), 1,
      aux_sym_offset_clause_token1,
    ACTIONS(586), 1,
      anon_sym_RPAREN,
    STATE(297), 1,
      sym_formatting_comment,
    STATE(387), 1,
      sym_limit_clause,
    STATE(882), 1,
      sym_offset_clause,
  [8102] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(731), 1,
      anon_sym_RPAREN,
    STATE(298), 1,
      sym_formatting_comment,
    STATE(457), 1,
      sym_for_clause,
    STATE(686), 1,
      sym_update_clause,
  [8124] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(733), 1,
      anon_sym_RPAREN,
    STATE(299), 1,
      sym_formatting_comment,
    STATE(452), 1,
      sym_for_clause,
    STATE(723), 1,
      sym_update_clause,
  [8146] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(735), 1,
      anon_sym_RPAREN,
    STATE(300), 1,
      sym_formatting_comment,
    STATE(460), 1,
      sym_for_clause,
    STATE(693), 1,
      sym_update_clause,
  [8168] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(737), 1,
      anon_sym_RPAREN,
    STATE(301), 1,
      sym_formatting_comment,
    STATE(461), 1,
      sym_for_clause,
    STATE(697), 1,
      sym_update_clause,
  [8190] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(739), 1,
      anon_sym_RPAREN,
    STATE(302), 1,
      sym_formatting_comment,
    STATE(462), 1,
      sym_for_clause,
    STATE(701), 1,
      sym_update_clause,
  [8212] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(741), 1,
      anon_sym_RPAREN,
    STATE(303), 1,
      sym_formatting_comment,
    STATE(465), 1,
      sym_for_clause,
    STATE(708), 1,
      sym_update_clause,
  [8234] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(743), 1,
      anon_sym_RPAREN,
    STATE(304), 1,
      sym_formatting_comment,
    STATE(466), 1,
      sym_for_clause,
    STATE(712), 1,
      sym_update_clause,
  [8256] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(745), 1,
      anon_sym_RPAREN,
    STATE(305), 1,
      sym_formatting_comment,
    STATE(538), 1,
      sym_for_clause,
    STATE(772), 1,
      sym_update_clause,
  [8278] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(747), 1,
      anon_sym_RPAREN,
    STATE(306), 1,
      sym_formatting_comment,
    STATE(473), 1,
      sym_for_clause,
    STATE(727), 1,
      sym_update_clause,
  [8300] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(749), 1,
      anon_sym_RPAREN,
    STATE(307), 1,
      sym_formatting_comment,
    STATE(474), 1,
      sym_for_clause,
    STATE(731), 1,
      sym_update_clause,
  [8322] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(751), 1,
      anon_sym_RPAREN,
    STATE(308), 1,
      sym_formatting_comment,
    STATE(477), 1,
      sym_for_clause,
    STATE(738), 1,
      sym_update_clause,
  [8344] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(753), 1,
      anon_sym_RPAREN,
    STATE(309), 1,
      sym_formatting_comment,
    STATE(480), 1,
      sym_for_clause,
    STATE(744), 1,
      sym_update_clause,
  [8366] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(755), 1,
      anon_sym_RPAREN,
    STATE(310), 1,
      sym_formatting_comment,
    STATE(484), 1,
      sym_for_clause,
    STATE(751), 1,
      sym_update_clause,
  [8388] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(757), 1,
      anon_sym_RPAREN,
    STATE(311), 1,
      sym_formatting_comment,
    STATE(487), 1,
      sym_for_clause,
    STATE(758), 1,
      sym_update_clause,
  [8410] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(759), 1,
      anon_sym_RPAREN,
    STATE(312), 1,
      sym_formatting_comment,
    STATE(488), 1,
      sym_for_clause,
    STATE(762), 1,
      sym_update_clause,
  [8432] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(761), 1,
      anon_sym_RPAREN,
    STATE(313), 1,
      sym_formatting_comment,
    STATE(489), 1,
      sym_for_clause,
    STATE(766), 1,
      sym_update_clause,
  [8454] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(763), 1,
      anon_sym_RPAREN,
    STATE(314), 1,
      sym_formatting_comment,
    STATE(392), 1,
      sym_for_clause,
    STATE(876), 1,
      sym_update_clause,
  [8476] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(765), 1,
      anon_sym_RPAREN,
    STATE(315), 1,
      sym_formatting_comment,
    STATE(492), 1,
      sym_for_clause,
    STATE(773), 1,
      sym_update_clause,
  [8498] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(767), 1,
      anon_sym_RPAREN,
    STATE(316), 1,
      sym_formatting_comment,
    STATE(493), 1,
      sym_for_clause,
    STATE(777), 1,
      sym_update_clause,
  [8520] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(769), 1,
      anon_sym_RPAREN,
    STATE(317), 1,
      sym_formatting_comment,
    STATE(494), 1,
      sym_for_clause,
    STATE(784), 1,
      sym_update_clause,
  [8542] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(771), 1,
      anon_sym_RPAREN,
    STATE(318), 1,
      sym_formatting_comment,
    STATE(495), 1,
      sym_for_clause,
    STATE(788), 1,
      sym_update_clause,
  [8564] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(773), 1,
      anon_sym_RPAREN,
    STATE(319), 1,
      sym_formatting_comment,
    STATE(497), 1,
      sym_for_clause,
    STATE(795), 1,
      sym_update_clause,
  [8586] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(775), 1,
      anon_sym_RPAREN,
    STATE(320), 1,
      sym_formatting_comment,
    STATE(498), 1,
      sym_for_clause,
    STATE(799), 1,
      sym_update_clause,
  [8608] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(777), 1,
      anon_sym_RPAREN,
    STATE(321), 1,
      sym_formatting_comment,
    STATE(499), 1,
      sym_for_clause,
    STATE(805), 1,
      sym_update_clause,
  [8630] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(779), 1,
      anon_sym_RPAREN,
    STATE(322), 1,
      sym_formatting_comment,
    STATE(413), 1,
      sym_for_clause,
    STATE(918), 1,
      sym_update_clause,
  [8652] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(100), 1,
      aux_sym_offset_clause_token1,
    ACTIONS(275), 1,
      ts_builtin_sym_end,
    STATE(323), 1,
      sym_formatting_comment,
    STATE(370), 1,
      sym_offset_clause,
    STATE(917), 1,
      sym_update_clause,
  [8674] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(781), 1,
      anon_sym_RPAREN,
    STATE(324), 1,
      sym_formatting_comment,
    STATE(500), 1,
      sym_for_clause,
    STATE(813), 1,
      sym_update_clause,
  [8696] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(783), 1,
      anon_sym_RPAREN,
    STATE(325), 1,
      sym_formatting_comment,
    STATE(501), 1,
      sym_for_clause,
    STATE(817), 1,
      sym_update_clause,
  [8718] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(785), 1,
      anon_sym_RPAREN,
    STATE(326), 1,
      sym_formatting_comment,
    STATE(504), 1,
      sym_for_clause,
    STATE(824), 1,
      sym_update_clause,
  [8740] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(787), 1,
      anon_sym_RPAREN,
    STATE(327), 1,
      sym_formatting_comment,
    STATE(505), 1,
      sym_for_clause,
    STATE(828), 1,
      sym_update_clause,
  [8762] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(789), 1,
      anon_sym_RPAREN,
    STATE(328), 1,
      sym_formatting_comment,
    STATE(506), 1,
      sym_for_clause,
    STATE(834), 1,
      sym_update_clause,
  [8784] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(791), 1,
      anon_sym_RPAREN,
    STATE(329), 1,
      sym_formatting_comment,
    STATE(507), 1,
      sym_for_clause,
    STATE(841), 1,
      sym_update_clause,
  [8806] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(793), 1,
      anon_sym_RPAREN,
    STATE(330), 1,
      sym_formatting_comment,
    STATE(508), 1,
      sym_for_clause,
    STATE(849), 1,
      sym_update_clause,
  [8828] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(795), 1,
      anon_sym_RPAREN,
    STATE(331), 1,
      sym_formatting_comment,
    STATE(426), 1,
      sym_for_clause,
    STATE(590), 1,
      sym_update_clause,
  [8850] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(797), 1,
      anon_sym_RPAREN,
    STATE(332), 1,
      sym_formatting_comment,
    STATE(432), 1,
      sym_for_clause,
    STATE(605), 1,
      sym_update_clause,
  [8872] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(245), 1,
      anon_sym_RPAREN,
    ACTIONS(247), 1,
      aux_sym_and_expression_token1,
    ACTIONS(799), 1,
      aux_sym_or_expression_token1,
    STATE(99), 1,
      aux_sym_and_expression_repeat1,
    STATE(333), 1,
      sym_formatting_comment,
    STATE(529), 1,
      aux_sym_or_expression_repeat1,
  [8894] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(544), 1,
      anon_sym_COMMA,
    STATE(281), 1,
      aux_sym_field_list_repeat1,
    STATE(334), 1,
      sym_formatting_comment,
    ACTIONS(801), 3,
      aux_sym_type_of_clause_token2,
      aux_sym_when_expression_token1,
      aux_sym_else_expression_token1,
  [8912] = 6,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(51), 1,
      anon_sym_COMMA,
    ACTIONS(619), 1,
      anon_sym_DOT,
    STATE(335), 1,
      sym_formatting_comment,
    STATE(336), 1,
      aux_sym_dotted_identifier_repeat1,
    ACTIONS(49), 2,
      aux_sym_from_clause_token1,
      sym_identifier,
  [8932] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(57), 1,
      anon_sym_COMMA,
    ACTIONS(803), 1,
      anon_sym_DOT,
    ACTIONS(55), 2,
      aux_sym_from_clause_token1,
      sym_identifier,
    STATE(336), 2,
      sym_formatting_comment,
      aux_sym_dotted_identifier_repeat1,
  [8950] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(337), 1,
      sym_formatting_comment,
    ACTIONS(806), 5,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
      aux_sym_offset_clause_token1,
  [8964] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(100), 1,
      aux_sym_offset_clause_token1,
    ACTIONS(181), 1,
      ts_builtin_sym_end,
    STATE(338), 1,
      sym_formatting_comment,
    STATE(542), 1,
      sym_offset_clause,
    STATE(737), 1,
      sym_update_clause,
  [8986] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(98), 1,
      aux_sym_limit_clause_token1,
    ACTIONS(100), 1,
      aux_sym_offset_clause_token1,
    ACTIONS(417), 1,
      anon_sym_RPAREN,
    STATE(339), 1,
      sym_formatting_comment,
    STATE(390), 1,
      sym_limit_clause,
    STATE(833), 1,
      sym_offset_clause,
  [9008] = 7,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(92), 1,
      aux_sym_for_clause_token1,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(808), 1,
      anon_sym_RPAREN,
    STATE(340), 1,
      sym_formatting_comment,
    STATE(453), 1,
      sym_for_clause,
    STATE(679), 1,
      sym_update_clause,
  [9030] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(810), 1,
      anon_sym_COMMA,
    STATE(341), 1,
      sym_formatting_comment,
    STATE(364), 1,
      aux_sym_for_clause_repeat1,
    ACTIONS(812), 2,
      anon_sym_RPAREN,
      aux_sym_for_type_token1,
  [9047] = 6,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(7), 1,
      aux_sym_find_clause_token1,
    STATE(19), 1,
      sym_find_clause,
    STATE(342), 1,
      sym_formatting_comment,
    STATE(610), 1,
      sym__query_expression,
    STATE(704), 1,
      sym_sosl_query_body,
  [9066] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(343), 1,
      sym_formatting_comment,
    STATE(902), 1,
      sym_fields_type,
    ACTIONS(814), 3,
      aux_sym_in_type_token1,
      aux_sym_fields_type_token1,
      aux_sym_fields_type_token2,
  [9081] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(344), 1,
      sym_formatting_comment,
    ACTIONS(611), 4,
      anon_sym_COMMA,
      aux_sym_type_of_clause_token2,
      aux_sym_when_expression_token1,
      aux_sym_else_expression_token1,
  [9094] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(816), 1,
      aux_sym_with_record_visibility_param_token1,
    STATE(345), 1,
      sym_formatting_comment,
    STATE(581), 1,
      sym_with_record_visibility_param,
    ACTIONS(818), 2,
      aux_sym_with_record_visibility_param_token2,
      aux_sym_with_record_visibility_param_token3,
  [9111] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(820), 1,
      aux_sym_using_phrase_search_token1,
    ACTIONS(822), 1,
      aux_sym_using_advanced_search_token1,
    STATE(346), 1,
      sym_formatting_comment,
    STATE(356), 2,
      sym_using_phrase_search,
      sym_using_advanced_search,
  [9128] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(824), 1,
      anon_sym_LPAREN,
    ACTIONS(826), 1,
      sym_bound_apex_expression,
    STATE(347), 1,
      sym_formatting_comment,
    STATE(63), 2,
      sym_subquery,
      sym_comparable_list,
  [9145] = 6,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(828), 1,
      sym_identifier,
    STATE(50), 1,
      sym_dotted_identifier,
    STATE(52), 1,
      sym_storage_identifier,
    STATE(71), 1,
      sym_storage_alias,
    STATE(348), 1,
      sym_formatting_comment,
  [9164] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(830), 1,
      anon_sym_COMMA,
    ACTIONS(833), 2,
      anon_sym_RPAREN,
      aux_sym_for_type_token1,
    STATE(349), 2,
      sym_formatting_comment,
      aux_sym_for_clause_repeat1,
  [9179] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(816), 1,
      aux_sym_with_record_visibility_param_token1,
    STATE(350), 1,
      sym_formatting_comment,
    STATE(456), 1,
      sym_with_record_visibility_param,
    ACTIONS(818), 2,
      aux_sym_with_record_visibility_param_token2,
      aux_sym_with_record_visibility_param_token3,
  [9196] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(837), 1,
      anon_sym_COMMA,
    STATE(351), 1,
      sym_formatting_comment,
    STATE(358), 1,
      aux_sym_update_clause_repeat1,
    ACTIONS(835), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [9213] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(352), 1,
      sym_formatting_comment,
    ACTIONS(839), 4,
      ts_builtin_sym_end,
      aux_sym_for_type_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
  [9226] = 6,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(841), 1,
      sym_bound_apex_expression,
    ACTIONS(843), 1,
      sym_identifier,
    STATE(142), 1,
      sym_dotted_identifier,
    STATE(353), 1,
      sym_formatting_comment,
    STATE(929), 1,
      sym_field_identifier,
  [9245] = 6,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(828), 1,
      sym_identifier,
    STATE(48), 1,
      sym_storage_identifier,
    STATE(50), 1,
      sym_dotted_identifier,
    STATE(69), 1,
      sym_storage_alias,
    STATE(354), 1,
      sym_formatting_comment,
  [9264] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(355), 1,
      sym_formatting_comment,
    ACTIONS(845), 4,
      ts_builtin_sym_end,
      aux_sym_for_type_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
  [9277] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(356), 1,
      sym_formatting_comment,
    ACTIONS(847), 4,
      ts_builtin_sym_end,
      aux_sym_for_type_token1,
      aux_sym_limit_clause_token1,
      aux_sym_offset_clause_token1,
  [9290] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(357), 1,
      sym_formatting_comment,
    ACTIONS(55), 2,
      aux_sym_from_clause_token1,
      sym_identifier,
    ACTIONS(57), 2,
      anon_sym_COMMA,
      anon_sym_DOT,
  [9305] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(851), 1,
      anon_sym_COMMA,
    ACTIONS(849), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
    STATE(358), 2,
      sym_formatting_comment,
      aux_sym_update_clause_repeat1,
  [9320] = 6,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(843), 1,
      sym_identifier,
    ACTIONS(854), 1,
      sym_bound_apex_expression,
    STATE(142), 1,
      sym_dotted_identifier,
    STATE(359), 1,
      sym_formatting_comment,
    STATE(636), 1,
      sym_field_identifier,
  [9339] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(360), 1,
      sym_formatting_comment,
    STATE(561), 1,
      sym_for_type,
    ACTIONS(856), 3,
      aux_sym_for_type_token1,
      aux_sym_for_type_token2,
      aux_sym_for_type_token3,
  [9354] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(361), 1,
      sym_formatting_comment,
    ACTIONS(858), 4,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
      aux_sym_for_clause_token1,
      aux_sym_for_type_token1,
  [9367] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(860), 1,
      anon_sym_LPAREN,
    ACTIONS(862), 1,
      sym_bound_apex_expression,
    STATE(362), 1,
      sym_formatting_comment,
    STATE(519), 2,
      sym_subquery,
      sym_comparable_list,
  [9384] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(341), 1,
      sym_for_type,
    STATE(363), 1,
      sym_formatting_comment,
    ACTIONS(856), 3,
      aux_sym_for_type_token1,
      aux_sym_for_type_token2,
      aux_sym_for_type_token3,
  [9399] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(810), 1,
      anon_sym_COMMA,
    STATE(349), 1,
      aux_sym_for_clause_repeat1,
    STATE(364), 1,
      sym_formatting_comment,
    ACTIONS(864), 2,
      anon_sym_RPAREN,
      aux_sym_for_type_token1,
  [9416] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(837), 1,
      anon_sym_COMMA,
    STATE(351), 1,
      aux_sym_update_clause_repeat1,
    STATE(365), 1,
      sym_formatting_comment,
    ACTIONS(866), 2,
      ts_builtin_sym_end,
      anon_sym_RPAREN,
  [9433] = 6,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(843), 1,
      sym_identifier,
    ACTIONS(868), 1,
      sym_bound_apex_expression,
    STATE(142), 1,
      sym_dotted_identifier,
    STATE(366), 1,
      sym_formatting_comment,
    STATE(928), 1,
      sym_field_identifier,
  [9452] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(870), 1,
      anon_sym_RPAREN,
    STATE(367), 1,
      sym_formatting_comment,
    STATE(752), 1,
      sym_update_clause,
  [9468] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(64), 1,
      anon_sym_COMMA,
    ACTIONS(277), 1,
      anon_sym_DOT,
    STATE(101), 1,
      aux_sym_dotted_identifier_repeat1,
    STATE(368), 1,
      sym_formatting_comment,
  [9484] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(872), 1,
      anon_sym_COMMA,
    ACTIONS(874), 1,
      anon_sym_RPAREN,
    STATE(369), 1,
      sym_formatting_comment,
    STATE(455), 1,
      aux_sym_comparable_list_repeat1,
  [9500] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(512), 1,
      ts_builtin_sym_end,
    STATE(370), 1,
      sym_formatting_comment,
    STATE(879), 1,
      sym_update_clause,
  [9516] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(876), 1,
      anon_sym_RPAREN,
    STATE(371), 1,
      sym_formatting_comment,
    STATE(644), 1,
      sym_update_clause,
  [9532] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(878), 1,
      anon_sym_RPAREN,
    STATE(372), 1,
      sym_formatting_comment,
    STATE(647), 1,
      sym_update_clause,
  [9548] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(880), 1,
      anon_sym_RPAREN,
    STATE(373), 1,
      sym_formatting_comment,
    STATE(648), 1,
      sym_update_clause,
  [9564] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(882), 1,
      anon_sym_RPAREN,
    STATE(374), 1,
      sym_formatting_comment,
    STATE(670), 1,
      sym_update_clause,
  [9580] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(884), 1,
      anon_sym_RPAREN,
    STATE(375), 1,
      sym_formatting_comment,
    STATE(654), 1,
      sym_update_clause,
  [9596] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(886), 1,
      anon_sym_RPAREN,
    STATE(376), 1,
      sym_formatting_comment,
    STATE(655), 1,
      sym_update_clause,
  [9612] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(888), 1,
      anon_sym_RPAREN,
    STATE(377), 1,
      sym_formatting_comment,
    STATE(861), 1,
      sym_update_clause,
  [9628] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(890), 1,
      anon_sym_RPAREN,
    STATE(378), 1,
      sym_formatting_comment,
    STATE(658), 1,
      sym_update_clause,
  [9644] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(892), 1,
      anon_sym_RPAREN,
    STATE(379), 1,
      sym_formatting_comment,
    STATE(659), 1,
      sym_update_clause,
  [9660] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(894), 1,
      anon_sym_RPAREN,
    STATE(380), 1,
      sym_formatting_comment,
    STATE(660), 1,
      sym_update_clause,
  [9676] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(166), 1,
      ts_builtin_sym_end,
    STATE(381), 1,
      sym_formatting_comment,
    STATE(672), 1,
      sym_update_clause,
  [9692] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(896), 1,
      anon_sym_RPAREN,
    STATE(382), 1,
      sym_formatting_comment,
    STATE(663), 1,
      sym_update_clause,
  [9708] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(898), 1,
      anon_sym_RPAREN,
    STATE(383), 1,
      sym_formatting_comment,
    STATE(664), 1,
      sym_update_clause,
  [9724] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(900), 1,
      anon_sym_COMMA,
    ACTIONS(902), 1,
      aux_sym_from_clause_token1,
    STATE(384), 1,
      sym_formatting_comment,
    STATE(523), 1,
      aux_sym_selected_fields_repeat1,
  [9740] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(904), 1,
      anon_sym_RPAREN,
    STATE(385), 1,
      sym_formatting_comment,
    STATE(667), 1,
      sym_update_clause,
  [9756] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(906), 1,
      anon_sym_RPAREN,
    STATE(386), 1,
      sym_formatting_comment,
    STATE(668), 1,
      sym_update_clause,
  [9772] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(100), 1,
      aux_sym_offset_clause_token1,
    ACTIONS(686), 1,
      anon_sym_RPAREN,
    STATE(387), 1,
      sym_formatting_comment,
    STATE(780), 1,
      sym_offset_clause,
  [9788] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(908), 1,
      anon_sym_RPAREN,
    STATE(388), 1,
      sym_formatting_comment,
    STATE(871), 1,
      sym_update_clause,
  [9804] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(910), 1,
      anon_sym_RPAREN,
    STATE(389), 1,
      sym_formatting_comment,
    STATE(674), 1,
      sym_update_clause,
  [9820] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(100), 1,
      aux_sym_offset_clause_token1,
    ACTIONS(586), 1,
      anon_sym_RPAREN,
    STATE(390), 1,
      sym_formatting_comment,
    STATE(882), 1,
      sym_offset_clause,
  [9836] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(912), 1,
      anon_sym_COMMA,
    ACTIONS(915), 1,
      anon_sym_RPAREN,
    STATE(391), 2,
      sym_formatting_comment,
      aux_sym_with_data_cat_filter_repeat1,
  [9850] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(917), 1,
      anon_sym_RPAREN,
    STATE(392), 1,
      sym_formatting_comment,
    STATE(874), 1,
      sym_update_clause,
  [9866] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(919), 1,
      anon_sym_RPAREN,
    STATE(393), 1,
      sym_formatting_comment,
    STATE(684), 1,
      sym_update_clause,
  [9882] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(921), 1,
      anon_sym_RPAREN,
    STATE(394), 1,
      sym_formatting_comment,
    STATE(875), 1,
      sym_update_clause,
  [9898] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(923), 1,
      anon_sym_RPAREN,
    STATE(395), 1,
      sym_formatting_comment,
    STATE(687), 1,
      sym_update_clause,
  [9914] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(925), 1,
      anon_sym_RPAREN,
    STATE(396), 1,
      sym_formatting_comment,
    STATE(688), 1,
      sym_update_clause,
  [9930] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(365), 1,
      sym_update_type,
    STATE(397), 1,
      sym_formatting_comment,
    ACTIONS(927), 2,
      aux_sym_update_type_token1,
      aux_sym_update_type_token2,
  [9944] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(929), 1,
      anon_sym_RPAREN,
    STATE(398), 1,
      sym_formatting_comment,
    STATE(640), 1,
      sym_update_clause,
  [9960] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(931), 1,
      anon_sym_RPAREN,
    STATE(399), 1,
      sym_formatting_comment,
    STATE(694), 1,
      sym_update_clause,
  [9976] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(933), 1,
      anon_sym_RPAREN,
    STATE(400), 1,
      sym_formatting_comment,
    STATE(695), 1,
      sym_update_clause,
  [9992] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(935), 1,
      anon_sym_RPAREN,
    STATE(401), 1,
      sym_formatting_comment,
    STATE(881), 1,
      sym_update_clause,
  [10008] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(937), 1,
      anon_sym_RPAREN,
    STATE(402), 1,
      sym_formatting_comment,
    STATE(698), 1,
      sym_update_clause,
  [10024] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(939), 1,
      anon_sym_RPAREN,
    STATE(403), 1,
      sym_formatting_comment,
    STATE(699), 1,
      sym_update_clause,
  [10040] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(941), 1,
      anon_sym_RPAREN,
    STATE(404), 1,
      sym_formatting_comment,
    STATE(702), 1,
      sym_update_clause,
  [10056] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(943), 1,
      anon_sym_RPAREN,
    STATE(405), 1,
      sym_formatting_comment,
    STATE(703), 1,
      sym_update_clause,
  [10072] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(945), 1,
      anon_sym_RPAREN,
    STATE(406), 1,
      sym_formatting_comment,
    STATE(722), 1,
      sym_update_clause,
  [10088] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(947), 1,
      anon_sym_RPAREN,
    STATE(407), 1,
      sym_formatting_comment,
    STATE(709), 1,
      sym_update_clause,
  [10104] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(949), 1,
      anon_sym_RPAREN,
    STATE(408), 1,
      sym_formatting_comment,
    STATE(710), 1,
      sym_update_clause,
  [10120] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(951), 1,
      anon_sym_RPAREN,
    STATE(409), 1,
      sym_formatting_comment,
    STATE(891), 1,
      sym_update_clause,
  [10136] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(953), 1,
      anon_sym_RPAREN,
    STATE(410), 1,
      sym_formatting_comment,
    STATE(713), 1,
      sym_update_clause,
  [10152] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(955), 1,
      anon_sym_RPAREN,
    STATE(411), 1,
      sym_formatting_comment,
    STATE(714), 1,
      sym_update_clause,
  [10168] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(957), 1,
      anon_sym_RPAREN,
    STATE(412), 1,
      sym_formatting_comment,
    STATE(741), 1,
      sym_update_clause,
  [10184] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(959), 1,
      anon_sym_RPAREN,
    STATE(413), 1,
      sym_formatting_comment,
    STATE(894), 1,
      sym_update_clause,
  [10200] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(414), 1,
      sym_formatting_comment,
    STATE(580), 1,
      sym_boolean,
    ACTIONS(11), 2,
      aux_sym_boolean_token1,
      aux_sym_boolean_token2,
  [10214] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(961), 1,
      anon_sym_RPAREN,
    STATE(415), 1,
      sym_formatting_comment,
    STATE(895), 1,
      sym_update_clause,
  [10230] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(963), 1,
      anon_sym_COMMA,
    ACTIONS(965), 1,
      anon_sym_RPAREN,
    STATE(416), 1,
      sym_formatting_comment,
    STATE(420), 1,
      aux_sym_function_expression_repeat1,
  [10246] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(967), 1,
      anon_sym_COMMA,
    ACTIONS(969), 1,
      anon_sym_RPAREN,
    STATE(417), 1,
      sym_formatting_comment,
    STATE(472), 1,
      aux_sym_with_record_visibility_expression_repeat1,
  [10262] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(971), 1,
      anon_sym_RPAREN,
    STATE(418), 1,
      sym_formatting_comment,
    STATE(724), 1,
      sym_update_clause,
  [10278] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(973), 1,
      anon_sym_RPAREN,
    STATE(419), 1,
      sym_formatting_comment,
    STATE(725), 1,
      sym_update_clause,
  [10294] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(963), 1,
      anon_sym_COMMA,
    ACTIONS(975), 1,
      anon_sym_RPAREN,
    STATE(420), 1,
      sym_formatting_comment,
    STATE(471), 1,
      aux_sym_function_expression_repeat1,
  [10310] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(977), 1,
      anon_sym_RPAREN,
    STATE(421), 1,
      sym_formatting_comment,
    STATE(728), 1,
      sym_update_clause,
  [10326] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(979), 1,
      anon_sym_RPAREN,
    STATE(422), 1,
      sym_formatting_comment,
    STATE(729), 1,
      sym_update_clause,
  [10342] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(981), 1,
      anon_sym_RPAREN,
    STATE(423), 1,
      sym_formatting_comment,
    STATE(732), 1,
      sym_update_clause,
  [10358] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(983), 1,
      anon_sym_RPAREN,
    STATE(424), 1,
      sym_formatting_comment,
    STATE(733), 1,
      sym_update_clause,
  [10374] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(425), 1,
      sym_formatting_comment,
    STATE(540), 1,
      sym_update_type,
    ACTIONS(927), 2,
      aux_sym_update_type_token1,
      aux_sym_update_type_token2,
  [10388] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(985), 1,
      anon_sym_RPAREN,
    STATE(426), 1,
      sym_formatting_comment,
    STATE(904), 1,
      sym_update_clause,
  [10404] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(987), 1,
      anon_sym_RPAREN,
    STATE(427), 1,
      sym_formatting_comment,
    STATE(739), 1,
      sym_update_clause,
  [10420] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(989), 1,
      anon_sym_RPAREN,
    STATE(428), 1,
      sym_formatting_comment,
    STATE(856), 1,
      sym_update_clause,
  [10436] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(991), 1,
      anon_sym_RPAREN,
    STATE(429), 1,
      sym_formatting_comment,
    STATE(905), 1,
      sym_update_clause,
  [10452] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(993), 1,
      anon_sym_RPAREN,
    STATE(430), 1,
      sym_formatting_comment,
    STATE(749), 1,
      sym_update_clause,
  [10468] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(995), 1,
      anon_sym_RPAREN,
    STATE(431), 1,
      sym_formatting_comment,
    STATE(753), 1,
      sym_update_clause,
  [10484] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(997), 1,
      anon_sym_RPAREN,
    STATE(432), 1,
      sym_formatting_comment,
    STATE(911), 1,
      sym_update_clause,
  [10500] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(433), 1,
      sym_formatting_comment,
    ACTIONS(999), 3,
      aux_sym_type_of_clause_token2,
      aux_sym_when_expression_token1,
      aux_sym_else_expression_token1,
  [10512] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(1001), 1,
      anon_sym_RPAREN,
    STATE(434), 1,
      sym_formatting_comment,
    STATE(759), 1,
      sym_update_clause,
  [10528] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(1003), 1,
      anon_sym_RPAREN,
    STATE(435), 1,
      sym_formatting_comment,
    STATE(760), 1,
      sym_update_clause,
  [10544] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(1005), 1,
      anon_sym_RPAREN,
    STATE(436), 1,
      sym_formatting_comment,
    STATE(763), 1,
      sym_update_clause,
  [10560] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(1007), 1,
      anon_sym_RPAREN,
    STATE(437), 1,
      sym_formatting_comment,
    STATE(764), 1,
      sym_update_clause,
  [10576] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(1009), 1,
      anon_sym_RPAREN,
    STATE(438), 1,
      sym_formatting_comment,
    STATE(745), 1,
      sym_update_clause,
  [10592] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(1011), 1,
      anon_sym_RPAREN,
    STATE(439), 1,
      sym_formatting_comment,
    STATE(767), 1,
      sym_update_clause,
  [10608] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(1013), 1,
      anon_sym_RPAREN,
    STATE(440), 1,
      sym_formatting_comment,
    STATE(768), 1,
      sym_update_clause,
  [10624] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(1015), 1,
      anon_sym_RPAREN,
    STATE(441), 1,
      sym_formatting_comment,
    STATE(774), 1,
      sym_update_clause,
  [10640] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(1017), 1,
      anon_sym_RPAREN,
    STATE(442), 1,
      sym_formatting_comment,
    STATE(775), 1,
      sym_update_clause,
  [10656] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(443), 1,
      sym_formatting_comment,
    ACTIONS(1019), 3,
      ts_builtin_sym_end,
      anon_sym_COMMA,
      anon_sym_RPAREN,
  [10668] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(1021), 1,
      anon_sym_RPAREN,
    STATE(444), 1,
      sym_formatting_comment,
    STATE(778), 1,
      sym_update_clause,
  [10684] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(1023), 1,
      anon_sym_RPAREN,
    STATE(445), 1,
      sym_formatting_comment,
    STATE(779), 1,
      sym_update_clause,
  [10700] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(1025), 1,
      anon_sym_RPAREN,
    STATE(446), 1,
      sym_formatting_comment,
    STATE(934), 1,
      sym_update_clause,
  [10716] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(1027), 1,
      anon_sym_RPAREN,
    STATE(447), 1,
      sym_formatting_comment,
    STATE(781), 1,
      sym_update_clause,
  [10732] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(1029), 1,
      anon_sym_RPAREN,
    STATE(448), 1,
      sym_formatting_comment,
    STATE(782), 1,
      sym_update_clause,
  [10748] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1031), 1,
      anon_sym_COMMA,
    ACTIONS(1034), 1,
      anon_sym_RPAREN,
    STATE(449), 2,
      sym_formatting_comment,
      aux_sym_comparable_list_repeat1,
  [10762] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(1036), 1,
      anon_sym_RPAREN,
    STATE(450), 1,
      sym_formatting_comment,
    STATE(785), 1,
      sym_update_clause,
  [10778] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(1038), 1,
      anon_sym_RPAREN,
    STATE(451), 1,
      sym_formatting_comment,
    STATE(786), 1,
      sym_update_clause,
  [10794] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(1040), 1,
      anon_sym_RPAREN,
    STATE(452), 1,
      sym_formatting_comment,
    STATE(750), 1,
      sym_update_clause,
  [10810] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(1042), 1,
      anon_sym_RPAREN,
    STATE(453), 1,
      sym_formatting_comment,
    STATE(789), 1,
      sym_update_clause,
  [10826] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(1044), 1,
      anon_sym_RPAREN,
    STATE(454), 1,
      sym_formatting_comment,
    STATE(790), 1,
      sym_update_clause,
  [10842] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(872), 1,
      anon_sym_COMMA,
    ACTIONS(1046), 1,
      anon_sym_RPAREN,
    STATE(449), 1,
      aux_sym_comparable_list_repeat1,
    STATE(455), 1,
      sym_formatting_comment,
  [10858] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(967), 1,
      anon_sym_COMMA,
    ACTIONS(1048), 1,
      anon_sym_RPAREN,
    STATE(417), 1,
      aux_sym_with_record_visibility_expression_repeat1,
    STATE(456), 1,
      sym_formatting_comment,
  [10874] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(1050), 1,
      anon_sym_RPAREN,
    STATE(457), 1,
      sym_formatting_comment,
    STATE(796), 1,
      sym_update_clause,
  [10890] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(1052), 1,
      anon_sym_RPAREN,
    STATE(458), 1,
      sym_formatting_comment,
    STATE(797), 1,
      sym_update_clause,
  [10906] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(249), 1,
      sym_boolean,
    STATE(459), 1,
      sym_formatting_comment,
    ACTIONS(11), 2,
      aux_sym_boolean_token1,
      aux_sym_boolean_token2,
  [10920] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(1054), 1,
      anon_sym_RPAREN,
    STATE(460), 1,
      sym_formatting_comment,
    STATE(800), 1,
      sym_update_clause,
  [10936] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(1056), 1,
      anon_sym_RPAREN,
    STATE(461), 1,
      sym_formatting_comment,
    STATE(801), 1,
      sym_update_clause,
  [10952] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(1058), 1,
      anon_sym_RPAREN,
    STATE(462), 1,
      sym_formatting_comment,
    STATE(802), 1,
      sym_update_clause,
  [10968] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(1060), 1,
      anon_sym_RPAREN,
    STATE(463), 1,
      sym_formatting_comment,
    STATE(803), 1,
      sym_update_clause,
  [10984] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1062), 1,
      anon_sym_COMMA,
    ACTIONS(1064), 1,
      anon_sym_RPAREN,
    STATE(391), 1,
      aux_sym_with_data_cat_filter_repeat1,
    STATE(464), 1,
      sym_formatting_comment,
  [11000] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(1066), 1,
      anon_sym_RPAREN,
    STATE(465), 1,
      sym_formatting_comment,
    STATE(806), 1,
      sym_update_clause,
  [11016] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(1068), 1,
      anon_sym_RPAREN,
    STATE(466), 1,
      sym_formatting_comment,
    STATE(807), 1,
      sym_update_clause,
  [11032] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1062), 1,
      anon_sym_COMMA,
    ACTIONS(1070), 1,
      anon_sym_RPAREN,
    STATE(464), 1,
      aux_sym_with_data_cat_filter_repeat1,
    STATE(467), 1,
      sym_formatting_comment,
  [11048] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(1072), 1,
      anon_sym_RPAREN,
    STATE(468), 1,
      sym_formatting_comment,
    STATE(896), 1,
      sym_update_clause,
  [11064] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(1074), 1,
      ts_builtin_sym_end,
    STATE(469), 1,
      sym_formatting_comment,
    STATE(600), 1,
      sym_update_clause,
  [11080] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(1076), 1,
      anon_sym_RPAREN,
    STATE(470), 1,
      sym_formatting_comment,
    STATE(754), 1,
      sym_update_clause,
  [11096] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1078), 1,
      anon_sym_COMMA,
    ACTIONS(1081), 1,
      anon_sym_RPAREN,
    STATE(471), 2,
      sym_formatting_comment,
      aux_sym_function_expression_repeat1,
  [11110] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1083), 1,
      anon_sym_COMMA,
    ACTIONS(1086), 1,
      anon_sym_RPAREN,
    STATE(472), 2,
      sym_formatting_comment,
      aux_sym_with_record_visibility_expression_repeat1,
  [11124] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(1088), 1,
      anon_sym_RPAREN,
    STATE(473), 1,
      sym_formatting_comment,
    STATE(809), 1,
      sym_update_clause,
  [11140] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(1090), 1,
      anon_sym_RPAREN,
    STATE(474), 1,
      sym_formatting_comment,
    STATE(810), 1,
      sym_update_clause,
  [11156] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(1092), 1,
      anon_sym_RPAREN,
    STATE(475), 1,
      sym_formatting_comment,
    STATE(811), 1,
      sym_update_clause,
  [11172] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1094), 1,
      sym_identifier,
    STATE(262), 1,
      sym_dotted_identifier,
    STATE(476), 1,
      sym_formatting_comment,
    STATE(680), 1,
      sym_field_list,
  [11188] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(1096), 1,
      anon_sym_RPAREN,
    STATE(477), 1,
      sym_formatting_comment,
    STATE(814), 1,
      sym_update_clause,
  [11204] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(1098), 1,
      anon_sym_RPAREN,
    STATE(478), 1,
      sym_formatting_comment,
    STATE(815), 1,
      sym_update_clause,
  [11220] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(1100), 1,
      anon_sym_RPAREN,
    STATE(479), 1,
      sym_formatting_comment,
    STATE(589), 1,
      sym_update_clause,
  [11236] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(1102), 1,
      anon_sym_RPAREN,
    STATE(480), 1,
      sym_formatting_comment,
    STATE(818), 1,
      sym_update_clause,
  [11252] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(1104), 1,
      anon_sym_RPAREN,
    STATE(481), 1,
      sym_formatting_comment,
    STATE(819), 1,
      sym_update_clause,
  [11268] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(1106), 1,
      anon_sym_RPAREN,
    STATE(482), 1,
      sym_formatting_comment,
    STATE(931), 1,
      sym_update_clause,
  [11284] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(668), 1,
      ts_builtin_sym_end,
    STATE(483), 1,
      sym_formatting_comment,
    STATE(892), 1,
      sym_update_clause,
  [11300] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(1108), 1,
      anon_sym_RPAREN,
    STATE(484), 1,
      sym_formatting_comment,
    STATE(825), 1,
      sym_update_clause,
  [11316] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(1110), 1,
      anon_sym_RPAREN,
    STATE(485), 1,
      sym_formatting_comment,
    STATE(826), 1,
      sym_update_clause,
  [11332] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(1112), 1,
      anon_sym_RPAREN,
    STATE(486), 1,
      sym_formatting_comment,
    STATE(592), 1,
      sym_update_clause,
  [11348] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(1114), 1,
      anon_sym_RPAREN,
    STATE(487), 1,
      sym_formatting_comment,
    STATE(829), 1,
      sym_update_clause,
  [11364] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(1116), 1,
      anon_sym_RPAREN,
    STATE(488), 1,
      sym_formatting_comment,
    STATE(830), 1,
      sym_update_clause,
  [11380] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(1118), 1,
      anon_sym_RPAREN,
    STATE(489), 1,
      sym_formatting_comment,
    STATE(831), 1,
      sym_update_clause,
  [11396] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(1120), 1,
      anon_sym_RPAREN,
    STATE(490), 1,
      sym_formatting_comment,
    STATE(832), 1,
      sym_update_clause,
  [11412] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(1122), 1,
      anon_sym_RPAREN,
    STATE(491), 1,
      sym_formatting_comment,
    STATE(593), 1,
      sym_update_clause,
  [11428] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(1124), 1,
      anon_sym_RPAREN,
    STATE(492), 1,
      sym_formatting_comment,
    STATE(835), 1,
      sym_update_clause,
  [11444] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(1126), 1,
      anon_sym_RPAREN,
    STATE(493), 1,
      sym_formatting_comment,
    STATE(836), 1,
      sym_update_clause,
  [11460] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(1128), 1,
      anon_sym_RPAREN,
    STATE(494), 1,
      sym_formatting_comment,
    STATE(837), 1,
      sym_update_clause,
  [11476] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(1130), 1,
      anon_sym_RPAREN,
    STATE(495), 1,
      sym_formatting_comment,
    STATE(838), 1,
      sym_update_clause,
  [11492] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(1132), 1,
      anon_sym_RPAREN,
    STATE(496), 1,
      sym_formatting_comment,
    STATE(839), 1,
      sym_update_clause,
  [11508] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(1134), 1,
      anon_sym_RPAREN,
    STATE(497), 1,
      sym_formatting_comment,
    STATE(842), 1,
      sym_update_clause,
  [11524] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(1136), 1,
      anon_sym_RPAREN,
    STATE(498), 1,
      sym_formatting_comment,
    STATE(843), 1,
      sym_update_clause,
  [11540] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(1138), 1,
      anon_sym_RPAREN,
    STATE(499), 1,
      sym_formatting_comment,
    STATE(844), 1,
      sym_update_clause,
  [11556] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(1140), 1,
      anon_sym_RPAREN,
    STATE(500), 1,
      sym_formatting_comment,
    STATE(846), 1,
      sym_update_clause,
  [11572] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(1142), 1,
      anon_sym_RPAREN,
    STATE(501), 1,
      sym_formatting_comment,
    STATE(847), 1,
      sym_update_clause,
  [11588] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(1144), 1,
      anon_sym_RPAREN,
    STATE(502), 1,
      sym_formatting_comment,
    STATE(848), 1,
      sym_update_clause,
  [11604] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(1146), 1,
      anon_sym_RPAREN,
    STATE(503), 1,
      sym_formatting_comment,
    STATE(770), 1,
      sym_update_clause,
  [11620] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(1148), 1,
      anon_sym_RPAREN,
    STATE(504), 1,
      sym_formatting_comment,
    STATE(850), 1,
      sym_update_clause,
  [11636] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(1150), 1,
      anon_sym_RPAREN,
    STATE(505), 1,
      sym_formatting_comment,
    STATE(851), 1,
      sym_update_clause,
  [11652] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(1152), 1,
      anon_sym_RPAREN,
    STATE(506), 1,
      sym_formatting_comment,
    STATE(852), 1,
      sym_update_clause,
  [11668] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(1154), 1,
      anon_sym_RPAREN,
    STATE(507), 1,
      sym_formatting_comment,
    STATE(853), 1,
      sym_update_clause,
  [11684] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(1156), 1,
      anon_sym_RPAREN,
    STATE(508), 1,
      sym_formatting_comment,
    STATE(854), 1,
      sym_update_clause,
  [11700] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(509), 1,
      sym_formatting_comment,
    ACTIONS(255), 3,
      anon_sym_RPAREN,
      aux_sym_and_expression_token1,
      aux_sym_or_expression_token1,
  [11712] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(1158), 1,
      anon_sym_RPAREN,
    STATE(510), 1,
      sym_formatting_comment,
    STATE(599), 1,
      sym_update_clause,
  [11728] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(448), 1,
      anon_sym_COMMA,
    ACTIONS(473), 1,
      aux_sym_from_clause_token1,
    ACTIONS(475), 1,
      sym_identifier,
    STATE(511), 1,
      sym_formatting_comment,
  [11744] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(82), 1,
      anon_sym_COMMA,
    STATE(512), 1,
      sym_formatting_comment,
    ACTIONS(80), 2,
      aux_sym_from_clause_token1,
      sym_identifier,
  [11758] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(64), 1,
      anon_sym_COMMA,
    STATE(513), 1,
      sym_formatting_comment,
    ACTIONS(62), 2,
      aux_sym_from_clause_token1,
      sym_identifier,
  [11772] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(514), 1,
      sym_formatting_comment,
    ACTIONS(193), 3,
      anon_sym_RPAREN,
      aux_sym_and_expression_token1,
      aux_sym_or_expression_token1,
  [11784] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(515), 1,
      sym_formatting_comment,
    ACTIONS(189), 3,
      anon_sym_RPAREN,
      aux_sym_and_expression_token1,
      aux_sym_or_expression_token1,
  [11796] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(516), 1,
      sym_formatting_comment,
    ACTIONS(227), 3,
      anon_sym_RPAREN,
      aux_sym_and_expression_token1,
      aux_sym_or_expression_token1,
  [11808] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(517), 1,
      sym_formatting_comment,
    ACTIONS(185), 3,
      anon_sym_RPAREN,
      aux_sym_and_expression_token1,
      aux_sym_or_expression_token1,
  [11820] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(518), 1,
      sym_formatting_comment,
    ACTIONS(197), 3,
      anon_sym_RPAREN,
      aux_sym_and_expression_token1,
      aux_sym_or_expression_token1,
  [11832] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(519), 1,
      sym_formatting_comment,
    ACTIONS(251), 3,
      anon_sym_RPAREN,
      aux_sym_and_expression_token1,
      aux_sym_or_expression_token1,
  [11844] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(1160), 1,
      anon_sym_RPAREN,
    STATE(520), 1,
      sym_formatting_comment,
    STATE(594), 1,
      sym_update_clause,
  [11860] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(1162), 1,
      anon_sym_RPAREN,
    STATE(521), 1,
      sym_formatting_comment,
    STATE(595), 1,
      sym_update_clause,
  [11876] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(522), 1,
      sym_formatting_comment,
    ACTIONS(154), 3,
      anon_sym_RPAREN,
      aux_sym_and_expression_token1,
      aux_sym_or_expression_token1,
  [11888] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(900), 1,
      anon_sym_COMMA,
    ACTIONS(1164), 1,
      aux_sym_from_clause_token1,
    STATE(523), 1,
      sym_formatting_comment,
    STATE(526), 1,
      aux_sym_selected_fields_repeat1,
  [11904] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(524), 1,
      sym_formatting_comment,
    ACTIONS(1166), 3,
      aux_sym_type_of_clause_token2,
      aux_sym_when_expression_token1,
      aux_sym_else_expression_token1,
  [11916] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(525), 1,
      sym_formatting_comment,
    ACTIONS(336), 3,
      anon_sym_RPAREN,
      aux_sym_and_expression_token1,
      aux_sym_or_expression_token1,
  [11928] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(485), 1,
      aux_sym_from_clause_token1,
    ACTIONS(1168), 1,
      anon_sym_COMMA,
    STATE(526), 2,
      sym_formatting_comment,
      aux_sym_selected_fields_repeat1,
  [11942] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(527), 1,
      sym_formatting_comment,
    ACTIONS(201), 3,
      anon_sym_RPAREN,
      aux_sym_and_expression_token1,
      aux_sym_or_expression_token1,
  [11954] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(528), 1,
      sym_formatting_comment,
    ACTIONS(269), 3,
      anon_sym_RPAREN,
      aux_sym_and_expression_token1,
      aux_sym_or_expression_token1,
  [11966] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(357), 1,
      anon_sym_RPAREN,
    ACTIONS(799), 1,
      aux_sym_or_expression_token1,
    STATE(529), 1,
      sym_formatting_comment,
    STATE(534), 1,
      aux_sym_or_expression_repeat1,
  [11982] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(530), 1,
      sym_formatting_comment,
    ACTIONS(326), 3,
      anon_sym_RPAREN,
      aux_sym_and_expression_token1,
      aux_sym_or_expression_token1,
  [11994] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(70), 1,
      anon_sym_COMMA,
    STATE(531), 1,
      sym_formatting_comment,
    ACTIONS(68), 2,
      aux_sym_from_clause_token1,
      sym_identifier,
  [12008] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(532), 1,
      sym_formatting_comment,
    ACTIONS(238), 3,
      anon_sym_RPAREN,
      aux_sym_and_expression_token1,
      aux_sym_or_expression_token1,
  [12020] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(533), 1,
      sym_formatting_comment,
    ACTIONS(322), 3,
      anon_sym_RPAREN,
      aux_sym_and_expression_token1,
      aux_sym_or_expression_token1,
  [12032] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(340), 1,
      anon_sym_RPAREN,
    ACTIONS(1171), 1,
      aux_sym_or_expression_token1,
    STATE(534), 2,
      sym_formatting_comment,
      aux_sym_or_expression_repeat1,
  [12046] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(74), 1,
      anon_sym_COMMA,
    STATE(535), 1,
      sym_formatting_comment,
    ACTIONS(72), 2,
      aux_sym_from_clause_token1,
      sym_identifier,
  [12060] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(78), 1,
      anon_sym_COMMA,
    STATE(536), 1,
      sym_formatting_comment,
    ACTIONS(76), 2,
      aux_sym_from_clause_token1,
      sym_identifier,
  [12074] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1094), 1,
      sym_identifier,
    STATE(262), 1,
      sym_dotted_identifier,
    STATE(433), 1,
      sym_field_list,
    STATE(537), 1,
      sym_formatting_comment,
  [12090] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(1174), 1,
      anon_sym_RPAREN,
    STATE(538), 1,
      sym_formatting_comment,
    STATE(783), 1,
      sym_update_clause,
  [12106] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(1176), 1,
      anon_sym_RPAREN,
    STATE(539), 1,
      sym_formatting_comment,
    STATE(614), 1,
      sym_update_clause,
  [12122] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(540), 1,
      sym_formatting_comment,
    ACTIONS(849), 3,
      ts_builtin_sym_end,
      anon_sym_COMMA,
      anon_sym_RPAREN,
  [12134] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(1178), 1,
      anon_sym_RPAREN,
    STATE(541), 1,
      sym_formatting_comment,
    STATE(787), 1,
      sym_update_clause,
  [12150] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(275), 1,
      ts_builtin_sym_end,
    STATE(542), 1,
      sym_formatting_comment,
    STATE(917), 1,
      sym_update_clause,
  [12166] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(1180), 1,
      anon_sym_RPAREN,
    STATE(543), 1,
      sym_formatting_comment,
    STATE(624), 1,
      sym_update_clause,
  [12182] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(9), 1,
      aux_sym_select_clause_token1,
    STATE(544), 1,
      sym_formatting_comment,
    STATE(562), 1,
      sym_select_clause,
    STATE(621), 1,
      sym_soql_query_body,
  [12198] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(1182), 1,
      anon_sym_RPAREN,
    STATE(545), 1,
      sym_formatting_comment,
    STATE(678), 1,
      sym_update_clause,
  [12214] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(872), 1,
      anon_sym_COMMA,
    ACTIONS(1184), 1,
      anon_sym_RPAREN,
    STATE(546), 1,
      sym_formatting_comment,
    STATE(548), 1,
      aux_sym_comparable_list_repeat1,
  [12230] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(963), 1,
      anon_sym_COMMA,
    ACTIONS(1186), 1,
      anon_sym_RPAREN,
    STATE(547), 1,
      sym_formatting_comment,
    STATE(550), 1,
      aux_sym_function_expression_repeat1,
  [12246] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(872), 1,
      anon_sym_COMMA,
    ACTIONS(1188), 1,
      anon_sym_RPAREN,
    STATE(449), 1,
      aux_sym_comparable_list_repeat1,
    STATE(548), 1,
      sym_formatting_comment,
  [12262] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(1190), 1,
      anon_sym_RPAREN,
    STATE(549), 1,
      sym_formatting_comment,
    STATE(627), 1,
      sym_update_clause,
  [12278] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(963), 1,
      anon_sym_COMMA,
    ACTIONS(1192), 1,
      anon_sym_RPAREN,
    STATE(471), 1,
      aux_sym_function_expression_repeat1,
    STATE(550), 1,
      sym_formatting_comment,
  [12294] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(963), 1,
      anon_sym_COMMA,
    ACTIONS(1194), 1,
      anon_sym_RPAREN,
    STATE(551), 1,
      sym_formatting_comment,
    STATE(552), 1,
      aux_sym_function_expression_repeat1,
  [12310] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(963), 1,
      anon_sym_COMMA,
    ACTIONS(1196), 1,
      anon_sym_RPAREN,
    STATE(471), 1,
      aux_sym_function_expression_repeat1,
    STATE(552), 1,
      sym_formatting_comment,
  [12326] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(1198), 1,
      anon_sym_RPAREN,
    STATE(553), 1,
      sym_formatting_comment,
    STATE(628), 1,
      sym_update_clause,
  [12342] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(100), 1,
      aux_sym_offset_clause_token1,
    ACTIONS(417), 1,
      anon_sym_RPAREN,
    STATE(554), 1,
      sym_formatting_comment,
    STATE(833), 1,
      sym_offset_clause,
  [12358] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(1200), 1,
      anon_sym_RPAREN,
    STATE(555), 1,
      sym_formatting_comment,
    STATE(692), 1,
      sym_update_clause,
  [12374] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(556), 1,
      sym_formatting_comment,
    ACTIONS(1202), 3,
      anon_sym_COMMA,
      anon_sym_RPAREN,
      aux_sym_for_type_token1,
  [12386] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(100), 1,
      aux_sym_offset_clause_token1,
    ACTIONS(1204), 1,
      anon_sym_RPAREN,
    STATE(557), 1,
      sym_formatting_comment,
    STATE(620), 1,
      sym_offset_clause,
  [12402] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(181), 1,
      ts_builtin_sym_end,
    STATE(558), 1,
      sym_formatting_comment,
    STATE(737), 1,
      sym_update_clause,
  [12418] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(1206), 1,
      anon_sym_RPAREN,
    STATE(559), 1,
      sym_formatting_comment,
    STATE(812), 1,
      sym_update_clause,
  [12434] = 5,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(94), 1,
      aux_sym_for_type_token1,
    ACTIONS(1208), 1,
      anon_sym_RPAREN,
    STATE(560), 1,
      sym_formatting_comment,
    STATE(634), 1,
      sym_update_clause,
  [12450] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(561), 1,
      sym_formatting_comment,
    ACTIONS(833), 3,
      anon_sym_COMMA,
      anon_sym_RPAREN,
      aux_sym_for_type_token1,
  [12462] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1210), 1,
      aux_sym_from_clause_token1,
    STATE(18), 1,
      sym_from_clause,
    STATE(562), 1,
      sym_formatting_comment,
  [12475] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1212), 1,
      anon_sym_LPAREN,
    ACTIONS(1214), 1,
      sym_identifier,
    STATE(563), 1,
      sym_formatting_comment,
  [12488] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1216), 1,
      sym_identifier,
    STATE(79), 1,
      sym_using_lookup_bind_expression,
    STATE(564), 1,
      sym_formatting_comment,
  [12501] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1216), 1,
      sym_identifier,
    STATE(106), 1,
      sym_using_lookup_bind_expression,
    STATE(565), 1,
      sym_formatting_comment,
  [12514] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(566), 1,
      sym_formatting_comment,
    ACTIONS(1218), 2,
      sym_bound_apex_expression,
      sym_string_literal,
  [12525] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1220), 1,
      sym_identifier,
    STATE(141), 1,
      sym_sobject_return,
    STATE(567), 1,
      sym_formatting_comment,
  [12538] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1222), 1,
      sym_term_separator_start,
    ACTIONS(1224), 1,
      sym_bound_apex_expression,
    STATE(568), 1,
      sym_formatting_comment,
  [12551] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1226), 1,
      sym_identifier,
    STATE(245), 1,
      sym_dotted_identifier,
    STATE(569), 1,
      sym_formatting_comment,
  [12564] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(53), 1,
      anon_sym_DOT,
    STATE(9), 1,
      aux_sym_dotted_identifier_repeat1,
    STATE(570), 1,
      sym_formatting_comment,
  [12577] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(571), 1,
      sym_formatting_comment,
    ACTIONS(1228), 2,
      sym_bound_apex_expression,
      sym_decimal,
  [12588] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(572), 1,
      sym_formatting_comment,
    ACTIONS(1230), 2,
      sym_bound_apex_expression,
      sym_int,
  [12599] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(573), 1,
      sym_formatting_comment,
    ACTIONS(1232), 2,
      sym_bound_apex_expression,
      sym_decimal,
  [12610] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(574), 1,
      sym_formatting_comment,
    ACTIONS(1234), 2,
      anon_sym_LPAREN,
      sym_identifier,
  [12621] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1220), 1,
      sym_identifier,
    STATE(158), 1,
      sym_sobject_return,
    STATE(575), 1,
      sym_formatting_comment,
  [12634] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1236), 1,
      sym_identifier,
    STATE(87), 1,
      sym_dotted_identifier,
    STATE(576), 1,
      sym_formatting_comment,
  [12647] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(577), 1,
      sym_formatting_comment,
    ACTIONS(1238), 2,
      sym_bound_apex_expression,
      sym_int,
  [12658] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(578), 1,
      sym_formatting_comment,
    ACTIONS(340), 2,
      anon_sym_RPAREN,
      aux_sym_or_expression_token1,
  [12669] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1240), 1,
      sym_identifier,
    STATE(68), 1,
      sym_with_data_cat_filter,
    STATE(579), 1,
      sym_formatting_comment,
  [12682] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(580), 1,
      sym_formatting_comment,
    ACTIONS(1242), 2,
      anon_sym_COMMA,
      anon_sym_RPAREN,
  [12693] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(581), 1,
      sym_formatting_comment,
    ACTIONS(1086), 2,
      anon_sym_COMMA,
      anon_sym_RPAREN,
  [12704] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(582), 1,
      sym_formatting_comment,
    ACTIONS(1244), 2,
      anon_sym_LPAREN,
      sym_bound_apex_expression,
  [12715] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1246), 1,
      sym_identifier,
    STATE(344), 1,
      sym_dotted_identifier,
    STATE(583), 1,
      sym_formatting_comment,
  [12728] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(584), 1,
      sym_formatting_comment,
    ACTIONS(1081), 2,
      anon_sym_COMMA,
      anon_sym_RPAREN,
  [12739] = 4,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1240), 1,
      sym_identifier,
    STATE(89), 1,
      sym_with_data_cat_filter,
    STATE(585), 1,
      sym_formatting_comment,
  [12752] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(586), 1,
      sym_formatting_comment,
    ACTIONS(1034), 2,
      anon_sym_COMMA,
      anon_sym_RPAREN,
  [12763] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    STATE(587), 1,
      sym_formatting_comment,
    ACTIONS(915), 2,
      anon_sym_COMMA,
      anon_sym_RPAREN,
  [12774] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1248), 1,
      sym_string_literal,
    STATE(588), 1,
      sym_formatting_comment,
  [12784] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1250), 1,
      anon_sym_RPAREN,
    STATE(589), 1,
      sym_formatting_comment,
  [12794] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1252), 1,
      anon_sym_RPAREN,
    STATE(590), 1,
      sym_formatting_comment,
  [12804] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1254), 1,
      anon_sym_RPAREN,
    STATE(591), 1,
      sym_formatting_comment,
  [12814] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1256), 1,
      anon_sym_RPAREN,
    STATE(592), 1,
      sym_formatting_comment,
  [12824] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1258), 1,
      anon_sym_RPAREN,
    STATE(593), 1,
      sym_formatting_comment,
  [12834] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1260), 1,
      anon_sym_RPAREN,
    STATE(594), 1,
      sym_formatting_comment,
  [12844] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1262), 1,
      anon_sym_RPAREN,
    STATE(595), 1,
      sym_formatting_comment,
  [12854] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1264), 1,
      anon_sym_RPAREN,
    STATE(596), 1,
      sym_formatting_comment,
  [12864] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1266), 1,
      aux_sym_in_clause_token2,
    STATE(597), 1,
      sym_formatting_comment,
  [12874] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1268), 1,
      anon_sym_RPAREN,
    STATE(598), 1,
      sym_formatting_comment,
  [12884] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1270), 1,
      anon_sym_RPAREN,
    STATE(599), 1,
      sym_formatting_comment,
  [12894] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1272), 1,
      ts_builtin_sym_end,
    STATE(600), 1,
      sym_formatting_comment,
  [12904] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1274), 1,
      anon_sym_RPAREN,
    STATE(601), 1,
      sym_formatting_comment,
  [12914] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1276), 1,
      anon_sym_RPAREN,
    STATE(602), 1,
      sym_formatting_comment,
  [12924] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1278), 1,
      aux_sym_from_clause_token1,
    STATE(603), 1,
      sym_formatting_comment,
  [12934] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1280), 1,
      anon_sym_RPAREN,
    STATE(604), 1,
      sym_formatting_comment,
  [12944] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1282), 1,
      anon_sym_RPAREN,
    STATE(605), 1,
      sym_formatting_comment,
  [12954] = 3,
    ACTIONS(1284), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1286), 1,
      sym_term,
    STATE(606), 1,
      sym_formatting_comment,
  [12964] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1288), 1,
      sym_identifier,
    STATE(607), 1,
      sym_formatting_comment,
  [12974] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1290), 1,
      anon_sym_RPAREN,
    STATE(608), 1,
      sym_formatting_comment,
  [12984] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1292), 1,
      ts_builtin_sym_end,
    STATE(609), 1,
      sym_formatting_comment,
  [12994] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1294), 1,
      ts_builtin_sym_end,
    STATE(610), 1,
      sym_formatting_comment,
  [13004] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1296), 1,
      sym_identifier,
    STATE(611), 1,
      sym_formatting_comment,
  [13014] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1298), 1,
      anon_sym_RPAREN,
    STATE(612), 1,
      sym_formatting_comment,
  [13024] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1300), 1,
      sym_string_literal,
    STATE(613), 1,
      sym_formatting_comment,
  [13034] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1302), 1,
      anon_sym_RPAREN,
    STATE(614), 1,
      sym_formatting_comment,
  [13044] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1304), 1,
      anon_sym_EQ,
    STATE(615), 1,
      sym_formatting_comment,
  [13054] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1306), 1,
      anon_sym_EQ,
    STATE(616), 1,
      sym_formatting_comment,
  [13064] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1308), 1,
      anon_sym_RPAREN,
    STATE(617), 1,
      sym_formatting_comment,
  [13074] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1310), 1,
      anon_sym_LPAREN,
    STATE(618), 1,
      sym_formatting_comment,
  [13084] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1312), 1,
      anon_sym_RPAREN,
    STATE(619), 1,
      sym_formatting_comment,
  [13094] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1314), 1,
      anon_sym_RPAREN,
    STATE(620), 1,
      sym_formatting_comment,
  [13104] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1316), 1,
      anon_sym_RPAREN,
    STATE(621), 1,
      sym_formatting_comment,
  [13114] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1318), 1,
      anon_sym_EQ,
    STATE(622), 1,
      sym_formatting_comment,
  [13124] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1320), 1,
      anon_sym_RPAREN,
    STATE(623), 1,
      sym_formatting_comment,
  [13134] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1322), 1,
      anon_sym_RPAREN,
    STATE(624), 1,
      sym_formatting_comment,
  [13144] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1324), 1,
      anon_sym_EQ,
    STATE(625), 1,
      sym_formatting_comment,
  [13154] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1326), 1,
      anon_sym_RPAREN,
    STATE(626), 1,
      sym_formatting_comment,
  [13164] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1328), 1,
      anon_sym_RPAREN,
    STATE(627), 1,
      sym_formatting_comment,
  [13174] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1330), 1,
      anon_sym_RPAREN,
    STATE(628), 1,
      sym_formatting_comment,
  [13184] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1332), 1,
      sym_string_literal,
    STATE(629), 1,
      sym_formatting_comment,
  [13194] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1334), 1,
      anon_sym_LPAREN,
    STATE(630), 1,
      sym_formatting_comment,
  [13204] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1336), 1,
      anon_sym_RPAREN,
    STATE(631), 1,
      sym_formatting_comment,
  [13214] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(649), 1,
      aux_sym_type_of_clause_token2,
    STATE(632), 1,
      sym_formatting_comment,
  [13224] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1338), 1,
      anon_sym_RPAREN,
    STATE(633), 1,
      sym_formatting_comment,
  [13234] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1340), 1,
      anon_sym_RPAREN,
    STATE(634), 1,
      sym_formatting_comment,
  [13244] = 3,
    ACTIONS(1284), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1342), 1,
      aux_sym_header_comment_token1,
    STATE(635), 1,
      sym_formatting_comment,
  [13254] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1344), 1,
      anon_sym_COMMA,
    STATE(636), 1,
      sym_formatting_comment,
  [13264] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1346), 1,
      anon_sym_RPAREN,
    STATE(637), 1,
      sym_formatting_comment,
  [13274] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1348), 1,
      sym_string_literal,
    STATE(638), 1,
      sym_formatting_comment,
  [13284] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1350), 1,
      anon_sym_RPAREN,
    STATE(639), 1,
      sym_formatting_comment,
  [13294] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1352), 1,
      anon_sym_RPAREN,
    STATE(640), 1,
      sym_formatting_comment,
  [13304] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1354), 1,
      aux_sym_with_snippet_expression_token2,
    STATE(641), 1,
      sym_formatting_comment,
  [13314] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1356), 1,
      sym_identifier,
    STATE(642), 1,
      sym_formatting_comment,
  [13324] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1358), 1,
      anon_sym_RPAREN,
    STATE(643), 1,
      sym_formatting_comment,
  [13334] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1360), 1,
      anon_sym_RPAREN,
    STATE(644), 1,
      sym_formatting_comment,
  [13344] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1362), 1,
      anon_sym_RPAREN,
    STATE(645), 1,
      sym_formatting_comment,
  [13354] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1364), 1,
      anon_sym_RPAREN,
    STATE(646), 1,
      sym_formatting_comment,
  [13364] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1366), 1,
      anon_sym_RPAREN,
    STATE(647), 1,
      sym_formatting_comment,
  [13374] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1368), 1,
      anon_sym_RPAREN,
    STATE(648), 1,
      sym_formatting_comment,
  [13384] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1370), 1,
      anon_sym_RPAREN,
    STATE(649), 1,
      sym_formatting_comment,
  [13394] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1372), 1,
      anon_sym_RPAREN,
    STATE(650), 1,
      sym_formatting_comment,
  [13404] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1374), 1,
      anon_sym_RPAREN,
    STATE(651), 1,
      sym_formatting_comment,
  [13414] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1376), 1,
      ts_builtin_sym_end,
    STATE(652), 1,
      sym_formatting_comment,
  [13424] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1378), 1,
      anon_sym_RPAREN,
    STATE(653), 1,
      sym_formatting_comment,
  [13434] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1380), 1,
      anon_sym_RPAREN,
    STATE(654), 1,
      sym_formatting_comment,
  [13444] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1382), 1,
      anon_sym_RPAREN,
    STATE(655), 1,
      sym_formatting_comment,
  [13454] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1384), 1,
      aux_sym_when_expression_token2,
    STATE(656), 1,
      sym_formatting_comment,
  [13464] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1386), 1,
      anon_sym_RPAREN,
    STATE(657), 1,
      sym_formatting_comment,
  [13474] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1388), 1,
      anon_sym_RPAREN,
    STATE(658), 1,
      sym_formatting_comment,
  [13484] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1390), 1,
      anon_sym_RPAREN,
    STATE(659), 1,
      sym_formatting_comment,
  [13494] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1392), 1,
      anon_sym_RPAREN,
    STATE(660), 1,
      sym_formatting_comment,
  [13504] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1394), 1,
      anon_sym_RPAREN,
    STATE(661), 1,
      sym_formatting_comment,
  [13514] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1396), 1,
      anon_sym_RPAREN,
    STATE(662), 1,
      sym_formatting_comment,
  [13524] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1398), 1,
      anon_sym_RPAREN,
    STATE(663), 1,
      sym_formatting_comment,
  [13534] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1400), 1,
      anon_sym_RPAREN,
    STATE(664), 1,
      sym_formatting_comment,
  [13544] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1402), 1,
      anon_sym_RPAREN,
    STATE(665), 1,
      sym_formatting_comment,
  [13554] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1404), 1,
      anon_sym_RPAREN,
    STATE(666), 1,
      sym_formatting_comment,
  [13564] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1406), 1,
      anon_sym_RPAREN,
    STATE(667), 1,
      sym_formatting_comment,
  [13574] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1408), 1,
      anon_sym_RPAREN,
    STATE(668), 1,
      sym_formatting_comment,
  [13584] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1410), 1,
      anon_sym_RPAREN,
    STATE(669), 1,
      sym_formatting_comment,
  [13594] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1412), 1,
      anon_sym_RPAREN,
    STATE(670), 1,
      sym_formatting_comment,
  [13604] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1414), 1,
      anon_sym_RPAREN,
    STATE(671), 1,
      sym_formatting_comment,
  [13614] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(181), 1,
      ts_builtin_sym_end,
    STATE(672), 1,
      sym_formatting_comment,
  [13624] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1416), 1,
      anon_sym_RPAREN,
    STATE(673), 1,
      sym_formatting_comment,
  [13634] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1418), 1,
      anon_sym_RPAREN,
    STATE(674), 1,
      sym_formatting_comment,
  [13644] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1420), 1,
      anon_sym_RPAREN,
    STATE(675), 1,
      sym_formatting_comment,
  [13654] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1422), 1,
      anon_sym_EQ,
    STATE(676), 1,
      sym_formatting_comment,
  [13664] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1424), 1,
      anon_sym_RPAREN,
    STATE(677), 1,
      sym_formatting_comment,
  [13674] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1426), 1,
      anon_sym_RPAREN,
    STATE(678), 1,
      sym_formatting_comment,
  [13684] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1428), 1,
      anon_sym_RPAREN,
    STATE(679), 1,
      sym_formatting_comment,
  [13694] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1430), 1,
      aux_sym_type_of_clause_token2,
    STATE(680), 1,
      sym_formatting_comment,
  [13704] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1432), 1,
      anon_sym_EQ,
    STATE(681), 1,
      sym_formatting_comment,
  [13714] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1434), 1,
      anon_sym_RPAREN,
    STATE(682), 1,
      sym_formatting_comment,
  [13724] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1436), 1,
      anon_sym_RPAREN,
    STATE(683), 1,
      sym_formatting_comment,
  [13734] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1438), 1,
      anon_sym_RPAREN,
    STATE(684), 1,
      sym_formatting_comment,
  [13744] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1440), 1,
      anon_sym_RPAREN,
    STATE(685), 1,
      sym_formatting_comment,
  [13754] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1442), 1,
      anon_sym_RPAREN,
    STATE(686), 1,
      sym_formatting_comment,
  [13764] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1444), 1,
      anon_sym_RPAREN,
    STATE(687), 1,
      sym_formatting_comment,
  [13774] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1446), 1,
      anon_sym_RPAREN,
    STATE(688), 1,
      sym_formatting_comment,
  [13784] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1448), 1,
      anon_sym_RPAREN,
    STATE(689), 1,
      sym_formatting_comment,
  [13794] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1450), 1,
      aux_sym_type_of_clause_token2,
    STATE(690), 1,
      sym_formatting_comment,
  [13804] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1452), 1,
      anon_sym_RPAREN,
    STATE(691), 1,
      sym_formatting_comment,
  [13814] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1454), 1,
      anon_sym_RPAREN,
    STATE(692), 1,
      sym_formatting_comment,
  [13824] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1456), 1,
      anon_sym_RPAREN,
    STATE(693), 1,
      sym_formatting_comment,
  [13834] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1458), 1,
      anon_sym_RPAREN,
    STATE(694), 1,
      sym_formatting_comment,
  [13844] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1460), 1,
      anon_sym_RPAREN,
    STATE(695), 1,
      sym_formatting_comment,
  [13854] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1462), 1,
      anon_sym_COLON,
    STATE(696), 1,
      sym_formatting_comment,
  [13864] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1464), 1,
      anon_sym_RPAREN,
    STATE(697), 1,
      sym_formatting_comment,
  [13874] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1466), 1,
      anon_sym_RPAREN,
    STATE(698), 1,
      sym_formatting_comment,
  [13884] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1468), 1,
      anon_sym_RPAREN,
    STATE(699), 1,
      sym_formatting_comment,
  [13894] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1470), 1,
      anon_sym_RPAREN,
    STATE(700), 1,
      sym_formatting_comment,
  [13904] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1472), 1,
      anon_sym_RPAREN,
    STATE(701), 1,
      sym_formatting_comment,
  [13914] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1474), 1,
      anon_sym_RPAREN,
    STATE(702), 1,
      sym_formatting_comment,
  [13924] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1476), 1,
      anon_sym_RPAREN,
    STATE(703), 1,
      sym_formatting_comment,
  [13934] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1478), 1,
      ts_builtin_sym_end,
    STATE(704), 1,
      sym_formatting_comment,
  [13944] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1480), 1,
      anon_sym_RPAREN,
    STATE(705), 1,
      sym_formatting_comment,
  [13954] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1482), 1,
      anon_sym_RPAREN,
    STATE(706), 1,
      sym_formatting_comment,
  [13964] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1484), 1,
      anon_sym_EQ,
    STATE(707), 1,
      sym_formatting_comment,
  [13974] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1486), 1,
      anon_sym_RPAREN,
    STATE(708), 1,
      sym_formatting_comment,
  [13984] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1488), 1,
      anon_sym_RPAREN,
    STATE(709), 1,
      sym_formatting_comment,
  [13994] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1490), 1,
      anon_sym_RPAREN,
    STATE(710), 1,
      sym_formatting_comment,
  [14004] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1492), 1,
      anon_sym_RPAREN,
    STATE(711), 1,
      sym_formatting_comment,
  [14014] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1494), 1,
      anon_sym_RPAREN,
    STATE(712), 1,
      sym_formatting_comment,
  [14024] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1496), 1,
      anon_sym_RPAREN,
    STATE(713), 1,
      sym_formatting_comment,
  [14034] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1498), 1,
      anon_sym_RPAREN,
    STATE(714), 1,
      sym_formatting_comment,
  [14044] = 3,
    ACTIONS(1284), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1500), 1,
      aux_sym_header_comment_token1,
    STATE(715), 1,
      sym_formatting_comment,
  [14054] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1502), 1,
      anon_sym_RPAREN,
    STATE(716), 1,
      sym_formatting_comment,
  [14064] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1504), 1,
      anon_sym_EQ,
    STATE(717), 1,
      sym_formatting_comment,
  [14074] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1506), 1,
      anon_sym_RPAREN,
    STATE(718), 1,
      sym_formatting_comment,
  [14084] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1508), 1,
      anon_sym_RPAREN,
    STATE(719), 1,
      sym_formatting_comment,
  [14094] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1510), 1,
      anon_sym_RPAREN,
    STATE(720), 1,
      sym_formatting_comment,
  [14104] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(417), 1,
      anon_sym_RPAREN,
    STATE(721), 1,
      sym_formatting_comment,
  [14114] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1512), 1,
      anon_sym_RPAREN,
    STATE(722), 1,
      sym_formatting_comment,
  [14124] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1514), 1,
      anon_sym_RPAREN,
    STATE(723), 1,
      sym_formatting_comment,
  [14134] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1516), 1,
      anon_sym_RPAREN,
    STATE(724), 1,
      sym_formatting_comment,
  [14144] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1518), 1,
      anon_sym_RPAREN,
    STATE(725), 1,
      sym_formatting_comment,
  [14154] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1520), 1,
      sym_identifier,
    STATE(726), 1,
      sym_formatting_comment,
  [14164] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1522), 1,
      anon_sym_RPAREN,
    STATE(727), 1,
      sym_formatting_comment,
  [14174] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1524), 1,
      anon_sym_RPAREN,
    STATE(728), 1,
      sym_formatting_comment,
  [14184] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1526), 1,
      anon_sym_RPAREN,
    STATE(729), 1,
      sym_formatting_comment,
  [14194] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1528), 1,
      anon_sym_RPAREN,
    STATE(730), 1,
      sym_formatting_comment,
  [14204] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1530), 1,
      anon_sym_RPAREN,
    STATE(731), 1,
      sym_formatting_comment,
  [14214] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1532), 1,
      anon_sym_RPAREN,
    STATE(732), 1,
      sym_formatting_comment,
  [14224] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1534), 1,
      anon_sym_RPAREN,
    STATE(733), 1,
      sym_formatting_comment,
  [14234] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1536), 1,
      anon_sym_RPAREN,
    STATE(734), 1,
      sym_formatting_comment,
  [14244] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1538), 1,
      anon_sym_RPAREN,
    STATE(735), 1,
      sym_formatting_comment,
  [14254] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1540), 1,
      anon_sym_RPAREN,
    STATE(736), 1,
      sym_formatting_comment,
  [14264] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(275), 1,
      ts_builtin_sym_end,
    STATE(737), 1,
      sym_formatting_comment,
  [14274] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1542), 1,
      anon_sym_RPAREN,
    STATE(738), 1,
      sym_formatting_comment,
  [14284] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1544), 1,
      anon_sym_RPAREN,
    STATE(739), 1,
      sym_formatting_comment,
  [14294] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1546), 1,
      anon_sym_RPAREN,
    STATE(740), 1,
      sym_formatting_comment,
  [14304] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1548), 1,
      anon_sym_RPAREN,
    STATE(741), 1,
      sym_formatting_comment,
  [14314] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1550), 1,
      anon_sym_RPAREN,
    STATE(742), 1,
      sym_formatting_comment,
  [14324] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1552), 1,
      anon_sym_RPAREN,
    STATE(743), 1,
      sym_formatting_comment,
  [14334] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1554), 1,
      anon_sym_RPAREN,
    STATE(744), 1,
      sym_formatting_comment,
  [14344] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1556), 1,
      anon_sym_RPAREN,
    STATE(745), 1,
      sym_formatting_comment,
  [14354] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(166), 1,
      ts_builtin_sym_end,
    STATE(746), 1,
      sym_formatting_comment,
  [14364] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1558), 1,
      anon_sym_RPAREN,
    STATE(747), 1,
      sym_formatting_comment,
  [14374] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1560), 1,
      anon_sym_RPAREN,
    STATE(748), 1,
      sym_formatting_comment,
  [14384] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1562), 1,
      anon_sym_RPAREN,
    STATE(749), 1,
      sym_formatting_comment,
  [14394] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1564), 1,
      anon_sym_RPAREN,
    STATE(750), 1,
      sym_formatting_comment,
  [14404] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1566), 1,
      anon_sym_RPAREN,
    STATE(751), 1,
      sym_formatting_comment,
  [14414] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1568), 1,
      anon_sym_RPAREN,
    STATE(752), 1,
      sym_formatting_comment,
  [14424] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1570), 1,
      anon_sym_RPAREN,
    STATE(753), 1,
      sym_formatting_comment,
  [14434] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1572), 1,
      anon_sym_RPAREN,
    STATE(754), 1,
      sym_formatting_comment,
  [14444] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1574), 1,
      anon_sym_RPAREN,
    STATE(755), 1,
      sym_formatting_comment,
  [14454] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1576), 1,
      anon_sym_RPAREN,
    STATE(756), 1,
      sym_formatting_comment,
  [14464] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1578), 1,
      anon_sym_RPAREN,
    STATE(757), 1,
      sym_formatting_comment,
  [14474] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1580), 1,
      anon_sym_RPAREN,
    STATE(758), 1,
      sym_formatting_comment,
  [14484] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1582), 1,
      anon_sym_RPAREN,
    STATE(759), 1,
      sym_formatting_comment,
  [14494] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1584), 1,
      anon_sym_RPAREN,
    STATE(760), 1,
      sym_formatting_comment,
  [14504] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1586), 1,
      anon_sym_RPAREN,
    STATE(761), 1,
      sym_formatting_comment,
  [14514] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1588), 1,
      anon_sym_RPAREN,
    STATE(762), 1,
      sym_formatting_comment,
  [14524] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1590), 1,
      anon_sym_RPAREN,
    STATE(763), 1,
      sym_formatting_comment,
  [14534] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1592), 1,
      anon_sym_RPAREN,
    STATE(764), 1,
      sym_formatting_comment,
  [14544] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1594), 1,
      anon_sym_EQ,
    STATE(765), 1,
      sym_formatting_comment,
  [14554] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1596), 1,
      anon_sym_RPAREN,
    STATE(766), 1,
      sym_formatting_comment,
  [14564] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1598), 1,
      anon_sym_RPAREN,
    STATE(767), 1,
      sym_formatting_comment,
  [14574] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1600), 1,
      anon_sym_RPAREN,
    STATE(768), 1,
      sym_formatting_comment,
  [14584] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1602), 1,
      anon_sym_RPAREN,
    STATE(769), 1,
      sym_formatting_comment,
  [14594] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1604), 1,
      anon_sym_RPAREN,
    STATE(770), 1,
      sym_formatting_comment,
  [14604] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1606), 1,
      anon_sym_RPAREN,
    STATE(771), 1,
      sym_formatting_comment,
  [14614] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1608), 1,
      anon_sym_RPAREN,
    STATE(772), 1,
      sym_formatting_comment,
  [14624] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1610), 1,
      anon_sym_RPAREN,
    STATE(773), 1,
      sym_formatting_comment,
  [14634] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1612), 1,
      anon_sym_RPAREN,
    STATE(774), 1,
      sym_formatting_comment,
  [14644] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1614), 1,
      anon_sym_RPAREN,
    STATE(775), 1,
      sym_formatting_comment,
  [14654] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1616), 1,
      anon_sym_RPAREN,
    STATE(776), 1,
      sym_formatting_comment,
  [14664] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1618), 1,
      anon_sym_RPAREN,
    STATE(777), 1,
      sym_formatting_comment,
  [14674] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1620), 1,
      anon_sym_RPAREN,
    STATE(778), 1,
      sym_formatting_comment,
  [14684] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1622), 1,
      anon_sym_RPAREN,
    STATE(779), 1,
      sym_formatting_comment,
  [14694] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1204), 1,
      anon_sym_RPAREN,
    STATE(780), 1,
      sym_formatting_comment,
  [14704] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1624), 1,
      anon_sym_RPAREN,
    STATE(781), 1,
      sym_formatting_comment,
  [14714] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1626), 1,
      anon_sym_RPAREN,
    STATE(782), 1,
      sym_formatting_comment,
  [14724] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1628), 1,
      anon_sym_RPAREN,
    STATE(783), 1,
      sym_formatting_comment,
  [14734] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1630), 1,
      anon_sym_RPAREN,
    STATE(784), 1,
      sym_formatting_comment,
  [14744] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1632), 1,
      anon_sym_RPAREN,
    STATE(785), 1,
      sym_formatting_comment,
  [14754] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1634), 1,
      anon_sym_RPAREN,
    STATE(786), 1,
      sym_formatting_comment,
  [14764] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1636), 1,
      anon_sym_RPAREN,
    STATE(787), 1,
      sym_formatting_comment,
  [14774] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1638), 1,
      anon_sym_RPAREN,
    STATE(788), 1,
      sym_formatting_comment,
  [14784] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1640), 1,
      anon_sym_RPAREN,
    STATE(789), 1,
      sym_formatting_comment,
  [14794] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1642), 1,
      anon_sym_RPAREN,
    STATE(790), 1,
      sym_formatting_comment,
  [14804] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1644), 1,
      anon_sym_EQ,
    STATE(791), 1,
      sym_formatting_comment,
  [14814] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1646), 1,
      anon_sym_LPAREN,
    STATE(792), 1,
      sym_formatting_comment,
  [14824] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1648), 1,
      anon_sym_RPAREN,
    STATE(793), 1,
      sym_formatting_comment,
  [14834] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1650), 1,
      anon_sym_RPAREN,
    STATE(794), 1,
      sym_formatting_comment,
  [14844] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1652), 1,
      anon_sym_RPAREN,
    STATE(795), 1,
      sym_formatting_comment,
  [14854] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1654), 1,
      anon_sym_RPAREN,
    STATE(796), 1,
      sym_formatting_comment,
  [14864] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1656), 1,
      anon_sym_RPAREN,
    STATE(797), 1,
      sym_formatting_comment,
  [14874] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1658), 1,
      anon_sym_RPAREN,
    STATE(798), 1,
      sym_formatting_comment,
  [14884] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1660), 1,
      anon_sym_RPAREN,
    STATE(799), 1,
      sym_formatting_comment,
  [14894] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1662), 1,
      anon_sym_RPAREN,
    STATE(800), 1,
      sym_formatting_comment,
  [14904] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1664), 1,
      anon_sym_RPAREN,
    STATE(801), 1,
      sym_formatting_comment,
  [14914] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1666), 1,
      anon_sym_RPAREN,
    STATE(802), 1,
      sym_formatting_comment,
  [14924] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1668), 1,
      anon_sym_RPAREN,
    STATE(803), 1,
      sym_formatting_comment,
  [14934] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1670), 1,
      anon_sym_RPAREN,
    STATE(804), 1,
      sym_formatting_comment,
  [14944] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1672), 1,
      anon_sym_RPAREN,
    STATE(805), 1,
      sym_formatting_comment,
  [14954] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1674), 1,
      anon_sym_RPAREN,
    STATE(806), 1,
      sym_formatting_comment,
  [14964] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1676), 1,
      anon_sym_RPAREN,
    STATE(807), 1,
      sym_formatting_comment,
  [14974] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1678), 1,
      anon_sym_RPAREN,
    STATE(808), 1,
      sym_formatting_comment,
  [14984] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1680), 1,
      anon_sym_RPAREN,
    STATE(809), 1,
      sym_formatting_comment,
  [14994] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1682), 1,
      anon_sym_RPAREN,
    STATE(810), 1,
      sym_formatting_comment,
  [15004] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1684), 1,
      anon_sym_RPAREN,
    STATE(811), 1,
      sym_formatting_comment,
  [15014] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1686), 1,
      anon_sym_RPAREN,
    STATE(812), 1,
      sym_formatting_comment,
  [15024] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1688), 1,
      anon_sym_RPAREN,
    STATE(813), 1,
      sym_formatting_comment,
  [15034] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1690), 1,
      anon_sym_RPAREN,
    STATE(814), 1,
      sym_formatting_comment,
  [15044] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1692), 1,
      anon_sym_RPAREN,
    STATE(815), 1,
      sym_formatting_comment,
  [15054] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1694), 1,
      anon_sym_RPAREN,
    STATE(816), 1,
      sym_formatting_comment,
  [15064] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1696), 1,
      anon_sym_RPAREN,
    STATE(817), 1,
      sym_formatting_comment,
  [15074] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1698), 1,
      anon_sym_RPAREN,
    STATE(818), 1,
      sym_formatting_comment,
  [15084] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1700), 1,
      anon_sym_RPAREN,
    STATE(819), 1,
      sym_formatting_comment,
  [15094] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1702), 1,
      sym_int,
    STATE(820), 1,
      sym_formatting_comment,
  [15104] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1704), 1,
      anon_sym_RPAREN,
    STATE(821), 1,
      sym_formatting_comment,
  [15114] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1706), 1,
      anon_sym_RPAREN,
    STATE(822), 1,
      sym_formatting_comment,
  [15124] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1708), 1,
      anon_sym_RPAREN,
    STATE(823), 1,
      sym_formatting_comment,
  [15134] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1710), 1,
      anon_sym_RPAREN,
    STATE(824), 1,
      sym_formatting_comment,
  [15144] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1712), 1,
      anon_sym_RPAREN,
    STATE(825), 1,
      sym_formatting_comment,
  [15154] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1714), 1,
      anon_sym_RPAREN,
    STATE(826), 1,
      sym_formatting_comment,
  [15164] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1716), 1,
      anon_sym_RPAREN,
    STATE(827), 1,
      sym_formatting_comment,
  [15174] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1718), 1,
      anon_sym_RPAREN,
    STATE(828), 1,
      sym_formatting_comment,
  [15184] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1720), 1,
      anon_sym_RPAREN,
    STATE(829), 1,
      sym_formatting_comment,
  [15194] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1722), 1,
      anon_sym_RPAREN,
    STATE(830), 1,
      sym_formatting_comment,
  [15204] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1724), 1,
      anon_sym_RPAREN,
    STATE(831), 1,
      sym_formatting_comment,
  [15214] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1726), 1,
      anon_sym_RPAREN,
    STATE(832), 1,
      sym_formatting_comment,
  [15224] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(586), 1,
      anon_sym_RPAREN,
    STATE(833), 1,
      sym_formatting_comment,
  [15234] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1728), 1,
      anon_sym_RPAREN,
    STATE(834), 1,
      sym_formatting_comment,
  [15244] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1730), 1,
      anon_sym_RPAREN,
    STATE(835), 1,
      sym_formatting_comment,
  [15254] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1732), 1,
      anon_sym_RPAREN,
    STATE(836), 1,
      sym_formatting_comment,
  [15264] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1734), 1,
      anon_sym_RPAREN,
    STATE(837), 1,
      sym_formatting_comment,
  [15274] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1736), 1,
      anon_sym_RPAREN,
    STATE(838), 1,
      sym_formatting_comment,
  [15284] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1738), 1,
      anon_sym_RPAREN,
    STATE(839), 1,
      sym_formatting_comment,
  [15294] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1740), 1,
      anon_sym_RPAREN,
    STATE(840), 1,
      sym_formatting_comment,
  [15304] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1742), 1,
      anon_sym_RPAREN,
    STATE(841), 1,
      sym_formatting_comment,
  [15314] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1744), 1,
      anon_sym_RPAREN,
    STATE(842), 1,
      sym_formatting_comment,
  [15324] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1746), 1,
      anon_sym_RPAREN,
    STATE(843), 1,
      sym_formatting_comment,
  [15334] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1748), 1,
      anon_sym_RPAREN,
    STATE(844), 1,
      sym_formatting_comment,
  [15344] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1750), 1,
      anon_sym_COMMA,
    STATE(845), 1,
      sym_formatting_comment,
  [15354] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1752), 1,
      anon_sym_RPAREN,
    STATE(846), 1,
      sym_formatting_comment,
  [15364] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1754), 1,
      anon_sym_RPAREN,
    STATE(847), 1,
      sym_formatting_comment,
  [15374] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1756), 1,
      anon_sym_RPAREN,
    STATE(848), 1,
      sym_formatting_comment,
  [15384] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1758), 1,
      anon_sym_RPAREN,
    STATE(849), 1,
      sym_formatting_comment,
  [15394] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1760), 1,
      anon_sym_RPAREN,
    STATE(850), 1,
      sym_formatting_comment,
  [15404] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1762), 1,
      anon_sym_RPAREN,
    STATE(851), 1,
      sym_formatting_comment,
  [15414] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1764), 1,
      anon_sym_RPAREN,
    STATE(852), 1,
      sym_formatting_comment,
  [15424] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1766), 1,
      anon_sym_RPAREN,
    STATE(853), 1,
      sym_formatting_comment,
  [15434] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1768), 1,
      anon_sym_RPAREN,
    STATE(854), 1,
      sym_formatting_comment,
  [15444] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1770), 1,
      anon_sym_RPAREN,
    STATE(855), 1,
      sym_formatting_comment,
  [15454] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1772), 1,
      anon_sym_RPAREN,
    STATE(856), 1,
      sym_formatting_comment,
  [15464] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1774), 1,
      sym_int,
    STATE(857), 1,
      sym_formatting_comment,
  [15474] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1776), 1,
      anon_sym_RPAREN,
    STATE(858), 1,
      sym_formatting_comment,
  [15484] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1778), 1,
      anon_sym_RPAREN,
    STATE(859), 1,
      sym_formatting_comment,
  [15494] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1780), 1,
      sym_identifier,
    STATE(860), 1,
      sym_formatting_comment,
  [15504] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1782), 1,
      anon_sym_RPAREN,
    STATE(861), 1,
      sym_formatting_comment,
  [15514] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1784), 1,
      aux_sym_find_clause_token1,
    STATE(862), 1,
      sym_formatting_comment,
  [15524] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1786), 1,
      anon_sym_RPAREN,
    STATE(863), 1,
      sym_formatting_comment,
  [15534] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1788), 1,
      anon_sym_RPAREN,
    STATE(864), 1,
      sym_formatting_comment,
  [15544] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1790), 1,
      anon_sym_LPAREN,
    STATE(865), 1,
      sym_formatting_comment,
  [15554] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1792), 1,
      anon_sym_RPAREN,
    STATE(866), 1,
      sym_formatting_comment,
  [15564] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1794), 1,
      sym_term_separator_end,
    STATE(867), 1,
      sym_formatting_comment,
  [15574] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1796), 1,
      anon_sym_LPAREN,
    STATE(868), 1,
      sym_formatting_comment,
  [15584] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1798), 1,
      anon_sym_RPAREN,
    STATE(869), 1,
      sym_formatting_comment,
  [15594] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1800), 1,
      anon_sym_RPAREN,
    STATE(870), 1,
      sym_formatting_comment,
  [15604] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1802), 1,
      anon_sym_RPAREN,
    STATE(871), 1,
      sym_formatting_comment,
  [15614] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1804), 1,
      anon_sym_COMMA,
    STATE(872), 1,
      sym_formatting_comment,
  [15624] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1806), 1,
      anon_sym_RPAREN,
    STATE(873), 1,
      sym_formatting_comment,
  [15634] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1808), 1,
      anon_sym_RPAREN,
    STATE(874), 1,
      sym_formatting_comment,
  [15644] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1810), 1,
      anon_sym_RPAREN,
    STATE(875), 1,
      sym_formatting_comment,
  [15654] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1812), 1,
      anon_sym_RPAREN,
    STATE(876), 1,
      sym_formatting_comment,
  [15664] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1814), 1,
      anon_sym_COMMA,
    STATE(877), 1,
      sym_formatting_comment,
  [15674] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1816), 1,
      anon_sym_RPAREN,
    STATE(878), 1,
      sym_formatting_comment,
  [15684] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(668), 1,
      ts_builtin_sym_end,
    STATE(879), 1,
      sym_formatting_comment,
  [15694] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1818), 1,
      anon_sym_RPAREN,
    STATE(880), 1,
      sym_formatting_comment,
  [15704] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1820), 1,
      anon_sym_RPAREN,
    STATE(881), 1,
      sym_formatting_comment,
  [15714] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(686), 1,
      anon_sym_RPAREN,
    STATE(882), 1,
      sym_formatting_comment,
  [15724] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1822), 1,
      anon_sym_RPAREN,
    STATE(883), 1,
      sym_formatting_comment,
  [15734] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1824), 1,
      anon_sym_RPAREN,
    STATE(884), 1,
      sym_formatting_comment,
  [15744] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1826), 1,
      sym_string_literal,
    STATE(885), 1,
      sym_formatting_comment,
  [15754] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1828), 1,
      anon_sym_RPAREN,
    STATE(886), 1,
      sym_formatting_comment,
  [15764] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1830), 1,
      anon_sym_RPAREN,
    STATE(887), 1,
      sym_formatting_comment,
  [15774] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1832), 1,
      aux_sym_in_clause_token2,
    STATE(888), 1,
      sym_formatting_comment,
  [15784] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1834), 1,
      anon_sym_RPAREN,
    STATE(889), 1,
      sym_formatting_comment,
  [15794] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1836), 1,
      anon_sym_RPAREN,
    STATE(890), 1,
      sym_formatting_comment,
  [15804] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1838), 1,
      anon_sym_RPAREN,
    STATE(891), 1,
      sym_formatting_comment,
  [15814] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1074), 1,
      ts_builtin_sym_end,
    STATE(892), 1,
      sym_formatting_comment,
  [15824] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1840), 1,
      anon_sym_RPAREN,
    STATE(893), 1,
      sym_formatting_comment,
  [15834] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1842), 1,
      anon_sym_RPAREN,
    STATE(894), 1,
      sym_formatting_comment,
  [15844] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1844), 1,
      anon_sym_RPAREN,
    STATE(895), 1,
      sym_formatting_comment,
  [15854] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1846), 1,
      anon_sym_RPAREN,
    STATE(896), 1,
      sym_formatting_comment,
  [15864] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1848), 1,
      anon_sym_RPAREN,
    STATE(897), 1,
      sym_formatting_comment,
  [15874] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1850), 1,
      anon_sym_RPAREN,
    STATE(898), 1,
      sym_formatting_comment,
  [15884] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1852), 1,
      anon_sym_RPAREN,
    STATE(899), 1,
      sym_formatting_comment,
  [15894] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1854), 1,
      sym_identifier,
    STATE(900), 1,
      sym_formatting_comment,
  [15904] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1856), 1,
      sym_int,
    STATE(901), 1,
      sym_formatting_comment,
  [15914] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1858), 1,
      anon_sym_RPAREN,
    STATE(902), 1,
      sym_formatting_comment,
  [15924] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1860), 1,
      anon_sym_RPAREN,
    STATE(903), 1,
      sym_formatting_comment,
  [15934] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1862), 1,
      anon_sym_RPAREN,
    STATE(904), 1,
      sym_formatting_comment,
  [15944] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1864), 1,
      anon_sym_RPAREN,
    STATE(905), 1,
      sym_formatting_comment,
  [15954] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1866), 1,
      anon_sym_RPAREN,
    STATE(906), 1,
      sym_formatting_comment,
  [15964] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1868), 1,
      anon_sym_RPAREN,
    STATE(907), 1,
      sym_formatting_comment,
  [15974] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1870), 1,
      anon_sym_RPAREN,
    STATE(908), 1,
      sym_formatting_comment,
  [15984] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1872), 1,
      anon_sym_RPAREN,
    STATE(909), 1,
      sym_formatting_comment,
  [15994] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1874), 1,
      sym_identifier,
    STATE(910), 1,
      sym_formatting_comment,
  [16004] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1876), 1,
      anon_sym_RPAREN,
    STATE(911), 1,
      sym_formatting_comment,
  [16014] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1878), 1,
      anon_sym_COMMA,
    STATE(912), 1,
      sym_formatting_comment,
  [16024] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1880), 1,
      anon_sym_RPAREN,
    STATE(913), 1,
      sym_formatting_comment,
  [16034] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1882), 1,
      sym_identifier,
    STATE(914), 1,
      sym_formatting_comment,
  [16044] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1884), 1,
      anon_sym_COLON,
    STATE(915), 1,
      sym_formatting_comment,
  [16054] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1886), 1,
      anon_sym_RPAREN,
    STATE(916), 1,
      sym_formatting_comment,
  [16064] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(512), 1,
      ts_builtin_sym_end,
    STATE(917), 1,
      sym_formatting_comment,
  [16074] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1888), 1,
      anon_sym_RPAREN,
    STATE(918), 1,
      sym_formatting_comment,
  [16084] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(902), 1,
      aux_sym_from_clause_token1,
    STATE(919), 1,
      sym_formatting_comment,
  [16094] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1890), 1,
      sym_identifier,
    STATE(920), 1,
      sym_formatting_comment,
  [16104] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1892), 1,
      sym_string_literal,
    STATE(921), 1,
      sym_formatting_comment,
  [16114] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1894), 1,
      anon_sym_LPAREN,
    STATE(922), 1,
      sym_formatting_comment,
  [16124] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1896), 1,
      anon_sym_COMMA,
    STATE(923), 1,
      sym_formatting_comment,
  [16134] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1898), 1,
      anon_sym_LPAREN,
    STATE(924), 1,
      sym_formatting_comment,
  [16144] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1900), 1,
      anon_sym_COMMA,
    STATE(925), 1,
      sym_formatting_comment,
  [16154] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1902), 1,
      sym_int,
    STATE(926), 1,
      sym_formatting_comment,
  [16164] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1904), 1,
      anon_sym_LPAREN,
    STATE(927), 1,
      sym_formatting_comment,
  [16174] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1906), 1,
      anon_sym_COMMA,
    STATE(928), 1,
      sym_formatting_comment,
  [16184] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1908), 1,
      anon_sym_COMMA,
    STATE(929), 1,
      sym_formatting_comment,
  [16194] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1910), 1,
      anon_sym_RPAREN,
    STATE(930), 1,
      sym_formatting_comment,
  [16204] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1912), 1,
      anon_sym_RPAREN,
    STATE(931), 1,
      sym_formatting_comment,
  [16214] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1914), 1,
      anon_sym_LPAREN,
    STATE(932), 1,
      sym_formatting_comment,
  [16224] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1916), 1,
      anon_sym_LPAREN,
    STATE(933), 1,
      sym_formatting_comment,
  [16234] = 3,
    ACTIONS(3), 1,
      anon_sym_SLASH_SLASH_SLASH,
    ACTIONS(1918), 1,
      anon_sym_RPAREN,
    STATE(934), 1,
      sym_formatting_comment,
  [16244] = 1,
    ACTIONS(1920), 1,
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
  [SMALL_STATE(10)] = 492,
  [SMALL_STATE(11)] = 538,
  [SMALL_STATE(12)] = 581,
  [SMALL_STATE(13)] = 625,
  [SMALL_STATE(14)] = 660,
  [SMALL_STATE(15)] = 695,
  [SMALL_STATE(16)] = 730,
  [SMALL_STATE(17)] = 765,
  [SMALL_STATE(18)] = 798,
  [SMALL_STATE(19)] = 862,
  [SMALL_STATE(20)] = 920,
  [SMALL_STATE(21)] = 960,
  [SMALL_STATE(22)] = 1000,
  [SMALL_STATE(23)] = 1058,
  [SMALL_STATE(24)] = 1102,
  [SMALL_STATE(25)] = 1142,
  [SMALL_STATE(26)] = 1189,
  [SMALL_STATE(27)] = 1236,
  [SMALL_STATE(28)] = 1262,
  [SMALL_STATE(29)] = 1308,
  [SMALL_STATE(30)] = 1360,
  [SMALL_STATE(31)] = 1406,
  [SMALL_STATE(32)] = 1452,
  [SMALL_STATE(33)] = 1496,
  [SMALL_STATE(34)] = 1526,
  [SMALL_STATE(35)] = 1572,
  [SMALL_STATE(36)] = 1600,
  [SMALL_STATE(37)] = 1652,
  [SMALL_STATE(38)] = 1696,
  [SMALL_STATE(39)] = 1748,
  [SMALL_STATE(40)] = 1800,
  [SMALL_STATE(41)] = 1830,
  [SMALL_STATE(42)] = 1882,
  [SMALL_STATE(43)] = 1907,
  [SMALL_STATE(44)] = 1932,
  [SMALL_STATE(45)] = 1957,
  [SMALL_STATE(46)] = 1982,
  [SMALL_STATE(47)] = 2007,
  [SMALL_STATE(48)] = 2032,
  [SMALL_STATE(49)] = 2065,
  [SMALL_STATE(50)] = 2111,
  [SMALL_STATE(51)] = 2135,
  [SMALL_STATE(52)] = 2181,
  [SMALL_STATE(53)] = 2209,
  [SMALL_STATE(54)] = 2255,
  [SMALL_STATE(55)] = 2301,
  [SMALL_STATE(56)] = 2324,
  [SMALL_STATE(57)] = 2349,
  [SMALL_STATE(58)] = 2392,
  [SMALL_STATE(59)] = 2415,
  [SMALL_STATE(60)] = 2438,
  [SMALL_STATE(61)] = 2461,
  [SMALL_STATE(62)] = 2490,
  [SMALL_STATE(63)] = 2533,
  [SMALL_STATE(64)] = 2556,
  [SMALL_STATE(65)] = 2579,
  [SMALL_STATE(66)] = 2608,
  [SMALL_STATE(67)] = 2633,
  [SMALL_STATE(68)] = 2656,
  [SMALL_STATE(69)] = 2681,
  [SMALL_STATE(70)] = 2706,
  [SMALL_STATE(71)] = 2749,
  [SMALL_STATE(72)] = 2769,
  [SMALL_STATE(73)] = 2797,
  [SMALL_STATE(74)] = 2817,
  [SMALL_STATE(75)] = 2857,
  [SMALL_STATE(76)] = 2877,
  [SMALL_STATE(77)] = 2917,
  [SMALL_STATE(78)] = 2957,
  [SMALL_STATE(79)] = 2997,
  [SMALL_STATE(80)] = 3021,
  [SMALL_STATE(81)] = 3061,
  [SMALL_STATE(82)] = 3085,
  [SMALL_STATE(83)] = 3107,
  [SMALL_STATE(84)] = 3127,
  [SMALL_STATE(85)] = 3147,
  [SMALL_STATE(86)] = 3187,
  [SMALL_STATE(87)] = 3227,
  [SMALL_STATE(88)] = 3251,
  [SMALL_STATE(89)] = 3271,
  [SMALL_STATE(90)] = 3291,
  [SMALL_STATE(91)] = 3331,
  [SMALL_STATE(92)] = 3371,
  [SMALL_STATE(93)] = 3392,
  [SMALL_STATE(94)] = 3427,
  [SMALL_STATE(95)] = 3448,
  [SMALL_STATE(96)] = 3477,
  [SMALL_STATE(97)] = 3498,
  [SMALL_STATE(98)] = 3519,
  [SMALL_STATE(99)] = 3540,
  [SMALL_STATE(100)] = 3563,
  [SMALL_STATE(101)] = 3586,
  [SMALL_STATE(102)] = 3611,
  [SMALL_STATE(103)] = 3646,
  [SMALL_STATE(104)] = 3665,
  [SMALL_STATE(105)] = 3700,
  [SMALL_STATE(106)] = 3735,
  [SMALL_STATE(107)] = 3754,
  [SMALL_STATE(108)] = 3777,
  [SMALL_STATE(109)] = 3799,
  [SMALL_STATE(110)] = 3833,
  [SMALL_STATE(111)] = 3867,
  [SMALL_STATE(112)] = 3885,
  [SMALL_STATE(113)] = 3903,
  [SMALL_STATE(114)] = 3923,
  [SMALL_STATE(115)] = 3941,
  [SMALL_STATE(116)] = 3961,
  [SMALL_STATE(117)] = 3981,
  [SMALL_STATE(118)] = 3999,
  [SMALL_STATE(119)] = 4021,
  [SMALL_STATE(120)] = 4055,
  [SMALL_STATE(121)] = 4089,
  [SMALL_STATE(122)] = 4123,
  [SMALL_STATE(123)] = 4141,
  [SMALL_STATE(124)] = 4175,
  [SMALL_STATE(125)] = 4209,
  [SMALL_STATE(126)] = 4243,
  [SMALL_STATE(127)] = 4261,
  [SMALL_STATE(128)] = 4295,
  [SMALL_STATE(129)] = 4329,
  [SMALL_STATE(130)] = 4349,
  [SMALL_STATE(131)] = 4383,
  [SMALL_STATE(132)] = 4417,
  [SMALL_STATE(133)] = 4451,
  [SMALL_STATE(134)] = 4485,
  [SMALL_STATE(135)] = 4519,
  [SMALL_STATE(136)] = 4553,
  [SMALL_STATE(137)] = 4575,
  [SMALL_STATE(138)] = 4597,
  [SMALL_STATE(139)] = 4619,
  [SMALL_STATE(140)] = 4653,
  [SMALL_STATE(141)] = 4673,
  [SMALL_STATE(142)] = 4695,
  [SMALL_STATE(143)] = 4714,
  [SMALL_STATE(144)] = 4731,
  [SMALL_STATE(145)] = 4748,
  [SMALL_STATE(146)] = 4765,
  [SMALL_STATE(147)] = 4786,
  [SMALL_STATE(148)] = 4803,
  [SMALL_STATE(149)] = 4820,
  [SMALL_STATE(150)] = 4837,
  [SMALL_STATE(151)] = 4854,
  [SMALL_STATE(152)] = 4873,
  [SMALL_STATE(153)] = 4890,
  [SMALL_STATE(154)] = 4907,
  [SMALL_STATE(155)] = 4926,
  [SMALL_STATE(156)] = 4945,
  [SMALL_STATE(157)] = 4962,
  [SMALL_STATE(158)] = 4991,
  [SMALL_STATE(159)] = 5008,
  [SMALL_STATE(160)] = 5037,
  [SMALL_STATE(161)] = 5054,
  [SMALL_STATE(162)] = 5071,
  [SMALL_STATE(163)] = 5088,
  [SMALL_STATE(164)] = 5109,
  [SMALL_STATE(165)] = 5126,
  [SMALL_STATE(166)] = 5143,
  [SMALL_STATE(167)] = 5164,
  [SMALL_STATE(168)] = 5183,
  [SMALL_STATE(169)] = 5204,
  [SMALL_STATE(170)] = 5225,
  [SMALL_STATE(171)] = 5242,
  [SMALL_STATE(172)] = 5259,
  [SMALL_STATE(173)] = 5280,
  [SMALL_STATE(174)] = 5297,
  [SMALL_STATE(175)] = 5316,
  [SMALL_STATE(176)] = 5333,
  [SMALL_STATE(177)] = 5362,
  [SMALL_STATE(178)] = 5379,
  [SMALL_STATE(179)] = 5410,
  [SMALL_STATE(180)] = 5438,
  [SMALL_STATE(181)] = 5464,
  [SMALL_STATE(182)] = 5492,
  [SMALL_STATE(183)] = 5520,
  [SMALL_STATE(184)] = 5548,
  [SMALL_STATE(185)] = 5564,
  [SMALL_STATE(186)] = 5592,
  [SMALL_STATE(187)] = 5618,
  [SMALL_STATE(188)] = 5646,
  [SMALL_STATE(189)] = 5674,
  [SMALL_STATE(190)] = 5702,
  [SMALL_STATE(191)] = 5730,
  [SMALL_STATE(192)] = 5746,
  [SMALL_STATE(193)] = 5774,
  [SMALL_STATE(194)] = 5802,
  [SMALL_STATE(195)] = 5830,
  [SMALL_STATE(196)] = 5858,
  [SMALL_STATE(197)] = 5874,
  [SMALL_STATE(198)] = 5902,
  [SMALL_STATE(199)] = 5930,
  [SMALL_STATE(200)] = 5958,
  [SMALL_STATE(201)] = 5982,
  [SMALL_STATE(202)] = 6010,
  [SMALL_STATE(203)] = 6038,
  [SMALL_STATE(204)] = 6066,
  [SMALL_STATE(205)] = 6094,
  [SMALL_STATE(206)] = 6122,
  [SMALL_STATE(207)] = 6150,
  [SMALL_STATE(208)] = 6178,
  [SMALL_STATE(209)] = 6206,
  [SMALL_STATE(210)] = 6234,
  [SMALL_STATE(211)] = 6262,
  [SMALL_STATE(212)] = 6290,
  [SMALL_STATE(213)] = 6318,
  [SMALL_STATE(214)] = 6346,
  [SMALL_STATE(215)] = 6362,
  [SMALL_STATE(216)] = 6390,
  [SMALL_STATE(217)] = 6418,
  [SMALL_STATE(218)] = 6438,
  [SMALL_STATE(219)] = 6466,
  [SMALL_STATE(220)] = 6494,
  [SMALL_STATE(221)] = 6522,
  [SMALL_STATE(222)] = 6550,
  [SMALL_STATE(223)] = 6578,
  [SMALL_STATE(224)] = 6594,
  [SMALL_STATE(225)] = 6622,
  [SMALL_STATE(226)] = 6648,
  [SMALL_STATE(227)] = 6664,
  [SMALL_STATE(228)] = 6682,
  [SMALL_STATE(229)] = 6702,
  [SMALL_STATE(230)] = 6720,
  [SMALL_STATE(231)] = 6746,
  [SMALL_STATE(232)] = 6772,
  [SMALL_STATE(233)] = 6800,
  [SMALL_STATE(234)] = 6816,
  [SMALL_STATE(235)] = 6831,
  [SMALL_STATE(236)] = 6850,
  [SMALL_STATE(237)] = 6875,
  [SMALL_STATE(238)] = 6898,
  [SMALL_STATE(239)] = 6913,
  [SMALL_STATE(240)] = 6930,
  [SMALL_STATE(241)] = 6945,
  [SMALL_STATE(242)] = 6960,
  [SMALL_STATE(243)] = 6981,
  [SMALL_STATE(244)] = 6996,
  [SMALL_STATE(245)] = 7011,
  [SMALL_STATE(246)] = 7036,
  [SMALL_STATE(247)] = 7051,
  [SMALL_STATE(248)] = 7066,
  [SMALL_STATE(249)] = 7081,
  [SMALL_STATE(250)] = 7096,
  [SMALL_STATE(251)] = 7111,
  [SMALL_STATE(252)] = 7126,
  [SMALL_STATE(253)] = 7141,
  [SMALL_STATE(254)] = 7164,
  [SMALL_STATE(255)] = 7189,
  [SMALL_STATE(256)] = 7204,
  [SMALL_STATE(257)] = 7229,
  [SMALL_STATE(258)] = 7244,
  [SMALL_STATE(259)] = 7259,
  [SMALL_STATE(260)] = 7284,
  [SMALL_STATE(261)] = 7306,
  [SMALL_STATE(262)] = 7328,
  [SMALL_STATE(263)] = 7346,
  [SMALL_STATE(264)] = 7364,
  [SMALL_STATE(265)] = 7386,
  [SMALL_STATE(266)] = 7408,
  [SMALL_STATE(267)] = 7430,
  [SMALL_STATE(268)] = 7452,
  [SMALL_STATE(269)] = 7474,
  [SMALL_STATE(270)] = 7496,
  [SMALL_STATE(271)] = 7518,
  [SMALL_STATE(272)] = 7540,
  [SMALL_STATE(273)] = 7562,
  [SMALL_STATE(274)] = 7584,
  [SMALL_STATE(275)] = 7606,
  [SMALL_STATE(276)] = 7628,
  [SMALL_STATE(277)] = 7650,
  [SMALL_STATE(278)] = 7672,
  [SMALL_STATE(279)] = 7694,
  [SMALL_STATE(280)] = 7716,
  [SMALL_STATE(281)] = 7738,
  [SMALL_STATE(282)] = 7754,
  [SMALL_STATE(283)] = 7776,
  [SMALL_STATE(284)] = 7798,
  [SMALL_STATE(285)] = 7820,
  [SMALL_STATE(286)] = 7838,
  [SMALL_STATE(287)] = 7860,
  [SMALL_STATE(288)] = 7882,
  [SMALL_STATE(289)] = 7904,
  [SMALL_STATE(290)] = 7926,
  [SMALL_STATE(291)] = 7948,
  [SMALL_STATE(292)] = 7970,
  [SMALL_STATE(293)] = 7992,
  [SMALL_STATE(294)] = 8014,
  [SMALL_STATE(295)] = 8036,
  [SMALL_STATE(296)] = 8058,
  [SMALL_STATE(297)] = 8080,
  [SMALL_STATE(298)] = 8102,
  [SMALL_STATE(299)] = 8124,
  [SMALL_STATE(300)] = 8146,
  [SMALL_STATE(301)] = 8168,
  [SMALL_STATE(302)] = 8190,
  [SMALL_STATE(303)] = 8212,
  [SMALL_STATE(304)] = 8234,
  [SMALL_STATE(305)] = 8256,
  [SMALL_STATE(306)] = 8278,
  [SMALL_STATE(307)] = 8300,
  [SMALL_STATE(308)] = 8322,
  [SMALL_STATE(309)] = 8344,
  [SMALL_STATE(310)] = 8366,
  [SMALL_STATE(311)] = 8388,
  [SMALL_STATE(312)] = 8410,
  [SMALL_STATE(313)] = 8432,
  [SMALL_STATE(314)] = 8454,
  [SMALL_STATE(315)] = 8476,
  [SMALL_STATE(316)] = 8498,
  [SMALL_STATE(317)] = 8520,
  [SMALL_STATE(318)] = 8542,
  [SMALL_STATE(319)] = 8564,
  [SMALL_STATE(320)] = 8586,
  [SMALL_STATE(321)] = 8608,
  [SMALL_STATE(322)] = 8630,
  [SMALL_STATE(323)] = 8652,
  [SMALL_STATE(324)] = 8674,
  [SMALL_STATE(325)] = 8696,
  [SMALL_STATE(326)] = 8718,
  [SMALL_STATE(327)] = 8740,
  [SMALL_STATE(328)] = 8762,
  [SMALL_STATE(329)] = 8784,
  [SMALL_STATE(330)] = 8806,
  [SMALL_STATE(331)] = 8828,
  [SMALL_STATE(332)] = 8850,
  [SMALL_STATE(333)] = 8872,
  [SMALL_STATE(334)] = 8894,
  [SMALL_STATE(335)] = 8912,
  [SMALL_STATE(336)] = 8932,
  [SMALL_STATE(337)] = 8950,
  [SMALL_STATE(338)] = 8964,
  [SMALL_STATE(339)] = 8986,
  [SMALL_STATE(340)] = 9008,
  [SMALL_STATE(341)] = 9030,
  [SMALL_STATE(342)] = 9047,
  [SMALL_STATE(343)] = 9066,
  [SMALL_STATE(344)] = 9081,
  [SMALL_STATE(345)] = 9094,
  [SMALL_STATE(346)] = 9111,
  [SMALL_STATE(347)] = 9128,
  [SMALL_STATE(348)] = 9145,
  [SMALL_STATE(349)] = 9164,
  [SMALL_STATE(350)] = 9179,
  [SMALL_STATE(351)] = 9196,
  [SMALL_STATE(352)] = 9213,
  [SMALL_STATE(353)] = 9226,
  [SMALL_STATE(354)] = 9245,
  [SMALL_STATE(355)] = 9264,
  [SMALL_STATE(356)] = 9277,
  [SMALL_STATE(357)] = 9290,
  [SMALL_STATE(358)] = 9305,
  [SMALL_STATE(359)] = 9320,
  [SMALL_STATE(360)] = 9339,
  [SMALL_STATE(361)] = 9354,
  [SMALL_STATE(362)] = 9367,
  [SMALL_STATE(363)] = 9384,
  [SMALL_STATE(364)] = 9399,
  [SMALL_STATE(365)] = 9416,
  [SMALL_STATE(366)] = 9433,
  [SMALL_STATE(367)] = 9452,
  [SMALL_STATE(368)] = 9468,
  [SMALL_STATE(369)] = 9484,
  [SMALL_STATE(370)] = 9500,
  [SMALL_STATE(371)] = 9516,
  [SMALL_STATE(372)] = 9532,
  [SMALL_STATE(373)] = 9548,
  [SMALL_STATE(374)] = 9564,
  [SMALL_STATE(375)] = 9580,
  [SMALL_STATE(376)] = 9596,
  [SMALL_STATE(377)] = 9612,
  [SMALL_STATE(378)] = 9628,
  [SMALL_STATE(379)] = 9644,
  [SMALL_STATE(380)] = 9660,
  [SMALL_STATE(381)] = 9676,
  [SMALL_STATE(382)] = 9692,
  [SMALL_STATE(383)] = 9708,
  [SMALL_STATE(384)] = 9724,
  [SMALL_STATE(385)] = 9740,
  [SMALL_STATE(386)] = 9756,
  [SMALL_STATE(387)] = 9772,
  [SMALL_STATE(388)] = 9788,
  [SMALL_STATE(389)] = 9804,
  [SMALL_STATE(390)] = 9820,
  [SMALL_STATE(391)] = 9836,
  [SMALL_STATE(392)] = 9850,
  [SMALL_STATE(393)] = 9866,
  [SMALL_STATE(394)] = 9882,
  [SMALL_STATE(395)] = 9898,
  [SMALL_STATE(396)] = 9914,
  [SMALL_STATE(397)] = 9930,
  [SMALL_STATE(398)] = 9944,
  [SMALL_STATE(399)] = 9960,
  [SMALL_STATE(400)] = 9976,
  [SMALL_STATE(401)] = 9992,
  [SMALL_STATE(402)] = 10008,
  [SMALL_STATE(403)] = 10024,
  [SMALL_STATE(404)] = 10040,
  [SMALL_STATE(405)] = 10056,
  [SMALL_STATE(406)] = 10072,
  [SMALL_STATE(407)] = 10088,
  [SMALL_STATE(408)] = 10104,
  [SMALL_STATE(409)] = 10120,
  [SMALL_STATE(410)] = 10136,
  [SMALL_STATE(411)] = 10152,
  [SMALL_STATE(412)] = 10168,
  [SMALL_STATE(413)] = 10184,
  [SMALL_STATE(414)] = 10200,
  [SMALL_STATE(415)] = 10214,
  [SMALL_STATE(416)] = 10230,
  [SMALL_STATE(417)] = 10246,
  [SMALL_STATE(418)] = 10262,
  [SMALL_STATE(419)] = 10278,
  [SMALL_STATE(420)] = 10294,
  [SMALL_STATE(421)] = 10310,
  [SMALL_STATE(422)] = 10326,
  [SMALL_STATE(423)] = 10342,
  [SMALL_STATE(424)] = 10358,
  [SMALL_STATE(425)] = 10374,
  [SMALL_STATE(426)] = 10388,
  [SMALL_STATE(427)] = 10404,
  [SMALL_STATE(428)] = 10420,
  [SMALL_STATE(429)] = 10436,
  [SMALL_STATE(430)] = 10452,
  [SMALL_STATE(431)] = 10468,
  [SMALL_STATE(432)] = 10484,
  [SMALL_STATE(433)] = 10500,
  [SMALL_STATE(434)] = 10512,
  [SMALL_STATE(435)] = 10528,
  [SMALL_STATE(436)] = 10544,
  [SMALL_STATE(437)] = 10560,
  [SMALL_STATE(438)] = 10576,
  [SMALL_STATE(439)] = 10592,
  [SMALL_STATE(440)] = 10608,
  [SMALL_STATE(441)] = 10624,
  [SMALL_STATE(442)] = 10640,
  [SMALL_STATE(443)] = 10656,
  [SMALL_STATE(444)] = 10668,
  [SMALL_STATE(445)] = 10684,
  [SMALL_STATE(446)] = 10700,
  [SMALL_STATE(447)] = 10716,
  [SMALL_STATE(448)] = 10732,
  [SMALL_STATE(449)] = 10748,
  [SMALL_STATE(450)] = 10762,
  [SMALL_STATE(451)] = 10778,
  [SMALL_STATE(452)] = 10794,
  [SMALL_STATE(453)] = 10810,
  [SMALL_STATE(454)] = 10826,
  [SMALL_STATE(455)] = 10842,
  [SMALL_STATE(456)] = 10858,
  [SMALL_STATE(457)] = 10874,
  [SMALL_STATE(458)] = 10890,
  [SMALL_STATE(459)] = 10906,
  [SMALL_STATE(460)] = 10920,
  [SMALL_STATE(461)] = 10936,
  [SMALL_STATE(462)] = 10952,
  [SMALL_STATE(463)] = 10968,
  [SMALL_STATE(464)] = 10984,
  [SMALL_STATE(465)] = 11000,
  [SMALL_STATE(466)] = 11016,
  [SMALL_STATE(467)] = 11032,
  [SMALL_STATE(468)] = 11048,
  [SMALL_STATE(469)] = 11064,
  [SMALL_STATE(470)] = 11080,
  [SMALL_STATE(471)] = 11096,
  [SMALL_STATE(472)] = 11110,
  [SMALL_STATE(473)] = 11124,
  [SMALL_STATE(474)] = 11140,
  [SMALL_STATE(475)] = 11156,
  [SMALL_STATE(476)] = 11172,
  [SMALL_STATE(477)] = 11188,
  [SMALL_STATE(478)] = 11204,
  [SMALL_STATE(479)] = 11220,
  [SMALL_STATE(480)] = 11236,
  [SMALL_STATE(481)] = 11252,
  [SMALL_STATE(482)] = 11268,
  [SMALL_STATE(483)] = 11284,
  [SMALL_STATE(484)] = 11300,
  [SMALL_STATE(485)] = 11316,
  [SMALL_STATE(486)] = 11332,
  [SMALL_STATE(487)] = 11348,
  [SMALL_STATE(488)] = 11364,
  [SMALL_STATE(489)] = 11380,
  [SMALL_STATE(490)] = 11396,
  [SMALL_STATE(491)] = 11412,
  [SMALL_STATE(492)] = 11428,
  [SMALL_STATE(493)] = 11444,
  [SMALL_STATE(494)] = 11460,
  [SMALL_STATE(495)] = 11476,
  [SMALL_STATE(496)] = 11492,
  [SMALL_STATE(497)] = 11508,
  [SMALL_STATE(498)] = 11524,
  [SMALL_STATE(499)] = 11540,
  [SMALL_STATE(500)] = 11556,
  [SMALL_STATE(501)] = 11572,
  [SMALL_STATE(502)] = 11588,
  [SMALL_STATE(503)] = 11604,
  [SMALL_STATE(504)] = 11620,
  [SMALL_STATE(505)] = 11636,
  [SMALL_STATE(506)] = 11652,
  [SMALL_STATE(507)] = 11668,
  [SMALL_STATE(508)] = 11684,
  [SMALL_STATE(509)] = 11700,
  [SMALL_STATE(510)] = 11712,
  [SMALL_STATE(511)] = 11728,
  [SMALL_STATE(512)] = 11744,
  [SMALL_STATE(513)] = 11758,
  [SMALL_STATE(514)] = 11772,
  [SMALL_STATE(515)] = 11784,
  [SMALL_STATE(516)] = 11796,
  [SMALL_STATE(517)] = 11808,
  [SMALL_STATE(518)] = 11820,
  [SMALL_STATE(519)] = 11832,
  [SMALL_STATE(520)] = 11844,
  [SMALL_STATE(521)] = 11860,
  [SMALL_STATE(522)] = 11876,
  [SMALL_STATE(523)] = 11888,
  [SMALL_STATE(524)] = 11904,
  [SMALL_STATE(525)] = 11916,
  [SMALL_STATE(526)] = 11928,
  [SMALL_STATE(527)] = 11942,
  [SMALL_STATE(528)] = 11954,
  [SMALL_STATE(529)] = 11966,
  [SMALL_STATE(530)] = 11982,
  [SMALL_STATE(531)] = 11994,
  [SMALL_STATE(532)] = 12008,
  [SMALL_STATE(533)] = 12020,
  [SMALL_STATE(534)] = 12032,
  [SMALL_STATE(535)] = 12046,
  [SMALL_STATE(536)] = 12060,
  [SMALL_STATE(537)] = 12074,
  [SMALL_STATE(538)] = 12090,
  [SMALL_STATE(539)] = 12106,
  [SMALL_STATE(540)] = 12122,
  [SMALL_STATE(541)] = 12134,
  [SMALL_STATE(542)] = 12150,
  [SMALL_STATE(543)] = 12166,
  [SMALL_STATE(544)] = 12182,
  [SMALL_STATE(545)] = 12198,
  [SMALL_STATE(546)] = 12214,
  [SMALL_STATE(547)] = 12230,
  [SMALL_STATE(548)] = 12246,
  [SMALL_STATE(549)] = 12262,
  [SMALL_STATE(550)] = 12278,
  [SMALL_STATE(551)] = 12294,
  [SMALL_STATE(552)] = 12310,
  [SMALL_STATE(553)] = 12326,
  [SMALL_STATE(554)] = 12342,
  [SMALL_STATE(555)] = 12358,
  [SMALL_STATE(556)] = 12374,
  [SMALL_STATE(557)] = 12386,
  [SMALL_STATE(558)] = 12402,
  [SMALL_STATE(559)] = 12418,
  [SMALL_STATE(560)] = 12434,
  [SMALL_STATE(561)] = 12450,
  [SMALL_STATE(562)] = 12462,
  [SMALL_STATE(563)] = 12475,
  [SMALL_STATE(564)] = 12488,
  [SMALL_STATE(565)] = 12501,
  [SMALL_STATE(566)] = 12514,
  [SMALL_STATE(567)] = 12525,
  [SMALL_STATE(568)] = 12538,
  [SMALL_STATE(569)] = 12551,
  [SMALL_STATE(570)] = 12564,
  [SMALL_STATE(571)] = 12577,
  [SMALL_STATE(572)] = 12588,
  [SMALL_STATE(573)] = 12599,
  [SMALL_STATE(574)] = 12610,
  [SMALL_STATE(575)] = 12621,
  [SMALL_STATE(576)] = 12634,
  [SMALL_STATE(577)] = 12647,
  [SMALL_STATE(578)] = 12658,
  [SMALL_STATE(579)] = 12669,
  [SMALL_STATE(580)] = 12682,
  [SMALL_STATE(581)] = 12693,
  [SMALL_STATE(582)] = 12704,
  [SMALL_STATE(583)] = 12715,
  [SMALL_STATE(584)] = 12728,
  [SMALL_STATE(585)] = 12739,
  [SMALL_STATE(586)] = 12752,
  [SMALL_STATE(587)] = 12763,
  [SMALL_STATE(588)] = 12774,
  [SMALL_STATE(589)] = 12784,
  [SMALL_STATE(590)] = 12794,
  [SMALL_STATE(591)] = 12804,
  [SMALL_STATE(592)] = 12814,
  [SMALL_STATE(593)] = 12824,
  [SMALL_STATE(594)] = 12834,
  [SMALL_STATE(595)] = 12844,
  [SMALL_STATE(596)] = 12854,
  [SMALL_STATE(597)] = 12864,
  [SMALL_STATE(598)] = 12874,
  [SMALL_STATE(599)] = 12884,
  [SMALL_STATE(600)] = 12894,
  [SMALL_STATE(601)] = 12904,
  [SMALL_STATE(602)] = 12914,
  [SMALL_STATE(603)] = 12924,
  [SMALL_STATE(604)] = 12934,
  [SMALL_STATE(605)] = 12944,
  [SMALL_STATE(606)] = 12954,
  [SMALL_STATE(607)] = 12964,
  [SMALL_STATE(608)] = 12974,
  [SMALL_STATE(609)] = 12984,
  [SMALL_STATE(610)] = 12994,
  [SMALL_STATE(611)] = 13004,
  [SMALL_STATE(612)] = 13014,
  [SMALL_STATE(613)] = 13024,
  [SMALL_STATE(614)] = 13034,
  [SMALL_STATE(615)] = 13044,
  [SMALL_STATE(616)] = 13054,
  [SMALL_STATE(617)] = 13064,
  [SMALL_STATE(618)] = 13074,
  [SMALL_STATE(619)] = 13084,
  [SMALL_STATE(620)] = 13094,
  [SMALL_STATE(621)] = 13104,
  [SMALL_STATE(622)] = 13114,
  [SMALL_STATE(623)] = 13124,
  [SMALL_STATE(624)] = 13134,
  [SMALL_STATE(625)] = 13144,
  [SMALL_STATE(626)] = 13154,
  [SMALL_STATE(627)] = 13164,
  [SMALL_STATE(628)] = 13174,
  [SMALL_STATE(629)] = 13184,
  [SMALL_STATE(630)] = 13194,
  [SMALL_STATE(631)] = 13204,
  [SMALL_STATE(632)] = 13214,
  [SMALL_STATE(633)] = 13224,
  [SMALL_STATE(634)] = 13234,
  [SMALL_STATE(635)] = 13244,
  [SMALL_STATE(636)] = 13254,
  [SMALL_STATE(637)] = 13264,
  [SMALL_STATE(638)] = 13274,
  [SMALL_STATE(639)] = 13284,
  [SMALL_STATE(640)] = 13294,
  [SMALL_STATE(641)] = 13304,
  [SMALL_STATE(642)] = 13314,
  [SMALL_STATE(643)] = 13324,
  [SMALL_STATE(644)] = 13334,
  [SMALL_STATE(645)] = 13344,
  [SMALL_STATE(646)] = 13354,
  [SMALL_STATE(647)] = 13364,
  [SMALL_STATE(648)] = 13374,
  [SMALL_STATE(649)] = 13384,
  [SMALL_STATE(650)] = 13394,
  [SMALL_STATE(651)] = 13404,
  [SMALL_STATE(652)] = 13414,
  [SMALL_STATE(653)] = 13424,
  [SMALL_STATE(654)] = 13434,
  [SMALL_STATE(655)] = 13444,
  [SMALL_STATE(656)] = 13454,
  [SMALL_STATE(657)] = 13464,
  [SMALL_STATE(658)] = 13474,
  [SMALL_STATE(659)] = 13484,
  [SMALL_STATE(660)] = 13494,
  [SMALL_STATE(661)] = 13504,
  [SMALL_STATE(662)] = 13514,
  [SMALL_STATE(663)] = 13524,
  [SMALL_STATE(664)] = 13534,
  [SMALL_STATE(665)] = 13544,
  [SMALL_STATE(666)] = 13554,
  [SMALL_STATE(667)] = 13564,
  [SMALL_STATE(668)] = 13574,
  [SMALL_STATE(669)] = 13584,
  [SMALL_STATE(670)] = 13594,
  [SMALL_STATE(671)] = 13604,
  [SMALL_STATE(672)] = 13614,
  [SMALL_STATE(673)] = 13624,
  [SMALL_STATE(674)] = 13634,
  [SMALL_STATE(675)] = 13644,
  [SMALL_STATE(676)] = 13654,
  [SMALL_STATE(677)] = 13664,
  [SMALL_STATE(678)] = 13674,
  [SMALL_STATE(679)] = 13684,
  [SMALL_STATE(680)] = 13694,
  [SMALL_STATE(681)] = 13704,
  [SMALL_STATE(682)] = 13714,
  [SMALL_STATE(683)] = 13724,
  [SMALL_STATE(684)] = 13734,
  [SMALL_STATE(685)] = 13744,
  [SMALL_STATE(686)] = 13754,
  [SMALL_STATE(687)] = 13764,
  [SMALL_STATE(688)] = 13774,
  [SMALL_STATE(689)] = 13784,
  [SMALL_STATE(690)] = 13794,
  [SMALL_STATE(691)] = 13804,
  [SMALL_STATE(692)] = 13814,
  [SMALL_STATE(693)] = 13824,
  [SMALL_STATE(694)] = 13834,
  [SMALL_STATE(695)] = 13844,
  [SMALL_STATE(696)] = 13854,
  [SMALL_STATE(697)] = 13864,
  [SMALL_STATE(698)] = 13874,
  [SMALL_STATE(699)] = 13884,
  [SMALL_STATE(700)] = 13894,
  [SMALL_STATE(701)] = 13904,
  [SMALL_STATE(702)] = 13914,
  [SMALL_STATE(703)] = 13924,
  [SMALL_STATE(704)] = 13934,
  [SMALL_STATE(705)] = 13944,
  [SMALL_STATE(706)] = 13954,
  [SMALL_STATE(707)] = 13964,
  [SMALL_STATE(708)] = 13974,
  [SMALL_STATE(709)] = 13984,
  [SMALL_STATE(710)] = 13994,
  [SMALL_STATE(711)] = 14004,
  [SMALL_STATE(712)] = 14014,
  [SMALL_STATE(713)] = 14024,
  [SMALL_STATE(714)] = 14034,
  [SMALL_STATE(715)] = 14044,
  [SMALL_STATE(716)] = 14054,
  [SMALL_STATE(717)] = 14064,
  [SMALL_STATE(718)] = 14074,
  [SMALL_STATE(719)] = 14084,
  [SMALL_STATE(720)] = 14094,
  [SMALL_STATE(721)] = 14104,
  [SMALL_STATE(722)] = 14114,
  [SMALL_STATE(723)] = 14124,
  [SMALL_STATE(724)] = 14134,
  [SMALL_STATE(725)] = 14144,
  [SMALL_STATE(726)] = 14154,
  [SMALL_STATE(727)] = 14164,
  [SMALL_STATE(728)] = 14174,
  [SMALL_STATE(729)] = 14184,
  [SMALL_STATE(730)] = 14194,
  [SMALL_STATE(731)] = 14204,
  [SMALL_STATE(732)] = 14214,
  [SMALL_STATE(733)] = 14224,
  [SMALL_STATE(734)] = 14234,
  [SMALL_STATE(735)] = 14244,
  [SMALL_STATE(736)] = 14254,
  [SMALL_STATE(737)] = 14264,
  [SMALL_STATE(738)] = 14274,
  [SMALL_STATE(739)] = 14284,
  [SMALL_STATE(740)] = 14294,
  [SMALL_STATE(741)] = 14304,
  [SMALL_STATE(742)] = 14314,
  [SMALL_STATE(743)] = 14324,
  [SMALL_STATE(744)] = 14334,
  [SMALL_STATE(745)] = 14344,
  [SMALL_STATE(746)] = 14354,
  [SMALL_STATE(747)] = 14364,
  [SMALL_STATE(748)] = 14374,
  [SMALL_STATE(749)] = 14384,
  [SMALL_STATE(750)] = 14394,
  [SMALL_STATE(751)] = 14404,
  [SMALL_STATE(752)] = 14414,
  [SMALL_STATE(753)] = 14424,
  [SMALL_STATE(754)] = 14434,
  [SMALL_STATE(755)] = 14444,
  [SMALL_STATE(756)] = 14454,
  [SMALL_STATE(757)] = 14464,
  [SMALL_STATE(758)] = 14474,
  [SMALL_STATE(759)] = 14484,
  [SMALL_STATE(760)] = 14494,
  [SMALL_STATE(761)] = 14504,
  [SMALL_STATE(762)] = 14514,
  [SMALL_STATE(763)] = 14524,
  [SMALL_STATE(764)] = 14534,
  [SMALL_STATE(765)] = 14544,
  [SMALL_STATE(766)] = 14554,
  [SMALL_STATE(767)] = 14564,
  [SMALL_STATE(768)] = 14574,
  [SMALL_STATE(769)] = 14584,
  [SMALL_STATE(770)] = 14594,
  [SMALL_STATE(771)] = 14604,
  [SMALL_STATE(772)] = 14614,
  [SMALL_STATE(773)] = 14624,
  [SMALL_STATE(774)] = 14634,
  [SMALL_STATE(775)] = 14644,
  [SMALL_STATE(776)] = 14654,
  [SMALL_STATE(777)] = 14664,
  [SMALL_STATE(778)] = 14674,
  [SMALL_STATE(779)] = 14684,
  [SMALL_STATE(780)] = 14694,
  [SMALL_STATE(781)] = 14704,
  [SMALL_STATE(782)] = 14714,
  [SMALL_STATE(783)] = 14724,
  [SMALL_STATE(784)] = 14734,
  [SMALL_STATE(785)] = 14744,
  [SMALL_STATE(786)] = 14754,
  [SMALL_STATE(787)] = 14764,
  [SMALL_STATE(788)] = 14774,
  [SMALL_STATE(789)] = 14784,
  [SMALL_STATE(790)] = 14794,
  [SMALL_STATE(791)] = 14804,
  [SMALL_STATE(792)] = 14814,
  [SMALL_STATE(793)] = 14824,
  [SMALL_STATE(794)] = 14834,
  [SMALL_STATE(795)] = 14844,
  [SMALL_STATE(796)] = 14854,
  [SMALL_STATE(797)] = 14864,
  [SMALL_STATE(798)] = 14874,
  [SMALL_STATE(799)] = 14884,
  [SMALL_STATE(800)] = 14894,
  [SMALL_STATE(801)] = 14904,
  [SMALL_STATE(802)] = 14914,
  [SMALL_STATE(803)] = 14924,
  [SMALL_STATE(804)] = 14934,
  [SMALL_STATE(805)] = 14944,
  [SMALL_STATE(806)] = 14954,
  [SMALL_STATE(807)] = 14964,
  [SMALL_STATE(808)] = 14974,
  [SMALL_STATE(809)] = 14984,
  [SMALL_STATE(810)] = 14994,
  [SMALL_STATE(811)] = 15004,
  [SMALL_STATE(812)] = 15014,
  [SMALL_STATE(813)] = 15024,
  [SMALL_STATE(814)] = 15034,
  [SMALL_STATE(815)] = 15044,
  [SMALL_STATE(816)] = 15054,
  [SMALL_STATE(817)] = 15064,
  [SMALL_STATE(818)] = 15074,
  [SMALL_STATE(819)] = 15084,
  [SMALL_STATE(820)] = 15094,
  [SMALL_STATE(821)] = 15104,
  [SMALL_STATE(822)] = 15114,
  [SMALL_STATE(823)] = 15124,
  [SMALL_STATE(824)] = 15134,
  [SMALL_STATE(825)] = 15144,
  [SMALL_STATE(826)] = 15154,
  [SMALL_STATE(827)] = 15164,
  [SMALL_STATE(828)] = 15174,
  [SMALL_STATE(829)] = 15184,
  [SMALL_STATE(830)] = 15194,
  [SMALL_STATE(831)] = 15204,
  [SMALL_STATE(832)] = 15214,
  [SMALL_STATE(833)] = 15224,
  [SMALL_STATE(834)] = 15234,
  [SMALL_STATE(835)] = 15244,
  [SMALL_STATE(836)] = 15254,
  [SMALL_STATE(837)] = 15264,
  [SMALL_STATE(838)] = 15274,
  [SMALL_STATE(839)] = 15284,
  [SMALL_STATE(840)] = 15294,
  [SMALL_STATE(841)] = 15304,
  [SMALL_STATE(842)] = 15314,
  [SMALL_STATE(843)] = 15324,
  [SMALL_STATE(844)] = 15334,
  [SMALL_STATE(845)] = 15344,
  [SMALL_STATE(846)] = 15354,
  [SMALL_STATE(847)] = 15364,
  [SMALL_STATE(848)] = 15374,
  [SMALL_STATE(849)] = 15384,
  [SMALL_STATE(850)] = 15394,
  [SMALL_STATE(851)] = 15404,
  [SMALL_STATE(852)] = 15414,
  [SMALL_STATE(853)] = 15424,
  [SMALL_STATE(854)] = 15434,
  [SMALL_STATE(855)] = 15444,
  [SMALL_STATE(856)] = 15454,
  [SMALL_STATE(857)] = 15464,
  [SMALL_STATE(858)] = 15474,
  [SMALL_STATE(859)] = 15484,
  [SMALL_STATE(860)] = 15494,
  [SMALL_STATE(861)] = 15504,
  [SMALL_STATE(862)] = 15514,
  [SMALL_STATE(863)] = 15524,
  [SMALL_STATE(864)] = 15534,
  [SMALL_STATE(865)] = 15544,
  [SMALL_STATE(866)] = 15554,
  [SMALL_STATE(867)] = 15564,
  [SMALL_STATE(868)] = 15574,
  [SMALL_STATE(869)] = 15584,
  [SMALL_STATE(870)] = 15594,
  [SMALL_STATE(871)] = 15604,
  [SMALL_STATE(872)] = 15614,
  [SMALL_STATE(873)] = 15624,
  [SMALL_STATE(874)] = 15634,
  [SMALL_STATE(875)] = 15644,
  [SMALL_STATE(876)] = 15654,
  [SMALL_STATE(877)] = 15664,
  [SMALL_STATE(878)] = 15674,
  [SMALL_STATE(879)] = 15684,
  [SMALL_STATE(880)] = 15694,
  [SMALL_STATE(881)] = 15704,
  [SMALL_STATE(882)] = 15714,
  [SMALL_STATE(883)] = 15724,
  [SMALL_STATE(884)] = 15734,
  [SMALL_STATE(885)] = 15744,
  [SMALL_STATE(886)] = 15754,
  [SMALL_STATE(887)] = 15764,
  [SMALL_STATE(888)] = 15774,
  [SMALL_STATE(889)] = 15784,
  [SMALL_STATE(890)] = 15794,
  [SMALL_STATE(891)] = 15804,
  [SMALL_STATE(892)] = 15814,
  [SMALL_STATE(893)] = 15824,
  [SMALL_STATE(894)] = 15834,
  [SMALL_STATE(895)] = 15844,
  [SMALL_STATE(896)] = 15854,
  [SMALL_STATE(897)] = 15864,
  [SMALL_STATE(898)] = 15874,
  [SMALL_STATE(899)] = 15884,
  [SMALL_STATE(900)] = 15894,
  [SMALL_STATE(901)] = 15904,
  [SMALL_STATE(902)] = 15914,
  [SMALL_STATE(903)] = 15924,
  [SMALL_STATE(904)] = 15934,
  [SMALL_STATE(905)] = 15944,
  [SMALL_STATE(906)] = 15954,
  [SMALL_STATE(907)] = 15964,
  [SMALL_STATE(908)] = 15974,
  [SMALL_STATE(909)] = 15984,
  [SMALL_STATE(910)] = 15994,
  [SMALL_STATE(911)] = 16004,
  [SMALL_STATE(912)] = 16014,
  [SMALL_STATE(913)] = 16024,
  [SMALL_STATE(914)] = 16034,
  [SMALL_STATE(915)] = 16044,
  [SMALL_STATE(916)] = 16054,
  [SMALL_STATE(917)] = 16064,
  [SMALL_STATE(918)] = 16074,
  [SMALL_STATE(919)] = 16084,
  [SMALL_STATE(920)] = 16094,
  [SMALL_STATE(921)] = 16104,
  [SMALL_STATE(922)] = 16114,
  [SMALL_STATE(923)] = 16124,
  [SMALL_STATE(924)] = 16134,
  [SMALL_STATE(925)] = 16144,
  [SMALL_STATE(926)] = 16154,
  [SMALL_STATE(927)] = 16164,
  [SMALL_STATE(928)] = 16174,
  [SMALL_STATE(929)] = 16184,
  [SMALL_STATE(930)] = 16194,
  [SMALL_STATE(931)] = 16204,
  [SMALL_STATE(932)] = 16214,
  [SMALL_STATE(933)] = 16224,
  [SMALL_STATE(934)] = 16234,
  [SMALL_STATE(935)] = 16244,
};

static const TSParseActionEntry ts_parse_actions[] = {
  [0] = {.entry = {.count = 0, .reusable = false}},
  [1] = {.entry = {.count = 1, .reusable = false}}, RECOVER(),
  [3] = {.entry = {.count = 1, .reusable = true}}, SHIFT(715),
  [5] = {.entry = {.count = 1, .reusable = false}}, SHIFT(635),
  [7] = {.entry = {.count = 1, .reusable = true}}, SHIFT(568),
  [9] = {.entry = {.count = 1, .reusable = true}}, SHIFT(25),
  [11] = {.entry = {.count = 1, .reusable = true}}, SHIFT(44),
  [13] = {.entry = {.count = 1, .reusable = true}}, SHIFT(43),
  [15] = {.entry = {.count = 1, .reusable = true}}, SHIFT(696),
  [17] = {.entry = {.count = 1, .reusable = true}}, SHIFT(369),
  [19] = {.entry = {.count = 1, .reusable = true}}, SHIFT(42),
  [21] = {.entry = {.count = 1, .reusable = true}}, SHIFT(45),
  [23] = {.entry = {.count = 1, .reusable = false}}, SHIFT(45),
  [25] = {.entry = {.count = 1, .reusable = true}}, SHIFT(546),
  [27] = {.entry = {.count = 1, .reusable = true}}, SHIFT(586),
  [29] = {.entry = {.count = 1, .reusable = true}}, SHIFT(55),
  [31] = {.entry = {.count = 1, .reusable = true}}, SHIFT(514),
  [33] = {.entry = {.count = 1, .reusable = true}}, SHIFT(515),
  [35] = {.entry = {.count = 1, .reusable = true}}, SHIFT(915),
  [37] = {.entry = {.count = 1, .reusable = true}}, SHIFT(516),
  [39] = {.entry = {.count = 1, .reusable = true}}, SHIFT(517),
  [41] = {.entry = {.count = 1, .reusable = true}}, SHIFT(518),
  [43] = {.entry = {.count = 1, .reusable = false}}, SHIFT(518),
  [45] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_value_comparison_operator, 1, 0, 0),
  [47] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_value_comparison_operator, 1, 0, 0),
  [49] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_dotted_identifier, 2, 0, 0),
  [51] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_dotted_identifier, 2, 0, 0),
  [53] = {.entry = {.count = 1, .reusable = true}}, SHIFT(900),
  [55] = {.entry = {.count = 1, .reusable = false}}, REDUCE(aux_sym_dotted_identifier_repeat1, 2, 0, 0),
  [57] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_dotted_identifier_repeat1, 2, 0, 0),
  [59] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_dotted_identifier_repeat1, 2, 0, 0), SHIFT_REPEAT(900),
  [62] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_field_identifier, 1, 0, 0),
  [64] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_field_identifier, 1, 0, 0),
  [66] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym__function_name, 1, 0, 1),
  [68] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_function_expression, 4, 0, 13),
  [70] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_function_expression, 4, 0, 13),
  [72] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_function_expression, 5, 0, 13),
  [74] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_function_expression, 5, 0, 13),
  [76] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_function_expression, 8, 0, 1),
  [78] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_function_expression, 8, 0, 1),
  [80] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym__value_expression, 1, 0, 0),
  [82] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym__value_expression, 1, 0, 0),
  [84] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 2, 0, 2),
  [86] = {.entry = {.count = 1, .reusable = true}}, SHIFT(242),
  [88] = {.entry = {.count = 1, .reusable = true}}, SHIFT(95),
  [90] = {.entry = {.count = 1, .reusable = true}}, SHIFT(186),
  [92] = {.entry = {.count = 1, .reusable = true}}, SHIFT(363),
  [94] = {.entry = {.count = 1, .reusable = true}}, SHIFT(397),
  [96] = {.entry = {.count = 1, .reusable = true}}, SHIFT(28),
  [98] = {.entry = {.count = 1, .reusable = true}}, SHIFT(577),
  [100] = {.entry = {.count = 1, .reusable = true}}, SHIFT(572),
  [102] = {.entry = {.count = 1, .reusable = true}}, SHIFT(157),
  [104] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_sosl_query_body, 1, 0, 0),
  [106] = {.entry = {.count = 1, .reusable = true}}, SHIFT(239),
  [108] = {.entry = {.count = 1, .reusable = true}}, SHIFT(567),
  [110] = {.entry = {.count = 1, .reusable = true}}, SHIFT(346),
  [112] = {.entry = {.count = 1, .reusable = true}}, SHIFT(23),
  [114] = {.entry = {.count = 1, .reusable = false}}, SHIFT(582),
  [116] = {.entry = {.count = 1, .reusable = true}}, SHIFT(8),
  [118] = {.entry = {.count = 1, .reusable = false}}, SHIFT(8),
  [120] = {.entry = {.count = 1, .reusable = true}}, SHIFT(582),
  [122] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 3, 0, 3),
  [124] = {.entry = {.count = 1, .reusable = true}}, SHIFT(681),
  [126] = {.entry = {.count = 1, .reusable = true}}, SHIFT(258),
  [128] = {.entry = {.count = 1, .reusable = true}}, SHIFT(707),
  [130] = {.entry = {.count = 1, .reusable = true}}, SHIFT(21),
  [132] = {.entry = {.count = 1, .reusable = true}}, SHIFT(717),
  [134] = {.entry = {.count = 1, .reusable = true}}, SHIFT(229),
  [136] = {.entry = {.count = 1, .reusable = true}}, SHIFT(791),
  [138] = {.entry = {.count = 1, .reusable = true}}, SHIFT(579),
  [140] = {.entry = {.count = 1, .reusable = false}}, SHIFT(792),
  [142] = {.entry = {.count = 1, .reusable = true}}, SHIFT(544),
  [144] = {.entry = {.count = 1, .reusable = false}}, SHIFT(569),
  [146] = {.entry = {.count = 1, .reusable = false}}, SHIFT(932),
  [148] = {.entry = {.count = 1, .reusable = false}}, SHIFT(237),
  [150] = {.entry = {.count = 1, .reusable = false}}, SHIFT(865),
  [152] = {.entry = {.count = 1, .reusable = false}}, SHIFT(72),
  [154] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_subquery, 3, 0, 0),
  [156] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_subquery, 3, 0, 0),
  [158] = {.entry = {.count = 1, .reusable = true}}, SHIFT(31),
  [160] = {.entry = {.count = 1, .reusable = false}}, SHIFT(93),
  [162] = {.entry = {.count = 1, .reusable = false}}, SHIFT(933),
  [164] = {.entry = {.count = 1, .reusable = false}}, SHIFT(12),
  [166] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_sosl_query_body, 2, 0, 0),
  [168] = {.entry = {.count = 1, .reusable = true}}, SHIFT(30),
  [170] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_storage_identifier, 1, 0, 0),
  [172] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_storage_identifier, 1, 0, 0),
  [174] = {.entry = {.count = 1, .reusable = true}}, SHIFT(914),
  [176] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_dotted_identifier_repeat1, 2, 0, 0), SHIFT_REPEAT(914),
  [179] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 4, 0, 16),
  [181] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_sosl_query_body, 3, 0, 0),
  [183] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 3, 0, 6),
  [185] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_null_literal, 1, 0, 0),
  [187] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_null_literal, 1, 0, 0),
  [189] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_date_literal, 1, 0, 0),
  [191] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_date_literal, 1, 0, 0),
  [193] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_boolean, 1, 0, 0),
  [195] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_boolean, 1, 0, 0),
  [197] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym__soql_literal, 1, 0, 0),
  [199] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym__soql_literal, 1, 0, 0),
  [201] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_date_literal_with_param, 3, 0, 0),
  [203] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_date_literal_with_param, 3, 0, 0),
  [205] = {.entry = {.count = 1, .reusable = true}}, SHIFT(348),
  [207] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_from_clause, 2, 0, 0),
  [209] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_from_clause, 2, 0, 0),
  [211] = {.entry = {.count = 1, .reusable = false}}, SHIFT(607),
  [213] = {.entry = {.count = 1, .reusable = false}}, SHIFT(84),
  [215] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 4, 0, 30),
  [217] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 4, 0, 17),
  [219] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_from_clause_repeat1, 2, 0, 0),
  [221] = {.entry = {.count = 1, .reusable = false}}, REDUCE(aux_sym_from_clause_repeat1, 2, 0, 0),
  [223] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 3, 0, 7),
  [225] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 58),
  [227] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym__value_comparison, 2, 0, 0),
  [229] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym__value_comparison, 2, 0, 0),
  [231] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_from_clause, 3, 0, 0),
  [233] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_with_data_cat_expression_repeat1, 2, 0, 0),
  [235] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_with_data_cat_expression_repeat1, 2, 0, 0), SHIFT_REPEAT(585),
  [238] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_comparable_list, 4, 0, 0),
  [240] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_comparable_list, 4, 0, 0),
  [242] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_from_clause_repeat1, 2, 0, 0), SHIFT_REPEAT(348),
  [245] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym__boolean_expression, 1, 0, 0),
  [247] = {.entry = {.count = 1, .reusable = true}}, SHIFT(105),
  [249] = {.entry = {.count = 1, .reusable = false}}, SHIFT(102),
  [251] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym__set_comparison, 2, 0, 0),
  [253] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym__set_comparison, 2, 0, 0),
  [255] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym__comparison, 1, 0, 0),
  [257] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym__comparison, 1, 0, 0),
  [259] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_order_expression, 1, 0, 0),
  [261] = {.entry = {.count = 1, .reusable = true}}, SHIFT(160),
  [263] = {.entry = {.count = 1, .reusable = true}}, SHIFT(246),
  [265] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_with_data_cat_expression, 3, 0, 0),
  [267] = {.entry = {.count = 1, .reusable = true}}, SHIFT(585),
  [269] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_comparable_list, 3, 0, 0),
  [271] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_comparable_list, 3, 0, 0),
  [273] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_with_data_cat_expression, 2, 0, 0),
  [275] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_sosl_query_body, 4, 0, 0),
  [277] = {.entry = {.count = 1, .reusable = true}}, SHIFT(642),
  [279] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_with_data_cat_filter, 6, 0, 0),
  [281] = {.entry = {.count = 1, .reusable = true}}, SHIFT(177),
  [283] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_storage_alias, 3, 0, 0),
  [285] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 56),
  [287] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 63),
  [289] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 94),
  [291] = {.entry = {.count = 1, .reusable = true}}, SHIFT(565),
  [293] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_using_lookup_bind_clause, 2, 0, 0),
  [295] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 150),
  [297] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_using_lookup_bind_clause, 3, 0, 0),
  [299] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_using_lookup_bind_clause_repeat1, 2, 0, 0), SHIFT_REPEAT(565),
  [302] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_using_lookup_bind_clause_repeat1, 2, 0, 0),
  [304] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_with_data_cat_filter, 5, 0, 0),
  [306] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_storage_alias, 2, 0, 0),
  [308] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 3, 0, 4),
  [310] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 4, 0, 14),
  [312] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_using_lookup_clause, 2, 0, 12),
  [314] = {.entry = {.count = 1, .reusable = true}}, SHIFT(564),
  [316] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_with_data_cat_filter, 3, 0, 0),
  [318] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 4, 0, 28),
  [320] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 4, 0, 35),
  [322] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym__condition_expression, 3, 0, 0),
  [324] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym__condition_expression, 3, 0, 0),
  [326] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_comparison_expression, 2, 0, 0),
  [328] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_comparison_expression, 2, 0, 0),
  [330] = {.entry = {.count = 1, .reusable = true}}, SHIFT(233),
  [332] = {.entry = {.count = 1, .reusable = true}}, SHIFT(616),
  [334] = {.entry = {.count = 1, .reusable = true}}, SHIFT(618),
  [336] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym__condition_expression, 1, 0, 0),
  [338] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym__condition_expression, 1, 0, 0),
  [340] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_or_expression_repeat1, 2, 0, 0),
  [342] = {.entry = {.count = 2, .reusable = false}}, REDUCE(aux_sym_or_expression_repeat1, 2, 0, 0), SHIFT_REPEAT(102),
  [345] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_and_expression_repeat1, 2, 0, 0),
  [347] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_and_expression_repeat1, 2, 0, 0), SHIFT_REPEAT(105),
  [350] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_and_expression, 2, 0, 0),
  [352] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_dotted_identifier_repeat1, 2, 0, 0), SHIFT_REPEAT(642),
  [355] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_using_lookup_bind_expression, 3, 0, 386),
  [357] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_or_expression, 2, 0, 0),
  [359] = {.entry = {.count = 1, .reusable = true}}, SHIFT(253),
  [361] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym__group_by_expression, 1, 0, 0),
  [363] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 4, 0, 34),
  [365] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 4, 0, 40),
  [367] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_using_lookup_clause, 3, 0, 12),
  [369] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_using_listview_clause, 3, 0, 0),
  [371] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_returning_clause_repeat1, 2, 0, 0),
  [373] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_returning_clause_repeat1, 2, 0, 0), SHIFT_REPEAT(575),
  [376] = {.entry = {.count = 1, .reusable = false}}, REDUCE(aux_sym_or_expression_repeat1, 2, 0, 0),
  [378] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_using_clause, 2, 0, 0),
  [380] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym__group_by_expression, 2, 0, 0),
  [382] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 54),
  [384] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 3, 0, 11),
  [386] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 62),
  [388] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_using_scope_type, 1, 0, 0),
  [390] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 68),
  [392] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 4, 0, 21),
  [394] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 92),
  [396] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_using_scope_clause, 2, 0, 0),
  [398] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 99),
  [400] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 113),
  [402] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym__group_by_expression_repeat1, 2, 0, 0), SHIFT_REPEAT(253),
  [405] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym__group_by_expression_repeat1, 2, 0, 0),
  [407] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 148),
  [409] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 155),
  [411] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 169),
  [413] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 214),
  [415] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 284),
  [417] = {.entry = {.count = 1, .reusable = true}}, SHIFT(170),
  [419] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_order_expression, 2, 0, 0),
  [421] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_sosl_query_body_repeat1, 2, 0, 0),
  [423] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_sosl_query_body_repeat1, 2, 0, 0), SHIFT_REPEAT(567),
  [426] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_returning_clause, 3, 0, 0),
  [428] = {.entry = {.count = 1, .reusable = true}}, SHIFT(575),
  [430] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 4, 0, 26),
  [432] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_sobject_return, 1, 0, 0),
  [434] = {.entry = {.count = 1, .reusable = true}}, SHIFT(26),
  [436] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_returning_clause, 2, 0, 0),
  [438] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_sobject_return, 8, 0, 0),
  [440] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_fields_expression, 4, 0, 0),
  [442] = {.entry = {.count = 1, .reusable = true}}, SHIFT(32),
  [444] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_selected_fields, 1, 0, 0),
  [446] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_not_expression, 2, 0, 0),
  [448] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym__selectable_expression, 1, 0, 0),
  [450] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_type_of_clause, 4, 0, 0),
  [452] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_sobject_return, 7, 0, 0),
  [454] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_where_clause, 2, 0, 0),
  [456] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_sobject_return, 9, 0, 0),
  [458] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_order_direction, 1, 0, 0),
  [460] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_type_of_clause, 3, 0, 0),
  [462] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_type_of_clause, 5, 0, 0),
  [464] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_sosl_query_body_repeat2, 2, 0, 0),
  [466] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_sosl_query_body_repeat2, 2, 0, 0), SHIFT_REPEAT(23),
  [469] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_find_clause, 4, 0, 0),
  [471] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_sobject_return, 6, 0, 0),
  [473] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym__selectable_expression, 1, 0, 0),
  [475] = {.entry = {.count = 1, .reusable = false}}, SHIFT(175),
  [477] = {.entry = {.count = 1, .reusable = true}}, SHIFT(122),
  [479] = {.entry = {.count = 1, .reusable = false}}, SHIFT(122),
  [481] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_selected_fields, 2, 0, 0),
  [483] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_sobject_return, 5, 0, 0),
  [485] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_selected_fields_repeat1, 2, 0, 0),
  [487] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_group_by_clause, 2, 0, 0),
  [489] = {.entry = {.count = 1, .reusable = true}}, SHIFT(34),
  [491] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_find_clause, 2, 0, 0),
  [493] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_selected_fields_repeat1, 2, 0, 0), SHIFT_REPEAT(32),
  [496] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_alias_expression, 2, 0, 0),
  [498] = {.entry = {.count = 1, .reusable = true}}, SHIFT(603),
  [500] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_sobject_return, 4, 0, 0),
  [502] = {.entry = {.count = 1, .reusable = true}}, SHIFT(161),
  [504] = {.entry = {.count = 1, .reusable = true}}, SHIFT(611),
  [506] = {.entry = {.count = 1, .reusable = true}}, SHIFT(476),
  [508] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 4, 0, 18),
  [510] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 59),
  [512] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_sosl_query_body, 5, 0, 0),
  [514] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 65),
  [516] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_with_clause, 2, 0, 0),
  [518] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 75),
  [520] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 85),
  [522] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 89),
  [524] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 96),
  [526] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_sosl_query_body_repeat1, 1, 0, 0),
  [528] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 106),
  [530] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 110),
  [532] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 121),
  [534] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 3, 0, 8),
  [536] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_with_user_id_type, 3, 0, 0),
  [538] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 4, 0, 23),
  [540] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 141),
  [542] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 145),
  [544] = {.entry = {.count = 1, .reusable = true}}, SHIFT(583),
  [546] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_field_list, 1, 0, 0),
  [548] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 152),
  [550] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 4, 0, 31),
  [552] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 162),
  [554] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 166),
  [556] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 177),
  [558] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 207),
  [560] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 211),
  [562] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 222),
  [564] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 242),
  [566] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 4, 0, 37),
  [568] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 4, 0, 47),
  [570] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_with_record_visibility_expression, 4, 0, 0),
  [572] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 277),
  [574] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 281),
  [576] = {.entry = {.count = 1, .reusable = true}}, SHIFT(159),
  [578] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_order_by_clause, 2, 0, 0),
  [580] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 292),
  [582] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 312),
  [584] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 352),
  [586] = {.entry = {.count = 1, .reusable = true}}, SHIFT(165),
  [588] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_with_record_visibility_expression, 5, 0, 0),
  [590] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 409),
  [592] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_in_clause, 3, 0, 0),
  [594] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_order_by_clause_repeat1, 2, 0, 0), SHIFT_REPEAT(159),
  [597] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_order_by_clause_repeat1, 2, 0, 0),
  [599] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_order_by_clause, 3, 0, 0),
  [601] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_with_snippet_expression, 1, 0, 0),
  [603] = {.entry = {.count = 1, .reusable = true}}, SHIFT(641),
  [605] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 51),
  [607] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_with_type, 1, 0, 0),
  [609] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_sosl_with_clause, 2, 0, 0),
  [611] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_field_list_repeat1, 2, 0, 0),
  [613] = {.entry = {.count = 1, .reusable = false}}, SHIFT(868),
  [615] = {.entry = {.count = 1, .reusable = true}}, SHIFT(872),
  [617] = {.entry = {.count = 1, .reusable = false}}, SHIFT(368),
  [619] = {.entry = {.count = 1, .reusable = true}}, SHIFT(910),
  [621] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_with_network_expression, 2, 0, 0),
  [623] = {.entry = {.count = 1, .reusable = true}}, SHIFT(888),
  [625] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_with_metadata_expression, 3, 0, 0),
  [627] = {.entry = {.count = 1, .reusable = true}}, SHIFT(168),
  [629] = {.entry = {.count = 1, .reusable = true}}, SHIFT(576),
  [631] = {.entry = {.count = 1, .reusable = true}}, SHIFT(676),
  [633] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_order_expression, 3, 0, 0),
  [635] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_having_clause, 2, 0, 0),
  [637] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_order_null_direction, 1, 0, 0),
  [639] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_with_pricebook_expression, 3, 0, 0),
  [641] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_with_spell_correction_expression, 3, 0, 0),
  [643] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_with_division_expression, 3, 0, 0),
  [645] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_sosl_with_type, 1, 0, 0),
  [647] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_group_by_clause, 3, 0, 0),
  [649] = {.entry = {.count = 1, .reusable = true}}, SHIFT(149),
  [651] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_sosl_query_body_repeat2, 1, 0, 0),
  [653] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_with_snippet_expression, 6, 0, 0),
  [655] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_with_highlight, 1, 0, 0),
  [657] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 80),
  [659] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 3, 0, 9),
  [661] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_type_of_clause_repeat1, 2, 0, 0),
  [663] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_type_of_clause_repeat1, 2, 0, 0), SHIFT_REPEAT(611),
  [666] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 52),
  [668] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_sosl_query_body, 6, 0, 0),
  [670] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 60),
  [672] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 66),
  [674] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 70),
  [676] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 76),
  [678] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 86),
  [680] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 90),
  [682] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 97),
  [684] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 101),
  [686] = {.entry = {.count = 1, .reusable = true}}, SHIFT(150),
  [688] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 107),
  [690] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 111),
  [692] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 116),
  [694] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 122),
  [696] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 130),
  [698] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_field_list_repeat1, 2, 0, 0), SHIFT_REPEAT(583),
  [701] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 136),
  [703] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 142),
  [705] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 146),
  [707] = {.entry = {.count = 1, .reusable = true}}, SHIFT(574),
  [709] = {.entry = {.count = 1, .reusable = false}}, SHIFT(574),
  [711] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 153),
  [713] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 157),
  [715] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 163),
  [717] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 167),
  [719] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 172),
  [721] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 178),
  [723] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 186),
  [725] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 196),
  [727] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 202),
  [729] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 208),
  [731] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 217),
  [733] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 4, 0, 19),
  [735] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 223),
  [737] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 231),
  [739] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 237),
  [741] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 243),
  [743] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 251),
  [745] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 4, 0, 24),
  [747] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 266),
  [749] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 272),
  [751] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 278),
  [753] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 282),
  [755] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 287),
  [757] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 293),
  [759] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 301),
  [761] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 307),
  [763] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 4, 0, 32),
  [765] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 313),
  [767] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 321),
  [769] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 341),
  [771] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 347),
  [773] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 353),
  [775] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 361),
  [777] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 376),
  [779] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 4, 0, 38),
  [781] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 398),
  [783] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 404),
  [785] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 410),
  [787] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 418),
  [789] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 433),
  [791] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 455),
  [793] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 9, 0, 483),
  [795] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 4, 0, 42),
  [797] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 4, 0, 48),
  [799] = {.entry = {.count = 1, .reusable = true}}, SHIFT(104),
  [801] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_field_list, 2, 0, 0),
  [803] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_dotted_identifier_repeat1, 2, 0, 0), SHIFT_REPEAT(910),
  [806] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_limit_clause, 2, 0, 0),
  [808] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 212),
  [810] = {.entry = {.count = 1, .reusable = true}}, SHIFT(360),
  [812] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_for_clause, 2, 0, 0),
  [814] = {.entry = {.count = 1, .reusable = true}}, SHIFT(897),
  [816] = {.entry = {.count = 1, .reusable = true}}, SHIFT(622),
  [818] = {.entry = {.count = 1, .reusable = true}}, SHIFT(625),
  [820] = {.entry = {.count = 1, .reusable = true}}, SHIFT(352),
  [822] = {.entry = {.count = 1, .reusable = true}}, SHIFT(355),
  [824] = {.entry = {.count = 1, .reusable = true}}, SHIFT(2),
  [826] = {.entry = {.count = 1, .reusable = true}}, SHIFT(63),
  [828] = {.entry = {.count = 1, .reusable = true}}, SHIFT(33),
  [830] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_for_clause_repeat1, 2, 0, 0), SHIFT_REPEAT(360),
  [833] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_for_clause_repeat1, 2, 0, 0),
  [835] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_update_clause, 3, 0, 0),
  [837] = {.entry = {.count = 1, .reusable = true}}, SHIFT(425),
  [839] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_using_phrase_search, 1, 0, 0),
  [841] = {.entry = {.count = 1, .reusable = true}}, SHIFT(929),
  [843] = {.entry = {.count = 1, .reusable = true}}, SHIFT(368),
  [845] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_using_advanced_search, 1, 0, 0),
  [847] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_sosl_using_clause, 2, 0, 0),
  [849] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_update_clause_repeat1, 2, 0, 0),
  [851] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_update_clause_repeat1, 2, 0, 0), SHIFT_REPEAT(425),
  [854] = {.entry = {.count = 1, .reusable = true}}, SHIFT(636),
  [856] = {.entry = {.count = 1, .reusable = true}}, SHIFT(556),
  [858] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_offset_clause, 2, 0, 0),
  [860] = {.entry = {.count = 1, .reusable = true}}, SHIFT(3),
  [862] = {.entry = {.count = 1, .reusable = true}}, SHIFT(519),
  [864] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_for_clause, 3, 0, 0),
  [866] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_update_clause, 2, 0, 0),
  [868] = {.entry = {.count = 1, .reusable = true}}, SHIFT(928),
  [870] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 289),
  [872] = {.entry = {.count = 1, .reusable = true}}, SHIFT(4),
  [874] = {.entry = {.count = 1, .reusable = true}}, SHIFT(67),
  [876] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 171),
  [878] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 174),
  [880] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 176),
  [882] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 4, 0, 15),
  [884] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 181),
  [886] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 185),
  [888] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 95),
  [890] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 188),
  [892] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 191),
  [894] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 195),
  [896] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 198),
  [898] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 201),
  [900] = {.entry = {.count = 1, .reusable = true}}, SHIFT(37),
  [902] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_select_clause, 2, 0, 0),
  [904] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 204),
  [906] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 206),
  [908] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 100),
  [910] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 210),
  [912] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_with_data_cat_filter_repeat1, 2, 0, 0), SHIFT_REPEAT(920),
  [915] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_with_data_cat_filter_repeat1, 2, 0, 0),
  [917] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 103),
  [919] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 216),
  [921] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 105),
  [923] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 219),
  [925] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 221),
  [927] = {.entry = {.count = 1, .reusable = true}}, SHIFT(443),
  [929] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 50),
  [931] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 226),
  [933] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 230),
  [935] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 109),
  [937] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 233),
  [939] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 236),
  [941] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 239),
  [943] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 241),
  [945] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 64),
  [947] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 246),
  [949] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 250),
  [951] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 115),
  [953] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 253),
  [955] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 257),
  [957] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 4, 0, 22),
  [959] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 118),
  [961] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 120),
  [963] = {.entry = {.count = 1, .reusable = true}}, SHIFT(230),
  [965] = {.entry = {.count = 1, .reusable = true}}, SHIFT(167),
  [967] = {.entry = {.count = 1, .reusable = true}}, SHIFT(345),
  [969] = {.entry = {.count = 1, .reusable = true}}, SHIFT(223),
  [971] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 261),
  [973] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 265),
  [975] = {.entry = {.count = 1, .reusable = true}}, SHIFT(154),
  [977] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 268),
  [979] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 271),
  [981] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 274),
  [983] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 276),
  [985] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 125),
  [987] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 280),
  [989] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 4, 0, 29),
  [991] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 129),
  [993] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 286),
  [995] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 291),
  [997] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 132),
  [999] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_when_expression, 4, 0, 0),
  [1001] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 296),
  [1003] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 300),
  [1005] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 303),
  [1007] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 306),
  [1009] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 69),
  [1011] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 309),
  [1013] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 311),
  [1015] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 316),
  [1017] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 320),
  [1019] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_update_type, 1, 0, 0),
  [1021] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 323),
  [1023] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 327),
  [1025] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 332),
  [1027] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 336),
  [1029] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 340),
  [1031] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_comparable_list_repeat1, 2, 0, 0), SHIFT_REPEAT(4),
  [1034] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_comparable_list_repeat1, 2, 0, 0),
  [1036] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 343),
  [1038] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 346),
  [1040] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 72),
  [1042] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 349),
  [1044] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 351),
  [1046] = {.entry = {.count = 1, .reusable = true}}, SHIFT(59),
  [1048] = {.entry = {.count = 1, .reusable = true}}, SHIFT(214),
  [1050] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 356),
  [1052] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 360),
  [1054] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 363),
  [1056] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 367),
  [1058] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 371),
  [1060] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 375),
  [1062] = {.entry = {.count = 1, .reusable = true}}, SHIFT(920),
  [1064] = {.entry = {.count = 1, .reusable = true}}, SHIFT(73),
  [1066] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 378),
  [1068] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 382),
  [1070] = {.entry = {.count = 1, .reusable = true}}, SHIFT(83),
  [1072] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 4, 0, 36),
  [1074] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_sosl_query_body, 7, 0, 0),
  [1076] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 74),
  [1078] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_function_expression_repeat1, 2, 0, 0), SHIFT_REPEAT(230),
  [1081] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_function_expression_repeat1, 2, 0, 0),
  [1083] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_with_record_visibility_expression_repeat1, 2, 0, 0), SHIFT_REPEAT(345),
  [1086] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_with_record_visibility_expression_repeat1, 2, 0, 0),
  [1088] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 389),
  [1090] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 393),
  [1092] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 397),
  [1094] = {.entry = {.count = 1, .reusable = true}}, SHIFT(200),
  [1096] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 400),
  [1098] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 403),
  [1100] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 135),
  [1102] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 406),
  [1104] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 408),
  [1106] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 4, 0, 41),
  [1108] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 413),
  [1110] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 417),
  [1112] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 138),
  [1114] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 420),
  [1116] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 424),
  [1118] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 428),
  [1120] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 432),
  [1122] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 140),
  [1124] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 435),
  [1126] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 439),
  [1128] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 446),
  [1130] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 450),
  [1132] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 454),
  [1134] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 457),
  [1136] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 461),
  [1138] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 467),
  [1140] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 9, 0, 474),
  [1142] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 9, 0, 478),
  [1144] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 9, 0, 482),
  [1146] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 79),
  [1148] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 9, 0, 485),
  [1150] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 9, 0, 489),
  [1152] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 9, 0, 495),
  [1154] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 9, 0, 502),
  [1156] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 10, 0, 510),
  [1158] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 144),
  [1160] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 4, 0, 44),
  [1162] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 4, 0, 46),
  [1164] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_select_clause, 3, 0, 0),
  [1166] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_type_of_clause_repeat1, 1, 0, 0),
  [1168] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_selected_fields_repeat1, 2, 0, 0), SHIFT_REPEAT(37),
  [1171] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_or_expression_repeat1, 2, 0, 0), SHIFT_REPEAT(104),
  [1174] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 82),
  [1176] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 151),
  [1178] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 84),
  [1180] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 156),
  [1182] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 57),
  [1184] = {.entry = {.count = 1, .reusable = true}}, SHIFT(528),
  [1186] = {.entry = {.count = 1, .reusable = true}}, SHIFT(531),
  [1188] = {.entry = {.count = 1, .reusable = true}}, SHIFT(532),
  [1190] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 159),
  [1192] = {.entry = {.count = 1, .reusable = true}}, SHIFT(535),
  [1194] = {.entry = {.count = 1, .reusable = true}}, SHIFT(14),
  [1196] = {.entry = {.count = 1, .reusable = true}}, SHIFT(15),
  [1198] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 161),
  [1200] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 3, 0, 5),
  [1202] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_for_type, 1, 0, 0),
  [1204] = {.entry = {.count = 1, .reusable = true}}, SHIFT(143),
  [1206] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 88),
  [1208] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 165),
  [1210] = {.entry = {.count = 1, .reusable = true}}, SHIFT(354),
  [1212] = {.entry = {.count = 1, .reusable = true}}, SHIFT(860),
  [1214] = {.entry = {.count = 1, .reusable = true}}, SHIFT(88),
  [1216] = {.entry = {.count = 1, .reusable = true}}, SHIFT(615),
  [1218] = {.entry = {.count = 1, .reusable = true}}, SHIFT(250),
  [1220] = {.entry = {.count = 1, .reusable = true}}, SHIFT(140),
  [1222] = {.entry = {.count = 1, .reusable = true}}, SHIFT(606),
  [1224] = {.entry = {.count = 1, .reusable = true}}, SHIFT(173),
  [1226] = {.entry = {.count = 1, .reusable = true}}, SHIFT(178),
  [1228] = {.entry = {.count = 1, .reusable = true}}, SHIFT(912),
  [1230] = {.entry = {.count = 1, .reusable = true}}, SHIFT(361),
  [1232] = {.entry = {.count = 1, .reusable = true}}, SHIFT(808),
  [1234] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_with_data_cat_filter_type, 1, 0, 0),
  [1236] = {.entry = {.count = 1, .reusable = true}}, SHIFT(570),
  [1238] = {.entry = {.count = 1, .reusable = true}}, SHIFT(337),
  [1240] = {.entry = {.count = 1, .reusable = true}}, SHIFT(285),
  [1242] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_with_record_visibility_param, 3, 0, 0),
  [1244] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_set_comparison_operator, 1, 0, 0),
  [1246] = {.entry = {.count = 1, .reusable = true}}, SHIFT(235),
  [1248] = {.entry = {.count = 1, .reusable = true}}, SHIFT(909),
  [1250] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 260),
  [1252] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 126),
  [1254] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 262),
  [1256] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 263),
  [1258] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 264),
  [1260] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 127),
  [1262] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 128),
  [1264] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 267),
  [1266] = {.entry = {.count = 1, .reusable = true}}, SHIFT(226),
  [1268] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 269),
  [1270] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 270),
  [1272] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_sosl_query_body, 8, 0, 0),
  [1274] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 131),
  [1276] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 273),
  [1278] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_count_expression, 3, 0, 13),
  [1280] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 275),
  [1282] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 133),
  [1284] = {.entry = {.count = 1, .reusable = false}}, SHIFT(715),
  [1286] = {.entry = {.count = 1, .reusable = false}}, SHIFT(867),
  [1288] = {.entry = {.count = 1, .reusable = true}}, SHIFT(75),
  [1290] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 279),
  [1292] = {.entry = {.count = 1, .reusable = true}},  ACCEPT_INPUT(),
  [1294] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_source_file, 2, 0, 0),
  [1296] = {.entry = {.count = 1, .reusable = true}}, SHIFT(656),
  [1298] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 283),
  [1300] = {.entry = {.count = 1, .reusable = true}}, SHIFT(916),
  [1302] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 285),
  [1304] = {.entry = {.count = 1, .reusable = true}}, SHIFT(7),
  [1306] = {.entry = {.count = 1, .reusable = true}}, SHIFT(629),
  [1308] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 288),
  [1310] = {.entry = {.count = 1, .reusable = true}}, SHIFT(350),
  [1312] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 290),
  [1314] = {.entry = {.count = 1, .reusable = true}}, SHIFT(153),
  [1316] = {.entry = {.count = 1, .reusable = true}}, SHIFT(27),
  [1318] = {.entry = {.count = 1, .reusable = true}}, SHIFT(926),
  [1320] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 294),
  [1322] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 295),
  [1324] = {.entry = {.count = 1, .reusable = true}}, SHIFT(414),
  [1326] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 297),
  [1328] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 298),
  [1330] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 299),
  [1332] = {.entry = {.count = 1, .reusable = true}}, SHIFT(196),
  [1334] = {.entry = {.count = 1, .reusable = true}}, SHIFT(225),
  [1336] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 302),
  [1338] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 304),
  [1340] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 305),
  [1342] = {.entry = {.count = 1, .reusable = false}}, SHIFT(862),
  [1344] = {.entry = {.count = 1, .reusable = true}}, SHIFT(256),
  [1346] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 308),
  [1348] = {.entry = {.count = 1, .reusable = true}}, SHIFT(247),
  [1350] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 310),
  [1352] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 134),
  [1354] = {.entry = {.count = 1, .reusable = true}}, SHIFT(765),
  [1356] = {.entry = {.count = 1, .reusable = true}}, SHIFT(116),
  [1358] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 314),
  [1360] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 315),
  [1362] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 137),
  [1364] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 317),
  [1366] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 318),
  [1368] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 319),
  [1370] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 3, 0, 10),
  [1372] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 139),
  [1374] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 322),
  [1376] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_source_file, 1, 0, 0),
  [1378] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 324),
  [1380] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 325),
  [1382] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 326),
  [1384] = {.entry = {.count = 1, .reusable = true}}, SHIFT(537),
  [1386] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 328),
  [1388] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 329),
  [1390] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 330),
  [1392] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 331),
  [1394] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 53),
  [1396] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 333),
  [1398] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 334),
  [1400] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 335),
  [1402] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 143),
  [1404] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 337),
  [1406] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 338),
  [1408] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 339),
  [1410] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 4, 0, 20),
  [1412] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 55),
  [1414] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 342),
  [1416] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 344),
  [1418] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 345),
  [1420] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 147),
  [1422] = {.entry = {.count = 1, .reusable = true}}, SHIFT(726),
  [1424] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 348),
  [1426] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 149),
  [1428] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 350),
  [1430] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_else_expression, 2, 0, 0),
  [1432] = {.entry = {.count = 1, .reusable = true}}, SHIFT(566),
  [1434] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 4, 0, 25),
  [1436] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 354),
  [1438] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 355),
  [1440] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 61),
  [1442] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 357),
  [1444] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 358),
  [1446] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 359),
  [1448] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 154),
  [1450] = {.entry = {.count = 1, .reusable = true}}, SHIFT(162),
  [1452] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 362),
  [1454] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 4, 0, 27),
  [1456] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 364),
  [1458] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 365),
  [1460] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 366),
  [1462] = {.entry = {.count = 1, .reusable = true}}, SHIFT(820),
  [1464] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 368),
  [1466] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 369),
  [1468] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 370),
  [1470] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 158),
  [1472] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 372),
  [1474] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 373),
  [1476] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 374),
  [1478] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym__query_expression, 1, 0, 0),
  [1480] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 160),
  [1482] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 377),
  [1484] = {.entry = {.count = 1, .reusable = true}}, SHIFT(885),
  [1486] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 379),
  [1488] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 380),
  [1490] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 381),
  [1492] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 67),
  [1494] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 383),
  [1496] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 384),
  [1498] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 7, 0, 385),
  [1500] = {.entry = {.count = 1, .reusable = false}}, SHIFT(935),
  [1502] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 164),
  [1504] = {.entry = {.count = 1, .reusable = true}}, SHIFT(638),
  [1506] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 4, 0, 33),
  [1508] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 71),
  [1510] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 168),
  [1512] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 170),
  [1514] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 73),
  [1516] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 387),
  [1518] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 388),
  [1520] = {.entry = {.count = 1, .reusable = true}}, SHIFT(112),
  [1522] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 390),
  [1524] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 391),
  [1526] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 392),
  [1528] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 173),
  [1530] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 394),
  [1532] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 395),
  [1534] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 396),
  [1536] = {.entry = {.count = 1, .reusable = true}}, SHIFT(92),
  [1538] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 175),
  [1540] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 399),
  [1542] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 401),
  [1544] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 402),
  [1546] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 77),
  [1548] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 78),
  [1550] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 405),
  [1552] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 179),
  [1554] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 407),
  [1556] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 180),
  [1558] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 182),
  [1560] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 411),
  [1562] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 412),
  [1564] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 183),
  [1566] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 414),
  [1568] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 415),
  [1570] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 416),
  [1572] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 184),
  [1574] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 4, 0, 39),
  [1576] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 419),
  [1578] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 81),
  [1580] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 421),
  [1582] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 422),
  [1584] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 423),
  [1586] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 187),
  [1588] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 425),
  [1590] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 426),
  [1592] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 427),
  [1594] = {.entry = {.count = 1, .reusable = true}}, SHIFT(857),
  [1596] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 429),
  [1598] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 430),
  [1600] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 431),
  [1602] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 189),
  [1604] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 190),
  [1606] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 434),
  [1608] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 83),
  [1610] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 436),
  [1612] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 437),
  [1614] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 438),
  [1616] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 192),
  [1618] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 440),
  [1620] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 441),
  [1622] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 442),
  [1624] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 444),
  [1626] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 445),
  [1628] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 193),
  [1630] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 447),
  [1632] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 448),
  [1634] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 449),
  [1636] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 194),
  [1638] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 451),
  [1640] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 452),
  [1642] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 453),
  [1644] = {.entry = {.count = 1, .reusable = true}}, SHIFT(459),
  [1646] = {.entry = {.count = 1, .reusable = true}}, SHIFT(343),
  [1648] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 456),
  [1650] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 197),
  [1652] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 458),
  [1654] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 459),
  [1656] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 460),
  [1658] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 4, 0, 43),
  [1660] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 462),
  [1662] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 463),
  [1664] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 464),
  [1666] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 465),
  [1668] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 466),
  [1670] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 199),
  [1672] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 468),
  [1674] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 469),
  [1676] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 470),
  [1678] = {.entry = {.count = 1, .reusable = true}}, SHIFT(845),
  [1680] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 9, 0, 471),
  [1682] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 9, 0, 472),
  [1684] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 9, 0, 473),
  [1686] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 200),
  [1688] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 9, 0, 475),
  [1690] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 9, 0, 476),
  [1692] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 9, 0, 477),
  [1694] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 87),
  [1696] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 9, 0, 479),
  [1698] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 9, 0, 480),
  [1700] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 9, 0, 481),
  [1702] = {.entry = {.count = 1, .reusable = true}}, SHIFT(46),
  [1704] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 203),
  [1706] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 9, 0, 484),
  [1708] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 4, 0, 45),
  [1710] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 9, 0, 486),
  [1712] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 9, 0, 487),
  [1714] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 9, 0, 488),
  [1716] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 205),
  [1718] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 9, 0, 490),
  [1720] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 9, 0, 491),
  [1722] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 9, 0, 492),
  [1724] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 9, 0, 493),
  [1726] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 9, 0, 494),
  [1728] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 9, 0, 496),
  [1730] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 9, 0, 497),
  [1732] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 9, 0, 498),
  [1734] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 9, 0, 499),
  [1736] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 9, 0, 500),
  [1738] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 9, 0, 501),
  [1740] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 91),
  [1742] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 9, 0, 503),
  [1744] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 9, 0, 504),
  [1746] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 9, 0, 505),
  [1748] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 9, 0, 506),
  [1750] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_geo_location_type, 6, 0, 1),
  [1752] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 10, 0, 507),
  [1754] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 10, 0, 508),
  [1756] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 10, 0, 509),
  [1758] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 10, 0, 511),
  [1760] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 10, 0, 512),
  [1762] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 10, 0, 513),
  [1764] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 10, 0, 514),
  [1766] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 10, 0, 515),
  [1768] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 11, 0, 516),
  [1770] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 209),
  [1772] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 93),
  [1774] = {.entry = {.count = 1, .reusable = true}}, SHIFT(887),
  [1776] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 4, 0, 49),
  [1778] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 213),
  [1780] = {.entry = {.count = 1, .reusable = true}}, SHIFT(467),
  [1782] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 215),
  [1784] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_header_comment, 2, 0, 0),
  [1786] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 98),
  [1788] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 218),
  [1790] = {.entry = {.count = 1, .reusable = true}}, SHIFT(359),
  [1792] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 220),
  [1794] = {.entry = {.count = 1, .reusable = true}}, SHIFT(164),
  [1796] = {.entry = {.count = 1, .reusable = true}}, SHIFT(571),
  [1798] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 102),
  [1800] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 224),
  [1802] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 225),
  [1804] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_geo_location_type, 1, 0, 0),
  [1806] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 227),
  [1808] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 228),
  [1810] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 229),
  [1812] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 104),
  [1814] = {.entry = {.count = 1, .reusable = true}}, SHIFT(613),
  [1816] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 232),
  [1818] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 234),
  [1820] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 235),
  [1822] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 108),
  [1824] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 238),
  [1826] = {.entry = {.count = 1, .reusable = true}}, SHIFT(240),
  [1828] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 240),
  [1830] = {.entry = {.count = 1, .reusable = true}}, SHIFT(257),
  [1832] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_in_type, 1, 0, 0),
  [1834] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 112),
  [1836] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 244),
  [1838] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 245),
  [1840] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 247),
  [1842] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 248),
  [1844] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 249),
  [1846] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 114),
  [1848] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_fields_type, 1, 0, 0),
  [1850] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 252),
  [1852] = {.entry = {.count = 1, .reusable = true}}, SHIFT(522),
  [1854] = {.entry = {.count = 1, .reusable = true}}, SHIFT(11),
  [1856] = {.entry = {.count = 1, .reusable = true}}, SHIFT(527),
  [1858] = {.entry = {.count = 1, .reusable = true}}, SHIFT(145),
  [1860] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 254),
  [1862] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 255),
  [1864] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 256),
  [1866] = {.entry = {.count = 1, .reusable = true}}, SHIFT(533),
  [1868] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 117),
  [1870] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 258),
  [1872] = {.entry = {.count = 1, .reusable = true}}, SHIFT(536),
  [1874] = {.entry = {.count = 1, .reusable = true}}, SHIFT(357),
  [1876] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 6, 0, 259),
  [1878] = {.entry = {.count = 1, .reusable = true}}, SHIFT(573),
  [1880] = {.entry = {.count = 1, .reusable = true}}, SHIFT(16),
  [1882] = {.entry = {.count = 1, .reusable = true}}, SHIFT(47),
  [1884] = {.entry = {.count = 1, .reusable = true}}, SHIFT(901),
  [1886] = {.entry = {.count = 1, .reusable = true}}, SHIFT(155),
  [1888] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 119),
  [1890] = {.entry = {.count = 1, .reusable = true}}, SHIFT(587),
  [1892] = {.entry = {.count = 1, .reusable = true}}, SHIFT(913),
  [1894] = {.entry = {.count = 1, .reusable = true}}, SHIFT(231),
  [1896] = {.entry = {.count = 1, .reusable = true}}, SHIFT(588),
  [1898] = {.entry = {.count = 1, .reusable = true}}, SHIFT(180),
  [1900] = {.entry = {.count = 1, .reusable = true}}, SHIFT(921),
  [1902] = {.entry = {.count = 1, .reusable = true}}, SHIFT(580),
  [1904] = {.entry = {.count = 1, .reusable = true}}, SHIFT(176),
  [1906] = {.entry = {.count = 1, .reusable = true}}, SHIFT(259),
  [1908] = {.entry = {.count = 1, .reusable = true}}, SHIFT(236),
  [1910] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 123),
  [1912] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 5, 0, 124),
  [1914] = {.entry = {.count = 1, .reusable = true}}, SHIFT(366),
  [1916] = {.entry = {.count = 1, .reusable = true}}, SHIFT(353),
  [1918] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_soql_query_body, 8, 0, 443),
  [1920] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_formatting_comment, 2, 0, 0),
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

TS_PUBLIC const TSLanguage *tree_sitter_sosl(void) {
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
