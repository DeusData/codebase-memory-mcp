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
#define STATE_COUNT 921
#define LARGE_STATE_COUNT 2
#define SYMBOL_COUNT 371
#define ALIAS_COUNT 21
#define TOKEN_COUNT 171
#define EXTERNAL_TOKEN_COUNT 0
#define FIELD_COUNT 0
#define MAX_ALIAS_SEQUENCE_LENGTH 10
#define PRODUCTION_ID_COUNT 29

enum ts_symbol_identifiers {
  anon_sym_PERCENT_PERCENT_LBRACE = 1,
  aux_sym_directive_token1 = 2,
  aux_sym_diagram_sequence_token1 = 3,
  aux_sym__sequence_participant_type_token1 = 4,
  aux_sym__sequence_participant_type_token2 = 5,
  aux_sym_sequence_stmt_participant_token1 = 6,
  aux_sym_sequence_stmt_actor_token1 = 7,
  anon_sym_COLON = 8,
  aux_sym_sequence_stmt_autonumber_token1 = 9,
  aux_sym_sequence_stmt_activate_token1 = 10,
  aux_sym_sequence_stmt_deactivate_token1 = 11,
  aux_sym_sequence_stmt_note_token1 = 12,
  aux_sym_sequence_stmt_note_token2 = 13,
  anon_sym_COMMA = 14,
  aux_sym_sequence_stmt_links_token1 = 15,
  aux_sym_sequence_stmt_link_token1 = 16,
  aux_sym_sequence_stmt_properties_token1 = 17,
  aux_sym_sequence_stmt_details_token1 = 18,
  aux_sym_sequence_stmt_title_token1 = 19,
  aux_sym_sequence_stmt_loop_token1 = 20,
  aux_sym_sequence_stmt_loop_token2 = 21,
  aux_sym_sequence_stmt_rect_token1 = 22,
  aux_sym_sequence_stmt_opt_token1 = 23,
  aux_sym_sequence_stmt_alt_token1 = 24,
  aux_sym_sequence_stmt_alt_token2 = 25,
  aux_sym_sequence_stmt_par_token1 = 26,
  aux_sym_sequence_stmt_par_token2 = 27,
  aux_sym_diagram_class_token1 = 28,
  aux_sym_diagram_class_token2 = 29,
  anon_sym_TILDE = 30,
  aux_sym_class_generics_token1 = 31,
  anon_sym_class = 32,
  anon_sym_COLON_COLON_COLON = 33,
  anon_sym_LBRACE = 34,
  anon_sym_RBRACE = 35,
  anon_sym_PLUS = 36,
  anon_sym_DASH = 37,
  anon_sym_POUND = 38,
  anon_sym_LPAREN = 39,
  anon_sym_RPAREN = 40,
  anon_sym_STAR = 41,
  anon_sym_DOLLAR = 42,
  anon_sym_LT_LT = 43,
  anon_sym_GT_GT = 44,
  aux_sym_diagram_state_token1 = 45,
  aux_sym_diagram_state_token2 = 46,
  aux_sym_state_stmt_simple_token1 = 47,
  aux_sym_diagram_gantt_token1 = 48,
  aux_sym_gantt_stmt_dateformat_token1 = 49,
  aux_sym_gantt_stmt_inclusiveenddates_token1 = 50,
  aux_sym_gantt_stmt_topaxis_token1 = 51,
  aux_sym_gantt_stmt_axisformat_token1 = 52,
  aux_sym_gantt_stmt_includes_token1 = 53,
  aux_sym_gantt_stmt_excludes_token1 = 54,
  aux_sym_gantt_stmt_todaymarker_token1 = 55,
  aux_sym_gantt_stmt_section_token1 = 56,
  aux_sym_diagram_pie_token1 = 57,
  aux_sym_diagram_flow_token1 = 58,
  anon_sym_SEMI = 59,
  aux_sym_flow_stmt_direction_token1 = 60,
  anon_sym_AMP = 61,
  anon_sym_PIPE = 62,
  anon_sym_LBRACK = 63,
  anon_sym_RBRACK = 64,
  anon_sym_LPAREN_LPAREN = 65,
  anon_sym_RPAREN_RPAREN = 66,
  anon_sym_LPAREN_DASH = 67,
  anon_sym_DASH_RPAREN = 68,
  anon_sym_LPAREN_LBRACK = 69,
  anon_sym_RBRACK_RPAREN = 70,
  anon_sym_LBRACK_LBRACK = 71,
  anon_sym_RBRACK_RBRACK = 72,
  anon_sym_LBRACK_PIPE = 73,
  anon_sym_PIPE_RBRACK = 74,
  anon_sym_LBRACK_LPAREN = 75,
  anon_sym_RPAREN_RBRACK = 76,
  anon_sym_LBRACE_LBRACE = 77,
  anon_sym_RBRACE_RBRACE = 78,
  anon_sym_GT = 79,
  anon_sym_LBRACK_SLASH = 80,
  anon_sym_BSLASH_RBRACK = 81,
  anon_sym_LBRACK_BSLASH = 82,
  anon_sym_SLASH_RBRACK = 83,
  anon_sym_subgraph = 84,
  anon_sym_end = 85,
  aux_sym_diagram_er_token1 = 86,
  aux_sym_diagram_mindmap_token1 = 87,
  aux_sym_mmap_markdown_token1 = 88,
  aux_sym_mmap_text_token1 = 89,
  aux_sym_mmap_class_token1 = 90,
  aux_sym_mmap_class_token2 = 91,
  aux_sym_mmap_icon_token1 = 92,
  aux_sym_mmap_icon_token2 = 93,
  sym__whitespace = 94,
  sym__newline = 95,
  sym_comment = 96,
  sym__md_start = 97,
  sym__md_end = 98,
  sym_type_directive = 99,
  aux_sym_direction_tb_token1 = 100,
  aux_sym_direction_bt_token1 = 101,
  aux_sym_direction_rl_token1 = 102,
  aux_sym_direction_lr_token1 = 103,
  aux_sym__sequence_rest_text_token1 = 104,
  sym__sequence_actor_word = 105,
  sym_solid_arrow = 106,
  sym_dotted_arrow = 107,
  sym_solid_open_arrow = 108,
  anon_sym_DASH_DASH_GT = 109,
  sym_solid_cross = 110,
  sym_dotted_cross = 111,
  sym_dotted_point = 112,
  aux_sym_note_placement_left_token1 = 113,
  aux_sym_note_placement_right_token1 = 114,
  sym__class_name = 115,
  sym__alpha_num_token = 116,
  sym__bquote_string = 117,
  sym__dquote_string = 118,
  sym_class_reltype_aggregation = 119,
  anon_sym_LT_PIPE = 120,
  anon_sym_PIPE_GT = 121,
  anon_sym_LT = 122,
  anon_sym_DASH_DASH = 123,
  anon_sym_DOT_DOT = 124,
  aux_sym_class_label_token1 = 125,
  anon_sym_LBRACK_STAR_RBRACK = 126,
  aux_sym_state_name_token1 = 127,
  aux_sym_state_hide_empty_description_token1 = 128,
  sym_state_id = 129,
  aux_sym_state_annotation_fork_token1 = 130,
  aux_sym_state_annotation_fork_token2 = 131,
  aux_sym_state_annotation_join_token1 = 132,
  aux_sym_state_annotation_join_token2 = 133,
  aux_sym_state_annotation_choice_token1 = 134,
  aux_sym_state_annotation_choice_token2 = 135,
  aux_sym_gantt_task_text_token1 = 136,
  sym_gantt_task_data = 137,
  aux_sym_pie_showdata_token1 = 138,
  sym_pie_title = 139,
  aux_sym_pie_label_token1 = 140,
  sym_pie_value = 141,
  aux_sym_flowchart_direction_lr_token1 = 142,
  aux_sym_flowchart_direction_lr_token2 = 143,
  aux_sym_flowchart_direction_rl_token1 = 144,
  aux_sym_flowchart_direction_tb_token1 = 145,
  aux_sym_flowchart_direction_tb_token2 = 146,
  anon_sym_v = 147,
  aux_sym_flowchart_direction_bt_token1 = 148,
  anon_sym_CARET = 149,
  aux_sym_flow_text_literal_token1 = 150,
  sym_flow_text_quoted = 151,
  aux_sym_flow_link_arrow_token1 = 152,
  aux_sym_flow_link_arrow_token2 = 153,
  aux_sym_flow_link_arrow_token3 = 154,
  aux_sym_flow_link_arrow_start_token1 = 155,
  aux_sym_flow_link_arrow_start_token2 = 156,
  aux_sym_flow_link_arrow_start_token3 = 157,
  sym__er_alphanum = 158,
  anon_sym_PIPEo = 159,
  anon_sym_o_PIPE = 160,
  anon_sym_RBRACEo = 161,
  anon_sym_o_LBRACE = 162,
  anon_sym_RBRACE_PIPE = 163,
  anon_sym_PIPE_LBRACE = 164,
  sym_er_cardinarity_only_one = 165,
  anon_sym_DOT_DASH = 166,
  anon_sym_DASH_DOT = 167,
  aux_sym_er_attribute_key_type_pk_token1 = 168,
  aux_sym_er_attribute_key_type_fk_token1 = 169,
  sym__mindmap_text = 170,
  sym_source_file = 171,
  sym_directive = 172,
  sym__direction = 173,
  sym_diagram_sequence = 174,
  sym__sequence_stmt = 175,
  sym__sequence_participant_type = 176,
  sym_sequence_stmt_participant = 177,
  sym_sequence_stmt_actor = 178,
  sym_sequence_actor = 179,
  sym_sequence_stmt_signal = 180,
  sym_sequence_signal_type = 181,
  sym_sequence_text = 182,
  sym_sequence_stmt_autonumber = 183,
  sym_sequence_stmt_activate = 184,
  sym_sequence_stmt_deactivate = 185,
  sym_sequence_stmt_note = 186,
  sym_sequence_stmt_links = 187,
  sym_sequence_stmt_link = 188,
  sym_sequence_stmt_properties = 189,
  sym_sequence_stmt_details = 190,
  sym_sequence_note_placement = 191,
  sym_sequence_stmt_title = 192,
  sym_sequence_stmt_loop = 193,
  sym_sequence_stmt_rect = 194,
  sym_sequence_stmt_opt = 195,
  aux_sym__sequence_subdocument = 196,
  sym_sequence_stmt_alt = 197,
  sym_sequence_stmt_par = 198,
  sym_diagram_class = 199,
  sym__class_stmt = 200,
  sym_class_stmt_relation = 201,
  sym_class_name = 202,
  sym_class_name_body = 203,
  sym_class_generics = 204,
  sym_class_relation = 205,
  sym__class_reltype = 206,
  sym__class_linetype = 207,
  sym_class_stmt_class = 208,
  sym_class_method_line = 209,
  sym_class_annotation_line = 210,
  sym_class_stmt_method = 211,
  sym_class_stmt_annotation = 212,
  sym_diagram_state = 213,
  sym__state_stmt = 214,
  sym_state_stmt_simple = 215,
  sym_state_stmt_arrow = 216,
  sym_state_stmt_composite = 217,
  sym_state_composite_body = 218,
  sym_state_stmt_annotation = 219,
  sym_state_alias = 220,
  sym_state_stmt_hide_empty_description = 221,
  sym__state_annotation = 222,
  sym_state_note = 223,
  sym_state_note_placement = 224,
  sym_diagram_gantt = 225,
  sym__gantt_stmt = 226,
  sym_gantt_stmt_dateformat = 227,
  sym_gantt_stmt_inclusiveenddates = 228,
  sym_gantt_stmt_topaxis = 229,
  sym_gantt_stmt_axisformat = 230,
  sym_gantt_stmt_includes = 231,
  sym_gantt_stmt_excludes = 232,
  sym_gantt_stmt_todaymarker = 233,
  sym_gantt_stmt_title = 234,
  sym_gantt_stmt_section = 235,
  sym_gantt_stmt_task = 236,
  sym_diagram_pie = 237,
  sym__pie_stmt = 238,
  sym_pie_stmt_title = 239,
  sym_pie_stmt_element = 240,
  sym_diagram_flow = 241,
  sym__flowchart_direction = 242,
  sym__flow_stmt = 243,
  sym_flow_stmt_direction = 244,
  sym_flow_stmt_vertice = 245,
  sym_flow_node = 246,
  sym__flow_link = 247,
  sym_flow_link_simplelink = 248,
  sym_flow_link_arrowtext = 249,
  sym_flow_link_middletext = 250,
  sym_flow_arrow_text = 251,
  sym_flow_vertex_id = 252,
  sym_flow_vertex = 253,
  sym__flow_vertex_kind = 254,
  sym_flow_vertex_square = 255,
  sym_flow_vertex_circle = 256,
  sym_flow_vertex_ellipse = 257,
  sym_flow_vertex_stadium = 258,
  sym_flow_vertex_subroutine = 259,
  sym_flow_vertex_rect = 260,
  sym_flow_vertex_cylinder = 261,
  sym_flow_vertex_round = 262,
  sym_flow_vertex_diamond = 263,
  sym_flow_vertex_hexagon = 264,
  sym_flow_vertex_odd = 265,
  sym_flow_vertex_trapezoid = 266,
  sym_flow_vertex_inv_trapezoid = 267,
  sym_flow_vertex_leanright = 268,
  sym_flow_vertex_leanleft = 269,
  sym__flow_text = 270,
  sym_flow_stmt_subgraph = 271,
  sym_flow_stmt_subgraph_inner = 272,
  sym_flow_vertex_text = 273,
  sym_diagram_er = 274,
  sym__er_stmt = 275,
  sym_er_stmt_entity = 276,
  sym_er_stmt_entity_relation = 277,
  sym_er_entity_name = 278,
  sym_er_relation = 279,
  sym__er_cardinarity = 280,
  sym__er_reltype = 281,
  sym_er_role = 282,
  sym_er_stmt_entity_block = 283,
  sym_er_stmt_entity_block_inner = 284,
  sym_er_attribute = 285,
  sym_er_attribute_type = 286,
  sym_er_attribute_name = 287,
  sym__er_attribute_key_type = 288,
  sym_er_attribute_comment = 289,
  sym_diagram_mindmap = 290,
  sym_mmap_node = 291,
  sym_mmap_node_id = 292,
  sym_mmap_markdown = 293,
  sym_mmap_text = 294,
  sym_mmap_node_content = 295,
  sym_mmap_node_square = 296,
  sym_mmap_node_rounded = 297,
  sym_mmap_node_circle = 298,
  sym_mmap_node_cloud = 299,
  sym_mmap_node_bang = 300,
  sym_mmap_node_hexagon = 301,
  sym_mmap_class = 302,
  sym_mmap_icon = 303,
  sym_direction_tb = 304,
  sym_direction_bt = 305,
  sym_direction_rl = 306,
  sym_direction_lr = 307,
  sym__sequence_rest_text = 308,
  sym_dotted_open_arrow = 309,
  sym_solid_point = 310,
  sym_sequence_signal_plus_sign = 311,
  sym_sequence_signal_minus_sign = 312,
  sym_note_placement_left = 313,
  sym_note_placement_right = 314,
  sym_class_reltype_extension = 315,
  sym_class_reltype_composition = 316,
  sym_class_reltype_dependency = 317,
  sym_class_linetype_solid = 318,
  sym_class_linetype_dotted = 319,
  sym_class_label = 320,
  sym_state_name = 321,
  sym_state_arrow = 322,
  sym_state_description = 323,
  sym_state_hide_empty_description = 324,
  sym_state_division = 325,
  sym_state_annotation_fork = 326,
  sym_state_annotation_join = 327,
  sym_state_annotation_choice = 328,
  sym_gantt_meta_format = 329,
  sym_gantt_task_text = 330,
  sym_pie_showdata = 331,
  sym_pie_label = 332,
  sym_flowchart_direction_lr = 333,
  sym_flowchart_direction_rl = 334,
  sym_flowchart_direction_tb = 335,
  sym_flowchart_direction_bt = 336,
  sym_flow_text_literal = 337,
  sym_flow_link_arrow = 338,
  sym_flow_link_arrow_start = 339,
  sym__er_word = 340,
  sym_er_cardinarity_zero_or_one = 341,
  sym_er_cardinarity_zero_or_more = 342,
  sym_er_cardinarity_one_or_more = 343,
  sym_er_reltype_non_identifying = 344,
  sym_er_reltype_identifying = 345,
  sym_er_attribute_key_type_pk = 346,
  sym_er_attribute_key_type_fk = 347,
  aux_sym_source_file_repeat1 = 348,
  aux_sym_diagram_sequence_repeat1 = 349,
  aux_sym_sequence_actor_repeat1 = 350,
  aux_sym_sequence_stmt_alt_repeat1 = 351,
  aux_sym_sequence_stmt_par_repeat1 = 352,
  aux_sym_diagram_class_repeat1 = 353,
  aux_sym_class_name_body_repeat1 = 354,
  aux_sym_class_stmt_class_repeat1 = 355,
  aux_sym_class_method_line_repeat1 = 356,
  aux_sym_diagram_state_repeat1 = 357,
  aux_sym_state_composite_body_repeat1 = 358,
  aux_sym_diagram_gantt_repeat1 = 359,
  aux_sym_diagram_pie_repeat1 = 360,
  aux_sym_diagram_flow_repeat1 = 361,
  aux_sym_flow_stmt_vertice_repeat1 = 362,
  aux_sym_flow_node_repeat1 = 363,
  aux_sym_flow_arrow_text_repeat1 = 364,
  aux_sym_flow_stmt_subgraph_inner_repeat1 = 365,
  aux_sym_diagram_er_repeat1 = 366,
  aux_sym_er_stmt_entity_block_inner_repeat1 = 367,
  aux_sym_diagram_mindmap_repeat1 = 368,
  aux_sym_gantt_task_text_repeat1 = 369,
  aux_sym_flow_text_literal_repeat1 = 370,
  alias_sym_annotation = 371,
  alias_sym_class_classifier_abstract = 372,
  alias_sym_class_style_name = 373,
  alias_sym_class_visibility_internal = 374,
  alias_sym_class_visibility_private = 375,
  alias_sym_class_visibility_public = 376,
  alias_sym_gantt_axis_format = 377,
  alias_sym_gantt_end_dates = 378,
  alias_sym_gantt_excludes = 379,
  alias_sym_gantt_includes = 380,
  alias_sym_gantt_section = 381,
  alias_sym_gantt_title = 382,
  alias_sym_gantt_today_marker = 383,
  alias_sym_gantt_top_axis = 384,
  alias_sym_note_content = 385,
  alias_sym_sequence_alias = 386,
  alias_sym_sequence_stmt_alt_branch = 387,
  alias_sym_sequence_stmt_loop_inner = 388,
  alias_sym_sequence_stmt_opt_inner = 389,
  alias_sym_sequence_stmt_rect_inner = 390,
  alias_sym_title = 391,
};

static const char * const ts_symbol_names[] = {
  [ts_builtin_sym_end] = "end",
  [anon_sym_PERCENT_PERCENT_LBRACE] = "%%{",
  [aux_sym_directive_token1] = "arg_directive",
  [aux_sym_diagram_sequence_token1] = "sequenceDiagram",
  [aux_sym__sequence_participant_type_token1] = "participant",
  [aux_sym__sequence_participant_type_token2] = "actor",
  [aux_sym_sequence_stmt_participant_token1] = "as",
  [aux_sym_sequence_stmt_actor_token1] = "sequence_actor",
  [anon_sym_COLON] = ":",
  [aux_sym_sequence_stmt_autonumber_token1] = "autonumber",
  [aux_sym_sequence_stmt_activate_token1] = "activate",
  [aux_sym_sequence_stmt_deactivate_token1] = "deactivate",
  [aux_sym_sequence_stmt_note_token1] = "note ",
  [aux_sym_sequence_stmt_note_token2] = "over",
  [anon_sym_COMMA] = ",",
  [aux_sym_sequence_stmt_links_token1] = "links",
  [aux_sym_sequence_stmt_link_token1] = "link",
  [aux_sym_sequence_stmt_properties_token1] = "properties",
  [aux_sym_sequence_stmt_details_token1] = "details",
  [aux_sym_sequence_stmt_title_token1] = "title",
  [aux_sym_sequence_stmt_loop_token1] = "loop",
  [aux_sym_sequence_stmt_loop_token2] = "end",
  [aux_sym_sequence_stmt_rect_token1] = "rect",
  [aux_sym_sequence_stmt_opt_token1] = "opt",
  [aux_sym_sequence_stmt_alt_token1] = "alt",
  [aux_sym_sequence_stmt_alt_token2] = "else",
  [aux_sym_sequence_stmt_par_token1] = "par",
  [aux_sym_sequence_stmt_par_token2] = "and",
  [aux_sym_diagram_class_token1] = "classDiagram-v2",
  [aux_sym_diagram_class_token2] = "classDiagram",
  [anon_sym_TILDE] = "~",
  [aux_sym_class_generics_token1] = "class_name",
  [anon_sym_class] = "class",
  [anon_sym_COLON_COLON_COLON] = ":::",
  [anon_sym_LBRACE] = "{",
  [anon_sym_RBRACE] = "}",
  [anon_sym_PLUS] = "+",
  [anon_sym_DASH] = "-",
  [anon_sym_POUND] = "class_visibility_protected",
  [anon_sym_LPAREN] = "(",
  [anon_sym_RPAREN] = ")",
  [anon_sym_STAR] = "*",
  [anon_sym_DOLLAR] = "class_classifier_static",
  [anon_sym_LT_LT] = "<<",
  [anon_sym_GT_GT] = ">>",
  [aux_sym_diagram_state_token1] = "stateDiagram",
  [aux_sym_diagram_state_token2] = "stateDiagram-v2",
  [aux_sym_state_stmt_simple_token1] = "state ",
  [aux_sym_diagram_gantt_token1] = "gantt",
  [aux_sym_gantt_stmt_dateformat_token1] = "dateformat",
  [aux_sym_gantt_stmt_inclusiveenddates_token1] = "inclusiveenddates",
  [aux_sym_gantt_stmt_topaxis_token1] = "topaxis",
  [aux_sym_gantt_stmt_axisformat_token1] = "axisformat",
  [aux_sym_gantt_stmt_includes_token1] = "includes",
  [aux_sym_gantt_stmt_excludes_token1] = "excludes",
  [aux_sym_gantt_stmt_todaymarker_token1] = "todaymarker",
  [aux_sym_gantt_stmt_section_token1] = "section",
  [aux_sym_diagram_pie_token1] = "pie",
  [aux_sym_diagram_flow_token1] = "flowchart",
  [anon_sym_SEMI] = ";",
  [aux_sym_flow_stmt_direction_token1] = "direction",
  [anon_sym_AMP] = "&",
  [anon_sym_PIPE] = "|",
  [anon_sym_LBRACK] = "[",
  [anon_sym_RBRACK] = "]",
  [anon_sym_LPAREN_LPAREN] = "((",
  [anon_sym_RPAREN_RPAREN] = "))",
  [anon_sym_LPAREN_DASH] = "(-",
  [anon_sym_DASH_RPAREN] = "-)",
  [anon_sym_LPAREN_LBRACK] = "([",
  [anon_sym_RBRACK_RPAREN] = "])",
  [anon_sym_LBRACK_LBRACK] = "[[",
  [anon_sym_RBRACK_RBRACK] = "]]",
  [anon_sym_LBRACK_PIPE] = "[|",
  [anon_sym_PIPE_RBRACK] = "|]",
  [anon_sym_LBRACK_LPAREN] = "[(",
  [anon_sym_RPAREN_RBRACK] = ")]",
  [anon_sym_LBRACE_LBRACE] = "{{",
  [anon_sym_RBRACE_RBRACE] = "}}",
  [anon_sym_GT] = ">",
  [anon_sym_LBRACK_SLASH] = "[/",
  [anon_sym_BSLASH_RBRACK] = "\\]",
  [anon_sym_LBRACK_BSLASH] = "[\\",
  [anon_sym_SLASH_RBRACK] = "/]",
  [anon_sym_subgraph] = "subgraph",
  [anon_sym_end] = "end",
  [aux_sym_diagram_er_token1] = "erdiagram",
  [aux_sym_diagram_mindmap_token1] = "diagram_mindmap_token1",
  [aux_sym_mmap_markdown_token1] = "md_text",
  [aux_sym_mmap_text_token1] = "mmap_text_token1",
  [aux_sym_mmap_class_token1] = "mmap_class_token1",
  [aux_sym_mmap_class_token2] = "mmap_class_token2",
  [aux_sym_mmap_icon_token1] = "mmap_icon_token1",
  [aux_sym_mmap_icon_token2] = "mmap_icon_token2",
  [sym__whitespace] = "_whitespace",
  [sym__newline] = "_newline",
  [sym_comment] = "comment",
  [sym__md_start] = "_md_start",
  [sym__md_end] = "_md_end",
  [sym_type_directive] = "type_directive",
  [aux_sym_direction_tb_token1] = "direction tb",
  [aux_sym_direction_bt_token1] = "direction bt",
  [aux_sym_direction_rl_token1] = "direction rl",
  [aux_sym_direction_lr_token1] = "direction lr",
  [aux_sym__sequence_rest_text_token1] = "_sequence_rest_text_token1",
  [sym__sequence_actor_word] = "_sequence_actor_word",
  [sym_solid_arrow] = "solid_arrow",
  [sym_dotted_arrow] = "dotted_arrow",
  [sym_solid_open_arrow] = "solid_open_arrow",
  [anon_sym_DASH_DASH_GT] = "-->",
  [sym_solid_cross] = "solid_cross",
  [sym_dotted_cross] = "dotted_cross",
  [sym_dotted_point] = "dotted_point",
  [aux_sym_note_placement_left_token1] = "left of",
  [aux_sym_note_placement_right_token1] = "right of",
  [sym__class_name] = "_class_name",
  [sym__alpha_num_token] = "_alpha_num_token",
  [sym__bquote_string] = "_bquote_string",
  [sym__dquote_string] = "cardinality",
  [sym_class_reltype_aggregation] = "class_reltype_aggregation",
  [anon_sym_LT_PIPE] = "<|",
  [anon_sym_PIPE_GT] = "|>",
  [anon_sym_LT] = "<",
  [anon_sym_DASH_DASH] = "--",
  [anon_sym_DOT_DOT] = "..",
  [aux_sym_class_label_token1] = "class_label_token1",
  [anon_sym_LBRACK_STAR_RBRACK] = "[*]",
  [aux_sym_state_name_token1] = "state_name_token1",
  [aux_sym_state_hide_empty_description_token1] = "hide empty description",
  [sym_state_id] = "state_id",
  [aux_sym_state_annotation_fork_token1] = "state_annotation_fork_token1",
  [aux_sym_state_annotation_fork_token2] = "state_annotation_fork_token2",
  [aux_sym_state_annotation_join_token1] = "state_annotation_join_token1",
  [aux_sym_state_annotation_join_token2] = "state_annotation_join_token2",
  [aux_sym_state_annotation_choice_token1] = "state_annotation_choice_token1",
  [aux_sym_state_annotation_choice_token2] = "state_annotation_choice_token2",
  [aux_sym_gantt_task_text_token1] = "gantt_task_text_token1",
  [sym_gantt_task_data] = "gantt_task_data",
  [aux_sym_pie_showdata_token1] = "showdata",
  [sym_pie_title] = "pie_title",
  [aux_sym_pie_label_token1] = "pie_label_token1",
  [sym_pie_value] = "pie_value",
  [aux_sym_flowchart_direction_lr_token1] = "lr",
  [aux_sym_flowchart_direction_lr_token2] = "br",
  [aux_sym_flowchart_direction_rl_token1] = "rl",
  [aux_sym_flowchart_direction_tb_token1] = "tb",
  [aux_sym_flowchart_direction_tb_token2] = "td",
  [anon_sym_v] = "v",
  [aux_sym_flowchart_direction_bt_token1] = "bt",
  [anon_sym_CARET] = "^",
  [aux_sym_flow_text_literal_token1] = "flow_text_literal_token1",
  [sym_flow_text_quoted] = "flow_text_quoted",
  [aux_sym_flow_link_arrow_token1] = "flow_link_arrow_token1",
  [aux_sym_flow_link_arrow_token2] = "flow_link_arrow_token2",
  [aux_sym_flow_link_arrow_token3] = "flow_link_arrow_token3",
  [aux_sym_flow_link_arrow_start_token1] = "flow_link_arrow_start_token1",
  [aux_sym_flow_link_arrow_start_token2] = "flow_link_arrow_start_token2",
  [aux_sym_flow_link_arrow_start_token3] = "flow_link_arrow_start_token3",
  [sym__er_alphanum] = "_er_alphanum",
  [anon_sym_PIPEo] = "|o",
  [anon_sym_o_PIPE] = "o|",
  [anon_sym_RBRACEo] = "}o",
  [anon_sym_o_LBRACE] = "o{",
  [anon_sym_RBRACE_PIPE] = "}|",
  [anon_sym_PIPE_LBRACE] = "|{",
  [sym_er_cardinarity_only_one] = "er_cardinarity_only_one",
  [anon_sym_DOT_DASH] = ".-",
  [anon_sym_DASH_DOT] = "-.",
  [aux_sym_er_attribute_key_type_pk_token1] = "pk",
  [aux_sym_er_attribute_key_type_fk_token1] = "fk",
  [sym__mindmap_text] = "_mindmap_text",
  [sym_source_file] = "source_file",
  [sym_directive] = "directive",
  [sym__direction] = "_direction",
  [sym_diagram_sequence] = "diagram_sequence",
  [sym__sequence_stmt] = "_sequence_stmt",
  [sym__sequence_participant_type] = "_sequence_participant_type",
  [sym_sequence_stmt_participant] = "sequence_stmt_participant",
  [sym_sequence_stmt_actor] = "sequence_stmt_actor",
  [sym_sequence_actor] = "sequence_actor",
  [sym_sequence_stmt_signal] = "sequence_stmt_signal",
  [sym_sequence_signal_type] = "sequence_signal_type",
  [sym_sequence_text] = "sequence_text",
  [sym_sequence_stmt_autonumber] = "sequence_stmt_autonumber",
  [sym_sequence_stmt_activate] = "sequence_stmt_activate",
  [sym_sequence_stmt_deactivate] = "sequence_stmt_deactivate",
  [sym_sequence_stmt_note] = "sequence_stmt_note",
  [sym_sequence_stmt_links] = "sequence_stmt_links",
  [sym_sequence_stmt_link] = "sequence_stmt_link",
  [sym_sequence_stmt_properties] = "sequence_stmt_properties",
  [sym_sequence_stmt_details] = "sequence_stmt_details",
  [sym_sequence_note_placement] = "sequence_note_placement",
  [sym_sequence_stmt_title] = "sequence_stmt_title",
  [sym_sequence_stmt_loop] = "sequence_stmt_loop",
  [sym_sequence_stmt_rect] = "sequence_stmt_rect",
  [sym_sequence_stmt_opt] = "sequence_stmt_opt",
  [aux_sym__sequence_subdocument] = "_sequence_subdocument",
  [sym_sequence_stmt_alt] = "sequence_stmt_alt",
  [sym_sequence_stmt_par] = "sequence_stmt_par",
  [sym_diagram_class] = "diagram_class",
  [sym__class_stmt] = "_class_stmt",
  [sym_class_stmt_relation] = "class_stmt_relation",
  [sym_class_name] = "class_name",
  [sym_class_name_body] = "class_name_body",
  [sym_class_generics] = "class_generics",
  [sym_class_relation] = "class_relation",
  [sym__class_reltype] = "_class_reltype",
  [sym__class_linetype] = "_class_linetype",
  [sym_class_stmt_class] = "class_stmt_class",
  [sym_class_method_line] = "class_method_line",
  [sym_class_annotation_line] = "class_annotation_line",
  [sym_class_stmt_method] = "class_stmt_method",
  [sym_class_stmt_annotation] = "class_stmt_annotation",
  [sym_diagram_state] = "diagram_state",
  [sym__state_stmt] = "_state_stmt",
  [sym_state_stmt_simple] = "state_stmt_simple",
  [sym_state_stmt_arrow] = "state_stmt_arrow",
  [sym_state_stmt_composite] = "state_stmt_composite",
  [sym_state_composite_body] = "state_composite_body",
  [sym_state_stmt_annotation] = "state_stmt_annotation",
  [sym_state_alias] = "state_alias",
  [sym_state_stmt_hide_empty_description] = "state_stmt_hide_empty_description",
  [sym__state_annotation] = "_state_annotation",
  [sym_state_note] = "state_note",
  [sym_state_note_placement] = "state_note_placement",
  [sym_diagram_gantt] = "diagram_gantt",
  [sym__gantt_stmt] = "_gantt_stmt",
  [sym_gantt_stmt_dateformat] = "gantt_stmt_dateformat",
  [sym_gantt_stmt_inclusiveenddates] = "gantt_stmt_inclusiveenddates",
  [sym_gantt_stmt_topaxis] = "gantt_stmt_topaxis",
  [sym_gantt_stmt_axisformat] = "gantt_stmt_axisformat",
  [sym_gantt_stmt_includes] = "gantt_stmt_includes",
  [sym_gantt_stmt_excludes] = "gantt_stmt_excludes",
  [sym_gantt_stmt_todaymarker] = "gantt_stmt_todaymarker",
  [sym_gantt_stmt_title] = "gantt_stmt_title",
  [sym_gantt_stmt_section] = "gantt_stmt_section",
  [sym_gantt_stmt_task] = "gantt_stmt_task",
  [sym_diagram_pie] = "diagram_pie",
  [sym__pie_stmt] = "_pie_stmt",
  [sym_pie_stmt_title] = "pie_stmt_title",
  [sym_pie_stmt_element] = "pie_stmt_element",
  [sym_diagram_flow] = "diagram_flow",
  [sym__flowchart_direction] = "_flowchart_direction",
  [sym__flow_stmt] = "_flow_stmt",
  [sym_flow_stmt_direction] = "flow_stmt_direction",
  [sym_flow_stmt_vertice] = "flow_stmt_vertice",
  [sym_flow_node] = "flow_node",
  [sym__flow_link] = "_flow_link",
  [sym_flow_link_simplelink] = "flow_link_simplelink",
  [sym_flow_link_arrowtext] = "flow_link_arrowtext",
  [sym_flow_link_middletext] = "flow_link_middletext",
  [sym_flow_arrow_text] = "flow_arrow_text",
  [sym_flow_vertex_id] = "flow_vertex_id",
  [sym_flow_vertex] = "flow_vertex",
  [sym__flow_vertex_kind] = "_flow_vertex_kind",
  [sym_flow_vertex_square] = "flow_vertex_square",
  [sym_flow_vertex_circle] = "flow_vertex_circle",
  [sym_flow_vertex_ellipse] = "flow_vertex_ellipse",
  [sym_flow_vertex_stadium] = "flow_vertex_stadium",
  [sym_flow_vertex_subroutine] = "flow_vertex_subroutine",
  [sym_flow_vertex_rect] = "flow_vertex_rect",
  [sym_flow_vertex_cylinder] = "flow_vertex_cylinder",
  [sym_flow_vertex_round] = "flow_vertex_round",
  [sym_flow_vertex_diamond] = "flow_vertex_diamond",
  [sym_flow_vertex_hexagon] = "flow_vertex_hexagon",
  [sym_flow_vertex_odd] = "flow_vertex_odd",
  [sym_flow_vertex_trapezoid] = "flow_vertex_trapezoid",
  [sym_flow_vertex_inv_trapezoid] = "flow_vertex_inv_trapezoid",
  [sym_flow_vertex_leanright] = "flow_vertex_leanright",
  [sym_flow_vertex_leanleft] = "flow_vertex_leanleft",
  [sym__flow_text] = "_flow_text",
  [sym_flow_stmt_subgraph] = "flow_stmt_subgraph",
  [sym_flow_stmt_subgraph_inner] = "flow_stmt_subgraph_inner",
  [sym_flow_vertex_text] = "flow_vertex_text",
  [sym_diagram_er] = "diagram_er",
  [sym__er_stmt] = "_er_stmt",
  [sym_er_stmt_entity] = "er_stmt_entity",
  [sym_er_stmt_entity_relation] = "er_stmt_entity_relation",
  [sym_er_entity_name] = "er_entity_name",
  [sym_er_relation] = "er_relation",
  [sym__er_cardinarity] = "_er_cardinarity",
  [sym__er_reltype] = "_er_reltype",
  [sym_er_role] = "er_role",
  [sym_er_stmt_entity_block] = "er_stmt_entity_block",
  [sym_er_stmt_entity_block_inner] = "er_stmt_entity_block_inner",
  [sym_er_attribute] = "er_attribute",
  [sym_er_attribute_type] = "er_attribute_type",
  [sym_er_attribute_name] = "er_attribute_name",
  [sym__er_attribute_key_type] = "_er_attribute_key_type",
  [sym_er_attribute_comment] = "er_attribute_comment",
  [sym_diagram_mindmap] = "diagram_mindmap",
  [sym_mmap_node] = "mmap_node",
  [sym_mmap_node_id] = "mmap_node_id",
  [sym_mmap_markdown] = "mmap_markdown",
  [sym_mmap_text] = "mmap_text",
  [sym_mmap_node_content] = "mmap_node_content",
  [sym_mmap_node_square] = "mmap_node_square",
  [sym_mmap_node_rounded] = "mmap_node_rounded",
  [sym_mmap_node_circle] = "mmap_node_circle",
  [sym_mmap_node_cloud] = "mmap_node_cloud",
  [sym_mmap_node_bang] = "mmap_node_bang",
  [sym_mmap_node_hexagon] = "mmap_node_hexagon",
  [sym_mmap_class] = "mmap_class",
  [sym_mmap_icon] = "mmap_icon",
  [sym_direction_tb] = "direction_tb",
  [sym_direction_bt] = "direction_bt",
  [sym_direction_rl] = "direction_rl",
  [sym_direction_lr] = "direction_lr",
  [sym__sequence_rest_text] = "_sequence_rest_text",
  [sym_dotted_open_arrow] = "dotted_open_arrow",
  [sym_solid_point] = "solid_point",
  [sym_sequence_signal_plus_sign] = "sequence_signal_plus_sign",
  [sym_sequence_signal_minus_sign] = "sequence_signal_minus_sign",
  [sym_note_placement_left] = "note_placement_left",
  [sym_note_placement_right] = "note_placement_right",
  [sym_class_reltype_extension] = "class_reltype_extension",
  [sym_class_reltype_composition] = "class_reltype_composition",
  [sym_class_reltype_dependency] = "class_reltype_dependency",
  [sym_class_linetype_solid] = "class_linetype_solid",
  [sym_class_linetype_dotted] = "class_linetype_dotted",
  [sym_class_label] = "class_label",
  [sym_state_name] = "state_name",
  [sym_state_arrow] = "state_arrow",
  [sym_state_description] = "state_description",
  [sym_state_hide_empty_description] = "state_hide_empty_description",
  [sym_state_division] = "state_division",
  [sym_state_annotation_fork] = "state_annotation_fork",
  [sym_state_annotation_join] = "state_annotation_join",
  [sym_state_annotation_choice] = "state_annotation_choice",
  [sym_gantt_meta_format] = "gantt_date_format",
  [sym_gantt_task_text] = "gantt_task_text",
  [sym_pie_showdata] = "pie_showdata",
  [sym_pie_label] = "pie_label",
  [sym_flowchart_direction_lr] = "flowchart_direction_lr",
  [sym_flowchart_direction_rl] = "flowchart_direction_rl",
  [sym_flowchart_direction_tb] = "flowchart_direction_tb",
  [sym_flowchart_direction_bt] = "flowchart_direction_bt",
  [sym_flow_text_literal] = "flow_text_literal",
  [sym_flow_link_arrow] = "flow_link_arrow",
  [sym_flow_link_arrow_start] = "flow_link_arrow_start",
  [sym__er_word] = "_er_word",
  [sym_er_cardinarity_zero_or_one] = "er_cardinarity_zero_or_one",
  [sym_er_cardinarity_zero_or_more] = "er_cardinarity_zero_or_more",
  [sym_er_cardinarity_one_or_more] = "er_cardinarity_one_or_more",
  [sym_er_reltype_non_identifying] = "er_reltype_non_identifying",
  [sym_er_reltype_identifying] = "er_reltype_identifying",
  [sym_er_attribute_key_type_pk] = "er_attribute_key_type_pk",
  [sym_er_attribute_key_type_fk] = "er_attribute_key_type_fk",
  [aux_sym_source_file_repeat1] = "source_file_repeat1",
  [aux_sym_diagram_sequence_repeat1] = "diagram_sequence_repeat1",
  [aux_sym_sequence_actor_repeat1] = "sequence_actor_repeat1",
  [aux_sym_sequence_stmt_alt_repeat1] = "sequence_stmt_alt_repeat1",
  [aux_sym_sequence_stmt_par_repeat1] = "sequence_stmt_par_repeat1",
  [aux_sym_diagram_class_repeat1] = "diagram_class_repeat1",
  [aux_sym_class_name_body_repeat1] = "class_name_body_repeat1",
  [aux_sym_class_stmt_class_repeat1] = "class_stmt_class_repeat1",
  [aux_sym_class_method_line_repeat1] = "class_method_line_repeat1",
  [aux_sym_diagram_state_repeat1] = "diagram_state_repeat1",
  [aux_sym_state_composite_body_repeat1] = "state_composite_body_repeat1",
  [aux_sym_diagram_gantt_repeat1] = "diagram_gantt_repeat1",
  [aux_sym_diagram_pie_repeat1] = "diagram_pie_repeat1",
  [aux_sym_diagram_flow_repeat1] = "diagram_flow_repeat1",
  [aux_sym_flow_stmt_vertice_repeat1] = "flow_stmt_vertice_repeat1",
  [aux_sym_flow_node_repeat1] = "flow_node_repeat1",
  [aux_sym_flow_arrow_text_repeat1] = "flow_arrow_text_repeat1",
  [aux_sym_flow_stmt_subgraph_inner_repeat1] = "flow_stmt_subgraph_inner_repeat1",
  [aux_sym_diagram_er_repeat1] = "diagram_er_repeat1",
  [aux_sym_er_stmt_entity_block_inner_repeat1] = "er_stmt_entity_block_inner_repeat1",
  [aux_sym_diagram_mindmap_repeat1] = "diagram_mindmap_repeat1",
  [aux_sym_gantt_task_text_repeat1] = "gantt_task_text_repeat1",
  [aux_sym_flow_text_literal_repeat1] = "flow_text_literal_repeat1",
  [alias_sym_annotation] = "annotation",
  [alias_sym_class_classifier_abstract] = "class_classifier_abstract",
  [alias_sym_class_style_name] = "class_style_name",
  [alias_sym_class_visibility_internal] = "class_visibility_internal",
  [alias_sym_class_visibility_private] = "class_visibility_private",
  [alias_sym_class_visibility_public] = "class_visibility_public",
  [alias_sym_gantt_axis_format] = "gantt_axis_format",
  [alias_sym_gantt_end_dates] = "gantt_end_dates",
  [alias_sym_gantt_excludes] = "gantt_excludes",
  [alias_sym_gantt_includes] = "gantt_includes",
  [alias_sym_gantt_section] = "gantt_section",
  [alias_sym_gantt_title] = "gantt_title",
  [alias_sym_gantt_today_marker] = "gantt_today_marker",
  [alias_sym_gantt_top_axis] = "gantt_top_axis",
  [alias_sym_note_content] = "note_content",
  [alias_sym_sequence_alias] = "sequence_alias",
  [alias_sym_sequence_stmt_alt_branch] = "sequence_stmt_alt_branch",
  [alias_sym_sequence_stmt_loop_inner] = "sequence_stmt_loop_inner",
  [alias_sym_sequence_stmt_opt_inner] = "sequence_stmt_opt_inner",
  [alias_sym_sequence_stmt_rect_inner] = "sequence_stmt_rect_inner",
  [alias_sym_title] = "title",
};

static const TSSymbol ts_symbol_map[] = {
  [ts_builtin_sym_end] = ts_builtin_sym_end,
  [anon_sym_PERCENT_PERCENT_LBRACE] = anon_sym_PERCENT_PERCENT_LBRACE,
  [aux_sym_directive_token1] = aux_sym_directive_token1,
  [aux_sym_diagram_sequence_token1] = aux_sym_diagram_sequence_token1,
  [aux_sym__sequence_participant_type_token1] = aux_sym__sequence_participant_type_token1,
  [aux_sym__sequence_participant_type_token2] = aux_sym__sequence_participant_type_token2,
  [aux_sym_sequence_stmt_participant_token1] = aux_sym_sequence_stmt_participant_token1,
  [aux_sym_sequence_stmt_actor_token1] = aux_sym_sequence_stmt_actor_token1,
  [anon_sym_COLON] = anon_sym_COLON,
  [aux_sym_sequence_stmt_autonumber_token1] = aux_sym_sequence_stmt_autonumber_token1,
  [aux_sym_sequence_stmt_activate_token1] = aux_sym_sequence_stmt_activate_token1,
  [aux_sym_sequence_stmt_deactivate_token1] = aux_sym_sequence_stmt_deactivate_token1,
  [aux_sym_sequence_stmt_note_token1] = aux_sym_sequence_stmt_note_token1,
  [aux_sym_sequence_stmt_note_token2] = aux_sym_sequence_stmt_note_token2,
  [anon_sym_COMMA] = anon_sym_COMMA,
  [aux_sym_sequence_stmt_links_token1] = aux_sym_sequence_stmt_links_token1,
  [aux_sym_sequence_stmt_link_token1] = aux_sym_sequence_stmt_link_token1,
  [aux_sym_sequence_stmt_properties_token1] = aux_sym_sequence_stmt_properties_token1,
  [aux_sym_sequence_stmt_details_token1] = aux_sym_sequence_stmt_details_token1,
  [aux_sym_sequence_stmt_title_token1] = aux_sym_sequence_stmt_title_token1,
  [aux_sym_sequence_stmt_loop_token1] = aux_sym_sequence_stmt_loop_token1,
  [aux_sym_sequence_stmt_loop_token2] = anon_sym_end,
  [aux_sym_sequence_stmt_rect_token1] = aux_sym_sequence_stmt_rect_token1,
  [aux_sym_sequence_stmt_opt_token1] = aux_sym_sequence_stmt_opt_token1,
  [aux_sym_sequence_stmt_alt_token1] = aux_sym_sequence_stmt_alt_token1,
  [aux_sym_sequence_stmt_alt_token2] = aux_sym_sequence_stmt_alt_token2,
  [aux_sym_sequence_stmt_par_token1] = aux_sym_sequence_stmt_par_token1,
  [aux_sym_sequence_stmt_par_token2] = aux_sym_sequence_stmt_par_token2,
  [aux_sym_diagram_class_token1] = aux_sym_diagram_class_token1,
  [aux_sym_diagram_class_token2] = aux_sym_diagram_class_token2,
  [anon_sym_TILDE] = anon_sym_TILDE,
  [aux_sym_class_generics_token1] = sym_class_name,
  [anon_sym_class] = anon_sym_class,
  [anon_sym_COLON_COLON_COLON] = anon_sym_COLON_COLON_COLON,
  [anon_sym_LBRACE] = anon_sym_LBRACE,
  [anon_sym_RBRACE] = anon_sym_RBRACE,
  [anon_sym_PLUS] = anon_sym_PLUS,
  [anon_sym_DASH] = anon_sym_DASH,
  [anon_sym_POUND] = anon_sym_POUND,
  [anon_sym_LPAREN] = anon_sym_LPAREN,
  [anon_sym_RPAREN] = anon_sym_RPAREN,
  [anon_sym_STAR] = anon_sym_STAR,
  [anon_sym_DOLLAR] = anon_sym_DOLLAR,
  [anon_sym_LT_LT] = anon_sym_LT_LT,
  [anon_sym_GT_GT] = anon_sym_GT_GT,
  [aux_sym_diagram_state_token1] = aux_sym_diagram_state_token1,
  [aux_sym_diagram_state_token2] = aux_sym_diagram_state_token2,
  [aux_sym_state_stmt_simple_token1] = aux_sym_state_stmt_simple_token1,
  [aux_sym_diagram_gantt_token1] = aux_sym_diagram_gantt_token1,
  [aux_sym_gantt_stmt_dateformat_token1] = aux_sym_gantt_stmt_dateformat_token1,
  [aux_sym_gantt_stmt_inclusiveenddates_token1] = aux_sym_gantt_stmt_inclusiveenddates_token1,
  [aux_sym_gantt_stmt_topaxis_token1] = aux_sym_gantt_stmt_topaxis_token1,
  [aux_sym_gantt_stmt_axisformat_token1] = aux_sym_gantt_stmt_axisformat_token1,
  [aux_sym_gantt_stmt_includes_token1] = aux_sym_gantt_stmt_includes_token1,
  [aux_sym_gantt_stmt_excludes_token1] = aux_sym_gantt_stmt_excludes_token1,
  [aux_sym_gantt_stmt_todaymarker_token1] = aux_sym_gantt_stmt_todaymarker_token1,
  [aux_sym_gantt_stmt_section_token1] = aux_sym_gantt_stmt_section_token1,
  [aux_sym_diagram_pie_token1] = aux_sym_diagram_pie_token1,
  [aux_sym_diagram_flow_token1] = aux_sym_diagram_flow_token1,
  [anon_sym_SEMI] = anon_sym_SEMI,
  [aux_sym_flow_stmt_direction_token1] = aux_sym_flow_stmt_direction_token1,
  [anon_sym_AMP] = anon_sym_AMP,
  [anon_sym_PIPE] = anon_sym_PIPE,
  [anon_sym_LBRACK] = anon_sym_LBRACK,
  [anon_sym_RBRACK] = anon_sym_RBRACK,
  [anon_sym_LPAREN_LPAREN] = anon_sym_LPAREN_LPAREN,
  [anon_sym_RPAREN_RPAREN] = anon_sym_RPAREN_RPAREN,
  [anon_sym_LPAREN_DASH] = anon_sym_LPAREN_DASH,
  [anon_sym_DASH_RPAREN] = anon_sym_DASH_RPAREN,
  [anon_sym_LPAREN_LBRACK] = anon_sym_LPAREN_LBRACK,
  [anon_sym_RBRACK_RPAREN] = anon_sym_RBRACK_RPAREN,
  [anon_sym_LBRACK_LBRACK] = anon_sym_LBRACK_LBRACK,
  [anon_sym_RBRACK_RBRACK] = anon_sym_RBRACK_RBRACK,
  [anon_sym_LBRACK_PIPE] = anon_sym_LBRACK_PIPE,
  [anon_sym_PIPE_RBRACK] = anon_sym_PIPE_RBRACK,
  [anon_sym_LBRACK_LPAREN] = anon_sym_LBRACK_LPAREN,
  [anon_sym_RPAREN_RBRACK] = anon_sym_RPAREN_RBRACK,
  [anon_sym_LBRACE_LBRACE] = anon_sym_LBRACE_LBRACE,
  [anon_sym_RBRACE_RBRACE] = anon_sym_RBRACE_RBRACE,
  [anon_sym_GT] = anon_sym_GT,
  [anon_sym_LBRACK_SLASH] = anon_sym_LBRACK_SLASH,
  [anon_sym_BSLASH_RBRACK] = anon_sym_BSLASH_RBRACK,
  [anon_sym_LBRACK_BSLASH] = anon_sym_LBRACK_BSLASH,
  [anon_sym_SLASH_RBRACK] = anon_sym_SLASH_RBRACK,
  [anon_sym_subgraph] = anon_sym_subgraph,
  [anon_sym_end] = anon_sym_end,
  [aux_sym_diagram_er_token1] = aux_sym_diagram_er_token1,
  [aux_sym_diagram_mindmap_token1] = aux_sym_diagram_mindmap_token1,
  [aux_sym_mmap_markdown_token1] = aux_sym_mmap_markdown_token1,
  [aux_sym_mmap_text_token1] = aux_sym_mmap_text_token1,
  [aux_sym_mmap_class_token1] = aux_sym_mmap_class_token1,
  [aux_sym_mmap_class_token2] = aux_sym_mmap_class_token2,
  [aux_sym_mmap_icon_token1] = aux_sym_mmap_icon_token1,
  [aux_sym_mmap_icon_token2] = aux_sym_mmap_icon_token2,
  [sym__whitespace] = sym__whitespace,
  [sym__newline] = sym__newline,
  [sym_comment] = sym_comment,
  [sym__md_start] = sym__md_start,
  [sym__md_end] = sym__md_end,
  [sym_type_directive] = sym_type_directive,
  [aux_sym_direction_tb_token1] = aux_sym_direction_tb_token1,
  [aux_sym_direction_bt_token1] = aux_sym_direction_bt_token1,
  [aux_sym_direction_rl_token1] = aux_sym_direction_rl_token1,
  [aux_sym_direction_lr_token1] = aux_sym_direction_lr_token1,
  [aux_sym__sequence_rest_text_token1] = aux_sym__sequence_rest_text_token1,
  [sym__sequence_actor_word] = sym__sequence_actor_word,
  [sym_solid_arrow] = sym_solid_arrow,
  [sym_dotted_arrow] = sym_dotted_arrow,
  [sym_solid_open_arrow] = sym_solid_open_arrow,
  [anon_sym_DASH_DASH_GT] = anon_sym_DASH_DASH_GT,
  [sym_solid_cross] = sym_solid_cross,
  [sym_dotted_cross] = sym_dotted_cross,
  [sym_dotted_point] = sym_dotted_point,
  [aux_sym_note_placement_left_token1] = aux_sym_note_placement_left_token1,
  [aux_sym_note_placement_right_token1] = aux_sym_note_placement_right_token1,
  [sym__class_name] = sym__class_name,
  [sym__alpha_num_token] = sym__alpha_num_token,
  [sym__bquote_string] = sym__bquote_string,
  [sym__dquote_string] = sym__dquote_string,
  [sym_class_reltype_aggregation] = sym_class_reltype_aggregation,
  [anon_sym_LT_PIPE] = anon_sym_LT_PIPE,
  [anon_sym_PIPE_GT] = anon_sym_PIPE_GT,
  [anon_sym_LT] = anon_sym_LT,
  [anon_sym_DASH_DASH] = anon_sym_DASH_DASH,
  [anon_sym_DOT_DOT] = anon_sym_DOT_DOT,
  [aux_sym_class_label_token1] = aux_sym_class_label_token1,
  [anon_sym_LBRACK_STAR_RBRACK] = anon_sym_LBRACK_STAR_RBRACK,
  [aux_sym_state_name_token1] = aux_sym_state_name_token1,
  [aux_sym_state_hide_empty_description_token1] = aux_sym_state_hide_empty_description_token1,
  [sym_state_id] = sym_state_id,
  [aux_sym_state_annotation_fork_token1] = aux_sym_state_annotation_fork_token1,
  [aux_sym_state_annotation_fork_token2] = aux_sym_state_annotation_fork_token2,
  [aux_sym_state_annotation_join_token1] = aux_sym_state_annotation_join_token1,
  [aux_sym_state_annotation_join_token2] = aux_sym_state_annotation_join_token2,
  [aux_sym_state_annotation_choice_token1] = aux_sym_state_annotation_choice_token1,
  [aux_sym_state_annotation_choice_token2] = aux_sym_state_annotation_choice_token2,
  [aux_sym_gantt_task_text_token1] = aux_sym_gantt_task_text_token1,
  [sym_gantt_task_data] = sym_gantt_task_data,
  [aux_sym_pie_showdata_token1] = aux_sym_pie_showdata_token1,
  [sym_pie_title] = sym_pie_title,
  [aux_sym_pie_label_token1] = aux_sym_pie_label_token1,
  [sym_pie_value] = sym_pie_value,
  [aux_sym_flowchart_direction_lr_token1] = aux_sym_flowchart_direction_lr_token1,
  [aux_sym_flowchart_direction_lr_token2] = aux_sym_flowchart_direction_lr_token2,
  [aux_sym_flowchart_direction_rl_token1] = aux_sym_flowchart_direction_rl_token1,
  [aux_sym_flowchart_direction_tb_token1] = aux_sym_flowchart_direction_tb_token1,
  [aux_sym_flowchart_direction_tb_token2] = aux_sym_flowchart_direction_tb_token2,
  [anon_sym_v] = anon_sym_v,
  [aux_sym_flowchart_direction_bt_token1] = aux_sym_flowchart_direction_bt_token1,
  [anon_sym_CARET] = anon_sym_CARET,
  [aux_sym_flow_text_literal_token1] = aux_sym_flow_text_literal_token1,
  [sym_flow_text_quoted] = sym_flow_text_quoted,
  [aux_sym_flow_link_arrow_token1] = aux_sym_flow_link_arrow_token1,
  [aux_sym_flow_link_arrow_token2] = aux_sym_flow_link_arrow_token2,
  [aux_sym_flow_link_arrow_token3] = aux_sym_flow_link_arrow_token3,
  [aux_sym_flow_link_arrow_start_token1] = aux_sym_flow_link_arrow_start_token1,
  [aux_sym_flow_link_arrow_start_token2] = aux_sym_flow_link_arrow_start_token2,
  [aux_sym_flow_link_arrow_start_token3] = aux_sym_flow_link_arrow_start_token3,
  [sym__er_alphanum] = sym__er_alphanum,
  [anon_sym_PIPEo] = anon_sym_PIPEo,
  [anon_sym_o_PIPE] = anon_sym_o_PIPE,
  [anon_sym_RBRACEo] = anon_sym_RBRACEo,
  [anon_sym_o_LBRACE] = anon_sym_o_LBRACE,
  [anon_sym_RBRACE_PIPE] = anon_sym_RBRACE_PIPE,
  [anon_sym_PIPE_LBRACE] = anon_sym_PIPE_LBRACE,
  [sym_er_cardinarity_only_one] = sym_er_cardinarity_only_one,
  [anon_sym_DOT_DASH] = anon_sym_DOT_DASH,
  [anon_sym_DASH_DOT] = anon_sym_DASH_DOT,
  [aux_sym_er_attribute_key_type_pk_token1] = aux_sym_er_attribute_key_type_pk_token1,
  [aux_sym_er_attribute_key_type_fk_token1] = aux_sym_er_attribute_key_type_fk_token1,
  [sym__mindmap_text] = sym__mindmap_text,
  [sym_source_file] = sym_source_file,
  [sym_directive] = sym_directive,
  [sym__direction] = sym__direction,
  [sym_diagram_sequence] = sym_diagram_sequence,
  [sym__sequence_stmt] = sym__sequence_stmt,
  [sym__sequence_participant_type] = sym__sequence_participant_type,
  [sym_sequence_stmt_participant] = sym_sequence_stmt_participant,
  [sym_sequence_stmt_actor] = sym_sequence_stmt_actor,
  [sym_sequence_actor] = sym_sequence_actor,
  [sym_sequence_stmt_signal] = sym_sequence_stmt_signal,
  [sym_sequence_signal_type] = sym_sequence_signal_type,
  [sym_sequence_text] = sym_sequence_text,
  [sym_sequence_stmt_autonumber] = sym_sequence_stmt_autonumber,
  [sym_sequence_stmt_activate] = sym_sequence_stmt_activate,
  [sym_sequence_stmt_deactivate] = sym_sequence_stmt_deactivate,
  [sym_sequence_stmt_note] = sym_sequence_stmt_note,
  [sym_sequence_stmt_links] = sym_sequence_stmt_links,
  [sym_sequence_stmt_link] = sym_sequence_stmt_link,
  [sym_sequence_stmt_properties] = sym_sequence_stmt_properties,
  [sym_sequence_stmt_details] = sym_sequence_stmt_details,
  [sym_sequence_note_placement] = sym_sequence_note_placement,
  [sym_sequence_stmt_title] = sym_sequence_stmt_title,
  [sym_sequence_stmt_loop] = sym_sequence_stmt_loop,
  [sym_sequence_stmt_rect] = sym_sequence_stmt_rect,
  [sym_sequence_stmt_opt] = sym_sequence_stmt_opt,
  [aux_sym__sequence_subdocument] = aux_sym__sequence_subdocument,
  [sym_sequence_stmt_alt] = sym_sequence_stmt_alt,
  [sym_sequence_stmt_par] = sym_sequence_stmt_par,
  [sym_diagram_class] = sym_diagram_class,
  [sym__class_stmt] = sym__class_stmt,
  [sym_class_stmt_relation] = sym_class_stmt_relation,
  [sym_class_name] = sym_class_name,
  [sym_class_name_body] = sym_class_name_body,
  [sym_class_generics] = sym_class_generics,
  [sym_class_relation] = sym_class_relation,
  [sym__class_reltype] = sym__class_reltype,
  [sym__class_linetype] = sym__class_linetype,
  [sym_class_stmt_class] = sym_class_stmt_class,
  [sym_class_method_line] = sym_class_method_line,
  [sym_class_annotation_line] = sym_class_annotation_line,
  [sym_class_stmt_method] = sym_class_stmt_method,
  [sym_class_stmt_annotation] = sym_class_stmt_annotation,
  [sym_diagram_state] = sym_diagram_state,
  [sym__state_stmt] = sym__state_stmt,
  [sym_state_stmt_simple] = sym_state_stmt_simple,
  [sym_state_stmt_arrow] = sym_state_stmt_arrow,
  [sym_state_stmt_composite] = sym_state_stmt_composite,
  [sym_state_composite_body] = sym_state_composite_body,
  [sym_state_stmt_annotation] = sym_state_stmt_annotation,
  [sym_state_alias] = sym_state_alias,
  [sym_state_stmt_hide_empty_description] = sym_state_stmt_hide_empty_description,
  [sym__state_annotation] = sym__state_annotation,
  [sym_state_note] = sym_state_note,
  [sym_state_note_placement] = sym_state_note_placement,
  [sym_diagram_gantt] = sym_diagram_gantt,
  [sym__gantt_stmt] = sym__gantt_stmt,
  [sym_gantt_stmt_dateformat] = sym_gantt_stmt_dateformat,
  [sym_gantt_stmt_inclusiveenddates] = sym_gantt_stmt_inclusiveenddates,
  [sym_gantt_stmt_topaxis] = sym_gantt_stmt_topaxis,
  [sym_gantt_stmt_axisformat] = sym_gantt_stmt_axisformat,
  [sym_gantt_stmt_includes] = sym_gantt_stmt_includes,
  [sym_gantt_stmt_excludes] = sym_gantt_stmt_excludes,
  [sym_gantt_stmt_todaymarker] = sym_gantt_stmt_todaymarker,
  [sym_gantt_stmt_title] = sym_gantt_stmt_title,
  [sym_gantt_stmt_section] = sym_gantt_stmt_section,
  [sym_gantt_stmt_task] = sym_gantt_stmt_task,
  [sym_diagram_pie] = sym_diagram_pie,
  [sym__pie_stmt] = sym__pie_stmt,
  [sym_pie_stmt_title] = sym_pie_stmt_title,
  [sym_pie_stmt_element] = sym_pie_stmt_element,
  [sym_diagram_flow] = sym_diagram_flow,
  [sym__flowchart_direction] = sym__flowchart_direction,
  [sym__flow_stmt] = sym__flow_stmt,
  [sym_flow_stmt_direction] = sym_flow_stmt_direction,
  [sym_flow_stmt_vertice] = sym_flow_stmt_vertice,
  [sym_flow_node] = sym_flow_node,
  [sym__flow_link] = sym__flow_link,
  [sym_flow_link_simplelink] = sym_flow_link_simplelink,
  [sym_flow_link_arrowtext] = sym_flow_link_arrowtext,
  [sym_flow_link_middletext] = sym_flow_link_middletext,
  [sym_flow_arrow_text] = sym_flow_arrow_text,
  [sym_flow_vertex_id] = sym_flow_vertex_id,
  [sym_flow_vertex] = sym_flow_vertex,
  [sym__flow_vertex_kind] = sym__flow_vertex_kind,
  [sym_flow_vertex_square] = sym_flow_vertex_square,
  [sym_flow_vertex_circle] = sym_flow_vertex_circle,
  [sym_flow_vertex_ellipse] = sym_flow_vertex_ellipse,
  [sym_flow_vertex_stadium] = sym_flow_vertex_stadium,
  [sym_flow_vertex_subroutine] = sym_flow_vertex_subroutine,
  [sym_flow_vertex_rect] = sym_flow_vertex_rect,
  [sym_flow_vertex_cylinder] = sym_flow_vertex_cylinder,
  [sym_flow_vertex_round] = sym_flow_vertex_round,
  [sym_flow_vertex_diamond] = sym_flow_vertex_diamond,
  [sym_flow_vertex_hexagon] = sym_flow_vertex_hexagon,
  [sym_flow_vertex_odd] = sym_flow_vertex_odd,
  [sym_flow_vertex_trapezoid] = sym_flow_vertex_trapezoid,
  [sym_flow_vertex_inv_trapezoid] = sym_flow_vertex_inv_trapezoid,
  [sym_flow_vertex_leanright] = sym_flow_vertex_leanright,
  [sym_flow_vertex_leanleft] = sym_flow_vertex_leanleft,
  [sym__flow_text] = sym__flow_text,
  [sym_flow_stmt_subgraph] = sym_flow_stmt_subgraph,
  [sym_flow_stmt_subgraph_inner] = sym_flow_stmt_subgraph_inner,
  [sym_flow_vertex_text] = sym_flow_vertex_text,
  [sym_diagram_er] = sym_diagram_er,
  [sym__er_stmt] = sym__er_stmt,
  [sym_er_stmt_entity] = sym_er_stmt_entity,
  [sym_er_stmt_entity_relation] = sym_er_stmt_entity_relation,
  [sym_er_entity_name] = sym_er_entity_name,
  [sym_er_relation] = sym_er_relation,
  [sym__er_cardinarity] = sym__er_cardinarity,
  [sym__er_reltype] = sym__er_reltype,
  [sym_er_role] = sym_er_role,
  [sym_er_stmt_entity_block] = sym_er_stmt_entity_block,
  [sym_er_stmt_entity_block_inner] = sym_er_stmt_entity_block_inner,
  [sym_er_attribute] = sym_er_attribute,
  [sym_er_attribute_type] = sym_er_attribute_type,
  [sym_er_attribute_name] = sym_er_attribute_name,
  [sym__er_attribute_key_type] = sym__er_attribute_key_type,
  [sym_er_attribute_comment] = sym_er_attribute_comment,
  [sym_diagram_mindmap] = sym_diagram_mindmap,
  [sym_mmap_node] = sym_mmap_node,
  [sym_mmap_node_id] = sym_mmap_node_id,
  [sym_mmap_markdown] = sym_mmap_markdown,
  [sym_mmap_text] = sym_mmap_text,
  [sym_mmap_node_content] = sym_mmap_node_content,
  [sym_mmap_node_square] = sym_mmap_node_square,
  [sym_mmap_node_rounded] = sym_mmap_node_rounded,
  [sym_mmap_node_circle] = sym_mmap_node_circle,
  [sym_mmap_node_cloud] = sym_mmap_node_cloud,
  [sym_mmap_node_bang] = sym_mmap_node_bang,
  [sym_mmap_node_hexagon] = sym_mmap_node_hexagon,
  [sym_mmap_class] = sym_mmap_class,
  [sym_mmap_icon] = sym_mmap_icon,
  [sym_direction_tb] = sym_direction_tb,
  [sym_direction_bt] = sym_direction_bt,
  [sym_direction_rl] = sym_direction_rl,
  [sym_direction_lr] = sym_direction_lr,
  [sym__sequence_rest_text] = sym__sequence_rest_text,
  [sym_dotted_open_arrow] = sym_dotted_open_arrow,
  [sym_solid_point] = sym_solid_point,
  [sym_sequence_signal_plus_sign] = sym_sequence_signal_plus_sign,
  [sym_sequence_signal_minus_sign] = sym_sequence_signal_minus_sign,
  [sym_note_placement_left] = sym_note_placement_left,
  [sym_note_placement_right] = sym_note_placement_right,
  [sym_class_reltype_extension] = sym_class_reltype_extension,
  [sym_class_reltype_composition] = sym_class_reltype_composition,
  [sym_class_reltype_dependency] = sym_class_reltype_dependency,
  [sym_class_linetype_solid] = sym_class_linetype_solid,
  [sym_class_linetype_dotted] = sym_class_linetype_dotted,
  [sym_class_label] = sym_class_label,
  [sym_state_name] = sym_state_name,
  [sym_state_arrow] = sym_state_arrow,
  [sym_state_description] = sym_state_description,
  [sym_state_hide_empty_description] = sym_state_hide_empty_description,
  [sym_state_division] = sym_state_division,
  [sym_state_annotation_fork] = sym_state_annotation_fork,
  [sym_state_annotation_join] = sym_state_annotation_join,
  [sym_state_annotation_choice] = sym_state_annotation_choice,
  [sym_gantt_meta_format] = sym_gantt_meta_format,
  [sym_gantt_task_text] = sym_gantt_task_text,
  [sym_pie_showdata] = sym_pie_showdata,
  [sym_pie_label] = sym_pie_label,
  [sym_flowchart_direction_lr] = sym_flowchart_direction_lr,
  [sym_flowchart_direction_rl] = sym_flowchart_direction_rl,
  [sym_flowchart_direction_tb] = sym_flowchart_direction_tb,
  [sym_flowchart_direction_bt] = sym_flowchart_direction_bt,
  [sym_flow_text_literal] = sym_flow_text_literal,
  [sym_flow_link_arrow] = sym_flow_link_arrow,
  [sym_flow_link_arrow_start] = sym_flow_link_arrow_start,
  [sym__er_word] = sym__er_word,
  [sym_er_cardinarity_zero_or_one] = sym_er_cardinarity_zero_or_one,
  [sym_er_cardinarity_zero_or_more] = sym_er_cardinarity_zero_or_more,
  [sym_er_cardinarity_one_or_more] = sym_er_cardinarity_one_or_more,
  [sym_er_reltype_non_identifying] = sym_er_reltype_non_identifying,
  [sym_er_reltype_identifying] = sym_er_reltype_identifying,
  [sym_er_attribute_key_type_pk] = sym_er_attribute_key_type_pk,
  [sym_er_attribute_key_type_fk] = sym_er_attribute_key_type_fk,
  [aux_sym_source_file_repeat1] = aux_sym_source_file_repeat1,
  [aux_sym_diagram_sequence_repeat1] = aux_sym_diagram_sequence_repeat1,
  [aux_sym_sequence_actor_repeat1] = aux_sym_sequence_actor_repeat1,
  [aux_sym_sequence_stmt_alt_repeat1] = aux_sym_sequence_stmt_alt_repeat1,
  [aux_sym_sequence_stmt_par_repeat1] = aux_sym_sequence_stmt_par_repeat1,
  [aux_sym_diagram_class_repeat1] = aux_sym_diagram_class_repeat1,
  [aux_sym_class_name_body_repeat1] = aux_sym_class_name_body_repeat1,
  [aux_sym_class_stmt_class_repeat1] = aux_sym_class_stmt_class_repeat1,
  [aux_sym_class_method_line_repeat1] = aux_sym_class_method_line_repeat1,
  [aux_sym_diagram_state_repeat1] = aux_sym_diagram_state_repeat1,
  [aux_sym_state_composite_body_repeat1] = aux_sym_state_composite_body_repeat1,
  [aux_sym_diagram_gantt_repeat1] = aux_sym_diagram_gantt_repeat1,
  [aux_sym_diagram_pie_repeat1] = aux_sym_diagram_pie_repeat1,
  [aux_sym_diagram_flow_repeat1] = aux_sym_diagram_flow_repeat1,
  [aux_sym_flow_stmt_vertice_repeat1] = aux_sym_flow_stmt_vertice_repeat1,
  [aux_sym_flow_node_repeat1] = aux_sym_flow_node_repeat1,
  [aux_sym_flow_arrow_text_repeat1] = aux_sym_flow_arrow_text_repeat1,
  [aux_sym_flow_stmt_subgraph_inner_repeat1] = aux_sym_flow_stmt_subgraph_inner_repeat1,
  [aux_sym_diagram_er_repeat1] = aux_sym_diagram_er_repeat1,
  [aux_sym_er_stmt_entity_block_inner_repeat1] = aux_sym_er_stmt_entity_block_inner_repeat1,
  [aux_sym_diagram_mindmap_repeat1] = aux_sym_diagram_mindmap_repeat1,
  [aux_sym_gantt_task_text_repeat1] = aux_sym_gantt_task_text_repeat1,
  [aux_sym_flow_text_literal_repeat1] = aux_sym_flow_text_literal_repeat1,
  [alias_sym_annotation] = alias_sym_annotation,
  [alias_sym_class_classifier_abstract] = alias_sym_class_classifier_abstract,
  [alias_sym_class_style_name] = alias_sym_class_style_name,
  [alias_sym_class_visibility_internal] = alias_sym_class_visibility_internal,
  [alias_sym_class_visibility_private] = alias_sym_class_visibility_private,
  [alias_sym_class_visibility_public] = alias_sym_class_visibility_public,
  [alias_sym_gantt_axis_format] = alias_sym_gantt_axis_format,
  [alias_sym_gantt_end_dates] = alias_sym_gantt_end_dates,
  [alias_sym_gantt_excludes] = alias_sym_gantt_excludes,
  [alias_sym_gantt_includes] = alias_sym_gantt_includes,
  [alias_sym_gantt_section] = alias_sym_gantt_section,
  [alias_sym_gantt_title] = alias_sym_gantt_title,
  [alias_sym_gantt_today_marker] = alias_sym_gantt_today_marker,
  [alias_sym_gantt_top_axis] = alias_sym_gantt_top_axis,
  [alias_sym_note_content] = alias_sym_note_content,
  [alias_sym_sequence_alias] = alias_sym_sequence_alias,
  [alias_sym_sequence_stmt_alt_branch] = alias_sym_sequence_stmt_alt_branch,
  [alias_sym_sequence_stmt_loop_inner] = alias_sym_sequence_stmt_loop_inner,
  [alias_sym_sequence_stmt_opt_inner] = alias_sym_sequence_stmt_opt_inner,
  [alias_sym_sequence_stmt_rect_inner] = alias_sym_sequence_stmt_rect_inner,
  [alias_sym_title] = alias_sym_title,
};

static const TSSymbolMetadata ts_symbol_metadata[] = {
  [ts_builtin_sym_end] = {
    .visible = false,
    .named = true,
  },
  [anon_sym_PERCENT_PERCENT_LBRACE] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_directive_token1] = {
    .visible = true,
    .named = true,
  },
  [aux_sym_diagram_sequence_token1] = {
    .visible = true,
    .named = false,
  },
  [aux_sym__sequence_participant_type_token1] = {
    .visible = true,
    .named = false,
  },
  [aux_sym__sequence_participant_type_token2] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_sequence_stmt_participant_token1] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_sequence_stmt_actor_token1] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_COLON] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_sequence_stmt_autonumber_token1] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_sequence_stmt_activate_token1] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_sequence_stmt_deactivate_token1] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_sequence_stmt_note_token1] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_sequence_stmt_note_token2] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_COMMA] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_sequence_stmt_links_token1] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_sequence_stmt_link_token1] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_sequence_stmt_properties_token1] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_sequence_stmt_details_token1] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_sequence_stmt_title_token1] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_sequence_stmt_loop_token1] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_sequence_stmt_loop_token2] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_sequence_stmt_rect_token1] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_sequence_stmt_opt_token1] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_sequence_stmt_alt_token1] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_sequence_stmt_alt_token2] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_sequence_stmt_par_token1] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_sequence_stmt_par_token2] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_diagram_class_token1] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_diagram_class_token2] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_TILDE] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_class_generics_token1] = {
    .visible = true,
    .named = true,
  },
  [anon_sym_class] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_COLON_COLON_COLON] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_LBRACE] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_RBRACE] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_PLUS] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_DASH] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_POUND] = {
    .visible = true,
    .named = true,
  },
  [anon_sym_LPAREN] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_RPAREN] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_STAR] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_DOLLAR] = {
    .visible = true,
    .named = true,
  },
  [anon_sym_LT_LT] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_GT_GT] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_diagram_state_token1] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_diagram_state_token2] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_state_stmt_simple_token1] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_diagram_gantt_token1] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_gantt_stmt_dateformat_token1] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_gantt_stmt_inclusiveenddates_token1] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_gantt_stmt_topaxis_token1] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_gantt_stmt_axisformat_token1] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_gantt_stmt_includes_token1] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_gantt_stmt_excludes_token1] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_gantt_stmt_todaymarker_token1] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_gantt_stmt_section_token1] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_diagram_pie_token1] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_diagram_flow_token1] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_SEMI] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_flow_stmt_direction_token1] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_AMP] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_PIPE] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_LBRACK] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_RBRACK] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_LPAREN_LPAREN] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_RPAREN_RPAREN] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_LPAREN_DASH] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_DASH_RPAREN] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_LPAREN_LBRACK] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_RBRACK_RPAREN] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_LBRACK_LBRACK] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_RBRACK_RBRACK] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_LBRACK_PIPE] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_PIPE_RBRACK] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_LBRACK_LPAREN] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_RPAREN_RBRACK] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_LBRACE_LBRACE] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_RBRACE_RBRACE] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_GT] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_LBRACK_SLASH] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_BSLASH_RBRACK] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_LBRACK_BSLASH] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_SLASH_RBRACK] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_subgraph] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_end] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_diagram_er_token1] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_diagram_mindmap_token1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_mmap_markdown_token1] = {
    .visible = true,
    .named = true,
  },
  [aux_sym_mmap_text_token1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_mmap_class_token1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_mmap_class_token2] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_mmap_icon_token1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_mmap_icon_token2] = {
    .visible = false,
    .named = false,
  },
  [sym__whitespace] = {
    .visible = false,
    .named = true,
  },
  [sym__newline] = {
    .visible = false,
    .named = true,
  },
  [sym_comment] = {
    .visible = true,
    .named = true,
  },
  [sym__md_start] = {
    .visible = false,
    .named = true,
  },
  [sym__md_end] = {
    .visible = false,
    .named = true,
  },
  [sym_type_directive] = {
    .visible = true,
    .named = true,
  },
  [aux_sym_direction_tb_token1] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_direction_bt_token1] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_direction_rl_token1] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_direction_lr_token1] = {
    .visible = true,
    .named = false,
  },
  [aux_sym__sequence_rest_text_token1] = {
    .visible = false,
    .named = false,
  },
  [sym__sequence_actor_word] = {
    .visible = false,
    .named = true,
  },
  [sym_solid_arrow] = {
    .visible = true,
    .named = true,
  },
  [sym_dotted_arrow] = {
    .visible = true,
    .named = true,
  },
  [sym_solid_open_arrow] = {
    .visible = true,
    .named = true,
  },
  [anon_sym_DASH_DASH_GT] = {
    .visible = true,
    .named = false,
  },
  [sym_solid_cross] = {
    .visible = true,
    .named = true,
  },
  [sym_dotted_cross] = {
    .visible = true,
    .named = true,
  },
  [sym_dotted_point] = {
    .visible = true,
    .named = true,
  },
  [aux_sym_note_placement_left_token1] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_note_placement_right_token1] = {
    .visible = true,
    .named = false,
  },
  [sym__class_name] = {
    .visible = false,
    .named = true,
  },
  [sym__alpha_num_token] = {
    .visible = false,
    .named = true,
  },
  [sym__bquote_string] = {
    .visible = false,
    .named = true,
  },
  [sym__dquote_string] = {
    .visible = true,
    .named = true,
  },
  [sym_class_reltype_aggregation] = {
    .visible = true,
    .named = true,
  },
  [anon_sym_LT_PIPE] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_PIPE_GT] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_LT] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_DASH_DASH] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_DOT_DOT] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_class_label_token1] = {
    .visible = false,
    .named = false,
  },
  [anon_sym_LBRACK_STAR_RBRACK] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_state_name_token1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_state_hide_empty_description_token1] = {
    .visible = true,
    .named = false,
  },
  [sym_state_id] = {
    .visible = true,
    .named = true,
  },
  [aux_sym_state_annotation_fork_token1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_state_annotation_fork_token2] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_state_annotation_join_token1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_state_annotation_join_token2] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_state_annotation_choice_token1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_state_annotation_choice_token2] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_gantt_task_text_token1] = {
    .visible = false,
    .named = false,
  },
  [sym_gantt_task_data] = {
    .visible = true,
    .named = true,
  },
  [aux_sym_pie_showdata_token1] = {
    .visible = true,
    .named = false,
  },
  [sym_pie_title] = {
    .visible = true,
    .named = true,
  },
  [aux_sym_pie_label_token1] = {
    .visible = false,
    .named = false,
  },
  [sym_pie_value] = {
    .visible = true,
    .named = true,
  },
  [aux_sym_flowchart_direction_lr_token1] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_flowchart_direction_lr_token2] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_flowchart_direction_rl_token1] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_flowchart_direction_tb_token1] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_flowchart_direction_tb_token2] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_v] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_flowchart_direction_bt_token1] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_CARET] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_flow_text_literal_token1] = {
    .visible = false,
    .named = false,
  },
  [sym_flow_text_quoted] = {
    .visible = true,
    .named = true,
  },
  [aux_sym_flow_link_arrow_token1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_flow_link_arrow_token2] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_flow_link_arrow_token3] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_flow_link_arrow_start_token1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_flow_link_arrow_start_token2] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_flow_link_arrow_start_token3] = {
    .visible = false,
    .named = false,
  },
  [sym__er_alphanum] = {
    .visible = false,
    .named = true,
  },
  [anon_sym_PIPEo] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_o_PIPE] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_RBRACEo] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_o_LBRACE] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_RBRACE_PIPE] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_PIPE_LBRACE] = {
    .visible = true,
    .named = false,
  },
  [sym_er_cardinarity_only_one] = {
    .visible = true,
    .named = true,
  },
  [anon_sym_DOT_DASH] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_DASH_DOT] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_er_attribute_key_type_pk_token1] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_er_attribute_key_type_fk_token1] = {
    .visible = true,
    .named = false,
  },
  [sym__mindmap_text] = {
    .visible = false,
    .named = true,
  },
  [sym_source_file] = {
    .visible = true,
    .named = true,
  },
  [sym_directive] = {
    .visible = true,
    .named = true,
  },
  [sym__direction] = {
    .visible = false,
    .named = true,
    .supertype = true,
  },
  [sym_diagram_sequence] = {
    .visible = true,
    .named = true,
  },
  [sym__sequence_stmt] = {
    .visible = false,
    .named = true,
    .supertype = true,
  },
  [sym__sequence_participant_type] = {
    .visible = false,
    .named = true,
  },
  [sym_sequence_stmt_participant] = {
    .visible = true,
    .named = true,
  },
  [sym_sequence_stmt_actor] = {
    .visible = true,
    .named = true,
  },
  [sym_sequence_actor] = {
    .visible = true,
    .named = true,
  },
  [sym_sequence_stmt_signal] = {
    .visible = true,
    .named = true,
  },
  [sym_sequence_signal_type] = {
    .visible = true,
    .named = true,
  },
  [sym_sequence_text] = {
    .visible = true,
    .named = true,
  },
  [sym_sequence_stmt_autonumber] = {
    .visible = true,
    .named = true,
  },
  [sym_sequence_stmt_activate] = {
    .visible = true,
    .named = true,
  },
  [sym_sequence_stmt_deactivate] = {
    .visible = true,
    .named = true,
  },
  [sym_sequence_stmt_note] = {
    .visible = true,
    .named = true,
  },
  [sym_sequence_stmt_links] = {
    .visible = true,
    .named = true,
  },
  [sym_sequence_stmt_link] = {
    .visible = true,
    .named = true,
  },
  [sym_sequence_stmt_properties] = {
    .visible = true,
    .named = true,
  },
  [sym_sequence_stmt_details] = {
    .visible = true,
    .named = true,
  },
  [sym_sequence_note_placement] = {
    .visible = true,
    .named = true,
  },
  [sym_sequence_stmt_title] = {
    .visible = true,
    .named = true,
  },
  [sym_sequence_stmt_loop] = {
    .visible = true,
    .named = true,
  },
  [sym_sequence_stmt_rect] = {
    .visible = true,
    .named = true,
  },
  [sym_sequence_stmt_opt] = {
    .visible = true,
    .named = true,
  },
  [aux_sym__sequence_subdocument] = {
    .visible = false,
    .named = false,
  },
  [sym_sequence_stmt_alt] = {
    .visible = true,
    .named = true,
  },
  [sym_sequence_stmt_par] = {
    .visible = true,
    .named = true,
  },
  [sym_diagram_class] = {
    .visible = true,
    .named = true,
  },
  [sym__class_stmt] = {
    .visible = false,
    .named = true,
    .supertype = true,
  },
  [sym_class_stmt_relation] = {
    .visible = true,
    .named = true,
  },
  [sym_class_name] = {
    .visible = true,
    .named = true,
  },
  [sym_class_name_body] = {
    .visible = true,
    .named = true,
  },
  [sym_class_generics] = {
    .visible = true,
    .named = true,
  },
  [sym_class_relation] = {
    .visible = true,
    .named = true,
  },
  [sym__class_reltype] = {
    .visible = false,
    .named = true,
    .supertype = true,
  },
  [sym__class_linetype] = {
    .visible = false,
    .named = true,
    .supertype = true,
  },
  [sym_class_stmt_class] = {
    .visible = true,
    .named = true,
  },
  [sym_class_method_line] = {
    .visible = true,
    .named = true,
  },
  [sym_class_annotation_line] = {
    .visible = true,
    .named = true,
  },
  [sym_class_stmt_method] = {
    .visible = true,
    .named = true,
  },
  [sym_class_stmt_annotation] = {
    .visible = true,
    .named = true,
  },
  [sym_diagram_state] = {
    .visible = true,
    .named = true,
  },
  [sym__state_stmt] = {
    .visible = false,
    .named = true,
    .supertype = true,
  },
  [sym_state_stmt_simple] = {
    .visible = true,
    .named = true,
  },
  [sym_state_stmt_arrow] = {
    .visible = true,
    .named = true,
  },
  [sym_state_stmt_composite] = {
    .visible = true,
    .named = true,
  },
  [sym_state_composite_body] = {
    .visible = true,
    .named = true,
  },
  [sym_state_stmt_annotation] = {
    .visible = true,
    .named = true,
  },
  [sym_state_alias] = {
    .visible = true,
    .named = true,
  },
  [sym_state_stmt_hide_empty_description] = {
    .visible = true,
    .named = true,
  },
  [sym__state_annotation] = {
    .visible = false,
    .named = true,
    .supertype = true,
  },
  [sym_state_note] = {
    .visible = true,
    .named = true,
  },
  [sym_state_note_placement] = {
    .visible = true,
    .named = true,
  },
  [sym_diagram_gantt] = {
    .visible = true,
    .named = true,
  },
  [sym__gantt_stmt] = {
    .visible = false,
    .named = true,
    .supertype = true,
  },
  [sym_gantt_stmt_dateformat] = {
    .visible = true,
    .named = true,
  },
  [sym_gantt_stmt_inclusiveenddates] = {
    .visible = true,
    .named = true,
  },
  [sym_gantt_stmt_topaxis] = {
    .visible = true,
    .named = true,
  },
  [sym_gantt_stmt_axisformat] = {
    .visible = true,
    .named = true,
  },
  [sym_gantt_stmt_includes] = {
    .visible = true,
    .named = true,
  },
  [sym_gantt_stmt_excludes] = {
    .visible = true,
    .named = true,
  },
  [sym_gantt_stmt_todaymarker] = {
    .visible = true,
    .named = true,
  },
  [sym_gantt_stmt_title] = {
    .visible = true,
    .named = true,
  },
  [sym_gantt_stmt_section] = {
    .visible = true,
    .named = true,
  },
  [sym_gantt_stmt_task] = {
    .visible = true,
    .named = true,
  },
  [sym_diagram_pie] = {
    .visible = true,
    .named = true,
  },
  [sym__pie_stmt] = {
    .visible = false,
    .named = true,
    .supertype = true,
  },
  [sym_pie_stmt_title] = {
    .visible = true,
    .named = true,
  },
  [sym_pie_stmt_element] = {
    .visible = true,
    .named = true,
  },
  [sym_diagram_flow] = {
    .visible = true,
    .named = true,
  },
  [sym__flowchart_direction] = {
    .visible = false,
    .named = true,
    .supertype = true,
  },
  [sym__flow_stmt] = {
    .visible = false,
    .named = true,
    .supertype = true,
  },
  [sym_flow_stmt_direction] = {
    .visible = true,
    .named = true,
  },
  [sym_flow_stmt_vertice] = {
    .visible = true,
    .named = true,
  },
  [sym_flow_node] = {
    .visible = true,
    .named = true,
  },
  [sym__flow_link] = {
    .visible = false,
    .named = true,
    .supertype = true,
  },
  [sym_flow_link_simplelink] = {
    .visible = true,
    .named = true,
  },
  [sym_flow_link_arrowtext] = {
    .visible = true,
    .named = true,
  },
  [sym_flow_link_middletext] = {
    .visible = true,
    .named = true,
  },
  [sym_flow_arrow_text] = {
    .visible = true,
    .named = true,
  },
  [sym_flow_vertex_id] = {
    .visible = true,
    .named = true,
  },
  [sym_flow_vertex] = {
    .visible = true,
    .named = true,
  },
  [sym__flow_vertex_kind] = {
    .visible = false,
    .named = true,
    .supertype = true,
  },
  [sym_flow_vertex_square] = {
    .visible = true,
    .named = true,
  },
  [sym_flow_vertex_circle] = {
    .visible = true,
    .named = true,
  },
  [sym_flow_vertex_ellipse] = {
    .visible = true,
    .named = true,
  },
  [sym_flow_vertex_stadium] = {
    .visible = true,
    .named = true,
  },
  [sym_flow_vertex_subroutine] = {
    .visible = true,
    .named = true,
  },
  [sym_flow_vertex_rect] = {
    .visible = true,
    .named = true,
  },
  [sym_flow_vertex_cylinder] = {
    .visible = true,
    .named = true,
  },
  [sym_flow_vertex_round] = {
    .visible = true,
    .named = true,
  },
  [sym_flow_vertex_diamond] = {
    .visible = true,
    .named = true,
  },
  [sym_flow_vertex_hexagon] = {
    .visible = true,
    .named = true,
  },
  [sym_flow_vertex_odd] = {
    .visible = true,
    .named = true,
  },
  [sym_flow_vertex_trapezoid] = {
    .visible = true,
    .named = true,
  },
  [sym_flow_vertex_inv_trapezoid] = {
    .visible = true,
    .named = true,
  },
  [sym_flow_vertex_leanright] = {
    .visible = true,
    .named = true,
  },
  [sym_flow_vertex_leanleft] = {
    .visible = true,
    .named = true,
  },
  [sym__flow_text] = {
    .visible = false,
    .named = true,
  },
  [sym_flow_stmt_subgraph] = {
    .visible = true,
    .named = true,
  },
  [sym_flow_stmt_subgraph_inner] = {
    .visible = true,
    .named = true,
  },
  [sym_flow_vertex_text] = {
    .visible = true,
    .named = true,
  },
  [sym_diagram_er] = {
    .visible = true,
    .named = true,
  },
  [sym__er_stmt] = {
    .visible = false,
    .named = true,
    .supertype = true,
  },
  [sym_er_stmt_entity] = {
    .visible = true,
    .named = true,
  },
  [sym_er_stmt_entity_relation] = {
    .visible = true,
    .named = true,
  },
  [sym_er_entity_name] = {
    .visible = true,
    .named = true,
  },
  [sym_er_relation] = {
    .visible = true,
    .named = true,
  },
  [sym__er_cardinarity] = {
    .visible = false,
    .named = true,
    .supertype = true,
  },
  [sym__er_reltype] = {
    .visible = false,
    .named = true,
    .supertype = true,
  },
  [sym_er_role] = {
    .visible = true,
    .named = true,
  },
  [sym_er_stmt_entity_block] = {
    .visible = true,
    .named = true,
  },
  [sym_er_stmt_entity_block_inner] = {
    .visible = true,
    .named = true,
  },
  [sym_er_attribute] = {
    .visible = true,
    .named = true,
  },
  [sym_er_attribute_type] = {
    .visible = true,
    .named = true,
  },
  [sym_er_attribute_name] = {
    .visible = true,
    .named = true,
  },
  [sym__er_attribute_key_type] = {
    .visible = false,
    .named = true,
    .supertype = true,
  },
  [sym_er_attribute_comment] = {
    .visible = true,
    .named = true,
  },
  [sym_diagram_mindmap] = {
    .visible = true,
    .named = true,
  },
  [sym_mmap_node] = {
    .visible = true,
    .named = true,
  },
  [sym_mmap_node_id] = {
    .visible = true,
    .named = true,
  },
  [sym_mmap_markdown] = {
    .visible = true,
    .named = true,
  },
  [sym_mmap_text] = {
    .visible = true,
    .named = true,
  },
  [sym_mmap_node_content] = {
    .visible = true,
    .named = true,
  },
  [sym_mmap_node_square] = {
    .visible = true,
    .named = true,
  },
  [sym_mmap_node_rounded] = {
    .visible = true,
    .named = true,
  },
  [sym_mmap_node_circle] = {
    .visible = true,
    .named = true,
  },
  [sym_mmap_node_cloud] = {
    .visible = true,
    .named = true,
  },
  [sym_mmap_node_bang] = {
    .visible = true,
    .named = true,
  },
  [sym_mmap_node_hexagon] = {
    .visible = true,
    .named = true,
  },
  [sym_mmap_class] = {
    .visible = true,
    .named = true,
  },
  [sym_mmap_icon] = {
    .visible = true,
    .named = true,
  },
  [sym_direction_tb] = {
    .visible = true,
    .named = true,
  },
  [sym_direction_bt] = {
    .visible = true,
    .named = true,
  },
  [sym_direction_rl] = {
    .visible = true,
    .named = true,
  },
  [sym_direction_lr] = {
    .visible = true,
    .named = true,
  },
  [sym__sequence_rest_text] = {
    .visible = false,
    .named = true,
  },
  [sym_dotted_open_arrow] = {
    .visible = true,
    .named = true,
  },
  [sym_solid_point] = {
    .visible = true,
    .named = true,
  },
  [sym_sequence_signal_plus_sign] = {
    .visible = true,
    .named = true,
  },
  [sym_sequence_signal_minus_sign] = {
    .visible = true,
    .named = true,
  },
  [sym_note_placement_left] = {
    .visible = true,
    .named = true,
  },
  [sym_note_placement_right] = {
    .visible = true,
    .named = true,
  },
  [sym_class_reltype_extension] = {
    .visible = true,
    .named = true,
  },
  [sym_class_reltype_composition] = {
    .visible = true,
    .named = true,
  },
  [sym_class_reltype_dependency] = {
    .visible = true,
    .named = true,
  },
  [sym_class_linetype_solid] = {
    .visible = true,
    .named = true,
  },
  [sym_class_linetype_dotted] = {
    .visible = true,
    .named = true,
  },
  [sym_class_label] = {
    .visible = true,
    .named = true,
  },
  [sym_state_name] = {
    .visible = true,
    .named = true,
  },
  [sym_state_arrow] = {
    .visible = true,
    .named = true,
  },
  [sym_state_description] = {
    .visible = true,
    .named = true,
  },
  [sym_state_hide_empty_description] = {
    .visible = true,
    .named = true,
  },
  [sym_state_division] = {
    .visible = true,
    .named = true,
  },
  [sym_state_annotation_fork] = {
    .visible = true,
    .named = true,
  },
  [sym_state_annotation_join] = {
    .visible = true,
    .named = true,
  },
  [sym_state_annotation_choice] = {
    .visible = true,
    .named = true,
  },
  [sym_gantt_meta_format] = {
    .visible = true,
    .named = true,
  },
  [sym_gantt_task_text] = {
    .visible = true,
    .named = true,
  },
  [sym_pie_showdata] = {
    .visible = true,
    .named = true,
  },
  [sym_pie_label] = {
    .visible = true,
    .named = true,
  },
  [sym_flowchart_direction_lr] = {
    .visible = true,
    .named = true,
  },
  [sym_flowchart_direction_rl] = {
    .visible = true,
    .named = true,
  },
  [sym_flowchart_direction_tb] = {
    .visible = true,
    .named = true,
  },
  [sym_flowchart_direction_bt] = {
    .visible = true,
    .named = true,
  },
  [sym_flow_text_literal] = {
    .visible = true,
    .named = true,
  },
  [sym_flow_link_arrow] = {
    .visible = true,
    .named = true,
  },
  [sym_flow_link_arrow_start] = {
    .visible = true,
    .named = true,
  },
  [sym__er_word] = {
    .visible = false,
    .named = true,
  },
  [sym_er_cardinarity_zero_or_one] = {
    .visible = true,
    .named = true,
  },
  [sym_er_cardinarity_zero_or_more] = {
    .visible = true,
    .named = true,
  },
  [sym_er_cardinarity_one_or_more] = {
    .visible = true,
    .named = true,
  },
  [sym_er_reltype_non_identifying] = {
    .visible = true,
    .named = true,
  },
  [sym_er_reltype_identifying] = {
    .visible = true,
    .named = true,
  },
  [sym_er_attribute_key_type_pk] = {
    .visible = true,
    .named = true,
  },
  [sym_er_attribute_key_type_fk] = {
    .visible = true,
    .named = true,
  },
  [aux_sym_source_file_repeat1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_diagram_sequence_repeat1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_sequence_actor_repeat1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_sequence_stmt_alt_repeat1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_sequence_stmt_par_repeat1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_diagram_class_repeat1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_class_name_body_repeat1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_class_stmt_class_repeat1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_class_method_line_repeat1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_diagram_state_repeat1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_state_composite_body_repeat1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_diagram_gantt_repeat1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_diagram_pie_repeat1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_diagram_flow_repeat1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_flow_stmt_vertice_repeat1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_flow_node_repeat1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_flow_arrow_text_repeat1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_flow_stmt_subgraph_inner_repeat1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_diagram_er_repeat1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_er_stmt_entity_block_inner_repeat1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_diagram_mindmap_repeat1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_gantt_task_text_repeat1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_flow_text_literal_repeat1] = {
    .visible = false,
    .named = false,
  },
  [alias_sym_annotation] = {
    .visible = true,
    .named = true,
  },
  [alias_sym_class_classifier_abstract] = {
    .visible = true,
    .named = true,
  },
  [alias_sym_class_style_name] = {
    .visible = true,
    .named = true,
  },
  [alias_sym_class_visibility_internal] = {
    .visible = true,
    .named = true,
  },
  [alias_sym_class_visibility_private] = {
    .visible = true,
    .named = true,
  },
  [alias_sym_class_visibility_public] = {
    .visible = true,
    .named = true,
  },
  [alias_sym_gantt_axis_format] = {
    .visible = true,
    .named = true,
  },
  [alias_sym_gantt_end_dates] = {
    .visible = true,
    .named = true,
  },
  [alias_sym_gantt_excludes] = {
    .visible = true,
    .named = true,
  },
  [alias_sym_gantt_includes] = {
    .visible = true,
    .named = true,
  },
  [alias_sym_gantt_section] = {
    .visible = true,
    .named = true,
  },
  [alias_sym_gantt_title] = {
    .visible = true,
    .named = true,
  },
  [alias_sym_gantt_today_marker] = {
    .visible = true,
    .named = true,
  },
  [alias_sym_gantt_top_axis] = {
    .visible = true,
    .named = true,
  },
  [alias_sym_note_content] = {
    .visible = true,
    .named = true,
  },
  [alias_sym_sequence_alias] = {
    .visible = true,
    .named = true,
  },
  [alias_sym_sequence_stmt_alt_branch] = {
    .visible = true,
    .named = true,
  },
  [alias_sym_sequence_stmt_loop_inner] = {
    .visible = true,
    .named = true,
  },
  [alias_sym_sequence_stmt_opt_inner] = {
    .visible = true,
    .named = true,
  },
  [alias_sym_sequence_stmt_rect_inner] = {
    .visible = true,
    .named = true,
  },
  [alias_sym_title] = {
    .visible = true,
    .named = true,
  },
};

static const TSSymbol ts_alias_sequences[PRODUCTION_ID_COUNT][MAX_ALIAS_SEQUENCE_LENGTH] = {
  [0] = {0},
  [1] = {
    [1] = alias_sym_gantt_title,
  },
  [2] = {
    [1] = alias_sym_gantt_end_dates,
  },
  [3] = {
    [1] = alias_sym_gantt_top_axis,
  },
  [4] = {
    [1] = alias_sym_gantt_axis_format,
  },
  [5] = {
    [1] = alias_sym_gantt_includes,
  },
  [6] = {
    [1] = alias_sym_gantt_excludes,
  },
  [7] = {
    [1] = alias_sym_gantt_today_marker,
  },
  [8] = {
    [1] = alias_sym_gantt_section,
  },
  [9] = {
    [2] = alias_sym_title,
  },
  [10] = {
    [2] = alias_sym_sequence_stmt_alt_branch,
  },
  [11] = {
    [0] = sym_state_description,
  },
  [12] = {
    [3] = alias_sym_sequence_alias,
  },
  [13] = {
    [3] = alias_sym_sequence_stmt_loop_inner,
  },
  [14] = {
    [3] = alias_sym_sequence_stmt_rect_inner,
  },
  [15] = {
    [3] = alias_sym_sequence_stmt_opt_inner,
  },
  [16] = {
    [3] = alias_sym_sequence_stmt_alt_branch,
  },
  [17] = {
    [3] = alias_sym_class_style_name,
  },
  [18] = {
    [1] = alias_sym_annotation,
  },
  [19] = {
    [0] = alias_sym_class_visibility_internal,
  },
  [20] = {
    [0] = alias_sym_class_visibility_public,
  },
  [21] = {
    [0] = alias_sym_class_visibility_private,
  },
  [22] = {
    [1] = alias_sym_class_classifier_abstract,
  },
  [23] = {
    [4] = alias_sym_note_content,
  },
  [24] = {
    [1] = alias_sym_sequence_stmt_alt_branch,
  },
  [25] = {
    [0] = alias_sym_class_visibility_internal,
    [2] = alias_sym_class_classifier_abstract,
  },
  [26] = {
    [0] = alias_sym_class_visibility_public,
    [2] = alias_sym_class_classifier_abstract,
  },
  [27] = {
    [0] = alias_sym_class_visibility_private,
    [2] = alias_sym_class_classifier_abstract,
  },
  [28] = {
    [2] = alias_sym_class_classifier_abstract,
  },
};

static const uint16_t ts_non_terminal_alias_map[] = {
  aux_sym__sequence_subdocument, 5,
    aux_sym__sequence_subdocument,
    alias_sym_sequence_stmt_alt_branch,
    alias_sym_sequence_stmt_loop_inner,
    alias_sym_sequence_stmt_opt_inner,
    alias_sym_sequence_stmt_rect_inner,
  sym__sequence_rest_text, 3,
    sym__sequence_rest_text,
    alias_sym_sequence_alias,
    alias_sym_title,
  sym_state_description, 2,
    sym_state_description,
    alias_sym_note_content,
  sym_gantt_meta_format, 9,
    sym_gantt_meta_format,
    alias_sym_gantt_axis_format,
    alias_sym_gantt_end_dates,
    alias_sym_gantt_excludes,
    alias_sym_gantt_includes,
    alias_sym_gantt_section,
    alias_sym_gantt_title,
    alias_sym_gantt_today_marker,
    alias_sym_gantt_top_axis,
  0,
};

static const TSStateId ts_primary_state_ids[STATE_COUNT] = {
  [0] = 0,
  [1] = 1,
  [2] = 2,
  [3] = 3,
  [4] = 3,
  [5] = 2,
  [6] = 6,
  [7] = 6,
  [8] = 2,
  [9] = 3,
  [10] = 6,
  [11] = 3,
  [12] = 2,
  [13] = 6,
  [14] = 14,
  [15] = 15,
  [16] = 16,
  [17] = 17,
  [18] = 17,
  [19] = 19,
  [20] = 20,
  [21] = 21,
  [22] = 22,
  [23] = 23,
  [24] = 21,
  [25] = 20,
  [26] = 26,
  [27] = 27,
  [28] = 22,
  [29] = 23,
  [30] = 21,
  [31] = 22,
  [32] = 23,
  [33] = 27,
  [34] = 26,
  [35] = 26,
  [36] = 27,
  [37] = 20,
  [38] = 38,
  [39] = 39,
  [40] = 27,
  [41] = 26,
  [42] = 20,
  [43] = 17,
  [44] = 23,
  [45] = 21,
  [46] = 22,
  [47] = 47,
  [48] = 48,
  [49] = 49,
  [50] = 50,
  [51] = 51,
  [52] = 51,
  [53] = 48,
  [54] = 48,
  [55] = 55,
  [56] = 47,
  [57] = 51,
  [58] = 48,
  [59] = 47,
  [60] = 51,
  [61] = 47,
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
  [86] = 85,
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
  [108] = 97,
  [109] = 96,
  [110] = 93,
  [111] = 92,
  [112] = 91,
  [113] = 113,
  [114] = 114,
  [115] = 115,
  [116] = 116,
  [117] = 117,
  [118] = 118,
  [119] = 119,
  [120] = 90,
  [121] = 89,
  [122] = 88,
  [123] = 87,
  [124] = 124,
  [125] = 125,
  [126] = 126,
  [127] = 105,
  [128] = 106,
  [129] = 115,
  [130] = 107,
  [131] = 124,
  [132] = 113,
  [133] = 94,
  [134] = 95,
  [135] = 114,
  [136] = 116,
  [137] = 119,
  [138] = 98,
  [139] = 126,
  [140] = 99,
  [141] = 100,
  [142] = 117,
  [143] = 101,
  [144] = 102,
  [145] = 118,
  [146] = 125,
  [147] = 103,
  [148] = 104,
  [149] = 92,
  [150] = 105,
  [151] = 90,
  [152] = 91,
  [153] = 93,
  [154] = 85,
  [155] = 96,
  [156] = 97,
  [157] = 104,
  [158] = 95,
  [159] = 98,
  [160] = 99,
  [161] = 93,
  [162] = 92,
  [163] = 91,
  [164] = 94,
  [165] = 88,
  [166] = 87,
  [167] = 124,
  [168] = 125,
  [169] = 126,
  [170] = 119,
  [171] = 118,
  [172] = 117,
  [173] = 97,
  [174] = 100,
  [175] = 116,
  [176] = 114,
  [177] = 113,
  [178] = 115,
  [179] = 90,
  [180] = 107,
  [181] = 103,
  [182] = 106,
  [183] = 94,
  [184] = 95,
  [185] = 119,
  [186] = 96,
  [187] = 106,
  [188] = 105,
  [189] = 126,
  [190] = 98,
  [191] = 101,
  [192] = 125,
  [193] = 102,
  [194] = 124,
  [195] = 87,
  [196] = 99,
  [197] = 88,
  [198] = 89,
  [199] = 102,
  [200] = 85,
  [201] = 118,
  [202] = 103,
  [203] = 117,
  [204] = 107,
  [205] = 100,
  [206] = 101,
  [207] = 104,
  [208] = 89,
  [209] = 113,
  [210] = 114,
  [211] = 116,
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
  [249] = 249,
  [250] = 250,
  [251] = 251,
  [252] = 252,
  [253] = 253,
  [254] = 254,
  [255] = 255,
  [256] = 256,
  [257] = 257,
  [258] = 258,
  [259] = 259,
  [260] = 260,
  [261] = 261,
  [262] = 262,
  [263] = 263,
  [264] = 264,
  [265] = 265,
  [266] = 266,
  [267] = 228,
  [268] = 268,
  [269] = 269,
  [270] = 270,
  [271] = 271,
  [272] = 272,
  [273] = 273,
  [274] = 274,
  [275] = 228,
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
  [307] = 301,
  [308] = 308,
  [309] = 309,
  [310] = 310,
  [311] = 301,
  [312] = 312,
  [313] = 301,
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
  [354] = 353,
  [355] = 355,
  [356] = 261,
  [357] = 259,
  [358] = 358,
  [359] = 353,
  [360] = 360,
  [361] = 361,
  [362] = 362,
  [363] = 363,
  [364] = 364,
  [365] = 365,
  [366] = 353,
  [367] = 367,
  [368] = 368,
  [369] = 369,
  [370] = 259,
  [371] = 371,
  [372] = 372,
  [373] = 373,
  [374] = 374,
  [375] = 373,
  [376] = 376,
  [377] = 377,
  [378] = 378,
  [379] = 379,
  [380] = 380,
  [381] = 381,
  [382] = 382,
  [383] = 261,
  [384] = 226,
  [385] = 373,
  [386] = 373,
  [387] = 387,
  [388] = 388,
  [389] = 389,
  [390] = 390,
  [391] = 391,
  [392] = 392,
  [393] = 258,
  [394] = 394,
  [395] = 395,
  [396] = 396,
  [397] = 397,
  [398] = 263,
  [399] = 399,
  [400] = 400,
  [401] = 401,
  [402] = 402,
  [403] = 403,
  [404] = 299,
  [405] = 405,
  [406] = 369,
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
  [417] = 315,
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
  [428] = 421,
  [429] = 429,
  [430] = 223,
  [431] = 431,
  [432] = 432,
  [433] = 433,
  [434] = 434,
  [435] = 435,
  [436] = 227,
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
  [449] = 447,
  [450] = 450,
  [451] = 451,
  [452] = 452,
  [453] = 453,
  [454] = 276,
  [455] = 455,
  [456] = 447,
  [457] = 457,
  [458] = 447,
  [459] = 257,
  [460] = 460,
  [461] = 229,
  [462] = 462,
  [463] = 463,
  [464] = 464,
  [465] = 465,
  [466] = 466,
  [467] = 467,
  [468] = 468,
  [469] = 469,
  [470] = 470,
  [471] = 288,
  [472] = 472,
  [473] = 473,
  [474] = 474,
  [475] = 475,
  [476] = 476,
  [477] = 477,
  [478] = 478,
  [479] = 478,
  [480] = 480,
  [481] = 481,
  [482] = 482,
  [483] = 483,
  [484] = 484,
  [485] = 485,
  [486] = 486,
  [487] = 487,
  [488] = 482,
  [489] = 483,
  [490] = 490,
  [491] = 491,
  [492] = 492,
  [493] = 482,
  [494] = 494,
  [495] = 485,
  [496] = 486,
  [497] = 487,
  [498] = 482,
  [499] = 499,
  [500] = 483,
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
  [512] = 485,
  [513] = 486,
  [514] = 514,
  [515] = 487,
  [516] = 516,
  [517] = 517,
  [518] = 518,
  [519] = 519,
  [520] = 520,
  [521] = 521,
  [522] = 522,
  [523] = 523,
  [524] = 524,
  [525] = 516,
  [526] = 518,
  [527] = 527,
  [528] = 519,
  [529] = 520,
  [530] = 521,
  [531] = 522,
  [532] = 523,
  [533] = 533,
  [534] = 534,
  [535] = 516,
  [536] = 518,
  [537] = 537,
  [538] = 519,
  [539] = 539,
  [540] = 520,
  [541] = 521,
  [542] = 522,
  [543] = 523,
  [544] = 490,
  [545] = 545,
  [546] = 534,
  [547] = 484,
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
  [560] = 490,
  [561] = 545,
  [562] = 534,
  [563] = 563,
  [564] = 564,
  [565] = 484,
  [566] = 566,
  [567] = 567,
  [568] = 551,
  [569] = 554,
  [570] = 570,
  [571] = 545,
  [572] = 557,
  [573] = 573,
  [574] = 558,
  [575] = 490,
  [576] = 545,
  [577] = 534,
  [578] = 578,
  [579] = 579,
  [580] = 580,
  [581] = 484,
  [582] = 582,
  [583] = 483,
  [584] = 584,
  [585] = 558,
  [586] = 551,
  [587] = 554,
  [588] = 588,
  [589] = 589,
  [590] = 557,
  [591] = 558,
  [592] = 592,
  [593] = 593,
  [594] = 594,
  [595] = 578,
  [596] = 589,
  [597] = 557,
  [598] = 524,
  [599] = 599,
  [600] = 554,
  [601] = 523,
  [602] = 522,
  [603] = 426,
  [604] = 604,
  [605] = 521,
  [606] = 520,
  [607] = 519,
  [608] = 592,
  [609] = 593,
  [610] = 594,
  [611] = 611,
  [612] = 478,
  [613] = 613,
  [614] = 578,
  [615] = 589,
  [616] = 616,
  [617] = 518,
  [618] = 618,
  [619] = 524,
  [620] = 516,
  [621] = 487,
  [622] = 622,
  [623] = 551,
  [624] = 624,
  [625] = 625,
  [626] = 592,
  [627] = 593,
  [628] = 628,
  [629] = 594,
  [630] = 478,
  [631] = 613,
  [632] = 578,
  [633] = 589,
  [634] = 524,
  [635] = 635,
  [636] = 636,
  [637] = 592,
  [638] = 593,
  [639] = 594,
  [640] = 613,
  [641] = 641,
  [642] = 582,
  [643] = 643,
  [644] = 644,
  [645] = 645,
  [646] = 646,
  [647] = 647,
  [648] = 648,
  [649] = 584,
  [650] = 613,
  [651] = 486,
  [652] = 652,
  [653] = 485,
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
  [675] = 669,
  [676] = 676,
  [677] = 677,
  [678] = 678,
  [679] = 679,
  [680] = 680,
  [681] = 681,
  [682] = 669,
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
  [694] = 679,
  [695] = 695,
  [696] = 696,
  [697] = 659,
  [698] = 698,
  [699] = 663,
  [700] = 700,
  [701] = 701,
  [702] = 702,
  [703] = 703,
  [704] = 667,
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
  [716] = 663,
  [717] = 717,
  [718] = 703,
  [719] = 719,
  [720] = 667,
  [721] = 721,
  [722] = 722,
  [723] = 723,
  [724] = 663,
  [725] = 703,
  [726] = 679,
  [727] = 667,
  [728] = 464,
  [729] = 659,
  [730] = 730,
  [731] = 731,
  [732] = 732,
  [733] = 703,
  [734] = 679,
  [735] = 735,
  [736] = 736,
  [737] = 659,
  [738] = 738,
  [739] = 739,
  [740] = 669,
  [741] = 741,
  [742] = 450,
  [743] = 743,
  [744] = 467,
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
  [764] = 748,
  [765] = 765,
  [766] = 766,
  [767] = 767,
  [768] = 768,
  [769] = 769,
  [770] = 770,
  [771] = 771,
  [772] = 755,
  [773] = 773,
  [774] = 758,
  [775] = 762,
  [776] = 776,
  [777] = 761,
  [778] = 762,
  [779] = 779,
  [780] = 763,
  [781] = 748,
  [782] = 765,
  [783] = 755,
  [784] = 784,
  [785] = 785,
  [786] = 786,
  [787] = 787,
  [788] = 788,
  [789] = 789,
  [790] = 766,
  [791] = 791,
  [792] = 792,
  [793] = 793,
  [794] = 794,
  [795] = 795,
  [796] = 796,
  [797] = 797,
  [798] = 798,
  [799] = 765,
  [800] = 800,
  [801] = 801,
  [802] = 802,
  [803] = 803,
  [804] = 804,
  [805] = 805,
  [806] = 806,
  [807] = 763,
  [808] = 808,
  [809] = 809,
  [810] = 786,
  [811] = 787,
  [812] = 788,
  [813] = 789,
  [814] = 763,
  [815] = 791,
  [816] = 792,
  [817] = 793,
  [818] = 818,
  [819] = 819,
  [820] = 820,
  [821] = 762,
  [822] = 756,
  [823] = 761,
  [824] = 824,
  [825] = 825,
  [826] = 826,
  [827] = 758,
  [828] = 828,
  [829] = 829,
  [830] = 830,
  [831] = 786,
  [832] = 787,
  [833] = 788,
  [834] = 789,
  [835] = 767,
  [836] = 791,
  [837] = 792,
  [838] = 793,
  [839] = 839,
  [840] = 840,
  [841] = 841,
  [842] = 842,
  [843] = 755,
  [844] = 844,
  [845] = 845,
  [846] = 846,
  [847] = 847,
  [848] = 848,
  [849] = 849,
  [850] = 767,
  [851] = 851,
  [852] = 773,
  [853] = 766,
  [854] = 854,
  [855] = 855,
  [856] = 856,
  [857] = 761,
  [858] = 858,
  [859] = 859,
  [860] = 860,
  [861] = 779,
  [862] = 795,
  [863] = 839,
  [864] = 776,
  [865] = 865,
  [866] = 808,
  [867] = 804,
  [868] = 765,
  [869] = 773,
  [870] = 870,
  [871] = 871,
  [872] = 805,
  [873] = 766,
  [874] = 858,
  [875] = 859,
  [876] = 767,
  [877] = 779,
  [878] = 795,
  [879] = 839,
  [880] = 776,
  [881] = 881,
  [882] = 882,
  [883] = 722,
  [884] = 723,
  [885] = 773,
  [886] = 886,
  [887] = 887,
  [888] = 888,
  [889] = 786,
  [890] = 858,
  [891] = 859,
  [892] = 787,
  [893] = 779,
  [894] = 795,
  [895] = 839,
  [896] = 776,
  [897] = 800,
  [898] = 828,
  [899] = 788,
  [900] = 789,
  [901] = 95,
  [902] = 902,
  [903] = 748,
  [904] = 904,
  [905] = 905,
  [906] = 791,
  [907] = 94,
  [908] = 908,
  [909] = 792,
  [910] = 910,
  [911] = 793,
  [912] = 912,
  [913] = 913,
  [914] = 914,
  [915] = 758,
  [916] = 916,
  [917] = 858,
  [918] = 918,
  [919] = 919,
  [920] = 859,
};

static inline bool aux_sym_flow_text_literal_token1_character_set_1(int32_t c) {
  return (c < '-'
    ? (c < ' '
      ? (c < '\t'
        ? c == 0
        : c <= '\r')
      : (c <= ' ' || c == ')'))
    : (c <= '-' || (c < '\\'
      ? (c < ';'
        ? c == '/'
        : c <= ';')
      : (c <= ']' || (c >= '|' && c <= '}')))));
}

static inline bool sym_flow_text_quoted_character_set_1(int32_t c) {
  return (c < '-'
    ? (c < ' '
      ? (c < 11
        ? c == '\t'
        : c <= '\r')
      : (c <= ' ' || c == ')'))
    : (c <= '-' || (c < '\\'
      ? (c < ';'
        ? c == '/'
        : c <= ';')
      : (c <= ']' || (c >= '|' && c <= '}')))));
}

static bool ts_lex(TSLexer *lexer, TSStateId state) {
  START_LEXER();
  eof = lexer->eof(lexer);
  switch (state) {
    case 0:
      if (eof) ADVANCE(442);
      if (lookahead == '\n') ADVANCE(581);
      if (lookahead == '"') ADVANCE(115);
      if (lookahead == '#') ADVANCE(492);
      if (lookahead == '$') ADVANCE(499);
      if (lookahead == '%') ADVANCE(29);
      if (lookahead == '&') ADVANCE(519);
      if (lookahead == '(') ADVANCE(495);
      if (lookahead == ')') ADVANCE(497);
      if (lookahead == '*') ADVANCE(498);
      if (lookahead == '+') ADVANCE(489);
      if (lookahead == ',') ADVANCE(457);
      if (lookahead == '-') ADVANCE(491);
      if (lookahead == '.') ADVANCE(56);
      if (lookahead == '/') ADVANCE(104);
      if (lookahead == ':') ADVANCE(451);
      if (lookahead == ';') ADVANCE(516);
      if (lookahead == '<') ADVANCE(729);
      if (lookahead == '=') ADVANCE(82);
      if (lookahead == '>') ADVANCE(540);
      if (lookahead == 'C') ADVANCE(286);
      if (lookahead == 'E') ADVANCE(283);
      if (lookahead == 'S') ADVANCE(204);
      if (lookahead == '[') ADVANCE(523);
      if (lookahead == '\\') ADVANCE(105);
      if (lookahead == ']') ADVANCE(525);
      if (lookahead == '^') ADVANCE(919);
      if (lookahead == '`') ADVANCE(22);
      if (lookahead == 'c') ADVANCE(99);
      if (lookahead == 'e') ADVANCE(100);
      if (lookahead == 'm') ADVANCE(125);
      if (lookahead == 'o') ADVANCE(725);
      if (lookahead == 's') ADVANCE(137);
      if (lookahead == 'v') ADVANCE(917);
      if (lookahead == 'x') ADVANCE(81);
      if (lookahead == '{') ADVANCE(485);
      if (lookahead == '|') ADVANCE(521);
      if (lookahead == '}') ADVANCE(487);
      if (lookahead == '~') ADVANCE(474);
      if (lookahead == '\t' ||
          lookahead == ' ') ADVANCE(569);
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(176);
      if (lookahead == 'B' ||
          lookahead == 'b') ADVANCE(340);
      if (lookahead == 'D' ||
          lookahead == 'd') ADVANCE(149);
      if (lookahead == 'F' ||
          lookahead == 'f') ADVANCE(277);
      if (lookahead == 'G' ||
          lookahead == 'g') ADVANCE(145);
      if (lookahead == 'H' ||
          lookahead == 'h') ADVANCE(258);
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(313);
      if (lookahead == 'L' ||
          lookahead == 'l') ADVANCE(206);
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(322);
      if (lookahead == 'P' ||
          lookahead == 'p') ADVANCE(147);
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(216);
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(172);
      if ((11 <= lookahead && lookahead <= '\r')) ADVANCE(70);
      END_STATE();
    case 1:
      if (lookahead == '\t') ADVANCE(572);
      if (lookahead == ' ') ADVANCE(560);
      if (lookahead == '%') ADVANCE(30);
      if (lookahead == '-' ||
          ('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(561);
      END_STATE();
    case 2:
      if (lookahead == '\t') ADVANCE(568);
      if (lookahead == '\n') ADVANCE(582);
      if (lookahead == ' ') ADVANCE(572);
      if (lookahead == '%') ADVANCE(601);
      if (lookahead == ',') ADVANCE(457);
      if (lookahead == '-') ADVANCE(53);
      if (lookahead == ':') ADVANCE(450);
      if (lookahead != 0 &&
          lookahead != '+' &&
          lookahead != ';' &&
          lookahead != '>') ADVANCE(687);
      END_STATE();
    case 3:
      if (lookahead == '\t') ADVANCE(568);
      if (lookahead == '\n') ADVANCE(582);
      if (lookahead == ' ') ADVANCE(572);
      if (lookahead == '%') ADVANCE(601);
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(612);
      if (lookahead == 'D' ||
          lookahead == 'd') ADVANCE(624);
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(642);
      if (lookahead == 'L' ||
          lookahead == 'l') ADVANCE(633);
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(654);
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(658);
      if (lookahead == 'P' ||
          lookahead == 'p') ADVANCE(603);
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(625);
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(626);
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(638);
      if (lookahead != 0 &&
          (lookahead < '+' || '-' < lookahead) &&
          lookahead != ':' &&
          lookahead != ';' &&
          lookahead != '>') ADVANCE(687);
      END_STATE();
    case 4:
      if (lookahead == '\t') ADVANCE(568);
      if (lookahead == '\n') ADVANCE(582);
      if (lookahead == ' ') ADVANCE(572);
      if (lookahead == '%') ADVANCE(601);
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(612);
      if (lookahead == 'D' ||
          lookahead == 'd') ADVANCE(624);
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(646);
      if (lookahead == 'L' ||
          lookahead == 'l') ADVANCE(633);
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(654);
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(658);
      if (lookahead == 'P' ||
          lookahead == 'p') ADVANCE(603);
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(625);
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(626);
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(638);
      if (lookahead != 0 &&
          (lookahead < '+' || '-' < lookahead) &&
          lookahead != ':' &&
          lookahead != ';' &&
          lookahead != '>') ADVANCE(687);
      END_STATE();
    case 5:
      if (lookahead == '\t') ADVANCE(568);
      if (lookahead == '\n') ADVANCE(582);
      if (lookahead == ' ') ADVANCE(572);
      if (lookahead == '%') ADVANCE(601);
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(611);
      if (lookahead == 'D' ||
          lookahead == 'd') ADVANCE(624);
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(646);
      if (lookahead == 'L' ||
          lookahead == 'l') ADVANCE(633);
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(654);
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(658);
      if (lookahead == 'P' ||
          lookahead == 'p') ADVANCE(603);
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(625);
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(626);
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(638);
      if (lookahead != 0 &&
          (lookahead < '+' || '-' < lookahead) &&
          lookahead != ':' &&
          lookahead != ';' &&
          lookahead != '>') ADVANCE(687);
      END_STATE();
    case 6:
      if (lookahead == '\t') ADVANCE(568);
      if (lookahead == '\n') ADVANCE(582);
      if (lookahead == ' ') ADVANCE(572);
      if (lookahead == '%') ADVANCE(601);
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(666);
      if (lookahead != 0 &&
          (lookahead < '+' || '-' < lookahead) &&
          lookahead != ':' &&
          lookahead != ';' &&
          lookahead != '>') ADVANCE(687);
      END_STATE();
    case 7:
      if (lookahead == '\t') ADVANCE(568);
      if (lookahead == ' ') ADVANCE(572);
      if (lookahead == '%') ADVANCE(601);
      if (lookahead == '+') ADVANCE(489);
      if (lookahead == '-') ADVANCE(490);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ',' &&
          lookahead != ':' &&
          lookahead != ';' &&
          lookahead != '>') ADVANCE(687);
      END_STATE();
    case 8:
      if (lookahead == '\n') ADVANCE(585);
      if (lookahead == '{') ADVANCE(443);
      if (lookahead != 0) ADVANCE(10);
      END_STATE();
    case 9:
      if (lookahead == '\n') ADVANCE(585);
      if (lookahead != 0 &&
          lookahead != '{') ADVANCE(10);
      END_STATE();
    case 10:
      if (lookahead == '\n') ADVANCE(584);
      if (lookahead != 0) ADVANCE(10);
      END_STATE();
    case 11:
      if (lookahead == '\n') ADVANCE(582);
      if (lookahead == '#') ADVANCE(492);
      if (lookahead == '%') ADVANCE(30);
      if (lookahead == '(') ADVANCE(493);
      if (lookahead == ')') ADVANCE(496);
      if (lookahead == '+') ADVANCE(489);
      if (lookahead == '-') ADVANCE(490);
      if (lookahead == '<') ADVANCE(79);
      if (lookahead == '}') ADVANCE(486);
      if (lookahead == '~') ADVANCE(475);
      if (lookahead == '\t' ||
          lookahead == ' ') ADVANCE(572);
      if (lookahead == '!' ||
          ('0' <= lookahead && lookahead <= '9') ||
          lookahead == '?' ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(721);
      END_STATE();
    case 12:
      if (lookahead == '\n') ADVANCE(582);
      if (lookahead == '%') ADVANCE(743);
      if (lookahead == '-') ADVANCE(745);
      if (lookahead == '[') ADVANCE(744);
      if (lookahead == '}') ADVANCE(488);
      if (lookahead == '\t' ||
          lookahead == ' ') ADVANCE(573);
      if (lookahead == 'D' ||
          lookahead == 'd') ADVANCE(760);
      if (lookahead == 'H' ||
          lookahead == 'h') ADVANCE(761);
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(771);
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(779);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(786);
      if (lookahead != 0 &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(787);
      END_STATE();
    case 13:
      if (lookahead == '\n') ADVANCE(582);
      if (lookahead == '%') ADVANCE(596);
      if (lookahead == '\t' ||
          lookahead == ' ') ADVANCE(580);
      if (lookahead != 0 &&
          lookahead != '#' &&
          lookahead != ';') ADVANCE(597);
      END_STATE();
    case 14:
      if (lookahead == '\n') ADVANCE(582);
      if (lookahead == '%') ADVANCE(30);
      if (lookahead == '-') ADVANCE(62);
      if (lookahead == ':') ADVANCE(450);
      if (lookahead == '[') ADVANCE(54);
      if (lookahead == '}') ADVANCE(486);
      if (lookahead == '\t' ||
          lookahead == ' ') ADVANCE(572);
      if (lookahead == 'D' ||
          lookahead == 'd') ADVANCE(801);
      if (lookahead == 'H' ||
          lookahead == 'h') ADVANCE(802);
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(806);
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(809);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(813);
      END_STATE();
    case 15:
      if (lookahead == ' ') ADVANCE(455);
      END_STATE();
    case 16:
      if (lookahead == ' ') ADVANCE(504);
      if (lookahead == 'D') ADVANCE(274);
      END_STATE();
    case 17:
      if (lookahead == ' ') ADVANCE(318);
      END_STATE();
    case 18:
      if (lookahead == ' ') ADVANCE(320);
      END_STATE();
    case 19:
      if (lookahead == ' ') ADVANCE(218);
      END_STATE();
    case 20:
      if (lookahead == ' ') ADVANCE(200);
      END_STATE();
    case 21:
      if (lookahead == '"') ADVANCE(115);
      if (lookahead == '%') ADVANCE(34);
      if (lookahead == '\t' ||
          lookahead == ' ') ADVANCE(574);
      if (lookahead != 0) ADVANCE(424);
      END_STATE();
    case 22:
      if (lookahead == '"') ADVANCE(587);
      END_STATE();
    case 23:
      if (lookahead == '"') ADVANCE(909);
      if (lookahead != 0) ADVANCE(23);
      END_STATE();
    case 24:
      if (lookahead == '"') ADVANCE(422);
      if (lookahead == '%') ADVANCE(30);
      if (lookahead == '*') ADVANCE(498);
      if (lookahead == '-') ADVANCE(60);
      if (lookahead == '.') ADVANCE(67);
      if (lookahead == ':') ADVANCE(450);
      if (lookahead == '<') ADVANCE(730);
      if (lookahead == '>') ADVANCE(540);
      if (lookahead == '`') ADVANCE(423);
      if (lookahead == 'o') ADVANCE(726);
      if (lookahead == '|') ADVANCE(85);
      if (lookahead == '~') ADVANCE(474);
      if (lookahead == '\t' ||
          lookahead == ' ') ADVANCE(572);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          ('_' <= lookahead && lookahead <= 'z')) ADVANCE(702);
      END_STATE();
    case 25:
      if (lookahead == '"') ADVANCE(422);
      if (lookahead == '%') ADVANCE(30);
      if (lookahead == '[') ADVANCE(54);
      if (lookahead == '\t' ||
          lookahead == ' ') ADVANCE(572);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(813);
      END_STATE();
    case 26:
      if (lookahead == '"') ADVANCE(723);
      if (lookahead != 0) ADVANCE(26);
      END_STATE();
    case 27:
      if (lookahead == '"') ADVANCE(922);
      if (lookahead == '%') ADVANCE(923);
      if (lookahead == '\t' ||
          lookahead == ' ') ADVANCE(572);
      if (lookahead != 0 &&
          (lookahead < '\n' || '\r' < lookahead) &&
          lookahead != ')' &&
          lookahead != '-' &&
          lookahead != '/' &&
          lookahead != ';' &&
          lookahead != '\\' &&
          lookahead != ']' &&
          lookahead != '|' &&
          lookahead != '}') ADVANCE(924);
      END_STATE();
    case 28:
      if (lookahead == '"') ADVANCE(925);
      if (lookahead != 0 &&
          lookahead != '\n') ADVANCE(28);
      END_STATE();
    case 29:
      if (lookahead == '%') ADVANCE(8);
      END_STATE();
    case 30:
      if (lookahead == '%') ADVANCE(9);
      END_STATE();
    case 31:
      if (lookahead == '%') ADVANCE(827);
      if (lookahead == ':') ADVANCE(450);
      if (lookahead == '\t' ||
          lookahead == ' ') ADVANCE(572);
      if (lookahead != 0 &&
          (lookahead < '\n' || '\r' < lookahead) &&
          lookahead != '#' &&
          lookahead != ';') ADVANCE(894);
      END_STATE();
    case 32:
      if (lookahead == '%') ADVANCE(923);
      if (lookahead == ')') ADVANCE(48);
      if (lookahead == '}') ADVANCE(140);
      if (lookahead == '\t' ||
          lookahead == ' ') ADVANCE(572);
      if (lookahead != 0 &&
          (lookahead < '\n' || '\r' < lookahead) &&
          lookahead != '-' &&
          lookahead != '/' &&
          lookahead != ';' &&
          lookahead != '\\' &&
          lookahead != ']' &&
          lookahead != '|') ADVANCE(924);
      END_STATE();
    case 33:
      if (lookahead == '%') ADVANCE(923);
      if (lookahead == ')') ADVANCE(496);
      if (lookahead == '-') ADVANCE(51);
      if (lookahead == '/') ADVANCE(104);
      if (lookahead == '\\') ADVANCE(105);
      if (lookahead == ']') ADVANCE(525);
      if (lookahead == '|') ADVANCE(106);
      if (lookahead == '}') ADVANCE(486);
      if (lookahead == '\t' ||
          lookahead == ' ') ADVANCE(572);
      if (lookahead != 0 &&
          (lookahead < '\n' || '\r' < lookahead) &&
          lookahead != ';') ADVANCE(924);
      END_STATE();
    case 34:
      if (lookahead == '%') ADVANCE(557);
      if (lookahead != 0 &&
          lookahead != '`') ADVANCE(556);
      END_STATE();
    case 35:
      if (lookahead == '%') ADVANCE(948);
      if (lookahead == '\t' ||
          lookahead == ' ') ADVANCE(576);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != '\r' &&
          lookahead != '(' &&
          lookahead != ')' &&
          lookahead != '[' &&
          lookahead != ']' &&
          lookahead != '{' &&
          lookahead != '}') ADVANCE(949);
      END_STATE();
    case 36:
      if (lookahead == '%') ADVANCE(565);
      if (lookahead == '\t' ||
          lookahead == ' ') ADVANCE(566);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != '\r' &&
          lookahead != ')') ADVANCE(567);
      END_STATE();
    case 37:
      if (lookahead == '%') ADVANCE(444);
      END_STATE();
    case 38:
      if (lookahead == '%') ADVANCE(478);
      if (lookahead == '\t' ||
          lookahead == ' ') ADVANCE(479);
      if (lookahead != 0 &&
          lookahead != '~') ADVANCE(480);
      END_STATE();
    case 39:
      if (lookahead == '%') ADVANCE(897);
      if (lookahead == '\t' ||
          lookahead == ' ') ADVANCE(578);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != '#' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(898);
      END_STATE();
    case 40:
      if (lookahead == '%') ADVANCE(553);
      if (lookahead == '\t' ||
          lookahead == ' ') ADVANCE(554);
      if (lookahead != 0 &&
          lookahead != '"' &&
          lookahead != '`') ADVANCE(555);
      END_STATE();
    case 41:
      if (lookahead == '%') ADVANCE(817);
      if (lookahead == '\t' ||
          lookahead == ' ') ADVANCE(572);
      if (lookahead != 0 &&
          (lookahead < '\n' || '\r' < lookahead) &&
          lookahead != '-' &&
          lookahead != ':' &&
          lookahead != '{') ADVANCE(818);
      END_STATE();
    case 42:
      if (lookahead == '%') ADVANCE(30);
      if (lookahead == '-') ADVANCE(63);
      if (lookahead == '.') ADVANCE(58);
      if (lookahead == '<') ADVANCE(66);
      if (lookahead == '=') ADVANCE(84);
      if (lookahead == '\t' ||
          lookahead == ' ') ADVANCE(572);
      if (lookahead == 'o' ||
          lookahead == 'x') ADVANCE(703);
      if (lookahead == '!' ||
          ('0' <= lookahead && lookahead <= '9') ||
          lookahead == '?' ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z') ||
          lookahead == '~') ADVANCE(721);
      END_STATE();
    case 43:
      if (lookahead == '%') ADVANCE(30);
      if (lookahead == 'e') ADVANCE(709);
      if (lookahead == 's') ADVANCE(712);
      if (lookahead == '\t' ||
          lookahead == ' ') ADVANCE(572);
      if (lookahead == 'D' ||
          lookahead == 'd') ADVANCE(715);
      if (lookahead == '!' ||
          ('0' <= lookahead && lookahead <= '9') ||
          lookahead == '?' ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z') ||
          lookahead == '~') ADVANCE(721);
      END_STATE();
    case 44:
      if (lookahead == '%') ADVANCE(30);
      if (lookahead == '}') ADVANCE(46);
      if (lookahead == '\t' ||
          lookahead == ' ') ADVANCE(571);
      if (lookahead != 0) ADVANCE(141);
      END_STATE();
    case 45:
      if (lookahead == '%') ADVANCE(30);
      if (lookahead == '\t' ||
          lookahead == ' ') ADVANCE(577);
      if (('\n' <= lookahead && lookahead <= '\r')) ADVANCE(420);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(910);
      END_STATE();
    case 46:
      if (lookahead == '%') ADVANCE(37);
      if (lookahead == '}') ADVANCE(46);
      if (lookahead != 0) ADVANCE(141);
      END_STATE();
    case 47:
      if (lookahead == '(') ADVANCE(562);
      END_STATE();
    case 48:
      if (lookahead == ')') ADVANCE(527);
      if (lookahead == ']') ADVANCE(537);
      END_STATE();
    case 49:
      if (lookahead == ')') ADVANCE(531);
      if (lookahead == ']') ADVANCE(533);
      END_STATE();
    case 50:
      if (lookahead == ')') ADVANCE(695);
      if (lookahead == '>') ADVANCE(692);
      if (lookahead == 'x') ADVANCE(694);
      END_STATE();
    case 51:
      if (lookahead == ')') ADVANCE(529);
      END_STATE();
    case 52:
      if (lookahead == ')') ADVANCE(529);
      if (lookahead == '-') ADVANCE(931);
      if (lookahead == '.') ADVANCE(933);
      END_STATE();
    case 53:
      if (lookahead == ')') ADVANCE(529);
      if (lookahead == '-') ADVANCE(50);
      if (lookahead == '>') ADVANCE(690);
      if (lookahead == 'x') ADVANCE(693);
      END_STATE();
    case 54:
      if (lookahead == '*') ADVANCE(108);
      END_STATE();
    case 55:
      if (lookahead == '*') ADVANCE(108);
      if (lookahead == '[') ADVANCE(191);
      END_STATE();
    case 56:
      if (lookahead == '-') ADVANCE(942);
      if (lookahead == '.') ADVANCE(735);
      END_STATE();
    case 57:
      if (lookahead == '-') ADVANCE(931);
      if (lookahead == '.') ADVANCE(933);
      END_STATE();
    case 58:
      if (lookahead == '-') ADVANCE(419);
      if (lookahead == '.') ADVANCE(58);
      END_STATE();
    case 59:
      if (lookahead == '-') ADVANCE(927);
      if (lookahead == '>' ||
          lookahead == 'o' ||
          lookahead == 'x') ADVANCE(926);
      END_STATE();
    case 60:
      if (lookahead == '-') ADVANCE(731);
      END_STATE();
    case 61:
      if (lookahead == '-') ADVANCE(731);
      if (lookahead == '.') ADVANCE(943);
      END_STATE();
    case 62:
      if (lookahead == '-') ADVANCE(733);
      END_STATE();
    case 63:
      if (lookahead == '-') ADVANCE(59);
      if (lookahead == '.') ADVANCE(58);
      END_STATE();
    case 64:
      if (lookahead == '-') ADVANCE(57);
      if (lookahead == '.') ADVANCE(58);
      if (lookahead == '=') ADVANCE(82);
      END_STATE();
    case 65:
      if (lookahead == '-') ADVANCE(57);
      if (lookahead == '.') ADVANCE(58);
      if (lookahead == '=') ADVANCE(82);
      if (lookahead == '{') ADVANCE(938);
      if (lookahead == '|') ADVANCE(936);
      if (lookahead == 'V' ||
          lookahead == 'v') ADVANCE(228);
      END_STATE();
    case 66:
      if (lookahead == '-') ADVANCE(63);
      if (lookahead == '.') ADVANCE(58);
      if (lookahead == '=') ADVANCE(84);
      END_STATE();
    case 67:
      if (lookahead == '.') ADVANCE(735);
      END_STATE();
    case 68:
      if (lookahead == '2') ADVANCE(472);
      END_STATE();
    case 69:
      if (lookahead == '2') ADVANCE(503);
      END_STATE();
    case 70:
      if (lookahead == ':') ADVANCE(75);
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') ADVANCE(70);
      END_STATE();
    case 71:
      if (lookahead == ':') ADVANCE(483);
      END_STATE();
    case 72:
      if (lookahead == ':') ADVANCE(483);
      if (lookahead == 'i') ADVANCE(120);
      END_STATE();
    case 73:
      if (lookahead == ':') ADVANCE(559);
      END_STATE();
    case 74:
      if (lookahead == ':') ADVANCE(559);
      if (lookahead == 'i') ADVANCE(120);
      END_STATE();
    case 75:
      if (lookahead == ':') ADVANCE(74);
      END_STATE();
    case 76:
      if (lookahead == ':') ADVANCE(71);
      END_STATE();
    case 77:
      if (lookahead == ':') ADVANCE(73);
      END_STATE();
    case 78:
      if (lookahead == ':') ADVANCE(77);
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') ADVANCE(78);
      END_STATE();
    case 79:
      if (lookahead == '<') ADVANCE(500);
      END_STATE();
    case 80:
      if (lookahead == '<') ADVANCE(179);
      END_STATE();
    case 81:
      if (lookahead == '=') ADVANCE(82);
      END_STATE();
    case 82:
      if (lookahead == '=') ADVANCE(932);
      END_STATE();
    case 83:
      if (lookahead == '=') ADVANCE(929);
      if (lookahead == '>' ||
          lookahead == 'o' ||
          lookahead == 'x') ADVANCE(928);
      END_STATE();
    case 84:
      if (lookahead == '=') ADVANCE(83);
      END_STATE();
    case 85:
      if (lookahead == '>') ADVANCE(728);
      END_STATE();
    case 86:
      if (lookahead == '>') ADVANCE(501);
      END_STATE();
    case 87:
      if (lookahead == '>') ADVANCE(819);
      END_STATE();
    case 88:
      if (lookahead == '>') ADVANCE(821);
      END_STATE();
    case 89:
      if (lookahead == '>') ADVANCE(823);
      END_STATE();
    case 90:
      if (lookahead == '>') ADVANCE(87);
      END_STATE();
    case 91:
      if (lookahead == '>') ADVANCE(88);
      END_STATE();
    case 92:
      if (lookahead == '>') ADVANCE(89);
      END_STATE();
    case 93:
      if (lookahead == 'A') ADVANCE(372);
      if (lookahead == 'a') ADVANCE(102);
      END_STATE();
    case 94:
      if (lookahead == 'D') ADVANCE(464);
      if (lookahead == 'd') ADVANCE(464);
      END_STATE();
    case 95:
      if (lookahead == 'D') ADVANCE(273);
      END_STATE();
    case 96:
      if (lookahead == 'D') ADVANCE(274);
      END_STATE();
    case 97:
      if (lookahead == 'D') ADVANCE(275);
      END_STATE();
    case 98:
      if (lookahead == 'D') ADVANCE(275);
      if (lookahead == '_') ADVANCE(164);
      END_STATE();
    case 99:
      if (lookahead == 'L') ADVANCE(146);
      if (lookahead == 'l') ADVANCE(93);
      END_STATE();
    case 100:
      if (lookahead == 'N') ADVANCE(193);
      if (lookahead == 'n') ADVANCE(94);
      if (lookahead == 'L' ||
          lookahead == 'l') ADVANCE(371);
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(195);
      if (lookahead == 'X' ||
          lookahead == 'x') ADVANCE(177);
      END_STATE();
    case 101:
      if (lookahead == 'S') ADVANCE(95);
      if (lookahead == 's') ADVANCE(481);
      END_STATE();
    case 102:
      if (lookahead == 'S') ADVANCE(363);
      if (lookahead == 's') ADVANCE(101);
      END_STATE();
    case 103:
      if (lookahead == '[') ADVANCE(191);
      END_STATE();
    case 104:
      if (lookahead == ']') ADVANCE(544);
      END_STATE();
    case 105:
      if (lookahead == ']') ADVANCE(542);
      END_STATE();
    case 106:
      if (lookahead == ']') ADVANCE(535);
      END_STATE();
    case 107:
      if (lookahead == ']') ADVANCE(535);
      if (lookahead == 'o') ADVANCE(935);
      if (lookahead == '{') ADVANCE(940);
      if (lookahead == '|') ADVANCE(941);
      END_STATE();
    case 108:
      if (lookahead == ']') ADVANCE(788);
      END_STATE();
    case 109:
      if (lookahead == ']') ADVANCE(820);
      END_STATE();
    case 110:
      if (lookahead == ']') ADVANCE(822);
      END_STATE();
    case 111:
      if (lookahead == ']') ADVANCE(824);
      END_STATE();
    case 112:
      if (lookahead == ']') ADVANCE(109);
      END_STATE();
    case 113:
      if (lookahead == ']') ADVANCE(110);
      END_STATE();
    case 114:
      if (lookahead == ']') ADVANCE(111);
      END_STATE();
    case 115:
      if (lookahead == '`') ADVANCE(586);
      END_STATE();
    case 116:
      if (lookahead == '`') ADVANCE(722);
      if (lookahead != 0) ADVANCE(116);
      END_STATE();
    case 117:
      if (lookahead == 'a') ADVANCE(134);
      END_STATE();
    case 118:
      if (lookahead == 'a') ADVANCE(135);
      END_STATE();
    case 119:
      if (lookahead == 'b') ADVANCE(123);
      END_STATE();
    case 120:
      if (lookahead == 'c') ADVANCE(133);
      END_STATE();
    case 121:
      if (lookahead == 'd') ADVANCE(126);
      END_STATE();
    case 122:
      if (lookahead == 'd') ADVANCE(547);
      END_STATE();
    case 123:
      if (lookahead == 'g') ADVANCE(136);
      END_STATE();
    case 124:
      if (lookahead == 'h') ADVANCE(545);
      END_STATE();
    case 125:
      if (lookahead == 'i') ADVANCE(127);
      END_STATE();
    case 126:
      if (lookahead == 'm') ADVANCE(117);
      END_STATE();
    case 127:
      if (lookahead == 'n') ADVANCE(121);
      END_STATE();
    case 128:
      if (lookahead == 'n') ADVANCE(47);
      END_STATE();
    case 129:
      if (lookahead == 'n') ADVANCE(122);
      END_STATE();
    case 130:
      if (lookahead == 'o') ADVANCE(937);
      if (lookahead == '|') ADVANCE(939);
      END_STATE();
    case 131:
      if (lookahead == 'o') ADVANCE(937);
      if (lookahead == '|') ADVANCE(939);
      if (lookahead == '}') ADVANCE(539);
      END_STATE();
    case 132:
      if (lookahead == 'o') ADVANCE(935);
      if (lookahead == '{') ADVANCE(940);
      if (lookahead == '|') ADVANCE(941);
      END_STATE();
    case 133:
      if (lookahead == 'o') ADVANCE(128);
      END_STATE();
    case 134:
      if (lookahead == 'p') ADVANCE(550);
      END_STATE();
    case 135:
      if (lookahead == 'p') ADVANCE(124);
      END_STATE();
    case 136:
      if (lookahead == 'r') ADVANCE(118);
      END_STATE();
    case 137:
      if (lookahead == 'u') ADVANCE(119);
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(180);
      if (lookahead == 'H' ||
          lookahead == 'h') ADVANCE(314);
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(162);
      END_STATE();
    case 138:
      if (lookahead == '{') ADVANCE(938);
      if (lookahead == '|') ADVANCE(936);
      END_STATE();
    case 139:
      if (lookahead == '{') ADVANCE(538);
      END_STATE();
    case 140:
      if (lookahead == '}') ADVANCE(539);
      END_STATE();
    case 141:
      if (lookahead == '}') ADVANCE(46);
      if (lookahead != 0 &&
          lookahead != '%') ADVANCE(141);
      END_STATE();
    case 142:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(417);
      END_STATE();
    case 143:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(416);
      END_STATE();
    case 144:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(899);
      END_STATE();
    case 145:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(308);
      END_STATE();
    case 146:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(372);
      END_STATE();
    case 147:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(342);
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(207);
      if (lookahead == 'K' ||
          lookahead == 'k') ADVANCE(944);
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(317);
      END_STATE();
    case 148:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(244);
      END_STATE();
    case 149:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(386);
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(160);
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(351);
      END_STATE();
    case 150:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(292);
      END_STATE();
    case 151:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(257);
      END_STATE();
    case 152:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(293);
      END_STATE();
    case 153:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(294);
      END_STATE();
    case 154:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(349);
      END_STATE();
    case 155:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(295);
      END_STATE();
    case 156:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(393);
      END_STATE();
    case 157:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(379);
      END_STATE();
    case 158:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(380);
      END_STATE();
    case 159:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(309);
      END_STATE();
    case 160:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(183);
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(151);
      END_STATE();
    case 161:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(359);
      END_STATE();
    case 162:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(391);
      END_STATE();
    case 163:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(396);
      END_STATE();
    case 164:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(189);
      END_STATE();
    case 165:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(397);
      END_STATE();
    case 166:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(398);
      END_STATE();
    case 167:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(399);
      END_STATE();
    case 168:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(245);
      END_STATE();
    case 169:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(246);
      END_STATE();
    case 170:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(247);
      END_STATE();
    case 171:
      if (lookahead == 'B' ||
          lookahead == 'b') ADVANCE(915);
      if (lookahead == 'D' ||
          lookahead == 'd') ADVANCE(916);
      END_STATE();
    case 172:
      if (lookahead == 'B' ||
          lookahead == 'b') ADVANCE(915);
      if (lookahead == 'D' ||
          lookahead == 'd') ADVANCE(916);
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(385);
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(196);
      END_STATE();
    case 173:
      if (lookahead == 'B' ||
          lookahead == 'b') ADVANCE(590);
      END_STATE();
    case 174:
      if (lookahead == 'B' ||
          lookahead == 'b') ADVANCE(223);
      END_STATE();
    case 175:
      if (lookahead == 'B' ||
          lookahead == 'b') ADVANCE(382);
      if (lookahead == 'L' ||
          lookahead == 'l') ADVANCE(346);
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(285);
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(173);
      END_STATE();
    case 176:
      if (lookahead == 'C' ||
          lookahead == 'c') ADVANCE(374);
      if (lookahead == 'L' ||
          lookahead == 'l') ADVANCE(375);
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(194);
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(448);
      if (lookahead == 'U' ||
          lookahead == 'u') ADVANCE(388);
      if (lookahead == 'X' ||
          lookahead == 'x') ADVANCE(256);
      END_STATE();
    case 177:
      if (lookahead == 'C' ||
          lookahead == 'c') ADVANCE(287);
      END_STATE();
    case 178:
      if (lookahead == 'C' ||
          lookahead == 'c') ADVANCE(248);
      END_STATE();
    case 179:
      if (lookahead == 'C' ||
          lookahead == 'c') ADVANCE(249);
      if (lookahead == 'F' ||
          lookahead == 'f') ADVANCE(329);
      if (lookahead == 'J' ||
          lookahead == 'j') ADVANCE(327);
      END_STATE();
    case 180:
      if (lookahead == 'C' ||
          lookahead == 'c') ADVANCE(387);
      if (lookahead == 'Q' ||
          lookahead == 'q') ADVANCE(405);
      END_STATE();
    case 181:
      if (lookahead == 'C' ||
          lookahead == 'c') ADVANCE(260);
      END_STATE();
    case 182:
      if (lookahead == 'C' ||
          lookahead == 'c') ADVANCE(376);
      END_STATE();
    case 183:
      if (lookahead == 'C' ||
          lookahead == 'c') ADVANCE(390);
      END_STATE();
    case 184:
      if (lookahead == 'C' ||
          lookahead == 'c') ADVANCE(354);
      END_STATE();
    case 185:
      if (lookahead == 'C' ||
          lookahead == 'c') ADVANCE(212);
      END_STATE();
    case 186:
      if (lookahead == 'C' ||
          lookahead == 'c') ADVANCE(233);
      END_STATE();
    case 187:
      if (lookahead == 'C' ||
          lookahead == 'c') ADVANCE(234);
      END_STATE();
    case 188:
      if (lookahead == 'C' ||
          lookahead == 'c') ADVANCE(288);
      END_STATE();
    case 189:
      if (lookahead == 'C' ||
          lookahead == 'c') ADVANCE(394);
      END_STATE();
    case 190:
      if (lookahead == 'C' ||
          lookahead == 'c') ADVANCE(401);
      END_STATE();
    case 191:
      if (lookahead == 'C' ||
          lookahead == 'c') ADVANCE(251);
      if (lookahead == 'F' ||
          lookahead == 'f') ADVANCE(331);
      if (lookahead == 'J' ||
          lookahead == 'j') ADVANCE(328);
      END_STATE();
    case 192:
      if (lookahead == 'C' ||
          lookahead == 'c') ADVANCE(237);
      END_STATE();
    case 193:
      if (lookahead == 'D' ||
          lookahead == 'd') ADVANCE(464);
      END_STATE();
    case 194:
      if (lookahead == 'D' ||
          lookahead == 'd') ADVANCE(471);
      END_STATE();
    case 195:
      if (lookahead == 'D' ||
          lookahead == 'd') ADVANCE(263);
      END_STATE();
    case 196:
      if (lookahead == 'D' ||
          lookahead == 'd') ADVANCE(142);
      if (lookahead == 'P' ||
          lookahead == 'p') ADVANCE(143);
      END_STATE();
    case 197:
      if (lookahead == 'D' ||
          lookahead == 'd') ADVANCE(209);
      END_STATE();
    case 198:
      if (lookahead == 'D' ||
          lookahead == 'd') ADVANCE(221);
      END_STATE();
    case 199:
      if (lookahead == 'D' ||
          lookahead == 'd') ADVANCE(222);
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(255);
      END_STATE();
    case 200:
      if (lookahead == 'D' ||
          lookahead == 'd') ADVANCE(226);
      END_STATE();
    case 201:
      if (lookahead == 'D' ||
          lookahead == 'd') ADVANCE(156);
      END_STATE();
    case 202:
      if (lookahead == 'D' ||
          lookahead == 'd') ADVANCE(203);
      END_STATE();
    case 203:
      if (lookahead == 'D' ||
          lookahead == 'd') ADVANCE(166);
      END_STATE();
    case 204:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(180);
      if (lookahead == 'H' ||
          lookahead == 'h') ADVANCE(314);
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(162);
      END_STATE();
    case 205:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(241);
      END_STATE();
    case 206:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(241);
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(300);
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(315);
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(912);
      END_STATE();
    case 207:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(514);
      END_STATE();
    case 208:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(468);
      END_STATE();
    case 209:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(19);
      END_STATE();
    case 210:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(16);
      END_STATE();
    case 211:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(462);
      END_STATE();
    case 212:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(98);
      END_STATE();
    case 213:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(453);
      END_STATE();
    case 214:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(454);
      END_STATE();
    case 215:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(339);
      if (lookahead == 'H' ||
          lookahead == 'h') ADVANCE(314);
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(167);
      END_STATE();
    case 216:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(182);
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(243);
      if (lookahead == 'L' ||
          lookahead == 'l') ADVANCE(914);
      END_STATE();
    case 217:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(15);
      END_STATE();
    case 218:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(296);
      END_STATE();
    case 219:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(357);
      END_STATE();
    case 220:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(307);
      END_STATE();
    case 221:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(366);
      END_STATE();
    case 222:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(367);
      END_STATE();
    case 223:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(344);
      END_STATE();
    case 224:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(368);
      END_STATE();
    case 225:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(345);
      END_STATE();
    case 226:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(373);
      END_STATE();
    case 227:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(369);
      END_STATE();
    case 228:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(348);
      END_STATE();
    case 229:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(220);
      END_STATE();
    case 230:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(306);
      END_STATE();
    case 231:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(312);
      END_STATE();
    case 232:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(190);
      END_STATE();
    case 233:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(92);
      END_STATE();
    case 234:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(114);
      END_STATE();
    case 235:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(96);
      END_STATE();
    case 236:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(242);
      END_STATE();
    case 237:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(97);
      END_STATE();
    case 238:
      if (lookahead == 'F' ||
          lookahead == 'f') ADVANCE(696);
      END_STATE();
    case 239:
      if (lookahead == 'F' ||
          lookahead == 'f') ADVANCE(697);
      END_STATE();
    case 240:
      if (lookahead == 'F' ||
          lookahead == 'f') ADVANCE(324);
      END_STATE();
    case 241:
      if (lookahead == 'F' ||
          lookahead == 'f') ADVANCE(383);
      END_STATE();
    case 242:
      if (lookahead == 'F' ||
          lookahead == 'f') ADVANCE(333);
      END_STATE();
    case 243:
      if (lookahead == 'G' ||
          lookahead == 'g') ADVANCE(250);
      END_STATE();
    case 244:
      if (lookahead == 'G' ||
          lookahead == 'g') ADVANCE(353);
      END_STATE();
    case 245:
      if (lookahead == 'G' ||
          lookahead == 'g') ADVANCE(355);
      END_STATE();
    case 246:
      if (lookahead == 'G' ||
          lookahead == 'g') ADVANCE(356);
      END_STATE();
    case 247:
      if (lookahead == 'G' ||
          lookahead == 'g') ADVANCE(358);
      END_STATE();
    case 248:
      if (lookahead == 'H' ||
          lookahead == 'h') ADVANCE(161);
      END_STATE();
    case 249:
      if (lookahead == 'H' ||
          lookahead == 'h') ADVANCE(330);
      END_STATE();
    case 250:
      if (lookahead == 'H' ||
          lookahead == 'h') ADVANCE(400);
      END_STATE();
    case 251:
      if (lookahead == 'H' ||
          lookahead == 'h') ADVANCE(332);
      END_STATE();
    case 252:
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(207);
      if (lookahead == 'K' ||
          lookahead == 'k') ADVANCE(944);
      END_STATE();
    case 253:
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(243);
      END_STATE();
    case 254:
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(412);
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(343);
      END_STATE();
    case 255:
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(410);
      END_STATE();
    case 256:
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(370);
      END_STATE();
    case 257:
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(289);
      END_STATE();
    case 258:
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(197);
      END_STATE();
    case 259:
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(181);
      END_STATE();
    case 260:
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(338);
      END_STATE();
    case 261:
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(365);
      END_STATE();
    case 262:
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(385);
      END_STATE();
    case 263:
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(148);
      END_STATE();
    case 264:
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(321);
      END_STATE();
    case 265:
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(310);
      END_STATE();
    case 266:
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(311);
      END_STATE();
    case 267:
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(323);
      END_STATE();
    case 268:
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(325);
      END_STATE();
    case 269:
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(224);
      END_STATE();
    case 270:
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(337);
      END_STATE();
    case 271:
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(186);
      END_STATE();
    case 272:
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(187);
      END_STATE();
    case 273:
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(168);
      END_STATE();
    case 274:
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(169);
      END_STATE();
    case 275:
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(170);
      END_STATE();
    case 276:
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(413);
      END_STATE();
    case 277:
      if (lookahead == 'K' ||
          lookahead == 'k') ADVANCE(945);
      if (lookahead == 'L' ||
          lookahead == 'l') ADVANCE(316);
      END_STATE();
    case 278:
      if (lookahead == 'K' ||
          lookahead == 'k') ADVANCE(459);
      END_STATE();
    case 279:
      if (lookahead == 'K' ||
          lookahead == 'k') ADVANCE(90);
      END_STATE();
    case 280:
      if (lookahead == 'K' ||
          lookahead == 'k') ADVANCE(112);
      END_STATE();
    case 281:
      if (lookahead == 'K' ||
          lookahead == 'k') ADVANCE(225);
      END_STATE();
    case 282:
      if (lookahead == 'L' ||
          lookahead == 'l') ADVANCE(371);
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(193);
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(195);
      END_STATE();
    case 283:
      if (lookahead == 'L' ||
          lookahead == 'l') ADVANCE(371);
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(193);
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(195);
      if (lookahead == 'X' ||
          lookahead == 'x') ADVANCE(177);
      END_STATE();
    case 284:
      if (lookahead == 'L' ||
          lookahead == 'l') ADVANCE(914);
      END_STATE();
    case 285:
      if (lookahead == 'L' ||
          lookahead == 'l') ADVANCE(592);
      END_STATE();
    case 286:
      if (lookahead == 'L' ||
          lookahead == 'l') ADVANCE(146);
      END_STATE();
    case 287:
      if (lookahead == 'L' ||
          lookahead == 'l') ADVANCE(406);
      END_STATE();
    case 288:
      if (lookahead == 'L' ||
          lookahead == 'l') ADVANCE(403);
      END_STATE();
    case 289:
      if (lookahead == 'L' ||
          lookahead == 'l') ADVANCE(364);
      END_STATE();
    case 290:
      if (lookahead == 'L' ||
          lookahead == 'l') ADVANCE(211);
      END_STATE();
    case 291:
      if (lookahead == 'M' ||
          lookahead == 'm') ADVANCE(174);
      END_STATE();
    case 292:
      if (lookahead == 'M' ||
          lookahead == 'm') ADVANCE(549);
      END_STATE();
    case 293:
      if (lookahead == 'M' ||
          lookahead == 'm') ADVANCE(473);
      END_STATE();
    case 294:
      if (lookahead == 'M' ||
          lookahead == 'm') ADVANCE(502);
      END_STATE();
    case 295:
      if (lookahead == 'M' ||
          lookahead == 'm') ADVANCE(445);
      END_STATE();
    case 296:
      if (lookahead == 'M' ||
          lookahead == 'm') ADVANCE(336);
      END_STATE();
    case 297:
      if (lookahead == 'M' ||
          lookahead == 'm') ADVANCE(154);
      END_STATE();
    case 298:
      if (lookahead == 'M' ||
          lookahead == 'm') ADVANCE(157);
      END_STATE();
    case 299:
      if (lookahead == 'M' ||
          lookahead == 'm') ADVANCE(158);
      END_STATE();
    case 300:
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(278);
      END_STATE();
    case 301:
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(513);
      END_STATE();
    case 302:
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(518);
      END_STATE();
    case 303:
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(814);
      END_STATE();
    case 304:
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(194);
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(448);
      END_STATE();
    case 305:
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(404);
      END_STATE();
    case 306:
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(185);
      END_STATE();
    case 307:
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(202);
      END_STATE();
    case 308:
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(395);
      END_STATE();
    case 309:
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(381);
      END_STATE();
    case 310:
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(91);
      END_STATE();
    case 311:
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(113);
      END_STATE();
    case 312:
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(192);
      END_STATE();
    case 313:
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(188);
      END_STATE();
    case 314:
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(415);
      END_STATE();
    case 315:
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(334);
      END_STATE();
    case 316:
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(414);
      END_STATE();
    case 317:
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(335);
      END_STATE();
    case 318:
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(238);
      END_STATE();
    case 319:
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(305);
      END_STATE();
    case 320:
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(239);
      END_STATE();
    case 321:
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(301);
      END_STATE();
    case 322:
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(389);
      END_STATE();
    case 323:
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(302);
      END_STATE();
    case 324:
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(360);
      END_STATE();
    case 325:
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(303);
      END_STATE();
    case 326:
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(347);
      END_STATE();
    case 327:
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(265);
      END_STATE();
    case 328:
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(266);
      END_STATE();
    case 329:
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(350);
      END_STATE();
    case 330:
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(271);
      END_STATE();
    case 331:
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(352);
      END_STATE();
    case 332:
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(272);
      END_STATE();
    case 333:
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(361);
      END_STATE();
    case 334:
      if (lookahead == 'P' ||
          lookahead == 'p') ADVANCE(463);
      END_STATE();
    case 335:
      if (lookahead == 'P' ||
          lookahead == 'p') ADVANCE(219);
      END_STATE();
    case 336:
      if (lookahead == 'P' ||
          lookahead == 'p') ADVANCE(384);
      END_STATE();
    case 337:
      if (lookahead == 'P' ||
          lookahead == 'p') ADVANCE(402);
      END_STATE();
    case 338:
      if (lookahead == 'P' ||
          lookahead == 'p') ADVANCE(159);
      END_STATE();
    case 339:
      if (lookahead == 'Q' ||
          lookahead == 'q') ADVANCE(407);
      END_STATE();
    case 340:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(913);
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(918);
      END_STATE();
    case 341:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(912);
      END_STATE();
    case 342:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(470);
      END_STATE();
    case 343:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(447);
      END_STATE();
    case 344:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(452);
      END_STATE();
    case 345:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(512);
      END_STATE();
    case 346:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(593);
      END_STATE();
    case 347:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(449);
      END_STATE();
    case 348:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(456);
      END_STATE();
    case 349:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(281);
      END_STATE();
    case 350:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(279);
      END_STATE();
    case 351:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(232);
      END_STATE();
    case 352:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(280);
      END_STATE();
    case 353:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(150);
      END_STATE();
    case 354:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(270);
      END_STATE();
    case 355:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(152);
      END_STATE();
    case 356:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(153);
      END_STATE();
    case 357:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(392);
      END_STATE();
    case 358:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(155);
      END_STATE();
    case 359:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(378);
      END_STATE();
    case 360:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(298);
      END_STATE();
    case 361:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(299);
      END_STATE();
    case 362:
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(448);
      END_STATE();
    case 363:
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(95);
      END_STATE();
    case 364:
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(461);
      END_STATE();
    case 365:
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(508);
      END_STATE();
    case 366:
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(511);
      END_STATE();
    case 367:
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(510);
      END_STATE();
    case 368:
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(460);
      END_STATE();
    case 369:
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(507);
      END_STATE();
    case 370:
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(240);
      END_STATE();
    case 371:
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(208);
      END_STATE();
    case 372:
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(363);
      END_STATE();
    case 373:
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(184);
      END_STATE();
    case 374:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(254);
      END_STATE();
    case 375:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(467);
      END_STATE();
    case 376:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(465);
      END_STATE();
    case 377:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(505);
      END_STATE();
    case 378:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(515);
      END_STATE();
    case 379:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(509);
      END_STATE();
    case 380:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(506);
      END_STATE();
    case 381:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(446);
      END_STATE();
    case 382:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(591);
      END_STATE();
    case 383:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(17);
      END_STATE();
    case 384:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(418);
      END_STATE();
    case 385:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(290);
      END_STATE();
    case 386:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(236);
      END_STATE();
    case 387:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(264);
      END_STATE();
    case 388:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(319);
      END_STATE();
    case 389:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(217);
      END_STATE();
    case 390:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(276);
      END_STATE();
    case 391:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(210);
      END_STATE();
    case 392:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(269);
      END_STATE();
    case 393:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(144);
      END_STATE();
    case 394:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(326);
      END_STATE();
    case 395:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(377);
      END_STATE();
    case 396:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(213);
      END_STATE();
    case 397:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(214);
      END_STATE();
    case 398:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(227);
      END_STATE();
    case 399:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(235);
      END_STATE();
    case 400:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(18);
      END_STATE();
    case 401:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(267);
      END_STATE();
    case 402:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(268);
      END_STATE();
    case 403:
      if (lookahead == 'U' ||
          lookahead == 'u') ADVANCE(199);
      END_STATE();
    case 404:
      if (lookahead == 'U' ||
          lookahead == 'u') ADVANCE(291);
      END_STATE();
    case 405:
      if (lookahead == 'U' ||
          lookahead == 'u') ADVANCE(230);
      END_STATE();
    case 406:
      if (lookahead == 'U' ||
          lookahead == 'u') ADVANCE(198);
      END_STATE();
    case 407:
      if (lookahead == 'U' ||
          lookahead == 'u') ADVANCE(231);
      END_STATE();
    case 408:
      if (lookahead == 'V' ||
          lookahead == 'v') ADVANCE(68);
      END_STATE();
    case 409:
      if (lookahead == 'V' ||
          lookahead == 'v') ADVANCE(69);
      END_STATE();
    case 410:
      if (lookahead == 'V' ||
          lookahead == 'v') ADVANCE(229);
      END_STATE();
    case 411:
      if (lookahead == 'V' ||
          lookahead == 'v') ADVANCE(228);
      END_STATE();
    case 412:
      if (lookahead == 'V' ||
          lookahead == 'v') ADVANCE(163);
      END_STATE();
    case 413:
      if (lookahead == 'V' ||
          lookahead == 'v') ADVANCE(165);
      END_STATE();
    case 414:
      if (lookahead == 'W' ||
          lookahead == 'w') ADVANCE(178);
      END_STATE();
    case 415:
      if (lookahead == 'W' ||
          lookahead == 'w') ADVANCE(201);
      END_STATE();
    case 416:
      if (lookahead == 'X' ||
          lookahead == 'x') ADVANCE(261);
      END_STATE();
    case 417:
      if (lookahead == 'Y' ||
          lookahead == 'y') ADVANCE(297);
      END_STATE();
    case 418:
      if (lookahead == 'Y' ||
          lookahead == 'y') ADVANCE(20);
      END_STATE();
    case 419:
      if (lookahead == '>' ||
          lookahead == 'o' ||
          lookahead == 'x') ADVANCE(930);
      END_STATE();
    case 420:
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') ADVANCE(420);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(910);
      END_STATE();
    case 421:
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(911);
      END_STATE();
    case 422:
      if (lookahead != 0 &&
          lookahead != '"') ADVANCE(26);
      END_STATE();
    case 423:
      if (lookahead != 0 &&
          lookahead != '`') ADVANCE(116);
      END_STATE();
    case 424:
      if (lookahead != 0 &&
          lookahead != '`') ADVANCE(556);
      END_STATE();
    case 425:
      if (eof) ADVANCE(442);
      if (lookahead == '\t') ADVANCE(568);
      if (lookahead == '\n') ADVANCE(582);
      if (lookahead == ' ') ADVANCE(572);
      if (lookahead == '%') ADVANCE(601);
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(612);
      if (lookahead == 'D' ||
          lookahead == 'd') ADVANCE(624);
      if (lookahead == 'L' ||
          lookahead == 'l') ADVANCE(633);
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(654);
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(658);
      if (lookahead == 'P' ||
          lookahead == 'p') ADVANCE(603);
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(625);
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(626);
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(638);
      if (lookahead != 0 &&
          (lookahead < '+' || '-' < lookahead) &&
          lookahead != ':' &&
          lookahead != ';' &&
          lookahead != '>') ADVANCE(687);
      END_STATE();
    case 426:
      if (eof) ADVANCE(442);
      if (lookahead == '\n') ADVANCE(581);
      if (lookahead == '%') ADVANCE(30);
      if (lookahead == '(') ADVANCE(494);
      if (lookahead == ')') ADVANCE(497);
      if (lookahead == ':') ADVANCE(75);
      if (lookahead == '[') ADVANCE(522);
      if (lookahead == '{') ADVANCE(139);
      if (lookahead == '\t' ||
          lookahead == ' ') ADVANCE(569);
      if ((11 <= lookahead && lookahead <= '\r')) ADVANCE(70);
      END_STATE();
    case 427:
      if (eof) ADVANCE(442);
      if (lookahead == '\n') ADVANCE(582);
      if (lookahead == '"') ADVANCE(23);
      if (lookahead == '%') ADVANCE(29);
      if (lookahead == '&') ADVANCE(519);
      if (lookahead == '(') ADVANCE(495);
      if (lookahead == ')') ADVANCE(48);
      if (lookahead == ',') ADVANCE(457);
      if (lookahead == '-') ADVANCE(52);
      if (lookahead == '.') ADVANCE(58);
      if (lookahead == '/') ADVANCE(104);
      if (lookahead == ':') ADVANCE(450);
      if (lookahead == ';') ADVANCE(516);
      if (lookahead == '=') ADVANCE(82);
      if (lookahead == '>') ADVANCE(540);
      if (lookahead == 'O') ADVANCE(411);
      if (lookahead == '[') ADVANCE(523);
      if (lookahead == '\\') ADVANCE(105);
      if (lookahead == ']') ADVANCE(524);
      if (lookahead == '`') ADVANCE(22);
      if (lookahead == 'm') ADVANCE(125);
      if (lookahead == 'o') ADVANCE(65);
      if (lookahead == '{') ADVANCE(485);
      if (lookahead == '|') ADVANCE(107);
      if (lookahead == '}') ADVANCE(131);
      if (lookahead == '~') ADVANCE(474);
      if (lookahead == '\t' ||
          lookahead == ' ') ADVANCE(572);
      if (lookahead == '<' ||
          lookahead == 'x') ADVANCE(64);
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(304);
      if (lookahead == 'C' ||
          lookahead == 'c') ADVANCE(286);
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(282);
      if (lookahead == 'F' ||
          lookahead == 'f') ADVANCE(277);
      if (lookahead == 'G' ||
          lookahead == 'g') ADVANCE(145);
      if (lookahead == 'L' ||
          lookahead == 'l') ADVANCE(205);
      if (lookahead == 'P' ||
          lookahead == 'p') ADVANCE(252);
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(253);
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(215);
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(262);
      END_STATE();
    case 428:
      if (eof) ADVANCE(442);
      if (lookahead == '\n') ADVANCE(582);
      if (lookahead == '"') ADVANCE(23);
      if (lookahead == '%') ADVANCE(30);
      if (lookahead == '-') ADVANCE(61);
      if (lookahead == '.') ADVANCE(56);
      if (lookahead == '}') ADVANCE(486);
      if (lookahead == '\t' ||
          lookahead == ' ') ADVANCE(572);
      if (('A' <= lookahead && lookahead <= 'Z') ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(934);
      END_STATE();
    case 429:
      if (eof) ADVANCE(442);
      if (lookahead == '\n') ADVANCE(582);
      if (lookahead == '"') ADVANCE(422);
      if (lookahead == '%') ADVANCE(30);
      if (lookahead == '(') ADVANCE(493);
      if (lookahead == ')') ADVANCE(496);
      if (lookahead == '*') ADVANCE(498);
      if (lookahead == '-') ADVANCE(60);
      if (lookahead == '.') ADVANCE(67);
      if (lookahead == ':') ADVANCE(450);
      if (lookahead == '<') ADVANCE(730);
      if (lookahead == '>') ADVANCE(540);
      if (lookahead == '[') ADVANCE(522);
      if (lookahead == ']') ADVANCE(524);
      if (lookahead == '^') ADVANCE(919);
      if (lookahead == 'e') ADVANCE(129);
      if (lookahead == 'o') ADVANCE(724);
      if (lookahead == 'v') ADVANCE(917);
      if (lookahead == '|') ADVANCE(85);
      if (lookahead == '}') ADVANCE(140);
      if (lookahead == '~') ADVANCE(474);
      if (lookahead == '\t' ||
          lookahead == ' ') ADVANCE(572);
      if (lookahead == 'B' ||
          lookahead == 'b') ADVANCE(340);
      if (lookahead == 'L' ||
          lookahead == 'l') ADVANCE(341);
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(284);
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(171);
      END_STATE();
    case 430:
      if (eof) ADVANCE(442);
      if (lookahead == '\n') ADVANCE(582);
      if (lookahead == '"') ADVANCE(422);
      if (lookahead == '%') ADVANCE(30);
      if (lookahead == '-') ADVANCE(60);
      if (lookahead == '.') ADVANCE(67);
      if (lookahead == ':') ADVANCE(76);
      if (lookahead == '`') ADVANCE(423);
      if (lookahead == '{') ADVANCE(484);
      if (lookahead == '~') ADVANCE(474);
      if (lookahead == '\t' ||
          lookahead == ' ') ADVANCE(572);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          ('_' <= lookahead && lookahead <= 'z')) ADVANCE(702);
      END_STATE();
    case 431:
      if (eof) ADVANCE(442);
      if (lookahead == '\n') ADVANCE(582);
      if (lookahead == '"') ADVANCE(902);
      if (lookahead == '%') ADVANCE(903);
      if (lookahead == '\t' ||
          lookahead == ' ') ADVANCE(575);
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(905);
      if (lookahead != 0 &&
          lookahead != '#' &&
          lookahead != ';') ADVANCE(908);
      END_STATE();
    case 432:
      if (eof) ADVANCE(442);
      if (lookahead == '\n') ADVANCE(582);
      if (lookahead == '$') ADVANCE(499);
      if (lookahead == '%') ADVANCE(30);
      if (lookahead == '(') ADVANCE(493);
      if (lookahead == ')') ADVANCE(496);
      if (lookahead == '*') ADVANCE(498);
      if (lookahead == ';') ADVANCE(516);
      if (lookahead == '[') ADVANCE(522);
      if (lookahead == ']') ADVANCE(524);
      if (lookahead == '|') ADVANCE(520);
      if (lookahead == '}') ADVANCE(486);
      if (lookahead == '\t' ||
          lookahead == ' ') ADVANCE(572);
      if (lookahead == '!' ||
          ('0' <= lookahead && lookahead <= '9') ||
          lookahead == '?' ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z') ||
          lookahead == '~') ADVANCE(721);
      END_STATE();
    case 433:
      if (eof) ADVANCE(442);
      if (lookahead == '\n') ADVANCE(582);
      if (lookahead == '%') ADVANCE(827);
      if (lookahead == '\t' ||
          lookahead == ' ') ADVANCE(572);
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(890);
      if (lookahead == 'D' ||
          lookahead == 'd') ADVANCE(828);
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(891);
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(868);
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(844);
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(856);
      if (lookahead != 0 &&
          (lookahead < 11 || '\r' < lookahead) &&
          lookahead != '#' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(894);
      END_STATE();
    case 434:
      if (eof) ADVANCE(442);
      if (lookahead == '\n') ADVANCE(582);
      if (lookahead == '%') ADVANCE(743);
      if (lookahead == '\t' ||
          lookahead == ' ') ADVANCE(573);
      if (lookahead != 0 &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(787);
      END_STATE();
    case 435:
      if (eof) ADVANCE(442);
      if (lookahead == '\n') ADVANCE(582);
      if (lookahead == '%') ADVANCE(30);
      if (lookahead == '(') ADVANCE(493);
      if (lookahead == ')') ADVANCE(496);
      if (lookahead == '-') ADVANCE(61);
      if (lookahead == '.') ADVANCE(56);
      if (lookahead == ':') ADVANCE(450);
      if (lookahead == '>') ADVANCE(86);
      if (lookahead == '[') ADVANCE(54);
      if (lookahead == ']') ADVANCE(49);
      if (lookahead == '{') ADVANCE(484);
      if (lookahead == '|') ADVANCE(520);
      if (lookahead == '}') ADVANCE(486);
      if (lookahead == '\t' ||
          lookahead == ' ') ADVANCE(572);
      if (lookahead == 'D' ||
          lookahead == 'd') ADVANCE(801);
      if (lookahead == 'H' ||
          lookahead == 'h') ADVANCE(802);
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(806);
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(809);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(813);
      END_STATE();
    case 436:
      if (eof) ADVANCE(442);
      if (lookahead == '\n') ADVANCE(582);
      if (lookahead == '%') ADVANCE(30);
      if (lookahead == '-') ADVANCE(60);
      if (lookahead == ':') ADVANCE(76);
      if (lookahead == '<') ADVANCE(80);
      if (lookahead == '[') ADVANCE(55);
      if (lookahead == '{') ADVANCE(484);
      if (lookahead == '}') ADVANCE(486);
      if (lookahead == '~') ADVANCE(474);
      if (lookahead == '\t' ||
          lookahead == ' ') ADVANCE(572);
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(808);
      if (lookahead == 'D' ||
          lookahead == 'd') ADVANCE(801);
      if (lookahead == 'H' ||
          lookahead == 'h') ADVANCE(802);
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(806);
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(809);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('B' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('b' <= lookahead && lookahead <= 'z')) ADVANCE(813);
      END_STATE();
    case 437:
      if (eof) ADVANCE(442);
      if (lookahead == '\n') ADVANCE(582);
      if (lookahead == '%') ADVANCE(30);
      if (lookahead == '-') ADVANCE(53);
      if (lookahead == ':') ADVANCE(450);
      if (lookahead == '<') ADVANCE(80);
      if (lookahead == '[') ADVANCE(103);
      if (lookahead == 'o') ADVANCE(138);
      if (lookahead == '{') ADVANCE(484);
      if (lookahead == '|') ADVANCE(132);
      if (lookahead == '}') ADVANCE(130);
      if (lookahead == '\t' ||
          lookahead == ' ') ADVANCE(572);
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(362);
      END_STATE();
    case 438:
      if (eof) ADVANCE(442);
      if (lookahead == '\n') ADVANCE(582);
      if (lookahead == '%') ADVANCE(30);
      if (lookahead == ':') ADVANCE(450);
      if (lookahead == '`') ADVANCE(423);
      if (lookahead == '~') ADVANCE(474);
      if (lookahead == '\t' ||
          lookahead == ' ') ADVANCE(572);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          ('_' <= lookahead && lookahead <= 'z')) ADVANCE(702);
      END_STATE();
    case 439:
      if (eof) ADVANCE(442);
      if (lookahead == '\n') ADVANCE(582);
      if (lookahead == '%') ADVANCE(30);
      if (lookahead == ';') ADVANCE(516);
      if (lookahead == 's') ADVANCE(712);
      if (lookahead == '\t' ||
          lookahead == ' ') ADVANCE(572);
      if (lookahead == 'D' ||
          lookahead == 'd') ADVANCE(715);
      if (lookahead == '!' ||
          ('0' <= lookahead && lookahead <= '9') ||
          lookahead == '?' ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z') ||
          lookahead == '~') ADVANCE(721);
      END_STATE();
    case 440:
      if (eof) ADVANCE(442);
      if (lookahead == '\n') ADVANCE(583);
      if (lookahead == '%') ADVANCE(30);
      if (lookahead == ':') ADVANCE(77);
      if (lookahead == '\t' ||
          lookahead == ' ') ADVANCE(570);
      if ((11 <= lookahead && lookahead <= '\r')) ADVANCE(78);
      END_STATE();
    case 441:
      if (eof) ADVANCE(442);
      if (lookahead == '%') ADVANCE(30);
      if (lookahead == '<') ADVANCE(79);
      if (lookahead == '`') ADVANCE(423);
      if (lookahead == 'c') ADVANCE(699);
      if (lookahead == '\t' ||
          lookahead == ' ') ADVANCE(572);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          ('_' <= lookahead && lookahead <= 'z')) ADVANCE(702);
      END_STATE();
    case 442:
      ACCEPT_TOKEN(ts_builtin_sym_end);
      END_STATE();
    case 443:
      ACCEPT_TOKEN(anon_sym_PERCENT_PERCENT_LBRACE);
      END_STATE();
    case 444:
      ACCEPT_TOKEN(aux_sym_directive_token1);
      END_STATE();
    case 445:
      ACCEPT_TOKEN(aux_sym_diagram_sequence_token1);
      END_STATE();
    case 446:
      ACCEPT_TOKEN(aux_sym__sequence_participant_type_token1);
      END_STATE();
    case 447:
      ACCEPT_TOKEN(aux_sym__sequence_participant_type_token2);
      END_STATE();
    case 448:
      ACCEPT_TOKEN(aux_sym_sequence_stmt_participant_token1);
      END_STATE();
    case 449:
      ACCEPT_TOKEN(aux_sym_sequence_stmt_actor_token1);
      END_STATE();
    case 450:
      ACCEPT_TOKEN(anon_sym_COLON);
      END_STATE();
    case 451:
      ACCEPT_TOKEN(anon_sym_COLON);
      if (lookahead == ':') ADVANCE(72);
      END_STATE();
    case 452:
      ACCEPT_TOKEN(aux_sym_sequence_stmt_autonumber_token1);
      END_STATE();
    case 453:
      ACCEPT_TOKEN(aux_sym_sequence_stmt_activate_token1);
      END_STATE();
    case 454:
      ACCEPT_TOKEN(aux_sym_sequence_stmt_deactivate_token1);
      END_STATE();
    case 455:
      ACCEPT_TOKEN(aux_sym_sequence_stmt_note_token1);
      END_STATE();
    case 456:
      ACCEPT_TOKEN(aux_sym_sequence_stmt_note_token2);
      END_STATE();
    case 457:
      ACCEPT_TOKEN(anon_sym_COMMA);
      END_STATE();
    case 458:
      ACCEPT_TOKEN(aux_sym_sequence_stmt_links_token1);
      END_STATE();
    case 459:
      ACCEPT_TOKEN(aux_sym_sequence_stmt_link_token1);
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(458);
      END_STATE();
    case 460:
      ACCEPT_TOKEN(aux_sym_sequence_stmt_properties_token1);
      END_STATE();
    case 461:
      ACCEPT_TOKEN(aux_sym_sequence_stmt_details_token1);
      END_STATE();
    case 462:
      ACCEPT_TOKEN(aux_sym_sequence_stmt_title_token1);
      END_STATE();
    case 463:
      ACCEPT_TOKEN(aux_sym_sequence_stmt_loop_token1);
      END_STATE();
    case 464:
      ACCEPT_TOKEN(aux_sym_sequence_stmt_loop_token2);
      END_STATE();
    case 465:
      ACCEPT_TOKEN(aux_sym_sequence_stmt_rect_token1);
      END_STATE();
    case 466:
      ACCEPT_TOKEN(aux_sym_sequence_stmt_opt_token1);
      END_STATE();
    case 467:
      ACCEPT_TOKEN(aux_sym_sequence_stmt_alt_token1);
      END_STATE();
    case 468:
      ACCEPT_TOKEN(aux_sym_sequence_stmt_alt_token2);
      END_STATE();
    case 469:
      ACCEPT_TOKEN(aux_sym_sequence_stmt_par_token1);
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(635);
      END_STATE();
    case 470:
      ACCEPT_TOKEN(aux_sym_sequence_stmt_par_token1);
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(259);
      END_STATE();
    case 471:
      ACCEPT_TOKEN(aux_sym_sequence_stmt_par_token2);
      END_STATE();
    case 472:
      ACCEPT_TOKEN(aux_sym_diagram_class_token1);
      END_STATE();
    case 473:
      ACCEPT_TOKEN(aux_sym_diagram_class_token2);
      if (lookahead == '-') ADVANCE(408);
      END_STATE();
    case 474:
      ACCEPT_TOKEN(anon_sym_TILDE);
      END_STATE();
    case 475:
      ACCEPT_TOKEN(anon_sym_TILDE);
      if (lookahead == '!' ||
          ('0' <= lookahead && lookahead <= '9') ||
          lookahead == '?' ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z') ||
          lookahead == '~') ADVANCE(721);
      END_STATE();
    case 476:
      ACCEPT_TOKEN(aux_sym_class_generics_token1);
      if (lookahead == '\n') ADVANCE(480);
      if (lookahead == '~') ADVANCE(10);
      if (lookahead != 0) ADVANCE(476);
      END_STATE();
    case 477:
      ACCEPT_TOKEN(aux_sym_class_generics_token1);
      if (lookahead == '\n') ADVANCE(476);
      if (lookahead == '{') ADVANCE(480);
      if (lookahead == '~') ADVANCE(10);
      if (lookahead != 0) ADVANCE(476);
      END_STATE();
    case 478:
      ACCEPT_TOKEN(aux_sym_class_generics_token1);
      if (lookahead == '%') ADVANCE(477);
      if (lookahead != 0 &&
          lookahead != '~') ADVANCE(480);
      END_STATE();
    case 479:
      ACCEPT_TOKEN(aux_sym_class_generics_token1);
      if (lookahead == '\t' ||
          lookahead == ' ') ADVANCE(479);
      if (lookahead != 0 &&
          lookahead != '~') ADVANCE(480);
      END_STATE();
    case 480:
      ACCEPT_TOKEN(aux_sym_class_generics_token1);
      if (lookahead != 0 &&
          lookahead != '~') ADVANCE(480);
      END_STATE();
    case 481:
      ACCEPT_TOKEN(anon_sym_class);
      if (lookahead == 'D') ADVANCE(273);
      END_STATE();
    case 482:
      ACCEPT_TOKEN(anon_sym_class);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(702);
      END_STATE();
    case 483:
      ACCEPT_TOKEN(anon_sym_COLON_COLON_COLON);
      END_STATE();
    case 484:
      ACCEPT_TOKEN(anon_sym_LBRACE);
      END_STATE();
    case 485:
      ACCEPT_TOKEN(anon_sym_LBRACE);
      if (lookahead == '{') ADVANCE(538);
      END_STATE();
    case 486:
      ACCEPT_TOKEN(anon_sym_RBRACE);
      END_STATE();
    case 487:
      ACCEPT_TOKEN(anon_sym_RBRACE);
      if (lookahead == 'o') ADVANCE(937);
      if (lookahead == '|') ADVANCE(939);
      if (lookahead == '}') ADVANCE(539);
      END_STATE();
    case 488:
      ACCEPT_TOKEN(anon_sym_RBRACE);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(787);
      END_STATE();
    case 489:
      ACCEPT_TOKEN(anon_sym_PLUS);
      END_STATE();
    case 490:
      ACCEPT_TOKEN(anon_sym_DASH);
      END_STATE();
    case 491:
      ACCEPT_TOKEN(anon_sym_DASH);
      if (lookahead == '-') ADVANCE(732);
      if (lookahead == '>') ADVANCE(690);
      END_STATE();
    case 492:
      ACCEPT_TOKEN(anon_sym_POUND);
      END_STATE();
    case 493:
      ACCEPT_TOKEN(anon_sym_LPAREN);
      END_STATE();
    case 494:
      ACCEPT_TOKEN(anon_sym_LPAREN);
      if (lookahead == '(') ADVANCE(526);
      END_STATE();
    case 495:
      ACCEPT_TOKEN(anon_sym_LPAREN);
      if (lookahead == '(') ADVANCE(526);
      if (lookahead == '-') ADVANCE(528);
      if (lookahead == '[') ADVANCE(530);
      END_STATE();
    case 496:
      ACCEPT_TOKEN(anon_sym_RPAREN);
      END_STATE();
    case 497:
      ACCEPT_TOKEN(anon_sym_RPAREN);
      if (lookahead == ')') ADVANCE(527);
      END_STATE();
    case 498:
      ACCEPT_TOKEN(anon_sym_STAR);
      END_STATE();
    case 499:
      ACCEPT_TOKEN(anon_sym_DOLLAR);
      END_STATE();
    case 500:
      ACCEPT_TOKEN(anon_sym_LT_LT);
      END_STATE();
    case 501:
      ACCEPT_TOKEN(anon_sym_GT_GT);
      END_STATE();
    case 502:
      ACCEPT_TOKEN(aux_sym_diagram_state_token1);
      if (lookahead == '-') ADVANCE(409);
      END_STATE();
    case 503:
      ACCEPT_TOKEN(aux_sym_diagram_state_token2);
      END_STATE();
    case 504:
      ACCEPT_TOKEN(aux_sym_state_stmt_simple_token1);
      END_STATE();
    case 505:
      ACCEPT_TOKEN(aux_sym_diagram_gantt_token1);
      END_STATE();
    case 506:
      ACCEPT_TOKEN(aux_sym_gantt_stmt_dateformat_token1);
      END_STATE();
    case 507:
      ACCEPT_TOKEN(aux_sym_gantt_stmt_inclusiveenddates_token1);
      END_STATE();
    case 508:
      ACCEPT_TOKEN(aux_sym_gantt_stmt_topaxis_token1);
      END_STATE();
    case 509:
      ACCEPT_TOKEN(aux_sym_gantt_stmt_axisformat_token1);
      END_STATE();
    case 510:
      ACCEPT_TOKEN(aux_sym_gantt_stmt_includes_token1);
      END_STATE();
    case 511:
      ACCEPT_TOKEN(aux_sym_gantt_stmt_excludes_token1);
      END_STATE();
    case 512:
      ACCEPT_TOKEN(aux_sym_gantt_stmt_todaymarker_token1);
      END_STATE();
    case 513:
      ACCEPT_TOKEN(aux_sym_gantt_stmt_section_token1);
      END_STATE();
    case 514:
      ACCEPT_TOKEN(aux_sym_diagram_pie_token1);
      END_STATE();
    case 515:
      ACCEPT_TOKEN(aux_sym_diagram_flow_token1);
      END_STATE();
    case 516:
      ACCEPT_TOKEN(anon_sym_SEMI);
      END_STATE();
    case 517:
      ACCEPT_TOKEN(aux_sym_flow_stmt_direction_token1);
      END_STATE();
    case 518:
      ACCEPT_TOKEN(aux_sym_flow_stmt_direction_token1);
      if (lookahead == ' ') ADVANCE(175);
      END_STATE();
    case 519:
      ACCEPT_TOKEN(anon_sym_AMP);
      END_STATE();
    case 520:
      ACCEPT_TOKEN(anon_sym_PIPE);
      END_STATE();
    case 521:
      ACCEPT_TOKEN(anon_sym_PIPE);
      if (lookahead == '>') ADVANCE(728);
      if (lookahead == ']') ADVANCE(535);
      if (lookahead == '{') ADVANCE(940);
      if (lookahead == '|') ADVANCE(941);
      END_STATE();
    case 522:
      ACCEPT_TOKEN(anon_sym_LBRACK);
      END_STATE();
    case 523:
      ACCEPT_TOKEN(anon_sym_LBRACK);
      if (lookahead == '(') ADVANCE(536);
      if (lookahead == '/') ADVANCE(541);
      if (lookahead == '[') ADVANCE(532);
      if (lookahead == '\\') ADVANCE(543);
      if (lookahead == '|') ADVANCE(534);
      END_STATE();
    case 524:
      ACCEPT_TOKEN(anon_sym_RBRACK);
      END_STATE();
    case 525:
      ACCEPT_TOKEN(anon_sym_RBRACK);
      if (lookahead == ')') ADVANCE(531);
      if (lookahead == ']') ADVANCE(533);
      END_STATE();
    case 526:
      ACCEPT_TOKEN(anon_sym_LPAREN_LPAREN);
      END_STATE();
    case 527:
      ACCEPT_TOKEN(anon_sym_RPAREN_RPAREN);
      END_STATE();
    case 528:
      ACCEPT_TOKEN(anon_sym_LPAREN_DASH);
      END_STATE();
    case 529:
      ACCEPT_TOKEN(anon_sym_DASH_RPAREN);
      END_STATE();
    case 530:
      ACCEPT_TOKEN(anon_sym_LPAREN_LBRACK);
      END_STATE();
    case 531:
      ACCEPT_TOKEN(anon_sym_RBRACK_RPAREN);
      END_STATE();
    case 532:
      ACCEPT_TOKEN(anon_sym_LBRACK_LBRACK);
      END_STATE();
    case 533:
      ACCEPT_TOKEN(anon_sym_RBRACK_RBRACK);
      END_STATE();
    case 534:
      ACCEPT_TOKEN(anon_sym_LBRACK_PIPE);
      END_STATE();
    case 535:
      ACCEPT_TOKEN(anon_sym_PIPE_RBRACK);
      END_STATE();
    case 536:
      ACCEPT_TOKEN(anon_sym_LBRACK_LPAREN);
      END_STATE();
    case 537:
      ACCEPT_TOKEN(anon_sym_RPAREN_RBRACK);
      END_STATE();
    case 538:
      ACCEPT_TOKEN(anon_sym_LBRACE_LBRACE);
      END_STATE();
    case 539:
      ACCEPT_TOKEN(anon_sym_RBRACE_RBRACE);
      END_STATE();
    case 540:
      ACCEPT_TOKEN(anon_sym_GT);
      END_STATE();
    case 541:
      ACCEPT_TOKEN(anon_sym_LBRACK_SLASH);
      END_STATE();
    case 542:
      ACCEPT_TOKEN(anon_sym_BSLASH_RBRACK);
      END_STATE();
    case 543:
      ACCEPT_TOKEN(anon_sym_LBRACK_BSLASH);
      END_STATE();
    case 544:
      ACCEPT_TOKEN(anon_sym_SLASH_RBRACK);
      END_STATE();
    case 545:
      ACCEPT_TOKEN(anon_sym_subgraph);
      END_STATE();
    case 546:
      ACCEPT_TOKEN(anon_sym_subgraph);
      if (lookahead == '!' ||
          ('0' <= lookahead && lookahead <= '9') ||
          lookahead == '?' ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z') ||
          lookahead == '~') ADVANCE(721);
      END_STATE();
    case 547:
      ACCEPT_TOKEN(anon_sym_end);
      END_STATE();
    case 548:
      ACCEPT_TOKEN(anon_sym_end);
      if (lookahead == '!' ||
          ('0' <= lookahead && lookahead <= '9') ||
          lookahead == '?' ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z') ||
          lookahead == '~') ADVANCE(721);
      END_STATE();
    case 549:
      ACCEPT_TOKEN(aux_sym_diagram_er_token1);
      END_STATE();
    case 550:
      ACCEPT_TOKEN(aux_sym_diagram_mindmap_token1);
      END_STATE();
    case 551:
      ACCEPT_TOKEN(aux_sym_mmap_markdown_token1);
      if (lookahead == '\n') ADVANCE(555);
      if (lookahead == '"' ||
          lookahead == '`') ADVANCE(10);
      if (lookahead != 0) ADVANCE(551);
      END_STATE();
    case 552:
      ACCEPT_TOKEN(aux_sym_mmap_markdown_token1);
      if (lookahead == '\n') ADVANCE(551);
      if (lookahead == '{') ADVANCE(555);
      if (lookahead == '"' ||
          lookahead == '`') ADVANCE(10);
      if (lookahead != 0) ADVANCE(551);
      END_STATE();
    case 553:
      ACCEPT_TOKEN(aux_sym_mmap_markdown_token1);
      if (lookahead == '%') ADVANCE(552);
      if (lookahead != 0 &&
          lookahead != '"' &&
          lookahead != '`') ADVANCE(555);
      END_STATE();
    case 554:
      ACCEPT_TOKEN(aux_sym_mmap_markdown_token1);
      if (lookahead == '\t' ||
          lookahead == ' ') ADVANCE(554);
      if (lookahead != 0 &&
          lookahead != '"' &&
          lookahead != '`') ADVANCE(555);
      END_STATE();
    case 555:
      ACCEPT_TOKEN(aux_sym_mmap_markdown_token1);
      if (lookahead != 0 &&
          lookahead != '"' &&
          lookahead != '`') ADVANCE(555);
      END_STATE();
    case 556:
      ACCEPT_TOKEN(aux_sym_mmap_text_token1);
      END_STATE();
    case 557:
      ACCEPT_TOKEN(aux_sym_mmap_text_token1);
      if (lookahead == '\n') ADVANCE(585);
      if (lookahead != 0 &&
          lookahead != '{') ADVANCE(10);
      END_STATE();
    case 558:
      ACCEPT_TOKEN(aux_sym_mmap_text_token1);
      if (lookahead == '\t' ||
          lookahead == ' ') ADVANCE(572);
      END_STATE();
    case 559:
      ACCEPT_TOKEN(aux_sym_mmap_class_token1);
      END_STATE();
    case 560:
      ACCEPT_TOKEN(aux_sym_mmap_class_token2);
      if (lookahead == '\t') ADVANCE(572);
      if (lookahead == ' ') ADVANCE(560);
      if (lookahead == '-' ||
          ('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(561);
      END_STATE();
    case 561:
      ACCEPT_TOKEN(aux_sym_mmap_class_token2);
      if (lookahead == ' ' ||
          lookahead == '-' ||
          ('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(561);
      END_STATE();
    case 562:
      ACCEPT_TOKEN(aux_sym_mmap_icon_token1);
      END_STATE();
    case 563:
      ACCEPT_TOKEN(aux_sym_mmap_icon_token2);
      if (lookahead == '\n') ADVANCE(585);
      if (lookahead == '{') ADVANCE(567);
      if (lookahead == '\r' ||
          lookahead == ')') ADVANCE(10);
      if (lookahead != 0) ADVANCE(564);
      END_STATE();
    case 564:
      ACCEPT_TOKEN(aux_sym_mmap_icon_token2);
      if (lookahead == '\n') ADVANCE(584);
      if (lookahead == '\r' ||
          lookahead == ')') ADVANCE(10);
      if (lookahead != 0) ADVANCE(564);
      END_STATE();
    case 565:
      ACCEPT_TOKEN(aux_sym_mmap_icon_token2);
      if (lookahead == '%') ADVANCE(563);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != '\r' &&
          lookahead != ')') ADVANCE(567);
      END_STATE();
    case 566:
      ACCEPT_TOKEN(aux_sym_mmap_icon_token2);
      if (lookahead == '\t' ||
          lookahead == ' ') ADVANCE(566);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != '\r' &&
          lookahead != ')') ADVANCE(567);
      END_STATE();
    case 567:
      ACCEPT_TOKEN(aux_sym_mmap_icon_token2);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != '\r' &&
          lookahead != ')') ADVANCE(567);
      END_STATE();
    case 568:
      ACCEPT_TOKEN(sym__whitespace);
      if (lookahead == '\t') ADVANCE(568);
      if (lookahead == ' ') ADVANCE(572);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          (lookahead < '+' || '-' < lookahead) &&
          lookahead != ':' &&
          lookahead != ';' &&
          lookahead != '>') ADVANCE(687);
      END_STATE();
    case 569:
      ACCEPT_TOKEN(sym__whitespace);
      if (lookahead == ':') ADVANCE(75);
      if (lookahead == '\t' ||
          lookahead == ' ') ADVANCE(569);
      if (('\n' <= lookahead && lookahead <= '\r')) ADVANCE(70);
      END_STATE();
    case 570:
      ACCEPT_TOKEN(sym__whitespace);
      if (lookahead == ':') ADVANCE(77);
      if (lookahead == '\t' ||
          lookahead == ' ') ADVANCE(570);
      if (('\n' <= lookahead && lookahead <= '\r')) ADVANCE(78);
      END_STATE();
    case 571:
      ACCEPT_TOKEN(sym__whitespace);
      if (lookahead == '}') ADVANCE(46);
      if (lookahead == '\t' ||
          lookahead == ' ') ADVANCE(571);
      if (lookahead != 0 &&
          lookahead != '%') ADVANCE(141);
      END_STATE();
    case 572:
      ACCEPT_TOKEN(sym__whitespace);
      if (lookahead == '\t' ||
          lookahead == ' ') ADVANCE(572);
      END_STATE();
    case 573:
      ACCEPT_TOKEN(sym__whitespace);
      if (lookahead == '\t' ||
          lookahead == ' ') ADVANCE(573);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(787);
      END_STATE();
    case 574:
      ACCEPT_TOKEN(sym__whitespace);
      if (lookahead == '\t' ||
          lookahead == ' ') ADVANCE(558);
      if (lookahead != 0 &&
          lookahead != '`') ADVANCE(556);
      END_STATE();
    case 575:
      ACCEPT_TOKEN(sym__whitespace);
      if (lookahead == '\t' ||
          lookahead == ' ') ADVANCE(575);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != '#' &&
          lookahead != ';') ADVANCE(908);
      END_STATE();
    case 576:
      ACCEPT_TOKEN(sym__whitespace);
      if (lookahead == '\t' ||
          lookahead == ' ') ADVANCE(576);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != '\r' &&
          lookahead != '(' &&
          lookahead != ')' &&
          lookahead != '[' &&
          lookahead != ']' &&
          lookahead != '{' &&
          lookahead != '}') ADVANCE(949);
      END_STATE();
    case 577:
      ACCEPT_TOKEN(sym__whitespace);
      if (lookahead == '\t' ||
          lookahead == ' ') ADVANCE(577);
      if (('\n' <= lookahead && lookahead <= '\r')) ADVANCE(420);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(910);
      END_STATE();
    case 578:
      ACCEPT_TOKEN(sym__whitespace);
      if (lookahead == '\t' ||
          lookahead == ' ') ADVANCE(578);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != '#' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(898);
      END_STATE();
    case 579:
      ACCEPT_TOKEN(sym__whitespace);
      if (lookahead == '\t' ||
          lookahead == ' ') ADVANCE(579);
      if (lookahead != 0 &&
          lookahead != '%' &&
          lookahead != ':' &&
          lookahead != '}') ADVANCE(589);
      END_STATE();
    case 580:
      ACCEPT_TOKEN(sym__whitespace);
      if (lookahead == '\t' ||
          lookahead == ' ') ADVANCE(580);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != '#' &&
          lookahead != ';') ADVANCE(597);
      END_STATE();
    case 581:
      ACCEPT_TOKEN(sym__newline);
      if (lookahead == '\n') ADVANCE(581);
      if (lookahead == ':') ADVANCE(75);
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') ADVANCE(70);
      END_STATE();
    case 582:
      ACCEPT_TOKEN(sym__newline);
      if (lookahead == '\n') ADVANCE(582);
      END_STATE();
    case 583:
      ACCEPT_TOKEN(sym__newline);
      if (lookahead == '\n') ADVANCE(583);
      if (lookahead == ':') ADVANCE(77);
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') ADVANCE(78);
      END_STATE();
    case 584:
      ACCEPT_TOKEN(sym_comment);
      END_STATE();
    case 585:
      ACCEPT_TOKEN(sym_comment);
      if (lookahead == '\n') ADVANCE(584);
      if (lookahead != 0) ADVANCE(10);
      END_STATE();
    case 586:
      ACCEPT_TOKEN(sym__md_start);
      END_STATE();
    case 587:
      ACCEPT_TOKEN(sym__md_end);
      END_STATE();
    case 588:
      ACCEPT_TOKEN(sym_type_directive);
      if (lookahead == '%') ADVANCE(30);
      if (lookahead == '\t' ||
          lookahead == ' ') ADVANCE(579);
      if (lookahead != 0 &&
          lookahead != ':' &&
          lookahead != '}') ADVANCE(589);
      END_STATE();
    case 589:
      ACCEPT_TOKEN(sym_type_directive);
      if (lookahead != 0 &&
          lookahead != '%' &&
          lookahead != ':' &&
          lookahead != '}') ADVANCE(589);
      END_STATE();
    case 590:
      ACCEPT_TOKEN(aux_sym_direction_tb_token1);
      END_STATE();
    case 591:
      ACCEPT_TOKEN(aux_sym_direction_bt_token1);
      END_STATE();
    case 592:
      ACCEPT_TOKEN(aux_sym_direction_rl_token1);
      END_STATE();
    case 593:
      ACCEPT_TOKEN(aux_sym_direction_lr_token1);
      END_STATE();
    case 594:
      ACCEPT_TOKEN(aux_sym__sequence_rest_text_token1);
      if (lookahead == '\n') ADVANCE(585);
      if (lookahead == '{') ADVANCE(597);
      if (lookahead == '#' ||
          lookahead == ';') ADVANCE(10);
      if (lookahead != 0) ADVANCE(595);
      END_STATE();
    case 595:
      ACCEPT_TOKEN(aux_sym__sequence_rest_text_token1);
      if (lookahead == '\n') ADVANCE(584);
      if (lookahead == '#' ||
          lookahead == ';') ADVANCE(10);
      if (lookahead != 0) ADVANCE(595);
      END_STATE();
    case 596:
      ACCEPT_TOKEN(aux_sym__sequence_rest_text_token1);
      if (lookahead == '%') ADVANCE(594);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != '#' &&
          lookahead != ';') ADVANCE(597);
      END_STATE();
    case 597:
      ACCEPT_TOKEN(aux_sym__sequence_rest_text_token1);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != '#' &&
          lookahead != ';') ADVANCE(597);
      END_STATE();
    case 598:
      ACCEPT_TOKEN(sym__sequence_actor_word);
      if (lookahead == '\n') ADVANCE(585);
      if (lookahead == '{') ADVANCE(687);
      if (lookahead == ' ' ||
          ('+' <= lookahead && lookahead <= '-') ||
          lookahead == ':' ||
          lookahead == ';' ||
          lookahead == '>') ADVANCE(10);
      if (lookahead != 0) ADVANCE(599);
      END_STATE();
    case 599:
      ACCEPT_TOKEN(sym__sequence_actor_word);
      if (lookahead == '\n') ADVANCE(584);
      if (lookahead == ' ' ||
          ('+' <= lookahead && lookahead <= '-') ||
          lookahead == ':' ||
          lookahead == ';' ||
          lookahead == '>') ADVANCE(10);
      if (lookahead != 0) ADVANCE(599);
      END_STATE();
    case 600:
      ACCEPT_TOKEN(sym__sequence_actor_word);
      if (lookahead == ' ') ADVANCE(455);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          (lookahead < '+' || '-' < lookahead) &&
          lookahead != ':' &&
          lookahead != ';' &&
          lookahead != '>') ADVANCE(687);
      END_STATE();
    case 601:
      ACCEPT_TOKEN(sym__sequence_actor_word);
      if (lookahead == '%') ADVANCE(598);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ' ' &&
          (lookahead < '+' || '-' < lookahead) &&
          lookahead != ':' &&
          lookahead != ';' &&
          lookahead != '>') ADVANCE(687);
      END_STATE();
    case 602:
      ACCEPT_TOKEN(sym__sequence_actor_word);
      if (lookahead == '_') ADVANCE(608);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ' ' &&
          (lookahead < '+' || '-' < lookahead) &&
          lookahead != ':' &&
          lookahead != ';' &&
          lookahead != '>') ADVANCE(687);
      END_STATE();
    case 603:
      ACCEPT_TOKEN(sym__sequence_actor_word);
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(664);
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(653);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ' ' &&
          (lookahead < '+' || '-' < lookahead) &&
          lookahead != ':' &&
          lookahead != ';' &&
          lookahead != '>') ADVANCE(687);
      END_STATE();
    case 604:
      ACCEPT_TOKEN(sym__sequence_actor_word);
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(636);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ' ' &&
          (lookahead < '+' || '-' < lookahead) &&
          lookahead != ':' &&
          lookahead != ';' &&
          lookahead != '>') ADVANCE(687);
      END_STATE();
    case 605:
      ACCEPT_TOKEN(sym__sequence_actor_word);
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(650);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ' ' &&
          (lookahead < '+' || '-' < lookahead) &&
          lookahead != ':' &&
          lookahead != ';' &&
          lookahead != '>') ADVANCE(687);
      END_STATE();
    case 606:
      ACCEPT_TOKEN(sym__sequence_actor_word);
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(615);
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(604);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ' ' &&
          (lookahead < '+' || '-' < lookahead) &&
          lookahead != ':' &&
          lookahead != ';' &&
          lookahead != '>') ADVANCE(687);
      END_STATE();
    case 607:
      ACCEPT_TOKEN(sym__sequence_actor_word);
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(681);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ' ' &&
          (lookahead < '+' || '-' < lookahead) &&
          lookahead != ':' &&
          lookahead != ';' &&
          lookahead != '>') ADVANCE(687);
      END_STATE();
    case 608:
      ACCEPT_TOKEN(sym__sequence_actor_word);
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(617);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ' ' &&
          (lookahead < '+' || '-' < lookahead) &&
          lookahead != ':' &&
          lookahead != ';' &&
          lookahead != '>') ADVANCE(687);
      END_STATE();
    case 609:
      ACCEPT_TOKEN(sym__sequence_actor_word);
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(682);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ' ' &&
          (lookahead < '+' || '-' < lookahead) &&
          lookahead != ':' &&
          lookahead != ';' &&
          lookahead != '>') ADVANCE(687);
      END_STATE();
    case 610:
      ACCEPT_TOKEN(sym__sequence_actor_word);
      if (lookahead == 'B' ||
          lookahead == 'b') ADVANCE(632);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ' ' &&
          (lookahead < '+' || '-' < lookahead) &&
          lookahead != ':' &&
          lookahead != ';' &&
          lookahead != '>') ADVANCE(687);
      END_STATE();
    case 611:
      ACCEPT_TOKEN(sym__sequence_actor_word);
      if (lookahead == 'C' ||
          lookahead == 'c') ADVANCE(675);
      if (lookahead == 'L' ||
          lookahead == 'l') ADVANCE(670);
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(619);
      if (lookahead == 'U' ||
          lookahead == 'u') ADVANCE(676);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ' ' &&
          (lookahead < '+' || '-' < lookahead) &&
          lookahead != ':' &&
          lookahead != ';' &&
          lookahead != '>') ADVANCE(687);
      END_STATE();
    case 612:
      ACCEPT_TOKEN(sym__sequence_actor_word);
      if (lookahead == 'C' ||
          lookahead == 'c') ADVANCE(675);
      if (lookahead == 'L' ||
          lookahead == 'l') ADVANCE(670);
      if (lookahead == 'U' ||
          lookahead == 'u') ADVANCE(676);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ' ' &&
          (lookahead < '+' || '-' < lookahead) &&
          lookahead != ':' &&
          lookahead != ';' &&
          lookahead != '>') ADVANCE(687);
      END_STATE();
    case 613:
      ACCEPT_TOKEN(sym__sequence_actor_word);
      if (lookahead == 'C' ||
          lookahead == 'c') ADVANCE(637);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ' ' &&
          (lookahead < '+' || '-' < lookahead) &&
          lookahead != ':' &&
          lookahead != ';' &&
          lookahead != '>') ADVANCE(687);
      END_STATE();
    case 614:
      ACCEPT_TOKEN(sym__sequence_actor_word);
      if (lookahead == 'C' ||
          lookahead == 'c') ADVANCE(671);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ' ' &&
          (lookahead < '+' || '-' < lookahead) &&
          lookahead != ':' &&
          lookahead != ';' &&
          lookahead != '>') ADVANCE(687);
      END_STATE();
    case 615:
      ACCEPT_TOKEN(sym__sequence_actor_word);
      if (lookahead == 'C' ||
          lookahead == 'c') ADVANCE(678);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ' ' &&
          (lookahead < '+' || '-' < lookahead) &&
          lookahead != ':' &&
          lookahead != ';' &&
          lookahead != '>') ADVANCE(687);
      END_STATE();
    case 616:
      ACCEPT_TOKEN(sym__sequence_actor_word);
      if (lookahead == 'C' ||
          lookahead == 'c') ADVANCE(628);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ' ' &&
          (lookahead < '+' || '-' < lookahead) &&
          lookahead != ':' &&
          lookahead != ';' &&
          lookahead != '>') ADVANCE(687);
      END_STATE();
    case 617:
      ACCEPT_TOKEN(sym__sequence_actor_word);
      if (lookahead == 'C' ||
          lookahead == 'c') ADVANCE(677);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ' ' &&
          (lookahead < '+' || '-' < lookahead) &&
          lookahead != ':' &&
          lookahead != ';' &&
          lookahead != '>') ADVANCE(687);
      END_STATE();
    case 618:
      ACCEPT_TOKEN(sym__sequence_actor_word);
      if (lookahead == 'D' ||
          lookahead == 'd') ADVANCE(464);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ' ' &&
          (lookahead < '+' || '-' < lookahead) &&
          lookahead != ':' &&
          lookahead != ';' &&
          lookahead != '>') ADVANCE(687);
      END_STATE();
    case 619:
      ACCEPT_TOKEN(sym__sequence_actor_word);
      if (lookahead == 'D' ||
          lookahead == 'd') ADVANCE(471);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ' ' &&
          (lookahead < '+' || '-' < lookahead) &&
          lookahead != ':' &&
          lookahead != ';' &&
          lookahead != '>') ADVANCE(687);
      END_STATE();
    case 620:
      ACCEPT_TOKEN(sym__sequence_actor_word);
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(468);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ' ' &&
          (lookahead < '+' || '-' < lookahead) &&
          lookahead != ':' &&
          lookahead != ';' &&
          lookahead != '>') ADVANCE(687);
      END_STATE();
    case 621:
      ACCEPT_TOKEN(sym__sequence_actor_word);
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(462);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ' ' &&
          (lookahead < '+' || '-' < lookahead) &&
          lookahead != ':' &&
          lookahead != ';' &&
          lookahead != '>') ADVANCE(687);
      END_STATE();
    case 622:
      ACCEPT_TOKEN(sym__sequence_actor_word);
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(453);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ' ' &&
          (lookahead < '+' || '-' < lookahead) &&
          lookahead != ':' &&
          lookahead != ';' &&
          lookahead != '>') ADVANCE(687);
      END_STATE();
    case 623:
      ACCEPT_TOKEN(sym__sequence_actor_word);
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(454);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ' ' &&
          (lookahead < '+' || '-' < lookahead) &&
          lookahead != ':' &&
          lookahead != ';' &&
          lookahead != '>') ADVANCE(687);
      END_STATE();
    case 624:
      ACCEPT_TOKEN(sym__sequence_actor_word);
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(606);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ' ' &&
          (lookahead < '+' || '-' < lookahead) &&
          lookahead != ':' &&
          lookahead != ';' &&
          lookahead != '>') ADVANCE(687);
      END_STATE();
    case 625:
      ACCEPT_TOKEN(sym__sequence_actor_word);
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(614);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ' ' &&
          (lookahead < '+' || '-' < lookahead) &&
          lookahead != ':' &&
          lookahead != ';' &&
          lookahead != '>') ADVANCE(687);
      END_STATE();
    case 626:
      ACCEPT_TOKEN(sym__sequence_actor_word);
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(660);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ' ' &&
          (lookahead < '+' || '-' < lookahead) &&
          lookahead != ':' &&
          lookahead != ';' &&
          lookahead != '>') ADVANCE(687);
      END_STATE();
    case 627:
      ACCEPT_TOKEN(sym__sequence_actor_word);
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(600);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ' ' &&
          (lookahead < '+' || '-' < lookahead) &&
          lookahead != ':' &&
          lookahead != ';' &&
          lookahead != '>') ADVANCE(687);
      END_STATE();
    case 628:
      ACCEPT_TOKEN(sym__sequence_actor_word);
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(602);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ' ' &&
          (lookahead < '+' || '-' < lookahead) &&
          lookahead != ':' &&
          lookahead != ';' &&
          lookahead != '>') ADVANCE(687);
      END_STATE();
    case 629:
      ACCEPT_TOKEN(sym__sequence_actor_word);
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(668);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ' ' &&
          (lookahead < '+' || '-' < lookahead) &&
          lookahead != ':' &&
          lookahead != ';' &&
          lookahead != '>') ADVANCE(687);
      END_STATE();
    case 630:
      ACCEPT_TOKEN(sym__sequence_actor_word);
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(649);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ' ' &&
          (lookahead < '+' || '-' < lookahead) &&
          lookahead != ':' &&
          lookahead != ';' &&
          lookahead != '>') ADVANCE(687);
      END_STATE();
    case 631:
      ACCEPT_TOKEN(sym__sequence_actor_word);
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(665);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ' ' &&
          (lookahead < '+' || '-' < lookahead) &&
          lookahead != ':' &&
          lookahead != ';' &&
          lookahead != '>') ADVANCE(687);
      END_STATE();
    case 632:
      ACCEPT_TOKEN(sym__sequence_actor_word);
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(662);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ' ' &&
          (lookahead < '+' || '-' < lookahead) &&
          lookahead != ':' &&
          lookahead != ';' &&
          lookahead != '>') ADVANCE(687);
      END_STATE();
    case 633:
      ACCEPT_TOKEN(sym__sequence_actor_word);
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(647);
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(651);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ' ' &&
          (lookahead < '+' || '-' < lookahead) &&
          lookahead != ':' &&
          lookahead != ';' &&
          lookahead != '>') ADVANCE(687);
      END_STATE();
    case 634:
      ACCEPT_TOKEN(sym__sequence_actor_word);
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(685);
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(661);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ' ' &&
          (lookahead < '+' || '-' < lookahead) &&
          lookahead != ':' &&
          lookahead != ';' &&
          lookahead != '>') ADVANCE(687);
      END_STATE();
    case 635:
      ACCEPT_TOKEN(sym__sequence_actor_word);
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(613);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ' ' &&
          (lookahead < '+' || '-' < lookahead) &&
          lookahead != ':' &&
          lookahead != ';' &&
          lookahead != '>') ADVANCE(687);
      END_STATE();
    case 636:
      ACCEPT_TOKEN(sym__sequence_actor_word);
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(643);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ' ' &&
          (lookahead < '+' || '-' < lookahead) &&
          lookahead != ':' &&
          lookahead != ';' &&
          lookahead != '>') ADVANCE(687);
      END_STATE();
    case 637:
      ACCEPT_TOKEN(sym__sequence_actor_word);
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(657);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ' ' &&
          (lookahead < '+' || '-' < lookahead) &&
          lookahead != ':' &&
          lookahead != ';' &&
          lookahead != '>') ADVANCE(687);
      END_STATE();
    case 638:
      ACCEPT_TOKEN(sym__sequence_actor_word);
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(674);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ' ' &&
          (lookahead < '+' || '-' < lookahead) &&
          lookahead != ':' &&
          lookahead != ';' &&
          lookahead != '>') ADVANCE(687);
      END_STATE();
    case 639:
      ACCEPT_TOKEN(sym__sequence_actor_word);
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(629);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ' ' &&
          (lookahead < '+' || '-' < lookahead) &&
          lookahead != ':' &&
          lookahead != ';' &&
          lookahead != '>') ADVANCE(687);
      END_STATE();
    case 640:
      ACCEPT_TOKEN(sym__sequence_actor_word);
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(686);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ' ' &&
          (lookahead < '+' || '-' < lookahead) &&
          lookahead != ':' &&
          lookahead != ';' &&
          lookahead != '>') ADVANCE(687);
      END_STATE();
    case 641:
      ACCEPT_TOKEN(sym__sequence_actor_word);
      if (lookahead == 'K' ||
          lookahead == 'k') ADVANCE(459);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ' ' &&
          (lookahead < '+' || '-' < lookahead) &&
          lookahead != ':' &&
          lookahead != ';' &&
          lookahead != '>') ADVANCE(687);
      END_STATE();
    case 642:
      ACCEPT_TOKEN(sym__sequence_actor_word);
      if (lookahead == 'L' ||
          lookahead == 'l') ADVANCE(669);
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(618);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ' ' &&
          (lookahead < '+' || '-' < lookahead) &&
          lookahead != ':' &&
          lookahead != ';' &&
          lookahead != '>') ADVANCE(687);
      END_STATE();
    case 643:
      ACCEPT_TOKEN(sym__sequence_actor_word);
      if (lookahead == 'L' ||
          lookahead == 'l') ADVANCE(667);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ' ' &&
          (lookahead < '+' || '-' < lookahead) &&
          lookahead != ':' &&
          lookahead != ';' &&
          lookahead != '>') ADVANCE(687);
      END_STATE();
    case 644:
      ACCEPT_TOKEN(sym__sequence_actor_word);
      if (lookahead == 'L' ||
          lookahead == 'l') ADVANCE(621);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ' ' &&
          (lookahead < '+' || '-' < lookahead) &&
          lookahead != ':' &&
          lookahead != ';' &&
          lookahead != '>') ADVANCE(687);
      END_STATE();
    case 645:
      ACCEPT_TOKEN(sym__sequence_actor_word);
      if (lookahead == 'M' ||
          lookahead == 'm') ADVANCE(610);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ' ' &&
          (lookahead < '+' || '-' < lookahead) &&
          lookahead != ':' &&
          lookahead != ';' &&
          lookahead != '>') ADVANCE(687);
      END_STATE();
    case 646:
      ACCEPT_TOKEN(sym__sequence_actor_word);
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(618);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ' ' &&
          (lookahead < '+' || '-' < lookahead) &&
          lookahead != ':' &&
          lookahead != ';' &&
          lookahead != '>') ADVANCE(687);
      END_STATE();
    case 647:
      ACCEPT_TOKEN(sym__sequence_actor_word);
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(641);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ' ' &&
          (lookahead < '+' || '-' < lookahead) &&
          lookahead != ':' &&
          lookahead != ';' &&
          lookahead != '>') ADVANCE(687);
      END_STATE();
    case 648:
      ACCEPT_TOKEN(sym__sequence_actor_word);
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(683);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ' ' &&
          (lookahead < '+' || '-' < lookahead) &&
          lookahead != ':' &&
          lookahead != ';' &&
          lookahead != '>') ADVANCE(687);
      END_STATE();
    case 649:
      ACCEPT_TOKEN(sym__sequence_actor_word);
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(616);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ' ' &&
          (lookahead < '+' || '-' < lookahead) &&
          lookahead != ':' &&
          lookahead != ';' &&
          lookahead != '>') ADVANCE(687);
      END_STATE();
    case 650:
      ACCEPT_TOKEN(sym__sequence_actor_word);
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(672);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ' ' &&
          (lookahead < '+' || '-' < lookahead) &&
          lookahead != ':' &&
          lookahead != ';' &&
          lookahead != '>') ADVANCE(687);
      END_STATE();
    case 651:
      ACCEPT_TOKEN(sym__sequence_actor_word);
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(656);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ' ' &&
          (lookahead < '+' || '-' < lookahead) &&
          lookahead != ':' &&
          lookahead != ';' &&
          lookahead != '>') ADVANCE(687);
      END_STATE();
    case 652:
      ACCEPT_TOKEN(sym__sequence_actor_word);
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(648);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ' ' &&
          (lookahead < '+' || '-' < lookahead) &&
          lookahead != ':' &&
          lookahead != ';' &&
          lookahead != '>') ADVANCE(687);
      END_STATE();
    case 653:
      ACCEPT_TOKEN(sym__sequence_actor_word);
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(659);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ' ' &&
          (lookahead < '+' || '-' < lookahead) &&
          lookahead != ':' &&
          lookahead != ';' &&
          lookahead != '>') ADVANCE(687);
      END_STATE();
    case 654:
      ACCEPT_TOKEN(sym__sequence_actor_word);
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(679);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ' ' &&
          (lookahead < '+' || '-' < lookahead) &&
          lookahead != ':' &&
          lookahead != ';' &&
          lookahead != '>') ADVANCE(687);
      END_STATE();
    case 655:
      ACCEPT_TOKEN(sym__sequence_actor_word);
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(663);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ' ' &&
          (lookahead < '+' || '-' < lookahead) &&
          lookahead != ':' &&
          lookahead != ';' &&
          lookahead != '>') ADVANCE(687);
      END_STATE();
    case 656:
      ACCEPT_TOKEN(sym__sequence_actor_word);
      if (lookahead == 'P' ||
          lookahead == 'p') ADVANCE(463);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ' ' &&
          (lookahead < '+' || '-' < lookahead) &&
          lookahead != ':' &&
          lookahead != ';' &&
          lookahead != '>') ADVANCE(687);
      END_STATE();
    case 657:
      ACCEPT_TOKEN(sym__sequence_actor_word);
      if (lookahead == 'P' ||
          lookahead == 'p') ADVANCE(605);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ' ' &&
          (lookahead < '+' || '-' < lookahead) &&
          lookahead != ':' &&
          lookahead != ';' &&
          lookahead != '>') ADVANCE(687);
      END_STATE();
    case 658:
      ACCEPT_TOKEN(sym__sequence_actor_word);
      if (lookahead == 'P' ||
          lookahead == 'p') ADVANCE(673);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ' ' &&
          (lookahead < '+' || '-' < lookahead) &&
          lookahead != ':' &&
          lookahead != ';' &&
          lookahead != '>') ADVANCE(687);
      END_STATE();
    case 659:
      ACCEPT_TOKEN(sym__sequence_actor_word);
      if (lookahead == 'P' ||
          lookahead == 'p') ADVANCE(631);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ' ' &&
          (lookahead < '+' || '-' < lookahead) &&
          lookahead != ':' &&
          lookahead != ';' &&
          lookahead != '>') ADVANCE(687);
      END_STATE();
    case 660:
      ACCEPT_TOKEN(sym__sequence_actor_word);
      if (lookahead == 'Q' ||
          lookahead == 'q') ADVANCE(684);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ' ' &&
          (lookahead < '+' || '-' < lookahead) &&
          lookahead != ':' &&
          lookahead != ';' &&
          lookahead != '>') ADVANCE(687);
      END_STATE();
    case 661:
      ACCEPT_TOKEN(sym__sequence_actor_word);
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(447);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ' ' &&
          (lookahead < '+' || '-' < lookahead) &&
          lookahead != ':' &&
          lookahead != ';' &&
          lookahead != '>') ADVANCE(687);
      END_STATE();
    case 662:
      ACCEPT_TOKEN(sym__sequence_actor_word);
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(452);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ' ' &&
          (lookahead < '+' || '-' < lookahead) &&
          lookahead != ':' &&
          lookahead != ';' &&
          lookahead != '>') ADVANCE(687);
      END_STATE();
    case 663:
      ACCEPT_TOKEN(sym__sequence_actor_word);
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(449);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ' ' &&
          (lookahead < '+' || '-' < lookahead) &&
          lookahead != ':' &&
          lookahead != ';' &&
          lookahead != '>') ADVANCE(687);
      END_STATE();
    case 664:
      ACCEPT_TOKEN(sym__sequence_actor_word);
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(469);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ' ' &&
          (lookahead < '+' || '-' < lookahead) &&
          lookahead != ':' &&
          lookahead != ';' &&
          lookahead != '>') ADVANCE(687);
      END_STATE();
    case 665:
      ACCEPT_TOKEN(sym__sequence_actor_word);
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(680);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ' ' &&
          (lookahead < '+' || '-' < lookahead) &&
          lookahead != ':' &&
          lookahead != ';' &&
          lookahead != '>') ADVANCE(687);
      END_STATE();
    case 666:
      ACCEPT_TOKEN(sym__sequence_actor_word);
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(448);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ' ' &&
          (lookahead < '+' || '-' < lookahead) &&
          lookahead != ':' &&
          lookahead != ';' &&
          lookahead != '>') ADVANCE(687);
      END_STATE();
    case 667:
      ACCEPT_TOKEN(sym__sequence_actor_word);
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(461);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ' ' &&
          (lookahead < '+' || '-' < lookahead) &&
          lookahead != ':' &&
          lookahead != ';' &&
          lookahead != '>') ADVANCE(687);
      END_STATE();
    case 668:
      ACCEPT_TOKEN(sym__sequence_actor_word);
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(460);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ' ' &&
          (lookahead < '+' || '-' < lookahead) &&
          lookahead != ':' &&
          lookahead != ';' &&
          lookahead != '>') ADVANCE(687);
      END_STATE();
    case 669:
      ACCEPT_TOKEN(sym__sequence_actor_word);
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(620);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ' ' &&
          (lookahead < '+' || '-' < lookahead) &&
          lookahead != ':' &&
          lookahead != ';' &&
          lookahead != '>') ADVANCE(687);
      END_STATE();
    case 670:
      ACCEPT_TOKEN(sym__sequence_actor_word);
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(467);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ' ' &&
          (lookahead < '+' || '-' < lookahead) &&
          lookahead != ':' &&
          lookahead != ';' &&
          lookahead != '>') ADVANCE(687);
      END_STATE();
    case 671:
      ACCEPT_TOKEN(sym__sequence_actor_word);
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(465);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ' ' &&
          (lookahead < '+' || '-' < lookahead) &&
          lookahead != ':' &&
          lookahead != ';' &&
          lookahead != '>') ADVANCE(687);
      END_STATE();
    case 672:
      ACCEPT_TOKEN(sym__sequence_actor_word);
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(446);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ' ' &&
          (lookahead < '+' || '-' < lookahead) &&
          lookahead != ':' &&
          lookahead != ';' &&
          lookahead != '>') ADVANCE(687);
      END_STATE();
    case 673:
      ACCEPT_TOKEN(sym__sequence_actor_word);
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(466);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ' ' &&
          (lookahead < '+' || '-' < lookahead) &&
          lookahead != ':' &&
          lookahead != ';' &&
          lookahead != '>') ADVANCE(687);
      END_STATE();
    case 674:
      ACCEPT_TOKEN(sym__sequence_actor_word);
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(644);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ' ' &&
          (lookahead < '+' || '-' < lookahead) &&
          lookahead != ':' &&
          lookahead != ';' &&
          lookahead != '>') ADVANCE(687);
      END_STATE();
    case 675:
      ACCEPT_TOKEN(sym__sequence_actor_word);
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(634);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ' ' &&
          (lookahead < '+' || '-' < lookahead) &&
          lookahead != ':' &&
          lookahead != ';' &&
          lookahead != '>') ADVANCE(687);
      END_STATE();
    case 676:
      ACCEPT_TOKEN(sym__sequence_actor_word);
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(652);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ' ' &&
          (lookahead < '+' || '-' < lookahead) &&
          lookahead != ':' &&
          lookahead != ';' &&
          lookahead != '>') ADVANCE(687);
      END_STATE();
    case 677:
      ACCEPT_TOKEN(sym__sequence_actor_word);
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(655);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ' ' &&
          (lookahead < '+' || '-' < lookahead) &&
          lookahead != ':' &&
          lookahead != ';' &&
          lookahead != '>') ADVANCE(687);
      END_STATE();
    case 678:
      ACCEPT_TOKEN(sym__sequence_actor_word);
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(640);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ' ' &&
          (lookahead < '+' || '-' < lookahead) &&
          lookahead != ':' &&
          lookahead != ';' &&
          lookahead != '>') ADVANCE(687);
      END_STATE();
    case 679:
      ACCEPT_TOKEN(sym__sequence_actor_word);
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(627);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ' ' &&
          (lookahead < '+' || '-' < lookahead) &&
          lookahead != ':' &&
          lookahead != ';' &&
          lookahead != '>') ADVANCE(687);
      END_STATE();
    case 680:
      ACCEPT_TOKEN(sym__sequence_actor_word);
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(639);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ' ' &&
          (lookahead < '+' || '-' < lookahead) &&
          lookahead != ':' &&
          lookahead != ';' &&
          lookahead != '>') ADVANCE(687);
      END_STATE();
    case 681:
      ACCEPT_TOKEN(sym__sequence_actor_word);
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(622);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ' ' &&
          (lookahead < '+' || '-' < lookahead) &&
          lookahead != ':' &&
          lookahead != ';' &&
          lookahead != '>') ADVANCE(687);
      END_STATE();
    case 682:
      ACCEPT_TOKEN(sym__sequence_actor_word);
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(623);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ' ' &&
          (lookahead < '+' || '-' < lookahead) &&
          lookahead != ':' &&
          lookahead != ';' &&
          lookahead != '>') ADVANCE(687);
      END_STATE();
    case 683:
      ACCEPT_TOKEN(sym__sequence_actor_word);
      if (lookahead == 'U' ||
          lookahead == 'u') ADVANCE(645);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ' ' &&
          (lookahead < '+' || '-' < lookahead) &&
          lookahead != ':' &&
          lookahead != ';' &&
          lookahead != '>') ADVANCE(687);
      END_STATE();
    case 684:
      ACCEPT_TOKEN(sym__sequence_actor_word);
      if (lookahead == 'U' ||
          lookahead == 'u') ADVANCE(630);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ' ' &&
          (lookahead < '+' || '-' < lookahead) &&
          lookahead != ':' &&
          lookahead != ';' &&
          lookahead != '>') ADVANCE(687);
      END_STATE();
    case 685:
      ACCEPT_TOKEN(sym__sequence_actor_word);
      if (lookahead == 'V' ||
          lookahead == 'v') ADVANCE(607);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ' ' &&
          (lookahead < '+' || '-' < lookahead) &&
          lookahead != ':' &&
          lookahead != ';' &&
          lookahead != '>') ADVANCE(687);
      END_STATE();
    case 686:
      ACCEPT_TOKEN(sym__sequence_actor_word);
      if (lookahead == 'V' ||
          lookahead == 'v') ADVANCE(609);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ' ' &&
          (lookahead < '+' || '-' < lookahead) &&
          lookahead != ':' &&
          lookahead != ';' &&
          lookahead != '>') ADVANCE(687);
      END_STATE();
    case 687:
      ACCEPT_TOKEN(sym__sequence_actor_word);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ' ' &&
          (lookahead < '+' || '-' < lookahead) &&
          lookahead != ':' &&
          lookahead != ';' &&
          lookahead != '>') ADVANCE(687);
      END_STATE();
    case 688:
      ACCEPT_TOKEN(sym_solid_arrow);
      END_STATE();
    case 689:
      ACCEPT_TOKEN(sym_dotted_arrow);
      END_STATE();
    case 690:
      ACCEPT_TOKEN(sym_solid_open_arrow);
      if (lookahead == '>') ADVANCE(688);
      END_STATE();
    case 691:
      ACCEPT_TOKEN(anon_sym_DASH_DASH_GT);
      END_STATE();
    case 692:
      ACCEPT_TOKEN(anon_sym_DASH_DASH_GT);
      if (lookahead == '>') ADVANCE(689);
      END_STATE();
    case 693:
      ACCEPT_TOKEN(sym_solid_cross);
      END_STATE();
    case 694:
      ACCEPT_TOKEN(sym_dotted_cross);
      END_STATE();
    case 695:
      ACCEPT_TOKEN(sym_dotted_point);
      END_STATE();
    case 696:
      ACCEPT_TOKEN(aux_sym_note_placement_left_token1);
      END_STATE();
    case 697:
      ACCEPT_TOKEN(aux_sym_note_placement_right_token1);
      END_STATE();
    case 698:
      ACCEPT_TOKEN(sym__class_name);
      if (lookahead == 'a') ADVANCE(701);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('b' <= lookahead && lookahead <= 'z')) ADVANCE(702);
      END_STATE();
    case 699:
      ACCEPT_TOKEN(sym__class_name);
      if (lookahead == 'l') ADVANCE(698);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(702);
      END_STATE();
    case 700:
      ACCEPT_TOKEN(sym__class_name);
      if (lookahead == 's') ADVANCE(482);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(702);
      END_STATE();
    case 701:
      ACCEPT_TOKEN(sym__class_name);
      if (lookahead == 's') ADVANCE(700);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(702);
      END_STATE();
    case 702:
      ACCEPT_TOKEN(sym__class_name);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(702);
      END_STATE();
    case 703:
      ACCEPT_TOKEN(sym__alpha_num_token);
      if (lookahead == '-') ADVANCE(63);
      if (lookahead == '.') ADVANCE(58);
      if (lookahead == '=') ADVANCE(84);
      if (lookahead == '!' ||
          ('0' <= lookahead && lookahead <= '9') ||
          lookahead == '?' ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z') ||
          lookahead == '~') ADVANCE(721);
      END_STATE();
    case 704:
      ACCEPT_TOKEN(sym__alpha_num_token);
      if (lookahead == 'a') ADVANCE(710);
      if (lookahead == '!' ||
          ('0' <= lookahead && lookahead <= '9') ||
          lookahead == '?' ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('b' <= lookahead && lookahead <= 'z') ||
          lookahead == '~') ADVANCE(721);
      END_STATE();
    case 705:
      ACCEPT_TOKEN(sym__alpha_num_token);
      if (lookahead == 'b') ADVANCE(707);
      if (lookahead == '!' ||
          ('0' <= lookahead && lookahead <= '9') ||
          lookahead == '?' ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z') ||
          lookahead == '~') ADVANCE(721);
      END_STATE();
    case 706:
      ACCEPT_TOKEN(sym__alpha_num_token);
      if (lookahead == 'd') ADVANCE(548);
      if (lookahead == '!' ||
          ('0' <= lookahead && lookahead <= '9') ||
          lookahead == '?' ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z') ||
          lookahead == '~') ADVANCE(721);
      END_STATE();
    case 707:
      ACCEPT_TOKEN(sym__alpha_num_token);
      if (lookahead == 'g') ADVANCE(711);
      if (lookahead == '!' ||
          ('0' <= lookahead && lookahead <= '9') ||
          lookahead == '?' ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z') ||
          lookahead == '~') ADVANCE(721);
      END_STATE();
    case 708:
      ACCEPT_TOKEN(sym__alpha_num_token);
      if (lookahead == 'h') ADVANCE(546);
      if (lookahead == '!' ||
          ('0' <= lookahead && lookahead <= '9') ||
          lookahead == '?' ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z') ||
          lookahead == '~') ADVANCE(721);
      END_STATE();
    case 709:
      ACCEPT_TOKEN(sym__alpha_num_token);
      if (lookahead == 'n') ADVANCE(706);
      if (lookahead == '!' ||
          ('0' <= lookahead && lookahead <= '9') ||
          lookahead == '?' ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z') ||
          lookahead == '~') ADVANCE(721);
      END_STATE();
    case 710:
      ACCEPT_TOKEN(sym__alpha_num_token);
      if (lookahead == 'p') ADVANCE(708);
      if (lookahead == '!' ||
          ('0' <= lookahead && lookahead <= '9') ||
          lookahead == '?' ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z') ||
          lookahead == '~') ADVANCE(721);
      END_STATE();
    case 711:
      ACCEPT_TOKEN(sym__alpha_num_token);
      if (lookahead == 'r') ADVANCE(704);
      if (lookahead == '!' ||
          ('0' <= lookahead && lookahead <= '9') ||
          lookahead == '?' ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z') ||
          lookahead == '~') ADVANCE(721);
      END_STATE();
    case 712:
      ACCEPT_TOKEN(sym__alpha_num_token);
      if (lookahead == 'u') ADVANCE(705);
      if (lookahead == '!' ||
          ('0' <= lookahead && lookahead <= '9') ||
          lookahead == '?' ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z') ||
          lookahead == '~') ADVANCE(721);
      END_STATE();
    case 713:
      ACCEPT_TOKEN(sym__alpha_num_token);
      if (lookahead == 'C' ||
          lookahead == 'c') ADVANCE(720);
      if (lookahead == '!' ||
          ('0' <= lookahead && lookahead <= '9') ||
          lookahead == '?' ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z') ||
          lookahead == '~') ADVANCE(721);
      END_STATE();
    case 714:
      ACCEPT_TOKEN(sym__alpha_num_token);
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(713);
      if (lookahead == '!' ||
          ('0' <= lookahead && lookahead <= '9') ||
          lookahead == '?' ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z') ||
          lookahead == '~') ADVANCE(721);
      END_STATE();
    case 715:
      ACCEPT_TOKEN(sym__alpha_num_token);
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(719);
      if (lookahead == '!' ||
          ('0' <= lookahead && lookahead <= '9') ||
          lookahead == '?' ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z') ||
          lookahead == '~') ADVANCE(721);
      END_STATE();
    case 716:
      ACCEPT_TOKEN(sym__alpha_num_token);
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(718);
      if (lookahead == '!' ||
          ('0' <= lookahead && lookahead <= '9') ||
          lookahead == '?' ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z') ||
          lookahead == '~') ADVANCE(721);
      END_STATE();
    case 717:
      ACCEPT_TOKEN(sym__alpha_num_token);
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(517);
      if (lookahead == '!' ||
          ('0' <= lookahead && lookahead <= '9') ||
          lookahead == '?' ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z') ||
          lookahead == '~') ADVANCE(721);
      END_STATE();
    case 718:
      ACCEPT_TOKEN(sym__alpha_num_token);
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(717);
      if (lookahead == '!' ||
          ('0' <= lookahead && lookahead <= '9') ||
          lookahead == '?' ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z') ||
          lookahead == '~') ADVANCE(721);
      END_STATE();
    case 719:
      ACCEPT_TOKEN(sym__alpha_num_token);
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(714);
      if (lookahead == '!' ||
          ('0' <= lookahead && lookahead <= '9') ||
          lookahead == '?' ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z') ||
          lookahead == '~') ADVANCE(721);
      END_STATE();
    case 720:
      ACCEPT_TOKEN(sym__alpha_num_token);
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(716);
      if (lookahead == '!' ||
          ('0' <= lookahead && lookahead <= '9') ||
          lookahead == '?' ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z') ||
          lookahead == '~') ADVANCE(721);
      END_STATE();
    case 721:
      ACCEPT_TOKEN(sym__alpha_num_token);
      if (lookahead == '!' ||
          ('0' <= lookahead && lookahead <= '9') ||
          lookahead == '?' ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z') ||
          lookahead == '~') ADVANCE(721);
      END_STATE();
    case 722:
      ACCEPT_TOKEN(sym__bquote_string);
      END_STATE();
    case 723:
      ACCEPT_TOKEN(sym__dquote_string);
      END_STATE();
    case 724:
      ACCEPT_TOKEN(sym_class_reltype_aggregation);
      END_STATE();
    case 725:
      ACCEPT_TOKEN(sym_class_reltype_aggregation);
      if (lookahead == '=') ADVANCE(82);
      if (lookahead == '{') ADVANCE(938);
      if (lookahead == '|') ADVANCE(936);
      END_STATE();
    case 726:
      ACCEPT_TOKEN(sym_class_reltype_aggregation);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(702);
      END_STATE();
    case 727:
      ACCEPT_TOKEN(anon_sym_LT_PIPE);
      END_STATE();
    case 728:
      ACCEPT_TOKEN(anon_sym_PIPE_GT);
      END_STATE();
    case 729:
      ACCEPT_TOKEN(anon_sym_LT);
      if (lookahead == '<') ADVANCE(500);
      if (lookahead == '=') ADVANCE(82);
      if (lookahead == '|') ADVANCE(727);
      END_STATE();
    case 730:
      ACCEPT_TOKEN(anon_sym_LT);
      if (lookahead == '|') ADVANCE(727);
      END_STATE();
    case 731:
      ACCEPT_TOKEN(anon_sym_DASH_DASH);
      END_STATE();
    case 732:
      ACCEPT_TOKEN(anon_sym_DASH_DASH);
      if (lookahead == ')') ADVANCE(695);
      if (lookahead == '>') ADVANCE(691);
      END_STATE();
    case 733:
      ACCEPT_TOKEN(anon_sym_DASH_DASH);
      if (lookahead == '>') ADVANCE(691);
      END_STATE();
    case 734:
      ACCEPT_TOKEN(anon_sym_DASH_DASH);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(787);
      END_STATE();
    case 735:
      ACCEPT_TOKEN(anon_sym_DOT_DOT);
      END_STATE();
    case 736:
      ACCEPT_TOKEN(aux_sym_class_label_token1);
      if (lookahead == '\n') ADVANCE(585);
      if (lookahead == '{') ADVANCE(787);
      if (lookahead == ':' ||
          lookahead == ';') ADVANCE(10);
      if (lookahead != 0) ADVANCE(737);
      END_STATE();
    case 737:
      ACCEPT_TOKEN(aux_sym_class_label_token1);
      if (lookahead == '\n') ADVANCE(584);
      if (lookahead == ':' ||
          lookahead == ';') ADVANCE(10);
      if (lookahead != 0) ADVANCE(737);
      END_STATE();
    case 738:
      ACCEPT_TOKEN(aux_sym_class_label_token1);
      if (lookahead == ' ') ADVANCE(455);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(786);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(787);
      END_STATE();
    case 739:
      ACCEPT_TOKEN(aux_sym_class_label_token1);
      if (lookahead == ' ') ADVANCE(504);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(786);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(787);
      END_STATE();
    case 740:
      ACCEPT_TOKEN(aux_sym_class_label_token1);
      if (lookahead == ' ') ADVANCE(756);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(786);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(787);
      END_STATE();
    case 741:
      ACCEPT_TOKEN(aux_sym_class_label_token1);
      if (lookahead == ' ') ADVANCE(749);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(786);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(787);
      END_STATE();
    case 742:
      ACCEPT_TOKEN(aux_sym_class_label_token1);
      if (lookahead == ' ') ADVANCE(753);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(787);
      END_STATE();
    case 743:
      ACCEPT_TOKEN(aux_sym_class_label_token1);
      if (lookahead == '%') ADVANCE(736);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(787);
      END_STATE();
    case 744:
      ACCEPT_TOKEN(aux_sym_class_label_token1);
      if (lookahead == '*') ADVANCE(746);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(787);
      END_STATE();
    case 745:
      ACCEPT_TOKEN(aux_sym_class_label_token1);
      if (lookahead == '-') ADVANCE(734);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(787);
      END_STATE();
    case 746:
      ACCEPT_TOKEN(aux_sym_class_label_token1);
      if (lookahead == ']') ADVANCE(789);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(787);
      END_STATE();
    case 747:
      ACCEPT_TOKEN(aux_sym_class_label_token1);
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(784);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('B' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('b' <= lookahead && lookahead <= 'z')) ADVANCE(786);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(787);
      END_STATE();
    case 748:
      ACCEPT_TOKEN(aux_sym_class_label_token1);
      if (lookahead == 'B' ||
          lookahead == 'b') ADVANCE(590);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(787);
      END_STATE();
    case 749:
      ACCEPT_TOKEN(aux_sym_class_label_token1);
      if (lookahead == 'B' ||
          lookahead == 'b') ADVANCE(778);
      if (lookahead == 'L' ||
          lookahead == 'l') ADVANCE(774);
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(765);
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(748);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(787);
      END_STATE();
    case 750:
      ACCEPT_TOKEN(aux_sym_class_label_token1);
      if (lookahead == 'C' ||
          lookahead == 'c') ADVANCE(776);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(787);
      END_STATE();
    case 751:
      ACCEPT_TOKEN(aux_sym_class_label_token1);
      if (lookahead == 'C' ||
          lookahead == 'c') ADVANCE(782);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(786);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(787);
      END_STATE();
    case 752:
      ACCEPT_TOKEN(aux_sym_class_label_token1);
      if (lookahead == 'D' ||
          lookahead == 'd') ADVANCE(755);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(786);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(787);
      END_STATE();
    case 753:
      ACCEPT_TOKEN(aux_sym_class_label_token1);
      if (lookahead == 'D' ||
          lookahead == 'd') ADVANCE(757);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(787);
      END_STATE();
    case 754:
      ACCEPT_TOKEN(aux_sym_class_label_token1);
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(751);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(786);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(787);
      END_STATE();
    case 755:
      ACCEPT_TOKEN(aux_sym_class_label_token1);
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(740);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(786);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(787);
      END_STATE();
    case 756:
      ACCEPT_TOKEN(aux_sym_class_label_token1);
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(766);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(787);
      END_STATE();
    case 757:
      ACCEPT_TOKEN(aux_sym_class_label_token1);
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(777);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(787);
      END_STATE();
    case 758:
      ACCEPT_TOKEN(aux_sym_class_label_token1);
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(738);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(786);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(787);
      END_STATE();
    case 759:
      ACCEPT_TOKEN(aux_sym_class_label_token1);
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(739);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(786);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(787);
      END_STATE();
    case 760:
      ACCEPT_TOKEN(aux_sym_class_label_token1);
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(775);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(786);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(787);
      END_STATE();
    case 761:
      ACCEPT_TOKEN(aux_sym_class_label_token1);
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(752);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(786);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(787);
      END_STATE();
    case 762:
      ACCEPT_TOKEN(aux_sym_class_label_token1);
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(770);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(787);
      END_STATE();
    case 763:
      ACCEPT_TOKEN(aux_sym_class_label_token1);
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(769);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(786);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(787);
      END_STATE();
    case 764:
      ACCEPT_TOKEN(aux_sym_class_label_token1);
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(773);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(787);
      END_STATE();
    case 765:
      ACCEPT_TOKEN(aux_sym_class_label_token1);
      if (lookahead == 'L' ||
          lookahead == 'l') ADVANCE(592);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(787);
      END_STATE();
    case 766:
      ACCEPT_TOKEN(aux_sym_class_label_token1);
      if (lookahead == 'M' ||
          lookahead == 'm') ADVANCE(772);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(787);
      END_STATE();
    case 767:
      ACCEPT_TOKEN(aux_sym_class_label_token1);
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(814);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(787);
      END_STATE();
    case 768:
      ACCEPT_TOKEN(aux_sym_class_label_token1);
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(741);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(786);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(787);
      END_STATE();
    case 769:
      ACCEPT_TOKEN(aux_sym_class_label_token1);
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(768);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(786);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(787);
      END_STATE();
    case 770:
      ACCEPT_TOKEN(aux_sym_class_label_token1);
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(767);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(787);
      END_STATE();
    case 771:
      ACCEPT_TOKEN(aux_sym_class_label_token1);
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(783);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(786);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(787);
      END_STATE();
    case 772:
      ACCEPT_TOKEN(aux_sym_class_label_token1);
      if (lookahead == 'P' ||
          lookahead == 'p') ADVANCE(780);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(787);
      END_STATE();
    case 773:
      ACCEPT_TOKEN(aux_sym_class_label_token1);
      if (lookahead == 'P' ||
          lookahead == 'p') ADVANCE(781);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(787);
      END_STATE();
    case 774:
      ACCEPT_TOKEN(aux_sym_class_label_token1);
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(593);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(787);
      END_STATE();
    case 775:
      ACCEPT_TOKEN(aux_sym_class_label_token1);
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(754);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(786);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(787);
      END_STATE();
    case 776:
      ACCEPT_TOKEN(aux_sym_class_label_token1);
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(764);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(787);
      END_STATE();
    case 777:
      ACCEPT_TOKEN(aux_sym_class_label_token1);
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(750);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(787);
      END_STATE();
    case 778:
      ACCEPT_TOKEN(aux_sym_class_label_token1);
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(591);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(787);
      END_STATE();
    case 779:
      ACCEPT_TOKEN(aux_sym_class_label_token1);
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(747);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(786);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(787);
      END_STATE();
    case 780:
      ACCEPT_TOKEN(aux_sym_class_label_token1);
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(785);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(787);
      END_STATE();
    case 781:
      ACCEPT_TOKEN(aux_sym_class_label_token1);
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(762);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(787);
      END_STATE();
    case 782:
      ACCEPT_TOKEN(aux_sym_class_label_token1);
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(763);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(786);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(787);
      END_STATE();
    case 783:
      ACCEPT_TOKEN(aux_sym_class_label_token1);
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(758);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(786);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(787);
      END_STATE();
    case 784:
      ACCEPT_TOKEN(aux_sym_class_label_token1);
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(759);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(786);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(787);
      END_STATE();
    case 785:
      ACCEPT_TOKEN(aux_sym_class_label_token1);
      if (lookahead == 'Y' ||
          lookahead == 'y') ADVANCE(742);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(787);
      END_STATE();
    case 786:
      ACCEPT_TOKEN(aux_sym_class_label_token1);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(786);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(787);
      END_STATE();
    case 787:
      ACCEPT_TOKEN(aux_sym_class_label_token1);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(787);
      END_STATE();
    case 788:
      ACCEPT_TOKEN(anon_sym_LBRACK_STAR_RBRACK);
      END_STATE();
    case 789:
      ACCEPT_TOKEN(anon_sym_LBRACK_STAR_RBRACK);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(787);
      END_STATE();
    case 790:
      ACCEPT_TOKEN(aux_sym_state_name_token1);
      if (lookahead == ' ') ADVANCE(455);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(813);
      END_STATE();
    case 791:
      ACCEPT_TOKEN(aux_sym_state_name_token1);
      if (lookahead == ' ') ADVANCE(504);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(813);
      END_STATE();
    case 792:
      ACCEPT_TOKEN(aux_sym_state_name_token1);
      if (lookahead == ' ') ADVANCE(175);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(813);
      END_STATE();
    case 793:
      ACCEPT_TOKEN(aux_sym_state_name_token1);
      if (lookahead == ' ') ADVANCE(218);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(813);
      END_STATE();
    case 794:
      ACCEPT_TOKEN(aux_sym_state_name_token1);
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(812);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('B' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('b' <= lookahead && lookahead <= 'z')) ADVANCE(813);
      END_STATE();
    case 795:
      ACCEPT_TOKEN(aux_sym_state_name_token1);
      if (lookahead == 'C' ||
          lookahead == 'c') ADVANCE(810);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(813);
      END_STATE();
    case 796:
      ACCEPT_TOKEN(aux_sym_state_name_token1);
      if (lookahead == 'D' ||
          lookahead == 'd') ADVANCE(798);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(813);
      END_STATE();
    case 797:
      ACCEPT_TOKEN(aux_sym_state_name_token1);
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(795);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(813);
      END_STATE();
    case 798:
      ACCEPT_TOKEN(aux_sym_state_name_token1);
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(793);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(813);
      END_STATE();
    case 799:
      ACCEPT_TOKEN(aux_sym_state_name_token1);
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(790);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(813);
      END_STATE();
    case 800:
      ACCEPT_TOKEN(aux_sym_state_name_token1);
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(791);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(813);
      END_STATE();
    case 801:
      ACCEPT_TOKEN(aux_sym_state_name_token1);
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(807);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(813);
      END_STATE();
    case 802:
      ACCEPT_TOKEN(aux_sym_state_name_token1);
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(796);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(813);
      END_STATE();
    case 803:
      ACCEPT_TOKEN(aux_sym_state_name_token1);
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(805);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(813);
      END_STATE();
    case 804:
      ACCEPT_TOKEN(aux_sym_state_name_token1);
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(792);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(813);
      END_STATE();
    case 805:
      ACCEPT_TOKEN(aux_sym_state_name_token1);
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(804);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(813);
      END_STATE();
    case 806:
      ACCEPT_TOKEN(aux_sym_state_name_token1);
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(811);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(813);
      END_STATE();
    case 807:
      ACCEPT_TOKEN(aux_sym_state_name_token1);
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(797);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(813);
      END_STATE();
    case 808:
      ACCEPT_TOKEN(aux_sym_state_name_token1);
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(448);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(813);
      END_STATE();
    case 809:
      ACCEPT_TOKEN(aux_sym_state_name_token1);
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(794);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(813);
      END_STATE();
    case 810:
      ACCEPT_TOKEN(aux_sym_state_name_token1);
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(803);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(813);
      END_STATE();
    case 811:
      ACCEPT_TOKEN(aux_sym_state_name_token1);
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(799);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(813);
      END_STATE();
    case 812:
      ACCEPT_TOKEN(aux_sym_state_name_token1);
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(800);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(813);
      END_STATE();
    case 813:
      ACCEPT_TOKEN(aux_sym_state_name_token1);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(813);
      END_STATE();
    case 814:
      ACCEPT_TOKEN(aux_sym_state_hide_empty_description_token1);
      END_STATE();
    case 815:
      ACCEPT_TOKEN(sym_state_id);
      if (lookahead == '\n') ADVANCE(585);
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ' ||
          lookahead == '-' ||
          lookahead == ':') ADVANCE(10);
      if (lookahead != 0 &&
          lookahead != '{') ADVANCE(816);
      END_STATE();
    case 816:
      ACCEPT_TOKEN(sym_state_id);
      if (lookahead == '\n') ADVANCE(584);
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ' ||
          lookahead == '-' ||
          lookahead == ':' ||
          lookahead == '{') ADVANCE(10);
      if (lookahead != 0) ADVANCE(816);
      END_STATE();
    case 817:
      ACCEPT_TOKEN(sym_state_id);
      if (lookahead == '%') ADVANCE(815);
      if (lookahead != 0 &&
          (lookahead < '\t' || '\r' < lookahead) &&
          lookahead != ' ' &&
          lookahead != '-' &&
          lookahead != ':' &&
          lookahead != '{') ADVANCE(818);
      END_STATE();
    case 818:
      ACCEPT_TOKEN(sym_state_id);
      if (lookahead != 0 &&
          (lookahead < '\t' || '\r' < lookahead) &&
          lookahead != ' ' &&
          lookahead != '-' &&
          lookahead != ':' &&
          lookahead != '{') ADVANCE(818);
      END_STATE();
    case 819:
      ACCEPT_TOKEN(aux_sym_state_annotation_fork_token1);
      END_STATE();
    case 820:
      ACCEPT_TOKEN(aux_sym_state_annotation_fork_token2);
      END_STATE();
    case 821:
      ACCEPT_TOKEN(aux_sym_state_annotation_join_token1);
      END_STATE();
    case 822:
      ACCEPT_TOKEN(aux_sym_state_annotation_join_token2);
      END_STATE();
    case 823:
      ACCEPT_TOKEN(aux_sym_state_annotation_choice_token1);
      END_STATE();
    case 824:
      ACCEPT_TOKEN(aux_sym_state_annotation_choice_token2);
      END_STATE();
    case 825:
      ACCEPT_TOKEN(aux_sym_gantt_task_text_token1);
      if (lookahead == '\n') ADVANCE(585);
      if (lookahead == '{') ADVANCE(894);
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ' ||
          lookahead == '#' ||
          lookahead == ':' ||
          lookahead == ';') ADVANCE(10);
      if (lookahead != 0) ADVANCE(826);
      END_STATE();
    case 826:
      ACCEPT_TOKEN(aux_sym_gantt_task_text_token1);
      if (lookahead == '\n') ADVANCE(584);
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ' ||
          lookahead == '#' ||
          lookahead == ':' ||
          lookahead == ';') ADVANCE(10);
      if (lookahead != 0) ADVANCE(826);
      END_STATE();
    case 827:
      ACCEPT_TOKEN(aux_sym_gantt_task_text_token1);
      if (lookahead == '%') ADVANCE(825);
      if (lookahead != 0 &&
          (lookahead < '\t' || '\r' < lookahead) &&
          lookahead != ' ' &&
          lookahead != '#' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(894);
      END_STATE();
    case 828:
      ACCEPT_TOKEN(aux_sym_gantt_task_text_token1);
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(883);
      if (lookahead != 0 &&
          (lookahead < '\t' || '\r' < lookahead) &&
          lookahead != ' ' &&
          lookahead != '#' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(894);
      END_STATE();
    case 829:
      ACCEPT_TOKEN(aux_sym_gantt_task_text_token1);
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(893);
      if (lookahead != 0 &&
          (lookahead < '\t' || '\r' < lookahead) &&
          lookahead != ' ' &&
          lookahead != '#' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(894);
      END_STATE();
    case 830:
      ACCEPT_TOKEN(aux_sym_gantt_task_text_token1);
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(873);
      if (lookahead != 0 &&
          (lookahead < '\t' || '\r' < lookahead) &&
          lookahead != ' ' &&
          lookahead != '#' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(894);
      END_STATE();
    case 831:
      ACCEPT_TOKEN(aux_sym_gantt_task_text_token1);
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(881);
      if (lookahead != 0 &&
          (lookahead < '\t' || '\r' < lookahead) &&
          lookahead != ' ' &&
          lookahead != '#' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(894);
      END_STATE();
    case 832:
      ACCEPT_TOKEN(aux_sym_gantt_task_text_token1);
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(882);
      if (lookahead != 0 &&
          (lookahead < '\t' || '\r' < lookahead) &&
          lookahead != ' ' &&
          lookahead != '#' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(894);
      END_STATE();
    case 833:
      ACCEPT_TOKEN(aux_sym_gantt_task_text_token1);
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(886);
      if (lookahead != 0 &&
          (lookahead < '\t' || '\r' < lookahead) &&
          lookahead != ' ' &&
          lookahead != '#' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(894);
      END_STATE();
    case 834:
      ACCEPT_TOKEN(aux_sym_gantt_task_text_token1);
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(892);
      if (lookahead != 0 &&
          (lookahead < '\t' || '\r' < lookahead) &&
          lookahead != ' ' &&
          lookahead != '#' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(894);
      END_STATE();
    case 835:
      ACCEPT_TOKEN(aux_sym_gantt_task_text_token1);
      if (lookahead == 'C' ||
          lookahead == 'c') ADVANCE(860);
      if (lookahead != 0 &&
          (lookahead < '\t' || '\r' < lookahead) &&
          lookahead != ' ' &&
          lookahead != '#' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(894);
      END_STATE();
    case 836:
      ACCEPT_TOKEN(aux_sym_gantt_task_text_token1);
      if (lookahead == 'C' ||
          lookahead == 'c') ADVANCE(884);
      if (lookahead != 0 &&
          (lookahead < '\t' || '\r' < lookahead) &&
          lookahead != ' ' &&
          lookahead != '#' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(894);
      END_STATE();
    case 837:
      ACCEPT_TOKEN(aux_sym_gantt_task_text_token1);
      if (lookahead == 'C' ||
          lookahead == 'c') ADVANCE(861);
      if (lookahead != 0 &&
          (lookahead < '\t' || '\r' < lookahead) &&
          lookahead != ' ' &&
          lookahead != '#' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(894);
      END_STATE();
    case 838:
      ACCEPT_TOKEN(aux_sym_gantt_task_text_token1);
      if (lookahead == 'D' ||
          lookahead == 'd') ADVANCE(829);
      if (lookahead == 'P' ||
          lookahead == 'p') ADVANCE(834);
      if (lookahead != 0 &&
          (lookahead < '\t' || '\r' < lookahead) &&
          lookahead != ' ' &&
          lookahead != '#' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(894);
      END_STATE();
    case 839:
      ACCEPT_TOKEN(aux_sym_gantt_task_text_token1);
      if (lookahead == 'D' ||
          lookahead == 'd') ADVANCE(842);
      if (lookahead != 0 &&
          (lookahead < '\t' || '\r' < lookahead) &&
          lookahead != ' ' &&
          lookahead != '#' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(894);
      END_STATE();
    case 840:
      ACCEPT_TOKEN(aux_sym_gantt_task_text_token1);
      if (lookahead == 'D' ||
          lookahead == 'd') ADVANCE(846);
      if (lookahead != 0 &&
          (lookahead < '\t' || '\r' < lookahead) &&
          lookahead != ' ' &&
          lookahead != '#' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(894);
      END_STATE();
    case 841:
      ACCEPT_TOKEN(aux_sym_gantt_task_text_token1);
      if (lookahead == 'D' ||
          lookahead == 'd') ADVANCE(848);
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(855);
      if (lookahead != 0 &&
          (lookahead < '\t' || '\r' < lookahead) &&
          lookahead != ' ' &&
          lookahead != '#' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(894);
      END_STATE();
    case 842:
      ACCEPT_TOKEN(aux_sym_gantt_task_text_token1);
      if (lookahead == 'D' ||
          lookahead == 'd') ADVANCE(833);
      if (lookahead != 0 &&
          (lookahead < '\t' || '\r' < lookahead) &&
          lookahead != ' ' &&
          lookahead != '#' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(894);
      END_STATE();
    case 843:
      ACCEPT_TOKEN(aux_sym_gantt_task_text_token1);
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(462);
      if (lookahead != 0 &&
          (lookahead < '\t' || '\r' < lookahead) &&
          lookahead != ' ' &&
          lookahead != '#' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(894);
      END_STATE();
    case 844:
      ACCEPT_TOKEN(aux_sym_gantt_task_text_token1);
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(836);
      if (lookahead != 0 &&
          (lookahead < '\t' || '\r' < lookahead) &&
          lookahead != ' ' &&
          lookahead != '#' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(894);
      END_STATE();
    case 845:
      ACCEPT_TOKEN(aux_sym_gantt_task_text_token1);
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(867);
      if (lookahead != 0 &&
          (lookahead < '\t' || '\r' < lookahead) &&
          lookahead != ' ' &&
          lookahead != '#' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(894);
      END_STATE();
    case 846:
      ACCEPT_TOKEN(aux_sym_gantt_task_text_token1);
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(877);
      if (lookahead != 0 &&
          (lookahead < '\t' || '\r' < lookahead) &&
          lookahead != ' ' &&
          lookahead != '#' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(894);
      END_STATE();
    case 847:
      ACCEPT_TOKEN(aux_sym_gantt_task_text_token1);
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(872);
      if (lookahead != 0 &&
          (lookahead < '\t' || '\r' < lookahead) &&
          lookahead != ' ' &&
          lookahead != '#' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(894);
      END_STATE();
    case 848:
      ACCEPT_TOKEN(aux_sym_gantt_task_text_token1);
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(878);
      if (lookahead != 0 &&
          (lookahead < '\t' || '\r' < lookahead) &&
          lookahead != ' ' &&
          lookahead != '#' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(894);
      END_STATE();
    case 849:
      ACCEPT_TOKEN(aux_sym_gantt_task_text_token1);
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(879);
      if (lookahead != 0 &&
          (lookahead < '\t' || '\r' < lookahead) &&
          lookahead != ' ' &&
          lookahead != '#' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(894);
      END_STATE();
    case 850:
      ACCEPT_TOKEN(aux_sym_gantt_task_text_token1);
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(845);
      if (lookahead != 0 &&
          (lookahead < '\t' || '\r' < lookahead) &&
          lookahead != ' ' &&
          lookahead != '#' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(894);
      END_STATE();
    case 851:
      ACCEPT_TOKEN(aux_sym_gantt_task_text_token1);
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(853);
      if (lookahead != 0 &&
          (lookahead < '\t' || '\r' < lookahead) &&
          lookahead != ' ' &&
          lookahead != '#' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(894);
      END_STATE();
    case 852:
      ACCEPT_TOKEN(aux_sym_gantt_task_text_token1);
      if (lookahead == 'F' ||
          lookahead == 'f') ADVANCE(869);
      if (lookahead != 0 &&
          (lookahead < '\t' || '\r' < lookahead) &&
          lookahead != ' ' &&
          lookahead != '#' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(894);
      END_STATE();
    case 853:
      ACCEPT_TOKEN(aux_sym_gantt_task_text_token1);
      if (lookahead == 'F' ||
          lookahead == 'f') ADVANCE(871);
      if (lookahead != 0 &&
          (lookahead < '\t' || '\r' < lookahead) &&
          lookahead != ' ' &&
          lookahead != '#' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(894);
      END_STATE();
    case 854:
      ACCEPT_TOKEN(aux_sym_gantt_task_text_token1);
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(880);
      if (lookahead != 0 &&
          (lookahead < '\t' || '\r' < lookahead) &&
          lookahead != ' ' &&
          lookahead != '#' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(894);
      END_STATE();
    case 855:
      ACCEPT_TOKEN(aux_sym_gantt_task_text_token1);
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(889);
      if (lookahead != 0 &&
          (lookahead < '\t' || '\r' < lookahead) &&
          lookahead != ' ' &&
          lookahead != '#' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(894);
      END_STATE();
    case 856:
      ACCEPT_TOKEN(aux_sym_gantt_task_text_token1);
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(885);
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(838);
      if (lookahead != 0 &&
          (lookahead < '\t' || '\r' < lookahead) &&
          lookahead != ' ' &&
          lookahead != '#' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(894);
      END_STATE();
    case 857:
      ACCEPT_TOKEN(aux_sym_gantt_task_text_token1);
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(876);
      if (lookahead != 0 &&
          (lookahead < '\t' || '\r' < lookahead) &&
          lookahead != ' ' &&
          lookahead != '#' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(894);
      END_STATE();
    case 858:
      ACCEPT_TOKEN(aux_sym_gantt_task_text_token1);
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(870);
      if (lookahead != 0 &&
          (lookahead < '\t' || '\r' < lookahead) &&
          lookahead != ' ' &&
          lookahead != '#' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(894);
      END_STATE();
    case 859:
      ACCEPT_TOKEN(aux_sym_gantt_task_text_token1);
      if (lookahead == 'K' ||
          lookahead == 'k') ADVANCE(847);
      if (lookahead != 0 &&
          (lookahead < '\t' || '\r' < lookahead) &&
          lookahead != ' ' &&
          lookahead != '#' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(894);
      END_STATE();
    case 860:
      ACCEPT_TOKEN(aux_sym_gantt_task_text_token1);
      if (lookahead == 'L' ||
          lookahead == 'l') ADVANCE(887);
      if (lookahead != 0 &&
          (lookahead < '\t' || '\r' < lookahead) &&
          lookahead != ' ' &&
          lookahead != '#' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(894);
      END_STATE();
    case 861:
      ACCEPT_TOKEN(aux_sym_gantt_task_text_token1);
      if (lookahead == 'L' ||
          lookahead == 'l') ADVANCE(888);
      if (lookahead != 0 &&
          (lookahead < '\t' || '\r' < lookahead) &&
          lookahead != ' ' &&
          lookahead != '#' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(894);
      END_STATE();
    case 862:
      ACCEPT_TOKEN(aux_sym_gantt_task_text_token1);
      if (lookahead == 'L' ||
          lookahead == 'l') ADVANCE(843);
      if (lookahead != 0 &&
          (lookahead < '\t' || '\r' < lookahead) &&
          lookahead != ' ' &&
          lookahead != '#' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(894);
      END_STATE();
    case 863:
      ACCEPT_TOKEN(aux_sym_gantt_task_text_token1);
      if (lookahead == 'M' ||
          lookahead == 'm') ADVANCE(830);
      if (lookahead != 0 &&
          (lookahead < '\t' || '\r' < lookahead) &&
          lookahead != ' ' &&
          lookahead != '#' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(894);
      END_STATE();
    case 864:
      ACCEPT_TOKEN(aux_sym_gantt_task_text_token1);
      if (lookahead == 'M' ||
          lookahead == 'm') ADVANCE(831);
      if (lookahead != 0 &&
          (lookahead < '\t' || '\r' < lookahead) &&
          lookahead != ' ' &&
          lookahead != '#' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(894);
      END_STATE();
    case 865:
      ACCEPT_TOKEN(aux_sym_gantt_task_text_token1);
      if (lookahead == 'M' ||
          lookahead == 'm') ADVANCE(832);
      if (lookahead != 0 &&
          (lookahead < '\t' || '\r' < lookahead) &&
          lookahead != ' ' &&
          lookahead != '#' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(894);
      END_STATE();
    case 866:
      ACCEPT_TOKEN(aux_sym_gantt_task_text_token1);
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(513);
      if (lookahead != 0 &&
          (lookahead < '\t' || '\r' < lookahead) &&
          lookahead != ' ' &&
          lookahead != '#' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(894);
      END_STATE();
    case 867:
      ACCEPT_TOKEN(aux_sym_gantt_task_text_token1);
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(839);
      if (lookahead != 0 &&
          (lookahead < '\t' || '\r' < lookahead) &&
          lookahead != ' ' &&
          lookahead != '#' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(894);
      END_STATE();
    case 868:
      ACCEPT_TOKEN(aux_sym_gantt_task_text_token1);
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(837);
      if (lookahead != 0 &&
          (lookahead < '\t' || '\r' < lookahead) &&
          lookahead != ' ' &&
          lookahead != '#' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(894);
      END_STATE();
    case 869:
      ACCEPT_TOKEN(aux_sym_gantt_task_text_token1);
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(874);
      if (lookahead != 0 &&
          (lookahead < '\t' || '\r' < lookahead) &&
          lookahead != ' ' &&
          lookahead != '#' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(894);
      END_STATE();
    case 870:
      ACCEPT_TOKEN(aux_sym_gantt_task_text_token1);
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(866);
      if (lookahead != 0 &&
          (lookahead < '\t' || '\r' < lookahead) &&
          lookahead != ' ' &&
          lookahead != '#' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(894);
      END_STATE();
    case 871:
      ACCEPT_TOKEN(aux_sym_gantt_task_text_token1);
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(875);
      if (lookahead != 0 &&
          (lookahead < '\t' || '\r' < lookahead) &&
          lookahead != ' ' &&
          lookahead != '#' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(894);
      END_STATE();
    case 872:
      ACCEPT_TOKEN(aux_sym_gantt_task_text_token1);
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(512);
      if (lookahead != 0 &&
          (lookahead < '\t' || '\r' < lookahead) &&
          lookahead != ' ' &&
          lookahead != '#' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(894);
      END_STATE();
    case 873:
      ACCEPT_TOKEN(aux_sym_gantt_task_text_token1);
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(859);
      if (lookahead != 0 &&
          (lookahead < '\t' || '\r' < lookahead) &&
          lookahead != ' ' &&
          lookahead != '#' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(894);
      END_STATE();
    case 874:
      ACCEPT_TOKEN(aux_sym_gantt_task_text_token1);
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(864);
      if (lookahead != 0 &&
          (lookahead < '\t' || '\r' < lookahead) &&
          lookahead != ' ' &&
          lookahead != '#' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(894);
      END_STATE();
    case 875:
      ACCEPT_TOKEN(aux_sym_gantt_task_text_token1);
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(865);
      if (lookahead != 0 &&
          (lookahead < '\t' || '\r' < lookahead) &&
          lookahead != ' ' &&
          lookahead != '#' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(894);
      END_STATE();
    case 876:
      ACCEPT_TOKEN(aux_sym_gantt_task_text_token1);
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(508);
      if (lookahead != 0 &&
          (lookahead < '\t' || '\r' < lookahead) &&
          lookahead != ' ' &&
          lookahead != '#' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(894);
      END_STATE();
    case 877:
      ACCEPT_TOKEN(aux_sym_gantt_task_text_token1);
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(511);
      if (lookahead != 0 &&
          (lookahead < '\t' || '\r' < lookahead) &&
          lookahead != ' ' &&
          lookahead != '#' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(894);
      END_STATE();
    case 878:
      ACCEPT_TOKEN(aux_sym_gantt_task_text_token1);
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(510);
      if (lookahead != 0 &&
          (lookahead < '\t' || '\r' < lookahead) &&
          lookahead != ' ' &&
          lookahead != '#' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(894);
      END_STATE();
    case 879:
      ACCEPT_TOKEN(aux_sym_gantt_task_text_token1);
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(507);
      if (lookahead != 0 &&
          (lookahead < '\t' || '\r' < lookahead) &&
          lookahead != ' ' &&
          lookahead != '#' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(894);
      END_STATE();
    case 880:
      ACCEPT_TOKEN(aux_sym_gantt_task_text_token1);
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(852);
      if (lookahead != 0 &&
          (lookahead < '\t' || '\r' < lookahead) &&
          lookahead != ' ' &&
          lookahead != '#' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(894);
      END_STATE();
    case 881:
      ACCEPT_TOKEN(aux_sym_gantt_task_text_token1);
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(509);
      if (lookahead != 0 &&
          (lookahead < '\t' || '\r' < lookahead) &&
          lookahead != ' ' &&
          lookahead != '#' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(894);
      END_STATE();
    case 882:
      ACCEPT_TOKEN(aux_sym_gantt_task_text_token1);
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(506);
      if (lookahead != 0 &&
          (lookahead < '\t' || '\r' < lookahead) &&
          lookahead != ' ' &&
          lookahead != '#' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(894);
      END_STATE();
    case 883:
      ACCEPT_TOKEN(aux_sym_gantt_task_text_token1);
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(851);
      if (lookahead != 0 &&
          (lookahead < '\t' || '\r' < lookahead) &&
          lookahead != ' ' &&
          lookahead != '#' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(894);
      END_STATE();
    case 884:
      ACCEPT_TOKEN(aux_sym_gantt_task_text_token1);
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(858);
      if (lookahead != 0 &&
          (lookahead < '\t' || '\r' < lookahead) &&
          lookahead != ' ' &&
          lookahead != '#' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(894);
      END_STATE();
    case 885:
      ACCEPT_TOKEN(aux_sym_gantt_task_text_token1);
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(862);
      if (lookahead != 0 &&
          (lookahead < '\t' || '\r' < lookahead) &&
          lookahead != ' ' &&
          lookahead != '#' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(894);
      END_STATE();
    case 886:
      ACCEPT_TOKEN(aux_sym_gantt_task_text_token1);
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(849);
      if (lookahead != 0 &&
          (lookahead < '\t' || '\r' < lookahead) &&
          lookahead != ' ' &&
          lookahead != '#' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(894);
      END_STATE();
    case 887:
      ACCEPT_TOKEN(aux_sym_gantt_task_text_token1);
      if (lookahead == 'U' ||
          lookahead == 'u') ADVANCE(840);
      if (lookahead != 0 &&
          (lookahead < '\t' || '\r' < lookahead) &&
          lookahead != ' ' &&
          lookahead != '#' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(894);
      END_STATE();
    case 888:
      ACCEPT_TOKEN(aux_sym_gantt_task_text_token1);
      if (lookahead == 'U' ||
          lookahead == 'u') ADVANCE(841);
      if (lookahead != 0 &&
          (lookahead < '\t' || '\r' < lookahead) &&
          lookahead != ' ' &&
          lookahead != '#' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(894);
      END_STATE();
    case 889:
      ACCEPT_TOKEN(aux_sym_gantt_task_text_token1);
      if (lookahead == 'V' ||
          lookahead == 'v') ADVANCE(850);
      if (lookahead != 0 &&
          (lookahead < '\t' || '\r' < lookahead) &&
          lookahead != ' ' &&
          lookahead != '#' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(894);
      END_STATE();
    case 890:
      ACCEPT_TOKEN(aux_sym_gantt_task_text_token1);
      if (lookahead == 'X' ||
          lookahead == 'x') ADVANCE(854);
      if (lookahead != 0 &&
          (lookahead < '\t' || '\r' < lookahead) &&
          lookahead != ' ' &&
          lookahead != '#' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(894);
      END_STATE();
    case 891:
      ACCEPT_TOKEN(aux_sym_gantt_task_text_token1);
      if (lookahead == 'X' ||
          lookahead == 'x') ADVANCE(835);
      if (lookahead != 0 &&
          (lookahead < '\t' || '\r' < lookahead) &&
          lookahead != ' ' &&
          lookahead != '#' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(894);
      END_STATE();
    case 892:
      ACCEPT_TOKEN(aux_sym_gantt_task_text_token1);
      if (lookahead == 'X' ||
          lookahead == 'x') ADVANCE(857);
      if (lookahead != 0 &&
          (lookahead < '\t' || '\r' < lookahead) &&
          lookahead != ' ' &&
          lookahead != '#' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(894);
      END_STATE();
    case 893:
      ACCEPT_TOKEN(aux_sym_gantt_task_text_token1);
      if (lookahead == 'Y' ||
          lookahead == 'y') ADVANCE(863);
      if (lookahead != 0 &&
          (lookahead < '\t' || '\r' < lookahead) &&
          lookahead != ' ' &&
          lookahead != '#' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(894);
      END_STATE();
    case 894:
      ACCEPT_TOKEN(aux_sym_gantt_task_text_token1);
      if (lookahead != 0 &&
          (lookahead < '\t' || '\r' < lookahead) &&
          lookahead != ' ' &&
          lookahead != '#' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(894);
      END_STATE();
    case 895:
      ACCEPT_TOKEN(sym_gantt_task_data);
      if (lookahead == '\n') ADVANCE(585);
      if (lookahead == '{') ADVANCE(898);
      if (lookahead == '#' ||
          lookahead == ':' ||
          lookahead == ';') ADVANCE(10);
      if (lookahead != 0) ADVANCE(896);
      END_STATE();
    case 896:
      ACCEPT_TOKEN(sym_gantt_task_data);
      if (lookahead == '\n') ADVANCE(584);
      if (lookahead == '#' ||
          lookahead == ':' ||
          lookahead == ';') ADVANCE(10);
      if (lookahead != 0) ADVANCE(896);
      END_STATE();
    case 897:
      ACCEPT_TOKEN(sym_gantt_task_data);
      if (lookahead == '%') ADVANCE(895);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != '#' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(898);
      END_STATE();
    case 898:
      ACCEPT_TOKEN(sym_gantt_task_data);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != '#' &&
          lookahead != ':' &&
          lookahead != ';') ADVANCE(898);
      END_STATE();
    case 899:
      ACCEPT_TOKEN(aux_sym_pie_showdata_token1);
      END_STATE();
    case 900:
      ACCEPT_TOKEN(sym_pie_title);
      if (lookahead == '\n') ADVANCE(585);
      if (lookahead == '{') ADVANCE(908);
      if (lookahead == '#' ||
          lookahead == ';') ADVANCE(10);
      if (lookahead != 0) ADVANCE(901);
      END_STATE();
    case 901:
      ACCEPT_TOKEN(sym_pie_title);
      if (lookahead == '\n') ADVANCE(584);
      if (lookahead == '#' ||
          lookahead == ';') ADVANCE(10);
      if (lookahead != 0) ADVANCE(901);
      END_STATE();
    case 902:
      ACCEPT_TOKEN(sym_pie_title);
      if (lookahead == '"') ADVANCE(908);
      if (lookahead == '\n' ||
          lookahead == '#' ||
          lookahead == ';') ADVANCE(23);
      if (lookahead != 0) ADVANCE(902);
      END_STATE();
    case 903:
      ACCEPT_TOKEN(sym_pie_title);
      if (lookahead == '%') ADVANCE(900);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != '#' &&
          lookahead != ';') ADVANCE(908);
      END_STATE();
    case 904:
      ACCEPT_TOKEN(sym_pie_title);
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(462);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != '#' &&
          lookahead != ';') ADVANCE(908);
      END_STATE();
    case 905:
      ACCEPT_TOKEN(sym_pie_title);
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(907);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != '#' &&
          lookahead != ';') ADVANCE(908);
      END_STATE();
    case 906:
      ACCEPT_TOKEN(sym_pie_title);
      if (lookahead == 'L' ||
          lookahead == 'l') ADVANCE(904);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != '#' &&
          lookahead != ';') ADVANCE(908);
      END_STATE();
    case 907:
      ACCEPT_TOKEN(sym_pie_title);
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(906);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != '#' &&
          lookahead != ';') ADVANCE(908);
      END_STATE();
    case 908:
      ACCEPT_TOKEN(sym_pie_title);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != '#' &&
          lookahead != ';') ADVANCE(908);
      END_STATE();
    case 909:
      ACCEPT_TOKEN(aux_sym_pie_label_token1);
      END_STATE();
    case 910:
      ACCEPT_TOKEN(sym_pie_value);
      if (lookahead == '.') ADVANCE(421);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(910);
      END_STATE();
    case 911:
      ACCEPT_TOKEN(sym_pie_value);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(911);
      END_STATE();
    case 912:
      ACCEPT_TOKEN(aux_sym_flowchart_direction_lr_token1);
      END_STATE();
    case 913:
      ACCEPT_TOKEN(aux_sym_flowchart_direction_lr_token2);
      END_STATE();
    case 914:
      ACCEPT_TOKEN(aux_sym_flowchart_direction_rl_token1);
      END_STATE();
    case 915:
      ACCEPT_TOKEN(aux_sym_flowchart_direction_tb_token1);
      END_STATE();
    case 916:
      ACCEPT_TOKEN(aux_sym_flowchart_direction_tb_token2);
      END_STATE();
    case 917:
      ACCEPT_TOKEN(anon_sym_v);
      END_STATE();
    case 918:
      ACCEPT_TOKEN(aux_sym_flowchart_direction_bt_token1);
      END_STATE();
    case 919:
      ACCEPT_TOKEN(anon_sym_CARET);
      END_STATE();
    case 920:
      ACCEPT_TOKEN(aux_sym_flow_text_literal_token1);
      if (lookahead == '\n') ADVANCE(585);
      if (lookahead == '{') ADVANCE(924);
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ' ||
          lookahead == ')' ||
          lookahead == '-' ||
          lookahead == '/' ||
          lookahead == ';' ||
          lookahead == '\\' ||
          lookahead == ']' ||
          lookahead == '|' ||
          lookahead == '}') ADVANCE(10);
      if (lookahead != 0) ADVANCE(921);
      END_STATE();
    case 921:
      ACCEPT_TOKEN(aux_sym_flow_text_literal_token1);
      if (lookahead == '\n') ADVANCE(584);
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ' ||
          lookahead == ')' ||
          lookahead == '-' ||
          lookahead == '/' ||
          lookahead == ';' ||
          lookahead == '\\' ||
          lookahead == ']' ||
          lookahead == '|' ||
          lookahead == '}') ADVANCE(10);
      if (lookahead != 0) ADVANCE(921);
      END_STATE();
    case 922:
      ACCEPT_TOKEN(aux_sym_flow_text_literal_token1);
      if (lookahead == '"') ADVANCE(924);
      if (sym_flow_text_quoted_character_set_1(lookahead)) ADVANCE(28);
      if (lookahead != 0 &&
          lookahead != '\n') ADVANCE(922);
      END_STATE();
    case 923:
      ACCEPT_TOKEN(aux_sym_flow_text_literal_token1);
      if (lookahead == '%') ADVANCE(920);
      if (!aux_sym_flow_text_literal_token1_character_set_1(lookahead)) ADVANCE(924);
      END_STATE();
    case 924:
      ACCEPT_TOKEN(aux_sym_flow_text_literal_token1);
      if (!aux_sym_flow_text_literal_token1_character_set_1(lookahead)) ADVANCE(924);
      END_STATE();
    case 925:
      ACCEPT_TOKEN(sym_flow_text_quoted);
      END_STATE();
    case 926:
      ACCEPT_TOKEN(aux_sym_flow_link_arrow_token1);
      END_STATE();
    case 927:
      ACCEPT_TOKEN(aux_sym_flow_link_arrow_token1);
      if (lookahead == '-') ADVANCE(927);
      if (lookahead == '>' ||
          lookahead == 'o' ||
          lookahead == 'x') ADVANCE(926);
      END_STATE();
    case 928:
      ACCEPT_TOKEN(aux_sym_flow_link_arrow_token2);
      END_STATE();
    case 929:
      ACCEPT_TOKEN(aux_sym_flow_link_arrow_token2);
      if (lookahead == '=') ADVANCE(929);
      if (lookahead == '>' ||
          lookahead == 'o' ||
          lookahead == 'x') ADVANCE(928);
      END_STATE();
    case 930:
      ACCEPT_TOKEN(aux_sym_flow_link_arrow_token3);
      END_STATE();
    case 931:
      ACCEPT_TOKEN(aux_sym_flow_link_arrow_start_token1);
      if (lookahead == '-') ADVANCE(927);
      if (lookahead == '>' ||
          lookahead == 'o' ||
          lookahead == 'x') ADVANCE(926);
      END_STATE();
    case 932:
      ACCEPT_TOKEN(aux_sym_flow_link_arrow_start_token2);
      if (lookahead == '=') ADVANCE(929);
      if (lookahead == '>' ||
          lookahead == 'o' ||
          lookahead == 'x') ADVANCE(928);
      END_STATE();
    case 933:
      ACCEPT_TOKEN(aux_sym_flow_link_arrow_start_token3);
      if (lookahead == '-') ADVANCE(419);
      if (lookahead == '.') ADVANCE(933);
      END_STATE();
    case 934:
      ACCEPT_TOKEN(sym__er_alphanum);
      if (lookahead == '-' ||
          ('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(934);
      END_STATE();
    case 935:
      ACCEPT_TOKEN(anon_sym_PIPEo);
      END_STATE();
    case 936:
      ACCEPT_TOKEN(anon_sym_o_PIPE);
      END_STATE();
    case 937:
      ACCEPT_TOKEN(anon_sym_RBRACEo);
      END_STATE();
    case 938:
      ACCEPT_TOKEN(anon_sym_o_LBRACE);
      END_STATE();
    case 939:
      ACCEPT_TOKEN(anon_sym_RBRACE_PIPE);
      END_STATE();
    case 940:
      ACCEPT_TOKEN(anon_sym_PIPE_LBRACE);
      END_STATE();
    case 941:
      ACCEPT_TOKEN(sym_er_cardinarity_only_one);
      END_STATE();
    case 942:
      ACCEPT_TOKEN(anon_sym_DOT_DASH);
      END_STATE();
    case 943:
      ACCEPT_TOKEN(anon_sym_DASH_DOT);
      END_STATE();
    case 944:
      ACCEPT_TOKEN(aux_sym_er_attribute_key_type_pk_token1);
      END_STATE();
    case 945:
      ACCEPT_TOKEN(aux_sym_er_attribute_key_type_fk_token1);
      END_STATE();
    case 946:
      ACCEPT_TOKEN(sym__mindmap_text);
      if (lookahead == '\n') ADVANCE(585);
      if (lookahead == '\r' ||
          lookahead == '(' ||
          lookahead == ')' ||
          lookahead == '[' ||
          lookahead == ']' ||
          lookahead == '}') ADVANCE(10);
      if (lookahead != 0 &&
          lookahead != '{') ADVANCE(947);
      END_STATE();
    case 947:
      ACCEPT_TOKEN(sym__mindmap_text);
      if (lookahead == '\n') ADVANCE(584);
      if (lookahead == '\r' ||
          lookahead == '(' ||
          lookahead == ')' ||
          lookahead == '[' ||
          lookahead == ']' ||
          lookahead == '{' ||
          lookahead == '}') ADVANCE(10);
      if (lookahead != 0) ADVANCE(947);
      END_STATE();
    case 948:
      ACCEPT_TOKEN(sym__mindmap_text);
      if (lookahead == '%') ADVANCE(946);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != '\r' &&
          lookahead != '(' &&
          lookahead != ')' &&
          lookahead != '[' &&
          lookahead != ']' &&
          lookahead != '{' &&
          lookahead != '}') ADVANCE(949);
      END_STATE();
    case 949:
      ACCEPT_TOKEN(sym__mindmap_text);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != '\r' &&
          lookahead != '(' &&
          lookahead != ')' &&
          lookahead != '[' &&
          lookahead != ']' &&
          lookahead != '{' &&
          lookahead != '}') ADVANCE(949);
      END_STATE();
    default:
      return false;
  }
}

static const TSLexMode ts_lex_modes[STATE_COUNT] = {
  [0] = {.lex_state = 0},
  [1] = {.lex_state = 427},
  [2] = {.lex_state = 3},
  [3] = {.lex_state = 5},
  [4] = {.lex_state = 5},
  [5] = {.lex_state = 3},
  [6] = {.lex_state = 5},
  [7] = {.lex_state = 5},
  [8] = {.lex_state = 3},
  [9] = {.lex_state = 5},
  [10] = {.lex_state = 5},
  [11] = {.lex_state = 5},
  [12] = {.lex_state = 3},
  [13] = {.lex_state = 5},
  [14] = {.lex_state = 5},
  [15] = {.lex_state = 5},
  [16] = {.lex_state = 3},
  [17] = {.lex_state = 5},
  [18] = {.lex_state = 3},
  [19] = {.lex_state = 425},
  [20] = {.lex_state = 4},
  [21] = {.lex_state = 4},
  [22] = {.lex_state = 4},
  [23] = {.lex_state = 4},
  [24] = {.lex_state = 4},
  [25] = {.lex_state = 4},
  [26] = {.lex_state = 4},
  [27] = {.lex_state = 4},
  [28] = {.lex_state = 4},
  [29] = {.lex_state = 4},
  [30] = {.lex_state = 4},
  [31] = {.lex_state = 4},
  [32] = {.lex_state = 4},
  [33] = {.lex_state = 4},
  [34] = {.lex_state = 4},
  [35] = {.lex_state = 4},
  [36] = {.lex_state = 4},
  [37] = {.lex_state = 4},
  [38] = {.lex_state = 425},
  [39] = {.lex_state = 425},
  [40] = {.lex_state = 4},
  [41] = {.lex_state = 4},
  [42] = {.lex_state = 4},
  [43] = {.lex_state = 4},
  [44] = {.lex_state = 4},
  [45] = {.lex_state = 4},
  [46] = {.lex_state = 4},
  [47] = {.lex_state = 425},
  [48] = {.lex_state = 425},
  [49] = {.lex_state = 425},
  [50] = {.lex_state = 425},
  [51] = {.lex_state = 425},
  [52] = {.lex_state = 425},
  [53] = {.lex_state = 425},
  [54] = {.lex_state = 425},
  [55] = {.lex_state = 425},
  [56] = {.lex_state = 425},
  [57] = {.lex_state = 425},
  [58] = {.lex_state = 425},
  [59] = {.lex_state = 425},
  [60] = {.lex_state = 425},
  [61] = {.lex_state = 425},
  [62] = {.lex_state = 427},
  [63] = {.lex_state = 435},
  [64] = {.lex_state = 435},
  [65] = {.lex_state = 435},
  [66] = {.lex_state = 435},
  [67] = {.lex_state = 435},
  [68] = {.lex_state = 435},
  [69] = {.lex_state = 435},
  [70] = {.lex_state = 435},
  [71] = {.lex_state = 435},
  [72] = {.lex_state = 435},
  [73] = {.lex_state = 435},
  [74] = {.lex_state = 433},
  [75] = {.lex_state = 433},
  [76] = {.lex_state = 435},
  [77] = {.lex_state = 433},
  [78] = {.lex_state = 435},
  [79] = {.lex_state = 435},
  [80] = {.lex_state = 435},
  [81] = {.lex_state = 435},
  [82] = {.lex_state = 435},
  [83] = {.lex_state = 427},
  [84] = {.lex_state = 427},
  [85] = {.lex_state = 3},
  [86] = {.lex_state = 5},
  [87] = {.lex_state = 3},
  [88] = {.lex_state = 3},
  [89] = {.lex_state = 3},
  [90] = {.lex_state = 3},
  [91] = {.lex_state = 3},
  [92] = {.lex_state = 3},
  [93] = {.lex_state = 3},
  [94] = {.lex_state = 3},
  [95] = {.lex_state = 3},
  [96] = {.lex_state = 3},
  [97] = {.lex_state = 3},
  [98] = {.lex_state = 3},
  [99] = {.lex_state = 3},
  [100] = {.lex_state = 3},
  [101] = {.lex_state = 3},
  [102] = {.lex_state = 3},
  [103] = {.lex_state = 3},
  [104] = {.lex_state = 3},
  [105] = {.lex_state = 3},
  [106] = {.lex_state = 3},
  [107] = {.lex_state = 3},
  [108] = {.lex_state = 5},
  [109] = {.lex_state = 5},
  [110] = {.lex_state = 5},
  [111] = {.lex_state = 5},
  [112] = {.lex_state = 5},
  [113] = {.lex_state = 3},
  [114] = {.lex_state = 3},
  [115] = {.lex_state = 5},
  [116] = {.lex_state = 3},
  [117] = {.lex_state = 3},
  [118] = {.lex_state = 3},
  [119] = {.lex_state = 3},
  [120] = {.lex_state = 5},
  [121] = {.lex_state = 5},
  [122] = {.lex_state = 5},
  [123] = {.lex_state = 5},
  [124] = {.lex_state = 5},
  [125] = {.lex_state = 5},
  [126] = {.lex_state = 5},
  [127] = {.lex_state = 5},
  [128] = {.lex_state = 5},
  [129] = {.lex_state = 3},
  [130] = {.lex_state = 5},
  [131] = {.lex_state = 3},
  [132] = {.lex_state = 5},
  [133] = {.lex_state = 5},
  [134] = {.lex_state = 5},
  [135] = {.lex_state = 5},
  [136] = {.lex_state = 5},
  [137] = {.lex_state = 5},
  [138] = {.lex_state = 5},
  [139] = {.lex_state = 3},
  [140] = {.lex_state = 5},
  [141] = {.lex_state = 5},
  [142] = {.lex_state = 5},
  [143] = {.lex_state = 5},
  [144] = {.lex_state = 5},
  [145] = {.lex_state = 5},
  [146] = {.lex_state = 3},
  [147] = {.lex_state = 5},
  [148] = {.lex_state = 5},
  [149] = {.lex_state = 4},
  [150] = {.lex_state = 4},
  [151] = {.lex_state = 4},
  [152] = {.lex_state = 4},
  [153] = {.lex_state = 4},
  [154] = {.lex_state = 425},
  [155] = {.lex_state = 4},
  [156] = {.lex_state = 4},
  [157] = {.lex_state = 425},
  [158] = {.lex_state = 4},
  [159] = {.lex_state = 4},
  [160] = {.lex_state = 4},
  [161] = {.lex_state = 425},
  [162] = {.lex_state = 425},
  [163] = {.lex_state = 425},
  [164] = {.lex_state = 4},
  [165] = {.lex_state = 4},
  [166] = {.lex_state = 4},
  [167] = {.lex_state = 4},
  [168] = {.lex_state = 4},
  [169] = {.lex_state = 4},
  [170] = {.lex_state = 4},
  [171] = {.lex_state = 4},
  [172] = {.lex_state = 4},
  [173] = {.lex_state = 425},
  [174] = {.lex_state = 4},
  [175] = {.lex_state = 4},
  [176] = {.lex_state = 4},
  [177] = {.lex_state = 4},
  [178] = {.lex_state = 4},
  [179] = {.lex_state = 425},
  [180] = {.lex_state = 4},
  [181] = {.lex_state = 425},
  [182] = {.lex_state = 425},
  [183] = {.lex_state = 425},
  [184] = {.lex_state = 425},
  [185] = {.lex_state = 425},
  [186] = {.lex_state = 425},
  [187] = {.lex_state = 4},
  [188] = {.lex_state = 425},
  [189] = {.lex_state = 425},
  [190] = {.lex_state = 425},
  [191] = {.lex_state = 4},
  [192] = {.lex_state = 425},
  [193] = {.lex_state = 425},
  [194] = {.lex_state = 425},
  [195] = {.lex_state = 425},
  [196] = {.lex_state = 425},
  [197] = {.lex_state = 425},
  [198] = {.lex_state = 425},
  [199] = {.lex_state = 4},
  [200] = {.lex_state = 4},
  [201] = {.lex_state = 425},
  [202] = {.lex_state = 4},
  [203] = {.lex_state = 425},
  [204] = {.lex_state = 425},
  [205] = {.lex_state = 425},
  [206] = {.lex_state = 425},
  [207] = {.lex_state = 4},
  [208] = {.lex_state = 4},
  [209] = {.lex_state = 425},
  [210] = {.lex_state = 425},
  [211] = {.lex_state = 425},
  [212] = {.lex_state = 426},
  [213] = {.lex_state = 429},
  [214] = {.lex_state = 427},
  [215] = {.lex_state = 427},
  [216] = {.lex_state = 429},
  [217] = {.lex_state = 436},
  [218] = {.lex_state = 427},
  [219] = {.lex_state = 429},
  [220] = {.lex_state = 436},
  [221] = {.lex_state = 437},
  [222] = {.lex_state = 429},
  [223] = {.lex_state = 14},
  [224] = {.lex_state = 427},
  [225] = {.lex_state = 435},
  [226] = {.lex_state = 429},
  [227] = {.lex_state = 12},
  [228] = {.lex_state = 14},
  [229] = {.lex_state = 2},
  [230] = {.lex_state = 435},
  [231] = {.lex_state = 435},
  [232] = {.lex_state = 435},
  [233] = {.lex_state = 435},
  [234] = {.lex_state = 435},
  [235] = {.lex_state = 435},
  [236] = {.lex_state = 435},
  [237] = {.lex_state = 43},
  [238] = {.lex_state = 436},
  [239] = {.lex_state = 435},
  [240] = {.lex_state = 435},
  [241] = {.lex_state = 435},
  [242] = {.lex_state = 435},
  [243] = {.lex_state = 435},
  [244] = {.lex_state = 43},
  [245] = {.lex_state = 435},
  [246] = {.lex_state = 435},
  [247] = {.lex_state = 435},
  [248] = {.lex_state = 435},
  [249] = {.lex_state = 435},
  [250] = {.lex_state = 24},
  [251] = {.lex_state = 435},
  [252] = {.lex_state = 441},
  [253] = {.lex_state = 435},
  [254] = {.lex_state = 435},
  [255] = {.lex_state = 435},
  [256] = {.lex_state = 435},
  [257] = {.lex_state = 2},
  [258] = {.lex_state = 429},
  [259] = {.lex_state = 24},
  [260] = {.lex_state = 24},
  [261] = {.lex_state = 24},
  [262] = {.lex_state = 435},
  [263] = {.lex_state = 429},
  [264] = {.lex_state = 43},
  [265] = {.lex_state = 435},
  [266] = {.lex_state = 441},
  [267] = {.lex_state = 435},
  [268] = {.lex_state = 433},
  [269] = {.lex_state = 433},
  [270] = {.lex_state = 11},
  [271] = {.lex_state = 441},
  [272] = {.lex_state = 11},
  [273] = {.lex_state = 11},
  [274] = {.lex_state = 11},
  [275] = {.lex_state = 437},
  [276] = {.lex_state = 429},
  [277] = {.lex_state = 427},
  [278] = {.lex_state = 433},
  [279] = {.lex_state = 433},
  [280] = {.lex_state = 11},
  [281] = {.lex_state = 433},
  [282] = {.lex_state = 433},
  [283] = {.lex_state = 433},
  [284] = {.lex_state = 433},
  [285] = {.lex_state = 43},
  [286] = {.lex_state = 11},
  [287] = {.lex_state = 435},
  [288] = {.lex_state = 429},
  [289] = {.lex_state = 433},
  [290] = {.lex_state = 433},
  [291] = {.lex_state = 433},
  [292] = {.lex_state = 43},
  [293] = {.lex_state = 433},
  [294] = {.lex_state = 11},
  [295] = {.lex_state = 441},
  [296] = {.lex_state = 11},
  [297] = {.lex_state = 427},
  [298] = {.lex_state = 427},
  [299] = {.lex_state = 33},
  [300] = {.lex_state = 437},
  [301] = {.lex_state = 437},
  [302] = {.lex_state = 427},
  [303] = {.lex_state = 439},
  [304] = {.lex_state = 427},
  [305] = {.lex_state = 427},
  [306] = {.lex_state = 439},
  [307] = {.lex_state = 437},
  [308] = {.lex_state = 439},
  [309] = {.lex_state = 11},
  [310] = {.lex_state = 11},
  [311] = {.lex_state = 437},
  [312] = {.lex_state = 11},
  [313] = {.lex_state = 437},
  [314] = {.lex_state = 439},
  [315] = {.lex_state = 33},
  [316] = {.lex_state = 427},
  [317] = {.lex_state = 427},
  [318] = {.lex_state = 427},
  [319] = {.lex_state = 427},
  [320] = {.lex_state = 427},
  [321] = {.lex_state = 427},
  [322] = {.lex_state = 427},
  [323] = {.lex_state = 439},
  [324] = {.lex_state = 427},
  [325] = {.lex_state = 427},
  [326] = {.lex_state = 427},
  [327] = {.lex_state = 426},
  [328] = {.lex_state = 427},
  [329] = {.lex_state = 427},
  [330] = {.lex_state = 439},
  [331] = {.lex_state = 427},
  [332] = {.lex_state = 427},
  [333] = {.lex_state = 427},
  [334] = {.lex_state = 427},
  [335] = {.lex_state = 427},
  [336] = {.lex_state = 427},
  [337] = {.lex_state = 432},
  [338] = {.lex_state = 432},
  [339] = {.lex_state = 432},
  [340] = {.lex_state = 11},
  [341] = {.lex_state = 432},
  [342] = {.lex_state = 432},
  [343] = {.lex_state = 24},
  [344] = {.lex_state = 427},
  [345] = {.lex_state = 427},
  [346] = {.lex_state = 427},
  [347] = {.lex_state = 427},
  [348] = {.lex_state = 427},
  [349] = {.lex_state = 24},
  [350] = {.lex_state = 24},
  [351] = {.lex_state = 432},
  [352] = {.lex_state = 427},
  [353] = {.lex_state = 7},
  [354] = {.lex_state = 7},
  [355] = {.lex_state = 427},
  [356] = {.lex_state = 430},
  [357] = {.lex_state = 430},
  [358] = {.lex_state = 427},
  [359] = {.lex_state = 7},
  [360] = {.lex_state = 427},
  [361] = {.lex_state = 428},
  [362] = {.lex_state = 428},
  [363] = {.lex_state = 435},
  [364] = {.lex_state = 428},
  [365] = {.lex_state = 428},
  [366] = {.lex_state = 7},
  [367] = {.lex_state = 428},
  [368] = {.lex_state = 439},
  [369] = {.lex_state = 432},
  [370] = {.lex_state = 438},
  [371] = {.lex_state = 439},
  [372] = {.lex_state = 428},
  [373] = {.lex_state = 427},
  [374] = {.lex_state = 439},
  [375] = {.lex_state = 427},
  [376] = {.lex_state = 439},
  [377] = {.lex_state = 439},
  [378] = {.lex_state = 430},
  [379] = {.lex_state = 426},
  [380] = {.lex_state = 430},
  [381] = {.lex_state = 428},
  [382] = {.lex_state = 432},
  [383] = {.lex_state = 438},
  [384] = {.lex_state = 436},
  [385] = {.lex_state = 427},
  [386] = {.lex_state = 427},
  [387] = {.lex_state = 27},
  [388] = {.lex_state = 27},
  [389] = {.lex_state = 428},
  [390] = {.lex_state = 21},
  [391] = {.lex_state = 27},
  [392] = {.lex_state = 432},
  [393] = {.lex_state = 436},
  [394] = {.lex_state = 428},
  [395] = {.lex_state = 27},
  [396] = {.lex_state = 27},
  [397] = {.lex_state = 27},
  [398] = {.lex_state = 436},
  [399] = {.lex_state = 27},
  [400] = {.lex_state = 428},
  [401] = {.lex_state = 435},
  [402] = {.lex_state = 27},
  [403] = {.lex_state = 428},
  [404] = {.lex_state = 32},
  [405] = {.lex_state = 430},
  [406] = {.lex_state = 42},
  [407] = {.lex_state = 27},
  [408] = {.lex_state = 21},
  [409] = {.lex_state = 430},
  [410] = {.lex_state = 21},
  [411] = {.lex_state = 21},
  [412] = {.lex_state = 21},
  [413] = {.lex_state = 21},
  [414] = {.lex_state = 428},
  [415] = {.lex_state = 428},
  [416] = {.lex_state = 27},
  [417] = {.lex_state = 32},
  [418] = {.lex_state = 27},
  [419] = {.lex_state = 430},
  [420] = {.lex_state = 430},
  [421] = {.lex_state = 427},
  [422] = {.lex_state = 27},
  [423] = {.lex_state = 430},
  [424] = {.lex_state = 430},
  [425] = {.lex_state = 27},
  [426] = {.lex_state = 42},
  [427] = {.lex_state = 430},
  [428] = {.lex_state = 427},
  [429] = {.lex_state = 25},
  [430] = {.lex_state = 437},
  [431] = {.lex_state = 431},
  [432] = {.lex_state = 430},
  [433] = {.lex_state = 427},
  [434] = {.lex_state = 427},
  [435] = {.lex_state = 43},
  [436] = {.lex_state = 434},
  [437] = {.lex_state = 432},
  [438] = {.lex_state = 427},
  [439] = {.lex_state = 434},
  [440] = {.lex_state = 427},
  [441] = {.lex_state = 427},
  [442] = {.lex_state = 432},
  [443] = {.lex_state = 432},
  [444] = {.lex_state = 432},
  [445] = {.lex_state = 432},
  [446] = {.lex_state = 427},
  [447] = {.lex_state = 13},
  [448] = {.lex_state = 427},
  [449] = {.lex_state = 13},
  [450] = {.lex_state = 429},
  [451] = {.lex_state = 434},
  [452] = {.lex_state = 427},
  [453] = {.lex_state = 13},
  [454] = {.lex_state = 436},
  [455] = {.lex_state = 428},
  [456] = {.lex_state = 13},
  [457] = {.lex_state = 427},
  [458] = {.lex_state = 13},
  [459] = {.lex_state = 6},
  [460] = {.lex_state = 436},
  [461] = {.lex_state = 6},
  [462] = {.lex_state = 427},
  [463] = {.lex_state = 426},
  [464] = {.lex_state = 429},
  [465] = {.lex_state = 434},
  [466] = {.lex_state = 426},
  [467] = {.lex_state = 429},
  [468] = {.lex_state = 426},
  [469] = {.lex_state = 426},
  [470] = {.lex_state = 440},
  [471] = {.lex_state = 436},
  [472] = {.lex_state = 432},
  [473] = {.lex_state = 427},
  [474] = {.lex_state = 440},
  [475] = {.lex_state = 426},
  [476] = {.lex_state = 426},
  [477] = {.lex_state = 427},
  [478] = {.lex_state = 13},
  [479] = {.lex_state = 13},
  [480] = {.lex_state = 427},
  [481] = {.lex_state = 427},
  [482] = {.lex_state = 2},
  [483] = {.lex_state = 2},
  [484] = {.lex_state = 13},
  [485] = {.lex_state = 2},
  [486] = {.lex_state = 2},
  [487] = {.lex_state = 13},
  [488] = {.lex_state = 2},
  [489] = {.lex_state = 2},
  [490] = {.lex_state = 13},
  [491] = {.lex_state = 427},
  [492] = {.lex_state = 427},
  [493] = {.lex_state = 2},
  [494] = {.lex_state = 427},
  [495] = {.lex_state = 2},
  [496] = {.lex_state = 2},
  [497] = {.lex_state = 13},
  [498] = {.lex_state = 2},
  [499] = {.lex_state = 435},
  [500] = {.lex_state = 2},
  [501] = {.lex_state = 435},
  [502] = {.lex_state = 435},
  [503] = {.lex_state = 435},
  [504] = {.lex_state = 435},
  [505] = {.lex_state = 435},
  [506] = {.lex_state = 435},
  [507] = {.lex_state = 435},
  [508] = {.lex_state = 435},
  [509] = {.lex_state = 435},
  [510] = {.lex_state = 427},
  [511] = {.lex_state = 432},
  [512] = {.lex_state = 2},
  [513] = {.lex_state = 2},
  [514] = {.lex_state = 430},
  [515] = {.lex_state = 13},
  [516] = {.lex_state = 13},
  [517] = {.lex_state = 427},
  [518] = {.lex_state = 13},
  [519] = {.lex_state = 13},
  [520] = {.lex_state = 2},
  [521] = {.lex_state = 2},
  [522] = {.lex_state = 2},
  [523] = {.lex_state = 2},
  [524] = {.lex_state = 427},
  [525] = {.lex_state = 13},
  [526] = {.lex_state = 13},
  [527] = {.lex_state = 440},
  [528] = {.lex_state = 13},
  [529] = {.lex_state = 2},
  [530] = {.lex_state = 2},
  [531] = {.lex_state = 2},
  [532] = {.lex_state = 2},
  [533] = {.lex_state = 435},
  [534] = {.lex_state = 13},
  [535] = {.lex_state = 13},
  [536] = {.lex_state = 13},
  [537] = {.lex_state = 435},
  [538] = {.lex_state = 13},
  [539] = {.lex_state = 435},
  [540] = {.lex_state = 2},
  [541] = {.lex_state = 2},
  [542] = {.lex_state = 2},
  [543] = {.lex_state = 2},
  [544] = {.lex_state = 13},
  [545] = {.lex_state = 13},
  [546] = {.lex_state = 13},
  [547] = {.lex_state = 13},
  [548] = {.lex_state = 427},
  [549] = {.lex_state = 430},
  [550] = {.lex_state = 435},
  [551] = {.lex_state = 2},
  [552] = {.lex_state = 427},
  [553] = {.lex_state = 435},
  [554] = {.lex_state = 2},
  [555] = {.lex_state = 435},
  [556] = {.lex_state = 435},
  [557] = {.lex_state = 2},
  [558] = {.lex_state = 2},
  [559] = {.lex_state = 427},
  [560] = {.lex_state = 13},
  [561] = {.lex_state = 13},
  [562] = {.lex_state = 13},
  [563] = {.lex_state = 435},
  [564] = {.lex_state = 435},
  [565] = {.lex_state = 13},
  [566] = {.lex_state = 427},
  [567] = {.lex_state = 427},
  [568] = {.lex_state = 2},
  [569] = {.lex_state = 2},
  [570] = {.lex_state = 435},
  [571] = {.lex_state = 13},
  [572] = {.lex_state = 2},
  [573] = {.lex_state = 427},
  [574] = {.lex_state = 2},
  [575] = {.lex_state = 13},
  [576] = {.lex_state = 13},
  [577] = {.lex_state = 13},
  [578] = {.lex_state = 427},
  [579] = {.lex_state = 31},
  [580] = {.lex_state = 427},
  [581] = {.lex_state = 13},
  [582] = {.lex_state = 25},
  [583] = {.lex_state = 2},
  [584] = {.lex_state = 25},
  [585] = {.lex_state = 2},
  [586] = {.lex_state = 2},
  [587] = {.lex_state = 2},
  [588] = {.lex_state = 427},
  [589] = {.lex_state = 427},
  [590] = {.lex_state = 2},
  [591] = {.lex_state = 2},
  [592] = {.lex_state = 13},
  [593] = {.lex_state = 13},
  [594] = {.lex_state = 13},
  [595] = {.lex_state = 427},
  [596] = {.lex_state = 427},
  [597] = {.lex_state = 2},
  [598] = {.lex_state = 427},
  [599] = {.lex_state = 427},
  [600] = {.lex_state = 2},
  [601] = {.lex_state = 2},
  [602] = {.lex_state = 2},
  [603] = {.lex_state = 432},
  [604] = {.lex_state = 427},
  [605] = {.lex_state = 2},
  [606] = {.lex_state = 2},
  [607] = {.lex_state = 13},
  [608] = {.lex_state = 13},
  [609] = {.lex_state = 13},
  [610] = {.lex_state = 13},
  [611] = {.lex_state = 427},
  [612] = {.lex_state = 13},
  [613] = {.lex_state = 430},
  [614] = {.lex_state = 427},
  [615] = {.lex_state = 427},
  [616] = {.lex_state = 427},
  [617] = {.lex_state = 13},
  [618] = {.lex_state = 427},
  [619] = {.lex_state = 427},
  [620] = {.lex_state = 13},
  [621] = {.lex_state = 13},
  [622] = {.lex_state = 427},
  [623] = {.lex_state = 2},
  [624] = {.lex_state = 427},
  [625] = {.lex_state = 430},
  [626] = {.lex_state = 13},
  [627] = {.lex_state = 13},
  [628] = {.lex_state = 31},
  [629] = {.lex_state = 13},
  [630] = {.lex_state = 13},
  [631] = {.lex_state = 430},
  [632] = {.lex_state = 427},
  [633] = {.lex_state = 427},
  [634] = {.lex_state = 427},
  [635] = {.lex_state = 427},
  [636] = {.lex_state = 432},
  [637] = {.lex_state = 13},
  [638] = {.lex_state = 13},
  [639] = {.lex_state = 13},
  [640] = {.lex_state = 430},
  [641] = {.lex_state = 427},
  [642] = {.lex_state = 25},
  [643] = {.lex_state = 432},
  [644] = {.lex_state = 427},
  [645] = {.lex_state = 427},
  [646] = {.lex_state = 427},
  [647] = {.lex_state = 432},
  [648] = {.lex_state = 427},
  [649] = {.lex_state = 25},
  [650] = {.lex_state = 430},
  [651] = {.lex_state = 2},
  [652] = {.lex_state = 427},
  [653] = {.lex_state = 2},
  [654] = {.lex_state = 7},
  [655] = {.lex_state = 7},
  [656] = {.lex_state = 7},
  [657] = {.lex_state = 427},
  [658] = {.lex_state = 427},
  [659] = {.lex_state = 427},
  [660] = {.lex_state = 427},
  [661] = {.lex_state = 427},
  [662] = {.lex_state = 427},
  [663] = {.lex_state = 13},
  [664] = {.lex_state = 427},
  [665] = {.lex_state = 427},
  [666] = {.lex_state = 427},
  [667] = {.lex_state = 13},
  [668] = {.lex_state = 427},
  [669] = {.lex_state = 427},
  [670] = {.lex_state = 432},
  [671] = {.lex_state = 427},
  [672] = {.lex_state = 428},
  [673] = {.lex_state = 427},
  [674] = {.lex_state = 427},
  [675] = {.lex_state = 427},
  [676] = {.lex_state = 427},
  [677] = {.lex_state = 427},
  [678] = {.lex_state = 427},
  [679] = {.lex_state = 427},
  [680] = {.lex_state = 427},
  [681] = {.lex_state = 427},
  [682] = {.lex_state = 427},
  [683] = {.lex_state = 427},
  [684] = {.lex_state = 25},
  [685] = {.lex_state = 435},
  [686] = {.lex_state = 427},
  [687] = {.lex_state = 427},
  [688] = {.lex_state = 434},
  [689] = {.lex_state = 25},
  [690] = {.lex_state = 435},
  [691] = {.lex_state = 427},
  [692] = {.lex_state = 13},
  [693] = {.lex_state = 434},
  [694] = {.lex_state = 427},
  [695] = {.lex_state = 427},
  [696] = {.lex_state = 428},
  [697] = {.lex_state = 427},
  [698] = {.lex_state = 13},
  [699] = {.lex_state = 13},
  [700] = {.lex_state = 13},
  [701] = {.lex_state = 13},
  [702] = {.lex_state = 13},
  [703] = {.lex_state = 13},
  [704] = {.lex_state = 13},
  [705] = {.lex_state = 427},
  [706] = {.lex_state = 427},
  [707] = {.lex_state = 427},
  [708] = {.lex_state = 427},
  [709] = {.lex_state = 428},
  [710] = {.lex_state = 432},
  [711] = {.lex_state = 435},
  [712] = {.lex_state = 13},
  [713] = {.lex_state = 427},
  [714] = {.lex_state = 13},
  [715] = {.lex_state = 427},
  [716] = {.lex_state = 13},
  [717] = {.lex_state = 427},
  [718] = {.lex_state = 13},
  [719] = {.lex_state = 13},
  [720] = {.lex_state = 13},
  [721] = {.lex_state = 427},
  [722] = {.lex_state = 25},
  [723] = {.lex_state = 25},
  [724] = {.lex_state = 13},
  [725] = {.lex_state = 13},
  [726] = {.lex_state = 427},
  [727] = {.lex_state = 13},
  [728] = {.lex_state = 427},
  [729] = {.lex_state = 427},
  [730] = {.lex_state = 428},
  [731] = {.lex_state = 35},
  [732] = {.lex_state = 427},
  [733] = {.lex_state = 13},
  [734] = {.lex_state = 427},
  [735] = {.lex_state = 427},
  [736] = {.lex_state = 428},
  [737] = {.lex_state = 427},
  [738] = {.lex_state = 427},
  [739] = {.lex_state = 427},
  [740] = {.lex_state = 427},
  [741] = {.lex_state = 427},
  [742] = {.lex_state = 427},
  [743] = {.lex_state = 427},
  [744] = {.lex_state = 427},
  [745] = {.lex_state = 13},
  [746] = {.lex_state = 427},
  [747] = {.lex_state = 427},
  [748] = {.lex_state = 427},
  [749] = {.lex_state = 432},
  [750] = {.lex_state = 427},
  [751] = {.lex_state = 427},
  [752] = {.lex_state = 427},
  [753] = {.lex_state = 427},
  [754] = {.lex_state = 427},
  [755] = {.lex_state = 427},
  [756] = {.lex_state = 427},
  [757] = {.lex_state = 45},
  [758] = {.lex_state = 427},
  [759] = {.lex_state = 428},
  [760] = {.lex_state = 435},
  [761] = {.lex_state = 427},
  [762] = {.lex_state = 427},
  [763] = {.lex_state = 427},
  [764] = {.lex_state = 427},
  [765] = {.lex_state = 427},
  [766] = {.lex_state = 427},
  [767] = {.lex_state = 427},
  [768] = {.lex_state = 428},
  [769] = {.lex_state = 1},
  [770] = {.lex_state = 427},
  [771] = {.lex_state = 36},
  [772] = {.lex_state = 427},
  [773] = {.lex_state = 427},
  [774] = {.lex_state = 427},
  [775] = {.lex_state = 427},
  [776] = {.lex_state = 427},
  [777] = {.lex_state = 427},
  [778] = {.lex_state = 427},
  [779] = {.lex_state = 427},
  [780] = {.lex_state = 427},
  [781] = {.lex_state = 427},
  [782] = {.lex_state = 427},
  [783] = {.lex_state = 427},
  [784] = {.lex_state = 429},
  [785] = {.lex_state = 2},
  [786] = {.lex_state = 427},
  [787] = {.lex_state = 427},
  [788] = {.lex_state = 427},
  [789] = {.lex_state = 427},
  [790] = {.lex_state = 427},
  [791] = {.lex_state = 427},
  [792] = {.lex_state = 427},
  [793] = {.lex_state = 427},
  [794] = {.lex_state = 2},
  [795] = {.lex_state = 427},
  [796] = {.lex_state = 427},
  [797] = {.lex_state = 44},
  [798] = {.lex_state = 427},
  [799] = {.lex_state = 427},
  [800] = {.lex_state = 38},
  [801] = {.lex_state = 427},
  [802] = {.lex_state = 39},
  [803] = {.lex_state = 435},
  [804] = {.lex_state = 40},
  [805] = {.lex_state = 427},
  [806] = {.lex_state = 427},
  [807] = {.lex_state = 427},
  [808] = {.lex_state = 35},
  [809] = {.lex_state = 432},
  [810] = {.lex_state = 427},
  [811] = {.lex_state = 427},
  [812] = {.lex_state = 427},
  [813] = {.lex_state = 427},
  [814] = {.lex_state = 427},
  [815] = {.lex_state = 427},
  [816] = {.lex_state = 427},
  [817] = {.lex_state = 427},
  [818] = {.lex_state = 427},
  [819] = {.lex_state = 435},
  [820] = {.lex_state = 427},
  [821] = {.lex_state = 427},
  [822] = {.lex_state = 427},
  [823] = {.lex_state = 427},
  [824] = {.lex_state = 588},
  [825] = {.lex_state = 435},
  [826] = {.lex_state = 429},
  [827] = {.lex_state = 427},
  [828] = {.lex_state = 427},
  [829] = {.lex_state = 427},
  [830] = {.lex_state = 432},
  [831] = {.lex_state = 427},
  [832] = {.lex_state = 427},
  [833] = {.lex_state = 427},
  [834] = {.lex_state = 427},
  [835] = {.lex_state = 427},
  [836] = {.lex_state = 427},
  [837] = {.lex_state = 427},
  [838] = {.lex_state = 427},
  [839] = {.lex_state = 427},
  [840] = {.lex_state = 427},
  [841] = {.lex_state = 427},
  [842] = {.lex_state = 427},
  [843] = {.lex_state = 427},
  [844] = {.lex_state = 427},
  [845] = {.lex_state = 435},
  [846] = {.lex_state = 427},
  [847] = {.lex_state = 429},
  [848] = {.lex_state = 435},
  [849] = {.lex_state = 432},
  [850] = {.lex_state = 427},
  [851] = {.lex_state = 427},
  [852] = {.lex_state = 427},
  [853] = {.lex_state = 427},
  [854] = {.lex_state = 429},
  [855] = {.lex_state = 427},
  [856] = {.lex_state = 435},
  [857] = {.lex_state = 427},
  [858] = {.lex_state = 427},
  [859] = {.lex_state = 427},
  [860] = {.lex_state = 435},
  [861] = {.lex_state = 427},
  [862] = {.lex_state = 427},
  [863] = {.lex_state = 427},
  [864] = {.lex_state = 427},
  [865] = {.lex_state = 427},
  [866] = {.lex_state = 35},
  [867] = {.lex_state = 40},
  [868] = {.lex_state = 427},
  [869] = {.lex_state = 427},
  [870] = {.lex_state = 427},
  [871] = {.lex_state = 432},
  [872] = {.lex_state = 427},
  [873] = {.lex_state = 427},
  [874] = {.lex_state = 427},
  [875] = {.lex_state = 427},
  [876] = {.lex_state = 427},
  [877] = {.lex_state = 427},
  [878] = {.lex_state = 427},
  [879] = {.lex_state = 427},
  [880] = {.lex_state = 427},
  [881] = {.lex_state = 427},
  [882] = {.lex_state = 427},
  [883] = {.lex_state = 2},
  [884] = {.lex_state = 2},
  [885] = {.lex_state = 427},
  [886] = {.lex_state = 432},
  [887] = {.lex_state = 432},
  [888] = {.lex_state = 2},
  [889] = {.lex_state = 427},
  [890] = {.lex_state = 427},
  [891] = {.lex_state = 427},
  [892] = {.lex_state = 427},
  [893] = {.lex_state = 427},
  [894] = {.lex_state = 427},
  [895] = {.lex_state = 427},
  [896] = {.lex_state = 427},
  [897] = {.lex_state = 38},
  [898] = {.lex_state = 427},
  [899] = {.lex_state = 427},
  [900] = {.lex_state = 427},
  [901] = {.lex_state = 427},
  [902] = {.lex_state = 435},
  [903] = {.lex_state = 427},
  [904] = {.lex_state = 41},
  [905] = {.lex_state = 41},
  [906] = {.lex_state = 427},
  [907] = {.lex_state = 427},
  [908] = {.lex_state = 427},
  [909] = {.lex_state = 427},
  [910] = {.lex_state = 435},
  [911] = {.lex_state = 427},
  [912] = {.lex_state = 427},
  [913] = {.lex_state = 427},
  [914] = {.lex_state = 427},
  [915] = {.lex_state = 427},
  [916] = {.lex_state = 427},
  [917] = {.lex_state = 427},
  [918] = {.lex_state = 427},
  [919] = {.lex_state = 435},
  [920] = {.lex_state = 427},
};

static const uint16_t ts_parse_table[LARGE_STATE_COUNT][SYMBOL_COUNT] = {
  [0] = {
    [ts_builtin_sym_end] = ACTIONS(1),
    [anon_sym_PERCENT_PERCENT_LBRACE] = ACTIONS(1),
    [aux_sym_diagram_sequence_token1] = ACTIONS(1),
    [aux_sym__sequence_participant_type_token1] = ACTIONS(1),
    [aux_sym__sequence_participant_type_token2] = ACTIONS(1),
    [aux_sym_sequence_stmt_participant_token1] = ACTIONS(1),
    [aux_sym_sequence_stmt_actor_token1] = ACTIONS(1),
    [anon_sym_COLON] = ACTIONS(1),
    [aux_sym_sequence_stmt_autonumber_token1] = ACTIONS(1),
    [aux_sym_sequence_stmt_activate_token1] = ACTIONS(1),
    [aux_sym_sequence_stmt_deactivate_token1] = ACTIONS(1),
    [aux_sym_sequence_stmt_note_token1] = ACTIONS(1),
    [anon_sym_COMMA] = ACTIONS(1),
    [aux_sym_sequence_stmt_links_token1] = ACTIONS(1),
    [aux_sym_sequence_stmt_link_token1] = ACTIONS(1),
    [aux_sym_sequence_stmt_properties_token1] = ACTIONS(1),
    [aux_sym_sequence_stmt_details_token1] = ACTIONS(1),
    [aux_sym_sequence_stmt_title_token1] = ACTIONS(1),
    [aux_sym_sequence_stmt_loop_token1] = ACTIONS(1),
    [aux_sym_sequence_stmt_loop_token2] = ACTIONS(1),
    [aux_sym_sequence_stmt_rect_token1] = ACTIONS(1),
    [aux_sym_sequence_stmt_alt_token1] = ACTIONS(1),
    [aux_sym_sequence_stmt_alt_token2] = ACTIONS(1),
    [aux_sym_sequence_stmt_par_token1] = ACTIONS(1),
    [aux_sym_sequence_stmt_par_token2] = ACTIONS(1),
    [aux_sym_diagram_class_token1] = ACTIONS(1),
    [aux_sym_diagram_class_token2] = ACTIONS(1),
    [anon_sym_TILDE] = ACTIONS(1),
    [anon_sym_class] = ACTIONS(1),
    [anon_sym_COLON_COLON_COLON] = ACTIONS(1),
    [anon_sym_LBRACE] = ACTIONS(1),
    [anon_sym_RBRACE] = ACTIONS(1),
    [anon_sym_PLUS] = ACTIONS(1),
    [anon_sym_DASH] = ACTIONS(1),
    [anon_sym_POUND] = ACTIONS(1),
    [anon_sym_LPAREN] = ACTIONS(1),
    [anon_sym_RPAREN] = ACTIONS(1),
    [anon_sym_STAR] = ACTIONS(1),
    [anon_sym_DOLLAR] = ACTIONS(1),
    [anon_sym_LT_LT] = ACTIONS(1),
    [aux_sym_diagram_state_token1] = ACTIONS(1),
    [aux_sym_diagram_state_token2] = ACTIONS(1),
    [aux_sym_state_stmt_simple_token1] = ACTIONS(1),
    [aux_sym_diagram_gantt_token1] = ACTIONS(1),
    [aux_sym_gantt_stmt_dateformat_token1] = ACTIONS(1),
    [aux_sym_gantt_stmt_inclusiveenddates_token1] = ACTIONS(1),
    [aux_sym_gantt_stmt_topaxis_token1] = ACTIONS(1),
    [aux_sym_gantt_stmt_axisformat_token1] = ACTIONS(1),
    [aux_sym_gantt_stmt_includes_token1] = ACTIONS(1),
    [aux_sym_gantt_stmt_excludes_token1] = ACTIONS(1),
    [aux_sym_gantt_stmt_todaymarker_token1] = ACTIONS(1),
    [aux_sym_gantt_stmt_section_token1] = ACTIONS(1),
    [aux_sym_diagram_pie_token1] = ACTIONS(1),
    [aux_sym_diagram_flow_token1] = ACTIONS(1),
    [anon_sym_SEMI] = ACTIONS(1),
    [aux_sym_flow_stmt_direction_token1] = ACTIONS(1),
    [anon_sym_AMP] = ACTIONS(1),
    [anon_sym_PIPE] = ACTIONS(1),
    [anon_sym_LBRACK] = ACTIONS(1),
    [anon_sym_RBRACK] = ACTIONS(1),
    [anon_sym_LPAREN_LPAREN] = ACTIONS(1),
    [anon_sym_RPAREN_RPAREN] = ACTIONS(1),
    [anon_sym_LPAREN_DASH] = ACTIONS(1),
    [anon_sym_LPAREN_LBRACK] = ACTIONS(1),
    [anon_sym_RBRACK_RPAREN] = ACTIONS(1),
    [anon_sym_LBRACK_LBRACK] = ACTIONS(1),
    [anon_sym_RBRACK_RBRACK] = ACTIONS(1),
    [anon_sym_LBRACK_PIPE] = ACTIONS(1),
    [anon_sym_PIPE_RBRACK] = ACTIONS(1),
    [anon_sym_LBRACK_LPAREN] = ACTIONS(1),
    [anon_sym_LBRACE_LBRACE] = ACTIONS(1),
    [anon_sym_RBRACE_RBRACE] = ACTIONS(1),
    [anon_sym_GT] = ACTIONS(1),
    [anon_sym_LBRACK_SLASH] = ACTIONS(1),
    [anon_sym_BSLASH_RBRACK] = ACTIONS(1),
    [anon_sym_LBRACK_BSLASH] = ACTIONS(1),
    [anon_sym_SLASH_RBRACK] = ACTIONS(1),
    [anon_sym_subgraph] = ACTIONS(1),
    [anon_sym_end] = ACTIONS(1),
    [aux_sym_diagram_er_token1] = ACTIONS(1),
    [aux_sym_diagram_mindmap_token1] = ACTIONS(1),
    [aux_sym_mmap_class_token1] = ACTIONS(1),
    [aux_sym_mmap_icon_token1] = ACTIONS(1),
    [sym__whitespace] = ACTIONS(3),
    [sym__newline] = ACTIONS(1),
    [sym_comment] = ACTIONS(5),
    [sym__md_start] = ACTIONS(1),
    [sym__md_end] = ACTIONS(1),
    [aux_sym_direction_tb_token1] = ACTIONS(1),
    [aux_sym_direction_bt_token1] = ACTIONS(1),
    [aux_sym_direction_rl_token1] = ACTIONS(1),
    [aux_sym_direction_lr_token1] = ACTIONS(1),
    [sym_solid_arrow] = ACTIONS(1),
    [sym_solid_open_arrow] = ACTIONS(1),
    [anon_sym_DASH_DASH_GT] = ACTIONS(1),
    [sym_dotted_point] = ACTIONS(1),
    [aux_sym_note_placement_left_token1] = ACTIONS(1),
    [aux_sym_note_placement_right_token1] = ACTIONS(1),
    [sym_class_reltype_aggregation] = ACTIONS(1),
    [anon_sym_LT_PIPE] = ACTIONS(1),
    [anon_sym_PIPE_GT] = ACTIONS(1),
    [anon_sym_LT] = ACTIONS(1),
    [anon_sym_DASH_DASH] = ACTIONS(1),
    [anon_sym_DOT_DOT] = ACTIONS(1),
    [aux_sym_state_hide_empty_description_token1] = ACTIONS(1),
    [aux_sym_pie_showdata_token1] = ACTIONS(1),
    [aux_sym_flowchart_direction_lr_token1] = ACTIONS(1),
    [aux_sym_flowchart_direction_lr_token2] = ACTIONS(1),
    [aux_sym_flowchart_direction_rl_token1] = ACTIONS(1),
    [aux_sym_flowchart_direction_tb_token1] = ACTIONS(1),
    [aux_sym_flowchart_direction_tb_token2] = ACTIONS(1),
    [anon_sym_v] = ACTIONS(1),
    [aux_sym_flowchart_direction_bt_token1] = ACTIONS(1),
    [anon_sym_CARET] = ACTIONS(1),
    [aux_sym_flow_link_arrow_token2] = ACTIONS(1),
    [aux_sym_flow_link_arrow_start_token2] = ACTIONS(1),
    [anon_sym_o_PIPE] = ACTIONS(1),
    [anon_sym_RBRACEo] = ACTIONS(1),
    [anon_sym_o_LBRACE] = ACTIONS(1),
    [anon_sym_RBRACE_PIPE] = ACTIONS(1),
    [anon_sym_PIPE_LBRACE] = ACTIONS(1),
    [sym_er_cardinarity_only_one] = ACTIONS(1),
    [anon_sym_DOT_DASH] = ACTIONS(1),
    [aux_sym_er_attribute_key_type_pk_token1] = ACTIONS(1),
    [aux_sym_er_attribute_key_type_fk_token1] = ACTIONS(1),
  },
  [1] = {
    [sym_source_file] = STATE(818),
    [sym_directive] = STATE(84),
    [sym_diagram_sequence] = STATE(798),
    [sym_diagram_class] = STATE(798),
    [sym_diagram_state] = STATE(798),
    [sym_diagram_gantt] = STATE(798),
    [sym_diagram_pie] = STATE(798),
    [sym_diagram_flow] = STATE(798),
    [sym_diagram_er] = STATE(798),
    [sym_diagram_mindmap] = STATE(798),
    [aux_sym_source_file_repeat1] = STATE(84),
    [anon_sym_PERCENT_PERCENT_LBRACE] = ACTIONS(7),
    [aux_sym_diagram_sequence_token1] = ACTIONS(9),
    [aux_sym_diagram_class_token1] = ACTIONS(11),
    [aux_sym_diagram_class_token2] = ACTIONS(13),
    [aux_sym_diagram_state_token1] = ACTIONS(15),
    [aux_sym_diagram_state_token2] = ACTIONS(17),
    [aux_sym_diagram_gantt_token1] = ACTIONS(19),
    [aux_sym_diagram_pie_token1] = ACTIONS(21),
    [aux_sym_diagram_flow_token1] = ACTIONS(23),
    [aux_sym_diagram_er_token1] = ACTIONS(25),
    [aux_sym_diagram_mindmap_token1] = ACTIONS(27),
    [sym__whitespace] = ACTIONS(5),
    [sym__newline] = ACTIONS(29),
    [sym_comment] = ACTIONS(5),
  },
};

static const uint16_t ts_small_parse_table[] = {
  [0] = 29,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(33), 1,
      aux_sym_sequence_stmt_actor_token1,
    ACTIONS(35), 1,
      aux_sym_sequence_stmt_autonumber_token1,
    ACTIONS(37), 1,
      aux_sym_sequence_stmt_activate_token1,
    ACTIONS(39), 1,
      aux_sym_sequence_stmt_deactivate_token1,
    ACTIONS(41), 1,
      aux_sym_sequence_stmt_note_token1,
    ACTIONS(43), 1,
      aux_sym_sequence_stmt_links_token1,
    ACTIONS(45), 1,
      aux_sym_sequence_stmt_link_token1,
    ACTIONS(47), 1,
      aux_sym_sequence_stmt_properties_token1,
    ACTIONS(49), 1,
      aux_sym_sequence_stmt_details_token1,
    ACTIONS(51), 1,
      aux_sym_sequence_stmt_title_token1,
    ACTIONS(53), 1,
      aux_sym_sequence_stmt_loop_token1,
    ACTIONS(55), 1,
      aux_sym_sequence_stmt_loop_token2,
    ACTIONS(57), 1,
      aux_sym_sequence_stmt_rect_token1,
    ACTIONS(59), 1,
      aux_sym_sequence_stmt_opt_token1,
    ACTIONS(61), 1,
      aux_sym_sequence_stmt_alt_token1,
    ACTIONS(63), 1,
      aux_sym_sequence_stmt_alt_token2,
    ACTIONS(65), 1,
      aux_sym_sequence_stmt_par_token1,
    ACTIONS(67), 1,
      sym__newline,
    ACTIONS(69), 1,
      sym__sequence_actor_word,
    STATE(18), 1,
      aux_sym__sequence_subdocument,
    STATE(129), 1,
      sym__sequence_stmt,
    STATE(257), 1,
      aux_sym_sequence_actor_repeat1,
    STATE(313), 1,
      sym_sequence_actor,
    STATE(551), 1,
      sym__sequence_participant_type,
    STATE(615), 1,
      aux_sym_sequence_stmt_alt_repeat1,
    ACTIONS(31), 2,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
    STATE(96), 17,
      sym_sequence_stmt_participant,
      sym_sequence_stmt_actor,
      sym_sequence_stmt_signal,
      sym_sequence_stmt_autonumber,
      sym_sequence_stmt_activate,
      sym_sequence_stmt_deactivate,
      sym_sequence_stmt_note,
      sym_sequence_stmt_links,
      sym_sequence_stmt_link,
      sym_sequence_stmt_properties,
      sym_sequence_stmt_details,
      sym_sequence_stmt_title,
      sym_sequence_stmt_loop,
      sym_sequence_stmt_rect,
      sym_sequence_stmt_opt,
      sym_sequence_stmt_alt,
      sym_sequence_stmt_par,
  [105] = 29,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(69), 1,
      sym__sequence_actor_word,
    ACTIONS(73), 1,
      aux_sym_sequence_stmt_actor_token1,
    ACTIONS(75), 1,
      aux_sym_sequence_stmt_autonumber_token1,
    ACTIONS(77), 1,
      aux_sym_sequence_stmt_activate_token1,
    ACTIONS(79), 1,
      aux_sym_sequence_stmt_deactivate_token1,
    ACTIONS(81), 1,
      aux_sym_sequence_stmt_note_token1,
    ACTIONS(83), 1,
      aux_sym_sequence_stmt_links_token1,
    ACTIONS(85), 1,
      aux_sym_sequence_stmt_link_token1,
    ACTIONS(87), 1,
      aux_sym_sequence_stmt_properties_token1,
    ACTIONS(89), 1,
      aux_sym_sequence_stmt_details_token1,
    ACTIONS(91), 1,
      aux_sym_sequence_stmt_title_token1,
    ACTIONS(93), 1,
      aux_sym_sequence_stmt_loop_token1,
    ACTIONS(95), 1,
      aux_sym_sequence_stmt_loop_token2,
    ACTIONS(97), 1,
      aux_sym_sequence_stmt_rect_token1,
    ACTIONS(99), 1,
      aux_sym_sequence_stmt_opt_token1,
    ACTIONS(101), 1,
      aux_sym_sequence_stmt_alt_token1,
    ACTIONS(103), 1,
      aux_sym_sequence_stmt_par_token1,
    ACTIONS(105), 1,
      aux_sym_sequence_stmt_par_token2,
    ACTIONS(107), 1,
      sym__newline,
    STATE(17), 1,
      aux_sym__sequence_subdocument,
    STATE(115), 1,
      sym__sequence_stmt,
    STATE(257), 1,
      aux_sym_sequence_actor_repeat1,
    STATE(301), 1,
      sym_sequence_actor,
    STATE(524), 1,
      aux_sym_sequence_stmt_par_repeat1,
    STATE(586), 1,
      sym__sequence_participant_type,
    ACTIONS(71), 2,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
    STATE(109), 17,
      sym_sequence_stmt_participant,
      sym_sequence_stmt_actor,
      sym_sequence_stmt_signal,
      sym_sequence_stmt_autonumber,
      sym_sequence_stmt_activate,
      sym_sequence_stmt_deactivate,
      sym_sequence_stmt_note,
      sym_sequence_stmt_links,
      sym_sequence_stmt_link,
      sym_sequence_stmt_properties,
      sym_sequence_stmt_details,
      sym_sequence_stmt_title,
      sym_sequence_stmt_loop,
      sym_sequence_stmt_rect,
      sym_sequence_stmt_opt,
      sym_sequence_stmt_alt,
      sym_sequence_stmt_par,
  [210] = 29,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(69), 1,
      sym__sequence_actor_word,
    ACTIONS(73), 1,
      aux_sym_sequence_stmt_actor_token1,
    ACTIONS(75), 1,
      aux_sym_sequence_stmt_autonumber_token1,
    ACTIONS(77), 1,
      aux_sym_sequence_stmt_activate_token1,
    ACTIONS(79), 1,
      aux_sym_sequence_stmt_deactivate_token1,
    ACTIONS(81), 1,
      aux_sym_sequence_stmt_note_token1,
    ACTIONS(83), 1,
      aux_sym_sequence_stmt_links_token1,
    ACTIONS(85), 1,
      aux_sym_sequence_stmt_link_token1,
    ACTIONS(87), 1,
      aux_sym_sequence_stmt_properties_token1,
    ACTIONS(89), 1,
      aux_sym_sequence_stmt_details_token1,
    ACTIONS(91), 1,
      aux_sym_sequence_stmt_title_token1,
    ACTIONS(93), 1,
      aux_sym_sequence_stmt_loop_token1,
    ACTIONS(97), 1,
      aux_sym_sequence_stmt_rect_token1,
    ACTIONS(99), 1,
      aux_sym_sequence_stmt_opt_token1,
    ACTIONS(101), 1,
      aux_sym_sequence_stmt_alt_token1,
    ACTIONS(103), 1,
      aux_sym_sequence_stmt_par_token1,
    ACTIONS(105), 1,
      aux_sym_sequence_stmt_par_token2,
    ACTIONS(107), 1,
      sym__newline,
    ACTIONS(109), 1,
      aux_sym_sequence_stmt_loop_token2,
    STATE(17), 1,
      aux_sym__sequence_subdocument,
    STATE(115), 1,
      sym__sequence_stmt,
    STATE(257), 1,
      aux_sym_sequence_actor_repeat1,
    STATE(301), 1,
      sym_sequence_actor,
    STATE(586), 1,
      sym__sequence_participant_type,
    STATE(634), 1,
      aux_sym_sequence_stmt_par_repeat1,
    ACTIONS(71), 2,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
    STATE(109), 17,
      sym_sequence_stmt_participant,
      sym_sequence_stmt_actor,
      sym_sequence_stmt_signal,
      sym_sequence_stmt_autonumber,
      sym_sequence_stmt_activate,
      sym_sequence_stmt_deactivate,
      sym_sequence_stmt_note,
      sym_sequence_stmt_links,
      sym_sequence_stmt_link,
      sym_sequence_stmt_properties,
      sym_sequence_stmt_details,
      sym_sequence_stmt_title,
      sym_sequence_stmt_loop,
      sym_sequence_stmt_rect,
      sym_sequence_stmt_opt,
      sym_sequence_stmt_alt,
      sym_sequence_stmt_par,
  [315] = 29,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(33), 1,
      aux_sym_sequence_stmt_actor_token1,
    ACTIONS(35), 1,
      aux_sym_sequence_stmt_autonumber_token1,
    ACTIONS(37), 1,
      aux_sym_sequence_stmt_activate_token1,
    ACTIONS(39), 1,
      aux_sym_sequence_stmt_deactivate_token1,
    ACTIONS(41), 1,
      aux_sym_sequence_stmt_note_token1,
    ACTIONS(43), 1,
      aux_sym_sequence_stmt_links_token1,
    ACTIONS(45), 1,
      aux_sym_sequence_stmt_link_token1,
    ACTIONS(47), 1,
      aux_sym_sequence_stmt_properties_token1,
    ACTIONS(49), 1,
      aux_sym_sequence_stmt_details_token1,
    ACTIONS(51), 1,
      aux_sym_sequence_stmt_title_token1,
    ACTIONS(53), 1,
      aux_sym_sequence_stmt_loop_token1,
    ACTIONS(57), 1,
      aux_sym_sequence_stmt_rect_token1,
    ACTIONS(59), 1,
      aux_sym_sequence_stmt_opt_token1,
    ACTIONS(61), 1,
      aux_sym_sequence_stmt_alt_token1,
    ACTIONS(63), 1,
      aux_sym_sequence_stmt_alt_token2,
    ACTIONS(65), 1,
      aux_sym_sequence_stmt_par_token1,
    ACTIONS(67), 1,
      sym__newline,
    ACTIONS(69), 1,
      sym__sequence_actor_word,
    ACTIONS(111), 1,
      aux_sym_sequence_stmt_loop_token2,
    STATE(18), 1,
      aux_sym__sequence_subdocument,
    STATE(129), 1,
      sym__sequence_stmt,
    STATE(257), 1,
      aux_sym_sequence_actor_repeat1,
    STATE(313), 1,
      sym_sequence_actor,
    STATE(551), 1,
      sym__sequence_participant_type,
    STATE(633), 1,
      aux_sym_sequence_stmt_alt_repeat1,
    ACTIONS(31), 2,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
    STATE(96), 17,
      sym_sequence_stmt_participant,
      sym_sequence_stmt_actor,
      sym_sequence_stmt_signal,
      sym_sequence_stmt_autonumber,
      sym_sequence_stmt_activate,
      sym_sequence_stmt_deactivate,
      sym_sequence_stmt_note,
      sym_sequence_stmt_links,
      sym_sequence_stmt_link,
      sym_sequence_stmt_properties,
      sym_sequence_stmt_details,
      sym_sequence_stmt_title,
      sym_sequence_stmt_loop,
      sym_sequence_stmt_rect,
      sym_sequence_stmt_opt,
      sym_sequence_stmt_alt,
      sym_sequence_stmt_par,
  [420] = 29,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(69), 1,
      sym__sequence_actor_word,
    ACTIONS(73), 1,
      aux_sym_sequence_stmt_actor_token1,
    ACTIONS(75), 1,
      aux_sym_sequence_stmt_autonumber_token1,
    ACTIONS(77), 1,
      aux_sym_sequence_stmt_activate_token1,
    ACTIONS(79), 1,
      aux_sym_sequence_stmt_deactivate_token1,
    ACTIONS(81), 1,
      aux_sym_sequence_stmt_note_token1,
    ACTIONS(83), 1,
      aux_sym_sequence_stmt_links_token1,
    ACTIONS(85), 1,
      aux_sym_sequence_stmt_link_token1,
    ACTIONS(87), 1,
      aux_sym_sequence_stmt_properties_token1,
    ACTIONS(89), 1,
      aux_sym_sequence_stmt_details_token1,
    ACTIONS(91), 1,
      aux_sym_sequence_stmt_title_token1,
    ACTIONS(93), 1,
      aux_sym_sequence_stmt_loop_token1,
    ACTIONS(97), 1,
      aux_sym_sequence_stmt_rect_token1,
    ACTIONS(99), 1,
      aux_sym_sequence_stmt_opt_token1,
    ACTIONS(101), 1,
      aux_sym_sequence_stmt_alt_token1,
    ACTIONS(103), 1,
      aux_sym_sequence_stmt_par_token1,
    ACTIONS(105), 1,
      aux_sym_sequence_stmt_par_token2,
    ACTIONS(107), 1,
      sym__newline,
    ACTIONS(113), 1,
      aux_sym_sequence_stmt_loop_token2,
    STATE(17), 1,
      aux_sym__sequence_subdocument,
    STATE(115), 1,
      sym__sequence_stmt,
    STATE(257), 1,
      aux_sym_sequence_actor_repeat1,
    STATE(301), 1,
      sym_sequence_actor,
    STATE(578), 1,
      aux_sym_sequence_stmt_par_repeat1,
    STATE(586), 1,
      sym__sequence_participant_type,
    ACTIONS(71), 2,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
    STATE(109), 17,
      sym_sequence_stmt_participant,
      sym_sequence_stmt_actor,
      sym_sequence_stmt_signal,
      sym_sequence_stmt_autonumber,
      sym_sequence_stmt_activate,
      sym_sequence_stmt_deactivate,
      sym_sequence_stmt_note,
      sym_sequence_stmt_links,
      sym_sequence_stmt_link,
      sym_sequence_stmt_properties,
      sym_sequence_stmt_details,
      sym_sequence_stmt_title,
      sym_sequence_stmt_loop,
      sym_sequence_stmt_rect,
      sym_sequence_stmt_opt,
      sym_sequence_stmt_alt,
      sym_sequence_stmt_par,
  [525] = 29,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(69), 1,
      sym__sequence_actor_word,
    ACTIONS(73), 1,
      aux_sym_sequence_stmt_actor_token1,
    ACTIONS(75), 1,
      aux_sym_sequence_stmt_autonumber_token1,
    ACTIONS(77), 1,
      aux_sym_sequence_stmt_activate_token1,
    ACTIONS(79), 1,
      aux_sym_sequence_stmt_deactivate_token1,
    ACTIONS(81), 1,
      aux_sym_sequence_stmt_note_token1,
    ACTIONS(83), 1,
      aux_sym_sequence_stmt_links_token1,
    ACTIONS(85), 1,
      aux_sym_sequence_stmt_link_token1,
    ACTIONS(87), 1,
      aux_sym_sequence_stmt_properties_token1,
    ACTIONS(89), 1,
      aux_sym_sequence_stmt_details_token1,
    ACTIONS(91), 1,
      aux_sym_sequence_stmt_title_token1,
    ACTIONS(93), 1,
      aux_sym_sequence_stmt_loop_token1,
    ACTIONS(97), 1,
      aux_sym_sequence_stmt_rect_token1,
    ACTIONS(99), 1,
      aux_sym_sequence_stmt_opt_token1,
    ACTIONS(101), 1,
      aux_sym_sequence_stmt_alt_token1,
    ACTIONS(103), 1,
      aux_sym_sequence_stmt_par_token1,
    ACTIONS(105), 1,
      aux_sym_sequence_stmt_par_token2,
    ACTIONS(107), 1,
      sym__newline,
    ACTIONS(115), 1,
      aux_sym_sequence_stmt_loop_token2,
    STATE(17), 1,
      aux_sym__sequence_subdocument,
    STATE(115), 1,
      sym__sequence_stmt,
    STATE(257), 1,
      aux_sym_sequence_actor_repeat1,
    STATE(301), 1,
      sym_sequence_actor,
    STATE(586), 1,
      sym__sequence_participant_type,
    STATE(632), 1,
      aux_sym_sequence_stmt_par_repeat1,
    ACTIONS(71), 2,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
    STATE(109), 17,
      sym_sequence_stmt_participant,
      sym_sequence_stmt_actor,
      sym_sequence_stmt_signal,
      sym_sequence_stmt_autonumber,
      sym_sequence_stmt_activate,
      sym_sequence_stmt_deactivate,
      sym_sequence_stmt_note,
      sym_sequence_stmt_links,
      sym_sequence_stmt_link,
      sym_sequence_stmt_properties,
      sym_sequence_stmt_details,
      sym_sequence_stmt_title,
      sym_sequence_stmt_loop,
      sym_sequence_stmt_rect,
      sym_sequence_stmt_opt,
      sym_sequence_stmt_alt,
      sym_sequence_stmt_par,
  [630] = 29,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(33), 1,
      aux_sym_sequence_stmt_actor_token1,
    ACTIONS(35), 1,
      aux_sym_sequence_stmt_autonumber_token1,
    ACTIONS(37), 1,
      aux_sym_sequence_stmt_activate_token1,
    ACTIONS(39), 1,
      aux_sym_sequence_stmt_deactivate_token1,
    ACTIONS(41), 1,
      aux_sym_sequence_stmt_note_token1,
    ACTIONS(43), 1,
      aux_sym_sequence_stmt_links_token1,
    ACTIONS(45), 1,
      aux_sym_sequence_stmt_link_token1,
    ACTIONS(47), 1,
      aux_sym_sequence_stmt_properties_token1,
    ACTIONS(49), 1,
      aux_sym_sequence_stmt_details_token1,
    ACTIONS(51), 1,
      aux_sym_sequence_stmt_title_token1,
    ACTIONS(53), 1,
      aux_sym_sequence_stmt_loop_token1,
    ACTIONS(57), 1,
      aux_sym_sequence_stmt_rect_token1,
    ACTIONS(59), 1,
      aux_sym_sequence_stmt_opt_token1,
    ACTIONS(61), 1,
      aux_sym_sequence_stmt_alt_token1,
    ACTIONS(63), 1,
      aux_sym_sequence_stmt_alt_token2,
    ACTIONS(65), 1,
      aux_sym_sequence_stmt_par_token1,
    ACTIONS(67), 1,
      sym__newline,
    ACTIONS(69), 1,
      sym__sequence_actor_word,
    ACTIONS(117), 1,
      aux_sym_sequence_stmt_loop_token2,
    STATE(18), 1,
      aux_sym__sequence_subdocument,
    STATE(129), 1,
      sym__sequence_stmt,
    STATE(257), 1,
      aux_sym_sequence_actor_repeat1,
    STATE(313), 1,
      sym_sequence_actor,
    STATE(551), 1,
      sym__sequence_participant_type,
    STATE(589), 1,
      aux_sym_sequence_stmt_alt_repeat1,
    ACTIONS(31), 2,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
    STATE(96), 17,
      sym_sequence_stmt_participant,
      sym_sequence_stmt_actor,
      sym_sequence_stmt_signal,
      sym_sequence_stmt_autonumber,
      sym_sequence_stmt_activate,
      sym_sequence_stmt_deactivate,
      sym_sequence_stmt_note,
      sym_sequence_stmt_links,
      sym_sequence_stmt_link,
      sym_sequence_stmt_properties,
      sym_sequence_stmt_details,
      sym_sequence_stmt_title,
      sym_sequence_stmt_loop,
      sym_sequence_stmt_rect,
      sym_sequence_stmt_opt,
      sym_sequence_stmt_alt,
      sym_sequence_stmt_par,
  [735] = 29,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(69), 1,
      sym__sequence_actor_word,
    ACTIONS(73), 1,
      aux_sym_sequence_stmt_actor_token1,
    ACTIONS(75), 1,
      aux_sym_sequence_stmt_autonumber_token1,
    ACTIONS(77), 1,
      aux_sym_sequence_stmt_activate_token1,
    ACTIONS(79), 1,
      aux_sym_sequence_stmt_deactivate_token1,
    ACTIONS(81), 1,
      aux_sym_sequence_stmt_note_token1,
    ACTIONS(83), 1,
      aux_sym_sequence_stmt_links_token1,
    ACTIONS(85), 1,
      aux_sym_sequence_stmt_link_token1,
    ACTIONS(87), 1,
      aux_sym_sequence_stmt_properties_token1,
    ACTIONS(89), 1,
      aux_sym_sequence_stmt_details_token1,
    ACTIONS(91), 1,
      aux_sym_sequence_stmt_title_token1,
    ACTIONS(93), 1,
      aux_sym_sequence_stmt_loop_token1,
    ACTIONS(97), 1,
      aux_sym_sequence_stmt_rect_token1,
    ACTIONS(99), 1,
      aux_sym_sequence_stmt_opt_token1,
    ACTIONS(101), 1,
      aux_sym_sequence_stmt_alt_token1,
    ACTIONS(103), 1,
      aux_sym_sequence_stmt_par_token1,
    ACTIONS(105), 1,
      aux_sym_sequence_stmt_par_token2,
    ACTIONS(107), 1,
      sym__newline,
    ACTIONS(119), 1,
      aux_sym_sequence_stmt_loop_token2,
    STATE(17), 1,
      aux_sym__sequence_subdocument,
    STATE(115), 1,
      sym__sequence_stmt,
    STATE(257), 1,
      aux_sym_sequence_actor_repeat1,
    STATE(301), 1,
      sym_sequence_actor,
    STATE(586), 1,
      sym__sequence_participant_type,
    STATE(619), 1,
      aux_sym_sequence_stmt_par_repeat1,
    ACTIONS(71), 2,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
    STATE(109), 17,
      sym_sequence_stmt_participant,
      sym_sequence_stmt_actor,
      sym_sequence_stmt_signal,
      sym_sequence_stmt_autonumber,
      sym_sequence_stmt_activate,
      sym_sequence_stmt_deactivate,
      sym_sequence_stmt_note,
      sym_sequence_stmt_links,
      sym_sequence_stmt_link,
      sym_sequence_stmt_properties,
      sym_sequence_stmt_details,
      sym_sequence_stmt_title,
      sym_sequence_stmt_loop,
      sym_sequence_stmt_rect,
      sym_sequence_stmt_opt,
      sym_sequence_stmt_alt,
      sym_sequence_stmt_par,
  [840] = 29,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(69), 1,
      sym__sequence_actor_word,
    ACTIONS(73), 1,
      aux_sym_sequence_stmt_actor_token1,
    ACTIONS(75), 1,
      aux_sym_sequence_stmt_autonumber_token1,
    ACTIONS(77), 1,
      aux_sym_sequence_stmt_activate_token1,
    ACTIONS(79), 1,
      aux_sym_sequence_stmt_deactivate_token1,
    ACTIONS(81), 1,
      aux_sym_sequence_stmt_note_token1,
    ACTIONS(83), 1,
      aux_sym_sequence_stmt_links_token1,
    ACTIONS(85), 1,
      aux_sym_sequence_stmt_link_token1,
    ACTIONS(87), 1,
      aux_sym_sequence_stmt_properties_token1,
    ACTIONS(89), 1,
      aux_sym_sequence_stmt_details_token1,
    ACTIONS(91), 1,
      aux_sym_sequence_stmt_title_token1,
    ACTIONS(93), 1,
      aux_sym_sequence_stmt_loop_token1,
    ACTIONS(97), 1,
      aux_sym_sequence_stmt_rect_token1,
    ACTIONS(99), 1,
      aux_sym_sequence_stmt_opt_token1,
    ACTIONS(101), 1,
      aux_sym_sequence_stmt_alt_token1,
    ACTIONS(103), 1,
      aux_sym_sequence_stmt_par_token1,
    ACTIONS(105), 1,
      aux_sym_sequence_stmt_par_token2,
    ACTIONS(107), 1,
      sym__newline,
    ACTIONS(121), 1,
      aux_sym_sequence_stmt_loop_token2,
    STATE(17), 1,
      aux_sym__sequence_subdocument,
    STATE(115), 1,
      sym__sequence_stmt,
    STATE(257), 1,
      aux_sym_sequence_actor_repeat1,
    STATE(301), 1,
      sym_sequence_actor,
    STATE(586), 1,
      sym__sequence_participant_type,
    STATE(595), 1,
      aux_sym_sequence_stmt_par_repeat1,
    ACTIONS(71), 2,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
    STATE(109), 17,
      sym_sequence_stmt_participant,
      sym_sequence_stmt_actor,
      sym_sequence_stmt_signal,
      sym_sequence_stmt_autonumber,
      sym_sequence_stmt_activate,
      sym_sequence_stmt_deactivate,
      sym_sequence_stmt_note,
      sym_sequence_stmt_links,
      sym_sequence_stmt_link,
      sym_sequence_stmt_properties,
      sym_sequence_stmt_details,
      sym_sequence_stmt_title,
      sym_sequence_stmt_loop,
      sym_sequence_stmt_rect,
      sym_sequence_stmt_opt,
      sym_sequence_stmt_alt,
      sym_sequence_stmt_par,
  [945] = 29,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(69), 1,
      sym__sequence_actor_word,
    ACTIONS(73), 1,
      aux_sym_sequence_stmt_actor_token1,
    ACTIONS(75), 1,
      aux_sym_sequence_stmt_autonumber_token1,
    ACTIONS(77), 1,
      aux_sym_sequence_stmt_activate_token1,
    ACTIONS(79), 1,
      aux_sym_sequence_stmt_deactivate_token1,
    ACTIONS(81), 1,
      aux_sym_sequence_stmt_note_token1,
    ACTIONS(83), 1,
      aux_sym_sequence_stmt_links_token1,
    ACTIONS(85), 1,
      aux_sym_sequence_stmt_link_token1,
    ACTIONS(87), 1,
      aux_sym_sequence_stmt_properties_token1,
    ACTIONS(89), 1,
      aux_sym_sequence_stmt_details_token1,
    ACTIONS(91), 1,
      aux_sym_sequence_stmt_title_token1,
    ACTIONS(93), 1,
      aux_sym_sequence_stmt_loop_token1,
    ACTIONS(97), 1,
      aux_sym_sequence_stmt_rect_token1,
    ACTIONS(99), 1,
      aux_sym_sequence_stmt_opt_token1,
    ACTIONS(101), 1,
      aux_sym_sequence_stmt_alt_token1,
    ACTIONS(103), 1,
      aux_sym_sequence_stmt_par_token1,
    ACTIONS(105), 1,
      aux_sym_sequence_stmt_par_token2,
    ACTIONS(107), 1,
      sym__newline,
    ACTIONS(123), 1,
      aux_sym_sequence_stmt_loop_token2,
    STATE(17), 1,
      aux_sym__sequence_subdocument,
    STATE(115), 1,
      sym__sequence_stmt,
    STATE(257), 1,
      aux_sym_sequence_actor_repeat1,
    STATE(301), 1,
      sym_sequence_actor,
    STATE(586), 1,
      sym__sequence_participant_type,
    STATE(598), 1,
      aux_sym_sequence_stmt_par_repeat1,
    ACTIONS(71), 2,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
    STATE(109), 17,
      sym_sequence_stmt_participant,
      sym_sequence_stmt_actor,
      sym_sequence_stmt_signal,
      sym_sequence_stmt_autonumber,
      sym_sequence_stmt_activate,
      sym_sequence_stmt_deactivate,
      sym_sequence_stmt_note,
      sym_sequence_stmt_links,
      sym_sequence_stmt_link,
      sym_sequence_stmt_properties,
      sym_sequence_stmt_details,
      sym_sequence_stmt_title,
      sym_sequence_stmt_loop,
      sym_sequence_stmt_rect,
      sym_sequence_stmt_opt,
      sym_sequence_stmt_alt,
      sym_sequence_stmt_par,
  [1050] = 29,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(33), 1,
      aux_sym_sequence_stmt_actor_token1,
    ACTIONS(35), 1,
      aux_sym_sequence_stmt_autonumber_token1,
    ACTIONS(37), 1,
      aux_sym_sequence_stmt_activate_token1,
    ACTIONS(39), 1,
      aux_sym_sequence_stmt_deactivate_token1,
    ACTIONS(41), 1,
      aux_sym_sequence_stmt_note_token1,
    ACTIONS(43), 1,
      aux_sym_sequence_stmt_links_token1,
    ACTIONS(45), 1,
      aux_sym_sequence_stmt_link_token1,
    ACTIONS(47), 1,
      aux_sym_sequence_stmt_properties_token1,
    ACTIONS(49), 1,
      aux_sym_sequence_stmt_details_token1,
    ACTIONS(51), 1,
      aux_sym_sequence_stmt_title_token1,
    ACTIONS(53), 1,
      aux_sym_sequence_stmt_loop_token1,
    ACTIONS(57), 1,
      aux_sym_sequence_stmt_rect_token1,
    ACTIONS(59), 1,
      aux_sym_sequence_stmt_opt_token1,
    ACTIONS(61), 1,
      aux_sym_sequence_stmt_alt_token1,
    ACTIONS(63), 1,
      aux_sym_sequence_stmt_alt_token2,
    ACTIONS(65), 1,
      aux_sym_sequence_stmt_par_token1,
    ACTIONS(67), 1,
      sym__newline,
    ACTIONS(69), 1,
      sym__sequence_actor_word,
    ACTIONS(125), 1,
      aux_sym_sequence_stmt_loop_token2,
    STATE(18), 1,
      aux_sym__sequence_subdocument,
    STATE(129), 1,
      sym__sequence_stmt,
    STATE(257), 1,
      aux_sym_sequence_actor_repeat1,
    STATE(313), 1,
      sym_sequence_actor,
    STATE(551), 1,
      sym__sequence_participant_type,
    STATE(596), 1,
      aux_sym_sequence_stmt_alt_repeat1,
    ACTIONS(31), 2,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
    STATE(96), 17,
      sym_sequence_stmt_participant,
      sym_sequence_stmt_actor,
      sym_sequence_stmt_signal,
      sym_sequence_stmt_autonumber,
      sym_sequence_stmt_activate,
      sym_sequence_stmt_deactivate,
      sym_sequence_stmt_note,
      sym_sequence_stmt_links,
      sym_sequence_stmt_link,
      sym_sequence_stmt_properties,
      sym_sequence_stmt_details,
      sym_sequence_stmt_title,
      sym_sequence_stmt_loop,
      sym_sequence_stmt_rect,
      sym_sequence_stmt_opt,
      sym_sequence_stmt_alt,
      sym_sequence_stmt_par,
  [1155] = 29,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(69), 1,
      sym__sequence_actor_word,
    ACTIONS(73), 1,
      aux_sym_sequence_stmt_actor_token1,
    ACTIONS(75), 1,
      aux_sym_sequence_stmt_autonumber_token1,
    ACTIONS(77), 1,
      aux_sym_sequence_stmt_activate_token1,
    ACTIONS(79), 1,
      aux_sym_sequence_stmt_deactivate_token1,
    ACTIONS(81), 1,
      aux_sym_sequence_stmt_note_token1,
    ACTIONS(83), 1,
      aux_sym_sequence_stmt_links_token1,
    ACTIONS(85), 1,
      aux_sym_sequence_stmt_link_token1,
    ACTIONS(87), 1,
      aux_sym_sequence_stmt_properties_token1,
    ACTIONS(89), 1,
      aux_sym_sequence_stmt_details_token1,
    ACTIONS(91), 1,
      aux_sym_sequence_stmt_title_token1,
    ACTIONS(93), 1,
      aux_sym_sequence_stmt_loop_token1,
    ACTIONS(97), 1,
      aux_sym_sequence_stmt_rect_token1,
    ACTIONS(99), 1,
      aux_sym_sequence_stmt_opt_token1,
    ACTIONS(101), 1,
      aux_sym_sequence_stmt_alt_token1,
    ACTIONS(103), 1,
      aux_sym_sequence_stmt_par_token1,
    ACTIONS(105), 1,
      aux_sym_sequence_stmt_par_token2,
    ACTIONS(107), 1,
      sym__newline,
    ACTIONS(127), 1,
      aux_sym_sequence_stmt_loop_token2,
    STATE(17), 1,
      aux_sym__sequence_subdocument,
    STATE(115), 1,
      sym__sequence_stmt,
    STATE(257), 1,
      aux_sym_sequence_actor_repeat1,
    STATE(301), 1,
      sym_sequence_actor,
    STATE(586), 1,
      sym__sequence_participant_type,
    STATE(614), 1,
      aux_sym_sequence_stmt_par_repeat1,
    ACTIONS(71), 2,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
    STATE(109), 17,
      sym_sequence_stmt_participant,
      sym_sequence_stmt_actor,
      sym_sequence_stmt_signal,
      sym_sequence_stmt_autonumber,
      sym_sequence_stmt_activate,
      sym_sequence_stmt_deactivate,
      sym_sequence_stmt_note,
      sym_sequence_stmt_links,
      sym_sequence_stmt_link,
      sym_sequence_stmt_properties,
      sym_sequence_stmt_details,
      sym_sequence_stmt_title,
      sym_sequence_stmt_loop,
      sym_sequence_stmt_rect,
      sym_sequence_stmt_opt,
      sym_sequence_stmt_alt,
      sym_sequence_stmt_par,
  [1260] = 27,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(69), 1,
      sym__sequence_actor_word,
    ACTIONS(73), 1,
      aux_sym_sequence_stmt_actor_token1,
    ACTIONS(75), 1,
      aux_sym_sequence_stmt_autonumber_token1,
    ACTIONS(77), 1,
      aux_sym_sequence_stmt_activate_token1,
    ACTIONS(79), 1,
      aux_sym_sequence_stmt_deactivate_token1,
    ACTIONS(81), 1,
      aux_sym_sequence_stmt_note_token1,
    ACTIONS(83), 1,
      aux_sym_sequence_stmt_links_token1,
    ACTIONS(85), 1,
      aux_sym_sequence_stmt_link_token1,
    ACTIONS(87), 1,
      aux_sym_sequence_stmt_properties_token1,
    ACTIONS(89), 1,
      aux_sym_sequence_stmt_details_token1,
    ACTIONS(91), 1,
      aux_sym_sequence_stmt_title_token1,
    ACTIONS(93), 1,
      aux_sym_sequence_stmt_loop_token1,
    ACTIONS(97), 1,
      aux_sym_sequence_stmt_rect_token1,
    ACTIONS(99), 1,
      aux_sym_sequence_stmt_opt_token1,
    ACTIONS(101), 1,
      aux_sym_sequence_stmt_alt_token1,
    ACTIONS(103), 1,
      aux_sym_sequence_stmt_par_token1,
    ACTIONS(107), 1,
      sym__newline,
    STATE(17), 1,
      aux_sym__sequence_subdocument,
    STATE(115), 1,
      sym__sequence_stmt,
    STATE(257), 1,
      aux_sym_sequence_actor_repeat1,
    STATE(301), 1,
      sym_sequence_actor,
    STATE(586), 1,
      sym__sequence_participant_type,
    ACTIONS(71), 2,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
    ACTIONS(129), 2,
      aux_sym_sequence_stmt_loop_token2,
      aux_sym_sequence_stmt_par_token2,
    STATE(109), 17,
      sym_sequence_stmt_participant,
      sym_sequence_stmt_actor,
      sym_sequence_stmt_signal,
      sym_sequence_stmt_autonumber,
      sym_sequence_stmt_activate,
      sym_sequence_stmt_deactivate,
      sym_sequence_stmt_note,
      sym_sequence_stmt_links,
      sym_sequence_stmt_link,
      sym_sequence_stmt_properties,
      sym_sequence_stmt_details,
      sym_sequence_stmt_title,
      sym_sequence_stmt_loop,
      sym_sequence_stmt_rect,
      sym_sequence_stmt_opt,
      sym_sequence_stmt_alt,
      sym_sequence_stmt_par,
  [1360] = 27,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(69), 1,
      sym__sequence_actor_word,
    ACTIONS(73), 1,
      aux_sym_sequence_stmt_actor_token1,
    ACTIONS(75), 1,
      aux_sym_sequence_stmt_autonumber_token1,
    ACTIONS(77), 1,
      aux_sym_sequence_stmt_activate_token1,
    ACTIONS(79), 1,
      aux_sym_sequence_stmt_deactivate_token1,
    ACTIONS(81), 1,
      aux_sym_sequence_stmt_note_token1,
    ACTIONS(83), 1,
      aux_sym_sequence_stmt_links_token1,
    ACTIONS(85), 1,
      aux_sym_sequence_stmt_link_token1,
    ACTIONS(87), 1,
      aux_sym_sequence_stmt_properties_token1,
    ACTIONS(89), 1,
      aux_sym_sequence_stmt_details_token1,
    ACTIONS(91), 1,
      aux_sym_sequence_stmt_title_token1,
    ACTIONS(93), 1,
      aux_sym_sequence_stmt_loop_token1,
    ACTIONS(97), 1,
      aux_sym_sequence_stmt_rect_token1,
    ACTIONS(99), 1,
      aux_sym_sequence_stmt_opt_token1,
    ACTIONS(101), 1,
      aux_sym_sequence_stmt_alt_token1,
    ACTIONS(103), 1,
      aux_sym_sequence_stmt_par_token1,
    ACTIONS(107), 1,
      sym__newline,
    STATE(17), 1,
      aux_sym__sequence_subdocument,
    STATE(115), 1,
      sym__sequence_stmt,
    STATE(257), 1,
      aux_sym_sequence_actor_repeat1,
    STATE(301), 1,
      sym_sequence_actor,
    STATE(586), 1,
      sym__sequence_participant_type,
    ACTIONS(71), 2,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
    ACTIONS(131), 2,
      aux_sym_sequence_stmt_loop_token2,
      aux_sym_sequence_stmt_par_token2,
    STATE(109), 17,
      sym_sequence_stmt_participant,
      sym_sequence_stmt_actor,
      sym_sequence_stmt_signal,
      sym_sequence_stmt_autonumber,
      sym_sequence_stmt_activate,
      sym_sequence_stmt_deactivate,
      sym_sequence_stmt_note,
      sym_sequence_stmt_links,
      sym_sequence_stmt_link,
      sym_sequence_stmt_properties,
      sym_sequence_stmt_details,
      sym_sequence_stmt_title,
      sym_sequence_stmt_loop,
      sym_sequence_stmt_rect,
      sym_sequence_stmt_opt,
      sym_sequence_stmt_alt,
      sym_sequence_stmt_par,
  [1460] = 27,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(33), 1,
      aux_sym_sequence_stmt_actor_token1,
    ACTIONS(35), 1,
      aux_sym_sequence_stmt_autonumber_token1,
    ACTIONS(37), 1,
      aux_sym_sequence_stmt_activate_token1,
    ACTIONS(39), 1,
      aux_sym_sequence_stmt_deactivate_token1,
    ACTIONS(41), 1,
      aux_sym_sequence_stmt_note_token1,
    ACTIONS(43), 1,
      aux_sym_sequence_stmt_links_token1,
    ACTIONS(45), 1,
      aux_sym_sequence_stmt_link_token1,
    ACTIONS(47), 1,
      aux_sym_sequence_stmt_properties_token1,
    ACTIONS(49), 1,
      aux_sym_sequence_stmt_details_token1,
    ACTIONS(51), 1,
      aux_sym_sequence_stmt_title_token1,
    ACTIONS(53), 1,
      aux_sym_sequence_stmt_loop_token1,
    ACTIONS(57), 1,
      aux_sym_sequence_stmt_rect_token1,
    ACTIONS(59), 1,
      aux_sym_sequence_stmt_opt_token1,
    ACTIONS(61), 1,
      aux_sym_sequence_stmt_alt_token1,
    ACTIONS(65), 1,
      aux_sym_sequence_stmt_par_token1,
    ACTIONS(67), 1,
      sym__newline,
    ACTIONS(69), 1,
      sym__sequence_actor_word,
    STATE(18), 1,
      aux_sym__sequence_subdocument,
    STATE(129), 1,
      sym__sequence_stmt,
    STATE(257), 1,
      aux_sym_sequence_actor_repeat1,
    STATE(313), 1,
      sym_sequence_actor,
    STATE(551), 1,
      sym__sequence_participant_type,
    ACTIONS(31), 2,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
    ACTIONS(133), 2,
      aux_sym_sequence_stmt_loop_token2,
      aux_sym_sequence_stmt_alt_token2,
    STATE(96), 17,
      sym_sequence_stmt_participant,
      sym_sequence_stmt_actor,
      sym_sequence_stmt_signal,
      sym_sequence_stmt_autonumber,
      sym_sequence_stmt_activate,
      sym_sequence_stmt_deactivate,
      sym_sequence_stmt_note,
      sym_sequence_stmt_links,
      sym_sequence_stmt_link,
      sym_sequence_stmt_properties,
      sym_sequence_stmt_details,
      sym_sequence_stmt_title,
      sym_sequence_stmt_loop,
      sym_sequence_stmt_rect,
      sym_sequence_stmt_opt,
      sym_sequence_stmt_alt,
      sym_sequence_stmt_par,
  [1560] = 27,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(138), 1,
      aux_sym_sequence_stmt_actor_token1,
    ACTIONS(141), 1,
      aux_sym_sequence_stmt_autonumber_token1,
    ACTIONS(144), 1,
      aux_sym_sequence_stmt_activate_token1,
    ACTIONS(147), 1,
      aux_sym_sequence_stmt_deactivate_token1,
    ACTIONS(150), 1,
      aux_sym_sequence_stmt_note_token1,
    ACTIONS(153), 1,
      aux_sym_sequence_stmt_links_token1,
    ACTIONS(156), 1,
      aux_sym_sequence_stmt_link_token1,
    ACTIONS(159), 1,
      aux_sym_sequence_stmt_properties_token1,
    ACTIONS(162), 1,
      aux_sym_sequence_stmt_details_token1,
    ACTIONS(165), 1,
      aux_sym_sequence_stmt_title_token1,
    ACTIONS(168), 1,
      aux_sym_sequence_stmt_loop_token1,
    ACTIONS(173), 1,
      aux_sym_sequence_stmt_rect_token1,
    ACTIONS(176), 1,
      aux_sym_sequence_stmt_opt_token1,
    ACTIONS(179), 1,
      aux_sym_sequence_stmt_alt_token1,
    ACTIONS(182), 1,
      aux_sym_sequence_stmt_par_token1,
    ACTIONS(185), 1,
      sym__newline,
    ACTIONS(188), 1,
      sym__sequence_actor_word,
    STATE(17), 1,
      aux_sym__sequence_subdocument,
    STATE(115), 1,
      sym__sequence_stmt,
    STATE(257), 1,
      aux_sym_sequence_actor_repeat1,
    STATE(301), 1,
      sym_sequence_actor,
    STATE(586), 1,
      sym__sequence_participant_type,
    ACTIONS(135), 2,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
    ACTIONS(171), 2,
      aux_sym_sequence_stmt_loop_token2,
      aux_sym_sequence_stmt_par_token2,
    STATE(109), 17,
      sym_sequence_stmt_participant,
      sym_sequence_stmt_actor,
      sym_sequence_stmt_signal,
      sym_sequence_stmt_autonumber,
      sym_sequence_stmt_activate,
      sym_sequence_stmt_deactivate,
      sym_sequence_stmt_note,
      sym_sequence_stmt_links,
      sym_sequence_stmt_link,
      sym_sequence_stmt_properties,
      sym_sequence_stmt_details,
      sym_sequence_stmt_title,
      sym_sequence_stmt_loop,
      sym_sequence_stmt_rect,
      sym_sequence_stmt_opt,
      sym_sequence_stmt_alt,
      sym_sequence_stmt_par,
  [1660] = 27,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(188), 1,
      sym__sequence_actor_word,
    ACTIONS(194), 1,
      aux_sym_sequence_stmt_actor_token1,
    ACTIONS(197), 1,
      aux_sym_sequence_stmt_autonumber_token1,
    ACTIONS(200), 1,
      aux_sym_sequence_stmt_activate_token1,
    ACTIONS(203), 1,
      aux_sym_sequence_stmt_deactivate_token1,
    ACTIONS(206), 1,
      aux_sym_sequence_stmt_note_token1,
    ACTIONS(209), 1,
      aux_sym_sequence_stmt_links_token1,
    ACTIONS(212), 1,
      aux_sym_sequence_stmt_link_token1,
    ACTIONS(215), 1,
      aux_sym_sequence_stmt_properties_token1,
    ACTIONS(218), 1,
      aux_sym_sequence_stmt_details_token1,
    ACTIONS(221), 1,
      aux_sym_sequence_stmt_title_token1,
    ACTIONS(224), 1,
      aux_sym_sequence_stmt_loop_token1,
    ACTIONS(227), 1,
      aux_sym_sequence_stmt_rect_token1,
    ACTIONS(230), 1,
      aux_sym_sequence_stmt_opt_token1,
    ACTIONS(233), 1,
      aux_sym_sequence_stmt_alt_token1,
    ACTIONS(236), 1,
      aux_sym_sequence_stmt_par_token1,
    ACTIONS(239), 1,
      sym__newline,
    STATE(18), 1,
      aux_sym__sequence_subdocument,
    STATE(129), 1,
      sym__sequence_stmt,
    STATE(257), 1,
      aux_sym_sequence_actor_repeat1,
    STATE(313), 1,
      sym_sequence_actor,
    STATE(551), 1,
      sym__sequence_participant_type,
    ACTIONS(171), 2,
      aux_sym_sequence_stmt_loop_token2,
      aux_sym_sequence_stmt_alt_token2,
    ACTIONS(191), 2,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
    STATE(96), 17,
      sym_sequence_stmt_participant,
      sym_sequence_stmt_actor,
      sym_sequence_stmt_signal,
      sym_sequence_stmt_autonumber,
      sym_sequence_stmt_activate,
      sym_sequence_stmt_deactivate,
      sym_sequence_stmt_note,
      sym_sequence_stmt_links,
      sym_sequence_stmt_link,
      sym_sequence_stmt_properties,
      sym_sequence_stmt_details,
      sym_sequence_stmt_title,
      sym_sequence_stmt_loop,
      sym_sequence_stmt_rect,
      sym_sequence_stmt_opt,
      sym_sequence_stmt_alt,
      sym_sequence_stmt_par,
  [1760] = 26,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(69), 1,
      sym__sequence_actor_word,
    ACTIONS(242), 1,
      ts_builtin_sym_end,
    ACTIONS(246), 1,
      aux_sym_sequence_stmt_actor_token1,
    ACTIONS(248), 1,
      aux_sym_sequence_stmt_autonumber_token1,
    ACTIONS(250), 1,
      aux_sym_sequence_stmt_activate_token1,
    ACTIONS(252), 1,
      aux_sym_sequence_stmt_deactivate_token1,
    ACTIONS(254), 1,
      aux_sym_sequence_stmt_note_token1,
    ACTIONS(256), 1,
      aux_sym_sequence_stmt_links_token1,
    ACTIONS(258), 1,
      aux_sym_sequence_stmt_link_token1,
    ACTIONS(260), 1,
      aux_sym_sequence_stmt_properties_token1,
    ACTIONS(262), 1,
      aux_sym_sequence_stmt_details_token1,
    ACTIONS(264), 1,
      aux_sym_sequence_stmt_title_token1,
    ACTIONS(266), 1,
      aux_sym_sequence_stmt_loop_token1,
    ACTIONS(268), 1,
      aux_sym_sequence_stmt_rect_token1,
    ACTIONS(270), 1,
      aux_sym_sequence_stmt_opt_token1,
    ACTIONS(272), 1,
      aux_sym_sequence_stmt_alt_token1,
    ACTIONS(274), 1,
      aux_sym_sequence_stmt_par_token1,
    ACTIONS(276), 1,
      sym__newline,
    STATE(257), 1,
      aux_sym_sequence_actor_repeat1,
    STATE(311), 1,
      sym_sequence_actor,
    STATE(623), 1,
      sym__sequence_participant_type,
    ACTIONS(244), 2,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
    STATE(38), 2,
      sym__sequence_stmt,
      aux_sym_diagram_sequence_repeat1,
    STATE(186), 17,
      sym_sequence_stmt_participant,
      sym_sequence_stmt_actor,
      sym_sequence_stmt_signal,
      sym_sequence_stmt_autonumber,
      sym_sequence_stmt_activate,
      sym_sequence_stmt_deactivate,
      sym_sequence_stmt_note,
      sym_sequence_stmt_links,
      sym_sequence_stmt_link,
      sym_sequence_stmt_properties,
      sym_sequence_stmt_details,
      sym_sequence_stmt_title,
      sym_sequence_stmt_loop,
      sym_sequence_stmt_rect,
      sym_sequence_stmt_opt,
      sym_sequence_stmt_alt,
      sym_sequence_stmt_par,
  [1857] = 27,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(69), 1,
      sym__sequence_actor_word,
    ACTIONS(280), 1,
      aux_sym_sequence_stmt_actor_token1,
    ACTIONS(282), 1,
      aux_sym_sequence_stmt_autonumber_token1,
    ACTIONS(284), 1,
      aux_sym_sequence_stmt_activate_token1,
    ACTIONS(286), 1,
      aux_sym_sequence_stmt_deactivate_token1,
    ACTIONS(288), 1,
      aux_sym_sequence_stmt_note_token1,
    ACTIONS(290), 1,
      aux_sym_sequence_stmt_links_token1,
    ACTIONS(292), 1,
      aux_sym_sequence_stmt_link_token1,
    ACTIONS(294), 1,
      aux_sym_sequence_stmt_properties_token1,
    ACTIONS(296), 1,
      aux_sym_sequence_stmt_details_token1,
    ACTIONS(298), 1,
      aux_sym_sequence_stmt_title_token1,
    ACTIONS(300), 1,
      aux_sym_sequence_stmt_loop_token1,
    ACTIONS(302), 1,
      aux_sym_sequence_stmt_loop_token2,
    ACTIONS(304), 1,
      aux_sym_sequence_stmt_rect_token1,
    ACTIONS(306), 1,
      aux_sym_sequence_stmt_opt_token1,
    ACTIONS(308), 1,
      aux_sym_sequence_stmt_alt_token1,
    ACTIONS(310), 1,
      aux_sym_sequence_stmt_par_token1,
    ACTIONS(312), 1,
      sym__newline,
    STATE(28), 1,
      aux_sym__sequence_subdocument,
    STATE(178), 1,
      sym__sequence_stmt,
    STATE(257), 1,
      aux_sym_sequence_actor_repeat1,
    STATE(307), 1,
      sym_sequence_actor,
    STATE(568), 1,
      sym__sequence_participant_type,
    ACTIONS(278), 2,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
    STATE(155), 17,
      sym_sequence_stmt_participant,
      sym_sequence_stmt_actor,
      sym_sequence_stmt_signal,
      sym_sequence_stmt_autonumber,
      sym_sequence_stmt_activate,
      sym_sequence_stmt_deactivate,
      sym_sequence_stmt_note,
      sym_sequence_stmt_links,
      sym_sequence_stmt_link,
      sym_sequence_stmt_properties,
      sym_sequence_stmt_details,
      sym_sequence_stmt_title,
      sym_sequence_stmt_loop,
      sym_sequence_stmt_rect,
      sym_sequence_stmt_opt,
      sym_sequence_stmt_alt,
      sym_sequence_stmt_par,
  [1956] = 27,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(69), 1,
      sym__sequence_actor_word,
    ACTIONS(280), 1,
      aux_sym_sequence_stmt_actor_token1,
    ACTIONS(282), 1,
      aux_sym_sequence_stmt_autonumber_token1,
    ACTIONS(284), 1,
      aux_sym_sequence_stmt_activate_token1,
    ACTIONS(286), 1,
      aux_sym_sequence_stmt_deactivate_token1,
    ACTIONS(288), 1,
      aux_sym_sequence_stmt_note_token1,
    ACTIONS(290), 1,
      aux_sym_sequence_stmt_links_token1,
    ACTIONS(292), 1,
      aux_sym_sequence_stmt_link_token1,
    ACTIONS(294), 1,
      aux_sym_sequence_stmt_properties_token1,
    ACTIONS(296), 1,
      aux_sym_sequence_stmt_details_token1,
    ACTIONS(298), 1,
      aux_sym_sequence_stmt_title_token1,
    ACTIONS(300), 1,
      aux_sym_sequence_stmt_loop_token1,
    ACTIONS(304), 1,
      aux_sym_sequence_stmt_rect_token1,
    ACTIONS(306), 1,
      aux_sym_sequence_stmt_opt_token1,
    ACTIONS(308), 1,
      aux_sym_sequence_stmt_alt_token1,
    ACTIONS(310), 1,
      aux_sym_sequence_stmt_par_token1,
    ACTIONS(312), 1,
      sym__newline,
    ACTIONS(314), 1,
      aux_sym_sequence_stmt_loop_token2,
    STATE(43), 1,
      aux_sym__sequence_subdocument,
    STATE(178), 1,
      sym__sequence_stmt,
    STATE(257), 1,
      aux_sym_sequence_actor_repeat1,
    STATE(307), 1,
      sym_sequence_actor,
    STATE(568), 1,
      sym__sequence_participant_type,
    ACTIONS(278), 2,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
    STATE(155), 17,
      sym_sequence_stmt_participant,
      sym_sequence_stmt_actor,
      sym_sequence_stmt_signal,
      sym_sequence_stmt_autonumber,
      sym_sequence_stmt_activate,
      sym_sequence_stmt_deactivate,
      sym_sequence_stmt_note,
      sym_sequence_stmt_links,
      sym_sequence_stmt_link,
      sym_sequence_stmt_properties,
      sym_sequence_stmt_details,
      sym_sequence_stmt_title,
      sym_sequence_stmt_loop,
      sym_sequence_stmt_rect,
      sym_sequence_stmt_opt,
      sym_sequence_stmt_alt,
      sym_sequence_stmt_par,
  [2055] = 27,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(69), 1,
      sym__sequence_actor_word,
    ACTIONS(280), 1,
      aux_sym_sequence_stmt_actor_token1,
    ACTIONS(282), 1,
      aux_sym_sequence_stmt_autonumber_token1,
    ACTIONS(284), 1,
      aux_sym_sequence_stmt_activate_token1,
    ACTIONS(286), 1,
      aux_sym_sequence_stmt_deactivate_token1,
    ACTIONS(288), 1,
      aux_sym_sequence_stmt_note_token1,
    ACTIONS(290), 1,
      aux_sym_sequence_stmt_links_token1,
    ACTIONS(292), 1,
      aux_sym_sequence_stmt_link_token1,
    ACTIONS(294), 1,
      aux_sym_sequence_stmt_properties_token1,
    ACTIONS(296), 1,
      aux_sym_sequence_stmt_details_token1,
    ACTIONS(298), 1,
      aux_sym_sequence_stmt_title_token1,
    ACTIONS(300), 1,
      aux_sym_sequence_stmt_loop_token1,
    ACTIONS(304), 1,
      aux_sym_sequence_stmt_rect_token1,
    ACTIONS(306), 1,
      aux_sym_sequence_stmt_opt_token1,
    ACTIONS(308), 1,
      aux_sym_sequence_stmt_alt_token1,
    ACTIONS(310), 1,
      aux_sym_sequence_stmt_par_token1,
    ACTIONS(312), 1,
      sym__newline,
    ACTIONS(316), 1,
      aux_sym_sequence_stmt_loop_token2,
    STATE(43), 1,
      aux_sym__sequence_subdocument,
    STATE(178), 1,
      sym__sequence_stmt,
    STATE(257), 1,
      aux_sym_sequence_actor_repeat1,
    STATE(307), 1,
      sym_sequence_actor,
    STATE(568), 1,
      sym__sequence_participant_type,
    ACTIONS(278), 2,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
    STATE(155), 17,
      sym_sequence_stmt_participant,
      sym_sequence_stmt_actor,
      sym_sequence_stmt_signal,
      sym_sequence_stmt_autonumber,
      sym_sequence_stmt_activate,
      sym_sequence_stmt_deactivate,
      sym_sequence_stmt_note,
      sym_sequence_stmt_links,
      sym_sequence_stmt_link,
      sym_sequence_stmt_properties,
      sym_sequence_stmt_details,
      sym_sequence_stmt_title,
      sym_sequence_stmt_loop,
      sym_sequence_stmt_rect,
      sym_sequence_stmt_opt,
      sym_sequence_stmt_alt,
      sym_sequence_stmt_par,
  [2154] = 27,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(69), 1,
      sym__sequence_actor_word,
    ACTIONS(280), 1,
      aux_sym_sequence_stmt_actor_token1,
    ACTIONS(282), 1,
      aux_sym_sequence_stmt_autonumber_token1,
    ACTIONS(284), 1,
      aux_sym_sequence_stmt_activate_token1,
    ACTIONS(286), 1,
      aux_sym_sequence_stmt_deactivate_token1,
    ACTIONS(288), 1,
      aux_sym_sequence_stmt_note_token1,
    ACTIONS(290), 1,
      aux_sym_sequence_stmt_links_token1,
    ACTIONS(292), 1,
      aux_sym_sequence_stmt_link_token1,
    ACTIONS(294), 1,
      aux_sym_sequence_stmt_properties_token1,
    ACTIONS(296), 1,
      aux_sym_sequence_stmt_details_token1,
    ACTIONS(298), 1,
      aux_sym_sequence_stmt_title_token1,
    ACTIONS(300), 1,
      aux_sym_sequence_stmt_loop_token1,
    ACTIONS(304), 1,
      aux_sym_sequence_stmt_rect_token1,
    ACTIONS(306), 1,
      aux_sym_sequence_stmt_opt_token1,
    ACTIONS(308), 1,
      aux_sym_sequence_stmt_alt_token1,
    ACTIONS(310), 1,
      aux_sym_sequence_stmt_par_token1,
    ACTIONS(312), 1,
      sym__newline,
    ACTIONS(318), 1,
      aux_sym_sequence_stmt_loop_token2,
    STATE(43), 1,
      aux_sym__sequence_subdocument,
    STATE(178), 1,
      sym__sequence_stmt,
    STATE(257), 1,
      aux_sym_sequence_actor_repeat1,
    STATE(307), 1,
      sym_sequence_actor,
    STATE(568), 1,
      sym__sequence_participant_type,
    ACTIONS(278), 2,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
    STATE(155), 17,
      sym_sequence_stmt_participant,
      sym_sequence_stmt_actor,
      sym_sequence_stmt_signal,
      sym_sequence_stmt_autonumber,
      sym_sequence_stmt_activate,
      sym_sequence_stmt_deactivate,
      sym_sequence_stmt_note,
      sym_sequence_stmt_links,
      sym_sequence_stmt_link,
      sym_sequence_stmt_properties,
      sym_sequence_stmt_details,
      sym_sequence_stmt_title,
      sym_sequence_stmt_loop,
      sym_sequence_stmt_rect,
      sym_sequence_stmt_opt,
      sym_sequence_stmt_alt,
      sym_sequence_stmt_par,
  [2253] = 27,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(69), 1,
      sym__sequence_actor_word,
    ACTIONS(280), 1,
      aux_sym_sequence_stmt_actor_token1,
    ACTIONS(282), 1,
      aux_sym_sequence_stmt_autonumber_token1,
    ACTIONS(284), 1,
      aux_sym_sequence_stmt_activate_token1,
    ACTIONS(286), 1,
      aux_sym_sequence_stmt_deactivate_token1,
    ACTIONS(288), 1,
      aux_sym_sequence_stmt_note_token1,
    ACTIONS(290), 1,
      aux_sym_sequence_stmt_links_token1,
    ACTIONS(292), 1,
      aux_sym_sequence_stmt_link_token1,
    ACTIONS(294), 1,
      aux_sym_sequence_stmt_properties_token1,
    ACTIONS(296), 1,
      aux_sym_sequence_stmt_details_token1,
    ACTIONS(298), 1,
      aux_sym_sequence_stmt_title_token1,
    ACTIONS(300), 1,
      aux_sym_sequence_stmt_loop_token1,
    ACTIONS(304), 1,
      aux_sym_sequence_stmt_rect_token1,
    ACTIONS(306), 1,
      aux_sym_sequence_stmt_opt_token1,
    ACTIONS(308), 1,
      aux_sym_sequence_stmt_alt_token1,
    ACTIONS(310), 1,
      aux_sym_sequence_stmt_par_token1,
    ACTIONS(312), 1,
      sym__newline,
    ACTIONS(320), 1,
      aux_sym_sequence_stmt_loop_token2,
    STATE(43), 1,
      aux_sym__sequence_subdocument,
    STATE(178), 1,
      sym__sequence_stmt,
    STATE(257), 1,
      aux_sym_sequence_actor_repeat1,
    STATE(307), 1,
      sym_sequence_actor,
    STATE(568), 1,
      sym__sequence_participant_type,
    ACTIONS(278), 2,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
    STATE(155), 17,
      sym_sequence_stmt_participant,
      sym_sequence_stmt_actor,
      sym_sequence_stmt_signal,
      sym_sequence_stmt_autonumber,
      sym_sequence_stmt_activate,
      sym_sequence_stmt_deactivate,
      sym_sequence_stmt_note,
      sym_sequence_stmt_links,
      sym_sequence_stmt_link,
      sym_sequence_stmt_properties,
      sym_sequence_stmt_details,
      sym_sequence_stmt_title,
      sym_sequence_stmt_loop,
      sym_sequence_stmt_rect,
      sym_sequence_stmt_opt,
      sym_sequence_stmt_alt,
      sym_sequence_stmt_par,
  [2352] = 27,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(69), 1,
      sym__sequence_actor_word,
    ACTIONS(280), 1,
      aux_sym_sequence_stmt_actor_token1,
    ACTIONS(282), 1,
      aux_sym_sequence_stmt_autonumber_token1,
    ACTIONS(284), 1,
      aux_sym_sequence_stmt_activate_token1,
    ACTIONS(286), 1,
      aux_sym_sequence_stmt_deactivate_token1,
    ACTIONS(288), 1,
      aux_sym_sequence_stmt_note_token1,
    ACTIONS(290), 1,
      aux_sym_sequence_stmt_links_token1,
    ACTIONS(292), 1,
      aux_sym_sequence_stmt_link_token1,
    ACTIONS(294), 1,
      aux_sym_sequence_stmt_properties_token1,
    ACTIONS(296), 1,
      aux_sym_sequence_stmt_details_token1,
    ACTIONS(298), 1,
      aux_sym_sequence_stmt_title_token1,
    ACTIONS(300), 1,
      aux_sym_sequence_stmt_loop_token1,
    ACTIONS(304), 1,
      aux_sym_sequence_stmt_rect_token1,
    ACTIONS(306), 1,
      aux_sym_sequence_stmt_opt_token1,
    ACTIONS(308), 1,
      aux_sym_sequence_stmt_alt_token1,
    ACTIONS(310), 1,
      aux_sym_sequence_stmt_par_token1,
    ACTIONS(312), 1,
      sym__newline,
    ACTIONS(322), 1,
      aux_sym_sequence_stmt_loop_token2,
    STATE(22), 1,
      aux_sym__sequence_subdocument,
    STATE(178), 1,
      sym__sequence_stmt,
    STATE(257), 1,
      aux_sym_sequence_actor_repeat1,
    STATE(307), 1,
      sym_sequence_actor,
    STATE(568), 1,
      sym__sequence_participant_type,
    ACTIONS(278), 2,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
    STATE(155), 17,
      sym_sequence_stmt_participant,
      sym_sequence_stmt_actor,
      sym_sequence_stmt_signal,
      sym_sequence_stmt_autonumber,
      sym_sequence_stmt_activate,
      sym_sequence_stmt_deactivate,
      sym_sequence_stmt_note,
      sym_sequence_stmt_links,
      sym_sequence_stmt_link,
      sym_sequence_stmt_properties,
      sym_sequence_stmt_details,
      sym_sequence_stmt_title,
      sym_sequence_stmt_loop,
      sym_sequence_stmt_rect,
      sym_sequence_stmt_opt,
      sym_sequence_stmt_alt,
      sym_sequence_stmt_par,
  [2451] = 27,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(69), 1,
      sym__sequence_actor_word,
    ACTIONS(280), 1,
      aux_sym_sequence_stmt_actor_token1,
    ACTIONS(282), 1,
      aux_sym_sequence_stmt_autonumber_token1,
    ACTIONS(284), 1,
      aux_sym_sequence_stmt_activate_token1,
    ACTIONS(286), 1,
      aux_sym_sequence_stmt_deactivate_token1,
    ACTIONS(288), 1,
      aux_sym_sequence_stmt_note_token1,
    ACTIONS(290), 1,
      aux_sym_sequence_stmt_links_token1,
    ACTIONS(292), 1,
      aux_sym_sequence_stmt_link_token1,
    ACTIONS(294), 1,
      aux_sym_sequence_stmt_properties_token1,
    ACTIONS(296), 1,
      aux_sym_sequence_stmt_details_token1,
    ACTIONS(298), 1,
      aux_sym_sequence_stmt_title_token1,
    ACTIONS(300), 1,
      aux_sym_sequence_stmt_loop_token1,
    ACTIONS(304), 1,
      aux_sym_sequence_stmt_rect_token1,
    ACTIONS(306), 1,
      aux_sym_sequence_stmt_opt_token1,
    ACTIONS(308), 1,
      aux_sym_sequence_stmt_alt_token1,
    ACTIONS(310), 1,
      aux_sym_sequence_stmt_par_token1,
    ACTIONS(312), 1,
      sym__newline,
    ACTIONS(324), 1,
      aux_sym_sequence_stmt_loop_token2,
    STATE(23), 1,
      aux_sym__sequence_subdocument,
    STATE(178), 1,
      sym__sequence_stmt,
    STATE(257), 1,
      aux_sym_sequence_actor_repeat1,
    STATE(307), 1,
      sym_sequence_actor,
    STATE(568), 1,
      sym__sequence_participant_type,
    ACTIONS(278), 2,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
    STATE(155), 17,
      sym_sequence_stmt_participant,
      sym_sequence_stmt_actor,
      sym_sequence_stmt_signal,
      sym_sequence_stmt_autonumber,
      sym_sequence_stmt_activate,
      sym_sequence_stmt_deactivate,
      sym_sequence_stmt_note,
      sym_sequence_stmt_links,
      sym_sequence_stmt_link,
      sym_sequence_stmt_properties,
      sym_sequence_stmt_details,
      sym_sequence_stmt_title,
      sym_sequence_stmt_loop,
      sym_sequence_stmt_rect,
      sym_sequence_stmt_opt,
      sym_sequence_stmt_alt,
      sym_sequence_stmt_par,
  [2550] = 27,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(69), 1,
      sym__sequence_actor_word,
    ACTIONS(280), 1,
      aux_sym_sequence_stmt_actor_token1,
    ACTIONS(282), 1,
      aux_sym_sequence_stmt_autonumber_token1,
    ACTIONS(284), 1,
      aux_sym_sequence_stmt_activate_token1,
    ACTIONS(286), 1,
      aux_sym_sequence_stmt_deactivate_token1,
    ACTIONS(288), 1,
      aux_sym_sequence_stmt_note_token1,
    ACTIONS(290), 1,
      aux_sym_sequence_stmt_links_token1,
    ACTIONS(292), 1,
      aux_sym_sequence_stmt_link_token1,
    ACTIONS(294), 1,
      aux_sym_sequence_stmt_properties_token1,
    ACTIONS(296), 1,
      aux_sym_sequence_stmt_details_token1,
    ACTIONS(298), 1,
      aux_sym_sequence_stmt_title_token1,
    ACTIONS(300), 1,
      aux_sym_sequence_stmt_loop_token1,
    ACTIONS(304), 1,
      aux_sym_sequence_stmt_rect_token1,
    ACTIONS(306), 1,
      aux_sym_sequence_stmt_opt_token1,
    ACTIONS(308), 1,
      aux_sym_sequence_stmt_alt_token1,
    ACTIONS(310), 1,
      aux_sym_sequence_stmt_par_token1,
    ACTIONS(312), 1,
      sym__newline,
    ACTIONS(326), 1,
      aux_sym_sequence_stmt_loop_token2,
    STATE(24), 1,
      aux_sym__sequence_subdocument,
    STATE(178), 1,
      sym__sequence_stmt,
    STATE(257), 1,
      aux_sym_sequence_actor_repeat1,
    STATE(307), 1,
      sym_sequence_actor,
    STATE(568), 1,
      sym__sequence_participant_type,
    ACTIONS(278), 2,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
    STATE(155), 17,
      sym_sequence_stmt_participant,
      sym_sequence_stmt_actor,
      sym_sequence_stmt_signal,
      sym_sequence_stmt_autonumber,
      sym_sequence_stmt_activate,
      sym_sequence_stmt_deactivate,
      sym_sequence_stmt_note,
      sym_sequence_stmt_links,
      sym_sequence_stmt_link,
      sym_sequence_stmt_properties,
      sym_sequence_stmt_details,
      sym_sequence_stmt_title,
      sym_sequence_stmt_loop,
      sym_sequence_stmt_rect,
      sym_sequence_stmt_opt,
      sym_sequence_stmt_alt,
      sym_sequence_stmt_par,
  [2649] = 27,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(69), 1,
      sym__sequence_actor_word,
    ACTIONS(280), 1,
      aux_sym_sequence_stmt_actor_token1,
    ACTIONS(282), 1,
      aux_sym_sequence_stmt_autonumber_token1,
    ACTIONS(284), 1,
      aux_sym_sequence_stmt_activate_token1,
    ACTIONS(286), 1,
      aux_sym_sequence_stmt_deactivate_token1,
    ACTIONS(288), 1,
      aux_sym_sequence_stmt_note_token1,
    ACTIONS(290), 1,
      aux_sym_sequence_stmt_links_token1,
    ACTIONS(292), 1,
      aux_sym_sequence_stmt_link_token1,
    ACTIONS(294), 1,
      aux_sym_sequence_stmt_properties_token1,
    ACTIONS(296), 1,
      aux_sym_sequence_stmt_details_token1,
    ACTIONS(298), 1,
      aux_sym_sequence_stmt_title_token1,
    ACTIONS(300), 1,
      aux_sym_sequence_stmt_loop_token1,
    ACTIONS(304), 1,
      aux_sym_sequence_stmt_rect_token1,
    ACTIONS(306), 1,
      aux_sym_sequence_stmt_opt_token1,
    ACTIONS(308), 1,
      aux_sym_sequence_stmt_alt_token1,
    ACTIONS(310), 1,
      aux_sym_sequence_stmt_par_token1,
    ACTIONS(312), 1,
      sym__newline,
    ACTIONS(328), 1,
      aux_sym_sequence_stmt_loop_token2,
    STATE(43), 1,
      aux_sym__sequence_subdocument,
    STATE(178), 1,
      sym__sequence_stmt,
    STATE(257), 1,
      aux_sym_sequence_actor_repeat1,
    STATE(307), 1,
      sym_sequence_actor,
    STATE(568), 1,
      sym__sequence_participant_type,
    ACTIONS(278), 2,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
    STATE(155), 17,
      sym_sequence_stmt_participant,
      sym_sequence_stmt_actor,
      sym_sequence_stmt_signal,
      sym_sequence_stmt_autonumber,
      sym_sequence_stmt_activate,
      sym_sequence_stmt_deactivate,
      sym_sequence_stmt_note,
      sym_sequence_stmt_links,
      sym_sequence_stmt_link,
      sym_sequence_stmt_properties,
      sym_sequence_stmt_details,
      sym_sequence_stmt_title,
      sym_sequence_stmt_loop,
      sym_sequence_stmt_rect,
      sym_sequence_stmt_opt,
      sym_sequence_stmt_alt,
      sym_sequence_stmt_par,
  [2748] = 27,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(69), 1,
      sym__sequence_actor_word,
    ACTIONS(280), 1,
      aux_sym_sequence_stmt_actor_token1,
    ACTIONS(282), 1,
      aux_sym_sequence_stmt_autonumber_token1,
    ACTIONS(284), 1,
      aux_sym_sequence_stmt_activate_token1,
    ACTIONS(286), 1,
      aux_sym_sequence_stmt_deactivate_token1,
    ACTIONS(288), 1,
      aux_sym_sequence_stmt_note_token1,
    ACTIONS(290), 1,
      aux_sym_sequence_stmt_links_token1,
    ACTIONS(292), 1,
      aux_sym_sequence_stmt_link_token1,
    ACTIONS(294), 1,
      aux_sym_sequence_stmt_properties_token1,
    ACTIONS(296), 1,
      aux_sym_sequence_stmt_details_token1,
    ACTIONS(298), 1,
      aux_sym_sequence_stmt_title_token1,
    ACTIONS(300), 1,
      aux_sym_sequence_stmt_loop_token1,
    ACTIONS(304), 1,
      aux_sym_sequence_stmt_rect_token1,
    ACTIONS(306), 1,
      aux_sym_sequence_stmt_opt_token1,
    ACTIONS(308), 1,
      aux_sym_sequence_stmt_alt_token1,
    ACTIONS(310), 1,
      aux_sym_sequence_stmt_par_token1,
    ACTIONS(312), 1,
      sym__newline,
    ACTIONS(330), 1,
      aux_sym_sequence_stmt_loop_token2,
    STATE(43), 1,
      aux_sym__sequence_subdocument,
    STATE(178), 1,
      sym__sequence_stmt,
    STATE(257), 1,
      aux_sym_sequence_actor_repeat1,
    STATE(307), 1,
      sym_sequence_actor,
    STATE(568), 1,
      sym__sequence_participant_type,
    ACTIONS(278), 2,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
    STATE(155), 17,
      sym_sequence_stmt_participant,
      sym_sequence_stmt_actor,
      sym_sequence_stmt_signal,
      sym_sequence_stmt_autonumber,
      sym_sequence_stmt_activate,
      sym_sequence_stmt_deactivate,
      sym_sequence_stmt_note,
      sym_sequence_stmt_links,
      sym_sequence_stmt_link,
      sym_sequence_stmt_properties,
      sym_sequence_stmt_details,
      sym_sequence_stmt_title,
      sym_sequence_stmt_loop,
      sym_sequence_stmt_rect,
      sym_sequence_stmt_opt,
      sym_sequence_stmt_alt,
      sym_sequence_stmt_par,
  [2847] = 27,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(69), 1,
      sym__sequence_actor_word,
    ACTIONS(280), 1,
      aux_sym_sequence_stmt_actor_token1,
    ACTIONS(282), 1,
      aux_sym_sequence_stmt_autonumber_token1,
    ACTIONS(284), 1,
      aux_sym_sequence_stmt_activate_token1,
    ACTIONS(286), 1,
      aux_sym_sequence_stmt_deactivate_token1,
    ACTIONS(288), 1,
      aux_sym_sequence_stmt_note_token1,
    ACTIONS(290), 1,
      aux_sym_sequence_stmt_links_token1,
    ACTIONS(292), 1,
      aux_sym_sequence_stmt_link_token1,
    ACTIONS(294), 1,
      aux_sym_sequence_stmt_properties_token1,
    ACTIONS(296), 1,
      aux_sym_sequence_stmt_details_token1,
    ACTIONS(298), 1,
      aux_sym_sequence_stmt_title_token1,
    ACTIONS(300), 1,
      aux_sym_sequence_stmt_loop_token1,
    ACTIONS(304), 1,
      aux_sym_sequence_stmt_rect_token1,
    ACTIONS(306), 1,
      aux_sym_sequence_stmt_opt_token1,
    ACTIONS(308), 1,
      aux_sym_sequence_stmt_alt_token1,
    ACTIONS(310), 1,
      aux_sym_sequence_stmt_par_token1,
    ACTIONS(312), 1,
      sym__newline,
    ACTIONS(332), 1,
      aux_sym_sequence_stmt_loop_token2,
    STATE(43), 1,
      aux_sym__sequence_subdocument,
    STATE(178), 1,
      sym__sequence_stmt,
    STATE(257), 1,
      aux_sym_sequence_actor_repeat1,
    STATE(307), 1,
      sym_sequence_actor,
    STATE(568), 1,
      sym__sequence_participant_type,
    ACTIONS(278), 2,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
    STATE(155), 17,
      sym_sequence_stmt_participant,
      sym_sequence_stmt_actor,
      sym_sequence_stmt_signal,
      sym_sequence_stmt_autonumber,
      sym_sequence_stmt_activate,
      sym_sequence_stmt_deactivate,
      sym_sequence_stmt_note,
      sym_sequence_stmt_links,
      sym_sequence_stmt_link,
      sym_sequence_stmt_properties,
      sym_sequence_stmt_details,
      sym_sequence_stmt_title,
      sym_sequence_stmt_loop,
      sym_sequence_stmt_rect,
      sym_sequence_stmt_opt,
      sym_sequence_stmt_alt,
      sym_sequence_stmt_par,
  [2946] = 27,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(69), 1,
      sym__sequence_actor_word,
    ACTIONS(280), 1,
      aux_sym_sequence_stmt_actor_token1,
    ACTIONS(282), 1,
      aux_sym_sequence_stmt_autonumber_token1,
    ACTIONS(284), 1,
      aux_sym_sequence_stmt_activate_token1,
    ACTIONS(286), 1,
      aux_sym_sequence_stmt_deactivate_token1,
    ACTIONS(288), 1,
      aux_sym_sequence_stmt_note_token1,
    ACTIONS(290), 1,
      aux_sym_sequence_stmt_links_token1,
    ACTIONS(292), 1,
      aux_sym_sequence_stmt_link_token1,
    ACTIONS(294), 1,
      aux_sym_sequence_stmt_properties_token1,
    ACTIONS(296), 1,
      aux_sym_sequence_stmt_details_token1,
    ACTIONS(298), 1,
      aux_sym_sequence_stmt_title_token1,
    ACTIONS(300), 1,
      aux_sym_sequence_stmt_loop_token1,
    ACTIONS(304), 1,
      aux_sym_sequence_stmt_rect_token1,
    ACTIONS(306), 1,
      aux_sym_sequence_stmt_opt_token1,
    ACTIONS(308), 1,
      aux_sym_sequence_stmt_alt_token1,
    ACTIONS(310), 1,
      aux_sym_sequence_stmt_par_token1,
    ACTIONS(312), 1,
      sym__newline,
    ACTIONS(334), 1,
      aux_sym_sequence_stmt_loop_token2,
    STATE(43), 1,
      aux_sym__sequence_subdocument,
    STATE(178), 1,
      sym__sequence_stmt,
    STATE(257), 1,
      aux_sym_sequence_actor_repeat1,
    STATE(307), 1,
      sym_sequence_actor,
    STATE(568), 1,
      sym__sequence_participant_type,
    ACTIONS(278), 2,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
    STATE(155), 17,
      sym_sequence_stmt_participant,
      sym_sequence_stmt_actor,
      sym_sequence_stmt_signal,
      sym_sequence_stmt_autonumber,
      sym_sequence_stmt_activate,
      sym_sequence_stmt_deactivate,
      sym_sequence_stmt_note,
      sym_sequence_stmt_links,
      sym_sequence_stmt_link,
      sym_sequence_stmt_properties,
      sym_sequence_stmt_details,
      sym_sequence_stmt_title,
      sym_sequence_stmt_loop,
      sym_sequence_stmt_rect,
      sym_sequence_stmt_opt,
      sym_sequence_stmt_alt,
      sym_sequence_stmt_par,
  [3045] = 27,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(69), 1,
      sym__sequence_actor_word,
    ACTIONS(280), 1,
      aux_sym_sequence_stmt_actor_token1,
    ACTIONS(282), 1,
      aux_sym_sequence_stmt_autonumber_token1,
    ACTIONS(284), 1,
      aux_sym_sequence_stmt_activate_token1,
    ACTIONS(286), 1,
      aux_sym_sequence_stmt_deactivate_token1,
    ACTIONS(288), 1,
      aux_sym_sequence_stmt_note_token1,
    ACTIONS(290), 1,
      aux_sym_sequence_stmt_links_token1,
    ACTIONS(292), 1,
      aux_sym_sequence_stmt_link_token1,
    ACTIONS(294), 1,
      aux_sym_sequence_stmt_properties_token1,
    ACTIONS(296), 1,
      aux_sym_sequence_stmt_details_token1,
    ACTIONS(298), 1,
      aux_sym_sequence_stmt_title_token1,
    ACTIONS(300), 1,
      aux_sym_sequence_stmt_loop_token1,
    ACTIONS(304), 1,
      aux_sym_sequence_stmt_rect_token1,
    ACTIONS(306), 1,
      aux_sym_sequence_stmt_opt_token1,
    ACTIONS(308), 1,
      aux_sym_sequence_stmt_alt_token1,
    ACTIONS(310), 1,
      aux_sym_sequence_stmt_par_token1,
    ACTIONS(312), 1,
      sym__newline,
    ACTIONS(336), 1,
      aux_sym_sequence_stmt_loop_token2,
    STATE(43), 1,
      aux_sym__sequence_subdocument,
    STATE(178), 1,
      sym__sequence_stmt,
    STATE(257), 1,
      aux_sym_sequence_actor_repeat1,
    STATE(307), 1,
      sym_sequence_actor,
    STATE(568), 1,
      sym__sequence_participant_type,
    ACTIONS(278), 2,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
    STATE(155), 17,
      sym_sequence_stmt_participant,
      sym_sequence_stmt_actor,
      sym_sequence_stmt_signal,
      sym_sequence_stmt_autonumber,
      sym_sequence_stmt_activate,
      sym_sequence_stmt_deactivate,
      sym_sequence_stmt_note,
      sym_sequence_stmt_links,
      sym_sequence_stmt_link,
      sym_sequence_stmt_properties,
      sym_sequence_stmt_details,
      sym_sequence_stmt_title,
      sym_sequence_stmt_loop,
      sym_sequence_stmt_rect,
      sym_sequence_stmt_opt,
      sym_sequence_stmt_alt,
      sym_sequence_stmt_par,
  [3144] = 27,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(69), 1,
      sym__sequence_actor_word,
    ACTIONS(280), 1,
      aux_sym_sequence_stmt_actor_token1,
    ACTIONS(282), 1,
      aux_sym_sequence_stmt_autonumber_token1,
    ACTIONS(284), 1,
      aux_sym_sequence_stmt_activate_token1,
    ACTIONS(286), 1,
      aux_sym_sequence_stmt_deactivate_token1,
    ACTIONS(288), 1,
      aux_sym_sequence_stmt_note_token1,
    ACTIONS(290), 1,
      aux_sym_sequence_stmt_links_token1,
    ACTIONS(292), 1,
      aux_sym_sequence_stmt_link_token1,
    ACTIONS(294), 1,
      aux_sym_sequence_stmt_properties_token1,
    ACTIONS(296), 1,
      aux_sym_sequence_stmt_details_token1,
    ACTIONS(298), 1,
      aux_sym_sequence_stmt_title_token1,
    ACTIONS(300), 1,
      aux_sym_sequence_stmt_loop_token1,
    ACTIONS(304), 1,
      aux_sym_sequence_stmt_rect_token1,
    ACTIONS(306), 1,
      aux_sym_sequence_stmt_opt_token1,
    ACTIONS(308), 1,
      aux_sym_sequence_stmt_alt_token1,
    ACTIONS(310), 1,
      aux_sym_sequence_stmt_par_token1,
    ACTIONS(312), 1,
      sym__newline,
    ACTIONS(338), 1,
      aux_sym_sequence_stmt_loop_token2,
    STATE(21), 1,
      aux_sym__sequence_subdocument,
    STATE(178), 1,
      sym__sequence_stmt,
    STATE(257), 1,
      aux_sym_sequence_actor_repeat1,
    STATE(307), 1,
      sym_sequence_actor,
    STATE(568), 1,
      sym__sequence_participant_type,
    ACTIONS(278), 2,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
    STATE(155), 17,
      sym_sequence_stmt_participant,
      sym_sequence_stmt_actor,
      sym_sequence_stmt_signal,
      sym_sequence_stmt_autonumber,
      sym_sequence_stmt_activate,
      sym_sequence_stmt_deactivate,
      sym_sequence_stmt_note,
      sym_sequence_stmt_links,
      sym_sequence_stmt_link,
      sym_sequence_stmt_properties,
      sym_sequence_stmt_details,
      sym_sequence_stmt_title,
      sym_sequence_stmt_loop,
      sym_sequence_stmt_rect,
      sym_sequence_stmt_opt,
      sym_sequence_stmt_alt,
      sym_sequence_stmt_par,
  [3243] = 27,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(69), 1,
      sym__sequence_actor_word,
    ACTIONS(280), 1,
      aux_sym_sequence_stmt_actor_token1,
    ACTIONS(282), 1,
      aux_sym_sequence_stmt_autonumber_token1,
    ACTIONS(284), 1,
      aux_sym_sequence_stmt_activate_token1,
    ACTIONS(286), 1,
      aux_sym_sequence_stmt_deactivate_token1,
    ACTIONS(288), 1,
      aux_sym_sequence_stmt_note_token1,
    ACTIONS(290), 1,
      aux_sym_sequence_stmt_links_token1,
    ACTIONS(292), 1,
      aux_sym_sequence_stmt_link_token1,
    ACTIONS(294), 1,
      aux_sym_sequence_stmt_properties_token1,
    ACTIONS(296), 1,
      aux_sym_sequence_stmt_details_token1,
    ACTIONS(298), 1,
      aux_sym_sequence_stmt_title_token1,
    ACTIONS(300), 1,
      aux_sym_sequence_stmt_loop_token1,
    ACTIONS(304), 1,
      aux_sym_sequence_stmt_rect_token1,
    ACTIONS(306), 1,
      aux_sym_sequence_stmt_opt_token1,
    ACTIONS(308), 1,
      aux_sym_sequence_stmt_alt_token1,
    ACTIONS(310), 1,
      aux_sym_sequence_stmt_par_token1,
    ACTIONS(312), 1,
      sym__newline,
    ACTIONS(340), 1,
      aux_sym_sequence_stmt_loop_token2,
    STATE(32), 1,
      aux_sym__sequence_subdocument,
    STATE(178), 1,
      sym__sequence_stmt,
    STATE(257), 1,
      aux_sym_sequence_actor_repeat1,
    STATE(307), 1,
      sym_sequence_actor,
    STATE(568), 1,
      sym__sequence_participant_type,
    ACTIONS(278), 2,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
    STATE(155), 17,
      sym_sequence_stmt_participant,
      sym_sequence_stmt_actor,
      sym_sequence_stmt_signal,
      sym_sequence_stmt_autonumber,
      sym_sequence_stmt_activate,
      sym_sequence_stmt_deactivate,
      sym_sequence_stmt_note,
      sym_sequence_stmt_links,
      sym_sequence_stmt_link,
      sym_sequence_stmt_properties,
      sym_sequence_stmt_details,
      sym_sequence_stmt_title,
      sym_sequence_stmt_loop,
      sym_sequence_stmt_rect,
      sym_sequence_stmt_opt,
      sym_sequence_stmt_alt,
      sym_sequence_stmt_par,
  [3342] = 27,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(69), 1,
      sym__sequence_actor_word,
    ACTIONS(280), 1,
      aux_sym_sequence_stmt_actor_token1,
    ACTIONS(282), 1,
      aux_sym_sequence_stmt_autonumber_token1,
    ACTIONS(284), 1,
      aux_sym_sequence_stmt_activate_token1,
    ACTIONS(286), 1,
      aux_sym_sequence_stmt_deactivate_token1,
    ACTIONS(288), 1,
      aux_sym_sequence_stmt_note_token1,
    ACTIONS(290), 1,
      aux_sym_sequence_stmt_links_token1,
    ACTIONS(292), 1,
      aux_sym_sequence_stmt_link_token1,
    ACTIONS(294), 1,
      aux_sym_sequence_stmt_properties_token1,
    ACTIONS(296), 1,
      aux_sym_sequence_stmt_details_token1,
    ACTIONS(298), 1,
      aux_sym_sequence_stmt_title_token1,
    ACTIONS(300), 1,
      aux_sym_sequence_stmt_loop_token1,
    ACTIONS(304), 1,
      aux_sym_sequence_stmt_rect_token1,
    ACTIONS(306), 1,
      aux_sym_sequence_stmt_opt_token1,
    ACTIONS(308), 1,
      aux_sym_sequence_stmt_alt_token1,
    ACTIONS(310), 1,
      aux_sym_sequence_stmt_par_token1,
    ACTIONS(312), 1,
      sym__newline,
    ACTIONS(342), 1,
      aux_sym_sequence_stmt_loop_token2,
    STATE(29), 1,
      aux_sym__sequence_subdocument,
    STATE(178), 1,
      sym__sequence_stmt,
    STATE(257), 1,
      aux_sym_sequence_actor_repeat1,
    STATE(307), 1,
      sym_sequence_actor,
    STATE(568), 1,
      sym__sequence_participant_type,
    ACTIONS(278), 2,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
    STATE(155), 17,
      sym_sequence_stmt_participant,
      sym_sequence_stmt_actor,
      sym_sequence_stmt_signal,
      sym_sequence_stmt_autonumber,
      sym_sequence_stmt_activate,
      sym_sequence_stmt_deactivate,
      sym_sequence_stmt_note,
      sym_sequence_stmt_links,
      sym_sequence_stmt_link,
      sym_sequence_stmt_properties,
      sym_sequence_stmt_details,
      sym_sequence_stmt_title,
      sym_sequence_stmt_loop,
      sym_sequence_stmt_rect,
      sym_sequence_stmt_opt,
      sym_sequence_stmt_alt,
      sym_sequence_stmt_par,
  [3441] = 27,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(69), 1,
      sym__sequence_actor_word,
    ACTIONS(280), 1,
      aux_sym_sequence_stmt_actor_token1,
    ACTIONS(282), 1,
      aux_sym_sequence_stmt_autonumber_token1,
    ACTIONS(284), 1,
      aux_sym_sequence_stmt_activate_token1,
    ACTIONS(286), 1,
      aux_sym_sequence_stmt_deactivate_token1,
    ACTIONS(288), 1,
      aux_sym_sequence_stmt_note_token1,
    ACTIONS(290), 1,
      aux_sym_sequence_stmt_links_token1,
    ACTIONS(292), 1,
      aux_sym_sequence_stmt_link_token1,
    ACTIONS(294), 1,
      aux_sym_sequence_stmt_properties_token1,
    ACTIONS(296), 1,
      aux_sym_sequence_stmt_details_token1,
    ACTIONS(298), 1,
      aux_sym_sequence_stmt_title_token1,
    ACTIONS(300), 1,
      aux_sym_sequence_stmt_loop_token1,
    ACTIONS(304), 1,
      aux_sym_sequence_stmt_rect_token1,
    ACTIONS(306), 1,
      aux_sym_sequence_stmt_opt_token1,
    ACTIONS(308), 1,
      aux_sym_sequence_stmt_alt_token1,
    ACTIONS(310), 1,
      aux_sym_sequence_stmt_par_token1,
    ACTIONS(312), 1,
      sym__newline,
    ACTIONS(344), 1,
      aux_sym_sequence_stmt_loop_token2,
    STATE(30), 1,
      aux_sym__sequence_subdocument,
    STATE(178), 1,
      sym__sequence_stmt,
    STATE(257), 1,
      aux_sym_sequence_actor_repeat1,
    STATE(307), 1,
      sym_sequence_actor,
    STATE(568), 1,
      sym__sequence_participant_type,
    ACTIONS(278), 2,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
    STATE(155), 17,
      sym_sequence_stmt_participant,
      sym_sequence_stmt_actor,
      sym_sequence_stmt_signal,
      sym_sequence_stmt_autonumber,
      sym_sequence_stmt_activate,
      sym_sequence_stmt_deactivate,
      sym_sequence_stmt_note,
      sym_sequence_stmt_links,
      sym_sequence_stmt_link,
      sym_sequence_stmt_properties,
      sym_sequence_stmt_details,
      sym_sequence_stmt_title,
      sym_sequence_stmt_loop,
      sym_sequence_stmt_rect,
      sym_sequence_stmt_opt,
      sym_sequence_stmt_alt,
      sym_sequence_stmt_par,
  [3540] = 27,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(69), 1,
      sym__sequence_actor_word,
    ACTIONS(280), 1,
      aux_sym_sequence_stmt_actor_token1,
    ACTIONS(282), 1,
      aux_sym_sequence_stmt_autonumber_token1,
    ACTIONS(284), 1,
      aux_sym_sequence_stmt_activate_token1,
    ACTIONS(286), 1,
      aux_sym_sequence_stmt_deactivate_token1,
    ACTIONS(288), 1,
      aux_sym_sequence_stmt_note_token1,
    ACTIONS(290), 1,
      aux_sym_sequence_stmt_links_token1,
    ACTIONS(292), 1,
      aux_sym_sequence_stmt_link_token1,
    ACTIONS(294), 1,
      aux_sym_sequence_stmt_properties_token1,
    ACTIONS(296), 1,
      aux_sym_sequence_stmt_details_token1,
    ACTIONS(298), 1,
      aux_sym_sequence_stmt_title_token1,
    ACTIONS(300), 1,
      aux_sym_sequence_stmt_loop_token1,
    ACTIONS(304), 1,
      aux_sym_sequence_stmt_rect_token1,
    ACTIONS(306), 1,
      aux_sym_sequence_stmt_opt_token1,
    ACTIONS(308), 1,
      aux_sym_sequence_stmt_alt_token1,
    ACTIONS(310), 1,
      aux_sym_sequence_stmt_par_token1,
    ACTIONS(312), 1,
      sym__newline,
    ACTIONS(346), 1,
      aux_sym_sequence_stmt_loop_token2,
    STATE(31), 1,
      aux_sym__sequence_subdocument,
    STATE(178), 1,
      sym__sequence_stmt,
    STATE(257), 1,
      aux_sym_sequence_actor_repeat1,
    STATE(307), 1,
      sym_sequence_actor,
    STATE(568), 1,
      sym__sequence_participant_type,
    ACTIONS(278), 2,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
    STATE(155), 17,
      sym_sequence_stmt_participant,
      sym_sequence_stmt_actor,
      sym_sequence_stmt_signal,
      sym_sequence_stmt_autonumber,
      sym_sequence_stmt_activate,
      sym_sequence_stmt_deactivate,
      sym_sequence_stmt_note,
      sym_sequence_stmt_links,
      sym_sequence_stmt_link,
      sym_sequence_stmt_properties,
      sym_sequence_stmt_details,
      sym_sequence_stmt_title,
      sym_sequence_stmt_loop,
      sym_sequence_stmt_rect,
      sym_sequence_stmt_opt,
      sym_sequence_stmt_alt,
      sym_sequence_stmt_par,
  [3639] = 26,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(348), 1,
      ts_builtin_sym_end,
    ACTIONS(353), 1,
      aux_sym_sequence_stmt_actor_token1,
    ACTIONS(356), 1,
      aux_sym_sequence_stmt_autonumber_token1,
    ACTIONS(359), 1,
      aux_sym_sequence_stmt_activate_token1,
    ACTIONS(362), 1,
      aux_sym_sequence_stmt_deactivate_token1,
    ACTIONS(365), 1,
      aux_sym_sequence_stmt_note_token1,
    ACTIONS(368), 1,
      aux_sym_sequence_stmt_links_token1,
    ACTIONS(371), 1,
      aux_sym_sequence_stmt_link_token1,
    ACTIONS(374), 1,
      aux_sym_sequence_stmt_properties_token1,
    ACTIONS(377), 1,
      aux_sym_sequence_stmt_details_token1,
    ACTIONS(380), 1,
      aux_sym_sequence_stmt_title_token1,
    ACTIONS(383), 1,
      aux_sym_sequence_stmt_loop_token1,
    ACTIONS(386), 1,
      aux_sym_sequence_stmt_rect_token1,
    ACTIONS(389), 1,
      aux_sym_sequence_stmt_opt_token1,
    ACTIONS(392), 1,
      aux_sym_sequence_stmt_alt_token1,
    ACTIONS(395), 1,
      aux_sym_sequence_stmt_par_token1,
    ACTIONS(398), 1,
      sym__newline,
    ACTIONS(401), 1,
      sym__sequence_actor_word,
    STATE(257), 1,
      aux_sym_sequence_actor_repeat1,
    STATE(311), 1,
      sym_sequence_actor,
    STATE(623), 1,
      sym__sequence_participant_type,
    ACTIONS(350), 2,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
    STATE(38), 2,
      sym__sequence_stmt,
      aux_sym_diagram_sequence_repeat1,
    STATE(186), 17,
      sym_sequence_stmt_participant,
      sym_sequence_stmt_actor,
      sym_sequence_stmt_signal,
      sym_sequence_stmt_autonumber,
      sym_sequence_stmt_activate,
      sym_sequence_stmt_deactivate,
      sym_sequence_stmt_note,
      sym_sequence_stmt_links,
      sym_sequence_stmt_link,
      sym_sequence_stmt_properties,
      sym_sequence_stmt_details,
      sym_sequence_stmt_title,
      sym_sequence_stmt_loop,
      sym_sequence_stmt_rect,
      sym_sequence_stmt_opt,
      sym_sequence_stmt_alt,
      sym_sequence_stmt_par,
  [3736] = 26,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(69), 1,
      sym__sequence_actor_word,
    ACTIONS(246), 1,
      aux_sym_sequence_stmt_actor_token1,
    ACTIONS(248), 1,
      aux_sym_sequence_stmt_autonumber_token1,
    ACTIONS(250), 1,
      aux_sym_sequence_stmt_activate_token1,
    ACTIONS(252), 1,
      aux_sym_sequence_stmt_deactivate_token1,
    ACTIONS(254), 1,
      aux_sym_sequence_stmt_note_token1,
    ACTIONS(256), 1,
      aux_sym_sequence_stmt_links_token1,
    ACTIONS(258), 1,
      aux_sym_sequence_stmt_link_token1,
    ACTIONS(260), 1,
      aux_sym_sequence_stmt_properties_token1,
    ACTIONS(262), 1,
      aux_sym_sequence_stmt_details_token1,
    ACTIONS(264), 1,
      aux_sym_sequence_stmt_title_token1,
    ACTIONS(266), 1,
      aux_sym_sequence_stmt_loop_token1,
    ACTIONS(268), 1,
      aux_sym_sequence_stmt_rect_token1,
    ACTIONS(270), 1,
      aux_sym_sequence_stmt_opt_token1,
    ACTIONS(272), 1,
      aux_sym_sequence_stmt_alt_token1,
    ACTIONS(274), 1,
      aux_sym_sequence_stmt_par_token1,
    ACTIONS(404), 1,
      ts_builtin_sym_end,
    ACTIONS(406), 1,
      sym__newline,
    STATE(257), 1,
      aux_sym_sequence_actor_repeat1,
    STATE(311), 1,
      sym_sequence_actor,
    STATE(623), 1,
      sym__sequence_participant_type,
    ACTIONS(244), 2,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
    STATE(19), 2,
      sym__sequence_stmt,
      aux_sym_diagram_sequence_repeat1,
    STATE(186), 17,
      sym_sequence_stmt_participant,
      sym_sequence_stmt_actor,
      sym_sequence_stmt_signal,
      sym_sequence_stmt_autonumber,
      sym_sequence_stmt_activate,
      sym_sequence_stmt_deactivate,
      sym_sequence_stmt_note,
      sym_sequence_stmt_links,
      sym_sequence_stmt_link,
      sym_sequence_stmt_properties,
      sym_sequence_stmt_details,
      sym_sequence_stmt_title,
      sym_sequence_stmt_loop,
      sym_sequence_stmt_rect,
      sym_sequence_stmt_opt,
      sym_sequence_stmt_alt,
      sym_sequence_stmt_par,
  [3833] = 27,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(69), 1,
      sym__sequence_actor_word,
    ACTIONS(280), 1,
      aux_sym_sequence_stmt_actor_token1,
    ACTIONS(282), 1,
      aux_sym_sequence_stmt_autonumber_token1,
    ACTIONS(284), 1,
      aux_sym_sequence_stmt_activate_token1,
    ACTIONS(286), 1,
      aux_sym_sequence_stmt_deactivate_token1,
    ACTIONS(288), 1,
      aux_sym_sequence_stmt_note_token1,
    ACTIONS(290), 1,
      aux_sym_sequence_stmt_links_token1,
    ACTIONS(292), 1,
      aux_sym_sequence_stmt_link_token1,
    ACTIONS(294), 1,
      aux_sym_sequence_stmt_properties_token1,
    ACTIONS(296), 1,
      aux_sym_sequence_stmt_details_token1,
    ACTIONS(298), 1,
      aux_sym_sequence_stmt_title_token1,
    ACTIONS(300), 1,
      aux_sym_sequence_stmt_loop_token1,
    ACTIONS(304), 1,
      aux_sym_sequence_stmt_rect_token1,
    ACTIONS(306), 1,
      aux_sym_sequence_stmt_opt_token1,
    ACTIONS(308), 1,
      aux_sym_sequence_stmt_alt_token1,
    ACTIONS(310), 1,
      aux_sym_sequence_stmt_par_token1,
    ACTIONS(312), 1,
      sym__newline,
    ACTIONS(408), 1,
      aux_sym_sequence_stmt_loop_token2,
    STATE(45), 1,
      aux_sym__sequence_subdocument,
    STATE(178), 1,
      sym__sequence_stmt,
    STATE(257), 1,
      aux_sym_sequence_actor_repeat1,
    STATE(307), 1,
      sym_sequence_actor,
    STATE(568), 1,
      sym__sequence_participant_type,
    ACTIONS(278), 2,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
    STATE(155), 17,
      sym_sequence_stmt_participant,
      sym_sequence_stmt_actor,
      sym_sequence_stmt_signal,
      sym_sequence_stmt_autonumber,
      sym_sequence_stmt_activate,
      sym_sequence_stmt_deactivate,
      sym_sequence_stmt_note,
      sym_sequence_stmt_links,
      sym_sequence_stmt_link,
      sym_sequence_stmt_properties,
      sym_sequence_stmt_details,
      sym_sequence_stmt_title,
      sym_sequence_stmt_loop,
      sym_sequence_stmt_rect,
      sym_sequence_stmt_opt,
      sym_sequence_stmt_alt,
      sym_sequence_stmt_par,
  [3932] = 27,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(69), 1,
      sym__sequence_actor_word,
    ACTIONS(280), 1,
      aux_sym_sequence_stmt_actor_token1,
    ACTIONS(282), 1,
      aux_sym_sequence_stmt_autonumber_token1,
    ACTIONS(284), 1,
      aux_sym_sequence_stmt_activate_token1,
    ACTIONS(286), 1,
      aux_sym_sequence_stmt_deactivate_token1,
    ACTIONS(288), 1,
      aux_sym_sequence_stmt_note_token1,
    ACTIONS(290), 1,
      aux_sym_sequence_stmt_links_token1,
    ACTIONS(292), 1,
      aux_sym_sequence_stmt_link_token1,
    ACTIONS(294), 1,
      aux_sym_sequence_stmt_properties_token1,
    ACTIONS(296), 1,
      aux_sym_sequence_stmt_details_token1,
    ACTIONS(298), 1,
      aux_sym_sequence_stmt_title_token1,
    ACTIONS(300), 1,
      aux_sym_sequence_stmt_loop_token1,
    ACTIONS(304), 1,
      aux_sym_sequence_stmt_rect_token1,
    ACTIONS(306), 1,
      aux_sym_sequence_stmt_opt_token1,
    ACTIONS(308), 1,
      aux_sym_sequence_stmt_alt_token1,
    ACTIONS(310), 1,
      aux_sym_sequence_stmt_par_token1,
    ACTIONS(312), 1,
      sym__newline,
    ACTIONS(410), 1,
      aux_sym_sequence_stmt_loop_token2,
    STATE(44), 1,
      aux_sym__sequence_subdocument,
    STATE(178), 1,
      sym__sequence_stmt,
    STATE(257), 1,
      aux_sym_sequence_actor_repeat1,
    STATE(307), 1,
      sym_sequence_actor,
    STATE(568), 1,
      sym__sequence_participant_type,
    ACTIONS(278), 2,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
    STATE(155), 17,
      sym_sequence_stmt_participant,
      sym_sequence_stmt_actor,
      sym_sequence_stmt_signal,
      sym_sequence_stmt_autonumber,
      sym_sequence_stmt_activate,
      sym_sequence_stmt_deactivate,
      sym_sequence_stmt_note,
      sym_sequence_stmt_links,
      sym_sequence_stmt_link,
      sym_sequence_stmt_properties,
      sym_sequence_stmt_details,
      sym_sequence_stmt_title,
      sym_sequence_stmt_loop,
      sym_sequence_stmt_rect,
      sym_sequence_stmt_opt,
      sym_sequence_stmt_alt,
      sym_sequence_stmt_par,
  [4031] = 27,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(69), 1,
      sym__sequence_actor_word,
    ACTIONS(280), 1,
      aux_sym_sequence_stmt_actor_token1,
    ACTIONS(282), 1,
      aux_sym_sequence_stmt_autonumber_token1,
    ACTIONS(284), 1,
      aux_sym_sequence_stmt_activate_token1,
    ACTIONS(286), 1,
      aux_sym_sequence_stmt_deactivate_token1,
    ACTIONS(288), 1,
      aux_sym_sequence_stmt_note_token1,
    ACTIONS(290), 1,
      aux_sym_sequence_stmt_links_token1,
    ACTIONS(292), 1,
      aux_sym_sequence_stmt_link_token1,
    ACTIONS(294), 1,
      aux_sym_sequence_stmt_properties_token1,
    ACTIONS(296), 1,
      aux_sym_sequence_stmt_details_token1,
    ACTIONS(298), 1,
      aux_sym_sequence_stmt_title_token1,
    ACTIONS(300), 1,
      aux_sym_sequence_stmt_loop_token1,
    ACTIONS(304), 1,
      aux_sym_sequence_stmt_rect_token1,
    ACTIONS(306), 1,
      aux_sym_sequence_stmt_opt_token1,
    ACTIONS(308), 1,
      aux_sym_sequence_stmt_alt_token1,
    ACTIONS(310), 1,
      aux_sym_sequence_stmt_par_token1,
    ACTIONS(312), 1,
      sym__newline,
    ACTIONS(412), 1,
      aux_sym_sequence_stmt_loop_token2,
    STATE(46), 1,
      aux_sym__sequence_subdocument,
    STATE(178), 1,
      sym__sequence_stmt,
    STATE(257), 1,
      aux_sym_sequence_actor_repeat1,
    STATE(307), 1,
      sym_sequence_actor,
    STATE(568), 1,
      sym__sequence_participant_type,
    ACTIONS(278), 2,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
    STATE(155), 17,
      sym_sequence_stmt_participant,
      sym_sequence_stmt_actor,
      sym_sequence_stmt_signal,
      sym_sequence_stmt_autonumber,
      sym_sequence_stmt_activate,
      sym_sequence_stmt_deactivate,
      sym_sequence_stmt_note,
      sym_sequence_stmt_links,
      sym_sequence_stmt_link,
      sym_sequence_stmt_properties,
      sym_sequence_stmt_details,
      sym_sequence_stmt_title,
      sym_sequence_stmt_loop,
      sym_sequence_stmt_rect,
      sym_sequence_stmt_opt,
      sym_sequence_stmt_alt,
      sym_sequence_stmt_par,
  [4130] = 27,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(171), 1,
      aux_sym_sequence_stmt_loop_token2,
    ACTIONS(188), 1,
      sym__sequence_actor_word,
    ACTIONS(417), 1,
      aux_sym_sequence_stmt_actor_token1,
    ACTIONS(420), 1,
      aux_sym_sequence_stmt_autonumber_token1,
    ACTIONS(423), 1,
      aux_sym_sequence_stmt_activate_token1,
    ACTIONS(426), 1,
      aux_sym_sequence_stmt_deactivate_token1,
    ACTIONS(429), 1,
      aux_sym_sequence_stmt_note_token1,
    ACTIONS(432), 1,
      aux_sym_sequence_stmt_links_token1,
    ACTIONS(435), 1,
      aux_sym_sequence_stmt_link_token1,
    ACTIONS(438), 1,
      aux_sym_sequence_stmt_properties_token1,
    ACTIONS(441), 1,
      aux_sym_sequence_stmt_details_token1,
    ACTIONS(444), 1,
      aux_sym_sequence_stmt_title_token1,
    ACTIONS(447), 1,
      aux_sym_sequence_stmt_loop_token1,
    ACTIONS(450), 1,
      aux_sym_sequence_stmt_rect_token1,
    ACTIONS(453), 1,
      aux_sym_sequence_stmt_opt_token1,
    ACTIONS(456), 1,
      aux_sym_sequence_stmt_alt_token1,
    ACTIONS(459), 1,
      aux_sym_sequence_stmt_par_token1,
    ACTIONS(462), 1,
      sym__newline,
    STATE(43), 1,
      aux_sym__sequence_subdocument,
    STATE(178), 1,
      sym__sequence_stmt,
    STATE(257), 1,
      aux_sym_sequence_actor_repeat1,
    STATE(307), 1,
      sym_sequence_actor,
    STATE(568), 1,
      sym__sequence_participant_type,
    ACTIONS(414), 2,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
    STATE(155), 17,
      sym_sequence_stmt_participant,
      sym_sequence_stmt_actor,
      sym_sequence_stmt_signal,
      sym_sequence_stmt_autonumber,
      sym_sequence_stmt_activate,
      sym_sequence_stmt_deactivate,
      sym_sequence_stmt_note,
      sym_sequence_stmt_links,
      sym_sequence_stmt_link,
      sym_sequence_stmt_properties,
      sym_sequence_stmt_details,
      sym_sequence_stmt_title,
      sym_sequence_stmt_loop,
      sym_sequence_stmt_rect,
      sym_sequence_stmt_opt,
      sym_sequence_stmt_alt,
      sym_sequence_stmt_par,
  [4229] = 27,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(69), 1,
      sym__sequence_actor_word,
    ACTIONS(280), 1,
      aux_sym_sequence_stmt_actor_token1,
    ACTIONS(282), 1,
      aux_sym_sequence_stmt_autonumber_token1,
    ACTIONS(284), 1,
      aux_sym_sequence_stmt_activate_token1,
    ACTIONS(286), 1,
      aux_sym_sequence_stmt_deactivate_token1,
    ACTIONS(288), 1,
      aux_sym_sequence_stmt_note_token1,
    ACTIONS(290), 1,
      aux_sym_sequence_stmt_links_token1,
    ACTIONS(292), 1,
      aux_sym_sequence_stmt_link_token1,
    ACTIONS(294), 1,
      aux_sym_sequence_stmt_properties_token1,
    ACTIONS(296), 1,
      aux_sym_sequence_stmt_details_token1,
    ACTIONS(298), 1,
      aux_sym_sequence_stmt_title_token1,
    ACTIONS(300), 1,
      aux_sym_sequence_stmt_loop_token1,
    ACTIONS(304), 1,
      aux_sym_sequence_stmt_rect_token1,
    ACTIONS(306), 1,
      aux_sym_sequence_stmt_opt_token1,
    ACTIONS(308), 1,
      aux_sym_sequence_stmt_alt_token1,
    ACTIONS(310), 1,
      aux_sym_sequence_stmt_par_token1,
    ACTIONS(312), 1,
      sym__newline,
    ACTIONS(465), 1,
      aux_sym_sequence_stmt_loop_token2,
    STATE(43), 1,
      aux_sym__sequence_subdocument,
    STATE(178), 1,
      sym__sequence_stmt,
    STATE(257), 1,
      aux_sym_sequence_actor_repeat1,
    STATE(307), 1,
      sym_sequence_actor,
    STATE(568), 1,
      sym__sequence_participant_type,
    ACTIONS(278), 2,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
    STATE(155), 17,
      sym_sequence_stmt_participant,
      sym_sequence_stmt_actor,
      sym_sequence_stmt_signal,
      sym_sequence_stmt_autonumber,
      sym_sequence_stmt_activate,
      sym_sequence_stmt_deactivate,
      sym_sequence_stmt_note,
      sym_sequence_stmt_links,
      sym_sequence_stmt_link,
      sym_sequence_stmt_properties,
      sym_sequence_stmt_details,
      sym_sequence_stmt_title,
      sym_sequence_stmt_loop,
      sym_sequence_stmt_rect,
      sym_sequence_stmt_opt,
      sym_sequence_stmt_alt,
      sym_sequence_stmt_par,
  [4328] = 27,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(69), 1,
      sym__sequence_actor_word,
    ACTIONS(280), 1,
      aux_sym_sequence_stmt_actor_token1,
    ACTIONS(282), 1,
      aux_sym_sequence_stmt_autonumber_token1,
    ACTIONS(284), 1,
      aux_sym_sequence_stmt_activate_token1,
    ACTIONS(286), 1,
      aux_sym_sequence_stmt_deactivate_token1,
    ACTIONS(288), 1,
      aux_sym_sequence_stmt_note_token1,
    ACTIONS(290), 1,
      aux_sym_sequence_stmt_links_token1,
    ACTIONS(292), 1,
      aux_sym_sequence_stmt_link_token1,
    ACTIONS(294), 1,
      aux_sym_sequence_stmt_properties_token1,
    ACTIONS(296), 1,
      aux_sym_sequence_stmt_details_token1,
    ACTIONS(298), 1,
      aux_sym_sequence_stmt_title_token1,
    ACTIONS(300), 1,
      aux_sym_sequence_stmt_loop_token1,
    ACTIONS(304), 1,
      aux_sym_sequence_stmt_rect_token1,
    ACTIONS(306), 1,
      aux_sym_sequence_stmt_opt_token1,
    ACTIONS(308), 1,
      aux_sym_sequence_stmt_alt_token1,
    ACTIONS(310), 1,
      aux_sym_sequence_stmt_par_token1,
    ACTIONS(312), 1,
      sym__newline,
    ACTIONS(467), 1,
      aux_sym_sequence_stmt_loop_token2,
    STATE(43), 1,
      aux_sym__sequence_subdocument,
    STATE(178), 1,
      sym__sequence_stmt,
    STATE(257), 1,
      aux_sym_sequence_actor_repeat1,
    STATE(307), 1,
      sym_sequence_actor,
    STATE(568), 1,
      sym__sequence_participant_type,
    ACTIONS(278), 2,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
    STATE(155), 17,
      sym_sequence_stmt_participant,
      sym_sequence_stmt_actor,
      sym_sequence_stmt_signal,
      sym_sequence_stmt_autonumber,
      sym_sequence_stmt_activate,
      sym_sequence_stmt_deactivate,
      sym_sequence_stmt_note,
      sym_sequence_stmt_links,
      sym_sequence_stmt_link,
      sym_sequence_stmt_properties,
      sym_sequence_stmt_details,
      sym_sequence_stmt_title,
      sym_sequence_stmt_loop,
      sym_sequence_stmt_rect,
      sym_sequence_stmt_opt,
      sym_sequence_stmt_alt,
      sym_sequence_stmt_par,
  [4427] = 27,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(69), 1,
      sym__sequence_actor_word,
    ACTIONS(280), 1,
      aux_sym_sequence_stmt_actor_token1,
    ACTIONS(282), 1,
      aux_sym_sequence_stmt_autonumber_token1,
    ACTIONS(284), 1,
      aux_sym_sequence_stmt_activate_token1,
    ACTIONS(286), 1,
      aux_sym_sequence_stmt_deactivate_token1,
    ACTIONS(288), 1,
      aux_sym_sequence_stmt_note_token1,
    ACTIONS(290), 1,
      aux_sym_sequence_stmt_links_token1,
    ACTIONS(292), 1,
      aux_sym_sequence_stmt_link_token1,
    ACTIONS(294), 1,
      aux_sym_sequence_stmt_properties_token1,
    ACTIONS(296), 1,
      aux_sym_sequence_stmt_details_token1,
    ACTIONS(298), 1,
      aux_sym_sequence_stmt_title_token1,
    ACTIONS(300), 1,
      aux_sym_sequence_stmt_loop_token1,
    ACTIONS(304), 1,
      aux_sym_sequence_stmt_rect_token1,
    ACTIONS(306), 1,
      aux_sym_sequence_stmt_opt_token1,
    ACTIONS(308), 1,
      aux_sym_sequence_stmt_alt_token1,
    ACTIONS(310), 1,
      aux_sym_sequence_stmt_par_token1,
    ACTIONS(312), 1,
      sym__newline,
    ACTIONS(469), 1,
      aux_sym_sequence_stmt_loop_token2,
    STATE(43), 1,
      aux_sym__sequence_subdocument,
    STATE(178), 1,
      sym__sequence_stmt,
    STATE(257), 1,
      aux_sym_sequence_actor_repeat1,
    STATE(307), 1,
      sym_sequence_actor,
    STATE(568), 1,
      sym__sequence_participant_type,
    ACTIONS(278), 2,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
    STATE(155), 17,
      sym_sequence_stmt_participant,
      sym_sequence_stmt_actor,
      sym_sequence_stmt_signal,
      sym_sequence_stmt_autonumber,
      sym_sequence_stmt_activate,
      sym_sequence_stmt_deactivate,
      sym_sequence_stmt_note,
      sym_sequence_stmt_links,
      sym_sequence_stmt_link,
      sym_sequence_stmt_properties,
      sym_sequence_stmt_details,
      sym_sequence_stmt_title,
      sym_sequence_stmt_loop,
      sym_sequence_stmt_rect,
      sym_sequence_stmt_opt,
      sym_sequence_stmt_alt,
      sym_sequence_stmt_par,
  [4526] = 26,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(33), 1,
      aux_sym_sequence_stmt_actor_token1,
    ACTIONS(35), 1,
      aux_sym_sequence_stmt_autonumber_token1,
    ACTIONS(37), 1,
      aux_sym_sequence_stmt_activate_token1,
    ACTIONS(39), 1,
      aux_sym_sequence_stmt_deactivate_token1,
    ACTIONS(41), 1,
      aux_sym_sequence_stmt_note_token1,
    ACTIONS(43), 1,
      aux_sym_sequence_stmt_links_token1,
    ACTIONS(45), 1,
      aux_sym_sequence_stmt_link_token1,
    ACTIONS(47), 1,
      aux_sym_sequence_stmt_properties_token1,
    ACTIONS(49), 1,
      aux_sym_sequence_stmt_details_token1,
    ACTIONS(51), 1,
      aux_sym_sequence_stmt_title_token1,
    ACTIONS(53), 1,
      aux_sym_sequence_stmt_loop_token1,
    ACTIONS(57), 1,
      aux_sym_sequence_stmt_rect_token1,
    ACTIONS(59), 1,
      aux_sym_sequence_stmt_opt_token1,
    ACTIONS(61), 1,
      aux_sym_sequence_stmt_alt_token1,
    ACTIONS(65), 1,
      aux_sym_sequence_stmt_par_token1,
    ACTIONS(67), 1,
      sym__newline,
    ACTIONS(69), 1,
      sym__sequence_actor_word,
    STATE(2), 1,
      aux_sym__sequence_subdocument,
    STATE(129), 1,
      sym__sequence_stmt,
    STATE(257), 1,
      aux_sym_sequence_actor_repeat1,
    STATE(313), 1,
      sym_sequence_actor,
    STATE(551), 1,
      sym__sequence_participant_type,
    ACTIONS(31), 2,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
    STATE(96), 17,
      sym_sequence_stmt_participant,
      sym_sequence_stmt_actor,
      sym_sequence_stmt_signal,
      sym_sequence_stmt_autonumber,
      sym_sequence_stmt_activate,
      sym_sequence_stmt_deactivate,
      sym_sequence_stmt_note,
      sym_sequence_stmt_links,
      sym_sequence_stmt_link,
      sym_sequence_stmt_properties,
      sym_sequence_stmt_details,
      sym_sequence_stmt_title,
      sym_sequence_stmt_loop,
      sym_sequence_stmt_rect,
      sym_sequence_stmt_opt,
      sym_sequence_stmt_alt,
      sym_sequence_stmt_par,
  [4622] = 26,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(69), 1,
      sym__sequence_actor_word,
    ACTIONS(73), 1,
      aux_sym_sequence_stmt_actor_token1,
    ACTIONS(75), 1,
      aux_sym_sequence_stmt_autonumber_token1,
    ACTIONS(77), 1,
      aux_sym_sequence_stmt_activate_token1,
    ACTIONS(79), 1,
      aux_sym_sequence_stmt_deactivate_token1,
    ACTIONS(81), 1,
      aux_sym_sequence_stmt_note_token1,
    ACTIONS(83), 1,
      aux_sym_sequence_stmt_links_token1,
    ACTIONS(85), 1,
      aux_sym_sequence_stmt_link_token1,
    ACTIONS(87), 1,
      aux_sym_sequence_stmt_properties_token1,
    ACTIONS(89), 1,
      aux_sym_sequence_stmt_details_token1,
    ACTIONS(91), 1,
      aux_sym_sequence_stmt_title_token1,
    ACTIONS(93), 1,
      aux_sym_sequence_stmt_loop_token1,
    ACTIONS(97), 1,
      aux_sym_sequence_stmt_rect_token1,
    ACTIONS(99), 1,
      aux_sym_sequence_stmt_opt_token1,
    ACTIONS(101), 1,
      aux_sym_sequence_stmt_alt_token1,
    ACTIONS(103), 1,
      aux_sym_sequence_stmt_par_token1,
    ACTIONS(107), 1,
      sym__newline,
    STATE(4), 1,
      aux_sym__sequence_subdocument,
    STATE(115), 1,
      sym__sequence_stmt,
    STATE(257), 1,
      aux_sym_sequence_actor_repeat1,
    STATE(301), 1,
      sym_sequence_actor,
    STATE(586), 1,
      sym__sequence_participant_type,
    ACTIONS(71), 2,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
    STATE(109), 17,
      sym_sequence_stmt_participant,
      sym_sequence_stmt_actor,
      sym_sequence_stmt_signal,
      sym_sequence_stmt_autonumber,
      sym_sequence_stmt_activate,
      sym_sequence_stmt_deactivate,
      sym_sequence_stmt_note,
      sym_sequence_stmt_links,
      sym_sequence_stmt_link,
      sym_sequence_stmt_properties,
      sym_sequence_stmt_details,
      sym_sequence_stmt_title,
      sym_sequence_stmt_loop,
      sym_sequence_stmt_rect,
      sym_sequence_stmt_opt,
      sym_sequence_stmt_alt,
      sym_sequence_stmt_par,
  [4718] = 26,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(33), 1,
      aux_sym_sequence_stmt_actor_token1,
    ACTIONS(35), 1,
      aux_sym_sequence_stmt_autonumber_token1,
    ACTIONS(37), 1,
      aux_sym_sequence_stmt_activate_token1,
    ACTIONS(39), 1,
      aux_sym_sequence_stmt_deactivate_token1,
    ACTIONS(41), 1,
      aux_sym_sequence_stmt_note_token1,
    ACTIONS(43), 1,
      aux_sym_sequence_stmt_links_token1,
    ACTIONS(45), 1,
      aux_sym_sequence_stmt_link_token1,
    ACTIONS(47), 1,
      aux_sym_sequence_stmt_properties_token1,
    ACTIONS(49), 1,
      aux_sym_sequence_stmt_details_token1,
    ACTIONS(51), 1,
      aux_sym_sequence_stmt_title_token1,
    ACTIONS(53), 1,
      aux_sym_sequence_stmt_loop_token1,
    ACTIONS(57), 1,
      aux_sym_sequence_stmt_rect_token1,
    ACTIONS(59), 1,
      aux_sym_sequence_stmt_opt_token1,
    ACTIONS(61), 1,
      aux_sym_sequence_stmt_alt_token1,
    ACTIONS(65), 1,
      aux_sym_sequence_stmt_par_token1,
    ACTIONS(67), 1,
      sym__newline,
    ACTIONS(69), 1,
      sym__sequence_actor_word,
    STATE(16), 1,
      aux_sym__sequence_subdocument,
    STATE(129), 1,
      sym__sequence_stmt,
    STATE(257), 1,
      aux_sym_sequence_actor_repeat1,
    STATE(313), 1,
      sym_sequence_actor,
    STATE(551), 1,
      sym__sequence_participant_type,
    ACTIONS(31), 2,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
    STATE(96), 17,
      sym_sequence_stmt_participant,
      sym_sequence_stmt_actor,
      sym_sequence_stmt_signal,
      sym_sequence_stmt_autonumber,
      sym_sequence_stmt_activate,
      sym_sequence_stmt_deactivate,
      sym_sequence_stmt_note,
      sym_sequence_stmt_links,
      sym_sequence_stmt_link,
      sym_sequence_stmt_properties,
      sym_sequence_stmt_details,
      sym_sequence_stmt_title,
      sym_sequence_stmt_loop,
      sym_sequence_stmt_rect,
      sym_sequence_stmt_opt,
      sym_sequence_stmt_alt,
      sym_sequence_stmt_par,
  [4814] = 26,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(69), 1,
      sym__sequence_actor_word,
    ACTIONS(73), 1,
      aux_sym_sequence_stmt_actor_token1,
    ACTIONS(75), 1,
      aux_sym_sequence_stmt_autonumber_token1,
    ACTIONS(77), 1,
      aux_sym_sequence_stmt_activate_token1,
    ACTIONS(79), 1,
      aux_sym_sequence_stmt_deactivate_token1,
    ACTIONS(81), 1,
      aux_sym_sequence_stmt_note_token1,
    ACTIONS(83), 1,
      aux_sym_sequence_stmt_links_token1,
    ACTIONS(85), 1,
      aux_sym_sequence_stmt_link_token1,
    ACTIONS(87), 1,
      aux_sym_sequence_stmt_properties_token1,
    ACTIONS(89), 1,
      aux_sym_sequence_stmt_details_token1,
    ACTIONS(91), 1,
      aux_sym_sequence_stmt_title_token1,
    ACTIONS(93), 1,
      aux_sym_sequence_stmt_loop_token1,
    ACTIONS(97), 1,
      aux_sym_sequence_stmt_rect_token1,
    ACTIONS(99), 1,
      aux_sym_sequence_stmt_opt_token1,
    ACTIONS(101), 1,
      aux_sym_sequence_stmt_alt_token1,
    ACTIONS(103), 1,
      aux_sym_sequence_stmt_par_token1,
    ACTIONS(107), 1,
      sym__newline,
    STATE(15), 1,
      aux_sym__sequence_subdocument,
    STATE(115), 1,
      sym__sequence_stmt,
    STATE(257), 1,
      aux_sym_sequence_actor_repeat1,
    STATE(301), 1,
      sym_sequence_actor,
    STATE(586), 1,
      sym__sequence_participant_type,
    ACTIONS(71), 2,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
    STATE(109), 17,
      sym_sequence_stmt_participant,
      sym_sequence_stmt_actor,
      sym_sequence_stmt_signal,
      sym_sequence_stmt_autonumber,
      sym_sequence_stmt_activate,
      sym_sequence_stmt_deactivate,
      sym_sequence_stmt_note,
      sym_sequence_stmt_links,
      sym_sequence_stmt_link,
      sym_sequence_stmt_properties,
      sym_sequence_stmt_details,
      sym_sequence_stmt_title,
      sym_sequence_stmt_loop,
      sym_sequence_stmt_rect,
      sym_sequence_stmt_opt,
      sym_sequence_stmt_alt,
      sym_sequence_stmt_par,
  [4910] = 26,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(69), 1,
      sym__sequence_actor_word,
    ACTIONS(73), 1,
      aux_sym_sequence_stmt_actor_token1,
    ACTIONS(75), 1,
      aux_sym_sequence_stmt_autonumber_token1,
    ACTIONS(77), 1,
      aux_sym_sequence_stmt_activate_token1,
    ACTIONS(79), 1,
      aux_sym_sequence_stmt_deactivate_token1,
    ACTIONS(81), 1,
      aux_sym_sequence_stmt_note_token1,
    ACTIONS(83), 1,
      aux_sym_sequence_stmt_links_token1,
    ACTIONS(85), 1,
      aux_sym_sequence_stmt_link_token1,
    ACTIONS(87), 1,
      aux_sym_sequence_stmt_properties_token1,
    ACTIONS(89), 1,
      aux_sym_sequence_stmt_details_token1,
    ACTIONS(91), 1,
      aux_sym_sequence_stmt_title_token1,
    ACTIONS(93), 1,
      aux_sym_sequence_stmt_loop_token1,
    ACTIONS(97), 1,
      aux_sym_sequence_stmt_rect_token1,
    ACTIONS(99), 1,
      aux_sym_sequence_stmt_opt_token1,
    ACTIONS(101), 1,
      aux_sym_sequence_stmt_alt_token1,
    ACTIONS(103), 1,
      aux_sym_sequence_stmt_par_token1,
    ACTIONS(107), 1,
      sym__newline,
    STATE(10), 1,
      aux_sym__sequence_subdocument,
    STATE(115), 1,
      sym__sequence_stmt,
    STATE(257), 1,
      aux_sym_sequence_actor_repeat1,
    STATE(301), 1,
      sym_sequence_actor,
    STATE(586), 1,
      sym__sequence_participant_type,
    ACTIONS(71), 2,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
    STATE(109), 17,
      sym_sequence_stmt_participant,
      sym_sequence_stmt_actor,
      sym_sequence_stmt_signal,
      sym_sequence_stmt_autonumber,
      sym_sequence_stmt_activate,
      sym_sequence_stmt_deactivate,
      sym_sequence_stmt_note,
      sym_sequence_stmt_links,
      sym_sequence_stmt_link,
      sym_sequence_stmt_properties,
      sym_sequence_stmt_details,
      sym_sequence_stmt_title,
      sym_sequence_stmt_loop,
      sym_sequence_stmt_rect,
      sym_sequence_stmt_opt,
      sym_sequence_stmt_alt,
      sym_sequence_stmt_par,
  [5006] = 26,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(69), 1,
      sym__sequence_actor_word,
    ACTIONS(73), 1,
      aux_sym_sequence_stmt_actor_token1,
    ACTIONS(75), 1,
      aux_sym_sequence_stmt_autonumber_token1,
    ACTIONS(77), 1,
      aux_sym_sequence_stmt_activate_token1,
    ACTIONS(79), 1,
      aux_sym_sequence_stmt_deactivate_token1,
    ACTIONS(81), 1,
      aux_sym_sequence_stmt_note_token1,
    ACTIONS(83), 1,
      aux_sym_sequence_stmt_links_token1,
    ACTIONS(85), 1,
      aux_sym_sequence_stmt_link_token1,
    ACTIONS(87), 1,
      aux_sym_sequence_stmt_properties_token1,
    ACTIONS(89), 1,
      aux_sym_sequence_stmt_details_token1,
    ACTIONS(91), 1,
      aux_sym_sequence_stmt_title_token1,
    ACTIONS(93), 1,
      aux_sym_sequence_stmt_loop_token1,
    ACTIONS(97), 1,
      aux_sym_sequence_stmt_rect_token1,
    ACTIONS(99), 1,
      aux_sym_sequence_stmt_opt_token1,
    ACTIONS(101), 1,
      aux_sym_sequence_stmt_alt_token1,
    ACTIONS(103), 1,
      aux_sym_sequence_stmt_par_token1,
    ACTIONS(107), 1,
      sym__newline,
    STATE(13), 1,
      aux_sym__sequence_subdocument,
    STATE(115), 1,
      sym__sequence_stmt,
    STATE(257), 1,
      aux_sym_sequence_actor_repeat1,
    STATE(301), 1,
      sym_sequence_actor,
    STATE(586), 1,
      sym__sequence_participant_type,
    ACTIONS(71), 2,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
    STATE(109), 17,
      sym_sequence_stmt_participant,
      sym_sequence_stmt_actor,
      sym_sequence_stmt_signal,
      sym_sequence_stmt_autonumber,
      sym_sequence_stmt_activate,
      sym_sequence_stmt_deactivate,
      sym_sequence_stmt_note,
      sym_sequence_stmt_links,
      sym_sequence_stmt_link,
      sym_sequence_stmt_properties,
      sym_sequence_stmt_details,
      sym_sequence_stmt_title,
      sym_sequence_stmt_loop,
      sym_sequence_stmt_rect,
      sym_sequence_stmt_opt,
      sym_sequence_stmt_alt,
      sym_sequence_stmt_par,
  [5102] = 26,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(69), 1,
      sym__sequence_actor_word,
    ACTIONS(73), 1,
      aux_sym_sequence_stmt_actor_token1,
    ACTIONS(75), 1,
      aux_sym_sequence_stmt_autonumber_token1,
    ACTIONS(77), 1,
      aux_sym_sequence_stmt_activate_token1,
    ACTIONS(79), 1,
      aux_sym_sequence_stmt_deactivate_token1,
    ACTIONS(81), 1,
      aux_sym_sequence_stmt_note_token1,
    ACTIONS(83), 1,
      aux_sym_sequence_stmt_links_token1,
    ACTIONS(85), 1,
      aux_sym_sequence_stmt_link_token1,
    ACTIONS(87), 1,
      aux_sym_sequence_stmt_properties_token1,
    ACTIONS(89), 1,
      aux_sym_sequence_stmt_details_token1,
    ACTIONS(91), 1,
      aux_sym_sequence_stmt_title_token1,
    ACTIONS(93), 1,
      aux_sym_sequence_stmt_loop_token1,
    ACTIONS(97), 1,
      aux_sym_sequence_stmt_rect_token1,
    ACTIONS(99), 1,
      aux_sym_sequence_stmt_opt_token1,
    ACTIONS(101), 1,
      aux_sym_sequence_stmt_alt_token1,
    ACTIONS(103), 1,
      aux_sym_sequence_stmt_par_token1,
    ACTIONS(107), 1,
      sym__newline,
    STATE(11), 1,
      aux_sym__sequence_subdocument,
    STATE(115), 1,
      sym__sequence_stmt,
    STATE(257), 1,
      aux_sym_sequence_actor_repeat1,
    STATE(301), 1,
      sym_sequence_actor,
    STATE(586), 1,
      sym__sequence_participant_type,
    ACTIONS(71), 2,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
    STATE(109), 17,
      sym_sequence_stmt_participant,
      sym_sequence_stmt_actor,
      sym_sequence_stmt_signal,
      sym_sequence_stmt_autonumber,
      sym_sequence_stmt_activate,
      sym_sequence_stmt_deactivate,
      sym_sequence_stmt_note,
      sym_sequence_stmt_links,
      sym_sequence_stmt_link,
      sym_sequence_stmt_properties,
      sym_sequence_stmt_details,
      sym_sequence_stmt_title,
      sym_sequence_stmt_loop,
      sym_sequence_stmt_rect,
      sym_sequence_stmt_opt,
      sym_sequence_stmt_alt,
      sym_sequence_stmt_par,
  [5198] = 26,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(69), 1,
      sym__sequence_actor_word,
    ACTIONS(73), 1,
      aux_sym_sequence_stmt_actor_token1,
    ACTIONS(75), 1,
      aux_sym_sequence_stmt_autonumber_token1,
    ACTIONS(77), 1,
      aux_sym_sequence_stmt_activate_token1,
    ACTIONS(79), 1,
      aux_sym_sequence_stmt_deactivate_token1,
    ACTIONS(81), 1,
      aux_sym_sequence_stmt_note_token1,
    ACTIONS(83), 1,
      aux_sym_sequence_stmt_links_token1,
    ACTIONS(85), 1,
      aux_sym_sequence_stmt_link_token1,
    ACTIONS(87), 1,
      aux_sym_sequence_stmt_properties_token1,
    ACTIONS(89), 1,
      aux_sym_sequence_stmt_details_token1,
    ACTIONS(91), 1,
      aux_sym_sequence_stmt_title_token1,
    ACTIONS(93), 1,
      aux_sym_sequence_stmt_loop_token1,
    ACTIONS(97), 1,
      aux_sym_sequence_stmt_rect_token1,
    ACTIONS(99), 1,
      aux_sym_sequence_stmt_opt_token1,
    ACTIONS(101), 1,
      aux_sym_sequence_stmt_alt_token1,
    ACTIONS(103), 1,
      aux_sym_sequence_stmt_par_token1,
    ACTIONS(107), 1,
      sym__newline,
    STATE(3), 1,
      aux_sym__sequence_subdocument,
    STATE(115), 1,
      sym__sequence_stmt,
    STATE(257), 1,
      aux_sym_sequence_actor_repeat1,
    STATE(301), 1,
      sym_sequence_actor,
    STATE(586), 1,
      sym__sequence_participant_type,
    ACTIONS(71), 2,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
    STATE(109), 17,
      sym_sequence_stmt_participant,
      sym_sequence_stmt_actor,
      sym_sequence_stmt_signal,
      sym_sequence_stmt_autonumber,
      sym_sequence_stmt_activate,
      sym_sequence_stmt_deactivate,
      sym_sequence_stmt_note,
      sym_sequence_stmt_links,
      sym_sequence_stmt_link,
      sym_sequence_stmt_properties,
      sym_sequence_stmt_details,
      sym_sequence_stmt_title,
      sym_sequence_stmt_loop,
      sym_sequence_stmt_rect,
      sym_sequence_stmt_opt,
      sym_sequence_stmt_alt,
      sym_sequence_stmt_par,
  [5294] = 26,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(69), 1,
      sym__sequence_actor_word,
    ACTIONS(73), 1,
      aux_sym_sequence_stmt_actor_token1,
    ACTIONS(75), 1,
      aux_sym_sequence_stmt_autonumber_token1,
    ACTIONS(77), 1,
      aux_sym_sequence_stmt_activate_token1,
    ACTIONS(79), 1,
      aux_sym_sequence_stmt_deactivate_token1,
    ACTIONS(81), 1,
      aux_sym_sequence_stmt_note_token1,
    ACTIONS(83), 1,
      aux_sym_sequence_stmt_links_token1,
    ACTIONS(85), 1,
      aux_sym_sequence_stmt_link_token1,
    ACTIONS(87), 1,
      aux_sym_sequence_stmt_properties_token1,
    ACTIONS(89), 1,
      aux_sym_sequence_stmt_details_token1,
    ACTIONS(91), 1,
      aux_sym_sequence_stmt_title_token1,
    ACTIONS(93), 1,
      aux_sym_sequence_stmt_loop_token1,
    ACTIONS(97), 1,
      aux_sym_sequence_stmt_rect_token1,
    ACTIONS(99), 1,
      aux_sym_sequence_stmt_opt_token1,
    ACTIONS(101), 1,
      aux_sym_sequence_stmt_alt_token1,
    ACTIONS(103), 1,
      aux_sym_sequence_stmt_par_token1,
    ACTIONS(107), 1,
      sym__newline,
    STATE(14), 1,
      aux_sym__sequence_subdocument,
    STATE(115), 1,
      sym__sequence_stmt,
    STATE(257), 1,
      aux_sym_sequence_actor_repeat1,
    STATE(301), 1,
      sym_sequence_actor,
    STATE(586), 1,
      sym__sequence_participant_type,
    ACTIONS(71), 2,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
    STATE(109), 17,
      sym_sequence_stmt_participant,
      sym_sequence_stmt_actor,
      sym_sequence_stmt_signal,
      sym_sequence_stmt_autonumber,
      sym_sequence_stmt_activate,
      sym_sequence_stmt_deactivate,
      sym_sequence_stmt_note,
      sym_sequence_stmt_links,
      sym_sequence_stmt_link,
      sym_sequence_stmt_properties,
      sym_sequence_stmt_details,
      sym_sequence_stmt_title,
      sym_sequence_stmt_loop,
      sym_sequence_stmt_rect,
      sym_sequence_stmt_opt,
      sym_sequence_stmt_alt,
      sym_sequence_stmt_par,
  [5390] = 26,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(33), 1,
      aux_sym_sequence_stmt_actor_token1,
    ACTIONS(35), 1,
      aux_sym_sequence_stmt_autonumber_token1,
    ACTIONS(37), 1,
      aux_sym_sequence_stmt_activate_token1,
    ACTIONS(39), 1,
      aux_sym_sequence_stmt_deactivate_token1,
    ACTIONS(41), 1,
      aux_sym_sequence_stmt_note_token1,
    ACTIONS(43), 1,
      aux_sym_sequence_stmt_links_token1,
    ACTIONS(45), 1,
      aux_sym_sequence_stmt_link_token1,
    ACTIONS(47), 1,
      aux_sym_sequence_stmt_properties_token1,
    ACTIONS(49), 1,
      aux_sym_sequence_stmt_details_token1,
    ACTIONS(51), 1,
      aux_sym_sequence_stmt_title_token1,
    ACTIONS(53), 1,
      aux_sym_sequence_stmt_loop_token1,
    ACTIONS(57), 1,
      aux_sym_sequence_stmt_rect_token1,
    ACTIONS(59), 1,
      aux_sym_sequence_stmt_opt_token1,
    ACTIONS(61), 1,
      aux_sym_sequence_stmt_alt_token1,
    ACTIONS(65), 1,
      aux_sym_sequence_stmt_par_token1,
    ACTIONS(67), 1,
      sym__newline,
    ACTIONS(69), 1,
      sym__sequence_actor_word,
    STATE(12), 1,
      aux_sym__sequence_subdocument,
    STATE(129), 1,
      sym__sequence_stmt,
    STATE(257), 1,
      aux_sym_sequence_actor_repeat1,
    STATE(313), 1,
      sym_sequence_actor,
    STATE(551), 1,
      sym__sequence_participant_type,
    ACTIONS(31), 2,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
    STATE(96), 17,
      sym_sequence_stmt_participant,
      sym_sequence_stmt_actor,
      sym_sequence_stmt_signal,
      sym_sequence_stmt_autonumber,
      sym_sequence_stmt_activate,
      sym_sequence_stmt_deactivate,
      sym_sequence_stmt_note,
      sym_sequence_stmt_links,
      sym_sequence_stmt_link,
      sym_sequence_stmt_properties,
      sym_sequence_stmt_details,
      sym_sequence_stmt_title,
      sym_sequence_stmt_loop,
      sym_sequence_stmt_rect,
      sym_sequence_stmt_opt,
      sym_sequence_stmt_alt,
      sym_sequence_stmt_par,
  [5486] = 26,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(69), 1,
      sym__sequence_actor_word,
    ACTIONS(73), 1,
      aux_sym_sequence_stmt_actor_token1,
    ACTIONS(75), 1,
      aux_sym_sequence_stmt_autonumber_token1,
    ACTIONS(77), 1,
      aux_sym_sequence_stmt_activate_token1,
    ACTIONS(79), 1,
      aux_sym_sequence_stmt_deactivate_token1,
    ACTIONS(81), 1,
      aux_sym_sequence_stmt_note_token1,
    ACTIONS(83), 1,
      aux_sym_sequence_stmt_links_token1,
    ACTIONS(85), 1,
      aux_sym_sequence_stmt_link_token1,
    ACTIONS(87), 1,
      aux_sym_sequence_stmt_properties_token1,
    ACTIONS(89), 1,
      aux_sym_sequence_stmt_details_token1,
    ACTIONS(91), 1,
      aux_sym_sequence_stmt_title_token1,
    ACTIONS(93), 1,
      aux_sym_sequence_stmt_loop_token1,
    ACTIONS(97), 1,
      aux_sym_sequence_stmt_rect_token1,
    ACTIONS(99), 1,
      aux_sym_sequence_stmt_opt_token1,
    ACTIONS(101), 1,
      aux_sym_sequence_stmt_alt_token1,
    ACTIONS(103), 1,
      aux_sym_sequence_stmt_par_token1,
    ACTIONS(107), 1,
      sym__newline,
    STATE(6), 1,
      aux_sym__sequence_subdocument,
    STATE(115), 1,
      sym__sequence_stmt,
    STATE(257), 1,
      aux_sym_sequence_actor_repeat1,
    STATE(301), 1,
      sym_sequence_actor,
    STATE(586), 1,
      sym__sequence_participant_type,
    ACTIONS(71), 2,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
    STATE(109), 17,
      sym_sequence_stmt_participant,
      sym_sequence_stmt_actor,
      sym_sequence_stmt_signal,
      sym_sequence_stmt_autonumber,
      sym_sequence_stmt_activate,
      sym_sequence_stmt_deactivate,
      sym_sequence_stmt_note,
      sym_sequence_stmt_links,
      sym_sequence_stmt_link,
      sym_sequence_stmt_properties,
      sym_sequence_stmt_details,
      sym_sequence_stmt_title,
      sym_sequence_stmt_loop,
      sym_sequence_stmt_rect,
      sym_sequence_stmt_opt,
      sym_sequence_stmt_alt,
      sym_sequence_stmt_par,
  [5582] = 26,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(69), 1,
      sym__sequence_actor_word,
    ACTIONS(73), 1,
      aux_sym_sequence_stmt_actor_token1,
    ACTIONS(75), 1,
      aux_sym_sequence_stmt_autonumber_token1,
    ACTIONS(77), 1,
      aux_sym_sequence_stmt_activate_token1,
    ACTIONS(79), 1,
      aux_sym_sequence_stmt_deactivate_token1,
    ACTIONS(81), 1,
      aux_sym_sequence_stmt_note_token1,
    ACTIONS(83), 1,
      aux_sym_sequence_stmt_links_token1,
    ACTIONS(85), 1,
      aux_sym_sequence_stmt_link_token1,
    ACTIONS(87), 1,
      aux_sym_sequence_stmt_properties_token1,
    ACTIONS(89), 1,
      aux_sym_sequence_stmt_details_token1,
    ACTIONS(91), 1,
      aux_sym_sequence_stmt_title_token1,
    ACTIONS(93), 1,
      aux_sym_sequence_stmt_loop_token1,
    ACTIONS(97), 1,
      aux_sym_sequence_stmt_rect_token1,
    ACTIONS(99), 1,
      aux_sym_sequence_stmt_opt_token1,
    ACTIONS(101), 1,
      aux_sym_sequence_stmt_alt_token1,
    ACTIONS(103), 1,
      aux_sym_sequence_stmt_par_token1,
    ACTIONS(107), 1,
      sym__newline,
    STATE(9), 1,
      aux_sym__sequence_subdocument,
    STATE(115), 1,
      sym__sequence_stmt,
    STATE(257), 1,
      aux_sym_sequence_actor_repeat1,
    STATE(301), 1,
      sym_sequence_actor,
    STATE(586), 1,
      sym__sequence_participant_type,
    ACTIONS(71), 2,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
    STATE(109), 17,
      sym_sequence_stmt_participant,
      sym_sequence_stmt_actor,
      sym_sequence_stmt_signal,
      sym_sequence_stmt_autonumber,
      sym_sequence_stmt_activate,
      sym_sequence_stmt_deactivate,
      sym_sequence_stmt_note,
      sym_sequence_stmt_links,
      sym_sequence_stmt_link,
      sym_sequence_stmt_properties,
      sym_sequence_stmt_details,
      sym_sequence_stmt_title,
      sym_sequence_stmt_loop,
      sym_sequence_stmt_rect,
      sym_sequence_stmt_opt,
      sym_sequence_stmt_alt,
      sym_sequence_stmt_par,
  [5678] = 26,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(33), 1,
      aux_sym_sequence_stmt_actor_token1,
    ACTIONS(35), 1,
      aux_sym_sequence_stmt_autonumber_token1,
    ACTIONS(37), 1,
      aux_sym_sequence_stmt_activate_token1,
    ACTIONS(39), 1,
      aux_sym_sequence_stmt_deactivate_token1,
    ACTIONS(41), 1,
      aux_sym_sequence_stmt_note_token1,
    ACTIONS(43), 1,
      aux_sym_sequence_stmt_links_token1,
    ACTIONS(45), 1,
      aux_sym_sequence_stmt_link_token1,
    ACTIONS(47), 1,
      aux_sym_sequence_stmt_properties_token1,
    ACTIONS(49), 1,
      aux_sym_sequence_stmt_details_token1,
    ACTIONS(51), 1,
      aux_sym_sequence_stmt_title_token1,
    ACTIONS(53), 1,
      aux_sym_sequence_stmt_loop_token1,
    ACTIONS(57), 1,
      aux_sym_sequence_stmt_rect_token1,
    ACTIONS(59), 1,
      aux_sym_sequence_stmt_opt_token1,
    ACTIONS(61), 1,
      aux_sym_sequence_stmt_alt_token1,
    ACTIONS(65), 1,
      aux_sym_sequence_stmt_par_token1,
    ACTIONS(67), 1,
      sym__newline,
    ACTIONS(69), 1,
      sym__sequence_actor_word,
    STATE(8), 1,
      aux_sym__sequence_subdocument,
    STATE(129), 1,
      sym__sequence_stmt,
    STATE(257), 1,
      aux_sym_sequence_actor_repeat1,
    STATE(313), 1,
      sym_sequence_actor,
    STATE(551), 1,
      sym__sequence_participant_type,
    ACTIONS(31), 2,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
    STATE(96), 17,
      sym_sequence_stmt_participant,
      sym_sequence_stmt_actor,
      sym_sequence_stmt_signal,
      sym_sequence_stmt_autonumber,
      sym_sequence_stmt_activate,
      sym_sequence_stmt_deactivate,
      sym_sequence_stmt_note,
      sym_sequence_stmt_links,
      sym_sequence_stmt_link,
      sym_sequence_stmt_properties,
      sym_sequence_stmt_details,
      sym_sequence_stmt_title,
      sym_sequence_stmt_loop,
      sym_sequence_stmt_rect,
      sym_sequence_stmt_opt,
      sym_sequence_stmt_alt,
      sym_sequence_stmt_par,
  [5774] = 26,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(69), 1,
      sym__sequence_actor_word,
    ACTIONS(73), 1,
      aux_sym_sequence_stmt_actor_token1,
    ACTIONS(75), 1,
      aux_sym_sequence_stmt_autonumber_token1,
    ACTIONS(77), 1,
      aux_sym_sequence_stmt_activate_token1,
    ACTIONS(79), 1,
      aux_sym_sequence_stmt_deactivate_token1,
    ACTIONS(81), 1,
      aux_sym_sequence_stmt_note_token1,
    ACTIONS(83), 1,
      aux_sym_sequence_stmt_links_token1,
    ACTIONS(85), 1,
      aux_sym_sequence_stmt_link_token1,
    ACTIONS(87), 1,
      aux_sym_sequence_stmt_properties_token1,
    ACTIONS(89), 1,
      aux_sym_sequence_stmt_details_token1,
    ACTIONS(91), 1,
      aux_sym_sequence_stmt_title_token1,
    ACTIONS(93), 1,
      aux_sym_sequence_stmt_loop_token1,
    ACTIONS(97), 1,
      aux_sym_sequence_stmt_rect_token1,
    ACTIONS(99), 1,
      aux_sym_sequence_stmt_opt_token1,
    ACTIONS(101), 1,
      aux_sym_sequence_stmt_alt_token1,
    ACTIONS(103), 1,
      aux_sym_sequence_stmt_par_token1,
    ACTIONS(107), 1,
      sym__newline,
    STATE(7), 1,
      aux_sym__sequence_subdocument,
    STATE(115), 1,
      sym__sequence_stmt,
    STATE(257), 1,
      aux_sym_sequence_actor_repeat1,
    STATE(301), 1,
      sym_sequence_actor,
    STATE(586), 1,
      sym__sequence_participant_type,
    ACTIONS(71), 2,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
    STATE(109), 17,
      sym_sequence_stmt_participant,
      sym_sequence_stmt_actor,
      sym_sequence_stmt_signal,
      sym_sequence_stmt_autonumber,
      sym_sequence_stmt_activate,
      sym_sequence_stmt_deactivate,
      sym_sequence_stmt_note,
      sym_sequence_stmt_links,
      sym_sequence_stmt_link,
      sym_sequence_stmt_properties,
      sym_sequence_stmt_details,
      sym_sequence_stmt_title,
      sym_sequence_stmt_loop,
      sym_sequence_stmt_rect,
      sym_sequence_stmt_opt,
      sym_sequence_stmt_alt,
      sym_sequence_stmt_par,
  [5870] = 26,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(33), 1,
      aux_sym_sequence_stmt_actor_token1,
    ACTIONS(35), 1,
      aux_sym_sequence_stmt_autonumber_token1,
    ACTIONS(37), 1,
      aux_sym_sequence_stmt_activate_token1,
    ACTIONS(39), 1,
      aux_sym_sequence_stmt_deactivate_token1,
    ACTIONS(41), 1,
      aux_sym_sequence_stmt_note_token1,
    ACTIONS(43), 1,
      aux_sym_sequence_stmt_links_token1,
    ACTIONS(45), 1,
      aux_sym_sequence_stmt_link_token1,
    ACTIONS(47), 1,
      aux_sym_sequence_stmt_properties_token1,
    ACTIONS(49), 1,
      aux_sym_sequence_stmt_details_token1,
    ACTIONS(51), 1,
      aux_sym_sequence_stmt_title_token1,
    ACTIONS(53), 1,
      aux_sym_sequence_stmt_loop_token1,
    ACTIONS(57), 1,
      aux_sym_sequence_stmt_rect_token1,
    ACTIONS(59), 1,
      aux_sym_sequence_stmt_opt_token1,
    ACTIONS(61), 1,
      aux_sym_sequence_stmt_alt_token1,
    ACTIONS(65), 1,
      aux_sym_sequence_stmt_par_token1,
    ACTIONS(67), 1,
      sym__newline,
    ACTIONS(69), 1,
      sym__sequence_actor_word,
    STATE(5), 1,
      aux_sym__sequence_subdocument,
    STATE(129), 1,
      sym__sequence_stmt,
    STATE(257), 1,
      aux_sym_sequence_actor_repeat1,
    STATE(313), 1,
      sym_sequence_actor,
    STATE(551), 1,
      sym__sequence_participant_type,
    ACTIONS(31), 2,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
    STATE(96), 17,
      sym_sequence_stmt_participant,
      sym_sequence_stmt_actor,
      sym_sequence_stmt_signal,
      sym_sequence_stmt_autonumber,
      sym_sequence_stmt_activate,
      sym_sequence_stmt_deactivate,
      sym_sequence_stmt_note,
      sym_sequence_stmt_links,
      sym_sequence_stmt_link,
      sym_sequence_stmt_properties,
      sym_sequence_stmt_details,
      sym_sequence_stmt_title,
      sym_sequence_stmt_loop,
      sym_sequence_stmt_rect,
      sym_sequence_stmt_opt,
      sym_sequence_stmt_alt,
      sym_sequence_stmt_par,
  [5966] = 18,
    ACTIONS(473), 1,
      anon_sym_LBRACE,
    ACTIONS(475), 1,
      anon_sym_LPAREN,
    ACTIONS(477), 1,
      anon_sym_LBRACK,
    ACTIONS(479), 1,
      anon_sym_LPAREN_LPAREN,
    ACTIONS(481), 1,
      anon_sym_LPAREN_DASH,
    ACTIONS(483), 1,
      anon_sym_LPAREN_LBRACK,
    ACTIONS(485), 1,
      anon_sym_LBRACK_LBRACK,
    ACTIONS(487), 1,
      anon_sym_LBRACK_PIPE,
    ACTIONS(489), 1,
      anon_sym_LBRACK_LPAREN,
    ACTIONS(491), 1,
      anon_sym_LBRACE_LBRACE,
    ACTIONS(493), 1,
      anon_sym_GT,
    ACTIONS(495), 1,
      anon_sym_LBRACK_SLASH,
    ACTIONS(497), 1,
      anon_sym_LBRACK_BSLASH,
    STATE(326), 1,
      sym__flow_vertex_kind,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(499), 3,
      aux_sym_flow_link_arrow_start_token1,
      aux_sym_flow_link_arrow_start_token2,
      aux_sym_flow_link_arrow_start_token3,
    ACTIONS(471), 7,
      ts_builtin_sym_end,
      anon_sym_SEMI,
      anon_sym_AMP,
      sym__newline,
      aux_sym_flow_link_arrow_token1,
      aux_sym_flow_link_arrow_token2,
      aux_sym_flow_link_arrow_token3,
    STATE(322), 15,
      sym_flow_vertex_square,
      sym_flow_vertex_circle,
      sym_flow_vertex_ellipse,
      sym_flow_vertex_stadium,
      sym_flow_vertex_subroutine,
      sym_flow_vertex_rect,
      sym_flow_vertex_cylinder,
      sym_flow_vertex_round,
      sym_flow_vertex_diamond,
      sym_flow_vertex_hexagon,
      sym_flow_vertex_odd,
      sym_flow_vertex_trapezoid,
      sym_flow_vertex_inv_trapezoid,
      sym_flow_vertex_leanright,
      sym_flow_vertex_leanleft,
  [6044] = 18,
    ACTIONS(501), 1,
      aux_sym_sequence_stmt_note_token1,
    ACTIONS(503), 1,
      anon_sym_RBRACE,
    ACTIONS(505), 1,
      aux_sym_state_stmt_simple_token1,
    ACTIONS(507), 1,
      sym__newline,
    ACTIONS(509), 1,
      aux_sym_direction_tb_token1,
    ACTIONS(511), 1,
      aux_sym_direction_bt_token1,
    ACTIONS(513), 1,
      aux_sym_direction_rl_token1,
    ACTIONS(515), 1,
      aux_sym_direction_lr_token1,
    ACTIONS(517), 1,
      anon_sym_DASH_DASH,
    ACTIONS(519), 1,
      anon_sym_LBRACK_STAR_RBRACK,
    ACTIONS(521), 1,
      aux_sym_state_name_token1,
    ACTIONS(523), 1,
      aux_sym_state_hide_empty_description_token1,
    STATE(223), 1,
      sym_state_name,
    STATE(235), 1,
      sym_state_hide_empty_description,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    STATE(65), 2,
      sym__state_stmt,
      aux_sym_state_composite_body_repeat1,
    STATE(236), 4,
      sym_direction_tb,
      sym_direction_bt,
      sym_direction_rl,
      sym_direction_lr,
    STATE(245), 8,
      sym__direction,
      sym_state_stmt_simple,
      sym_state_stmt_arrow,
      sym_state_stmt_composite,
      sym_state_stmt_annotation,
      sym_state_stmt_hide_empty_description,
      sym_state_note,
      sym_state_division,
  [6111] = 18,
    ACTIONS(501), 1,
      aux_sym_sequence_stmt_note_token1,
    ACTIONS(505), 1,
      aux_sym_state_stmt_simple_token1,
    ACTIONS(509), 1,
      aux_sym_direction_tb_token1,
    ACTIONS(511), 1,
      aux_sym_direction_bt_token1,
    ACTIONS(513), 1,
      aux_sym_direction_rl_token1,
    ACTIONS(515), 1,
      aux_sym_direction_lr_token1,
    ACTIONS(517), 1,
      anon_sym_DASH_DASH,
    ACTIONS(519), 1,
      anon_sym_LBRACK_STAR_RBRACK,
    ACTIONS(521), 1,
      aux_sym_state_name_token1,
    ACTIONS(523), 1,
      aux_sym_state_hide_empty_description_token1,
    ACTIONS(525), 1,
      anon_sym_RBRACE,
    ACTIONS(527), 1,
      sym__newline,
    STATE(223), 1,
      sym_state_name,
    STATE(235), 1,
      sym_state_hide_empty_description,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    STATE(66), 2,
      sym__state_stmt,
      aux_sym_state_composite_body_repeat1,
    STATE(236), 4,
      sym_direction_tb,
      sym_direction_bt,
      sym_direction_rl,
      sym_direction_lr,
    STATE(245), 8,
      sym__direction,
      sym_state_stmt_simple,
      sym_state_stmt_arrow,
      sym_state_stmt_composite,
      sym_state_stmt_annotation,
      sym_state_stmt_hide_empty_description,
      sym_state_note,
      sym_state_division,
  [6178] = 18,
    ACTIONS(501), 1,
      aux_sym_sequence_stmt_note_token1,
    ACTIONS(505), 1,
      aux_sym_state_stmt_simple_token1,
    ACTIONS(509), 1,
      aux_sym_direction_tb_token1,
    ACTIONS(511), 1,
      aux_sym_direction_bt_token1,
    ACTIONS(513), 1,
      aux_sym_direction_rl_token1,
    ACTIONS(515), 1,
      aux_sym_direction_lr_token1,
    ACTIONS(517), 1,
      anon_sym_DASH_DASH,
    ACTIONS(519), 1,
      anon_sym_LBRACK_STAR_RBRACK,
    ACTIONS(521), 1,
      aux_sym_state_name_token1,
    ACTIONS(523), 1,
      aux_sym_state_hide_empty_description_token1,
    ACTIONS(525), 1,
      anon_sym_RBRACE,
    ACTIONS(527), 1,
      sym__newline,
    STATE(223), 1,
      sym_state_name,
    STATE(235), 1,
      sym_state_hide_empty_description,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    STATE(67), 2,
      sym__state_stmt,
      aux_sym_state_composite_body_repeat1,
    STATE(236), 4,
      sym_direction_tb,
      sym_direction_bt,
      sym_direction_rl,
      sym_direction_lr,
    STATE(245), 8,
      sym__direction,
      sym_state_stmt_simple,
      sym_state_stmt_arrow,
      sym_state_stmt_composite,
      sym_state_stmt_annotation,
      sym_state_stmt_hide_empty_description,
      sym_state_note,
      sym_state_division,
  [6245] = 18,
    ACTIONS(501), 1,
      aux_sym_sequence_stmt_note_token1,
    ACTIONS(505), 1,
      aux_sym_state_stmt_simple_token1,
    ACTIONS(509), 1,
      aux_sym_direction_tb_token1,
    ACTIONS(511), 1,
      aux_sym_direction_bt_token1,
    ACTIONS(513), 1,
      aux_sym_direction_rl_token1,
    ACTIONS(515), 1,
      aux_sym_direction_lr_token1,
    ACTIONS(517), 1,
      anon_sym_DASH_DASH,
    ACTIONS(519), 1,
      anon_sym_LBRACK_STAR_RBRACK,
    ACTIONS(521), 1,
      aux_sym_state_name_token1,
    ACTIONS(523), 1,
      aux_sym_state_hide_empty_description_token1,
    ACTIONS(529), 1,
      anon_sym_RBRACE,
    ACTIONS(531), 1,
      sym__newline,
    STATE(223), 1,
      sym_state_name,
    STATE(235), 1,
      sym_state_hide_empty_description,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    STATE(67), 2,
      sym__state_stmt,
      aux_sym_state_composite_body_repeat1,
    STATE(236), 4,
      sym_direction_tb,
      sym_direction_bt,
      sym_direction_rl,
      sym_direction_lr,
    STATE(245), 8,
      sym__direction,
      sym_state_stmt_simple,
      sym_state_stmt_arrow,
      sym_state_stmt_composite,
      sym_state_stmt_annotation,
      sym_state_stmt_hide_empty_description,
      sym_state_note,
      sym_state_division,
  [6312] = 18,
    ACTIONS(533), 1,
      aux_sym_sequence_stmt_note_token1,
    ACTIONS(536), 1,
      anon_sym_RBRACE,
    ACTIONS(538), 1,
      aux_sym_state_stmt_simple_token1,
    ACTIONS(541), 1,
      sym__newline,
    ACTIONS(544), 1,
      aux_sym_direction_tb_token1,
    ACTIONS(547), 1,
      aux_sym_direction_bt_token1,
    ACTIONS(550), 1,
      aux_sym_direction_rl_token1,
    ACTIONS(553), 1,
      aux_sym_direction_lr_token1,
    ACTIONS(556), 1,
      anon_sym_DASH_DASH,
    ACTIONS(559), 1,
      anon_sym_LBRACK_STAR_RBRACK,
    ACTIONS(562), 1,
      aux_sym_state_name_token1,
    ACTIONS(565), 1,
      aux_sym_state_hide_empty_description_token1,
    STATE(223), 1,
      sym_state_name,
    STATE(235), 1,
      sym_state_hide_empty_description,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    STATE(67), 2,
      sym__state_stmt,
      aux_sym_state_composite_body_repeat1,
    STATE(236), 4,
      sym_direction_tb,
      sym_direction_bt,
      sym_direction_rl,
      sym_direction_lr,
    STATE(245), 8,
      sym__direction,
      sym_state_stmt_simple,
      sym_state_stmt_arrow,
      sym_state_stmt_composite,
      sym_state_stmt_annotation,
      sym_state_stmt_hide_empty_description,
      sym_state_note,
      sym_state_division,
  [6379] = 17,
    ACTIONS(505), 1,
      aux_sym_state_stmt_simple_token1,
    ACTIONS(509), 1,
      aux_sym_direction_tb_token1,
    ACTIONS(511), 1,
      aux_sym_direction_bt_token1,
    ACTIONS(513), 1,
      aux_sym_direction_rl_token1,
    ACTIONS(515), 1,
      aux_sym_direction_lr_token1,
    ACTIONS(517), 1,
      anon_sym_DASH_DASH,
    ACTIONS(523), 1,
      aux_sym_state_hide_empty_description_token1,
    ACTIONS(568), 1,
      ts_builtin_sym_end,
    ACTIONS(570), 1,
      aux_sym_sequence_stmt_note_token1,
    ACTIONS(572), 1,
      anon_sym_LBRACK_STAR_RBRACK,
    ACTIONS(574), 1,
      aux_sym_state_name_token1,
    STATE(235), 1,
      sym_state_hide_empty_description,
    STATE(430), 1,
      sym_state_name,
    STATE(664), 1,
      sym__state_stmt,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    STATE(236), 4,
      sym_direction_tb,
      sym_direction_bt,
      sym_direction_rl,
      sym_direction_lr,
    STATE(245), 8,
      sym__direction,
      sym_state_stmt_simple,
      sym_state_stmt_arrow,
      sym_state_stmt_composite,
      sym_state_stmt_annotation,
      sym_state_stmt_hide_empty_description,
      sym_state_note,
      sym_state_division,
  [6442] = 17,
    ACTIONS(501), 1,
      aux_sym_sequence_stmt_note_token1,
    ACTIONS(505), 1,
      aux_sym_state_stmt_simple_token1,
    ACTIONS(509), 1,
      aux_sym_direction_tb_token1,
    ACTIONS(511), 1,
      aux_sym_direction_bt_token1,
    ACTIONS(513), 1,
      aux_sym_direction_rl_token1,
    ACTIONS(515), 1,
      aux_sym_direction_lr_token1,
    ACTIONS(517), 1,
      anon_sym_DASH_DASH,
    ACTIONS(519), 1,
      anon_sym_LBRACK_STAR_RBRACK,
    ACTIONS(521), 1,
      aux_sym_state_name_token1,
    ACTIONS(523), 1,
      aux_sym_state_hide_empty_description_token1,
    ACTIONS(576), 1,
      anon_sym_RBRACE,
    STATE(223), 1,
      sym_state_name,
    STATE(235), 1,
      sym_state_hide_empty_description,
    STATE(287), 1,
      sym__state_stmt,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    STATE(236), 4,
      sym_direction_tb,
      sym_direction_bt,
      sym_direction_rl,
      sym_direction_lr,
    STATE(245), 8,
      sym__direction,
      sym_state_stmt_simple,
      sym_state_stmt_arrow,
      sym_state_stmt_composite,
      sym_state_stmt_annotation,
      sym_state_stmt_hide_empty_description,
      sym_state_note,
      sym_state_division,
  [6505] = 17,
    ACTIONS(505), 1,
      aux_sym_state_stmt_simple_token1,
    ACTIONS(509), 1,
      aux_sym_direction_tb_token1,
    ACTIONS(511), 1,
      aux_sym_direction_bt_token1,
    ACTIONS(513), 1,
      aux_sym_direction_rl_token1,
    ACTIONS(515), 1,
      aux_sym_direction_lr_token1,
    ACTIONS(517), 1,
      anon_sym_DASH_DASH,
    ACTIONS(523), 1,
      aux_sym_state_hide_empty_description_token1,
    ACTIONS(570), 1,
      aux_sym_sequence_stmt_note_token1,
    ACTIONS(572), 1,
      anon_sym_LBRACK_STAR_RBRACK,
    ACTIONS(574), 1,
      aux_sym_state_name_token1,
    ACTIONS(578), 1,
      ts_builtin_sym_end,
    STATE(235), 1,
      sym_state_hide_empty_description,
    STATE(430), 1,
      sym_state_name,
    STATE(664), 1,
      sym__state_stmt,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    STATE(236), 4,
      sym_direction_tb,
      sym_direction_bt,
      sym_direction_rl,
      sym_direction_lr,
    STATE(245), 8,
      sym__direction,
      sym_state_stmt_simple,
      sym_state_stmt_arrow,
      sym_state_stmt_composite,
      sym_state_stmt_annotation,
      sym_state_stmt_hide_empty_description,
      sym_state_note,
      sym_state_division,
  [6568] = 17,
    ACTIONS(501), 1,
      aux_sym_sequence_stmt_note_token1,
    ACTIONS(505), 1,
      aux_sym_state_stmt_simple_token1,
    ACTIONS(509), 1,
      aux_sym_direction_tb_token1,
    ACTIONS(511), 1,
      aux_sym_direction_bt_token1,
    ACTIONS(513), 1,
      aux_sym_direction_rl_token1,
    ACTIONS(515), 1,
      aux_sym_direction_lr_token1,
    ACTIONS(517), 1,
      anon_sym_DASH_DASH,
    ACTIONS(519), 1,
      anon_sym_LBRACK_STAR_RBRACK,
    ACTIONS(521), 1,
      aux_sym_state_name_token1,
    ACTIONS(523), 1,
      aux_sym_state_hide_empty_description_token1,
    ACTIONS(580), 1,
      sym__newline,
    STATE(63), 1,
      sym__state_stmt,
    STATE(223), 1,
      sym_state_name,
    STATE(235), 1,
      sym_state_hide_empty_description,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    STATE(236), 4,
      sym_direction_tb,
      sym_direction_bt,
      sym_direction_rl,
      sym_direction_lr,
    STATE(245), 8,
      sym__direction,
      sym_state_stmt_simple,
      sym_state_stmt_arrow,
      sym_state_stmt_composite,
      sym_state_stmt_annotation,
      sym_state_stmt_hide_empty_description,
      sym_state_note,
      sym_state_division,
  [6631] = 17,
    ACTIONS(501), 1,
      aux_sym_sequence_stmt_note_token1,
    ACTIONS(505), 1,
      aux_sym_state_stmt_simple_token1,
    ACTIONS(509), 1,
      aux_sym_direction_tb_token1,
    ACTIONS(511), 1,
      aux_sym_direction_bt_token1,
    ACTIONS(513), 1,
      aux_sym_direction_rl_token1,
    ACTIONS(515), 1,
      aux_sym_direction_lr_token1,
    ACTIONS(517), 1,
      anon_sym_DASH_DASH,
    ACTIONS(519), 1,
      anon_sym_LBRACK_STAR_RBRACK,
    ACTIONS(521), 1,
      aux_sym_state_name_token1,
    ACTIONS(523), 1,
      aux_sym_state_hide_empty_description_token1,
    ACTIONS(525), 1,
      anon_sym_RBRACE,
    STATE(223), 1,
      sym_state_name,
    STATE(235), 1,
      sym_state_hide_empty_description,
    STATE(287), 1,
      sym__state_stmt,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    STATE(236), 4,
      sym_direction_tb,
      sym_direction_bt,
      sym_direction_rl,
      sym_direction_lr,
    STATE(245), 8,
      sym__direction,
      sym_state_stmt_simple,
      sym_state_stmt_arrow,
      sym_state_stmt_composite,
      sym_state_stmt_annotation,
      sym_state_stmt_hide_empty_description,
      sym_state_note,
      sym_state_division,
  [6694] = 17,
    ACTIONS(501), 1,
      aux_sym_sequence_stmt_note_token1,
    ACTIONS(505), 1,
      aux_sym_state_stmt_simple_token1,
    ACTIONS(509), 1,
      aux_sym_direction_tb_token1,
    ACTIONS(511), 1,
      aux_sym_direction_bt_token1,
    ACTIONS(513), 1,
      aux_sym_direction_rl_token1,
    ACTIONS(515), 1,
      aux_sym_direction_lr_token1,
    ACTIONS(517), 1,
      anon_sym_DASH_DASH,
    ACTIONS(519), 1,
      anon_sym_LBRACK_STAR_RBRACK,
    ACTIONS(521), 1,
      aux_sym_state_name_token1,
    ACTIONS(523), 1,
      aux_sym_state_hide_empty_description_token1,
    ACTIONS(529), 1,
      anon_sym_RBRACE,
    STATE(223), 1,
      sym_state_name,
    STATE(235), 1,
      sym_state_hide_empty_description,
    STATE(287), 1,
      sym__state_stmt,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    STATE(236), 4,
      sym_direction_tb,
      sym_direction_bt,
      sym_direction_rl,
      sym_direction_lr,
    STATE(245), 8,
      sym__direction,
      sym_state_stmt_simple,
      sym_state_stmt_arrow,
      sym_state_stmt_composite,
      sym_state_stmt_annotation,
      sym_state_stmt_hide_empty_description,
      sym_state_note,
      sym_state_division,
  [6757] = 17,
    ACTIONS(582), 1,
      ts_builtin_sym_end,
    ACTIONS(584), 1,
      aux_sym_sequence_stmt_title_token1,
    ACTIONS(587), 1,
      aux_sym_gantt_stmt_dateformat_token1,
    ACTIONS(590), 1,
      aux_sym_gantt_stmt_inclusiveenddates_token1,
    ACTIONS(593), 1,
      aux_sym_gantt_stmt_topaxis_token1,
    ACTIONS(596), 1,
      aux_sym_gantt_stmt_axisformat_token1,
    ACTIONS(599), 1,
      aux_sym_gantt_stmt_includes_token1,
    ACTIONS(602), 1,
      aux_sym_gantt_stmt_excludes_token1,
    ACTIONS(605), 1,
      aux_sym_gantt_stmt_todaymarker_token1,
    ACTIONS(608), 1,
      aux_sym_gantt_stmt_section_token1,
    ACTIONS(611), 1,
      sym__newline,
    ACTIONS(614), 1,
      aux_sym_gantt_task_text_token1,
    STATE(628), 1,
      aux_sym_gantt_task_text_repeat1,
    STATE(751), 1,
      sym_gantt_task_text,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    STATE(74), 2,
      sym__gantt_stmt,
      aux_sym_diagram_gantt_repeat1,
    STATE(269), 10,
      sym_gantt_stmt_dateformat,
      sym_gantt_stmt_inclusiveenddates,
      sym_gantt_stmt_topaxis,
      sym_gantt_stmt_axisformat,
      sym_gantt_stmt_includes,
      sym_gantt_stmt_excludes,
      sym_gantt_stmt_todaymarker,
      sym_gantt_stmt_title,
      sym_gantt_stmt_section,
      sym_gantt_stmt_task,
  [6820] = 17,
    ACTIONS(617), 1,
      ts_builtin_sym_end,
    ACTIONS(619), 1,
      aux_sym_sequence_stmt_title_token1,
    ACTIONS(621), 1,
      aux_sym_gantt_stmt_dateformat_token1,
    ACTIONS(623), 1,
      aux_sym_gantt_stmt_inclusiveenddates_token1,
    ACTIONS(625), 1,
      aux_sym_gantt_stmt_topaxis_token1,
    ACTIONS(627), 1,
      aux_sym_gantt_stmt_axisformat_token1,
    ACTIONS(629), 1,
      aux_sym_gantt_stmt_includes_token1,
    ACTIONS(631), 1,
      aux_sym_gantt_stmt_excludes_token1,
    ACTIONS(633), 1,
      aux_sym_gantt_stmt_todaymarker_token1,
    ACTIONS(635), 1,
      aux_sym_gantt_stmt_section_token1,
    ACTIONS(637), 1,
      sym__newline,
    ACTIONS(639), 1,
      aux_sym_gantt_task_text_token1,
    STATE(628), 1,
      aux_sym_gantt_task_text_repeat1,
    STATE(751), 1,
      sym_gantt_task_text,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    STATE(77), 2,
      sym__gantt_stmt,
      aux_sym_diagram_gantt_repeat1,
    STATE(269), 10,
      sym_gantt_stmt_dateformat,
      sym_gantt_stmt_inclusiveenddates,
      sym_gantt_stmt_topaxis,
      sym_gantt_stmt_axisformat,
      sym_gantt_stmt_includes,
      sym_gantt_stmt_excludes,
      sym_gantt_stmt_todaymarker,
      sym_gantt_stmt_title,
      sym_gantt_stmt_section,
      sym_gantt_stmt_task,
  [6883] = 17,
    ACTIONS(505), 1,
      aux_sym_state_stmt_simple_token1,
    ACTIONS(509), 1,
      aux_sym_direction_tb_token1,
    ACTIONS(511), 1,
      aux_sym_direction_bt_token1,
    ACTIONS(513), 1,
      aux_sym_direction_rl_token1,
    ACTIONS(515), 1,
      aux_sym_direction_lr_token1,
    ACTIONS(517), 1,
      anon_sym_DASH_DASH,
    ACTIONS(523), 1,
      aux_sym_state_hide_empty_description_token1,
    ACTIONS(570), 1,
      aux_sym_sequence_stmt_note_token1,
    ACTIONS(572), 1,
      anon_sym_LBRACK_STAR_RBRACK,
    ACTIONS(574), 1,
      aux_sym_state_name_token1,
    ACTIONS(641), 1,
      sym__newline,
    STATE(235), 1,
      sym_state_hide_empty_description,
    STATE(430), 1,
      sym_state_name,
    STATE(635), 1,
      sym__state_stmt,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    STATE(236), 4,
      sym_direction_tb,
      sym_direction_bt,
      sym_direction_rl,
      sym_direction_lr,
    STATE(245), 8,
      sym__direction,
      sym_state_stmt_simple,
      sym_state_stmt_arrow,
      sym_state_stmt_composite,
      sym_state_stmt_annotation,
      sym_state_stmt_hide_empty_description,
      sym_state_note,
      sym_state_division,
  [6946] = 17,
    ACTIONS(619), 1,
      aux_sym_sequence_stmt_title_token1,
    ACTIONS(621), 1,
      aux_sym_gantt_stmt_dateformat_token1,
    ACTIONS(623), 1,
      aux_sym_gantt_stmt_inclusiveenddates_token1,
    ACTIONS(625), 1,
      aux_sym_gantt_stmt_topaxis_token1,
    ACTIONS(627), 1,
      aux_sym_gantt_stmt_axisformat_token1,
    ACTIONS(629), 1,
      aux_sym_gantt_stmt_includes_token1,
    ACTIONS(631), 1,
      aux_sym_gantt_stmt_excludes_token1,
    ACTIONS(633), 1,
      aux_sym_gantt_stmt_todaymarker_token1,
    ACTIONS(635), 1,
      aux_sym_gantt_stmt_section_token1,
    ACTIONS(639), 1,
      aux_sym_gantt_task_text_token1,
    ACTIONS(643), 1,
      ts_builtin_sym_end,
    ACTIONS(645), 1,
      sym__newline,
    STATE(628), 1,
      aux_sym_gantt_task_text_repeat1,
    STATE(751), 1,
      sym_gantt_task_text,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    STATE(74), 2,
      sym__gantt_stmt,
      aux_sym_diagram_gantt_repeat1,
    STATE(269), 10,
      sym_gantt_stmt_dateformat,
      sym_gantt_stmt_inclusiveenddates,
      sym_gantt_stmt_topaxis,
      sym_gantt_stmt_axisformat,
      sym_gantt_stmt_includes,
      sym_gantt_stmt_excludes,
      sym_gantt_stmt_todaymarker,
      sym_gantt_stmt_title,
      sym_gantt_stmt_section,
      sym_gantt_stmt_task,
  [7009] = 17,
    ACTIONS(505), 1,
      aux_sym_state_stmt_simple_token1,
    ACTIONS(509), 1,
      aux_sym_direction_tb_token1,
    ACTIONS(511), 1,
      aux_sym_direction_bt_token1,
    ACTIONS(513), 1,
      aux_sym_direction_rl_token1,
    ACTIONS(515), 1,
      aux_sym_direction_lr_token1,
    ACTIONS(517), 1,
      anon_sym_DASH_DASH,
    ACTIONS(523), 1,
      aux_sym_state_hide_empty_description_token1,
    ACTIONS(570), 1,
      aux_sym_sequence_stmt_note_token1,
    ACTIONS(572), 1,
      anon_sym_LBRACK_STAR_RBRACK,
    ACTIONS(574), 1,
      aux_sym_state_name_token1,
    ACTIONS(647), 1,
      ts_builtin_sym_end,
    STATE(235), 1,
      sym_state_hide_empty_description,
    STATE(430), 1,
      sym_state_name,
    STATE(664), 1,
      sym__state_stmt,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    STATE(236), 4,
      sym_direction_tb,
      sym_direction_bt,
      sym_direction_rl,
      sym_direction_lr,
    STATE(245), 8,
      sym__direction,
      sym_state_stmt_simple,
      sym_state_stmt_arrow,
      sym_state_stmt_composite,
      sym_state_stmt_annotation,
      sym_state_stmt_hide_empty_description,
      sym_state_note,
      sym_state_division,
  [7072] = 16,
    ACTIONS(505), 1,
      aux_sym_state_stmt_simple_token1,
    ACTIONS(509), 1,
      aux_sym_direction_tb_token1,
    ACTIONS(511), 1,
      aux_sym_direction_bt_token1,
    ACTIONS(513), 1,
      aux_sym_direction_rl_token1,
    ACTIONS(515), 1,
      aux_sym_direction_lr_token1,
    ACTIONS(517), 1,
      anon_sym_DASH_DASH,
    ACTIONS(523), 1,
      aux_sym_state_hide_empty_description_token1,
    ACTIONS(570), 1,
      aux_sym_sequence_stmt_note_token1,
    ACTIONS(572), 1,
      anon_sym_LBRACK_STAR_RBRACK,
    ACTIONS(574), 1,
      aux_sym_state_name_token1,
    STATE(235), 1,
      sym_state_hide_empty_description,
    STATE(430), 1,
      sym_state_name,
    STATE(664), 1,
      sym__state_stmt,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    STATE(236), 4,
      sym_direction_tb,
      sym_direction_bt,
      sym_direction_rl,
      sym_direction_lr,
    STATE(245), 8,
      sym__direction,
      sym_state_stmt_simple,
      sym_state_stmt_arrow,
      sym_state_stmt_composite,
      sym_state_stmt_annotation,
      sym_state_stmt_hide_empty_description,
      sym_state_note,
      sym_state_division,
  [7132] = 16,
    ACTIONS(501), 1,
      aux_sym_sequence_stmt_note_token1,
    ACTIONS(505), 1,
      aux_sym_state_stmt_simple_token1,
    ACTIONS(509), 1,
      aux_sym_direction_tb_token1,
    ACTIONS(511), 1,
      aux_sym_direction_bt_token1,
    ACTIONS(513), 1,
      aux_sym_direction_rl_token1,
    ACTIONS(515), 1,
      aux_sym_direction_lr_token1,
    ACTIONS(517), 1,
      anon_sym_DASH_DASH,
    ACTIONS(519), 1,
      anon_sym_LBRACK_STAR_RBRACK,
    ACTIONS(521), 1,
      aux_sym_state_name_token1,
    ACTIONS(523), 1,
      aux_sym_state_hide_empty_description_token1,
    STATE(223), 1,
      sym_state_name,
    STATE(235), 1,
      sym_state_hide_empty_description,
    STATE(287), 1,
      sym__state_stmt,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    STATE(236), 4,
      sym_direction_tb,
      sym_direction_bt,
      sym_direction_rl,
      sym_direction_lr,
    STATE(245), 8,
      sym__direction,
      sym_state_stmt_simple,
      sym_state_stmt_arrow,
      sym_state_stmt_composite,
      sym_state_stmt_annotation,
      sym_state_stmt_hide_empty_description,
      sym_state_note,
      sym_state_division,
  [7192] = 16,
    ACTIONS(501), 1,
      aux_sym_sequence_stmt_note_token1,
    ACTIONS(505), 1,
      aux_sym_state_stmt_simple_token1,
    ACTIONS(509), 1,
      aux_sym_direction_tb_token1,
    ACTIONS(511), 1,
      aux_sym_direction_bt_token1,
    ACTIONS(513), 1,
      aux_sym_direction_rl_token1,
    ACTIONS(515), 1,
      aux_sym_direction_lr_token1,
    ACTIONS(517), 1,
      anon_sym_DASH_DASH,
    ACTIONS(519), 1,
      anon_sym_LBRACK_STAR_RBRACK,
    ACTIONS(521), 1,
      aux_sym_state_name_token1,
    ACTIONS(523), 1,
      aux_sym_state_hide_empty_description_token1,
    STATE(64), 1,
      sym__state_stmt,
    STATE(223), 1,
      sym_state_name,
    STATE(235), 1,
      sym_state_hide_empty_description,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    STATE(236), 4,
      sym_direction_tb,
      sym_direction_bt,
      sym_direction_rl,
      sym_direction_lr,
    STATE(245), 8,
      sym__direction,
      sym_state_stmt_simple,
      sym_state_stmt_arrow,
      sym_state_stmt_composite,
      sym_state_stmt_annotation,
      sym_state_stmt_hide_empty_description,
      sym_state_note,
      sym_state_division,
  [7252] = 16,
    ACTIONS(505), 1,
      aux_sym_state_stmt_simple_token1,
    ACTIONS(509), 1,
      aux_sym_direction_tb_token1,
    ACTIONS(511), 1,
      aux_sym_direction_bt_token1,
    ACTIONS(513), 1,
      aux_sym_direction_rl_token1,
    ACTIONS(515), 1,
      aux_sym_direction_lr_token1,
    ACTIONS(517), 1,
      anon_sym_DASH_DASH,
    ACTIONS(523), 1,
      aux_sym_state_hide_empty_description_token1,
    ACTIONS(570), 1,
      aux_sym_sequence_stmt_note_token1,
    ACTIONS(572), 1,
      anon_sym_LBRACK_STAR_RBRACK,
    ACTIONS(574), 1,
      aux_sym_state_name_token1,
    STATE(235), 1,
      sym_state_hide_empty_description,
    STATE(430), 1,
      sym_state_name,
    STATE(646), 1,
      sym__state_stmt,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    STATE(236), 4,
      sym_direction_tb,
      sym_direction_bt,
      sym_direction_rl,
      sym_direction_lr,
    STATE(245), 8,
      sym__direction,
      sym_state_stmt_simple,
      sym_state_stmt_arrow,
      sym_state_stmt_composite,
      sym_state_stmt_annotation,
      sym_state_stmt_hide_empty_description,
      sym_state_note,
      sym_state_division,
  [7312] = 3,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(651), 6,
      anon_sym_LBRACE,
      anon_sym_LPAREN,
      anon_sym_LBRACK,
      aux_sym_flow_link_arrow_start_token1,
      aux_sym_flow_link_arrow_start_token2,
      aux_sym_flow_link_arrow_start_token3,
    ACTIONS(649), 17,
      ts_builtin_sym_end,
      anon_sym_SEMI,
      anon_sym_AMP,
      anon_sym_LPAREN_LPAREN,
      anon_sym_LPAREN_DASH,
      anon_sym_LPAREN_LBRACK,
      anon_sym_LBRACK_LBRACK,
      anon_sym_LBRACK_PIPE,
      anon_sym_LBRACK_LPAREN,
      anon_sym_LBRACE_LBRACE,
      anon_sym_GT,
      anon_sym_LBRACK_SLASH,
      anon_sym_LBRACK_BSLASH,
      sym__newline,
      aux_sym_flow_link_arrow_token1,
      aux_sym_flow_link_arrow_token2,
      aux_sym_flow_link_arrow_token3,
  [7344] = 15,
    ACTIONS(7), 1,
      anon_sym_PERCENT_PERCENT_LBRACE,
    ACTIONS(9), 1,
      aux_sym_diagram_sequence_token1,
    ACTIONS(11), 1,
      aux_sym_diagram_class_token1,
    ACTIONS(13), 1,
      aux_sym_diagram_class_token2,
    ACTIONS(15), 1,
      aux_sym_diagram_state_token1,
    ACTIONS(17), 1,
      aux_sym_diagram_state_token2,
    ACTIONS(19), 1,
      aux_sym_diagram_gantt_token1,
    ACTIONS(21), 1,
      aux_sym_diagram_pie_token1,
    ACTIONS(23), 1,
      aux_sym_diagram_flow_token1,
    ACTIONS(25), 1,
      aux_sym_diagram_er_token1,
    ACTIONS(27), 1,
      aux_sym_diagram_mindmap_token1,
    ACTIONS(653), 1,
      sym__newline,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    STATE(224), 2,
      sym_directive,
      aux_sym_source_file_repeat1,
    STATE(870), 8,
      sym_diagram_sequence,
      sym_diagram_class,
      sym_diagram_state,
      sym_diagram_gantt,
      sym_diagram_pie,
      sym_diagram_flow,
      sym_diagram_er,
      sym_diagram_mindmap,
  [7399] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(657), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(655), 18,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_loop_token2,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      aux_sym_sequence_stmt_alt_token2,
      sym__newline,
  [7431] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(657), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(655), 18,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_loop_token2,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      aux_sym_sequence_stmt_par_token2,
      sym__newline,
  [7463] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(661), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(659), 18,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_loop_token2,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      aux_sym_sequence_stmt_alt_token2,
      sym__newline,
  [7495] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(665), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(663), 18,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_loop_token2,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      aux_sym_sequence_stmt_alt_token2,
      sym__newline,
  [7527] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(669), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(667), 18,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_loop_token2,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      aux_sym_sequence_stmt_alt_token2,
      sym__newline,
  [7559] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(673), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(671), 18,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_loop_token2,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      aux_sym_sequence_stmt_alt_token2,
      sym__newline,
  [7591] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(677), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(675), 18,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_loop_token2,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      aux_sym_sequence_stmt_alt_token2,
      sym__newline,
  [7623] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(681), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(679), 18,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_loop_token2,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      aux_sym_sequence_stmt_alt_token2,
      sym__newline,
  [7655] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(685), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(683), 18,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_loop_token2,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      aux_sym_sequence_stmt_alt_token2,
      sym__newline,
  [7687] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(689), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(687), 18,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_loop_token2,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      aux_sym_sequence_stmt_alt_token2,
      sym__newline,
  [7719] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(693), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(691), 18,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_loop_token2,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      aux_sym_sequence_stmt_alt_token2,
      sym__newline,
  [7751] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(697), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(695), 18,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_loop_token2,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      aux_sym_sequence_stmt_alt_token2,
      sym__newline,
  [7783] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(701), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(699), 18,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_loop_token2,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      aux_sym_sequence_stmt_alt_token2,
      sym__newline,
  [7815] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(705), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(703), 18,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_loop_token2,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      aux_sym_sequence_stmt_alt_token2,
      sym__newline,
  [7847] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(709), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(707), 18,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_loop_token2,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      aux_sym_sequence_stmt_alt_token2,
      sym__newline,
  [7879] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(713), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(711), 18,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_loop_token2,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      aux_sym_sequence_stmt_alt_token2,
      sym__newline,
  [7911] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(717), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(715), 18,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_loop_token2,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      aux_sym_sequence_stmt_alt_token2,
      sym__newline,
  [7943] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(721), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(719), 18,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_loop_token2,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      aux_sym_sequence_stmt_alt_token2,
      sym__newline,
  [7975] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(725), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(723), 18,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_loop_token2,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      aux_sym_sequence_stmt_alt_token2,
      sym__newline,
  [8007] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(729), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(727), 18,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_loop_token2,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      aux_sym_sequence_stmt_alt_token2,
      sym__newline,
  [8039] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(733), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(731), 18,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_loop_token2,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      aux_sym_sequence_stmt_alt_token2,
      sym__newline,
  [8071] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(737), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(735), 18,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_loop_token2,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      aux_sym_sequence_stmt_alt_token2,
      sym__newline,
  [8103] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(741), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(739), 18,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_loop_token2,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      aux_sym_sequence_stmt_alt_token2,
      sym__newline,
  [8135] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(701), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(699), 18,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_loop_token2,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      aux_sym_sequence_stmt_par_token2,
      sym__newline,
  [8167] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(697), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(695), 18,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_loop_token2,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      aux_sym_sequence_stmt_par_token2,
      sym__newline,
  [8199] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(685), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(683), 18,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_loop_token2,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      aux_sym_sequence_stmt_par_token2,
      sym__newline,
  [8231] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(681), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(679), 18,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_loop_token2,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      aux_sym_sequence_stmt_par_token2,
      sym__newline,
  [8263] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(677), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(675), 18,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_loop_token2,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      aux_sym_sequence_stmt_par_token2,
      sym__newline,
  [8295] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(745), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(743), 18,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_loop_token2,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      aux_sym_sequence_stmt_alt_token2,
      sym__newline,
  [8327] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(749), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(747), 18,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_loop_token2,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      aux_sym_sequence_stmt_alt_token2,
      sym__newline,
  [8359] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(753), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(751), 18,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_loop_token2,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      aux_sym_sequence_stmt_par_token2,
      sym__newline,
  [8391] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(757), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(755), 18,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_loop_token2,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      aux_sym_sequence_stmt_alt_token2,
      sym__newline,
  [8423] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(761), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(759), 18,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_loop_token2,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      aux_sym_sequence_stmt_alt_token2,
      sym__newline,
  [8455] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(765), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(763), 18,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_loop_token2,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      aux_sym_sequence_stmt_alt_token2,
      sym__newline,
  [8487] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(769), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(767), 18,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_loop_token2,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      aux_sym_sequence_stmt_alt_token2,
      sym__newline,
  [8519] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(673), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(671), 18,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_loop_token2,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      aux_sym_sequence_stmt_par_token2,
      sym__newline,
  [8551] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(669), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(667), 18,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_loop_token2,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      aux_sym_sequence_stmt_par_token2,
      sym__newline,
  [8583] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(665), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(663), 18,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_loop_token2,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      aux_sym_sequence_stmt_par_token2,
      sym__newline,
  [8615] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(661), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(659), 18,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_loop_token2,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      aux_sym_sequence_stmt_par_token2,
      sym__newline,
  [8647] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(773), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(771), 18,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_loop_token2,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      aux_sym_sequence_stmt_par_token2,
      sym__newline,
  [8679] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(777), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(775), 18,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_loop_token2,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      aux_sym_sequence_stmt_par_token2,
      sym__newline,
  [8711] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(781), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(779), 18,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_loop_token2,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      aux_sym_sequence_stmt_par_token2,
      sym__newline,
  [8743] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(733), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(731), 18,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_loop_token2,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      aux_sym_sequence_stmt_par_token2,
      sym__newline,
  [8775] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(737), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(735), 18,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_loop_token2,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      aux_sym_sequence_stmt_par_token2,
      sym__newline,
  [8807] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(753), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(751), 18,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_loop_token2,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      aux_sym_sequence_stmt_alt_token2,
      sym__newline,
  [8839] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(741), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(739), 18,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_loop_token2,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      aux_sym_sequence_stmt_par_token2,
      sym__newline,
  [8871] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(773), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(771), 18,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_loop_token2,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      aux_sym_sequence_stmt_alt_token2,
      sym__newline,
  [8903] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(745), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(743), 18,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_loop_token2,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      aux_sym_sequence_stmt_par_token2,
      sym__newline,
  [8935] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(689), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(687), 18,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_loop_token2,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      aux_sym_sequence_stmt_par_token2,
      sym__newline,
  [8967] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(693), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(691), 18,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_loop_token2,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      aux_sym_sequence_stmt_par_token2,
      sym__newline,
  [8999] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(749), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(747), 18,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_loop_token2,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      aux_sym_sequence_stmt_par_token2,
      sym__newline,
  [9031] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(757), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(755), 18,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_loop_token2,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      aux_sym_sequence_stmt_par_token2,
      sym__newline,
  [9063] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(769), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(767), 18,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_loop_token2,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      aux_sym_sequence_stmt_par_token2,
      sym__newline,
  [9095] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(705), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(703), 18,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_loop_token2,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      aux_sym_sequence_stmt_par_token2,
      sym__newline,
  [9127] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(781), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(779), 18,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_loop_token2,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      aux_sym_sequence_stmt_alt_token2,
      sym__newline,
  [9159] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(709), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(707), 18,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_loop_token2,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      aux_sym_sequence_stmt_par_token2,
      sym__newline,
  [9191] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(713), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(711), 18,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_loop_token2,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      aux_sym_sequence_stmt_par_token2,
      sym__newline,
  [9223] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(761), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(759), 18,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_loop_token2,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      aux_sym_sequence_stmt_par_token2,
      sym__newline,
  [9255] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(717), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(715), 18,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_loop_token2,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      aux_sym_sequence_stmt_par_token2,
      sym__newline,
  [9287] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(721), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(719), 18,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_loop_token2,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      aux_sym_sequence_stmt_par_token2,
      sym__newline,
  [9319] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(765), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(763), 18,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_loop_token2,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      aux_sym_sequence_stmt_par_token2,
      sym__newline,
  [9351] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(777), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(775), 18,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_loop_token2,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      aux_sym_sequence_stmt_alt_token2,
      sym__newline,
  [9383] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(725), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(723), 18,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_loop_token2,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      aux_sym_sequence_stmt_par_token2,
      sym__newline,
  [9415] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(729), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(727), 18,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_loop_token2,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      aux_sym_sequence_stmt_par_token2,
      sym__newline,
  [9447] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(681), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(679), 17,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_loop_token2,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      sym__newline,
  [9478] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(733), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(731), 17,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_loop_token2,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      sym__newline,
  [9509] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(673), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(671), 17,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_loop_token2,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      sym__newline,
  [9540] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(677), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(675), 17,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_loop_token2,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      sym__newline,
  [9571] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(685), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(683), 17,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_loop_token2,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      sym__newline,
  [9602] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(657), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(655), 17,
      ts_builtin_sym_end,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      sym__newline,
  [9633] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(697), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(695), 17,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_loop_token2,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      sym__newline,
  [9664] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(701), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(699), 17,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_loop_token2,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      sym__newline,
  [9695] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(729), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(727), 17,
      ts_builtin_sym_end,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      sym__newline,
  [9726] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(693), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(691), 17,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_loop_token2,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      sym__newline,
  [9757] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(705), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(703), 17,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_loop_token2,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      sym__newline,
  [9788] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(709), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(707), 17,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_loop_token2,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      sym__newline,
  [9819] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(685), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(683), 17,
      ts_builtin_sym_end,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      sym__newline,
  [9850] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(681), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(679), 17,
      ts_builtin_sym_end,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      sym__newline,
  [9881] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(677), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(675), 17,
      ts_builtin_sym_end,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      sym__newline,
  [9912] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(689), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(687), 17,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_loop_token2,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      sym__newline,
  [9943] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(665), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(663), 17,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_loop_token2,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      sym__newline,
  [9974] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(661), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(659), 17,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_loop_token2,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      sym__newline,
  [10005] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(773), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(771), 17,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_loop_token2,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      sym__newline,
  [10036] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(777), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(775), 17,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_loop_token2,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      sym__newline,
  [10067] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(781), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(779), 17,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_loop_token2,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      sym__newline,
  [10098] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(769), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(767), 17,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_loop_token2,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      sym__newline,
  [10129] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(765), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(763), 17,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_loop_token2,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      sym__newline,
  [10160] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(761), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(759), 17,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_loop_token2,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      sym__newline,
  [10191] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(701), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(699), 17,
      ts_builtin_sym_end,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      sym__newline,
  [10222] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(713), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(711), 17,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_loop_token2,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      sym__newline,
  [10253] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(757), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(755), 17,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_loop_token2,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      sym__newline,
  [10284] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(749), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(747), 17,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_loop_token2,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      sym__newline,
  [10315] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(745), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(743), 17,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_loop_token2,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      sym__newline,
  [10346] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(753), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(751), 17,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_loop_token2,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      sym__newline,
  [10377] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(673), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(671), 17,
      ts_builtin_sym_end,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      sym__newline,
  [10408] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(741), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(739), 17,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_loop_token2,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      sym__newline,
  [10439] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(725), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(723), 17,
      ts_builtin_sym_end,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      sym__newline,
  [10470] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(737), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(735), 17,
      ts_builtin_sym_end,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      sym__newline,
  [10501] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(689), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(687), 17,
      ts_builtin_sym_end,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      sym__newline,
  [10532] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(693), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(691), 17,
      ts_builtin_sym_end,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      sym__newline,
  [10563] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(769), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(767), 17,
      ts_builtin_sym_end,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      sym__newline,
  [10594] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(697), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(695), 17,
      ts_builtin_sym_end,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      sym__newline,
  [10625] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(737), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(735), 17,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_loop_token2,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      sym__newline,
  [10656] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(733), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(731), 17,
      ts_builtin_sym_end,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      sym__newline,
  [10687] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(781), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(779), 17,
      ts_builtin_sym_end,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      sym__newline,
  [10718] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(705), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(703), 17,
      ts_builtin_sym_end,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      sym__newline,
  [10749] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(717), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(715), 17,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_loop_token2,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      sym__newline,
  [10780] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(777), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(775), 17,
      ts_builtin_sym_end,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      sym__newline,
  [10811] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(721), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(719), 17,
      ts_builtin_sym_end,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      sym__newline,
  [10842] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(773), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(771), 17,
      ts_builtin_sym_end,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      sym__newline,
  [10873] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(661), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(659), 17,
      ts_builtin_sym_end,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      sym__newline,
  [10904] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(709), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(707), 17,
      ts_builtin_sym_end,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      sym__newline,
  [10935] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(665), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(663), 17,
      ts_builtin_sym_end,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      sym__newline,
  [10966] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(669), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(667), 17,
      ts_builtin_sym_end,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      sym__newline,
  [10997] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(721), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(719), 17,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_loop_token2,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      sym__newline,
  [11028] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(657), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(655), 17,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_loop_token2,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      sym__newline,
  [11059] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(765), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(763), 17,
      ts_builtin_sym_end,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      sym__newline,
  [11090] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(725), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(723), 17,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_loop_token2,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      sym__newline,
  [11121] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(761), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(759), 17,
      ts_builtin_sym_end,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      sym__newline,
  [11152] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(741), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(739), 17,
      ts_builtin_sym_end,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      sym__newline,
  [11183] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(713), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(711), 17,
      ts_builtin_sym_end,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      sym__newline,
  [11214] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(717), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(715), 17,
      ts_builtin_sym_end,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      sym__newline,
  [11245] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(729), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(727), 17,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_loop_token2,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      sym__newline,
  [11276] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(669), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(667), 17,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_loop_token2,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      sym__newline,
  [11307] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(745), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(743), 17,
      ts_builtin_sym_end,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      sym__newline,
  [11338] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(749), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(747), 17,
      ts_builtin_sym_end,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      sym__newline,
  [11369] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(757), 3,
      aux_sym_sequence_stmt_link_token1,
      aux_sym_sequence_stmt_par_token1,
      sym__sequence_actor_word,
    ACTIONS(755), 17,
      ts_builtin_sym_end,
      aux_sym__sequence_participant_type_token1,
      aux_sym__sequence_participant_type_token2,
      aux_sym_sequence_stmt_actor_token1,
      aux_sym_sequence_stmt_autonumber_token1,
      aux_sym_sequence_stmt_activate_token1,
      aux_sym_sequence_stmt_deactivate_token1,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_sequence_stmt_links_token1,
      aux_sym_sequence_stmt_properties_token1,
      aux_sym_sequence_stmt_details_token1,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_sequence_stmt_loop_token1,
      aux_sym_sequence_stmt_rect_token1,
      aux_sym_sequence_stmt_opt_token1,
      aux_sym_sequence_stmt_alt_token1,
      sym__newline,
  [11400] = 15,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(783), 1,
      ts_builtin_sym_end,
    ACTIONS(785), 1,
      anon_sym_LPAREN,
    ACTIONS(787), 1,
      anon_sym_RPAREN,
    ACTIONS(789), 1,
      anon_sym_LBRACK,
    ACTIONS(791), 1,
      anon_sym_LPAREN_LPAREN,
    ACTIONS(793), 1,
      anon_sym_RPAREN_RPAREN,
    ACTIONS(795), 1,
      anon_sym_LBRACE_LBRACE,
    ACTIONS(797), 1,
      aux_sym_mmap_class_token1,
    ACTIONS(799), 1,
      aux_sym_mmap_icon_token1,
    ACTIONS(801), 1,
      sym__newline,
    STATE(470), 1,
      sym_mmap_icon,
    STATE(657), 1,
      sym_mmap_class,
    STATE(379), 6,
      sym_mmap_node_square,
      sym_mmap_node_rounded,
      sym_mmap_node_circle,
      sym_mmap_node_cloud,
      sym_mmap_node_bang,
      sym_mmap_node_hexagon,
  [11451] = 15,
    ACTIONS(803), 1,
      anon_sym_COLON,
    ACTIONS(805), 1,
      anon_sym_STAR,
    ACTIONS(807), 1,
      anon_sym_GT,
    ACTIONS(809), 1,
      sym__dquote_string,
    ACTIONS(811), 1,
      sym_class_reltype_aggregation,
    ACTIONS(815), 1,
      anon_sym_LT,
    ACTIONS(817), 1,
      anon_sym_DASH_DASH,
    ACTIONS(819), 1,
      anon_sym_DOT_DOT,
    STATE(260), 1,
      sym__class_linetype,
    STATE(378), 1,
      sym_class_relation,
    STATE(401), 1,
      sym__class_reltype,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(813), 2,
      anon_sym_LT_PIPE,
      anon_sym_PIPE_GT,
    STATE(343), 2,
      sym_class_linetype_solid,
      sym_class_linetype_dotted,
    STATE(420), 3,
      sym_class_reltype_extension,
      sym_class_reltype_composition,
      sym_class_reltype_dependency,
  [11502] = 9,
    STATE(214), 1,
      aux_sym_flow_stmt_vertice_repeat1,
    STATE(437), 1,
      sym__flow_link,
    STATE(636), 1,
      sym_flow_link_arrow_start,
    STATE(710), 1,
      sym_flow_link_arrow,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(821), 3,
      ts_builtin_sym_end,
      anon_sym_SEMI,
      sym__newline,
    ACTIONS(823), 3,
      aux_sym_flow_link_arrow_token1,
      aux_sym_flow_link_arrow_token2,
      aux_sym_flow_link_arrow_token3,
    ACTIONS(826), 3,
      aux_sym_flow_link_arrow_start_token1,
      aux_sym_flow_link_arrow_start_token2,
      aux_sym_flow_link_arrow_start_token3,
    STATE(830), 3,
      sym_flow_link_simplelink,
      sym_flow_link_arrowtext,
      sym_flow_link_middletext,
  [11539] = 9,
    STATE(214), 1,
      aux_sym_flow_stmt_vertice_repeat1,
    STATE(437), 1,
      sym__flow_link,
    STATE(636), 1,
      sym_flow_link_arrow_start,
    STATE(710), 1,
      sym_flow_link_arrow,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(829), 3,
      ts_builtin_sym_end,
      anon_sym_SEMI,
      sym__newline,
    ACTIONS(831), 3,
      aux_sym_flow_link_arrow_token1,
      aux_sym_flow_link_arrow_token2,
      aux_sym_flow_link_arrow_token3,
    ACTIONS(833), 3,
      aux_sym_flow_link_arrow_start_token1,
      aux_sym_flow_link_arrow_start_token2,
      aux_sym_flow_link_arrow_start_token3,
    STATE(830), 3,
      sym_flow_link_simplelink,
      sym_flow_link_arrowtext,
      sym_flow_link_middletext,
  [11576] = 13,
    ACTIONS(805), 1,
      anon_sym_STAR,
    ACTIONS(807), 1,
      anon_sym_GT,
    ACTIONS(811), 1,
      sym_class_reltype_aggregation,
    ACTIONS(815), 1,
      anon_sym_LT,
    ACTIONS(817), 1,
      anon_sym_DASH_DASH,
    ACTIONS(819), 1,
      anon_sym_DOT_DOT,
    STATE(260), 1,
      sym__class_linetype,
    STATE(380), 1,
      sym_class_relation,
    STATE(401), 1,
      sym__class_reltype,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(813), 2,
      anon_sym_LT_PIPE,
      anon_sym_PIPE_GT,
    STATE(343), 2,
      sym_class_linetype_solid,
      sym_class_linetype_dotted,
    STATE(420), 3,
      sym_class_reltype_extension,
      sym_class_reltype_composition,
      sym_class_reltype_dependency,
  [11621] = 6,
    ACTIONS(837), 1,
      aux_sym_sequence_stmt_participant_token1,
    ACTIONS(839), 1,
      anon_sym_LBRACE,
    ACTIONS(841), 1,
      aux_sym_state_name_token1,
    STATE(251), 1,
      sym_state_composite_body,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(835), 12,
      ts_builtin_sym_end,
      aux_sym_sequence_stmt_note_token1,
      anon_sym_RBRACE,
      aux_sym_state_stmt_simple_token1,
      sym__newline,
      aux_sym_direction_tb_token1,
      aux_sym_direction_bt_token1,
      aux_sym_direction_rl_token1,
      aux_sym_direction_lr_token1,
      anon_sym_DASH_DASH,
      anon_sym_LBRACK_STAR_RBRACK,
      aux_sym_state_hide_empty_description_token1,
  [11652] = 9,
    STATE(215), 1,
      aux_sym_flow_stmt_vertice_repeat1,
    STATE(437), 1,
      sym__flow_link,
    STATE(636), 1,
      sym_flow_link_arrow_start,
    STATE(710), 1,
      sym_flow_link_arrow,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(831), 3,
      aux_sym_flow_link_arrow_token1,
      aux_sym_flow_link_arrow_token2,
      aux_sym_flow_link_arrow_token3,
    ACTIONS(833), 3,
      aux_sym_flow_link_arrow_start_token1,
      aux_sym_flow_link_arrow_start_token2,
      aux_sym_flow_link_arrow_start_token3,
    ACTIONS(843), 3,
      ts_builtin_sym_end,
      anon_sym_SEMI,
      sym__newline,
    STATE(830), 3,
      sym_flow_link_simplelink,
      sym_flow_link_arrowtext,
      sym_flow_link_middletext,
  [11689] = 8,
    ACTIONS(847), 1,
      sym__newline,
    STATE(308), 1,
      sym__flowchart_direction,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(849), 2,
      anon_sym_LT,
      aux_sym_flowchart_direction_rl_token1,
    ACTIONS(853), 2,
      aux_sym_flowchart_direction_bt_token1,
      anon_sym_CARET,
    ACTIONS(845), 3,
      anon_sym_GT,
      aux_sym_flowchart_direction_lr_token1,
      aux_sym_flowchart_direction_lr_token2,
    ACTIONS(851), 3,
      aux_sym_flowchart_direction_tb_token1,
      aux_sym_flowchart_direction_tb_token2,
      anon_sym_v,
    STATE(368), 4,
      sym_flowchart_direction_lr,
      sym_flowchart_direction_rl,
      sym_flowchart_direction_tb,
      sym_flowchart_direction_bt,
  [11724] = 3,
    ACTIONS(857), 1,
      aux_sym_state_name_token1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(855), 14,
      ts_builtin_sym_end,
      aux_sym_sequence_stmt_participant_token1,
      aux_sym_sequence_stmt_note_token1,
      anon_sym_LBRACE,
      anon_sym_RBRACE,
      aux_sym_state_stmt_simple_token1,
      sym__newline,
      aux_sym_direction_tb_token1,
      aux_sym_direction_bt_token1,
      aux_sym_direction_rl_token1,
      aux_sym_direction_lr_token1,
      anon_sym_DASH_DASH,
      anon_sym_LBRACK_STAR_RBRACK,
      aux_sym_state_hide_empty_description_token1,
  [11748] = 10,
    ACTIONS(861), 1,
      anon_sym_LBRACE,
    ACTIONS(869), 1,
      sym_er_cardinarity_only_one,
    STATE(363), 1,
      sym__er_cardinarity,
    STATE(736), 1,
      sym_er_relation,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(859), 2,
      ts_builtin_sym_end,
      sym__newline,
    ACTIONS(863), 2,
      anon_sym_PIPEo,
      anon_sym_o_PIPE,
    ACTIONS(865), 2,
      anon_sym_RBRACEo,
      anon_sym_o_LBRACE,
    ACTIONS(867), 2,
      anon_sym_RBRACE_PIPE,
      anon_sym_PIPE_LBRACE,
    STATE(400), 3,
      sym_er_cardinarity_zero_or_one,
      sym_er_cardinarity_zero_or_more,
      sym_er_cardinarity_one_or_more,
  [11786] = 7,
    STATE(588), 1,
      sym__flowchart_direction,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(849), 2,
      anon_sym_LT,
      aux_sym_flowchart_direction_rl_token1,
    ACTIONS(853), 2,
      aux_sym_flowchart_direction_bt_token1,
      anon_sym_CARET,
    ACTIONS(845), 3,
      anon_sym_GT,
      aux_sym_flowchart_direction_lr_token1,
      aux_sym_flowchart_direction_lr_token2,
    ACTIONS(851), 3,
      aux_sym_flowchart_direction_tb_token1,
      aux_sym_flowchart_direction_tb_token2,
      anon_sym_v,
    STATE(368), 4,
      sym_flowchart_direction_lr,
      sym_flowchart_direction_rl,
      sym_flowchart_direction_tb,
      sym_flowchart_direction_bt,
  [11818] = 6,
    ACTIONS(871), 1,
      anon_sym_COLON,
    ACTIONS(875), 1,
      anon_sym_DASH_DASH_GT,
    STATE(582), 1,
      sym_state_arrow,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(877), 2,
      anon_sym_DASH_DASH,
      aux_sym_state_name_token1,
    ACTIONS(873), 10,
      aux_sym_sequence_stmt_note_token1,
      anon_sym_RBRACE,
      aux_sym_state_stmt_simple_token1,
      sym__newline,
      aux_sym_direction_tb_token1,
      aux_sym_direction_bt_token1,
      aux_sym_direction_rl_token1,
      aux_sym_direction_lr_token1,
      anon_sym_LBRACK_STAR_RBRACK,
      aux_sym_state_hide_empty_description_token1,
  [11848] = 6,
    ACTIONS(879), 1,
      anon_sym_PERCENT_PERCENT_LBRACE,
    ACTIONS(886), 1,
      sym__newline,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(884), 2,
      aux_sym_diagram_class_token2,
      aux_sym_diagram_state_token1,
    STATE(224), 2,
      sym_directive,
      aux_sym_source_file_repeat1,
    ACTIONS(882), 8,
      aux_sym_diagram_sequence_token1,
      aux_sym_diagram_class_token1,
      aux_sym_diagram_state_token2,
      aux_sym_diagram_gantt_token1,
      aux_sym_diagram_pie_token1,
      aux_sym_diagram_flow_token1,
      aux_sym_diagram_er_token1,
      aux_sym_diagram_mindmap_token1,
  [11877] = 4,
    ACTIONS(891), 1,
      anon_sym_COLON,
    ACTIONS(893), 1,
      aux_sym_state_name_token1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(889), 12,
      ts_builtin_sym_end,
      aux_sym_sequence_stmt_note_token1,
      anon_sym_RBRACE,
      aux_sym_state_stmt_simple_token1,
      sym__newline,
      aux_sym_direction_tb_token1,
      aux_sym_direction_bt_token1,
      aux_sym_direction_rl_token1,
      aux_sym_direction_lr_token1,
      anon_sym_DASH_DASH,
      anon_sym_LBRACK_STAR_RBRACK,
      aux_sym_state_hide_empty_description_token1,
  [11902] = 5,
    ACTIONS(897), 1,
      anon_sym_TILDE,
    ACTIONS(899), 1,
      anon_sym_LT,
    STATE(288), 1,
      sym_class_generics,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(895), 11,
      ts_builtin_sym_end,
      anon_sym_COLON,
      anon_sym_STAR,
      anon_sym_GT,
      sym__newline,
      sym__dquote_string,
      sym_class_reltype_aggregation,
      anon_sym_LT_PIPE,
      anon_sym_PIPE_GT,
      anon_sym_DASH_DASH,
      anon_sym_DOT_DOT,
  [11929] = 6,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(905), 1,
      aux_sym_class_label_token1,
    STATE(234), 1,
      sym_state_description,
    ACTIONS(903), 4,
      anon_sym_RBRACE,
      anon_sym_DASH_DASH,
      anon_sym_LBRACK_STAR_RBRACK,
      aux_sym_state_name_token1,
    ACTIONS(901), 8,
      aux_sym_sequence_stmt_note_token1,
      aux_sym_state_stmt_simple_token1,
      sym__newline,
      aux_sym_direction_tb_token1,
      aux_sym_direction_bt_token1,
      aux_sym_direction_rl_token1,
      aux_sym_direction_lr_token1,
      aux_sym_state_hide_empty_description_token1,
  [11958] = 3,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(909), 2,
      anon_sym_DASH_DASH,
      aux_sym_state_name_token1,
    ACTIONS(907), 12,
      anon_sym_COLON,
      aux_sym_sequence_stmt_note_token1,
      anon_sym_RBRACE,
      aux_sym_state_stmt_simple_token1,
      sym__newline,
      aux_sym_direction_tb_token1,
      aux_sym_direction_bt_token1,
      aux_sym_direction_rl_token1,
      aux_sym_direction_lr_token1,
      anon_sym_DASH_DASH_GT,
      anon_sym_LBRACK_STAR_RBRACK,
      aux_sym_state_hide_empty_description_token1,
  [11981] = 6,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(913), 1,
      sym__sequence_actor_word,
    STATE(229), 1,
      aux_sym_sequence_actor_repeat1,
    ACTIONS(916), 2,
      sym_solid_open_arrow,
      anon_sym_DASH_DASH_GT,
    ACTIONS(911), 9,
      anon_sym_COLON,
      anon_sym_COMMA,
      anon_sym_DASH_RPAREN,
      sym__newline,
      sym_solid_arrow,
      sym_dotted_arrow,
      sym_solid_cross,
      sym_dotted_cross,
      sym_dotted_point,
  [12009] = 3,
    ACTIONS(920), 1,
      aux_sym_state_name_token1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(918), 12,
      ts_builtin_sym_end,
      aux_sym_sequence_stmt_note_token1,
      anon_sym_RBRACE,
      aux_sym_state_stmt_simple_token1,
      sym__newline,
      aux_sym_direction_tb_token1,
      aux_sym_direction_bt_token1,
      aux_sym_direction_rl_token1,
      aux_sym_direction_lr_token1,
      anon_sym_DASH_DASH,
      anon_sym_LBRACK_STAR_RBRACK,
      aux_sym_state_hide_empty_description_token1,
  [12031] = 3,
    ACTIONS(924), 1,
      aux_sym_state_name_token1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(922), 12,
      ts_builtin_sym_end,
      aux_sym_sequence_stmt_note_token1,
      anon_sym_RBRACE,
      aux_sym_state_stmt_simple_token1,
      sym__newline,
      aux_sym_direction_tb_token1,
      aux_sym_direction_bt_token1,
      aux_sym_direction_rl_token1,
      aux_sym_direction_lr_token1,
      anon_sym_DASH_DASH,
      anon_sym_LBRACK_STAR_RBRACK,
      aux_sym_state_hide_empty_description_token1,
  [12053] = 3,
    ACTIONS(928), 1,
      aux_sym_state_name_token1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(926), 12,
      ts_builtin_sym_end,
      aux_sym_sequence_stmt_note_token1,
      anon_sym_RBRACE,
      aux_sym_state_stmt_simple_token1,
      sym__newline,
      aux_sym_direction_tb_token1,
      aux_sym_direction_bt_token1,
      aux_sym_direction_rl_token1,
      aux_sym_direction_lr_token1,
      anon_sym_DASH_DASH,
      anon_sym_LBRACK_STAR_RBRACK,
      aux_sym_state_hide_empty_description_token1,
  [12075] = 3,
    ACTIONS(932), 1,
      aux_sym_state_name_token1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(930), 12,
      ts_builtin_sym_end,
      aux_sym_sequence_stmt_note_token1,
      anon_sym_RBRACE,
      aux_sym_state_stmt_simple_token1,
      sym__newline,
      aux_sym_direction_tb_token1,
      aux_sym_direction_bt_token1,
      aux_sym_direction_rl_token1,
      aux_sym_direction_lr_token1,
      anon_sym_DASH_DASH,
      anon_sym_LBRACK_STAR_RBRACK,
      aux_sym_state_hide_empty_description_token1,
  [12097] = 3,
    ACTIONS(936), 1,
      aux_sym_state_name_token1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(934), 12,
      ts_builtin_sym_end,
      aux_sym_sequence_stmt_note_token1,
      anon_sym_RBRACE,
      aux_sym_state_stmt_simple_token1,
      sym__newline,
      aux_sym_direction_tb_token1,
      aux_sym_direction_bt_token1,
      aux_sym_direction_rl_token1,
      aux_sym_direction_lr_token1,
      anon_sym_DASH_DASH,
      anon_sym_LBRACK_STAR_RBRACK,
      aux_sym_state_hide_empty_description_token1,
  [12119] = 3,
    ACTIONS(940), 1,
      aux_sym_state_name_token1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(938), 12,
      ts_builtin_sym_end,
      aux_sym_sequence_stmt_note_token1,
      anon_sym_RBRACE,
      aux_sym_state_stmt_simple_token1,
      sym__newline,
      aux_sym_direction_tb_token1,
      aux_sym_direction_bt_token1,
      aux_sym_direction_rl_token1,
      aux_sym_direction_lr_token1,
      anon_sym_DASH_DASH,
      anon_sym_LBRACK_STAR_RBRACK,
      aux_sym_state_hide_empty_description_token1,
  [12141] = 3,
    ACTIONS(944), 1,
      aux_sym_state_name_token1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(942), 12,
      ts_builtin_sym_end,
      aux_sym_sequence_stmt_note_token1,
      anon_sym_RBRACE,
      aux_sym_state_stmt_simple_token1,
      sym__newline,
      aux_sym_direction_tb_token1,
      aux_sym_direction_bt_token1,
      aux_sym_direction_rl_token1,
      aux_sym_direction_lr_token1,
      anon_sym_DASH_DASH,
      anon_sym_LBRACK_STAR_RBRACK,
      aux_sym_state_hide_empty_description_token1,
  [12163] = 12,
    ACTIONS(946), 1,
      aux_sym_flow_stmt_direction_token1,
    ACTIONS(948), 1,
      anon_sym_subgraph,
    ACTIONS(950), 1,
      anon_sym_end,
    ACTIONS(952), 1,
      sym__alpha_num_token,
    STATE(62), 1,
      sym_flow_vertex_id,
    STATE(218), 1,
      sym_flow_node,
    STATE(285), 1,
      aux_sym_flow_stmt_subgraph_inner_repeat1,
    STATE(305), 1,
      sym_flow_vertex,
    STATE(691), 1,
      sym__flow_stmt,
    STATE(854), 1,
      sym_flow_stmt_subgraph_inner,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    STATE(618), 3,
      sym_flow_stmt_direction,
      sym_flow_stmt_vertice,
      sym_flow_stmt_subgraph,
  [12203] = 9,
    ACTIONS(837), 1,
      aux_sym_sequence_stmt_participant_token1,
    ACTIONS(839), 1,
      anon_sym_LBRACE,
    STATE(241), 1,
      sym__state_annotation,
    STATE(251), 1,
      sym_state_composite_body,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(954), 2,
      aux_sym_state_annotation_fork_token1,
      aux_sym_state_annotation_fork_token2,
    ACTIONS(956), 2,
      aux_sym_state_annotation_join_token1,
      aux_sym_state_annotation_join_token2,
    ACTIONS(958), 2,
      aux_sym_state_annotation_choice_token1,
      aux_sym_state_annotation_choice_token2,
    STATE(240), 3,
      sym_state_annotation_fork,
      sym_state_annotation_join,
      sym_state_annotation_choice,
  [12237] = 3,
    ACTIONS(962), 1,
      aux_sym_state_name_token1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(960), 12,
      ts_builtin_sym_end,
      aux_sym_sequence_stmt_note_token1,
      anon_sym_RBRACE,
      aux_sym_state_stmt_simple_token1,
      sym__newline,
      aux_sym_direction_tb_token1,
      aux_sym_direction_bt_token1,
      aux_sym_direction_rl_token1,
      aux_sym_direction_lr_token1,
      anon_sym_DASH_DASH,
      anon_sym_LBRACK_STAR_RBRACK,
      aux_sym_state_hide_empty_description_token1,
  [12259] = 3,
    ACTIONS(966), 1,
      aux_sym_state_name_token1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(964), 12,
      ts_builtin_sym_end,
      aux_sym_sequence_stmt_note_token1,
      anon_sym_RBRACE,
      aux_sym_state_stmt_simple_token1,
      sym__newline,
      aux_sym_direction_tb_token1,
      aux_sym_direction_bt_token1,
      aux_sym_direction_rl_token1,
      aux_sym_direction_lr_token1,
      anon_sym_DASH_DASH,
      anon_sym_LBRACK_STAR_RBRACK,
      aux_sym_state_hide_empty_description_token1,
  [12281] = 3,
    ACTIONS(970), 1,
      aux_sym_state_name_token1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(968), 12,
      ts_builtin_sym_end,
      aux_sym_sequence_stmt_note_token1,
      anon_sym_RBRACE,
      aux_sym_state_stmt_simple_token1,
      sym__newline,
      aux_sym_direction_tb_token1,
      aux_sym_direction_bt_token1,
      aux_sym_direction_rl_token1,
      aux_sym_direction_lr_token1,
      anon_sym_DASH_DASH,
      anon_sym_LBRACK_STAR_RBRACK,
      aux_sym_state_hide_empty_description_token1,
  [12303] = 3,
    ACTIONS(974), 1,
      aux_sym_state_name_token1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(972), 12,
      ts_builtin_sym_end,
      aux_sym_sequence_stmt_note_token1,
      anon_sym_RBRACE,
      aux_sym_state_stmt_simple_token1,
      sym__newline,
      aux_sym_direction_tb_token1,
      aux_sym_direction_bt_token1,
      aux_sym_direction_rl_token1,
      aux_sym_direction_lr_token1,
      anon_sym_DASH_DASH,
      anon_sym_LBRACK_STAR_RBRACK,
      aux_sym_state_hide_empty_description_token1,
  [12325] = 3,
    ACTIONS(978), 1,
      aux_sym_state_name_token1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(976), 12,
      ts_builtin_sym_end,
      aux_sym_sequence_stmt_note_token1,
      anon_sym_RBRACE,
      aux_sym_state_stmt_simple_token1,
      sym__newline,
      aux_sym_direction_tb_token1,
      aux_sym_direction_bt_token1,
      aux_sym_direction_rl_token1,
      aux_sym_direction_lr_token1,
      anon_sym_DASH_DASH,
      anon_sym_LBRACK_STAR_RBRACK,
      aux_sym_state_hide_empty_description_token1,
  [12347] = 12,
    ACTIONS(946), 1,
      aux_sym_flow_stmt_direction_token1,
    ACTIONS(948), 1,
      anon_sym_subgraph,
    ACTIONS(952), 1,
      sym__alpha_num_token,
    ACTIONS(980), 1,
      anon_sym_end,
    STATE(62), 1,
      sym_flow_vertex_id,
    STATE(218), 1,
      sym_flow_node,
    STATE(285), 1,
      aux_sym_flow_stmt_subgraph_inner_repeat1,
    STATE(305), 1,
      sym_flow_vertex,
    STATE(691), 1,
      sym__flow_stmt,
    STATE(847), 1,
      sym_flow_stmt_subgraph_inner,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    STATE(618), 3,
      sym_flow_stmt_direction,
      sym_flow_stmt_vertice,
      sym_flow_stmt_subgraph,
  [12387] = 3,
    ACTIONS(984), 1,
      aux_sym_state_name_token1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(982), 12,
      ts_builtin_sym_end,
      aux_sym_sequence_stmt_note_token1,
      anon_sym_RBRACE,
      aux_sym_state_stmt_simple_token1,
      sym__newline,
      aux_sym_direction_tb_token1,
      aux_sym_direction_bt_token1,
      aux_sym_direction_rl_token1,
      aux_sym_direction_lr_token1,
      anon_sym_DASH_DASH,
      anon_sym_LBRACK_STAR_RBRACK,
      aux_sym_state_hide_empty_description_token1,
  [12409] = 3,
    ACTIONS(988), 1,
      aux_sym_state_name_token1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(986), 12,
      ts_builtin_sym_end,
      aux_sym_sequence_stmt_note_token1,
      anon_sym_RBRACE,
      aux_sym_state_stmt_simple_token1,
      sym__newline,
      aux_sym_direction_tb_token1,
      aux_sym_direction_bt_token1,
      aux_sym_direction_rl_token1,
      aux_sym_direction_lr_token1,
      anon_sym_DASH_DASH,
      anon_sym_LBRACK_STAR_RBRACK,
      aux_sym_state_hide_empty_description_token1,
  [12431] = 3,
    ACTIONS(992), 1,
      aux_sym_state_name_token1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(990), 12,
      ts_builtin_sym_end,
      aux_sym_sequence_stmt_note_token1,
      anon_sym_RBRACE,
      aux_sym_state_stmt_simple_token1,
      sym__newline,
      aux_sym_direction_tb_token1,
      aux_sym_direction_bt_token1,
      aux_sym_direction_rl_token1,
      aux_sym_direction_lr_token1,
      anon_sym_DASH_DASH,
      anon_sym_LBRACK_STAR_RBRACK,
      aux_sym_state_hide_empty_description_token1,
  [12453] = 3,
    ACTIONS(996), 1,
      aux_sym_state_name_token1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(994), 12,
      ts_builtin_sym_end,
      aux_sym_sequence_stmt_note_token1,
      anon_sym_RBRACE,
      aux_sym_state_stmt_simple_token1,
      sym__newline,
      aux_sym_direction_tb_token1,
      aux_sym_direction_bt_token1,
      aux_sym_direction_rl_token1,
      aux_sym_direction_lr_token1,
      anon_sym_DASH_DASH,
      anon_sym_LBRACK_STAR_RBRACK,
      aux_sym_state_hide_empty_description_token1,
  [12475] = 3,
    ACTIONS(1000), 1,
      aux_sym_state_name_token1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(998), 12,
      ts_builtin_sym_end,
      aux_sym_sequence_stmt_note_token1,
      anon_sym_RBRACE,
      aux_sym_state_stmt_simple_token1,
      sym__newline,
      aux_sym_direction_tb_token1,
      aux_sym_direction_bt_token1,
      aux_sym_direction_rl_token1,
      aux_sym_direction_lr_token1,
      anon_sym_DASH_DASH,
      anon_sym_LBRACK_STAR_RBRACK,
      aux_sym_state_hide_empty_description_token1,
  [12497] = 10,
    ACTIONS(805), 1,
      anon_sym_STAR,
    ACTIONS(807), 1,
      anon_sym_GT,
    ACTIONS(815), 1,
      anon_sym_LT,
    ACTIONS(1002), 1,
      sym__class_name,
    ACTIONS(1006), 1,
      sym_class_reltype_aggregation,
    STATE(549), 1,
      sym__class_reltype,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(813), 2,
      anon_sym_LT_PIPE,
      anon_sym_PIPE_GT,
    ACTIONS(1004), 2,
      sym__bquote_string,
      sym__dquote_string,
    STATE(420), 3,
      sym_class_reltype_extension,
      sym_class_reltype_composition,
      sym_class_reltype_dependency,
  [12533] = 3,
    ACTIONS(1010), 1,
      aux_sym_state_name_token1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1008), 12,
      ts_builtin_sym_end,
      aux_sym_sequence_stmt_note_token1,
      anon_sym_RBRACE,
      aux_sym_state_stmt_simple_token1,
      sym__newline,
      aux_sym_direction_tb_token1,
      aux_sym_direction_bt_token1,
      aux_sym_direction_rl_token1,
      aux_sym_direction_lr_token1,
      anon_sym_DASH_DASH,
      anon_sym_LBRACK_STAR_RBRACK,
      aux_sym_state_hide_empty_description_token1,
  [12555] = 11,
    ACTIONS(1012), 1,
      ts_builtin_sym_end,
    ACTIONS(1014), 1,
      anon_sym_class,
    ACTIONS(1016), 1,
      anon_sym_LT_LT,
    ACTIONS(1018), 1,
      sym__class_name,
    ACTIONS(1020), 1,
      sym__bquote_string,
    STATE(213), 1,
      sym_class_name,
    STATE(226), 1,
      sym_class_name_body,
    STATE(650), 1,
      aux_sym_class_name_body_repeat1,
    STATE(747), 1,
      sym__class_stmt,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    STATE(676), 4,
      sym_class_stmt_relation,
      sym_class_stmt_class,
      sym_class_stmt_method,
      sym_class_stmt_annotation,
  [12593] = 3,
    ACTIONS(1024), 1,
      aux_sym_state_name_token1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1022), 12,
      ts_builtin_sym_end,
      aux_sym_sequence_stmt_note_token1,
      anon_sym_RBRACE,
      aux_sym_state_stmt_simple_token1,
      sym__newline,
      aux_sym_direction_tb_token1,
      aux_sym_direction_bt_token1,
      aux_sym_direction_rl_token1,
      aux_sym_direction_lr_token1,
      anon_sym_DASH_DASH,
      anon_sym_LBRACK_STAR_RBRACK,
      aux_sym_state_hide_empty_description_token1,
  [12615] = 3,
    ACTIONS(1028), 1,
      aux_sym_state_name_token1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1026), 12,
      ts_builtin_sym_end,
      aux_sym_sequence_stmt_note_token1,
      anon_sym_RBRACE,
      aux_sym_state_stmt_simple_token1,
      sym__newline,
      aux_sym_direction_tb_token1,
      aux_sym_direction_bt_token1,
      aux_sym_direction_rl_token1,
      aux_sym_direction_lr_token1,
      anon_sym_DASH_DASH,
      anon_sym_LBRACK_STAR_RBRACK,
      aux_sym_state_hide_empty_description_token1,
  [12637] = 3,
    ACTIONS(1032), 1,
      aux_sym_state_name_token1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1030), 12,
      ts_builtin_sym_end,
      aux_sym_sequence_stmt_note_token1,
      anon_sym_RBRACE,
      aux_sym_state_stmt_simple_token1,
      sym__newline,
      aux_sym_direction_tb_token1,
      aux_sym_direction_bt_token1,
      aux_sym_direction_rl_token1,
      aux_sym_direction_lr_token1,
      anon_sym_DASH_DASH,
      anon_sym_LBRACK_STAR_RBRACK,
      aux_sym_state_hide_empty_description_token1,
  [12659] = 3,
    ACTIONS(1036), 1,
      aux_sym_state_name_token1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1034), 12,
      ts_builtin_sym_end,
      aux_sym_sequence_stmt_note_token1,
      anon_sym_RBRACE,
      aux_sym_state_stmt_simple_token1,
      sym__newline,
      aux_sym_direction_tb_token1,
      aux_sym_direction_bt_token1,
      aux_sym_direction_rl_token1,
      aux_sym_direction_lr_token1,
      anon_sym_DASH_DASH,
      anon_sym_LBRACK_STAR_RBRACK,
      aux_sym_state_hide_empty_description_token1,
  [12681] = 6,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(1040), 1,
      sym__sequence_actor_word,
    STATE(229), 1,
      aux_sym_sequence_actor_repeat1,
    ACTIONS(1042), 2,
      sym_solid_open_arrow,
      anon_sym_DASH_DASH_GT,
    ACTIONS(1038), 9,
      anon_sym_COLON,
      anon_sym_COMMA,
      anon_sym_DASH_RPAREN,
      sym__newline,
      sym_solid_arrow,
      sym_dotted_arrow,
      sym_solid_cross,
      sym_dotted_cross,
      sym_dotted_point,
  [12709] = 3,
    ACTIONS(1046), 1,
      anon_sym_LT,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1044), 12,
      ts_builtin_sym_end,
      anon_sym_COLON,
      anon_sym_TILDE,
      anon_sym_STAR,
      anon_sym_GT,
      sym__newline,
      sym__dquote_string,
      sym_class_reltype_aggregation,
      anon_sym_LT_PIPE,
      anon_sym_PIPE_GT,
      anon_sym_DASH_DASH,
      anon_sym_DOT_DOT,
  [12731] = 5,
    ACTIONS(1048), 1,
      sym__class_name,
    ACTIONS(1050), 1,
      sym__bquote_string,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1046), 2,
      sym_class_reltype_aggregation,
      anon_sym_LT,
    ACTIONS(1044), 9,
      anon_sym_COLON,
      anon_sym_TILDE,
      anon_sym_STAR,
      anon_sym_GT,
      sym__dquote_string,
      anon_sym_LT_PIPE,
      anon_sym_PIPE_GT,
      anon_sym_DASH_DASH,
      anon_sym_DOT_DOT,
  [12757] = 10,
    ACTIONS(805), 1,
      anon_sym_STAR,
    ACTIONS(807), 1,
      anon_sym_GT,
    ACTIONS(815), 1,
      anon_sym_LT,
    ACTIONS(1006), 1,
      sym_class_reltype_aggregation,
    ACTIONS(1052), 1,
      sym__class_name,
    STATE(625), 1,
      sym__class_reltype,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(813), 2,
      anon_sym_LT_PIPE,
      anon_sym_PIPE_GT,
    ACTIONS(1054), 2,
      sym__bquote_string,
      sym__dquote_string,
    STATE(420), 3,
      sym_class_reltype_extension,
      sym_class_reltype_composition,
      sym_class_reltype_dependency,
  [12793] = 5,
    ACTIONS(1048), 1,
      sym__class_name,
    ACTIONS(1050), 1,
      sym__bquote_string,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1058), 2,
      sym_class_reltype_aggregation,
      anon_sym_LT,
    ACTIONS(1056), 9,
      anon_sym_COLON,
      anon_sym_TILDE,
      anon_sym_STAR,
      anon_sym_GT,
      sym__dquote_string,
      anon_sym_LT_PIPE,
      anon_sym_PIPE_GT,
      anon_sym_DASH_DASH,
      anon_sym_DOT_DOT,
  [12819] = 3,
    ACTIONS(1062), 1,
      aux_sym_state_name_token1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1060), 12,
      ts_builtin_sym_end,
      aux_sym_sequence_stmt_note_token1,
      anon_sym_RBRACE,
      aux_sym_state_stmt_simple_token1,
      sym__newline,
      aux_sym_direction_tb_token1,
      aux_sym_direction_bt_token1,
      aux_sym_direction_rl_token1,
      aux_sym_direction_lr_token1,
      anon_sym_DASH_DASH,
      anon_sym_LBRACK_STAR_RBRACK,
      aux_sym_state_hide_empty_description_token1,
  [12841] = 3,
    ACTIONS(1058), 1,
      anon_sym_LT,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1056), 12,
      ts_builtin_sym_end,
      anon_sym_COLON,
      anon_sym_TILDE,
      anon_sym_STAR,
      anon_sym_GT,
      sym__newline,
      sym__dquote_string,
      sym_class_reltype_aggregation,
      anon_sym_LT_PIPE,
      anon_sym_PIPE_GT,
      anon_sym_DASH_DASH,
      anon_sym_DOT_DOT,
  [12863] = 12,
    ACTIONS(946), 1,
      aux_sym_flow_stmt_direction_token1,
    ACTIONS(948), 1,
      anon_sym_subgraph,
    ACTIONS(952), 1,
      sym__alpha_num_token,
    ACTIONS(1064), 1,
      anon_sym_end,
    STATE(62), 1,
      sym_flow_vertex_id,
    STATE(218), 1,
      sym_flow_node,
    STATE(285), 1,
      aux_sym_flow_stmt_subgraph_inner_repeat1,
    STATE(305), 1,
      sym_flow_vertex,
    STATE(691), 1,
      sym__flow_stmt,
    STATE(826), 1,
      sym_flow_stmt_subgraph_inner,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    STATE(618), 3,
      sym_flow_stmt_direction,
      sym_flow_stmt_vertice,
      sym_flow_stmt_subgraph,
  [12903] = 3,
    ACTIONS(1068), 1,
      aux_sym_state_name_token1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1066), 12,
      ts_builtin_sym_end,
      aux_sym_sequence_stmt_note_token1,
      anon_sym_RBRACE,
      aux_sym_state_stmt_simple_token1,
      sym__newline,
      aux_sym_direction_tb_token1,
      aux_sym_direction_bt_token1,
      aux_sym_direction_rl_token1,
      aux_sym_direction_lr_token1,
      anon_sym_DASH_DASH,
      anon_sym_LBRACK_STAR_RBRACK,
      aux_sym_state_hide_empty_description_token1,
  [12925] = 11,
    ACTIONS(1014), 1,
      anon_sym_class,
    ACTIONS(1016), 1,
      anon_sym_LT_LT,
    ACTIONS(1018), 1,
      sym__class_name,
    ACTIONS(1020), 1,
      sym__bquote_string,
    ACTIONS(1070), 1,
      ts_builtin_sym_end,
    STATE(213), 1,
      sym_class_name,
    STATE(226), 1,
      sym_class_name_body,
    STATE(650), 1,
      aux_sym_class_name_body_repeat1,
    STATE(747), 1,
      sym__class_stmt,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    STATE(676), 4,
      sym_class_stmt_relation,
      sym_class_stmt_class,
      sym_class_stmt_method,
      sym_class_stmt_annotation,
  [12963] = 3,
    ACTIONS(909), 1,
      aux_sym_state_name_token1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(907), 12,
      anon_sym_COLON,
      aux_sym_sequence_stmt_note_token1,
      anon_sym_RBRACE,
      aux_sym_state_stmt_simple_token1,
      sym__newline,
      aux_sym_direction_tb_token1,
      aux_sym_direction_bt_token1,
      aux_sym_direction_rl_token1,
      aux_sym_direction_lr_token1,
      anon_sym_DASH_DASH,
      anon_sym_LBRACK_STAR_RBRACK,
      aux_sym_state_hide_empty_description_token1,
  [12985] = 3,
    ACTIONS(1074), 1,
      aux_sym_gantt_task_text_token1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1072), 11,
      ts_builtin_sym_end,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_gantt_stmt_dateformat_token1,
      aux_sym_gantt_stmt_inclusiveenddates_token1,
      aux_sym_gantt_stmt_topaxis_token1,
      aux_sym_gantt_stmt_axisformat_token1,
      aux_sym_gantt_stmt_includes_token1,
      aux_sym_gantt_stmt_excludes_token1,
      aux_sym_gantt_stmt_todaymarker_token1,
      aux_sym_gantt_stmt_section_token1,
      sym__newline,
  [13006] = 3,
    ACTIONS(1078), 1,
      aux_sym_gantt_task_text_token1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1076), 11,
      ts_builtin_sym_end,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_gantt_stmt_dateformat_token1,
      aux_sym_gantt_stmt_inclusiveenddates_token1,
      aux_sym_gantt_stmt_topaxis_token1,
      aux_sym_gantt_stmt_axisformat_token1,
      aux_sym_gantt_stmt_includes_token1,
      aux_sym_gantt_stmt_excludes_token1,
      aux_sym_gantt_stmt_todaymarker_token1,
      aux_sym_gantt_stmt_section_token1,
      sym__newline,
  [13027] = 11,
    ACTIONS(1080), 1,
      anon_sym_TILDE,
    ACTIONS(1082), 1,
      anon_sym_RBRACE,
    ACTIONS(1084), 1,
      anon_sym_PLUS,
    ACTIONS(1086), 1,
      anon_sym_DASH,
    ACTIONS(1088), 1,
      anon_sym_POUND,
    ACTIONS(1092), 1,
      anon_sym_LT_LT,
    ACTIONS(1094), 1,
      sym__alpha_num_token,
    STATE(341), 1,
      aux_sym_class_method_line_repeat1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1090), 2,
      anon_sym_LPAREN,
      anon_sym_RPAREN,
    STATE(690), 2,
      sym_class_method_line,
      sym_class_annotation_line,
  [13064] = 10,
    ACTIONS(1014), 1,
      anon_sym_class,
    ACTIONS(1016), 1,
      anon_sym_LT_LT,
    ACTIONS(1018), 1,
      sym__class_name,
    ACTIONS(1020), 1,
      sym__bquote_string,
    STATE(213), 1,
      sym_class_name,
    STATE(226), 1,
      sym_class_name_body,
    STATE(650), 1,
      aux_sym_class_name_body_repeat1,
    STATE(747), 1,
      sym__class_stmt,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    STATE(676), 4,
      sym_class_stmt_relation,
      sym_class_stmt_class,
      sym_class_stmt_method,
      sym_class_stmt_annotation,
  [13099] = 11,
    ACTIONS(1080), 1,
      anon_sym_TILDE,
    ACTIONS(1084), 1,
      anon_sym_PLUS,
    ACTIONS(1086), 1,
      anon_sym_DASH,
    ACTIONS(1088), 1,
      anon_sym_POUND,
    ACTIONS(1092), 1,
      anon_sym_LT_LT,
    ACTIONS(1094), 1,
      sym__alpha_num_token,
    ACTIONS(1096), 1,
      anon_sym_RBRACE,
    STATE(341), 1,
      aux_sym_class_method_line_repeat1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1090), 2,
      anon_sym_LPAREN,
      anon_sym_RPAREN,
    STATE(690), 2,
      sym_class_method_line,
      sym_class_annotation_line,
  [13136] = 11,
    ACTIONS(1080), 1,
      anon_sym_TILDE,
    ACTIONS(1084), 1,
      anon_sym_PLUS,
    ACTIONS(1086), 1,
      anon_sym_DASH,
    ACTIONS(1088), 1,
      anon_sym_POUND,
    ACTIONS(1092), 1,
      anon_sym_LT_LT,
    ACTIONS(1094), 1,
      sym__alpha_num_token,
    ACTIONS(1098), 1,
      anon_sym_RBRACE,
    STATE(341), 1,
      aux_sym_class_method_line_repeat1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1090), 2,
      anon_sym_LPAREN,
      anon_sym_RPAREN,
    STATE(690), 2,
      sym_class_method_line,
      sym_class_annotation_line,
  [13173] = 11,
    ACTIONS(1080), 1,
      anon_sym_TILDE,
    ACTIONS(1084), 1,
      anon_sym_PLUS,
    ACTIONS(1086), 1,
      anon_sym_DASH,
    ACTIONS(1088), 1,
      anon_sym_POUND,
    ACTIONS(1092), 1,
      anon_sym_LT_LT,
    ACTIONS(1094), 1,
      sym__alpha_num_token,
    ACTIONS(1100), 1,
      anon_sym_RBRACE,
    STATE(341), 1,
      aux_sym_class_method_line_repeat1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1090), 2,
      anon_sym_LPAREN,
      anon_sym_RPAREN,
    STATE(690), 2,
      sym_class_method_line,
      sym_class_annotation_line,
  [13210] = 2,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(907), 12,
      ts_builtin_sym_end,
      aux_sym_sequence_stmt_participant_token1,
      anon_sym_COLON,
      anon_sym_LBRACE,
      sym__newline,
      anon_sym_DASH_DASH_GT,
      aux_sym_state_annotation_fork_token1,
      aux_sym_state_annotation_fork_token2,
      aux_sym_state_annotation_join_token1,
      aux_sym_state_annotation_join_token2,
      aux_sym_state_annotation_choice_token1,
      aux_sym_state_annotation_choice_token2,
  [13229] = 3,
    ACTIONS(1104), 1,
      anon_sym_LT,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1102), 11,
      ts_builtin_sym_end,
      anon_sym_COLON,
      anon_sym_STAR,
      anon_sym_GT,
      sym__newline,
      sym__dquote_string,
      sym_class_reltype_aggregation,
      anon_sym_LT_PIPE,
      anon_sym_PIPE_GT,
      anon_sym_DASH_DASH,
      anon_sym_DOT_DOT,
  [13250] = 3,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1108), 2,
      aux_sym_diagram_class_token2,
      aux_sym_diagram_state_token1,
    ACTIONS(1106), 10,
      anon_sym_PERCENT_PERCENT_LBRACE,
      aux_sym_diagram_sequence_token1,
      aux_sym_diagram_class_token1,
      aux_sym_diagram_state_token2,
      aux_sym_diagram_gantt_token1,
      aux_sym_diagram_pie_token1,
      aux_sym_diagram_flow_token1,
      aux_sym_diagram_er_token1,
      aux_sym_diagram_mindmap_token1,
      sym__newline,
  [13271] = 3,
    ACTIONS(1112), 1,
      aux_sym_gantt_task_text_token1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1110), 11,
      ts_builtin_sym_end,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_gantt_stmt_dateformat_token1,
      aux_sym_gantt_stmt_inclusiveenddates_token1,
      aux_sym_gantt_stmt_topaxis_token1,
      aux_sym_gantt_stmt_axisformat_token1,
      aux_sym_gantt_stmt_includes_token1,
      aux_sym_gantt_stmt_excludes_token1,
      aux_sym_gantt_stmt_todaymarker_token1,
      aux_sym_gantt_stmt_section_token1,
      sym__newline,
  [13292] = 3,
    ACTIONS(1116), 1,
      aux_sym_gantt_task_text_token1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1114), 11,
      ts_builtin_sym_end,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_gantt_stmt_dateformat_token1,
      aux_sym_gantt_stmt_inclusiveenddates_token1,
      aux_sym_gantt_stmt_topaxis_token1,
      aux_sym_gantt_stmt_axisformat_token1,
      aux_sym_gantt_stmt_includes_token1,
      aux_sym_gantt_stmt_excludes_token1,
      aux_sym_gantt_stmt_todaymarker_token1,
      aux_sym_gantt_stmt_section_token1,
      sym__newline,
  [13313] = 11,
    ACTIONS(1080), 1,
      anon_sym_TILDE,
    ACTIONS(1084), 1,
      anon_sym_PLUS,
    ACTIONS(1086), 1,
      anon_sym_DASH,
    ACTIONS(1088), 1,
      anon_sym_POUND,
    ACTIONS(1092), 1,
      anon_sym_LT_LT,
    ACTIONS(1094), 1,
      sym__alpha_num_token,
    ACTIONS(1118), 1,
      anon_sym_RBRACE,
    STATE(341), 1,
      aux_sym_class_method_line_repeat1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1090), 2,
      anon_sym_LPAREN,
      anon_sym_RPAREN,
    STATE(690), 2,
      sym_class_method_line,
      sym_class_annotation_line,
  [13350] = 3,
    ACTIONS(1122), 1,
      aux_sym_gantt_task_text_token1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1120), 11,
      ts_builtin_sym_end,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_gantt_stmt_dateformat_token1,
      aux_sym_gantt_stmt_inclusiveenddates_token1,
      aux_sym_gantt_stmt_topaxis_token1,
      aux_sym_gantt_stmt_axisformat_token1,
      aux_sym_gantt_stmt_includes_token1,
      aux_sym_gantt_stmt_excludes_token1,
      aux_sym_gantt_stmt_todaymarker_token1,
      aux_sym_gantt_stmt_section_token1,
      sym__newline,
  [13371] = 3,
    ACTIONS(1126), 1,
      aux_sym_gantt_task_text_token1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1124), 11,
      ts_builtin_sym_end,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_gantt_stmt_dateformat_token1,
      aux_sym_gantt_stmt_inclusiveenddates_token1,
      aux_sym_gantt_stmt_topaxis_token1,
      aux_sym_gantt_stmt_axisformat_token1,
      aux_sym_gantt_stmt_includes_token1,
      aux_sym_gantt_stmt_excludes_token1,
      aux_sym_gantt_stmt_todaymarker_token1,
      aux_sym_gantt_stmt_section_token1,
      sym__newline,
  [13392] = 3,
    ACTIONS(1130), 1,
      aux_sym_gantt_task_text_token1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1128), 11,
      ts_builtin_sym_end,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_gantt_stmt_dateformat_token1,
      aux_sym_gantt_stmt_inclusiveenddates_token1,
      aux_sym_gantt_stmt_topaxis_token1,
      aux_sym_gantt_stmt_axisformat_token1,
      aux_sym_gantt_stmt_includes_token1,
      aux_sym_gantt_stmt_excludes_token1,
      aux_sym_gantt_stmt_todaymarker_token1,
      aux_sym_gantt_stmt_section_token1,
      sym__newline,
  [13413] = 3,
    ACTIONS(1134), 1,
      aux_sym_gantt_task_text_token1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1132), 11,
      ts_builtin_sym_end,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_gantt_stmt_dateformat_token1,
      aux_sym_gantt_stmt_inclusiveenddates_token1,
      aux_sym_gantt_stmt_topaxis_token1,
      aux_sym_gantt_stmt_axisformat_token1,
      aux_sym_gantt_stmt_includes_token1,
      aux_sym_gantt_stmt_excludes_token1,
      aux_sym_gantt_stmt_todaymarker_token1,
      aux_sym_gantt_stmt_section_token1,
      sym__newline,
  [13434] = 11,
    ACTIONS(946), 1,
      aux_sym_flow_stmt_direction_token1,
    ACTIONS(948), 1,
      anon_sym_subgraph,
    ACTIONS(952), 1,
      sym__alpha_num_token,
    ACTIONS(1136), 1,
      anon_sym_end,
    STATE(62), 1,
      sym_flow_vertex_id,
    STATE(218), 1,
      sym_flow_node,
    STATE(292), 1,
      aux_sym_flow_stmt_subgraph_inner_repeat1,
    STATE(305), 1,
      sym_flow_vertex,
    STATE(691), 1,
      sym__flow_stmt,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    STATE(618), 3,
      sym_flow_stmt_direction,
      sym_flow_stmt_vertice,
      sym_flow_stmt_subgraph,
  [13471] = 11,
    ACTIONS(1080), 1,
      anon_sym_TILDE,
    ACTIONS(1084), 1,
      anon_sym_PLUS,
    ACTIONS(1086), 1,
      anon_sym_DASH,
    ACTIONS(1088), 1,
      anon_sym_POUND,
    ACTIONS(1092), 1,
      anon_sym_LT_LT,
    ACTIONS(1094), 1,
      sym__alpha_num_token,
    ACTIONS(1138), 1,
      sym__newline,
    STATE(341), 1,
      aux_sym_class_method_line_repeat1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1090), 2,
      anon_sym_LPAREN,
      anon_sym_RPAREN,
    STATE(533), 2,
      sym_class_method_line,
      sym_class_annotation_line,
  [13508] = 3,
    ACTIONS(1140), 1,
      aux_sym_state_name_token1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(536), 11,
      aux_sym_sequence_stmt_note_token1,
      anon_sym_RBRACE,
      aux_sym_state_stmt_simple_token1,
      sym__newline,
      aux_sym_direction_tb_token1,
      aux_sym_direction_bt_token1,
      aux_sym_direction_rl_token1,
      aux_sym_direction_lr_token1,
      anon_sym_DASH_DASH,
      anon_sym_LBRACK_STAR_RBRACK,
      aux_sym_state_hide_empty_description_token1,
  [13529] = 3,
    ACTIONS(1144), 1,
      anon_sym_LT,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1142), 11,
      ts_builtin_sym_end,
      anon_sym_COLON,
      anon_sym_STAR,
      anon_sym_GT,
      sym__newline,
      sym__dquote_string,
      sym_class_reltype_aggregation,
      anon_sym_LT_PIPE,
      anon_sym_PIPE_GT,
      anon_sym_DASH_DASH,
      anon_sym_DOT_DOT,
  [13550] = 3,
    ACTIONS(1148), 1,
      aux_sym_gantt_task_text_token1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1146), 11,
      ts_builtin_sym_end,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_gantt_stmt_dateformat_token1,
      aux_sym_gantt_stmt_inclusiveenddates_token1,
      aux_sym_gantt_stmt_topaxis_token1,
      aux_sym_gantt_stmt_axisformat_token1,
      aux_sym_gantt_stmt_includes_token1,
      aux_sym_gantt_stmt_excludes_token1,
      aux_sym_gantt_stmt_todaymarker_token1,
      aux_sym_gantt_stmt_section_token1,
      sym__newline,
  [13571] = 3,
    ACTIONS(1152), 1,
      aux_sym_gantt_task_text_token1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1150), 11,
      ts_builtin_sym_end,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_gantt_stmt_dateformat_token1,
      aux_sym_gantt_stmt_inclusiveenddates_token1,
      aux_sym_gantt_stmt_topaxis_token1,
      aux_sym_gantt_stmt_axisformat_token1,
      aux_sym_gantt_stmt_includes_token1,
      aux_sym_gantt_stmt_excludes_token1,
      aux_sym_gantt_stmt_todaymarker_token1,
      aux_sym_gantt_stmt_section_token1,
      sym__newline,
  [13592] = 3,
    ACTIONS(1156), 1,
      aux_sym_gantt_task_text_token1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1154), 11,
      ts_builtin_sym_end,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_gantt_stmt_dateformat_token1,
      aux_sym_gantt_stmt_inclusiveenddates_token1,
      aux_sym_gantt_stmt_topaxis_token1,
      aux_sym_gantt_stmt_axisformat_token1,
      aux_sym_gantt_stmt_includes_token1,
      aux_sym_gantt_stmt_excludes_token1,
      aux_sym_gantt_stmt_todaymarker_token1,
      aux_sym_gantt_stmt_section_token1,
      sym__newline,
  [13613] = 11,
    ACTIONS(1158), 1,
      aux_sym_flow_stmt_direction_token1,
    ACTIONS(1161), 1,
      anon_sym_subgraph,
    ACTIONS(1164), 1,
      anon_sym_end,
    ACTIONS(1166), 1,
      sym__alpha_num_token,
    STATE(62), 1,
      sym_flow_vertex_id,
    STATE(218), 1,
      sym_flow_node,
    STATE(292), 1,
      aux_sym_flow_stmt_subgraph_inner_repeat1,
    STATE(305), 1,
      sym_flow_vertex,
    STATE(691), 1,
      sym__flow_stmt,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    STATE(618), 3,
      sym_flow_stmt_direction,
      sym_flow_stmt_vertice,
      sym_flow_stmt_subgraph,
  [13650] = 3,
    ACTIONS(1171), 1,
      aux_sym_gantt_task_text_token1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1169), 11,
      ts_builtin_sym_end,
      aux_sym_sequence_stmt_title_token1,
      aux_sym_gantt_stmt_dateformat_token1,
      aux_sym_gantt_stmt_inclusiveenddates_token1,
      aux_sym_gantt_stmt_topaxis_token1,
      aux_sym_gantt_stmt_axisformat_token1,
      aux_sym_gantt_stmt_includes_token1,
      aux_sym_gantt_stmt_excludes_token1,
      aux_sym_gantt_stmt_todaymarker_token1,
      aux_sym_gantt_stmt_section_token1,
      sym__newline,
  [13671] = 11,
    ACTIONS(1080), 1,
      anon_sym_TILDE,
    ACTIONS(1084), 1,
      anon_sym_PLUS,
    ACTIONS(1086), 1,
      anon_sym_DASH,
    ACTIONS(1088), 1,
      anon_sym_POUND,
    ACTIONS(1092), 1,
      anon_sym_LT_LT,
    ACTIONS(1094), 1,
      sym__alpha_num_token,
    ACTIONS(1173), 1,
      anon_sym_RBRACE,
    STATE(341), 1,
      aux_sym_class_method_line_repeat1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1090), 2,
      anon_sym_LPAREN,
      anon_sym_RPAREN,
    STATE(690), 2,
      sym_class_method_line,
      sym_class_annotation_line,
  [13708] = 10,
    ACTIONS(1014), 1,
      anon_sym_class,
    ACTIONS(1016), 1,
      anon_sym_LT_LT,
    ACTIONS(1018), 1,
      sym__class_name,
    ACTIONS(1020), 1,
      sym__bquote_string,
    STATE(213), 1,
      sym_class_name,
    STATE(226), 1,
      sym_class_name_body,
    STATE(650), 1,
      aux_sym_class_name_body_repeat1,
    STATE(652), 1,
      sym__class_stmt,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    STATE(676), 4,
      sym_class_stmt_relation,
      sym_class_stmt_class,
      sym_class_stmt_method,
      sym_class_stmt_annotation,
  [13743] = 11,
    ACTIONS(1080), 1,
      anon_sym_TILDE,
    ACTIONS(1084), 1,
      anon_sym_PLUS,
    ACTIONS(1086), 1,
      anon_sym_DASH,
    ACTIONS(1088), 1,
      anon_sym_POUND,
    ACTIONS(1092), 1,
      anon_sym_LT_LT,
    ACTIONS(1094), 1,
      sym__alpha_num_token,
    ACTIONS(1175), 1,
      sym__newline,
    STATE(341), 1,
      aux_sym_class_method_line_repeat1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1090), 2,
      anon_sym_LPAREN,
      anon_sym_RPAREN,
    STATE(563), 2,
      sym_class_method_line,
      sym_class_annotation_line,
  [13780] = 5,
    ACTIONS(1179), 1,
      anon_sym_AMP,
    STATE(302), 1,
      aux_sym_flow_node_repeat1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1181), 3,
      aux_sym_flow_link_arrow_start_token1,
      aux_sym_flow_link_arrow_start_token2,
      aux_sym_flow_link_arrow_start_token3,
    ACTIONS(1177), 6,
      ts_builtin_sym_end,
      anon_sym_SEMI,
      sym__newline,
      aux_sym_flow_link_arrow_token1,
      aux_sym_flow_link_arrow_token2,
      aux_sym_flow_link_arrow_token3,
  [13804] = 7,
    ACTIONS(869), 1,
      sym_er_cardinarity_only_one,
    STATE(768), 1,
      sym__er_cardinarity,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(863), 2,
      anon_sym_PIPEo,
      anon_sym_o_PIPE,
    ACTIONS(865), 2,
      anon_sym_RBRACEo,
      anon_sym_o_LBRACE,
    ACTIONS(867), 2,
      anon_sym_RBRACE_PIPE,
      anon_sym_PIPE_LBRACE,
    STATE(400), 3,
      sym_er_cardinarity_zero_or_one,
      sym_er_cardinarity_zero_or_more,
      sym_er_cardinarity_one_or_more,
  [13832] = 5,
    ACTIONS(1185), 1,
      anon_sym_RBRACK,
    ACTIONS(1187), 1,
      aux_sym_flow_text_literal_token1,
    STATE(315), 1,
      aux_sym_flow_text_literal_repeat1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1183), 8,
      anon_sym_RBRACE,
      anon_sym_RPAREN,
      anon_sym_DASH_RPAREN,
      anon_sym_RBRACK_RPAREN,
      anon_sym_RBRACK_RBRACK,
      anon_sym_PIPE_RBRACK,
      anon_sym_BSLASH_RBRACK,
      anon_sym_SLASH_RBRACK,
  [13856] = 2,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1189), 11,
      ts_builtin_sym_end,
      anon_sym_COLON,
      anon_sym_LBRACE,
      sym__newline,
      anon_sym_PIPEo,
      anon_sym_o_PIPE,
      anon_sym_RBRACEo,
      anon_sym_o_LBRACE,
      anon_sym_RBRACE_PIPE,
      anon_sym_PIPE_LBRACE,
      sym_er_cardinarity_only_one,
  [13874] = 7,
    ACTIONS(1191), 1,
      anon_sym_DASH_RPAREN,
    ACTIONS(1195), 1,
      sym_solid_open_arrow,
    ACTIONS(1197), 1,
      anon_sym_DASH_DASH_GT,
    STATE(366), 1,
      sym_sequence_signal_type,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    STATE(655), 2,
      sym_dotted_open_arrow,
      sym_solid_point,
    ACTIONS(1193), 5,
      sym_solid_arrow,
      sym_dotted_arrow,
      sym_solid_cross,
      sym_dotted_cross,
      sym_dotted_point,
  [13902] = 5,
    ACTIONS(1201), 1,
      anon_sym_AMP,
    STATE(302), 1,
      aux_sym_flow_node_repeat1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1204), 3,
      aux_sym_flow_link_arrow_start_token1,
      aux_sym_flow_link_arrow_start_token2,
      aux_sym_flow_link_arrow_start_token3,
    ACTIONS(1199), 6,
      ts_builtin_sym_end,
      anon_sym_SEMI,
      sym__newline,
      aux_sym_flow_link_arrow_token1,
      aux_sym_flow_link_arrow_token2,
      aux_sym_flow_link_arrow_token3,
  [13926] = 10,
    ACTIONS(946), 1,
      aux_sym_flow_stmt_direction_token1,
    ACTIONS(948), 1,
      anon_sym_subgraph,
    ACTIONS(952), 1,
      sym__alpha_num_token,
    ACTIONS(1206), 1,
      ts_builtin_sym_end,
    STATE(62), 1,
      sym_flow_vertex_id,
    STATE(218), 1,
      sym_flow_node,
    STATE(305), 1,
      sym_flow_vertex,
    STATE(644), 1,
      sym__flow_stmt,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    STATE(618), 3,
      sym_flow_stmt_direction,
      sym_flow_stmt_vertice,
      sym_flow_stmt_subgraph,
  [13960] = 10,
    ACTIONS(1208), 1,
      ts_builtin_sym_end,
    ACTIONS(1210), 1,
      aux_sym_sequence_stmt_title_token1,
    ACTIONS(1212), 1,
      sym__newline,
    ACTIONS(1214), 1,
      aux_sym_pie_showdata_token1,
    ACTIONS(1216), 1,
      aux_sym_pie_label_token1,
    STATE(347), 1,
      sym_pie_showdata,
    STATE(840), 1,
      sym_pie_label,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    STATE(346), 2,
      sym__pie_stmt,
      aux_sym_diagram_pie_repeat1,
    STATE(433), 2,
      sym_pie_stmt_title,
      sym_pie_stmt_element,
  [13994] = 5,
    ACTIONS(1179), 1,
      anon_sym_AMP,
    STATE(297), 1,
      aux_sym_flow_node_repeat1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1220), 3,
      aux_sym_flow_link_arrow_start_token1,
      aux_sym_flow_link_arrow_start_token2,
      aux_sym_flow_link_arrow_start_token3,
    ACTIONS(1218), 6,
      ts_builtin_sym_end,
      anon_sym_SEMI,
      sym__newline,
      aux_sym_flow_link_arrow_token1,
      aux_sym_flow_link_arrow_token2,
      aux_sym_flow_link_arrow_token3,
  [14018] = 10,
    ACTIONS(946), 1,
      aux_sym_flow_stmt_direction_token1,
    ACTIONS(948), 1,
      anon_sym_subgraph,
    ACTIONS(952), 1,
      sym__alpha_num_token,
    ACTIONS(1222), 1,
      ts_builtin_sym_end,
    STATE(62), 1,
      sym_flow_vertex_id,
    STATE(218), 1,
      sym_flow_node,
    STATE(305), 1,
      sym_flow_vertex,
    STATE(644), 1,
      sym__flow_stmt,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    STATE(618), 3,
      sym_flow_stmt_direction,
      sym_flow_stmt_vertice,
      sym_flow_stmt_subgraph,
  [14052] = 7,
    ACTIONS(1191), 1,
      anon_sym_DASH_RPAREN,
    ACTIONS(1195), 1,
      sym_solid_open_arrow,
    ACTIONS(1197), 1,
      anon_sym_DASH_DASH_GT,
    STATE(354), 1,
      sym_sequence_signal_type,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    STATE(655), 2,
      sym_dotted_open_arrow,
      sym_solid_point,
    ACTIONS(1193), 5,
      sym_solid_arrow,
      sym_dotted_arrow,
      sym_solid_cross,
      sym_dotted_cross,
      sym_dotted_point,
  [14080] = 10,
    ACTIONS(946), 1,
      aux_sym_flow_stmt_direction_token1,
    ACTIONS(948), 1,
      anon_sym_subgraph,
    ACTIONS(952), 1,
      sym__alpha_num_token,
    ACTIONS(1224), 1,
      sym__newline,
    STATE(62), 1,
      sym_flow_vertex_id,
    STATE(218), 1,
      sym_flow_node,
    STATE(305), 1,
      sym_flow_vertex,
    STATE(452), 1,
      sym__flow_stmt,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    STATE(618), 3,
      sym_flow_stmt_direction,
      sym_flow_stmt_vertice,
      sym_flow_stmt_subgraph,
  [14114] = 10,
    ACTIONS(1080), 1,
      anon_sym_TILDE,
    ACTIONS(1084), 1,
      anon_sym_PLUS,
    ACTIONS(1086), 1,
      anon_sym_DASH,
    ACTIONS(1088), 1,
      anon_sym_POUND,
    ACTIONS(1092), 1,
      anon_sym_LT_LT,
    ACTIONS(1094), 1,
      sym__alpha_num_token,
    STATE(341), 1,
      aux_sym_class_method_line_repeat1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1090), 2,
      anon_sym_LPAREN,
      anon_sym_RPAREN,
    STATE(690), 2,
      sym_class_method_line,
      sym_class_annotation_line,
  [14148] = 10,
    ACTIONS(1080), 1,
      anon_sym_TILDE,
    ACTIONS(1084), 1,
      anon_sym_PLUS,
    ACTIONS(1086), 1,
      anon_sym_DASH,
    ACTIONS(1088), 1,
      anon_sym_POUND,
    ACTIONS(1092), 1,
      anon_sym_LT_LT,
    ACTIONS(1094), 1,
      sym__alpha_num_token,
    STATE(341), 1,
      aux_sym_class_method_line_repeat1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1090), 2,
      anon_sym_LPAREN,
      anon_sym_RPAREN,
    STATE(550), 2,
      sym_class_method_line,
      sym_class_annotation_line,
  [14182] = 7,
    ACTIONS(1191), 1,
      anon_sym_DASH_RPAREN,
    ACTIONS(1195), 1,
      sym_solid_open_arrow,
    ACTIONS(1197), 1,
      anon_sym_DASH_DASH_GT,
    STATE(359), 1,
      sym_sequence_signal_type,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    STATE(655), 2,
      sym_dotted_open_arrow,
      sym_solid_point,
    ACTIONS(1193), 5,
      sym_solid_arrow,
      sym_dotted_arrow,
      sym_solid_cross,
      sym_dotted_cross,
      sym_dotted_point,
  [14210] = 10,
    ACTIONS(1080), 1,
      anon_sym_TILDE,
    ACTIONS(1084), 1,
      anon_sym_PLUS,
    ACTIONS(1086), 1,
      anon_sym_DASH,
    ACTIONS(1088), 1,
      anon_sym_POUND,
    ACTIONS(1092), 1,
      anon_sym_LT_LT,
    ACTIONS(1094), 1,
      sym__alpha_num_token,
    STATE(341), 1,
      aux_sym_class_method_line_repeat1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1090), 2,
      anon_sym_LPAREN,
      anon_sym_RPAREN,
    STATE(499), 2,
      sym_class_method_line,
      sym_class_annotation_line,
  [14244] = 7,
    ACTIONS(1191), 1,
      anon_sym_DASH_RPAREN,
    ACTIONS(1195), 1,
      sym_solid_open_arrow,
    ACTIONS(1197), 1,
      anon_sym_DASH_DASH_GT,
    STATE(353), 1,
      sym_sequence_signal_type,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    STATE(655), 2,
      sym_dotted_open_arrow,
      sym_solid_point,
    ACTIONS(1193), 5,
      sym_solid_arrow,
      sym_dotted_arrow,
      sym_solid_cross,
      sym_dotted_cross,
      sym_dotted_point,
  [14272] = 10,
    ACTIONS(946), 1,
      aux_sym_flow_stmt_direction_token1,
    ACTIONS(948), 1,
      anon_sym_subgraph,
    ACTIONS(952), 1,
      sym__alpha_num_token,
    ACTIONS(1226), 1,
      ts_builtin_sym_end,
    STATE(62), 1,
      sym_flow_vertex_id,
    STATE(218), 1,
      sym_flow_node,
    STATE(305), 1,
      sym_flow_vertex,
    STATE(644), 1,
      sym__flow_stmt,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    STATE(618), 3,
      sym_flow_stmt_direction,
      sym_flow_stmt_vertice,
      sym_flow_stmt_subgraph,
  [14306] = 5,
    ACTIONS(1230), 1,
      anon_sym_RBRACK,
    ACTIONS(1232), 1,
      aux_sym_flow_text_literal_token1,
    STATE(315), 1,
      aux_sym_flow_text_literal_repeat1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1228), 8,
      anon_sym_RBRACE,
      anon_sym_RPAREN,
      anon_sym_DASH_RPAREN,
      anon_sym_RBRACK_RPAREN,
      anon_sym_RBRACK_RBRACK,
      anon_sym_PIPE_RBRACK,
      anon_sym_BSLASH_RBRACK,
      anon_sym_SLASH_RBRACK,
  [14330] = 3,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1237), 3,
      aux_sym_flow_link_arrow_start_token1,
      aux_sym_flow_link_arrow_start_token2,
      aux_sym_flow_link_arrow_start_token3,
    ACTIONS(1235), 7,
      ts_builtin_sym_end,
      anon_sym_SEMI,
      anon_sym_AMP,
      sym__newline,
      aux_sym_flow_link_arrow_token1,
      aux_sym_flow_link_arrow_token2,
      aux_sym_flow_link_arrow_token3,
  [14349] = 3,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1241), 3,
      aux_sym_flow_link_arrow_start_token1,
      aux_sym_flow_link_arrow_start_token2,
      aux_sym_flow_link_arrow_start_token3,
    ACTIONS(1239), 7,
      ts_builtin_sym_end,
      anon_sym_SEMI,
      anon_sym_AMP,
      sym__newline,
      aux_sym_flow_link_arrow_token1,
      aux_sym_flow_link_arrow_token2,
      aux_sym_flow_link_arrow_token3,
  [14368] = 3,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1245), 3,
      aux_sym_flow_link_arrow_start_token1,
      aux_sym_flow_link_arrow_start_token2,
      aux_sym_flow_link_arrow_start_token3,
    ACTIONS(1243), 7,
      ts_builtin_sym_end,
      anon_sym_SEMI,
      anon_sym_AMP,
      sym__newline,
      aux_sym_flow_link_arrow_token1,
      aux_sym_flow_link_arrow_token2,
      aux_sym_flow_link_arrow_token3,
  [14387] = 3,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1249), 3,
      aux_sym_flow_link_arrow_start_token1,
      aux_sym_flow_link_arrow_start_token2,
      aux_sym_flow_link_arrow_start_token3,
    ACTIONS(1247), 7,
      ts_builtin_sym_end,
      anon_sym_SEMI,
      anon_sym_AMP,
      sym__newline,
      aux_sym_flow_link_arrow_token1,
      aux_sym_flow_link_arrow_token2,
      aux_sym_flow_link_arrow_token3,
  [14406] = 3,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1253), 3,
      aux_sym_flow_link_arrow_start_token1,
      aux_sym_flow_link_arrow_start_token2,
      aux_sym_flow_link_arrow_start_token3,
    ACTIONS(1251), 7,
      ts_builtin_sym_end,
      anon_sym_SEMI,
      anon_sym_AMP,
      sym__newline,
      aux_sym_flow_link_arrow_token1,
      aux_sym_flow_link_arrow_token2,
      aux_sym_flow_link_arrow_token3,
  [14425] = 3,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1257), 3,
      aux_sym_flow_link_arrow_start_token1,
      aux_sym_flow_link_arrow_start_token2,
      aux_sym_flow_link_arrow_start_token3,
    ACTIONS(1255), 7,
      ts_builtin_sym_end,
      anon_sym_SEMI,
      anon_sym_AMP,
      sym__newline,
      aux_sym_flow_link_arrow_token1,
      aux_sym_flow_link_arrow_token2,
      aux_sym_flow_link_arrow_token3,
  [14444] = 3,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1261), 3,
      aux_sym_flow_link_arrow_start_token1,
      aux_sym_flow_link_arrow_start_token2,
      aux_sym_flow_link_arrow_start_token3,
    ACTIONS(1259), 7,
      ts_builtin_sym_end,
      anon_sym_SEMI,
      anon_sym_AMP,
      sym__newline,
      aux_sym_flow_link_arrow_token1,
      aux_sym_flow_link_arrow_token2,
      aux_sym_flow_link_arrow_token3,
  [14463] = 9,
    ACTIONS(946), 1,
      aux_sym_flow_stmt_direction_token1,
    ACTIONS(948), 1,
      anon_sym_subgraph,
    ACTIONS(952), 1,
      sym__alpha_num_token,
    STATE(62), 1,
      sym_flow_vertex_id,
    STATE(218), 1,
      sym_flow_node,
    STATE(305), 1,
      sym_flow_vertex,
    STATE(644), 1,
      sym__flow_stmt,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    STATE(618), 3,
      sym_flow_stmt_direction,
      sym_flow_stmt_vertice,
      sym_flow_stmt_subgraph,
  [14494] = 3,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1265), 3,
      aux_sym_flow_link_arrow_start_token1,
      aux_sym_flow_link_arrow_start_token2,
      aux_sym_flow_link_arrow_start_token3,
    ACTIONS(1263), 7,
      ts_builtin_sym_end,
      anon_sym_SEMI,
      anon_sym_AMP,
      sym__newline,
      aux_sym_flow_link_arrow_token1,
      aux_sym_flow_link_arrow_token2,
      aux_sym_flow_link_arrow_token3,
  [14513] = 3,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1269), 3,
      aux_sym_flow_link_arrow_start_token1,
      aux_sym_flow_link_arrow_start_token2,
      aux_sym_flow_link_arrow_start_token3,
    ACTIONS(1267), 7,
      ts_builtin_sym_end,
      anon_sym_SEMI,
      anon_sym_AMP,
      sym__newline,
      aux_sym_flow_link_arrow_token1,
      aux_sym_flow_link_arrow_token2,
      aux_sym_flow_link_arrow_token3,
  [14532] = 3,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1273), 3,
      aux_sym_flow_link_arrow_start_token1,
      aux_sym_flow_link_arrow_start_token2,
      aux_sym_flow_link_arrow_start_token3,
    ACTIONS(1271), 7,
      ts_builtin_sym_end,
      anon_sym_SEMI,
      anon_sym_AMP,
      sym__newline,
      aux_sym_flow_link_arrow_token1,
      aux_sym_flow_link_arrow_token2,
      aux_sym_flow_link_arrow_token3,
  [14551] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(1277), 3,
      anon_sym_LPAREN,
      anon_sym_RPAREN,
      sym__newline,
    ACTIONS(1275), 7,
      ts_builtin_sym_end,
      anon_sym_LBRACK,
      anon_sym_LPAREN_LPAREN,
      anon_sym_RPAREN_RPAREN,
      anon_sym_LBRACE_LBRACE,
      aux_sym_mmap_class_token1,
      aux_sym_mmap_icon_token1,
  [14572] = 3,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1281), 3,
      aux_sym_flow_link_arrow_start_token1,
      aux_sym_flow_link_arrow_start_token2,
      aux_sym_flow_link_arrow_start_token3,
    ACTIONS(1279), 7,
      ts_builtin_sym_end,
      anon_sym_SEMI,
      anon_sym_AMP,
      sym__newline,
      aux_sym_flow_link_arrow_token1,
      aux_sym_flow_link_arrow_token2,
      aux_sym_flow_link_arrow_token3,
  [14591] = 3,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1285), 3,
      aux_sym_flow_link_arrow_start_token1,
      aux_sym_flow_link_arrow_start_token2,
      aux_sym_flow_link_arrow_start_token3,
    ACTIONS(1283), 7,
      ts_builtin_sym_end,
      anon_sym_SEMI,
      anon_sym_AMP,
      sym__newline,
      aux_sym_flow_link_arrow_token1,
      aux_sym_flow_link_arrow_token2,
      aux_sym_flow_link_arrow_token3,
  [14610] = 9,
    ACTIONS(946), 1,
      aux_sym_flow_stmt_direction_token1,
    ACTIONS(948), 1,
      anon_sym_subgraph,
    ACTIONS(952), 1,
      sym__alpha_num_token,
    STATE(62), 1,
      sym_flow_vertex_id,
    STATE(218), 1,
      sym_flow_node,
    STATE(305), 1,
      sym_flow_vertex,
    STATE(477), 1,
      sym__flow_stmt,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    STATE(618), 3,
      sym_flow_stmt_direction,
      sym_flow_stmt_vertice,
      sym_flow_stmt_subgraph,
  [14641] = 3,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1204), 3,
      aux_sym_flow_link_arrow_start_token1,
      aux_sym_flow_link_arrow_start_token2,
      aux_sym_flow_link_arrow_start_token3,
    ACTIONS(1199), 7,
      ts_builtin_sym_end,
      anon_sym_SEMI,
      anon_sym_AMP,
      sym__newline,
      aux_sym_flow_link_arrow_token1,
      aux_sym_flow_link_arrow_token2,
      aux_sym_flow_link_arrow_token3,
  [14660] = 3,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1289), 3,
      aux_sym_flow_link_arrow_start_token1,
      aux_sym_flow_link_arrow_start_token2,
      aux_sym_flow_link_arrow_start_token3,
    ACTIONS(1287), 7,
      ts_builtin_sym_end,
      anon_sym_SEMI,
      anon_sym_AMP,
      sym__newline,
      aux_sym_flow_link_arrow_token1,
      aux_sym_flow_link_arrow_token2,
      aux_sym_flow_link_arrow_token3,
  [14679] = 3,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1293), 3,
      aux_sym_flow_link_arrow_start_token1,
      aux_sym_flow_link_arrow_start_token2,
      aux_sym_flow_link_arrow_start_token3,
    ACTIONS(1291), 7,
      ts_builtin_sym_end,
      anon_sym_SEMI,
      anon_sym_AMP,
      sym__newline,
      aux_sym_flow_link_arrow_token1,
      aux_sym_flow_link_arrow_token2,
      aux_sym_flow_link_arrow_token3,
  [14698] = 3,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1297), 3,
      aux_sym_flow_link_arrow_start_token1,
      aux_sym_flow_link_arrow_start_token2,
      aux_sym_flow_link_arrow_start_token3,
    ACTIONS(1295), 7,
      ts_builtin_sym_end,
      anon_sym_SEMI,
      anon_sym_AMP,
      sym__newline,
      aux_sym_flow_link_arrow_token1,
      aux_sym_flow_link_arrow_token2,
      aux_sym_flow_link_arrow_token3,
  [14717] = 3,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1301), 3,
      aux_sym_flow_link_arrow_start_token1,
      aux_sym_flow_link_arrow_start_token2,
      aux_sym_flow_link_arrow_start_token3,
    ACTIONS(1299), 7,
      ts_builtin_sym_end,
      anon_sym_SEMI,
      anon_sym_AMP,
      sym__newline,
      aux_sym_flow_link_arrow_token1,
      aux_sym_flow_link_arrow_token2,
      aux_sym_flow_link_arrow_token3,
  [14736] = 3,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1305), 3,
      aux_sym_flow_link_arrow_start_token1,
      aux_sym_flow_link_arrow_start_token2,
      aux_sym_flow_link_arrow_start_token3,
    ACTIONS(1303), 7,
      ts_builtin_sym_end,
      anon_sym_SEMI,
      anon_sym_AMP,
      sym__newline,
      aux_sym_flow_link_arrow_token1,
      aux_sym_flow_link_arrow_token2,
      aux_sym_flow_link_arrow_token3,
  [14755] = 6,
    ACTIONS(1311), 1,
      anon_sym_STAR,
    ACTIONS(1313), 1,
      anon_sym_DOLLAR,
    STATE(351), 1,
      aux_sym_class_method_line_repeat1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1307), 3,
      ts_builtin_sym_end,
      anon_sym_RBRACE,
      sym__newline,
    ACTIONS(1309), 3,
      anon_sym_LPAREN,
      anon_sym_RPAREN,
      sym__alpha_num_token,
  [14779] = 6,
    ACTIONS(1317), 1,
      anon_sym_STAR,
    ACTIONS(1319), 1,
      anon_sym_DOLLAR,
    STATE(351), 1,
      aux_sym_class_method_line_repeat1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1309), 3,
      anon_sym_LPAREN,
      anon_sym_RPAREN,
      sym__alpha_num_token,
    ACTIONS(1315), 3,
      ts_builtin_sym_end,
      anon_sym_RBRACE,
      sym__newline,
  [14803] = 6,
    ACTIONS(1323), 1,
      anon_sym_STAR,
    ACTIONS(1325), 1,
      anon_sym_DOLLAR,
    STATE(351), 1,
      aux_sym_class_method_line_repeat1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1309), 3,
      anon_sym_LPAREN,
      anon_sym_RPAREN,
      sym__alpha_num_token,
    ACTIONS(1321), 3,
      ts_builtin_sym_end,
      anon_sym_RBRACE,
      sym__newline,
  [14827] = 9,
    ACTIONS(1080), 1,
      anon_sym_TILDE,
    ACTIONS(1084), 1,
      anon_sym_PLUS,
    ACTIONS(1086), 1,
      anon_sym_DASH,
    ACTIONS(1088), 1,
      anon_sym_POUND,
    ACTIONS(1094), 1,
      sym__alpha_num_token,
    STATE(341), 1,
      aux_sym_class_method_line_repeat1,
    STATE(739), 1,
      sym_class_method_line,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1090), 2,
      anon_sym_LPAREN,
      anon_sym_RPAREN,
  [14857] = 6,
    ACTIONS(1329), 1,
      anon_sym_STAR,
    ACTIONS(1331), 1,
      anon_sym_DOLLAR,
    STATE(351), 1,
      aux_sym_class_method_line_repeat1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1309), 3,
      anon_sym_LPAREN,
      anon_sym_RPAREN,
      sym__alpha_num_token,
    ACTIONS(1327), 3,
      ts_builtin_sym_end,
      anon_sym_RBRACE,
      sym__newline,
  [14881] = 6,
    ACTIONS(1335), 1,
      anon_sym_STAR,
    ACTIONS(1337), 1,
      anon_sym_DOLLAR,
    STATE(351), 1,
      aux_sym_class_method_line_repeat1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1309), 3,
      anon_sym_LPAREN,
      anon_sym_RPAREN,
      sym__alpha_num_token,
    ACTIONS(1333), 3,
      ts_builtin_sym_end,
      anon_sym_RBRACE,
      sym__newline,
  [14905] = 3,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1341), 3,
      sym__class_name,
      sym_class_reltype_aggregation,
      anon_sym_LT,
    ACTIONS(1339), 6,
      anon_sym_STAR,
      anon_sym_GT,
      sym__bquote_string,
      sym__dquote_string,
      anon_sym_LT_PIPE,
      anon_sym_PIPE_GT,
  [14923] = 3,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1343), 3,
      aux_sym_flow_link_arrow_start_token1,
      aux_sym_flow_link_arrow_start_token2,
      aux_sym_flow_link_arrow_start_token3,
    ACTIONS(821), 6,
      ts_builtin_sym_end,
      anon_sym_SEMI,
      sym__newline,
      aux_sym_flow_link_arrow_token1,
      aux_sym_flow_link_arrow_token2,
      aux_sym_flow_link_arrow_token3,
  [14941] = 8,
    ACTIONS(1345), 1,
      ts_builtin_sym_end,
    ACTIONS(1347), 1,
      aux_sym_sequence_stmt_title_token1,
    ACTIONS(1350), 1,
      sym__newline,
    ACTIONS(1353), 1,
      aux_sym_pie_label_token1,
    STATE(840), 1,
      sym_pie_label,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    STATE(345), 2,
      sym__pie_stmt,
      aux_sym_diagram_pie_repeat1,
    STATE(433), 2,
      sym_pie_stmt_title,
      sym_pie_stmt_element,
  [14969] = 8,
    ACTIONS(1210), 1,
      aux_sym_sequence_stmt_title_token1,
    ACTIONS(1216), 1,
      aux_sym_pie_label_token1,
    ACTIONS(1356), 1,
      ts_builtin_sym_end,
    ACTIONS(1358), 1,
      sym__newline,
    STATE(840), 1,
      sym_pie_label,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    STATE(345), 2,
      sym__pie_stmt,
      aux_sym_diagram_pie_repeat1,
    STATE(433), 2,
      sym_pie_stmt_title,
      sym_pie_stmt_element,
  [14997] = 8,
    ACTIONS(1210), 1,
      aux_sym_sequence_stmt_title_token1,
    ACTIONS(1216), 1,
      aux_sym_pie_label_token1,
    ACTIONS(1356), 1,
      ts_builtin_sym_end,
    ACTIONS(1360), 1,
      sym__newline,
    STATE(840), 1,
      sym_pie_label,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    STATE(352), 2,
      sym__pie_stmt,
      aux_sym_diagram_pie_repeat1,
    STATE(433), 2,
      sym_pie_stmt_title,
      sym_pie_stmt_element,
  [15025] = 9,
    ACTIONS(1362), 1,
      sym__newline,
    ACTIONS(1364), 1,
      aux_sym_pie_label_token1,
    ACTIONS(1366), 1,
      aux_sym_er_attribute_key_type_pk_token1,
    ACTIONS(1368), 1,
      aux_sym_er_attribute_key_type_fk_token1,
    STATE(473), 1,
      sym__er_attribute_key_type,
    STATE(881), 1,
      sym__er_word,
    STATE(882), 1,
      sym_er_attribute_comment,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    STATE(677), 2,
      sym_er_attribute_key_type_pk,
      sym_er_attribute_key_type_fk,
  [15055] = 3,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1372), 3,
      sym__class_name,
      sym_class_reltype_aggregation,
      anon_sym_LT,
    ACTIONS(1370), 6,
      anon_sym_STAR,
      anon_sym_GT,
      sym__bquote_string,
      sym__dquote_string,
      anon_sym_LT_PIPE,
      anon_sym_PIPE_GT,
  [15073] = 3,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1376), 3,
      sym__class_name,
      sym_class_reltype_aggregation,
      anon_sym_LT,
    ACTIONS(1374), 6,
      anon_sym_STAR,
      anon_sym_GT,
      sym__bquote_string,
      sym__dquote_string,
      anon_sym_LT_PIPE,
      anon_sym_PIPE_GT,
  [15091] = 4,
    STATE(351), 1,
      aux_sym_class_method_line_repeat1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1380), 3,
      anon_sym_LPAREN,
      anon_sym_RPAREN,
      sym__alpha_num_token,
    ACTIONS(1378), 5,
      ts_builtin_sym_end,
      anon_sym_RBRACE,
      anon_sym_STAR,
      anon_sym_DOLLAR,
      sym__newline,
  [15111] = 8,
    ACTIONS(1210), 1,
      aux_sym_sequence_stmt_title_token1,
    ACTIONS(1216), 1,
      aux_sym_pie_label_token1,
    ACTIONS(1358), 1,
      sym__newline,
    ACTIONS(1383), 1,
      ts_builtin_sym_end,
    STATE(840), 1,
      sym_pie_label,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    STATE(345), 2,
      sym__pie_stmt,
      aux_sym_diagram_pie_repeat1,
    STATE(433), 2,
      sym_pie_stmt_title,
      sym_pie_stmt_element,
  [15139] = 8,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(69), 1,
      sym__sequence_actor_word,
    ACTIONS(1385), 1,
      anon_sym_PLUS,
    ACTIONS(1387), 1,
      anon_sym_DASH,
    STATE(257), 1,
      aux_sym_sequence_actor_repeat1,
    STATE(894), 1,
      sym_sequence_actor,
    STATE(483), 2,
      sym_sequence_signal_plus_sign,
      sym_sequence_signal_minus_sign,
  [15165] = 8,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(69), 1,
      sym__sequence_actor_word,
    ACTIONS(1385), 1,
      anon_sym_PLUS,
    ACTIONS(1387), 1,
      anon_sym_DASH,
    STATE(257), 1,
      aux_sym_sequence_actor_repeat1,
    STATE(878), 1,
      sym_sequence_actor,
    STATE(489), 2,
      sym_sequence_signal_plus_sign,
      sym_sequence_signal_minus_sign,
  [15191] = 2,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1389), 7,
      anon_sym_PIPEo,
      anon_sym_o_PIPE,
      anon_sym_RBRACEo,
      anon_sym_o_LBRACE,
      anon_sym_RBRACE_PIPE,
      anon_sym_PIPE_LBRACE,
      sym_er_cardinarity_only_one,
  [15205] = 3,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1050), 2,
      sym__class_name,
      sym__bquote_string,
    ACTIONS(1056), 5,
      ts_builtin_sym_end,
      anon_sym_TILDE,
      anon_sym_COLON_COLON_COLON,
      anon_sym_LBRACE,
      sym__newline,
  [15221] = 3,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1050), 2,
      sym__class_name,
      sym__bquote_string,
    ACTIONS(1044), 5,
      ts_builtin_sym_end,
      anon_sym_TILDE,
      anon_sym_COLON_COLON_COLON,
      anon_sym_LBRACE,
      sym__newline,
  [15237] = 2,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1391), 7,
      anon_sym_PIPEo,
      anon_sym_o_PIPE,
      anon_sym_RBRACEo,
      anon_sym_o_LBRACE,
      anon_sym_RBRACE_PIPE,
      anon_sym_PIPE_LBRACE,
      sym_er_cardinarity_only_one,
  [15251] = 8,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(69), 1,
      sym__sequence_actor_word,
    ACTIONS(1385), 1,
      anon_sym_PLUS,
    ACTIONS(1387), 1,
      anon_sym_DASH,
    STATE(257), 1,
      aux_sym_sequence_actor_repeat1,
    STATE(795), 1,
      sym_sequence_actor,
    STATE(583), 2,
      sym_sequence_signal_plus_sign,
      sym_sequence_signal_minus_sign,
  [15277] = 2,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1393), 7,
      anon_sym_PIPEo,
      anon_sym_o_PIPE,
      anon_sym_RBRACEo,
      anon_sym_o_LBRACE,
      anon_sym_RBRACE_PIPE,
      anon_sym_PIPE_LBRACE,
      sym_er_cardinarity_only_one,
  [15291] = 6,
    ACTIONS(1395), 1,
      ts_builtin_sym_end,
    ACTIONS(1397), 1,
      sym__er_alphanum,
    STATE(221), 1,
      sym_er_entity_name,
    STATE(658), 1,
      sym__er_stmt,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    STATE(683), 3,
      sym_er_stmt_entity,
      sym_er_stmt_entity_relation,
      sym_er_stmt_entity_block,
  [15313] = 6,
    ACTIONS(1397), 1,
      sym__er_alphanum,
    ACTIONS(1399), 1,
      sym__newline,
    STATE(221), 1,
      sym_er_entity_name,
    STATE(648), 1,
      sym__er_stmt,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    STATE(683), 3,
      sym_er_stmt_entity,
      sym_er_stmt_entity_relation,
      sym_er_stmt_entity_block,
  [15335] = 5,
    ACTIONS(1401), 1,
      anon_sym_DASH_DASH,
    STATE(298), 1,
      sym__er_reltype,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    STATE(358), 2,
      sym_er_reltype_non_identifying,
      sym_er_reltype_identifying,
    ACTIONS(1403), 3,
      anon_sym_DOT_DOT,
      anon_sym_DOT_DASH,
      anon_sym_DASH_DOT,
  [15355] = 6,
    ACTIONS(1397), 1,
      sym__er_alphanum,
    ACTIONS(1405), 1,
      ts_builtin_sym_end,
    STATE(221), 1,
      sym_er_entity_name,
    STATE(658), 1,
      sym__er_stmt,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    STATE(683), 3,
      sym_er_stmt_entity,
      sym_er_stmt_entity_relation,
      sym_er_stmt_entity_block,
  [15377] = 6,
    ACTIONS(1397), 1,
      sym__er_alphanum,
    ACTIONS(1407), 1,
      ts_builtin_sym_end,
    STATE(221), 1,
      sym_er_entity_name,
    STATE(658), 1,
      sym__er_stmt,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    STATE(683), 3,
      sym_er_stmt_entity,
      sym_er_stmt_entity_relation,
      sym_er_stmt_entity_block,
  [15399] = 8,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(69), 1,
      sym__sequence_actor_word,
    ACTIONS(1385), 1,
      anon_sym_PLUS,
    ACTIONS(1387), 1,
      anon_sym_DASH,
    STATE(257), 1,
      aux_sym_sequence_actor_repeat1,
    STATE(862), 1,
      sym_sequence_actor,
    STATE(500), 2,
      sym_sequence_signal_plus_sign,
      sym_sequence_signal_minus_sign,
  [15425] = 6,
    ACTIONS(1409), 1,
      anon_sym_RBRACE,
    ACTIONS(1411), 1,
      sym__er_alphanum,
    STATE(730), 1,
      sym_er_attribute_type,
    STATE(760), 1,
      sym_er_stmt_entity_block_inner,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    STATE(415), 2,
      sym_er_attribute,
      aux_sym_er_stmt_entity_block_inner_repeat1,
  [15446] = 3,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1415), 2,
      anon_sym_subgraph,
      sym__alpha_num_token,
    ACTIONS(1413), 4,
      ts_builtin_sym_end,
      anon_sym_SEMI,
      aux_sym_flow_stmt_direction_token1,
      sym__newline,
  [15461] = 4,
    ACTIONS(1419), 1,
      sym__alpha_num_token,
    STATE(369), 1,
      aux_sym_flow_arrow_text_repeat1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1417), 4,
      anon_sym_SEMI,
      anon_sym_PIPE,
      anon_sym_RBRACK,
      sym__newline,
  [15478] = 3,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1050), 2,
      sym__class_name,
      sym__bquote_string,
    ACTIONS(1044), 4,
      ts_builtin_sym_end,
      anon_sym_COLON,
      anon_sym_TILDE,
      sym__newline,
  [15493] = 3,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1424), 2,
      anon_sym_subgraph,
      sym__alpha_num_token,
    ACTIONS(1422), 4,
      ts_builtin_sym_end,
      anon_sym_SEMI,
      aux_sym_flow_stmt_direction_token1,
      sym__newline,
  [15508] = 5,
    ACTIONS(1397), 1,
      sym__er_alphanum,
    STATE(221), 1,
      sym_er_entity_name,
    STATE(658), 1,
      sym__er_stmt,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    STATE(683), 3,
      sym_er_stmt_entity,
      sym_er_stmt_entity_relation,
      sym_er_stmt_entity_block,
  [15527] = 6,
    ACTIONS(1426), 1,
      aux_sym_sequence_stmt_note_token2,
    ACTIONS(1428), 1,
      aux_sym_note_placement_left_token1,
    ACTIONS(1430), 1,
      aux_sym_note_placement_right_token1,
    STATE(512), 1,
      sym_sequence_note_placement,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    STATE(888), 2,
      sym_note_placement_left,
      sym_note_placement_right,
  [15548] = 3,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1434), 2,
      anon_sym_subgraph,
      sym__alpha_num_token,
    ACTIONS(1432), 4,
      ts_builtin_sym_end,
      anon_sym_SEMI,
      aux_sym_flow_stmt_direction_token1,
      sym__newline,
  [15563] = 6,
    ACTIONS(1428), 1,
      aux_sym_note_placement_left_token1,
    ACTIONS(1430), 1,
      aux_sym_note_placement_right_token1,
    ACTIONS(1436), 1,
      aux_sym_sequence_stmt_note_token2,
    STATE(485), 1,
      sym_sequence_note_placement,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    STATE(888), 2,
      sym_note_placement_left,
      sym_note_placement_right,
  [15584] = 3,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1440), 2,
      anon_sym_subgraph,
      sym__alpha_num_token,
    ACTIONS(1438), 4,
      ts_builtin_sym_end,
      anon_sym_SEMI,
      aux_sym_flow_stmt_direction_token1,
      sym__newline,
  [15599] = 3,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1444), 2,
      anon_sym_subgraph,
      sym__alpha_num_token,
    ACTIONS(1442), 4,
      ts_builtin_sym_end,
      anon_sym_SEMI,
      aux_sym_flow_stmt_direction_token1,
      sym__newline,
  [15614] = 7,
    ACTIONS(1020), 1,
      sym__bquote_string,
    ACTIONS(1446), 1,
      sym__class_name,
    ACTIONS(1448), 1,
      sym__dquote_string,
    STATE(226), 1,
      sym_class_name_body,
    STATE(624), 1,
      sym_class_name,
    STATE(631), 1,
      aux_sym_class_name_body_repeat1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [15637] = 8,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(797), 1,
      aux_sym_mmap_class_token1,
    ACTIONS(799), 1,
      aux_sym_mmap_icon_token1,
    ACTIONS(1450), 1,
      ts_builtin_sym_end,
    ACTIONS(1452), 1,
      sym__newline,
    STATE(474), 1,
      sym_mmap_icon,
    STATE(662), 1,
      sym_mmap_class,
  [15662] = 7,
    ACTIONS(1020), 1,
      sym__bquote_string,
    ACTIONS(1446), 1,
      sym__class_name,
    ACTIONS(1454), 1,
      sym__dquote_string,
    STATE(226), 1,
      sym_class_name_body,
    STATE(552), 1,
      sym_class_name,
    STATE(631), 1,
      aux_sym_class_name_body_repeat1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [15685] = 5,
    ACTIONS(1397), 1,
      sym__er_alphanum,
    STATE(221), 1,
      sym_er_entity_name,
    STATE(616), 1,
      sym__er_stmt,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    STATE(683), 3,
      sym_er_stmt_entity,
      sym_er_stmt_entity_relation,
      sym_er_stmt_entity_block,
  [15704] = 6,
    ACTIONS(1458), 1,
      sym__alpha_num_token,
    STATE(392), 1,
      aux_sym_flow_arrow_text_repeat1,
    STATE(746), 1,
      sym_flow_vertex_text,
    STATE(784), 1,
      sym_flow_vertex_id,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1456), 2,
      anon_sym_SEMI,
      sym__newline,
  [15725] = 3,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1050), 2,
      sym__class_name,
      sym__bquote_string,
    ACTIONS(1056), 4,
      ts_builtin_sym_end,
      anon_sym_COLON,
      anon_sym_TILDE,
      sym__newline,
  [15740] = 4,
    ACTIONS(1460), 1,
      anon_sym_TILDE,
    STATE(471), 1,
      sym_class_generics,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(895), 4,
      ts_builtin_sym_end,
      anon_sym_COLON_COLON_COLON,
      anon_sym_LBRACE,
      sym__newline,
  [15757] = 6,
    ACTIONS(1428), 1,
      aux_sym_note_placement_left_token1,
    ACTIONS(1430), 1,
      aux_sym_note_placement_right_token1,
    ACTIONS(1462), 1,
      aux_sym_sequence_stmt_note_token2,
    STATE(495), 1,
      sym_sequence_note_placement,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    STATE(888), 2,
      sym_note_placement_left,
      sym_note_placement_right,
  [15778] = 6,
    ACTIONS(1428), 1,
      aux_sym_note_placement_left_token1,
    ACTIONS(1430), 1,
      aux_sym_note_placement_right_token1,
    ACTIONS(1464), 1,
      aux_sym_sequence_stmt_note_token2,
    STATE(653), 1,
      sym_sequence_note_placement,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    STATE(888), 2,
      sym_note_placement_left,
      sym_note_placement_right,
  [15799] = 5,
    ACTIONS(1466), 1,
      aux_sym_flow_text_literal_token1,
    ACTIONS(1468), 1,
      sym_flow_text_quoted,
    STATE(299), 1,
      aux_sym_flow_text_literal_repeat1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    STATE(856), 2,
      sym__flow_text,
      sym_flow_text_literal,
  [15817] = 5,
    ACTIONS(1466), 1,
      aux_sym_flow_text_literal_token1,
    ACTIONS(1470), 1,
      sym_flow_text_quoted,
    STATE(299), 1,
      aux_sym_flow_text_literal_repeat1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    STATE(825), 2,
      sym__flow_text,
      sym_flow_text_literal,
  [15835] = 2,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1472), 5,
      anon_sym_DASH_DASH,
      anon_sym_DOT_DOT,
      sym__er_alphanum,
      anon_sym_DOT_DASH,
      anon_sym_DASH_DOT,
  [15847] = 6,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(1474), 1,
      aux_sym_mmap_text_token1,
    ACTIONS(1476), 1,
      sym__md_start,
    STATE(912), 1,
      sym_mmap_node_content,
    STATE(464), 2,
      sym_mmap_markdown,
      sym_mmap_text,
  [15867] = 5,
    ACTIONS(1466), 1,
      aux_sym_flow_text_literal_token1,
    ACTIONS(1478), 1,
      sym_flow_text_quoted,
    STATE(299), 1,
      aux_sym_flow_text_literal_repeat1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    STATE(750), 2,
      sym__flow_text,
      sym_flow_text_literal,
  [15885] = 4,
    ACTIONS(1482), 1,
      sym__alpha_num_token,
    STATE(369), 1,
      aux_sym_flow_arrow_text_repeat1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1480), 3,
      anon_sym_SEMI,
      anon_sym_RBRACK,
      sym__newline,
  [15901] = 2,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1044), 5,
      ts_builtin_sym_end,
      anon_sym_TILDE,
      anon_sym_COLON_COLON_COLON,
      anon_sym_LBRACE,
      sym__newline,
  [15913] = 2,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1484), 5,
      anon_sym_DASH_DASH,
      anon_sym_DOT_DOT,
      sym__er_alphanum,
      anon_sym_DOT_DASH,
      anon_sym_DASH_DOT,
  [15925] = 5,
    ACTIONS(1486), 1,
      aux_sym_flow_text_literal_token1,
    ACTIONS(1488), 1,
      sym_flow_text_quoted,
    STATE(404), 1,
      aux_sym_flow_text_literal_repeat1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    STATE(752), 2,
      sym__flow_text,
      sym_flow_text_literal,
  [15943] = 5,
    ACTIONS(1486), 1,
      aux_sym_flow_text_literal_token1,
    ACTIONS(1490), 1,
      sym_flow_text_quoted,
    STATE(404), 1,
      aux_sym_flow_text_literal_repeat1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    STATE(753), 2,
      sym__flow_text,
      sym_flow_text_literal,
  [15961] = 5,
    ACTIONS(1466), 1,
      aux_sym_flow_text_literal_token1,
    ACTIONS(1492), 1,
      sym_flow_text_quoted,
    STATE(299), 1,
      aux_sym_flow_text_literal_repeat1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    STATE(754), 2,
      sym__flow_text,
      sym_flow_text_literal,
  [15979] = 2,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1056), 5,
      ts_builtin_sym_end,
      anon_sym_TILDE,
      anon_sym_COLON_COLON_COLON,
      anon_sym_LBRACE,
      sym__newline,
  [15991] = 5,
    ACTIONS(1466), 1,
      aux_sym_flow_text_literal_token1,
    ACTIONS(1494), 1,
      sym_flow_text_quoted,
    STATE(299), 1,
      aux_sym_flow_text_literal_repeat1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    STATE(738), 2,
      sym__flow_text,
      sym_flow_text_literal,
  [16009] = 2,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1496), 5,
      anon_sym_DASH_DASH,
      anon_sym_DOT_DOT,
      sym__er_alphanum,
      anon_sym_DOT_DASH,
      anon_sym_DASH_DOT,
  [16021] = 5,
    ACTIONS(817), 1,
      anon_sym_DASH_DASH,
    ACTIONS(819), 1,
      anon_sym_DOT_DOT,
    STATE(250), 1,
      sym__class_linetype,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    STATE(343), 2,
      sym_class_linetype_solid,
      sym_class_linetype_dotted,
  [16039] = 5,
    ACTIONS(1466), 1,
      aux_sym_flow_text_literal_token1,
    ACTIONS(1498), 1,
      sym_flow_text_quoted,
    STATE(299), 1,
      aux_sym_flow_text_literal_repeat1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    STATE(741), 2,
      sym__flow_text,
      sym_flow_text_literal,
  [16057] = 5,
    ACTIONS(1500), 1,
      anon_sym_RBRACE,
    ACTIONS(1502), 1,
      sym__er_alphanum,
    STATE(730), 1,
      sym_er_attribute_type,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    STATE(403), 2,
      sym_er_attribute,
      aux_sym_er_stmt_entity_block_inner_repeat1,
  [16075] = 4,
    ACTIONS(1505), 1,
      aux_sym_flow_text_literal_token1,
    STATE(417), 1,
      aux_sym_flow_text_literal_repeat1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1183), 3,
      anon_sym_RPAREN_RPAREN,
      anon_sym_RPAREN_RBRACK,
      anon_sym_RBRACE_RBRACE,
  [16091] = 2,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1507), 5,
      sym__class_name,
      sym__bquote_string,
      sym__dquote_string,
      anon_sym_DASH_DASH,
      anon_sym_DOT_DOT,
  [16103] = 4,
    ACTIONS(1509), 1,
      sym__alpha_num_token,
    STATE(406), 1,
      aux_sym_flow_arrow_text_repeat1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1417), 3,
      aux_sym_flow_link_arrow_token1,
      aux_sym_flow_link_arrow_token2,
      aux_sym_flow_link_arrow_token3,
  [16119] = 5,
    ACTIONS(1466), 1,
      aux_sym_flow_text_literal_token1,
    ACTIONS(1512), 1,
      sym_flow_text_quoted,
    STATE(299), 1,
      aux_sym_flow_text_literal_repeat1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    STATE(819), 2,
      sym__flow_text,
      sym_flow_text_literal,
  [16137] = 6,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(1514), 1,
      aux_sym_mmap_text_token1,
    ACTIONS(1516), 1,
      sym__md_start,
    STATE(913), 1,
      sym_mmap_node_content,
    STATE(728), 2,
      sym_mmap_markdown,
      sym_mmap_text,
  [16157] = 2,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1518), 5,
      sym__class_name,
      sym__bquote_string,
      sym__dquote_string,
      anon_sym_DASH_DASH,
      anon_sym_DOT_DOT,
  [16169] = 6,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(1514), 1,
      aux_sym_mmap_text_token1,
    ACTIONS(1516), 1,
      sym__md_start,
    STATE(914), 1,
      sym_mmap_node_content,
    STATE(728), 2,
      sym_mmap_markdown,
      sym_mmap_text,
  [16189] = 6,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(1474), 1,
      aux_sym_mmap_text_token1,
    ACTIONS(1476), 1,
      sym__md_start,
    STATE(916), 1,
      sym_mmap_node_content,
    STATE(464), 2,
      sym_mmap_markdown,
      sym_mmap_text,
  [16209] = 6,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(1474), 1,
      aux_sym_mmap_text_token1,
    ACTIONS(1476), 1,
      sym__md_start,
    STATE(919), 1,
      sym_mmap_node_content,
    STATE(464), 2,
      sym_mmap_markdown,
      sym_mmap_text,
  [16229] = 6,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(1474), 1,
      aux_sym_mmap_text_token1,
    ACTIONS(1476), 1,
      sym__md_start,
    STATE(845), 1,
      sym_mmap_node_content,
    STATE(464), 2,
      sym_mmap_markdown,
      sym_mmap_text,
  [16249] = 2,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1520), 5,
      anon_sym_DASH_DASH,
      anon_sym_DOT_DOT,
      sym__er_alphanum,
      anon_sym_DOT_DASH,
      anon_sym_DASH_DOT,
  [16261] = 5,
    ACTIONS(1411), 1,
      sym__er_alphanum,
    ACTIONS(1522), 1,
      anon_sym_RBRACE,
    STATE(730), 1,
      sym_er_attribute_type,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    STATE(403), 2,
      sym_er_attribute,
      aux_sym_er_stmt_entity_block_inner_repeat1,
  [16279] = 5,
    ACTIONS(1466), 1,
      aux_sym_flow_text_literal_token1,
    ACTIONS(1524), 1,
      sym_flow_text_quoted,
    STATE(299), 1,
      aux_sym_flow_text_literal_repeat1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    STATE(796), 2,
      sym__flow_text,
      sym_flow_text_literal,
  [16297] = 4,
    ACTIONS(1526), 1,
      aux_sym_flow_text_literal_token1,
    STATE(417), 1,
      aux_sym_flow_text_literal_repeat1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1228), 3,
      anon_sym_RPAREN_RPAREN,
      anon_sym_RPAREN_RBRACK,
      anon_sym_RBRACE_RBRACE,
  [16313] = 5,
    ACTIONS(1486), 1,
      aux_sym_flow_text_literal_token1,
    ACTIONS(1529), 1,
      sym_flow_text_quoted,
    STATE(404), 1,
      aux_sym_flow_text_literal_repeat1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    STATE(806), 2,
      sym__flow_text,
      sym_flow_text_literal,
  [16331] = 6,
    ACTIONS(1020), 1,
      sym__bquote_string,
    ACTIONS(1446), 1,
      sym__class_name,
    STATE(226), 1,
      sym_class_name_body,
    STATE(510), 1,
      sym_class_name,
    STATE(631), 1,
      aux_sym_class_name_body_repeat1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [16351] = 2,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1531), 5,
      sym__class_name,
      sym__bquote_string,
      sym__dquote_string,
      anon_sym_DASH_DASH,
      anon_sym_DOT_DOT,
  [16363] = 5,
    ACTIONS(1533), 1,
      aux_sym_note_placement_left_token1,
    ACTIONS(1535), 1,
      aux_sym_note_placement_right_token1,
    STATE(584), 1,
      sym_state_note_placement,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    STATE(684), 2,
      sym_note_placement_left,
      sym_note_placement_right,
  [16381] = 5,
    ACTIONS(1466), 1,
      aux_sym_flow_text_literal_token1,
    ACTIONS(1537), 1,
      sym_flow_text_quoted,
    STATE(299), 1,
      aux_sym_flow_text_literal_repeat1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    STATE(801), 2,
      sym__flow_text,
      sym_flow_text_literal,
  [16399] = 6,
    ACTIONS(1020), 1,
      sym__bquote_string,
    ACTIONS(1539), 1,
      sym__class_name,
    STATE(226), 1,
      sym_class_name_body,
    STATE(613), 1,
      aux_sym_class_name_body_repeat1,
    STATE(708), 1,
      sym_class_name,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [16419] = 6,
    ACTIONS(1539), 1,
      sym__class_name,
    ACTIONS(1541), 1,
      sym__bquote_string,
    STATE(384), 1,
      sym_class_name_body,
    STATE(460), 1,
      sym_class_name,
    STATE(640), 1,
      aux_sym_class_name_body_repeat1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [16439] = 5,
    ACTIONS(1466), 1,
      aux_sym_flow_text_literal_token1,
    ACTIONS(1543), 1,
      sym_flow_text_quoted,
    STATE(299), 1,
      aux_sym_flow_text_literal_repeat1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    STATE(860), 2,
      sym__flow_text,
      sym_flow_text_literal,
  [16457] = 4,
    ACTIONS(1545), 1,
      sym__alpha_num_token,
    STATE(406), 1,
      aux_sym_flow_arrow_text_repeat1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1547), 3,
      aux_sym_flow_link_arrow_token1,
      aux_sym_flow_link_arrow_token2,
      aux_sym_flow_link_arrow_token3,
  [16473] = 6,
    ACTIONS(1020), 1,
      sym__bquote_string,
    ACTIONS(1446), 1,
      sym__class_name,
    STATE(226), 1,
      sym_class_name_body,
    STATE(552), 1,
      sym_class_name,
    STATE(631), 1,
      aux_sym_class_name_body_repeat1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [16493] = 5,
    ACTIONS(1533), 1,
      aux_sym_note_placement_left_token1,
    ACTIONS(1535), 1,
      aux_sym_note_placement_right_token1,
    STATE(649), 1,
      sym_state_note_placement,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    STATE(684), 2,
      sym_note_placement_left,
      sym_note_placement_right,
  [16511] = 5,
    ACTIONS(1549), 1,
      sym__dquote_string,
    STATE(217), 1,
      sym_state_alias,
    STATE(238), 1,
      sym_state_name,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(572), 2,
      anon_sym_LBRACK_STAR_RBRACK,
      aux_sym_state_name_token1,
  [16529] = 5,
    ACTIONS(871), 1,
      anon_sym_COLON,
    ACTIONS(875), 1,
      anon_sym_DASH_DASH_GT,
    STATE(642), 1,
      sym_state_arrow,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(873), 2,
      ts_builtin_sym_end,
      sym__newline,
  [16547] = 5,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(1553), 1,
      sym_pie_title,
    ACTIONS(1555), 1,
      aux_sym_pie_label_token1,
    ACTIONS(1551), 3,
      ts_builtin_sym_end,
      aux_sym_sequence_stmt_title_token1,
      sym__newline,
  [16565] = 2,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1557), 5,
      sym__class_name,
      sym__bquote_string,
      sym__dquote_string,
      anon_sym_DASH_DASH,
      anon_sym_DOT_DOT,
  [16577] = 2,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1559), 4,
      ts_builtin_sym_end,
      aux_sym_sequence_stmt_title_token1,
      sym__newline,
      aux_sym_pie_label_token1,
  [16588] = 2,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1561), 4,
      ts_builtin_sym_end,
      aux_sym_sequence_stmt_title_token1,
      sym__newline,
      aux_sym_pie_label_token1,
  [16599] = 3,
    ACTIONS(1563), 1,
      aux_sym_flow_stmt_direction_token1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1164), 3,
      anon_sym_subgraph,
      anon_sym_end,
      sym__alpha_num_token,
  [16612] = 5,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(905), 1,
      aux_sym_class_label_token1,
    STATE(234), 1,
      sym_state_description,
    ACTIONS(901), 2,
      ts_builtin_sym_end,
      sym__newline,
  [16629] = 5,
    ACTIONS(1565), 1,
      sym__alpha_num_token,
    STATE(62), 1,
      sym_flow_vertex_id,
    STATE(305), 1,
      sym_flow_vertex,
    STATE(344), 1,
      sym_flow_node,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [16646] = 4,
    ACTIONS(1206), 1,
      ts_builtin_sym_end,
    STATE(440), 1,
      aux_sym_diagram_flow_repeat1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1567), 2,
      anon_sym_SEMI,
      sym__newline,
  [16661] = 5,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(1571), 1,
      aux_sym_class_label_token1,
    STATE(707), 1,
      sym_class_label,
    ACTIONS(1569), 2,
      ts_builtin_sym_end,
      sym__newline,
  [16678] = 4,
    ACTIONS(1573), 1,
      ts_builtin_sym_end,
    STATE(440), 1,
      aux_sym_diagram_flow_repeat1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1575), 2,
      anon_sym_SEMI,
      sym__newline,
  [16693] = 3,
    STATE(849), 1,
      sym_flow_link_arrow,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(831), 3,
      aux_sym_flow_link_arrow_token1,
      aux_sym_flow_link_arrow_token2,
      aux_sym_flow_link_arrow_token3,
  [16706] = 3,
    STATE(339), 1,
      aux_sym_class_method_line_repeat1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1578), 3,
      anon_sym_LPAREN,
      anon_sym_RPAREN,
      sym__alpha_num_token,
  [16719] = 3,
    STATE(338), 1,
      aux_sym_class_method_line_repeat1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1580), 3,
      anon_sym_LPAREN,
      anon_sym_RPAREN,
      sym__alpha_num_token,
  [16732] = 3,
    STATE(337), 1,
      aux_sym_class_method_line_repeat1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1582), 3,
      anon_sym_LPAREN,
      anon_sym_RPAREN,
      sym__alpha_num_token,
  [16745] = 3,
    STATE(342), 1,
      aux_sym_class_method_line_repeat1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1584), 3,
      anon_sym_LPAREN,
      anon_sym_RPAREN,
      sym__alpha_num_token,
  [16758] = 4,
    ACTIONS(1222), 1,
      ts_builtin_sym_end,
    STATE(440), 1,
      aux_sym_diagram_flow_repeat1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1586), 2,
      anon_sym_SEMI,
      sym__newline,
  [16773] = 6,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(1588), 1,
      sym__newline,
    ACTIONS(1590), 1,
      aux_sym__sequence_rest_text_token1,
    STATE(907), 1,
      sym__sequence_rest_text,
    STATE(920), 1,
      sym_sequence_text,
  [16792] = 2,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1592), 4,
      ts_builtin_sym_end,
      aux_sym_sequence_stmt_title_token1,
      sym__newline,
      aux_sym_pie_label_token1,
  [16803] = 6,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(1590), 1,
      aux_sym__sequence_rest_text_token1,
    ACTIONS(1594), 1,
      sym__newline,
    STATE(859), 1,
      sym_sequence_text,
    STATE(907), 1,
      sym__sequence_rest_text,
  [16822] = 2,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1596), 4,
      anon_sym_LPAREN,
      anon_sym_RPAREN,
      anon_sym_RBRACK,
      anon_sym_RBRACE_RBRACE,
  [16833] = 5,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(1571), 1,
      aux_sym_class_label_token1,
    STATE(668), 1,
      sym_class_label,
    ACTIONS(1598), 2,
      ts_builtin_sym_end,
      sym__newline,
  [16850] = 4,
    ACTIONS(1600), 1,
      ts_builtin_sym_end,
    STATE(446), 1,
      aux_sym_diagram_flow_repeat1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1602), 2,
      anon_sym_SEMI,
      sym__newline,
  [16865] = 6,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(1590), 1,
      aux_sym__sequence_rest_text_token1,
    ACTIONS(1604), 1,
      sym__newline,
    STATE(851), 1,
      sym_sequence_text,
    STATE(907), 1,
      sym__sequence_rest_text,
  [16884] = 2,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1102), 4,
      ts_builtin_sym_end,
      anon_sym_COLON_COLON_COLON,
      anon_sym_LBRACE,
      sym__newline,
  [16895] = 4,
    STATE(681), 1,
      sym_er_role,
    STATE(687), 1,
      sym__er_word,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1606), 2,
      aux_sym_pie_label_token1,
      sym__er_alphanum,
  [16910] = 6,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(1590), 1,
      aux_sym__sequence_rest_text_token1,
    ACTIONS(1608), 1,
      sym__newline,
    STATE(875), 1,
      sym_sequence_text,
    STATE(907), 1,
      sym__sequence_rest_text,
  [16929] = 2,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1610), 4,
      sym__newline,
      aux_sym_pie_label_token1,
      aux_sym_er_attribute_key_type_pk_token1,
      aux_sym_er_attribute_key_type_fk_token1,
  [16940] = 6,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(1590), 1,
      aux_sym__sequence_rest_text_token1,
    ACTIONS(1612), 1,
      sym__newline,
    STATE(891), 1,
      sym_sequence_text,
    STATE(907), 1,
      sym__sequence_rest_text,
  [16959] = 5,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(1614), 1,
      sym__sequence_actor_word,
    STATE(461), 1,
      aux_sym_sequence_actor_repeat1,
    ACTIONS(1038), 2,
      aux_sym_sequence_stmt_participant_token1,
      sym__newline,
  [16976] = 4,
    ACTIONS(1618), 1,
      anon_sym_COLON_COLON_COLON,
    ACTIONS(1620), 1,
      anon_sym_LBRACE,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1616), 2,
      ts_builtin_sym_end,
      sym__newline,
  [16991] = 5,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(1622), 1,
      sym__sequence_actor_word,
    STATE(461), 1,
      aux_sym_sequence_actor_repeat1,
    ACTIONS(911), 2,
      aux_sym_sequence_stmt_participant_token1,
      sym__newline,
  [17008] = 2,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1625), 4,
      ts_builtin_sym_end,
      aux_sym_sequence_stmt_title_token1,
      sym__newline,
      aux_sym_pie_label_token1,
  [17019] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(1629), 1,
      sym__newline,
    ACTIONS(1627), 3,
      ts_builtin_sym_end,
      aux_sym_mmap_class_token1,
      aux_sym_mmap_icon_token1,
  [17034] = 2,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1631), 4,
      anon_sym_LPAREN,
      anon_sym_RPAREN,
      anon_sym_RBRACK,
      anon_sym_RBRACE_RBRACE,
  [17045] = 5,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(1571), 1,
      aux_sym_class_label_token1,
    STATE(695), 1,
      sym_class_label,
    ACTIONS(1633), 2,
      ts_builtin_sym_end,
      sym__newline,
  [17062] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(1637), 1,
      sym__newline,
    ACTIONS(1635), 3,
      ts_builtin_sym_end,
      aux_sym_mmap_class_token1,
      aux_sym_mmap_icon_token1,
  [17077] = 2,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1639), 4,
      anon_sym_LPAREN,
      anon_sym_RPAREN,
      anon_sym_RBRACK,
      anon_sym_RBRACE_RBRACE,
  [17088] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(1643), 1,
      sym__newline,
    ACTIONS(1641), 3,
      ts_builtin_sym_end,
      aux_sym_mmap_class_token1,
      aux_sym_mmap_icon_token1,
  [17103] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(1647), 1,
      sym__newline,
    ACTIONS(1645), 3,
      ts_builtin_sym_end,
      aux_sym_mmap_class_token1,
      aux_sym_mmap_icon_token1,
  [17118] = 6,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(797), 1,
      aux_sym_mmap_class_token1,
    ACTIONS(1450), 1,
      ts_builtin_sym_end,
    ACTIONS(1452), 1,
      sym__newline,
    STATE(662), 1,
      sym_mmap_class,
  [17137] = 2,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1142), 4,
      ts_builtin_sym_end,
      anon_sym_COLON_COLON_COLON,
      anon_sym_LBRACE,
      sym__newline,
  [17148] = 3,
    ACTIONS(649), 1,
      anon_sym_LBRACK,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1649), 3,
      anon_sym_SEMI,
      sym__newline,
      sym__alpha_num_token,
  [17161] = 5,
    ACTIONS(1364), 1,
      aux_sym_pie_label_token1,
    ACTIONS(1651), 1,
      sym__newline,
    STATE(844), 1,
      sym_er_attribute_comment,
    STATE(881), 1,
      sym__er_word,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [17178] = 6,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(797), 1,
      aux_sym_mmap_class_token1,
    ACTIONS(1653), 1,
      ts_builtin_sym_end,
    ACTIONS(1655), 1,
      sym__newline,
    STATE(680), 1,
      sym_mmap_class,
  [17197] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(1659), 1,
      sym__newline,
    ACTIONS(1657), 3,
      ts_builtin_sym_end,
      aux_sym_mmap_class_token1,
      aux_sym_mmap_icon_token1,
  [17212] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(1663), 1,
      sym__newline,
    ACTIONS(1661), 3,
      ts_builtin_sym_end,
      aux_sym_mmap_class_token1,
      aux_sym_mmap_icon_token1,
  [17227] = 4,
    ACTIONS(1222), 1,
      ts_builtin_sym_end,
    STATE(438), 1,
      aux_sym_diagram_flow_repeat1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1586), 2,
      anon_sym_SEMI,
      sym__newline,
  [17242] = 5,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(1665), 1,
      aux_sym__sequence_rest_text_token1,
    STATE(183), 1,
      sym__sequence_rest_text,
    STATE(198), 1,
      sym_sequence_text,
  [17258] = 5,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(1667), 1,
      aux_sym__sequence_rest_text_token1,
    STATE(121), 1,
      sym_sequence_text,
    STATE(133), 1,
      sym__sequence_rest_text,
  [17274] = 4,
    ACTIONS(1669), 1,
      ts_builtin_sym_end,
    ACTIONS(1671), 1,
      sym__newline,
    STATE(480), 1,
      aux_sym_diagram_er_repeat1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [17288] = 4,
    ACTIONS(1674), 1,
      ts_builtin_sym_end,
    ACTIONS(1676), 1,
      sym__newline,
    STATE(604), 1,
      aux_sym_diagram_mindmap_repeat1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [17302] = 5,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(69), 1,
      sym__sequence_actor_word,
    STATE(257), 1,
      aux_sym_sequence_actor_repeat1,
    STATE(896), 1,
      sym_sequence_actor,
  [17318] = 5,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(69), 1,
      sym__sequence_actor_word,
    STATE(257), 1,
      aux_sym_sequence_actor_repeat1,
    STATE(895), 1,
      sym_sequence_actor,
  [17334] = 5,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(1590), 1,
      aux_sym__sequence_rest_text_token1,
    STATE(775), 1,
      sym_sequence_text,
    STATE(907), 1,
      sym__sequence_rest_text,
  [17350] = 5,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(69), 1,
      sym__sequence_actor_word,
    STATE(257), 1,
      aux_sym_sequence_actor_repeat1,
    STATE(893), 1,
      sym_sequence_actor,
  [17366] = 5,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(69), 1,
      sym__sequence_actor_word,
    STATE(257), 1,
      aux_sym_sequence_actor_repeat1,
    STATE(669), 1,
      sym_sequence_actor,
  [17382] = 5,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(1590), 1,
      aux_sym__sequence_rest_text_token1,
    STATE(890), 1,
      sym_sequence_text,
    STATE(907), 1,
      sym__sequence_rest_text,
  [17398] = 5,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(69), 1,
      sym__sequence_actor_word,
    STATE(257), 1,
      aux_sym_sequence_actor_repeat1,
    STATE(880), 1,
      sym_sequence_actor,
  [17414] = 5,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(69), 1,
      sym__sequence_actor_word,
    STATE(257), 1,
      aux_sym_sequence_actor_repeat1,
    STATE(879), 1,
      sym_sequence_actor,
  [17430] = 5,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(1590), 1,
      aux_sym__sequence_rest_text_token1,
    STATE(868), 1,
      sym_sequence_text,
    STATE(907), 1,
      sym__sequence_rest_text,
  [17446] = 4,
    ACTIONS(1678), 1,
      ts_builtin_sym_end,
    ACTIONS(1680), 1,
      sym__newline,
    STATE(491), 1,
      aux_sym_diagram_state_repeat1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [17460] = 4,
    ACTIONS(1683), 1,
      aux_sym_sequence_stmt_loop_token2,
    ACTIONS(1685), 1,
      aux_sym_sequence_stmt_alt_token2,
    STATE(492), 1,
      aux_sym_sequence_stmt_alt_repeat1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [17474] = 5,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(69), 1,
      sym__sequence_actor_word,
    STATE(257), 1,
      aux_sym_sequence_actor_repeat1,
    STATE(776), 1,
      sym_sequence_actor,
  [17490] = 4,
    ACTIONS(578), 1,
      ts_builtin_sym_end,
    ACTIONS(1688), 1,
      sym__newline,
    STATE(491), 1,
      aux_sym_diagram_state_repeat1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [17504] = 5,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(69), 1,
      sym__sequence_actor_word,
    STATE(257), 1,
      aux_sym_sequence_actor_repeat1,
    STATE(877), 1,
      sym_sequence_actor,
  [17520] = 5,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(69), 1,
      sym__sequence_actor_word,
    STATE(257), 1,
      aux_sym_sequence_actor_repeat1,
    STATE(675), 1,
      sym_sequence_actor,
  [17536] = 5,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(1590), 1,
      aux_sym__sequence_rest_text_token1,
    STATE(874), 1,
      sym_sequence_text,
    STATE(907), 1,
      sym__sequence_rest_text,
  [17552] = 5,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(69), 1,
      sym__sequence_actor_word,
    STATE(257), 1,
      aux_sym_sequence_actor_repeat1,
    STATE(864), 1,
      sym_sequence_actor,
  [17568] = 4,
    ACTIONS(1118), 1,
      anon_sym_RBRACE,
    ACTIONS(1690), 1,
      sym__newline,
    STATE(537), 1,
      aux_sym_class_stmt_class_repeat1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [17582] = 5,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(69), 1,
      sym__sequence_actor_word,
    STATE(257), 1,
      aux_sym_sequence_actor_repeat1,
    STATE(863), 1,
      sym_sequence_actor,
  [17598] = 4,
    ACTIONS(1118), 1,
      anon_sym_RBRACE,
    ACTIONS(1690), 1,
      sym__newline,
    STATE(539), 1,
      aux_sym_class_stmt_class_repeat1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [17612] = 2,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1692), 3,
      ts_builtin_sym_end,
      anon_sym_RBRACE,
      sym__newline,
  [17622] = 2,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1694), 3,
      ts_builtin_sym_end,
      anon_sym_RBRACE,
      sym__newline,
  [17632] = 2,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1696), 3,
      ts_builtin_sym_end,
      anon_sym_RBRACE,
      sym__newline,
  [17642] = 2,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1698), 3,
      ts_builtin_sym_end,
      anon_sym_RBRACE,
      sym__newline,
  [17652] = 2,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1700), 3,
      ts_builtin_sym_end,
      anon_sym_RBRACE,
      sym__newline,
  [17662] = 2,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1702), 3,
      ts_builtin_sym_end,
      anon_sym_RBRACE,
      sym__newline,
  [17672] = 2,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1704), 3,
      ts_builtin_sym_end,
      anon_sym_RBRACE,
      sym__newline,
  [17682] = 2,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1706), 3,
      ts_builtin_sym_end,
      anon_sym_RBRACE,
      sym__newline,
  [17692] = 3,
    ACTIONS(1708), 1,
      anon_sym_COLON,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1633), 2,
      ts_builtin_sym_end,
      sym__newline,
  [17704] = 4,
    ACTIONS(1565), 1,
      sym__alpha_num_token,
    STATE(62), 1,
      sym_flow_vertex_id,
    STATE(331), 1,
      sym_flow_vertex,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [17718] = 5,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(69), 1,
      sym__sequence_actor_word,
    STATE(257), 1,
      aux_sym_sequence_actor_repeat1,
    STATE(861), 1,
      sym_sequence_actor,
  [17734] = 5,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(69), 1,
      sym__sequence_actor_word,
    STATE(257), 1,
      aux_sym_sequence_actor_repeat1,
    STATE(682), 1,
      sym_sequence_actor,
  [17750] = 4,
    ACTIONS(1710), 1,
      sym__class_name,
    ACTIONS(1713), 1,
      sym__bquote_string,
    STATE(514), 1,
      aux_sym_class_name_body_repeat1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [17764] = 5,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(1590), 1,
      aux_sym__sequence_rest_text_token1,
    STATE(858), 1,
      sym_sequence_text,
    STATE(907), 1,
      sym__sequence_rest_text,
  [17780] = 5,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(1590), 1,
      aux_sym__sequence_rest_text_token1,
    STATE(838), 1,
      sym_sequence_text,
    STATE(907), 1,
      sym__sequence_rest_text,
  [17796] = 2,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1715), 3,
      ts_builtin_sym_end,
      anon_sym_SEMI,
      sym__newline,
  [17806] = 5,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(1590), 1,
      aux_sym__sequence_rest_text_token1,
    STATE(837), 1,
      sym_sequence_text,
    STATE(907), 1,
      sym__sequence_rest_text,
  [17822] = 5,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(1590), 1,
      aux_sym__sequence_rest_text_token1,
    STATE(836), 1,
      sym_sequence_text,
    STATE(907), 1,
      sym__sequence_rest_text,
  [17838] = 5,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(69), 1,
      sym__sequence_actor_word,
    STATE(257), 1,
      aux_sym_sequence_actor_repeat1,
    STATE(834), 1,
      sym_sequence_actor,
  [17854] = 5,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(69), 1,
      sym__sequence_actor_word,
    STATE(257), 1,
      aux_sym_sequence_actor_repeat1,
    STATE(833), 1,
      sym_sequence_actor,
  [17870] = 5,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(69), 1,
      sym__sequence_actor_word,
    STATE(257), 1,
      aux_sym_sequence_actor_repeat1,
    STATE(832), 1,
      sym_sequence_actor,
  [17886] = 5,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(69), 1,
      sym__sequence_actor_word,
    STATE(257), 1,
      aux_sym_sequence_actor_repeat1,
    STATE(831), 1,
      sym_sequence_actor,
  [17902] = 4,
    ACTIONS(105), 1,
      aux_sym_sequence_stmt_par_token2,
    ACTIONS(1717), 1,
      aux_sym_sequence_stmt_loop_token2,
    STATE(580), 1,
      aux_sym_sequence_stmt_par_repeat1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [17916] = 5,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(1590), 1,
      aux_sym__sequence_rest_text_token1,
    STATE(817), 1,
      sym_sequence_text,
    STATE(907), 1,
      sym__sequence_rest_text,
  [17932] = 5,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(1590), 1,
      aux_sym__sequence_rest_text_token1,
    STATE(816), 1,
      sym_sequence_text,
    STATE(907), 1,
      sym__sequence_rest_text,
  [17948] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(1721), 1,
      sym__newline,
    ACTIONS(1719), 2,
      ts_builtin_sym_end,
      aux_sym_mmap_class_token1,
  [17962] = 5,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(1590), 1,
      aux_sym__sequence_rest_text_token1,
    STATE(815), 1,
      sym_sequence_text,
    STATE(907), 1,
      sym__sequence_rest_text,
  [17978] = 5,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(69), 1,
      sym__sequence_actor_word,
    STATE(257), 1,
      aux_sym_sequence_actor_repeat1,
    STATE(813), 1,
      sym_sequence_actor,
  [17994] = 5,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(69), 1,
      sym__sequence_actor_word,
    STATE(257), 1,
      aux_sym_sequence_actor_repeat1,
    STATE(812), 1,
      sym_sequence_actor,
  [18010] = 5,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(69), 1,
      sym__sequence_actor_word,
    STATE(257), 1,
      aux_sym_sequence_actor_repeat1,
    STATE(811), 1,
      sym_sequence_actor,
  [18026] = 5,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(69), 1,
      sym__sequence_actor_word,
    STATE(257), 1,
      aux_sym_sequence_actor_repeat1,
    STATE(810), 1,
      sym_sequence_actor,
  [18042] = 4,
    ACTIONS(1723), 1,
      anon_sym_RBRACE,
    ACTIONS(1725), 1,
      sym__newline,
    STATE(553), 1,
      aux_sym_class_stmt_class_repeat1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [18056] = 5,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(1590), 1,
      aux_sym__sequence_rest_text_token1,
    STATE(807), 1,
      sym_sequence_text,
    STATE(907), 1,
      sym__sequence_rest_text,
  [18072] = 5,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(1590), 1,
      aux_sym__sequence_rest_text_token1,
    STATE(793), 1,
      sym_sequence_text,
    STATE(907), 1,
      sym__sequence_rest_text,
  [18088] = 5,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(1590), 1,
      aux_sym__sequence_rest_text_token1,
    STATE(792), 1,
      sym_sequence_text,
    STATE(907), 1,
      sym__sequence_rest_text,
  [18104] = 4,
    ACTIONS(1173), 1,
      anon_sym_RBRACE,
    ACTIONS(1727), 1,
      sym__newline,
    STATE(539), 1,
      aux_sym_class_stmt_class_repeat1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [18118] = 5,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(1590), 1,
      aux_sym__sequence_rest_text_token1,
    STATE(791), 1,
      sym_sequence_text,
    STATE(907), 1,
      sym__sequence_rest_text,
  [18134] = 4,
    ACTIONS(1729), 1,
      anon_sym_RBRACE,
    ACTIONS(1731), 1,
      sym__newline,
    STATE(539), 1,
      aux_sym_class_stmt_class_repeat1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [18148] = 5,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(69), 1,
      sym__sequence_actor_word,
    STATE(257), 1,
      aux_sym_sequence_actor_repeat1,
    STATE(789), 1,
      sym_sequence_actor,
  [18164] = 5,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(69), 1,
      sym__sequence_actor_word,
    STATE(257), 1,
      aux_sym_sequence_actor_repeat1,
    STATE(788), 1,
      sym_sequence_actor,
  [18180] = 5,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(69), 1,
      sym__sequence_actor_word,
    STATE(257), 1,
      aux_sym_sequence_actor_repeat1,
    STATE(787), 1,
      sym_sequence_actor,
  [18196] = 5,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(69), 1,
      sym__sequence_actor_word,
    STATE(257), 1,
      aux_sym_sequence_actor_repeat1,
    STATE(786), 1,
      sym_sequence_actor,
  [18212] = 5,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(1590), 1,
      aux_sym__sequence_rest_text_token1,
    STATE(782), 1,
      sym_sequence_text,
    STATE(907), 1,
      sym__sequence_rest_text,
  [18228] = 5,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(1590), 1,
      aux_sym__sequence_rest_text_token1,
    STATE(781), 1,
      sym_sequence_text,
    STATE(907), 1,
      sym__sequence_rest_text,
  [18244] = 5,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(1590), 1,
      aux_sym__sequence_rest_text_token1,
    STATE(780), 1,
      sym_sequence_text,
    STATE(907), 1,
      sym__sequence_rest_text,
  [18260] = 5,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(1590), 1,
      aux_sym__sequence_rest_text_token1,
    STATE(778), 1,
      sym_sequence_text,
    STATE(907), 1,
      sym__sequence_rest_text,
  [18276] = 2,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1734), 3,
      ts_builtin_sym_end,
      anon_sym_SEMI,
      sym__newline,
  [18286] = 2,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1736), 3,
      sym__class_name,
      sym__bquote_string,
      sym__dquote_string,
  [18296] = 4,
    ACTIONS(1098), 1,
      anon_sym_RBRACE,
    ACTIONS(1738), 1,
      sym__newline,
    STATE(564), 1,
      aux_sym_class_stmt_class_repeat1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [18310] = 5,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(1740), 1,
      sym__sequence_actor_word,
    STATE(459), 1,
      aux_sym_sequence_actor_repeat1,
    STATE(737), 1,
      sym_sequence_actor,
  [18326] = 3,
    ACTIONS(1742), 1,
      anon_sym_COLON,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1598), 2,
      ts_builtin_sym_end,
      sym__newline,
  [18338] = 4,
    ACTIONS(1098), 1,
      anon_sym_RBRACE,
    ACTIONS(1738), 1,
      sym__newline,
    STATE(539), 1,
      aux_sym_class_stmt_class_repeat1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [18352] = 5,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(69), 1,
      sym__sequence_actor_word,
    STATE(257), 1,
      aux_sym_sequence_actor_repeat1,
    STATE(767), 1,
      sym_sequence_actor,
  [18368] = 2,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1321), 3,
      ts_builtin_sym_end,
      anon_sym_RBRACE,
      sym__newline,
  [18378] = 2,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1744), 3,
      ts_builtin_sym_end,
      anon_sym_RBRACE,
      sym__newline,
  [18388] = 5,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(69), 1,
      sym__sequence_actor_word,
    STATE(257), 1,
      aux_sym_sequence_actor_repeat1,
    STATE(766), 1,
      sym_sequence_actor,
  [18404] = 5,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(1740), 1,
      sym__sequence_actor_word,
    STATE(459), 1,
      aux_sym_sequence_actor_repeat1,
    STATE(734), 1,
      sym_sequence_actor,
  [18420] = 2,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1746), 3,
      ts_builtin_sym_end,
      anon_sym_SEMI,
      sym__newline,
  [18430] = 5,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(1590), 1,
      aux_sym__sequence_rest_text_token1,
    STATE(765), 1,
      sym_sequence_text,
    STATE(907), 1,
      sym__sequence_rest_text,
  [18446] = 5,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(1590), 1,
      aux_sym__sequence_rest_text_token1,
    STATE(764), 1,
      sym_sequence_text,
    STATE(907), 1,
      sym__sequence_rest_text,
  [18462] = 5,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(1590), 1,
      aux_sym__sequence_rest_text_token1,
    STATE(763), 1,
      sym_sequence_text,
    STATE(907), 1,
      sym__sequence_rest_text,
  [18478] = 4,
    ACTIONS(1748), 1,
      anon_sym_RBRACE,
    ACTIONS(1750), 1,
      sym__newline,
    STATE(501), 1,
      aux_sym_class_stmt_class_repeat1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [18492] = 4,
    ACTIONS(1082), 1,
      anon_sym_RBRACE,
    ACTIONS(1752), 1,
      sym__newline,
    STATE(539), 1,
      aux_sym_class_stmt_class_repeat1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [18506] = 5,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(1590), 1,
      aux_sym__sequence_rest_text_token1,
    STATE(762), 1,
      sym_sequence_text,
    STATE(907), 1,
      sym__sequence_rest_text,
  [18522] = 2,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1754), 3,
      ts_builtin_sym_end,
      anon_sym_SEMI,
      sym__newline,
  [18532] = 4,
    ACTIONS(1395), 1,
      ts_builtin_sym_end,
    ACTIONS(1756), 1,
      sym__newline,
    STATE(480), 1,
      aux_sym_diagram_er_repeat1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [18546] = 5,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(1740), 1,
      sym__sequence_actor_word,
    STATE(459), 1,
      aux_sym_sequence_actor_repeat1,
    STATE(729), 1,
      sym_sequence_actor,
  [18562] = 5,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(69), 1,
      sym__sequence_actor_word,
    STATE(257), 1,
      aux_sym_sequence_actor_repeat1,
    STATE(835), 1,
      sym_sequence_actor,
  [18578] = 3,
    ACTIONS(1760), 1,
      anon_sym_LBRACE,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1758), 2,
      ts_builtin_sym_end,
      sym__newline,
  [18590] = 5,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(1590), 1,
      aux_sym__sequence_rest_text_token1,
    STATE(903), 1,
      sym_sequence_text,
    STATE(907), 1,
      sym__sequence_rest_text,
  [18606] = 5,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(69), 1,
      sym__sequence_actor_word,
    STATE(257), 1,
      aux_sym_sequence_actor_repeat1,
    STATE(790), 1,
      sym_sequence_actor,
  [18622] = 4,
    ACTIONS(1070), 1,
      ts_builtin_sym_end,
    ACTIONS(1762), 1,
      sym__newline,
    STATE(622), 1,
      aux_sym_diagram_class_repeat1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [18636] = 5,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(1740), 1,
      sym__sequence_actor_word,
    STATE(459), 1,
      aux_sym_sequence_actor_repeat1,
    STATE(726), 1,
      sym_sequence_actor,
  [18652] = 5,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(1590), 1,
      aux_sym__sequence_rest_text_token1,
    STATE(799), 1,
      sym_sequence_text,
    STATE(907), 1,
      sym__sequence_rest_text,
  [18668] = 5,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(1590), 1,
      aux_sym__sequence_rest_text_token1,
    STATE(748), 1,
      sym_sequence_text,
    STATE(907), 1,
      sym__sequence_rest_text,
  [18684] = 5,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(1590), 1,
      aux_sym__sequence_rest_text_token1,
    STATE(814), 1,
      sym_sequence_text,
    STATE(907), 1,
      sym__sequence_rest_text,
  [18700] = 4,
    ACTIONS(105), 1,
      aux_sym_sequence_stmt_par_token2,
    ACTIONS(1764), 1,
      aux_sym_sequence_stmt_loop_token2,
    STATE(580), 1,
      aux_sym_sequence_stmt_par_repeat1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [18714] = 4,
    ACTIONS(1766), 1,
      anon_sym_COLON,
    ACTIONS(1768), 1,
      aux_sym_gantt_task_text_token1,
    STATE(579), 1,
      aux_sym_gantt_task_text_repeat1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [18728] = 4,
    ACTIONS(1771), 1,
      aux_sym_sequence_stmt_loop_token2,
    ACTIONS(1773), 1,
      aux_sym_sequence_stmt_par_token2,
    STATE(580), 1,
      aux_sym_sequence_stmt_par_repeat1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [18742] = 5,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(1590), 1,
      aux_sym__sequence_rest_text_token1,
    STATE(821), 1,
      sym_sequence_text,
    STATE(907), 1,
      sym__sequence_rest_text,
  [18758] = 3,
    STATE(225), 1,
      sym_state_name,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1776), 2,
      anon_sym_LBRACK_STAR_RBRACK,
      aux_sym_state_name_token1,
  [18770] = 5,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(69), 1,
      sym__sequence_actor_word,
    STATE(257), 1,
      aux_sym_sequence_actor_repeat1,
    STATE(839), 1,
      sym_sequence_actor,
  [18786] = 3,
    STATE(828), 1,
      sym_state_name,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(572), 2,
      anon_sym_LBRACK_STAR_RBRACK,
      aux_sym_state_name_token1,
  [18798] = 5,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(1740), 1,
      sym__sequence_actor_word,
    STATE(459), 1,
      aux_sym_sequence_actor_repeat1,
    STATE(679), 1,
      sym_sequence_actor,
  [18814] = 5,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(1740), 1,
      sym__sequence_actor_word,
    STATE(459), 1,
      aux_sym_sequence_actor_repeat1,
    STATE(697), 1,
      sym_sequence_actor,
  [18830] = 5,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(69), 1,
      sym__sequence_actor_word,
    STATE(257), 1,
      aux_sym_sequence_actor_repeat1,
    STATE(850), 1,
      sym_sequence_actor,
  [18846] = 2,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1778), 3,
      ts_builtin_sym_end,
      anon_sym_SEMI,
      sym__newline,
  [18856] = 4,
    ACTIONS(63), 1,
      aux_sym_sequence_stmt_alt_token2,
    ACTIONS(1780), 1,
      aux_sym_sequence_stmt_loop_token2,
    STATE(492), 1,
      aux_sym_sequence_stmt_alt_repeat1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [18870] = 5,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(69), 1,
      sym__sequence_actor_word,
    STATE(257), 1,
      aux_sym_sequence_actor_repeat1,
    STATE(853), 1,
      sym_sequence_actor,
  [18886] = 5,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(1740), 1,
      sym__sequence_actor_word,
    STATE(459), 1,
      aux_sym_sequence_actor_repeat1,
    STATE(694), 1,
      sym_sequence_actor,
  [18902] = 5,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(1665), 1,
      aux_sym__sequence_rest_text_token1,
    STATE(183), 1,
      sym__sequence_rest_text,
    STATE(194), 1,
      sym_sequence_text,
  [18918] = 5,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(1665), 1,
      aux_sym__sequence_rest_text_token1,
    STATE(183), 1,
      sym__sequence_rest_text,
    STATE(195), 1,
      sym_sequence_text,
  [18934] = 5,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(1665), 1,
      aux_sym__sequence_rest_text_token1,
    STATE(183), 1,
      sym__sequence_rest_text,
    STATE(197), 1,
      sym_sequence_text,
  [18950] = 4,
    ACTIONS(105), 1,
      aux_sym_sequence_stmt_par_token2,
    ACTIONS(1782), 1,
      aux_sym_sequence_stmt_loop_token2,
    STATE(580), 1,
      aux_sym_sequence_stmt_par_repeat1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [18964] = 4,
    ACTIONS(63), 1,
      aux_sym_sequence_stmt_alt_token2,
    ACTIONS(1784), 1,
      aux_sym_sequence_stmt_loop_token2,
    STATE(492), 1,
      aux_sym_sequence_stmt_alt_repeat1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [18978] = 5,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(69), 1,
      sym__sequence_actor_word,
    STATE(257), 1,
      aux_sym_sequence_actor_repeat1,
    STATE(873), 1,
      sym_sequence_actor,
  [18994] = 4,
    ACTIONS(105), 1,
      aux_sym_sequence_stmt_par_token2,
    ACTIONS(1786), 1,
      aux_sym_sequence_stmt_loop_token2,
    STATE(580), 1,
      aux_sym_sequence_stmt_par_repeat1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [19008] = 4,
    ACTIONS(1788), 1,
      ts_builtin_sym_end,
    ACTIONS(1790), 1,
      sym__newline,
    STATE(481), 1,
      aux_sym_diagram_mindmap_repeat1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [19022] = 5,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(69), 1,
      sym__sequence_actor_word,
    STATE(257), 1,
      aux_sym_sequence_actor_repeat1,
    STATE(876), 1,
      sym_sequence_actor,
  [19038] = 5,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(69), 1,
      sym__sequence_actor_word,
    STATE(257), 1,
      aux_sym_sequence_actor_repeat1,
    STATE(889), 1,
      sym_sequence_actor,
  [19054] = 5,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(69), 1,
      sym__sequence_actor_word,
    STATE(257), 1,
      aux_sym_sequence_actor_repeat1,
    STATE(892), 1,
      sym_sequence_actor,
  [19070] = 4,
    ACTIONS(1482), 1,
      sym__alpha_num_token,
    ACTIONS(1547), 1,
      anon_sym_PIPE,
    STATE(369), 1,
      aux_sym_flow_arrow_text_repeat1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [19084] = 4,
    ACTIONS(1792), 1,
      ts_builtin_sym_end,
    ACTIONS(1794), 1,
      sym__newline,
    STATE(604), 1,
      aux_sym_diagram_mindmap_repeat1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [19098] = 5,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(69), 1,
      sym__sequence_actor_word,
    STATE(257), 1,
      aux_sym_sequence_actor_repeat1,
    STATE(899), 1,
      sym_sequence_actor,
  [19114] = 5,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(69), 1,
      sym__sequence_actor_word,
    STATE(257), 1,
      aux_sym_sequence_actor_repeat1,
    STATE(900), 1,
      sym_sequence_actor,
  [19130] = 5,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(1590), 1,
      aux_sym__sequence_rest_text_token1,
    STATE(906), 1,
      sym_sequence_text,
    STATE(907), 1,
      sym__sequence_rest_text,
  [19146] = 5,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(1797), 1,
      aux_sym__sequence_rest_text_token1,
    STATE(94), 1,
      sym__sequence_rest_text,
    STATE(131), 1,
      sym_sequence_text,
  [19162] = 5,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(1797), 1,
      aux_sym__sequence_rest_text_token1,
    STATE(87), 1,
      sym_sequence_text,
    STATE(94), 1,
      sym__sequence_rest_text,
  [19178] = 5,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(1797), 1,
      aux_sym__sequence_rest_text_token1,
    STATE(88), 1,
      sym_sequence_text,
    STATE(94), 1,
      sym__sequence_rest_text,
  [19194] = 4,
    ACTIONS(1405), 1,
      ts_builtin_sym_end,
    ACTIONS(1799), 1,
      sym__newline,
    STATE(480), 1,
      aux_sym_diagram_er_repeat1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [19208] = 5,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(1797), 1,
      aux_sym__sequence_rest_text_token1,
    STATE(89), 1,
      sym_sequence_text,
    STATE(94), 1,
      sym__sequence_rest_text,
  [19224] = 4,
    ACTIONS(1801), 1,
      sym__class_name,
    ACTIONS(1803), 1,
      sym__bquote_string,
    STATE(514), 1,
      aux_sym_class_name_body_repeat1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [19238] = 4,
    ACTIONS(105), 1,
      aux_sym_sequence_stmt_par_token2,
    ACTIONS(1805), 1,
      aux_sym_sequence_stmt_loop_token2,
    STATE(580), 1,
      aux_sym_sequence_stmt_par_repeat1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [19252] = 4,
    ACTIONS(63), 1,
      aux_sym_sequence_stmt_alt_token2,
    ACTIONS(1807), 1,
      aux_sym_sequence_stmt_loop_token2,
    STATE(492), 1,
      aux_sym_sequence_stmt_alt_repeat1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [19266] = 4,
    ACTIONS(1405), 1,
      ts_builtin_sym_end,
    ACTIONS(1799), 1,
      sym__newline,
    STATE(567), 1,
      aux_sym_diagram_er_repeat1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [19280] = 5,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(1590), 1,
      aux_sym__sequence_rest_text_token1,
    STATE(907), 1,
      sym__sequence_rest_text,
    STATE(909), 1,
      sym_sequence_text,
  [19296] = 2,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1809), 3,
      ts_builtin_sym_end,
      anon_sym_SEMI,
      sym__newline,
  [19306] = 4,
    ACTIONS(105), 1,
      aux_sym_sequence_stmt_par_token2,
    ACTIONS(1811), 1,
      aux_sym_sequence_stmt_loop_token2,
    STATE(580), 1,
      aux_sym_sequence_stmt_par_repeat1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [19320] = 5,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(1590), 1,
      aux_sym__sequence_rest_text_token1,
    STATE(907), 1,
      sym__sequence_rest_text,
    STATE(911), 1,
      sym_sequence_text,
  [19336] = 5,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(1590), 1,
      aux_sym__sequence_rest_text_token1,
    STATE(907), 1,
      sym__sequence_rest_text,
    STATE(917), 1,
      sym_sequence_text,
  [19352] = 4,
    ACTIONS(1813), 1,
      ts_builtin_sym_end,
    ACTIONS(1815), 1,
      sym__newline,
    STATE(622), 1,
      aux_sym_diagram_class_repeat1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [19366] = 5,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(1740), 1,
      sym__sequence_actor_word,
    STATE(459), 1,
      aux_sym_sequence_actor_repeat1,
    STATE(659), 1,
      sym_sequence_actor,
  [19382] = 3,
    ACTIONS(1820), 1,
      anon_sym_COLON,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1818), 2,
      ts_builtin_sym_end,
      sym__newline,
  [19394] = 2,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1004), 3,
      sym__class_name,
      sym__bquote_string,
      sym__dquote_string,
  [19404] = 5,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(1822), 1,
      aux_sym__sequence_rest_text_token1,
    STATE(164), 1,
      sym__sequence_rest_text,
    STATE(167), 1,
      sym_sequence_text,
  [19420] = 5,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(1822), 1,
      aux_sym__sequence_rest_text_token1,
    STATE(164), 1,
      sym__sequence_rest_text,
    STATE(166), 1,
      sym_sequence_text,
  [19436] = 4,
    ACTIONS(1824), 1,
      anon_sym_COLON,
    ACTIONS(1826), 1,
      aux_sym_gantt_task_text_token1,
    STATE(579), 1,
      aux_sym_gantt_task_text_repeat1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [19450] = 5,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(1822), 1,
      aux_sym__sequence_rest_text_token1,
    STATE(164), 1,
      sym__sequence_rest_text,
    STATE(165), 1,
      sym_sequence_text,
  [19466] = 5,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(1822), 1,
      aux_sym__sequence_rest_text_token1,
    STATE(164), 1,
      sym__sequence_rest_text,
    STATE(208), 1,
      sym_sequence_text,
  [19482] = 4,
    ACTIONS(1803), 1,
      sym__bquote_string,
    ACTIONS(1828), 1,
      sym__class_name,
    STATE(514), 1,
      aux_sym_class_name_body_repeat1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [19496] = 4,
    ACTIONS(105), 1,
      aux_sym_sequence_stmt_par_token2,
    ACTIONS(1830), 1,
      aux_sym_sequence_stmt_loop_token2,
    STATE(580), 1,
      aux_sym_sequence_stmt_par_repeat1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [19510] = 4,
    ACTIONS(63), 1,
      aux_sym_sequence_stmt_alt_token2,
    ACTIONS(1832), 1,
      aux_sym_sequence_stmt_loop_token2,
    STATE(492), 1,
      aux_sym_sequence_stmt_alt_repeat1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [19524] = 4,
    ACTIONS(105), 1,
      aux_sym_sequence_stmt_par_token2,
    ACTIONS(1834), 1,
      aux_sym_sequence_stmt_loop_token2,
    STATE(580), 1,
      aux_sym_sequence_stmt_par_repeat1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [19538] = 4,
    ACTIONS(1836), 1,
      ts_builtin_sym_end,
    ACTIONS(1838), 1,
      sym__newline,
    STATE(645), 1,
      aux_sym_diagram_state_repeat1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [19552] = 4,
    ACTIONS(1840), 1,
      sym__alpha_num_token,
    STATE(426), 1,
      aux_sym_flow_arrow_text_repeat1,
    STATE(441), 1,
      sym_flow_arrow_text,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [19566] = 5,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(1667), 1,
      aux_sym__sequence_rest_text_token1,
    STATE(124), 1,
      sym_sequence_text,
    STATE(133), 1,
      sym__sequence_rest_text,
  [19582] = 5,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(1667), 1,
      aux_sym__sequence_rest_text_token1,
    STATE(123), 1,
      sym_sequence_text,
    STATE(133), 1,
      sym__sequence_rest_text,
  [19598] = 5,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(1667), 1,
      aux_sym__sequence_rest_text_token1,
    STATE(122), 1,
      sym_sequence_text,
    STATE(133), 1,
      sym__sequence_rest_text,
  [19614] = 4,
    ACTIONS(1801), 1,
      sym__class_name,
    ACTIONS(1842), 1,
      sym__bquote_string,
    STATE(514), 1,
      aux_sym_class_name_body_repeat1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [19628] = 2,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1844), 3,
      ts_builtin_sym_end,
      anon_sym_SEMI,
      sym__newline,
  [19638] = 3,
    STATE(225), 1,
      sym_state_name,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(572), 2,
      anon_sym_LBRACK_STAR_RBRACK,
      aux_sym_state_name_token1,
  [19650] = 4,
    ACTIONS(1846), 1,
      sym__alpha_num_token,
    STATE(392), 1,
      aux_sym_flow_arrow_text_repeat1,
    STATE(846), 1,
      sym_flow_vertex_text,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [19664] = 2,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1573), 3,
      ts_builtin_sym_end,
      anon_sym_SEMI,
      sym__newline,
  [19674] = 4,
    ACTIONS(647), 1,
      ts_builtin_sym_end,
    ACTIONS(1848), 1,
      sym__newline,
    STATE(491), 1,
      aux_sym_diagram_state_repeat1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [19688] = 4,
    ACTIONS(647), 1,
      ts_builtin_sym_end,
    ACTIONS(1848), 1,
      sym__newline,
    STATE(494), 1,
      aux_sym_diagram_state_repeat1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [19702] = 4,
    ACTIONS(1850), 1,
      sym__alpha_num_token,
    STATE(603), 1,
      aux_sym_flow_arrow_text_repeat1,
    STATE(848), 1,
      sym_flow_arrow_text,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [19716] = 4,
    ACTIONS(1852), 1,
      ts_builtin_sym_end,
    ACTIONS(1854), 1,
      sym__newline,
    STATE(611), 1,
      aux_sym_diagram_er_repeat1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [19730] = 3,
    STATE(898), 1,
      sym_state_name,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(572), 2,
      anon_sym_LBRACK_STAR_RBRACK,
      aux_sym_state_name_token1,
  [19742] = 4,
    ACTIONS(1803), 1,
      sym__bquote_string,
    ACTIONS(1856), 1,
      sym__class_name,
    STATE(514), 1,
      aux_sym_class_name_body_repeat1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [19756] = 5,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(69), 1,
      sym__sequence_actor_word,
    STATE(257), 1,
      aux_sym_sequence_actor_repeat1,
    STATE(740), 1,
      sym_sequence_actor,
  [19772] = 4,
    ACTIONS(1858), 1,
      ts_builtin_sym_end,
    ACTIONS(1860), 1,
      sym__newline,
    STATE(573), 1,
      aux_sym_diagram_class_repeat1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [19786] = 5,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(69), 1,
      sym__sequence_actor_word,
    STATE(257), 1,
      aux_sym_sequence_actor_repeat1,
    STATE(779), 1,
      sym_sequence_actor,
  [19802] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(1864), 1,
      sym__sequence_actor_word,
    ACTIONS(1862), 2,
      anon_sym_PLUS,
      anon_sym_DASH,
  [19816] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(1868), 1,
      sym__sequence_actor_word,
    ACTIONS(1866), 2,
      anon_sym_PLUS,
      anon_sym_DASH,
  [19830] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(1872), 1,
      sym__sequence_actor_word,
    ACTIONS(1870), 2,
      anon_sym_PLUS,
      anon_sym_DASH,
  [19844] = 2,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1450), 2,
      ts_builtin_sym_end,
      sym__newline,
  [19853] = 2,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1669), 2,
      ts_builtin_sym_end,
      sym__newline,
  [19862] = 3,
    ACTIONS(1874), 1,
      aux_sym_sequence_stmt_participant_token1,
    ACTIONS(1876), 1,
      sym__newline,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [19873] = 4,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(1674), 1,
      ts_builtin_sym_end,
    ACTIONS(1878), 1,
      sym__whitespace,
    STATE(732), 1,
      sym_mmap_node,
  [19886] = 2,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1880), 2,
      ts_builtin_sym_end,
      sym__newline,
  [19895] = 2,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1653), 2,
      ts_builtin_sym_end,
      sym__newline,
  [19904] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(1590), 1,
      aux_sym__sequence_rest_text_token1,
    STATE(857), 1,
      sym__sequence_rest_text,
  [19917] = 2,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1678), 2,
      ts_builtin_sym_end,
      sym__newline,
  [19926] = 2,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1882), 2,
      ts_builtin_sym_end,
      sym__newline,
  [19935] = 2,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1884), 2,
      ts_builtin_sym_end,
      sym__newline,
  [19944] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(1590), 1,
      aux_sym__sequence_rest_text_token1,
    STATE(783), 1,
      sym__sequence_rest_text,
  [19957] = 2,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1633), 2,
      ts_builtin_sym_end,
      sym__newline,
  [19966] = 3,
    ACTIONS(1886), 1,
      anon_sym_COLON,
    ACTIONS(1888), 1,
      anon_sym_COMMA,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [19977] = 2,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1890), 2,
      anon_sym_PIPE,
      sym__alpha_num_token,
  [19986] = 2,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1892), 2,
      anon_sym_SEMI,
      sym__newline,
  [19995] = 2,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1894), 2,
      anon_sym_RBRACE,
      sym__er_alphanum,
  [20004] = 2,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1896), 2,
      sym__newline,
      aux_sym_pie_label_token1,
  [20013] = 2,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1898), 2,
      sym__newline,
      aux_sym_pie_label_token1,
  [20022] = 3,
    ACTIONS(1900), 1,
      anon_sym_COLON,
    ACTIONS(1902), 1,
      anon_sym_COMMA,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [20033] = 2,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1904), 2,
      ts_builtin_sym_end,
      sym__newline,
  [20042] = 2,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1906), 2,
      sym__newline,
      aux_sym_pie_label_token1,
  [20051] = 2,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1908), 2,
      ts_builtin_sym_end,
      sym__newline,
  [20060] = 3,
    ACTIONS(1910), 1,
      aux_sym_sequence_stmt_participant_token1,
    ACTIONS(1912), 1,
      sym__newline,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [20071] = 2,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1914), 2,
      ts_builtin_sym_end,
      sym__newline,
  [20080] = 2,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1916), 2,
      ts_builtin_sym_end,
      sym__newline,
  [20089] = 3,
    ACTIONS(1918), 1,
      anon_sym_COLON,
    ACTIONS(1920), 1,
      anon_sym_COMMA,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [20100] = 2,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1922), 2,
      ts_builtin_sym_end,
      sym__newline,
  [20109] = 2,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1924), 2,
      anon_sym_LBRACK_STAR_RBRACK,
      aux_sym_state_name_token1,
  [20118] = 2,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1926), 2,
      anon_sym_RBRACE,
      sym__newline,
  [20127] = 2,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1928), 2,
      ts_builtin_sym_end,
      sym__newline,
  [20136] = 2,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1930), 2,
      ts_builtin_sym_end,
      sym__newline,
  [20145] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(905), 1,
      aux_sym_class_label_token1,
    STATE(232), 1,
      sym_state_description,
  [20158] = 2,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1932), 2,
      anon_sym_LBRACK_STAR_RBRACK,
      aux_sym_state_name_token1,
  [20167] = 2,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1729), 2,
      anon_sym_RBRACE,
      sym__newline,
  [20176] = 2,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1934), 2,
      anon_sym_SEMI,
      sym__newline,
  [20185] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(1936), 1,
      aux_sym__sequence_rest_text_token1,
    STATE(284), 1,
      sym_gantt_meta_format,
  [20198] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(905), 1,
      aux_sym_class_label_token1,
    STATE(230), 1,
      sym_state_description,
  [20211] = 3,
    ACTIONS(1938), 1,
      aux_sym_sequence_stmt_participant_token1,
    ACTIONS(1940), 1,
      sym__newline,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [20222] = 2,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1569), 2,
      ts_builtin_sym_end,
      sym__newline,
  [20231] = 2,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1942), 2,
      anon_sym_RBRACE,
      sym__er_alphanum,
  [20240] = 3,
    ACTIONS(1944), 1,
      aux_sym_sequence_stmt_participant_token1,
    ACTIONS(1946), 1,
      sym__newline,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [20251] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(1936), 1,
      aux_sym__sequence_rest_text_token1,
    STATE(291), 1,
      sym_gantt_meta_format,
  [20264] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(1590), 1,
      aux_sym__sequence_rest_text_token1,
    STATE(777), 1,
      sym__sequence_rest_text,
  [20277] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(1936), 1,
      aux_sym__sequence_rest_text_token1,
    STATE(290), 1,
      sym_gantt_meta_format,
  [20290] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(1936), 1,
      aux_sym__sequence_rest_text_token1,
    STATE(283), 1,
      sym_gantt_meta_format,
  [20303] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(1936), 1,
      aux_sym__sequence_rest_text_token1,
    STATE(282), 1,
      sym_gantt_meta_format,
  [20316] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(1590), 1,
      aux_sym__sequence_rest_text_token1,
    STATE(774), 1,
      sym__sequence_rest_text,
  [20329] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(1590), 1,
      aux_sym__sequence_rest_text_token1,
    STATE(772), 1,
      sym__sequence_rest_text,
  [20342] = 2,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1948), 2,
      ts_builtin_sym_end,
      sym__newline,
  [20351] = 2,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1950), 2,
      ts_builtin_sym_end,
      sym__newline,
  [20360] = 2,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1952), 2,
      ts_builtin_sym_end,
      sym__newline,
  [20369] = 2,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1954), 2,
      ts_builtin_sym_end,
      sym__newline,
  [20378] = 2,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1956), 2,
      anon_sym_RBRACE,
      sym__er_alphanum,
  [20387] = 3,
    ACTIONS(1958), 1,
      anon_sym_PIPE,
    ACTIONS(1960), 1,
      sym__alpha_num_token,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [20398] = 3,
    ACTIONS(839), 1,
      anon_sym_LBRACE,
    STATE(233), 1,
      sym_state_composite_body,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [20409] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(1936), 1,
      aux_sym__sequence_rest_text_token1,
    STATE(281), 1,
      sym_gantt_meta_format,
  [20422] = 2,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1962), 2,
      ts_builtin_sym_end,
      sym__newline,
  [20431] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(1936), 1,
      aux_sym__sequence_rest_text_token1,
    STATE(279), 1,
      sym_gantt_meta_format,
  [20444] = 2,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1964), 2,
      ts_builtin_sym_end,
      sym__newline,
  [20453] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(1590), 1,
      aux_sym__sequence_rest_text_token1,
    STATE(761), 1,
      sym__sequence_rest_text,
  [20466] = 2,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1966), 2,
      ts_builtin_sym_end,
      sym__newline,
  [20475] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(1590), 1,
      aux_sym__sequence_rest_text_token1,
    STATE(758), 1,
      sym__sequence_rest_text,
  [20488] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(1936), 1,
      aux_sym__sequence_rest_text_token1,
    STATE(268), 1,
      sym_gantt_meta_format,
  [20501] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(1590), 1,
      aux_sym__sequence_rest_text_token1,
    STATE(755), 1,
      sym__sequence_rest_text,
  [20514] = 2,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1968), 2,
      ts_builtin_sym_end,
      sym__newline,
  [20523] = 2,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1970), 2,
      anon_sym_LBRACK_STAR_RBRACK,
      aux_sym_state_name_token1,
  [20532] = 2,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1972), 2,
      anon_sym_LBRACK_STAR_RBRACK,
      aux_sym_state_name_token1,
  [20541] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(1590), 1,
      aux_sym__sequence_rest_text_token1,
    STATE(823), 1,
      sym__sequence_rest_text,
  [20554] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(1590), 1,
      aux_sym__sequence_rest_text_token1,
    STATE(827), 1,
      sym__sequence_rest_text,
  [20567] = 3,
    ACTIONS(1974), 1,
      aux_sym_sequence_stmt_participant_token1,
    ACTIONS(1976), 1,
      sym__newline,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [20578] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(1590), 1,
      aux_sym__sequence_rest_text_token1,
    STATE(843), 1,
      sym__sequence_rest_text,
  [20591] = 2,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1631), 2,
      anon_sym_LPAREN_LPAREN,
      anon_sym_RPAREN_RPAREN,
  [20600] = 3,
    ACTIONS(1978), 1,
      aux_sym_sequence_stmt_participant_token1,
    ACTIONS(1980), 1,
      sym__newline,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [20611] = 3,
    ACTIONS(1982), 1,
      sym__er_alphanum,
    STATE(348), 1,
      sym_er_attribute_name,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [20622] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(1984), 1,
      sym__mindmap_text,
    STATE(212), 1,
      sym_mmap_node_id,
  [20635] = 2,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1792), 2,
      ts_builtin_sym_end,
      sym__newline,
  [20644] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(1590), 1,
      aux_sym__sequence_rest_text_token1,
    STATE(915), 1,
      sym__sequence_rest_text,
  [20657] = 3,
    ACTIONS(1986), 1,
      aux_sym_sequence_stmt_participant_token1,
    ACTIONS(1988), 1,
      sym__newline,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [20668] = 4,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(1878), 1,
      sym__whitespace,
    ACTIONS(1990), 1,
      ts_builtin_sym_end,
    STATE(732), 1,
      sym_mmap_node,
  [20681] = 3,
    ACTIONS(1397), 1,
      sym__er_alphanum,
    STATE(918), 1,
      sym_er_entity_name,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [20692] = 3,
    ACTIONS(1992), 1,
      aux_sym_sequence_stmt_participant_token1,
    ACTIONS(1994), 1,
      sym__newline,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [20703] = 3,
    ACTIONS(1996), 1,
      anon_sym_BSLASH_RBRACK,
    ACTIONS(1998), 1,
      anon_sym_SLASH_RBRACK,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [20714] = 2,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(2000), 2,
      ts_builtin_sym_end,
      sym__newline,
  [20723] = 3,
    ACTIONS(2002), 1,
      anon_sym_COLON,
    ACTIONS(2004), 1,
      anon_sym_COMMA,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [20734] = 3,
    ACTIONS(2006), 1,
      anon_sym_BSLASH_RBRACK,
    ACTIONS(2008), 1,
      anon_sym_SLASH_RBRACK,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [20745] = 2,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1596), 2,
      anon_sym_LPAREN_LPAREN,
      anon_sym_RPAREN_RPAREN,
  [20754] = 2,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(2010), 2,
      ts_builtin_sym_end,
      sym__newline,
  [20763] = 2,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1639), 2,
      anon_sym_LPAREN_LPAREN,
      anon_sym_RPAREN_RPAREN,
  [20772] = 4,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(1936), 1,
      aux_sym__sequence_rest_text_token1,
    STATE(278), 1,
      sym_gantt_meta_format,
  [20785] = 2,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(2012), 2,
      anon_sym_SEMI,
      sym__newline,
  [20794] = 2,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
    ACTIONS(1813), 2,
      ts_builtin_sym_end,
      sym__newline,
  [20803] = 2,
    ACTIONS(2014), 1,
      sym__newline,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [20811] = 2,
    ACTIONS(2016), 1,
      sym__alpha_num_token,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [20819] = 2,
    ACTIONS(2018), 1,
      anon_sym_PIPE_RBRACK,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [20827] = 2,
    ACTIONS(2020), 1,
      anon_sym_COLON,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [20835] = 2,
    ACTIONS(2022), 1,
      anon_sym_RPAREN_RBRACK,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [20843] = 2,
    ACTIONS(2024), 1,
      anon_sym_RBRACE_RBRACE,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [20851] = 2,
    ACTIONS(2026), 1,
      anon_sym_RBRACK,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [20859] = 2,
    ACTIONS(2028), 1,
      sym__newline,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [20867] = 2,
    ACTIONS(2030), 1,
      anon_sym_TILDE,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [20875] = 3,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(2032), 1,
      sym_pie_value,
  [20885] = 2,
    ACTIONS(2034), 1,
      sym__newline,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [20893] = 2,
    ACTIONS(2036), 1,
      sym__er_alphanum,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [20901] = 2,
    ACTIONS(2038), 1,
      anon_sym_RBRACE,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [20909] = 2,
    ACTIONS(2040), 1,
      sym__newline,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [20917] = 2,
    ACTIONS(2042), 1,
      sym__newline,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [20925] = 2,
    ACTIONS(2044), 1,
      sym__newline,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [20933] = 2,
    ACTIONS(2046), 1,
      sym__newline,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [20941] = 2,
    ACTIONS(2048), 1,
      sym__newline,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [20949] = 2,
    ACTIONS(2050), 1,
      sym__newline,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [20957] = 2,
    ACTIONS(2052), 1,
      sym__newline,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [20965] = 2,
    ACTIONS(2054), 1,
      sym__er_alphanum,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [20973] = 3,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(2056), 1,
      aux_sym_mmap_class_token2,
  [20983] = 2,
    ACTIONS(2058), 1,
      sym__newline,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [20991] = 3,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(2060), 1,
      aux_sym_mmap_icon_token2,
  [21001] = 2,
    ACTIONS(2062), 1,
      sym__newline,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [21009] = 2,
    ACTIONS(2064), 1,
      anon_sym_COLON,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [21017] = 2,
    ACTIONS(2066), 1,
      sym__newline,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [21025] = 2,
    ACTIONS(2068), 1,
      sym__newline,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [21033] = 2,
    ACTIONS(2070), 1,
      anon_sym_COLON,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [21041] = 2,
    ACTIONS(2072), 1,
      sym__newline,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [21049] = 2,
    ACTIONS(2074), 1,
      sym__newline,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [21057] = 2,
    ACTIONS(2002), 1,
      anon_sym_COLON,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [21065] = 2,
    ACTIONS(2076), 1,
      sym__newline,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [21073] = 2,
    ACTIONS(2078), 1,
      sym__newline,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [21081] = 2,
    ACTIONS(2080), 1,
      sym__newline,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [21089] = 2,
    ACTIONS(2082), 1,
      sym__newline,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [21097] = 2,
    ACTIONS(2084), 1,
      anon_sym_LBRACK,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [21105] = 3,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(2086), 1,
      sym__sequence_actor_word,
  [21115] = 2,
    ACTIONS(2088), 1,
      anon_sym_COLON,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [21123] = 2,
    ACTIONS(2090), 1,
      anon_sym_COLON,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [21131] = 2,
    ACTIONS(2092), 1,
      anon_sym_COLON,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [21139] = 2,
    ACTIONS(2094), 1,
      anon_sym_COLON,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [21147] = 2,
    ACTIONS(2096), 1,
      sym__newline,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [21155] = 2,
    ACTIONS(2098), 1,
      sym__newline,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [21163] = 2,
    ACTIONS(2100), 1,
      sym__newline,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [21171] = 2,
    ACTIONS(2102), 1,
      sym__newline,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [21179] = 3,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(2104), 1,
      sym__sequence_actor_word,
  [21189] = 2,
    ACTIONS(2106), 1,
      anon_sym_COLON,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [21197] = 2,
    ACTIONS(2108), 1,
      anon_sym_DASH_RPAREN,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [21205] = 3,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(2110), 1,
      aux_sym_directive_token1,
  [21215] = 2,
    ACTIONS(2112), 1,
      ts_builtin_sym_end,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [21223] = 2,
    ACTIONS(2114), 1,
      sym__newline,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [21231] = 2,
    ACTIONS(2116), 1,
      aux_sym_class_generics_token1,
    ACTIONS(3), 2,
      sym__whitespace,
      sym_comment,
  [21239] = 2,
    ACTIONS(2118), 1,
      anon_sym_RBRACK,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [21247] = 3,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(2120), 1,
      sym_gantt_task_data,
  [21257] = 2,
    ACTIONS(2122), 1,
      anon_sym_GT_GT,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [21265] = 2,
    ACTIONS(2124), 1,
      aux_sym_mmap_markdown_token1,
    ACTIONS(3), 2,
      sym__whitespace,
      sym_comment,
  [21273] = 2,
    ACTIONS(2126), 1,
      sym__md_end,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [21281] = 2,
    ACTIONS(2128), 1,
      anon_sym_RPAREN_RPAREN,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [21289] = 2,
    ACTIONS(2130), 1,
      sym__newline,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [21297] = 3,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(2132), 1,
      sym__mindmap_text,
  [21307] = 2,
    ACTIONS(2134), 1,
      sym__alpha_num_token,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [21315] = 2,
    ACTIONS(2136), 1,
      anon_sym_COLON,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [21323] = 2,
    ACTIONS(2138), 1,
      anon_sym_COLON,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [21331] = 2,
    ACTIONS(2140), 1,
      anon_sym_COLON,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [21339] = 2,
    ACTIONS(2142), 1,
      anon_sym_COLON,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [21347] = 2,
    ACTIONS(2144), 1,
      sym__newline,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [21355] = 2,
    ACTIONS(2146), 1,
      sym__newline,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [21363] = 2,
    ACTIONS(2148), 1,
      sym__newline,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [21371] = 2,
    ACTIONS(2150), 1,
      sym__newline,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [21379] = 2,
    ACTIONS(2152), 1,
      ts_builtin_sym_end,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [21387] = 2,
    ACTIONS(2154), 1,
      anon_sym_RBRACK_RBRACK,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [21395] = 2,
    ACTIONS(2156), 1,
      sym__newline,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [21403] = 2,
    ACTIONS(2158), 1,
      sym__newline,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [21411] = 2,
    ACTIONS(2160), 1,
      anon_sym_TILDE,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [21419] = 2,
    ACTIONS(2162), 1,
      sym__newline,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [21427] = 3,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(2164), 1,
      sym_type_directive,
  [21437] = 2,
    ACTIONS(2166), 1,
      anon_sym_RBRACK_RPAREN,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [21445] = 2,
    ACTIONS(2168), 1,
      anon_sym_end,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [21453] = 2,
    ACTIONS(2170), 1,
      sym__newline,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [21461] = 2,
    ACTIONS(2172), 1,
      anon_sym_COLON,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [21469] = 2,
    ACTIONS(2174), 1,
      anon_sym_COLON,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [21477] = 2,
    ACTIONS(2176), 1,
      sym__alpha_num_token,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [21485] = 2,
    ACTIONS(2178), 1,
      anon_sym_COLON,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [21493] = 2,
    ACTIONS(2180), 1,
      anon_sym_COLON,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [21501] = 2,
    ACTIONS(2182), 1,
      anon_sym_COLON,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [21509] = 2,
    ACTIONS(2184), 1,
      anon_sym_COLON,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [21517] = 2,
    ACTIONS(2186), 1,
      sym__newline,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [21525] = 2,
    ACTIONS(2188), 1,
      sym__newline,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [21533] = 2,
    ACTIONS(2190), 1,
      sym__newline,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [21541] = 2,
    ACTIONS(2192), 1,
      sym__newline,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [21549] = 2,
    ACTIONS(2194), 1,
      anon_sym_COLON,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [21557] = 2,
    ACTIONS(2196), 1,
      anon_sym_COLON,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [21565] = 2,
    ACTIONS(2198), 1,
      sym__newline,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [21573] = 2,
    ACTIONS(2200), 1,
      sym__newline,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [21581] = 2,
    ACTIONS(2202), 1,
      sym__newline,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [21589] = 2,
    ACTIONS(2204), 1,
      sym__newline,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [21597] = 2,
    ACTIONS(2206), 1,
      anon_sym_RPAREN,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [21605] = 2,
    ACTIONS(2208), 1,
      anon_sym_RBRACK,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [21613] = 2,
    ACTIONS(2210), 1,
      anon_sym_end,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [21621] = 2,
    ACTIONS(2212), 1,
      anon_sym_PIPE,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [21629] = 2,
    ACTIONS(2214), 1,
      sym__alpha_num_token,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [21637] = 2,
    ACTIONS(2216), 1,
      sym__newline,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [21645] = 2,
    ACTIONS(2218), 1,
      sym__newline,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [21653] = 2,
    ACTIONS(2220), 1,
      anon_sym_COLON,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [21661] = 2,
    ACTIONS(2222), 1,
      sym__newline,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [21669] = 2,
    ACTIONS(2224), 1,
      anon_sym_end,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [21677] = 2,
    ACTIONS(2226), 1,
      aux_sym_sequence_stmt_participant_token1,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [21685] = 2,
    ACTIONS(2228), 1,
      anon_sym_RBRACE,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [21693] = 2,
    ACTIONS(2230), 1,
      sym__newline,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [21701] = 2,
    ACTIONS(2232), 1,
      sym__newline,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [21709] = 2,
    ACTIONS(2234), 1,
      sym__newline,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [21717] = 2,
    ACTIONS(2236), 1,
      anon_sym_RPAREN,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [21725] = 2,
    ACTIONS(1918), 1,
      anon_sym_COLON,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [21733] = 2,
    ACTIONS(2238), 1,
      anon_sym_COLON,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [21741] = 2,
    ACTIONS(2240), 1,
      anon_sym_COLON,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [21749] = 2,
    ACTIONS(2242), 1,
      anon_sym_COLON,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [21757] = 3,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(1878), 1,
      sym__whitespace,
    STATE(599), 1,
      sym_mmap_node,
  [21767] = 3,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(2244), 1,
      sym__mindmap_text,
  [21777] = 2,
    ACTIONS(2246), 1,
      aux_sym_mmap_markdown_token1,
    ACTIONS(3), 2,
      sym__whitespace,
      sym_comment,
  [21785] = 2,
    ACTIONS(2248), 1,
      sym__newline,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [21793] = 2,
    ACTIONS(2250), 1,
      anon_sym_COLON,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [21801] = 2,
    ACTIONS(2252), 1,
      ts_builtin_sym_end,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [21809] = 2,
    ACTIONS(2254), 1,
      sym__alpha_num_token,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [21817] = 2,
    ACTIONS(2256), 1,
      sym__md_end,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [21825] = 2,
    ACTIONS(2258), 1,
      sym__newline,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [21833] = 2,
    ACTIONS(2260), 1,
      sym__newline,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [21841] = 2,
    ACTIONS(2262), 1,
      sym__newline,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [21849] = 2,
    ACTIONS(2264), 1,
      sym__newline,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [21857] = 2,
    ACTIONS(1900), 1,
      anon_sym_COLON,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [21865] = 2,
    ACTIONS(2266), 1,
      anon_sym_COLON,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [21873] = 2,
    ACTIONS(2268), 1,
      anon_sym_COLON,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [21881] = 2,
    ACTIONS(2270), 1,
      anon_sym_COLON,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [21889] = 2,
    ACTIONS(2272), 1,
      sym__newline,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [21897] = 2,
    ACTIONS(1651), 1,
      sym__newline,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [21905] = 3,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(2274), 1,
      sym__sequence_actor_word,
  [21915] = 3,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(2276), 1,
      sym__sequence_actor_word,
  [21925] = 2,
    ACTIONS(2278), 1,
      anon_sym_COLON,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [21933] = 2,
    ACTIONS(2280), 1,
      sym__alpha_num_token,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [21941] = 2,
    ACTIONS(2282), 1,
      sym__alpha_num_token,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [21949] = 3,
    ACTIONS(3), 1,
      sym__whitespace,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(2284), 1,
      sym__sequence_actor_word,
  [21959] = 2,
    ACTIONS(2286), 1,
      anon_sym_COLON,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [21967] = 2,
    ACTIONS(2288), 1,
      sym__newline,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [21975] = 2,
    ACTIONS(2290), 1,
      sym__newline,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [21983] = 2,
    ACTIONS(2292), 1,
      anon_sym_COLON,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [21991] = 2,
    ACTIONS(1886), 1,
      anon_sym_COLON,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [21999] = 2,
    ACTIONS(2294), 1,
      anon_sym_COLON,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [22007] = 2,
    ACTIONS(2296), 1,
      anon_sym_COLON,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [22015] = 2,
    ACTIONS(2298), 1,
      anon_sym_COLON,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [22023] = 2,
    ACTIONS(2300), 1,
      aux_sym_class_generics_token1,
    ACTIONS(3), 2,
      sym__whitespace,
      sym_comment,
  [22031] = 2,
    ACTIONS(2302), 1,
      anon_sym_COLON,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [22039] = 2,
    ACTIONS(2304), 1,
      anon_sym_COLON,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [22047] = 2,
    ACTIONS(2306), 1,
      anon_sym_COLON,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [22055] = 2,
    ACTIONS(691), 1,
      sym__newline,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [22063] = 2,
    ACTIONS(2308), 1,
      anon_sym_GT_GT,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [22071] = 2,
    ACTIONS(2310), 1,
      sym__newline,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [22079] = 2,
    ACTIONS(2312), 1,
      sym_state_id,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [22087] = 2,
    ACTIONS(2314), 1,
      sym_state_id,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [22095] = 2,
    ACTIONS(2316), 1,
      sym__newline,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [22103] = 2,
    ACTIONS(687), 1,
      sym__newline,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [22111] = 3,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(1878), 1,
      sym__whitespace,
    STATE(732), 1,
      sym_mmap_node,
  [22121] = 2,
    ACTIONS(2318), 1,
      sym__newline,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [22129] = 2,
    ACTIONS(2320), 1,
      anon_sym_RPAREN,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [22137] = 2,
    ACTIONS(2322), 1,
      sym__newline,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [22145] = 2,
    ACTIONS(2324), 1,
      anon_sym_RBRACE_RBRACE,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [22153] = 2,
    ACTIONS(2326), 1,
      anon_sym_LPAREN_LPAREN,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [22161] = 2,
    ACTIONS(2328), 1,
      anon_sym_RPAREN_RPAREN,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [22169] = 2,
    ACTIONS(2330), 1,
      sym__newline,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [22177] = 2,
    ACTIONS(2332), 1,
      anon_sym_RBRACK,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [22185] = 2,
    ACTIONS(2334), 1,
      sym__newline,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [22193] = 2,
    ACTIONS(2336), 1,
      anon_sym_COLON,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [22201] = 2,
    ACTIONS(2338), 1,
      anon_sym_LPAREN,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
  [22209] = 2,
    ACTIONS(2340), 1,
      sym__newline,
    ACTIONS(5), 2,
      sym__whitespace,
      sym_comment,
};

static const uint32_t ts_small_parse_table_map[] = {
  [SMALL_STATE(2)] = 0,
  [SMALL_STATE(3)] = 105,
  [SMALL_STATE(4)] = 210,
  [SMALL_STATE(5)] = 315,
  [SMALL_STATE(6)] = 420,
  [SMALL_STATE(7)] = 525,
  [SMALL_STATE(8)] = 630,
  [SMALL_STATE(9)] = 735,
  [SMALL_STATE(10)] = 840,
  [SMALL_STATE(11)] = 945,
  [SMALL_STATE(12)] = 1050,
  [SMALL_STATE(13)] = 1155,
  [SMALL_STATE(14)] = 1260,
  [SMALL_STATE(15)] = 1360,
  [SMALL_STATE(16)] = 1460,
  [SMALL_STATE(17)] = 1560,
  [SMALL_STATE(18)] = 1660,
  [SMALL_STATE(19)] = 1760,
  [SMALL_STATE(20)] = 1857,
  [SMALL_STATE(21)] = 1956,
  [SMALL_STATE(22)] = 2055,
  [SMALL_STATE(23)] = 2154,
  [SMALL_STATE(24)] = 2253,
  [SMALL_STATE(25)] = 2352,
  [SMALL_STATE(26)] = 2451,
  [SMALL_STATE(27)] = 2550,
  [SMALL_STATE(28)] = 2649,
  [SMALL_STATE(29)] = 2748,
  [SMALL_STATE(30)] = 2847,
  [SMALL_STATE(31)] = 2946,
  [SMALL_STATE(32)] = 3045,
  [SMALL_STATE(33)] = 3144,
  [SMALL_STATE(34)] = 3243,
  [SMALL_STATE(35)] = 3342,
  [SMALL_STATE(36)] = 3441,
  [SMALL_STATE(37)] = 3540,
  [SMALL_STATE(38)] = 3639,
  [SMALL_STATE(39)] = 3736,
  [SMALL_STATE(40)] = 3833,
  [SMALL_STATE(41)] = 3932,
  [SMALL_STATE(42)] = 4031,
  [SMALL_STATE(43)] = 4130,
  [SMALL_STATE(44)] = 4229,
  [SMALL_STATE(45)] = 4328,
  [SMALL_STATE(46)] = 4427,
  [SMALL_STATE(47)] = 4526,
  [SMALL_STATE(48)] = 4622,
  [SMALL_STATE(49)] = 4718,
  [SMALL_STATE(50)] = 4814,
  [SMALL_STATE(51)] = 4910,
  [SMALL_STATE(52)] = 5006,
  [SMALL_STATE(53)] = 5102,
  [SMALL_STATE(54)] = 5198,
  [SMALL_STATE(55)] = 5294,
  [SMALL_STATE(56)] = 5390,
  [SMALL_STATE(57)] = 5486,
  [SMALL_STATE(58)] = 5582,
  [SMALL_STATE(59)] = 5678,
  [SMALL_STATE(60)] = 5774,
  [SMALL_STATE(61)] = 5870,
  [SMALL_STATE(62)] = 5966,
  [SMALL_STATE(63)] = 6044,
  [SMALL_STATE(64)] = 6111,
  [SMALL_STATE(65)] = 6178,
  [SMALL_STATE(66)] = 6245,
  [SMALL_STATE(67)] = 6312,
  [SMALL_STATE(68)] = 6379,
  [SMALL_STATE(69)] = 6442,
  [SMALL_STATE(70)] = 6505,
  [SMALL_STATE(71)] = 6568,
  [SMALL_STATE(72)] = 6631,
  [SMALL_STATE(73)] = 6694,
  [SMALL_STATE(74)] = 6757,
  [SMALL_STATE(75)] = 6820,
  [SMALL_STATE(76)] = 6883,
  [SMALL_STATE(77)] = 6946,
  [SMALL_STATE(78)] = 7009,
  [SMALL_STATE(79)] = 7072,
  [SMALL_STATE(80)] = 7132,
  [SMALL_STATE(81)] = 7192,
  [SMALL_STATE(82)] = 7252,
  [SMALL_STATE(83)] = 7312,
  [SMALL_STATE(84)] = 7344,
  [SMALL_STATE(85)] = 7399,
  [SMALL_STATE(86)] = 7431,
  [SMALL_STATE(87)] = 7463,
  [SMALL_STATE(88)] = 7495,
  [SMALL_STATE(89)] = 7527,
  [SMALL_STATE(90)] = 7559,
  [SMALL_STATE(91)] = 7591,
  [SMALL_STATE(92)] = 7623,
  [SMALL_STATE(93)] = 7655,
  [SMALL_STATE(94)] = 7687,
  [SMALL_STATE(95)] = 7719,
  [SMALL_STATE(96)] = 7751,
  [SMALL_STATE(97)] = 7783,
  [SMALL_STATE(98)] = 7815,
  [SMALL_STATE(99)] = 7847,
  [SMALL_STATE(100)] = 7879,
  [SMALL_STATE(101)] = 7911,
  [SMALL_STATE(102)] = 7943,
  [SMALL_STATE(103)] = 7975,
  [SMALL_STATE(104)] = 8007,
  [SMALL_STATE(105)] = 8039,
  [SMALL_STATE(106)] = 8071,
  [SMALL_STATE(107)] = 8103,
  [SMALL_STATE(108)] = 8135,
  [SMALL_STATE(109)] = 8167,
  [SMALL_STATE(110)] = 8199,
  [SMALL_STATE(111)] = 8231,
  [SMALL_STATE(112)] = 8263,
  [SMALL_STATE(113)] = 8295,
  [SMALL_STATE(114)] = 8327,
  [SMALL_STATE(115)] = 8359,
  [SMALL_STATE(116)] = 8391,
  [SMALL_STATE(117)] = 8423,
  [SMALL_STATE(118)] = 8455,
  [SMALL_STATE(119)] = 8487,
  [SMALL_STATE(120)] = 8519,
  [SMALL_STATE(121)] = 8551,
  [SMALL_STATE(122)] = 8583,
  [SMALL_STATE(123)] = 8615,
  [SMALL_STATE(124)] = 8647,
  [SMALL_STATE(125)] = 8679,
  [SMALL_STATE(126)] = 8711,
  [SMALL_STATE(127)] = 8743,
  [SMALL_STATE(128)] = 8775,
  [SMALL_STATE(129)] = 8807,
  [SMALL_STATE(130)] = 8839,
  [SMALL_STATE(131)] = 8871,
  [SMALL_STATE(132)] = 8903,
  [SMALL_STATE(133)] = 8935,
  [SMALL_STATE(134)] = 8967,
  [SMALL_STATE(135)] = 8999,
  [SMALL_STATE(136)] = 9031,
  [SMALL_STATE(137)] = 9063,
  [SMALL_STATE(138)] = 9095,
  [SMALL_STATE(139)] = 9127,
  [SMALL_STATE(140)] = 9159,
  [SMALL_STATE(141)] = 9191,
  [SMALL_STATE(142)] = 9223,
  [SMALL_STATE(143)] = 9255,
  [SMALL_STATE(144)] = 9287,
  [SMALL_STATE(145)] = 9319,
  [SMALL_STATE(146)] = 9351,
  [SMALL_STATE(147)] = 9383,
  [SMALL_STATE(148)] = 9415,
  [SMALL_STATE(149)] = 9447,
  [SMALL_STATE(150)] = 9478,
  [SMALL_STATE(151)] = 9509,
  [SMALL_STATE(152)] = 9540,
  [SMALL_STATE(153)] = 9571,
  [SMALL_STATE(154)] = 9602,
  [SMALL_STATE(155)] = 9633,
  [SMALL_STATE(156)] = 9664,
  [SMALL_STATE(157)] = 9695,
  [SMALL_STATE(158)] = 9726,
  [SMALL_STATE(159)] = 9757,
  [SMALL_STATE(160)] = 9788,
  [SMALL_STATE(161)] = 9819,
  [SMALL_STATE(162)] = 9850,
  [SMALL_STATE(163)] = 9881,
  [SMALL_STATE(164)] = 9912,
  [SMALL_STATE(165)] = 9943,
  [SMALL_STATE(166)] = 9974,
  [SMALL_STATE(167)] = 10005,
  [SMALL_STATE(168)] = 10036,
  [SMALL_STATE(169)] = 10067,
  [SMALL_STATE(170)] = 10098,
  [SMALL_STATE(171)] = 10129,
  [SMALL_STATE(172)] = 10160,
  [SMALL_STATE(173)] = 10191,
  [SMALL_STATE(174)] = 10222,
  [SMALL_STATE(175)] = 10253,
  [SMALL_STATE(176)] = 10284,
  [SMALL_STATE(177)] = 10315,
  [SMALL_STATE(178)] = 10346,
  [SMALL_STATE(179)] = 10377,
  [SMALL_STATE(180)] = 10408,
  [SMALL_STATE(181)] = 10439,
  [SMALL_STATE(182)] = 10470,
  [SMALL_STATE(183)] = 10501,
  [SMALL_STATE(184)] = 10532,
  [SMALL_STATE(185)] = 10563,
  [SMALL_STATE(186)] = 10594,
  [SMALL_STATE(187)] = 10625,
  [SMALL_STATE(188)] = 10656,
  [SMALL_STATE(189)] = 10687,
  [SMALL_STATE(190)] = 10718,
  [SMALL_STATE(191)] = 10749,
  [SMALL_STATE(192)] = 10780,
  [SMALL_STATE(193)] = 10811,
  [SMALL_STATE(194)] = 10842,
  [SMALL_STATE(195)] = 10873,
  [SMALL_STATE(196)] = 10904,
  [SMALL_STATE(197)] = 10935,
  [SMALL_STATE(198)] = 10966,
  [SMALL_STATE(199)] = 10997,
  [SMALL_STATE(200)] = 11028,
  [SMALL_STATE(201)] = 11059,
  [SMALL_STATE(202)] = 11090,
  [SMALL_STATE(203)] = 11121,
  [SMALL_STATE(204)] = 11152,
  [SMALL_STATE(205)] = 11183,
  [SMALL_STATE(206)] = 11214,
  [SMALL_STATE(207)] = 11245,
  [SMALL_STATE(208)] = 11276,
  [SMALL_STATE(209)] = 11307,
  [SMALL_STATE(210)] = 11338,
  [SMALL_STATE(211)] = 11369,
  [SMALL_STATE(212)] = 11400,
  [SMALL_STATE(213)] = 11451,
  [SMALL_STATE(214)] = 11502,
  [SMALL_STATE(215)] = 11539,
  [SMALL_STATE(216)] = 11576,
  [SMALL_STATE(217)] = 11621,
  [SMALL_STATE(218)] = 11652,
  [SMALL_STATE(219)] = 11689,
  [SMALL_STATE(220)] = 11724,
  [SMALL_STATE(221)] = 11748,
  [SMALL_STATE(222)] = 11786,
  [SMALL_STATE(223)] = 11818,
  [SMALL_STATE(224)] = 11848,
  [SMALL_STATE(225)] = 11877,
  [SMALL_STATE(226)] = 11902,
  [SMALL_STATE(227)] = 11929,
  [SMALL_STATE(228)] = 11958,
  [SMALL_STATE(229)] = 11981,
  [SMALL_STATE(230)] = 12009,
  [SMALL_STATE(231)] = 12031,
  [SMALL_STATE(232)] = 12053,
  [SMALL_STATE(233)] = 12075,
  [SMALL_STATE(234)] = 12097,
  [SMALL_STATE(235)] = 12119,
  [SMALL_STATE(236)] = 12141,
  [SMALL_STATE(237)] = 12163,
  [SMALL_STATE(238)] = 12203,
  [SMALL_STATE(239)] = 12237,
  [SMALL_STATE(240)] = 12259,
  [SMALL_STATE(241)] = 12281,
  [SMALL_STATE(242)] = 12303,
  [SMALL_STATE(243)] = 12325,
  [SMALL_STATE(244)] = 12347,
  [SMALL_STATE(245)] = 12387,
  [SMALL_STATE(246)] = 12409,
  [SMALL_STATE(247)] = 12431,
  [SMALL_STATE(248)] = 12453,
  [SMALL_STATE(249)] = 12475,
  [SMALL_STATE(250)] = 12497,
  [SMALL_STATE(251)] = 12533,
  [SMALL_STATE(252)] = 12555,
  [SMALL_STATE(253)] = 12593,
  [SMALL_STATE(254)] = 12615,
  [SMALL_STATE(255)] = 12637,
  [SMALL_STATE(256)] = 12659,
  [SMALL_STATE(257)] = 12681,
  [SMALL_STATE(258)] = 12709,
  [SMALL_STATE(259)] = 12731,
  [SMALL_STATE(260)] = 12757,
  [SMALL_STATE(261)] = 12793,
  [SMALL_STATE(262)] = 12819,
  [SMALL_STATE(263)] = 12841,
  [SMALL_STATE(264)] = 12863,
  [SMALL_STATE(265)] = 12903,
  [SMALL_STATE(266)] = 12925,
  [SMALL_STATE(267)] = 12963,
  [SMALL_STATE(268)] = 12985,
  [SMALL_STATE(269)] = 13006,
  [SMALL_STATE(270)] = 13027,
  [SMALL_STATE(271)] = 13064,
  [SMALL_STATE(272)] = 13099,
  [SMALL_STATE(273)] = 13136,
  [SMALL_STATE(274)] = 13173,
  [SMALL_STATE(275)] = 13210,
  [SMALL_STATE(276)] = 13229,
  [SMALL_STATE(277)] = 13250,
  [SMALL_STATE(278)] = 13271,
  [SMALL_STATE(279)] = 13292,
  [SMALL_STATE(280)] = 13313,
  [SMALL_STATE(281)] = 13350,
  [SMALL_STATE(282)] = 13371,
  [SMALL_STATE(283)] = 13392,
  [SMALL_STATE(284)] = 13413,
  [SMALL_STATE(285)] = 13434,
  [SMALL_STATE(286)] = 13471,
  [SMALL_STATE(287)] = 13508,
  [SMALL_STATE(288)] = 13529,
  [SMALL_STATE(289)] = 13550,
  [SMALL_STATE(290)] = 13571,
  [SMALL_STATE(291)] = 13592,
  [SMALL_STATE(292)] = 13613,
  [SMALL_STATE(293)] = 13650,
  [SMALL_STATE(294)] = 13671,
  [SMALL_STATE(295)] = 13708,
  [SMALL_STATE(296)] = 13743,
  [SMALL_STATE(297)] = 13780,
  [SMALL_STATE(298)] = 13804,
  [SMALL_STATE(299)] = 13832,
  [SMALL_STATE(300)] = 13856,
  [SMALL_STATE(301)] = 13874,
  [SMALL_STATE(302)] = 13902,
  [SMALL_STATE(303)] = 13926,
  [SMALL_STATE(304)] = 13960,
  [SMALL_STATE(305)] = 13994,
  [SMALL_STATE(306)] = 14018,
  [SMALL_STATE(307)] = 14052,
  [SMALL_STATE(308)] = 14080,
  [SMALL_STATE(309)] = 14114,
  [SMALL_STATE(310)] = 14148,
  [SMALL_STATE(311)] = 14182,
  [SMALL_STATE(312)] = 14210,
  [SMALL_STATE(313)] = 14244,
  [SMALL_STATE(314)] = 14272,
  [SMALL_STATE(315)] = 14306,
  [SMALL_STATE(316)] = 14330,
  [SMALL_STATE(317)] = 14349,
  [SMALL_STATE(318)] = 14368,
  [SMALL_STATE(319)] = 14387,
  [SMALL_STATE(320)] = 14406,
  [SMALL_STATE(321)] = 14425,
  [SMALL_STATE(322)] = 14444,
  [SMALL_STATE(323)] = 14463,
  [SMALL_STATE(324)] = 14494,
  [SMALL_STATE(325)] = 14513,
  [SMALL_STATE(326)] = 14532,
  [SMALL_STATE(327)] = 14551,
  [SMALL_STATE(328)] = 14572,
  [SMALL_STATE(329)] = 14591,
  [SMALL_STATE(330)] = 14610,
  [SMALL_STATE(331)] = 14641,
  [SMALL_STATE(332)] = 14660,
  [SMALL_STATE(333)] = 14679,
  [SMALL_STATE(334)] = 14698,
  [SMALL_STATE(335)] = 14717,
  [SMALL_STATE(336)] = 14736,
  [SMALL_STATE(337)] = 14755,
  [SMALL_STATE(338)] = 14779,
  [SMALL_STATE(339)] = 14803,
  [SMALL_STATE(340)] = 14827,
  [SMALL_STATE(341)] = 14857,
  [SMALL_STATE(342)] = 14881,
  [SMALL_STATE(343)] = 14905,
  [SMALL_STATE(344)] = 14923,
  [SMALL_STATE(345)] = 14941,
  [SMALL_STATE(346)] = 14969,
  [SMALL_STATE(347)] = 14997,
  [SMALL_STATE(348)] = 15025,
  [SMALL_STATE(349)] = 15055,
  [SMALL_STATE(350)] = 15073,
  [SMALL_STATE(351)] = 15091,
  [SMALL_STATE(352)] = 15111,
  [SMALL_STATE(353)] = 15139,
  [SMALL_STATE(354)] = 15165,
  [SMALL_STATE(355)] = 15191,
  [SMALL_STATE(356)] = 15205,
  [SMALL_STATE(357)] = 15221,
  [SMALL_STATE(358)] = 15237,
  [SMALL_STATE(359)] = 15251,
  [SMALL_STATE(360)] = 15277,
  [SMALL_STATE(361)] = 15291,
  [SMALL_STATE(362)] = 15313,
  [SMALL_STATE(363)] = 15335,
  [SMALL_STATE(364)] = 15355,
  [SMALL_STATE(365)] = 15377,
  [SMALL_STATE(366)] = 15399,
  [SMALL_STATE(367)] = 15425,
  [SMALL_STATE(368)] = 15446,
  [SMALL_STATE(369)] = 15461,
  [SMALL_STATE(370)] = 15478,
  [SMALL_STATE(371)] = 15493,
  [SMALL_STATE(372)] = 15508,
  [SMALL_STATE(373)] = 15527,
  [SMALL_STATE(374)] = 15548,
  [SMALL_STATE(375)] = 15563,
  [SMALL_STATE(376)] = 15584,
  [SMALL_STATE(377)] = 15599,
  [SMALL_STATE(378)] = 15614,
  [SMALL_STATE(379)] = 15637,
  [SMALL_STATE(380)] = 15662,
  [SMALL_STATE(381)] = 15685,
  [SMALL_STATE(382)] = 15704,
  [SMALL_STATE(383)] = 15725,
  [SMALL_STATE(384)] = 15740,
  [SMALL_STATE(385)] = 15757,
  [SMALL_STATE(386)] = 15778,
  [SMALL_STATE(387)] = 15799,
  [SMALL_STATE(388)] = 15817,
  [SMALL_STATE(389)] = 15835,
  [SMALL_STATE(390)] = 15847,
  [SMALL_STATE(391)] = 15867,
  [SMALL_STATE(392)] = 15885,
  [SMALL_STATE(393)] = 15901,
  [SMALL_STATE(394)] = 15913,
  [SMALL_STATE(395)] = 15925,
  [SMALL_STATE(396)] = 15943,
  [SMALL_STATE(397)] = 15961,
  [SMALL_STATE(398)] = 15979,
  [SMALL_STATE(399)] = 15991,
  [SMALL_STATE(400)] = 16009,
  [SMALL_STATE(401)] = 16021,
  [SMALL_STATE(402)] = 16039,
  [SMALL_STATE(403)] = 16057,
  [SMALL_STATE(404)] = 16075,
  [SMALL_STATE(405)] = 16091,
  [SMALL_STATE(406)] = 16103,
  [SMALL_STATE(407)] = 16119,
  [SMALL_STATE(408)] = 16137,
  [SMALL_STATE(409)] = 16157,
  [SMALL_STATE(410)] = 16169,
  [SMALL_STATE(411)] = 16189,
  [SMALL_STATE(412)] = 16209,
  [SMALL_STATE(413)] = 16229,
  [SMALL_STATE(414)] = 16249,
  [SMALL_STATE(415)] = 16261,
  [SMALL_STATE(416)] = 16279,
  [SMALL_STATE(417)] = 16297,
  [SMALL_STATE(418)] = 16313,
  [SMALL_STATE(419)] = 16331,
  [SMALL_STATE(420)] = 16351,
  [SMALL_STATE(421)] = 16363,
  [SMALL_STATE(422)] = 16381,
  [SMALL_STATE(423)] = 16399,
  [SMALL_STATE(424)] = 16419,
  [SMALL_STATE(425)] = 16439,
  [SMALL_STATE(426)] = 16457,
  [SMALL_STATE(427)] = 16473,
  [SMALL_STATE(428)] = 16493,
  [SMALL_STATE(429)] = 16511,
  [SMALL_STATE(430)] = 16529,
  [SMALL_STATE(431)] = 16547,
  [SMALL_STATE(432)] = 16565,
  [SMALL_STATE(433)] = 16577,
  [SMALL_STATE(434)] = 16588,
  [SMALL_STATE(435)] = 16599,
  [SMALL_STATE(436)] = 16612,
  [SMALL_STATE(437)] = 16629,
  [SMALL_STATE(438)] = 16646,
  [SMALL_STATE(439)] = 16661,
  [SMALL_STATE(440)] = 16678,
  [SMALL_STATE(441)] = 16693,
  [SMALL_STATE(442)] = 16706,
  [SMALL_STATE(443)] = 16719,
  [SMALL_STATE(444)] = 16732,
  [SMALL_STATE(445)] = 16745,
  [SMALL_STATE(446)] = 16758,
  [SMALL_STATE(447)] = 16773,
  [SMALL_STATE(448)] = 16792,
  [SMALL_STATE(449)] = 16803,
  [SMALL_STATE(450)] = 16822,
  [SMALL_STATE(451)] = 16833,
  [SMALL_STATE(452)] = 16850,
  [SMALL_STATE(453)] = 16865,
  [SMALL_STATE(454)] = 16884,
  [SMALL_STATE(455)] = 16895,
  [SMALL_STATE(456)] = 16910,
  [SMALL_STATE(457)] = 16929,
  [SMALL_STATE(458)] = 16940,
  [SMALL_STATE(459)] = 16959,
  [SMALL_STATE(460)] = 16976,
  [SMALL_STATE(461)] = 16991,
  [SMALL_STATE(462)] = 17008,
  [SMALL_STATE(463)] = 17019,
  [SMALL_STATE(464)] = 17034,
  [SMALL_STATE(465)] = 17045,
  [SMALL_STATE(466)] = 17062,
  [SMALL_STATE(467)] = 17077,
  [SMALL_STATE(468)] = 17088,
  [SMALL_STATE(469)] = 17103,
  [SMALL_STATE(470)] = 17118,
  [SMALL_STATE(471)] = 17137,
  [SMALL_STATE(472)] = 17148,
  [SMALL_STATE(473)] = 17161,
  [SMALL_STATE(474)] = 17178,
  [SMALL_STATE(475)] = 17197,
  [SMALL_STATE(476)] = 17212,
  [SMALL_STATE(477)] = 17227,
  [SMALL_STATE(478)] = 17242,
  [SMALL_STATE(479)] = 17258,
  [SMALL_STATE(480)] = 17274,
  [SMALL_STATE(481)] = 17288,
  [SMALL_STATE(482)] = 17302,
  [SMALL_STATE(483)] = 17318,
  [SMALL_STATE(484)] = 17334,
  [SMALL_STATE(485)] = 17350,
  [SMALL_STATE(486)] = 17366,
  [SMALL_STATE(487)] = 17382,
  [SMALL_STATE(488)] = 17398,
  [SMALL_STATE(489)] = 17414,
  [SMALL_STATE(490)] = 17430,
  [SMALL_STATE(491)] = 17446,
  [SMALL_STATE(492)] = 17460,
  [SMALL_STATE(493)] = 17474,
  [SMALL_STATE(494)] = 17490,
  [SMALL_STATE(495)] = 17504,
  [SMALL_STATE(496)] = 17520,
  [SMALL_STATE(497)] = 17536,
  [SMALL_STATE(498)] = 17552,
  [SMALL_STATE(499)] = 17568,
  [SMALL_STATE(500)] = 17582,
  [SMALL_STATE(501)] = 17598,
  [SMALL_STATE(502)] = 17612,
  [SMALL_STATE(503)] = 17622,
  [SMALL_STATE(504)] = 17632,
  [SMALL_STATE(505)] = 17642,
  [SMALL_STATE(506)] = 17652,
  [SMALL_STATE(507)] = 17662,
  [SMALL_STATE(508)] = 17672,
  [SMALL_STATE(509)] = 17682,
  [SMALL_STATE(510)] = 17692,
  [SMALL_STATE(511)] = 17704,
  [SMALL_STATE(512)] = 17718,
  [SMALL_STATE(513)] = 17734,
  [SMALL_STATE(514)] = 17750,
  [SMALL_STATE(515)] = 17764,
  [SMALL_STATE(516)] = 17780,
  [SMALL_STATE(517)] = 17796,
  [SMALL_STATE(518)] = 17806,
  [SMALL_STATE(519)] = 17822,
  [SMALL_STATE(520)] = 17838,
  [SMALL_STATE(521)] = 17854,
  [SMALL_STATE(522)] = 17870,
  [SMALL_STATE(523)] = 17886,
  [SMALL_STATE(524)] = 17902,
  [SMALL_STATE(525)] = 17916,
  [SMALL_STATE(526)] = 17932,
  [SMALL_STATE(527)] = 17948,
  [SMALL_STATE(528)] = 17962,
  [SMALL_STATE(529)] = 17978,
  [SMALL_STATE(530)] = 17994,
  [SMALL_STATE(531)] = 18010,
  [SMALL_STATE(532)] = 18026,
  [SMALL_STATE(533)] = 18042,
  [SMALL_STATE(534)] = 18056,
  [SMALL_STATE(535)] = 18072,
  [SMALL_STATE(536)] = 18088,
  [SMALL_STATE(537)] = 18104,
  [SMALL_STATE(538)] = 18118,
  [SMALL_STATE(539)] = 18134,
  [SMALL_STATE(540)] = 18148,
  [SMALL_STATE(541)] = 18164,
  [SMALL_STATE(542)] = 18180,
  [SMALL_STATE(543)] = 18196,
  [SMALL_STATE(544)] = 18212,
  [SMALL_STATE(545)] = 18228,
  [SMALL_STATE(546)] = 18244,
  [SMALL_STATE(547)] = 18260,
  [SMALL_STATE(548)] = 18276,
  [SMALL_STATE(549)] = 18286,
  [SMALL_STATE(550)] = 18296,
  [SMALL_STATE(551)] = 18310,
  [SMALL_STATE(552)] = 18326,
  [SMALL_STATE(553)] = 18338,
  [SMALL_STATE(554)] = 18352,
  [SMALL_STATE(555)] = 18368,
  [SMALL_STATE(556)] = 18378,
  [SMALL_STATE(557)] = 18388,
  [SMALL_STATE(558)] = 18404,
  [SMALL_STATE(559)] = 18420,
  [SMALL_STATE(560)] = 18430,
  [SMALL_STATE(561)] = 18446,
  [SMALL_STATE(562)] = 18462,
  [SMALL_STATE(563)] = 18478,
  [SMALL_STATE(564)] = 18492,
  [SMALL_STATE(565)] = 18506,
  [SMALL_STATE(566)] = 18522,
  [SMALL_STATE(567)] = 18532,
  [SMALL_STATE(568)] = 18546,
  [SMALL_STATE(569)] = 18562,
  [SMALL_STATE(570)] = 18578,
  [SMALL_STATE(571)] = 18590,
  [SMALL_STATE(572)] = 18606,
  [SMALL_STATE(573)] = 18622,
  [SMALL_STATE(574)] = 18636,
  [SMALL_STATE(575)] = 18652,
  [SMALL_STATE(576)] = 18668,
  [SMALL_STATE(577)] = 18684,
  [SMALL_STATE(578)] = 18700,
  [SMALL_STATE(579)] = 18714,
  [SMALL_STATE(580)] = 18728,
  [SMALL_STATE(581)] = 18742,
  [SMALL_STATE(582)] = 18758,
  [SMALL_STATE(583)] = 18770,
  [SMALL_STATE(584)] = 18786,
  [SMALL_STATE(585)] = 18798,
  [SMALL_STATE(586)] = 18814,
  [SMALL_STATE(587)] = 18830,
  [SMALL_STATE(588)] = 18846,
  [SMALL_STATE(589)] = 18856,
  [SMALL_STATE(590)] = 18870,
  [SMALL_STATE(591)] = 18886,
  [SMALL_STATE(592)] = 18902,
  [SMALL_STATE(593)] = 18918,
  [SMALL_STATE(594)] = 18934,
  [SMALL_STATE(595)] = 18950,
  [SMALL_STATE(596)] = 18964,
  [SMALL_STATE(597)] = 18978,
  [SMALL_STATE(598)] = 18994,
  [SMALL_STATE(599)] = 19008,
  [SMALL_STATE(600)] = 19022,
  [SMALL_STATE(601)] = 19038,
  [SMALL_STATE(602)] = 19054,
  [SMALL_STATE(603)] = 19070,
  [SMALL_STATE(604)] = 19084,
  [SMALL_STATE(605)] = 19098,
  [SMALL_STATE(606)] = 19114,
  [SMALL_STATE(607)] = 19130,
  [SMALL_STATE(608)] = 19146,
  [SMALL_STATE(609)] = 19162,
  [SMALL_STATE(610)] = 19178,
  [SMALL_STATE(611)] = 19194,
  [SMALL_STATE(612)] = 19208,
  [SMALL_STATE(613)] = 19224,
  [SMALL_STATE(614)] = 19238,
  [SMALL_STATE(615)] = 19252,
  [SMALL_STATE(616)] = 19266,
  [SMALL_STATE(617)] = 19280,
  [SMALL_STATE(618)] = 19296,
  [SMALL_STATE(619)] = 19306,
  [SMALL_STATE(620)] = 19320,
  [SMALL_STATE(621)] = 19336,
  [SMALL_STATE(622)] = 19352,
  [SMALL_STATE(623)] = 19366,
  [SMALL_STATE(624)] = 19382,
  [SMALL_STATE(625)] = 19394,
  [SMALL_STATE(626)] = 19404,
  [SMALL_STATE(627)] = 19420,
  [SMALL_STATE(628)] = 19436,
  [SMALL_STATE(629)] = 19450,
  [SMALL_STATE(630)] = 19466,
  [SMALL_STATE(631)] = 19482,
  [SMALL_STATE(632)] = 19496,
  [SMALL_STATE(633)] = 19510,
  [SMALL_STATE(634)] = 19524,
  [SMALL_STATE(635)] = 19538,
  [SMALL_STATE(636)] = 19552,
  [SMALL_STATE(637)] = 19566,
  [SMALL_STATE(638)] = 19582,
  [SMALL_STATE(639)] = 19598,
  [SMALL_STATE(640)] = 19614,
  [SMALL_STATE(641)] = 19628,
  [SMALL_STATE(642)] = 19638,
  [SMALL_STATE(643)] = 19650,
  [SMALL_STATE(644)] = 19664,
  [SMALL_STATE(645)] = 19674,
  [SMALL_STATE(646)] = 19688,
  [SMALL_STATE(647)] = 19702,
  [SMALL_STATE(648)] = 19716,
  [SMALL_STATE(649)] = 19730,
  [SMALL_STATE(650)] = 19742,
  [SMALL_STATE(651)] = 19756,
  [SMALL_STATE(652)] = 19772,
  [SMALL_STATE(653)] = 19786,
  [SMALL_STATE(654)] = 19802,
  [SMALL_STATE(655)] = 19816,
  [SMALL_STATE(656)] = 19830,
  [SMALL_STATE(657)] = 19844,
  [SMALL_STATE(658)] = 19853,
  [SMALL_STATE(659)] = 19862,
  [SMALL_STATE(660)] = 19873,
  [SMALL_STATE(661)] = 19886,
  [SMALL_STATE(662)] = 19895,
  [SMALL_STATE(663)] = 19904,
  [SMALL_STATE(664)] = 19917,
  [SMALL_STATE(665)] = 19926,
  [SMALL_STATE(666)] = 19935,
  [SMALL_STATE(667)] = 19944,
  [SMALL_STATE(668)] = 19957,
  [SMALL_STATE(669)] = 19966,
  [SMALL_STATE(670)] = 19977,
  [SMALL_STATE(671)] = 19986,
  [SMALL_STATE(672)] = 19995,
  [SMALL_STATE(673)] = 20004,
  [SMALL_STATE(674)] = 20013,
  [SMALL_STATE(675)] = 20022,
  [SMALL_STATE(676)] = 20033,
  [SMALL_STATE(677)] = 20042,
  [SMALL_STATE(678)] = 20051,
  [SMALL_STATE(679)] = 20060,
  [SMALL_STATE(680)] = 20071,
  [SMALL_STATE(681)] = 20080,
  [SMALL_STATE(682)] = 20089,
  [SMALL_STATE(683)] = 20100,
  [SMALL_STATE(684)] = 20109,
  [SMALL_STATE(685)] = 20118,
  [SMALL_STATE(686)] = 20127,
  [SMALL_STATE(687)] = 20136,
  [SMALL_STATE(688)] = 20145,
  [SMALL_STATE(689)] = 20158,
  [SMALL_STATE(690)] = 20167,
  [SMALL_STATE(691)] = 20176,
  [SMALL_STATE(692)] = 20185,
  [SMALL_STATE(693)] = 20198,
  [SMALL_STATE(694)] = 20211,
  [SMALL_STATE(695)] = 20222,
  [SMALL_STATE(696)] = 20231,
  [SMALL_STATE(697)] = 20240,
  [SMALL_STATE(698)] = 20251,
  [SMALL_STATE(699)] = 20264,
  [SMALL_STATE(700)] = 20277,
  [SMALL_STATE(701)] = 20290,
  [SMALL_STATE(702)] = 20303,
  [SMALL_STATE(703)] = 20316,
  [SMALL_STATE(704)] = 20329,
  [SMALL_STATE(705)] = 20342,
  [SMALL_STATE(706)] = 20351,
  [SMALL_STATE(707)] = 20360,
  [SMALL_STATE(708)] = 20369,
  [SMALL_STATE(709)] = 20378,
  [SMALL_STATE(710)] = 20387,
  [SMALL_STATE(711)] = 20398,
  [SMALL_STATE(712)] = 20409,
  [SMALL_STATE(713)] = 20422,
  [SMALL_STATE(714)] = 20431,
  [SMALL_STATE(715)] = 20444,
  [SMALL_STATE(716)] = 20453,
  [SMALL_STATE(717)] = 20466,
  [SMALL_STATE(718)] = 20475,
  [SMALL_STATE(719)] = 20488,
  [SMALL_STATE(720)] = 20501,
  [SMALL_STATE(721)] = 20514,
  [SMALL_STATE(722)] = 20523,
  [SMALL_STATE(723)] = 20532,
  [SMALL_STATE(724)] = 20541,
  [SMALL_STATE(725)] = 20554,
  [SMALL_STATE(726)] = 20567,
  [SMALL_STATE(727)] = 20578,
  [SMALL_STATE(728)] = 20591,
  [SMALL_STATE(729)] = 20600,
  [SMALL_STATE(730)] = 20611,
  [SMALL_STATE(731)] = 20622,
  [SMALL_STATE(732)] = 20635,
  [SMALL_STATE(733)] = 20644,
  [SMALL_STATE(734)] = 20657,
  [SMALL_STATE(735)] = 20668,
  [SMALL_STATE(736)] = 20681,
  [SMALL_STATE(737)] = 20692,
  [SMALL_STATE(738)] = 20703,
  [SMALL_STATE(739)] = 20714,
  [SMALL_STATE(740)] = 20723,
  [SMALL_STATE(741)] = 20734,
  [SMALL_STATE(742)] = 20745,
  [SMALL_STATE(743)] = 20754,
  [SMALL_STATE(744)] = 20763,
  [SMALL_STATE(745)] = 20772,
  [SMALL_STATE(746)] = 20785,
  [SMALL_STATE(747)] = 20794,
  [SMALL_STATE(748)] = 20803,
  [SMALL_STATE(749)] = 20811,
  [SMALL_STATE(750)] = 20819,
  [SMALL_STATE(751)] = 20827,
  [SMALL_STATE(752)] = 20835,
  [SMALL_STATE(753)] = 20843,
  [SMALL_STATE(754)] = 20851,
  [SMALL_STATE(755)] = 20859,
  [SMALL_STATE(756)] = 20867,
  [SMALL_STATE(757)] = 20875,
  [SMALL_STATE(758)] = 20885,
  [SMALL_STATE(759)] = 20893,
  [SMALL_STATE(760)] = 20901,
  [SMALL_STATE(761)] = 20909,
  [SMALL_STATE(762)] = 20917,
  [SMALL_STATE(763)] = 20925,
  [SMALL_STATE(764)] = 20933,
  [SMALL_STATE(765)] = 20941,
  [SMALL_STATE(766)] = 20949,
  [SMALL_STATE(767)] = 20957,
  [SMALL_STATE(768)] = 20965,
  [SMALL_STATE(769)] = 20973,
  [SMALL_STATE(770)] = 20983,
  [SMALL_STATE(771)] = 20991,
  [SMALL_STATE(772)] = 21001,
  [SMALL_STATE(773)] = 21009,
  [SMALL_STATE(774)] = 21017,
  [SMALL_STATE(775)] = 21025,
  [SMALL_STATE(776)] = 21033,
  [SMALL_STATE(777)] = 21041,
  [SMALL_STATE(778)] = 21049,
  [SMALL_STATE(779)] = 21057,
  [SMALL_STATE(780)] = 21065,
  [SMALL_STATE(781)] = 21073,
  [SMALL_STATE(782)] = 21081,
  [SMALL_STATE(783)] = 21089,
  [SMALL_STATE(784)] = 21097,
  [SMALL_STATE(785)] = 21105,
  [SMALL_STATE(786)] = 21115,
  [SMALL_STATE(787)] = 21123,
  [SMALL_STATE(788)] = 21131,
  [SMALL_STATE(789)] = 21139,
  [SMALL_STATE(790)] = 21147,
  [SMALL_STATE(791)] = 21155,
  [SMALL_STATE(792)] = 21163,
  [SMALL_STATE(793)] = 21171,
  [SMALL_STATE(794)] = 21179,
  [SMALL_STATE(795)] = 21189,
  [SMALL_STATE(796)] = 21197,
  [SMALL_STATE(797)] = 21205,
  [SMALL_STATE(798)] = 21215,
  [SMALL_STATE(799)] = 21223,
  [SMALL_STATE(800)] = 21231,
  [SMALL_STATE(801)] = 21239,
  [SMALL_STATE(802)] = 21247,
  [SMALL_STATE(803)] = 21257,
  [SMALL_STATE(804)] = 21265,
  [SMALL_STATE(805)] = 21273,
  [SMALL_STATE(806)] = 21281,
  [SMALL_STATE(807)] = 21289,
  [SMALL_STATE(808)] = 21297,
  [SMALL_STATE(809)] = 21307,
  [SMALL_STATE(810)] = 21315,
  [SMALL_STATE(811)] = 21323,
  [SMALL_STATE(812)] = 21331,
  [SMALL_STATE(813)] = 21339,
  [SMALL_STATE(814)] = 21347,
  [SMALL_STATE(815)] = 21355,
  [SMALL_STATE(816)] = 21363,
  [SMALL_STATE(817)] = 21371,
  [SMALL_STATE(818)] = 21379,
  [SMALL_STATE(819)] = 21387,
  [SMALL_STATE(820)] = 21395,
  [SMALL_STATE(821)] = 21403,
  [SMALL_STATE(822)] = 21411,
  [SMALL_STATE(823)] = 21419,
  [SMALL_STATE(824)] = 21427,
  [SMALL_STATE(825)] = 21437,
  [SMALL_STATE(826)] = 21445,
  [SMALL_STATE(827)] = 21453,
  [SMALL_STATE(828)] = 21461,
  [SMALL_STATE(829)] = 21469,
  [SMALL_STATE(830)] = 21477,
  [SMALL_STATE(831)] = 21485,
  [SMALL_STATE(832)] = 21493,
  [SMALL_STATE(833)] = 21501,
  [SMALL_STATE(834)] = 21509,
  [SMALL_STATE(835)] = 21517,
  [SMALL_STATE(836)] = 21525,
  [SMALL_STATE(837)] = 21533,
  [SMALL_STATE(838)] = 21541,
  [SMALL_STATE(839)] = 21549,
  [SMALL_STATE(840)] = 21557,
  [SMALL_STATE(841)] = 21565,
  [SMALL_STATE(842)] = 21573,
  [SMALL_STATE(843)] = 21581,
  [SMALL_STATE(844)] = 21589,
  [SMALL_STATE(845)] = 21597,
  [SMALL_STATE(846)] = 21605,
  [SMALL_STATE(847)] = 21613,
  [SMALL_STATE(848)] = 21621,
  [SMALL_STATE(849)] = 21629,
  [SMALL_STATE(850)] = 21637,
  [SMALL_STATE(851)] = 21645,
  [SMALL_STATE(852)] = 21653,
  [SMALL_STATE(853)] = 21661,
  [SMALL_STATE(854)] = 21669,
  [SMALL_STATE(855)] = 21677,
  [SMALL_STATE(856)] = 21685,
  [SMALL_STATE(857)] = 21693,
  [SMALL_STATE(858)] = 21701,
  [SMALL_STATE(859)] = 21709,
  [SMALL_STATE(860)] = 21717,
  [SMALL_STATE(861)] = 21725,
  [SMALL_STATE(862)] = 21733,
  [SMALL_STATE(863)] = 21741,
  [SMALL_STATE(864)] = 21749,
  [SMALL_STATE(865)] = 21757,
  [SMALL_STATE(866)] = 21767,
  [SMALL_STATE(867)] = 21777,
  [SMALL_STATE(868)] = 21785,
  [SMALL_STATE(869)] = 21793,
  [SMALL_STATE(870)] = 21801,
  [SMALL_STATE(871)] = 21809,
  [SMALL_STATE(872)] = 21817,
  [SMALL_STATE(873)] = 21825,
  [SMALL_STATE(874)] = 21833,
  [SMALL_STATE(875)] = 21841,
  [SMALL_STATE(876)] = 21849,
  [SMALL_STATE(877)] = 21857,
  [SMALL_STATE(878)] = 21865,
  [SMALL_STATE(879)] = 21873,
  [SMALL_STATE(880)] = 21881,
  [SMALL_STATE(881)] = 21889,
  [SMALL_STATE(882)] = 21897,
  [SMALL_STATE(883)] = 21905,
  [SMALL_STATE(884)] = 21915,
  [SMALL_STATE(885)] = 21925,
  [SMALL_STATE(886)] = 21933,
  [SMALL_STATE(887)] = 21941,
  [SMALL_STATE(888)] = 21949,
  [SMALL_STATE(889)] = 21959,
  [SMALL_STATE(890)] = 21967,
  [SMALL_STATE(891)] = 21975,
  [SMALL_STATE(892)] = 21983,
  [SMALL_STATE(893)] = 21991,
  [SMALL_STATE(894)] = 21999,
  [SMALL_STATE(895)] = 22007,
  [SMALL_STATE(896)] = 22015,
  [SMALL_STATE(897)] = 22023,
  [SMALL_STATE(898)] = 22031,
  [SMALL_STATE(899)] = 22039,
  [SMALL_STATE(900)] = 22047,
  [SMALL_STATE(901)] = 22055,
  [SMALL_STATE(902)] = 22063,
  [SMALL_STATE(903)] = 22071,
  [SMALL_STATE(904)] = 22079,
  [SMALL_STATE(905)] = 22087,
  [SMALL_STATE(906)] = 22095,
  [SMALL_STATE(907)] = 22103,
  [SMALL_STATE(908)] = 22111,
  [SMALL_STATE(909)] = 22121,
  [SMALL_STATE(910)] = 22129,
  [SMALL_STATE(911)] = 22137,
  [SMALL_STATE(912)] = 22145,
  [SMALL_STATE(913)] = 22153,
  [SMALL_STATE(914)] = 22161,
  [SMALL_STATE(915)] = 22169,
  [SMALL_STATE(916)] = 22177,
  [SMALL_STATE(917)] = 22185,
  [SMALL_STATE(918)] = 22193,
  [SMALL_STATE(919)] = 22201,
  [SMALL_STATE(920)] = 22209,
};

static const TSParseActionEntry ts_parse_actions[] = {
  [0] = {.entry = {.count = 0, .reusable = false}},
  [1] = {.entry = {.count = 1, .reusable = false}}, RECOVER(),
  [3] = {.entry = {.count = 1, .reusable = false}}, SHIFT_EXTRA(),
  [5] = {.entry = {.count = 1, .reusable = true}}, SHIFT_EXTRA(),
  [7] = {.entry = {.count = 1, .reusable = true}}, SHIFT(824),
  [9] = {.entry = {.count = 1, .reusable = true}}, SHIFT(39),
  [11] = {.entry = {.count = 1, .reusable = true}}, SHIFT(842),
  [13] = {.entry = {.count = 1, .reusable = false}}, SHIFT(842),
  [15] = {.entry = {.count = 1, .reusable = false}}, SHIFT(76),
  [17] = {.entry = {.count = 1, .reusable = true}}, SHIFT(76),
  [19] = {.entry = {.count = 1, .reusable = true}}, SHIFT(841),
  [21] = {.entry = {.count = 1, .reusable = true}}, SHIFT(304),
  [23] = {.entry = {.count = 1, .reusable = true}}, SHIFT(219),
  [25] = {.entry = {.count = 1, .reusable = true}}, SHIFT(362),
  [27] = {.entry = {.count = 1, .reusable = true}}, SHIFT(820),
  [29] = {.entry = {.count = 1, .reusable = true}}, SHIFT(84),
  [31] = {.entry = {.count = 1, .reusable = true}}, SHIFT(551),
  [33] = {.entry = {.count = 1, .reusable = true}}, SHIFT(558),
  [35] = {.entry = {.count = 1, .reusable = true}}, SHIFT(97),
  [37] = {.entry = {.count = 1, .reusable = true}}, SHIFT(557),
  [39] = {.entry = {.count = 1, .reusable = true}}, SHIFT(554),
  [41] = {.entry = {.count = 1, .reusable = true}}, SHIFT(375),
  [43] = {.entry = {.count = 1, .reusable = true}}, SHIFT(523),
  [45] = {.entry = {.count = 1, .reusable = false}}, SHIFT(522),
  [47] = {.entry = {.count = 1, .reusable = true}}, SHIFT(521),
  [49] = {.entry = {.count = 1, .reusable = true}}, SHIFT(520),
  [51] = {.entry = {.count = 1, .reusable = true}}, SHIFT(885),
  [53] = {.entry = {.count = 1, .reusable = true}}, SHIFT(519),
  [55] = {.entry = {.count = 1, .reusable = true}}, SHIFT(187),
  [57] = {.entry = {.count = 1, .reusable = true}}, SHIFT(518),
  [59] = {.entry = {.count = 1, .reusable = true}}, SHIFT(516),
  [61] = {.entry = {.count = 1, .reusable = true}}, SHIFT(487),
  [63] = {.entry = {.count = 1, .reusable = true}}, SHIFT(49),
  [65] = {.entry = {.count = 1, .reusable = false}}, SHIFT(458),
  [67] = {.entry = {.count = 1, .reusable = true}}, SHIFT(129),
  [69] = {.entry = {.count = 1, .reusable = false}}, SHIFT(257),
  [71] = {.entry = {.count = 1, .reusable = true}}, SHIFT(586),
  [73] = {.entry = {.count = 1, .reusable = true}}, SHIFT(591),
  [75] = {.entry = {.count = 1, .reusable = true}}, SHIFT(108),
  [77] = {.entry = {.count = 1, .reusable = true}}, SHIFT(590),
  [79] = {.entry = {.count = 1, .reusable = true}}, SHIFT(587),
  [81] = {.entry = {.count = 1, .reusable = true}}, SHIFT(373),
  [83] = {.entry = {.count = 1, .reusable = true}}, SHIFT(543),
  [85] = {.entry = {.count = 1, .reusable = false}}, SHIFT(542),
  [87] = {.entry = {.count = 1, .reusable = true}}, SHIFT(541),
  [89] = {.entry = {.count = 1, .reusable = true}}, SHIFT(540),
  [91] = {.entry = {.count = 1, .reusable = true}}, SHIFT(852),
  [93] = {.entry = {.count = 1, .reusable = true}}, SHIFT(538),
  [95] = {.entry = {.count = 1, .reusable = true}}, SHIFT(203),
  [97] = {.entry = {.count = 1, .reusable = true}}, SHIFT(536),
  [99] = {.entry = {.count = 1, .reusable = true}}, SHIFT(535),
  [101] = {.entry = {.count = 1, .reusable = true}}, SHIFT(515),
  [103] = {.entry = {.count = 1, .reusable = false}}, SHIFT(449),
  [105] = {.entry = {.count = 1, .reusable = true}}, SHIFT(453),
  [107] = {.entry = {.count = 1, .reusable = true}}, SHIFT(115),
  [109] = {.entry = {.count = 1, .reusable = true}}, SHIFT(142),
  [111] = {.entry = {.count = 1, .reusable = true}}, SHIFT(128),
  [113] = {.entry = {.count = 1, .reusable = true}}, SHIFT(154),
  [115] = {.entry = {.count = 1, .reusable = true}}, SHIFT(86),
  [117] = {.entry = {.count = 1, .reusable = true}}, SHIFT(182),
  [119] = {.entry = {.count = 1, .reusable = true}}, SHIFT(172),
  [121] = {.entry = {.count = 1, .reusable = true}}, SHIFT(85),
  [123] = {.entry = {.count = 1, .reusable = true}}, SHIFT(117),
  [125] = {.entry = {.count = 1, .reusable = true}}, SHIFT(106),
  [127] = {.entry = {.count = 1, .reusable = true}}, SHIFT(200),
  [129] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_sequence_stmt_par_repeat1, 4, .production_id = 16),
  [131] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_sequence_stmt_par_repeat1, 3, .production_id = 10),
  [133] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_sequence_stmt_alt_repeat1, 2, .production_id = 24),
  [135] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym__sequence_subdocument, 2), SHIFT_REPEAT(586),
  [138] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym__sequence_subdocument, 2), SHIFT_REPEAT(591),
  [141] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym__sequence_subdocument, 2), SHIFT_REPEAT(108),
  [144] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym__sequence_subdocument, 2), SHIFT_REPEAT(590),
  [147] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym__sequence_subdocument, 2), SHIFT_REPEAT(587),
  [150] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym__sequence_subdocument, 2), SHIFT_REPEAT(373),
  [153] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym__sequence_subdocument, 2), SHIFT_REPEAT(543),
  [156] = {.entry = {.count = 2, .reusable = false}}, REDUCE(aux_sym__sequence_subdocument, 2), SHIFT_REPEAT(542),
  [159] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym__sequence_subdocument, 2), SHIFT_REPEAT(541),
  [162] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym__sequence_subdocument, 2), SHIFT_REPEAT(540),
  [165] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym__sequence_subdocument, 2), SHIFT_REPEAT(852),
  [168] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym__sequence_subdocument, 2), SHIFT_REPEAT(538),
  [171] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym__sequence_subdocument, 2),
  [173] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym__sequence_subdocument, 2), SHIFT_REPEAT(536),
  [176] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym__sequence_subdocument, 2), SHIFT_REPEAT(535),
  [179] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym__sequence_subdocument, 2), SHIFT_REPEAT(515),
  [182] = {.entry = {.count = 2, .reusable = false}}, REDUCE(aux_sym__sequence_subdocument, 2), SHIFT_REPEAT(449),
  [185] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym__sequence_subdocument, 2), SHIFT_REPEAT(115),
  [188] = {.entry = {.count = 2, .reusable = false}}, REDUCE(aux_sym__sequence_subdocument, 2), SHIFT_REPEAT(257),
  [191] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym__sequence_subdocument, 2), SHIFT_REPEAT(551),
  [194] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym__sequence_subdocument, 2), SHIFT_REPEAT(558),
  [197] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym__sequence_subdocument, 2), SHIFT_REPEAT(97),
  [200] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym__sequence_subdocument, 2), SHIFT_REPEAT(557),
  [203] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym__sequence_subdocument, 2), SHIFT_REPEAT(554),
  [206] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym__sequence_subdocument, 2), SHIFT_REPEAT(375),
  [209] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym__sequence_subdocument, 2), SHIFT_REPEAT(523),
  [212] = {.entry = {.count = 2, .reusable = false}}, REDUCE(aux_sym__sequence_subdocument, 2), SHIFT_REPEAT(522),
  [215] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym__sequence_subdocument, 2), SHIFT_REPEAT(521),
  [218] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym__sequence_subdocument, 2), SHIFT_REPEAT(520),
  [221] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym__sequence_subdocument, 2), SHIFT_REPEAT(885),
  [224] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym__sequence_subdocument, 2), SHIFT_REPEAT(519),
  [227] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym__sequence_subdocument, 2), SHIFT_REPEAT(518),
  [230] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym__sequence_subdocument, 2), SHIFT_REPEAT(516),
  [233] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym__sequence_subdocument, 2), SHIFT_REPEAT(487),
  [236] = {.entry = {.count = 2, .reusable = false}}, REDUCE(aux_sym__sequence_subdocument, 2), SHIFT_REPEAT(458),
  [239] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym__sequence_subdocument, 2), SHIFT_REPEAT(129),
  [242] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_diagram_sequence, 2),
  [244] = {.entry = {.count = 1, .reusable = true}}, SHIFT(623),
  [246] = {.entry = {.count = 1, .reusable = true}}, SHIFT(585),
  [248] = {.entry = {.count = 1, .reusable = true}}, SHIFT(173),
  [250] = {.entry = {.count = 1, .reusable = true}}, SHIFT(597),
  [252] = {.entry = {.count = 1, .reusable = true}}, SHIFT(600),
  [254] = {.entry = {.count = 1, .reusable = true}}, SHIFT(386),
  [256] = {.entry = {.count = 1, .reusable = true}}, SHIFT(601),
  [258] = {.entry = {.count = 1, .reusable = false}}, SHIFT(602),
  [260] = {.entry = {.count = 1, .reusable = true}}, SHIFT(605),
  [262] = {.entry = {.count = 1, .reusable = true}}, SHIFT(606),
  [264] = {.entry = {.count = 1, .reusable = true}}, SHIFT(773),
  [266] = {.entry = {.count = 1, .reusable = true}}, SHIFT(607),
  [268] = {.entry = {.count = 1, .reusable = true}}, SHIFT(617),
  [270] = {.entry = {.count = 1, .reusable = true}}, SHIFT(620),
  [272] = {.entry = {.count = 1, .reusable = true}}, SHIFT(621),
  [274] = {.entry = {.count = 1, .reusable = false}}, SHIFT(447),
  [276] = {.entry = {.count = 1, .reusable = true}}, SHIFT(38),
  [278] = {.entry = {.count = 1, .reusable = true}}, SHIFT(568),
  [280] = {.entry = {.count = 1, .reusable = true}}, SHIFT(574),
  [282] = {.entry = {.count = 1, .reusable = true}}, SHIFT(156),
  [284] = {.entry = {.count = 1, .reusable = true}}, SHIFT(572),
  [286] = {.entry = {.count = 1, .reusable = true}}, SHIFT(569),
  [288] = {.entry = {.count = 1, .reusable = true}}, SHIFT(385),
  [290] = {.entry = {.count = 1, .reusable = true}}, SHIFT(532),
  [292] = {.entry = {.count = 1, .reusable = false}}, SHIFT(531),
  [294] = {.entry = {.count = 1, .reusable = true}}, SHIFT(530),
  [296] = {.entry = {.count = 1, .reusable = true}}, SHIFT(529),
  [298] = {.entry = {.count = 1, .reusable = true}}, SHIFT(869),
  [300] = {.entry = {.count = 1, .reusable = true}}, SHIFT(528),
  [302] = {.entry = {.count = 1, .reusable = true}}, SHIFT(171),
  [304] = {.entry = {.count = 1, .reusable = true}}, SHIFT(526),
  [306] = {.entry = {.count = 1, .reusable = true}}, SHIFT(525),
  [308] = {.entry = {.count = 1, .reusable = true}}, SHIFT(497),
  [310] = {.entry = {.count = 1, .reusable = false}}, SHIFT(456),
  [312] = {.entry = {.count = 1, .reusable = true}}, SHIFT(178),
  [314] = {.entry = {.count = 1, .reusable = true}}, SHIFT(210),
  [316] = {.entry = {.count = 1, .reusable = true}}, SHIFT(107),
  [318] = {.entry = {.count = 1, .reusable = true}}, SHIFT(113),
  [320] = {.entry = {.count = 1, .reusable = true}}, SHIFT(114),
  [322] = {.entry = {.count = 1, .reusable = true}}, SHIFT(118),
  [324] = {.entry = {.count = 1, .reusable = true}}, SHIFT(119),
  [326] = {.entry = {.count = 1, .reusable = true}}, SHIFT(139),
  [328] = {.entry = {.count = 1, .reusable = true}}, SHIFT(180),
  [330] = {.entry = {.count = 1, .reusable = true}}, SHIFT(177),
  [332] = {.entry = {.count = 1, .reusable = true}}, SHIFT(176),
  [334] = {.entry = {.count = 1, .reusable = true}}, SHIFT(204),
  [336] = {.entry = {.count = 1, .reusable = true}}, SHIFT(209),
  [338] = {.entry = {.count = 1, .reusable = true}}, SHIFT(189),
  [340] = {.entry = {.count = 1, .reusable = true}}, SHIFT(185),
  [342] = {.entry = {.count = 1, .reusable = true}}, SHIFT(170),
  [344] = {.entry = {.count = 1, .reusable = true}}, SHIFT(169),
  [346] = {.entry = {.count = 1, .reusable = true}}, SHIFT(201),
  [348] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_diagram_sequence_repeat1, 2),
  [350] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_diagram_sequence_repeat1, 2), SHIFT_REPEAT(623),
  [353] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_diagram_sequence_repeat1, 2), SHIFT_REPEAT(585),
  [356] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_diagram_sequence_repeat1, 2), SHIFT_REPEAT(173),
  [359] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_diagram_sequence_repeat1, 2), SHIFT_REPEAT(597),
  [362] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_diagram_sequence_repeat1, 2), SHIFT_REPEAT(600),
  [365] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_diagram_sequence_repeat1, 2), SHIFT_REPEAT(386),
  [368] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_diagram_sequence_repeat1, 2), SHIFT_REPEAT(601),
  [371] = {.entry = {.count = 2, .reusable = false}}, REDUCE(aux_sym_diagram_sequence_repeat1, 2), SHIFT_REPEAT(602),
  [374] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_diagram_sequence_repeat1, 2), SHIFT_REPEAT(605),
  [377] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_diagram_sequence_repeat1, 2), SHIFT_REPEAT(606),
  [380] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_diagram_sequence_repeat1, 2), SHIFT_REPEAT(773),
  [383] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_diagram_sequence_repeat1, 2), SHIFT_REPEAT(607),
  [386] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_diagram_sequence_repeat1, 2), SHIFT_REPEAT(617),
  [389] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_diagram_sequence_repeat1, 2), SHIFT_REPEAT(620),
  [392] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_diagram_sequence_repeat1, 2), SHIFT_REPEAT(621),
  [395] = {.entry = {.count = 2, .reusable = false}}, REDUCE(aux_sym_diagram_sequence_repeat1, 2), SHIFT_REPEAT(447),
  [398] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_diagram_sequence_repeat1, 2), SHIFT_REPEAT(38),
  [401] = {.entry = {.count = 2, .reusable = false}}, REDUCE(aux_sym_diagram_sequence_repeat1, 2), SHIFT_REPEAT(257),
  [404] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_diagram_sequence, 1),
  [406] = {.entry = {.count = 1, .reusable = true}}, SHIFT(19),
  [408] = {.entry = {.count = 1, .reusable = true}}, SHIFT(126),
  [410] = {.entry = {.count = 1, .reusable = true}}, SHIFT(137),
  [412] = {.entry = {.count = 1, .reusable = true}}, SHIFT(145),
  [414] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym__sequence_subdocument, 2), SHIFT_REPEAT(568),
  [417] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym__sequence_subdocument, 2), SHIFT_REPEAT(574),
  [420] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym__sequence_subdocument, 2), SHIFT_REPEAT(156),
  [423] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym__sequence_subdocument, 2), SHIFT_REPEAT(572),
  [426] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym__sequence_subdocument, 2), SHIFT_REPEAT(569),
  [429] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym__sequence_subdocument, 2), SHIFT_REPEAT(385),
  [432] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym__sequence_subdocument, 2), SHIFT_REPEAT(532),
  [435] = {.entry = {.count = 2, .reusable = false}}, REDUCE(aux_sym__sequence_subdocument, 2), SHIFT_REPEAT(531),
  [438] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym__sequence_subdocument, 2), SHIFT_REPEAT(530),
  [441] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym__sequence_subdocument, 2), SHIFT_REPEAT(529),
  [444] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym__sequence_subdocument, 2), SHIFT_REPEAT(869),
  [447] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym__sequence_subdocument, 2), SHIFT_REPEAT(528),
  [450] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym__sequence_subdocument, 2), SHIFT_REPEAT(526),
  [453] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym__sequence_subdocument, 2), SHIFT_REPEAT(525),
  [456] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym__sequence_subdocument, 2), SHIFT_REPEAT(497),
  [459] = {.entry = {.count = 2, .reusable = false}}, REDUCE(aux_sym__sequence_subdocument, 2), SHIFT_REPEAT(456),
  [462] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym__sequence_subdocument, 2), SHIFT_REPEAT(178),
  [465] = {.entry = {.count = 1, .reusable = true}}, SHIFT(132),
  [467] = {.entry = {.count = 1, .reusable = true}}, SHIFT(135),
  [469] = {.entry = {.count = 1, .reusable = true}}, SHIFT(130),
  [471] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_flow_vertex, 1),
  [473] = {.entry = {.count = 1, .reusable = false}}, SHIFT(387),
  [475] = {.entry = {.count = 1, .reusable = false}}, SHIFT(425),
  [477] = {.entry = {.count = 1, .reusable = false}}, SHIFT(422),
  [479] = {.entry = {.count = 1, .reusable = true}}, SHIFT(418),
  [481] = {.entry = {.count = 1, .reusable = true}}, SHIFT(416),
  [483] = {.entry = {.count = 1, .reusable = true}}, SHIFT(388),
  [485] = {.entry = {.count = 1, .reusable = true}}, SHIFT(407),
  [487] = {.entry = {.count = 1, .reusable = true}}, SHIFT(391),
  [489] = {.entry = {.count = 1, .reusable = true}}, SHIFT(395),
  [491] = {.entry = {.count = 1, .reusable = true}}, SHIFT(396),
  [493] = {.entry = {.count = 1, .reusable = true}}, SHIFT(397),
  [495] = {.entry = {.count = 1, .reusable = true}}, SHIFT(399),
  [497] = {.entry = {.count = 1, .reusable = true}}, SHIFT(402),
  [499] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_flow_vertex, 1),
  [501] = {.entry = {.count = 1, .reusable = true}}, SHIFT(421),
  [503] = {.entry = {.count = 1, .reusable = true}}, SHIFT(231),
  [505] = {.entry = {.count = 1, .reusable = true}}, SHIFT(429),
  [507] = {.entry = {.count = 1, .reusable = true}}, SHIFT(72),
  [509] = {.entry = {.count = 1, .reusable = true}}, SHIFT(256),
  [511] = {.entry = {.count = 1, .reusable = true}}, SHIFT(254),
  [513] = {.entry = {.count = 1, .reusable = true}}, SHIFT(253),
  [515] = {.entry = {.count = 1, .reusable = true}}, SHIFT(248),
  [517] = {.entry = {.count = 1, .reusable = true}}, SHIFT(247),
  [519] = {.entry = {.count = 1, .reusable = true}}, SHIFT(228),
  [521] = {.entry = {.count = 1, .reusable = false}}, SHIFT(228),
  [523] = {.entry = {.count = 1, .reusable = true}}, SHIFT(246),
  [525] = {.entry = {.count = 1, .reusable = true}}, SHIFT(255),
  [527] = {.entry = {.count = 1, .reusable = true}}, SHIFT(73),
  [529] = {.entry = {.count = 1, .reusable = true}}, SHIFT(262),
  [531] = {.entry = {.count = 1, .reusable = true}}, SHIFT(69),
  [533] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_state_composite_body_repeat1, 2), SHIFT_REPEAT(421),
  [536] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_state_composite_body_repeat1, 2),
  [538] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_state_composite_body_repeat1, 2), SHIFT_REPEAT(429),
  [541] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_state_composite_body_repeat1, 2), SHIFT_REPEAT(80),
  [544] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_state_composite_body_repeat1, 2), SHIFT_REPEAT(256),
  [547] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_state_composite_body_repeat1, 2), SHIFT_REPEAT(254),
  [550] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_state_composite_body_repeat1, 2), SHIFT_REPEAT(253),
  [553] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_state_composite_body_repeat1, 2), SHIFT_REPEAT(248),
  [556] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_state_composite_body_repeat1, 2), SHIFT_REPEAT(247),
  [559] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_state_composite_body_repeat1, 2), SHIFT_REPEAT(228),
  [562] = {.entry = {.count = 2, .reusable = false}}, REDUCE(aux_sym_state_composite_body_repeat1, 2), SHIFT_REPEAT(228),
  [565] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_state_composite_body_repeat1, 2), SHIFT_REPEAT(246),
  [568] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_diagram_state, 5),
  [570] = {.entry = {.count = 1, .reusable = true}}, SHIFT(428),
  [572] = {.entry = {.count = 1, .reusable = true}}, SHIFT(275),
  [574] = {.entry = {.count = 1, .reusable = false}}, SHIFT(275),
  [576] = {.entry = {.count = 1, .reusable = true}}, SHIFT(265),
  [578] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_diagram_state, 4),
  [580] = {.entry = {.count = 1, .reusable = true}}, SHIFT(81),
  [582] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_diagram_gantt_repeat1, 2),
  [584] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_diagram_gantt_repeat1, 2), SHIFT_REPEAT(692),
  [587] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_diagram_gantt_repeat1, 2), SHIFT_REPEAT(698),
  [590] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_diagram_gantt_repeat1, 2), SHIFT_REPEAT(700),
  [593] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_diagram_gantt_repeat1, 2), SHIFT_REPEAT(701),
  [596] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_diagram_gantt_repeat1, 2), SHIFT_REPEAT(702),
  [599] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_diagram_gantt_repeat1, 2), SHIFT_REPEAT(712),
  [602] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_diagram_gantt_repeat1, 2), SHIFT_REPEAT(714),
  [605] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_diagram_gantt_repeat1, 2), SHIFT_REPEAT(719),
  [608] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_diagram_gantt_repeat1, 2), SHIFT_REPEAT(745),
  [611] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_diagram_gantt_repeat1, 2), SHIFT_REPEAT(74),
  [614] = {.entry = {.count = 2, .reusable = false}}, REDUCE(aux_sym_diagram_gantt_repeat1, 2), SHIFT_REPEAT(628),
  [617] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_diagram_gantt, 2),
  [619] = {.entry = {.count = 1, .reusable = true}}, SHIFT(692),
  [621] = {.entry = {.count = 1, .reusable = true}}, SHIFT(698),
  [623] = {.entry = {.count = 1, .reusable = true}}, SHIFT(700),
  [625] = {.entry = {.count = 1, .reusable = true}}, SHIFT(701),
  [627] = {.entry = {.count = 1, .reusable = true}}, SHIFT(702),
  [629] = {.entry = {.count = 1, .reusable = true}}, SHIFT(712),
  [631] = {.entry = {.count = 1, .reusable = true}}, SHIFT(714),
  [633] = {.entry = {.count = 1, .reusable = true}}, SHIFT(719),
  [635] = {.entry = {.count = 1, .reusable = true}}, SHIFT(745),
  [637] = {.entry = {.count = 1, .reusable = true}}, SHIFT(77),
  [639] = {.entry = {.count = 1, .reusable = false}}, SHIFT(628),
  [641] = {.entry = {.count = 1, .reusable = true}}, SHIFT(82),
  [643] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_diagram_gantt, 3),
  [645] = {.entry = {.count = 1, .reusable = true}}, SHIFT(74),
  [647] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_diagram_state, 3),
  [649] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_flow_vertex_id, 1),
  [651] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_flow_vertex_id, 1),
  [653] = {.entry = {.count = 1, .reusable = true}}, SHIFT(224),
  [655] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_sequence_stmt_par, 5, .production_id = 16),
  [657] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_sequence_stmt_par, 5, .production_id = 16),
  [659] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_sequence_stmt_properties, 4),
  [661] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_sequence_stmt_properties, 4),
  [663] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_sequence_stmt_link, 4),
  [665] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_sequence_stmt_link, 4),
  [667] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_sequence_stmt_links, 4),
  [669] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_sequence_stmt_links, 4),
  [671] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_sequence_stmt_participant, 3),
  [673] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_sequence_stmt_participant, 3),
  [675] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_sequence_stmt_deactivate, 3),
  [677] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_sequence_stmt_deactivate, 3),
  [679] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_sequence_stmt_activate, 3),
  [681] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_sequence_stmt_activate, 3),
  [683] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_sequence_stmt_actor, 3),
  [685] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_sequence_stmt_actor, 3),
  [687] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_sequence_text, 1),
  [689] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_sequence_text, 1),
  [691] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym__sequence_rest_text, 1),
  [693] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym__sequence_rest_text, 1),
  [695] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym__sequence_stmt, 1),
  [697] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym__sequence_stmt, 1),
  [699] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_sequence_stmt_autonumber, 1),
  [701] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_sequence_stmt_autonumber, 1),
  [703] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_sequence_stmt_note, 8),
  [705] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_sequence_stmt_note, 8),
  [707] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_sequence_stmt_signal, 7),
  [709] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_sequence_stmt_signal, 7),
  [711] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_sequence_stmt_signal, 6),
  [713] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_sequence_stmt_signal, 6),
  [715] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_sequence_stmt_par, 6, .production_id = 16),
  [717] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_sequence_stmt_par, 6, .production_id = 16),
  [719] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_sequence_stmt_alt, 6, .production_id = 16),
  [721] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_sequence_stmt_alt, 6, .production_id = 16),
  [723] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_sequence_stmt_note, 6),
  [725] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_sequence_stmt_note, 6),
  [727] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_sequence_stmt_participant, 5, .production_id = 12),
  [729] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_sequence_stmt_participant, 5, .production_id = 12),
  [731] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_sequence_stmt_par, 5, .production_id = 10),
  [733] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_sequence_stmt_par, 5, .production_id = 10),
  [735] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_sequence_stmt_alt, 5, .production_id = 16),
  [737] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_sequence_stmt_alt, 5, .production_id = 16),
  [739] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_sequence_stmt_opt, 5, .production_id = 15),
  [741] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_sequence_stmt_opt, 5, .production_id = 15),
  [743] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_sequence_stmt_rect, 5, .production_id = 14),
  [745] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_sequence_stmt_rect, 5, .production_id = 14),
  [747] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_sequence_stmt_loop, 5, .production_id = 13),
  [749] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_sequence_stmt_loop, 5, .production_id = 13),
  [751] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym__sequence_subdocument, 1),
  [753] = {.entry = {.count = 1, .reusable = false}}, REDUCE(aux_sym__sequence_subdocument, 1),
  [755] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_sequence_stmt_actor, 5, .production_id = 12),
  [757] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_sequence_stmt_actor, 5, .production_id = 12),
  [759] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_sequence_stmt_par, 4, .production_id = 10),
  [761] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_sequence_stmt_par, 4, .production_id = 10),
  [763] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_sequence_stmt_opt, 4),
  [765] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_sequence_stmt_opt, 4),
  [767] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_sequence_stmt_rect, 4),
  [769] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_sequence_stmt_rect, 4),
  [771] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_sequence_stmt_details, 4),
  [773] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_sequence_stmt_details, 4),
  [775] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_sequence_stmt_title, 4, .production_id = 9),
  [777] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_sequence_stmt_title, 4, .production_id = 9),
  [779] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_sequence_stmt_loop, 4),
  [781] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_sequence_stmt_loop, 4),
  [783] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_mmap_node, 2),
  [785] = {.entry = {.count = 1, .reusable = false}}, SHIFT(413),
  [787] = {.entry = {.count = 1, .reusable = false}}, SHIFT(412),
  [789] = {.entry = {.count = 1, .reusable = true}}, SHIFT(411),
  [791] = {.entry = {.count = 1, .reusable = true}}, SHIFT(410),
  [793] = {.entry = {.count = 1, .reusable = true}}, SHIFT(408),
  [795] = {.entry = {.count = 1, .reusable = true}}, SHIFT(390),
  [797] = {.entry = {.count = 1, .reusable = true}}, SHIFT(769),
  [799] = {.entry = {.count = 1, .reusable = true}}, SHIFT(771),
  [801] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_mmap_node, 2),
  [803] = {.entry = {.count = 1, .reusable = true}}, SHIFT(340),
  [805] = {.entry = {.count = 1, .reusable = true}}, SHIFT(405),
  [807] = {.entry = {.count = 1, .reusable = true}}, SHIFT(409),
  [809] = {.entry = {.count = 1, .reusable = true}}, SHIFT(216),
  [811] = {.entry = {.count = 1, .reusable = true}}, SHIFT(420),
  [813] = {.entry = {.count = 1, .reusable = true}}, SHIFT(432),
  [815] = {.entry = {.count = 1, .reusable = false}}, SHIFT(409),
  [817] = {.entry = {.count = 1, .reusable = true}}, SHIFT(349),
  [819] = {.entry = {.count = 1, .reusable = true}}, SHIFT(350),
  [821] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_flow_stmt_vertice_repeat1, 2),
  [823] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_flow_stmt_vertice_repeat1, 2), SHIFT_REPEAT(670),
  [826] = {.entry = {.count = 2, .reusable = false}}, REDUCE(aux_sym_flow_stmt_vertice_repeat1, 2), SHIFT_REPEAT(749),
  [829] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_flow_stmt_vertice, 2),
  [831] = {.entry = {.count = 1, .reusable = true}}, SHIFT(670),
  [833] = {.entry = {.count = 1, .reusable = false}}, SHIFT(749),
  [835] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_state_stmt_simple, 2),
  [837] = {.entry = {.count = 1, .reusable = true}}, SHIFT(905),
  [839] = {.entry = {.count = 1, .reusable = true}}, SHIFT(71),
  [841] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_state_stmt_simple, 2),
  [843] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_flow_stmt_vertice, 1),
  [845] = {.entry = {.count = 1, .reusable = true}}, SHIFT(377),
  [847] = {.entry = {.count = 1, .reusable = true}}, SHIFT(308),
  [849] = {.entry = {.count = 1, .reusable = true}}, SHIFT(376),
  [851] = {.entry = {.count = 1, .reusable = true}}, SHIFT(374),
  [853] = {.entry = {.count = 1, .reusable = true}}, SHIFT(371),
  [855] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_state_alias, 3, .production_id = 11),
  [857] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_state_alias, 3, .production_id = 11),
  [859] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_er_stmt_entity, 1),
  [861] = {.entry = {.count = 1, .reusable = true}}, SHIFT(770),
  [863] = {.entry = {.count = 1, .reusable = true}}, SHIFT(414),
  [865] = {.entry = {.count = 1, .reusable = true}}, SHIFT(389),
  [867] = {.entry = {.count = 1, .reusable = true}}, SHIFT(394),
  [869] = {.entry = {.count = 1, .reusable = true}}, SHIFT(400),
  [871] = {.entry = {.count = 1, .reusable = true}}, SHIFT(688),
  [873] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_state_stmt_simple, 1),
  [875] = {.entry = {.count = 1, .reusable = true}}, SHIFT(689),
  [877] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_state_stmt_simple, 1),
  [879] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_source_file_repeat1, 2), SHIFT_REPEAT(824),
  [882] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_source_file_repeat1, 2),
  [884] = {.entry = {.count = 1, .reusable = false}}, REDUCE(aux_sym_source_file_repeat1, 2),
  [886] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_source_file_repeat1, 2), SHIFT_REPEAT(224),
  [889] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_state_stmt_arrow, 3),
  [891] = {.entry = {.count = 1, .reusable = true}}, SHIFT(693),
  [893] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_state_stmt_arrow, 3),
  [895] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_class_name, 1),
  [897] = {.entry = {.count = 1, .reusable = true}}, SHIFT(897),
  [899] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_class_name, 1),
  [901] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_state_note, 4),
  [903] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_state_note, 4),
  [905] = {.entry = {.count = 1, .reusable = false}}, SHIFT(239),
  [907] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_state_name, 1),
  [909] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_state_name, 1),
  [911] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_sequence_actor_repeat1, 2),
  [913] = {.entry = {.count = 2, .reusable = false}}, REDUCE(aux_sym_sequence_actor_repeat1, 2), SHIFT_REPEAT(229),
  [916] = {.entry = {.count = 1, .reusable = false}}, REDUCE(aux_sym_sequence_actor_repeat1, 2),
  [918] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_state_stmt_arrow, 5),
  [920] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_state_stmt_arrow, 5),
  [922] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_state_composite_body, 3),
  [924] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_state_composite_body, 3),
  [926] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_state_stmt_simple, 3),
  [928] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_state_stmt_simple, 3),
  [930] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_state_stmt_composite, 5),
  [932] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_state_stmt_composite, 5),
  [934] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_state_note, 5, .production_id = 23),
  [936] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_state_note, 5, .production_id = 23),
  [938] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_state_stmt_hide_empty_description, 1),
  [940] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_state_stmt_hide_empty_description, 1),
  [942] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym__direction, 1),
  [944] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym__direction, 1),
  [946] = {.entry = {.count = 1, .reusable = true}}, SHIFT(222),
  [948] = {.entry = {.count = 1, .reusable = false}}, SHIFT(382),
  [950] = {.entry = {.count = 1, .reusable = false}}, SHIFT(641),
  [952] = {.entry = {.count = 1, .reusable = false}}, SHIFT(83),
  [954] = {.entry = {.count = 1, .reusable = true}}, SHIFT(249),
  [956] = {.entry = {.count = 1, .reusable = true}}, SHIFT(243),
  [958] = {.entry = {.count = 1, .reusable = true}}, SHIFT(242),
  [960] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_state_description, 1),
  [962] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_state_description, 1),
  [964] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym__state_annotation, 1),
  [966] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym__state_annotation, 1),
  [968] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_state_stmt_annotation, 3),
  [970] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_state_stmt_annotation, 3),
  [972] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_state_annotation_choice, 1),
  [974] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_state_annotation_choice, 1),
  [976] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_state_annotation_join, 1),
  [978] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_state_annotation_join, 1),
  [980] = {.entry = {.count = 1, .reusable = false}}, SHIFT(548),
  [982] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym__state_stmt, 1),
  [984] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym__state_stmt, 1),
  [986] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_state_hide_empty_description, 1),
  [988] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_state_hide_empty_description, 1),
  [990] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_state_division, 1),
  [992] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_state_division, 1),
  [994] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_direction_lr, 1),
  [996] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_direction_lr, 1),
  [998] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_state_annotation_fork, 1),
  [1000] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_state_annotation_fork, 1),
  [1002] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_class_relation, 2),
  [1004] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_class_relation, 2),
  [1006] = {.entry = {.count = 1, .reusable = false}}, SHIFT(420),
  [1008] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_state_stmt_composite, 3),
  [1010] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_state_stmt_composite, 3),
  [1012] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_diagram_class, 5),
  [1014] = {.entry = {.count = 1, .reusable = false}}, SHIFT(424),
  [1016] = {.entry = {.count = 1, .reusable = true}}, SHIFT(886),
  [1018] = {.entry = {.count = 1, .reusable = false}}, SHIFT(261),
  [1020] = {.entry = {.count = 1, .reusable = true}}, SHIFT(263),
  [1022] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_direction_rl, 1),
  [1024] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_direction_rl, 1),
  [1026] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_direction_bt, 1),
  [1028] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_direction_bt, 1),
  [1030] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_state_composite_body, 4),
  [1032] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_state_composite_body, 4),
  [1034] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_direction_tb, 1),
  [1036] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_direction_tb, 1),
  [1038] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_sequence_actor, 1),
  [1040] = {.entry = {.count = 1, .reusable = false}}, SHIFT(229),
  [1042] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_sequence_actor, 1),
  [1044] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_class_name_body, 2),
  [1046] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_class_name_body, 2),
  [1048] = {.entry = {.count = 1, .reusable = false}}, REDUCE(aux_sym_class_name_body_repeat1, 1),
  [1050] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_class_name_body_repeat1, 1),
  [1052] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_class_relation, 1),
  [1054] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_class_relation, 1),
  [1056] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_class_name_body, 1),
  [1058] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_class_name_body, 1),
  [1060] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_state_composite_body, 5),
  [1062] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_state_composite_body, 5),
  [1064] = {.entry = {.count = 1, .reusable = false}}, SHIFT(559),
  [1066] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_state_composite_body, 6),
  [1068] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_state_composite_body, 6),
  [1070] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_diagram_class, 4),
  [1072] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_gantt_stmt_todaymarker, 2, .production_id = 7),
  [1074] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_gantt_stmt_todaymarker, 2, .production_id = 7),
  [1076] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym__gantt_stmt, 1),
  [1078] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym__gantt_stmt, 1),
  [1080] = {.entry = {.count = 1, .reusable = false}}, SHIFT(445),
  [1082] = {.entry = {.count = 1, .reusable = true}}, SHIFT(717),
  [1084] = {.entry = {.count = 1, .reusable = true}}, SHIFT(444),
  [1086] = {.entry = {.count = 1, .reusable = true}}, SHIFT(443),
  [1088] = {.entry = {.count = 1, .reusable = true}}, SHIFT(442),
  [1090] = {.entry = {.count = 1, .reusable = true}}, SHIFT(341),
  [1092] = {.entry = {.count = 1, .reusable = true}}, SHIFT(809),
  [1094] = {.entry = {.count = 1, .reusable = false}}, SHIFT(341),
  [1096] = {.entry = {.count = 1, .reusable = true}}, SHIFT(715),
  [1098] = {.entry = {.count = 1, .reusable = true}}, SHIFT(713),
  [1100] = {.entry = {.count = 1, .reusable = true}}, SHIFT(721),
  [1102] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_class_generics, 3),
  [1104] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_class_generics, 3),
  [1106] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_directive, 3),
  [1108] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_directive, 3),
  [1110] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_gantt_stmt_section, 2, .production_id = 8),
  [1112] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_gantt_stmt_section, 2, .production_id = 8),
  [1114] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_gantt_stmt_excludes, 2, .production_id = 6),
  [1116] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_gantt_stmt_excludes, 2, .production_id = 6),
  [1118] = {.entry = {.count = 1, .reusable = true}}, SHIFT(686),
  [1120] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_gantt_stmt_includes, 2, .production_id = 5),
  [1122] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_gantt_stmt_includes, 2, .production_id = 5),
  [1124] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_gantt_stmt_axisformat, 2, .production_id = 4),
  [1126] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_gantt_stmt_axisformat, 2, .production_id = 4),
  [1128] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_gantt_stmt_topaxis, 2, .production_id = 3),
  [1130] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_gantt_stmt_topaxis, 2, .production_id = 3),
  [1132] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_gantt_stmt_title, 2, .production_id = 1),
  [1134] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_gantt_stmt_title, 2, .production_id = 1),
  [1136] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_flow_stmt_subgraph_inner, 1),
  [1138] = {.entry = {.count = 1, .reusable = true}}, SHIFT(310),
  [1140] = {.entry = {.count = 1, .reusable = false}}, REDUCE(aux_sym_state_composite_body_repeat1, 2),
  [1142] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_class_name, 2),
  [1144] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_class_name, 2),
  [1146] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_gantt_stmt_task, 3),
  [1148] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_gantt_stmt_task, 3),
  [1150] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_gantt_stmt_inclusiveenddates, 2, .production_id = 2),
  [1152] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_gantt_stmt_inclusiveenddates, 2, .production_id = 2),
  [1154] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_gantt_stmt_dateformat, 2),
  [1156] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_gantt_stmt_dateformat, 2),
  [1158] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_flow_stmt_subgraph_inner_repeat1, 2), SHIFT_REPEAT(222),
  [1161] = {.entry = {.count = 2, .reusable = false}}, REDUCE(aux_sym_flow_stmt_subgraph_inner_repeat1, 2), SHIFT_REPEAT(382),
  [1164] = {.entry = {.count = 1, .reusable = false}}, REDUCE(aux_sym_flow_stmt_subgraph_inner_repeat1, 2),
  [1166] = {.entry = {.count = 2, .reusable = false}}, REDUCE(aux_sym_flow_stmt_subgraph_inner_repeat1, 2), SHIFT_REPEAT(83),
  [1169] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_gantt_meta_format, 1),
  [1171] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_gantt_meta_format, 1),
  [1173] = {.entry = {.count = 1, .reusable = true}}, SHIFT(706),
  [1175] = {.entry = {.count = 1, .reusable = true}}, SHIFT(312),
  [1177] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_flow_node, 2),
  [1179] = {.entry = {.count = 1, .reusable = true}}, SHIFT(511),
  [1181] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_flow_node, 2),
  [1183] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_flow_text_literal, 1),
  [1185] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_flow_text_literal, 1),
  [1187] = {.entry = {.count = 1, .reusable = false}}, SHIFT(315),
  [1189] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_er_entity_name, 1),
  [1191] = {.entry = {.count = 1, .reusable = true}}, SHIFT(654),
  [1193] = {.entry = {.count = 1, .reusable = true}}, SHIFT(655),
  [1195] = {.entry = {.count = 1, .reusable = false}}, SHIFT(655),
  [1197] = {.entry = {.count = 1, .reusable = false}}, SHIFT(656),
  [1199] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_flow_node_repeat1, 2),
  [1201] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_flow_node_repeat1, 2), SHIFT_REPEAT(511),
  [1204] = {.entry = {.count = 1, .reusable = false}}, REDUCE(aux_sym_flow_node_repeat1, 2),
  [1206] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_diagram_flow, 5),
  [1208] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_diagram_pie, 1),
  [1210] = {.entry = {.count = 1, .reusable = true}}, SHIFT(431),
  [1212] = {.entry = {.count = 1, .reusable = true}}, SHIFT(346),
  [1214] = {.entry = {.count = 1, .reusable = true}}, SHIFT(434),
  [1216] = {.entry = {.count = 1, .reusable = true}}, SHIFT(829),
  [1218] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_flow_node, 1),
  [1220] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_flow_node, 1),
  [1222] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_diagram_flow, 4),
  [1224] = {.entry = {.count = 1, .reusable = true}}, SHIFT(330),
  [1226] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_diagram_flow, 6),
  [1228] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_flow_text_literal_repeat1, 2),
  [1230] = {.entry = {.count = 1, .reusable = false}}, REDUCE(aux_sym_flow_text_literal_repeat1, 2),
  [1232] = {.entry = {.count = 2, .reusable = false}}, REDUCE(aux_sym_flow_text_literal_repeat1, 2), SHIFT_REPEAT(315),
  [1235] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_flow_vertex_ellipse, 3),
  [1237] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_flow_vertex_ellipse, 3),
  [1239] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_flow_vertex_inv_trapezoid, 3),
  [1241] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_flow_vertex_inv_trapezoid, 3),
  [1243] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_flow_vertex_square, 3),
  [1245] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_flow_vertex_square, 3),
  [1247] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_flow_vertex_round, 3),
  [1249] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_flow_vertex_round, 3),
  [1251] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_flow_vertex_trapezoid, 3),
  [1253] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_flow_vertex_trapezoid, 3),
  [1255] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_flow_vertex_diamond, 3),
  [1257] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_flow_vertex_diamond, 3),
  [1259] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym__flow_vertex_kind, 1),
  [1261] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym__flow_vertex_kind, 1),
  [1263] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_flow_vertex_odd, 3),
  [1265] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_flow_vertex_odd, 3),
  [1267] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_flow_vertex_hexagon, 3),
  [1269] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_flow_vertex_hexagon, 3),
  [1271] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_flow_vertex, 2),
  [1273] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_flow_vertex, 2),
  [1275] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_mmap_node_id, 1),
  [1277] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_mmap_node_id, 1),
  [1279] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_flow_vertex_cylinder, 3),
  [1281] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_flow_vertex_cylinder, 3),
  [1283] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_flow_vertex_leanright, 3),
  [1285] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_flow_vertex_leanright, 3),
  [1287] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_flow_vertex_rect, 3),
  [1289] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_flow_vertex_rect, 3),
  [1291] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_flow_vertex_stadium, 3),
  [1293] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_flow_vertex_stadium, 3),
  [1295] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_flow_vertex_leanleft, 3),
  [1297] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_flow_vertex_leanleft, 3),
  [1299] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_flow_vertex_circle, 3),
  [1301] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_flow_vertex_circle, 3),
  [1303] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_flow_vertex_subroutine, 3),
  [1305] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_flow_vertex_subroutine, 3),
  [1307] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_class_method_line, 2, .production_id = 20),
  [1309] = {.entry = {.count = 1, .reusable = true}}, SHIFT(351),
  [1311] = {.entry = {.count = 1, .reusable = true}}, SHIFT(504),
  [1313] = {.entry = {.count = 1, .reusable = true}}, SHIFT(505),
  [1315] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_class_method_line, 2, .production_id = 21),
  [1317] = {.entry = {.count = 1, .reusable = true}}, SHIFT(506),
  [1319] = {.entry = {.count = 1, .reusable = true}}, SHIFT(507),
  [1321] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_class_method_line, 2),
  [1323] = {.entry = {.count = 1, .reusable = true}}, SHIFT(508),
  [1325] = {.entry = {.count = 1, .reusable = true}}, SHIFT(509),
  [1327] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_class_method_line, 1),
  [1329] = {.entry = {.count = 1, .reusable = true}}, SHIFT(556),
  [1331] = {.entry = {.count = 1, .reusable = true}}, SHIFT(555),
  [1333] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_class_method_line, 2, .production_id = 19),
  [1335] = {.entry = {.count = 1, .reusable = true}}, SHIFT(502),
  [1337] = {.entry = {.count = 1, .reusable = true}}, SHIFT(503),
  [1339] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym__class_linetype, 1),
  [1341] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym__class_linetype, 1),
  [1343] = {.entry = {.count = 1, .reusable = false}}, REDUCE(aux_sym_flow_stmt_vertice_repeat1, 2),
  [1345] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_diagram_pie_repeat1, 2),
  [1347] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_diagram_pie_repeat1, 2), SHIFT_REPEAT(431),
  [1350] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_diagram_pie_repeat1, 2), SHIFT_REPEAT(345),
  [1353] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_diagram_pie_repeat1, 2), SHIFT_REPEAT(829),
  [1356] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_diagram_pie, 2),
  [1358] = {.entry = {.count = 1, .reusable = true}}, SHIFT(345),
  [1360] = {.entry = {.count = 1, .reusable = true}}, SHIFT(352),
  [1362] = {.entry = {.count = 1, .reusable = true}}, SHIFT(672),
  [1364] = {.entry = {.count = 1, .reusable = true}}, SHIFT(881),
  [1366] = {.entry = {.count = 1, .reusable = true}}, SHIFT(673),
  [1368] = {.entry = {.count = 1, .reusable = true}}, SHIFT(674),
  [1370] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_class_linetype_solid, 1),
  [1372] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_class_linetype_solid, 1),
  [1374] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_class_linetype_dotted, 1),
  [1376] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_class_linetype_dotted, 1),
  [1378] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_class_method_line_repeat1, 2),
  [1380] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_class_method_line_repeat1, 2), SHIFT_REPEAT(351),
  [1383] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_diagram_pie, 3),
  [1385] = {.entry = {.count = 1, .reusable = true}}, SHIFT(785),
  [1387] = {.entry = {.count = 1, .reusable = true}}, SHIFT(794),
  [1389] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_er_reltype_identifying, 1),
  [1391] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym__er_reltype, 1),
  [1393] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_er_reltype_non_identifying, 1),
  [1395] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_diagram_er, 4),
  [1397] = {.entry = {.count = 1, .reusable = true}}, SHIFT(300),
  [1399] = {.entry = {.count = 1, .reusable = true}}, SHIFT(381),
  [1401] = {.entry = {.count = 1, .reusable = true}}, SHIFT(355),
  [1403] = {.entry = {.count = 1, .reusable = true}}, SHIFT(360),
  [1405] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_diagram_er, 3),
  [1407] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_diagram_er, 5),
  [1409] = {.entry = {.count = 1, .reusable = true}}, SHIFT(743),
  [1411] = {.entry = {.count = 1, .reusable = true}}, SHIFT(759),
  [1413] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym__flowchart_direction, 1),
  [1415] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym__flowchart_direction, 1),
  [1417] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_flow_arrow_text_repeat1, 2),
  [1419] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_flow_arrow_text_repeat1, 2), SHIFT_REPEAT(369),
  [1422] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_flowchart_direction_bt, 1),
  [1424] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_flowchart_direction_bt, 1),
  [1426] = {.entry = {.count = 1, .reusable = true}}, SHIFT(513),
  [1428] = {.entry = {.count = 1, .reusable = true}}, SHIFT(883),
  [1430] = {.entry = {.count = 1, .reusable = true}}, SHIFT(884),
  [1432] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_flowchart_direction_tb, 1),
  [1434] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_flowchart_direction_tb, 1),
  [1436] = {.entry = {.count = 1, .reusable = true}}, SHIFT(486),
  [1438] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_flowchart_direction_rl, 1),
  [1440] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_flowchart_direction_rl, 1),
  [1442] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_flowchart_direction_lr, 1),
  [1444] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_flowchart_direction_lr, 1),
  [1446] = {.entry = {.count = 1, .reusable = true}}, SHIFT(383),
  [1448] = {.entry = {.count = 1, .reusable = true}}, SHIFT(427),
  [1450] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_mmap_node, 3),
  [1452] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_mmap_node, 3),
  [1454] = {.entry = {.count = 1, .reusable = true}}, SHIFT(419),
  [1456] = {.entry = {.count = 1, .reusable = true}}, SHIFT(237),
  [1458] = {.entry = {.count = 1, .reusable = true}}, SHIFT(472),
  [1460] = {.entry = {.count = 1, .reusable = true}}, SHIFT(800),
  [1462] = {.entry = {.count = 1, .reusable = true}}, SHIFT(496),
  [1464] = {.entry = {.count = 1, .reusable = true}}, SHIFT(651),
  [1466] = {.entry = {.count = 1, .reusable = false}}, SHIFT(299),
  [1468] = {.entry = {.count = 1, .reusable = false}}, SHIFT(856),
  [1470] = {.entry = {.count = 1, .reusable = false}}, SHIFT(825),
  [1472] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_er_cardinarity_zero_or_more, 1),
  [1474] = {.entry = {.count = 1, .reusable = false}}, SHIFT(866),
  [1476] = {.entry = {.count = 1, .reusable = true}}, SHIFT(867),
  [1478] = {.entry = {.count = 1, .reusable = false}}, SHIFT(750),
  [1480] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_flow_vertex_text, 1),
  [1482] = {.entry = {.count = 1, .reusable = true}}, SHIFT(369),
  [1484] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_er_cardinarity_one_or_more, 1),
  [1486] = {.entry = {.count = 1, .reusable = false}}, SHIFT(404),
  [1488] = {.entry = {.count = 1, .reusable = false}}, SHIFT(752),
  [1490] = {.entry = {.count = 1, .reusable = false}}, SHIFT(753),
  [1492] = {.entry = {.count = 1, .reusable = false}}, SHIFT(754),
  [1494] = {.entry = {.count = 1, .reusable = false}}, SHIFT(738),
  [1496] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym__er_cardinarity, 1),
  [1498] = {.entry = {.count = 1, .reusable = false}}, SHIFT(741),
  [1500] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_er_stmt_entity_block_inner_repeat1, 2),
  [1502] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_er_stmt_entity_block_inner_repeat1, 2), SHIFT_REPEAT(759),
  [1505] = {.entry = {.count = 1, .reusable = false}}, SHIFT(417),
  [1507] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_class_reltype_composition, 1),
  [1509] = {.entry = {.count = 2, .reusable = false}}, REDUCE(aux_sym_flow_arrow_text_repeat1, 2), SHIFT_REPEAT(406),
  [1512] = {.entry = {.count = 1, .reusable = false}}, SHIFT(819),
  [1514] = {.entry = {.count = 1, .reusable = false}}, SHIFT(808),
  [1516] = {.entry = {.count = 1, .reusable = true}}, SHIFT(804),
  [1518] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_class_reltype_dependency, 1),
  [1520] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_er_cardinarity_zero_or_one, 1),
  [1522] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_er_stmt_entity_block_inner, 1),
  [1524] = {.entry = {.count = 1, .reusable = false}}, SHIFT(796),
  [1526] = {.entry = {.count = 2, .reusable = false}}, REDUCE(aux_sym_flow_text_literal_repeat1, 2), SHIFT_REPEAT(417),
  [1529] = {.entry = {.count = 1, .reusable = false}}, SHIFT(806),
  [1531] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym__class_reltype, 1),
  [1533] = {.entry = {.count = 1, .reusable = true}}, SHIFT(722),
  [1535] = {.entry = {.count = 1, .reusable = true}}, SHIFT(723),
  [1537] = {.entry = {.count = 1, .reusable = false}}, SHIFT(801),
  [1539] = {.entry = {.count = 1, .reusable = true}}, SHIFT(356),
  [1541] = {.entry = {.count = 1, .reusable = true}}, SHIFT(398),
  [1543] = {.entry = {.count = 1, .reusable = false}}, SHIFT(860),
  [1545] = {.entry = {.count = 1, .reusable = false}}, SHIFT(406),
  [1547] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_flow_arrow_text, 1),
  [1549] = {.entry = {.count = 1, .reusable = true}}, SHIFT(855),
  [1551] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_pie_stmt_title, 1),
  [1553] = {.entry = {.count = 1, .reusable = false}}, SHIFT(448),
  [1555] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_pie_stmt_title, 1),
  [1557] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_class_reltype_extension, 1),
  [1559] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym__pie_stmt, 1),
  [1561] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_pie_showdata, 1),
  [1563] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_flow_stmt_subgraph_inner_repeat1, 2),
  [1565] = {.entry = {.count = 1, .reusable = true}}, SHIFT(83),
  [1567] = {.entry = {.count = 1, .reusable = true}}, SHIFT(314),
  [1569] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_class_stmt_relation, 6),
  [1571] = {.entry = {.count = 1, .reusable = false}}, SHIFT(666),
  [1573] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_diagram_flow_repeat1, 2),
  [1575] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_diagram_flow_repeat1, 2), SHIFT_REPEAT(323),
  [1578] = {.entry = {.count = 1, .reusable = true}}, SHIFT(339),
  [1580] = {.entry = {.count = 1, .reusable = true}}, SHIFT(338),
  [1582] = {.entry = {.count = 1, .reusable = true}}, SHIFT(337),
  [1584] = {.entry = {.count = 1, .reusable = true}}, SHIFT(342),
  [1586] = {.entry = {.count = 1, .reusable = true}}, SHIFT(303),
  [1588] = {.entry = {.count = 1, .reusable = true}}, SHIFT(54),
  [1590] = {.entry = {.count = 1, .reusable = false}}, SHIFT(901),
  [1592] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_pie_stmt_title, 2),
  [1594] = {.entry = {.count = 1, .reusable = true}}, SHIFT(48),
  [1596] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_mmap_markdown, 3),
  [1598] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_class_stmt_relation, 4),
  [1600] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_diagram_flow, 3),
  [1602] = {.entry = {.count = 1, .reusable = true}}, SHIFT(306),
  [1604] = {.entry = {.count = 1, .reusable = true}}, SHIFT(50),
  [1606] = {.entry = {.count = 1, .reusable = true}}, SHIFT(687),
  [1608] = {.entry = {.count = 1, .reusable = true}}, SHIFT(58),
  [1610] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_er_attribute_name, 1),
  [1612] = {.entry = {.count = 1, .reusable = true}}, SHIFT(53),
  [1614] = {.entry = {.count = 1, .reusable = false}}, SHIFT(461),
  [1616] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_class_stmt_class, 2),
  [1618] = {.entry = {.count = 1, .reusable = true}}, SHIFT(871),
  [1620] = {.entry = {.count = 1, .reusable = true}}, SHIFT(296),
  [1622] = {.entry = {.count = 2, .reusable = false}}, REDUCE(aux_sym_sequence_actor_repeat1, 2), SHIFT_REPEAT(461),
  [1625] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_pie_stmt_element, 3),
  [1627] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_mmap_node_hexagon, 3),
  [1629] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_mmap_node_hexagon, 3),
  [1631] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_mmap_node_content, 1),
  [1633] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_class_stmt_relation, 5),
  [1635] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_mmap_node_circle, 3),
  [1637] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_mmap_node_circle, 3),
  [1639] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_mmap_text, 2),
  [1641] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_mmap_node_bang, 3),
  [1643] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_mmap_node_bang, 3),
  [1645] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_mmap_node_rounded, 3),
  [1647] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_mmap_node_rounded, 3),
  [1649] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_flow_arrow_text_repeat1, 1),
  [1651] = {.entry = {.count = 1, .reusable = true}}, SHIFT(696),
  [1653] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_mmap_node, 4),
  [1655] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_mmap_node, 4),
  [1657] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_mmap_node_cloud, 3),
  [1659] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_mmap_node_cloud, 3),
  [1661] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_mmap_node_square, 3),
  [1663] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_mmap_node_square, 3),
  [1665] = {.entry = {.count = 1, .reusable = false}}, SHIFT(184),
  [1667] = {.entry = {.count = 1, .reusable = false}}, SHIFT(134),
  [1669] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_diagram_er_repeat1, 2),
  [1671] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_diagram_er_repeat1, 2), SHIFT_REPEAT(372),
  [1674] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_diagram_mindmap, 4),
  [1676] = {.entry = {.count = 1, .reusable = true}}, SHIFT(735),
  [1678] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_diagram_state_repeat1, 2),
  [1680] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_diagram_state_repeat1, 2), SHIFT_REPEAT(79),
  [1683] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_sequence_stmt_alt_repeat1, 2),
  [1685] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_sequence_stmt_alt_repeat1, 2), SHIFT_REPEAT(49),
  [1688] = {.entry = {.count = 1, .reusable = true}}, SHIFT(68),
  [1690] = {.entry = {.count = 1, .reusable = true}}, SHIFT(294),
  [1692] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_class_method_line, 3, .production_id = 25),
  [1694] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_class_method_line, 3, .production_id = 19),
  [1696] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_class_method_line, 3, .production_id = 26),
  [1698] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_class_method_line, 3, .production_id = 20),
  [1700] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_class_method_line, 3, .production_id = 27),
  [1702] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_class_method_line, 3, .production_id = 21),
  [1704] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_class_method_line, 3, .production_id = 28),
  [1706] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_class_method_line, 3),
  [1708] = {.entry = {.count = 1, .reusable = true}}, SHIFT(439),
  [1710] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_class_name_body_repeat1, 2), SHIFT_REPEAT(514),
  [1713] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_class_name_body_repeat1, 2),
  [1715] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_flow_stmt_subgraph, 5),
  [1717] = {.entry = {.count = 1, .reusable = true}}, SHIFT(188),
  [1719] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_mmap_icon, 3),
  [1721] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_mmap_icon, 3),
  [1723] = {.entry = {.count = 1, .reusable = true}}, SHIFT(705),
  [1725] = {.entry = {.count = 1, .reusable = true}}, SHIFT(273),
  [1727] = {.entry = {.count = 1, .reusable = true}}, SHIFT(272),
  [1729] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_class_stmt_class_repeat1, 2),
  [1731] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_class_stmt_class_repeat1, 2), SHIFT_REPEAT(309),
  [1734] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_flow_stmt_subgraph, 4),
  [1736] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_class_relation, 3),
  [1738] = {.entry = {.count = 1, .reusable = true}}, SHIFT(270),
  [1740] = {.entry = {.count = 1, .reusable = false}}, SHIFT(459),
  [1742] = {.entry = {.count = 1, .reusable = true}}, SHIFT(465),
  [1744] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_class_method_line, 2, .production_id = 22),
  [1746] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_flow_stmt_subgraph, 7),
  [1748] = {.entry = {.count = 1, .reusable = true}}, SHIFT(665),
  [1750] = {.entry = {.count = 1, .reusable = true}}, SHIFT(280),
  [1752] = {.entry = {.count = 1, .reusable = true}}, SHIFT(274),
  [1754] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_flow_stmt_subgraph, 8),
  [1756] = {.entry = {.count = 1, .reusable = true}}, SHIFT(365),
  [1758] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_class_stmt_class, 4, .production_id = 17),
  [1760] = {.entry = {.count = 1, .reusable = true}}, SHIFT(286),
  [1762] = {.entry = {.count = 1, .reusable = true}}, SHIFT(252),
  [1764] = {.entry = {.count = 1, .reusable = true}}, SHIFT(206),
  [1766] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_gantt_task_text_repeat1, 2),
  [1768] = {.entry = {.count = 2, .reusable = false}}, REDUCE(aux_sym_gantt_task_text_repeat1, 2), SHIFT_REPEAT(579),
  [1771] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_sequence_stmt_par_repeat1, 2),
  [1773] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_sequence_stmt_par_repeat1, 2), SHIFT_REPEAT(453),
  [1776] = {.entry = {.count = 1, .reusable = true}}, SHIFT(267),
  [1778] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_flow_stmt_direction, 2),
  [1780] = {.entry = {.count = 1, .reusable = true}}, SHIFT(193),
  [1782] = {.entry = {.count = 1, .reusable = true}}, SHIFT(101),
  [1784] = {.entry = {.count = 1, .reusable = true}}, SHIFT(102),
  [1786] = {.entry = {.count = 1, .reusable = true}}, SHIFT(105),
  [1788] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_diagram_mindmap, 3),
  [1790] = {.entry = {.count = 1, .reusable = true}}, SHIFT(660),
  [1792] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_diagram_mindmap_repeat1, 2),
  [1794] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_diagram_mindmap_repeat1, 2), SHIFT_REPEAT(908),
  [1797] = {.entry = {.count = 1, .reusable = false}}, SHIFT(95),
  [1799] = {.entry = {.count = 1, .reusable = true}}, SHIFT(361),
  [1801] = {.entry = {.count = 1, .reusable = true}}, SHIFT(357),
  [1803] = {.entry = {.count = 1, .reusable = true}}, SHIFT(258),
  [1805] = {.entry = {.count = 1, .reusable = true}}, SHIFT(191),
  [1807] = {.entry = {.count = 1, .reusable = true}}, SHIFT(199),
  [1809] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym__flow_stmt, 1),
  [1811] = {.entry = {.count = 1, .reusable = true}}, SHIFT(150),
  [1813] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_diagram_class_repeat1, 2),
  [1815] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_diagram_class_repeat1, 2), SHIFT_REPEAT(271),
  [1818] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_class_stmt_relation, 3),
  [1820] = {.entry = {.count = 1, .reusable = true}}, SHIFT(451),
  [1822] = {.entry = {.count = 1, .reusable = false}}, SHIFT(158),
  [1824] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_gantt_task_text, 1),
  [1826] = {.entry = {.count = 1, .reusable = false}}, SHIFT(579),
  [1828] = {.entry = {.count = 1, .reusable = true}}, SHIFT(370),
  [1830] = {.entry = {.count = 1, .reusable = true}}, SHIFT(143),
  [1832] = {.entry = {.count = 1, .reusable = true}}, SHIFT(144),
  [1834] = {.entry = {.count = 1, .reusable = true}}, SHIFT(127),
  [1836] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_diagram_state, 2),
  [1838] = {.entry = {.count = 1, .reusable = true}}, SHIFT(78),
  [1840] = {.entry = {.count = 1, .reusable = true}}, SHIFT(426),
  [1842] = {.entry = {.count = 1, .reusable = true}}, SHIFT(393),
  [1844] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_flow_stmt_subgraph, 3),
  [1846] = {.entry = {.count = 1, .reusable = true}}, SHIFT(392),
  [1848] = {.entry = {.count = 1, .reusable = true}}, SHIFT(70),
  [1850] = {.entry = {.count = 1, .reusable = true}}, SHIFT(603),
  [1852] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_diagram_er, 2),
  [1854] = {.entry = {.count = 1, .reusable = true}}, SHIFT(364),
  [1856] = {.entry = {.count = 1, .reusable = true}}, SHIFT(259),
  [1858] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_diagram_class, 3),
  [1860] = {.entry = {.count = 1, .reusable = true}}, SHIFT(266),
  [1862] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_solid_point, 1),
  [1864] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_solid_point, 1),
  [1866] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_sequence_signal_type, 1),
  [1868] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_sequence_signal_type, 1),
  [1870] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_dotted_open_arrow, 1),
  [1872] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_dotted_open_arrow, 1),
  [1874] = {.entry = {.count = 1, .reusable = true}}, SHIFT(663),
  [1876] = {.entry = {.count = 1, .reusable = true}}, SHIFT(179),
  [1878] = {.entry = {.count = 1, .reusable = true}}, SHIFT(731),
  [1880] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_mmap_class, 2),
  [1882] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_class_stmt_class, 5),
  [1884] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_class_label, 1),
  [1886] = {.entry = {.count = 1, .reusable = true}}, SHIFT(547),
  [1888] = {.entry = {.count = 1, .reusable = true}}, SHIFT(482),
  [1890] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_flow_link_arrow, 1),
  [1892] = {.entry = {.count = 1, .reusable = true}}, SHIFT(264),
  [1894] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_er_attribute, 3),
  [1896] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_er_attribute_key_type_pk, 1),
  [1898] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_er_attribute_key_type_fk, 1),
  [1900] = {.entry = {.count = 1, .reusable = true}}, SHIFT(565),
  [1902] = {.entry = {.count = 1, .reusable = true}}, SHIFT(488),
  [1904] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym__class_stmt, 1),
  [1906] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym__er_attribute_key_type, 1),
  [1908] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_er_stmt_entity_block, 5),
  [1910] = {.entry = {.count = 1, .reusable = true}}, SHIFT(733),
  [1912] = {.entry = {.count = 1, .reusable = true}}, SHIFT(161),
  [1914] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_mmap_node, 5),
  [1916] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_er_stmt_entity_relation, 5),
  [1918] = {.entry = {.count = 1, .reusable = true}}, SHIFT(581),
  [1920] = {.entry = {.count = 1, .reusable = true}}, SHIFT(498),
  [1922] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym__er_stmt, 1),
  [1924] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_state_note_placement, 1),
  [1926] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_class_annotation_line, 3, .production_id = 18),
  [1928] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_class_stmt_class, 6),
  [1930] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_er_role, 1),
  [1932] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_state_arrow, 1),
  [1934] = {.entry = {.count = 1, .reusable = true}}, SHIFT(435),
  [1936] = {.entry = {.count = 1, .reusable = false}}, SHIFT(293),
  [1938] = {.entry = {.count = 1, .reusable = true}}, SHIFT(725),
  [1940] = {.entry = {.count = 1, .reusable = true}}, SHIFT(110),
  [1942] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_er_attribute, 4),
  [1944] = {.entry = {.count = 1, .reusable = true}}, SHIFT(724),
  [1946] = {.entry = {.count = 1, .reusable = true}}, SHIFT(120),
  [1948] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_class_stmt_class, 7, .production_id = 17),
  [1950] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_class_stmt_class, 7),
  [1952] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_class_stmt_relation, 7),
  [1954] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_class_stmt_annotation, 4, .production_id = 18),
  [1956] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_er_attribute, 5),
  [1958] = {.entry = {.count = 1, .reusable = true}}, SHIFT(647),
  [1960] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_flow_link_simplelink, 1),
  [1962] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_class_stmt_class, 8, .production_id = 17),
  [1964] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_class_stmt_class, 8),
  [1966] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_class_stmt_class, 9, .production_id = 17),
  [1968] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_class_stmt_class, 10, .production_id = 17),
  [1970] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_note_placement_left, 1),
  [1972] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_note_placement_right, 1),
  [1974] = {.entry = {.count = 1, .reusable = true}}, SHIFT(718),
  [1976] = {.entry = {.count = 1, .reusable = true}}, SHIFT(153),
  [1978] = {.entry = {.count = 1, .reusable = true}}, SHIFT(716),
  [1980] = {.entry = {.count = 1, .reusable = true}}, SHIFT(151),
  [1982] = {.entry = {.count = 1, .reusable = true}}, SHIFT(457),
  [1984] = {.entry = {.count = 1, .reusable = false}}, SHIFT(327),
  [1986] = {.entry = {.count = 1, .reusable = true}}, SHIFT(703),
  [1988] = {.entry = {.count = 1, .reusable = true}}, SHIFT(93),
  [1990] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_diagram_mindmap, 5),
  [1992] = {.entry = {.count = 1, .reusable = true}}, SHIFT(699),
  [1994] = {.entry = {.count = 1, .reusable = true}}, SHIFT(90),
  [1996] = {.entry = {.count = 1, .reusable = true}}, SHIFT(320),
  [1998] = {.entry = {.count = 1, .reusable = true}}, SHIFT(329),
  [2000] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_class_stmt_method, 3),
  [2002] = {.entry = {.count = 1, .reusable = true}}, SHIFT(484),
  [2004] = {.entry = {.count = 1, .reusable = true}}, SHIFT(493),
  [2006] = {.entry = {.count = 1, .reusable = true}}, SHIFT(334),
  [2008] = {.entry = {.count = 1, .reusable = true}}, SHIFT(317),
  [2010] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_er_stmt_entity_block, 4),
  [2012] = {.entry = {.count = 1, .reusable = true}}, SHIFT(244),
  [2014] = {.entry = {.count = 1, .reusable = true}}, SHIFT(140),
  [2016] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_flow_link_arrow_start, 1),
  [2018] = {.entry = {.count = 1, .reusable = true}}, SHIFT(332),
  [2020] = {.entry = {.count = 1, .reusable = true}}, SHIFT(802),
  [2022] = {.entry = {.count = 1, .reusable = true}}, SHIFT(328),
  [2024] = {.entry = {.count = 1, .reusable = true}}, SHIFT(325),
  [2026] = {.entry = {.count = 1, .reusable = true}}, SHIFT(324),
  [2028] = {.entry = {.count = 1, .reusable = true}}, SHIFT(168),
  [2030] = {.entry = {.count = 1, .reusable = true}}, SHIFT(276),
  [2032] = {.entry = {.count = 1, .reusable = true}}, SHIFT(462),
  [2034] = {.entry = {.count = 1, .reusable = true}}, SHIFT(175),
  [2036] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_er_attribute_type, 1),
  [2038] = {.entry = {.count = 1, .reusable = true}}, SHIFT(678),
  [2040] = {.entry = {.count = 1, .reusable = true}}, SHIFT(207),
  [2042] = {.entry = {.count = 1, .reusable = true}}, SHIFT(202),
  [2044] = {.entry = {.count = 1, .reusable = true}}, SHIFT(174),
  [2046] = {.entry = {.count = 1, .reusable = true}}, SHIFT(160),
  [2048] = {.entry = {.count = 1, .reusable = true}}, SHIFT(159),
  [2050] = {.entry = {.count = 1, .reusable = true}}, SHIFT(92),
  [2052] = {.entry = {.count = 1, .reusable = true}}, SHIFT(91),
  [2054] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_er_relation, 3),
  [2056] = {.entry = {.count = 1, .reusable = false}}, SHIFT(661),
  [2058] = {.entry = {.count = 1, .reusable = true}}, SHIFT(367),
  [2060] = {.entry = {.count = 1, .reusable = false}}, SHIFT(910),
  [2062] = {.entry = {.count = 1, .reusable = true}}, SHIFT(146),
  [2064] = {.entry = {.count = 1, .reusable = true}}, SHIFT(667),
  [2066] = {.entry = {.count = 1, .reusable = true}}, SHIFT(116),
  [2068] = {.entry = {.count = 1, .reusable = true}}, SHIFT(181),
  [2070] = {.entry = {.count = 1, .reusable = true}}, SHIFT(490),
  [2072] = {.entry = {.count = 1, .reusable = true}}, SHIFT(104),
  [2074] = {.entry = {.count = 1, .reusable = true}}, SHIFT(103),
  [2076] = {.entry = {.count = 1, .reusable = true}}, SHIFT(100),
  [2078] = {.entry = {.count = 1, .reusable = true}}, SHIFT(99),
  [2080] = {.entry = {.count = 1, .reusable = true}}, SHIFT(98),
  [2082] = {.entry = {.count = 1, .reusable = true}}, SHIFT(192),
  [2084] = {.entry = {.count = 1, .reusable = true}}, SHIFT(643),
  [2086] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_sequence_signal_plus_sign, 1),
  [2088] = {.entry = {.count = 1, .reusable = true}}, SHIFT(479),
  [2090] = {.entry = {.count = 1, .reusable = true}}, SHIFT(639),
  [2092] = {.entry = {.count = 1, .reusable = true}}, SHIFT(638),
  [2094] = {.entry = {.count = 1, .reusable = true}}, SHIFT(637),
  [2096] = {.entry = {.count = 1, .reusable = true}}, SHIFT(149),
  [2098] = {.entry = {.count = 1, .reusable = true}}, SHIFT(40),
  [2100] = {.entry = {.count = 1, .reusable = true}}, SHIFT(41),
  [2102] = {.entry = {.count = 1, .reusable = true}}, SHIFT(42),
  [2104] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_sequence_signal_minus_sign, 1),
  [2106] = {.entry = {.count = 1, .reusable = true}}, SHIFT(534),
  [2108] = {.entry = {.count = 1, .reusable = true}}, SHIFT(316),
  [2110] = {.entry = {.count = 1, .reusable = true}}, SHIFT(277),
  [2112] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_source_file, 1),
  [2114] = {.entry = {.count = 1, .reusable = true}}, SHIFT(138),
  [2116] = {.entry = {.count = 1, .reusable = false}}, SHIFT(822),
  [2118] = {.entry = {.count = 1, .reusable = true}}, SHIFT(318),
  [2120] = {.entry = {.count = 1, .reusable = false}}, SHIFT(289),
  [2122] = {.entry = {.count = 1, .reusable = true}}, SHIFT(423),
  [2124] = {.entry = {.count = 1, .reusable = false}}, SHIFT(805),
  [2126] = {.entry = {.count = 1, .reusable = true}}, SHIFT(742),
  [2128] = {.entry = {.count = 1, .reusable = true}}, SHIFT(335),
  [2130] = {.entry = {.count = 1, .reusable = true}}, SHIFT(205),
  [2132] = {.entry = {.count = 1, .reusable = false}}, SHIFT(744),
  [2134] = {.entry = {.count = 1, .reusable = true}}, SHIFT(902),
  [2136] = {.entry = {.count = 1, .reusable = true}}, SHIFT(630),
  [2138] = {.entry = {.count = 1, .reusable = true}}, SHIFT(629),
  [2140] = {.entry = {.count = 1, .reusable = true}}, SHIFT(627),
  [2142] = {.entry = {.count = 1, .reusable = true}}, SHIFT(626),
  [2144] = {.entry = {.count = 1, .reusable = true}}, SHIFT(141),
  [2146] = {.entry = {.count = 1, .reusable = true}}, SHIFT(36),
  [2148] = {.entry = {.count = 1, .reusable = true}}, SHIFT(35),
  [2150] = {.entry = {.count = 1, .reusable = true}}, SHIFT(20),
  [2152] = {.entry = {.count = 1, .reusable = true}},  ACCEPT_INPUT(),
  [2154] = {.entry = {.count = 1, .reusable = true}}, SHIFT(336),
  [2156] = {.entry = {.count = 1, .reusable = true}}, SHIFT(865),
  [2158] = {.entry = {.count = 1, .reusable = true}}, SHIFT(147),
  [2160] = {.entry = {.count = 1, .reusable = true}}, SHIFT(454),
  [2162] = {.entry = {.count = 1, .reusable = true}}, SHIFT(148),
  [2164] = {.entry = {.count = 1, .reusable = false}}, SHIFT(797),
  [2166] = {.entry = {.count = 1, .reusable = true}}, SHIFT(333),
  [2168] = {.entry = {.count = 1, .reusable = true}}, SHIFT(566),
  [2170] = {.entry = {.count = 1, .reusable = true}}, SHIFT(136),
  [2172] = {.entry = {.count = 1, .reusable = true}}, SHIFT(227),
  [2174] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_pie_label, 1),
  [2176] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym__flow_link, 1),
  [2178] = {.entry = {.count = 1, .reusable = true}}, SHIFT(612),
  [2180] = {.entry = {.count = 1, .reusable = true}}, SHIFT(610),
  [2182] = {.entry = {.count = 1, .reusable = true}}, SHIFT(609),
  [2184] = {.entry = {.count = 1, .reusable = true}}, SHIFT(608),
  [2186] = {.entry = {.count = 1, .reusable = true}}, SHIFT(152),
  [2188] = {.entry = {.count = 1, .reusable = true}}, SHIFT(27),
  [2190] = {.entry = {.count = 1, .reusable = true}}, SHIFT(26),
  [2192] = {.entry = {.count = 1, .reusable = true}}, SHIFT(25),
  [2194] = {.entry = {.count = 1, .reusable = true}}, SHIFT(571),
  [2196] = {.entry = {.count = 1, .reusable = true}}, SHIFT(757),
  [2198] = {.entry = {.count = 1, .reusable = true}}, SHIFT(75),
  [2200] = {.entry = {.count = 1, .reusable = true}}, SHIFT(295),
  [2202] = {.entry = {.count = 1, .reusable = true}}, SHIFT(125),
  [2204] = {.entry = {.count = 1, .reusable = true}}, SHIFT(709),
  [2206] = {.entry = {.count = 1, .reusable = true}}, SHIFT(469),
  [2208] = {.entry = {.count = 1, .reusable = true}}, SHIFT(671),
  [2210] = {.entry = {.count = 1, .reusable = true}}, SHIFT(517),
  [2212] = {.entry = {.count = 1, .reusable = true}}, SHIFT(887),
  [2214] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_flow_link_middletext, 3),
  [2216] = {.entry = {.count = 1, .reusable = true}}, SHIFT(112),
  [2218] = {.entry = {.count = 1, .reusable = true}}, SHIFT(55),
  [2220] = {.entry = {.count = 1, .reusable = true}}, SHIFT(727),
  [2222] = {.entry = {.count = 1, .reusable = true}}, SHIFT(111),
  [2224] = {.entry = {.count = 1, .reusable = true}}, SHIFT(548),
  [2226] = {.entry = {.count = 1, .reusable = true}}, SHIFT(904),
  [2228] = {.entry = {.count = 1, .reusable = true}}, SHIFT(321),
  [2230] = {.entry = {.count = 1, .reusable = true}}, SHIFT(157),
  [2232] = {.entry = {.count = 1, .reusable = true}}, SHIFT(61),
  [2234] = {.entry = {.count = 1, .reusable = true}}, SHIFT(60),
  [2236] = {.entry = {.count = 1, .reusable = true}}, SHIFT(319),
  [2238] = {.entry = {.count = 1, .reusable = true}}, SHIFT(577),
  [2240] = {.entry = {.count = 1, .reusable = true}}, SHIFT(576),
  [2242] = {.entry = {.count = 1, .reusable = true}}, SHIFT(575),
  [2244] = {.entry = {.count = 1, .reusable = false}}, SHIFT(467),
  [2246] = {.entry = {.count = 1, .reusable = false}}, SHIFT(872),
  [2248] = {.entry = {.count = 1, .reusable = true}}, SHIFT(190),
  [2250] = {.entry = {.count = 1, .reusable = true}}, SHIFT(720),
  [2252] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_source_file, 2),
  [2254] = {.entry = {.count = 1, .reusable = true}}, SHIFT(570),
  [2256] = {.entry = {.count = 1, .reusable = true}}, SHIFT(450),
  [2258] = {.entry = {.count = 1, .reusable = true}}, SHIFT(162),
  [2260] = {.entry = {.count = 1, .reusable = true}}, SHIFT(47),
  [2262] = {.entry = {.count = 1, .reusable = true}}, SHIFT(52),
  [2264] = {.entry = {.count = 1, .reusable = true}}, SHIFT(163),
  [2266] = {.entry = {.count = 1, .reusable = true}}, SHIFT(562),
  [2268] = {.entry = {.count = 1, .reusable = true}}, SHIFT(561),
  [2270] = {.entry = {.count = 1, .reusable = true}}, SHIFT(560),
  [2272] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_er_attribute_comment, 1),
  [2274] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_note_placement_left, 1),
  [2276] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_note_placement_right, 1),
  [2278] = {.entry = {.count = 1, .reusable = true}}, SHIFT(704),
  [2280] = {.entry = {.count = 1, .reusable = true}}, SHIFT(803),
  [2282] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_flow_link_arrowtext, 4),
  [2284] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_sequence_note_placement, 1),
  [2286] = {.entry = {.count = 1, .reusable = true}}, SHIFT(478),
  [2288] = {.entry = {.count = 1, .reusable = true}}, SHIFT(56),
  [2290] = {.entry = {.count = 1, .reusable = true}}, SHIFT(51),
  [2292] = {.entry = {.count = 1, .reusable = true}}, SHIFT(594),
  [2294] = {.entry = {.count = 1, .reusable = true}}, SHIFT(546),
  [2296] = {.entry = {.count = 1, .reusable = true}}, SHIFT(545),
  [2298] = {.entry = {.count = 1, .reusable = true}}, SHIFT(544),
  [2300] = {.entry = {.count = 1, .reusable = false}}, SHIFT(756),
  [2302] = {.entry = {.count = 1, .reusable = true}}, SHIFT(436),
  [2304] = {.entry = {.count = 1, .reusable = true}}, SHIFT(593),
  [2306] = {.entry = {.count = 1, .reusable = true}}, SHIFT(592),
  [2308] = {.entry = {.count = 1, .reusable = true}}, SHIFT(685),
  [2310] = {.entry = {.count = 1, .reusable = true}}, SHIFT(196),
  [2312] = {.entry = {.count = 1, .reusable = false}}, SHIFT(220),
  [2314] = {.entry = {.count = 1, .reusable = false}}, SHIFT(711),
  [2316] = {.entry = {.count = 1, .reusable = true}}, SHIFT(33),
  [2318] = {.entry = {.count = 1, .reusable = true}}, SHIFT(34),
  [2320] = {.entry = {.count = 1, .reusable = true}}, SHIFT(527),
  [2322] = {.entry = {.count = 1, .reusable = true}}, SHIFT(37),
  [2324] = {.entry = {.count = 1, .reusable = true}}, SHIFT(463),
  [2326] = {.entry = {.count = 1, .reusable = true}}, SHIFT(468),
  [2328] = {.entry = {.count = 1, .reusable = true}}, SHIFT(466),
  [2330] = {.entry = {.count = 1, .reusable = true}}, SHIFT(211),
  [2332] = {.entry = {.count = 1, .reusable = true}}, SHIFT(476),
  [2334] = {.entry = {.count = 1, .reusable = true}}, SHIFT(59),
  [2336] = {.entry = {.count = 1, .reusable = true}}, SHIFT(455),
  [2338] = {.entry = {.count = 1, .reusable = true}}, SHIFT(475),
  [2340] = {.entry = {.count = 1, .reusable = true}}, SHIFT(57),
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

TS_PUBLIC const TSLanguage *tree_sitter_mermaid() {
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
