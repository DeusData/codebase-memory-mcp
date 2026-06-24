#include "tree_sitter/parser.h"

#if defined(__GNUC__) || defined(__clang__)
#pragma GCC diagnostic ignored "-Wmissing-field-initializers"
#endif

#define LANGUAGE_VERSION 14
#define STATE_COUNT 325
#define LARGE_STATE_COUNT 2
#define SYMBOL_COUNT 164
#define ALIAS_COUNT 0
#define TOKEN_COUNT 87
#define EXTERNAL_TOKEN_COUNT 5
#define FIELD_COUNT 9
#define MAX_ALIAS_SEQUENCE_LENGTH 5
#define PRODUCTION_ID_COUNT 16

enum ts_symbol_identifiers {
  anon_sym_LF = 1,
  aux_sym_from_instruction_token1 = 2,
  aux_sym_from_instruction_token2 = 3,
  aux_sym_run_instruction_token1 = 4,
  aux_sym_cmd_instruction_token1 = 5,
  aux_sym_label_instruction_token1 = 6,
  aux_sym_expose_instruction_token1 = 7,
  aux_sym_env_instruction_token1 = 8,
  aux_sym_add_instruction_token1 = 9,
  aux_sym_copy_instruction_token1 = 10,
  aux_sym_entrypoint_instruction_token1 = 11,
  aux_sym_volume_instruction_token1 = 12,
  aux_sym_user_instruction_token1 = 13,
  anon_sym_COLON = 14,
  aux_sym__user_name_or_group_token1 = 15,
  aux_sym__immediate_user_name_or_group_fragment_token1 = 16,
  aux_sym_workdir_instruction_token1 = 17,
  aux_sym_arg_instruction_token1 = 18,
  aux_sym_arg_instruction_token2 = 19,
  anon_sym_EQ = 20,
  aux_sym_onbuild_instruction_token1 = 21,
  aux_sym_stopsignal_instruction_token1 = 22,
  aux_sym__stopsignal_value_token1 = 23,
  aux_sym__stopsignal_value_token2 = 24,
  aux_sym_healthcheck_instruction_token1 = 25,
  anon_sym_NONE = 26,
  aux_sym_shell_instruction_token1 = 27,
  aux_sym_maintainer_instruction_token1 = 28,
  aux_sym_maintainer_instruction_token2 = 29,
  aux_sym_cross_build_instruction_token1 = 30,
  aux_sym_path_token1 = 31,
  aux_sym_path_token2 = 32,
  aux_sym_path_token3 = 33,
  aux_sym_path_with_heredoc_token1 = 34,
  anon_sym_DOLLAR = 35,
  anon_sym_DOLLAR2 = 36,
  anon_sym_LBRACE = 37,
  aux_sym__expansion_body_token1 = 38,
  anon_sym_RBRACE = 39,
  sym_variable = 40,
  aux_sym__spaced_env_pair_token1 = 41,
  aux_sym__env_key_token1 = 42,
  aux_sym_expose_port_token1 = 43,
  anon_sym_SLASHtcp = 44,
  anon_sym_SLASHudp = 45,
  aux_sym_label_pair_token1 = 46,
  aux_sym_image_name_token1 = 47,
  aux_sym_image_name_token2 = 48,
  aux_sym_image_tag_token1 = 49,
  anon_sym_AT = 50,
  aux_sym_image_digest_token1 = 51,
  anon_sym_DASH_DASH = 52,
  aux_sym_param_token1 = 53,
  aux_sym_param_token2 = 54,
  anon_sym_mount = 55,
  anon_sym_COMMA = 56,
  aux_sym_mount_param_param_token1 = 57,
  aux_sym_image_alias_token1 = 58,
  aux_sym_image_alias_token2 = 59,
  aux_sym_shell_fragment_token1 = 60,
  aux_sym_shell_fragment_token2 = 61,
  aux_sym_shell_fragment_token3 = 62,
  aux_sym_shell_fragment_token4 = 63,
  sym_line_continuation = 64,
  sym_required_line_continuation = 65,
  anon_sym_LBRACK = 66,
  anon_sym_COMMA2 = 67,
  anon_sym_RBRACK = 68,
  anon_sym_DQUOTE = 69,
  aux_sym_json_string_token1 = 70,
  sym_json_escape_sequence = 71,
  aux_sym_double_quoted_string_token1 = 72,
  anon_sym_BSLASH = 73,
  anon_sym_SQUOTE = 74,
  aux_sym_single_quoted_string_token1 = 75,
  aux_sym_unquoted_string_token1 = 76,
  anon_sym_BSLASH2 = 77,
  sym_double_quoted_escape_sequence = 78,
  sym_single_quoted_escape_sequence = 79,
  sym__non_newline_whitespace = 80,
  sym_comment = 81,
  sym_heredoc_marker = 82,
  sym_heredoc_line = 83,
  sym_heredoc_end = 84,
  sym_heredoc_nl = 85,
  sym_error_sentinel = 86,
  sym_source_file = 87,
  sym__instruction = 88,
  sym_from_instruction = 89,
  sym_run_instruction = 90,
  sym_cmd_instruction = 91,
  sym_label_instruction = 92,
  sym_expose_instruction = 93,
  sym_env_instruction = 94,
  sym_add_instruction = 95,
  sym_copy_instruction = 96,
  sym_entrypoint_instruction = 97,
  sym_volume_instruction = 98,
  sym_user_instruction = 99,
  sym__user_name_or_group = 100,
  aux_sym__immediate_user_name_or_group = 101,
  sym__immediate_user_name_or_group_fragment = 102,
  sym_workdir_instruction = 103,
  sym_arg_instruction = 104,
  sym_onbuild_instruction = 105,
  sym_stopsignal_instruction = 106,
  sym__stopsignal_value = 107,
  sym_healthcheck_instruction = 108,
  sym_shell_instruction = 109,
  sym_maintainer_instruction = 110,
  sym_cross_build_instruction = 111,
  sym_heredoc_block = 112,
  sym_path = 113,
  sym_path_with_heredoc = 114,
  sym_expansion = 115,
  sym__immediate_expansion = 116,
  sym__imm_expansion = 117,
  sym__expansion_body = 118,
  sym_env_pair = 119,
  sym__spaced_env_pair = 120,
  sym__env_key = 121,
  sym_expose_port = 122,
  sym_label_pair = 123,
  sym_image_spec = 124,
  sym_image_name = 125,
  sym_image_tag = 126,
  sym_image_digest = 127,
  sym_param = 128,
  sym_mount_param = 129,
  sym_mount_param_param = 130,
  sym_image_alias = 131,
  sym_shell_command = 132,
  sym_shell_fragment = 133,
  sym_json_string_array = 134,
  sym_json_string = 135,
  sym_double_quoted_string = 136,
  sym_single_quoted_string = 137,
  sym_unquoted_string = 138,
  aux_sym_source_file_repeat1 = 139,
  aux_sym_run_instruction_repeat1 = 140,
  aux_sym_run_instruction_repeat2 = 141,
  aux_sym_label_instruction_repeat1 = 142,
  aux_sym_expose_instruction_repeat1 = 143,
  aux_sym_env_instruction_repeat1 = 144,
  aux_sym_add_instruction_repeat1 = 145,
  aux_sym_add_instruction_repeat2 = 146,
  aux_sym_volume_instruction_repeat1 = 147,
  aux_sym__user_name_or_group_repeat1 = 148,
  aux_sym__stopsignal_value_repeat1 = 149,
  aux_sym_heredoc_block_repeat1 = 150,
  aux_sym_path_repeat1 = 151,
  aux_sym_image_name_repeat1 = 152,
  aux_sym_image_tag_repeat1 = 153,
  aux_sym_image_digest_repeat1 = 154,
  aux_sym_mount_param_repeat1 = 155,
  aux_sym_image_alias_repeat1 = 156,
  aux_sym_shell_command_repeat1 = 157,
  aux_sym_shell_fragment_repeat1 = 158,
  aux_sym_json_string_array_repeat1 = 159,
  aux_sym_json_string_repeat1 = 160,
  aux_sym_double_quoted_string_repeat1 = 161,
  aux_sym_single_quoted_string_repeat1 = 162,
  aux_sym_unquoted_string_repeat1 = 163,
};

static const char * const ts_symbol_names[] = {
  [ts_builtin_sym_end] = "end",
  [anon_sym_LF] = "\n",
  [aux_sym_from_instruction_token1] = "FROM",
  [aux_sym_from_instruction_token2] = "AS",
  [aux_sym_run_instruction_token1] = "RUN",
  [aux_sym_cmd_instruction_token1] = "CMD",
  [aux_sym_label_instruction_token1] = "LABEL",
  [aux_sym_expose_instruction_token1] = "EXPOSE",
  [aux_sym_env_instruction_token1] = "ENV",
  [aux_sym_add_instruction_token1] = "ADD",
  [aux_sym_copy_instruction_token1] = "COPY",
  [aux_sym_entrypoint_instruction_token1] = "ENTRYPOINT",
  [aux_sym_volume_instruction_token1] = "VOLUME",
  [aux_sym_user_instruction_token1] = "USER",
  [anon_sym_COLON] = ":",
  [aux_sym__user_name_or_group_token1] = "_user_name_or_group_token1",
  [aux_sym__immediate_user_name_or_group_fragment_token1] = "_immediate_user_name_or_group_fragment_token1",
  [aux_sym_workdir_instruction_token1] = "WORKDIR",
  [aux_sym_arg_instruction_token1] = "ARG",
  [aux_sym_arg_instruction_token2] = "unquoted_string",
  [anon_sym_EQ] = "=",
  [aux_sym_onbuild_instruction_token1] = "ONBUILD",
  [aux_sym_stopsignal_instruction_token1] = "STOPSIGNAL",
  [aux_sym__stopsignal_value_token1] = "_stopsignal_value_token1",
  [aux_sym__stopsignal_value_token2] = "_stopsignal_value_token2",
  [aux_sym_healthcheck_instruction_token1] = "HEALTHCHECK",
  [anon_sym_NONE] = "NONE",
  [aux_sym_shell_instruction_token1] = "SHELL",
  [aux_sym_maintainer_instruction_token1] = "MAINTAINER",
  [aux_sym_maintainer_instruction_token2] = "maintainer_instruction_token2",
  [aux_sym_cross_build_instruction_token1] = "CROSS_BUILD",
  [aux_sym_path_token1] = "path_token1",
  [aux_sym_path_token2] = "path_token2",
  [aux_sym_path_token3] = "path_token3",
  [aux_sym_path_with_heredoc_token1] = "path_with_heredoc_token1",
  [anon_sym_DOLLAR] = "$",
  [anon_sym_DOLLAR2] = "$",
  [anon_sym_LBRACE] = "{",
  [aux_sym__expansion_body_token1] = "variable",
  [anon_sym_RBRACE] = "}",
  [sym_variable] = "variable",
  [aux_sym__spaced_env_pair_token1] = "_spaced_env_pair_token1",
  [aux_sym__env_key_token1] = "unquoted_string",
  [aux_sym_expose_port_token1] = "expose_port_token1",
  [anon_sym_SLASHtcp] = "/tcp",
  [anon_sym_SLASHudp] = "/udp",
  [aux_sym_label_pair_token1] = "unquoted_string",
  [aux_sym_image_name_token1] = "image_name_token1",
  [aux_sym_image_name_token2] = "image_name_token2",
  [aux_sym_image_tag_token1] = "image_tag_token1",
  [anon_sym_AT] = "@",
  [aux_sym_image_digest_token1] = "image_digest_token1",
  [anon_sym_DASH_DASH] = "--",
  [aux_sym_param_token1] = "param_token1",
  [aux_sym_param_token2] = "param_token2",
  [anon_sym_mount] = "mount",
  [anon_sym_COMMA] = ",",
  [aux_sym_mount_param_param_token1] = "mount_param_param_token1",
  [aux_sym_image_alias_token1] = "image_alias_token1",
  [aux_sym_image_alias_token2] = "image_alias_token2",
  [aux_sym_shell_fragment_token1] = "shell_fragment_token1",
  [aux_sym_shell_fragment_token2] = "shell_fragment_token2",
  [aux_sym_shell_fragment_token3] = "shell_fragment_token3",
  [aux_sym_shell_fragment_token4] = "shell_fragment_token4",
  [sym_line_continuation] = "line_continuation",
  [sym_required_line_continuation] = "line_continuation",
  [anon_sym_LBRACK] = "[",
  [anon_sym_COMMA2] = ",",
  [anon_sym_RBRACK] = "]",
  [anon_sym_DQUOTE] = "\"",
  [aux_sym_json_string_token1] = "json_string_token1",
  [sym_json_escape_sequence] = "escape_sequence",
  [aux_sym_double_quoted_string_token1] = "double_quoted_string_token1",
  [anon_sym_BSLASH] = "\\",
  [anon_sym_SQUOTE] = "'",
  [aux_sym_single_quoted_string_token1] = "single_quoted_string_token1",
  [aux_sym_unquoted_string_token1] = "unquoted_string_token1",
  [anon_sym_BSLASH2] = "\\ ",
  [sym_double_quoted_escape_sequence] = "escape_sequence",
  [sym_single_quoted_escape_sequence] = "escape_sequence",
  [sym__non_newline_whitespace] = "_non_newline_whitespace",
  [sym_comment] = "comment",
  [sym_heredoc_marker] = "heredoc_marker",
  [sym_heredoc_line] = "heredoc_line",
  [sym_heredoc_end] = "heredoc_end",
  [sym_heredoc_nl] = "_heredoc_nl",
  [sym_error_sentinel] = "error_sentinel",
  [sym_source_file] = "source_file",
  [sym__instruction] = "_instruction",
  [sym_from_instruction] = "from_instruction",
  [sym_run_instruction] = "run_instruction",
  [sym_cmd_instruction] = "cmd_instruction",
  [sym_label_instruction] = "label_instruction",
  [sym_expose_instruction] = "expose_instruction",
  [sym_env_instruction] = "env_instruction",
  [sym_add_instruction] = "add_instruction",
  [sym_copy_instruction] = "copy_instruction",
  [sym_entrypoint_instruction] = "entrypoint_instruction",
  [sym_volume_instruction] = "volume_instruction",
  [sym_user_instruction] = "user_instruction",
  [sym__user_name_or_group] = "unquoted_string",
  [aux_sym__immediate_user_name_or_group] = "_immediate_user_name_or_group",
  [sym__immediate_user_name_or_group_fragment] = "_immediate_user_name_or_group_fragment",
  [sym_workdir_instruction] = "workdir_instruction",
  [sym_arg_instruction] = "arg_instruction",
  [sym_onbuild_instruction] = "onbuild_instruction",
  [sym_stopsignal_instruction] = "stopsignal_instruction",
  [sym__stopsignal_value] = "_stopsignal_value",
  [sym_healthcheck_instruction] = "healthcheck_instruction",
  [sym_shell_instruction] = "shell_instruction",
  [sym_maintainer_instruction] = "maintainer_instruction",
  [sym_cross_build_instruction] = "cross_build_instruction",
  [sym_heredoc_block] = "heredoc_block",
  [sym_path] = "path",
  [sym_path_with_heredoc] = "path",
  [sym_expansion] = "expansion",
  [sym__immediate_expansion] = "_immediate_expansion",
  [sym__imm_expansion] = "expansion",
  [sym__expansion_body] = "_expansion_body",
  [sym_env_pair] = "env_pair",
  [sym__spaced_env_pair] = "env_pair",
  [sym__env_key] = "_env_key",
  [sym_expose_port] = "expose_port",
  [sym_label_pair] = "label_pair",
  [sym_image_spec] = "image_spec",
  [sym_image_name] = "image_name",
  [sym_image_tag] = "image_tag",
  [sym_image_digest] = "image_digest",
  [sym_param] = "param",
  [sym_mount_param] = "mount_param",
  [sym_mount_param_param] = "mount_param_param",
  [sym_image_alias] = "image_alias",
  [sym_shell_command] = "shell_command",
  [sym_shell_fragment] = "shell_fragment",
  [sym_json_string_array] = "json_string_array",
  [sym_json_string] = "json_string",
  [sym_double_quoted_string] = "double_quoted_string",
  [sym_single_quoted_string] = "single_quoted_string",
  [sym_unquoted_string] = "unquoted_string",
  [aux_sym_source_file_repeat1] = "source_file_repeat1",
  [aux_sym_run_instruction_repeat1] = "run_instruction_repeat1",
  [aux_sym_run_instruction_repeat2] = "run_instruction_repeat2",
  [aux_sym_label_instruction_repeat1] = "label_instruction_repeat1",
  [aux_sym_expose_instruction_repeat1] = "expose_instruction_repeat1",
  [aux_sym_env_instruction_repeat1] = "env_instruction_repeat1",
  [aux_sym_add_instruction_repeat1] = "add_instruction_repeat1",
  [aux_sym_add_instruction_repeat2] = "add_instruction_repeat2",
  [aux_sym_volume_instruction_repeat1] = "volume_instruction_repeat1",
  [aux_sym__user_name_or_group_repeat1] = "_user_name_or_group_repeat1",
  [aux_sym__stopsignal_value_repeat1] = "_stopsignal_value_repeat1",
  [aux_sym_heredoc_block_repeat1] = "heredoc_block_repeat1",
  [aux_sym_path_repeat1] = "path_repeat1",
  [aux_sym_image_name_repeat1] = "image_name_repeat1",
  [aux_sym_image_tag_repeat1] = "image_tag_repeat1",
  [aux_sym_image_digest_repeat1] = "image_digest_repeat1",
  [aux_sym_mount_param_repeat1] = "mount_param_repeat1",
  [aux_sym_image_alias_repeat1] = "image_alias_repeat1",
  [aux_sym_shell_command_repeat1] = "shell_command_repeat1",
  [aux_sym_shell_fragment_repeat1] = "shell_fragment_repeat1",
  [aux_sym_json_string_array_repeat1] = "json_string_array_repeat1",
  [aux_sym_json_string_repeat1] = "json_string_repeat1",
  [aux_sym_double_quoted_string_repeat1] = "double_quoted_string_repeat1",
  [aux_sym_single_quoted_string_repeat1] = "single_quoted_string_repeat1",
  [aux_sym_unquoted_string_repeat1] = "unquoted_string_repeat1",
};

static const TSSymbol ts_symbol_map[] = {
  [ts_builtin_sym_end] = ts_builtin_sym_end,
  [anon_sym_LF] = anon_sym_LF,
  [aux_sym_from_instruction_token1] = aux_sym_from_instruction_token1,
  [aux_sym_from_instruction_token2] = aux_sym_from_instruction_token2,
  [aux_sym_run_instruction_token1] = aux_sym_run_instruction_token1,
  [aux_sym_cmd_instruction_token1] = aux_sym_cmd_instruction_token1,
  [aux_sym_label_instruction_token1] = aux_sym_label_instruction_token1,
  [aux_sym_expose_instruction_token1] = aux_sym_expose_instruction_token1,
  [aux_sym_env_instruction_token1] = aux_sym_env_instruction_token1,
  [aux_sym_add_instruction_token1] = aux_sym_add_instruction_token1,
  [aux_sym_copy_instruction_token1] = aux_sym_copy_instruction_token1,
  [aux_sym_entrypoint_instruction_token1] = aux_sym_entrypoint_instruction_token1,
  [aux_sym_volume_instruction_token1] = aux_sym_volume_instruction_token1,
  [aux_sym_user_instruction_token1] = aux_sym_user_instruction_token1,
  [anon_sym_COLON] = anon_sym_COLON,
  [aux_sym__user_name_or_group_token1] = aux_sym__user_name_or_group_token1,
  [aux_sym__immediate_user_name_or_group_fragment_token1] = aux_sym__immediate_user_name_or_group_fragment_token1,
  [aux_sym_workdir_instruction_token1] = aux_sym_workdir_instruction_token1,
  [aux_sym_arg_instruction_token1] = aux_sym_arg_instruction_token1,
  [aux_sym_arg_instruction_token2] = sym_unquoted_string,
  [anon_sym_EQ] = anon_sym_EQ,
  [aux_sym_onbuild_instruction_token1] = aux_sym_onbuild_instruction_token1,
  [aux_sym_stopsignal_instruction_token1] = aux_sym_stopsignal_instruction_token1,
  [aux_sym__stopsignal_value_token1] = aux_sym__stopsignal_value_token1,
  [aux_sym__stopsignal_value_token2] = aux_sym__stopsignal_value_token2,
  [aux_sym_healthcheck_instruction_token1] = aux_sym_healthcheck_instruction_token1,
  [anon_sym_NONE] = anon_sym_NONE,
  [aux_sym_shell_instruction_token1] = aux_sym_shell_instruction_token1,
  [aux_sym_maintainer_instruction_token1] = aux_sym_maintainer_instruction_token1,
  [aux_sym_maintainer_instruction_token2] = aux_sym_maintainer_instruction_token2,
  [aux_sym_cross_build_instruction_token1] = aux_sym_cross_build_instruction_token1,
  [aux_sym_path_token1] = aux_sym_path_token1,
  [aux_sym_path_token2] = aux_sym_path_token2,
  [aux_sym_path_token3] = aux_sym_path_token3,
  [aux_sym_path_with_heredoc_token1] = aux_sym_path_with_heredoc_token1,
  [anon_sym_DOLLAR] = anon_sym_DOLLAR,
  [anon_sym_DOLLAR2] = anon_sym_DOLLAR,
  [anon_sym_LBRACE] = anon_sym_LBRACE,
  [aux_sym__expansion_body_token1] = sym_variable,
  [anon_sym_RBRACE] = anon_sym_RBRACE,
  [sym_variable] = sym_variable,
  [aux_sym__spaced_env_pair_token1] = aux_sym__spaced_env_pair_token1,
  [aux_sym__env_key_token1] = sym_unquoted_string,
  [aux_sym_expose_port_token1] = aux_sym_expose_port_token1,
  [anon_sym_SLASHtcp] = anon_sym_SLASHtcp,
  [anon_sym_SLASHudp] = anon_sym_SLASHudp,
  [aux_sym_label_pair_token1] = sym_unquoted_string,
  [aux_sym_image_name_token1] = aux_sym_image_name_token1,
  [aux_sym_image_name_token2] = aux_sym_image_name_token2,
  [aux_sym_image_tag_token1] = aux_sym_image_tag_token1,
  [anon_sym_AT] = anon_sym_AT,
  [aux_sym_image_digest_token1] = aux_sym_image_digest_token1,
  [anon_sym_DASH_DASH] = anon_sym_DASH_DASH,
  [aux_sym_param_token1] = aux_sym_param_token1,
  [aux_sym_param_token2] = aux_sym_param_token2,
  [anon_sym_mount] = anon_sym_mount,
  [anon_sym_COMMA] = anon_sym_COMMA,
  [aux_sym_mount_param_param_token1] = aux_sym_mount_param_param_token1,
  [aux_sym_image_alias_token1] = aux_sym_image_alias_token1,
  [aux_sym_image_alias_token2] = aux_sym_image_alias_token2,
  [aux_sym_shell_fragment_token1] = aux_sym_shell_fragment_token1,
  [aux_sym_shell_fragment_token2] = aux_sym_shell_fragment_token2,
  [aux_sym_shell_fragment_token3] = aux_sym_shell_fragment_token3,
  [aux_sym_shell_fragment_token4] = aux_sym_shell_fragment_token4,
  [sym_line_continuation] = sym_line_continuation,
  [sym_required_line_continuation] = sym_line_continuation,
  [anon_sym_LBRACK] = anon_sym_LBRACK,
  [anon_sym_COMMA2] = anon_sym_COMMA,
  [anon_sym_RBRACK] = anon_sym_RBRACK,
  [anon_sym_DQUOTE] = anon_sym_DQUOTE,
  [aux_sym_json_string_token1] = aux_sym_json_string_token1,
  [sym_json_escape_sequence] = sym_json_escape_sequence,
  [aux_sym_double_quoted_string_token1] = aux_sym_double_quoted_string_token1,
  [anon_sym_BSLASH] = anon_sym_BSLASH,
  [anon_sym_SQUOTE] = anon_sym_SQUOTE,
  [aux_sym_single_quoted_string_token1] = aux_sym_single_quoted_string_token1,
  [aux_sym_unquoted_string_token1] = aux_sym_unquoted_string_token1,
  [anon_sym_BSLASH2] = anon_sym_BSLASH2,
  [sym_double_quoted_escape_sequence] = sym_json_escape_sequence,
  [sym_single_quoted_escape_sequence] = sym_json_escape_sequence,
  [sym__non_newline_whitespace] = sym__non_newline_whitespace,
  [sym_comment] = sym_comment,
  [sym_heredoc_marker] = sym_heredoc_marker,
  [sym_heredoc_line] = sym_heredoc_line,
  [sym_heredoc_end] = sym_heredoc_end,
  [sym_heredoc_nl] = sym_heredoc_nl,
  [sym_error_sentinel] = sym_error_sentinel,
  [sym_source_file] = sym_source_file,
  [sym__instruction] = sym__instruction,
  [sym_from_instruction] = sym_from_instruction,
  [sym_run_instruction] = sym_run_instruction,
  [sym_cmd_instruction] = sym_cmd_instruction,
  [sym_label_instruction] = sym_label_instruction,
  [sym_expose_instruction] = sym_expose_instruction,
  [sym_env_instruction] = sym_env_instruction,
  [sym_add_instruction] = sym_add_instruction,
  [sym_copy_instruction] = sym_copy_instruction,
  [sym_entrypoint_instruction] = sym_entrypoint_instruction,
  [sym_volume_instruction] = sym_volume_instruction,
  [sym_user_instruction] = sym_user_instruction,
  [sym__user_name_or_group] = sym_unquoted_string,
  [aux_sym__immediate_user_name_or_group] = aux_sym__immediate_user_name_or_group,
  [sym__immediate_user_name_or_group_fragment] = sym__immediate_user_name_or_group_fragment,
  [sym_workdir_instruction] = sym_workdir_instruction,
  [sym_arg_instruction] = sym_arg_instruction,
  [sym_onbuild_instruction] = sym_onbuild_instruction,
  [sym_stopsignal_instruction] = sym_stopsignal_instruction,
  [sym__stopsignal_value] = sym__stopsignal_value,
  [sym_healthcheck_instruction] = sym_healthcheck_instruction,
  [sym_shell_instruction] = sym_shell_instruction,
  [sym_maintainer_instruction] = sym_maintainer_instruction,
  [sym_cross_build_instruction] = sym_cross_build_instruction,
  [sym_heredoc_block] = sym_heredoc_block,
  [sym_path] = sym_path,
  [sym_path_with_heredoc] = sym_path,
  [sym_expansion] = sym_expansion,
  [sym__immediate_expansion] = sym__immediate_expansion,
  [sym__imm_expansion] = sym_expansion,
  [sym__expansion_body] = sym__expansion_body,
  [sym_env_pair] = sym_env_pair,
  [sym__spaced_env_pair] = sym_env_pair,
  [sym__env_key] = sym__env_key,
  [sym_expose_port] = sym_expose_port,
  [sym_label_pair] = sym_label_pair,
  [sym_image_spec] = sym_image_spec,
  [sym_image_name] = sym_image_name,
  [sym_image_tag] = sym_image_tag,
  [sym_image_digest] = sym_image_digest,
  [sym_param] = sym_param,
  [sym_mount_param] = sym_mount_param,
  [sym_mount_param_param] = sym_mount_param_param,
  [sym_image_alias] = sym_image_alias,
  [sym_shell_command] = sym_shell_command,
  [sym_shell_fragment] = sym_shell_fragment,
  [sym_json_string_array] = sym_json_string_array,
  [sym_json_string] = sym_json_string,
  [sym_double_quoted_string] = sym_double_quoted_string,
  [sym_single_quoted_string] = sym_single_quoted_string,
  [sym_unquoted_string] = sym_unquoted_string,
  [aux_sym_source_file_repeat1] = aux_sym_source_file_repeat1,
  [aux_sym_run_instruction_repeat1] = aux_sym_run_instruction_repeat1,
  [aux_sym_run_instruction_repeat2] = aux_sym_run_instruction_repeat2,
  [aux_sym_label_instruction_repeat1] = aux_sym_label_instruction_repeat1,
  [aux_sym_expose_instruction_repeat1] = aux_sym_expose_instruction_repeat1,
  [aux_sym_env_instruction_repeat1] = aux_sym_env_instruction_repeat1,
  [aux_sym_add_instruction_repeat1] = aux_sym_add_instruction_repeat1,
  [aux_sym_add_instruction_repeat2] = aux_sym_add_instruction_repeat2,
  [aux_sym_volume_instruction_repeat1] = aux_sym_volume_instruction_repeat1,
  [aux_sym__user_name_or_group_repeat1] = aux_sym__user_name_or_group_repeat1,
  [aux_sym__stopsignal_value_repeat1] = aux_sym__stopsignal_value_repeat1,
  [aux_sym_heredoc_block_repeat1] = aux_sym_heredoc_block_repeat1,
  [aux_sym_path_repeat1] = aux_sym_path_repeat1,
  [aux_sym_image_name_repeat1] = aux_sym_image_name_repeat1,
  [aux_sym_image_tag_repeat1] = aux_sym_image_tag_repeat1,
  [aux_sym_image_digest_repeat1] = aux_sym_image_digest_repeat1,
  [aux_sym_mount_param_repeat1] = aux_sym_mount_param_repeat1,
  [aux_sym_image_alias_repeat1] = aux_sym_image_alias_repeat1,
  [aux_sym_shell_command_repeat1] = aux_sym_shell_command_repeat1,
  [aux_sym_shell_fragment_repeat1] = aux_sym_shell_fragment_repeat1,
  [aux_sym_json_string_array_repeat1] = aux_sym_json_string_array_repeat1,
  [aux_sym_json_string_repeat1] = aux_sym_json_string_repeat1,
  [aux_sym_double_quoted_string_repeat1] = aux_sym_double_quoted_string_repeat1,
  [aux_sym_single_quoted_string_repeat1] = aux_sym_single_quoted_string_repeat1,
  [aux_sym_unquoted_string_repeat1] = aux_sym_unquoted_string_repeat1,
};

static const TSSymbolMetadata ts_symbol_metadata[] = {
  [ts_builtin_sym_end] = {
    .visible = false,
    .named = true,
  },
  [anon_sym_LF] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_from_instruction_token1] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_from_instruction_token2] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_run_instruction_token1] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_cmd_instruction_token1] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_label_instruction_token1] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_expose_instruction_token1] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_env_instruction_token1] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_add_instruction_token1] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_copy_instruction_token1] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_entrypoint_instruction_token1] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_volume_instruction_token1] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_user_instruction_token1] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_COLON] = {
    .visible = true,
    .named = false,
  },
  [aux_sym__user_name_or_group_token1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym__immediate_user_name_or_group_fragment_token1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_workdir_instruction_token1] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_arg_instruction_token1] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_arg_instruction_token2] = {
    .visible = true,
    .named = true,
  },
  [anon_sym_EQ] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_onbuild_instruction_token1] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_stopsignal_instruction_token1] = {
    .visible = true,
    .named = false,
  },
  [aux_sym__stopsignal_value_token1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym__stopsignal_value_token2] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_healthcheck_instruction_token1] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_NONE] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_shell_instruction_token1] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_maintainer_instruction_token1] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_maintainer_instruction_token2] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_cross_build_instruction_token1] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_path_token1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_path_token2] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_path_token3] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_path_with_heredoc_token1] = {
    .visible = false,
    .named = false,
  },
  [anon_sym_DOLLAR] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_DOLLAR2] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_LBRACE] = {
    .visible = true,
    .named = false,
  },
  [aux_sym__expansion_body_token1] = {
    .visible = true,
    .named = true,
  },
  [anon_sym_RBRACE] = {
    .visible = true,
    .named = false,
  },
  [sym_variable] = {
    .visible = true,
    .named = true,
  },
  [aux_sym__spaced_env_pair_token1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym__env_key_token1] = {
    .visible = true,
    .named = true,
  },
  [aux_sym_expose_port_token1] = {
    .visible = false,
    .named = false,
  },
  [anon_sym_SLASHtcp] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_SLASHudp] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_label_pair_token1] = {
    .visible = true,
    .named = true,
  },
  [aux_sym_image_name_token1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_image_name_token2] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_image_tag_token1] = {
    .visible = false,
    .named = false,
  },
  [anon_sym_AT] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_image_digest_token1] = {
    .visible = false,
    .named = false,
  },
  [anon_sym_DASH_DASH] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_param_token1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_param_token2] = {
    .visible = false,
    .named = false,
  },
  [anon_sym_mount] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_COMMA] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_mount_param_param_token1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_image_alias_token1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_image_alias_token2] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_shell_fragment_token1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_shell_fragment_token2] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_shell_fragment_token3] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_shell_fragment_token4] = {
    .visible = false,
    .named = false,
  },
  [sym_line_continuation] = {
    .visible = true,
    .named = true,
  },
  [sym_required_line_continuation] = {
    .visible = true,
    .named = true,
  },
  [anon_sym_LBRACK] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_COMMA2] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_RBRACK] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_DQUOTE] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_json_string_token1] = {
    .visible = false,
    .named = false,
  },
  [sym_json_escape_sequence] = {
    .visible = true,
    .named = true,
  },
  [aux_sym_double_quoted_string_token1] = {
    .visible = false,
    .named = false,
  },
  [anon_sym_BSLASH] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_SQUOTE] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_single_quoted_string_token1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_unquoted_string_token1] = {
    .visible = false,
    .named = false,
  },
  [anon_sym_BSLASH2] = {
    .visible = true,
    .named = false,
  },
  [sym_double_quoted_escape_sequence] = {
    .visible = true,
    .named = true,
  },
  [sym_single_quoted_escape_sequence] = {
    .visible = true,
    .named = true,
  },
  [sym__non_newline_whitespace] = {
    .visible = false,
    .named = true,
  },
  [sym_comment] = {
    .visible = true,
    .named = true,
  },
  [sym_heredoc_marker] = {
    .visible = true,
    .named = true,
  },
  [sym_heredoc_line] = {
    .visible = true,
    .named = true,
  },
  [sym_heredoc_end] = {
    .visible = true,
    .named = true,
  },
  [sym_heredoc_nl] = {
    .visible = true,
    .named = false,
  },
  [sym_error_sentinel] = {
    .visible = true,
    .named = true,
  },
  [sym_source_file] = {
    .visible = true,
    .named = true,
  },
  [sym__instruction] = {
    .visible = false,
    .named = true,
  },
  [sym_from_instruction] = {
    .visible = true,
    .named = true,
  },
  [sym_run_instruction] = {
    .visible = true,
    .named = true,
  },
  [sym_cmd_instruction] = {
    .visible = true,
    .named = true,
  },
  [sym_label_instruction] = {
    .visible = true,
    .named = true,
  },
  [sym_expose_instruction] = {
    .visible = true,
    .named = true,
  },
  [sym_env_instruction] = {
    .visible = true,
    .named = true,
  },
  [sym_add_instruction] = {
    .visible = true,
    .named = true,
  },
  [sym_copy_instruction] = {
    .visible = true,
    .named = true,
  },
  [sym_entrypoint_instruction] = {
    .visible = true,
    .named = true,
  },
  [sym_volume_instruction] = {
    .visible = true,
    .named = true,
  },
  [sym_user_instruction] = {
    .visible = true,
    .named = true,
  },
  [sym__user_name_or_group] = {
    .visible = true,
    .named = true,
  },
  [aux_sym__immediate_user_name_or_group] = {
    .visible = false,
    .named = false,
  },
  [sym__immediate_user_name_or_group_fragment] = {
    .visible = false,
    .named = true,
  },
  [sym_workdir_instruction] = {
    .visible = true,
    .named = true,
  },
  [sym_arg_instruction] = {
    .visible = true,
    .named = true,
  },
  [sym_onbuild_instruction] = {
    .visible = true,
    .named = true,
  },
  [sym_stopsignal_instruction] = {
    .visible = true,
    .named = true,
  },
  [sym__stopsignal_value] = {
    .visible = false,
    .named = true,
  },
  [sym_healthcheck_instruction] = {
    .visible = true,
    .named = true,
  },
  [sym_shell_instruction] = {
    .visible = true,
    .named = true,
  },
  [sym_maintainer_instruction] = {
    .visible = true,
    .named = true,
  },
  [sym_cross_build_instruction] = {
    .visible = true,
    .named = true,
  },
  [sym_heredoc_block] = {
    .visible = true,
    .named = true,
  },
  [sym_path] = {
    .visible = true,
    .named = true,
  },
  [sym_path_with_heredoc] = {
    .visible = true,
    .named = true,
  },
  [sym_expansion] = {
    .visible = true,
    .named = true,
  },
  [sym__immediate_expansion] = {
    .visible = false,
    .named = true,
  },
  [sym__imm_expansion] = {
    .visible = true,
    .named = true,
  },
  [sym__expansion_body] = {
    .visible = false,
    .named = true,
  },
  [sym_env_pair] = {
    .visible = true,
    .named = true,
  },
  [sym__spaced_env_pair] = {
    .visible = true,
    .named = true,
  },
  [sym__env_key] = {
    .visible = false,
    .named = true,
  },
  [sym_expose_port] = {
    .visible = true,
    .named = true,
  },
  [sym_label_pair] = {
    .visible = true,
    .named = true,
  },
  [sym_image_spec] = {
    .visible = true,
    .named = true,
  },
  [sym_image_name] = {
    .visible = true,
    .named = true,
  },
  [sym_image_tag] = {
    .visible = true,
    .named = true,
  },
  [sym_image_digest] = {
    .visible = true,
    .named = true,
  },
  [sym_param] = {
    .visible = true,
    .named = true,
  },
  [sym_mount_param] = {
    .visible = true,
    .named = true,
  },
  [sym_mount_param_param] = {
    .visible = true,
    .named = true,
  },
  [sym_image_alias] = {
    .visible = true,
    .named = true,
  },
  [sym_shell_command] = {
    .visible = true,
    .named = true,
  },
  [sym_shell_fragment] = {
    .visible = true,
    .named = true,
  },
  [sym_json_string_array] = {
    .visible = true,
    .named = true,
  },
  [sym_json_string] = {
    .visible = true,
    .named = true,
  },
  [sym_double_quoted_string] = {
    .visible = true,
    .named = true,
  },
  [sym_single_quoted_string] = {
    .visible = true,
    .named = true,
  },
  [sym_unquoted_string] = {
    .visible = true,
    .named = true,
  },
  [aux_sym_source_file_repeat1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_run_instruction_repeat1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_run_instruction_repeat2] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_label_instruction_repeat1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_expose_instruction_repeat1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_env_instruction_repeat1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_add_instruction_repeat1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_add_instruction_repeat2] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_volume_instruction_repeat1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym__user_name_or_group_repeat1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym__stopsignal_value_repeat1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_heredoc_block_repeat1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_path_repeat1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_image_name_repeat1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_image_tag_repeat1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_image_digest_repeat1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_mount_param_repeat1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_image_alias_repeat1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_shell_command_repeat1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_shell_fragment_repeat1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_json_string_array_repeat1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_json_string_repeat1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_double_quoted_string_repeat1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_single_quoted_string_repeat1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_unquoted_string_repeat1] = {
    .visible = false,
    .named = false,
  },
};

enum ts_field_identifiers {
  field_as = 1,
  field_default = 2,
  field_digest = 3,
  field_group = 4,
  field_key = 5,
  field_name = 6,
  field_tag = 7,
  field_user = 8,
  field_value = 9,
};

static const char * const ts_field_names[] = {
  [0] = NULL,
  [field_as] = "as",
  [field_default] = "default",
  [field_digest] = "digest",
  [field_group] = "group",
  [field_key] = "key",
  [field_name] = "name",
  [field_tag] = "tag",
  [field_user] = "user",
  [field_value] = "value",
};

static const TSFieldMapSlice ts_field_map_slices[PRODUCTION_ID_COUNT] = {
  [1] = {.index = 0, .length = 1},
  [2] = {.index = 1, .length = 2},
  [3] = {.index = 3, .length = 1},
  [4] = {.index = 4, .length = 1},
  [5] = {.index = 5, .length = 2},
  [6] = {.index = 7, .length = 2},
  [7] = {.index = 9, .length = 1},
  [8] = {.index = 10, .length = 3},
  [9] = {.index = 13, .length = 2},
  [10] = {.index = 15, .length = 2},
  [11] = {.index = 17, .length = 2},
  [12] = {.index = 19, .length = 2},
  [13] = {.index = 21, .length = 2},
  [14] = {.index = 23, .length = 1},
  [15] = {.index = 24, .length = 3},
};

static const TSFieldMapEntry ts_field_map_entries[] = {
  [0] =
    {field_name, 0},
  [1] =
    {field_name, 1, .inherited = true},
    {field_value, 1, .inherited = true},
  [3] =
    {field_user, 1},
  [4] =
    {field_name, 1},
  [5] =
    {field_name, 0},
    {field_tag, 1},
  [7] =
    {field_digest, 1},
    {field_name, 0},
  [9] =
    {field_as, 3},
  [10] =
    {field_digest, 2},
    {field_name, 0},
    {field_tag, 1},
  [13] =
    {field_key, 0},
    {field_value, 2},
  [15] =
    {field_name, 0},
    {field_value, 2},
  [17] =
    {field_group, 3},
    {field_user, 1},
  [19] =
    {field_default, 3},
    {field_name, 1},
  [21] =
    {field_name, 1},
    {field_value, 3},
  [23] =
    {field_as, 4},
  [24] =
    {field_name, 1},
    {field_value, 3},
    {field_value, 4},
};

static const TSSymbol ts_alias_sequences[PRODUCTION_ID_COUNT][MAX_ALIAS_SEQUENCE_LENGTH] = {
  [0] = {0},
  [11] = {
    [3] = sym_unquoted_string,
  },
};

static const uint16_t ts_non_terminal_alias_map[] = {
  aux_sym__immediate_user_name_or_group, 2,
    aux_sym__immediate_user_name_or_group,
    sym_unquoted_string,
  0,
};

static const TSStateId ts_primary_state_ids[STATE_COUNT] = {
  [0] = 0,
  [1] = 1,
  [2] = 2,
  [3] = 3,
  [4] = 4,
  [5] = 5,
  [6] = 6,
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
  [24] = 24,
  [25] = 25,
  [26] = 26,
  [27] = 27,
  [28] = 28,
  [29] = 29,
  [30] = 30,
  [31] = 31,
  [32] = 32,
  [33] = 30,
  [34] = 34,
  [35] = 35,
  [36] = 36,
  [37] = 37,
  [38] = 38,
  [39] = 39,
  [40] = 40,
  [41] = 41,
  [42] = 42,
  [43] = 22,
  [44] = 44,
  [45] = 23,
  [46] = 46,
  [47] = 18,
  [48] = 20,
  [49] = 49,
  [50] = 36,
  [51] = 30,
  [52] = 36,
  [53] = 53,
  [54] = 54,
  [55] = 55,
  [56] = 56,
  [57] = 57,
  [58] = 58,
  [59] = 59,
  [60] = 60,
  [61] = 61,
  [62] = 32,
  [63] = 58,
  [64] = 64,
  [65] = 65,
  [66] = 66,
  [67] = 49,
  [68] = 68,
  [69] = 69,
  [70] = 70,
  [71] = 71,
  [72] = 72,
  [73] = 73,
  [74] = 74,
  [75] = 72,
  [76] = 60,
  [77] = 77,
  [78] = 73,
  [79] = 79,
  [80] = 80,
  [81] = 70,
  [82] = 49,
  [83] = 83,
  [84] = 44,
  [85] = 85,
  [86] = 49,
  [87] = 87,
  [88] = 88,
  [89] = 89,
  [90] = 90,
  [91] = 91,
  [92] = 92,
  [93] = 46,
  [94] = 94,
  [95] = 72,
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
  [109] = 72,
  [110] = 110,
  [111] = 68,
  [112] = 112,
  [113] = 80,
  [114] = 73,
  [115] = 73,
  [116] = 116,
  [117] = 72,
  [118] = 73,
  [119] = 72,
  [120] = 73,
  [121] = 121,
  [122] = 112,
  [123] = 106,
  [124] = 124,
  [125] = 112,
  [126] = 106,
  [127] = 127,
  [128] = 128,
  [129] = 129,
  [130] = 130,
  [131] = 131,
  [132] = 132,
  [133] = 80,
  [134] = 72,
  [135] = 135,
  [136] = 73,
  [137] = 80,
  [138] = 72,
  [139] = 73,
  [140] = 140,
  [141] = 141,
  [142] = 142,
  [143] = 143,
  [144] = 144,
  [145] = 145,
  [146] = 146,
  [147] = 147,
  [148] = 148,
  [149] = 73,
  [150] = 150,
  [151] = 151,
  [152] = 152,
  [153] = 64,
  [154] = 154,
  [155] = 155,
  [156] = 156,
  [157] = 157,
  [158] = 158,
  [159] = 159,
  [160] = 160,
  [161] = 72,
  [162] = 162,
  [163] = 163,
  [164] = 164,
  [165] = 165,
  [166] = 166,
  [167] = 150,
  [168] = 168,
  [169] = 169,
  [170] = 80,
  [171] = 171,
  [172] = 171,
  [173] = 152,
  [174] = 129,
  [175] = 73,
  [176] = 80,
  [177] = 72,
  [178] = 73,
  [179] = 179,
  [180] = 180,
  [181] = 80,
  [182] = 72,
  [183] = 73,
  [184] = 80,
  [185] = 72,
  [186] = 171,
  [187] = 187,
  [188] = 80,
  [189] = 189,
  [190] = 190,
  [191] = 191,
  [192] = 73,
  [193] = 72,
  [194] = 162,
  [195] = 191,
  [196] = 189,
  [197] = 162,
  [198] = 198,
  [199] = 179,
  [200] = 171,
  [201] = 162,
  [202] = 202,
  [203] = 171,
  [204] = 171,
  [205] = 162,
  [206] = 171,
  [207] = 162,
  [208] = 171,
  [209] = 162,
  [210] = 171,
  [211] = 162,
  [212] = 171,
  [213] = 162,
  [214] = 171,
  [215] = 171,
  [216] = 171,
  [217] = 73,
  [218] = 218,
  [219] = 219,
  [220] = 220,
  [221] = 68,
  [222] = 148,
  [223] = 151,
  [224] = 224,
  [225] = 159,
  [226] = 160,
  [227] = 227,
  [228] = 228,
  [229] = 229,
  [230] = 230,
  [231] = 231,
  [232] = 68,
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
  [243] = 187,
  [244] = 244,
  [245] = 245,
  [246] = 246,
  [247] = 247,
  [248] = 248,
  [249] = 249,
  [250] = 250,
  [251] = 248,
  [252] = 159,
  [253] = 253,
  [254] = 160,
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
  [267] = 267,
  [268] = 268,
  [269] = 236,
  [270] = 270,
  [271] = 271,
  [272] = 148,
  [273] = 273,
  [274] = 274,
  [275] = 275,
  [276] = 267,
  [277] = 268,
  [278] = 278,
  [279] = 151,
  [280] = 267,
  [281] = 268,
  [282] = 282,
  [283] = 267,
  [284] = 267,
  [285] = 285,
  [286] = 260,
  [287] = 267,
  [288] = 218,
  [289] = 231,
  [290] = 267,
  [291] = 291,
  [292] = 292,
  [293] = 267,
  [294] = 168,
  [295] = 295,
  [296] = 267,
  [297] = 297,
  [298] = 267,
  [299] = 299,
  [300] = 267,
  [301] = 301,
  [302] = 267,
  [303] = 303,
  [304] = 267,
  [305] = 267,
  [306] = 248,
  [307] = 274,
  [308] = 248,
  [309] = 274,
  [310] = 248,
  [311] = 274,
  [312] = 248,
  [313] = 313,
  [314] = 248,
  [315] = 248,
  [316] = 248,
  [317] = 248,
  [318] = 248,
  [319] = 248,
  [320] = 248,
  [321] = 248,
  [322] = 303,
  [323] = 303,
  [324] = 268,
};

static bool ts_lex(TSLexer *lexer, TSStateId state) {
  START_LEXER();
  eof = lexer->eof(lexer);
  switch (state) {
    case 0:
      if (eof) ADVANCE(167);
      ADVANCE_MAP(
        '"', 270,
        '#', 205,
        '$', 213,
        '\'', 281,
        ',', 252,
        '-', 260,
        ':', 181,
        '<', 228,
        '=', 190,
        '@', 239,
        '[', 267,
        '\\', 278,
        ']', 269,
        '_', 220,
        '{', 214,
        '}', 219,
      );
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') SKIP(164);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(185);
      if (('A' <= lookahead && lookahead <= 'Z') ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(184);
      if (lookahead != 0) ADVANCE(203);
      END_STATE();
    case 1:
      if (lookahead == '\t') ADVANCE(3);
      if (lookahead == '\n') ADVANCE(265);
      if (lookahead == ' ') ADVANCE(289);
      END_STATE();
    case 2:
      ADVANCE_MAP(
        '\n', 265,
        'u', 161,
        '\t', 3,
        ' ', 3,
        '"', 274,
        '/', 274,
        '\\', 274,
        'b', 274,
        'f', 274,
        'n', 274,
        'r', 274,
        't', 274,
      );
      END_STATE();
    case 3:
      if (lookahead == '\n') ADVANCE(265);
      if (lookahead == '\t' ||
          lookahead == ' ') ADVANCE(3);
      END_STATE();
    case 4:
      if (lookahead == '\n') ADVANCE(265);
      if (lookahead == '\t' ||
          lookahead == ' ') ADVANCE(264);
      if (lookahead != 0 &&
          lookahead != ',' &&
          lookahead != '-' &&
          lookahead != '=') ADVANCE(263);
      END_STATE();
    case 5:
      if (lookahead == '\n') ADVANCE(168);
      if (lookahead == '"') ADVANCE(270);
      if (lookahead == '#') ADVANCE(293);
      if (lookahead == '\'') ADVANCE(281);
      if (lookahead == '\\') ADVANCE(3);
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') SKIP(5);
      if (('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(222);
      END_STATE();
    case 6:
      if (lookahead == '\n') ADVANCE(168);
      if (lookahead == '"') ADVANCE(270);
      if (lookahead == '#') ADVANCE(293);
      if (lookahead == '\'') ADVANCE(281);
      if (lookahead == '\\') ADVANCE(3);
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') SKIP(6);
      if (lookahead == '-' ||
          lookahead == '.' ||
          ('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(227);
      END_STATE();
    case 7:
      if (lookahead == '\n') ADVANCE(168);
      if (lookahead == '"') ADVANCE(270);
      if (lookahead == '#') ADVANCE(285);
      if (lookahead == '$') ADVANCE(213);
      if (lookahead == '\'') ADVANCE(281);
      if (lookahead == '\\') ADVANCE(1);
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') SKIP(5);
      if (('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(286);
      if (lookahead != 0) ADVANCE(288);
      END_STATE();
    case 8:
      if (lookahead == '\n') ADVANCE(168);
      if (lookahead == '"') ADVANCE(270);
      if (lookahead == '#') ADVANCE(285);
      if (lookahead == '$') ADVANCE(213);
      if (lookahead == '\'') ADVANCE(281);
      if (lookahead == '\\') ADVANCE(1);
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') SKIP(6);
      if (lookahead == '-' ||
          lookahead == '.' ||
          ('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(287);
      if (lookahead != 0) ADVANCE(288);
      END_STATE();
    case 9:
      if (lookahead == '\n') ADVANCE(168);
      if (lookahead == '#') ADVANCE(293);
      if (lookahead == '$') ADVANCE(213);
      if (lookahead == ':') ADVANCE(181);
      if (lookahead == '=') ADVANCE(190);
      if (lookahead == '\\') ADVANCE(3);
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') SKIP(18);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(185);
      if (('A' <= lookahead && lookahead <= 'Z') ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(186);
      END_STATE();
    case 10:
      if (lookahead == '\n') ADVANCE(168);
      if (lookahead == '#') ADVANCE(293);
      if (lookahead == '$') ADVANCE(213);
      if (lookahead == '\\') ADVANCE(3);
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(240);
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') SKIP(17);
      if (('0' <= lookahead && lookahead <= ':') ||
          ('B' <= lookahead && lookahead <= 'Z') ||
          ('b' <= lookahead && lookahead <= 'z')) ADVANCE(241);
      END_STATE();
    case 11:
      if (lookahead == '\n') ADVANCE(168);
      if (lookahead == '#') ADVANCE(293);
      if (lookahead == '$') ADVANCE(213);
      if (lookahead == '\\') ADVANCE(3);
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') SKIP(18);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z')) ADVANCE(194);
      END_STATE();
    case 12:
      if (lookahead == '\n') ADVANCE(168);
      if (lookahead == '#') ADVANCE(293);
      if (lookahead == '$') ADVANCE(213);
      if (lookahead == '\\') ADVANCE(3);
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') SKIP(18);
      if (lookahead == '-' ||
          ('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(257);
      END_STATE();
    case 13:
      ADVANCE_MAP(
        '\n', 168,
        '#', 293,
        '$', 212,
        '/', 67,
        ':', 181,
        '@', 239,
        '\\', 3,
        'A', 142,
        'a', 142,
      );
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') SKIP(14);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(223);
      END_STATE();
    case 14:
      if (lookahead == '\n') ADVANCE(168);
      if (lookahead == '#') ADVANCE(293);
      if (lookahead == '$') ADVANCE(212);
      if (lookahead == '/') ADVANCE(67);
      if (lookahead == '\\') ADVANCE(3);
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(142);
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') SKIP(14);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(223);
      END_STATE();
    case 15:
      if (lookahead == '\n') ADVANCE(168);
      if (lookahead == '#') ADVANCE(293);
      if (lookahead == '<') ADVANCE(261);
      if (lookahead == '\\') ADVANCE(24);
      if (lookahead == ',' ||
          lookahead == '-' ||
          lookahead == '=') ADVANCE(259);
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') SKIP(15);
      if (lookahead != 0 &&
          lookahead != '[' &&
          lookahead != '\\') ADVANCE(262);
      END_STATE();
    case 16:
      if (lookahead == '\n') ADVANCE(168);
      if (lookahead == '#') ADVANCE(293);
      if (lookahead == '\\') ADVANCE(3);
      if (lookahead == '\t' ||
          lookahead == ' ') ADVANCE(292);
      if ((0x0b <= lookahead && lookahead <= '\r')) SKIP(18);
      END_STATE();
    case 17:
      if (lookahead == '\n') ADVANCE(168);
      if (lookahead == '#') ADVANCE(293);
      if (lookahead == '\\') ADVANCE(3);
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(142);
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') SKIP(17);
      END_STATE();
    case 18:
      if (lookahead == '\n') ADVANCE(168);
      if (lookahead == '#') ADVANCE(293);
      if (lookahead == '\\') ADVANCE(3);
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') SKIP(18);
      END_STATE();
    case 19:
      if (lookahead == '\n') ADVANCE(168);
      if (lookahead == '#') ADVANCE(293);
      if (lookahead == '\\') ADVANCE(3);
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') SKIP(19);
      if (('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(222);
      END_STATE();
    case 20:
      ADVANCE_MAP(
        '\n', 168,
        '#', 233,
        '$', 213,
        ':', 181,
        '@', 239,
        '\\', 231,
        'A', 232,
        'a', 232,
      );
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') SKIP(17);
      if (lookahead != 0) ADVANCE(234);
      END_STATE();
    case 21:
      if (lookahead == '\n') ADVANCE(168);
      if (lookahead == '#') ADVANCE(237);
      if (lookahead == '$') ADVANCE(213);
      if (lookahead == '@') ADVANCE(239);
      if (lookahead == '\\') ADVANCE(235);
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(236);
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') SKIP(17);
      if (lookahead != 0) ADVANCE(238);
      END_STATE();
    case 22:
      if (lookahead == '\n') ADVANCE(168);
      if (lookahead == '#') ADVANCE(209);
      if (lookahead == '$') ADVANCE(213);
      if (lookahead == '\\') ADVANCE(208);
      if (lookahead == '\t' ||
          lookahead == ' ') ADVANCE(292);
      if ((0x0b <= lookahead && lookahead <= '\r')) SKIP(18);
      if (lookahead != 0) ADVANCE(210);
      END_STATE();
    case 23:
      if (lookahead == '\n') ADVANCE(168);
      if (lookahead == '#') ADVANCE(209);
      if (lookahead == '$') ADVANCE(213);
      if (lookahead == '\\') ADVANCE(208);
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') SKIP(18);
      if (lookahead != 0) ADVANCE(210);
      END_STATE();
    case 24:
      if (lookahead == '\n') ADVANCE(266);
      if (lookahead == '\t' ||
          lookahead == ' ') ADVANCE(264);
      if (lookahead != 0 &&
          lookahead != ',' &&
          lookahead != '-' &&
          lookahead != '=') ADVANCE(263);
      END_STATE();
    case 25:
      if (lookahead == '\n') SKIP(29);
      if (lookahead == '"') ADVANCE(270);
      if (lookahead == '#') ADVANCE(276);
      if (lookahead == '$') ADVANCE(213);
      if (lookahead == '\\') ADVANCE(279);
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') ADVANCE(275);
      if (lookahead != 0) ADVANCE(277);
      END_STATE();
    case 26:
      if (lookahead == '\n') SKIP(41);
      if (lookahead == '#') ADVANCE(283);
      if (lookahead == '\'') ADVANCE(281);
      if (lookahead == '\\') ADVANCE(280);
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') ADVANCE(282);
      if (lookahead != 0) ADVANCE(284);
      END_STATE();
    case 27:
      if (lookahead == '"') ADVANCE(270);
      if (lookahead == '#') ADVANCE(293);
      if (lookahead == '\'') ADVANCE(281);
      if (lookahead == '\\') ADVANCE(3);
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') SKIP(27);
      END_STATE();
    case 28:
      if (lookahead == '"') ADVANCE(270);
      if (lookahead == '#') ADVANCE(293);
      if (lookahead == '\'') ADVANCE(281);
      if (lookahead == '\\') ADVANCE(3);
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') SKIP(28);
      if (lookahead == '-' ||
          lookahead == '.' ||
          ('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(227);
      END_STATE();
    case 29:
      if (lookahead == '"') ADVANCE(270);
      if (lookahead == '#') ADVANCE(293);
      if (lookahead == '\\') ADVANCE(278);
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') SKIP(29);
      END_STATE();
    case 30:
      if (lookahead == '"') ADVANCE(270);
      if (lookahead == '#') ADVANCE(285);
      if (lookahead == '$') ADVANCE(213);
      if (lookahead == '\'') ADVANCE(281);
      if (lookahead == '\\') ADVANCE(1);
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') SKIP(27);
      if (lookahead != 0) ADVANCE(288);
      END_STATE();
    case 31:
      if (lookahead == '"') ADVANCE(270);
      if (lookahead == '#') ADVANCE(271);
      if (lookahead == '\\') ADVANCE(2);
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') ADVANCE(272);
      if (lookahead != 0) ADVANCE(273);
      END_STATE();
    case 32:
      if (lookahead == '#') ADVANCE(205);
      if (lookahead == '$') ADVANCE(212);
      if (lookahead == '-') ADVANCE(58);
      if (lookahead == '<') ADVANCE(162);
      if (lookahead == '\\') ADVANCE(204);
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') SKIP(32);
      if (lookahead != 0) ADVANCE(203);
      END_STATE();
    case 33:
      if (lookahead == '#') ADVANCE(205);
      if (lookahead == '$') ADVANCE(212);
      if (lookahead == '<') ADVANCE(163);
      if (lookahead == '[') ADVANCE(267);
      if (lookahead == '\\') ADVANCE(204);
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') SKIP(33);
      if (lookahead != 0 &&
          lookahead != '-') ADVANCE(203);
      END_STATE();
    case 34:
      if (lookahead == '#') ADVANCE(205);
      if (lookahead == '$') ADVANCE(212);
      if (lookahead == '<') ADVANCE(163);
      if (lookahead == '\\') ADVANCE(204);
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') SKIP(34);
      if (lookahead != 0 &&
          lookahead != '-') ADVANCE(203);
      END_STATE();
    case 35:
      if (lookahead == '#') ADVANCE(293);
      if (lookahead == '$') ADVANCE(213);
      if (lookahead == '\\') ADVANCE(3);
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') SKIP(48);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(185);
      if (('A' <= lookahead && lookahead <= 'Z') ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(186);
      END_STATE();
    case 36:
      if (lookahead == '#') ADVANCE(293);
      if (lookahead == '$') ADVANCE(213);
      if (lookahead == '\\') ADVANCE(3);
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') SKIP(48);
      if (('0' <= lookahead && lookahead <= ':') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(241);
      END_STATE();
    case 37:
      if (lookahead == '#') ADVANCE(293);
      if (lookahead == '$') ADVANCE(212);
      if (lookahead == '\\') ADVANCE(3);
      if (lookahead == 'm') ADVANCE(244);
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') SKIP(38);
      if (('a' <= lookahead && lookahead <= 'z')) ADVANCE(247);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z')) ADVANCE(193);
      END_STATE();
    case 38:
      if (lookahead == '#') ADVANCE(293);
      if (lookahead == '$') ADVANCE(212);
      if (lookahead == '\\') ADVANCE(3);
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') SKIP(38);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z')) ADVANCE(193);
      END_STATE();
    case 39:
      if (lookahead == '#') ADVANCE(293);
      if (lookahead == '$') ADVANCE(212);
      if (lookahead == '\\') ADVANCE(3);
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') SKIP(39);
      if (lookahead == '-' ||
          ('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(256);
      END_STATE();
    case 40:
      if (lookahead == '#') ADVANCE(293);
      if (lookahead == '$') ADVANCE(212);
      if (lookahead == '\\') ADVANCE(3);
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') SKIP(40);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(182);
      if (('A' <= lookahead && lookahead <= 'Z') ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(183);
      END_STATE();
    case 41:
      if (lookahead == '#') ADVANCE(293);
      if (lookahead == '\'') ADVANCE(281);
      if (lookahead == '\\') ADVANCE(278);
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') SKIP(41);
      END_STATE();
    case 42:
      if (lookahead == '#') ADVANCE(293);
      if (lookahead == ',') ADVANCE(252);
      if (lookahead == '-') ADVANCE(260);
      if (lookahead == '<') ADVANCE(261);
      if (lookahead == '=') ADVANCE(259);
      if (lookahead == '[') ADVANCE(267);
      if (lookahead == '\\') ADVANCE(4);
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') SKIP(43);
      if (lookahead != 0) ADVANCE(262);
      END_STATE();
    case 43:
      if (lookahead == '#') ADVANCE(293);
      if (lookahead == '-') ADVANCE(260);
      if (lookahead == '<') ADVANCE(261);
      if (lookahead == '[') ADVANCE(267);
      if (lookahead == '\\') ADVANCE(4);
      if (lookahead == ',' ||
          lookahead == '=') ADVANCE(259);
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') SKIP(43);
      if (lookahead != 0) ADVANCE(262);
      END_STATE();
    case 44:
      if (lookahead == '#') ADVANCE(293);
      if (lookahead == '<') ADVANCE(261);
      if (lookahead == '[') ADVANCE(267);
      if (lookahead == '\\') ADVANCE(4);
      if (lookahead == ',' ||
          lookahead == '-' ||
          lookahead == '=') ADVANCE(259);
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') SKIP(44);
      if (lookahead != 0) ADVANCE(262);
      END_STATE();
    case 45:
      if (lookahead == '#') ADVANCE(293);
      if (lookahead == '=') ADVANCE(190);
      if (lookahead == '\\') ADVANCE(3);
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') ADVANCE(221);
      END_STATE();
    case 46:
      if (lookahead == '#') ADVANCE(293);
      if (lookahead == '\\') ADVANCE(3);
      if (lookahead == '{') ADVANCE(214);
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') SKIP(48);
      if (('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(220);
      END_STATE();
    case 47:
      if (lookahead == '#') ADVANCE(293);
      if (lookahead == '\\') ADVANCE(3);
      if (lookahead == '\t' ||
          lookahead == ' ') ADVANCE(292);
      if (('\n' <= lookahead && lookahead <= '\r')) SKIP(48);
      END_STATE();
    case 48:
      if (lookahead == '#') ADVANCE(293);
      if (lookahead == '\\') ADVANCE(3);
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') SKIP(48);
      END_STATE();
    case 49:
      if (lookahead == '#') ADVANCE(293);
      if (lookahead == '\\') ADVANCE(3);
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') SKIP(48);
      if (('a' <= lookahead && lookahead <= 'z')) ADVANCE(247);
      END_STATE();
    case 50:
      if (lookahead == '#') ADVANCE(293);
      if (lookahead == '\\') ADVANCE(3);
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') SKIP(50);
      if (('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(222);
      END_STATE();
    case 51:
      if (lookahead == '#') ADVANCE(293);
      if (lookahead == '\\') ADVANCE(3);
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') SKIP(51);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(189);
      END_STATE();
    case 52:
      if (lookahead == '#') ADVANCE(237);
      if (lookahead == '$') ADVANCE(213);
      if (lookahead == '\\') ADVANCE(235);
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') SKIP(48);
      if (lookahead != 0 &&
          lookahead != '@') ADVANCE(238);
      END_STATE();
    case 53:
      if (lookahead == '#') ADVANCE(209);
      if (lookahead == '$') ADVANCE(213);
      if (lookahead == '\\') ADVANCE(208);
      if (lookahead == '\t' ||
          lookahead == ' ') ADVANCE(292);
      if (('\n' <= lookahead && lookahead <= '\r')) SKIP(48);
      if (lookahead != 0) ADVANCE(210);
      END_STATE();
    case 54:
      if (lookahead == '#') ADVANCE(230);
      if (lookahead == '$') ADVANCE(212);
      if (lookahead == '-') ADVANCE(58);
      if (lookahead == '\\') ADVANCE(229);
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') SKIP(54);
      if (lookahead != 0 &&
          lookahead != ':' &&
          lookahead != '@') ADVANCE(228);
      END_STATE();
    case 55:
      if (lookahead == '#') ADVANCE(254);
      if (lookahead == '\\') ADVANCE(253);
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') SKIP(48);
      if (lookahead != 0 &&
          lookahead != ',' &&
          lookahead != '=') ADVANCE(255);
      END_STATE();
    case 56:
      if (lookahead == '#') ADVANCE(215);
      if (lookahead == '\\') ADVANCE(216);
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') ADVANCE(217);
      if (lookahead != 0 &&
          lookahead != '}') ADVANCE(218);
      END_STATE();
    case 57:
      if (lookahead == '#') ADVANCE(249);
      if (lookahead == '\\') ADVANCE(248);
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') SKIP(48);
      if (lookahead != 0) ADVANCE(250);
      END_STATE();
    case 58:
      if (lookahead == '-') ADVANCE(242);
      END_STATE();
    case 59:
      if (lookahead == 'E') ADVANCE(196);
      END_STATE();
    case 60:
      if (lookahead == 'N') ADVANCE(59);
      END_STATE();
    case 61:
      if (lookahead == 'O') ADVANCE(60);
      END_STATE();
    case 62:
      if (lookahead == '_') ADVANCE(75);
      END_STATE();
    case 63:
      if (lookahead == 'c') ADVANCE(65);
      END_STATE();
    case 64:
      if (lookahead == 'd') ADVANCE(66);
      END_STATE();
    case 65:
      if (lookahead == 'p') ADVANCE(225);
      END_STATE();
    case 66:
      if (lookahead == 'p') ADVANCE(226);
      END_STATE();
    case 67:
      if (lookahead == 't') ADVANCE(63);
      if (lookahead == 'u') ADVANCE(64);
      END_STATE();
    case 68:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(74);
      END_STATE();
    case 69:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(98);
      END_STATE();
    case 70:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(106);
      END_STATE();
    case 71:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(109);
      END_STATE();
    case 72:
      if (lookahead == 'A' ||
          lookahead == 'a') ADVANCE(99);
      END_STATE();
    case 73:
      if (lookahead == 'B' ||
          lookahead == 'b') ADVANCE(152);
      END_STATE();
    case 74:
      if (lookahead == 'B' ||
          lookahead == 'b') ADVANCE(90);
      END_STATE();
    case 75:
      if (lookahead == 'B' ||
          lookahead == 'b') ADVANCE(154);
      END_STATE();
    case 76:
      if (lookahead == 'C' ||
          lookahead == 'c') ADVANCE(104);
      END_STATE();
    case 77:
      if (lookahead == 'C' ||
          lookahead == 'c') ADVANCE(96);
      END_STATE();
    case 78:
      if (lookahead == 'D' ||
          lookahead == 'd') ADVANCE(79);
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(92);
      END_STATE();
    case 79:
      if (lookahead == 'D' ||
          lookahead == 'd') ADVANCE(176);
      END_STATE();
    case 80:
      if (lookahead == 'D' ||
          lookahead == 'd') ADVANCE(172);
      END_STATE();
    case 81:
      if (lookahead == 'D' ||
          lookahead == 'd') ADVANCE(191);
      END_STATE();
    case 82:
      if (lookahead == 'D' ||
          lookahead == 'd') ADVANCE(202);
      END_STATE();
    case 83:
      if (lookahead == 'D' ||
          lookahead == 'd') ADVANCE(100);
      END_STATE();
    case 84:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(174);
      END_STATE();
    case 85:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(179);
      END_STATE();
    case 86:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(76);
      END_STATE();
    case 87:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(70);
      END_STATE();
    case 88:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(113);
      END_STATE();
    case 89:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(136);
      END_STATE();
    case 90:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(107);
      END_STATE();
    case 91:
      if (lookahead == 'E' ||
          lookahead == 'e') ADVANCE(138);
      END_STATE();
    case 92:
      if (lookahead == 'G' ||
          lookahead == 'g') ADVANCE(188);
      END_STATE();
    case 93:
      if (lookahead == 'G' ||
          lookahead == 'g') ADVANCE(122);
      END_STATE();
    case 94:
      if (lookahead == 'H' ||
          lookahead == 'h') ADVANCE(77);
      END_STATE();
    case 95:
      if (lookahead == 'H' ||
          lookahead == 'h') ADVANCE(88);
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(128);
      END_STATE();
    case 96:
      if (lookahead == 'H' ||
          lookahead == 'h') ADVANCE(86);
      END_STATE();
    case 97:
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(93);
      END_STATE();
    case 98:
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(120);
      END_STATE();
    case 99:
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(123);
      END_STATE();
    case 100:
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(137);
      END_STATE();
    case 101:
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(121);
      END_STATE();
    case 102:
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(111);
      END_STATE();
    case 103:
      if (lookahead == 'I' ||
          lookahead == 'i') ADVANCE(112);
      END_STATE();
    case 104:
      if (lookahead == 'K' ||
          lookahead == 'k') ADVANCE(195);
      END_STATE();
    case 105:
      if (lookahead == 'K' ||
          lookahead == 'k') ADVANCE(83);
      END_STATE();
    case 106:
      if (lookahead == 'L' ||
          lookahead == 'l') ADVANCE(147);
      END_STATE();
    case 107:
      if (lookahead == 'L' ||
          lookahead == 'l') ADVANCE(173);
      END_STATE();
    case 108:
      if (lookahead == 'L' ||
          lookahead == 'l') ADVANCE(197);
      END_STATE();
    case 109:
      if (lookahead == 'L' ||
          lookahead == 'l') ADVANCE(192);
      END_STATE();
    case 110:
      if (lookahead == 'L' ||
          lookahead == 'l') ADVANCE(153);
      END_STATE();
    case 111:
      if (lookahead == 'L' ||
          lookahead == 'l') ADVANCE(81);
      END_STATE();
    case 112:
      if (lookahead == 'L' ||
          lookahead == 'l') ADVANCE(82);
      END_STATE();
    case 113:
      if (lookahead == 'L' ||
          lookahead == 'l') ADVANCE(108);
      END_STATE();
    case 114:
      if (lookahead == 'M' ||
          lookahead == 'm') ADVANCE(169);
      END_STATE();
    case 115:
      if (lookahead == 'M' ||
          lookahead == 'm') ADVANCE(80);
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(131);
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(127);
      END_STATE();
    case 116:
      if (lookahead == 'M' ||
          lookahead == 'm') ADVANCE(85);
      END_STATE();
    case 117:
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(149);
      if (lookahead == 'X' ||
          lookahead == 'x') ADVANCE(133);
      END_STATE();
    case 118:
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(171);
      END_STATE();
    case 119:
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(73);
      END_STATE();
    case 120:
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(150);
      END_STATE();
    case 121:
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(148);
      END_STATE();
    case 122:
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(71);
      END_STATE();
    case 123:
      if (lookahead == 'N' ||
          lookahead == 'n') ADVANCE(91);
      END_STATE();
    case 124:
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(110);
      END_STATE();
    case 125:
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(114);
      END_STATE();
    case 126:
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(135);
      END_STATE();
    case 127:
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(144);
      END_STATE();
    case 128:
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(132);
      END_STATE();
    case 129:
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(146);
      END_STATE();
    case 130:
      if (lookahead == 'O' ||
          lookahead == 'o') ADVANCE(101);
      END_STATE();
    case 131:
      if (lookahead == 'P' ||
          lookahead == 'p') ADVANCE(155);
      END_STATE();
    case 132:
      if (lookahead == 'P' ||
          lookahead == 'p') ADVANCE(145);
      END_STATE();
    case 133:
      if (lookahead == 'P' ||
          lookahead == 'p') ADVANCE(129);
      END_STATE();
    case 134:
      if (lookahead == 'P' ||
          lookahead == 'p') ADVANCE(130);
      END_STATE();
    case 135:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(105);
      END_STATE();
    case 136:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(180);
      END_STATE();
    case 137:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(187);
      END_STATE();
    case 138:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(198);
      END_STATE();
    case 139:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(156);
      END_STATE();
    case 140:
      if (lookahead == 'R' ||
          lookahead == 'r') ADVANCE(125);
      END_STATE();
    case 141:
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(62);
      END_STATE();
    case 142:
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(170);
      END_STATE();
    case 143:
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(89);
      END_STATE();
    case 144:
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(141);
      END_STATE();
    case 145:
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(97);
      END_STATE();
    case 146:
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(84);
      END_STATE();
    case 147:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(94);
      END_STATE();
    case 148:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(178);
      END_STATE();
    case 149:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(139);
      if (lookahead == 'V' ||
          lookahead == 'v') ADVANCE(175);
      END_STATE();
    case 150:
      if (lookahead == 'T' ||
          lookahead == 't') ADVANCE(72);
      END_STATE();
    case 151:
      if (lookahead == 'U' ||
          lookahead == 'u') ADVANCE(118);
      END_STATE();
    case 152:
      if (lookahead == 'U' ||
          lookahead == 'u') ADVANCE(102);
      END_STATE();
    case 153:
      if (lookahead == 'U' ||
          lookahead == 'u') ADVANCE(116);
      END_STATE();
    case 154:
      if (lookahead == 'U' ||
          lookahead == 'u') ADVANCE(103);
      END_STATE();
    case 155:
      if (lookahead == 'Y' ||
          lookahead == 'y') ADVANCE(177);
      END_STATE();
    case 156:
      if (lookahead == 'Y' ||
          lookahead == 'y') ADVANCE(134);
      END_STATE();
    case 157:
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(224);
      END_STATE();
    case 158:
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'F') ||
          ('a' <= lookahead && lookahead <= 'f')) ADVANCE(274);
      END_STATE();
    case 159:
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'F') ||
          ('a' <= lookahead && lookahead <= 'f')) ADVANCE(158);
      END_STATE();
    case 160:
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'F') ||
          ('a' <= lookahead && lookahead <= 'f')) ADVANCE(159);
      END_STATE();
    case 161:
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'F') ||
          ('a' <= lookahead && lookahead <= 'f')) ADVANCE(160);
      END_STATE();
    case 162:
      if (lookahead != 0 &&
          (lookahead < '\t' || '\r' < lookahead) &&
          lookahead != ' ' &&
          lookahead != '$' &&
          lookahead != '-' &&
          lookahead != '<') ADVANCE(211);
      END_STATE();
    case 163:
      if (lookahead != 0 &&
          lookahead != '<') ADVANCE(206);
      END_STATE();
    case 164:
      if (eof) ADVANCE(167);
      ADVANCE_MAP(
        '"', 270,
        '#', 205,
        '$', 212,
        '\'', 281,
        ',', 268,
        '-', 260,
        '<', 228,
        '=', 203,
        '[', 267,
        '\\', 278,
        ']', 269,
        ':', 203,
        '@', 203,
      );
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') SKIP(164);
      if (lookahead != 0) ADVANCE(203);
      END_STATE();
    case 165:
      if (eof) ADVANCE(167);
      ADVANCE_MAP(
        '"', 270,
        '#', 293,
        '$', 212,
        ',', 268,
        '-', 58,
        '=', 190,
        'N', 61,
        '[', 267,
        '\\', 3,
        ']', 269,
        '}', 219,
        'A', 78,
        'a', 78,
        'C', 115,
        'c', 115,
        'E', 117,
        'e', 117,
        'F', 140,
        'f', 140,
        'H', 87,
        'h', 87,
        'L', 68,
        'l', 68,
        'M', 69,
        'm', 69,
        'O', 119,
        'o', 119,
        'R', 151,
        'r', 151,
        'S', 95,
        's', 95,
        'U', 143,
        'u', 143,
        'V', 124,
        'v', 124,
        'W', 126,
        'w', 126,
      );
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') SKIP(166);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(223);
      END_STATE();
    case 166:
      if (eof) ADVANCE(167);
      ADVANCE_MAP(
        '"', 270,
        '#', 293,
        '$', 212,
        ',', 268,
        '-', 58,
        'N', 61,
        '[', 267,
        '\\', 3,
        ']', 269,
        'A', 78,
        'a', 78,
        'C', 115,
        'c', 115,
        'E', 117,
        'e', 117,
        'F', 140,
        'f', 140,
        'H', 87,
        'h', 87,
        'L', 68,
        'l', 68,
        'M', 69,
        'm', 69,
        'O', 119,
        'o', 119,
        'R', 151,
        'r', 151,
        'S', 95,
        's', 95,
        'U', 143,
        'u', 143,
        'V', 124,
        'v', 124,
        'W', 126,
        'w', 126,
      );
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') SKIP(166);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(223);
      END_STATE();
    case 167:
      ACCEPT_TOKEN(ts_builtin_sym_end);
      END_STATE();
    case 168:
      ACCEPT_TOKEN(anon_sym_LF);
      if (lookahead == '\n') ADVANCE(168);
      END_STATE();
    case 169:
      ACCEPT_TOKEN(aux_sym_from_instruction_token1);
      END_STATE();
    case 170:
      ACCEPT_TOKEN(aux_sym_from_instruction_token2);
      END_STATE();
    case 171:
      ACCEPT_TOKEN(aux_sym_run_instruction_token1);
      END_STATE();
    case 172:
      ACCEPT_TOKEN(aux_sym_cmd_instruction_token1);
      END_STATE();
    case 173:
      ACCEPT_TOKEN(aux_sym_label_instruction_token1);
      END_STATE();
    case 174:
      ACCEPT_TOKEN(aux_sym_expose_instruction_token1);
      END_STATE();
    case 175:
      ACCEPT_TOKEN(aux_sym_env_instruction_token1);
      END_STATE();
    case 176:
      ACCEPT_TOKEN(aux_sym_add_instruction_token1);
      END_STATE();
    case 177:
      ACCEPT_TOKEN(aux_sym_copy_instruction_token1);
      END_STATE();
    case 178:
      ACCEPT_TOKEN(aux_sym_entrypoint_instruction_token1);
      END_STATE();
    case 179:
      ACCEPT_TOKEN(aux_sym_volume_instruction_token1);
      END_STATE();
    case 180:
      ACCEPT_TOKEN(aux_sym_user_instruction_token1);
      END_STATE();
    case 181:
      ACCEPT_TOKEN(anon_sym_COLON);
      END_STATE();
    case 182:
      ACCEPT_TOKEN(aux_sym__user_name_or_group_token1);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(182);
      END_STATE();
    case 183:
      ACCEPT_TOKEN(aux_sym__user_name_or_group_token1);
      if (lookahead == '-' ||
          ('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(183);
      END_STATE();
    case 184:
      ACCEPT_TOKEN(aux_sym__immediate_user_name_or_group_fragment_token1);
      if (lookahead == '-') ADVANCE(186);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(184);
      END_STATE();
    case 185:
      ACCEPT_TOKEN(aux_sym__immediate_user_name_or_group_fragment_token1);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(185);
      END_STATE();
    case 186:
      ACCEPT_TOKEN(aux_sym__immediate_user_name_or_group_fragment_token1);
      if (lookahead == '-' ||
          ('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(186);
      END_STATE();
    case 187:
      ACCEPT_TOKEN(aux_sym_workdir_instruction_token1);
      END_STATE();
    case 188:
      ACCEPT_TOKEN(aux_sym_arg_instruction_token1);
      END_STATE();
    case 189:
      ACCEPT_TOKEN(aux_sym_arg_instruction_token2);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(189);
      END_STATE();
    case 190:
      ACCEPT_TOKEN(anon_sym_EQ);
      END_STATE();
    case 191:
      ACCEPT_TOKEN(aux_sym_onbuild_instruction_token1);
      END_STATE();
    case 192:
      ACCEPT_TOKEN(aux_sym_stopsignal_instruction_token1);
      END_STATE();
    case 193:
      ACCEPT_TOKEN(aux_sym__stopsignal_value_token1);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z')) ADVANCE(193);
      END_STATE();
    case 194:
      ACCEPT_TOKEN(aux_sym__stopsignal_value_token2);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z')) ADVANCE(194);
      END_STATE();
    case 195:
      ACCEPT_TOKEN(aux_sym_healthcheck_instruction_token1);
      END_STATE();
    case 196:
      ACCEPT_TOKEN(anon_sym_NONE);
      END_STATE();
    case 197:
      ACCEPT_TOKEN(aux_sym_shell_instruction_token1);
      END_STATE();
    case 198:
      ACCEPT_TOKEN(aux_sym_maintainer_instruction_token1);
      END_STATE();
    case 199:
      ACCEPT_TOKEN(aux_sym_maintainer_instruction_token2);
      if (lookahead == '\n') ADVANCE(265);
      if (lookahead == '\t' ||
          lookahead == ' ') ADVANCE(199);
      if (lookahead != 0) ADVANCE(201);
      END_STATE();
    case 200:
      ACCEPT_TOKEN(aux_sym_maintainer_instruction_token2);
      if (lookahead == '#') ADVANCE(201);
      if (lookahead == '\\') ADVANCE(199);
      if (lookahead == '\t' ||
          (0x0b <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') ADVANCE(200);
      if (lookahead != 0 &&
          (lookahead < '\t' || '\r' < lookahead)) ADVANCE(201);
      END_STATE();
    case 201:
      ACCEPT_TOKEN(aux_sym_maintainer_instruction_token2);
      if (lookahead != 0 &&
          lookahead != '\n') ADVANCE(201);
      END_STATE();
    case 202:
      ACCEPT_TOKEN(aux_sym_cross_build_instruction_token1);
      if (('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(202);
      END_STATE();
    case 203:
      ACCEPT_TOKEN(aux_sym_path_token1);
      END_STATE();
    case 204:
      ACCEPT_TOKEN(aux_sym_path_token1);
      if (lookahead == '\n') ADVANCE(265);
      if (lookahead == '\t' ||
          lookahead == ' ') ADVANCE(3);
      END_STATE();
    case 205:
      ACCEPT_TOKEN(aux_sym_path_token1);
      if (lookahead != 0 &&
          lookahead != '\n') ADVANCE(293);
      END_STATE();
    case 206:
      ACCEPT_TOKEN(aux_sym_path_token2);
      END_STATE();
    case 207:
      ACCEPT_TOKEN(aux_sym_path_token2);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != '<' &&
          lookahead != '\\') ADVANCE(262);
      END_STATE();
    case 208:
      ACCEPT_TOKEN(aux_sym_path_token3);
      if (lookahead == '\n') ADVANCE(265);
      if (lookahead == '\t' ||
          lookahead == ' ') ADVANCE(3);
      if (lookahead != 0 &&
          (lookahead < '\t' || '\r' < lookahead) &&
          lookahead != '$') ADVANCE(210);
      END_STATE();
    case 209:
      ACCEPT_TOKEN(aux_sym_path_token3);
      if (lookahead == '\t' ||
          (0x0b <= lookahead && lookahead <= '\r') ||
          lookahead == ' ' ||
          lookahead == '$') ADVANCE(293);
      if (lookahead != 0 &&
          (lookahead < '\t' || '\r' < lookahead)) ADVANCE(209);
      END_STATE();
    case 210:
      ACCEPT_TOKEN(aux_sym_path_token3);
      if (lookahead != 0 &&
          (lookahead < '\t' || '\r' < lookahead) &&
          lookahead != ' ' &&
          lookahead != '$') ADVANCE(210);
      END_STATE();
    case 211:
      ACCEPT_TOKEN(aux_sym_path_with_heredoc_token1);
      END_STATE();
    case 212:
      ACCEPT_TOKEN(anon_sym_DOLLAR);
      END_STATE();
    case 213:
      ACCEPT_TOKEN(anon_sym_DOLLAR2);
      END_STATE();
    case 214:
      ACCEPT_TOKEN(anon_sym_LBRACE);
      END_STATE();
    case 215:
      ACCEPT_TOKEN(aux_sym__expansion_body_token1);
      if (lookahead == '\n') ADVANCE(218);
      if (lookahead == '}') ADVANCE(293);
      if (lookahead != 0) ADVANCE(215);
      END_STATE();
    case 216:
      ACCEPT_TOKEN(aux_sym__expansion_body_token1);
      if (lookahead == '\n') ADVANCE(218);
      if (lookahead == '\t' ||
          lookahead == ' ') ADVANCE(216);
      if (lookahead != 0 &&
          lookahead != '}') ADVANCE(218);
      END_STATE();
    case 217:
      ACCEPT_TOKEN(aux_sym__expansion_body_token1);
      if (lookahead == '#') ADVANCE(215);
      if (lookahead == '\\') ADVANCE(216);
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') ADVANCE(217);
      if (lookahead != 0 &&
          lookahead != '}') ADVANCE(218);
      END_STATE();
    case 218:
      ACCEPT_TOKEN(aux_sym__expansion_body_token1);
      if (lookahead != 0 &&
          lookahead != '}') ADVANCE(218);
      END_STATE();
    case 219:
      ACCEPT_TOKEN(anon_sym_RBRACE);
      END_STATE();
    case 220:
      ACCEPT_TOKEN(sym_variable);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(220);
      END_STATE();
    case 221:
      ACCEPT_TOKEN(aux_sym__spaced_env_pair_token1);
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') ADVANCE(221);
      END_STATE();
    case 222:
      ACCEPT_TOKEN(aux_sym__env_key_token1);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(222);
      END_STATE();
    case 223:
      ACCEPT_TOKEN(aux_sym_expose_port_token1);
      if (lookahead == '-') ADVANCE(157);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(223);
      END_STATE();
    case 224:
      ACCEPT_TOKEN(aux_sym_expose_port_token1);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(224);
      END_STATE();
    case 225:
      ACCEPT_TOKEN(anon_sym_SLASHtcp);
      END_STATE();
    case 226:
      ACCEPT_TOKEN(anon_sym_SLASHudp);
      END_STATE();
    case 227:
      ACCEPT_TOKEN(aux_sym_label_pair_token1);
      if (lookahead == '-' ||
          lookahead == '.' ||
          ('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(227);
      END_STATE();
    case 228:
      ACCEPT_TOKEN(aux_sym_image_name_token1);
      END_STATE();
    case 229:
      ACCEPT_TOKEN(aux_sym_image_name_token1);
      if (lookahead == '\n') ADVANCE(265);
      if (lookahead == '\t' ||
          lookahead == ' ') ADVANCE(3);
      END_STATE();
    case 230:
      ACCEPT_TOKEN(aux_sym_image_name_token1);
      if (lookahead != 0 &&
          lookahead != '\n') ADVANCE(293);
      END_STATE();
    case 231:
      ACCEPT_TOKEN(aux_sym_image_name_token2);
      if (lookahead == '\n') ADVANCE(265);
      if (lookahead == '\t' ||
          lookahead == ' ') ADVANCE(3);
      if (lookahead != 0 &&
          (lookahead < '\t' || '\r' < lookahead) &&
          lookahead != '$' &&
          lookahead != ':' &&
          lookahead != '@') ADVANCE(234);
      END_STATE();
    case 232:
      ACCEPT_TOKEN(aux_sym_image_name_token2);
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(234);
      if (lookahead != 0 &&
          (lookahead < '\t' || '\r' < lookahead) &&
          lookahead != ' ' &&
          lookahead != '$' &&
          lookahead != ':' &&
          lookahead != '@') ADVANCE(234);
      END_STATE();
    case 233:
      ACCEPT_TOKEN(aux_sym_image_name_token2);
      if (lookahead == '\t' ||
          (0x0b <= lookahead && lookahead <= '\r') ||
          lookahead == ' ' ||
          lookahead == '$' ||
          lookahead == ':' ||
          lookahead == '@') ADVANCE(293);
      if (lookahead != 0 &&
          (lookahead < '\t' || '\r' < lookahead)) ADVANCE(233);
      END_STATE();
    case 234:
      ACCEPT_TOKEN(aux_sym_image_name_token2);
      if (lookahead != 0 &&
          (lookahead < '\t' || '\r' < lookahead) &&
          lookahead != ' ' &&
          lookahead != '$' &&
          lookahead != ':' &&
          lookahead != '@') ADVANCE(234);
      END_STATE();
    case 235:
      ACCEPT_TOKEN(aux_sym_image_tag_token1);
      if (lookahead == '\n') ADVANCE(265);
      if (lookahead == '\t' ||
          lookahead == ' ') ADVANCE(3);
      if (lookahead != 0 &&
          (lookahead < '\t' || '\r' < lookahead) &&
          lookahead != '$' &&
          lookahead != '@') ADVANCE(238);
      END_STATE();
    case 236:
      ACCEPT_TOKEN(aux_sym_image_tag_token1);
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(238);
      if (lookahead != 0 &&
          (lookahead < '\t' || '\r' < lookahead) &&
          lookahead != ' ' &&
          lookahead != '$' &&
          lookahead != '@') ADVANCE(238);
      END_STATE();
    case 237:
      ACCEPT_TOKEN(aux_sym_image_tag_token1);
      if (lookahead == '\t' ||
          (0x0b <= lookahead && lookahead <= '\r') ||
          lookahead == ' ' ||
          lookahead == '$' ||
          lookahead == '@') ADVANCE(293);
      if (lookahead != 0 &&
          (lookahead < '\t' || '\r' < lookahead)) ADVANCE(237);
      END_STATE();
    case 238:
      ACCEPT_TOKEN(aux_sym_image_tag_token1);
      if (lookahead != 0 &&
          (lookahead < '\t' || '\r' < lookahead) &&
          lookahead != ' ' &&
          lookahead != '$' &&
          lookahead != '@') ADVANCE(238);
      END_STATE();
    case 239:
      ACCEPT_TOKEN(anon_sym_AT);
      END_STATE();
    case 240:
      ACCEPT_TOKEN(aux_sym_image_digest_token1);
      if (lookahead == 'S' ||
          lookahead == 's') ADVANCE(241);
      if (('0' <= lookahead && lookahead <= ':') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(241);
      END_STATE();
    case 241:
      ACCEPT_TOKEN(aux_sym_image_digest_token1);
      if (('0' <= lookahead && lookahead <= ':') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(241);
      END_STATE();
    case 242:
      ACCEPT_TOKEN(anon_sym_DASH_DASH);
      END_STATE();
    case 243:
      ACCEPT_TOKEN(aux_sym_param_token1);
      if (lookahead == 'n') ADVANCE(245);
      if (lookahead == '-' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(247);
      END_STATE();
    case 244:
      ACCEPT_TOKEN(aux_sym_param_token1);
      if (lookahead == 'o') ADVANCE(246);
      if (lookahead == '-' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(247);
      END_STATE();
    case 245:
      ACCEPT_TOKEN(aux_sym_param_token1);
      if (lookahead == 't') ADVANCE(251);
      if (lookahead == '-' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(247);
      END_STATE();
    case 246:
      ACCEPT_TOKEN(aux_sym_param_token1);
      if (lookahead == 'u') ADVANCE(243);
      if (lookahead == '-' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(247);
      END_STATE();
    case 247:
      ACCEPT_TOKEN(aux_sym_param_token1);
      if (lookahead == '-' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(247);
      END_STATE();
    case 248:
      ACCEPT_TOKEN(aux_sym_param_token2);
      if (lookahead == '\n') ADVANCE(265);
      if (lookahead == '\t' ||
          lookahead == ' ') ADVANCE(3);
      if (lookahead != 0 &&
          (lookahead < '\t' || '\r' < lookahead)) ADVANCE(250);
      END_STATE();
    case 249:
      ACCEPT_TOKEN(aux_sym_param_token2);
      if (lookahead == '\t' ||
          (0x0b <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') ADVANCE(293);
      if (lookahead != 0 &&
          (lookahead < '\t' || '\r' < lookahead)) ADVANCE(249);
      END_STATE();
    case 250:
      ACCEPT_TOKEN(aux_sym_param_token2);
      if (lookahead != 0 &&
          (lookahead < '\t' || '\r' < lookahead) &&
          lookahead != ' ') ADVANCE(250);
      END_STATE();
    case 251:
      ACCEPT_TOKEN(anon_sym_mount);
      if (lookahead == '-' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(247);
      END_STATE();
    case 252:
      ACCEPT_TOKEN(anon_sym_COMMA);
      END_STATE();
    case 253:
      ACCEPT_TOKEN(aux_sym_mount_param_param_token1);
      if (lookahead == '\n') ADVANCE(265);
      if (lookahead == '\t' ||
          lookahead == ' ') ADVANCE(3);
      if (lookahead != 0 &&
          (lookahead < '\t' || '\r' < lookahead) &&
          lookahead != ',' &&
          lookahead != '=') ADVANCE(255);
      END_STATE();
    case 254:
      ACCEPT_TOKEN(aux_sym_mount_param_param_token1);
      if (lookahead == '\t' ||
          (0x0b <= lookahead && lookahead <= '\r') ||
          lookahead == ' ' ||
          lookahead == ',' ||
          lookahead == '=') ADVANCE(293);
      if (lookahead != 0 &&
          (lookahead < '\t' || '\r' < lookahead)) ADVANCE(254);
      END_STATE();
    case 255:
      ACCEPT_TOKEN(aux_sym_mount_param_param_token1);
      if (lookahead != 0 &&
          (lookahead < '\t' || '\r' < lookahead) &&
          lookahead != ' ' &&
          lookahead != ',' &&
          lookahead != '=') ADVANCE(255);
      END_STATE();
    case 256:
      ACCEPT_TOKEN(aux_sym_image_alias_token1);
      if (lookahead == '-' ||
          ('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(256);
      END_STATE();
    case 257:
      ACCEPT_TOKEN(aux_sym_image_alias_token2);
      if (lookahead == '-' ||
          ('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(257);
      END_STATE();
    case 258:
      ACCEPT_TOKEN(aux_sym_shell_fragment_token1);
      if (lookahead == '\t' ||
          lookahead == ' ') ADVANCE(258);
      END_STATE();
    case 259:
      ACCEPT_TOKEN(aux_sym_shell_fragment_token2);
      END_STATE();
    case 260:
      ACCEPT_TOKEN(aux_sym_shell_fragment_token2);
      if (lookahead == '-') ADVANCE(242);
      END_STATE();
    case 261:
      ACCEPT_TOKEN(aux_sym_shell_fragment_token3);
      if (lookahead == '\n' ||
          lookahead == '\\') ADVANCE(206);
      if (lookahead != 0 &&
          lookahead != '<') ADVANCE(207);
      END_STATE();
    case 262:
      ACCEPT_TOKEN(aux_sym_shell_fragment_token3);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != '<' &&
          lookahead != '\\') ADVANCE(262);
      END_STATE();
    case 263:
      ACCEPT_TOKEN(aux_sym_shell_fragment_token4);
      END_STATE();
    case 264:
      ACCEPT_TOKEN(aux_sym_shell_fragment_token4);
      if (lookahead == '\n') ADVANCE(265);
      if (lookahead == '\t' ||
          lookahead == ' ') ADVANCE(3);
      END_STATE();
    case 265:
      ACCEPT_TOKEN(sym_line_continuation);
      END_STATE();
    case 266:
      ACCEPT_TOKEN(sym_required_line_continuation);
      END_STATE();
    case 267:
      ACCEPT_TOKEN(anon_sym_LBRACK);
      END_STATE();
    case 268:
      ACCEPT_TOKEN(anon_sym_COMMA2);
      END_STATE();
    case 269:
      ACCEPT_TOKEN(anon_sym_RBRACK);
      END_STATE();
    case 270:
      ACCEPT_TOKEN(anon_sym_DQUOTE);
      END_STATE();
    case 271:
      ACCEPT_TOKEN(aux_sym_json_string_token1);
      if (lookahead == '\n') ADVANCE(273);
      if (lookahead == '"' ||
          lookahead == '\\') ADVANCE(293);
      if (lookahead != 0) ADVANCE(271);
      END_STATE();
    case 272:
      ACCEPT_TOKEN(aux_sym_json_string_token1);
      if (lookahead == '#') ADVANCE(271);
      if (('\t' <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') ADVANCE(272);
      if (lookahead != 0 &&
          lookahead != '"' &&
          lookahead != '#' &&
          lookahead != '\\') ADVANCE(273);
      END_STATE();
    case 273:
      ACCEPT_TOKEN(aux_sym_json_string_token1);
      if (lookahead != 0 &&
          lookahead != '"' &&
          lookahead != '\\') ADVANCE(273);
      END_STATE();
    case 274:
      ACCEPT_TOKEN(sym_json_escape_sequence);
      END_STATE();
    case 275:
      ACCEPT_TOKEN(aux_sym_double_quoted_string_token1);
      if (lookahead == '#') ADVANCE(276);
      if (lookahead == '\t' ||
          (0x0b <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') ADVANCE(275);
      if (lookahead != 0 &&
          (lookahead < '\t' || '\r' < lookahead) &&
          (lookahead < '"' || '$' < lookahead) &&
          lookahead != '\\') ADVANCE(277);
      END_STATE();
    case 276:
      ACCEPT_TOKEN(aux_sym_double_quoted_string_token1);
      if (lookahead == '"' ||
          lookahead == '$' ||
          lookahead == '\\') ADVANCE(293);
      if (lookahead != 0 &&
          lookahead != '\n') ADVANCE(276);
      END_STATE();
    case 277:
      ACCEPT_TOKEN(aux_sym_double_quoted_string_token1);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != '"' &&
          lookahead != '$' &&
          lookahead != '\\') ADVANCE(277);
      END_STATE();
    case 278:
      ACCEPT_TOKEN(anon_sym_BSLASH);
      if (lookahead == '\n') ADVANCE(265);
      if (lookahead == '\t' ||
          lookahead == ' ') ADVANCE(3);
      END_STATE();
    case 279:
      ACCEPT_TOKEN(anon_sym_BSLASH);
      if (lookahead == '\n') ADVANCE(265);
      if (lookahead == '\t' ||
          lookahead == ' ') ADVANCE(3);
      if (lookahead == '"' ||
          lookahead == '\\') ADVANCE(290);
      END_STATE();
    case 280:
      ACCEPT_TOKEN(anon_sym_BSLASH);
      if (lookahead == '\n') ADVANCE(265);
      if (lookahead == '\t' ||
          lookahead == ' ') ADVANCE(3);
      if (lookahead == '\'' ||
          lookahead == '\\') ADVANCE(291);
      END_STATE();
    case 281:
      ACCEPT_TOKEN(anon_sym_SQUOTE);
      END_STATE();
    case 282:
      ACCEPT_TOKEN(aux_sym_single_quoted_string_token1);
      if (lookahead == '#') ADVANCE(283);
      if (lookahead == '\t' ||
          (0x0b <= lookahead && lookahead <= '\r') ||
          lookahead == ' ') ADVANCE(282);
      if (lookahead != 0 &&
          (lookahead < '\t' || '\r' < lookahead) &&
          lookahead != '\'' &&
          lookahead != '\\') ADVANCE(284);
      END_STATE();
    case 283:
      ACCEPT_TOKEN(aux_sym_single_quoted_string_token1);
      if (lookahead == '\'' ||
          lookahead == '\\') ADVANCE(293);
      if (lookahead != 0 &&
          lookahead != '\n') ADVANCE(283);
      END_STATE();
    case 284:
      ACCEPT_TOKEN(aux_sym_single_quoted_string_token1);
      if (lookahead != 0 &&
          lookahead != '\n' &&
          lookahead != '\'' &&
          lookahead != '\\') ADVANCE(284);
      END_STATE();
    case 285:
      ACCEPT_TOKEN(aux_sym_unquoted_string_token1);
      if (lookahead == '\t' ||
          (0x0b <= lookahead && lookahead <= '\r') ||
          lookahead == ' ' ||
          lookahead == '"' ||
          lookahead == '$' ||
          lookahead == '\'' ||
          lookahead == '\\') ADVANCE(293);
      if (lookahead != 0 &&
          (lookahead < '\t' || '\r' < lookahead)) ADVANCE(285);
      END_STATE();
    case 286:
      ACCEPT_TOKEN(aux_sym_unquoted_string_token1);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(286);
      if (lookahead != 0 &&
          (lookahead < '\t' || '\r' < lookahead) &&
          lookahead != ' ' &&
          lookahead != '"' &&
          lookahead != '$' &&
          lookahead != '\'' &&
          lookahead != '\\') ADVANCE(288);
      END_STATE();
    case 287:
      ACCEPT_TOKEN(aux_sym_unquoted_string_token1);
      if (lookahead == '-' ||
          lookahead == '.' ||
          ('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(287);
      if (lookahead != 0 &&
          (lookahead < '\t' || '\r' < lookahead) &&
          lookahead != ' ' &&
          lookahead != '"' &&
          lookahead != '$' &&
          lookahead != '\'' &&
          lookahead != '\\') ADVANCE(288);
      END_STATE();
    case 288:
      ACCEPT_TOKEN(aux_sym_unquoted_string_token1);
      if (lookahead != 0 &&
          (lookahead < '\t' || '\r' < lookahead) &&
          lookahead != ' ' &&
          lookahead != '"' &&
          lookahead != '$' &&
          lookahead != '\'' &&
          lookahead != '\\') ADVANCE(288);
      END_STATE();
    case 289:
      ACCEPT_TOKEN(anon_sym_BSLASH2);
      if (lookahead == '\n') ADVANCE(265);
      if (lookahead == '\t' ||
          lookahead == ' ') ADVANCE(3);
      END_STATE();
    case 290:
      ACCEPT_TOKEN(sym_double_quoted_escape_sequence);
      END_STATE();
    case 291:
      ACCEPT_TOKEN(sym_single_quoted_escape_sequence);
      END_STATE();
    case 292:
      ACCEPT_TOKEN(sym__non_newline_whitespace);
      if (lookahead == '\t' ||
          lookahead == ' ') ADVANCE(292);
      END_STATE();
    case 293:
      ACCEPT_TOKEN(sym_comment);
      if (lookahead != 0 &&
          lookahead != '\n') ADVANCE(293);
      END_STATE();
    default:
      return false;
  }
}

static const TSLexMode ts_lex_modes[STATE_COUNT] = {
  [0] = {.lex_state = 0, .external_lex_state = 1},
  [1] = {.lex_state = 165},
  [2] = {.lex_state = 165},
  [3] = {.lex_state = 165},
  [4] = {.lex_state = 165},
  [5] = {.lex_state = 165},
  [6] = {.lex_state = 43, .external_lex_state = 2},
  [7] = {.lex_state = 43, .external_lex_state = 2},
  [8] = {.lex_state = 7},
  [9] = {.lex_state = 30},
  [10] = {.lex_state = 30},
  [11] = {.lex_state = 30},
  [12] = {.lex_state = 44, .external_lex_state = 2},
  [13] = {.lex_state = 32, .external_lex_state = 2},
  [14] = {.lex_state = 32, .external_lex_state = 2},
  [15] = {.lex_state = 32, .external_lex_state = 2},
  [16] = {.lex_state = 44, .external_lex_state = 2},
  [17] = {.lex_state = 32, .external_lex_state = 2},
  [18] = {.lex_state = 8},
  [19] = {.lex_state = 43, .external_lex_state = 2},
  [20] = {.lex_state = 8},
  [21] = {.lex_state = 20},
  [22] = {.lex_state = 15, .external_lex_state = 3},
  [23] = {.lex_state = 15, .external_lex_state = 3},
  [24] = {.lex_state = 20},
  [25] = {.lex_state = 20},
  [26] = {.lex_state = 42, .external_lex_state = 2},
  [27] = {.lex_state = 42, .external_lex_state = 2},
  [28] = {.lex_state = 42, .external_lex_state = 2},
  [29] = {.lex_state = 21},
  [30] = {.lex_state = 25},
  [31] = {.lex_state = 9},
  [32] = {.lex_state = 15, .external_lex_state = 3},
  [33] = {.lex_state = 25},
  [34] = {.lex_state = 6},
  [35] = {.lex_state = 9},
  [36] = {.lex_state = 25},
  [37] = {.lex_state = 25},
  [38] = {.lex_state = 9},
  [39] = {.lex_state = 21},
  [40] = {.lex_state = 6},
  [41] = {.lex_state = 42, .external_lex_state = 2},
  [42] = {.lex_state = 42, .external_lex_state = 2},
  [43] = {.lex_state = 15, .external_lex_state = 2},
  [44] = {.lex_state = 22, .external_lex_state = 4},
  [45] = {.lex_state = 15, .external_lex_state = 2},
  [46] = {.lex_state = 22, .external_lex_state = 4},
  [47] = {.lex_state = 7},
  [48] = {.lex_state = 7},
  [49] = {.lex_state = 22, .external_lex_state = 4},
  [50] = {.lex_state = 25},
  [51] = {.lex_state = 25},
  [52] = {.lex_state = 25},
  [53] = {.lex_state = 9},
  [54] = {.lex_state = 33},
  [55] = {.lex_state = 10},
  [56] = {.lex_state = 32, .external_lex_state = 2},
  [57] = {.lex_state = 9},
  [58] = {.lex_state = 44, .external_lex_state = 2},
  [59] = {.lex_state = 10},
  [60] = {.lex_state = 22},
  [61] = {.lex_state = 28},
  [62] = {.lex_state = 15, .external_lex_state = 2},
  [63] = {.lex_state = 44, .external_lex_state = 2},
  [64] = {.lex_state = 32, .external_lex_state = 2},
  [65] = {.lex_state = 32, .external_lex_state = 2},
  [66] = {.lex_state = 32, .external_lex_state = 2},
  [67] = {.lex_state = 22},
  [68] = {.lex_state = 43, .external_lex_state = 2},
  [69] = {.lex_state = 32, .external_lex_state = 2},
  [70] = {.lex_state = 22},
  [71] = {.lex_state = 54},
  [72] = {.lex_state = 8},
  [73] = {.lex_state = 8},
  [74] = {.lex_state = 32, .external_lex_state = 2},
  [75] = {.lex_state = 20},
  [76] = {.lex_state = 23},
  [77] = {.lex_state = 11},
  [78] = {.lex_state = 20},
  [79] = {.lex_state = 12},
  [80] = {.lex_state = 20},
  [81] = {.lex_state = 23},
  [82] = {.lex_state = 53},
  [83] = {.lex_state = 13},
  [84] = {.lex_state = 53},
  [85] = {.lex_state = 12},
  [86] = {.lex_state = 23},
  [87] = {.lex_state = 13},
  [88] = {.lex_state = 165},
  [89] = {.lex_state = 12},
  [90] = {.lex_state = 13},
  [91] = {.lex_state = 11},
  [92] = {.lex_state = 35},
  [93] = {.lex_state = 53},
  [94] = {.lex_state = 11},
  [95] = {.lex_state = 21},
  [96] = {.lex_state = 19},
  [97] = {.lex_state = 165},
  [98] = {.lex_state = 16, .external_lex_state = 4},
  [99] = {.lex_state = 13},
  [100] = {.lex_state = 16, .external_lex_state = 4},
  [101] = {.lex_state = 34},
  [102] = {.lex_state = 50},
  [103] = {.lex_state = 19},
  [104] = {.lex_state = 165},
  [105] = {.lex_state = 54},
  [106] = {.lex_state = 26},
  [107] = {.lex_state = 52},
  [108] = {.lex_state = 36},
  [109] = {.lex_state = 25},
  [110] = {.lex_state = 34},
  [111] = {.lex_state = 32, .external_lex_state = 2},
  [112] = {.lex_state = 26},
  [113] = {.lex_state = 22, .external_lex_state = 4},
  [114] = {.lex_state = 25},
  [115] = {.lex_state = 22, .external_lex_state = 4},
  [116] = {.lex_state = 26},
  [117] = {.lex_state = 7},
  [118] = {.lex_state = 21},
  [119] = {.lex_state = 22, .external_lex_state = 4},
  [120] = {.lex_state = 7},
  [121] = {.lex_state = 16, .external_lex_state = 4},
  [122] = {.lex_state = 26},
  [123] = {.lex_state = 26},
  [124] = {.lex_state = 16, .external_lex_state = 4},
  [125] = {.lex_state = 26},
  [126] = {.lex_state = 26},
  [127] = {.lex_state = 6},
  [128] = {.lex_state = 9, .external_lex_state = 4},
  [129] = {.lex_state = 15, .external_lex_state = 4},
  [130] = {.lex_state = 9, .external_lex_state = 4},
  [131] = {.lex_state = 9, .external_lex_state = 4},
  [132] = {.lex_state = 37},
  [133] = {.lex_state = 22},
  [134] = {.lex_state = 22},
  [135] = {.lex_state = 31},
  [136] = {.lex_state = 22},
  [137] = {.lex_state = 9},
  [138] = {.lex_state = 9},
  [139] = {.lex_state = 9},
  [140] = {.lex_state = 31},
  [141] = {.lex_state = 9, .external_lex_state = 4},
  [142] = {.lex_state = 9, .external_lex_state = 4},
  [143] = {.lex_state = 13},
  [144] = {.lex_state = 39},
  [145] = {.lex_state = 40},
  [146] = {.lex_state = 9, .external_lex_state = 4},
  [147] = {.lex_state = 32, .external_lex_state = 2},
  [148] = {.lex_state = 6},
  [149] = {.lex_state = 10},
  [150] = {.lex_state = 15, .external_lex_state = 4},
  [151] = {.lex_state = 6},
  [152] = {.lex_state = 15, .external_lex_state = 4},
  [153] = {.lex_state = 165},
  [154] = {.lex_state = 9, .external_lex_state = 4},
  [155] = {.lex_state = 39},
  [156] = {.lex_state = 9, .external_lex_state = 4},
  [157] = {.lex_state = 31},
  [158] = {.lex_state = 9, .external_lex_state = 4},
  [159] = {.lex_state = 6},
  [160] = {.lex_state = 6},
  [161] = {.lex_state = 10},
  [162] = {.lex_state = 46},
  [163] = {.lex_state = 13},
  [164] = {.lex_state = 9},
  [165] = {.lex_state = 165},
  [166] = {.lex_state = 165, .external_lex_state = 5},
  [167] = {.lex_state = 15},
  [168] = {.lex_state = 16, .external_lex_state = 4},
  [169] = {.lex_state = 16},
  [170] = {.lex_state = 13},
  [171] = {.lex_state = 46},
  [172] = {.lex_state = 46},
  [173] = {.lex_state = 15},
  [174] = {.lex_state = 15},
  [175] = {.lex_state = 13},
  [176] = {.lex_state = 53},
  [177] = {.lex_state = 53},
  [178] = {.lex_state = 53},
  [179] = {.lex_state = 165},
  [180] = {.lex_state = 165, .external_lex_state = 5},
  [181] = {.lex_state = 23},
  [182] = {.lex_state = 23},
  [183] = {.lex_state = 23},
  [184] = {.lex_state = 11},
  [185] = {.lex_state = 11},
  [186] = {.lex_state = 46},
  [187] = {.lex_state = 15, .external_lex_state = 4},
  [188] = {.lex_state = 12},
  [189] = {.lex_state = 165},
  [190] = {.lex_state = 165, .external_lex_state = 5},
  [191] = {.lex_state = 165},
  [192] = {.lex_state = 12},
  [193] = {.lex_state = 12},
  [194] = {.lex_state = 46},
  [195] = {.lex_state = 165},
  [196] = {.lex_state = 165},
  [197] = {.lex_state = 46},
  [198] = {.lex_state = 16},
  [199] = {.lex_state = 165},
  [200] = {.lex_state = 46},
  [201] = {.lex_state = 46},
  [202] = {.lex_state = 16},
  [203] = {.lex_state = 46},
  [204] = {.lex_state = 46},
  [205] = {.lex_state = 46},
  [206] = {.lex_state = 46},
  [207] = {.lex_state = 46},
  [208] = {.lex_state = 46},
  [209] = {.lex_state = 46},
  [210] = {.lex_state = 46},
  [211] = {.lex_state = 46},
  [212] = {.lex_state = 46},
  [213] = {.lex_state = 46},
  [214] = {.lex_state = 46},
  [215] = {.lex_state = 46},
  [216] = {.lex_state = 46},
  [217] = {.lex_state = 11},
  [218] = {.lex_state = 9, .external_lex_state = 4},
  [219] = {.lex_state = 13},
  [220] = {.lex_state = 37},
  [221] = {.lex_state = 54},
  [222] = {.lex_state = 19},
  [223] = {.lex_state = 19},
  [224] = {.lex_state = 165, .external_lex_state = 5},
  [225] = {.lex_state = 19},
  [226] = {.lex_state = 19},
  [227] = {.lex_state = 55},
  [228] = {.lex_state = 9, .external_lex_state = 4},
  [229] = {.lex_state = 165},
  [230] = {.lex_state = 165},
  [231] = {.lex_state = 9, .external_lex_state = 4},
  [232] = {.lex_state = 165},
  [233] = {.lex_state = 9, .external_lex_state = 4},
  [234] = {.lex_state = 165},
  [235] = {.lex_state = 55},
  [236] = {.lex_state = 9, .external_lex_state = 4},
  [237] = {.lex_state = 13},
  [238] = {.lex_state = 9},
  [239] = {.lex_state = 19},
  [240] = {.lex_state = 165},
  [241] = {.lex_state = 16},
  [242] = {.lex_state = 45},
  [243] = {.lex_state = 15},
  [244] = {.lex_state = 165},
  [245] = {.lex_state = 13},
  [246] = {.lex_state = 13},
  [247] = {.lex_state = 9},
  [248] = {.lex_state = 56},
  [249] = {.lex_state = 165},
  [250] = {.lex_state = 9},
  [251] = {.lex_state = 56},
  [252] = {.lex_state = 165},
  [253] = {.lex_state = 165},
  [254] = {.lex_state = 165},
  [255] = {.lex_state = 9},
  [256] = {.lex_state = 9},
  [257] = {.lex_state = 9},
  [258] = {.lex_state = 9},
  [259] = {.lex_state = 200},
  [260] = {.lex_state = 258},
  [261] = {.lex_state = 9},
  [262] = {.lex_state = 51},
  [263] = {.lex_state = 9},
  [264] = {.lex_state = 165},
  [265] = {.lex_state = 9},
  [266] = {.lex_state = 165},
  [267] = {.lex_state = 165},
  [268] = {.lex_state = 57},
  [269] = {.lex_state = 9},
  [270] = {.lex_state = 200},
  [271] = {.lex_state = 165},
  [272] = {.lex_state = 165},
  [273] = {.lex_state = 9},
  [274] = {.lex_state = 165},
  [275] = {.lex_state = 9},
  [276] = {.lex_state = 165},
  [277] = {.lex_state = 57},
  [278] = {.lex_state = 9},
  [279] = {.lex_state = 165},
  [280] = {.lex_state = 165},
  [281] = {.lex_state = 57},
  [282] = {.lex_state = 9},
  [283] = {.lex_state = 165},
  [284] = {.lex_state = 165},
  [285] = {.lex_state = 55},
  [286] = {.lex_state = 258},
  [287] = {.lex_state = 165},
  [288] = {.lex_state = 9},
  [289] = {.lex_state = 9},
  [290] = {.lex_state = 165},
  [291] = {.lex_state = 9},
  [292] = {.lex_state = 9},
  [293] = {.lex_state = 165},
  [294] = {.lex_state = 47},
  [295] = {.lex_state = 9},
  [296] = {.lex_state = 165},
  [297] = {.lex_state = 9},
  [298] = {.lex_state = 165},
  [299] = {.lex_state = 47},
  [300] = {.lex_state = 165},
  [301] = {.lex_state = 9},
  [302] = {.lex_state = 165},
  [303] = {.lex_state = 49},
  [304] = {.lex_state = 165},
  [305] = {.lex_state = 165},
  [306] = {.lex_state = 56},
  [307] = {.lex_state = 165},
  [308] = {.lex_state = 56},
  [309] = {.lex_state = 165},
  [310] = {.lex_state = 56},
  [311] = {.lex_state = 165},
  [312] = {.lex_state = 56},
  [313] = {.lex_state = 9},
  [314] = {.lex_state = 56},
  [315] = {.lex_state = 56},
  [316] = {.lex_state = 56},
  [317] = {.lex_state = 56},
  [318] = {.lex_state = 56},
  [319] = {.lex_state = 56},
  [320] = {.lex_state = 56},
  [321] = {.lex_state = 56},
  [322] = {.lex_state = 49},
  [323] = {.lex_state = 49},
  [324] = {.lex_state = 57},
};

static const uint16_t ts_parse_table[LARGE_STATE_COUNT][SYMBOL_COUNT] = {
  [0] = {
    [ts_builtin_sym_end] = ACTIONS(1),
    [anon_sym_COLON] = ACTIONS(1),
    [aux_sym__immediate_user_name_or_group_fragment_token1] = ACTIONS(1),
    [anon_sym_EQ] = ACTIONS(1),
    [aux_sym_path_token1] = ACTIONS(1),
    [anon_sym_DOLLAR] = ACTIONS(1),
    [anon_sym_DOLLAR2] = ACTIONS(1),
    [anon_sym_LBRACE] = ACTIONS(1),
    [anon_sym_RBRACE] = ACTIONS(1),
    [sym_variable] = ACTIONS(1),
    [aux_sym_image_name_token1] = ACTIONS(1),
    [anon_sym_AT] = ACTIONS(1),
    [anon_sym_DASH_DASH] = ACTIONS(1),
    [anon_sym_COMMA] = ACTIONS(1),
    [aux_sym_shell_fragment_token2] = ACTIONS(1),
    [sym_line_continuation] = ACTIONS(3),
    [anon_sym_LBRACK] = ACTIONS(1),
    [anon_sym_COMMA2] = ACTIONS(1),
    [anon_sym_RBRACK] = ACTIONS(1),
    [anon_sym_DQUOTE] = ACTIONS(1),
    [anon_sym_BSLASH] = ACTIONS(1),
    [anon_sym_SQUOTE] = ACTIONS(1),
    [sym_comment] = ACTIONS(5),
    [sym_heredoc_marker] = ACTIONS(1),
    [sym_heredoc_line] = ACTIONS(1),
    [sym_heredoc_end] = ACTIONS(1),
    [sym_heredoc_nl] = ACTIONS(1),
    [sym_error_sentinel] = ACTIONS(1),
  },
  [1] = {
    [sym_source_file] = STATE(253),
    [sym__instruction] = STATE(255),
    [sym_from_instruction] = STATE(255),
    [sym_run_instruction] = STATE(255),
    [sym_cmd_instruction] = STATE(255),
    [sym_label_instruction] = STATE(255),
    [sym_expose_instruction] = STATE(255),
    [sym_env_instruction] = STATE(255),
    [sym_add_instruction] = STATE(255),
    [sym_copy_instruction] = STATE(255),
    [sym_entrypoint_instruction] = STATE(255),
    [sym_volume_instruction] = STATE(255),
    [sym_user_instruction] = STATE(255),
    [sym_workdir_instruction] = STATE(255),
    [sym_arg_instruction] = STATE(255),
    [sym_onbuild_instruction] = STATE(255),
    [sym_stopsignal_instruction] = STATE(255),
    [sym_healthcheck_instruction] = STATE(255),
    [sym_shell_instruction] = STATE(255),
    [sym_maintainer_instruction] = STATE(255),
    [sym_cross_build_instruction] = STATE(255),
    [aux_sym_source_file_repeat1] = STATE(2),
    [ts_builtin_sym_end] = ACTIONS(7),
    [aux_sym_from_instruction_token1] = ACTIONS(9),
    [aux_sym_run_instruction_token1] = ACTIONS(11),
    [aux_sym_cmd_instruction_token1] = ACTIONS(13),
    [aux_sym_label_instruction_token1] = ACTIONS(15),
    [aux_sym_expose_instruction_token1] = ACTIONS(17),
    [aux_sym_env_instruction_token1] = ACTIONS(19),
    [aux_sym_add_instruction_token1] = ACTIONS(21),
    [aux_sym_copy_instruction_token1] = ACTIONS(23),
    [aux_sym_entrypoint_instruction_token1] = ACTIONS(25),
    [aux_sym_volume_instruction_token1] = ACTIONS(27),
    [aux_sym_user_instruction_token1] = ACTIONS(29),
    [aux_sym_workdir_instruction_token1] = ACTIONS(31),
    [aux_sym_arg_instruction_token1] = ACTIONS(33),
    [aux_sym_onbuild_instruction_token1] = ACTIONS(35),
    [aux_sym_stopsignal_instruction_token1] = ACTIONS(37),
    [aux_sym_healthcheck_instruction_token1] = ACTIONS(39),
    [aux_sym_shell_instruction_token1] = ACTIONS(41),
    [aux_sym_maintainer_instruction_token1] = ACTIONS(43),
    [aux_sym_cross_build_instruction_token1] = ACTIONS(45),
    [sym_line_continuation] = ACTIONS(3),
    [sym_comment] = ACTIONS(3),
  },
};

static const uint16_t ts_small_parse_table[] = {
  [0] = 23,
    ACTIONS(9), 1,
      aux_sym_from_instruction_token1,
    ACTIONS(11), 1,
      aux_sym_run_instruction_token1,
    ACTIONS(13), 1,
      aux_sym_cmd_instruction_token1,
    ACTIONS(15), 1,
      aux_sym_label_instruction_token1,
    ACTIONS(17), 1,
      aux_sym_expose_instruction_token1,
    ACTIONS(19), 1,
      aux_sym_env_instruction_token1,
    ACTIONS(21), 1,
      aux_sym_add_instruction_token1,
    ACTIONS(23), 1,
      aux_sym_copy_instruction_token1,
    ACTIONS(25), 1,
      aux_sym_entrypoint_instruction_token1,
    ACTIONS(27), 1,
      aux_sym_volume_instruction_token1,
    ACTIONS(29), 1,
      aux_sym_user_instruction_token1,
    ACTIONS(31), 1,
      aux_sym_workdir_instruction_token1,
    ACTIONS(33), 1,
      aux_sym_arg_instruction_token1,
    ACTIONS(35), 1,
      aux_sym_onbuild_instruction_token1,
    ACTIONS(37), 1,
      aux_sym_stopsignal_instruction_token1,
    ACTIONS(39), 1,
      aux_sym_healthcheck_instruction_token1,
    ACTIONS(41), 1,
      aux_sym_shell_instruction_token1,
    ACTIONS(43), 1,
      aux_sym_maintainer_instruction_token1,
    ACTIONS(45), 1,
      aux_sym_cross_build_instruction_token1,
    ACTIONS(47), 1,
      ts_builtin_sym_end,
    STATE(3), 1,
      aux_sym_source_file_repeat1,
    ACTIONS(3), 2,
      sym_line_continuation,
      sym_comment,
    STATE(255), 20,
      sym__instruction,
      sym_from_instruction,
      sym_run_instruction,
      sym_cmd_instruction,
      sym_label_instruction,
      sym_expose_instruction,
      sym_env_instruction,
      sym_add_instruction,
      sym_copy_instruction,
      sym_entrypoint_instruction,
      sym_volume_instruction,
      sym_user_instruction,
      sym_workdir_instruction,
      sym_arg_instruction,
      sym_onbuild_instruction,
      sym_stopsignal_instruction,
      sym_healthcheck_instruction,
      sym_shell_instruction,
      sym_maintainer_instruction,
      sym_cross_build_instruction,
  [90] = 23,
    ACTIONS(49), 1,
      ts_builtin_sym_end,
    ACTIONS(51), 1,
      aux_sym_from_instruction_token1,
    ACTIONS(54), 1,
      aux_sym_run_instruction_token1,
    ACTIONS(57), 1,
      aux_sym_cmd_instruction_token1,
    ACTIONS(60), 1,
      aux_sym_label_instruction_token1,
    ACTIONS(63), 1,
      aux_sym_expose_instruction_token1,
    ACTIONS(66), 1,
      aux_sym_env_instruction_token1,
    ACTIONS(69), 1,
      aux_sym_add_instruction_token1,
    ACTIONS(72), 1,
      aux_sym_copy_instruction_token1,
    ACTIONS(75), 1,
      aux_sym_entrypoint_instruction_token1,
    ACTIONS(78), 1,
      aux_sym_volume_instruction_token1,
    ACTIONS(81), 1,
      aux_sym_user_instruction_token1,
    ACTIONS(84), 1,
      aux_sym_workdir_instruction_token1,
    ACTIONS(87), 1,
      aux_sym_arg_instruction_token1,
    ACTIONS(90), 1,
      aux_sym_onbuild_instruction_token1,
    ACTIONS(93), 1,
      aux_sym_stopsignal_instruction_token1,
    ACTIONS(96), 1,
      aux_sym_healthcheck_instruction_token1,
    ACTIONS(99), 1,
      aux_sym_shell_instruction_token1,
    ACTIONS(102), 1,
      aux_sym_maintainer_instruction_token1,
    ACTIONS(105), 1,
      aux_sym_cross_build_instruction_token1,
    STATE(3), 1,
      aux_sym_source_file_repeat1,
    ACTIONS(3), 2,
      sym_line_continuation,
      sym_comment,
    STATE(255), 20,
      sym__instruction,
      sym_from_instruction,
      sym_run_instruction,
      sym_cmd_instruction,
      sym_label_instruction,
      sym_expose_instruction,
      sym_env_instruction,
      sym_add_instruction,
      sym_copy_instruction,
      sym_entrypoint_instruction,
      sym_volume_instruction,
      sym_user_instruction,
      sym_workdir_instruction,
      sym_arg_instruction,
      sym_onbuild_instruction,
      sym_stopsignal_instruction,
      sym_healthcheck_instruction,
      sym_shell_instruction,
      sym_maintainer_instruction,
      sym_cross_build_instruction,
  [180] = 21,
    ACTIONS(9), 1,
      aux_sym_from_instruction_token1,
    ACTIONS(11), 1,
      aux_sym_run_instruction_token1,
    ACTIONS(13), 1,
      aux_sym_cmd_instruction_token1,
    ACTIONS(15), 1,
      aux_sym_label_instruction_token1,
    ACTIONS(17), 1,
      aux_sym_expose_instruction_token1,
    ACTIONS(19), 1,
      aux_sym_env_instruction_token1,
    ACTIONS(21), 1,
      aux_sym_add_instruction_token1,
    ACTIONS(23), 1,
      aux_sym_copy_instruction_token1,
    ACTIONS(25), 1,
      aux_sym_entrypoint_instruction_token1,
    ACTIONS(27), 1,
      aux_sym_volume_instruction_token1,
    ACTIONS(29), 1,
      aux_sym_user_instruction_token1,
    ACTIONS(31), 1,
      aux_sym_workdir_instruction_token1,
    ACTIONS(33), 1,
      aux_sym_arg_instruction_token1,
    ACTIONS(35), 1,
      aux_sym_onbuild_instruction_token1,
    ACTIONS(37), 1,
      aux_sym_stopsignal_instruction_token1,
    ACTIONS(39), 1,
      aux_sym_healthcheck_instruction_token1,
    ACTIONS(41), 1,
      aux_sym_shell_instruction_token1,
    ACTIONS(43), 1,
      aux_sym_maintainer_instruction_token1,
    ACTIONS(45), 1,
      aux_sym_cross_build_instruction_token1,
    ACTIONS(3), 2,
      sym_line_continuation,
      sym_comment,
    STATE(265), 20,
      sym__instruction,
      sym_from_instruction,
      sym_run_instruction,
      sym_cmd_instruction,
      sym_label_instruction,
      sym_expose_instruction,
      sym_env_instruction,
      sym_add_instruction,
      sym_copy_instruction,
      sym_entrypoint_instruction,
      sym_volume_instruction,
      sym_user_instruction,
      sym_workdir_instruction,
      sym_arg_instruction,
      sym_onbuild_instruction,
      sym_stopsignal_instruction,
      sym_healthcheck_instruction,
      sym_shell_instruction,
      sym_maintainer_instruction,
      sym_cross_build_instruction,
  [264] = 2,
    ACTIONS(3), 2,
      sym_line_continuation,
      sym_comment,
    ACTIONS(49), 20,
      ts_builtin_sym_end,
      aux_sym_from_instruction_token1,
      aux_sym_run_instruction_token1,
      aux_sym_cmd_instruction_token1,
      aux_sym_label_instruction_token1,
      aux_sym_expose_instruction_token1,
      aux_sym_env_instruction_token1,
      aux_sym_add_instruction_token1,
      aux_sym_copy_instruction_token1,
      aux_sym_entrypoint_instruction_token1,
      aux_sym_volume_instruction_token1,
      aux_sym_user_instruction_token1,
      aux_sym_workdir_instruction_token1,
      aux_sym_arg_instruction_token1,
      aux_sym_onbuild_instruction_token1,
      aux_sym_stopsignal_instruction_token1,
      aux_sym_healthcheck_instruction_token1,
      aux_sym_shell_instruction_token1,
      aux_sym_maintainer_instruction_token1,
      aux_sym_cross_build_instruction_token1,
  [291] = 9,
    ACTIONS(110), 1,
      anon_sym_DASH_DASH,
    ACTIONS(112), 1,
      anon_sym_LBRACK,
    ACTIONS(114), 1,
      sym_heredoc_marker,
    STATE(22), 1,
      aux_sym_shell_fragment_repeat1,
    STATE(150), 1,
      sym_shell_fragment,
    ACTIONS(3), 2,
      sym_line_continuation,
      sym_comment,
    STATE(131), 2,
      sym_shell_command,
      sym_json_string_array,
    STATE(19), 3,
      sym_param,
      sym_mount_param,
      aux_sym_run_instruction_repeat1,
    ACTIONS(108), 4,
      aux_sym_path_token2,
      aux_sym_shell_fragment_token2,
      aux_sym_shell_fragment_token3,
      aux_sym_shell_fragment_token4,
  [326] = 9,
    ACTIONS(110), 1,
      anon_sym_DASH_DASH,
    ACTIONS(112), 1,
      anon_sym_LBRACK,
    ACTIONS(114), 1,
      sym_heredoc_marker,
    STATE(22), 1,
      aux_sym_shell_fragment_repeat1,
    STATE(150), 1,
      sym_shell_fragment,
    ACTIONS(3), 2,
      sym_line_continuation,
      sym_comment,
    STATE(141), 2,
      sym_shell_command,
      sym_json_string_array,
    STATE(6), 3,
      sym_param,
      sym_mount_param,
      aux_sym_run_instruction_repeat1,
    ACTIONS(108), 4,
      aux_sym_path_token2,
      aux_sym_shell_fragment_token2,
      aux_sym_shell_fragment_token3,
      aux_sym_shell_fragment_token4,
  [361] = 9,
    ACTIONS(116), 1,
      anon_sym_LF,
    ACTIONS(118), 1,
      anon_sym_DOLLAR2,
    ACTIONS(120), 1,
      aux_sym__env_key_token1,
    ACTIONS(122), 1,
      anon_sym_DQUOTE,
    ACTIONS(124), 1,
      anon_sym_SQUOTE,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    ACTIONS(126), 2,
      aux_sym_unquoted_string_token1,
      anon_sym_BSLASH2,
    STATE(47), 3,
      sym__immediate_expansion,
      sym__imm_expansion,
      aux_sym_unquoted_string_repeat1,
    STATE(239), 3,
      sym_double_quoted_string,
      sym_single_quoted_string,
      sym_unquoted_string,
  [395] = 8,
    ACTIONS(3), 1,
      sym_line_continuation,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(128), 1,
      anon_sym_DOLLAR2,
    ACTIONS(130), 1,
      anon_sym_DQUOTE,
    ACTIONS(132), 1,
      anon_sym_SQUOTE,
    ACTIONS(134), 2,
      aux_sym_unquoted_string_token1,
      anon_sym_BSLASH2,
    STATE(18), 3,
      sym__immediate_expansion,
      sym__imm_expansion,
      aux_sym_unquoted_string_repeat1,
    STATE(313), 3,
      sym_double_quoted_string,
      sym_single_quoted_string,
      sym_unquoted_string,
  [425] = 8,
    ACTIONS(3), 1,
      sym_line_continuation,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(128), 1,
      anon_sym_DOLLAR2,
    ACTIONS(130), 1,
      anon_sym_DQUOTE,
    ACTIONS(132), 1,
      anon_sym_SQUOTE,
    ACTIONS(134), 2,
      aux_sym_unquoted_string_token1,
      anon_sym_BSLASH2,
    STATE(18), 3,
      sym__immediate_expansion,
      sym__imm_expansion,
      aux_sym_unquoted_string_repeat1,
    STATE(127), 3,
      sym_double_quoted_string,
      sym_single_quoted_string,
      sym_unquoted_string,
  [455] = 8,
    ACTIONS(3), 1,
      sym_line_continuation,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(128), 1,
      anon_sym_DOLLAR2,
    ACTIONS(130), 1,
      anon_sym_DQUOTE,
    ACTIONS(132), 1,
      anon_sym_SQUOTE,
    ACTIONS(134), 2,
      aux_sym_unquoted_string_token1,
      anon_sym_BSLASH2,
    STATE(18), 3,
      sym__immediate_expansion,
      sym__imm_expansion,
      aux_sym_unquoted_string_repeat1,
    STATE(258), 3,
      sym_double_quoted_string,
      sym_single_quoted_string,
      sym_unquoted_string,
  [485] = 8,
    ACTIONS(138), 1,
      aux_sym_shell_fragment_token2,
    ACTIONS(140), 1,
      anon_sym_LBRACK,
    ACTIONS(142), 1,
      sym_heredoc_marker,
    STATE(43), 1,
      aux_sym_shell_fragment_repeat1,
    STATE(167), 1,
      sym_shell_fragment,
    ACTIONS(3), 2,
      sym_line_continuation,
      sym_comment,
    STATE(263), 2,
      sym_shell_command,
      sym_json_string_array,
    ACTIONS(136), 3,
      aux_sym_path_token2,
      aux_sym_shell_fragment_token3,
      aux_sym_shell_fragment_token4,
  [514] = 11,
    ACTIONS(3), 1,
      sym_line_continuation,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(144), 1,
      aux_sym_path_token1,
    ACTIONS(146), 1,
      aux_sym_path_with_heredoc_token1,
    ACTIONS(148), 1,
      anon_sym_DOLLAR,
    ACTIONS(150), 1,
      anon_sym_DASH_DASH,
    ACTIONS(152), 1,
      sym_heredoc_marker,
    STATE(56), 1,
      aux_sym_add_instruction_repeat2,
    STATE(84), 1,
      sym_expansion,
    STATE(299), 1,
      sym_path_with_heredoc,
    STATE(17), 2,
      sym_param,
      aux_sym_add_instruction_repeat1,
  [549] = 11,
    ACTIONS(3), 1,
      sym_line_continuation,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(144), 1,
      aux_sym_path_token1,
    ACTIONS(146), 1,
      aux_sym_path_with_heredoc_token1,
    ACTIONS(148), 1,
      anon_sym_DOLLAR,
    ACTIONS(150), 1,
      anon_sym_DASH_DASH,
    ACTIONS(152), 1,
      sym_heredoc_marker,
    STATE(74), 1,
      aux_sym_add_instruction_repeat2,
    STATE(84), 1,
      sym_expansion,
    STATE(299), 1,
      sym_path_with_heredoc,
    STATE(15), 2,
      sym_param,
      aux_sym_add_instruction_repeat1,
  [584] = 11,
    ACTIONS(3), 1,
      sym_line_continuation,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(144), 1,
      aux_sym_path_token1,
    ACTIONS(146), 1,
      aux_sym_path_with_heredoc_token1,
    ACTIONS(148), 1,
      anon_sym_DOLLAR,
    ACTIONS(150), 1,
      anon_sym_DASH_DASH,
    ACTIONS(152), 1,
      sym_heredoc_marker,
    STATE(65), 1,
      aux_sym_add_instruction_repeat2,
    STATE(84), 1,
      sym_expansion,
    STATE(299), 1,
      sym_path_with_heredoc,
    STATE(64), 2,
      sym_param,
      aux_sym_add_instruction_repeat1,
  [619] = 8,
    ACTIONS(138), 1,
      aux_sym_shell_fragment_token2,
    ACTIONS(140), 1,
      anon_sym_LBRACK,
    ACTIONS(142), 1,
      sym_heredoc_marker,
    STATE(43), 1,
      aux_sym_shell_fragment_repeat1,
    STATE(167), 1,
      sym_shell_fragment,
    ACTIONS(3), 2,
      sym_line_continuation,
      sym_comment,
    STATE(292), 2,
      sym_shell_command,
      sym_json_string_array,
    ACTIONS(136), 3,
      aux_sym_path_token2,
      aux_sym_shell_fragment_token3,
      aux_sym_shell_fragment_token4,
  [648] = 11,
    ACTIONS(3), 1,
      sym_line_continuation,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(144), 1,
      aux_sym_path_token1,
    ACTIONS(146), 1,
      aux_sym_path_with_heredoc_token1,
    ACTIONS(148), 1,
      anon_sym_DOLLAR,
    ACTIONS(150), 1,
      anon_sym_DASH_DASH,
    ACTIONS(152), 1,
      sym_heredoc_marker,
    STATE(69), 1,
      aux_sym_add_instruction_repeat2,
    STATE(84), 1,
      sym_expansion,
    STATE(299), 1,
      sym_path_with_heredoc,
    STATE(64), 2,
      sym_param,
      aux_sym_add_instruction_repeat1,
  [683] = 6,
    ACTIONS(128), 1,
      anon_sym_DOLLAR2,
    ACTIONS(154), 1,
      anon_sym_LF,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    ACTIONS(158), 2,
      aux_sym_unquoted_string_token1,
      anon_sym_BSLASH2,
    ACTIONS(156), 3,
      aux_sym_label_pair_token1,
      anon_sym_DQUOTE,
      anon_sym_SQUOTE,
    STATE(20), 3,
      sym__immediate_expansion,
      sym__imm_expansion,
      aux_sym_unquoted_string_repeat1,
  [708] = 5,
    ACTIONS(162), 1,
      anon_sym_DASH_DASH,
    ACTIONS(3), 2,
      sym_line_continuation,
      sym_comment,
    ACTIONS(165), 2,
      sym_heredoc_marker,
      anon_sym_LBRACK,
    STATE(19), 3,
      sym_param,
      sym_mount_param,
      aux_sym_run_instruction_repeat1,
    ACTIONS(160), 4,
      aux_sym_path_token2,
      aux_sym_shell_fragment_token2,
      aux_sym_shell_fragment_token3,
      aux_sym_shell_fragment_token4,
  [731] = 6,
    ACTIONS(167), 1,
      anon_sym_LF,
    ACTIONS(169), 1,
      anon_sym_DOLLAR2,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    ACTIONS(174), 2,
      aux_sym_unquoted_string_token1,
      anon_sym_BSLASH2,
    ACTIONS(172), 3,
      aux_sym_label_pair_token1,
      anon_sym_DQUOTE,
      anon_sym_SQUOTE,
    STATE(20), 3,
      sym__immediate_expansion,
      sym__imm_expansion,
      aux_sym_unquoted_string_repeat1,
  [756] = 6,
    ACTIONS(179), 1,
      aux_sym_from_instruction_token2,
    ACTIONS(181), 1,
      anon_sym_DOLLAR2,
    ACTIONS(183), 1,
      aux_sym_image_name_token2,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    ACTIONS(177), 3,
      anon_sym_LF,
      anon_sym_COLON,
      anon_sym_AT,
    STATE(24), 3,
      sym__immediate_expansion,
      sym__imm_expansion,
      aux_sym_image_name_repeat1,
  [780] = 6,
    ACTIONS(114), 1,
      sym_heredoc_marker,
    ACTIONS(189), 1,
      sym_required_line_continuation,
    STATE(23), 1,
      aux_sym_shell_fragment_repeat1,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    ACTIONS(185), 2,
      sym_heredoc_nl,
      anon_sym_LF,
    ACTIONS(187), 4,
      aux_sym_path_token2,
      aux_sym_shell_fragment_token2,
      aux_sym_shell_fragment_token3,
      aux_sym_shell_fragment_token4,
  [804] = 6,
    ACTIONS(196), 1,
      sym_required_line_continuation,
    ACTIONS(198), 1,
      sym_heredoc_marker,
    STATE(23), 1,
      aux_sym_shell_fragment_repeat1,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    ACTIONS(191), 2,
      sym_heredoc_nl,
      anon_sym_LF,
    ACTIONS(193), 4,
      aux_sym_path_token2,
      aux_sym_shell_fragment_token2,
      aux_sym_shell_fragment_token3,
      aux_sym_shell_fragment_token4,
  [828] = 6,
    ACTIONS(181), 1,
      anon_sym_DOLLAR2,
    ACTIONS(203), 1,
      aux_sym_from_instruction_token2,
    ACTIONS(205), 1,
      aux_sym_image_name_token2,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    ACTIONS(201), 3,
      anon_sym_LF,
      anon_sym_COLON,
      anon_sym_AT,
    STATE(25), 3,
      sym__immediate_expansion,
      sym__imm_expansion,
      aux_sym_image_name_repeat1,
  [852] = 6,
    ACTIONS(209), 1,
      aux_sym_from_instruction_token2,
    ACTIONS(211), 1,
      anon_sym_DOLLAR2,
    ACTIONS(214), 1,
      aux_sym_image_name_token2,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    ACTIONS(207), 3,
      anon_sym_LF,
      anon_sym_COLON,
      anon_sym_AT,
    STATE(25), 3,
      sym__immediate_expansion,
      sym__imm_expansion,
      aux_sym_image_name_repeat1,
  [876] = 5,
    ACTIONS(221), 1,
      anon_sym_COMMA,
    STATE(27), 1,
      aux_sym_mount_param_repeat1,
    ACTIONS(3), 2,
      sym_line_continuation,
      sym_comment,
    ACTIONS(219), 3,
      sym_heredoc_marker,
      anon_sym_DASH_DASH,
      anon_sym_LBRACK,
    ACTIONS(217), 4,
      aux_sym_path_token2,
      aux_sym_shell_fragment_token2,
      aux_sym_shell_fragment_token3,
      aux_sym_shell_fragment_token4,
  [898] = 5,
    ACTIONS(221), 1,
      anon_sym_COMMA,
    STATE(28), 1,
      aux_sym_mount_param_repeat1,
    ACTIONS(3), 2,
      sym_line_continuation,
      sym_comment,
    ACTIONS(225), 3,
      sym_heredoc_marker,
      anon_sym_DASH_DASH,
      anon_sym_LBRACK,
    ACTIONS(223), 4,
      aux_sym_path_token2,
      aux_sym_shell_fragment_token2,
      aux_sym_shell_fragment_token3,
      aux_sym_shell_fragment_token4,
  [920] = 5,
    ACTIONS(231), 1,
      anon_sym_COMMA,
    STATE(28), 1,
      aux_sym_mount_param_repeat1,
    ACTIONS(3), 2,
      sym_line_continuation,
      sym_comment,
    ACTIONS(229), 3,
      sym_heredoc_marker,
      anon_sym_DASH_DASH,
      anon_sym_LBRACK,
    ACTIONS(227), 4,
      aux_sym_path_token2,
      aux_sym_shell_fragment_token2,
      aux_sym_shell_fragment_token3,
      aux_sym_shell_fragment_token4,
  [942] = 6,
    ACTIONS(236), 1,
      aux_sym_from_instruction_token2,
    ACTIONS(238), 1,
      anon_sym_DOLLAR2,
    ACTIONS(240), 1,
      aux_sym_image_tag_token1,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    ACTIONS(234), 2,
      anon_sym_LF,
      anon_sym_AT,
    STATE(39), 3,
      sym__immediate_expansion,
      sym__imm_expansion,
      aux_sym_image_tag_repeat1,
  [965] = 6,
    ACTIONS(242), 1,
      anon_sym_DOLLAR2,
    ACTIONS(244), 1,
      anon_sym_DQUOTE,
    ACTIONS(248), 1,
      sym_double_quoted_escape_sequence,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    ACTIONS(246), 2,
      aux_sym_double_quoted_string_token1,
      anon_sym_BSLASH,
    STATE(37), 3,
      sym__immediate_expansion,
      sym__imm_expansion,
      aux_sym_double_quoted_string_repeat1,
  [988] = 5,
    ACTIONS(252), 1,
      aux_sym__immediate_user_name_or_group_fragment_token1,
    ACTIONS(254), 1,
      anon_sym_DOLLAR2,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    ACTIONS(250), 2,
      anon_sym_LF,
      anon_sym_COLON,
    STATE(35), 4,
      sym__immediate_user_name_or_group_fragment,
      sym__immediate_expansion,
      sym__imm_expansion,
      aux_sym__user_name_or_group_repeat1,
  [1009] = 3,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    ACTIONS(191), 3,
      sym_heredoc_marker,
      sym_heredoc_nl,
      anon_sym_LF,
    ACTIONS(196), 5,
      aux_sym_path_token2,
      aux_sym_shell_fragment_token2,
      aux_sym_shell_fragment_token3,
      aux_sym_shell_fragment_token4,
      sym_required_line_continuation,
  [1026] = 6,
    ACTIONS(242), 1,
      anon_sym_DOLLAR2,
    ACTIONS(248), 1,
      sym_double_quoted_escape_sequence,
    ACTIONS(256), 1,
      anon_sym_DQUOTE,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    ACTIONS(246), 2,
      aux_sym_double_quoted_string_token1,
      anon_sym_BSLASH,
    STATE(37), 3,
      sym__immediate_expansion,
      sym__imm_expansion,
      aux_sym_double_quoted_string_repeat1,
  [1049] = 7,
    ACTIONS(258), 1,
      anon_sym_LF,
    ACTIONS(260), 1,
      aux_sym_label_pair_token1,
    ACTIONS(263), 1,
      anon_sym_DQUOTE,
    ACTIONS(266), 1,
      anon_sym_SQUOTE,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    STATE(34), 2,
      sym_label_pair,
      aux_sym_label_instruction_repeat1,
    STATE(249), 2,
      sym_double_quoted_string,
      sym_single_quoted_string,
  [1074] = 5,
    ACTIONS(254), 1,
      anon_sym_DOLLAR2,
    ACTIONS(271), 1,
      aux_sym__immediate_user_name_or_group_fragment_token1,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    ACTIONS(269), 2,
      anon_sym_LF,
      anon_sym_COLON,
    STATE(38), 4,
      sym__immediate_user_name_or_group_fragment,
      sym__immediate_expansion,
      sym__imm_expansion,
      aux_sym__user_name_or_group_repeat1,
  [1095] = 6,
    ACTIONS(242), 1,
      anon_sym_DOLLAR2,
    ACTIONS(273), 1,
      anon_sym_DQUOTE,
    ACTIONS(277), 1,
      sym_double_quoted_escape_sequence,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    ACTIONS(275), 2,
      aux_sym_double_quoted_string_token1,
      anon_sym_BSLASH,
    STATE(33), 3,
      sym__immediate_expansion,
      sym__imm_expansion,
      aux_sym_double_quoted_string_repeat1,
  [1118] = 6,
    ACTIONS(279), 1,
      anon_sym_DOLLAR2,
    ACTIONS(282), 1,
      anon_sym_DQUOTE,
    ACTIONS(287), 1,
      sym_double_quoted_escape_sequence,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    ACTIONS(284), 2,
      aux_sym_double_quoted_string_token1,
      anon_sym_BSLASH,
    STATE(37), 3,
      sym__immediate_expansion,
      sym__imm_expansion,
      aux_sym_double_quoted_string_repeat1,
  [1141] = 5,
    ACTIONS(292), 1,
      aux_sym__immediate_user_name_or_group_fragment_token1,
    ACTIONS(295), 1,
      anon_sym_DOLLAR2,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    ACTIONS(290), 2,
      anon_sym_LF,
      anon_sym_COLON,
    STATE(38), 4,
      sym__immediate_user_name_or_group_fragment,
      sym__immediate_expansion,
      sym__imm_expansion,
      aux_sym__user_name_or_group_repeat1,
  [1162] = 6,
    ACTIONS(300), 1,
      aux_sym_from_instruction_token2,
    ACTIONS(302), 1,
      anon_sym_DOLLAR2,
    ACTIONS(305), 1,
      aux_sym_image_tag_token1,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    ACTIONS(298), 2,
      anon_sym_LF,
      anon_sym_AT,
    STATE(39), 3,
      sym__immediate_expansion,
      sym__imm_expansion,
      aux_sym_image_tag_repeat1,
  [1185] = 7,
    ACTIONS(308), 1,
      anon_sym_LF,
    ACTIONS(310), 1,
      aux_sym_label_pair_token1,
    ACTIONS(312), 1,
      anon_sym_DQUOTE,
    ACTIONS(314), 1,
      anon_sym_SQUOTE,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    STATE(34), 2,
      sym_label_pair,
      aux_sym_label_instruction_repeat1,
    STATE(249), 2,
      sym_double_quoted_string,
      sym_single_quoted_string,
  [1210] = 3,
    ACTIONS(3), 2,
      sym_line_continuation,
      sym_comment,
    ACTIONS(316), 4,
      aux_sym_path_token2,
      aux_sym_shell_fragment_token2,
      aux_sym_shell_fragment_token3,
      aux_sym_shell_fragment_token4,
    ACTIONS(318), 4,
      sym_heredoc_marker,
      anon_sym_DASH_DASH,
      anon_sym_COMMA,
      anon_sym_LBRACK,
  [1227] = 3,
    ACTIONS(3), 2,
      sym_line_continuation,
      sym_comment,
    ACTIONS(227), 4,
      aux_sym_path_token2,
      aux_sym_shell_fragment_token2,
      aux_sym_shell_fragment_token3,
      aux_sym_shell_fragment_token4,
    ACTIONS(229), 4,
      sym_heredoc_marker,
      anon_sym_DASH_DASH,
      anon_sym_COMMA,
      anon_sym_LBRACK,
  [1244] = 6,
    ACTIONS(142), 1,
      sym_heredoc_marker,
    ACTIONS(185), 1,
      anon_sym_LF,
    ACTIONS(189), 1,
      sym_required_line_continuation,
    STATE(45), 1,
      aux_sym_shell_fragment_repeat1,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    ACTIONS(320), 4,
      aux_sym_path_token2,
      aux_sym_shell_fragment_token2,
      aux_sym_shell_fragment_token3,
      aux_sym_shell_fragment_token4,
  [1267] = 6,
    ACTIONS(322), 1,
      anon_sym_LF,
    ACTIONS(324), 1,
      aux_sym_path_token3,
    ACTIONS(326), 1,
      anon_sym_DOLLAR2,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    ACTIONS(328), 2,
      sym_heredoc_nl,
      sym__non_newline_whitespace,
    STATE(46), 3,
      sym__immediate_expansion,
      sym__imm_expansion,
      aux_sym_path_repeat1,
  [1290] = 6,
    ACTIONS(191), 1,
      anon_sym_LF,
    ACTIONS(196), 1,
      sym_required_line_continuation,
    ACTIONS(333), 1,
      sym_heredoc_marker,
    STATE(45), 1,
      aux_sym_shell_fragment_repeat1,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    ACTIONS(330), 4,
      aux_sym_path_token2,
      aux_sym_shell_fragment_token2,
      aux_sym_shell_fragment_token3,
      aux_sym_shell_fragment_token4,
  [1313] = 6,
    ACTIONS(326), 1,
      anon_sym_DOLLAR2,
    ACTIONS(336), 1,
      anon_sym_LF,
    ACTIONS(338), 1,
      aux_sym_path_token3,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    ACTIONS(340), 2,
      sym_heredoc_nl,
      sym__non_newline_whitespace,
    STATE(49), 3,
      sym__immediate_expansion,
      sym__imm_expansion,
      aux_sym_path_repeat1,
  [1336] = 6,
    ACTIONS(118), 1,
      anon_sym_DOLLAR2,
    ACTIONS(154), 1,
      anon_sym_LF,
    ACTIONS(156), 1,
      aux_sym__env_key_token1,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    ACTIONS(342), 2,
      aux_sym_unquoted_string_token1,
      anon_sym_BSLASH2,
    STATE(48), 3,
      sym__immediate_expansion,
      sym__imm_expansion,
      aux_sym_unquoted_string_repeat1,
  [1359] = 6,
    ACTIONS(167), 1,
      anon_sym_LF,
    ACTIONS(172), 1,
      aux_sym__env_key_token1,
    ACTIONS(344), 1,
      anon_sym_DOLLAR2,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    ACTIONS(347), 2,
      aux_sym_unquoted_string_token1,
      anon_sym_BSLASH2,
    STATE(48), 3,
      sym__immediate_expansion,
      sym__imm_expansion,
      aux_sym_unquoted_string_repeat1,
  [1382] = 6,
    ACTIONS(350), 1,
      anon_sym_LF,
    ACTIONS(352), 1,
      aux_sym_path_token3,
    ACTIONS(355), 1,
      anon_sym_DOLLAR2,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    ACTIONS(358), 2,
      sym_heredoc_nl,
      sym__non_newline_whitespace,
    STATE(49), 3,
      sym__immediate_expansion,
      sym__imm_expansion,
      aux_sym_path_repeat1,
  [1405] = 6,
    ACTIONS(242), 1,
      anon_sym_DOLLAR2,
    ACTIONS(360), 1,
      anon_sym_DQUOTE,
    ACTIONS(364), 1,
      sym_double_quoted_escape_sequence,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    ACTIONS(362), 2,
      aux_sym_double_quoted_string_token1,
      anon_sym_BSLASH,
    STATE(51), 3,
      sym__immediate_expansion,
      sym__imm_expansion,
      aux_sym_double_quoted_string_repeat1,
  [1428] = 6,
    ACTIONS(242), 1,
      anon_sym_DOLLAR2,
    ACTIONS(248), 1,
      sym_double_quoted_escape_sequence,
    ACTIONS(366), 1,
      anon_sym_DQUOTE,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    ACTIONS(246), 2,
      aux_sym_double_quoted_string_token1,
      anon_sym_BSLASH,
    STATE(37), 3,
      sym__immediate_expansion,
      sym__imm_expansion,
      aux_sym_double_quoted_string_repeat1,
  [1451] = 6,
    ACTIONS(242), 1,
      anon_sym_DOLLAR2,
    ACTIONS(368), 1,
      anon_sym_DQUOTE,
    ACTIONS(372), 1,
      sym_double_quoted_escape_sequence,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    ACTIONS(370), 2,
      aux_sym_double_quoted_string_token1,
      anon_sym_BSLASH,
    STATE(30), 3,
      sym__immediate_expansion,
      sym__imm_expansion,
      aux_sym_double_quoted_string_repeat1,
  [1474] = 6,
    ACTIONS(374), 1,
      anon_sym_LF,
    ACTIONS(376), 1,
      aux_sym__immediate_user_name_or_group_fragment_token1,
    ACTIONS(379), 1,
      anon_sym_DOLLAR2,
    STATE(53), 1,
      aux_sym__immediate_user_name_or_group,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    STATE(164), 3,
      sym__immediate_user_name_or_group_fragment,
      sym__immediate_expansion,
      sym__imm_expansion,
  [1496] = 9,
    ACTIONS(3), 1,
      sym_line_continuation,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(140), 1,
      anon_sym_LBRACK,
    ACTIONS(382), 1,
      aux_sym_path_token1,
    ACTIONS(384), 1,
      aux_sym_path_token2,
    ACTIONS(386), 1,
      anon_sym_DOLLAR,
    STATE(60), 1,
      sym_expansion,
    STATE(198), 1,
      sym_path,
    STATE(275), 1,
      sym_json_string_array,
  [1524] = 6,
    ACTIONS(388), 1,
      anon_sym_LF,
    ACTIONS(390), 1,
      aux_sym_from_instruction_token2,
    ACTIONS(392), 1,
      anon_sym_DOLLAR2,
    ACTIONS(394), 1,
      aux_sym_image_digest_token1,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    STATE(59), 3,
      sym__immediate_expansion,
      sym__imm_expansion,
      aux_sym_image_digest_repeat1,
  [1546] = 9,
    ACTIONS(3), 1,
      sym_line_continuation,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(396), 1,
      aux_sym_path_token1,
    ACTIONS(398), 1,
      aux_sym_path_with_heredoc_token1,
    ACTIONS(400), 1,
      anon_sym_DOLLAR,
    ACTIONS(402), 1,
      sym_heredoc_marker,
    STATE(44), 1,
      sym_expansion,
    STATE(66), 1,
      aux_sym_add_instruction_repeat2,
    STATE(100), 1,
      sym_path_with_heredoc,
  [1574] = 6,
    ACTIONS(254), 1,
      anon_sym_DOLLAR2,
    ACTIONS(404), 1,
      anon_sym_LF,
    ACTIONS(406), 1,
      aux_sym__immediate_user_name_or_group_fragment_token1,
    STATE(53), 1,
      aux_sym__immediate_user_name_or_group,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    STATE(164), 3,
      sym__immediate_user_name_or_group_fragment,
      sym__immediate_expansion,
      sym__imm_expansion,
  [1596] = 6,
    ACTIONS(114), 1,
      sym_heredoc_marker,
    ACTIONS(408), 1,
      aux_sym_shell_fragment_token2,
    STATE(22), 1,
      aux_sym_shell_fragment_repeat1,
    STATE(187), 1,
      sym_shell_fragment,
    ACTIONS(3), 2,
      sym_line_continuation,
      sym_comment,
    ACTIONS(108), 3,
      aux_sym_path_token2,
      aux_sym_shell_fragment_token3,
      aux_sym_shell_fragment_token4,
  [1618] = 6,
    ACTIONS(410), 1,
      anon_sym_LF,
    ACTIONS(412), 1,
      aux_sym_from_instruction_token2,
    ACTIONS(414), 1,
      anon_sym_DOLLAR2,
    ACTIONS(417), 1,
      aux_sym_image_digest_token1,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    STATE(59), 3,
      sym__immediate_expansion,
      sym__imm_expansion,
      aux_sym_image_digest_repeat1,
  [1640] = 6,
    ACTIONS(420), 1,
      anon_sym_LF,
    ACTIONS(422), 1,
      aux_sym_path_token3,
    ACTIONS(424), 1,
      anon_sym_DOLLAR2,
    ACTIONS(426), 1,
      sym__non_newline_whitespace,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    STATE(70), 3,
      sym__immediate_expansion,
      sym__imm_expansion,
      aux_sym_path_repeat1,
  [1662] = 6,
    ACTIONS(428), 1,
      aux_sym_label_pair_token1,
    ACTIONS(430), 1,
      anon_sym_DQUOTE,
    ACTIONS(432), 1,
      anon_sym_SQUOTE,
    ACTIONS(3), 2,
      sym_line_continuation,
      sym_comment,
    STATE(40), 2,
      sym_label_pair,
      aux_sym_label_instruction_repeat1,
    STATE(249), 2,
      sym_double_quoted_string,
      sym_single_quoted_string,
  [1684] = 3,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    ACTIONS(191), 2,
      sym_heredoc_marker,
      anon_sym_LF,
    ACTIONS(196), 5,
      aux_sym_path_token2,
      aux_sym_shell_fragment_token2,
      aux_sym_shell_fragment_token3,
      aux_sym_shell_fragment_token4,
      sym_required_line_continuation,
  [1700] = 6,
    ACTIONS(138), 1,
      aux_sym_shell_fragment_token2,
    ACTIONS(142), 1,
      sym_heredoc_marker,
    STATE(43), 1,
      aux_sym_shell_fragment_repeat1,
    STATE(243), 1,
      sym_shell_fragment,
    ACTIONS(3), 2,
      sym_line_continuation,
      sym_comment,
    ACTIONS(136), 3,
      aux_sym_path_token2,
      aux_sym_shell_fragment_token3,
      aux_sym_shell_fragment_token4,
  [1722] = 6,
    ACTIONS(3), 1,
      sym_line_continuation,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(434), 1,
      aux_sym_path_token1,
    ACTIONS(438), 1,
      anon_sym_DASH_DASH,
    STATE(64), 2,
      sym_param,
      aux_sym_add_instruction_repeat1,
    ACTIONS(436), 3,
      sym_heredoc_marker,
      aux_sym_path_with_heredoc_token1,
      anon_sym_DOLLAR,
  [1744] = 9,
    ACTIONS(3), 1,
      sym_line_continuation,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(396), 1,
      aux_sym_path_token1,
    ACTIONS(398), 1,
      aux_sym_path_with_heredoc_token1,
    ACTIONS(400), 1,
      anon_sym_DOLLAR,
    ACTIONS(402), 1,
      sym_heredoc_marker,
    STATE(44), 1,
      sym_expansion,
    STATE(66), 1,
      aux_sym_add_instruction_repeat2,
    STATE(121), 1,
      sym_path_with_heredoc,
  [1772] = 9,
    ACTIONS(3), 1,
      sym_line_continuation,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(441), 1,
      aux_sym_path_token1,
    ACTIONS(444), 1,
      aux_sym_path_with_heredoc_token1,
    ACTIONS(447), 1,
      anon_sym_DOLLAR,
    ACTIONS(450), 1,
      sym_heredoc_marker,
    STATE(66), 1,
      aux_sym_add_instruction_repeat2,
    STATE(84), 1,
      sym_expansion,
    STATE(299), 1,
      sym_path_with_heredoc,
  [1800] = 6,
    ACTIONS(350), 1,
      anon_sym_LF,
    ACTIONS(358), 1,
      sym__non_newline_whitespace,
    ACTIONS(453), 1,
      aux_sym_path_token3,
    ACTIONS(456), 1,
      anon_sym_DOLLAR2,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    STATE(67), 3,
      sym__immediate_expansion,
      sym__imm_expansion,
      aux_sym_path_repeat1,
  [1822] = 3,
    ACTIONS(3), 2,
      sym_line_continuation,
      sym_comment,
    ACTIONS(461), 3,
      sym_heredoc_marker,
      anon_sym_DASH_DASH,
      anon_sym_LBRACK,
    ACTIONS(459), 4,
      aux_sym_path_token2,
      aux_sym_shell_fragment_token2,
      aux_sym_shell_fragment_token3,
      aux_sym_shell_fragment_token4,
  [1838] = 9,
    ACTIONS(3), 1,
      sym_line_continuation,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(396), 1,
      aux_sym_path_token1,
    ACTIONS(398), 1,
      aux_sym_path_with_heredoc_token1,
    ACTIONS(400), 1,
      anon_sym_DOLLAR,
    ACTIONS(402), 1,
      sym_heredoc_marker,
    STATE(44), 1,
      sym_expansion,
    STATE(66), 1,
      aux_sym_add_instruction_repeat2,
    STATE(124), 1,
      sym_path_with_heredoc,
  [1866] = 6,
    ACTIONS(424), 1,
      anon_sym_DOLLAR2,
    ACTIONS(463), 1,
      anon_sym_LF,
    ACTIONS(465), 1,
      aux_sym_path_token3,
    ACTIONS(467), 1,
      sym__non_newline_whitespace,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    STATE(67), 3,
      sym__immediate_expansion,
      sym__imm_expansion,
      aux_sym_path_repeat1,
  [1888] = 9,
    ACTIONS(3), 1,
      sym_line_continuation,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(469), 1,
      anon_sym_DOLLAR,
    ACTIONS(471), 1,
      aux_sym_image_name_token1,
    ACTIONS(473), 1,
      anon_sym_DASH_DASH,
    STATE(21), 1,
      sym_expansion,
    STATE(83), 1,
      sym_image_name,
    STATE(105), 1,
      sym_param,
    STATE(237), 1,
      sym_image_spec,
  [1916] = 3,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    ACTIONS(475), 2,
      anon_sym_LF,
      anon_sym_DOLLAR2,
    ACTIONS(477), 5,
      aux_sym_label_pair_token1,
      anon_sym_DQUOTE,
      anon_sym_SQUOTE,
      aux_sym_unquoted_string_token1,
      anon_sym_BSLASH2,
  [1932] = 3,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    ACTIONS(479), 2,
      anon_sym_LF,
      anon_sym_DOLLAR2,
    ACTIONS(481), 5,
      aux_sym_label_pair_token1,
      anon_sym_DQUOTE,
      anon_sym_SQUOTE,
      aux_sym_unquoted_string_token1,
      anon_sym_BSLASH2,
  [1948] = 9,
    ACTIONS(3), 1,
      sym_line_continuation,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(396), 1,
      aux_sym_path_token1,
    ACTIONS(398), 1,
      aux_sym_path_with_heredoc_token1,
    ACTIONS(400), 1,
      anon_sym_DOLLAR,
    ACTIONS(402), 1,
      sym_heredoc_marker,
    STATE(44), 1,
      sym_expansion,
    STATE(66), 1,
      aux_sym_add_instruction_repeat2,
    STATE(98), 1,
      sym_path_with_heredoc,
  [1976] = 3,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    ACTIONS(477), 2,
      aux_sym_from_instruction_token2,
      aux_sym_image_name_token2,
    ACTIONS(475), 4,
      anon_sym_LF,
      anon_sym_COLON,
      anon_sym_DOLLAR2,
      anon_sym_AT,
  [1991] = 5,
    ACTIONS(426), 1,
      anon_sym_LF,
    ACTIONS(483), 1,
      aux_sym_path_token3,
    ACTIONS(485), 1,
      anon_sym_DOLLAR2,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    STATE(81), 3,
      sym__immediate_expansion,
      sym__imm_expansion,
      aux_sym_path_repeat1,
  [2010] = 5,
    ACTIONS(487), 1,
      anon_sym_LF,
    ACTIONS(489), 1,
      aux_sym__stopsignal_value_token2,
    ACTIONS(492), 1,
      anon_sym_DOLLAR2,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    STATE(77), 3,
      sym__immediate_expansion,
      sym__imm_expansion,
      aux_sym__stopsignal_value_repeat1,
  [2029] = 3,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    ACTIONS(481), 2,
      aux_sym_from_instruction_token2,
      aux_sym_image_name_token2,
    ACTIONS(479), 4,
      anon_sym_LF,
      anon_sym_COLON,
      anon_sym_DOLLAR2,
      anon_sym_AT,
  [2044] = 5,
    ACTIONS(495), 1,
      anon_sym_LF,
    ACTIONS(497), 1,
      anon_sym_DOLLAR2,
    ACTIONS(499), 1,
      aux_sym_image_alias_token2,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    STATE(85), 3,
      sym__immediate_expansion,
      sym__imm_expansion,
      aux_sym_image_alias_repeat1,
  [2063] = 3,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    ACTIONS(503), 2,
      aux_sym_from_instruction_token2,
      aux_sym_image_name_token2,
    ACTIONS(501), 4,
      anon_sym_LF,
      anon_sym_COLON,
      anon_sym_DOLLAR2,
      anon_sym_AT,
  [2078] = 5,
    ACTIONS(467), 1,
      anon_sym_LF,
    ACTIONS(485), 1,
      anon_sym_DOLLAR2,
    ACTIONS(505), 1,
      aux_sym_path_token3,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    STATE(86), 3,
      sym__immediate_expansion,
      sym__imm_expansion,
      aux_sym_path_repeat1,
  [2097] = 5,
    ACTIONS(358), 1,
      sym__non_newline_whitespace,
    ACTIONS(507), 1,
      aux_sym_path_token3,
    ACTIONS(510), 1,
      anon_sym_DOLLAR2,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    STATE(82), 3,
      sym__immediate_expansion,
      sym__imm_expansion,
      aux_sym_path_repeat1,
  [2116] = 7,
    ACTIONS(513), 1,
      anon_sym_LF,
    ACTIONS(515), 1,
      aux_sym_from_instruction_token2,
    ACTIONS(517), 1,
      anon_sym_COLON,
    ACTIONS(519), 1,
      anon_sym_AT,
    STATE(143), 1,
      sym_image_tag,
    STATE(245), 1,
      sym_image_digest,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
  [2139] = 5,
    ACTIONS(328), 1,
      sym__non_newline_whitespace,
    ACTIONS(521), 1,
      aux_sym_path_token3,
    ACTIONS(523), 1,
      anon_sym_DOLLAR2,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    STATE(93), 3,
      sym__immediate_expansion,
      sym__imm_expansion,
      aux_sym_path_repeat1,
  [2158] = 5,
    ACTIONS(525), 1,
      anon_sym_LF,
    ACTIONS(527), 1,
      anon_sym_DOLLAR2,
    ACTIONS(530), 1,
      aux_sym_image_alias_token2,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    STATE(85), 3,
      sym__immediate_expansion,
      sym__imm_expansion,
      aux_sym_image_alias_repeat1,
  [2177] = 5,
    ACTIONS(358), 1,
      anon_sym_LF,
    ACTIONS(533), 1,
      aux_sym_path_token3,
    ACTIONS(536), 1,
      anon_sym_DOLLAR2,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    STATE(86), 3,
      sym__immediate_expansion,
      sym__imm_expansion,
      aux_sym_path_repeat1,
  [2196] = 5,
    ACTIONS(539), 1,
      anon_sym_LF,
    ACTIONS(541), 1,
      anon_sym_DOLLAR,
    ACTIONS(543), 1,
      aux_sym_expose_port_token1,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    STATE(90), 3,
      sym_expansion,
      sym_expose_port,
      aux_sym_expose_instruction_repeat1,
  [2215] = 6,
    ACTIONS(13), 1,
      aux_sym_cmd_instruction_token1,
    ACTIONS(545), 1,
      anon_sym_NONE,
    ACTIONS(547), 1,
      anon_sym_DASH_DASH,
    STATE(257), 1,
      sym_cmd_instruction,
    ACTIONS(3), 2,
      sym_line_continuation,
      sym_comment,
    STATE(97), 2,
      sym_param,
      aux_sym_add_instruction_repeat1,
  [2236] = 5,
    ACTIONS(497), 1,
      anon_sym_DOLLAR2,
    ACTIONS(549), 1,
      anon_sym_LF,
    ACTIONS(551), 1,
      aux_sym_image_alias_token2,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    STATE(79), 3,
      sym__immediate_expansion,
      sym__imm_expansion,
      aux_sym_image_alias_repeat1,
  [2255] = 5,
    ACTIONS(553), 1,
      anon_sym_LF,
    ACTIONS(555), 1,
      anon_sym_DOLLAR,
    ACTIONS(558), 1,
      aux_sym_expose_port_token1,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    STATE(90), 3,
      sym_expansion,
      sym_expose_port,
      aux_sym_expose_instruction_repeat1,
  [2274] = 5,
    ACTIONS(561), 1,
      anon_sym_LF,
    ACTIONS(563), 1,
      aux_sym__stopsignal_value_token2,
    ACTIONS(565), 1,
      anon_sym_DOLLAR2,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    STATE(94), 3,
      sym__immediate_expansion,
      sym__imm_expansion,
      aux_sym__stopsignal_value_repeat1,
  [2293] = 5,
    ACTIONS(254), 1,
      anon_sym_DOLLAR2,
    ACTIONS(406), 1,
      aux_sym__immediate_user_name_or_group_fragment_token1,
    STATE(57), 1,
      aux_sym__immediate_user_name_or_group,
    ACTIONS(3), 2,
      sym_line_continuation,
      sym_comment,
    STATE(164), 3,
      sym__immediate_user_name_or_group_fragment,
      sym__immediate_expansion,
      sym__imm_expansion,
  [2312] = 5,
    ACTIONS(340), 1,
      sym__non_newline_whitespace,
    ACTIONS(523), 1,
      anon_sym_DOLLAR2,
    ACTIONS(567), 1,
      aux_sym_path_token3,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    STATE(82), 3,
      sym__immediate_expansion,
      sym__imm_expansion,
      aux_sym_path_repeat1,
  [2331] = 5,
    ACTIONS(565), 1,
      anon_sym_DOLLAR2,
    ACTIONS(569), 1,
      anon_sym_LF,
    ACTIONS(571), 1,
      aux_sym__stopsignal_value_token2,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    STATE(77), 3,
      sym__immediate_expansion,
      sym__imm_expansion,
      aux_sym__stopsignal_value_repeat1,
  [2350] = 3,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    ACTIONS(477), 2,
      aux_sym_from_instruction_token2,
      aux_sym_image_tag_token1,
    ACTIONS(475), 3,
      anon_sym_LF,
      anon_sym_DOLLAR2,
      anon_sym_AT,
  [2364] = 5,
    ACTIONS(573), 1,
      anon_sym_LF,
    ACTIONS(575), 1,
      aux_sym__env_key_token1,
    STATE(271), 1,
      sym__env_key,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    STATE(96), 2,
      sym_env_pair,
      aux_sym_env_instruction_repeat1,
  [2382] = 5,
    ACTIONS(13), 1,
      aux_sym_cmd_instruction_token1,
    ACTIONS(547), 1,
      anon_sym_DASH_DASH,
    STATE(282), 1,
      sym_cmd_instruction,
    ACTIONS(3), 2,
      sym_line_continuation,
      sym_comment,
    STATE(153), 2,
      sym_param,
      aux_sym_add_instruction_repeat1,
  [2400] = 5,
    ACTIONS(578), 1,
      anon_sym_LF,
    ACTIONS(580), 1,
      sym__non_newline_whitespace,
    ACTIONS(582), 1,
      sym_heredoc_nl,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    STATE(154), 2,
      sym_heredoc_block,
      aux_sym_run_instruction_repeat2,
  [2418] = 4,
    ACTIONS(584), 1,
      anon_sym_LF,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    ACTIONS(586), 2,
      anon_sym_DOLLAR,
      aux_sym_expose_port_token1,
    ACTIONS(588), 2,
      anon_sym_SLASHtcp,
      anon_sym_SLASHudp,
  [2434] = 5,
    ACTIONS(580), 1,
      sym__non_newline_whitespace,
    ACTIONS(582), 1,
      sym_heredoc_nl,
    ACTIONS(590), 1,
      anon_sym_LF,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    STATE(158), 2,
      sym_heredoc_block,
      aux_sym_run_instruction_repeat2,
  [2452] = 7,
    ACTIONS(3), 1,
      sym_line_continuation,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(382), 1,
      aux_sym_path_token1,
    ACTIONS(384), 1,
      aux_sym_path_token2,
    ACTIONS(386), 1,
      anon_sym_DOLLAR,
    STATE(60), 1,
      sym_expansion,
    STATE(241), 1,
      sym_path,
  [2474] = 5,
    ACTIONS(592), 1,
      aux_sym__env_key_token1,
    STATE(242), 1,
      sym__env_key,
    STATE(278), 1,
      sym__spaced_env_pair,
    ACTIONS(3), 2,
      sym_line_continuation,
      sym_comment,
    STATE(103), 2,
      sym_env_pair,
      aux_sym_env_instruction_repeat1,
  [2492] = 5,
    ACTIONS(594), 1,
      anon_sym_LF,
    ACTIONS(596), 1,
      aux_sym__env_key_token1,
    STATE(271), 1,
      sym__env_key,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    STATE(96), 2,
      sym_env_pair,
      aux_sym_env_instruction_repeat1,
  [2510] = 4,
    ACTIONS(598), 1,
      anon_sym_DOLLAR,
    ACTIONS(600), 1,
      aux_sym_expose_port_token1,
    ACTIONS(3), 2,
      sym_line_continuation,
      sym_comment,
    STATE(87), 3,
      sym_expansion,
      sym_expose_port,
      aux_sym_expose_instruction_repeat1,
  [2526] = 7,
    ACTIONS(3), 1,
      sym_line_continuation,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(469), 1,
      anon_sym_DOLLAR,
    ACTIONS(471), 1,
      aux_sym_image_name_token1,
    STATE(21), 1,
      sym_expansion,
    STATE(83), 1,
      sym_image_name,
    STATE(246), 1,
      sym_image_spec,
  [2548] = 5,
    ACTIONS(604), 1,
      anon_sym_SQUOTE,
    ACTIONS(606), 1,
      sym_single_quoted_escape_sequence,
    STATE(116), 1,
      aux_sym_single_quoted_string_repeat1,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    ACTIONS(602), 2,
      anon_sym_BSLASH,
      aux_sym_single_quoted_string_token1,
  [2566] = 5,
    ACTIONS(3), 1,
      sym_line_continuation,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(238), 1,
      anon_sym_DOLLAR2,
    ACTIONS(608), 1,
      aux_sym_image_tag_token1,
    STATE(29), 3,
      sym__immediate_expansion,
      sym__imm_expansion,
      aux_sym_image_tag_repeat1,
  [2584] = 4,
    ACTIONS(392), 1,
      anon_sym_DOLLAR2,
    ACTIONS(610), 1,
      aux_sym_image_digest_token1,
    ACTIONS(3), 2,
      sym_line_continuation,
      sym_comment,
    STATE(55), 3,
      sym__immediate_expansion,
      sym__imm_expansion,
      aux_sym_image_digest_repeat1,
  [2600] = 3,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    ACTIONS(475), 2,
      anon_sym_DOLLAR2,
      sym_double_quoted_escape_sequence,
    ACTIONS(477), 3,
      anon_sym_DQUOTE,
      aux_sym_double_quoted_string_token1,
      anon_sym_BSLASH,
  [2614] = 7,
    ACTIONS(3), 1,
      sym_line_continuation,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(612), 1,
      aux_sym_path_token1,
    ACTIONS(614), 1,
      aux_sym_path_token2,
    ACTIONS(616), 1,
      anon_sym_DOLLAR,
    STATE(76), 1,
      sym_expansion,
    STATE(250), 1,
      sym_path,
  [2636] = 4,
    ACTIONS(3), 1,
      sym_line_continuation,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(459), 1,
      aux_sym_path_token1,
    ACTIONS(461), 4,
      sym_heredoc_marker,
      aux_sym_path_with_heredoc_token1,
      anon_sym_DOLLAR,
      anon_sym_DASH_DASH,
  [2652] = 5,
    ACTIONS(620), 1,
      anon_sym_SQUOTE,
    ACTIONS(622), 1,
      sym_single_quoted_escape_sequence,
    STATE(106), 1,
      aux_sym_single_quoted_string_repeat1,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    ACTIONS(618), 2,
      anon_sym_BSLASH,
      aux_sym_single_quoted_string_token1,
  [2670] = 3,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    ACTIONS(503), 2,
      anon_sym_LF,
      aux_sym_path_token3,
    ACTIONS(501), 3,
      sym_heredoc_nl,
      anon_sym_DOLLAR2,
      sym__non_newline_whitespace,
  [2684] = 3,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    ACTIONS(479), 2,
      anon_sym_DOLLAR2,
      sym_double_quoted_escape_sequence,
    ACTIONS(481), 3,
      anon_sym_DQUOTE,
      aux_sym_double_quoted_string_token1,
      anon_sym_BSLASH,
  [2698] = 3,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    ACTIONS(481), 2,
      anon_sym_LF,
      aux_sym_path_token3,
    ACTIONS(479), 3,
      sym_heredoc_nl,
      anon_sym_DOLLAR2,
      sym__non_newline_whitespace,
  [2712] = 5,
    ACTIONS(627), 1,
      anon_sym_SQUOTE,
    ACTIONS(629), 1,
      sym_single_quoted_escape_sequence,
    STATE(116), 1,
      aux_sym_single_quoted_string_repeat1,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    ACTIONS(624), 2,
      anon_sym_BSLASH,
      aux_sym_single_quoted_string_token1,
  [2730] = 3,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    ACTIONS(475), 2,
      anon_sym_LF,
      anon_sym_DOLLAR2,
    ACTIONS(477), 3,
      aux_sym__env_key_token1,
      aux_sym_unquoted_string_token1,
      anon_sym_BSLASH2,
  [2744] = 3,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    ACTIONS(481), 2,
      aux_sym_from_instruction_token2,
      aux_sym_image_tag_token1,
    ACTIONS(479), 3,
      anon_sym_LF,
      anon_sym_DOLLAR2,
      anon_sym_AT,
  [2758] = 3,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    ACTIONS(477), 2,
      anon_sym_LF,
      aux_sym_path_token3,
    ACTIONS(475), 3,
      sym_heredoc_nl,
      anon_sym_DOLLAR2,
      sym__non_newline_whitespace,
  [2772] = 3,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    ACTIONS(479), 2,
      anon_sym_LF,
      anon_sym_DOLLAR2,
    ACTIONS(481), 3,
      aux_sym__env_key_token1,
      aux_sym_unquoted_string_token1,
      anon_sym_BSLASH2,
  [2786] = 5,
    ACTIONS(580), 1,
      sym__non_newline_whitespace,
    ACTIONS(582), 1,
      sym_heredoc_nl,
    ACTIONS(632), 1,
      anon_sym_LF,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    STATE(142), 2,
      sym_heredoc_block,
      aux_sym_run_instruction_repeat2,
  [2804] = 5,
    ACTIONS(636), 1,
      anon_sym_SQUOTE,
    ACTIONS(638), 1,
      sym_single_quoted_escape_sequence,
    STATE(123), 1,
      aux_sym_single_quoted_string_repeat1,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    ACTIONS(634), 2,
      anon_sym_BSLASH,
      aux_sym_single_quoted_string_token1,
  [2822] = 5,
    ACTIONS(606), 1,
      sym_single_quoted_escape_sequence,
    ACTIONS(640), 1,
      anon_sym_SQUOTE,
    STATE(116), 1,
      aux_sym_single_quoted_string_repeat1,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    ACTIONS(602), 2,
      anon_sym_BSLASH,
      aux_sym_single_quoted_string_token1,
  [2840] = 5,
    ACTIONS(580), 1,
      sym__non_newline_whitespace,
    ACTIONS(582), 1,
      sym_heredoc_nl,
    ACTIONS(642), 1,
      anon_sym_LF,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    STATE(146), 2,
      sym_heredoc_block,
      aux_sym_run_instruction_repeat2,
  [2858] = 5,
    ACTIONS(646), 1,
      anon_sym_SQUOTE,
    ACTIONS(648), 1,
      sym_single_quoted_escape_sequence,
    STATE(126), 1,
      aux_sym_single_quoted_string_repeat1,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    ACTIONS(644), 2,
      anon_sym_BSLASH,
      aux_sym_single_quoted_string_token1,
  [2876] = 5,
    ACTIONS(606), 1,
      sym_single_quoted_escape_sequence,
    ACTIONS(650), 1,
      anon_sym_SQUOTE,
    STATE(116), 1,
      aux_sym_single_quoted_string_repeat1,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    ACTIONS(602), 2,
      anon_sym_BSLASH,
      aux_sym_single_quoted_string_token1,
  [2894] = 3,
    ACTIONS(652), 1,
      anon_sym_LF,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    ACTIONS(654), 3,
      aux_sym_label_pair_token1,
      anon_sym_DQUOTE,
      anon_sym_SQUOTE,
  [2907] = 4,
    ACTIONS(656), 1,
      anon_sym_LF,
    ACTIONS(658), 1,
      sym_heredoc_nl,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    STATE(128), 2,
      sym_heredoc_block,
      aux_sym_run_instruction_repeat2,
  [2922] = 4,
    ACTIONS(663), 1,
      sym_required_line_continuation,
    STATE(129), 1,
      aux_sym_shell_command_repeat1,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    ACTIONS(661), 2,
      sym_heredoc_nl,
      anon_sym_LF,
  [2937] = 4,
    ACTIONS(582), 1,
      sym_heredoc_nl,
    ACTIONS(666), 1,
      anon_sym_LF,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    STATE(128), 2,
      sym_heredoc_block,
      aux_sym_run_instruction_repeat2,
  [2952] = 4,
    ACTIONS(582), 1,
      sym_heredoc_nl,
    ACTIONS(668), 1,
      anon_sym_LF,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    STATE(130), 2,
      sym_heredoc_block,
      aux_sym_run_instruction_repeat2,
  [2967] = 5,
    ACTIONS(670), 1,
      aux_sym__stopsignal_value_token1,
    ACTIONS(672), 1,
      anon_sym_DOLLAR,
    STATE(91), 1,
      sym_expansion,
    STATE(256), 1,
      sym__stopsignal_value,
    ACTIONS(3), 2,
      sym_line_continuation,
      sym_comment,
  [2984] = 3,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    ACTIONS(501), 2,
      anon_sym_DOLLAR2,
      sym__non_newline_whitespace,
    ACTIONS(503), 2,
      anon_sym_LF,
      aux_sym_path_token3,
  [2997] = 3,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    ACTIONS(475), 2,
      anon_sym_DOLLAR2,
      sym__non_newline_whitespace,
    ACTIONS(477), 2,
      anon_sym_LF,
      aux_sym_path_token3,
  [3010] = 5,
    ACTIONS(674), 1,
      anon_sym_DQUOTE,
    ACTIONS(676), 1,
      aux_sym_json_string_token1,
    ACTIONS(679), 1,
      sym_json_escape_sequence,
    STATE(135), 1,
      aux_sym_json_string_repeat1,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
  [3027] = 3,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    ACTIONS(479), 2,
      anon_sym_DOLLAR2,
      sym__non_newline_whitespace,
    ACTIONS(481), 2,
      anon_sym_LF,
      aux_sym_path_token3,
  [3040] = 2,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    ACTIONS(501), 4,
      anon_sym_LF,
      anon_sym_COLON,
      aux_sym__immediate_user_name_or_group_fragment_token1,
      anon_sym_DOLLAR2,
  [3051] = 2,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    ACTIONS(475), 4,
      anon_sym_LF,
      anon_sym_COLON,
      aux_sym__immediate_user_name_or_group_fragment_token1,
      anon_sym_DOLLAR2,
  [3062] = 2,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    ACTIONS(479), 4,
      anon_sym_LF,
      anon_sym_COLON,
      aux_sym__immediate_user_name_or_group_fragment_token1,
      anon_sym_DOLLAR2,
  [3073] = 5,
    ACTIONS(682), 1,
      anon_sym_DQUOTE,
    ACTIONS(684), 1,
      aux_sym_json_string_token1,
    ACTIONS(686), 1,
      sym_json_escape_sequence,
    STATE(157), 1,
      aux_sym_json_string_repeat1,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
  [3090] = 4,
    ACTIONS(582), 1,
      sym_heredoc_nl,
    ACTIONS(688), 1,
      anon_sym_LF,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    STATE(156), 2,
      sym_heredoc_block,
      aux_sym_run_instruction_repeat2,
  [3105] = 4,
    ACTIONS(582), 1,
      sym_heredoc_nl,
    ACTIONS(690), 1,
      anon_sym_LF,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    STATE(128), 2,
      sym_heredoc_block,
      aux_sym_run_instruction_repeat2,
  [3120] = 5,
    ACTIONS(519), 1,
      anon_sym_AT,
    ACTIONS(692), 1,
      anon_sym_LF,
    ACTIONS(694), 1,
      aux_sym_from_instruction_token2,
    STATE(219), 1,
      sym_image_digest,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
  [3137] = 5,
    ACTIONS(696), 1,
      anon_sym_DOLLAR,
    ACTIONS(698), 1,
      aux_sym_image_alias_token1,
    STATE(89), 1,
      sym_expansion,
    STATE(295), 1,
      sym_image_alias,
    ACTIONS(3), 2,
      sym_line_continuation,
      sym_comment,
  [3154] = 5,
    ACTIONS(700), 1,
      aux_sym__user_name_or_group_token1,
    ACTIONS(702), 1,
      anon_sym_DOLLAR,
    STATE(31), 1,
      sym_expansion,
    STATE(247), 1,
      sym__user_name_or_group,
    ACTIONS(3), 2,
      sym_line_continuation,
      sym_comment,
  [3171] = 4,
    ACTIONS(582), 1,
      sym_heredoc_nl,
    ACTIONS(704), 1,
      anon_sym_LF,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    STATE(128), 2,
      sym_heredoc_block,
      aux_sym_run_instruction_repeat2,
  [3186] = 4,
    ACTIONS(3), 1,
      sym_line_continuation,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(706), 1,
      aux_sym_path_token1,
    ACTIONS(708), 3,
      sym_heredoc_marker,
      aux_sym_path_with_heredoc_token1,
      anon_sym_DOLLAR,
  [3201] = 3,
    ACTIONS(710), 1,
      anon_sym_LF,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    ACTIONS(712), 3,
      aux_sym_label_pair_token1,
      anon_sym_DQUOTE,
      anon_sym_SQUOTE,
  [3214] = 3,
    ACTIONS(481), 1,
      aux_sym_from_instruction_token2,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    ACTIONS(479), 3,
      anon_sym_LF,
      anon_sym_DOLLAR2,
      aux_sym_image_digest_token1,
  [3227] = 4,
    ACTIONS(716), 1,
      sym_required_line_continuation,
    STATE(152), 1,
      aux_sym_shell_command_repeat1,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    ACTIONS(714), 2,
      sym_heredoc_nl,
      anon_sym_LF,
  [3242] = 3,
    ACTIONS(718), 1,
      anon_sym_LF,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    ACTIONS(720), 3,
      aux_sym_label_pair_token1,
      anon_sym_DQUOTE,
      anon_sym_SQUOTE,
  [3255] = 4,
    ACTIONS(716), 1,
      sym_required_line_continuation,
    STATE(129), 1,
      aux_sym_shell_command_repeat1,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    ACTIONS(722), 2,
      sym_heredoc_nl,
      anon_sym_LF,
  [3270] = 4,
    ACTIONS(436), 1,
      aux_sym_cmd_instruction_token1,
    ACTIONS(724), 1,
      anon_sym_DASH_DASH,
    ACTIONS(3), 2,
      sym_line_continuation,
      sym_comment,
    STATE(153), 2,
      sym_param,
      aux_sym_add_instruction_repeat1,
  [3285] = 4,
    ACTIONS(582), 1,
      sym_heredoc_nl,
    ACTIONS(727), 1,
      anon_sym_LF,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    STATE(128), 2,
      sym_heredoc_block,
      aux_sym_run_instruction_repeat2,
  [3300] = 5,
    ACTIONS(696), 1,
      anon_sym_DOLLAR,
    ACTIONS(698), 1,
      aux_sym_image_alias_token1,
    STATE(89), 1,
      sym_expansion,
    STATE(261), 1,
      sym_image_alias,
    ACTIONS(3), 2,
      sym_line_continuation,
      sym_comment,
  [3317] = 4,
    ACTIONS(582), 1,
      sym_heredoc_nl,
    ACTIONS(668), 1,
      anon_sym_LF,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    STATE(128), 2,
      sym_heredoc_block,
      aux_sym_run_instruction_repeat2,
  [3332] = 5,
    ACTIONS(729), 1,
      anon_sym_DQUOTE,
    ACTIONS(731), 1,
      aux_sym_json_string_token1,
    ACTIONS(733), 1,
      sym_json_escape_sequence,
    STATE(135), 1,
      aux_sym_json_string_repeat1,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
  [3349] = 4,
    ACTIONS(582), 1,
      sym_heredoc_nl,
    ACTIONS(735), 1,
      anon_sym_LF,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    STATE(128), 2,
      sym_heredoc_block,
      aux_sym_run_instruction_repeat2,
  [3364] = 3,
    ACTIONS(737), 1,
      anon_sym_LF,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    ACTIONS(739), 3,
      aux_sym_label_pair_token1,
      anon_sym_DQUOTE,
      anon_sym_SQUOTE,
  [3377] = 3,
    ACTIONS(741), 1,
      anon_sym_LF,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    ACTIONS(743), 3,
      aux_sym_label_pair_token1,
      anon_sym_DQUOTE,
      anon_sym_SQUOTE,
  [3390] = 3,
    ACTIONS(477), 1,
      aux_sym_from_instruction_token2,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    ACTIONS(475), 3,
      anon_sym_LF,
      anon_sym_DOLLAR2,
      aux_sym_image_digest_token1,
  [3403] = 4,
    ACTIONS(745), 1,
      anon_sym_LBRACE,
    ACTIONS(747), 1,
      sym_variable,
    STATE(133), 1,
      sym__expansion_body,
    ACTIONS(3), 2,
      sym_line_continuation,
      sym_comment,
  [3417] = 3,
    ACTIONS(749), 1,
      anon_sym_LF,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    ACTIONS(751), 2,
      anon_sym_DOLLAR,
      aux_sym_expose_port_token1,
  [3429] = 2,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    ACTIONS(753), 3,
      anon_sym_LF,
      aux_sym__immediate_user_name_or_group_fragment_token1,
      anon_sym_DOLLAR2,
  [3439] = 4,
    ACTIONS(755), 1,
      anon_sym_COMMA2,
    ACTIONS(758), 1,
      anon_sym_RBRACK,
    STATE(165), 1,
      aux_sym_json_string_array_repeat1,
    ACTIONS(3), 2,
      sym_line_continuation,
      sym_comment,
  [3453] = 4,
    ACTIONS(760), 1,
      sym_heredoc_line,
    ACTIONS(763), 1,
      sym_heredoc_end,
    STATE(166), 1,
      aux_sym_heredoc_block_repeat1,
    ACTIONS(3), 2,
      sym_line_continuation,
      sym_comment,
  [3467] = 4,
    ACTIONS(714), 1,
      anon_sym_LF,
    ACTIONS(765), 1,
      sym_required_line_continuation,
    STATE(173), 1,
      aux_sym_shell_command_repeat1,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
  [3481] = 3,
    ACTIONS(322), 1,
      anon_sym_LF,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    ACTIONS(328), 2,
      sym_heredoc_nl,
      sym__non_newline_whitespace,
  [3493] = 4,
    ACTIONS(767), 1,
      anon_sym_LF,
    ACTIONS(769), 1,
      sym__non_newline_whitespace,
    STATE(202), 1,
      aux_sym_volume_instruction_repeat1,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
  [3507] = 3,
    ACTIONS(501), 1,
      anon_sym_LF,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    ACTIONS(503), 2,
      anon_sym_DOLLAR,
      aux_sym_expose_port_token1,
  [3519] = 4,
    ACTIONS(771), 1,
      anon_sym_LBRACE,
    ACTIONS(773), 1,
      sym_variable,
    STATE(109), 1,
      sym__expansion_body,
    ACTIONS(3), 2,
      sym_line_continuation,
      sym_comment,
  [3533] = 4,
    ACTIONS(775), 1,
      anon_sym_LBRACE,
    ACTIONS(777), 1,
      sym_variable,
    STATE(75), 1,
      sym__expansion_body,
    ACTIONS(3), 2,
      sym_line_continuation,
      sym_comment,
  [3547] = 4,
    ACTIONS(722), 1,
      anon_sym_LF,
    ACTIONS(765), 1,
      sym_required_line_continuation,
    STATE(174), 1,
      aux_sym_shell_command_repeat1,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
  [3561] = 4,
    ACTIONS(661), 1,
      anon_sym_LF,
    ACTIONS(779), 1,
      sym_required_line_continuation,
    STATE(174), 1,
      aux_sym_shell_command_repeat1,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
  [3575] = 3,
    ACTIONS(479), 1,
      anon_sym_LF,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    ACTIONS(481), 2,
      anon_sym_DOLLAR,
      aux_sym_expose_port_token1,
  [3587] = 3,
    ACTIONS(503), 1,
      aux_sym_path_token3,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    ACTIONS(501), 2,
      anon_sym_DOLLAR2,
      sym__non_newline_whitespace,
  [3599] = 3,
    ACTIONS(477), 1,
      aux_sym_path_token3,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    ACTIONS(475), 2,
      anon_sym_DOLLAR2,
      sym__non_newline_whitespace,
  [3611] = 3,
    ACTIONS(481), 1,
      aux_sym_path_token3,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    ACTIONS(479), 2,
      anon_sym_DOLLAR2,
      sym__non_newline_whitespace,
  [3623] = 4,
    ACTIONS(782), 1,
      anon_sym_COMMA2,
    ACTIONS(784), 1,
      anon_sym_RBRACK,
    STATE(165), 1,
      aux_sym_json_string_array_repeat1,
    ACTIONS(3), 2,
      sym_line_continuation,
      sym_comment,
  [3637] = 4,
    ACTIONS(786), 1,
      sym_heredoc_line,
    ACTIONS(788), 1,
      sym_heredoc_end,
    STATE(166), 1,
      aux_sym_heredoc_block_repeat1,
    ACTIONS(3), 2,
      sym_line_continuation,
      sym_comment,
  [3651] = 3,
    ACTIONS(503), 1,
      aux_sym_path_token3,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    ACTIONS(501), 2,
      anon_sym_LF,
      anon_sym_DOLLAR2,
  [3663] = 3,
    ACTIONS(477), 1,
      aux_sym_path_token3,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    ACTIONS(475), 2,
      anon_sym_LF,
      anon_sym_DOLLAR2,
  [3675] = 3,
    ACTIONS(481), 1,
      aux_sym_path_token3,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    ACTIONS(479), 2,
      anon_sym_LF,
      anon_sym_DOLLAR2,
  [3687] = 2,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    ACTIONS(501), 3,
      anon_sym_LF,
      aux_sym__stopsignal_value_token2,
      anon_sym_DOLLAR2,
  [3697] = 2,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    ACTIONS(475), 3,
      anon_sym_LF,
      aux_sym__stopsignal_value_token2,
      anon_sym_DOLLAR2,
  [3707] = 4,
    ACTIONS(790), 1,
      anon_sym_LBRACE,
    ACTIONS(792), 1,
      sym_variable,
    STATE(193), 1,
      sym__expansion_body,
    ACTIONS(3), 2,
      sym_line_continuation,
      sym_comment,
  [3721] = 3,
    ACTIONS(794), 1,
      sym_required_line_continuation,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    ACTIONS(661), 2,
      sym_heredoc_nl,
      anon_sym_LF,
  [3733] = 2,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    ACTIONS(501), 3,
      anon_sym_LF,
      anon_sym_DOLLAR2,
      aux_sym_image_alias_token2,
  [3743] = 4,
    ACTIONS(782), 1,
      anon_sym_COMMA2,
    ACTIONS(796), 1,
      anon_sym_RBRACK,
    STATE(179), 1,
      aux_sym_json_string_array_repeat1,
    ACTIONS(3), 2,
      sym_line_continuation,
      sym_comment,
  [3757] = 4,
    ACTIONS(786), 1,
      sym_heredoc_line,
    ACTIONS(798), 1,
      sym_heredoc_end,
    STATE(180), 1,
      aux_sym_heredoc_block_repeat1,
    ACTIONS(3), 2,
      sym_line_continuation,
      sym_comment,
  [3771] = 4,
    ACTIONS(800), 1,
      anon_sym_RBRACK,
    ACTIONS(802), 1,
      anon_sym_DQUOTE,
    STATE(189), 1,
      sym_json_string,
    ACTIONS(3), 2,
      sym_line_continuation,
      sym_comment,
  [3785] = 2,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    ACTIONS(479), 3,
      anon_sym_LF,
      anon_sym_DOLLAR2,
      aux_sym_image_alias_token2,
  [3795] = 2,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    ACTIONS(475), 3,
      anon_sym_LF,
      anon_sym_DOLLAR2,
      aux_sym_image_alias_token2,
  [3805] = 4,
    ACTIONS(804), 1,
      anon_sym_LBRACE,
    ACTIONS(806), 1,
      sym_variable,
    STATE(170), 1,
      sym__expansion_body,
    ACTIONS(3), 2,
      sym_line_continuation,
      sym_comment,
  [3819] = 4,
    ACTIONS(802), 1,
      anon_sym_DQUOTE,
    ACTIONS(808), 1,
      anon_sym_RBRACK,
    STATE(196), 1,
      sym_json_string,
    ACTIONS(3), 2,
      sym_line_continuation,
      sym_comment,
  [3833] = 4,
    ACTIONS(782), 1,
      anon_sym_COMMA2,
    ACTIONS(810), 1,
      anon_sym_RBRACK,
    STATE(199), 1,
      aux_sym_json_string_array_repeat1,
    ACTIONS(3), 2,
      sym_line_continuation,
      sym_comment,
  [3847] = 4,
    ACTIONS(775), 1,
      anon_sym_LBRACE,
    ACTIONS(812), 1,
      sym_variable,
    STATE(80), 1,
      sym__expansion_body,
    ACTIONS(3), 2,
      sym_line_continuation,
      sym_comment,
  [3861] = 4,
    ACTIONS(769), 1,
      sym__non_newline_whitespace,
    ACTIONS(814), 1,
      anon_sym_LF,
    STATE(169), 1,
      aux_sym_volume_instruction_repeat1,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
  [3875] = 4,
    ACTIONS(782), 1,
      anon_sym_COMMA2,
    ACTIONS(816), 1,
      anon_sym_RBRACK,
    STATE(165), 1,
      aux_sym_json_string_array_repeat1,
    ACTIONS(3), 2,
      sym_line_continuation,
      sym_comment,
  [3889] = 4,
    ACTIONS(818), 1,
      anon_sym_LBRACE,
    ACTIONS(820), 1,
      sym_variable,
    STATE(177), 1,
      sym__expansion_body,
    ACTIONS(3), 2,
      sym_line_continuation,
      sym_comment,
  [3903] = 4,
    ACTIONS(818), 1,
      anon_sym_LBRACE,
    ACTIONS(822), 1,
      sym_variable,
    STATE(176), 1,
      sym__expansion_body,
    ACTIONS(3), 2,
      sym_line_continuation,
      sym_comment,
  [3917] = 4,
    ACTIONS(824), 1,
      anon_sym_LF,
    ACTIONS(826), 1,
      sym__non_newline_whitespace,
    STATE(202), 1,
      aux_sym_volume_instruction_repeat1,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
  [3931] = 4,
    ACTIONS(745), 1,
      anon_sym_LBRACE,
    ACTIONS(829), 1,
      sym_variable,
    STATE(134), 1,
      sym__expansion_body,
    ACTIONS(3), 2,
      sym_line_continuation,
      sym_comment,
  [3945] = 4,
    ACTIONS(831), 1,
      anon_sym_LBRACE,
    ACTIONS(833), 1,
      sym_variable,
    STATE(138), 1,
      sym__expansion_body,
    ACTIONS(3), 2,
      sym_line_continuation,
      sym_comment,
  [3959] = 4,
    ACTIONS(831), 1,
      anon_sym_LBRACE,
    ACTIONS(835), 1,
      sym_variable,
    STATE(137), 1,
      sym__expansion_body,
    ACTIONS(3), 2,
      sym_line_continuation,
      sym_comment,
  [3973] = 4,
    ACTIONS(837), 1,
      anon_sym_LBRACE,
    ACTIONS(839), 1,
      sym_variable,
    STATE(182), 1,
      sym__expansion_body,
    ACTIONS(3), 2,
      sym_line_continuation,
      sym_comment,
  [3987] = 4,
    ACTIONS(837), 1,
      anon_sym_LBRACE,
    ACTIONS(841), 1,
      sym_variable,
    STATE(181), 1,
      sym__expansion_body,
    ACTIONS(3), 2,
      sym_line_continuation,
      sym_comment,
  [4001] = 4,
    ACTIONS(843), 1,
      anon_sym_LBRACE,
    ACTIONS(845), 1,
      sym_variable,
    STATE(185), 1,
      sym__expansion_body,
    ACTIONS(3), 2,
      sym_line_continuation,
      sym_comment,
  [4015] = 4,
    ACTIONS(843), 1,
      anon_sym_LBRACE,
    ACTIONS(847), 1,
      sym_variable,
    STATE(184), 1,
      sym__expansion_body,
    ACTIONS(3), 2,
      sym_line_continuation,
      sym_comment,
  [4029] = 4,
    ACTIONS(849), 1,
      anon_sym_LBRACE,
    ACTIONS(851), 1,
      sym_variable,
    STATE(95), 1,
      sym__expansion_body,
    ACTIONS(3), 2,
      sym_line_continuation,
      sym_comment,
  [4043] = 4,
    ACTIONS(853), 1,
      anon_sym_LBRACE,
    ACTIONS(855), 1,
      sym_variable,
    STATE(113), 1,
      sym__expansion_body,
    ACTIONS(3), 2,
      sym_line_continuation,
      sym_comment,
  [4057] = 4,
    ACTIONS(857), 1,
      anon_sym_LBRACE,
    ACTIONS(859), 1,
      sym_variable,
    STATE(161), 1,
      sym__expansion_body,
    ACTIONS(3), 2,
      sym_line_continuation,
      sym_comment,
  [4071] = 4,
    ACTIONS(790), 1,
      anon_sym_LBRACE,
    ACTIONS(861), 1,
      sym_variable,
    STATE(188), 1,
      sym__expansion_body,
    ACTIONS(3), 2,
      sym_line_continuation,
      sym_comment,
  [4085] = 4,
    ACTIONS(863), 1,
      anon_sym_LBRACE,
    ACTIONS(865), 1,
      sym_variable,
    STATE(72), 1,
      sym__expansion_body,
    ACTIONS(3), 2,
      sym_line_continuation,
      sym_comment,
  [4099] = 4,
    ACTIONS(867), 1,
      anon_sym_LBRACE,
    ACTIONS(869), 1,
      sym_variable,
    STATE(117), 1,
      sym__expansion_body,
    ACTIONS(3), 2,
      sym_line_continuation,
      sym_comment,
  [4113] = 4,
    ACTIONS(853), 1,
      anon_sym_LBRACE,
    ACTIONS(871), 1,
      sym_variable,
    STATE(119), 1,
      sym__expansion_body,
    ACTIONS(3), 2,
      sym_line_continuation,
      sym_comment,
  [4127] = 2,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    ACTIONS(479), 3,
      anon_sym_LF,
      aux_sym__stopsignal_value_token2,
      anon_sym_DOLLAR2,
  [4137] = 2,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    ACTIONS(873), 2,
      sym_heredoc_nl,
      anon_sym_LF,
  [4146] = 3,
    ACTIONS(875), 1,
      anon_sym_LF,
    ACTIONS(877), 1,
      aux_sym_from_instruction_token2,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
  [4157] = 3,
    ACTIONS(879), 1,
      aux_sym_param_token1,
    ACTIONS(881), 1,
      anon_sym_mount,
    ACTIONS(3), 2,
      sym_line_continuation,
      sym_comment,
  [4168] = 4,
    ACTIONS(3), 1,
      sym_line_continuation,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(459), 1,
      aux_sym_image_name_token1,
    ACTIONS(461), 1,
      anon_sym_DOLLAR,
  [4181] = 3,
    ACTIONS(710), 1,
      anon_sym_LF,
    ACTIONS(712), 1,
      aux_sym__env_key_token1,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
  [4192] = 3,
    ACTIONS(718), 1,
      anon_sym_LF,
    ACTIONS(720), 1,
      aux_sym__env_key_token1,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
  [4203] = 2,
    ACTIONS(3), 2,
      sym_line_continuation,
      sym_comment,
    ACTIONS(763), 2,
      sym_heredoc_line,
      sym_heredoc_end,
  [4212] = 3,
    ACTIONS(737), 1,
      anon_sym_LF,
    ACTIONS(739), 1,
      aux_sym__env_key_token1,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
  [4223] = 3,
    ACTIONS(741), 1,
      anon_sym_LF,
    ACTIONS(743), 1,
      aux_sym__env_key_token1,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
  [4234] = 4,
    ACTIONS(3), 1,
      sym_line_continuation,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(883), 1,
      aux_sym_mount_param_param_token1,
    STATE(26), 1,
      sym_mount_param_param,
  [4247] = 2,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    ACTIONS(885), 2,
      sym_heredoc_nl,
      anon_sym_LF,
  [4256] = 2,
    ACTIONS(3), 2,
      sym_line_continuation,
      sym_comment,
    ACTIONS(887), 2,
      anon_sym_COMMA2,
      anon_sym_RBRACK,
  [4265] = 3,
    ACTIONS(802), 1,
      anon_sym_DQUOTE,
    STATE(244), 1,
      sym_json_string,
    ACTIONS(3), 2,
      sym_line_continuation,
      sym_comment,
  [4276] = 2,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    ACTIONS(889), 2,
      sym_heredoc_nl,
      anon_sym_LF,
  [4285] = 2,
    ACTIONS(3), 2,
      sym_line_continuation,
      sym_comment,
    ACTIONS(461), 2,
      aux_sym_cmd_instruction_token1,
      anon_sym_DASH_DASH,
  [4294] = 2,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    ACTIONS(891), 2,
      sym_heredoc_nl,
      anon_sym_LF,
  [4303] = 3,
    ACTIONS(140), 1,
      anon_sym_LBRACK,
    STATE(273), 1,
      sym_json_string_array,
    ACTIONS(3), 2,
      sym_line_continuation,
      sym_comment,
  [4314] = 4,
    ACTIONS(3), 1,
      sym_line_continuation,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(883), 1,
      aux_sym_mount_param_param_token1,
    STATE(42), 1,
      sym_mount_param_param,
  [4327] = 2,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
    ACTIONS(893), 2,
      sym_heredoc_nl,
      anon_sym_LF,
  [4336] = 3,
    ACTIONS(895), 1,
      anon_sym_LF,
    ACTIONS(897), 1,
      aux_sym_from_instruction_token2,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
  [4347] = 3,
    ACTIONS(899), 1,
      anon_sym_LF,
    ACTIONS(901), 1,
      anon_sym_EQ,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
  [4358] = 3,
    ACTIONS(903), 1,
      anon_sym_LF,
    ACTIONS(905), 1,
      aux_sym__env_key_token1,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
  [4369] = 2,
    ACTIONS(3), 2,
      sym_line_continuation,
      sym_comment,
    ACTIONS(907), 2,
      anon_sym_COMMA2,
      anon_sym_RBRACK,
  [4378] = 3,
    ACTIONS(824), 1,
      anon_sym_LF,
    ACTIONS(909), 1,
      sym__non_newline_whitespace,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
  [4389] = 3,
    ACTIONS(911), 1,
      anon_sym_EQ,
    ACTIONS(913), 1,
      aux_sym__spaced_env_pair_token1,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
  [4400] = 3,
    ACTIONS(661), 1,
      anon_sym_LF,
    ACTIONS(794), 1,
      sym_required_line_continuation,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
  [4411] = 2,
    ACTIONS(3), 2,
      sym_line_continuation,
      sym_comment,
    ACTIONS(758), 2,
      anon_sym_COMMA2,
      anon_sym_RBRACK,
  [4420] = 3,
    ACTIONS(915), 1,
      anon_sym_LF,
    ACTIONS(917), 1,
      aux_sym_from_instruction_token2,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
  [4431] = 3,
    ACTIONS(919), 1,
      anon_sym_LF,
    ACTIONS(921), 1,
      aux_sym_from_instruction_token2,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
  [4442] = 3,
    ACTIONS(923), 1,
      anon_sym_LF,
    ACTIONS(925), 1,
      anon_sym_COLON,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
  [4453] = 2,
    ACTIONS(927), 1,
      aux_sym__expansion_body_token1,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
  [4461] = 2,
    ACTIONS(929), 1,
      anon_sym_EQ,
    ACTIONS(3), 2,
      sym_line_continuation,
      sym_comment,
  [4469] = 2,
    ACTIONS(931), 1,
      anon_sym_LF,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
  [4477] = 2,
    ACTIONS(933), 1,
      aux_sym__expansion_body_token1,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
  [4485] = 2,
    ACTIONS(737), 1,
      anon_sym_EQ,
    ACTIONS(3), 2,
      sym_line_continuation,
      sym_comment,
  [4493] = 2,
    ACTIONS(935), 1,
      ts_builtin_sym_end,
    ACTIONS(3), 2,
      sym_line_continuation,
      sym_comment,
  [4501] = 2,
    ACTIONS(741), 1,
      anon_sym_EQ,
    ACTIONS(3), 2,
      sym_line_continuation,
      sym_comment,
  [4509] = 2,
    ACTIONS(937), 1,
      anon_sym_LF,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
  [4517] = 2,
    ACTIONS(939), 1,
      anon_sym_LF,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
  [4525] = 2,
    ACTIONS(941), 1,
      anon_sym_LF,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
  [4533] = 2,
    ACTIONS(943), 1,
      anon_sym_LF,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
  [4541] = 2,
    ACTIONS(945), 1,
      aux_sym_maintainer_instruction_token2,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
  [4549] = 2,
    ACTIONS(947), 1,
      aux_sym_shell_fragment_token1,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
  [4557] = 2,
    ACTIONS(949), 1,
      anon_sym_LF,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
  [4565] = 2,
    ACTIONS(951), 1,
      aux_sym_arg_instruction_token2,
    ACTIONS(3), 2,
      sym_line_continuation,
      sym_comment,
  [4573] = 2,
    ACTIONS(953), 1,
      anon_sym_LF,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
  [4581] = 2,
    ACTIONS(955), 1,
      anon_sym_EQ,
    ACTIONS(3), 2,
      sym_line_continuation,
      sym_comment,
  [4589] = 2,
    ACTIONS(957), 1,
      anon_sym_LF,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
  [4597] = 2,
    ACTIONS(959), 1,
      anon_sym_EQ,
    ACTIONS(3), 2,
      sym_line_continuation,
      sym_comment,
  [4605] = 2,
    ACTIONS(961), 1,
      anon_sym_RBRACE,
    ACTIONS(3), 2,
      sym_line_continuation,
      sym_comment,
  [4613] = 3,
    ACTIONS(3), 1,
      sym_line_continuation,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(963), 1,
      aux_sym_param_token2,
  [4623] = 2,
    ACTIONS(893), 1,
      anon_sym_LF,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
  [4631] = 2,
    ACTIONS(965), 1,
      aux_sym_maintainer_instruction_token2,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
  [4639] = 2,
    ACTIONS(911), 1,
      anon_sym_EQ,
    ACTIONS(3), 2,
      sym_line_continuation,
      sym_comment,
  [4647] = 2,
    ACTIONS(710), 1,
      anon_sym_EQ,
    ACTIONS(3), 2,
      sym_line_continuation,
      sym_comment,
  [4655] = 2,
    ACTIONS(967), 1,
      anon_sym_LF,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
  [4663] = 2,
    ACTIONS(969), 1,
      anon_sym_EQ,
    ACTIONS(3), 2,
      sym_line_continuation,
      sym_comment,
  [4671] = 2,
    ACTIONS(971), 1,
      anon_sym_LF,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
  [4679] = 2,
    ACTIONS(973), 1,
      anon_sym_RBRACE,
    ACTIONS(3), 2,
      sym_line_continuation,
      sym_comment,
  [4687] = 3,
    ACTIONS(3), 1,
      sym_line_continuation,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(975), 1,
      aux_sym_param_token2,
  [4697] = 2,
    ACTIONS(977), 1,
      anon_sym_LF,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
  [4705] = 2,
    ACTIONS(718), 1,
      anon_sym_EQ,
    ACTIONS(3), 2,
      sym_line_continuation,
      sym_comment,
  [4713] = 2,
    ACTIONS(979), 1,
      anon_sym_RBRACE,
    ACTIONS(3), 2,
      sym_line_continuation,
      sym_comment,
  [4721] = 3,
    ACTIONS(3), 1,
      sym_line_continuation,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(981), 1,
      aux_sym_param_token2,
  [4731] = 2,
    ACTIONS(983), 1,
      anon_sym_LF,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
  [4739] = 2,
    ACTIONS(985), 1,
      anon_sym_RBRACE,
    ACTIONS(3), 2,
      sym_line_continuation,
      sym_comment,
  [4747] = 2,
    ACTIONS(987), 1,
      anon_sym_RBRACE,
    ACTIONS(3), 2,
      sym_line_continuation,
      sym_comment,
  [4755] = 3,
    ACTIONS(3), 1,
      sym_line_continuation,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(989), 1,
      aux_sym_mount_param_param_token1,
  [4765] = 2,
    ACTIONS(991), 1,
      aux_sym_shell_fragment_token1,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
  [4773] = 2,
    ACTIONS(993), 1,
      anon_sym_RBRACE,
    ACTIONS(3), 2,
      sym_line_continuation,
      sym_comment,
  [4781] = 2,
    ACTIONS(873), 1,
      anon_sym_LF,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
  [4789] = 2,
    ACTIONS(889), 1,
      anon_sym_LF,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
  [4797] = 2,
    ACTIONS(995), 1,
      anon_sym_RBRACE,
    ACTIONS(3), 2,
      sym_line_continuation,
      sym_comment,
  [4805] = 2,
    ACTIONS(997), 1,
      anon_sym_LF,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
  [4813] = 2,
    ACTIONS(999), 1,
      anon_sym_LF,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
  [4821] = 2,
    ACTIONS(1001), 1,
      anon_sym_RBRACE,
    ACTIONS(3), 2,
      sym_line_continuation,
      sym_comment,
  [4829] = 2,
    ACTIONS(328), 1,
      sym__non_newline_whitespace,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
  [4837] = 2,
    ACTIONS(1003), 1,
      anon_sym_LF,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
  [4845] = 2,
    ACTIONS(1005), 1,
      anon_sym_RBRACE,
    ACTIONS(3), 2,
      sym_line_continuation,
      sym_comment,
  [4853] = 2,
    ACTIONS(1007), 1,
      anon_sym_LF,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
  [4861] = 2,
    ACTIONS(1009), 1,
      anon_sym_RBRACE,
    ACTIONS(3), 2,
      sym_line_continuation,
      sym_comment,
  [4869] = 2,
    ACTIONS(580), 1,
      sym__non_newline_whitespace,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
  [4877] = 2,
    ACTIONS(1011), 1,
      anon_sym_RBRACE,
    ACTIONS(3), 2,
      sym_line_continuation,
      sym_comment,
  [4885] = 2,
    ACTIONS(1013), 1,
      anon_sym_LF,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
  [4893] = 2,
    ACTIONS(1015), 1,
      anon_sym_RBRACE,
    ACTIONS(3), 2,
      sym_line_continuation,
      sym_comment,
  [4901] = 2,
    ACTIONS(1017), 1,
      aux_sym_param_token1,
    ACTIONS(3), 2,
      sym_line_continuation,
      sym_comment,
  [4909] = 2,
    ACTIONS(1019), 1,
      anon_sym_RBRACE,
    ACTIONS(3), 2,
      sym_line_continuation,
      sym_comment,
  [4917] = 2,
    ACTIONS(1021), 1,
      anon_sym_RBRACE,
    ACTIONS(3), 2,
      sym_line_continuation,
      sym_comment,
  [4925] = 2,
    ACTIONS(1023), 1,
      aux_sym__expansion_body_token1,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
  [4933] = 2,
    ACTIONS(1025), 1,
      anon_sym_EQ,
    ACTIONS(3), 2,
      sym_line_continuation,
      sym_comment,
  [4941] = 2,
    ACTIONS(1027), 1,
      aux_sym__expansion_body_token1,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
  [4949] = 2,
    ACTIONS(1029), 1,
      anon_sym_EQ,
    ACTIONS(3), 2,
      sym_line_continuation,
      sym_comment,
  [4957] = 2,
    ACTIONS(1031), 1,
      aux_sym__expansion_body_token1,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
  [4965] = 2,
    ACTIONS(1033), 1,
      anon_sym_EQ,
    ACTIONS(3), 2,
      sym_line_continuation,
      sym_comment,
  [4973] = 2,
    ACTIONS(1035), 1,
      aux_sym__expansion_body_token1,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
  [4981] = 2,
    ACTIONS(1037), 1,
      anon_sym_LF,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
  [4989] = 2,
    ACTIONS(1039), 1,
      aux_sym__expansion_body_token1,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
  [4997] = 2,
    ACTIONS(1041), 1,
      aux_sym__expansion_body_token1,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
  [5005] = 2,
    ACTIONS(1043), 1,
      aux_sym__expansion_body_token1,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
  [5013] = 2,
    ACTIONS(1045), 1,
      aux_sym__expansion_body_token1,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
  [5021] = 2,
    ACTIONS(1047), 1,
      aux_sym__expansion_body_token1,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
  [5029] = 2,
    ACTIONS(1049), 1,
      aux_sym__expansion_body_token1,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
  [5037] = 2,
    ACTIONS(1051), 1,
      aux_sym__expansion_body_token1,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
  [5045] = 2,
    ACTIONS(1053), 1,
      aux_sym__expansion_body_token1,
    ACTIONS(5), 2,
      sym_line_continuation,
      sym_comment,
  [5053] = 2,
    ACTIONS(1055), 1,
      aux_sym_param_token1,
    ACTIONS(3), 2,
      sym_line_continuation,
      sym_comment,
  [5061] = 2,
    ACTIONS(1057), 1,
      aux_sym_param_token1,
    ACTIONS(3), 2,
      sym_line_continuation,
      sym_comment,
  [5069] = 3,
    ACTIONS(3), 1,
      sym_line_continuation,
    ACTIONS(5), 1,
      sym_comment,
    ACTIONS(1059), 1,
      aux_sym_param_token2,
};

static const uint32_t ts_small_parse_table_map[] = {
  [SMALL_STATE(2)] = 0,
  [SMALL_STATE(3)] = 90,
  [SMALL_STATE(4)] = 180,
  [SMALL_STATE(5)] = 264,
  [SMALL_STATE(6)] = 291,
  [SMALL_STATE(7)] = 326,
  [SMALL_STATE(8)] = 361,
  [SMALL_STATE(9)] = 395,
  [SMALL_STATE(10)] = 425,
  [SMALL_STATE(11)] = 455,
  [SMALL_STATE(12)] = 485,
  [SMALL_STATE(13)] = 514,
  [SMALL_STATE(14)] = 549,
  [SMALL_STATE(15)] = 584,
  [SMALL_STATE(16)] = 619,
  [SMALL_STATE(17)] = 648,
  [SMALL_STATE(18)] = 683,
  [SMALL_STATE(19)] = 708,
  [SMALL_STATE(20)] = 731,
  [SMALL_STATE(21)] = 756,
  [SMALL_STATE(22)] = 780,
  [SMALL_STATE(23)] = 804,
  [SMALL_STATE(24)] = 828,
  [SMALL_STATE(25)] = 852,
  [SMALL_STATE(26)] = 876,
  [SMALL_STATE(27)] = 898,
  [SMALL_STATE(28)] = 920,
  [SMALL_STATE(29)] = 942,
  [SMALL_STATE(30)] = 965,
  [SMALL_STATE(31)] = 988,
  [SMALL_STATE(32)] = 1009,
  [SMALL_STATE(33)] = 1026,
  [SMALL_STATE(34)] = 1049,
  [SMALL_STATE(35)] = 1074,
  [SMALL_STATE(36)] = 1095,
  [SMALL_STATE(37)] = 1118,
  [SMALL_STATE(38)] = 1141,
  [SMALL_STATE(39)] = 1162,
  [SMALL_STATE(40)] = 1185,
  [SMALL_STATE(41)] = 1210,
  [SMALL_STATE(42)] = 1227,
  [SMALL_STATE(43)] = 1244,
  [SMALL_STATE(44)] = 1267,
  [SMALL_STATE(45)] = 1290,
  [SMALL_STATE(46)] = 1313,
  [SMALL_STATE(47)] = 1336,
  [SMALL_STATE(48)] = 1359,
  [SMALL_STATE(49)] = 1382,
  [SMALL_STATE(50)] = 1405,
  [SMALL_STATE(51)] = 1428,
  [SMALL_STATE(52)] = 1451,
  [SMALL_STATE(53)] = 1474,
  [SMALL_STATE(54)] = 1496,
  [SMALL_STATE(55)] = 1524,
  [SMALL_STATE(56)] = 1546,
  [SMALL_STATE(57)] = 1574,
  [SMALL_STATE(58)] = 1596,
  [SMALL_STATE(59)] = 1618,
  [SMALL_STATE(60)] = 1640,
  [SMALL_STATE(61)] = 1662,
  [SMALL_STATE(62)] = 1684,
  [SMALL_STATE(63)] = 1700,
  [SMALL_STATE(64)] = 1722,
  [SMALL_STATE(65)] = 1744,
  [SMALL_STATE(66)] = 1772,
  [SMALL_STATE(67)] = 1800,
  [SMALL_STATE(68)] = 1822,
  [SMALL_STATE(69)] = 1838,
  [SMALL_STATE(70)] = 1866,
  [SMALL_STATE(71)] = 1888,
  [SMALL_STATE(72)] = 1916,
  [SMALL_STATE(73)] = 1932,
  [SMALL_STATE(74)] = 1948,
  [SMALL_STATE(75)] = 1976,
  [SMALL_STATE(76)] = 1991,
  [SMALL_STATE(77)] = 2010,
  [SMALL_STATE(78)] = 2029,
  [SMALL_STATE(79)] = 2044,
  [SMALL_STATE(80)] = 2063,
  [SMALL_STATE(81)] = 2078,
  [SMALL_STATE(82)] = 2097,
  [SMALL_STATE(83)] = 2116,
  [SMALL_STATE(84)] = 2139,
  [SMALL_STATE(85)] = 2158,
  [SMALL_STATE(86)] = 2177,
  [SMALL_STATE(87)] = 2196,
  [SMALL_STATE(88)] = 2215,
  [SMALL_STATE(89)] = 2236,
  [SMALL_STATE(90)] = 2255,
  [SMALL_STATE(91)] = 2274,
  [SMALL_STATE(92)] = 2293,
  [SMALL_STATE(93)] = 2312,
  [SMALL_STATE(94)] = 2331,
  [SMALL_STATE(95)] = 2350,
  [SMALL_STATE(96)] = 2364,
  [SMALL_STATE(97)] = 2382,
  [SMALL_STATE(98)] = 2400,
  [SMALL_STATE(99)] = 2418,
  [SMALL_STATE(100)] = 2434,
  [SMALL_STATE(101)] = 2452,
  [SMALL_STATE(102)] = 2474,
  [SMALL_STATE(103)] = 2492,
  [SMALL_STATE(104)] = 2510,
  [SMALL_STATE(105)] = 2526,
  [SMALL_STATE(106)] = 2548,
  [SMALL_STATE(107)] = 2566,
  [SMALL_STATE(108)] = 2584,
  [SMALL_STATE(109)] = 2600,
  [SMALL_STATE(110)] = 2614,
  [SMALL_STATE(111)] = 2636,
  [SMALL_STATE(112)] = 2652,
  [SMALL_STATE(113)] = 2670,
  [SMALL_STATE(114)] = 2684,
  [SMALL_STATE(115)] = 2698,
  [SMALL_STATE(116)] = 2712,
  [SMALL_STATE(117)] = 2730,
  [SMALL_STATE(118)] = 2744,
  [SMALL_STATE(119)] = 2758,
  [SMALL_STATE(120)] = 2772,
  [SMALL_STATE(121)] = 2786,
  [SMALL_STATE(122)] = 2804,
  [SMALL_STATE(123)] = 2822,
  [SMALL_STATE(124)] = 2840,
  [SMALL_STATE(125)] = 2858,
  [SMALL_STATE(126)] = 2876,
  [SMALL_STATE(127)] = 2894,
  [SMALL_STATE(128)] = 2907,
  [SMALL_STATE(129)] = 2922,
  [SMALL_STATE(130)] = 2937,
  [SMALL_STATE(131)] = 2952,
  [SMALL_STATE(132)] = 2967,
  [SMALL_STATE(133)] = 2984,
  [SMALL_STATE(134)] = 2997,
  [SMALL_STATE(135)] = 3010,
  [SMALL_STATE(136)] = 3027,
  [SMALL_STATE(137)] = 3040,
  [SMALL_STATE(138)] = 3051,
  [SMALL_STATE(139)] = 3062,
  [SMALL_STATE(140)] = 3073,
  [SMALL_STATE(141)] = 3090,
  [SMALL_STATE(142)] = 3105,
  [SMALL_STATE(143)] = 3120,
  [SMALL_STATE(144)] = 3137,
  [SMALL_STATE(145)] = 3154,
  [SMALL_STATE(146)] = 3171,
  [SMALL_STATE(147)] = 3186,
  [SMALL_STATE(148)] = 3201,
  [SMALL_STATE(149)] = 3214,
  [SMALL_STATE(150)] = 3227,
  [SMALL_STATE(151)] = 3242,
  [SMALL_STATE(152)] = 3255,
  [SMALL_STATE(153)] = 3270,
  [SMALL_STATE(154)] = 3285,
  [SMALL_STATE(155)] = 3300,
  [SMALL_STATE(156)] = 3317,
  [SMALL_STATE(157)] = 3332,
  [SMALL_STATE(158)] = 3349,
  [SMALL_STATE(159)] = 3364,
  [SMALL_STATE(160)] = 3377,
  [SMALL_STATE(161)] = 3390,
  [SMALL_STATE(162)] = 3403,
  [SMALL_STATE(163)] = 3417,
  [SMALL_STATE(164)] = 3429,
  [SMALL_STATE(165)] = 3439,
  [SMALL_STATE(166)] = 3453,
  [SMALL_STATE(167)] = 3467,
  [SMALL_STATE(168)] = 3481,
  [SMALL_STATE(169)] = 3493,
  [SMALL_STATE(170)] = 3507,
  [SMALL_STATE(171)] = 3519,
  [SMALL_STATE(172)] = 3533,
  [SMALL_STATE(173)] = 3547,
  [SMALL_STATE(174)] = 3561,
  [SMALL_STATE(175)] = 3575,
  [SMALL_STATE(176)] = 3587,
  [SMALL_STATE(177)] = 3599,
  [SMALL_STATE(178)] = 3611,
  [SMALL_STATE(179)] = 3623,
  [SMALL_STATE(180)] = 3637,
  [SMALL_STATE(181)] = 3651,
  [SMALL_STATE(182)] = 3663,
  [SMALL_STATE(183)] = 3675,
  [SMALL_STATE(184)] = 3687,
  [SMALL_STATE(185)] = 3697,
  [SMALL_STATE(186)] = 3707,
  [SMALL_STATE(187)] = 3721,
  [SMALL_STATE(188)] = 3733,
  [SMALL_STATE(189)] = 3743,
  [SMALL_STATE(190)] = 3757,
  [SMALL_STATE(191)] = 3771,
  [SMALL_STATE(192)] = 3785,
  [SMALL_STATE(193)] = 3795,
  [SMALL_STATE(194)] = 3805,
  [SMALL_STATE(195)] = 3819,
  [SMALL_STATE(196)] = 3833,
  [SMALL_STATE(197)] = 3847,
  [SMALL_STATE(198)] = 3861,
  [SMALL_STATE(199)] = 3875,
  [SMALL_STATE(200)] = 3889,
  [SMALL_STATE(201)] = 3903,
  [SMALL_STATE(202)] = 3917,
  [SMALL_STATE(203)] = 3931,
  [SMALL_STATE(204)] = 3945,
  [SMALL_STATE(205)] = 3959,
  [SMALL_STATE(206)] = 3973,
  [SMALL_STATE(207)] = 3987,
  [SMALL_STATE(208)] = 4001,
  [SMALL_STATE(209)] = 4015,
  [SMALL_STATE(210)] = 4029,
  [SMALL_STATE(211)] = 4043,
  [SMALL_STATE(212)] = 4057,
  [SMALL_STATE(213)] = 4071,
  [SMALL_STATE(214)] = 4085,
  [SMALL_STATE(215)] = 4099,
  [SMALL_STATE(216)] = 4113,
  [SMALL_STATE(217)] = 4127,
  [SMALL_STATE(218)] = 4137,
  [SMALL_STATE(219)] = 4146,
  [SMALL_STATE(220)] = 4157,
  [SMALL_STATE(221)] = 4168,
  [SMALL_STATE(222)] = 4181,
  [SMALL_STATE(223)] = 4192,
  [SMALL_STATE(224)] = 4203,
  [SMALL_STATE(225)] = 4212,
  [SMALL_STATE(226)] = 4223,
  [SMALL_STATE(227)] = 4234,
  [SMALL_STATE(228)] = 4247,
  [SMALL_STATE(229)] = 4256,
  [SMALL_STATE(230)] = 4265,
  [SMALL_STATE(231)] = 4276,
  [SMALL_STATE(232)] = 4285,
  [SMALL_STATE(233)] = 4294,
  [SMALL_STATE(234)] = 4303,
  [SMALL_STATE(235)] = 4314,
  [SMALL_STATE(236)] = 4327,
  [SMALL_STATE(237)] = 4336,
  [SMALL_STATE(238)] = 4347,
  [SMALL_STATE(239)] = 4358,
  [SMALL_STATE(240)] = 4369,
  [SMALL_STATE(241)] = 4378,
  [SMALL_STATE(242)] = 4389,
  [SMALL_STATE(243)] = 4400,
  [SMALL_STATE(244)] = 4411,
  [SMALL_STATE(245)] = 4420,
  [SMALL_STATE(246)] = 4431,
  [SMALL_STATE(247)] = 4442,
  [SMALL_STATE(248)] = 4453,
  [SMALL_STATE(249)] = 4461,
  [SMALL_STATE(250)] = 4469,
  [SMALL_STATE(251)] = 4477,
  [SMALL_STATE(252)] = 4485,
  [SMALL_STATE(253)] = 4493,
  [SMALL_STATE(254)] = 4501,
  [SMALL_STATE(255)] = 4509,
  [SMALL_STATE(256)] = 4517,
  [SMALL_STATE(257)] = 4525,
  [SMALL_STATE(258)] = 4533,
  [SMALL_STATE(259)] = 4541,
  [SMALL_STATE(260)] = 4549,
  [SMALL_STATE(261)] = 4557,
  [SMALL_STATE(262)] = 4565,
  [SMALL_STATE(263)] = 4573,
  [SMALL_STATE(264)] = 4581,
  [SMALL_STATE(265)] = 4589,
  [SMALL_STATE(266)] = 4597,
  [SMALL_STATE(267)] = 4605,
  [SMALL_STATE(268)] = 4613,
  [SMALL_STATE(269)] = 4623,
  [SMALL_STATE(270)] = 4631,
  [SMALL_STATE(271)] = 4639,
  [SMALL_STATE(272)] = 4647,
  [SMALL_STATE(273)] = 4655,
  [SMALL_STATE(274)] = 4663,
  [SMALL_STATE(275)] = 4671,
  [SMALL_STATE(276)] = 4679,
  [SMALL_STATE(277)] = 4687,
  [SMALL_STATE(278)] = 4697,
  [SMALL_STATE(279)] = 4705,
  [SMALL_STATE(280)] = 4713,
  [SMALL_STATE(281)] = 4721,
  [SMALL_STATE(282)] = 4731,
  [SMALL_STATE(283)] = 4739,
  [SMALL_STATE(284)] = 4747,
  [SMALL_STATE(285)] = 4755,
  [SMALL_STATE(286)] = 4765,
  [SMALL_STATE(287)] = 4773,
  [SMALL_STATE(288)] = 4781,
  [SMALL_STATE(289)] = 4789,
  [SMALL_STATE(290)] = 4797,
  [SMALL_STATE(291)] = 4805,
  [SMALL_STATE(292)] = 4813,
  [SMALL_STATE(293)] = 4821,
  [SMALL_STATE(294)] = 4829,
  [SMALL_STATE(295)] = 4837,
  [SMALL_STATE(296)] = 4845,
  [SMALL_STATE(297)] = 4853,
  [SMALL_STATE(298)] = 4861,
  [SMALL_STATE(299)] = 4869,
  [SMALL_STATE(300)] = 4877,
  [SMALL_STATE(301)] = 4885,
  [SMALL_STATE(302)] = 4893,
  [SMALL_STATE(303)] = 4901,
  [SMALL_STATE(304)] = 4909,
  [SMALL_STATE(305)] = 4917,
  [SMALL_STATE(306)] = 4925,
  [SMALL_STATE(307)] = 4933,
  [SMALL_STATE(308)] = 4941,
  [SMALL_STATE(309)] = 4949,
  [SMALL_STATE(310)] = 4957,
  [SMALL_STATE(311)] = 4965,
  [SMALL_STATE(312)] = 4973,
  [SMALL_STATE(313)] = 4981,
  [SMALL_STATE(314)] = 4989,
  [SMALL_STATE(315)] = 4997,
  [SMALL_STATE(316)] = 5005,
  [SMALL_STATE(317)] = 5013,
  [SMALL_STATE(318)] = 5021,
  [SMALL_STATE(319)] = 5029,
  [SMALL_STATE(320)] = 5037,
  [SMALL_STATE(321)] = 5045,
  [SMALL_STATE(322)] = 5053,
  [SMALL_STATE(323)] = 5061,
  [SMALL_STATE(324)] = 5069,
};

static const TSParseActionEntry ts_parse_actions[] = {
  [0] = {.entry = {.count = 0, .reusable = false}},
  [1] = {.entry = {.count = 1, .reusable = false}}, RECOVER(),
  [3] = {.entry = {.count = 1, .reusable = true}}, SHIFT_EXTRA(),
  [5] = {.entry = {.count = 1, .reusable = false}}, SHIFT_EXTRA(),
  [7] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_source_file, 0, 0, 0),
  [9] = {.entry = {.count = 1, .reusable = true}}, SHIFT(71),
  [11] = {.entry = {.count = 1, .reusable = true}}, SHIFT(7),
  [13] = {.entry = {.count = 1, .reusable = true}}, SHIFT(16),
  [15] = {.entry = {.count = 1, .reusable = true}}, SHIFT(61),
  [17] = {.entry = {.count = 1, .reusable = true}}, SHIFT(104),
  [19] = {.entry = {.count = 1, .reusable = true}}, SHIFT(102),
  [21] = {.entry = {.count = 1, .reusable = true}}, SHIFT(14),
  [23] = {.entry = {.count = 1, .reusable = true}}, SHIFT(13),
  [25] = {.entry = {.count = 1, .reusable = true}}, SHIFT(12),
  [27] = {.entry = {.count = 1, .reusable = true}}, SHIFT(54),
  [29] = {.entry = {.count = 1, .reusable = true}}, SHIFT(145),
  [31] = {.entry = {.count = 1, .reusable = true}}, SHIFT(110),
  [33] = {.entry = {.count = 1, .reusable = true}}, SHIFT(262),
  [35] = {.entry = {.count = 1, .reusable = true}}, SHIFT(4),
  [37] = {.entry = {.count = 1, .reusable = true}}, SHIFT(132),
  [39] = {.entry = {.count = 1, .reusable = true}}, SHIFT(88),
  [41] = {.entry = {.count = 1, .reusable = true}}, SHIFT(234),
  [43] = {.entry = {.count = 1, .reusable = true}}, SHIFT(259),
  [45] = {.entry = {.count = 1, .reusable = true}}, SHIFT(270),
  [47] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_source_file, 1, 0, 0),
  [49] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_source_file_repeat1, 2, 0, 0),
  [51] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_source_file_repeat1, 2, 0, 0), SHIFT_REPEAT(71),
  [54] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_source_file_repeat1, 2, 0, 0), SHIFT_REPEAT(7),
  [57] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_source_file_repeat1, 2, 0, 0), SHIFT_REPEAT(16),
  [60] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_source_file_repeat1, 2, 0, 0), SHIFT_REPEAT(61),
  [63] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_source_file_repeat1, 2, 0, 0), SHIFT_REPEAT(104),
  [66] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_source_file_repeat1, 2, 0, 0), SHIFT_REPEAT(102),
  [69] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_source_file_repeat1, 2, 0, 0), SHIFT_REPEAT(14),
  [72] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_source_file_repeat1, 2, 0, 0), SHIFT_REPEAT(13),
  [75] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_source_file_repeat1, 2, 0, 0), SHIFT_REPEAT(12),
  [78] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_source_file_repeat1, 2, 0, 0), SHIFT_REPEAT(54),
  [81] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_source_file_repeat1, 2, 0, 0), SHIFT_REPEAT(145),
  [84] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_source_file_repeat1, 2, 0, 0), SHIFT_REPEAT(110),
  [87] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_source_file_repeat1, 2, 0, 0), SHIFT_REPEAT(262),
  [90] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_source_file_repeat1, 2, 0, 0), SHIFT_REPEAT(4),
  [93] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_source_file_repeat1, 2, 0, 0), SHIFT_REPEAT(132),
  [96] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_source_file_repeat1, 2, 0, 0), SHIFT_REPEAT(88),
  [99] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_source_file_repeat1, 2, 0, 0), SHIFT_REPEAT(234),
  [102] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_source_file_repeat1, 2, 0, 0), SHIFT_REPEAT(259),
  [105] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_source_file_repeat1, 2, 0, 0), SHIFT_REPEAT(270),
  [108] = {.entry = {.count = 1, .reusable = false}}, SHIFT(22),
  [110] = {.entry = {.count = 1, .reusable = true}}, SHIFT(220),
  [112] = {.entry = {.count = 1, .reusable = true}}, SHIFT(191),
  [114] = {.entry = {.count = 1, .reusable = true}}, SHIFT(286),
  [116] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_env_pair, 2, 0, 1),
  [118] = {.entry = {.count = 1, .reusable = true}}, SHIFT(215),
  [120] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_env_pair, 2, 0, 1),
  [122] = {.entry = {.count = 1, .reusable = false}}, SHIFT(52),
  [124] = {.entry = {.count = 1, .reusable = false}}, SHIFT(125),
  [126] = {.entry = {.count = 1, .reusable = false}}, SHIFT(47),
  [128] = {.entry = {.count = 1, .reusable = true}}, SHIFT(214),
  [130] = {.entry = {.count = 1, .reusable = true}}, SHIFT(50),
  [132] = {.entry = {.count = 1, .reusable = true}}, SHIFT(122),
  [134] = {.entry = {.count = 1, .reusable = false}}, SHIFT(18),
  [136] = {.entry = {.count = 1, .reusable = false}}, SHIFT(43),
  [138] = {.entry = {.count = 1, .reusable = true}}, SHIFT(43),
  [140] = {.entry = {.count = 1, .reusable = true}}, SHIFT(195),
  [142] = {.entry = {.count = 1, .reusable = true}}, SHIFT(260),
  [144] = {.entry = {.count = 1, .reusable = false}}, SHIFT(84),
  [146] = {.entry = {.count = 1, .reusable = true}}, SHIFT(84),
  [148] = {.entry = {.count = 1, .reusable = true}}, SHIFT(201),
  [150] = {.entry = {.count = 1, .reusable = true}}, SHIFT(322),
  [152] = {.entry = {.count = 1, .reusable = true}}, SHIFT(294),
  [154] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_unquoted_string, 1, 0, 0),
  [156] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_unquoted_string, 1, 0, 0),
  [158] = {.entry = {.count = 1, .reusable = false}}, SHIFT(20),
  [160] = {.entry = {.count = 1, .reusable = false}}, REDUCE(aux_sym_run_instruction_repeat1, 2, 0, 0),
  [162] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_run_instruction_repeat1, 2, 0, 0), SHIFT_REPEAT(220),
  [165] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_run_instruction_repeat1, 2, 0, 0),
  [167] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_unquoted_string_repeat1, 2, 0, 0),
  [169] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_unquoted_string_repeat1, 2, 0, 0), SHIFT_REPEAT(214),
  [172] = {.entry = {.count = 1, .reusable = false}}, REDUCE(aux_sym_unquoted_string_repeat1, 2, 0, 0),
  [174] = {.entry = {.count = 2, .reusable = false}}, REDUCE(aux_sym_unquoted_string_repeat1, 2, 0, 0), SHIFT_REPEAT(20),
  [177] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_image_name, 1, 0, 0),
  [179] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_image_name, 1, 0, 0),
  [181] = {.entry = {.count = 1, .reusable = true}}, SHIFT(172),
  [183] = {.entry = {.count = 1, .reusable = false}}, SHIFT(24),
  [185] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_shell_fragment, 1, 0, 0),
  [187] = {.entry = {.count = 1, .reusable = false}}, SHIFT(23),
  [189] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_shell_fragment, 1, 0, 0),
  [191] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_shell_fragment_repeat1, 2, 0, 0),
  [193] = {.entry = {.count = 2, .reusable = false}}, REDUCE(aux_sym_shell_fragment_repeat1, 2, 0, 0), SHIFT_REPEAT(23),
  [196] = {.entry = {.count = 1, .reusable = false}}, REDUCE(aux_sym_shell_fragment_repeat1, 2, 0, 0),
  [198] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_shell_fragment_repeat1, 2, 0, 0), SHIFT_REPEAT(286),
  [201] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_image_name, 2, 0, 0),
  [203] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_image_name, 2, 0, 0),
  [205] = {.entry = {.count = 1, .reusable = false}}, SHIFT(25),
  [207] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_image_name_repeat1, 2, 0, 0),
  [209] = {.entry = {.count = 1, .reusable = false}}, REDUCE(aux_sym_image_name_repeat1, 2, 0, 0),
  [211] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_image_name_repeat1, 2, 0, 0), SHIFT_REPEAT(172),
  [214] = {.entry = {.count = 2, .reusable = false}}, REDUCE(aux_sym_image_name_repeat1, 2, 0, 0), SHIFT_REPEAT(25),
  [217] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_mount_param, 4, 0, 13),
  [219] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_mount_param, 4, 0, 13),
  [221] = {.entry = {.count = 1, .reusable = true}}, SHIFT(235),
  [223] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_mount_param, 5, 0, 15),
  [225] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_mount_param, 5, 0, 15),
  [227] = {.entry = {.count = 1, .reusable = false}}, REDUCE(aux_sym_mount_param_repeat1, 2, 0, 0),
  [229] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_mount_param_repeat1, 2, 0, 0),
  [231] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_mount_param_repeat1, 2, 0, 0), SHIFT_REPEAT(235),
  [234] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_image_tag, 2, 0, 0),
  [236] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_image_tag, 2, 0, 0),
  [238] = {.entry = {.count = 1, .reusable = true}}, SHIFT(210),
  [240] = {.entry = {.count = 1, .reusable = false}}, SHIFT(39),
  [242] = {.entry = {.count = 1, .reusable = true}}, SHIFT(171),
  [244] = {.entry = {.count = 1, .reusable = false}}, SHIFT(225),
  [246] = {.entry = {.count = 1, .reusable = false}}, SHIFT(37),
  [248] = {.entry = {.count = 1, .reusable = true}}, SHIFT(37),
  [250] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym__user_name_or_group, 1, 0, 0),
  [252] = {.entry = {.count = 1, .reusable = true}}, SHIFT(35),
  [254] = {.entry = {.count = 1, .reusable = true}}, SHIFT(204),
  [256] = {.entry = {.count = 1, .reusable = false}}, SHIFT(252),
  [258] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_label_instruction_repeat1, 2, 0, 0),
  [260] = {.entry = {.count = 2, .reusable = false}}, REDUCE(aux_sym_label_instruction_repeat1, 2, 0, 0), SHIFT_REPEAT(249),
  [263] = {.entry = {.count = 2, .reusable = false}}, REDUCE(aux_sym_label_instruction_repeat1, 2, 0, 0), SHIFT_REPEAT(36),
  [266] = {.entry = {.count = 2, .reusable = false}}, REDUCE(aux_sym_label_instruction_repeat1, 2, 0, 0), SHIFT_REPEAT(112),
  [269] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym__user_name_or_group, 2, 0, 0),
  [271] = {.entry = {.count = 1, .reusable = true}}, SHIFT(38),
  [273] = {.entry = {.count = 1, .reusable = false}}, SHIFT(272),
  [275] = {.entry = {.count = 1, .reusable = false}}, SHIFT(33),
  [277] = {.entry = {.count = 1, .reusable = true}}, SHIFT(33),
  [279] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_double_quoted_string_repeat1, 2, 0, 0), SHIFT_REPEAT(171),
  [282] = {.entry = {.count = 1, .reusable = false}}, REDUCE(aux_sym_double_quoted_string_repeat1, 2, 0, 0),
  [284] = {.entry = {.count = 2, .reusable = false}}, REDUCE(aux_sym_double_quoted_string_repeat1, 2, 0, 0), SHIFT_REPEAT(37),
  [287] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_double_quoted_string_repeat1, 2, 0, 0), SHIFT_REPEAT(37),
  [290] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym__user_name_or_group_repeat1, 2, 0, 0),
  [292] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym__user_name_or_group_repeat1, 2, 0, 0), SHIFT_REPEAT(38),
  [295] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym__user_name_or_group_repeat1, 2, 0, 0), SHIFT_REPEAT(204),
  [298] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_image_tag_repeat1, 2, 0, 0),
  [300] = {.entry = {.count = 1, .reusable = false}}, REDUCE(aux_sym_image_tag_repeat1, 2, 0, 0),
  [302] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_image_tag_repeat1, 2, 0, 0), SHIFT_REPEAT(210),
  [305] = {.entry = {.count = 2, .reusable = false}}, REDUCE(aux_sym_image_tag_repeat1, 2, 0, 0), SHIFT_REPEAT(39),
  [308] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_label_instruction, 2, 0, 0),
  [310] = {.entry = {.count = 1, .reusable = false}}, SHIFT(249),
  [312] = {.entry = {.count = 1, .reusable = false}}, SHIFT(36),
  [314] = {.entry = {.count = 1, .reusable = false}}, SHIFT(112),
  [316] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_mount_param_param, 3, 0, 0),
  [318] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_mount_param_param, 3, 0, 0),
  [320] = {.entry = {.count = 1, .reusable = false}}, SHIFT(45),
  [322] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_path_with_heredoc, 1, 0, 0),
  [324] = {.entry = {.count = 1, .reusable = false}}, SHIFT(46),
  [326] = {.entry = {.count = 1, .reusable = true}}, SHIFT(216),
  [328] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_path_with_heredoc, 1, 0, 0),
  [330] = {.entry = {.count = 2, .reusable = false}}, REDUCE(aux_sym_shell_fragment_repeat1, 2, 0, 0), SHIFT_REPEAT(45),
  [333] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_shell_fragment_repeat1, 2, 0, 0), SHIFT_REPEAT(260),
  [336] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_path_with_heredoc, 2, 0, 0),
  [338] = {.entry = {.count = 1, .reusable = false}}, SHIFT(49),
  [340] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_path_with_heredoc, 2, 0, 0),
  [342] = {.entry = {.count = 1, .reusable = false}}, SHIFT(48),
  [344] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_unquoted_string_repeat1, 2, 0, 0), SHIFT_REPEAT(215),
  [347] = {.entry = {.count = 2, .reusable = false}}, REDUCE(aux_sym_unquoted_string_repeat1, 2, 0, 0), SHIFT_REPEAT(48),
  [350] = {.entry = {.count = 1, .reusable = false}}, REDUCE(aux_sym_path_repeat1, 2, 0, 0),
  [352] = {.entry = {.count = 2, .reusable = false}}, REDUCE(aux_sym_path_repeat1, 2, 0, 0), SHIFT_REPEAT(49),
  [355] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_path_repeat1, 2, 0, 0), SHIFT_REPEAT(216),
  [358] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_path_repeat1, 2, 0, 0),
  [360] = {.entry = {.count = 1, .reusable = false}}, SHIFT(148),
  [362] = {.entry = {.count = 1, .reusable = false}}, SHIFT(51),
  [364] = {.entry = {.count = 1, .reusable = true}}, SHIFT(51),
  [366] = {.entry = {.count = 1, .reusable = false}}, SHIFT(159),
  [368] = {.entry = {.count = 1, .reusable = false}}, SHIFT(222),
  [370] = {.entry = {.count = 1, .reusable = false}}, SHIFT(30),
  [372] = {.entry = {.count = 1, .reusable = true}}, SHIFT(30),
  [374] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym__immediate_user_name_or_group, 2, 0, 0),
  [376] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym__immediate_user_name_or_group, 2, 0, 0), SHIFT_REPEAT(164),
  [379] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym__immediate_user_name_or_group, 2, 0, 0), SHIFT_REPEAT(204),
  [382] = {.entry = {.count = 1, .reusable = false}}, SHIFT(60),
  [384] = {.entry = {.count = 1, .reusable = true}}, SHIFT(60),
  [386] = {.entry = {.count = 1, .reusable = true}}, SHIFT(162),
  [388] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_image_digest, 2, 0, 0),
  [390] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_image_digest, 2, 0, 0),
  [392] = {.entry = {.count = 1, .reusable = true}}, SHIFT(212),
  [394] = {.entry = {.count = 1, .reusable = true}}, SHIFT(59),
  [396] = {.entry = {.count = 1, .reusable = false}}, SHIFT(44),
  [398] = {.entry = {.count = 1, .reusable = true}}, SHIFT(44),
  [400] = {.entry = {.count = 1, .reusable = true}}, SHIFT(211),
  [402] = {.entry = {.count = 1, .reusable = true}}, SHIFT(168),
  [404] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_user_instruction, 4, 0, 11),
  [406] = {.entry = {.count = 1, .reusable = true}}, SHIFT(164),
  [408] = {.entry = {.count = 1, .reusable = true}}, SHIFT(22),
  [410] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_image_digest_repeat1, 2, 0, 0),
  [412] = {.entry = {.count = 1, .reusable = false}}, REDUCE(aux_sym_image_digest_repeat1, 2, 0, 0),
  [414] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_image_digest_repeat1, 2, 0, 0), SHIFT_REPEAT(212),
  [417] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_image_digest_repeat1, 2, 0, 0), SHIFT_REPEAT(59),
  [420] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_path, 1, 0, 0),
  [422] = {.entry = {.count = 1, .reusable = false}}, SHIFT(70),
  [424] = {.entry = {.count = 1, .reusable = true}}, SHIFT(203),
  [426] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_path, 1, 0, 0),
  [428] = {.entry = {.count = 1, .reusable = true}}, SHIFT(249),
  [430] = {.entry = {.count = 1, .reusable = true}}, SHIFT(36),
  [432] = {.entry = {.count = 1, .reusable = true}}, SHIFT(112),
  [434] = {.entry = {.count = 1, .reusable = false}}, REDUCE(aux_sym_add_instruction_repeat1, 2, 0, 0),
  [436] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_add_instruction_repeat1, 2, 0, 0),
  [438] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_add_instruction_repeat1, 2, 0, 0), SHIFT_REPEAT(322),
  [441] = {.entry = {.count = 2, .reusable = false}}, REDUCE(aux_sym_add_instruction_repeat2, 2, 0, 0), SHIFT_REPEAT(84),
  [444] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_add_instruction_repeat2, 2, 0, 0), SHIFT_REPEAT(84),
  [447] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_add_instruction_repeat2, 2, 0, 0), SHIFT_REPEAT(201),
  [450] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_add_instruction_repeat2, 2, 0, 0), SHIFT_REPEAT(294),
  [453] = {.entry = {.count = 2, .reusable = false}}, REDUCE(aux_sym_path_repeat1, 2, 0, 0), SHIFT_REPEAT(67),
  [456] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_path_repeat1, 2, 0, 0), SHIFT_REPEAT(203),
  [459] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_param, 4, 0, 13),
  [461] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_param, 4, 0, 13),
  [463] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_path, 2, 0, 0),
  [465] = {.entry = {.count = 1, .reusable = false}}, SHIFT(67),
  [467] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_path, 2, 0, 0),
  [469] = {.entry = {.count = 1, .reusable = true}}, SHIFT(197),
  [471] = {.entry = {.count = 1, .reusable = false}}, SHIFT(21),
  [473] = {.entry = {.count = 1, .reusable = true}}, SHIFT(303),
  [475] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym__imm_expansion, 2, 0, 0),
  [477] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym__imm_expansion, 2, 0, 0),
  [479] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym__expansion_body, 3, 0, 0),
  [481] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym__expansion_body, 3, 0, 0),
  [483] = {.entry = {.count = 1, .reusable = false}}, SHIFT(81),
  [485] = {.entry = {.count = 1, .reusable = true}}, SHIFT(206),
  [487] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym__stopsignal_value_repeat1, 2, 0, 0),
  [489] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym__stopsignal_value_repeat1, 2, 0, 0), SHIFT_REPEAT(77),
  [492] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym__stopsignal_value_repeat1, 2, 0, 0), SHIFT_REPEAT(208),
  [495] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_image_alias, 2, 0, 0),
  [497] = {.entry = {.count = 1, .reusable = true}}, SHIFT(186),
  [499] = {.entry = {.count = 1, .reusable = true}}, SHIFT(85),
  [501] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_expansion, 2, 0, 0),
  [503] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_expansion, 2, 0, 0),
  [505] = {.entry = {.count = 1, .reusable = false}}, SHIFT(86),
  [507] = {.entry = {.count = 2, .reusable = false}}, REDUCE(aux_sym_path_repeat1, 2, 0, 0), SHIFT_REPEAT(82),
  [510] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_path_repeat1, 2, 0, 0), SHIFT_REPEAT(200),
  [513] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_image_spec, 1, 0, 1),
  [515] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_image_spec, 1, 0, 1),
  [517] = {.entry = {.count = 1, .reusable = true}}, SHIFT(107),
  [519] = {.entry = {.count = 1, .reusable = true}}, SHIFT(108),
  [521] = {.entry = {.count = 1, .reusable = false}}, SHIFT(93),
  [523] = {.entry = {.count = 1, .reusable = true}}, SHIFT(200),
  [525] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_image_alias_repeat1, 2, 0, 0),
  [527] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_image_alias_repeat1, 2, 0, 0), SHIFT_REPEAT(186),
  [530] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_image_alias_repeat1, 2, 0, 0), SHIFT_REPEAT(85),
  [533] = {.entry = {.count = 2, .reusable = false}}, REDUCE(aux_sym_path_repeat1, 2, 0, 0), SHIFT_REPEAT(86),
  [536] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_path_repeat1, 2, 0, 0), SHIFT_REPEAT(206),
  [539] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_expose_instruction, 2, 0, 0),
  [541] = {.entry = {.count = 1, .reusable = false}}, SHIFT(194),
  [543] = {.entry = {.count = 1, .reusable = false}}, SHIFT(99),
  [545] = {.entry = {.count = 1, .reusable = true}}, SHIFT(257),
  [547] = {.entry = {.count = 1, .reusable = true}}, SHIFT(323),
  [549] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_image_alias, 1, 0, 0),
  [551] = {.entry = {.count = 1, .reusable = true}}, SHIFT(79),
  [553] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_expose_instruction_repeat1, 2, 0, 0),
  [555] = {.entry = {.count = 2, .reusable = false}}, REDUCE(aux_sym_expose_instruction_repeat1, 2, 0, 0), SHIFT_REPEAT(194),
  [558] = {.entry = {.count = 2, .reusable = false}}, REDUCE(aux_sym_expose_instruction_repeat1, 2, 0, 0), SHIFT_REPEAT(99),
  [561] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym__stopsignal_value, 1, 0, 0),
  [563] = {.entry = {.count = 1, .reusable = true}}, SHIFT(94),
  [565] = {.entry = {.count = 1, .reusable = true}}, SHIFT(208),
  [567] = {.entry = {.count = 1, .reusable = false}}, SHIFT(82),
  [569] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym__stopsignal_value, 2, 0, 0),
  [571] = {.entry = {.count = 1, .reusable = true}}, SHIFT(77),
  [573] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_env_instruction_repeat1, 2, 0, 0),
  [575] = {.entry = {.count = 2, .reusable = false}}, REDUCE(aux_sym_env_instruction_repeat1, 2, 0, 0), SHIFT_REPEAT(271),
  [578] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_add_instruction, 3, 0, 0),
  [580] = {.entry = {.count = 1, .reusable = true}}, SHIFT(147),
  [582] = {.entry = {.count = 1, .reusable = true}}, SHIFT(190),
  [584] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_expose_port, 1, 0, 0),
  [586] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_expose_port, 1, 0, 0),
  [588] = {.entry = {.count = 1, .reusable = false}}, SHIFT(163),
  [590] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_copy_instruction, 3, 0, 0),
  [592] = {.entry = {.count = 1, .reusable = true}}, SHIFT(242),
  [594] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_env_instruction, 2, 0, 0),
  [596] = {.entry = {.count = 1, .reusable = false}}, SHIFT(271),
  [598] = {.entry = {.count = 1, .reusable = true}}, SHIFT(194),
  [600] = {.entry = {.count = 1, .reusable = true}}, SHIFT(99),
  [602] = {.entry = {.count = 1, .reusable = false}}, SHIFT(116),
  [604] = {.entry = {.count = 1, .reusable = false}}, SHIFT(254),
  [606] = {.entry = {.count = 1, .reusable = true}}, SHIFT(116),
  [608] = {.entry = {.count = 1, .reusable = false}}, SHIFT(29),
  [610] = {.entry = {.count = 1, .reusable = true}}, SHIFT(55),
  [612] = {.entry = {.count = 1, .reusable = false}}, SHIFT(76),
  [614] = {.entry = {.count = 1, .reusable = true}}, SHIFT(76),
  [616] = {.entry = {.count = 1, .reusable = true}}, SHIFT(207),
  [618] = {.entry = {.count = 1, .reusable = false}}, SHIFT(106),
  [620] = {.entry = {.count = 1, .reusable = false}}, SHIFT(279),
  [622] = {.entry = {.count = 1, .reusable = true}}, SHIFT(106),
  [624] = {.entry = {.count = 2, .reusable = false}}, REDUCE(aux_sym_single_quoted_string_repeat1, 2, 0, 0), SHIFT_REPEAT(116),
  [627] = {.entry = {.count = 1, .reusable = false}}, REDUCE(aux_sym_single_quoted_string_repeat1, 2, 0, 0),
  [629] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_single_quoted_string_repeat1, 2, 0, 0), SHIFT_REPEAT(116),
  [632] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_add_instruction, 4, 0, 0),
  [634] = {.entry = {.count = 1, .reusable = false}}, SHIFT(123),
  [636] = {.entry = {.count = 1, .reusable = false}}, SHIFT(151),
  [638] = {.entry = {.count = 1, .reusable = true}}, SHIFT(123),
  [640] = {.entry = {.count = 1, .reusable = false}}, SHIFT(160),
  [642] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_copy_instruction, 4, 0, 0),
  [644] = {.entry = {.count = 1, .reusable = false}}, SHIFT(126),
  [646] = {.entry = {.count = 1, .reusable = false}}, SHIFT(223),
  [648] = {.entry = {.count = 1, .reusable = true}}, SHIFT(126),
  [650] = {.entry = {.count = 1, .reusable = false}}, SHIFT(226),
  [652] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_label_pair, 3, 0, 9),
  [654] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_label_pair, 3, 0, 9),
  [656] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_run_instruction_repeat2, 2, 0, 0),
  [658] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_run_instruction_repeat2, 2, 0, 0), SHIFT_REPEAT(190),
  [661] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_shell_command_repeat1, 2, 0, 0),
  [663] = {.entry = {.count = 2, .reusable = false}}, REDUCE(aux_sym_shell_command_repeat1, 2, 0, 0), SHIFT_REPEAT(58),
  [666] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_run_instruction, 4, 0, 0),
  [668] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_run_instruction, 3, 0, 0),
  [670] = {.entry = {.count = 1, .reusable = true}}, SHIFT(91),
  [672] = {.entry = {.count = 1, .reusable = true}}, SHIFT(209),
  [674] = {.entry = {.count = 1, .reusable = false}}, REDUCE(aux_sym_json_string_repeat1, 2, 0, 0),
  [676] = {.entry = {.count = 2, .reusable = false}}, REDUCE(aux_sym_json_string_repeat1, 2, 0, 0), SHIFT_REPEAT(135),
  [679] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_json_string_repeat1, 2, 0, 0), SHIFT_REPEAT(135),
  [682] = {.entry = {.count = 1, .reusable = false}}, SHIFT(229),
  [684] = {.entry = {.count = 1, .reusable = false}}, SHIFT(157),
  [686] = {.entry = {.count = 1, .reusable = true}}, SHIFT(157),
  [688] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_run_instruction, 2, 0, 0),
  [690] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_add_instruction, 5, 0, 0),
  [692] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_image_spec, 2, 0, 5),
  [694] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_image_spec, 2, 0, 5),
  [696] = {.entry = {.count = 1, .reusable = true}}, SHIFT(213),
  [698] = {.entry = {.count = 1, .reusable = true}}, SHIFT(89),
  [700] = {.entry = {.count = 1, .reusable = true}}, SHIFT(31),
  [702] = {.entry = {.count = 1, .reusable = true}}, SHIFT(205),
  [704] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_copy_instruction, 5, 0, 0),
  [706] = {.entry = {.count = 1, .reusable = false}}, REDUCE(aux_sym_add_instruction_repeat2, 2, 0, 0),
  [708] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_add_instruction_repeat2, 2, 0, 0),
  [710] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_double_quoted_string, 2, 0, 0),
  [712] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_double_quoted_string, 2, 0, 0),
  [714] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_shell_command, 1, 0, 0),
  [716] = {.entry = {.count = 1, .reusable = false}}, SHIFT(58),
  [718] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_single_quoted_string, 2, 0, 0),
  [720] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_single_quoted_string, 2, 0, 0),
  [722] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_shell_command, 2, 0, 0),
  [724] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_add_instruction_repeat1, 2, 0, 0), SHIFT_REPEAT(323),
  [727] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_add_instruction, 4, 0, 0),
  [729] = {.entry = {.count = 1, .reusable = false}}, SHIFT(240),
  [731] = {.entry = {.count = 1, .reusable = false}}, SHIFT(135),
  [733] = {.entry = {.count = 1, .reusable = true}}, SHIFT(135),
  [735] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_copy_instruction, 4, 0, 0),
  [737] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_double_quoted_string, 3, 0, 0),
  [739] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_double_quoted_string, 3, 0, 0),
  [741] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_single_quoted_string, 3, 0, 0),
  [743] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_single_quoted_string, 3, 0, 0),
  [745] = {.entry = {.count = 1, .reusable = true}}, SHIFT(310),
  [747] = {.entry = {.count = 1, .reusable = true}}, SHIFT(133),
  [749] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_expose_port, 2, 0, 0),
  [751] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_expose_port, 2, 0, 0),
  [753] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym__immediate_user_name_or_group, 1, 0, 0),
  [755] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_json_string_array_repeat1, 2, 0, 0), SHIFT_REPEAT(230),
  [758] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_json_string_array_repeat1, 2, 0, 0),
  [760] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_heredoc_block_repeat1, 2, 0, 0), SHIFT_REPEAT(291),
  [763] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_heredoc_block_repeat1, 2, 0, 0),
  [765] = {.entry = {.count = 1, .reusable = false}}, SHIFT(63),
  [767] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_volume_instruction, 3, 0, 0),
  [769] = {.entry = {.count = 1, .reusable = true}}, SHIFT(101),
  [771] = {.entry = {.count = 1, .reusable = true}}, SHIFT(315),
  [773] = {.entry = {.count = 1, .reusable = true}}, SHIFT(109),
  [775] = {.entry = {.count = 1, .reusable = true}}, SHIFT(251),
  [777] = {.entry = {.count = 1, .reusable = true}}, SHIFT(75),
  [779] = {.entry = {.count = 2, .reusable = false}}, REDUCE(aux_sym_shell_command_repeat1, 2, 0, 0), SHIFT_REPEAT(63),
  [782] = {.entry = {.count = 1, .reusable = true}}, SHIFT(230),
  [784] = {.entry = {.count = 1, .reusable = true}}, SHIFT(218),
  [786] = {.entry = {.count = 1, .reusable = true}}, SHIFT(291),
  [788] = {.entry = {.count = 1, .reusable = true}}, SHIFT(228),
  [790] = {.entry = {.count = 1, .reusable = true}}, SHIFT(317),
  [792] = {.entry = {.count = 1, .reusable = true}}, SHIFT(193),
  [794] = {.entry = {.count = 1, .reusable = false}}, REDUCE(aux_sym_shell_command_repeat1, 2, 0, 0),
  [796] = {.entry = {.count = 1, .reusable = true}}, SHIFT(231),
  [798] = {.entry = {.count = 1, .reusable = true}}, SHIFT(233),
  [800] = {.entry = {.count = 1, .reusable = true}}, SHIFT(236),
  [802] = {.entry = {.count = 1, .reusable = true}}, SHIFT(140),
  [804] = {.entry = {.count = 1, .reusable = true}}, SHIFT(306),
  [806] = {.entry = {.count = 1, .reusable = true}}, SHIFT(170),
  [808] = {.entry = {.count = 1, .reusable = true}}, SHIFT(269),
  [810] = {.entry = {.count = 1, .reusable = true}}, SHIFT(289),
  [812] = {.entry = {.count = 1, .reusable = true}}, SHIFT(80),
  [814] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_volume_instruction, 2, 0, 0),
  [816] = {.entry = {.count = 1, .reusable = true}}, SHIFT(288),
  [818] = {.entry = {.count = 1, .reusable = true}}, SHIFT(308),
  [820] = {.entry = {.count = 1, .reusable = true}}, SHIFT(177),
  [822] = {.entry = {.count = 1, .reusable = true}}, SHIFT(176),
  [824] = {.entry = {.count = 1, .reusable = false}}, REDUCE(aux_sym_volume_instruction_repeat1, 2, 0, 0),
  [826] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_volume_instruction_repeat1, 2, 0, 0), SHIFT_REPEAT(101),
  [829] = {.entry = {.count = 1, .reusable = true}}, SHIFT(134),
  [831] = {.entry = {.count = 1, .reusable = true}}, SHIFT(312),
  [833] = {.entry = {.count = 1, .reusable = true}}, SHIFT(138),
  [835] = {.entry = {.count = 1, .reusable = true}}, SHIFT(137),
  [837] = {.entry = {.count = 1, .reusable = true}}, SHIFT(248),
  [839] = {.entry = {.count = 1, .reusable = true}}, SHIFT(182),
  [841] = {.entry = {.count = 1, .reusable = true}}, SHIFT(181),
  [843] = {.entry = {.count = 1, .reusable = true}}, SHIFT(314),
  [845] = {.entry = {.count = 1, .reusable = true}}, SHIFT(185),
  [847] = {.entry = {.count = 1, .reusable = true}}, SHIFT(184),
  [849] = {.entry = {.count = 1, .reusable = true}}, SHIFT(318),
  [851] = {.entry = {.count = 1, .reusable = true}}, SHIFT(95),
  [853] = {.entry = {.count = 1, .reusable = true}}, SHIFT(316),
  [855] = {.entry = {.count = 1, .reusable = true}}, SHIFT(113),
  [857] = {.entry = {.count = 1, .reusable = true}}, SHIFT(319),
  [859] = {.entry = {.count = 1, .reusable = true}}, SHIFT(161),
  [861] = {.entry = {.count = 1, .reusable = true}}, SHIFT(188),
  [863] = {.entry = {.count = 1, .reusable = true}}, SHIFT(320),
  [865] = {.entry = {.count = 1, .reusable = true}}, SHIFT(72),
  [867] = {.entry = {.count = 1, .reusable = true}}, SHIFT(321),
  [869] = {.entry = {.count = 1, .reusable = true}}, SHIFT(117),
  [871] = {.entry = {.count = 1, .reusable = true}}, SHIFT(119),
  [873] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_json_string_array, 4, 0, 0),
  [875] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_image_spec, 3, 0, 8),
  [877] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_image_spec, 3, 0, 8),
  [879] = {.entry = {.count = 1, .reusable = false}}, SHIFT(307),
  [881] = {.entry = {.count = 1, .reusable = false}}, SHIFT(266),
  [883] = {.entry = {.count = 1, .reusable = false}}, SHIFT(264),
  [885] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_heredoc_block, 3, 0, 0),
  [887] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_json_string, 2, 0, 0),
  [889] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_json_string_array, 3, 0, 0),
  [891] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_heredoc_block, 2, 0, 0),
  [893] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_json_string_array, 2, 0, 0),
  [895] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_from_instruction, 2, 0, 0),
  [897] = {.entry = {.count = 1, .reusable = false}}, SHIFT(144),
  [899] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_arg_instruction, 2, 0, 4),
  [901] = {.entry = {.count = 1, .reusable = true}}, SHIFT(9),
  [903] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_env_pair, 3, 0, 10),
  [905] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_env_pair, 3, 0, 10),
  [907] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_json_string, 3, 0, 0),
  [909] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_volume_instruction_repeat1, 2, 0, 0),
  [911] = {.entry = {.count = 1, .reusable = true}}, SHIFT(8),
  [913] = {.entry = {.count = 1, .reusable = true}}, SHIFT(11),
  [915] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_image_spec, 2, 0, 6),
  [917] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_image_spec, 2, 0, 6),
  [919] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_from_instruction, 3, 0, 0),
  [921] = {.entry = {.count = 1, .reusable = false}}, SHIFT(155),
  [923] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_user_instruction, 2, 0, 3),
  [925] = {.entry = {.count = 1, .reusable = true}}, SHIFT(92),
  [927] = {.entry = {.count = 1, .reusable = false}}, SHIFT(287),
  [929] = {.entry = {.count = 1, .reusable = true}}, SHIFT(10),
  [931] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_workdir_instruction, 2, 0, 0),
  [933] = {.entry = {.count = 1, .reusable = false}}, SHIFT(283),
  [935] = {.entry = {.count = 1, .reusable = true}},  ACCEPT_INPUT(),
  [937] = {.entry = {.count = 1, .reusable = true}}, SHIFT(5),
  [939] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_stopsignal_instruction, 2, 0, 0),
  [941] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_healthcheck_instruction, 2, 0, 0),
  [943] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym__spaced_env_pair, 3, 0, 10),
  [945] = {.entry = {.count = 1, .reusable = false}}, SHIFT(297),
  [947] = {.entry = {.count = 1, .reusable = true}}, SHIFT(62),
  [949] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_from_instruction, 5, 0, 14),
  [951] = {.entry = {.count = 1, .reusable = true}}, SHIFT(238),
  [953] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_entrypoint_instruction, 2, 0, 0),
  [955] = {.entry = {.count = 1, .reusable = true}}, SHIFT(285),
  [957] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_onbuild_instruction, 2, 0, 0),
  [959] = {.entry = {.count = 1, .reusable = true}}, SHIFT(227),
  [961] = {.entry = {.count = 1, .reusable = true}}, SHIFT(175),
  [963] = {.entry = {.count = 1, .reusable = false}}, SHIFT(68),
  [965] = {.entry = {.count = 1, .reusable = false}}, SHIFT(301),
  [967] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_shell_instruction, 2, 0, 0),
  [969] = {.entry = {.count = 1, .reusable = true}}, SHIFT(324),
  [971] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_volume_instruction, 2, 0, 0),
  [973] = {.entry = {.count = 1, .reusable = true}}, SHIFT(178),
  [975] = {.entry = {.count = 1, .reusable = false}}, SHIFT(111),
  [977] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_env_instruction, 2, 0, 2),
  [979] = {.entry = {.count = 1, .reusable = true}}, SHIFT(136),
  [981] = {.entry = {.count = 1, .reusable = false}}, SHIFT(232),
  [983] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_healthcheck_instruction, 3, 0, 0),
  [985] = {.entry = {.count = 1, .reusable = true}}, SHIFT(78),
  [987] = {.entry = {.count = 1, .reusable = true}}, SHIFT(139),
  [989] = {.entry = {.count = 1, .reusable = false}}, SHIFT(41),
  [991] = {.entry = {.count = 1, .reusable = true}}, SHIFT(32),
  [993] = {.entry = {.count = 1, .reusable = true}}, SHIFT(183),
  [995] = {.entry = {.count = 1, .reusable = true}}, SHIFT(217),
  [997] = {.entry = {.count = 1, .reusable = true}}, SHIFT(224),
  [999] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_cmd_instruction, 2, 0, 0),
  [1001] = {.entry = {.count = 1, .reusable = true}}, SHIFT(114),
  [1003] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_from_instruction, 4, 0, 7),
  [1005] = {.entry = {.count = 1, .reusable = true}}, SHIFT(115),
  [1007] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_maintainer_instruction, 2, 0, 0),
  [1009] = {.entry = {.count = 1, .reusable = true}}, SHIFT(192),
  [1011] = {.entry = {.count = 1, .reusable = true}}, SHIFT(118),
  [1013] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_cross_build_instruction, 2, 0, 0),
  [1015] = {.entry = {.count = 1, .reusable = true}}, SHIFT(149),
  [1017] = {.entry = {.count = 1, .reusable = true}}, SHIFT(274),
  [1019] = {.entry = {.count = 1, .reusable = true}}, SHIFT(73),
  [1021] = {.entry = {.count = 1, .reusable = true}}, SHIFT(120),
  [1023] = {.entry = {.count = 1, .reusable = false}}, SHIFT(267),
  [1025] = {.entry = {.count = 1, .reusable = true}}, SHIFT(268),
  [1027] = {.entry = {.count = 1, .reusable = false}}, SHIFT(276),
  [1029] = {.entry = {.count = 1, .reusable = true}}, SHIFT(277),
  [1031] = {.entry = {.count = 1, .reusable = false}}, SHIFT(280),
  [1033] = {.entry = {.count = 1, .reusable = true}}, SHIFT(281),
  [1035] = {.entry = {.count = 1, .reusable = false}}, SHIFT(284),
  [1037] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_arg_instruction, 4, 0, 12),
  [1039] = {.entry = {.count = 1, .reusable = false}}, SHIFT(290),
  [1041] = {.entry = {.count = 1, .reusable = false}}, SHIFT(293),
  [1043] = {.entry = {.count = 1, .reusable = false}}, SHIFT(296),
  [1045] = {.entry = {.count = 1, .reusable = false}}, SHIFT(298),
  [1047] = {.entry = {.count = 1, .reusable = false}}, SHIFT(300),
  [1049] = {.entry = {.count = 1, .reusable = false}}, SHIFT(302),
  [1051] = {.entry = {.count = 1, .reusable = false}}, SHIFT(304),
  [1053] = {.entry = {.count = 1, .reusable = false}}, SHIFT(305),
  [1055] = {.entry = {.count = 1, .reusable = true}}, SHIFT(309),
  [1057] = {.entry = {.count = 1, .reusable = true}}, SHIFT(311),
  [1059] = {.entry = {.count = 1, .reusable = false}}, SHIFT(221),
};

enum ts_external_scanner_symbol_identifiers {
  ts_external_token_heredoc_marker = 0,
  ts_external_token_heredoc_line = 1,
  ts_external_token_heredoc_end = 2,
  ts_external_token_heredoc_nl = 3,
  ts_external_token_error_sentinel = 4,
};

static const TSSymbol ts_external_scanner_symbol_map[EXTERNAL_TOKEN_COUNT] = {
  [ts_external_token_heredoc_marker] = sym_heredoc_marker,
  [ts_external_token_heredoc_line] = sym_heredoc_line,
  [ts_external_token_heredoc_end] = sym_heredoc_end,
  [ts_external_token_heredoc_nl] = sym_heredoc_nl,
  [ts_external_token_error_sentinel] = sym_error_sentinel,
};

static const bool ts_external_scanner_states[6][EXTERNAL_TOKEN_COUNT] = {
  [1] = {
    [ts_external_token_heredoc_marker] = true,
    [ts_external_token_heredoc_line] = true,
    [ts_external_token_heredoc_end] = true,
    [ts_external_token_heredoc_nl] = true,
    [ts_external_token_error_sentinel] = true,
  },
  [2] = {
    [ts_external_token_heredoc_marker] = true,
  },
  [3] = {
    [ts_external_token_heredoc_marker] = true,
    [ts_external_token_heredoc_nl] = true,
  },
  [4] = {
    [ts_external_token_heredoc_nl] = true,
  },
  [5] = {
    [ts_external_token_heredoc_line] = true,
    [ts_external_token_heredoc_end] = true,
  },
};

#ifdef __cplusplus
extern "C" {
#endif
void *tree_sitter_dockerfile_external_scanner_create(void);
void tree_sitter_dockerfile_external_scanner_destroy(void *);
bool tree_sitter_dockerfile_external_scanner_scan(void *, TSLexer *, const bool *);
unsigned tree_sitter_dockerfile_external_scanner_serialize(void *, char *);
void tree_sitter_dockerfile_external_scanner_deserialize(void *, const char *, unsigned);

#ifdef TREE_SITTER_HIDE_SYMBOLS
#define TS_PUBLIC
#elif defined(_WIN32)
#define TS_PUBLIC __declspec(dllexport)
#else
#define TS_PUBLIC __attribute__((visibility("default")))
#endif

TS_PUBLIC const TSLanguage *tree_sitter_dockerfile(void) {
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
    .external_scanner = {
      &ts_external_scanner_states[0][0],
      ts_external_scanner_symbol_map,
      tree_sitter_dockerfile_external_scanner_create,
      tree_sitter_dockerfile_external_scanner_destroy,
      tree_sitter_dockerfile_external_scanner_scan,
      tree_sitter_dockerfile_external_scanner_serialize,
      tree_sitter_dockerfile_external_scanner_deserialize,
    },
    .primary_state_ids = ts_primary_state_ids,
  };
  return &language;
}
#ifdef __cplusplus
}
#endif