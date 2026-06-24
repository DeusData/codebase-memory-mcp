#include "chars.h"
#include "punctuation_chars.h"
#include <string.h>

static bool is_newline(int32_t c)
{
  const int32_t newline_chars[] = {
    CHAR_EOF,
    CHAR_NEWLINE,
    CHAR_CARRIAGE_RETURN,
  };
  const int length = sizeof(newline_chars) / sizeof(int32_t);
  for (int i = 0; i < length; i++) {
    if (c == newline_chars[i]) {
      return true;
    }
  }
  return false;
}

static bool is_space(int32_t c)
{
  const int32_t space_chars[] = {
    CHAR_SPACE,
    CHAR_FORM_FEED,
    CHAR_TAB,
    CHAR_VERTICAL_TAB,
    CHAR_NBSP,
  };
  const int length = sizeof(space_chars) / sizeof(int32_t);
  bool is_space_char = false;
  for (int i = 0; i < length; i++) {
    if (c == space_chars[i]) {
      is_space_char = true;
      break;
    }
  }
  return is_space_char || is_newline(c);
}

static bool is_number(int32_t c)
{
  const int32_t upper = 48;
  const int32_t lower = 57;
  return c >= upper && c <= lower;
}

static bool is_abc_lower(int32_t c)
{
  const int32_t upper = 97;
  const int32_t lower = 122;
  return c >= upper && c <= lower;
}

static bool is_abc_upper(int32_t c)
{
  const int32_t upper = 65;
  const int32_t lower = 90;
  return c >= upper && c <= lower;
}

static bool is_abc(int32_t c)
{
  return is_abc_lower(c) || is_abc_upper(c);
}

static bool is_alphanumeric(int32_t c)
{
  return is_abc(c) || is_number(c);
}

/// Check if it's an adornment char.
///
/// Adorment characters are used for sections and transitions.
static bool is_adornment_char(int32_t c)
{
  const int32_t adornment_chars[] = {
    '!',
    '"',
    '#',
    '$',
    '%',
    '&',
    '\'',
    '(',
    ')',
    '*',
    '+',
    ',',
    '-',
    '.',
    '/',
    ':',
    ';',
    '<',
    '=',
    '>',
    '?',
    '@',
    '[',
    '\\',
    ']',
    '^',
    '_',
    '`',
    '{',
    '|',
    '}',
    '~',
  };
  const int length = sizeof(adornment_chars) / sizeof(int32_t);
  for (int i = 0; i < length; i++) {
    if (c == adornment_chars[i]) {
      return true;
    }
  }
  return false;
}

static bool is_delim_char(int32_t c)
{
  int length = sizeof(delim_chars) / sizeof(int32_t);
  for (int i = 0; i < length; i++) {
    if (c == delim_chars[i]) {
      return true;
    }
  }
  length = sizeof(delim_chars_range) / sizeof(delim_chars_range[0]);
  for (int i = 0; i < length; i++) {
    if (c >= delim_chars_range[i][0] && c <= delim_chars_range[i][1]) {
      return true;
    }
  }
  return false;
}

/// Check if it's a start char.
///
/// Some tokens can start after non-whitespace chars.
static bool is_start_char(int32_t c)
{
  int length = sizeof(start_chars) / sizeof(int32_t);
  for (int i = 0; i < length; i++) {
    if (c == start_chars[i]) {
      return true;
    }
  }
  return is_delim_char(c);
}

/// Check if it's an end char.
///
/// Some tokens can end after non-whitespace chars.
static bool is_end_char(int32_t c)
{
  int length = sizeof(end_chars) / sizeof(int32_t);
  for (int i = 0; i < length; i++) {
    if (c == end_chars[i]) {
      return true;
    }
  }
  return is_delim_char(c);
}

static bool is_inline_markup_start_char(int32_t c)
{
  const int32_t inline_markup_chars[] = {
    '*', // *emphasis*, and **strong**.
    '`', // `interpreted text`, ``literals``, `hyperlink references`_, and `anonymous references`__.
    '|', // |substitution references|.
    '_', // _`inline internal target`.
    '[', // [foot-note]_.
  };
  const int length = sizeof(inline_markup_chars) / sizeof(int32_t);
  for (int i = 0; i < length; i++) {
    if (c == inline_markup_chars[i]) {
      return true;
    }
  }
  return false;
}

static bool is_inline_markup_end_char(int32_t c)
{
  const int32_t inline_markup_chars[] = {
    '*', // *emphasis*, and **strong**.
    '`', // `interpreted text`, ``literals``, _`inline internal target`,
    // `hyperlink references`_, and `anonymous references`__.
    '|', // |substitution references|.
    ']', // [foot-note]_.
  };
  const int length = sizeof(inline_markup_chars) / sizeof(int32_t);
  for (int i = 0; i < length; i++) {
    if (c == inline_markup_chars[i]) {
      return true;
    }
  }
  return false;
}

/// Check if it's an internal reference char.
///
/// References and some other names can't have two consecutive internal characters.
static bool is_internal_reference_char(int32_t c)
{
  const int32_t internal_chars[] = { '-', '_', '.', ':', '+' };
  const int length = sizeof(internal_chars) / sizeof(int32_t);
  for (int i = 0; i < length; i++) {
    if (c == internal_chars[i]) {
      return true;
    }
  }
  return false;
}

/// Check if it's a bullet char.
///
/// Lists use these characters to start an item.
static bool is_char_bullet(int32_t c)
{
  const int32_t bullets[] = {
    '*',
    '+',
    '-',
    8226, // '•'
    8227, // '‣'
    8259, // '⁃'
  };
  const int length = sizeof(bullets) / sizeof(int32_t);
  for (int i = 0; i < length; i++) {
    if (c == bullets[i]) {
      return true;
    }
  }
  return false;
}

/// Check if it's a numeric bullet char.
///
/// Lists cacn use different number formats to start an item.
static bool is_numeric_bullet(int32_t c)
{
  return (
      is_numeric_bullet_simple(c)
      || is_numeric_bullet_roman_lower(c)
      || is_numeric_bullet_roman_upper(c)
      || is_numeric_bullet_abc_lower(c)
      || is_numeric_bullet_abc_upper(c));
}

static bool is_numeric_bullet_simple(int32_t c)
{
  return is_number(c) || c == '#';
}

static bool is_numeric_bullet_roman_lower(int32_t c)
{
  const int32_t valid_chars[] = {
    'i',
    'v',
    'x',
    'l',
    'c',
    'd',
    'm',
  };
  const int length = sizeof(valid_chars) / sizeof(int32_t);
  for (int i = 0; i < length; i++) {
    if (c == valid_chars[i]) {
      return true;
    }
  }
  return false;
}

static bool is_numeric_bullet_roman_upper(int32_t c)
{
  const int32_t valid_chars[] = {
    'I',
    'V',
    'X',
    'L',
    'C',
    'D',
    'M',
  };
  const int length = sizeof(valid_chars) / sizeof(int32_t);
  for (int i = 0; i < length; i++) {
    if (c == valid_chars[i]) {
      return true;
    }
  }
  return false;
}

static bool is_numeric_bullet_abc_lower(int32_t c)
{
  return is_abc_lower(c);
}

static bool is_numeric_bullet_abc_upper(int32_t c)
{
  return is_abc_upper(c);
}

/// Check if it's a valid attribution char.
///
/// Attribution chars are used to denot the author of a quote.
static bool is_attribution_mark(int32_t c)
{
  const int32_t valid_chars[] = {
    '-',
    CHAR_EMDASH,
  };
  const int length = sizeof(valid_chars) / sizeof(int32_t);
  for (int i = 0; i < length; i++) {
    if (c == valid_chars[i]) {
      return true;
    }
  }
  return false;
}

/// Get the current indentation level.
///
/// The scanner should be set to a char after a newline.
static int get_indent_level(RSTScanner* scanner)
{
  int32_t current = scanner->lookahead;
  int indent = 0;

  while (true) {
    current = scanner->lookahead;
    if (current == CHAR_SPACE || current == CHAR_VERTICAL_TAB || current == CHAR_FORM_FEED) {
      indent += 1;
    } else if (current == CHAR_TAB) {
      indent += TAB_STOP;
    } else {
      break;
    }
    scanner->advance(scanner);
  }

  return indent;
}

static bool is_known_schema(char* string, unsigned string_len)
{
  char* valid_schemas[] = {
    "http",
    "https",
    "ftp",
    "mailto",
    "telnet",
    "ssh",
  };
  const int length = sizeof(valid_schemas) / sizeof(char*);
  for (int i = 0; i < length; i++) {
    if (string_len != strlen(valid_schemas[i])) {
      continue;
    }
    int result = memcmp(string, valid_schemas[i], string_len);
    if (result == 0) {
      return true;
    }
  }
  return false;
}

static bool is_invalid_uri_char(int32_t c)
{
  const int32_t invalid_chars[] = {
    '^',
    '}',
    '{',
    '\\',
  };
  const int length = sizeof(invalid_chars) / sizeof(int32_t);
  for (int i = 0; i < length; i++) {
    if (c == invalid_chars[i]) {
      return true;
    }
  }
  return false;
}
