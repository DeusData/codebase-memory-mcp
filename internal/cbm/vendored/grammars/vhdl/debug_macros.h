/*------------------------------------------------------------------------------

Copyright (C) John-Philip Taylor
jpt13653903@gmail.com
--------------------------------------------------------------------------------

Defines the following printf-like macros:
- debug
- info
- warning
- error
- assert

All the macros only produce output when "DEBUG" is defined.
The macros report the current file, function and line number
as part of the console output.
------------------------------------------------------------------------------*/

#ifndef debug_macros_h
#define debug_macros_h
//------------------------------------------------------------------------------

#include <stdio.h>
//------------------------------------------------------------------------------

#define ANSI_RESET             "\e[0m"
#define ANSI_FG_BLACK          "\e[30m"
#define ANSI_BG_BLACK          "\e[40m"
#define ANSI_FG_RED            "\e[31m"
#define ANSI_BG_RED            "\e[41m"
#define ANSI_FG_GREEN          "\e[32m"
#define ANSI_BG_GREEN          "\e[42m"
#define ANSI_FG_YELLOW         "\e[33m"
#define ANSI_BG_YELLOW         "\e[43m"
#define ANSI_FG_BLUE           "\e[34m"
#define ANSI_BG_BLUE           "\e[44m"
#define ANSI_FG_MAGENTA        "\e[35m"
#define ANSI_BG_MAGENTA        "\e[45m"
#define ANSI_FG_CYAN           "\e[36m"
#define ANSI_BG_CYAN           "\e[46m"
#define ANSI_FG_WHITE          "\e[37m"
#define ANSI_BG_WHITE          "\e[47m"
#define ANSI_FG_BRIGHT_BLACK   "\e[90m"
#define ANSI_BG_BRIGHT_BLACK   "\e[100m"
#define ANSI_FG_BRIGHT_RED     "\e[91m"
#define ANSI_BG_BRIGHT_RED     "\e[101m"
#define ANSI_FG_BRIGHT_GREEN   "\e[92m"
#define ANSI_BG_BRIGHT_GREEN   "\e[102m"
#define ANSI_FG_BRIGHT_YELLOW  "\e[93m"
#define ANSI_BG_BRIGHT_YELLOW  "\e[103m"
#define ANSI_FG_BRIGHT_BLUE    "\e[94m"
#define ANSI_BG_BRIGHT_BLUE    "\e[104m"
#define ANSI_FG_BRIGHT_MAGENTA "\e[95m"
#define ANSI_BG_BRIGHT_MAGENTA "\e[105m"
#define ANSI_FG_BRIGHT_CYAN    "\e[96m"
#define ANSI_BG_BRIGHT_CYAN    "\e[106m"
#define ANSI_FG_BRIGHT_WHITE   "\e[97m"
#define ANSI_BG_BRIGHT_WHITE   "\e[107m"
//------------------------------------------------------------------------------

#if defined(DEBUG)
  #define debug(...) do {                                   \
    printf(ANSI_FG_GREEN "Debug: " ANSI_RESET __VA_ARGS__); \
    printf(ANSI_FG_BRIGHT_BLACK " [%s +%d \"%s(...)\"]\n"   \
           ANSI_RESET, __FILE__, __LINE__, __func__);       \
    fflush(stdout);                                         \
  }while(0)

  #define info(...) do {                                  \
    printf(ANSI_FG_CYAN "Info: " ANSI_RESET __VA_ARGS__); \
    printf(ANSI_FG_BRIGHT_BLACK " [%s +%d \"%s(...)\"]\n" \
           ANSI_RESET, __FILE__, __LINE__, __func__);     \
    fflush(stdout);                                       \
  }while(0)

  #define warning(...) do {                                    \
    printf(ANSI_FG_YELLOW "Warning: " ANSI_RESET __VA_ARGS__); \
    printf(ANSI_FG_BRIGHT_BLACK " [%s +%d \"%s(...)\"]\n"      \
           ANSI_RESET, __FILE__, __LINE__, __func__);          \
    fflush(stdout);                                            \
  }while(0)

  #define error(...) do {                                        \
    printf(ANSI_FG_BRIGHT_RED "Error: " ANSI_RESET __VA_ARGS__); \
    printf(ANSI_FG_BRIGHT_BLACK " [%s +%d \"%s(...)\"]\n"        \
           ANSI_RESET, __FILE__, __LINE__, __func__);            \
    fflush(stdout);                                              \
  }while(0)

  #define assert(condition, ...) do {                       \
    if(!(condition)){                                       \
      printf(ANSI_FG_BRIGHT_RED "Failed assertion: ");      \
      printf(ANSI_RESET "%s", #condition);                  \
      printf(ANSI_FG_BRIGHT_BLACK " [%s +%d \"%s(...)\"]\n" \
             ANSI_RESET, __FILE__, __LINE__, __func__);     \
      fflush(stdout);                                       \
      __VA_ARGS__;                                          \
    }                                                       \
  }while(0)
#else
  #define debug(...)
  #define info(...)
  #define warning(...)
  #define error(...)
  #define assert(...)
#endif
//------------------------------------------------------------------------------

#endif
//------------------------------------------------------------------------------
