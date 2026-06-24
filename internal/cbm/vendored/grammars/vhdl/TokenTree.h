/*------------------------------------------------------------------------------

Copyright (C) John-Philip Taylor
jpt13653903@gmail.com
--------------------------------------------------------------------------------

This is a binary search tree to optimise the scanner.  It facilitates the
greedy match operation.  The tree is only balanced once, so something like
a red-black tree is not required.

It consists of a balanced BST of the first character, each containing a
balanced sub-tree of the next character, etc.
------------------------------------------------------------------------------*/

#ifndef TokenTree_h
#define TokenTree_h
//------------------------------------------------------------------------------

#include <stdio.h>
#include <stdint.h>

#include "TokenType.h"
#include "debug_macros.h"
#include "tree_sitter/parser.h"
#include "tree_sitter/alloc.h"
//------------------------------------------------------------------------------

// Finds the longest match and returns the token type null-terminated array
const TokenType* token_tree_match(TSLexer* lexer);
//------------------------------------------------------------------------------


#endif
//------------------------------------------------------------------------------

