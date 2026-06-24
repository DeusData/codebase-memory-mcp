// Vendored tree-sitter grammar: cfml (CFML tag dialect — .cfm templates)
// Each grammar compiled as separate unit (conflicting static symbols).
// scanner.c pulls in the shared ../../common/scanner.h (and tag.h), which is
// all-static; tag.c is therefore NOT included here (it would redefine it).
#include "vendored/grammars/cfml/parser.c"
#include "vendored/grammars/cfml/scanner.c"
