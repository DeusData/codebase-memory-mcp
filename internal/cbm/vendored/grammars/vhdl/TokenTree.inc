static int32_t lowercase(int32_t lookahead)
{
    if(lookahead >= 'A' && lookahead <= 'Z') lookahead += 'a' - 'A';
    debug("lookahead = %c(0x%x)", (char)lookahead, lookahead);
    return lookahead;
}
//------------------------------------------------------------------------------

static int32_t advance(TSLexer* lexer)
{
    lexer->advance(lexer, false);
    return lowercase(lexer->lookahead);
}
//------------------------------------------------------------------------------

const TokenType* token_tree_match(TSLexer* lexer)
{
    uint32_t lookahead    = lowercase(lexer->lookahead);
    const TokenType* type = 0;

    #include "token_tree_match.inc"

    debug("type->head = %s", type ? token_type_to_string(type->type) : "UNKNOWN");
    return type;
}
//------------------------------------------------------------------------------

