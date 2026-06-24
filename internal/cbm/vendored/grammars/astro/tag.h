#include "tree_sitter/parser.h"
#include "tree_sitter/array.h"

#include <string.h>

typedef enum {
    AREA,
    BASE,
    BR,
    COL,
    COMMAND,
    EMBED,
    FRAME,
    HR,
    IMAGE,
    IMG,
    INPUT,
    ISINDEX,
    KEYGEN,
    LINK,
    MENUITEM,
    META,
    NEXTID,
    PARAM,
    SOURCE,
    TRACK,
    WBR,
    END_OF_VOID_TAGS,

    A,
    ABBR,
    ADDRESS,
    ARTICLE,
    ASIDE,
    AUDIO,
    B,
    BDI,
    BDO,
    BLOCKQUOTE,
    BODY,
    BUTTON,
    CANVAS,
    CAPTION,
    CITE,
    CODE,
    COLGROUP,
    DATA,
    DATALIST,
    DD,
    DEL,
    DETAILS,
    DFN,
    DIALOG,
    DIV,
    DL,
    DT,
    EM,
    FIELDSET,
    FIGCAPTION,
    FIGURE,
    FOOTER,
    FORM,
    H1,
    H2,
    H3,
    H4,
    H5,
    H6,
    HEAD,
    HEADER,
    HGROUP,
    HTML,
    I,
    IFRAME,
    INS,
    KBD,
    LABEL,
    LEGEND,
    LI,
    MAIN,
    MAP,
    MARK,
    MATH,
    MENU,
    METER,
    NAV,
    NOSCRIPT,
    OBJECT,
    OL,
    OPTGROUP,
    OPTION,
    OUTPUT,
    P,
    PICTURE,
    PRE,
    PROGRESS,
    Q,
    RB,
    RP,
    RT,
    RTC,
    RUBY,
    S,
    SAMP,
    SCRIPT,
    SECTION,
    SELECT,
    SLOT,
    SMALL,
    SPAN,
    STRONG,
    STYLE,
    SUB,
    SUMMARY,
    SUP,
    SVG,
    TABLE,
    TBODY,
    TD,
    TEMPLATE,
    TEXTAREA,
    TFOOT,
    TH,
    THEAD,
    TIME,
    TITLE,
    TR,
    U,
    UL,
    VAR,
    VIDEO,
    // Technically not a 'tag' as such, but rather Astro's curly brace
    // HTML interpolations.
    INTERPOLATION,

    // Fragment tags,
    // e.g. <> </>.
    FRAGMENT,

    CUSTOM,

    END_,
} TagType;

typedef Array(char) String;

typedef struct {
    char tag_name[16];
    TagType tag_type;
} TagMapEntry;

typedef struct {
    TagType type;
    String custom_tag_name;
} Tag;

static const TagMapEntry TAG_TYPES_BY_TAG_NAME[126] = {
    {"area",       AREA      },
    {"base",       BASE      },
    {"br",         BR        },
    {"col",        COL       },
    {"command",    COMMAND   },
    {"embed",      EMBED     },
    {"frame",      FRAME     },
    {"hr",         HR        },
    {"image",      IMAGE     },
    {"img",        IMG       },
    {"input",      INPUT     },
    {"isindex",    ISINDEX   },
    {"keygen",     KEYGEN    },
    {"link",       LINK      },
    {"menuitem",   MENUITEM  },
    {"meta",       META      },
    {"nextid",     NEXTID    },
    {"param",      PARAM     },
    {"source",     SOURCE    },
    {"track",      TRACK     },
    {"wbr",        WBR       },
    {"a",          A         },
    {"abbr",       ABBR      },
    {"address",    ADDRESS   },
    {"article",    ARTICLE   },
    {"aside",      ASIDE     },
    {"audio",      AUDIO     },
    {"b",          B         },
    {"bdi",        BDI       },
    {"bdo",        BDO       },
    {"blockquote", BLOCKQUOTE},
    {"body",       BODY      },
    {"button",     BUTTON    },
    {"canvas",     CANVAS    },
    {"caption",    CAPTION   },
    {"cite",       CITE      },
    {"code",       CODE      },
    {"colgroup",   COLGROUP  },
    {"data",       DATA      },
    {"datalist",   DATALIST  },
    {"dd",         DD        },
    {"del",        DEL       },
    {"details",    DETAILS   },
    {"dfn",        DFN       },
    {"dialog",     DIALOG    },
    {"div",        DIV       },
    {"dl",         DL        },
    {"dt",         DT        },
    {"em",         EM        },
    {"fieldset",   FIELDSET  },
    {"figcaption", FIGCAPTION},
    {"figure",     FIGURE    },
    {"footer",     FOOTER    },
    {"form",       FORM      },
    {"h1",         H1        },
    {"h2",         H2        },
    {"h3",         H3        },
    {"h4",         H4        },
    {"h5",         H5        },
    {"h6",         H6        },
    {"head",       HEAD      },
    {"header",     HEADER    },
    {"hgroup",     HGROUP    },
    {"html",       HTML      },
    {"i",          I         },
    {"iframe",     IFRAME    },
    {"ins",        INS       },
    {"kbd",        KBD       },
    {"label",      LABEL     },
    {"legend",     LEGEND    },
    {"li",         LI        },
    {"main",       MAIN      },
    {"map",        MAP       },
    {"mark",       MARK      },
    {"math",       MATH      },
    {"menu",       MENU      },
    {"meter",      METER     },
    {"nav",        NAV       },
    {"noscript",   NOSCRIPT  },
    {"object",     OBJECT    },
    {"ol",         OL        },
    {"optgroup",   OPTGROUP  },
    {"option",     OPTION    },
    {"output",     OUTPUT    },
    {"p",          P         },
    {"picture",    PICTURE   },
    {"pre",        PRE       },
    {"progress",   PROGRESS  },
    {"q",          Q         },
    {"rb",         RB        },
    {"rp",         RP        },
    {"rt",         RT        },
    {"rtc",        RTC       },
    {"ruby",       RUBY      },
    {"s",          S         },
    {"samp",       SAMP      },
    {"script",     SCRIPT    },
    {"section",    SECTION   },
    {"select",     SELECT    },
    {"slot",       SLOT      },
    {"small",      SMALL     },
    {"span",       SPAN      },
    {"strong",     STRONG    },
    {"style",      STYLE     },
    {"sub",        SUB       },
    {"summary",    SUMMARY   },
    {"sup",        SUP       },
    {"svg",        SVG       },
    {"table",      TABLE     },
    {"tbody",      TBODY     },
    {"td",         TD        },
    {"template",   TEMPLATE  },
    {"textarea",   TEXTAREA  },
    {"tfoot",      TFOOT     },
    {"th",         TH        },
    {"thead",      THEAD     },
    {"time",       TIME      },
    {"title",      TITLE     },
    {"tr",         TR        },
    {"u",          U         },
    {"ul",         UL        },
    {"var",        VAR       },
    {"video",      VIDEO     },
    {"custom",     CUSTOM    },
    // INTERPOLATION is deliberately excluded from this list,
    // as it has no name.
};

static const TagType TAG_TYPES_NOT_ALLOWED_IN_PARAGRAPHS[] = {
    ADDRESS,  ARTICLE,    ASIDE,  BLOCKQUOTE, DETAILS, DIV, DL,
    FIELDSET, FIGCAPTION, FIGURE, FOOTER,     FORM,    H1,  H2,
    H3,       H4,         H5,     H6,         HEADER,  HR,  MAIN,
    NAV,      OL,         P,      PRE,        SECTION,
};

static TagType tag_type_for_name(const String *tag_name) {
    for (int i = 0; i < 126; i++) {
        const TagMapEntry *entry = &TAG_TYPES_BY_TAG_NAME[i];
        if (
            strlen(entry->tag_name) == tag_name->size &&
            memcmp(tag_name->contents, entry->tag_name, tag_name->size) == 0
        ) {
            return entry->tag_type;
        }
    }
    return CUSTOM;
}

static inline Tag tag_new() {
    Tag tag;
    tag.type = END_;
    tag.custom_tag_name = (String) array_new();
    return tag;
}

static inline Tag tag_for_name(String name) {
    Tag tag = tag_new();
    tag.type = tag_type_for_name(&name);
    if (tag.type == CUSTOM) {
        tag.custom_tag_name = name;
    } else {
        array_delete(&name);
    }
    return tag;
}

static inline void tag_free(Tag *tag) {
    if (tag->type == CUSTOM) {
        array_delete(&tag->custom_tag_name);
    }
}

static inline bool tag_is_void(const Tag *self) {
    return self->type < END_OF_VOID_TAGS;
}

static inline bool tag_eq(const Tag *self, const Tag *other) {
    if (self->type != other->type) return false;
    if (self->type == CUSTOM) {
        if (self->custom_tag_name.size != other->custom_tag_name.size) {
            return false;
        }
        if (memcmp(
            self->custom_tag_name.contents,
            other->custom_tag_name.contents,
            self->custom_tag_name.size
        ) != 0) {
            return false;
        }
    }
    return true;
}

static bool tag_can_contain(Tag *self, const Tag *other) {
    TagType child = other->type;

    if (child == INTERPOLATION) {
        // can be contained anywhere
        return true;
    }

    switch (self->type) {
        case LI:
            return child != LI;

        case DT:
        case DD:
            return child != DT && child != DD;

        case P:
            for (int i = 0; i < sizeof(TAG_TYPES_NOT_ALLOWED_IN_PARAGRAPHS) /
                                    sizeof(TagType);
                 i++) {
                if (child == TAG_TYPES_NOT_ALLOWED_IN_PARAGRAPHS[i]) {
                    return false;
                }
            }
            return true;

        case COLGROUP:
            return child == COL;

        case RB:
        case RT:
        case RP:
            return child != RB && child != RT && child != RP;

        case OPTGROUP:
            return child != OPTGROUP;

        case TR:
            return child != TR;

        case TD:
        case TH:
            return child != TD && child != TH && child != TR;

        default:
            return true;
    }
}
