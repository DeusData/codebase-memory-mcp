#include "test_framework.h"
#include <mcp/test_impact_changes.h>
#include <stdint.h>

#define CI_MOD(path, oldline, newline) \
    "diff --git a/" path " b/" path "\n" \
    "index 1111111..2222222 100644\n--- a/" path "\n+++ b/" path "\n" \
    "@@ -1 +1 @@\n-" oldline "\n+" newline "\n"
#define CI_ADD(path) \
    "diff --git a/" path " b/" path "\nnew file mode 100644\n" \
    "index 0000000..1111111\n--- /dev/null\n+++ b/" path "\n" \
    "@@ -0,0 +1,2 @@\n+alpha\n+beta\n"
#define CI_DEL(path) \
    "diff --git a/" path " b/" path "\ndeleted file mode 100644\n" \
    "index 1111111..0000000\n--- a/" path "\n+++ /dev/null\n" \
    "@@ -4,2 +3,0 @@\n-old one\n-old two\n"
static const unsigned char ci_empty[]="";
static const unsigned char ci_mixed_names[]="M\0z.c\0" "D\0gone.c\0" "A\0a.c\0";
static const unsigned char ci_mixed_patch[]=CI_MOD("z.c","before_z","after_z") CI_DEL("gone.c") CI_ADD("a.c");
static const unsigned char ci_two_names[]="M\0z.c\0" "M\0a.c\0";
static char ci_sentinel_byte;
#define CI_SENTINEL ((cbm_changes_t *)(void *)&ci_sentinel_byte)
static bool ci_eq(const char *a,const char *b) { return a && b && strcmp(a,b)==0; }
static bool ci_path(const cbm_change_file_t *row,const unsigned char *path,size_t n,char status) {
    return row && row->path && row->path_length==n && memcmp(row->path,path,n)==0 &&
        row->path[n]==0 && row->status==status;
}
static bool ci_cpath(const cbm_change_file_t *row,const char *path,char status) {
    return ci_path(row,(const unsigned char *)path,strlen(path),status);
}
static bool ci_floor(const cbm_changes_t *c,size_t want,unsigned issue) {
    size_t count=777; const cbm_change_file_t *rows=cbm_changes_files(c,&count);
    if (!c || count!=want || (want && !rows) || cbm_changes_patch_reconciled(c) ||
        cbm_changes_can_narrow(c) || (cbm_changes_issues(c)&issue)!=issue) return false;
    for (size_t i=0;i<count;i++)
        if (rows[i].evidence!=CBM_CHANGE_WHOLE_FILE || !(rows[i].reasons&CBM_CHANGE_UNRECONCILED) || rows[i].patch_file) return false;
    return true;
}
static bool ci_legacy(const unsigned char *patch,size_t len,bool complete,int want_count) {
    cbm_diff_t *d=cbm_diff_parse((const char *)patch,len); int count=-1;
    if (!d) return false;
    const cbm_diff_file_t *files=cbm_diff_files(d,&count);
    bool ok=d && cbm_diff_complete(d)==complete && (want_count<0 || count==want_count) && (!count || files);
    cbm_diff_free(d); return ok;
}
static bool ci_mixed_result(const cbm_changes_t *c) {
    size_t count=0; const cbm_change_file_t *rows=cbm_changes_files(c,&count);
    if (!c || count!=3 || !rows || cbm_changes_state(c)!=CBM_CHANGES_NONEMPTY ||
        !cbm_changes_patch_reconciled(c) || !cbm_changes_can_narrow(c) || cbm_changes_issues(c)!=0 ||
        !ci_cpath(&rows[0],"a.c",'A') || !ci_cpath(&rows[1],"gone.c",'D') || !ci_cpath(&rows[2],"z.c",'M')) return false;
    for (size_t i=0;i<3;i++)
        if (rows[i].evidence!=CBM_CHANGE_HUNKS || rows[i].reasons || !rows[i].patch_file || rows[i].patch_file->hunk_count!=1 || !rows[i].patch_file->hunks) return false;
    const cbm_diff_file_t *a=rows[0].patch_file,*d=rows[1].patch_file,*m=rows[2].patch_file;
    const cbm_diff_hunk_t *ha=&a->hunks[0],*hd=&d->hunks[0],*hm=&m->hunks[0];
    return ci_eq(a->path,"a.c") && a->created && !a->deleted && !a->binary && ha->start==1 && ha->count==2 &&
        ha->added_count==2 && ha->removed_count==0 && ha->added && ci_eq(ha->added[0],"alpha") && ci_eq(ha->added[1],"beta") &&
        ci_eq(d->path,"gone.c") && d->deleted && !d->created && !d->binary && hd->start==3 && hd->count==0 &&
        hd->added_count==0 && hd->removed_count==2 && hd->removed && ci_eq(hd->removed[0],"old one") && ci_eq(hd->removed[1],"old two") &&
        ci_eq(m->path,"z.c") && !m->created && !m->deleted && !m->binary && hm->start==1 && hm->count==1 &&
        hm->added_count==1 && hm->removed_count==1 && hm->added && hm->removed && ci_eq(hm->added[0],"after_z") && ci_eq(hm->removed[0],"before_z");
}
TEST(change_identity_mixed_names_match_legacy_hunks_and_unsigned_order) {
    bool control=ci_legacy(ci_mixed_patch,sizeof(ci_mixed_patch)-1,true,3);
    cbm_changes_t *c=NULL;
    cbm_changes_status_t rc=cbm_changes_parse(ci_mixed_names,sizeof(ci_mixed_names)-1,ci_mixed_patch,sizeof(ci_mixed_patch)-1,&c);
    bool result=ci_mixed_result(c); cbm_changes_free(c);
    ASSERT_TRUE(control); ASSERT_EQ(rc,CBM_CHANGES_OK); ASSERT_TRUE(result); PASS();
}
TEST(change_identity_whole_file_reasons_preserve_mode_binary_type_and_no_hunks) {
    const unsigned char names[]="M\0mode.c\0" "M\0binary.dat\0" "T\0kind.c\0" "A\0nohunk.c\0";
    const unsigned char patch[]=
        "diff --git a/mode.c b/mode.c\nold mode 100644\nnew mode 100755\n"
        "index 1111111..2222222\n--- a/mode.c\n+++ b/mode.c\n@@ -1 +1 @@\n-before\n+after\n"
        "diff --git a/binary.dat b/binary.dat\nindex 1111111..2222222 100644\nBinary files a/binary.dat and b/binary.dat differ\n"
        CI_MOD("kind.c","regular","target")
        "diff --git a/nohunk.c b/nohunk.c\nnew file mode 100644\nindex 0000000..e69de29\n";
    bool control=ci_legacy(patch,sizeof(patch)-1,true,4);
    cbm_changes_t *c=NULL; cbm_changes_status_t rc=cbm_changes_parse(names,sizeof(names)-1,patch,sizeof(patch)-1,&c);
    size_t count=0; const cbm_change_file_t *rows=cbm_changes_files(c,&count);
    bool valid=c && count==4 && rows && cbm_changes_state(c)==CBM_CHANGES_NONEMPTY &&
        cbm_changes_patch_reconciled(c) && !cbm_changes_can_narrow(c) && cbm_changes_issues(c)==0;
    const char *paths[]={"binary.dat","kind.c","mode.c","nohunk.c"};
    const unsigned reasons[]={CBM_CHANGE_BINARY,CBM_CHANGE_TYPE,CBM_CHANGE_MODE,CBM_CHANGE_NO_HUNKS};
    const char statuses[]={'M','T','M','A'};
    for (size_t i=0;valid && i<4;i++)
        valid=ci_cpath(&rows[i],paths[i],statuses[i]) && rows[i].evidence==CBM_CHANGE_WHOLE_FILE &&
            (rows[i].reasons&reasons[i])==reasons[i] && !(rows[i].reasons&CBM_CHANGE_UNRECONCILED) && !rows[i].patch_file;
    cbm_changes_free(c);
    ASSERT_TRUE(control); ASSERT_EQ(rc,CBM_CHANGES_OK); ASSERT_TRUE(valid); PASS();
}
TEST(change_identity_raw_path_bytes_are_complete_literal_and_stably_ordered) {
    const unsigned char names[]=
        "M\0\xff.c\0" "M\0tab\tname.c\0" "M\0back\\slash.c\0" "M\0aa\0" "M\0a/b\0"
        "M\0a\0" "M\0:literal\0" "M\0[glob]*?.c\0" "M\0-dash\0" "M\0ctl\x01.c\0"
        "M\0line\nbreak.c\0" "M\0\x80.c\0" "M\0.hidden/..cache.c\0";
    const char *expected[]={"-dash",".hidden/..cache.c",":literal","[glob]*?.c","a","a/b","aa","back\\slash.c","ctl\x01.c","line\nbreak.c","tab\tname.c","\x80.c","\xff.c"};
    bool control=ci_legacy(ci_empty,0,true,0);
    cbm_changes_t *c=NULL; cbm_changes_status_t rc=cbm_changes_parse(names,sizeof(names)-1,ci_empty,0,&c);
    size_t count=0; const cbm_change_file_t *rows=cbm_changes_files(c,&count);
    bool retained=ci_floor(c,13,CBM_CHANGES_PATCH_MISSING_PATH) && cbm_changes_state(c)==CBM_CHANGES_NONEMPTY && count==13 && rows;
    for (size_t i=0;retained && i<13;i++) retained=ci_cpath(&rows[i],expected[i],'M');
    cbm_changes_free(c);
    ASSERT_TRUE(control); ASSERT_EQ(rc,CBM_CHANGES_OK); ASSERT_TRUE(retained); PASS();
}

typedef struct { const unsigned char *bytes; size_t length; const char *label; } ci_bad_t;
#define CI_BAD(label, bytes) {(const unsigned char *)(bytes),sizeof(bytes)-1,label}
TEST(change_identity_rejects_malformed_authoritative_stream_without_prefix) {
    static const ci_bad_t cases[]={
        CI_BAD("missing status separator","Mpath.c\0"), CI_BAD("missing path terminator","M\0path.c"),
        CI_BAD("empty path","M\0\0"), CI_BAD("extra trailing NUL","M\0safe.c\0\0"),
        CI_BAD("rename","M\0safe.c\0R100\0old.c\0new.c\0"), CI_BAD("copy","C\0copy.c\0"),
        CI_BAD("unmerged","U\0conflict.c\0"), CI_BAD("lowercase","m\0file.c\0"),
        CI_BAD("two-byte status","MM\0file.c\0"), CI_BAD("leading slash","M\0safe.c\0M\0/absolute.c\0"),
        CI_BAD("trailing slash","M\0safe.c\0M\0trailing/\0"), CI_BAD("empty component","M\0a//b\0"),
        CI_BAD("dot component","M\0a/./b\0"), CI_BAD("dotdot component","M\0a/../b\0"),
        CI_BAD("dot path","M\0.\0"), CI_BAD("dotdot path","M\0..\0"),
        CI_BAD("duplicate same status","M\0safe.c\0M\0safe.c\0"),
        CI_BAD("duplicate different status","A\0safe.c\0D\0safe.c\0"),
        CI_BAD("partial second record","M\0safe.c\0A\0later.c"),
        CI_BAD("embedded separator cannot truncate into success","M\0a\0junk\0")
    };
    bool control=ci_legacy(ci_mixed_patch,sizeof(ci_mixed_patch)-1,true,3);
    ASSERT_TRUE(control);
    for (size_t i=0;i<sizeof(cases)/sizeof(cases[0]);i++) {
        cbm_changes_t *out=CI_SENTINEL;
        cbm_changes_status_t rc=cbm_changes_parse(cases[i].bytes,cases[i].length,ci_empty,0,&out);
        bool cleared=out==NULL;
        if (out && out!=CI_SENTINEL) cbm_changes_free(out);
        if (rc!=CBM_CHANGES_INVALID || !cleared) fprintf(stderr,"authoritative stream case: %s\n",cases[i].label);
        ASSERT_EQ(rc,CBM_CHANGES_INVALID); ASSERT_TRUE(cleared);
    }
    cbm_changes_t *out=CI_SENTINEL;
    cbm_changes_status_t names_null=cbm_changes_parse(NULL,1,ci_empty,0,&out);
    bool names_clear=out==NULL; if (out && out!=CI_SENTINEL) cbm_changes_free(out);
    out=CI_SENTINEL;
    cbm_changes_status_t patch_null=cbm_changes_parse(ci_empty,0,NULL,1,&out);
    bool patch_clear=out==NULL; if (out && out!=CI_SENTINEL) cbm_changes_free(out);
    cbm_changes_status_t no_out=cbm_changes_parse(ci_empty,0,ci_empty,0,NULL);
    ASSERT_EQ(names_null,CBM_CHANGES_INVALID); ASSERT_TRUE(names_clear);
    ASSERT_EQ(patch_null,CBM_CHANGES_INVALID); ASSERT_TRUE(patch_clear); ASSERT_EQ(no_out,CBM_CHANGES_INVALID); PASS();
}
TEST(change_identity_missing_extra_duplicate_patch_paths_never_expose_partial_hunks) {
    static const unsigned char missing[]=CI_MOD("a.c","a0","a1");
    static const unsigned char extra[]=CI_MOD("a.c","a0","a1") CI_MOD("z.c","z0","z1") CI_MOD("extra.c","e0","e1");
    static const unsigned char duplicate[]=CI_MOD("a.c","a0","a1") CI_MOD("z.c","z0","z1") CI_MOD("a.c","a1","a2");
    const unsigned char *patches[]={missing,extra,duplicate};
    const size_t lengths[]={sizeof(missing)-1,sizeof(extra)-1,sizeof(duplicate)-1};
    const int legacy_counts[]={1,3,3};
    const unsigned issues[]={CBM_CHANGES_PATCH_MISSING_PATH,CBM_CHANGES_PATCH_EXTRA_PATH,CBM_CHANGES_PATCH_DUPLICATE_PATH};
    for (size_t i=0;i<3;i++) {
        bool control=ci_legacy(patches[i],lengths[i],true,legacy_counts[i]);
        cbm_changes_t *c=NULL; cbm_changes_status_t rc=cbm_changes_parse(ci_two_names,sizeof(ci_two_names)-1,patches[i],lengths[i],&c);
        size_t count=0; const cbm_change_file_t *rows=cbm_changes_files(c,&count);
        bool retained=ci_floor(c,2,issues[i]) && cbm_changes_state(c)==CBM_CHANGES_NONEMPTY && count==2 && rows &&
            ci_cpath(&rows[0],"a.c",'M') && ci_cpath(&rows[1],"z.c",'M');
        cbm_changes_free(c);
        ASSERT_TRUE(control); ASSERT_EQ(rc,CBM_CHANGES_OK); ASSERT_TRUE(retained);
    }
    static const unsigned char type_names[]="T\0kind.c\0";
    static const unsigned char type_blocks[]=CI_DEL("kind.c") CI_ADD("kind.c");
    bool type_control=ci_legacy(type_blocks,sizeof(type_blocks)-1,true,2);
    cbm_changes_t *c=NULL;
    cbm_changes_status_t rc=cbm_changes_parse(type_names,sizeof(type_names)-1,type_blocks,sizeof(type_blocks)-1,&c);
    size_t count=0; const cbm_change_file_t *rows=cbm_changes_files(c,&count);
    bool type_floor=ci_floor(c,1,CBM_CHANGES_PATCH_DUPLICATE_PATH) && count==1 && rows && ci_cpath(rows,"kind.c",'T');
    cbm_changes_free(c);
    ASSERT_TRUE(type_control); ASSERT_EQ(rc,CBM_CHANGES_OK); ASSERT_TRUE(type_floor); PASS();
}
TEST(change_identity_creation_and_deletion_flag_mismatches_are_not_hunk_evidence) {
    static const unsigned char mod[]=CI_MOD("a.c","a0","a1"),add[]=CI_ADD("a.c"),del[]=CI_DEL("a.c");
    const unsigned char *patches[]={mod,add,mod,del,add};
    const size_t lengths[]={sizeof(mod)-1,sizeof(add)-1,sizeof(mod)-1,sizeof(del)-1,sizeof(add)-1};
    const char statuses[]={'A','M','D','A','T'};
    for (size_t i=0;i<5;i++) {
        unsigned char names[]={'M',0,'a','.','c',0}; names[0]=(unsigned char)statuses[i];
        bool control=ci_legacy(patches[i],lengths[i],true,1);
        cbm_changes_t *c=NULL; cbm_changes_status_t rc=cbm_changes_parse(names,sizeof(names),patches[i],lengths[i],&c);
        size_t count=0; const cbm_change_file_t *rows=cbm_changes_files(c,&count);
        bool retained=ci_floor(c,1,CBM_CHANGES_PATCH_FLAG_MISMATCH) && cbm_changes_state(c)==CBM_CHANGES_NONEMPTY && count==1 &&
            rows && ci_cpath(rows,"a.c",statuses[i]);
        cbm_changes_free(c);
        ASSERT_TRUE(control); ASSERT_EQ(rc,CBM_CHANGES_OK); ASSERT_TRUE(retained);
    }
    PASS();
}
TEST(change_identity_verified_empty_is_distinct_from_unknown_and_missing_evidence) {
    size_t absent_count=777;
    bool absent=cbm_changes_files(NULL,&absent_count)==NULL && absent_count==0 &&
        cbm_changes_state(NULL)==CBM_CHANGES_UNKNOWN && !cbm_changes_patch_reconciled(NULL) &&
        !cbm_changes_can_narrow(NULL) && cbm_changes_issues(NULL)==0;
    cbm_changes_free(NULL);
    bool control=ci_legacy(ci_empty,0,true,0);
    for (int null_buffers=0;null_buffers<=1;null_buffers++) {
        cbm_changes_t *c=NULL; cbm_changes_status_t rc=cbm_changes_parse(null_buffers?NULL:ci_empty,0,null_buffers?NULL:ci_empty,0,&c);
        size_t count=777; (void)cbm_changes_files(c,&count);
        bool empty=c && count==0 && cbm_changes_state(c)==CBM_CHANGES_EMPTY &&
            cbm_changes_patch_reconciled(c) && cbm_changes_can_narrow(c) && cbm_changes_issues(c)==0;
        cbm_changes_free(c);
        ASSERT_TRUE(absent); ASSERT_TRUE(control); ASSERT_EQ(rc,CBM_CHANGES_OK); ASSERT_TRUE(empty);
    }
    static const unsigned char valid_patch[]=CI_MOD("a.c","a0","a1"),junk[]="not a diff\n";
    const unsigned char *patches[]={valid_patch,junk}; const size_t lengths[]={sizeof(valid_patch)-1,sizeof(junk)-1};
    for (size_t i=0;i<2;i++) {
        cbm_changes_t *c=NULL; cbm_changes_status_t rc=cbm_changes_parse(ci_empty,0,patches[i],lengths[i],&c);
        bool unknown=ci_floor(c,0,CBM_CHANGES_PATCH_WITHOUT_NAMES) && cbm_changes_state(c)==CBM_CHANGES_UNKNOWN;
        cbm_changes_free(c);
        ASSERT_EQ(rc,CBM_CHANGES_OK); ASSERT_TRUE(unknown);
    }
    PASS();
}
TEST(change_identity_truncated_and_quoted_patch_keep_complete_names_and_no_hunk_views) {
    static const unsigned char truncated[]=CI_MOD("a.c","a0","a1")
        "diff --git a/z.c b/z.c\n--- a/z.c\n+++ b/z.c\n@@ -1,2 +1,2 @@\n-before\n+after\n";
    static const unsigned char quoted[]=
        "diff --git \"a/space name.c\" \"b/space name.c\"\n"
        "--- \"a/space name.c\"\n+++ \"b/space name.c\"\n@@ -1 +1 @@\n-before\n+after\n";
    static const unsigned char quoted_names[]="M\0space name.c\0";
    bool truncated_control=ci_legacy(truncated,sizeof(truncated)-1,false,-1);
    bool quoted_control=ci_legacy(quoted,sizeof(quoted)-1,false,-1);
    cbm_changes_t *a=NULL,*b=NULL;
    cbm_changes_status_t ra=cbm_changes_parse(ci_two_names,sizeof(ci_two_names)-1,truncated,sizeof(truncated)-1,&a);
    cbm_changes_status_t rb=cbm_changes_parse(quoted_names,sizeof(quoted_names)-1,quoted,sizeof(quoted)-1,&b);
    size_t ac=0,bc=0; const cbm_change_file_t *ar=cbm_changes_files(a,&ac),*br=cbm_changes_files(b,&bc);
    bool af=ci_floor(a,2,CBM_CHANGES_PATCH_INCOMPLETE) && cbm_changes_state(a)==CBM_CHANGES_NONEMPTY && ac==2 && ar &&
        ci_cpath(&ar[0],"a.c",'M') && ci_cpath(&ar[1],"z.c",'M');
    bool bf=ci_floor(b,1,CBM_CHANGES_PATCH_INCOMPLETE) && cbm_changes_state(b)==CBM_CHANGES_NONEMPTY && bc==1 && br &&
        ci_cpath(br,"space name.c",'M');
    cbm_changes_free(a); cbm_changes_free(b);
    ASSERT_TRUE(truncated_control); ASSERT_TRUE(quoted_control); ASSERT_EQ(ra,CBM_CHANGES_OK); ASSERT_EQ(rb,CBM_CHANGES_OK);
    ASSERT_TRUE(af); ASSERT_TRUE(bf); PASS();
}
TEST(change_identity_owns_inputs_and_nested_hunk_strings_across_other_results) {
    unsigned char *names=malloc(sizeof(ci_mixed_names)),*patch=malloc(sizeof(ci_mixed_patch));
    bool allocated=names && patch;
    cbm_changes_t *c=NULL,*other=NULL;
    cbm_changes_status_t rc=CBM_CHANGES_INVALID;
    bool control=ci_legacy(ci_mixed_patch,sizeof(ci_mixed_patch)-1,true,3);
    if (allocated) {
        memcpy(names,ci_mixed_names,sizeof(ci_mixed_names)); memcpy(patch,ci_mixed_patch,sizeof(ci_mixed_patch));
        rc=cbm_changes_parse(names,sizeof(ci_mixed_names)-1,patch,sizeof(ci_mixed_patch)-1,&c);
        memset(names,0xA5,sizeof(ci_mixed_names)); memset(patch,0x5A,sizeof(ci_mixed_patch));
    }
    free(names); free(patch);
    static const unsigned char different_names[]="M\0z.c\0";
    static const unsigned char different_patch[]=CI_MOD("z.c","unrelated-old","unrelated-new");
    cbm_changes_status_t second=cbm_changes_parse(different_names,sizeof(different_names)-1,different_patch,sizeof(different_patch)-1,&other);
    cbm_changes_free(other);
    bool owned=ci_mixed_result(c); cbm_changes_free(c);
    ASSERT_TRUE(allocated); ASSERT_TRUE(control); ASSERT_EQ(rc,CBM_CHANGES_OK); ASSERT_EQ(second,CBM_CHANGES_OK);
    ASSERT_TRUE(owned); PASS();
}

/* D4a-R1: the old-side start is not present in cbm_diff_hunk_t, so a
 * successfully parsed new-side hunk cannot certify this old-side range. */
TEST(change_identity_nonempty_old_range_zero_start_is_non_narrowable) {
    static const unsigned char names[]="M\0a.c\0";
    static const unsigned char invalid[]=
        "diff --git a/a.c b/a.c\n--- a/a.c\n+++ b/a.c\n"
        "@@ -0,1 +1,1 @@\n-old\n+new\n";
    static const unsigned char replacement[]=
        "diff --git a/a.c b/a.c\n--- a/a.c\n+++ b/a.c\n"
        "@@ -1,1 +1,1 @@\n-old\n+new\n";
    static const unsigned char insertion[]=
        "diff --git a/a.c b/a.c\n--- a/a.c\n+++ b/a.c\n"
        "@@ -0,0 +1,1 @@\n+new\n";
    bool legacy_control=ci_legacy(replacement,sizeof(replacement)-1,true,1) &&
        ci_legacy(insertion,sizeof(insertion)-1,true,1);
    cbm_changes_t *bad=NULL,*good=NULL,*insert=NULL;
    cbm_changes_status_t rb=cbm_changes_parse(names,sizeof(names)-1,invalid,sizeof(invalid)-1,&bad);
    cbm_changes_status_t rg=cbm_changes_parse(names,sizeof(names)-1,replacement,sizeof(replacement)-1,&good);
    cbm_changes_status_t ri=cbm_changes_parse(names,sizeof(names)-1,insertion,sizeof(insertion)-1,&insert);
    size_t bn=0,gn=0,in=0;
    const cbm_change_file_t *br=cbm_changes_files(bad,&bn),*gr=cbm_changes_files(good,&gn),*ir=cbm_changes_files(insert,&in);
    bool conservative=ci_floor(bad,1,0) && cbm_changes_state(bad)==CBM_CHANGES_NONEMPTY &&
        bn==1 && br && ci_cpath(br,"a.c",'M');
    bool replacement_ok=gn==1 && gr && ci_cpath(gr,"a.c",'M') && cbm_changes_state(good)==CBM_CHANGES_NONEMPTY &&
        cbm_changes_patch_reconciled(good) && cbm_changes_can_narrow(good) && cbm_changes_issues(good)==0 &&
        gr->evidence==CBM_CHANGE_HUNKS && gr->reasons==0 && gr->patch_file && gr->patch_file->hunk_count==1 &&
        gr->patch_file->hunks && gr->patch_file->hunks[0].start==1 && gr->patch_file->hunks[0].count==1 &&
        gr->patch_file->hunks[0].added_count==1 && gr->patch_file->hunks[0].added &&
        ci_eq(gr->patch_file->hunks[0].added[0],"new") && gr->patch_file->hunks[0].removed_count==1 &&
        gr->patch_file->hunks[0].removed && ci_eq(gr->patch_file->hunks[0].removed[0],"old");
    bool insertion_ok=in==1 && ir && ci_cpath(ir,"a.c",'M') && cbm_changes_state(insert)==CBM_CHANGES_NONEMPTY &&
        cbm_changes_patch_reconciled(insert) && cbm_changes_can_narrow(insert) && cbm_changes_issues(insert)==0 &&
        ir->evidence==CBM_CHANGE_HUNKS && ir->reasons==0 && ir->patch_file && ir->patch_file->hunk_count==1 &&
        ir->patch_file->hunks && ir->patch_file->hunks[0].start==1 && ir->patch_file->hunks[0].count==1 &&
        ir->patch_file->hunks[0].added_count==1 && ir->patch_file->hunks[0].added &&
        ci_eq(ir->patch_file->hunks[0].added[0],"new") && ir->patch_file->hunks[0].removed_count==0;
    cbm_changes_free(bad); cbm_changes_free(good); cbm_changes_free(insert);
    ASSERT_TRUE(legacy_control); ASSERT_EQ(rg,CBM_CHANGES_OK); ASSERT_TRUE(replacement_ok);
    ASSERT_EQ(ri,CBM_CHANGES_OK); ASSERT_TRUE(insertion_ok);
    ASSERT_EQ(rb,CBM_CHANGES_OK); ASSERT_TRUE(conservative); PASS();
}

SUITE(test_impact_changes) {
    RUN_TEST(change_identity_mixed_names_match_legacy_hunks_and_unsigned_order);
    RUN_TEST(change_identity_whole_file_reasons_preserve_mode_binary_type_and_no_hunks);
    RUN_TEST(change_identity_raw_path_bytes_are_complete_literal_and_stably_ordered);
    RUN_TEST(change_identity_rejects_malformed_authoritative_stream_without_prefix);
    RUN_TEST(change_identity_missing_extra_duplicate_patch_paths_never_expose_partial_hunks);
    RUN_TEST(change_identity_creation_and_deletion_flag_mismatches_are_not_hunk_evidence);
    RUN_TEST(change_identity_verified_empty_is_distinct_from_unknown_and_missing_evidence);
    RUN_TEST(change_identity_truncated_and_quoted_patch_keep_complete_names_and_no_hunk_views);
    RUN_TEST(change_identity_owns_inputs_and_nested_hunk_strings_across_other_results);
    RUN_TEST(change_identity_nonempty_old_range_zero_start_is_non_narrowable);
}
