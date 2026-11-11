#include "test_framework.h"
#include "test_helpers.h"
#include <store/store.h>
#include <store/store_impact.h>
#include <sqlite3.h>
#include <stdint.h>

#define SS_PROJECT "scopetests"
static char ss_sentinel;
#define SS_SENTINEL ((cbm_store_read_scope_t *)(void *)&ss_sentinel)
static const char *const ss_edges[] = {"USAGE", "CALLS"};
static const cbm_impact_policy_t ss_policy = {
    .project = SS_PROJECT, .edge_types = ss_edges, .edge_type_count = 2,
    .follow_routes = false, .max_hops = 0
};
typedef struct { int64_t root, a, z, top, island, tail; } ss_ids_t;
typedef struct { char dir[1024], path[1024]; cbm_store_t *writer, *reader; ss_ids_t ids; } ss_file_t;
typedef struct { uint64_t calls, trip; bool armed, stop; } ss_cancel_t;
/* Pure predicate: no SQLite, store, scope or walker call, including on cancellation. */
static bool ss_cancel(void *opaque) {
    ss_cancel_t *c = opaque;
    if (c->calls < UINT64_MAX) c->calls++;
    return c->stop || (c->armed && c->calls >= c->trip);
}
static bool ss_eq(const char *a, const char *b) { return a && b && strcmp(a,b)==0; }
static int ss_sql(cbm_store_t *s, const char *sql) {
    return sqlite3_exec(cbm_store_get_db(s), sql, NULL, NULL, NULL);
}
static bool ss_integer(cbm_store_t *s, const char *sql, int64_t want) {
    sqlite3_stmt *q = NULL;
    bool ok = sqlite3_prepare_v2(cbm_store_get_db(s), sql, -1, &q, NULL)==SQLITE_OK &&
        sqlite3_step(q)==SQLITE_ROW && sqlite3_column_int64(q,0)==want && sqlite3_step(q)==SQLITE_DONE;
    if (q && sqlite3_finalize(q)!=SQLITE_OK) ok=false;
    return ok;
}
static bool ss_text(cbm_store_t *s, const char *sql, const char *want) {
    sqlite3_stmt *q = NULL;
    bool ok = sqlite3_prepare_v2(cbm_store_get_db(s), sql, -1, &q, NULL)==SQLITE_OK &&
        sqlite3_step(q)==SQLITE_ROW && ss_eq((const char *)sqlite3_column_text(q,0),want) &&
        sqlite3_step(q)==SQLITE_DONE;
    if (q && sqlite3_finalize(q)!=SQLITE_OK) ok=false;
    return ok;
}
static int64_t ss_node(cbm_store_t *s, const char *name, int line) {
    char qn[128]; int n=snprintf(qn,sizeof(qn),SS_PROJECT ".%s",name);
    if (n<=0 || (size_t)n>=sizeof(qn)) return -1;
    cbm_node_t row={.project=SS_PROJECT,.label="Function",.name=name,.qualified_name=qn,
        .file_path="unit.c",.start_line=line,.end_line=line,.properties_json="{}"};
    return cbm_store_upsert_node(s,&row);
}
static bool ss_edge(cbm_store_t *s,int64_t from,int64_t to,const char *type) {
    cbm_edge_t e={.project=SS_PROJECT,.source_id=from,.target_id=to,.type=type,.properties_json="{}"};
    return cbm_store_insert_edge(s,&e)>0;
}
static bool ss_seed(cbm_store_t *s,ss_ids_t *id) {
    if (!s || cbm_store_upsert_project(s,SS_PROJECT,"/scope-fixture")!=CBM_STORE_OK) return false;
    /* Deliberately nonlexical insertion order. */
    id->z=ss_node(s,"z",30); id->root=ss_node(s,"root",11); id->top=ss_node(s,"top",40);
    id->a=ss_node(s,"a",20); id->tail=ss_node(s,"tail",60); id->island=ss_node(s,"island",50);
    return id->z>0 && id->root>0 && id->top>0 && id->a>0 && id->tail>0 && id->island>0 &&
        ss_edge(s,id->a,id->root,"CALLS") && ss_edge(s,id->z,id->root,"USAGE") &&
        ss_edge(s,id->top,id->a,"CALLS") && ss_edge(s,id->top,id->z,"USAGE") &&
        ss_edge(s,id->tail,id->island,"CALLS") && cbm_store_generation_advance(s)==CBM_STORE_OK &&
        cbm_store_count_nodes(s,SS_PROJECT)==6;
}
static bool ss_line(cbm_store_t *s,int64_t id,int line) {
    cbm_node_t row={0}; bool ok=cbm_store_find_node_by_id(s,id,&row)==CBM_STORE_OK && row.start_line==line;
    cbm_node_free_fields(&row); return ok;
}
static bool ss_file_open(ss_file_t *f) {
    memset(f,0,sizeof(*f)); const char *dir=th_mktempdir("cbm-store-scope");
    if (!dir || strlen(dir)>=sizeof(f->dir)) return false;
    memcpy(f->dir,dir,strlen(dir)+1);
    int n=snprintf(f->path,sizeof(f->path),"%s/graph.db",dir);
    if (n<=0 || (size_t)n>=sizeof(f->path)) return false;
    f->writer=cbm_store_open_path(f->path);
    if (!ss_seed(f->writer,&f->ids) || !ss_text(f->writer,"PRAGMA journal_mode","wal")) return false;
    f->reader=cbm_store_open_path_query(f->path);
    return f->reader && ss_line(f->reader,f->ids.root,11);
}
static bool ss_file_close(ss_file_t *f) {
    cbm_store_close(f->reader); cbm_store_close(f->writer); f->reader=f->writer=NULL;
    return !f->dir[0] || th_rmtree(f->dir)==0;
}
static int ss_scope_close(cbm_store_read_scope_t *s) {
    return s==SS_SENTINEL ? CBM_STORE_ERR : cbm_store_read_scope_close(s);
}
static bool ss_hidden(cbm_impact_walk_t *w,int64_t previously_reached) {
    return w && cbm_impact_walk_count(w)==0 && cbm_impact_walk_hits(w)==NULL &&
        cbm_impact_walk_hit(w,previously_reached)==NULL && !cbm_impact_walk_reached(w,previously_reached);
}
static bool ss_hit(const cbm_impact_hit_t *h,int64_t id,int hop,int64_t via,const char *edge,const char *qn) {
    return h && h->id==id && h->hop==hop && h->via_id==via &&
        (edge ? ss_eq(h->via_edge,edge) : h->via_edge==NULL) && ss_eq(h->qualified_name,qn) && ss_eq(h->label,"Function");
}
static bool ss_golden(cbm_impact_walk_t *w,const ss_ids_t *id,bool lifted) {
    int count=cbm_impact_walk_count(w); const cbm_impact_hit_t *h=cbm_impact_walk_hits(w);
    bool ok=count==(lifted?6:4) && h &&
        ss_hit(&h[0],id->root,0,0,NULL,SS_PROJECT ".root") &&
        ss_hit(&h[1],id->a,1,id->root,"CALLS",SS_PROJECT ".a") &&
        ss_hit(&h[2],id->z,1,id->root,"USAGE",SS_PROJECT ".z") &&
        ss_hit(&h[3],id->top,2,id->z,"USAGE",SS_PROJECT ".top");
    return ok && (!lifted || (ss_hit(&h[4],id->island,3,0,NULL,SS_PROJECT ".island") &&
        ss_hit(&h[5],id->tail,4,id->island,"CALLS",SS_PROJECT ".tail")));
}
static bool ss_normal_walk(cbm_store_t *s,const ss_ids_t *id) {
    cbm_impact_walk_t *w=NULL;
    bool ok=cbm_impact_walk_open(s,&ss_policy,&w)==CBM_STORE_OK &&
        cbm_impact_walk_run(w,&id->root,1)==CBM_STORE_OK && ss_golden(w,id,false);
    if (w) { cbm_impact_walk_close(w); } return ok;
}

TEST(store_scope_wal_snapshot_pins_metadata_nodes_and_edges) {
    ss_file_t f; bool setup=ss_file_open(&f), legacy=setup && ss_normal_walk(f.reader,&f.ids);
    char before[128]={0},during[128]={0},after[128]={0};
    bool metadata=legacy && cbm_store_generation(f.reader,before,sizeof(before))==CBM_STORE_OK;
    cbm_store_read_scope_t *scope=NULL; cbm_impact_walk_t *walk=NULL;
    int opened=metadata ? cbm_store_read_scope_open(f.reader,NULL,NULL,&scope) : CBM_STORE_ERR;
    bool bound=opened==CBM_STORE_OK && cbm_store_read_scope_store(scope)==f.reader;
    int64_t fresh=0;
    bool committed=bound && cbm_store_begin(f.writer)==CBM_STORE_OK &&
        ss_node(f.writer,"root",99)==f.ids.root && (fresh=ss_node(f.writer,"newcaller",70))>0 &&
        ss_edge(f.writer,fresh,f.ids.root,"CALLS") && cbm_store_generation_advance(f.writer)==CBM_STORE_OK &&
        cbm_store_commit(f.writer)==CBM_STORE_OK;
    bool old=committed && cbm_store_generation(f.reader,during,sizeof(during))==CBM_STORE_OK &&
        strcmp(before,during)==0 && ss_line(f.reader,f.ids.root,11) &&
        cbm_impact_walk_open_scoped(scope,&ss_policy,&walk)==CBM_STORE_OK &&
        cbm_impact_walk_run(walk,&f.ids.root,1)==CBM_STORE_OK && ss_golden(walk,&f.ids,false) &&
        !cbm_impact_walk_reached(walk,fresh);
    if (walk) { cbm_impact_walk_close(walk); } walk=NULL;
    int closed=ss_scope_close(scope); scope=NULL;
    if (closed==CBM_STORE_SCOPE_DISCARD) { cbm_store_close(f.reader); f.reader=NULL; }
    int reopened=committed && f.reader ? cbm_store_read_scope_open(f.reader,NULL,NULL,&scope) : CBM_STORE_ERR;
    bool newer=reopened==CBM_STORE_OK && cbm_store_generation(f.reader,after,sizeof(after))==CBM_STORE_OK &&
        strcmp(before,after)!=0 && ss_line(f.reader,f.ids.root,99) &&
        cbm_impact_walk_open_scoped(scope,&ss_policy,&walk)==CBM_STORE_OK &&
        cbm_impact_walk_run(walk,&f.ids.root,1)==CBM_STORE_OK && cbm_impact_walk_count(walk)==5 &&
        cbm_impact_walk_reached(walk,fresh);
    if (walk) { cbm_impact_walk_close(walk); }
    int reclosed=ss_scope_close(scope); bool cleanup=ss_file_close(&f);
    ASSERT_TRUE(setup); ASSERT_TRUE(legacy); ASSERT_TRUE(metadata); ASSERT_TRUE(cleanup);
    ASSERT_EQ(opened,CBM_STORE_OK); ASSERT_TRUE(bound); ASSERT_TRUE(committed); ASSERT_TRUE(old);
    ASSERT_EQ(closed,CBM_STORE_OK); ASSERT_EQ(reopened,CBM_STORE_OK); ASSERT_TRUE(newer);
    ASSERT_EQ(reclosed,CBM_STORE_OK); PASS();
}

TEST(store_scope_refuses_existing_transaction_and_busy_statement) {
    cbm_store_t *s=cbm_store_open_memory(); ss_ids_t id={0};
    bool setup=ss_seed(s,&id) && ss_sql(s,"PRAGMA busy_timeout=321; PRAGMA query_only=0")==SQLITE_OK;
    bool begun=setup && cbm_store_begin(s)==CBM_STORE_OK;
    cbm_store_read_scope_t *scope=SS_SENTINEL;
    int txrc=begun ? cbm_store_read_scope_open(s,NULL,NULL,&scope) : CBM_STORE_ERR;
    bool tx_clear=scope==NULL, tx_owned=begun && txrc!=CBM_STORE_SCOPE_DISCARD && !sqlite3_get_autocommit(cbm_store_get_db(s));
    if (scope && scope!=SS_SENTINEL) { (void)ss_scope_close(scope); scope=NULL; }
    bool tx_unchanged=tx_owned && ss_integer(s,"PRAGMA busy_timeout",321) && ss_integer(s,"PRAGMA query_only",0) &&
        ss_node(s,"root",88)==id.root && cbm_store_rollback(s)==CBM_STORE_OK && ss_line(s,id.root,11);
    sqlite3_stmt *q=NULL;
    bool busy=tx_unchanged && sqlite3_prepare_v2(cbm_store_get_db(s),"SELECT id FROM main.nodes ORDER BY id",-1,&q,NULL)==SQLITE_OK &&
        sqlite3_step(q)==SQLITE_ROW && sqlite3_stmt_busy(q);
    scope=SS_SENTINEL;
    int busyrc=busy ? cbm_store_read_scope_open(s,NULL,NULL,&scope) : CBM_STORE_ERR;
    bool busy_clear=scope==NULL;
    bool busy_unchanged=busy && busyrc!=CBM_STORE_SCOPE_DISCARD && sqlite3_stmt_busy(q) && sqlite3_step(q)==SQLITE_ROW &&
        ss_integer(s,"PRAGMA busy_timeout",321) && ss_integer(s,"PRAGMA query_only",0);
    if (scope && scope!=SS_SENTINEL) { (void)ss_scope_close(scope); scope=NULL; }
    if (q) { sqlite3_finalize(q); }
    bool idle=busy_unchanged && ss_line(s,id.root,11); /* Leaves ordinary cached statements idle. */
    scope=NULL; int goodrc=idle ? cbm_store_read_scope_open(s,NULL,NULL,&scope) : CBM_STORE_ERR;
    int closed=ss_scope_close(scope); cbm_store_close(s);
    ASSERT_TRUE(setup); ASSERT_TRUE(begun); ASSERT_EQ(txrc,CBM_STORE_ERR); ASSERT_TRUE(tx_clear); ASSERT_TRUE(tx_unchanged);
    ASSERT_TRUE(busy); ASSERT_EQ(busyrc,CBM_STORE_ERR); ASSERT_TRUE(busy_clear); ASSERT_TRUE(busy_unchanged);
    ASSERT_TRUE(idle); ASSERT_EQ(goodrc,CBM_STORE_OK); ASSERT_EQ(closed,CBM_STORE_OK); PASS();
}

TEST(store_scope_restores_query_only_timeout_and_cancelled_open) {
    for (int prior=0;prior<=1;prior++) {
        cbm_store_t *s=cbm_store_open_memory(); ss_ids_t id={0}; ss_cancel_t cancel={0};
        bool setup=ss_seed(s,&id) && ss_sql(s,prior?"PRAGMA query_only=1; PRAGMA busy_timeout=4321":"PRAGMA query_only=0; PRAGMA busy_timeout=4321")==SQLITE_OK;
        cbm_store_read_scope_t *scope=NULL;
        int opened=setup?cbm_store_read_scope_open(s,ss_cancel,&cancel,&scope):CBM_STORE_ERR;
        bool during=opened==CBM_STORE_OK && !sqlite3_get_autocommit(cbm_store_get_db(s)) &&
            ss_integer(s,"PRAGMA query_only",1) && ss_integer(s,"PRAGMA busy_timeout",0);
        int write_rc=during?ss_sql(s,"UPDATE main.nodes SET start_line=98"):SQLITE_ERROR;
        int normalized=opened==CBM_STORE_OK && prior?cbm_store_read_scope_fail(scope,CBM_STORE_NOT_FOUND):CBM_STORE_OK;
        int closed=ss_scope_close(scope); scope=SS_SENTINEL;
        bool restored=setup && opened!=CBM_STORE_SCOPE_DISCARD && closed!=CBM_STORE_SCOPE_DISCARD && sqlite3_get_autocommit(cbm_store_get_db(s)) && ss_integer(s,"PRAGMA query_only",prior) &&
            ss_integer(s,"PRAGMA busy_timeout",4321) && ss_line(s,id.root,11);
        cancel.stop=true;
        int cancelled=restored?cbm_store_read_scope_open(s,ss_cancel,&cancel,&scope):CBM_STORE_ERR;
        bool cleared=scope==NULL;
        if (scope && scope!=SS_SENTINEL) (void)ss_scope_close(scope);
        bool after=restored && cancelled!=CBM_STORE_SCOPE_DISCARD && sqlite3_get_autocommit(cbm_store_get_db(s)) && ss_integer(s,"PRAGMA query_only",prior) &&
            ss_integer(s,"PRAGMA busy_timeout",4321) && ss_integer(s,"SELECT 1",1);
        cbm_store_close(s);
        ASSERT_TRUE(setup); ASSERT_EQ(opened,CBM_STORE_OK); ASSERT_TRUE(during);
        ASSERT_EQ(write_rc&255,SQLITE_READONLY); ASSERT_EQ(normalized,prior?CBM_STORE_ERR:CBM_STORE_OK); ASSERT_EQ(closed,prior?CBM_STORE_ERR:CBM_STORE_OK); ASSERT_TRUE(restored);
        ASSERT_EQ(cancelled,CBM_STORE_CANCELLED); ASSERT_TRUE(cleared); ASSERT_TRUE(after);
    }
    ASSERT_EQ(cbm_store_read_scope_close(NULL),CBM_STORE_OK); PASS();
}

TEST(store_scope_nested_outline_and_compare_preserve_owned_guard) {
    ss_file_t f; bool setup=ss_file_open(&f);
    cbm_store_t *other=setup?cbm_store_open_path_query(f.path):NULL;
    cbm_graph_compare_result_t comparison={0};
    cbm_file_outline_row_t *rows=NULL; int count=0,total=0;
    bool compare_control=other && cbm_store_compare_graphs(f.reader,SS_PROJECT,other,SS_PROJECT,1000,NULL,NULL,NULL,NULL,&comparison)==CBM_STORE_OK;
    bool outline_control=compare_control && cbm_store_get_file_outline(other,SS_PROJECT,"unit.c",NULL,0,20,0,NULL,NULL,&rows,&count,&total)==CBM_STORE_OK && count==6 && total==6;
    cbm_store_free_file_outline(rows,count); rows=NULL; count=total=0;
    ss_cancel_t cancel={0}; cbm_store_read_scope_t *scope=NULL,*base_scope=NULL;
    int opened=outline_control?cbm_store_read_scope_open(other,ss_cancel,&cancel,&scope):CBM_STORE_ERR;
    int null_outline=opened==CBM_STORE_OK?cbm_store_get_file_outline(other,SS_PROJECT,"unit.c",NULL,0,20,0,NULL,NULL,&rows,&count,&total):CBM_STORE_OK;
    bool null_empty=rows==NULL && count==0; cbm_store_free_file_outline(rows,count); rows=NULL; count=total=0;
    ss_cancel_t nested={0};
    int cb_outline=opened==CBM_STORE_OK?cbm_store_get_file_outline(other,SS_PROJECT,"unit.c",NULL,0,20,0,ss_cancel,&nested,&rows,&count,&total):CBM_STORE_OK;
    bool cb_empty=rows==NULL && count==0; cbm_store_free_file_outline(rows,count);
    int compare_rc=opened==CBM_STORE_OK?cbm_store_compare_graphs(f.reader,SS_PROJECT,other,SS_PROJECT,1000,NULL,NULL,NULL,NULL,&comparison):CBM_STORE_OK;
    /* A refused second acquisition must not strand the first connection's guard/transaction. */
    int base_open=opened==CBM_STORE_OK?cbm_store_read_scope_open(f.reader,NULL,NULL,&base_scope):CBM_STORE_ERR;
    int base_close=ss_scope_close(base_scope);
    cancel.stop=true;
    int sql_rc=opened==CBM_STORE_OK?ss_sql(other,"WITH RECURSIVE n(x) AS (VALUES(0) UNION ALL SELECT x+1 FROM n WHERE x<10000) SELECT sum(x) FROM n"):SQLITE_ERROR;
    int check=scope?cbm_store_read_scope_check(scope):CBM_STORE_ERR;
    int closed=ss_scope_close(scope); cbm_store_close(other); bool cleanup=ss_file_close(&f);
    ASSERT_TRUE(setup); ASSERT_TRUE(compare_control); ASSERT_TRUE(outline_control); ASSERT_TRUE(cleanup);
    ASSERT_EQ(opened,CBM_STORE_OK); ASSERT_EQ(null_outline,CBM_STORE_ERR); ASSERT_TRUE(null_empty);
    ASSERT_EQ(cb_outline,CBM_STORE_ERR); ASSERT_TRUE(cb_empty); ASSERT_EQ(compare_rc,CBM_STORE_ERR);
    ASSERT_EQ(base_open,CBM_STORE_OK); ASSERT_EQ(base_close,CBM_STORE_OK);
    ASSERT_EQ(sql_rc&255,SQLITE_INTERRUPT); ASSERT_EQ(check,CBM_STORE_CANCELLED); ASSERT_EQ(closed,CBM_STORE_CANCELLED); PASS();
}

TEST(store_scope_sql_progress_cancellation_latches_and_unwinds) {
    cbm_store_t *s=cbm_store_open_memory(); ss_ids_t id={0}; ss_cancel_t cancel={0};
    bool setup=ss_seed(s,&id) && ss_integer(s,"WITH RECURSIVE n(x) AS (VALUES(0) UNION ALL SELECT x+1 FROM n WHERE x<10000) SELECT sum(x) FROM n",50005000);
    cbm_store_read_scope_t *scope=NULL;
    int opened=setup?cbm_store_read_scope_open(s,ss_cancel,&cancel,&scope):CBM_STORE_ERR;
    cancel.calls=0;
    bool sql_control=opened==CBM_STORE_OK && ss_integer(s,"WITH RECURSIVE n(x) AS (VALUES(0) UNION ALL SELECT x+1 FROM n WHERE x<10000) SELECT sum(x) FROM n",50005000);
    uint64_t polls=cancel.calls;
    cancel.calls=0; cancel.trip=polls/2+polls%2; cancel.armed=true;
    int sql_rc=sql_control && polls>0?ss_sql(s,"WITH RECURSIVE n(x) AS (VALUES(0) UNION ALL SELECT x+1 FROM n WHERE x<10000) SELECT sum(x) FROM n"):SQLITE_ERROR;
    bool polled=polls>0 && cancel.calls>=cancel.trip;
    cancel.armed=false; cancel.stop=false;
    int checked=scope?cbm_store_read_scope_check(scope):CBM_STORE_ERR;
    int closed=ss_scope_close(scope);
    bool released=setup && opened!=CBM_STORE_SCOPE_DISCARD && closed!=CBM_STORE_SCOPE_DISCARD && sqlite3_get_autocommit(cbm_store_get_db(s)) && ss_integer(s,"SELECT 7",7) && ss_line(s,id.root,11);
    cbm_store_close(s);
    ASSERT_TRUE(setup); ASSERT_EQ(opened,CBM_STORE_OK); ASSERT_TRUE(sql_control); ASSERT_GT(polls,0); ASSERT_TRUE(polled); ASSERT_EQ(sql_rc&255,SQLITE_INTERRUPT);
    ASSERT_EQ(checked,CBM_STORE_CANCELLED); ASSERT_EQ(closed,CBM_STORE_CANCELLED); ASSERT_TRUE(released); PASS();
}

typedef struct { bool setup, open, success, cancelled, hidden, restored; uint64_t calls; } ss_sweep_result_t;
static ss_sweep_result_t ss_sweep_run(uint64_t trip) {
    ss_sweep_result_t out={0}; cbm_store_t *s=cbm_store_open_memory(); ss_ids_t id={0}; ss_cancel_t cancel={0};
    out.setup=ss_seed(s,&id);
    for (int i=31;out.setup && i>=0;i--) {
        char name[48]; (void)snprintf(name,sizeof(name),"fan_%02d",i);
        int64_t node=ss_node(s,name,100+i);
        out.setup=node>0 && ss_edge(s,node,id.root,"CALLS");
    }
    cbm_store_read_scope_t *scope=NULL; cbm_impact_walk_t *walk=NULL;
    int rc=out.setup?cbm_store_read_scope_open(s,ss_cancel,&cancel,&scope):CBM_STORE_ERR;
    out.open=rc==CBM_STORE_OK;
    cancel.calls=0; cancel.trip=trip; cancel.armed=trip!=0;
    if (rc==CBM_STORE_OK) rc=cbm_impact_walk_open_scoped(scope,&ss_policy,&walk);
    int64_t sinks[4]={id.z,id.tail,id.z,id.tail};
    if (rc==CBM_STORE_OK) rc=cbm_impact_walk_add_sinks(walk,sinks,4);
    int64_t seeds[4]={id.root,id.root,id.root,id.root};
    if (rc==CBM_STORE_OK) rc=cbm_impact_walk_run(walk,seeds,4);
    if (rc==CBM_STORE_OK) rc=cbm_impact_walk_run(walk,&id.island,1);
    out.calls=cancel.calls;
    out.success=rc==CBM_STORE_OK && walk && cbm_impact_walk_count(walk)==38 &&
        cbm_impact_walk_reached(walk,id.root) && cbm_impact_walk_reached(walk,id.tail);
    if (trip) {
        /* Stop asserting the predicate, then require the operation's latch to persist. */
        cancel.armed=false; cancel.stop=false;
        out.cancelled=rc==CBM_STORE_CANCELLED && scope && cbm_store_read_scope_check(scope)==CBM_STORE_CANCELLED;
        out.hidden=walk?ss_hidden(walk,id.root):rc!=CBM_STORE_OK;
    }
    if (walk) { cbm_impact_walk_close(walk); }
    int closed=ss_scope_close(scope);
    out.restored=out.open && closed==(trip?CBM_STORE_CANCELLED:CBM_STORE_OK) &&
        sqlite3_get_autocommit(cbm_store_get_db(s)) && ss_integer(s,"PRAGMA query_only",0) && ss_integer(s,"SELECT 1",1);
    cbm_store_close(s); return out;
}
TEST(store_scope_counter_sweep_never_exposes_a_cancelled_prefix) {
    ss_sweep_result_t control=ss_sweep_run(0);
    ASSERT_TRUE(control.setup); ASSERT_TRUE(control.open); ASSERT_TRUE(control.success); ASSERT_TRUE(control.restored);
    ASSERT_GT(control.calls,1);
    /* Nine evenly spaced positions from this successful fixed fixture. Counts
     * are observed, never hardcoded. This is not a sort/loop phase identifier. */
    uint64_t previous=0;
    for (unsigned i=0;i<9;i++) {
        uint64_t span=control.calls-1;
        uint64_t trip=1+(span/8)*i+((span%8)*i)/8;
        if (trip==previous) { continue; }
        previous=trip;
        ss_sweep_result_t result=ss_sweep_run(trip);
        if (!result.cancelled || !result.hidden)
            fprintf(stderr,"scope callback sweep position %llu of %llu\n",(unsigned long long)trip,(unsigned long long)control.calls);
        ASSERT_TRUE(result.setup); ASSERT_TRUE(result.open); ASSERT_TRUE(result.cancelled);
        ASSERT_TRUE(result.hidden); ASSERT_TRUE(result.restored); ASSERT_GTE(result.calls,trip);
    }
    PASS();
}

TEST(store_scope_failed_later_lift_hides_previous_hits) {
    for (int invalid=0;invalid<2;invalid++) {
        cbm_store_t *s=cbm_store_open_memory(); ss_ids_t id={0}; ss_cancel_t cancel={0};
        bool setup=ss_seed(s,&id) && ss_normal_walk(s,&id);
        cbm_store_read_scope_t *scope=NULL; cbm_impact_walk_t *walk=NULL;
        int opened=setup?cbm_store_read_scope_open(s,ss_cancel,&cancel,&scope):CBM_STORE_ERR;
        bool prior=opened==CBM_STORE_OK && cbm_impact_walk_open_scoped(scope,&ss_policy,&walk)==CBM_STORE_OK &&
            cbm_impact_walk_run(walk,&id.root,1)==CBM_STORE_OK && ss_golden(walk,&id,false);
        cancel.stop=!invalid;
        int64_t next=invalid?INT64_MAX:id.island;
        int rc=prior?cbm_impact_walk_run(walk,&next,1):CBM_STORE_OK;
        cancel.stop=false;
        int expected=invalid?CBM_STORE_ERR:CBM_STORE_CANCELLED;
        bool hidden=walk && ss_hidden(walk,id.root);
        int checked=scope?cbm_store_read_scope_check(scope):CBM_STORE_OK;
        int retained=scope?cbm_store_read_scope_fail(scope,invalid?CBM_STORE_CANCELLED:CBM_STORE_ERR):CBM_STORE_OK;
        if (walk) { cbm_impact_walk_close(walk); }
        int closed=ss_scope_close(scope);
        bool usable=setup && opened!=CBM_STORE_SCOPE_DISCARD && closed!=CBM_STORE_SCOPE_DISCARD && ss_normal_walk(s,&id);
        cbm_store_close(s);
        ASSERT_TRUE(setup); ASSERT_EQ(opened,CBM_STORE_OK); ASSERT_TRUE(prior);
        ASSERT_EQ(rc,expected); ASSERT_TRUE(hidden); ASSERT_EQ(checked,expected); ASSERT_EQ(retained,expected);
        ASSERT_EQ(closed,expected); ASSERT_TRUE(usable);
    }
    PASS();
}

static int ss_busy_count(void *opaque,int attempt) {
    (void)attempt; int *calls=opaque; (*calls)++; return 0; /* Never waits or sleeps. */
}
TEST(store_scope_held_lock_refuses_without_calling_busy_handler) {
    ss_file_t f; bool setup=ss_file_open(&f);
    /* DELETE mode supplies a deterministic conflicting main-table read lock. */
    cbm_store_close(f.reader); f.reader=NULL;
    bool mode=setup && ss_sql(f.writer,"PRAGMA journal_mode=DELETE")==SQLITE_OK && ss_text(f.writer,"PRAGMA journal_mode","delete");
    if (mode) f.reader=cbm_store_open_path_query(f.path);
    int busy_calls=0;
    bool ready=f.reader && ss_line(f.reader,f.ids.root,11) &&
        sqlite3_busy_handler(cbm_store_get_db(f.reader),ss_busy_count,&busy_calls)==SQLITE_OK;
    bool locked=ready && ss_sql(f.writer,"BEGIN EXCLUSIVE")==SQLITE_OK;
    cbm_store_read_scope_t *scope=SS_SENTINEL;
    int rc=locked?cbm_store_read_scope_open(f.reader,NULL,NULL,&scope):CBM_STORE_OK;
    bool cleared=scope==NULL;
    if (scope && scope!=SS_SENTINEL) (void)ss_scope_close(scope);
    int observed=busy_calls;
    bool unlocked=locked && ss_sql(f.writer,"ROLLBACK")==SQLITE_OK;
    scope=NULL;
    int good=unlocked && rc!=CBM_STORE_SCOPE_DISCARD?cbm_store_read_scope_open(f.reader,NULL,NULL,&scope):CBM_STORE_ERR;
    int closed=ss_scope_close(scope);
    bool cleanup=ss_file_close(&f);
    ASSERT_TRUE(setup); ASSERT_TRUE(mode); ASSERT_TRUE(ready); ASSERT_TRUE(locked); ASSERT_TRUE(cleanup);
    ASSERT_EQ(rc,CBM_STORE_ERR); ASSERT_TRUE(cleared); ASSERT_EQ(observed,0); ASSERT_TRUE(unlocked);
    ASSERT_EQ(good,CBM_STORE_OK); ASSERT_EQ(closed,CBM_STORE_OK); PASS();
}

typedef struct { bool deny_restore, deny_pin, pin_seen, restore_armed; int restore_denied; } ss_auth_t;
static int ss_authorize(void *opaque,int action,const char *a,const char *b,const char *db,const char *trigger) {
    (void)trigger; ss_auth_t *auth=opaque;
    if (action==SQLITE_READ && ss_eq(a,"nodes") && ss_eq(db,"main") && auth->deny_pin) {
        auth->pin_seen=true; auth->restore_armed=true; return SQLITE_DENY;
    }
    if (action==SQLITE_PRAGMA && a && sqlite3_stricmp(a,"query_only")==0 && b &&
        auth->deny_restore && auth->restore_armed) {
        auth->restore_denied++; return SQLITE_DENY;
    }
    return SQLITE_OK;
}
TEST(store_scope_close_restore_denial_requires_connection_discard) {
    cbm_store_t *s=cbm_store_open_memory(); ss_ids_t id={0};
    bool setup=ss_seed(s,&id) && ss_sql(s,"PRAGMA query_only=0; PRAGMA busy_timeout=2468")==SQLITE_OK;
    cbm_store_read_scope_t *scope=NULL;
    int opened=setup?cbm_store_read_scope_open(s,NULL,NULL,&scope):CBM_STORE_ERR;
    ss_auth_t auth={.deny_restore=true,.restore_armed=true};
    bool installed=opened==CBM_STORE_OK && sqlite3_set_authorizer(cbm_store_get_db(s),ss_authorize,&auth)==SQLITE_OK;
    /* An operation error must not mask a subsequent restoration failure. */
    int latched=installed?cbm_store_read_scope_fail(scope,CBM_STORE_CANCELLED):CBM_STORE_ERR;
    int closed=ss_scope_close(scope); scope=NULL;
    cbm_store_close(s); s=NULL; /* DISCARD: do not query/reuse the connection or close the scope twice. */
    ASSERT_TRUE(setup); ASSERT_EQ(opened,CBM_STORE_OK); ASSERT_TRUE(installed);
    ASSERT_EQ(latched,CBM_STORE_CANCELLED); ASSERT_GT(auth.restore_denied,0);
    ASSERT_EQ(closed,CBM_STORE_SCOPE_DISCARD); PASS();
}
TEST(store_scope_failed_open_reports_cleanup_discard_without_a_handle) {
    for (int denial=0;denial<=1;denial++) {
        cbm_store_t *s=cbm_store_open_memory(); ss_ids_t id={0};
        bool setup=ss_seed(s,&id) && ss_sql(s,"PRAGMA query_only=0; PRAGMA busy_timeout=2468")==SQLITE_OK;
        ss_auth_t auth={.deny_restore=denial!=0,.deny_pin=true};
        bool installed=setup && sqlite3_set_authorizer(cbm_store_get_db(s),ss_authorize,&auth)==SQLITE_OK;
        cbm_store_read_scope_t *scope=SS_SENTINEL;
        int rc=installed?cbm_store_read_scope_open(s,NULL,NULL,&scope):CBM_STORE_OK;
        bool cleared=scope==NULL;
        if (scope && scope!=SS_SENTINEL) { (void)ss_scope_close(scope); scope=NULL; }
        bool restored=false;
        if (installed && !denial && rc!=CBM_STORE_SCOPE_DISCARD) {
            restored=sqlite3_set_authorizer(cbm_store_get_db(s),NULL,NULL)==SQLITE_OK &&
                sqlite3_get_autocommit(cbm_store_get_db(s)) && ss_integer(s,"PRAGMA query_only",0) &&
                ss_integer(s,"PRAGMA busy_timeout",2468) && ss_normal_walk(s,&id);
        }
        cbm_store_close(s); s=NULL; /* Mandatory disposal on the denial branch. */
        ASSERT_TRUE(setup); ASSERT_TRUE(installed); ASSERT_TRUE(auth.pin_seen); ASSERT_TRUE(cleared);
        ASSERT_EQ(rc,denial?CBM_STORE_SCOPE_DISCARD:CBM_STORE_ERR);
        if (denial) ASSERT_GT(auth.restore_denied,0); else ASSERT_TRUE(restored);
    }
    PASS();
}

TEST(store_scope_legacy_golden_order_parents_and_lifts_are_preserved) {
    cbm_store_t *s=cbm_store_open_memory(); ss_ids_t id={0}; bool setup=ss_seed(s,&id);
    cbm_impact_walk_t *legacy=NULL,*scoped=NULL;
    bool original=setup && cbm_impact_walk_open(s,&ss_policy,&legacy)==CBM_STORE_OK &&
        cbm_impact_walk_run(legacy,&id.root,1)==CBM_STORE_OK && ss_golden(legacy,&id,false) &&
        cbm_impact_walk_run(legacy,&id.island,1)==CBM_STORE_OK && ss_golden(legacy,&id,true);
    cbm_store_read_scope_t *scope=NULL;
    int opened=original?cbm_store_read_scope_open(s,NULL,NULL,&scope):CBM_STORE_ERR;
    bool equivalent=opened==CBM_STORE_OK && cbm_impact_walk_open_scoped(scope,&ss_policy,&scoped)==CBM_STORE_OK &&
        cbm_impact_walk_run(scoped,&id.root,1)==CBM_STORE_OK && ss_golden(scoped,&id,false) &&
        cbm_impact_walk_run(scoped,&id.island,1)==CBM_STORE_OK && ss_golden(scoped,&id,true);
    if (scoped) { cbm_impact_walk_close(scoped); }
    if (legacy) { cbm_impact_walk_close(legacy); }
    int closed=ss_scope_close(scope); cbm_store_close(s);
    ASSERT_TRUE(setup); ASSERT_TRUE(original); ASSERT_EQ(opened,CBM_STORE_OK); ASSERT_TRUE(equivalent);
    ASSERT_EQ(closed,CBM_STORE_OK); PASS();
}

SUITE(store_scope) {
    RUN_TEST(store_scope_wal_snapshot_pins_metadata_nodes_and_edges);
    RUN_TEST(store_scope_refuses_existing_transaction_and_busy_statement);
    RUN_TEST(store_scope_restores_query_only_timeout_and_cancelled_open);
    RUN_TEST(store_scope_nested_outline_and_compare_preserve_owned_guard);
    RUN_TEST(store_scope_sql_progress_cancellation_latches_and_unwinds);
    RUN_TEST(store_scope_counter_sweep_never_exposes_a_cancelled_prefix);
    RUN_TEST(store_scope_failed_later_lift_hides_previous_hits);
    RUN_TEST(store_scope_held_lock_refuses_without_calling_busy_handler);
    RUN_TEST(store_scope_close_restore_denial_requires_connection_discard);
    RUN_TEST(store_scope_failed_open_reports_cleanup_discard_without_a_handle);
    RUN_TEST(store_scope_legacy_golden_order_parents_and_lifts_are_preserved);
}
