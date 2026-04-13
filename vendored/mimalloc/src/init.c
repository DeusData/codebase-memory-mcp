/* ----------------------------------------------------------------------------
Copyright (c) 2018-2025, Microsoft Research, Daan Leijen
This is free software; you can redistribute it and/or modify it under the
terms of the MIT license. A copy of the license can be found in the file
"LICENSE" at the root of this distribution.
-----------------------------------------------------------------------------*/
#include "mimalloc.h"
#include "mimalloc/internal.h"
#include "mimalloc/prim.h"

#include <string.h>  // memcpy, memset
#include <stdlib.h>  // atexit

#define MI_MEMID_INIT(kind)   {{{NULL,0}}, kind, true /* pinned */, true /* committed */, false /* zero */ }
#define MI_MEMID_STATIC       MI_MEMID_INIT(MI_MEM_STATIC)

// Empty page used to initialize the small free pages array
const mi_page_t _mi_page_empty = {
  MI_ATOMIC_VAR_INIT(0),  // xthread_id
  NULL,                   // free
  0,                      // used
  0,                      // capacity
  0,                      // reserved capacity
  0,                      // retire_expire
  false,                  // is_zero
  NULL,                   // local_free
  MI_ATOMIC_VAR_INIT(0),  // xthread_free
  0,                      // block_size
  NULL,                   // page_start
  #if (MI_PADDING || MI_ENCODE_FREELIST)
  { 0, 0 },               // keys
  #endif
  NULL,                   // theap
  NULL,                   // heap
  NULL, NULL,             // next, prev
  MI_ARENA_SLICE_SIZE,    // page_committed
  MI_MEMID_STATIC         // memid
};

#define MI_PAGE_EMPTY() ((mi_page_t*)&_mi_page_empty)

#if (MI_PADDING>0) && (MI_INTPTR_SIZE >= 8)
#define MI_SMALL_PAGES_EMPTY  { MI_INIT128(MI_PAGE_EMPTY), MI_PAGE_EMPTY(), MI_PAGE_EMPTY() }
#elif (MI_PADDING>0)
#define MI_SMALL_PAGES_EMPTY  { MI_INIT128(MI_PAGE_EMPTY), MI_PAGE_EMPTY(), MI_PAGE_EMPTY(), MI_PAGE_EMPTY() }
#else
#define MI_SMALL_PAGES_EMPTY  { MI_INIT128(MI_PAGE_EMPTY), MI_PAGE_EMPTY() }
#endif


// Empty page queues for every bin
#define QNULL(sz)  { NULL, NULL, 0, (sz)*sizeof(uintptr_t) }
#define MI_PAGE_QUEUES_EMPTY \
  { QNULL(1), \
    QNULL(     1), QNULL(     2), QNULL(     3), QNULL(     4), QNULL(     5), QNULL(     6), QNULL(     7), QNULL(     8), /* 8 */ \
    QNULL(    10), QNULL(    12), QNULL(    14), QNULL(    16), QNULL(    20), QNULL(    24), QNULL(    28), QNULL(    32), /* 16 */ \
    QNULL(    40), QNULL(    48), QNULL(    56), QNULL(    64), QNULL(    80), QNULL(    96), QNULL(   112), QNULL(   128), /* 24 */ \
    QNULL(   160), QNULL(   192), QNULL(   224), QNULL(   256), QNULL(   320), QNULL(   384), QNULL(   448), QNULL(   512), /* 32 */ \
    QNULL(   640), QNULL(   768), QNULL(   896), QNULL(  1024), QNULL(  1280), QNULL(  1536), QNULL(  1792), QNULL(  2048), /* 40 */ \
    QNULL(  2560), QNULL(  3072), QNULL(  3584), QNULL(  4096), QNULL(  5120), QNULL(  6144), QNULL(  7168), QNULL(  8192), /* 48 */ \
    QNULL( 10240), QNULL( 12288), QNULL( 14336), QNULL( 16384), QNULL( 20480), QNULL( 24576), QNULL( 28672), QNULL( 32768), /* 56 */ \
    QNULL( 40960), QNULL( 49152), QNULL( 57344), QNULL( 65536), QNULL( 81920), QNULL( 98304), QNULL(114688), QNULL(131072), /* 64 */ \
    QNULL(163840), QNULL(196608), QNULL(229376), QNULL(262144), QNULL(327680), QNULL(393216), QNULL(458752), QNULL(524288), /* 72 */ \
    QNULL(MI_LARGE_MAX_OBJ_WSIZE + 1  /* 655360, Huge queue */), \
    QNULL(MI_LARGE_MAX_OBJ_WSIZE + 2) /* Full queue */ }

#define MI_STAT_COUNT_NULL()  {0,0,0}

// Empty statistics
#define MI_STATS_NULL  \
  MI_STAT_COUNT_NULL(), MI_STAT_COUNT_NULL(), MI_STAT_COUNT_NULL(), \
  { 0 }, { 0 }, \
  MI_STAT_COUNT_NULL(), MI_STAT_COUNT_NULL(), MI_STAT_COUNT_NULL(), \
  MI_STAT_COUNT_NULL(), MI_STAT_COUNT_NULL(), MI_STAT_COUNT_NULL(), \
  { 0 }, { 0 }, { 0 }, { 0 }, \
  { 0 }, { 0 }, { 0 }, { 0 }, \
  \
  { 0 }, { 0 }, { 0 }, { 0 }, { 0 }, { 0 }, \
  MI_INIT5(MI_STAT_COUNT_NULL), \
  { 0 }, { 0 }, { 0 }, { 0 },  \
  \
  { MI_INIT4(MI_STAT_COUNT_NULL) }, \
  { { 0 }, { 0 }, { 0 }, { 0 } }, \
  \
  { MI_INIT74(MI_STAT_COUNT_NULL) }, \
  { MI_INIT74(MI_STAT_COUNT_NULL) }, \
  { MI_INIT5(MI_STAT_COUNT_NULL) }

// --------------------------------------------------------
// Statically allocate an empty theap as the initial
// thread local value for the default theap,
// and statically allocate the backing theap for the main
// thread so it can function without doing any allocation
// itself (as accessing a thread local for the first time
// may lead to allocation itself on some platforms)
// --------------------------------------------------------

static mi_decl_cache_align mi_subproc_t subproc_main
#if __cplusplus
= { };     // empty initializer to prevent running the constructor (with msvc)
#else
= { 0 };   // C zero initialize
#endif

static mi_subproc_t* subprocs = &subproc_main;
static mi_lock_t     subprocs_lock;

static mi_decl_cache_align mi_tld_t tld_empty = {
  0,                      // thread_id
  0,                      // thread_seq
  0,                      // default numa node
  &subproc_main,          // subproc
  NULL,                   // theaps list
  false,                  // recurse
  false,                  // is_in_threadpool
  MI_MEMID_STATIC         // memid
};

mi_decl_cache_align const mi_theap_t _mi_theap_empty = {
  &tld_empty,             // tld
  NULL,                   // heap
  0,                      // heartbeat
  0,                      // cookie
  { {0}, {0}, 0, true },  // random
  0,                      // page count
  MI_BIN_FULL, 0,         // page retired min/max
  0, 0,                   // generic count
  NULL, NULL,             // tnext, tprev
  NULL, NULL,             // hnext, hprev
  0,                      // full page retain
  false,                  // allow reclaim
  true,                   // allow abandon
  #if MI_GUARDED
  0, 0, 0, 1,             // sample count is 1 so we never write to it (see `internal.h:mi_theap_malloc_use_guarded`)
  #endif
  MI_SMALL_PAGES_EMPTY,
  MI_PAGE_QUEUES_EMPTY,
  MI_MEMID_STATIC,
  { sizeof(mi_stats_t), MI_STAT_VERSION, MI_STATS_NULL },      // stats
};

mi_decl_cache_align const mi_theap_t _mi_theap_empty_wrong = {
  &tld_empty,             // tld
  NULL,                   // heap
  0,                      // heartbeat
  0,                      // cookie
  { {0}, {0}, 0, true },  // random
  0,                      // page count
  MI_BIN_FULL, 0,         // page retired min/max
  0, 0,                   // generic count
  NULL, NULL,             // tnext, tprev
  NULL, NULL,             // hnext, hprev
  0,                      // full page retain
  false,                  // allow reclaim
  true,                   // allow abandon
  #if MI_GUARDED
  0, 0, 0, 1,             // sample count is 1 so we never write to it (see `internal.h:mi_theap_malloc_use_guarded`)
  #endif
  MI_SMALL_PAGES_EMPTY,
  MI_PAGE_QUEUES_EMPTY,
  MI_MEMID_STATIC,
  { sizeof(mi_stats_t), MI_STAT_VERSION, MI_STATS_NULL },      // stats
};

// Heap for the main thread

extern mi_decl_hidden mi_decl_cache_align mi_theap_t theap_main;
extern mi_decl_hidden mi_decl_cache_align mi_heap_t  heap_main;

static mi_decl_cache_align mi_tld_t tld_main = {
  0,                      // thread_id
  0,                      // thread_seq
  0,                      // numa node
  &subproc_main,          // subproc
  &theap_main,            // theaps list
  false,                  // recurse
  false,                  // is_in_threadpool
  MI_MEMID_STATIC         // memid
};

mi_decl_cache_align mi_theap_t theap_main = {
  &tld_main,              // thread local data
  &heap_main,             // main heap
  0,                      // heartbeat
  0,                      // initial cookie
  { {0x846ca68b}, {0}, 0, true },  // random
  0,                      // page count
  MI_BIN_FULL, 0,         // page retired min/max
  0, 0,                   // generic count
  NULL, NULL,             // tnext, tprev
  NULL, NULL,             // hnext, hprev
  2,                      // full page retain
  true,                   // allow page reclaim
  true,                   // allow page abandon
  #if MI_GUARDED
  0, 0, 0, 0,
  #endif
  MI_SMALL_PAGES_EMPTY,
  MI_PAGE_QUEUES_EMPTY,
  MI_MEMID_STATIC,
  { sizeof(mi_stats_t), MI_STAT_VERSION, MI_STATS_NULL },      // stats
};

mi_decl_cache_align mi_heap_t heap_main
#if __cplusplus
  = { };     // empty initializer to prevent running the constructor (with msvc)
#else
  = { 0 };   // C zero initialize
#endif

mi_threadid_t _mi_thread_id(void) mi_attr_noexcept {
  return _mi_prim_thread_id();
}

// the theap belonging to the main heap
mi_decl_hidden mi_decl_thread mi_theap_t* __mi_theap_main = NULL;

#if MI_TLS_MODEL_THREAD_LOCAL
// the thread-local main theap for allocation
mi_decl_hidden mi_decl_thread mi_theap_t* __mi_theap_default = (mi_theap_t*)&_mi_theap_empty;
// the last used non-main theap
mi_decl_hidden mi_decl_thread mi_theap_t* __mi_theap_cached = (mi_theap_t*)&_mi_theap_empty;
#endif

bool _mi_process_is_initialized = false;  // set to `true` in `mi_process_init`.

mi_stats_t _mi_stats_main = { sizeof(mi_stats_t), MI_STAT_VERSION, MI_STATS_NULL };

#if MI_GUARDED
mi_decl_export void mi_theap_guarded_set_sample_rate(mi_theap_t* theap, size_t sample_rate, size_t seed) {
  theap->guarded_sample_rate  = sample_rate;
  theap->guarded_sample_count = sample_rate;  // count down samples
  if (theap->guarded_sample_rate > 1) {
    if (seed == 0) {
      seed = _mi_theap_random_next(theap);
    }
    theap->guarded_sample_count = (seed % theap->guarded_sample_rate) + 1;  // start at random count between 1 and `sample_rate`
  }
}

mi_decl_export void mi_theap_guarded_set_size_bound(mi_theap_t* theap, size_t min, size_t max) {
  theap->guarded_size_min = min;
  theap->guarded_size_max = (min > max ? min : max);
}

void _mi_theap_guarded_init(mi_theap_t* theap) {
  mi_theap_guarded_set_sample_rate(theap,
    (size_t)mi_option_get_clamp(mi_option_guarded_sample_rate, 0, LONG_MAX),
    (size_t)mi_option_get(mi_option_guarded_sample_seed));
  mi_theap_guarded_set_size_bound(theap,
    (size_t)mi_option_get_clamp(mi_option_guarded_min, 0, LONG_MAX),
    (size_t)mi_option_get_clamp(mi_option_guarded_max, 0, LONG_MAX) );
}
#else
mi_decl_export void mi_theap_guarded_set_sample_rate(mi_theap_t* theap, size_t sample_rate, size_t seed) {
  MI_UNUSED(theap); MI_UNUSED(sample_rate); MI_UNUSED(seed);
}

mi_decl_export void mi_theap_guarded_set_size_bound(mi_theap_t* theap, size_t min, size_t max) {
  MI_UNUSED(theap); MI_UNUSED(min); MI_UNUSED(max);
}
void _mi_theap_guarded_init(mi_theap_t* theap) {
  MI_UNUSED(theap);
}
#endif

/* -----------------------------------------------------------
  Initialization
  Note: on some platforms lock_init or just a thread local access
  can cause allocation and induce recursion during initialization.
----------------------------------------------------------- */


// Initialize main subproc
static void mi_subproc_main_init(void) {
  if (subproc_main.memid.memkind != MI_MEM_STATIC) {
    subproc_main.memid = _mi_memid_create(MI_MEM_STATIC);
    subproc_main.heaps = &heap_main;
    subproc_main.heap_total_count = 1;
    subproc_main.heap_count = 1;
    mi_atomic_store_ptr_release(mi_heap_t, &subproc_main.heap_main, &heap_main);
    __mi_stat_increase_mt(&subproc_main.stats.heaps, 1);
    mi_lock_init(&subproc_main.arena_reserve_lock);
    mi_lock_init(&subproc_main.heaps_lock);
    mi_lock_init(&subprocs_lock);
  }
}

// Initialize main tld
static void mi_tld_main_init(void) {
  if (tld_main.thread_id == 0) {
    tld_main.thread_id = _mi_prim_thread_id();
  }
}

void _mi_theap_options_init(mi_theap_t* theap) {
  theap->allow_page_reclaim = (mi_option_get(mi_option_page_reclaim_on_free) >= 0);
  theap->allow_page_abandon = (mi_option_get(mi_option_page_full_retain) >= 0);
  theap->page_full_retain = mi_option_get_clamp(mi_option_page_full_retain, -1, 32);
}

// Initialization of the (statically allocated) main theap, and the main tld and subproc.
static void mi_theap_main_init(void) {
  if mi_unlikely(theap_main.memid.memkind != MI_MEM_STATIC) {
    // theap
    theap_main.memid = _mi_memid_create(MI_MEM_STATIC);
    #if defined(__APPLE__) || defined(_WIN32) && !defined(MI_SHARED_LIB)
      _mi_random_init_weak(&theap_main.random);    // prevent allocation failure during bcrypt dll initialization with static linking (issue #1185)
    #else
      _mi_random_init(&theap_main.random);
    #endif
    theap_main.cookie  = _mi_theap_random_next(&theap_main);
    _mi_theap_options_init(&theap_main);
    _mi_theap_guarded_init(&theap_main);
  }
}

// Initialize main heap
static void mi_heap_main_init(void) {
  if mi_unlikely(heap_main.subproc == NULL) {
    heap_main.subproc = &subproc_main;
    heap_main.theaps = &theap_main;

    mi_theap_main_init();
    mi_subproc_main_init();
    mi_tld_main_init();

    mi_lock_init(&heap_main.theaps_lock);
    mi_lock_init(&heap_main.os_abandoned_pages_lock);
    mi_lock_init(&heap_main.arena_pages_lock);
  }
}


/* -----------------------------------------------------------
  Thread local data
----------------------------------------------------------- */

// Allocate fresh tld
static mi_tld_t* mi_tld_alloc(void) {
  if (_mi_is_main_thread()) {
    mi_atomic_increment_relaxed(&tld_main.subproc->thread_count);
    return &tld_main;
  }
  else {
    // allocate tld meta-data
    // note: we need to be careful to not access the tld from `_mi_meta_zalloc`
    // (and in turn from `_mi_arena_alloc_aligned` and `_mi_os_alloc_aligned`).
    mi_memid_t memid;
    mi_tld_t* tld = (mi_tld_t*)_mi_meta_zalloc(sizeof(mi_tld_t), &memid);
    if (tld==NULL) {
      _mi_error_message(ENOMEM, "unable to allocate memory for thread local data\n");
      return NULL;
    }
    tld->memid = memid;
    tld->theaps = NULL;
    tld->subproc = &subproc_main;
    tld->numa_node = _mi_os_numa_node();
    tld->thread_id = _mi_prim_thread_id();
    tld->thread_seq = mi_atomic_increment_relaxed(&tld->subproc->thread_total_count);
    tld->is_in_threadpool = _mi_prim_thread_is_in_threadpool();
    mi_atomic_increment_relaxed(&tld->subproc->thread_count);
    return tld;
  }
}

#define MI_TLD_INVALID  ((mi_tld_t*)1)

mi_decl_noinline static void mi_tld_free(mi_tld_t* tld) {
  if (tld != NULL && tld != MI_TLD_INVALID) {
    mi_atomic_decrement_relaxed(&tld->subproc->thread_count);
    _mi_meta_free(tld, sizeof(mi_tld_t), tld->memid);
  }
  #if 0
  // do not read/write to `thread_tld` on older macOS <= 14 as that will re-initialize the thread local storage
  // (since we are calling this during pthread shutdown)
  // (and this could happen on other systems as well, so let's never do it)
  thread_tld = MI_TLD_INVALID;
  #endif
}

// return the thread local heap ensuring it is initialized (and not `NULL` or `&_mi_theap_empty`);
mi_theap_t* _mi_theap_default_safe(void) {
  mi_theap_t* theap = _mi_theap_default();
  if mi_likely(mi_theap_is_initialized(theap)) return theap;
  mi_thread_init();
  mi_assert_internal(mi_theap_is_initialized(_mi_theap_default()));
  return _mi_theap_default();
}


mi_subproc_t* _mi_subproc_main(void) {
  return &subproc_main;
}

mi_subproc_t* _mi_subproc(void) {
  // should work without doing initialization (as it may be called from `_mi_tld -> mi_tld_alloc ... -> os_alloc -> _mi_subproc()`
  // todo: this will still fail on OS systems where the first access to a thread-local causes allocation.
  //       on such systems we can check for this with the _mi_prim_get_default_theap as those are protected (by being
  //       stored in a TLS slot for example)
  mi_theap_t* theap = _mi_theap_default();
  if (theap == NULL) {
    return _mi_subproc_main();
  }
  else {
    return theap->tld->subproc;  // avoid using thread local storage (`thread_tld`)
  }
}

mi_heap_t* _mi_subproc_heap_main(mi_subproc_t* subproc) {
  mi_heap_t* heap = mi_atomic_load_ptr_relaxed(mi_heap_t,&subproc->heap_main);
  if mi_likely(heap!=NULL) {
    return heap;
  }
  else {
    mi_heap_main_init();
    mi_assert_internal(mi_atomic_load_relaxed(&subproc->heap_main) != NULL);
    return mi_atomic_load_ptr_relaxed(mi_heap_t,&subproc->heap_main);
  }
}

mi_heap_t* mi_heap_main(void) {
  return _mi_subproc_heap_main(_mi_subproc()); // don't use _mi_theap_main() so this call works during process_init
}

bool _mi_is_heap_main(const mi_heap_t* heap) {
  mi_assert_internal(heap!=NULL);
  return (_mi_subproc_heap_main(heap->subproc) == heap);
}

/* -----------------------------------------------------------
  Sub process
----------------------------------------------------------- */

mi_subproc_id_t mi_subproc_main(void) {
  return _mi_subproc_main();
}

mi_subproc_id_t mi_subproc_current(void) {
  return _mi_subproc();
}

mi_subproc_id_t mi_subproc_new(void) {
  static _Atomic(size_t) subproc_total_count;
  mi_memid_t memid;
  mi_subproc_t* subproc = (mi_subproc_t*)_mi_meta_zalloc(sizeof(mi_subproc_t),&memid);
  if (subproc == NULL) return NULL;
  subproc->memid = memid;
  subproc->subproc_seq = mi_atomic_increment_relaxed(&subproc_total_count) + 1;
  mi_lock_init(&subproc->arena_reserve_lock);
  mi_lock_init(&subproc->heaps_lock);
  mi_lock(&subprocs_lock) {
    // push on subproc list
    subproc->next = subprocs;
    if (subprocs!=NULL) { subprocs->prev = subproc; }
    subprocs = subproc;
  }
  return subproc;
}

mi_subproc_t* _mi_subproc_from_id(mi_subproc_id_t subproc_id) {
  return (subproc_id == NULL ? &subproc_main : (mi_subproc_t*)subproc_id);
}

// destroy all subproc resources including arena's, heap's etc.
static void mi_subproc_unsafe_destroy(mi_subproc_t* subproc)
{
  // remove from the subproc list
  mi_lock(&subprocs_lock) {
    if (subproc->next!=NULL) { subproc->next->prev = subproc->prev;  }
    if (subproc->prev!=NULL) { subproc->prev->next = subproc->next;  }
                        else { mi_assert_internal(subprocs==subproc);  subprocs = subproc->next; }
  }

  // destroy all subproc heaps
  mi_lock(&subproc->heaps_lock) {
    mi_heap_t* heap = subproc->heaps;
    while (heap != NULL) {
      mi_heap_t* next = heap->next;
      if (heap!=subproc->heap_main) {mi_heap_destroy(heap); }
      heap = next;
    }
    mi_assert_internal(subproc->heaps == subproc->heap_main);
    mi_heap_destroy(subproc->heap_main);
  }

  // merge stats back into the main subproc?
  if (subproc!=&subproc_main) {
    _mi_arenas_unsafe_destroy_all(subproc);
    _mi_stats_merge_into(&subproc_main.stats, &subproc->stats);

    // safe to release
    // todo: should we refcount subprocesses?
    mi_lock_done(&subproc->arena_reserve_lock);
    mi_lock_done(&subproc->heaps_lock);
    _mi_meta_free(subproc, sizeof(mi_subproc_t), subproc->memid);
  }
}

void mi_subproc_destroy(mi_subproc_id_t subproc_id) {
  if (subproc_id == NULL) return;
  mi_subproc_unsafe_destroy(_mi_subproc_from_id(subproc_id));
}

static void mi_subprocs_unsafe_destroy_all(void) {
  mi_lock(&subprocs_lock) {
    mi_subproc_t* subproc = subprocs;
    while (subproc!=NULL) {
      mi_subproc_t* next = subproc->next;
      if (subproc!=&subproc_main) {
        mi_subproc_unsafe_destroy(subproc);
      }
      subproc = next;
    }
  }
  mi_subproc_unsafe_destroy(&subproc_main);
}


void mi_subproc_add_current_thread(mi_subproc_id_t subproc_id) {
  mi_subproc_t* subproc = _mi_subproc_from_id(subproc_id);
  mi_tld_t* const tld = _mi_theap_default_safe()->tld;
  mi_assert(tld->subproc== &subproc_main);
  if (tld->subproc != &subproc_main) {
    _mi_warning_message("unable to add thread to the subprocess as it was already in another subprocess (id: %p)\n", subproc);
    return;
  }
  tld->subproc = subproc;
  tld->thread_seq = mi_atomic_increment_relaxed(&subproc->thread_total_count);
  mi_atomic_decrement_relaxed(&subproc_main.thread_count);
  mi_atomic_increment_relaxed(&subproc->thread_count);
}


bool mi_subproc_visit_heaps(mi_subproc_id_t subproc_id, mi_heap_visit_fun* visitor, void* arg) {
  mi_subproc_t* subproc = _mi_subproc_from_id(subproc_id);
  if (subproc==NULL) return false;
  bool ok = true;
  mi_lock(&subproc->heaps_lock) {
    for (mi_heap_t* heap = subproc->heaps; heap!=NULL && ok; heap = heap->next) {
      ok = (*visitor)(heap, arg);
    }
  }
  return ok;
}


/* -----------------------------------------------------------
  Allocate theap data
----------------------------------------------------------- */

// Initialize the thread local default theap, called from `mi_thread_init`
static mi_theap_t* _mi_thread_init_theap_default(void) {
  mi_theap_t* theap = _mi_theap_default();
  if (mi_theap_is_initialized(theap)) return theap;
  if (_mi_is_main_thread()) {
    mi_heap_main_init();
    theap = &theap_main;
  }
  else {
    // allocates tld data
    // note: we cannot access thread-locals yet as that can cause (recursive) allocation
    // (on macOS <= 14 for example where the loader allocates thread-local data on demand).
    mi_tld_t* tld = mi_tld_alloc();
    // allocate and initialize the theap for the main heap
    theap = _mi_theap_create(mi_heap_main(), tld);
  }
  // associate the theap with this thread
  // (this is safe, on macOS for example, the theap is set in a dedicated TLS slot and thus does not cause recursive allocation)
  _mi_theap_default_set(theap);
  return theap;
}


// Free the thread local theaps
static void mi_thread_theaps_done(mi_tld_t* tld)
{
  // reset the thread local theaps
  __mi_theap_main = NULL;
  _mi_theap_default_set((mi_theap_t*)&_mi_theap_empty);
  _mi_theap_cached_set((mi_theap_t*)&_mi_theap_empty);

  // delete all theaps in this thread
  mi_theap_t* curr = tld->theaps;
  while (curr != NULL) {
    mi_theap_t* next = curr->tnext; // save `tnext` as `curr` will be freed
    // never destroy theaps; if a dll is linked statically with mimalloc,
    // there may still be delete/free calls after the mi_fls_done is called. Issue #207
    _mi_theap_delete(curr);
    curr = next;
  }
  mi_assert(_mi_theap_default()==(mi_theap_t*)&_mi_theap_empty); // careful to not re-initialize the default theap during theap_delete
  mi_assert(!mi_theap_is_initialized(_mi_theap_default()));
}



// --------------------------------------------------------
// Try to run `mi_thread_done()` automatically so any memory
// owned by the thread but not yet released can be abandoned
// and re-owned by another thread.
//
// 1. windows dynamic library:
//     call from DllMain on DLL_THREAD_DETACH
// 2. windows static library:
//     use special linker section to call a destructor when the thread is done
// 3. unix, pthreads:
//     use a pthread key to call a destructor when a pthread is done
//
// In the last two cases we also need to call `mi_process_init`
// to set up the thread local keys.
// --------------------------------------------------------

// Set up handlers so `mi_thread_done` is called automatically
static void mi_process_setup_auto_thread_done(void) {
  static bool tls_initialized = false; // fine if it races
  if (tls_initialized) return;
  tls_initialized = true;
  _mi_prim_thread_init_auto_done();
  _mi_theap_default_set(&theap_main);
}


bool _mi_is_main_thread(void) {
  return (tld_main.thread_id==0 || tld_main.thread_id == _mi_thread_id());
}


// Initialize thread
void mi_thread_init(void) mi_attr_noexcept
{
  // ensure our process has started already
  mi_process_init();
  // if the theap_default is already set we have already initialized
  if (_mi_thread_is_initialized()) return;

  // initialize the default theap
  _mi_thread_init_theap_default();

  mi_heap_stat_increase(mi_heap_main(), threads, 1);
  // _mi_verbose_message("thread init: 0x%zx\n", _mi_thread_id());
}

void mi_thread_done(void) mi_attr_noexcept {
  _mi_thread_done(NULL);
}

void _mi_thread_done(mi_theap_t* _theap_main)
{
  // NULL can be passed on some platforms
  if (_theap_main==NULL) {
    _theap_main = __mi_theap_main;
  }

  // prevent re-entrancy through theap_done/theap_set_default_direct (issue #699)
  if (!mi_theap_is_initialized(_theap_main)) {
    return;
  }

  // release dynamic thread_local's
  _mi_thread_locals_thread_done();

  // note: we store the tld as we should avoid reading `thread_tld` at this point (to avoid reinitializing the thread local storage)
  mi_tld_t* const tld = _theap_main->tld;

  // adjust stats
  mi_heap_stat_decrease(_mi_subproc_heap_main(tld->subproc), threads, 1);  // todo: or `_theap_main->heap`?

  // check thread-id as on Windows shutdown with FLS the main (exit) thread may call this on thread-local theaps...
  if (tld->thread_id != _mi_prim_thread_id()) return;

  // delete the thread local theaps
  mi_thread_theaps_done(tld);

  // free thread local data
  mi_tld_free(tld);
}


mi_decl_cold mi_decl_noinline mi_theap_t* _mi_theap_empty_get(void) {
  return (mi_theap_t*)&_mi_theap_empty;
}

#if MI_TLS_MODEL_DYNAMIC_WIN32

// If we can, we use one of the 64 direct TLS slots (but fall back to expansion slots if needed)
// See <https://en.wikipedia.org/wiki/Win32_Thread_Information_Block> for the offsets.
#if MI_SIZE_SIZE==4
#define MI_TLS_DIRECT_FIRST             (0x0E10 / MI_SIZE_SIZE)
#else
#define MI_TLS_DIRECT_FIRST             (0x1480 / MI_SIZE_SIZE)
#endif
#define MI_TLS_DIRECT_SLOTS             (64)
#define MI_TLS_EXPANSION_SLOTS          (1024)

#if !MI_WIN_DIRECT_TLS
#define MI_TLS_INITIAL_SLOT             MI_TLS_EXPANSION_SLOT
#define MI_TLS_INITIAL_EXPANSION_SLOT   (MI_TLS_EXPANSION_SLOTS-1)
#else
// with only direct entries, use the "arbitrary user data" field 
// and assume it is NULL (see also <http://www.nynaeve.net/?p=98>)
#define MI_TLS_INITIAL_EXPANSION_SLOT   (0)
#define MI_TLS_INITIAL_SLOT             (5)
#endif

// we initially use the last of the expansion slots as the default NULL.
// note: this will fail if the program allocates exactly 1024+64 slots with TlsAlloc (which is quite unlikely)
mi_decl_hidden mi_decl_cache_align size_t _mi_theap_default_slot = MI_TLS_INITIAL_SLOT;
mi_decl_hidden size_t _mi_theap_default_expansion_slot = MI_TLS_INITIAL_EXPANSION_SLOT;
mi_decl_hidden size_t _mi_theap_cached_slot            = MI_TLS_INITIAL_SLOT;
mi_decl_hidden size_t _mi_theap_cached_expansion_slot  = MI_TLS_INITIAL_EXPANSION_SLOT;

static size_t mi_win_tls_slot_alloc(size_t* extended) {
  const DWORD slot = TlsAlloc();
  if (slot==TLS_OUT_OF_INDEXES || slot >= MI_TLS_DIRECT_SLOTS + MI_TLS_EXPANSION_SLOTS - 1) {
    // note: we also fail if the program already allocated the maximum number of expansion slots (as we use the last one as the default)
    *extended = 0;
    return 0;
  }
  else if (slot<MI_TLS_DIRECT_SLOTS) {
    *extended = 0;
    return (slot + MI_TLS_DIRECT_FIRST);
  }
  else {
    #if MI_WIN_DIRECT_TLS
    *extended = 0;
    return 0;
    #else
    *extended = (slot - MI_TLS_DIRECT_SLOTS);
    return MI_TLS_EXPANSION_SLOT;
    #endif
  }
}

mi_decl_cold mi_theap_t* _mi_win_tls_slots_init(void) {
  static mi_atomic_once_t tls_slots_init;
  if (mi_atomic_once(&tls_slots_init)) {
    _mi_theap_default_slot = mi_win_tls_slot_alloc(&_mi_theap_default_expansion_slot);
    _mi_theap_cached_slot = mi_win_tls_slot_alloc(&_mi_theap_cached_expansion_slot);
    if (_mi_theap_cached_slot==0) {
      _mi_error_message(EFAULT, "unable to allocate fast TLS user slot (0x%zx)\n", _mi_theap_cached_slot);
    }
  }
  return (mi_theap_t*)&_mi_theap_empty;
}

static void mi_win_tls_slot_set(size_t slot, size_t extended_slot, void* value) {
  mi_assert_internal((slot >= MI_TLS_DIRECT_FIRST && slot < MI_TLS_DIRECT_FIRST + MI_TLS_DIRECT_SLOTS) || slot == MI_TLS_EXPANSION_SLOT);
  if (slot < MI_TLS_DIRECT_FIRST + MI_TLS_DIRECT_SLOTS) {
    mi_prim_tls_slot_set(slot, value);
  }
  else {
    mi_assert_internal(extended_slot < MI_TLS_EXPANSION_SLOTS);
    TlsSetValue((DWORD)(extended_slot + MI_TLS_DIRECT_SLOTS), value);  // use TlsSetValue to initialize the TlsExpansion array if needed
  }
}

#elif MI_TLS_MODEL_DYNAMIC_PTHREADS

// only for pthreads for now
mi_decl_hidden pthread_key_t _mi_theap_default_key = 0;
mi_decl_hidden pthread_key_t _mi_theap_cached_key = 0;

mi_decl_cold mi_theap_t* _mi_tls_keys_init(void) {
  static mi_atomic_once_t tls_keys_init;
  if (mi_atomic_once(&tls_keys_init)) {
    pthread_key_create(&_mi_theap_default_key, NULL);
    pthread_key_create(&_mi_theap_cached_key, NULL);
  }
  return (mi_theap_t*)&_mi_theap_empty;
}

#endif

void _mi_theap_cached_set(mi_theap_t* theap) {
  #if MI_TLS_MODEL_THREAD_LOCAL
    __mi_theap_cached = theap;
  #elif MI_TLS_MODEL_FIXED_SLOT
    mi_prim_tls_slot_set(MI_TLS_MODEL_FIXED_SLOT_CACHED, theap);
  #elif MI_TLS_MODEL_DYNAMIC_WIN32
    _mi_win_tls_slots_init();
    mi_win_tls_slot_set(_mi_theap_cached_slot, _mi_theap_cached_expansion_slot, theap);
  #elif MI_TLS_MODEL_DYNAMIC_PTHREADS
    _mi_tls_keys_init();
    if (_mi_theap_cached_key!=0) pthread_setspecific(_mi_theap_cached_key, theap);
  #endif
}

void _mi_theap_default_set(mi_theap_t* theap)  {
  mi_assert_internal(theap != NULL);
  mi_assert_internal(theap->tld->thread_id==0 || theap->tld->thread_id==_mi_thread_id());
  #if MI_TLS_MODEL_THREAD_LOCAL
    __mi_theap_default = theap;
  #elif MI_TLS_MODEL_FIXED_SLOT
    mi_prim_tls_slot_set(MI_TLS_MODEL_FIXED_SLOT_DEFAULT, theap);
  #elif MI_TLS_MODEL_DYNAMIC_WIN32
    _mi_win_tls_slots_init();
    mi_win_tls_slot_set(_mi_theap_default_slot, _mi_theap_default_expansion_slot, theap);
  #elif MI_TLS_MODEL_DYNAMIC_PTHREADS
    _mi_tls_keys_init();
    if (_mi_theap_default_key!=0) pthread_setspecific(_mi_theap_default_key, theap);
  #endif

  // set theap main if needed
  if (mi_theap_is_initialized(theap)) {
    // ensure the default theap is passed to `_mi_thread_done` as on some platforms we cannot access TLS at thread termination (as it would allocate again)
    _mi_prim_thread_associate_default_theap(theap);
    if (_mi_is_heap_main(theap->heap)) {
      __mi_theap_main = theap;
    }
  }
}

void mi_thread_set_in_threadpool(void) mi_attr_noexcept {
  mi_theap_t* theap = _mi_theap_default_safe();
  theap->tld->is_in_threadpool = true;
}

// --------------------------------------------------------
// Run functions on process init/done, and thread init/done
// --------------------------------------------------------
static bool os_preloading = true;    // true until this module is initialized

// Returns true if this module has not been initialized; Don't use C runtime routines until it returns false.
bool mi_decl_noinline _mi_preloading(void) {
  return os_preloading;
}

// Returns true if mimalloc was redirected
mi_decl_nodiscard bool mi_is_redirected(void) mi_attr_noexcept {
  return _mi_is_redirected();
}

// Called once by the process loader from `src/prim/prim.c`
void _mi_auto_process_init(void) {
  // mi_heap_main_init();
  // #if defined(__APPLE__) || defined(MI_TLS_RECURSE_GUARD)
  // volatile mi_theap_t* dummy = __mi_theap_default; // access TLS to allocate it before setting tls_initialized to true;
  // if (dummy == NULL) return;                       // use dummy or otherwise the access may get optimized away (issue #697)
  // #endif

  os_preloading = false;
  mi_assert_internal(_mi_is_main_thread());

  mi_process_init();
  mi_process_setup_auto_thread_done();
  _mi_thread_locals_init();
  _mi_options_post_init();  // now we can print to stderr
  if (_mi_is_redirected()) _mi_verbose_message("malloc is redirected.\n");

  // show message from the redirector (if present)
  const char* msg = NULL;
  _mi_allocator_init(&msg);
  if (msg != NULL && (mi_option_is_enabled(mi_option_verbose) || mi_option_is_enabled(mi_option_show_errors))) {
    _mi_fputs(NULL,NULL,NULL,msg);
  }

  // reseed random
  _mi_random_reinit_if_weak(&theap_main.random);
}

// CPU features
mi_decl_cache_align size_t _mi_cpu_movsb_max = 0;  // for size <= max, rep movsb is fast
mi_decl_cache_align size_t _mi_cpu_stosb_max = 0;  // for size <= max, rep stosb is fast
mi_decl_cache_align bool _mi_cpu_has_popcnt = false;

#if (MI_ARCH_X64 || MI_ARCH_X86)
#if defined(__GNUC__)
// #include <cpuid.h>
static bool mi_cpuid(uint32_t* regs4, uint32_t level, uint32_t sublevel) {
  // note: use explicit assembly instead of __get_cpuid as we need the sublevel (in ecx)
  // (on Ubuntu 22 with WSL the __get_cpuid does not clear ecx for level 7 which is incorrect).
  uint32_t eax, ebx, ecx, edx;
  __asm __volatile("cpuid" : "=a"(eax), "=b"(ebx), "=c"(ecx), "=d"(edx) : "a"(level), "c"(sublevel) : );
  regs4[0] = eax;
  regs4[1] = ebx;
  regs4[2] = ecx;
  regs4[3] = edx;
  return true;
}

#elif defined(_MSC_VER)
static bool mi_cpuid(uint32_t* regs4, uint32_t level, uint32_t sublevel) {
  __cpuidex((int32_t*)regs4, (int32_t)level, (int32_t)sublevel);
  return true;
}
#else
static bool mi_cpuid(uint32_t* regs4, uint32_t level, uint32_t sublevel) {
  MI_UNUSED(regs4); MI_UNUSED(level); MI_UNUSED(sublevel);
  return false;
}
#endif

static void mi_detect_cpu_features(void) {
  // FSRM for fast short rep movsb support (AMD Zen3+ (~2020) or Intel Ice Lake+ (~2017))
  // EMRS for fast enhanced rep movsb/stosb support (not used at the moment, memcpy always seems faster?)
  // FSRS for fast short rep stosb
  bool amd = false;
  bool fsrm = false;
  // bool erms = false;
  bool fsrs = false;
  uint32_t cpu_info[4];
  if (mi_cpuid(cpu_info, 0, 0)) {
    amd = (cpu_info[2]==0x444d4163); // (Auth enti cAMD)
  }
  if (mi_cpuid(cpu_info, 7, 0)) {
    fsrm = ((cpu_info[3] & (1 << 4)) != 0); // bit 4 of EDX : see <https://en.wikipedia.org/wiki/CPUID#EAX=7,_ECX=0:_Extended_Features>
    // erms = ((cpu_info[1] & (1 << 9)) != 0); // bit 9 of EBX : see <https://en.wikipedia.org/wiki/CPUID#EAX=7,_ECX=0:_Extended_Features>
  }
  if (mi_cpuid(cpu_info, 7, 1)) {
    fsrs = ((cpu_info[1] & (1 << 11)) != 0); // bit 11 of EBX: see <https://en.wikipedia.org/wiki/CPUID#EAX=7,_ECX=1:_Extended_Features>
  }
  if (mi_cpuid(cpu_info, 1, 0)) {
    _mi_cpu_has_popcnt = ((cpu_info[2] & (1 << 23)) != 0); // bit 23 of ECX : see <https://en.wikipedia.org/wiki/CPUID#EAX=1:_Processor_Info_and_Feature_Bits>
  }

  if (fsrm) {
    _mi_cpu_movsb_max = 127;
  }
  if (fsrs || (amd && fsrm)) {  // fsrm on amd implies fsrs, see: https://marc.info/?l=git-commits-head&m=168186277717803
    _mi_cpu_stosb_max = 127;
  }
}

#else
static void mi_detect_cpu_features(void) {
  #if MI_ARCH_ARM64
  _mi_cpu_has_popcnt = true;
  #endif
}
#endif


// Initialize the process; called by thread_init or the process loader
void mi_process_init(void) mi_attr_noexcept {
  // ensure we are called once
  static mi_atomic_once_t process_init;
	// #if _MSC_VER < 1920
	// mi_heap_main_init(); // vs2017 can dynamically re-initialize theap_main
	// #endif
  if (!mi_atomic_once(&process_init)) return;
  _mi_verbose_message("process init: 0x%zx\n", _mi_thread_id());

  mi_detect_cpu_features();
  _mi_options_init();
  _mi_stats_init();
  _mi_os_init();
  // the following can potentially allocate (on freeBSD for pthread keys)
  // todo: do 2-phase so we can use stats at first, then later init the keys?
  mi_heap_main_init(); // before page_map_init so stats are working
  _mi_page_map_init(); // todo: this could fail.. should we abort in that case?
  mi_thread_init();
  _mi_process_is_initialized = true;

  #if defined(_WIN32) && defined(MI_WIN_USE_FLS)
  // On windows, when building as a static lib the FLS cleanup happens to early for the main thread.
  // To avoid this, set the FLS value for the main thread to NULL so the fls cleanup
  // will not call _mi_thread_done on the (still executing) main thread. See issue #508.
  _mi_prim_thread_associate_default_theap(NULL);
  #endif

  // mi_stats_reset();  // only call stat reset *after* thread init (or the theap tld == NULL)
  mi_track_init();
  if (mi_option_is_enabled(mi_option_reserve_huge_os_pages)) {
    size_t pages = mi_option_get_clamp(mi_option_reserve_huge_os_pages, 0, 128*1024);
    int reserve_at  = (int)mi_option_get_clamp(mi_option_reserve_huge_os_pages_at, -1, INT_MAX);
    if (reserve_at != -1) {
      mi_reserve_huge_os_pages_at(pages, reserve_at, pages*500);
    } else {
      mi_reserve_huge_os_pages_interleave(pages, 0, pages*500);
    }
  }
  if (mi_option_is_enabled(mi_option_reserve_os_memory)) {
    long ksize = mi_option_get(mi_option_reserve_os_memory);
    if (ksize > 0) {
      mi_reserve_os_memory((size_t)ksize*MI_KiB, true, true);
    }
  }
}

// Called when the process is done (cdecl as it is used with `at_exit` on some platforms)
void mi_cdecl mi_process_done(void) mi_attr_noexcept {
  // only shutdown if we were initialized
  if (!_mi_process_is_initialized) return;
  // ensure we are called once
  static bool process_done = false;
  if (process_done) return;
  process_done = true;

  // free dynamic thread locals (if used at all)
  _mi_thread_locals_done();

  // release any thread specific resources and ensure _mi_thread_done is called on all but the main thread
  _mi_prim_thread_done_auto_done();

  #ifndef MI_SKIP_COLLECT_ON_EXIT
    #if (MI_DEBUG || !defined(MI_SHARED_LIB))
    // free all memory if possible on process exit. This is not needed for a stand-alone process
    // but should be done if mimalloc is statically linked into another shared library which
    // is repeatedly loaded/unloaded, see issue #281.
    mi_theap_collect(_mi_theap_default(), true /* force */);
    #endif
  #endif

  // Forcefully release all retained memory; this can be dangerous in general if overriding regular malloc/free
  // since after process_done there might still be other code running that calls `free` (like at_exit routines,
  // or C-runtime termination code.
  if (mi_option_is_enabled(mi_option_destroy_on_exit)) {
    mi_subprocs_unsafe_destroy_all();
    _mi_page_map_unsafe_destroy(_mi_subproc_main());
  }
  else {
    mi_heap_stats_merge_to_subproc(mi_heap_main());
  }

  if (mi_option_is_enabled(mi_option_show_stats) || mi_option_is_enabled(mi_option_verbose)) {
    mi_subproc_stats_print_out(NULL, NULL, NULL);
  }
  mi_lock_done(&subprocs_lock);
  _mi_allocator_done();
  _mi_verbose_message("process done: 0x%zx\n", tld_main.thread_id);
  os_preloading = true; // don't call the C runtime anymore
}

void mi_cdecl _mi_auto_process_done(void) mi_attr_noexcept {
  if (_mi_option_get_fast(mi_option_destroy_on_exit)>1) return;
  mi_process_done();
}
