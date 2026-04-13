/*----------------------------------------------------------------------------
Copyright (c) 2018-2025, Microsoft Research, Daan Leijen
This is free software; you can redistribute it and/or modify it under the
terms of the MIT license. A copy of the license can be found in the file
"LICENSE" at the root of this distribution.
-----------------------------------------------------------------------------*/

#include "mimalloc.h"
#include "mimalloc/internal.h"
#include "mimalloc/prim.h"  // _mi_theap_default


/* -----------------------------------------------------------
  Heap's
----------------------------------------------------------- */

mi_theap_t* mi_heap_theap(mi_heap_t* heap) {
  return _mi_heap_theap(heap);
}

void mi_heap_set_numa_affinity(mi_heap_t* heap, int numa_node) {
  if (heap==NULL) { heap = mi_heap_main(); }
  heap->numa_node = (numa_node < 0 ? -1 : numa_node % _mi_os_numa_node_count());
}

void mi_heap_stats_merge_to_subproc(mi_heap_t* heap) {
  if (heap==NULL) { heap = mi_heap_main(); }
  _mi_stats_merge_into(&heap->subproc->stats, &heap->stats);
}

void mi_heap_stats_merge_to_main(mi_heap_t* heap) {
  if (heap==NULL) return;
  _mi_stats_merge_into(&mi_heap_main()->stats, &heap->stats);
}

static mi_theap_t* mi_heap_init_theap(const mi_heap_t* const_heap)
{
  mi_heap_t* heap = (mi_heap_t*)const_heap;
  mi_assert_internal(heap!=NULL);

  if (_mi_is_heap_main(heap)) {
    // this can be called if the (main) thread is not yet initialized (as no allocation happened)
    mi_thread_init();
    mi_theap_t* theap = _mi_heap_theap(heap);
    mi_assert_internal(theap!=NULL);
    return theap;
  }

  // otherwise initialize the theap for this heap
  // get the thread local
  mi_theap_t* theap = NULL;
  if (heap->theap==0) {
    // initialize thread locals
    heap->theap = _mi_thread_local_create();
    if (heap->theap==0) {
      _mi_error_message(EFAULT, "unable to dynamically create a thread local for a heap\n");
      return NULL;
    }
  }
  else {
    // get current thread local
    theap = (mi_theap_t*)_mi_thread_local_get(heap->theap);
  }

  // create a fresh theap?
  if (theap==NULL) {
    theap = _mi_theap_create(heap, _mi_theap_default_safe()->tld);
    if (theap==NULL) {
      _mi_error_message(EFAULT, "unable to allocate memory for a thread local heap\n");
      return NULL;
    }
    if (!_mi_thread_local_set(heap->theap, theap)) {
      _mi_error_message(EFAULT, "unable to allocate memory for a thread local storage\n");
      return NULL;
    }
  }
  return theap;
}


// get the theap for a heap without initializing (and return NULL in that case)
mi_theap_t* _mi_heap_theap_get_peek(const mi_heap_t* heap) {
  if (heap==NULL || _mi_is_heap_main(heap)) {
    return __mi_theap_main;  // don't call _mi_theap_main as it may still be NULL
  }
  else {
    return (mi_theap_t*)_mi_thread_local_get(heap->theap);
  }
}

// get (and possibly create) the theap belonging to a heap
mi_theap_t* _mi_heap_theap_get_or_init(const mi_heap_t* heap)
{
  mi_theap_t* theap = _mi_heap_theap_peek(heap);
  if mi_unlikely(theap==NULL) {
    theap = mi_heap_init_theap(heap);
    if (theap==NULL) { return (mi_theap_t*)&_mi_theap_empty_wrong; }  // this will return NULL from page.c:_mi_malloc_generic
  }
  _mi_theap_cached_set(theap);
  return theap;
}


mi_heap_t* mi_heap_new_in_arena(mi_arena_id_t exclusive_arena_id) {
  // always allocate heap data in the (subprocess) main heap
  mi_heap_t* heap_main = mi_heap_main();
  // todo: allocate heap data in the exclusive arena ?
  mi_heap_t* heap = (mi_heap_t*)mi_heap_zalloc( heap_main, sizeof(mi_heap_t) );
  if (heap==NULL) return NULL;

  // init fields
  heap->subproc = heap_main->subproc;
  heap->heap_seq = mi_atomic_increment_relaxed(&heap_main->subproc->heap_total_count);
  heap->exclusive_arena = _mi_arena_from_id(exclusive_arena_id);
  heap->numa_node = -1; // no initial affinity

  mi_lock_init(&heap->theaps_lock);
  mi_lock_init(&heap->os_abandoned_pages_lock);
  mi_lock_init(&heap->arena_pages_lock);

  // push onto the subproc heaps
  mi_lock(&heap->subproc->heaps_lock) {
    mi_heap_t* head = heap->subproc->heaps;
    heap->prev = NULL;
    heap->next = head;
    if (head!=NULL) { head->prev = heap;  }
    heap->subproc->heaps = heap;
  }
  mi_atomic_increment_relaxed(&heap_main->subproc->heap_count);
  mi_subproc_stat_increase(heap_main->subproc, heaps, 1);
  return heap;
}

mi_heap_t* mi_heap_new(void) {
  return mi_heap_new_in_arena(0);
}

// free the heap resources (assuming the pages are already moved/destroyed)
static void mi_heap_free(mi_heap_t* heap) {
  mi_assert_internal(heap!=NULL && !_mi_is_heap_main(heap));

  // free all theaps belonging to this heap
  mi_theap_t* theap = NULL;
  mi_lock(&heap->theaps_lock) { theap = heap->theaps; }
  while(theap != NULL) {
    mi_theap_t* next = NULL;
    mi_lock(&heap->theaps_lock) { next = theap->hnext; }
    _mi_theap_free(theap);
    theap = next;
  }
  mi_lock(&heap->theaps_lock) { theap = heap->theaps; }
  mi_assert_internal(theap==NULL);

  // free all arena pages infos
  mi_lock(&heap->arena_pages_lock) {
    for (size_t i = 0; i < MI_MAX_ARENAS; i++) {
      mi_arena_pages_t* arena_pages = mi_atomic_load_ptr_relaxed(mi_arena_pages_t, &heap->arena_pages[i]);
      if (arena_pages!=NULL) {
        mi_atomic_store_ptr_relaxed(mi_arena_pages_t, &heap->arena_pages[i], NULL);
        mi_free(arena_pages);
      }
    }
  }

  // remove the heap from the subproc
  mi_heap_stats_merge_to_main(heap);
  mi_atomic_decrement_relaxed(&heap->subproc->heap_count);
  mi_subproc_stat_decrease(heap->subproc, heaps, 1);
  mi_lock(&heap->subproc->heaps_lock) {
    if (heap->next!=NULL) { heap->next->prev = heap->prev; }
    if (heap->prev!=NULL) { heap->prev->next = heap->next; }
                     else { heap->subproc->heaps = heap->next; }
  }

  _mi_thread_local_free(heap->theap);
  mi_lock_done(&heap->theaps_lock);
  mi_lock_done(&heap->os_abandoned_pages_lock);
  mi_free(heap);
}

void mi_heap_delete(mi_heap_t* heap) {
  if (heap==NULL) return;
  if (_mi_is_heap_main(heap)) {
    _mi_warning_message("cannot delete the main heap\n");
    return;
  }
  _mi_heap_move_pages(heap, mi_heap_main());
  mi_heap_free(heap);
}

void mi_heap_destroy(mi_heap_t* heap) {
  if (heap==NULL) return;
  if (_mi_is_heap_main(heap)) {
    _mi_warning_message("cannot destroy the main heap\n");
    return;
  }
  _mi_heap_destroy_pages(heap);
  mi_heap_free(heap);
}

mi_heap_t* mi_heap_of(const void* p) {
  mi_page_t* page = _mi_safe_ptr_page(p);
  if (page==NULL) return NULL;
  return mi_page_heap(page);
}

bool mi_any_heap_contains(const void* p) {
  return (mi_heap_of(p)!=NULL);
}

bool mi_heap_contains(const mi_heap_t* heap, const void* p) {
  if (heap==NULL) { heap = mi_heap_main(); }
  return (heap==mi_heap_of(p));
}

// deprecated
bool mi_check_owned(const void* p) {
  return mi_any_heap_contains(p);
}
