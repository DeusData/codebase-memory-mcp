/*----------------------------------------------------------------------------
Copyright (c) 2018-2026, Microsoft Research, Daan Leijen
This is free software; you can redistribute it and/or modify it under the
terms of the MIT license. A copy of the license can be found in the file
"LICENSE" at the root of this distribution.
-----------------------------------------------------------------------------*/

#include "mimalloc.h"
#include "mimalloc/internal.h"
#include "mimalloc/prim.h"      // _mi_prim_thread_yield
#include "mimalloc/prim-tls.h"  // _mi_heap_theap


/* -----------------------------------------------------------
  Heap's
----------------------------------------------------------- */

mi_theap_t* mi_heap_theap(mi_heap_t* heap) {
  return _mi_heap_theap(heap);  // in prim.h
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
  _mi_stats_merge_into(&mi_heap_get_heap_main(heap)->stats, &heap->stats);
}

bool _mi_heap_theap_set(mi_heap_t* heap, mi_theap_t* theap) {
  mi_assert_internal((uintptr_t)theap == 1 || _mi_theap_heap(theap)==heap);
  mi_assert_internal(!_mi_is_empty_theap(theap));
  mi_assert_internal(heap->theap != 0);
  return _mi_thread_local_set(heap->theap,theap);
}

// mi_theap_t* _mi_heap_theap_get_peek(const mi_heap_t* heap) {
//   mi_theap_t* theap;
//   mi_assert_internal(heap->theap != 0);
//   if mi_likely(heap->theap!=0) {  // paranoia
//     theap = (mi_theap_t*)_mi_thread_local_get(heap->theap);
//   }
//   else {
//     _mi_error_message(EFAULT, "no thread-local reserved for heap (%p)\n", heap);
//     return NULL;
//   }
//   mi_assert_internal(!_mi_is_empty_theap(theap));
//   mi_assert_internal(theap->heap == heap);  // this goes wrong if using main heaps across subprocesses (as all share the same key)
//   return theap;
// }


static mi_decl_noinline mi_theap_t* mi_heap_init_theap(const mi_heap_t* const_heap)
{
  mi_heap_t* heap = (mi_heap_t*)const_heap;
  mi_assert_internal(heap!=NULL);

  // if (_mi_is_process_heap_main(heap)) {
  //   // this can be called if the (main) thread is not yet initialized (as no allocation happened)
  //   // but `theap_main_init_get()` will call `mi_thread_init()`
  //   mi_theap_t* const theap = _mi_theap_main_safe();
  //   mi_assert_internal(theap!=NULL && _mi_is_heap_main(_mi_theap_heap(theap)));
  //   return theap;
  // }

  // initialize thread first in case this is the main heap
  // (which may allocate the default theap already for the main heap)
  if (!_mi_thread_is_initialized()) {
    mi_thread_init();
  }

  // get the thread local theap
  mi_theap_t* theap = (mi_theap_t*)_mi_thread_local_get(heap->theap);

  // create a fresh theap?
  if (theap==NULL) {
    // allocate a fresh theap
    theap = _mi_theap_create(heap, mi_theap_get_default()->tld); // sets the theap thread local
    if (theap==NULL) {
      _mi_error_message(EFAULT, "unable to allocate memory for a thread local heap\n");
      return NULL;
    }
    _mi_heap_theap_set(heap, theap);
    mi_assert_internal(theap == (mi_theap_t*)_mi_thread_local_get(heap->theap));
  }
  return theap;
}


// get (and possibly create) the theap belonging to a heap
mi_theap_t* _mi_heap_theap_get_or_init(const mi_heap_t* heap)
{
  mi_assert_internal(heap->theap != 0);
  mi_theap_t* theap = (mi_theap_t*)_mi_thread_local_get(heap->theap);
  if mi_unlikely(theap==NULL) {
    theap = mi_heap_init_theap(heap);
    if (theap==NULL) { return (mi_theap_t*)&_mi_theap_empty_wrong; }  // this will return NULL from page.c:_mi_malloc_generic
  }
  _mi_theap_cached_set(theap);
  return theap;
}

static void mi_heap_initialize(mi_heap_t* heap, mi_thread_local_t theap_slot, mi_subproc_t* subproc, mi_arena_id_t exclusive_arena_id)
{
  // init fields
  heap->theap = theap_slot;
  heap->subproc = subproc;
  heap->heap_seq = mi_atomic_increment_relaxed(&subproc->heap_total_count);
  heap->exclusive_arena = _mi_arena_from_id(exclusive_arena_id);
  heap->numa_node = -1; // no initial affinity
  mi_stats_header_init(&heap->stats);
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
  mi_atomic_increment_relaxed(&subproc->heap_count);
  mi_subproc_stat_increase(subproc, heaps, 1);
  mi_assert_internal(_mi_is_heap_main(heap) ? heap->theap == mi_thread_local_key_fast : heap->theap != 0);
}

mi_heap_t* _mi_heap_new_for_subproc(mi_subproc_t* subproc, mi_arena_id_t exclusive_arena_id, bool is_main_heap) {
  mi_assert_internal(is_main_heap ? (subproc->heap_main == NULL && subproc->parent != NULL) : subproc->heap_main != NULL);
  // heap data is allocated in the current subproc
  mi_heap_t* const heap_main = (is_main_heap ? subproc->parent->heap_main : subproc->heap_main);
  // todo: allocate heap data in the exclusive arena ?
  mi_heap_t* const heap = (mi_heap_t*)mi_heap_zalloc( heap_main, sizeof(mi_heap_t) );
  if (heap==NULL) return NULL;
  // reserve a thread local slot for this heap (see also issue #1230)
  mi_thread_local_t theap_slot = (is_main_heap ? mi_thread_local_key_fast : _mi_thread_local_create());
  if (theap_slot == 0) {
    _mi_error_message(EFAULT, "unable to dynamically create a thread local for a heap\n");
    mi_free(heap);
    return NULL;
  }
  if (is_main_heap) {
    mi_assert_internal(subproc->heap_main == NULL);
    subproc->heap_main = heap;
  }
  mi_heap_initialize(heap, theap_slot, subproc, exclusive_arena_id);
  return heap;
}

mi_heap_t* mi_heap_new_in_arena(mi_arena_id_t exclusive_arena_id) {
  return _mi_heap_new_for_subproc(_mi_subproc(), exclusive_arena_id, false);
}

mi_heap_t* mi_heap_new(void) {
  return mi_heap_new_in_arena(0);
}

// free all theaps belonging to this heap (without deleting their pages as we do this arena wise for efficiency)
static void mi_heap_free_theaps(mi_heap_t* heap) {
  // This can run concurrently with a thread that terminates (see `init.c:mi_thread_theaps_done`),
  // and we need to ensure we free theaps atomically.
  // We do this in a loop where we release the theaps_lock at every potential re-iteration to unblock
  // potential concurrent thread termination which tries to remove the theap from our theaps list.
  bool all_freed;
  do {
    all_freed = true;
    mi_theap_t* theap = NULL;
    mi_lock(&heap->theaps_lock) {
      theap = heap->theaps;
      while(theap != NULL) {
        mi_theap_t* next = theap->hnext;
        if (!_mi_theap_free(theap, false /* dont re-acquire the heap->theaps_lock */, true /* acquire the tld->theaps_lock though */ )) {
          all_freed = false;
        }
        theap = next;
      }
    }
    if (!all_freed) {
      mi_heap_stat_counter_increase(heap,heaps_delete_wait,1);
      _mi_prim_thread_yield();
    }
    else {
      mi_assert_internal(heap->theaps==NULL);
    }
  }
  while(!all_freed);
}

// free the heap resources (assuming the pages are already moved/destroyed, and all theaps have been freed)
static void mi_heap_free(mi_heap_t* heap, bool acquire_heaps_lock) {
  mi_assert_internal(heap!=NULL && !_mi_is_heap_main(heap));

  // free all arena pages infos
  mi_lock(&heap->arena_pages_lock) {
    for (size_t i = 0; i < MI_MAX_ARENAS; i++) {
      mi_arena_pages_t* arena_pages = mi_atomic_load_ptr_relaxed(mi_arena_pages_t, &heap->arena_pages[i]);
      if (arena_pages!=NULL) {
        mi_atomic_store_ptr_relaxed(mi_arena_pages_t, &heap->arena_pages[i], NULL);
        _mi_free_subproc_safe(arena_pages);
      }
    }
  }

  // remove the heap from the subproc
  mi_heap_stats_merge_to_main(heap);
  mi_atomic_decrement_relaxed(&heap->subproc->heap_count);
  mi_subproc_stat_decrease(heap->subproc, heaps, 1);
  mi_lock_maybe(&heap->subproc->heaps_lock, acquire_heaps_lock) {
    if (heap->next!=NULL) { heap->next->prev = heap->prev; }
    if (heap->prev!=NULL) { heap->prev->next = heap->next; }
                     else { heap->subproc->heaps = heap->next; }
  }

  _mi_thread_local_free(heap->theap);
  mi_lock_done(&heap->theaps_lock);
  mi_lock_done(&heap->os_abandoned_pages_lock);
  mi_lock_done(&heap->arena_pages_lock);
  _mi_free_subproc_safe(heap);
}

void mi_heap_delete(mi_heap_t* heap) {
  if (heap==NULL) return;
  mi_heap_t* heap_main = mi_heap_get_heap_main(heap);
  if (heap == heap_main) {
    _mi_warning_message("cannot delete the main heap\n");
    return;
  }
  mi_heap_free_theaps(heap);
  _mi_heap_move_pages(heap, heap_main);
  mi_heap_free(heap,true /* acquire subproc->heaps_lock */);
}

void _mi_heap_force_destroy(mi_heap_t* heap, bool acquire_heaps_lock) {
  if (heap==NULL) return;
  mi_heap_free_theaps(heap);
  _mi_heap_destroy_pages(heap);
  if (!_mi_is_heap_main(heap)) { mi_heap_free(heap, acquire_heaps_lock); }  // todo: release locks of the main heap?
}

void mi_heap_destroy(mi_heap_t* heap) {
  if (heap==NULL) return;
  if (_mi_is_heap_main(heap)) {
    _mi_warning_message("cannot destroy the main heap\n");
    return;
  }
  _mi_heap_force_destroy(heap,true /* acquire subproc->heaps_lock */);
}

mi_heap_t* mi_heap_of(const void* p) {
  mi_page_t* const page = _mi_safe_ptr_page(p);
  if (page==NULL) return NULL;
  return mi_page_heap(page);
}

bool mi_any_heap_contains(const void* p) {
  mi_page_t* const page = _mi_safe_ptr_page(p);
  return (page!=NULL);
}

bool mi_heap_contains(const mi_heap_t* heap, const void* p) {
  if (heap==NULL) { heap = mi_heap_main(); }
  return (heap==mi_heap_of(p));
}

// deprecated
bool mi_check_owned(const void* p) {
  return mi_any_heap_contains(p);
}

// unsafe heap utilization function for DragonFly (see issue #1258)
// If the page of pointer `p` belongs to `heap` (or `heap==NULL`) and has less than `perc_threshold` used blocks in its used area return `true`.
// This function is unsafe in general as it assumes we are the only thread accessing the page of `p`.
bool mi_unsafe_heap_page_is_under_utilized(mi_heap_t* heap, void* p, size_t perc_threshold) mi_attr_noexcept {
  if (p==NULL) return false;
  const mi_page_t* const page = _mi_safe_ptr_page(p);   // Get the page containing this pointer
  if (page==NULL || page->used==page->capacity || page->capacity < page->reserved) return false;
  // If the page is the head of the queue, it is currently being used for
  // allocations; we skip it to avoid immediate thrashing.
  if (page->prev == NULL)  return false;

  // match heap?
  const mi_heap_t* const page_heap = mi_page_heap(page);
  if (page_heap==NULL) return false;
  if (heap!=NULL && page_heap!=heap) return false;

  // check utilization
  if (page->capacity==0)   return false;
  if (perc_threshold>=100) return true;
  return (perc_threshold >= ((100UL*page->used) / page->capacity));
}
