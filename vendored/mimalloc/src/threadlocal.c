/* ----------------------------------------------------------------------------
Copyright (c) 2019-2025, Microsoft Research, Daan Leijen
This is free software; you can redistribute it and/or modify it under the
terms of the MIT license. A copy of the license can be found in the file
"LICENSE" at the root of this distribution.
-----------------------------------------------------------------------------*/

/* ----------------------------------------------------------------------------
Implement dynamic thread local variables (for heap's).
Unlike most OS native implementations there is no limit on the number
that can be allocated.
-----------------------------------------------------------------------------*/

#include "mimalloc.h"
#include "mimalloc/internal.h"
#include "mimalloc/prim.h"

/* -----------------------------------------------------------
Each thread can have (a dynamically expanding) array of
thread-local values.
----------------------------------------------------------- */

typedef struct mi_thread_locals_s {
  size_t count;
  void*  slots[1];
} mi_thread_locals_t;

static mi_thread_locals_t mi_thread_locals_empty = { 0, {NULL} };

mi_decl_thread mi_thread_locals_t* mi_thread_locals = &mi_thread_locals_empty;  // always point to a valid `mi_thread_locals_t`

// dynamically reallocate the thread local slots when needed
static mi_thread_locals_t* mi_thread_locals_expand(mi_thread_local_t atleast) {
  mi_thread_locals_t* tls_old = mi_thread_locals;
  const size_t count_old = tls_old->count;
  size_t count;
  if (count_old==0) {
    tls_old = NULL; // so we allocate fresh from mi_thread_locals_empty
    count = 16;     // start with 16 slots
  } 
  else if (count_old >= 1024) {
    count = count_old + 1024;  // at some point increase linearly
  }
  else {
    count = 2*count_old;       // and double initially
  }
  if (count <= atleast) {
    count = atleast + 1;
  }
  mi_thread_locals_t* tls = (mi_thread_locals_t*)mi_rezalloc(tls_old, sizeof(mi_thread_locals_t) + count*sizeof(void*));
  if mi_unlikely(tls==NULL) return NULL;
  tls->count = count;
  mi_thread_locals = tls;
  return tls;
}

static mi_decl_noinline bool mi_thread_local_set_expand( mi_thread_local_t key, void* val ) {
  if (val==NULL) return true;
  mi_thread_locals_t* tls = mi_thread_locals_expand(key);
  if (tls==NULL) return false;
  mi_assert_internal(key < tls->count);
  mi_assert_internal(tls == mi_thread_locals);
  tls->slots[key] = val;
  return true;
}

// set a tls slot; returns `true` if successful.
// Can return `false` if we could not reallocate the slots array.
bool _mi_thread_local_set( mi_thread_local_t key, void* val ) {
  mi_thread_locals_t* tls = mi_thread_locals;
  mi_assert_internal(tls!=NULL);
  if mi_likely(key < tls->count) {
    tls->slots[key] = val;
    return true;
  }
  else {
    return mi_thread_local_set_expand( key, val );  // tailcall
  }
}

// get a tls slot value
void* _mi_thread_local_get( mi_thread_local_t key ) {
  const mi_thread_locals_t* const tls = mi_thread_locals;
  mi_assert_internal(tls!=NULL);
  if mi_likely(key < tls->count) {
    return tls->slots[key];
  }
  else {
    return NULL;
  }
}

void _mi_thread_locals_thread_done(void) {
  mi_thread_locals_t* const tls = mi_thread_locals;
  if (tls!=NULL && tls->count > 0) {
    mi_free(tls);
    mi_thread_locals = &mi_thread_locals_empty;
  }
}

/* -----------------------------------------------------------
Create and free fresh TLS key's
----------------------------------------------------------- */
#include "bitmap.h"

static mi_lock_t    mi_thread_locals_lock;    // we need a lock in order to re-allocate the slot bits
static mi_bitmap_t* mi_thread_locals_free;    // reuse an arena bitmap to track which slots were assigned (1=free, 0=in-use)

void _mi_thread_locals_init(void) {
  mi_lock_init(&mi_thread_locals_lock);
}

void _mi_thread_locals_done(void) {
  mi_lock(&mi_thread_locals_lock) {
    mi_bitmap_t* const slots = mi_thread_locals_free;
    mi_free(slots);
  }
  mi_lock_done(&mi_thread_locals_lock);
}

// strange signature but allows us to reuse the arena code for claiming free pages
static bool mi_thread_local_claim(size_t _slice_index, mi_arena_t* _arena, bool* keep_set) {
  MI_UNUSED(_slice_index); MI_UNUSED(_arena);
  *keep_set = false;
  return true;
}

static mi_thread_local_t mi_thread_local_create_expand(void) {
  size_t key = 0;
  mi_bitmap_t* slots = mi_thread_locals_free;
  // 1024 bits at a time
  const size_t oldcount = (slots==NULL ? 0 : mi_bitmap_max_bits(slots));
  const size_t newcount = 1024 + oldcount;
  const size_t newsize = mi_bitmap_size( newcount, NULL );
  slots = (mi_bitmap_t*)mi_realloc_aligned(slots, newsize, MI_BCHUNK_SIZE);
  if (slots != NULL) {
    mi_bitmap_init(slots, newcount, true /* or otherwise we would zero all old entries */);
    mi_bitmap_unsafe_setN(slots, oldcount, newcount - oldcount);
    mi_thread_locals_free = slots;
    size_t idx = 0;
    if mi_likely(slots!=NULL && mi_bitmap_try_find_and_claim(slots,0,&idx,&mi_thread_local_claim,NULL)) {
      key = idx+1;
    }
  }
  return key;
}

// create a fresh key
mi_thread_local_t _mi_thread_local_create(void) {
  mi_thread_local_t key = 0;
  mi_lock(&mi_thread_locals_lock) {
    mi_bitmap_t* slots = mi_thread_locals_free;
    size_t idx = 0;
    if mi_likely(slots!=NULL && mi_bitmap_try_find_and_claim(slots,0,&idx,&mi_thread_local_claim,NULL)) {
      key = idx+1;
    }
    else {
      key = mi_thread_local_create_expand();
    }
  }
  return key;
}

// free a key
void _mi_thread_local_free(mi_thread_local_t key) {
  if (key==0) return;
  const size_t idx = key-1;
  mi_lock(&mi_thread_locals_lock) {
    mi_bitmap_t* const slots = mi_thread_locals_free;
    if (slots!=NULL && idx < mi_bitmap_max_bits(slots)) {
      mi_bitmap_set(slots,key-1);
    }
  }
}

