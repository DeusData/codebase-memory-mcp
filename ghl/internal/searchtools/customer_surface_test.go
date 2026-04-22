package searchtools

import (
	"context"
	"testing"
)

// TestHandleCustomerSurface_Batch verifies the happy path: multiple files,
// mixed Vue + backend, a known repo in the embedded product map, a fetch call
// in the Vue file. Confirms the handler composes enricher.BuildCustomerSurface
// for each input file and returns them in input order.
func TestHandleCustomerSurface_Batch(t *testing.T) {
	vueSource := `<template>
  <div>{{ t('user.permissions.title') }}</div>
</template>

<script setup lang="ts">
import axios from 'axios'

async function load(id: string) {
  const { data } = await axios.get('/v2/users/' + id + '/permissions')
  return data
}
</script>
`
	backendSource := `
import { Injectable } from '@nestjs/common'

@Injectable()
export class IamCacheWorker {
  async refresh() {}
}
`

	args := CustomerSurfaceArgs{
		Repo: "ghl-crm-frontend",
		Files: []CustomerSurfaceFile{
			{Path: "apps/settings/src/components/user/UserPermissionsV2.vue", Source: vueSource},
			{Path: "apps/settings/src/workers/unmapped.ts", Source: backendSource},
		},
	}

	res, err := HandleCustomerSurface(context.Background(), args)
	if err != nil {
		t.Fatalf("HandleCustomerSurface returned error: %v", err)
	}
	if res == nil {
		t.Fatal("result is nil")
	}
	if res.Repo != "ghl-crm-frontend" {
		t.Errorf("Repo = %q, want %q", res.Repo, "ghl-crm-frontend")
	}
	if res.Count != 2 || len(res.Surfaces) != 2 {
		t.Fatalf("Count=%d len(Surfaces)=%d, want 2/2", res.Count, len(res.Surfaces))
	}

	// First surface: Vue file → product resolved, component name + fetch call present.
	v := res.Surfaces[0]
	if v.FilePath != "apps/settings/src/components/user/UserPermissionsV2.vue" {
		t.Errorf("Surface[0].FilePath = %q", v.FilePath)
	}
	if v.Product == "" || v.Product == "Unknown — no product mapping" {
		t.Errorf("Surface[0].Product = %q, want a real CRM — Settings label", v.Product)
	}
	if v.ComponentName != "UserPermissionsV2" {
		t.Errorf("Surface[0].ComponentName = %q, want UserPermissionsV2", v.ComponentName)
	}
	if !v.HasTemplate || !v.HasScriptSetup {
		t.Errorf("Surface[0] flags: HasTemplate=%v HasScriptSetup=%v", v.HasTemplate, v.HasScriptSetup)
	}
	if len(v.FetchCalls) != 1 {
		t.Fatalf("Surface[0].FetchCalls = %d, want 1", len(v.FetchCalls))
	}
	if v.FetchCalls[0].Method != "GET" || v.FetchCalls[0].Style != "axios" {
		t.Errorf("Surface[0].FetchCalls[0] = %+v", v.FetchCalls[0])
	}
	if len(v.I18nKeys) == 0 || v.I18nKeys[0] != "user.permissions.title" {
		t.Errorf("Surface[0].I18nKeys = %+v", v.I18nKeys)
	}

	// Second surface: backend-only unmapped path → product resolved via apps/settings prefix.
	b := res.Surfaces[1]
	if b.FilePath != "apps/settings/src/workers/unmapped.ts" {
		t.Errorf("Surface[1].FilePath = %q", b.FilePath)
	}
	if b.ComponentName != "" || b.HasTemplate || b.HasScriptSetup {
		t.Errorf("Surface[1] should have no Vue metadata; got %+v", b)
	}
	if len(b.FetchCalls) != 0 {
		t.Errorf("Surface[1].FetchCalls should be empty; got %+v", b.FetchCalls)
	}
}

// TestHandleCustomerSurface_UnknownRepo verifies that files in a repo with no
// product_map coverage are labelled "Unknown" — never erroring the whole batch.
func TestHandleCustomerSurface_UnknownRepo(t *testing.T) {
	args := CustomerSurfaceArgs{
		Repo: "some-repo-that-does-not-exist",
		Files: []CustomerSurfaceFile{
			{Path: "apps/foo/bar.ts", Source: "export const x = 1"},
		},
	}
	res, err := HandleCustomerSurface(context.Background(), args)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(res.Surfaces) != 1 {
		t.Fatalf("want 1 surface, got %d", len(res.Surfaces))
	}
	if res.Surfaces[0].Product != "Unknown — no product mapping" {
		t.Errorf("Product = %q, want Unknown label", res.Surfaces[0].Product)
	}
}

// TestHandleCustomerSurface_EmptyFiles verifies the handler accepts an empty
// batch gracefully (returns count=0, no error). Needed because PR-diff input
// may be empty if only deletes happened.
func TestHandleCustomerSurface_EmptyFiles(t *testing.T) {
	res, err := HandleCustomerSurface(context.Background(), CustomerSurfaceArgs{
		Repo:  "ghl-crm-frontend",
		Files: nil,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Count != 0 || len(res.Surfaces) != 0 {
		t.Errorf("empty input: Count=%d len=%d, want 0/0", res.Count, len(res.Surfaces))
	}
}

// TestHandleCustomerSurface_MissingRepo verifies that omitting `repo` is an
// error: the caller must always scope a request to a repo, otherwise product
// resolution is ambiguous.
func TestHandleCustomerSurface_MissingRepo(t *testing.T) {
	_, err := HandleCustomerSurface(context.Background(), CustomerSurfaceArgs{
		Files: []CustomerSurfaceFile{{Path: "x.ts", Source: "x"}},
	})
	if err == nil {
		t.Fatal("expected error on missing repo, got nil")
	}
}
