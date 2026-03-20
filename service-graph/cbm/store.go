package cbm

/*
#include <stdlib.h>
#include "store/store.h"
#include "ui/layout3d.h"
#include "foundation/platform.h"
*/
import "C"
import (
	"errors"
	"unsafe"
)

// StoreHandle wraps a read-only C store for a project.
type StoreHandle struct {
	s *C.cbm_store_t
}

// OpenStore opens a SQLite store at the given path.
func OpenStore(dbPath string) (*StoreHandle, error) {
	cPath := C.CString(dbPath)
	defer C.free(unsafe.Pointer(cPath))
	s := C.cbm_store_open_path(cPath)
	if s == nil {
		return nil, errors.New("cannot open store")
	}
	return &StoreHandle{s: s}, nil
}

// Close closes the store.
func (h *StoreHandle) Close() {
	if h.s != nil {
		C.cbm_store_close(h.s)
		h.s = nil
	}
}

// CountNodes returns the number of nodes for a project.
func (h *StoreHandle) CountNodes(project string) int {
	cProj := C.CString(project)
	defer C.free(unsafe.Pointer(cProj))
	return int(C.cbm_store_count_nodes(h.s, cProj))
}

// CountEdges returns the number of edges for a project.
func (h *StoreHandle) CountEdges(project string) int {
	cProj := C.CString(project)
	defer C.free(unsafe.Pointer(cProj))
	return int(C.cbm_store_count_edges(h.s, cProj))
}

// ComputeLayout runs the C 3D layout engine and returns JSON.
func (h *StoreHandle) ComputeLayout(project string, maxNodes int) (string, error) {
	cProj := C.CString(project)
	defer C.free(unsafe.Pointer(cProj))

	layout := C.cbm_layout_compute(h.s, cProj, C.CBM_LAYOUT_OVERVIEW, nil, 0, C.int(maxNodes))
	if layout == nil {
		return "", errors.New("layout computation failed")
	}
	defer C.cbm_layout_free(layout)

	cJSON := C.cbm_layout_to_json(layout)
	if cJSON == nil {
		return "", errors.New("JSON serialization failed")
	}
	defer C.free(unsafe.Pointer(cJSON))

	return C.GoString(cJSON), nil
}

// AdrGet retrieves the ADR content for a project.
func (h *StoreHandle) AdrGet(project string) (content, updatedAt string, hasAdr bool) {
	cProj := C.CString(project)
	defer C.free(unsafe.Pointer(cProj))

	var adr C.cbm_adr_t
	rc := C.cbm_store_adr_get(h.s, cProj, &adr)
	if rc != C.CBM_STORE_OK || adr.content == nil {
		return "", "", false
	}
	defer C.cbm_store_adr_free(&adr)

	c := C.GoString(adr.content)
	u := ""
	if adr.updated_at != nil {
		u = C.GoString(adr.updated_at)
	}
	return c, u, true
}

// AdrStore saves ADR content for a project.
func (h *StoreHandle) AdrStore(project, content string) error {
	cProj := C.CString(project)
	defer C.free(unsafe.Pointer(cProj))
	cContent := C.CString(content)
	defer C.free(unsafe.Pointer(cContent))

	rc := C.cbm_store_adr_store(h.s, cProj, cContent)
	if rc != C.CBM_STORE_OK {
		return errors.New("save failed")
	}
	return nil
}

// FileExists checks if a file exists.
func FileExists(path string) bool {
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))
	return bool(C.cbm_file_exists(cPath))
}

// IsDir checks if a path is a directory.
func IsDir(path string) bool {
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))
	return bool(C.cbm_is_dir(cPath))
}

// FileSize returns the size of a file in bytes.
func FileSize(path string) int64 {
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))
	return int64(C.cbm_file_size(cPath))
}
