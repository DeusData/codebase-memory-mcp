package cbm

/*
#cgo CFLAGS: -I../../src -I../../vendored -I../../vendored/sqlite3 -I../../vendored/mimalloc/include -I../../internal/cbm -I../../internal/cbm/vendored/ts_runtime/include
#cgo LDFLAGS: -L../../build/c -lcbm -lm -lstdc++ -lpthread -lz
#include <stdlib.h>
#include "mcp/mcp.h"
#include "foundation/mem.h"
*/
import "C"
import (
	"errors"
	"sync"
	"unsafe"
)

// Bridge wraps the C MCP server for use from Go.
type Bridge struct {
	srv *C.cbm_mcp_server_t
	mu  sync.Mutex
}

var initOnce sync.Once

// Init initializes the C memory subsystem and creates a new MCP server.
// storePath is the SQLite database directory (pass "" for in-memory default).
func Init(storePath string) (*Bridge, error) {
	initOnce.Do(func() {
		C.cbm_mem_init(C.double(0.5))
	})

	var cPath *C.char
	if storePath != "" {
		cPath = C.CString(storePath)
		defer C.free(unsafe.Pointer(cPath))
	}

	srv := C.cbm_mcp_server_new(cPath)
	if srv == nil {
		return nil, errors.New("cbm: failed to create MCP server")
	}

	return &Bridge{srv: srv}, nil
}

// HandleTool calls a C-implemented MCP tool and returns the JSON result.
func (b *Bridge) HandleTool(toolName, argsJSON string) (string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	cName := C.CString(toolName)
	defer C.free(unsafe.Pointer(cName))

	cArgs := C.CString(argsJSON)
	defer C.free(unsafe.Pointer(cArgs))

	result := C.cbm_mcp_handle_tool(b.srv, cName, cArgs)
	if result == nil {
		return "", errors.New("cbm: tool returned nil")
	}
	defer C.free(unsafe.Pointer(result))

	return C.GoString(result), nil
}

// HandleRequest processes a raw JSON-RPC request line through the C server.
func (b *Bridge) HandleRequest(line string) (string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	cLine := C.CString(line)
	defer C.free(unsafe.Pointer(cLine))

	result := C.cbm_mcp_server_handle(b.srv, cLine)
	if result == nil {
		// Notification — no response needed
		return "", nil
	}
	defer C.free(unsafe.Pointer(result))

	return C.GoString(result), nil
}

// ToolsList returns the JSON tools/list response from C tools.
func ToolsList() string {
	result := C.cbm_mcp_tools_list()
	if result == nil {
		return "[]"
	}
	defer C.free(unsafe.Pointer(result))
	return C.GoString(result)
}

// InitializeResponse returns the MCP initialize response JSON.
func InitializeResponse() string {
	result := C.cbm_mcp_initialize_response()
	if result == nil {
		return "{}"
	}
	defer C.free(unsafe.Pointer(result))
	return C.GoString(result)
}

// Close frees the C MCP server.
func (b *Bridge) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.srv != nil {
		C.cbm_mcp_server_free(b.srv)
		b.srv = nil
	}
}
