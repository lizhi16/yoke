package wasm

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sync"

	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
	"github.com/tetratelabs/wazero/imports/wasi_snapshot_preview1"
)

// Runner defines the interface for executing WASM modules.
type Runner interface {
	// Run executes the given WASM module bytes with the provided input (passed via stdin)
	// and returns the output (captured from stdout).
	Run(ctx context.Context, wasmBytes []byte, input []byte) ([]byte, error)
}

// WazeroRunner implements Runner using the wazero runtime.
type WazeroRunner struct {
	mu    sync.Mutex
	cache map[string]wazero.CompiledModule
}

// NewRunner creates a new WazeroRunner.
func NewRunner() Runner {
	return &WazeroRunner{
		cache: make(map[string]wazero.CompiledModule),
	}
}

// Run executes the WASM module with the given input bytes piped to stdin,
// and returns the bytes written to stdout.
func (r *WazeroRunner) Run(ctx context.Context, wasmBytes []byte, input []byte) ([]byte, error) {
	// Create a new runtime for each execution to ensure isolation.
	rtConfig := wazero.NewRuntimeConfig().
		WithCloseOnContextDone(true)

	rt := wazero.NewRuntimeWithConfig(ctx, rtConfig)
	defer rt.Close(ctx)

	// Instantiate WASI to provide stdin/stdout/stderr support.
	wasi_snapshot_preview1.MustInstantiate(ctx, rt)

	// Compute hash of the WASM bytes for caching compiled modules.
	hash := hashBytes(wasmBytes)

	// Try to get a cached compiled module.
	r.mu.Lock()
	compiled, cached := r.cache[hash]
	r.mu.Unlock()

	var err error
	if !cached {
		compiled, err = rt.CompileModule(ctx, wasmBytes)
		if err != nil {
			return nil, fmt.Errorf("failed to compile WASM module: %w", err)
		}
		r.mu.Lock()
		r.cache[hash] = compiled
		r.mu.Unlock()
	}

	// Create a buffer to capture stdout.
	stdinBuf := &readCloserBuffer{data: input}
	stdoutBuf := &writeBuffer{}
	stderrBuf := &writeBuffer{}

	moduleConfig := wazero.NewModuleConfig().
		WithStdin(stdinBuf).
		WithStdout(stdoutBuf).
		WithStderr(stderrBuf).
		WithName("").
		WithArgs("flight")

	mod, err := rt.InstantiateModule(ctx, compiled, moduleConfig)
	if err != nil {
		// If there's stderr output, include it in the error for debugging.
		if stderrBuf.Len() > 0 {
			return nil, fmt.Errorf("failed to run WASM module: %w; stderr: %s", err, stderrBuf.Bytes())
		}
		return nil, fmt.Errorf("failed to run WASM module: %w", err)
	}

	// Close the module to ensure cleanup.
	if mod != nil {
		_ = mod.Close(ctx)
	}

	output := stdoutBuf.Bytes()
	if len(output) == 0 {
		if stderrBuf.Len() > 0 {
			return nil, fmt.Errorf("WASM module produced no output; stderr: %s", stderrBuf.Bytes())
		}
		return nil, fmt.Errorf("WASM module produced no output")
	}

	return output, nil
}

// Ensure api.Module is used (avoid import errors).
var _ api.Module

func hashBytes(data []byte) string {
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:])
}

// readCloserBuffer implements io.Reader for piping input to stdin.
type readCloserBuffer struct {
	data []byte
	pos  int
}

func (b *readCloserBuffer) Read(p []byte) (int, error) {
	if b.pos >= len(b.data) {
		return 0, fmt.Errorf("EOF")
	}
	n := copy(p, b.data[b.pos:])
	b.pos += n
	if b.pos >= len(b.data) {
		return n, fmt.Errorf("EOF")
	}
	return n, nil
}

// writeBuffer implements io.Writer for capturing stdout/stderr.
type writeBuffer struct {
	mu   sync.Mutex
	data []byte
}

func (b *writeBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.data = append(b.data, p...)
	return len(p), nil
}

func (b *writeBuffer) Bytes() []byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]byte, len(b.data))
	copy(out, b.data)
	return out
}

func (b *writeBuffer) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.data)
}