package flight

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Fetcher defines the interface for fetching WASM flight modules.
type Fetcher interface {
	// Fetch retrieves the WASM bytes for the given flight source.
	// The source can be an HTTP(S) URL, a configmap reference (configmap://<namespace>/<name>/<key>),
	// or a local file path.
	Fetch(ctx context.Context, source string) ([]byte, error)
}

// MultiFetcher combines multiple fetch strategies.
type MultiFetcher struct {
	Client     client.Client
	HTTPClient *http.Client
	cache      sync.Map
}

// NewFetcher creates a new MultiFetcher with the given Kubernetes client.
func NewFetcher(c client.Client) Fetcher {
	return &MultiFetcher{
		Client: c,
		HTTPClient: &http.Client{
			Timeout: 60 * time.Second,
		},
	}
}

// Fetch retrieves WASM bytes based on the source string.
func (f *MultiFetcher) Fetch(ctx context.Context, source string) ([]byte, error) {
	if source == "" {
		return nil, fmt.Errorf("empty source")
	}

	switch {
	case strings.HasPrefix(source, "http://") || strings.HasPrefix(source, "https://"):
		return f.fetchHTTP(ctx, source)
	case strings.HasPrefix(source, "configmap://"):
		return f.fetchConfigMap(ctx, source)
	case strings.HasPrefix(source, "file://"):
		return f.fetchFile(strings.TrimPrefix(source, "file://"))
	default:
		// Try as file path
		return f.fetchFile(source)
	}
}

// fetchHTTP downloads the WASM module from an HTTP(S) URL.
func (f *MultiFetcher) fetchHTTP(ctx context.Context, url string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("creating HTTP request for %s: %w", url, err)
	}

	resp, err := f.HTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetching WASM from %s: %w", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("fetching WASM from %s: unexpected status %d", url, resp.StatusCode)
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading WASM response from %s: %w", url, err)
	}

	return data, nil
}

// fetchConfigMap retrieves the WASM module from a Kubernetes ConfigMap.
// The source format is: configmap://<namespace>/<name>/<key>
// If key is omitted, it defaults to "module.wasm".
func (f *MultiFetcher) fetchConfigMap(ctx context.Context, source string) ([]byte, error) {
	trimmed := strings.TrimPrefix(source, "configmap://")
	parts := strings.SplitN(trimmed, "/", 3)

	if len(parts) < 2 {
		return nil, fmt.Errorf("invalid configmap source format %q: expected configmap://<namespace>/<name>[/<key>]", source)
	}

	namespace := parts[0]
	name := parts[1]
	key := "module.wasm"
	if len(parts) == 3 && parts[2] != "" {
		key = parts[2]
	}

	cm := &corev1.ConfigMap{}
	if err := f.Client.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, cm); err != nil {
		return nil, fmt.Errorf("getting ConfigMap %s/%s: %w", namespace, name, err)
	}

	// Check BinaryData first
	if cm.BinaryData != nil {
		if data, ok := cm.BinaryData[key]; ok {
			return data, nil
		}
	}

	// Fall back to Data (base64 or plain text)
	if cm.Data != nil {
		if data, ok := cm.Data[key]; ok {
			return []byte(data), nil
		}
	}

	return nil, fmt.Errorf("key %q not found in ConfigMap %s/%s", key, namespace, name)
}

// fetchFile reads the WASM module from a local file path.
func (f *MultiFetcher) fetchFile(path string) ([]byte, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading WASM file %s: %w", path, err)
	}
	return data, nil
}

// CachingFetcher wraps a Fetcher and caches results.
type CachingFetcher struct {
	Inner Fetcher
	mu    sync.RWMutex
	cache map[string]cachedEntry
	TTL   time.Duration
}

type cachedEntry struct {
	data      []byte
	fetchedAt time.Time
}

// NewCachingFetcher creates a new CachingFetcher wrapping the given Fetcher.
func NewCachingFetcher(inner Fetcher, ttl time.Duration) Fetcher {
	return &CachingFetcher{
		Inner: inner,
		cache: make(map[string]cachedEntry),
		TTL:   ttl,
	}
}

// Fetch retrieves WASM bytes, using cache if available and not expired.
func (f *CachingFetcher) Fetch(ctx context.Context, source string) ([]byte, error) {
	f.mu.RLock()
	if entry, ok := f.cache[source]; ok && time.Since(entry.fetchedAt) < f.TTL {
		f.mu.RUnlock()
		return entry.data, nil
	}
	f.mu.RUnlock()

	data, err := f.Inner.Fetch(ctx, source)
	if err != nil {
		return nil, err
	}

	f.mu.Lock()
	f.cache[source] = cachedEntry{data: data, fetchedAt: time.Now()}
	f.mu.Unlock()

	return data, nil
}