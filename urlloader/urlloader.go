// Copyright 2024 xgfone
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package urlloader provides a loader based on the url to load a resource.
package urlloader

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/xgfone/go-loader/resource"
)

// UrlLoader is a resource loader based on the url.
type UrlLoader[T any] struct {
	rsc *resource.Resource[T]
	url string

	decode func(data []byte, dst any) error
	sender func(*http.Request) (*http.Response, error)

	lock sync.Mutex
}

// New returns a new UrlLoader with the url.
func New[T any](url string) *UrlLoader[T] {
	if url == "" {
		panic("UrlLoader: url must not be empty")
	}

	return &UrlLoader[T]{
		url:    url,
		rsc:    resource.New[T](),
		decode: json.Unmarshal,
		sender: http.DefaultClient.Do,
	}
}

// SetSender sets the http request send function.
func (l *UrlLoader[T]) SetSender(sender func(*http.Request) (*http.Response, error)) *UrlLoader[T] {
	if sender == nil {
		panic("UrlLoader.SetSender: request send function must not be nil")
	}

	l.lock.Lock()
	l.sender = sender
	l.lock.Unlock()
	return l
}

// SetDecoder resets the resource decoder.
func (l *UrlLoader[T]) SetDecoder(decode func(data []byte, dst any) error) *UrlLoader[T] {
	if decode == nil {
		panic("UrlLoader.SetDecoder: decode function must not be nil")
	}

	l.lock.Lock()
	l.decode = decode
	l.lock.Unlock()
	return l
}

// Resource returns the inner resource.
func (l *UrlLoader[T]) Resource() *resource.Resource[T] {
	return l.rsc
}

func wrappanic(ctx context.Context) {
	if r := recover(); r != nil {
		slog.ErrorContext(ctx, "wrap a panic", "panic", r)
	}
}

// Sync is used to synchronize the resource to the chan ch periodically.
//
// If cb is nil, never call it when reload the resources.
func (l *UrlLoader[T]) Sync(ctx context.Context, rsctype string, interval time.Duration, reload <-chan struct{}, cb func(T)) {
	if interval <= 0 {
		interval = time.Minute
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	var lastEtag string
	load := func() {
		defer wrappanic(ctx)
		slog.LogAttrs(ctx, slog.LevelDebug-4, "start to load the resource", slog.String("type", rsctype))

		resource, etag, err := l.Load()
		if err != nil {
			slog.Error("fail to load the resources from the url", "type", rsctype, "url", l.url, "err", err)
			return
		}

		if etag == lastEtag {
			return
		}

		lastEtag = etag
		if cb != nil {
			cb(resource)
		}
	}

	// first laod
	load()
	for {
		select {
		case <-ctx.Done():
			select {
			case <-ticker.C:
			default:
			}
			return

		case <-reload:
			load()

		case <-ticker.C:
			load()
		}
	}
}

// Load checks whether the resource has changed by the url,
// and reloads it if changed.
func (l *UrlLoader[T]) Load() (resource T, etag string, err error) {
	l.lock.Lock()
	defer l.lock.Unlock()

	if err = l.download(); err == nil {
		resource, etag = l.rsc.Get()
	}

	return
}

func (l *UrlLoader[T]) download() (err error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, l.url, nil)
	if err != nil {
		return
	}

	etag := l.rsc.Etag()
	if etag != "" {
		req.Header.Set("If-Match", etag)
	}

	resp, err := l.sender(req)
	if resp != nil {
		defer resp.Body.Close()
	}

	if err != nil || resp.StatusCode == 304 {
		return
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return
	}

	if resp.StatusCode != 200 {
		err = fmt.Errorf("code=%d, err=%s", resp.StatusCode, string(data))
		return
	}

	var rsc T
	err = l.decode(data, &rsc)
	if err != nil {
		return
	}

	etag = resp.Header.Get("Etag")
	l.rsc.Set(rsc, etag)
	return
}
