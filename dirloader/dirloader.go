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

// Package dirloader provides a loader based on the local directory
// to load some resources.
package dirloader

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xgfone/go-loader/internal/comments"
	"github.com/xgfone/go-loader/internal/mapx"
	"github.com/xgfone/go-loader/resource"
)

type info struct {
	modtime time.Time
	size    int64
}

func (i info) Equal(other info) bool {
	return i.size == other.size && i.modtime.Equal(other.modtime)
}

type file struct {
	buf  *bytes.Buffer
	data []byte

	last info
	now  info
}

// DirLoader is used to load the resources from the files in a directory.
type DirLoader[T any] struct {
	rsc *resource.Resource[[]T]
	dec func(data []byte, dst any) error

	dir   string
	lock  sync.Mutex
	files map[string]*file
	epoch uint64
}

// New returns a new DirLoader with the directory.
func New[T any](dir string) *DirLoader[T] {
	dir, err := filepath.Abs(dir)
	if err != nil {
		panic(err)
	}

	return &DirLoader[T]{
		dir:   dir,
		dec:   json.Unmarshal,
		rsc:   resource.New[[]T](),
		files: make(map[string]*file, 8),
	}
}

func wrappanic(ctx context.Context) {
	if r := recover(); r != nil {
		slog.ErrorContext(ctx, "wrap a panic", "panic", r)
	}
}

func (l *DirLoader[T]) SetDecoder(decode func(data []byte, dst any) error) *DirLoader[T] {
	if decode == nil {
		panic("DirLoader.SetDecoder: decode function must not be nil")
	}

	l.dec = decode
	return l
}

// Sync is used to synchronize the resources to the chan ch periodically.
//
// If cb is nil, never call it when reload the resources.
func (l *DirLoader[T]) Sync(ctx context.Context, rsctype string, interval time.Duration, reload <-chan struct{}, cb func([]T) (changed bool)) {
	if interval <= 0 {
		interval = time.Minute
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	var lastEtag string
	load := func() {
		defer wrappanic(ctx)
		slog.LogAttrs(ctx, slog.LevelDebug-4, "start to load the resource", slog.String("type", rsctype))

		resources, etag, err := l.Load()
		if err != nil {
			slog.Error("fail to load the resources from the local files", "type", rsctype, "err", err)
			return
		}

		if etag == lastEtag {
			return
		}

		if cb != nil && cb(resources) {
			l.rsc.SetResource(resources)
		}

		lastEtag = etag
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

func (l *DirLoader[T]) updateEpoch() {
	epoch := atomic.AddUint64(&l.epoch, 1)
	l.rsc.SetEtag(strconv.FormatUint(epoch, 10))
}

// Resource returns the inner resource.
func (l *DirLoader[T]) Resource() *resource.Resource[[]T] {
	return l.rsc
}

// Load scans the files in the directory, loads and returns them if changed.
func (l *DirLoader[T]) Load() (resources []T, etag string, err error) {
	l.lock.Lock()
	defer l.lock.Unlock()

	if err = l.scanfiles(); err != nil {
		return
	}

	changed, err := l.checkfiles()
	if err != nil {
		return
	} else if !changed {
		return l.rsc.Resource(), l.rsc.Etag(), nil
	}

	resources = make([]T, 0, len(l.files))
	paths := mapx.Keys(l.files)
	slices.Sort(paths)
	for _, path := range paths {
		file := l.files[path]

		var resource []T
		if err = l.decode(&resource, file.data); err != nil {
			err = fmt.Errorf("fail to decode resource file '%s': %w", path, err)
			return
		}
		resources = append(resources, resource...)
	}

	l.rsc.SetResource(resources)
	etag = l.rsc.Etag()
	return
}

func (l *DirLoader[T]) decode(dst *[]T, data []byte) error {
	if len(data) == 0 {
		return nil
	}
	return l.dec(data, dst)
}

func (l *DirLoader[T]) checkfiles() (changed bool, err error) {
	for path, file := range l.files {
		if file.last.Equal(file.now) {
			continue
		}

		changed = true
		if err = l._readfile(file.buf, path); err != nil {
			err = fmt.Errorf("fail to read the file '%s': %w", path, err)
			return
		}

		file.data = comments.RemoveLineComments(file.buf.Bytes(), comments.CommentSlashes)
		file.last = file.now
	}

	if changed {
		l.updateEpoch()
	}

	return
}

func (l *DirLoader[T]) _readfile(buf *bytes.Buffer, path string) (err error) {
	file, err := os.Open(path)
	if err != nil {
		return
	}
	defer file.Close()

	buf.Reset()
	_, err = io.CopyBuffer(buf, file, make([]byte, 1024))
	return
}

func (l *DirLoader[T]) scanfiles() (err error) {
	files := make(map[string]struct{}, max(8, len(l.files)))
	err = filepath.WalkDir(l.dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return fmt.Errorf("fail to walk dir '%s': %w", l.dir, err)
		}

		if d.IsDir() {
			return nil
		}

		if name := d.Name(); strings.HasPrefix(name, "_") || !strings.HasSuffix(name, ".json") {
			return nil
		}

		for _path := strings.TrimPrefix(path, l.dir); len(_path) > 0; {
			var name string
			index := strings.IndexByte(_path, filepath.Separator)
			if index < 0 {
				name, _path = _path, ""
			} else {
				name, _path = _path[:index], _path[index+1:]
			}

			if strings.HasPrefix(name, "_") {
				return nil
			}
		}

		fi, err := d.Info()
		if err != nil {
			return fmt.Errorf("fail to get info of file '%s': %w", path, err)
		}

		f, ok := l.files[path]
		if !ok {
			f = &file{buf: bytes.NewBuffer(make([]byte, 0, fi.Size()))}
			l.files[path] = f
		}

		f.now = info{modtime: fi.ModTime(), size: fi.Size()}
		files[path] = struct{}{}
		return nil
	})

	if err != nil {
		return
	}

	// Clean the non-exist files.
	for path := range l.files {
		if _, ok := files[path]; !ok {
			delete(l.files, path)
		}
	}

	return
}
