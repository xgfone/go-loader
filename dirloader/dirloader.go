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
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/xgfone/go-loader/internal/comments"
	"github.com/xgfone/go-loader/resource"
)

// DefaultFileFilter is the default file filter.
var DefaultFileFilter = AndFilter(JsonFileFilter, DenyPrefixFileFilter("_"))

// FileFilter is used to filter the files which are allowed.
type FileFilter func(File) (ok bool)

// AndFilter returns a file filter which allows the files
// only if all the filters returns true.
func AndFilter(filters ...FileFilter) FileFilter {
	for _, filter := range filters {
		if filter == nil {
			panic("AndFilter: filter must not be nil")
		}
	}

	return func(file File) bool {
		for _, filter := range filters {
			if !filter(file) {
				return false
			}
		}
		return true
	}
}

// JsonFileFilter is a file filter which only allows the filename ending with ".json".
func JsonFileFilter(file File) bool {
	return strings.HasSuffix(file.Name, ".json")
}

func AllowPrefixFileFilter(prefixes ...string) FileFilter {
	return matchPreifxFileFilter(true, prefixes)
}

func DenyPrefixFileFilter(prefixes ...string) FileFilter {
	return matchPreifxFileFilter(false, prefixes)
}

func matchPreifxFileFilter(match bool, prefixes []string) FileFilter {
	if len(prefixes) == 0 {
		return func(File) bool { return true }
	}

	return func(file File) bool {
		if matchprefixes(file, prefixes) {
			return match
		}
		return !match
	}
}

func matchprefixes(file File, prefixes []string) bool {
	refpath := strings.TrimPrefix(file.Path, file.Root)
	refpath = strings.TrimPrefix(refpath, string(os.PathSeparator))
	for len(refpath) > 0 {
		var name string
		index := strings.IndexByte(refpath, filepath.Separator)
		if index < 0 {
			name, refpath = refpath, ""
		} else {
			name, refpath = refpath[:index], refpath[index+1:]
		}

		for _, prefix := range prefixes {
			if strings.HasPrefix(name, prefix) {
				return true
			}
		}
	}
	return false
}

/// ----------------------------------------------------------------------- ///

// DefaultFileDecoder is the default file decoder.
var DefaultFileDecoder = JsonFileDecoder

// FileDecoder is used to decode the data of the file.
type FileDecoder func(dst any, file File) error

// JsonFileDecoder is a json decoder which supports to remove the comment lines firstly.
func JsonFileDecoder(dst any, file File) (err error) {
	file.Data = comments.RemoveLineComments(file.Data, comments.CommentSlashes)
	if len(file.Data) > 0 {
		err = json.Unmarshal(file.Data, dst)
	}
	return
}

/// ----------------------------------------------------------------------- ///

// DefaultEtagEncoder is the default etag encoder.
var DefaultEtagEncoder = Md5HexEtagEncoder

// EtagEncoder is used to encode etag of the files.
type EtagEncoder func(changed time.Time) string

// Md5HexEtagEncoder encodes the etag from the change time by md5+hex.
func Md5HexEtagEncoder(changed time.Time) string {
	etag := changed.Format(time.RFC3339)
	md5sum := md5.Sum([]byte(etag))
	return hex.EncodeToString(md5sum[:])
}

/// ----------------------------------------------------------------------- ///

// File represents a file.
type File struct {
	Name string
	Root string
	Path string
	Data []byte
}

type info struct {
	modtime time.Time
	size    int64
}

func (i info) Equal(other info) bool {
	return i.size == other.size && i.modtime.Equal(other.modtime)
}

type file struct {
	buf  *bytes.Buffer
	file File

	last info
	now  info
}

// DirLoader is used to load the resources from the files in a directory.
type DirLoader[T any] struct {
	rsc *resource.Resource[map[string]T]

	dir   string
	last  time.Time
	lock  sync.Mutex
	files map[string]*file

	filter  FileFilter
	decoder FileDecoder
	encoder func(changed time.Time) string
	updater func(map[string]T) map[string]T
}

// New returns a new DirLoader with the directory.
func New[T any](dir string) *DirLoader[T] {
	dir, err := filepath.Abs(dir)
	if err != nil {
		panic(err)
	}

	return (&DirLoader[T]{
		dir:   dir,
		rsc:   resource.New[map[string]T](),
		files: make(map[string]*file, 8),
	}).
		SetFileFilter(DefaultFileFilter).
		SetFileDecoder(DefaultFileDecoder).
		SetEtagEncoder(DefaultEtagEncoder)
}

func wrappanic(ctx context.Context) {
	if r := recover(); r != nil {
		slog.ErrorContext(ctx, "wrap a panic", "panic", r)
	}
}

// RootDir returns the root directory.
func (l *DirLoader[T]) RootDir() string {
	return l.dir
}

// SetResourceUpdater resets the updater to fix the loaded resource.
func (l *DirLoader[T]) SetResourceUpdater(updater func(map[string]T) map[string]T) *DirLoader[T] {
	l.lock.Lock()
	l.updater = updater
	l.lock.Unlock()
	return l
}

// SetFileFilter resets the file filter.
func (l *DirLoader[T]) SetFileFilter(filter FileFilter) *DirLoader[T] {
	if filter == nil {
		panic("DirLoader.SetFileFilter: file filter must not be nil")
	}

	l.lock.Lock()
	l.filter = filter
	l.lock.Unlock()
	return l
}

// SetFileDecoder resets the file decoder.
func (l *DirLoader[T]) SetFileDecoder(decoder FileDecoder) *DirLoader[T] {
	if decoder == nil {
		panic("DirLoader.SetFileDecoder: file decoder must not be nil")
	}

	l.lock.Lock()
	l.decoder = decoder
	l.lock.Unlock()
	return l
}

// SetEtagEncoder resets the etag encoder.
func (l *DirLoader[T]) SetEtagEncoder(encoder EtagEncoder) *DirLoader[T] {
	if encoder == nil {
		panic("DirLoader.SetEtagEncoder: etag encoder must not be nil")
	}

	l.lock.Lock()
	l.encoder = encoder
	l.lock.Unlock()
	return l
}

// Sync is used to synchronize the resources to the chan ch periodically.
//
// If cb is nil, never call it when reload the resources.
func (l *DirLoader[T]) Sync(ctx context.Context, rsctype string, interval time.Duration, reload <-chan struct{}, cb func(map[string]T)) {
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

		if lastEtag != "" && etag == lastEtag {
			return
		}

		lastEtag = etag
		if cb != nil {
			cb(resources)
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

func (l *DirLoader[T]) updateEpoch() {
	l.rsc.SetEtag(l.encoder(l.last))
}

// Resource returns the inner resource.
func (l *DirLoader[T]) Resource() *resource.Resource[map[string]T] {
	return l.rsc
}

// Load scans the files in the directory, loads and returns them if changed.
func (l *DirLoader[T]) Load() (resources map[string]T, etag string, err error) {
	l.lock.Lock()
	defer l.lock.Unlock()

	if err = l.scanfiles(); err != nil {
		return
	}

	changed, err := l.checkfiles()
	if err != nil {
		return
	} else if !changed {
		resources, etag = l.rsc.Get()
		return
	}

	resources = make(map[string]T, len(l.files))
	for path, file := range l.files {
		var resource T
		if err = l.decode(&resource, file.file); err != nil {
			err = fmt.Errorf("fail to decode resource file '%s': %w", path, err)
			return
		}
		resources[path] = resource
	}

	if l.updater != nil {
		resources = l.updater(resources)
	}

	l.rsc.SetResource(resources)
	etag = l.rsc.Etag()
	return
}

func (l *DirLoader[T]) decode(dst any, file File) error {
	if len(file.Data) == 0 {
		return nil
	}
	return l.decoder(dst, file)
}

func (l *DirLoader[T]) checkfiles() (changed bool, err error) {
	last := l.last
	for path, file := range l.files {
		if file.last.Equal(file.now) {
			continue
		}

		changed = true
		if err = l._readfile(file.buf, path); err != nil {
			err = fmt.Errorf("fail to read the file '%s': %w", path, err)
			return
		}
		file.file.Data = file.buf.Bytes()

		file.last = file.now
		if file.last.modtime.After(last) {
			last = file.last.modtime
		}
	}

	if changed && !l.last.Equal(last) {
		l.last = last
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

		_file := File{Name: d.Name(), Root: l.dir, Path: path}
		if !l.filter(_file) {
			return nil
		}

		fi, err := d.Info()
		if err != nil {
			return fmt.Errorf("fail to get info of file '%s': %w", path, err)
		}

		f, ok := l.files[path]
		if !ok {
			f = &file{buf: bytes.NewBuffer(make([]byte, 0, fi.Size())), file: _file}
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
