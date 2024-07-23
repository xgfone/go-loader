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
	"sort"
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

func alwaysTrueFilter(File) bool { return true }

// OrFilter returns a file filter which allows the files
// only if any of filters returns true.
func OrFilter(filters ...FileFilter) FileFilter {
	for _, filter := range filters {
		if filter == nil {
			panic("OrFilter: filter must not be nil")
		}
	}

	if len(filters) == 0 {
		return alwaysTrueFilter
	}

	return func(file File) bool {
		for _, filter := range filters {
			if filter(file) {
				return true
			}
		}
		return false
	}
}

// AndFilter returns a file filter which allows the files
// only if all the filters returns true.
func AndFilter(filters ...FileFilter) FileFilter {
	for _, filter := range filters {
		if filter == nil {
			panic("AndFilter: filter must not be nil")
		}
	}

	if len(filters) == 0 {
		return alwaysTrueFilter
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
		return alwaysTrueFilter
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
type FileDecoder func(any, File) error

// JsonFileDecoder is a json decoder which supports to remove the comment lines firstly.
func JsonFileDecoder(dst any, file File) (err error) {
	if len(file.Data) == 0 {
		return
	}

	file.Data = comments.RemoveLineComments(file.Data, comments.CommentSlashes)
	if len(file.Data) > 0 {
		err = json.Unmarshal(file.Data, dst)
	}
	return
}

/// ----------------------------------------------------------------------- ///

// FileHandler is used to handle the loaded files.
type FileHandler func([]File) (any, error)

// DefaultFileHandler is the default file handler.
var DefaultFileHandler = noopFileHandler

func noopFileHandler(files []File) (any, error) { return files, nil }

// DecodeSliceFileHandler is a file handler, which decodes the file data
// as a slice and merges them.
func DecodeSliceFileHandler[T any](files []File) (any, error) {
	resources := make([]T, 0, len(files)*2)
	for _, file := range files {
		var vs []T
		if err := JsonFileDecoder(&vs, file); err != nil {
			return files, fmt.Errorf("fail to decode resource file '%s': %w", file.Path, err)
		}
		resources = append(resources, vs...)
	}
	return resources, nil
}

/// ----------------------------------------------------------------------- ///

// DefaultEtagEncoder is the default etag encoder.
var DefaultEtagEncoder = Md5HexEtagEncoder

// EtagEncoder is used to encode etag of the files.
type EtagEncoder func(modtime time.Time) string

// Md5HexEtagEncoder encodes the etag from the change time by md5+hex.
func Md5HexEtagEncoder(modtime time.Time) string {
	etag := modtime.Format(time.RFC3339)
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
type DirLoader struct {
	rsc *resource.Resource[any]

	dir   string
	last  time.Time
	lock  sync.Mutex
	files map[string]*file
	etag  string

	filter  FileFilter
	handler FileHandler
	encoder EtagEncoder
}

// New returns a new DirLoader with the directory.
func New(dir string) *DirLoader {
	dir, err := filepath.Abs(dir)
	if err != nil {
		panic(err)
	}

	return (&DirLoader{
		dir:   dir,
		rsc:   resource.New[any](),
		files: make(map[string]*file, 8),
	}).
		SetFileFilter(DefaultFileFilter).
		SetFileHandler(DefaultFileHandler).
		SetEtagEncoder(DefaultEtagEncoder)
}

func wrappanic(ctx context.Context) {
	if r := recover(); r != nil {
		slog.ErrorContext(ctx, "wrap a panic", "panic", r)
	}
}

// RootDir returns the root directory.
func (l *DirLoader) RootDir() string {
	return l.dir
}

// SetFileFilter resets the file filter.
func (l *DirLoader) SetFileFilter(filter FileFilter) *DirLoader {
	if filter == nil {
		panic("DirLoader.SetFileFilter: file filter must not be nil")
	}

	l.lock.Lock()
	l.filter = filter
	l.lock.Unlock()
	return l
}

// SetFileHandler resets the file handler.
func (l *DirLoader) SetFileHandler(handler FileHandler) *DirLoader {
	if handler == nil {
		panic("DirLoader.SetFileHandler: file handler must not be nil")
	}

	l.lock.Lock()
	l.handler = handler
	l.lock.Unlock()
	return l
}

// SetEtagEncoder resets the etag encoder.
func (l *DirLoader) SetEtagEncoder(encoder EtagEncoder) *DirLoader {
	if encoder == nil {
		panic("DirLoader.SetEtagEncoder: etag encoder must not be nil")
	}

	l.lock.Lock()
	l.encoder = encoder
	l.lock.Unlock()
	return l
}

func (l *DirLoader) updateEpoch()     { l.etag = l.encoder(l.last) }
func (l *DirLoader) getEpoch() string { return l.etag }

// Sync is used to synchronize the resources to the chan ch periodically.
//
// If cb is nil, never call it when reload the resources.
func (l *DirLoader) Sync(ctx context.Context, interval time.Duration, reload <-chan struct{}, cb func(any)) {
	if interval <= 0 {
		interval = time.Minute
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	load := func() {
		defer wrappanic(ctx)
		slog.LogAttrs(ctx, slog.LevelDebug-4, "start to load the resource", slog.String("dir", l.dir))

		changed, err := l.Load()
		if err != nil {
			slog.Error("fail to load the resources from the local files", "dir", l.dir, "err", err)
			return
		}

		if !changed {
			return
		}

		if cb != nil {
			cb(l.Resource().Resource())
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

// Resource returns the inner resource.
func (l *DirLoader) Resource() *resource.Resource[any] { return l.rsc }
func (l *DirLoader) setrsc(rsc any, etag string)       { l.rsc.Set(rsc, etag) }

// Load scans the files in the directory, loads and returns them if changed.
func (l *DirLoader) Load() (changed bool, err error) {
	l.lock.Lock()
	defer l.lock.Unlock()

	if err = l.scanfiles(); err != nil {
		return
	}

	if changed, err = l.checkfiles(); err != nil || !changed {
		return
	}

	files := make([]File, 0, len(l.files))
	for _, file := range l.files {
		files = append(files, file.file)
	}
	sort.Slice(files, func(i, j int) bool {
		return files[i].Path < files[j].Path
	})

	rsc, err := l.handler(files)
	if err != nil {
		return
	}

	l.setrsc(rsc, l.getEpoch())
	return
}

func (l *DirLoader) checkfiles() (changed bool, err error) {
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

func (l *DirLoader) _readfile(buf *bytes.Buffer, path string) (err error) {
	file, err := os.Open(path)
	if err != nil {
		return
	}
	defer file.Close()

	buf.Reset()
	_, err = io.CopyBuffer(buf, file, make([]byte, 1024))
	return
}

func (l *DirLoader) scanfiles() (err error) {
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
