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

package dirloader

import (
	"encoding/json"
	"fmt"

	"github.com/xgfone/go-loader/internal/comments"
)

/// ----------------------------------------------------------------------- ///

// DefaultFileDecoder is the default file decoder.
var DefaultFileDecoder = JsonFileDecoder

// FileDecoder is used to decode the data of the file.
type FileDecoder func(any, File) error

// JsonFileDecoder is a json decoder which supports to remove the comment lines firstly.
func JsonFileDecoder(dst any, file File) (err error) {
	file.Data = comments.RemoveLineComments(file.Data, comments.CommentSlashes)
	if len(file.Data) > 0 {
		err = json.Unmarshal(file.Data, dst)
	}
	return
}

/// ----------------------------------------------------------------------- ///

type FileExt[T any] struct {
	File
	Obj T
}

type DirLoaderExt[T any] struct {
	*DirLoader

	decoder FileDecoder
}

func NewDirLoaderExt[T any](dir string) *DirLoaderExt[T] {
	loader := new(DirLoaderExt[T]).SetFileDecoder(DefaultFileDecoder)
	loader.DirLoader = New(dir).SetFileHandler(loader.DecodeFiles)
	return loader
}

// SetFileDecoder resets the file decoder.
func (l *DirLoaderExt[T]) SetFileDecoder(decoder FileDecoder) *DirLoaderExt[T] {
	if decoder == nil {
		panic("DirLoaderExt.SetFileDecoder: file decoder must not be nil")
	}

	l.decoder = decoder
	return l
}

// DecodeFiles is used to decode the file data, which is set as the file handler of DirLoader.
func (l *DirLoaderExt[T]) DecodeFiles(files []File) ([]File, error) {
	for i, file := range files {
		var obj T
		if err := l.decode(&obj, file); err != nil {
			err = fmt.Errorf("fail to decode resource file '%s': %w", file.Path, err)
			return files, err
		}
		files[i].Extra = obj
	}
	return files, nil
}

func (l *DirLoaderExt[T]) decode(dst any, file File) error {
	if len(file.Data) == 0 {
		return nil
	}
	return l.decoder(dst, file)
}
