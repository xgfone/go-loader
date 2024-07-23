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
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func checkerr(t *testing.T, err error) {
	if err != nil {
		t.Fatal(err)
	}
}

func TestDirLoader(t *testing.T) {
	root, err := filepath.Abs("testdir")
	if err != nil {
		t.Fatal(err)
	}

	ignoredir := filepath.Join(root, "_ignoredir")
	subdir1 := filepath.Join(root, "dir1")
	subdir2 := filepath.Join(root, "dir2")

	_ = os.MkdirAll(ignoredir, 0777)
	_ = os.MkdirAll(subdir1, 0777)
	_ = os.MkdirAll(subdir2, 0777)

	defer func() {
		_ = os.RemoveAll(root)
	}()

	file1 := filepath.Join(ignoredir, "file1.json")
	file2 := filepath.Join(subdir1, "_file2.json")
	file3 := filepath.Join(subdir1, "file3.json")
	file4 := filepath.Join(subdir2, "file4.json")
	file5 := filepath.Join(subdir2, "file5.json")

	checkerr(t, os.WriteFile(file1, []byte(`[{"Name": "111", "Age": 111}]`), 0600))
	checkerr(t, os.WriteFile(file2, []byte(`[{"Name": "222", "Age": 222}]`), 0600))
	checkerr(t, os.WriteFile(file3, []byte(`[{"Name": "333", "Age": 333}]`), 0600))
	checkerr(t, os.WriteFile(file4, []byte(`[{"Name": "444", "Age": 444}]`), 0600))
	checkerr(t, os.WriteFile(file5, []byte(`[{"Name": "555", "Age": 555}]`), 0600))

	type Person struct {
		Name string
		Age  int
	}

	expects := []Person{
		{Name: "333", Age: 333},
		{Name: "444", Age: 444},
		{Name: "555", Age: 555},
	}

	loader := New(root).SetFileHandler(DecodeSliceFileHandler[Person])
	if _, err = loader.Load(); err != nil {
		t.Fatal(err)
	}

	persons := loader.Resource().Resource().([]Person)
	if !reflect.DeepEqual(expects, persons) {
		t.Errorf("expect persons %+v, but got %+v", expects, persons)
	}
}
