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

package mapx

import (
	"slices"
	"sort"
	"testing"
)

func TestKeys(t *testing.T) {
	expectints := []int{1, 2}
	intmap := map[int]int{1: 11, 2: 22}
	ints := Keys(intmap)
	sort.Ints(ints)
	if !slices.Equal(expectints, ints) {
		t.Errorf("expect %v, but got %v", expectints, ints)
	}

	expectstrs := []string{"a", "b"}
	strmap := map[string]string{"a": "aa", "b": "bb"}
	strs := Keys(strmap)
	sort.Strings(strs)
	if !slices.Equal(expectstrs, strs) {
		t.Errorf("expect %v, but got %v", expectstrs, strs)
	}
}
