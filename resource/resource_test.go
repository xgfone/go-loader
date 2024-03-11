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

package resource

import "testing"

func TestResource(t *testing.T) {
	m := New[int]()

	m.Set(1, "1")
	if rsc, etag := m.Get(); rsc != 1 {
		t.Errorf("expect resource %v, but got %v", 1, rsc)
	} else if etag != "1" {
		t.Errorf("expect etag '%s', but got '%s'", "1", etag)
	}

	m.SetEtag("2")
	if etag := m.Etag(); etag != "2" {
		t.Errorf("expect etag '%s', but got '%s'", "2", etag)
	}

	m.SetResource(2)
	if rsc := m.Resource(); rsc != 2 {
		t.Errorf("expect resource %v, but got %v", 2, rsc)
	}
}
