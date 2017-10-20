///////////////////////////////////////////////////////////////////////////////

package jobs

///////////////////////////////////////////////////////////////////////////////

import (
	"testing"
)

///////////////////////////////////////////////////////////////////////////////

func TestParseRangeString(t *testing.T) {
	for _, tc := range []struct {
		input  string
		exp    []int
		experr bool
	}{
		// Base case
		{"", []int{}, false},

		// Good scalar cases
		{"42", []int{42}, false},
		{"-42", []int{-42}, false},

		// Bad scalars
		{"--42", []int{}, true},

		// Good ranges
		{"10-12", []int{10, 11, 12}, false},

		// Good ranges with scalar
		{"10-12,13", []int{10, 11, 12, 13}, false},

		// Multiple scalars
		{"10,12,14", []int{10, 12, 14}, false},
	} {
		act, err := ParseRangeString(tc.input)
		if err != nil && tc.experr == false {
			t.Errorf("test case with input (%s) error: %s", tc.input, err.Error())
		} else if err == nil && tc.experr == true {
			t.Errorf("test case with input (%s) expected error, got nil", tc.input)
		} else {
			if len(act) != len(tc.exp) {
				t.Errorf("test case with input (%s) range length mismatch. Actual %d Expected %d", tc.input, len(act), len(tc.exp))
			}
			for i, ev := range tc.exp {
				if ev != act[i] {
					t.Errorf("test case with input (%s) item mismatch at index %d. Actual %d Expected %d", tc.input, i, act[i], ev)
				}
			}
		}
	}
}

///////////////////////////////////////////////////////////////////////////////
