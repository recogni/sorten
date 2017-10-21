////////////////////////////////////////////////////////////////////////////////

// Package jobs implements generic helpers for use in various specific job
// types as defined in the subdirectories that follow.
package jobs

///////////////////////////////////////////////////////////////////////////////

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
)

///////////////////////////////////////////////////////////////////////////////

// ParseRangeString accepts a string input and returns a list of integers that
// fall within the described range string.  Returns an error if the string is
// malformed.
// A range string may contain any number of comma separated groups, where each
// group may either be a single scalar value, or a range specified in the form
// of `a-b`.  A valid range string would be `32,50-52` which would return the
// list of integers `[32,50,51,52]`.
func ParseRangeString(s string) ([]int, error) {
	ret := []int{}
	for _, group := range strings.Split(s, ",") {
		// If the group is empty, no-op.
		if len(group) == 0 {
			continue
		}

		// Try to parse a single int.
		v, err := strconv.ParseInt(group, 10, 32)
		if err == nil {
			ret = append(ret, int(v))
			continue
		}

		// Not a single int, try to parse a range.
		vv := strings.Split(group, "-")
		if len(vv) != 2 {
			return nil, fmt.Errorf("%s is a malformed group definition", group)
		}
		min, err := strconv.ParseInt(vv[0], 10, 32)
		if err != nil {
			return nil, err
		}
		max, err := strconv.ParseInt(vv[1], 10, 32)
		if err != nil {
			return nil, err
		}

		// Build the specified range.
		for i := min; i <= max; i++ {
			ret = append(ret, int(i))
		}
	}
	return ret, nil
}

///////////////////////////////////////////////////////////////////////////////

var mkdirLock sync.RWMutex

// SafeMkdir guards the access to the directory creation using a simple RW
// mutex.  This is in-case one of the N cores tries to create the same directory
// when anther already has defined the node.
func SafeMkdir(path string) error {
	mkdirLock.Lock()
	defer mkdirLock.Unlock()

	if _, err := os.Stat(path); os.IsNotExist(err) {
		return os.MkdirAll(path, 0755)
	}
	return nil
}

///////////////////////////////////////////////////////////////////////////////

func RangeContainsClass(xs []int, x int) bool {
	if len(xs) == 0 {
		return true
	}
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}

///////////////////////////////////////////////////////////////////////////////
