package platform

import (
	"errors"
	"strconv"
)

// NativeThreadEnvironment projects an approved CPU reservation into Go and
// common numerical-library thread hints. Callers must first admit the allocation
// against actual host capacity and explicitly choose the rest of the environment.
// These cooperative settings are not an OS CPU quota or descendant containment.
func NativeThreadEnvironment(cpuMilli int64) ([]string, error) {
	if cpuMilli <= 0 {
		return nil, errors.New("native thread hints require a positive CPU allocation")
	}
	cores := cpuMilli / 1000
	if cpuMilli%1000 != 0 {
		cores++
	}
	count := strconv.FormatInt(cores, 10)
	names := []string{"GOMAXPROCS", "OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS", "VECLIB_MAXIMUM_THREADS", "NUMEXPR_NUM_THREADS", "BLIS_NUM_THREADS"}
	env := make([]string, 0, len(names))
	for _, name := range names {
		env = append(env, name+"="+count)
	}
	return env, nil
}
