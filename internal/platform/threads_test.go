package platform

import (
	"math"
	"strconv"
	"strings"
	"testing"
)

func TestNativeThreadEnvironment(t *testing.T) {
	for _, tc := range []struct {
		cpu     int64
		threads string
	}{{1, "1"}, {999, "1"}, {1000, "1"}, {1001, "2"}, {4000, "4"}, {math.MaxInt64, "9223372036854776"}} {
		t.Run(strconv.FormatInt(tc.cpu, 10), func(t *testing.T) {
			env, err := NativeThreadEnvironment(tc.cpu)
			if err != nil {
				t.Fatal(err)
			}
			wanted := map[string]bool{"GOMAXPROCS": true, "OMP_NUM_THREADS": true, "OPENBLAS_NUM_THREADS": true, "MKL_NUM_THREADS": true, "VECLIB_MAXIMUM_THREADS": true, "NUMEXPR_NUM_THREADS": true, "BLIS_NUM_THREADS": true}
			for _, entry := range env {
				name, value, ok := strings.Cut(entry, "=")
				if !ok || !wanted[name] || value != tc.threads {
					t.Fatalf("unexpected hint %q for CPU %d", entry, tc.cpu)
				}
				delete(wanted, name)
			}
			if len(wanted) != 0 {
				t.Fatal("missing thread hints", wanted)
			}
		})
	}
}

func TestNativeThreadEnvironmentRejectsUnknownCPU(t *testing.T) {
	for _, cpu := range []int64{0, -1, math.MinInt64} {
		if env, err := NativeThreadEnvironment(cpu); err == nil || len(env) != 0 {
			t.Fatalf("CPU %d invented hints: %v %v", cpu, env, err)
		}
	}
}
