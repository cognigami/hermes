package contracts

// SupportsCapabilities recognizes only facts qualified by the current owned-leaf
// executor. Observed hard-memory support alone does not approve its use.
func SupportsCapabilities(required []string, goos, goarch, hardTreeMemory string, allocation Allocation) bool {
	for _, capability := range required {
		supported := capability == "os:"+goos || capability == "arch:"+goarch || capability == "leaf_exit" || capability == "hard_tree_memory" && hardTreeMemory == "supported" && allocation.RequiresHardTreeMemory
		if !supported {
			return false
		}
	}
	return true
}
