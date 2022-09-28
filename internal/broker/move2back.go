package broker

// Move2back moves item v to the end of the slice ss.
// For example, given slice [a, b, c, d, e, f] and item c,
// the result is [a, b, d, e, f, c].
// The running time is linear in the worst case.
func Move2back(ss []string, v string) {
	n := len(ss)
	if n <= 1 {
		return
	}
	// Nothing to do when an item is already at the end of the slice.
	if ss[n-1] == v {
		return
	}

	var found bool
	i := 0
	for ; i < n; i++ {
		if ss[i] == v {
			found = true
			break
		}
	}
	if !found {
		return
	}

	// Swap the found item with the last item in the slice,
	// and then swap the neighbors starting from the found index i till the n-2:
	// the last item is already in its place,
	// and the one before it shouldn't be swapped with the last item.
	ss[i], ss[n-1] = ss[n-1], ss[i]
	for ; i < n-2; i++ {
		ss[i], ss[i+1] = ss[i+1], ss[i]
	}
}
