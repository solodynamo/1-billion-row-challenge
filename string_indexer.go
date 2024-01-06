package main

// stringIndexer maps string keys to uint32 "slots", which are unique integers
// allocated from a counter. They are called slots because they map to space in
// the statsAggregator below.
/*
map weather station names, which are strings, to unique integer "slots".
These slots then serve as indices for storing and retrieving aggregated data within the `statsAggregator`
*/

// Allocate unique integers to strings.
// If already seen string, return previous associated integer

type stringIndexer struct {
	// `uint32` counter that keeps track of the next available integer to allocate
	next    uint32
	storage map[string]uint32
}

func newStringIndexer() *stringIndexer {
	return &stringIndexer{
		storage: map[string]uint32{},
	}
}

func (k *stringIndexer) alloc(s string) uint32 {
	slot, ok := k.storage[s]
	if !ok {
		// effectively makes a copy of the original string `s`
		// `[]byte(nil)`: This creates an empty byte slice.
		// `s...`: This syntax is called "unpacking" or "spreading" the string `s` into a slice of bytes
		// `append([]byte(nil), s...)`: This appends the bytes of string `s` to the empty byte slice, creating a new slice of bytes that contains a copy of the bytes that were in string `s`.
		// Since `append` always returns a new slice when the capacity of the original slice is exceeded (which is always the case with an empty starting slice),
		// this operation ensures that the resulting slice is a separate copy, and not just a reference to the same underlying array that holds `s`

		/* Why we need to do all this?
		original string `s` is that strings in Go are immutable but can share underlying byte arrays with other strings if they are created through slicing operations.
		This step ensures that the key used in the stringIndexer's `storage` map is not affected by any external changes to `s` after it has been stored in the `storage`
		*/
		safeKey := string(append([]byte(nil), s...))
		k.storage[safeKey] = k.next
		slot = k.next
		k.next++
	}

	return slot
}
