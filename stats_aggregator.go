package main

// statsAggregator stores the intermediate state for aggregates
type statsAggregator struct {
	max   []float32
	min   []float32
	sum   []float64
	count []uint32
	// a slice of `bool` indicating whether a particular slot in the statsAggregator is in use
	isInit []bool
}

func newStatsAggregator() *statsAggregator {
	return &statsAggregator{
		max:    make([]float32, 32),
		min:    make([]float32, 32),
		sum:    make([]float64, 32),
		count:  make([]uint32, 32),
		isInit: make([]bool, 32),
	}
}

// EnsureCapacity checks that there is enough space in the statsAggregator the requested
// slot and reallocs the aggregate storage slices if not.
func (a *statsAggregator) EnsureCapacity(slot uint32) {
	if len(a.isInit)-1 > int(slot) {
		return
	}

	newMax := make([]float32, len(a.isInit)*2)
	copy(newMax, a.max)
	a.max = newMax

	newMin := make([]float32, len(a.isInit)*2)
	copy(newMin, a.min)
	a.min = newMin

	newSum := make([]float64, len(a.isInit)*2)
	copy(newSum, a.sum)
	a.sum = newSum

	newCount := make([]uint32, len(a.isInit)*2)
	copy(newCount, a.count)
	a.count = newCount

	newOccupied := make([]bool, len(a.isInit)*2)
	copy(newOccupied, a.isInit)
	a.isInit = newOccupied
}
