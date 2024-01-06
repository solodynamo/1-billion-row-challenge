package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
)

// aggregationTask wraps an input, stringIndexer, and statsAggregator to run our hard-coded query
// over a subset of the input.
type aggregationTask struct {
	alloc *stringIndexer
	accum *statsAggregator
	input io.Reader
}

func newTask(input io.Reader) *aggregationTask {
	return &aggregationTask{
		alloc: newStringIndexer(),
		accum: newStatsAggregator(),
		input: input,
	}
}

func (t *aggregationTask) run() {
	scanner := bufio.NewScanner(t.input)

	for scanner.Scan() {
		var (
			line = scanner.Bytes()
			end  int
		)

		end = bytes.IndexByte(line, ';') + 1

		// triggers compiler to insert bounds check here but then it omits later checks that would be otherwise added
		_ = line[end-1]

		var (
			key  = bytesToString(line[:end-1])
			val  = bytesToString(line[end:])
			slot = t.alloc.alloc(key)
		)

		t.accum.EnsureCapacity(slot)

		v, err := strconv.ParseFloat(val, 32)
		if err != nil {
			panic(err)
		}

		v32 := float32(v)

		t.accum.sum[slot] += v
		t.accum.count[slot]++

		if !t.accum.isInit[slot] {
			t.accum.min[slot] = v32
			t.accum.max[slot] = v32
			t.accum.isInit[slot] = true
			continue
		}

		if v32 < t.accum.min[slot] {
			t.accum.min[slot] = v32
		}

		if v32 > t.accum.max[slot] {
			t.accum.max[slot] = v32
		}
	}

	if scanner.Err() != nil {
		panic(scanner.Err())
	}
}

// merge takes the intermediate results of the other aggregationTask `ot` and merges it
// with the intermediate results of this aggregationTask `t`.
func (t *aggregationTask) merge(ot *aggregationTask) {
	for key, os := range ot.alloc.storage {
		ts := t.alloc.alloc(key)
		t.accum.EnsureCapacity(ts)

		t.accum.count[ts] += ot.accum.count[os]
		t.accum.sum[ts] += ot.accum.sum[os]

		if !t.accum.isInit[ts] {
			t.accum.min[ts] = ot.accum.min[os]
			t.accum.max[ts] = ot.accum.max[os]
			t.accum.isInit[ts] = true
			continue
		}

		if ot.accum.min[os] < t.accum.min[ts] {
			t.accum.min[ts] = ot.accum.min[os]
		}

		if ot.accum.max[os] > t.accum.max[ts] {
			t.accum.max[ts] = ot.accum.max[os]
		}
	}
}

func divideInputIntoTasks(path string, taskCount int) []*aggregationTask {
	// 1. Open the file at the given path.
	f, err := os.Open(path)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		panic(err)
	}

	// 2. Get the file size.
	size := fi.Size()

	// 3. Determine the step size by dividing the file size by
	// the number of tasks.
	step := size / int64(taskCount)

	// 4. Initialize a type `split` to keep track of the
	// starting (begin) and ending (end) byte positions for each aggregationTask
	type split struct {
		begin, end int64
	}

	var splits []split

	var base = int64(0)
	// 5. Loop through the file to determine the byte positions
	// where each aggregationTask should begin and end.
	for {
		targetEnd := base + step

		//  If the target end position plus another step would exceed the file size,
		// the last aggregationTask is created to go to the end of the file.
		if targetEnd+step >= size {
			splits = append(splits, split{begin: int64(base), end: size})
			break
		}

		// 6. seek to the target end position, and use a scanner to find the
		// end of the current line (denoted by `\n`)
		_, err := f.Seek(int64(targetEnd), 0)
		if err != nil {
			panic(err)
		}
		scanner := bufio.NewScanner(bufio.NewReader(f))
		scanner.Split(bufio.ScanBytes)
		// We have to keep scanning from `targetEnd` to the end of the next
		// full record.
		for scanner.Scan() {
			targetEnd++
			if scanner.Bytes()[0] == '\n' {
				break
			}
		}

		// 7. Append the current split to the splits slice.
		splits = append(splits, split{
			begin: base,
			end:   targetEnd,
		})

		// In each iteration of the loop, increment the target ending position by the step size.
		base = targetEnd
	}

	var tasks []*aggregationTask

	// 8. After creating all the splits, loop through them to create the tasks.
	for _, s := range splits {
		// open the file again because we needs an independent file descriptor
		// that will be used exclusively for the aggregationTask that is being created
		// safe when we want to create goroutine(s)
		f, err := os.Open(path)
		if err != nil {
			panic(err)
		}
		// position the read pointer of the file at the beginning of the current split (`s.begin`).
		_, err = f.Seek(s.begin, 0)
		if err != nil {
			panic(err)
		}

		// 9. The `SectionReader` will present a view of the file
		// that's restricted to just the part the aggregationTask is responsible for
		sr := io.NewSectionReader(f, s.begin, s.end-s.begin)
		// buffer size chosen somewhat arbitrarily but it is reasonable
		// `1<<19` equals 524288 bytes or 512KB
		// We wrap the section reader to reduce system calls while reading the section
		br := bufio.NewReaderSize(sr, 1<<19)

		tasks = append(tasks, newTask(br))
	}

	return tasks
}

func mergeAndGenerateResult(tasks []*aggregationTask) string {
	for i, t := range tasks {
		if i == 0 {
			continue
		}

		tasks[0].merge(t)
	}

	t := tasks[0]

	var keys []string
	for k := range t.alloc.storage {
		keys = append(keys, k)
	}

	var lines []string
	for _, key := range keys {
		slot := t.alloc.alloc(key)
		min := t.accum.min[slot]
		avg := float32(t.accum.sum[slot] / float64(t.accum.count[slot]))
		max := t.accum.max[slot]

		lines = append(lines, fmt.Sprintf(
			"%q;%s;%s;%s;%d",
			key,
			formatRemoveTrailingZero(min),
			formatRemoveTrailingZero(max),
			formatRemoveTrailingZero(avg),
			t.accum.count[slot]))
	}

	return strings.Join(lines, "\n")
}

// Pool for bufio.Reader to reduce allocations
var readerPool = sync.Pool{
	New: func() interface{} {
		return bufio.NewReader(nil)
	},
}

func executeTasksConcurrently(tasks []*aggregationTask) {
	var wg sync.WaitGroup
	wg.Add(len(tasks))

	for _, t := range tasks {
		go func(t *aggregationTask) {
			defer wg.Done()
			t.run()
			if br, ok := t.input.(*bufio.Reader); ok { // Return reader to pool after use
				readerPool.Put(br)
			}
		}(t)
	}

	wg.Wait()
}

func processFileAndGenerateReport(path string, taskCount int) string {
	tasks := divideInputIntoTasks(path, taskCount)

	executeTasksConcurrently(tasks)

	result := mergeAndGenerateResult(tasks)

	return result
}

func main() {
	fmt.Println(processFileAndGenerateReport("temperature_records.txt", 1000))
}
