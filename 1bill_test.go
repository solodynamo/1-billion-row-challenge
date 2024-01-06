package main

import "testing"

func BenchmarkRun(b *testing.B) {
	inputFile := "temperature_records.txt"
	bufferSize := 128 // Assuming this is the size of the buffer you want to test with.

	// The actual benchmark loop starts here
	for i := 0; i < b.N; i++ {
		processFileAndGenerateReport(inputFile, bufferSize)
	}
}

func TestRun(b *testing.T) {
	inputFile := "temperature_records.txt"
	bufferSize := 128 // Assuming this is the size of the buffer you want to test with.

	processFileAndGenerateReport(inputFile, bufferSize)
}
