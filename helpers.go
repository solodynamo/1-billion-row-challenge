package main

import (
	"fmt"
	"strings"
	"unsafe"
)

func bytesToString(b []byte) string {
	return unsafe.String(unsafe.SliceData(b), len(b))
}

func formatRemoveTrailingZero(f float32) string {
	s := fmt.Sprintf("%.1f", f)
	if strings.HasSuffix(s, ".0") {
		return s[:len(s)-2]
	}

	return s
}
