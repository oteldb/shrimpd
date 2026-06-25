//go:build windows

package main

import "os"

func heapDumpSignal() (os.Signal, string, bool) {
	return nil, "", false
}
