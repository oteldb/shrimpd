//go:build !windows

package main

import (
	"os"
	"syscall"
)

func heapDumpSignal() (os.Signal, string, bool) {
	return syscall.SIGUSR1, "SIGUSR1", true
}
