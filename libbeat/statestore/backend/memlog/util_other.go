// +build linux dragonfly freebsd netbsd openbsd solaris

package memlog

import (
	"os"

	"golang.org/x/sys/unix"
)

func syncFile(f *os.File) error {
	// best effort
	for {
		err := f.Sync()
		if err == nil || (err != unix.EINTR && err != unix.EAGAIN) {
			return err
		}
	}
}
