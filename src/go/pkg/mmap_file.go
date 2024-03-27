package pkg

import (
	"fmt"
	"os"
	"syscall"
	"unsafe"
)

func MMapFile(name string) ([]byte, int64, error) {
	file, err := os.Open(name)
	if err != nil {
		return nil, 0, fmt.Errorf("open '%s': %w", name, err)
	}
	defer file.Close()

	fi, err := file.Stat()
	if err != nil {
		return nil, 0, fmt.Errorf("stat '%s': %w", name, err)
	}

	size := fi.Size()
	low, high := uint32(size), uint32(size>>32)
	fMap, err := syscall.CreateFileMapping(syscall.Handle(file.Fd()), nil, syscall.PAGE_READONLY, high, low, nil)
	if err != nil {
		return nil, 0, fmt.Errorf("syscall CreateFileMapping '%s': %w", name, err)
	}
	defer syscall.CloseHandle(fMap)

	ptr, err := syscall.MapViewOfFile(fMap, syscall.FILE_MAP_READ, 0, 0, uintptr(size))
	if err != nil {
		return nil, 0, fmt.Errorf("syscall MapViewOfFile '%s': %w", name, err)
	}

	data := unsafe.Slice((*byte)(unsafe.Pointer(ptr)), size)
	return data, size, nil
}
