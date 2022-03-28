// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package system

import (
	"time"
)

type SysStat struct {
	Hostname      string    `json:"hostname"`
	ContainerName string    `json:"container_name"`
	Timestamp     time.Time `json:"timestamp"`

	NumCpu       int    `json:"num_cpu"`
	NumGoroutine int    `json:"num_goroutine"`
	NumThreads   uint64 `json:"num_threads"`
	TotalMem     uint64 `json:"total_mem"`
	TotalSwap    uint64 `json:"total_swap"`

	VmPageFaults uint64 `json:"vm_page_faults"`
	VmPeak       uint64 `json:"vm_peak"` // Peak virtual memory size.
	VmSize       uint64 `json:"vm_size"` // Virtual memory size.
	VmSwap       uint64 `json:"vm_swap"` // Swapped-out virtual memory size
	VmRss        uint64 `json:"vm_rss"`  // Resident set size. (mapped + anon + shm)
	VmMapped     uint64 `json:"vm_map"`  // Size of resident file mappings.
	VmAnon       uint64 `json:"vm_anon"` // Size of resident anonymous memory.
	VmShm        uint64 `json:"vm_shm"`  // Size of resident shared memory.

	MemMallocs    uint64 `json:"mem_mallocs"`
	MemFrees      uint64 `json:"mem_frees"`
	MemHeapAlloc  uint64 `json:"mem_heap"`
	MemStackInuse uint64 `json:"mem_stack"`

	DiskSize uint64 `json:"disk_size"`
	DiskUsed uint64 `json:"disk_used"`
	DiskFree uint64 `json:"disk_free"`

	CpuUser  float64 `json:"cpu_user"`
	CpuSys   float64 `json:"cpu_system"`
	CpuTotal float64 `json:"cpu_total"`
}
