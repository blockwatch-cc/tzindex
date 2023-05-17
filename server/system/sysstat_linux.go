// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package system

import (
	"context"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"sync/atomic"
	"syscall"
	"time"

	"blockwatch.cc/packdb/util"
	"github.com/echa/config"
	"github.com/echa/goprocinfo/linux"
)

// all sizes in bytes
// https://man7.org/linux/man-pages/man5/proc.5.html
func GetSysStat(ctx context.Context) (SysStat, error) {
	s := SysStat{
		Timestamp: time.Now().UTC(),
	}
	host, _ := os.Hostname()
	s.Hostname = host
	if n := os.Getenv("HOST_HOSTNAME"); n != "" {
		s.ContainerName = host
		u, _ := url.Parse(n)
		s.Hostname = u.Hostname()
	}
	s.NumCpu = runtime.NumCPU()
	s.NumGoroutine = runtime.NumGoroutine()

	// mem + sys
	memStats := &runtime.MemStats{}
	runtime.ReadMemStats(memStats)
	s.MemMallocs = memStats.Mallocs
	s.MemFrees = memStats.Frees
	s.MemHeapAlloc = memStats.HeapAlloc
	s.MemStackInuse = memStats.StackInuse

	// total mem
	var si syscall.Sysinfo_t
	_ = syscall.Sysinfo(&si)
	s.TotalMem = si.Totalram
	s.TotalSwap = si.Totalswap

	// vm stats
	p := filepath.Join("/proc", strconv.Itoa(os.Getpid()))
	pStatus, _ := linux.ReadProcessStatus(filepath.Join(p, "status"))
	s.NumThreads = pStatus.Threads
	s.VmPeak = pStatus.VmPeak * 1024
	s.VmSize = pStatus.VmSize * 1024
	s.VmSwap = pStatus.VmSwap * 1024
	s.VmRss = pStatus.VmRSS * 1024
	s.VmMapped = pStatus.RssFile * 1024
	s.VmAnon = pStatus.RssAnon * 1024
	s.VmShm = pStatus.RssShmem * 1024

	pStat, _ := linux.ReadProcessStat(filepath.Join(p, "stat"))
	s.VmPageFaults = pStat.Majflt

	// cpu used
	s.CpuUser = float64(atomic.LoadInt64(&cpuUser)) / 100.0
	s.CpuSys = float64(atomic.LoadInt64(&cpuSys)) / 100.0
	s.CpuTotal = s.CpuUser + s.CpuSys

	// Disk
	pDisk, _ := linux.ReadDisk(config.GetString("db.path"))
	s.DiskSize = pDisk.All
	s.DiskUsed = pDisk.Used
	s.DiskFree = pDisk.Free

	return s, nil
}

func init() {
	ticker := util.NewWallTicker(5*time.Second, 0)
	go func() {
		defer ticker.Stop()
		for {
			<-ticker.C
			_ = updateCpuUsage()
		}
	}()
}

var (
	lastUsage syscall.Rusage
	lastTime  time.Time
	cpuUser   int64
	cpuSys    int64
)

func updateCpuUsage() error {
	var rusage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &rusage); err != nil {
		return err
	}
	now := time.Now()

	if lastTime.IsZero() {
		lastTime = now
		lastUsage = rusage
		return nil
	}

	ns := int64(now.Sub(lastTime))
	if ns > 0 {
		usr := syscall.TimevalToNsec(rusage.Utime) - syscall.TimevalToNsec(lastUsage.Utime)
		sys := syscall.TimevalToNsec(rusage.Stime) - syscall.TimevalToNsec(lastUsage.Stime)
		atomic.StoreInt64(&cpuUser, usr*10000/ns)
		atomic.StoreInt64(&cpuSys, sys*10000/ns)
	}
	lastTime = now
	lastUsage = rusage
	return nil
}
