package table

import (
	"os"
	"syscall"
)

// tableLock 是表级跨进程互斥锁的接口（进程内由 Table.mu 串行化，
// 跨进程由文件锁保证同一时刻只有一个提交生效）。
type tableLock interface {
	lock() error
	unlock() error
}

// fileLock 基于锁文件 + flock(2) 的跨进程互斥锁。
// flock 在 Linux/macOS 上对进程互斥（同进程内由 Table.mu 保证不重入冲突）。
type fileLock struct {
	path string
	f    *os.File
}

// newTableLock 创建/打开表级锁文件（打开不持有锁，实际加锁在 lock 中）。
func newTableLock(lockPath string) (tableLock, error) {
	f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	return &fileLock{path: lockPath, f: f}, nil
}

// lock 阻塞获取排他文件锁；获取成功期间其他进程的 lock 会被阻塞，
// 因此多实例/多进程并发提交在文件锁之后串行化，配合重读 HEAD 的 CAS
// 判定，恰好一个提交生效、其余明确收到 ErrCommitConflict。
// 锁可重复获取：unlock 关闭 fd 后再次 lock 会重新打开锁文件。
func (l *fileLock) lock() error {
	if l.f == nil {
		f, err := os.OpenFile(l.path, os.O_CREATE|os.O_RDWR, 0o644)
		if err != nil {
			return err
		}
		l.f = f
	}
	return syscall.Flock(int(l.f.Fd()), syscall.LOCK_EX)
}

// unlock 释放排他锁并关闭锁文件。
func (l *fileLock) unlock() error {
	if l.f == nil {
		return nil
	}
	err := syscall.Flock(int(l.f.Fd()), syscall.LOCK_UN)
	if cerr := l.f.Close(); cerr != nil && err == nil {
		err = cerr
	}
	l.f = nil
	return err
}
