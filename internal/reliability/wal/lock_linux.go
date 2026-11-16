//go:build linux

package wal

import (
 "fmt"
 "os"
 "syscall"
)

type fileLock struct { f *os.File }
func acquireLock(dir string) (*fileLock,error) {
 f,err:=os.OpenFile(dir+"/.wal.lock",os.O_CREATE|os.O_RDWR,0600); if err!=nil{return nil,err}
 if err=syscall.Flock(int(f.Fd()),syscall.LOCK_EX|syscall.LOCK_NB); err!=nil { f.Close(); return nil,fmt.Errorf("WAL already locked: %w",err) }
 return &fileLock{f},nil
}
func (l *fileLock) close() error { _=syscall.Flock(int(l.f.Fd()),syscall.LOCK_UN); return l.f.Close() }
