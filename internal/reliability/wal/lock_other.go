//go:build !linux

package wal

import ("errors")
type fileLock struct{}
func acquireLock(string)(*fileLock,error){return nil,errors.New("WAL file locking is currently supported only on Linux")}
func (l *fileLock) close()error{return nil}
