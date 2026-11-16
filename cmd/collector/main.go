package main

import (
 "context"
 "log"
 "os"
 "os/signal"
 "syscall"
 "time"
 "github.com/likongshun/chat/internal/reliability/wal"
)
func main(){
 dir:=os.Getenv("WAL_DIR");if dir==""{dir="./data/wal"}
 w,e:=wal.Open(wal.Options{Dir:dir,SegmentBytes:64<<20,MaxBytes:10<<30,ReserveBytes:64<<20,Sync:true});if e!=nil{log.Fatal(e)};defer w.Close()
 log.Printf("collector core initialized; WAL=%s",dir)
 ctx,stop:=signal.NotifyContext(context.Background(),os.Interrupt,syscall.SIGTERM);defer stop()
 <-ctx.Done();_ = w.Close();time.Sleep(10*time.Millisecond)
}
