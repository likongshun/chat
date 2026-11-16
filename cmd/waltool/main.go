package main

import (
 "flag"
 "fmt"
 "log"
 "github.com/likongshun/chat/internal/reliability/wal"
)
func main(){
 dir:=flag.String("dir","./data/wal","WAL directory");flag.Parse()
 w,e:=wal.Open(wal.Options{Dir:*dir,MaxBytes:10<<30,ReserveBytes:64<<20});if e!=nil{log.Fatal(e)};defer w.Close()
 pending,e:=w.Pending();if e!=nil{log.Fatal(e)}
 fmt.Printf("pending records: %d\n",len(pending))
}
