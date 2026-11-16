package wal

import (
 "encoding/json"
 "errors"
 "testing"
)
func TestAppendAckCheckpointReopen(t *testing.T){
 d:=t.TempDir();w,e:=Open(Options{Dir:d,SegmentBytes:1024,MaxBytes:1<<20,ReserveBytes:4096,Sync:true});if e!=nil{t.Fatal(e)}
 r,e:=w.Append("e1",json.RawMessage(`{"v":1}`));if e!=nil{t.Fatal(e)}
 if e=w.Ack(Ack{Seq:r.Seq,Sink:"mqtt",EventID:"e1"});e!=nil{t.Fatal(e)}
 if e=w.Ack(Ack{Seq:r.Seq,Sink:"influx",EventID:"e1"});e!=nil{t.Fatal(e)}
 if e=w.Checkpoint(r.Seq);e!=nil{t.Fatal(e)}
 if e=w.Close();e!=nil{t.Fatal(e)}
 w,e=Open(Options{Dir:d,MaxBytes:1<<20,ReserveBytes:4096});if e!=nil{t.Fatal(e)};defer w.Close()
 p,e:=w.Pending();if e!=nil{t.Fatal(e)};if len(p)!=0{t.Fatalf("pending=%d",len(p))}
}
func TestCapacity(t *testing.T){
 w,e:=Open(Options{Dir:t.TempDir(),MaxBytes:1024,ReserveBytes:900});if e!=nil{t.Fatal(e)};defer w.Close()
 _,e=w.Append("x",json.RawMessage(`{"large":"payload"}`));if !errors.Is(e,ErrCapacity){t.Fatalf("want capacity error, got %v",e)}
}
func TestExclusiveLock(t *testing.T){
 d:=t.TempDir();w,e:=Open(Options{Dir:d});if e!=nil{t.Fatal(e)}
 if _,e=Open(Options{Dir:d});e==nil{t.Fatal("expected lock failure")}
 _=w.Close()
}
