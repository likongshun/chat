package wal

import (
 "encoding/json"
 "errors"
 "os"
 "path/filepath"
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


func TestCheckpointCannotMoveBackwards(t *testing.T) {
 dir:=t.TempDir()
 w,err:=Open(Options{Dir:dir,MaxBytes:1<<20,ReserveBytes:4096,Sync:true})
 if err!=nil{t.Fatal(err)}
 defer w.Close()
 r,err:=w.Append("evt-1",json.RawMessage(`{"v":1}`));if err!=nil{t.Fatal(err)}
 for _,sink:=range []string{"mqtt","influx"}{if err=w.Ack(Ack{Seq:r.Seq,Sink:sink,EventID:r.EventID});err!=nil{t.Fatal(err)}}
 if err=w.Checkpoint(r.Seq);err!=nil{t.Fatal(err)}
 if err=w.Checkpoint(0);err==nil{t.Fatal("expected backward checkpoint to be rejected")}
}

func TestReclaimFailsWhenManifestRangeSegmentMissing(t *testing.T) {
 dir:=t.TempDir()
 w,err:=Open(Options{Dir:dir,SegmentBytes:1,MaxBytes:1<<20,ReserveBytes:4096,Sync:true})
 if err!=nil{t.Fatal(err)}
 r,err:=w.Append("evt-1",json.RawMessage(`{"v":1}`));if err!=nil{t.Fatal(err)}
 for _,sink:=range []string{"mqtt","influx"}{if err=w.Ack(Ack{Seq:r.Seq,Sink:sink,EventID:r.EventID});err!=nil{t.Fatal(err)}}
 if err=w.Checkpoint(r.Seq);err!=nil{t.Fatal(err)}
 if err=os.Remove(filepath.Join(dir,"data-00000000000000000001.wal"));err!=nil{t.Fatal(err)}
 if err=w.Reclaim();err==nil{t.Fatal("expected missing required segment to fail closed")}
 _=w.Close()
}

func TestOpenRejectsMissingCommittedCheckpoint(t *testing.T) {
 dir:=t.TempDir()
 w,err:=Open(Options{Dir:dir,MaxBytes:1<<20,ReserveBytes:4096,Sync:true})
 if err!=nil{t.Fatal(err)}
 r,err:=w.Append("evt-1",json.RawMessage(`{"v":1}`));if err!=nil{t.Fatal(err)}
 for _,sink:=range []string{"mqtt","influx"}{if err=w.Ack(Ack{Seq:r.Seq,Sink:sink,EventID:r.EventID});err!=nil{t.Fatal(err)}}
 if err=w.Checkpoint(r.Seq);err!=nil{t.Fatal(err)}
 if err=w.Close();err!=nil{t.Fatal(err)}
 if err=os.Remove(filepath.Join(dir,"checkpoint.json"));err!=nil{t.Fatal(err)}
 if _,err=Open(Options{Dir:dir,MaxBytes:1<<20,ReserveBytes:4096});err==nil{t.Fatal("expected missing committed checkpoint to fail recovery")}
}

func TestRecoveryRejectsInvalidACKJSON(t *testing.T) {
 dir:=t.TempDir()
 w,err:=Open(Options{Dir:dir,MaxBytes:1<<20,ReserveBytes:4096,Sync:true})
 if err!=nil{t.Fatal(err)}
 r,err:=w.Append("evt-1",json.RawMessage(`{"v":1}`));if err!=nil{t.Fatal(err)}
 _=r
 if err=w.Close();err!=nil{t.Fatal(err)}
 f,err:=os.OpenFile(filepath.Join(dir,"ack.wal"),os.O_APPEND|os.O_WRONLY,0600);if err!=nil{t.Fatal(err)}
 if err=writeFrame(f,[]byte("{invalid json"));err!=nil{t.Fatal(err)}
 if err=f.Sync();err!=nil{t.Fatal(err)}
 _=f.Close()
 w,err=Open(Options{Dir:dir,MaxBytes:1<<20,ReserveBytes:4096})
 if err!=nil{t.Fatal(err)}
 defer w.Close()
 if _,err=w.Pending();err==nil{t.Fatal("expected malformed ACK JSON to be rejected")}
}
