package wal

import (
 "encoding/json"
 "errors"
 "os"
 "path/filepath"
 "testing"
 "time"
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
 if err=os.Remove(checkpointPath(dir,r.Seq));err!=nil{t.Fatal(err)}
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


func TestUncommittedCheckpointDoesNotReplaceCommittedGeneration(t *testing.T) {
 dir:=t.TempDir()
 w,err:=Open(Options{Dir:dir,MaxBytes:1<<20,ReserveBytes:4096,Sync:true})
 if err!=nil{t.Fatal(err)}
 r1,err:=w.Append("evt-1",json.RawMessage(`{"v":1}`));if err!=nil{t.Fatal(err)}
 for _,sink:=range []string{"mqtt","influx"}{if err=w.Ack(Ack{Seq:r1.Seq,Sink:sink,EventID:r1.EventID});err!=nil{t.Fatal(err)}}
 if err=w.Checkpoint(r1.Seq);err!=nil{t.Fatal(err)}
 oldManifest,err:=os.ReadFile(filepath.Join(dir,"MANIFEST"));if err!=nil{t.Fatal(err)}
 r2,err:=w.Append("evt-2",json.RawMessage(`{"v":2}`));if err!=nil{t.Fatal(err)}
 for _,sink:=range []string{"mqtt","influx"}{if err=w.Ack(Ack{Seq:r2.Seq,Sink:sink,EventID:r2.EventID});err!=nil{t.Fatal(err)}}
 orphan,e:=json.Marshal(Checkpoint{Seq:r2.Seq,CreatedAt:time.Now().UTC()});if e!=nil{t.Fatal(e)}
 if err=atomicFile(checkpointPath(dir,r2.Seq),orphan);err!=nil{t.Fatal(err)}
 if err=w.Close();err!=nil{t.Fatal(err)}
 // Simulate a crash after writing the new checkpoint but before committing MANIFEST.
 if err=os.WriteFile(filepath.Join(dir,"MANIFEST"),oldManifest,0600);err!=nil{t.Fatal(err)}
 w,err=Open(Options{Dir:dir,MaxBytes:1<<20,ReserveBytes:4096})
 if err!=nil{t.Fatalf("old committed checkpoint must remain recoverable: %v",err)}
 defer w.Close()
 pending,err:=w.Pending();if err!=nil{t.Fatal(err)}
 if len(pending)!=0{t.Fatalf("pending=%d, want 0",len(pending))}
}

func TestRecoveryCleansSegmentsLeftAfterCommittedReclaim(t *testing.T) {
 dir:=t.TempDir()
 w,err:=Open(Options{Dir:dir,SegmentBytes:1,MaxBytes:1<<20,ReserveBytes:4096,Sync:true})
 if err!=nil{t.Fatal(err)}
 r,err:=w.Append("evt-1",json.RawMessage(`{"v":1}`));if err!=nil{t.Fatal(err)}
 for _,sink:=range []string{"mqtt","influx"}{if err=w.Ack(Ack{Seq:r.Seq,Sink:sink,EventID:r.EventID});err!=nil{t.Fatal(err)}}
 if err=w.Checkpoint(r.Seq);err!=nil{t.Fatal(err)}
 oldPath:=filepath.Join(dir,"data-00000000000000000001.wal")
 oldBytes,err:=os.ReadFile(oldPath);if err!=nil{t.Fatal(err)}
 if err=w.Reclaim();err!=nil{t.Fatal(err)}
 if err=os.WriteFile(oldPath,oldBytes,0600);err!=nil{t.Fatal(err)}
 if err=w.Close();err!=nil{t.Fatal(err)}
 w,err=Open(Options{Dir:dir,MaxBytes:1<<20,ReserveBytes:4096})
 if err!=nil{t.Fatal(err)}
 defer w.Close()
 if _,err=os.Stat(oldPath);!os.IsNotExist(err){t.Fatalf("obsolete segment was not cleaned on recovery: %v",err)}
}


func TestSequenceContinuesAfterAllCoveredSegmentsReclaimed(t *testing.T) {
 dir:=t.TempDir()
 w,err:=Open(Options{Dir:dir,SegmentBytes:1,MaxBytes:1<<20,ReserveBytes:4096,Sync:true})
 if err!=nil{t.Fatal(err)}
 first,err:=w.Append("evt-1",json.RawMessage(`{"v":1}`));if err!=nil{t.Fatal(err)}
 for _,sink:=range []string{"mqtt","influx"}{if err=w.Ack(Ack{Seq:first.Seq,Sink:sink,EventID:first.EventID});err!=nil{t.Fatal(err)}}
 if err=w.Checkpoint(first.Seq);err!=nil{t.Fatal(err)}
 if err=w.Reclaim();err!=nil{t.Fatal(err)}
 if err=w.Close();err!=nil{t.Fatal(err)}
 w,err=Open(Options{Dir:dir,MaxBytes:1<<20,ReserveBytes:4096,Sync:true})
 if err!=nil{t.Fatal(err)}
 defer w.Close()
 second,err:=w.Append("evt-2",json.RawMessage(`{"v":2}`));if err!=nil{t.Fatal(err)}
 if second.Seq<=first.Seq{t.Fatalf("sequence reused after reclaim: first=%d second=%d",first.Seq,second.Seq)}
}


func TestOpenRepairsTornActiveDataAndACKTailsBeforeRecovery(t *testing.T) {
 dir:=t.TempDir()
 w,err:=Open(Options{Dir:dir,MaxBytes:1<<20,ReserveBytes:4096,Sync:true})
 if err!=nil{t.Fatal(err)}
 r,err:=w.Append("evt-1",json.RawMessage(`{"v":1}`));if err!=nil{t.Fatal(err)}
 if err=w.Ack(Ack{Seq:r.Seq,Sink:"mqtt",EventID:r.EventID});err!=nil{t.Fatal(err)}
 if err=w.Close();err!=nil{t.Fatal(err)}
 dataPath:=filepath.Join(dir,"data-00000000000000000001.wal")
 for _,path:=range []string{dataPath,filepath.Join(dir,"ack.wal")}{
  f,err:=os.OpenFile(path,os.O_APPEND|os.O_WRONLY,0600);if err!=nil{t.Fatal(err)}
  if _,err=f.Write([]byte{0,0,0});err!=nil{t.Fatal(err)}
  if err=f.Sync();err!=nil{t.Fatal(err)}
  _=f.Close()
 }
 w,err=Open(Options{Dir:dir,MaxBytes:1<<20,ReserveBytes:4096,Sync:true})
 if err!=nil{t.Fatalf("active torn tails should be repaired before recovery: %v",err)}
 defer w.Close()
 pending,err:=w.Pending();if err!=nil{t.Fatal(err)}
 if len(pending)!=1||pending[0].Seq!=r.Seq{t.Fatalf("pending after recovery=%v, want original record",pending)}
}


func TestCheckpointCompactsOnlyACKsAtOrBelowBoundary(t *testing.T) {
 dir:=t.TempDir()
 w,err:=Open(Options{Dir:dir,MaxBytes:1<<20,ReserveBytes:4096,Sync:true})
 if err!=nil{t.Fatal(err)}
 first,err:=w.Append("evt-1",json.RawMessage(`{"v":1}`));if err!=nil{t.Fatal(err)}
 second,err:=w.Append("evt-2",json.RawMessage(`{"v":2}`));if err!=nil{t.Fatal(err)}
 for _,sink:=range []string{"mqtt","influx"}{if err=w.Ack(Ack{Seq:first.Seq,Sink:sink,EventID:first.EventID});err!=nil{t.Fatal(err)}}
 if err=w.Ack(Ack{Seq:second.Seq,Sink:"mqtt",EventID:second.EventID});err!=nil{t.Fatal(err)}
 if err=w.Checkpoint(first.Seq);err!=nil{t.Fatal(err)}
 count:=0
 if err=readFrames(filepath.Join(dir,"ack.wal"),func(b []byte)error{var a Ack;if err:=json.Unmarshal(b,&a);err!=nil{return err};if a.Seq<=first.Seq{t.Fatalf("checkpointed ACK seq %d was not compacted",a.Seq)};count++;return nil});err!=nil{t.Fatal(err)}
 if count!=1{t.Fatalf("retained ACK count=%d, want 1",count)}
 pending,err:=w.Pending();if err!=nil{t.Fatal(err)}
 if len(pending)!=1||pending[0].Seq!=second.Seq{t.Fatalf("pending=%v, want second record only",pending)}
 if err=w.Close();err!=nil{t.Fatal(err)}
}
