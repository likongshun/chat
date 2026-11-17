package wal

import (
 "encoding/json"
 "fmt"
 "errors"
 "os"
 "os/exec"
 "path/filepath"
 "strings"
 "sync"
 "syscall"
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
 f,err:=os.OpenFile(ackSegmentPath(dir,w.manifest.AckActiveSegment),os.O_APPEND|os.O_WRONLY,0600);if err!=nil{t.Fatal(err)}
 if err=writeFrame(f,[]byte("{invalid json"));err!=nil{t.Fatal(err)}
 if err=f.Sync();err!=nil{t.Fatal(err)}
 _=f.Close()
 if _,err=Open(Options{Dir:dir,MaxBytes:1<<20,ReserveBytes:4096});err==nil{t.Fatal("expected recovery to reject malformed ACK JSON")}
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
 for _,path:=range []string{dataPath,ackSegmentPath(dir,w.manifest.AckActiveSegment)}{
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
 if err=readFrames(ackSegmentPath(dir,w.manifest.AckActiveSegment),func(b []byte)error{var a Ack;if err:=json.Unmarshal(b,&a);err!=nil{return err};if a.Seq<=first.Seq{t.Fatalf("checkpointed ACK seq %d was not compacted",a.Seq)};count++;return nil});err!=nil{t.Fatal(err)}
 if count!=1{t.Fatalf("retained ACK count=%d, want 1",count)}
 pending,err:=w.Pending();if err!=nil{t.Fatal(err)}
 if len(pending)!=1||pending[0].Seq!=second.Seq{t.Fatalf("pending=%v, want second record only",pending)}
 if err=w.Close();err!=nil{t.Fatal(err)}
}


func TestPendingWithAcksReportsIndependentSinkState(t *testing.T) {
 dir:=t.TempDir()
 w,err:=Open(Options{Dir:dir,MaxBytes:1<<20,ReserveBytes:4096,Sync:true})
 if err!=nil{t.Fatal(err)}
 defer w.Close()
 r,err:=w.Append("evt-partial",json.RawMessage(`{"v":1}`));if err!=nil{t.Fatal(err)}
 if err=w.Ack(Ack{Seq:r.Seq,Sink:"mqtt",EventID:r.EventID});err!=nil{t.Fatal(err)}
 pending,err:=w.PendingWithAcks();if err!=nil{t.Fatal(err)}
 if len(pending)!=1{t.Fatalf("pending count=%d, want 1",len(pending))}
 if !pending[0].MQTTAcked||pending[0].InfluxAcked{t.Fatalf("unexpected ACK state: %+v",pending[0])}
}
func TestAppendRequiresEventID(t *testing.T) {
 w,err:=Open(Options{Dir:t.TempDir(),MaxBytes:1<<20,ReserveBytes:4096})
 if err!=nil{t.Fatal(err)}
 defer w.Close()
 if _,err=w.Append("",json.RawMessage(`{"v":1}`));err==nil{t.Fatal("expected empty event_id to be rejected")}
}


func TestRecoveryRejectsACKEventIDMismatch(t *testing.T) {
 dir:=t.TempDir()
 w,err:=Open(Options{Dir:dir,MaxBytes:1<<20,ReserveBytes:4096,Sync:true})
 if err!=nil{t.Fatal(err)}
 r,err:=w.Append("correct-event",json.RawMessage(`{"v":1}`));if err!=nil{t.Fatal(err)}
 if err=w.Close();err!=nil{t.Fatal(err)}
 f,err:=os.OpenFile(ackSegmentPath(dir,w.manifest.AckActiveSegment),os.O_APPEND|os.O_WRONLY,0600);if err!=nil{t.Fatal(err)}
 raw,err:=json.Marshal(Ack{Seq:r.Seq,Sink:"mqtt",EventID:"wrong-event"});if err!=nil{t.Fatal(err)}
 if err=writeFrame(f,raw);err!=nil{t.Fatal(err)}
 if err=f.Sync();err!=nil{t.Fatal(err)}
 _=f.Close()
 if _,err=Open(Options{Dir:dir,MaxBytes:1<<20,ReserveBytes:4096});err==nil{t.Fatal("expected mismatched ACK identity to fail recovery")}
}


func TestAppendBatchAssignsContiguousSequences(t *testing.T) {
 w,err:=Open(Options{Dir:t.TempDir(),MaxBytes:1<<20,ReserveBytes:4096,Sync:true})
 if err!=nil{t.Fatal(err)}
 defer w.Close()
 records,err:=w.AppendBatch([]AppendInput{
  {EventID:"batch-1",Payload:json.RawMessage(`{"v":1}`)},
  {EventID:"batch-2",Payload:json.RawMessage(`{"v":2}`)},
 })
 if err!=nil{t.Fatal(err)}
 if len(records)!=2||records[0].Seq!=1||records[1].Seq!=2{t.Fatalf("batch records=%+v",records)}
}

func TestAppendBatchRejectsInvalidInputWithoutConsumingSequence(t *testing.T) {
 w,err:=Open(Options{Dir:t.TempDir(),MaxBytes:1<<20,ReserveBytes:4096,Sync:true})
 if err!=nil{t.Fatal(err)}
 defer w.Close()
 if _,err=w.AppendBatch([]AppendInput{{EventID:"bad",Payload:json.RawMessage("not-json")}});err==nil{t.Fatal("expected invalid JSON to be rejected")}
 record,err:=w.Append("good",json.RawMessage(`{"ok":true}`));if err!=nil{t.Fatal(err)}
 if record.Seq!=1{t.Fatalf("sequence=%d, want 1",record.Seq)}
}


func TestCheckpointReadyAdvancesOnlyFullyACKedPrefix(t *testing.T) {
 w,err:=Open(Options{Dir:t.TempDir(),SegmentBytes:1,MaxBytes:1<<20,ReserveBytes:4096,Sync:true})
 if err!=nil{t.Fatal(err)}
 defer w.Close()
 first,err:=w.Append("ready-1",json.RawMessage(`{"v":1}`));if err!=nil{t.Fatal(err)}
 second,err:=w.Append("ready-2",json.RawMessage(`{"v":2}`));if err!=nil{t.Fatal(err)}
 for _,sink:=range []string{"mqtt","influx"}{if err=w.Ack(Ack{Seq:first.Seq,Sink:sink,EventID:first.EventID});err!=nil{t.Fatal(err)}}
 if err=w.Ack(Ack{Seq:second.Seq,Sink:"mqtt",EventID:second.EventID});err!=nil{t.Fatal(err)}
 seq,err:=w.CheckpointReady();if err!=nil{t.Fatal(err)}
 if seq!=first.Seq{t.Fatalf("checkpoint=%d, want %d",seq,first.Seq)}
 if err=w.Ack(Ack{Seq:second.Seq,Sink:"influx",EventID:second.EventID});err!=nil{t.Fatal(err)}
 seq,err=w.CheckpointReady();if err!=nil{t.Fatal(err)}
 if seq!=second.Seq{t.Fatalf("checkpoint=%d, want %d",seq,second.Seq)}
 pending,err:=w.Pending();if err!=nil{t.Fatal(err)}
 if len(pending)!=0{t.Fatalf("pending after checkpoint=%d",len(pending))}
}


func TestCheckpointReadyRetriesACKCompactionAfterManifestCommit(t *testing.T) {
 dir:=t.TempDir()
 w,err:=Open(Options{Dir:dir,MaxBytes:1<<20,ReserveBytes:4096,Sync:true})
 if err!=nil{t.Fatal(err)}
 r,err:=w.Append("crash-ack-compact",json.RawMessage(`{"v":1}`));if err!=nil{t.Fatal(err)}
 for _,sink:=range []string{"mqtt","influx"}{if err=w.Ack(Ack{Seq:r.Seq,Sink:sink,EventID:r.EventID});err!=nil{t.Fatal(err)}}
 // Simulate a crash after MANIFEST commit but before ACK log compaction.
 cp,err:=json.Marshal(Checkpoint{Seq:r.Seq,CreatedAt:time.Now().UTC()});if err!=nil{t.Fatal(err)}
 if err=atomicFile(checkpointPath(dir,r.Seq),cp);err!=nil{t.Fatal(err)}
 w.manifest.CheckpointSeq=r.Seq
 if err=w.saveManifest();err!=nil{t.Fatal(err)}
 before,err:=os.Stat(ackSegmentPath(dir,w.manifest.AckActiveSegment));if err!=nil{t.Fatal(err)}
 if before.Size()==0{t.Fatal("test setup expected un-compacted ACK log")}
 if _,err=w.CheckpointReady();err!=nil{t.Fatal(err)}
 after,err:=os.Stat(ackSegmentPath(dir,w.manifest.AckActiveSegment));if err!=nil{t.Fatal(err)}
 if after.Size()!=0{t.Fatalf("ACK log size after recovery compaction=%d, want 0",after.Size())}
 if err=w.Close();err!=nil{t.Fatal(err)}
}


func TestWALStatsExposePendingCapacityAndSegments(t *testing.T) {
 w,err:=Open(Options{Dir:t.TempDir(),SegmentBytes:1,MaxBytes:2048,ReserveBytes:512,Sync:true})
 if err!=nil{t.Fatal(err)}
 defer w.Close()
 if _,err=w.Append("stats-event",json.RawMessage(`{"v":1}`));err!=nil{t.Fatal(err)}
 tooLarge,_:=json.Marshal(strings.Repeat("x",4096))
 if _,err=w.Append("too-large",tooLarge);!errors.Is(err,ErrCapacity){t.Fatalf("expected capacity rejection, got %v",err)}
 stats,err:=w.Stats();if err!=nil{t.Fatal(err)}
 if stats.PendingRecords!=1{t.Fatalf("pending=%d, want 1",stats.PendingRecords)}
 if stats.CapacityRejections!=1{t.Fatalf("capacity rejections=%d, want 1",stats.CapacityRejections)}
 if stats.AdmissionLimitBytes!=1536{t.Fatalf("admission limit=%d, want 1536",stats.AdmissionLimitBytes)}
 if stats.DiskBytes<=0||stats.DiskUsedRatio<=0{t.Fatalf("invalid disk stats: %+v",stats)}
 if stats.SegmentCount<2{t.Fatalf("segment count=%d, want rotated segments",stats.SegmentCount)}
 if stats.AckSegmentCount<1{t.Fatalf("ACK segment count=%d, want at least one",stats.AckSegmentCount)}
}


func TestCheckpointRejectsSequenceGap(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(Options{Dir: dir, MaxBytes: 1 << 20, ReserveBytes: 4096, Sync: true})
	if err != nil { t.Fatal(err) }
	defer w.Close()

	first, err := w.Append("gap-1", json.RawMessage(`{"v":1}`))
	if err != nil { t.Fatal(err) }
	second, err := w.Append("gap-2", json.RawMessage(`{"v":2}`))
	if err != nil { t.Fatal(err) }
	for _, sink := range []string{"mqtt", "influx"} {
		if err = w.Ack(Ack{Seq: first.Seq, Sink: sink, EventID: first.EventID}); err != nil { t.Fatal(err) }
		if err = w.Ack(Ack{Seq: second.Seq, Sink: sink, EventID: second.EventID}); err != nil { t.Fatal(err) }
	}

	// Simulate corruption/loss of the first record while retaining a valid frame
	// for sequence 2. A checkpoint must never jump over the missing sequence.
	path := filepath.Join(dir, "data-00000000000000000001.wal")
	f, err := os.OpenFile(path, os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil { t.Fatal(err) }
	raw, err := json.Marshal(second)
	if err != nil { t.Fatal(err) }
	if err = writeFrame(f, raw); err != nil { t.Fatal(err) }
	if err = f.Sync(); err != nil { t.Fatal(err) }
	if err = f.Close(); err != nil { t.Fatal(err) }

	if err = w.Checkpoint(second.Seq); err == nil || !strings.Contains(err.Error(), "sequence gap") {
		t.Fatalf("expected sequence gap rejection, got %v", err)
	}
}

func TestRotationPublishesDurableSegmentBeforeManifest(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(Options{Dir: dir, SegmentBytes: 1, MaxBytes: 1 << 20, ReserveBytes: 4096, Sync: true})
	if err != nil { t.Fatal(err) }
	defer w.Close()

	if _, err = w.Append("rotate-1", json.RawMessage(`{"v":1}`)); err != nil { t.Fatal(err) }
	if w.manifest.ActiveSegment != 2 { t.Fatalf("active segment=%d, want 2", w.manifest.ActiveSegment) }
	path := filepath.Join(dir, "data-00000000000000000002.wal")
	st, err := os.Stat(path)
	if err != nil { t.Fatalf("manifest points to missing segment: %v", err) }
	if st.Size() != 0 { t.Fatalf("new active segment should be empty after first rotation, got %d bytes", st.Size()) }
	manifest, err := os.ReadFile(filepath.Join(dir, "MANIFEST"))
	if err != nil { t.Fatal(err) }
	var saved Manifest
	if err = json.Unmarshal(manifest, &saved); err != nil { t.Fatal(err) }
	if saved.ActiveSegment != 2 { t.Fatalf("persisted active segment=%d, want 2", saved.ActiveSegment) }
}


func TestAckRetryRejectsMismatchedEventID(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(Options{Dir: dir, MaxBytes: 1 << 20, ReserveBytes: 4096, Sync: true})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	record, err := w.Append("ack-identity", json.RawMessage(`{"v":1}`))
	if err != nil {
		t.Fatal(err)
	}
	valid := Ack{Seq: record.Seq, Sink: "mqtt", EventID: record.EventID}
	if err = w.Ack(valid); err != nil {
		t.Fatal(err)
	}
	forged := valid
	forged.EventID = "different-event"
	if err = w.Ack(forged); err == nil || !strings.Contains(err.Error(), "event_id mismatch") {
		t.Fatalf("mismatched duplicate ACK must fail identity validation, got %v", err)
	}
}

func TestAckRetryIsIdempotent(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(Options{Dir: dir, MaxBytes: 1 << 20, ReserveBytes: 4096, Sync: true})
	if err != nil { t.Fatal(err) }
	defer w.Close()
	r, err := w.Append("ack-idempotent", json.RawMessage(`{"v":1}`))
	if err != nil { t.Fatal(err) }
	ack := Ack{Seq: r.Seq, Sink: "mqtt", EventID: r.EventID}
	if err = w.Ack(ack); err != nil { t.Fatal(err) }
	before, err := os.Stat(ackSegmentPath(dir, w.manifest.AckActiveSegment))
	if err != nil { t.Fatal(err) }
	for i := 0; i < 10; i++ {
		if err = w.Ack(ack); err != nil { t.Fatal(err) }
	}
	after, err := os.Stat(ackSegmentPath(dir, w.manifest.AckActiveSegment))
	if err != nil { t.Fatal(err) }
	if after.Size() != before.Size() {
		t.Fatalf("duplicate ACK retry grew log: before=%d after=%d", before.Size(), after.Size())
	}
}


func TestAppendFailStopKeepsACKButBlocksMetadataDeletion(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(Options{Dir: dir, MaxBytes: 1 << 20, ReserveBytes: 4096, Sync: true})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	record, err := w.Append("fail-stop-ack", json.RawMessage(`{"v":1}`))
	if err != nil {
		t.Fatal(err)
	}

	w.failed = errors.New("simulated uncertain append/manifest outcome")
	if err = w.Ack(Ack{Seq: record.Seq, Sink: "mqtt", EventID: record.EventID}); err != nil {
		t.Fatalf("ACK must remain available in append fail-stop: %v", err)
	}
	if err = w.Ack(Ack{Seq: record.Seq, Sink: "influx", EventID: record.EventID}); err != nil {
		t.Fatalf("second sink ACK must remain available in append fail-stop: %v", err)
	}
	if err = w.Checkpoint(record.Seq); err == nil {
		t.Fatal("checkpoint must fail closed while append outcome is uncertain")
	}
	if err = w.Reclaim(); err == nil {
		t.Fatal("reclaim must fail closed while append outcome is uncertain")
	}
}


func TestCheckpointReadyReplenishesACKReserve(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(Options{Dir: dir, MaxBytes: 1 << 20, ReserveBytes: 4096, Sync: true})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	record, err := w.Append("reserve-replenish", json.RawMessage(`{"v":1}`))
	if err != nil {
		t.Fatal(err)
	}
	for _, sink := range []string{"mqtt", "influx"} {
		if err = w.Ack(Ack{Seq: record.Seq, Sink: sink, EventID: record.EventID}); err != nil {
			t.Fatal(err)
		}
	}
	reservePath := filepath.Join(dir, "ack.reserve")
	if err = os.Remove(reservePath); err != nil {
		t.Fatal(err)
	}
	if _, err = w.CheckpointReady(); err != nil {
		t.Fatalf("checkpoint worker should replenish reserve: %v", err)
	}
	info, err := os.Stat(reservePath)
	if err != nil {
		t.Fatalf("ACK reserve was not recreated: %v", err)
	}
	if info.Size() != 4096 {
		t.Fatalf("ACK reserve size=%d, want 4096", info.Size())
	}
}


func TestManifestV5ChecksumRejectsTampering(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(Options{Dir: dir, SegmentBytes: 1, MaxBytes: 1 << 20, ReserveBytes: 4096, Sync: true})
	if err != nil { t.Fatal(err) }
	if _, err = w.Append("manifest-checksum", json.RawMessage(`{"v":1}`)); err != nil { t.Fatal(err) }
	if err = w.Close(); err != nil { t.Fatal(err) }

	path := filepath.Join(dir, "MANIFEST")
	raw, err := os.ReadFile(path)
	if err != nil { t.Fatal(err) }
	var m Manifest
	if err = json.Unmarshal(raw, &m); err != nil { t.Fatal(err) }
	if m.Version != 6 || m.Checksum == "" { t.Fatalf("expected checksummed v2 manifest, got %+v", m) }
	m.Checksum = strings.Repeat("0", len(m.Checksum))
	tampered, err := json.Marshal(m)
	if err != nil { t.Fatal(err) }
	if err = os.WriteFile(path, tampered, 0600); err != nil { t.Fatal(err) }
	if _, err = Open(Options{Dir: dir, MaxBytes: 1 << 20, ReserveBytes: 4096, Sync: true}); err == nil || !strings.Contains(err.Error(), "checksum mismatch") {
		t.Fatalf("expected manifest checksum rejection, got %v", err)
	}
}


func TestManifestRejectsTamperedCheckpointPayload(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(Options{Dir: dir, MaxBytes: 1 << 20, ReserveBytes: 4096, Sync: true})
	if err != nil { t.Fatal(err) }
	record, err := w.Append("checkpoint-integrity", json.RawMessage(`{"v":1}`))
	if err != nil { t.Fatal(err) }
	for _, sink := range []string{"mqtt", "influx"} {
		if err = w.Ack(Ack{Seq: record.Seq, Sink: sink, EventID: record.EventID}); err != nil { t.Fatal(err) }
	}
	if err = w.Checkpoint(record.Seq); err != nil { t.Fatal(err) }
	if err = w.Close(); err != nil { t.Fatal(err) }

	path := checkpointPath(dir, record.Seq)
	raw, err := os.ReadFile(path)
	if err != nil { t.Fatal(err) }
	var cp Checkpoint
	if err = json.Unmarshal(raw, &cp); err != nil { t.Fatal(err) }
	cp.CreatedAt = cp.CreatedAt.Add(time.Second)
	tampered, err := json.Marshal(cp)
	if err != nil { t.Fatal(err) }
	if err = os.WriteFile(path, tampered, 0600); err != nil { t.Fatal(err) }
	if _, err = Open(Options{Dir: dir, MaxBytes: 1 << 20, ReserveBytes: 4096, Sync: true}); err == nil || !strings.Contains(err.Error(), "checkpoint checksum mismatch") {
		t.Fatalf("expected checkpoint checksum rejection, got %v", err)
	}
}


func TestManifestV2UpgradesToV5OnOpen(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(dir, 0700); err != nil { t.Fatal(err) }
	if err := os.WriteFile(filepath.Join(dir, "data-00000000000000000001.wal"), nil, 0600); err != nil { t.Fatal(err) }
	legacy := Manifest{Version: 2, FirstSegment: 1, ActiveSegment: 1}
	legacy.Checksum, _ = manifestChecksum(legacy)
	raw, err := json.Marshal(legacy)
	if err != nil { t.Fatal(err) }
	if err = os.WriteFile(filepath.Join(dir, "MANIFEST"), raw, 0600); err != nil { t.Fatal(err) }

	w, err := Open(Options{Dir: dir, SegmentBytes: 1, MaxBytes: 1 << 20, ReserveBytes: 4096, Sync: true})
	if err != nil { t.Fatalf("load v2 manifest: %v", err) }
	if _, err = w.Append("v2-upgrade", json.RawMessage(`{"v":1}`)); err != nil { t.Fatal(err) }
	if err = w.Close(); err != nil { t.Fatal(err) }

	raw, err = os.ReadFile(filepath.Join(dir, "MANIFEST"))
	if err != nil { t.Fatal(err) }
	var upgraded Manifest
	if err = json.Unmarshal(raw, &upgraded); err != nil { t.Fatal(err) }
	if upgraded.Version != 6 || upgraded.Checksum == "" { t.Fatalf("v2 manifest was not upgraded: %+v", upgraded) }
	reopened, err := Open(Options{Dir: dir, MaxBytes: 1 << 20, ReserveBytes: 4096, Sync: true})
	if err != nil { t.Fatalf("reopen v6 manifest: %v", err) }
	if err = reopened.Close(); err != nil { t.Fatal(err) }
}

func TestLegacyManifestUpgradesToV5OnOpen(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(dir, 0700); err != nil { t.Fatal(err) }
	if err := os.WriteFile(filepath.Join(dir, "data-00000000000000000001.wal"), nil, 0600); err != nil { t.Fatal(err) }
	legacy, err := json.Marshal(Manifest{Version: 1, FirstSegment: 1, ActiveSegment: 1})
	if err != nil { t.Fatal(err) }
	if err = os.WriteFile(filepath.Join(dir, "MANIFEST"), legacy, 0600); err != nil { t.Fatal(err) }

	w, err := Open(Options{Dir: dir, SegmentBytes: 1, MaxBytes: 1 << 20, ReserveBytes: 4096, Sync: true})
	if err != nil { t.Fatal(err) }
	if _, err = w.Append("legacy-upgrade", json.RawMessage(`{"v":1}`)); err != nil { t.Fatal(err) }
	if err = w.Close(); err != nil { t.Fatal(err) }

	raw, err := os.ReadFile(filepath.Join(dir, "MANIFEST"))
	if err != nil { t.Fatal(err) }
	var upgraded Manifest
	if err = json.Unmarshal(raw, &upgraded); err != nil { t.Fatal(err) }
	if upgraded.Version != 6 || upgraded.Checksum == "" { t.Fatalf("legacy manifest was not upgraded: %+v", upgraded) }
	reopened, reopenErr := Open(Options{Dir: dir, MaxBytes: 1 << 20, ReserveBytes: 4096, Sync: true})
	if reopenErr != nil { t.Fatalf("reopen upgraded manifest: %v", reopenErr) }
	if err = reopened.Close(); err != nil { t.Fatal(err) }
}


func TestFsyncMetricsTrackDataAndACKDurability(t *testing.T) {
	before := snapshotFsyncMetrics()
	w, err := Open(Options{Dir: t.TempDir(), MaxBytes: 1 << 20, ReserveBytes: 0, Sync: true})
	if err != nil { t.Fatal(err) }
	record, err := w.Append("fsync-metric", json.RawMessage(`{"v":1}`))
	if err != nil { t.Fatal(err) }
	if err = w.Ack(Ack{Seq: record.Seq, Sink: "mqtt", EventID: record.EventID}); err != nil { t.Fatal(err) }
	stats, err := w.Stats()
	if err != nil { t.Fatal(err) }
	if err = w.Close(); err != nil { t.Fatal(err) }

	if stats.Fsync.Count < before.Count+2 {
		t.Fatalf("fsync count=%d, want at least %d", stats.Fsync.Count, before.Count+2)
	}
	if stats.Fsync.Errors < before.Errors {
		t.Fatalf("fsync error counter moved backwards: before=%d after=%d", before.Errors, stats.Fsync.Errors)
	}
	var bucketTotal uint64
	for _, count := range stats.Fsync.Buckets { bucketTotal += count }
	if bucketTotal != stats.Fsync.Count {
		t.Fatalf("fsync histogram buckets=%d, count=%d", bucketTotal, stats.Fsync.Count)
	}
}


func TestFsyncMetricsCountSyncFailures(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "closed-sync")
	if err != nil { t.Fatal(err) }
	if err = f.Close(); err != nil { t.Fatal(err) }
	before := snapshotFsyncMetrics()
	if err = syncFile(f); err == nil { t.Fatal("sync on closed file should fail") }
	after := snapshotFsyncMetrics()
	if after.Count != before.Count+1 || after.Errors != before.Errors+1 {
		t.Fatalf("fsync failure metrics not incremented: before=%+v after=%+v", before, after)
	}
}


func TestDiskWatermarkClassification(t *testing.T) {
	cases := []struct {
		ratio float64
		want string
	}{
		{0.69, "normal"},
		{0.70, "warning"},
		{0.84, "warning"},
		{0.85, "critical"},
		{0.94, "critical"},
		{0.95, "stop"},
	}
	for _, tc := range cases {
		if got := classifyDiskLevel(tc.ratio, 0); got != tc.want {
			t.Errorf("classifyDiskLevel(%0.2f)=%q, want %q", tc.ratio, got, tc.want)
		}
	}
	if got := classifyDiskLevel(0.40, 0.96); got != "stop" {
		t.Fatalf("filesystem pressure should force stop, got %q", got)
	}
}

func TestStatsExposeFilesystemUsage(t *testing.T) {
	w, err := Open(Options{Dir: t.TempDir(), MaxBytes: 1 << 20, ReserveBytes: 0, Sync: true})
	if err != nil { t.Fatal(err) }
	defer w.Close()
	stats, err := w.Stats()
	if err != nil { t.Fatal(err) }
	if stats.FilesystemTotalBytes > 0 && (stats.FilesystemBytes == 0 || stats.FilesystemUsedRatio <= 0) {
		t.Fatalf("filesystem usage not populated: %+v", stats)
	}
	if stats.DiskLevel == "" { t.Fatal("disk pressure level is empty") }
}


func TestReclaimRevalidatesCommittedCheckpointChecksum(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(Options{Dir: dir, SegmentBytes: 1, MaxBytes: 1 << 20, ReserveBytes: 4096, Sync: true})
	if err != nil { t.Fatal(err) }
	defer w.Close()

	first, err := w.Append("reclaim-check-1", json.RawMessage(`{"v":1}`))
	if err != nil { t.Fatal(err) }
	second, err := w.Append("reclaim-check-2", json.RawMessage(`{"v":2}`))
	if err != nil { t.Fatal(err) }
	for _, record := range []Record{first, second} {
		for _, sink := range []string{"mqtt", "influx"} {
			if err = w.Ack(Ack{Seq: record.Seq, Sink: sink, EventID: record.EventID}); err != nil { t.Fatal(err) }
		}
	}
	if err = w.Checkpoint(second.Seq); err != nil { t.Fatal(err) }

	path := checkpointPath(dir, second.Seq)
	raw, err := os.ReadFile(path)
	if err != nil { t.Fatal(err) }
	var cp Checkpoint
	if err = json.Unmarshal(raw, &cp); err != nil { t.Fatal(err) }
	cp.CreatedAt = cp.CreatedAt.Add(time.Second)
	tampered, err := json.Marshal(cp)
	if err != nil { t.Fatal(err) }
	if err = os.WriteFile(path, tampered, 0600); err != nil { t.Fatal(err) }

	if err = w.Reclaim(); err == nil || !strings.Contains(err.Error(), "checkpoint checksum mismatch") {
		t.Fatalf("reclaim should reject tampered checkpoint, got %v", err)
	}
	firstSegment := filepath.Join(dir, "data-00000000000000000001.wal")
	if _, err = os.Stat(firstSegment); err != nil {
		t.Fatalf("reclaim deleted a segment despite checkpoint checksum failure: %v", err)
	}
	if w.manifest.FirstSegment != 1 { t.Fatalf("first segment advanced to %d after failed validation", w.manifest.FirstSegment) }
}


func TestManifestCommitFailureFailStopsMetadataButKeepsACKs(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(Options{Dir: dir, MaxBytes: 1 << 20, ReserveBytes: 4096, Sync: true})
	if err != nil { t.Fatal(err) }
	defer w.Close()

	first, err := w.Append("manifest-fail-1", json.RawMessage(`{"v":1}`))
	if err != nil { t.Fatal(err) }
	second, err := w.Append("manifest-fail-2", json.RawMessage(`{"v":2}`))
	if err != nil { t.Fatal(err) }
	for _, sink := range []string{"mqtt", "influx"} {
		if err = w.Ack(Ack{Seq: first.Seq, Sink: sink, EventID: first.EventID}); err != nil { t.Fatal(err) }
	}
	if err = w.Ack(Ack{Seq: second.Seq, Sink: "mqtt", EventID: second.EventID}); err != nil { t.Fatal(err) }

	// Force the MANIFEST rename to fail after checkpoint bytes have been synced.
	if err = os.Remove(filepath.Join(dir, "MANIFEST")); err != nil { t.Fatal(err) }
	if err = os.Mkdir(filepath.Join(dir, "MANIFEST"), 0700); err != nil { t.Fatal(err) }
	if err = w.Checkpoint(first.Seq); err == nil { t.Fatal("expected manifest commit failure") }
	if w.failed == nil { t.Fatal("manifest commit failure must put metadata operations into fail-stop") }

	if err = w.Ack(Ack{Seq: second.Seq, Sink: "influx", EventID: second.EventID}); err != nil {
		t.Fatalf("ACK path must remain usable after metadata commit failure: %v", err)
	}
	if err = w.Reclaim(); err == nil { t.Fatal("reclaim must fail-stop after uncertain manifest commit") }
}


func TestAppendRejectsOversizedEventIDWithoutConsumingSequence(t *testing.T) {
	w, err := Open(Options{Dir: t.TempDir(), MaxBytes: 1 << 20, ReserveBytes: 0, Sync: true})
	if err != nil { t.Fatal(err) }
	defer w.Close()
	if _, err = w.Append(strings.Repeat("x", 1025), json.RawMessage(`{"v":1}`)); err == nil {
		t.Fatal("expected oversized event_id rejection")
	}
	record, err := w.Append("valid-event", json.RawMessage(`{"v":2}`))
	if err != nil { t.Fatal(err) }
	if record.Seq != 1 { t.Fatalf("first valid sequence=%d, want 1", record.Seq) }
}


func TestACKSegmentsRotateAndRecoverIndependentSinkState(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(Options{Dir: dir, SegmentBytes: 1, MaxBytes: 1 << 20, ReserveBytes: 4096, Sync: true})
	if err != nil { t.Fatal(err) }
	record, err := w.Append("ack-segment-rotation", json.RawMessage(`{"v":1}`))
	if err != nil { t.Fatal(err) }
	if err = w.Ack(Ack{Seq: record.Seq, Sink: "mqtt", EventID: record.EventID}); err != nil { t.Fatal(err) }
	if err = w.Ack(Ack{Seq: record.Seq, Sink: "influx", EventID: record.EventID}); err != nil { t.Fatal(err) }
	if w.manifest.AckActiveSegment <= w.manifest.AckFirstSegment {
		t.Fatalf("ACK segment did not rotate: %+v", w.manifest)
	}
	if err = w.Close(); err != nil { t.Fatal(err) }

	reopened, err := Open(Options{Dir: dir, SegmentBytes: 1, MaxBytes: 1 << 20, ReserveBytes: 4096, Sync: true})
	if err != nil { t.Fatal(err) }
	defer reopened.Close()
	pending, err := reopened.PendingWithAcks()
	if err != nil { t.Fatal(err) }
	if len(pending) != 0 { t.Fatalf("fully ACKed record should not be pending: %+v", pending) }
	state, err := reopened.ackState()
	if err != nil { t.Fatal(err) }
	if !state["mqtt"][record.Seq] || !state["influx"][record.Seq] {
		t.Fatalf("ACK state lost across segments: %+v", state)
	}
}

func TestLegacyACKLogMigratesToManifestManagedSegment(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(dir, 0700); err != nil { t.Fatal(err) }
	dataFile, err := os.Create(filepath.Join(dir, "data-00000000000000000001.wal"))
	if err != nil { t.Fatal(err) }
	dataRaw, err := json.Marshal(Record{Seq: 1, EventID: "legacy", Payload: json.RawMessage(`{"v":1}`), CreatedAt: time.Now().UTC()})
	if err != nil { t.Fatal(err) }
	if err = writeFrame(dataFile, dataRaw); err != nil { t.Fatal(err) }
	if err = syncFile(dataFile); err != nil { t.Fatal(err) }
	if err = dataFile.Close(); err != nil { t.Fatal(err) }
	legacy := filepath.Join(dir, "ack.wal")
	f, err := os.Create(legacy)
	if err != nil { t.Fatal(err) }
	if err = writeFrame(f, []byte(`{"seq":1,"sink":"mqtt","event_id":"legacy"}`)); err != nil { t.Fatal(err) }
	if err = syncFile(f); err != nil { t.Fatal(err) }
	if err = f.Close(); err != nil { t.Fatal(err) }

	manifest := Manifest{Version: 3, FirstSegment: 1, ActiveSegment: 1}
	manifest.Checksum, err = manifestChecksum(manifest)
	if err != nil { t.Fatal(err) }
	raw, err := json.Marshal(manifest)
	if err != nil { t.Fatal(err) }
	if err = os.WriteFile(filepath.Join(dir, "MANIFEST"), raw, 0600); err != nil { t.Fatal(err) }

	w, err := Open(Options{Dir: dir, MaxBytes: 1 << 20, ReserveBytes: 4096, Sync: true})
	if err != nil { t.Fatal(err) }
	defer w.Close()
	if w.manifest.Version != 6 || w.manifest.AckFirstSegment != 1 || w.manifest.AckActiveSegment != 1 {
		t.Fatalf("legacy manifest was not migrated: %+v", w.manifest)
	}
	if _, err = os.Stat(legacy); !os.IsNotExist(err) {
		t.Fatalf("legacy ACK log should be removed only after migration commit, stat err=%v", err)
	}
	state, err := w.ackState()
	if err != nil { t.Fatal(err) }
	if !state["mqtt"][1] { t.Fatalf("legacy ACK was not migrated: %+v", state) }
}


func TestACKSegmentCompactionCommitsBoundaryBeforeDeletingOldSegments(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(Options{Dir: dir, SegmentBytes: 1, MaxBytes: 1 << 20, ReserveBytes: 4096, Sync: true})
	if err != nil { t.Fatal(err) }
	record, err := w.Append("ack-compact", json.RawMessage(`{"v":1}`))
	if err != nil { t.Fatal(err) }
	for _, sink := range []string{"mqtt", "influx"} {
		if err = w.Ack(Ack{Seq: record.Seq, Sink: sink, EventID: record.EventID}); err != nil { t.Fatal(err) }
	}
	oldFirst, oldActive := w.manifest.AckFirstSegment, w.manifest.AckActiveSegment
	if oldActive <= oldFirst { t.Fatalf("expected multiple ACK segments before compaction: %+v", w.manifest) }
	if err = w.Checkpoint(record.Seq); err != nil { t.Fatal(err) }
	if w.manifest.AckFirstSegment != w.manifest.AckActiveSegment || w.manifest.AckActiveSegment <= oldActive {
		t.Fatalf("compaction did not publish a fresh ACK segment: %+v", w.manifest)
	}
	for id := oldFirst; id <= oldActive; id++ {
		if _, err = os.Stat(ackSegmentPath(dir, id)); !os.IsNotExist(err) {
			t.Fatalf("obsolete ACK segment %d remains after committed compaction, stat err=%v", id, err)
		}
	}
	if err = w.Close(); err != nil { t.Fatal(err) }
	reopened, err := Open(Options{Dir: dir, MaxBytes: 1 << 20, ReserveBytes: 4096, Sync: true})
	if err != nil { t.Fatalf("reopen after ACK compaction: %v", err) }
	defer reopened.Close()
	if reopened.manifest.CheckpointSeq != record.Seq { t.Fatalf("checkpoint sequence=%d, want %d", reopened.manifest.CheckpointSeq, record.Seq) }
}

func TestRecoveryRemovesUncommittedACKSegmentOrphan(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(Options{Dir: dir, MaxBytes: 1 << 20, ReserveBytes: 4096, Sync: true})
	if err != nil { t.Fatal(err) }
	orphanID := w.manifest.AckActiveSegment + 1
	if err = os.WriteFile(ackSegmentPath(dir, orphanID), []byte("uncommitted orphan"), 0600); err != nil { t.Fatal(err) }
	if err = w.Close(); err != nil { t.Fatal(err) }
	reopened, err := Open(Options{Dir: dir, MaxBytes: 1 << 20, ReserveBytes: 4096, Sync: true})
	if err != nil { t.Fatal(err) }
	defer reopened.Close()
	if _, err = os.Stat(ackSegmentPath(dir, orphanID)); !os.IsNotExist(err) {
		t.Fatalf("uncommitted ACK segment orphan was not removed, stat err=%v", err)
	}
}

func TestOpenFailsClosedWhenCommittedACKSegmentIsMissing(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(Options{Dir: dir, MaxBytes: 1 << 20, ReserveBytes: 4096, Sync: true})
	if err != nil { t.Fatal(err) }
	active := ackSegmentPath(dir, w.manifest.AckActiveSegment)
	if err = w.Close(); err != nil { t.Fatal(err) }
	if err = os.Remove(active); err != nil { t.Fatal(err) }
	if _, err = Open(Options{Dir: dir, MaxBytes: 1 << 20, ReserveBytes: 4096, Sync: true}); err == nil {
		t.Fatal("missing committed ACK segment must fail closed")
	}
}


func TestACKCompactionManifestFailureKeepsNewACKSegmentRecoverable(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(Options{Dir: dir, MaxBytes: 1 << 20, ReserveBytes: 4096, Sync: true})
	if err != nil { t.Fatal(err) }
	first, err := w.Append("compact-fault-first", json.RawMessage(`{"v":1}`))
	if err != nil { t.Fatal(err) }
	second, err := w.Append("compact-fault-second", json.RawMessage(`{"v":2}`))
	if err != nil { t.Fatal(err) }
	for _, sink := range []string{"mqtt", "influx"} {
		if err = w.Ack(Ack{Seq: first.Seq, Sink: sink, EventID: first.EventID}); err != nil { t.Fatal(err) }
	}
	calls := 0
	w.manifestCommitHook = func(Manifest) error {
		calls++
		if calls == 3 { return errors.New("injected second ACK-boundary commit failure") }
		return nil
	}
	if err = w.Checkpoint(first.Seq); err == nil {
		t.Fatal("expected injected ACK lower-boundary commit failure")
	}
	if w.manifest.AckActiveSegment <= w.manifest.AckFirstSegment {
		t.Fatalf("failed second commit must retain the extended ACK range: %+v", w.manifest)
	}
	if w.ack == nil { t.Fatal("new active ACK segment must remain writable") }
	if err = w.Ack(Ack{Seq: second.Seq, Sink: "mqtt", EventID: second.EventID}); err != nil {
		t.Fatalf("ACK must remain available on the new segment after compaction failure: %v", err)
	}
	if err = w.Close(); err != nil { t.Fatal(err) }

	reopened, err := Open(Options{Dir: dir, MaxBytes: 1 << 20, ReserveBytes: 4096, Sync: true})
	if err != nil { t.Fatalf("reopen after uncertain ACK compaction: %v", err) }
	defer reopened.Close()
	pending, err := reopened.PendingWithAcks()
	if err != nil { t.Fatal(err) }
	if len(pending) != 1 || pending[0].Seq != second.Seq || !pending[0].MQTTAcked || pending[0].InfluxAcked {
		t.Fatalf("ACK written after compaction failure was not recovered: %+v", pending)
	}
}


func TestMissingCommittedACKSegmentPreservesLegacyRecoveryCopy(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(Options{Dir: dir, MaxBytes: 1 << 20, ReserveBytes: 4096, Sync: true})
	if err != nil { t.Fatal(err) }
	active := ackSegmentPath(dir, w.manifest.AckActiveSegment)
	if err = w.Close(); err != nil { t.Fatal(err) }
	if err = os.Remove(active); err != nil { t.Fatal(err) }
	legacy := filepath.Join(dir, "ack.wal")
	if err = os.WriteFile(legacy, []byte("legacy recovery copy"), 0600); err != nil { t.Fatal(err) }
	if _, err = Open(Options{Dir: dir, MaxBytes: 1 << 20, ReserveBytes: 4096, Sync: true}); err == nil {
		t.Fatal("missing committed ACK segment must fail closed")
	}
	if _, err = os.Stat(legacy); err != nil {
		t.Fatalf("legacy recovery copy must be preserved on failed migration validation: %v", err)
	}
}


func TestOpenFailsClosedWhenCommittedActiveDataSegmentIsMissing(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(Options{Dir: dir, MaxBytes: 1 << 20, ReserveBytes: 4096, Sync: true})
	if err != nil { t.Fatal(err) }
	active := filepath.Join(dir, fmt.Sprintf("data-%020d.wal", w.manifest.ActiveSegment))
	if err = w.Close(); err != nil { t.Fatal(err) }
	if err = os.Remove(active); err != nil { t.Fatal(err) }
	if _, err = Open(Options{Dir: dir, MaxBytes: 1 << 20, ReserveBytes: 4096, Sync: true}); err == nil {
		t.Fatal("missing committed active Data segment must fail closed")
	}
	if _, err = os.Stat(active); !os.IsNotExist(err) {
		t.Fatalf("Open must not silently recreate a missing committed Data segment, stat err=%v", err)
	}
}


func TestRecoveryCleansACKSegmentsAfterCommittedBoundaryAdvance(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(Options{Dir: dir, MaxBytes: 1 << 20, ReserveBytes: 4096, Sync: true})
	if err != nil { t.Fatal(err) }
	oldID := w.manifest.AckActiveSegment
	newID := oldID + 1
	newPath := ackSegmentPath(dir, newID)
	f, err := os.OpenFile(newPath, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil { t.Fatal(err) }
	if err = syncFile(f); err != nil { t.Fatal(err) }
	if err = f.Close(); err != nil { t.Fatal(err) }
	d, err := os.Open(dir)
	if err != nil { t.Fatal(err) }
	if err = syncFile(d); err != nil { t.Fatal(err) }
	if err = d.Close(); err != nil { t.Fatal(err) }

	w.manifest.AckFirstSegment, w.manifest.AckActiveSegment = newID, newID
	if err = w.saveManifest(); err != nil { t.Fatal(err) }
	if _, err = os.Stat(ackSegmentPath(dir, oldID)); err != nil { t.Fatalf("old ACK segment should remain until cleanup: %v", err) }
	if err = w.Close(); err != nil { t.Fatal(err) }

	reopened, err := Open(Options{Dir: dir, MaxBytes: 1 << 20, ReserveBytes: 4096, Sync: true})
	if err != nil { t.Fatal(err) }
	defer reopened.Close()
	if _, err = os.Stat(ackSegmentPath(dir, oldID)); !os.IsNotExist(err) {
		t.Fatalf("recovery did not remove ACK segment left after committed boundary advance: %v", err)
	}
	if _, err = os.Stat(newPath); err != nil { t.Fatalf("committed ACK segment was removed: %v", err) }
}


func TestManifestV6UsesExplicitCheckpointFileReference(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(Options{Dir: dir, SegmentBytes: 1, MaxBytes: 1 << 20, ReserveBytes: 4096, Sync: true})
	if err != nil { t.Fatal(err) }
	record, err := w.Append("explicit-checkpoint-ref", json.RawMessage(`{"v":1}`))
	if err != nil { t.Fatal(err) }
	for _, sink := range []string{"mqtt", "influx"} {
		if err = w.Ack(Ack{Seq: record.Seq, Sink: sink, EventID: record.EventID}); err != nil { t.Fatal(err) }
	}
	if err = w.Checkpoint(record.Seq); err != nil { t.Fatal(err) }
	oldPath := checkpointPath(dir, record.Seq)
	customName := fmt.Sprintf("committed-checkpoint-%020d.json", record.Seq)
	customPath := filepath.Join(dir, customName)
	if err = os.Rename(oldPath, customPath); err != nil { t.Fatal(err) }
	w.manifest.CheckpointFile = customName
	if err = w.saveManifest(); err != nil { t.Fatal(err) }
	if w.manifest.Version != 6 || w.manifest.CheckpointFile != customName {
		t.Fatalf("explicit checkpoint reference not committed: %+v", w.manifest)
	}
	if err = w.Close(); err != nil { t.Fatal(err) }

	reopened, err := Open(Options{Dir: dir, MaxBytes: 1 << 20, ReserveBytes: 4096, Sync: true})
	if err != nil { t.Fatalf("open using explicit checkpoint reference: %v", err) }
	defer reopened.Close()
	if reopened.manifest.CheckpointFile != customName {
		t.Fatalf("checkpoint reference changed on reopen: %+v", reopened.manifest)
	}
	if err = reopened.Reclaim(); err != nil {
		t.Fatalf("reclaim must read the explicit checkpoint file reference: %v", err)
	}
}

func TestManifestV5RejectsCheckpointPathTraversal(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(Options{Dir: dir, SegmentBytes: 1, MaxBytes: 1 << 20, ReserveBytes: 4096, Sync: true})
	if err != nil { t.Fatal(err) }
	record, err := w.Append("checkpoint-path-traversal", json.RawMessage(`{"v":1}`))
	if err != nil { t.Fatal(err) }
	for _, sink := range []string{"mqtt", "influx"} {
		if err = w.Ack(Ack{Seq: record.Seq, Sink: sink, EventID: record.EventID}); err != nil { t.Fatal(err) }
	}
	if err = w.Checkpoint(record.Seq); err != nil { t.Fatal(err) }
	w.manifest.CheckpointFile = "../outside.json"
	if err = w.saveManifest(); err == nil { t.Fatal("unsafe checkpoint reference must not be committed") }
	if err = w.Close(); err != nil { t.Fatal(err) }

	raw, err := os.ReadFile(filepath.Join(dir, "MANIFEST"))
	if err != nil { t.Fatal(err) }
	var manifest Manifest
	if err = json.Unmarshal(raw, &manifest); err != nil { t.Fatal(err) }
	manifest.CheckpointFile = "../outside.json"
	manifest.Checksum, err = manifestChecksum(manifest)
	if err != nil { t.Fatal(err) }
	raw, err = json.Marshal(manifest)
	if err != nil { t.Fatal(err) }
	if err = os.WriteFile(filepath.Join(dir, "MANIFEST"), raw, 0600); err != nil { t.Fatal(err) }
	if _, err = Open(Options{Dir: dir, MaxBytes: 1 << 20, ReserveBytes: 4096, Sync: true}); err == nil || !strings.Contains(err.Error(), "invalid manifest checkpoint file reference") {
		t.Fatalf("unsafe checkpoint reference must be rejected on load, got %v", err)
	}
}


func TestACKReserveReplenishmentSerializesWithConcurrentACK(t *testing.T) {
	w, err := Open(Options{Dir: t.TempDir(), SegmentBytes: 1, MaxBytes: 1 << 20, ReserveBytes: 4096, Sync: true})
	if err != nil { t.Fatal(err) }
	defer w.Close()
	first, err := w.Append("reserve-sync-first", json.RawMessage(`{"v":1}`))
	if err != nil { t.Fatal(err) }
	second, err := w.Append("reserve-sync-second", json.RawMessage(`{"v":2}`))
	if err != nil { t.Fatal(err) }
	for _, sink := range []string{"mqtt", "influx"} {
		if err = w.Ack(Ack{Seq: first.Seq, Sink: sink, EventID: first.EventID}); err != nil { t.Fatal(err) }
	}

	var wg sync.WaitGroup
	errs := make(chan error, 2)
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 5; i++ {
			if _, err := w.CheckpointReady(); err != nil { errs <- err; return }
		}
	}()
	go func() {
		defer wg.Done()
		ack := Ack{Seq: second.Seq, Sink: "mqtt", EventID: second.EventID}
		for i := 0; i < 5; i++ {
			if err := w.Ack(ack); err != nil { errs <- err; return }
		}
	}()
	wg.Wait()
	close(errs)
	for err := range errs { t.Fatal(err) }
}


func TestManifestV4MigrationMaterializesLegacyCheckpointFileToV6(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(Options{Dir: dir, SegmentBytes: 1, MaxBytes: 1 << 20, ReserveBytes: 4096, Sync: true})
	if err != nil { t.Fatal(err) }
	record, err := w.Append("legacy-checkpoint-file", json.RawMessage(`{"v":1}`))
	if err != nil { t.Fatal(err) }
	for _, sink := range []string{"mqtt", "influx"} {
		if err = w.Ack(Ack{Seq: record.Seq, Sink: sink, EventID: record.EventID}); err != nil { t.Fatal(err) }
	}
	if err = w.Checkpoint(record.Seq); err != nil { t.Fatal(err) }
	if err = w.Close(); err != nil { t.Fatal(err) }

	manifestPath := filepath.Join(dir, "MANIFEST")
	raw, err := os.ReadFile(manifestPath)
	if err != nil { t.Fatal(err) }
	var manifest Manifest
	if err = json.Unmarshal(raw, &manifest); err != nil { t.Fatal(err) }
	oldCheckpoint := checkpointPath(dir, record.Seq)
	legacyCheckpoint := filepath.Join(dir, "checkpoint.json")
	if err = os.Rename(oldCheckpoint, legacyCheckpoint); err != nil { t.Fatal(err) }
	manifest.Version = 4
	manifest.CheckpointFile = ""
	manifest.Checksum, err = manifestChecksum(manifest)
	if err != nil { t.Fatal(err) }
	raw, err = json.Marshal(manifest)
	if err != nil { t.Fatal(err) }
	if err = os.WriteFile(manifestPath, raw, 0600); err != nil { t.Fatal(err) }

	reopened, err := Open(Options{Dir: dir, MaxBytes: 1 << 20, ReserveBytes: 4096, Sync: true})
	if err != nil { t.Fatalf("load legacy checkpoint.json: %v", err) }
	if err = reopened.saveManifest(); err != nil { t.Fatalf("migrate explicit checkpoint reference: %v", err) }
	if reopened.manifest.Version != 6 || reopened.manifest.CheckpointFile != filepath.Base(oldCheckpoint) {
		t.Fatalf("legacy checkpoint reference was not migrated: %+v", reopened.manifest)
	}
	if _, err = os.Stat(oldCheckpoint); err != nil { t.Fatalf("derived checkpoint copy was not materialized: %v", err) }
	if err = reopened.Close(); err != nil { t.Fatal(err) }
	final, err := Open(Options{Dir: dir, MaxBytes: 1 << 20, ReserveBytes: 4096, Sync: true})
	if err != nil { t.Fatalf("reopen migrated checkpoint: %v", err) }
	defer final.Close()
}


func TestRecoveryRemovesEmptyUncommittedDataSegmentOrphan(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(Options{Dir: dir, MaxBytes: 1 << 20, ReserveBytes: 4096, Sync: true})
	if err != nil { t.Fatal(err) }
	orphan := filepath.Join(dir, "data-00000000000000000002.wal")
	if err = os.WriteFile(orphan, nil, 0600); err != nil { t.Fatal(err) }
	if err = w.Close(); err != nil { t.Fatal(err) }
	reopened, err := Open(Options{Dir: dir, MaxBytes: 1 << 20, ReserveBytes: 4096, Sync: true})
	if err != nil { t.Fatal(err) }
	defer reopened.Close()
	if _, err = os.Stat(orphan); !os.IsNotExist(err) {
		t.Fatalf("empty uncommitted data segment was not cleaned: %v", err)
	}
}

func TestRecoveryFailsClosedOnNonEmptyUncommittedDataSegment(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(Options{Dir: dir, MaxBytes: 1 << 20, ReserveBytes: 4096, Sync: true})
	if err != nil { t.Fatal(err) }
	if err = w.Close(); err != nil { t.Fatal(err) }
	orphan := filepath.Join(dir, "data-00000000000000000002.wal")
	if err = os.WriteFile(orphan, []byte("uncommitted data"), 0600); err != nil { t.Fatal(err) }
	if _, err = Open(Options{Dir: dir, MaxBytes: 1 << 20, ReserveBytes: 4096, Sync: true}); err == nil || !strings.Contains(err.Error(), "uncommitted data segment") {
		t.Fatalf("non-empty orphan must fail closed, got %v", err)
	}
	if _, err = os.Stat(orphan); err != nil { t.Fatalf("non-empty orphan must be preserved for diagnosis: %v", err) }
}


func TestOpenAdoptsLegacyNonEmptyDataAndACKWithoutManifest(t *testing.T) {
	dir := t.TempDir()
	dataPath := filepath.Join(dir, "data-00000000000000000001.wal")
	dataFile, err := os.Create(dataPath)
	if err != nil { t.Fatal(err) }
	legacyRecord := Record{Seq: 1, EventID: "legacy-no-manifest", Payload: json.RawMessage(`{"v":1}`), CreatedAt: time.Now().UTC()}
	raw, err := json.Marshal(legacyRecord)
	if err != nil { t.Fatal(err) }
	if err = writeFrame(dataFile, raw); err != nil { t.Fatal(err) }
	if err = syncFile(dataFile); err != nil { t.Fatal(err) }
	if err = dataFile.Close(); err != nil { t.Fatal(err) }

	ackFile, err := os.Create(filepath.Join(dir, "ack.wal"))
	if err != nil { t.Fatal(err) }
	ackRaw, err := json.Marshal(Ack{Seq: 1, Sink: "mqtt", EventID: legacyRecord.EventID})
	if err != nil { t.Fatal(err) }
	if err = writeFrame(ackFile, ackRaw); err != nil { t.Fatal(err) }
	if err = syncFile(ackFile); err != nil { t.Fatal(err) }
	if err = ackFile.Close(); err != nil { t.Fatal(err) }

	w, err := Open(Options{Dir: dir, MaxBytes: 1 << 20, ReserveBytes: 4096, Sync: true})
	if err != nil { t.Fatalf("adopt legacy WAL without manifest: %v", err) }
	defer w.Close()
	if w.manifest.Version != 6 || w.next != 2 { t.Fatalf("legacy WAL was not adopted: manifest=%+v next=%d", w.manifest, w.next) }
	pending, err := w.PendingWithAcks()
	if err != nil { t.Fatal(err) }
	if len(pending) != 1 || pending[0].Seq != 1 || !pending[0].MQTTAcked || pending[0].InfluxAcked {
		t.Fatalf("legacy per-sink ACK state was not recovered: %+v", pending)
	}
	record, err := w.Append("after-legacy-adoption", json.RawMessage(`{"v":2}`))
	if err != nil { t.Fatal(err) }
	if record.Seq != 2 { t.Fatalf("next sequence=%d, want 2", record.Seq) }
}


func TestCheckpointSnapshotPreservesPendingAndIndependentACKState(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(Options{Dir: dir, SegmentBytes: 1, MaxBytes: 8 << 20, ReserveBytes: 4096, Sync: true})
	if err != nil { t.Fatal(err) }
	records := make([]Record, 0, 3)
	for i := 0; i < 3; i++ {
		record, appendErr := w.Append(fmt.Sprintf("snapshot-%d", i+1), json.RawMessage(fmt.Sprintf(`{"n":%d}`, i+1)))
		if appendErr != nil { t.Fatal(appendErr) }
		records = append(records, record)
	}
	if err = w.Ack(Ack{Seq: records[0].Seq, Sink: "mqtt", EventID: records[0].EventID}); err != nil { t.Fatal(err) }

	if target, checkpointErr := w.CheckpointReady(); checkpointErr != nil || target != 0 {
		t.Fatalf("pending prefix checkpoint target=%d err=%v", target, checkpointErr)
	}
	if w.manifest.Version != 6 || w.manifest.CheckpointFile == "" || w.manifest.CheckpointSeq != 0 {
		t.Fatalf("expected v6 pending snapshot at zero prefix: %+v", w.manifest)
	}
	firstFile := w.manifest.CheckpointFile
	pending, err := w.PendingWithAcks()
	if err != nil { t.Fatal(err) }
	if len(pending) != 3 || !pending[0].MQTTAcked || pending[0].InfluxAcked || pending[1].MQTTAcked || pending[1].InfluxAcked {
		t.Fatalf("incorrect initial pending ACK snapshot: %+v", pending)
	}
	if err = w.Close(); err != nil { t.Fatal(err) }

	w, err = Open(Options{Dir: dir, MaxBytes: 8 << 20, ReserveBytes: 4096, Sync: true})
	if err != nil { t.Fatalf("reopen pending snapshot: %v", err) }
	if pending, err = w.PendingWithAcks(); err != nil { t.Fatal(err) }
	if len(pending) != 3 || !pending[0].MQTTAcked || pending[0].InfluxAcked {
		t.Fatalf("per-sink state did not survive reopen: %+v", pending)
	}

	if err = w.Ack(Ack{Seq: records[0].Seq, Sink: "influx", EventID: records[0].EventID}); err != nil { t.Fatal(err) }
	if target, checkpointErr := w.CheckpointReady(); checkpointErr != nil || target != 1 {
		t.Fatalf("advance acknowledged prefix target=%d err=%v", target, checkpointErr)
	}
	if w.manifest.CheckpointSeq != 1 { t.Fatalf("checkpoint prefix=%d, want 1", w.manifest.CheckpointSeq) }
	if w.manifest.CheckpointFile == firstFile { t.Fatal("checkpoint generation did not advance") }
	pending, err = w.PendingWithAcks()
	if err != nil { t.Fatal(err) }
	if len(pending) != 2 || pending[0].Seq != 2 || pending[1].Seq != 3 {
		t.Fatalf("pending records not preserved after prefix advancement: %+v", pending)
	}
	if err = w.Close(); err != nil { t.Fatal(err) }

	w, err = Open(Options{Dir: dir, MaxBytes: 8 << 20, ReserveBytes: 4096, Sync: true})
	if err != nil { t.Fatalf("reopen advanced checkpoint: %v", err) }
	defer w.Close()
	pending, err = w.PendingWithAcks()
	if err != nil { t.Fatal(err) }
	if len(pending) != 2 || pending[0].Seq != 2 || pending[1].Seq != 3 {
		t.Fatalf("pending snapshot did not survive second reopen: %+v", pending)
	}
}

func TestCheckpointSnapshotRejectsDataMismatch(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(Options{Dir: dir, MaxBytes: 1 << 20, ReserveBytes: 4096, Sync: true})
	if err != nil { t.Fatal(err) }
	record, err := w.Append("snapshot-integrity", json.RawMessage(`{"value":1}`))
	if err != nil { t.Fatal(err) }
	if _, err = w.CheckpointReady(); err != nil { t.Fatal(err) }
	checkpointFile := filepath.Join(dir, w.manifest.CheckpointFile)
	raw, err := os.ReadFile(checkpointFile)
	if err != nil { t.Fatal(err) }
	var snapshot Checkpoint
	if err = json.Unmarshal(raw, &snapshot); err != nil { t.Fatal(err) }
	snapshot.Pending[0].Payload = json.RawMessage(`{"value":2}`)
	raw, err = json.Marshal(snapshot)
	if err != nil { t.Fatal(err) }
	if err = atomicFile(checkpointFile, raw); err != nil { t.Fatal(err) }
	// Rebind the checksum to ensure recovery detects semantic mismatch, not only a bad digest.
	if err = w.saveManifest(); err != nil { t.Fatal(err) }
	if err = w.Close(); err != nil { t.Fatal(err) }
	if _, err = Open(Options{Dir: dir, MaxBytes: 1 << 20, ReserveBytes: 4096, Sync: true}); err == nil {
		t.Fatal("recovery must reject a checkpoint payload that differs from the retained Data WAL")
	}
	_ = record
}


func TestWALCrashHelper(t *testing.T) {
	if os.Getenv("WAL_CRASH_HELPER") != "1" { return }
	dir := os.Getenv("WAL_CRASH_DIR")
	point := os.Getenv("WAL_CRASH_POINT")
	w, err := Open(Options{Dir: dir, SegmentBytes: 1, MaxBytes: 8 << 20, ReserveBytes: 4096, Sync: true})
	if err != nil { os.Exit(81) }
	w.faultHook = func(name string) { if name == point { os.Exit(86) } }
	switch os.Getenv("WAL_CRASH_MODE") {
	case "checkpoint":
		err = w.Checkpoint(1)
	case "ack":
		err = w.Ack(Ack{Seq: 1, Sink: "influx", EventID: "crash-event"})
	case "reclaim":
		err = w.Reclaim()
	default:
		os.Exit(82)
	}
	if err != nil { os.Exit(83) }
	os.Exit(84)
}

func runWALCrashProcess(t *testing.T, dir, mode, point string) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestWALCrashHelper$")
	cmd.Env = append(os.Environ(),
		"WAL_CRASH_HELPER=1",
		"WAL_CRASH_DIR="+dir,
		"WAL_CRASH_MODE="+mode,
		"WAL_CRASH_POINT="+point,
	)
	output, err := cmd.CombinedOutput()
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) || exitErr.ExitCode() != 86 {
		t.Fatalf("child did not terminate at crash point %q: err=%v output=%s", point, err, output)
	}
}

func prepareCrashCheckpointWAL(t *testing.T, dir string, segmentBytes int64) {
	t.Helper()
	w, err := Open(Options{Dir: dir, SegmentBytes: segmentBytes, MaxBytes: 8 << 20, ReserveBytes: 4096, Sync: true})
	if err != nil { t.Fatal(err) }
	first, err := w.Append("crash-event", json.RawMessage(`{"v":1}`))
	if err != nil { t.Fatal(err) }
	for _, sink := range []string{"mqtt", "influx"} {
		if err = w.Ack(Ack{Seq: first.Seq, Sink: sink, EventID: first.EventID}); err != nil { t.Fatal(err) }
	}
	if err = w.Close(); err != nil { t.Fatal(err) }
}

func TestWALProcessKillCheckpointManifestAndACKRotationBoundaries(t *testing.T) {
	t.Run("checkpoint file durable before manifest", func(t *testing.T) {
		dir := t.TempDir()
		prepareCrashCheckpointWAL(t, dir, 1<<20)
		runWALCrashProcess(t, dir, "checkpoint", "checkpoint.file.durable")
		w, err := Open(Options{Dir: dir, MaxBytes: 8 << 20, ReserveBytes: 4096, Sync: true})
		if err != nil { t.Fatal(err) }
		defer w.Close()
		if w.manifest.CheckpointSeq != 0 { t.Fatalf("uncommitted checkpoint advanced manifest: %+v", w.manifest) }
		if _, err = os.Stat(checkpointPath(dir, 1)); !os.IsNotExist(err) {
			t.Fatalf("uncommitted checkpoint generation was not cleaned: %v", err)
		}
	})

	t.Run("manifest renamed before directory sync", func(t *testing.T) {
		dir := t.TempDir()
		prepareCrashCheckpointWAL(t, dir, 1<<20)
		runWALCrashProcess(t, dir, "checkpoint", "manifest.renamed")
		w, err := Open(Options{Dir: dir, MaxBytes: 8 << 20, ReserveBytes: 4096, Sync: true})
		if err != nil { t.Fatalf("recovery after uncertain manifest rename: %v", err) }
		defer w.Close()
		if w.manifest.CheckpointSeq > 1 { t.Fatalf("manifest advanced beyond operation: %+v", w.manifest) }
		pending, err := w.PendingWithAcks()
		if err != nil { t.Fatal(err) }
		if len(pending) != 0 { t.Fatalf("fully ACKed event unexpectedly pending after recovery: %+v", pending) }
	})

	t.Run("checkpoint manifest committed", func(t *testing.T) {
		dir := t.TempDir()
		prepareCrashCheckpointWAL(t, dir, 1<<20)
		runWALCrashProcess(t, dir, "checkpoint", "checkpoint.manifest.committed")
		w, err := Open(Options{Dir: dir, MaxBytes: 8 << 20, ReserveBytes: 4096, Sync: true})
		if err != nil { t.Fatal(err) }
		defer w.Close()
		if w.manifest.CheckpointSeq != 1 { t.Fatalf("committed checkpoint lost: %+v", w.manifest) }
	})

	t.Run("ACK segment created before manifest", func(t *testing.T) {
		dir := t.TempDir()
		w, err := Open(Options{Dir: dir, SegmentBytes: 1, MaxBytes: 8 << 20, ReserveBytes: 4096, Sync: true})
		if err != nil { t.Fatal(err) }
		record, err := w.Append("crash-event", json.RawMessage(`{"v":1}`))
		if err != nil { t.Fatal(err) }
		if err = w.Ack(Ack{Seq: record.Seq, Sink: "mqtt", EventID: record.EventID}); err != nil { t.Fatal(err) }
		if err = w.Close(); err != nil { t.Fatal(err) }
		runWALCrashProcess(t, dir, "ack", "ack.rotate.segment.durable")
		w, err = Open(Options{Dir: dir, SegmentBytes: 1, MaxBytes: 8 << 20, ReserveBytes: 4096, Sync: true})
		if err != nil { t.Fatal(err) }
		defer w.Close()
		pending, err := w.PendingWithAcks()
		if err != nil { t.Fatal(err) }
		if len(pending) != 1 || !pending[0].MQTTAcked || pending[0].InfluxAcked {
			t.Fatalf("ACK rotation crash lost or invented ACK state: %+v", pending)
		}
		if _, err = os.Stat(ackSegmentPath(dir, 2)); !os.IsNotExist(err) {
			t.Fatalf("uncommitted ACK segment orphan was not removed: %v", err)
		}
	})
}

func TestWALProcessKillACKCompactionAndReclaimBoundaries(t *testing.T) {
	t.Run("ACK compaction after first manifest commit", func(t *testing.T) {
		dir := t.TempDir()
		w, err := Open(Options{Dir: dir, SegmentBytes: 1, MaxBytes: 8 << 20, ReserveBytes: 4096, Sync: true})
		if err != nil { t.Fatal(err) }
		first, err := w.Append("crash-event", json.RawMessage(`{"v":1}`))
		if err != nil { t.Fatal(err) }
		second, err := w.Append("crash-event-2", json.RawMessage(`{"v":2}`))
		if err != nil { t.Fatal(err) }
		for _, sink := range []string{"mqtt", "influx"} {
			if err = w.Ack(Ack{Seq: first.Seq, Sink: sink, EventID: first.EventID}); err != nil { t.Fatal(err) }
		}
		if err = w.Ack(Ack{Seq: second.Seq, Sink: "mqtt", EventID: second.EventID}); err != nil { t.Fatal(err) }
		if err = w.Close(); err != nil { t.Fatal(err) }
		runWALCrashProcess(t, dir, "checkpoint", "ack.compact.manifest.expanded")
		w, err = Open(Options{Dir: dir, SegmentBytes: 1, MaxBytes: 8 << 20, ReserveBytes: 4096, Sync: true})
		if err != nil { t.Fatal(err) }
		defer w.Close()
		if w.manifest.CheckpointSeq != 1 { t.Fatalf("checkpoint boundary lost after ACK compaction crash: %+v", w.manifest) }
		pending, err := w.PendingWithAcks()
		if err != nil { t.Fatal(err) }
		if len(pending) != 1 || pending[0].Seq != 2 || !pending[0].MQTTAcked || pending[0].InfluxAcked {
			t.Fatalf("ACK compaction crash lost independent ACK state: %+v", pending)
		}
	})

	t.Run("reclaim manifest committed before unlink", func(t *testing.T) {
		dir := t.TempDir()
		prepareCrashCheckpointWAL(t, dir, 1)
		w, err := Open(Options{Dir: dir, SegmentBytes: 1, MaxBytes: 8 << 20, ReserveBytes: 4096, Sync: true})
		if err != nil { t.Fatal(err) }
		if err = w.Checkpoint(1); err != nil { t.Fatal(err) }
		if err = w.Close(); err != nil { t.Fatal(err) }
		runWALCrashProcess(t, dir, "reclaim", "data.reclaim.manifest.committed")
		w, err = Open(Options{Dir: dir, SegmentBytes: 1, MaxBytes: 8 << 20, ReserveBytes: 4096, Sync: true})
		if err != nil { t.Fatal(err) }
		defer w.Close()
		if w.manifest.FirstSegment < 2 { t.Fatalf("reclaim boundary was not recovered: %+v", w.manifest) }
		if _, err = os.Stat(filepath.Join(dir, "data-00000000000000000001.wal")); !os.IsNotExist(err) {
			t.Fatalf("recovery did not finish committed Data segment deletion: %v", err)
		}
	})
}


func TestACKWriteENOSPCReleasesReserveRepairsTailAndRetries(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(Options{Dir: dir, SegmentBytes: 1 << 20, MaxBytes: 8 << 20, ReserveBytes: 64 << 10, Sync: true})
	if err != nil { t.Fatal(err) }
	defer w.Close()
	record, err := w.Append("ack-enospc-write", json.RawMessage(`{"v":1}`))
	if err != nil { t.Fatal(err) }
	calls := 0
	w.ackWriteHook = func(file *os.File, payload []byte) error {
		calls++
		if calls == 1 {
			if _, writeErr := file.Write([]byte{0, 0}); writeErr != nil { return writeErr }
			return syscall.ENOSPC
		}
		return writeFrame(file, payload)
	}
	if err = w.Ack(Ack{Seq: record.Seq, Sink: "mqtt", EventID: record.EventID}); err != nil { t.Fatalf("ACK should succeed after reserve release: %v", err) }
	if calls != 2 { t.Fatalf("ACK write attempts=%d, want 2", calls) }
	if _, err = os.Stat(filepath.Join(dir, "ack.reserve")); !os.IsNotExist(err) { t.Fatalf("reserve should be released after ENOSPC: %v", err) }
	state, err := w.ackState()
	if err != nil { t.Fatal(err) }
	if !state["mqtt"][record.Seq] { t.Fatal("ACK was not durable after retry") }
}

func TestACKSyncENOSPCReleasesReserveAndRetriesSync(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(Options{Dir: dir, MaxBytes: 8 << 20, ReserveBytes: 64 << 10, Sync: true})
	if err != nil { t.Fatal(err) }
	defer w.Close()
	record, err := w.Append("ack-enospc-sync", json.RawMessage(`{"v":1}`))
	if err != nil { t.Fatal(err) }
	calls := 0
	w.ackSyncHook = func(file *os.File) error {
		calls++
		if calls == 1 { return syscall.ENOSPC }
		return syncFile(file)
	}
	if err = w.Ack(Ack{Seq: record.Seq, Sink: "influx", EventID: record.EventID}); err != nil { t.Fatalf("ACK should succeed after reserve release: %v", err) }
	if calls != 2 { t.Fatalf("ACK sync attempts=%d, want 2", calls) }
	state, err := w.ackState()
	if err != nil { t.Fatal(err) }
	if !state["influx"][record.Seq] { t.Fatal("ACK was not durable after sync retry") }
}

func TestACKReserveExhaustionNeverReportsSuccess(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(Options{Dir: dir, MaxBytes: 8 << 20, ReserveBytes: 64 << 10, Sync: true})
	if err != nil { t.Fatal(err) }
	defer w.Close()
	record, err := w.Append("ack-reserve-exhausted", json.RawMessage(`{"v":1}`))
	if err != nil { t.Fatal(err) }
	attempts := 0
	w.ackWriteHook = func(_ *os.File, _ []byte) error { attempts++; return syscall.ENOSPC }
	if err = w.Ack(Ack{Seq: record.Seq, Sink: "mqtt", EventID: record.EventID}); !errors.Is(err, syscall.ENOSPC) {
		t.Fatalf("ACK must report reserve exhaustion, got %v", err)
	}
	if attempts != 2 { t.Fatalf("ACK write attempts=%d, want 2", attempts) }
	state, err := w.ackState()
	if err != nil { t.Fatal(err) }
	if state["mqtt"][record.Seq] { t.Fatal("failed ACK was incorrectly marked durable") }
}

func TestACKReserveReleaseFailureNeverReportsSuccess(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(Options{Dir: dir, MaxBytes: 8 << 20, ReserveBytes: 4096, Sync: true})
	if err != nil { t.Fatal(err) }
	defer w.Close()
	record, err := w.Append("ack-reserve-release-failure", json.RawMessage(`{"v":1}`))
	if err != nil { t.Fatal(err) }
	w.ackWriteHook = func(_ *os.File, _ []byte) error { return syscall.ENOSPC }
	w.reserveReleaseHook = func(string) error { return errors.New("simulated reserve release failure") }
	if err = w.Ack(Ack{Seq: record.Seq, Sink: "influx", EventID: record.EventID}); err == nil {
		t.Fatal("ACK must fail if emergency reserve cannot be released")
	}
	state, err := w.ackState()
	if err != nil { t.Fatal(err) }
	if state["influx"][record.Seq] { t.Fatal("failed ACK was incorrectly marked durable") }
}


func TestWALBacklogSnapshotRecoveryStress(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(Options{Dir: dir, SegmentBytes: 32 << 10, MaxBytes: 64 << 20, ReserveBytes: 1 << 20, Sync: true})
	if err != nil { t.Fatal(err) }
	const total = 3000
	for start := 0; start < total; start += 100 {
		batch := make([]AppendInput, 0, 100)
		for i := start; i < start+100; i++ {
			batch = append(batch, AppendInput{EventID: fmt.Sprintf("backlog-%06d", i+1), Payload: json.RawMessage(fmt.Sprintf(`{"index":%d,"padding":"%0128d"}`, i+1, i))})
		}
		if _, err = w.AppendBatch(batch); err != nil { t.Fatalf("append batch at %d: %v", start, err) }
	}
	first, second := uint64(1), uint64(2)
	if err = w.Ack(Ack{Seq: first, Sink: "mqtt", EventID: "backlog-000001"}); err != nil { t.Fatal(err) }
	if err = w.Ack(Ack{Seq: first, Sink: "influx", EventID: "backlog-000001"}); err != nil { t.Fatal(err) }
	if err = w.Ack(Ack{Seq: second, Sink: "mqtt", EventID: "backlog-000002"}); err != nil { t.Fatal(err) }
	if target, err := w.CheckpointReady(); err != nil || target != 1 {
		t.Fatalf("checkpoint prefix=%d err=%v", target, err)
	}
	pending, err := w.PendingWithAcks()
	if err != nil { t.Fatal(err) }
	if len(pending) != total-1 || pending[0].Seq != 2 || !pending[0].MQTTAcked || pending[0].InfluxAcked {
		t.Fatalf("backlog snapshot has wrong pending state: count=%d first=%+v", len(pending), pending[0])
	}
	stats, err := w.Stats()
	if err != nil { t.Fatal(err) }
	if stats.SegmentCount < 2 || stats.PendingRecords != total-1 {
		t.Fatalf("unexpected backlog storage stats: %+v", stats)
	}
	if err = w.Close(); err != nil { t.Fatal(err) }

	w, err = Open(Options{Dir: dir, SegmentBytes: 32 << 10, MaxBytes: 64 << 20, ReserveBytes: 1 << 20, Sync: true})
	if err != nil { t.Fatalf("recover large pending snapshot: %v", err) }
	defer w.Close()
	pending, err = w.PendingWithAcks()
	if err != nil { t.Fatal(err) }
	if len(pending) != total-1 || pending[0].Seq != 2 || !pending[0].MQTTAcked || pending[0].InfluxAcked {
		t.Fatalf("large pending backlog changed across recovery: count=%d first=%+v", len(pending), pending[0])
	}
	if _, err = w.CheckpointReady(); err != nil { t.Fatalf("idempotent checkpoint refresh: %v", err) }
}

func BenchmarkWALBacklogSnapshotRecovery(b *testing.B) {
	for n := 0; n < b.N; n++ {
		dir := b.TempDir()
		w, err := Open(Options{Dir: dir, SegmentBytes: 1 << 20, MaxBytes: 128 << 20, ReserveBytes: 1 << 20, Sync: true})
		if err != nil { b.Fatal(err) }
		for start := 0; start < 1000; start += 100 {
			batch := make([]AppendInput, 0, 100)
			for i := start; i < start+100; i++ {
				batch = append(batch, AppendInput{EventID: fmt.Sprintf("bench-%d", i), Payload: json.RawMessage(`{"temperature":23.5,"pressure":101.3}`)})
			}
			if _, err = w.AppendBatch(batch); err != nil { b.Fatal(err) }
		}
		first := "bench-0"
		for _, sink := range []string{"mqtt", "influx"} {
			if err = w.Ack(Ack{Seq: 1, Sink: sink, EventID: first}); err != nil { b.Fatal(err) }
		}
		if _, err = w.CheckpointReady(); err != nil { b.Fatal(err) }
		if err = w.Close(); err != nil { b.Fatal(err) }
		w, err = Open(Options{Dir: dir, MaxBytes: 128 << 20, ReserveBytes: 1 << 20, Sync: true})
		if err != nil { b.Fatal(err) }
		if err = w.Close(); err != nil { b.Fatal(err) }
		if err = os.RemoveAll(dir); err != nil { b.Fatal(err) }
	}
}


func TestWALProcessKillAfterACKCompactionBoundaryAndDuringPartialDeletion(t *testing.T) {
	t.Run("ACK lower boundary committed before old segment unlink", func(t *testing.T) {
		dir := t.TempDir()
		w, err := Open(Options{Dir: dir, SegmentBytes: 1, MaxBytes: 8 << 20, ReserveBytes: 4096, Sync: true})
		if err != nil { t.Fatal(err) }
		first, err := w.Append("crash-event", json.RawMessage(`{"v":1}`))
		if err != nil { t.Fatal(err) }
		second, err := w.Append("crash-event-2", json.RawMessage(`{"v":2}`))
		if err != nil { t.Fatal(err) }
		for _, sink := range []string{"mqtt", "influx"} {
			if err = w.Ack(Ack{Seq: first.Seq, Sink: sink, EventID: first.EventID}); err != nil { t.Fatal(err) }
		}
		if err = w.Ack(Ack{Seq: second.Seq, Sink: "mqtt", EventID: second.EventID}); err != nil { t.Fatal(err) }
		if err = w.Close(); err != nil { t.Fatal(err) }
		runWALCrashProcess(t, dir, "checkpoint", "ack.compact.manifest.narrowed")
		w, err = Open(Options{Dir: dir, SegmentBytes: 1, MaxBytes: 8 << 20, ReserveBytes: 4096, Sync: true})
		if err != nil { t.Fatal(err) }
		defer w.Close()
		if w.manifest.AckFirstSegment != w.manifest.AckActiveSegment {
			t.Fatalf("ACK lower boundary was not recovered: %+v", w.manifest)
		}
		pending, err := w.PendingWithAcks()
		if err != nil { t.Fatal(err) }
		if len(pending) != 1 || pending[0].Seq != 2 || !pending[0].MQTTAcked || pending[0].InfluxAcked {
			t.Fatalf("ACK state changed after old-segment deletion recovery: %+v", pending)
		}
		if _, err = os.Stat(ackSegmentPath(dir, 1)); !os.IsNotExist(err) {
			t.Fatalf("obsolete ACK segment was not removed during recovery: %v", err)
		}
	})

	t.Run("partial Data segment unlink resumes after restart", func(t *testing.T) {
		dir := t.TempDir()
		w, err := Open(Options{Dir: dir, SegmentBytes: 1, MaxBytes: 8 << 20, ReserveBytes: 4096, Sync: true})
		if err != nil { t.Fatal(err) }
		for i := 1; i <= 3; i++ {
			record, appendErr := w.Append(fmt.Sprintf("delete-%d", i), json.RawMessage(fmt.Sprintf(`{"v":%d}`, i)))
			if appendErr != nil { t.Fatal(appendErr) }
			for _, sink := range []string{"mqtt", "influx"} {
				if err = w.Ack(Ack{Seq: record.Seq, Sink: sink, EventID: record.EventID}); err != nil { t.Fatal(err) }
			}
		}
		if err = w.Checkpoint(3); err != nil { t.Fatal(err) }
		if err = w.Close(); err != nil { t.Fatal(err) }
		runWALCrashProcess(t, dir, "reclaim", "data.reclaim.segment.deleted")
		w, err = Open(Options{Dir: dir, SegmentBytes: 1, MaxBytes: 8 << 20, ReserveBytes: 4096, Sync: true})
		if err != nil { t.Fatalf("recover partial segment deletion: %v", err) }
		defer w.Close()
		if w.manifest.FirstSegment < 4 { t.Fatalf("reclaim did not preserve committed deletion boundary: %+v", w.manifest) }
		for id := uint64(1); id <= 3; id++ {
			if _, statErr := os.Stat(filepath.Join(dir, fmt.Sprintf("data-%020d.wal", id))); !os.IsNotExist(statErr) {
				t.Fatalf("obsolete segment %d remains after recovery: %v", id, statErr)
			}
		}
	})
}


func TestWALProcessKillCleansUncommittedMetadataTemps(t *testing.T) {
	t.Run("manifest temp synced before rename", func(t *testing.T) {
		dir := t.TempDir()
		prepareCrashCheckpointWAL(t, dir, 1<<20)
		runWALCrashProcess(t, dir, "checkpoint", "manifest.tmp.synced")
		w, err := Open(Options{Dir: dir, MaxBytes: 8 << 20, ReserveBytes: 4096, Sync: true})
		if err != nil { t.Fatal(err) }
		defer w.Close()
		if w.manifest.CheckpointSeq != 0 { t.Fatalf("unrenamed manifest temp changed committed state: %+v", w.manifest) }
		for _, path := range []string{filepath.Join(dir, "MANIFEST.tmp"), checkpointPath(dir, 1)} {
			if _, err = os.Stat(path); !os.IsNotExist(err) { t.Fatalf("uncommitted metadata temp/orphan remains at %s: %v", path, err) }
		}
	})

	t.Run("ACK compact temp synced before rename", func(t *testing.T) {
		dir := t.TempDir()
		w, err := Open(Options{Dir: dir, SegmentBytes: 1, MaxBytes: 8 << 20, ReserveBytes: 4096, Sync: true})
		if err != nil { t.Fatal(err) }
		first, err := w.Append("crash-event", json.RawMessage(`{"v":1}`))
		if err != nil { t.Fatal(err) }
		second, err := w.Append("crash-event-2", json.RawMessage(`{"v":2}`))
		if err != nil { t.Fatal(err) }
		for _, sink := range []string{"mqtt", "influx"} {
			if err = w.Ack(Ack{Seq: first.Seq, Sink: sink, EventID: first.EventID}); err != nil { t.Fatal(err) }
		}
		if err = w.Ack(Ack{Seq: second.Seq, Sink: "mqtt", EventID: second.EventID}); err != nil { t.Fatal(err) }
		if err = w.Close(); err != nil { t.Fatal(err) }
		runWALCrashProcess(t, dir, "checkpoint", "ack.compact.segment.durable")
		w, err = Open(Options{Dir: dir, SegmentBytes: 1, MaxBytes: 8 << 20, ReserveBytes: 4096, Sync: true})
		if err != nil { t.Fatal(err) }
		defer w.Close()
		pending, err := w.PendingWithAcks()
		if err != nil { t.Fatal(err) }
		if len(pending) != 1 || pending[0].Seq != 2 || !pending[0].MQTTAcked || pending[0].InfluxAcked {
			t.Fatalf("ACK state changed after compact-temp crash: %+v", pending)
		}
		if _, err = os.Stat(ackSegmentPath(dir, 4)+".tmp"); !os.IsNotExist(err) {
			t.Fatalf("uncommitted ACK compaction temp remains: %v", err)
		}
	})
}
