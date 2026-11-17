package wal

import (
 "encoding/json"
 "errors"
 "os"
 "path/filepath"
 "strings"
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
 f,err:=os.OpenFile(filepath.Join(dir,"ack.wal"),os.O_APPEND|os.O_WRONLY,0600);if err!=nil{t.Fatal(err)}
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
 before,err:=os.Stat(filepath.Join(dir,"ack.wal"));if err!=nil{t.Fatal(err)}
 if before.Size()==0{t.Fatal("test setup expected un-compacted ACK log")}
 if _,err=w.CheckpointReady();err!=nil{t.Fatal(err)}
 after,err:=os.Stat(filepath.Join(dir,"ack.wal"));if err!=nil{t.Fatal(err)}
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
	before, err := os.Stat(filepath.Join(dir, "ack.wal"))
	if err != nil { t.Fatal(err) }
	for i := 0; i < 10; i++ {
		if err = w.Ack(ack); err != nil { t.Fatal(err) }
	}
	after, err := os.Stat(filepath.Join(dir, "ack.wal"))
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


func TestManifestV3ChecksumRejectsTampering(t *testing.T) {
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
	if m.Version != 3 || m.Checksum == "" { t.Fatalf("expected checksummed v2 manifest, got %+v", m) }
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


func TestManifestV2UpgradesToV3OnCommit(t *testing.T) {
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
	if upgraded.Version != 3 || upgraded.Checksum == "" { t.Fatalf("v2 manifest was not upgraded: %+v", upgraded) }
	reopened, err := Open(Options{Dir: dir, MaxBytes: 1 << 20, ReserveBytes: 4096, Sync: true})
	if err != nil { t.Fatalf("reopen v3 manifest: %v", err) }
	if err = reopened.Close(); err != nil { t.Fatal(err) }
}

func TestLegacyManifestUpgradesToV2OnCommit(t *testing.T) {
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
	if upgraded.Version != 3 || upgraded.Checksum == "" { t.Fatalf("legacy manifest was not upgraded: %+v", upgraded) }
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
