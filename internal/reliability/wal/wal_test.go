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
