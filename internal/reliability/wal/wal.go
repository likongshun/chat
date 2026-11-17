package wal

import (
 "encoding/json"
 "errors"
 "fmt"
 "io"
 "os"
 "path/filepath"
 "sort"
 "strings"
 "sync"
	"sync/atomic"
 "syscall"
 "time"
)

type WAL struct { mu sync.Mutex; opts Options; lock *fileLock; data *os.File; ack *os.File; manifest Manifest; next uint64; closed bool; failed error; capacityRejections atomic.Uint64 }
func Open(o Options)(*WAL,error){
 if o.Dir==""{return nil,fmt.Errorf("WAL dir required")};if o.SegmentBytes<=0{o.SegmentBytes=64<<20};if o.MaxBytes<=0{o.MaxBytes=10<<30};if o.ReserveBytes<0||o.ReserveBytes>=o.MaxBytes{return nil,fmt.Errorf("reserve must be >=0 and less than max bytes")}
 if err:=os.MkdirAll(o.Dir,0700);err!=nil{return nil,err};l,err:=acquireLock(o.Dir);if err!=nil{return nil,err}
 w:=&WAL{opts:o,lock:l,next:1};fail:=func(e error)(*WAL,error){l.close();return nil,e}
 if err=ensureAckReserve(o.Dir,o.ReserveBytes);err!=nil&&!errors.Is(err,syscall.ENOSPC){return fail(err)}
 if err=w.loadManifest();err!=nil{return fail(err)};if err=w.repairActiveTails();err!=nil{return fail(err)};if err=w.recover();err!=nil{return fail(err)};if err=w.openActive();err!=nil{return fail(err)};return w,nil
}
func (w *WAL) loadManifest()error{
 b,e:=os.ReadFile(filepath.Join(w.opts.Dir,"MANIFEST"));if os.IsNotExist(e){w.manifest=Manifest{Version:1,FirstSegment:1,ActiveSegment:1};return nil};if e!=nil{return e}
 if e=json.Unmarshal(b,&w.manifest);e!=nil{return fmt.Errorf("manifest: %w",e)};if w.manifest.Version!=1{return fmt.Errorf("unsupported manifest version %d",w.manifest.Version)};if w.manifest.FirstSegment==0||w.manifest.ActiveSegment<w.manifest.FirstSegment{return fmt.Errorf("invalid manifest segment range")};if w.manifest.CheckpointSeq>0{cpPath:=checkpointPath(w.opts.Dir,w.manifest.CheckpointSeq);cp,e:=os.ReadFile(cpPath);if os.IsNotExist(e){cp,e=os.ReadFile(filepath.Join(w.opts.Dir,"checkpoint.json"))};if e!=nil{return fmt.Errorf("committed checkpoint missing: %w",e)};var checkpoint Checkpoint;if e=json.Unmarshal(cp,&checkpoint);e!=nil{return fmt.Errorf("decode committed checkpoint: %w",e)};if checkpoint.Seq!=w.manifest.CheckpointSeq{return fmt.Errorf("checkpoint sequence %d does not match manifest %d",checkpoint.Seq,w.manifest.CheckpointSeq)}};return nil
}
func (w *WAL) repairActiveTails()error{
 paths:=[]string{
  filepath.Join(w.opts.Dir,fmt.Sprintf("data-%020d.wal",w.manifest.ActiveSegment)),
  filepath.Join(w.opts.Dir,"ack.wal"),
 }
 for _,p:=range paths{
  f,e:=os.OpenFile(p,os.O_CREATE|os.O_RDWR,0600);if e!=nil{return e}
  if e=f.Close();e!=nil{return e}
  if e=repairFrameTail(p);e!=nil{return e}
 }
 return nil
}

func (w *WAL) recover()error{
 if e:=w.cleanupObsoleteSegments();e!=nil{return e}
 max:=w.manifest.CheckpointSeq
 eventIDs:=make(map[uint64]string)
 for seg:=w.manifest.FirstSegment;seg<=w.manifest.ActiveSegment;seg++{
  p:=filepath.Join(w.opts.Dir,fmt.Sprintf("data-%020d.wal",seg))
  if _,e:=os.Stat(p);e!=nil{return fmt.Errorf("required data segment %d is unavailable: %w",seg,e)}
  if e:=readFrames(p,func(b []byte)error{var r Record;if e:=json.Unmarshal(b,&r);e!=nil{return fmt.Errorf("decode data record in %s: %w",p,e)};if r.Seq==0||r.EventID==""||len(r.Payload)==0{return fmt.Errorf("invalid data record in %s",p)};if r.Seq>max{max=r.Seq};if r.Seq>w.manifest.CheckpointSeq{if _,ok:=eventIDs[r.Seq];ok{return fmt.Errorf("duplicate WAL sequence %d",r.Seq)};eventIDs[r.Seq]=r.EventID};return nil});e!=nil{return e}
  if seg==^uint64(0){break}
 }
 if max > w.manifest.CheckpointSeq {
		for seq := w.manifest.CheckpointSeq + 1; seq <= max; seq++ {
			if _, ok := eventIDs[seq]; !ok {
				return fmt.Errorf("non-contiguous WAL sequence: missing %d before %d", seq, max)
			}
			if seq == ^uint64(0) { break }
		}
	}
	if _,e:=w.ackState();e!=nil{return fmt.Errorf("recover ACK log: %w",e)}
 if e:=readFrames(filepath.Join(w.opts.Dir,"ack.wal"),func(b []byte)error{var a Ack;if e:=json.Unmarshal(b,&a);e!=nil{return e};if a.Seq<=w.manifest.CheckpointSeq{return nil};id,ok:=eventIDs[a.Seq];if !ok{return fmt.Errorf("ACK references unknown sequence %d during recovery",a.Seq)};if id!=a.EventID{return fmt.Errorf("ACK event_id mismatch for sequence %d during recovery",a.Seq)};return nil});e!=nil{return fmt.Errorf("validate ACK references: %w",e)}
 w.next=max+1;if w.next==0{return fmt.Errorf("WAL sequence exhausted")};return nil
}
func (w *WAL) openActive()error{
 p:=filepath.Join(w.opts.Dir,fmt.Sprintf("data-%020d.wal",w.manifest.ActiveSegment));f,e:=os.OpenFile(p,os.O_CREATE|os.O_RDWR|os.O_APPEND,0600);if e!=nil{return e}
 if e=repairFrameTail(p);e!=nil{f.Close();return e};w.data=f
 ackPath:=filepath.Join(w.opts.Dir,"ack.wal")
 w.ack,e=os.OpenFile(ackPath,os.O_CREATE|os.O_RDWR|os.O_APPEND,0600);if e!=nil{f.Close();return e}
 if e=repairFrameTail(ackPath);e!=nil{f.Close();w.ack.Close();return e};return nil
}
func (w *WAL) diskUsage()(int64,error){var total int64;e:=filepath.Walk(w.opts.Dir,func(_ string,i os.FileInfo,e error)error{if e!=nil{return e};if !i.IsDir()&&i.Name()!="ack.reserve"{total+=i.Size()};return nil});return total,e}
func (w *WAL) Append(eventID string,payload json.RawMessage)(Record,error){
 records,e:=w.AppendBatch([]AppendInput{{EventID:eventID,Payload:payload}})
 if e!=nil{return Record{},e}
 return records[0],nil
}

// AppendBatch assigns a contiguous sequence range and performs one data fsync
// for the entire batch. A failed write is rolled back to the pre-batch offset;
// if rollback itself fails, the WAL becomes fail-stop until reopened.
func (w *WAL) AppendBatch(events []AppendInput)([]Record,error){
 w.mu.Lock();defer w.mu.Unlock()
 if w.closed{return nil,os.ErrClosed}
 if w.failed!=nil{return nil,fmt.Errorf("WAL is fail-stopped: %w",w.failed)}
 if len(events)==0{return nil,nil}
 if uint64(len(events))>^uint64(0)-w.next{return nil,fmt.Errorf("WAL sequence exhausted")}
 var estimate int64
 for i,event:=range events{
  if event.EventID==""{return nil,fmt.Errorf("event %d: event_id is required",i)}
  if len(event.Payload)==0||!json.Valid(event.Payload){return nil,fmt.Errorf("event %d: payload must be valid JSON",i)}
  estimate+=int64(len(event.Payload))+256
 }
 usage,e:=w.diskUsage();if e!=nil{return nil,e}
 if usage+estimate>w.opts.MaxBytes-w.opts.ReserveBytes{w.capacityRejections.Add(1);return nil,ErrCapacity}
 if w.data==nil{return nil,os.ErrClosed}
 start,e:=w.data.Seek(0,io.SeekEnd);if e!=nil{return nil,e}
 records:=make([]Record,0,len(events))
 rollback:=func(cause error)([]Record,error){
  if err:=w.data.Truncate(start);err!=nil{w.failed=errors.Join(cause,fmt.Errorf("rollback WAL batch: %w",err));return nil,w.failed}
  if err:=w.data.Sync();err!=nil{w.failed=errors.Join(cause,fmt.Errorf("sync WAL rollback: %w",err));return nil,w.failed}
  if _,err:=w.data.Seek(0,io.SeekEnd);err!=nil{w.failed=errors.Join(cause,fmt.Errorf("seek WAL after rollback: %w",err));return nil,w.failed}
  return nil,cause
 }
 for i,event:=range events{
  record:=Record{Seq:w.next+uint64(i),EventID:event.EventID,Payload:event.Payload,CreatedAt:time.Now().UTC()}
  encoded,err:=json.Marshal(record);if err!=nil{return rollback(err)}
  if err=writeFrame(w.data,encoded);err!=nil{return rollback(err)}
  records=append(records,record)
 }
 if w.opts.Sync{
  if e=w.data.Sync();e!=nil{return rollback(e)}
 }
 w.next+=uint64(len(records))
 st,e:=w.data.Stat();if e!=nil{return records,e}
 if st.Size()>=w.opts.SegmentBytes{
  if e=w.rotate();e!=nil{w.failed=fmt.Errorf("segment rotation outcome is uncertain: %w",e);return records,fmt.Errorf("records committed but WAL entered fail-stop after rotation failure: %w",e)}
 }
 return records,nil
}
func (w *WAL) rotate() error {
	if w.data == nil {
		return os.ErrClosed
	}
	if w.manifest.ActiveSegment == ^uint64(0) {
		return fmt.Errorf("WAL segment sequence exhausted")
	}
	if err := w.data.Sync(); err != nil {
		return err
	}

	// Publish the new segment's directory entry before MANIFEST can refer to it.
	// A crash before the manifest commit leaves only an ignored orphan segment;
	// a crash after the commit leaves a segment that recovery can always open.
	oldID := w.manifest.ActiveSegment
	newID := oldID + 1
	newPath := filepath.Join(w.opts.Dir, fmt.Sprintf("data-%020d.wal", newID))
	created, err := os.OpenFile(newPath, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil && !os.IsExist(err) {
		return fmt.Errorf("create next WAL segment: %w", err)
	}
	if os.IsExist(err) {
		// An empty file may be an orphan from a crash before MANIFEST commit.
		created, err = os.OpenFile(newPath, os.O_RDWR, 0600)
		if err != nil {
			return err
		}
		st, statErr := created.Stat()
		if statErr != nil {
			_ = created.Close()
			return statErr
		}
		if st.Size() != 0 {
			_ = created.Close()
			return fmt.Errorf("uncommitted next segment %d is not empty", newID)
		}
	}
	if err = created.Sync(); err != nil {
		_ = created.Close()
		return err
	}
	if err = created.Close(); err != nil {
		return err
	}
	dir, err := os.Open(w.opts.Dir)
	if err != nil {
		return err
	}
	err = dir.Sync()
	_ = dir.Close()
	if err != nil {
		return fmt.Errorf("sync directory after creating segment %d: %w", newID, err)
	}

	w.manifest.ActiveSegment = newID
	if err = w.saveManifest(); err != nil {
		w.manifest.ActiveSegment = oldID
		return fmt.Errorf("commit active segment %d: %w", newID, err)
	}

	// MANIFEST is now durable. Switch handles only after the commit.
	if err = w.data.Close(); err != nil {
		w.data = nil
		return fmt.Errorf("manifest committed to segment %d but closing old segment failed: %w", newID, err)
	}
	w.data = nil
	if err = w.openDataOnly(); err != nil {
		return fmt.Errorf("manifest committed to segment %d but opening it failed: %w", newID, err)
	}
	return nil
}
func (w *WAL) openDataOnly()error{p:=filepath.Join(w.opts.Dir,fmt.Sprintf("data-%020d.wal",w.manifest.ActiveSegment));f,e:=os.OpenFile(p,os.O_CREATE|os.O_RDWR|os.O_APPEND,0600);w.data=f;return e}
func (w *WAL) Ack(a Ack)error{
 w.mu.Lock();defer w.mu.Unlock()
 if w.closed||w.ack==nil{return os.ErrClosed}
 if a.Sink!="mqtt"&&a.Sink!="influx"{return fmt.Errorf("unsupported sink %q",a.Sink)}
 if a.Seq==0||a.EventID==""{return fmt.Errorf("ACK sequence and event_id are required")}
 // ACK retries are idempotent: avoid growing the append-only ACK log with
 // duplicate confirmations for the same sink and sequence.
 state,err:=w.ackState();if err!=nil{return err}
 if state[a.Sink][a.Seq]{return nil}
 found:=false
 files,e:=filepath.Glob(filepath.Join(w.opts.Dir,"data-*.wal"));if e!=nil{return e};sort.Strings(files)
 for _,p:=range files{
  var seg uint64;_,_=fmt.Sscanf(filepath.Base(p),"data-%d.wal",&seg)
  if seg<w.manifest.FirstSegment||seg>w.manifest.ActiveSegment{continue}
  e=readFrames(p,func(b []byte)error{var r Record;if e:=json.Unmarshal(b,&r);e!=nil{return e};if r.Seq==a.Seq{if r.EventID!=a.EventID{return fmt.Errorf("ACK event_id mismatch for seq %d",a.Seq)};found=true};return nil})
  if e!=nil{return e};if found{break}
 }
 if !found{return fmt.Errorf("ACK references unknown or checkpointed seq %d",a.Seq)}
 // An ACK is considered committed only after the log sync succeeds.
 b,e:=json.Marshal(a);if e!=nil{return e}
 ackPath:=filepath.Join(w.opts.Dir,"ack.wal")
 if e=writeFrame(w.ack,b);e!=nil{
  if !errors.Is(e,syscall.ENOSPC){return e}
  if e=releaseAckReserve(w.opts.Dir);e!=nil{return fmt.Errorf("ACK write failed and reserve release failed: %w",e)}
  if e=repairFrameTail(ackPath);e!=nil{return e}
  if _,e=w.ack.Seek(0,io.SeekEnd);e!=nil{return e}
  if e=writeFrame(w.ack,b);e!=nil{return e}
 }
 if e=w.ack.Sync();e!=nil{
  if !errors.Is(e,syscall.ENOSPC){return e}
  if e=releaseAckReserve(w.opts.Dir);e!=nil{return fmt.Errorf("ACK sync failed and reserve release failed: %w",e)}
  if e=w.ack.Sync();e!=nil{return e}
 }
 return nil
}
func (w *WAL) ackState()(map[string]map[uint64]bool,error){
 acked:=map[string]map[uint64]bool{}
 e:=readFrames(filepath.Join(w.opts.Dir,"ack.wal"),func(b []byte)error{
  var a Ack
  if e:=json.Unmarshal(b,&a);e!=nil{return fmt.Errorf("decode ACK record: %w",e)}
  if a.Seq==0||a.EventID==""||(a.Sink!="mqtt"&&a.Sink!="influx"){return fmt.Errorf("invalid ACK record")}
  if acked[a.Sink]==nil{acked[a.Sink]=map[uint64]bool{}}
  acked[a.Sink][a.Seq]=true
  return nil
 })
 return acked,e
}
func (w *WAL) Pending()([]Record,error){
 pending,e:=w.PendingWithAcks();if e!=nil{return nil,e}
 records:=make([]Record,0,len(pending))
 for _,p:=range pending{records=append(records,p.Record)}
 return records,nil
}

// PendingWithAcks exposes independent sink state so a dispatcher can avoid
// republishing to a destination that has already durably acknowledged an event.
func (w *WAL) PendingWithAcks()([]PendingRecord,error){
 w.mu.Lock();defer w.mu.Unlock()
 if w.closed{return nil,os.ErrClosed}
 acked,e:=w.ackState();if e!=nil{return nil,e}
 files,e:=filepath.Glob(filepath.Join(w.opts.Dir,"data-*.wal"));if e!=nil{return nil,e};sort.Strings(files)
 var out []PendingRecord
 for _,p:=range files{
  var seg uint64;_,_=fmt.Sscanf(filepath.Base(p),"data-%d.wal",&seg)
  if seg<w.manifest.FirstSegment||seg>w.manifest.ActiveSegment{continue}
  e=readFrames(p,func(b []byte)error{
   var r Record;if e:=json.Unmarshal(b,&r);e!=nil{return e}
   if r.Seq<=w.manifest.CheckpointSeq{return nil}
   mqtt:=acked["mqtt"][r.Seq];influx:=acked["influx"][r.Seq]
   if !mqtt||!influx{out=append(out,PendingRecord{Record:r,MQTTAcked:mqtt,InfluxAcked:influx})}
   return nil
  })
  if e!=nil{return nil,e}
 }
 return out,nil
}
// CheckpointReady advances the committed prefix as far as both downstream
// sinks have durably acknowledged every preceding record, then reclaims safe
// segments. It is safe to call periodically from the delivery worker.
func (w *WAL) CheckpointReady()(uint64,error){
 w.mu.Lock()
 if w.closed{w.mu.Unlock();return 0,os.ErrClosed}
 current:=w.manifest.CheckpointSeq
 acked,e:=w.ackState()
 if e!=nil{w.mu.Unlock();return current,e}
 needsCompaction:=false
 for _,sinkState:=range acked{for seq:=range sinkState{if seq<=current{needsCompaction=true;break}}}
 files,e:=filepath.Glob(filepath.Join(w.opts.Dir,"data-*.wal"))
 if e!=nil{w.mu.Unlock();return current,e}
 sort.Strings(files)
 candidate:=current+1
 for _,p:=range files{
  var seg uint64;_,_=fmt.Sscanf(filepath.Base(p),"data-%d.wal",&seg)
  if seg<w.manifest.FirstSegment||seg>w.manifest.ActiveSegment{continue}
  e=readFrames(p,func(b []byte)error{
   var r Record;if e:=json.Unmarshal(b,&r);e!=nil{return e}
   if r.Seq<=current{return nil}
   if r.Seq!=candidate{return fmt.Errorf("non-contiguous WAL sequence: got %d, expected %d",r.Seq,candidate)}
   if !acked["mqtt"][r.Seq]||!acked["influx"][r.Seq]{return errCheckpointBlocked}
   candidate++
   return nil
  })
  if errors.Is(e,errCheckpointBlocked){e=nil;goto ready}
  if e!=nil{w.mu.Unlock();return current,e}
 }
ready:
 target:=candidate-1
 w.mu.Unlock()
 if target>current||needsCompaction{
  if e=w.Checkpoint(target);e!=nil{return target,e}
 }
 if e=w.Reclaim();e!=nil{return target,e}
 return target,nil
}

var errCheckpointBlocked=errors.New("checkpoint blocked by pending downstream ACK")

func (w *WAL) Checkpoint(seq uint64)error{
 w.mu.Lock();defer w.mu.Unlock()
 if w.closed{return os.ErrClosed}
 if seq<w.manifest.CheckpointSeq{return fmt.Errorf("checkpoint cannot move backwards from %d to %d",w.manifest.CheckpointSeq,seq)}
 if seq>=w.next{return fmt.Errorf("checkpoint seq %d is beyond last appended seq %d",seq,w.next-1)}
 // Freeze and persist both logs before recording a checkpoint boundary.
 if w.data!=nil{if e:=w.data.Sync();e!=nil{return e}}
 if w.ack!=nil{if e:=w.ack.Sync();e!=nil{return e}}
 acked,e:=w.ackState();if e!=nil{return e}
 files,e:=filepath.Glob(filepath.Join(w.opts.Dir,"data-*.wal"));if e!=nil{return e}
 sort.Strings(files)
 expected := w.manifest.CheckpointSeq + 1
 foundTarget := seq == w.manifest.CheckpointSeq
 for _,p:=range files{
  var seg uint64;_,_=fmt.Sscanf(filepath.Base(p),"data-%d.wal",&seg)
  if seg<w.manifest.FirstSegment||seg>w.manifest.ActiveSegment{continue}
  e=readFrames(p,func(b []byte)error{
   var r Record;if e:=json.Unmarshal(b,&r);e!=nil{return e}
   if r.Seq<=w.manifest.CheckpointSeq{return nil}
   if r.Seq<=seq {
    if r.Seq!=expected{return fmt.Errorf("cannot checkpoint across sequence gap: got %d, expected %d",r.Seq,expected)}
    if !acked["mqtt"][r.Seq]||!acked["influx"][r.Seq]{return fmt.Errorf("cannot checkpoint unacknowledged seq %d",r.Seq)}
    expected++
    if r.Seq==seq{foundTarget=true}
   }
   return nil
  });if e!=nil{return e}
 }
 if !foundTarget{return fmt.Errorf("checkpoint target sequence %d is not present in retained WAL",seq)}
 cp:=Checkpoint{Seq:seq,CreatedAt:time.Now().UTC()}
 cpPath:=checkpointPath(w.opts.Dir,seq)
 if _,statErr:=os.Stat(cpPath);os.IsNotExist(statErr){
  b,e:=json.Marshal(cp);if e!=nil{return e}
  if e=atomicFile(cpPath,b);e!=nil{return e}
 }else if statErr!=nil{return statErr}else{
  existing,e:=os.ReadFile(cpPath);if e!=nil{return e};var saved Checkpoint
  if e=json.Unmarshal(existing,&saved);e!=nil{return e}
  if saved.Seq!=seq{return fmt.Errorf("existing checkpoint %d does not match requested %d",saved.Seq,seq)}
 }
 old:=w.manifest.CheckpointSeq
 w.manifest.CheckpointSeq=seq
 if e=w.saveManifest();e!=nil{w.manifest.CheckpointSeq=old;return e}
 if e=w.compactAckLog(seq);e!=nil{return fmt.Errorf("checkpoint %d committed but ACK compaction failed: %w",seq,e)}
 return nil
}

func (w *WAL) compactAckLog(boundary uint64)error{
 ackPath:=filepath.Join(w.opts.Dir,"ack.wal")
 tmpPath:=ackPath+".tmp"
 tmp,e:=os.OpenFile(tmpPath,os.O_CREATE|os.O_TRUNC|os.O_WRONLY,0600);if e!=nil{return e}
 keepErr:=readFrames(ackPath,func(b []byte)error{
  var a Ack
  if e:=json.Unmarshal(b,&a);e!=nil{return e}
  if a.Seq>boundary{return writeFrame(tmp,b)}
  return nil
 })
 if keepErr!=nil{_ = tmp.Close();_ = os.Remove(tmpPath);return keepErr}
 if e=tmp.Sync();e!=nil{_ = tmp.Close();_ = os.Remove(tmpPath);return e}
 if e=tmp.Close();e!=nil{_ = os.Remove(tmpPath);return e}
 if w.ack!=nil{
  if e=w.ack.Sync();e!=nil{_ = os.Remove(tmpPath);return e}
  if e=w.ack.Close();e!=nil{_ = os.Remove(tmpPath);return e}
  w.ack=nil
 }
 if e=os.Rename(tmpPath,ackPath);e!=nil{
  w.ack,_=os.OpenFile(ackPath,os.O_CREATE|os.O_RDWR|os.O_APPEND,0600)
  return e
 }
 d,e:=os.Open(w.opts.Dir)
 var syncErr error
 if e!=nil{syncErr=e}else{syncErr=d.Sync();_ = d.Close()}
 w.ack,e=os.OpenFile(ackPath,os.O_CREATE|os.O_RDWR|os.O_APPEND,0600)
 if e!=nil{return e}
 return syncErr
}

func checkpointPath(dir string,seq uint64)string{return filepath.Join(dir,fmt.Sprintf("checkpoint-%020d.json",seq))}

// cleanupObsoleteSegments resumes deletion after a crash that occurred after
// the manifest commit but before every covered segment was unlinked.
func (w *WAL) cleanupObsoleteSegments()error{
 files,e:=filepath.Glob(filepath.Join(w.opts.Dir,"data-*.wal"));if e!=nil{return e}
 changed:=false
 for _,p:=range files{
  var seg uint64
  if _,e=fmt.Sscanf(filepath.Base(p),"data-%d.wal",&seg);e!=nil{return e}
  if seg<w.manifest.FirstSegment{
   if e=os.Remove(p);e!=nil&&!os.IsNotExist(e){return e}
   changed=true
  }
 }
 if changed{d,e:=os.Open(w.opts.Dir);if e!=nil{return e};defer d.Close();return d.Sync()}
 return nil
}

func (w *WAL) saveManifest()error{b,e:=json.Marshal(w.manifest);if e!=nil{return e};return atomicFile(filepath.Join(w.opts.Dir,"MANIFEST"),b)}
func atomicFile(path string,b []byte)error{
 tmp:=path+".tmp";f,e:=os.OpenFile(tmp,os.O_CREATE|os.O_TRUNC|os.O_WRONLY,0600);if e!=nil{return e};if _,e=f.Write(b);e!=nil{f.Close();return e};if e=f.Sync();e!=nil{f.Close();return e};if e=f.Close();e!=nil{return e};if e=os.Rename(tmp,path);e!=nil{return e};d,e:=os.Open(filepath.Dir(path));if e!=nil{return e};defer d.Close();return d.Sync()
}
func (w *WAL) Reclaim()error{
 w.mu.Lock();defer w.mu.Unlock()
 if w.closed{return os.ErrClosed}
 if w.manifest.CheckpointSeq==0{return nil}
 // A reclaim operation must be backed by the checkpoint committed by MANIFEST.
 raw,e:=os.ReadFile(checkpointPath(w.opts.Dir,w.manifest.CheckpointSeq));if os.IsNotExist(e){raw,e=os.ReadFile(filepath.Join(w.opts.Dir,"checkpoint.json"))};if e!=nil{return fmt.Errorf("read committed checkpoint before reclaim: %w",e)}
 var cp Checkpoint
 if e=json.Unmarshal(raw,&cp);e!=nil{return fmt.Errorf("decode committed checkpoint before reclaim: %w",e)}
 if cp.Seq!=w.manifest.CheckpointSeq{return fmt.Errorf("checkpoint sequence %d does not match committed manifest %d",cp.Seq,w.manifest.CheckpointSeq)}
 next:=w.manifest.FirstSegment
 var remove []string
 for seg:=w.manifest.FirstSegment;seg<w.manifest.ActiveSegment;seg++{
  p:=filepath.Join(w.opts.Dir,fmt.Sprintf("data-%020d.wal",seg))
  if _,e=os.Stat(p);e!=nil{
   if os.IsNotExist(e){return fmt.Errorf("required segment %d is missing before manifest reclaim boundary advances",seg)}
   return e
  }
  safe:=true
  e=readFrames(p,func(b []byte)error{
   var r Record;if e:=json.Unmarshal(b,&r);e!=nil{return e}
   if r.Seq>w.manifest.CheckpointSeq{safe=false}
   return nil
  })
  if e!=nil{return e}
  if !safe{break}
  remove=append(remove,p);next=seg+1
 }
 if next==w.manifest.FirstSegment{return nil}
 old:=w.manifest.FirstSegment
 w.manifest.FirstSegment=next
 if e=w.saveManifest();e!=nil{w.manifest.FirstSegment=old;return e}
 // Once MANIFEST is durable, deletion may be resumed safely after a crash.
 for _,p:=range remove{if e=os.Remove(p);e!=nil&&!os.IsNotExist(e){return e}}
 d,e:=os.Open(w.opts.Dir);if e!=nil{return e};defer d.Close();return d.Sync()
}
// Stats returns an operational snapshot for health and Prometheus exporters.
func (w *WAL) Stats()(Stats,error){
 pending,e:=w.PendingWithAcks();if e!=nil{return Stats{},e}
 usage,e:=w.diskUsage();if e!=nil{return Stats{},e}
 ids,e:=listSegmentIDs(w.opts.Dir,"data");if e!=nil{return Stats{},e}
 w.mu.Lock()
 stats:=Stats{
  DiskBytes:usage,MaxBytes:w.opts.MaxBytes,
  PendingRecords:len(pending),SegmentCount:len(ids),CapacityRejections:w.capacityRejections.Load(),AdmissionLimitBytes:w.opts.MaxBytes-w.opts.ReserveBytes,
  CheckpointSeq:w.manifest.CheckpointSeq,
  FirstSegment:w.manifest.FirstSegment,ActiveSegment:w.manifest.ActiveSegment,
 }
 w.mu.Unlock()
 if stats.MaxBytes>0{stats.DiskUsedRatio=float64(stats.DiskBytes)/float64(stats.MaxBytes)}
 now:=time.Now()
 for _,record:=range pending{age:=now.Sub(record.CreatedAt);if age>stats.OldestPendingAge{stats.OldestPendingAge=age}}
 return stats,nil
}

func (w *WAL) Close()error{
 w.mu.Lock();defer w.mu.Unlock();if w.closed{return nil};w.closed=true;var es []string
 if w.data!=nil{if e:=w.data.Sync();e!=nil{es=append(es,e.Error())};if e:=w.data.Close();e!=nil{es=append(es,e.Error())}}
 if w.ack!=nil{if e:=w.ack.Sync();e!=nil{es=append(es,e.Error())};if e:=w.ack.Close();e!=nil{es=append(es,e.Error())}}
 if w.lock!=nil{if e:=w.lock.close();e!=nil{es=append(es,e.Error())}};if len(es)>0{return fmt.Errorf("%s",strings.Join(es,"; "))};return nil
}
