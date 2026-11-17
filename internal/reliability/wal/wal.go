package wal

import (
 "encoding/json"
 "bytes"
 "crypto/sha256"
 "encoding/hex"
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

type WAL struct { mu sync.Mutex; opts Options; lock *fileLock; data *os.File; ack *os.File; manifest Manifest; manifestExists bool; next uint64; closed bool; failed error; capacityRejections atomic.Uint64; manifestCommitHook func(Manifest) error; faultHook func(string); ackWriteHook func(*os.File, []byte) error; ackSyncHook func(*os.File) error; reserveReleaseHook func(string) error }
func (w *WAL) crashPoint(name string) { if w.faultHook != nil { w.faultHook(name) } }

func Open(o Options)(*WAL,error){
 if o.Dir==""{return nil,fmt.Errorf("WAL dir required")};if o.SegmentBytes<=0{o.SegmentBytes=64<<20};if o.MaxBytes<=0{o.MaxBytes=10<<30};if o.ReserveBytes<0||o.ReserveBytes>=o.MaxBytes{return nil,fmt.Errorf("reserve must be >=0 and less than max bytes")}
 if err:=os.MkdirAll(o.Dir,0700);err!=nil{return nil,err};l,err:=acquireLock(o.Dir);if err!=nil{return nil,err}
 w:=&WAL{opts:o,lock:l,next:1};fail:=func(e error)(*WAL,error){l.close();return nil,e}
 if err=ensureAckReserve(o.Dir,o.ReserveBytes);err!=nil&&!errors.Is(err,syscall.ENOSPC){return fail(err)}
 if err=w.loadManifest();err!=nil{return fail(err)};if err=w.prepareInitialDataSegment();err!=nil{return fail(err)};if err=w.prepareAckSegments();err!=nil{return fail(err)};if err=w.repairActiveTails();err!=nil{return fail(err)};if err=w.recover();err!=nil{return fail(err)};if err=w.openActive();err!=nil{return fail(err)};return w,nil
}
func (w *WAL) loadManifest() error {
	b, err := os.ReadFile(filepath.Join(w.opts.Dir, "MANIFEST"))
	if os.IsNotExist(err) {
		w.manifest = Manifest{Version: 1, FirstSegment: 1, ActiveSegment: 1}
		return nil
	}
	if err != nil { return err }
	w.manifestExists = true
	if err = json.Unmarshal(b, &w.manifest); err != nil { return fmt.Errorf("manifest: %w", err) }
	if w.manifest.Version < 1 || w.manifest.Version > 6 {
		return fmt.Errorf("unsupported manifest version %d", w.manifest.Version)
	}
	if w.manifest.FirstSegment == 0 || w.manifest.ActiveSegment < w.manifest.FirstSegment {
		return fmt.Errorf("invalid manifest segment range")
	}
	if w.manifest.Version >= 4 && (w.manifest.AckFirstSegment == 0 || w.manifest.AckActiveSegment < w.manifest.AckFirstSegment) {
		return fmt.Errorf("invalid manifest ACK segment range")
	}
	if w.manifest.Version >= 5 && w.manifest.Version < 6 && w.manifest.CheckpointSeq == 0 && w.manifest.CheckpointFile != "" {
		return fmt.Errorf("checkpoint file reference must be empty when checkpoint sequence is zero")
	}
	if w.manifest.Version >= 2 {
		if w.manifest.Checksum == "" { return fmt.Errorf("manifest checksum is required for version 2") }
		expected, checksumErr := manifestChecksum(w.manifest)
		if checksumErr != nil { return checksumErr }
		if !strings.EqualFold(expected, w.manifest.Checksum) { return fmt.Errorf("manifest checksum mismatch") }
	}
	if w.manifest.CheckpointSeq > 0 || (w.manifest.Version >= 6 && w.manifest.CheckpointFile != "") {
		cpPath := checkpointPath(w.opts.Dir, w.manifest.CheckpointSeq)
		if w.manifest.Version >= 5 {
			name := w.manifest.CheckpointFile
			if name == "" || filepath.Base(name) != name || strings.ContainsAny(name, "/\\") {
				return fmt.Errorf("invalid manifest checkpoint file reference %q", name)
			}
			cpPath = filepath.Join(w.opts.Dir, name)
		}
		cp, readErr := os.ReadFile(cpPath)
		if os.IsNotExist(readErr) && w.manifest.Version < 5 {
			cp, readErr = os.ReadFile(filepath.Join(w.opts.Dir, "checkpoint.json"))
		}
		if readErr != nil { return fmt.Errorf("committed checkpoint missing: %w", readErr) }
		if w.manifest.Version >= 3 {
			if w.manifest.CheckpointChecksum == "" { return fmt.Errorf("manifest checkpoint checksum is required") }
			sum := sha256.Sum256(cp)
			if !strings.EqualFold(hex.EncodeToString(sum[:]), w.manifest.CheckpointChecksum) {
				return fmt.Errorf("checkpoint checksum mismatch")
			}
		}
		var checkpoint Checkpoint
		if err = json.Unmarshal(cp, &checkpoint); err != nil { return fmt.Errorf("decode committed checkpoint: %w", err) }
		if checkpoint.Seq != w.manifest.CheckpointSeq {
			return fmt.Errorf("checkpoint sequence %d does not match manifest %d", checkpoint.Seq, w.manifest.CheckpointSeq)
		}
	}
	return nil
}
func (w *WAL) repairActiveTails()error{
 paths:=[]string{
  filepath.Join(w.opts.Dir,fmt.Sprintf("data-%020d.wal",w.manifest.ActiveSegment)),
  ackSegmentPath(w.opts.Dir, func() uint64 { if w.manifest.AckActiveSegment == 0 { return 1 }; return w.manifest.AckActiveSegment }()),
 }
 for _,p:=range paths{
  f,e:=os.OpenFile(p,os.O_RDWR,0600);if e!=nil{return fmt.Errorf("open committed WAL file %s: %w",p,e)}
  if e=f.Close();e!=nil{return e}
  if e=repairFrameTail(p);e!=nil{return e}
 }
 return nil
}

func (w *WAL) recover()error{
 if e:=w.cleanupObsoleteSegments();e!=nil{return e}
 max:=w.manifest.CheckpointSeq
 eventIDs:=make(map[uint64]string)
 recordsBySeq:=make(map[uint64]Record)
 for seg:=w.manifest.FirstSegment;seg<=w.manifest.ActiveSegment;seg++{
  p:=filepath.Join(w.opts.Dir,fmt.Sprintf("data-%020d.wal",seg))
  if _,e:=os.Stat(p);e!=nil{return fmt.Errorf("required data segment %d is unavailable: %w",seg,e)}
  if e:=readFrames(p,func(b []byte)error{var r Record;if e:=json.Unmarshal(b,&r);e!=nil{return fmt.Errorf("decode data record in %s: %w",p,e)};if r.Seq==0||r.EventID==""||len(r.Payload)==0{return fmt.Errorf("invalid data record in %s",p)};if r.Seq>max{max=r.Seq};if r.Seq>w.manifest.CheckpointSeq{if _,ok:=eventIDs[r.Seq];ok{return fmt.Errorf("duplicate WAL sequence %d",r.Seq)};eventIDs[r.Seq]=r.EventID;recordsBySeq[r.Seq]=r};return nil});e!=nil{return e}
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
	if w.manifest.CheckpointSeq > 0 || w.manifest.CheckpointFile != "" {
		cpPath := checkpointPath(w.opts.Dir, w.manifest.CheckpointSeq)
		if w.manifest.Version >= 5 && w.manifest.CheckpointFile != "" { cpPath = filepath.Join(w.opts.Dir, w.manifest.CheckpointFile) }
		raw, readErr := os.ReadFile(cpPath)
		if os.IsNotExist(readErr) && w.manifest.Version < 5 { raw, readErr = os.ReadFile(filepath.Join(w.opts.Dir, "checkpoint.json")) }
		if readErr != nil { return fmt.Errorf("read committed checkpoint snapshot: %w", readErr) }
		var snapshot Checkpoint
		if readErr = json.Unmarshal(raw, &snapshot); readErr != nil { return fmt.Errorf("decode committed checkpoint snapshot: %w", readErr) }
		if snapshot.FormatVersion > 2 { return fmt.Errorf("unsupported checkpoint format version %d", snapshot.FormatVersion) }
		seen := make(map[uint64]bool, len(snapshot.Pending))
		for _, pending := range snapshot.Pending {
			if pending.Seq <= snapshot.Seq || pending.Seq == 0 || pending.EventID == "" || len(pending.Payload) == 0 || !json.Valid(pending.Payload) {
				return fmt.Errorf("invalid pending record in checkpoint snapshot: seq=%d", pending.Seq)
			}
			if pending.MQTTAcked && pending.InfluxAcked { return fmt.Errorf("checkpoint snapshot contains fully acknowledged pending record %d", pending.Seq) }
			if seen[pending.Seq] { return fmt.Errorf("duplicate pending record %d in checkpoint snapshot", pending.Seq) }
			seen[pending.Seq] = true
			record, ok := recordsBySeq[pending.Seq]
			if !ok { return fmt.Errorf("checkpoint pending record %d is missing from retained Data WAL", pending.Seq) }
			if record.EventID != pending.EventID || !bytes.Equal(record.Payload, pending.Payload) || !record.CreatedAt.Equal(pending.CreatedAt) {
				return fmt.Errorf("checkpoint pending record %d does not match retained Data WAL", pending.Seq)
			}
		}
	}
	if _,e:=w.ackState();e!=nil{return fmt.Errorf("recover ACK log: %w",e)}
 if e:=readAckFrames(w.opts.Dir,w.manifest,func(b []byte)error{var a Ack;if e:=json.Unmarshal(b,&a);e!=nil{return e};if a.Seq<=w.manifest.CheckpointSeq{return nil};id,ok:=eventIDs[a.Seq];if !ok{return fmt.Errorf("ACK references unknown sequence %d during recovery",a.Seq)};if id!=a.EventID{return fmt.Errorf("ACK event_id mismatch for sequence %d during recovery",a.Seq)};return nil});e!=nil{return fmt.Errorf("validate ACK references: %w",e)}
 w.next=max+1;if w.next==0{return fmt.Errorf("WAL sequence exhausted")}
 if e:=w.cleanupCheckpointGenerations();e!=nil{return e}
 return nil
}
func (w *WAL) openActive()error{
 p:=filepath.Join(w.opts.Dir,fmt.Sprintf("data-%020d.wal",w.manifest.ActiveSegment));f,e:=os.OpenFile(p,os.O_CREATE|os.O_RDWR|os.O_APPEND,0600);if e!=nil{return e}
 if e=repairFrameTail(p);e!=nil{f.Close();return e};w.data=f
 ackPath:=ackSegmentPath(w.opts.Dir,w.manifest.AckActiveSegment)
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
  if len(event.EventID)>1024{return nil,fmt.Errorf("event %d: event_id exceeds 1024 bytes",i)}
  if len(event.Payload)==0||!json.Valid(event.Payload){return nil,fmt.Errorf("event %d: payload must be valid JSON",i)}
  // JSON escaping can expand each event ID byte to six bytes; account for
  // that worst case so admission never underestimates encoded WAL size.
  estimate+=int64(len(event.Payload))+int64(len(event.EventID))*6+256
 }
 usage,e:=w.diskUsage();if e!=nil{return nil,e}
 if usage+estimate>w.admissionLimitBytes(){w.capacityRejections.Add(1);return nil,ErrCapacity}
 fsUsed,fsTotal,e:=filesystemUsage(w.opts.Dir);if e!=nil{return nil,fmt.Errorf("read filesystem capacity: %w",e)}
 if fsTotal>0&&float64(fsUsed+uint64(estimate))/float64(fsTotal)>=0.95{w.capacityRejections.Add(1);return nil,ErrCapacity}
 if w.data==nil{return nil,os.ErrClosed}
 start,e:=w.data.Seek(0,io.SeekEnd);if e!=nil{return nil,e}
 records:=make([]Record,0,len(events))
 rollback:=func(cause error)([]Record,error){
  if err:=w.data.Truncate(start);err!=nil{w.failed=errors.Join(cause,fmt.Errorf("rollback WAL batch: %w",err));return nil,w.failed}
  if err:=syncFile(w.data);err!=nil{w.failed=errors.Join(cause,fmt.Errorf("sync WAL rollback: %w",err));return nil,w.failed}
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
  if e=syncFile(w.data);e!=nil{return rollback(e)}
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
	if err := syncFile(w.data); err != nil {
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
	if err = syncFile(created); err != nil {
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
	err = syncFile(dir)
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
func (w *WAL) writeACKFrame(payload []byte) error {
	if w.ackWriteHook != nil { return w.ackWriteHook(w.ack, payload) }
	return writeFrame(w.ack, payload)
}
func (w *WAL) syncACK() error {
	if w.ackSyncHook != nil { return w.ackSyncHook(w.ack) }
	return syncFile(w.ack)
}
func (w *WAL) releaseACKReserve() error {
	if w.reserveReleaseHook != nil { return w.reserveReleaseHook(w.opts.Dir) }
	return releaseAckReserve(w.opts.Dir)
}

func (w *WAL) Ack(a Ack)error{
 w.mu.Lock();defer w.mu.Unlock()
 if w.closed||w.ack==nil{return os.ErrClosed}
 if a.Sink!="mqtt"&&a.Sink!="influx"{return fmt.Errorf("unsupported sink %q",a.Sink)}
 if a.Seq==0||a.EventID==""{return fmt.Errorf("ACK sequence and event_id are required")}
 found:=false
 files,e:=filepath.Glob(filepath.Join(w.opts.Dir,"data-*.wal"));if e!=nil{return e};sort.Strings(files)
 for _,p:=range files{
  var seg uint64;_,_=fmt.Sscanf(filepath.Base(p),"data-%d.wal",&seg)
  if seg<w.manifest.FirstSegment||seg>w.manifest.ActiveSegment{continue}
  e=readFrames(p,func(b []byte)error{var r Record;if e:=json.Unmarshal(b,&r);e!=nil{return e};if r.Seq==a.Seq{if r.EventID!=a.EventID{return fmt.Errorf("ACK event_id mismatch for seq %d",a.Seq)};found=true};return nil})
  if e!=nil{return e};if found{break}
 }
 if !found{return fmt.Errorf("ACK references unknown or checkpointed seq %d",a.Seq)}
 // Validate the supplied identity against the retained Data WAL before the
 // idempotent fast path. A duplicate sequence with a forged EventID must fail.
 state,err:=w.ackState();if err!=nil{return err}
 if state[a.Sink][a.Seq]{return nil}
 // Rotate only when metadata is healthy. If a previous manifest commit
 // became uncertain, ACKs must still append to the last known active file.
 b,e:=json.Marshal(a);if e!=nil{return e}
 if w.failed==nil {
  st,statErr:=w.ack.Stat();if statErr!=nil{return statErr}
  if st.Size()>0&&st.Size()+int64(len(b))+8>w.opts.SegmentBytes {
   if e=w.rotateAckSegment();e!=nil&&w.ack==nil{return fmt.Errorf("rotate ACK segment: %w",e)}
  }
 }
 ackPath:=ackSegmentPath(w.opts.Dir,w.manifest.AckActiveSegment)
 if e=w.writeACKFrame(b);e!=nil{
  if !errors.Is(e,syscall.ENOSPC){return e}
  if e=w.releaseACKReserve();e!=nil{return fmt.Errorf("ACK write failed and reserve release failed: %w",e)}
  if e=repairFrameTail(ackPath);e!=nil{return e}
  if _,e=w.ack.Seek(0,io.SeekEnd);e!=nil{return e}
  if e=w.writeACKFrame(b);e!=nil{return e}
 }
 if e=w.syncACK();e!=nil{
  if !errors.Is(e,syscall.ENOSPC){return e}
  if e=w.releaseACKReserve();e!=nil{return fmt.Errorf("ACK sync failed and reserve release failed: %w",e)}
  if e=w.syncACK();e!=nil{return e}
 }
 return nil
}
func (w *WAL) ackState()(map[string]map[uint64]bool,error){
 acked:=map[string]map[uint64]bool{}
 if w.manifest.CheckpointSeq > 0 || w.manifest.CheckpointFile != "" {
  cpPath:=checkpointPath(w.opts.Dir,w.manifest.CheckpointSeq)
  if w.manifest.Version>=5 && w.manifest.CheckpointFile!="" { cpPath=filepath.Join(w.opts.Dir,w.manifest.CheckpointFile) }
  raw,err:=os.ReadFile(cpPath)
  if os.IsNotExist(err) && w.manifest.Version<5 { raw,err=os.ReadFile(filepath.Join(w.opts.Dir,"checkpoint.json")) };if err!=nil{return nil,err}
  var snapshot Checkpoint
  if err=json.Unmarshal(raw,&snapshot);err!=nil{return nil,err}
  for _,pending:=range snapshot.Pending {
   if acked["mqtt"]==nil{acked["mqtt"]=map[uint64]bool{}}
   if acked["influx"]==nil{acked["influx"]=map[uint64]bool{}}
   if pending.MQTTAcked{acked["mqtt"][pending.Seq]=true}
   if pending.InfluxAcked{acked["influx"][pending.Seq]=true}
  }
 }
 e:=readAckFrames(w.opts.Dir,w.manifest,func(b []byte)error{
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
 if e=w.Checkpoint(target);e!=nil{return target,e}
 if e=w.Reclaim();e!=nil{return target,e}
 // ACK reserve is released only as an emergency last-resort write allowance.
 // Retry preallocation after checkpoint compaction/reclaim has freed space.
 // Serialize reserve replacement with ACK's emergency reserve release.
 w.mu.Lock()
 if w.closed{w.mu.Unlock();return target,os.ErrClosed}
 e=ensureAckReserve(w.opts.Dir,w.opts.ReserveBytes)
 w.mu.Unlock()
 if e!=nil{return target,fmt.Errorf("replenish ACK reserve: %w",e)}
 return target,nil
}

var errCheckpointBlocked=errors.New("checkpoint blocked by pending downstream ACK")

func (w *WAL) Checkpoint(seq uint64)error{
 w.mu.Lock();defer w.mu.Unlock()
 if w.closed{return os.ErrClosed}
 // A failed append/rotation may leave MANIFEST durability uncertain. ACKs
 // remain available, but metadata commits and deletion must fail closed.
 if w.failed!=nil{return fmt.Errorf("WAL metadata operations are fail-stopped: %w",w.failed)}
 if seq<w.manifest.CheckpointSeq{return fmt.Errorf("checkpoint cannot move backwards from %d to %d",w.manifest.CheckpointSeq,seq)}
 if seq>=w.next{return fmt.Errorf("checkpoint seq %d is beyond last appended seq %d",seq,w.next-1)}
 // Freeze and persist both logs before recording a checkpoint boundary.
 if w.data!=nil{if e:=syncFile(w.data);e!=nil{return e}}
 if w.ack!=nil{if e:=syncFile(w.ack);e!=nil{return e}}
 acked,e:=w.ackState();if e!=nil{return e}
 hasCoveredACK:=false
 for _,sinkState:=range acked{for ackSeq:=range sinkState{if ackSeq<=seq{hasCoveredACK=true;break}}}
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
 pendingSnapshot:=make([]PendingRecord,0)
 expectedSnapshot:=seq+1
 for _,p:=range files{
  var seg uint64;_,_=fmt.Sscanf(filepath.Base(p),"data-%d.wal",&seg)
  if seg<w.manifest.FirstSegment||seg>w.manifest.ActiveSegment{continue}
  e=readFrames(p,func(b []byte)error{
   var record Record;if e:=json.Unmarshal(b,&record);e!=nil{return e}
   if record.Seq<=seq{return nil}
   if record.Seq!=expectedSnapshot{return fmt.Errorf("checkpoint snapshot sequence gap: got %d, expected %d",record.Seq,expectedSnapshot)}
   expectedSnapshot++
   mqtt,influx:=acked["mqtt"][record.Seq],acked["influx"][record.Seq]
   if !mqtt||!influx{pendingSnapshot=append(pendingSnapshot,PendingRecord{Record:record,MQTTAcked:mqtt,InfluxAcked:influx})}
   return nil
  });if e!=nil{return e}
 }
 if expectedSnapshot!=w.next{return fmt.Errorf("checkpoint snapshot ends at %d but WAL next sequence is %d",expectedSnapshot-1,w.next-1)}
 cp:=Checkpoint{FormatVersion:2,Seq:seq,CreatedAt:time.Now().UTC(),Pending:pendingSnapshot}
 if seq==0 && len(pendingSnapshot)==0 && w.manifest.CheckpointFile=="" { return nil }
 if w.manifest.CheckpointSeq==seq && w.manifest.CheckpointFile!="" {
  existingRaw,readErr:=os.ReadFile(filepath.Join(w.opts.Dir,w.manifest.CheckpointFile))
  if readErr==nil {
   var existing Checkpoint
   if json.Unmarshal(existingRaw,&existing)==nil && existing.FormatVersion>=2 {
    oldPending,_:=json.Marshal(existing.Pending);newPending,_:=json.Marshal(cp.Pending)
    if bytes.Equal(oldPending,newPending){
     if hasCoveredACK { if compactErr:=w.compactAckLog(seq);compactErr!=nil{return fmt.Errorf("retry ACK compaction for checkpoint %d: %w",seq,compactErr)} }
     return nil
    }
   }
  }
 }
 cpPath, e:=nextCheckpointPath(w.opts.Dir,seq);if e!=nil{return e}
 b,e:=json.Marshal(cp);if e!=nil{return e}
 if e=atomicFile(cpPath,b);e!=nil{return e}
 w.crashPoint("checkpoint.file.durable")
 old:=w.manifest.CheckpointSeq
 oldFile:=w.manifest.CheckpointFile
 w.manifest.CheckpointSeq=seq
 w.manifest.CheckpointFile=filepath.Base(cpPath)
 if e=w.saveManifest();e!=nil{w.manifest.CheckpointSeq=old;w.manifest.CheckpointFile=oldFile;return e}
 w.crashPoint("checkpoint.manifest.committed")
 if e=w.compactAckLog(seq);e!=nil{return fmt.Errorf("checkpoint %d committed but ACK compaction failed: %w",seq,e)}
 if e=w.cleanupCheckpointGenerations();e!=nil{return fmt.Errorf("checkpoint %d committed but old generation cleanup failed: %w",seq,e)}
 return nil
}

func (w *WAL) compactAckLog(boundary uint64)error{
	return w.compactAckSegments(boundary)
}

func checkpointPath(dir string,seq uint64)string{return filepath.Join(dir,fmt.Sprintf("checkpoint-%020d.json",seq))}
func nextCheckpointPath(dir string,seq uint64)(string,error){
 base:=checkpointPath(dir,seq)
 if _,err:=os.Stat(base);os.IsNotExist(err){return base,nil}else if err!=nil{return "",err}
 generation:=time.Now().UTC().UnixNano()
 for generation>0 {
  path:=filepath.Join(dir,fmt.Sprintf("checkpoint-%020d-%020d.json",seq,generation))
  if _,err:=os.Stat(path);os.IsNotExist(err){return path,nil}else if err!=nil{return "",err}
  generation++
 }
 return "",fmt.Errorf("checkpoint generation exhausted")
}
func (w *WAL) cleanupCheckpointGenerations()error{
 keep:=w.manifest.CheckpointFile
 if keep==""&&w.manifest.CheckpointSeq>0{keep=filepath.Base(checkpointPath(w.opts.Dir,w.manifest.CheckpointSeq))}
 files,err:=filepath.Glob(filepath.Join(w.opts.Dir,"checkpoint-*.json"));if err!=nil{return err}
 changed:=false
 temps,err:=filepath.Glob(filepath.Join(w.opts.Dir,"checkpoint-*.json.tmp"));if err!=nil{return err}
 for _,temp:=range temps{if err=os.Remove(temp);err!=nil&&!os.IsNotExist(err){return err};changed=true}
 manifestTemp:=filepath.Join(w.opts.Dir,"MANIFEST.tmp")
 if err=os.Remove(manifestTemp);err==nil{changed=true}else if !os.IsNotExist(err){return err}
 for _,path:=range files {
  if filepath.Base(path)==keep{continue}
  if err=os.Remove(path);err!=nil&&!os.IsNotExist(err){return err}
  changed=true
 }
 legacy:=filepath.Join(w.opts.Dir,"checkpoint.json")
 if w.manifest.Version>=5 && keep!="checkpoint.json" { if err=os.Remove(legacy);err==nil{changed=true}else if !os.IsNotExist(err){return err} }
 if changed { d,openErr:=os.Open(w.opts.Dir);if openErr!=nil{return openErr};syncErr:=syncFile(d);_ = d.Close();if syncErr!=nil{return syncErr} }
 return nil
}

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
  }else if seg>w.manifest.ActiveSegment{
   st,statErr:=os.Stat(p);if statErr!=nil{return statErr}
   if st.Size()!=0{return fmt.Errorf("uncommitted data segment %d is non-empty",seg)}
   if e=os.Remove(p);e!=nil&&!os.IsNotExist(e){return e}
   changed=true
  }
 }
 if changed{d,e:=os.Open(w.opts.Dir);if e!=nil{return e};if e=syncFile(d);e!=nil{_ = d.Close();return e};_ = d.Close()}
 return w.cleanupObsoleteAckSegments()
}

func manifestChecksum(m Manifest)(string,error){
 m.Checksum=""
 b,e:=json.Marshal(m);if e!=nil{return "",e}
 sum:=sha256.Sum256(b)
 return hex.EncodeToString(sum[:]),nil
}

func (w *WAL) saveManifest()error{
 next:=w.manifest
 next.Version=6
 next.CheckpointChecksum=""
 if next.CheckpointSeq==0 && next.CheckpointFile==""{next.CheckpointChecksum=""}
 if next.CheckpointSeq>0 || next.CheckpointFile!=""{
  if next.CheckpointFile==""{next.CheckpointFile=filepath.Base(checkpointPath(w.opts.Dir,next.CheckpointSeq))}
  if filepath.Base(next.CheckpointFile)!=next.CheckpointFile||strings.ContainsAny(next.CheckpointFile,"/\\"){return fmt.Errorf("invalid checkpoint file reference %q",next.CheckpointFile)}
  cpPath:=filepath.Join(w.opts.Dir,next.CheckpointFile)
  cp,e:=os.ReadFile(cpPath)
  if os.IsNotExist(e)&&w.manifest.Version<5{
   cp,e=os.ReadFile(filepath.Join(w.opts.Dir,"checkpoint.json"))
   if e==nil{if copyErr:=atomicFile(cpPath,cp);copyErr!=nil{return fmt.Errorf("migrate legacy checkpoint file reference: %w",copyErr)}}
  }
  if e!=nil{return fmt.Errorf("read checkpoint for manifest commit: %w",e)}
  var checkpoint Checkpoint
  if e=json.Unmarshal(cp,&checkpoint);e!=nil{return fmt.Errorf("decode checkpoint for manifest commit: %w",e)}
  if checkpoint.Seq!=next.CheckpointSeq{return fmt.Errorf("checkpoint file sequence %d does not match manifest sequence %d",checkpoint.Seq,next.CheckpointSeq)}
  sum:=sha256.Sum256(cp)
  next.CheckpointChecksum=hex.EncodeToString(sum[:])
 }
 checksum,e:=manifestChecksum(next);if e!=nil{return e}
 next.Checksum=checksum
 b,e:=json.Marshal(next);if e!=nil{return e}
 if w.manifestCommitHook!=nil{if hookErr:=w.manifestCommitHook(next);hookErr!=nil{w.failed=errors.Join(w.failed,fmt.Errorf("manifest commit hook: %w",hookErr));return hookErr}}
 tmpPath:=filepath.Join(w.opts.Dir,"MANIFEST.tmp")
 f,e:=os.OpenFile(tmpPath,os.O_CREATE|os.O_TRUNC|os.O_WRONLY,0600);if e!=nil{return e}
 if _,e=f.Write(b);e!=nil{_ = f.Close();return e}
 if e=syncFile(f);e!=nil{_ = f.Close();return e}
 if e=f.Close();e!=nil{return e}
 w.crashPoint("manifest.tmp.synced")
 if e=os.Rename(tmpPath,filepath.Join(w.opts.Dir,"MANIFEST"));e!=nil{
  w.failed=errors.Join(w.failed,fmt.Errorf("manifest commit outcome is uncertain: %w",e));return e
 }
 w.crashPoint("manifest.renamed")
 d,e:=os.Open(w.opts.Dir);if e!=nil{w.failed=errors.Join(w.failed,e);return e}
 e=syncFile(d);_ = d.Close()
 if e!=nil{w.failed=errors.Join(w.failed,fmt.Errorf("manifest directory sync outcome is uncertain: %w",e));return e}
 w.crashPoint("manifest.dir.synced")
 w.manifest=next
 w.manifestExists=true
 return nil
}
func atomicFile(path string,b []byte)error{
 tmp:=path+".tmp";f,e:=os.OpenFile(tmp,os.O_CREATE|os.O_TRUNC|os.O_WRONLY,0600);if e!=nil{return e};if _,e=f.Write(b);e!=nil{f.Close();return e};if e=syncFile(f);e!=nil{f.Close();return e};if e=f.Close();e!=nil{return e};if e=os.Rename(tmp,path);e!=nil{return e};d,e:=os.Open(filepath.Dir(path));if e!=nil{return e};defer d.Close();return syncFile(d)
}
func (w *WAL) Reclaim()error{
 w.mu.Lock();defer w.mu.Unlock()
 if w.closed{return os.ErrClosed}
 // Never advance deletion boundaries while a prior manifest/append outcome
 // is uncertain. ACK appends are intentionally not gated by this flag. 
 if w.failed!=nil{return fmt.Errorf("WAL reclaim is fail-stopped: %w",w.failed)}
 if w.manifest.CheckpointSeq==0{return nil}
 // A reclaim operation must be backed by the checkpoint committed by MANIFEST.
 cpPath:=checkpointPath(w.opts.Dir,w.manifest.CheckpointSeq)
 if w.manifest.Version>=5{cpPath=filepath.Join(w.opts.Dir,w.manifest.CheckpointFile)}
 raw,e:=os.ReadFile(cpPath);if os.IsNotExist(e)&&w.manifest.Version<5{raw,e=os.ReadFile(filepath.Join(w.opts.Dir,"checkpoint.json"))};if e!=nil{return fmt.Errorf("read committed checkpoint before reclaim: %w",e)}
 if w.manifest.Version>=3{
  if w.manifest.CheckpointChecksum==""{return fmt.Errorf("manifest checkpoint checksum is required before reclaim")}
  sum:=sha256.Sum256(raw)
  if !strings.EqualFold(hex.EncodeToString(sum[:]),w.manifest.CheckpointChecksum){return fmt.Errorf("checkpoint checksum mismatch before reclaim")}
 }
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
 w.crashPoint("data.reclaim.manifest.committed")
 for _,p:=range remove{if e=os.Remove(p);e!=nil&&!os.IsNotExist(e){return e};w.crashPoint("data.reclaim.segment.deleted")}
 d,e:=os.Open(w.opts.Dir);if e!=nil{return e};defer d.Close();return syncFile(d)
}
// Stats returns an operational snapshot for health and Prometheus exporters.
func (w *WAL) admissionLimitBytes()int64{
 limit:=w.opts.MaxBytes-w.opts.ReserveBytes
 stopLimit:=int64(float64(w.opts.MaxBytes)*0.95)
 if stopLimit<limit{limit=stopLimit}
 return limit
}

func classifyDiskLevel(walRatio,filesystemRatio float64)string{
 ratio:=walRatio
 if filesystemRatio>ratio{ratio=filesystemRatio}
 switch{
 case ratio>=0.95:return "stop"
 case ratio>=0.85:return "critical"
 case ratio>=0.70:return "warning"
 default:return "normal"
 }
}

func (w *WAL) Stats()(Stats,error){
 pending,e:=w.PendingWithAcks();if e!=nil{return Stats{},e}
 usage,e:=w.diskUsage();if e!=nil{return Stats{},e}
 fsUsed,fsTotal,e:=filesystemUsage(w.opts.Dir);if e!=nil{return Stats{},e}
 ids,e:=listSegmentIDs(w.opts.Dir,"data");if e!=nil{return Stats{},e}
 w.mu.Lock()
 stats:=Stats{
  DiskBytes:usage,MaxBytes:w.opts.MaxBytes,
  FilesystemBytes:fsUsed,FilesystemTotalBytes:fsTotal,
  PendingRecords:len(pending),SegmentCount:len(ids),AckSegmentCount:int(w.manifest.AckActiveSegment-w.manifest.AckFirstSegment+1),CapacityRejections:w.capacityRejections.Load(),AdmissionLimitBytes:w.admissionLimitBytes(),
  CheckpointSeq:w.manifest.CheckpointSeq,
  FirstSegment:w.manifest.FirstSegment,ActiveSegment:w.manifest.ActiveSegment,
  Fsync:snapshotFsyncMetrics(),
 }
 w.mu.Unlock()
 if stats.MaxBytes>0{stats.DiskUsedRatio=float64(stats.DiskBytes)/float64(stats.MaxBytes)}
 if stats.FilesystemTotalBytes>0{stats.FilesystemUsedRatio=float64(stats.FilesystemBytes)/float64(stats.FilesystemTotalBytes)}
 stats.DiskLevel=classifyDiskLevel(stats.DiskUsedRatio,stats.FilesystemUsedRatio)
 now:=time.Now()
 for _,record:=range pending{age:=now.Sub(record.CreatedAt);if age>stats.OldestPendingAge{stats.OldestPendingAge=age}}
 return stats,nil
}

func (w *WAL) Close()error{
 w.mu.Lock();defer w.mu.Unlock();if w.closed{return nil};w.closed=true;var es []string
 if w.data!=nil{if e:=syncFile(w.data);e!=nil{es=append(es,e.Error())};if e:=w.data.Close();e!=nil{es=append(es,e.Error())}}
 if w.ack!=nil{if e:=syncFile(w.ack);e!=nil{es=append(es,e.Error())};if e:=w.ack.Close();e!=nil{es=append(es,e.Error())}}
 if w.lock!=nil{if e:=w.lock.close();e!=nil{es=append(es,e.Error())}};if len(es)>0{return fmt.Errorf("%s",strings.Join(es,"; "))};return nil
}
