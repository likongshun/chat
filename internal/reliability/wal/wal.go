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
 "syscall"
 "time"
)

type WAL struct { mu sync.Mutex; opts Options; lock *fileLock; data *os.File; ack *os.File; manifest Manifest; next uint64; closed bool }
func Open(o Options)(*WAL,error){
 if o.Dir==""{return nil,fmt.Errorf("WAL dir required")};if o.SegmentBytes<=0{o.SegmentBytes=64<<20};if o.MaxBytes<=0{o.MaxBytes=10<<30};if o.ReserveBytes<0||o.ReserveBytes>=o.MaxBytes{return nil,fmt.Errorf("reserve must be >=0 and less than max bytes")}
 if err:=os.MkdirAll(o.Dir,0700);err!=nil{return nil,err};l,err:=acquireLock(o.Dir);if err!=nil{return nil,err}
 w:=&WAL{opts:o,lock:l,next:1};fail:=func(e error)(*WAL,error){l.close();return nil,e}
 if err=ensureAckReserve(o.Dir,o.ReserveBytes);err!=nil&&!errors.Is(err,syscall.ENOSPC){return fail(err)}
 if err=w.loadManifest();err!=nil{return fail(err)};if err=w.recover();err!=nil{return fail(err)};if err=w.openActive();err!=nil{return fail(err)};return w,nil
}
func (w *WAL) loadManifest()error{
 b,e:=os.ReadFile(filepath.Join(w.opts.Dir,"MANIFEST"));if os.IsNotExist(e){w.manifest=Manifest{Version:1,FirstSegment:1,ActiveSegment:1};return nil};if e!=nil{return e}
 if e=json.Unmarshal(b,&w.manifest);e!=nil{return fmt.Errorf("manifest: %w",e)};if w.manifest.Version!=1{return fmt.Errorf("unsupported manifest version %d",w.manifest.Version)};if w.manifest.FirstSegment==0||w.manifest.ActiveSegment<w.manifest.FirstSegment{return fmt.Errorf("invalid manifest segment range")};if w.manifest.CheckpointSeq>0{cpPath:=checkpointPath(w.opts.Dir,w.manifest.CheckpointSeq);cp,e:=os.ReadFile(cpPath);if os.IsNotExist(e){cp,e=os.ReadFile(filepath.Join(w.opts.Dir,"checkpoint.json"))};if e!=nil{return fmt.Errorf("committed checkpoint missing: %w",e)};var checkpoint Checkpoint;if e=json.Unmarshal(cp,&checkpoint);e!=nil{return fmt.Errorf("decode committed checkpoint: %w",e)};if checkpoint.Seq!=w.manifest.CheckpointSeq{return fmt.Errorf("checkpoint sequence %d does not match manifest %d",checkpoint.Seq,w.manifest.CheckpointSeq)}};return nil
}
func (w *WAL) recover()error{
 if e:=w.cleanupObsoleteSegments();e!=nil{return e}
 files,e:=filepath.Glob(filepath.Join(w.opts.Dir,"data-*.wal"));if e!=nil{return e};sort.Strings(files);max:=w.manifest.CheckpointSeq
 for _,p:=range files{var seg uint64;_,_=fmt.Sscanf(filepath.Base(p),"data-%d.wal",&seg);if seg<w.manifest.FirstSegment||seg>w.manifest.ActiveSegment{continue}
  e=readFrames(p,func(b []byte)error{var r Record;if e:=json.Unmarshal(b,&r);e!=nil{return fmt.Errorf("decode data record in %s: %w",p,e)};if r.Seq==0||r.EventID==""||len(r.Payload)==0{return fmt.Errorf("invalid data record in %s",p)};if r.Seq>max{max=r.Seq};return nil});if e!=nil{return e}
 }
 w.next=max+1;if w.next==0{w.next=1};return nil
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
 w.mu.Lock();defer w.mu.Unlock();if w.closed{return Record{},os.ErrClosed}
 usage,e:=w.diskUsage();if e!=nil{return Record{},e};if usage+int64(len(payload))+256>w.opts.MaxBytes-w.opts.ReserveBytes{return Record{},ErrCapacity}
 if w.data==nil{return Record{},os.ErrClosed}
 r:=Record{Seq:w.next,EventID:eventID,Payload:payload,CreatedAt:time.Now().UTC()};b,e:=json.Marshal(r);if e!=nil{return Record{},e}
 if e=writeFrame(w.data,b);e!=nil{return Record{},e};if w.opts.Sync{if e=w.data.Sync();e!=nil{return Record{},e}};w.next++
 st,e:=w.data.Stat();if e!=nil{return r,e};if st.Size()>=w.opts.SegmentBytes{if e=w.rotate();e!=nil{return r,e}};return r,nil
}
func (w *WAL) rotate()error{
 if w.data==nil{return os.ErrClosed}
 if e:=w.data.Sync();e!=nil{return e}
 if e:=w.data.Close();e!=nil{return e}
 old:=w.manifest.ActiveSegment
 w.manifest.ActiveSegment++
 if e:=w.saveManifest();e!=nil{w.manifest.ActiveSegment=old;_ = w.openDataOnly();return e}
 if e:=w.openDataOnly();e!=nil{return fmt.Errorf("manifest advanced to segment %d but opening it failed: %w",w.manifest.ActiveSegment,e)}
 d,e:=os.Open(w.opts.Dir);if e!=nil{return e};defer d.Close();return d.Sync()
}
func (w *WAL) openDataOnly()error{p:=filepath.Join(w.opts.Dir,fmt.Sprintf("data-%020d.wal",w.manifest.ActiveSegment));f,e:=os.OpenFile(p,os.O_CREATE|os.O_RDWR|os.O_APPEND,0600);w.data=f;return e}
func (w *WAL) Ack(a Ack)error{
 w.mu.Lock();defer w.mu.Unlock()
 if w.closed{return os.ErrClosed}
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
 w.mu.Lock();defer w.mu.Unlock();acked,e:=w.ackState();if e!=nil{return nil,e}
 files,e:=filepath.Glob(filepath.Join(w.opts.Dir,"data-*.wal"));if e!=nil{return nil,e};sort.Strings(files);var out []Record
 for _,p:=range files{var seg uint64;_,_=fmt.Sscanf(filepath.Base(p),"data-%d.wal",&seg);if seg<w.manifest.FirstSegment||seg>w.manifest.ActiveSegment{continue}
  e=readFrames(p,func(b []byte)error{var r Record;if json.Unmarshal(b,&r)!=nil||r.Seq<=w.manifest.CheckpointSeq{return nil};if !acked["mqtt"][r.Seq]||!acked["influx"][r.Seq]{out=append(out,r)};return nil});if e!=nil{return nil,e}
 };return out,nil
}
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
 for _,p:=range files{
  var seg uint64;_,_=fmt.Sscanf(filepath.Base(p),"data-%d.wal",&seg)
  if seg<w.manifest.FirstSegment||seg>w.manifest.ActiveSegment{continue}
  e=readFrames(p,func(b []byte)error{
   var r Record;if e:=json.Unmarshal(b,&r);e!=nil{return e}
   if r.Seq<=seq&&(!acked["mqtt"][r.Seq]||!acked["influx"][r.Seq]){return fmt.Errorf("cannot checkpoint unacknowledged seq %d",r.Seq)}
   return nil
  });if e!=nil{return e}
 }
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
 return nil
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
func (w *WAL) Close()error{
 w.mu.Lock();defer w.mu.Unlock();if w.closed{return nil};w.closed=true;var es []string
 if w.data!=nil{if e:=w.data.Sync();e!=nil{es=append(es,e.Error())};if e:=w.data.Close();e!=nil{es=append(es,e.Error())}}
 if w.ack!=nil{if e:=w.ack.Sync();e!=nil{es=append(es,e.Error())};if e:=w.ack.Close();e!=nil{es=append(es,e.Error())}}
 if w.lock!=nil{if e:=w.lock.close();e!=nil{es=append(es,e.Error())}};if len(es)>0{return fmt.Errorf("%s",strings.Join(es,"; "))};return nil
}
