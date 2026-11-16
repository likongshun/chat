package wal

import (
 "encoding/binary"
 "hash/crc32"
 "io"
 "os"
)

func writeFrame(f *os.File, b []byte) error {
 var h [8]byte
 binary.BigEndian.PutUint32(h[:4],uint32(len(b))); binary.BigEndian.PutUint32(h[4:],crc32.ChecksumIEEE(b))
 if _,e:=f.Write(h[:]);e!=nil{return e}; _,e:=f.Write(b); return e
}
func readFrames(path string, fn func([]byte)error) error {
 f,e:=os.Open(path); if e!=nil{return e}; defer f.Close()
 for { var h [8]byte; _,e=io.ReadFull(f,h[:]); if e==io.EOF{return nil}; if e==io.ErrUnexpectedEOF{return nil}; if e!=nil{return e}
 n:=binary.BigEndian.Uint32(h[:4]); if n==0||n>64*1024*1024{return nil}
 b:=make([]byte,n); if _,e=io.ReadFull(f,b);e!=nil{return nil}
 if crc32.ChecksumIEEE(b)!=binary.BigEndian.Uint32(h[4:]){return nil}
 if e=fn(b);e!=nil{return e}
 }
}
