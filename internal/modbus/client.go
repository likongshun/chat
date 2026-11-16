package modbus

import (
 "context"
 "encoding/binary"
 "errors"
 "fmt"
 "io"
 "net"
 "sync/atomic"
 "time"
)
var ErrException=errors.New("modbus exception")
type Client struct { Address string; Timeout time.Duration; tx atomic.Uint32 }
func (c *Client) request(ctx context.Context,unit,fn byte,payload []byte)([]byte,error){
 timeout:=c.Timeout;if timeout<=0{timeout=2*time.Second}
 d:=net.Dialer{Timeout:timeout};conn,e:=d.DialContext(ctx,"tcp",c.Address);if e!=nil{return nil,e};defer conn.Close()
 deadline:=time.Now().Add(timeout);if x,ok:=ctx.Deadline();ok&&x.Before(deadline){deadline=x};_ = conn.SetDeadline(deadline)
 id:=uint16(c.tx.Add(1));pdu:=append([]byte{fn},payload...)
 mbap:=make([]byte,8+len(payload));binary.BigEndian.PutUint16(mbap[0:2],id);binary.BigEndian.PutUint16(mbap[2:4],0);binary.BigEndian.PutUint16(mbap[4:6],uint16(1+len(pdu)));mbap[6]=unit;copy(mbap[7:],pdu)
 if _,e=conn.Write(mbap);e!=nil{return nil,e}
 var h [7]byte;if _,e=io.ReadFull(conn,h[:]);e!=nil{return nil,e}
 n:=binary.BigEndian.Uint16(h[4:6]);if n<2||n>254{return nil,fmt.Errorf("invalid MBAP length %d",n)}
 body:=make([]byte,n-1);if _,e=io.ReadFull(conn,body);e!=nil{return nil,e}
 if binary.BigEndian.Uint16(h[:2])!=id{return nil,errors.New("transaction id mismatch")}
 if body[0]==fn|0x80{return nil,fmt.Errorf("%w: code %d",ErrException,body[1])};if body[0]!=fn{return nil,errors.New("function mismatch")}
 return body[1:],nil
}
func (c *Client) ReadHolding(ctx context.Context,unit byte,address,count uint16)([]uint16,error){return c.read(ctx,unit,3,address,count)}
func (c *Client) ReadInput(ctx context.Context,unit byte,address,count uint16)([]uint16,error){return c.read(ctx,unit,4,address,count)}
func (c *Client) read(ctx context.Context,unit,fn byte,address,count uint16)([]uint16,error){
 if count==0||count>125{return nil,errors.New("register count must be 1..125")}
 p:=make([]byte,4);binary.BigEndian.PutUint16(p,address);binary.BigEndian.PutUint16(p[2:],count)
 b,e:=c.request(ctx,unit,fn,p);if e!=nil{return nil,e};if len(b)<1||int(b[0])!=int(count)*2||len(b)!=int(b[0])+1{return nil,errors.New("invalid register response")}
 out:=make([]uint16,count);for i:=range out{out[i]=binary.BigEndian.Uint16(b[1+i*2:])};return out,nil
}
func (c *Client) WriteSingle(ctx context.Context,unit byte,address,value uint16)error{
 p:=make([]byte,4);binary.BigEndian.PutUint16(p,address);binary.BigEndian.PutUint16(p[2:],value)
 b,e:=c.request(ctx,unit,6,p);if e!=nil{return e};if len(b)!=4{return errors.New("invalid write echo")};return nil
}
