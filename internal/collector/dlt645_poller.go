package collector

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
		"time"

	"github.com/likongshun/chat/internal/config"
	"github.com/likongshun/chat/internal/dlt645"
	"github.com/likongshun/chat/internal/processing"
)

// DL645Poller communicates with a DL/T645-2007 meter through a transparent
// TCP-to-RS485 gateway. The gateway must provide a raw byte stream; it must
// not modify DL/T645 frames. Native serial ports can be added as another
// transport without changing this protocol engine.
type DL645Poller struct {
	Config config.Config
	Latest *LatestStore
	ConfigVersion int64
	Timeout time.Duration
}

func (p *DL645Poller) Protocol() string { return "dl-t645-2007" }
func (p *DL645Poller) Close() error { return nil }

func (p *DL645Poller) Poll(ctx context.Context, d config.Device) ([]Sample, error) {
	if !d.Enabled { return nil,nil }
	address := strings.TrimSpace(d.MeterAddress)
	if address == "" { return nil, fmt.Errorf("device %s: meter_address is required for DL/T645", d.ID) }
	points:=make([]config.Point,0)
	for _,pt:=range p.Config.Points { if pt.Enabled && pt.Model==d.Model { points=append(points,pt) } }
	var samples []Sample
	var failures []error
	timeout:=p.Timeout; if timeout<=0 { timeout=1500*time.Millisecond }
	for _,pt:=range points {
		if err:=ctx.Err(); err!=nil { return samples,err }
		now:=time.Now().UTC()
		s:=Sample{DeviceID:d.ID,Model:d.Model,PointCode:pt.PointCode,Unit:pt.Unit,ConfigVersion:p.ConfigVersion,Timestamp:now,Quality:QualityGood}
		s.EventID=fmt.Sprintf("%s/%s/%d",d.ID,pt.PointCode,now.UnixNano())
		if len(pt.DataID)!=8 {
			s.Quality=QualityBad; s.Error=fmt.Sprintf("point %s: data_id must be 8 hex digits",pt.PointCode); samples=append(samples,s); failures=append(failures,fmt.Errorf("%s: %s",pt.PointCode,s.Error)); continue
		}
		request,err:=dlt645.EncodeReadData(address,pt.DataID)
		if err==nil {
			var conn net.Conn
			dialer:=net.Dialer{Timeout:timeout}
			conn,err=dialer.DialContext(ctx,"tcp",net.JoinHostPort(d.Host,strconv.Itoa(int(d.Port))))
			if err==nil {
				deadline:=time.Now().Add(timeout); if dl,ok:=ctx.Deadline(); ok && dl.Before(deadline) { deadline=dl }
				_ = conn.SetDeadline(deadline)
				_,err=conn.Write(request)
				if err==nil {
					header:=make([]byte,10)
					_,err=io.ReadFull(conn,header)
					if err==nil {
						n:=int(header[9])
						if n>200 { err=fmt.Errorf("DL/T645 response length %d exceeds limit",n) } else {
							raw:=make([]byte,12+n)
							copy(raw,header)
							_,err=io.ReadFull(conn,raw[10:])
							if err==nil {
								var frame dlt645.Frame
								frame,err=dlt645.DecodeResponse(raw,address)
								if err==nil {
									if frame.Control&0x40!=0 { err=fmt.Errorf("meter returned DL/T645 error response") } else {
										value,decodeErr:=decode645Value(frame.Data,pt.DataID,pt.Scale,pt.Offset)
										if decodeErr!=nil { err=decodeErr } else {
											value,err=processing.Apply(pt.PointCode,value,p.Config.Processing,p.Config.Enums)
											if err==nil { s.Value=value }
										}
									}
								}
							}
						}
					}
				}
				_ = conn.Close()
			}
		}
		if err!=nil { s.Quality=QualityCommError; s.Error=err.Error(); failures=append(failures,fmt.Errorf("point %s: %w",pt.PointCode,err)) }
		samples=append(samples,s)
	}
	if p.Latest!=nil { p.Latest.Update(samples) }
	return samples,errors.Join(failures...)
}

func decode645Value(data []byte, expectedID string, scale, offset float64) (float64,error) {
	if len(data)<5 { return 0,fmt.Errorf("DL/T645 payload too short: %d",len(data)) }
	id:=strings.ToUpper(hex.EncodeToString([]byte{data[3],data[2],data[1],data[0]}))
	if id!=strings.ToUpper(expectedID) { return 0,fmt.Errorf("DL/T645 data identifier mismatch: got %s want %s",id,expectedID) }
	digits:=make([]byte,0,(len(data)-4)*2)
	for _,b:=range data[4:] {
		lo,hi:=b&0x0f,b>>4
		if lo>9 || hi>9 { return 0,fmt.Errorf("invalid BCD byte 0x%02x",b) }
		digits=append(digits,'0'+lo,'0'+hi)
	}
	n,err:=strconv.ParseFloat(string(reverseDigits(digits)),64)
	if err!=nil { return 0,fmt.Errorf("decode DL/T645 BCD: %w",err) }
	return n*scale+offset,nil
}

func reverseDigits(in []byte) []byte {
	out:=make([]byte,len(in)); for i:=range in { out[len(in)-1-i]=in[i] }; return out
}
