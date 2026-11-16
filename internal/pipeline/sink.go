package pipeline

import (
 "context"
 "encoding/json"
 "errors"
)
type Event struct { Seq uint64 `json:"seq"`; ID string `json:"id"`; Payload json.RawMessage `json:"payload"` }
type Sink interface { Name()string; Publish(context.Context,[]Event)error }
type Dispatcher struct { Sinks []Sink }
func (d Dispatcher) Publish(ctx context.Context,events []Event,ack func(uint64,string)error)error{
 if len(d.Sinks)==0{return errors.New("no sinks configured")}
 for _,s:=range d.Sinks{if e:=s.Publish(ctx,events);e!=nil{return e};for _,v:=range events{if e:=ack(v.Seq,s.Name());e!=nil{return e}}}
 return nil
}
