package collector

import (
	"context"
	"testing"

	"github.com/likongshun/chat/internal/config"
)

type testProtocol struct { name string; calls int }
func (p *testProtocol) Protocol() string { return p.name }
func (p *testProtocol) Poll(context.Context, config.Device) ([]Sample,error) { p.calls++; return nil,nil }
func (p *testProtocol) Close() error { return nil }

func TestProtocolRouterDispatchAndDefault(t *testing.T) {
	modbus := &testProtocol{name:"modbus-tcp"}
	dlt := &testProtocol{name:"dl-t645-2007"}
	r := &ProtocolRouter{Engines:map[string]ProtocolPoller{"modbus-tcp":modbus,"dl-t645-2007":dlt}}
	if _,err:=r.Poll(context.Background(),config.Device{ID:"m"}); err!=nil { t.Fatal(err) }
	if _,err:=r.Poll(context.Background(),config.Device{ID:"d",Protocol:" DL-T645-2007 "}); err!=nil { t.Fatal(err) }
	if modbus.calls!=1 || dlt.calls!=1 { t.Fatalf("dispatch counts modbus=%d dlt=%d",modbus.calls,dlt.calls) }
	if _,err:=r.Poll(context.Background(),config.Device{Protocol:"unknown"}); err==nil { t.Fatal("expected unknown protocol error") }
}
