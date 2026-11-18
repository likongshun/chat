package collector

import (
	"context"
	"errors"
	"testing"

	"github.com/likongshun/chat/internal/config"
)

type testProtocol struct {
	name string
	calls int
	closed int
	closeErr error
}

func (p *testProtocol) Protocol() string { return p.name }
func (p *testProtocol) Poll(context.Context, config.Device) ([]Sample, error) {
	p.calls++
	return []Sample{{DeviceID:"test", Protocol:p.name}}, nil
}
func (p *testProtocol) Close() error { p.closed++; return p.closeErr }

func TestProtocolRouterDispatchAndDefault(t *testing.T) {
	modbus := &testProtocol{name:"modbus-tcp"}
	dlt := &testProtocol{name:"dl-t645-2007"}
	r := &ProtocolRouter{Engines:map[string]ProtocolPoller{"modbus-tcp":modbus,"dl-t645-2007":dlt}}
	if _,err:=r.Poll(context.Background(),config.Device{ID:"m"}); err!=nil { t.Fatal(err) }
	if _,err:=r.Poll(context.Background(),config.Device{ID:"d",Protocol:" DL-T645-2007 "}); err!=nil { t.Fatal(err) }
	if modbus.calls!=1 || dlt.calls!=1 { t.Fatalf("dispatch counts modbus=%d dlt=%d",modbus.calls,dlt.calls) }
	if _,err:=r.Poll(context.Background(),config.Device{Protocol:"unknown"}); err==nil { t.Fatal("expected unknown protocol error") }
}

func TestProtocolRouterCloseDeduplicatesAndReturnsErrors(t *testing.T) {
	closeErr:=errors.New("close failure")
	shared:=&testProtocol{name:"shared",closeErr:closeErr}
	r:=&ProtocolRouter{Engines:map[string]ProtocolPoller{"a":shared,"b":shared}}
	err:=r.Close()
	if !errors.Is(err,closeErr) { t.Fatalf("expected close error, got %v",err) }
	if shared.closed!=1 { t.Fatalf("shared engine closed %d times",shared.closed) }
}
