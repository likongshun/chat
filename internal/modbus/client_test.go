package modbus

import (\n\t"github.com/likongshun/chat/internal/config"
	"context"
	"encoding/binary"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

func startTestServer(t *testing.T) (string, *atomic.Int32, func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil { t.Fatal(err) }
	var accepts atomic.Int32
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, err := ln.Accept()
			if err != nil { return }
			accepts.Add(1)
			go func(c net.Conn) {
				defer c.Close()
				for {
					var h [7]byte
					if _, err := io.ReadFull(c, h[:]); err != nil { return }
					n := int(binary.BigEndian.Uint16(h[4:6])) - 1
					if n < 1 || n > 253 { return }
					pdu := make([]byte, n)
					if _, err := io.ReadFull(c, pdu); err != nil { return }
					fn := pdu[0]
					var data []byte
					switch fn {
					case 1, 2:
						count := binary.BigEndian.Uint16(pdu[3:5])
						byteCount := int((count+7)/8)
						data = make([]byte, byteCount+1)
						data[0] = byte(byteCount)
						if byteCount > 0 { data[1] = 0x05 }
					case 3, 4:
						count := int(binary.BigEndian.Uint16(pdu[3:5]))
						data = make([]byte, 1+count*2)
						data[0] = byte(count*2)
						for i:=0;i<count;i++ { binary.BigEndian.PutUint16(data[1+i*2:], uint16(100+i)) }
					default:
						return
					}
					resp := make([]byte, 7+1+len(data))
					copy(resp[:2], h[:2])
					binary.BigEndian.PutUint16(resp[4:6], uint16(1+1+len(data)))
					resp[6] = h[6]
					resp[7] = fn
					copy(resp[8:], data)
					if _, err := c.Write(resp); err != nil { return }
				}
			}(conn)
		}
	}()
	return ln.Addr().String(), &accepts, func() { _ = ln.Close(); <-done }
}

func TestClientReusesConnectionAndReadsFC03(t *testing.T) {
	address, accepts, stop := startTestServer(t)
	defer stop()
	c := &Client{Address:address, Timeout:time.Second}
	ctx := context.Background()
	first, err := c.ReadHolding(ctx, 1, 0, 2)
	if err != nil { t.Fatal(err) }
	second, err := c.ReadInput(ctx, 1, 2, 1)
	if err != nil { t.Fatal(err) }
	if len(first)!=2 || first[0]!=100 || first[1]!=101 || len(second)!=1 || second[0]!=100 {
		t.Fatalf("unexpected register values: %v / %v", first, second)
	}
	if got:=accepts.Load(); got!=1 { t.Fatalf("expected one TCP connection, got %d", got) }
	if err:=c.Close(); err!=nil { t.Fatal(err) }
	if _,err:=c.ReadHolding(ctx,1,0,1); err!=ErrClosed { t.Fatalf("expected ErrClosed, got %v",err) }
}

func TestClientReadsFC01AndFC02(t *testing.T) {
	address, _, stop := startTestServer(t)
	defer stop()
	c := &Client{Address:address, Timeout:time.Second}
	coils, err := c.ReadCoils(context.Background(), 1, 0, 3)
	if err != nil { t.Fatal(err) }
	if len(coils)!=3 || !coils[0] || coils[1] || !coils[2] { t.Fatalf("unexpected coils: %v",coils) }
	inputs, err := c.ReadDiscreteInputs(context.Background(), 1, 0, 2)
	if err != nil { t.Fatal(err) }
	if len(inputs)!=2 || !inputs[0] || inputs[1] { t.Fatalf("unexpected discrete inputs: %v",inputs) }
	_ = c.Close()
}

func TestReadLimits(t *testing.T) {
	c := &Client{Address:"127.0.0.1:1"}
	if _,err:=c.ReadHolding(context.Background(),1,0,126);err==nil { t.Fatal("expected register limit error") }
	if _,err:=c.ReadCoils(context.Background(),1,0,2001);err==nil { t.Fatal("expected bit limit error") }
}

func TestBuildReadBatchesMergesAdjacentAndRespectsFunction(t *testing.T) {
	points := []config.Point{
		{PointCode:"c",Function:3,Address:4,Count:2,Enabled:true},
		{PointCode:"a",Function:3,Address:0,Count:2,Enabled:true},
		{PointCode:"b",Function:3,Address:2,Count:2,Enabled:true},
		{PointCode:"coil",Function:1,Address:0,Count:8,Enabled:true},
		{PointCode:"off",Function:3,Address:8,Count:1,Enabled:false},
	}
	batches, err := BuildReadBatches(points,0)
	if err != nil { t.Fatal(err) }
	if len(batches)!=2 { t.Fatalf("expected two batches, got %#v",batches) }
	var registers *ReadBatch
	for i:=range batches { if batches[i].Function==3 { registers=&batches[i] } }
	if registers==nil || registers.Start!=0 || registers.Count!=6 || len(registers.Spans)!=3 {
		t.Fatalf("unexpected register batch: %#v",registers)
	}
	if registers.Spans[2].Offset!=4 { t.Fatalf("unexpected span offset: %#v",registers.Spans[2]) }
}

func TestBuildReadBatchesSplitsAtProtocolLimit(t *testing.T) {
	points:=[]config.Point{
		{PointCode:"a",Function:3,Address:0,Count:100,Enabled:true},
		{PointCode:"b",Function:3,Address:100,Count:25,Enabled:true},
		{PointCode:"c",Function:3,Address:125,Count:2,Enabled:true},
	}
	batches,err:=BuildReadBatches(points,0)
	if err!=nil {t.Fatal(err)}
	if len(batches)!=2 || batches[0].Count!=125 || batches[1].Count!=2 {t.Fatalf("unexpected split: %#v",batches)}
}
