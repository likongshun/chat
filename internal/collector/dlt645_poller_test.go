package collector

import (
	"context"
	"fmt"
	"io"
	"net"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"testing"

	"github.com/likongshun/chat/internal/dlt645"
	"github.com/likongshun/chat/internal/config"
)

func TestDecode645ValueBCDAndScale(t *testing.T) {
	got, err := decode645Value([]byte{0x00, 0x00, 0x01, 0x00, 0x34, 0x12}, "00010000", "BCD", 2, 0.01, 0)
	if err != nil { t.Fatal(err) }
	if math.Abs(got-12.34) > 1e-9 { t.Fatalf("got %v want 12.34", got) }
}

func TestDecode645ValueRejectsBadBCDAndIdentifier(t *testing.T) {
	if _, err := decode645Value([]byte{0, 0, 1, 0, 0xfa}, "00010000", "BCD", 1, 1, 0); err == nil { t.Fatal("expected invalid BCD") }
	if _, err := decode645Value([]byte{0, 0, 1, 0, 1}, "00020000", "BCD", 1, 1, 0); err == nil { t.Fatal("expected data identifier mismatch") }
}

func TestValidateReadResponseControl(t *testing.T) {
	if err := validateReadResponseControl(0x91); err != nil { t.Fatal(err) }
	if err := validateReadResponseControl(0xB1); err != nil { t.Fatal("continuation response rejected:", err) }
	for _, control := range []byte{0x11, 0xD1, 0x92} {
		if err := validateReadResponseControl(control); err == nil { t.Fatalf("control 0x%02x unexpectedly accepted", control) }
	}
}

func TestWriteAllHandlesShortWrites(t *testing.T) {
	w := &shortWriter{max:2}
	if err := writeAll(w, []byte{1,2,3,4,5}); err != nil { t.Fatal(err) }
	if string(w.data) != string([]byte{1,2,3,4,5}) { t.Fatalf("unexpected output %v", w.data) }
}

type shortWriter struct { data []byte; max int }
func (w *shortWriter) Write(p []byte) (int,error) {
	if len(p) > w.max { p = p[:w.max] }
	w.data = append(w.data, p...)
	return len(p),nil
}

func TestDecode645ValueLengthAndBCD(t *testing.T) {
	got, err := decode645Value([]byte{0x00,0x00,0x01,0x00,0x45,0x23,0x01}, "00010000", "BCD", 3, 0.01, 0)
	if err != nil { t.Fatal(err) }
	if got != 123.45 { t.Fatalf("got %v, want 123.45", got) }
	if _, err := decode645Value([]byte{0x00,0x00,0x01,0x00,0x45}, "00010000", "BCD", 3, 1, 0); err == nil { t.Fatal("expected length mismatch") }
	if _, err := decode645Value([]byte{0x00,0x00,0x01,0x00,0x4a,0x23,0x01}, "00010000", "BCD", 3, 1, 0); err == nil { t.Fatal("expected invalid BCD") }
}

func TestGatewayAcquisitionHonorsContextCancellation(t *testing.T) {
	p := &DL645Poller{}
	release, err := p.acquireGateway(context.Background(), "gateway:4001")
	if err != nil { t.Fatal(err) }
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := p.acquireGateway(ctx, "gateway:4001"); err == nil { t.Fatal("expected canceled acquisition") }
	release()
	next, err := p.acquireGateway(context.Background(), "gateway:4001")
	if err != nil { t.Fatal(err) }
	next()
}

func TestGatewayAcquisitionSerializesAndReleases(t *testing.T) {
	p := &DL645Poller{}
	release, err := p.acquireGateway(context.Background(), "gateway:4001")
	if err != nil { t.Fatal(err) }
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if _, err := p.acquireGateway(ctx, "gateway:4001"); err == nil { t.Fatal("expected serialized acquisition timeout") }
	release()
	next, err := p.acquireGateway(context.Background(), "gateway:4001")
	if err != nil { t.Fatal(err) }
	next()
}

func TestRead645PayloadAssemblesFollowFrames(t *testing.T) {
	address, dataID := "123456789012", "00010000"
	firstData := []byte{0x00,0x00,0x01,0x00,0x12}
	firstRaw := test645RawFrame(address, 0xB1, add645Offset(firstData))
	first, err := dlt645.DecodeResponse(firstRaw, address)
	if err != nil { t.Fatal(err) }
	client, peer := net.Pipe()
	defer client.Close()
	defer peer.Close()
	serverErr := make(chan error, 1)
	go func() {
		for seq, control := range []byte{0xB2,0x92} {
			request := make([]byte, 17)
			if _, err := io.ReadFull(peer, request); err != nil { serverErr <- err; return }
			if request[8] != dlt645.ReadFollow || request[9] != 5 || request[14] != byte(seq+1)+0x33 { serverErr <- fmt.Errorf("bad continuation request %x", request); return }
			data := []byte{0x00,0x00,0x01,0x00,byte(0x34+seq),byte(seq+1)}
			if err := writeAll(peer, test645RawFrame(address, control, add645Offset(data))); err != nil { serverErr <- err; return }
		}
		serverErr <- nil
	}()
	got, err := read645Payload(client, first, address, dataID, time.Second, context.Background())
	if err != nil { t.Fatal(err) }
	if len(got) != 7 || got[4] != 0x12 || got[5] != 0x34 || got[6] != 0x35 { t.Fatalf("unexpected assembled payload %x", got) }
	if err := <-serverErr; err != nil { t.Fatal(err) }
}

func TestRead645PayloadRejectsSequenceMismatch(t *testing.T) {
	address, dataID := "123456789012", "00010000"
	first, err := dlt645.DecodeResponse(test645RawFrame(address, 0xB1, add645Offset([]byte{0,0,1,0,0x12})), address)
	if err != nil { t.Fatal(err) }
	client, peer := net.Pipe()
	defer client.Close()
	defer peer.Close()
	go func() {
		request := make([]byte, 17)
		_, _ = io.ReadFull(peer, request)
		bad := []byte{0,0,1,0,0x34,0x35} // sequence 2 while sequence 1 is expected
		_ = writeAll(peer, test645RawFrame(address, 0x92, add645Offset(bad)))
	}()
	if _, err := read645Payload(client, first, address, dataID, time.Second, context.Background()); err == nil { t.Fatal("expected sequence mismatch") }
}

func add645Offset(data []byte) []byte {
	out := append([]byte(nil), data...)
	for i := range out { out[i] += 0x33 }
	return out
}

func test645RawFrame(address string, control byte, data []byte) []byte {
	addr := make([]byte, 6)
	for i := 0; i < 6; i++ { addr[i] = (address[10-2*i]-'0')<<4 | (address[11-2*i]-'0') }
	out := []byte{0x68}
	out = append(out, addr...)
	out = append(out, 0x68, control, byte(len(data)))
	out = append(out, data...)
	var sum byte
	for _, b := range out { sum += b }
	return append(out, sum, 0x16)
}

func TestDecode645SignedBCD(t *testing.T) {
	got, err := decode645Value([]byte{0,0,1,0,0x34,0xB2}, "00010000", "BCD_SIGNED", 2, 0.01, 0)
	if err != nil { t.Fatal(err) }
	if math.Abs(got-(-2.34)) > 1e-9 { t.Fatalf("got %v, want -2.34", got) }
}

func TestRead645PayloadRejectsAssembledLengthOverflow(t *testing.T) {
	address, dataID := "123456789012", "00010000"
	first, err := dlt645.DecodeResponse(test645RawFrame(address, 0xB1, add645Offset([]byte{0,0,1,0,0x12})), address)
	if err != nil { t.Fatal(err) }
	client, peer := net.Pipe()
	defer client.Close()
	defer peer.Close()
	serverErr := make(chan error,1)
	go func() {
		for seq:=byte(1);seq<=22;seq++ {
			request:=make([]byte,17)
			if _,err:=io.ReadFull(peer,request);err!=nil{serverErr<-err;return}
			data:=make([]byte,200)
			copy(data,[]byte{0,0,1,0})
			data[199]=seq
			control:=byte(0xB2)
			if err:=writeAll(peer,test645RawFrame(address,control,add645Offset(data)));err!=nil{serverErr<-err;return}
		}
		serverErr<-nil
	}()
	_, err = read645Payload(client,first,address,dataID,time.Second,context.Background())
	if err==nil || !strings.Contains(err.Error(),"exceeds 4096 bytes"){t.Fatalf("expected assembled length rejection, got %v",err)}
	if serverErr:=<-serverErr;serverErr!=nil{t.Fatal(serverErr)}
}

func TestSerialDevicesOnSamePhysicalPortShareBusLockKey(t *testing.T) {
	a := config.Device{Transport:"serial", SerialPort:"/dev/ttyUSB0", BaudRate:2400}
	b := config.Device{Transport:"serial", SerialPort:"/dev/ttyUSB0", BaudRate:9600}
	if dlt645BusKey(a) != dlt645BusKey(b) {
		t.Fatalf("same physical serial port must share lock: %q != %q", dlt645BusKey(a), dlt645BusKey(b))
	}
	if dlt645Endpoint(a) == dlt645Endpoint(b) {
		t.Fatal("different serial modes should produce distinct endpoints")
	}
}

func TestTCPDevicesUseGatewayAsBusLockKey(t *testing.T) {
	a := config.Device{Host:"127.0.0.1", Port:4001}
	b := config.Device{Host:"127.0.0.1", Port:4001}
	if dlt645BusKey(a) != dlt645BusKey(b) {
		t.Fatal("devices sharing a TCP gateway must share the bus lock")
	}
}

func TestDLT645SharedSerialBusLockSerializesConcurrentDevices(t *testing.T) {
	poller := &DL645Poller{}
	const workers = 64
	var active atomic.Int32
	var maximum atomic.Int32
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			device := config.Device{Transport: "serial", SerialPort: "/dev/ttyUSB0", BaudRate: 2400}
			release, err := poller.acquireGateway(context.Background(), dlt645BusKey(device))
			if err != nil {
				t.Errorf("acquire shared bus: %v", err)
				return
			}
			current := active.Add(1)
			for {
				old := maximum.Load()
				if current <= old || maximum.CompareAndSwap(old, current) {
					break
				}
			}
			time.Sleep(time.Millisecond)
			active.Add(-1)
			release()
		}()
	}
	close(start)
	wg.Wait()
	if got := maximum.Load(); got != 1 {
		t.Fatalf("maximum concurrent users on one RS485 bus = %d, want 1", got)
	}
}

func TestDLT645SharedSerialBusLockHonorsCancellation(t *testing.T) {
	poller := &DL645Poller{}
	key := dlt645BusKey(config.Device{Transport: "serial", SerialPort: "/dev/ttyUSB0"})
	release, err := poller.acquireGateway(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := poller.acquireGateway(ctx, key); err == nil {
		t.Fatal("waiting device unexpectedly acquired an occupied RS485 bus")
	}
}


func TestDecode645PointValueUsesFormatPrecisionAndCalibration(t *testing.T) {
	point := config.Point{DataID:"00010000", Count:4, DataType:"BCD", DataFormat:"XXXXXX.XX", Scale:2, Offset:1}
	got, err := decode645PointValue([]byte{0,0,1,0,0x78,0x56,0x34,0x12}, point)
	if err != nil { t.Fatal(err) }
	if got != 246914.56 { t.Fatalf("got %v, want 246914.56", got) }
}

func TestDecode645PointValueReturnsTypedDateAndCompositeDemand(t *testing.T) {
	datePoint := config.Point{DataID:"04000102", Count:3, DataType:"BCD", DataFormat:"hhmmss", Scale:1}
	got, err := decode645PointValue([]byte{0,0,1,4,0x56,0x34,0x12}, datePoint)
	if err != nil { t.Fatal(err) }
	if got != "123456" { t.Fatalf("got %#v, want time string", got) }

	demandPoint := config.Point{DataID:"01010000", Count:8, DataType:"BCD", DataFormat:"XXX.XXX+YYMMDDhhmm", Scale:1}
	payload := []byte{0,0,0,1,0,0,0,0}
	// Three little-endian BCD bytes for 123.456 followed by five date/time bytes.
	copy(payload[3:], []byte{0x56,0x34,0x12,0x26,0x11})
	// Rebuild the DI prefix independently of the encoded field bytes.
	wire := []byte{0,0,1,0,0x56,0x34,0x12,0x26,0x11}
	got, err = decode645PointValue(wire, demandPoint)
	if err != nil { t.Fatal(err) }
	composite, ok := got.(map[string]any)
	if !ok { t.Fatalf("got %T, want composite map", got) }
	if composite["value"] != float64(123456) { t.Fatalf("unexpected demand value: %#v", composite) }
	if composite["occurred_at"] == nil { t.Fatal("missing demand occurrence time") }
}
