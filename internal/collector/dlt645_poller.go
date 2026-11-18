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
	"sync"
	"time"

	"github.com/likongshun/chat/internal/config"
	"github.com/likongshun/chat/internal/dlt645"
	"github.com/likongshun/chat/internal/processing"
)

// DL645Poller communicates with a meter through a transparent TCP-to-RS485
// gateway. Requests are serialized per gateway because its RS485 bus is
// half-duplex. A native serial transport can be added without changing Sample.
type DL645Poller struct {
	Config config.Config
	Latest *LatestStore
	ConfigVersion int64
	Timeout time.Duration
	Transport dlt645.Transport
	mu sync.Mutex
	gateways map[string]chan struct{}
}

func (p *DL645Poller) Protocol() string { return "dl-t645-2007" }
func (p *DL645Poller) Close() error { return nil }

func (p *DL645Poller) acquireGateway(ctx context.Context, gateway string) (func(), error) {
	p.mu.Lock()
	if p.gateways == nil { p.gateways = make(map[string]chan struct{}) }
	sem := p.gateways[gateway]
	if sem == nil { sem = make(chan struct{}, 1); p.gateways[gateway] = sem }
	p.mu.Unlock()
	select {
	case sem <- struct{}{}:
		return func() { <-sem }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (p *DL645Poller) Poll(ctx context.Context, d config.Device) ([]Sample, error) {
	if !d.Enabled { return nil, nil }
	gateway := net.JoinHostPort(d.Host, strconv.Itoa(int(d.Port)))
	release, err := p.acquireGateway(ctx, gateway)
	if err != nil { return nil, err }
	defer release()

	address := strings.TrimSpace(d.MeterAddress)
	if address == "" { return nil, fmt.Errorf("device %s: meter_address is required for DL/T645", d.ID) }
	points := make([]config.Point, 0)
	for _, pt := range p.Config.Points {
		if pt.Enabled && pt.Model == d.Model { points = append(points, pt) }
	}
	var samples []Sample
	var failures []error
	timeout := p.Timeout
	if timeout <= 0 { timeout = 1500 * time.Millisecond }

	for _, pt := range points {
		if err := ctx.Err(); err != nil { return samples, errors.Join(append(failures, err)...)}
		now := time.Now().UTC()
		s := Sample{DeviceID:d.ID, Protocol:"dl-t645-2007", Model:d.Model, PointCode:pt.PointCode, Unit:pt.Unit, ConfigVersion:p.ConfigVersion, Timestamp:now, Quality:QualityGood}
		s.EventID = fmt.Sprintf("%s/%s/%d", d.ID, pt.PointCode, now.UnixNano())
		request, err := dlt645.EncodeReadData(address, pt.DataID)
		if err == nil {
			conn, dialErr := (&net.Dialer{Timeout:timeout}).DialContext(ctx, "tcp", gateway)
			err = dialErr
			if err == nil {
				deadline := time.Now().Add(timeout)
				if dl, ok := ctx.Deadline(); ok && dl.Before(deadline) { deadline = dl }
				_ = conn.SetDeadline(deadline)
				err = writeAll(conn, request)
				if err == nil {
					var frame dlt645.Frame
					frame, err = read645Frame(conn, address)
					if err == nil {
						var payload []byte
						payload, err = read645Payload(conn, frame, address, pt.DataID, timeout, ctx)
						if err == nil {
							var decoded float64
							decoded, err = decode645Value(payload, pt.DataID, pt.DataType, pt.Count, pt.Scale, pt.Offset)
							if err == nil {
								var value any = decoded
								value, err = processing.Apply(pt.PointCode, value, p.Config.Processing, p.Config.Enums)
								if err == nil { s.Value = value }
							}
						}
					}
				}
				_ = conn.Close()
			}
		}
		if err != nil {
			// Valid frames with a meter error, unexpected function, invalid BCD
			// or processing failure are bad data; framing/network faults are link errors.
			s.Quality = QualityBad
			if errors.Is(err, dlt645.ErrFrame) || errors.Is(err, dlt645.ErrChecksum) || isNetworkError(err) {
				s.Quality = QualityCommError
			}
			s.Error = err.Error()
			failures = append(failures, fmt.Errorf("point %s: %w", pt.PointCode, err))
		}
		samples = append(samples, s)
	}
	if p.Latest != nil { p.Latest.Update(samples) }
	return samples, errors.Join(failures...)
}

func writeAll(w io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := w.Write(data)
		if err != nil { return err }
		if n == 0 { return io.ErrShortWrite }
		data = data[n:]
	}
	return nil
}

func validateReadResponseControl(control byte) error {
	if control&0x80 == 0 || control&0x0f != 0x01 {
		return fmt.Errorf("unexpected DL/T645 read response control code 0x%02x", control)
	}
	if control&0x40 != 0 { return fmt.Errorf("meter returned DL/T645 error response (control 0x%02x)", control) }
	return nil
}

func read645Payload(conn net.Conn, frame dlt645.Frame, address, dataID string, timeout time.Duration, ctx context.Context) ([]byte, error) {
	if err := validateReadResponseControl(frame.Control); err != nil { return nil, err }
	if len(frame.Data) < 4 { return nil, dlt645.ErrFrame }
	id := strings.ToUpper(hex.EncodeToString([]byte{frame.Data[3], frame.Data[2], frame.Data[1], frame.Data[0]}))
	if id != strings.ToUpper(strings.TrimSpace(dataID)) { return nil, fmt.Errorf("DL/T645 data identifier mismatch: got %s want %s", id, dataID) }
	payload := append([]byte(nil), frame.Data[4:]...)
	if frame.Control&0x20 == 0 { return payloadWithID(frame.Data), nil }
	for seq := byte(1); ; seq++ {
		if seq > 32 { return nil, fmt.Errorf("DL/T645 response exceeds 32 follow-up frames") }
		request, err := dlt645.EncodeReadFollow(address, dataID, seq)
		if err != nil { return nil, err }
		deadline := time.Now().Add(timeout)
		if dl, ok := ctx.Deadline(); ok && dl.Before(deadline) { deadline = dl }
		if err := conn.SetDeadline(deadline); err != nil { return nil, err }
		if err := writeAll(conn, request); err != nil { return nil, err }
		next, err := read645Frame(conn, address)
		if err != nil { return nil, err }
		if next.Control != 0x92 && next.Control != 0xB2 {
			return nil, fmt.Errorf("unexpected DL/T645 follow-up response control code 0x%02x", next.Control)
		}
		if len(next.Data) < 5 { return nil, dlt645.ErrFrame }
		nextID := strings.ToUpper(hex.EncodeToString([]byte{next.Data[3], next.Data[2], next.Data[1], next.Data[0]}))
		if nextID != strings.ToUpper(strings.TrimSpace(dataID)) { return nil, fmt.Errorf("DL/T645 follow-up data identifier mismatch: got %s want %s", nextID, dataID) }
		gotSeq := next.Data[len(next.Data)-1]
		if gotSeq != seq { return nil, fmt.Errorf("DL/T645 follow-up sequence mismatch: got %d want %d", gotSeq, seq) }
		payload = append(payload, next.Data[4:len(next.Data)-1]...)
		if len(payload) > 4096 { return nil, fmt.Errorf("DL/T645 assembled payload exceeds 32 bytes") }
		if next.Control&0x20 == 0 { return append([]byte{frame.Data[0], frame.Data[1], frame.Data[2], frame.Data[3]}, payload...), nil }
	}
}

func payloadWithID(data []byte) []byte { return append([]byte(nil), data...) }

func isNetworkError(err error) bool {
	var netErr net.Error
	return errors.As(err, &netErr) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)
}

func decode645Value(data []byte, expectedID, dataType string, count uint16, scale, offset float64) (float64, error) {
	if len(data) < 5 { return 0, fmt.Errorf("DL/T645 payload too short: %d", len(data)) }
	id := strings.ToUpper(hex.EncodeToString([]byte{data[3], data[2], data[1], data[0]}))
	if id != strings.ToUpper(strings.TrimSpace(expectedID)) { return 0, fmt.Errorf("DL/T645 data identifier mismatch: got %s want %s", id, expectedID) }
	if count == 0 || count > 4096 { return 0, fmt.Errorf("invalid DL/T645 data length %d", count) }
	payload := data[4:]
	if len(payload) != int(count) { return 0, fmt.Errorf("DL/T645 data length mismatch: got %d want %d", len(payload), count) }
	if dataType != "BCD" && dataType != "BCD_SIGNED" { return 0, fmt.Errorf("unsupported DL/T645 data type %q", dataType) }
	digits := make([]byte, 0, len(payload)*2)
	for i := len(payload)-1; i >= 0; i-- {
		b := payload[i]
		hi, lo := b>>4, b&0x0f
		if hi > 9 || lo > 9 { return 0, fmt.Errorf("invalid BCD byte 0x%02x", b) }
		digits = append(digits, '0'+hi, '0'+lo)
	}
	negative := false
	if dataType == "BCD_SIGNED" && len(digits) > 0 {
		// DL/T645 signed BCD commonly uses the high nibble of the most
		// significant byte as a sign marker. Accept 0xB as negative.
		msb := payload[len(payload)-1]
		if msb>>4 == 0x0B {
			negative = true
			digits[0] = '0'
		}
	}
	n, err := strconv.ParseFloat(string(digits), 64)
	if err != nil { return 0, fmt.Errorf("decode DL/T645 BCD: %w", err) }
	if negative { n = -n }
	return n*scale + offset, nil
}


func read645Frame(r io.Reader, expectedAddress string) (dlt645.Frame, error) {
	// Optical/RS485 adapters may prepend 0xFE wake-up bytes.
	var start [1]byte
	for i := 0; i < 16; i++ {
		if _, err := io.ReadFull(r, start[:]); err != nil { return dlt645.Frame{}, err }
		if start[0] == dlt645.FrameStart { break }
		if i == 15 { return dlt645.Frame{}, dlt645.ErrFrame }
	}
	header := make([]byte, 9)
	if _, err := io.ReadFull(r, header); err != nil { return dlt645.Frame{}, err }
	if header[6] != dlt645.FrameStart { return dlt645.Frame{}, dlt645.ErrFrame }
	n := int(header[8])
	if n > 200 { return dlt645.Frame{}, fmt.Errorf("DL/T645 response length %d exceeds limit", n) }
	raw := make([]byte, 10+n+2)
	raw[0] = dlt645.FrameStart
	copy(raw[1:10], header)
	if _, err := io.ReadFull(r, raw[10:]); err != nil { return dlt645.Frame{}, err }
	return dlt645.DecodeResponse(raw, expectedAddress)
}
