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
						err = validateReadResponseControl(frame.Control)
						if err == nil {
							var decoded float64
							decoded, err = decode645Value(frame.Data, pt.DataID, pt.Scale, pt.Offset)
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
	// 0x91 is a normal read-data response; 0xD1 is the error response.
	// Reject write acknowledgements and all unrelated function codes.
	if control&0x80 == 0 || control&0x0f != 0x01 {
		return fmt.Errorf("unexpected DL/T645 read response control code 0x%02x", control)
	}
	if control&0x40 != 0 {
		return fmt.Errorf("meter returned DL/T645 error response (control 0x%02x)", control)
	}
	return nil
}

func isNetworkError(err error) bool {
	var netErr net.Error
	return errors.As(err, &netErr) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)
}

func decode645Value(data []byte, expectedID string, scale, offset float64) (float64, error) {
	if len(data) < 5 { return 0, fmt.Errorf("DL/T645 payload too short: %d", len(data)) }
	id := strings.ToUpper(hex.EncodeToString([]byte{data[3], data[2], data[1], data[0]}))
	if id != strings.ToUpper(expectedID) { return 0, fmt.Errorf("DL/T645 data identifier mismatch: got %s want %s", id, expectedID) }
	digits := make([]byte, 0, (len(data)-4)*2)
	for _, b := range data[4:] {
		lo, hi := b&0x0f, b>>4
		if lo > 9 || hi > 9 { return 0, fmt.Errorf("invalid BCD byte 0x%02x", b) }
		digits = append(digits, '0'+lo, '0'+hi)
	}
	n, err := strconv.ParseFloat(string(reverseDigits(digits)), 64)
	if err != nil { return 0, fmt.Errorf("decode DL/T645 BCD: %w", err) }
	return n*scale + offset, nil
}

func reverseDigits(in []byte) []byte {
	out := make([]byte, len(in))
	for i := range in { out[len(in)-1-i] = in[i] }
	return out
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
