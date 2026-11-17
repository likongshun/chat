package collector

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/likongshun/chat/internal/config"
	"github.com/likongshun/chat/internal/modbus"
	"github.com/likongshun/chat/internal/processing"
)

type ModbusPoller struct {
	Config  config.Config
	Latest  *LatestStore
	ConfigVersion int64
	Timeout time.Duration
	MaxGap  uint16

	mu      sync.Mutex
	clients map[string]*modbus.Client
}

func (p *ModbusPoller) clientFor(d config.Device) *modbus.Client {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.clients == nil { p.clients = make(map[string]*modbus.Client) }
	if c := p.clients[d.ID]; c != nil { return c }
	address := net.JoinHostPort(d.Host, strconv.Itoa(int(d.Port)))
	timeout := p.Timeout
	if timeout <= 0 { timeout = 800 * time.Millisecond }
	c := &modbus.Client{Address:address, Timeout:timeout}
	p.clients[d.ID] = c
	return c
}

func (p *ModbusPoller) Poll(ctx context.Context, d config.Device) ([]Sample, error) {
	if !d.Enabled { return nil, nil }
	points := make([]config.Point, 0)
	byCode := make(map[string]config.Point)
	for _, point := range p.Config.Points {
		if point.Enabled && point.Model == d.Model {
			points = append(points, point)
			byCode[point.PointCode] = point
		}
	}
	batches, err := modbus.BuildReadBatches(points, p.MaxGap)
	if err != nil { return nil, err }
	client := p.clientFor(d)
	var samples []Sample
	var failures []error
	for _, batch := range batches {
		if err := ctx.Err(); err != nil { return samples, errors.Join(append(failures, err)...)}
		data, readErr := client.ExecuteBatch(ctx, d.UnitID, batch)
		now := time.Now().UTC()
		for _, span := range batch.Spans {
			point := byCode[span.PointCode]
			sample := Sample{
				DeviceID:d.ID, Model:d.Model, PointCode:point.PointCode, Unit:point.Unit, ConfigVersion:p.ConfigVersion,
				Timestamp:now, Quality:QualityGood,
			}
			sample.EventID = fmt.Sprintf("%s/%s/%d", d.ID, point.PointCode, now.UnixNano())
			if readErr != nil {
				sample.Quality = QualityCommError
				sample.Error = readErr.Error()
				samples = append(samples, sample)
				continue
			}
			var value any
			var decodeErr error
			if batch.Function <= 2 {
				end := int(span.Offset)+int(span.Count)
				if end > len(data.Bits) {
					decodeErr = fmt.Errorf("point %s: bit span exceeds batch response", point.PointCode)
				} else {
					value, decodeErr = modbus.DecodeBit(point, data.Bits[int(span.Offset):end])
				}
			} else {
				end := int(span.Offset)+int(span.Count)
				if end > len(data.Registers) {
					decodeErr = fmt.Errorf("point %s: register span exceeds batch response", point.PointCode)
				} else {
					value, decodeErr = modbus.DecodeRegisters(point, data.Registers[int(span.Offset):end])
				}
			}
			if decodeErr == nil {
				value, decodeErr = processing.Apply(point.PointCode, value, p.Config.Processing, p.Config.Enums)
			}
			if decodeErr != nil {
				sample.Quality = QualityBad
				sample.Error = decodeErr.Error()
			} else {
				sample.Value = value
			}
			samples = append(samples, sample)
		}
		if readErr != nil { failures = append(failures, fmt.Errorf("function %d start %d count %d: %w", batch.Function, batch.Start, batch.Count, readErr)) }
	}
	if p.Latest != nil { p.Latest.Update(samples) }
	return samples, errors.Join(failures...)
}

func (p *ModbusPoller) Close() error {
	p.mu.Lock()
	clients := p.clients
	p.clients = make(map[string]*modbus.Client)
	p.mu.Unlock()
	var failures []error
	for _, c := range clients { if err := c.Close(); err != nil { failures = append(failures, err) } }
	return errors.Join(failures...)
}
