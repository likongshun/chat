package modbus

import (
	"fmt"
	"sort"

	"github.com/likongshun/chat/internal/config"
)

type PointSpan struct {
	PointCode string
	Address   uint16
	Count     uint16
	Offset    uint16
}

type ReadBatch struct {
	Function  byte
	ReadGroup string
	Start     uint16
	Count     uint16
	Spans     []PointSpan
}

type readPoint struct {
	function byte
	group string
	address uint16
	count uint16
	code string
}

func BuildReadBatches(points []config.Point, maxGap uint16) ([]ReadBatch, error) {
	items := make([]readPoint, 0, len(points))
	for _, p := range points {
		if !p.Enabled { continue }
		if p.Function < 1 || p.Function > 4 {
			return nil, fmt.Errorf("point %s: unsupported read function %d", p.PointCode, p.Function)
		}
		if p.Count == 0 { return nil, fmt.Errorf("point %s: zero read count", p.PointCode) }
		limit := uint32(125)
		if p.Function <= 2 { limit = 2000 }
		if uint32(p.Count) > limit { return nil, fmt.Errorf("point %s: count %d exceeds function limit %d", p.PointCode, p.Count, limit) }
		if uint32(p.Address)+uint32(p.Count) > 65536 {
			return nil, fmt.Errorf("point %s: address range exceeds Modbus address space", p.PointCode)
		}
		items = append(items, readPoint{p.Function, p.ReadGroup, p.Address, p.Count, p.PointCode})
	}
	sort.Slice(items, func(i, j int) bool {
		a, b := items[i], items[j]
		if a.function != b.function { return a.function < b.function }
		if a.group != b.group { return a.group < b.group }
		if a.address != b.address { return a.address < b.address }
		return a.code < b.code
	})

	var batches []ReadBatch
	for _, p := range items {
		limit := uint32(125)
		if p.function <= 2 { limit = 2000 }
		if len(batches) == 0 {
			batches = append(batches, newBatch(p))
			continue
		}
		last := &batches[len(batches)-1]
		end := uint32(last.Start) + uint32(last.Count)
		nextEnd := uint32(p.address) + uint32(p.count)
		gapEnd := end + uint32(maxGap)
		merge := last.Function == p.function && last.ReadGroup == p.group &&
			uint32(p.address) <= gapEnd && max(end, nextEnd)-uint32(last.Start) <= limit
		if !merge {
			batches = append(batches, newBatch(p))
			continue
		}
		newEnd := max(end, nextEnd)
		last.Spans = append(last.Spans, PointSpan{
			PointCode:p.code, Address:p.address, Count:p.count,
			Offset:uint16(uint32(p.address)-uint32(last.Start)),
		})
		last.Count = uint16(newEnd-uint32(last.Start))
	}
	return batches, nil
}

func newBatch(p readPoint) ReadBatch {
	return ReadBatch{
		Function:p.function, ReadGroup:p.group, Start:p.address, Count:p.count,
		Spans:[]PointSpan{{PointCode:p.code, Address:p.address, Count:p.count, Offset:0}},
	}
}

type BatchData struct {
	Registers []uint16
	Bits      []bool
}

func (c *Client) ExecuteBatch(ctx context.Context, unit byte, batch ReadBatch) (BatchData, error) {
	switch batch.Function {
	case 1:
		bits, err := c.ReadCoils(ctx, unit, batch.Start, batch.Count)
		return BatchData{Bits:bits}, err
	case 2:
		bits, err := c.ReadDiscreteInputs(ctx, unit, batch.Start, batch.Count)
		return BatchData{Bits:bits}, err
	case 3:
		regs, err := c.ReadHolding(ctx, unit, batch.Start, batch.Count)
		return BatchData{Registers:regs}, err
	case 4:
		regs, err := c.ReadInput(ctx, unit, batch.Start, batch.Count)
		return BatchData{Registers:regs}, err
	default:
		return BatchData{}, fmt.Errorf("unsupported Modbus read function %d", batch.Function)
	}
}
