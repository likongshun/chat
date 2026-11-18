package collector

import (
	"context"
	"fmt"
	"strings"

	"github.com/likongshun/chat/internal/config"
)

// ProtocolPoller is implemented by each fieldbus acquisition engine.
// Implementations return the shared Sample model; persistence and downstream
// delivery remain protocol-independent.
type ProtocolPoller interface {
	Protocol() string
	Poll(context.Context, config.Device) ([]Sample, error)
	Close() error
}

// ProtocolRouter dispatches each device to an engine based on Device.Protocol.
// An empty protocol is kept backward-compatible and means modbus-tcp.
type ProtocolRouter struct {
	Engines map[string]ProtocolPoller
}

func (r *ProtocolRouter) engineFor(d config.Device) (ProtocolPoller, error) {
	name := strings.ToLower(strings.TrimSpace(d.Protocol))
	if name == "" { name = "modbus-tcp" }
	if r == nil { return nil, fmt.Errorf("protocol router is nil") }
	engine := r.Engines[name]
	if engine == nil { return nil, fmt.Errorf("no acquisition engine registered for protocol %q", name) }
	return engine, nil
}

func (r *ProtocolRouter) Poll(ctx context.Context, d config.Device) ([]Sample, error) {
	engine, err := r.engineFor(d)
	if err != nil { return nil, err }
	return engine.Poll(ctx, d)
}

func (r *ProtocolRouter) Close() error {
	var errs []error
	if r == nil { return nil }
	seen := make(map[ProtocolPoller]bool)
	for _, engine := range r.Engines {
		if engine == nil || seen[engine] { continue }
		seen[engine] = true
		if err := engine.Close(); err != nil { errs = append(errs, err) }
	}
	return errorsJoin(errs)
}
