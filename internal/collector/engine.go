package collector

import (
	"context"
	"errors"

	"github.com/likongshun/chat/internal/config"
)

type Engine struct {
	Scheduler Scheduler
	Poller    *ModbusPoller
	Router *ProtocolRouter
}

func (e *Engine) Run(ctx context.Context, devices []config.Device) <-chan Result {
	if e.Router != nil { return e.Scheduler.Run(ctx, devices, e.Router.Poll) }
	if e.Poller == nil {
		return e.Scheduler.Run(ctx, devices, func(context.Context, config.Device) ([]Sample,error) {
			return nil, errors.New("collector Modbus poller is not configured")
		})
	}
	return e.Scheduler.Run(ctx, devices, e.Poller.Poll)
}

func (e *Engine) Close() error {
	if e.Router != nil { return e.Router.Close() }
	if e.Poller == nil { return nil }
	return e.Poller.Close()
}
