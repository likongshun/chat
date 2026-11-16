package collector

import (
	"context"
	"errors"

	"github.com/likongshun/chat/internal/config"
)

type Engine struct {
	Scheduler Scheduler
	Poller    *ModbusPoller
}

func (e *Engine) Run(ctx context.Context, devices []config.Device) <-chan Result {
	if e.Poller == nil {
		return e.Scheduler.Run(ctx, devices, func(context.Context, config.Device) ([]Sample,error) {
			return nil, errors.New("collector Modbus poller is not configured")
		})
	}
	return e.Scheduler.Run(ctx, devices, e.Poller.Poll)
}

func (e *Engine) Close() error {
	if e.Poller == nil { return nil }
	return e.Poller.Close()
}
