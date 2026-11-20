# DL/T645-2007 load-record acquisition

## Scope

The collector now has a protocol-layer encoder for the standard 0x11 load-record read forms. This is deliberately separate from ordinary single-DI point reads.

The standard request data field is:

- Block-count form: `DI0 DI1 DI2 DI3 N` (L=05H).
- Time-and-block-count form: `DI0 DI1 DI2 DI3 N mm hh DD MM YY` (L=0AH).

All data-field bytes are transmitted with the DL/T645 +0x33 transform. DI is sent least-significant byte first. The timestamp fields are BCD and represent meter-local wall-clock time; the encoder does not convert time zones. The two-digit year is limited to 2000–2099.

Example API:

```go
// Read the newest 12 load-record blocks.
request, err := dlt645.EncodeReadLoadRecord(address, "04000001", 12, nil)

// Read 12 blocks beginning at the meter-local timestamp.
start := time.Date(2026, 11, 20, 13, 15, 0, 0, meterLocation)
request, err := dlt645.EncodeReadLoadRecord(address, "04000001", 12, &start)
```

## Response and continuation

A meter may answer with 0x91 (no continuation) or 0xB1 (more data follows). For continuation, the master uses 0x12 and the meter responds with 0x92 or 0xB2. The existing collector continuation reader validates the meter address, DI, sequence number, checksum and maximum assembled payload size.

## Deliberate limitations

This commit adds and tests request encoding only. It does not yet map arbitrary load-record byte layouts into telemetry points. The record content and interval can vary by DI, meter class and manufacturer; a model-specific record schema must define field order, byte widths, decimal precision, timestamp interpretation and supported interval before enabling Poller integration.

No write, freeze, clear-demand or clock-setting command is issued. Do not assume that every meter supports every load-record DI. Validate the selected DI and response against the exact meter manual and a captured physical-meter exchange before production use.
