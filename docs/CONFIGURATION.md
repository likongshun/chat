# Excel device configuration

The collector uses `configs/modbus_device_config.xlsx` as the device-model catalog. Device-specific register maps stay outside the Modbus TCP client: adding a compatible Modbus TCP device model is normally an Excel/configuration change, not a protocol-core change.

## Workbook sheets

| Sheet | Purpose | Important fields |
|---|---|---|
| Devices | Physical device instances | `device_id`, `protocol`, `meter_address`, `discovery_only`, `name`, `host`, `port`, `model`, `poll_ms`, `enabled` |
| Points | Register/bit definitions | `model`, `point_code`, `function`, `address`, `type`, `length/count`, `scale`, `offset`, `byte_order`, `word_order`, `unit`, `read_group`, `rw`, `enabled` |
| Processing | Predefined post-decode operations | `point_code`, `operation`, `param1`, `param2` |
| WriteRules | Explicit write permission constraints | `model`, `point_code`, `enabled`, `min`, `max`, `require_confirm` |
| Enums | Numeric/status labels | `point_code`, `value`, `label` |

Column lookup is header-based. Keep the sheet names and required headers stable when creating brand/model templates.

## Modbus function and data type rules

- FC01: read coils; point type must be BOOL.
- FC02: read discrete inputs; point type must be BOOL.
- FC03: read holding registers.
- FC04: read input registers.
- A single bit request is limited to 2,000 bits; a register request is limited to 125 registers.
- INT16/UINT16 use one register; INT32/UINT32/FLOAT32 use two; INT64/UINT64/FLOAT64 use four.
- STRING uses a configured register count.
- Supported byte order values: ABCD, BADC, CDAB, DCBA.
- Supported word order values: HIGH_LOW, LOW_HIGH.
- Adjacent/overlapping points are batch-read only when function, model and read group are compatible.

## DL/T645-2007 point rules

Set `Devices.protocol` to `dl-t645-2007`. For normal collection, configure the 12-digit decimal `meter_address`. By default `transport=tcp` and `host`/`port` identify a transparent TCP-to-RS485 gateway. Set `transport=serial` to use a local Linux serial device instead; then `serial_port` must identify the device path inside the collector container.

For initial commissioning, set optional `discovery_only=TRUE` and leave `meter_address` empty. Discovery-only devices are not polled; they are available only for the explicit read-only address discovery action and may be configured before any measurement points exist. After discovering the address, copy it into `meter_address` and set `discovery_only=FALSE` to enable collection. This prevents polling an unknown or placeholder address.

For a DL/T645 model, `Points` uses:

| Field | Meaning |
|---|---|
| `data_id` | 8 hexadecimal digits, conventional DI3..DI0 notation |
| `type` | `BCD` or `BCD_SIGNED` |
| `length` | number of data bytes after the four-byte data identifier; 1..4096 |
| `scale` | engineering multiplier |
| `offset` | engineering offset |
| `unit` | engineering unit |

`address`, `function`, `byte_order` and `word_order` are Modbus fields and are not used by the DL/T645 reader. Excel column `discovery_only` is optional and defaults to FALSE.

### Data identifier templates

The read-only catalog is available at `GET /api/v1/dlt645/templates?q=电能`. It currently includes common total-energy and phase voltage/current identifiers. The catalog provides suggested lengths/scales, not a substitute for the meter manual. Manufacturer firmware may vary in supported IDs, byte lengths, decimal places and signed representation.

### Multi-frame responses

The reader supports DL/T645-2007 multi-frame read responses. When the first response uses `B1H`, it requests subsequent frames with `12H`, validates each `92H/B2H` response, DI and sequence number, and assembles the data before decoding. It bounds the exchange to 32 follow-up frames and 4096 assembled data bytes. Incomplete, malformed, out-of-order or over-limit responses never produce a good-quality sample.

### Meter address discovery

The dashboard's **搜索电表地址** action calls `POST /api/v1/dlt645/discover` with a configured DL/T645 device ID. It sends the standard `13H` read-communication-address command using the `AA..AA` address field and validates the `93H` response.

The standard defines this command as point-to-point. **Isolate one meter on the RS485 segment before searching**; do not run discovery on a shared multidrop bus where multiple meters may answer simultaneously. The operation is read-only and does not change the meter or configuration. Copy the returned address into the Devices sheet and disable `discovery_only` when ready to collect.

### Transport abstraction

The DL/T645 poller uses a session/transport interface with both transparent TCP gateway and native serial implementations.

Optional Devices columns for native serial transport:

| Field | Default | Meaning |
|---|---:|---|
| `transport` | `tcp` | `tcp` or `serial` |
| `serial_port` | empty | Linux path such as `/dev/ttyUSB0` or `/dev/serial/by-id/...` |
| `baud_rate` | 9600 | Serial baud rate |
| `data_bits` | 8 | 5..8 data bits |
| `parity` | `even` | `none`, `even`, or `odd` |
| `stop_bits` | 1 | 1 or 2 stop bits |

The serial adapter enforces an absolute read deadline, closes the session when its context is cancelled, and serializes collection/discovery across all Device entries that refer to the same physical serial path. It does not enable container-wide privileged mode.

For Compose, use the optional overlay:

```bash
export RS485_DEVICE=/dev/ttyUSB0
export RS485_CONTAINER_DEVICE=/dev/ttyUSB0
export RS485_GID=$(stat -c '%g' "$RS485_DEVICE")
docker compose -f compose.yaml -f compose.serial.yaml up -d --build
```

The overlay maps only the selected host device and adds its numeric group ID. Configure `serial_port` using the container path. USB-RS485 adapters with automatic transmit/receive direction are supported; hardware requiring GPIO or Linux `TIOCSRS485` direction control still needs a dedicated adapter. Real device timing, isolation and bus behavior must be validated on the target hardware.

The exact `data_id`, data length, scale and signed representation must be taken from the meter manufacturer's DL/T645-2007 communication specification. Do not assume that two meter models expose identical data identifiers.

## Processing operators

Processing is a fixed allow-list; arbitrary formulas, scripts, and code are never evaluated.

| Operator | Meaning | Parameters |
|---|---|---|
| SCALE | `value * param1 + param2` | multiplier, offset |
| LINEAR | `value * param1 + param2` | slope, intercept |
| ABS | absolute value | none |
| CLAMP | clamp to inclusive range | minimum, maximum |
| BIT | extract a bit as BOOL | bit index 0..63 |
| ENUM | map current value to configured label | lookup in Enums |
| ROUND | round to decimal places | integer precision -9..9 |
| DIVIDE | divide by a constant | non-zero divisor in param1 |

Processing rules run in workbook order for a point. Invalid parameter ranges and non-finite values fail validation or mark the point sample bad.

## Write safety

Write execution is disabled in this release:
- writable Points are rejected by configuration validation;
- enabled WriteRules are rejected;
- the Modbus client exposes read functions only and has no write API.

Do not enable PLC/VFD writes by changing Excel alone. A future write feature requires a separate reviewed implementation, explicit point allow-list, device manual verification, safety interlock review, and on-site approval.

## Configuration changes and reload

The collector loads the active validated configuration version from PostgreSQL at startup. If no active version exists, it imports the workbook and activates the new version. Set `CONFIG_IMPORT_ON_START=true` to import and activate the workbook on the next startup. The repository also supports explicit version activation and version listing. Automatic file watching and live hot reload of the running scheduler are not yet connected; restart the collector after changing configuration.

## Adding a device model

1. Add the physical device instance to Devices and assign a new `device_id`.
2. Add the model's point/register map to Points.
3. Add optional predefined Processing and Enums rows.
4. Keep all WriteRules disabled.
5. Run `go test ./...` and validate against the manufacturer's register map before connecting a real device.
