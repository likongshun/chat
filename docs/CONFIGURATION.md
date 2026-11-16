# Excel device configuration

The collector uses `configs/modbus_device_config.xlsx` as the device-model catalog. Device-specific register maps stay outside the Modbus TCP client: adding a compatible Modbus TCP device model is normally an Excel/configuration change, not a protocol-core change.

## Workbook sheets

| Sheet | Purpose | Important fields |
|---|---|---|
| Devices | Physical device instances | `device_id`, `name`, `host`, `port`, `unit_id`, `model`, `poll_ms`, `enabled` |
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
