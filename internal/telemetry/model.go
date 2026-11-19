package telemetry

import (
 "fmt"
 "math"
 "strings"
 "time"
)

type Protocol string
const (
 ProtocolModbusTCP Protocol = "modbus-tcp"
 ProtocolModbusRTU Protocol = "modbus-rtu"
 ProtocolDLT645 Protocol = "dl-t645-2007"
 ProtocolSNMP Protocol = "snmp"
 ProtocolBACnet Protocol = "bacnet"
 ProtocolOPCUA Protocol = "opc-ua"
 ProtocolIEC61850 Protocol = "iec-61850"
 ProtocolIEC104 Protocol = "iec-104"
 ProtocolMQTT Protocol = "mqtt"
)

func NormalizeProtocol(v string) (Protocol, error) {
 switch strings.ToLower(strings.TrimSpace(v)) {
 case "modbus", "modbus-tcp", "modbus_tcp": return ProtocolModbusTCP, nil
 case "modbus-rtu", "modbus_rtu": return ProtocolModbusRTU, nil
 case "dl-t645", "dlt645", "dl/t645-2007", "dl-t645-2007": return ProtocolDLT645, nil
 case "snmp", "snmp-v2c", "snmp-v3": return ProtocolSNMP, nil
 case "bacnet", "bacnet-ip": return ProtocolBACnet, nil
 case "opcua", "opc-ua": return ProtocolOPCUA, nil
 case "iec61850", "iec-61850": return ProtocolIEC61850, nil
 case "iec104", "iec-104": return ProtocolIEC104, nil
 case "mqtt": return ProtocolMQTT, nil
 default: return "", fmt.Errorf("unsupported protocol %q", v)
 }
}

type Quality string
const (
 QualityGood Quality = "GOOD"
 QualityBad Quality = "BAD"
 QualityUncertain Quality = "UNCERTAIN"
 QualityStale Quality = "STALE"
)

type QualityReason string
const (
 ReasonNone QualityReason = ""
 ReasonTimeout QualityReason = "TIMEOUT"
 ReasonCRCError QualityReason = "CRC_ERROR"
 ReasonProtocolError QualityReason = "PROTOCOL_ERROR"
 ReasonDecodeError QualityReason = "DECODE_ERROR"
 ReasonOutOfRange QualityReason = "OUT_OF_RANGE"
 ReasonNonFinite QualityReason = "NON_FINITE"
 ReasonStaleTimeout QualityReason = "STALE_TIMEOUT"
 ReasonDeviceError QualityReason = "DEVICE_ERROR"
 ReasonSourceStale QualityReason = "SOURCE_STALE"
 ReasonEstimated QualityReason = "ESTIMATED_VALUE"
 ReasonUnknown QualityReason = "UNKNOWN_ERROR"
)

type Device struct {
 TenantID string `json:"tenant_id,omitempty"`
 SiteID string `json:"site_id,omitempty"`
 BuildingID string `json:"building_id,omitempty"`
 RoomID string `json:"room_id,omitempty"`
 RowID string `json:"row_id,omitempty"`
 RackID string `json:"rack_id,omitempty"`
 ID string `json:"device_id"`
 Name string `json:"name,omitempty"`
 Protocol Protocol `json:"protocol"`
 Model string `json:"model"`
 ModelVersion string `json:"model_version,omitempty"`
 ConfigVersion int64 `json:"config_version"`
 Enabled bool `json:"enabled"`
 DiscoveryOnly bool `json:"discovery_only,omitempty"`
 UpdatedAt time.Time `json:"updated_at,omitempty"`
}

type Point struct {
 DeviceID string `json:"device_id"`
 ID string `json:"point_id"`
 Code string `json:"point_code"`
 Name string `json:"name,omitempty"`
 DataType string `json:"data_type"`
 Unit string `json:"unit,omitempty"`
 SourceAddress string `json:"source_address,omitempty"`
 Scale float64 `json:"scale"`
 Offset float64 `json:"offset"`
 ExpectedInterval time.Duration `json:"expected_interval"`
 Enabled bool `json:"enabled"`
}

type Sample struct {
 TenantID string `json:"tenant_id,omitempty"`
 SiteID string `json:"site_id,omitempty"`
 DeviceID string `json:"device_id"`
 PointID string `json:"point_id"`
 PointCode string `json:"point_code"`
 Protocol Protocol `json:"protocol"`
 Model string `json:"model,omitempty"`
 ConfigVersion int64 `json:"config_version"`
 Sequence uint64 `json:"sequence,omitempty"`
 Value any `json:"value,omitempty"`
 RawValue any `json:"raw_value,omitempty"`
 Unit string `json:"unit,omitempty"`
 Quality Quality `json:"quality"`
 Reason QualityReason `json:"quality_reason,omitempty"`
 ObservedAt time.Time `json:"observed_at,omitempty"`
 CollectedAt time.Time `json:"collected_at"`
 ReceivedAt time.Time `json:"received_at,omitempty"`
}

func (s Sample) Validate() error {
 if strings.TrimSpace(s.DeviceID)=="" { return fmt.Errorf("device_id is required") }
 if strings.TrimSpace(s.PointCode)=="" && strings.TrimSpace(s.PointID)=="" { return fmt.Errorf("point_id or point_code is required") }
 if s.Protocol=="" { return fmt.Errorf("protocol is required") }
 if s.CollectedAt.IsZero() { return fmt.Errorf("collected_at is required") }
 switch s.Quality {
 case QualityGood:
  if s.Value==nil { return fmt.Errorf("GOOD sample must contain a value") }
  if !finiteValue(s.Value) { return fmt.Errorf("GOOD sample contains non-finite numeric value") }
  if s.Reason!=ReasonNone { return fmt.Errorf("GOOD sample must not contain a quality reason") }
 case QualityUncertain, QualityBad, QualityStale:
  if s.Reason==ReasonNone { return fmt.Errorf("%s sample requires a quality reason",s.Quality) }
 default: return fmt.Errorf("unsupported quality %q",s.Quality)
 }
 return nil
}

func finiteValue(v any) bool {
 switch n:=v.(type) {
 case float32: return !math.IsNaN(float64(n)) && !math.IsInf(float64(n),0)
 case float64: return !math.IsNaN(n) && !math.IsInf(n,0)
 default: return true
 }
}
