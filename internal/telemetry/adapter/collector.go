// Package adapter contains migration adapters from the legacy collector model
// to the canonical DCIM telemetry contract.
package adapter

import (
 "strings"
 "time"

 "github.com/likongshun/chat/internal/collector"
 "github.com/likongshun/chat/internal/config"
 "github.com/likongshun/chat/internal/telemetry"
)

func DeviceFromConfig(d config.Device, configVersion int64) telemetry.Device {
 protocol,err:=telemetry.NormalizeProtocol(d.Protocol)
 if err!=nil && strings.TrimSpace(d.Protocol)=="" { protocol=telemetry.ProtocolModbusTCP }
 return telemetry.Device{
  ID:d.ID,Name:d.Name,Protocol:protocol,Model:d.Model,
  ConfigVersion:configVersion,Enabled:d.Enabled,DiscoveryOnly:d.DiscoveryOnly,
 }
}

func PointFromConfig(p config.Point, expectedInterval int64) telemetry.Point {
 return telemetry.Point{
  DeviceID:p.DeviceID,ID:p.ID,Code:p.PointCode,Name:p.Name,
  DataType:p.DataType,Unit:p.Unit,SourceAddress:p.DataID,
  Scale:p.Scale,Offset:p.Offset,
  ExpectedInterval:durationFromNanos(expectedInterval),Enabled:p.Enabled,
 }
}

func SampleFromCollector(s collector.Sample) telemetry.Sample {
 protocol,err:=telemetry.NormalizeProtocol(s.Protocol)
 if err!=nil && strings.TrimSpace(s.Protocol)=="" { protocol=telemetry.ProtocolModbusTCP }
 out:=telemetry.Sample{
  DeviceID:s.DeviceID,PointCode:s.PointCode,Protocol:protocol,
  Model:s.Model,ConfigVersion:s.ConfigVersion,Value:s.Value,
  Unit:s.Unit,CollectedAt:s.Timestamp,
 }
 switch strings.ToLower(string(s.Quality)) {
 case "good":
  out.Quality=telemetry.QualityGood
 case "bad":
  out.Quality=telemetry.QualityBad
  out.Reason=telemetry.ReasonDecodeError
 case "communication_error":
  out.Quality=telemetry.QualityBad
  out.Reason=reasonFromError(s.Error)
 default:
  out.Quality=telemetry.QualityBad
  out.Reason=telemetry.ReasonUnknown
 }
 if out.Quality==telemetry.QualityGood && out.Value==nil {
  out.Quality=telemetry.QualityBad
  out.Reason=telemetry.ReasonDecodeError
 }
 if out.Quality!=telemetry.QualityGood && out.Reason==telemetry.ReasonNone {
  out.Reason=telemetry.ReasonUnknown
 }
 return out
}

func reasonFromError(message string) telemetry.QualityReason {
 m:=strings.ToLower(message)
 switch {
 case strings.Contains(m,"timeout"),strings.Contains(m,"deadline"):
  return telemetry.ReasonTimeout
 case strings.Contains(m,"crc"),strings.Contains(m,"checksum"):
  return telemetry.ReasonCRCError
 case strings.Contains(m,"decode"),strings.Contains(m,"bcd"):
  return telemetry.ReasonDecodeError
 case strings.Contains(m,"range"):
  return telemetry.ReasonOutOfRange
 case strings.Contains(m,"meter returned"):
  return telemetry.ReasonDeviceError
 default:
  return telemetry.ReasonProtocolError
 }
}

func durationFromNanos(n int64) time.Duration { return time.Duration(n) }
