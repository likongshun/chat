package adapter

import (
 "testing"
 "time"

 "github.com/likongshun/chat/internal/collector"
 "github.com/likongshun/chat/internal/config"
 "github.com/likongshun/chat/internal/telemetry"
)

func TestSampleFromCollectorCompatibility(t *testing.T) {
 legacy:=collector.Sample{DeviceID:"meter-1",Protocol:"dl-t645-2007",Model:"meter-x",PointCode:"voltage",Value:220.0,Timestamp:time.Now(),Quality:collector.QualityGood,ConfigVersion:7}
 got:=SampleFromCollector(legacy)
 if got.Quality!=telemetry.QualityGood || got.Protocol!=telemetry.ProtocolDLT645 || got.ConfigVersion!=7 { t.Fatalf("unexpected adapted sample: %+v",got) }
 if err:=got.Validate(); err!=nil { t.Fatal(err) }
}

func TestDeviceFromConfigDefaultsProtocol(t *testing.T) {
 got:=DeviceFromConfig(config.Device{ID:"d1",Model:"m1",Enabled:true},3)
 if got.Protocol!=telemetry.ProtocolModbusTCP || got.ConfigVersion!=3 { t.Fatalf("unexpected device: %+v",got) }
}
