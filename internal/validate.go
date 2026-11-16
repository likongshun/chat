package config

import (
    "fmt"
    "math"
    "strings"
)

var widths = map[string]int{"INT16":1,"UINT16":1,"INT32":2,"UINT32":2,"FLOAT32":2,"INT64":4,"UINT64":4,"FLOAT64":4,"BOOL":1}
var operations = map[string]bool{"SCALE":true,"ABS":true,"CLAMP":true,"BIT":true,"ENUM":true,"ROUND":true,"DIVIDE":true,"LINEAR":true}

func Validate(s *Snapshot) error {
    devices:=map[string]bool{}
    for _,d:=range s.Devices {
        if d.DeviceID==""||d.Host==""||d.Model=="" {return fmt.Errorf("device %q: device_id, host and model are required",d.DeviceID)}
        if devices[d.DeviceID] {return fmt.Errorf("duplicate device_id %s",d.DeviceID)}; devices[d.DeviceID]=true
        if d.Port<1||d.Port>65535||d.PollMS<100 {return fmt.Errorf("device %s: invalid port or poll_ms (minimum 100ms)",d.DeviceID)}
        if d.UnitID>255 {return fmt.Errorf("device %s: unit_id out of range",d.DeviceID)}
    }
    pointKeys:=map[string]bool{}
    for _,p:=range s.Points {
        key:=p.Model+"/"+p.PointCode
        if p.Model==""||p.PointCode=="" {return fmt.Errorf("point model and point_code are required")}
        if pointKeys[key] {return fmt.Errorf("duplicate point %s",key)}; pointKeys[key]=true
        if p.Function<1||p.Function>4 {return fmt.Errorf("%s: function must be 1,2,3 or 4",key)}
        if p.RW!="R"&&p.RW!="RW" {return fmt.Errorf("%s: rw must be R or RW",key)}
        if p.RW=="RW" { return fmt.Errorf("%s: writable point declaration is not enough; write execution is not enabled in this phase",key) }
        if p.Address<0 {return fmt.Errorf("%s: invalid address",key)}
        if p.DataType=="STRING" {if p.Length<1||p.Length>120{return fmt.Errorf("%s: STRING length must be 1..120 registers",key)}} else if _,ok:=widths[p.DataType];!ok{return fmt.Errorf("%s: unsupported data type %s",key,p.DataType)}
        if p.ByteOrder!=""&&!contains([]string{"ABCD","BADC","CDAB","DCBA"},p.ByteOrder){return fmt.Errorf("%s: invalid byte_order",key)}
        if p.WordOrder!=""&&!contains([]string{"HIGH_LOW","LOW_HIGH"},p.WordOrder){return fmt.Errorf("%s: invalid word_order",key)}
        if math.IsNaN(p.Scale)||math.IsInf(p.Scale,0)||math.IsNaN(p.Offset)||math.IsInf(p.Offset,0){return fmt.Errorf("%s: scale/offset must be finite",key)}
    }
    for _,d:=range s.Devices { if d.Enabled {found:=false;for _,p:=range s.Points{if p.Model==d.Model&&p.Enabled{found=true;break}};if !found{return fmt.Errorf("device %s references model %s with no enabled points",d.DeviceID,d.Model)}} }
    for _,p:=range s.Processing {if !operations[strings.ToUpper(p.Operation)]{return fmt.Errorf("point %s: unsupported operation %s",p.PointCode,p.Operation)}}
    for _,w:=range s.WriteRules {if w.Enabled{return fmt.Errorf("write rule %s/%s is enabled; write path is deliberately disabled in phase 1",w.Model,w.PointCode)};if w.Min>w.Max{return fmt.Errorf("write rule %s/%s: min > max",w.Model,w.PointCode)}}
    return nil
}
func contains(xs []string,v string)bool{for _,x:=range xs{if x==v{return true}};return false}
