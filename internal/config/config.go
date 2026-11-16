package config

import (
 "errors"
 "fmt"
 "time"
)
type Device struct { ID string; Name string; Host string; Port uint16; UnitID byte; Interval time.Duration; Enabled bool }
type Point struct { ID string; DeviceID string; Name string; Address uint16; Count uint16; Function byte; DataType string; Scale float64; Offset float64; Enabled bool; Writable bool }
type Config struct { Devices []Device; Points []Point }
func (c Config) Validate()error{
 ids:=map[string]bool{}
 for _,d:=range c.Devices{if d.ID==""||d.Host==""||d.Port==0{return fmt.Errorf("invalid device %q",d.ID)};if ids[d.ID]{return fmt.Errorf("duplicate device %s",d.ID)};ids[d.ID]=true;if d.Interval<=0{return fmt.Errorf("device %s: interval must be positive",d.ID)}}
 pids:=map[string]bool{}
 for _,p:=range c.Points{if p.ID==""||p.DeviceID==""||p.Count==0{return fmt.Errorf("invalid point %q",p.ID)};if pids[p.ID]{return fmt.Errorf("duplicate point %s",p.ID)};pids[p.ID]=true;if !ids[p.DeviceID]{return fmt.Errorf("point %s references unknown device",p.ID)};if p.Writable&&p.Function!=6&&p.Function!=16{return errors.New("writable point must use function 6 or 16")}}
 return nil
}
