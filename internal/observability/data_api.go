package observability

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/likongshun/chat/internal/collector"
	"github.com/likongshun/chat/internal/config"
	"github.com/likongshun/chat/internal/dlt645"
)

type InfluxQueryOptions struct {
	URL string
	Org string
	Bucket string
	Token string
}

type deviceView struct {
	ID string `json:"id"`
	Name string `json:"name"`
	Protocol string `json:"protocol"`
	Transport string `json:"transport"`
	SerialPort string `json:"serial_port,omitempty"`
	BaudRate int `json:"baud_rate,omitempty"`
	DataBits int `json:"data_bits,omitempty"`
	Parity string `json:"parity,omitempty"`
	StopBits int `json:"stop_bits,omitempty"`
	DiscoveryOnly bool `json:"discovery_only"`
	MeterAddress string `json:"meter_address,omitempty"`
	Host string `json:"host"`
	Port uint16 `json:"port"`
	UnitID byte `json:"unit_id"`
	Model string `json:"model"`
	IntervalMS int64 `json:"interval_ms"`
	Enabled bool `json:"enabled"`
	PointCount int `json:"point_count"`
	LastSeen *time.Time `json:"last_seen,omitempty"`
}

type pointView struct {
	ID string `json:"id"`
	Code string `json:"code"`
	Name string `json:"name"`
	DataID string `json:"data_id,omitempty"`
	Address uint16 `json:"address"`
	Function byte `json:"function"`
	DataType string `json:"data_type"`
	Unit string `json:"unit"`
	Enabled bool `json:"enabled"`
	Latest *collector.LatestSample `json:"latest,omitempty"`
}

type telemetryView struct {
	DeviceID string `json:"device_id"`
	PointCode string `json:"point_code"`
	Timestamp time.Time `json:"timestamp"`
	Value any `json:"value"`
	Quality string `json:"quality,omitempty"`
}

func (s *Server) registerDataAPI(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/devices", s.listDevices)
	mux.HandleFunc("/api/v1/devices/", s.devicePoints)
	mux.HandleFunc("/api/v1/telemetry", s.queryTelemetry)
	mux.HandleFunc("/api/v1/dlt645/discover", s.discoverDLT645Meter)
	mux.HandleFunc("/api/v1/dlt645/templates", s.listDLT645Templates)
}

func (s *Server) listDLT645Templates(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet { http.Error(w, "method not allowed", http.StatusMethodNotAllowed); return }
	query := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("q")))
	category := strings.TrimSpace(r.URL.Query().Get("category"))
	items := dlt645.ListDataIDTemplates()
	filtered := make([]dlt645.DataIDTemplate, 0, len(items))
	for _, item := range items {
		if category != "" && item.Category != category { continue }
		if query != "" && !strings.Contains(strings.ToLower(item.ID+" "+item.Name+" "+item.Category+" "+item.Unit), query) { continue }
		filtered = append(filtered, item)
	}
	writeJSON(w, http.StatusOK, filtered)
}

type discoverMeterRequest struct { DeviceID string `json:"device_id"` }
type discoverMeterResponse struct { DeviceID string `json:"device_id"`; Address string `json:"address"`; Protocol string `json:"protocol"` }

func (s *Server) discoverDLT645Meter(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost { http.Error(w, "method not allowed", http.StatusMethodNotAllowed); return }
	if s.DLT645 == nil { http.Error(w, "DL/T645 transport is unavailable", http.StatusServiceUnavailable); return }
	var request discoverMeterRequest
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil { http.Error(w, "invalid request body", http.StatusBadRequest); return }
	request.DeviceID = strings.TrimSpace(request.DeviceID)
	if request.DeviceID == "" { http.Error(w, "device_id is required", http.StatusBadRequest); return }
	cfg := s.currentConfig()
	if cfg == nil { http.Error(w, "configuration unavailable", http.StatusServiceUnavailable); return }
	var device *config.Device
	for i := range cfg.Devices { if cfg.Devices[i].ID == request.DeviceID { device = &cfg.Devices[i]; break } }
	if device == nil || device.Protocol != "dl-t645-2007" { http.Error(w, "unknown DL/T645 device", http.StatusNotFound); return }
	if !device.Enabled { http.Error(w, "device is disabled", http.StatusConflict); return }
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	address, err := s.DLT645.DiscoverMeter(ctx, *device)
	if err != nil { http.Error(w, "meter discovery failed: "+err.Error(), http.StatusBadGateway); return }
	writeJSON(w, http.StatusOK, discoverMeterResponse{DeviceID:device.ID, Address:address, Protocol:"dl-t645-2007"})
}

func (s *Server) currentConfig() *config.Config {
	if s.Config == nil { return nil }
	return s.Config.ReadOnly()
}

func (s *Server) configurationIndex() (*config.Config,map[string][]config.Point,map[string]int) {
	cfg:=s.currentConfig()
	if cfg==nil { return nil,nil,nil }
	s.configIndexMu.RLock()
	if s.indexedConfig==cfg {
		points,counts:=s.pointsByModel,s.pointCounts
		s.configIndexMu.RUnlock()
		return cfg,points,counts
	}
	s.configIndexMu.RUnlock()
	points:=make(map[string][]config.Point)
	counts:=make(map[string]int)
	for _,p:=range cfg.Points { points[p.Model]=append(points[p.Model],p);if p.Enabled {counts[p.Model]++} }
	if s.Config.ReadOnly()!=cfg { return cfg,points,counts }
	s.configIndexMu.Lock()
	if s.indexedConfig!=cfg { s.indexedConfig=cfg;s.pointsByModel=points;s.pointCounts=counts }
	points,counts=s.pointsByModel,s.pointCounts
	s.configIndexMu.Unlock()
	return cfg,points,counts
}

func (s *Server) listDevices(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet { http.Error(w,"method not allowed",http.StatusMethodNotAllowed);return }
	cfg,_,pointCounts:=s.configurationIndex()
	if cfg==nil { writeJSON(w,http.StatusOK,[]deviceView{});return }
	items:=make([]deviceView,0,len(cfg.Devices))
	for _,d:=range cfg.Devices {
		var seen *time.Time
		if last:=s.Latest.LastSuccess(d.ID);!last.IsZero(){seen=&last}
		items=append(items,deviceView{ID:d.ID,Name:d.Name,Protocol:d.Protocol,Transport:d.Transport,SerialPort:d.SerialPort,BaudRate:d.BaudRate,DataBits:d.DataBits,Parity:d.Parity,StopBits:d.StopBits,DiscoveryOnly:d.DiscoveryOnly,MeterAddress:d.MeterAddress,Host:d.Host,Port:d.Port,UnitID:d.UnitID,Model:d.Model,IntervalMS:d.Interval.Milliseconds(),Enabled:d.Enabled,PointCount:pointCounts[d.Model],LastSeen:seen})
	}
	writeJSON(w,http.StatusOK,items)
}

func (s *Server) devicePoints(w http.ResponseWriter,r *http.Request) {
	if r.Method!=http.MethodGet { http.Error(w,"method not allowed",http.StatusMethodNotAllowed);return }
	id:=strings.TrimPrefix(r.URL.EscapedPath(),"/api/v1/devices/")
	if !strings.HasSuffix(id,"/points") { http.NotFound(w,r);return }
	id=strings.TrimSuffix(id,"/points")
	id=strings.Trim(id,"/")
	decodedID,decodeErr:=url.PathUnescape(id)
	if decodeErr!=nil||decodedID=="" { http.NotFound(w,r);return }
	id=decodedID
	cfg,pointsByModel,_:=s.configurationIndex()
	if cfg==nil { writeJSON(w,http.StatusOK,[]pointView{});return }
	var device *config.Device
	for i:=range cfg.Devices { if cfg.Devices[i].ID==id { device=&cfg.Devices[i];break } }
	if device==nil { http.NotFound(w,r);return }
	latest:=s.Latest.Snapshot(id)
	items:=make([]pointView,0)
	for _,p:=range pointsByModel[device.Model] {
		if !p.Enabled {continue}
		item:=pointView{ID:p.ID,Code:p.PointCode,Name:p.Name,DataID:p.DataID,Address:p.Address,Function:p.Function,DataType:p.DataType,Unit:p.Unit,Enabled:p.Enabled}
		if v,ok:=latest[p.PointCode];ok { sample:=v;item.Latest=&sample }
		items=append(items,item)
	}
	writeJSON(w,http.StatusOK,items)
}

func (s *Server) queryTelemetry(w http.ResponseWriter,r *http.Request) {
	if r.Method!=http.MethodGet { http.Error(w,"method not allowed",http.StatusMethodNotAllowed);return }
	deviceID:=strings.TrimSpace(r.URL.Query().Get("device_id"))
	pointCode:=strings.TrimSpace(r.URL.Query().Get("point_id"))
	if deviceID==""||pointCode==""||len(deviceID)>128||len(pointCode)>128 { http.Error(w,"device_id and point_id are required",http.StatusBadRequest);return }
	cfg,pointsByModel,_:=s.configurationIndex()
	if cfg==nil { http.Error(w,"configuration unavailable",http.StatusServiceUnavailable);return }
	var model string
	for _,d:=range cfg.Devices { if d.ID==deviceID { model=d.Model;break } }
	if model=="" { http.Error(w,"unknown device or point",http.StatusNotFound);return }
	valid:=false
	for _,p:=range pointsByModel[model] { if p.PointCode==pointCode&&p.Enabled {valid=true;break} }
	if !valid { http.Error(w,"unknown device or point",http.StatusNotFound);return }
	if s.Influx==nil||s.Influx.Token=="" { http.Error(w,"historical data source is not configured",http.StatusServiceUnavailable);return }
	now:=time.Now().UTC()
	to:=now
	if raw:=r.URL.Query().Get("to");raw!="" { parsed,err:=time.Parse(time.RFC3339,raw);if err!=nil{http.Error(w,"invalid to timestamp",http.StatusBadRequest);return};to=parsed.UTC() }
	from:=to.Add(-time.Hour)
	if raw:=r.URL.Query().Get("from");raw!="" { parsed,err:=time.Parse(time.RFC3339,raw);if err!=nil{http.Error(w,"invalid from timestamp",http.StatusBadRequest);return};from=parsed.UTC() }
	if !from.Before(to)||to.After(now.Add(5*time.Minute))||to.Sub(from)>7*24*time.Hour {http.Error(w,"time range must be positive and no longer than 7 days",http.StatusBadRequest);return}
	limit:=500
	if raw:=r.URL.Query().Get("limit");raw!="" { n,err:=strconv.Atoi(raw);if err!=nil||n<1||n>1000 {http.Error(w,"limit must be between 1 and 1000",http.StatusBadRequest);return};limit=n}
	offset:=0
	if raw:=r.URL.Query().Get("offset");raw!="" { n,err:=strconv.Atoi(raw);if err!=nil||n<0||n>10000 {http.Error(w,"offset must be between 0 and 10000",http.StatusBadRequest);return};offset=n}
	rows,err:=s.queryInflux(r.Context(),deviceID,pointCode,from,to,limit,offset)
	if err!=nil { http.Error(w,"historical query failed",http.StatusBadGateway);return }
	writeJSON(w,http.StatusOK,rows)
}

func (s *Server) queryInflux(ctx context.Context,deviceID,pointCode string,from,to time.Time,limit,offset int)([]telemetryView,error) {
	if s.Influx==nil{return nil,fmt.Errorf("InfluxDB not configured")}
	base,err:=url.Parse(strings.TrimRight(s.Influx.URL,"/")+"/api/v2/query")
	if err!=nil{return nil,err}
	q:=base.Query();q.Set("org",s.Influx.Org);base.RawQuery=q.Encode()
	windowSeconds:=int64(math.Ceil(to.Sub(from).Seconds()/float64(limit)))
	if windowSeconds<1 {windowSeconds=1}
	flux:=fmt.Sprintf("from(bucket: %s) |> range(start: time(v: %s), stop: time(v: %s)) |> filter(fn: (r) => r._measurement == \"modbus_point\" and r._field == \"value\" and r.device_id == %s and r.point_code == %s) |> aggregateWindow(every: %ds, fn: last, createEmpty: false) |> sort(columns: [\"_time\"], desc: true) |> limit(n: %d, offset: %d) |> sort(columns: [\"_time\"])",strconv.Quote(s.Influx.Bucket),strconv.Quote(from.Format(time.RFC3339Nano)),strconv.Quote(to.Format(time.RFC3339Nano)),strconv.Quote(deviceID),strconv.Quote(pointCode),windowSeconds,limit,offset)
	req,err:=http.NewRequestWithContext(ctx,http.MethodPost,base.String(),strings.NewReader(flux));if err!=nil{return nil,err}
	req.Header.Set("Authorization","Token "+s.Influx.Token);req.Header.Set("Content-Type","application/vnd.flux");req.Header.Set("Accept","application/csv")
	client:=s.InfluxClient;if client==nil {client=&http.Client{Timeout:10*time.Second}}
	resp,err:=client.Do(req);if err!=nil{return nil,err};defer resp.Body.Close()
	if resp.StatusCode<200||resp.StatusCode>=300 {_,_=io.Copy(io.Discard,io.LimitReader(resp.Body,4096));return nil,fmt.Errorf("InfluxDB returned %s",resp.Status)}
	reader:=csv.NewReader(io.LimitReader(resp.Body,4<<20));reader.FieldsPerRecord=-1
	var headers []string
	var dataTypes []string
	out:=make([]telemetryView,0)
	for { record,e:=reader.Read();if e==io.EOF{break};if e!=nil{return nil,e};if len(record)==0{continue}
		if record[0]=="#datatype" { dataTypes=record;continue }
		if strings.HasPrefix(record[0],"#"){continue}
		if headers==nil {headers=record;continue}
		row:=map[string]string{};for i,v:=range record{if i<len(headers){row[headers[i]]=v}}
		ts,parseErr:=time.Parse(time.RFC3339Nano,row["_time"]);if parseErr!=nil{continue}
		valueType:=""
		for i,name:=range headers { if name=="_value"&&i<len(dataTypes) {valueType=dataTypes[i];break} }
		value:=parseInfluxValue(row["_value"],valueType)
		out=append(out,telemetryView{DeviceID:deviceID,PointCode:pointCode,Timestamp:ts,Value:value,Quality:row["quality"]})
		if len(out)>=limit{break}
	}
	return out,nil
}

func parseInfluxValue(raw, dataType string) any {
	switch dataType {
	case "long":
		if v,e:=strconv.ParseInt(raw,10,64);e==nil{return v}
	case "unsignedLong":
		if v,e:=strconv.ParseUint(raw,10,64);e==nil{return v}
	case "double":
		if v,e:=strconv.ParseFloat(raw,64);e==nil&&!math.IsNaN(v)&&!math.IsInf(v,0){return v}
	case "boolean":
		if v,e:=strconv.ParseBool(raw);e==nil{return v}
	case "string":
		return raw
	}
	if v,e:=strconv.ParseFloat(raw,64);e==nil&&!math.IsNaN(v)&&!math.IsInf(v,0){return v}
	if v,e:=strconv.ParseBool(raw);e==nil{return v}
	return raw
}

func writeJSON(w http.ResponseWriter,status int,value any) {
	w.Header().Set("Content-Type","application/json; charset=utf-8")
	w.Header().Set("Cache-Control","no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
