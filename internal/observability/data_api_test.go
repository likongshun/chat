package observability

import (
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/likongshun/chat/internal/collector"
	"github.com/likongshun/chat/internal/config"
	"github.com/likongshun/chat/internal/dlt645"
)

func testDataServer(t *testing.T) (*Server,*collector.LatestStore) {
	t.Helper()
	cfg:=config.Config{
		Devices:[]config.Device{{ID:"meter-1",Name:"主进线电表",Host:"127.0.0.1",Port:502,UnitID:1,Model:"meter",Interval:time.Second,Enabled:true}},
		Points:[]config.Point{{ID:"meter/power",Model:"meter",PointCode:"power",Name:"有功功率",Address:10,Count:1,Function:3,DataType:"UINT16",Scale:1,Enabled:true}},
	}
	manager,err:=config.NewManager(cfg);if err!=nil{t.Fatal(err)}
	latest:=collector.NewLatestStore(100)
	latest.Update([]collector.Sample{{DeviceID:"meter-1",Model:"meter",PointCode:"power",Value:float64(123.5),Quality:collector.QualityGood,Timestamp:time.Now().UTC()}})
	return &Server{Config:manager,Latest:latest},latest
}

func TestDeviceAndLatestPointAPIs(t *testing.T) {
	server,_:=testDataServer(t)
	h:=server.Handler()
	devices:=httptest.NewRecorder();h.ServeHTTP(devices,httptest.NewRequest(http.MethodGet,"/api/v1/devices",nil))
	if devices.Code!=http.StatusOK||!strings.Contains(devices.Body.String(),"\"id\":\"meter-1\"")||!strings.Contains(devices.Body.String(),"\"point_count\":1"){t.Fatalf("devices response %d: %s",devices.Code,devices.Body.String())}
	points:=httptest.NewRecorder();h.ServeHTTP(points,httptest.NewRequest(http.MethodGet,"/api/v1/devices/meter-1/points",nil))
	if points.Code!=http.StatusOK||!strings.Contains(points.Body.String(),"\"value\":123.5"){t.Fatalf("points response %d: %s",points.Code,points.Body.String())}
	missing:=httptest.NewRecorder();h.ServeHTTP(missing,httptest.NewRequest(http.MethodGet,"/api/v1/devices/missing/points",nil))
	if missing.Code!=http.StatusNotFound{t.Fatalf("missing device status=%d",missing.Code)}
}

func TestInfluxHistoryAPIAndQueryValidation(t *testing.T) {
	var query string
	influx:=httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter,r *http.Request){
		if r.Header.Get("Authorization")!="Token secret"{t.Errorf("authorization header missing")}
		body,_:=io.ReadAll(r.Body);query=string(body)
		w.Header().Set("Content-Type","application/csv")
		_,_=io.WriteString(w,"#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string,string,string\n,result,table,_start,_stop,_time,_value,device_id,point_code,quality\n,,0,2026-01-01T00:00:00Z,2026-01-01T01:00:00Z,2026-01-01T00:30:00Z,42.5,meter-1,power,good\n")
	}))
	defer influx.Close()
	server,_:=testDataServer(t)
	server.Influx=&InfluxQueryOptions{URL:influx.URL,Org:"modbus",Bucket:"telemetry",Token:"secret"}
	server.InfluxClient=influx.Client()
	from:=time.Now().UTC().Add(-time.Hour).Format(time.RFC3339)
	to:=time.Now().UTC().Format(time.RFC3339)
	path:="/api/v1/telemetry?"+url.Values{"device_id":{"meter-1"},"point_id":{"power"},"from":{from},"to":{to}}.Encode()
	response:=httptest.NewRecorder();server.Handler().ServeHTTP(response,httptest.NewRequest(http.MethodGet,path,nil))
	if response.Code!=http.StatusOK||!strings.Contains(response.Body.String(),"\"value\":42.5")||!strings.Contains(response.Body.String(),"\"quality\":\"good\""){t.Fatalf("history response %d: %s",response.Code,response.Body.String())}
	for _,part:=range []string{`r.device_id == "meter-1"`,`r.point_code == "power"`,`limit(n: 500)`,`aggregateWindow(every:`}{if !strings.Contains(query,part){t.Fatalf("Flux query missing %q: %s",part,query)}}
	old:=time.Now().UTC().Add(-8*24*time.Hour).Format(time.RFC3339)
	badPath:="/api/v1/telemetry?"+url.Values{"device_id":{"meter-1"},"point_id":{"power"},"from":{old},"to":{to}}.Encode()
	bad:=httptest.NewRecorder();server.Handler().ServeHTTP(bad,httptest.NewRequest(http.MethodGet,badPath,nil))
	if bad.Code!=http.StatusBadRequest{t.Fatalf("wide range status=%d body=%s",bad.Code,bad.Body.String())}
}

func TestParseInfluxValuePreservesIntegerPrecision(t *testing.T) {
	if got:=parseInfluxValue("9007199254740993","long");got!=int64(9007199254740993){t.Fatalf("int64 precision lost: %T %v",got,got)}
	if got:=parseInfluxValue("18446744073709551615","unsignedLong");got!=uint64(^uint64(0)){t.Fatalf("uint64 precision lost: %T %v",got,got)}
}

type discoveryTestTransport struct { address string }
func (t discoveryTestTransport) Open(ctx context.Context, endpoint string, timeout time.Duration) (dlt645.Session, error) {
	client, peer := net.Pipe()
	go func() {
		defer peer.Close()
		request := make([]byte, 12)
		if _, err := io.ReadFull(peer, request); err != nil { return }
		if request[8] != dlt645.ReadAddress { return }
		addr := make([]byte, 6)
		for i:=0;i<6;i++ { addr[i]=(t.address[10-2*i]-'0')<<4 | (t.address[11-2*i]-'0') }
		response := []byte{0x68}
		response = append(response, addr...)
		response = append(response, 0x68, 0x93, 0x06)
		response = append(response, addr...)
		var sum byte
		for _, b := range response { sum += b }
		response = append(response, sum, 0x16)
		_, _ = peer.Write(response)
	}()
	return client, nil
}

func TestDLT645MeterDiscoveryAPI(t *testing.T) {
	cfg := config.Config{
		Devices: []config.Device{{ID:"find-meter",Host:"127.0.0.1",Port:4001,Protocol:"dl-t645-2007",DiscoveryOnly:true,Model:"DLT",Interval:time.Second,Enabled:true}},
		Points: []config.Point{{ID:"DLT/energy",Model:"DLT",PointCode:"energy",DataID:"00010000",Count:4,DataType:"BCD",Scale:0.01,Enabled:true}},
	}
	manager, err := config.NewManager(cfg); if err != nil { t.Fatal(err) }
	poller := &collector.DL645Poller{Timeout:time.Second,Transport:discoveryTestTransport{address:"123456789012"}}
	server := &Server{Config:manager,DLT645:poller}
	body := strings.NewReader(`{"device_id":"find-meter"}`)
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response,httptest.NewRequest(http.MethodPost,"/api/v1/dlt645/discover",body))
	if response.Code!=http.StatusOK || !strings.Contains(response.Body.String(),`"address":"123456789012"`) { t.Fatalf("discovery response %d: %s",response.Code,response.Body.String()) }
}

func TestDLT645MeterDiscoveryRejectsUnknownDevice(t *testing.T) {
	server,_ := testDataServer(t)
	server.DLT645 = &collector.DL645Poller{}
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response,httptest.NewRequest(http.MethodPost,"/api/v1/dlt645/discover",strings.NewReader(`{"device_id":"missing"}`)))
	if response.Code!=http.StatusNotFound { t.Fatalf("status=%d body=%s",response.Code,response.Body.String()) }
}
