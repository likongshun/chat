package observability

import (
 "io"
 "net/http"
 "net/http/httptest"
 "net/url"
 "strings"
 "testing"
 "time"
)

func TestTelemetryAggregateAPI(t *testing.T) {
 var flux string
 influx:=httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter,r *http.Request) {
  body,_:=io.ReadAll(r.Body);flux=string(body)
  w.Header().Set("Content-Type","application/csv")
  _,_=io.WriteString(w,"#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double\n,result,table,_start,_stop,_time,_value\n,,0,2026-01-01T00:00:00Z,2026-01-01T01:00:00Z,2026-01-01T00:01:00Z,220.5\n")
 }))
 defer influx.Close()
 server,_:=testDataServer(t)
 server.Influx=&InfluxQueryOptions{URL:influx.URL,Org:"modbus",Bucket:"telemetry",Token:"secret"}
 server.InfluxClient=influx.Client()
 from:=time.Now().UTC().Add(-time.Hour).Format(time.RFC3339)
 to:=time.Now().UTC().Format(time.RFC3339)
 query:="/api/v1/telemetry/aggregate?"+url.Values{
  "device_id":{"meter-1"},"point_id":{"power"},"from":{from},"to":{to},"step":{"60s"},"fn":{"mean"},
 }.Encode()
 response:=httptest.NewRecorder()
 server.Handler().ServeHTTP(response,httptest.NewRequest(http.MethodGet,query,nil))
 body:=response.Body.String()
 if response.Code!=http.StatusOK||!strings.Contains(body,"\"function\":\"mean\"")||!strings.Contains(body,"\"value\":220.5") {
  t.Fatalf("aggregate response %d: %s",response.Code,body)
 }
 for _,part:=range []string{"aggregateWindow(every: 60s, fn: mean","r.quality == \"good\"","r.device_id == \"meter-1\""} {
  if !strings.Contains(flux,part) { t.Fatalf("Flux query missing %q: %s",part,flux) }
 }
 bad:=httptest.NewRecorder()
 badPath:="/api/v1/telemetry/aggregate?"+url.Values{
  "device_id":{"meter-1"},"point_id":{"power"},"from":{from},"to":{to},"step":{"60s"},"fn":{"drop"},
 }.Encode()
 server.Handler().ServeHTTP(bad,httptest.NewRequest(http.MethodGet,badPath,nil))
 if bad.Code!=http.StatusBadRequest { t.Fatalf("invalid aggregate function status=%d",bad.Code) }
}
