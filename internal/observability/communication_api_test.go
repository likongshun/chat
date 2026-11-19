package observability

import (
 "net/http"
 "net/http/httptest"
 "strings"
 "testing"
 "time"

 "github.com/likongshun/chat/internal/telemetry"
)

func TestDeviceCommunicationDiagnosticsAPI(t *testing.T) {
 server,_:=testDataServer(t)
 server.Communication=telemetry.NewCommunicationStats()
 server.Communication.Record("meter-1",time.Now(),35*time.Millisecond,"",false,false)
 server.Communication.Record("meter-1",time.Now(),900*time.Millisecond,"TIMEOUT",true,false)
 response:=httptest.NewRecorder()
 server.Handler().ServeHTTP(response,httptest.NewRequest(http.MethodGet,"/api/v1/devices/meter-1/communication",nil))
 body:=response.Body.String()
 if response.Code!=http.StatusOK || !strings.Contains(body,"\"attempts\":2") || !strings.Contains(body,"\"successes\":1") || !strings.Contains(body,"\"timeouts\":1") {
  t.Fatalf("communication response %d: %s",response.Code,body)
 }
 missing:=httptest.NewRecorder()
 server.Handler().ServeHTTP(missing,httptest.NewRequest(http.MethodGet,"/api/v1/devices/missing/communication",nil))
 if missing.Code!=http.StatusNotFound { t.Fatalf("missing device status=%d",missing.Code) }
}
