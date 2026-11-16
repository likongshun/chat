package config

import "testing"

func TestRejectEnabledWriteRule(t *testing.T) {
    s:=&Snapshot{WriteRules:[]WriteRule{{Model:"VFD_A",PointCode:"frequency_set",Enabled:true,Min:0,Max:50,RequireConfirm:true}}}
    if Validate(s)==nil { t.Fatal("expected enabled write rule to be rejected in phase 1") }
}
func TestRejectUnknownOperation(t *testing.T) {
    s:=&Snapshot{Processing:[]Processing{{PointCode:"x",Operation:"EXEC",Param1:0,Param2:0}}}
    if Validate(s)==nil { t.Fatal("expected unknown operation to be rejected") }
}
