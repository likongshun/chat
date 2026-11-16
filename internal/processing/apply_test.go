package processing

import (
	"testing"

	"github.com/likongshun/chat/internal/config"
)

func TestApplyPredefinedOperators(t *testing.T) {
	rules := []config.Processing{
		{PointCode: "temperature", Operation: "SCALE", Param1: 0.1, Param2: -40},
		{PointCode: "temperature", Operation: "ABS"},
		{PointCode: "temperature", Operation: "ROUND", Param1: 1},
	}
	got, err := Apply("temperature", float64(-250), rules, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got != float64(15) {
		t.Fatalf("processed value=%v, want 15", got)
	}
}

func TestApplyEnumAndBit(t *testing.T) {
	enumValue, err := Apply("status", float64(2), []config.Processing{{PointCode: "status", Operation: "ENUM"}}, []config.EnumValue{{PointCode: "status", Value: "2", Label: "running"}})
	if err != nil {
		t.Fatal(err)
	}
	if enumValue != "running" {
		t.Fatalf("enum value=%v", enumValue)
	}

	bitValue, err := Apply("flags", float64(5), []config.Processing{{PointCode: "flags", Operation: "BIT", Param1: 2}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if bitValue != true {
		t.Fatalf("bit value=%v, want true", bitValue)
	}
}

func TestApplyRejectsUnknownEnumAndZeroDivide(t *testing.T) {
	if _, err := Apply("state", float64(99), []config.Processing{{PointCode: "state", Operation: "ENUM"}}, nil); err == nil {
		t.Fatal("expected unknown enum value to fail")
	}
	if _, err := Apply("ratio", float64(10), []config.Processing{{PointCode: "ratio", Operation: "DIVIDE", Param1: 0}}, nil); err == nil {
		t.Fatal("expected zero divisor to fail")
	}
}
