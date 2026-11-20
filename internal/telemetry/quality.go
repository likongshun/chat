package telemetry

import (
 "fmt"
 "time"
)

// EvaluateStaleness changes only quality metadata and preserves the last known value.
func EvaluateStaleness(sample Sample, now time.Time, expected time.Duration, multiplier float64) (Sample, bool) {
 if sample.Quality!=QualityGood && sample.Quality!=QualityUncertain { return sample,false }
 if sample.CollectedAt.IsZero() || expected<=0 { return sample,false }
 if multiplier<=0 { multiplier=2 }
 deadline:=time.Duration(float64(expected)*multiplier)
 if deadline<expected { deadline=expected }
 if now.Before(sample.CollectedAt.Add(deadline)) { return sample,false }
 sample.Quality=QualityStale
 sample.Reason=ReasonStaleTimeout
 return sample,true
}

// MarkFailure updates quality without replacing the previous valid value.
func MarkFailure(sample Sample, reason QualityReason, now time.Time) Sample {
 if reason==ReasonNone { reason=ReasonUnknown }
 sample.Quality=QualityBad
 sample.Reason=reason
 if sample.CollectedAt.IsZero() { sample.CollectedAt=now.UTC() }
 return sample
}

func ValidateFreshness(expected time.Duration, multiplier float64) error {
 if expected<=0 { return fmt.Errorf("expected interval must be positive") }
 if multiplier<=0 { return fmt.Errorf("stale multiplier must be positive") }
 return nil
}
