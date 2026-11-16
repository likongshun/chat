package pipeline

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type InfluxOptions struct {
	URL     string
	Org     string
	Bucket  string
	Token   string
	Timeout time.Duration
}

type InfluxSink struct {
	endpoint string
	org      string
	bucket   string
	token    string
	client   *http.Client
}

type samplePayload struct {
	DeviceID  string    `json:"device_id"`
	Model     string    `json:"model"`
	PointCode string    `json:"point_code"`
	Unit      string    `json:"unit"`
	Timestamp time.Time `json:"timestamp"`
	Value     any       `json:"value"`
	Quality   string    `json:"quality"`
	Error     string    `json:"error"`
}

func NewInfluxSink(options InfluxOptions) (*InfluxSink, error) {
	if options.URL == "" || options.Org == "" || options.Bucket == "" || options.Token == "" {
		return nil, fmt.Errorf("InfluxDB URL, org, bucket and token are required")
	}
	parsed, err := url.Parse(strings.TrimRight(options.URL, "/") + "/api/v2/write")
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return nil, fmt.Errorf("invalid InfluxDB URL %q", options.URL)
	}
	query := parsed.Query()
	query.Set("org", options.Org)
	query.Set("bucket", options.Bucket)
	query.Set("precision", "ns")
	parsed.RawQuery = query.Encode()
	if options.Timeout <= 0 {
		options.Timeout = 10 * time.Second
	}
	return &InfluxSink{
		endpoint: parsed.String(),
		org: options.Org,
		bucket: options.Bucket,
		token: options.Token,
		client: &http.Client{Timeout: options.Timeout},
	}, nil
}

func (s *InfluxSink) Name() string { return "influx" }

func (s *InfluxSink) Publish(ctx context.Context, events []Event) error {
	if s == nil || s.client == nil {
		return fmt.Errorf("InfluxDB sink is not initialized")
	}
	if len(events) == 0 {
		return nil
	}
	var body bytes.Buffer
	for _, event := range events {
		line, err := influxLine(event)
		if err != nil {
			return err
		}
		body.WriteString(line)
		body.WriteByte('\n')
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.endpoint, bytes.NewReader(body.Bytes()))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Token "+s.token)
	req.Header.Set("Content-Type", "text/plain; charset=utf-8")
	req.Header.Set("Accept", "application/json")
	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("write InfluxDB batch: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		message, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("InfluxDB write returned %s: %s", resp.Status, strings.TrimSpace(string(message)))
	}
	return nil
}

func influxLine(event Event) (string, error) {
	var sample samplePayload
	if err := json.Unmarshal(event.Payload, &sample); err != nil {
		return "", fmt.Errorf("decode InfluxDB event %d: %w", event.Seq, err)
	}
	if sample.DeviceID == "" || sample.PointCode == "" || sample.Timestamp.IsZero() {
		return "", fmt.Errorf("event %d is missing device_id, point_code or timestamp", event.Seq)
	}
	tags := []string{"device_id=" + escapeTag(sample.DeviceID), "point_code=" + escapeTag(sample.PointCode)}
	if sample.Model != "" {
		tags = append(tags, "model="+escapeTag(sample.Model))
	}
	if sample.Quality != "" {
		tags = append(tags, "quality="+escapeTag(sample.Quality))
	}
	if sample.Unit != "" {
		tags = append(tags, "unit="+escapeTag(sample.Unit))
	}
	fields := make([]string, 0, 3)
	if sample.Value != nil {
		value, err := influxFieldValue(sample.Value)
		if err != nil {
			return "", fmt.Errorf("event %d value: %w", event.Seq, err)
		}
		fields = append(fields, "value="+value)
	}
	fields = append(fields, "wal_seq="+strconv.FormatUint(event.Seq, 10)+"i")
	if sample.Error != "" {
		fields = append(fields, "error="+strconv.Quote(sample.Error))
	}
	if sample.Value == nil && sample.Error == "" {
		return "", fmt.Errorf("event %d has neither value nor error", event.Seq)
	}
	return "modbus_point," + strings.Join(tags, ",") + " " + strings.Join(fields, ",") + " " + strconv.FormatInt(sample.Timestamp.UTC().UnixNano(), 10), nil
}

func influxFieldValue(value any) (string, error) {
	switch v := value.(type) {
	case string:
		return strconv.Quote(v), nil
	case bool:
		return strconv.FormatBool(v), nil
	case float64:
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return "", fmt.Errorf("non-finite numeric value")
		}
		return strconv.FormatFloat(v, 'g', -1, 64), nil
	case float32:
		if math.IsNaN(float64(v)) || math.IsInf(float64(v), 0) {
			return "", fmt.Errorf("non-finite numeric value")
		}
		return strconv.FormatFloat(float64(v), 'g', -1, 32), nil
	case int:
		return strconv.Itoa(v) + "i", nil
	case int64:
		return strconv.FormatInt(v, 10) + "i", nil
	case uint64:
		return strconv.FormatUint(v, 10) + "u", nil
	default:
		return "", fmt.Errorf("unsupported field type %T", value)
	}
}

func escapeTag(value string) string {
	replacer := strings.NewReplacer("\\", "\\\\", ",", "\\,", " ", "\\ ", "=", "\\=")
	return replacer.Replace(value)
}
