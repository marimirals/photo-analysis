package logx

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
)

func TestRequestIDInEveryLine(t *testing.T) {
	var buf bytes.Buffer
	log := New(&buf, "api").With("photo_id", "p1")
	ctx := WithRequestID(context.Background(), "abc123")

	log.InfoContext(ctx, "with ctx")
	log.Info("without ctx")

	lines := bytes.Split(bytes.TrimSpace(buf.Bytes()), []byte("\n"))
	var first, second map[string]any
	_ = json.Unmarshal(lines[0], &first)
	_ = json.Unmarshal(lines[1], &second)
	if first["request_id"] != "abc123" || first["service"] != "api" || first["photo_id"] != "p1" {
		t.Fatalf("first line: %v", first)
	}
	if _, ok := second["request_id"]; ok {
		t.Fatalf("second line must not have request_id: %v", second)
	}
}
