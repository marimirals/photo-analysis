package api

import "testing"

func TestRoutesRegister(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("route conflict: %v", r)
		}
	}()
	NewServer(nil, nil, nil, nil, "t", 1).Routes()
}
