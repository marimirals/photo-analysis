package worker

import (
	"context"
	"errors"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type fakeRun struct {
	attempts []int
	pauses   []time.Duration
}

func (f *fakeRun) hooks() retryHooks {
	return retryHooks{
		beforeAttempt: func(a int) { f.attempts = append(f.attempts, a) },
		onWait:        func(err error, a int, d time.Duration, u bool) { f.pauses = append(f.pauses, d) },
		sleep:         func(ctx context.Context, d time.Duration) bool { return ctx.Err() == nil },
	}
}

// script возвращает call, который отдаёт ошибки по очереди, а потом успех.
func script(errs ...error) func(context.Context) (string, error) {
	i := 0
	return func(context.Context) (string, error) {
		if i < len(errs) {
			i++
			return "", errs[i-1]
		}
		return "ok", nil
	}
}

var policy = retryPolicy{maxAttempts: 3, base: 2 * time.Second, maxWait: 30 * time.Second}

func TestUnavailableWaitsWithoutSpendingAttempts(t *testing.T) {
	down := status.Error(codes.Unavailable, "connection refused")
	f := &fakeRun{}
	// analyzer лежит 7 раз подряд — больше, чем maxAttempts
	res, attempts, err := runWithRetry(context.Background(), policy, f.hooks(),
		script(down, down, down, down, down, down, down))
	if err != nil || res != "ok" {
		t.Fatalf("want ok, got %q, %v", res, err)
	}
	if attempts != 1 || len(f.attempts) != 1 {
		t.Fatalf("attempts should stay 1, got %d (%v)", attempts, f.attempts)
	}
	want := []time.Duration{2, 4, 8, 16, 30, 30, 30}
	for i, d := range want {
		if f.pauses[i] != d*time.Second {
			t.Fatalf("pauses = %v, want %v s", f.pauses, want)
		}
	}
}

func TestInternalErrorUsesAttempts(t *testing.T) {
	boom := status.Error(codes.Internal, "boom")
	f := &fakeRun{}
	_, attempts, err := runWithRetry(context.Background(), policy, f.hooks(), script(boom, boom, boom, boom))
	if err == nil || attempts != 3 {
		t.Fatalf("want failure after 3 attempts, got attempts=%d err=%v", attempts, err)
	}
	if len(f.pauses) != 2 || f.pauses[0] != 2*time.Second || f.pauses[1] != 4*time.Second {
		t.Fatalf("pauses = %v, want [2s 4s]", f.pauses)
	}
}

func TestBadImageFailsImmediately(t *testing.T) {
	for _, c := range []codes.Code{codes.InvalidArgument, codes.NotFound} {
		f := &fakeRun{}
		_, attempts, err := runWithRetry(context.Background(), policy, f.hooks(), script(status.Error(c, "x")))
		if err == nil || attempts != 1 || len(f.pauses) != 0 {
			t.Fatalf("%v: want immediate failure, got attempts=%d pauses=%v", c, attempts, f.pauses)
		}
	}
}

func TestRecoversAfterOneInternal(t *testing.T) {
	f := &fakeRun{}
	res, attempts, err := runWithRetry(context.Background(), policy, f.hooks(),
		script(status.Error(codes.Internal, "x")))
	if err != nil || res != "ok" || attempts != 2 {
		t.Fatalf("got %q attempts=%d err=%v", res, attempts, err)
	}
}

func TestStopsOnShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	f := &fakeRun{}
	h := f.hooks()
	h.sleep = func(context.Context, time.Duration) bool { cancel(); return false } // Ctrl+C во время паузы
	_, _, err := runWithRetry(ctx, policy, h, script(status.Error(codes.Unavailable, "down")))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}
}
