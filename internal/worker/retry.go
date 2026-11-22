package worker

import (
	"context"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// retryPolicy — как повторять вызов analyzer.
//
//   - Unavailable: лежит analyzer или хранилище. Это беда всего сервиса, а не
//     этого фото, поэтому ждём сколько угодно (пауза растёт до maxWait) и
//     попытки не тратим. Иначе за время перезапуска analyzer все загруженные
//     фото получили бы failed.
//   - InvalidArgument, NotFound: файл битый или его нет — сразу ошибка.
//   - Остальное (Internal, DeadlineExceeded…): до maxAttempts попыток.
type retryPolicy struct {
	maxAttempts int
	base        time.Duration // первая пауза, дальше удваивается
	maxWait     time.Duration // потолок паузы
}

// retryHooks — что делать вокруг попыток. Вынесено, чтобы логику можно было
// проверить тестом без настоящих analyzer'а, базы и часов.
type retryHooks struct {
	beforeAttempt func(attempt int) // перед каждой настоящей попыткой (не перед ожиданием analyzer'а)
	onWait        func(err error, attempt int, pause time.Duration, unavailable bool)
	sleep         func(ctx context.Context, d time.Duration) bool // false — ctx отменён
}

// runWithRetry вызывает call по правилам policy. Возвращает результат,
// число настоящих попыток и последнюю ошибку.
func runWithRetry[T any](ctx context.Context, p retryPolicy, h retryHooks, call func(context.Context) (T, error)) (T, int, error) {
	var zero T
	attempt := 1
	unavailableStreak := 0
	h.beforeAttempt(attempt)
	for {
		res, err := call(ctx)
		if err == nil {
			return res, attempt, nil
		}
		if ctx.Err() != nil {
			return zero, attempt, ctx.Err()
		}

		var pause time.Duration
		unavailable := status.Code(err) == codes.Unavailable
		switch {
		case unavailable:
			pause = backoff(p.base, unavailableStreak, p.maxWait)
			unavailableStreak++
		case !retryable(err) || attempt >= p.maxAttempts:
			return zero, attempt, err
		default:
			pause = backoff(p.base, attempt-1, p.maxWait)
			unavailableStreak = 0
		}

		h.onWait(err, attempt, pause, unavailable)
		if !h.sleep(ctx, pause) {
			return zero, attempt, ctx.Err()
		}
		if !unavailable {
			attempt++
			h.beforeAttempt(attempt)
		}
	}
}

// backoff: base, 2·base, 4·base… но не больше max.
func backoff(base time.Duration, n int, max time.Duration) time.Duration {
	if n > 20 {
		return max
	}
	d := base << n
	if d > max || d <= 0 {
		return max
	}
	return d
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
