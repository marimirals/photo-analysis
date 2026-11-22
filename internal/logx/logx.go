// Package logx — общий логгер: JSON в stdout и request_id из контекста
// в каждой строке, где передан контекст (InfoContext, ErrorContext…).
package logx

import (
	"context"
	"io"
	"log/slog"
)

type ctxKey struct{}

func WithRequestID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, ctxKey{}, id)
}

func RequestID(ctx context.Context) string {
	id, _ := ctx.Value(ctxKey{}).(string)
	return id
}

// handler дописывает request_id, если он есть в контексте.
type handler struct{ slog.Handler }

func (h handler) Handle(ctx context.Context, r slog.Record) error {
	if id := RequestID(ctx); id != "" {
		r.AddAttrs(slog.String("request_id", id))
	}
	return h.Handler.Handle(ctx, r)
}

func (h handler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return handler{h.Handler.WithAttrs(attrs)}
}

func (h handler) WithGroup(name string) slog.Handler {
	return handler{h.Handler.WithGroup(name)}
}

// New — JSON-логгер с полем service.
func New(w io.Writer, service string) *slog.Logger {
	return slog.New(handler{slog.NewJSONHandler(w, nil)}).With("service", service)
}
