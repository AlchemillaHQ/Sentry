package logger

import (
	"context"
	"log/slog"
	"time"

	"github.com/rs/zerolog"
)

type zerologHandler struct {
	zl    zerolog.Logger
	level slog.Leveler
}

func NewSlogBridge(zl zerolog.Logger, level slog.Leveler) slog.Handler {
	if level == nil {
		level = slog.LevelDebug
	}
	return &zerologHandler{zl: zl, level: level}
}

func (h *zerologHandler) Enabled(_ context.Context, l slog.Level) bool {
	return l >= h.level.Level()
}

func (h *zerologHandler) Handle(_ context.Context, r slog.Record) error {
	ev := h.zl.WithLevel(slogToZerolog(r.Level))
	r.Attrs(func(a slog.Attr) bool {
		ev = appendAttr(ev, a)
		return true
	})
	ev.Msg(r.Message)
	return nil
}

func (h *zerologHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	zl2 := h.zl.With()
	for _, a := range attrs {
		zl2 = appendAttrCtx(zl2, a)
	}
	return &zerologHandler{zl: zl2.Logger(), level: h.level}
}

func (h *zerologHandler) WithGroup(name string) slog.Handler {
	return &zerologHandler{zl: h.zl.With().Str(name, "").Logger(), level: h.level}
}

func slogToZerolog(l slog.Level) zerolog.Level {
	switch {
	case l >= slog.LevelError:
		return zerolog.ErrorLevel
	case l >= slog.LevelWarn:
		return zerolog.WarnLevel
	case l >= slog.LevelInfo:
		return zerolog.InfoLevel
	default:
		return zerolog.DebugLevel
	}
}

func appendAttr(ev *zerolog.Event, a slog.Attr) *zerolog.Event {
	v := a.Value
	switch v.Kind() {
	case slog.KindString:
		return ev.Str(a.Key, v.String())
	case slog.KindInt64:
		return ev.Int64(a.Key, v.Int64())
	case slog.KindUint64:
		return ev.Uint64(a.Key, v.Uint64())
	case slog.KindFloat64:
		return ev.Float64(a.Key, v.Float64())
	case slog.KindBool:
		return ev.Bool(a.Key, v.Bool())
	case slog.KindDuration:
		return ev.Dur(a.Key, v.Duration())
	case slog.KindTime:
		return ev.Time(a.Key, v.Time())
	case slog.KindGroup:
		for _, ga := range v.Group() {
			ev = appendAttr(ev, ga)
		}
		return ev
	case slog.KindLogValuer:
		return appendAttr(ev, slog.Attr{Key: a.Key, Value: v.LogValuer().LogValue()})
	default:
		return ev.Str(a.Key, v.String())
	}
}

func appendAttrCtx(ctx zerolog.Context, a slog.Attr) zerolog.Context {
	v := a.Value
	switch v.Kind() {
	case slog.KindString:
		return ctx.Str(a.Key, v.String())
	case slog.KindInt64:
		return ctx.Int64(a.Key, v.Int64())
	case slog.KindUint64:
		return ctx.Uint64(a.Key, v.Uint64())
	case slog.KindFloat64:
		return ctx.Float64(a.Key, v.Float64())
	case slog.KindBool:
		return ctx.Bool(a.Key, v.Bool())
	case slog.KindDuration:
		return ctx.Dur(a.Key, time.Duration(v.Int64()))
	case slog.KindTime:
		v.Time()
		return ctx.Time(a.Key, v.Time())
	case slog.KindGroup:
		for _, ga := range v.Group() {
			ctx = appendAttrCtx(ctx, ga)
		}
		return ctx
	case slog.KindLogValuer:
		return appendAttrCtx(ctx, slog.Attr{Key: a.Key, Value: v.LogValuer().LogValue()})
	default:
		return ctx.Str(a.Key, v.String())
	}
}
