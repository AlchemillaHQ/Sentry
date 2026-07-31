package sipstack

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"syscall"

	"github.com/AlchemillaHQ/Sentry/config"
	"github.com/AlchemillaHQ/Sentry/logger"
	"github.com/emiago/sipgo"
	"github.com/emiago/sipgo/sip"
	"github.com/rs/zerolog/log"
	"golang.org/x/sys/unix"
)

type Stack struct {
	cfg       config.SIPConfig
	ua        *sipgo.UserAgent
	server    *sipgo.Server
	client    *sipgo.Client
	listenTLS *tls.Config

	mu         sync.RWMutex
	onRegister func(req *sip.Request, tx sip.ServerTransaction)
	onInvite   func(req *sip.Request, tx sip.ServerTransaction)
	onAck      func(req *sip.Request, tx sip.ServerTransaction)
	onBye      func(req *sip.Request, tx sip.ServerTransaction)
	onCancel   func(req *sip.Request, tx sip.ServerTransaction)
}

var (
	newUA     = sipgo.NewUA
	newServer = sipgo.NewServer
	newClient = sipgo.NewClient
)

func New(cfg config.SIPConfig) (*Stack, error) {
	var level slog.Level
	if cfg.LogSIP {
		level = slog.LevelDebug
	} else {
		level = slog.LevelWarn
	}

	zl := log.With().Str("subsys", "sipgo").Logger()
	bridge := logger.NewSlogBridge(zl, level)
	sl := slog.New(bridge)

	slog.SetDefault(sl)
	sip.SetDefaultLogger(sl)

	uaOpts := []sipgo.UserAgentOption{
		sipgo.WithUserAgent(cfg.UserAgent),
		sipgo.WithUserAgentTransportLayerOptions(
			sip.WithTransportLayerConnectionReuse(true),
		),
	}

	outboundTLS := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: cfg.TLSInsecureSkipVerify,
	}
	if cfg.TLSCert != "" && cfg.TLSKey != "" {
		cert, err := tls.LoadX509KeyPair(cfg.TLSCert, cfg.TLSKey)
		if err != nil {
			return nil, fmt.Errorf("load TLS cert: %w", err)
		}
		outboundTLS.Certificates = []tls.Certificate{cert}
	}
	uaOpts = append(uaOpts, sipgo.WithUserAgenTLSConfig(outboundTLS))

	ua, err := newUA(uaOpts...)
	if err != nil {
		return nil, fmt.Errorf("create UA: %w", err)
	}

	server, err := newServer(ua)
	if err != nil {
		ua.Close()
		return nil, fmt.Errorf("create server: %w", err)
	}

	client, err := newClient(ua)
	if err != nil {
		ua.Close()
		return nil, fmt.Errorf("create client: %w", err)
	}

	s := &Stack{
		cfg:       cfg,
		ua:        ua,
		server:    server,
		client:    client,
		listenTLS: outboundTLS,
	}

	server.OnRegister(s.handleRegister)
	server.OnInvite(s.handleInvite)
	server.OnAck(s.handleAck)
	server.OnBye(s.handleBye)
	server.OnCancel(s.handleCancel)

	return s, nil
}

func (s *Stack) SetOnRegister(fn func(req *sip.Request, tx sip.ServerTransaction)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.onRegister = fn
}

func (s *Stack) SetOnInvite(fn func(req *sip.Request, tx sip.ServerTransaction)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.onInvite = fn
}

func (s *Stack) SetOnAck(fn func(req *sip.Request, tx sip.ServerTransaction)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.onAck = fn
}

func (s *Stack) SetOnBye(fn func(req *sip.Request, tx sip.ServerTransaction)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.onBye = fn
}

func (s *Stack) SetOnCancel(fn func(req *sip.Request, tx sip.ServerTransaction)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.onCancel = fn
}

func (s *Stack) Client() *sipgo.Client        { return s.client }
func (s *Stack) Server() *sipgo.Server        { return s.server }
func (s *Stack) UA() *sipgo.UserAgent         { return s.ua }
func (s *Stack) ExternalIP() string           { return s.cfg.ExternalIP }
func (s *Stack) ExternalSIPPort() int         { return s.cfg.ExternalSIPPort }
func (s *Stack) ExternalSIPTransport() string { return s.cfg.ExternalSIPTransport }

func (s *Stack) handleRegister(req *sip.Request, tx sip.ServerTransaction) {
	s.mu.RLock()
	fn := s.onRegister
	s.mu.RUnlock()
	if fn != nil {
		fn(req, tx)
	} else {
		res := sip.NewResponseFromRequest(req, 405, "Method Not Allowed", nil)
		tx.Respond(res)
	}
}

func (s *Stack) handleInvite(req *sip.Request, tx sip.ServerTransaction) {
	s.mu.RLock()
	fn := s.onInvite
	s.mu.RUnlock()
	if fn != nil {
		fn(req, tx)
	} else {
		res := sip.NewResponseFromRequest(req, 480, "Temporarily Unavailable", nil)
		tx.Respond(res)
	}
}

func (s *Stack) handleAck(req *sip.Request, tx sip.ServerTransaction) {
	s.mu.RLock()
	fn := s.onAck
	s.mu.RUnlock()
	if fn != nil {
		fn(req, tx)
	}
}

func (s *Stack) handleBye(req *sip.Request, tx sip.ServerTransaction) {
	s.mu.RLock()
	fn := s.onBye
	s.mu.RUnlock()
	if fn != nil {
		fn(req, tx)
	} else {
		res := sip.NewResponseFromRequest(req, 200, "OK", nil)
		tx.Respond(res)
	}
}

func (s *Stack) handleCancel(req *sip.Request, tx sip.ServerTransaction) {
	s.mu.RLock()
	fn := s.onCancel
	s.mu.RUnlock()
	if fn != nil {
		fn(req, tx)
	}
}

var (
	serveUDP = serveUDPImpl
	serveTCP = serveTCPImpl
	serveTLS = serveTLSImpl
)

func serveUDPImpl(s *Stack, errCh chan<- error, ctx context.Context) {
	log.Info().Str("transport", "udp").Str("addr", s.cfg.UDPAddr).Msg("SIP listening")

	lc := net.ListenConfig{
		Control: func(network, address string, c syscall.RawConn) error {
			var opErr error
			if err := c.Control(func(fd uintptr) {
				opErr = unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_REUSEPORT, 1)
			}); err != nil {
				return err
			}
			return opErr
		},
	}

	conn, err := lc.ListenPacket(ctx, "udp", s.cfg.UDPAddr)
	if err != nil {
		errCh <- fmt.Errorf("udp listen: %w", err)
		return
	}

	if err := s.server.ServeUDP(conn.(*net.UDPConn)); err != nil {
		errCh <- fmt.Errorf("udp serve: %w", err)
	}
}

func serveTCPImpl(s *Stack, errCh chan<- error, ctx context.Context) {
	log.Info().Str("transport", "tcp").Str("addr", s.cfg.TCPAddr).Msg("SIP listening")
	if err := s.server.ListenAndServe(ctx, "tcp", s.cfg.TCPAddr); err != nil {
		errCh <- fmt.Errorf("tcp: %w", err)
	}
}

func serveTLSImpl(s *Stack, errCh chan<- error, ctx context.Context) {
	log.Info().Str("transport", "tls").Str("addr", s.cfg.TLSAddr).Msg("SIP listening")
	if err := s.server.ListenAndServeTLS(ctx, "tcp", s.cfg.TLSAddr, s.listenTLS); err != nil {
		errCh <- fmt.Errorf("tls: %w", err)
	}
}

func (s *Stack) ListenAndServe(ctx context.Context) error {
	errCh := make(chan error, 4)

	if s.cfg.UDPAddr != "" {
		go serveUDP(s, errCh, ctx)
	}

	if s.cfg.TCPAddr != "" {
		go serveTCP(s, errCh, ctx)
	}

	if s.cfg.TLSAddr != "" && s.cfg.TLSCert != "" && s.cfg.TLSKey != "" {
		go serveTLS(s, errCh, ctx)
	}

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Stack) Close() {
	s.ua.Close()
}
