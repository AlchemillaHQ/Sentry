package sipstack

import (
	"bufio"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/emiago/sipgo"
	"github.com/emiago/sipgo/sip"
	"github.com/google/uuid"
	"github.com/icholy/digest"
)

const (
	registerExpiry    = 120
	reregisterPercent = 0.75
)

type UpstreamReg struct {
	DeviceID  string
	User      string
	Host      string
	Port      int
	Transport string
	Password  string
	Realm     string

	cancel context.CancelFunc

	// TLS persistent connection state
	tlsMu   sync.Mutex
	tlsConn *tls.Conn
	tlsRaw  net.Conn
	reader  *bufio.Reader
	parser  *sip.Parser
	callID  sip.CallIDHeader
	cseqNo  uint32
}

func (r *UpstreamReg) closeTLS() {
	r.tlsMu.Lock()
	defer r.tlsMu.Unlock()
	if r.tlsConn != nil {
		r.tlsConn.Close()
		r.tlsConn = nil
		r.tlsRaw = nil
		r.reader = nil
	}
}

type UpstreamRegistrar struct {
	stack *Stack

	mu   sync.RWMutex
	regs map[string]*UpstreamReg
}

func NewUpstreamRegistrar(stack *Stack) *UpstreamRegistrar {
	return &UpstreamRegistrar{
		stack: stack,
		regs:  make(map[string]*UpstreamReg),
	}
}

func (ur *UpstreamRegistrar) Register(ctx context.Context, reg *UpstreamReg) error {
	ur.mu.Lock()
	if existing, ok := ur.regs[reg.DeviceID]; ok {
		if existing.cancel != nil {
			existing.cancel()
		}
	}
	regCtx, cancel := context.WithCancel(ctx)
	reg.cancel = cancel
	ur.regs[reg.DeviceID] = reg
	ur.mu.Unlock()

	if err := ur.sendRegister(regCtx, reg, registerExpiry); err != nil {
		ur.mu.Lock()
		delete(ur.regs, reg.DeviceID)
		ur.mu.Unlock()
		cancel()
		return err
	}

	go ur.reregisterLoop(regCtx, reg)
	return nil
}

func (ur *UpstreamRegistrar) Unregister(ctx context.Context, deviceID string) error {
	ur.mu.Lock()
	reg, ok := ur.regs[deviceID]
	if !ok {
		ur.mu.Unlock()
		return nil
	}
	if reg.cancel != nil {
		reg.cancel()
	}
	delete(ur.regs, deviceID)
	ur.mu.Unlock()

	err := ur.sendRegister(ctx, reg, 0)
	reg.closeTLS()
	return err
}

func (ur *UpstreamRegistrar) IsRegistered(deviceID string) bool {
	ur.mu.RLock()
	defer ur.mu.RUnlock()
	_, ok := ur.regs[deviceID]
	return ok
}

func (ur *UpstreamRegistrar) GetReg(deviceID string) *UpstreamReg {
	ur.mu.RLock()
	defer ur.mu.RUnlock()
	return ur.regs[deviceID]
}

func (ur *UpstreamRegistrar) StopAll() {
	ur.mu.Lock()
	defer ur.mu.Unlock()
	for _, reg := range ur.regs {
		if reg.cancel != nil {
			reg.cancel()
		}
		reg.closeTLS()
	}
	ur.regs = make(map[string]*UpstreamReg)
}

func (ur *UpstreamRegistrar) buildRegisterRequest(reg *UpstreamReg, expires int) *sip.Request {
	target := sip.Uri{
		Host: reg.Host,
		Port: reg.Port,
	}
	if reg.Transport != "" {
		target.UriParams = sip.NewParams()
		target.UriParams.Add("transport", reg.Transport)
	}

	req := sip.NewRequest(sip.REGISTER, target)
	req.AppendHeader(&sip.FromHeader{
		Address: sip.Uri{User: reg.User, Host: reg.Host},
	})
	req.AppendHeader(&sip.ToHeader{
		Address: sip.Uri{User: reg.User, Host: reg.Host},
	})

	contactHost := ur.stack.ExternalIP()
	if contactHost == "" {
		contactHost = "127.0.0.1"
	}

	req.AppendHeader(&sip.ContactHeader{
		Address: sip.Uri{
			User: reg.User,
			Host: contactHost,
			Port: 5060,
		},
	})

	expiresHdr := sip.ExpiresHeader(expires)
	req.AppendHeader(&expiresHdr)

	req.AppendHeader(&sip.CSeqHeader{
		SeqNo:      1,
		MethodName: sip.REGISTER,
	})

	return req
}

func (ur *UpstreamRegistrar) sendRegister(ctx context.Context, reg *UpstreamReg, expires int) error {
	if reg.Transport == "tls" {
		return ur.sendRegisterTLS(ctx, reg, expires)
	}
	return ur.sendRegisterSipgo(ctx, reg, expires)
}

func (ur *UpstreamRegistrar) sendRegisterSipgo(ctx context.Context, reg *UpstreamReg, expires int) error {
	req := ur.buildRegisterRequest(reg, expires)

	slog.Info("sending REGISTER",
		"device", reg.DeviceID,
		"target", fmt.Sprintf("%s:%d", reg.Host, reg.Port),
		"transport", reg.Transport,
		"user", reg.User,
		"request_uri", req.StartLine())

	res, err := ur.stack.Client().Do(ctx, req)
	if err != nil {
		slog.Error("REGISTER failed",
			"device", reg.DeviceID,
			"target", fmt.Sprintf("%s:%d", reg.Host, reg.Port),
			"transport", reg.Transport,
			"error", err)
		return fmt.Errorf("send REGISTER: %w", err)
	}

	if res.StatusCode == 401 || res.StatusCode == 407 {
		authReq := ur.buildRegisterRequest(reg, expires)
		res, err = ur.stack.Client().DoDigestAuth(ctx, authReq, res, sipgo.DigestAuth{
			Username: reg.User,
			Password: reg.Password,
		})
		if err != nil {
			return fmt.Errorf("digest auth REGISTER: %w", err)
		}
	}

	if res.StatusCode >= 200 && res.StatusCode < 300 {
		slog.Info("upstream registration successful",
			"device", reg.DeviceID,
			"user", reg.User,
			"host", reg.Host,
			"expires", expires)
		return nil
	}

	return fmt.Errorf("REGISTER rejected: %d %s", res.StatusCode, res.Reason)
}

func (ur *UpstreamRegistrar) prepareTLSRequest(req *sip.Request, localAddr string) {
	// Add From tag (mandatory per RFC 3261)
	if from := req.From(); from != nil && !from.Params.Has("tag") {
		from.Params.Add("tag", sip.GenerateTagN(16))
	}

	// Add Via header
	localHost, _, _ := net.SplitHostPort(localAddr)
	if localHost == "" {
		localHost = ur.stack.ExternalIP()
	}
	via := &sip.ViaHeader{
		ProtocolName:    "SIP",
		ProtocolVersion: "2.0",
		Transport:       "TLS",
		Host:            localHost,
		Port:            5061,
	}
	via.Params.Add("branch", sip.GenerateBranchN(16))
	via.Params.Add("rport", "")
	req.PrependHeader(via)

	// Add Call-ID
	if req.CallID() == nil {
		callID := sip.CallIDHeader(uuid.New().String())
		req.AppendHeader(&callID)
	}

	// Add Max-Forwards
	if req.MaxForwards() == nil {
		maxfwd := sip.MaxForwardsHeader(70)
		req.AppendHeader(&maxfwd)
	}

	// Add Content-Length: 0 (required for stream transports per RFC 3261 §7.5)
	if req.Body() == nil {
		req.SetBody(nil)
	}
}

func (ur *UpstreamRegistrar) sendRegisterTLS(ctx context.Context, reg *UpstreamReg, expires int) error {
	reg.tlsMu.Lock()
	defer reg.tlsMu.Unlock()

	addr := fmt.Sprintf("%s:%d", reg.Host, reg.Port)

	// Establish connection if we don't have one
	if reg.tlsConn == nil {
		slog.Info("establishing TLS connection",
			"device", reg.DeviceID,
			"target", addr)

		dialer := &net.Dialer{Timeout: 10 * time.Second}
		rawConn, err := dialer.DialContext(ctx, "tcp", addr)
		if err != nil {
			return fmt.Errorf("tcp dial %s: %w", addr, err)
		}

		tlsConf := &tls.Config{
			ServerName:         reg.Host,
			InsecureSkipVerify: true,
			MinVersion:         tls.VersionTLS12,
		}
		conn := tls.Client(rawConn, tlsConf)
		if err := conn.HandshakeContext(ctx); err != nil {
			rawConn.Close()
			return fmt.Errorf("tls handshake %s: %w", addr, err)
		}

		reg.tlsConn = conn
		reg.tlsRaw = rawConn
		reg.reader = bufio.NewReader(conn)
		reg.parser = sip.NewParser()
		reg.callID = sip.CallIDHeader(uuid.New().String())
		reg.cseqNo = 1

		slog.Info("TLS connection established",
			"device", reg.DeviceID,
			"target", addr,
			"local", rawConn.LocalAddr().String())
	}

	slog.Info("sending REGISTER (TLS)",
		"device", reg.DeviceID,
		"target", addr,
		"user", reg.User,
		"expires", expires)

	req := ur.buildRegisterRequest(reg, expires)
	req.Recipient.UriParams = nil
	req.AppendHeader(&reg.callID)
	ur.prepareTLSRequest(req, reg.tlsRaw.LocalAddr().String())
	cseq := req.CSeq()
	if cseq != nil {
		cseq.SeqNo = reg.cseqNo
	}
	reg.cseqNo++

	if _, err := reg.tlsConn.Write([]byte(req.String())); err != nil {
		reg.tlsConn.Close()
		reg.tlsConn = nil
		return fmt.Errorf("write REGISTER: %w", err)
	}

	reg.tlsConn.SetReadDeadline(time.Now().Add(10 * time.Second))
	res, err := ur.readSIPResponse(reg.reader, reg.parser)
	if err != nil {
		reg.tlsConn.Close()
		reg.tlsConn = nil
		return fmt.Errorf("read response: %w", err)
	}

	slog.Info("REGISTER response", "status", res.StatusCode, "reason", res.Reason)

	if res.StatusCode == 401 || res.StatusCode == 407 {
		authReq := ur.buildRegisterRequest(reg, expires)
		authReq.Recipient.UriParams = nil
		authReq.AppendHeader(&reg.callID)
		ur.prepareTLSRequest(authReq, reg.tlsRaw.LocalAddr().String())
		authCseq := authReq.CSeq()
		if authCseq != nil {
			authCseq.SeqNo = reg.cseqNo
		}
		reg.cseqNo++

		var challengeHeader string
		if res.StatusCode == 407 {
			h := res.GetHeader("Proxy-Authenticate")
			if h != nil {
				challengeHeader = h.Value()
			}
		} else {
			h := res.GetHeader("WWW-Authenticate")
			if h != nil {
				challengeHeader = h.Value()
			}
		}
		if challengeHeader == "" {
			return fmt.Errorf("401/407 but no auth challenge header")
		}

		chal, err := digest.ParseChallenge(challengeHeader)
		if err != nil {
			return fmt.Errorf("parse digest challenge: %w", err)
		}
		chal.Algorithm = sip.ASCIIToUpper(chal.Algorithm)

		cred, err := digest.Digest(chal, digest.Options{
			Method:   sip.REGISTER.String(),
			URI:      authReq.Recipient.Addr(),
			Username: reg.User,
			Password: reg.Password,
		})
		if err != nil {
			return fmt.Errorf("compute digest: %w", err)
		}

		authReq.RemoveHeader("Authorization")
		authReq.AppendHeader(sip.NewHeader("Authorization", cred.String()))

		if _, err := reg.tlsConn.Write([]byte(authReq.String())); err != nil {
			reg.tlsConn.Close()
			reg.tlsConn = nil
			return fmt.Errorf("write auth REGISTER: %w", err)
		}

		reg.tlsConn.SetReadDeadline(time.Now().Add(10 * time.Second))
		res, err = ur.readSIPResponse(reg.reader, reg.parser)
		if err != nil {
			reg.tlsConn.Close()
			reg.tlsConn = nil
			return fmt.Errorf("read auth response: %w", err)
		}

		slog.Info("REGISTER auth response", "status", res.StatusCode, "reason", res.Reason)
	}

	if res.StatusCode >= 200 && res.StatusCode < 300 {
		slog.Info("upstream registration successful (TLS)",
			"device", reg.DeviceID,
			"user", reg.User,
			"host", reg.Host,
			"expires", expires)
		// Clear read deadline so the connection stays open for keep-alive
		reg.tlsConn.SetReadDeadline(time.Time{})
		return nil
	}

	return fmt.Errorf("REGISTER rejected: %d %s", res.StatusCode, res.Reason)
}

func (ur *UpstreamRegistrar) readSIPResponse(reader *bufio.Reader, parser *sip.Parser) (*sip.Response, error) {
	stream := parser.NewSIPStream()
	defer stream.Close()

	buf := make([]byte, 4096)
	for {
		n, err := reader.Read(buf)
		if err != nil {
			return nil, fmt.Errorf("read: %w", err)
		}
		if n == 0 {
			continue
		}

		var res *sip.Response
		parseErr := stream.ParseSIPStream(buf[:n], func(msg sip.Message) {
			if r, ok := msg.(*sip.Response); ok {
				res = r
			}
		})

		if res != nil {
			return res, nil
		}

		if parseErr != nil && parseErr != sip.ErrParseSipPartial {
			return nil, fmt.Errorf("parse: %w", parseErr)
		}
		// ErrParseSipPartial means we need more data, continue reading
	}
}

func (ur *UpstreamRegistrar) reregisterLoop(ctx context.Context, reg *UpstreamReg) {
	interval := time.Duration(float64(registerExpiry)*reregisterPercent) * time.Second
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// For TLS, also monitor the connection for unexpected closure
	if reg.Transport == "tls" {
		go ur.tlsConnWatcher(ctx, reg)
	}

	for {
		select {
		case <-ticker.C:
			if err := ur.sendRegister(ctx, reg, registerExpiry); err != nil {
				slog.Error("re-register failed",
					"device", reg.DeviceID,
					"user", reg.User,
					"error", err)
				// For TLS, kill the dead connection so next attempt reconnects
				if reg.Transport == "tls" {
					reg.closeTLS()
				}
			}
		case <-ctx.Done():
			// Send expires=0 to unregister on clean shutdown
			if reg.Transport == "tls" {
				reg.closeTLS()
			}
			return
		}
	}
}

// tlsConnWatcher monitors the TLS connection for unexpected data or closure.
// If the PBX closes the connection, this triggers an immediate re-register.
func (ur *UpstreamRegistrar) tlsConnWatcher(ctx context.Context, reg *UpstreamReg) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		reg.tlsMu.Lock()
		conn := reg.tlsConn
		reader := reg.reader
		reg.tlsMu.Unlock()

		if conn == nil || reader == nil {
			// No connection yet or it was closed; wait and retry
			select {
			case <-time.After(2 * time.Second):
				continue
			case <-ctx.Done():
				return
			}
		}

		// Set a long read deadline to detect connection drops
		conn.SetReadDeadline(time.Now().Add(30 * time.Second))
		_, err := reader.Peek(1)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				// timeout is normal — connection still alive
				continue
			}
			if err == io.EOF {
				slog.Warn("TLS connection closed by PBX, will reconnect",
					"device", reg.DeviceID)
			} else {
				slog.Warn("TLS connection error, will reconnect",
					"device", reg.DeviceID,
					"error", err)
			}
			reg.closeTLS()
			// Trigger immediate re-register
			if err := ur.sendRegister(ctx, reg, registerExpiry); err != nil {
				slog.Error("reconnect re-register failed",
					"device", reg.DeviceID,
					"error", err)
			}
		}
	}
}
