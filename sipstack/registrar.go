package sipstack

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/emiago/sipgo"
	"github.com/emiago/sipgo/sip"
	"github.com/google/uuid"
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
	// Use a background context for the long-lived registration loop.
	// The passed-in ctx is only used for the initial REGISTER transaction.
	regCtx, cancel := context.WithCancel(context.Background())
	reg.cancel = cancel
	ur.regs[reg.DeviceID] = reg
	ur.mu.Unlock()

	if err := ur.sendRegister(ctx, reg, registerExpiry); err != nil {
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

	return ur.sendRegister(ctx, reg, 0)
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
	from := &sip.FromHeader{
		Address: sip.Uri{User: reg.User, Host: reg.Host},
	}
	from.Params.Add("tag", sip.GenerateTagN(16))
	req.AppendHeader(from)
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

	// Call-ID
	callID := sip.CallIDHeader(uuid.New().String())
	req.AppendHeader(&callID)

	// Max-Forwards
	maxfwd := sip.MaxForwardsHeader(70)
	req.AppendHeader(&maxfwd)

	// Content-Length: 0 — must be explicit because DoDigestAuth skips
	// clientRequestBuildReq which normally adds these headers.
	req.SetBody(nil)

	return req
}

func (ur *UpstreamRegistrar) sendRegister(ctx context.Context, reg *UpstreamReg, expires int) error {
	req := ur.buildRegisterRequest(reg, expires)

	slog.Info("sending REGISTER",
		"device", reg.DeviceID,
		"target", fmt.Sprintf("%s:%d", reg.Host, reg.Port),
		"transport", reg.Transport,
		"user", reg.User,
		"expires", expires)

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

func (ur *UpstreamRegistrar) reregisterLoop(ctx context.Context, reg *UpstreamReg) {
	interval := time.Duration(float64(registerExpiry)*reregisterPercent) * time.Second
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			regCtx, regCancel := context.WithTimeout(ctx, 15*time.Second)
			if err := ur.sendRegister(regCtx, reg, registerExpiry); err != nil {
				slog.Error("re-register failed",
					"device", reg.DeviceID,
					"user", reg.User,
					"error", err)
			}
			regCancel()
		case <-ctx.Done():
			return
		}
	}
}
