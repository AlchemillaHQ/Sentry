package sipstack

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/emiago/sipgo"
	"github.com/emiago/sipgo/sip"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
)

const (
	registerExpiry    = 600
	reregisterPercent = 0.75
)

var reregisterInterval = time.Duration(float64(registerExpiry)*reregisterPercent) * time.Second

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

type Registrar interface {
	Register(ctx context.Context, reg *UpstreamReg) error
	Unregister(ctx context.Context, deviceID string) error
	IsRegistered(deviceID string) bool
	GetReg(deviceID string) *UpstreamReg
	StopAll()
	UnregisterAll(ctx context.Context)
}

type sipClient interface {
	Do(ctx context.Context, req *sip.Request, opts ...sipgo.ClientRequestOption) (*sip.Response, error)
	DoDigestAuth(ctx context.Context, req *sip.Request, res *sip.Response, auth sipgo.DigestAuth) (*sip.Response, error)
}

type UpstreamRegistrar struct {
	stack  *Stack
	client sipClient

	mu   sync.RWMutex
	regs map[string]*UpstreamReg
}

var _ Registrar = (*UpstreamRegistrar)(nil)


func NewUpstreamRegistrar(stack *Stack) *UpstreamRegistrar {
	ur := &UpstreamRegistrar{
		stack: stack,
		regs:  make(map[string]*UpstreamReg),
	}
	if stack != nil {
		ur.client = stack.Client()
	}
	return ur
}

func newUpstreamRegistrarWithClient(stack *Stack, client sipClient) *UpstreamRegistrar {
	return &UpstreamRegistrar{
		stack:  stack,
		client: client,
		regs:   make(map[string]*UpstreamReg),
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

func (ur *UpstreamRegistrar) UnregisterAll(ctx context.Context) {
	ur.mu.Lock()
	regs := make([]*UpstreamReg, 0, len(ur.regs))
	for _, reg := range ur.regs {
		regs = append(regs, reg)
		if reg.cancel != nil {
			reg.cancel()
		}
	}
	ur.regs = make(map[string]*UpstreamReg)
	ur.mu.Unlock()

	const maxConcurrent = 50
	sem := make(chan struct{}, maxConcurrent)
	var wg sync.WaitGroup

	for _, reg := range regs {
		wg.Add(1)
		sem <- struct{}{}
		go func(r *UpstreamReg) {
			defer func() { <-sem }()
			defer wg.Done()
			unregCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			if err := ur.sendRegister(unregCtx, r, 0); err != nil {
				log.Error().Err(err).Str("device", r.DeviceID).Msg("unregister on shutdown failed")
			}
		}(reg)
	}
	wg.Wait()
	log.Info().Int("count", len(regs)).Msg("all upstream registrations cancelled")
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
	b2buaSIPUser := fmt.Sprintf("%s_%s", reg.User, reg.DeviceID[:8])

	contactAddr := sip.Uri{
		User: b2buaSIPUser,
		Host: contactHost,
		Port: ur.stack.ExternalSIPPort(),
	}
	if reg.Transport != "" && reg.Transport != "udp" {
		contactAddr.UriParams = sip.NewParams()
		contactAddr.UriParams.Add("transport", reg.Transport)
	}

	req.AppendHeader(&sip.ContactHeader{Address: contactAddr})

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

	log.Info().
		Str("device", reg.DeviceID).
		Str("target", fmt.Sprintf("%s:%d", reg.Host, reg.Port)).
		Str("transport", reg.Transport).
		Str("user", reg.User).
		Int("expires", expires).
		Msg("sending REGISTER")

	res, err := ur.client.Do(ctx, req)
	if err != nil {
		log.Error().Err(err).
			Str("device", reg.DeviceID).
			Str("target", fmt.Sprintf("%s:%d", reg.Host, reg.Port)).
			Str("transport", reg.Transport).
			Msg("REGISTER failed")
		return fmt.Errorf("send REGISTER: %w", err)
	}

	if res.StatusCode == 401 || res.StatusCode == 407 {
		authReq := ur.buildRegisterRequest(reg, expires)
		res, err = ur.client.DoDigestAuth(ctx, authReq, res, sipgo.DigestAuth{
			Username: reg.User,
			Password: reg.Password,
		})
		if err != nil {
			return fmt.Errorf("digest auth REGISTER: %w", err)
		}
	}

	if res.StatusCode >= 200 && res.StatusCode < 300 {
		log.Info().
			Str("device", reg.DeviceID).
			Str("user", reg.User).
			Str("host", reg.Host).
			Int("expires", expires).
			Msg("upstream registration successful")
		return nil
	}

	return fmt.Errorf("REGISTER rejected: %d %s", res.StatusCode, res.Reason)
}

func (ur *UpstreamRegistrar) reregisterLoop(ctx context.Context, reg *UpstreamReg) {
	jitter := time.Duration(rand.Int63n(int64(reregisterInterval)))

	timer := time.NewTimer(jitter)
	defer timer.Stop()

	for {
		select {
		case <-timer.C:
			regCtx, regCancel := context.WithTimeout(ctx, 15*time.Second)
			if err := ur.sendRegister(regCtx, reg, registerExpiry); err != nil {
				log.Error().Err(err).
					Str("device", reg.DeviceID).
					Str("user", reg.User).
					Msg("re-register failed")
			}
			regCancel()
			timer.Reset(reregisterInterval)
		case <-ctx.Done():
			return
		}
	}
}
