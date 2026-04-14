package b2bua

import (
	"errors"
	"fmt"
	"net"
	"sync"

	"github.com/AlchemillaHQ/Difuse-B2BUA/config"
	"github.com/AlchemillaHQ/Difuse-B2BUA/fcm"
	"github.com/AlchemillaHQ/Difuse-B2BUA/pushkit"
	"github.com/AlchemillaHQ/Difuse-B2BUA/registry"

	"github.com/cloudwebrtc/go-sip-ua/pkg/account"
	"github.com/cloudwebrtc/go-sip-ua/pkg/auth"
	"github.com/cloudwebrtc/go-sip-ua/pkg/session"
	"github.com/cloudwebrtc/go-sip-ua/pkg/stack"
	"github.com/cloudwebrtc/go-sip-ua/pkg/ua"
	"github.com/cloudwebrtc/go-sip-ua/pkg/utils"
	"github.com/ghettovoice/gosip/log"
	"github.com/ghettovoice/gosip/sip"
	"github.com/ghettovoice/gosip/sip/parser"
	"github.com/ghettovoice/gosip/transport"
)

type B2BCall struct {
	src *session.Session
	//TODO: Add support for forked calls
	dest *session.Session
}

func (b *B2BCall) ToString() string {
	return b.src.Contact() + " => " + b.dest.Contact()
}

// UpstreamAccount maps a locally-registered user to an upstream PBX.
// The B2BUA registers on behalf of the user with the upstream PBX so that:
//   - Inbound calls from the PBX are forwarded to the locally-registered device.
//   - Outbound calls from the device are forwarded through the PBX.
type UpstreamAccount struct {
	// LocalUser is the SIP username as registered with this B2BUA (e.g. "1000").
	LocalUser string
	// UpstreamHost is the PBX hostname/IP (e.g. "example.com").
	UpstreamHost string
	// UpstreamUser is the username used to authenticate with the PBX (often the same as LocalUser).
	UpstreamUser string
	// Password is the SIP digest password for the upstream PBX account.
	Password string
	// Transport is the SIP transport to use towards the PBX (udp, tcp, tls). Defaults to udp.
	Transport string
	// register is the live registration handle; kept so it can be unregistered on shutdown.
	register *ua.Register
}

// B2BUA .
type B2BUA struct {
	mu               sync.RWMutex
	stack            *stack.SipStack
	ua               *ua.UserAgent
	accounts         map[string]string
	registry         registry.Registry
	domains          []string
	calls            []*B2BCall
	rfc8599          *registry.RFC8599
	upstreamAccounts map[string]*UpstreamAccount // keyed by LocalUser
}

var (
	logger log.Logger
)

func init() {
	logger = utils.NewLogrusLogger(log.InfoLevel, "B2BUA", nil)
}

// NewB2BUA creates a new B2BUA from the provided configuration.
func NewB2BUA(cfg *config.Config) *B2BUA {
	pushCallback := func(pn *registry.PNParams, payload map[string]string) error {
		fmt.Printf("Handle Push Request:\nprovider=%v\nparam=%v\nprid=%v\npayload=%v", pn.Provider, pn.Param, pn.PRID, payload)
		switch pn.Provider {
		case "apns":
			go pushkit.DoPushKit(cfg.Push.APNSCert, pn.PRID, payload)
			return nil
		case "fcm":
			go fcm.FCMPush(cfg.Push.FCMServiceAccount, pn.PRID, payload)
			return nil
		}
		return fmt.Errorf("%v provider not found", pn.Provider)
	}

	b := &B2BUA{
		registry:         registry.Registry(registry.NewMemoryRegistry()),
		accounts:         make(map[string]string),
		rfc8599:          registry.NewRFC8599(pushCallback),
		upstreamAccounts: make(map[string]*UpstreamAccount),
	}

	var authenticator *auth.ServerAuthorizer = nil

	if !cfg.SIP.DisableAuth {
		authenticator = auth.NewServerAuthorizer(b.requestCredential, "b2bua", false)
	}

	stack := stack.NewSipStack(&stack.SipStackConfig{
		UserAgent:  "Go B2BUA/1.0.0",
		Extensions: []string{"replaces", "outbound"},
		Dns:        cfg.SIP.DNS,
		ServerAuthManager: stack.ServerAuthManager{
			Authenticator:     authenticator,
			RequiresChallenge: b.requiresChallenge,
		},
	})

	stack.OnConnectionError(b.handleConnectionError)

	if err := stack.Listen("udp", cfg.SIP.UDPAddr); err != nil {
		logger.Panic(err)
	}

	if err := stack.Listen("tcp", cfg.SIP.TCPAddr); err != nil {
		logger.Panic(err)
	}

	if cfg.SIP.TLSAddr != "" {
		tlsOptions := &transport.TLSConfig{Cert: cfg.SIP.TLSCert, Key: cfg.SIP.TLSKey}

		if err := stack.ListenTLS("tls", cfg.SIP.TLSAddr, tlsOptions); err != nil {
			logger.Panic(err)
		}
	}

	if cfg.SIP.WSSAddr != "" {
		tlsOptions := &transport.TLSConfig{Cert: cfg.SIP.TLSCert, Key: cfg.SIP.TLSKey}

		if err := stack.ListenTLS("wss", cfg.SIP.WSSAddr, tlsOptions); err != nil {
			logger.Panic(err)
		}
	}

	ua := ua.NewUserAgent(&ua.UserAgentConfig{

		SipStack: stack,
	})

	ua.InviteStateHandler = func(sess *session.Session, req *sip.Request, resp *sip.Response, state session.Status) {
		logger.Infof("InviteStateHandler: state => %v, type => %s", state, sess.Direction())

		switch state {
		// Handle incoming call.
		case session.InviteReceived:
			to, _ := (*req).To()
			from, _ := (*req).From()
			caller := from.Address
			called := to.Address

			doInvite := func(instance *registry.ContactInstance) {
				displayName := ""
				if from.DisplayName != nil {
					displayName = from.DisplayName.String()
				}

				// Create a temporary profile. In the future, it will support reading profiles from files or data
				// For example: use a specific ip or sip account as outbound trunk
				profile := account.NewProfile(caller, displayName, nil, 0, stack)

				recipient, err2 := parser.ParseSipUri("sip:" + called.User().String() + "@" + instance.Source + ";transport=" + instance.Transport)
				if err2 != nil {
					logger.Error(err2)
				}

				offer := sess.RemoteSdp()
				dest, err := ua.Invite(profile, called, recipient, &offer)
				if err != nil {
					logger.Errorf("B-Leg session error: %v", err)
					return
				}
				b.mu.Lock()
				b.calls = append(b.calls, &B2BCall{src: sess, dest: dest})
				b.mu.Unlock()
			}

			// Try to find online contact records.
			if contacts, found := b.registry.GetContacts(called); found {
				sess.Provisional(100, "Trying")
				for _, instance := range *contacts {
					doInvite(instance)
				}
				return
			}

			// Pushable: try to find pn-params in contact records.
			// Try to push the UA and wait for it to wake up.
			pusher, ok := b.rfc8599.TryPush(called, from)
			if ok {
				sess.Provisional(100, "Trying")
				instance, err := pusher.WaitContactOnline()
				if err != nil {
					logger.Errorf("Push failed, error: %v", err)
					sess.Reject(500, "Push failed")
					return
				}
				doInvite(instance)
				return
			}

			// Outbound routing: the caller is a locally-registered user and the
			// destination is external. Forward through the caller's upstream PBX.
			callerUser := caller.User().String()
			if upstream, found := b.upstreamForUser(callerUser); found {
				sess.Provisional(100, "Trying")
				displayName := ""
				if from.DisplayName != nil {
					displayName = from.DisplayName.String()
				}

				// Authenticate outbound leg as the upstream account user.
				outProfile := account.NewProfile(
					mustParseUri("sip:"+upstream.UpstreamUser+"@"+upstream.UpstreamHost),
					displayName,
					&account.AuthInfo{
						AuthUser: upstream.UpstreamUser,
						Password: upstream.Password,
						Realm:    realmFromHost(upstream.UpstreamHost),
					},
					0,
					stack,
				)

				// Route to the upstream PBX; let the PBX resolve the final destination.
				recipient, err2 := parser.ParseSipUri("sip:" + called.User().String() + "@" + upstream.UpstreamHost + ";transport=" + upstream.Transport)
				if err2 != nil {
					logger.Errorf("Failed to build upstream recipient: %v", err2)
					sess.Reject(500, "Internal error")
					return
				}

				offer := sess.RemoteSdp()
				dest, err := ua.Invite(outProfile, called, recipient, &offer)
				if err != nil {
					logger.Errorf("Upstream B-Leg session error: %v", err)
					sess.Reject(502, "Bad Gateway")
					return
				}
				b.mu.Lock()
				b.calls = append(b.calls, &B2BCall{src: sess, dest: dest})
				b.mu.Unlock()
				return
			}

			// Could not found any records
			sess.Reject(404, fmt.Sprintf("%v Not found", called))

		// Handle re-INVITE or UPDATE.
		case session.ReInviteReceived:
			logger.Infof("re-INVITE")
			switch sess.Direction() {
			case session.Incoming:
				sess.Accept(200)
			case session.Outgoing:
				//TODO: Need to provide correct answer.
			}

		// Handle 1XX
		case session.EarlyMedia:
			fallthrough
		case session.Provisional:
			call := b.findCall(sess)
			if call != nil && call.dest == sess {
				answer := call.dest.RemoteSdp()
				call.src.ProvideAnswer(answer)
				call.src.Provisional((*resp).StatusCode(), (*resp).Reason())
			}

		// Handle 200OK or ACK
		case session.Confirmed:
			//TODO: Add support for forked calls
			call := b.findCall(sess)
			if call != nil && call.dest == sess {
				answer := call.dest.RemoteSdp()
				call.src.ProvideAnswer(answer)
				call.src.Accept(200)
			}

		// Handle 4XX+
		case session.Failure:
			fallthrough
		case session.Canceled:
			fallthrough
		case session.Terminated:
			//TODO: Add support for forked calls
			call := b.findCall(sess)
			if call != nil {
				if call.src == sess {
					call.dest.End()
				} else if call.dest == sess {
					call.src.End()
				}
			}
			b.removeCall(sess)

		}
	}

	ua.RegisterStateHandler = func(state account.RegisterState) {
		// Forward the result to a per-registration channel when provided.
		if ch, ok := state.UserData.(chan account.RegisterState); ok {
			select {
			case ch <- state:
			default:
			}
		}
		if state.StatusCode == 200 {
			logger.Infof("Upstream registration OK: %s (expires %ds)", state.Account.URI, state.Expiration)
		} else if state.StatusCode != 0 {
			logger.Errorf("Upstream registration failed: %s — %d %s", state.Account.URI, state.StatusCode, state.Reason)
		}
	}

	stack.OnRequest(sip.REGISTER, b.handleRegister)
	b.stack = stack
	b.ua = ua
	return b
}

func (b *B2BUA) Calls() []*B2BCall {
	b.mu.RLock()
	defer b.mu.RUnlock()
	copy := make([]*B2BCall, len(b.calls))
	for i, c := range b.calls {
		copy[i] = c
	}
	return copy
}

func (b *B2BUA) findCall(sess *session.Session) *B2BCall {
	b.mu.RLock()
	defer b.mu.RUnlock()
	for _, call := range b.calls {
		if call.src == sess || call.dest == sess {
			return call
		}
	}
	return nil
}

func (b *B2BUA) removeCall(sess *session.Session) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for idx, call := range b.calls {
		if call.src == sess || call.dest == sess {
			b.calls = append(b.calls[:idx], b.calls[idx+1:]...)
			return
		}
	}
}

// Shutdown .
func (b *B2BUA) Shutdown() {
	// Unregister all upstream accounts gracefully.
	b.mu.Lock()
	upstream := make([]*UpstreamAccount, 0, len(b.upstreamAccounts))
	for _, acc := range b.upstreamAccounts {
		upstream = append(upstream, acc)
	}
	b.mu.Unlock()

	for _, acc := range upstream {
		if acc.register != nil {
			if err := acc.register.SendRegister(0); err != nil {
				logger.Warnf("Failed to unregister upstream %s@%s: %v", acc.UpstreamUser, acc.UpstreamHost, err)
			}
		}
	}
	b.ua.Shutdown()
}

// realmFromHost returns just the hostname portion of a host-or-host:port string,
// suitable for use as the SIP digest auth realm.
func realmFromHost(hostport string) string {
	if host, _, err := net.SplitHostPort(hostport); err == nil {
		return host
	}
	return hostport
}

func mustParseUri(raw string) sip.Uri {
	uri, err := parser.ParseUri(raw)
	if err != nil {
		panic("invalid URI: " + raw + ": " + err.Error())
	}
	return uri
}

func mustParseSipUri(raw string) sip.SipUri {
	uri, err := parser.ParseSipUri(raw)
	if err != nil {
		panic("invalid SIP URI: " + raw + ": " + err.Error())
	}
	return uri
}

func (b *B2BUA) requiresChallenge(req sip.Request) bool {
	switch req.Method() {
	case sip.REGISTER:
		// Only challenge if we have local accounts configured.
		// When using dynamic upstream registration, disable_auth should be
		// set in config and the upstream PBX is the real authenticator.
		b.mu.RLock()
		hasAccounts := len(b.accounts) > 0
		b.mu.RUnlock()
		return hasAccounts
	}
	// INVITEs and everything else: don't challenge locally.
	// Devices proved identity at REGISTER time; upstream PBX authenticates
	// outbound legs independently.
	return false
}

// AddAccount .
func (b *B2BUA) AddAccount(username string, password string) {
	b.accounts[username] = password
}

// GetAccounts .
func (b *B2BUA) GetAccounts() map[string]string {
	return b.accounts
}

// AddUpstreamAccount registers a user with their upstream PBX and stores the
// mapping so that outbound calls from that user are routed through the PBX.
//
// Example:
//
//	b2bua.AddUpstreamAccount("1000", "example.com", "1000", "pass1000")
//	b2bua.AddUpstreamAccount("1001", "example2.com", "1001", "pass1001")
func (b *B2BUA) AddUpstreamAccount(localUser, upstreamHost, upstreamUser, password, transport string) error {
	if transport == "" {
		transport = "udp"
	}
	profile := account.NewProfile(
		mustParseUri("sip:"+upstreamUser+"@"+upstreamHost),
		"",
		&account.AuthInfo{
			AuthUser: upstreamUser,
			Password: password,
			Realm:    realmFromHost(upstreamHost),
		},
		3600,
		b.stack,
	)

	recipient := mustParseSipUri("sip:" + upstreamUser + "@" + upstreamHost + ";transport=" + transport)

	// Use a buffered channel as userdata so RegisterStateHandler can deliver
	// the first registration result back to us synchronously.
	resultCh := make(chan account.RegisterState, 1)
	reg, err := b.ua.SendRegister(profile, recipient, profile.Expires, resultCh)
	if err != nil {
		// Library-level error (e.g. could not build request).
		return fmt.Errorf("upstream register for %s@%s failed: %w", upstreamUser, upstreamHost, err)
	}

	// Block until the first SIP response (or network-level failure) arrives.
	state := <-resultCh
	if state.StatusCode != 200 {
		if reg != nil {
			reg.Stop()
		}
		return &sip.RequestError{
			Code:   uint(state.StatusCode),
			Reason: state.Reason,
		}
	}

	b.mu.Lock()
	b.upstreamAccounts[localUser] = &UpstreamAccount{
		LocalUser:    localUser,
		UpstreamHost: upstreamHost,
		UpstreamUser: upstreamUser,
		Password:     password,
		Transport:    transport,
		register:     reg,
	}
	b.mu.Unlock()
	logger.Infof("Upstream account registered: %s -> %s@%s", localUser, upstreamUser, upstreamHost)
	return nil
}

// upstreamForUser returns the upstream account for a locally-registered user, if any.
func (b *B2BUA) upstreamForUser(localUser string) (*UpstreamAccount, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	acc, ok := b.upstreamAccounts[localUser]
	return acc, ok
}

// GetRegistry .
func (b *B2BUA) GetRegistry() registry.Registry {
	return b.registry
}

// GetRFC8599 .
func (b *B2BUA) GetRFC8599() *registry.RFC8599 {
	return b.rfc8599
}

func (b *B2BUA) requestCredential(username string) (string, string, error) {
	b.mu.RLock()
	password, found := b.accounts[username]
	b.mu.RUnlock()
	if found {
		return password, "", nil
	}
	return "", "", fmt.Errorf("username [%s] not found", username)
}

// upstreamHeaderVal extracts the string value of a single custom header, e.g. "X-Upstream-Host".
func upstreamHeaderVal(request sip.Request, name string) string {
	hdrs := request.GetHeaders(name)
	if len(hdrs) == 0 {
		return ""
	}
	// GenericHeader stores its value in Value().
	return hdrs[0].Value()
}

func (b *B2BUA) handleRegister(request sip.Request, tx sip.ServerTransaction) {
	headers := request.GetHeaders("Expires")
	to, _ := request.To()
	aor := to.Address.Clone()
	localUser := aor.User().String()
	var expires sip.Expires = 0
	if len(headers) > 0 {
		expires = *headers[0].(*sip.Expires)
	}

	reason := ""
	if len(headers) > 0 && expires != sip.Expires(0) {
		upstreamHost := upstreamHeaderVal(request, "X-Upstream-Host")
		upstreamUser := upstreamHeaderVal(request, "X-Upstream-User")
		upstreamPass := upstreamHeaderVal(request, "X-Upstream-Password")
		upstreamTransport := upstreamHeaderVal(request, "X-Upstream-Transport")

		logger.Infof("REGISTER from %s: upstream headers: host=%q user=%q transport=%q hasPass=%v",
			localUser, upstreamHost, upstreamUser, upstreamTransport, upstreamPass != "")

		if upstreamHost != "" && upstreamUser != "" && upstreamPass != "" {
			// Upstream credentials present — register with the PBX first.
			// Only accept the device's REGISTER if the upstream succeeds.
			b.mu.Lock()
			existing, alreadyRegistered := b.upstreamAccounts[localUser]
			b.mu.Unlock()

			needsRegister := !alreadyRegistered ||
				existing.UpstreamHost != upstreamHost ||
				existing.UpstreamUser != upstreamUser ||
				existing.Password != upstreamPass ||
				existing.Transport != upstreamTransport

			if needsRegister {
				if alreadyRegistered && existing.register != nil {
					existing.register.SendRegister(0) //nolint:errcheck
				}
				if err := b.AddUpstreamAccount(localUser, upstreamHost, upstreamUser, upstreamPass, upstreamTransport); err != nil {
					logger.Errorf("Upstream registration failed for %s: %v", localUser, err)
					code, reason := uint(502), "Upstream Registration Failed"
					var reqErr *sip.RequestError
					if errors.As(err, &reqErr) {
						code, reason = reqErr.Code, reqErr.Reason
					}
					resp := sip.NewResponseFromRequest(request.MessageID(), request, sip.StatusCode(code), reason, "")
					tx.Respond(resp)
					return
				}
			}
		}

		instance := registry.NewContactInstanceForRequest(request)
		logger.Infof("Registered [%v] expires [%d] source %s", to, expires, request.Source())
		reason = "Registered"
		b.registry.AddAor(aor, instance)
		b.rfc8599.HandleContactInstance(aor, instance)
	} else {
		logger.Infof("Logged out [%v] expires [%d] ", to, expires)
		reason = "UnRegistered"
		instance := registry.NewContactInstanceForRequest(request)
		b.registry.RemoveContact(aor, instance)
		b.rfc8599.HandleContactInstance(aor, instance)

		// Device unregistered — tear down the upstream registration too.
		b.mu.Lock()
		acc, ok := b.upstreamAccounts[localUser]
		if ok {
			delete(b.upstreamAccounts, localUser)
		}
		b.mu.Unlock()
		if ok {
			if acc.register != nil {
				if err := acc.register.SendRegister(0); err != nil {
					logger.Warnf("Failed to unregister upstream %s@%s: %v", acc.UpstreamUser, acc.UpstreamHost, err)
				}
			}
		}
	}

	resp := sip.NewResponseFromRequest(request.MessageID(), request, 200, reason, "")
	sip.CopyHeaders("Expires", request, resp)
	utils.BuildContactHeader("Contact", request, resp, &expires)
	tx.Respond(resp)

}

func (b *B2BUA) handleConnectionError(connError *transport.ConnectionError) {
	logger.Debugf("Handle Connection Lost: Source: %v, Dest: %v, Network: %v", connError.Source, connError.Dest, connError.Net)
	b.registry.HandleConnectionError(connError)
}

func (b *B2BUA) SetLogLevel(level log.Level) {
	utils.SetLogLevel("B2BUA", level)
}
