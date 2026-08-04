package callmanager

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/AlchemillaHQ/Sentry/db"
	"github.com/AlchemillaHQ/Sentry/push"
	"github.com/AlchemillaHQ/Sentry/secrets"
	"github.com/AlchemillaHQ/Sentry/sipstack"
	"github.com/emiago/sipgo"
	"github.com/emiago/sipgo/sip"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/rs/zerolog/log"
)

var callTimeout = 30 * time.Second
var reInviteTimeout = 10 * time.Second
var refreshAckTimeout = 64 * sip.T1
var refreshRetransmitInterval = sip.T1
var sessionWatchdogGrace = 5 * time.Second
var errAmbiguousDevice = errors.New("multiple enabled devices match upstream user")

const maxRejectsBeforeBan = 10
const banDuration = 24 * time.Hour
const dialogAllowMethods = "INVITE, ACK, CANCEL, OPTIONS, BYE, REFER, NOTIFY, INFO, UPDATE"

type callLeg uint8

const (
	upstreamLeg callLeg = iota + 1
	downstreamLeg
)

func (leg callLeg) String() string {
	if leg == downstreamLeg {
		return "device"
	}
	return "pbx"
}

type dialogSession interface {
	Do(ctx context.Context, req *sip.Request) (*sip.Response, error)
	WriteRequest(req *sip.Request) error
	InviteResponse() *sip.Response
	InviteRequest() *sip.Request
}

type dialogAckWriter interface {
	WriteAck(ctx context.Context, ack *sip.Request) error
}

type serverSession interface {
	dialogSession
	Respond(statusCode int, reason string, body []byte, headers ...sip.Header) error
	Close() error
	Bye(ctx context.Context) error
	Context() context.Context
}

type clientSession interface {
	dialogSession
	WaitAnswer(ctx context.Context, opts sipgo.AnswerOptions) error
	Ack(ctx context.Context) error
	Bye(ctx context.Context) error
	Close() error
	Context() context.Context
}

type serverSessionWrapper struct {
	*sipgo.DialogServerSession
}

func (w *serverSessionWrapper) InviteResponse() *sip.Response {
	return w.DialogServerSession.Dialog.InviteResponse
}

func (w *serverSessionWrapper) InviteRequest() *sip.Request {
	return w.DialogServerSession.Dialog.InviteRequest
}

type clientSessionWrapper struct {
	*sipgo.DialogClientSession
}

func (w *clientSessionWrapper) InviteResponse() *sip.Response {
	return w.DialogClientSession.InviteResponse
}

func (w *clientSessionWrapper) InviteRequest() *sip.Request {
	return w.DialogClientSession.InviteRequest
}

type pendingCall struct {
	id               string
	deviceID         string
	sipUser          string
	callID           string
	callerURI        string
	callerName       string
	callerUser       string
	callerHost       string
	sdpOffer         []byte
	sdpContentType   sip.Header
	serverDlg        serverSession
	clientDlg        clientSession
	clientDlgMu      sync.Mutex
	bridgeMu         sync.Mutex
	refresh          *pendingRefresh
	refreshMu        sync.Mutex
	initialAck       *pendingInitialAck
	initialAckMu     sync.Mutex
	terminateCh      chan struct{}
	terminateOnce    sync.Once
	sessionRefreshCh chan time.Duration
	sessionWatchOnce sync.Once
	upstreamKey      string
	downstreamKey    string
	upstreamTxKey    string
	upstreamTarget   sip.Uri
	downstreamTarget sip.Uri
	readyCh          chan struct{}
	readyOnce        sync.Once
	ctx              context.Context
	cancel           context.CancelFunc
	sessionExpires   string
}

type pendingRefresh struct {
	callID          string
	dialogKey       string
	sourceLeg       callLeg
	cseq            uint32
	sessionInterval time.Duration
	ackCh           chan struct{}
	ackOnce         sync.Once
	downstreamAck   func(body []byte, contentType sip.Header) error
}

type pendingInitialAck struct {
	dialogKey string
	cseq      uint32
	complete  func(body []byte, contentType sip.Header) error
	once      sync.Once
	errMu     sync.Mutex
	err       error
}

type dialogBinding struct {
	pc  *pendingCall
	leg callLeg
}

type dialogServerReader interface {
	ReadAck(req *sip.Request, tx sip.ServerTransaction) error
	ReadBye(req *sip.Request, tx sip.ServerTransaction) error
	ReadInvite(req *sip.Request, tx sip.ServerTransaction) (serverSession, error)
}

type dialogClientFull interface {
	ReadBye(req *sip.Request, tx sip.ServerTransaction) error
	Invite(ctx context.Context, recipient sip.Uri, body []byte, from *sip.FromHeader, contentType sip.Header) (clientSession, error)
}

var (
	newDialogSrv = func(client *sipgo.Client, contactHdr sip.ContactHeader) dialogServerReader {
		cache := sipgo.NewDialogServerCache(client, contactHdr)
		return &dialogSrvAdapter{cache: cache}
	}
	newDialogCli = func(client *sipgo.Client, contactHdr sip.ContactHeader) dialogClientFull {
		cache := sipgo.NewDialogClientCache(client, contactHdr)
		return &dialogCliAdapter{cache: cache}
	}
)

type dialogSrvAdapter struct {
	cache *sipgo.DialogServerCache
}

func (a *dialogSrvAdapter) ReadAck(req *sip.Request, tx sip.ServerTransaction) error {
	return a.cache.ReadAck(req, tx)
}

func (a *dialogSrvAdapter) ReadBye(req *sip.Request, tx sip.ServerTransaction) error {
	return a.cache.ReadBye(req, tx)
}

func (a *dialogSrvAdapter) ReadInvite(req *sip.Request, tx sip.ServerTransaction) (serverSession, error) {
	session, err := a.cache.ReadInvite(req, tx)
	if err != nil {
		return nil, err
	}
	return &serverSessionWrapper{DialogServerSession: session}, nil
}

type dialogCliAdapter struct {
	cache *sipgo.DialogClientCache
}

func (a *dialogCliAdapter) ReadBye(req *sip.Request, tx sip.ServerTransaction) error {
	return a.cache.ReadBye(req, tx)
}

func (a *dialogCliAdapter) Invite(ctx context.Context, recipient sip.Uri, body []byte, from *sip.FromHeader, contentType sip.Header) (clientSession, error) {
	headers := []sip.Header{from}
	if len(body) > 0 && contentType != nil {
		headers = append(headers, contentType)
	}
	s, err := a.cache.Invite(ctx, recipient, body, headers...)
	if err != nil {
		return nil, err
	}
	return &clientSessionWrapper{s}, nil
}

type CallManager struct {
	dbQueries  db.Querier
	stack      *sipstack.Stack
	registrar  sipstack.Registrar
	pushSender push.Sender
	box        *secrets.Box

	mu           sync.RWMutex
	pending      map[string]*pendingCall
	dialogs      map[string]dialogBinding
	deviceSource map[string]sip.Uri
	suspended    map[string]struct{}

	dialogSrv dialogServerReader
	dialogCli dialogClientFull

	rejectThrottle   map[string]time.Time
	rejectThrottleMu sync.Mutex

	banlist    map[string]time.Time
	banlistMu  sync.Mutex
	failCounts map[string]int
	failMu     sync.Mutex
}

func New(database *db.Database, stack *sipstack.Stack, registrar sipstack.Registrar, pushSender push.Sender, box *secrets.Box) *CallManager {
	contactHdr := sip.ContactHeader{
		Address: sip.Uri{
			Host: stack.ExternalIP(),
			Port: stack.ExternalSIPPort(),
		},
	}
	if stack.ExternalSIPTransport() != "" && stack.ExternalSIPTransport() != "udp" {
		contactHdr.Address.UriParams = sip.NewParams()
		contactHdr.Address.UriParams.Add("transport", stack.ExternalSIPTransport())
	}

	cm := &CallManager{
		dbQueries:      database.Queries,
		stack:          stack,
		registrar:      registrar,
		pushSender:     pushSender,
		box:            box,
		pending:        make(map[string]*pendingCall),
		dialogs:        make(map[string]dialogBinding),
		deviceSource:   make(map[string]sip.Uri),
		suspended:      make(map[string]struct{}),
		rejectThrottle: make(map[string]time.Time),
		banlist:        make(map[string]time.Time),
		failCounts:     make(map[string]int),
		dialogSrv:      newDialogSrv(stack.Client(), contactHdr),
		dialogCli:      newDialogCli(stack.Client(), contactHdr),
	}

	stack.SetOnRegister(cm.handleRegister)
	stack.SetOnInvite(cm.handleInvite)
	stack.SetOnAck(cm.handleAck)
	stack.SetOnBye(cm.handleBye)
	stack.SetOnCancel(cm.handleCancel)
	stack.SetOnUpdate(cm.handleUpdate)
	stack.SetOnInfo(cm.handleInfo)
	stack.SetOnRefer(cm.handleRefer)
	stack.SetOnNotify(cm.handleNotify)

	go cm.cleanupRejectThrottle()

	return cm
}

func (cm *CallManager) matchDevice(ctx context.Context, sipUser string) (*db.Device, error) {
	device, err := cm.dbQueries.GetDeviceByB2BUASIPUser(ctx, sipUser)
	if err == nil {
		return &device, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return nil, err
	}

	devices, err := cm.dbQueries.GetDevicesByUpstreamUser(ctx, sipUser)
	if err != nil || len(devices) == 0 {
		return nil, pgx.ErrNoRows
	}
	if len(devices) > 1 {
		return nil, errAmbiguousDevice
	}
	return &devices[0], nil
}

func (cm *CallManager) matchInviteDevice(
	ctx context.Context,
	requestUser string,
	toUser string,
) (*db.Device, string, error) {
	// A registrar routes an INVITE to the per-device Contact in the Request-URI
	// while preserving the original extension in the To header. Never collapse a
	// distinct Contact back to the shared extension, as that can wake the wrong
	// device or make otherwise deterministic routing ambiguous.
	if requestUser != "" && requestUser != toUser {
		device, err := cm.dbQueries.GetDeviceByB2BUASIPUser(ctx, requestUser)
		if err != nil {
			return nil, requestUser, err
		}
		return &device, requestUser, nil
	}

	if toUser == "" {
		return nil, "", pgx.ErrNoRows
	}
	device, err := cm.matchDevice(ctx, toUser)
	return device, toUser, err
}

func (cm *CallManager) handleRegister(req *sip.Request, tx sip.ServerTransaction) {
	toHdr := req.To()
	if toHdr == nil {
		tx.Respond(sip.NewResponseFromRequest(req, 400, "Bad Request", nil))
		return
	}

	sipUser := toHdr.Address.User
	source := req.Source()

	if host, _, err := net.SplitHostPort(source); err == nil && cm.isBanned(host) {
		tx.Respond(sip.NewResponseFromRequest(req, 403, "Forbidden", nil))
		return
	}

	ctx := context.Background()
	device, err := cm.matchDevice(ctx, sipUser)
	if err != nil {
		if cm.allowRejectLog(sipUser) {
			log.Warn().Str("sip_user", sipUser).Str("source", source).Msg("REGISTER rejected: unknown user")
		}
		if host, _, err := net.SplitHostPort(source); err == nil {
			cm.recordReject(host)
		}
		tx.Respond(sip.NewResponseFromRequest(req, 403, "Forbidden", nil))
		return
	}

	deviceKey := device.B2buaSipUser
	cm.mu.RLock()
	_, suspended := cm.suspended[device.DeviceID]
	cm.mu.RUnlock()
	if suspended {
		log.Info().Str("device", device.DeviceID).Msg("REGISTER rejected: device is suspended")
		tx.Respond(sip.NewResponseFromRequest(req, 403, "Forbidden", nil))
		return
	}
	expires := requestedRegistrationExpires(req)
	if expires > 120 {
		expires = 120
	}
	res := sip.NewResponseFromRequest(req, sip.StatusOK, "OK", nil)
	expiresHdr := sip.ExpiresHeader(expires)
	res.AppendHeader(&expiresHdr)
	if contact := req.Contact(); contact != nil {
		responseContact := contact.Clone()
		if responseContact.Params == nil {
			responseContact.Params = sip.NewParams()
		}
		responseContact.Params.Add("expires", strconv.Itoa(int(expires)))
		res.AppendHeader(responseContact)
	}
	_ = tx.Respond(res)

	if host, _, err := net.SplitHostPort(source); err == nil {
		cm.clearFailures(host)
	}

	sourceURI, hasSource := registrationSource(req)
	if expires == 0 {
		removed := false
		cm.mu.Lock()
		if current, ok := cm.deviceSource[deviceKey]; ok && hasSource && sameRegistrationSource(current, sourceURI) {
			delete(cm.deviceSource, deviceKey)
			removed = true
		}
		cm.mu.Unlock()
		log.Info().
			Str("sip_user", sipUser).
			Str("device_id", device.DeviceID).
			Bool("source_removed", removed).
			Msg("shadow de-registration received")
		return
	}

	log.Info().Str("sip_user", sipUser).Str("device_id", device.DeviceID).Msg("REGISTER received")

	contact := req.Contact()
	userAgent := ""
	if uaHdr := req.GetHeader("User-Agent"); uaHdr != nil {
		userAgent = uaHdr.Value()
	}
	if contact != nil {
		cm.updateDeviceFromContact(ctx, device.B2buaSipUser, contact, userAgent)
	}

	cm.mu.Lock()
	defer cm.mu.Unlock()
	if _, suspended := cm.suspended[device.DeviceID]; suspended {
		log.Info().Str("device", device.DeviceID).Msg("REGISTER ignored after concurrent device suspension")
		return
	}
	if hasSource {
		cm.deviceSource[deviceKey] = sourceURI
	}
	for _, pc := range cm.pending {
		if pc.sipUser == deviceKey {
			pc.readyOnce.Do(func() {
				close(pc.readyCh)
			})
			cm.pushSender.CancelPush(pc.id)
		}
	}
}

func requestedRegistrationExpires(req *sip.Request) uint32 {
	const defaultExpires = uint32(120)
	if req == nil {
		return defaultExpires
	}
	if contact := req.Contact(); contact != nil {
		for _, params := range []sip.HeaderParams{contact.Params, contact.Address.UriParams} {
			if params == nil {
				continue
			}
			if value, ok := params.Get("expires"); ok {
				if parsed, err := strconv.ParseUint(strings.TrimSpace(value), 10, 32); err == nil {
					return uint32(parsed)
				}
			}
		}
	}
	if header := req.GetHeader("Expires"); header != nil {
		if parsed, err := strconv.ParseUint(strings.TrimSpace(header.Value()), 10, 32); err == nil {
			return uint32(parsed)
		}
	}
	return defaultExpires
}

func registrationSource(req *sip.Request) (sip.Uri, bool) {
	if req == nil || req.Source() == "" {
		return sip.Uri{}, false
	}
	host, portString, err := net.SplitHostPort(req.Source())
	if err != nil {
		return sip.Uri{}, false
	}
	port, err := strconv.Atoi(portString)
	if err != nil {
		return sip.Uri{}, false
	}
	source := sip.Uri{Host: host, Port: port}
	if transport := req.Transport(); transport != "" {
		source.UriParams = sip.NewParams()
		source.UriParams.Add("transport", transport)
	}
	return source, true
}

func sameRegistrationSource(left, right sip.Uri) bool {
	if !strings.EqualFold(strings.Trim(left.Host, "[]"), strings.Trim(right.Host, "[]")) || left.Port != right.Port {
		return false
	}
	leftTransport, _ := left.UriParams.Get("transport")
	rightTransport, _ := right.UriParams.Get("transport")
	return strings.EqualFold(leftTransport, rightTransport)
}

func (cm *CallManager) updateDeviceFromContact(ctx context.Context, sipUser string, contact *sip.ContactHeader, userAgent string) {
	device, err := cm.dbQueries.GetDeviceByB2BUASIPUser(ctx, sipUser)
	if err != nil {
		return
	}

	deviceContact := contact.Address.String()
	lastSeen := time.Now()

	pushProvider := device.PushProvider.String
	pushParam := device.PushParam.String
	pushPrid := device.PushPrid.String
	pushToken := device.PushToken

	if params := contact.Address.UriParams; params != nil {
		if provider, ok := params.Get("pn-provider"); ok && provider != "" {
			pushProvider = provider
		}
		if param, ok := params.Get("pn-param"); ok && param != "" {
			pushParam = param
		}
		if prid, ok := params.Get("pn-prid"); ok && prid != "" {
			encToken, err := cm.box.Encrypt([]byte(prid))
			if err == nil {
				pushToken = encToken
				pushPrid = prid
			}
		}
	}

	err = cm.dbQueries.UpsertDevice(ctx, db.UpsertDeviceParams{
		DeviceID:          device.DeviceID,
		Platform:          device.Platform,
		PushToken:         pushToken,
		UpstreamHost:      device.UpstreamHost,
		UpstreamPort:      device.UpstreamPort,
		UpstreamTransport: device.UpstreamTransport,
		UpstreamUser:      device.UpstreamUser,
		UpstreamPassword:  device.UpstreamPassword,
		UpstreamRealm:     device.UpstreamRealm,
		DisplayName:       device.DisplayName,
		B2buaSipUser:      device.B2buaSipUser,
		DeviceContact:     pgtype.Text{String: deviceContact, Valid: true},
		UserAgent:         pgtype.Text{String: userAgent, Valid: userAgent != ""},
		PushProvider:      pgtype.Text{String: pushProvider, Valid: pushProvider != ""},
		PushParam:         pgtype.Text{String: pushParam, Valid: pushParam != ""},
		PushPrid:          pgtype.Text{String: pushPrid, Valid: pushPrid != ""},
		ExpiresAt:         device.ExpiresAt,
		LastSeen:          pgtype.Timestamptz{Time: lastSeen, Valid: true},
	})
	if err != nil {
		log.Error().Err(err).Msg("failed to update device from contact")
	}
}

func (cm *CallManager) handleInvite(req *sip.Request, tx sip.ServerTransaction) {
	toHdr := req.To()
	if toHdr == nil {
		tx.Respond(sip.NewResponseFromRequest(req, 400, "Bad Request", nil))
		return
	}

	requestUser := req.Recipient.User
	toUser := toHdr.Address.User
	log.Info().
		Str("to", toHdr.String()).
		Str("request_uri", req.Recipient.String()).
		Str("call_id", requestCallID(req)).
		Msg("INVITE received")

	// A tagged INVITE belongs to an existing dialog. Passing it through
	// DialogServerCache.ReadInvite would create a second dialog with a new To
	// tag and replace the original cache entry. In addition to producing an
	// invalid offer/answer response, that leaves the bridged call detached from
	// its original dialog. Handle the refresh against the active bridge instead.
	if toHdr.Params.Has("tag") {
		cm.handleReInvite(req, tx)
		return
	}
	if headerHasOptionTag(req.GetHeader("Require"), "100rel") {
		res := sip.NewResponseFromRequest(req, sip.StatusBadExtension, "Bad Extension", nil)
		res.AppendHeader(sip.NewHeader("Unsupported", "100rel"))
		_ = tx.Respond(res)
		return
	}

	dlg, err := cm.dialogSrv.ReadInvite(req, tx)
	if err != nil {
		log.Error().Err(err).Msg("failed to read invite into dialog")
		tx.Respond(sip.NewResponseFromRequest(req, 500, "Server Error", nil))
		return
	}
	defer dlg.Close()

	dlg.Respond(sip.StatusTrying, "Trying", nil)

	ctx := context.Background()
	device, sipUser, err := cm.matchInviteDevice(ctx, requestUser, toUser)
	if err != nil {
		log.Warn().
			Err(err).
			Str("request_user", requestUser).
			Str("to_user", toUser).
			Str("routing_user", sipUser).
			Str("source", req.Source()).
			Msg("INVITE rejected: device match failed")
		_ = dlg.Respond(sip.StatusNotFound, "Not Found", nil)
		return
	}

	if device.Disabled {
		log.Info().Str("device", device.DeviceID).Msg("INVITE rejected: device is disabled")
		_ = dlg.Respond(sip.StatusTemporarilyUnavailable, "Temporarily Unavailable", nil)
		return
	}

	fromHdr := req.From()
	callerURI := ""
	callerName := ""
	callerUser := ""
	callerHost := ""
	if fromHdr != nil {
		callerURI = fromHdr.Address.String()
		callerName = fromHdr.DisplayName
		callerUser = fromHdr.Address.User
		callerHost = fromHdr.Address.Host
	}

	callID := uuid.New().String()
	callCtx, callCancel := context.WithCancel(context.Background())
	defer callCancel()

	timeoutCtx, timeoutCancel := context.WithTimeout(callCtx, callTimeout)
	defer timeoutCancel()

	pc := &pendingCall{
		id:               callID,
		deviceID:         device.DeviceID,
		sipUser:          device.B2buaSipUser,
		callID:           req.CallID().Value(),
		callerURI:        callerURI,
		callerName:       callerName,
		callerUser:       callerUser,
		callerHost:       callerHost,
		sdpOffer:         req.Body(),
		sdpContentType:   requestContentType(req),
		serverDlg:        dlg,
		readyCh:          make(chan struct{}),
		terminateCh:      make(chan struct{}),
		sessionRefreshCh: make(chan time.Duration, 1),
		ctx:              callCtx,
		cancel:           callCancel,
		sessionExpires:   normalizedSessionExpires(req),
	}
	if txKey, keyErr := sip.ServerTxKeyMake(req); keyErr == nil {
		pc.upstreamTxKey = txKey
	}

	cm.mu.Lock()
	if _, suspended := cm.suspended[device.DeviceID]; suspended {
		cm.mu.Unlock()
		log.Info().Str("device", device.DeviceID).Msg("INVITE rejected: device became suspended")
		dlg.Respond(sip.StatusTemporarilyUnavailable, "Temporarily Unavailable", nil)
		return
	}
	cm.pending[callID] = pc
	cm.mu.Unlock()
	defer cm.cleanup(callID)
	if !cm.bindDialog(pc, upstreamLeg, dlg) {
		log.Error().Str("call_id", callID).Msg("failed to index upstream SIP dialog")
		_ = dlg.Respond(sip.StatusInternalServerError, "Server Internal Error", nil)
		return
	}

	go func() {
		<-dlg.Context().Done()
		log.Info().Str("call_id", callID).Msg("PBX dialog ended")
		callCancel()
	}()

	err = cm.dbQueries.CreatePendingCall(ctx, db.CreatePendingCallParams{
		CallID:     callID,
		DeviceID:   device.DeviceID,
		SipCallID:  req.CallID().Value(),
		SipFrom:    callerURI,
		SipTo:      toHdr.Address.String(),
		SdpOffer:   pgtype.Text{String: string(req.Body()), Valid: len(req.Body()) > 0},
		CallerUri:  callerURI,
		CallerName: pgtype.Text{String: callerName, Valid: callerName != ""},
		State:      "PENDING_PUSH",
		ExpiresAt:  pgtype.Timestamptz{Time: time.Now().Add(callTimeout), Valid: true},
	})
	if err != nil {
		log.Error().Err(err).Msg("failed to create pending call in DB")
	}

	dlg.Respond(110, "Push sent", nil)

	pushTokenBytes, err := cm.box.Decrypt(device.PushToken)
	if err != nil {
		log.Error().Err(err).Str("device", device.DeviceID).Msg("failed to decrypt push token")
		dlg.Respond(sip.StatusTemporarilyUnavailable, "Temporarily Unavailable", nil)
		return
	}
	if callCtx.Err() != nil {
		log.Info().Str("call_id", callID).Str("device", device.DeviceID).Msg("call cancelled before push enqueue")
		return
	}

	log.Info().Str("call_id", callID).Str("device", device.DeviceID).Msg("sending push notification")
	if err := cm.pushSender.Send(context.Background(), push.CallPush{
		Platform:   device.Platform,
		Token:      string(pushTokenBytes),
		CallID:     callID,
		DeviceID:   device.DeviceID,
		CallerURI:  callerURI,
		CallerName: callerName,
	}); err != nil {
		log.Error().Err(err).Str("device", device.DeviceID).Msg("push notification failed")
		dlg.Respond(sip.StatusTemporarilyUnavailable, "Temporarily Unavailable", nil)
		return
	}

	cm.dbQueries.UpdatePendingCallState(ctx, db.UpdatePendingCallStateParams{
		CallID: callID,
		State:  "PUSH_SENT",
	})

	dlg.Respond(sip.StatusRinging, "Ringing", nil)

	log.Info().Str("call_id", callID).Str("device", device.DeviceID).Msg("push sent, waiting for device re-register")

	select {
	case <-pc.readyCh:
		log.Info().Str("call_id", callID).Msg("device ready, relaying call")
		cm.dbQueries.UpdatePendingCallState(ctx, db.UpdatePendingCallStateParams{
			CallID: callID,
			State:  "DEVICE_READY",
		})
		cm.relayCall(callCtx, pc, device)
		return
	case <-timeoutCtx.Done():
		log.Warn().Str("call_id", callID).Str("device", device.DeviceID).Msg("call timed out waiting for device")
		if callCtx.Err() != nil {
			return
		}
		dlg.Respond(sip.StatusTemporarilyUnavailable, "Temporarily Unavailable", nil)
		return
	case <-callCtx.Done():
		log.Info().Str("call_id", callID).Msg("call cancelled before device wake")
		return
	}
}

func (cm *CallManager) handleReInvite(req *sip.Request, tx sip.ServerTransaction) {
	callID := requestCallID(req)
	cseq := req.CSeq()
	if callID == "" || cseq == nil {
		_ = tx.Respond(sip.NewResponseFromRequest(req, sip.StatusBadRequest, "Bad Request", nil))
		return
	}

	binding, ok := cm.dialogForRequest(req)
	if !ok {
		log.Warn().Str("call_id", callID).Msg("re-INVITE rejected: active bridge not found")
		_ = tx.Respond(sip.NewResponseFromRequest(req, sip.StatusCallTransactionDoesNotExists, "Call/Transaction Does Not Exist", nil))
		return
	}
	pc := binding.pc

	_, target, targetLeg := dialogSessions(pc, binding.leg)
	if target == nil {
		log.Warn().Str("call_id", callID).Str("source", binding.leg.String()).Msg("re-INVITE deferred: opposite dialog is not ready")
		_ = tx.Respond(sip.NewResponseFromRequest(req, sip.StatusRequestPending, "Request Pending", nil))
		return
	}

	dialogKey, _ := requestDialogKey(req)
	refresh, ok := pc.beginRefresh(
		callID,
		dialogKey,
		binding.leg,
		cseq.SeqNo,
		sessionIntervalForRequest(req, pc.sessionExpires),
	)
	if !ok {
		log.Warn().Str("call_id", callID).Str("source", binding.leg.String()).Msg("re-INVITE rejected: another dialog refresh is in progress")
		_ = tx.Respond(sip.NewResponseFromRequest(req, sip.StatusRequestPending, "Request Pending", nil))
		return
	}
	waitingForAck := false
	defer func() {
		if !waitingForAck {
			pc.finishRefresh(refresh)
		}
	}()

	log.Info().
		Str("call_id", callID).
		Str("source", binding.leg.String()).
		Uint32("cseq", cseq.SeqNo).
		Int("sdp_len", len(req.Body())).
		Msg("bridging in-dialog re-INVITE")

	if err := tx.Respond(sip.NewResponseFromRequest(req, sip.StatusTrying, "Trying", nil)); err != nil {
		log.Error().Err(err).Str("call_id", callID).Msg("failed to acknowledge re-INVITE")
		return
	}

	ctx := pc.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	refreshCtx, cancel := context.WithTimeout(ctx, reInviteTimeout)
	defer cancel()

	var downstreamRes *sip.Response
	pc.bridgeMu.Lock()
	updateRemoteTargetFromRequest(pc, binding.leg, req)
	downstreamReq := newInDialogRequest(
		sip.INVITE,
		remoteTargetForLeg(pc, targetLeg, target),
		req.Body(),
		requestContentType(req),
	)
	downstreamRes, err := target.Do(refreshCtx, downstreamReq)
	if err != nil {
		pc.bridgeMu.Unlock()
		log.Error().Err(err).Str("call_id", callID).Str("target", targetLeg.String()).Msg("opposite-leg re-INVITE failed")
		_ = tx.Respond(sip.NewResponseFromRequest(req, sip.StatusGatewayTimeout, "Server Time-out", nil))
		return
	}
	if downstreamRes == nil {
		pc.bridgeMu.Unlock()
		log.Error().Str("call_id", callID).Str("target", targetLeg.String()).Msg("opposite-leg re-INVITE returned no response")
		_ = tx.Respond(sip.NewResponseFromRequest(req, sip.StatusGatewayTimeout, "Server Time-out", nil))
		return
	}

	if !downstreamRes.IsSuccess() {
		pc.bridgeMu.Unlock()
		res := cm.newDialogResponse(
			req,
			downstreamRes.StatusCode,
			downstreamRes.Reason,
			downstreamRes.Body(),
			responseContentType(downstreamRes),
			pc,
		)
		appendRelayedResponseHeaders(res, downstreamRes)
		if err := tx.Respond(res); err != nil {
			log.Error().Err(err).Str("call_id", callID).Msg("failed to relay rejected device re-INVITE")
		}
		return
	}
	updateRemoteTargetFromResponse(pc, targetLeg, downstreamRes)

	if len(downstreamRes.Body()) == 0 {
		// A successful response to either an SDP offer or an offerless INVITE
		// must contain SDP. ACK the downstream transaction so it does not linger,
		// but reject the invalid negotiation on the upstream leg.
		_ = writeDialogAck(target, remoteTargetForLeg(pc, targetLeg, target), nil, nil)
		pc.bridgeMu.Unlock()
		log.Error().Str("call_id", callID).Str("target", targetLeg.String()).Msg("opposite leg returned re-INVITE success without SDP")
		_ = tx.Respond(sip.NewResponseFromRequest(req, sip.StatusNotAcceptableHere, "Not Acceptable Here", nil))
		return
	}

	if len(req.Body()) > 0 {
		// The source leg supplied the offer, so the opposite response is the
		// answer and its ACK has no SDP body.
		if err := writeDialogAck(target, remoteTargetForLeg(pc, targetLeg, target), nil, nil); err != nil {
			pc.bridgeMu.Unlock()
			log.Error().Err(err).Str("call_id", callID).Str("target", targetLeg.String()).Msg("failed to ACK opposite-leg re-INVITE")
			_ = tx.Respond(sip.NewResponseFromRequest(req, sip.StatusGatewayTimeout, "Server Time-out", nil))
			return
		}
	} else {
		// For an offerless re-INVITE the opposite leg's 2xx carries the offer.
		// The source answer arrives in its ACK and must be forwarded across.
		pc.setRefreshDownstreamAck(refresh, func(body []byte, contentType sip.Header) error {
			pc.bridgeMu.Lock()
			defer pc.bridgeMu.Unlock()
			if len(body) == 0 {
				_ = writeDialogAck(target, remoteTargetForLeg(pc, targetLeg, target), nil, nil)
				return errors.New("source refresh ACK did not contain an SDP answer")
			}
			return writeDialogAck(target, remoteTargetForLeg(pc, targetLeg, target), body, contentType)
		})
	}
	pc.bridgeMu.Unlock()

	res := cm.newDialogResponse(
		req,
		downstreamRes.StatusCode,
		downstreamRes.Reason,
		downstreamRes.Body(),
		responseContentType(downstreamRes),
		pc,
	)
	appendRelayedResponseHeaders(res, downstreamRes)
	if err := tx.Respond(res); err != nil {
		log.Error().Err(err).Str("call_id", callID).Msg("failed to answer upstream re-INVITE")
		return
	}

	waitingForAck = true
	log.Info().
		Str("call_id", callID).
		Str("source", binding.leg.String()).
		Uint32("cseq", cseq.SeqNo).
		Int("sdp_len", len(downstreamRes.Body())).
		Msg("re-INVITE bridged; waiting for source ACK")

	// Keep the request handler alive until the ACK arrives. sipgo terminates
	// server transactions on reliable transports as soon as their handler
	// returns. Returning here and waiting in a detached goroutine would close
	// tx.Done(), discard the refresh state, and route the subsequent ACK into
	// the original INVITE dialog where its newer CSeq is rejected.
	cm.waitForRefreshAck(pc, refresh, tx, res)
}

func (cm *CallManager) handleUpdate(req *sip.Request, tx sip.ServerTransaction) {
	callID := requestCallID(req)
	cseq := req.CSeq()
	if callID == "" || cseq == nil {
		_ = tx.Respond(sip.NewResponseFromRequest(req, sip.StatusBadRequest, "Bad Request", nil))
		return
	}

	binding, ok := cm.dialogForRequest(req)
	if !ok {
		log.Warn().Str("call_id", callID).Msg("UPDATE rejected: active bridge not found")
		_ = tx.Respond(sip.NewResponseFromRequest(req, sip.StatusCallTransactionDoesNotExists, "Call/Transaction Does Not Exist", nil))
		return
	}
	pc := binding.pc

	_, target, targetLeg := dialogSessions(pc, binding.leg)
	if target == nil {
		_ = tx.Respond(sip.NewResponseFromRequest(req, sip.StatusRequestPending, "Request Pending", nil))
		return
	}

	// An UPDATE without SDP only refreshes the upstream B2BUA leg. The device
	// leg has an independent dialog and does not need another transaction.
	if len(req.Body()) == 0 {
		pc.bridgeMu.Lock()
		updateRemoteTargetFromRequest(pc, binding.leg, req)
		pc.bridgeMu.Unlock()
		if binding.leg == upstreamLeg {
			pc.touchSession(sessionIntervalForRequest(req, pc.sessionExpires))
		}
		res := cm.newDialogResponse(req, sip.StatusOK, "OK", nil, nil, pc)
		if err := tx.Respond(res); err != nil {
			log.Error().Err(err).Str("call_id", callID).Msg("failed to answer session refresh UPDATE")
			return
		}
		log.Info().Str("call_id", callID).Str("source", binding.leg.String()).Uint32("cseq", cseq.SeqNo).Msg("session refresh UPDATE accepted")
		return
	}

	dialogKey, _ := requestDialogKey(req)
	refresh, ok := pc.beginRefresh(callID, dialogKey, binding.leg, cseq.SeqNo, 0)
	if !ok {
		_ = tx.Respond(sip.NewResponseFromRequest(req, sip.StatusRequestPending, "Request Pending", nil))
		return
	}
	defer pc.finishRefresh(refresh)

	ctx := pc.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	updateCtx, cancel := context.WithTimeout(ctx, reInviteTimeout)
	defer cancel()

	pc.bridgeMu.Lock()
	updateRemoteTargetFromRequest(pc, binding.leg, req)
	downstreamReq := newInDialogRequest(
		sip.UPDATE,
		remoteTargetForLeg(pc, targetLeg, target),
		req.Body(),
		requestContentType(req),
	)
	downstreamRes, err := target.Do(updateCtx, downstreamReq)
	if err != nil {
		pc.bridgeMu.Unlock()
		log.Error().Err(err).Str("call_id", callID).Str("target", targetLeg.String()).Msg("opposite-leg UPDATE failed")
		_ = tx.Respond(sip.NewResponseFromRequest(req, sip.StatusGatewayTimeout, "Server Time-out", nil))
		return
	}
	if downstreamRes == nil {
		pc.bridgeMu.Unlock()
		log.Error().Str("call_id", callID).Str("target", targetLeg.String()).Msg("opposite-leg UPDATE returned no response")
		_ = tx.Respond(sip.NewResponseFromRequest(req, sip.StatusGatewayTimeout, "Server Time-out", nil))
		return
	}
	if downstreamRes.IsSuccess() && len(downstreamRes.Body()) == 0 {
		pc.bridgeMu.Unlock()
		log.Error().Str("call_id", callID).Str("target", targetLeg.String()).Msg("opposite leg returned UPDATE success without an SDP answer")
		_ = tx.Respond(sip.NewResponseFromRequest(req, sip.StatusNotAcceptableHere, "Not Acceptable Here", nil))
		return
	}
	if downstreamRes.IsSuccess() {
		updateRemoteTargetFromResponse(pc, targetLeg, downstreamRes)
	}
	pc.bridgeMu.Unlock()
	if downstreamRes.IsSuccess() && binding.leg == upstreamLeg {
		pc.touchSession(sessionIntervalForRequest(req, pc.sessionExpires))
	}

	res := cm.newDialogResponse(
		req,
		downstreamRes.StatusCode,
		downstreamRes.Reason,
		downstreamRes.Body(),
		responseContentType(downstreamRes),
		pc,
	)
	appendRelayedResponseHeaders(res, downstreamRes)
	if err := tx.Respond(res); err != nil {
		log.Error().Err(err).Str("call_id", callID).Msg("failed to relay device UPDATE response")
		return
	}
	log.Info().
		Str("call_id", callID).
		Str("source", binding.leg.String()).
		Uint32("cseq", cseq.SeqNo).
		Int("sdp_len", len(downstreamRes.Body())).
		Msg("UPDATE bridged")
}

func (cm *CallManager) handleInfo(req *sip.Request, tx sip.ServerTransaction) {
	cm.bridgeInDialogRequest(
		req,
		tx,
		"Info-Package",
		"Content-Disposition",
		"Recv-Info",
	)
}

func (cm *CallManager) handleRefer(req *sip.Request, tx sip.ServerTransaction) {
	cm.bridgeInDialogRequest(
		req,
		tx,
		"Refer-To",
		"Referred-By",
		"Refer-Sub",
		"Target-Dialog",
		"Replaces",
	)
}

func (cm *CallManager) handleNotify(req *sip.Request, tx sip.ServerTransaction) {
	if _, ok := cm.dialogForRequest(req); !ok {
		// Upstream gateways can send unsolicited MWI/keepalive NOTIFY traffic.
		// It is unrelated to a bridged call, but acknowledging it avoids a
		// retransmission storm.
		_ = tx.Respond(sip.NewResponseFromRequest(req, sip.StatusOK, "OK", nil))
		return
	}
	cm.bridgeInDialogRequest(
		req,
		tx,
		"Event",
		"Subscription-State",
		"Content-Disposition",
	)
}

func (cm *CallManager) bridgeInDialogRequest(
	req *sip.Request,
	tx sip.ServerTransaction,
	forwardHeaders ...string,
) {
	callID := requestCallID(req)
	binding, ok := cm.dialogForRequest(req)
	if !ok {
		log.Warn().Str("call_id", callID).Str("method", req.Method.String()).Msg("in-dialog request rejected: active bridge not found")
		_ = tx.Respond(sip.NewResponseFromRequest(req, sip.StatusCallTransactionDoesNotExists, "Call/Transaction Does Not Exist", nil))
		return
	}
	pc := binding.pc
	_, target, targetLeg := dialogSessions(pc, binding.leg)
	if target == nil {
		_ = tx.Respond(sip.NewResponseFromRequest(req, sip.StatusRequestPending, "Request Pending", nil))
		return
	}

	ctx := pc.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	requestCtx, cancel := context.WithTimeout(ctx, reInviteTimeout)
	defer cancel()

	pc.bridgeMu.Lock()
	updateRemoteTargetFromRequest(pc, binding.leg, req)
	targetReq := newInDialogRequest(
		req.Method,
		remoteTargetForLeg(pc, targetLeg, target),
		req.Body(),
		requestContentType(req),
	)
	for _, name := range forwardHeaders {
		for _, header := range req.GetHeaders(name) {
			targetReq.AppendHeader(sip.HeaderClone(header))
		}
	}
	targetRes, err := target.Do(requestCtx, targetReq)
	if err != nil {
		pc.bridgeMu.Unlock()
		log.Error().Err(err).Str("call_id", callID).Str("method", req.Method.String()).Str("target", targetLeg.String()).Msg("in-dialog request relay failed")
		_ = tx.Respond(sip.NewResponseFromRequest(req, sip.StatusGatewayTimeout, "Server Time-out", nil))
		return
	}
	if targetRes == nil {
		pc.bridgeMu.Unlock()
		_ = tx.Respond(sip.NewResponseFromRequest(req, sip.StatusGatewayTimeout, "Server Time-out", nil))
		return
	}
	if targetRes.IsSuccess() {
		updateRemoteTargetFromResponse(pc, targetLeg, targetRes)
	}
	pc.bridgeMu.Unlock()

	res := cm.newDialogResponse(
		req,
		targetRes.StatusCode,
		targetRes.Reason,
		targetRes.Body(),
		responseContentType(targetRes),
		pc,
	)
	for _, header := range responseHeaders(
		targetRes,
		"Content-Disposition",
		"Warning",
		"Reason",
		"Retry-After",
		"Unsupported",
		"Allow",
		"Accept",
		"Refer-Sub",
	) {
		res.AppendHeader(header)
	}
	if err := tx.Respond(res); err != nil {
		log.Error().Err(err).Str("call_id", callID).Str("method", req.Method.String()).Msg("failed to relay in-dialog response")
	}
}

func activeClientDialog(pc *pendingCall) clientSession {
	pc.clientDlgMu.Lock()
	defer pc.clientDlgMu.Unlock()
	return pc.clientDlg
}

func dialogSessions(pc *pendingCall, sourceLeg callLeg) (dialogSession, dialogSession, callLeg) {
	client := activeClientDialog(pc)
	if sourceLeg == upstreamLeg {
		return pc.serverDlg, client, downstreamLeg
	}
	return client, pc.serverDlg, upstreamLeg
}

func requestDialogKey(req *sip.Request) (string, bool) {
	if req == nil {
		return "", false
	}
	return dialogKey(req.CallID(), req.From(), req.To())
}

func responseDialogKey(res *sip.Response) (string, bool) {
	if res == nil {
		return "", false
	}
	return dialogKey(res.CallID(), res.From(), res.To())
}

func dialogKey(callID *sip.CallIDHeader, from *sip.FromHeader, to *sip.ToHeader) (string, bool) {
	if callID == nil || from == nil || to == nil || from.Params == nil || to.Params == nil {
		return "", false
	}
	fromTag, fromOK := from.Params.Get("tag")
	toTag, toOK := to.Params.Get("tag")
	if !fromOK || !toOK || fromTag == "" || toTag == "" {
		return "", false
	}
	// The same dialog arrives with From and To reversed depending on which
	// endpoint sent the request. Sort the tags so one key identifies either
	// direction without weakening the match to Call-ID alone.
	if fromTag > toTag {
		fromTag, toTag = toTag, fromTag
	}
	return strings.Join([]string{callID.Value(), fromTag, toTag}, "\x00"), true
}

func sessionDialogKey(dlg dialogSession, leg callLeg) (string, bool) {
	if dlg == nil {
		return "", false
	}
	if leg == downstreamLeg {
		return responseDialogKey(dlg.InviteResponse())
	}
	return requestDialogKey(dlg.InviteRequest())
}

func (cm *CallManager) bindDialog(pc *pendingCall, leg callLeg, dlg dialogSession) bool {
	key, ok := sessionDialogKey(dlg, leg)
	if !ok {
		return false
	}

	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.dialogs == nil {
		cm.dialogs = make(map[string]dialogBinding)
	}
	if existing, exists := cm.dialogs[key]; exists && existing.pc != pc {
		return false
	}
	cm.dialogs[key] = dialogBinding{pc: pc, leg: leg}
	if leg == upstreamLeg {
		pc.upstreamKey = key
		if contact := dlg.InviteRequest().Contact(); contact != nil {
			pc.upstreamTarget = *contact.Address.Clone()
		}
	} else {
		pc.downstreamKey = key
		if contact := dlg.InviteResponse().Contact(); contact != nil {
			pc.downstreamTarget = *contact.Address.Clone()
		}
	}
	return true
}

func (cm *CallManager) dialogForKey(key string) (dialogBinding, bool) {
	cm.mu.RLock()
	binding, ok := cm.dialogs[key]
	cm.mu.RUnlock()
	return binding, ok
}

func (cm *CallManager) dialogForRequest(req *sip.Request) (dialogBinding, bool) {
	key, ok := requestDialogKey(req)
	if !ok {
		return dialogBinding{}, false
	}
	return cm.dialogForKey(key)
}

func remoteTargetForLeg(pc *pendingCall, leg callLeg, dlg dialogSession) sip.Uri {
	if leg == upstreamLeg && pc.upstreamTarget.Host != "" {
		return *pc.upstreamTarget.Clone()
	}
	if leg == downstreamLeg && pc.downstreamTarget.Host != "" {
		return *pc.downstreamTarget.Clone()
	}
	if dlg != nil {
		if leg == upstreamLeg {
			if invite := dlg.InviteRequest(); invite != nil {
				if contact := invite.Contact(); contact != nil {
					return *contact.Address.Clone()
				}
			}
		} else if response := dlg.InviteResponse(); response != nil {
			if contact := response.Contact(); contact != nil {
				return *contact.Address.Clone()
			}
		}
	}
	return sip.Uri{}
}

func updateRemoteTargetFromRequest(pc *pendingCall, leg callLeg, req *sip.Request) {
	if req == nil || req.Contact() == nil {
		return
	}
	target := *req.Contact().Address.Clone()
	if leg == upstreamLeg {
		pc.upstreamTarget = target
	} else {
		pc.downstreamTarget = target
	}
}

func updateRemoteTargetFromResponse(pc *pendingCall, leg callLeg, res *sip.Response) {
	if res == nil || res.Contact() == nil {
		return
	}
	target := *res.Contact().Address.Clone()
	if leg == upstreamLeg {
		pc.upstreamTarget = target
	} else {
		pc.downstreamTarget = target
	}
}

func (pc *pendingCall) beginRefresh(
	callID string,
	dialogKey string,
	sourceLeg callLeg,
	cseq uint32,
	sessionInterval time.Duration,
) (*pendingRefresh, bool) {
	pc.refreshMu.Lock()
	defer pc.refreshMu.Unlock()
	if pc.refresh != nil {
		return nil, false
	}
	refresh := &pendingRefresh{
		callID:          callID,
		dialogKey:       dialogKey,
		sourceLeg:       sourceLeg,
		cseq:            cseq,
		sessionInterval: sessionInterval,
		ackCh:           make(chan struct{}),
	}
	pc.refresh = refresh
	return refresh, true
}

func (pc *pendingCall) finishRefresh(refresh *pendingRefresh) {
	pc.refreshMu.Lock()
	if pc.refresh == refresh {
		pc.refresh = nil
	}
	pc.refreshMu.Unlock()
}

func (pc *pendingCall) setRefreshDownstreamAck(
	refresh *pendingRefresh,
	ack func(body []byte, contentType sip.Header) error,
) {
	pc.refreshMu.Lock()
	if pc.refresh == refresh {
		refresh.downstreamAck = ack
	}
	pc.refreshMu.Unlock()
}

func (pc *pendingCall) beginInitialAck(
	dialogKey string,
	cseq uint32,
	complete func(body []byte, contentType sip.Header) error,
) *pendingInitialAck {
	ack := &pendingInitialAck{dialogKey: dialogKey, cseq: cseq, complete: complete}
	pc.initialAckMu.Lock()
	pc.initialAck = ack
	pc.initialAckMu.Unlock()
	return ack
}

func (pc *pendingCall) clearInitialAck(ack *pendingInitialAck) {
	pc.initialAckMu.Lock()
	if pc.initialAck == ack {
		pc.initialAck = nil
	}
	pc.initialAckMu.Unlock()
}

func (ack *pendingInitialAck) setError(err error) {
	ack.errMu.Lock()
	ack.err = err
	ack.errMu.Unlock()
}

func (ack *pendingInitialAck) error() error {
	ack.errMu.Lock()
	defer ack.errMu.Unlock()
	return ack.err
}

func (pc *pendingCall) requestTermination() {
	if pc.terminateCh == nil {
		if pc.cancel != nil {
			pc.cancel()
		}
		return
	}
	pc.terminateOnce.Do(func() {
		close(pc.terminateCh)
	})
}

func (pc *pendingCall) touchSession(interval time.Duration) {
	if interval <= 0 || pc.sessionRefreshCh == nil {
		return
	}
	select {
	case <-pc.sessionRefreshCh:
	default:
	}
	select {
	case pc.sessionRefreshCh <- interval:
	default:
	}
}

func (pc *pendingCall) startSessionWatchdog() {
	interval := sessionInterval(pc.sessionExpires)
	if interval <= 0 || pc.sessionRefreshCh == nil {
		return
	}
	ctx := pc.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	pc.sessionWatchOnce.Do(func() {
		go func() {
			timer := time.NewTimer(sessionWatchdogDuration(interval))
			defer timer.Stop()
			for {
				select {
				case next := <-pc.sessionRefreshCh:
					if next <= 0 {
						next = interval
					}
					interval = next
					if !timer.Stop() {
						select {
						case <-timer.C:
						default:
						}
					}
					timer.Reset(sessionWatchdogDuration(interval))
				case <-timer.C:
					log.Warn().Str("call_id", pc.id).Dur("session_interval", interval).Msg("SIP session timer expired; terminating bridged call")
					pc.requestTermination()
					return
				case <-ctx.Done():
					return
				}
			}
		}()
	})
}

func (cm *CallManager) waitForRefreshAck(
	pc *pendingCall,
	refresh *pendingRefresh,
	tx sip.ServerTransaction,
	res *sip.Response,
) {
	defer pc.finishRefresh(refresh)

	interval := refreshRetransmitInterval
	if interval <= 0 {
		interval = sip.T1
	}
	retransmit := time.NewTimer(interval)
	deadline := time.NewTimer(refreshAckTimeout)
	defer retransmit.Stop()
	defer deadline.Stop()

	ctx := pc.ctx
	if ctx == nil {
		ctx = context.Background()
	}

	for {
		select {
		case <-refresh.ackCh:
			log.Info().Str("call_id", refresh.callID).Str("source", refresh.sourceLeg.String()).Uint32("cseq", refresh.cseq).Msg("re-INVITE ACK received")
			return
		case <-ctx.Done():
			return
		case <-tx.Done():
			select {
			case <-refresh.ackCh:
				return
			default:
			}
			log.Warn().Str("call_id", refresh.callID).Str("source", refresh.sourceLeg.String()).Uint32("cseq", refresh.cseq).Msg("re-INVITE transaction ended before ACK")
			pc.requestTermination()
			return
		case <-deadline.C:
			log.Warn().Str("call_id", refresh.callID).Str("source", refresh.sourceLeg.String()).Uint32("cseq", refresh.cseq).Msg("timed out waiting for re-INVITE ACK")
			pc.requestTermination()
			return
		case <-retransmit.C:
			if err := tx.Respond(res.Clone()); err != nil {
				log.Warn().Err(err).Str("call_id", refresh.callID).Str("source", refresh.sourceLeg.String()).Uint32("cseq", refresh.cseq).Msg("failed to retransmit re-INVITE response")
				select {
				case <-refresh.ackCh:
					return
				default:
					pc.requestTermination()
				}
				return
			}
			interval = min(2*interval, sip.T2)
			retransmit.Reset(interval)
		}
	}
}

func (cm *CallManager) handleRefreshAck(req *sip.Request) bool {
	cseq := req.CSeq()
	dialogKey, ok := requestDialogKey(req)
	if !ok || cseq == nil {
		return false
	}

	binding, ok := cm.dialogForKey(dialogKey)
	if !ok {
		return false
	}
	pc := binding.pc
	pc.refreshMu.Lock()
	refresh := pc.refresh
	matched := refresh != nil &&
		refresh.dialogKey == dialogKey &&
		refresh.sourceLeg == binding.leg &&
		refresh.cseq == cseq.SeqNo
	var downstreamAck func(body []byte, contentType sip.Header) error
	var sessionInterval time.Duration
	if matched {
		downstreamAck = refresh.downstreamAck
		sessionInterval = refresh.sessionInterval
	}
	pc.refreshMu.Unlock()
	if !matched {
		return false
	}

	var ackErr error
	processed := false
	refresh.ackOnce.Do(func() {
		processed = true
		if downstreamAck != nil {
			ackErr = downstreamAck(req.Body(), requestContentType(req))
		}
		close(refresh.ackCh)
	})
	if ackErr != nil {
		log.Error().Err(ackErr).Str("call_id", requestCallID(req)).Str("source", binding.leg.String()).Uint32("cseq", cseq.SeqNo).Msg("failed to complete offerless re-INVITE")
		pc.requestTermination()
	} else if processed && binding.leg == upstreamLeg {
		pc.touchSession(sessionInterval)
	}
	return true
}

func (cm *CallManager) handleInitialAck(req *sip.Request, tx sip.ServerTransaction) bool {
	cseq := req.CSeq()
	dialogKey, ok := requestDialogKey(req)
	if !ok || cseq == nil {
		return false
	}
	binding, ok := cm.dialogForKey(dialogKey)
	if !ok || binding.leg != upstreamLeg {
		return false
	}
	pc := binding.pc
	pc.initialAckMu.Lock()
	ack := pc.initialAck
	matched := ack != nil && ack.dialogKey == dialogKey && ack.cseq == cseq.SeqNo
	pc.initialAckMu.Unlock()
	if !matched {
		return false
	}

	processed := false
	ack.once.Do(func() {
		processed = true
		if err := ack.complete(req.Body(), requestContentType(req)); err != nil {
			ack.setError(err)
		}
		if err := cm.dialogSrv.ReadAck(req, tx); err != nil {
			ack.setError(err)
			log.Error().Err(err).Str("call_id", requestCallID(req)).Msg("failed to read initial ACK into dialog")
		} else {
			log.Info().Str("call_id", requestCallID(req)).Msg("initial offerless ACK received and relayed")
		}
	})
	if processed && ack.error() != nil {
		pc.requestTermination()
	}
	return true
}

func newInDialogRequest(method sip.RequestMethod, remoteTarget sip.Uri, body []byte, contentType sip.Header) *sip.Request {
	req := sip.NewRequest(method, remoteTarget)
	if len(body) > 0 {
		req.SetBody(append([]byte(nil), body...))
		if contentType == nil {
			contentType = sip.NewHeader("Content-Type", "application/sdp")
		}
		req.AppendHeader(sip.NewHeader("Content-Type", contentType.Value()))
	}
	return req
}

func writeDialogAck(dlg dialogSession, remoteTarget sip.Uri, body []byte, contentType sip.Header) error {
	return dlg.WriteRequest(newDialogAck(remoteTarget, body, contentType))
}

func writeInitialDialogAck(dlg clientSession, remoteTarget sip.Uri, body []byte, contentType sip.Header) error {
	ack := newDialogAck(remoteTarget, body, contentType)
	if writer, ok := dlg.(dialogAckWriter); ok {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return writer.WriteAck(ctx, ack)
	}
	return dlg.WriteRequest(ack)
}

func newDialogAck(remoteTarget sip.Uri, body []byte, contentType sip.Header) *sip.Request {
	ack := sip.NewRequest(sip.ACK, remoteTarget)
	if len(body) > 0 {
		ack.SetBody(append([]byte(nil), body...))
		if contentType == nil {
			contentType = sip.NewHeader("Content-Type", "application/sdp")
		}
		ack.AppendHeader(sip.NewHeader("Content-Type", contentType.Value()))
	}
	return ack
}

func requestCallID(req *sip.Request) string {
	if req == nil || req.CallID() == nil {
		return ""
	}
	return req.CallID().Value()
}

func requestContentType(req *sip.Request) sip.Header {
	if req == nil {
		return nil
	}
	if header := req.GetHeader("Content-Type"); header != nil {
		return sip.NewHeader("Content-Type", header.Value())
	}
	return nil
}

func responseContentType(res *sip.Response) sip.Header {
	if res == nil {
		return nil
	}
	if header := res.GetHeader("Content-Type"); header != nil {
		return sip.NewHeader("Content-Type", header.Value())
	}
	return nil
}

func headerHasOptionTag(header sip.Header, option string) bool {
	if header == nil {
		return false
	}
	for _, value := range strings.Split(header.Value(), ",") {
		if strings.EqualFold(strings.TrimSpace(value), option) {
			return true
		}
	}
	return false
}

func normalizedSessionExpires(req *sip.Request) string {
	if req == nil {
		return ""
	}
	header := req.GetHeader("Session-Expires")
	if header == nil {
		return ""
	}
	parts := strings.Split(header.Value(), ";")
	if len(parts) == 0 || strings.TrimSpace(parts[0]) == "" {
		return ""
	}

	normalized := []string{strings.TrimSpace(parts[0])}
	for _, part := range parts[1:] {
		part = strings.TrimSpace(part)
		if part == "" || strings.HasPrefix(strings.ToLower(part), "refresher=") {
			continue
		}
		normalized = append(normalized, part)
	}
	// Sentry terminates the upstream timer leg, so keep the PBX (the UAC for
	// this transaction) responsible for sending refreshes.
	normalized = append(normalized, "refresher=uac")
	return strings.Join(normalized, ";")
}

func sessionInterval(value string) time.Duration {
	secondsString := strings.TrimSpace(strings.SplitN(value, ";", 2)[0])
	if secondsString == "" {
		return 0
	}
	seconds, err := strconv.ParseInt(secondsString, 10, 32)
	if err != nil || seconds <= 0 {
		return 0
	}
	return time.Duration(seconds) * time.Second
}

func sessionIntervalForRequest(req *sip.Request, fallback string) time.Duration {
	if req != nil {
		if header := req.GetHeader("Session-Expires"); header != nil {
			if interval := sessionInterval(header.Value()); interval > 0 {
				return interval
			}
		}
	}
	return sessionInterval(fallback)
}

func sessionWatchdogDuration(interval time.Duration) time.Duration {
	grace := max(sessionWatchdogGrace, interval/10)
	return interval + grace
}

func (cm *CallManager) newDialogResponse(
	req *sip.Request,
	statusCode int,
	reason string,
	body []byte,
	contentType sip.Header,
	pc *pendingCall,
) *sip.Response {
	res := sip.NewResponseFromRequest(req, statusCode, reason, body)
	if len(body) > 0 {
		if contentType == nil {
			contentType = sip.NewHeader("Content-Type", "application/sdp")
		}
		res.AppendHeader(sip.NewHeader("Content-Type", contentType.Value()))
	}
	if statusCode < 200 || statusCode >= 300 {
		return res
	}
	if req.Method != sip.INVITE && req.Method != sip.UPDATE {
		return res
	}

	if contact := cm.upstreamContactHeader(pc.sipUser); contact != nil {
		res.AppendHeader(contact)
	}
	res.AppendHeader(sip.NewHeader("Allow", dialogAllowMethods))
	res.AppendHeader(sip.NewHeader("Supported", "timer"))
	sessionExpires := normalizedSessionExpires(req)
	if sessionExpires == "" {
		sessionExpires = pc.sessionExpires
	}
	if sessionExpires != "" {
		res.AppendHeader(sip.NewHeader("Session-Expires", sessionExpires))
	}
	return res
}

func (cm *CallManager) upstreamContactHeader(sipUser string) *sip.ContactHeader {
	if cm.stack == nil {
		return nil
	}
	contact := &sip.ContactHeader{
		Address: sip.Uri{
			User: sipUser,
			Host: cm.stack.ExternalIP(),
			Port: cm.stack.ExternalSIPPort(),
		},
	}
	if transport := cm.stack.ExternalSIPTransport(); transport != "" && transport != "udp" {
		contact.Address.UriParams = sip.NewParams()
		contact.Address.UriParams.Add("transport", transport)
	}
	return contact
}

func responseHeaders(res *sip.Response, names ...string) []sip.Header {
	if res == nil {
		return nil
	}
	headers := make([]sip.Header, 0, len(names))
	for _, name := range names {
		for _, header := range res.GetHeaders(name) {
			headers = append(headers, sip.HeaderClone(header))
		}
	}
	return headers
}

func appendRelayedResponseHeaders(dst, src *sip.Response) {
	if dst == nil || src == nil {
		return
	}
	for _, name := range []string{
		"Content-Disposition",
		"Warning",
		"Reason",
		"Retry-After",
		"Unsupported",
		"Allow",
		"Accept",
	} {
		if dst.GetHeader(name) != nil {
			continue
		}
		for _, header := range src.GetHeaders(name) {
			dst.AppendHeader(sip.HeaderClone(header))
		}
	}
}

func initialResponseHeaders(res *sip.Response) []sip.Header {
	return responseHeaders(
		res,
		"Content-Type",
		"Content-Disposition",
		"Warning",
		"Reason",
		"Retry-After",
		"Unsupported",
		"Allow",
		"Accept",
	)
}

func (cm *CallManager) initialSuccessHeaders(pc *pendingCall, res *sip.Response) []sip.Header {
	headers := responseHeaders(res, "Content-Type", "Content-Disposition")
	if len(res.Body()) > 0 && res.GetHeader("Content-Type") == nil {
		headers = append(headers, sip.NewHeader("Content-Type", "application/sdp"))
	}
	headers = append(headers,
		sip.NewHeader("Allow", dialogAllowMethods),
		sip.NewHeader("Supported", "timer"),
	)
	if contact := cm.upstreamContactHeader(pc.sipUser); contact != nil {
		headers = append(headers, contact)
	}
	if pc.sessionExpires != "" {
		headers = append(headers, sip.NewHeader("Session-Expires", pc.sessionExpires))
	}
	return headers
}

func sendDialogBye(dlg interface{ Bye(context.Context) error }) {
	if dlg == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := dlg.Bye(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Warn().Err(err).Msg("failed to terminate SIP dialog")
	}
}

func (cm *CallManager) relayCall(ctx context.Context, pc *pendingCall, device *db.Device) {
	cm.mu.RLock()
	srcUri, ok := cm.deviceSource[pc.sipUser]
	cm.mu.RUnlock()

	defer func() {
		cm.dbQueries.UpdatePendingCallState(context.Background(), db.UpdatePendingCallStateParams{
			CallID: pc.id,
			State:  "TERMINATED",
		})
		log.Info().Str("call_id", pc.id).Msg("call terminated (fail-safe)")
	}()

	if !ok {
		log.Error().Str("call_id", pc.id).Str("sip_user", pc.sipUser).Msg("no device source stored")
		pc.serverDlg.Respond(sip.StatusTemporarilyUnavailable, "Temporarily Unavailable", nil)
		pc.serverDlg.Close()
		return
	}

	recipient := sip.Uri{
		User: device.B2buaSipUser,
		Host: srcUri.Host,
		Port: srcUri.Port,
	}
	if srcUri.UriParams != nil {
		recipient.UriParams = srcUri.UriParams
	}

	log.Info().Str("call_id", pc.id).Str("device", device.DeviceID).Str("recipient", recipient.String()).Msg("relaying call to device")

	fromAddr := sip.Uri{User: pc.callerUser, Host: pc.callerHost}
	if fromAddr.User == "" || fromAddr.Host == "" {
		fromAddr = sip.Uri{User: "caller", Host: cm.stack.ExternalIP()}
	}
	fromHdr := &sip.FromHeader{
		DisplayName: pc.callerName,
		Address:     fromAddr,
	}
	fromHdr.Params = sip.NewParams()
	fromHdr.Params.Add("tag", sip.GenerateTagN(16))

	contentType := pc.sdpContentType
	if len(pc.sdpOffer) > 0 && contentType == nil {
		contentType = sip.NewHeader("Content-Type", "application/sdp")
	}

	dlgClient, err := cm.dialogCli.Invite(ctx, recipient, pc.sdpOffer, fromHdr, contentType)
	if err != nil {
		log.Error().Err(err).Str("call_id", pc.id).Msg("failed to send relay INVITE to device")
		pc.serverDlg.Respond(sip.StatusTemporarilyUnavailable, "Temporarily Unavailable", nil)
		pc.serverDlg.Close()
		return
	}
	defer func() {
		dlgClient.Close()
		pc.clientDlgMu.Lock()
		pc.clientDlg = nil
		pc.clientDlgMu.Unlock()
	}()

	pc.clientDlgMu.Lock()
	pc.clientDlg = dlgClient
	pc.clientDlgMu.Unlock()

	answerCtx, answerCancel := context.WithTimeout(ctx, callTimeout)
	defer answerCancel()
	err = dlgClient.WaitAnswer(answerCtx, sipgo.AnswerOptions{OnResponse: func(res *sip.Response) error {
		if res == nil || !res.IsProvisional() || res.StatusCode == sip.StatusTrying {
			return nil
		}
		// Sentry terminates reliable provisional-response negotiation and does
		// not advertise 100rel. Forward useful ringing/early-media responses
		// without Require/RSeq so the two dialog legs remain independent.
		return pc.serverDlg.Respond(
			res.StatusCode,
			res.Reason,
			append([]byte(nil), res.Body()...),
			initialResponseHeaders(res)...,
		)
	}})
	if err != nil {
		log.Error().Err(err).Str("call_id", pc.id).Msg("device did not answer relay INVITE")
		if ctx.Err() != nil || pc.ctx.Err() != nil {
			return
		}
		if answerCtx.Err() != nil {
			_ = pc.serverDlg.Respond(sip.StatusTemporarilyUnavailable, "Temporarily Unavailable", nil)
			return
		}
		var responseErr *sipgo.ErrDialogResponse
		if errors.As(err, &responseErr) && responseErr.Res != nil {
			res := responseErr.Res
			_ = pc.serverDlg.Respond(
				res.StatusCode,
				res.Reason,
				append([]byte(nil), res.Body()...),
				initialResponseHeaders(res)...,
			)
		} else {
			_ = pc.serverDlg.Respond(sip.StatusTemporarilyUnavailable, "Temporarily Unavailable", nil)
		}
		return
	}

	if ctx.Err() != nil || pc.ctx.Err() != nil {
		log.Info().Str("call_id", pc.id).Msg("call cancelled while waiting for device answer, terminating relay leg")
		sendDialogBye(dlgClient)
		return
	}

	inviteResponse := dlgClient.InviteResponse()
	if inviteResponse == nil {
		log.Error().Str("call_id", pc.id).Msg("no invite response from device")
		pc.serverDlg.Respond(sip.StatusTemporarilyUnavailable, "Temporarily Unavailable", nil)
		pc.serverDlg.Close()
		return
	}
	log.Info().Str("call_id", pc.id).Int("status_code", int(inviteResponse.StatusCode)).Int("sdp_len", len(inviteResponse.Body())).Msg("device answered")

	if !cm.bindDialog(pc, downstreamLeg, dlgClient) {
		log.Error().Str("call_id", pc.id).Msg("failed to index device SIP dialog")
		_ = dlgClient.Ack(ctx)
		sendDialogBye(dlgClient)
		_ = pc.serverDlg.Respond(sip.StatusInternalServerError, "Server Internal Error", nil)
		return
	}

	deviceSDP := inviteResponse.Body()
	if len(deviceSDP) == 0 {
		log.Error().Str("call_id", pc.id).Msg("device accepted INVITE without an SDP offer or answer")
		_ = dlgClient.Ack(ctx)
		sendDialogBye(dlgClient)
		_ = pc.serverDlg.Respond(sip.StatusNotAcceptableHere, "Not Acceptable Here", nil)
		return
	}

	log.Info().Str("call_id", pc.id).Int("sdp_len", len(deviceSDP)).Msg("sending 200 OK to PBX")

	var initialAck *pendingInitialAck
	if len(pc.sdpOffer) > 0 {
		if err := dlgClient.Ack(ctx); err != nil {
			log.Error().Err(err).Str("call_id", pc.id).Msg("failed to ACK device")
			_ = pc.serverDlg.Respond(sip.StatusTemporarilyUnavailable, "Temporarily Unavailable", nil)
			return
		}
	} else {
		invite := pc.serverDlg.InviteRequest()
		if invite == nil || invite.CSeq() == nil || pc.upstreamKey == "" {
			_ = writeInitialDialogAck(dlgClient, remoteTargetForLeg(pc, downstreamLeg, dlgClient), nil, nil)
			sendDialogBye(dlgClient)
			_ = pc.serverDlg.Respond(sip.StatusInternalServerError, "Server Internal Error", nil)
			return
		}
		initialAck = pc.beginInitialAck(pc.upstreamKey, invite.CSeq().SeqNo, func(body []byte, contentType sip.Header) error {
			pc.bridgeMu.Lock()
			defer pc.bridgeMu.Unlock()
			if len(body) == 0 {
				_ = writeInitialDialogAck(dlgClient, remoteTargetForLeg(pc, downstreamLeg, dlgClient), nil, nil)
				return errors.New("PBX ACK did not contain the SDP answer for an offerless INVITE")
			}
			return writeInitialDialogAck(
				dlgClient,
				remoteTargetForLeg(pc, downstreamLeg, dlgClient),
				body,
				contentType,
			)
		})
		defer pc.clearInitialAck(initialAck)
	}

	if err := pc.serverDlg.Respond(
		sip.StatusOK,
		"OK",
		deviceSDP,
		cm.initialSuccessHeaders(pc, inviteResponse)...,
	); err != nil {
		log.Error().Err(err).Str("call_id", pc.id).Msg("failed to send 200 OK to PBX")
		if initialAck != nil {
			_ = writeInitialDialogAck(dlgClient, remoteTargetForLeg(pc, downstreamLeg, dlgClient), nil, nil)
		}
		sendDialogBye(dlgClient)
		return
	}
	if initialAck != nil {
		if ackErr := initialAck.error(); ackErr != nil {
			log.Error().Err(ackErr).Str("call_id", pc.id).Msg("offerless initial INVITE negotiation failed")
			sendDialogBye(dlgClient)
			sendDialogBye(pc.serverDlg)
			return
		}
	}

	cm.dbQueries.UpdatePendingCallState(context.Background(), db.UpdatePendingCallStateParams{
		CallID: pc.id,
		State:  "BRIDGED",
	})
	log.Info().Str("call_id", pc.id).Msg("call bridged")
	pc.startSessionWatchdog()

	select {
	case <-pc.terminateCh:
		log.Warn().Str("call_id", pc.id).Msg("call termination requested by dialog fail-safe")
		sendDialogBye(pc.serverDlg)
		sendDialogBye(dlgClient)
	case <-dlgClient.Context().Done():
		log.Info().Str("call_id", pc.id).Msg("device ended call, sending BYE to PBX")
		sendDialogBye(pc.serverDlg)
	case <-pc.ctx.Done():
		log.Info().Str("call_id", pc.id).Msg("PBX ended call, sending BYE to device")
		sendDialogBye(dlgClient)
	}

	log.Info().Str("call_id", pc.id).Msg("call finished")
}

func (cm *CallManager) handleAck(req *sip.Request, tx sip.ServerTransaction) {
	if cm.handleRefreshAck(req) {
		return
	}
	if cm.handleInitialAck(req, tx) {
		return
	}
	err := cm.dialogSrv.ReadAck(req, tx)
	if err != nil {
		log.Error().Err(err).Str("call_id", requestCallID(req)).Msg("failed to read ACK into dialog")
	} else {
		log.Info().Str("call_id", requestCallID(req)).Msg("ACK received and processed")
	}
}

func (cm *CallManager) handleBye(req *sip.Request, tx sip.ServerTransaction) {
	binding, ok := cm.dialogForRequest(req)
	if !ok {
		log.Warn().Str("call_id", requestCallID(req)).Msg("BYE received for unknown dialog")
		_ = tx.Respond(sip.NewResponseFromRequest(req, sip.StatusCallTransactionDoesNotExists, "Call/Transaction Does Not Exist", nil))
		return
	}

	var err error
	if binding.leg == upstreamLeg {
		err = cm.dialogSrv.ReadBye(req, tx)
		if err == nil {
			log.Info().Str("call_id", requestCallID(req)).Msg("BYE received from PBX")
			if binding.pc.cancel != nil {
				binding.pc.cancel()
			}
		}
	} else {
		err = cm.dialogCli.ReadBye(req, tx)
		if err == nil {
			log.Info().Str("call_id", requestCallID(req)).Msg("BYE received from device")
		}
	}
	if err != nil {
		log.Warn().Err(err).Str("call_id", requestCallID(req)).Str("source", binding.leg.String()).Msg("failed to process BYE")
		_ = tx.Respond(sip.NewResponseFromRequest(req, sip.StatusCallTransactionDoesNotExists, "Call/Transaction Does Not Exist", nil))
	}
}

func (cm *CallManager) handleCancel(req *sip.Request, tx sip.ServerTransaction) {
	callIDVal := requestCallID(req)
	cancelKey := inviteTransactionKey(req)

	cm.mu.RLock()
	var found *pendingCall
	for _, pc := range cm.pending {
		if cancelKey != "" && pc.upstreamTxKey == cancelKey {
			found = pc
			break
		}
	}
	if found == nil && cancelKey == "" {
		for _, pc := range cm.pending {
			if pc.callID == callIDVal {
				found = pc
				break
			}
		}
	}
	cm.mu.RUnlock()

	if found == nil {
		log.Warn().Str("sip_call_id", callIDVal).Msg("CANCEL for unknown call")
		_ = tx.Respond(sip.NewResponseFromRequest(req, sip.StatusCallTransactionDoesNotExists, "Call/Transaction Does Not Exist", nil))
		return
	}

	log.Info().Str("call_id", found.id).Str("sip_call_id", callIDVal).Msg("call cancelled by PBX")
	cm.pushSender.CancelPush(found.id)
	if found.cancel != nil {
		found.cancel()
	}
	_ = cm.dbQueries.UpdatePendingCallState(context.Background(), db.UpdatePendingCallStateParams{
		CallID: found.id,
		State:  "CANCELLED",
	})
	_ = tx.Respond(sip.NewResponseFromRequest(req, sip.StatusOK, "OK", nil))
}

func inviteTransactionKey(req *sip.Request) string {
	if req == nil || req.CSeq() == nil {
		return ""
	}
	clone := req.Clone()
	clone.CSeq().MethodName = sip.INVITE
	key, err := sip.ServerTxKeyMake(clone)
	if err != nil {
		return ""
	}
	return key
}

func (cm *CallManager) GetPendingCallsCount() int {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	return len(cm.pending)
}

func (cm *CallManager) cleanup(callID string) {
	cm.mu.Lock()
	pc := cm.pending[callID]
	delete(cm.pending, callID)
	if pc != nil {
		if pc.upstreamKey != "" {
			delete(cm.dialogs, pc.upstreamKey)
		}
		if pc.downstreamKey != "" {
			delete(cm.dialogs, pc.downstreamKey)
		}
	}
	cm.mu.Unlock()
	cm.pushSender.CancelPush(callID)
}

func (cm *CallManager) RemoveDeviceSource(sipUser string) {
	cm.mu.Lock()
	delete(cm.deviceSource, sipUser)
	cm.mu.Unlock()
}

func (cm *CallManager) SuspendDevice(deviceID, sipUser string) {
	cm.mu.Lock()
	if cm.suspended == nil {
		cm.suspended = make(map[string]struct{})
	}
	cm.suspended[deviceID] = struct{}{}
	delete(cm.deviceSource, sipUser)
	pending := make([]*pendingCall, 0)
	for _, pc := range cm.pending {
		if pc.deviceID != deviceID {
			continue
		}
		pc.clientDlgMu.Lock()
		relaying := pc.clientDlg != nil
		pc.clientDlgMu.Unlock()
		if !relaying {
			pending = append(pending, pc)
		}
	}
	cm.mu.Unlock()

	for _, pc := range pending {
		cm.pushSender.CancelPush(pc.id)
		if pc.serverDlg != nil {
			_ = pc.serverDlg.Respond(sip.StatusTemporarilyUnavailable, "Temporarily Unavailable", nil)
		}
		if pc.cancel != nil {
			pc.cancel()
		}
		_ = cm.dbQueries.UpdatePendingCallState(context.Background(), db.UpdatePendingCallStateParams{
			CallID: pc.id,
			State:  "DEVICE_DISABLED",
		})
	}

	log.Info().
		Str("device", deviceID).
		Int("pending_calls_cancelled", len(pending)).
		Msg("device suspended in call manager")
}

func (cm *CallManager) ResumeDevice(deviceID string) {
	cm.mu.Lock()
	delete(cm.suspended, deviceID)
	cm.mu.Unlock()
	log.Info().Str("device", deviceID).Msg("device resumed in call manager")
}

func (cm *CallManager) ForgetDevice(deviceID, sipUser string) {
	cm.mu.Lock()
	delete(cm.suspended, deviceID)
	delete(cm.deviceSource, sipUser)
	cm.mu.Unlock()
	log.Info().Str("device", deviceID).Msg("device removed from call manager")
}

func (cm *CallManager) SendByeToAllBridgedCalls(ctx context.Context) {
	cm.mu.RLock()
	pendingCalls := make([]*pendingCall, 0, len(cm.pending))
	for _, pc := range cm.pending {
		pendingCalls = append(pendingCalls, pc)
	}
	cm.mu.RUnlock()

	for _, pc := range pendingCalls {
		pc.cancel()
		byeCtx, byeCancel := context.WithTimeout(ctx, 5*time.Second)
		pc.serverDlg.Bye(byeCtx)
		byeCancel()

		pc.clientDlgMu.Lock()
		if pc.clientDlg != nil {
			clientByeCtx, clientByeCancel := context.WithTimeout(ctx, 5*time.Second)
			pc.clientDlg.Bye(clientByeCtx)
			clientByeCancel()
		}
		pc.clientDlgMu.Unlock()
	}
	log.Info().Int("count", len(pendingCalls)).Msg("sent BYE to all bridged calls")
}

func (cm *CallManager) allowRejectLog(sipUser string) bool {
	cm.rejectThrottleMu.Lock()
	defer cm.rejectThrottleMu.Unlock()

	now := time.Now()
	if last, ok := cm.rejectThrottle[sipUser]; ok {
		if now.Sub(last) < 30*time.Second {
			return false
		}
	}
	cm.rejectThrottle[sipUser] = now
	return true
}

func (cm *CallManager) cleanupRejectThrottle() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	for range ticker.C {
		cm.pruneStaleState()
	}
}

func (cm *CallManager) pruneStaleState() {
	now := time.Now()

	cm.rejectThrottleMu.Lock()
	cutoff := now.Add(-5 * time.Minute)
	for k, v := range cm.rejectThrottle {
		if v.Before(cutoff) {
			delete(cm.rejectThrottle, k)
		}
	}
	cm.rejectThrottleMu.Unlock()

	cm.banlistMu.Lock()
	for k, v := range cm.banlist {
		if now.After(v) {
			delete(cm.banlist, k)
		}
	}
	cm.banlistMu.Unlock()

	cm.failMu.Lock()
	for k, v := range cm.failCounts {
		if v >= maxRejectsBeforeBan {
			delete(cm.failCounts, k)
		}
	}
	cm.failMu.Unlock()
}

func (cm *CallManager) isBanned(host string) bool {
	cm.banlistMu.Lock()
	expiry, ok := cm.banlist[host]
	cm.banlistMu.Unlock()
	if !ok {
		return false
	}
	if time.Now().After(expiry) {
		cm.banlistMu.Lock()
		delete(cm.banlist, host)
		cm.banlistMu.Unlock()
		return false
	}
	return true
}

func (cm *CallManager) recordReject(host string) {
	if isPrivateIP(host) {
		return
	}

	cm.failMu.Lock()
	cm.failCounts[host]++
	count := cm.failCounts[host]
	cm.failMu.Unlock()

	if count >= maxRejectsBeforeBan {
		cm.banlistMu.Lock()
		if _, exists := cm.banlist[host]; !exists {
			cm.banlist[host] = time.Now().Add(banDuration)
			log.Warn().Str("host", host).Dur("duration", banDuration).Msg("IP banned due to repeated rejected REGISTERs")
		}
		cm.banlistMu.Unlock()
	}
}

func isPrivateIP(host string) bool {
	host = strings.Trim(host, "[]")
	addr, err := netip.ParseAddr(host)
	if err != nil {
		return false
	}
	return addr.IsLoopback() || addr.IsPrivate() || addr.IsLinkLocalUnicast()
}

func (cm *CallManager) clearFailures(host string) {
	cm.failMu.Lock()
	delete(cm.failCounts, host)
	cm.failMu.Unlock()
}
