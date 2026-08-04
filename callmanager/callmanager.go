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
var errAmbiguousDevice = errors.New("multiple enabled devices match upstream user")

const maxRejectsBeforeBan = 10
const banDuration = 24 * time.Hour
const dialogAllowMethods = "INVITE, ACK, CANCEL, OPTIONS, BYE, REFER, NOTIFY, MESSAGE, SUBSCRIBE, INFO, UPDATE"

type serverSession interface {
	Respond(statusCode int, reason string, body []byte, headers ...sip.Header) error
	Close() error
	Bye(ctx context.Context) error
	Context() context.Context
}

type clientSession interface {
	WaitAnswer(ctx context.Context, opts sipgo.AnswerOptions) error
	Ack(ctx context.Context) error
	Bye(ctx context.Context) error
	Close() error
	Context() context.Context
	InviteResponse() *sip.Response
	InviteRequest() *sip.Request
	Do(ctx context.Context, req *sip.Request) (*sip.Response, error)
	WriteRequest(req *sip.Request) error
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
	id             string
	deviceID       string
	sipUser        string
	callID         string
	callerURI      string
	callerName     string
	callerUser     string
	callerHost     string
	sdpOffer       []byte
	serverDlg      serverSession
	clientDlg      clientSession
	clientDlgMu    sync.Mutex
	refresh        *pendingRefresh
	refreshMu      sync.Mutex
	readyCh        chan struct{}
	readyOnce      sync.Once
	ctx            context.Context
	cancel         context.CancelFunc
	sessionExpires string
}

type pendingRefresh struct {
	cseq          uint32
	ackCh         chan struct{}
	ackOnce       sync.Once
	downstreamAck func(body []byte, contentType sip.Header) error
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
	return a.cache.ReadInvite(req, tx)
}

type dialogCliAdapter struct {
	cache *sipgo.DialogClientCache
}

func (a *dialogCliAdapter) ReadBye(req *sip.Request, tx sip.ServerTransaction) error {
	return a.cache.ReadBye(req, tx)
}

func (a *dialogCliAdapter) Invite(ctx context.Context, recipient sip.Uri, body []byte, from *sip.FromHeader, contentType sip.Header) (clientSession, error) {
	s, err := a.cache.Invite(ctx, recipient, body, from, contentType)
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
	log.Info().Str("sip_user", sipUser).Str("device_id", device.DeviceID).Msg("REGISTER received")

	res := sip.NewResponseFromRequest(req, 200, "OK", nil)
	expiresHdr := sip.ExpiresHeader(120)
	res.AppendHeader(&expiresHdr)
	tx.Respond(res)

	if host, _, err := net.SplitHostPort(source); err == nil {
		cm.clearFailures(host)
	}

	transport := req.Transport()
	var sourceURI sip.Uri
	hasSource := false
	if source != "" {
		host, portStr, err := net.SplitHostPort(source)
		if err == nil {
			port, _ := strconv.Atoi(portStr)
			sourceURI = sip.Uri{
				Host: host,
				Port: port,
			}
			if transport != "" {
				sourceURI.UriParams = sip.NewParams()
				sourceURI.UriParams.Add("transport", transport)
			}
			hasSource = true
		}
	}

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
		Str("call_id", req.CallID().Value()).
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

	dlg, err := cm.dialogSrv.ReadInvite(req, tx)
	if err != nil {
		log.Error().Err(err).Msg("failed to read invite into dialog")
		tx.Respond(sip.NewResponseFromRequest(req, 500, "Server Error", nil))
		return
	}

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
		tx.Respond(sip.NewResponseFromRequest(req, 404, "Not Found", nil))
		return
	}

	if device.Disabled {
		log.Info().Str("device", device.DeviceID).Msg("INVITE rejected: device is disabled")
		tx.Respond(sip.NewResponseFromRequest(req, 480, "Temporarily Unavailable", nil))
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
		id:             callID,
		deviceID:       device.DeviceID,
		sipUser:        device.B2buaSipUser,
		callID:         req.CallID().Value(),
		callerURI:      callerURI,
		callerName:     callerName,
		callerUser:     callerUser,
		callerHost:     callerHost,
		sdpOffer:       req.Body(),
		serverDlg:      dlg,
		readyCh:        make(chan struct{}),
		ctx:            callCtx,
		cancel:         callCancel,
		sessionExpires: normalizedSessionExpires(req),
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

	go func() {
		<-dlg.Context().Done()
		log.Info().Str("call_id", callID).Msg("PBX dialog ended, propagating to device")

		select {
		case <-callCtx.Done():
			return
		default:
		}

		pc.clientDlgMu.Lock()
		d := pc.clientDlg
		pc.clientDlgMu.Unlock()
		if d == nil {
			callCancel()
			return
		}

		ir := d.InviteResponse()
		if ir != nil && ir.StatusCode == 200 {
			byeCtx, byeCancel := context.WithTimeout(context.Background(), 5*time.Second)
			d.Bye(byeCtx)
			byeCancel()
			log.Info().Str("call_id", callID).Msg("BYE sent to device")
		} else if ir == nil || ir.IsProvisional() {
			if irq := d.InviteRequest(); irq != nil {
				creq := sip.NewRequest(sip.CANCEL, irq.Recipient)
				creq.AppendHeader(sip.HeaderClone(irq.Via()))
				creq.AppendHeader(sip.HeaderClone(irq.From()))
				creq.AppendHeader(sip.HeaderClone(irq.To()))
				creq.AppendHeader(sip.HeaderClone(irq.CallID()))
				cseq := irq.CSeq()
				creq.AppendHeader(&sip.CSeqHeader{SeqNo: cseq.SeqNo, MethodName: sip.CANCEL})
				sip.CopyHeaders("Route", irq, creq)
				creq.SetSource(irq.Source())
				cctx, ccancel := context.WithTimeout(context.Background(), 5*time.Second)
				resp, err := cm.stack.Client().Do(cctx, creq)
				ccancel()
				if err != nil {
					log.Error().Err(err).Str("call_id", callID).Msg("failed to send CANCEL to device")
				} else if resp.StatusCode != 200 {
					log.Warn().Str("call_id", callID).Int("status", int(resp.StatusCode)).Msg("CANCEL got non-200")
				} else {
					log.Info().Str("call_id", callID).Msg("CANCEL sent to device")
				}
			}
		}
		callCancel()
	}()

	defer cm.cleanup(callID)

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

	pc := cm.pendingBySIPCallID(callID)
	if pc == nil {
		log.Warn().Str("call_id", callID).Msg("re-INVITE rejected: active bridge not found")
		_ = tx.Respond(sip.NewResponseFromRequest(req, sip.StatusCallTransactionDoesNotExists, "Call/Transaction Does Not Exist", nil))
		return
	}

	dlg := activeClientDialog(pc)
	if dlg == nil {
		log.Warn().Str("call_id", callID).Msg("re-INVITE deferred: device dialog is not ready")
		_ = tx.Respond(sip.NewResponseFromRequest(req, sip.StatusRequestPending, "Request Pending", nil))
		return
	}

	refresh, ok := pc.beginRefresh(cseq.SeqNo)
	if !ok {
		log.Warn().Str("call_id", callID).Msg("re-INVITE rejected: another dialog refresh is in progress")
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
		Uint32("cseq", cseq.SeqNo).
		Int("sdp_len", len(req.Body())).
		Msg("bridging in-dialog re-INVITE to device")

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

	downstreamReq := newInDialogRequest(sip.INVITE, dlg, req.Body(), requestContentType(req))
	downstreamRes, err := dlg.Do(refreshCtx, downstreamReq)
	if err != nil {
		log.Error().Err(err).Str("call_id", callID).Msg("device re-INVITE failed")
		_ = tx.Respond(sip.NewResponseFromRequest(req, sip.StatusGatewayTimeout, "Server Time-out", nil))
		return
	}
	if downstreamRes == nil {
		log.Error().Str("call_id", callID).Msg("device re-INVITE returned no response")
		_ = tx.Respond(sip.NewResponseFromRequest(req, sip.StatusGatewayTimeout, "Server Time-out", nil))
		return
	}

	if !downstreamRes.IsSuccess() {
		res := cm.newUpstreamDialogResponse(
			req,
			downstreamRes.StatusCode,
			downstreamRes.Reason,
			downstreamRes.Body(),
			responseContentType(downstreamRes),
			pc,
		)
		if err := tx.Respond(res); err != nil {
			log.Error().Err(err).Str("call_id", callID).Msg("failed to relay rejected device re-INVITE")
		}
		return
	}

	if len(downstreamRes.Body()) == 0 {
		// A successful response to either an SDP offer or an offerless INVITE
		// must contain SDP. ACK the downstream transaction so it does not linger,
		// but reject the invalid negotiation on the upstream leg.
		_ = writeDialogAck(dlg, downstreamRes, nil, nil)
		log.Error().Str("call_id", callID).Msg("device returned re-INVITE success without SDP")
		_ = tx.Respond(sip.NewResponseFromRequest(req, sip.StatusNotAcceptableHere, "Not Acceptable Here", nil))
		return
	}

	if len(req.Body()) > 0 {
		// The PBX supplied the offer, so the device response is the answer and
		// the downstream ACK has no SDP body.
		if err := writeDialogAck(dlg, downstreamRes, nil, nil); err != nil {
			log.Error().Err(err).Str("call_id", callID).Msg("failed to ACK device re-INVITE")
			_ = tx.Respond(sip.NewResponseFromRequest(req, sip.StatusGatewayTimeout, "Server Time-out", nil))
			return
		}
	} else {
		// For an offerless re-INVITE the device's 2xx carries the offer. The
		// PBX answer arrives in its ACK and must be forwarded in the device ACK.
		pc.setRefreshDownstreamAck(refresh, func(body []byte, contentType sip.Header) error {
			if len(body) == 0 {
				_ = writeDialogAck(dlg, downstreamRes, nil, nil)
				return errors.New("PBX refresh ACK did not contain an SDP answer")
			}
			return writeDialogAck(dlg, downstreamRes, body, contentType)
		})
	}

	res := cm.newUpstreamDialogResponse(
		req,
		downstreamRes.StatusCode,
		downstreamRes.Reason,
		downstreamRes.Body(),
		responseContentType(downstreamRes),
		pc,
	)
	if err := tx.Respond(res); err != nil {
		log.Error().Err(err).Str("call_id", callID).Msg("failed to answer upstream re-INVITE")
		return
	}

	waitingForAck = true
	go cm.waitForRefreshAck(pc, refresh, tx, res)
	log.Info().
		Str("call_id", callID).
		Uint32("cseq", cseq.SeqNo).
		Int("sdp_len", len(downstreamRes.Body())).
		Msg("re-INVITE bridged; waiting for PBX ACK")
}

func (cm *CallManager) handleUpdate(req *sip.Request, tx sip.ServerTransaction) {
	callID := requestCallID(req)
	cseq := req.CSeq()
	if callID == "" || cseq == nil {
		_ = tx.Respond(sip.NewResponseFromRequest(req, sip.StatusBadRequest, "Bad Request", nil))
		return
	}

	pc := cm.pendingBySIPCallID(callID)
	if pc == nil {
		log.Warn().Str("call_id", callID).Msg("UPDATE rejected: active bridge not found")
		_ = tx.Respond(sip.NewResponseFromRequest(req, sip.StatusCallTransactionDoesNotExists, "Call/Transaction Does Not Exist", nil))
		return
	}

	dlg := activeClientDialog(pc)
	if dlg == nil {
		_ = tx.Respond(sip.NewResponseFromRequest(req, sip.StatusRequestPending, "Request Pending", nil))
		return
	}

	// An UPDATE without SDP only refreshes the upstream B2BUA leg. The device
	// leg has an independent dialog and does not need another transaction.
	if len(req.Body()) == 0 {
		res := cm.newUpstreamDialogResponse(req, sip.StatusOK, "OK", nil, nil, pc)
		if err := tx.Respond(res); err != nil {
			log.Error().Err(err).Str("call_id", callID).Msg("failed to answer session refresh UPDATE")
			return
		}
		log.Info().Str("call_id", callID).Uint32("cseq", cseq.SeqNo).Msg("session refresh UPDATE accepted")
		return
	}

	refresh, ok := pc.beginRefresh(cseq.SeqNo)
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

	downstreamReq := newInDialogRequest(sip.UPDATE, dlg, req.Body(), requestContentType(req))
	downstreamRes, err := dlg.Do(updateCtx, downstreamReq)
	if err != nil {
		log.Error().Err(err).Str("call_id", callID).Msg("device UPDATE failed")
		_ = tx.Respond(sip.NewResponseFromRequest(req, sip.StatusGatewayTimeout, "Server Time-out", nil))
		return
	}
	if downstreamRes == nil {
		log.Error().Str("call_id", callID).Msg("device UPDATE returned no response")
		_ = tx.Respond(sip.NewResponseFromRequest(req, sip.StatusGatewayTimeout, "Server Time-out", nil))
		return
	}
	if downstreamRes.IsSuccess() && len(downstreamRes.Body()) == 0 {
		log.Error().Str("call_id", callID).Msg("device returned UPDATE success without an SDP answer")
		_ = tx.Respond(sip.NewResponseFromRequest(req, sip.StatusNotAcceptableHere, "Not Acceptable Here", nil))
		return
	}

	res := cm.newUpstreamDialogResponse(
		req,
		downstreamRes.StatusCode,
		downstreamRes.Reason,
		downstreamRes.Body(),
		responseContentType(downstreamRes),
		pc,
	)
	if err := tx.Respond(res); err != nil {
		log.Error().Err(err).Str("call_id", callID).Msg("failed to relay device UPDATE response")
		return
	}
	log.Info().
		Str("call_id", callID).
		Uint32("cseq", cseq.SeqNo).
		Int("sdp_len", len(downstreamRes.Body())).
		Msg("UPDATE bridged to device")
}

func (cm *CallManager) pendingBySIPCallID(callID string) *pendingCall {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	for _, pc := range cm.pending {
		if pc.callID == callID {
			return pc
		}
	}
	return nil
}

func activeClientDialog(pc *pendingCall) clientSession {
	pc.clientDlgMu.Lock()
	defer pc.clientDlgMu.Unlock()
	return pc.clientDlg
}

func (pc *pendingCall) beginRefresh(cseq uint32) (*pendingRefresh, bool) {
	pc.refreshMu.Lock()
	defer pc.refreshMu.Unlock()
	if pc.refresh != nil {
		return nil, false
	}
	refresh := &pendingRefresh{cseq: cseq, ackCh: make(chan struct{})}
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
			log.Info().Str("call_id", pc.callID).Uint32("cseq", refresh.cseq).Msg("re-INVITE ACK received from PBX")
			return
		case <-ctx.Done():
			return
		case <-tx.Done():
			log.Warn().Str("call_id", pc.callID).Uint32("cseq", refresh.cseq).Msg("re-INVITE transaction ended before PBX ACK")
			return
		case <-deadline.C:
			log.Warn().Str("call_id", pc.callID).Uint32("cseq", refresh.cseq).Msg("timed out waiting for PBX re-INVITE ACK")
			return
		case <-retransmit.C:
			if err := tx.Respond(res.Clone()); err != nil {
				log.Warn().Err(err).Str("call_id", pc.callID).Uint32("cseq", refresh.cseq).Msg("failed to retransmit re-INVITE response")
				return
			}
			interval = min(2*interval, sip.T2)
			retransmit.Reset(interval)
		}
	}
}

func (cm *CallManager) handleRefreshAck(req *sip.Request) bool {
	callID := requestCallID(req)
	cseq := req.CSeq()
	if callID == "" || cseq == nil {
		return false
	}

	pc := cm.pendingBySIPCallID(callID)
	if pc == nil {
		return false
	}
	pc.refreshMu.Lock()
	refresh := pc.refresh
	matched := refresh != nil && refresh.cseq == cseq.SeqNo
	var downstreamAck func(body []byte, contentType sip.Header) error
	if matched {
		downstreamAck = refresh.downstreamAck
	}
	pc.refreshMu.Unlock()
	if !matched {
		return false
	}

	var ackErr error
	refresh.ackOnce.Do(func() {
		if downstreamAck != nil {
			ackErr = downstreamAck(req.Body(), requestContentType(req))
		}
		close(refresh.ackCh)
	})
	if ackErr != nil {
		log.Error().Err(ackErr).Str("call_id", callID).Uint32("cseq", cseq.SeqNo).Msg("failed to complete offerless re-INVITE")
		if pc.cancel != nil {
			pc.cancel()
		}
	}
	return true
}

func newInDialogRequest(method sip.RequestMethod, dlg clientSession, body []byte, contentType sip.Header) *sip.Request {
	req := sip.NewRequest(method, dialogRemoteTarget(dlg, nil))
	if len(body) > 0 {
		req.SetBody(append([]byte(nil), body...))
		if contentType == nil {
			contentType = sip.NewHeader("Content-Type", "application/sdp")
		}
		req.AppendHeader(sip.NewHeader("Content-Type", contentType.Value()))
	}
	return req
}

func writeDialogAck(dlg clientSession, response *sip.Response, body []byte, contentType sip.Header) error {
	ack := sip.NewRequest(sip.ACK, dialogRemoteTarget(dlg, response))
	if len(body) > 0 {
		ack.SetBody(append([]byte(nil), body...))
		if contentType == nil {
			contentType = sip.NewHeader("Content-Type", "application/sdp")
		}
		ack.AppendHeader(sip.NewHeader("Content-Type", contentType.Value()))
	}
	return dlg.WriteRequest(ack)
}

func dialogRemoteTarget(dlg clientSession, response *sip.Response) sip.Uri {
	if response != nil {
		if contact := response.Contact(); contact != nil {
			return contact.Address
		}
	}
	if inviteResponse := dlg.InviteResponse(); inviteResponse != nil {
		if contact := inviteResponse.Contact(); contact != nil {
			return contact.Address
		}
	}
	if inviteRequest := dlg.InviteRequest(); inviteRequest != nil {
		return inviteRequest.Recipient
	}
	return sip.Uri{}
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

func (cm *CallManager) newUpstreamDialogResponse(
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

	contentType := sip.NewHeader("Content-Type", "application/sdp")

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

	if err := dlgClient.WaitAnswer(ctx, sipgo.AnswerOptions{}); err != nil {
		log.Error().Err(err).Str("call_id", pc.id).Msg("device did not answer relay INVITE")
		pc.serverDlg.Respond(sip.StatusTemporarilyUnavailable, "Temporarily Unavailable", nil)
		pc.serverDlg.Close()
		return
	}

	if ctx.Err() != nil {
		log.Info().Str("call_id", pc.id).Msg("call cancelled while waiting for device answer, terminating relay leg")
		byeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		dlgClient.Bye(byeCtx)
		cancel()
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

	if err := dlgClient.Ack(ctx); err != nil {
		log.Error().Err(err).Str("call_id", pc.id).Msg("failed to ACK device")
		pc.serverDlg.Respond(sip.StatusTemporarilyUnavailable, "Temporarily Unavailable", nil)
		pc.serverDlg.Close()
		return
	}

	deviceSDP := inviteResponse.Body()
	log.Info().Str("call_id", pc.id).Int("sdp_len", len(deviceSDP)).Msg("sending 200 OK to PBX")

	contentTypeHdr := sip.NewHeader("Content-Type", "application/sdp")
	headers := []sip.Header{
		contentTypeHdr,
		sip.NewHeader("Allow", dialogAllowMethods),
		sip.NewHeader("Supported", "timer"),
	}
	if contactHdr := cm.upstreamContactHeader(device.B2buaSipUser); contactHdr != nil {
		headers = append(headers, contactHdr)
	}
	if pc.sessionExpires != "" {
		headers = append(headers, sip.NewHeader("Session-Expires", pc.sessionExpires))
	}

	if err := pc.serverDlg.Respond(sip.StatusOK, "OK", deviceSDP, headers...); err != nil {
		log.Error().Err(err).Str("call_id", pc.id).Msg("failed to send 200 OK to PBX")
		pc.serverDlg.Close()
		return
	}

	cm.dbQueries.UpdatePendingCallState(context.Background(), db.UpdatePendingCallStateParams{
		CallID: pc.id,
		State:  "BRIDGED",
	})
	log.Info().Str("call_id", pc.id).Msg("call bridged")

	select {
	case <-dlgClient.Context().Done():
		log.Info().Str("call_id", pc.id).Msg("device ended call, sending BYE to PBX")
		byeCtx, byeCancel := context.WithTimeout(context.Background(), 5*time.Second)
		pc.serverDlg.Bye(byeCtx)
		byeCancel()
	case <-pc.ctx.Done():
		log.Info().Str("call_id", pc.id).Msg("PBX ended call, sending BYE to device")
		byeCtx, byeCancel := context.WithTimeout(context.Background(), 5*time.Second)
		dlgClient.Bye(byeCtx)
		byeCancel()
	}

	log.Info().Str("call_id", pc.id).Msg("call finished")
}

func (cm *CallManager) handleAck(req *sip.Request, tx sip.ServerTransaction) {
	if cm.handleRefreshAck(req) {
		return
	}
	err := cm.dialogSrv.ReadAck(req, tx)
	if err != nil {
		log.Error().Err(err).Str("call_id", req.CallID().Value()).Msg("failed to read ACK into dialog")
	} else {
		log.Info().Str("call_id", req.CallID().Value()).Msg("ACK received and processed")
	}
}

func (cm *CallManager) handleBye(req *sip.Request, tx sip.ServerTransaction) {
	err := cm.dialogSrv.ReadBye(req, tx)
	if err == nil {
		log.Info().Str("call_id", req.CallID().Value()).Msg("BYE received from PBX")
		callIDVal := req.CallID().Value()
		cm.mu.RLock()
		for _, pc := range cm.pending {
			if pc.callID == callIDVal {
				pc.cancel()
				break
			}
		}
		cm.mu.RUnlock()
		return
	}

	if err := cm.dialogCli.ReadBye(req, tx); err == nil {
		log.Info().Str("call_id", req.CallID().Value()).Msg("BYE received from device")
		return
	}

	log.Warn().Str("call_id", req.CallID().Value()).Msg("BYE received for unknown dialog")
	res := sip.NewResponseFromRequest(req, 200, "OK", nil)
	tx.Respond(res)
}

func (cm *CallManager) handleCancel(req *sip.Request, tx sip.ServerTransaction) {
	callIDVal := req.CallID().Value()

	cm.mu.RLock()
	var found *pendingCall
	for _, pc := range cm.pending {
		if pc.callID == callIDVal {
			found = pc
			break
		}
	}
	cm.mu.RUnlock()

	if found == nil {
		log.Warn().Str("sip_call_id", callIDVal).Msg("CANCEL for unknown call")
		tx.Respond(sip.NewResponseFromRequest(req, 200, "OK", nil))
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
	tx.Respond(sip.NewResponseFromRequest(req, 200, "OK", nil))
}

func (cm *CallManager) GetPendingCallsCount() int {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	return len(cm.pending)
}

func (cm *CallManager) cleanup(callID string) {
	cm.mu.Lock()
	delete(cm.pending, callID)
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
