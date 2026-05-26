package callmanager

import (
	"context"
	"net"
	"strconv"
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

const callTimeout = 30 * time.Second

type pendingCall struct {
	id          string
	deviceID    string
	sipUser     string
	callID      string
	callerURI   string
	callerName  string
	callerUser  string
	callerHost  string
	sdpOffer    []byte
	serverDlg   *sipgo.DialogServerSession
	clientDlg   *sipgo.DialogClientSession
	clientDlgMu sync.Mutex
	readyCh     chan struct{}
	ctx         context.Context
	cancel      context.CancelFunc
}

type CallManager struct {
	dbQueries    db.Querier
	stack        *sipstack.Stack
	registrar    sipstack.Registrar
	pushSender   push.Sender
	box          *secrets.Box

	mu           sync.RWMutex
	pending      map[string]*pendingCall
	deviceSource map[string]sip.Uri

	dialogSrv *sipgo.DialogServerCache
	dialogCli *sipgo.DialogClientCache
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
		dbQueries:    database.Queries,
		stack:        stack,
		registrar:    registrar,
		pushSender:   pushSender,
		box:          box,
		pending:      make(map[string]*pendingCall),
		deviceSource: make(map[string]sip.Uri),
		dialogSrv:    sipgo.NewDialogServerCache(stack.Client(), contactHdr),
		dialogCli:    sipgo.NewDialogClientCache(stack.Client(), contactHdr),
	}

	pushSender.OnDeadToken(func(platform, token, callID string) {
		log.Warn().Str("call_id", callID).Str("platform", platform).Msg("push token invalid, disabling device")
		pc, err := database.Queries.GetPendingCall(context.Background(), callID)
		if err != nil {
			log.Error().Err(err).Str("call_id", callID).Msg("failed to get pending call for dead token cleanup")
			return
		}
		_ = database.Queries.SetDeviceDisabled(context.Background(), db.SetDeviceDisabledParams{
			DeviceID: pc.DeviceID,
			Disabled: true,
		})
		log.Info().Str("device_id", pc.DeviceID).Msg("device disabled due to invalid push token")
	})

	stack.SetOnInvite(cm.handleInvite)
	stack.SetOnAck(cm.handleAck)
	stack.SetOnBye(cm.handleBye)
	stack.SetOnCancel(cm.handleCancel)
	stack.SetOnRegister(cm.handleRegister)

	return cm
}

func (cm *CallManager) matchDevice(ctx context.Context, sipUser string) (*db.Device, error) {
	device, err := cm.dbQueries.GetDeviceByB2BUASIPUser(ctx, sipUser)
	if err == nil {
		return &device, nil
	}

	devices, err := cm.dbQueries.GetDevicesByUpstreamUser(ctx, sipUser)
	if err != nil || len(devices) == 0 {
		return nil, pgx.ErrNoRows
	}

	cm.mu.RLock()
	var foundID string
	for _, pc := range cm.pending {
		for i := range devices {
			if devices[i].DeviceID == pc.deviceID {
				foundID = devices[i].DeviceID
				break
			}
		}
		if foundID != "" {
			break
		}
	}
	cm.mu.RUnlock()

	if foundID != "" {
		for i := range devices {
			if devices[i].DeviceID == foundID {
				return &devices[i], nil
			}
		}
	}

	return &devices[0], nil
}

func (cm *CallManager) handleRegister(req *sip.Request, tx sip.ServerTransaction) {
	toHdr := req.To()
	if toHdr == nil {
		tx.Respond(sip.NewResponseFromRequest(req, 400, "Bad Request", nil))
		return
	}

	sipUser := toHdr.Address.User
	ctx := context.Background()
	device, err := cm.matchDevice(ctx, sipUser)
	if err != nil {
		log.Warn().Str("sip_user", sipUser).Msg("REGISTER rejected: unknown user")
		tx.Respond(sip.NewResponseFromRequest(req, 403, "Forbidden", nil))
		return
	}

	deviceKey := device.B2buaSipUser
	log.Info().Str("sip_user", sipUser).Str("device_id", device.DeviceID).Msg("REGISTER received")

	res := sip.NewResponseFromRequest(req, 200, "OK", nil)
	expiresHdr := sip.ExpiresHeader(120)
	res.AppendHeader(&expiresHdr)
	tx.Respond(res)

	source := req.Source()
	transport := req.Transport()
	if source != "" {
		host, portStr, err := net.SplitHostPort(source)
		if err == nil {
			port, _ := strconv.Atoi(portStr)
			uri := sip.Uri{
				Host: host,
				Port: port,
			}
			if transport != "" {
				uri.UriParams = sip.NewParams()
				uri.UriParams.Add("transport", transport)
			}
			cm.mu.Lock()
			cm.deviceSource[deviceKey] = uri
			cm.mu.Unlock()
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

	cm.mu.RLock()
	defer cm.mu.RUnlock()

	for _, pc := range cm.pending {
		if pc.sipUser == deviceKey {
			select {
			case <-pc.readyCh:
			default:
				close(pc.readyCh)
			}
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

	sipUser := toHdr.Address.User
	log.Info().Str("to", toHdr.String()).Str("call_id", req.CallID().Value()).Msg("INVITE received")

	dlg, err := cm.dialogSrv.ReadInvite(req, tx)
	if err != nil {
		log.Error().Err(err).Msg("failed to read invite into dialog")
		tx.Respond(sip.NewResponseFromRequest(req, 500, "Server Error", nil))
		return
	}

	if toHdr.Params.Has("tag") {
		log.Info().Str("call_id", req.CallID().Value()).Msg("in-dialog INVITE (re-INVITE) received")
		dlg.Respond(sip.StatusOK, "OK", nil)
		return
	}

	dlg.Respond(sip.StatusTrying, "Trying", nil)

	ctx := context.Background()
	device, err := cm.matchDevice(ctx, sipUser)
	if err != nil {
		log.Warn().Str("user", sipUser).Msg("INVITE rejected: unknown user")
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
		id:         callID,
		deviceID:   device.DeviceID,
		sipUser:    device.B2buaSipUser,
		callID:     req.CallID().Value(),
		callerURI:  callerURI,
		callerName: callerName,
		callerUser: callerUser,
		callerHost: callerHost,
		sdpOffer:   req.Body(),
		serverDlg:  dlg,
		readyCh:    make(chan struct{}),
		ctx:        callCtx,
		cancel:     callCancel,
	}

	cm.mu.Lock()
	cm.pending[callID] = pc
	cm.mu.Unlock()

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

	log.Info().Str("call_id", callID).Str("device", device.DeviceID).Msg("sending push notification")
	if err := cm.pushSender.Send(context.Background(), device.Platform, string(pushTokenBytes), callID, callerURI, callerName); err != nil {
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
	defer dlgClient.Close()

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

	inviteResponse := dlgClient.InviteResponse
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

	contactHdr := &sip.ContactHeader{
		Address: sip.Uri{
			User: device.B2buaSipUser,
			Host: cm.stack.ExternalIP(),
			Port: cm.stack.ExternalSIPPort(),
		},
	}
	if cm.stack.ExternalSIPTransport() != "" && cm.stack.ExternalSIPTransport() != "udp" {
		contactHdr.Address.UriParams = sip.NewParams()
		contactHdr.Address.UriParams.Add("transport", cm.stack.ExternalSIPTransport())
	}

	contentTypeHdr := sip.NewHeader("Content-Type", "application/sdp")
	allowHdr := sip.NewHeader("Allow", "INVITE, ACK, CANCEL, OPTIONS, BYE, REFER, NOTIFY, MESSAGE, SUBSCRIBE, INFO, UPDATE")

	if err := pc.serverDlg.Respond(sip.StatusOK, "OK", deviceSDP, contactHdr, contentTypeHdr, allowHdr); err != nil {
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

	found.clientDlgMu.Lock()
	if found.clientDlg != nil {
		byeCtx, byeCancel := context.WithTimeout(context.Background(), 5*time.Second)
		found.clientDlg.Bye(byeCtx)
		byeCancel()
	}
	found.clientDlgMu.Unlock()

	found.cancel()

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
