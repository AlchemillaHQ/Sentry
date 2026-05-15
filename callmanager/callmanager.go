package callmanager

import (
	"context"
	"log/slog"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/AlchemillaHQ/Difuse-B2BUA/db"
	"github.com/AlchemillaHQ/Difuse-B2BUA/push"
	"github.com/AlchemillaHQ/Difuse-B2BUA/secrets"
	"github.com/AlchemillaHQ/Difuse-B2BUA/sipstack"
	"github.com/emiago/sipgo"
	"github.com/emiago/sipgo/sip"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

const callTimeout = 30 * time.Second

type pendingCall struct {
	id         string
	deviceID   string
	sipUser     string
	callID     string
	callerURI  string
	callerName string
	callerUser string
	callerHost string
	sdpOffer   []byte
	serverDlg  *sipgo.DialogServerSession
	clientDlg  *sipgo.DialogClientSession
	clientDlgMu sync.Mutex
	readyCh    chan struct{}
	ctx        context.Context
	cancel     context.CancelFunc
}

type CallManager struct {
	database   *gorm.DB
	stack      *sipstack.Stack
	registrar  *sipstack.UpstreamRegistrar
	pushSender *push.Dispatcher
	box        *secrets.Box

	mu            sync.RWMutex
	pending       map[string]*pendingCall
	deviceSource  map[string]sip.Uri

	dialogSrv *sipgo.DialogServerCache
	dialogCli *sipgo.DialogClientCache
}

func New(database *gorm.DB, stack *sipstack.Stack, registrar *sipstack.UpstreamRegistrar, pushSender *push.Dispatcher, box *secrets.Box) *CallManager {
	contactHdr := sip.ContactHeader{
		Address: sip.Uri{
			Host: stack.ExternalIP(),
			Port: stack.ExternalSIPPort(),
		},
	}

	cm := &CallManager{
		database:      database,
		stack:         stack,
		registrar:     registrar,
		pushSender:    pushSender,
		box:           box,
		pending:      make(map[string]*pendingCall),
		deviceSource: make(map[string]sip.Uri),
		dialogSrv:     sipgo.NewDialogServerCache(stack.Client(), contactHdr),
		dialogCli:     sipgo.NewDialogClientCache(stack.Client(), contactHdr),
	}

	stack.SetOnInvite(cm.handleInvite)
	stack.SetOnAck(cm.handleAck)
	stack.SetOnBye(cm.handleBye)
	stack.SetOnCancel(cm.handleCancel)
	stack.SetOnRegister(cm.handleRegister)

	return cm
}

func (cm *CallManager) handleRegister(req *sip.Request, tx sip.ServerTransaction) {
	toHdr := req.To()
	if toHdr == nil {
		tx.Respond(sip.NewResponseFromRequest(req, 400, "Bad Request", nil))
		return
	}

	sipUser := toHdr.Address.User
	slog.Info("REGISTER received", "sip_user", sipUser)

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
			cm.deviceSource[sipUser] = uri
			cm.mu.Unlock()
			slog.Info("stored device source", "sip_user", sipUser, "source", source, "transport", transport)
		}
	}

	contact := req.Contact()
	if contact != nil {
		cm.updateDeviceFromContact(sipUser, contact)
	}

	cm.mu.RLock()
	defer cm.mu.RUnlock()

	for _, pc := range cm.pending {
		if pc.sipUser == sipUser {
			select {
			case <-pc.readyCh:
			default:
				close(pc.readyCh)
			}
		}
	}
}

func (cm *CallManager) updateDeviceFromContact(sipUser string, contact *sip.ContactHeader) {
	var device db.Device
	if err := cm.database.Where("b2bua_sip_user = ?", sipUser).First(&device).Error; err != nil {
		return
	}

	updates := map[string]interface{}{
		"device_contact": contact.Address.String(),
		"last_seen":      time.Now(),
	}

	if params := contact.Address.UriParams; params != nil {
		if provider, ok := params.Get("pn-provider"); ok && provider != "" {
			updates["push_provider"] = provider
		}
		if param, ok := params.Get("pn-param"); ok && param != "" {
			updates["push_param"] = param
		}
		if prid, ok := params.Get("pn-prid"); ok && prid != "" {
			encToken, err := cm.box.Encrypt([]byte(prid))
			if err == nil {
				updates["push_token"] = encToken
				updates["push_prid"] = prid
			}
		}
	}

	cm.database.Model(&db.Device{}).Where("b2bua_sip_user = ?", sipUser).Updates(updates)
}

func (cm *CallManager) handleInvite(req *sip.Request, tx sip.ServerTransaction) {
	toHdr := req.To()
	if toHdr == nil {
		tx.Respond(sip.NewResponseFromRequest(req, 400, "Bad Request", nil))
		return
	}

	sipUser := toHdr.Address.User
	slog.Info("INVITE received", "to_user", sipUser, "call_id", req.CallID().Value())

	var device db.Device
	if err := cm.database.Where("upstream_user = ?", sipUser).First(&device).Error; err != nil {
		if err := cm.database.Where("b2bua_sip_user = ?", sipUser).First(&device).Error; err != nil {
			slog.Warn("INVITE for unknown user", "user", sipUser)
			tx.Respond(sip.NewResponseFromRequest(req, 404, "Not Found", nil))
			return
		}
	}

	dlg, err := cm.dialogSrv.ReadInvite(req, tx)
	if err != nil {
		slog.Error("failed to read invite into dialog", "error", err)
		tx.Respond(sip.NewResponseFromRequest(req, 500, "Server Error", nil))
		return
	}

	dlg.Respond(sip.StatusTrying, "Trying", nil)

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
		sipUser:    device.B2BUASIPUser,
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

	cm.database.Create(&db.PendingCall{
		CallID:     callID,
		DeviceID:   device.DeviceID,
		SIPCallID:  req.CallID().Value(),
		SIPFrom:    callerURI,
		SIPTo:      toHdr.Address.String(),
		CallerURI:  callerURI,
		CallerName: callerName,
		SDPOffer:   string(req.Body()),
		State:      "PENDING_PUSH",
		ExpiresAt:  time.Now().Add(callTimeout),
	})

	dlg.Respond(110, "Push sent", nil)

	pushTokenBytes, err := cm.box.Decrypt(device.PushToken)
	if err != nil {
		slog.Error("failed to decrypt push token", "device", device.DeviceID, "error", err)
		dlg.Respond(sip.StatusTemporarilyUnavailable, "Temporarily Unavailable", nil)
		return
	}

	slog.Info("sending push notification", "call_id", callID, "device", device.DeviceID)
	if err := cm.pushSender.Send(context.Background(), device.Platform, string(pushTokenBytes), callID, callerURI, callerName); err != nil {
		slog.Error("push notification failed", "device", device.DeviceID, "error", err)
		dlg.Respond(sip.StatusTemporarilyUnavailable, "Temporarily Unavailable", nil)
		return
	}

	cm.database.Model(&db.PendingCall{}).Where("call_id = ?", callID).Update("state", "PUSH_SENT")

	dlg.Respond(sip.StatusRinging, "Ringing", nil)

	slog.Info("push sent, waiting for device re-register", "call_id", callID, "device", device.DeviceID)

	select {
	case <-pc.readyCh:
		slog.Info("device ready, relaying call", "call_id", callID)
		cm.database.Model(&db.PendingCall{}).Where("call_id = ?", callID).Update("state", "DEVICE_READY")
	case <-timeoutCtx.Done():
		slog.Warn("call timed out waiting for device", "call_id", callID, "device", device.DeviceID)
		dlg.Respond(sip.StatusTemporarilyUnavailable, "Temporarily Unavailable", nil)
		return
	}

	cm.relayCall(callCtx, pc, &device)
}

func (cm *CallManager) relayCall(ctx context.Context, pc *pendingCall, device *db.Device) {
	cm.mu.RLock()
	srcUri, ok := cm.deviceSource[pc.sipUser]
	cm.mu.RUnlock()

	if !ok {
		slog.Error("no device source stored", "call_id", pc.id, "sip_user", pc.sipUser)
		pc.serverDlg.Respond(sip.StatusTemporarilyUnavailable, "Temporarily Unavailable", nil)
		pc.serverDlg.Close()
		return
	}

	recipient := sip.Uri{
		User: device.B2BUASIPUser,
		Host: srcUri.Host,
		Port: srcUri.Port,
	}
	if srcUri.UriParams != nil {
		recipient.UriParams = srcUri.UriParams
	}

	slog.Info("relaying call to device",
		"call_id", pc.id,
		"device", device.DeviceID,
		"recipient", recipient.String())

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
		slog.Error("failed to send relay INVITE to device", "call_id", pc.id, "error", err)
		pc.serverDlg.Respond(sip.StatusTemporarilyUnavailable, "Temporarily Unavailable", nil)
		pc.serverDlg.Close()
		return
	}

	pc.clientDlgMu.Lock()
	pc.clientDlg = dlgClient
	pc.clientDlgMu.Unlock()

	defer dlgClient.Close()

	if err := dlgClient.WaitAnswer(ctx, sipgo.AnswerOptions{}); err != nil {
		slog.Error("device did not answer relay INVITE", "call_id", pc.id, "error", err)
		pc.serverDlg.Respond(sip.StatusTemporarilyUnavailable, "Temporarily Unavailable", nil)
		pc.serverDlg.Close()
		return
	}

	inviteResponse := dlgClient.InviteResponse
	if inviteResponse == nil {
		slog.Error("no invite response from device", "call_id", pc.id)
		pc.serverDlg.Respond(sip.StatusTemporarilyUnavailable, "Temporarily Unavailable", nil)
		pc.serverDlg.Close()
		return
	}
	slog.Info("device answered", "call_id", pc.id, "status_code", inviteResponse.StatusCode, "sdp_len", len(inviteResponse.Body()))

	if err := dlgClient.Ack(ctx); err != nil {
		slog.Error("failed to ACK device", "call_id", pc.id, "error", err)
		pc.serverDlg.Respond(sip.StatusTemporarilyUnavailable, "Temporarily Unavailable", nil)
		pc.serverDlg.Close()
		return
	}

	deviceSDP := inviteResponse.Body()
	slog.Info("sending 200 OK to PBX", "call_id", pc.id, "sdp_len", len(deviceSDP))
	if err := pc.serverDlg.Respond(sip.StatusOK, "OK", deviceSDP); err != nil {
		slog.Error("failed to send 200 OK to PBX", "call_id", pc.id, "error", err)
		pc.serverDlg.Close()
		return
	}

	cm.database.Model(&db.PendingCall{}).Where("call_id = ?", pc.id).Update("state", "BRIDGED")
	slog.Info("call bridged", "call_id", pc.id)

	select {
	case <-dlgClient.Context().Done():
		slog.Info("device ended call, sending BYE to PBX", "call_id", pc.id)
		byeCtx, byeCancel := context.WithTimeout(context.Background(), 5*time.Second)
		pc.serverDlg.Bye(byeCtx)
		byeCancel()
	case <-pc.ctx.Done():
		slog.Info("PBX ended call, sending BYE to device", "call_id", pc.id)
		byeCtx, byeCancel := context.WithTimeout(context.Background(), 5*time.Second)
		dlgClient.Bye(byeCtx)
		byeCancel()
	}

	cm.database.Model(&db.PendingCall{}).Where("call_id = ?", pc.id).Update("state", "TERMINATED")
	slog.Info("call terminated", "call_id", pc.id)
}

func (cm *CallManager) handleAck(req *sip.Request, tx sip.ServerTransaction) {
	cm.dialogSrv.ReadAck(req, tx)
}

func (cm *CallManager) handleBye(req *sip.Request, tx sip.ServerTransaction) {
	if err := cm.dialogSrv.ReadBye(req, tx); err == nil {
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
		return
	}
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
		slog.Warn("CANCEL for unknown call", "sip_call_id", callIDVal)
		tx.Respond(sip.NewResponseFromRequest(req, 200, "OK", nil))
		return
	}

	slog.Info("call cancelled by PBX", "call_id", found.id, "sip_call_id", callIDVal)

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

func (cm *CallManager) cleanup(callID string) {
	cm.mu.Lock()
	delete(cm.pending, callID)
	cm.mu.Unlock()
}
