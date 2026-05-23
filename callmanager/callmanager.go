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
	database   *gorm.DB
	stack      *sipstack.Stack
	registrar  *sipstack.UpstreamRegistrar
	pushSender *push.Dispatcher
	box        *secrets.Box

	mu           sync.RWMutex
	pending      map[string]*pendingCall
	deviceSource map[string]sip.Uri

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
	if stack.ExternalSIPTransport() != "" && stack.ExternalSIPTransport() != "udp" {
		contactHdr.Address.UriParams = sip.NewParams()
		contactHdr.Address.UriParams.Add("transport", stack.ExternalSIPTransport())
	}

	cm := &CallManager{
		database:     database,
		stack:        stack,
		registrar:    registrar,
		pushSender:   pushSender,
		box:          box,
		pending:      make(map[string]*pendingCall),
		deviceSource: make(map[string]sip.Uri),
		dialogSrv:    sipgo.NewDialogServerCache(stack.Client(), contactHdr),
		dialogCli:    sipgo.NewDialogClientCache(stack.Client(), contactHdr),
	}

	stack.SetOnInvite(cm.handleInvite)
	stack.SetOnAck(cm.handleAck)
	stack.SetOnBye(cm.handleBye)
	stack.SetOnCancel(cm.handleCancel)
	stack.SetOnRegister(cm.handleRegister)

	return cm
}

func (cm *CallManager) matchDevice(sipUser string) (*db.Device, error) {
	var device db.Device
	if err := cm.database.Where("b2bua_sip_user = ?", sipUser).First(&device).Error; err == nil {
		return &device, nil
	}

	var devices []db.Device
	cm.database.Where("upstream_user = ?", sipUser).Find(&devices)
	if len(devices) == 0 {
		return nil, gorm.ErrRecordNotFound
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
	device, err := cm.matchDevice(sipUser)
	if err != nil {
		slog.Warn("REGISTER rejected: unknown user", "sip_user", sipUser)
		tx.Respond(sip.NewResponseFromRequest(req, 403, "Forbidden", nil))
		return
	}

	deviceKey := device.B2BUASIPUser
	slog.Info("REGISTER received", "sip_user", sipUser, "device_id", device.DeviceID)

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
	if contact != nil {
		cm.updateDeviceFromContact(device.B2BUASIPUser, contact)
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
	slog.Info("INVITE received", "to", toHdr.String(), "call_id", req.CallID().Value())

	dlg, err := cm.dialogSrv.ReadInvite(req, tx)
	if err != nil {
		slog.Error("failed to read invite into dialog", "error", err)
		tx.Respond(sip.NewResponseFromRequest(req, 500, "Server Error", nil))
		return
	}

	if toHdr.Params.Has("tag") {
		slog.Info("in-dialog INVITE (re-INVITE) received", "call_id", req.CallID().Value())
		dlg.Respond(sip.StatusOK, "OK", nil)
		return
	}

	dlg.Respond(sip.StatusTrying, "Trying", nil)

	device, err := cm.matchDevice(sipUser)
	if err != nil {
		slog.Warn("INVITE rejected: unknown user", "user", sipUser)
		tx.Respond(sip.NewResponseFromRequest(req, 404, "Not Found", nil))
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
		cm.relayCall(callCtx, pc, device)
		return
	case <-timeoutCtx.Done():
		slog.Warn("call timed out waiting for device", "call_id", callID, "device", device.DeviceID)
		if callCtx.Err() != nil {
			return
		}
		dlg.Respond(sip.StatusTemporarilyUnavailable, "Temporarily Unavailable", nil)
		return
	case <-callCtx.Done():
		slog.Info("call cancelled before device wake", "call_id", callID)
		return
	}
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

	slog.Info("relaying call to device", "call_id", pc.id, "device", device.DeviceID, "recipient", recipient.String())

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
	defer dlgClient.Close()

	pc.clientDlgMu.Lock()
	pc.clientDlg = dlgClient
	pc.clientDlgMu.Unlock()

	if err := dlgClient.WaitAnswer(ctx, sipgo.AnswerOptions{}); err != nil {
		slog.Error("device did not answer relay INVITE", "call_id", pc.id, "error", err)
		pc.serverDlg.Respond(sip.StatusTemporarilyUnavailable, "Temporarily Unavailable", nil)
		pc.serverDlg.Close()
		return
	}

	if ctx.Err() != nil {
		slog.Info("call cancelled while waiting for device answer, terminating relay leg", "call_id", pc.id)
		byeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		dlgClient.Bye(byeCtx)
		cancel()
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

	contactHdr := &sip.ContactHeader{
		Address: sip.Uri{
			User: device.B2BUASIPUser,
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
	err := cm.dialogSrv.ReadAck(req, tx)
	if err != nil {
		slog.Error("failed to read ACK into dialog", "error", err, "call_id", req.CallID().Value())
	} else {
		slog.Info("ACK received and processed", "call_id", req.CallID().Value())
	}
}

func (cm *CallManager) handleBye(req *sip.Request, tx sip.ServerTransaction) {
	err := cm.dialogSrv.ReadBye(req, tx)
	if err == nil {
		slog.Info("BYE received from PBX", "call_id", req.CallID().Value())
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
		slog.Info("BYE received from device", "call_id", req.CallID().Value())
		return
	}

	slog.Warn("BYE received for unknown dialog", "call_id", req.CallID().Value())
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
	slog.Info("sent BYE to all bridged calls", "count", len(pendingCalls))
}
