package callmanager

import (
	"context"
	"log/slog"
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

const callTimeout = 28 * time.Second

type pendingCall struct {
	callID     string
	deviceID   string
	sipCallID  string
	callerURI  string
	callerName string
	sdpOffer   []byte
	serverDlg  *sipgo.DialogServerSession
	readyCh    chan struct{}
	cancelFunc context.CancelFunc
}

type CallManager struct {
	database   *gorm.DB
	stack      *sipstack.Stack
	registrar  *sipstack.UpstreamRegistrar
	pushSender *push.Dispatcher
	box        *secrets.Box

	mu       sync.RWMutex
	pending  map[string]*pendingCall
	bySIPUsr map[string]*pendingCall

	dialogSrv *sipgo.DialogServerCache
	dialogCli *sipgo.DialogClientCache
}

func New(database *gorm.DB, stack *sipstack.Stack, registrar *sipstack.UpstreamRegistrar, pushSender *push.Dispatcher, box *secrets.Box) *CallManager {
	contactHdr := sip.ContactHeader{
		Address: sip.Uri{
			Host: stack.ExternalIP(),
			Port: 5060,
		},
	}
	if contactHdr.Address.Host == "" {
		contactHdr.Address.Host = "127.0.0.1"
	}

	cm := &CallManager{
		database:   database,
		stack:      stack,
		registrar:  registrar,
		pushSender: pushSender,
		box:        box,
		pending:    make(map[string]*pendingCall),
		bySIPUsr:   make(map[string]*pendingCall),
		dialogSrv:  sipgo.NewDialogServerCache(stack.Client(), contactHdr),
		dialogCli:  sipgo.NewDialogClientCache(stack.Client(), contactHdr),
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
		res := sip.NewResponseFromRequest(req, 400, "Bad Request", nil)
		tx.Respond(res)
		return
	}

	sipUser := toHdr.Address.User

	res := sip.NewResponseFromRequest(req, 200, "OK", nil)
	expiresHdr := sip.ExpiresHeader(120)
	res.AppendHeader(&expiresHdr)
	tx.Respond(res)

	cm.mu.RLock()
	pc, ok := cm.bySIPUsr[sipUser]
	cm.mu.RUnlock()

	if ok {
		slog.Info("device re-registered, signaling readiness",
			"sip_user", sipUser,
			"call_id", pc.callID)
		select {
		case <-pc.readyCh:
		default:
			close(pc.readyCh)
		}
	}
}

func (cm *CallManager) handleInvite(req *sip.Request, tx sip.ServerTransaction) {
	toHdr := req.To()
	if toHdr == nil {
		slog.Warn("INVITE missing To header")
		res := sip.NewResponseFromRequest(req, 400, "Bad Request", nil)
		tx.Respond(res)
		return
	}

	sipUser := toHdr.Address.User
	slog.Info("INVITE received",
		"to_user", sipUser,
		"from", req.From().String(),
		"call_id", req.CallID().Value(),
		"sdp_len", len(req.Body()))

	var device db.Device
	if err := cm.database.Where("upstream_user = ?", sipUser).First(&device).Error; err != nil {
		if err := cm.database.Where("b2bua_sip_user = ?", sipUser).First(&device).Error; err != nil {
			slog.Warn("INVITE for unknown user", "user", sipUser)
			res := sip.NewResponseFromRequest(req, 404, "Not Found", nil)
			tx.Respond(res)
			return
		}
	}

	slog.Info("INVITE matched device",
		"device_id", device.DeviceID,
		"platform", device.Platform,
		"b2bua_user", device.B2BUASIPUser,
		"has_push_token", len(device.PushToken) > 0)

	dlg, err := cm.dialogSrv.ReadInvite(req, tx)
	if err != nil {
		slog.Error("failed to read invite into dialog", "error", err)
		res := sip.NewResponseFromRequest(req, 500, "Server Error", nil)
		tx.Respond(res)
		return
	}

	slog.Info("sending 100 Trying", "call_id", req.CallID().Value())
	dlg.Respond(sip.StatusTrying, "Trying", nil)

	fromHdr := req.From()
	callerURI := ""
	callerName := ""
	if fromHdr != nil {
		callerURI = fromHdr.Address.String()
		callerName = fromHdr.DisplayName
	}

	callID := uuid.New().String()
	callCtx, callCancel := context.WithTimeout(context.Background(), callTimeout)

	pc := &pendingCall{
		callID:     callID,
		deviceID:   device.DeviceID,
		sipCallID:  req.CallID().Value(),
		callerURI:  callerURI,
		callerName: callerName,
		sdpOffer:   req.Body(),
		serverDlg:  dlg,
		readyCh:    make(chan struct{}),
		cancelFunc: callCancel,
	}

	cm.mu.Lock()
	cm.pending[callID] = pc
	cm.bySIPUsr[device.B2BUASIPUser] = pc
	cm.mu.Unlock()

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

	slog.Info("sending 180 Ringing", "call_id", callID)
	dlg.Respond(sip.StatusRinging, "Ringing", nil)

	slog.Info("starting push+wait goroutine", "call_id", callID, "device", device.DeviceID)
	go cm.sendPushAndWait(callCtx, pc, &device)
}

func (cm *CallManager) sendPushAndWait(ctx context.Context, pc *pendingCall, device *db.Device) {
	defer pc.cancelFunc()
	defer cm.cleanup(pc, device.B2BUASIPUser)

	pushTokenBytes, err := cm.box.Decrypt(device.PushToken)
	if err != nil {
		slog.Error("failed to decrypt push token", "device", device.DeviceID, "error", err)
		pc.serverDlg.Respond(sip.StatusTemporarilyUnavailable, "Temporarily Unavailable", nil)
		pc.serverDlg.Close()
		return
	}

	slog.Info("sending push notification",
		"call_id", pc.callID,
		"device", device.DeviceID,
		"platform", device.Platform)

	pushCtx := context.Background()
	if err := cm.pushSender.Send(pushCtx, device.Platform, string(pushTokenBytes), pc.callID, pc.callerURI, pc.callerName); err != nil {
		slog.Error("push notification failed", "device", device.DeviceID, "error", err)
		pc.serverDlg.Respond(sip.StatusTemporarilyUnavailable, "Temporarily Unavailable", nil)
		pc.serverDlg.Close()
		return
	}

	cm.database.Model(&db.PendingCall{}).Where("call_id = ?", pc.callID).Update("state", "PUSH_SENT")
	slog.Info("push sent, waiting for device re-register", "call_id", pc.callID, "device", device.DeviceID)

	select {
	case <-pc.readyCh:
		slog.Info("device ready, relaying call", "call_id", pc.callID)
		cm.database.Model(&db.PendingCall{}).Where("call_id = ?", pc.callID).Update("state", "DEVICE_READY")
		cm.relayCall(ctx, pc, device)
	case <-ctx.Done():
		slog.Warn("call timed out waiting for device", "call_id", pc.callID, "device", device.DeviceID)
		pc.serverDlg.Respond(sip.StatusTemporarilyUnavailable, "Temporarily Unavailable", nil)
		pc.serverDlg.Close()
		cm.database.Model(&db.PendingCall{}).Where("call_id = ?", pc.callID).Update("state", "EXPIRED")
	}
}

func (cm *CallManager) relayCall(ctx context.Context, pc *pendingCall, device *db.Device) {
	slog.Info("relaying call to device",
		"call_id", pc.callID,
		"device", device.DeviceID,
		"b2bua_user", device.B2BUASIPUser)

	recipient := sip.Uri{
		User: device.B2BUASIPUser,
		Host: cm.stack.ExternalIP(),
		Port: 5060,
	}
	if recipient.Host == "" {
		recipient.Host = "127.0.0.1"
	}

	fromHdr := &sip.FromHeader{
		DisplayName: pc.callerName,
		Address:     sip.Uri{User: "relay", Host: cm.stack.ExternalIP()},
	}

	dlgClient, err := cm.dialogCli.Invite(ctx, recipient, pc.sdpOffer, fromHdr)
	if err != nil {
		slog.Error("failed to send relay INVITE to device", "call_id", pc.callID, "error", err)
		pc.serverDlg.Respond(sip.StatusTemporarilyUnavailable, "Temporarily Unavailable", nil)
		pc.serverDlg.Close()
		return
	}
	defer dlgClient.Close()

	if err := dlgClient.WaitAnswer(ctx, sipgo.AnswerOptions{}); err != nil {
		slog.Error("device did not answer relay INVITE", "call_id", pc.callID, "error", err)
		pc.serverDlg.Respond(sip.StatusTemporarilyUnavailable, "Temporarily Unavailable", nil)
		pc.serverDlg.Close()
		return
	}

	if err := dlgClient.Ack(ctx); err != nil {
		slog.Error("failed to ACK device", "call_id", pc.callID, "error", err)
		return
	}

	deviceSDP := dlgClient.InviteResponse.Body()
	slog.Info("device answered, sending 200 OK to PBX",
		"call_id", pc.callID,
		"device_sdp_len", len(deviceSDP))
	pc.serverDlg.Respond(sip.StatusOK, "OK", deviceSDP)

	cm.database.Model(&db.PendingCall{}).Where("call_id = ?", pc.callID).Update("state", "BRIDGED")
	slog.Info("call bridged", "call_id", pc.callID)

	<-dlgClient.Context().Done()

	slog.Info("call ended, sending BYE to PBX", "call_id", pc.callID)
	pc.serverDlg.Bye(ctx)
	cm.database.Model(&db.PendingCall{}).Where("call_id = ?", pc.callID).Update("state", "TERMINATED")
	slog.Info("call terminated", "call_id", pc.callID)
}

func (cm *CallManager) handleAck(req *sip.Request, tx sip.ServerTransaction) {
	cm.dialogSrv.ReadAck(req, tx)
}

func (cm *CallManager) handleBye(req *sip.Request, tx sip.ServerTransaction) {
	if err := cm.dialogSrv.ReadBye(req, tx); err == nil {
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
		if pc.sipCallID == callIDVal {
			found = pc
			break
		}
	}
	cm.mu.RUnlock()

	if found != nil {
		slog.Info("call cancelled by PBX", "call_id", found.callID, "sip_call_id", callIDVal)
		found.cancelFunc()
	} else {
		slog.Warn("CANCEL for unknown call", "sip_call_id", callIDVal)
	}

	res := sip.NewResponseFromRequest(req, 200, "OK", nil)
	tx.Respond(res)
}

func (cm *CallManager) cleanup(pc *pendingCall, b2buaSIPUser string) {
	cm.mu.Lock()
	delete(cm.pending, pc.callID)
	delete(cm.bySIPUsr, b2buaSIPUser)
	cm.mu.Unlock()
}
