package db

import (
	"crypto/rand"
	"fmt"
	"time"

	"gorm.io/gorm"
)

type Setting struct {
	Key   string `gorm:"primaryKey;type:varchar(255)"`
	Value []byte `gorm:"type:blob;not null"`
}

func GetOrCreateEncryptionKey(database *gorm.DB) ([]byte, error) {
	var s Setting
	err := database.Where("key = ?", "encryption_key").First(&s).Error
	if err == nil {
		return s.Value, nil
	}
	if err != gorm.ErrRecordNotFound {
		return nil, fmt.Errorf("query setting: %w", err)
	}
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		return nil, fmt.Errorf("generate key: %w", err)
	}
	s = Setting{Key: "encryption_key", Value: key}
	if err := database.Create(&s).Error; err != nil {
		return nil, fmt.Errorf("store key: %w", err)
	}
	return key, nil
}

type Device struct {
	DeviceID          string    `gorm:"primaryKey;type:varchar(36)" json:"device_id"`
	Platform          string    `gorm:"type:varchar(10);not null" json:"platform"`
	PushToken         []byte    `gorm:"type:blob;not null" json:"-"`
	UpstreamHost      string    `gorm:"type:varchar(255);not null" json:"upstream_host"`
	UpstreamPort      int       `gorm:"not null;default:5060" json:"upstream_port"`
	UpstreamTransport string    `gorm:"type:varchar(10);not null;default:'udp'" json:"upstream_transport"`
	UpstreamUser      string    `gorm:"type:varchar(255);not null" json:"upstream_user"`
	UpstreamPassword  []byte    `gorm:"type:blob;not null" json:"-"`
	UpstreamRealm     string    `gorm:"type:varchar(255)" json:"upstream_realm"`
	DisplayName       string    `gorm:"type:varchar(255)" json:"display_name"`
	B2BUASIPUser      string    `gorm:"column:b2bua_sip_user;type:varchar(255);uniqueIndex;not null" json:"b2bua_sip_user"`
	RegisteredAt      time.Time `gorm:"autoCreateTime" json:"registered_at"`
	ExpiresAt         time.Time `gorm:"not null" json:"expires_at"`
	LastSeen          time.Time `json:"last_seen"`
}

type PendingCall struct {
	CallID     string    `gorm:"primaryKey;type:varchar(36)" json:"call_id"`
	DeviceID   string    `gorm:"type:varchar(36);index;not null" json:"device_id"`
	SIPCallID  string    `gorm:"column:sip_call_id;type:varchar(255);not null" json:"sip_call_id"`
	SIPFrom    string    `gorm:"column:sip_from;type:text;not null" json:"sip_from"`
	SIPTo      string    `gorm:"column:sip_to;type:text;not null" json:"sip_to"`
	SDPOffer   string    `gorm:"column:sdp_offer;type:text" json:"sdp_offer"`
	CallerURI  string    `gorm:"type:text;not null" json:"caller_uri"`
	CallerName string    `gorm:"type:varchar(255)" json:"caller_name"`
	State      string    `gorm:"type:varchar(30);not null;default:'PENDING_PUSH'" json:"state"`
	CreatedAt  time.Time `gorm:"autoCreateTime" json:"created_at"`
	ExpiresAt  time.Time `gorm:"not null" json:"expires_at"`
}
