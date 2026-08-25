package db

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/AlchemillaHQ/Sentry/config"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	pgtest "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

func setupDB(t *testing.T) (*Database, func()) {
	t.Helper()
	testcontainers.SkipIfProviderIsNotHealthy(t)

	ctx := context.Background()
	container, err := pgtest.Run(ctx,
		"postgres:15-alpine",
		pgtest.WithDatabase("testdb"),
		pgtest.WithUsername("testuser"),
		pgtest.WithPassword("testpass"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(30*time.Second)),
	)
	require.NoError(t, err)

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	database, err := Open(ctx, config.DatabaseConfig{
		Driver: "postgres",
		DSN:    dsn,
	})
	require.NoError(t, err)

	return database, func() {
		container.Terminate(ctx)
	}
}

func createDevice(t *testing.T, q Querier, deviceID, user string) Device {
	t.Helper()
	now := time.Now()
	err := q.UpsertDevice(context.Background(), UpsertDeviceParams{
		DeviceID:          deviceID,
		Platform:          "android",
		PushToken:         []byte("test-token"),
		UpstreamHost:      "pbx.example.com",
		UpstreamPort:      5060,
		UpstreamTransport: "udp",
		UpstreamUser:      user,
		UpstreamPassword:  []byte("enc-pass"),
		UpstreamRealm:     pgtype.Text{String: "realm", Valid: true},
		DisplayName:       pgtype.Text{String: "Test Device", Valid: true},
		B2buaSipUser:      user + "_" + deviceID[:8],
		ExpiresAt:         pgtype.Timestamptz{Time: now.Add(7 * 24 * time.Hour), Valid: true},
		LastSeen:          pgtype.Timestamptz{Time: now, Valid: true},
	})
	require.NoError(t, err)

	d, err := q.GetDeviceByID(context.Background(), deviceID)
	require.NoError(t, err)
	return d
}

func TestIntegration_Open(t *testing.T) {
	db, cleanup := setupDB(t)
	defer cleanup()
	assert.NotNil(t, db.Pool)
	assert.NotNil(t, db.Queries)
}

func TestIntegration_UpsertAndGetDevice(t *testing.T) {
	db, cleanup := setupDB(t)
	defer cleanup()

	d := createDevice(t, db.Queries, "device-001", "user1")
	assert.Equal(t, "device-001", d.DeviceID)
	assert.True(t, len(d.B2buaSipUser) > 8)

	d2, err := db.Queries.GetDeviceByB2BUASIPUser(context.Background(), d.B2buaSipUser)
	require.NoError(t, err)
	assert.Equal(t, d.DeviceID, d2.DeviceID)
}

func TestIntegration_GetDevicesByUpstreamUser(t *testing.T) {
	db, cleanup := setupDB(t)
	defer cleanup()

	deviceA := createDevice(t, db.Queries, "dev-a-0001", "alice")
	createDevice(t, db.Queries, "dev-b-0002", "alice")
	err := db.Queries.SetDeviceDisabled(context.Background(), SetDeviceDisabledParams{
		DeviceID: deviceA.DeviceID,
		Disabled: true,
	})
	require.NoError(t, err)

	devices, err := db.Queries.GetDevicesByUpstreamUser(context.Background(), "alice")
	require.NoError(t, err)
	require.Len(t, devices, 1)
	assert.Equal(t, "dev-b-0002", devices[0].DeviceID)
}

func TestIntegration_UpsertDevice_Update(t *testing.T) {
	db, cleanup := setupDB(t)
	defer cleanup()

	d := createDevice(t, db.Queries, "dev-update-01", "user1")
	err := db.Queries.UpdateDeviceContact(context.Background(), UpdateDeviceContactParams{
		B2buaSipUser:  d.B2buaSipUser,
		DeviceContact: pgtype.Text{String: "sip:shadow@10.0.0.2", Valid: true},
		UserAgent:     pgtype.Text{String: "Linphone/1.0", Valid: true},
	})
	require.NoError(t, err)

	err = db.Queries.UpsertDevice(context.Background(), UpsertDeviceParams{
		DeviceID:          d.DeviceID,
		Platform:          "ios",
		PushToken:         []byte("new-token"),
		UpstreamHost:      d.UpstreamHost,
		UpstreamPort:      d.UpstreamPort,
		UpstreamTransport: d.UpstreamTransport,
		UpstreamUser:      d.UpstreamUser,
		UpstreamPassword:  d.UpstreamPassword,
		B2buaSipUser:      d.B2buaSipUser,
		ExpiresAt:         d.ExpiresAt,
		LastSeen:          pgtype.Timestamptz{Time: time.Now(), Valid: true},
	})
	require.NoError(t, err)

	updated, err := db.Queries.GetDeviceByID(context.Background(), d.DeviceID)
	require.NoError(t, err)
	assert.Equal(t, "ios", updated.Platform)
	assert.Equal(t, []byte("new-token"), updated.PushToken)
	assert.Equal(t, "sip:shadow@10.0.0.2", updated.DeviceContact.String)
	assert.Equal(t, "Linphone/1.0", updated.UserAgent.String)
}

func TestIntegration_SetDeviceDisabled(t *testing.T) {
	db, cleanup := setupDB(t)
	defer cleanup()

	d := createDevice(t, db.Queries, "dev-disable-01", "user1")
	err := db.Queries.SetDeviceDisabled(context.Background(), SetDeviceDisabledParams{
		DeviceID: d.DeviceID, Disabled: true,
	})
	require.NoError(t, err)

	_, err = db.Queries.GetDeviceByB2BUASIPUser(context.Background(), d.B2buaSipUser)
	assert.Error(t, err)
}

func TestIntegration_DeleteDeviceByID(t *testing.T) {
	db, cleanup := setupDB(t)
	defer cleanup()

	d := createDevice(t, db.Queries, "dev-delete-01", "user1")
	err := db.Queries.DeleteDeviceByID(context.Background(), d.DeviceID)
	require.NoError(t, err)

	_, err = db.Queries.GetDeviceByID(context.Background(), d.DeviceID)
	assert.Error(t, err)
}

func TestIntegration_UpdateDeviceContact(t *testing.T) {
	db, cleanup := setupDB(t)
	defer cleanup()

	d := createDevice(t, db.Queries, "dev-contact-01", "user1")
	err := db.Queries.UpdateDeviceContact(context.Background(), UpdateDeviceContactParams{
		B2buaSipUser:  d.B2buaSipUser,
		DeviceContact: pgtype.Text{String: "sip:test@host", Valid: true},
		UserAgent:     pgtype.Text{String: "Test/1.0", Valid: true},
	})
	require.NoError(t, err)
}

func TestIntegration_UpdateDeviceLastSeen(t *testing.T) {
	db, cleanup := setupDB(t)
	defer cleanup()

	d := createDevice(t, db.Queries, "dev-lastseen-01", "user1")
	err := db.Queries.UpdateDeviceLastSeen(context.Background(), d.B2buaSipUser)
	require.NoError(t, err)
}

func TestIntegration_PruneDevices(t *testing.T) {
	db, cleanup := setupDB(t)
	defer cleanup()

	now := time.Now()
	type deviceFixture struct {
		id       string
		platform string
		disabled bool
		expires  time.Time
		pruned   bool
	}
	fixtures := []deviceFixture{
		{id: "expired-android-enabled", platform: "android", expires: now.Add(-time.Hour), pruned: true},
		{id: "expired-android-disabled", platform: "android", disabled: true, expires: now.Add(-time.Hour), pruned: true},
		{id: "expired-ios-enabled", platform: "ios", expires: now.Add(-time.Hour), pruned: false},
		{id: "expired-ios-disabled", platform: "ios", disabled: true, expires: now.Add(-time.Hour), pruned: true},
		{id: "current-android-enabled", platform: "android", expires: now.Add(time.Hour), pruned: false},
		{id: "current-ios-enabled", platform: "ios", expires: now.Add(time.Hour), pruned: false},
	}

	for index, fixture := range fixtures {
		err := db.Queries.UpsertDevice(context.Background(), UpsertDeviceParams{
			DeviceID:          fixture.id,
			Platform:          fixture.platform,
			PushToken:         []byte("t"),
			UpstreamHost:      "h",
			UpstreamPort:      5060,
			UpstreamTransport: "udp",
			UpstreamUser:      "user",
			UpstreamPassword:  []byte("p"),
			B2buaSipUser:      fmt.Sprintf("user_prune_%d", index),
			ExpiresAt:         pgtype.Timestamptz{Time: fixture.expires, Valid: true},
			LastSeen:          pgtype.Timestamptz{Time: now, Valid: true},
		})
		require.NoError(t, err)
		if fixture.disabled {
			require.NoError(t, db.Queries.SetDeviceDisabled(context.Background(), SetDeviceDisabledParams{
				DeviceID: fixture.id,
				Disabled: true,
			}))
		}
	}

	pruned, err := db.Queries.PruneDevices(context.Background(), pgtype.Timestamptz{Time: now, Valid: true})
	require.NoError(t, err)
	require.ElementsMatch(t, []PruneDevicesRow{
		{DeviceID: "expired-android-enabled", B2buaSipUser: "user_prune_0"},
		{DeviceID: "expired-android-disabled", B2buaSipUser: "user_prune_1"},
		{DeviceID: "expired-ios-disabled", B2buaSipUser: "user_prune_3"},
	}, pruned)

	for _, fixture := range fixtures {
		_, err = db.Queries.GetDeviceByID(context.Background(), fixture.id)
		if fixture.pruned {
			assert.Error(t, err, fixture.id)
		} else {
			assert.NoError(t, err, fixture.id)
		}
	}
}

func TestIntegration_PendingCalls(t *testing.T) {
	db, cleanup := setupDB(t)
	defer cleanup()

	d := createDevice(t, db.Queries, "dev-pcall-0001", "user1")
	now := time.Now()
	err := db.Queries.CreatePendingCall(context.Background(), CreatePendingCallParams{
		CallID:     "call-001",
		DeviceID:   d.DeviceID,
		SipCallID:  "sip-call-001",
		SipFrom:    "sip:alice@pbx.com",
		SipTo:      "sip:bob@pbx.com",
		SdpOffer:   pgtype.Text{String: "v=0", Valid: true},
		CallerUri:  "sip:alice@pbx.com",
		CallerName: pgtype.Text{String: "Alice", Valid: true},
		State:      "PENDING_PUSH",
		ExpiresAt:  pgtype.Timestamptz{Time: now.Add(time.Hour), Valid: true},
	})
	require.NoError(t, err)

	pc, err := db.Queries.GetPendingCall(context.Background(), "call-001")
	require.NoError(t, err)
	assert.Equal(t, "call-001", pc.CallID)

	err = db.Queries.UpdatePendingCallState(context.Background(), UpdatePendingCallStateParams{
		CallID: "call-001", State: "BRIDGED",
	})
	require.NoError(t, err)

	err = db.Queries.DeletePendingCall(context.Background(), "call-001")
	require.NoError(t, err)

	err = db.Queries.PrunePendingCalls(context.Background(), pgtype.Timestamptz{Time: now.Add(2 * time.Hour), Valid: true})
	require.NoError(t, err)
}

func TestIntegration_Users(t *testing.T) {
	db, cleanup := setupDB(t)
	defer cleanup()

	err := db.Queries.CreateUser(context.Background(), CreateUserParams{
		Username: "admin1", PasswordHash: "hash1", Role: "admin",
	})
	require.NoError(t, err)

	u, err := db.Queries.GetUser(context.Background(), "admin1")
	require.NoError(t, err)
	assert.Equal(t, "admin1", u.Username)

	err = db.Queries.UpdateUserPassword(context.Background(), UpdateUserPasswordParams{
		Username: "admin1", PasswordHash: "new-hash",
	})
	require.NoError(t, err)

	users, err := db.Queries.ListUsers(context.Background())
	require.NoError(t, err)
	assert.Len(t, users, 1)

	err = db.Queries.DeleteUser(context.Background(), "admin1")
	require.NoError(t, err)
}

func TestIntegration_SettingsAndEncryptionKey(t *testing.T) {
	db, cleanup := setupDB(t)
	defer cleanup()

	err := db.Queries.UpsertSetting(context.Background(), UpsertSettingParams{
		Key: "test-key", Value: []byte("test-value"),
	})
	require.NoError(t, err)

	val, err := db.Queries.GetSetting(context.Background(), "test-key")
	require.NoError(t, err)
	assert.Equal(t, []byte("test-value"), val)

	key, err := db.GetOrCreateEncryptionKey(context.Background(), "")
	require.NoError(t, err)
	assert.Len(t, key, 32)
}

func TestIntegration_Reset(t *testing.T) {
	db, cleanup := setupDB(t)
	defer cleanup()

	err := db.Reset(context.Background())
	require.NoError(t, err)

	_, err = db.Queries.GetSetting(context.Background(), "test-key")
	assert.Error(t, err)
}

func TestIntegration_OpenUnsupportedDriver(t *testing.T) {
	_, err := Open(context.Background(), config.DatabaseConfig{
		Driver: "sqlite",
		DSN:    ":memory:",
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "only postgres")
}

func TestIntegration_CleanupJunkDevices(t *testing.T) {
	db, cleanup := setupDB(t)
	defer cleanup()

	createDevice(t, db.Queries, "good-device01", "user1")
	count := db.CleanupJunkDevices(context.Background())
	assert.Equal(t, int64(0), count)
}

func TestIntegration_BootstrapUsers(t *testing.T) {
	db, cleanup := setupDB(t)
	defer cleanup()

	err := db.BootstrapUsers(context.Background(), []config.BootstrapUser{
		{Username: "admin1", Password: "test123"},
		{Username: "admin2", Password: "test456"},
	})
	require.NoError(t, err)

	u, err := db.Queries.GetUser(context.Background(), "admin1")
	require.NoError(t, err)
	assert.Equal(t, "admin1", u.Username)
	assert.Equal(t, "admin", u.Role)
}
