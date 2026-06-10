package auth

import (
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/assert"
)

func TestHashPassword_ReturnsNonEmptyHash(t *testing.T) {
	hash, err := HashPassword("mypassword")
	assert.NoError(t, err)
	assert.NotEmpty(t, hash)
	assert.NotEqual(t, "mypassword", hash)
}

func TestHashPassword_DifferentInputsProduceDifferentHashes(t *testing.T) {
	hash1, _ := HashPassword("password1")
	hash2, _ := HashPassword("password2")
	assert.NotEqual(t, hash1, hash2)
}

func TestCheckPasswordHash_CorrectPassword(t *testing.T) {
	hash, err := HashPassword("secret")
	assert.NoError(t, err)
	assert.True(t, CheckPasswordHash("secret", hash))
}

func TestCheckPasswordHash_WrongPassword(t *testing.T) {
	hash, _ := HashPassword("secret")
	assert.False(t, CheckPasswordHash("wrong", hash))
}

func TestCheckPasswordHash_InvalidHash(t *testing.T) {
	assert.False(t, CheckPasswordHash("password", "not-a-valid-hash"))
}

func TestCheckPasswordHash_EmptyPassword(t *testing.T) {
	hash, _ := HashPassword("")
	assert.True(t, CheckPasswordHash("", hash))
	assert.False(t, CheckPasswordHash("nonempty", hash))
}

func TestGenerateToken_ReturnsNonEmpty(t *testing.T) {
	token, err := GenerateToken("admin", "admin", "test-secret")
	assert.NoError(t, err)
	assert.NotEmpty(t, token)
}

func TestGenerateToken_ContainsClaims(t *testing.T) {
	tokenStr, err := GenerateToken("alice", "viewer", "test-secret")
	assert.NoError(t, err)

	token, err := jwt.ParseWithClaims(tokenStr, &Claims{}, func(token *jwt.Token) (interface{}, error) {
		return []byte("test-secret"), nil
	})
	assert.NoError(t, err)
	assert.True(t, token.Valid)

	claims, ok := token.Claims.(*Claims)
	assert.True(t, ok)
	assert.Equal(t, "alice", claims.Username)
	assert.Equal(t, "viewer", claims.Role)
	assert.NotNil(t, claims.ExpiresAt)
}

func TestGenerateToken_ExpiresIn24Hours(t *testing.T) {
	before := time.Now()
	tokenStr, _ := GenerateToken("user", "admin", "secret")
	after := time.Now()

	claims := &Claims{}
	jwt.ParseWithClaims(tokenStr, claims, func(token *jwt.Token) (interface{}, error) {
		return []byte("secret"), nil
	})

	expectedExpiry := before.Add(24 * time.Hour)
	assert.WithinDuration(t, expectedExpiry, claims.ExpiresAt.Time, 5*time.Second)
	assert.True(t, claims.ExpiresAt.Time.Before(after.Add(24*time.Hour+5*time.Second)))
}

func TestValidateToken_ValidToken(t *testing.T) {
	secret := "my-secret"
	tokenStr, err := GenerateToken("bob", "admin", secret)
	assert.NoError(t, err)

	claims, err := ValidateToken(tokenStr, secret)
	assert.NoError(t, err)
	assert.Equal(t, "bob", claims.Username)
	assert.Equal(t, "admin", claims.Role)
}

func TestValidateToken_WrongSecret(t *testing.T) {
	tokenStr, _ := GenerateToken("bob", "admin", "correct-secret")
	_, err := ValidateToken(tokenStr, "wrong-secret")
	assert.Error(t, err)
}

func TestValidateToken_MalformedToken(t *testing.T) {
	_, err := ValidateToken("not.a.jwt", "secret")
	assert.Error(t, err)
}

func TestValidateToken_EmptyToken(t *testing.T) {
	_, err := ValidateToken("", "secret")
	assert.Error(t, err)
}

func TestValidateToken_ExpiredToken(t *testing.T) {
	secret := "test-secret"
	claims := &Claims{
		Username: "user",
		Role:     "admin",
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(-1 * time.Hour)),
		},
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	tokenStr, err := token.SignedString([]byte(secret))
	assert.NoError(t, err)

	_, err = ValidateToken(tokenStr, secret)
	assert.Error(t, err)
}

func TestGenerateAndValidate_RoundTrip(t *testing.T) {
	secret := "round-trip-secret"
	tokenStr, err := GenerateToken("charlie", "viewer", secret)
	assert.NoError(t, err)

	claims, err := ValidateToken(tokenStr, secret)
	assert.NoError(t, err)
	assert.Equal(t, "charlie", claims.Username)
	assert.Equal(t, "viewer", claims.Role)
}
