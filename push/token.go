package push

import (
	"encoding/hex"
	"fmt"
	"strings"
)

// NormalizeToken converts platform-specific push-token representations into
// the value expected by the provider. Android FCM tokens are intentionally
// opaque. Linphone may expose an iOS PushKit token as raw hexadecimal,
// TOKEN:voip, or as one segment of a combined pn-prid value.
func NormalizeToken(platform, value string) (string, error) {
	if !strings.EqualFold(platform, "ios") {
		return value, nil
	}

	token := strings.TrimSpace(value)
	if token == "" {
		return "", fmt.Errorf("iOS VoIP push token is required")
	}

	segments := strings.Split(token, "&")
	hasSuffixedSegment := false
	selected := ""
	for _, segment := range segments {
		segment = strings.TrimSpace(segment)
		separator := strings.LastIndexByte(segment, ':')
		if separator < 0 {
			continue
		}

		hasSuffixedSegment = true
		if strings.EqualFold(strings.TrimSpace(segment[separator+1:]), "voip") {
			selected = strings.TrimSpace(segment[:separator])
			break
		}
	}

	if selected != "" {
		token = selected
	} else if hasSuffixedSegment || len(segments) > 1 {
		return "", fmt.Errorf("iOS push token does not contain a VoIP token")
	}

	if len(token)%2 != 0 {
		return "", fmt.Errorf("iOS VoIP push token must be even-length hexadecimal")
	}
	if _, err := hex.DecodeString(token); err != nil {
		return "", fmt.Errorf("iOS VoIP push token must be hexadecimal")
	}

	return strings.ToLower(token), nil
}
