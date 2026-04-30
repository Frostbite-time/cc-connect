package qq

import (
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/chenhg5/cc-connect/core"
)

func TestPlatform_Name(t *testing.T) {
	p := &Platform{}
	if got := p.Name(); got != "qq" {
		t.Errorf("Name() = %q, want %q", got, "qq")
	}
}

func TestNew_DefaultWSURL(t *testing.T) {
	p, err := New(map[string]any{})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	platform := p.(*Platform)
	if platform.wsURL != "ws://127.0.0.1:3001" {
		t.Errorf("wsURL = %q, want %q", platform.wsURL, "ws://127.0.0.1:3001")
	}
}

func TestNew_CustomWSURL(t *testing.T) {
	p, err := New(map[string]any{
		"ws_url": "ws://example.com:8080",
	})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	platform := p.(*Platform)
	if platform.wsURL != "ws://example.com:8080" {
		t.Errorf("wsURL = %q, want %q", platform.wsURL, "ws://example.com:8080")
	}
}

func TestNew_WithToken(t *testing.T) {
	p, err := New(map[string]any{
		"token": "my-secret-token",
	})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	platform := p.(*Platform)
	if platform.token != "my-secret-token" {
		t.Errorf("token = %q, want %q", platform.token, "my-secret-token")
	}
}

func TestNew_WithAllowFrom(t *testing.T) {
	p, err := New(map[string]any{
		"allow_from": "user1,user2,*",
	})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	platform := p.(*Platform)
	if platform.allowFrom != "user1,user2,*" {
		t.Errorf("allowFrom = %q, want %q", platform.allowFrom, "user1,user2,*")
	}
}

func TestNew_ShareSessionInChannel(t *testing.T) {
	p, err := New(map[string]any{
		"share_session_in_channel": true,
	})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	platform := p.(*Platform)
	if !platform.shareSessionInChannel {
		t.Error("shareSessionInChannel = false, want true")
	}
}

func TestNew_QQGroupOptions(t *testing.T) {
	p, err := New(map[string]any{
		"group_reply_all":         false,
		"group_context_messages":  int64(3),
		"group_context_max_chars": int64(1200),
	})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	platform := p.(*Platform)
	if platform.groupReplyAll {
		t.Error("groupReplyAll = true, want false")
	}
	if platform.groupContextMessages != 3 {
		t.Errorf("groupContextMessages = %d, want 3", platform.groupContextMessages)
	}
	if platform.groupContextMaxChars != 1200 {
		t.Errorf("groupContextMaxChars = %d, want 1200", platform.groupContextMaxChars)
	}
}

func TestNew_QQGroupReplyAllDefaultsToExistingBehavior(t *testing.T) {
	p, err := New(map[string]any{})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	platform := p.(*Platform)
	if !platform.groupReplyAll {
		t.Error("groupReplyAll = false, want true")
	}
}

func TestHandleMessage_QQGroupRequiresMentionWhenReplyAllDisabled(t *testing.T) {
	handled := make(chan *core.Message, 1)
	p := newQQTestPlatform(func(_ core.Platform, msg *core.Message) {
		handled <- msg
	})
	p.groupReplyAll = false

	p.handleMessage(qqPayload(1, 100, 7, "alice", []any{
		qqText("hello everyone"),
	}))
	select {
	case msg := <-handled:
		t.Fatalf("unexpected message handled: %+v", msg)
	default:
	}

	p.handleMessage(qqPayload(2, 100, 7, "alice", []any{
		qqAt(p.selfID),
		qqText(" please check"),
	}))
	select {
	case msg := <-handled:
		if msg.Content != "please check" {
			t.Fatalf("Content = %q, want %q", msg.Content, "please check")
		}
	case <-time.After(time.Second):
		t.Fatal("mentioned group message was not handled")
	}
}

func TestHandleMessage_QQRecentGroupContextDedupesPerSession(t *testing.T) {
	handled := make(chan *core.Message, 3)
	p := newQQTestPlatform(func(_ core.Platform, msg *core.Message) {
		handled <- msg
	})
	p.groupReplyAll = false
	p.groupContextMessages = 2
	p.groupContextMaxChars = 1000

	p.handleMessage(qqPayload(1, 100, 7, "alice", []any{qqText("first")}))
	p.handleMessage(qqPayload(2, 100, 8, "bob", []any{qqText("second")}))
	p.handleMessage(qqPayload(3, 100, 9, "carol", []any{qqText("third")}))
	p.handleMessage(qqPayload(4, 100, 7, "alice", []any{qqAt(p.selfID), qqText(" summarize")}))

	first := receiveQQMessage(t, handled)
	if first.Content != "summarize" {
		t.Fatalf("Content = %q, want summarize", first.Content)
	}
	if strings.Contains(first.ExtraContent, "first") {
		t.Fatalf("ExtraContent should keep only the last 2 messages, got %q", first.ExtraContent)
	}
	if !strings.Contains(first.ExtraContent, "second") || !strings.Contains(first.ExtraContent, "third") {
		t.Fatalf("ExtraContent missing recent messages: %q", first.ExtraContent)
	}

	p.handleMessage(qqPayload(5, 100, 7, "alice", []any{qqAt(p.selfID), qqText(" again")}))
	second := receiveQQMessage(t, handled)
	if second.ExtraContent != "" {
		t.Fatalf("ExtraContent = %q, want empty after dedupe", second.ExtraContent)
	}

	p.handleMessage(qqPayload(6, 100, 8, "bob", []any{qqText("fourth")}))
	p.handleMessage(qqPayload(7, 100, 7, "alice", []any{qqAt(p.selfID), qqText(" now")}))
	third := receiveQQMessage(t, handled)
	if !strings.Contains(third.ExtraContent, "fourth") {
		t.Fatalf("ExtraContent should include new context only, got %q", third.ExtraContent)
	}
	if strings.Contains(third.ExtraContent, "second") || strings.Contains(third.ExtraContent, "third") {
		t.Fatalf("ExtraContent repeated old context: %q", third.ExtraContent)
	}
}

func newQQTestPlatform(handler core.MessageHandler) *Platform {
	p := &Platform{
		selfID:        42,
		groupReplyAll: true,
		handler:       handler,
	}
	p.groupNameCache.Store(strconv.FormatInt(100, 10), "Test Group")
	return p
}

func receiveQQMessage(t *testing.T, ch <-chan *core.Message) *core.Message {
	t.Helper()
	select {
	case msg := <-ch:
		return msg
	case <-time.After(time.Second):
		t.Fatal("message was not handled")
	}
	return nil
}

func qqPayload(messageID, groupID, userID int64, userName string, message []any) map[string]any {
	return map[string]any{
		"post_type":    "message",
		"message_type": "group",
		"message_id":   float64(messageID),
		"group_id":     float64(groupID),
		"user_id":      float64(userID),
		"time":         float64(time.Now().Unix()),
		"sender": map[string]any{
			"nickname": userName,
		},
		"message": message,
	}
}

func qqText(text string) map[string]any {
	return map[string]any{"type": "text", "data": map[string]any{"text": text}}
}

func qqAt(qq int64) map[string]any {
	return map[string]any{"type": "at", "data": map[string]any{"qq": strconv.FormatInt(qq, 10)}}
}

// verify Platform implements core.Platform
var _ core.Platform = (*Platform)(nil)
