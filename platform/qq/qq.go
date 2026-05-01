package qq

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/chenhg5/cc-connect/core"
	"github.com/gorilla/websocket"
)

func init() {
	core.RegisterPlatform("qq", New)
}

// Platform connects to a OneBot v11 implementation (NapCat, LLOneBot, etc.)
// via forward WebSocket. It receives message events and sends messages back
// through the same WS connection.
type Platform struct {
	wsURL                 string // e.g. "ws://127.0.0.1:3001"
	token                 string // optional access_token
	allowFrom             string // comma-separated user IDs or "*"
	groupReplyAll         bool
	shareSessionInChannel bool
	groupContextMessages  int
	groupContextMaxChars  int
	handler               core.MessageHandler
	conn                  *websocket.Conn
	mu                    sync.Mutex
	echoSeq               atomic.Int64
	echoCh                sync.Map // echo -> chan json.RawMessage
	cancel                context.CancelFunc
	selfID                int64
	dedup                 core.MessageDedup
	groupNameCache        sync.Map // groupID -> group name
	groupContextMu        sync.Mutex
	groupContextSeq       uint64
	groupContext          map[int64][]groupContextItem
	groupContextLastSeq   map[string]uint64
}

const (
	defaultGroupContextMaxChars = 2000
	maxGroupContextHistory      = 200
)

type groupContextItem struct {
	seq      uint64
	userID   int64
	userName string
	text     string
}

func New(opts map[string]any) (core.Platform, error) {
	wsURL, _ := opts["ws_url"].(string)
	if wsURL == "" {
		wsURL = "ws://127.0.0.1:3001"
	}
	token, _ := opts["token"].(string)
	allowFrom, _ := opts["allow_from"].(string)
	groupReplyAll := true
	if v, ok := opts["group_reply_all"].(bool); ok {
		groupReplyAll = v
	}
	shareSessionInChannel, _ := opts["share_session_in_channel"].(bool)
	groupContextMessages := intOption(opts, "group_context_messages", 0)
	if groupContextMessages < 0 {
		groupContextMessages = 0
	}
	groupContextMaxChars := intOption(opts, "group_context_max_chars", defaultGroupContextMaxChars)
	if groupContextMaxChars < 0 {
		groupContextMaxChars = 0
	}

	core.CheckAllowFrom("qq", allowFrom)
	return &Platform{
		wsURL:                 wsURL,
		token:                 token,
		allowFrom:             allowFrom,
		groupReplyAll:         groupReplyAll,
		shareSessionInChannel: shareSessionInChannel,
		groupContextMessages:  groupContextMessages,
		groupContextMaxChars:  groupContextMaxChars,
	}, nil
}

func (p *Platform) Name() string { return "qq" }

func (p *Platform) Start(handler core.MessageHandler) error {
	p.handler = handler

	header := http.Header{}
	if p.token != "" {
		header.Set("Authorization", "Bearer "+p.token)
	}

	conn, _, err := websocket.DefaultDialer.Dial(p.wsURL, header)
	if err != nil {
		return fmt.Errorf("qq: ws connect failed (%s): %w", p.wsURL, err)
	}
	p.conn = conn

	slog.Info("qq: connected to OneBot", "url", p.wsURL)

	ctx, cancel := context.WithCancel(context.Background())
	p.cancel = cancel

	go p.readLoop(ctx)

	// Get bot self info
	if info, err := p.callAPI("get_login_info", nil); err == nil {
		if uid, ok := info["user_id"].(float64); ok {
			p.selfID = int64(uid)
		}
		nick, _ := info["nickname"].(string)
		slog.Info("qq: logged in", "qq", p.selfID, "nickname", nick)
	} else {
		slog.Warn("qq: get_login_info failed", "error", err)
	}
	if !p.groupReplyAll && p.selfID == 0 {
		slog.Warn("qq: group_reply_all=false requires get_login_info to return bot user_id for @ detection")
	}

	return nil
}

func (p *Platform) readLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		_, raw, err := p.conn.ReadMessage()
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			slog.Error("qq: ws read error, reconnecting...", "error", err)
			p.reconnect()
			continue
		}

		var payload map[string]any
		if json.Unmarshal(raw, &payload) != nil {
			continue
		}

		// If this is an API response (has "echo" field), route to caller
		if echo, ok := payload["echo"].(string); ok {
			if ch, loaded := p.echoCh.LoadAndDelete(echo); loaded {
				if dataCh, ok := ch.(chan json.RawMessage); ok {
					dataCh <- raw
				}
			}
			continue
		}

		// Otherwise it's an event
		postType, _ := payload["post_type"].(string)
		if postType == "message" {
			go p.handleMessage(payload)
		}
	}
}

func (p *Platform) reconnect() {
	for i := 1; i <= 30; i++ {
		time.Sleep(time.Duration(i) * 2 * time.Second)
		header := http.Header{}
		if p.token != "" {
			header.Set("Authorization", "Bearer "+p.token)
		}
		conn, _, err := websocket.DefaultDialer.Dial(p.wsURL, header)
		if err != nil {
			slog.Warn("qq: reconnect attempt failed", "attempt", i, "error", err)
			continue
		}
		p.mu.Lock()
		p.conn = conn
		p.mu.Unlock()
		slog.Info("qq: reconnected")
		return
	}
	slog.Error("qq: failed to reconnect after 30 attempts")
}

func (p *Platform) handleMessage(payload map[string]any) {
	msgType, _ := payload["message_type"].(string)
	userID := jsonInt64(payload, "user_id")
	groupID := jsonInt64(payload, "group_id")
	messageID := jsonInt64(payload, "message_id")

	if userID == p.selfID {
		return
	}

	if ts, ok := payload["time"].(float64); ok && ts > 0 {
		if core.IsOldMessage(time.Unix(int64(ts), 0)) {
			slog.Debug("qq: ignoring old message after restart", "time", int64(ts))
			return
		}
	}

	msgIDStr := strconv.FormatInt(messageID, 10)
	if p.dedup.IsDuplicate(msgIDStr) {
		slog.Debug("qq: duplicate message ignored", "message_id", messageID)
		return
	}

	if !p.isAllowed(userID) {
		return
	}

	// Extract sender info
	var userName string
	if sender, ok := payload["sender"].(map[string]any); ok {
		card, _ := sender["card"].(string)
		nick, _ := sender["nickname"].(string)
		if card != "" {
			userName = card
		} else {
			userName = nick
		}
	}

	// Parse message content from CQ message array or raw_message
	text, images, audio := p.parseMessage(payload)
	if text == "" && len(images) == 0 && audio == nil {
		return
	}

	directedAtBot := false
	if msgType == "group" {
		directedAtBot = p.isDirectedAtBot(payload)
		if !p.groupReplyAll && !directedAtBot {
			p.recordGroupContext(groupID, userID, userName, text)
			return
		}
	}

	var sessionKey string
	if msgType == "group" {
		if p.shareSessionInChannel {
			sessionKey = fmt.Sprintf("qq:g:%d", groupID)
		} else {
			sessionKey = fmt.Sprintf("qq:%d:%d", groupID, userID)
		}
	} else {
		sessionKey = fmt.Sprintf("qq:%d", userID)
	}

	rctx := &replyContext{
		messageType: msgType,
		userID:      userID,
		groupID:     groupID,
		messageID:   int32(messageID),
	}

	var chatName string
	if msgType == "group" {
		chatName = p.resolveGroupName(groupID)
	}

	extraContent := ""
	if msgType == "group" && !p.groupReplyAll && directedAtBot {
		extraContent = p.buildRecentGroupContext(groupID, sessionKey)
	}

	msg := &core.Message{
		SessionKey:   sessionKey,
		Platform:     "qq",
		MessageID:    strconv.FormatInt(messageID, 10),
		UserID:       strconv.FormatInt(userID, 10),
		UserName:     userName,
		ChatName:     chatName,
		Content:      text,
		Images:       images,
		Audio:        audio,
		ExtraContent: extraContent,
		ReplyCtx:     rctx,
	}

	slog.Debug("qq: message received", "type", msgType, "user", userID, "text_len", len(text))
	p.handler(p, msg)
}

func (p *Platform) parseMessage(payload map[string]any) (string, []core.ImageAttachment, *core.AudioAttachment) {
	var textParts []string
	var images []core.ImageAttachment
	var audio *core.AudioAttachment

	// OneBot message can be array of segments or a string
	switch msg := payload["message"].(type) {
	case []any:
		for _, seg := range msg {
			s, ok := seg.(map[string]any)
			if !ok {
				continue
			}
			segType, _ := s["type"].(string)
			data, _ := s["data"].(map[string]any)
			if data == nil {
				continue
			}

			switch segType {
			case "text":
				if text, ok := data["text"].(string); ok {
					textParts = append(textParts, text)
				}
			case "image":
				if url, ok := data["url"].(string); ok && url != "" {
					imgData, mime, err := downloadFile(url)
					if err != nil {
						slog.Warn("qq: download image failed", "error", err)
						continue
					}
					images = append(images, core.ImageAttachment{
						MimeType: mime,
						Data:     imgData,
					})
				}
			case "record":
				if url, ok := data["url"].(string); ok && url != "" {
					audioData, _, err := downloadFile(url)
					if err != nil {
						slog.Warn("qq: download audio failed", "error", err)
						continue
					}
					format := "silk"
					if f, ok := data["file"].(string); ok {
						if strings.HasSuffix(f, ".amr") {
							format = "amr"
						} else if strings.HasSuffix(f, ".mp3") {
							format = "mp3"
						}
					}
					audio = &core.AudioAttachment{
						Data:   audioData,
						Format: format,
					}
				}
			case "at":
				// Ignore @mentions in parsed text
			}
		}
	default:
		// raw_message fallback (string with CQ codes)
		if raw, ok := payload["raw_message"].(string); ok {
			textParts = append(textParts, stripCQCodes(raw))
		}
	}

	return strings.TrimSpace(strings.Join(textParts, "")), images, audio
}

func (p *Platform) isDirectedAtBot(payload map[string]any) bool {
	if p.selfID == 0 {
		return false
	}
	self := strconv.FormatInt(p.selfID, 10)
	switch msg := payload["message"].(type) {
	case []any:
		for _, seg := range msg {
			s, ok := seg.(map[string]any)
			if !ok {
				continue
			}
			segType, _ := s["type"].(string)
			if segType != "at" {
				continue
			}
			data, _ := s["data"].(map[string]any)
			if valueString(data["qq"]) == self {
				return true
			}
		}
	}
	if raw, ok := payload["raw_message"].(string); ok {
		return rawMessageMentions(raw, self)
	}
	return false
}

func (p *Platform) recordGroupContext(groupID int64, userID int64, userName, text string) {
	text = strings.TrimSpace(text)
	if p.groupContextMessages <= 0 || groupID == 0 || text == "" {
		return
	}

	p.groupContextMu.Lock()
	defer p.groupContextMu.Unlock()
	if p.groupContext == nil {
		p.groupContext = make(map[int64][]groupContextItem)
	}
	p.groupContextSeq++
	p.groupContext[groupID] = append(p.groupContext[groupID], groupContextItem{
		seq:      p.groupContextSeq,
		userID:   userID,
		userName: userName,
		text:     text,
	})

	limit := maxGroupContextHistory
	if p.groupContextMessages*4 > limit {
		limit = p.groupContextMessages * 4
	}
	items := p.groupContext[groupID]
	if len(items) > limit {
		p.groupContext[groupID] = items[len(items)-limit:]
	}
}

func (p *Platform) buildRecentGroupContext(groupID int64, sessionKey string) string {
	if p.groupContextMessages <= 0 || groupID == 0 || sessionKey == "" {
		return ""
	}

	p.groupContextMu.Lock()
	lastSeq := p.groupContextLastSeq[sessionKey]
	history := p.groupContext[groupID]
	items := make([]groupContextItem, 0, p.groupContextMessages)
	for i := len(history) - 1; i >= 0 && len(items) < p.groupContextMessages; i-- {
		item := history[i]
		if item.seq <= lastSeq {
			continue
		}
		items = append(items, item)
	}
	p.groupContextMu.Unlock()

	if len(items) == 0 {
		return ""
	}
	reverseGroupContextItems(items)
	contextText, maxSeq := formatGroupContext(items, p.groupContextMaxChars)
	if maxSeq == 0 {
		return ""
	}

	p.groupContextMu.Lock()
	if p.groupContextLastSeq == nil {
		p.groupContextLastSeq = make(map[string]uint64)
	}
	if p.groupContextLastSeq[sessionKey] < maxSeq {
		p.groupContextLastSeq[sessionKey] = maxSeq
	}
	p.groupContextMu.Unlock()
	return contextText
}

// Reply sends a message as a reply to an incoming message.
func (p *Platform) Reply(ctx context.Context, replyCtx any, content string) error {
	return p.Send(ctx, replyCtx, content)
}

// Send sends a message to the conversation identified by replyCtx.
func (p *Platform) Send(ctx context.Context, replyCtx any, content string) error {
	rctx, ok := replyCtx.(*replyContext)
	if !ok {
		return fmt.Errorf("qq: invalid reply context")
	}

	params := map[string]any{
		"message": content,
	}

	if rctx.messageType == "group" {
		params["group_id"] = rctx.groupID
		_, err := p.callAPI("send_group_msg", params)
		return err
	}

	params["user_id"] = rctx.userID
	_, err := p.callAPI("send_private_msg", params)
	return err
}

// SendImage sends an image to the conversation.
// Implements core.ImageSender.
func (p *Platform) SendImage(ctx context.Context, replyCtx any, img core.ImageAttachment) error {
	rctx, ok := replyCtx.(*replyContext)
	if !ok {
		return fmt.Errorf("qq: SendImage: invalid reply context type %T", replyCtx)
	}

	b64 := base64.StdEncoding.EncodeToString(img.Data)
	segments := []map[string]any{
		{"type": "image", "data": map[string]any{"file": "base64://" + b64}},
	}

	params := map[string]any{
		"message": segments,
	}

	if rctx.messageType == "group" {
		params["group_id"] = rctx.groupID
		_, err := p.callAPI("send_group_msg", params)
		if err != nil {
			return fmt.Errorf("qq: send image: %w", err)
		}
		return nil
	}

	params["user_id"] = rctx.userID
	_, err := p.callAPI("send_private_msg", params)
	if err != nil {
		return fmt.Errorf("qq: send image: %w", err)
	}
	return nil
}

var _ core.ImageSender = (*Platform)(nil)

func (p *Platform) Stop() error {
	if p.cancel != nil {
		p.cancel()
	}
	if p.conn != nil {
		return p.conn.Close()
	}
	return nil
}

func (p *Platform) resolveGroupName(groupID int64) string {
	if groupID == 0 {
		return ""
	}
	fallback := strconv.FormatInt(groupID, 10)
	if cached, ok := p.groupNameCache.Load(fallback); ok {
		return cached.(string)
	}
	result, err := p.callAPI("get_group_info", map[string]any{"group_id": groupID})
	if err != nil {
		slog.Debug("qq: resolve group name failed", "group_id", groupID, "error", err)
		return fallback
	}
	name, _ := result["group_name"].(string)
	if name != "" {
		p.groupNameCache.Store(fallback, name)
		return name
	}
	return fallback
}

// ── OneBot API call via WebSocket ───────────────────────────────

func (p *Platform) callAPI(action string, params map[string]any) (map[string]any, error) {
	seq := p.echoSeq.Add(1)
	echo := strconv.FormatInt(seq, 10)

	req := map[string]any{
		"action": action,
		"echo":   echo,
	}
	if params != nil {
		req["params"] = params
	}

	ch := make(chan json.RawMessage, 1)
	p.echoCh.Store(echo, ch)
	defer p.echoCh.Delete(echo)

	data, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}

	p.mu.Lock()
	err = p.conn.WriteMessage(websocket.TextMessage, data)
	p.mu.Unlock()
	if err != nil {
		return nil, fmt.Errorf("qq: ws write: %w", err)
	}

	select {
	case raw := <-ch:
		var resp struct {
			Status  string          `json:"status"`
			RetCode int             `json:"retcode"`
			Data    json.RawMessage `json:"data"`
		}
		if json.Unmarshal(raw, &resp) != nil {
			return nil, fmt.Errorf("qq: invalid API response")
		}
		if resp.RetCode != 0 {
			return nil, fmt.Errorf("qq: API %s failed (retcode=%d)", action, resp.RetCode)
		}
		var result map[string]any
		_ = json.Unmarshal(resp.Data, &result)
		return result, nil

	case <-time.After(15 * time.Second):
		return nil, fmt.Errorf("qq: API %s timeout", action)
	}
}

// ── Helpers ─────────────────────────────────────────────────────

type replyContext struct {
	messageType string // "private" or "group"
	userID      int64
	groupID     int64
	messageID   int32
}

func (p *Platform) ReconstructReplyCtx(sessionKey string) (any, error) {
	// qq:{userID}, qq:{groupID}:{userID} or qq:g:{groupID}
	parts := strings.SplitN(sessionKey, ":", 3)
	if len(parts) < 2 || parts[0] != "qq" {
		return nil, fmt.Errorf("qq: invalid session key %q", sessionKey)
	}
	if len(parts) == 3 {
		if parts[1] == "g" {
			gid, _ := strconv.ParseInt(parts[2], 10, 64)
			return &replyContext{messageType: "group", groupID: gid}, nil
		}
		gid, _ := strconv.ParseInt(parts[1], 10, 64)
		uid, _ := strconv.ParseInt(parts[2], 10, 64)
		return &replyContext{messageType: "group", groupID: gid, userID: uid}, nil
	}
	uid, _ := strconv.ParseInt(parts[1], 10, 64)
	return &replyContext{messageType: "private", userID: uid}, nil
}

func (p *Platform) isAllowed(userID int64) bool {
	if p.allowFrom == "" || p.allowFrom == "*" {
		return true
	}
	uid := strconv.FormatInt(userID, 10)
	for _, allowed := range strings.Split(p.allowFrom, ",") {
		if strings.TrimSpace(allowed) == uid {
			return true
		}
	}
	return false
}

func intOption(opts map[string]any, key string, fallback int) int {
	switch v := opts[key].(type) {
	case int:
		return v
	case int8:
		return int(v)
	case int16:
		return int(v)
	case int32:
		return int(v)
	case int64:
		return int(v)
	case uint:
		return int(v)
	case uint8:
		return int(v)
	case uint16:
		return int(v)
	case uint32:
		return int(v)
	case uint64:
		return int(v)
	case float64:
		return int(v)
	case json.Number:
		if n, err := v.Int64(); err == nil {
			return int(n)
		}
	}
	return fallback
}

func valueString(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case float64:
		return strconv.FormatInt(int64(x), 10)
	case json.Number:
		if n, err := x.Int64(); err == nil {
			return strconv.FormatInt(n, 10)
		}
	case int:
		return strconv.Itoa(x)
	case int64:
		return strconv.FormatInt(x, 10)
	case uint64:
		return strconv.FormatUint(x, 10)
	}
	return ""
}

func rawMessageMentions(raw, qq string) bool {
	if raw == "" || qq == "" {
		return false
	}
	for {
		idx := strings.Index(raw, "[CQ:at,")
		if idx < 0 {
			return false
		}
		end := strings.Index(raw[idx:], "]")
		if end < 0 {
			return false
		}
		code := raw[idx : idx+end+1]
		target := "qq=" + qq
		if strings.Contains(code, target+",") || strings.Contains(code, target+"]") {
			return true
		}
		raw = raw[idx+end+1:]
	}
}

func reverseGroupContextItems(items []groupContextItem) {
	for i, j := 0, len(items)-1; i < j; i, j = i+1, j-1 {
		items[i], items[j] = items[j], items[i]
	}
}

func formatGroupContext(items []groupContextItem, maxChars int) (string, uint64) {
	const header = "Recent QQ group messages not previously sent to the agent:"
	lines := make([]string, 0, len(items))
	total := len([]rune(header))
	var maxSeq uint64

	for i := len(items) - 1; i >= 0; i-- {
		line := formatGroupContextLine(items[i])
		lineLen := len([]rune(line)) + 1
		if maxChars > 0 && total+lineLen > maxChars {
			if len(lines) == 0 {
				remaining := maxChars - total - 1
				if remaining <= 0 {
					break
				}
				line = truncateRunes(line, remaining)
			} else {
				break
			}
		}
		lines = append(lines, line)
		total += len([]rune(line)) + 1
		if items[i].seq > maxSeq {
			maxSeq = items[i].seq
		}
	}
	if len(lines) == 0 {
		return "", 0
	}
	for i, j := 0, len(lines)-1; i < j; i, j = i+1, j-1 {
		lines[i], lines[j] = lines[j], lines[i]
	}
	return header + "\n" + strings.Join(lines, "\n"), maxSeq
}

func formatGroupContextLine(item groupContextItem) string {
	name := strings.TrimSpace(item.userName)
	if name == "" {
		name = strconv.FormatInt(item.userID, 10)
	}
	text := strings.Join(strings.Fields(item.text), " ")
	return fmt.Sprintf("- %s (%d): %s", name, item.userID, text)
}

func truncateRunes(s string, max int) string {
	if max <= 0 {
		return ""
	}
	runes := []rune(s)
	if len(runes) <= max {
		return s
	}
	if max <= 3 {
		return string(runes[:max])
	}
	return string(runes[:max-3]) + "..."
}

func jsonInt64(m map[string]any, key string) int64 {
	switch v := m[key].(type) {
	case float64:
		return int64(v)
	case json.Number:
		n, _ := v.Int64()
		return n
	}
	return 0
}

func stripCQCodes(s string) string {
	var result strings.Builder
	for len(s) > 0 {
		idx := strings.Index(s, "[CQ:")
		if idx < 0 {
			result.WriteString(s)
			break
		}
		result.WriteString(s[:idx])
		end := strings.Index(s[idx:], "]")
		if end < 0 {
			break
		}
		s = s[idx+end+1:]
	}
	return result.String()
}

func downloadFile(url string) ([]byte, string, error) {
	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		return nil, "", err
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, "", err
	}

	mime := resp.Header.Get("Content-Type")
	if mime == "" {
		mime = http.DetectContentType(data)
	}
	return data, mime, nil
}
