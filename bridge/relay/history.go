package relay

import (
	"sync"

	"github.com/sttts/claw64/bridge/llm"
)

// History stores per-user conversation histories, safe for concurrent use.
type History struct {
	mu    sync.Mutex
	convs map[string][]llm.Message
}

// NewHistory returns an initialized History.
func NewHistory() *History {
	return &History{convs: make(map[string][]llm.Message)}
}

// Append adds a message to the user's conversation history.
func (h *History) Append(userID string, msg llm.Message) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.convs[userID] = append(h.convs[userID], msg)
}

// Get returns a copy of the user's conversation history.
func (h *History) Get(userID string) []llm.Message {
	h.mu.Lock()
	defer h.mu.Unlock()
	msgs := h.convs[userID]
	out := make([]llm.Message, len(msgs))
	copy(out, msgs)
	return out
}

// ReplaceLast overwrites the most recent message in a user's history.
func (h *History) ReplaceLast(userID string, msg llm.Message) {
	h.mu.Lock()
	defer h.mu.Unlock()
	msgs := h.convs[userID]
	if len(msgs) == 0 {
		h.convs[userID] = []llm.Message{msg}
		return
	}
	msgs[len(msgs)-1] = msg
}

// Clear removes all messages for a user.
func (h *History) Clear(userID string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.convs, userID)
}
