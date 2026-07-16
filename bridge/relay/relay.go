// Package relay routes messages between chat, LLM, and the C64 serial link.
// It is not an agent — the C64 is the agent. The relay just forwards.
package relay

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/sttts/claw64/bridge/llm"
	"github.com/sttts/claw64/bridge/serial"
	"github.com/sttts/claw64/bridge/termstyle"
)

// The relay loop stays active while immediate C64/LLM work remains.
// User-visible TEXT is optional; silence is a valid completion.

// Relay routes messages between chat, LLM, and the C64 serial link.
// The C64 drives the agent loop; the relay just forwards.
type Relay struct {
	Link             *serial.Link
	LLM              llm.Completer
	History          *History
	DebugDir         string
	MonitorAddr      string
	SymbolPath       string
	StreamOut        io.Writer // streaming progress output (default: os.Stderr)
	SystemPrompt     string    // received from C64
	promptChunks     map[int]string
	resultChunks     map[int]string
	textBuf          []byte // accumulates multi-frame TEXT chunks (receive)
	textOutQueue     []byte // pending TEXT data to send in chunks (send)
	textInFlight     []byte // current TEXT chunk sent, waiting for forwarded ack
	textAckWaitID    byte
	textAckWaitSince time.Time
	textAckWaitSeen  int
	textAckWaitLast  string
	toolInFlight     []byte // current tool payload sent, waiting for RESULT/ERROR
	waitingTool      bool
	lastToolCallID   string
	lastToolName     string
	toolStartedAt    time.Time
	basicRunning     bool
	completionDrain  bool   // hold TEXT while RUNNING→RESULT drains
	textDrainPending bool   // log TEXT handoff after tool-completion drain
	nextTxID         byte   // bridge→C64 transport ID counter
	rxLastID         byte   // newest accepted C64→bridge transport ID
	rxLastType       byte   // frame type of newest accepted C64→bridge frame
	pendingAcks      []byte // queued ACK IDs to send during next quiet window
	pendingFrames    []queuedFrame
	lateAckIDs       map[byte]struct{}
	lastAckSentAt    time.Time
	lastC64FrameAt   time.Time
	recvOnce         sync.Once
	ackCh            chan serial.Frame
	recvCh           chan recvResult
	ackWaitMu        sync.Mutex
	ackWaiters       map[byte]chan serial.Frame
	txMu             sync.Mutex
	overlapMu        sync.Mutex
	overlapBusy      bool
	msgGateMu        sync.Mutex
	msgGateBusy      bool
	msgGateWaiters   []chan struct{}
	overflowMu       sync.Mutex
	overflowBusy     bool
	overflowWaiters  []chan struct{}
	msgRetryBase     time.Duration
	msgRetryMax      time.Duration
	DeliveryRetry    func(name string, attempt int, err error)
	C64Duplicate     func(name string, id byte, newest byte)
}

type recvResult struct {
	frame serial.Frame
	err   error
}

type queuedFrame struct {
	frame    serial.Frame
	accepted bool
}

const textChunkMax = 62
const bridgeFramePayloadMax = 0x7B
const bridgeFrameBodyMax = bridgeFramePayloadMax - 1

// ackTimeout is the universal timeout for any reliable frame ACK.
// With ID-based delivery, ACKs always arrive promptly regardless of
// whether BASIC is running — the C64 ACKs at transport level.
const ackTimeout = 3 * time.Second
const toolFrameTimeout = 5 * time.Second
const ackQuietWindow = 800 * time.Millisecond
const c64AckHandoffQuietWindow = 800 * time.Millisecond
const c64TextHandoffQuietWindow = 1200 * time.Millisecond
const c64LLMEventBatchQuietWindow = c64AckHandoffQuietWindow + 500*time.Millisecond
const runningAckQuietWindow = 3 * time.Second
const runningRecvQuietWindow = 1 * time.Second
const runningStatusPollInterval = 3 * time.Second
const gateHandoffQuietWindow = 250 * time.Millisecond
const queuedGateHandoffQuietWindow = runningRecvQuietWindow
const c64UserQueueSlots = 3

// Tool-result labels are adapter envelopes for backend chat APIs. They are
// not agent instructions; behavior policy must stay in the C64 soul.
const (
	toolResultScreenPrefix     = "[C64 screen output]: "
	toolResultScreenshotPrefix = "[C64 text screen screenshot]: "
	toolResultStatusPrefix     = "[C64 BASIC status]: "
	toolResultTimeout          = "ERROR: command timed out on C64"
)

// textAckTimeout allows one inbound TEXT chunk, the resulting USER frame,
// and the bridge ACK back to the C64 to drain at 2400 baud before retrying.
const textAckTimeout = 7 * time.Second

// msgAdmissionAckTimeout waits for the C64 to admit a user message into its
// event queue. This is queue admission, not ordinary idle transport ACK
// latency, because previous C64 TX/ACK handoff can still be draining.
const msgAdmissionAckTimeout = c64FrameTimeout

var reliableRetryDelays = []time.Duration{
	500 * time.Millisecond,
	1 * time.Second,
	2 * time.Second,
	3 * time.Second,
	5 * time.Second,
}

var execRetryDelays = []time.Duration{
	1 * time.Second,
	2 * time.Second,
	3 * time.Second,
	5 * time.Second,
	8 * time.Second,
}

func (r *Relay) frameWaitTimeout(waitingTextAck bool) time.Duration {
	if waitingTextAck {
		return textAckTimeout
	}
	if r.waitingTool {
		return toolFrameTimeout
	}
	if r.basicRunning {
		return 0
	}
	return c64FrameTimeout
}

func isExpectedStatus(status string) bool {
	switch status {
	case "RUNNING", "READY", "READY.", "STORED", "BUSY", "STOP REQUESTED", "UNKNOWN":
		return true
	}
	return false
}

// c64FrameTimeout is how long the bridge waits for any C64 frame before
// triggering a generic stall dump. It must stay above execAckTimeout so a
// slow transport retry does not produce a misleading silence dump.
const c64FrameTimeout = 12 * time.Second

var llmTools = []llm.Tool{
	llm.BasicExecTool,
	llm.TextScreenshotTool,
	llm.BasicStopTool,
	llm.BasicStatusTool,
}

func (r *Relay) messageRetryBase() time.Duration {
	if r.msgRetryBase > 0 {
		return r.msgRetryBase
	}
	return 100 * time.Millisecond
}

func (r *Relay) messageRetryMax() time.Duration {
	if r.msgRetryMax > 0 {
		return r.msgRetryMax
	}
	return 1 * time.Second
}

func (r *Relay) acquireMessageGate(ctx context.Context) error {
	r.msgGateMu.Lock()
	if !r.msgGateBusy {
		r.msgGateBusy = true
		r.msgGateMu.Unlock()
		return nil
	}
	waiter := make(chan struct{})
	r.msgGateWaiters = append(r.msgGateWaiters, waiter)
	r.msgGateMu.Unlock()

	select {
	case <-ctx.Done():
		r.msgGateMu.Lock()
		for i, ch := range r.msgGateWaiters {
			if ch == waiter {
				r.msgGateWaiters = append(r.msgGateWaiters[:i], r.msgGateWaiters[i+1:]...)
				break
			}
		}
		r.msgGateMu.Unlock()
		return ctx.Err()
	case <-waiter:
		if delay := r.messageGateHandoffDelay(); delay > 0 {
			time.Sleep(delay)
		}
		return nil
	}
}

func (r *Relay) tryAcquireMessageGate() bool {
	r.msgGateMu.Lock()
	defer r.msgGateMu.Unlock()
	if r.msgGateBusy {
		return false
	}
	r.msgGateBusy = true
	return true
}

func (r *Relay) canSendOverlappingMessage() bool {
	if r.SystemPrompt == "" {
		return false
	}
	if r.completionDrain {
		return true
	}
	return r.basicRunning && !r.waitingTool
}

func (r *Relay) canStartRunningOverlap() bool {
	if !r.basicRunning || r.waitingTool {
		return false
	}

	r.msgGateMu.Lock()
	defer r.msgGateMu.Unlock()

	return r.msgGateBusy && len(r.msgGateWaiters) == 0
}

func (r *Relay) releaseMessageGate() {
	r.msgGateMu.Lock()
	if len(r.msgGateWaiters) > 0 {
		waiter := r.msgGateWaiters[0]
		r.msgGateWaiters = r.msgGateWaiters[1:]
		r.msgGateMu.Unlock()
		close(waiter)
		return
	}
	r.msgGateBusy = false
	r.msgGateMu.Unlock()
}

func (r *Relay) hasQueuedMessageWaiter() bool {
	r.msgGateMu.Lock()
	defer r.msgGateMu.Unlock()

	return len(r.msgGateWaiters) > 0
}

func (r *Relay) overlapQueueDepth() int {
	r.msgGateMu.Lock()
	depth := len(r.msgGateWaiters)
	if r.msgGateBusy {
		depth++
	}
	r.msgGateMu.Unlock()

	r.overlapMu.Lock()
	if r.overlapBusy {
		depth++
	}
	r.overlapMu.Unlock()

	return depth
}

func (r *Relay) overlapQueueAtCapacity() bool {
	return r.overlapQueueDepth() >= c64UserQueueSlots
}

func (r *Relay) tryAcquireOverlapSend() bool {
	r.overlapMu.Lock()
	defer r.overlapMu.Unlock()
	if r.overlapBusy {
		return false
	}
	r.overlapBusy = true
	return true
}

func (r *Relay) releaseOverlapSend() {
	r.overlapMu.Lock()
	r.overlapBusy = false
	r.overlapMu.Unlock()
}

// handleSystemFrame assembles SYSTEM prompt chunks from the C64.
func (r *Relay) handleSystemFrame(f serial.Frame) {
	if len(f.Payload) < 2 {
		return
	}
	idx := int(f.Payload[0])
	total := int(f.Payload[1])
	text := string(f.Payload[2:])

	if r.promptChunks == nil {
		r.promptChunks = make(map[int]string)
	}
	r.promptChunks[idx] = text
	log.Printf("%s", flowLine("LLM", "←", "C64", "SYSTEM", fmt.Sprintf("chunk [%d/%d] %d bytes", idx+1, total, len(text))))

	if len(r.promptChunks) == total {
		var prompt string
		for i := 0; i < total; i++ {
			prompt += r.promptChunks[i]
		}
		r.SystemPrompt = prompt
		r.promptChunks = nil
		log.Printf("%s", flowLine("LLM", "←", "C64", "SYSTEM", fmt.Sprintf("received (%d bytes)", len(prompt))))
	}
}

// handleResultFrame assembles chunked RESULT output from the C64.
func (r *Relay) handleResultFrame(f serial.Frame) (string, bool) {
	if len(f.Payload) < 2 {
		return "", false
	}
	idx := int(f.Payload[0])
	total := int(f.Payload[1])
	text := string(f.Payload[2:])
	if r.lastToolName == "screen" || os.Getenv("CLAW64_DEBUG_RESULT_CHUNKS") != "" {
		log.Printf("%s", flowLine("LLM", "←", "C64", "RESULT", fmt.Sprintf("screen chunk %d/%d payload=%s text=%q", idx+1, total, hex.EncodeToString(f.Payload), text)))
	}

	if idx == 0 || r.resultChunks == nil {
		r.resultChunks = make(map[int]string)
	}
	r.resultChunks[idx] = text

	if len(r.resultChunks) != total {
		return "", false
	}

	var result string
	for i := 0; i < total; i++ {
		result += r.resultChunks[i]
	}
	r.resultChunks = nil
	return result, true
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

func (r *Relay) streamOut() io.Writer {
	if r.StreamOut != nil {
		return r.StreamOut
	}
	return os.Stderr
}

// logStream prints a log-style prefix without a trailing newline.
// Characters can be appended to the same line afterwards.
func logStream(w io.Writer, format string, args ...any) {
	ts := time.Now().Format("2006/01/02 15:04:05")
	msg := fmt.Sprintf(format, args...)
	fmt.Fprint(w, termstyle.Dim(fmt.Sprintf("%s %s", ts, msg)))
}

func flowLabel(src, arrow, dst, typ string) string {
	return fmt.Sprintf("%-4s %s %s: %-11s", src, arrow, dst, typ+":")
}

func flowLine(src, arrow, dst, typ, payload string) string {
	label := flowLabel(src, arrow, dst, typ)
	if payload == "" {
		return label
	}
	return label + " " + payload
}

func logWarnf(format string, args ...any) {
	log.Println(termstyle.Warn(fmt.Sprintf(format, args...)))
}

func logErrorf(format string, args ...any) {
	log.Println(termstyle.Error(fmt.Sprintf(format, args...)))
}

// SetupProgress installs a send-progress callback on the serial link
// that prints payload bytes char-by-char as they are sent.
func (r *Relay) SetupProgress() {
	w := r.streamOut()

	r.Link.OnSendByte = func(typeName string, payload []byte, idx int) {
		if typeName == "ACK" || typeName == "ACK_TO_C64" {
			return
		}
		if sendLogSkipsTransportID(typeName) && idx == 0 {
			return
		}
		if idx == -1 {
			fmt.Fprint(w, termstyle.Dim("\n"))
			return
		}
		if payload[idx] == '\n' {
			fmt.Fprint(w, termstyle.Dim(`\n`))
		} else {
			fmt.Fprint(w, termstyle.Dim(string(payload[idx])))
		}
	}

	// Receive-side streaming is limited to SYSTEM chunks. Other semantic
	// frames are logged only after full decode/unwrap so the logs reflect
	// accepted frames rather than raw payload bytes seen before checksum
	// validation and duplicate suppression.
	r.Link.OnRecvByte = func(frameType byte, idx int, b byte) {
		if frameType == serial.FrameSystem {
			if idx == 0 {
				logStream(w, "%s ", flowLabel("LLM", "←", "C64", "SYSTEM"))
			}
			if idx < 3 {
				return
			}
			if b == '\n' {
				fmt.Fprint(w, termstyle.Dim(`\n`))
			} else {
				fmt.Fprint(w, termstyle.Dim(string(b)))
			}
			return
		}
	}
}

// PrimeTransport verifies that the post-handshake serial path can carry a
// request/response frame before user messages enter the C64.
func (r *Relay) PrimeTransport(ctx context.Context) error {
	frame := serial.Frame{Type: serial.FrameStatusReq}
	for attempt := 0; attempt < len(reliableRetryDelays); attempt++ {
		if err := r.sendUnreliableBridgeFrame(frame); err != nil {
			return err
		}
		if err := r.waitForPrimeStatus(ctx, toolFrameTimeout); err == nil {
			return nil
		} else if attempt+1 == len(reliableRetryDelays) {
			return err
		}
		time.Sleep(reliableRetryDelays[attempt])
	}
	return fmt.Errorf("transport prime failed")
}

func sendLogSkipsTransportID(typeName string) bool {
	switch typeName {
	case "MSG", "EXEC", "STOP", "STATUS", "TEXT", "SCREENSHOT":
		return true
	default:
		return false
	}
}

// basicExecArgs is the JSON structure the LLM passes to exec.
type basicExecArgs struct {
	Command string `json:"command"`
}

// HandleMessageStream relays a user message and emits every C64 user-visible
// message through emit. A relay cycle may also complete without any
// user-visible text.
func (r *Relay) HandleMessageStream(ctx context.Context, userID string, text string, emit func(string) error) error {
	text = serial.ToASCII(text)
	if len(text) > bridgeFrameBodyMax {
		text = text[:bridgeFrameBodyMax]
	}

	msgFrame := serial.Frame{Type: serial.FrameMsg, Payload: []byte(text)}
	if r.overlapQueueAtCapacity() {
		if err := r.acquireOverflowLane(ctx); err != nil {
			return fmt.Errorf("wait for overlap overflow lane: %w", err)
		}
		defer r.releaseOverflowLane()
		if err := r.waitForOverlapQueueRoom(ctx); err != nil {
			return fmt.Errorf("wait for overlap queue room: %w", err)
		}
		if err := r.acquireMessageGate(ctx); err != nil {
			return fmt.Errorf("wait for relay idle: %w", err)
		}
		defer r.releaseMessageGate()
		if err := r.sendMSG(ctx, msgFrame); err != nil {
			return fmt.Errorf("send MSG: %w", err)
		}
		return r.eventLoop(ctx, userID, emit)
	}
	if !r.canSendOverlappingMessage() {
		if err := r.acquireMessageGate(ctx); err != nil {
			return fmt.Errorf("wait for relay idle: %w", err)
		}
		defer r.releaseMessageGate()
		if err := r.sendMSG(ctx, msgFrame); err != nil {
			return fmt.Errorf("send MSG: %w", err)
		}
		return r.eventLoop(ctx, userID, emit)
	}

	if r.tryAcquireMessageGate() {
		defer r.releaseMessageGate()
		if err := r.sendMSG(ctx, msgFrame); err != nil {
			return fmt.Errorf("send MSG: %w", err)
		}
		return r.eventLoop(ctx, userID, emit)
	}

	if r.basicRunning && !r.canStartRunningOverlap() {
		if err := r.acquireMessageGate(ctx); err != nil {
			return fmt.Errorf("wait for relay idle: %w", err)
		}
		defer r.releaseMessageGate()
		if err := r.sendMSG(ctx, msgFrame); err != nil {
			return fmt.Errorf("send MSG: %w", err)
		}
		return r.eventLoop(ctx, userID, emit)
	}

	if !r.tryAcquireOverlapSend() {
		if err := r.acquireMessageGate(ctx); err != nil {
			return fmt.Errorf("wait for relay idle: %w", err)
		}
		defer r.releaseMessageGate()
		if err := r.sendMSG(ctx, msgFrame); err != nil {
			return fmt.Errorf("send MSG: %w", err)
		}
		return r.eventLoop(ctx, userID, emit)
	}

	if err := r.sendOverlappingMSG(ctx, msgFrame); err != nil {
		r.releaseOverlapSend()
		return fmt.Errorf("send overlapping MSG: %w", err)
	}
	if err := r.acquireMessageGate(ctx); err != nil {
		r.releaseOverlapSend()
		return fmt.Errorf("wait for relay idle: %w", err)
	}
	r.releaseOverlapSend()
	defer r.releaseMessageGate()
	return r.eventLoop(ctx, userID, emit)
}

func (r *Relay) sendMSG(ctx context.Context, msgFrame serial.Frame) error {
	logStream(r.streamOut(), "%s ", flowLabel("USER", "→", "C64", "MSG"))
	return r.sendVerifiedWithTimeout(ctx, msgFrame, "MSG", msgAdmissionAckTimeout)
}

func (r *Relay) waitForOverlapQueueRoom(ctx context.Context) error {
	ticker := time.NewTicker(r.messageRetryBase())
	defer ticker.Stop()

	for r.overlapQueueAtCapacity() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
	return nil
}

func (r *Relay) sendOverlappingMSG(ctx context.Context, msgFrame serial.Frame) error {
	logStream(r.streamOut(), "%s ", flowLabel("USER", "→", "C64", "MSG"))
	return r.sendVerifiedWithAckWaiter(ctx, msgFrame, "MSG", msgAdmissionAckTimeout)
}

func (r *Relay) acquireOverflowLane(ctx context.Context) error {
	r.overflowMu.Lock()
	if !r.overflowBusy {
		r.overflowBusy = true
		r.overflowMu.Unlock()
		return nil
	}
	waiter := make(chan struct{})
	r.overflowWaiters = append(r.overflowWaiters, waiter)
	r.overflowMu.Unlock()

	select {
	case <-ctx.Done():
		r.overflowMu.Lock()
		for i, ch := range r.overflowWaiters {
			if ch == waiter {
				r.overflowWaiters = append(r.overflowWaiters[:i], r.overflowWaiters[i+1:]...)
				break
			}
		}
		r.overflowMu.Unlock()
		return ctx.Err()
	case <-waiter:
		return nil
	}
}

func (r *Relay) releaseOverflowLane() {
	r.overflowMu.Lock()
	if len(r.overflowWaiters) > 0 {
		waiter := r.overflowWaiters[0]
		r.overflowWaiters = r.overflowWaiters[1:]
		r.overflowMu.Unlock()
		close(waiter)
		return
	}
	r.overflowBusy = false
	r.overflowMu.Unlock()
}

// eventLoop waits for C64 frames and reacts.
func (r *Relay) eventLoop(ctx context.Context, userID string, emit func(string) error) error {
	deliveredUserText := false
	completedWithoutText := false

	for {
		recvCtx := ctx
		recvCancel := func() {}
		if grace := r.completionGraceWindow(deliveredUserText, completedWithoutText); grace > 0 {
			graceCtx, cancel := context.WithTimeout(ctx, grace)
			recvCtx = graceCtx
			recvCancel = cancel
		}

		f, accepted, err := r.recvFromC64(recvCtx, len(r.textOutQueue) > 0)
		recvCancel()
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) && (deliveredUserText || completedWithoutText) {
				return nil
			}
			return err
		}
		completedWithoutText = false

		// Strip transport ID from reliable C64→bridge frames, queue ACK,
		// and suppress duplicate semantic processing.
		if consumed, unwrapped := r.unwrapAcceptedFrame(&f, accepted, true); consumed {
			continue
		} else {
			accepted = unwrapped
		}

		// Post-unwrap semantic log — clean payload after id strip.
		// SYSTEM is streamed during receive; the other frame types are
		// logged only after full decode/unwrap.
		if f.Type == serial.FrameLLM {
			log.Printf("%s", flowLine("LLM", "←", "C64", "EVENT", formatC64LLMEvent(f.Payload)))
		}
		if f.Type == serial.FrameUser {
			log.Printf("%s", flowLine("USER", "←", "C64", "TEXT", fmt.Sprintf("len=%d text=%q", len(f.Payload), truncate(string(f.Payload), 60))))
		}
		if deliveredUserText && r.hasQueuedMessageWaiter() && shouldHandOffSemanticFrameAfterUserText(f.Type) {
			r.pendingFrames = append([]queuedFrame{{frame: f, accepted: accepted}}, r.pendingFrames...)
			return nil
		}

		switch f.Type {
		case serial.FrameLLM:
			fmt.Fprintln(r.streamOut()) // newline after streamed payload
			payloads := [][]byte{append([]byte(nil), f.Payload...)}
			payloads = append(payloads, r.drainTrailingLLMMessages()...)
			r.appendC64LLMEvents(userID, payloads)
			idle, err := r.callAndDispatch(ctx, userID)
			if err != nil {
				return err
			}
			if idle {
				completedWithoutText = true
				continue
			}

		case serial.FrameResult:
			fmt.Fprintln(r.streamOut()) // newline after streamed payload
			r.toolInFlight = nil
			r.waitingTool = false
			r.basicRunning = false
			r.completionDrain = false
			r.textDrainPending = true
			resultText, complete := r.handleResultFrame(f)
			if !complete {
				continue
			}

			resultPrefix := toolResultScreenPrefix
			if r.lastToolName == "screen" {
				resultPrefix = toolResultScreenshotPrefix
			}
			result := resultPrefix + resultText
			if resultText == "" {
				result = resultPrefix + "(empty)"
			}
			r.appendToolResult(userID, result)
			idle, err := r.callAndDispatch(ctx, userID)
			if err != nil {
				return err
			}
			if idle {
				completedWithoutText = true
				continue
			}

		case serial.FrameStatus:
			fmt.Fprintln(r.streamOut())
			r.toolInFlight = nil
			r.waitingTool = false
			status := string(f.Payload)
			if status == "" {
				status = "UNKNOWN"
			}
			r.basicRunning = status == "RUNNING" || status == "STOP REQUESTED"
			if status == "RUNNING" {
				r.completionDrain = true
			} else if status == "READY" || status == "READY." {
				r.completionDrain = false
				r.basicRunning = false
			}
			if !r.basicRunning {
				r.textDrainPending = true
			}

			if isExpectedStatus(status) {
				log.Printf("%s", flowLine("LLM", "←", "C64", "STATUS!", status))
			} else {
				log.Printf("%s", flowLine("LLM", "←", "C64", "STATUS!", fmt.Sprintf("%s payload=%s", status, hex.EncodeToString(f.Payload))))
				r.dumpMalformedStatus(status, f.Payload)
			}
			r.appendToolResult(userID, toolResultStatusPrefix+status)
			idle, err := r.callAndDispatch(ctx, userID)
			if err != nil {
				return err
			}
			if idle {
				completedWithoutText = true
				continue
			}

		case serial.FrameError:
			log.Printf("%s", flowLine("LLM", "←", "C64", "ERROR", "timeout"))
			r.toolInFlight = nil
			r.waitingTool = false
			r.basicRunning = false
			r.completionDrain = false
			r.textDrainPending = true
			r.appendToolResult(userID, toolResultTimeout)
			idle, err := r.callAndDispatch(ctx, userID)
			if err != nil {
				return err
			}
			if idle {
				completedWithoutText = true
				continue
			}

		case serial.FrameUser:
			// TEXT forwarded by C64 for the user — accumulate until the
			// C64 finishes this burst, then return it to the chat frontend.
			// Drain any immediately trailing internal frames first so the
			// next user turn does not start by consuming stale ACK/STATUS.
			fmt.Fprintln(r.streamOut())
			r.textBuf = append(r.textBuf, f.Payload...)
			r.drainTrailingAfterUserText()
			text := string(r.textBuf)
			r.textBuf = nil
			deliveredUserText = true
			if err := emit(text); err != nil {
				return err
			}
			continue

		case serial.FrameSystem:
			fmt.Fprintln(r.streamOut()) // newline after streamed payload
			r.handleSystemFrame(f)
			continue

		case serial.FrameAck:
			fmt.Fprintln(r.streamOut())
			if ackID, ok := serial.ExtractAckID(f.Payload); ok && r.consumeLateAck(ackID) {
				continue
			}
			logWarnf("%s", flowLine("", "←", "C64", "ACK", "unexpected"))
			continue

		default:
			log.Printf("%s", flowLine("", "←", "C64", "malformed", fmt.Sprintf("unknown frame 0x%02X", f.Type)))
		}
	}
}

func shouldHandOffSemanticFrameAfterUserText(frameType byte) bool {
	switch frameType {
	case serial.FrameLLM, serial.FrameStatus, serial.FrameResult, serial.FrameError:
		return true
	default:
		return false
	}
}

func (r *Relay) completionGraceWindow(deliveredUserText, completedWithoutText bool) time.Duration {
	if !deliveredUserText && !completedWithoutText {
		return 0
	}
	if len(r.textOutQueue) != 0 || r.waitingTool || r.basicRunning {
		return 0
	}
	if deliveredUserText {
		return c64TextHandoffQuietWindow
	}
	if r.hasPendingOverlapSend() {
		return 1 * time.Second
	}
	return 250 * time.Millisecond
}

func (r *Relay) hasPendingOverlapSend() bool {
	r.overlapMu.Lock()
	defer r.overlapMu.Unlock()

	return r.overlapBusy
}

func (r *Relay) messageGateHandoffDelay() time.Duration {
	delay := time.Duration(0)

	if !r.lastAckSentAt.IsZero() {
		if remain := ackQuietWindow - time.Since(r.lastAckSentAt); remain > delay {
			delay = remain
		}
	}
	if !r.lastC64FrameAt.IsZero() {
		if remain := gateHandoffQuietWindow - time.Since(r.lastC64FrameAt); remain > delay {
			delay = remain
		}
	}

	r.msgGateMu.Lock()
	hasQueuedWaiters := len(r.msgGateWaiters) > 0
	r.msgGateMu.Unlock()
	if hasQueuedWaiters && !r.lastC64FrameAt.IsZero() {
		if remain := queuedGateHandoffQuietWindow - time.Since(r.lastC64FrameAt); remain > delay {
			delay = remain
		}
	}

	if delay < 0 {
		return 0
	}
	return delay
}

func (r *Relay) drainTrailingAfterUserText() {
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
		f, accepted, err := r.recvFromC64(ctx, false)
		cancel()
		if err != nil {
			return
		}

		// Unwrap reliable C64→bridge frames (strip ID, queue ACK, dedup).
		if consumed, unwrapped := r.unwrapAcceptedFrame(&f, accepted, true); consumed {
			continue
		} else {
			accepted = unwrapped
		}

		switch f.Type {
		case serial.FrameUser:
			fmt.Fprintln(r.streamOut())
			r.textBuf = append(r.textBuf, f.Payload...)
		case serial.FrameSystem:
			fmt.Fprintln(r.streamOut())
			r.handleSystemFrame(f)
		default:
			r.pendingFrames = append(r.pendingFrames, queuedFrame{frame: f, accepted: accepted})
			return
		}
	}
}

func (r *Relay) drainTrailingLLMMessages() [][]byte {
	var payloads [][]byte
	for {
		ctx, cancel := context.WithTimeout(context.Background(), c64LLMEventBatchQuietWindow)
		f, accepted, err := r.recvFromC64(ctx, false)
		cancel()
		if err != nil {
			return payloads
		}

		if consumed, unwrapped := r.unwrapAcceptedFrame(&f, accepted, true); consumed {
			continue
		} else {
			accepted = unwrapped
		}

		switch f.Type {
		case serial.FrameLLM:
			fmt.Fprintln(r.streamOut())
			log.Printf("%s", flowLine("LLM", "←", "C64", "EVENT", formatC64LLMEvent(f.Payload)))
			payloads = append(payloads, append([]byte(nil), f.Payload...))
		case serial.FrameSystem:
			fmt.Fprintln(r.streamOut())
			r.handleSystemFrame(f)
		default:
			r.pendingFrames = append(r.pendingFrames, queuedFrame{frame: f, accepted: accepted})
			return payloads
		}
	}
}

// appendC64LLMEvents records C64-originated LLM input events in backend history.
// Current LLM backends only understand standard chat roles, so these events are
// serialized as one user message even though they originate on the C64 side.
func (r *Relay) appendC64LLMEvents(userID string, payloads [][]byte) {
	r.History.Append(userID, llm.Message{Role: "user", Content: formatC64LLMEvents(payloads)})
}

func (r *Relay) appendC64LLMEvent(userID string, payload []byte) {
	r.appendC64LLMEvents(userID, [][]byte{payload})
}

func formatC64LLMEvents(payloads [][]byte) string {
	if len(payloads) == 1 {
		return formatC64LLMEvent(payloads[0])
	}
	var b strings.Builder
	b.WriteString("[C64 event batch]")
	for i, payload := range payloads {
		label, body := parseC64LLMEvent(payload)
		fmt.Fprintf(&b, "\n[%d:%s] %s", i+1, label, body)
	}
	return b.String()
}

func formatC64LLMEvent(payload []byte) string {
	label, body := parseC64LLMEvent(payload)
	return fmt.Sprintf("[C64 %s]\n%s", label, body)
}

func parseC64LLMEvent(payload []byte) (string, string) {
	if len(payload) == 0 {
		return "event", ""
	}
	label := "event"
	switch payload[0] {
	case 1:
		label = "user"
	case 2:
		label = "heartbeat"
	case 3:
		label = "status"
	case 4:
		label = "result"
	case 5:
		label = "error"
	}
	return label, string(payload[1:])
}

// callAndDispatch calls the LLM and dispatches the response to the C64.
// It returns idle=true when the model chose no immediate outward action:
// no tool call and no user-visible text.
func (r *Relay) callAndDispatch(ctx context.Context, userID string) (bool, error) {
	history := r.History.Get(userID)
	msgs := make([]llm.Message, 0, 1+len(history))
	if r.SystemPrompt == "" {
		return false, fmt.Errorf("no soul — C64 has not sent system prompt")
	}
	msgs = append(msgs, llm.Message{Role: "system", Content: r.SystemPrompt})
	msgs = append(msgs, history...)

	r.logLLMRequest(msgs, llmTools)
	resp, err := r.LLM.Complete(ctx, msgs, llmTools)
	if err != nil {
		return false, fmt.Errorf("llm: %w", err)
	}

	// Text responses still flow through the C64. The bridge only translates
	// protocols; the C64 remains the user-facing agent.
	if len(resp.ToolCalls) == 0 {
		if resp.Content == "" {
			log.Printf("%s", flowLine("LLM", "←", "", "response", "silent completion"))
			logStream(r.streamOut(), "%s ", flowLabel("LLM", "→", "C64", "DONE"))
			fmt.Fprintln(r.streamOut())
			doneFrame := serial.Frame{Type: serial.FrameDone}
			if err := r.sendVerified(ctx, doneFrame, "DONE"); err != nil {
				return false, fmt.Errorf("send DONE: %w", err)
			}
			return true, nil
		}

		r.History.Append(userID, resp)
		r.textOutQueue = []byte(serial.ToC64Text(resp.Content))
		return false, r.sendNextTextChunk(ctx)
	}
	r.History.Append(userID, resp)

	// Tools are sequential here. The C64 is stateful, so each tool result
	// must feed the next model decision before another tool is dispatched.
	// Some backends (Bedrock) require a tool_result for every tool_use in a
	// response, so we immediately cancel the extra ones.
	tc := resp.ToolCalls[0]
	if len(resp.ToolCalls) > 1 {
		log.Printf("%s", flowLine("", "→", "LLM", "request", fmt.Sprintf("extra tool calls ignored in this turn (%d total)", len(resp.ToolCalls))))
		for _, extra := range resp.ToolCalls[1:] {
			r.History.Append(userID, llm.Message{
				Role:       "tool",
				Content:    "ERROR: only one tool call per turn is supported",
				ToolCallID: extra.ID,
			})
		}
	}
	r.lastToolCallID = tc.ID
	r.lastToolName = tc.Function.Name
	r.textDrainPending = false

	switch tc.Function.Name {
	case "exec":
		var args basicExecArgs
		if err := json.Unmarshal([]byte(tc.Function.Arguments), &args); err != nil {
			log.Printf("%s", flowLine("", "→", "LLM", "request", fmt.Sprintf("bad args: %v", err)))
			r.History.Append(userID, llm.Message{
				Role: "tool", Content: fmt.Sprintf("ERROR: %v", err), ToolCallID: tc.ID,
			})
			return false, nil
		}
		// Sanitize: strip newlines, take first line only, truncate to the
		// maximum EXEC payload the C64 frame receiver can hold.
		cmd := serial.ToASCII(args.Command)
		if i := strings.IndexAny(cmd, "\n\r"); i >= 0 {
			cmd = cmd[:i]
		}
		if len(cmd) > bridgeFrameBodyMax {
			cmd = cmd[:bridgeFrameBodyMax]
		}
		logStream(r.streamOut(), "%s ", flowLabel("LLM", "→", "C64", "EXEC"))
		if err := r.sendExec(ctx, []byte(cmd)); err != nil {
			return false, fmt.Errorf("send EXEC: %w", err)
		}

	case "screen":
		logStream(r.streamOut(), "%s ", flowLabel("LLM", "→", "C64", "SCREENSHOT"))
		fmt.Fprintln(r.streamOut())
		screenFrame := serial.Frame{Type: serial.FrameScreenshot}
		if err := r.sendVerifiedOrSemantic(ctx, screenFrame, "SCREENSHOT"); err != nil {
			return false, fmt.Errorf("send SCREENSHOT: %w", err)
		}
		r.toolInFlight = r.toolInFlight[:0]
		r.startToolWait()

	case "stop":
		logStream(r.streamOut(), "%s ", flowLabel("LLM", "→", "C64", "STOP"))
		fmt.Fprintln(r.streamOut())
		stopFrame := serial.Frame{Type: serial.FrameStop}
		if err := r.sendVerifiedOrSemantic(ctx, stopFrame, "STOP"); err != nil {
			return false, fmt.Errorf("send STOP: %w", err)
		}
		r.toolInFlight = r.toolInFlight[:0]
		r.startToolWait()

	case "status":
		logStream(r.streamOut(), "%s ", flowLabel("LLM", "→", "C64", "STATUS?"))
		fmt.Fprintln(r.streamOut())
		if r.basicRunning {
			timer := time.NewTimer(runningStatusPollInterval)
			select {
			case <-ctx.Done():
				timer.Stop()
				return false, ctx.Err()
			case <-timer.C:
			}
		}
		statusFrame := serial.Frame{Type: serial.FrameStatusReq}
		if err := r.sendStatusProbe(ctx, statusFrame); err != nil {
			return false, fmt.Errorf("send STATUS: %w", err)
		}
		r.toolInFlight = r.toolInFlight[:0]
		r.startToolWait()

	default:
		log.Printf("%s", flowLine("", "→", "LLM", "request", fmt.Sprintf("unknown tool %q", tc.Function.Name)))
		r.History.Append(userID, llm.Message{
			Role: "tool", Content: "ERROR: unknown tool", ToolCallID: tc.ID,
		})
	}
	return false, nil
}

func (r *Relay) logLLMRequest(messages []llm.Message, tools []llm.Tool) {
	if d, ok := r.LLM.(llm.RequestDescriber); ok {
		url, body, err := d.DescribeRequest(messages, tools)
		if err != nil {
			log.Printf("%s", flowLine("", "→", "LLM", "request", fmt.Sprintf("preview failed: %v", err)))
			return
		}
		log.Printf("%s", flowLine("", "→", "LLM", "request", url))
		if strings.TrimSpace(os.Getenv("CLAW64_DEBUG_LLM_REQUESTS")) != "" {
			log.Printf("%s", flowLine("", "→", "LLM", "request", string(body)))
		}
		return
	}
	log.Printf("%s", flowLine("", "→", "LLM", "request", "request"))
}

func (r *Relay) sendNextTextChunk(ctx context.Context) error {
	for len(r.textOutQueue) > 0 {
		// Completion drain: hold TEXT while C64 is draining RESULT/STATUS.
		if r.completionDrain {
			log.Printf("%s", flowLine("", "→", "C64", "TEXT", fmt.Sprintf("queued (%d bytes), waiting for completion drain", len(r.textOutQueue))))
			return nil
		}
		if r.textDrainPending {
			log.Printf("%s", flowLine("", "→", "C64", "TEXT", fmt.Sprintf("queued (%d bytes), draining C64 transport", len(r.textOutQueue))))
		} else {
			log.Printf("%s", flowLine("", "→", "C64", "TEXT", fmt.Sprintf("queued (%d bytes), waiting for C64 transport quiet", len(r.textOutQueue))))
		}
		if err := r.drainC64OutboundBeforeText(ctx); err != nil {
			return err
		}
		r.textDrainPending = false
		chunk := r.textOutQueue
		if len(chunk) > textChunkMax {
			chunk = chunk[:textChunkMax]
		}
		r.textOutQueue = r.textOutQueue[len(chunk):]
		r.textInFlight = append(r.textInFlight[:0], chunk...)
		if err := r.sendTextChunk(ctx, chunk); err != nil {
			fmt.Fprintln(r.streamOut())
			return fmt.Errorf("send TEXT: %w", err)
		}
		r.textInFlight = r.textInFlight[:0]
	}
	return nil
}

func (r *Relay) drainC64OutboundBeforeText(ctx context.Context) error {
	for {
		waitCtx, cancel := context.WithTimeout(ctx, c64TextHandoffQuietWindow)
		f, err := r.recvFromSocketOnly(waitCtx, false)
		cancel()
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				return nil
			}
			return err
		}

		switch f.Type {
		case serial.FrameAck:
			continue
		case serial.FrameStatus:
			if r.acceptC64FrameQueued(&f, true) {
				r.flushPendingAcks()
				continue
			}
			r.flushPendingAcks()
			continue
		case serial.FrameUser, serial.FrameSystem, serial.FrameResult,
			serial.FrameError, serial.FrameLLM:
			if r.acceptC64FrameQueued(&f, true) {
				r.flushPendingAcks()
				continue
			}
			r.flushPendingAcks()
			r.pendingFrames = append([]queuedFrame{{frame: f, accepted: true}}, r.pendingFrames...)
			return fmt.Errorf("unexpected %s while draining C64 transport before TEXT", serial.TypeName(f.Type))
		default:
			return fmt.Errorf("unexpected frame while draining C64 transport before TEXT: %s", serial.TypeName(f.Type))
		}
	}
}

func (r *Relay) sendTextChunk(ctx context.Context, chunk []byte) error {
	frame := serial.Frame{Type: serial.FrameText, Payload: chunk}
	retryDelays := reliableRetryDelays
	logStream(r.streamOut(), "%s ", flowLabel("LLM", "→", "C64", "TEXT"))
	id, frame, err := r.sendNewReliableBridgeFrame(frame)
	if err != nil {
		return err
	}

	for attempt := 0; attempt < len(retryDelays); attempt++ {
		if attempt > 0 {
			logStream(r.streamOut(), "%s ", flowLabel("LLM", "→", "C64", "TEXT"))
			if err := r.resendReliableBridgeFrame(frame); err != nil {
				return err
			}
		}

		waitCtx, cancel := context.WithTimeout(ctx, textAckTimeout)
		_, err := r.waitForAckID(waitCtx, id, true)
		cancel()
		if err == nil {
			return nil
		}
		r.noteDeliveryRetry("TEXT", attempt+1, err)
		logWarnf("%s", flowLine("", "→", "C64", "TEXT", fmt.Sprintf("ack attempt %d failed: %v", attempt+1, err)))
		if attempt+1 < len(retryDelays) {
			time.Sleep(retryDelays[attempt])
		}
	}
	return fmt.Errorf("TEXT delivery could not be verified after %d attempt(s)", len(retryDelays))
}

func (r *Relay) startToolWait() {
	r.waitingTool = true
	r.toolStartedAt = time.Now()
}

func (r *Relay) sendExec(ctx context.Context, cmd []byte) error {
	if err := r.drainC64OutboundBeforeText(ctx); err != nil {
		return err
	}

	execFrame := serial.Frame{Type: serial.FrameExec, Payload: cmd}
	if err := r.sendVerifiedOrSemanticWith(ctx, execFrame, "EXEC", execAckTimeoutFor(len(cmd)), execRetryDelays); err != nil {
		return err
	}
	fmt.Fprintln(r.streamOut())

	r.toolInFlight = append(r.toolInFlight[:0], cmd...)
	r.startToolWait()
	return nil
}

func execAckTimeoutFor(commandBytes int) time.Duration {
	timeout := toolFrameTimeout + time.Duration(commandBytes/4)*time.Second
	if timeout < toolFrameTimeout {
		return toolFrameTimeout
	}
	if timeout > 25*time.Second {
		return 25 * time.Second
	}
	return timeout
}

func (r *Relay) sendVerified(ctx context.Context, frame serial.Frame, name string) error {
	return r.sendVerifiedWithTimeout(ctx, frame, name, ackTimeout)
}

func (r *Relay) sendVerifiedWithTimeout(ctx context.Context, frame serial.Frame, name string, timeout time.Duration) error {
	retryDelays := reliableRetryDelays
	id, frame, err := r.sendNewReliableBridgeFrame(frame)
	if err != nil {
		return err
	}

	for attempt := 0; attempt < len(retryDelays); attempt++ {
		if attempt > 0 {
			if err := r.resendReliableBridgeFrame(frame); err != nil {
				return err
			}
		}

		waitCtx, cancel := context.WithTimeout(ctx, timeout)
		_, err := r.waitForAckID(waitCtx, id, false)
		cancel()
		if err == nil {
			return nil
		}
		r.noteDeliveryRetry(name, attempt+1, err)
		logWarnf("     ! %s ack attempt %d failed: %v", name, attempt+1, err)
		if attempt+1 < len(retryDelays) {
			time.Sleep(retryDelays[attempt])
		}
	}
	return fmt.Errorf("%s delivery could not be verified after %d attempts", name, len(retryDelays))
}

func (r *Relay) sendVerifiedWithAckWaiter(ctx context.Context, frame serial.Frame, name string, timeout time.Duration) error {
	retryDelays := reliableRetryDelays
	id, frame, err := r.sendNewReliableBridgeFrame(frame)
	if err != nil {
		return err
	}
	waiter := r.registerAckWaiter(id)
	defer r.unregisterAckWaiter(id)

	for attempt := 0; attempt < len(retryDelays); attempt++ {
		if attempt > 0 {
			if err := r.resendReliableBridgeFrame(frame); err != nil {
				return err
			}
		}

		waitCtx, cancel := context.WithTimeout(ctx, timeout)
		err := r.waitForAckWaiter(waitCtx, waiter, id)
		cancel()
		if err == nil {
			return nil
		}
		r.noteDeliveryRetry(name, attempt+1, err)
		logWarnf("     ! %s ack attempt %d failed: %v", name, attempt+1, err)
		if attempt+1 < len(retryDelays) {
			time.Sleep(retryDelays[attempt])
		}
	}
	return fmt.Errorf("%s delivery could not be verified after %d attempts", name, len(retryDelays))
}

func (r *Relay) waitForAckWaiter(ctx context.Context, waiter <-chan serial.Frame, id byte) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case f := <-waiter:
			ackID, ok := serial.ExtractAckID(f.Payload)
			if !ok || ackID != id {
				continue
			}
			fmt.Fprintln(r.streamOut())
			return nil
		}
	}
}

func (r *Relay) sendVerifiedOrSemantic(ctx context.Context, frame serial.Frame, name string) error {
	return r.sendVerifiedOrSemanticWith(ctx, frame, name, ackTimeout, reliableRetryDelays)
}

func (r *Relay) sendVerifiedOrSemanticWith(ctx context.Context, frame serial.Frame, name string, timeout time.Duration, retryDelays []time.Duration) error {
	id, frame, err := r.sendNewReliableBridgeFrame(frame)
	if err != nil {
		return err
	}
	for attempt := 0; attempt < len(retryDelays); attempt++ {
		if attempt > 0 {
			if err := r.resendReliableBridgeFrame(frame); err != nil {
				return err
			}
		}

		ok, err := r.waitForAckOrSemantic(ctx, id, timeout, name)
		if err != nil {
			r.noteDeliveryRetry(name, attempt+1, err)
			logWarnf("     ! %s ack attempt %d failed: %v", name, attempt+1, err)
			if attempt+1 < len(retryDelays) {
				time.Sleep(retryDelays[attempt])
			}
			continue
		}
		if ok {
			return nil
		}
		r.noteDeliveryRetry(name, attempt+1, nil)
		logWarnf("     ! %s ack attempt %d failed: no confirmation", name, attempt+1)
		if attempt+1 < len(retryDelays) {
			time.Sleep(retryDelays[attempt])
		}
	}
	if filename, err := r.WriteDebugDump("delivery failure: " + name); err == nil {
		logWarnf("     ! delivery failure dump written to %s", filename)
	} else {
		logWarnf("     ! delivery failure dump failed: %v", err)
	}
	return fmt.Errorf("%s delivery could not be verified after %d attempt(s)", name, len(retryDelays))
}

func (r *Relay) sendStatusProbe(ctx context.Context, frame serial.Frame) error {
	retryDelays := reliableRetryDelays
	for attempt := 0; attempt < len(retryDelays); attempt++ {
		if err := r.sendUnreliableBridgeFrame(frame); err != nil {
			return err
		}

		if err := r.waitForStatusSemantic(ctx, toolFrameTimeout); err == nil {
			return nil
		} else {
			r.noteDeliveryRetry("STATUS", attempt+1, err)
			logWarnf("     ! STATUS semantic attempt %d failed: %v", attempt+1, err)
		}
		if attempt+1 < len(retryDelays) {
			time.Sleep(retryDelays[attempt])
		}
	}
	return fmt.Errorf("STATUS semantic response was not received after %d attempt(s)", len(retryDelays))
}

func (r *Relay) waitForStatusSemantic(ctx context.Context, timeout time.Duration) error {
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	for {
		f, err := r.recvFromSocketOnly(waitCtx, false)
		if err != nil {
			return err
		}
		switch f.Type {
		case serial.FrameAck:
			continue
		case serial.FrameStatus, serial.FrameError:
			if r.acceptC64FrameQueued(&f, true) {
				continue
			}
			r.pendingFrames = append(r.pendingFrames, queuedFrame{frame: f, accepted: true})
			return nil
		case serial.FrameUser, serial.FrameSystem, serial.FrameResult, serial.FrameLLM:
			if r.acceptC64FrameQueued(&f, true) {
				continue
			}
			r.pendingFrames = append(r.pendingFrames, queuedFrame{frame: f, accepted: true})
		default:
			return fmt.Errorf("unexpected frame while waiting for STATUS: %s", serial.TypeName(f.Type))
		}
	}
}

func (r *Relay) waitForPrimeStatus(ctx context.Context, timeout time.Duration) error {
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	for {
		f, err := r.recvFromSocketOnly(waitCtx, false)
		if err != nil {
			return err
		}
		switch f.Type {
		case serial.FrameAck:
			continue
		case serial.FrameStatus, serial.FrameError:
			if r.acceptC64FrameQueued(&f, true) {
				continue
			}
			r.flushPendingAcks()
			return nil
		case serial.FrameUser, serial.FrameSystem, serial.FrameResult, serial.FrameLLM:
			if r.acceptC64FrameQueued(&f, true) {
				continue
			}
			r.pendingFrames = append(r.pendingFrames, queuedFrame{frame: f, accepted: true})
		default:
			return fmt.Errorf("unexpected frame while priming transport: %s", serial.TypeName(f.Type))
		}
	}
}

func (r *Relay) noteDeliveryRetry(name string, attempt int, err error) {
	if r.DeliveryRetry != nil {
		r.DeliveryRetry(name, attempt, err)
	}
}

// allocID returns the next transport ID and advances the counter.
// IDs start at 1; 0 is reserved for "unset"; transport is 7-bit clean.
func (r *Relay) allocID() byte {
	id := r.nextTxID
	if id == 0 || id > 0x7f {
		id = 1
	}

	next := id + 1
	if next == 0 || next > 0x7f {
		next = 1
	}
	r.nextTxID = next
	return id
}

// SetNextTransportID seeds the bridge->C64 reliable ID counter.
// Burn-in uses this to exercise wraparound without flooding the live link.
func (r *Relay) SetNextTransportID(id byte) {
	if id == 0 || id > 0x7f {
		id = 1
	}
	r.nextTxID = id
}

// queueAckToC64 queues an ACK ID to be sent during the next quiet window.
func (r *Relay) queueAckToC64(id byte) {
	r.pendingAcks = append(r.pendingAcks, id)
}

func (r *Relay) rememberLateAck(id byte) {
	if r.lateAckIDs == nil {
		r.lateAckIDs = make(map[byte]struct{})
	}
	r.lateAckIDs[id] = struct{}{}
}

func (r *Relay) consumeLateAck(id byte) bool {
	if _, ok := r.lateAckIDs[id]; !ok {
		return false
	}
	delete(r.lateAckIDs, id)
	return true
}

func (r *Relay) sendNewReliableBridgeFrame(frame serial.Frame) (byte, serial.Frame, error) {
	r.txMu.Lock()
	defer r.txMu.Unlock()

	id := r.allocID()
	frame.Payload = serial.PrependID(id, frame.Payload)
	r.settlePendingAcksLocked()
	if err := r.Link.Send(frame); err != nil {
		return 0, frame, err
	}
	return id, frame, nil
}

func (r *Relay) sendUnreliableBridgeFrame(frame serial.Frame) error {
	r.txMu.Lock()
	defer r.txMu.Unlock()

	r.settlePendingAcksLocked()
	return r.Link.Send(frame)
}

func (r *Relay) resendReliableBridgeFrame(frame serial.Frame) error {
	r.txMu.Lock()
	defer r.txMu.Unlock()

	r.settlePendingAcksLocked()
	return r.Link.Send(frame)
}

func (r *Relay) acceptC64Frame(f *serial.Frame, ackNow bool) bool {
	return r.acceptC64FrameWith(f, ackNow, true)
}

func (r *Relay) acceptC64FrameQueued(f *serial.Frame, ackNow bool) bool {
	return r.acceptC64FrameWith(f, ackNow, false)
}

func (r *Relay) acceptC64FrameWith(f *serial.Frame, ackNow bool, flush bool) bool {
	if !serial.IsReliableC64(f.Type) {
		return false
	}
	id, body, ok := serial.StripID(f.Payload)
	if !ok {
		log.Printf("%s", flowLine("", "←", "C64", "malformed", fmt.Sprintf("%s empty payload, cannot strip id", serial.TypeName(f.Type))))
		return false
	}
	if ackNow {
		r.queueAckToC64(id)
		if flush {
			r.flushPendingAcks()
			if f.Type == serial.FrameUser {
				r.resendAckToC64(id)
			}
		}
	}
	f.Payload = body
	if id == r.rxLastID && f.Type == r.rxLastType {
		log.Printf("%s", flowLine("", "←", "C64", "dedup", fmt.Sprintf("type=%s id=%d", serial.TypeName(f.Type), id)))
		r.noteC64Duplicate(f.Type, id, r.rxLastID)
		return true
	}
	if r.rxLastID != 0 && !transportIDAhead(id, r.rxLastID) {
		log.Printf("%s", flowLine("", "←", "C64", "dedup", fmt.Sprintf("stale type=%s id=%d newest=%d", serial.TypeName(f.Type), id, r.rxLastID)))
		r.noteC64Duplicate(f.Type, id, r.rxLastID)
		return true
	}
	r.rxLastID = id
	r.rxLastType = f.Type
	return false
}

func (r *Relay) noteC64Duplicate(frameType byte, id byte, newest byte) {
	if r.C64Duplicate != nil {
		r.C64Duplicate(serial.TypeName(frameType), id, newest)
	}
}

func transportIDAhead(id, last byte) bool {
	if id == last {
		return false
	}
	diff := int(id) - int(last)
	if diff < 0 {
		diff += 127
	}
	return diff > 0 && diff <= 63
}

// flushPendingAcks sends all queued ACK frames to the C64.
// Called in recvFromC64 before the select (quiet window) and before
// bridge→C64 sends to prevent ACK deadlocks.
func (r *Relay) flushPendingAcks() {
	r.txMu.Lock()
	defer r.txMu.Unlock()

	r.flushPendingAcksLocked()
}

func (r *Relay) settlePendingAcksLocked() {
	if len(r.pendingAcks) > 0 {
		r.flushPendingAcksLocked()
	}
	if r.lastAckSentAt.IsZero() {
		return
	}
	quietWindow := ackQuietWindow
	if r.basicRunning {
		quietWindow = runningAckQuietWindow
	}
	if remain := quietWindow - time.Since(r.lastAckSentAt); remain > 0 {
		time.Sleep(remain)
	}
	if !r.basicRunning || r.lastC64FrameAt.IsZero() {
		return
	}
	if remain := runningRecvQuietWindow - time.Since(r.lastC64FrameAt); remain > 0 {
		time.Sleep(remain)
	}
}

func (r *Relay) flushPendingAcksLocked() {
	if len(r.pendingAcks) > 0 && !r.lastC64FrameAt.IsZero() {
		if remain := c64AckHandoffQuietWindow - time.Since(r.lastC64FrameAt); remain > 0 {
			time.Sleep(remain)
		}
	}

	sent := false
	for _, id := range r.pendingAcks {
		log.Printf("%s", flowLine("", "→", "C64", "ACK", fmt.Sprintf("id=%d", id)))
		ack := serial.Frame{Type: serial.FrameAckToC64, Payload: ackToC64Payload(id)}
		if err := r.Link.Send(ack); err != nil {
			logErrorf("     ! failed to send ACK(%d) to C64: %v", id, err)
		}
		if r.basicRunning {
			time.Sleep(c64AckHandoffQuietWindow)
			if err := r.Link.Send(ack); err != nil {
				logErrorf("     ! failed to resend ACK(%d) to C64: %v", id, err)
			}
		}
		sent = true
	}
	r.pendingAcks = r.pendingAcks[:0]
	if sent {
		r.lastAckSentAt = time.Now()
	}
}

func (r *Relay) resendAckToC64(id byte) {
	time.Sleep(c64AckHandoffQuietWindow)
	r.txMu.Lock()
	defer r.txMu.Unlock()

	ack := serial.Frame{Type: serial.FrameAckToC64, Payload: ackToC64Payload(id)}
	if err := r.Link.Send(ack); err != nil {
		logErrorf("     ! failed to resend ACK(%d) to C64: %v", id, err)
	}
}

func ackToC64Payload(id byte) []byte {
	id &= 0x7F
	return []byte{id, id ^ 0x7F}
}

// unwrapC64Frame strips the transport ID from a reliable C64→bridge frame,
// queues ACK, and returns (frame with stripped payload, isDuplicate).
func (r *Relay) unwrapC64Frame(f *serial.Frame) bool {
	return r.acceptC64FrameQueued(f, true)
}

func (r *Relay) unwrapAcceptedFrame(f *serial.Frame, accepted bool, ackNow bool) (bool, bool) {
	if accepted {
		return false, true
	}
	if r.acceptC64FrameQueued(f, ackNow) {
		return true, false
	}
	return false, true
}

func (r *Relay) waitForAckID(ctx context.Context, id byte, waitingTextAck bool) (serial.Frame, error) {
	if waitingTextAck {
		r.startTextAckDiagnostics(id)
		defer r.clearTextAckDiagnostics()
	}

	for {
		f, err := r.recvFromSocketOnly(ctx, waitingTextAck)
		if err != nil {
			return serial.Frame{}, err
		}
		switch f.Type {
		case serial.FrameAck:
			fmt.Fprintln(r.streamOut())
			ackID, ok := serial.ExtractAckID(f.Payload)
			if waitingTextAck {
				r.noteTextAckFrame(fmt.Sprintf("ACK id=%d", ackID))
			}
			if ok && ackID == id {
				return f, nil
			}
			if ok && r.consumeLateAck(ackID) {
				continue
			}
			logWarnf("     ! ACK id mismatch: got %d want %d", ackID, id)
			continue
		case serial.FrameUser, serial.FrameSystem, serial.FrameStatus,
			serial.FrameResult, serial.FrameError, serial.FrameLLM:
			if waitingTextAck {
				r.noteTextAckFrame(fmt.Sprintf("%s len=%d", serial.TypeName(f.Type), len(f.Payload)))
			}
			acceptedDuplicate := false
			if waitingTextAck {
				acceptedDuplicate = r.acceptC64Frame(&f, true)
			} else {
				acceptedDuplicate = r.acceptC64FrameQueued(&f, true)
			}
			if acceptedDuplicate {
				continue
			}
			r.pendingFrames = append(r.pendingFrames, queuedFrame{frame: f, accepted: true})
			if waitingTextAck && f.Type == serial.FrameUser {
				r.rememberLateAck(id)
				return f, nil
			}
			continue
		default:
			if waitingTextAck {
				r.noteTextAckFrame(serial.TypeName(f.Type))
			}
			return serial.Frame{}, fmt.Errorf("unexpected frame while waiting for ACK: %s", serial.TypeName(f.Type))
		}
	}
}

func (r *Relay) startTextAckDiagnostics(id byte) {
	r.textAckWaitID = id
	r.textAckWaitSince = time.Now()
	r.textAckWaitSeen = 0
	r.textAckWaitLast = "none"
}

func (r *Relay) clearTextAckDiagnostics() {
	r.textAckWaitID = 0
	r.textAckWaitSince = time.Time{}
	r.textAckWaitSeen = 0
	r.textAckWaitLast = ""
}

func (r *Relay) noteTextAckFrame(desc string) {
	r.textAckWaitSeen++
	r.textAckWaitLast = desc
}

func (r *Relay) waitForAckOrSemantic(ctx context.Context, id byte, timeout time.Duration, name string) (bool, error) {
	deadline := time.Now().Add(timeout)

	for {
		waitCtx, cancel := context.WithDeadline(ctx, deadline)
		f, err := r.recvFromSocketOnly(waitCtx, false)
		cancel()
		if err != nil {
			return false, err
		}

		switch f.Type {
		case serial.FrameAck:
			fmt.Fprintln(r.streamOut())
			ackID, ok := serial.ExtractAckID(f.Payload)
			if ok && ackID == id {
				if requiresSemanticConfirmation(name) {
					continue
				}
				return true, nil
			}
			if ok && r.consumeLateAck(ackID) {
				continue
			}
			logWarnf("     ! ACK id mismatch: got %d want %d", ackID, id)
			continue
		case serial.FrameUser, serial.FrameSystem:
			if r.acceptC64FrameQueued(&f, true) {
				continue
			}
			deadline = time.Now().Add(timeout)
			fmt.Fprintln(r.streamOut())
			r.pendingFrames = append(r.pendingFrames, queuedFrame{frame: f, accepted: true})
		case serial.FrameStatus, serial.FrameResult, serial.FrameError, serial.FrameLLM:
			if r.acceptC64FrameQueued(&f, true) {
				if name == "STATUS" && f.Type == serial.FrameStatus {
					r.rememberLateAck(id)
					r.flushPendingAcks()
					return true, nil
				}
				continue
			}
			deadline = time.Now().Add(timeout)
			r.pendingFrames = append(r.pendingFrames, queuedFrame{frame: f, accepted: true})
			if semanticConfirmsDelivery(name, f.Type) {
				r.rememberLateAck(id)
				r.flushPendingAcks()
				return true, nil
			}
			continue
		default:
			return false, fmt.Errorf("unexpected frame while waiting for ACK: %s", serial.TypeName(f.Type))
		}
	}
}

func semanticConfirmsDelivery(name string, frameType byte) bool {
	switch name {
	case "EXEC":
		return frameType == serial.FrameStatus || frameType == serial.FrameResult || frameType == serial.FrameError
	case "SCREENSHOT":
		return frameType == serial.FrameResult || frameType == serial.FrameError
	case "STATUS", "STOP":
		return frameType == serial.FrameStatus || frameType == serial.FrameError
	default:
		return false
	}
}

func requiresSemanticConfirmation(name string) bool {
	switch name {
	case "EXEC", "SCREENSHOT", "STATUS", "STOP":
		return true
	default:
		return false
	}
}

// DrainTransport consumes late transport frames until the C64 link is quiet.
// It is intended for deterministic test/burn-in boundaries, not agent logic.
func (r *Relay) DrainTransport(ctx context.Context, quiet, max time.Duration) error {
	r.flushPendingAcks()
	deadline := time.Now().Add(max)
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return fmt.Errorf("transport did not settle within %s", max)
		}
		wait := quiet
		if remaining < wait {
			wait = remaining
		}

		waitCtx, cancel := context.WithTimeout(ctx, wait)
		f, err := r.recvFromSocketOnly(waitCtx, false)
		cancel()
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				r.flushPendingAcks()
				return nil
			}
			return err
		}

		switch f.Type {
		case serial.FrameAck:
			continue
		case serial.FrameUser, serial.FrameSystem, serial.FrameStatus,
			serial.FrameResult, serial.FrameError, serial.FrameLLM:
			r.acceptC64FrameQueued(&f, true)
		default:
			log.Printf("%s", flowLine("", "←", "C64", "drain", fmt.Sprintf("discarded %s", serial.TypeName(f.Type))))
		}
	}
}

func (r *Relay) appendToolResult(userID, result string) {
	r.History.Append(userID, llm.Message{
		Role:       "tool",
		Content:    result,
		ToolCallID: r.lastToolCallID,
	})
}

func (r *Relay) ensureRecvLoop() {
	r.recvOnce.Do(func() {
		r.ackCh = make(chan serial.Frame, 32)
		r.recvCh = make(chan recvResult, 1)
		go func() {
			for {
				f, err := r.Link.Recv()
				if err == nil && f.Type == serial.FrameAck {
					if r.dispatchAckWaiter(f) {
						continue
					}
					r.ackCh <- f
					continue
				}
				r.recvCh <- recvResult{frame: f, err: err}
				if err != nil {
					return
				}
			}
		}()
	})
}

func (r *Relay) registerAckWaiter(id byte) chan serial.Frame {
	ch := make(chan serial.Frame, 1)
	r.ackWaitMu.Lock()
	if r.ackWaiters == nil {
		r.ackWaiters = make(map[byte]chan serial.Frame)
	}
	r.ackWaiters[id] = ch
	r.ackWaitMu.Unlock()
	return ch
}

func (r *Relay) unregisterAckWaiter(id byte) {
	r.ackWaitMu.Lock()
	delete(r.ackWaiters, id)
	r.ackWaitMu.Unlock()
}

func (r *Relay) dispatchAckWaiter(f serial.Frame) bool {
	ackID, ok := serial.ExtractAckID(f.Payload)
	if !ok {
		return false
	}

	r.ackWaitMu.Lock()
	ch, ok := r.ackWaiters[ackID]
	r.ackWaitMu.Unlock()
	if !ok {
		return false
	}

	select {
	case ch <- f:
	default:
	}
	return true
}

func (r *Relay) recvFromSocketOnly(ctx context.Context, waitingTextAck bool) (serial.Frame, error) {
	r.ensureRecvLoop()
	stallDumped := false

	for {
		if ctx.Err() != nil {
			return serial.Frame{}, ctx.Err()
		}

		var timeout <-chan time.Time
		if wait := r.frameWaitTimeout(waitingTextAck); wait > 0 {
			timeout = time.After(wait)
		}

		if !waitingTextAck {
			if len(r.pendingAcks) > 0 {
				time.Sleep(10 * time.Millisecond)
			}
			r.flushPendingAcks()
		}

		select {
		case <-ctx.Done():
			return serial.Frame{}, ctx.Err()

		case f := <-r.ackCh:
			stallDumped = false
			return f, nil

		case res := <-r.recvCh:
			if res.err != nil {
				return serial.Frame{}, fmt.Errorf("recv: %w", res.err)
			}
			stallDumped = false
			r.lastC64FrameAt = time.Now()
			return res.frame, nil

		case <-timeout:
			if stallDumped {
				continue
			}
			if waitingTextAck {
				r.dumpTextAckStall()
				stallDumped = true
				continue
			}
			if r.waitingTool {
				r.dumpToolAckStall()
				stallDumped = true
				continue
			}
			r.dumpC64SilenceStall()
			stallDumped = true
		}
	}
}

func (r *Relay) recvFromC64(ctx context.Context, waitingTextAck bool) (serial.Frame, bool, error) {
	r.ensureRecvLoop()

	if len(r.pendingFrames) > 0 {
		q := r.pendingFrames[0]
		r.pendingFrames = r.pendingFrames[1:]
		return q.frame, q.accepted, nil
	}
	stallDumped := false

	for {
		if ctx.Err() != nil {
			return serial.Frame{}, false, ctx.Err()
		}

		var timeout <-chan time.Time
		if wait := r.frameWaitTimeout(waitingTextAck); wait > 0 {
			timeout = time.After(wait)
		}

		// While waiting for a bridge→C64 TEXT ACK, do not inject queued
		// bridge ACKs back into the C64. At that point the C64 is expected
		// to emit USER and then its own ACK; extra inbound bytes would
		// reintroduce RX-side jitter during the sensitive TX window.
		if !waitingTextAck {
			// Flush queued ACKs after a brief quiet window. The C64's
			// KERNAL TX ring may still be draining the last byte's stop
			// bit. Wait 10ms to let it finish before injecting RX traffic
			// that would cause NMI jitter on the TX path.
			if len(r.pendingAcks) > 0 {
				time.Sleep(10 * time.Millisecond)
			}
			r.flushPendingAcks()
		}

		select {
		case <-ctx.Done():
			return serial.Frame{}, false, ctx.Err()

		case f := <-r.ackCh:
			stallDumped = false
			return f, false, nil

		case res := <-r.recvCh:
			if res.err != nil {
				return serial.Frame{}, false, fmt.Errorf("recv: %w", res.err)
			}
			stallDumped = false
			r.lastC64FrameAt = time.Now()
			return res.frame, false, nil

		case <-timeout:
			if stallDumped {
				continue
			}
			if waitingTextAck {
				r.dumpTextAckStall()
				stallDumped = true
				continue
			}
			if r.waitingTool {
				r.dumpToolAckStall()
				stallDumped = true
				continue
			}
			r.dumpC64SilenceStall()
			stallDumped = true
		}
	}
}

func (r *Relay) dumpTextAckStall() {
	logErrorf("     ! text ack stall detail: %s", r.currentTextAckDiagnostics())
	filename, err := writeStallDump(
		r.DebugDir,
		r.MonitorAddr,
		r.SymbolPath,
		"waiting for TEXT ack from C64",
		r.currentTextChunk(),
		r.currentRelayState(),
	)
	if err != nil {
		logErrorf("     ! text ack stall; debug dump failed: %v", err)
		return
	}
	logErrorf("     ! text ack stall; wrote debug dump to %s", filename)
}

func (r *Relay) currentTextAckDiagnostics() string {
	id := r.textAckWaitID
	age := "unknown"
	if !r.textAckWaitSince.IsZero() {
		age = fmt.Sprintf("%dms", time.Since(r.textAckWaitSince).Milliseconds())
	}
	last := r.textAckWaitLast
	if last == "" {
		last = "none"
	}
	chunk := truncate(string(r.currentTextChunk()), 80)
	return fmt.Sprintf("id=%d age=%s seen_frames=%d last=%s chunk=%q", id, age, r.textAckWaitSeen, last, chunk)
}

func (r *Relay) currentTextChunk() []byte {
	if len(r.textInFlight) == 0 {
		return nil
	}
	return append([]byte(nil), r.textInFlight...)
}

func (r *Relay) dumpToolAckStall() {
	reason := "waiting for tool result from C64"
	if r.lastToolName != "" {
		reason = fmt.Sprintf("waiting for %s result from C64", r.lastToolName)
	}

	filename, err := writeStallDump(
		r.DebugDir,
		r.MonitorAddr,
		r.SymbolPath,
		reason,
		r.currentToolPayload(),
		r.currentRelayState(),
	)
	if err != nil {
		logErrorf("     ! tool stall; debug dump failed: %v", err)
		return
	}
	logErrorf("     ! tool stall; wrote debug dump to %s", filename)
}

func (r *Relay) dumpMalformedStatus(status string, payload []byte) {
	reason := fmt.Sprintf("malformed status from C64: %q", status)
	filename, err := writeStallDump(
		r.DebugDir,
		r.MonitorAddr,
		r.SymbolPath,
		reason,
		append([]byte(nil), payload...),
		r.currentRelayState(),
	)
	if err != nil {
		logErrorf("     ! malformed status; debug dump failed: %v", err)
		return
	}
	logErrorf("     ! malformed status; wrote debug dump to %s", filename)
}

func (r *Relay) currentToolPayload() []byte {
	if len(r.toolInFlight) == 0 {
		return nil
	}
	return append([]byte(nil), r.toolInFlight...)
}

// WriteDebugDump captures the same monitor state as stall dumps for explicit
// callers such as deterministic burn-in failures.
func (r *Relay) WriteDebugDump(reason string) (string, error) {
	pending := r.currentToolPayload()
	if pending == nil {
		pending = r.currentTextChunk()
	}
	return writeStallDump(
		r.DebugDir,
		r.MonitorAddr,
		r.SymbolPath,
		reason,
		pending,
		r.currentRelayState(),
	)
}

func (r *Relay) currentRelayState() []string {
	r.msgGateMu.Lock()
	msgGateBusy := r.msgGateBusy
	msgGateWaiters := len(r.msgGateWaiters)
	r.msgGateMu.Unlock()
	r.overlapMu.Lock()
	overlapBusy := r.overlapBusy
	r.overlapMu.Unlock()
	r.overflowMu.Lock()
	overflowBusy := r.overflowBusy
	overflowWaiters := len(r.overflowWaiters)
	r.overflowMu.Unlock()
	r.ackWaitMu.Lock()
	ackWaiters := len(r.ackWaiters)
	ackWaiterIDs := sortedAckWaiterIDs(r.ackWaiters)
	r.ackWaitMu.Unlock()
	lateAcks := len(r.lateAckIDs)
	lateAckIDs := sortedByteSet(r.lateAckIDs)
	pendingAckIDs := sortedByteSlice(r.pendingAcks)

	state := []string{
		fmt.Sprintf("relay.waiting_tool: %t", r.waitingTool),
		fmt.Sprintf("relay.basic_running: %t", r.basicRunning),
		fmt.Sprintf("relay.completion_drain: %t", r.completionDrain),
		fmt.Sprintf("relay.text_drain_pending: %t", r.textDrainPending),
		fmt.Sprintf("relay.last_tool: %q", r.lastToolName),
		fmt.Sprintf("relay.pending_frames: %d", len(r.pendingFrames)),
		fmt.Sprintf("relay.pending_acks: %d", len(r.pendingAcks)),
		fmt.Sprintf("relay.text_out_queue: %d", len(r.textOutQueue)),
		fmt.Sprintf("relay.text_in_flight: %d", len(r.textInFlight)),
		fmt.Sprintf("relay.text_ack_wait: %s", r.currentTextAckDiagnostics()),
		fmt.Sprintf("relay.overlap_busy: %t", overlapBusy),
		fmt.Sprintf("relay.overflow_busy: %t", overflowBusy),
		fmt.Sprintf("relay.overflow_waiters: %d", overflowWaiters),
		fmt.Sprintf("relay.msg_gate_busy: %t", msgGateBusy),
		fmt.Sprintf("relay.msg_gate_waiters: %d", msgGateWaiters),
		fmt.Sprintf("relay.overlap_queue_depth: %d", r.overlapQueueDepth()),
		fmt.Sprintf("relay.overlap_queue_at_capacity: %t", r.overlapQueueAtCapacity()),
		fmt.Sprintf("relay.ack_waiters: %d", ackWaiters),
		fmt.Sprintf("relay.ack_waiter_ids: %v", ackWaiterIDs),
		fmt.Sprintf("relay.late_acks: %d", lateAcks),
		fmt.Sprintf("relay.late_ack_ids: %v", lateAckIDs),
		fmt.Sprintf("relay.pending_ack_ids: %v", pendingAckIDs),
	}

	if !r.lastC64FrameAt.IsZero() {
		state = append(state, fmt.Sprintf("relay.last_c64_frame_ago_ms: %d", time.Since(r.lastC64FrameAt).Milliseconds()))
	}
	if !r.toolStartedAt.IsZero() {
		state = append(state, fmt.Sprintf("relay.tool_started_ago_ms: %d", time.Since(r.toolStartedAt).Milliseconds()))
	}

	return state
}

func sortedAckWaiterIDs(waiters map[byte]chan serial.Frame) []byte {
	ids := make([]byte, 0, len(waiters))
	for id := range waiters {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool {
		return ids[i] < ids[j]
	})
	return ids
}

func sortedByteSet(ids map[byte]struct{}) []byte {
	values := make([]byte, 0, len(ids))
	for id := range ids {
		values = append(values, id)
	}
	sort.Slice(values, func(i, j int) bool {
		return values[i] < values[j]
	})
	return values
}

func sortedByteSlice(ids []byte) []byte {
	values := append([]byte(nil), ids...)
	sort.Slice(values, func(i, j int) bool {
		return values[i] < values[j]
	})
	return values
}

func (r *Relay) dumpC64SilenceStall() {
	filename, err := writeStallDump(
		r.DebugDir,
		r.MonitorAddr,
		r.SymbolPath,
		"waiting for any C64 frame",
		nil,
		r.currentRelayState(),
	)
	if err != nil {
		logErrorf("     ! c64 silence stall; debug dump failed: %v", err)
		return
	}
	logErrorf("     ! c64 silence stall; wrote debug dump to %s", filename)
}
