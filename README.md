<p align="center">
  <img src="logo-834c95f.png" width="50%" alt="Claw64">
</p>

# Claw64

The Commodore 64 is the agent. BASIC and the visible text screen are its tools.

Claw64 turns a Commodore 64 into an autonomous AI agent. The C64 receives
messages from chat users, consults an LLM for decisions, and acts by typing
BASIC commands into its own REPL — reading the screen to see what happened.
The bridge is necessary because a stock C64 has no Ethernet or Wi-Fi and only
its user port, with a fairly poor RS232 implementation, to talk to the outside
world. The bridge is a dumb relay: it proxies LLM calls and chat messages on
behalf of the C64 over that serial link.

<p align="center">
  <img src="video-874892ea.gif" width="80%" alt="Claw64 running in VICE">
</p>

> [!IMPORTANT]
> The full agent loop runs on the C64.
> The C64 receives the user message, decides what to do via the LLM, uses BASIC
> and the visible screen as its tools, and sends the result back out.
> The bridge only proxies HTTPS APIs to serial and chat or stdin to serial.
> Data flows `user <-> bridge <-> C64 <-> bridge <-> LLM`, with the bridge acting
> only as transport and protocol glue.

[![Release](https://github.com/sttts/claw64/actions/workflows/release.yml/badge.svg)](https://github.com/sttts/claw64/actions/workflows/release.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/sttts/claw64)](https://goreportcard.com/report/github.com/sttts/claw64)
[![License: GPL-3.0](https://img.shields.io/badge/license-GPL--3.0-blue.svg)](LICENSE)
![Vibe Coded](https://img.shields.io/badge/100%25-vibe_coded-blueviolet)

## Where To Read What

- [README.md](/Users/sts/Quellen/slagent/claw64/README.md): quickstart, workflows, and a high-level overview
- [SPEC.md](/Users/sts/Quellen/slagent/claw64/SPEC.md): product and architecture specification
- [PROTOCOL.md](/Users/sts/Quellen/slagent/claw64/PROTOCOL.md): wire protocol and transport design reference
- [AGENTS.md](/Users/sts/Quellen/slagent/claw64/AGENTS.md): repo-specific working rules for coding and debugging

## Tools

The C64 exposes four tools to the LLM through the bridge:

- `exec(command)`: send one line of C64 BASIC input. This is how the agent computes, changes hardware state, stores program lines, runs programs, and asks BASIC to `LIST`. Numbered lines are stored on the C64; they are not just returned as text.
- `screen()`: return the current visible 40x25 text screen without typing anything into BASIC.
- `status()`: report whether BASIC is currently `RUNNING`, `STOP REQUESTED`, or back at `READY.`
- `stop()`: request a RUN/STOP-style break for the currently running BASIC program.

Those four tools are the whole interface. The bridge exposes them, but the C64 decides when and how to use them.

## Quickstart

### Prerequisites

- **Java** (any JDK/JRE) — for KickAssembler (downloaded automatically)
- **VICE** — C64 emulator: `brew install --cask vice`
- **Go** — for the bridge: `brew install go`

### Terminal

```bash
go run ./cmd/claw64-bridge stdin
go run ./cmd/claw64-bridge --say stdin
make run SAY=1
```

Starts the local terminal chat. This is the default and the fastest way to
get a working setup. `stdin` does not use chat-target filtering or the `🕹️`
message trigger. Add `--say` to speak every outgoing backend message locally
with macOS `say -v Zarvox`; it works with `stdin`, Slack, WhatsApp, and Signal.

### Slack

```bash
go run ./cmd/claw64-bridge slack '#claw64'
go run ./cmd/claw64-bridge slack @alice
go run ./cmd/claw64-bridge slack 'https://team.slack.com/archives/C123/p1234567890123456'
```

Only messages in that explicit Slack target are considered, and only if they
start exactly with `:joystick: ` or `:joystick::`.

### WhatsApp

```bash
go run ./cmd/claw64-bridge whatsapp 491701234567@s.whatsapp.net
go run ./cmd/claw64-bridge whatsapp 120363123456789012@g.us
```

On first run, scan the QR code shown by the bridge. After pairing, the bridge
only listens in the explicit target chat JID. Private chats accept every
message from that chat. Group chats require messages to start exactly with
`🕹️ ` or `🕹️:`. While Claw is working on a reply, the backend sends WhatsApp
typing presence for that chat.

### Signal

```bash
go run ./cmd/claw64-bridge signal +49... user:+491701234567
go run ./cmd/claw64-bridge signal +49... group:BASE64GROUPID
```

Optional:

```bash
go run ./cmd/claw64-bridge signal +49... user:+491701234567 --config ~/.local/share/signal-cli
```

The bridge only listens in the explicit Signal target. Private `user:<phone>`
targets accept every message in that chat. Group targets require messages to
start exactly with `🕹️ ` or `🕹️:`. While Claw is working on a reply, the backend
sends Signal typing indicators and refreshes them until the reply is ready.

### LLM Provider

```bash
go run ./cmd/claw64-bridge stdin
go run ./cmd/claw64-bridge --llm openai stdin
export OPENAI_API_KEY=sk-proj-...
go run ./cmd/claw64-bridge auth set-key
export ANTHROPIC_API_KEY=sk-ant-...
go run ./cmd/claw64-bridge --llm openai --llm-key ... stdin
go run ./cmd/claw64-bridge --llm openai --llm-key ... --model gpt-5.5 stdin
go run ./cmd/claw64-bridge --llm ollama --llm-url http://localhost:11434/v1/chat/completions stdin
```

OpenAI is the default backend. If no `OPENAI_API_KEY` is configured, the bridge
can reuse an existing Codex/ChatGPT OAuth login and talk to the Codex backend
directly.

Anthropic uses the direct Messages API and requires a real Anthropic API key.
Claude subscription tokens are not supported. Provide the key via `--llm-key`,
`ANTHROPIC_API_KEY`, or `claw64-bridge auth set-key`.

### Manual Steps

```bash
make assemble      # build the C64 agent PRG
make vice          # launch VICE (auto-starts agent)
make bridge        # run bridge in another terminal
```

For a physical C64 serial adapter, start the bridge without VICE:

```bash
make bridge-serial                         # uses /dev/cu.C64
make bridge-serial SERIAL_DEVICE=/dev/ttyUSB0
make bridge-serial SAY=1                   # speak replies with Zarvox
go run ./cmd/claw64-bridge --say --serial-port /dev/cu.C64 stdin
```

This opens the device as raw `2400,0,0` / 8N1 with no flow control. The
underlying bridge flag is `--serial-port`; setting it implies
`--spawn-vice=false`. If a Bluetooth serial connection drops before the C64
handshake arrives, the bridge logs the error, reopens the device, and keeps
waiting. If macOS opens the device while Bluetooth is not actually connected,
the bridge logs a periodic `still waiting for C64 handshake` message instead
of blocking silently.

For real hardware bring-up, the top-left screen cells show dim dark-gray
checkpoints. Start `make bridge-serial` first, then start the C64 program. When
the character reaches `M`, RS232 is configured. The C64 keeps sending the
handshake byte until the first valid bridge frame arrives, then clears `M` back
to spaces. If the bridge still says `waiting for C64 handshake` after `M`
appears, check the serial device, Bluetooth pairing, wiring, and baud settings.

| Character | Startup phase |
| --- | --- |
| `A` | loader entered |
| `B` | splash visible |
| `C` | resident agent copied |
| `D` | guarded helper copied |
| `E` | busy-color table seeded |
| `F` | READY table seeded |
| `G` | status text seeded |
| `H` | text screen restored |
| `I` | sprite copy starting |
| `J` | sprite data copied |
| `K` | resident agent install entered |
| `L` | vectors and sprites installed |
| `M` | RS232 configured; handshake is being repeated |

Runtime checkpoints use two characters: first the message type, then the
stage.

| Pair | Runtime phase |
| --- | --- |
| `M A` | MSG frame accepted |
| `E A` | EXEC frame accepted |
| `T A` | TEXT frame accepted |
| `Q A` | STATUS request accepted |
| `P A` | SCREENSHOT request accepted |
| `K A` | STOP request accepted |
| `E R` | EXEC RETURN injected |
| `E Z` | numbered BASIC line stored |
| `R O` | RESULT frame queued for bridge |
| `S O` | SYSTEM frame queued for bridge |
| `Q O` | STATUS frame queued for bridge |
| `L O` | LLM event frame queued for bridge |
| `X O` | ERROR frame queued for bridge |
| `U O` | USER text frame queued for bridge |
| `A O` | ACK frame queued for bridge |

The physical setup currently uses a user port RS232 adapter based on Jan
Klingel's [Commodore 64/128 to PC serial guide](https://janklingel.de/2025/03/21/connecting-commodore-64-128-to-pc/)
with an [HC-05 serial Bluetooth module](https://www.amazon.de/dp/B07J66PR6B?ref=ppx_yo2ov_dt_b_fed_asin_title).
The case is this [3D-printed user port adapter](https://www.thingiverse.com/thing:3368773).

<p align="center">
  <img src="docs/userport-adapter.jpg" width="80%" alt="Claw64 C64 user port serial Bluetooth adapter">
</p>

At startup, the loader shows a lobster logo in multicolor bitmap mode for
roughly two seconds before restoring the normal BASIC text screen and
starting the agent.

When `--spawn-vice` is used, the bridge uses an embedded copy of the loader
PRG by default. The repo build writes that loader directly to
[`cmd/claw64-bridge/claw64.prg`](/Users/sts/Quellen/slagent/claw64/cmd/claw64-bridge/claw64.prg),
so `--loader-prg` is only needed to override it.

## Architecture

```
                    Chat (Slack/WhatsApp/Signal/stdin)
                              │
                              ▼
                 ┌──────────────────────┐
                 │     Bridge (Go)      │
                 │                      │
                 │  Proxy only:         │
                 │  chat/stdin ↔ serial │
                 │  and HTTPS ↔ serial  │
                 │                      │
                 │  • chat/stdin ↔ C64  │     ┌─────────────────┐
                 │  • LLM        ↔ C64  │────▶│ LLM (Anthropic, │
                 │  • serial transport  │◀────│ OpenAI, Ollama) │
                 └──────────┬───────────┘     └─────────────────┘
                            │
                     RS232, 2400 baud
                     (TCP in VICE for dev)
                            │
┌───────────────────────────┴───────────────────────────┐
│                    Commodore 64                        │
│                                                        │
│  ┌─────────────────────────────────────────────────┐  │
│  │              TSR Agent ($C000)                   │  │
│  │                                                 │  │
│  │  IDLE ──▶ Receive MSG from bridge               │  │
│  │           │                                     │  │
│  │           ▼                                     │  │
│  │         Send LLM_MSG ──▶ bridge calls LLM       │  │
│  │           │                                     │  │
│  │           ▼                                     │  │
│  │    ┌── Receive EXEC, SCREENSHOT or TEXT          │  │
│  │    │                                            │  │
│  │    │──▶ TEXT: forward to user, back to IDLE     │  │
│  │    │                                            │  │
│  │    │──▶ SCREENSHOT: scrape visible text screen   │  │
│  │    │              send RESULT, loop back        │  │
│  │    │                                            │  │
│  │    └──▶ EXEC: inject keystrokes into BASIC ──┐  │  │
│  │                                              │  │  │
│  │         Wait for READY. prompt ◀─────────────┘  │  │
│  │           │                                     │  │
│  │           ▼                                     │  │
│  │         Scrape screen, send RESULT              │  │
│  │           │                                     │  │
│  │           ▼                                     │  │
│  │         Bridge feeds to LLM, loop back          │  │
│  └─────────────────────────────────────────────────┘  │
│                                                        │
│  ┌─────────────────────────────────────────────────┐  │
│  │           BASIC Interpreter (ROM)               │  │
│  │                                                 │  │
│  │  The agent's tool. Types commands into the      │  │
│  │  REPL via keyboard buffer injection:            │  │
│  │                                                 │  │
│  │  PRINT 6502*8     → compute                     │  │
│  │  POKE 53281,3     → change hardware             │  │
│  │  LIST / LOAD / RUN → inspect and run programs   │  │
│  │                                                 │  │
│  │  Visible text screen is also inspectable        │  │
│  │  directly via text_screenshot                   │  │
│  └─────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────┘
```

## C64 Memory And Queues

The installed agent lowers BASIC memory so the user program cannot overwrite
the agent-owned high-RAM areas. Current protected layout:

| Range | Purpose |
| --- | --- |
| `$9000-$91FF` | guarded helper code copied by the loader |
| `$9300-$95FF` | fixed C64-side user-message queue, 3 slots x 256 bytes |
| `$9600-$9603` | user-queue metadata: staged length, head, tail, count |
| `$9604+` | guarded busy/READY/status lookup tables |
| `$9700-$977F` | EXEC staging buffer |
| `$9800+` | C64 soul / system prompt text |
| `$A000-$A3FF` | cold helper-code reserve |
| `$A800-$BFFF` | memory staging reserve for future durable-memory work |
| `$C000-$CEFF` | resident TSR agent code and state, growth checked at assemble time |
| `$CF00-$CF7F` | receive / tool payload buffer |
| `$CF80-$CFFF` | transmit / injection buffer |

The queue is owned by the C64. While a relay cycle is busy, the bridge may
deliver only bounded overlap up to those three slots. Excess chat messages wait
in FIFO order outside the serial path before entering the C64 queue. If the
C64-side queue is still full, the guarded enqueue helper defensively drops the
oldest pending message and keeps the newest one.

The durable-memory feature is still planned, not implemented. The current
`$A800-$BFFF` region is only a staging reservation; durable memory is intended
to live on C64-owned disk media, not as hidden bridge context.

Heartbeat is a C64-originated typed `LLM_MSG` after roughly two idle minutes,
not a bridge-understood transport frame.

## Serial Protocol

The wire protocol is defined in [`PROTOCOL.md`](/Users/sts/Quellen/slagent/claw64/PROTOCOL.md).

At a high level:

- the C64 communicates with the outside world via binary serial frames
- the bridge translates those frames to HTTP/chat APIs, but does not decide anything
- reliable frames use 1-byte transport ids with explicit ACKs
- bridge -> C64 ACK payloads include the id plus its 7-bit complement
- `EXEC` is the only execution request
- TEXT responses still flow `LLM -> bridge -> C64 -> bridge -> user`
- SYSTEM and RESULT use chunked text payloads above the frame transport
- tool calls are strictly sequential

See `PROTOCOL.md` for:

- exact frame layout
- frame classes
- ACK semantics
- duplicate suppression
- retry rules
- chunking and serialization rules

### Example flow

```
User (Slack):     "What is 6502 * 8?"

Bridge → C64:      M │ What is 6502 * 8?      ← user's message
C64 → Bridge:      L │ What is 6502 * 8?      ← C64 asks bridge to call LLM
Bridge → LLM:      (calls model with user message)
LLM → Bridge:      tool_call: exec("PRINT 6502*8")
Bridge → C64:      E │ PRINT 6502*8            ← tool call

C64 types "PRINT 6502*8" into BASIC REPL
BASIC prints " 52016" on screen
C64 scrapes screen from old cursor to READY.

C64 → Bridge:      R │  52016                  ← tool result
Bridge → LLM:      (feeds tool result back)
LLM → Bridge:      "6502 * 8 = 52016"
Bridge → C64:      T │ 6502 * 8 = 52016        ← final answer
Bridge → Slack:    "6502 * 8 = 52016"
```

Screenshot-only flow:

```
User:               "Do a screenshot"
Bridge → C64:       M │ Do a screenshot
C64 → Bridge:       L │ Do a screenshot
LLM → Bridge:       tool_call: screen()
Bridge → C64:       P │
C64 → Bridge:       R │ [chunked visible text screen]
Bridge → LLM:       (feeds screenshot text back)
LLM → Bridge:       plain text answer quoting the screenshot
```

Long-running BASIC flow:

```
User:               "Print 1 to 1001"
Bridge → C64:       M │ Print 1 to 1001
C64 → Bridge:       L │ Print 1 to 1001
LLM → Bridge:       tool_call: exec("FORI=1TO1001:PRINTI:NEXTI")
Bridge → C64:       E │ FORI=1TO1001:PRINTI:NEXTI

C64 starts the BASIC program
If it keeps running too long, the C64 returns:

C64 → Bridge:       Q │ RUNNING

LLM may then use:
- status()
- stop()
- screen()

`exec()` accepts immediate commands, colon-separated statements, and numbered BASIC program lines, up to 127 characters. Numbered program lines return `STORED` and are not executed; follow them with `exec("RUN")` if you want to run the program.
While BASIC is running, another `exec()` is rejected with `BUSY`.
```

## Chat Platforms

### Slack

Uses [slagent](https://github.com/sttts/slagent) for Slack threads.
Credentials are auto-extracted from the local Slack desktop app on first use.
Accepts a thread URL, `@user`, `#channel`, or a Slack channel ID as the positional target.
`--workspace` is optional if a default slagent workspace exists.
For new threads, the bridge prompts for a topic unless `--topic` is given.
Only messages in the explicit target are considered, and only if they start
exactly with `:joystick: ` or `:joystick::`. Slack output is also rendered
with `:joystick:` quote prefixes so it matches `slagent` shortcode rendering.

### WhatsApp

Uses [whatsmeow](https://github.com/tulir/whatsmeow) with local session
persistence. First run pairs by QR code. After pairing, it listens only in the
explicit target chat JID and only for messages that start exactly with `🕹️ `
or `🕹️:`.

### Signal

Uses [signal-cli](https://github.com/AsamK/signal-cli) as a subprocess.
The current backend keeps a single long-running `jsonRpc` process open for
receive, send, and typing indicators. This avoids competing `signal-cli`
processes fighting over the same config lock.
The first positional argument is the Signal account / phone number used by
`signal-cli`. The second positional argument is the explicit target,
`user:<phone>` or `group:<group-id>`. Private `user:<phone>` targets accept
every message from that chat. Group targets require messages to start exactly
with `🕹️ ` or `🕹️:`. Signal typing indicators are sent while Claw is working.
`--config` is optional.

### stdin

Built-in terminal REPL for local testing, with colored prompts and dimmed
wire logs.

## LLM Backends

| Backend | `--llm` | Auth |
|---------|---------|------|
| OpenAI / Codex | `openai` (default) | `OPENAI_API_KEY`, `--llm-key`, or Codex/ChatGPT OAuth |
| Anthropic (API) | `anthropic` | `ANTHROPIC_API_KEY`, `--llm-key`, or `auth set-key` |
| AWS Bedrock | `bedrock` | AWS credential chain (env, shared config, SSO, IAM role) |
| Ollama | `ollama` | none needed |

### AWS Bedrock

Uses the [Converse API](https://docs.aws.amazon.com/bedrock/latest/userguide/conversation-inference-call.html)
via the AWS SDK for Go v2. Authentication uses the standard AWS credential
provider chain: environment variables, shared credentials/config files, SSO,
ECS task roles, EC2 instance metadata, and web identity tokens.

```bash
# Minimal: region + default credentials
go run ./cmd/claw64-bridge --llm bedrock --aws-region us-east-1 stdin

# Named profile
go run ./cmd/claw64-bridge --llm bedrock --aws-region us-east-1 --aws-profile myprofile stdin

# Cross-region inference profile (recommended for higher throughput)
go run ./cmd/claw64-bridge --llm bedrock --aws-region us-east-1 \
    --model us.anthropic.claude-sonnet-4-5-20250929-v1:0 stdin

# EU cross-region inference
go run ./cmd/claw64-bridge --llm bedrock --aws-region eu-west-1 \
    --model eu.anthropic.claude-sonnet-4-5-20250929-v1:0 stdin
```

The `--aws-region` flag is required when using Bedrock. The default model is
`us.anthropic.claude-sonnet-4-5-20250929-v1:0` (US cross-region Claude Sonnet 4.5).
Pass any Bedrock model ID or inference profile ID via `--model`.

## Status

| Phase | Status |
|-------|--------|
| Skeleton + Build System | :white_check_mark: |
| Serial I/O (C64 TSR) | :white_check_mark: |
| Frame Protocol (C64 + Go) | :white_check_mark: |
| Keystroke Injection | :white_check_mark: |
| Screen Scraping + READY. Detection | :white_check_mark: |
| Startup Loader Logo | :white_check_mark: |
| Tool: screen | :white_check_mark: |
| Tool: status | :white_check_mark: |
| Tool: stop | :white_check_mark: |
| Bridge LLM Client | :white_check_mark: |
| Bridge Relay (orchestrator) | :white_check_mark: |
| Chat: Slack | :white_check_mark: |
| Chat: WhatsApp | :white_check_mark: |
| Chat: Signal | :white_check_mark: |
| Agent Loop (MSG→LLM→EXEC/SCREENSHOT→RESULT) | :white_check_mark: |
| Robustness + Polish | |

See [SPEC.md](SPEC.md) for the full specification.
