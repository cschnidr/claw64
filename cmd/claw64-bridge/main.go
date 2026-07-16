package main

import (
	"bufio"
	"bytes"
	"context"
	_ "embed"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/alecthomas/kong"

	"github.com/sttts/claw64/bridge/chat"
	"github.com/sttts/claw64/bridge/llm"
	"github.com/sttts/claw64/bridge/relay"
	"github.com/sttts/claw64/bridge/serial"
	"github.com/sttts/claw64/bridge/termstyle"
)

type lineLogWriter struct {
	prefix string
	buf    bytes.Buffer
}

func (w *lineLogWriter) Write(p []byte) (int, error) {
	for len(p) > 0 {
		idx := bytes.IndexByte(p, '\n')
		if idx < 0 {
			_, _ = w.buf.Write(p)
			return len(p), nil
		}

		_, _ = w.buf.Write(p[:idx])
		w.flush()
		p = p[idx+1:]
	}
	return len(p), nil
}

func (w *lineLogWriter) flush() {
	line := strings.TrimRight(w.buf.String(), "\r")
	w.buf.Reset()
	if line == "" {
		return
	}
	log.Printf("%s%s", w.prefix, line)
}

type CLI struct {
	SerialAddr  string `name:"serial-addr" help:"Serial TCP address VICE connects to. Defaults to the worktree .ports file when present."`
	SerialPort  string `name:"serial-port" help:"Real serial device path, for example /dev/cu.C64. Implies --spawn-vice=false."`
	MonitorAddr string `name:"monitor-addr" help:"VICE remote monitor address. Defaults to the worktree .ports file when present."`
	LLM         string `name:"llm" default:"openai" enum:"anthropic,openai,ollama" help:"LLM backend."`
	Model       string `name:"model" help:"Override the LLM model name."`
	LLMURL      string `name:"llm-url" help:"Override the OpenAI/Ollama-compatible endpoint URL."`
	LLMKey      string `name:"llm-key" help:"API key for direct API backends."`
	SpawnVICE   bool   `name:"spawn-vice" default:"true" help:"Spawn VICE automatically."`
	ViceBin     string `name:"vice-bin" default:"x64sc" help:"VICE binary to launch when spawning."`
	ViceConsole bool   `name:"vice-console" help:"Start VICE in console mode for headless burn-in runs."`
	LoaderPRG   string `name:"loader-prg" help:"Override the embedded loader PRG path."`
	CompactLog  bool   `name:"compact-log" help:"Use condensed human-friendly log output."`
	Say         bool   `name:"say" help:"Speak every outgoing backend message with macOS say -v Zarvox."`

	Stdin      StdinCmd      `cmd:"" help:"Chat in the local terminal."`
	Slack      SlackCmd      `cmd:"" help:"Chat over Slack."`
	WhatsApp   WhatsAppCmd   `cmd:"" name:"whatsapp" help:"Chat over WhatsApp."`
	Signal     SignalCmd     `cmd:"" help:"Chat over Signal."`
	Burnin     BurninCmd     `cmd:"" help:"Run deterministic end-to-end protocol burn-in scenarios."`
	Auth       AuthCmd       `cmd:"" help:"Manage Anthropic direct-API credentials."`
	TestSerial TestSerialCmd `cmd:"" name:"test-serial" help:"Send a test EXEC and print the RESULT."`
}

type StdinCmd struct{}

type SlackCmd struct {
	Workspace string `name:"workspace" help:"Slack workspace like team.slack.com. Uses the default slagent workspace if omitted."`
	Topic     string `name:"topic" help:"Slack thread title for new threads."`
	Target    string `arg:"" required:"" help:"Slack thread URL, @user, #channel, or Slack channel ID."`
}

type WhatsAppCmd struct {
	Target string `arg:"" required:"" help:"Explicit WhatsApp chat target JID (private or group)." `
	DB     string `name:"db" default:"whatsapp.db" help:"SQLite session database path."`
}

type SignalCmd struct {
	Account string `arg:"" required:"" help:"Signal account / phone number used by signal-cli."`
	Target  string `arg:"" required:"" help:"Explicit Signal target: user:<phone> or group:<group-id>."`
	Config  string `name:"config" help:"Optional signal-cli config directory."`
}

type BurninCmd struct {
	List     bool   `name:"list" help:"List supported burn-in scenarios and exit."`
	Scenario string `arg:"" default:"stop-screen" help:"Deterministic protocol scenario to run."`
}

type AuthCmd struct {
	SetKey AuthSetKeyCmd `cmd:"" name:"set-key" help:"Save a real Anthropic API key for direct API use."`
}

type AuthSetKeyCmd struct {
	Token string `arg:"" help:"Anthropic API key. If omitted, read from stdin."`
}

type TestSerialCmd struct {
	Command string `name:"command" default:"PRINT 42" help:"BASIC command sent as EXEC."`
}

//go:embed claw64.prg
var embeddedLoaderPRG []byte

func main() {
	log.SetOutput(termstyle.DimWriter(os.Stderr))

	serialAddr, monitorAddr := defaultPortAddrs()
	cli := CLI{
		SerialAddr:  serialAddr,
		MonitorAddr: monitorAddr,
	}
	ctx := kong.Parse(
		&cli,
		kong.Name("claw64-bridge"),
		kong.Description("Bridge chat platforms to the C64 agent."),
	)
	normalizeCLI(&cli)

	switch ctx.Command() {
	case "stdin":
		termstyle.ForceColor()
		runChatBridge(cli, chat.NewStdin())
	case "slack <target>":
		runChatBridge(cli, chat.NewSlack(cli.Slack.Workspace, cli.Slack.Target, cli.Slack.Topic))
	case "whatsapp <target>":
		waCh, err := chat.NewWhatsApp(cli.WhatsApp.DB, cli.WhatsApp.Target)
		if err != nil {
			log.Fatalf("whatsapp: %v", err)
		}
		runChatBridge(cli, waCh)
	case "signal <account> <target>":
		runChatBridge(cli, chat.NewSignal(cli.Signal.Account, cli.Signal.Config, cli.Signal.Target))
	case "burnin", "burnin <scenario>":
		if cli.Burnin.List {
			printBurninScenarios(os.Stdout)
			return
		}
		runBurnin(cli, cli.Burnin.Scenario)
	case "auth set-key":
		token := cli.Auth.SetKey.Token
		if token == "" {
			var err error
			token, err = readLineFromStdin("Paste Anthropic API key: ")
			if err != nil {
				log.Fatalf("auth: %v", err)
			}
		}
		path, err := llm.SaveAnthropicToken(token)
		if err != nil {
			log.Fatalf("auth: %v", err)
		}
		log.Printf("auth: saved Anthropic token to %s", path)
	case "test-serial":
		testSerial(cli, cli.TestSerial.Command)
	default:
		ctx.FatalIfErrorf(fmt.Errorf("unknown command %q", ctx.Command()))
	}
}

func normalizeCLI(cli *CLI) {
	if cli.SerialPort != "" {
		cli.SpawnVICE = false
	}
}

func readLineFromStdin(prompt string) (string, error) {
	fmt.Fprint(os.Stderr, prompt)
	line, err := bufio.NewReader(os.Stdin).ReadString('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(line), nil
}

func newLLM(cfg CLI) (llm.Completer, string) {
	switch cfg.LLM {
	case "anthropic":
		c := llm.NewAnthropic(cfg.LLMKey, cfg.Model)
		return c, fmt.Sprintf("anthropic(api) model=%s", c.Model)

	case "openai":
		url := cfg.LLMURL
		if url == "" {
			url = "https://api.openai.com/v1/chat/completions"
		}
		model := cfg.Model
		if model == "" {
			model = "gpt-5.5"
		}
		return &llm.OpenAIClient{URL: url, APIKey: cfg.LLMKey, Model: model},
			fmt.Sprintf("openai url=%s model=%s", url, model)

	case "ollama":
		url := cfg.LLMURL
		if url == "" {
			url = "http://localhost:11434/v1/chat/completions"
		}
		model := cfg.Model
		if model == "" {
			model = "llama3"
		}
		return &llm.OpenAIClient{URL: url, Model: model},
			fmt.Sprintf("ollama url=%s model=%s", url, model)
	}

	log.Fatalf("unknown LLM backend: %q", cfg.LLM)
	return nil, ""
}

func runChatBridge(cfg CLI, ch chat.Channel) {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	llmClient, llmDesc := newLLM(cfg)
	if err := preflightInfra(ctx, cfg, ch, llmClient); err != nil {
		log.Fatalf("setup: %v", err)
	}

	link, viceCmd, cleanupLoader, err := startSerialLink(ctx, cfg)
	if err != nil {
		log.Fatalf("serial: %v", err)
	}
	defer cleanupLoader()
	defer link.Close()
	defer stopVICE(viceCmd)

	rl := &relay.Relay{
		Link:        link,
		LLM:         llmClient,
		History:     relay.NewHistory(),
		CompactLog:  cfg.CompactLog,
		DebugDir:    "debug",
		MonitorAddr: cfg.MonitorAddr,
		SymbolPath:  defaultSymbolPath(),
	}

	// Only route logs into the stdin TUI after all startup failure points have
	// passed. Otherwise fatal startup logs are buffered before the TUI exists.
	type logWriter interface{ LogWriter() io.Writer }
	if lw, ok := ch.(logWriter); ok {
		log.SetOutput(termstyle.DimWriter(lw.LogWriter()))
	}

	log.Println("serial: ready")

	// Route streaming progress through the TUI when available.
	type streamWriter interface{ StreamWriter() io.Writer }
	if sw, ok := ch.(streamWriter); ok {
		rl.StreamOut = sw.StreamWriter()
	}
	rl.SetupProgress()

	if err := rl.PrimeTransport(ctx); err != nil {
		log.Printf("serial: transport prime failed: %v", err)
	}

	log.Printf("bridge: chat=%s llm=%s serial=%s", ch.Name(), llmDesc, serialLabel(cfg))

	err = ch.Start(ctx, func(ctx context.Context, userID, text string) error {
		typing := startTypingIndicator(ctx, ch, userID)
		defer typing.Stop()

		err := rl.HandleMessageStream(ctx, userID, text, func(message string) error {
			typing.Stop()
			return sendBackendMessage(ctx, cfg, ch, userID, message)
		})
		if err != nil {
			log.Printf("     ! error: %v", err)
			return err
		}
		return nil
	})
	if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, chat.ErrInterrupted) && ctx.Err() == nil {
		log.Fatalf("chat: %v", err)
	}
}

type typingSession struct {
	stop func()
}

func (t typingSession) Stop() {
	if t.stop != nil {
		t.stop()
	}
}

func startTypingIndicator(ctx context.Context, ch chat.Channel, userID string) typingSession {
	indicator, ok := ch.(chat.TypingIndicator)
	if !ok {
		return typingSession{}
	}

	typingCtx, cancel := context.WithCancel(ctx)
	var once sync.Once

	sendTypingState(ctx, indicator, userID, true)
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-typingCtx.Done():
				return
			case <-ticker.C:
				sendTypingState(typingCtx, indicator, userID, true)
			}
		}
	}()

	return typingSession{stop: func() {
		once.Do(func() {
			cancel()
			sendTypingState(context.Background(), indicator, userID, false)
		})
	}}
}

func sendTypingState(parent context.Context, indicator chat.TypingIndicator, userID string, active bool) {
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()
	if err := indicator.Typing(ctx, userID, active); err != nil && parent.Err() == nil {
		log.Printf("typing: %v", err)
	}
}

func sendBackendMessage(ctx context.Context, cfg CLI, ch chat.Channel, userID, message string) error {
	if cfg.Say {
		sayZarvox(message)
	}
	return ch.Send(ctx, userID, message)
}

func sayZarvox(message string) {
	if strings.TrimSpace(message) == "" {
		return
	}

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()

		if err := exec.CommandContext(ctx, "say", "-v", "Zarvox", message).Run(); err != nil {
			log.Printf("say: %v", err)
		}
	}()
}

type runningVICE struct {
	cmd  *exec.Cmd
	done chan struct{}
	err  error
}

func (v *runningVICE) exitError() <-chan error {
	ch := make(chan error, 1)
	go func() {
		<-v.done
		ch <- v.err
	}()
	return ch
}

func (v *runningVICE) wait() error {
	<-v.done
	return v.err
}

func startSerialLink(ctx context.Context, cfg CLI) (*serial.Link, *runningVICE, func(), error) {
	if cfg.SerialPort != "" {
		link, err := serial.OpenDeviceContext(ctx, cfg.SerialPort)
		return link, nil, func() {}, err
	}
	if cfg.SerialAddr == "" || cfg.MonitorAddr == "" {
		serialAddr, monitorAddr := defaultPortAddrs()
		if cfg.SerialAddr == "" {
			cfg.SerialAddr = serialAddr
		}
		if cfg.MonitorAddr == "" {
			cfg.MonitorAddr = monitorAddr
		}
	}

	if !cfg.SpawnVICE {
		link, err := serial.Listen(cfg.SerialAddr)
		return link, nil, func() {}, err
	}

	return startSpawnedSerialLink(cfg)
}

func serialLabel(cfg CLI) string {
	if cfg.SerialPort != "" {
		return cfg.SerialPort
	}
	return cfg.SerialAddr
}

func startSpawnedSerialLink(cfg CLI) (*serial.Link, *runningVICE, func(), error) {
	const attempts = 8
	const handshakeTimeout = 45 * time.Second

	loaderPath, cleanupLoader, err := loaderPRGPath(cfg)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("vice: %w", err)
	}

	for attempt := 1; attempt <= attempts; attempt++ {
		var viceCmd *runningVICE
		link, err := serial.ListenAndStartUntilTimeout(cfg.SerialAddr, func() (<-chan error, error) {
			log.Printf("vice: spawning %s with %s", cfg.ViceBin, loaderPath)

			viceCmd, err = spawnVICE(cfg, loaderPath)
			if err != nil {
				return nil, fmt.Errorf("vice: %w", err)
			}
			return viceCmd.exitError(), nil
		}, handshakeTimeout)
		if err == nil {
			return link, viceCmd, cleanupLoader, nil
		}
		if filename, dumpErr := relay.WriteMonitorDump("debug", cfg.MonitorAddr, defaultSymbolPath(), "startup failure: "+err.Error()); dumpErr == nil {
			log.Printf("vice: startup failure dump written to %s", filename)
		} else {
			log.Printf("vice: startup failure dump failed: %v", dumpErr)
		}
		stopVICE(viceCmd)
		if attempt == attempts {
			cleanupLoader()
			return nil, nil, nil, err
		}
		log.Printf("vice: startup attempt %d/%d failed: %v; retrying", attempt, attempts, err)
		time.Sleep(time.Duration(attempt) * time.Second)
	}

	cleanupLoader()
	return nil, nil, nil, fmt.Errorf("vice: startup failed")
}

func defaultPortAddrs() (string, string) {
	serialAddr := "127.0.0.1:25232"
	monitorAddr := "127.0.0.1:6510"

	f, err := os.Open(".ports")
	if err != nil {
		return serialAddr, monitorAddr
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		key, value, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}

		switch strings.TrimSpace(key) {
		case "SERIAL_PORT":
			if value = strings.TrimSpace(value); value != "" {
				serialAddr = "127.0.0.1:" + value
			}
		case "MONITOR_PORT":
			if value = strings.TrimSpace(value); value != "" {
				monitorAddr = "127.0.0.1:" + value
			}
		}
	}

	return serialAddr, monitorAddr
}

func preflightInfra(ctx context.Context, cfg CLI, ch chat.Channel, llmClient llm.Completer) error {
	if p, ok := ch.(chat.Preflighter); ok {
		if err := p.Preflight(ctx); err != nil {
			return err
		}
	}

	type llmPreflighter interface {
		Preflight(context.Context) error
	}
	if p, ok := llmClient.(llmPreflighter); ok {
		if err := p.Preflight(ctx); err != nil {
			if cfg.LLM == "openai" && errors.Is(err, llm.ErrOpenAICodexAuthRequired) && llm.CanPromptForOpenAICodexAuth() {
				ok, promptErr := llm.ConfirmOpenAICodexAuth()
				if promptErr != nil {
					return promptErr
				}
				if ok {
					if authErr := llm.RunOpenAICodexAuth(); authErr != nil {
						return authErr
					}
					return p.Preflight(ctx)
				}
			}
			return err
		}
	}
	return nil
}

func loaderPRGPath(cfg CLI) (string, func(), error) {
	if cfg.LoaderPRG != "" {
		return cfg.LoaderPRG, func() {}, nil
	}

	// Prefer the freshly assembled repo artifact during development.
	if _, err := os.Stat(filepath.Join("cmd", "claw64-bridge", "claw64.prg")); err == nil {
		return filepath.Join("cmd", "claw64-bridge", "claw64.prg"), func() {}, nil
	}

	f, err := os.CreateTemp("", "claw64-loader-*.prg")
	if err != nil {
		return "", nil, fmt.Errorf("create embedded loader temp file: %w", err)
	}
	if _, err := f.Write(embeddedLoaderPRG); err != nil {
		f.Close()
		os.Remove(f.Name())
		return "", nil, fmt.Errorf("write embedded loader temp file: %w", err)
	}
	if err := f.Close(); err != nil {
		os.Remove(f.Name())
		return "", nil, fmt.Errorf("close embedded loader temp file: %w", err)
	}

	return f.Name(), func() { _ = os.Remove(f.Name()) }, nil
}

func defaultSymbolPath() string {
	candidates := []string{
		filepath.Join("c64", "loader.sym"),
		filepath.Join("c64", "agent.sym"),
	}
	for _, path := range candidates {
		if _, err := os.Stat(path); err == nil {
			return path
		}
	}
	return filepath.Join("c64", "loader.sym")
}

func spawnVICE(cfg CLI, loaderPath string) (*runningVICE, error) {
	viceBin := cfg.ViceBin
	if path, err := exec.LookPath(cfg.ViceBin); err == nil {
		viceBin = path
	}

	args := []string{"-default"}
	if cfg.ViceConsole {
		args = append(args, "-console")
	}
	if dataDir := viceDataDir(viceBin); dataDir != "" {
		args = append(args, "-directory", dataDir)
	}
	args = append(args,
		"-loglimit", "0",
		"+logtofile",
		"+logtostdout",
		"-drive8type", "0",
		"-drive9type", "0",
		"-drive10type", "0",
		"-drive11type", "0",
		"+sound",
		"-sounddev", "dummy",
		"-rsdev1", cfg.SerialAddr,
		"-userportdevice", "2",
		"-rsuserdev", "0",
		"-rsuserbaud", "2400",
		"-autostartprgmode", "1",
		"-remotemonitor",
		"-remotemonitoraddress", cfg.MonitorAddr,
		"-autostart", loaderPath,
	)

	cmd := exec.Command(viceBin, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return nil, err
	}

	vice := &runningVICE{
		cmd:  cmd,
		done: make(chan struct{}),
	}
	go func() {
		vice.err = cmd.Wait()
		close(vice.done)
	}()
	return vice, nil
}

func viceDataDir(viceBin string) string {
	path, err := filepath.EvalSymlinks(viceBin)
	if err != nil {
		path = viceBin
	}

	candidates := []string{
		filepath.Join(filepath.Dir(path), "..", "share", "vice"),
		filepath.Join(filepath.Dir(path), "..", "..", "share", "vice"),
		"/opt/homebrew/share/vice",
		"/usr/local/share/vice",
	}
	for _, candidate := range candidates {
		abs, err := filepath.Abs(candidate)
		if err != nil {
			continue
		}
		if st, err := os.Stat(filepath.Join(abs, "C64")); err == nil && st.IsDir() {
			return abs
		}
	}
	return ""
}

func stopVICE(vice *runningVICE) {
	if vice == nil || vice.cmd == nil || vice.cmd.Process == nil {
		return
	}
	_ = vice.cmd.Process.Kill()
	_ = vice.wait()
}

func testSerial(cfg CLI, command string) {
	var (
		link *serial.Link
		err  error
	)
	if cfg.SerialPort != "" {
		link, err = serial.OpenDevice(cfg.SerialPort)
	} else {
		link, err = serial.Listen(cfg.SerialAddr)
	}
	if err != nil {
		log.Fatalf("serial: %v", err)
	}
	defer link.Close()

	log.Printf("send EXEC: %q", command)
	if err := link.Send(serial.Frame{Type: serial.FrameExec, Payload: []byte(command)}); err != nil {
		log.Fatalf("send: %v", err)
	}

	log.Println("waiting for C64 reply...")
	var resultChunks map[int]string
	for {
		f, err := link.Recv()
		if err != nil {
			log.Fatalf("recv: %v", err)
		}
		if f.Type == serial.FrameError {
			fmt.Println("C64: command timed out")
			return
		}
		if f.Type != serial.FrameResult {
			log.Printf("ignoring unexpected %s frame", serial.TypeName(f.Type))
			continue
		}
		if len(f.Payload) < 2 {
			log.Printf("short RESULT payload: %q", string(f.Payload))
			continue
		}

		idx := int(f.Payload[0])
		total := int(f.Payload[1])
		if resultChunks == nil {
			resultChunks = make(map[int]string)
		}
		resultChunks[idx] = string(f.Payload[2:])
		if len(resultChunks) != total {
			continue
		}

		var result string
		for i := 0; i < total; i++ {
			result += resultChunks[i]
		}
		log.Printf("result [%d bytes]: %q", len(result), result)
		fmt.Printf("C64> %s\n", result)
		return
	}
}
