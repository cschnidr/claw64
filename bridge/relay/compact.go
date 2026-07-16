// Package relay - compact log output for --compact-log mode.
package relay

import (
	"fmt"
	"log"

	"github.com/sttts/claw64/bridge/termstyle"
)

// compactLine formats a compact log line with direction arrow and content.
// Direction is "send" (arrow right) or "recv" (arrow left).
func compactLine(dir, target, content string) string {
	arrow := "\u2192" // right arrow
	if dir == "recv" {
		arrow = "\u2190" // left arrow
	}
	return fmt.Sprintf("%s %-4s %s", arrow, target, content)
}

func (r *Relay) compactLog(dir, target, content string) {
	log.Printf("%s", compactLine(dir, target, content))
}

func (r *Relay) compactLogOK(dir, target, content string) {
	log.Printf("%s %s", compactLine(dir, target, content), termstyle.Dim("\u2713"))
}

func (r *Relay) compactError(format string, args ...any) {
	log.Println(termstyle.Error(fmt.Sprintf(format, args...)))
}
