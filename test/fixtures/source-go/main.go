// Command source-go is the event-source fixture in Go
// (docs/EVENT_SOURCE_IMAGES.md): the Bash fixture's broker and exchange,
// test/fixtures/source-bash/entry.sh, written with the standard library and
// nothing of OrkStack's, to show the profile is files in any language.
//
// Its "broker" is a directory under the container's memory-backed /tmp: a
// file in ready/ is a message, held/ is the lease, done/ the acknowledgement,
// dead/ the dead letter, and count/<id> its delivery count.
// `orkctl service test --publish-exec` writes a file into ready/ through the
// container's own root.
package main

import (
	"encoding/json/v2"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"
)

const (
	ask        = "/orkstack/service/ask"
	ans        = "/orkstack/service/answer"
	lane       = "lanes/0"
	broker     = "/tmp/broker"
	maxDeliver = 5
)

var stopping atomic.Bool

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "source-go:", err)
		os.Exit(1)
	}
}

type limits struct {
	HoldSeconds   int `json:"holdSeconds"`
	OrphanSeconds int `json:"orphanSeconds"`
}

func run() error {
	var svc struct {
		Limits limits `json:"limits"`
	}
	raw, err := os.ReadFile("/orkstack/input/service.json")
	if err != nil {
		return err
	}
	if err := json.Unmarshal(raw, &svc); err != nil {
		return fmt.Errorf("service.json: %w", err)
	}
	for _, d := range []string{ans + "/tmp", ans + "/" + lane, broker + "/ready", broker + "/held", broker + "/done", broker + "/dead", broker + "/count"} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			return err
		}
	}
	term := make(chan os.Signal, 1)
	signal.Notify(term, syscall.SIGTERM)
	go func() { <-term; stopping.Store(true) }()

	s := &source{lim: svc.Limits, beatAt: time.Now()}
	if err := s.status(); err != nil {
		return err
	}
	for {
		if err := s.tick(); err != nil {
			return err
		}
		if s.orphaned {
			os.Exit(3)
		}
		seq := readInt(ask+"/"+lane+"/poll.json", "seq")
		if seq == 0 || seq == s.answered {
			if stopping.Load() {
				return nil
			}
			time.Sleep(200 * time.Millisecond)
			continue
		}
		if err := s.exchange(seq); err != nil {
			return err
		}
		if s.orphaned {
			os.Exit(3)
		}
	}
}

type source struct {
	lim      limits
	beat     string
	beatAt   time.Time
	check    int64
	statusAt time.Time
	answered int64
	orphaned bool
}

// say writes a step, then renames it into place: a reader never sees half.
func say(name string, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	tmp := ans + "/tmp/f"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, ans+"/"+name)
}

func (s *source) status() error {
	s.statusAt = time.Now()
	return say("status.json", map[string]any{
		"schemaVersion": 1, "checkSeq": s.check, "ready": true, "check": map[string]any{"ok": true},
		"broker": map[string]any{"object": "dir:" + broker, "maxDeliveries": maxDeliver, "messageLimitBytes": 1 << 20, "leaseSeconds": 60},
	})
}

// tick is everything owed whatever the lane is doing: the beat, a check
// asked for, and the status at least every 20 seconds.
func (s *source) tick() error {
	if b := readString(ask+"/alive.json", "beat"); b != s.beat {
		s.beat, s.beatAt = b, time.Now()
	}
	s.orphaned = time.Since(s.beatAt) > time.Duration(s.lim.OrphanSeconds)*time.Second
	c := readInt(ask+"/check.json", "seq")
	if c != s.check || time.Since(s.statusAt) >= 20*time.Second {
		s.check = c
		return s.status()
	}
	return nil
}

func (s *source) exchange(seq int64) error {
	defer func() { s.answered = seq }()
	id := firstReady()
	if id == "" || stopping.Load() {
		if !stopping.Load() {
			time.Sleep(time.Second)
		}
		return say(lane+"/empty.json", map[string]any{"schemaVersion": 1, "seq": seq})
	}
	if err := os.Rename(broker+"/ready/"+id, broker+"/held/"+id); err != nil { // the lease
		return err
	}
	n := readCount(id) + 1
	if err := os.WriteFile(broker+"/count/"+id, []byte(strconv.Itoa(n)), 0o644); err != nil {
		return err
	}
	body, err := os.ReadFile(broker + "/held/" + id)
	if err != nil {
		return err
	}
	if err := os.WriteFile(ans+"/tmp/b", body, 0o644); err != nil {
		return err
	}
	if err := os.Rename(ans+"/tmp/b", ans+"/"+lane+"/body"); err != nil {
		return err
	}
	if err := say(lane+"/message.json", map[string]any{"schemaVersion": 1, "seq": seq, "id": "dir:" + id, "bodyBytes": len(body), "deliveryCount": n}); err != nil {
		return err
	}
	heldAt, action := time.Now(), ""
	for action == "" {
		if err := s.tick(); err != nil {
			return err
		}
		switch v := verdict(seq); {
		case v != "":
			action = v
		case stopping.Load() || s.orphaned:
			action = "release"
		case time.Since(heldAt) >= time.Duration(s.lim.HoldSeconds)*time.Second:
			action = "hold_expired"
		default:
			time.Sleep(200 * time.Millisecond)
		}
	}
	settled := map[string]any{"schemaVersion": 1, "seq": seq, "ok": true}
	switch action {
	case "ack":
		err = os.Rename(broker+"/held/"+id, broker+"/done/"+id)
	case "hold_expired":
		err = giveBack(id, n)
		settled["ok"], settled["error"] = false, map[string]any{"kind": "hold_expired"}
	default:
		err = giveBack(id, n)
	}
	if err != nil {
		return err
	}
	return say(lane+"/settled.json", settled)
}

// giveBack ends a lease: back to ready, or to the dead letter at the limit.
func giveBack(id string, n int) error {
	to := broker + "/ready/" + id
	if n >= maxDeliver {
		to = broker + "/dead/" + id
	}
	return os.Rename(broker+"/held/"+id, to)
}

func firstReady() string {
	names, err := filepath.Glob(broker + "/ready/*")
	if err != nil || len(names) == 0 {
		return ""
	}
	sort.Strings(names)
	return filepath.Base(names[0])
}

func readCount(id string) int {
	b, err := os.ReadFile(broker + "/count/" + id)
	if err != nil {
		return 0
	}
	n, _ := strconv.Atoi(strings.TrimSpace(string(b)))
	return n
}

// verdict is the Orker's action for this exchange, "" while there is none.
func verdict(seq int64) string {
	var v struct {
		Seq    int64  `json:"seq"`
		Action string `json:"action"`
	}
	b, err := os.ReadFile(ask + "/" + lane + "/verdict.json")
	if err != nil || json.Unmarshal(b, &v) != nil || v.Seq != seq {
		return ""
	}
	return v.Action
}

// readJSON reads one of the Orker's files; a file not yet there, or caught
// mid-replace, reads as nothing, and the next tick reads it again.
func readJSON(path string) map[string]any {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	var m map[string]any
	if json.Unmarshal(b, &m) != nil {
		return nil
	}
	return m
}

func readInt(path, key string) int64 {
	f, _ := readJSON(path)[key].(float64)
	return int64(f)
}

func readString(path, key string) string {
	switch v := readJSON(path)[key].(type) {
	case string:
		return v
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	}
	return ""
}
