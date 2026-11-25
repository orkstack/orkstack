package engine

import (
	"encoding/json/v2"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/orkstack/orkstack/internal/stablejson"
)

// The gate's liveness and rebuild rules (E88 §5.1.5, §5.1.6), pure like the
// rest of its state: the local activities read the cluster and return what
// they found, and these functions decide from it.

// gateMemoSeqKey is the run memo key holding its current incarnation's
// seq_base (§5.2.6).
const gateMemoSeqKey = "orkflow_conc_seq"

// gateMemoKey is the run memo key holding its entry at gateID for taskIx:
// "_r" for the run level, "_<task_ix>" for a task (§5.2.6).
func gateMemoKey(gateID string, taskIx int) string {
	if taskIx == gateRunLevel {
		return "orkflow_conc_" + gateID + "_r"
	}
	return "orkflow_conc_" + gateID + "_" + strconv.Itoa(taskIx)
}

// parseGateMemoKey is gateMemoKey's inverse for gateID: the task index a
// key holds, and whether it is one of gateID's at all.
func parseGateMemoKey(gateID, key string) (int, bool) {
	rest, ok := strings.CutPrefix(key, "orkflow_conc_"+gateID+"_")
	if !ok {
		return 0, false
	}
	if rest == "r" {
		return gateRunLevel, true
	}
	ix, err := strconv.Atoi(rest)
	if err != nil || ix < 0 || strconv.Itoa(ix) != rest {
		return 0, false
	}
	return ix, true
}

// gateMemoEntry is one entry as a run records it in its memo (§5.2.6):
// enough for a gate that lost its record to put it back where it was.
type gateMemoEntry struct {
	Proto     int       `json:"proto"`
	Held      bool      `json:"held,omitempty"`
	Limit     int       `json:"limit"`
	Strategy  string    `json:"strategy"`
	Label     string    `json:"label,omitempty"`
	ArrivedAt time.Time `json:"arrived_at"`
	Seq       int64     `json:"seq"`
	SeqBase   int64     `json:"seq_base"`
	WaitUntil time.Time `json:"wait_until,omitzero"`
	// N is the entry's slot ordinal (§22.14.1), written only above 0.
	N int64 `json:"n,omitempty"`
}

// gateRunFinding is what one describe of a run found (§5.1.5).
type gateRunFinding struct {
	Run string `json:"run"`
	// NotFound: the cluster has no such run.
	NotFound bool `json:"not_found,omitempty"`
	// Open: the run's latest execution is running.
	Open bool `json:"open,omitempty"`
	// MemoSeq is the run's orkflow_conc_seq, 0 when it has none.
	MemoSeq int64 `json:"memo_seq,omitempty"`
	// Keys holds the task indexes whose memo key for this gate is present.
	Keys map[int]bool `json:"keys,omitempty"`
	// StuckSince is when a workflow task the run keeps failing (attempt ≥ 3)
	// was first scheduled, zero when none is.
	StuckSince time.Time `json:"stuck_since,omitzero"`
	// Err is any other failure to describe it: the entry is kept.
	Err string `json:"err,omitempty"`
}

// gatePassOutcome is what a pass changed, for the gate's metrics.
type gatePassOutcome struct {
	Released int
	Marked   int
	Stalled  int
	Errors   int
	// Reclaimed counts the released entries by limit and why.
	Reclaimed map[gateReclaimKey]int
}

// applyFindings applies one pass's findings (§5.1.5, §20 CR-H2): a run gone,
// closed or moved to a newer incarnation loses its entries; an entry whose
// key is missing, or whose run's incarnation is older, is marked once its
// grace has passed (at once when adopted) and released by the next pass
// that confirms it, at most task_confirm_batch per pass; a stuck run is
// marked stalled and kept; an error keeps the entry.
func (g *gateState) applyFindings(findings []gateRunFinding, now time.Time) gatePassOutcome {
	byRun := make(map[string]gateRunFinding, len(findings))
	for _, f := range findings {
		byRun[f.Run] = f
	}
	out := gatePassOutcome{Reclaimed: map[gateReclaimKey]int{}}
	confirmed := 0
	type ref struct {
		run     string
		seqBase int64
		taskIx  int
		n       int64
		why     gateReclaimKey
	}
	var drop []ref
	take := func(e *gateEntry, outcome string) {
		drop = append(drop, ref{e.Run, e.SeqBase, e.TaskIx, e.N, gateReclaimKey{gateLabel(g, e.LabelIx), outcome}})
	}
	visit := func(e *gateEntry, held bool) {
		f, ok := byRun[e.Run]
		if !ok {
			return
		}
		switch {
		case f.Err != "":
			out.Errors++
			return
		case f.NotFound:
			take(e, gateReclaimNotFound)
			return
		case !f.Open:
			take(e, gateReclaimClosed)
			return
		case f.MemoSeq > e.SeqBase:
			take(e, gateReclaimStale)
			return
		}
		e.Stalled = held && !f.StuckSince.IsZero() && now.Sub(f.StuckSince) >= g.Tunables.StalledAfter
		if e.Stalled {
			out.Stalled++
		}
		if f.MemoSeq == e.SeqBase && f.Keys[e.TaskIx] {
			e.MissingSince = time.Time{}
			return
		}
		// Missing key, or an older incarnation the run's re-upsert may not
		// have made visible yet (§20 CR-H2).
		if !e.MissingSince.IsZero() {
			if confirmed < g.Tunables.TaskConfirmBatch {
				confirmed++
				take(e, gateReclaimMissingSlot)
			}
			return
		}
		since := e.GrantedAt
		if !held {
			since = e.ArrivedAt
		}
		if e.Adopted || now.Sub(since) >= g.Tunables.TaskAckGrace {
			e.MissingSince = now
			out.Marked++
		}
	}
	for i := range g.Holders {
		visit(&g.Holders[i], true)
	}
	for i := range g.Queue {
		visit(&g.Queue[i], false)
	}
	for _, r := range drop {
		if g.release(r.run, r.seqBase, r.taskIx, r.n, now) {
			out.Released++
			out.Reclaimed[r.why]++
		}
	}
	g.LastPassAt = now
	return out
}

// gateRunRecord is one open run as ListConcurrencyEntries found it: its
// incarnation and its raw entries for this gate, by task index.
type gateRunRecord struct {
	Run     string                     `json:"run"`
	MemoSeq int64                      `json:"memo_seq"`
	Entries map[int]stablejson.RawJSON `json:"entries"`
	// SeqUnreadable: the run's incarnation could not be read, so none of
	// its entries can be matched to it; each is adopted.
	SeqUnreadable bool `json:"seq_unreadable,omitempty"`
}

// restore puts the runs' recorded entries back (§5.1.6): holders as
// holders, queued entries in line order, entries of a stale incarnation
// skipped, and an undecodable one, or one whose run's incarnation cannot be
// read, adopted as a holder at limit 1, strategy queue, which only ever
// lowers capacity. An entry a live tombstone fences was released while the
// line was read, and visibility had not caught up: it stays released. seq
// resumes above the largest seen. Records and entries are taken in a fixed
// order, and a run listed twice counts once, so the state is the same
// however the cluster listed them. It reports what it skipped and adopted.
func (g *gateState) restore(records []gateRunRecord, now time.Time) gateRestoreOutcome {
	records = slices.Clone(records)
	slices.SortFunc(records, func(a, b gateRunRecord) int { return strings.Compare(a.Run, b.Run) })
	var out gateRestoreOutcome
	var held, queued, adopted []gateEntry
	seen := map[string]bool{}
	for _, r := range records {
		for _, ix := range slices.Sorted(maps.Keys(r.Entries)) {
			var m gateMemoEntry
			decodeErr := decodeGateMemoEntry(r.Entries[ix], &m)
			if decodeErr == nil && m.N > 0 && m.N >= g.Tunables.OrdinalModulus {
				// An ordinal this gate's modulus cannot order: adopted,
				// as any entry the gate cannot read is.
				decodeErr = fmt.Errorf("entry ordinal %d, modulus %d", m.N, g.Tunables.OrdinalModulus)
			}
			if decodeErr != nil || r.SeqUnreadable {
				seqBase := r.MemoSeq
				if decodeErr == nil {
					seqBase = m.SeqBase
				}
				id := slotTombKey(r.Run, seqBase, ix)
				if seen[id] || g.holderIx(r.Run, seqBase, ix) >= 0 || g.queueIx(r.Run, seqBase, ix) >= 0 {
					continue
				}
				if _, fenced := g.tombFor(r.Run, seqBase, ix, 0, now); fenced {
					out.Fenced++
					continue
				}
				seen[id] = true
				adopted = append(adopted, gateEntry{Run: r.Run, TaskIx: ix, SeqBase: seqBase, Limit: 1,
					Strategy: gateStrategyQueue, Proto: gateProto, LabelIx: g.labelIx(""), Adopted: true, GrantedAt: now})
				out.Adopted++
				continue
			}
			if m.SeqBase != r.MemoSeq {
				out.Stale++
				continue
			}
			id := slotTombKey(r.Run, m.SeqBase, ix)
			if seen[id] {
				continue
			}
			if _, fenced := g.tombFor(r.Run, m.SeqBase, ix, m.N, now); fenced {
				out.Fenced++
				continue
			}
			seen[id] = true
			e := gateEntry{Run: r.Run, TaskIx: ix, SeqBase: m.SeqBase, Limit: m.Limit, Strategy: m.Strategy, N: m.N,
				Proto: m.Proto, LabelIx: g.labelIx(m.Label), ArrivedAt: m.ArrivedAt, Seq: m.Seq}
			g.Seq = max(g.Seq, m.Seq)
			// An entry its run re-asserted while the line was read is
			// already here: a holder stays one, and a queued one the run
			// recorded as held becomes one.
			if g.holderIx(e.Run, e.SeqBase, e.TaskIx) >= 0 {
				continue
			}
			if i := g.queueIx(e.Run, e.SeqBase, e.TaskIx); i >= 0 {
				if !m.Held {
					continue
				}
				g.Queue = slices.Delete(g.Queue, i, i+1)
			}
			if m.Held {
				e.GrantedAt = m.ArrivedAt
				held = append(held, e)
			} else {
				e.WaitUntil = m.WaitUntil
				queued = append(queued, e)
			}
		}
	}
	inLine := func(a, b gateEntry) int {
		if a.before(b.ArrivedAt, b.Seq) {
			return -1
		}
		if b.before(a.ArrivedAt, a.Seq) {
			return 1
		}
		return 0
	}
	slices.SortStableFunc(held, inLine)
	slices.SortStableFunc(queued, inLine)
	g.Holders = append(append(g.Holders, held...), adopted...)
	g.Queue = append(g.Queue, queued...)
	slices.SortStableFunc(g.Queue, inLine)
	out.Restored = len(held) + len(queued)
	return out
}

// gateRestoreOutcome is what a restore skipped and adopted, for its log.
type gateRestoreOutcome struct {
	Restored int
	Adopted  int
	Stale    int
	Fenced   int
}

// decodeGateMemoEntry reads one memo entry strictly: an unknown or
// mistyped field, a protocol this gate does not speak, or a strategy it
// does not decide is undecodable, and the entry is adopted.
func decodeGateMemoEntry(raw stablejson.RawJSON, m *gateMemoEntry) error {
	if err := json.Unmarshal(raw, m, json.RejectUnknownMembers(true)); err != nil {
		return err
	}
	if m.Proto < 1 || m.Proto > gateProtoOrdinal || m.Limit < 1 || !gateStrategyDecided(m.Strategy) ||
		m.N < 0 || m.N >= gateOrdinalWire || (m.N > 0 && m.Proto < gateProtoOrdinal) {
		return fmt.Errorf("entry protocol %d, limit %d, strategy %q, ordinal %d", m.Proto, m.Limit, m.Strategy, m.N)
	}
	return nil
}
