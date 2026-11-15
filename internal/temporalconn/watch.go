package temporalconn

import (
	"bytes"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

// How often the files are looked at, and when a certificate's end is warned.
const (
	// statEvery is the least time between two looks at a file's stamp.
	statEvery = time.Second
	// rereadEvery re-reads a file whose stamp did not change: a replacement
	// within one second, at the same size, has the same stamp.
	rereadEvery = time.Minute
	// expiryWarnWithin and expiryWarnEvery: a certificate this close to its
	// end is warned about at most once a day.
	expiryWarnWithin = 7 * 24 * time.Hour
	expiryWarnEvery  = 24 * time.Hour
	// failureLogEvery: a refusal that stays is logged again this often.
	failureLogEvery = time.Hour
)

// loaded is what a file decodes to; end is its certificates' earliest end,
// zero for an API key.
type loaded interface{ end() time.Time }

// watched is one credential read from one or more files (a certificate and
// its key are one), re-read when a file's stamp changes and once a minute in
// any case. A re-read is checked as boot checks it; one that fails keeps the
// last good value. The value is read under the mutex: current is called from
// a TLS handshake or once per Temporal call, and holds the lock for a stat at
// most once a second.
type watched[T loaded] struct {
	setting string // the one a metric and a log line name
	paths   []string
	load    func([][]byte) (T, error)
	o       Options

	mu        sync.Mutex
	value     T
	checkedAt time.Time   // the last look at the stamps
	attempt   fingerprint // the stamps of the last read, good or not
	attemptAt time.Time
	failure   string    // the last refusal logged, so it is logged once an hour
	failedAt  time.Time // when it was logged
	busySince time.Time // when the look under way began
	stuckSaid bool      // that look's being stuck has been logged
	digest    [32]byte  // the adopted files' bytes: a re-read of the same is no reload
	busy      bool      // a look or a read is under way, outside the lock
	warnedAt  time.Time // the last expiry warning
}

// watch reads the files for the first time, as boot does: a failure is the
// refusal New returns.
func watch[T loaded](o Options, setting string, paths []string, load func([][]byte) (T, error)) (*watched[T], error) {
	w := &watched[T]{setting: setting, paths: paths, load: load, o: o}
	now := o.Now()
	fp := stampsOf(paths)
	value, digest, err := w.read()
	if err != nil {
		return nil, err
	}
	w.value, w.digest, w.attempt, w.attemptAt, w.checkedAt = value, digest, fp, now, now
	report := w.adopted(now)
	report = append(report, w.warnExpiry(now)...)
	report.run()
	return w, nil
}

// current is the value in force, re-read first if a file looks changed or a
// minute has passed. The look and the read happen outside the lock, one at
// a time: a caller that comes while one is under way takes the value in
// force, so a slow file never holds up a handshake or a call behind it.
func (w *watched[T]) current() T {
	w.mu.Lock()
	now := w.o.Now()
	if since := now.Sub(w.checkedAt); w.busy || (since >= 0 && since < statEvery) {
		report := w.warnExpiry(now)
		if w.busy && !w.stuckSaid && now.Sub(w.busySince) > rereadEvery {
			w.stuckSaid = true
			began := w.busySince
			report = append(report, func() {
				w.o.Logger.Error("temporal TLS: a look at a file has not returned; the value in force stays in use until it does",
					"setting", w.setting, "since", began)
			})
		}
		value := w.value
		w.mu.Unlock()
		report.run()
		return value
	}
	w.busy, w.busySince, w.stuckSaid, w.checkedAt = true, now, false, now
	last, lastAt := w.attempt, w.attemptAt
	w.mu.Unlock()
	// busy is cleared whatever the look does, so a reload can never stop for
	// good; a panic in it still reaches the caller.
	settled := false
	defer func() {
		if !settled {
			w.mu.Lock()
			w.busy = false
			w.mu.Unlock()
		}
	}()

	fp := stampsOf(w.paths)
	fresh := fp.equal(last) && now.Sub(lastAt) < rereadEvery && !now.Before(lastAt)
	var (
		value  T
		digest [32]byte
		err    error
	)
	if !fresh {
		value, digest, err = w.read()
	}

	w.mu.Lock()
	w.busy, settled = false, true
	var report reports
	switch {
	case fresh:
	case err != nil:
		w.attempt, w.attemptAt = fp, now
		report = append(report, func() { w.o.Observer.FileReloaded(w.setting, false) })
		if msg := err.Error(); msg != w.failure || now.Sub(w.failedAt) >= failureLogEvery || now.Before(w.failedAt) {
			w.failure, w.failedAt = msg, now
			report = append(report, func() {
				w.o.Logger.Error("temporal TLS: a changed file was not adopted; the last good one stays in use",
					"setting", w.setting, "error", msg)
			})
		}
	case digest == w.digest:
		w.attempt, w.attemptAt = fp, now
		if w.failure != "" { // back to what is in force: nothing to adopt
			w.failure = ""
			report = append(report, func() {
				w.o.Logger.Info("temporal TLS: a refused file is good again; it holds what is in force", "setting", w.setting)
			})
		}
	default:
		w.attempt, w.attemptAt = fp, now
		w.value, w.digest, w.failure = value, digest, ""
		report = append(report, func() { w.o.Observer.FileReloaded(w.setting, true) })
		report = append(report, w.adopted(now)...)
	}
	report = append(report, w.warnExpiry(now)...)
	value = w.value
	w.mu.Unlock()
	report.run()
	return value
}

// reports are what a look tells the logger and the observer, called after
// the lock is let go so a slow handler holds up no handshake.
type reports []func()

func (r reports) run() {
	for _, f := range r {
		f()
	}
}

// adopted reports a newly adopted value's end, and lets its expiry be warned
// about afresh. It is called with w.mu held.
func (w *watched[T]) adopted(now time.Time) reports {
	var report reports
	if end := w.value.end(); !end.IsZero() {
		report = append(report, func() { w.o.Observer.CertificateLoaded(w.setting, end) })
	}
	w.warnedAt = time.Time{}
	return report
}

// warnExpiry warns, at most once a day, of a certificate within a week of
// its end or past it. It is called with w.mu held.
func (w *watched[T]) warnExpiry(now time.Time) reports {
	end := w.value.end()
	if end.IsZero() || end.Sub(now) > expiryWarnWithin {
		return nil
	}
	if !w.warnedAt.IsZero() && now.Sub(w.warnedAt) < expiryWarnEvery && !now.Before(w.warnedAt) {
		return nil
	}
	w.warnedAt = now
	msg := "temporal TLS: a certificate ends within a week; replace its file"
	if !end.After(now) {
		msg = "temporal TLS: a certificate has ended; replace its file"
	}
	return reports{func() { w.o.Logger.Warn(msg, "setting", w.setting, "not_after", end) }}
}

// read reads every file and decodes them; digest is their bytes', each
// file's length-prefixed.
func (w *watched[T]) read() (T, [32]byte, error) {
	var zero T
	h := sha256.New()
	contents := make([][]byte, len(w.paths))
	for i, path := range w.paths {
		data, err := readSmallFile(path)
		if err != nil {
			return zero, [32]byte{}, refusal(settingOfPath(w.setting, i), err.Error())
		}
		contents[i] = data
		var n [8]byte
		binary.BigEndian.PutUint64(n[:], uint64(len(data)))
		h.Write(n[:])
		h.Write(data)
	}
	value, err := w.load(contents)
	if err != nil {
		return zero, [32]byte{}, err
	}
	var digest [32]byte
	h.Sum(digest[:0])
	return value, digest, nil
}

// maxFileBytes bounds a file the settings name: a CA bundle, a certificate
// chain, a key or an API key is far smaller.
const maxFileBytes = 1 << 20

// readSmallFile reads a regular file of at most maxFileBytes. A FIFO, a
// device or a directory is refused before it is opened, so a read can
// neither block nor run without bound; the opened file is checked again, so
// a file swapped for another between the two is refused too.
func readSmallFile(path string) ([]byte, error) {
	before, err := os.Stat(path)
	if err != nil {
		return nil, errors.New("cannot be read: " + pathError(err))
	}
	if !before.Mode().IsRegular() {
		return nil, errors.New("is not a regular file")
	}
	f, err := openForRead(path)
	if err != nil {
		return nil, errors.New("cannot be read: " + pathError(err))
	}
	defer f.Close()
	opened, err := f.Stat()
	if err != nil {
		return nil, errors.New("cannot be read: " + pathError(err))
	}
	if !opened.Mode().IsRegular() || !os.SameFile(before, opened) {
		return nil, errors.New("changed while it was opened; it is read again at the next look")
	}
	data, err := io.ReadAll(io.LimitReader(f, maxFileBytes+1))
	if err != nil {
		return nil, errors.New("cannot be read: " + pathError(err))
	}
	if len(data) > maxFileBytes {
		return nil, errors.New("is larger than 1 MiB")
	}
	return data, nil
}

// settingOfPath is the setting that names a watched credential's i-th file:
// a client pair's second file is its key.
func settingOfPath(setting string, i int) string {
	if setting == SettingCertFile && i == 1 {
		return SettingKeyFile
	}
	return setting
}

// pathError is a read failure's words, which name the path and the cause and
// never the content.
func pathError(err error) string {
	var pe *os.PathError
	if errors.As(err, &pe) {
		return pe.Op + " " + pe.Path + ": " + pe.Err.Error()
	}
	return err.Error()
}

// fingerprint is what a look at the files saw: each one's identity, size and
// modification time, or the failure to look.
type fingerprint []stamp

type stamp struct {
	info os.FileInfo
	err  string
}

func stampsOf(paths []string) fingerprint {
	fp := make(fingerprint, len(paths))
	for i, path := range paths {
		info, err := os.Stat(path) // through a link: a swapped target is a new file
		if err != nil {
			fp[i] = stamp{err: err.Error()}
			continue
		}
		fp[i] = stamp{info: info}
	}
	return fp
}

func (f fingerprint) equal(g fingerprint) bool {
	if len(f) != len(g) {
		return false
	}
	for i := range f {
		a, b := f[i], g[i]
		if a.err != b.err || (a.info == nil) != (b.info == nil) {
			return false
		}
		if a.info == nil {
			continue
		}
		if !os.SameFile(a.info, b.info) || a.info.Size() != b.info.Size() || !a.info.ModTime().Equal(b.info.ModTime()) {
			return false
		}
	}
	return true
}

// roots are a CA file's certificates.
type roots struct {
	pool     *x509.CertPool
	notAfter time.Time
}

func (r roots) end() time.Time { return r.notAfter }

func loadRoots(contents [][]byte) (roots, error) {
	certs, err := parseCertificates(contents[0])
	if err != nil {
		return roots{}, refusal(SettingCAFile, err.Error())
	}
	pool := x509.NewCertPool()
	for _, c := range certs {
		pool.AddCert(c)
	}
	return roots{pool: pool, notAfter: earliestEnd(certs)}, nil
}

// pair is a client certificate chain and its key.
type pair struct {
	cert     tls.Certificate
	notAfter time.Time
}

func (p pair) end() time.Time { return p.notAfter }

func loadPair(contents [][]byte) (pair, error) {
	certs, err := parseCertificates(contents[0])
	if err != nil {
		return pair{}, refusal(SettingCertFile, err.Error())
	}
	cert, err := tls.X509KeyPair(contents[0], contents[1])
	if err != nil {
		// The standard library's words name the cause (no key found, a
		// mismatch, an unsupported type) and never hold key material.
		return pair{}, refusal(SettingKeyFile, "is not the private key of "+SettingCertFile+"'s certificate, or does not parse: "+err.Error())
	}
	return pair{cert: cert, notAfter: certs[0].NotAfter}, nil
}

// apiKey is the API key a file holds.
type apiKey struct{ key string }

func (apiKey) end() time.Time { return time.Time{} }

func loadAPIKey(contents [][]byte) (apiKey, error) {
	key := string(bytes.TrimSpace(contents[0]))
	if key == "" {
		return apiKey{}, refusal(SettingAPIKeyFile, "the file is empty")
	}
	for i := 0; i < len(key); i++ {
		if key[i] < 0x21 || key[i] > 0x7e {
			return apiKey{}, refusal(SettingAPIKeyFile, "holds a character a request header cannot carry: a space, a line end or a byte outside printable ASCII")
		}
	}
	return apiKey{key: key}, nil
}

// parseCertificates decodes every PEM block of data, which must all be
// certificates that parse, and at least one. Text between blocks is ignored,
// as the standard library ignores it.
func parseCertificates(data []byte) ([]*x509.Certificate, error) {
	var certs []*x509.Certificate
	for n := 1; ; n++ {
		var block *pem.Block
		block, data = pem.Decode(data)
		if block == nil {
			break
		}
		if block.Type != "CERTIFICATE" {
			return nil, fmt.Errorf("PEM block %d is not a certificate", n)
		}
		c, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("PEM block %d is not a certificate that parses", n)
		}
		certs = append(certs, c)
	}
	if len(certs) == 0 {
		return nil, errors.New("holds no PEM certificate")
	}
	if bytes.Contains(data, []byte("-----BEGIN")) {
		// A block pem.Decode could not finish: a file cut short, or read
		// while it was written.
		return nil, fmt.Errorf("PEM block %d is not a certificate that parses", len(certs)+1)
	}
	return certs, nil
}

func earliestEnd(certs []*x509.Certificate) time.Time {
	var end time.Time
	for _, c := range certs {
		if end.IsZero() || c.NotAfter.Before(end) {
			end = c.NotAfter
		}
	}
	return end
}
