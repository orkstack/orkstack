package artifacts

import (
	"context"
	"encoding/xml"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
)

// s3Double is the smallest S3-compatible server a unit test can point the
// real SDK client at: path-style PutObject (honouring If-None-Match: *),
// GetObject, and ListObjectsV2 with prefix, delimiter, start-after, max-keys
// and continuation. It counts each call, so a test can say how many bodies a
// listing fetched and how many list requests a read made.
type s3Double struct {
	mu      sync.Mutex
	bucket  string
	objects map[string][]byte
	puts    int
	gets    int
	lists   int
	server  *httptest.Server
}

func newS3Double(t *testing.T) *s3Double {
	t.Helper()
	d := &s3Double{bucket: "double", objects: map[string][]byte{}}
	d.server = httptest.NewServer(http.HandlerFunc(d.serve))
	t.Cleanup(d.server.Close)
	return d
}

// config is the artifacts Config that reaches this double.
func (d *s3Double) config() Config {
	return Config{
		Backend: BackendS3, S3Endpoint: d.server.URL, S3Region: "us-east-1", S3Bucket: d.bucket,
		S3AccessKey: "test", S3SecretKey: "test", S3PathStyle: true,
	}
}

func (d *s3Double) deliveryStore(t *testing.T) *S3DeliveryStore {
	t.Helper()
	store, err := NewS3DeliveryStore(context.Background(), d.config())
	if err != nil {
		t.Fatalf("NewS3DeliveryStore: %v", err)
	}
	return store
}

func (d *s3Double) counts() (puts, gets, lists int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.puts, d.gets, d.lists
}

func (d *s3Double) resetCounts() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.puts, d.gets, d.lists = 0, 0, 0
}

func (d *s3Double) object(key string) ([]byte, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	body, ok := d.objects[key]
	return body, ok
}

func (d *s3Double) serve(w http.ResponseWriter, r *http.Request) {
	rest, ok := strings.CutPrefix(r.URL.Path, "/"+d.bucket)
	if !ok {
		s3Error(w, http.StatusNotFound, "NoSuchBucket")
		return
	}
	key := strings.TrimPrefix(rest, "/")
	d.mu.Lock()
	defer d.mu.Unlock()
	switch {
	case r.Method == http.MethodPut && key != "":
		d.puts++
		body, _ := io.ReadAll(r.Body)
		if r.Header.Get("If-None-Match") == "*" {
			if _, exists := d.objects[key]; exists {
				s3Error(w, http.StatusPreconditionFailed, "PreconditionFailed")
				return
			}
		}
		d.objects[key] = body
		w.WriteHeader(http.StatusOK)
	case r.Method == http.MethodGet && key != "":
		d.gets++
		body, exists := d.objects[key]
		if !exists {
			s3Error(w, http.StatusNotFound, "NoSuchKey")
			return
		}
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		_, _ = w.Write(body)
	case r.Method == http.MethodGet && r.URL.Query().Get("list-type") == "2":
		d.lists++
		d.list(w, r)
	default:
		s3Error(w, http.StatusNotImplemented, "NotImplemented")
	}
}

type listBucketResult struct {
	XMLName               xml.Name        `xml:"ListBucketResult"`
	Name                  string          `xml:"Name"`
	Prefix                string          `xml:"Prefix"`
	KeyCount              int             `xml:"KeyCount"`
	MaxKeys               int             `xml:"MaxKeys"`
	IsTruncated           bool            `xml:"IsTruncated"`
	NextContinuationToken string          `xml:"NextContinuationToken,omitempty"`
	Contents              []listedContent `xml:"Contents"`
	CommonPrefixes        []listedPrefix  `xml:"CommonPrefixes"`
}

type listedPrefix struct {
	Prefix string `xml:"Prefix"`
}

type listedContent struct {
	Key  string `xml:"Key"`
	Size int    `xml:"Size"`
}

func (d *s3Double) list(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	prefix := query.Get("prefix")
	delimiter := query.Get("delimiter")
	after := query.Get("start-after")
	if token := query.Get("continuation-token"); token != "" {
		after = token
	}
	maxKeys := 1000
	if raw := query.Get("max-keys"); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n > 0 && n < maxKeys {
			maxKeys = n
		}
	}
	keys := make([]string, 0, len(d.objects))
	for key := range d.objects {
		if strings.HasPrefix(key, prefix) && key > after {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	// With a delimiter, every key with one past the prefix rolls up into its
	// common prefix, and a common prefix counts once against max-keys, as S3
	// does. A continuation from a common prefix resumes past all of it.
	type entry struct {
		key    string
		common bool
	}
	var entries []entry
	for _, key := range keys {
		if delimiter != "" {
			if at := strings.Index(key[len(prefix):], delimiter); at >= 0 {
				common := key[:len(prefix)+at+len(delimiter)]
				if strings.HasSuffix(after, delimiter) && strings.HasPrefix(key, after) {
					continue
				}
				if len(entries) > 0 && entries[len(entries)-1].key == common {
					continue
				}
				entries = append(entries, entry{key: common, common: true})
				continue
			}
		}
		entries = append(entries, entry{key: key})
	}
	out := listBucketResult{Name: d.bucket, Prefix: prefix, MaxKeys: maxKeys}
	if len(entries) > maxKeys {
		entries = entries[:maxKeys]
		out.IsTruncated = true
		out.NextContinuationToken = entries[len(entries)-1].key
	}
	for _, e := range entries {
		if e.common {
			out.CommonPrefixes = append(out.CommonPrefixes, listedPrefix{Prefix: e.key})
			continue
		}
		out.Contents = append(out.Contents, listedContent{Key: e.key, Size: len(d.objects[e.key])})
	}
	out.KeyCount = len(out.Contents) + len(out.CommonPrefixes)
	w.Header().Set("Content-Type", "application/xml")
	_, _ = io.WriteString(w, xml.Header)
	_ = xml.NewEncoder(w).Encode(out)
}

func s3Error(w http.ResponseWriter, status int, code string) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	_, _ = io.WriteString(w, xml.Header+"<Error><Code>"+code+"</Code><Message>"+code+"</Message></Error>")
}
