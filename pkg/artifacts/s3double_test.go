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
// GetObject, HeadObject, DeleteObject, DeleteObjects, and ListObjectsV2 with prefix,
// start-after, max-keys and continuation. It counts each call, so a test can
// say how many bodies a listing fetched and how many list requests a read
// made. A key in refuse is never deleted; DeleteObjects reports it AccessDenied.
type s3Double struct {
	mu           sync.Mutex
	bucket       string
	objects      map[string][]byte
	puts         int
	gets         int
	lists        int
	deletes      int
	batchDeletes int
	batchKeys    []int
	refuse       map[string]bool
	server       *httptest.Server
}

func newS3Double(t *testing.T) *s3Double {
	t.Helper()
	d := &s3Double{bucket: "double", objects: map[string][]byte{}, refuse: map[string]bool{}}
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
	case r.Method == http.MethodHead && key != "":
		body, exists := d.objects[key]
		if !exists {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		w.WriteHeader(http.StatusOK)
	case r.Method == http.MethodDelete && key != "":
		d.deletes++
		if !d.refuse[key] {
			delete(d.objects, key)
		}
		w.WriteHeader(http.StatusNoContent)
	case r.Method == http.MethodPost && key == "" && r.URL.Query().Has("delete"):
		d.batchDeletes++
		d.deleteBatch(w, r)
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
}

type listedContent struct {
	Key  string `xml:"Key"`
	Size int    `xml:"Size"`
}

func (d *s3Double) list(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	prefix := query.Get("prefix")
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
	out := listBucketResult{Name: d.bucket, Prefix: prefix, MaxKeys: maxKeys}
	if len(keys) > maxKeys {
		keys = keys[:maxKeys]
		out.IsTruncated = true
		out.NextContinuationToken = keys[len(keys)-1]
	}
	for _, key := range keys {
		out.Contents = append(out.Contents, listedContent{Key: key, Size: len(d.objects[key])})
	}
	out.KeyCount = len(out.Contents)
	w.Header().Set("Content-Type", "application/xml")
	_, _ = io.WriteString(w, xml.Header)
	_ = xml.NewEncoder(w).Encode(out)
}

type deleteRequest struct {
	Quiet   bool `xml:"Quiet"`
	Objects []struct {
		Key string `xml:"Key"`
	} `xml:"Object"`
}

type deleteResult struct {
	XMLName xml.Name        `xml:"DeleteResult"`
	Deleted []deletedObject `xml:"Deleted"`
	Errors  []deleteError   `xml:"Error"`
}

type deletedObject struct {
	Key string `xml:"Key"`
}

type deleteError struct {
	Key     string `xml:"Key"`
	Code    string `xml:"Code"`
	Message string `xml:"Message"`
}

func (d *s3Double) deleteBatch(w http.ResponseWriter, r *http.Request) {
	var req deleteRequest
	if err := xml.NewDecoder(r.Body).Decode(&req); err != nil || len(req.Objects) > 1000 {
		s3Error(w, http.StatusBadRequest, "MalformedXML")
		return
	}
	d.batchKeys = append(d.batchKeys, len(req.Objects))
	out := deleteResult{}
	for _, object := range req.Objects {
		if d.refuse[object.Key] {
			out.Errors = append(out.Errors, deleteError{Key: object.Key, Code: "AccessDenied", Message: "Access Denied"})
			continue
		}
		delete(d.objects, object.Key)
		if !req.Quiet {
			out.Deleted = append(out.Deleted, deletedObject{Key: object.Key})
		}
	}
	w.Header().Set("Content-Type", "application/xml")
	_, _ = io.WriteString(w, xml.Header)
	_ = xml.NewEncoder(w).Encode(out)
}

func s3Error(w http.ResponseWriter, status int, code string) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	_, _ = io.WriteString(w, xml.Header+"<Error><Code>"+code+"</Code><Message>"+code+"</Message></Error>")
}
