package artifacts

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
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
// GetObject, and ListObjectsV2 with prefix, start-after, max-keys and
// continuation. It counts each call, so a test can say how many bodies a
// listing fetched and how many list requests a read made.
type s3Double struct {
	mu      sync.Mutex
	bucket  string
	objects map[string][]byte
	puts    int
	gets    int
	lists   int
	server  *httptest.Server

	// headers is the last PutObject (or CreateMultipartUpload) request's
	// headers per key, for content-type, metadata and checksum assertions.
	headers map[string]http.Header
	// uploads holds in-progress multipart uploads by upload id.
	uploads map[string]*doubleUpload
	// partRequests counts UploadPart requests per part number; failParts
	// makes the next n requests for a part number answer 500.
	partRequests map[int]int
	failParts    map[int]int
	aborts       int
	completes    int
	// onRequest, when set, runs at the start of every request (outside the
	// lock), so a test can observe the client's state mid-upload.
	onRequest func(*http.Request)
}

type doubleUpload struct {
	key   string
	parts map[int][]byte
}

func newS3Double(t *testing.T) *s3Double {
	t.Helper()
	d := &s3Double{bucket: "double", objects: map[string][]byte{},
		headers: map[string]http.Header{}, uploads: map[string]*doubleUpload{},
		partRequests: map[int]int{}, failParts: map[int]int{}}
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
	if d.onRequest != nil {
		d.onRequest(r)
	}
	rest, ok := strings.CutPrefix(r.URL.Path, "/"+d.bucket)
	if !ok {
		s3Error(w, http.StatusNotFound, "NoSuchBucket")
		return
	}
	key := strings.TrimPrefix(rest, "/")
	d.mu.Lock()
	defer d.mu.Unlock()
	query := r.URL.Query()
	if d.serveMultipart(w, r, key, query) {
		return
	}
	switch {
	case r.Method == http.MethodPut && key != "":
		d.puts++
		body, _ := io.ReadAll(r.Body)
		// Like S3 and MinIO: a supplied x-amz-checksum-sha256 is verified
		// against the bytes received, and a mismatch stores nothing.
		if want := r.Header.Get("X-Amz-Checksum-Sha256"); want != "" {
			sum := sha256.Sum256(body)
			if base64.StdEncoding.EncodeToString(sum[:]) != want {
				s3Error(w, http.StatusBadRequest, "BadDigest")
				return
			}
		}
		d.headers[key] = r.Header.Clone()
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

// serveMultipart answers the four multipart operations the upload manager
// uses, and reports whether the request was one of them.
func (d *s3Double) serveMultipart(w http.ResponseWriter, r *http.Request, key string, query map[string][]string) bool {
	has := func(name string) bool { _, ok := query[name]; return ok }
	get := func(name string) string {
		if v := query[name]; len(v) > 0 {
			return v[0]
		}
		return ""
	}
	switch {
	case r.Method == http.MethodPost && has("uploads"):
		id := "upload-" + strconv.Itoa(len(d.uploads)+1)
		d.uploads[id] = &doubleUpload{key: key, parts: map[int][]byte{}}
		d.headers[key] = r.Header.Clone()
		w.Header().Set("Content-Type", "application/xml")
		_, _ = io.WriteString(w, xml.Header+"<InitiateMultipartUploadResult><Bucket>"+d.bucket+
			"</Bucket><Key>"+key+"</Key><UploadId>"+id+"</UploadId></InitiateMultipartUploadResult>")
		return true
	case r.Method == http.MethodPut && has("partNumber") && has("uploadId"):
		n, _ := strconv.Atoi(get("partNumber"))
		d.partRequests[n]++
		upload := d.uploads[get("uploadId")]
		body, _ := io.ReadAll(r.Body)
		if d.failParts[n] > 0 {
			d.failParts[n]--
			s3Error(w, http.StatusInternalServerError, "InternalError")
			return true
		}
		if upload == nil {
			s3Error(w, http.StatusNotFound, "NoSuchUpload")
			return true
		}
		upload.parts[n] = body
		w.Header().Set("ETag", `"etag-`+strconv.Itoa(n)+`"`)
		w.WriteHeader(http.StatusOK)
		return true
	case r.Method == http.MethodPost && has("uploadId"):
		upload := d.uploads[get("uploadId")]
		if upload == nil {
			s3Error(w, http.StatusNotFound, "NoSuchUpload")
			return true
		}
		numbers := make([]int, 0, len(upload.parts))
		for n := range upload.parts {
			numbers = append(numbers, n)
		}
		sort.Ints(numbers)
		var whole []byte
		for _, n := range numbers {
			whole = append(whole, upload.parts[n]...)
		}
		d.objects[upload.key] = whole
		d.completes++
		delete(d.uploads, get("uploadId"))
		w.Header().Set("Content-Type", "application/xml")
		_, _ = io.WriteString(w, xml.Header+"<CompleteMultipartUploadResult><Bucket>"+d.bucket+
			"</Bucket><Key>"+upload.key+"</Key><ETag>\"whole\"</ETag></CompleteMultipartUploadResult>")
		return true
	case r.Method == http.MethodDelete && has("uploadId"):
		d.aborts++
		delete(d.uploads, get("uploadId"))
		w.WriteHeader(http.StatusNoContent)
		return true
	}
	return false
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

func s3Error(w http.ResponseWriter, status int, code string) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	_, _ = io.WriteString(w, xml.Header+"<Error><Code>"+code+"</Code><Message>"+code+"</Message></Error>")
}
