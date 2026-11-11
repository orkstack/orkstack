package artifacts

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// s3Double is the smallest S3-compatible server a unit test can point the
// real SDK client at: path-style PutObject (honouring If-None-Match: * and
// If-Match), GetObject (with an ETag and a byte Range), HeadObject,
// DeleteObject, DeleteObjects, and ListObjectsV2 with prefix, delimiter,
// start-after, max-keys and continuation. It counts each
// call, so a test can say how many bodies a listing fetched and how many list
// requests a read made. A key in refuse is never deleted; DeleteObjects
// reports it AccessDenied.
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
	// failPuts makes the next n PutObject requests of a key answer 500
	// before anything is checked or stored; storedThenFail makes the next n
	// that store their body answer 500 afterwards, as a server whose reply is
	// lost after the write landed. The SDK resends either.
	failPuts       map[string]int
	storedThenFail map[string]int
	// resendsFail is how many PutObject requests of a key fail, as failPuts,
	// once a storedThenFail write of it was answered 500 (loseAnswer).
	resendsFail map[string]int
	// failGets makes the next n GetObject requests of a key answer 500.
	failGets map[string]int
	// ignoreMissingIfMatch makes a PutObject with If-Match on a key that is
	// not there store its body, and ignoreDeleteCondition makes DeleteObject
	// ignore If-Match: a store without the conditional answers a rollback
	// needs (docs/STORAGE.md).
	ignoreMissingIfMatch  bool
	ignoreDeleteCondition bool
	// deleteConditionAnswer, when set, answers every DeleteObject carrying
	// If-Match with that status and deletes nothing: a store that rejects a
	// conditional delete. keepOnDelete answers a DeleteObject as done and
	// keeps the object.
	deleteConditionAnswer int
	keepOnDelete          bool
	// missingIfMatchDelete404 answers a DeleteObject with If-Match on a key
	// that is not there 404, as a store may: a resend of a delete that landed.
	missingIfMatchDelete404 bool
}

type doubleUpload struct {
	key   string
	parts map[int][]byte
}

func newS3Double(t *testing.T) *s3Double {
	t.Helper()
	d := &s3Double{bucket: "double", objects: map[string][]byte{}, refuse: map[string]bool{},
		headers: map[string]http.Header{}, uploads: map[string]*doubleUpload{},
		partRequests: map[int]int{}, failParts: map[int]int{},
		failPuts: map[string]int{}, storedThenFail: map[string]int{}, resendsFail: map[string]int{},
		failGets: map[string]int{}}
	d.server = httptest.NewServer(http.HandlerFunc(d.serve))
	t.Cleanup(d.server.Close)
	return d
}

// loseAnswer makes the next PutObject of key a write whose answer is lost:
// stored, then answered 500. The SDK resends it; failingResends of those fail
// before anything is checked (2 is every resend the SDK makes, so it gives
// up), and a resend after them meets the stored write.
func (d *s3Double) loseAnswer(key string, failingResends int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.storedThenFail[key] = 1
	d.resendsFail[key] = failingResends
}

// fastClient is a client of this double whose SDK retries wait for nothing,
// so a test of a resend does not wait out the SDK's jittered backoff. It is
// otherwise the client newS3Client builds: the same retryer, three attempts.
func (d *s3Double) fastClient(t *testing.T, opts ...func(*s3.Options)) *s3.Client {
	t.Helper()
	awsCfg, err := loadS3Config(context.Background(), d.config())
	if err != nil {
		t.Fatalf("loadS3Config: %v", err)
	}
	return s3.NewFromConfig(awsCfg, append([]func(*s3.Options){func(o *s3.Options) {
		o.BaseEndpoint = aws.String(d.server.URL)
		o.UsePathStyle = true
		o.Retryer = retry.NewStandard(func(so *retry.StandardOptions) { so.Backoff = noBackoff{} })
	}}, opts...)...)
}

type noBackoff struct{}

func (noBackoff) BackoffDelay(int, error) (time.Duration, error) { return 0, nil }

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

// doubleETag is the quoted ETag the double gives a body: its SHA-256, so a
// conditional write sees any change of content.
func doubleETag(body []byte) string {
	sum := sha256.Sum256(body)
	return `"` + hex.EncodeToString(sum[:]) + `"`
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
		if d.failPuts[key] > 0 {
			d.failPuts[key]--
			s3Error(w, http.StatusInternalServerError, "InternalError")
			return
		}
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
		if want := r.Header.Get("If-Match"); want != "" {
			current, exists := d.objects[key]
			if !exists && !d.ignoreMissingIfMatch {
				s3Error(w, http.StatusNotFound, "NoSuchKey")
				return
			}
			if exists && doubleETag(current) != want {
				s3Error(w, http.StatusPreconditionFailed, "PreconditionFailed")
				return
			}
		}
		d.objects[key] = body
		if d.storedThenFail[key] > 0 {
			d.storedThenFail[key]--
			d.failPuts[key] += d.resendsFail[key]
			delete(d.resendsFail, key)
			s3Error(w, http.StatusInternalServerError, "InternalError")
			return
		}
		// Like S3: a stored write is answered with its object's ETag.
		w.Header().Set("ETag", doubleETag(body))
		w.WriteHeader(http.StatusOK)
	case r.Method == http.MethodGet && key != "":
		d.gets++
		if d.failGets[key] > 0 {
			d.failGets[key]--
			s3Error(w, http.StatusInternalServerError, "InternalError")
			return
		}
		body, exists := d.objects[key]
		if !exists {
			s3Error(w, http.StatusNotFound, "NoSuchKey")
			return
		}
		w.Header().Set("ETag", doubleETag(body))
		if spec := r.Header.Get("Range"); spec != "" {
			var first, last int
			if _, err := fmt.Sscanf(spec, "bytes=%d-%d", &first, &last); err != nil || first >= len(body) {
				s3Error(w, http.StatusRequestedRangeNotSatisfiable, "InvalidRange")
				return
			}
			last = min(last, len(body)-1)
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", first, last, len(body)))
			w.Header().Set("Content-Length", strconv.Itoa(last-first+1))
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(body[first : last+1])
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
		w.Header().Set("ETag", doubleETag(body))
		w.WriteHeader(http.StatusOK)
	case r.Method == http.MethodDelete && key != "":
		d.deletes++
		if r.Header.Get("If-Match") != "" && d.deleteConditionAnswer != 0 {
			s3Error(w, d.deleteConditionAnswer, strings.ReplaceAll(http.StatusText(d.deleteConditionAnswer), " ", ""))
			return
		}
		if want := r.Header.Get("If-Match"); want != "" && !d.ignoreDeleteCondition {
			if _, exists := d.objects[key]; !exists && d.missingIfMatchDelete404 {
				s3Error(w, http.StatusNotFound, "NoSuchKey")
				return
			}
			if current, exists := d.objects[key]; exists && doubleETag(current) != want {
				s3Error(w, http.StatusPreconditionFailed, "PreconditionFailed")
				return
			}
		}
		if !d.refuse[key] && !d.keepOnDelete {
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
