package artifacts

import (
	"context"
	"errors"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
)

// blobBackends opens each backend a record store can be written over: a
// local directory, and the real S3 client against the in-process double.
func blobBackends(t *testing.T) map[string]blobs {
	t.Helper()
	local, err := openLocalBlobs("blob test", t.TempDir(), "metadata")
	if err != nil {
		t.Fatalf("openLocalBlobs: %v", err)
	}
	t.Cleanup(func() { _ = local.close() })
	remote, err := openS3Blobs(context.Background(), newS3Double(t).config())
	if err != nil {
		t.Fatalf("openS3Blobs: %v", err)
	}
	return map[string]blobs{"local": local, "s3": remote}
}

// TestBlobs_ConditionalWrites holds both backends to one contract: create
// never replaces, replace needs the version it read and never recreates a
// removed object, and a missing object is errBlobNotFound.
func TestBlobs_ConditionalWrites(t *testing.T) {
	ctx := context.Background()
	for name, b := range blobBackends(t) {
		t.Run(name, func(t *testing.T) {
			const key = "metadata/things/one.json"
			if _, _, err := b.get(ctx, key, 0); !errors.Is(err, errBlobNotFound) {
				t.Fatalf("get missing = %v, want errBlobNotFound", err)
			}
			if _, err := b.head(ctx, key); !errors.Is(err, errBlobNotFound) {
				t.Fatalf("head missing = %v, want errBlobNotFound", err)
			}
			if err := b.create(ctx, key, "application/json", []byte("first")); err != nil {
				t.Fatalf("create: %v", err)
			}
			if err := b.create(ctx, key, "application/json", []byte("second")); !errors.Is(err, errBlobExists) {
				t.Fatalf("second create = %v, want errBlobExists", err)
			}
			body, version, err := b.get(ctx, key, 0)
			if err != nil || string(body) != "first" {
				t.Fatalf("get = %q, %v; want first", body, err)
			}
			if size, err := b.head(ctx, key); err != nil || size != 5 {
				t.Fatalf("head = %d, %v; want 5", size, err)
			}
			if limited, _, err := b.get(ctx, key, 2); err != nil || string(limited) != "fi" {
				t.Fatalf("bounded get = %q, %v; want fi", limited, err)
			}
			if err := b.replace(ctx, key, "application/json", []byte("third"), version); err != nil {
				t.Fatalf("replace with the read version: %v", err)
			}
			if err := b.replace(ctx, key, "application/json", []byte("stale"), version); !errors.Is(err, errBlobChanged) {
				t.Fatalf("replace with a stale version = %v, want errBlobChanged", err)
			}
			_, version, _ = b.get(ctx, key, 0)
			if err := b.remove(ctx, key); err != nil {
				t.Fatalf("remove: %v", err)
			}
			if err := b.remove(ctx, key); err != nil {
				t.Fatalf("remove of a missing object: %v", err)
			}
			if err := b.replace(ctx, key, "application/json", []byte("again"), version); !errors.Is(err, errBlobChanged) {
				t.Fatalf("replace after remove = %v, want errBlobChanged", err)
			}
			if _, _, err := b.get(ctx, key, 0); !errors.Is(err, errBlobNotFound) {
				t.Fatalf("replace recreated a removed object: %v", err)
			}
			if err := b.put(ctx, key, "", []byte("plain")); err != nil {
				t.Fatalf("put: %v", err)
			}
			if err := b.put(ctx, key, "", []byte("plain again")); err != nil {
				t.Fatalf("put over an object: %v", err)
			}
		})
	}
}

// TestBlobs_ListInKeyOrder: both backends list in byte order — "a-b/" sorts
// before "a/" although directory "a" sorts before "a-b" by name — page with
// MaxKeys and resume by token, start after a cursor, and roll keys up into
// common prefixes under a delimiter.
func TestBlobs_ListInKeyOrder(t *testing.T) {
	ctx := context.Background()
	keys := []string{
		"metadata/x/a/1", "metadata/x/a/2", "metadata/x/a-b/1", "metadata/x/a-c",
		"metadata/x/b/deep/1", "metadata/x/c", "metadata/y/1",
	}
	for name, b := range blobBackends(t) {
		t.Run(name, func(t *testing.T) {
			for _, key := range keys {
				if err := b.put(ctx, key, "", []byte(key)); err != nil {
					t.Fatalf("put %q: %v", key, err)
				}
			}
			all, err := listKeys(ctx, b, "metadata/x/")
			if err != nil {
				t.Fatalf("list: %v", err)
			}
			want := []string{
				"metadata/x/a-b/1", "metadata/x/a-c", "metadata/x/a/1", "metadata/x/a/2",
				"metadata/x/b/deep/1", "metadata/x/c",
			}
			if !reflect.DeepEqual(all, want) {
				t.Fatalf("list = %q\nwant  %q", all, want)
			}

			var paged []string
			pages := 0
			err = listPages(ctx, b, blobListInput{Prefix: "metadata/x/", MaxKeys: 2}, func(page blobListPage) (bool, error) {
				pages++
				for _, object := range page.Objects {
					paged = append(paged, object.Key)
				}
				return false, nil
			})
			if err != nil || !reflect.DeepEqual(paged, want) || pages != 3 {
				t.Fatalf("paged list = %q in %d pages, %v; want %q in 3", paged, pages, err, want)
			}

			after, err := b.list(ctx, blobListInput{Prefix: "metadata/x/", StartAfter: "metadata/x/a/1"})
			if err != nil {
				t.Fatalf("list after: %v", err)
			}
			if got := objectKeys(after); !reflect.DeepEqual(got, want[3:]) {
				t.Fatalf("list after = %q, want %q", got, want[3:])
			}

			partial, err := listKeys(ctx, b, "metadata/x/a")
			if err != nil || !reflect.DeepEqual(partial, want[:4]) {
				t.Fatalf("list of a partial segment = %q, %v; want %q", partial, err, want[:4])
			}

			grouped, err := b.list(ctx, blobListInput{Prefix: "metadata/x/", Delimiter: "/"})
			if err != nil {
				t.Fatalf("delimited list: %v", err)
			}
			wantPrefixes := []string{"metadata/x/a-b/", "metadata/x/a/", "metadata/x/b/"}
			if !reflect.DeepEqual(grouped.Prefixes, wantPrefixes) ||
				!reflect.DeepEqual(objectKeys(grouped), []string{"metadata/x/a-c", "metadata/x/c"}) {
				t.Fatalf("delimited list = prefixes %q objects %q", grouped.Prefixes, objectKeys(grouped))
			}

			// A delimited listing paged one entry at a time resumes past a
			// common prefix, never into it: each group is listed once.
			var groups []string
			err = listPages(ctx, b, blobListInput{Prefix: "metadata/x/", Delimiter: "/", MaxKeys: 1}, func(page blobListPage) (bool, error) {
				groups = append(groups, page.Prefixes...)
				groups = append(groups, objectKeys(page)...)
				return false, nil
			})
			if want := []string{"metadata/x/a-b/", "metadata/x/a-c", "metadata/x/a/", "metadata/x/b/", "metadata/x/c"}; err != nil || !reflect.DeepEqual(groups, want) {
				t.Fatalf("delimited pages = %q, %v; want %q", groups, err, want)
			}

			failed, err := b.removeKeys(ctx, []string{"metadata/x/a/1", "metadata/x/a/2", "metadata/x/missing"})
			if err != nil || len(failed) != 0 {
				t.Fatalf("removeKeys = %v, %v", failed, err)
			}
			rest, err := listKeys(ctx, b, "metadata/x/")
			if err != nil || !reflect.DeepEqual(rest, []string{"metadata/x/a-b/1", "metadata/x/a-c", "metadata/x/b/deep/1", "metadata/x/c"}) {
				t.Fatalf("after removeKeys = %q, %v", rest, err)
			}
		})
	}
}

// TestBlobs_GetRange reads a byte range and reports the whole object's size;
// a range past the end is errBlobRange.
func TestBlobs_GetRange(t *testing.T) {
	ctx := context.Background()
	for name, b := range blobBackends(t) {
		t.Run(name, func(t *testing.T) {
			const key = "metadata/ranged"
			if _, _, err := b.getRange(ctx, key, 0, 1); !errors.Is(err, errBlobNotFound) {
				t.Fatalf("range of a missing object = %v, want errBlobNotFound", err)
			}
			if err := b.put(ctx, key, "", []byte("0123456789")); err != nil {
				t.Fatalf("put: %v", err)
			}
			data, total, err := b.getRange(ctx, key, 3, 4)
			if err != nil || string(data) != "3456" || total != 10 {
				t.Fatalf("range = %q of %d, %v; want 3456 of 10", data, total, err)
			}
			if _, _, err := b.getRange(ctx, key, 20, 4); !errors.Is(err, errBlobRange) {
				t.Fatalf("range past the end = %v, want errBlobRange", err)
			}
		})
	}
}

// FuzzBlobResolve_AResolvedKeyIsInsideTheStore: a URI either resolves to a key
// inside the store — one that locate renders back to a URI resolving to the
// same key, and that never climbs out of a local directory — or is refused
// with ErrInvalidURI. Nothing else.
func FuzzBlobResolve_AResolvedKeyIsInsideTheStore(f *testing.F) {
	dir := f.TempDir()
	local, err := openLocalBlobs("fuzz", dir)
	if err != nil {
		f.Fatal(err)
	}
	f.Cleanup(func() { _ = local.close() })
	backends := []blobs{local, &s3Blobs{bucket: "bucket"}}
	for _, seed := range []string{
		"s3://bucket/metadata/snapshots/team-a/ork-1-2/7.pb",
		"s3://other/metadata/snapshots/x/7.pb",
		"file://" + dir + "/metadata/snapshots/team-a/ork-1-2/7.pb",
		"file://" + dir + "/../escape",
		"file:///elsewhere/7.pb",
		"https://bucket/x", "%zz", "", "s3://bucket", "file://" + dir,
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, uri string) {
		for _, b := range backends {
			key, err := b.resolve(uri)
			if err != nil {
				if !errors.Is(err, ErrInvalidURI) {
					t.Fatalf("resolve(%q) = %v, want ErrInvalidURI", uri, err)
				}
				continue
			}
			if _, isLocal := b.(*localBlobs); isLocal && (key == ".." || strings.HasPrefix(key, "../") || strings.HasPrefix(key, "/")) {
				t.Fatalf("local resolve(%q) = %q, outside the store", uri, key)
			}
			again, err := b.resolve(b.locate(key))
			if err != nil || again != key {
				t.Fatalf("resolve(locate(%q)) = %q, %v; want the key back", key, again, err)
			}
		}
	})
}

// TestLocalBlobs_APutNeverLandsInAPrunedDirectory: removing the last key of a
// directory prunes it, and a put into that directory racing the removal must
// still land — the directory is created and the object published under the
// lock the pruning holds.
func TestLocalBlobs_APutNeverLandsInAPrunedDirectory(t *testing.T) {
	ctx := context.Background()
	b, err := openLocalBlobs("prune race", t.TempDir(), "metadata")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.close() })
	const rounds = 200
	for i := range rounds {
		dir := "metadata/refs/" + strconv.Itoa(i) + "/"
		if err := b.put(ctx, dir+"first", "", nil); err != nil {
			t.Fatal(err)
		}
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := b.removeKeys(ctx, []string{dir + "first"}); err != nil {
				t.Errorf("removeKeys: %v", err)
			}
		}()
		if err := b.put(ctx, dir+"second", "", nil); err != nil {
			t.Fatalf("round %d: a put racing a prune failed: %v", i, err)
		}
		wg.Wait()
		if _, err := b.head(ctx, dir+"second"); err != nil {
			t.Fatalf("round %d: the put that raced a prune is gone: %v", i, err)
		}
	}
}
