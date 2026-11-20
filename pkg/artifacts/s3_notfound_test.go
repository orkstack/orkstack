package artifacts

import (
	"errors"
	"net/http"
	"testing"

	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

func TestIsNotFound_OnlyAnAbsentObject(t *testing.T) {
	bare := &smithyhttp.ResponseError{Response: &smithyhttp.Response{Response: &http.Response{StatusCode: http.StatusNotFound}}, Err: errors.New("not found")}
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{"NoSuchKey", &smithy.GenericAPIError{Code: "NoSuchKey"}, true},
		{"NotFound", &smithy.GenericAPIError{Code: "NotFound"}, true},
		{"a bare 404 of a HEAD", bare, true},
		{"no such bucket", &smithy.GenericAPIError{Code: "NoSuchBucket"}, false},
		{"access denied", &smithy.GenericAPIError{Code: "AccessDenied"}, false},
		{"another error", errors.New("connection reset"), false},
	} {
		if got := isNotFound(tc.err); got != tc.want {
			t.Errorf("%s: isNotFound = %v, want %v", tc.name, got, tc.want)
		}
	}
}
