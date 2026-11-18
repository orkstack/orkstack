package telemetry

import "testing"

func TestObjectLabelsFrom(t *testing.T) {
	for _, tc := range []struct {
		name    string
		value   string
		set     bool
		want    bool
		wantErr bool
	}{
		{name: "unset is off", want: false},
		{name: "empty is off", set: true, value: "  ", want: false},
		{name: "true", set: true, value: "true", want: true},
		{name: "false", set: true, value: "false", want: false},
		{name: "not a boolean", set: true, value: "sometimes", wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := objectLabelsFrom(func(key string) (string, bool) {
				if key != ObjectLabelsVar {
					t.Fatalf("read %q, want only %q", key, ObjectLabelsVar)
				}
				return tc.value, tc.set
			})
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, want error %v", err, tc.wantErr)
			}
			if !tc.wantErr && got != tc.want {
				t.Fatalf("object labels = %v, want %v", got, tc.want)
			}
		})
	}
}
