package stablejson

import (
	"math"
	"time"
)

type corpusInner struct {
	X int    `json:"x,omitzero"`
	Y string `json:"y"`
}

type corpusRecord struct {
	Name    string            `json:"name"`
	Count   int               `json:"count,omitzero"`
	Flag    bool              `json:"flag,omitzero"`
	Tags    []string          `json:"tags"`
	Opt     []string          `json:"opt,omitempty"`
	Labels  map[string]string `json:"labels"`
	Bytes   []byte            `json:"bytes"`
	When    time.Time         `json:"when"`
	Inner   corpusInner       `json:"inner"`
	Ptr     *corpusInner      `json:"ptr,omitzero"`
	Raw     RawJSON           `json:"raw"`
	Num     Number            `json:"num"`
	Any     any               `json:"any"`
	Ratio   float64           `json:"ratio"`
	Untaged string
}

// corpus is what the golden tests encode: every shape whose v1 bytes the
// module records somewhere.
func corpus() []any {
	return []any{
		nil,
		"plain",
		"a<b>&c d e\x7f\x01\b\f\t\"\\/é😀",
		"bad \xff utf-8",
		[]string(nil),
		[]string{},
		map[string]any(nil),
		map[string]any{},
		map[string]any{"b": 1, "a": []any{true, nil, "x"}, "B": map[string]any{"z": 1.5, "y": "<"}, "�": 1, "\U0001F600": 2},
		[]byte("bytes"),
		[]byte(nil),
		1e21, 1e20, 1e-7, 0.1, math.Copysign(0, -1), 123456789.125, float32(0.1),
		int64(-9007199254740993), uint64(18446744073709551615),
		corpusRecord{},
		corpusRecord{
			Name: "n<", Count: 3, Flag: true, Tags: []string{"t"}, Opt: []string{"o"},
			Labels: map[string]string{"z": "1", "a": "2"}, Bytes: []byte{0, 1, 2},
			When: time.Date(2026, 11, 8, 1, 2, 3, 4, time.UTC), Inner: corpusInner{X: 1, Y: "y"},
			Ptr: &corpusInner{}, Raw: RawJSON(`{"k" : [1, "<"]}`), Num: Number("1.50"),
			Any: map[string]any{"q": []any{1, "2"}}, Ratio: 0.5, Untaged: "u",
		},
		struct {
			R RawJSON `json:"r"`
		}{},
	}
}
