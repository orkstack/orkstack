package stablejson

import (
	"encoding/json/jsontext"
	"encoding/json/v2"
	"fmt"
	"reflect"
	"sort"
	"strings"
)

// RefuseCaseOnlyMatch refuses a top-level key that names one of v's fields
// only when case is ignored ("URL" for url). encoding/json filled the field
// from it; encoding/json/v2 matches exactly and would drop it without a word,
// so a body that meant to set the field would be read as one that left it
// out. A key no field takes in any case is still ignored. v is a pointer to
// the struct the body was decoded into, and the body is one valid object.
func RefuseCaseOnlyMatch(raw []byte, v any) error {
	t := reflect.TypeOf(v).Elem()
	if t.Kind() != reflect.Struct {
		return nil
	}
	var members map[string]jsontext.Value
	if json.Unmarshal(raw, &members) != nil {
		return nil // not an object: the decode into v already said what it is
	}
	names := map[string]bool{}
	for i := range t.NumField() {
		field := t.Field(i)
		name, _, _ := strings.Cut(field.Tag.Get("json"), ",")
		if !field.IsExported() || name == "-" {
			continue
		}
		if name == "" {
			name = field.Name
		}
		names[name] = true
	}
	keys := make([]string, 0, len(members))
	for key := range members {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if names[key] {
			continue
		}
		for name := range names {
			if strings.EqualFold(key, name) {
				return fmt.Errorf("the top-level object has the key %q, which is not one it takes: did you mean %q? (keys match exactly, case included)", key, name)
			}
		}
	}
	return nil
}
