package enumname_test

import (
	"strings"
	"testing"
	"unicode/utf8"

	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/orkstack/orkstack/internal/enumname"
)

// seedValue finds an enum and one of its values by name, as the indexes the
// fuzz targets draw them by.
func seedValue(tb testing.TB, enums []protoreflect.EnumDescriptor, enum, value string) (uint8, uint8) {
	tb.Helper()
	for i, candidate := range enums {
		if string(candidate.FullName()) != enum {
			continue
		}
		if v := candidate.Values().ByName(protoreflect.Name(value)); v != nil {
			return uint8(i), uint8(v.Index())
		}
	}
	tb.Fatalf("seed: enum %s has no value %s", enum, value)
	return 0, 0
}

// declaredValue reports whether name is a value the enum declares.
func declaredValue(enum protoreflect.EnumDescriptor, name string) bool {
	return enum.Values().ByName(protoreflect.Name(name)) != nil
}

// FuzzProto_ResolvesToADeclaredValueOrNothing states what the dialect
// (docs/JSON.md) promises of any text a client sends: it either names exactly
// one declared value, and then that value's short spelling reads back as the
// same value, or it is returned untouched for the caller to refuse.
func FuzzProto_ResolvesToADeclaredValueOrNothing(f *testing.F) {
	enums := orkstackEnums(f)
	for i := range enums {
		for _, seed := range []string{"", "failed", "STATUS_FAILED", "parent-run", "Run_Again", "  ", "nope", "EVENT_TYPE_", "_", "-"} {
			f.Add(uint8(i), seed)
		}
	}
	f.Fuzz(func(t *testing.T, which uint8, in string) {
		enum := enums[int(which)%len(enums)]
		got := enumname.Proto(enum, in)
		if got == in {
			return // unknown, or already declared: the caller decides
		}
		if !declaredValue(enum, got) {
			t.Fatalf("Proto(%s, %q) = %q, which the enum does not declare and is not the input", enum.FullName(), in, got)
		}
		if enumname.Proto(enum, got) != got {
			t.Fatalf("Proto(%s, %q) = %q is not a fixed point", enum.FullName(), in, got)
		}
		if back := enumname.Proto(enum, enumname.Short(enum, got)); back != got {
			t.Fatalf("%s: Proto(Short(%q)) = %q", enum.FullName(), got, back)
		}
	})
}

// FuzzProto_ReadsOnlyASCIICaseAndDashAsTheSameValue: a declared value read
// back from any ASCII casing of its full or short spelling, with "-" for "_",
// is that value; and a spelling that holds a byte outside ASCII is never a
// declared value, because no declared name is spelled with one.
func FuzzProto_ReadsOnlyASCIICaseAndDashAsTheSameValue(f *testing.F) {
	enums := orkstackEnums(f)
	status, succeeded := seedValue(f, enums, "orkstack.v1.Status", "STATUS_SUCCEEDED")
	origin, parentRun := seedValue(f, enums, "orkstack.v1.RunOriginKind", "RUN_ORIGIN_KIND_PARENT_RUN")
	f.Add(status, succeeded, uint64(0), false, false, "", uint8(0))
	f.Add(origin, parentRun, uint64(0xAAAA), true, true, "", uint8(0))
	// U+017F upper-cases to S and U+0131 to I: "\u017fucceeded" and "runn\u0131ng"
	// must not read as SUCCEEDED and RUNNING.
	f.Add(status, succeeded, uint64(0), true, false, string(rune(0x17f)), uint8(0))
	running, runningAt := seedValue(f, enums, "orkstack.v1.Status", "STATUS_RUNNING")
	f.Add(running, runningAt, uint64(0), true, false, string(rune(0x131)), uint8(4))
	f.Fuzz(func(t *testing.T, which, value uint8, caseMask uint64, short, dash bool, junk string, at uint8) {
		enum := enums[int(which)%len(enums)]
		values := enum.Values()
		declared := string(values.Get(int(value) % values.Len()).Name())
		spelling := declared
		if short {
			spelling = enumname.Short(enum, declared)
		}
		var b strings.Builder
		for i := 0; i < len(spelling); i++ {
			c := spelling[i]
			switch {
			case c >= 'A' && c <= 'Z' && caseMask>>(i%64)&1 == 1:
				c += 'a' - 'A'
			case c == '_' && dash:
				c = '-'
			}
			b.WriteByte(c)
		}
		variant := b.String()
		if got := enumname.Proto(enum, variant); got != declared {
			t.Fatalf("%s: Proto(%q) = %q; want %q", enum.FullName(), variant, got, declared)
		}
		if junk == "" {
			return
		}
		p := int(at) % len(variant)
		for _, in := range []string{junk, variant + junk, junk + variant, variant[:p] + junk + variant[p+1:]} {
			nonASCII := false
			for i := 0; i < len(in); i++ {
				nonASCII = nonASCII || in[i] >= utf8.RuneSelf
			}
			if !nonASCII {
				continue
			}
			if got := enumname.Proto(enum, in); got != in {
				t.Fatalf("%s: Proto(%q) = %q; a spelling outside ASCII is not a declared value", enum.FullName(), in, got)
			}
		}
	})
}
