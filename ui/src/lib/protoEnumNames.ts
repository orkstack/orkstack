import type { DescEnum, DescMessage, JsonObject, JsonValue } from '@bufbuild/protobuf';

// The engine's JSON dialect (internal/api/json.go, internal/enumname) writes
// every enum value without its type's prefix: "RUNNING", never
// "STATUS_RUNNING", in nested messages too. The generated decoder (fromJson)
// knows only the declared names, and throws on any other. withProtoEnumNames
// restores the declared name of every enum value a frame holds, walking the
// message's own descriptor, so a field the proto adds later is restored with
// no change here: restoring only some fields once made fromJson throw on a
// task's interruption notice (Event.interruption.phase) and drop the frame.
//
// It mirrors enumname.Proto: a declared name is kept; otherwise the prefix is
// put back when that names a declared value; anything else is left for
// fromJson to refuse.

const isUpper = (c: string) => c !== c.toLowerCase();
const isLower = (c: string) => c !== c.toUpperCase();

/**
 * prefixOf is enumname.Prefix, rune for rune: the enum's name in UPPER_SNAKE
 * and "_", a word starting at each capital that follows a non-capital and at
 * the last capital of an acronym (TLSMode is TLS_MODE_).
 */
export function prefixOf(enumName: string): string {
	const name = [...enumName];
	let prefix = '';
	name.forEach((r, i) => {
		if (i > 0 && isUpper(r)) {
			const startsWord = !isUpper(name[i - 1]);
			const endsAcronym = i + 1 < name.length && isLower(name[i + 1]);
			if (startsWord || endsAcronym) prefix += '_';
		}
		prefix += r.toUpperCase();
	});
	return prefix + '_';
}

function declaredName(enumDesc: DescEnum, value: JsonValue): JsonValue {
	if (typeof value !== 'string') return value; // a number, or not an enum value at all
	const declared = (name: string) => enumDesc.values.some((v) => v.name === name);
	if (declared(value)) return value;
	const long = prefixOf(enumDesc.name) + value;
	return declared(long) ? long : value;
}

function isObject(value: JsonValue): value is JsonObject {
	return typeof value === 'object' && value !== null && !Array.isArray(value);
}

// The well-known types have JSON forms of their own (a Timestamp is a string,
// a Struct any JSON) and hold no OrkStack enum: they are never walked.
function walked(message: DescMessage): boolean {
	return !message.typeName.startsWith('google.protobuf.');
}

/**
 * withProtoEnumNames returns `frame` with every enum value of `schema`'s
 * fields, at any depth, under its declared name. The frame is not modified.
 */
export function withProtoEnumNames(frame: JsonValue, schema: DescMessage): JsonValue {
	if (!isObject(frame)) return frame;
	const restored: JsonObject = { ...frame };
	for (const field of schema.fields) {
		const key = field.jsonName in restored ? field.jsonName : field.name;
		const value = restored[key];
		if (value === undefined || value === null) continue;
		switch (field.fieldKind) {
			case 'enum':
				restored[key] = declaredName(field.enum, value);
				break;
			case 'message':
				if (walked(field.message)) restored[key] = withProtoEnumNames(value, field.message);
				break;
			case 'list':
				if (!Array.isArray(value)) break;
				if (field.listKind === 'enum') {
					const enumDesc = field.enum;
					restored[key] = value.map((v) => declaredName(enumDesc, v));
				} else if (field.listKind === 'message' && walked(field.message)) {
					const message = field.message;
					restored[key] = value.map((v) => withProtoEnumNames(v, message));
				}
				break;
			case 'map': {
				if (!isObject(value)) break;
				const entries = Object.entries(value);
				if (field.mapKind === 'enum') {
					const enumDesc = field.enum;
					restored[key] = Object.fromEntries(entries.map(([k, v]) => [k, declaredName(enumDesc, v)]));
				} else if (field.mapKind === 'message' && walked(field.message)) {
					const message = field.message;
					restored[key] = Object.fromEntries(entries.map(([k, v]) => [k, withProtoEnumNames(v, message)]));
				}
				break;
			}
		}
	}
	return restored;
}
