package service

import (
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strings"
	"unicode/utf16"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// rewriteCodexTurnMetadataJSON replaces only selected top-level identity values.
// The header and the opaque client_metadata string must retain the caller's key
// order, whitespace, Unicode escapes and unknown values. Never marshal the object.
func rewriteCodexTurnMetadataJSON(raw string, rebuildInvalid bool, updates func(map[string]any) map[string]any) string {
	original := raw
	var metadata map[string]any
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.UseNumber()
	if !json.Valid([]byte(raw)) || decoder.Decode(&metadata) != nil || metadata == nil {
		if !rebuildInvalid {
			return original
		}
		raw, metadata = "{}", map[string]any{}
	}
	fields := updates(metadata)
	encoded := make(map[string]string, len(fields))
	for name, value := range fields {
		next, err := marshalCodexTurnMetadataValue(value)
		if err != nil {
			return original
		}
		encoded[name] = next
	}
	out := make([]byte, 0, len(raw))
	offset := 0
	seen := make(map[string]bool, len(fields))
	gjson.Parse(raw).ForEach(func(key, value gjson.Result) bool {
		name := key.String()
		next, ok := encoded[name]
		if !ok {
			return true
		}
		seen[name] = true
		// Preserve an already-correct string's original escape spelling.
		if text, ok := fields[name].(string); ok && value.Type == gjson.String && value.Str == text {
			return true
		}
		// Visit all duplicates, not just the first match returned by Get/Set.
		// Identity derivation still uses encoding/json's last-value-wins rule.
		out = append(out, raw[offset:value.Index]...)
		out = append(out, next...)
		offset = value.Index + len(value.Raw)
		return true
	})
	out = append(out, raw[offset:]...)
	next := string(out)
	// Only absent identity fields are appended; existing fields never move.
	for _, name := range slices.Sorted(maps.Keys(encoded)) {
		if seen[name] {
			continue
		}
		var err error
		next, err = sjson.SetRaw(next, name, encoded[name])
		if err != nil {
			return original
		}
	}
	return codexTurnMetadataASCII(next)
}

// New scalar values follow Codex's ASCII JSON spelling, without HTML escaping.
func marshalCodexTurnMetadataValue(value any) (string, error) {
	raw, err := marshalOpenAIUpstreamJSON(value)
	if err != nil {
		return "", err
	}
	return codexTurnMetadataASCII(string(raw)), nil
}

// codexTurnMetadataASCII escapes non-ASCII as \uXXXX, like the real client's
// to_ascii_json_string (codex-rs utils/string/src/json.rs): turn metadata is also an
// HTTP header. Client text that is already ASCII stays byte-identical, and non-ASCII
// can only appear inside JSON strings, so decoded values do not change.
func codexTurnMetadataASCII(raw string) string {
	for i := 0; i < len(raw); i++ {
		if raw[i] < 0x80 {
			continue
		}
		out := []byte(raw[:i])
		for _, r := range raw[i:] {
			switch {
			case r < 0x80:
				out = append(out, byte(r))
			case r <= 0xffff:
				out = fmt.Appendf(out, `\u%04x`, r)
			default:
				high, low := utf16.EncodeRune(r)
				out = fmt.Appendf(out, `\u%04x\u%04x`, high, low)
			}
		}
		return string(out)
	}
	return raw
}
