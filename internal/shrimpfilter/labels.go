package shrimpfilter

import (
	"strings"

	"github.com/go-faster/jx"
)

func ExtractLabels(data string) map[string]string {
	if data == "" {
		return map[string]string{}
	}
	var resRaw, attrRaw []byte
	var level, body, traceID, spanID string
	extra := make(map[string]string)

	d := jx.DecodeStr(data)
	if err := d.ObjBytes(func(d *jx.Decoder, key []byte) error {
		switch string(key) {
		case "resource":
			b, err := d.Raw()
			if err == nil {
				resRaw = b
			}
			return nil
		case "attributes":
			b, err := d.Raw()
			if err == nil {
				attrRaw = b
			}
			return nil
		case "severity_text":
			s, err := d.Str()
			if err == nil {
				level = s
			}
			return nil
		case "body":
			if d.Next() == jx.String {
				s, _ := d.Str()
				body = s
			} else {
				b, _ := d.Raw()
				body = string(b)
			}
			return nil
		case "trace_id":
			s, _ := d.Str()
			traceID = s
			return nil
		case "span_id":
			s, _ := d.Str()
			spanID = s
			return nil
		default:
			// Capture unknown top-level string/scalar keys as labels.
			// Handles non-OTLP JSON shapes (e.g. ch2shrimpd imports)
			// where labels like service_name sit at the top level.
			k := string(key)
			switch d.Next() {
			case jx.String:
				v, _ := d.Str()
				extra[strings.ReplaceAll(k, ".", "_")] = v
			case jx.Number:
				v, _ := d.Num()
				extra[strings.ReplaceAll(k, ".", "_")] = string(v)
			default:
				return d.Skip()
			}
			return nil
		}
	}); err != nil {
		return map[string]string{}
	}

	labels := make(map[string]string)
	if len(resRaw) > 0 {
		decodeAttrMapRaw(resRaw, labels, true)
	}
	if len(attrRaw) > 0 {
		decodeAttrMapRaw(attrRaw, labels, false)
	}
	for k, v := range extra {
		if _, exists := labels[k]; !exists {
			labels[k] = v
		}
	}
	if level != "" {
		labels["level"] = level
	}
	if body != "" {
		labels["body"] = body
	}
	if traceID != "" {
		labels["trace_id"] = traceID
	}
	if spanID != "" {
		labels["span_id"] = spanID
	}
	return labels
}

func decodeAttrMapRaw(raw []byte, out map[string]string, force bool) {
	d := jx.DecodeBytes(raw)
	// If raw is a JSON-escaped string (e.g. "{\"key\":\"val\"}"), unwrap it.
	if d.Next() == jx.String {
		s, err := d.Str()
		if err != nil || s == "" {
			return
		}
		d = jx.DecodeStr(s)
	}
	_ = d.ObjBytes(func(d *jx.Decoder, key []byte) error {
		k := string(key)
		flat := strings.ReplaceAll(k, ".", "_")
		if !force {
			if _, exists := out[flat]; exists {
				return d.Skip()
			}
		}
		switch d.Next() {
		case jx.String:
			s, _ := d.Str()
			out[flat] = s
		default:
			b, _ := d.Raw()
			out[flat] = string(b)
		}
		return nil
	})
}
