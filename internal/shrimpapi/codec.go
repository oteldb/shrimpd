package shrimpapi

import (
	"io"

	"github.com/go-faster/errors"
	"github.com/go-faster/jx"
)

// decodeIngestBatch streams `{"data":[{"timestamp":…,"data":"…"}]}`, calling emit once per entry.
//
// It is streamed rather than unmarshalled so a large batch never materializes twice. The body
// bytes handed to emit are freshly allocated: the caller retains them in a batch that outlives
// the decoder's buffer.
func decodeIngestBatch(d *jx.Decoder, emit func(timestamp int64, body []byte)) error {
	return d.Obj(func(d *jx.Decoder, key string) error {
		if key != "data" {
			return d.Skip()
		}

		return d.Arr(func(d *jx.Decoder) error {
			var (
				timestamp int64
				body      []byte
			)

			if err := d.Obj(func(d *jx.Decoder, field string) error {
				switch field {
				case "timestamp":
					v, err := d.Int64()
					if err != nil {
						return errors.Wrap(err, "decode timestamp")
					}

					timestamp = v

					return nil
				case "data":
					v, err := d.StrBytes()
					if err != nil {
						return errors.Wrap(err, "decode data")
					}

					body = append(body[:0:0], v...)

					return nil
				default:
					return d.Skip()
				}
			}); err != nil {
				return errors.Wrap(err, "decode entry")
			}

			emit(timestamp, body)

			return nil
		})
	})
}

// readAllLimited reads a request body already wrapped in a size limiter, turning the limiter's
// error into a plain one.
func readAllLimited(r io.Reader) ([]byte, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, errors.Wrap(err, "read body")
	}

	return data, nil
}
