package artifacts

import (
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

// AnswerClock is the store's own clock, as its last answer to a request made
// under WithAnswerClock dated itself. Each request replaces it, and one that
// got no answer clears it, so it is read right after the request it is meant
// of. The zero value holds no instant.
type AnswerClock struct {
	mu   sync.Mutex
	sent time.Time
	date time.Time
}

type answerClockKey struct{}

// WithAnswerClock answers a context whose requests to the store are noted on
// the clock returned with it.
func WithAnswerClock(ctx context.Context) (context.Context, *AnswerClock) {
	c := &AnswerClock{}
	return context.WithValue(ctx, answerClockKey{}, c), c
}

// Now is the instant of the store's last answer, in whole seconds as the
// store dates it, and whether there is one to use: an answer with no Date, or
// one dated earlier than its request was sent by more than skew, was kept or
// stamped by something in front of the store and is none.
func (c *AnswerClock) Now(skew time.Duration) (time.Time, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.date.IsZero() || c.date.Before(c.sent.Truncate(time.Second).Add(-skew)) {
		return time.Time{}, false
	}
	return c.date, true
}

func (c *AnswerClock) note(sent, date time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sent, c.date = sent, date
}

// noteAnswerClock notes an answer of the local backend, whose clock is this
// host's.
func noteAnswerClock(ctx context.Context) {
	if c, ok := ctx.Value(answerClockKey{}).(*AnswerClock); ok {
		now := time.Now().UTC()
		c.note(now, now.Truncate(time.Second))
	}
}

// answerDate is when the store dated an answer with these headers: zero
// when it has no Date, one that does not parse, or an Age or a Via, which
// something that kept or relayed the answer adds.
func answerDate(h http.Header) time.Time {
	if h.Get("Age") != "" || h.Get("Via") != "" {
		return time.Time{}
	}
	date, err := http.ParseTime(h.Get("Date"))
	if err != nil {
		return time.Time{}
	}
	return date.UTC()
}

// observeAnswerClock notes each attempt's answer, an error's included, on the
// clock its context carries. A request that got no answer leaves none.
func observeAnswerClock(stack *middleware.Stack) error {
	return stack.Deserialize.Add(middleware.DeserializeMiddlewareFunc("OrkStackAnswerClock",
		func(ctx context.Context, in middleware.DeserializeInput, next middleware.DeserializeHandler) (middleware.DeserializeOutput, middleware.Metadata, error) {
			c, ok := ctx.Value(answerClockKey{}).(*AnswerClock)
			if !ok {
				return next.HandleDeserialize(ctx, in)
			}
			sent := time.Now().UTC()
			out, md, err := next.HandleDeserialize(ctx, in)
			var date time.Time
			if resp, ok := out.RawResponse.(*smithyhttp.Response); ok && resp != nil && resp.Response != nil {
				date = answerDate(resp.Header)
			}
			c.note(sent, date)
			return out, md, err
		}), middleware.After)
}
