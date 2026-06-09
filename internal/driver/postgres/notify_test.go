package postgres

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/software78/fluvio/internal/driver"
)

func TestQueueNotifyChannel(t *testing.T) {
	require.Equal(t, "fluvio.default", queueNotifyChannel(""))
	require.Equal(t, "fluvio.emails", queueNotifyChannel("emails"))
}

type fakeClock struct{ t time.Time }

func (c *fakeClock) Now() time.Time { return c.t }

func (c *fakeClock) Advance(d time.Duration) { c.t = c.t.Add(d) }

func TestNotifyLimiterDebounce(t *testing.T) {
	clk := &fakeClock{t: time.Unix(0, 0)}
	l := newNotifyLimiter(50 * time.Millisecond)
	l.clock = clk
	require.True(t, l.Allow("fluvio.default"))
	require.False(t, l.Allow("fluvio.default"))
	require.True(t, l.Allow("fluvio.other"))

	clk.Advance(60 * time.Millisecond)
	require.True(t, l.Allow("fluvio.default"))
}

func TestConfigureNotifyPollOnly(t *testing.T) {
	d := &Driver{notifyLimiter: newNotifyLimiter(time.Millisecond)}
	d.ConfigureNotify(true, 0)
	require.True(t, d.pollOnly)
	require.Equal(t, 100*time.Millisecond, d.notifyLimiter.cooldown)
}

func TestMaybeNotifySkipsWhenPollOnly(t *testing.T) {
	d := &Driver{
		pollOnly:      true,
		notifyLimiter: newNotifyLimiter(time.Millisecond),
	}
	err := d.maybeNotifyQueue(t.Context(), nil, driver.QueueDefault)
	require.NoError(t, err)
}
