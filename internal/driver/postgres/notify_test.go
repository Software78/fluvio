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

func TestNotifyLimiterDebounce(t *testing.T) {
	l := newNotifyLimiter(50 * time.Millisecond)
	require.True(t, l.Allow("fluvio.default"))
	require.False(t, l.Allow("fluvio.default"))
	require.True(t, l.Allow("fluvio.other"))

	time.Sleep(60 * time.Millisecond)
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
