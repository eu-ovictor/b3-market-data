package loader

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseEntryTime(t *testing.T) {
	hour := 16
	minute := 50
	second := 59
	nanosecond := 559

	entryTimeStr := fmt.Sprintf("%v%v%v%v", hour, minute, second, nanosecond)

	entryTime, err := parseEntryTime(entryTimeStr)

	assert.NoError(t, err, "expected no error parsing entry time, got: %v", err)

	assert.Equal(t, entryTime.Hour(), hour, "expected hour to be %v, got: %v", hour+3, entryTime.Hour())
	assert.Equal(t, entryTime.Minute(), minute, "expected minute to be %v, got: %v", minute, entryTime.Minute())
	assert.Equal(t, entryTime.Second(), second, "expected second to be %v, got: %v", second, entryTime.Second())
	assert.Equal(
		t,
		entryTime.Nanosecond(),
		nanosecond*1000000,
		"expected nanosecond to be %v, got: %v", nanosecond*1000000, entryTime.Hour(),
	)
}
