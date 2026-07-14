package channel

import "time"

// Status represents how a channel recording session ended.
type Status int

const (
	StatusCompleted   Status = iota // normal finish (stopped by monitor or graceful exit)
	StatusMaxDuration               // configured max_duration reached
	StatusMaxFilesize               // configured max_filesize reached
	StatusError                     // unexpected error during recording
	StatusDesync                    // audio/video duration drift exceeded threshold
	StatusEnded                     // recorder ended without a requested stop
)

func (s Status) String() string {
	switch s {
	case StatusCompleted:
		return "completed"
	case StatusMaxDuration:
		return "max_duration"
	case StatusMaxFilesize:
		return "max_filesize"
	case StatusError:
		return "error"
	case StatusDesync:
		return "desync"
	case StatusEnded:
		return "ended"
	default:
		return "unknown"
	}
}

// Result is sent from a channel goroutine back to the monitor when recording ends.
type Result struct {
	Username  string
	Session   uint64
	Status    Status
	Err       error
	FastRetry bool
	Duration  time.Duration
	Filesize  int64
	Path      string
}
