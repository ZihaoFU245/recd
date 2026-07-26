package recorder

import "time"

// Status describes how a recording session ended.
type Status int

const (
	StatusCompleted Status = iota
	StatusMaxDuration
	StatusMaxFilesize
	StatusError
	StatusDesync
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
	default:
		return "unknown"
	}
}

// Result is returned to the monitor after one recording session finishes.
// Session identity and username belong to the monitor, not the recorder.
type Result struct {
	Status    Status
	Err       error
	FastRetry bool
	Duration  time.Duration
	Filesize  int64
	Path      string
}
