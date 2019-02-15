package storage

import "errors"

var (
	//ErrMissingDateRange  message
	ErrMissingDateRange = errors.New("start and end dates must be supplied")
)
