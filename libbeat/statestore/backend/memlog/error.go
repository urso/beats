package memlog

import "errors"

var (
	errRegClosed   = errors.New("registry has been closed")
	errStoreClosed = errors.New("store has been closed")
	errLogInvalid  = errors.New("can not add operation to log file, a checkpoint is required")
	errTxIDInvalid = errors.New("invalid update sequence number")

	errTxIncomplete = errors.New("incomplete transaction")

	errKeyUnknown   = errors.New("key unknown")
	errValueRemoved = errors.New("value has been removed")

	errSigStopEach = errors.New("sig stop each loop")
)
