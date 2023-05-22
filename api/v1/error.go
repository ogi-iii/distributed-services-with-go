package log_v1

type ErrOffsetOutOfRange struct {
	Offset uint64
}

func (e ErrOffsetOutOfRange) Error() string {
	return e.Error() // TODO: write Error Handling later
}
