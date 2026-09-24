package eventbus

type Result struct {
	Data any
	Err  error
}

func NewResult() *Result {
	return &Result{}
}

func NewResultOK(data any) *Result {
	return &Result{
		Data: data,
	}
}

func NewResultErr(err error) *Result {
	return &Result{
		Err: err,
	}
}
