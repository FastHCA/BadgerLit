package sdk

var (
	_ error = Error("")
)

type Error string

func (e Error) Error() string {
	return string(e)
}
