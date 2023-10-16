package sdk

var (
	_ Constraint[int64]   = IntegerConstraintFunc(nil)
	_ Constraint[float64] = NumberConstraintFunc(nil)
)

// -----------------------------------------
type IntegerConstraintFunc func(v int64) bool

func (fn IntegerConstraintFunc) Check(v int64) bool {
	return fn(v)
}

func IntegerLessOrEqual(boundary int64) IntegerConstraintFunc {
	return func(v int64) bool {
		return v <= boundary
	}
}

func IntegerGreaterOrEqual(boundary int64) IntegerConstraintFunc {
	return func(v int64) bool {
		return v >= boundary
	}
}

func IntegerNonNegativeValue() IntegerConstraintFunc {
	return func(v int64) bool {
		return v >= 0
	}
}

func IntegerNonZero() IntegerConstraintFunc {
	return func(v int64) bool {
		return v != 0
	}
}

// -----------------------------------------
type NumberConstraintFunc func(v float64) bool

func (fn NumberConstraintFunc) Check(v float64) bool {
	return fn(v)
}

func NumberLess(boundary float64) NumberConstraintFunc {
	return func(v float64) bool {
		return v < boundary
	}
}

func NumberLessOrEqual(boundary float64) NumberConstraintFunc {
	return func(v float64) bool {
		return v <= boundary
	}
}

func NumberGreater(boundary float64) NumberConstraintFunc {
	return func(v float64) bool {
		return v > boundary
	}
}

func NumberGreaterOrEqual(boundary float64) NumberConstraintFunc {
	return func(v float64) bool {
		return v >= boundary
	}
}

func NumberNonNegativeValue() NumberConstraintFunc {
	return func(v float64) bool {
		return v >= 0
	}
}

func NumberNonZero() NumberConstraintFunc {
	return func(v float64) bool {
		return v != 0
	}
}
