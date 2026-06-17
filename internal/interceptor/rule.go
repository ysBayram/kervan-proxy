package interceptor

type Rule struct {
	Name      string
	Predicate func() bool
	Action    func()
}

func NewRule(name string, predicate func() bool, action func()) Rule {
	return Rule{
		Name:      name,
		Predicate: predicate,
		Action:    action,
	}
}
