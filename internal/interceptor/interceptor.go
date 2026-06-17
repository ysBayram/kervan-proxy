package interceptor

type EventType int

const (
	EventValveTransition EventType = iota
	EventBackpressure
	EventTargetFailure
	EventSourceDisconnect
)

type ActionResult struct {
	RuleName string
	Fired    bool
}

type InterceptorChain struct {
	rules []Rule
}

func NewInterceptorChain() *InterceptorChain {
	return &InterceptorChain{}
}

func (ic *InterceptorChain) AddRule(r Rule) {
	ic.rules = append(ic.rules, r)
}

func (ic *InterceptorChain) Evaluate() []ActionResult {
	var results []ActionResult
	for _, r := range ic.rules {
		if r.Predicate != nil && r.Predicate() {
			if r.Action != nil {
				r.Action()
			}
			results = append(results, ActionResult{RuleName: r.Name, Fired: true})
		} else {
			results = append(results, ActionResult{RuleName: r.Name, Fired: false})
		}
	}
	return results
}

func (ic *InterceptorChain) Rules() []Rule {
	return ic.rules
}
