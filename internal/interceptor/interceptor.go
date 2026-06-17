package interceptor

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
	return ic.evaluate(nil)
}

func (ic *InterceptorChain) EvaluateEvent(event Event) []ActionResult {
	return ic.evaluate(&event)
}

func (ic *InterceptorChain) evaluate(event *Event) []ActionResult {
	var results []ActionResult
	for _, r := range ic.rules {
		if event != nil && r.OnEvent != EventAny && r.OnEvent != event.Type {
			results = append(results, ActionResult{RuleName: r.Name, Fired: false})
			continue
		}
		if r.Predicate != nil && !r.Predicate() {
			results = append(results, ActionResult{RuleName: r.Name, Fired: false})
			continue
		}
		if r.Action != nil {
			r.Action()
		}
		results = append(results, ActionResult{RuleName: r.Name, Fired: true})
	}
	return results
}

func (ic *InterceptorChain) Rules() []Rule {
	return ic.rules
}
