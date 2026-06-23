package interceptor

import "sync"

type ActionResult struct {
	RuleName string
	Fired    bool
}

type InterceptorChain struct {
	rules []Rule
	mu    sync.Mutex
}

func NewInterceptorChain() *InterceptorChain {
	return &InterceptorChain{}
}

func (ic *InterceptorChain) AddRule(r Rule) {
	ic.mu.Lock()
	ic.rules = append(ic.rules, r)
	ic.mu.Unlock()
}

func (ic *InterceptorChain) Evaluate() []ActionResult {
	return ic.evaluate(nil)
}

func (ic *InterceptorChain) EvaluateEvent(event Event) []ActionResult {
	return ic.evaluate(&event)
}

func (ic *InterceptorChain) evaluate(event *Event) []ActionResult {
	ic.mu.Lock()
	rules := make([]Rule, len(ic.rules))
	copy(rules, ic.rules)
	ic.mu.Unlock()

	var results []ActionResult
	for _, r := range rules {
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
	ic.mu.Lock()
	defer ic.mu.Unlock()
	return ic.rules
}
