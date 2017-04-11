package dynamo

import (
	"strings"
)

//ExpressionHolder makes working with expression attributes easier
type ExpressionHolder struct {
	ExpAttrNames  map[string]string
	ExpAttrValues map[string]interface{}
}

//AddExpressionName adds an dynamo expression name
func (eh *ExpressionHolder) AddExpressionName(placeholder, name string) {
	if eh.ExpAttrNames == nil {
		eh.ExpAttrNames = map[string]string{}
	}

	placeholder = strings.TrimLeft(placeholder, "#")
	eh.ExpAttrNames["#"+placeholder] = name
}

//AddExpressionValue adds an dynamo expression value
func (eh *ExpressionHolder) AddExpressionValue(placeholder string, val interface{}) {
	if eh.ExpAttrValues == nil {
		eh.ExpAttrValues = map[string]interface{}{}
	}

	placeholder = strings.TrimLeft(placeholder, ":")
	eh.ExpAttrValues[":"+placeholder] = val
}

//ConditionInput allows working with condition expressions
type ConditionInput struct {
	ConditionError error
}

//SetConditionError configures the err that returns
func (ci *ConditionInput) SetConditionError(err error) { ci.ConditionError = err }

//PagingInput is used when paging can be configured
type PagingInput struct {
	MaxPages int
}

//SetMaxPages limits the number of pages returned
func (pi *PagingInput) SetMaxPages(n int) { pi.MaxPages = n }
