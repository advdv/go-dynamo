package dynamo

import (
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
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

//E is a convenient builder for creating DynamoDB expressions
type E struct {
	exp     string
	names   map[string]*string
	values  map[string]*dynamodb.AttributeValue
	lastErr error
}

//Exp starts a new expression
func Exp(exp string) *E {
	return &E{
		exp:    exp,
		names:  map[string]*string{},
		values: map[string]*dynamodb.AttributeValue{},
	}
}

//N adds an attribute name
func (e *E) N(placeholder, name string) *E {
	placeholder = strings.TrimLeft(placeholder, "#")
	e.names["#"+placeholder] = aws.String(name)
	return e
}

//V adds an attribute value
func (e *E) V(placeholder string, val interface{}) *E {
	attr, err := dynamodbattribute.Marshal(val)
	if err != nil {
		e.lastErr = err
		return e
	}

	placeholder = strings.TrimLeft(placeholder, ":")
	e.values[":"+placeholder] = attr
	return e
}

//Get returns attribute names and values for the dynamo instruction
func (e *E) Get() (*string, map[string]*string, map[string]*dynamodb.AttributeValue, error) {
	vals := e.values
	if len(vals) < 1 {
		vals = nil
	}

	names := e.names
	if len(names) < 1 {
		names = nil
	}

	return aws.String(e.exp), names, vals, e.lastErr
}

//GetMerged returns a dynamo instruction after merging in existing vals and keys
func (e *E) GetMerged(names map[string]*string, vals map[string]*dynamodb.AttributeValue) (*string, map[string]*string, map[string]*dynamodb.AttributeValue, error) {
	for ph, name := range names {
		e.names[ph] = name
	}

	for ph, val := range vals {
		e.values[ph] = val
	}

	return e.Get()
}
