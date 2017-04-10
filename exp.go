package dynamo

import (
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

//Exp is a convenient builder for creating dynamo expressions
type Exp struct {
	exp     string
	names   map[string]*string
	values  map[string]*dynamodb.AttributeValue
	lastErr error
}

//NewExp starts a new expression
func NewExp(exp string) *Exp {
	return &Exp{
		exp:    exp,
		names:  map[string]*string{},
		values: map[string]*dynamodb.AttributeValue{},
	}
}

//Name adds an attribute name
func (e *Exp) Name(placeholder, name string) *Exp {
	placeholder = strings.TrimLeft(placeholder, "#")
	e.names["#"+placeholder] = aws.String(name)
	return e
}

//Value adds an attribute value
func (e *Exp) Value(placeholder string, val interface{}) *Exp {
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
func (e *Exp) Get() (*string, map[string]*string, map[string]*dynamodb.AttributeValue, error) {
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
func (e *Exp) GetMerged(names map[string]*string, vals map[string]*dynamodb.AttributeValue) (*string, map[string]*string, map[string]*dynamodb.AttributeValue, error) {
	for ph, name := range names {
		e.names[ph] = name
	}

	for ph, val := range vals {
		e.values[ph] = val
	}

	return e.Get()
}
