package dynamo

import (
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
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

//QueryInput holds configuration for a query
type QueryInput struct {
	ExpressionHolder
	dynamodb.QueryInput
	MaxPages int
}

//SetMaxPages limits the number of pages returned
func (qi *QueryInput) SetMaxPages(n int) { qi.MaxPages = n }

//NewQueryInput prepares a query with it mandatory elements
func NewQueryInput(tname, kcond string) *QueryInput {
	return &QueryInput{QueryInput: dynamodb.QueryInput{
		TableName:              aws.String(tname),
		KeyConditionExpression: aws.String(kcond),
	}}
}

// Query reads items of a DynamoDB partition
func Query(db dynamodbiface.DynamoDBAPI, inp *QueryInput, items interface{}) (err error) {
	if inp.MaxPages == 0 {
		inp.MaxPages = 1
	}

	if len(inp.ExpAttrNames) > 0 {
		inp.SetExpressionAttributeNames(aws.StringMap(inp.ExpAttrNames))
	}

	if len(inp.ExpAttrValues) > 0 {
		if inp.ExpressionAttributeValues, err = dynamodbattribute.MarshalMap(inp.ExpAttrValues); err != nil {
			return fmt.Errorf("failed to marshal expression values: %+v", err)
		}
	}

	pageNum := 0
	var lastErr error
	if err = db.QueryPages(&inp.QueryInput,
		func(out *dynamodb.QueryOutput, lastPage bool) bool {
			pageNum++
			err = dynamodbattribute.UnmarshalListOfMaps(out.Items, items)
			if err != nil {
				lastErr = fmt.Errorf("failed to unmarshal items: %+v", err)
				return false
			}

			return pageNum < inp.MaxPages
		}); err != nil {
		return fmt.Errorf("failed to perform request: %+v", err)
	}

	if lastErr != nil {
		return lastErr
	}

	return nil
}
