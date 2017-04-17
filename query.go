package dynamo

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
)

//Query holds configuration for a query
type Query struct {
	PagingInput
	ExpressionHolder
	dynamodb.QueryInput
}

//NewQuery prepares a query with it mandatory elements
func NewQuery(tname, kcond string) *Query {
	return &Query{QueryInput: dynamodb.QueryInput{
		TableName:              aws.String(tname),
		KeyConditionExpression: aws.String(kcond),
	}}
}

// Execute will perform the query with a background context
func (inp *Query) Execute(db dynamodbiface.DynamoDBAPI, items interface{}) (count int64, err error) {
	return inp.ExecuteWithContext(aws.BackgroundContext(), db, items)
}

// ExecuteWithContext will perform the query
func (inp *Query) ExecuteWithContext(ctx aws.Context, db dynamodbiface.DynamoDBAPI, items interface{}) (count int64, err error) {
	if inp.MaxPages == 0 {
		inp.MaxPages = 1
	}

	if len(inp.ExpAttrNames) > 0 {
		inp.SetExpressionAttributeNames(aws.StringMap(inp.ExpAttrNames))
	}

	if len(inp.ExpAttrValues) > 0 {
		if inp.ExpressionAttributeValues, err = dynamodbattribute.MarshalMap(inp.ExpAttrValues); err != nil {
			return 0, fmt.Errorf("failed to marshal expression values: %+v", err)
		}
	}

	pageNum := 0
	var lastErr error
	if err = db.QueryPagesWithContext(ctx, &inp.QueryInput,
		func(out *dynamodb.QueryOutput, lastPage bool) bool {
			count += aws.Int64Value(out.Count)
			pageNum++

			if len(out.Items) > 0 {
				err = dynamodbattribute.UnmarshalListOfMaps(out.Items, items)
				if err != nil {
					lastErr = fmt.Errorf("failed to unmarshal items: %+v", err)
					return false
				}
			}

			return pageNum < inp.MaxPages
		}); err != nil {
		return count, fmt.Errorf("failed to perform request: %+v", err)
	}

	if lastErr != nil {
		return count, lastErr
	}

	return count, nil
}
