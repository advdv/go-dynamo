package dynamo

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
)

//QueryInput holds configuration for a query
type QueryInput struct {
	PagingInput
	ExpressionHolder
	dynamodb.QueryInput
}

//NewQueryInput prepares a query with it mandatory elements
func NewQueryInput(tname, kcond string) *QueryInput {
	return &QueryInput{QueryInput: dynamodb.QueryInput{
		TableName:              aws.String(tname),
		KeyConditionExpression: aws.String(kcond),
	}}
}

// Query reads items of a DynamoDB partition
func Query(db dynamodbiface.DynamoDBAPI, inp *QueryInput, items interface{}) (count int64, err error) {
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
	if err = db.QueryPages(&inp.QueryInput,
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
