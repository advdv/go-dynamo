package dynamo

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
)

//ScanInput holds configuration for a query
type ScanInput struct {
	PagingInput
	ExpressionHolder
	dynamodb.ScanInput
}

//NewScanInput prepares a query with it mandatory elements
func NewScanInput(tname string) *ScanInput {
	return &ScanInput{ScanInput: dynamodb.ScanInput{
		TableName: aws.String(tname),
	}}
}

// Scan reads all items (across partitions) in a DynamoDB table or index
func Scan(db dynamodbiface.DynamoDBAPI, inp *ScanInput, items interface{}) (count int64, err error) {
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
	if err = db.ScanPages(&inp.ScanInput,
		func(out *dynamodb.ScanOutput, lastPage bool) bool {
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
