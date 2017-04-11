package dynamo

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
)

// Scan reads all items (across partitions) in a DynamoDB table or index
func Scan(db dynamodbiface.DynamoDBAPI, tname, iname string, next func() interface{}, proj *Exp, filt *Exp, limit int64, maxPages int) (err error) {
	if maxPages == 0 {
		maxPages = 1
	}

	inp := &dynamodb.ScanInput{
		TableName: aws.String(tname),
	}

	if iname != "" {
		inp.SetIndexName(iname)
	}
	if proj != nil {
		inp.ProjectionExpression, inp.ExpressionAttributeNames, inp.ExpressionAttributeValues, err = proj.Get()
		if err != nil {
			return fmt.Errorf("error in projection expression: %+v", err)
		}
	}

	if filt != nil {
		inp.FilterExpression, inp.ExpressionAttributeNames, inp.ExpressionAttributeValues, err = filt.GetMerged(inp.ExpressionAttributeNames, inp.ExpressionAttributeValues)
		if err != nil {
			return fmt.Errorf("error in filter expression: %+v", err)
		}
	}

	if limit != 0 {
		inp.SetLimit(limit)
	}

	pageNum := 0
	var lastErr error
	if err = db.ScanPages(inp,
		func(out *dynamodb.ScanOutput, lastPage bool) bool {
			pageNum++
			for _, item := range out.Items {
				next := next()
				err := dynamodbattribute.UnmarshalMap(item, next)
				if err != nil {
					lastErr = fmt.Errorf("failed to unmarshal item: %+v", err)
					return false
				}
			}

			return pageNum < maxPages
		}); err != nil {
		return fmt.Errorf("failed to perform request: %+v", err)
	}

	if lastErr != nil {
		return lastErr
	}

	return nil
}
