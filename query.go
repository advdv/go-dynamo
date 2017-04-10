package dynamo

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
)

// Query reads a list of items from dynamodb
func Query(db dynamodbiface.DynamoDBAPI, tname, iname string, kcond *Exp, next func() interface{}, proj *Exp, filt *Exp, limit int64, maxPages int) (err error) {
	if maxPages == 0 {
		maxPages = 1
	}

	inp := &dynamodb.QueryInput{
		TableName: aws.String(tname),
	}

	if iname != "" {
		inp.SetIndexName(iname)
	}

	if kcond == nil {
		return fmt.Errorf("must provide a key condition expression")
	}

	inp.KeyConditionExpression, inp.ExpressionAttributeNames, inp.ExpressionAttributeValues, err = kcond.Get()
	if err != nil {
		return fmt.Errorf("error in key condition expression: %+v", err)
	}

	if proj != nil {
		inp.ProjectionExpression, inp.ExpressionAttributeNames, inp.ExpressionAttributeValues, err = proj.GetMerged(inp.ExpressionAttributeNames, inp.ExpressionAttributeValues)
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
	if err = db.QueryPages(inp,
		func(out *dynamodb.QueryOutput, lastPage bool) bool {
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
