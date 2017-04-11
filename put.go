package dynamo

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
)

// Put will put a item into a DynamoDB table
func Put(db dynamodbiface.DynamoDBAPI, tname string, item interface{}, cond *E, condErr error) (err error) {
	it, err := dynamodbattribute.MarshalMap(item)
	if err != nil {
		return fmt.Errorf("failed to marshal item map: %+v", err)
	}

	inp := &dynamodb.PutItemInput{
		TableName: aws.String(tname),
		Item:      it,
	}

	if cond != nil {
		inp.ConditionExpression, inp.ExpressionAttributeNames, inp.ExpressionAttributeValues, err = cond.Get()
		if err != nil {
			return fmt.Errorf("error in conditional expression: %+v", err)
		}
	}

	if _, err = db.PutItem(inp); err != nil {
		aerr, ok := err.(awserr.Error)
		if !ok || aerr.Code() != dynamodb.ErrCodeConditionalCheckFailedException {
			return fmt.Errorf("failed to perform request: %+v", err)
		}

		if condErr != nil {
			return condErr
		}

		return err
	}

	return nil
}
