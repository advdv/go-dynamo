package dynamo

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
)

// Update an item in a DynamoDB table by its primary key pk with exp
func Update(db dynamodbiface.DynamoDBAPI, tname string, pk interface{}, upd *E, cond *E, condErr error) (err error) {
	ipk, err := dynamodbattribute.MarshalMap(pk)
	if err != nil {
		return fmt.Errorf("failed to marshal primary key: %+v", err)
	}

	inp := &dynamodb.UpdateItemInput{
		Key:       ipk,
		TableName: aws.String(tname),
	}

	if upd != nil {
		inp.UpdateExpression, inp.ExpressionAttributeNames, inp.ExpressionAttributeValues, err = upd.Get()
		if err != nil {
			return fmt.Errorf("error in update expression: %+v", err)
		}
	}

	if cond != nil {
		inp.ConditionExpression, inp.ExpressionAttributeNames, inp.ExpressionAttributeValues, err = cond.GetMerged(inp.ExpressionAttributeNames, inp.ExpressionAttributeValues)
		if err != nil {
			return fmt.Errorf("error in conditional expression: %+v", err)
		}
	}

	if _, err = db.UpdateItem(inp); err != nil {
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
