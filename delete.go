package dynamo

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
)

// Delete a item from a DynamoDB table by its primary key
func Delete(db dynamodbiface.DynamoDBAPI, tname string, pk interface{}, cond *Exp, condErr error) (err error) {
	ipk, err := dynamodbattribute.MarshalMap(pk)
	if err != nil {
		return fmt.Errorf("failed to marshal primarky key: %+v", err)
	}

	inp := &dynamodb.DeleteItemInput{
		TableName: aws.String(tname),
		Key:       ipk,
	}

	if cond != nil {
		inp.ConditionExpression, inp.ExpressionAttributeNames, inp.ExpressionAttributeValues, err = cond.Get()
		if err != nil {
			return fmt.Errorf("error in conditional expression: %+v", err)
		}
	}

	if _, err = db.DeleteItem(inp); err != nil {
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
