package dynamo

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/pkg/errors"
)

// Put an item into a dynamodb table
func Put(db dynamodbiface.DynamoDBAPI, tname string, item interface{}, cond *Exp, condErr error) (err error) {
	it, err := dynamodbattribute.MarshalMap(item)
	if err != nil {
		return errors.Wrap(err, "failed to marshal item map")
	}

	inp := &dynamodb.PutItemInput{
		TableName: aws.String(tname),
		Item:      it,
	}

	if cond != nil {
		inp.ConditionExpression, inp.ExpressionAttributeNames, inp.ExpressionAttributeValues, err = cond.Get()
		if err != nil {
			return errors.Wrap(err, "error in conditional expression")
		}
	}

	if _, err = db.PutItem(inp); err != nil {
		aerr, ok := err.(awserr.Error)
		if !ok || aerr.Code() != dynamodb.ErrCodeConditionalCheckFailedException {
			return errors.Wrap(err, "failed to put item")
		}

		if condErr != nil {
			return condErr
		}
		return err
	}

	return nil
}
