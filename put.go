package dynamo

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
)

//PutInput holds configuration for getting an item
type PutInput struct {
	ExpressionHolder
	dynamodb.PutItemInput
	ConditionInput
	Item interface{}
}

//NewPutInput prepares a query with it mandatory elements
func NewPutInput(tname string, item interface{}) *PutInput {
	return &PutInput{PutItemInput: dynamodb.PutItemInput{
		TableName: aws.String(tname),
	}, Item: item}
}

// Put will put a item into a DynamoDB table
func Put(db dynamodbiface.DynamoDBAPI, inp *PutInput) (err error) {
	it, err := dynamodbattribute.MarshalMap(inp.Item)
	if err != nil {
		return fmt.Errorf("failed to marshal item map: %+v", err)
	}

	inp.SetItem(it)
	if len(inp.ExpAttrNames) > 0 {
		inp.SetExpressionAttributeNames(aws.StringMap(inp.ExpAttrNames))
	}

	if len(inp.ExpAttrValues) > 0 {
		if inp.ExpressionAttributeValues, err = dynamodbattribute.MarshalMap(inp.ExpAttrValues); err != nil {
			return fmt.Errorf("failed to marshal expression values: %+v", err)
		}
	}

	if _, err = db.PutItem(&inp.PutItemInput); err != nil {
		aerr, ok := err.(awserr.Error)
		if !ok || aerr.Code() != dynamodb.ErrCodeConditionalCheckFailedException {
			return fmt.Errorf("failed to perform request: %+v", err)
		}

		if inp.ConditionError != nil {
			return inp.ConditionError
		}

		return err
	}

	return nil
}
