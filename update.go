package dynamo

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
)

//UpdateInput holds configuration for a delete
type UpdateInput struct {
	ConditionInput
	ExpressionHolder
	dynamodb.UpdateItemInput
	PrimaryKey interface{}
}

//NewUpdateInput prepares a query with it mandatory elements
func NewUpdateInput(tname string, pk interface{}) *UpdateInput {
	return &UpdateInput{UpdateItemInput: dynamodb.UpdateItemInput{
		TableName: aws.String(tname),
	}, PrimaryKey: pk}
}

// Update an item in a DynamoDB table by its primary key pk with exp
func Update(db dynamodbiface.DynamoDBAPI, inp *UpdateInput) (err error) {
	ipk, err := dynamodbattribute.MarshalMap(inp.PrimaryKey)
	if err != nil {
		return fmt.Errorf("failed to marshal primary key: %+v", err)
	}

	inp.SetKey(ipk)
	if len(inp.ExpAttrNames) > 0 {
		inp.SetExpressionAttributeNames(aws.StringMap(inp.ExpAttrNames))
	}

	if len(inp.ExpAttrValues) > 0 {
		if inp.ExpressionAttributeValues, err = dynamodbattribute.MarshalMap(inp.ExpAttrValues); err != nil {
			return fmt.Errorf("failed to marshal expression values: %+v", err)
		}
	}

	if _, err = db.UpdateItem(&inp.UpdateItemInput); err != nil {
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
