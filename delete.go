package dynamo

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
)

//Delete holds configuration for a delete
type Delete struct {
	ConditionInput
	ExpressionHolder
	dynamodb.DeleteItemInput
	PrimaryKey interface{}
}

//NewDelete prepares a query with it mandatory elements
func NewDelete(tname string, pk interface{}) *Delete {
	return &Delete{DeleteItemInput: dynamodb.DeleteItemInput{
		TableName: aws.String(tname),
	}, PrimaryKey: pk}
}

//Execute will delete an item with the background context
func (inp *Delete) Execute(db dynamodbiface.DynamoDBAPI) (err error) {
	return inp.ExecuteWithContext(aws.BackgroundContext(), db)
}

// ExecuteWithContext will delete an item from by its primary key
func (inp *Delete) ExecuteWithContext(ctx aws.Context, db dynamodbiface.DynamoDBAPI) (err error) {
	ipk, err := dynamodbattribute.MarshalMap(inp.PrimaryKey)
	if err != nil {
		return fmt.Errorf("failed to marshal primarky key: %+v", err)
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

	if _, err = db.DeleteItemWithContext(ctx, &inp.DeleteItemInput); err != nil {
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
