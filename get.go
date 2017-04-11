package dynamo

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
)

// Get will retrieve a specific item from a DynamoDB table by its primary key
func Get(db dynamodbiface.DynamoDBAPI, tname string, pk interface{}, item interface{}, proj *E, errItemNil error) (err error) {
	ipk, err := dynamodbattribute.MarshalMap(pk)
	if err != nil {
		return fmt.Errorf("failed to marshal primary key: %+v", err)
	}

	inp := &dynamodb.GetItemInput{
		TableName: aws.String(tname),
		Key:       ipk,
	}

	if proj != nil {
		inp.ProjectionExpression, inp.ExpressionAttributeNames, _, err = proj.Get()
		if err != nil {
			return fmt.Errorf("error in projection expression: %+v", err)
		}
	}

	var out *dynamodb.GetItemOutput
	if out, err = db.GetItem(inp); err != nil {
		return fmt.Errorf("failed to perform request: %+v", err)
	}

	if out.Item == nil {
		return errItemNil
	}

	err = dynamodbattribute.UnmarshalMap(out.Item, item)
	if err != nil {
		return fmt.Errorf("failed to unmarshal item: %+v", err)
	}

	return nil
}
