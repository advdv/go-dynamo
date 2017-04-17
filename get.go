package dynamo

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
)

//Get holds configuration for getting an item
type Get struct {
	ExpressionHolder
	dynamodb.GetItemInput
	ItemNilError error
	PrimaryKey   interface{}
}

//SetItemNilError allows for configured the error (if any) when nothing is found
func (inp *Get) SetItemNilError(err error) { inp.ItemNilError = err }

//NewGet prepares a query with it mandatory elements
func NewGet(tname string, pk interface{}) *Get {
	return &Get{GetItemInput: dynamodb.GetItemInput{
		TableName: aws.String(tname),
	}, PrimaryKey: pk}
}

//Execute will get an item with the background context
func (inp *Get) Execute(db dynamodbiface.DynamoDBAPI, item interface{}) (err error) {
	return inp.ExecuteWithContext(aws.BackgroundContext(), db, item)
}

// ExecuteWithContext will retrieve a specific item from a DynamoDB table by its primary key
func (inp *Get) ExecuteWithContext(ctx aws.Context, db dynamodbiface.DynamoDBAPI, item interface{}) (err error) {
	ipk, err := dynamodbattribute.MarshalMap(inp.PrimaryKey)
	if err != nil {
		return fmt.Errorf("failed to marshal primary key: %+v", err)
	}

	inp.SetKey(ipk)
	if len(inp.ExpAttrNames) > 0 {
		inp.SetExpressionAttributeNames(aws.StringMap(inp.ExpAttrNames))
	}

	var out *dynamodb.GetItemOutput
	if out, err = db.GetItemWithContext(ctx, &inp.GetItemInput); err != nil {
		return fmt.Errorf("failed to perform request: %+v", err)
	}

	if out.Item == nil {
		return inp.ItemNilError
	}

	err = dynamodbattribute.UnmarshalMap(out.Item, item)
	if err != nil {
		return fmt.Errorf("failed to unmarshal item: %+v", err)
	}

	return nil
}
