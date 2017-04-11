package dynamo

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
)

//GetInput holds configuration for getting an item
type GetInput struct {
	ExpressionHolder
	dynamodb.GetItemInput
	ItemNilError error
	PrimaryKey   interface{}
}

//SetItemNilError allows for configured the error (if any) when nothing is found
func (gi *GetInput) SetItemNilError(err error) { gi.ItemNilError = err }

//NewGetInput prepares a query with it mandatory elements
func NewGetInput(tname string, pk interface{}) *GetInput {
	return &GetInput{GetItemInput: dynamodb.GetItemInput{
		TableName: aws.String(tname),
	}, PrimaryKey: pk}
}

// Get will retrieve a specific item from a DynamoDB table by its primary key
func Get(db dynamodbiface.DynamoDBAPI, inp *GetInput, item interface{}) (err error) {
	ipk, err := dynamodbattribute.MarshalMap(inp.PrimaryKey)
	if err != nil {
		return fmt.Errorf("failed to marshal primary key: %+v", err)
	}

	inp.SetKey(ipk)
	if len(inp.ExpAttrNames) > 0 {
		inp.SetExpressionAttributeNames(aws.StringMap(inp.ExpAttrNames))
	}

	var out *dynamodb.GetItemOutput
	if out, err = db.GetItem(&inp.GetItemInput); err != nil {
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
