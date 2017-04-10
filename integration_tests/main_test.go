package main

import (
	"testing"

	"github.com/advanderveer/go-dynamo"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func TestBasicTable(t *testing.T) {
	sess := newsess(t)
	tname := tablename(t)
	db := dynamodb.New(sess)

	score := &GameScore{"Alien Adventure", "User-5", 100}
	err := dynamo.Put(db, tname, score, nil, nil)
	ok(t, err)
}
