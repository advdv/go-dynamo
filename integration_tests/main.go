package main

//GameScore represents the the top score a user has achieved
type GameScore struct {
	GameTitle string `dynamodbav:"GameTitle"`
	UserID    string `dynamodbav:"UserId"`
	TopScore  int64  `dynamodbav:"TopScore"`
}
