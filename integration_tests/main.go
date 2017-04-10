package main

import "errors"

var (
	//ErrGameScoreExists is returned when we expect a score not to exist
	ErrGameScoreExists = errors.New("game score already exists")

	//ErrGameScoreNotExists is returned when we expect a score to exist
	ErrGameScoreNotExists = errors.New("game score doesn't exist")
)

//GameScorePK is the primary key of a game score
type GameScorePK struct {
	GameTitle string `dynamodbav:"GameTitle"`
	UserID    string `dynamodbav:"UserId"`
}

//GameScore represents the the top score a user has achieved
type GameScore struct {
	GameScorePK
	TopScore int64 `dynamodbav:"TopScore"`
}
