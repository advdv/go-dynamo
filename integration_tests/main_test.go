package main

import (
	"strings"
	"testing"

	"github.com/advanderveer/go-dynamo"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func TestPutGetUpdateDelete(t *testing.T) {
	sess := newsess(t)
	tname := tablename(t)
	db := dynamodb.New(sess)

	pk1 := GameScorePK{"Alien Adventure", "User-5"}
	score1 := &GameScore{pk1, 100}

	t.Run("Put", func(t *testing.T) {
		t.Run("unconditionally put a game score", func(t *testing.T) {
			in := dynamo.NewPutInput(tname, score1)
			err := dynamo.Put(db, in)
			ok(t, err)
		})

		t.Run("put with conditional exp and custom error", func(t *testing.T) {
			in := dynamo.NewPutInput(tname, score1)
			in.SetConditionExpression("attribute_not_exists(GameTitle)")
			in.SetConditionError(ErrGameScoreExists)
			err := dynamo.Put(db, in)
			equals(t, ErrGameScoreExists, err)
		})

		t.Run("put with a conditional exp and no custom error", func(t *testing.T) {
			in := dynamo.NewPutInput(tname, score1)
			in.SetConditionExpression("attribute_not_exists(GameTitle)")
			err := dynamo.Put(db, in)
			assert(t, strings.Contains(err.Error(), "ConditionalCheckFailedException"), "expected normal conditional failed error, got: %+v", err)
		})
	})

	t.Run("Get", func(t *testing.T) {
		t.Run("get non-existing with no error configured", func(t *testing.T) {
			score2 := &GameScore{}
			in := dynamo.NewGetInput(tname, GameScorePK{"No Such Game", "User-5"})
			err := dynamo.Get(db, in, score2)
			ok(t, err)
			equals(t, "", score2.GameTitle)
		})

		t.Run("get non-existing with an error configured", func(t *testing.T) {
			score3 := &GameScore{}
			in := dynamo.NewGetInput(tname, GameScorePK{"No Such Game", "User-5"})
			in.SetItemNilError(ErrGameScoreNotExists)
			err := dynamo.Get(db, in, score3)
			equals(t, ErrGameScoreNotExists, err)
		})

		t.Run("get an existing gamescore", func(t *testing.T) {
			score4 := &GameScore{}
			in := dynamo.NewGetInput(tname, pk1)
			in.SetItemNilError(ErrGameScoreNotExists)
			err := dynamo.Get(db, in, score4)
			ok(t, err)
			equals(t, score1.GameTitle, score4.GameTitle)
			equals(t, score1.UserID, score4.UserID)
			equals(t, score1.TopScore, score4.TopScore)
		})

		t.Run("get an projected existing gamescore", func(t *testing.T) {
			score5 := &GameScore{}
			in := dynamo.NewGetInput(tname, pk1)
			in.SetProjectionExpression("GameTitle, UserId")
			in.SetItemNilError(ErrGameScoreNotExists)
			err := dynamo.Get(db, in, score5)
			ok(t, err)
			equals(t, score1.GameTitle, score5.GameTitle)
			equals(t, score1.UserID, score5.UserID)
			equals(t, int64(0), score5.TopScore)
		})
	})

	t.Run("Update", func(t *testing.T) {
		t.Run("update non-existing without condition", func(t *testing.T) {
			in := dynamo.NewUpdateInput(tname, GameScorePK{"No Any Game", "User-5"})
			err := dynamo.Update(db, in)
			ok(t, err)
		})

		t.Run("update non-existing with condition and error", func(t *testing.T) {
			in := dynamo.NewUpdateInput(tname, GameScorePK{"No Such Game", "User-5"})
			in.SetUpdateExpression("SET TopScore = :TopScore")
			in.AddExpressionValue(":TopScore", 120)
			in.SetConditionExpression("attribute_exists(GameTitle)")
			in.SetConditionError(ErrGameScoreNotExists)

			err := dynamo.Update(db, in)
			equals(t, ErrGameScoreNotExists, err)
		})

		t.Run("update existing", func(t *testing.T) {
			in := dynamo.NewUpdateInput(tname, pk1)
			in.SetUpdateExpression("SET TopScore = :TopScore")
			in.AddExpressionValue(":TopScore", 120)
			in.SetConditionExpression("attribute_exists(GameTitle)")
			in.SetConditionError(ErrGameScoreNotExists)

			err := dynamo.Update(db, in)
			ok(t, err)

			item := &GameScore{}
			gin := dynamo.NewGetInput(tname, pk1)
			gin.SetItemNilError(ErrGameScoreNotExists)
			err = dynamo.Get(db, gin, item)
			ok(t, err)
			equals(t, int64(120), item.TopScore)
		})
	})

	t.Run("Delete", func(t *testing.T) {
		t.Run("delete non-existing without condition", func(t *testing.T) {
			in := dynamo.NewDeleteInput(tname, GameScorePK{"No Such Game", "User-5"})
			err := dynamo.Delete(db, in)
			ok(t, err)
		})

		t.Run("delete non-existing with condition and error", func(t *testing.T) {
			in := dynamo.NewDeleteInput(tname, GameScorePK{"No Such Game", "User-5"})
			in.SetConditionExpression("attribute_exists(GameTitle)")
			in.SetConditionError(ErrGameScoreNotExists)
			err := dynamo.Delete(db, in)
			equals(t, ErrGameScoreNotExists, err)
		})

		t.Run("delete existing with condition and error", func(t *testing.T) {
			in := dynamo.NewDeleteInput(tname, pk1)
			in.SetConditionExpression("attribute_exists(GameTitle)")
			in.SetConditionError(ErrGameScoreNotExists)
			err := dynamo.Delete(db, in)
			ok(t, err)

			in = dynamo.NewDeleteInput(tname, GameScorePK{"No Any Game", "User-5"})
			err = dynamo.Delete(db, in)
			ok(t, err)
		})
	})
}

func TestQueryScan(t *testing.T) {
	sess := newsess(t)
	tname := tablename(t)
	db := dynamodb.New(sess)

	score1 := &GameScore{GameScorePK{"Alien Adventure", "User-1"}, 20}
	ok(t, dynamo.Put(db, dynamo.NewPutInput(tname, score1)))
	score2 := &GameScore{GameScorePK{"Alien Adventure", "User-2"}, 75}
	ok(t, dynamo.Put(db, dynamo.NewPutInput(tname, score2)))
	score3 := &GameScore{GameScorePK{"Alien Adventure", "User-3"}, 100}
	ok(t, dynamo.Put(db, dynamo.NewPutInput(tname, score3)))
	defer func() {
		ok(t, dynamo.Delete(db, dynamo.NewDeleteInput(tname, score1.GameScorePK)))
		ok(t, dynamo.Delete(db, dynamo.NewDeleteInput(tname, score2.GameScorePK)))
		ok(t, dynamo.Delete(db, dynamo.NewDeleteInput(tname, score3.GameScorePK)))
	}()

	t.Run("Query", func(t *testing.T) {
		t.Run("query all partition items in base table", func(t *testing.T) {
			list := []*GameScore{}

			in := dynamo.NewQueryInput(tname, "GameTitle = :GameTitle")
			in.AddExpressionValue(":GameTitle", "Alien Adventure")

			n, err := dynamo.Query(db, in, &list)
			ok(t, err)

			equals(t, int64(3), n)
			equals(t, 3, len(list))
			equals(t, int64(20), list[0].TopScore)
		})

		t.Run("query all projected in base table", func(t *testing.T) {
			list := []*GameScore{}

			in := dynamo.NewQueryInput(tname, "GameTitle = :GameTitle")
			in.SetProjectionExpression("GameTitle, UserId")
			in.AddExpressionValue(":GameTitle", "Alien Adventure")

			n, err := dynamo.Query(db, in, &list)
			ok(t, err)
			equals(t, int64(3), n)
			equals(t, 3, len(list))
			equals(t, int64(0), list[0].TopScore)
		})

		t.Run("query filtered projection in base table", func(t *testing.T) {
			list := []*GameScore{}

			in := dynamo.NewQueryInput(tname, "GameTitle = :GameTitle")
			in.SetProjectionExpression("GameTitle, UserId")
			in.SetFilterExpression("#ts > :minTopScore")
			in.AddExpressionName("#ts", "TopScore")
			in.AddExpressionValue(":GameTitle", "Alien Adventure")
			in.AddExpressionValue(":minTopScore", 20)

			n, err := dynamo.Query(db, in, &list)
			ok(t, err)
			equals(t, int64(2), n)
			equals(t, 2, len(list))
			equals(t, "User-2", list[0].UserID)
			equals(t, int64(0), list[0].TopScore)
		})

		t.Run("query page filtered projection in base table", func(t *testing.T) {
			list := []*GameScore{}

			in := dynamo.NewQueryInput(tname, "GameTitle = :GameTitle")
			in.SetProjectionExpression("GameTitle, UserId")
			in.SetFilterExpression("#ts > :minTopScore")
			in.AddExpressionName("#ts", "TopScore")
			in.AddExpressionValue(":GameTitle", "Alien Adventure")
			in.AddExpressionValue(":minTopScore", 20)
			in.SetMaxPages(1)
			in.SetLimit(2)

			n, err := dynamo.Query(db, in, &list)
			ok(t, err)
			equals(t, int64(1), n)
			equals(t, 1, len(list))
			equals(t, "User-2", list[0].UserID)
			equals(t, int64(0), list[0].TopScore)
		})

		t.Run("query page filtered projection on index", func(t *testing.T) {
			list := []*GameScore{}

			in := dynamo.NewQueryInput(tname,
				"GameTitle = :GameTitle AND TopScore > :minTopScore")

			in.SetProjectionExpression("GameTitle, TopScore")
			in.SetIndexName("GameTitleIndex")
			in.AddExpressionValue(":GameTitle", "Alien Adventure")
			in.AddExpressionValue(":minTopScore", 20)
			in.SetMaxPages(1)
			in.SetLimit(2)

			n, err := dynamo.Query(db, in, &list)
			ok(t, err)
			equals(t, int64(2), n)
			equals(t, 2, len(list))
			equals(t, int64(75), list[0].TopScore)
			equals(t, "", list[0].UserID)
		})

		t.Run("count page filtered on index", func(t *testing.T) {
			in := dynamo.NewQueryInput(tname,
				"GameTitle = :GameTitle AND TopScore > :minTopScore")

			in.SetIndexName("GameTitleIndex")
			in.AddExpressionValue(":GameTitle", "Alien Adventure")
			in.AddExpressionValue(":minTopScore", 20)
			in.SetMaxPages(1)
			in.SetLimit(2)
			in.SetSelect("COUNT")

			n, err := dynamo.Query(db, in, nil)
			ok(t, err)
			equals(t, int64(2), n)
		})
	})

	t.Run("Scan", func(t *testing.T) {
		t.Run("scan all items in base table", func(t *testing.T) {
			list := []*GameScore{}
			in := dynamo.NewScanInput(tname)

			n, err := dynamo.Scan(db, in, &list)
			ok(t, err)

			equals(t, int64(3), n)
			equals(t, 3, len(list))
			equals(t, int64(20), list[0].TopScore)
		})

		t.Run("scan all projected in base table", func(t *testing.T) {
			list := []*GameScore{}
			in := dynamo.NewScanInput(tname)
			in.SetProjectionExpression("GameTitle, UserId")

			n, err := dynamo.Scan(db, in, &list)
			ok(t, err)

			equals(t, int64(3), n)
			equals(t, 3, len(list))
			equals(t, int64(0), list[0].TopScore)
		})

		t.Run("scan filtered projection in base table", func(t *testing.T) {
			list := []*GameScore{}
			in := dynamo.NewScanInput(tname)
			in.SetProjectionExpression("GameTitle, UserId")
			in.SetFilterExpression("TopScore > :minTopScore")
			in.AddExpressionValue(":minTopScore", 20)

			n, err := dynamo.Scan(db, in, &list)
			ok(t, err)
			equals(t, int64(2), n)
			equals(t, 2, len(list))
			equals(t, "User-2", list[0].UserID)
			equals(t, int64(0), list[0].TopScore)
		})

		t.Run("scan page filtered projection in base table", func(t *testing.T) {
			list := []*GameScore{}
			in := dynamo.NewScanInput(tname)
			in.SetProjectionExpression("GameTitle, UserId")
			in.SetFilterExpression("TopScore > :minTopScore")
			in.AddExpressionValue(":minTopScore", 20)
			in.SetLimit(2)
			in.SetMaxPages(1)

			n, err := dynamo.Scan(db, in, &list)
			ok(t, err)

			equals(t, int64(1), n)
			equals(t, 1, len(list))
			equals(t, "User-2", list[0].UserID)
			equals(t, int64(0), list[0].TopScore)
		})

		t.Run("scan page filtered projection on index", func(t *testing.T) {
			list := []*GameScore{}
			in := dynamo.NewScanInput(tname)
			in.SetIndexName("GameTitleIndex")
			in.SetProjectionExpression("GameTitle, TopScore")
			in.SetLimit(2)
			in.SetMaxPages(1)

			n, err := dynamo.Scan(db, in, &list)

			ok(t, err)
			equals(t, int64(2), n)
			equals(t, 2, len(list))
			equals(t, int64(20), list[0].TopScore)
			equals(t, "", list[0].UserID)
		})

		//@TODO Scan: (total)segment (parralell scan)
		//@TODO All: context based (deadline, cancel)
		//@TODO BatchWriteItem/BatchGetItem: Implement
	})
}
