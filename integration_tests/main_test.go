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
			put := dynamo.NewPut(tname, score1)
			err := put.Execute(db)
			ok(t, err)
		})

		t.Run("put with conditional exp and custom error", func(t *testing.T) {
			put := dynamo.NewPut(tname, score1)
			put.SetConditionExpression("attribute_not_exists(GameTitle)")
			put.SetConditionError(ErrGameScoreExists)
			err := put.Execute(db)
			equals(t, ErrGameScoreExists, err)
		})

		t.Run("put with a conditional exp and no custom error", func(t *testing.T) {
			put := dynamo.NewPut(tname, score1)
			put.SetConditionExpression("attribute_not_exists(GameTitle)")
			err := put.Execute(db)
			assert(t, strings.Contains(err.Error(), "ConditionalCheckFailedException"), "expected normal conditional failed error, got: %+v", err)
		})
	})

	t.Run("Get", func(t *testing.T) {
		t.Run("get non-existing with no error configured", func(t *testing.T) {
			score2 := &GameScore{}
			get := dynamo.NewGet(tname, GameScorePK{"No Such Game", "User-5"})
			err := get.Execute(db, score2)
			ok(t, err)
			equals(t, "", score2.GameTitle)
		})

		t.Run("get non-existing with an error configured", func(t *testing.T) {
			score3 := &GameScore{}
			get := dynamo.NewGet(tname, GameScorePK{"No Such Game", "User-5"})
			get.SetItemNilError(ErrGameScoreNotExists)
			err := get.Execute(db, score3)
			equals(t, ErrGameScoreNotExists, err)
		})

		t.Run("get an existing gamescore", func(t *testing.T) {
			score4 := &GameScore{}
			get := dynamo.NewGet(tname, pk1)
			get.SetItemNilError(ErrGameScoreNotExists)
			err := get.Execute(db, score4)
			ok(t, err)
			equals(t, score1.GameTitle, score4.GameTitle)
			equals(t, score1.UserID, score4.UserID)
			equals(t, score1.TopScore, score4.TopScore)
		})

		t.Run("get an projected existing gamescore", func(t *testing.T) {
			score5 := &GameScore{}
			get := dynamo.NewGet(tname, pk1)
			get.SetProjectionExpression("GameTitle, UserId")
			get.SetItemNilError(ErrGameScoreNotExists)
			err := get.Execute(db, score5)
			ok(t, err)
			equals(t, score1.GameTitle, score5.GameTitle)
			equals(t, score1.UserID, score5.UserID)
			equals(t, int64(0), score5.TopScore)
		})
	})

	t.Run("Update", func(t *testing.T) {
		t.Run("update non-existing without condition", func(t *testing.T) {
			update := dynamo.NewUpdate(tname, GameScorePK{"No Any Game", "User-5"})
			err := update.Execute(db)
			ok(t, err)
		})

		t.Run("update non-existing with condition and error", func(t *testing.T) {
			update := dynamo.NewUpdate(tname, GameScorePK{"No Such Game", "User-5"})
			update.SetUpdateExpression("SET TopScore = :TopScore")
			update.AddExpressionValue(":TopScore", 120)
			update.SetConditionExpression("attribute_exists(GameTitle)")
			update.SetConditionError(ErrGameScoreNotExists)

			err := update.Execute(db)
			equals(t, ErrGameScoreNotExists, err)
		})

		t.Run("update existing", func(t *testing.T) {
			update := dynamo.NewUpdate(tname, pk1)
			update.SetUpdateExpression("SET TopScore = :TopScore")
			update.AddExpressionValue(":TopScore", 120)
			update.SetConditionExpression("attribute_exists(GameTitle)")
			update.SetConditionError(ErrGameScoreNotExists)

			err := update.Execute(db)
			ok(t, err)

			item := &GameScore{}
			g := dynamo.NewGet(tname, pk1)
			g.SetItemNilError(ErrGameScoreNotExists)
			err = g.Execute(db, item)
			ok(t, err)
			equals(t, int64(120), item.TopScore)
		})
	})

	t.Run("Delete", func(t *testing.T) {
		t.Run("delete non-existing without condition", func(t *testing.T) {
			del := dynamo.NewDelete(tname, GameScorePK{"No Such Game", "User-5"})
			err := del.Execute(db)
			ok(t, err)
		})

		t.Run("delete non-existing with condition and error", func(t *testing.T) {
			del := dynamo.NewDelete(tname, GameScorePK{"No Such Game", "User-5"})
			del.SetConditionExpression("attribute_exists(GameTitle)")
			del.SetConditionError(ErrGameScoreNotExists)
			err := del.Execute(db)
			equals(t, ErrGameScoreNotExists, err)
		})

		t.Run("delete existing with condition and error", func(t *testing.T) {
			del := dynamo.NewDelete(tname, pk1)
			del.SetConditionExpression("attribute_exists(GameTitle)")
			del.SetConditionError(ErrGameScoreNotExists)
			err := del.Execute(db)
			ok(t, err)

			err = dynamo.NewDelete(tname, GameScorePK{"No Any Game", "User-5"}).Execute(db)
			ok(t, err)
		})
	})
}

func TestQueryScan(t *testing.T) {
	sess := newsess(t)
	tname := tablename(t)
	db := dynamodb.New(sess)

	score1 := &GameScore{GameScorePK{"Alien Adventure", "User-1"}, 20}
	ok(t, dynamo.NewPut(tname, score1).Execute(db))
	score2 := &GameScore{GameScorePK{"Alien Adventure", "User-2"}, 75}
	ok(t, dynamo.NewPut(tname, score2).Execute(db))
	score3 := &GameScore{GameScorePK{"Alien Adventure", "User-3"}, 100}
	ok(t, dynamo.NewPut(tname, score3).Execute(db))
	defer func() {
		ok(t, dynamo.NewDelete(tname, score1.GameScorePK).Execute(db))
		ok(t, dynamo.NewDelete(tname, score2.GameScorePK).Execute(db))
		ok(t, dynamo.NewDelete(tname, score3.GameScorePK).Execute(db))
	}()

	t.Run("Query", func(t *testing.T) {
		t.Run("query all partition items in base table", func(t *testing.T) {
			list := []*GameScore{}

			q := dynamo.NewQuery(tname, "GameTitle = :GameTitle")
			q.AddExpressionValue(":GameTitle", "Alien Adventure")

			n, err := q.Execute(db, &list)
			ok(t, err)

			equals(t, int64(3), n)
			equals(t, 3, len(list))
			equals(t, int64(20), list[0].TopScore)
		})

		t.Run("query all projected in base table", func(t *testing.T) {
			list := []*GameScore{}

			q := dynamo.NewQuery(tname, "GameTitle = :GameTitle")
			q.SetProjectionExpression("GameTitle, UserId")
			q.AddExpressionValue(":GameTitle", "Alien Adventure")

			n, err := q.Execute(db, &list)
			ok(t, err)
			equals(t, int64(3), n)
			equals(t, 3, len(list))
			equals(t, int64(0), list[0].TopScore)
		})

		t.Run("query filtered projection in base table", func(t *testing.T) {
			list := []*GameScore{}

			q := dynamo.NewQuery(tname, "GameTitle = :GameTitle")
			q.SetProjectionExpression("GameTitle, UserId")
			q.SetFilterExpression("#ts > :minTopScore")
			q.AddExpressionName("#ts", "TopScore")
			q.AddExpressionValue(":GameTitle", "Alien Adventure")
			q.AddExpressionValue(":minTopScore", 20)

			n, err := q.Execute(db, &list)
			ok(t, err)
			equals(t, int64(2), n)
			equals(t, 2, len(list))
			equals(t, "User-2", list[0].UserID)
			equals(t, int64(0), list[0].TopScore)
		})

		t.Run("query page filtered projection in base table", func(t *testing.T) {
			list := []*GameScore{}

			q := dynamo.NewQuery(tname, "GameTitle = :GameTitle")
			q.SetProjectionExpression("GameTitle, UserId")
			q.SetFilterExpression("#ts > :minTopScore")
			q.AddExpressionName("#ts", "TopScore")
			q.AddExpressionValue(":GameTitle", "Alien Adventure")
			q.AddExpressionValue(":minTopScore", 20)
			q.SetMaxPages(1)
			q.SetLimit(2)

			n, err := q.Execute(db, &list)
			ok(t, err)
			equals(t, int64(1), n)
			equals(t, 1, len(list))
			equals(t, "User-2", list[0].UserID)
			equals(t, int64(0), list[0].TopScore)
		})

		t.Run("query page filtered projection on index", func(t *testing.T) {
			list := []*GameScore{}

			q := dynamo.NewQuery(tname,
				"GameTitle = :GameTitle AND TopScore > :minTopScore")

			q.SetProjectionExpression("GameTitle, TopScore")
			q.SetIndexName("GameTitleIndex")
			q.AddExpressionValue(":GameTitle", "Alien Adventure")
			q.AddExpressionValue(":minTopScore", 20)
			q.SetMaxPages(1)
			q.SetLimit(2)

			n, err := q.Execute(db, &list)
			ok(t, err)
			equals(t, int64(2), n)
			equals(t, 2, len(list))
			equals(t, int64(75), list[0].TopScore)
			equals(t, "", list[0].UserID)
		})

		t.Run("count page filtered on index", func(t *testing.T) {
			q := dynamo.NewQuery(tname,
				"GameTitle = :GameTitle AND TopScore > :minTopScore")

			q.SetIndexName("GameTitleIndex")
			q.AddExpressionValue(":GameTitle", "Alien Adventure")
			q.AddExpressionValue(":minTopScore", 20)
			q.SetMaxPages(1)
			q.SetLimit(2)
			q.SetSelect("COUNT")

			n, err := q.Execute(db, nil)
			ok(t, err)
			equals(t, int64(2), n)
		})
	})

	t.Run("Scan", func(t *testing.T) {
		t.Run("scan all items in base table", func(t *testing.T) {
			list := []*GameScore{}
			in := dynamo.NewScan(tname)

			n, err := in.Execute(db, &list)
			ok(t, err)

			equals(t, int64(3), n)
			equals(t, 3, len(list))
			equals(t, int64(20), list[0].TopScore)
		})

		t.Run("scan all projected in base table", func(t *testing.T) {
			list := []*GameScore{}
			in := dynamo.NewScan(tname)
			in.SetProjectionExpression("GameTitle, UserId")

			n, err := in.Execute(db, &list)
			ok(t, err)

			equals(t, int64(3), n)
			equals(t, 3, len(list))
			equals(t, int64(0), list[0].TopScore)
		})

		t.Run("scan filtered projection in base table", func(t *testing.T) {
			list := []*GameScore{}
			in := dynamo.NewScan(tname)
			in.SetProjectionExpression("GameTitle, UserId")
			in.SetFilterExpression("TopScore > :minTopScore")
			in.AddExpressionValue(":minTopScore", 20)

			n, err := in.Execute(db, &list)
			ok(t, err)
			equals(t, int64(2), n)
			equals(t, 2, len(list))
			equals(t, "User-2", list[0].UserID)
			equals(t, int64(0), list[0].TopScore)
		})

		t.Run("scan page filtered projection in base table", func(t *testing.T) {
			list := []*GameScore{}
			in := dynamo.NewScan(tname)
			in.SetProjectionExpression("GameTitle, UserId")
			in.SetFilterExpression("TopScore > :minTopScore")
			in.AddExpressionValue(":minTopScore", 20)
			in.SetLimit(2)
			in.SetMaxPages(1)

			n, err := in.Execute(db, &list)
			ok(t, err)

			equals(t, int64(1), n)
			equals(t, 1, len(list))
			equals(t, "User-2", list[0].UserID)
			equals(t, int64(0), list[0].TopScore)
		})

		t.Run("scan page filtered projection on index", func(t *testing.T) {
			list := []*GameScore{}
			in := dynamo.NewScan(tname)
			in.SetIndexName("GameTitleIndex")
			in.SetProjectionExpression("GameTitle, TopScore")
			in.SetLimit(2)
			in.SetMaxPages(1)

			n, err := in.Execute(db, &list)

			ok(t, err)
			equals(t, int64(2), n)
			equals(t, 2, len(list))
			equals(t, int64(20), list[0].TopScore)
			equals(t, "", list[0].UserID)
		})

		//@TODO BatchWriteItem/BatchGetItem: Implement
	})
}
