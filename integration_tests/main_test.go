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
			err := dynamo.Put(db, tname, score1, nil, nil)
			ok(t, err)
		})

		t.Run("put with conditional exp and custom error", func(t *testing.T) {
			err := dynamo.Put(
				db,
				tname,
				score1,
				dynamo.NewExp("attribute_not_exists(GameTitle)"), ErrGameScoreExists)
			equals(t, ErrGameScoreExists, err)
		})

		t.Run("put with a conditional exp and no custom error", func(t *testing.T) {
			err := dynamo.Put(
				db,
				tname,
				score1,
				dynamo.NewExp("attribute_not_exists(GameTitle)"), nil)
			assert(t, strings.Contains(err.Error(), "ConditionalCheckFailedException"), "expected normal conditional failed error, got: %+v", err)
		})
	})

	t.Run("Get", func(t *testing.T) {
		t.Run("get non-existing with no error configured", func(t *testing.T) {
			score2 := &GameScore{}
			err := dynamo.Get(db, tname, GameScorePK{"No Such Game", "User-5"}, score2, nil, nil)
			ok(t, err)
			equals(t, "", score2.GameTitle)
		})

		t.Run("get non-existing with an error configured", func(t *testing.T) {
			score3 := &GameScore{}
			err := dynamo.Get(db, tname, GameScorePK{"No Such Game", "User-5"}, score3, nil, ErrGameScoreNotExists)
			equals(t, ErrGameScoreNotExists, err)
		})

		t.Run("get an existing gamescore", func(t *testing.T) {
			score4 := &GameScore{}
			err := dynamo.Get(db, tname, pk1, score4, nil, ErrGameScoreNotExists)
			ok(t, err)
			equals(t, score1.GameTitle, score4.GameTitle)
			equals(t, score1.UserID, score4.UserID)
			equals(t, score1.TopScore, score4.TopScore)
		})

		t.Run("get an projected existing gamescore", func(t *testing.T) {
			score5 := &GameScore{}
			err := dynamo.Get(db, tname, pk1, score5, dynamo.NewExp("GameTitle, UserId"), ErrGameScoreNotExists)
			ok(t, err)
			equals(t, score1.GameTitle, score5.GameTitle)
			equals(t, score1.UserID, score5.UserID)
			equals(t, int64(0), score5.TopScore)
		})
	})

	t.Run("Update", func(t *testing.T) {
		t.Run("update non-existing without condition", func(t *testing.T) {
			err := dynamo.Update(db, tname, GameScorePK{"No Such Game 2", "User-5"}, nil, nil, nil)
			ok(t, err)
		})

		t.Run("update non-existing with condition and error", func(t *testing.T) {
			err := dynamo.Update(db, tname, GameScorePK{"No Such Game", "User-5"}, dynamo.NewExp("SET TopScore = :TopScore").Value(":TopScore", 120), dynamo.NewExp("attribute_exists(GameTitle)"), ErrGameScoreNotExists)
			equals(t, ErrGameScoreNotExists, err)
		})

		t.Run("update existing", func(t *testing.T) {
			err := dynamo.Update(db, tname, pk1, dynamo.NewExp("SET TopScore = :TopScore").Value(":TopScore", 120), dynamo.NewExp("attribute_exists(GameTitle)"), ErrGameScoreNotExists)
			ok(t, err)

			item := &GameScore{}
			err = dynamo.Get(db, tname, pk1, item, nil, ErrGameScoreNotExists)
			ok(t, err)
			equals(t, int64(120), item.TopScore)
		})
	})

	t.Run("Delete", func(t *testing.T) {
		t.Run("delete non-existing without condition", func(t *testing.T) {
			err := dynamo.Delete(db, tname, GameScorePK{"No Such Game", "User-5"}, nil, nil)
			ok(t, err)
		})

		t.Run("delete non-existing with condition and error", func(t *testing.T) {
			err := dynamo.Delete(db, tname, GameScorePK{"No Such Game", "User-5"}, dynamo.NewExp("attribute_exists(GameTitle)"), ErrGameScoreNotExists)
			equals(t, ErrGameScoreNotExists, err)
		})

		t.Run("delete existing with condition and error", func(t *testing.T) {
			err := dynamo.Delete(db, tname, pk1, dynamo.NewExp("attribute_exists(GameTitle)"), ErrGameScoreNotExists)
			ok(t, err)

			//clean up the side effect or our unconditionaly update
			err = dynamo.Delete(db, tname, GameScorePK{"No Such Game 2", "User-5"}, dynamo.NewExp("attribute_exists(GameTitle)"), ErrGameScoreNotExists)
			ok(t, err)
		})
	})
}

func TestQueryScan(t *testing.T) {
	sess := newsess(t)
	tname := tablename(t)
	db := dynamodb.New(sess)

	score1 := &GameScore{GameScorePK{"Alien Adventure", "User-1"}, 20}
	ok(t, dynamo.Put(db, tname, score1, nil, nil))
	score2 := &GameScore{GameScorePK{"Alien Adventure", "User-2"}, 75}
	ok(t, dynamo.Put(db, tname, score2, nil, nil))
	score3 := &GameScore{GameScorePK{"Alien Adventure", "User-3"}, 100}
	ok(t, dynamo.Put(db, tname, score3, nil, nil))
	defer func() {
		ok(t, dynamo.Delete(db, tname, score1.GameScorePK, nil, nil))
		ok(t, dynamo.Delete(db, tname, score2.GameScorePK, nil, nil))
		ok(t, dynamo.Delete(db, tname, score3.GameScorePK, nil, nil))
	}()

	t.Run("Query", func(t *testing.T) {
		t.Run("query all partition items in base table", func(t *testing.T) {
			list := []*GameScore{}
			err := dynamo.Query(db, tname, "", dynamo.NewExp("GameTitle = :GameTitle").Value(":GameTitle", "Alien Adventure"), func() interface{} {
				item := &GameScore{}
				list = append(list, item)
				return item
			}, nil, nil, 0, 0)
			ok(t, err)
			equals(t, 3, len(list))
			equals(t, int64(20), list[0].TopScore)
		})

		t.Run("query all projected in base table", func(t *testing.T) {
			list := []*GameScore{}
			err := dynamo.Query(db, tname, "", dynamo.NewExp("GameTitle = :GameTitle").Value(":GameTitle", "Alien Adventure"), func() interface{} {
				item := &GameScore{}
				list = append(list, item)
				return item
			}, dynamo.NewExp("GameTitle, UserId"), nil, 0, 0)
			ok(t, err)
			equals(t, 3, len(list))
			equals(t, int64(0), list[0].TopScore)
		})

		t.Run("query filtered projection in base table", func(t *testing.T) {
			list := []*GameScore{}
			err := dynamo.Query(db, tname, "", dynamo.NewExp("GameTitle = :GameTitle").Value(":GameTitle", "Alien Adventure"), func() interface{} {
				item := &GameScore{}
				list = append(list, item)
				return item
			}, dynamo.NewExp("GameTitle, UserId"), dynamo.NewExp("TopScore > :minTopScore").Value(":minTopScore", 20), 0, 0)
			ok(t, err)
			equals(t, 2, len(list))
			equals(t, "User-2", list[0].UserID)
			equals(t, int64(0), list[0].TopScore)
		})

		t.Run("query page filtered projection in base table", func(t *testing.T) {
			list := []*GameScore{}
			err := dynamo.Query(db, tname, "", dynamo.NewExp("GameTitle = :GameTitle").Value(":GameTitle", "Alien Adventure"), func() interface{} {
				item := &GameScore{}
				list = append(list, item)
				return item
			}, dynamo.NewExp("GameTitle, UserId"), dynamo.NewExp("TopScore > :minTopScore").Value(":minTopScore", 20), 2, 1)
			ok(t, err)
			equals(t, 1, len(list))
			equals(t, "User-2", list[0].UserID)
			equals(t, int64(0), list[0].TopScore)
		})

		t.Run("query page filtered projection on index", func(t *testing.T) {
			list := []*GameScore{}
			err := dynamo.Query(db, tname, "GameTitleIndex", dynamo.NewExp("GameTitle = :GameTitle AND TopScore > :minTopScore").Value(":GameTitle", "Alien Adventure").Value(":minTopScore", 20), func() interface{} {
				item := &GameScore{}
				list = append(list, item)
				return item
			}, dynamo.NewExp("GameTitle, TopScore"), nil, 2, 1)
			ok(t, err)
			equals(t, 2, len(list))
			equals(t, int64(75), list[0].TopScore)
			equals(t, "", list[0].UserID)
		})
	})

	t.Run("Scan", func(t *testing.T) {
		t.Run("scan all items in base table", func(t *testing.T) {
			list := []*GameScore{}
			err := dynamo.Scan(db, tname, "", func() interface{} {
				item := &GameScore{}
				list = append(list, item)
				return item
			}, nil, nil, 0, 0)
			ok(t, err)
			equals(t, 3, len(list))
			equals(t, int64(20), list[0].TopScore)
		})

		t.Run("scan all projected in base table", func(t *testing.T) {
			list := []*GameScore{}
			err := dynamo.Scan(db, tname, "", func() interface{} {
				item := &GameScore{}
				list = append(list, item)
				return item
			}, dynamo.NewExp("GameTitle, UserId"), nil, 0, 0)
			ok(t, err)
			equals(t, 3, len(list))
			equals(t, int64(0), list[0].TopScore)
		})

		t.Run("scan filtered projection in base table", func(t *testing.T) {
			list := []*GameScore{}
			err := dynamo.Scan(db, tname, "", func() interface{} {
				item := &GameScore{}
				list = append(list, item)
				return item
			}, dynamo.NewExp("GameTitle, UserId"), dynamo.NewExp("TopScore > :minTopScore").Value(":minTopScore", 20), 0, 0)
			ok(t, err)
			equals(t, 2, len(list))
			equals(t, "User-2", list[0].UserID)
			equals(t, int64(0), list[0].TopScore)
		})

		t.Run("scan page filtered projection in base table", func(t *testing.T) {
			list := []*GameScore{}
			err := dynamo.Scan(db, tname, "", func() interface{} {
				item := &GameScore{}
				list = append(list, item)
				return item
			}, dynamo.NewExp("GameTitle, UserId"), dynamo.NewExp("TopScore > :minTopScore").Value(":minTopScore", 20), 2, 1)
			ok(t, err)
			equals(t, 1, len(list))
			equals(t, "User-2", list[0].UserID)
			equals(t, int64(0), list[0].TopScore)
		})

		t.Run("scan page filtered projection on index", func(t *testing.T) {
			list := []*GameScore{}
			err := dynamo.Scan(db, tname, "GameTitleIndex", func() interface{} {
				item := &GameScore{}
				list = append(list, item)
				return item
			}, dynamo.NewExp("GameTitle, TopScore"), nil, 2, 1)
			ok(t, err)
			equals(t, 2, len(list))
			equals(t, int64(20), list[0].TopScore)
			equals(t, "", list[0].UserID)
		})

		//@TODO Scan: (total)segment (parralell scan)
		//@TODO Query/Scan/GetItem: consistent reads
		//@TODO All: context based (deadline, cancel)
		//@TODO Query/Scan: "select" attributes: ALL_ATTRIBUTES | ALL_PROJECTED_ATTRIBUTES | COUNT | SPECIFIC_ATTRIBUTES
		//@TODO Query: scan direction: forward, backward
	})
}
