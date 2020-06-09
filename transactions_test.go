package transactions

import (
	"log"
	"testing"

	"github.com/couchbase/gocb/v2"
)

func TestSomething(t *testing.T) {
	cluster, err := gocb.Connect("couchbase://172.23.111.132", gocb.ClusterOptions{
		Username: "Administrator",
		Password: "password",
	})
	if err != nil {
		log.Printf("Connect Error: %+v", err)
		panic(err)
	}

	bucket := cluster.Bucket("travel-sample")
	collection := bucket.DefaultCollection()

	testDummy := map[string]string{"name": "frank"}
	_, err = collection.Upsert("anotherDoc", testDummy, nil)
	if err == nil {
		_, err = collection.Upsert("yetAnotherDoc", testDummy, nil)
	}
	if err != nil {
		log.Printf("Insert Test Error: %+v", err)
		panic(err)
	}

	transactions, err := Init(cluster, &Config{
		DurabilityLevel: DurabilityLevelMajority,
	})
	if err != nil {
		log.Printf("Init Error: %+v", err)
		panic(err)
	}

	err = transactions.Run(func(ctx *AttemptContext) error {
		// Inserting a doc:
		docID := "test-id"
		testData := map[string]string{"name": "mike"}
		_, err := ctx.Insert(collection, docID, testData)
		if err != nil {
			return err
		}

		// Getting documents:
		docOpt, err := ctx.GetOptional(collection, docID)
		if err != nil {
			return err
		}
		log.Printf("GetOptional Result: %+v", docOpt)

		doc, err := ctx.Get(collection, docID)
		if err != nil {
			return err
		}
		log.Printf("Get Result: %+v", doc)

		// Replacing a doc:
		anotherDoc, err := ctx.Get(collection, "anotherDoc")
		var testReplace map[string]string
		err = anotherDoc.Content(&testReplace)
		if err != nil {
			log.Printf("ContentAs Error: %+v", err)
			panic(err) // hehe, this will probably suck
		}
		testReplace["transactions"] = "are awesome"
		replaceDoc, err := ctx.Replace(anotherDoc, testReplace)
		if err != nil {
			return err
		}
		log.Printf("Replace Result: %+v", replaceDoc)

		// Removing a doc:
		yetAnotherDoc, err := ctx.Get(collection, "yetAnotherDoc")
		if err != nil {
			return err
		}

		err = ctx.Remove(yetAnotherDoc)
		if err != nil {
			return err
		}

		err = ctx.Commit()
		if err != nil {
			return err
		}

		return nil
	}, nil)
	if err != nil {
		log.Printf("Run Error: %+v", err)
	}

}
