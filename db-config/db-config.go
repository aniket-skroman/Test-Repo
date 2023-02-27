package dbconfig

import (
	"context"
	"log"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func EnvMongoURI() string {
	var dbURL = "mongodb+srv://aniket_mauto:9TxEsbh9vZVzIz68@shared-cluster.tb9e4zs.mongodb.net/test?maxPoolSize=200&w=majority"

	return dbURL
}

var client *mongo.Client

func ResolveClientDB() *mongo.Client {
	if client != nil {
		return client
	}

	var err error
	//TODO add to your .env.yml or .config.yml MONGODB_URI: mongodb://localhost:27017
	clientOptions := options.Client().ApplyURI(EnvMongoURI())
	client, err = mongo.Connect(context.Background(), clientOptions)
	if err != nil {
		log.Fatal(err)
	}

	// check the connection
	err = client.Ping(context.Background(), nil)
	if err != nil {
		log.Fatal(err)
	}

	// TODO optional you can log your connected MongoDB client
	// fmt.Println("Connection established...")
	return client
}

func CloseClientDB() {
	if client == nil {
		return
	}

	err := client.Disconnect(context.TODO())
	if err != nil {
		log.Fatal(err)
	}

	// TODO optional you can log your closed MongoDB client
	// fmt.Println("Connection to MongoDB closed.")
}

func GetCollection(client *mongo.Client, collectionName string) *mongo.Collection {
	// var local = "golangAPI"
	var remote = "mautodb"
	collection := client.Database(remote).Collection(collectionName)
	return collection
}
