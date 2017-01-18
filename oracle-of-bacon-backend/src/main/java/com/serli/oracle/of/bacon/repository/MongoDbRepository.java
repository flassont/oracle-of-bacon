package com.serli.oracle.of.bacon.repository;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.Document;

import java.util.Optional;

public class MongoDbRepository {

    private final MongoClient mongoClient;

    public MongoDbRepository() {
        mongoClient = new MongoClient("localhost", 27017);
    }

    public Optional<Document> getActorByName(String name) {
        Document maybeActor = mongoClient.getDatabase("bacon")
                .getCollection("actors")
                .find(new BsonDocument("name", new BsonString(name)))
                .first();
        return Optional.ofNullable(maybeActor);
    }
}
