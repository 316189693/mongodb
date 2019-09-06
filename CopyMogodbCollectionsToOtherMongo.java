package core.util;

import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.*;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CopyMogodbCollectionsToOtherMongo {
    private static String from_mongodb_host = "192..199.1";
    private static int from_mongodb_port = 37017;
    private static String from_mongodb_username = "wimongo";
    private static String from_mongodb_password = "u2Cg4o";
    private static String from_mongodb_database = "attendan_app";


    private static String target_mongodb_host = "localhost";
    private static int target_mongodb_port = 27017;
    private static String target_mongodb_database = "attendance_app";


    public static void main(String[] args) {

        MongoCredential credential = MongoCredential.createCredential(from_mongodb_username, from_mongodb_database, from_mongodb_password.toCharArray());
        MongoClient remoteClient = new MongoClient(new ServerAddress(from_mongodb_host, from_mongodb_port), Arrays.asList(credential));
        MongoDatabase remotwDatabase = remoteClient.getDatabase(from_mongodb_database);
        MongoClient local = new MongoClient(new ServerAddress(target_mongodb_host, target_mongodb_port));
        MongoDatabase localDatabase = local.getDatabase(target_mongodb_database);
        MongoIterable<String> remoteCollections = remotwDatabase.listCollectionNames();
        MongoCursor<String> iterator = remoteCollections.iterator();
        while (iterator.hasNext()) {

            String collectionName = iterator.next();
            System.out.println(collectionName+" start ....");
            localDatabase.createCollection(collectionName);
            MongoCollection<Document> localDbCollection = localDatabase.getCollection(collectionName);
            MongoCollection<Document> remoteDbCollection = remotwDatabase.getCollection(collectionName);
            List<Document> insertDocuments = new ArrayList<>();
            FindIterable<Document> findDocuments = remoteDbCollection.find();
            MongoCursor<Document> iter = findDocuments.iterator();
            while (iter.hasNext()) {
                insertDocuments.add(iter.next());
            }
            if (insertDocuments.size() > 0) {
                localDbCollection.insertMany(insertDocuments);
            }
           
            System.out.println(collectionName+" end ....");
        }


    }


}
