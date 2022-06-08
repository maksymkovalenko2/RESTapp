package app;

import com.mongodb.BasicDBObject;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.json.JSONArray;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

import javax.websocket.server.PathParam;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import static org.springframework.util.MimeTypeUtils.APPLICATION_JSON_VALUE;

@RestController
@RequestMapping("/database")
public class DatabaseController {

    //MongoClient mongoClient = new MongoClient("localhost", 27017);

    ConnectionString connectionString = new ConnectionString("mongodb+srv://admin:admin@mydemocluster.ijz2kk5.mongodb.net/?retryWrites=true&w=majority");
    MongoClientSettings settings = MongoClientSettings.builder().applyConnectionString(connectionString).build();
    MongoClient mongoClient = MongoClients.create(settings);


    @GetMapping
    public String getDBs() {
        Iterator<String> iterator =  mongoClient.listDatabaseNames().iterator();
        StringBuilder sb = new StringBuilder();
        while (iterator.hasNext()) {
            sb.append(iterator.next()).append("\n");
        }
        return sb.toString();
    }

    @GetMapping(value = "/{database}")
    public String getCollections(@PathVariable("database") String database) {
        Iterator<String> iterator = mongoClient.getDatabase(database).listCollectionNames().iterator();
        StringBuilder sb = new StringBuilder();
        while (iterator.hasNext()) {
            sb.append(iterator.next()).append("\n");
        }
        return sb.toString();
    }

    @GetMapping(value = "/{database}/{collection}")
    public String getDocuments(@PathVariable("database") String database, @PathVariable("collection") String collection) {
        MongoCursor<Document> iterator = mongoClient.getDatabase(database).getCollection(collection).find().iterator();
        StringBuilder sb = new StringBuilder();
        while (iterator.hasNext()) {
            sb.append(iterator.next().toJson()).append("\n");
        }
        return sb.toString();
    }

    @GetMapping(value = "/{database}/{collection}", params = "name")
    public String getDocumentsByFilter(@PathVariable("database") String database, @PathVariable("collection") String collection, @PathParam("name") String name, @PathParam("value") String value) {
        Bson bson = new BasicDBObject(name, value);
        MongoCursor<Document> iterator = mongoClient.getDatabase(database).getCollection(collection).find(bson).iterator();
        StringBuilder sb = new StringBuilder();
        while (iterator.hasNext()) {
            sb.append(iterator.next().toJson()).append("\n");
        }
        return sb.toString();
    }


    @PutMapping(value = "/{database}/{collection}", consumes = APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.CREATED)
    @ResponseBody
    public String putObjects(@PathVariable("database") String database, @PathVariable("collection") String collection, HttpEntity<String> httpEntity) {
        JSONArray array = new JSONArray(Objects.requireNonNull(httpEntity.getBody()));
        List<Document> documents = new ArrayList<>();
        for (Object object : array) {
            documents.add(Document.parse(object.toString()));
        }
        MongoCollection<Document> dbCollection = mongoClient.getDatabase(database).getCollection(collection);
        dbCollection.insertMany(documents);
        MongoCursor<Document> iterator = dbCollection.find().iterator();
        StringBuilder sb = new StringBuilder();
        while (iterator.hasNext()) {
            sb.append(iterator.next().toJson()).append("\n");
        }
        return sb.toString();
    }

    @PostMapping(value = "/{database}/{collection}", consumes = APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.CREATED)
    @ResponseBody
    public String updateObjects(@PathVariable("database") String database,
                                @PathVariable("collection") String collection,
                                @RequestParam String filterName,
                                @RequestParam String filterValue,
                                @RequestParam String fieldName,
                                @RequestParam String fieldValue) {
        MongoCollection<Document> dbCollection = mongoClient.getDatabase(database).getCollection(collection);
        dbCollection.updateMany(Filters.eq(filterName, filterValue), Updates.set(fieldName, fieldValue));
        MongoCursor<Document> iterator = dbCollection.find().iterator();
        StringBuilder sb = new StringBuilder();
        while (iterator.hasNext()) {
            sb.append(iterator.next().toJson()).append("\n");
        }
        return sb.toString();
    }


    @DeleteMapping(value = "/{database}/{collection}", consumes = APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.CREATED)
    @ResponseBody
    public String deleteObjects(@PathVariable("database") String database,
                                @PathVariable("collection") String collection,
                                @RequestParam String filterName,
                                @RequestParam String filterValue) {
        MongoCollection<Document> dbCollection = mongoClient.getDatabase(database).getCollection(collection);
        dbCollection.deleteMany(Filters.regex(filterName, filterValue));
        MongoCursor<Document> iterator = dbCollection.find().iterator();
        StringBuilder sb = new StringBuilder();
        while (iterator.hasNext()) {
            sb.append(iterator.next().toJson()).append("\n");
        }
        return sb.toString();
    }

}