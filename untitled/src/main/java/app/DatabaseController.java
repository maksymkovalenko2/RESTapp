package app;

import com.mongodb.BasicDBObject;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import org.bson.Document;
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
    private static final String STR = "\n";
    private static final String DATABASE = "database";
    private static final String COLLECTION = "collection";
    public static final String NAME = "name";
    public static final String VALUE = "value";

    //MongoClient mongoClient = new MongoClient("localhost", 27017);k

    ConnectionString connectionString = new ConnectionString("mongodb+srv://admin:admin@mydemocluster.ijz2kk5.mongodb.net/?retryWrites=true&w=majority");
    MongoClientSettings settings = MongoClientSettings.builder().applyConnectionString(connectionString).build();
    MongoClient mongoClient = MongoClients.create(settings);


    @GetMapping
    public String getDBs() {
        return collectString(mongoClient.listDatabaseNames().iterator());
    }

    @GetMapping(value = "/{database}")
    public String getCollections(@PathVariable(DATABASE) String database) {
        return collectString(mongoClient.getDatabase(database).listCollectionNames().iterator());
    }

    @GetMapping(value = "/{database}/{collection}")
    public String getDocuments(@PathVariable(DATABASE) String database,
                               @PathVariable(COLLECTION) String collection) {
        return collectDocuments(mongoClient.getDatabase(database).getCollection(collection).find().iterator());
    }

    @GetMapping(value = "/{database}/{collection}", params = NAME)
    public String getDocumentsByFilter(@PathVariable(DATABASE) String database,
                                       @PathVariable(COLLECTION) String collection,
                                       @PathParam(NAME) String name,
                                       @PathParam(VALUE) String value) {
        return collectDocuments(mongoClient.getDatabase(database).getCollection(collection).find(new BasicDBObject(name, value)).iterator());
    }


    @PutMapping(value = "/{database}/{collection}", consumes = APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.CREATED)
    @ResponseBody
    public String putObjects(@PathVariable(DATABASE) String database,
                             @PathVariable(COLLECTION) String collection,
                             HttpEntity<String> httpEntity) {
        JSONArray array = new JSONArray(Objects.requireNonNull(httpEntity.getBody()));
        List<Document> documents = new ArrayList<>();
        for (Object object : array) {
            documents.add(Document.parse(object.toString()));
        }
        MongoCollection<Document> dbCollection = mongoClient.getDatabase(database).getCollection(collection);
        dbCollection.insertMany(documents);
        return collectDocuments(dbCollection.find().iterator());
    }

    @PostMapping(value = "/{database}/{collection}", consumes = APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.CREATED)
    @ResponseBody
    public String updateObjects(@PathVariable(DATABASE) String database,
                                @PathVariable(COLLECTION) String collection,
                                @RequestParam String filterName,
                                @RequestParam String filterValue,
                                @RequestParam String fieldName,
                                @RequestParam String fieldValue) {
        MongoCollection<Document> dbCollection = mongoClient.getDatabase(database).getCollection(collection);
        dbCollection.updateMany(Filters.eq(filterName, filterValue), Updates.set(fieldName, fieldValue));
        return collectDocuments(dbCollection.find().iterator());
    }


    @DeleteMapping(value = "/{database}/{collection}", consumes = APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.CREATED)
    @ResponseBody
    public String deleteObjects(@PathVariable(DATABASE) String database,
                                @PathVariable(COLLECTION) String collection,
                                @RequestParam String filterName,
                                @RequestParam String filterValue) {
        MongoCollection<Document> dbCollection = mongoClient.getDatabase(database).getCollection(collection);
        dbCollection.deleteMany(Filters.regex(filterName, filterValue));
        return collectDocuments(dbCollection.find().iterator());
    }

    private String collectDocuments(Iterator<Document> iterator) {
        StringBuilder sb = new StringBuilder();
        while (iterator.hasNext()) {
            sb.append(iterator.next().toJson()).append(STR);
        }
        return sb.toString();
    }

    private String collectString(Iterator<String> iterator) {
        StringBuilder sb = new StringBuilder();
        while (iterator.hasNext()) {
            sb.append(iterator.next()).append(STR);
        }
        return sb.toString();
    }

}