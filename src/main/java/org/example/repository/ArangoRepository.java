package org.example.repository;

import com.arangodb.ArangoDB;
import com.arangodb.mapping.ArangoJack;
import org.apache.spark.sql.Dataset;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.sql.Row;

public class ArangoRepository {
    public static void save(Dataset<Row> data, String collectionName) {

        // Database connection properties
        Map<String, String> optionsMap = new HashMap<String, String>() {{
            put("password", "root");
            put("endpoints", "localhost:8529");
            put("table", collectionName);
            put("database", "TestSpark");
            put("confirmTruncate", "true");
        }};

        data.write().format("com.arangodb.spark").mode("overwrite").options(optionsMap).save();
        System.out.println("Saved data to ArangoDB");
    }
}
