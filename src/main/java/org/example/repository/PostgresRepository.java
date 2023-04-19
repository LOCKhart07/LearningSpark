package org.example.repository;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Properties;

public class PostgresRepository {
    public static void save(Dataset<Row> data, String tableName) {
        // Database connection properties
        Properties properties = new Properties();
        properties.setProperty("user", "org.postgresql.Driver");
        properties.setProperty("url", "jdbc:postgresql://localhost:5432/spark_test");
        properties.setProperty("user", "dbadmin");
        properties.setProperty("password", "LOCKhart@0701");

        data.write().mode("overwrite").jdbc(properties.getProperty("url"), tableName, properties);
        System.out.println("Wrote data to Database");
    }
}
