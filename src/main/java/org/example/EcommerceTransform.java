package org.example;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.example.repository.ArangoRepository;
import org.example.service.KafkaService;
import org.example.service.SpendingService;

import static org.apache.spark.sql.functions.*;


public class EcommerceTransform {

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder().master("local").appName("Global Spark Context").getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");

        String filepath = "D:\\temp\\ecommerce_dataset.csv";
        Dataset<Row> rawData = spark.read().option("header", true).option("inferSchema", true).option("multiline", true).format("csv").load(filepath);

        String newFilepath = "D:\\temp\\newData.xlsx";
        Dataset<Row> newData = spark.read().option("header", true).option("inferSchema", true).option("multiline", true).format("com.crealytics.spark.excel").load(newFilepath);

        // Remove unnecessary Columns
        // Remove newline characters from Address fields
        Dataset<Row> formattedData = rawData.drop("Avg. Session Length").withColumn("Address", regexp_replace(col("Address"), "\\\n", " "));

        Dataset<Row> finalData = formattedData.as("formattedData").join(newData.as("newData"), rawData.col("Email").equalTo(newData.col("Email")), "leftouter").selectExpr("formattedData.Email", "formattedData.Address", "newData.Prime");

        finalData.show();

        // Save to Database
//        PostgresRepository.save(formattedData, "users");
        ArangoRepository.save(formattedData, "users");


        SpendingService.getAverageYearlySpendingPerDomain(formattedData).show();

        SpendingService.trainLinearRegressionModel(formattedData);

        //spark.stop();
        KafkaService.startKafkaStreaming(spark);

        spark.stop();


//        SpendingService.getYearlySpendingBySubscriptionLength(4.5).show();
    }
}