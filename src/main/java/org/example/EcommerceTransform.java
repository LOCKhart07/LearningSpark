package org.example;


import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.example.repository.ArangoRepository;
import org.example.service.SpendingService;


import static org.apache.spark.sql.functions.*;


public class EcommerceTransform {

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder().master("local").appName("EmailFilter").getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");

        String filepath = "D:\\temp\\ecommerce_dataset.csv";
        Dataset<Row> rawData = spark.read().option("header", true).option("inferSchema", true).option("multiline", true).format("csv").load(filepath);

        // Remove unnecessary Columns
        // Remove newline characters from Address fields
        Dataset<Row> formattedData = rawData.drop("Avg. Session Length").withColumn("Address", regexp_replace(col("Address"), "\\\n", " "));


        // Save to Databases
//        PostgresRepository.save(formattedData, "users");
        ArangoRepository.save(formattedData, "users");


        SpendingService.getAverageYearlySpendingPerDomain(formattedData).show();


        // Linear Regression
        LinearRegressionModel linearRegressionModel = SpendingService.trainLinearRegressionModel(formattedData);

        spark.stop();
    }
}