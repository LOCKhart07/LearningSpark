package org.example;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.internal.config.R;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.example.repository.ArangoRepository;
import org.example.service.KafkaService;
import org.example.service.SpendingService;


import java.util.*;
import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.*;


public class EcommerceTransform {

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder().master("local").appName("Global Spark Context").getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");

        String filepath = "D:\\temp\\ecommerce_dataset.csv";
        Dataset<Row> rawData = spark.read().option("header", true).option("inferSchema", true).option("multiline", true).format("csv").load(filepath);

        // Remove unnecessary Columns
        // Remove newline characters from Address fields
        Dataset<Row> formattedData = rawData.drop("Avg. Session Length").withColumn("Address", regexp_replace(col("Address"), "\\\n", " "));


        // Save to Database
//        PostgresRepository.save(formattedData, "users");
        ArangoRepository.save(formattedData, "users");


        SpendingService.getAverageYearlySpendingPerDomain(formattedData).show();

        SpendingService.trainLinearRegressionModel(formattedData);

        //spark.stop();
        KafkaService.startKafkaStreaming(spark);

//        spark.stop();


//        SpendingService.getYearlySpendingBySubscriptionLength(4.5).show();
    }
}