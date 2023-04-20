package org.example.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;


import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import java.util.*;
import java.util.concurrent.TimeoutException;

public class KafkaService {
    public static void startKafkaStreaming(SparkSession spark) {
//        SparkConf sparkConf = new SparkConf().setAppName("TestSparkStreaming").setMaster("local");

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "test-group");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

        Collection<String> topics = Arrays.asList("test");


        JavaStreamingContext javaStreamingContext = new JavaStreamingContext(new JavaSparkContext(spark.sparkContext()), Durations.seconds(1));
        javaStreamingContext.sparkContext().setLogLevel("ERROR");


        JavaInputDStream<ConsumerRecord<String, String>> kafkaStream = KafkaUtils.createDirectStream(javaStreamingContext, LocationStrategies.PreferConsistent(), ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));


        kafkaStream.foreachRDD(rdd -> {
            if (!rdd.isEmpty()) {
                try {
                    JavaRDD<Row> rowJavaRDD = rdd.map(record -> {
                        String value = record.value();
                        double convertedValue = Double.parseDouble(value);
                        return RowFactory.create(convertedValue);
                    });

                    StructType schema = new StructType(new StructField[]{
                            new StructField("Length of Membership", DataTypes.DoubleType, true, Metadata.empty())});
                    Dataset<Row> df = spark.createDataFrame(rowJavaRDD, schema);

                    Dataset<Row> hello = SpendingService.getYearlySpendingBySubscriptionLength(df);
                    hello.show();
                }
                catch (Exception exception){
                    System.out.println("Please Enter the number of years as a number");
                }

            }
        });


        try {
            javaStreamingContext.start();
            javaStreamingContext.awaitTermination();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

    }
}
