package org.example.service;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.concurrent.TimeoutException;

public class KafkaService {
    public static void startKafkaStreaming() throws TimeoutException, StreamingQueryException {
        SparkSession spark = SparkSession.builder().master("local").appName("Kafka Streaming").getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");

        Dataset<Row> df = spark.readStream().format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "test").option("startingOffsets", "latest").load();

        df.writeStream().outputMode("append").format("console").start().awaitTermination();

//        SparkConf sparkConf = new SparkConf();
//        sparkConf.setAppName("TestSparkStreaming");
//        sparkConf.setMaster("local");
////        sparkConf.set("spark.cassandra.connection.host", "127.0.0.1");
//
//        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(1));
//
//        Map<String, Object> kafkaParams = new HashMap<>();
//        kafkaParams.put("bootstrap.servers", "localhost:9092");
//        kafkaParams.put("key.deserializer", StringDeserializer.class);
//        kafkaParams.put("value.deserializer", StringDeserializer.class);
//        kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream");
//        kafkaParams.put("auto.offset.reset", "latest");
//        kafkaParams.put("enable.auto.commit", false);
//        Collection<String> topics = Arrays.asList("test");
//
//        JavaInputDStream<ConsumerRecord<String, String>> messages = KafkaUtils.createDirectStream(streamingContext, LocationStrategies.PreferConsistent(), ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));
//
//
//        streamingContext.start();
//
//        try {
//            streamingContext.awaitTermination();
//        } catch (InterruptedException e) {
//            throw new RuntimeException(e);
//        }
    }
}
