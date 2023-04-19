package org.example;


import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.example.repository.ArangoRepository;
import org.example.service.SpendingService;


import java.util.Arrays;
import java.util.List;

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


        List<Row> rowList = Arrays.asList(RowFactory.create(4.5), RowFactory.create(6.2));

        StructType schema = new StructType(new StructField[]{new StructField("Length of Membership", DataTypes.DoubleType, false, Metadata.empty())});

        Dataset<Row> newData = spark.createDataFrame(rowList, schema);


        // Linear Regression
        LinearRegressionModel linearRegressionModel = SpendingService.trainLinearRegressionModel(formattedData, newData);

        VectorAssembler vectorAssembler = new VectorAssembler().setInputCols(new String[]{"Length of Membership"}).setOutputCol("features");

        Dataset<Row> newDataWithFeatures = vectorAssembler.transform(newData).select("features");

        // Make predictions
        Dataset<Row> predictions = linearRegressionModel.transform(newDataWithFeatures);
        predictions.withColumnRenamed("features","Length of Membership").withColumnRenamed("prediction","Predicted Average Yearly Spending").show();

        spark.stop();
    }
}