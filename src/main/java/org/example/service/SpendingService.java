package org.example.service;

import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import javax.xml.crypto.Data;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.col;

public class SpendingService {
    public static Dataset<Row> getAverageYearlySpendingPerDomain(Dataset<Row> formattedData) {

        // Filter out only the rows with @gmail.com as email
        Dataset<Row> gmailUsers = formattedData.filter(col("Email").contains("@gmail.com"));

        // Filter out only the rows with @hotmail.com as email
        Dataset<Row> hotmailUsers = formattedData.filter(col("Email").contains("@hotmail.com"));

        // Filter out only the rows with @yahoo.com as email
        Dataset<Row> yahooUsers = formattedData.filter(col("Email").contains("@yahoo.com"));

        // Filter out only the rows with other emails
        Dataset<Row> otherUsers = formattedData.filter(not(col("Email").contains("@gmail.com")).and(not(col("Email").contains("@hotmail.com"))).and(not(col("Email").contains("@yahoo.com"))));

        // Get the average yearly spending for each domain
        Dataset<Row> averageYearlyAmountGmail = gmailUsers.agg(avg(col("Yearly Amount Spent")).alias("Average Yearly Spending")).withColumn("Domain", lit("Gmail.com"));

        Dataset<Row> averageYearlyAmountYahoo = yahooUsers.agg(avg(col("Yearly Amount Spent")).alias("Average Yearly Spending")).withColumn("Domain", lit("Yahoo.com"));

        Dataset<Row> averageYearlyAmountHotmail = hotmailUsers.agg(avg(col("Yearly Amount Spent")).alias("Average Yearly Spending")).withColumn("Domain", lit("Hotmail.com"));

        Dataset<Row> averageYearlyAmountOther = otherUsers.agg(avg(col("Yearly Amount Spent")).alias("Average Yearly Spending")).withColumn("Domain", lit("All others domains"));


        // Combine all the average datasets into one
        Dataset<Row> averageYearlyAmountAll = averageYearlyAmountGmail.union(averageYearlyAmountHotmail).union(averageYearlyAmountYahoo).union(averageYearlyAmountOther);

        // Reorder the columns
        averageYearlyAmountAll = averageYearlyAmountAll.select(col("Domain"), col("Average Yearly Spending"));

        return averageYearlyAmountAll;
    }

    public static void trainLinearRegressionModel(Dataset<Row> formattedData) {
        VectorAssembler assembler = new VectorAssembler().setInputCols(new String[]{"Length of Membership"}).setOutputCol("features");


        Dataset<Row> data = formattedData.select("Yearly Amount Spent", "Length of Membership");
        Dataset<Row> trainingData = assembler.transform(data).select(col("features"), col("Yearly Amount Spent"));

        LinearRegression linearRegression = new LinearRegression().setLabelCol("Yearly Amount Spent").setFeaturesCol("features");

        LinearRegressionModel linearRegressionModel = linearRegression.fit(trainingData);

        String modelFilePath = "D:\\Training\\Spark\\LearningSpark\\ml_model\\linear_regression_model";
        try {
            linearRegressionModel.write().overwrite().save(modelFilePath);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
//        Dataset<Row> predictions = model.transform(assembler.transform(data)).select(col("Yearly Amount Spent"), col("prediction"));
//
//        RegressionEvaluator evaluator = new RegressionEvaluator().setLabelCol("Yearly Amount Spent").setPredictionCol("prediction").setMetricName("r2");
//
//        double rSquared = evaluator.evaluate(predictions);
//
//        System.out.println("R-squared: " + rSquared);
    }

    public static Dataset<Row> getYearlySpendingBySubscriptionLength(Dataset<Row> newData) {

        LinearRegressionModel linearRegressionModel = LinearRegressionModel.load("D:\\Training\\Spark\\LearningSpark\\ml_model\\linear_regression_model");

        VectorAssembler vectorAssembler = new VectorAssembler().setInputCols(new String[]{"Length of Membership"}).setOutputCol("features");

        Dataset<Row> newDataWithFeatures = vectorAssembler.transform(newData).select("features");

        // Make predictions
        Dataset<Row> predictions = linearRegressionModel.transform(newDataWithFeatures);
        predictions = predictions.withColumnRenamed("features", "Length of Membership").withColumnRenamed("prediction", "Predicted Yearly Spending");

        return predictions;
//        return null;
    }

}
