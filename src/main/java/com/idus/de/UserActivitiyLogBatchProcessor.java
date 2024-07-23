package com.idus.de;

import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

public class UserActivitiyLogBatchProcessor {

    public static void main(String[] args) {

        if (args.length < 1) {
            System.err.println("Usage: UserActivitiyLogBatchProcessor <data_file_path>");
            System.exit(-1);
        }

        String dataFilePath = args[0];
        UserActivitiyLogBatchProcessor userActivitiyLogBatchProcessor = new UserActivitiyLogBatchProcessor();
        userActivitiyLogBatchProcessor.processData(dataFilePath);
    }

    private void processData(String dataFilePath) {
        String outputPath = "hdfs:///user/spark/user_activity_logs";
        String logFilePath = "/var/log/spark/failure.log";
        String hiveDb = "spark";
        String hiveTable = "user_activity_logs";

        SparkSession spark = createSparkSession();

        try {
            Dataset<Row> df = readData(spark, dataFilePath);
            df = transformData(df);
            createPartitions(spark, df, hiveDb, hiveTable);
            writeData(df, outputPath);
            System.out.println("Data processing and writing to HDFS completed successfully.");
        } catch (Exception e) {
            logFailure(logFilePath, e.getMessage());
            System.err.println("Error occurred: " + e.getMessage());
            System.exit(-1);
        } finally {
            spark.stop();
        }
    }

    private SparkSession createSparkSession() {
        return SparkSession.builder()
                .appName("UserActivitiyLogBatchProcessor")
                .config("spark.sql.warehouse.dir", "hdfs:///user/hive/warehouse")
                .config("hive.metastore.uris", "thrift://hive-metastore:9083")
                .enableHiveSupport()
                .getOrCreate();
    }

    private Dataset<Row> readData(SparkSession spark, String dataFilePath) {
        Dataset<Row> df = spark.read().option("header", "true").csv(dataFilePath);
        for (String colName : df.columns()) {
            df = df.withColumn(colName, df.col(colName).cast("string"));
        }
        return df;
    }

    private Dataset<Row> transformData(Dataset<Row> df) {
        df = df.withColumn("event_time_kst", functions.from_utc_timestamp(df.col("event_time"), "Asia/Seoul").cast("string"));
        df = df.withColumn("yyyymmdd", functions.date_format(df.col("event_time_kst"), "yyyyMMdd"));
        return df;
    }

    private void createPartitions(SparkSession spark, Dataset<Row> df, String hiveDb, String hiveTable) {
        Dataset<Row> distinctDates = df.select("yyyymmdd").distinct();
        List<Row> partitionValues = distinctDates.collectAsList();
        for (Row row : partitionValues) {
            String partitionValue = row.getString(0);
            spark.sql(String.format("ALTER TABLE %s.%s ADD IF NOT EXISTS PARTITION (yyyymmdd='%s')", hiveDb, hiveTable, partitionValue));
        }
    }

    private void writeData(Dataset<Row> df, String outputPath) {
        df.write()
                .mode("append")
                .format("parquet")
                .option("compression", "snappy")
                .partitionBy("yyyymmdd")
                .save(outputPath);
    }

    private void logFailure(String logFilePath, String message) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(logFilePath, true))) {
            String timeStamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
            writer.write(timeStamp + " - " + message);
            writer.newLine();
        } catch (IOException e) {
            System.err.println("Failed to write to log file: " + e.getMessage());
        }
    }
}
