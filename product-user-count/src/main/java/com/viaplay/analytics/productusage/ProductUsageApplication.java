package com.viaplay.analytics.productusage;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.count;

public class ProductUsageApplication {

    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkSession session = SparkSession.builder().appName("BroadcastRightApplication").master("local[1]").getOrCreate();
        Dataset<Row> streamsummary = session.read().option("header", "true").csv("in/started_streams_new.csv")
                .withColumn("product_type", upper(col("product_type")))
                .withColumn("user_id", lower(col("user_id")))
                .withColumn("device_name", lower(col("device_name")))
                .withColumn("country_code", lower(col("country_code")));
        streamsummary.cache();
        streamsummary = streamsummary.groupBy("country_code", "device_name", "dt", "product_type",
                "program_title").agg(countDistinct("user_id").as("unique_users"),
                count("time").as("content_count"));

        streamsummary.coalesce(1).write().option("header","true").mode(SaveMode.Overwrite)
                .csv("out/product_user_count.csv");

    }
}
