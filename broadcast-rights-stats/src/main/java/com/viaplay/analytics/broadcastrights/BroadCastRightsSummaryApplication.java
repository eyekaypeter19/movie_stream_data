package com.viaplay.analytics.broadcastrights;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import scala.collection.JavaConverters;

import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.col;

public class BroadCastRightsSummaryApplication {

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkSession session = SparkSession.builder().appName("BroadcastRightApplication").master("local[1]").getOrCreate();
        Dataset<Row> broadCastRights = session.read().option("header", "true").csv("in/started_streams_new.csv")
                .withColumn("product_type", upper(col("product_type")))
                .withColumn("user_id", lower(col("user_id")))
                .withColumn("device_name", lower(col("device_name")))
                .withColumnRenamed("country_code", "Region");

        Dataset<Row> metadata = session.read().option("header", "true").csv("in/whatson.csv")
                .withColumnRenamed("broadcast_right_region", "Region");


        Dataset<Row> countries = session.read().option("header", "true").csv("in/countries.csv")
                .withColumn("Code", lower(col("Code"))).withColumnRenamed("Name", "Region");


        metadata = metadata.join(countries, "Region").drop("Region").withColumn("Region",
                col("Code")).drop("Code").drop("dt")
                .withColumn("joincol", concat(col("house_number"), lit("|"), col("Region")))
                .drop("house_number").drop("Region");
        metadata.cache();

        Dataset<Row> filtered = broadCastRights.filter(col("product_type")
                .equalTo("TVOD").or(col("product_type").equalTo("EST")))
                .withColumn("joincol", concat(col("house_number"), lit("|"),
                        col("Region")));
        filtered = filtered.join(metadata, "joincol");
        filtered = filtered.selectExpr(convertListToSeq()).withColumnRenamed("Region", "country_code");
        filtered.write().option("header", "true").mode(SaveMode.Overwrite).csv("out/broad-cast-writes.csv");
    }

    private static List<String> getColumnNames() {
        List<String> colNames = Arrays.asList("dt", "time", "device_name", "house_number",
                "user_id", "Region", "program_title", "season", "season_episode", "genre",
                "product_type", "broadcast_right_start_date",
                "broadcast_right_end_date");
        return colNames;
    }

    public static scala.collection.Seq<String> convertListToSeq() {
        return JavaConverters.asScalaIteratorConverter(getColumnNames().iterator()).asScala().toSeq();
    }

}
