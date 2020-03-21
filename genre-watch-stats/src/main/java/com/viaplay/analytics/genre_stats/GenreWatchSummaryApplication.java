package com.viaplay.analytics.genre_stats;
import com.oracle.xmlns.internal.webservices.jaxws_databinding.JavaParam;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.catalyst.expressions.Encode;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.roaringbitmap.RoaringBitmapWriter;
import scala.Function1;
import scala.Tuple2;
import scala.reflect.ClassTag;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.col;

public class GenreWatchSummaryApplication implements Serializable {

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkSession sparkSession =
                SparkSession.builder().master("local[*]").appName("GenreWatchSummaryApplication").getOrCreate();
        //We could get the file location as a parameter passed in as part of the argument
        //But we want to keep things simple... so we are just going to load it from disk
        Dataset<Row> streamData = sparkSession.read().option("header", "true").csv("in/started_streams_new.csv")
                .withColumn("user_id", lower(col("user_id")))
                .withColumn("watched_hour", split(col("time"), ":").getItem(0));
        //we drop the columns that we do not need to save computation resources
        streamData.drop("dt", "program_title", "device_name", "country_code", "time",
                "product_type", "house_number", "season", "product_type", "season_episode");

        //next we create a key value pair mapping where key is the genre and hour it was watched and
        // the value is the user that watched it
        JavaPairRDD<Tuple2<String, String>, String> genreSummary =
                streamData.toJavaRDD().mapToPair(row ->
                        new Tuple2<>(new Tuple2<>(row.getAs("genre").toString(),
                                row.getAs("watched_hour").toString()), row.getAs("user_id").toString()));

        //caching this, we do not want to repeat the above steps for any reason whatsoever
        genreSummary.cache();
        //Lets attempt to use a more performant approach that minimizes shuffling rather than just grouping and aggregating
        //we are  going to use the combineBykey Approach here

        //first we need a combiner
        Function<String, Set<String>> createCombiner = new Function<String, Set<String>>() {
            @Override
            public Set<String> call(String s) {
                Set<String> set = new HashSet<>();
                set.add(s);
                return set;
            }
        };

        //next we need a function to merge values for same key
        Function2<Set<String>, String, Set<String>> mergeValues =
                new Function2<Set<String>, String, Set<String>>() {
                    @Override
                    public Set<String> call(Set<String> v1, String v2) {
                        v1.add(v2);
                        return v1;
                    }
                };

        //this function combines values for same key buckets
        Function2<Set<String>, Set<String>, Set<String>> combine =
                new Function2<Set<String>, Set<String>, Set<String>>() {
                    @Override
                    public Set<String> call(Set<String> v1, Set<String> v2) {
                        v1.addAll(v2);
                        return v1;
                    }
                };


        //actually run the combine by key aggregation;
        JavaPairRDD<Tuple2<String, String>, Set<String>> result =
                genreSummary.combineByKey(createCombiner, mergeValues, combine);
        JavaPairRDD<Tuple2<String, String>, Long> userCountResult =
                result.mapToPair(t -> {
                    return new Tuple2<>(t._1(), (long) t._2().size());
                });

        //we have a grouping of Movie Hour Keys, and unique count of users that watched that grouping
        //lets convert it to a dataframe and persist it to disk as results

        Dataset<Row> df = sparkSession.createDataset(JavaPairRDD.toRDD(userCountResult),
                Encoders.tuple(Encoders.tuple(Encoders.STRING(), Encoders.STRING()), Encoders.LONG())).toDF("genre   " +
                "watched_hour", "unique_users");
        df.show(10);
        df.write().mode(SaveMode.Overwrite).csv("out/genre_summary.csv");

    }

    static Function1<Tuple2<Tuple2<String, String>, Long>, Row> mapFunction = new Function1<Tuple2<Tuple2<String, String>, Long>, Row>() {
        @Override
        public Row apply(Tuple2<Tuple2<String, String>, Long> v1) {
            return RowFactory.create(v1._1()._1(), v1._1()._2(), v1._2());
        }
    };

    static Function1<Row, Row> mapDfFunction = new Function1<Row, Row>() {
        @Override
        public Row apply(Row v1) {
            return RowFactory.create(v1.getList(0).get(1), v1.getList(0).get(0), v1.getLong(1));
        }
    };

}