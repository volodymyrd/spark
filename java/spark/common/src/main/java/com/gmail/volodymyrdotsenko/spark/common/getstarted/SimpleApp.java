package com.gmail.volodymyrdotsenko.spark.common.getstarted;

import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;

/**
 * spark-submit --class "com.gmail.volodymyrdotsenko.spark.common.getstarted.SimpleApp" --master local[4] ~/work/workspace/idea/spark/java/spark/common/target/libs/common.jar
 */
public class SimpleApp {
    public static void main(String[] args) {
        String path = System.getenv("SPARK_HOME");
        System.out.println(System.getenv("SPARK_HOME"));
        String logFile = path + "/README.md"; // Should be some file on your system
        SparkConf conf = new SparkConf().setAppName("Simple Application");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> logData = sc.textFile(logFile).cache();

        long numAs = logData.filter(new Function<String, Boolean>() {
            public Boolean call(String s) {
                return s.contains("a");
            }
        }).count();

        long numBs = logData.filter(new Function<String, Boolean>() {
            public Boolean call(String s) {
                return s.contains("b");
            }
        }).count();

        System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);

        sc.stop();
    }
}
