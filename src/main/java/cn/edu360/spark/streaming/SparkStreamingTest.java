package cn.edu360.spark.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/**
 * created by LMR on 2019/6/3
 */
public class SparkStreamingTest {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("SparkStreaming");
       // JavaStreamingContext jsc = new JavaStreamingContext(conf);
    }
}
