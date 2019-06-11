package cn.edu360.spark.dataskewed;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.actors.threadpool.Arrays;

import java.util.List;

/**
 * created by LMR on 2019/5/27
 */
public class Test6 {
    static SparkConf conf = null;
    static JavaSparkContext jsc = null;
    static {
        conf = new SparkConf();
        conf.setMaster("local").setAppName("Transformation");
        jsc = new JavaSparkContext(conf);
    }
    public static void main(String[] args) {
        Tuple2<String, Integer> data[] = new Tuple2[]{
                new Tuple2<String, Integer>("武当", 99),
                new Tuple2<String, Integer>("少林", 97),
                new Tuple2<String, Integer>("武当", 89),
                new Tuple2<String, Integer>("少林", 77)};
        List<Tuple2<String, Integer>> list = Arrays.asList(data);

        JavaRDD<Tuple2<String, Integer>> rdd = jsc.parallelize(list);
       // rdd.flatMapToPair()

    }

}
