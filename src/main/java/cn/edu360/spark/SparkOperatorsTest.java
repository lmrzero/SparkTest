package cn.edu360.spark;

import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.actors.threadpool.Arrays;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * created by LMR on 2019/5/27
 */
public class SparkOperatorsTest {

    static SparkConf conf = null;
    static JavaSparkContext jsc = null;
    static {
        conf = new SparkConf();
        conf.setMaster("local").setAppName("Transformation");
        jsc = new JavaSparkContext(conf);
    }
    public static void main(String[] args) {
        aggregateByKey();

    }

    public static void map(){
        String[] name = {"aa","bb","cc"};
        List<String> list = Arrays.asList(name);
        JavaRDD<String> parallelize = jsc.parallelize(list);
        JavaRDD<String> map = parallelize.map(n -> {
            return "Hello " + n;
        });
        map.foreach(n -> System.out.println(n));
    }

    public static void flatMap(){
        String[] name = {"aa AA","bb BB","cc CC"};
        List<String> list = Arrays.asList(name);
        JavaRDD<String> parallelize = jsc.parallelize(list);

        JavaRDD map = parallelize.flatMap(line -> Arrays.asList(line.split(" ")).iterator()).map(l -> "Hello " + l);
        map.foreach(n -> System.out.println(n));
    }

    public static void mapPartitions(){//每次处理一个分区的数据
        Object[] nums = {1,2,3,4,5,6,7};
        List<Integer> list = Arrays.asList(nums);
        JavaRDD<Integer> parallelize = jsc.parallelize(list,2);//参数2表示有2个分区
        parallelize.mapPartitions(iterator -> {
            ArrayList<String> array = new ArrayList<>();
            while (iterator.hasNext())
            {
                array.add("Hello "+ iterator.next());
            }
            return array.iterator();
        }).foreach(name -> System.out.println(name));
    }

    public static void mapParatitionWithIndex(){
        Object[] nums = {1, 2, 3, 4, 5, 6, 7, 8};
        List<Integer> list = Arrays.asList(nums);
        JavaRDD<Integer> listRDD = jsc.parallelize(list, 2);
        listRDD.mapPartitionsWithIndex((index,iterator) -> {
            ArrayList<String> list1 = new ArrayList<>();
            while (iterator.hasNext()){
                list1.add(index+"_"+iterator.next());
            }
            return list1.iterator();
        },true)
                .foreach(str -> System.out.println(str));
    }

    public static void reduce(){
        Object[] nums = {1, 2, 3, 4, 5, 6, 7, 8};
        List<Integer> list = Arrays.asList(nums);
        JavaRDD<Integer> listRDD = jsc.parallelize(list);
        Integer result = listRDD.reduce((x, y) -> x + y);
        System.out.println(result);
    }

    public static void reduceByKey(){
        Tuple2<String, Integer> data[] = new Tuple2[]{
                new Tuple2<String, Integer>("武当", 99),
                new Tuple2<String, Integer>("少林", 97),
                new Tuple2<String, Integer>("武当", 89),
                new Tuple2<String, Integer>("少林", 77)};
        List<Tuple2<String, Integer>> list = Arrays.asList(data);
        JavaPairRDD<String, Integer> listRDD = jsc.parallelizePairs(list);

        JavaPairRDD<String, Integer> resultRDD = listRDD.reduceByKey((x, y) -> x + y);
        resultRDD.foreach(tuple -> System.out.println("门派: " + tuple._1 + "->" + tuple._2));
    }

    public static void sample(){//采样
        ArrayList<Integer> list = new ArrayList<>();
        for(int i=1;i<=100;i++){
            list.add(i);
        }
        JavaRDD<Integer> listRDD = jsc.parallelize(list);

        JavaRDD<Integer> sampleRDD = listRDD.sample(false, 0.1, 0);
        sampleRDD.foreach(num -> System.out.println(num + " "));
    }

    public static void cartesian(){//笛卡尔乘积

        String data1[] = {"A", "B"};
        Integer data2[] = {1, 2, 3};
        List<String> list1 = Arrays.asList(data1);
        List<Integer> list2 = Arrays.asList(data2);
        JavaRDD<String> list1RDD = jsc.parallelize(list1);
        JavaRDD<Integer> list2RDD = jsc.parallelize(list2);
        list1RDD.cartesian(list2RDD).foreach(tuple -> System.out.println(tuple._1 + "->" + tuple._2));
    }

    public static void intersection() {//交集
        Integer data1[] = {1, 2, 3, 4};
        Integer data2[] = {3, 4, 5, 6};
        List<Integer> list1 = Arrays.asList(data1);
        List<Integer> list2 = Arrays.asList(data2);
        JavaRDD<Integer> list1RDD = jsc.parallelize(list1);
        JavaRDD<Integer> list2RDD = jsc.parallelize(list2);
        list1RDD.intersection(list2RDD).foreach(num ->System.out.println(num));
    }

    public static void coalesce() {//Return a new RDD that is reduced into `numPartitions` partitions.
        //原本的3个分区变成一个分区
        Integer data[] = {1, 2, 3, 4, 5, 6, 7, 8, 9};
        List<Integer> list = Arrays.asList(data);
        JavaRDD<Integer> listRDD = jsc.parallelize(list, 3);
        listRDD.coalesce(1).foreach(num -> System.out.println(num));
    }

    public static void replication(){//增加分区
        Integer data[] = {1, 2, 3, 4, 5, 6, 7, 8, 9};
        List<Integer> list = Arrays.asList(data);
        JavaRDD<Integer> listRDD = jsc.parallelize(list, 1);
        listRDD.repartition(2).foreach(num -> System.out.println(num));
    }

    public static void repartitionAndSortWithinPartitions(){//增加分区，并且每个分区内排序
        Integer data[] = {1, 4, 55, 66, 33, 48, 23};
        List<Integer> list = Arrays.asList(data);
        JavaRDD<Integer> listRDD = jsc.parallelize(list, 1);
        JavaPairRDD<Integer, Integer> pairRDD = listRDD.mapToPair(num -> new Tuple2<>(num, num));
        pairRDD.repartitionAndSortWithinPartitions(new HashPartitioner(2))
                .mapPartitionsWithIndex((index,iterator) -> {
                    ArrayList<String> list1 = new ArrayList<>();
                    while (iterator.hasNext()){
                        list1.add(index+"_"+iterator.next());
                    }
                    return list1.iterator();
                },false)
                .foreach(str -> System.out.println(str));

        /*
        0_(4,4)
        0_(48,48)
        0_(66,66)
        1_(1,1)
        1_(23,23)
        1_(33,33)
        1_(55,55)
        *
        * */
    }

    public static void cogroup(){//一个RDD与其余2个RDD聚合

        Tuple2<Integer, String> data1[] = new Tuple2[]{
                new Tuple2<Integer, String>(1, "www"),
                new Tuple2<Integer, String>(2, "bbs")};
        List<Tuple2<Integer, String>> list1 = Arrays.asList(data1);

        Tuple2<Integer, String>[] data2 = new Tuple2[]{
                new Tuple2<Integer, String>(1, "cnblog"),
                new Tuple2<Integer, String>(2, "cnblog"),
                new Tuple2<Integer, String>(3, "very")
        };
        List<Tuple2<Integer, String>> list2 = Arrays.asList(data2);

        Tuple2<Integer, String> []data3 = new Tuple2[]{
                new Tuple2<Integer, String>(1, "com"),
                new Tuple2<Integer, String>(2, "com"),
                new Tuple2<Integer, String>(3, "good")
        };
        List<Tuple2<Integer, String>> list3 = Arrays.asList(data3);

        JavaPairRDD<Integer, String> list1RDD = jsc.parallelizePairs(list1);
        JavaPairRDD<Integer, String> list2RDD = jsc.parallelizePairs(list2);
        JavaPairRDD<Integer, String> list3RDD = jsc.parallelizePairs(list3);

        list1RDD.cogroup(list2RDD,list3RDD).foreach(tuple ->
                System.out.println(tuple._1+" " +tuple._2._1() +" "+tuple._2._2()+" "+tuple._2._3()));
    }

    public static void sortByKey(){

        Tuple2<Integer, String> data[] = new Tuple2[]{
                new Tuple2<>(99, "张三丰"),
                new Tuple2<>(96, "东方不败"),
                new Tuple2<>(66, "林平之"),
                new Tuple2<>(98, "聂风")
        };
        List<Tuple2<Integer, String>> list = Arrays.asList(data);
        JavaPairRDD<Integer, String> listRDD = jsc.parallelizePairs(list);
        listRDD.sortByKey(false).foreach(tuple ->System.out.println(tuple._2+"->"+tuple._1));
    }

    public static void aggregateByKey() {
        String[] data = {"you,jump", "i,jump"};
        List<String> list = Arrays.asList(data);
        JavaRDD<String> listRDD = jsc.parallelize(list);
        JavaPairRDD<String, Integer> javaPairRDD = listRDD.flatMap(line -> Arrays.asList(line.split(",")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .aggregateByKey(0, (x, y) -> (int) x + (int) y, (m, n) -> (int) m + (int) n);
        javaPairRDD.foreach(tuple -> System.out.println(tuple._1+"->"+tuple._2));
    }


}
