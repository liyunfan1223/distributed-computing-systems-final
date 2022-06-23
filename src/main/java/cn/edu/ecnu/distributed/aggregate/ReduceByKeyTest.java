package cn.edu.ecnu.distributed.aggregate;

import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.codehaus.janino.Java;
import scala.Tuple2;
import scala.Tuple3;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ReduceByKeyTest {
    public static void run(String []args) {
        SparkConf conf = new SparkConf().setAppName("ReduceByKeyTest").setMaster("local[*]");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        JavaRDD<String> table1 = jsc.textFile(args[0]);
        JavaRDD<Tuple2<String, Integer>> javaRDD1  = table1.map(new Function<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                String[] items = s.split("\t");
                return new Tuple2<>(items[0], Integer.valueOf(items[1]));
            }
        });
        JavaPairRDD<String, Integer> javaRDD3 = javaRDD1.mapToPair(new PairFunction<Tuple2<String, Integer>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<String, Integer> tuple2) {
                return tuple2;
            }
        });
        // reduceByKey实现聚合
        JavaPairRDD<String, Integer> javaRDD5 = javaRDD3.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });
        javaRDD5.saveAsTextFile(args[1]);
        jsc.stop();
    }

    public static void main(String[] args) {
        run(args);
    }
}
