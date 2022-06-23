package cn.edu.ecnu.distributed.join;

import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

public class SaltShuffleJoin {
    public static void run(String []args) {
        SparkConf conf = new SparkConf().setAppName("SaltShuffleJoin").setMaster("local[*]");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        int partition_num = Integer.valueOf(args[3]);
        String skewed_key = "key0";
        JavaRDD<String> table1 = jsc.textFile(args[0]);
        Random r = new Random();
        //通过parallelize构建第一个RDD
//        JavaRDD<Tuple2<String, Integer>> javaRDD1 = jsc.parallelize(tuple2List1);
        JavaRDD<Tuple2<String, Integer>> javaRDD1  = table1.map(new Function<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                String[] items = s.split("\t");
                if (items[0].equals(skewed_key)) {
                    items[0] += "_" + r.nextInt(partition_num);
                }
                return new Tuple2<>(items[0], Integer.valueOf(items[1]));
            }
        });

        JavaRDD<String> table2 = jsc.textFile(args[1]);
        //通过parallelize构建第二个RDD
        JavaRDD<Tuple2<String, Integer>> javaRDD2  = table2.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Iterator<Tuple2<String, Integer>> call(String s) throws Exception {
                List<Tuple2<String, Integer>> list = new ArrayList<>();
                String[] items = s.split("\t");
                if (items[0].equals(skewed_key)) {
                    for (int i = 0; i < partition_num; i++) {
                        list.add(new Tuple2<>(items[0] + "_" + i, Integer.valueOf(items[1])));
                    }
                } else {
                    list.add(new Tuple2<>(items[0], Integer.valueOf(items[1])));
                }
                return list.iterator();
            }
        });

        //通过mapToPair根据第一个RDD构建第三个RDD
        JavaPairRDD<String, Integer> javaRDD3 = javaRDD1.mapToPair(new PairFunction<Tuple2<String, Integer>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<String, Integer> tuple2) {
                return tuple2;
            }
        });
        //通过partitionBy根据第三个RDD构建第五个RDD
        JavaPairRDD<String, Integer> javaRDD31 = javaRDD3.partitionBy(new HashPartitioner(Integer.valueOf(args[3])));

//        通过mapToPair根据第二个RDD构建第四个RDD
        JavaPairRDD<String, Integer> javaRDD4 = javaRDD2.mapToPair(new PairFunction<Tuple2<String, Integer>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<String, Integer> tuple2) {
                return tuple2;
            }
        });
//
//        //通过partitionBy根据第四个RDD构建第六个RDD
        JavaPairRDD<String, Integer> javaRDD41 = javaRDD4.partitionBy(new HashPartitioner(Integer.valueOf(args[3])));

        //通过join 根据第五和第六个RDD构建出第七个RDD
        JavaPairRDD<String, Tuple2<Integer, Integer>> javaRDD6 = javaRDD31.join(javaRDD41);

        JavaPairRDD<String, Tuple2<Integer, Integer>> javaRDD7 = javaRDD6.mapToPair(new PairFunction<Tuple2<String, Tuple2<Integer, Integer>>, String, Tuple2<Integer, Integer>>() {
            @Override
            public Tuple2<String, Tuple2<Integer, Integer>> call(Tuple2<String, Tuple2<Integer, Integer>> t) throws Exception {
                String[] strings = t._1.split("_");
                return new Tuple2<>(strings[0], t._2);
            }
        });
        javaRDD7.saveAsTextFile(args[2]);
        /* 步骤3：关闭SparkContext */
        jsc.stop();
    }

    public static void main(String[] args) {
        run(args);
    }
}
