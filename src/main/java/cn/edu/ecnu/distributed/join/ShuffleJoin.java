package cn.edu.ecnu.distributed.join;

import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import scala.Tuple3;

public class ShuffleJoin {
    public static void run(String []args) {
        SparkConf conf = new SparkConf().setAppName("ShuffleJoin").setMaster("local[*]");
        JavaSparkContext jsc = new JavaSparkContext(conf);

//        List<Tuple2<String, Integer>> tuple2List1 = Arrays.asList(new Tuple2<>("Alice", 15), new Tuple2<>("Bob", 18), new Tuple2<>("Thomas", 20), new Tuple2<>("Catalina", 25));
//        List<Tuple3<String, String, String>> tuple3List = Arrays.asList(new Tuple3<>("Alice", "Female", "NanJ"), new Tuple3<>("Thomas", "Male", "ShangH"), new Tuple3<>("Tom", "Male", "BeiJ"));

//        //通过parallelize构建第一个RDD
//        JavaRDD<Tuple2<String, Integer>> javaRDD1 = jsc.parallelize(tuple2List1);
//
//        //通过parallelize构建第二个RDD
//        JavaRDD<Tuple3<String, String, String>> javaRDD2 = jsc.parallelize(tuple3List);
        JavaRDD<String> table1 = jsc.textFile(args[0]);
        //通过parallelize构建第一个RDD
//        JavaRDD<Tuple2<String, Integer>> javaRDD1 = jsc.parallelize(tuple2List1);
        JavaRDD<Tuple2<String, Integer>> javaRDD1  = table1.map(new Function<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                String[] items = s.split("\t");
                return new Tuple2<>(items[0], Integer.valueOf(items[1]));
            }
        });

//        JavaRDD<String> table2 = jsc.textFile("src/main/resources/input/table2.txt");
        JavaRDD<String> table2 = jsc.textFile(args[1]);
        //通过parallelize构建第二个RDD
//        JavaRDD<Tuple3<String, String, String>> javaRDD2 = jsc.parallelize(tuple3List);
        JavaRDD<Tuple2<String, Integer>> javaRDD2  = table2.map(new Function<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                String[] items = s.split("\t");
                return new Tuple2<>(items[0], Integer.valueOf(items[1]));
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
//        JavaPairRDD<String, Integer> javaRDD31 = javaRDD3.partitionBy(new HashPartitioner(Integer.valueOf(args[3])));

//        通过mapToPair根据第二个RDD构建第四个RDD
        JavaPairRDD<String, Integer> javaRDD4 = javaRDD2.mapToPair(new PairFunction<Tuple2<String, Integer>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<String, Integer> tuple2) {
                return tuple2;
            }
        });
//
//        //通过partitionBy根据第四个RDD构建第六个RDD
//        JavaPairRDD<String, Integer> javaRDD41 = javaRDD4.partitionBy(new HashPartitioner(Integer.valueOf(args[3])));

        //通过join 根据第五和第六个RDD构建出第七个RDD
        JavaPairRDD<String, Tuple2<Integer, Integer>> javaRDD6 = javaRDD3.join(javaRDD4);

//        javaRDD6.foreach(new VoidFunction<Tuple2<String, Tuple2<Integer, Tuple2<String, String>>>>() {
//            @Override
//            public void call(Tuple2<String, Tuple2<Integer, Tuple2<String, String>>> stringTuple2Tuple2) throws Exception {
//                System.out.print(stringTuple2Tuple2);
//            }
//        });
        javaRDD6.saveAsTextFile(args[2]);
        /* 步骤3：关闭SparkContext */
        jsc.stop();
    }

    public static void main(String[] args) {
        run(args);
    }
}
