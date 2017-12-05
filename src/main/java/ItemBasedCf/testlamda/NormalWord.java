package ItemBasedCf.testlamda;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

/**
 * Author: changdalin
 * Date: 2017/12/4
 * Description:
 **/
public class NormalWord {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("NORMAL_WORD").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        String path = "u.data";
        JavaRDD<String> data = sc.textFile(path);

        /**
         * date:2017/12/4
         * description:因为flatMap肯定是返回一个list，所以< String >是一个泛型
         */        
        JavaRDD<String> words = data.flatMap(
                new FlatMapFunction<String, String>() {
            public Iterable<String> call(String s) {
                return Arrays.asList(s.split("\t"));
            }
        });

        /**
         * date:2017/12/4
         * description:注意，new function里面是partition的，则是一个整体，是普通的function的，是对应一行
         */
        JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) {
                //tule2是一个map,一会儿reduceByKey
                return new Tuple2<String, Integer>(s, 1);
            }
        });

        JavaPairRDD<String, Integer> counts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            //v1 和 v2表示的是两个相同key的value
            public Integer call(Integer a, Integer b) {
                return a + b;
            }
        });
        //collect是转化成为一个list
        System.out.println(counts.collect());
    }
}