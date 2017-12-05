package ItemBasedCf.testlamda;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

/**
 * Author: changdalin
 * Date: 2017/12/4
 * Description:
 **/
public class LambdaWord {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("LAMBDA_WORD").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        String path = "u.data";
        JavaRDD<String> data = sc.textFile(path);

        JavaPairRDD<String, Integer> countMap = data
                //对每一行使用，然后形成一个总的list
                //基本用法，左面是param - >右面表达式
                .flatMap((line) -> Arrays.asList(line.split("\t")))
                //对形成的list里面的每一个元素使用
                .mapToPair((w) -> new Tuple2<String, Integer>(w, 1))
                //对于两个value而言
                .reduceByKey((x, y) -> x + y);
        List<Tuple2<String, Integer>> result = countMap.collect();
        Collections.sort(result, new Comparator<Tuple2<String, Integer>>() {
            @Override
            public int compare(Tuple2<String, Integer> o1, Tuple2<String, Integer> o2) {
                if (o1._2() < o2._2()) {
                    return 1;
                } else if (o1._2() > o2._2()) {
                    return -1;
                }
                return 0;
            }
        });
        System.out.println(result);
    }
}