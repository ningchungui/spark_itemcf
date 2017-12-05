package ItemBasedCf.testlamda;

import java.util.ArrayList;
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
public class Lambda2Word {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("LAMBDA2_WORD").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        String path = "u.data";
        JavaRDD<String> data = sc.textFile(path);
        //一个word形成的list
        JavaRDD<String> wordListRDD
                = data.flatMap((line) -> Arrays.asList(line.split("\t")));

        //注意maptopair这个意思是一个一个的进入，形成一个一个的tuple
        //注意mapPartitionsToPair这个意思是整个group 进入，也是形成一个一个的tuple
        JavaPairRDD<String, Integer> allPairs
                = wordListRDD.mapPartitionsToPair((list) -> {
            List<Tuple2<String, Integer>> pairs = new ArrayList<Tuple2<String, Integer>>();
            while (list.hasNext()) {
                String temp = list.next();
                pairs.add(new Tuple2<>(temp, 1));
            }
            return pairs;
        }, true);
        JavaPairRDD<String, Integer> reducePairs = allPairs.reduceByKey((v1, v2) -> v1 + v2);
        List<Tuple2<String, Integer>> result = reducePairs.collect();
        //排序
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
