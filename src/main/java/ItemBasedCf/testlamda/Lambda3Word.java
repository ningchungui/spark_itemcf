package ItemBasedCf.testlamda;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

/**
 * Author: changdalin
 * Date: 2017/12/5
 * Description:
 **/
public class Lambda3Word {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("LAMBDA3_WORD").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        String path = "u.data";
        JavaRDD<String> data = sc.textFile(path);

        JavaRDD<String> wordList = data.flatMap((line) -> {
            String[] split = line.split("\t");
            List<String> list = Arrays.asList(split);
            return list;
        });

        JavaPairRDD<String, Integer> pairList = wordList.mapToPair((word) -> {
            return new Tuple2<String, Integer>(word, 1);
        });

        JavaPairRDD<String, Integer> resultRdd = pairList.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        List<Tuple2<String, Integer>> result = resultRdd.collect();
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
