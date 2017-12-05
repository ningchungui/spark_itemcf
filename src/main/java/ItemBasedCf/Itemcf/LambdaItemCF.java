package ItemBasedCf.Itemcf;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import Jama.Matrix;
import org.apache.commons.collections.CollectionUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

/**
 * Author: changdalin
 * Date: 2017/12/5
 * Description:
 **/
public class LambdaItemCF {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("LAMBDA_ITEWCF").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        String path = "u.data";
        JavaRDD<String> data = sc.textFile(path);

        //去重找到userList
        JavaRDD<Integer> userRdd = data.map((line) -> {
            return Integer.parseInt(line.split("\t")[0]);
        });
        List<Integer> userList = userRdd.distinct().collect();
        System.out.println(userList.size());
        //去重找到movieList
        JavaRDD<Integer> movieRdd = data.map((line) -> {
            return Integer.parseInt(line.split("\t")[1]);
        });
        List<Integer> movieList = movieRdd.distinct().collect();
        System.out.println(movieList.size());
        Collections.sort(movieList);
        Collections.sort(userList);

        final int usersSize = userList.size();
        final int moviesSize = movieList.size();


        /**
         * date:2017/12/5
         * description:将原来的一行，转化成为 < userid, < product,rating > > ,即pairRdd
         * 最后的return值是一个tuple2
         */
        JavaPairRDD<Integer, Tuple2<Integer, Double>> pairRDD = data.mapToPair((line) -> {
            String[] arr = line.split("\t");
            int movieId = Integer.parseInt(arr[1]);
            int userId = Integer.parseInt(arr[0]);
            double rating = Double.parseDouble(arr[2]);
            return new Tuple2(userId, new Tuple2<>(movieId, rating));
        });
        //按照userid做局部有序
        pairRDD = pairRDD.sortByKey();


        /**
         * date:2017/12/5
         * description:将pairRdd全部导入
         * 因为不是每一行改变，而是整体改变，所以全部导入
         * 输入是iterator，其实就是data的行数
         *     < u1 , < m1,1.0 > >
         *     < u1 , < m3,0 > >
         * 输出是iterator,一个list,这个就是 u,movieList 的userMatrix
         *     < u1, [0,1.0,4.0,0 ....] >
         *     < u2, [0,.....] >
         */
        JavaPairRDD<Integer, List<Double>> userMatrix = pairRDD.mapPartitionsToPair((pairs) -> {
            List<Tuple2<Integer, List<Double>>> userMat
                    = new ArrayList<>();
            List<Double> tempUserRating = new ArrayList<Double>();
            int tempUserId = 0;
            while (pairs.hasNext()) {
                Tuple2<Integer, Tuple2<Integer, Double>> oneLine = pairs.next();
                int userId = oneLine._1();
                int movieId = oneLine._2()._1();
                double rating = oneLine._2()._2();
                int movieIndex = movieList.indexOf(movieId);
                //如果新读进来的userId和tempId不相等，则更新，说明一个用户的读完了
                if (userId != tempUserId) {
                    if (CollectionUtils.isNotEmpty(tempUserRating)) {
                        userMat.add(new Tuple2<>(tempUserId, tempUserRating));
                    }
                    tempUserId = userId;
                    tempUserRating = new ArrayList<Double>();
                    for (int i = 0; i < moviesSize; i++) {
                        tempUserRating.add((double) 0);
                    }
                    tempUserRating.set(movieIndex, rating);
                } else {
                    //如果等于userId
                    tempUserRating.set(movieIndex, rating);
                }
            }
            return userMat;
        });


        /**
         * date:2017/12/5
         * description:
         * 输入 : < u1 ,[1.0,2.0,....] > 这个list
         * 输出 : [
         *   [1.0,2.0, .......],
         *   [2.0,3.0,........]....
         * ]
         * 即相关度的List < List >
         * 也是一起输入，但输出是list(list)
         * 注意，带有partition的函数，输入是iterator的迭代，输出也是iterator的list,一般都是用一个list接
         */
        JavaRDD<List<Double>> relationMatrix = userMatrix.mapPartitions((pairs) -> {
            List<List<Double>> relationMat = new ArrayList<List<Double>>();
            Matrix uMatrix = new Matrix(usersSize, moviesSize);
            Matrix rMatrix = new Matrix(moviesSize, moviesSize);
            int i = 0;
            //将userMatrix的数据全部放入matrix里面
            while (pairs.hasNext()) {
                Tuple2<Integer, List<Double>> oneUserPair = pairs.next();
                List<Double> oneUserList = oneUserPair._2();
                for (int j = 0; j < oneUserList.size(); j++) {
                    uMatrix.set(i, j, oneUserList.get(j));
                }
                i++;
            }

            //记录两个相邻的product的得分情况
            double[] r1 = new double[usersSize];
            double[] r2 = new double[usersSize];
            //循环所有product,给r1和r2分别赋值
            for (int k = 0; k < moviesSize; k++) {
                //给r1赋值
                for (int j = 0; j < usersSize; j++) {
                    double d1 = uMatrix.get(j, k);
                    r1[j] = d1;
                }
                for (int k1 = k + 1; k1 < moviesSize; k1++) {
                    //给r2赋值
                    for (int g = 0; g < usersSize; g++) {
                        r2[g] = uMatrix.get(g, k1);
                    }
                    double num = 0, d2 = 0, e2 = 0;
                    //开始求r1和r2的关系，计算相关度
                    //r1就是一条向量，里面有userSize的值
                    for (int w = 0; w < usersSize; w++) {
                        // Σ(u1i*u2i)/ 开方(Σu1i*u1i) * 开方(Σ u2i*u2i)
                        num += r1[w] * r2[w];
                        d2 += r1[w] * r1[w];
                        e2 += r2[w] * r2[w];
                    }
                    double score;
                    if ((d2 == 0 || e2 == 0)) {
                        //这是有可能的，一个product没有评分
                        score = 0.0;
                    } else {
                        score = (num) / (Math.sqrt(e2) * Math.sqrt(d2));
                    }
                    //这里默认m1和m2的相关度是相同的
                    rMatrix.set(k, k1, score);
                    rMatrix.set(k1, k, score);
                }
            }

            double[][] itemArray = rMatrix.getArray();
            //rMatrix入最后的结果
            for (int p = 0; p < moviesSize; p++) {
                List<Double> oneMovieList = new ArrayList<Double>();
                for (int f = 0; f < moviesSize; f++) {
                    oneMovieList.add(itemArray[p][f]);
                }
                relationMat.add(oneMovieList);
            }
            return relationMat;
        });

        final List<List<Double>> itemCfList = relationMatrix.collect();


        /**
         * date:2017/12/5
         * description:
         * 拿到了这个二维list,有了item*item之间的关系
         * 也有了  < un,[1.0,2.0.....] > 这个list< tuple >
         * 就可以做出< un,[(m1,1.0),(m9,2.0)...] > 注意这个地方,是un没有做评分的product
         */
        JavaPairRDD<Integer, List<Tuple2<Integer, Double>>> predictsMatrix = userMatrix.mapPartitionsToPair((pairs) -> {
            List<Tuple2<Integer, List<Tuple2<Integer, Double>>>> predictionPairs = new ArrayList<Tuple2<Integer, List<Tuple2<Integer, Double>>>>();
            //每一个user做一遍，就形成最后的结果
            while (pairs.hasNext()) {
                Tuple2<Integer, List<Double>> userLine = pairs.next();
                Integer userId = userLine._1();
                List<Double> scoreList = userLine._2();
                //给用户准备finalmovies和score预测
                List<List<Double>> finalMovies = new ArrayList<List<Double>>();
                List<Double> scorePredict = null;
                //该用户的评分List
                for (int i = 0; i < scoreList.size(); i++) {
                    if (scoreList.get(i) != 0) {
                        continue;
                    } else {//只看等于0的,才推荐,预测它的评分
                        int movieId = movieList.get(i);
                        List<Double> similarmoviesdist = itemCfList.get(i);
                        List<Double> temp = itemCfList.get(i);
                        //相关度从高到低排列，并且找出前20
                        Collections.sort(temp, Collections.reverseOrder());
                        double weightedsum = 0.0, numsum = 0.0, denumsum = 0.0;
                        for (int topI = 0; topI < 20; topI++) {
                            //相关度
                            Double relationScore = temp.get(topI);
                            //相关度高的movieId
                            int rMovieId = similarmoviesdist.indexOf(relationScore);
                            //用户给rmovie的评分 * rmovie和空movie的相关度
                            Double rmovieScore = scoreList.get(rMovieId);
                            double rScore = rmovieScore * relationScore;
                            numsum +=rScore;
                        }
                        //核心思想是:如果用户给一个product的评分是0，那么就用20个相似的，算出给它预测的评分

                    }
                }
            }
            return predictionPairs;
        });

        predictsMatrix.count();

//        JavaRDD<Integer> u1map = data.map(line -> {
//            int movieid = 0;
//            if (line.split("\t")[0].equals("2")) {
//                movieid = Integer.parseInt(line.split("\t")[1]);
//                return movieid;
//            }
//            return movieid;
//        });
//        List<Integer> u1movieList = u1map.filter(num -> {
//            return num > 0;
//        }).collect();
//        Collections.sort(u1movieList);
//        System.out.println(u1movieList);
//        System.out.println(userMatrix.collect().get(1));
    }
}
