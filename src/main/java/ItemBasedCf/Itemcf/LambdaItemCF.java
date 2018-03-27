package ItemBasedCf.Itemcf;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import Jama.Matrix;
import org.apache.spark.Partitioner;
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
        //这个地方的partition确实会影响mapPartition函数，一般就整个放进去
        JavaRDD<String> data = sc.textFile(path);

        //去重找到userList
        JavaRDD<Integer> userRdd = data.map((line) -> {
            return Integer.parseInt(line.split("\t")[0]);
        });
        List<Integer> userList = userRdd.repartition(1).distinct().collect();
        //去重找到movieList
        JavaRDD<Integer> movieRdd = data.map((line) -> {
            return Integer.parseInt(line.split("\t")[1]);
        });
        List<Integer> movieList = movieRdd.repartition(1).distinct().collect();
//        movieRdd.count();
        // 有一个排序的过程，所以后面不用总是带着 < productId,value >了
        Collections.sort(movieList);
        Collections.sort(userList);
        //943
        final int usersSize = userList.size();
        System.out.println("用户数量:" + usersSize);
        //1682
        final int moviesSize = movieList.size();
        System.out.println("product数量:" + moviesSize);

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
        pairRDD = pairRDD.partitionBy(new Partitioner() {
            @Override
            public int numPartitions() {
                return 3;
            }

            //可见只要按照key分的话，那么不会出现同一个key在两个不同partition里面的情况
            //相同一个key，对应的就是同一个分区
            @Override
            public int getPartition(Object key) {
                return 1;
            }
        });

        /**
         * date:2017/12/5
         * description:将pairRdd全部导入
         * 因为不是每一行改变，而是整体改变，所以全部导入
         * 输入是iterator，其实就是data的行数
         *     < u1 , < m1,1.0 > >
         *     < u1 , < m3,0 > >
         * 输出是iterator,一个list,这个就是 u,movieList 的userMatrix
         *     < u1, [0,1.0,4.0] > ,     < u2, [0,.....] >0 ....] >
         *
         * 从输出程序可以看出，userMatrix是最后合在一块儿了
         * pairRdd是5个partition，那么这个函数调用5次
         * 唯一有个问题，为什么user的分组都恰恰刚好隔离开userId,这肯定是partition的问题
         * 分区问题在于partitioner
         */
        JavaPairRDD<Integer, List<Double>> userMatrix = pairRDD.mapPartitionsToPair((pairs) -> {
            System.out.println(1.1);
            List<Tuple2<Integer, List<Double>>> userMat
                    = new ArrayList<>();
            while (pairs.hasNext()) {
                Tuple2<Integer, Tuple2<Integer, Double>> temp = pairs.next();
                ArrayList<Double> mList = new ArrayList<Double>();
                for (int i = 0; i < moviesSize; i++) {
                    mList.add((double) 0);
                }
                int userId = temp._1();
                int movieId = temp._2()._1();
                double rating = temp._2()._2();
                int index = movieList.indexOf(movieId);
                mList.set(index, rating);
                while (pairs.hasNext()) {
                    Tuple2<Integer, Tuple2<Integer, Double>> t = pairs.next();
                    if (t._1() == userId) {
                        movieId = t._2()._1();
                        rating = t._2()._2();
                        index = movieList.indexOf(movieId);
                        mList.set(index, rating);
                    } else {
                        break;
                    }
                }
                Tuple2<Integer, List<Double>> oneUserTuple =
                        new Tuple2<Integer, List<Double>>(userId, mList);
                userMat.add(oneUserTuple);
            }
            System.out.println("userMatSize:" + userMat.size());
            return userMat;
        });
        // 942 938 933 923
        System.out.println("用户矩阵大小:" + userMatrix.count());


        /**
         * date:2017/12/7
         * description:通过scala的代码可知，很多函数返回值都是new了个东西，所以必须有接收的，不会是更改内存地址
         */
        userMatrix = userMatrix.repartition(1);
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


        System.out.println("itemCfSize:" + itemCfList.size() + "-----" + itemCfList.get(0).size());
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
                Tuple2<Integer, Double> scorePredict = null;
                List<Tuple2<Integer, Double>> finalMovies = new ArrayList<Tuple2<Integer, Double>>();
                //该用户的评分List
                for (int i = 0; i < scoreList.size(); i++) {
                    if (scoreList.get(i) != 0) {
                        continue;
                    } else {//只看等于0的,才推荐,预测它的评分
                        int movieId = movieList.get(i);
                        //user给0的这个item矩阵
                        List<Double> similarmoviesdist = itemCfList.get(i);
                        //user给0的这个item矩阵排序
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
                            numsum += rScore;
                            denumsum += relationScore;
                        }
                        //核心是:如果用户给一个product的评分是0，那么就用20个相似的，算出给它预测的评分
                        if (denumsum != 0) {
                            weightedsum = numsum / denumsum;
                        }
                        scorePredict = new Tuple2<Integer, Double>(movieId, weightedsum);
                        finalMovies.add(scorePredict);
                    }
                }
                //finalMovies是该用户评分=0的product，赋值
                //找最大的
                Collections.sort(finalMovies, (t1, t2) -> {
                    if (t1._2() < t2._2()) {
                        return 1;
                    } else if (t1._2() > t2._2()) {
                        return -1;
                    }
                    return 0;
                });


                //从finalMovies里面，找top5
                List<Tuple2<Integer, Double>> oneUserRecommend = new ArrayList<Tuple2<Integer, Double>>();
                for (int j = 0; j < 5 && j < finalMovies.size(); j++) {
                    //(movieId,rating)
                    oneUserRecommend.add(
                            new Tuple2<Integer, Double>(finalMovies.get(j)._1(), finalMovies.get(j)._2()));
                }
                predictionPairs.add(new Tuple2(userId, oneUserRecommend));
            }
            return predictionPairs;
        });


        /**
         * date:2017/12/6
         * description:predicsMatrix是比较高的要求了，对每一个userId都recommend相似的product
         * 这是离线的个性化推荐体系
         * 普通情况下，有了itemCfMatrix就可以了，推荐相似度高的product
         * 想一想实时个性化推荐
         */
        System.out.println(predictsMatrix.collect());

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
