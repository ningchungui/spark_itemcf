package ItemBasedCf.Itemcf;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import Jama.Matrix;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

/**
 * Author: changdalin
 * Date: 2017/12/1
 * Description:
 **/
public class ItemCF {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("ITEM_CF").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        //
        String path = "u.data";
        JavaRDD<String> data = sc.textFile(path);
        //对每一行做处理，然后变成另外一个rdd
        //注意，无论是Function还是FlatMapFunction，都是第二个泛型是返回值
        //第一个就是call的传入参数
        JavaRDD<Integer> userRdd = data.map(
                new Function<String, Integer>() {
                    public Integer call(String s) throws Exception {
                        String[] arr = s.split("\t");
                        int userId = Integer.parseInt(arr[0]);
                        return userId;
                    }
                });
        List<Integer> userList = userRdd.repartition(1).distinct().collect();
        JavaRDD<Integer> movieRdd = data.map(
                new Function<String, Integer>() {
                    public Integer call(String v1) throws Exception {
                        String[] arr = v1.split("\t");
                        int movieId = Integer.parseInt(arr[1]);
                        return movieId;
                    }
                });
        final List<Integer> movieList = movieRdd.repartition(1).distinct().collect();
        Collections.sort(movieList);
        Collections.sort(userList);

        final int usersSize = userList.size();
        final int moviesSize = movieList.size();

        /**
         * date:2017/12/1
         * description: 制作rdd < userid, < movie,rating > >
         *     注意这是map型的rdd，初始的rdd是一行一个的。
         */
        JavaPairRDD<Integer, Tuple2<Integer, Double>> ratingMapRdd
                = data.mapToPair(new PairFunction<String, Integer, Tuple2<Integer, Double>>() {
            //返回的是tuple2,也就是map形式，进入的是string，形成tuple2
            public Tuple2<Integer, Tuple2<Integer, Double>> call(String s) throws Exception {
                String[] split = s.split("\t");
                int movieId = Integer.parseInt(split[1]);
                int userId = Integer.parseInt(split[0]);
                double rating = Double.parseDouble(split[2]);
                Tuple2<Integer, Double> m1 = new Tuple2<Integer, Double>(movieId, rating);
                Tuple2<Integer, Tuple2<Integer, Double>> result
                        = new Tuple2<Integer, Tuple2<Integer, Double>>(userId, m1);
                return result;
            }
        });
        //这一步排序很重要,是下面while的基础
        ratingMapRdd = ratingMapRdd.repartition(1).sortByKey();


        /**
         * date:2017/12/1
         * description:制作 List < map < userId,List< ratings > > >
         *     其中ratings是固定大小的list 一个用户带一个list，形成一个map，然后所有形成一个list
         *             要看输入是谁，是一整个partition还是一行
         *             mapPartitionsToPair 这个方法是输入全部的map,返回的是一个list< map >
         *     输入的是 iterator < map < userid,movieId> ,
         *     输出的是 iterator < map < userid,list< ratings > >
         *     mapPartitions 这个在下文，也是返回一个list，里面是Object
         *     mapPartitionsToPair 这个是返回一个list 里面装的是tuple
         *
         *
         *     输入是iterator
         *     < u1 , < m1,1.0 > >
         *     < u1 , < m3,0 > >
         *     输出是iterator,一个list < map >
         *     < u1, [0,1.0,4.0,0 ....] >
         *     < u2, [0,.....] >
         *     这个就是user矩阵,map的value是一个list，按照顺序排着movieSize的rating
         */
        JavaPairRDD<Integer, List<Double>> userMatrix = ratingMapRdd.mapPartitionsToPair(
                new PairFlatMapFunction<
                        Iterator<Tuple2<Integer, Tuple2<Integer, Double>>>, //输入值
                        //这个输出的K,V带到rdd的泛型
                        Integer,//输出key
                        List<Double>>() {
                    public Iterable<Tuple2<Integer, List<Double>>> call(
                            Iterator<Tuple2<Integer, Tuple2<Integer, Double>>> s) throws Exception {
                        //要返回的东西
                        List<Tuple2<Integer, List<Double>>> userMatrix
                                = new ArrayList<Tuple2<Integer, List<Double>>>();
                        while (s.hasNext()) {
                            Tuple2<Integer, Tuple2<Integer, Double>> temp = s.next();
                            ArrayList<Double> mList = new ArrayList<Double>();
                            for (int i = 0; i < moviesSize; i++) {
                                mList.add((double) 0);
                            }
                            int userId = temp._1();
                            int movieId = temp._2()._1();
                            double rating = temp._2()._2();
                            int index = movieList.indexOf(movieId);
                            mList.set(movieId, rating);
                            while (s.hasNext()) {
                                Tuple2<Integer, Tuple2<Integer, Double>> t = s.next();
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
                            userMatrix.add(oneUserTuple);
                        }
                        return userMatrix;
                    }
                });
        //       System.out.println(123);


        /**
         * date:2017/12/1
         * description:现在有 List < map < userId,List< ratings > > > 作为输入
         * 即输入是 List ( u1 list(movieSize) )
         * 输出是 Object = List(Double)
         * 输出是list< list > movieSize*movieSize
         * m1:[1.0,2.0,0,......]
         * m2:[0,0,.....]
         */
        JavaRDD<List<Double>> itemCfMatrix = userMatrix.mapPartitions(
                new FlatMapFunction<Iterator<Tuple2<Integer, List<Double>>>, List<Double>>() {
                    public Iterable<List<Double>> call(Iterator<Tuple2<Integer, List<Double>>> s) throws Exception {
                        List<List<Double>> result = new ArrayList<List<Double>>();
                        // users * movies users在前
                        Matrix userMatrix = new Matrix(usersSize, moviesSize);
                        // movie * movie
                        Matrix itemMatrix = new Matrix(moviesSize, moviesSize);
                        //s有多少user，就有多少次循环。
                        int i = 0;
                        while (s.hasNext()) {
                            Tuple2<Integer, List<Double>> temp1 = s.next();
                            List<Double> oneUserRatingList = temp1._2();
                            for (int j = 0; j < oneUserRatingList.size(); j++) {
                                userMatrix.set(i, j, oneUserRatingList.get(j));
                            }
                            i++;
                        }

                        //记录两个相邻的product的得分情况
                        double[] s1 = new double[usersSize];
                        double[] s2 = new double[usersSize];
                        //循环所有物品 p2
                        for (int k = 0; k < moviesSize; k++) {
                            //循环整个userSize，即给当前这个product，一个user长度的数组
                            for (int j = 0; j < usersSize; j++) {
                                double d1 = userMatrix.get(j, k);
                                s1[j] = d1;
                            }
                            //后面的product p2
                            for (int k1 = k + 1; k1 < moviesSize; k1++) {
                                double num = 0;
                                double d2 = 0;
                                double e2 = 0;
                                for (int g = 0; g < usersSize; g++) {
                                    s2[g] = userMatrix.get(g, k1);
                                }
                                for (int w = 0; w < usersSize; w++) {
                                    //两个数组得分
                                    num += s1[w] * s2[w];
                                    d2 += s1[w] * s1[w];
                                    e2 += s2[w] * s2[w];
                                }
                                double score;
                                if ((d2 == 0 || e2 == 0))
                                    score = 0.0;
                                else
                                    score = (num) / (Math.sqrt(e2) * Math.sqrt(d2));
                                //cosine是一个p*p的矩阵
                                itemMatrix.set(k, k1, score);
                                itemMatrix.set(k1, k, score);
                            }
                        }
                        userMatrix = null;
                        double[][] itemArray = itemMatrix.getArray();
                        for (int p = 0; p < moviesSize; p++) {
                            List<Double> oneMovieList = new ArrayList<Double>();
                            for (int f = 0; f < moviesSize; f++) {
                                oneMovieList.add(itemArray[p][f]);
                            }
                            result.add(oneMovieList);
                        }
                        itemMatrix = null;
                        return result;
                    }
                });

        //其实一个list代表一个product的相关度，形成的list是所有的相关度
        final List<List<Double>> itemCfList = itemCfMatrix.collect();


        /**
         * date:2017/12/4
         * description:
         * 输出是整体一个list
         *       map < u1 , [(m1,0.4),(m2,0.3 )...] >
         *       map < u2 , [(m1,3),(m3,0.1)....] >
         */
        JavaPairRDD<Integer, List<Tuple2<Integer, Double>>> predictMatrix = userMatrix.mapPartitionsToPair(
                new PairFlatMapFunction<
                        Iterator<Tuple2<Integer, List<Double>>>,
                        Integer,
                        List<Tuple2<Integer, Double>>>() {

                    public Iterable<Tuple2<Integer, List<Tuple2<Integer, Double>>>> call(
                            //输入的是List( u1,[0.1,0.2 ...] )，这个就是userMatrix
                            Iterator<Tuple2<Integer, List<Double>>> t) throws Exception {

                        List<Tuple2<Integer, List<Tuple2<Integer, Double>>>> weightedAvgPrediction
                                = new ArrayList<Tuple2<Integer, List<Tuple2<Integer, Double>>>>();
                        //每一个用户
                        while (t.hasNext()) {
                            //一个userId的记录
                            //( u1,[1.0,2,3,5,....] )
                            Tuple2<Integer, List<Double>> userrating = t.next();
                            Integer userid = userrating._1();
                            List<Double> ratinglist = userrating._2();

                            //一个userId有一个推荐的finalmovies
                            List<List<Double>> finalmovies = new ArrayList<List<Double>>();
                            List<Double> ratingpredict = null;


                            //循环这个user的ratingList
                            for (int i = 0; i < ratinglist.size(); i++) {
                                //找评分为0的，评分为0的，就是可能要推荐的
                                if ((ratinglist.get(i) == 0)) {
                                    //这个用户评0分的movieId
                                    int movieid = movieList.get(i);
                                    //找到它的相似list
                                    List<Double> similarmoviesdist = itemCfList.get(i);
                                    List<Double> temp = itemCfList.get(i);

                                    double weightedsum = 0.0;
                                    double numsum = 0.0;
                                    double denumsum = 0.0;

                                    //temp按照分数倒序排列，相似度最高
                                    Collections.sort(temp, Collections.reverseOrder());
                                    //recommend 50个相似的，加起来得出score
                                    for (int limit = 0; limit < 50 && limit < moviesSize; limit++) {
                                        // 0 if user is not rated for that movie. Note: This default value depends on range of ratings
                                        //temp.get(limit)是那个分数
                                        //indexOf是movieId
                                        //rating.get是该user给这个 相似的movieId的评分
                                        Double userrated_movie = ratinglist.get(similarmoviesdist.indexOf(temp.get(limit)));
                                        // 相似度 * 该用户给它的评分
                                        numsum += userrated_movie * temp.get(limit);
                                        denumsum += temp.get(limit);

                                    }
                                    if (denumsum != 0)
                                        //用户A对m1的评分为0，那就找到m1的相似的m50，求一个平均权重
                                        weightedsum = numsum / denumsum;

                                    ratingpredict = new ArrayList<Double>();
                                    ratingpredict.add((double) movieid);
                                    ratingpredict.add(weightedsum);
                                    // 一个ratingpredict的size = 2
                                    // 对于用户A评分为0的product，就会有一个ratingpredict
                                    // 一个用户有一个finalmovies
                                    finalmovies.add(ratingpredict);


                                } else {

                                }
                            }
                            //相关度高的排在上面
                            Collections.sort(finalmovies, new Comparator<List<Double>>() {

                                public int compare(List<Double> o1, List<Double> o2) {
                                    // TODO Auto-generated method stub
                                    if ((o1.get(1)) < (o2.get(1)))
                                        return 1;
                                    else if ((o1.get(1)) > (o2.get(1)))
                                        return -1;
                                    else
                                        return 0;
                                }
                            });


                            // Return Top N Recommendations, Suppose you want 10 Recommendations for the user
                            List<Tuple2<Integer, Double>> temp2 = new ArrayList<Tuple2<Integer, Double>>();
                            for (int j = 0; j < 10 && j < finalmovies.size(); j++) {
                                //(movieId,rating)
                                temp2.add(
                                        new Tuple2<Integer, Double>(
                                                finalmovies.get(j).get(0).intValue(),
                                                finalmovies.get(j).get(1)));
                            }
                            weightedAvgPrediction.add(
                                    new Tuple2<Integer, List<Tuple2<Integer, Double>>>(userid, temp2));
                        }
                        return weightedAvgPrediction;
                    }
                });

        System.out.println(predictMatrix.collect().get(0));
        /**
         * date:2017/12/1
         * description:这个地方，所谓扁平化，就是把每一行变成list，最后再合起来成为一整个list
         * 注意两个参数，第一个是输入的一行的类型，第二个是iterable的泛型的类型
         */
//        JavaRDD<Integer> matrix = data.flatMap(new FlatMapFunction<String, Integer>() {
//            //这个方法必须返回一个list
//            public Iterable<Integer> call(String s) throws Exception {
//                String[] split = s.split("\t");
//                int index0 = Integer.parseInt(split[0]);
//                int index1 = Integer.parseInt(split[1]);
//                int index2 = Integer.parseInt(split[2]);
//                List<Integer> line = new ArrayList<Integer>();
//                line.add(index0);
//                line.add(index1);
//                line.add(index2);
//                return line;
//            }
//        });
//        List<Integer> distinctMatrix = matrix.repartition(1).distinct().collect();
//        Collections.sort(distinctMatrix);
        //       System.out.println(distinctMatrix);
    }
}
