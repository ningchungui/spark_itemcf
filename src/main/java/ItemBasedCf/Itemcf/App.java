package ItemBasedCf.Itemcf;

import java.util.*;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import Jama.Matrix;

import scala.Tuple2;

public class App {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("Collaborative Filtering").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        String path = "u.data";
        JavaRDD<String> data = sc.textFile(path);

        //2列代表movei_id,repartition是形成一个新的rdd
        final List<Integer> movies = data.map(
                new Function<String, Integer>() {
                    public Integer call(String s) {
                        String[] sarray = s.split("\t");
                        return new Integer(sarray[1]);
                    }
                }
        ).repartition(1).distinct().collect();
        Collections.sort(movies);

        //1列代表user_id
        final List<Integer> users = data.map(
                new Function<String, Integer>() {
                    public Integer call(String s) {
                        String[] sarray = s.split("\t");
                        return new Integer(sarray[0]);
                    }
                }
        ).repartition(1).distinct().collect();

        // Creating RDD. It is of the form <userid,<itemid,rating>> userid is key
        JavaPairRDD<Integer, Tuple2<Integer, Double>> ratings = data.mapToPair(
                new PairFunction<String, Integer, Tuple2<Integer, Double>>() {
                    public Tuple2<Integer, Tuple2<Integer, Double>> call(String r) {
                        String[] record = r.split("\t");
                        Double rating = Double.parseDouble(record[2]);
                        Integer userid = Integer.parseInt(record[0]);
                        Integer movieid = Integer.parseInt(record[1]);
                        return new Tuple2<Integer, Tuple2<Integer, Double>>(userid, new Tuple2<Integer, Double>(movieid, rating));
                    }
                }
        ).repartition(1).sortByKey();

        final int usersSize = users.size();
        final int m = users.size();
        final int moviesSize = movies.size();
        final int n = movies.size();

        JavaPairRDD<Integer, List<Double>> utility = ratings.mapPartitionsToPair
                (new PairFlatMapFunction<Iterator<Tuple2<Integer, Tuple2<Integer, Double>>>, Integer, List<Double>>() {

                     public Iterable<Tuple2<Integer, List<Double>>> call(
                             Iterator<Tuple2<Integer, Tuple2<Integer, Double>>> s)
                             throws Exception {
                         //s是整个ratingmap读进来
                         //一共10W行，每一行是userId,(mId,rating)
                         List<Double> userratings = new ArrayList<Double>();
                         //List <map < userid,list<rating> > >
                         List<Tuple2<Integer, List<Double>>> utilmatrix = new ArrayList<Tuple2<Integer, List<Double>>>();


                         while (s.hasNext()) {
                             userratings = new ArrayList<Double>();
                             for (int i = 0; i < movies.size(); i++) {
                                 userratings.add((double) 0);
                             }
                             Tuple2<Integer, Tuple2<Integer, Double>> t = s.next();
                             int userid = t._1();
                             int moviesid = t._2()._1();
                             Double rating = t._2()._2();
                             int index = movies.indexOf(moviesid);
                             //System.out.println(index);
                             userratings.set(index, rating);
                             //System.out.println(userratings);
                             while (s.hasNext()) {
                                 t = s.next();
                                 if (t._1() == userid) {
                                     moviesid = t._2()._1();
                                     rating = t._2()._2();
                                     index = movies.indexOf(moviesid);
                                     userratings.set(index, rating);
                                 } else
                                     break;

                             }
                             Tuple2<Integer, List<Double>> t1 = new Tuple2<Integer, List<Double>>(userid, userratings);

                             utilmatrix.add(t1);

                         }
                         return utilmatrix;
                     }
                 }

                );

        //utility.saveAsTextFile("utility");

        JavaRDD<List<Double>> cosinematrix = utility.mapPartitions(
                new FlatMapFunction<Iterator<Tuple2<Integer, List<Double>>>, List<Double>>() {

            public Iterable<List<Double>> call(
                    Iterator<Tuple2<Integer, List<Double>>> t) throws Exception {
                // TODO Auto-generated method stub

                int i = 0;

                List<Double> cosimilarlist = new ArrayList<Double>();
                List<List<Double>> cosinesimilarity = new ArrayList<List<Double>>();
                // users * movies users在前
                Matrix utils1 = new Matrix(m, n);
                // movie * movie
                Matrix cosine = new Matrix(n, n);
                double[][] y = null, y1 = null;
                //t有多少user，就有多少次循环。
                while (t.hasNext()) {
                    Tuple2<Integer, List<Double>> temp1 = t.next();
                    List<Double> lis = temp1._2();
                    for (int j = 0; j < lis.size(); j++) {
                        utils1.set(i, j, lis.get(j));
                    }
                    i++;
                }
                //users个double的数组
                double[] d = new double[m];
                double[] e = new double[m];
                //循环整个movieSize
                //当k=0的时候，指的就是第一个product
                for (int k = 0; k < n; k++) {
                    //循环整个userSize，即给当前这个product，一个user长度的数组
                    for (int j = 0; j < m; j++) {
                        double d1 = utils1.get(j, k);
                        //System.out.println(d1);
                        d[j] = d1;
                    }
                    //后面的product
                    for (int k1 = k + 1; k1 < n; k1++) {
                        double num = 0;
                        double d2 = 0;
                        double e2 = 0;
                        //构造这个数组
                        for (int g = 0; g < m; g++) {
                            e[g] = utils1.get(g, k1);
                        }
                        for (int w = 0; w < m; w++) {
                            //两个数组得分
                            num += d[w] * e[w];
                            d2 += d[w] * d[w];
                            e2 += e[w] * e[w];
                        }
                        double result;
                        if ((d2 == 0 || e2 == 0))
                            result = 0.0;
                        else
                            result = (num) / (Math.sqrt(e2) * Math.sqrt(d2));
                        //cosine是一个p*p的矩阵
                        cosine.set(k, k1, result);
                        cosine.set(k1, k, result);
                    }
                }
                utils1 = null;
                y1 = cosine.getArray();
                for (int p = 0; p < n; p++) {
                    cosimilarlist = new ArrayList<Double>();
                    for (int f = 0; f < n; f++) {

                        cosimilarlist.add(y1[p][f]);
                    }
                    cosinesimilarity.add(cosimilarlist);
                }

                cosine = null;

                return cosinesimilarity;

            }

        });



        final List<List<Double>> cosines = cosinematrix.collect();

        JavaPairRDD<Integer, List<Tuple2<Integer, Double>>> prediction =
            utility.mapPartitionsToPair(
                new PairFlatMapFunction<
                        Iterator<Tuple2<Integer, List<Double>>>,
                        Integer,
                        List<Tuple2<Integer, Double>>>() {

            public Iterable<Tuple2<Integer, List<Tuple2<Integer, Double>>>> call(
                    //输入的是List(map (userid,Listscore ) )
                    Iterator<Tuple2<Integer, List<Double>>>  t  ) throws Exception {

                List<Tuple2<Integer, List<Tuple2<Integer, Double>>>> weightedAvgPrediction
                        = new ArrayList<Tuple2<Integer, List<Tuple2<Integer, Double>>>>();
                //每一个用户
                while (t.hasNext()) {
                    //一个userId的记录
                    //一个userId的movieSize的list
                    Tuple2<Integer, List<Double>> userrating = t.next();
                    Integer userid = userrating._1();
                    List<Double> ratinglist = userrating._2();

                    List<Integer> predictedmovies = new ArrayList<Integer>();
                    List<Double> predictedratings = new ArrayList<Double>();
                    //一个userId有一个finalmovies
                    List<List<Double>> finalmovies = new ArrayList<List<Double>>();
                    List<Double> ratingpredict = new ArrayList<Double>();

                    //循环movieList的长度
                    for (int i = 0; i < ratinglist.size(); i++) {
                        //如果这个user在当前movieid的评分=0
                        if ((ratinglist.get(i) == 0)) {
                            //这个用户评0分的movieId
                            int movieid = movies.get(i);
                            //找到这个movie的List<Double>
                            //这个list有movieSize的大小,跟其他movie的相关程度
                            List<Double> similarmoviesdist = cosines.get(i);
                            List<Double> temp = cosines.get(i);

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
                                Double userrated_movie = ratinglist.get( similarmoviesdist.indexOf(temp.get(limit)) );
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


                        }else{

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
                        temp2.add(new Tuple2<Integer, Double>(finalmovies.get(j).get(0).intValue(), finalmovies.get(j).get(1)));
                        //weightedAvgPrediction.add(new Tuple2<Integer, List<Tuple2<Integer, Double>>>(userid,finalmovies.get(j).get(0),finalmovies.get(j).get(1));
                    }
                    //Tuple2<Integer, List<Tuple2<Integer, Double>>> t2=new Tuple2<Integer, List<Tuple2<Integer, Double>>>()

                    weightedAvgPrediction.add(new Tuple2<Integer, List<Tuple2<Integer, Double>>>(userid, temp2));
                }


                return weightedAvgPrediction;
            }


        });

//      prediction.saveAsTextFile("Prediction");

        System.out.println(prediction.count());
    }
}
