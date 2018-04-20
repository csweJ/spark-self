package com.cswe.spark.core;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class Transformation {

    public static void main(String[] args) {
        // testMapTrans();
        // testFilterTrans();
        // testFilterTrans();
        //testGroupByKeyTrans();
//        testReduceByKey();
//        testSortByKey();
//        testJoinTrans();
        testCogroupTrans();
    }

    /**
     * 
     * @Title: testMapTrans 
     * @Description: map算子，将集合中每个元素乘以2 
     * @param 
     * @return void
     * @throws
     */
    private static void testMapTrans() {
        // 创建sparkConf 本地运行
        SparkConf sparkConf = new SparkConf().setAppName("mapTransformation").setMaster("local");
        // 创建JavaSparkContext
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        // 构造集合
        List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5);
        // 并行化集合，创建RDD
        JavaRDD<Integer> numberRDD = sc.parallelize(numbers);

        // map 算子对任何类型RDD都可以调用 相当于对每个RDD元素遍历处理计算，返回新的类型值元素，所有新的元素就会组成一个新的RDD
        JavaRDD<Integer> multipleNumberRDD = numberRDD.map(new Function<Integer, Integer>() {

            private static final long serialVersionUID = 1L;

            @Override
            public Integer call(Integer v1) throws Exception {
                return v1 * 2;
            }
        });

        multipleNumberRDD.foreach(new VoidFunction<Integer>() {

            @Override
            public void call(Integer t) throws Exception {
                System.out.println(t);
            }
        });

        sc.close();
    }

    /**
     * 
     * @Title: testFilterTrans 
     * @Description: filter 算子，过滤偶数
     * @param 
     * @return void 
     * @throws
     */
    private static void testFilterTrans() {
        SparkConf sparkConf = new SparkConf().setAppName("testFilterTrans").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        // 并行化集合创建RDD
        JavaRDD<Integer> numbersRDD = sc.parallelize(numbers);
        // 对RDD执行filter算子操作，过滤其中的偶数 。 如果你想在新的RDD中保留这个元素，那么就返回true；否则，不想保留这个元素，返回false
        JavaRDD<Integer> evenNumbersRDD = numbersRDD.filter(new Function<Integer, Boolean>() {

            @Override
            public Boolean call(Integer v1) throws Exception {
                return v1 % 2 == 0;
            }
        });

        evenNumbersRDD.foreach(new VoidFunction<Integer>() {

            @Override
            public void call(Integer t) throws Exception {
                System.out.println(t);
            }

        });

        sc.close();
    }

    /**
     * 
     * @Title: testFlatMapTrans 
     * @Description: flatMap 將文本拆分成多个单词 
     * @param 
     * @return void 
     * @throws
     */
    private static void testFlatMapTrans() {
        SparkConf sparkConf = new SparkConf().setAppName("testFlatMapTrans").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        List<String> lineList = Arrays.asList("hello java", "hello scala", "hello php");

        // 执行并行华操作，创建RDD
        JavaRDD<String> lines = sc.parallelize(lineList);
        // 对RDD执行flatMap算子，将每一行文本，拆分为多个单词
        // flatMap其实就是，接收原始RDD中的每个元素，并进行各种逻辑的计算和处理，返回可以返回多个元素
        // 多个元素，即封装在Iterable集合中，可以使用ArrayList等集合
        // 新的RDD中，即封装了所有的新元素；也就是说，新的RDD的大小一定是 >= 原始RDD的大小
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {

            @Override
            public Iterator<String> call(String t) throws Exception {
                return  Arrays.asList(t.split(" ")).iterator();
            }
        });

        words.foreach(new VoidFunction<String>() {

            @Override
            public void call(String t) throws Exception {
                System.out.println(t);
            }
        });

        sc.close();

    }

    /**
     * 
     * @Title: testGroupByKeyTrans
     * @Description: groupByKey算子 按照姓名对成绩进行分组
     * @param 
     * @return void
     * @throws
     */
    private static void testGroupByKeyTrans() {
        SparkConf sparkConf = new SparkConf().setAppName("testGroupByKeyTrans").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // 创建数据集合
        List<Tuple2<String, Integer>> scoreList = Arrays.asList(new Tuple2<String, Integer>("Jack", 98),
                new Tuple2<String, Integer>("kitty", 78), new Tuple2<String, Integer>("Jack", 62),
                new Tuple2<String, Integer>("sf", 85), new Tuple2<String, Integer>("kitty", 82));
        // 并行化数据 创建RDD
        JavaPairRDD<String, Integer> scores = sc.parallelizePairs(scoreList);

        //执行groupByKey算子，按照姓名对成绩进行分组 返回的仍是JavaPairRDD
        //但是，JavaPairRDD的第一个泛型类型不变，第二个泛型类型变成Iterable这种集合类型
        //也就是说，按照了key进行分组，那么每个key可能都会有多个value，此时多个value聚合成了Iterable
        JavaPairRDD<String, Iterable<Integer>> groupedScores = scores.groupByKey();

        groupedScores.foreach(new VoidFunction<Tuple2<String, Iterable<Integer>>>() {

            @Override
            public void call(Tuple2<String, Iterable<Integer>> t) throws Exception {
                System.out.println("name:" + t._1);
                Iterator<Integer> ite = t._2.iterator();
                while (ite.hasNext()) {
                    System.out.println(ite.next());
                }
                System.out.println("===========================================================");

            }
        });

        sc.close();

    }

    /**
     * 
     * @Title: testReduceByKey
     * @Description: reduceByKey 算子，按照姓名统计学生总成绩
     * @param 
     * @return void
     * @throws
     */
    private static void testReduceByKey() {
        SparkConf sparkConf=new SparkConf().setAppName("testReduceByKey").setMaster("local");
        JavaSparkContext sc=new JavaSparkContext(sparkConf);
        
        // 创建数据集合
        List<Tuple2<String, Integer>> scoreList = Arrays.asList(new Tuple2<String, Integer>("Jack", 98),
                new Tuple2<String, Integer>("kitty", 78), new Tuple2<String, Integer>("Jack", 62),
                new Tuple2<String, Integer>("sf", 85), new Tuple2<String, Integer>("kitty", 82));
        // 并行化数据 创建RDD
        JavaPairRDD<String, Integer> scores = sc.parallelizePairs(scoreList);
        
        
        //执行reduceByKey 算子  Function2第一个和第二个参数泛型表示输入参数类型， 第三个表示输出 
        JavaPairRDD<String, Integer> totalScores= scores.reduceByKey(new Function2<Integer, Integer, Integer>() {
            
            // 对每个key，都会将其value，依次传入call方法
            // 从而聚合出每个key对应的一个value
            // 然后，将每个key对应的一个value，组合成一个Tuple2，作为新RDD的元素
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1+v2;
            }
        });
        
        totalScores.foreach(new VoidFunction<Tuple2<String,Integer>>() {

            @Override
            public void call(Tuple2<String, Integer> t) throws Exception {
                System.out.println(t._1+" score is "+t._2);
            }
        });
        
        sc.close();
    }
    
    /**
     * 
     * @Title: testSortByKey
     * @Description: sortByKey 算子，根据成绩进行排序
     * @param 
     * @return void
     * @throws
     */
    private static void testSortByKey() {
        SparkConf sparkConf=new SparkConf().setAppName("testSortByKey").setMaster("local");
        
        JavaSparkContext sc=new JavaSparkContext(sparkConf);
        
        // 创建数据集合
        List<Tuple2<Integer, String>> scoreList = Arrays.asList(new Tuple2<Integer, String>(98,"Jack"),
                new Tuple2<Integer, String>(78,"joson"), new Tuple2<Integer, String>(62,"puck"),
                new Tuple2<Integer, String>(85,"sf"), new Tuple2<Integer, String>(82,"kitty"));
        // 并行化数据 创建RDD
        JavaPairRDD<Integer, String> scores = sc.parallelizePairs(scoreList);
        
        //sortByKey 根据key进行排序，可以手动指定升序或者降序
        //返回的，还是JavaPairRDD，其中的元素内容，都是和原始的RDD一模一样的 只是顺序不同了
        //指定false 为降序
        JavaPairRDD<Integer, String> sortScores= scores.sortByKey(false);
        
        sortScores.foreach(new VoidFunction<Tuple2<Integer, String>>() {

            @Override
            public void call(Tuple2<Integer, String> t) throws Exception {
                System.out.println(t._2+" score is "+t._1);
                
            }
        });
        sc.close();
    }
    
    /**
     * 
     * @Title: testJoinTrans
     * @Description: join 根据key关联打印学生成绩 
     * @param 
     * @return void
     * @throws
     */
    private static void testJoinTrans() {
        SparkConf sparkConf=new SparkConf().setAppName("testJoinTrans").setMaster("local");
        JavaSparkContext sc=new JavaSparkContext(sparkConf);
        
        // 模拟集合
        List<Tuple2<Integer, String>> studentList = Arrays.asList(
                new Tuple2<Integer, String>(1, "leo"),
                new Tuple2<Integer, String>(2, "jack"),
                new Tuple2<Integer, String>(3, "tom"));
        
        List<Tuple2<Integer, Integer>> scoreList = Arrays.asList(
                new Tuple2<Integer, Integer>(1, 100),
                new Tuple2<Integer, Integer>(2, 90),
                new Tuple2<Integer, Integer>(3, 60));
        
        //创建两个RDD
        JavaPairRDD<Integer, String> students=sc.parallelizePairs(studentList);
        JavaPairRDD<Integer, Integer> scores=sc.parallelizePairs(scoreList);
        
        //使用join 算子根据key关联两个RDD,join 之后，会返回JavaPairRDD 其第一个泛型是key的类型，第二个泛型是Tuple2<v1,v2>
        //Tuple2的两个泛型分别为原始两个RDD的value的类型
        // join，就返回的RDD的每一个元素，就是通过key join上的一个pair
        // 比如有(1, 1) (1, 2) (1, 3)的一个RDD
        // 还有一个(1, 4) (2, 1) (2, 2)的一个RDD
        // join以后，实际上会得到(1 (1, 4)) (1, (2, 4)) (1, (3, 4))
        JavaPairRDD<Integer, Tuple2<String, Integer>> studentScores=students.join(scores);
        
        studentScores.foreach(new VoidFunction<Tuple2<Integer,Tuple2<String,Integer>>>() {

            @Override
            public void call(Tuple2<Integer, Tuple2<String, Integer>> t) throws Exception {
                System.out.println("studentId:"+t._1);
                System.out.println("studentName:"+t._2._1);
                System.out.println("score:"+t._2._2);
                System.out.println("===============================================================");
                
            }
        });
        
        sc.close();
    }
    
    /**
     * 
     * @Title: testCogroupTrans
     * @Description: cogroup 算子
     * @param 
     * @return void
     * @throws
     */
    private static void testCogroupTrans() {
        SparkConf sparkConf=new SparkConf().setAppName("testCogroupTrans").setMaster("local");
        JavaSparkContext sc=new JavaSparkContext(sparkConf);
        
        // 模拟集合
        List<Tuple2<Integer, String>> studentList = Arrays.asList(
                new Tuple2<Integer, String>(1, "leo"),
                new Tuple2<Integer, String>(2, "jack"),
                new Tuple2<Integer, String>(3, "tom"));
        
        List<Tuple2<Integer, Integer>> scoreList = Arrays.asList(
                new Tuple2<Integer, Integer>(1, 100),
                new Tuple2<Integer, Integer>(2, 90),
                new Tuple2<Integer, Integer>(3, 60),
                new Tuple2<Integer, Integer>(1, 70),
                new Tuple2<Integer, Integer>(2, 80),
                new Tuple2<Integer, Integer>(3, 50));
        
        // 并行化两个RDD
        JavaPairRDD<Integer, String> students = sc.parallelizePairs(studentList);
        JavaPairRDD<Integer, Integer> scores = sc.parallelizePairs(scoreList);
        // cogroup与join不同   一对多 或者多对多的关系
        // 相当于是，一个key join上的所有value，都给放到一个Iterable里面去了 
        JavaPairRDD<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> studentScores = students.cogroup(scores);
        
        studentScores.foreach(new VoidFunction<Tuple2<Integer,Tuple2<Iterable<String>,Iterable<Integer>>>>() {

            @Override
            public void call(Tuple2<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> t) throws Exception {
                System.out.println("studentId:"+t._1);
                System.out.println("student name: " + t._2._1);  
                System.out.println("student score: " + t._2._2);
                System.out.println("==============================="); 
                
            }
        });
        
        sc.close();
    }
}
