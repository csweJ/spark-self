package com.cswe.spark.core;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

/**
 * 
 * @ClassName: WordCountLocal
 * @Description: 本地运行spark WordCountLocal 程序
 * @author Cswe
 * @date 2018年2月27日
 */
public class WordCountLocal {

    // 本地执行 可以在eclipse中执行main方法，看到对应的输出结果
    public static void main(String[] args) {
        // 1.创建SparkConf对象，设置spark应用的配置信息
        // 使用setMaster()可以设置Spark应用程序要连接的Spark集群的master节点的url
        // 但是如果是本地运行的话，则设置为local
        SparkConf sparkConf = new SparkConf().setAppName("WordCountLocal").setMaster("local");

        // 2.创建JavaSparkContext对象
        // 在Spark中，SparkContext是Spark所有功能的一个入口，你无论是用java，scala，或者是python编写，都必须有一个SaprkContext
        // 主要作用是，初始化Rdd数据，核心组件（如DAGSchedule、TaskScheduler调度器），还会去到Spark Master节点上进行注册，等等
        // 在Spark中，编写不同类型的Spark应用程序，使用的SparkContext是不同的，如果使用scala，
        // 使用的就是原生的SparkContext对象
        // 但是如果使用Java，那么就是JavaSparkContext对象
        // 如果是开发Spark SQL程序，那么就是SQLContext、HiveContext
        // 如果是开发Spark Streaming程序，那么就是它独有的SparkContext

        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        // 3.针对数据的输入源（hdfs文件，本地文件等）创建一个初始的RDD
        // 输入源中的数据会打散，分配到RDD的每个partition中，从而形成一个初始的分布式的数据集
        // SparkContext中，根据文件类型的输入源创建RDD的方法，叫做textFile()方法。
        // 在Java中，创建的普通RDD，都叫做JavaRDD
        // 在这里呢，RDD中，有元素这种概念，如果是hdfs或者本地文件呢，创建的RDD，每一个元素就相当于是文件里的一行
        JavaRDD<String> lines = sparkContext.textFile("G://test//spark.txt");

        // 4.对初始JavaRDD进行transformation操作，也就是一些算子计算操作，操作会通过创建一个function,配合RDD的map,reduceByKey
        // 等算子来执行。function如果比较简单，则创建指定Function的匿名内部类，但是如果比较复杂，则会单独创建一个类，实现对应Function接口
        
        
        //a.这里先将每一行字符串拆分成单个的单词，FlatMapFunction,有两个参数类型，第一个代表输入参数类型，第二个代表返回参数类型
        //flatMap算子的作用，其实就是，将RDD的一个元素，给拆分成一个或多个元素
        JavaRDD<String> words=lines.flatMap(new FlatMapFunction<String, String>() {

            /**
             * @Fields field:field:{todo}
             */
                
            private static final long serialVersionUID = 1L;

			@Override
			public Iterator<String> call(String arg0) throws Exception {
				// TODO Auto-generated method stub
				return Arrays.asList(arg0.split(" ")).iterator();
			}

           
        });

        
        //b.将每个单词映射成（word,1）这样的键值对，方便后面根据单词key累加个数
        //mapToPair 就是将每个元素映射成（v1,v2）这样的Tuple2类型的元素
        //mapToPair这个算子，要求的是与PairFunction配合使用，第一个泛型参数代表了输入类型
        // 第二个和第三个泛型参数，代表的输出的Tuple2的第一个值和第二个值的类型,Tuple 元组，存储不同类型，区别于数组
        // JavaPairRDD的两个泛型参数，分别代表了Tuple2,3元素的第一个值和第二个值的类型
        JavaPairRDD<String, Integer> pairs=words.mapToPair(new PairFunction<String, String, Integer>() {

            /**
             * @Fields field:field:{todo}
             */
                
            private static final long serialVersionUID = 1L;

            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                
                return new Tuple2<String, Integer>(word, 1);
            }
            
        });
        
        
        //c.以单词为key，统计每次出现的次数，
        //reduceByKey这个算子，对每个key对应的value,都进行reduce操作，第一个和第二个参数表示输入参数类型，第三个表示输出参数类型
        //比如JavaPairRDD中有几个元素，分别为(hello, 1) (hello, 1) (hello, 1) (world, 1)
        // reduce操作，相当于是把第一个值和第二个值进行计算，然后再将结果与第三个值进行计算
        // 比如这里的hello，那么就相当于是，首先是1 + 1 = 2，然后再将2 + 1 = 3
        // 最后返回的JavaPairRDD中的元素，也是tuple，但是第一个值就是每个key，第二个值就是key的value
        // reduce之后的结果，相当于就是每个单词出现的次数，
        JavaPairRDD<String, Integer> wordCounts=pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            
            /**
             * @Fields field:field:{todo}
             */
                
            private static final long serialVersionUID = 1L;

            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1+v2;
            }
        });
        
        // 到这里为止，我们通过几个Spark算子操作，已经统计出了单词的次数
        // 但是，之前我们使用的flatMap、mapToPair、reduceByKey这种操作，都叫做transformation操作
        // 一个Spark应用中，光是有transformation操作，是不行的，是不会执行的，必须要有一种叫做action
        // 接着，最后，可以使用一种叫做action操作的，比如:foreach，来触发程序的执行
        // t._1 Tuple2的第一个参数，t._2 Tuple2的第一个参数
        wordCounts.foreach(new VoidFunction<Tuple2<String,Integer>>() {
            
            @Override
            public void call(Tuple2<String, Integer> t) throws Exception {
                System.out.println(t._1 + " appeared " + t._2 + " times.");    
                
            }
        });
        
        
        sparkContext.close();
    }
}
