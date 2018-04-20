package com.cswe.spark.streaming;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

/**
 * 
 * @ClassName: WordCount
 * @Description: 利用sparkStreaming 实现wordCount
 * @author Cswe
 * @date 2018年3月5日
 */
public class WordCount {

    public static void main(String[] args) throws InterruptedException {
        
        //创建SparkConf
        //测试使用本地模式，设置master属性，local[*]至少2条线程，至少有一条用于不断的接受数据，至少有一条线程用户处理数据
        SparkConf sparkConf=new SparkConf().setAppName("WordCount Dstram").setMaster("local[2]");
        
        //创建sparkStreamContext  类似于spark.core 中的JavaSparkContext,该对象除了接收sparkConf之外，
        //还必须接收一个batch Interval 参数，就是每收集多长时间的数据，划分为一个batch,进行处理，这里设置1秒
        JavaStreamingContext jsc=new JavaStreamingContext(sparkConf, Durations.seconds(1));
        
        
        //创建DStream，代表了一个从数据源（File,HDFS,FLume,Kafka,Socket等）不断的实时数据流，
        //这里我们制定的数据来源于socket接口，Spark Streaming 连接上该端口并在运行的时候一直监听该端口的数据
        //JavaReceiverInputStream，代表了一个输入的DStream， 底层是某一时间段的RDD 数据集
        JavaReceiverInputDStream<String> lines=jsc.socketTextStream("192.168.31.222",8888);
        
        //JavaReceiverInputStream的泛型类型<String>，其实就代表了它底层的RDD的泛型类型
        //在底层，实际上是会对DStream中的一个一个的RDD，执行我们应用在DStream上的算子产生的新RDD，会作为新DStream中的RDD
        //用Spark Streaming开发程序，和Spark Core很相像
        //唯一不同的是Spark Core中的JavaRDD、JavaPairRDD，都变成了JavaDStream、JavaPairDStream
        JavaDStream<String> words= lines.flatMap(new FlatMapFunction<String, String>() {

            @Override
            public Iterator<String> call(String t) throws Exception {
                return Arrays.asList(t.split(" ")).iterator();
            }
        });
        
        JavaPairDStream<String, Integer> wordPairs=words.mapToPair(new PairFunction<String, String, Integer>() {

            @Override
            public Tuple2<String, Integer> call(String t) throws Exception {
                return new Tuple2<String, Integer>(t, 1);
            }
        });
        
        JavaPairDStream<String, Integer> wordCount=wordPairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                
                return v1+v2;
            }
        });
        
        
        //打印
        //此处的print并不会直接出发Job的执行，因为现在的一切都是在Spark Streaming框架的控制之下的，对于Spark Streaming 
        //而言具体是否触发真正的Job运行是基于设置的Duration时间间隔的 
        //一定要注意的是Spark Streaming应用程序要想执行具体的Job，对Dtream就必须有output Stream操作， 
        //output Stream有很多类型的函数触发，类print、saveAsTextFile、saveAsHadoopFiles等，最为重要的一个 
        //方法是foraeachRDD,因为Spark Streaming处理的结果一般都会放在Redis、DB、DashBoard等上面，foreachRDD 
        //主要就是用用来完成这些功能的，而且可以随意的自定义具体数据到底放在哪里！！
        wordCount.print();
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        // 首先对JavaSteamingContext进行一下后续处理
        // 必须调用JavaStreamingContext的start()方法，整个Spark Streaming Application才会启动执行
        // 否则是不会执行的
        jsc.start();
        jsc.awaitTermination();
        jsc.close();
    }

}
