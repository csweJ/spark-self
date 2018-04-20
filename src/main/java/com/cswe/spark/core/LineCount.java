package com.cswe.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

/**
 * 
 * @ClassName: LineCount
 * @Description: 统计每行在文件中出现的次数
 * @author Cswe
 * @date 2018年2月28日
 */
public class LineCount {

    public static void main(String[] args) {
        // 创建sparkconf
        SparkConf sparkConf = new SparkConf().setAppName("LineCount").setMaster("local");
        // 创建JavaSparkContext
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        // 创建RDD
        JavaRDD<String> lines = sc.textFile("G:\\test\\hello.txt");

        // 转换成键值对PairRDD <lineString,count>
        JavaPairRDD<String, Integer> linePair = lines.mapToPair(new PairFunction<String, String, Integer>() {

            /**
             * @Fields field:field:{todo}
             */

            private static final long serialVersionUID = 1L;

            @Override
            public Tuple2<String, Integer> call(String t) throws Exception {
                return new Tuple2<String, Integer>(t, 1);
            }

        });

        // 统计次数
        JavaPairRDD<String, Integer> count = linePair.reduceByKey(new Function2<Integer, Integer, Integer>() {

            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        count.foreach(new VoidFunction<Tuple2<String, Integer>>() {

            @Override
            public void call(Tuple2<String, Integer> t) throws Exception {
                System.out.println(t._1 + " appear " + t._2+" times");
            }
        });
        sc.close();
    }

}
