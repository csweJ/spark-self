package com.cswe.spark.core;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

/**
 * 
 * @ClassName: ParallelizeCollection
 * @Description: 并行集合创建数据，累加1到10
 * @author Cswe
 * @date 2018年2月28日
 */
public class ParallelizeCollection {

    public static void main(String[] args) {
        // 创建sparkConf
        SparkConf sparkConf = new SparkConf().setAppName("ParallelizeCollection").setMaster("local");

        // 创建javaSparkContext
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // 通过并行化集合的方式创建RDD,调用SparkContext或者其子类的parallelize()方法
        List<Integer> nums = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        JavaRDD<Integer> numRDD = sc.parallelize(nums);

        // 执行reduce算子，叠加数据
        Integer sum = numRDD.reduce(new Function2<Integer, Integer, Integer>() {

            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        System.out.println("sum="+sum);
        
      
        sc.close();
    }

}
