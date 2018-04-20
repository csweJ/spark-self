package com.cswe.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

/**
 * 
 * @ClassName: LocalFileToRDD
 * @Description: 使用本地文件創建RDD
 * @author Cswe
 * @date 2018年2月28日
 */
public class LocalFileToRDD {

    public static void main(String[] args) {
        // 创建SparkConf
        SparkConf sparkConf = new SparkConf().setAppName("LocalFileToRDD").setMaster("local");

        // 创建JavaSparkContext
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // 使用SparkContext以及其子类的textFile()方法，针对本地文件创建RDD
        JavaRDD<String> lines = sc.textFile("G://test//spark.txt");
        
        //统计文本文件的字数
        
        //统计每行的字数
        JavaRDD<Integer> lineLength=lines.map(new Function<String, Integer>() {

            /**
             * @Fields field:field:{todo}
             */
                
            private static final long serialVersionUID = 1L;

            @Override
            public Integer call(String v1) throws Exception {
                return v1.length();
            }
            
        });
        
        //将每行的字数相加
        Integer count=lineLength.reduce(new Function2<Integer, Integer, Integer>() {
            
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                // TODO Auto-generated method stub
                return v1+v2;
            }
        });
        
        
        System.out.println("字符统计个数："+count);
        sc.close();
    }

}
