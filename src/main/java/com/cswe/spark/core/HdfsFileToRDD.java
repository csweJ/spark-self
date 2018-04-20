package com.cswe.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

/**
 * 
 * @ClassName: HdfsFileToRDD
 * @Description: hdfs集群上文件生成，使用spark-sumbit 提交spark程序包执行，利用mvn 打包：clean package
 * @author Cswe
 * @date 2018年2月28日
 */
public class HdfsFileToRDD {

    public static void main(String[] args) {
        // 创建SparkConf  相对于本地 不需要设置setmaster
        SparkConf sparkConf = new SparkConf().setAppName("LocalFileToRDD");

        // 创建JavaSparkContext
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // 使用SparkContext以及其子类的textFile()方法，使用hdfs上的文件
        JavaRDD<String> lines = sc.textFile("hdfs://hacluster//test//spark.txt");
        
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
