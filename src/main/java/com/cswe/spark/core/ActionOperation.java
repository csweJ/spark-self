package com.cswe.spark.core;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import scala.Tuple2;


/**
 * 
 * @ClassName: ActionOperation
 * @Description: action 操作
 * @author Cswe
 * @date 2018年3月1日
 */
public class ActionOperation {

    public static void main(String[] args) {
        // TODO Auto-generated method stub
//        testReduceAction();
//        testCollectAction();
//        testCountAction();
//        testTakeAction();
        
        testCountByKeyAction();
    }

    /**
     * 
     * @Title: testReduceAction
     * @Description: reduce action
     * @param 
     * @return void
     * @throws
     */
    private static void testReduceAction() {
        SparkConf sparkConf=new SparkConf().setAppName("testReduceAction").setMaster("local");
        JavaSparkContext sc=new JavaSparkContext(sparkConf);
        
        List<Integer> numbers=Arrays.asList(1,2,3,4,5,6,7,8,9,10);
        
        JavaRDD<Integer> numberRDD=sc.parallelize(numbers);
        
        //使用reduce对集合中的数据进行累加  返回的是Integer
        //Function2 的第一个和第二个参数泛型为输入参数，第三个为返回的参数类型
        Integer count= numberRDD.reduce(new Function2<Integer, Integer, Integer>() {
            
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                // TODO Auto-generated method stub
                return v1+v2;
            }
        });
        
        System.out.println(count);
        sc.close();
        
    }
    
    /**
     * 
     * @Title: testCollectAction
     * @Description: Collect Action
     * @param 
     * @return void
     * @throws
     */
    private static void testCollectAction() {
        SparkConf sparkConf=new SparkConf().setAppName("testCollectAction").setMaster("local");
        JavaSparkContext sc=new JavaSparkContext(sparkConf);
        
        List<Integer> numberList = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        JavaRDD<Integer> numbers = sc.parallelize(numberList);
        
        //使用map 将每个元素都乘以2
        JavaRDD<Integer> multiNumber=numbers.map(new Function<Integer, Integer>() {

            @Override
            public Integer call(Integer v1) throws Exception {
                // TODO Auto-generated method stub
                return v1*2;
            }
        });
        //没有用foreach action 操作，在远程的集群上遍历Rdd中的元素 ，而是使用的collect 将远程集群上的RDD数据拉取到本地
        //这种方式一般不建议使用，如果数据量比较大的话，性能可能会差，而且还肯可能发生oom异常，内存溢出，通常推荐使用foreach
        List<Integer> multiNumberList=multiNumber.collect();
        
        for(Integer num:multiNumberList) {
            System.out.println(num);
        }
    }
    
    
    /**
     * 
     * @Title: testCountAction
     * @Description: count action  统计元素个数
     * @param 
     * @return void
     * @throws
     */
    private static void testCountAction() {
        SparkConf sparkConf=new SparkConf().clone().setAppName("testCountAction").setMaster("local");
        
        JavaSparkContext sc=new JavaSparkContext(sparkConf);
        
        List<Integer> numberList = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        JavaRDD<Integer> numbers = sc.parallelize(numberList);
        // 对rdd使用count操作，统计它的元素个数
        Long count= numbers.count();
        System.out.println(count);
        
        sc.close();
    }
    
    /**
     * 
     * @Title: testTakeAction
     * @Description: Take Action 获取前N个元素
     * @param 
     * @return void
     * @throws
     */
    private static void testTakeAction() {
        SparkConf sparkConf=new SparkConf().clone().setAppName("testTakeAction").setMaster("local");
        
        JavaSparkContext sc=new JavaSparkContext(sparkConf);
        
        List<Integer> numberList = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        JavaRDD<Integer> numbers = sc.parallelize(numberList);
        // take操作，与collect类似，也是从远程集群上，获取rdd的数据
        // 但是collect是获取rdd的所有数据，take只是获取前n个数据
        List<Integer> top3Num =numbers.take(3);
        System.out.println(top3Num);
        
        sc.close();
    }
    
    //根据key统计元素个数
    private static void testCountByKeyAction() {
        SparkConf sparkConf=new SparkConf().clone().setAppName("testCountByKeyAction").setMaster("local");
        
        JavaSparkContext sc=new JavaSparkContext(sparkConf);
        
        // 模拟集合
        List<Tuple2<String, String>> scoreList = Arrays.asList(
                new Tuple2<String, String>("class1", "leo"),
                new Tuple2<String, String>("class2", "jack"),
                new Tuple2<String, String>("class1", "marry"),
                new Tuple2<String, String>("class2", "tom"),
                new Tuple2<String, String>("class2", "david")); 
        
        JavaPairRDD<String, String> students = sc.parallelizePairs(scoreList);
        // 对rdd应用countByKey操作，统计每个班级的学生人数，也就是统计每个key对应的元素个数
        // 这就是countByKey的作用
        // countByKey返回的类型，直接就是Map<String, Long>
        Map<String, Long> studentCounts= students.countByKey();
        for(Entry<String, Long> studentCount:studentCounts.entrySet()) {
            System.out.println(studentCount.getKey()+":"+studentCount.getValue());
        }
        
        sc.close();
    }
    
    //将rdd数据保存在文件中
    private static void testSaveAsTextFile() {
        // 创建SparkConf和JavaSparkContext
        SparkConf conf = new SparkConf()
                .setAppName("saveAsTextFile");  
        JavaSparkContext sc = new JavaSparkContext(conf);
        
        List<Integer> numberList = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        JavaRDD<Integer> numbers = sc.parallelize(numberList);
        
        // 使用map操作将集合中所有数字乘以2
        JavaRDD<Integer> doubleNumbers = numbers.map(
                
                new Function<Integer, Integer>() {

                    private static final long serialVersionUID = 1L;
        
                    @Override
                    public Integer call(Integer v1) throws Exception {
                        return v1 * 2;
                    }
                    
                });
        
        // 直接将rdd中的数据，保存在HFDS文件中
        // 但是要注意，我们这里只能指定文件夹，也就是目录
        // 那么实际上，会保存为目录中的/double_number.txt/part-00000文件
        doubleNumbers.saveAsTextFile("hdfs://spark1:9000/double_number.txt");   
        
        // 关闭JavaSparkContext
        sc.close();
    }
}
