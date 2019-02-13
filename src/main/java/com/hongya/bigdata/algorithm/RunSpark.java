package com.hongya.bigdata.algorithm;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import com.hongya.bigdata.run.SparkDeployUtils;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;

/**
 * 使用单线程运行算法调用，前台可以直接返回
 * @author root
 *
 */
public class RunSpark {
//	private Logger log = LoggerFactory.getLogger(getClass());

	public static String als_driver_memory = SparkDeployUtils.getProperty("als.driver-memory");
	public static String ratings = SparkDeployUtils.getProperty("ratings.data");
	public static String output = SparkDeployUtils.getProperty("output.data");
	public static String jar = SparkDeployUtils.getProperty("als.jar");




	public RunSpark() {
		
	}
	public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
		//<input> <output> <train_percent> <ranks> <lambda> <iteration>
		String[] inputArgs= new String[]{
				ratings,
				output,
				"0.8",
				"10",
				"10.0",
				"20"
		};
		String[] runArgs=new String[]{
//                "--name","ALS Model Train ",
                "--class","als.ALSModelTrainer",
//                "--driver-memory","512m",
//                "--num-executors", "2",
//                "--executor-memory", "512m",
                "--jar",jar,//
//                "--master","local[3]",
                "--arg",inputArgs[0],
                "--arg",inputArgs[1],
                "--arg",inputArgs[2],
                "--arg",inputArgs[3],
                "--arg",inputArgs[4],
                "--arg",inputArgs[5]
        };
		FileSystem.get(SparkDeployUtils.getConf()).delete(new Path(inputArgs[1]), true);

        System.setProperty("spark.master", "local[3]");


//		SparkDeployUtils.runSpark(runArgs);

        Class<?> clazz = Class.forName("als.ALSModelTrainer");
        Method method = clazz.getMethod("main", String[].class);
        method.invoke(clazz, (Object) inputArgs);
    }

	//<input> <output> <train_percent> <ranks> <lambda> <iteration>
	public static String runALS(String input,String output,String train_percent,String ranks,String lambda,
			String iteration) throws IllegalArgumentException, IOException{
		String[] runArgs=new String[]{
                "--name", SparkDeployUtils.getProperty("als.name"),
                "--class", SparkDeployUtils.getProperty("als.class"),
                "--driver-memory", SparkDeployUtils.getProperty("als.driver-memory"),
                "--num-executors", SparkDeployUtils.getProperty("als.num-executors"),
                "--executor-memory", SparkDeployUtils.getProperty("als.executor-memory"),
                "--jar", SparkDeployUtils.getProperty("als.jar"),//
                "--files", SparkDeployUtils.getProperty("als.files"),
                "--arg",input,
                "--arg",output,
                "--arg",train_percent,
                "--arg",ranks,
                "--arg",lambda,
                "--arg",iteration
        };
		String[] inputArgs= new String[]{
				input,
				output,
				train_percent,
				ranks,
				lambda,
				iteration
		};
		System.setProperty("HADOOP_USER_NAME", "root");
        System.setProperty("spark.master", "local[3]");
		FileSystem.get(SparkDeployUtils.getConf()).delete(new Path(output), true);

		return SparkDeployUtils.runSpark(inputArgs);
	}
}
