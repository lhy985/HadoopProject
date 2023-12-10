package cn.lhy.mr.wordcount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WCDriver {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// 通过Job来封装本次MR相关信息
		Configuration conf = new Configuration();
		//配置MR运行模式
		conf.set("mapreduce.framework.name", "local");

		Job wcjob = Job.getInstance(conf);
		//指定MR JOB jar包运行主类
		wcjob.setJarByClass(WCDriver.class);
		//指定本次MR的Mapper、Reducer类
		wcjob.setMapperClass(WCMapper.class);
		wcjob.setReducerClass(WCReducer.class);
		//设置业务逻辑输出类型
		wcjob.setMapOutputKeyClass(Text.class);
		wcjob.setMapOutputValueClass(IntWritable.class);
		wcjob.setOutputKeyClass(Text.class);
		wcjob.setOutputValueClass(IntWritable.class);
		//设置处理数据输入输出所在位置
		FileInputFormat.setInputPaths(wcjob, "E:\\export\\data\\mapReduce\\wordCount\\input");
		FileOutputFormat.setOutputPath(wcjob,new Path("E:\\export\\data\\mapReduce\\wordCount\\output") );
		boolean res =wcjob.waitForCompletion(true);
		System.exit(res ? 0:1);

	}

}
