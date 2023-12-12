package cn.lhy.mr.nb;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class NavieBayes {

	public static void main(String[] args) throws Exception {
		// 创建一个配置对象
		Configuration conf = new Configuration();
		// 解析输入参数，将命令行参数中除了通用选项之外的其他参数提取出来
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 4) {
			System.err.println("Usage: categorycount <train_in> <train_in> <test_in> <test_in>");
			System.exit(4);
		}
		// 创建一个作业对象
		Job job1 = Job.getInstance(conf);
		// 设置主类
		job1.setJarByClass(NavieBayes.class);
		// 设置Mapper和Reducer类
		job1.setMapperClass(SetupMapper.class);
		job1.setReducerClass(SetupReducer.class);
		// 设置输出键值对类型
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		// 添加多个输入文件路径
		FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
		FileInputFormat.addInputPath(job1, new Path(otherArgs[1]));
		// 设置输出文件路径
		FileOutputFormat.setOutputPath(job1, new Path("/NaiveBayes/data/training/Output"));

		// 计算
		Job job2 = Job.getInstance(conf);
		job2.setJarByClass(NavieBayes.class);
		job2.setMapperClass(CalculateMapper.class);
		job2.setReducerClass(CalculateReducer.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job2, new Path(otherArgs[2]));
		FileOutputFormat.setOutputPath(job2, new Path(otherArgs[2] + "/Output"));
		// 计算
		Job job3 = Job.getInstance(conf);
		job3.setJarByClass(NavieBayes.class);
		job3.setMapperClass(CalculateMapper.class);
		job3.setReducerClass(CalculateReducer.class);
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job3, new Path(otherArgs[3]));
		FileOutputFormat.setOutputPath(job3, new Path(otherArgs[3] + "/Output"));
		// 提交作业并等待完成
		Boolean finish = job1.waitForCompletion(true) && job2.waitForCompletion(true) && job3.waitForCompletion(true);
		System.exit(finish ? 0 : 1);
	}

}
