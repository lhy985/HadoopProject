package cn.lhy.mr.nb;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class NavieBayes {

	public static void main(String[] args) throws Exception {
		// 创建一个配置对象
		Configuration conf = new Configuration();
		conf.set("mapreduce.framework.name", "local");
		String[] otherArgs = new String[5];
		otherArgs[0] = "C:\\Users\\31840\\Desktop\\MapReduce Lab\\data\\training\\AUSTR";
		otherArgs[1] = "C:\\Users\\31840\\Desktop\\MapReduce Lab\\data\\training\\CANA";
		otherArgs[2] = "C:\\Users\\31840\\Desktop\\MapReduce Lab\\data\\training\\Output";
		otherArgs[3] = "C:\\Users\\31840\\Desktop\\MapReduce Lab\\data\\testing\\AUSTR";
		otherArgs[4] = "C:\\Users\\31840\\Desktop\\MapReduce Lab\\data\\testing\\AUSTR\\Output";
		// 解析输出参数
//		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
//		if (otherArgs.length != 5) {
//			System.err.println("Usage: categorycount <in> <in> <out> <in> <out>");
//			System.exit(5);
//		}
		// 创建一个作业对象
		Job job1 = Job.getInstance(conf);
//		Job job1 = new Job(conf, "setup for trainingFiles");
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
		FileOutputFormat.setOutputPath(job1, new Path(otherArgs[2]));

		// 计算
		Job job2 = Job.getInstance(conf);
		job2.setJarByClass(NavieBayes.class);
		job2.setMapperClass(CalculateMapper.class);
		job2.setReducerClass(CalculateReducer.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job2, new Path(otherArgs[3]));
		FileOutputFormat.setOutputPath(job2, new Path(otherArgs[4]));
		// 提交作业并等待完成
//		System.exit(job1.waitForCompletion(true) ? 0 : 1);
		System.exit(job1.waitForCompletion(true) && job2.waitForCompletion(true) ? 0 : 1);
	}
}
