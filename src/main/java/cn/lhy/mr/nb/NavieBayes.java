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
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 5) {
			System.err.println("Usage: categorycount <in> <in> <out> <in> <out>");
			System.exit(5);
		}
		Job job1 = new Job(conf, "setup for trainingFiles");
		job1.setJarByClass(NavieBayes.class);
		job1.setMapperClass(SetupMapper.class);
		job1.setReducerClass(SetupReducer.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
		FileInputFormat.addInputPath(job1, new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(job1, new Path(otherArgs[2]));
		Job job2 = new Job(conf, "calculate probability for testFiles");
		job2.setJarByClass(NavieBayes.class);
		job2.setMapperClass(CalculateMapper.class);
		job2.setReducerClass(CalculateReducer.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job2, new Path(otherArgs[3]));
		FileOutputFormat.setOutputPath(job2, new Path(otherArgs[4]));
		System.exit(job1.waitForCompletion(true) && job2.waitForCompletion(true) ? 0 : 1);
	}
}
