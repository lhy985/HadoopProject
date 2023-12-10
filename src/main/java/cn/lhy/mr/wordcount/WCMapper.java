package cn.lhy.mr.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*
 * WordCount的mapper方法
 * 
*/
public class WCMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	private static final IntWritable One =  new IntWritable(1);
	private Text word = new Text();
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
			String line = value.toString();
			String[] words = line.split(" ");
			for(String token : words) {
				word.set(token);
				context.write(word, One);
			}
			
		
	}
	
}
