package cn.lhy.mr.nb;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class SetupMapper extends Mapper<Object, Text, Text, Text> {
	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		// filename and dirname
		InputSplit inputSplit = context.getInputSplit();
		String trainingFileName = ((FileSplit) inputSplit).getPath().getName();
		String trainingDirName = ((FileSplit) inputSplit).getPath().getParent().getName();
		String classification = trainingDirName;
		StringTokenizer tokenizer = new StringTokenizer(value.toString());
		while (tokenizer.hasMoreTokens()) {
			String word = tokenizer.nextToken();
			// <k,v> = <classification, 1>
			context.write(new Text("classificationCount"), new Text(classification));
			// <k,v> = <fileName, 1>
			context.write(new Text("fileNameCount"), new Text(trainingFileName));
			// <k,v> = <classification, fileName>
			context.write(new Text("cfPair_" + classification), new Text(trainingFileName));
			// <k,v> = <word_classification, fileName>
			context.write(new Text("wcPair_" + word + "_" + classification), new Text(trainingFileName));
		}
	}
}
