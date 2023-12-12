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
		// 获取当前输入记录所属的InputSplit，它代表了输入文件的切片
		InputSplit inputSplit = context.getInputSplit();
		// 从InputSplit中获取当前输入记录所属的文件名
		String trainingFileName = ((FileSplit) inputSplit).getPath().getName();
		// InputSplit中获取当前输入记录所属的文件夹名（目录名）
		String trainingDirName = ((FileSplit) inputSplit).getPath().getParent().getName();
		String classification = trainingDirName;// 将文件夹名作为分类标签
		// 将输入记录（文本行）转换为StringTokenizer对象，用于逐个提取单词
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
			// 输出中间键值对，键为wcPair_单词_分类标签，值为当前文件名。这用于计算单词、分类和文件名的对应关系
			context.write(new Text("wcPair_" + word + "_" + classification), new Text(trainingFileName));
		}
	}
}
