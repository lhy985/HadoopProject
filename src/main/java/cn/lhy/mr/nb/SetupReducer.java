package cn.lhy.mr.nb;

import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SetupReducer extends Reducer<Text, Text, Text, Text> {
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		String keyStr = key.toString();
		if (keyStr.startsWith("classificationCount")) {
			// 创建一个HashSet集合classificationSet用于存储不重复的分类标签
			HashSet<String> classificationSet = new HashSet<String>();
			for (Text classification : values) {
				classificationSet.add(classification.toString());
			}
			int countTraningClassifications = classificationSet.size();
			// 输出键值对，键为classificationCount，值为分类标签数
			context.write(key, new Text("" + countTraningClassifications));
		} else if (keyStr.startsWith("fileNameCount")) {
			// <k,v> = <"fileName", fileNameCount>
			HashSet<String> fileNameSet = new HashSet<String>();
			for (Text fileName : values) {
				fileNameSet.add(fileName.toString());
			}
			int trainingFileCount = fileNameSet.size();
			context.write(key, new Text("" + trainingFileCount));
		} else if (keyStr.startsWith("cfPair")) {
			// <k,v> = <classification, fileNameCount>
			HashSet<String> fileNameSet = new HashSet<String>();
			for (Text fileName : values) {
				fileNameSet.add(fileName.toString());
			}
			context.write(key, new Text("" + fileNameSet.size()));
		} else if (keyStr.startsWith("wcPair")) {
			// <k,v> = <word_classification, fileNameCount>
			HashSet<String> fileNameSet = new HashSet<String>();
			for (Text fileName : values) {
				fileNameSet.add(fileName.toString());
			}
			context.write(key, new Text("" + fileNameSet.size()));
		}
	}

	// 经过rudue函数，输出文件内容为：
	// "classificationCount" num(分类标签数)
	// "fileName" num(训练文件总数)
	// cfpair +标签 num(各标签的文件数量)
	// wcPair_单词_分类标签 num(单词数量）
}
