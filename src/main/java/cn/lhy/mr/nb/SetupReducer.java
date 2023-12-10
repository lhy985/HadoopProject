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
			// <k,v> = <"classification", classificationCount>
			HashSet<String> classificationSet = new HashSet<String>();
			for (Text classification : values) {
				classificationSet.add(classification.toString());
			}
			int countTraningClassifications = classificationSet.size();
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
}
