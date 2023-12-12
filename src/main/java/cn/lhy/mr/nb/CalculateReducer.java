package cn.lhy.mr.nb;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CalculateReducer extends Reducer<Text, Text, Text, Text> {
	private static final double M = 0.0;
	private double zoomFactor = 1000.0;
	int classificationCount = 0;// 训练文件的类别数
	int fileNameCount = 0;// 训练文件的总数
	HashMap<String, Integer> cfPair = new HashMap<String, Integer>();
	// <k,v> =<classification,fileNameCount>
	HashMap<String, Integer> wcPair = new HashMap<String, Integer>();
	// <k,v> =<word_classification,fileNameCount>，某一类别中单词出现的次数
	HashMap<String, HashSet<String>> fwPair = new HashMap<String, HashSet<String>>();
	// <k,v> = <fileName, set<word> >，测试文件名--->文件中包含的单词
	HashMap<String, Integer> classCoutPair = new HashMap<String, Integer>();
	// 记录预测每个类别的数量，方便计算f1值

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		String keyStr = key.toString();
		// 初始化上面成员变量
		if (keyStr.startsWith("classificationCount")) {
			for (Text valueStr : values)
				classificationCount = Integer.parseInt(valueStr.toString());
		} else if (keyStr.startsWith("fileNameCount")) {
			for (Text valueStr : values)
				fileNameCount = Integer.parseInt(valueStr.toString());
		} else if (keyStr.startsWith("cfPair")) {
			// <k,v> = <classification, fileNameCount>
			for (Text valueStr : values)
				cfPair.put(keyStr.split("_")[1], Integer.parseInt(valueStr.toString()));
		} else if (keyStr.startsWith("wcPair")) {
			// <k,v> = <word_classification, fileNameCount>
			String[] wcPairArr = keyStr.split("_");
			for (Text valueStr : values)
				wcPair.put(wcPairArr[1] + "_" + wcPairArr[2], Integer.parseInt(valueStr.toString()));
		} else if (keyStr.startsWith("fwPair")) {
			// <k,v> = <fileName, set<word> >
			String[] fwPairArr = keyStr.split("_");
			String testFileName = fwPairArr[1];
			for (Text valueStr : values) {
				if (fwPair.get(testFileName) != null) {
					fwPair.get(testFileName).add(valueStr.toString());
				} else {
					HashSet<String> wordSet = new HashSet<String>();
					wordSet.add(valueStr.toString());
					fwPair.put(testFileName, wordSet);
				}
			}
		}

		if (keyStr.startsWith("z_endofkey")) {
			// 根据排序，"z_endofkey"这个键在最后，此时已经遍历完所有的测试文件
			Set<String> keySet = fwPair.keySet();// 获取所有测试文件名
			for (Iterator<String> iterTestFileSet = keySet.iterator(); iterTestFileSet.hasNext();) {
				// 遍历每个测试文件，计算其属于每个分类可能性
				String curFileName = iterTestFileSet.next();
				Set<String> cfPairKeySet = cfPair.keySet();// 获取所有类别
				HashMap<String, Double> resultMap = new HashMap<String, Double>();
				for (Iterator<String> iter = cfPairKeySet.iterator(); iter.hasNext();) {
					// 遍历每个类别，计算属于当前curClassification的可能性
					String curClassification = iter.next();
					double Nc = 0.0;
					if (cfPair.get(curClassification) != null)
						Nc = cfPair.get(curClassification);
					double N = fileNameCount;
					double priorProbability = Nc / N;
					// 计算当前类的可能性
					double ret = 1.0;
					HashSet<String> wordSet = fwPair.get(curFileName);// 获取当前文件的所有单词
					for (Iterator<String> iterWordSet = wordSet.iterator(); iterWordSet.hasNext();) {
						String curWord = iterWordSet.next();
						double Nwc = 0.0;
						if (wcPair.get(curWord + "_" + curClassification) != null)
							Nwc = wcPair.get(curWord + "_" + curClassification);
						double V = classificationCount;
						// avoid Nwc = 0.0， add 1
						// the result is too small ， so multiply it by zoomFactor,numerical size of the
						// zoomFactor depends on the Nc
						ret *= ((Nwc + 1) / (Nc + M + V)) * zoomFactor;
					}
					ret *= priorProbability;
					resultMap.put(curClassification, Double.valueOf(ret));
				}
				// get the max probability classification of this file
				double maxProbability = 0.0;
				String maxProbabilityClassification = "";
				Set<String> classificationSet = resultMap.keySet();
				for (Iterator<String> iterClassificationSet = classificationSet.iterator(); iterClassificationSet
						.hasNext();) {
					String curClassification = iterClassificationSet.next();
					if (resultMap.get(curClassification) > maxProbability) {
						maxProbability = resultMap.get(curClassification);
						maxProbabilityClassification = curClassification;
					}
				}
				context.write(new Text(curFileName + " is belong to"), new Text(maxProbabilityClassification));
				if (classCoutPair.get(maxProbabilityClassification) == null) {
					classCoutPair.put(maxProbabilityClassification, Integer.valueOf(1));
				} else {
					int curvalue = classCoutPair.get(maxProbabilityClassification);
					classCoutPair.put(maxProbabilityClassification, curvalue + 1);
				}
			}
		} // end if("z_endofkey")
			// 在输出文件末尾，写入它判断每个类别的文件数目
		Set<String> classSet = classCoutPair.keySet();
		for (Iterator<String> iterclass = classSet.iterator(); iterclass.hasNext();) {
			String classfication = (String) iterclass.next();
			int num = classCoutPair.get(classfication);
			context.write(new Text(classfication), new Text(String.valueOf(num)));
		}
	}
}
