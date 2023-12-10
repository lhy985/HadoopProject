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
	int classificationCount = 0;
	int fileNameCount = 0;
	HashMap<String, Integer> cfPair = new HashMap<String, Integer>();
	// <k,v> =<classification,fileNameCount>
	HashMap<String, Integer> wcPair = new HashMap<String, Integer>();
	// <k,v> =<word_classification,fileNameCount>
	HashMap<String, HashSet<String>> fwPair = new HashMap<String, HashSet<String>>();// <k,v> = <fileName, set<word> >

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		String keyStr = key.toString();
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
			// traversal all testFiles
			Set<String> keySet = fwPair.keySet();
			for (Iterator<String> iterTestFileSet = keySet.iterator(); iterTestFileSet.hasNext();) {
				// have read a file over
				// calculate the probability of this file
				String curFileName = iterTestFileSet.next();
				Set<String> cfPairKeySet = cfPair.keySet();
				HashMap<String, Double> resultMap = new HashMap<String, Double>();
				for (Iterator<String> iter = cfPairKeySet.iterator(); iter.hasNext();) {
					String curClassification = iter.next();
					// calculate priorProbability
					double Nc = 0.0;
					if (cfPair.get(curClassification) != null)
						Nc = cfPair.get(curClassification);
					double N = fileNameCount;
					double priorProbability = Nc / N;
					// calculate classConditionalProbability
					double ret = 1.0;
					HashSet<String> wordSet = fwPair.get(curFileName);
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
					resultMap.put(curClassification, new Double(ret));
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
			}
		}
	}
}
