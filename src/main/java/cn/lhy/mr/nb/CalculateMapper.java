package cn.lhy.mr.nb;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class CalculateMapper extends Mapper<Object, Text, Text, Text> {
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		try {
			FileSystem fs = FileSystem.get(URI.create("hdfs://192.168.210.132:9000"), new Configuration());
			Path ModelPath = new Path("/user/hadoop/output01/part-r-00000");
			if (!fs.exists(ModelPath))
				throw new IOException("Input file not found");
			if (!fs.isFile(ModelPath))
				throw new IOException("Input should be a file");
			FSDataInputStream in = fs.open(ModelPath);
			BufferedReader bufread = new BufferedReader(new InputStreamReader(in));
			String lineStr, keyStr = "", valueStr = "";
			while ((lineStr = bufread.readLine()) != null) {
				StringTokenizer tokenizer = new StringTokenizer(lineStr);
				if (tokenizer.hasMoreTokens())
					keyStr = tokenizer.nextToken();
				if (tokenizer.hasMoreTokens())
					valueStr = tokenizer.nextToken();
				context.write(new Text(keyStr), new Text(valueStr));
			}
			context.write(new Text("z_endofkey"), new Text(""));
			in.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		InputSplit inputSplit = context.getInputSplit();
		String testFileName = ((FileSplit) inputSplit).getPath().getName();
		StringTokenizer tokenizer = new StringTokenizer(value.toString());
		while (tokenizer.hasMoreTokens()) {
			String word = tokenizer.nextToken();
			context.write(new Text("fwPair_" + testFileName), new Text(word));
		}
	}
}
