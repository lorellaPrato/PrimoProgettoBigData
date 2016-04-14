package job1;

import java.io.IOException;
import java.util.LinkedList;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

import input.Data;
import input.Receipt;

public class CountByMonthMapper extends Mapper<LongWritable, Text, Text, Writable>  {
	
//	private static final IntWritable one = new IntWritable();
	private Text word = new Text();
	private Text data = new Text();
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		
		String line = value.toString();
		
		StringTokenizer tokenizer = new StringTokenizer(line);
		String k = tokenizer.nextToken();
		data.set(k.substring(0,7));
		
		while(tokenizer.hasMoreTokens()){
			word.set(tokenizer.nextToken());
			context.write(data, word);
		}
	}
}
