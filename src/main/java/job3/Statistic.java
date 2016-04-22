package job3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Statistic {
	public static void main(String[] args) throws Exception {
		
		Job job = new Job(new Configuration(), "Statistic");
		job.setJarByClass(Statistic.class);
		
		job.setMapperClass(StatisticMapper.class);
		job.setCombinerClass(StatisticCombiner.class);
		
		job.setReducerClass(StatisticReducer.class);

		
		FileInputFormat.addInputPath(job, new Path(args[0]));	
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
	
		job.setMapOutputKeyClass(BiKeyWritable.class);
		job.setMapOutputValueClass(ItemWritable.class);
		job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
		job.waitForCompletion(true);
	}
}
