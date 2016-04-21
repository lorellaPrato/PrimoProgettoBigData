package noJob3;

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
		
		job.setMapperClass(PairFoodMapper.class);
		job.setCombinerClass(PairFoodCombiner.class);
		
		job.setMapperClass(CountReceiptMapper.class);
		job.setCombinerClass(CountReceiptCombiner.class);
		
		job.setMapperClass(SingleFoodMapper.class);
		job.setCombinerClass(SingleFoodCombiner.class);
		
		//job.setReducerClass(StatisticReducer.class);

		
//		FileInputFormat.addInputPath(job, new Path(args[0]));
		Path p1= new Path(args[0]);
		Path p2= new Path(args[1]);
		Path p3= new Path(args[2]);
		MultipleInputs.addInputPath(job, p1, TextInputFormat.class, CountReceiptMapper.class);
		MultipleInputs.addInputPath(job, p2, TextInputFormat.class, SingleFoodMapper.class);
		MultipleInputs.addInputPath(job, p3, TextInputFormat.class, PairFoodMapper.class);		
		FileOutputFormat.setOutputPath(job, new Path(args[3]));
	
		job.setMapOutputKeyClass(BiKeyWritable.class);
		job.setMapOutputValueClass(ItemWritable.class);
		job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
		job.waitForCompletion(true);
	}
}
